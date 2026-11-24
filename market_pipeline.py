#!/usr/bin/env python3
"""
Three-stage sports market discovery: Kalshi -> Polymarket -> ProphetX.

  1. Pull every Kalshi moneyline / spread / total market for games happening today.
  2. Pull today's Polymarket games, match them to the Kalshi games, match the individual
     markets (same bet type + same line), and trim Kalshi markets Polymarket doesn't have.
  3. Pull today's ProphetX events, match them to the surviving games, match markets again,
     and trim anything ProphetX doesn't have.

What survives is a bet that exists, identically, on all three venues, returned as a dict
with the tickers / ids you need to track prices.

Only moneyline, spread and total (full game) are handled: that's the overlap between all
three venues (ProphetX's Market Data API only exposes those three types).

Caching: results of stage 1 and stage 2 are saved to out/cache/ so that if ProphetX fails
you can rerun and skip the slow Kalshi/Polymarket pulls. An interim CSV with just
Kalshi + Polymarket ids (out/markets_kalshi_polymarket.csv) is written after stage 2.
Only ids are cached (no prices), so nothing goes stale except market open/close status;
the cache expires after --max-age minutes (default 120). Use --refresh to ignore it.

Credentials (env vars or a .env / secrets.txt file):
    PROPHETX_AFFILIATE_KEY      Market Data API key (sent raw in the Authorization header)
      or
    PROPHETX_ACCESS_KEY + PROPHETX_SECRET_KEY   Trading API token (logs in for a bearer token)
    PROPHETX_ENV=production|sandbox             default production
Kalshi and Polymarket market listings are public: no keys needed for this script.

From another file:
    from market_pipeline import discover
    games = discover()                      # today, in America/Chicago
    games = discover(tz="America/New_York")
    for key, g in games.items():
        for ident, m in g["markets"].items():     # "moneyline", "spread|-3.5", "total|45.5"
            m["kalshi"]       # [{"selection": "team0", "id": <kalshi ticker>, ...}, ...]
            m["polymarket"]   # [{"selection": "team0", "id": <CLOB token id>, "market_id": <condition id>}]
            m["prophetx"]     # [{"selection": "team0", "id": <strike_id>, "market_id": <market id>}]

Command line:
    python market_pipeline.py                 # writes out/markets.json
    python market_pipeline.py --date 2026-11-25 --tz America/New_York -v
    python market_pipeline.py --refresh       # ignore cached stage 1/2 results
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import pickle
import re
import sys
import time
import unicodedata
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from difflib import SequenceMatcher
from functools import lru_cache
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def _load_secret_files() -> None:
    for folder in (Path.cwd(), Path(__file__).resolve().parent):
        for name in (".env", "secrets.txt"):
            p = folder / name
            if not p.is_file():
                continue
            for line in p.read_text(encoding="utf-8-sig").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip().removeprefix("export ").strip()
                os.environ.setdefault(k, v.strip().strip("'\""))


_load_secret_files()

log = logging.getLogger("pipeline")

DEFAULT_TZ = "America/Chicago"
KALSHI_BASE = os.environ.get("KALSHI_API_BASE", "https://api.elections.kalshi.com/trade-api/v2")
POLY_GAMMA = "https://gamma-api.polymarket.com"
PX_BASES = {
    "production": "https://cash.api.prophetx.co/partner",
    "sandbox": "https://api.sandbox.prophetx.dev/partner",
}
MATCH_THRESHOLD = 0.85  # both teams must be at least this similar to count as the same game
UNRESOLVED: list = []  # samples of legs we couldn't interpret, dumped to out/debug for diagnosis


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #
def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=5, backoff_factor=1.0, status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=("GET", "POST"), respect_retry_after_header=True)
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers["User-Agent"] = "sports-market-pipeline/0.1"
    return s


def get_json(s, url: str, **kw):
    r = s.get(url, timeout=30, **kw)
    r.raise_for_status()
    return r.json()


# --------------------------------------------------------------------------- #
# Parsing helpers
# --------------------------------------------------------------------------- #
_SPLIT = re.compile(r"\s+(?:vs\.?|v\.?|at|@)\s+", re.I)
_TRAIL = re.compile(r"\b(?:winner|to win|moneyline|spread|totals?|total points|more markets|game \d+)\b.*$", re.I)
DRAW_RE = re.compile(r"\b(?:draw|tie)\b", re.I)
HALF_RE = re.compile(r"\b(?:1st|2nd|3rd|4th|first|second|third|fourth)\s+(?:half|quarter|period|inning)\b|\b[1-4]h\b",
                     re.I)
_MONTHS = {m: i for i, m in enumerate("JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split(), 1)}


def _clean_side(s: str) -> str:
    s = re.sub(r"^(?:who will win\s+(?:the\s+)?|will\s+(?:the\s+)?)", "", s, flags=re.I)
    s = re.sub(r"\s*[-–|:]\s.*$", "", s)
    s = re.sub(r"\s*\(.*?\)\s*", " ", s)
    s = _TRAIL.sub("", s)
    return s.replace("?", "").strip(" .")


def parse_matchup(title: str) -> Optional[tuple]:
    parts = _SPLIT.split((title or "").strip())
    if len(parts) != 2:
        return None
    a, b = _clean_side(parts[0]), _clean_side(parts[1])
    if not (2 <= len(a) <= 40 and 2 <= len(b) <= 40) or a.lower() == b.lower():
        return None
    return (a, b)


def to_day(value, tz: ZoneInfo) -> Optional[date]:
    """ISO timestamp -> calendar date in `tz`. Plain dates pass through unchanged."""
    if not value or not isinstance(value, str):
        return None
    v = value.strip().replace("Z", "+00:00")
    if len(v) == 10:
        try:
            return date.fromisoformat(v)
        except ValueError:
            return None
    if re.search(r"[+-]\d{2}$", v):  # "2026-11-24 18:00:00+00"
        v += ":00"
    try:
        dt = datetime.fromisoformat(v)
    except ValueError:
        try:
            return date.fromisoformat(v[:10])
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(tz).date()


def kalshi_ticker_date(ticker: str) -> Optional[date]:
    m = re.search(r"-(\d{2})([A-Z]{3})(\d{2})", ticker or "")
    if not m or m.group(2) not in _MONTHS:
        return None
    try:
        return date(2000 + int(m.group(1)), _MONTHS[m.group(2)], int(m.group(3)))
    except ValueError:
        return None


def _num(x) -> Optional[float]:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _jl(x) -> list:
    if isinstance(x, list):
        return x
    try:
        v = json.loads(x or "[]")
        return v if isinstance(v, list) else []
    except (TypeError, ValueError):
        return []


# --------------------------------------------------------------------------- #
# Team-name matching
# --------------------------------------------------------------------------- #
STOP = {"fc", "cf", "afc", "sc", "the", "of", "university", "univ", "club"}
TOKEN_MAP = {"man": "manchester", "utd": "united", "la": "los angeles", "ny": "new york", "saint": "st"}
GENERIC = {"new", "los", "san", "st", "city", "united", "state", "north", "south", "east", "west"}

ALIASES = """
Arizona|Cardinals;Atlanta|Falcons;Baltimore|Ravens;Buffalo|Bills;Carolina|Panthers;Chicago|Bears;
Cincinnati|Bengals;Cleveland|Browns;Dallas|Cowboys;Denver|Broncos;Detroit|Lions;Green Bay|Packers;
Houston|Texans;Indianapolis|Colts;Jacksonville|Jaguars;Kansas City|Chiefs;Las Vegas|Raiders;
Los Angeles|Chargers;Los Angeles|Rams;Miami|Dolphins;Minnesota|Vikings;New England|Patriots;
New Orleans|Saints;New York|Giants;New York|Jets;Philadelphia|Eagles;Pittsburgh|Steelers;
San Francisco|49ers;Seattle|Seahawks;Tampa Bay|Buccaneers;Tennessee|Titans;Washington|Commanders;
Atlanta|Hawks;Boston|Celtics;Brooklyn|Nets;Charlotte|Hornets;Chicago|Bulls;Cleveland|Cavaliers;
Dallas|Mavericks;Denver|Nuggets;Detroit|Pistons;Golden State|Warriors;Houston|Rockets;Indiana|Pacers;
Los Angeles|Clippers;Los Angeles|Lakers;Memphis|Grizzlies;Miami|Heat;Milwaukee|Bucks;
Minnesota|Timberwolves;New Orleans|Pelicans;New York|Knicks;Oklahoma City|Thunder;Orlando|Magic;
Philadelphia|76ers;Phoenix|Suns;Portland|Trail Blazers;Sacramento|Kings;San Antonio|Spurs;
Toronto|Raptors;Utah|Jazz;Washington|Wizards;
Anaheim|Ducks;Boston|Bruins;Buffalo|Sabres;Calgary|Flames;Carolina|Hurricanes;Chicago|Blackhawks;
Colorado|Avalanche;Columbus|Blue Jackets;Dallas|Stars;Detroit|Red Wings;Edmonton|Oilers;
Florida|Panthers;Los Angeles|Kings;Minnesota|Wild;Montreal|Canadiens;Nashville|Predators;
New Jersey|Devils;New York|Islanders;New York|Rangers;Ottawa|Senators;Philadelphia|Flyers;
Pittsburgh|Penguins;San Jose|Sharks;Seattle|Kraken;St. Louis|Blues;Tampa Bay|Lightning;
Toronto|Maple Leafs;Utah|Mammoth;Vancouver|Canucks;Vegas|Golden Knights;Washington|Capitals;
Winnipeg|Jets;
Arizona|Diamondbacks;Atlanta|Braves;Baltimore|Orioles;Boston|Red Sox;Chicago|Cubs;Chicago|White Sox;
Cincinnati|Reds;Cleveland|Guardians;Colorado|Rockies;Detroit|Tigers;Houston|Astros;Kansas City|Royals;
Los Angeles|Angels;Los Angeles|Dodgers;Miami|Marlins;Milwaukee|Brewers;Minnesota|Twins;New York|Mets;
New York|Yankees;Philadelphia|Phillies;Pittsburgh|Pirates;San Diego|Padres;San Francisco|Giants;
Seattle|Mariners;St. Louis|Cardinals;Tampa Bay|Rays;Texas|Rangers;Toronto|Blue Jays;Washington|Nationals
"""


@lru_cache(maxsize=None)
def norm(s: str) -> tuple:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower().replace("&", " and ")
    out = []
    for t in re.sub(r"[^a-z0-9 ]+", " ", s).split():
        if t in STOP:
            continue
        out.extend(TOKEN_MAP.get(t, t).split())
    return tuple(out)


ALIAS_SETS = [
    (frozenset(norm(c)), frozenset(norm(n)))
    for c, n in (p.strip().split("|") for p in ALIASES.replace("\n", "").split(";") if p.strip())
]


@lru_cache(maxsize=None)
def team_sim(a: str, b: str) -> float:
    ta, tb = norm(a), norm(b)
    if not ta or not tb:
        return 0.0
    sa, sb = set(ta), set(tb)
    if sa == sb:
        return 1.0
    best = 0.0
    small, big = (sa, sb) if len(sa) <= len(sb) else (sb, sa)
    if small <= big and (small - GENERIC):
        best = 0.9
    for city, nick in ALIAS_SETS:  # bare city vs nickname / full name
        if (sa == city and nick <= sb) or (sb == city and nick <= sa):
            best = max(best, 0.95)
            break
    return max(best, SequenceMatcher(None, " ".join(ta), " ".join(tb)).ratio())


def pair_similarity(a, b) -> float:
    straight = min(team_sim(a.teams[0], b.teams[0]), team_sim(a.teams[1], b.teams[1]))
    swapped = min(team_sim(a.teams[0], b.teams[1]), team_sim(a.teams[1], b.teams[0]))
    return max(straight, swapped)


def orientation(a_teams: tuple, b_teams: tuple) -> int:
    """0 if b's teams are in the same order as a's, 1 if swapped."""
    s = min(team_sim(a_teams[0], b_teams[0]), team_sim(a_teams[1], b_teams[1]))
    w = min(team_sim(a_teams[0], b_teams[1]), team_sim(a_teams[1], b_teams[0]))
    return 0 if s >= w else 1


@lru_cache(maxsize=None)
def team_forms(name: str) -> tuple:
    """Token sets that identify a team in free text ('Detroit', 'Lions', 'Detroit Lions')."""
    t = frozenset(norm(name))
    forms = {t}
    for city, nick in ALIAS_SETS:
        if t == city:
            forms.add(nick)
        elif nick <= t:
            forms.add(nick)
            if city <= t:
                forms.add(city)
    return tuple(f for f in forms if f and (f - GENERIC))


class Resolver:
    """Maps free text ('Packers +3.5', 'Detroit wins by over 3.5') to team index 0 or 1."""

    def __init__(self, names: tuple):
        self.names = names  # (set of spellings of team0, set of spellings of team1)

    def resolve(self, text: str) -> Optional[int]:
        toks = set(norm(text or ""))
        score = []
        for spellings in self.names:
            best = 0
            for n in spellings:
                for f in team_forms(n):
                    if f <= toks:
                        best = max(best, len(f))
            score.append(best)
        if score[0] == score[1]:
            return None  # neither, or ambiguous
        return 0 if score[0] > score[1] else 1


# --------------------------------------------------------------------------- #
# Data model
# --------------------------------------------------------------------------- #
def mk_leg(selection: str, market_id, id_, label: str = "", **extra) -> dict:
    return {"selection": selection, "market_id": str(market_id), "id": str(id_), "label": label, "extra": extra}


def ident_spread(h0: float) -> str:
    return f"spread|{round(h0, 2):+g}"  # handicap from team0's point of view


def ident_total(line: float) -> str:
    return f"total|{round(line, 2):g}"


@dataclass
class Ev:
    """One venue's event, before it's attached to a game."""
    venue: str
    id: str
    title: str
    teams: tuple
    day: date
    league: str = ""
    url: str = ""
    raw: dict = field(default_factory=dict)


@dataclass
class Game:
    teams: tuple  # canonical order = Kalshi's
    day: date
    league: str
    names: tuple  # (set, set): every spelling of each team seen across venues
    events: dict = field(default_factory=dict)  # venue -> {...ids/urls}
    legs: dict = field(default_factory=lambda: {"kalshi": {}, "polymarket": {}, "prophetx": {}})

    @property
    def resolver(self) -> Resolver:
        return Resolver(self.names)


def add_leg(game: Game, venue: str, ident: str, leg: dict) -> None:
    game.legs[venue].setdefault(ident, []).append(leg)


def note_unresolved(venue: str, why: str, text: str) -> None:
    if len(UNRESOLVED) < 200:
        UNRESOLVED.append({"venue": venue, "why": why, "text": text})


def trim_to_common(game: Game, venues: list) -> bool:
    """Keep only market identities present on every venue in `venues`."""
    common = set.intersection(*(set(game.legs[v]) for v in venues))
    for v in venues:
        game.legs[v] = {k: game.legs[v][k] for k in common}
    return bool(common)


def group_events(evs: list) -> list:
    """Collapse same-venue events about the same game (e.g. 'More Markets' pages)."""
    groups: list = []
    for e in evs:
        for g in groups:
            if g[0].day == e.day and pair_similarity(g[0], e) >= MATCH_THRESHOLD:
                g.append(e)
                break
        else:
            groups.append([e])
    return groups


def match_games(games: list, evs: list) -> list:
    """Greedy one-to-one best matches between Game objects and Ev groups -> [(game, [Ev,...])]."""
    groups = group_events(evs)
    cands = []
    for gi, g in enumerate(games):
        for ji, grp in enumerate(groups):
            if grp[0].day != g.day:
                continue
            sim = pair_similarity(g, grp[0])
            if sim >= MATCH_THRESHOLD:
                cands.append((sim, gi, ji))
    cands.sort(reverse=True)
    used_g, used_j, out = set(), set(), []
    for _, gi, ji in cands:
        if gi in used_g or ji in used_j:
            continue
        used_g.add(gi)
        used_j.add(ji)
        out.append((games[gi], groups[ji]))
    return out


def absorb_names(game: Game, ev: Ev) -> tuple:
    """Add ev's team spellings to the game (aligned by orientation); returns teams in game order."""
    o = orientation(game.teams, ev.teams)
    aligned = ev.teams if o == 0 else (ev.teams[1], ev.teams[0])
    game.names[0].add(aligned[0])
    game.names[1].add(aligned[1])
    return aligned


# --------------------------------------------------------------------------- #
# Stage 1: Kalshi
# --------------------------------------------------------------------------- #
_KX_EXCLUDE = re.compile(r"\d[HQP]|HALF|QTR|QUARTER|PERIOD|INNING|F5|TEAM|PLAYER|PROP", re.I)


def kalshi_series_type(ticker: str) -> Optional[str]:
    t = ticker.upper()
    if _KX_EXCLUDE.search(t.replace("KX", "", 1)):
        return None
    if t.endswith("SPREAD"):
        return "spread"
    if t.endswith(("TOTAL", "TOTALS")):
        return "total"
    if t.endswith(("GAME", "MATCH")):
        return "moneyline"
    return None


_WIN_BY = re.compile(r"wins?\s+by\s+(?:over|more\s+than)\s+(\d+(?:\.\d+)?)", re.I)
_SIGNED = re.compile(r"(?<![\w.])([+-]\d+(?:\.\d+)?)(?![\w.])")
_OU = re.compile(r"\b(over|under)\b", re.I)
_NUM = re.compile(r"(\d+(?:\.\d+)?)")
_DEAD = {"closed", "settled", "determined", "finalized", "disputed", "amended", "inactive"}


def parse_kalshi_market(m: dict, mtype: str, res: Resolver) -> Optional[tuple]:
    tick = m.get("ticker")
    ysub = (m.get("yes_sub_title") or "").strip()
    text = ysub or (m.get("subtitle") or "").strip()
    if not tick or not text:
        return None
    close = m.get("close_time")
    if mtype == "moneyline":
        if DRAW_RE.search(text):
            return "moneyline", mk_leg("draw", tick, tick, ysub, close_time=close)
        i = res.resolve(text)
        if i is None:
            note_unresolved("kalshi", "moneyline team", text)
            return None
        return "moneyline", mk_leg(f"team{i}", tick, tick, ysub, close_time=close)
    if mtype == "spread":
        i = res.resolve(text)
        if i is None:
            note_unresolved("kalshi", "spread team", text)
            return None
        mw, ms = _WIN_BY.search(text), _SIGNED.search(text)
        if mw:
            h = -float(mw.group(1))
        elif ms:
            h = float(ms.group(1))
        else:
            fs = _num(m.get("floor_strike"))
            if fs is None:
                note_unresolved("kalshi", "spread line", text)
                return None
            h = -abs(fs)  # assumption: "team covers" markets are the favourite's handicap
        h0 = h if i == 0 else -h
        return ident_spread(h0), mk_leg(f"team{i}", tick, tick, ysub, close_time=close)
    if mtype == "total":
        mo = _OU.search(text)
        mn = _NUM.search(text)
        line = float(mn.group(1)) if mn else _num(m.get("floor_strike"))
        if not mo or line is None:
            note_unresolved("kalshi", "total", text)
            return None
        return ident_total(line), mk_leg(mo.group(1).lower(), tick, tick, ysub, close_time=close)
    return None


def stage1_kalshi(s, today: date) -> list:
    sports = get_json(s, f"{KALSHI_BASE}/series", params={"category": "Sports"}).get("series") or []
    wanted = [(x["ticker"], kalshi_series_type(x["ticker"])) for x in sports]
    wanted = [(t, ty) for t, ty in wanted if ty]
    log.info("kalshi: %d sports series, %d are moneyline/spread/total game series", len(sports), len(wanted))

    groups: dict = {}  # (league prefix, ticker suffix) -> [(mtype, event)]
    for ticker, mtype in wanted:
        prefix = re.sub(r"(SPREAD|TOTALS?|GAME|MATCH)$", "", ticker)
        cursor = None
        while True:
            params = {"series_ticker": ticker, "status": "open", "limit": 200, "with_nested_markets": "true"}
            if cursor:
                params["cursor"] = cursor
            data = get_json(s, f"{KALSHI_BASE}/events", params=params)
            for ev in data.get("events") or []:
                et = ev.get("event_ticker", "")
                if kalshi_ticker_date(et) != today:
                    continue
                if HALF_RE.search(f"{ev.get('title', '')} {ev.get('sub_title', '')}"):
                    continue
                suffix = et.split("-", 1)[1] if "-" in et else et
                groups.setdefault((prefix, suffix), []).append((mtype, ev))
            cursor = data.get("cursor")
            if not cursor:
                break
        time.sleep(0.05)

    games = []
    for (prefix, _), items in groups.items():
        items.sort(key=lambda x: x[0] != "moneyline")  # prefer the winner event's title for team names
        teams = next((t for _, ev in items if (t := parse_matchup(ev.get("title", "")))), None)
        if not teams:
            log.debug("kalshi: couldn't parse teams for %s", items[0][1].get("event_ticker"))
            continue
        et0 = items[0][1]["event_ticker"]
        g = Game(teams=teams, day=today, league=prefix, names=({teams[0]}, {teams[1]}))
        g.events["kalshi"] = {
            "event_tickers": [ev["event_ticker"] for _, ev in items],
            "title": items[0][1].get("title", ""),
            "url": f"https://kalshi.com/markets/{items[0][1].get('series_ticker', '').lower()}/{et0.lower()}",
        }
        for mtype, ev in items:
            for m in ev.get("markets") or []:
                if str(m.get("status", "")).lower() in _DEAD:
                    continue
                parsed = parse_kalshi_market(m, mtype, g.resolver)
                if parsed:
                    add_leg(g, "kalshi", *parsed)
        if g.legs["kalshi"]:
            games.append(g)
    log.info("stage 1 (kalshi): %d games today, %d market identities",
             len(games), sum(len(g.legs["kalshi"]) for g in games))
    return games


# --------------------------------------------------------------------------- #
# Stage 2: Polymarket
# --------------------------------------------------------------------------- #
def _poly_events(s, today: date, tz: ZoneInfo, windowed: bool) -> list:
    sports = get_json(s, f"{POLY_GAMMA}/sports")
    tag_ids = sorted({t.strip() for x in sports for t in str(x.get("tags") or "").split(",") if t.strip()})
    start = datetime.combine(today, datetime.min.time(), tz).astimezone(timezone.utc)
    window = {
        "end_date_min": (start - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_date_max": (start + timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    seen: dict = {}
    for tag in tag_ids:
        offset = 0
        for _ in range(100):
            params = {"tag_id": tag, "active": "true", "closed": "false", "limit": 100, "offset": offset}
            if windowed:
                params.update(window)
            batch = get_json(s, f"{POLY_GAMMA}/events", params=params)
            if not batch:
                break
            for ev in batch:
                seen.setdefault(str(ev.get("id")), ev)
            if len(batch) < 100:
                break
            offset += 100
        time.sleep(0.05)

    out = []
    for ev in seen.values():
        teams = parse_matchup(ev.get("title", ""))
        if not teams:
            continue
        slug = ev.get("slug") or str(ev.get("id"))
        day = None
        for c in [ev.get("startTime"), ev.get("gameStartTime")] + [m.get("gameStartTime") for m in ev.get("markets") or []]:
            day = to_day(c, tz)
            if day:
                break
        if not day:  # fall back to the date embedded in the slug, e.g. nfl-det-gb-2026-11-27
            for d in reversed(re.findall(r"\d{4}-\d{2}-\d{2}", slug)):
                try:
                    day = date.fromisoformat(d)
                    break
                except ValueError:
                    pass
        if day != today:
            continue
        out.append(Ev("polymarket", slug, ev.get("title", ""), teams, day, url=f"https://polymarket.com/event/{slug}", raw=ev))
    return out


def parse_poly_market(m: dict, res: Resolver) -> list:
    """-> [(identity, leg), ...]. Only full-game moneyline / spreads / totals."""
    mt = str(m.get("sportsMarketType") or "").lower()
    if m.get("closed") or mt not in {"moneyline", "spreads", "spread", "totals", "total"}:
        return []
    outcomes, tokens = _jl(m.get("outcomes")), _jl(m.get("clobTokenIds"))
    if not outcomes or len(outcomes) != len(tokens):
        return []
    q = m.get("question") or ""
    cid = m.get("conditionId") or m.get("id")
    out = []

    if mt == "moneyline":
        for name, tok in zip(outcomes, tokens):
            if str(name).strip().lower() in {"yes", "no"}:  # per-outcome binary markets (soccer)
                if str(name).strip().lower() == "no":
                    continue
                label = q
            else:
                label = str(name)
            if DRAW_RE.search(label):
                out.append(("moneyline", mk_leg("draw", cid, tok, label)))
                continue
            i = res.resolve(label)
            if i is None:
                note_unresolved("polymarket", "moneyline team", label)
                continue
            out.append(("moneyline", mk_leg(f"team{i}", cid, tok, label)))
        return out

    if mt in {"spreads", "spread"}:
        iq = res.resolve(q)
        mp = re.search(r"\(([+-]?\d+(?:\.\d+)?)\)", q)
        hq = float(mp.group(1)) if mp else _num(m.get("line"))
        if iq is None:  # fall back to the first outcome's team
            iq = res.resolve(str(outcomes[0]))
        if iq is None or hq is None:
            note_unresolved("polymarket", "spread team/line", q)
            return []
        for name, tok in zip(outcomes, tokens):
            i = res.resolve(str(name))
            if i is None:
                note_unresolved("polymarket", "spread outcome", str(name))
                continue
            h = hq if i == iq else -hq
            out.append((ident_spread(h if i == 0 else -h), mk_leg(f"team{i}", cid, tok, f"{name} {h:+g}")))
        return out

    # totals
    mo = re.search(r"(?:o/u|over/under|totals?)\D{0,12}(\d+(?:\.\d+)?)", q, re.I)
    line = float(mo.group(1)) if mo else _num(m.get("line"))
    if line is None:
        nums = _NUM.findall(q)
        line = float(nums[-1]) if nums else None
    if line is None:
        note_unresolved("polymarket", "total line", q)
        return []
    for name, tok in zip(outcomes, tokens):
        k = str(name).strip().lower()
        if k in {"over", "under"}:
            out.append((ident_total(line), mk_leg(k, cid, tok, f"{name} {line:g}")))
    return out


def stage2_polymarket(s, games: list, today: date, tz: ZoneInfo) -> list:
    evs = _poly_events(s, today, tz, windowed=True)
    if not evs and games:
        log.warning("polymarket: nothing for today with the end-date window; retrying without it")
        evs = _poly_events(s, today, tz, windowed=False)
    log.info("polymarket: %d game events dated today", len(evs))

    kept = []
    for game, grp in match_games(games, evs):
        for e in grp:
            absorb_names(game, e)
        res = game.resolver
        for e in grp:
            for m in e.raw.get("markets") or []:
                for ident, leg in parse_poly_market(m, res):
                    add_leg(game, "polymarket", ident, leg)
        game.events["polymarket"] = {"slugs": [e.id for e in grp], "url": grp[0].url, "title": grp[0].title}
        if trim_to_common(game, ["kalshi", "polymarket"]):
            kept.append(game)
    log.info("stage 2 (kalshi+polymarket): %d of %d games kept, %d market identities",
             len(kept), len(games), sum(len(g.legs["kalshi"]) for g in kept))
    return kept


# --------------------------------------------------------------------------- #
# Stage 3: ProphetX
# --------------------------------------------------------------------------- #
class ProphetX:
    def __init__(self, s):
        self.s = s
        self.base = PX_BASES[os.environ.get("PROPHETX_ENV", "production").lower()]
        key = os.environ.get("PROPHETX_AFFILIATE_KEY")
        access, secret = os.environ.get("PROPHETX_ACCESS_KEY"), os.environ.get("PROPHETX_SECRET_KEY")
        if key:  # Market Data API: raw key, no "Bearer"
            self.headers = {"Authorization": key, "Accept": "application/json"}
            self.mode, self.pfx = "affiliate", "/affiliate"
            self.markets_path = "/v4/affiliate/get_markets"
        elif access and secret:  # Trading API
            r = s.post(f"{self.base}/auth/login", json={"access_key": access, "secret_key": secret}, timeout=30)
            r.raise_for_status()
            self.headers = {"Authorization": f"Bearer {r.json()['data']['access_token']}"}
            self.mode, self.pfx = "trading", "/mm"
            self.markets_path = "/v4/mm/get_markets"
        else:
            raise RuntimeError("ProphetX: set PROPHETX_AFFILIATE_KEY (Market Data API) or "
                               "PROPHETX_ACCESS_KEY + PROPHETX_SECRET_KEY (Trading API)")
        log.info("prophetx: using %s API at %s", self.mode, self.base)

    def get(self, path: str, **params):
        r = self.s.get(f"{self.base}{path}", headers=self.headers, params=params, timeout=30)
        if r.status_code == 404:  # data_not_found: nothing listed (e.g. off-season league)
            return None
        r.raise_for_status()
        return r.json().get("data") or {}

    def events_today(self, today: date, tz: ZoneInfo) -> list:
        params = {"has_active_events": "true"} if self.mode == "affiliate" else {}
        tournaments = (self.get(f"{self.pfx}/get_tournaments", **params) or {}).get("tournaments") or []
        out = []
        for t in tournaments:
            data = self.get(f"{self.pfx}/get_sport_events", tournament_id=t["id"])
            time.sleep(0.05)
            for e in (data or {}).get("sport_events") or []:
                day = to_day(e.get("scheduled"), tz)
                if day != today:
                    continue
                names = [(c.get("display_name") or c.get("name") or "").strip() for c in e.get("competitors") or []]
                teams = tuple(names) if len(names) == 2 and all(names) else parse_matchup(e.get("name", ""))
                if teams:
                    out.append(Ev("prophetx", str(e.get("event_id")), e.get("name", ""), teams, day,
                                  league=t.get("name", ""), raw=e))
        return out

    def markets(self, event_id: str):
        params = {"event_id": event_id}
        if self.mode == "affiliate":
            params["market_types"] = "moneyline,spread,total"
        return self.get(self.markets_path, **params)


def _flatten(x):
    if isinstance(x, dict):
        yield x
    elif isinstance(x, list):
        for y in x:
            yield from _flatten(y)


_PX_NAME_KEYS = ("name", "display_name", "outcome_name", "label", "competitor_name", "team_name", "side")
_PX_DEAD = {"closed", "settled", "suspended", "inactive", "cancelled", "canceled"}


def parse_px_market(m: dict, res: Resolver) -> list:
    mtype = str(m.get("type") or m.get("market_type") or "").lower()
    if mtype not in {"moneyline", "spread", "total"}:
        return []
    if str(m.get("status", "")).lower() in _PX_DEAD:
        return []
    mid = m.get("id") or m.get("market_id")
    out = []
    for sel in _flatten(m.get("selections") or []):
        text = next((str(sel[k]) for k in _PX_NAME_KEYS if sel.get(k)), "")
        sid = sel.get("strike_id") or sel.get("id")
        if not text or sid is None:
            note_unresolved("prophetx", f"{mtype} selection", json.dumps(sel)[:200])
            continue
        if mtype == "moneyline":
            if DRAW_RE.search(text):
                out.append(("moneyline", mk_leg("draw", mid, sid, text)))
                continue
            i = res.resolve(text)
            if i is None:
                note_unresolved("prophetx", "moneyline team", text)
                continue
            out.append(("moneyline", mk_leg(f"team{i}", mid, sid, text)))
        elif mtype == "spread":
            i = res.resolve(text)
            h = _num(sel.get("strike"))  # each side carries its own signed handicap
            if i is None or h is None:
                note_unresolved("prophetx", "spread team/line", f"{text} strike={sel.get('strike')}")
                continue
            out.append((ident_spread(h if i == 0 else -h), mk_leg(f"team{i}", mid, sid, f"{text} {h:+g}")))
        else:
            k = "over" if re.search(r"\bover\b", text, re.I) else "under" if re.search(r"\bunder\b", text, re.I) else None
            line = _num(m.get("strike"))
            if line is None:
                line = _num(sel.get("strike"))
            if k is None or line is None:
                note_unresolved("prophetx", "total", f"{text} strike={m.get('strike')}")
                continue
            out.append((ident_total(abs(line)), mk_leg(k, mid, sid, f"{text} {abs(line):g}")))
    return out


def stage3_prophetx(s, games: list, today: date, tz: ZoneInfo, debug_dir: Optional[Path]) -> list:
    px = ProphetX(s)
    evs = px.events_today(today, tz)
    log.info("prophetx: %d events dated today", len(evs))

    kept, dumped = [], False
    for game, grp in match_games(games, evs):
        ev = grp[0]
        absorb_names(game, ev)
        data = px.markets(ev.id)
        time.sleep(0.05)
        if debug_dir and not dumped and data:
            (debug_dir / "prophetx_sample_markets.json").write_text(json.dumps(data, indent=1)[:200_000])
            dumped = True
        res = game.resolver
        for m in (data or {}).get("markets") or []:
            for ident, leg in parse_px_market(m, res):
                add_leg(game, "prophetx", ident, leg)
        game.events["prophetx"] = {"event_id": ev.id, "title": ev.title}
        if trim_to_common(game, ["kalshi", "polymarket", "prophetx"]):
            kept.append(game)
    log.info("stage 3 (all three): %d of %d games kept, %d market identities",
             len(kept), len(games), sum(len(g.legs["kalshi"]) for g in kept))
    return kept


# --------------------------------------------------------------------------- #
# Output
# --------------------------------------------------------------------------- #
def _slug(s: str) -> str:
    return "-".join(norm(s)) or "unknown"


def games_to_dict(games: list) -> dict:
    """
    {"2026-11-24|lions-vs-packers": {
        "matchup", "day", "league", "teams": [team0, team1],
        "events":  {"kalshi": {...tickers}, "polymarket": {...}, "prophetx": {...}},
        "markets": {"moneyline": {"type", "line", "kalshi": [leg...], "polymarket": [...], "prophetx": [...]},
                    "spread|-3.5": {...}, "total|45.5": {...}}}}
    team0/team1 follow the order of "teams". A spread's "line" is team0's handicap.
    Each leg: {"selection": team0|team1|draw|over|under, "id": tradable id, "market_id", "label", "extra"}.
    Venues with no legs for a market (e.g. prophetx before stage 3 has run) get an empty list.
    """
    out: dict = {}
    for g in sorted(games, key=lambda g: (g.day, g.teams)):
        base = f"{g.day.isoformat()}|{'-vs-'.join(sorted(_slug(t) for t in g.teams))}"
        key, n = base, 1
        while key in out:
            n += 1
            key = f"{base}#{n}"
        markets = {}
        for ident in sorted(g.legs["kalshi"]):
            kind, _, val = ident.partition("|")
            markets[ident] = {
                "type": kind,
                "line": float(val) if val else None,
                **{v: g.legs[v].get(ident, []) for v in ("kalshi", "polymarket", "prophetx")},
            }
        out[key] = {
            "matchup": " vs ".join(g.teams),
            "day": g.day.isoformat(),
            "league": g.league,
            "teams": list(g.teams),
            "events": g.events,
            "markets": markets,
        }
    return out


CSV_FIELDS = ["day", "matchup", "league", "market", "type", "line", "selection", "selection_name",
              "kalshi_id", "kalshi_label", "polymarket_id", "polymarket_market_id", "polymarket_label",
              "prophetx_id", "prophetx_market_id", "prophetx_label"]
_SEL_ORDER = ["team0", "team1", "draw", "over", "under"]


def write_csv(result: dict, path) -> int:
    """One row per (game, market, selection) with each venue's id side by side. Returns row count."""
    rows = []
    for g in result.values():
        for ident, m in g["markets"].items():
            sels = sorted({l["selection"] for v in ("kalshi", "polymarket", "prophetx") for l in m[v]},
                          key=_SEL_ORDER.index)
            for sel in sels:
                row = {
                    "day": g["day"], "matchup": g["matchup"], "league": g["league"], "market": ident,
                    "type": m["type"], "line": "" if m["line"] is None else m["line"], "selection": sel,
                    "selection_name": g["teams"][int(sel[4:])] if sel.startswith("team") else sel.title(),
                }
                for v in ("kalshi", "polymarket", "prophetx"):
                    legs = [l for l in m[v] if l["selection"] == sel]
                    row[f"{v}_id"] = " | ".join(l["id"] for l in legs)
                    row[f"{v}_market_id"] = " | ".join(dict.fromkeys(l["market_id"] for l in legs))
                    row[f"{v}_label"] = " | ".join(l["label"] for l in legs)
                rows.append({k: row.get(k, "") for k in CSV_FIELDS})
    with open(path, "w", newline="", encoding="utf-8-sig") as f:  # utf-8-sig so Excel opens it cleanly
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        w.writerows(rows)
    return len(rows)


# --------------------------------------------------------------------------- #
# Caching (stage 1 and stage 2 results)
# --------------------------------------------------------------------------- #
def _cache_path(out_dir, stage: int, today: date, tz: str) -> Path:
    return Path(out_dir) / "cache" / f"stage{stage}_{today.isoformat()}_{tz.replace('/', '-')}.pkl"


def load_cache(path: Path, max_age_min: float):
    if not path.is_file():
        return None
    age = (time.time() - path.stat().st_mtime) / 60
    if age > max_age_min:
        log.info("cache %s is %.0f min old (limit %.0f), ignoring", path.name, age, max_age_min)
        return None
    try:
        with open(path, "rb") as f:
            games = pickle.load(f)
    except Exception as e:  # corrupt file, or the Game class changed since it was saved
        log.warning("couldn't read cache %s (%s), ignoring", path.name, e)
        return None
    log.info("loaded %d games from %s (%.0f min old)", len(games), path.name, age)
    return games


def save_cache(path: Path, games: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "wb") as f:
        pickle.dump(games, f)
    tmp.replace(path)  # atomic, so a crash mid-write can't leave a half-written cache


def discover(
    day: Optional[date] = None,
    tz: str = DEFAULT_TZ,
    out_dir: Optional[str] = "out",
    session=None,
    use_cache: bool = True,
    max_age_min: float = 120,
) -> dict:
    """Run the three stages for `day` (default: today in `tz`) and return the match dict."""
    zone = ZoneInfo(tz)
    today = day or datetime.now(zone).date()
    s = session or make_session()
    debug_dir = None
    if out_dir:
        debug_dir = Path(out_dir) / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
    UNRESOLVED.clear()
    log.info("running for %s (%s)", today, tz)

    games, done = None, 0
    if out_dir and use_cache:
        for n in (2, 1):  # prefer the furthest-along cache
            games = load_cache(_cache_path(out_dir, n, today, tz), max_age_min)
            if games is not None:
                done = n
                break

    if done < 1:
        games = stage1_kalshi(s, today)
        if out_dir and games:
            save_cache(_cache_path(out_dir, 1, today, tz), games)
    if done < 2 and games:
        games = stage2_polymarket(s, games, today, zone)
        if out_dir and games:
            save_cache(_cache_path(out_dir, 2, today, tz), games)
            n_rows = write_csv(games_to_dict(games), Path(out_dir) / "markets_kalshi_polymarket.csv")
            log.info("wrote interim markets_kalshi_polymarket.csv (%d rows)", n_rows)
    if games:
        games = stage3_prophetx(s, games, today, zone, debug_dir)
    result = games_to_dict(games or [])

    if out_dir:
        (Path(out_dir) / "markets.json").write_text(json.dumps(result, indent=1))
        write_csv(result, Path(out_dir) / "markets.csv")
        (debug_dir / "unresolved.json").write_text(json.dumps(UNRESOLVED, indent=1))
    if UNRESOLVED:
        log.warning("%d legs couldn't be interpreted (see out/debug/unresolved.json)", len(UNRESOLVED))
    return result


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date", help="YYYY-MM-DD (default: today in --tz)")
    ap.add_argument("--tz", default=DEFAULT_TZ)
    ap.add_argument("--out-dir", default="out")
    ap.add_argument("--refresh", action="store_true", help="ignore cached results and re-pull everything")
    ap.add_argument("--max-age", type=float, default=120, help="max cache age in minutes (default 120)")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    try:
        res = discover(date.fromisoformat(args.date) if args.date else None, args.tz, args.out_dir,
                       use_cache=not args.refresh, max_age_min=args.max_age)
    except (RuntimeError, requests.RequestException) as e:
        log.error("%s", e)
        return 1
    print(f"\n{len(res)} games on all three venues")
    for key, g in res.items():
        print(f"  {g['matchup']:<45} {len(g['markets'])} markets: {', '.join(g['markets'])}")
    print(f"\nWrote {Path(args.out_dir) / 'markets.csv'} (spreadsheet view) and markets.json (full detail)")
    return 0


if __name__ == "__main__":
    sys.exit(main())