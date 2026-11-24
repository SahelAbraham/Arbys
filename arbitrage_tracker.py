#!/usr/bin/env python3
"""Live moneyline price tracker and cross-venue arbitrage scanner.

Consumes the JSON written by market_pipeline.py. It does not place orders.
Hybrid feed design:
  * REST snapshots for Kalshi and ProphetX (ProphetX affiliate API is REST here).
  * Polymarket REST snapshots plus its public CLOB market WebSocket for updates.
  * REST refreshes remain the recovery path if a WebSocket disconnects.

Prices are normalized to cost per $1 of eventual payout, so Kalshi contracts,
Polymarket shares, and ProphetX USD-stake odds can be compared consistently.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import math
import os
import sqlite3
import sys
import time
import threading
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Explicitly load the existing secrets file.
from pathlib import Path

secrets_file = Path(__file__).resolve().parent / "secrets.txt"
if secrets_file.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(secrets_file, override=False)
    except ImportError:
        for line in secrets_file.read_text(encoding="utf-8-sig").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip().strip("'\""))

LOG = logging.getLogger("arb_tracker")
VENUES = ("kalshi", "polymarket", "prophetx")
KALSHI_BASE = os.getenv("KALSHI_API_BASE", "https://api.elections.kalshi.com/trade-api/v2")
POLY_CLOB = "https://clob.polymarket.com"
POLY_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PX_BASES = {
    "production": "https://cash.api.prophetx.co/partner",
    "sandbox": "https://api.sandbox.prophetx.dev/partner",
}


def now_ts() -> float:
    return time.time()


def as_float(value: Any) -> float | None:
    try:
        x = float(value)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def american_to_decimal(odds: float) -> float | None:
    if odds >= 100:
        return 1.0 + odds / 100.0
    if odds <= -100:
        return 1.0 + 100.0 / abs(odds)
    return None


def norm_team(s: str) -> str:
    import re, unicodedata
    s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode().lower()
    s = re.sub(r"\([^)]*\)", " ", s)
    s = re.sub(r"\b(fc|cf|sc|afc|club|bulldogs|lions|tigers|eagles|falcons|wildcats|rangers)\b", " ", s)
    return " ".join(re.findall(r"[a-z0-9]+", s))


def resolve_team(text: str, teams: list[str]) -> str | None:
    """Conservative team-name resolver for ProphetX market labels."""
    n = norm_team(text)
    if not n:
        return None
    exact = [f"team{i}" for i, t in enumerate(teams) if norm_team(t) == n]
    if len(exact) == 1:
        return exact[0]
    matches = []
    for i, team in enumerate(teams):
        tn = norm_team(team)
        if tn and (tn in n or n in tn):
            matches.append((len(tn), f"team{i}"))
    if matches:
        matches.sort(reverse=True)
        if len(matches) == 1 or matches[0][0] > matches[1][0]:
            return matches[0][1]
    return None


def dedupe_legs(legs: list[dict]) -> list[dict]:
    """Collapse repeated ProphetX instrument IDs without losing separate price levels.

    The discovery JSON may contain many copies of the same (market_id, strike_id)
    because parse_px_market flattened a liquidity ladder. The live ProphetX request
    below is the source of price levels; these legs are only instrument references.
    """
    out, seen = [], set()
    for leg in legs or []:
        key = (str(leg.get("market_id", "")), str(leg.get("id", "")), str(leg.get("selection", "")))
        if key not in seen and key[1]:
            seen.add(key)
            out.append(leg)
    return out


@dataclass
class Level:
    venue: str
    selection: str
    instrument_id: str
    market_id: str
    cost_per_payout: float
    payout_capacity: float
    raw_price: float
    raw_quantity: float
    timestamp: float
    label: str = ""

    @property
    def effective_cost(self) -> float:
        return self.cost_per_payout


class Store:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        # run_once() executes in asyncio.to_thread(), so this connection is
        # intentionally shared across the main thread and the worker thread.
        # All access is serialized by _lock below.
        self._lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False, timeout=30.0)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS price_levels (
          ts REAL NOT NULL, game_key TEXT NOT NULL, market_key TEXT NOT NULL,
          selection TEXT NOT NULL, venue TEXT NOT NULL, instrument_id TEXT NOT NULL,
          market_id TEXT, cost_per_payout REAL NOT NULL, payout_capacity REAL NOT NULL,
          raw_price REAL, raw_quantity REAL, label TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_price_levels_game_ts ON price_levels(game_key, ts);
        CREATE TABLE IF NOT EXISTS opportunities (
          ts REAL NOT NULL, game_key TEXT NOT NULL, market_key TEXT NOT NULL,
          outcomes TEXT NOT NULL, payout_size REAL NOT NULL, total_cost REAL NOT NULL,
          gross_profit REAL NOT NULL, gross_edge REAL NOT NULL, legs_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_opps_ts ON opportunities(ts);
        """)
        self.db.commit()

    def save_snapshot(self, rows: list[dict], opportunities: list[dict]):
        # Keep the transaction atomic and prevent concurrent access to the
        # shared SQLite connection. The tracker has one polling worker today;
        # the lock also protects future callers and orderly shutdown.
        with self._lock:
            with self.db:
                if rows:
                    self.db.executemany("""INSERT INTO price_levels VALUES
                      (:ts,:game_key,:market_key,:selection,:venue,:instrument_id,:market_id,
                       :cost_per_payout,:payout_capacity,:raw_price,:raw_quantity,:label)""", rows)
                if opportunities:
                    self.db.executemany("""INSERT INTO opportunities VALUES
                      (:ts,:game_key,:market_key,:outcomes,:payout_size,:total_cost,
                       :gross_profit,:gross_edge,:legs_json)""", opportunities)

    def close(self):
        with self._lock:
            self.db.close()


class Api:
    def __init__(self, timeout: float = 8.0):
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": "moneyline-arbitrage-tracker/1.0", "Accept": "application/json"})
        self.timeout = timeout
        self.px_key = os.getenv("PROPHETX_AFFILIATE_KEY")
        self.px_access = os.getenv("PROPHETX_ACCESS_KEY")
        self.px_secret = os.getenv("PROPHETX_SECRET_KEY")
        env = os.getenv("PROPHETX_ENV", "production").lower()
        self.px_base = PX_BASES.get(env, PX_BASES["production"])
        self.px_cache: dict[str, tuple[float, list[dict]]] = {}
        self.px_mode = None
        self.px_headers = {}
        self.px_auth_time = 0.0
        if self.px_key:
            self.px_mode = "affiliate"
            self.px_headers = {"Authorization": self.px_key, "Accept": "application/json"}
        elif self.px_access and self.px_secret:
            self.px_mode = "trading"
            self._px_login()

    def _px_login(self):
        response = self.s.post(f"{self.px_base}/auth/login",
                               json={"access_key": self.px_access, "secret_key": self.px_secret},
                               timeout=self.timeout)
        response.raise_for_status()
        token = response.json()["data"]["access_token"]
        self.px_headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        self.px_auth_time = now_ts()

    def get(self, url: str, **kwargs):
        r = self.s.get(url, timeout=self.timeout, **kwargs)
        r.raise_for_status()
        return r.json()

    def kalshi_books(self, tickers: list[str]) -> dict[str, dict]:
        out = {}

        if not tickers:
            return out

        for i in range(0, len(tickers), 100):
            chunk = tickers[i:i + 100]

            # Kalshi expects repeated tickers parameters
            params = [("tickers", ticker) for ticker in chunk]

            data = self.get(
                f"{KALSHI_BASE}/markets/orderbooks",
                params=params
            )

            books = data.get("orderbooks") or []

            for book in books:
                ticker = book.get("ticker")
                if ticker:
                    out[ticker] = book

        return out

    def poly_books(self, token_ids: list[str]) -> dict[str, dict]:
        out = {}
        if not token_ids:
            return out
        for i in range(0, len(token_ids), 500):
            chunk = token_ids[i:i + 500]
            r = self.s.post(f"{POLY_CLOB}/books", json=[{"token_id": x} for x in chunk], timeout=self.timeout)
            r.raise_for_status()
            body = r.json()
            if isinstance(body, list):
                for book in body:
                    token = str(book.get("asset_id") or book.get("token_id") or "")
                    if token:
                        out[token] = book
        return out

    def prophetx_markets(self, event_id: str, refresh_seconds: float = 8.0) -> list[dict]:
        if not self.px_mode:
            return []
        cached = self.px_cache.get(str(event_id))
        if cached and now_ts() - cached[0] < refresh_seconds:
            return cached[1]
        # The Trading API access token is short-lived (~20 minutes). Re-login
        # only after the prior token's expected lifetime, not on each request.
        if self.px_mode == "trading" and now_ts() - self.px_auth_time > 20 * 60 + 5:
            self._px_login()
        path = "/v4/affiliate/get_markets" if self.px_mode == "affiliate" else "/v4/mm/get_markets"
        params = {"event_id": event_id}
        if self.px_mode == "affiliate":
            params["market_types"] = "moneyline"
        r = self.s.get(f"{self.px_base}{path}", headers=self.px_headers,
                       params=params, timeout=self.timeout)
        r.raise_for_status()
        data = r.json().get("data") or {}
        markets = data.get("markets") or []
        self.px_cache[str(event_id)] = (now_ts(), markets)
        return markets


# ---------- orderbook normalization ----------

def _book_levels(raw: Any) -> list[tuple[float, float]]:
    """Convert common API price-level shapes to (price, size)."""
    out = []
    for item in raw or []:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            p, q = as_float(item[0]), as_float(item[1])
        elif isinstance(item, dict):
            p = as_float(item.get("price") or item.get("p"))
            q = as_float(item.get("quantity") or item.get("size") or item.get("count") or item.get("q"))
        else:
            continue
        if p is not None and q is not None and q > 0:
            out.append((p, q))
    return out


def kalshi_levels(ticker: str, book: dict, selection: str, fee_bps: float) -> list[Level]:
    ob = book.get("orderbook") if isinstance(book.get("orderbook"), dict) else book
    if "yes_dollars" in ob or "no_dollars" in ob:
        yes = _book_levels(ob.get("yes_dollars") or [])
        no = _book_levels(ob.get("no_dollars") or [])
    else:
        # Legacy orderbook fields are integer cents, not dollar probabilities.
        yes = [(p / 100.0 if p > 1 else p, q) for p, q in _book_levels(ob.get("yes") or [])]
        no = [(p / 100.0 if p > 1 else p, q) for p, q in _book_levels(ob.get("no") or [])]
    # Kalshi exposes bids only. A YES ask is 1 - best NO bid; all NO bids form
    # the executable YES ask ladder. The inverse is true for a NO ask.
    asks = [(1.0 - p, q) for p, q in no]
    # Each pipeline ticker's YES contract represents its named selection.
    levels = []
    for ask, qty in asks:
        if 0 < ask < 1:
            cost = ask * (1.0 + fee_bps / 10000.0)
            levels.append(Level("kalshi", selection, ticker, ticker, cost, qty, ask, qty, now_ts(), ticker))
    return sorted(levels, key=lambda x: x.cost_per_payout)


def poly_levels(token: str, book: dict, selection: str, fee_bps: float) -> list[Level]:
    levels = []
    for price, size in _book_levels(book.get("asks") or []):
        if 0 < price < 1:
            cost = price * (1.0 + fee_bps / 10000.0)
            levels.append(Level("polymarket", selection, token, str(book.get("market", "")), cost, size,
                                price, size, now_ts(), token))
    return sorted(levels, key=lambda x: x.cost_per_payout)


def _flatten_dicts(value: Any) -> Iterable[dict]:
    if isinstance(value, dict):
        yield value
    elif isinstance(value, list):
        for child in value:
            yield from _flatten_dicts(child)


def prophetx_levels(markets: list[dict], teams: list[str], expected: set[str], fee_bps: float) -> tuple[dict[str, list[Level]], list[dict]]:
    """Parse affiliate market ladders. Keep every unique (strike_id, price) level.

    ProphetX's `quantity` is USD stake, not contracts. For American odds, a $1
    stake returns decimal_odds dollars when successful. Thus payout capacity is
    quantity * decimal_odds and cost per $1 payout is 1 / decimal_odds.
    """
    result: dict[str, list[Level]] = defaultdict(list)
    diagnostics = []
    seen = set()
    for market in markets:
        if str(market.get("type", market.get("market_type", ""))).lower() != "moneyline":
            continue
        status = str(market.get("status", "active")).lower()
        if status in {"closed", "settled", "suspended", "inactive", "cancelled", "canceled"}:
            continue
        subtype = str(market.get("sub_type", "")).lower()
        market_name = str(market.get("name", ""))
        market_id = str(market.get("id") or market.get("market_id") or "")
        # Do not accidentally mix first-half/quarter/set/map moneylines with
        # the full-game market identity from the discovery pipeline.
        scope_text = f"{subtype} {market_name}".lower()
        if any(term in scope_text for term in ("first_half", "1st half", "first half", "second_half",
                                                "2nd half", "second half", "quarter", "period",
                                                "1st set", "first set", "2nd set", "second set",
                                                "map winner", "1st map", "2nd map")):
            continue
        # In a 3-way market ProphetX publishes one binary YES/NO market per
        # result (home win, away win, draw). YES means that named result occurs.
        context_selection = None
        low_name = market_name.lower()
        if "draw" in low_name:
            context_selection = "draw"
        else:
            import re
            context = re.sub(r"\b(to win|wins?|winner|\(90 min\)|90 min)\b", " ", market_name, flags=re.I)
            context_selection = resolve_team(context, teams)
        is_yes_no = subtype == "moneyline_3_way" or "(90 min)" in low_name or "to win" in low_name
        for sel in _flatten_dicts(market.get("selections") or []):
            strike_id = str(sel.get("strike_id") or sel.get("id") or "")
            if not strike_id:
                continue
            text = str(sel.get("name") or sel.get("display_name") or sel.get("label") or "")
            low = text.strip().lower()
            if is_yes_no and low in {"no", "no ", "false"}:
                continue
            if is_yes_no and low in {"yes", "true"}:
                selection = context_selection
            elif "draw" in low:
                selection = "draw"
            else:
                selection = resolve_team(text, teams)
            if selection not in expected:
                if text or market_name:
                    diagnostics.append({"market_id": market_id, "market_name": market_name,
                                        "selection_text": text, "reason": "unmapped_or_unexpected_outcome"})
                continue
            odds = as_float(sel.get("price"))
            qty = as_float(sel.get("quantity"))
            if odds is None or qty is None or qty <= 0:
                continue
            dec = american_to_decimal(odds)
            if not dec or dec <= 1:
                continue
            key = (market_id, strike_id, odds, qty)
            if key in seen:
                continue
            seen.add(key)
            # If affiliate payload includes an explicit taker price, retain it in
            # diagnostics but do not guess its units; sample payload uses `price`.
            cost = (1.0 / dec) * (1.0 + fee_bps / 10000.0)
            result[selection].append(Level("prophetx", selection, strike_id, market_id,
                                           cost, qty * dec, odds, qty, now_ts(), text or market_name))
    for selection in result:
        result[selection].sort(key=lambda x: x.cost_per_payout)
    return result, diagnostics


# ---------- arbitrage calculation ----------

def cost_for_payout(levels: list[Level], payout: float) -> tuple[float, list[dict]] | None:
    remaining = payout
    total_cost = 0.0
    fills = []
    for level in sorted(levels, key=lambda x: x.cost_per_payout):
        take = min(remaining, level.payout_capacity)
        if take <= 0:
            continue
        cost = take * level.cost_per_payout
        total_cost += cost
        fills.append({"venue": level.venue, "instrument_id": level.instrument_id,
                      "market_id": level.market_id, "payout": take, "cost": cost,
                      "cost_per_payout": level.cost_per_payout, "raw_price": level.raw_price,
                      "raw_quantity": level.raw_quantity, "label": level.label})
        remaining -= take
        if remaining <= 1e-8:
            return total_cost, fills
    return None


def evaluate_arb(game_key: str, game: dict, market_key: str, market: dict,
                 books: dict[str, dict[str, list[Level]]], min_edge: float) -> dict | None:
    selections = sorted({str(x) for x in (
        [leg.get("selection") for v in VENUES for leg in (market.get(v) or [])]
    ) if x}, key=lambda s: (s == "draw", s))
    # Require at least two distinct outcomes and a live ask for every outcome.
    if len(selections) < 2 or any(not books.get(sel) for sel in selections):
        return None
    max_payout = min(sum(l.payout_capacity for l in books[sel]) for sel in selections)
    if max_payout <= 0:
        return None
    # Profit per payout can peak before the full depth is consumed. Test every
    # cumulative depth breakpoint from every outcome, plus a tiny minimum size.
    candidates = {max_payout}
    for sel in selections:
        acc = 0.0
        for level in sorted(books[sel], key=lambda x: x.cost_per_payout):
            acc += level.payout_capacity
            if 0 < acc <= max_payout:
                candidates.add(acc)
    best = None
    for payout in sorted(candidates):
        if payout <= 0:
            continue
        all_fills, total_cost = {}, 0.0
        valid = True
        for sel in selections:
            got = cost_for_payout(books[sel], payout)
            if got is None:
                valid = False
                break
            cost, fills = got
            total_cost += cost
            all_fills[sel] = fills
        if not valid:
            continue
        profit = payout - total_cost
        edge = profit / payout
        if edge >= min_edge and (best is None or profit > best["gross_profit"]):
            best = {"game_key": game_key, "matchup": game.get("matchup", game_key),
                    "market": market_key, "outcomes": selections, "payout_size": payout,
                    "total_cost": total_cost, "gross_profit": profit, "gross_edge": edge,
                    "legs": all_fills}
    return best


class PolyWsCache:
    """Best-effort Polymarket market-channel cache; REST remains authoritative fallback."""
    def __init__(self, token_ids: list[str]):
        self.token_ids = token_ids
        self.books: dict[str, dict] = {}
        self.last_update: dict[str, float] = {}
        self.connected = False

    async def run(self):
        try:
            import websockets
        except ImportError:
            LOG.warning("websockets package missing; Polymarket will use REST polling only")
            return
        while True:
            try:
                async with websockets.connect(POLY_WS, ping_interval=20, ping_timeout=20,
                                              close_timeout=5, max_size=8_000_000) as ws:
                    self.connected = True
                    for i in range(0, len(self.token_ids), 500):
                        await ws.send(json.dumps({"assets_ids": self.token_ids[i:i+500], "type": "market"}))
                        if i:
                            await asyncio.sleep(0.1)
                    LOG.info("Polymarket WS subscribed to %d tokens", len(self.token_ids))
                    async for raw in ws:
                        try:
                            payload = json.loads(raw)
                            events = payload if isinstance(payload, list) else [payload]
                            for event in events:
                                etype = event.get("event_type") or event.get("type")
                                if etype == "book":
                                    token = str(event.get("asset_id") or "")
                                    if token:
                                        self.books[token] = {"asks": event.get("asks") or [],
                                                             "bids": event.get("bids") or [],
                                                             "market": event.get("market")}
                                        self.last_update[token] = now_ts()
                                elif etype == "price_change":
                                    for change in event.get("price_changes") or []:
                                        token = str(change.get("asset_id") or "")
                                        if not token:
                                            continue
                                        book = self.books.setdefault(token, {"asks": [], "bids": []})
                                        side = str(change.get("side", "")).upper()
                                        key = "asks" if side == "SELL" else "bids"
                                        p, size = as_float(change.get("price")), as_float(change.get("size"))
                                        if p is None or size is None:
                                            continue
                                        levels = {float(x.get("price")): float(x.get("size", 0))
                                                  for x in book.get(key, []) if as_float(x.get("price")) is not None}
                                        if size <= 0:
                                            levels.pop(p, None)
                                        else:
                                            levels[p] = size
                                        book[key] = [{"price": str(k), "size": str(v)} for k, v in levels.items()]
                                        self.last_update[token] = now_ts()
                        except Exception:
                            LOG.debug("Ignoring malformed Polymarket WS message", exc_info=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.connected = False
                LOG.warning("Polymarket WS disconnected (%s); retrying in 2s", exc)
                await asyncio.sleep(2)


class Tracker:
    def __init__(self, markets_path: Path, out_dir: Path, interval: float, min_edge: float,
                 kalshi_fee_bps: float, poly_fee_bps: float, px_fee_bps: float, timeout: float,
                 px_refresh: float, pipeline_debug: Path | None):
        self.markets_path, self.out_dir = markets_path, out_dir
        self.interval, self.min_edge = interval, min_edge
        self.px_refresh, self.pipeline_debug = px_refresh, pipeline_debug
        self.fees = {"kalshi": kalshi_fee_bps, "polymarket": poly_fee_bps, "prophetx": px_fee_bps}
        self.api = Api(timeout)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.store = Store(out_dir / "price_history.sqlite")
        self.diagnostics: dict[str, Any] = {}

    def load_markets(self) -> dict:
        data = json.loads(self.markets_path.read_text(encoding="utf-8-sig"))
        games = {}
        for key, game in data.items():
            markets = {}
            for mkey, market in (game.get("markets") or {}).items():
                if str(market.get("type", "")).lower() != "moneyline":
                    continue
                clean = dict(market)
                for venue in VENUES:
                    clean[venue] = dedupe_legs(market.get(venue) or [])
                if clean["kalshi"] or clean["polymarket"] or clean["prophetx"]:
                    markets[mkey] = clean
            if markets:
                games[key] = {**game, "markets": markets}
        self.diagnostics["games_loaded"] = len(games)
        self.diagnostics["moneyline_markets_loaded"] = sum(len(g["markets"]) for g in games.values())
        # The source pipeline caps unresolved.json at 200 examples. Preserve that
        # context rather than interpreting the sample as a full error count.
        if self.pipeline_debug and self.pipeline_debug.exists():
            try:
                unresolved = json.loads(self.pipeline_debug.read_text(encoding="utf-8-sig"))
                counts = defaultdict(int)
                for item in unresolved if isinstance(unresolved, list) else []:
                    counts[f"{item.get('venue','?')}::{item.get('why','?')}"] += 1
                self.diagnostics["pipeline_unresolved_sample_count"] = len(unresolved) if isinstance(unresolved, list) else 0
                self.diagnostics["pipeline_unresolved_sample_by_reason"] = dict(counts)
                self.diagnostics["pipeline_unresolved_is_capped_sample"] = True
            except Exception as exc:
                self.diagnostics["pipeline_unresolved_read_error"] = str(exc)
        return games

    def collect_ids(self, games: dict):
        kalshi, poly = set(), set()
        for game in games.values():
            for market in game["markets"].values():
                for leg in market["kalshi"]:
                    kalshi.add(str(leg["id"]))
                for leg in market["polymarket"]:
                    poly.add(str(leg["id"]))
        return sorted(kalshi), sorted(poly)

    def build_venue_books(self, games: dict, kalshi_raw: dict, poly_raw: dict,
                          ws_cache: PolyWsCache | None = None) -> dict:
        output: dict[tuple[str, str], dict[str, list[Level]]] = {}
        missing = defaultdict(int)
        px_debug = []
        for gkey, game in games.items():
            for mkey, market in game["markets"].items():
                expected = {leg["selection"] for v in VENUES for leg in market[v] if leg.get("selection")}
                books: dict[str, list[Level]] = defaultdict(list)
                # Kalshi: each ticker is the YES contract for that outcome.
                for leg in market["kalshi"]:
                    ticker = str(leg["id"])
                    raw = kalshi_raw.get(ticker)
                    if not raw:
                        missing["kalshi"] += 1
                        continue
                    books[leg["selection"]].extend(kalshi_levels(ticker, raw, leg["selection"], self.fees["kalshi"]))
                # Polymarket: token IDs are individual outcome tokens.
                for leg in market["polymarket"]:
                    token = str(leg["id"])
                    raw = None
                    if ws_cache and ws_cache.last_update.get(token, 0) > now_ts() - max(10.0, self.interval * 5):
                        raw = ws_cache.books.get(token)
                    raw = raw or poly_raw.get(token)
                    if not raw:
                        missing["polymarket"] += 1
                        continue
                    books[leg["selection"]].extend(poly_levels(token, raw, leg["selection"], self.fees["polymarket"]))
                # ProphetX: refetch the event's markets once per event, not once per leg.
                # Use live market ladders so repeated CSV/JSON IDs don't duplicate liquidity.
                output[(gkey, mkey)] = books
            event_id = ((game.get("events") or {}).get("prophetx") or {}).get("event_id")
            if event_id and self.api.px_mode:
                try:
                    px_markets = self.api.prophetx_markets(str(event_id), self.px_refresh)
                    for mkey, market in game["markets"].items():
                        expected = {leg["selection"] for v in VENUES for leg in market[v] if leg.get("selection")}
                        px, diag = prophetx_levels(px_markets, game.get("teams") or [], expected, self.fees["prophetx"])
                        px_debug.extend(diag[:50])
                        target = output[(gkey, mkey)]
                        for selection, levels in px.items():
                            target[selection].extend(levels)
                except Exception as exc:
                    LOG.warning("ProphetX event %s refresh failed: %s", event_id, exc)
                    missing["prophetx_event_errors"] += 1
            elif not self.api.px_mode:
                missing["prophetx_no_credentials"] += 1
        self.diagnostics["missing_book_counts"] = dict(missing)
        self.diagnostics["prophetx_unmapped_samples"] = px_debug[:100]
        return output

    def write_csvs(self, prices: list[dict], opportunities: list[dict]):
        for filename, rows in (("prices_latest.csv", prices), ("opportunities.csv", opportunities)):
            path = self.out_dir / filename
            if not rows:
                path.write_text("", encoding="utf-8")
                continue
            fields = list(dict.fromkeys(k for row in rows for k in row.keys()))
            tmp = path.with_suffix(path.suffix + ".tmp")
            with tmp.open("w", newline="", encoding="utf-8-sig") as f:
                w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
                w.writeheader()
                w.writerows(rows)
            tmp.replace(path)

    def run_once(self, games: dict, ws_cache: PolyWsCache | None = None):
        tickers, tokens = self.collect_ids(games)
        errors = {}
        try:
            kalshi_raw = self.api.kalshi_books(tickers)
        except Exception as exc:
            LOG.exception("Kalshi orderbook request failed")
            kalshi_raw, errors["kalshi"] = {}, str(exc)
        try:
            poly_raw = self.api.poly_books(tokens)
        except Exception as exc:
            LOG.exception("Polymarket orderbook request failed")
            poly_raw, errors["polymarket"] = {}, str(exc)
        venue_books = self.build_venue_books(games, kalshi_raw, poly_raw, ws_cache)
        ts = now_ts()
        price_rows, opp_rows, opp_console = [], [], []
        for (gkey, mkey), by_outcome in venue_books.items():
            game = games[gkey]
            market = game["markets"][mkey]
            # Save every executable level (after deduping identical instrument/price entries).
            seen_levels = set()
            for selection, levels in by_outcome.items():
                for level in levels:
                    signature = (selection, level.venue, level.instrument_id, level.raw_price,
                                 level.raw_quantity, level.market_id)
                    if signature in seen_levels:
                        continue
                    seen_levels.add(signature)
                    price_rows.append({"ts": ts, "game_key": gkey, "matchup": game.get("matchup", gkey),
                        "market_key": mkey, "selection": selection, "venue": level.venue,
                        "instrument_id": level.instrument_id, "market_id": level.market_id,
                        "cost_per_payout": level.cost_per_payout, "payout_capacity": level.payout_capacity,
                        "raw_price": level.raw_price, "raw_quantity": level.raw_quantity, "label": level.label})
            # Selection coverage: if the market's discovery legs contain an outcome
            # but no venue has a live ask for it, evaluate_arb safely returns None.
            arb = evaluate_arb(gkey, game, mkey, market, by_outcome, self.min_edge)
            if arb:
                opp_console.append(arb)
                opp_rows.append({"ts": ts, "game_key": gkey, "matchup": arb["matchup"],
                    "market_key": mkey, "outcomes": " | ".join(arb["outcomes"]),
                    "payout_size": round(arb["payout_size"], 4), "total_cost": round(arb["total_cost"], 4),
                    "gross_profit": round(arb["gross_profit"], 4), "gross_edge_pct": round(100*arb["gross_edge"], 4),
                    "legs_json": json.dumps(arb["legs"], separators=(",", ":"))})
        db_price_rows = [{k: r[k] for k in ("ts", "game_key", "market_key", "selection", "venue", "instrument_id",
                        "market_id", "cost_per_payout", "payout_capacity", "raw_price", "raw_quantity", "label")} for r in price_rows]
        db_opp_rows = [{"ts": r["ts"], "game_key": r["game_key"], "market_key": r["market_key"],
                        "outcomes": r["outcomes"], "payout_size": r["payout_size"], "total_cost": r["total_cost"],
                        "gross_profit": r["gross_profit"], "gross_edge": r["gross_edge_pct"] / 100,
                        "legs_json": r["legs_json"]} for r in opp_rows]
        self.store.save_snapshot(db_price_rows, db_opp_rows)
        self.write_csvs(price_rows, opp_rows)
        self.diagnostics["last_run_utc"] = datetime.now(timezone.utc).isoformat()
        self.diagnostics["kalshi_books_returned"] = len(kalshi_raw)
        self.diagnostics["polymarket_books_returned"] = len(poly_raw)
        self.diagnostics["api_errors"] = errors
        (self.out_dir / "tracker_diagnostics.json").write_text(json.dumps(self.diagnostics, indent=2), encoding="utf-8")
        if opp_console:
            for o in sorted(opp_console, key=lambda x: x["gross_profit"], reverse=True)[:10]:
                LOG.warning("ARB %.2f%% | %s | payout $%.2f cost $%.2f profit $%.2f | %s",
                            o["gross_edge"]*100, o["matchup"], o["payout_size"], o["total_cost"],
                            o["gross_profit"], o["market"] if "market" in o else o["legs"].keys())
        else:
            LOG.info("No opportunities above %.2f%% gross edge; stored %d price levels",
                     self.min_edge*100, len(price_rows))
        return len(price_rows), len(opp_rows)

    async def run(self, once: bool = False):
        games = self.load_markets()
        tickers, tokens = self.collect_ids(games)
        if not games:
            raise RuntimeError(f"No moneyline markets found in {self.markets_path}")
        LOG.info("Loaded %d games, %d moneyline markets, %d Kalshi tickers, %d Polymarket tokens",
                 len(games), sum(len(g["markets"]) for g in games.values()), len(tickers), len(tokens))
        ws_cache = PolyWsCache(tokens) if tokens else None
        ws_task = asyncio.create_task(ws_cache.run()) if ws_cache and not once else None
        try:
            while True:
                started = now_ts()
                prices, opps = await asyncio.to_thread(self.run_once, games, ws_cache)
                LOG.info("Snapshot complete: %d price levels, %d opportunities", prices, opps)
                if once:
                    break
                # poll Kalshi/ProphetX; Poly WS updates are merged over REST snapshots.
                await asyncio.sleep(max(0.1, self.interval - (now_ts() - started)))
        finally:
            if ws_task:
                ws_task.cancel()
                try:
                    await ws_task
                except asyncio.CancelledError:
                    pass
            self.store.close()


def main():
    p = argparse.ArgumentParser(description="Track moneyline prices and cross-venue arbitrage")
    p.add_argument("--markets", type=Path, default=Path("out/markets.json"), help="market_pipeline.py JSON output")
    p.add_argument("--out", type=Path, default=Path("out/tracker"))
    p.add_argument("--interval", type=float, default=3.0, help="Kalshi/ProphetX REST refresh seconds")
    p.add_argument("--min-edge", type=float, default=0.005, help="minimum gross edge as decimal; 0.005 = 0.5%%")
    p.add_argument("--kalshi-fee-bps", type=float, default=float(os.getenv("KALSHI_FEE_BPS", "0")))
    p.add_argument("--poly-fee-bps", type=float, default=float(os.getenv("POLY_FEE_BPS", "0")))
    p.add_argument("--prophetx-fee-bps", type=float, default=float(os.getenv("PROPHETX_FEE_BPS", "0")))
    p.add_argument("--timeout", type=float, default=8.0)
    p.add_argument("--prophetx-refresh", type=float, default=8.0,
                   help="minimum seconds between ProphetX event-market REST refreshes")
    p.add_argument("--pipeline-debug", type=Path, default=Path("out/debug/unresolved.json"),
                   help="optional unresolved.json emitted by market_pipeline.py")
    p.add_argument("--once", action="store_true", help="take one snapshot and exit")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if not args.markets.exists():
        p.error(f"Cannot find {args.markets}; run market_pipeline.py first")
    tracker = Tracker(args.markets, args.out, args.interval, args.min_edge,
                      args.kalshi_fee_bps, args.poly_fee_bps, args.prophetx_fee_bps, args.timeout,
                      args.prophetx_refresh, args.pipeline_debug)
    try:
        asyncio.run(tracker.run(args.once))
    except KeyboardInterrupt:
        LOG.info("Stopped")

if __name__ == "__main__":
    main()
