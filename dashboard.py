#!/usr/bin/env python3
"""Tiny read-only local dashboard for arbitrage_tracker.py output.

Uses only the Python standard library. It reads the tracker's CSV/JSON output;
it does not call exchange APIs or share the tracker's polling loop.
"""
from __future__ import annotations

import argparse
import csv
import json
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

HTML = r'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Arbitrage Monitor</title>
<style>
:root{color-scheme:dark;--bg:#0b1020;--panel:#121a2b;--line:#26334a;--muted:#94a3b8;--text:#e5edf8;--green:#35d49a;--red:#fb7185;--blue:#7dd3fc}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.45 system-ui,-apple-system,Segoe UI, sans-serif}
main{max-width:1250px;margin:0 auto;padding:24px}header{display:flex;align-items:center;justify-content:space-between;gap:16px;margin-bottom:20px}
h1{font-size:21px;margin:0;font-weight:650}.sub{color:var(--muted);font-size:12px;margin-top:3px}.status{display:flex;align-items:center;gap:8px;color:var(--muted);font-size:12px}.dot{width:8px;height:8px;border-radius:50%;background:#64748b}.dot.live{background:var(--green);box-shadow:0 0 10px #35d49a66}.dot.bad{background:var(--red)}
.cards{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-bottom:16px}.card,.panel{background:var(--panel);border:1px solid var(--line);border-radius:12px}.card{padding:14px}.label{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.06em}.value{font-size:25px;font-weight:650;margin-top:5px;font-variant-numeric:tabular-nums}.green{color:var(--green)}.muted{color:var(--muted)}
.panel{overflow:hidden;margin-bottom:14px}.panel-head{padding:14px 16px;border-bottom:1px solid var(--line);display:flex;justify-content:space-between;align-items:center;gap:10px}.panel-head h2{font-size:14px;margin:0;font-weight:620}.panel-head span{font-size:11px;color:var(--muted)}
.table-wrap{overflow-x:auto}table{width:100%;border-collapse:collapse;white-space:nowrap}th,td{text-align:left;padding:11px 14px;border-bottom:1px solid #202c40}th{font-size:10px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);font-weight:600;background:#101829}td{font-size:12px}tbody tr:last-child td{border-bottom:0}.num{font-variant-numeric:tabular-nums}.profit{color:var(--green);font-weight:650}.edge{color:var(--green);font-weight:650}.pill{display:inline-block;padding:3px 7px;border-radius:6px;background:#1b2940;color:#c7d7ee;font-size:10px;margin:2px 3px 2px 0}.empty{padding:30px 16px;text-align:center;color:var(--muted)}.foot{color:var(--muted);font-size:11px;padding:2px 2px 12px}.error{color:var(--red)}
@media(max-width:760px){main{padding:14px}.cards{grid-template-columns:repeat(2,minmax(0,1fr))}header{align-items:flex-start;flex-direction:column}.value{font-size:22px}}
</style></head><body><main>
<header><div><h1>Arbitrage Monitor</h1><div class="sub">Read-only view of your local tracker · no exchange requests from this page</div></div><div class="status"><i class="dot" id="dot"></i><span id="state">Connecting…</span><span>·</span><span id="updated">Waiting for first snapshot</span></div></header>
<section class="cards">
 <div class="card"><div class="label">Live opportunities</div><div class="value" id="opps">—</div></div>
 <div class="card"><div class="label">Best gross edge</div><div class="value green" id="best-edge">—</div></div>
 <div class="card"><div class="label">Best estimated profit</div><div class="value green" id="best-profit">—</div></div>
 <div class="card"><div class="label">Price levels loaded</div><div class="value" id="levels">—</div></div>
</section>
<section class="panel"><div class="panel-head"><h2>Arbitrage opportunities</h2><span id="threshold">Gross edge · before account fees not configured here</span></div>
<div class="table-wrap"><table><thead><tr><th>Matchup</th><th>Outcomes / execution legs</th><th>Payout</th><th>Total cost</th><th>Est. profit</th><th>Gross edge</th></tr></thead><tbody id="opportunity-rows"><tr><td colspan="6" class="empty">Waiting for tracker data…</td></tr></tbody></table></div></section>
<section class="panel"><div class="panel-head"><h2>Data feed status</h2><span id="snapshot-counts">—</span></div><div class="table-wrap"><table><thead><tr><th>Source</th><th>Price levels</th><th>Missing books / errors</th></tr></thead><tbody id="venue-rows"><tr><td colspan="3" class="empty">Waiting for tracker data…</td></tr></tbody></table></div></section>
<div class="foot">Auto-refreshes from local output files every 1.5 seconds. A quote or displayed opportunity is not a guaranteed fill; verify prices and settlement rules before trading.</div>
</main><script>
const $=id=>document.getElementById(id);const money=n=>Number.isFinite(+n)?'$'+(+n).toFixed(2):'—';
const pct=n=>Number.isFinite(+n)?(+n).toFixed(2)+'%':'—';
function esc(s){return String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function renderLegs(raw){let x={};try{x=JSON.parse(raw||'{}')}catch{};let out=[];for(const [selection,fills] of Object.entries(x)){for(const f of (Array.isArray(fills)?fills:[])){out.push(`<span class="pill">${esc(selection)} · ${esc(f.venue||'?')} · ${(Number(f.cost_per_payout)*100).toFixed(2)}¢</span>`);}}return out.join('')||'<span class="muted">No leg details</span>';}
async function refresh(){try{const r=await fetch('/api/data',{cache:'no-store'});if(!r.ok)throw Error('HTTP '+r.status);const d=await r.json();const opp=d.opportunities||[],prices=d.prices||[],diag=d.diagnostics||{};
$('opps').textContent=opp.length;$('levels').textContent=prices.length.toLocaleString();const best=opp.reduce((a,b)=>(+b.gross_edge_pct>+(a?.gross_edge_pct??-Infinity)?b:a),null);$('best-edge').textContent=best?pct(best.gross_edge_pct):'—';$('best-profit').textContent=best?money(best.gross_profit):'—';
$('opportunity-rows').innerHTML=opp.length?opp.map(o=>`<tr><td><strong>${esc(o.matchup||o.game_key)}</strong><div class="sub">${esc(o.market_key||'Moneyline')}</div></td><td>${renderLegs(o.legs_json)}</td><td class="num">${money(o.payout_size)}</td><td class="num">${money(o.total_cost)}</td><td class="num profit">${money(o.gross_profit)}</td><td class="num edge">${pct(o.gross_edge_pct)}</td></tr>`).join(''):'<tr><td colspan="6" class="empty">No opportunities currently meet your minimum edge threshold.</td></tr>';
const counts={kalshi:0,polymarket:0,prophetx:0};for(const p of prices){if(p.venue in counts)counts[p.venue]++;}const missing=diag.missing_book_counts||{};const errors=diag.api_errors||{};
$('venue-rows').innerHTML=Object.keys(counts).map(v=>`<tr><td>${esc(v[0].toUpperCase()+v.slice(1))}</td><td class="num">${counts[v].toLocaleString()}</td><td>${esc(errors[v]||missing[v]||'—')}</td></tr>`).join('');
const stamp=diag.last_run_utc?new Date(diag.last_run_utc):null;const age=stamp?Math.max(0,(Date.now()-stamp.getTime())/1000):Infinity;const fresh=age<15;$('dot').className='dot '+(fresh?'live':(stamp?'bad':''));$('state').textContent=fresh?'Tracker live':(stamp?'Tracker data stale':'Waiting for tracker');$('updated').textContent=stamp?`Snapshot ${age<60?Math.round(age)+'s ago':Math.round(age/60)+'m ago'}`:'No snapshot yet';$('snapshot-counts').textContent=`${diag.games_loaded??'—'} games · ${diag.moneyline_markets_loaded??'—'} moneyline markets`;
}catch(e){$('dot').className='dot bad';$('state').textContent='Dashboard read error';$('updated').textContent=e.message;}}
refresh();setInterval(refresh,1500);
</script></body></html>'''


def read_csv(path: Path) -> list[dict]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    try:
        with path.open("r", newline="", encoding="utf-8-sig") as f:
            return list(csv.DictReader(f))
    except (OSError, csv.Error, UnicodeError):
        return []


def read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, json.JSONDecodeError, UnicodeError):
        return {}


def make_handler(out_dir: Path):
    # Cache parsed files by modification time so the browser's frequent refreshes
    # do not repeatedly parse the same CSVs.
    cache: dict[str, tuple[int, int, object]] = {}

    def cached(path: Path, reader):
        try:
            stat = path.stat()
            signature = (stat.st_mtime_ns, stat.st_size)
        except OSError:
            signature = (0, 0)
        key = str(path)
        previous = cache.get(key)
        if previous and previous[:2] == signature:
            return previous[2]
        value = reader(path)
        cache[key] = (signature[0], signature[1], value)
        return value

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            route = urlparse(self.path).path
            if route == "/":
                body = HTML.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
            elif route == "/api/data":
                data = {
                    "opportunities": cached(out_dir / "opportunities.csv", read_csv),
                    "prices": cached(out_dir / "prices_latest.csv", read_csv),
                    "diagnostics": cached(out_dir / "tracker_diagnostics.json", read_json),
                    "server_time": datetime.now(timezone.utc).isoformat(),
                }
                body = json.dumps(data, separators=(",", ":")).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
            else:
                body = b"Not found"
                self.send_response(404)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt, *args):
            # Keep the terminal quiet; this server only serves local dashboard requests.
            return
    return Handler


def main():
    parser = argparse.ArgumentParser(description="Serve a lightweight local dashboard for arbitrage_tracker.py")
    parser.add_argument("--out", type=Path, default=Path("out/tracker"), help="tracker output directory")
    parser.add_argument("--host", default="127.0.0.1", help="default is localhost only")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    out_dir = args.out.resolve()
    server = ThreadingHTTPServer((args.host, args.port), make_handler(out_dir))
    print(f"Dashboard reading: {out_dir}")
    print(f"Open http://{args.host}:{args.port} in your browser")
    print("This server only reads tracker output; it does not call exchange APIs.")
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        print("\nDashboard stopped.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
