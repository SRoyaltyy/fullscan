"""Paper-trading engine + dashboard for the daily stock book.

Simulates 10 sleeves (5 horizons x 2 selection sets), each with its own
capital, following the daily stock book:

    {1d,3d,1w,2w,1m}_top   — top N overall BUY names from the book
    {1d,3d,1w,2w,1m}_size  — top 3 per size bucket (large+ / mid / small-micro)

Rules
- Rebuild from scratch every run: replay all books chronologically (idempotent).
- Entry/exit at the signal day's closing price (yfinance, auto-adjusted).
- Follow-the-book: hold a name while it stays in the sleeve's pick list;
  sell when it drops out; new names split available cash equally (whole shares).
- Every order is charged the Futubull US-stock fee schedule
  (00_grounding/futubull_fees.json).
- SPY tracked as benchmark with the same starting capital.

Outputs
- data/paper/equity_curve.csv   — daily equity per sleeve + SPY
- data/paper/trades.csv         — every simulated order
- data/paper/state.json         — positions / cash per sleeve (latest)
- 03_scoreboard/PAPER_TRADING.md — summary table
- dashboard/index.html          — self-contained equity dashboard

CLI: python -m src.paper_trade [--date YYYY-MM-DD] [--top 10] [--capital 10000]
"""
from __future__ import annotations

import argparse
import json
import math
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from . import config

ROOT = Path(__file__).resolve().parent.parent
BOOK_DIR = ROOT / "data" / "stock_book"
PAPER_DIR = ROOT / "data" / "paper"
SCOREBOARD = ROOT / "03_scoreboard"
DASH_DIR = ROOT / "dashboard"
FEES_PATH = ROOT / "00_grounding" / "futubull_fees.json"
PRICE_CACHE = PAPER_DIR / "prices_cache.csv"

HOLD_DAYS = {"1d": 1, "3d": 3, "1w": 5, "2w": 10, "1m": 21}
HORIZONS = list(HOLD_DAYS.keys())


# ---------------------------------------------------------------- fees ----

def load_fees() -> dict:
    return json.loads(FEES_PATH.read_text(encoding="utf-8"))


def order_fees(shares: int, price: float, side: str, f: dict) -> float:
    """Futubull US-stock order fees for one order."""
    if shares <= 0 or price <= 0:
        return 0.0
    amount = shares * price
    comm = min(max(f["commission_per_share"] * shares, f["commission_min_per_order"]),
               f["commission_max_pct_of_amount"] * amount)
    plat = min(max(f["platform_per_share"] * shares, f["platform_min_per_order"]),
               f["platform_max_pct_of_amount"] * amount)
    settle = f["settlement_per_share"] * shares
    total = comm + plat + settle
    if side == "sell":
        reg = max(f["regulatory_pct_of_amount_sell_only"] * amount,
                  f["regulatory_min_per_order"])
        taf = min(max(f["taf_per_share_sell_only"] * shares, f["taf_min_per_order"]),
                  f["taf_max_per_order"])
        total += reg + taf
    return round(total, 4)


# -------------------------------------------------------------- prices ----

def get_prices(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """Date-indexed close prices; incremental cache in data/paper/.

    Fetches a padded window (books are generated pre-market, so the signal
    day's own close may not exist yet — fills then use the last available
    close on/before the signal date, resolved in run_sim)."""
    import yfinance as yf

    cache = pd.DataFrame()
    if PRICE_CACHE.exists():
        cache = pd.read_csv(PRICE_CACHE, index_col=0, parse_dates=True)
    missing_cols = [t for t in tickers if t not in cache.columns]
    need_refresh = len(cache) == 0 or cache.index.max() < pd.Timestamp(end)
    if missing_cols or need_refresh:
        fetch = sorted(set(tickers) | set(cache.columns))
        # yfinance `end` is EXCLUSIVE — pad so the signal day is included once
        # its close exists; start padded back for a pre-book price baseline.
        start_pad = (pd.Timestamp(start) - pd.Timedelta(days=10)).date().isoformat()
        end_excl = (pd.Timestamp(end) + pd.Timedelta(days=6)).date().isoformat()
        raw = yf.download(fetch, start=start_pad, end=end_excl, auto_adjust=True,
                          group_by="ticker", progress=False, threads=True)
        frames = {}
        for t in fetch:
            try:
                s = raw[(t, "Close")] if len(fetch) > 1 else raw["Close"]
            except KeyError:
                continue
            frames[t] = s.dropna()
        if not frames:
            raise SystemExit("[paper] yfinance returned no prices at all")
        new = pd.DataFrame(frames)
        new.index = pd.to_datetime(new.index)
        # new data wins on overlap; keep older cached rows outside the window
        combined = new.combine_first(cache) if not cache.empty else new
        cache = combined.dropna(how="all").sort_index()
        PAPER_DIR.mkdir(parents=True, exist_ok=True)
        cache.to_csv(PRICE_CACHE)
    return cache


# -------------------------------------------------------------- books -----

def list_books() -> list[tuple[str, Path]]:
    out = []
    for p in sorted(BOOK_DIR.glob("*_stock_book.json")):
        out.append((p.name.replace("_stock_book.json", ""), p))
    return out


def picks_from_book(book: dict, top_n: int) -> dict[str, list[str]]:
    """sleeve -> ordered pick list for one book."""
    books = book.get("books", {})
    picks: dict[str, list[str]] = {}
    for h in HORIZONS:
        hb = books.get(h) or {}
        top = [r["ticker"] for r in (hb.get("buy") or [])[:top_n]]
        picks[f"{h}_top"] = top
        sized: list[str] = []
        by_size = hb.get("buy_by_size") or {}
        for bucket in ("large+", "mid", "small/micro"):
            sized += [r["ticker"] for r in (by_size.get(bucket) or [])[:3]]
        picks[f"{h}_size"] = sized
    return picks


# ------------------------------------------------------------- engine -----

def run_sim(books: list[tuple[str, Path]], prices: pd.DataFrame,
            capital: float, top_n: int, fees: dict):
    sleeves = [f"{h}_{k}" for h in HORIZONS for k in ("top", "size")]
    st = {
        s: {"cash": capital, "pos": {}, "realized": 0.0, "fees": 0.0,
            "trades": 0, "wins": 0, "closed": 0}
        for s in sleeves
    }
    curve_rows: list[dict] = []
    trade_rows: list[dict] = []
    spy0 = None

    for date, path in books:
        day_px = prices.loc[:date]
        if day_px.empty:
            continue
        px = day_px.iloc[-1]  # close on (or last close before) signal date

        def price_of(t: str) -> float | None:
            v = px.get(t)
            if v is None or (isinstance(v, float) and (math.isnan(v) or v <= 0)):
                return None
            return float(v)

        book = json.loads(path.read_text(encoding="utf-8"))
        picks = picks_from_book(book, top_n)

        for sleeve, targets in picks.items():
            S = st[sleeve]
            tset = set(targets)

            # exits: no longer recommended
            for t in list(S["pos"]):
                if t in tset:
                    continue
                p = price_of(t)
                if p is None:
                    continue  # can't price -> carry position
                pos = S["pos"].pop(t)
                fee = order_fees(pos["shares"], p, "sell", fees)
                proceeds = pos["shares"] * p - fee
                S["cash"] += proceeds
                pnl = proceeds - pos["cost"]
                S["realized"] += pnl
                S["fees"] += fee
                S["trades"] += 1
                S["closed"] += 1
                S["wins"] += 1 if pnl > 0 else 0
                trade_rows.append({"date": date, "sleeve": sleeve, "ticker": t,
                                   "side": "sell", "shares": pos["shares"],
                                   "price": round(p, 4), "fees": fee,
                                   "amount": round(proceeds, 2),
                                   "realized_pnl": round(pnl, 2)})

            # entries: new names split available cash equally
            new = [t for t in targets if t not in S["pos"]]
            if new:
                per = S["cash"] / len(new)
                for t in new:
                    p = price_of(t)
                    if p is None or per <= 0:
                        continue
                    shares = int(per // p)
                    if shares < 1:
                        continue
                    fee = order_fees(shares, p, "buy", fees)
                    cost = shares * p + fee
                    if cost > S["cash"]:
                        shares = int((S["cash"] - fee) // p)
                        if shares < 1:
                            continue
                        fee = order_fees(shares, p, "buy", fees)
                        cost = shares * p + fee
                    S["cash"] -= cost
                    S["pos"][t] = {"shares": shares, "entry_date": date,
                                   "entry_px": p, "cost": cost}
                    S["fees"] += fee
                    S["trades"] += 1
                    trade_rows.append({"date": date, "sleeve": sleeve,
                                       "ticker": t, "side": "buy",
                                       "shares": shares, "price": round(p, 4),
                                       "fees": fee, "amount": round(cost, 2),
                                       "realized_pnl": ""})

        # mark-to-market
        spy = price_of("SPY")
        if spy and spy0 is None:
            spy0 = spy
        for sleeve in sleeves:
            S = st[sleeve]
            invested = 0.0
            for t, pos in S["pos"].items():
                p = price_of(t)
                invested += pos["shares"] * (p if p else pos["entry_px"])
            curve_rows.append({"date": date, "sleeve": sleeve,
                               "equity": round(S["cash"] + invested, 2),
                               "cash": round(S["cash"], 2),
                               "invested": round(invested, 2),
                               "fees_cum": round(S["fees"], 2),
                               "realized_cum": round(S["realized"], 2)})
        if spy and spy0:
            curve_rows.append({"date": date, "sleeve": "SPY (benchmark)",
                               "equity": round(capital * spy / spy0, 2),
                               "cash": "", "invested": "", "fees_cum": "",
                               "realized_cum": ""})
    return st, curve_rows, trade_rows


# ------------------------------------------------------------ outputs -----

def sleeve_stats(sleeve: str, S: dict, prices: pd.DataFrame, capital: float) -> dict:
    px = prices.iloc[-1] if len(prices) else pd.Series(dtype=float)
    invested = 0.0
    for t, pos in S["pos"].items():
        v = px.get(t)
        invested += pos["shares"] * (float(v) if v == v and v else pos["entry_px"])
    equity = S["cash"] + invested
    return {"sleeve": sleeve, "equity": round(equity, 2),
            "return_pct": round(100 * (equity / capital - 1), 2),
            "cash": round(S["cash"], 2), "open": len(S["pos"]),
            "trades": S["trades"], "fees": round(S["fees"], 2),
            "realized": round(S["realized"], 2),
            "win_rate": round(100 * S["wins"] / S["closed"], 1) if S["closed"] else None}


def write_dashboard(curve: pd.DataFrame, stats: list[dict], st: dict,
                    prices: pd.DataFrame, date: str, capital: float,
                    fees: dict) -> None:
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    curve = curve.copy()
    curve["date"] = pd.to_datetime(curve["date"])
    pivot = curve.pivot_table(index="date", columns="sleeve", values="equity",
                              aggfunc="last").sort_index()
    series = {c: [None if (v != v) else v for v in pivot[c].tolist()]
              for c in pivot.columns}
    payload = {
        "generated": datetime.now(ZoneInfo(config.TZ)).isoformat(),
        "dates": [str(d.date()) for d in pivot.index],
        "series": series,
        "stats": stats,
        "capital": capital,
        "fees": {k: fees[k] for k in fees if not k.startswith("_")},
    }
    positions = []
    px = prices.iloc[-1] if len(prices) else pd.Series(dtype=float)
    for sleeve, S in st.items():
        for t, pos in S["pos"].items():
            cur = px.get(t)
            cur = float(cur) if cur == cur and cur else pos["entry_px"]
            positions.append({
                "sleeve": sleeve, "ticker": t, "shares": pos["shares"],
                "entry_date": pos["entry_date"], "entry_px": round(pos["entry_px"], 2),
                "last": round(cur, 2),
                "unrealized": round(pos["shares"] * cur - pos["cost"], 2)})
    payload["positions"] = positions

    html = _DASH_TEMPLATE.replace("__DATA__", json.dumps(payload))
    (DASH_DIR / "index.html").write_text(html, encoding="utf-8")


def write_report(stats: list[dict], date: str, capital: float) -> None:
    SCOREBOARD.mkdir(parents=True, exist_ok=True)
    L = [
        "# Paper trading — Futubull-fee simulation",
        "",
        f"As of **{date}** · ${capital:,.0f} starting capital per sleeve · "
        "fees per `00_grounding/futubull_fees.json`",
        "",
        "Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 "
        "per size bucket. Entries/exits at signal-day close; hold while the book "
        "keeps recommending.",
        "",
        "| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Win rate |",
        "|--------|--------|--------|------|----------|--------|-----------|--------------|----------|",
    ]
    for s in stats:
        wr = f"{s['win_rate']}%" if s["win_rate"] is not None else "—"
        L.append(f"| {s['sleeve']} | ${s['equity']:,.2f} | {s['return_pct']:+.2f}% | "
                 f"${s['cash']:,.2f} | {s['open']} | {s['trades']} | "
                 f"${s['fees']:,.2f} | ${s['realized']:+,.2f} | {wr} |")
    L += ["", "Equity curves + positions: `dashboard/index.html`", ""]
    (SCOREBOARD / "PAPER_TRADING.md").write_text("\n".join(L), encoding="utf-8")


# ------------------------------------------------------------ driver ------

def run(date: str | None = None, top_n: int = 10, capital: float | None = None) -> None:
    fees = load_fees()
    capital = capital or float(fees["paper_account"]["starting_capital_per_sleeve"])
    books = list_books()
    if date:
        books = [b for b in books if b[0] <= date]
    if not books:
        raise SystemExit("[paper] no stock books found — run stock_book first")

    # collect every ticker we may need to price
    tickers = {"SPY"}
    for _, p in books:
        bk = json.loads(p.read_text(encoding="utf-8"))
        for picks in picks_from_book(bk, top_n).values():
            tickers.update(picks)
    start, end = books[0][0], books[-1][0]
    prices = get_prices(sorted(tickers), start, end)

    st, curve_rows, trade_rows = run_sim(books, prices, capital, top_n, fees)
    if not curve_rows:
        raise SystemExit(
            f"[paper] no price data on/before {books[0][0]} — cannot simulate. "
            "Check yfinance connectivity.")

    PAPER_DIR.mkdir(parents=True, exist_ok=True)
    curve = pd.DataFrame(curve_rows)
    curve.to_csv(PAPER_DIR / "equity_curve.csv", index=False)
    pd.DataFrame(trade_rows).to_csv(PAPER_DIR / "trades.csv", index=False)
    (PAPER_DIR / "state.json").write_text(json.dumps(st, indent=2, default=str),
                                          encoding="utf-8")

    stats = [sleeve_stats(s, st[s], prices, capital) for s in st]
    last = books[-1][0]
    write_report(stats, last, capital)
    write_dashboard(curve, stats, st, prices, last, capital, fees)
    print(f"[paper] {len(books)} book(s), {len(trade_rows)} trades, "
          f"curves → dashboard/index.html, summary → 03_scoreboard/PAPER_TRADING.md")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--capital", type=float, default=None)
    args = ap.parse_args()
    run(date=args.date, top_n=args.top, capital=args.capital)


_DASH_TEMPLATE = """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Paper Trading — Stock Book Simulation</title>
<style>
 body{margin:0;background:#0f1420;color:#dfe6f2;font:14px/1.5 -apple-system,Segoe UI,Roboto,sans-serif}
 .wrap{max-width:1180px;margin:0 auto;padding:24px}
 h1{font-size:20px;margin:0 0 4px} .sub{color:#8b96ab;font-size:12px;margin-bottom:20px}
 .cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:10px;margin-bottom:20px}
 .card{background:#171e2e;border:1px solid #262f45;border-radius:10px;padding:12px 14px}
 .card b{display:block;font-size:18px;margin-top:2px}
 .pos{color:#4ade80}.neg{color:#f87171}.mut{color:#8b96ab}
 canvas{width:100%;background:#171e2e;border:1px solid #262f45;border-radius:10px}
 .legend{display:flex;flex-wrap:wrap;gap:8px;margin:10px 0 18px}
 .legend span{cursor:pointer;padding:2px 10px;border-radius:12px;border:1px solid #333;font-size:12px;user-select:none}
 table{width:100%;border-collapse:collapse;margin:14px 0 26px;font-size:12.5px}
 th,td{padding:6px 8px;border-bottom:1px solid #232c42;text-align:right;white-space:nowrap}
 th{color:#8b96ab;font-weight:600;text-align:right} td:first-child,th:first-child{text-align:left}
 h2{font-size:15px;margin:26px 0 6px;color:#aeb9cf}
 .note{color:#66708a;font-size:11.5px;margin-top:8px}
</style></head><body><div class="wrap">
<h1>Paper Trading — Stock Book Simulation</h1>
<div class="sub" id="gen"></div>
<div class="cards" id="cards"></div>
<h2>Equity curves (per sleeve, Futubull fees applied)</h2>
<div class="legend" id="legend"></div>
<canvas id="chart" height="380"></canvas>
<h2>Sleeve statistics</h2>
<table id="stats"></table>
<h2>Open positions</h2>
<table id="pos"></table>
<div class="note" id="fees"></div>
</div>
<script>
const D = __DATA__;
const COLORS = ["#60a5fa","#34d399","#fbbf24","#f472b6","#a78bfa","#f87171","#22d3ee","#fb923c","#4ade80","#e879f9","#94a3b8"];
const keys = Object.keys(D.series);
const hidden = new Set();
document.getElementById('gen').textContent =
  "Generated "+D.generated.slice(0,16).replace('T',' ')+" · "+D.dates.length+" trading day(s) · $"+
  D.capital.toLocaleString()+" per sleeve · long-only, whole shares, signal-day close fills";
// cards: best/worst/median sleeve + SPY
const sorted=[...D.stats].sort((a,b)=>b.return_pct-a.return_pct);
const cards=[["Best sleeve",sorted[0]],["Worst sleeve",sorted[sorted.length-1]]];
const spy=D.series["SPY (benchmark)"]; 
if(spy){const r=100*(spy[spy.length-1]/spy[0]-1);cards.push(["SPY benchmark",{sleeve:"SPY",return_pct:r.toFixed(2)}]);}
const tot=D.stats.reduce((a,s)=>a+s.equity,0);
cards.push(["Total equity",{sleeve:D.stats.length+" sleeves",return_pct:(100*(tot/(D.capital*D.stats.length)-1)).toFixed(2)}]);
document.getElementById('cards').innerHTML=cards.map(c=>{
  const cls=c[1].return_pct>=0?'pos':'neg';
  return `<div class="card"><span class="mut">${c[0]}</span><b>${c[1].sleeve}</b><span class="${cls}">${c[1].return_pct}%</span></div>`;}).join('');
// legend
const leg=document.getElementById('legend');
keys.forEach((k,i)=>{const s=document.createElement('span');s.textContent=k;
 s.style.borderColor=COLORS[i%COLORS.length];s.style.color=COLORS[i%COLORS.length];
 s.onclick=()=>{hidden.has(k)?hidden.delete(k):hidden.add(k);s.style.opacity=hidden.has(k)?0.3:1;draw();};
 leg.appendChild(s);});
// chart
function draw(){
 const cv=document.getElementById('chart'),ctx=cv.getContext('2d');
 const W=cv.width=cv.clientWidth*2,H=cv.height=760;ctx.scale(1,1);
 ctx.clearRect(0,0,W,H);
 const vis=keys.filter(k=>!hidden.has(k));
 let lo=Infinity,hi=-Infinity;
 vis.forEach(k=>D.series[k].forEach(v=>{if(v!=null){lo=Math.min(lo,v);hi=Math.max(hi,v);}}));
 if(lo===Infinity)return;const pad=(hi-lo)*0.06||1;lo-=pad;hi+=pad;
 const X=i=>40+i/(Math.max(1,D.dates.length-1))*(W-70);
 const Y=v=>H-30-(v-lo)/(hi-lo)*(H-60);
 ctx.strokeStyle='#2a3450';ctx.fillStyle='#66708a';ctx.font='20px sans-serif';
 for(let g=0;g<=4;g++){const v=lo+(hi-lo)*g/4;ctx.beginPath();ctx.moveTo(40,Y(v));ctx.lineTo(W-30,Y(v));ctx.stroke();
  ctx.fillText('$'+(v/1000).toFixed(1)+'k',2,Y(v)+6);}
 D.dates.forEach((d,i)=>{if(i%Math.ceil(D.dates.length/6)===0)ctx.fillText(d.slice(5),X(i)-18,H-8);});
 vis.forEach((k)=>{const i=keys.indexOf(k);ctx.strokeStyle=COLORS[i%COLORS.length];
  ctx.lineWidth=k.includes('SPY')?2.5:1.8;ctx.setLineDash(k.includes('SPY')?[6,4]:[]);
  ctx.beginPath();let started=false;
  D.series[k].forEach((v,j)=>{if(v==null)return;started?ctx.lineTo(X(j),Y(v)):ctx.moveTo(X(j),Y(v));started=true;});
  ctx.stroke();ctx.setLineDash([]);});
}
draw();addEventListener('resize',draw);
// stats table
document.getElementById('stats').innerHTML=
 "<tr><th>Sleeve</th><th>Equity</th><th>Return</th><th>Cash</th><th>Open</th><th>Trades</th><th>Fees</th><th>Realized P/L</th><th>Win rate</th></tr>"+
 D.stats.map(s=>`<tr><td>${s.sleeve}</td><td>$${s.equity.toLocaleString()}</td>
  <td class="${s.return_pct>=0?'pos':'neg'}">${s.return_pct}%</td><td>$${s.cash.toLocaleString()}</td>
  <td>${s.open}</td><td>${s.trades}</td><td>$${s.fees.toLocaleString()}</td>
  <td class="${s.realized>=0?'pos':'neg'}">$${s.realized.toLocaleString()}</td>
  <td>${s.win_rate==null?'—':s.win_rate+'%'}</td></tr>`).join('');
// positions
document.getElementById('pos').innerHTML=
 "<tr><th>Sleeve</th><th>Ticker</th><th>Shares</th><th>Entry date</th><th>Entry px</th><th>Last</th><th>Unrealized P/L</th></tr>"+
 (D.positions.length?D.positions.map(p=>`<tr><td>${p.sleeve}</td><td><b>${p.ticker}</b></td><td>${p.shares}</td>
  <td>${p.entry_date}</td><td>$${p.entry_px}</td><td>$${p.last}</td>
  <td class="${p.unrealized>=0?'pos':'neg'}">$${p.unrealized.toLocaleString()}</td></tr>`).join('')
  :'<tr><td colspan="7" class="mut">No open positions.</td></tr>');
const F=D.fees;
document.getElementById('fees').textContent=
 `Fee model (Futubull US stocks): commission $${F.commission_per_share}/sh (min $${F.commission_min_per_order}, cap 0.5%) + platform $${F.platform_per_share}/sh (min $${F.platform_min_per_order}, cap 0.5%) + settlement $${F.settlement_per_share}/sh; sells add regulatory ${F.regulatory_pct_of_amount_sell_only*100}% of amount + TAF $${F.taf_per_share_sell_only}/sh (max $${F.taf_max_per_order}).`;
</script></body></html>"""


if __name__ == "__main__":
    main()
