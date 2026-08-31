"""Portfolio-level backtest: turn the 254k historical signals into actual
buy/sell decisions with position sizing, capital limits and Futubull fees.

The signal ledger (backtest/trades.csv) says WHAT could have been traded.
This module answers: given real money and real constraints, WHICH signals do
you actually take, and what does the equity curve look like?

Accounting notes (documented, not hidden):
  - entries at confirmation-day close, exits per strategy rule (from ledger)
  - equity marks OPEN positions at cost basis (no intra-trade daily prices in
    the ledger) -> drawdown is understated for the long-hold tp strategies;
    1-2 day strategies (L3, L5, S1, S2) are near-exact
  - shorts: entry notional locked as margin collateral, borrow fee pro-rated
  - fees = Futubull US-stock schedule (FEES dict below, all configurable)

Usage:
  python engine/portfolio.py                 # all scenarios -> md + html
"""
import csv
import json
import math
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

BT_DIR = "backtest"
TRADES_CSV = os.path.join(BT_DIR, "trades.csv")

# ----------------------------------------------------------- Futubull fees -
# Futu Inc (moomoo/Futubull) US-stock pricing, per order:
#   commission  $0.0049/share, min $0.99
#   platform    $0.0050/share, min $1.00
#   SEC fee     sell-side only, x SEC_RATE of proceeds (0.0000278 = $27.8/M,
#               conservative; the live rate was cut toward $0 in 2025)
#   TAF         sell-side only, $0.000166/share, min $0.01, max $8.30
#   short borrow: annualized BORROW_ANNUAL on short notional, pro-rated daily
FEES = {
    "commission_per_share": 0.0049,
    "commission_min": 0.99,
    "platform_per_share": 0.005,
    "platform_min": 1.00,
    "sec_rate": 0.0000278,
    "taf_per_share": 0.000166,
    "taf_min": 0.01,
    "taf_max": 8.30,
    "borrow_annual": 0.01,
    "slippage_bps": 5.0,          # 5 bps per side, conservative
}


def order_fees(shares, price, is_sell, cfg=FEES):
    proceeds = shares * price
    f = max(cfg["commission_per_share"] * shares, cfg["commission_min"])
    f += max(cfg["platform_per_share"] * shares, cfg["platform_min"])
    if is_sell:
        f += cfg["sec_rate"] * proceeds
        f += min(max(cfg["taf_per_share"] * shares, cfg["taf_min"]),
                 cfg["taf_max"])
    f += proceeds * cfg["slippage_bps"] / 1e4
    return f


# --------------------------------------------------------------- scenarios -
LONGS = ["L1_long_green_tp8_lowvol", "L2_long_green_tp3_lowvol",
         "L3_long_green_hold2_midcap", "L4_long_green_hold8_bbailike",
         "L5_long_green_hold2_midhibeta"]
SHORTS = ["S1_short_red_1day_optionable", "S2_short_red_1day_hivol"]

SCENARIOS = [
    {"name": "A_all_signals",
     "desc": "Every signal from all 7 strategies, $10k each, max 40 open",
     "strategies": "ALL", "per_trade": 10000, "max_new_day": 999,
     "max_open": 40, "capital": 500000, "rank": False},
    {"name": "B_top3_strength",
     "desc": "Top 3 signals/day ranked by highlight strength, $10k each",
     "strategies": "ALL", "per_trade": 10000, "max_new_day": 3,
     "max_open": 30, "capital": 300000, "rank": True},
    {"name": "C_long_only_top3",
     "desc": "Longs only (L1-L5), top 3/day by strength, $10k each",
     "strategies": LONGS, "per_trade": 10000, "max_new_day": 3,
     "max_open": 20, "capital": 200000, "rank": True},
    {"name": "D_short_only_top5",
     "desc": "Shorts only (S1-S2), top 5/day by redness, $10k each",
     "strategies": SHORTS, "per_trade": 10000, "max_new_day": 5,
     "max_open": 25, "capital": 250000, "rank": True},
    {"name": "E_sleeves",
     "desc": "$75k sleeve per strategy, $10k/trade, max 7 open per sleeve",
     "strategies": "SLEEVES", "per_trade": 10000, "max_new_day": 999,
     "max_open": 7, "capital": 75000, "rank": False},
    {"name": "F_best_pair_L2_S1",
     "desc": "Only L2 (tp3 longs) + S1 (1-day shorts), top 3/day each side",
     "strategies": ["L2_long_green_tp3_lowvol",
                    "S1_short_red_1day_optionable"],
     "per_trade": 10000, "max_new_day": 3,
     "max_open": 25, "capital": 250000, "rank": True},
    {"name": "G_long_compound",
     "desc": "Longs only, top 3/day, 8% of CURRENT equity per trade "
             "(compounding)",
     "strategies": LONGS, "per_trade": 0.08, "size_mode": "pct",
     "max_new_day": 3, "max_open": 20, "capital": 200000, "rank": True},
    {"name": "H_all_compound",
     "desc": "All strategies, top 4/day, 5% of CURRENT equity per trade "
             "(compounding)",
     "strategies": "ALL", "per_trade": 0.05, "size_mode": "pct",
     "max_new_day": 4, "max_open": 30, "capital": 300000, "rank": True},
]

COLOR_SCORE = {"deep green": 2.0, "green": 1.5, "light green": 1.0,
               "deep red": -2.0, "red": -1.5, "pink": -1.0}


def color_strength(colors_pipe, side):
    s = sum(COLOR_SCORE.get(c.strip(), 0.0) for c in colors_pipe.split("|"))
    return s * (1 if side == "LONG" else -1)


# ------------------------------------------------------------------ engine -
def load_trades():
    out = []
    with open(TRADES_CSV, newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            try:
                out.append({
                    "ticker": r["ticker"], "strategy": r["strategy"],
                    "side": r["side"],
                    "d_in": date.fromisoformat(r["signal_date"]),
                    "p_in": float(r["entry_close"]),
                    "d_out": date.fromisoformat(r["exit_date"]),
                    "p_out": float(r["exit_price"]),
                    "reason": r["exit_reason"],
                    "hold_days": int(r["hold_days"]),
                    "strength": color_strength(r["signal_colors"], r["side"]),
                })
            except (ValueError, KeyError):
                continue
    return out


def run_scenario(cfg, trades):
    sleeves = (cfg["strategies"] == "SLEEVES")
    if sleeves:
        books = {s: {"cash": cfg["capital"], "open": []}
                 for s in LONGS + SHORTS}
        capital0 = cfg["capital"] * len(books)
    else:
        allowed = set(LONGS + SHORTS if cfg["strategies"] == "ALL"
                      else cfg["strategies"])
        books = {"_": {"cash": cfg["capital"], "open": []}}
        capital0 = cfg["capital"]

    by_in = defaultdict(list)
    for t in trades:
        if sleeves or t["strategy"] in allowed:
            by_in[t["d_in"]].append(t)
    all_days = sorted(set(by_in) | {t["d_out"] for v in by_in.values()
                                    for t in v})
    equity_pts, realized, fees_paid = [], [], 0.0
    n_taken = n_skip_cash = n_skip_cap = 0

    def book_equity():
        tot = 0.0
        for b in books.values():
            eq = b["cash"]
            for p in b["open"]:
                if p["side"] == "LONG":
                    eq += p["shares"] * p["p_in"]          # mark at cost
                else:
                    eq += p["lock"]                        # collateral back
            tot += eq
        return tot

    for d in all_days:
        # ---- exits first
        for name, b in books.items():
            still = []
            for p in b["open"]:
                if p["d_out"] <= d:
                    f = order_fees(p["shares"], p["p_out"], True)
                    fees_paid += f
                    if p["side"] == "LONG":
                        pnl = (p["p_out"] - p["p_in"]) * p["shares"]
                        b["cash"] += p["shares"] * p["p_out"] - f
                    else:
                        pnl = (p["p_in"] - p["p_out"]) * p["shares"]
                        borrow = (cfg.get("borrow", FEES["borrow_annual"])
                                  * p["lock"] * p["hold_days"] / 365)
                        fees_paid += borrow
                        b["cash"] += p["lock"] + pnl - f - borrow
                    realized.append({**p, "pnl": pnl - f -
                                     (0 if p["side"] == "LONG" else borrow),
                                     "book": name})
                else:
                    still.append(p)
            b["open"] = still
        # ---- entries
        cands = by_in.get(d, [])
        if cfg["rank"]:
            cands = sorted(cands, key=lambda t: -t["strength"])
        per_book_new = defaultdict(int)
        for t in cands:
            key = t["strategy"] if sleeves else "_"
            b = books.get(key)
            if b is None:
                continue
            if per_book_new[key] >= cfg["max_new_day"]:
                continue
            if len(b["open"]) >= cfg["max_open"]:
                n_skip_cap += 1
                continue
            if cfg.get("size_mode") == "pct":
                held = sum(p["shares"] * p["p_in"] for p in b["open"]
                           if p["side"] == "LONG") + \
                       sum(p["lock"] for p in b["open"] if p["side"] != "LONG")
                alloc = (b["cash"] + held) * cfg["per_trade"]
            else:
                alloc = cfg["per_trade"]
            shares = int(alloc // t["p_in"])
            if shares < 1:
                n_skip_cash += 1
                continue
            f = order_fees(shares, t["p_in"], False)
            cost = shares * t["p_in"]
            if t["side"] == "LONG":
                if b["cash"] < cost + f:
                    n_skip_cash += 1
                    continue
                b["cash"] -= cost + f
                lock = 0.0
            else:
                if b["cash"] < cost + f:
                    n_skip_cash += 1
                    continue
                b["cash"] -= cost + f
                lock = cost
            fees_paid += f
            b["open"].append({**t, "shares": shares, "lock": lock})
            per_book_new[key] += 1
            n_taken += 1
        equity_pts.append((d, book_equity()))

    # ---- metrics
    eq = [v for _, v in equity_pts]
    days = [d for d, _ in equity_pts]
    total_ret = eq[-1] / capital0 - 1 if eq else 0
    yrs = max((days[-1] - days[0]).days / 365.25, 0.01) if days else 0
    cagr = (eq[-1] / capital0) ** (1 / yrs) - 1 if eq and eq[-1] > 0 else -1
    peak, mdd = -1e18, 0.0
    for v in eq:
        peak = max(peak, v)
        mdd = min(mdd, v / peak - 1)
    rets = [eq[i] / eq[i - 1] - 1 for i in range(1, len(eq)) if eq[i - 1] > 0]
    mu = sum(rets) / len(rets) if rets else 0
    sd = math.sqrt(sum((r - mu) ** 2 for r in rets) / max(len(rets) - 1, 1))
    sharpe = mu / sd * math.sqrt(252) if sd > 0 else 0
    pnls = [p["pnl"] for p in realized]
    wins = [p for p in pnls if p > 0]
    gross_w = sum(wins)
    gross_l = -sum(p for p in pnls if p < 0)
    return {
        "name": cfg["name"], "desc": cfg["desc"], "capital0": capital0,
        "equity": equity_pts, "trades": realized,
        "n_taken": n_taken, "n_skip_cash": n_skip_cash,
        "n_skip_cap": n_skip_cap, "fees": fees_paid,
        "total_ret": total_ret, "cagr": cagr, "mdd": mdd, "sharpe": sharpe,
        "win": len(wins) / len(pnls) if pnls else 0,
        "avg_pnl": sum(pnls) / len(pnls) if pnls else 0,
        "pf": gross_w / gross_l if gross_l > 0 else float("inf"),
        "final_eq": eq[-1] if eq else capital0,
    }


def spy_benchmark(d0, d1, base=100000):
    """SPY buy-and-hold over the same window, for honest comparison."""
    try:
        from fastfetch import fetch_daily_fast
        rows = fetch_daily_fast("SPY", d0, d1)
    except Exception as e:  # noqa: BLE001
        print(f"[benchmark] SPY fetch failed: {e}")
        return None
    if not rows:
        return None
    p0 = rows[0]["close"]
    pts = [(r["date"], base * r["close"] / p0) for r in rows]
    eq = [v for _, v in pts]
    total = eq[-1] / base - 1
    yrs = max((pts[-1][0] - pts[0][0]).days / 365.25, 0.01)
    peak, mdd = -1e18, 0.0
    for v in eq:
        peak = max(peak, v)
        mdd = min(mdd, v / peak - 1)
    return {"name": "SPY buy&hold (benchmark)", "equity": pts,
            "total_ret": total, "cagr": (eq[-1] / base) ** (1 / yrs) - 1,
            "mdd": mdd, "final_eq": eq[-1], "capital0": base,
            "sharpe": None, "win": None, "n_taken": 1, "fees": 0.0,
            "benchmark": True, "desc": "S&P 500 ETF, same window"}


def monthly_returns(equity):
    bym = {}
    for d, v in equity:
        bym.setdefault((d.year, d.month), []).append((d, v))
    out = {}
    prev = None
    for k in sorted(bym):
        last_v = bym[k][-1][1]
        if prev is not None and prev > 0:
            out[k] = last_v / prev - 1
        prev = last_v
    return out


# ------------------------------------------------------------------ report -
def fmt_money(v):
    return f"${v:,.0f}"


def build_report(results, path, bench=None):
    L = [f"# Portfolio backtest — {date.today()}",
         "",
         "Actual buy/sell simulation over the historical signal ledger with "
         "position sizing, capital caps and Futubull fees "
         "($0.0049/sh min $0.99 commission + $0.005/sh min $1 platform + "
         "SEC/TAF sell-side + 5bps slippage + 1%/yr short borrow). "
         "Open positions are marked at cost (see caveats).",
         "",
         "| scenario | final equity | total ret | CAGR | max DD* | Sharpe | "
         "win% | trades | skipped | fees paid |",
         "|---|---|---|---|---|---|---|---|---|---|"]
    for r in results:
        L.append(f"| {r['name']} | {fmt_money(r['final_eq'])} | "
                 f"{r['total_ret']:+.1%} | {r['cagr']:+.1%} | "
                 f"{r['mdd']:.1%} | {r['sharpe']:.2f} | {r['win']:.1%} | "
                 f"{r['n_taken']} | {r['n_skip_cash'] + r['n_skip_cap']} | "
                 f"{fmt_money(r['fees'])} |")
    if bench:
        L.append(f"| **{bench['name']}** | {fmt_money(bench['final_eq'])} | "
                 f"**{bench['total_ret']:+.1%}** | **{bench['cagr']:+.1%}** | "
                 f"{bench['mdd']:.1%} | - | - | 1 | 0 | - |")
    L += ["", "\\* drawdown on cost-basis equity — understated for "
          "long-hold tp strategies.", ""]
    for r in results:
        L.append(f"## {r['name']} — {r['desc']}")
        L.append("")
        t = r["trades"]
        if t:
            best = sorted(t, key=lambda p: -p["pnl"])[:3]
            worst = sorted(t, key=lambda p: p["pnl"])[:3]
            L.append(f"fees were {r['fees']/max(sum(p['pnl'] for p in t if p['pnl']>0),1):.1%} "
                     f"of gross winning P&L")
            L.append("")
            L.append("best: " + "; ".join(
                f"{p['ticker']} {p['d_in']} {p['side']} {fmt_money(p['pnl'])}"
                for p in best))
            L.append("")
            L.append("worst: " + "; ".join(
                f"{p['ticker']} {p['d_in']} {p['side']} {fmt_money(p['pnl'])}"
                for p in worst))
            L.append("")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(L) + "\n")


def build_dashboard(results, path, bench=None):
    # weekly-downsampled equity curves keep the html small
    series = {}
    for r in results:
        pts = r["equity"][::5]
        if r["equity"][-1] not in pts:
            pts.append(r["equity"][-1])
        series[r["name"]] = [[str(d), round(v, 0)] for d, v in pts]
    labels = sorted({d for pts in series.values() for d, _ in pts})
    bench_series = None
    bench_card = None
    bench_monthly = None
    if bench:
        bench_series = {str(d): round(v, 0) for d, v in bench["equity"][::2]}
        labels = sorted(set(labels) | set(bench_series))
        bench_card = {kk: (round(vv, 4) if isinstance(vv, float) else vv)
                      for kk, vv in bench.items()
                      if kk in ("name", "desc", "capital0", "final_eq",
                                "total_ret", "cagr", "mdd", "benchmark")}
        bench_monthly = {f"{y}-{m:02d}": round(v, 4) for (y, m), v in
                         monthly_returns(bench["equity"]).items()}
    cards = [{kk: (round(vv, 4) if isinstance(vv, float) else vv)
              for kk, vv in r.items()
              if kk in ("name", "desc", "capital0", "final_eq",
                        "total_ret", "cagr", "mdd", "sharpe", "win",
                        "n_taken", "fees", "pf", "avg_pnl")}
             for r in results]
    if bench_card:
        cards.append(bench_card)
    monthly = {r["name"]: {f"{y}-{m:02d}": round(v, 4)
                           for (y, m), v in
                           monthly_returns(r["equity"]).items()}
               for r in results}
    if bench_monthly:
        monthly["SPY"] = bench_monthly
    data = {
        "labels": labels,
        "series": {k: dict(v) for k, v in series.items()},
        "bench": bench_series,
        "cards": cards,
        "monthly": monthly,
    }
    html = DASH_HTML.replace("__DATA__", json.dumps(data))
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(html)


DASH_HTML = """<!doctype html>
<html><head><meta charset="utf-8"><title>Excel-bot portfolio backtest</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
body{font-family:system-ui,Segoe UI,Arial;margin:24px;background:#0f1115;color:#e8e8ea}
h1{font-size:22px} h2{font-size:16px;margin-top:28px;color:#9ecbff}
.cards{display:flex;flex-wrap:wrap;gap:12px}
.card{background:#181c24;border:1px solid #2a3040;border-radius:10px;padding:12px 14px;min-width:230px}
.card b{font-size:15px} .card .d{color:#8a93a6;font-size:12px;margin:2px 0 8px}
.kv{display:flex;justify-content:space-between;font-size:13px;padding:1px 0}
.pos{color:#4ade80}.neg{color:#f87171}
canvas{background:#12151c;border-radius:10px;padding:8px}
table{border-collapse:collapse;font-size:12px}
td,th{border:1px solid #2a3040;padding:3px 7px;text-align:right}
th{background:#181c24}
</style></head><body>
<h1>Excel-bot portfolio backtest <span style="font-size:13px;color:#8a93a6">
actual buys/sells, Futubull fees, cost-basis marks</span></h1>
<div class="cards" id="cards"></div>
<h2>Equity curves</h2><canvas id="eq" height="110"></canvas>
<h2>Drawdown (cost-basis)</h2><canvas id="dd" height="70"></canvas>
<h2>Monthly returns</h2><div id="mon"></div>
<script>
const D = __DATA__;
const PAL = ["#4ade80","#60a5fa","#f472b6","#facc15","#34d399","#fb923c","#a78bfa"];
const fm = v => "$" + Math.round(v).toLocaleString();
const fp = v => (v>=0?"+":"") + (v*100).toFixed(1) + "%";
const cls = v => v>=0 ? "pos" : "neg";
document.getElementById("cards").innerHTML = D.cards.map((c,i)=>`
<div class="card" style="border-top:3px solid ${c.benchmark?"#e5e7eb":PAL[i%PAL.length]}">
<b>${c.name}</b><div class="d">${c.desc}</div>
<div class="kv"><span>final</span><b>${fm(c.final_eq)}</b></div>
<div class="kv"><span>total / CAGR</span><b class="${cls(c.total_ret)}">${fp(c.total_ret)} / ${fp(c.cagr)}</b></div>
<div class="kv"><span>max DD*</span><b class="neg">${(c.mdd*100).toFixed(1)}%</b></div>
${c.benchmark?`<div class="kv"><span>benchmark</span><b>buy & hold</b></div>`
:`<div class="kv"><span>Sharpe / win</span><b>${c.sharpe.toFixed(2)} / ${(c.win*100).toFixed(0)}%</b></div>
<div class="kv"><span>trades / fees</span><b>${c.n_taken.toLocaleString()} / ${fm(c.fees)}</b></div>`}
</div>`).join("");
function ds(key, map, i, fill){return {label:key,
 data:D.labels.map(l=>map[l] ?? null), borderColor:PAL[i%PAL.length],
 backgroundColor:fill?PAL[i%PAL.length]+"22":undefined, fill:!!fill,
 pointRadius:0, borderWidth:1.5, spanGaps:true, tension:.15};}
function benchDs(fill){return D.bench?[{label:"SPY",
 data:D.labels.map(l=>D.bench[l] ?? null), borderColor:"#e5e7eb",
 borderDash:[6,4], backgroundColor:fill?"#e5e7eb22":undefined, fill:!!fill,
 pointRadius:0, borderWidth:1.5, spanGaps:true, tension:.15}]:[];}
new Chart(eq,{type:"line",data:{labels:D.labels,
 datasets:[...Object.entries(D.series).map(([k,v],i)=>ds(k,v,i)),...benchDs(false)]},
 options:{plugins:{legend:{labels:{color:"#ccc"}}},
 scales:{x:{ticks:{color:"#889",maxTicksLimit:12}},
 y:{ticks:{color:"#889",callback:fm}}}}});
new Chart(dd,{type:"line",data:{labels:D.labels,
 datasets:[...Object.entries(D.series).map(([k,v],i)=>{
  let peak=-1e18;const m={};for(const [d0,val] of Object.entries(v)){
   peak=Math.max(peak,val);m[d0]=val/peak-1;}
  return ds(k,m,i,true);}),
  ...(D.bench?(()=>{let peak=-1e18;const m={};
   for(const [d0,val] of Object.entries(D.bench)){
    peak=Math.max(peak,val);m[d0]=val/peak-1;}
   return [{label:"SPY",data:D.labels.map(l=>m[l]??null),
    borderColor:"#e5e7eb",borderDash:[6,4],pointRadius:0,borderWidth:1.5,
    spanGaps:true,tension:.15}];})():[])]},
 options:{plugins:{legend:{display:false}},
 scales:{x:{ticks:{color:"#889",maxTicksLimit:12}},
 y:{ticks:{color:"#889",callback:v=>(v*100).toFixed(0)+"%"}}}}});
const months=[...new Set(Object.values(D.monthly).flatMap(o=>Object.keys(o)))].sort();
let h="<table><tr><th>month</th>"+Object.keys(D.monthly).map(k=>`<th>${k}</th>`).join("")+"</tr>";
for(const m of months){h+=`<tr><th>${m}</th>`+Object.keys(D.monthly).map(k=>{
 const v=D.monthly[k][m];if(v===undefined)return"<td></td>";
 const g=v>0?Math.min(v*8,0.8):0, r=v<0?Math.min(-v*8,0.8):0;
 return `<td style="background:rgba(${r?248:g?74:0},${r?113:g?222:0},${r?113:g?128:0},${Math.max(r,g)})" class="${cls(v)}">${fp(v)}</td>`;}).join("")+"</tr>";}
document.getElementById("mon").innerHTML=h+"</table>";
</script></body></html>"""


def main():
    trades = load_trades()
    print(f"{len(trades)} signals loaded")
    results = []
    for cfg in SCENARIOS:
        r = run_scenario(cfg, trades)
        results.append(r)
        print(f"{r['name']:22s} eq={fmt_money(r['final_eq']):>12s} "
              f"ret={r['total_ret']:+8.1%} cagr={r['cagr']:+7.1%} "
              f"dd={r['mdd']:7.1%} sharpe={r['sharpe']:5.2f} "
              f"win={r['win']:.1%} n={r['n_taken']} fees={fmt_money(r['fees'])}",
              flush=True)
    d0 = min(t["d_in"] for t in trades)
    d1 = max(t["d_out"] for t in trades)
    bench = spy_benchmark(d0, d1)
    if bench:
        print(f"{'SPY benchmark':22s} eq={fmt_money(bench['final_eq']):>12s} "
              f"ret={bench['total_ret']:+8.1%} cagr={bench['cagr']:+7.1%} "
              f"dd={bench['mdd']:7.1%}", flush=True)
    md = os.path.join(BT_DIR, f"portfolio_{date.today()}.md")
    html = os.path.join(BT_DIR, "dashboard.html")
    build_report(results, md, bench)
    build_dashboard(results, html, bench)
    print(f"wrote {md} and {html}")


if __name__ == "__main__":
    main()
