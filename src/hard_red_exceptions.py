"""Daily investigator list — names weather sit sat on.

Cyrus: even on hard-red mornings (S≤−3) some names still have enough
idiosyncratic cameras / E / news / sector to be a net gain. Live
flatten_robust stay full sit. This board ranks every 09:30 looker
(the investigator card, plus extra tape) so those names are not only
visible after a click.

Universe = factor-mine panel rows that session (shopping lists), not
the 11k Finviz dump. LLM coaches are not invented for names flatten
never saw.

Does not change live sit / Webull / flatten_robust.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from . import factor_mine as fm
from . import factor_mine_book as fmb
from . import factor_mine_probe as fmp
from . import ticker_lookback as tl

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "data" / "hard_red_exceptions"
DASH_DIR = ROOT / "dashboard" / "hard-red-exceptions"
OUT_MD = ROOT / "03_scoreboard" / "HARD_RED_EXCEPTIONS.md"
TOP_N = 8
HOLDS = (1, 3, 5)

# Weather / market cameras — not idiosyncratic. Still shown, not scored
# as "this name is strong."
WEATHER_CAMS = ("gen", "sect", "sector")


def _tone(card: dict, key: str) -> str:
    boxes = card.get("boxes") or {}
    return str(boxes.get(key) or "missing").lower()


def idio_parts(card: dict) -> list[dict]:
    """Clock-clean score pieces. Gen/sect are labeled weather, not idio."""
    parts: list[dict] = []
    good = int(card.get("cond_good") or 0)
    bad = int(card.get("cond_bad") or 0)
    parts.append({
        "k": "cameras", "pts": 2 * (good - bad),
        "why": f"+{good} −{bad} cameras",
    })
    e = str(card.get("e_pol") or "missing")
    if e == "good":
        parts.append({"k": "E", "pts": 4, "why": card.get("e_label") or "E beat"})
    elif e == "bad":
        parts.append({"k": "E", "pts": -4, "why": card.get("e_label") or "E miss"})
    r = str(card.get("r_pol") or "missing")
    if r == "good":
        parts.append({"k": "R", "pts": 2, "why": card.get("r_label") or "R up"})
    elif r == "bad":
        parts.append({"k": "R", "pts": -2, "why": card.get("r_label") or "R down"})
    news = str((card.get("news") or {}).get("tone") or "missing")
    if news == "good":
        parts.append({"k": "news", "pts": 2, "why": "news camera green"})
    elif news == "bad":
        parts.append({"k": "news", "pts": -2, "why": "news camera red"})
    if _tone(card, "join") == "good":
        parts.append({"k": "join", "pts": 1, "why": "join green"})
    elif _tone(card, "join") == "bad":
        parts.append({"k": "join", "pts": -1, "why": "join red"})
    if _tone(card, "peer") == "good":
        parts.append({"k": "peer", "pts": 1, "why": "peer green"})
    elif _tone(card, "peer") == "bad":
        parts.append({"k": "peer", "pts": -1, "why": "peer red"})
    if _tone(card, "catal") == "good" or card.get("earn_react"):
        parts.append({"k": "catalyst", "pts": 2, "why": "catalyst / earn-react"})
    y = fm._finite(card.get("yday_ret"))
    if y is not None:
        if y > 0:
            parts.append({"k": "yday", "pts": 1, "why": f"yday {y:+.1f}%"})
        elif y < 0:
            parts.append({"k": "yday", "pts": -1, "why": f"yday {y:+.1f}%"})
    elif card.get("last_green"):
        parts.append({"k": "yday", "pts": 1, "why": "last bar green"})
    elif card.get("last_red"):
        parts.append({"k": "yday", "pts": -1, "why": "last bar red"})
    if card.get("white"):
        parts.append({"k": "white", "pts": 2, "why": "zero red cameras"})
    if card.get("alarm"):
        parts.append({"k": "alarm", "pts": -3, "why": "alarm overnight"})
    if card.get("flow_in"):
        parts.append({"k": "flow", "pts": 1, "why": "FLOW IN"})
    if card.get("macd_up") or card.get("macd_cross_up"):
        parts.append({"k": "macd", "pts": 1, "why": "MACD hist + / X+"})
    if card.get("burst"):
        parts.append({"k": "burst", "pts": 1, "why": "parabolic prior tape"})
    # RSI crash-long / melt-up-short is a pause, not a weather sit.
    rsi = fm._finite(card.get("rsi"))
    if card.get("rsi_ob") or (rsi is not None and rsi >= 75):
        parts.append({"k": "rsi", "pts": -2, "why": f"RSI {rsi:.0f} OB — pause longs"})
    if card.get("rsi_os") or (rsi is not None and rsi <= 25):
        parts.append({"k": "rsi", "pts": 1, "why": f"RSI {rsi:.0f} OS — long-friendly"})
    return parts


def idio_score(card: dict) -> dict:
    parts = idio_parts(card)
    total = int(sum(int(p["pts"]) for p in parts))
    weather = []
    for k in ("gen", "sect", "sector"):
        t = _tone(card, k)
        if t in ("good", "bad"):
            weather.append(f"{k} {t}")
    return {
        "score": total,
        "parts": parts,
        "weather": weather,
        "n_pos": int(card.get("cond_good") or 0),
        "n_neg": int(card.get("cond_bad") or card.get("n_neg") or 0),
    }


def long_ok(card: dict, pack: dict) -> bool:
    """Idiosyncratic long — not merely 'weather was red'."""
    if pack["score"] < 3:
        return False
    if card.get("alarm"):
        return False
    rsi = fm._finite(card.get("rsi"))
    if rsi is not None and rsi >= 80:
        return False
    if int(card.get("cond_bad") or 0) >= 5:
        return False
    return True


def short_ok(card: dict, pack: dict) -> bool:
    if pack["score"] > -3:
        return False
    if int(card.get("cond_good") or 0) >= 5:
        return False
    return True


def why_still(card: dict, pack: dict, *, side: str, hard: bool, s) -> list[str]:
    lines = []
    if hard:
        lines.append(
            f"Weather S={s} ≤ {fmb.HARD_RED:g} — live sleeve SITS. "
            "This name is ranked anyway so idiosyncratic green is not invisible."
        )
    else:
        lines.append(f"Weather S={s}. Not a sit morning — still ranked.")
    top = sorted(pack["parts"], key=lambda p: -abs(int(p["pts"])))[:6]
    for p in top:
        sign = "+" if p["pts"] > 0 else ""
        lines.append(f"{sign}{p['pts']} {p['why']}")
    if pack.get("weather"):
        lines.append("Weather cameras (not in idio score): " + ", ".join(pack["weather"]))
    news = card.get("news") or {}
    if news.get("title"):
        lines.append(f"News: {news.get('tone')} — {news['title'][:120]}")
    if card.get("e_label"):
        lines.append(card["e_label"])
    rsi = fm._finite(card.get("rsi"))
    if rsi is not None:
        lines.append(
            f"Tape RSI {rsi:.0f}"
            f"{' OS' if card.get('rsi_os') else ''}"
            f"{' OB' if card.get('rsi_ob') else ''}"
            f" · MACD hist {card.get('macd_hist')}"
        )
    lines.append(f"Side lean: {side} · idio {pack['score']:+d}")
    return lines


def _grade(ticker: str, date: str, hold: int, side: str, cal: list[str], bars):
    try:
        return fm.hold_return(ticker, date, hold, cal, side, None, {}, bars)
    except Exception:
        return None


def rank_day(date: str, cards: dict, *, s, hard: bool,
             cal: list[str], bars) -> dict:
    longs, shorts, all_rows = [], [], []
    for ticker, card in (cards or {}).items():
        pack = idio_score(card)
        rec = {
            "ticker": ticker,
            "date": date,
            "idio": pack["score"],
            "n_pos": pack["n_pos"],
            "n_neg": pack["n_neg"],
            "e_pol": card.get("e_pol"),
            "e_label": card.get("e_label"),
            "r_pol": card.get("r_pol"),
            "news_tone": (card.get("news") or {}).get("tone"),
            "headline": card.get("headline") or (card.get("news") or {}).get("title"),
            "yday_ret": card.get("yday_ret"),
            "rsi": card.get("rsi"),
            "macd_hist": card.get("macd_hist"),
            "flow_in": bool(card.get("flow_in")),
            "white": bool(card.get("white")),
            "alarm": bool(card.get("alarm")),
            "on_list": bool(card.get("on_list")),
            "sources": card.get("sources") or [],
            "open": card.get("open"),
            "boxes": card.get("boxes") or {},
            "domains": card.get("domains"),
            "parts": pack["parts"],
            "weather": pack["weather"],
            "h1": None, "h3": None, "h5": None,
        }
        all_rows.append(rec)
        if long_ok(card, pack):
            rec_l = dict(rec, side="long",
                         why=why_still(card, pack, side="long", hard=hard, s=s))
            for h in HOLDS:
                rec_l[f"h{h}"] = _grade(ticker, date, h, "long", cal, bars)
            longs.append(rec_l)
        if short_ok(card, pack):
            rec_s = dict(rec, side="short",
                         why=why_still(card, pack, side="short", hard=hard, s=s))
            for h in HOLDS:
                rec_s[f"h{h}"] = _grade(ticker, date, h, "short", cal, bars)
            shorts.append(rec_s)
    longs.sort(key=lambda r: (-r["idio"], -r["n_pos"], r["ticker"]))
    shorts.sort(key=lambda r: (r["idio"], r["n_neg"], r["ticker"]))
    all_rows.sort(key=lambda r: (-r["idio"], r["ticker"]))
    return {
        "date": date,
        "s": s,
        "hard_red": hard,
        "n_cards": len(cards or {}),
        "longs": longs[:TOP_N],
        "shorts": shorts[:TOP_N],
        "n_long_ok": len(longs),
        "n_short_ok": len(shorts),
        "all_top": all_rows[:40],
    }


def _pack_rets(vals: list) -> dict:
    xs = [float(v) for v in vals if v is not None]
    if not xs:
        return {"n": 0, "win": None, "mean": None}
    wins = sum(1 for x in xs if x > 0)
    return {
        "n": len(xs),
        "win": round(wins / len(xs), 4),
        "mean": round(sum(xs) / len(xs), 3),
    }


def backtest(days: list[dict]) -> dict:
    """If we had taken top longs/shorts on hard-red sits, hold-1/3."""
    red = [d for d in days if d.get("hard_red")]
    out = {"n_hard_red": len(red), "long_h1": {}, "long_h3": {},
           "short_h1": {}, "short_h3": {}}
    for side, key in (("longs", "long"), ("shorts", "short")):
        for h in (1, 3):
            vals = []
            for d in red:
                for rec in d.get(side) or []:
                    vals.append(rec.get(f"h{h}"))
            out[f"{key}_h{h}"] = _pack_rets(vals)
    return out


def run(*, panel: dict | None = None, probe: dict | None = None,
        mornings: dict | None = None) -> dict:
    if panel is None:
        if not fm.PANEL_PATH.is_file():
            return {"ok": False, "error": "no panel.json"}
        panel = json.loads(fm.PANEL_PATH.read_text(encoding="utf-8"))
    probe = probe if probe is not None else fmp.build_probe(panel)
    mornings = mornings if mornings is not None else fmp.build_mornings()
    cal = list(panel.get("session_dates") or [])
    bars = None
    try:
        bars = fm._load_bars() if hasattr(fm, "_load_bars") else None
    except Exception:
        bars = None
    days = []
    for date in cal or sorted(probe):
        morn = mornings.get(date) or {}
        s = morn.get("s")
        try:
            hard = bool(morn.get("hard_red") or (
                s is not None and float(s) <= float(fmb.HARD_RED)))
        except (TypeError, ValueError):
            hard = False
        cards = probe.get(date) or {}
        days.append(rank_day(date, cards, s=s, hard=hard, cal=cal, bars=bars))
    bt = backtest(days)
    latest = next((d for d in reversed(days) if d.get("n_cards")), None)
    return {
        "ok": True,
        "generated_at": datetime.now(tl.ET).isoformat(),
        "from_date": cal[0] if cal else None,
        "to_date": cal[-1] if cal else None,
        "n_days": len(days),
        "top_n": TOP_N,
        "live_sit_untouched": True,
        "backtest": bt,
        "latest": latest,
        "days": days,
        "note": (
            "Live sleeves still SIT on S≤−3. This list is the investigator "
            "card for every looker that morning, ranked by idiosyncratic "
            "score so hard-red does not hide E-beats / green cameras."
        ),
    }


def write_md(doc: dict) -> Path:
    bt = doc.get("backtest") or {}
    latest = doc.get("latest") or {}
    lines = [
        "# Hard-red exceptions — daily investigator list",
        "",
        doc.get("note") or "",
        "",
        f"Generated {doc.get('generated_at')} · "
        f"{doc.get('from_date')} → {doc.get('to_date')} · "
        f"live sit **untouched**.",
        "",
        "## Latest morning",
        "",
        f"- Date **{latest.get('date')}** S={latest.get('s')} "
        f"hard-red={latest.get('hard_red')} · "
        f"{latest.get('n_cards')} investigator cards · "
        f"{latest.get('n_long_ok')} long-ok · {latest.get('n_short_ok')} short-ok",
        "",
        "### Longs weather sat on (top)",
        "",
    ]
    for rec in latest.get("longs") or []:
        lines.append(
            f"- **{rec['ticker']}** idio {rec['idio']:+d} · "
            f"+{rec['n_pos']} −{rec['n_neg']} · E {rec.get('e_pol')} · "
            f"H1 {rec.get('h1')}% H3 {rec.get('h3')}%"
        )
    lines += [
        "",
        "### Shorts weather sat on (top)",
        "",
    ]
    for rec in latest.get("shorts") or []:
        lines.append(
            f"- **{rec['ticker']}** idio {rec['idio']:+d} · "
            f"+{rec['n_pos']} −{rec['n_neg']} · H1 {rec.get('h1')}%"
        )
    def _bt(lab, pack):
        if not pack or not pack.get("n"):
            return f"- {lab}: no graded names yet"
        win = pack.get("win")
        return (
            f"- {lab}: n={pack['n']} win="
            f"{None if win is None else round(100*win, 1)}% "
            f"mean {pack.get('mean')}%"
        )
    lines += [
        "",
        "## If we had taken them on hard-red sits (research)",
        "",
        f"- Hard-red mornings in window: {bt.get('n_hard_red')}",
        _bt("long hold-1", bt.get("long_h1")),
        _bt("long hold-3", bt.get("long_h3")),
        _bt("short hold-1", bt.get("short_h1")),
        _bt("short hold-3", bt.get("short_h3")),
        "",
        "KEEP still wants >55% after fees and n≥30. Thin n is not a wire.",
        "",
        f"Dashboard: [hard-red-exceptions](../dashboard/hard-red-exceptions/).",
        "",
    ]
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")
    return OUT_MD


def write_json(doc: dict) -> Path:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    slim = dict(doc)
    # Keep days but drop all_top bulk from archived days except latest.
    days = []
    latest_date = (doc.get("latest") or {}).get("date")
    for d in doc.get("days") or []:
        row = dict(d)
        if d.get("date") != latest_date:
            row.pop("all_top", None)
        days.append(row)
    slim["days"] = days
    path = OUT_DIR / "latest.json"
    path.write_text(json.dumps(slim, indent=2, default=str), encoding="utf-8")
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    (DASH_DIR / "latest.json").write_text(path.read_text(encoding="utf-8"),
                                          encoding="utf-8")
    return path


def write_html() -> Path:
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    dest = DASH_DIR / "index.html"
    dest.write_text(DASH_HTML, encoding="utf-8")
    return dest


DASH_HTML = r"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Hard-red exceptions — daily investigator list</title>
<meta http-equiv="refresh" content="120">
<style>
 :root{--bg:#0f1420;--card:#171e2e;--line:#262f45;--fg:#dfe6f2;--mut:#8b96ab;
  --pos:#4ade80;--neg:#f87171;--gold:#fbbf24;--day:#121826}
 *{box-sizing:border-box}
 html,body{margin:0;background:var(--bg);color:var(--fg);
   font:14px/1.45 -apple-system,Segoe UI,Roboto,sans-serif}
 .wrap{max-width:1100px;margin:0 auto;padding:16px 14px 48px}
 h1{font-size:20px;margin:0 0 4px}
 .sub{color:var(--mut);font-size:12px;margin:0 0 12px}
 .sub a{color:#93c5fd}
 .cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:8px;margin:0 0 14px}
 .card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:10px 12px}
 .card b{display:block;font-size:18px;margin-top:2px;font-variant-numeric:tabular-nums}
 .pos{color:var(--pos)}.neg{color:var(--neg)}.mut{color:var(--mut)}.gold{color:var(--gold)}
 .name{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px;margin:0 0 10px}
 .name h3{margin:0 0 6px;font:800 18px/1.2 ui-monospace,Menlo,monospace}
 .why{margin:0;padding-left:18px;color:#c5d0e6;font-size:13px}
 .cam{display:inline-block;margin:2px 4px 2px 0;padding:2px 8px;border:1px solid var(--line);border-radius:999px;font:11px ui-monospace,Menlo,monospace}
 .banner{font:800 22px/1.2 system-ui;text-align:center;padding:10px;border-radius:10px;margin:0 0 8px}
 .banner.sit{background:#7f1d1d;color:#fecaca}
 .banner.ok{background:#14532d;color:#86efac}
 .cols{display:grid;grid-template-columns:1fr 1fr;gap:12px}
 @media(max-width:800px){.cols{grid-template-columns:1fr}}
</style></head><body><div class="wrap">
<h1>Daily investigator list</h1>
<div class="sub">Same card as the stock investigator, for <b>every looker</b> that morning — not one click.
Hard-red sit still blocks live buys. This page ranks who had enough idiosyncratic green (E / cameras / news / peer)
that sitting on the whole tape may have been the miss. Live flatten_robust untouched.
 · <a href="../factor-mine/">factor mine</a>
 · <a href="../day-board/">day board</a></div>
<div class="cards" id="cards"></div>
<div id="banner"></div>
<div class="cols">
  <div><h2>Longs sit hid</h2><div id="longs"></div></div>
  <div><h2>Shorts sit hid</h2><div id="shorts"></div></div>
</div>
<h2>Top 40 by idio score (this morning)</h2>
<div id="all"></div>
<p class="mut" id="note"></p>
</div>
<script>
const RAW = "https://raw.githubusercontent.com/SRoyaltyy/fullscan/main/data/hard_red_exceptions/latest.json";
function esc(s){return String(s||"").replace(/[&<>]/g,c=>({'&':'&','<':'<','>':'>'}[c]));}
function cls(n){return n==null?'mut':(Number(n)>0?'pos':(Number(n)<0?'neg':'mut'));}
function hcell(v){return v==null?'—':((v>=0?'+':'')+Number(v).toFixed(2)+'%');}
function boxpills(boxes){
  return Object.entries(boxes||{}).map(([k,v])=>{
    const t=String(v||'missing');
    const c=t==='good'?'pos':(t==='bad'?'neg':'mut');
    return `<span class="cam ${c}">${esc(k)} ${esc(t)}</span>`;
  }).join('');
}
function nameCard(r){
  const side=r.side||'';
  return `<div class="name">
    <h3>${esc(r.ticker)} <span class="${cls(r.idio)}">${r.idio>=0?'+':''}${esc(r.idio)}</span>
      <span class="mut">${esc(side)}</span></h3>
    <div class="mut">+${esc(r.n_pos)} −${esc(r.n_neg)} · E ${esc(r.e_pol)} · RSI ${esc(r.rsi??'—')}
      · H1 <span class="${cls(r.h1)}">${hcell(r.h1)}</span>
      · H3 <span class="${cls(r.h3)}">${hcell(r.h3)}</span>
      · H5 <span class="${cls(r.h5)}">${hcell(r.h5)}</span></div>
    <div style="margin:6px 0">${boxpills(r.boxes)}</div>
    <ul class="why">${(r.why||[]).slice(0,10).map(x=>'<li>'+esc(x)+'</li>').join('')}</ul>
    ${r.headline?`<div class="mut">${esc(r.headline)}</div>`:''}
  </div>`;
}
function rowLine(r){
  return `<div class="name" style="padding:8px 10px">
    <b>${esc(r.ticker)}</b>
    <span class="${cls(r.idio)}"> idio ${r.idio>=0?'+':''}${esc(r.idio)}</span>
    · +${esc(r.n_pos)} −${esc(r.n_neg)} · E ${esc(r.e_pol)}
    · yday ${esc(r.yday_ret??'—')} · RSI ${esc(r.rsi??'—')}
    ${r.headline?(' · '+esc(r.headline).slice(0,80)):''}
  </div>`;
}
async function boot(){
  let d;
  try{
    const r=await fetch(RAW+'?t='+Date.now(),{cache:'no-store'});
    if(!r.ok) throw new Error(r.status);
    d=await r.json();
  }catch(e){
    document.getElementById('banner').innerHTML='<div class="mut">No latest.json yet — wait for the daily job.</div>';
    return;
  }
  const L=d.latest||{};
  const bt=d.backtest||{};
  document.getElementById('cards').innerHTML=[
    ['Session', L.date||'—', ''],
    ['Weather S', L.s==null?'—':L.s, Number(L.s)<=-3?'neg':'pos'],
    ['Cards', L.n_cards??'—', ''],
    ['Long-ok', L.n_long_ok??'—', 'pos'],
    ['Short-ok', L.n_short_ok??'—', 'neg'],
    ['Hard-red days', bt.n_hard_red??'—', ''],
  ].map(([k,v,c])=>`<div class="card">${k}<b class="${c}">${esc(v)}</b></div>`).join('');
  document.getElementById('banner').innerHTML = L.hard_red
    ? `<div class="banner sit">SIT morning — live book bought nobody. Names below still had idiosyncratic green.</div>`
    : `<div class="banner ok">Not a sit morning. List is still the full investigator rank.</div>`;
  document.getElementById('longs').innerHTML=(L.longs||[]).map(nameCard).join('')||'<div class="mut">No long-ok names.</div>';
  document.getElementById('shorts').innerHTML=(L.shorts||[]).map(nameCard).join('')||'<div class="mut">No short-ok names.</div>';
  document.getElementById('all').innerHTML=(L.all_top||[]).map(rowLine).join('');
  const lh=bt.long_h1||{};
  document.getElementById('note').textContent =
    (d.note||'') + ' Research long hold-1 on sit days: n='+(lh.n||0)+
    ' win='+(lh.win==null?'—':Math.round(100*lh.win)+'%')+
    ' mean='+(lh.mean==null?'—':lh.mean+'%')+'. Not a live wire.';
}
boot();
</script></body></html>
"""


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--write", action="store_true")
    args = p.parse_args(argv)
    doc = run()
    if args.write and doc.get("ok"):
        write_json(doc)
        write_md(doc)
        write_html()
        print("wrote", OUT_DIR / "latest.json", "latest",
              (doc.get("latest") or {}).get("date"),
              "longs", (doc.get("latest") or {}).get("n_long_ok"))
    else:
        print(json.dumps({
            "ok": doc.get("ok"),
            "latest": (doc.get("latest") or {}).get("date"),
            "hard_red": (doc.get("latest") or {}).get("hard_red"),
            "n_long_ok": (doc.get("latest") or {}).get("n_long_ok"),
            "backtest": doc.get("backtest"),
            "error": doc.get("error"),
        }, indent=2, default=str))
    return 0 if doc.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
