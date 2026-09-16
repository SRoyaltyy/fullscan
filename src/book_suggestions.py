"""Live 1d BUY/SELL strip for dashboards.

After Pre-Open ALL / Stock Book ALL write the book, this sidecar
(`{date}_suggestions.json` + `latest_suggestions.json`) is one of the
JSON files dashboards poll from raw `main`. The other is
`data/day_board/today.json` (same names, already rebuilt on land).

Pages HTML does not need a same-run `paper_trade` rebuild for names
to appear — the poller in `dashboard/index.html` / `paper_dash.html`
reads those files.
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data" / "stock_book"
POLLER_MARK = "live-book-poller"
TODAY_URL = (
    "https://raw.githubusercontent.com/SRoyaltyy/fullscan/main"
    "/data/day_board/today.json"
)
SUG_URL = (
    "https://raw.githubusercontent.com/SRoyaltyy/fullscan/main"
    "/data/stock_book/latest_suggestions.json"
)
STRAT_URL = (
    "https://raw.githubusercontent.com/SRoyaltyy/fullscan/main"
    "/data/day_board/today_strategies.json"
)
HELD_URL = (
    "https://raw.githubusercontent.com/SRoyaltyy/fullscan/main"
    "/data/factor_mine/held_live.json"
)

_POLLER_HTML = """
<div id="liveBook" class="live-book" data-live-book="1">
  <div class="live-book-kicker">Today's book (live from main)</div>
  <div class="live-book-body">Loading book…</div>
</div>
"""

_POLLER_CSS = """
.live-book{background:#16161e;border:1px solid #2a2a3a;border-radius:10px;padding:12px 16px;margin:0 0 16px}
.live-book-kicker{font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:#7a7a90;margin-bottom:8px}
.live-book-date{font-size:12px;color:#9a9ab0;margin-bottom:6px}
.live-book-row{font-size:14px;line-height:1.45;margin:4px 0}
.live-book-row b{color:#7aa2f7;margin-right:8px}
.live-book-row.sell b{color:#f7768e}
"""

_POLLER_JS = r"""
<script id="live-book-poller">
(function(){
  var TODAY = "https://raw.githubusercontent.com/SRoyaltyy/fullscan/main/data/day_board/today.json";
  var STRAT = "https://raw.githubusercontent.com/SRoyaltyy/fullscan/main/data/day_board/today_strategies.json";
  var SUG = "https://raw.githubusercontent.com/SRoyaltyy/fullscan/main/data/stock_book/latest_suggestions.json";
  var HELD = "https://raw.githubusercontent.com/SRoyaltyy/fullscan/main/data/factor_mine/held_live.json";
  function quoteLabel(q){
    if(!q) return "";
    var src = q.src || "";
    if(!src && !q.at) return "";
    var banned = /theme|radar|_close|yesterday/i.test(src);
    var live = !banned && (src === "elite_live" || src === "elite_live_file");
    var bit = " · px " + (src || "?") + (live ? " (now)" : " (not live)");
    if(banned) bit += " — Theme Radar/close refused";
    if(q.at) bit += " @ " + q.at;
    return bit;
  }
  function names(rows, preds){
    var by = {};
    (preds || []).forEach(function(p){
      if(p && p.ticker) by[String(p.ticker).toUpperCase()] = p;
    });
    return (rows || []).map(function(x){
      var t, px, op;
      if(typeof x === "string"){
        t = x;
        var p = by[t.toUpperCase()] || {};
        px = p.px; op = p.open_px;
      } else {
        t = (x && (x.ticker || x.symbol)) || "";
        px = x && x.px; op = x && x.open_px;
        var hit = t ? by[String(t).toUpperCase()] : null;
        if(px == null && hit) px = hit.px;
        if(op == null && hit) op = hit.open_px;
      }
      if(!t) return "";
      var bit = t;
      if(px != null) bit += " " + px;
      if(op != null && op !== px) bit += " (open " + op + ")";
      return bit;
    }).filter(Boolean);
  }
  function paint(d){
    var el = document.getElementById("liveBook");
    if(!el || !d) return;
    var buys = names(d.buy_1d);
    var sells = names(d.sell_1d);
    var extra = "";
    var strats = d.strategies || {};
    var keys = Object.keys(strats);
    if(keys.length){
      extra += '<div class="live-book-date">' + keys.length + ' strategies</div>';
      keys.forEach(function(name){
        var s = strats[name] || {};
        var b = names(s.buy, s.predict);
        var sl = names(s.sell, s.predict);
        var sit = !!(s.sit || s.would_have || s.status === "sit");
        extra += '<div class="live-book-row' + (sl.length && !b.length ? " sell" : "") + '"><b>' + name + '</b> ';
        var side = String(s.side || "").toUpperCase();
        if(side) extra += side + " ";
        if(sit) extra += "SIT would-have ";
        else if(s.status && s.status !== "ok") extra += s.status + " ";
        extra += (b.length ? (sit ? "" : "BUY ") + b.join(" · ") : "");
        extra += (b.length && sl.length ? " · " : "");
        extra += (sl.length ? (sit ? "would-sell " : "SELL ") + sl.join(" · ") : "");
        if(!b.length && !sl.length) extra += (sit ? "sit" : "—");
        var res = s.research;
        if(res && res.tag){
          extra += ' <span class="live-book-date">RESEARCH';
          var so = res.short_only || [];
          var dp = res.dip_scoop || [];
          if(so.length){
            extra += " (A) short-only " + so.map(function(r){
              return (r.ticker || "") + (r.open != null ? (" @" + r.open) : "");
            }).filter(Boolean).join(" · ");
          }
          if(dp.length){
            extra += " (B) dip-scoop " + dp.map(function(r){
              var hits = [];
              var sc = r.scoops || {};
              Object.keys(sc).forEach(function(x){
                if((sc[x] || {}).kind === "scoop") hits.push(x + "%");
              });
              return (r.ticker || "") + (hits.length ? (" scoop " + hits.join(",")) : " miss");
            }).filter(Boolean).join(" · ");
          }
          extra += " — not a wire</span>";
        }
        var preds = s.predict || [];
        if(preds.length){
          extra += " · ";
          extra += preds.map(function(p){
            var t = (p && p.ticker) || "";
            var dir = (p && p.predict) || "";
            return t ? (t + " " + dir) : "";
          }).filter(Boolean).join(" · ");
        }
        extra += "</div>";
      });
    }
    el.innerHTML =
      '<div class="live-book-kicker">Today\'s tickets (every strategy) — SIT = paper would-have, not a wire. Elite px after 09:30 only; Theme Radar close never live</div>' +
      '<div class="live-book-date">' + (d.date || "") +
        (d.clock_legal_for ? (" · open " + d.clock_legal_for) : "") +
        quoteLabel(d.quote) + '</div>' +
      '<div class="live-book-row"><b>BUY 1d</b> ' + (buys.join(" · ") || "—") + '</div>' +
      '<div class="live-book-row sell"><b>SELL 1d</b> ' + (sells.join(" · ") || "—") + '</div>' + extra;
  }
  function paintHolds(h){
    if(!h) return;
    window.__HELD_LIVE = h;
    var el = document.getElementById("liveBook");
    if(!el) return;
    var sleeves = h.sleeves || {};
    var keys = Object.keys(sleeves);
    if(!keys.length && !(h.prices)) return;
    var box = document.getElementById("heldLiveStrip");
    if(!box){
      box = document.createElement("div");
      box.id = "heldLiveStrip";
      box.className = "live-book-date";
      el.appendChild(box);
    }
    var bits = [];
    keys.forEach(function(name){
      (sleeves[name] || []).forEach(function(r){
        if(!r || !r.ticker) return;
        var bit = r.ticker + " " + (r.side || "long") + " ×" + (r.shares || "?");
        if(r.px != null) bit += " @" + r.px;
        if(r.pnl != null) bit += " P/L $" + r.pnl;
        bits.push(bit);
      });
    });
    box.textContent = (h.banner || "Elite Overview") +
      (bits.length ? (" — open lots now: " + bits.join(" · ")) : " — no open lots now");
  }
  function load(){
    var rels = [
      "today_strategies.json",
      "factor-mine/today_strategies.json",
      "../factor-mine/today_strategies.json"
    ].map(function(u){
      return fetch(u + "?t=" + Date.now(), {cache: "no-store"}).then(function(r){ return r.ok ? r.json() : {}; }).catch(function(){ return {}; });
    });
    Promise.all([
      fetch(TODAY + "?t=" + Date.now(), {cache: "no-store"}).then(function(r){ return r.ok ? r.json() : {}; }).catch(function(){ return {}; }),
      fetch(STRAT + "?t=" + Date.now(), {cache: "no-store"}).then(function(r){ return r.ok ? r.json() : {}; }).catch(function(){ return {}; }),
      fetch(SUG + "?t=" + Date.now(), {cache: "no-store"}).then(function(r){ return r.ok ? r.json() : {}; }).catch(function(){ return {}; }),
      fetch(HELD + "?t=" + Date.now(), {cache: "no-store"}).then(function(r){ return r.ok ? r.json() : null; }).catch(function(){ return null; })
    ].concat(rels)).then(function(arr){
      var today = arr[0] || {}, strat = arr[1] || {}, sug = arr[2] || {}, held = arr[3];
      var local = arr.slice(4).filter(function(x){ return x && x.strategies && Object.keys(x.strategies).length; })[0];
      if(local && local.strategies) strat = local;
      var d = Object.assign({}, sug, today);
      if(strat && strat.strategies) d.strategies = strat.strategies;
      if(strat && strat.date) d.date = strat.date;
      if(strat && strat.n) d.n_strategies = strat.n;
      if(strat && strat.quote) d.quote = strat.quote;
      if(strat && strat.clock_legal_for) d.clock_legal_for = strat.clock_legal_for;
      if(!d.buy_1d && sug.buy_1d) d.buy_1d = sug.buy_1d;
      if(!d.sell_1d && sug.sell_1d) d.sell_1d = sug.sell_1d;
      paint(d);
      paintHolds(held);
    });
  }
  load();
  setInterval(load, 60000);
})();
</script>
"""


def _row_ticker_score(row: object) -> dict:
    if isinstance(row, dict):
        ticker = str(row.get("ticker") or row.get("symbol") or "").upper()
        score = row.get("score")
        if score is None:
            score = row.get("total")
        return {"ticker": ticker, "score": score}
    return {"ticker": str(row).upper(), "score": None}


def suggestions_from_book(book: dict) -> dict:
    from .stock_book_diag_signals import _horizon_rows

    date = str(book.get("date") or (book.get("meta") or {}).get("date") or "")
    horizons: dict = {}
    for h in ("1d", "3d", "1w", "2w", "1m"):
        buys, sells = _horizon_rows(book, h)
        horizons[h] = {
            "buy": [_row_ticker_score(r) for r in (buys or [])[:15]],
            "sell": [_row_ticker_score(r) for r in (sells or [])[:15]],
        }
    return {
        "date": date,
        "horizons": horizons,
        "buy_1d": [x["ticker"] for x in horizons["1d"]["buy"] if x["ticker"]],
        "sell_1d": [x["ticker"] for x in horizons["1d"]["sell"] if x["ticker"]],
    }


def write(book: dict | None = None, date: str | None = None) -> Path | None:
    """Write dated + latest suggestion sidecars next to the stock book."""
    if (book or {}).get("meta", {}).get("degraded"):
        print("  book suggestions skipped — degraded book", flush=True)
        return None
    if book is None:
        from datetime import datetime
        from zoneinfo import ZoneInfo

        from . import config

        date = date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()
        path = DATA / f"{date}_stock_book.json"
        if not path.exists():
            return None
        book = json.loads(path.read_text(encoding="utf-8"))
    date = str(book.get("date") or (book.get("meta") or {}).get("date") or date or "")
    if not date:
        return None
    payload = suggestions_from_book({**book, "date": date})
    text = json.dumps(payload, indent=2)
    DATA.mkdir(parents=True, exist_ok=True)
    dated = DATA / f"{date}_suggestions.json"
    latest = DATA / "latest_suggestions.json"
    dated.write_text(text, encoding="utf-8")
    latest.write_text(text, encoding="utf-8")
    print(
        f"  book suggestions → {dated.name}  "
        f"buy_1d={payload['buy_1d']} sell_1d={payload['sell_1d']}",
        flush=True,
    )
    return dated


LIVE_BOARD_HTML = (
    ROOT / "dashboard" / "index.html",
    ROOT / "dashboard" / "sleeve-merge" / "index.html",
    ROOT / "dashboard" / "strategy-board" / "index.html",
)


def ensure_dashboard_poller(html_path: Path | None = None) -> bool:
    """Inject the live-book poller into baked paper HTML if missing.

    After this snippet is on gh-pages once, later book lands update
    suggestions via raw JSON — no paper_trade rebuild required.
    """
    path = html_path or (ROOT / "dashboard" / "index.html")
    if not path.exists():
        return False
    text = path.read_text(encoding="utf-8", errors="replace")
    if POLLER_MARK in text:
        import re
        nxt = re.sub(
            r'<script id="live-book-poller">[\s\S]*?</script>',
            _POLLER_JS.strip(),
            text,
            count=1,
        )
        if nxt != text:
            path.write_text(nxt, encoding="utf-8")
            print(f"  refreshed live-book poller → {path}", flush=True)
            return True
        return False
    if '<div class="wrap">' in text:
        text = text.replace(
            '<div class="wrap">',
            '<div class="wrap">' + _POLLER_HTML,
            1,
        )
    elif "<main>" in text:
        text = text.replace("<main>", "<main>" + _POLLER_HTML, 1)
    elif "<body>" in text:
        text = text.replace("<body>", "<body>" + _POLLER_HTML, 1)
    else:
        return False
    if "</style>" in text:
        text = text.replace("</style>", _POLLER_CSS + "\n</style>", 1)
    if "</body>" in text:
        text = text.replace("</body>", _POLLER_JS + "\n</body>", 1)
    else:
        text += _POLLER_JS
    path.write_text(text, encoding="utf-8")
    print(f"  injected live-book poller → {path}", flush=True)
    return True


def ensure_live_board_pollers() -> list[str]:
    """Keep .io / sleeve-merge / strategy-board polling today_strategies."""
    wrote = []
    for path in LIVE_BOARD_HTML:
        if ensure_dashboard_poller(path):
            try:
                wrote.append(str(path.relative_to(ROOT)))
            except ValueError:
                wrote.append(str(path))
    return wrote
