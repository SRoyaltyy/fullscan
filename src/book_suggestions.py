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
  var URLS = [
    "https://raw.githubusercontent.com/SRoyaltyy/fullscan/main/data/day_board/today.json",
    "https://raw.githubusercontent.com/SRoyaltyy/fullscan/main/data/stock_book/latest_suggestions.json"
  ];
  function names(rows){
    if(!rows) return [];
    return rows.map(function(x){
      if(typeof x === "string") return x;
      return (x && (x.ticker || x.symbol)) || "";
    }).filter(Boolean);
  }
  function paint(d){
    var el = document.getElementById("liveBook");
    if(!el || !d) return;
    var buys = names(d.buy_1d);
    var sells = names(d.sell_1d);
    el.innerHTML =
      '<div class="live-book-kicker">Today\'s book (live from main)</div>' +
      '<div class="live-book-date">' + (d.date || "") + '</div>' +
      '<div class="live-book-row"><b>BUY 1d</b> ' + (buys.join(" · ") || "—") + '</div>' +
      '<div class="live-book-row sell"><b>SELL 1d</b> ' + (sells.join(" · ") || "—") + '</div>';
  }
  function loadOne(i){
    if(i >= URLS.length) return;
    fetch(URLS[i] + "?t=" + Date.now(), {cache: "no-store"})
      .then(function(r){ return r.ok ? r.json() : Promise.reject(); })
      .then(function(d){
        if((d.buy_1d && d.buy_1d.length) || (d.sell_1d && d.sell_1d.length)) paint(d);
        else loadOne(i + 1);
      })
      .catch(function(){ loadOne(i + 1); });
  }
  loadOne(0);
  setInterval(function(){ loadOne(0); }, 60000);
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
        return False
    if '<div class="wrap">' in text:
        text = text.replace(
            '<div class="wrap">',
            '<div class="wrap">' + _POLLER_HTML,
            1,
        )
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
