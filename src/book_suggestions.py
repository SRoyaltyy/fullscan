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
