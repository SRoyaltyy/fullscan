# dashboard/updown: every-stock up/down read (research)

Static research page for `updown_o2c_v2`. It shows p(up) for today's official open → close, for every stock on every day
from 2020-01-02 to 2026-10-05. Past days show only walk-forward out-of-sample predictions. The final holdout starts 2026-07-06.
**Research page, not a trading signal.**

v2 replaces v1 after a leak audit (`LEAK_AUDIT.md`). v1 used the split-adjusted price level, which leaks future reverse splits.
The open→close edge depends on today's official open, which cannot be traded at the printed open. With no same-day open
information, there is no after-fee edge. Details are in the audit.

- `index.html`: self-contained vanilla JS with no external requests. Deep links: `#d=YYYY-MM-DD&t=TICKER`.
- `data/index.json`: tickers, days, per-day stats, summary.
- `data/y/YYYY.bin.gz`: one gzip-compressed binary per year (format in index.json `format`), decoded in the browser.
- `LEAK_AUDIT.md`: the leak audit, with its verdict on the first line.

Additive only. Nothing in the daily pipeline reads or writes this folder. It is regenerated offline. It is not part of
h1, Factor Mine, or any ledger.
