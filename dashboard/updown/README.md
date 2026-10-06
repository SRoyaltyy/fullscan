# dashboard/updown — every-stock up/down read (research)

Static research page for `updown_o2c_v1`: p(up) for today's official open → close, for every stock on every day from
2020-01-02 to 2026-10-05. Past days show only walk-forward out-of-sample predictions. The final holdout starts 2026-07-06.
**Research page, not a trading signal.**

- `index.html`: self-contained vanilla JS with no external requests.
- `data/index.json`: tickers, days, per-day stats, summary.
- `data/y/YYYY.bin.gz`: one gzip-compressed binary per year (format in index.json `format`), decoded in the browser.

Additive only. Nothing in the daily pipeline reads or writes this folder. It is regenerated offline. It is not part of
h1, Factor Mine, or any ledger.
