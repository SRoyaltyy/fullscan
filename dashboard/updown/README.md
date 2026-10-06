# updown_o2c_v1 — every-stock morning up/down read (research page)

New, additive research page. Not a trading signal; touches no existing strategy, ledger or workflow.
Data (walk-forward out-of-sample predictions, 2020-01-02..2026-10-05, final holdout from 2026-07-06) lives on the
orphan branch `updown-data` (`updown/index.json` + `updown/m/YYYY-MM.bin`) and is fetched from raw.githubusercontent.com,
so the open-path Pages deploy (dashboard/ -> gh-pages) only carries this small HTML file.
