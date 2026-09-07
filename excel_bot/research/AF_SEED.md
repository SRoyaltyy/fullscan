# A–F seed (STOCKHISTORY stand-in)

_Generated 2026-09-07. Research only._

In the real workbook, columns A–F are Excel `STOCKHISTORY` (date, close, open, high, low, volume), aliased from the IR:IW spill. The GitHub replica must fill that spill from **our** Yahoo history, day-by-day — never Excel’s last-calc cache (`run.py --from-cache`).

## What we fetched

- Restored `excel-state` `state/rows.tar.gz` (3,603 tickers, ~169 sessions through 2026-09-04).
- `engine/fetch_af.py --spy --extend-days 750` via Yahoo v8 (`fastfetch.py`): **3,603 ok / 1 dead (NCL)** in 81s.
- Resulting tape: **3,604** files (incl. SPY), typically **514** trading days, **2024-08-19 → 2026-09-04**.
- Not used: `--from-cache`, Excel frozen A–F, or invented Finviz history.

## Engine proof

`python engine/check_af_hi.py --ticker T` seeds IR:IW from that ticker’s rows and compares evaluated H/I to `(B−C)/C` and `(B−B[t−1])/B[t−1]`:

| ticker | match |
|---|---|
| AAPL | 139 / 139 |
| MSFT | 139 / 139 |
| BBAI | 139 / 139 |

PASS: the Yahoo A–F seed reproduces Excel H and I exactly on those grids.
