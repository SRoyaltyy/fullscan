# Peer map + Compare-style relative strength

## Peer list file (pick one)

1. `Correlations.xlsx` — ticker in last column, up to 10 peers to the left
2. `correlations.csv` — columns `ticker,peer_1,...,peer_10`

## Two peer metrics

### A. Snapshot RS (fast, full universe) — `src/peer_rs.py`

Uses Finviz `Performance (Week/Month)` only:

```
rs_week = stock_week_perf − median(peer_week_perf)
```

Output: `data/peers/YYYY-MM-DD_peer_rs.csv`

### B. Finviz Compare lines (correct for overtaking) — `src/peer_compare.py`

Matches the Finviz **Compare** chart:

```
rel(t) = price(t) / price(~1 year ago) − 1
```

Then inspects the **last 7 trading sessions** before the as-of date:

| Field | Meaning |
|-------|---------|
| `ret_7d` | stock absolute 7d return |
| `peer_med_ret_7d` | median peer absolute 7d return |
| `peer_breadth_7d` | fraction of peers with **positive** 7d return |
| `rs_7d` | change in stock rel-line − change in peer-median rel-line over 7d |
| `overtake_7d` | was ≤ peer-median rel 7d ago, now above |
| `leadership_7d` | stock rel-line gained more than peer median over 7d |

Requires **yfinance** (OHLC). Run on stock-book names or an explicit list (not full 11k by default):

```bash
python -m src.peer_compare --date 2026-08-14 --from-book --top 40
python -m src.peer_compare --date 2026-08-14 --tickers XPON,AAPL,NVDA,ETON
```

Output: `data/peers/YYYY-MM-DD_peer_compare_7d.csv` and `01_daily/YYYY-MM-DD_peer_compare_7d.md`

`ticker_checklist` **prefers** (B) when the 7d file exists; otherwise falls back to (A).
