# Peer map (Finviz Compare-style)

Drop **one** of these here:

1. `Correlations.xlsx` — the Finviz-style correlation export (ticker in last column, up to 10 peers left)
2. `correlations.csv` — columns `ticker,peer_1,...,peer_10`

Then run:

```bash
python -m src.peer_rs --date YYYY-MM-DD
```

or **Stock Book ALL** (orchestrator runs peer_rs automatically).

Outputs:

- `data/peers/YYYY-MM-DD_peer_rs.csv` — full universe relative strength
- `01_daily/YYYY-MM-DD_peer_rs.md` — top/bottom leadership table

Formula:

```
rs_week  = Performance(Week)_stock  − median(Performance(Week)_peers)
rs_month = Performance(Month)_stock − median(Performance(Month)_peers)
```

Positive = leadership vs peers; negative = lagging peers.
