# excel-state

Snapshot state for the Excel-replica bot (excel_bot/ on main).

- state/rows.tar.gz — per-ticker OHLCV cache (data/rows/*.json), repacked and force-pushed by every daily run.
- History is intentionally squashed: this branch never grows.
