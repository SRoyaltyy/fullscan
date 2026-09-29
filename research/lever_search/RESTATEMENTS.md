# Restatements

Append-only. A later entry is added at the bottom. An earlier entry is not rewritten.

## Price store may grow; pinned bars stay

Cyrus approved this on 2026-09-28 ET.

`test_initial_inputs_and_hash_guard` hashed the whole live file `data/prices/ohlc.parquet`. The live sha256 is `a3bb6172ad4abc6aa43bd7f4727e6f889bdfbb2e3151d397609f680ed65a0d4c`. The manifest pin stays `559c8cf099808930bef2b4de4280b4e902883c9a1de85c8a417074f11aaefa55`, git blob `3456f7f489a6fa7033e8ae5cc942d8279f0113e3`, commit `ff996f535e1343dd739cc801780ae224018bd96c`.

The live file only gained rows: 2,746 new ticker-dates. No existing row's open, high, low, close, or volume changed.

The guard loads that pinned blob from git and checks that every pinned ticker-date is still present with identical OHLCV. New rows are allowed. A missing pinned row, or a change to a pinned open, high, low, close, or volume, still fails. The manifest sha256 is still the whole-file hash of the pinned blob, not of the live file.

`data/prices/meta.json` is the summary `price_store` writes from that file. Its pin stays `3f6f5037a6b5d44bbd8bd9ede2346389167f96b29177709b18beca5d0cdaeee1` at commit `77db2793d6a2fc4941156a896eafaf85eb892460`. The live summary moved with the same append: `n_rows` 3,065,066 to 3,067,812 (the same 2,746 rows), `last_date` 2026-09-25 to 2026-09-28, `updated` changed. `first_date` stayed 2024-03-04 and `n_tickers` stayed 11,712. The guard allows that forward move. `first_date` cannot move later, and `n_rows`, `n_tickers`, and `last_date` cannot shrink.

## Blank first_open may be filled once

Cyrus approved this on 2026-09-28 ET.

`test_suggestions_signal_cell_is_frozen` failed on suggestions row 2669, CLM, signal date 2026-09-25: pinned `first_open` `''` versus live `'6.4300'`.

`excel_bot` writes a new suggestion with a blank `first_open` and fills it once, on a later run, with the next session's open (`excel_bot/engine/daily_run.py`). A blank cell may become a value. A non-blank `first_open` that changes still fails. The other signal cells stay frozen: `run_date`, `signal_date`, `ticker`, `side`, `strategy`, `exit_rule`, `ref_close`, and `signal_colors`.
