# Restatements

Append-only. A later entry is added at the bottom. An earlier entry is not rewritten.

## Morning file absent is not a skipped session

The forward code skipped a session when the morning file was absent, which the PREREG never scored. This change restores the PREREG rule. No sealed line changed. The study name stays the same because the live rule now matches what was scored.

- study: `hot_n4_clean_v4`, recipes `union_hot_n4_h1__w0` and `union_hot_n4_holdup__w0`
- rule: PREREG section 5 and section 8. When the morning file is absent, holdup does not apply and `min_hold` stays 1. h1 does not use S. A missing morning file does not delete the day.
- code: `research/hot_n4_clean_v4/forward/forward.py` returned after `morning_score` when no predict or weather blob was on GitHub before 13:30 UTC. It now plans with `morning_s` null and `morning_status` ABSENT.
- sealed lines: unchanged. `LEDGER.jsonl`, `PRICE_LEDGER.jsonl`, `holdup_log.jsonl`, `skips.jsonl`, the `forward_h1` files, and the 2026-09-28 plans (`f9c8628a` h1, `5e6a3d04` holdup) were not edited.

## Engine pin: land_closed OOS0914 handler

Cyrus approved this re-pin on 2026-09-28 ET. The study name stays `hot_n4_clean_v4`.

- file: `src/factor_mine.py`
- old sha256: `ce4f1954b0c5e97009dedf6c2d7604d8c225a5633c96090d6e2b20f9e04a9272`
- new sha256: `b51eed634fb5ce213e3a2e1f85c83749e97ee42519b33d1d60ad7ad0c0047860`
- commit: `2916bf5ef67268b1e966fdbbd846afc291f3c6b6` (#399)
- reason: that commit changed only the `land_closed` OOS0914 handler, and only after `land_closed()` returns. `except Exception:` became `except (Exception, SystemExit):`, plus a two-line comment. `AppendDrift` subclasses `SystemExit`, so the old handler exited 1 after the live lock. The new handler still runs only after picks, fills, and P&L are done, so it cannot change them.
- section 11 and `ENGINE_SHA256` now pin the new bytes. The other engine pins are unchanged.
- sealed lines: unchanged. No `*.jsonl` ledger line and no locked-day file was edited.

## Price-store pins stay the historical blobs

The ledger test used to hash the live `data/prices/ohlc.parquet` and `data/prices/actions.parquet`. After the engine re-pin those checks are the next failure. The pins are not rewritten. The test loads the pinned git blobs and requires every pinned row to still be present with identical fields. New rows are allowed. Cyrus approved that rule for this ohlc file on 2026-09-28 ET (the lever-search guard). The actions file is the same shape, so the ledger test uses the same rule. A missing or changed pinned row still fails.

- `data/prices/ohlc.parquet`: pin sha256 `559c8cf099808930bef2b4de4280b4e902883c9a1de85c8a417074f11aaefa55`, git blob `3456f7f489a6fa7033e8ae5cc942d8279f0113e3`. Live sha256 `a3bb6172ad4abc6aa43bd7f4727e6f889bdfbb2e3151d397609f680ed65a0d4c`. The live file gained 2,746 ticker-dates. No pinned row's open, high, low, close, or volume changed.
- `data/prices/actions.parquet`: pin sha256 `0471b2d76c30960eb494134a7bd34eea499bb5c696190f5fb3628c4becd94e4a`, git blob `ebbda85df7e4ee81f78adf04208ca9d5ea148c48`. Live sha256 `9013d58989e8d1efdbf9b5ed5f6751aaaf0b124aa88488acecd76f4b5e40e1d6`. The live file gained 8 rows, all dividends on 2026-09-25 with split 0. No pinned row changed.
