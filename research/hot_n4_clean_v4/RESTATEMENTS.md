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

## 2026-09-28 — Seal only inside D's 08:00–09:30 ET window

This enforces the PREREG's "seal on D before 09:30 ET" timing. A plan run whose clock is before 08:00 ET on the target session's own calendar date does not seal. It changes no picks, so the study name is unchanged.

- study: `hot_n4_clean_v4`, recipes `union_hot_n4_h1__w0` and `union_hot_n4_holdup__w0`
- rule: seal on D before 09:30 ET. The lower bound is 08:00 ET on D. After D's close fill, a run on D evening targets D+1 and writes nothing. A run at D+1 07:59 ET writes nothing. D+1 08:00–09:29 ET seals. At or after 09:30 ET the missing record is unchanged.
- code: `plan_main` in `research/hot_n4_clean_v4/forward/forward.py`. Holdup and h1 share that entry point. `PLAN_ALLOW_EARLY=1` skips the lower bound for tests and is not set by any workflow.
- sealed lines: unchanged. No ledger, log, price ledger, PREREG, IRONCLAD, or manifest line was edited.

## 2026-09-28 — Open fill used the previous session's bar

The 09:41 ET open fill sealed `yahoo_split_adjusted_daily_open` from bars that were still 2026-09-25 for two legs. SRFM was filled at 0.9439 and SECZ at 16.21, the 09-25 opens (09-25 closes were 1.11 and 15.96). The 09-28 opens are SRFM 1.125 and SECZ 16.00. GLND, SHMD, FEAM, and TJGC were the 09-28 opens. That night's close fill refused because the stored 09-28 open no longer matched the sealed fill. This entry does not correct that seal.

- study: `hot_n4_clean_v4`, recipes `union_hot_n4_h1__w0` and `union_hot_n4_holdup__w0`
- rule: every buy and sell leg's Yahoo bar must be dated the session in America/New_York. A missing bar or any other date does not seal. The run exits non-zero and writes nothing. A fill that equals the prior session's open or close while that session bar is absent is the same refusal.
- code: `open_fill_main` in `research/hot_n4_clean_v4/forward/forward.py`. Holdup and h1 share that entry point.
- sealed lines: unchanged. No 2026-09-28 correction was written.

## 2026-09-28 — Open fill correction for the stale bar

The sealed 2026-09-28 open fills stay as written. A later `open_fill_correction` is the open fill the close, the next plan, and the pages use. Keeping the sealed share counts at the true opens would spend cash the book does not have. The correction reruns the open-fill sizing code on the sealed plan at the stored 2026-09-28 opens, so the share counts are the code's. This entry does not append that line.

- study: `hot_n4_clean_v4`, recipes `union_hot_n4_h1__w0` and `union_hot_n4_holdup__w0`
- rule: an `open_fill_correction` references the sealed open fill's sha256 and the plan's sha256. It records reason `stale_bar: previous-session bar used for SRFM, SECZ`, approved_by `Cyrus 2026-09-28 21:02 ET`, the corrected open fill, and the sealed-versus-corrected values for each leg that moved. Cash after the correction must be at least zero. A second identical run appends nothing. The close fill and the book's cash and positions read the latest correction. Sealed lines are not edited.
- code: `HOLDUP_MODE=correct_open` with `CORRECT_OPEN_DATE` in `research/hot_n4_clean_v4/forward/forward.py`. Holdup and h1 share that entry point. The manual workflow is `.github/workflows/correct_open.yml`. It has no schedule.
- sealed lines: unchanged. The 2026-09-28 correction was not written to either log or ledger.

## Engine pin: hold a partial Yahoo page

2026-11-04. The study name stays `hot_n4_clean_v4`. This re-pin is the partial-fill hold in commit `edd116196a`. No sealed row was rewritten or deleted, and no row on or before the last sealed day was filled in.

- file: `src/factor_mine.py`
- old sha256: `b51eed634fb5ce213e3a2e1f85c83749e97ee42519b33d1d60ad7ad0c0047860`
- new sha256: `f904788699186111ad246dcc2a0ed9c8ebd2048ff096196ce8bc8a9d11872e0c`
- commit: `edd116196a61df77b39739d9576ca81f5468fe5b`
- reason: `build_panel` now counts new drops on an unlocked session and raises `HoldDay` (`incomplete session bars; refusing to bake a partial drop list`) when that count is above 40. The branch runs only when `fail_closed` is set and `frozen is None`. A frozen day never enters it. `pick_day`, `matches`, `should_exit`, and `lot_should_sell` are unchanged, so sealed picks, fills, and P&L are unchanged.
- section 11 and `ENGINE_SHA256` now pin the new bytes. The other engine pins are unchanged. The covered prereg fingerprint moved from `5059366464a6fdd19571436296a759717d077a413cc0c693dbbb61f5dc295c5e` to `bac073d1a0e1f64c39ca1e817d5919dc46266ecfa407665aec34afabc6410f67` because section 11 is inside the covered body. The 2026-09-28 restatement above is unchanged.
- sealed lines: unchanged. No `*.jsonl` ledger line, no locked-day file, and no buy/sell row on or before the last sealed day was edited or removed. `03_scoreboard/mover_lookback_action.json` moved its 97,558 `called_rows` into gzip shards; reassembled, those rows match the previous monolith.

## Engine pin: memoize session bars

2026-11-04. The study name stays `hot_n4_clean_v4`. This re-pin is the session-bar memo in commit `b1a86cd1d7`. No sealed row was rewritten or deleted, and no row on or before the last sealed day was filled in.

- file: `src/ticker_lookback.py`
- old sha256: `1e08f2f42c732407f834847f2e347b0218d8a18b2612a2ee565f24bcc6a2731e`
- new sha256: `30d7b899d839367671b6969cf3527d2f7acae0e5d67e88ee97c27f4ae318b7ff`
- commit: `b1a86cd1d7016ee41e00fb46fe4637f799a4ad08`
- reason: combo replay called `session_bar` on every mark, and each call rebuilt the NYSE holiday year and looked the bar up again. The same `(ticker, date)` was asked for hundreds of times. The memo returns those same prints. `reset_price_caches` drops it when the store is replaced. `pick_day`, `matches`, `should_exit`, and `lot_should_sell` are unchanged. A full `replay_combo_starts` of `combo_se_5050_shared` on the 57-session panel was byte-identical to the uncached read (398,304 bytes).
- section 11 and `ENGINE_SHA256` now pin the new bytes. The other engine pins are unchanged. The covered prereg fingerprint moved from `bac073d1a0e1f64c39ca1e817d5919dc46266ecfa407665aec34afabc6410f67` to `a45073dd2acd820b28d4a4a6d7cab5f3a9d3093ba67ec48332d8bc13885a186a` because section 11 is inside the covered body. The restatements above are unchanged.
- sealed lines: unchanged. No `*.jsonl` ledger line, no locked-day file, and no buy/sell row on or before the last sealed day was edited or removed.
