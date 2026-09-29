# Restatements

Records are append-only. A restatement is a one-time named correction of a day that was never computed. A date listed here cannot be restated again. The restated day stays designed after the fact and is not part of the clean record.

## 2026-09-25 oos0914

OOS0914_RESTATE 2026-09-25

- book: oos0914
- date: 2026-09-25
- approver: Cyrus
- approved: 2026-09-26 HKT
- record: designed after the fact; not part of the clean record
- reason: The first lock walked the retro store, which ends 2026-09-24, so 2026-09-25 had no bars. Buys, sells, and equity were carried from 2026-09-24. Cyrus approved one restatement from the same frozen snapshot, the same frozen rules, and the 2026-09-25 Yahoo prices (frozen price pin, then the live store).
- old_fingerprint: 655fb675b3f8934bde91cd9c47cbed145a173363a61c3cd291de965e759b469c
- new_fingerprint: 426d1d9e8e9ba595c4c75b937a6e10dc2051eae76b8a1d2028a7bd21fd7cfca0
- files:
- `data/factor_mine/oos0914/ledgers/2026-09-25.json` `655fb675b3f8934bde91cd9c47cbed145a173363a61c3cd291de965e759b469c` -> `426d1d9e8e9ba595c4c75b937a6e10dc2051eae76b8a1d2028a7bd21fd7cfca0`
- `data/factor_mine/oos0914/ledgers/2026-09-25.json.sha256` `45693879baa439caa839fd20afc746b56fac6e56d34d0026d5fd006ad84f9614` -> `db2dae5a8d17c3662d67ca01d3ecb0ca41b23c9e72f20a40a47c0fb4c3ce829d`
- `data/factor_mine/oos0914/state/oos0914_break10_h2_sx/2026-09-25.json` `51fad220510e8b0093cd92dea2cd2d56ed91b951d9b13761249d3d2befff9212` -> `0b51bbe7e42db444a587e7c54cf5e5da192b7bb0e53e5efce8884e6e53160e4a`
- `data/factor_mine/oos0914/state/oos0914_rvol_lg_h1_sx/2026-09-25.json` `f78557521a50e4e6893ad52fdea0b194ca731b0c145ac58cead035cb80b42a07` -> `59f2f83e5c5ddf78f9fe67cada16dab1cc8788d2c95bdfc46db34cffefaab0ed`
- `data/factor_mine/oos0914/state/oos0914_break10_h1_sx/2026-09-25.json` `ca1804a16248b60fbff5d12ae4fa2aa7ff65f830459b63978d0df6cfef8afc29` -> `4efbac9d8701acf3acaf57f2fcbfb7bb0ac7755b0d1d095597b56892f2270858`
- `data/factor_mine/oos0914/state/oos0914_zero_candle_h2_sx/2026-09-25.json` `ab4bf27273f620fc3e80cb5739371ae881e2117ff94a0ab73e03ab90df22d225` -> `61297bf32a8b2d32f917bd4e1f71662b1c7f532b9cac1bed4e5eb12c982d0bf2`

## factor_mine_recipe_search_v4 rule 20

RULE20_FORWARD_DROP 2026-09-26

- book: factor_mine_recipe_search_v4 forward check. This entry does not restate a locked trading day.
- window: 2026-09-14 through 2026-09-25
- record: `returns/FORWARD.json`, `returns/REPORT.md`, the daily return files, and `freeze/FREEZE.json` stay as committed on #370. The corrected reading is a new file, `research/factor_mine_recipe_search_v4/returns/RULE20_RESTATE.md`.
- reason: Rule 20 asks for the result without the strategy's single best stock. `_drop_compound` started that arithmetic at $10,000. The forward slice is a continuation, so the base is the equity at the 2026-09-11 close. The same miss hit the without-CYPH, without-GLND, and without-INDP figures. The window compound, win rate, up-day share, trade count, and reject status come from the daily return ratios and are unchanged.
- reading: without the best stock, `union_hot_n4_h1_time__w0` is +4.23% (the committed figure was +40.57%), `union_hot_n4_h1__w0` is +5.62% (was +46.72%), `union_hot_n4_holdup__w0` is +11.25% (was +72.01%), and `union_hot_n4_h1_nonews__w0` is +5.04% (was +52.07%). A positive contribution from the removed stock now sits below the window's own return.
- carry: unchanged. Still not rejected: `union_hot_n4_h1__w0`, `union_hot_n4_holdup__w0`, `union_hot_n4_h1_nonews__w0`. Still unproven: `union_hot_n4_h3__w0`, `union_hot_n4_h5__w0`, `union_hot_n4_h1_green__w0`. The tune leader `union_hot_n4_h1_time__w0` stays rejected. `union_hot_n4_holdup__w0` still carries because the window left it not rejected; it cleared 2 of 3 tuning starts.

## 2026-09-28 oos0914 append

This entry does not restate a locked day. `2026-09-25` stays the one approved restatement above. No ledger line through that day was rewritten.

Land-closed run 36486236072 called `factor_mine_oos0914.append_nightly` for 2026-09-28. `lock_books` rebuilt every ledger in the window, including 2026-09-25, and `write_oos_ledger` raised `AppendDrift: ledger rewrite 2026-09-25: locked day bytes would change`. Merged PR #399 catches that `SystemExit` and logs `OOS0914_APPEND_FAILED`. The land committed the 2026-09-28 state files and did not write the 2026-09-28 ledger.

Reproducing the rebuild on current main, without writing, changes only the 2026-09-25 ledger. Days 2026-09-14 through 2026-09-24 rebuild byte-identical. The 2026-09-25 diff is three fields the restatement stamp added and `_ledger_doc` does not emit:

| field | locked bytes | rebuild |
| --- | --- | --- |
| `record` | `designed_after` | absent |
| `clean_record` | `false` | absent |
| `note` | designed after the fact; not part of the clean record. One-time restatement approved by Cyrus on 2026-09-26 HKT. The first lock had no 2026-09-25 bars because the test tape ended 2026-09-24. Rebuilt from the same frozen snapshot and the same frozen rules, priced from the frozen pin then the live store. | absent |

`date`, `asof`, `dropped`, `input_sha256`, and every recipe slot (`buys`, `sells`, `trades`, `equity`, `mean`, `cash`, `fees`, `holdings`, `mean_flat_15bp`) match. Locked file `6536` bytes, sha256 `426d1d9e8e9ba595c4c75b937a6e10dc2051eae76b8a1d2028a7bd21fd7cfca0`. Rebuild `6176` bytes, sha256 `4ff53d0c775f4271c4825fa8cdc170b38b9601434b9cc0d63a89817107284080`.

Cause: `eafb431f931b6b6055f87246885dd2f412cccd25` (PR #346) wrote `record`, `clean_record`, and `note` onto `data/factor_mine/oos0914/ledgers/2026-09-25.json` after `_ledger_doc`. The nightly path from `a0c24ae070962c7bd6b1ab57c8db9893a74aca54` (PR #341) passes every day `<=` the new session to `lock_books`, which calls `write_oos_ledger`. That rebuilds the day from `_ledger_doc` and refuses when the canonical bytes differ. `assert_ledger_append` does not look at those three keys, so picks, fills, and P&L pass and the byte check fails.

Not a price revision of a stored bar, a snapshot drift, or a non-deterministic format. A resimulation of 2026-09-25 from the frozen 2026-09-24 state and the current tape matches the locked state files (`0b51bbe7e42db444…`, `59f2f83e5c5ddf78…`, `4efbac9d8701acf3…`, `61297bf32a8b2d32…`). The 2026-09-28 state committed by `2916bf5ef67268b1e966fdbbd846afc291f3c6b6` matches a replay that reads the frozen 2026-09-25 state and does not rewrite it. `src/price_store.py` has no commit after the restatement. The only OOS-module commit after the restatement and before this append is `d17f47104e8de69dbe8be0a94cc60e99fa8b0dd2` (PR #348), which rebuilt the scoreboard from the locked ledgers and did not change `2026-09-25.json`. `2916bf5ef67268b1e966fdbbd846afc291f3c6b6` only soft-fails the `AppendDrift`.

PR #399 merged with Lever search append-only red (run 36489869912). `test_initial_inputs_and_hash_guard` reports `data/prices/ohlc.parquet` sha256 `a3bb6172ad4abc6aa43bd7f4727e6f889bdfbb2e3151d397609f680ed65a0d4c` against the prereg pin `559c8cf099808930bef2b4de4280b4e902883c9a1de85c8a417074f11aaefa55` (blob `3456f7f489a6fa7033e8ae5cc942d8279f0113e3`, commit `ff996f535e1343dd739cc801780ae224018bd96c`). Comparing that pin to current `main` (`8860a55266d179c9888dcc09bdec5c81743409ab`, same blob `f18d1e9ad3929857427a336bdf398462683260c6`): no existing ticker-date changed open, high, low, close, or volume. The hash moved because 2,746 rows were added. `60e907dd0a0f78e4d4a7eb31515a42790e78383d` (Factor strategy mine run 36436895909, 2026-09-28 14:43 UTC) added 122 bars for AIBZ, ARQQ, DCX, FJET, IMTX, KLXE, PSQL, SPWR, UPXI, and WFF on 2026-09-09 through 2026-09-25, plus TBCVU on 2026-09-25. `2916bf5ef67268b1e966fdbbd846afc291f3c6b6` (PR #399's local land; run 36486236072 died before publish) added ETRA on 2026-09-18, 2026-09-21, 2026-09-22, and 2026-09-23, and 2,620 names on 2026-09-28. None of those names are in the 2026-09-25 OOS fills. ETRA and TBCVU are on that day's snapshot dropped list. The same job also fails `test_suggestions_signal_cell_is_frozen`: suggestions row 2669 (CLM, signal 2026-09-25) `first_open` `''` versus live `6.4300`, filled by `caaab36bfaa6ed8121ce6c5909e9cdabcc4fe208`. That workflow runs on pull requests, not on pushes to `main`. The checks on `main`'s tip are green. The tree on `main` still fails those two prereg panel tests, so the job is red on every open PR cut from it.

The append now treats a missing ledger as the pending day. It checks each locked sidecar and writes only the new ledger. The new day is stepped from the prior frozen state, so a revised historical bar cannot alter a locked file.

2026-09-28 was appended. Ledger sha256 `390d8f0bc78b8d2fcf4da67da96f760c9b76659e8ec3c0e4aaf7b8978de17ffe`. The sha256 of every locked OOS ledger and state file through 2026-09-25, and of the 2026-09-28 state files that were already on disk, was unchanged before and after. RANDOM4 and IWM baselines were not rebuilt; they are still the figures from the prior window, including the unpriced first 2026-09-25 lock. The rule table gained 2026-09-28 from the new ledger. The headline return is the eleven-session compound.
