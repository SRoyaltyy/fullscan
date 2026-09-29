# Restatements

Append-only. A later entry is added at the bottom. An earlier entry is not rewritten.

## Morning file absent is not a skipped session

The forward code skipped a session when the morning file was absent, which the PREREG never scored. This change restores the PREREG rule. No sealed line changed. The study name stays the same because the live rule now matches what was scored.

- study: `hot_n4_clean_v4`, recipes `union_hot_n4_h1__w0` and `union_hot_n4_holdup__w0`
- rule: PREREG section 5 and section 8. When the morning file is absent, holdup does not apply and `min_hold` stays 1. h1 does not use S. A missing morning file does not delete the day.
- code: `research/hot_n4_clean_v4/forward/forward.py` returned after `morning_score` when no predict or weather blob was on GitHub before 13:30 UTC. It now plans with `morning_s` null and `morning_status` ABSENT.
- sealed lines: unchanged. `LEDGER.jsonl`, `PRICE_LEDGER.jsonl`, `holdup_log.jsonl`, `skips.jsonl`, the `forward_h1` files, and the 2026-09-28 plans (`f9c8628a` h1, `5e6a3d04` holdup) were not edited.

## Page status follows the log

The h1 and holdup pages showed 2026-09-28 as missing (`plan not sealed before open`). Both logs already had a sealed plan and an open fill for that day, and neither had a close fill. The page now takes the day's status from those lines. A missing label is used only when the log has a missing record. The 2026-09-28 close-fill note is a date-keyed display note. It is not a change to the book.

- study: `hot_n4_clean_v4`, recipes `union_hot_n4_h1__w0` and `union_hot_n4_holdup__w0`
- rule: display only. A plan with no open fill is `plan sealed`. An open fill with no close fill is `opened; close fill pending`. A close fill is the normal P&L row. An explicit missing record is `missing` with its reason.
- code: `research/hot_n4_clean_v4/forward/render.py` and the h1 and holdup pages. Notes are `research/hot_n4_clean_v4/forward/day_notes.json`.
- sealed lines: unchanged. No fill or correction was written. `LEDGER.jsonl`, `PRICE_LEDGER.jsonl`, `holdup_log.jsonl`, `h1_log.jsonl`, and `skips.jsonl` were not edited.
