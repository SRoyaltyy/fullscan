# Pre-execution holdout novelty correction

A stricter, reproducible audit identified twelve normalized duplicates or conservative near duplicates in the unused future selection. This corrects an incompletely enforced novelty claim in the original selection. No classifier has executed batches 03–12; replacements were selected without classifier outputs or answer-correctness criteria.

The audit normalizes publisher suffixes and numbers, and excludes same URLs, normalized titles and declared token-overlap collisions. The final 1,000 rows have zero collisions under those rules against each other and 520 recovered prior/control rows including repetitions. This does not certify exhaustive exclusion from every older unrelated experiment or unique underlying events.

Original selection and frozen-reference files remain byte-for-byte intact at their original paths and in git history. `novelty_repair_20261015.json` records original/replacement rows, source provenance, original hashes, corrected hashes and retirement reasons. The twelve retired selections must not receive acceptance credit.

Use the following lineage for future tests:

| Batches | Input directory | Reference directory | Readiness |
|---|---|---|---|
| 03–04 | `tests/fixtures/semantic_frozen_20261014` | `docs/benchmarks/references_20261014` | Original inputs/references unchanged, classifier unrun |
| 05–08 | `tests/fixtures/semantic_novelty_repair_20261015` | `docs/benchmarks/references_novelty_repair_20261015` | Corrected inputs and independent replacement judgments frozen before execution |
| 09–12 | `tests/fixtures/semantic_novelty_repair_20261015` | Not yet frozen | Complete 100-row reference judgments required before execution |

Only replacement rows changed in references 05–08; unchanged judgments compare exactly as JSON values. Each new reference records the original reference hash, original input hash, original freeze time, corrected input hash and replacement rows. The replacement-judgment file is not a complete reference for 09–12.

Reproduce selection by checking out selection inputs at commit `07a5f2db9691d60dc786e318e6763e9f5912326f`, restoring the twelve daily archive files from pinned source commit `a62a3cc16b0a7303a7c4fa1b855566f75f23bd33`, and running `python scripts/repair_semantic_reserve_novelty.py`. The script reads input titles/URLs and archived intake rows only. It does not load labels, classifier outputs or runtime classification code. Generated 03–04 audit copies have the same bytes/hashes as the unchanged original inputs.

Acceptance remains 0/10. V16 development run 38710347188 passed 79 semantic tests and 5 provider tests on Linux; model execution is awaiting the self-hosted ECS runner. Fresh evaluation waits until all five development metrics reach 90%; it then uses the frozen candidate and fixed protocol. No production deployment is included.
