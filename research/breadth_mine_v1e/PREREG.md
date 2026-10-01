# Breadth mine v1e — preregistration

- study: `breadth_mine_v1e`
- status: protocol locked. This commit has no returns, no p-values, and no luck-test output.
- written: 2026-09-26
- owner: Cyrus, verdict rule 15:08 HKT. A rule wins only if it is formed on data through 2026-09-13 and then wins from 2026-09-14 onward.
- fingerprint_sha256: c5e58acf2547b70cee511357edd5adc1a891cdc9225956c3b8a0dbd667dae535
- fingerprint_scope: SHA-256 of the UTF-8 bytes after the line `<!-- BEGIN COVERED -->`, including the final newline. Line endings are LF.
- study label: `assumed pre-open, not server-proven`
- creation date: 2026-09-26
- live paths: not used. PR #360 is not edited. Nothing already scored is rewritten. `data/prices/ohlc.parquet` is not rewritten.

<!-- BEGIN COVERED -->

## What this study is

This is a new study. Its name is `breadth_mine_v1e`. `breadth_mine_v1` and `breadth_mine_v1b` were scored on PR #360 and stay there. `breadth_mine_v1c` and `breadth_mine_v1d` were fingerprinted there and stay there. This file does not edit them.

Nothing used to form a rule is fitted after 2026-09-13. Atoms, thresholds, the grid, the candidate cap, and the rank are fixed here. The rank uses session returns from 2026-08-20 through 2026-09-11 only. The scorer writes the rank-1 pick before it loads any bar dated 2026-09-14 or later.

Creation date is 2026-09-26. Every session in this file is before that date, so the book is `designed_after` as a live record. The verdict below is the 15:08 rule, not a live-money record.

## Formation cutoff

The formation tape is every bar in the pinned parquet with date on or before 2026-09-13. While the pick is chosen, the scorer loads no bar dated 2026-09-14 or later. A planted file dated after 2026-09-13 cannot change the pick.

## Bars

Prices are Yahoo split-adjusted daily bars, `auto_adjust=false`, dividends not applied. The source snapshot is the one pinned on PR #360 at sha256 `53ea564340a6d7fb452dc44c798f583d50d39968af145b577a8929d5e1daa797`. This study stores the cleaned file `research/breadth_mine_v1e/bars/ohlc.parquet`, sha256 `06a905777f571020dc7eefdfc42ea8489905ae5d8ed72fbc97f42cc513888154`, git blob `68eaa3500faa047f0dc198de30d093db87d5a15e`. Splits: `research/breadth_mine_v1e/bars/splits.json`, sha256 `40c3733b6428f885239236e358ba4ee6f5c5b6ac9f77c5f30e206aa8daa4d338`, git blob `b5380bede11ea265d084d384cb12289a0929938f`.

A consecutive-session jump is this session's open divided by the previous session's close. Above 3 or below 1/3 is unexplained unless a Yahoo split on the later date is within 25% of the price factor `1/split` or of the share factor. Names with an unexplained jump on or before 2026-09-13 are removed from the file. That list uses only jumps dated on or before 2026-09-13. Names whose only unexplained jump is after 2026-09-13 stay in the formation tape. Their rows after 2026-09-13 are omitted from the stored file so the forward book does not fill a broken print. That omission is not an input to the rank.

Removed for a jump on or before 2026-09-13: AIXI, ALP, ASTC, CAPR, CAST, CELZ, CHAI, CISS, CLRO, DSY, EDHL, EHGO, ENGN, EYPT, FCUV, FTRK, HAO, HKIT, HYFM, ILLR, IPST, JEM, JZ, LHSW, MB, MGN, MYSE, NAMI, OFAL, PLAG, PPCB, QH, QNME, SCKT, SION, SLBT, SXTC, TGHL, TNMG, UPC, VSME, WCT, WETO, WOK, XHG, YJ.

Forward rows omitted after 2026-09-13: GIPR, MPU, NFE, RITR, VWAV, ZTG.

The audit list is `research/breadth_mine_v1e/bars/DROPPED.json`, sha256 `b4c05f5786345eac8d252d32961038e157f7e57dea79087b837379f51c3ed5d7`. The stored parquet has no unexplained jump. CI fails if a rescan finds one.

A held name with no open on a session is sold at its previous mark that session. It is not left on a frozen price.

## #360 rank 1

The rank-1 rule on PR #360, from `breadth_mine_v1b`, is `p:catalyst_on+heat_up|short|cut_loser|h5m2|n1`. On that fresh-bar score it has 9 entries on store sessions from 2026-08-20 through 2026-09-11 and 11 entries in the full walk through 2026-09-25. Where the old pin `data/prices/ohlc.parquet` and the fresh snapshot both have an open for a name-day in that rule's P&L, none of those opens differ by more than $0.01 and 0.1% (56 name-days). Eight name-days in that P&L are absent from the old pin, so those marks are not the same trade: AMR and IPI on 2026-09-09 and 2026-09-10, and SKYW on 2026-09-22, 2026-09-23, 2026-09-24, and 2026-09-25. The 2026-09-22 SKYW print is an entry day.

## Fees and book

Fees are `00_grounding/futubull_fees.json`, sha256 `019ebdba0fc0b20e02c91f116dc5591b81e96630f60c110dfbfd3b5d8e16c0d3`. Capital is $10,000. Shares are whole shares. Entry is `open_equal`. Flat 15bp is reported beside Futubull. Shorts pay 1% annual borrow on the marked notional, `sessions / 252`.

## N

The grid is the same 57,600 combinations. Atoms, in byte order: `a09_sma50`, `ab_good`, `actions_good`, `book_buy`, `catalyst_on`, `cd_engulf_bull`, `fv_week_pos`, `hard_red`, `heat_up`, `hot_pos`, `judge_up`, `last_green`, `predict_up`, `s_gt_0`, `sector_up`.

Singles 15. Pairs 105. Signals 120. Cross: side 2, exit 4 (`time`, `list`, `cut_loser`, `trail`), hold pairs 15, top-N 1, 4, 8, 12. **N = 57,600.**

The luck denominator is 239,790 + 57,600 = **297,390**. The 239,790 are the tries already registered on PR #360, including the two fingerprinted studies that stopped before a return file. Luck p is reported, raw and adjusted. It does not decide the verdict.

Combination id: `{signal}|{side}|{exit}|h{hold}m{min_hold}|n{top_n}`.

## Candidate list

On day D the tradable names are the labelled union that has a positive open and is still in the pinned parquet. They are ranked by hot score descending, then ticker ascending. The candidate list is the first 50.

## Clock

Sessions are the 31 store sessions from 2026-08-13 through 2026-09-25. 2026-09-07 is not a session. The pre-09-14 window is every store session from 2026-08-20 through 2026-09-11, including a session the rule sat out. The forward window is every store session from 2026-09-14 through 2026-09-25. The book is continuous from 2026-08-13.

`cut_loser` is 3% against the entry at the open after min_hold. `trail` is 5% off the favorable extreme. Hold is counted in store sessions.

## Objective

Primary, pre-09-14 only: Futubull after-fee compound on 2026-08-20 through 2026-09-11 is at least 30%.

Guards on that same window, all required:

- Fires ≥ 30. A fire is an entry on a window session.
- Win rate of closed trades on window sessions > 55% after fees.
- Top-1 ticker is not above 50% of window P&L. A non-positive sum does not fail this guard.
- The window compound is strictly above the mean of 1,000 RANDOM4 compounds on that window.
- The window compound is strictly above the IWM compound on that window.

Ex-best is reported. It is not a guard. Median trade is reported. It is not a guard. Luck p is reported. It is not a guard. The 2026-09-14 through 2026-09-25 result is not a guard and is not used to pick.

RANDOM4: `random.Random(20260813 + draw)` for draws 0 through 999. Each draw samples 4 names, or fewer if the list is shorter, from that day's candidate list sorted A to Z. Long, hold 1, exit `time`, same Futubull book.

IWM: buy at the 2026-08-13 open, hold, mark at each close, sell at the 2026-09-25 close, Futubull fees on the buy and the final sell. The comparison uses the compound of that book's session returns on the pre-09-14 window.

## The pick

Among rules that pass every guard and the 30% primary, rank 1 by Futubull window compound descending, then id ascending, is the pick. If none pass, there is no pick. The pick is frozen in `PICK.json` before any forward bar is loaded. The forward table is not used to choose a different rule.

The forward report for that one rule is its Futubull compound, its ex-best compound, its closed-trade count, and the day count. The day count is the number of store sessions from 2026-09-14 through 2026-09-25.

The rule wins only if a pick exists and that forward compound is strictly greater than 0. Otherwise the verdict is `no win` if a pick exists, or `nothing selected` if none does.

## Other paths, labelled

Walk-forward, labelled `walk_forward`: for each session D from 2026-08-20 through 2026-09-11, rank with the same objective and guards using only window sessions strictly before D. Trade D with that rank-1 rule. If none pass, the day is cash, return 0. This path is not the pick.

Hindsight-best, labelled `hindsight`: the rule with the highest Futubull compound on 2026-09-14 through 2026-09-25. It is not the pick.

## Append-only

Each session is written once to `research/breadth_mine_v1e/returns/{session}.json`. `.github/workflows/breadth_mine_v1e_append_only.yml` runs the append check and the split check.

Inputs: `research/breadth_mine_v1e/INPUT_MANIFEST.json`, sha256 `980367c54e3edc3a44db8477b3be1e276ef2d7a7f7ea3f2a25fed112b251fe2f`.

## Refusal

The run refuses a fingerprint mismatch, a pin mismatch, a bar dated after 2026-09-13 while the pick is unwritten, an unexplained jump in the pinned parquet, a candidate list above 50, or a rewrite of an existing session file.
