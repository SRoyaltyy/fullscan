# ERRATA — breadth_mine_v1b stuck marks

This file records a defect in the scored book on PR #360. The day files, `summary.json`, and the return numbers in `returns/REPORT.md` are not rewritten. The engine fix is in `src/breadth_mine_v1b_score.py` and was not used to rescore those files.

## Cause

`advance_book` priced and exited a held name only from that session's candidate map. The map is the day's input names that also have a bar. A name that has left the candidate list was treated as having no open, even when `research/breadth_mine_v1b/bars/ohlc.parquet` still had the session.

The old exit loop then did `continue`. The hold-limit check never ran. The mark fell back to the previous close, so a short's only P&L was the daily borrow, about −0.20 on these notionals. The position stayed open until a later session put the name back on the map.

## Rank 1, index 33044

Rule `p:catalyst_on+heat_up|short|cut_loser|h5m2|n1`. Hold limit is 5 sessions.

On 2026-09-09 the candidate map had no open for four names the book still held. Each has a bar in the pinned store. Recorded P&L that session is borrow only:

| ticker | held | hold limit due | recorded P&L | store open | store close |
| --- | ---: | --- | ---: | ---: | ---: |
| AMR | 5 | yes | −0.2042 | 228.960007 | 226.940002 |
| IPI | 4 | no | −0.2374 | 42.060001 | 41.259998 |
| CF | 3 | no | −0.2079 | 137.880005 | 138.110001 |
| CTVA | 2 | no | −0.2386 | 86.400002 | 84.750000 |

AMR's hold-limit exit was due that day and did not fire. IPI, CF, and CTVA were inside the hold, so the missed price also blocked `cut_loser`. AMR and IPI are back on the map on 2026-09-10 and both close that session. They are not open from 2026-09-11 through 2026-09-25 in this recorded book. The flat −0.20 print is that one session, not a position carried to 2026-09-25.

## Every case in the scored walk

A full replay of the published engine, without the fix, counted held name-sessions whose open was absent from the candidate map:

- 18,822,434 held name-sessions
- 53,322 of 57,600 rules
- 14,653,151 of those name-sessions were already at the hold limit, so the exit was due and did not fire
- 0 of them lack a bar in the pinned store

The same defect is repeated across rules. It is not 18,822,434 missing bars. The rank-1 rows above are the only four for index 33044.

## Empty 2026-08-13 through 2026-08-25

Rank 1 sits out on every session from 2026-08-13 through 2026-08-25 because both the catalyst input and the heat input are absent. The pair signal needs both roles. The first session with both is 2026-08-26, which is the first entry.

## Fix, not applied to these results

A held name is now priced from the bar store when it is off the candidate map. If the store also has no positive open and the hold limit is due, the position is force-closed at the last known price. If the store has no bar and the hold limit is not yet due, the last mark is kept. This snapshot has no store-missing bar of that kind. The published day files still use the old skip.
