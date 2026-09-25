# IRONCLAD RULES — strategy mining, backtests and records (Cyrus, 2026-09-26)

These apply to every strategy, board and backtest in SRoyaltyy/fullscan (Factor Mine, OOS-0914 mine, and any future one). Code enforces each rule; a run that would break one fails instead of continuing.

## A. Records never change
1. Once a day's buys/sells are made, they never change for any reason. Past days are outcome data only, never rewritten or re-picked.
2. Records are append-only: each new day adds to the previous record.
3. Each locked day is fingerprinted; every run re-checks all past fingerprints and fails on any mismatch.
4. Changing any part of a strategy's rules (gates, weights, hold, exits, sizing, fees, fill rules) makes it a NEW strategy under a NEW name. The old strategy and its record stay untouched.
5. Only a brand-new strategy may build its own history once. Days before its creation date are labelled `designed_after` (hindsight) and never count toward its real record.
6. Dated ticket files are locked at 09:30 ET or once that day's paper send is journaled; later runs write a draft file only.

## B. Build one day at a time
7. Every strategy is built sequentially from its start date. Day N uses only inputs knowable at 09:30 ET on day N, plus day N-1's closing state (cash, holdings, fees).
8. Never compute all days in one pass. A restart from any saved day must give byte-identical results; a planted future file must change nothing.

## C. Inputs
9. Each day's inputs are frozen at send time / 09:30 ET and hashed. Nothing that arrives later can enter that day.
10. Live tickets and the strategy record use the same frozen input set. If the frozen set has no data for the day, the strategy sits; no live lookup outside it.
11. Price source: Yahoo split-adjusted daily bars only. Finviz/Stooq disagreement is a logged warning. A name missing bars is dropped, the dropped list is frozen, and the day always locks.

## D. Fills and fees
12. Buy at the 09:30 open. Exit at the level hit, at the open on a gap through it, or at the close.
13. If one bar touches both stop and target, the stop fills first.
14. Fees: Futubull schedule (per-order minimums) as the main figure; flat 15bp shown alongside.

## E. Mining (finding new strategies)
15. Train and test are split by date. The miner may not load any file or bar dated on/after the test start (OOS-0914: 2026-09-14).
16. The candidate list and the selection metric are written and committed before mining. Nothing is added after the freeze.
17. Chosen rules are frozen with fingerprints in a commit dated before any test day is scored.
18. Keep the candidate list small (≤50) and report the best-of-N luck test.

## F. Judging a strategy
19. Always compare against RANDOM4 (seed 20260813, 1000 draws) and IWM buy-and-hold on the same days, after fees.
20. Always show the result without the strategy's single best stock.
21. Keep bar: ≥30 fires and >55% win rate after fees, with after-fee P&L shown. Start-day wins and asymmetric wins are reported alongside.
22. Recipes that never traded are `untestable` and stay out of rankings.
23. No real money until about 20 locked sessions beat RANDOM4 and IWM after fees, including without the best stock.
