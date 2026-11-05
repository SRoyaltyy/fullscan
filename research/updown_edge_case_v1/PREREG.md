# Updown edge-case v1 — preregistration

- study: `updown_edge_case_v1`
- status: protocol locked. This commit has no filled session, no ledger, and no orders. Paper only.
- written: 2026-10-14
- forward start: not set. No candidate fills a session until its open TODOs are filled in a dated addendum committed before that session.
- fingerprint_sha256: 2e99b42498f81dae7fbeda745925f4819ab7731a289a30cf191ef7193657b819
- fingerprint_scope: SHA-256 of the UTF-8 bytes after the line `<!-- BEGIN COVERED -->`, including the final newline. Line endings are LF. The header above the marker is not hashed.
- live paths: not used. This commit does not edit h1, Factor Mine, Webull, live tickets, the morning list, `dashboard/updown` open-to-close data, or `research/updown_rel1_hv_v1`.

<!-- BEGIN COVERED -->

## 1. What this study is

This is a new research study named `updown_edge_case_v1`. It is paper only. It does not place orders. It does not join a morning list. A candidate stays off the pre-09:30 list until Cyrus promotes that candidate under section 10.

Two candidates are declared. They do not share a book, a ledger, a t-stat, or a kill. If one is later promoted, its name is the strategy name (`loud_tail_d8_v1` or `ab_ugly_tail_v1`), not the study name.

This file freezes the protocol. Numbers marked TODO are blanks on purpose. Filling a blank is a later addendum with its own fingerprint. Editing this covered body after the fingerprint above is a new study, not a rerun.

No code and no workflow ship in this commit. The ledgers in section 12 do not exist yet.

## 2. Why

The pretty open-to-close page is parked. `dashboard/updown/LEAK_AUDIT.md` (audit run 2026-10-06, verdict PASS) says `updown_o2c_v2` is clean of the v1 price-level leak and is still not a strategy. Check 7: the open-to-close edge uses the same-day 09:30 official open in the score and as the entry, so it is not tradable as printed. The same audit's no-same-day-open rerun, cited here and not rerun, is AUC about 0.52 and no after-fee long/short edge: dev −0.20% (t −6.3), holdout +0.07% (t 0.6). Those rows stand. This study does not refit that model.

`updown_expmove_v1` stays a risk column. It is not a candidate here.

`updown_rel1_hv_v1` (D8) stays on its own paper shadow. Its README already records the multiple-testing miss: cumulative t 2.02 against the bar 3.37. D8 keeps collecting under that README's kill/promote rule. This study does not promote D8 and does not put D8 on the pre-09:30 list.

The next try is pre-open edge cases. Two hypotheses are already peeked. Both are prior evidence. Neither is an out-of-sample pass, and neither sample enters a ledger of this study.

- Loud tail, on about four live D8 days (war room 2026-10-14): the loudest ~20% of predicted moves were about 79% wins and about +2.2% average, against a quiet book that was about flat, and about 85% of those loud names were shorts (fade moonshots). Hypothesis only. The ~20% figure is not the locked cut.
- AB ugly tail, `data/ab_backfill/AUDIT_6m_score_edge.md`, name-sessions 2026-02-19 through 2026-07-06, universe 1-week base rate 52%. As a buy-the-greens ranker the score does not help. The cited ugly-tail hit rates, which are not locked cuts: ≥3 red chips 60% (n 22,665, +8); 5-day mean reds ≥ 2.4 is 65% (n 17,266, +13); ≥3 red on 3+ days in a row is 68% (n 7,490, +16). The audit's red chip is LAG / peers↓ / ind↓ / sec↓ that session; white does not count. Close-to-close in that note agrees in sign (60% 1-week win, median close +1.6% against a universe median +0.1%). That close-to-close print is prior evidence. It is not this study's fill.

AB is a separate family from D8. The two are not merged. A merge would be a new study with a new name.

## 3. What is locked now, and what is still blank

| item | state |
| --- | --- |
| Paper only. No orders. No morning list until a promotion under section 10. | locked |
| Candidate names `loud_tail_d8_v1` and `ab_ugly_tail_v1`, separate ledgers | locked |
| A: D8 universe, frozen models, high-move half, side from d1, hold 1 session | locked |
| A: top X% by m1, and the d1 tail cuts | TODO |
| B: long-only ugly tail, prior-session inputs, cohort math in section 6 | locked |
| B: red-chip predicate and hold length H | TODO |
| Fill prices: 09:30 official open in, a later 09:30 official open out | locked |
| Fees 5 / 10 / 15 bp per side. Gate at 10 bp per side. | locked |
| Baselines: IWM open-to-open, RANDOM4 seed 20260813 | locked |
| Newey-West lag 5. Promotion t ≥ 3.37. Kill / promote in section 10. | locked |
| Freeze stamped before 09:25 ET. Miss stays missing. | locked |
| Parallel shadows or sequential | TODO |
| Auction or borrow cost as an extra reported column | TODO |
| First session date | not set |

Until the parallel-or-sequential TODO is filled, neither candidate freezes a day. A blank that belongs to one candidate blocks that candidate only. The shared TODO blocks both.

## 4. Candidate A — `loud_tail_d8_v1`

A is a filter on a D8 day that is already locked. It does not fit a model. It does not rewrite D8.

Universe, models, and parent book are the frozen D8 definitions in `research/updown_rel1_hv_v1/README.md`:

- Universe: top 1,000 US names by 20-day mean dollar volume, among names whose actual (unadjusted) prior close is at least $5, with at least 60 prior bars and at least $1M dollar volume.
- Models, trained once on rows dated 2018-03-29 through 2026-10-01, never retrained. sha256 from `research/updown_rel1_hv_v1/models/frozen_meta.json`: `d1_rel_oo1` `80190689c2f3f2388ea1558a7bdb1a4ec4eaac254e5fe7b66b422b0291e38716`, `m1_abs1` `176313c6a8a09e7baaa9c7d1351476f63ae05e70d71b6570d57bb9a05eb758a1`. `d1_rel_oo1` ranks the open-to-next-open return minus the industry median. `m1_abs1` predicts the absolute open-to-next-open move.
- Parent set: the high-move half. That is the names whose m1 is at or above that day's median of the 1,000, the `eligible` flag on that day's locked D8 `scores.csv`.

A reads that day's locked D8 score file and nothing fresher. If D8 has no lock for session N stamped before 09:25 ET, A misses N. A does not build its own scores.

Loud-tail rule, structure locked, cuts blank:

1. Rank the parent set by `m1`, highest first. Keep the top `x_pct` of that set. TODO `x_pct`. The peeked ~20% is not this number.
2. Inside that loud set, rank `d1`. Higher `d1` is the long side. Long when the `d1` percentile is above `d1_hi`. Short when it is at or below `d1_lo`. TODO `d1_hi` and `d1_lo`. D8's 0.9 / 0.1 cuts on the whole high-move half are the parent recipe. They are not these cuts.
3. Side comes only from that `d1` rank. No discretion. A name that is loud and not in either tail is not held.

Ties on a cutoff stay in, so the book may be slightly larger than the round count. Percentiles use average ranks for ties.

Weights: equal weight within each side. Long weights sum to +1. Short weights sum to −1. The peeked ~85% short count is a description of those four days. It is not a weight target. If one side is empty, that side sums to 0 and the other side stays at 1. If both sides are empty, the day is flat: realized return 0, and it is not a miss.

Hold is one session. The book is restated every session from that morning's locked scores.

## 5. Candidate B — `ab_ugly_tail_v1`

B is the AB red-chip ugly-tail family. It does not use the D8 models, the D8 scores, or the D8 1,000-name universe. It does not short.

Side is long only. The audit's hit rate is a long outcome (upside against downside, and a positive close-to-close median). A short of ugly names, or a short of pretty names, is a different candidate and is not this one.

Inputs are AB chip marks and AB scores from sessions strictly before the buy session. The eligible pool is names that have those prior-session marks. A missing mark stays missing. It is not imputed and it is not filled from a later reprint.

The predicate is blank. The addendum must name it before B's first session, using only the chip family in the audit (LAG, peers↓, ind↓, sec↓; white does not count) plus counts, a rolling window, or a streak of those marks. It may add a prior-bar price floor or a prior-bar dollar-volume floor. It may not add the buy session's open, high, low, close, volume, or VWAP. The three hit-rate rows in section 2 are examples of the family. They are not the predicate.

Each buy session N that has a filled predicate forms one cohort: the names that pass that morning, equal weight, weights sum to +1. No names pass means no cohort that morning. A session with no live cohort is flat: realized return 0, not a miss.

## 6. Fill

Both candidates fill at official opens. B does not fill close-to-close. The audit's close-to-close 1-week number stays prior evidence.

- Entry is the 09:30 ET official open of session N, Yahoo daily bars, split-adjusted, `auto_adjust` false. Dividends are not applied.
- Exit is the 09:30 ET official open of a later session. Same bar source.
- A exits at the next session's open. Hold is 1. Rebalance daily.
- B's cohort bought on N exits at the open H sessions later. TODO `H`, a positive integer. Overlapping cohorts are allowed. Each cohort has gross 1. The day's published return is the equal-weight average of the cohorts alive that day, so a larger H does not lever the book.

The open is the fill. It is not a feature. A name with no open print that session is a missing fill: counted as 0, listed, not replaced with a later price and not replaced with another ticker.

A consecutive stored jump above 3x or below 1/3, with no Yahoo split between those sessions, is flagged and that name's return is zeroed for the day. Same rule as D8.

## 7. Fees

Report three nets from the same weights: 5, 10, and 15 bp per side. The primary gate is **10 bp per side**. The 5 bp and 15 bp columns are printed. They are not gates.

Cost is bp per side times turnover. Turnover is the sum of absolute weight changes that day.

- A: one book. Turnover is Σ |w_N − w_{N−1}|, with w before the first session equal to 0.
- B: the same formula inside each cohort. A cohort pays on the day its weights leave 0 and on the day they return to 0. The published day is the average of the live cohorts' nets.

No notional is set. The Futubull per-order schedule is not applied. Auction and borrow are not in the gate. Whether a later addendum also prints an auction or borrow column is a TODO. That column, if added, sits beside the gate. It does not replace 10 bp per side, and it does not lower the promotion bar.

## 8. Baselines

Both baselines use the candidate's own sessions and the candidate's own hold. A's baseline is not compared with B's book.

IWM is the Yahoo split-adjusted IWM open-to-open return over the same entry and exit opens as that day's book (for B, the same average across live cohorts, which on a daily mark is that session's IWM open-to-next-open). No fee is charged on IWM. Cumulative IWM is the sum of those session returns on the candidate's realized locked days.

RANDOM4 is 1,000 paths. Path `k` uses `random.Random(20260813 + k)` for `k` from 0 through 999. Each session it draws, without replacement, a book of the same size as the candidate that session: A draws the same number of longs and the same number of shorts from that day's high-move half; B draws the same number of names, all long, from that morning's pre-cut eligible pool. A flat candidate session is a flat draw. Holds, fees, and the 10 bp gate match the candidate. The promotion comparison is the candidate's cumulative net at 10 bp against the median of the 1,000 path cumulative nets. The running record also prints that median, the 95th percentile, and the candidate's percentile.

Missed sessions are not draws. They stay on the miss list.

## 9. Holdout and multiple testing

Each candidate's ledger is its own sample. Discovery does not enter it. Out of the ledger: the peeked D8 loud-tail days, the AB audit window 2026-02-19 through 2026-07-06, and any pressure-test that chooses the TODO numbers. The pressure-test may read those samples. It may not call them the holdout.

The t-stat is Newey-West with lag 5 on the daily net at 10 bp per side. Lag 5 is the promotion statistic even when B's H is longer than 1. A longer lag may be printed beside it. It does not replace lag 5.

Two rows are always printed once the series exists:

- Cumulative t: Newey-West lag 5 on every realized locked day from that candidate's first lock through the latest.
- Confirm-window t: the same statistic on the same days. Those days are all after this protocol and after that candidate's threshold addendum. The row exists so a chart cannot mix in the discovery samples and still be labeled confirm. On a clean ledger the two numbers match. A mismatch fails the report.

The promotion bar is cumulative t ≥ **3.37**, the same cumulative multiple-testing bar D8 uses. This file does not re-derive 3.37.

Bonferroni. The family declared here is these two candidates. A conventional two-sided 5% Bonferroni critical value for a family of two is about 2.24. A one-sided 5% split across two tests is about 1.96. Both are weaker than 3.37, so declaring two names does not cut the bar. One-sided, 3.37 is about 0.00038 in the tail. That is the bar each candidate must clear on its own ledger. The two ledgers are not pooled and the better one is not swapped in after the fact.

Deflated Sharpe (Bailey and López de Prado) is reported at a promotion review on that same daily net at 10 bp per side. The trial count is 2, these two names, unless the threshold addendum recorded a larger trial count before the first lock. DSR is a disclosure. It is not a second gate and it does not lower 3.37.

A third recipe, a new cut family, or a merge of A and B is a new study. It is not an edit here, and it does not inherit this bar unchanged.

## 10. Kill / promote

Applied to each candidate alone. Copied from the D8 rule set on 2026-10-07 (`research/updown_rel1_hv_v1/README.md`), with the gate kept at 10 bp per side. One candidate's kill does not kill the other. Nothing here promotes automatically.

- **KILL** if, after ≥ 60 realized locked days, the cumulative net return at **10 bp per side** is negative.
- **Early KILL** if, after ≥ 20 realized days, the Newey-West t (lag 5) of daily net at 10 bp per side is ≤ −2.5.
- A killed candidate stops freezing new days. Its record stays as written.
- **PROMOTION REVIEW** is a decision by Cyrus only. The book stays paper until he decides. All of these must hold:
  - ≥ 120 realized locked days
  - live Newey-West t of daily net at 10 bp per side ≥ **3.37** (the cumulative t in section 9)
  - cumulative net beats the RANDOM4 median and IWM over the same realized locked days
  - it is still positive without its single best stock (drop the one ticker with the largest contribution to that cumulative net, and the sum must stay above 0)
- Missed days count as days without returns and are listed. The kill counts and the 120-day count are realized locked days, not missed days.

Passing the numbers does not place the book on a morning list. The list changes only after Cyrus promotes that named candidate.

## 11. Clock and the knowable-before-buy bar

All times US/Eastern.

No feature, membership rule, chip mark, score, or rank may read the buy session's open, high, low, close, volume, or VWAP. Prior-session bars and prior-session AB marks are allowed. The official open is used only as a fill, after the lock.

D8's own inputs stay as that README states them: bars dated before N, SPY and VIX closes dated before N, earnings counted only from the session after the event. A adds no same-day open on top.

Clock, same cutoffs as D8, so "before 09:25" is checkable:

| step | when |
| --- | --- |
| Earliest freeze of session N | 17:15 ET on the previous session, after that session's bars are final |
| Writer cutoff | refuses to write N at or after 09:20 ET on N |
| Push cutoff | an unpushed freeze is dropped at or after 09:24 ET |
| Lock deadline | every lock line is stamped before 09:25 ET on its day. A stamp at or after 09:25 ET is invalid |
| Quiet window | nothing is written 09:20–09:45 ET on a session day |
| Realized | after the exit session's close (that session ≥ 16:30 ET), appended to that candidate's realized file |
| Missed | a session with no valid lock by 09:25 ET gets one append-only miss line after 09:45 ET. It is never back-filled and it earns nothing |

## 12. Records

IRONCLAD, once a candidate is allowed to start:

- One ledger per candidate: `research/updown_edge_case_v1/loud_tail_d8_v1/LOCKS.jsonl` and `research/updown_edge_case_v1/ab_ugly_tail_v1/LOCKS.jsonl`. Append-only. One line per locked session. A later change may only add a line after the last line. No line is edited, reordered, or removed.
- Each lock line carries the session date, the candidate name, the freeze timestamp, `prev_sha256` (the hash of the ledger file before that line), and the buy and sell fingerprints. Nothing is fingerprinted retroactively. Sessions before that candidate's forward start are not written.
- Fingerprint covers buys and sells only. Canonical JSON, UTF-8, keys sorted, separators `,` and `:` , of `{"buys":[...],"sells":[...]}`. Each item is `{"ticker","side","weight"}` with `weight` a decimal to 8 places, half away from zero. Lists are sorted by ticker, then side. `buys` restates every name held after the open, including a name kept overnight. `sells` lists every name that was held and is off. The hash does not cover feature matrices, score dumps, earnings files, or model bytes.
- A day folder may hold the buy list, the sell list, and a one-page manifest of input hashes. It does not hold a second copy of D8's feature matrix. Realized P&L is recomputed from the frozen buy and sell lists. Those lists are not edited when the exit print arrives.
- `MISSED.jsonl` beside each ledger is append-only. A missed day is not also locked. A locked day is not back-filled into a gap.
- No backfill. A gap stays a gap. A later run does not fill a session that already failed the 09:25 deadline, and it does not fill any session before the forward start named in the threshold addendum.

This commit creates neither ledger. An empty ledger is not a filled session.

## 13. Non-goals

This study does not:

- edit h1, Factor Mine, Webull, flatten paths, or live tickets
- rewrite `dashboard/updown` open-to-close binaries or published o2c data
- change D8's models, picks, locks, kill rule, or README, or promote D8
- put D8, A, or B on the pre-09:30 morning list
- merge the AB family into D8
- rerun the leak audit or the AB 6-month audit
- treat `updown_o2c_v2` or `updown_expmove_v1` as a tradable book
- place orders or set a notional

## 14. Open TODOs

For a pressure-test before any live day. The answers land in an addendum. They do not land by editing this file. The addendum is committed before the first lock it authorizes, and it names the first session, which is a session after that commit.

1. A's exact loud-tail cuts: `x_pct`, `d1_hi`, `d1_lo`.
2. B's exact ugly-tail predicate (which chips, what count, what rolling window or streak) and the hold `H` in sessions.
3. Whether A and B run as parallel shadows or sequential. Sequential means the second candidate's forward start is after the first has been killed or has reached a promotion review, and it must be named up front.
4. Whether to print an auction and borrow add-on beside the 5 / 10 / 15 bp columns. The 10 bp gate stays either way.

A pressure-test that wants a third book, a short ugly-tail, a close-to-close fill, a same-day open feature, or a bar below 3.37 does not fit in this addendum. That is a new study.
