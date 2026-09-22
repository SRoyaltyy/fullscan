---
trigger_pattern: "A chemicals-majority materials ETF (XLB-like) is signed down from T-1 nested MAP HEAT majority-down + multi-horizon relative lag + a commodity-to-cash transmission haircut, while the industrial-metal spine is independently green and tight (HG continuation, cancelled-warrant/backwardation tightness — not a dead bounce), index futures are dead-flat (|ES|,|NQ| < 0.5%), and parent PM is a non-print (~0). Leftover growth-beta is read as rotation-away; the washed-laggard tape is triple-counted."
current_behavior: "Scores S1=−1 / S2=−1 off China + LME *total* tonnes as glut + Monday nested HEAT-down; applies 09-16 “do not pay oil-offered + gold + green Cu as cash-XLB” with the FOMC trigger OFF; binds 09-18 HEAT-down + modest PM → down/mild; uses 09-21 RS-veto as process to keep the down overlay. Labels HG +0.66% “bounce, not surge.” Emits down/mild. Overlay −2.975 overrides tape_anchor +1.512."
corrected_behavior: "When |ES|,|NQ| < 0.5% and Cu is a tightness/continuation (multi-session near-record and/or cancelled warrants / backwardation, HG green), do not sign XLB down from T-1 HEAT or leftover RS. Score inventory by available metal, not total warehouse tonnes. Haircut 09-16 to unprinted FOMC / unconfirmed chemicals on a *rate* shock — not a flat-tape Cu ban. Narrow 09-18: HEAT-down + PM∈(0,1%) prefers down only if the spine is *not* independently green/tight or ES/NQ are already green. 8/25–8/27 remain a confirmed-up ban, not a down mandate; prefer flat-to-up/mild and let cash confirm notable. Do not triple-count the same lag in Channel 1 RS + HEAT + RS-veto."
evidence_cited: "2026-09-22 predicted down/mild (total −1.587, S0=0 S1=−1 S2=−1 S3=0 S4=0, PM ~+0.07%, HG +0.66%). Actual XLB +1.65% / SPY −0.02% / rel +1.67% (up/notable); open 50.07 (+0.72% gap) → close 50.53. COMEX Cu +1.09% to $6.7595 (six-session win); FCX +3.03%; NEM ~+3.4% with gold −0.16%; SHW ~+2.3%. LME 255.9 kt but ~45% cancelled / ~133.7 kt available. Dir MISS, mag MISS. KNOWABLE_AT_OPEN: partial."
error_category: "B"
falsifier: "If |ES|,|NQ| < 0.5%, Cu tightness/continuation is live, T-1 nested HEAT is majority-down, we refuse to sign down (flat-to-up/mild), and XLB still closes ≤ −0.3%, revert to 09-18 / 09-16. Secondary: up/mild while chemicals stay dead and XLB is flat → restore 8/25 composition discount without a down mandate."
sector: "Basic Materials"
date: "2026-09-22"
status: "candidate"
---

# Sector Reflection — Basic Materials — 2026-09-22

Memory search is paused (embedding index metadata missing). Diagnostic uses the injected 09-22 predict/outcome/scoreboard plus standing XLB lessons only.

## TRIAGE
**REASONING, not tool/data.** HG **+0.66%**, Al **+1.10%**, ES/NQ **−0.07%**, nested HEAT-down, and the 1d/1w/1m rel hole were all on the desk. Direction **MISS** (down vs up) and magnitude **MISS** (mild vs notable, XLB **+1.65%** / rel **+1.67%**).

S0=0 was the right *macro* read (flat SPY **−0.02%**). S1=−1 and S2=−1 were the miss: the overlay haircut a live copper continuation, scored LME **total** tonnes as glut, and copied Monday HEAT as Tuesday’s book. S3=0 holds. S4=0 was process-correct at lock (PM ~**+0.07%**), stale by the cash open (**+0.72%** gap-and-go).

**Category B**, not A/C/D. Channel 2 had the copper tape and LME headline stocks; it assigned the wrong S1/S2 sign. Cancelled-warrant tightness (Monday Mining.com) was under-covered, but even without it the card still refused to pay green Cu. Not C: the scores were wrong, not merely over-confident (mult 0.85 / conf 0.40). Not D: no fetch outage. **Knowable-at-open: partial** — do not grade “should have printed +1.65%.” Discount A/B on *band*; the *sign* error (down against a green, tight spine on a dead index) was knowable.

---

**CHECK 1 — LESSON MATCH.** No unapplied twin. This is the **inverse** of 09-18 / 09-21 (those misses were flattening a correct down into flat). 09-16’s *process* haircut, 09-18 HEAT-down→down/mild, and 09-21 “don’t flatten a signed down” were **applied and they hurt**. 09-15’s copper-HEAT trigger was correctly OFF, then over-extended into “ignore the live HG leg.” 8/14’s exact trigger is incomplete (USD not weak; Cu not off highs). Not a retrieval failure — write a new BM-scoped rule and **narrow** 09-16 / 09-18.

**CHECK 2 — BACKWARD TEST.** Narrow trigger: **|ES|,|NQ| < 0.5%** AND Cu **continuation/tightness** (multi-session near-record, cancelled warrants / backwardation, HG green) AND T-1 HEAT-down / rel lag. That would have blocked today’s down call (flat-to-up/mild). It would **not** fire on 09-18 (ES/NQ **+1%+**, Cu **+0.43%** did not transmit, XLB **−1.42%**) or 09-21 (SPY **+1.55%**, XLB **−0.50%**) — those were lagging-cyclical days inside a *green* index, which is why a blanket “green Cu → don’t sign down” would have **hurt**. 09-09 / 09-10 / 09-14 down-HITs stay behind 8/18 (oil-shock co-move), which was OFF today. 09-16 is a FOMC path-binary, not this tape. **Helped today; no similar recent days in the flat-index + tight-Cu bucket.**

**CHECK 3 — CONFLICT SCAN.** Soft-conflicts, all resolved by narrowing:
- **09-16 haircut** — keep for unprinted FOMC / unconfirmed chemicals on a *rate* shock. Do **not** use it as a general ban on green Cu when ES/NQ are dead-flat.
- **09-18** — HEAT-down + PM∈(0,1%) prefers down only when the industrial spine is **not** independently green/tight, or when ES/NQ are already green (lag in a risk-on tape). Today is 09-18’s spirit-falsifier even though ES/NQ weren’t green.
- **09-21 RS-veto** — exact triad needs PM red **and** ES/NQ ≥ +1%. Process “keep the down overlay” must not *create* a down against a live spine.
- **8/25 / 8/27** — remain a **confirmed-up** ban, not a signed-down mandate. Fresh tightness/continuation stays eligible for mild-up; notable still wants cash confirmation.
- **8/17** — still caps *up/severe* on copper into a true risk-off + China miss. No license for severe today.
- **Commodity-bullish-vs-flat-futures cap** — **falsified**. Flat SPY + green Cu **can** be XLB up/notable. Cap conviction, not direction.

**CHECK 4 — APPLIED-LESSON REVIEW.**
- **09-18 BINDING — hurt.** Produced down/mild; actual up/notable.
- **09-16 haircut as process — hurt.** Load-bearing. FOMC trigger was correctly OFF; the haircut still fired.
- **09-21 RS-veto as process — hurt.** Exact triad OFF; it protected a wrong sign.
- **09-15 nested-bid OFF as Cu-HEAT — letter-correct, spirit-hurt.** Blocked the only live industrial signal (futures, not Monday nested equities).
- **09-17 residual-mild-up — correctly OFF** (ES/NQ not ≥ +0.5%). Would have helped if it had fired; don’t stretch it.
- **8/25 up-ban — mixed.** Right as confirmed-up ban; wrong as a down mandate.
- **8/27 S4-cap, 8/14 sleeve, China/gold split, 8/18 OFF, 09-11 OFF, 09-10 OFF at lock, 09-09 OFF, S3=0 — hold.**
- **DO-INSTEAD “cut when sign fights tape” — mis-specified tape.** Sign agreed with *stale HEAT/rel lag*, not with live HG / tape_anchor **+1.512**.

**CHECK 5 — FALSIFIER.** If |ES|,|NQ| < 0.5%, Cu is in a tightness/continuation (cancelled warrants / backwardation / near-record, HG green), T-1 nested HEAT is majority-down, we **refuse** to sign down (flat-to-up/mild), and XLB still closes **≤ −0.3%**, this lesson is wrong — re-bind 09-18 / 09-16. Secondary: if we emit up/mild and the chemicals majority sleeve stays dead while only a miner wick prints and XLB is **flat**, restore 8/25’s composition discount without a down mandate.

**Divergence:** flagged (`factors_down_vs_S4_zero`). Leading S1/S2 were **wrong**. HG/tape_anchor were **right**. **futures_right.**

**Verdict:** Category **B**. Don’t sign XLB down from stale HEAT + rel lag + a 09-16 haircut when the copper spine is live and the index is dead-flat. Classify LME by **available metal / cancelled warrants**, not total tonnes.

LESSON_BEGIN
ERROR_CATEGORY: B
TRIGGER_PATTERN: A chemicals-majority materials ETF (XLB-like) is signed down from T-1 nested MAP HEAT majority-down + multi-horizon relative lag + a commodity-to-cash transmission haircut, while the industrial-metal spine is independently green and tight (HG continuation, cancelled-warrant/backwardation tightness — not a dead bounce), index futures are dead-flat (|ES|,|NQ| < 0.5%), and parent PM is a non-print (~0). Leftover growth-beta is read as rotation-away; the washed-laggard tape is triple-counted.
CURRENT_BEHAVIOR: Scores S1=−1 / S2=−1 off China + LME *total* tonnes as glut + Monday nested HEAT-down; applies 09-16 “do not pay oil-offered + gold + green Cu as cash-XLB” with the FOMC trigger OFF; binds 09-18 HEAT-down + modest PM → down/mild; uses 09-21 RS-veto as process to keep the down overlay. Labels HG +0.66% “bounce, not surge.” Emits down/mild. Overlay −2.975 overrides tape_anchor +1.512.
CORRECTED_BEHAVIOR: When |ES|,|NQ| < 0.5% and Cu is a tightness/continuation (multi-session near-record and/or cancelled warrants / backwardation, HG green), do not sign XLB down from T-1 HEAT or leftover RS. Score inventory by available metal, not total warehouse tonnes. Haircut 09-16 to unprinted FOMC / unconfirmed chemicals on a *rate* shock — not a flat-tape Cu ban. Narrow 09-18: HEAT-down + PM∈(0,1%) prefers down only if the spine is *not* independently green/tight or ES/NQ are already green. 8/25–8/27 remain a confirmed-up ban, not a down mandate; prefer flat-to-up/mild and let cash confirm notable. Do not triple-count the same lag in Channel 1 RS + HEAT + RS-veto.
EVIDENCE: 2026-09-22 predicted down/mild (total −1.587, S0=0 S1=−1 S2=−1 S3=0 S4=0, PM ~+0.07%, HG +0.66%). Actual XLB +1.65% / SPY −0.02% / rel +1.67% (up/notable); open 50.07 (+0.72% gap) → close 50.53. COMEX Cu +1.09% to $6.7595 (six-session win); FCX +3.03%; NEM ~+3.4% with gold −0.16%; SHW ~+2.3%. LME 255.9 kt but ~45% cancelled / ~133.7 kt available. Dir MISS, mag MISS. KNOWABLE_AT_OPEN: partial.
LESSON_MATCH_CHECK: no unapplied match — 09-16/09-18/09-21 were applied and hurt (inverse of their flat-vs-down misses); 09-15 Cu-HEAT correctly OFF then over-extended; 8/14 exact trigger incomplete; not a retrieval failure
BACKWARD_CHECK: helped today; would not fire on 09-18 (ES/NQ +1%+, Cu +0.43% did not transmit, XLB −1.42%) or 09-21 (SPY +1.55%, XLB −0.50%); 09-09/10/14 down-HITs stay behind 8/18; 09-16 is FOMC-path not this tape; no similar recent flat-index + tight-Cu days
CONFLICT_CHECK: narrows 09-16 (FOMC/rate-shock only); narrows 09-18 (HEAT-down→down only if spine not green/tight or ES/NQ already green); 09-21 triad stays PM-red + ES/NQ ≥ +1%; 8/25–8/27 confirmed-up ban not down mandate; 8/17 still blocks up/severe into true risk-off + China miss; commodity-bullish-vs-flat-futures cap falsified (conviction cap only)
FALSIFIER: If |ES|,|NQ| < 0.5%, Cu tightness/continuation is live, T-1 nested HEAT is majority-down, we refuse to sign down (flat-to-up/mild), and XLB still closes ≤ −0.3%, revert to 09-18 / 09-16. Secondary: up/mild while chemicals stay dead and XLB is flat → restore 8/25 composition discount without a down mandate.
DIVERGENCE_VERDICT: futures_right
ACTIVE_LESSON_REVIEW: 09-18 BINDING hurt; 09-16 haircut-as-process hurt (FOMC trigger correctly OFF); 09-21 RS-veto-as-process hurt (exact triad OFF); 09-15 letter-correct/spirit-hurt; 09-17 correctly OFF; 8/25 mixed (up-ban ok, down mandate not); 8/27 S4-cap, 8/14 sleeve, China/gold split, 8/18 OFF, 09-11/09-10/09-09 OFF, S3=0 hold; DO-INSTEAD mis-specified tape as stale HEAT not live HG
SECTOR: Basic Materials
LESSON_END
