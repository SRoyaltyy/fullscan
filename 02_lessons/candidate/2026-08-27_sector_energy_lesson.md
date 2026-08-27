---
trigger_pattern: "Energy/XLE is on a multi-session oil-down / geo-premium-fade spine, but the live same-morning oil shock is leftover rather than a fresh ≥1% CL/BZ dump, the EIA/PCE (or equivalent inventory/inflation) print is already out and small or two-sided, S0 is mildly risk-on, and S4 is 0 or a fading 1d bounce. Absolute XLE then prints flat-to-mild while relative vs SPY stays down."
current_behavior: "The model keeps S1 at −2 from carried builds/OPEC/IEA plus yesterday’s oil print, leaves the body dated as the prior session so spent PCE/EIA are still “today,” and lets pipeline leading_sum/total_score (~−8 / −8.55) auto-print down/notable even when HORIZON_3D is already down:mild, conf is already shrunk, and S4=0 with divergence_flagged."
corrected_behavior: "After live-oil verify, score S1 from *today’s* residual shock size, not the carried complex. If EIA/PCE are already printed, do not treat them as pending event risk; a ~0.1 mbbl crude build is not another −2. Keep direction down when 3d/1w rel and outflows confirm the fade, but do not emit notable: default the 1d band to flat (mild only if a fresh ≥1% oil print is still live). Do not let |leading_sum|≥8 override S4=0. Express the call as XLE vs SPY."
evidence_cited: "2026-08-27 predicted down/notable (S0 0 / S1 −2 / S2 −1 / S3 −1 / S4 0, mult 0.9, total −8.55, conf 0.55). Actual XLE −0.224%, SPY +0.655%, rel −0.880% → down/flat. EIA week ending 8/21 was +0.1 mbbl to 428.9 (already 8/26); product draws were a crack offset. Intraday low ~61.55 failed; close ~62.27. Dir HIT, mag MISS. Energy mag hit-rate ~0.2–0.33."
error_category: "B"
falsifier: "If leftover oil-down + spent/tiny EIA + S4=0/fading bounce recurs and XLE still closes notable down (|pct|≥1%) in 2 of 3 comparable episodes, drop the cap. Also revise if a fresh same-morning CL/BZ ≤ −1% is live and this rule still forces flat."
sector: "Energy"
date: "2026-08-27"
status: "candidate"
---

# Sector Reflection — Energy — 2026-08-27

Memory search is paused (index metadata missing). Used the injected Energy predict/outcome/scoreboard plus local fullscan Energy lessons.

**Triage:** REASONING failure (Category **B** — misweighted evidence). Not a missing-search miss, and not a true-shock miss. Direction was right; the 1d **band** was too hot. Live oil sign was verified down. S0/S2/S3/S4 were right. S1 = −2 treated a leftover premium-fade as a fresh oil shock, and the pipeline then printed **notable** from `leading_sum -8` / `total -8.55` even though Σ(S0..S4)×0.9 = **−3.6** and HORIZON_3D was already `down:mild`. Calendar copy-paste (body still “2026-08-26”, PCE/EIA as “today”) was a contributing stale-input, but the operational error is weighting/band, not a new fetch outage.

**CHECK 1 — Lesson match.** Partial, not a clean duplicate. Closest: 08-13 Energy (oil-down spine → cap absolute at mild/flat, relative is the tell) — **does not fully fire** because 1w rel was **−1.42%**, not >+4%. Cross-sector pipeline rule (S4=0 + `divergence_flagged` → do not auto-print notable) **did fire** and was **not applied** (retrieval miss). 08-25 Energy candidate is a grader `None/None` bug — **not this run** (scoreboard correctly recorded down/notable vs −0.22%). 08-11/08-14/08-21 Energy lessons were checked and correctly not used as up-oil playbooks. New lesson is the leftover-fade / spent-EIA magnitude cap, not another Hormuz-up rule.

**CHECK 2 — Backward test.** Narrow cap (leftover oil-down + spent/tiny EIA + non-confirming 1d tape → **do not emit notable**) would have **helped 08-27** and is consistent with **08-13** (down/mild, abs flat, rel down). It would **not hurt 08-25** (live dump, XLE −1.66%, notable was correct — fresh shock, not leftover). **08-26** was a direction miss on a green bounce, a different error; this rule does not rewrite that day into a hit. A blanket “never notable on oil-down” **fails** the backward test.

**CHECK 3 — Conflict.** None if scoped tightly. 08-14 green-oil escalation does not fire (oil down). 08-12 stale-run cap does not fire (1w rel negative). 08-21 oil-up/XLE-down decoupling is inverted (aligned lower). 08-11 live-oil verify remains the sign check, not a magnitude license. 08-13 remains the “prior Hormuz run cushions abs XLE” rule; this is the sibling for **spent inventory/calendar + leftover fade + risk-on S0 cancelling absolute**.

**CHECK 4 — Applied-lesson.** 08-11 live-oil verify: **applied, helped direction**. 08-14 / 08-12 / 08-21: **correctly not fired**. Energy experiment (keep direction, shrink confidence after mag misses): **applied** (conf 0.55, mult 0.9) — helped the dir call, **did not cut the 1d band**. S4=0 + divergence → no notable: **not applied, hurt**. Refiner-not-carrying-XLE: applied and held.

**CHECK 5 — Falsifier.** If this leftover-fade + spent/tiny EIA + S4 not confirming setup recurs and XLE still closes **notable down** (|pct| ≥ 1%) in **2 of 3** comparable episodes, the cap is too tight and must be revised. Also wrong if a **fresh** same-morning CL/BZ ≤ −1% is live and the rule still forces flat.

**Verdict:** Dir **HIT**, mag **MISS**, relative **HIT**. `KNOWABLE_AT_OPEN = partially` (EIA +0.1M and spent PCE were knowable; SPY +0.66% and the 61.55 bounce were not). Discount A; do not discount B. Divergence JSON vs prose: trust the oil spine — **leading_right**. Official 1d band should have been **down/flat** (at most mild), matching HORIZON_3D, not pipeline notable.

LESSON_BEGIN
ERROR_CATEGORY: B
TRIGGER_PATTERN: Energy/XLE is on a multi-session oil-down / geo-premium-fade spine, but the live same-morning oil shock is leftover rather than a fresh ≥1% CL/BZ dump, the EIA/PCE (or equivalent inventory/inflation) print is already out and small or two-sided, S0 is mildly risk-on, and S4 is 0 or a fading 1d bounce. Absolute XLE then prints flat-to-mild while relative vs SPY stays down.
CURRENT_BEHAVIOR: The model keeps S1 at −2 from carried builds/OPEC/IEA plus yesterday’s oil print, leaves the body dated as the prior session so spent PCE/EIA are still “today,” and lets pipeline leading_sum/total_score (~−8 / −8.55) auto-print down/notable even when HORIZON_3D is already down:mild, conf is already shrunk, and S4=0 with divergence_flagged.
CORRECTED_BEHAVIOR: After live-oil verify, score S1 from *today’s* residual shock size, not the carried complex. If EIA/PCE are already printed, do not treat them as pending event risk; a ~0.1 mbbl crude build is not another −2. Keep direction down when 3d/1w rel and outflows confirm the fade, but do not emit notable: default the 1d band to flat (mild only if a fresh ≥1% oil print is still live). Do not let |leading_sum|≥8 override S4=0. Express the call as XLE vs SPY.
EVIDENCE: 2026-08-27 predicted down/notable (S0 0 / S1 −2 / S2 −1 / S3 −1 / S4 0, mult 0.9, total −8.55, conf 0.55). Actual XLE −0.224%, SPY +0.655%, rel −0.880% → down/flat. EIA week ending 8/21 was +0.1 mbbl to 428.9 (already 8/26); product draws were a crack offset. Intraday low ~61.55 failed; close ~62.27. Dir HIT, mag MISS. Energy mag hit-rate ~0.2–0.33.
LESSON_MATCH_CHECK: Partial match to 08-13 Energy (oil-down → cap abs at mild/flat) — 1w rel >+4% not met, so not a duplicate. Matches the cross-sector S4=0 + divergence_flagged → no notable pipeline rule, which was not applied (retrieval miss). 08-25 Energy None/None grader bug does not match this correctly graded mag miss. New lesson is the leftover-fade / spent-tiny-EIA band cap.
BACKWARD_CHECK: Helps 08-27; consistent with 08-13 (down/mild, abs flat, rel down). Does not hurt 08-25 (fresh dump, −1.66%, notable HIT). 08-26 remains a separate direction miss on a green bounce. A blanket never-notable-on-oil-down rule would fail the backward test.
CONFLICT_CHECK: none if scoped to leftover fade + spent/tiny EIA + non-confirming S4. 08-14 green-oil, 08-12 stale-run (> +4% 1w), and 08-21 oil-up/XLE-down do not fire. 08-11 remains sign verification only. 08-13 stays the prior-Hormuz-run cushion; this is the spent-catalyst leftover-fade sibling.
FALSIFIER: If leftover oil-down + spent/tiny EIA + S4=0/fading bounce recurs and XLE still closes notable down (|pct|≥1%) in 2 of 3 comparable episodes, drop the cap. Also revise if a fresh same-morning CL/BZ ≤ −1% is live and this rule still forces flat.
DIVERGENCE_VERDICT: leading_right
ACTIVE_LESSON_REVIEW: 08-11 live-oil verify applied, helped direction. 08-14 / 08-12 / 08-21 correctly not fired. Energy keep-dir/shrink-conf experiment applied (0.55) — insufficient for the band. S4=0 + divergence no-notable rule not applied, hurt. Refiner-not-carrying-XLE held.
SECTOR: Energy
LESSON_END
