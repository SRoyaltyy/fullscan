---
trigger_pattern: "A chemicals-majority materials ETF (XLB-like) prints green premarket vs ES inside ±0.3% (relative bid) on a two-sided sector board (XLE and/or defensives green vs XLK/NQ ≤ −0.5%), while Hormuz/oil risk is live but crude is offered or mixed rather than a metals-dump spike, and leading S0–S3 are net-negative from hawkish-rates / HEAT-down / oil-co-move against a positive S4/PM — the card still emits down, treating 8/25 “cannot go up” and the 9-14 metals-co-move rule as a down mandate."
current_behavior: "Scored S0=−1 / S1=−1 / S2=−1 / S3=0 / S4=+0.5, cut confidence to 0.35, kept mild, flagged divergence, then followed the factor sign to down/mild. Hormuz was stacked as S0 risk-off + 9-14 co-move + index_carry −1.692 + llm_overlay −3.825. Skill multiplier S4=0.0 zeroed the only confirming tape. Actual XLB +0.566% / SPY −0.012% / rel +0.578% (up/mild); LIN/SHW/APD carried, FCX/NEM lagged."
corrected_behavior: "When XLB PM is green vs ES inside ±0.3% and the board is two-sided with oil offered/mixed, do NOT emit down. 8/25–8/27 forbids confirmed-up; the residual is FLAT (or flat/mild), not down. Count Hormuz once as an energy/XLE object, not as four XLB-bearish votes. 8/18 still forbids paying Cu/Au as an up floor; it does not require a down call. If sign fights live S4/PM, bind direction to flat and cut conviction; do not let skill_multiplier S4=0 erase a green PM. Score S2 as mixed (ETF-up / HEAT-down PARTIAL), not −1."
evidence_cited: "2026-10-13 predicted down/mild (total −1.893, conf 0.35–0.48, divergence True) vs XLB +0.566% / SPY −0.012% / rel +0.578%; open 49.70 ≈ close 49.72. XLE ~+1.8%; LIN +0.98%, SHW +1.22%, APD +1.79%; FCX −1.19%, NEM −1.33%; COMEX Cu settle −0.31%; WTI $94.14 offered vs Brent $105.69. Dir MISS, mag HIT. Repeat of 10-02/10-08 sign-vs-tape, different sleeve (chemicals carry, not metals impulse)."
error_category: "B"
falsifier: "If this trigger recurs, we emit flat, and XLB still closes ≤ −0.3% (or rel ≤ −0.3%) on 2 of the next 3 such days, restore the Hormuz metals-co-move down sign and revise this lesson rather than defend it."
sector: "Basic Materials"
date: "2026-10-13"
status: "candidate"
---

# Sector Reflection — Basic Materials — 2026-10-13

Memory search is paused (index metadata missing). This uses the injected 10-13 card, deterministic actuals, on-disk BM candidates (10-02 / 10-01 / 09-22–09-28), and standing XLB/Hormuz rules only.

## TRIAGE
**REASONING, not tool/data.** Green XLB PM **+0.76%**, ES **−0.09%**, NQ **−0.73%**, XLE/XLP/XLRE green vs XLK, hawkish minutes, Hormuz live with crude **offered**, HEAT-down, and the divergence flag were all on the desk. Direction **MISS** (down vs up); magnitude **HIT** (mild vs **+0.566%**).

Not **D**: no fetch outage; Cu PM **+0.66%** → settle **−0.31%** is a live fade, not a missing quote. Not **C**: confidence was already cut to **0.35** and the calendar size-gate kept **mild** — the band was right; the **sign** was wrong. Not **A**: chemicals-majority composition (LIN ~13%) and the two-sided board were standing knowledge (8/25, 09-08). **Category B:** S0 Hormuz/risk-off, S1 metals/HEAT, and S2 breadth-down were overweighted; S4/PM — the only sleeve that matched cash — was capped then **zeroed** (`skill_multipliers.S4 = 0.0`).

**KNOWABLE_AT_OPEN: partial.** Discount A/B for LIN/SHW/APD vs FCX/NEM and the Cu reverse. Do **not** discount signing **down** against a green sector PM vs flat ES on a two-sided board.

---

**CHECK 1 — LESSON MATCH.** Partial match, **enforcement gap**, not a clean retrieval miss. The card itself restated the 10-02/10-08 DO-INSTEAD (“cut conviction when sign fights sector tape; prefer **flat/mild** on conflict”) and applied only the conviction/band half; the pipeline still printed **down**. 10-02’s WHEN does **not** fire (needs green ES/NQ ≥ +0.5%, unprinted binary, PM non-print). 8/28 already says 8/25–8/27 forbids **up**, residual is **flat not down** — but its WHEN requires S0=S1=0 and **no Hormuz/oil>$90**, so it was treated as OFF. 09-08 composition is the **inverse** (don’t pay minority Cu as +1 when oil hurts chemicals). Same-day **10-13 general** (NQ-only + two-sided board + offered oil → SPX flat) was **not** available at the morning lock. New BM-scoped rule: green XLB PM vs flat ES + two-sided board + offered Hormuz → **flat**, not down.

**CHECK 2 — BACKWARD TEST.** **Helped 10-13** (flat vs down/miss). **Helped 10-08** only if gated on *sign-vs-PM conflict*; 10-08 PM was a **non-print**, so a strict green-PM gate would **not** have fired — don’t pretend it fixes that metals-impulse day. **Would not fire on 10-01 / 09-28 / 09-24** (those HITs had red ES/NQ beyond the flat band and/or no green XLB PM vs ES). **Would not flatten 09-22** under this gate (PM ~0, Cu-tightness is 09-22’s own rule). Net: selective, not a blanket flatten.

**CHECK 3 — CONFLICT SCAN.** Conflicts with a literal **9-14 / 8/18** “metals co-move; don’t pay a commodity floor” read as a **down mandate**. **Resolution:** 8/18 forbids paying Cu/Au as an **up floor** when oil **is** the shock; it does **not** require signing XLB down when PM is green and the majority sleeve is chemicals. Conflicts with **09-24** (keep S0 down on red ES/NQ into chemicals). **Resolution:** 09-24 needs |ES| **or** |NQ| ≥ 0.5% **and** red as **index** confirmation; today ES is inside **±0.3%**, only NQ is red — 09-24 **OFF**. Compatible with 8/17 (caps **up/severe**, residual flat) and 8/25–8/27 (no confirmed-up). Narrow 8/28: residual-is-flat still binds when 8/25 is ON **even if** Hormuz is a headline, provided oil is offered and XLB PM is green.

**CHECK 4 — APPLIED-LESSON REVIEW.** **9-14/8/18 Hormuz co-move — applied, hurt** (miners did co-move; ETF did not). **8/25–8/27 S4 cap — applied, hurt** (S4 +0.5 then skill **S4=0.0** zeroed the confirming tape; residual became down). **8/17 overlay — correctly not fully bound** (no same-morning China miss). **10-02 binary — correctly OFF.** **10-02/10-08 DO-INSTEAD — cited; helped magnitude, hurt direction.** **Calendar size-gate — helped** (mild). **8/28 residual-flat — not applied** (WHEN scoped off by live Hormuz). **09-08 composition — not inverted**; chemicals were the live book and were scored as HEAT-down instead.

**CHECK 5 — FALSIFIER.** If this trigger recurs and we emit **flat**, and XLB still closes **≤ −0.3%** (or rel ≤ −0.3%) on **2 of the next 3** such days, the Hormuz metals-co-move **down** sign should be restored and this lesson revised, not defended.

**Divergence:** flagged. Leading sum **−2.5** was wrong-signed; S4/PM **+0.76%** and tape_anchor **+3.624** matched cash **+0.57% / rel +0.58%**. **futures_right.**

**Verdict:** Direction miss, magnitude hit. Category **B**. Do not treat 8/25 + Hormuz co-move as a down mandate when XLB PM is green vs flat ES on a two-sided, oil-offered board.

LESSON_BEGIN
ERROR_CATEGORY: B
TRIGGER_PATTERN: A chemicals-majority materials ETF (XLB-like) prints green premarket vs ES inside ±0.3% (relative bid) on a two-sided sector board (XLE and/or defensives green vs XLK/NQ ≤ −0.5%), while Hormuz/oil risk is live but crude is offered or mixed rather than a metals-dump spike, and leading S0–S3 are net-negative from hawkish-rates / HEAT-down / oil-co-move against a positive S4/PM — the card still emits down, treating 8/25 “cannot go up” and the 9-14 metals-co-move rule as a down mandate.
CURRENT_BEHAVIOR: Scored S0=−1 / S1=−1 / S2=−1 / S3=0 / S4=+0.5, cut confidence to 0.35, kept mild, flagged divergence, then followed the factor sign to down/mild. Hormuz was stacked as S0 risk-off + 9-14 co-move + index_carry −1.692 + llm_overlay −3.825. Skill multiplier S4=0.0 zeroed the only confirming tape. Actual XLB +0.566% / SPY −0.012% / rel +0.578% (up/mild); LIN/SHW/APD carried, FCX/NEM lagged.
CORRECTED_BEHAVIOR: When XLB PM is green vs ES inside ±0.3% and the board is two-sided with oil offered/mixed, do NOT emit down. 8/25–8/27 forbids confirmed-up; the residual is FLAT (or flat/mild), not down. Count Hormuz once as an energy/XLE object, not as four XLB-bearish votes. 8/18 still forbids paying Cu/Au as an up floor; it does not require a down call. If sign fights live S4/PM, bind direction to flat and cut conviction; do not let skill_multiplier S4=0 erase a green PM. Score S2 as mixed (ETF-up / HEAT-down PARTIAL), not −1.
EVIDENCE: 2026-10-13 predicted down/mild (total −1.893, conf 0.35–0.48, divergence True) vs XLB +0.566% / SPY −0.012% / rel +0.578%; open 49.70 ≈ close 49.72. XLE ~+1.8%; LIN +0.98%, SHW +1.22%, APD +1.79%; FCX −1.19%, NEM −1.33%; COMEX Cu settle −0.31%; WTI $94.14 offered vs Brent $105.69. Dir MISS, mag HIT. Repeat of 10-02/10-08 sign-vs-tape, different sleeve (chemicals carry, not metals impulse).
LESSON_MATCH_CHECK: partial match to 10-02/10-08 DO-INSTEAD (prefer flat/mild when sign fights tape) — retrieved and applied to conviction/band only, not sign (enforcement gap, not a missing lesson); 8/28 residual-is-flat matches the spirit but WHEN excludes live Hormuz/oil>$90 so it was treated as OFF; 09-08 is the inverse composition case; 10-13 general NQ-only+two-sided-board was not available at the morning lock — write a BM-scoped residual-flat rule
BACKWARD_CHECK: helped 10-13; would not fire on 10-01/09-28/09-24 HITs (red ES/NQ beyond the flat band and/or no green XLB PM vs ES); 10-08 PM was a non-print so a strict green-PM gate would not have saved that metals-impulse miss; 09-22 stays on its own Cu-tightness rule
CONFLICT_CHECK: conflicts with literal 9-14/8/18 as a down mandate — resolution: 8/18 blocks an up floor, residual is flat when PM is green and oil is offered; conflicts with 09-24 — resolution: 09-24 needs |ES| or |NQ| ≥ 0.5% red as index confirmation, today ES inside ±0.3% so 09-24 OFF; narrows 8/28 so residual-flat still binds when 8/25 is ON even if Hormuz is a headline
FALSIFIER: If this trigger recurs, we emit flat, and XLB still closes ≤ −0.3% (or rel ≤ −0.3%) on 2 of the next 3 such days, restore the Hormuz metals-co-move down sign and revise this lesson rather than defend it.
DIVERGENCE_VERDICT: futures_right
ACTIVE_LESSON_REVIEW: 9-14/8/18 Hormuz co-move applied and hurt; 8/25–8/27 S4 cap applied and hurt (S4 skill 0.0); 8/17 overlay correctly not fully bound (no China miss); 10-02 binary correctly OFF; 10-02/10-08 DO-INSTEAD cited — helped mag, hurt dir; size-gate helped mild; 8/28 residual-flat not applied (WHEN scoped off); 09-08 composition not inverted
SECTOR: Basic Materials
LESSON_END
