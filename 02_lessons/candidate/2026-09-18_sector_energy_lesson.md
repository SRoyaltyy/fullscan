---
trigger_pattern: "Energy/XLE in a multi-session oil-fade continuation (same geo-premium-fade cluster already counted, no fresh EIA/OPEC), live crude offered only ~1–2% (not a collapse), PM:XLE red but |PM|<1%, large-caps confirm the barrel in premarket, nested refiners still bid / cracks extreme, and 1m relative is not crowded."
current_behavior: "Applied live-oil verify, 09-03 signed-down, 08-27 S1=−1, mag-discipline/size_gate, and nested refiners; still mapped S1=−1 with S0=S2=S3=S4=0 plus PM −0.56% onto a mild ETF down, copying a 1–2% barrel increment onto XLE."
corrected_behavior: "Keep direction down when live oil is offered and PM majors confirm (09-03). Prefer the flat band, not mild, when all of: oil increment sub-2%, |PM:XLE|<1% red, same cluster already printed, refiners/XOM are a live cushion, mag hit-rate <0.4 / size_gate on, 1m rel not ≥+8%. Do not copy CL’s 1–2% onto the ETF print. Mild requires oil extending ≳2% with transmitting names, PM:XLE ≤ −1%, or live S4 extension. Continue shrinking confidence. Do not fire on unprinted FOMC + crowded leftover-shock days (09-16) or 1w/1m exhaustion sleeves (09-03)."
evidence_cited: "2026-09-18 predicted down/mild vs XLE −0.2637% / SPY −0.1193% / rel −0.1443% (down/flat). Oil −1 to −2% third session on limited-disruption hopes; PM −0.56% mean-reverted (64.20→64.31); CVX/COP ~−1%, XOM ~flat, MPC ~+0.6%. S1=−1 correct. Dir HIT, mag MISS by ~4 bp vs the 0.3% mild gate."
error_category: "C"
falsifier: "Same continuation setup (third-session ~1–2% oil fade, |PM:XLE|<1% red, refiners/XOM cushion, no fresh EIA/OPEC, mag hit-rate <0.4) but XLE still closes in the mild band (≤ −0.3% and ≥ −1.0%) without oil extending beyond ~2% — then the flat preference is too tight and mild remains the right cap."
sector: "Energy"
date: "2026-09-18"
status: "candidate"
---

# Sector Reflection — Energy — 2026-09-18

Memory index is paused this run (different embedding provider); this uses the injected predict, outcome, scoreboard, and Energy candidate/active lessons only.

## TRIAGE
Reasoning, not tool/data. Direction **HIT** (down vs XLE **−0.26%**). Magnitude **MISS** (predicted **mild** vs actual **flat**; |XLE| **26 bp**, ~4 bp inside the &lt;0.3% flat gate). Live oil **did** fall a third session ~1–2% on East-West repair/workaround hopes — the morning’s S1 spine. Cash energy **did not match the barrel**: oil −1 to −2%, ETF −0.26%, PM −0.56% bought back (open **64.20** → close **64.31**). Cushion = refiners (MPC ~+0.6%, VLO flat-green) and XOM recovering to unchanged.

08-11 live-oil verify **caught** the stale Channel 1 CL=F **−6.31%** / Finviz $104.16; live sign stayed DOWN ~1–2%. Tape_anchor still carried leftover Finviz CL **−1.59%**, but that increment **matched** live oil, so it did not cause the miss. Do not call this **D**.

S0–S4 were right. The miss is **transmission**: they treated a sub-1% red PM + offered barrel as a **mild ETF** print. KNOWABLE_AT_OPEN was **partial** (sign yes, size no) — discount A/B for demanding a 26 bp close; the residual is overextended band/confidence → **C**.

## CHECK 1 — LESSON MATCH
Closest: **08-27** (after a green XLE day / S4=0 / leftover oil-down, S1 at most −1, cap **mild/flat**) — **applied as mild**, not taken to flat. **08-28** transmitting unless-clause (WTI ≥~1% **and** XOM/COP red PM) **kept direction** — that helped, it is not this mag miss. **09-08/09-09** only cap notable→mild. **09-17 leftover-S4** does **not** match: leftover 1d rel was **−0.43%** (not ≤ −1.5%), PM **−0.56%** (not ~flat); they correctly left 09-17’s “allow mixed/up when PM is flat” **OFF**. **09-03** emit-signed-down **matched and was applied** (direction). **09-04** S1+S4 aligned-negative **did not fire** (S4=0). Not a retrieval failure. New card is a **band** refinement of 08-27 + the open keep-dir experiment, not another direction rule.

## CHECK 2 — BACKWARD TEST
A hard “must emit flat” from a **4 bp** miss would be one unlucky gate. Narrowed: third-session **already-counted** oil-fade, oil only ~1–2%, |PM:XLE|&lt;1% red, refiners/XOM cushion, 1m rel **not** crowded, no fresh EIA/OPEC → **prefer down/flat**, mild only if oil extends or PM ≤ −1%.

- **09-16** (−2.88% notable, crowded, unprinted FOMC): must **not** fire — would hurt.
- **09-17** (+0.70%): different object (leftover S4 vs repair).
- **08-27** (−0.22% flat vs notable): **helped**.
- **09-03** (−0.74% mild, 1w/1m extended): must **not** fire (exhaustion sleeve; flat would miss the other way).
- **09-04** (−0.87%, S4 live): must **not** fire.

**Mixed unless tightly gated.** Thin boundary miss — do not promote a blind flat rule.

## CHECK 3 — CONFLICT SCAN
- **09-03** signed-down: keep **direction**; this only cuts the **band**.
- **09-04**: still no down-from-S4; PM confirmation remains the 09-03 direction license.
- **08-28**: unless-transmitting still forbids flattening **direction**.
- **08-27**: refine “mild/flat” → **prefer flat** when |PM|&lt;1% and refiners cushion.
- **09-16**: exclude unprinted FOMC + crowded 1m.
- **09-17**: PM was not flat; no clash.
- Open **sector_energy** experiment (keep dir, shrink conf): **complements** — also shrink the graded band.

## CHECK 4 — APPLIED-LESSON REVIEW
- **08-11** live-oil verify: applied, **helped**.
- **09-03** emit-signed-down: applied, **helped direction**.
- **09-17 leftover-S4** (non-force): applied, **helped** (no 09-16 −2.44% reuse); mixed/up clause correctly **off**.
- **09-08/09-09** mag-discipline + size_gate: applied, **helped** (blocked notable), still **one band high**.
- **08-27** S1 not −2: applied, **helped**.
- **08-28** transmitting unless: applied, **helped direction**.
- **Refiner nested/dampen**: applied, **helped** (didn’t flip to up); that cushion **is** why mag was flat.
- **S0=0** (no energy tailwind off tech-led ES): applied, **helped** — cash SPY **−0.12%**.
- **08-14 / 09-15 / 09-14 backwardation / 09-11 pending-binary / 09-10 crowded / 09-04 aligned-neg**: correctly **off**.
- Open experiment keep-dir/shrink-conf: applied (llm conf **0.45**), **helped direction**, did not pull mild→flat.

## CHECK 5 — FALSIFIER
Same continuation setup (third-session ~1–2% oil fade, |PM:XLE|&lt;1% red, refiners/XOM cushion, no fresh EIA/OPEC, mag hit-rate &lt;0.4) but XLE still closes **mild** (≤ −0.3% and ≥ −1.0%) **without** oil extending beyond ~2% — then the flat preference is too tight and mild stays the right cap.

**Divergence:** flagged **false**. Factors down; leftover 09-17 +0.70% was not a fight. Cash sided with the barrel’s **sign**, not with index_carry. **none_flagged**.

**Verdict:** Category **C**. Keep down. Prefer **flat** when the ETF is a damped claim on a already-counted 1–2% fade.

LESSON_BEGIN
ERROR_CATEGORY: C
TRIGGER_PATTERN: Energy/XLE in a multi-session oil-fade continuation (same geo-premium-fade cluster already counted, no fresh EIA/OPEC), live crude offered only ~1–2% (not a collapse), PM:XLE red but |PM|<1%, large-caps confirm the barrel in premarket, nested refiners still bid / cracks extreme, and 1m relative is not crowded.
CURRENT_BEHAVIOR: Applied live-oil verify, 09-03 signed-down, 08-27 S1=−1, mag-discipline/size_gate, and nested refiners; still mapped S1=−1 with S0=S2=S3=S4=0 plus PM −0.56% onto a mild ETF down, copying a 1–2% barrel increment onto XLE.
CORRECTED_BEHAVIOR: Keep direction down when live oil is offered and PM majors confirm (09-03). Prefer the flat band, not mild, when all of: oil increment sub-2%, |PM:XLE|<1% red, same cluster already printed, refiners/XOM are a live cushion, mag hit-rate <0.4 / size_gate on, 1m rel not ≥+8%. Do not copy CL’s 1–2% onto the ETF print. Mild requires oil extending ≳2% with transmitting names, PM:XLE ≤ −1%, or live S4 extension. Continue shrinking confidence. Do not fire on unprinted FOMC + crowded leftover-shock days (09-16) or 1w/1m exhaustion sleeves (09-03).
EVIDENCE: 2026-09-18 predicted down/mild vs XLE −0.2637% / SPY −0.1193% / rel −0.1443% (down/flat). Oil −1 to −2% third session on limited-disruption hopes; PM −0.56% mean-reverted (64.20→64.31); CVX/COP ~−1%, XOM ~flat, MPC ~+0.6%. S1=−1 correct. Dir HIT, mag MISS by ~4 bp vs the 0.3% mild gate.
LESSON_MATCH_CHECK: closest 08-27 (cap mild/flat after green XLE / S4=0) — applied as mild, not taken to flat; 08-28 transmitting unless kept direction (helped, not this miss); 09-08/09-09 only notable→mild; 09-17 leftover-S4 does not match (PM not flat; leftover rel not ≤ −1.5%). Not a retrieval failure.
BACKWARD_CHECK: mixed unless tightly gated — helped 08-27 (−0.22% flat vs notable); would hurt 09-16 (−2.88% notable, crowded, unprinted FOMC) if generalized; would hurt 09-03 (−0.74% mild, extended 1w/1m); 09-17 different object; 09-04 S4 live so would not fire. Thin 4 bp miss — do not promote a blind flat rule.
CONFLICT_CHECK: none if scoped as band-only — keeps 09-03 direction; does not repeal 08-28 unless-transmitting; refines 08-27 mild/flat toward flat when |PM|<1% and refiners cushion; excludes 09-16 FOMC/crowd and 09-17 leftover-S4; complements the keep-direction/shrink-confidence experiment.
FALSIFIER: Same continuation setup (third-session ~1–2% oil fade, |PM:XLE|<1% red, refiners/XOM cushion, no fresh EIA/OPEC, mag hit-rate <0.4) but XLE still closes in the mild band (≤ −0.3% and ≥ −1.0%) without oil extending beyond ~2% — then the flat preference is too tight and mild remains the right cap.
DIVERGENCE_VERDICT: none_flagged
ACTIVE_LESSON_REVIEW: 08-11 helped; 09-03 helped direction; 09-17 leftover-S4 non-force helped; 09-08/09-09 and 08-27 helped as a mild cap but still one band high; 08-28 transmitting unless helped direction; refiner dampen and S0=0 helped; 08-14/09-15/09-14-backwardation/09-11-pending-binary/09-10-crowded/09-04-aligned-neg correctly off; open sector_energy keep-dir/shrink-conf helped direction, did not pull mild→flat.
SECTOR: Energy
LESSON_END
