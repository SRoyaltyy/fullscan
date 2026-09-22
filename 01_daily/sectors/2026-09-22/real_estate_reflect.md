# Sector Reflect — Real Estate — 2026-09-22

Memory search is paused (index metadata mismatch). Diagnostic uses the injected card, outcome, scoreboard, standing REIT lessons, and the published magnitude rubric.

**Triage:** Reasoning, not tool/data. Live curve, oil, and XLRE/SPY prints matched the morning map. Direction was right; the miss is the **band**. Rubric: Flat `<0.3%` | Mild `0.3–1.0%`. XLRE **−0.211%** is **flat**. Scoreboard `magnitude_hit: False` is correct. Outcome `ACTUAL_MAGNITUDE: mild` is a narrative mislabel — do **not** apply the 08-14 accounting lesson (its falsifier fires: −0.211% is outside mild).

S0=0 mixed, S1 rotation-out once, and 09-11 no-force-down all survived the close. Pipeline still emitted **down/mild** from a modest sum (`leading_sum −1.5`, overlay −1.012, S3+S4 confirmation). That is **C**: scores were the right map; magnitude/conviction was overextended.

**CHECK 1 — LESSON MATCH:** No existing lesson is this miss. 08-14 “predicted band == outcome band but scoreboard False” does **not** match — outcome mild is wrong vs the rubric. 09-21 is the **inverse** (index_carry flattening a modest negative on a strong risk-on parent). 08-25 / 09-11 were applied for *direction* (S0 unsigned, no smash) and were not used as a **flat-band** cap. Not a retrieval failure of a standing mag rule.

**CHECK 2 — BACKWARD TEST:** Cap unsigned-S0 + relative-lag-only + mixed parent at **flat**. **Helped 09-15** (down/mild vs −0.12% = flat). Would **not** fire on 09-14 (−0.69%, live rates/oil overlay) or 09-16 (FOMC binary). 09-18 was a failed-5% / T+2 problem, not this trigger. 09-21 stays intact: parent was SPY **+1.55%**, not mixed/flat. Tight gates keep this from becoming another flatten-everything rule.

**CHECK 3 — CONFLICT:** None if scoped. 09-21 = don’t let carry erase relative skew on a **strong risk-on** tape. This = don’t promote that skew to **mild absolute** when the parent is mixed/flat and the curve is unsigned. 08-18 down/mild still requires a **live** long-end shock. 08-25 “don’t force down” is the direction sibling of this mag cap.

**CHECK 4 — APPLIED-LESSON REVIEW:** 08-25/08-21/08-27 kept S0 mixed — **helped**. 08-17/08-18 smash OFF, 08-11 spike OFF, 09-18 joint down-gate OFF, 09-14 PM=0 — **helped**. 09-11 no-force-down **helped** (blocked smash) but was not used to cap the band at flat — **partial**. 09-21 relative-skew expression **helped direction**, **hurt mag** if read as a mild license. Experiment “prefer flat/mild; shrink on modest |score|” — they picked **mild**. 08-14 accounting — correctly **not** applied.

**CHECK 5 — FALSIFIER:** Same setup (S0 unsigned, rotation-out only, ES/NQ inside ±0.5%, no 5% re-break) with SPY also inside ±0.3% but XLRE still `|pct| ≥ 0.3%` on 3 of the next 5 recurrences → flat cap is too tight; revise.

**Divergence:** LLM flagged it; pipeline `divergence_flagged=False`. Leftover green index beta was not REIT duration relief. Close: SPY −0.02%, XLRE −0.21% — **leading_right**.

**Verdict:** Dir HIT, mag MISS. Knowable-at-open = partially (exact −0.21% vs a Nasdaq-follow 09-21 shape was not a lock). Discount A/B. Fix is band: unsigned spine + relative lag vs a mixed parent → **down/flat**, not down/mild.

LESSON_BEGIN
ERROR_CATEGORY: C
TRIGGER_PATTERN: A long-duration REIT/XLRE-like call on a mixed T+1 session has S0 unsigned (live 10Y/30Y neither smash nor verified relief; oil offered in the same duration channel), a modest negative leading sum from rotation-out/relative lag scored once, ES/NQ inside ±0.5% vs prior close (no ≥+0.5% green-futures branch, no live curve rip), and no-force-down applies — yet confirmation stacking (S3 outflows + S4 multi-horizon lag) plus overlay still emits down/mild while the parent index is also mixed/flat.
CURRENT_BEHAVIOR: Correctly sets S0=0 and blocks a duration smash, then promotes every-horizon relative lag and dry flows into official down/mild, treating relative non-participation as a mild-absolute certificate.
CORRECTED_BEHAVIOR: When S0 is mixed/unsigned and the only signed object is relative rotation-out/funding-source lag versus a mixed/flat parent, keep the relative map but cap the official magnitude at flat (down/flat or flat/flat). Do not let S3+S4 confirmation or llm_overlay promote a grind into mild. Require a live curve impulse (10Y re-break / failed 5% hold) or a true risk-off |ES|≥0.5% before mild. Do not treat a scoreboard mag False as an accounting error when |ETF pct| < 0.3%.
EVIDENCE: 2026-09-22 predicted down/mild (S0=0, S1=−0.5, S3=−0.5, S4=−0.5, total −1.482, overlay −1.012); XLRE −0.211% / SPY −0.016% / rel −0.196%; 10Y ~4.96% and 30Y ~5.30% unsigned; Nasdaq record / SPX flat. Rubric flat <0.3% so mag MISS. Same overshoot on 09-15 down/mild vs −0.12%.
LESSON_MATCH_CHECK: no match for this mag-cap; 08-14 accounting lesson falsifier applies (−0.211% is flat, not mild); 09-21 candidate is the inverse (flattening on a strong risk-on parent); 08-25/09-11 applied for direction only — not a retrieval failure
BACKWARD_CHECK: helped 09-15 (down/mild vs −0.12% flat); would not fire on 09-14 live rates/oil mild HIT or 09-16 FOMC; 09-18 is a failed-5% card, not this trigger; 09-21 parent SPY +1.55% stays out of scope
CONFLICT_CHECK: none if gated on unsigned S0 + mixed parent; 09-21 remains the strong-rally relative-skew rule; 08-18 down/mild still requires a live long-end shock
FALSIFIER: If this unsigned-S0 + relative-lag-only + ES/NQ inside ±0.5% setup recurs with SPY also inside ±0.3% and XLRE still closes |pct| ≥ 0.3% on 3 of the next 5 recurrences, the flat cap is too tight and must be revised
DIVERGENCE_VERDICT: leading_right
ACTIVE_LESSON_REVIEW: 08-25/08-21/08-27 S0 mixed helped; 08-17/08-18 smash OFF and 08-11 spike OFF helped; 09-11 no-force-down helped direction, partial on mag; 09-21 relative-skew helped direction, hurt mag if read as mild license; experiment prefer-flat/mild partially applied (chose mild); 08-14 accounting not_applicable
SECTOR: Real Estate
LESSON_END

⚠️ 🛠️ Exec failed: `list files in ~/fullscan-persist/01_daily/sectors/2026-09-15/ → print text → search "S0_SHARED_MACRO" in 2>/dev/null (in ~)`
