---
trigger_pattern: "A two-name-concentrated sector ETF (top-2 ≳35–40%) whose largest holding just completed an after-cash company product event the prior session; the event is now printed; overnight/PM sleeve is red; a live rates/duration tax is binding the parent index — the card signs the printed product as fade/unwind because PM is red and leftover-spine rules forbid minting up."
current_behavior: "Scored S1 −1 on printed-and-unpaid Connect plus Muse partner blocks, S3 −1 as 09-23 crowded-long unwind on T+1, S4 −1 from XLC PM −0.71% as confirmation, mapped Warsh/yields onto XLC as the sector driver, and emitted down (engine notable / overlay mild)."
corrected_behavior: "Once an after-cash binary in a ≥15–20% name has printed, 09-23 crowded-long-unwind is exhausted — do not fire it on T+1. Red PM is one overnight snapshot, not fade confirmation. Score a just-printed product event as a continuation/spine candidate (S1 0 or +1) unless cash or a same-morning revenue/attach miss rejects it; partner-block headlines are distribution noise unless they are the load-bearing Street object. Do not map a live index duration tax onto XLC as the sector driver when the two-name book has a same-cycle product object. Prefer flat over down when the only down-creators are rates-on-SPY plus red PM. 09-22 still forbids minting UP from a stale thesis with no print; it does not require signing a just-printed event as DOWN."
evidence_cited: "2026-09-24 XLC predicted down/notable vs actual +1.27% (SPY −0.08%, rel +1.35%); META +4.3–4.6% on Connect/Muse follow-through and same-session PT raises (JPM $920); GOOGL also bid; 10Y stayed ~5.12% and bound SPY not XLC. Dir MISS, mag HIT. S1 sign inverted; 09-23 misfired on T+1."
error_category: "B"
falsifier: "If T+1 after a just-printed ≥15–20% name product event, with red sector PM and red ES/NQ plus morning Street reaction pieces, XLC still closes down or lags SPY on 2 of the next 3 such days, restore the unwind prior and discard the continuation-spine rule."
sector: "Communication Services"
date: "2026-09-24"
status: "candidate"
---

# Sector Reflection — Communication Services — 2026-09-24

**Triage:** Reasoning failure, not tool/data. Connect, Muse blocks, Warsh/yields, red ES/NQ, and XLC PM −0.71% were all in the morning card. Cash paid META (~+4.3–4.6%) and GOOGL (~+1.2–1.4%); XLC **+1.27%** vs SPY **−0.08%** (rel **+1.35%**). Direction miss, magnitude size roughly right. Category **B**: ingredients retrieved, weights inverted.

The miss is not the 09-15/16/17/18/22/23 overlay-flattening family. Engine and overlay both signed **down**. Failure: treating a **just-printed** top-holding product event as a fade because the overnight sleeve was red, then using that red PM as “confirmation.”

---

**CHECK 1 — LESSON MATCH:** Closest match is the injected **09-23 crowded-long-into-after-cash-binary** candidate. It was **applied** (`FIRES`) and **hurt**. That rule is the session of an *unprinted* after-cash binary, not T+1 after the print. **09-22 leftover-spine-as-sole-up-creator** was also stretched: it forbids minting *up* from a stale thesis; it does not require signing a just-printed product as *down*. **09-09** (META-only ≠ full-book negative) was cited and then violated via S1/S3. No retrieval miss of a save-the-day active rule. **mega-cap-earnings-over-macro-drag** does not fire as written (ES/NQ were red; this was product, not earnings). New lesson is a **scope restriction** on 09-23 plus a T+1 continuation prior, not a duplicate.

**CHECK 2 — BACKWARD TEST:** Last stretch of XLC misses were *up* calls that went *down* (09-15/16/17/18/22/23). Correction must be T+1-after-print only. On **09-23** the binary was still unprinted at predict time — applying today’s rule then would have kept the failed up call. **09-22** had no just-printed product object. **09-21** (+3.56%, up HIT) is the inverse (don’t damp an *unprinted* dual-leader catalyst) and is preserved. **Helped today; would not have flipped those prior up-misses if scoped to T+1 after print.**

**CHECK 3 — CONFLICT:** Conflicts with **09-23** if 09-23 may fire on T+1. Resolve: 09-23 dies when the binary prints; T+1 is Street reaction, a new object. Distinguisher vs **09-22**: leftover (no print) ≠ just-printed (T+1) ≠ unprinted (T+0). Does **not** relax 09-22’s ban on minting up from a stale ad/AI thesis. Does **not** override **mega-cap-earnings-over-macro-drag** for SPX: this is XLC two-name mapping only. **08-21 leftover hawkish** stays: Warsh was fresh and futures were red; S0 can still tax the *index* without being the XLC driver.

**CHECK 4 — APPLIED-LESSON REVIEW:** **09-23 crowded-long unwind — applied, hurt** (T+1 scope error; crowding *extended* to 52-week highs). **09-22 leftover-spine — applied, mixed/hurt on sign** (blocked a leftover up, then signed the print as fade). **09-21 no-damp — correctly did not fire. 08-21 leftover hawkish — correctly did not protect. 09-04/S0 Warsh — right object, wrong sector mapping** (bound SPY, not XLC). **09-16/18 no NQ-ES overlay, 09-15 PM-outranks-rel, 09-10 two-name book — applied as hygiene, not the miss.** Active geo/oil two-stock cap: not applicable.

**CHECK 5 — FALSIFIER:** If T+1 after a just-printed ≥15–20% name product event, with red sector PM and red ES/NQ, and morning Street reaction pieces, XLC still closes down or lags SPY on **2 of the next 3** such days, the continuation-spine prior is wrong and 09-23’s unwind read should be restored for T+1. Also revise if knowable partner-blocks are the load-bearing Street object and the top holding fails to lead.

**Divergence:** `none_flagged`. Leading −4 and S4 −1 agreed; both were wrong vs cash. Overnight sleeve was not the session. KNOWABLE_AT_OPEN = **partially** (PT-raise day / dual-anchor cash bid not fully foreseeable) — mild discount only; the fade-because-PM-red weighting was still a reasoning error.

**Verdict:** Category B. Promote a T+1-after-print continuation rule and **narrow 09-23 so it cannot fire once the binary has printed**.

```text
LESSON_BEGIN
ERROR_CATEGORY: B
TRIGGER_PATTERN: A two-name-concentrated sector ETF (top-2 ≳35–40%) whose largest holding just completed an after-cash company product event the prior session; the event is now printed; overnight/PM sleeve is red; a live rates/duration tax is binding the parent index — the card signs the printed product as fade/unwind because PM is red and leftover-spine rules forbid minting up.
CURRENT_BEHAVIOR: Scored S1 −1 on printed-and-unpaid Connect plus Muse partner blocks, S3 −1 as 09-23 crowded-long unwind on T+1, S4 −1 from XLC PM −0.71% as confirmation, mapped Warsh/yields onto XLC as the sector driver, and emitted down (engine notable / overlay mild).
CORRECTED_BEHAVIOR: Once an after-cash binary in a ≥15–20% name has printed, 09-23 crowded-long-unwind is exhausted — do not fire it on T+1. Red PM is one overnight snapshot, not fade confirmation. Score a just-printed product event as a continuation/spine candidate (S1 0 or +1) unless cash or a same-morning revenue/attach miss rejects it; partner-block headlines are distribution noise unless they are the load-bearing Street object. Do not map a live index duration tax onto XLC as the sector driver when the two-name book has a same-cycle product object. Prefer flat over down when the only down-creators are rates-on-SPY plus red PM. 09-22 still forbids minting UP from a stale thesis with no print; it does not require signing a just-printed event as DOWN.
EVIDENCE: 2026-09-24 XLC predicted down/notable vs actual +1.27% (SPY −0.08%, rel +1.35%); META +4.3–4.6% on Connect/Muse follow-through and same-session PT raises (JPM $920); GOOGL also bid; 10Y stayed ~5.12% and bound SPY not XLC. Dir MISS, mag HIT. S1 sign inverted; 09-23 misfired on T+1.
LESSON_MATCH_CHECK: matches 09-23 crowded-long-into-after-cash-binary — applied and hurt (wrong scope: unprinted T+0 vs printed T+1); 09-22 leftover-spine applied and over-extended into a down sign; 09-09 META-only≠full-book-negative cited then violated. Not a retrieval failure of an unapplied save. New lesson is a 09-23 scope restriction plus T+1 continuation prior.
BACKWARD_CHECK: helped today; would not have hurt 09-23 if scoped to T+1-after-print (09-23 binary still unprinted at that open); would not flip 09-15/16/17/18/22 overlay-created up misses; preserves 09-21 unprinted-catalyst HIT.
CONFLICT_CHECK: conflicts with 09-23 if it may fire on T+1 — resolution: 09-23 expires when the binary prints; T+1 is Street reaction. Distinguisher vs 09-22: leftover ≠ just-printed ≠ unprinted. Does not relax 09-22’s leftover-up ban. Does not override mega-cap-earnings-over-macro-drag for SPX (futures-red exception remains for the index). 08-21 leftover-hawkish unchanged (Warsh was fresh).
FALSIFIER: If T+1 after a just-printed ≥15–20% name product event, with red sector PM and red ES/NQ plus morning Street reaction pieces, XLC still closes down or lags SPY on 2 of the next 3 such days, restore the unwind prior and discard the continuation-spine rule.
DIVERGENCE_VERDICT: none_flagged
ACTIVE_LESSON_REVIEW: 09-23 crowded-long unwind applied and hurt (T+1); 09-22 leftover-spine applied mixed/hurt on sign; 09-21 no-damp correctly did not fire; 08-21 leftover hawkish correctly did not protect; 09-04 Warsh right object wrong XLC mapping; two-stock geo-oil cap not_applicable; mega-cap-earnings-over-macro-drag not_applicable as written (futures red, not earnings).
SECTOR: Communication Services
LESSON_END
```
