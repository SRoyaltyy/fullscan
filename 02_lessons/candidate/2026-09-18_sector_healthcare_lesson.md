---
trigger_pattern: "A low-beta defensive healthcare ETF (XLV-like) posts a net-non-positive S0–S4 card (funding-source S0 ≤ 0, no HC spine, leftover 3d/1w/1m RS banned) with live PM ≤ 0 (not already in the mild-green band) on the session AFTER an already-printed FOMC+SEP+presser, into a tech-led risk-on rebound (NQ≥ES, oil offered), while v2 tape_anchor (overnight ES-vs-cash rip + flat/red PM) + index_carry still write official up/mild."
current_behavior: "LLM scored S0=-0.5 / S1–S4=0, fired 08-13 as a ban on up/notable, prefer flat/mild, named 09-17’s mild-PM distinguisher as absent, overlay 0.0. Engine still emitted official up/mild (total 2.404) from tape_anchor 1.189 (ES +1.14%, PM:XLV -0.01%) + index_carry 1.215, with size_gate only blocking notable."
corrected_behavior: "When FOMC is paid but PM:XLV is ≤ 0 / not already mild-green, do not accept v2 up/mild from ES tape_anchor + index_carry. Official call is flat/flat. Size-gate is not enough. Keep 09-17’s keep-up license only when PM is already in the mild band (~+0.4%). Do not convert to down/mild from an unprinted same-session 10Y tick or a cash gap that was not in PM. Keep 09-11 S0 as the relative funding-source read; do not promote XENE/XBI sleeve or a same-day mega-cap Rx headline that does not move the name into S1."
evidence_cited: "2026-09-18 predicted up/mild vs XLV −0.249% / SPY −0.119% / rel −0.129% (dir MISS, mag MISS; |25 bp| is the flat band). LLM leading_sum −0.5, overlay 0.0, pipeline divergence_flagged false. Path: gap-down ~0.32% then a bounce that never recovered the open; XLK ~+0.8%, XBI −0.97%, LLY FDA combo stock flat. Finviz ES +0.20% tracked cash better than yfinance ES +1.14%."
error_category: "B"
falsifier: "If this trigger recurs (paid FOMC, tech-led risk-on, oil offered, XLV S0≤0 / leftover RS banned, PM ≤ 0 / not already-mild, official forced flat instead of tape_anchor up) and XLV still closes ≥ +0.3% absolute, revise the flat-cap rather than defend it. Forcing down/mild is separately wrong if the same setup then closes flat or up."
sector: "Healthcare"
date: "2026-09-18"
status: "candidate"
---

# Sector Reflection — Healthcare — 2026-09-18

Memory search is paused (embedding index metadata missing). Diagnostic uses the injected 2026-09-18 Healthcare packet, on-disk Healthcare active/candidate lessons, and same-day XLP/XLC siblings.

# Healthcare / XLV — 2026-09-18 reflect

**TRIAGE:** Tool/data in the v2 combine, not a Channel 2 spine miss. Channel 1 tape, paid FOMC, oil-offered, NQ≥ES, leftover 1w RS, and PM:XLV **−0.01% vs XLK +0.60%** were all in the morning book. Official grade: predicted **up/mild** vs XLV **−0.249%** / SPY **−0.119%** / rel **−0.129%**. Both axes miss. Rubric band for |0.25%| is **flat** (<0.3%), not mild — the autopsy’s “down/mild” label is looser than the grader.

LLM card was the right *object*: S0 **−0.5** (09-11 funding-source), S1–S4 **0**, 08-13 ban on **up/notable**, conf **0.42**, prefer **flat/mild**, and an explicit **no keep-up license** because 09-17’s distinguisher (PM already mild-green) was **absent**. Official **up/mild** (total **2.404**) is v2 **tape_anchor 1.189** (ES **+1.14%** / PM:XLV **−0.01%**) **+ index_carry 1.215**. Overlay **0.0** did not cancel it. Pipeline `divergence_flagged: False` even though the writeup already named the ES-vs-factor fight.

**Category B.** The ES +1.14% object was known, tagged as the same 09-15/16/17 conflict class, and still allowed to dominate a net-non-positive card and a PM already ≤ 0. Finviz ES **+0.20%** was the live panel; cash SPY **−0.12%** proved it. Not A (no missing MA/IRA/FDA-basket; LLY combo printed same-day and **did not move LLY**; XENE correctly non-dominating). Not C (mult 0.8 / size-gate already blocked notable). Not D as primary: inputs were not missing or stale — they were **misweighted**. Knowable-at-open discount applies only to the cash gap and the +5 bp 10Y tick, **not** to the engine-up.

---

**CHECK 1 — LESSON MATCH.** Closest cousin is **09-17 HC** (same T+1 paid-FOMC, net-non-positive card, NQ≥ES, oil offered, engine writes **up/mild**). Trigger **fails** on the PM clause: 09-17 requires PM already in the **mild** band (~**+0.4%**) and then lets tape_anchor own absolute. Today PM was **−0.01%**. Morning applied that distinguisher in prose; the engine ignored it. Not a retrieval miss of 09-17 — it is 09-17’s keep-up branch **closing**.

**09-16 HC** (active, engine-up / force-flat) requires an **unprinted** FOMC+SEP+presser. Correctly **off**. Same-day **09-18 XLP** is the mechanism analog: net-negative card + **PM ≤ 0** + post-event ES rip → do not accept official up/mild; cap **flat/flat**. **09-18 XLC** is the same overlay path on a net-zero card. **08-13** matched and was applied as a ban on **up/notable**, not on up/mild — enforcement gap on the official emit, not “never learned 08-13.” **09-11** matched and was applied (S0 **−0.5**). Not a mint-from-scratch: increment is **paid-FOMC + PM ≤ 0 closes 09-17’s keep-up license**.

**CHECK 2 — BACKWARD TEST.** Cap at **flat/flat** when the card is net-non-positive and PM ≤ 0; do not emit up. **Helps today** (removes wrong-side up). Does **not** fire **09-17** (PM **+0.37%** still uses the mild-PM keep-up; actual **+0.62%** HIT). Does **not** replace **09-16** (unprinted FOMC; PM was **+0.20%**). Would **not** fire **09-15** (risk-off, emitted down), **09-14** (NQ-led red destination), **09-09/09-10** down/mild HITs (factor-down, not overlay-up). **09-11** flat vs **−0.18%** is a different pending-CPI sign error. Mandating **down/mild** from this card would **hurt 09-17** and overfit a **−25 bp** print that is still inside the **flat** band — do not convert.

**CHECK 3 — CONFLICT SCAN.** None if scoped. **09-16** stays unprinted-FOMC only. **09-17 HC** stays: paid FOMC **and** PM already mild-green → tape_anchor may own absolute / S0 owns relative. This lesson is the complement: paid FOMC **and** PM ≤ 0 / not already-mild → official **flat**, not ES-carry up. **08-13** still bans leftover-RS **up/notable**. **09-11** S0 stays **0 to −0.5** without a standing absolute-down mandate. **09-14 / 09-15** complements unchanged. Aligns with **09-18 XLP** (PM ≤ 0 closes up/flat) and **08-28** (don’t restack leftover 1w RS). No new XLV-down mandate on post-FOMC green-ES days.

**CHECK 4 — APPLIED-LESSON REVIEW.**
- **08-13 reversal-tell:** applied as ban on **up/notable**. **Helped** vs notable; **failed to bind** official **up/mild**. Falsifier (rel **> +0.3%** on a tech-led SPY **up** day) **not** hit — cash SPY was **−0.12%**, XLV rel **−0.13%**, XLK still led.
- **09-11 funding-source:** applied (S0 **−0.5**). **Helped.** Oil-offered + NQ≥ES was not scored as a duration tailwind or HC bid. vs XLK the split held. Absolute was a small red, not a smash — 09-11 does not require one.
- **09-16 engine-up / force-flat:** correctly **off** (FOMC printed). Neutral as a trigger; the **paid** sibling still needs a PM≤0 clause.
- **09-17 HC keep-up:** correctly **not** applied in prose (PM not mild). Engine still printed **up/mild**. **This is the miss.** Narrow 09-17 rather than defend a general keep-up on any paid-FOMC green-ES day.
- **09-14 destination / 09-15 S0~0 / 09-10 decay:** correctly off. **Helped.**
- **08-28 leftover-stack / 09-10 same-shock:** applied. **Helped** (no 3d/1w/1m copy into S2/S4; oil+risk-on once in S0). HORIZON_3D/1W fade sketched and printed.
- **08-14 / 08-11 / 08-17 / 08-21:** correctly off or single-name capped. **Helped.** LLY Inluriyo+Verzenio was a same-session mega-cap Rx headline with **stock flat**; XENE cratered XBI (**−0.97%**), not XLV.
- **Open experiment** (factor sign fights tape → cut conviction, prefer flat/mild): **applied as a conviction cut, not a direction override.** Factors and live PM **agreed** near zero/slightly negative — no keep-up license. Engine overrode anyway.
- **Calendar size-gate / mult 0.8:** **helped** vs notable; **insufficient** vs up/mild.

**CHECK 5 — FALSIFIER.** If this setup recurs (paid FOMC, tech-led risk-on, oil offered, XLV S0≤0 / leftover RS banned, live PM:XLV ≤ 0 / not already-mild, official direction forced **flat** instead of tape_anchor **up**) and XLV still closes **≥ +0.3%** absolute, the flat-cap is wrong and must be revised, not defended. Separate: forcing **down/mild** from this card is wrong if the same setup then closes flat or up (09-17 already showed leftover beta can print mild-green when PM is already mild; today’s |25 bp| is still the flat band).

**Divergence:** LLM flagged the ES-vs-factor fight; pipeline `divergence_flagged: False`. Absolute followed the **flat/lag factor card** (PM **−0.01%** → close **−0.25%**), not ES **+1.14%**. Cash SPY also failed that ES object. **leading_right.** KNOWABLE_AT_OPEN: **partially** — discount only the gap and the unprinted +5 bp 10Y, not the engine-up.

**Verdict:** Category **B**. Fair morning call was **flat/flat** (08-13 still forbids leftover-RS up/notable; 09-11 still owns the relative lag). 09-17’s “tape_anchor owns absolute” split is **PM-conditional** and was already called out as off; the engine printed it anyway.

LESSON_BEGIN
ERROR_CATEGORY: B
TRIGGER_PATTERN: A low-beta defensive healthcare ETF (XLV-like) posts a net-non-positive S0–S4 card (funding-source S0 ≤ 0, no HC spine, leftover 3d/1w/1m RS banned) with live PM ≤ 0 (not already in the mild-green band) on the session AFTER an already-printed FOMC+SEP+presser, into a tech-led risk-on rebound (NQ≥ES, oil offered), while v2 tape_anchor (overnight ES-vs-cash rip + flat/red PM) + index_carry still write official up/mild.
CURRENT_BEHAVIOR: LLM scored S0=-0.5 / S1–S4=0, fired 08-13 as a ban on up/notable, prefer flat/mild, named 09-17’s mild-PM distinguisher as absent, overlay 0.0. Engine still emitted official up/mild (total 2.404) from tape_anchor 1.189 (ES +1.14%, PM:XLV -0.01%) + index_carry 1.215, with size_gate only blocking notable.
CORRECTED_BEHAVIOR: When FOMC is paid but PM:XLV is ≤ 0 / not already mild-green, do not accept v2 up/mild from ES tape_anchor + index_carry. Official call is flat/flat. Size-gate is not enough. Keep 09-17’s keep-up license only when PM is already in the mild band (~+0.4%). Do not convert to down/mild from an unprinted same-session 10Y tick or a cash gap that was not in PM. Keep 09-11 S0 as the relative funding-source read; do not promote XENE/XBI sleeve or a same-day mega-cap Rx headline that does not move the name into S1.
EVIDENCE: 2026-09-18 predicted up/mild vs XLV −0.249% / SPY −0.119% / rel −0.129% (dir MISS, mag MISS; |25 bp| is the flat band). LLM leading_sum −0.5, overlay 0.0, pipeline divergence_flagged false. Path: gap-down ~0.32% then a bounce that never recovered the open; XLK ~+0.8%, XBI −0.97%, LLY FDA combo stock flat. Finviz ES +0.20% tracked cash better than yfinance ES +1.14%.
LESSON_MATCH_CHECK: 09-17 HC is the closest cousin but does not match — it requires PM already ~+0.4% mild and then lets tape_anchor own absolute; today PM was −0.01% and morning correctly withheld that license. 09-16 HC requires unprinted FOMC (correctly off). 08-13 and 09-11 matched and were applied (notable banned; S0 funding-source). Same-day 09-18 XLP/XLC are the mechanism analog (PM ≤ 0 closes overlay-up). Increment, not a mint-from-scratch: paid-FOMC + PM ≤ 0 closes 09-17’s keep-up branch. Not a retrieval failure.
BACKWARD_CHECK: helps today (removes wrong-side up); does not fire 09-17 (PM +0.37% still keep-up / HIT); does not replace 09-16 (unprinted FOMC); off on 09-15/09-14/09-09/09-10. Mandating down/mild would hurt 09-17 and overfit a flat-band −25 bp print.
CONFLICT_CHECK: none — 09-16 remains unprinted-FOMC only; 09-17 HC remains PM-already-mild keep-up; this lesson is the PM ≤ 0 complement; 08-13 still bans leftover-RS up/notable; 09-11 S0 stays 0 to −0.5 without a standing down mandate; 09-14/09-15 unchanged.
FALSIFIER: If this trigger recurs (paid FOMC, tech-led risk-on, oil offered, XLV S0≤0 / leftover RS banned, PM ≤ 0 / not already-mild, official forced flat instead of tape_anchor up) and XLV still closes ≥ +0.3% absolute, revise the flat-cap rather than defend it. Forcing down/mild is separately wrong if the same setup then closes flat or up.
DIVERGENCE_VERDICT: leading_right
ACTIVE_LESSON_REVIEW: 08-13 applied, helped vs notable, failed to bind official up/mild. 09-11 S0 applied, helped (relative lag vs XLK; not an absolute-down mandate). 09-16 force-flat correctly off. 09-17 keep-up correctly withheld in prose, not bound by engine — this is the miss. 09-14/09-15/09-10 correctly off. 08-28/09-10 same-shock and leftover-stack helped. 08-14/08-11/08-17/08-21 correctly off or single-name capped (LLY flat; XENE→XBI not XLV). Open experiment cut conviction but did not override engine up/mild. Size-gate/mult 0.8 helped vs notable, not vs up/mild.
SECTOR: Healthcare
LESSON_END
