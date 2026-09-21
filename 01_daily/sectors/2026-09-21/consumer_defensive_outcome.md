# Sector Outcome — Consumer Defensive — 2026-09-21

Actuals: {'etf': 'XLP', 'pct': -1.086958324526055, 'spy_pct': 1.5518133737258744, 'rel': -2.6387716982519294, 'open': 82.09500122070312, 'close': 81.9000015258789, 'source': 'yf_download'}

# Sector Post-Session Review — Consumer Defensive (XLP) — 2026-09-21

## 0. FACTS

**Channel 1 actuals (deterministic, injected):**

| Metric | Value |
|---|---|
| XLP % | **−1.087%** |
| SPY % | **+1.552%** |
| Relative (XLP − SPY) | **−2.639%** |
| XLP open | 82.095 |
| XLP close | 81.900 |

**Path:** XLP opened at 82.095 (already below Friday's 82.80 close, consistent with the −0.65% pre-market print), then drifted lower through the session to close at 81.900 — a **monotone-ish grind down**, not a gap-and-recover. The open-to-close leg was only ~−0.24%, meaning **most of the damage was the pre-market gap plus a persistent inability to bounce while SPY ripped +1.55%**. This is the signature of a *funding-source / rotation-out* day, not a panic: XLP was sold steadily as the risk-on tape absorbed capital elsewhere.

**Direction:** down. **Magnitude:** notable (≈−1.09% absolute, −2.64% relative — well outside the flat band and beyond the morning's "mild" cap).

**Cross-check on the tape:** the morning board's own PM print (XLP −0.65% vs XLK +0.98%, XLE −1.29%) was the correct *sign*; the session simply extended it. The search thread confirms the regime: "Stocks rose in early trading as oil prices and Treasury yields fell and the AI trade regained its footing, with chipmakers gaining ground" (Schwab market update, 2026-09-21) — i.e., a **chip-led risk-on day with staples as the funding source**. XLP's RSI ~31 and price below its 50-day ($84.92) as of 09-21 (clearank) confirm the ETF was already in a downtrend and stayed there.

---

## 1. What drove the sector today

**Primary driver: risk-on rotation OUT of defensives, amplified by XLP's own relative-laggard status.**

The taxonomy-aligned decomposition:

1. **Risk-on / equity-beta expansion [−] defensives (dominant).** SPY +1.55% with NQ leading (the morning board had NQ +2.12% vs ES +1.35%, a 77 bp lead) is a textbook anti-FTS board. When the market rips on AI/chip leadership, staples are the *source* of funds, not the destination. This is the single largest object and it is exactly what the morning S0 named.

2. **Flight-to-safety RS vs cyclicals: MISS, confirmed.** XLP −1.09% vs a market +1.55% means the haven bid never showed. The morning's read that "PM XLP −0.65% vs XLK/XLY leaders → zero FTS credit" was correct and the session validated it.

3. **Input-cost relief (oil offered) — real but overwhelmed.** Crude was down hard (CL −5.94% pre-market; Schwab confirms "oil prices... fell"). This is a genuine gross-margin tailwind for staples, but as the morning explicitly capped it (~+0.2, single-session relative), it could not outrun a −2.6% relative rotation. **The morning's cap was correct.**

4. **Flows/positioning drag.** XLP had 5d −$325M / 1m −$251M net outflows (ETFDB, as of ~09-18). Persistent redemption pressure into a risk-on tape = no bid underneath. This is a *background* condition, not a same-day catalyst, but it explains why XLP couldn't even mean-revert intraday.

**What did NOT drive it:** no fresh staples earnings, no policy binary (FOMC paid 09-16), no food-crash print, no vol event (VIX contango). The move was **pure relative rotation**, which is precisely the object the morning identified.

---

## 2. Audit of morning S0–S4 reads against reality

The morning's *narrative* was excellent. The morning's *scored output* was wrong on direction. This is the central finding.

### S0_SHARED_MACRO: scored −1 → **directionally CORRECT, magnitude UNDERWEIGHTED**

The morning wrote: *"S0 carries the risk-on rotation overlay only → −1. Not −2 (no fresh mega-cap shock...). Not 0 (naming a relative headwind without scoring it is banned)."*

- **Correct:** it scored the rotation as a negative rather than narrating it. The 09-11 lesson ("no FTS bid is a relative negative, not S0=0") was applied.
- **Underweighted:** a −1 on a 0.8 multiplier contributes only −0.8 to a total that landed at −1.24. The realized relative move was −2.64%. The morning *itself* noted "NQ lead 77 bp with NQ +2.12% is a stronger anti-FTS board than 09-17/09-18 (those were ~36–39 bp)" — it recognized the board was *stronger* than prior days, yet kept S0 at the same −1. **That is the miss: the morning identified an escalated input and did not escalate the score.**

### S1_SECTOR_FACTORS: scored −0.5 → **CORRECT sign, correctly capped**

The morning capped oil relief at ~+0.2 and netted the sector-factor lean to −0.5 (residual rotation lean, not double-counted). Given XLP fell −1.09% absolute, a −0.5 residual is reasonable. **No complaint.** The same-shock audit (oil counted once) held up.

### S2_BREADTH: scored 0 → **CORRECT (and correctly not averaged)**

The morning insisted the MAP HEAT split book (WMT/COST/KR/ADM up, PEP down, PG/CL flat) "beats the parent for those sleeves and must not be averaged into XLP." Reality: the parent fell −1.09% while sleeves were mixed. **Scoring S2 at 0 was right** — breadth was neither expansion nor failure; the ETF move was macro-driven, not breadth-driven. The morning's refusal to let nested sleeve strength become an "XLP-up certificate" was vindicated.

### S3_FLOWS_POSITIONING: scored 0 → **CORRECT**

The morning said outflows are "not a forced-selling event; do not pile S3 to −2." XLP fell but not in a disorderly way (open-to-close only −0.24%). **A 0 was appropriate** — flows were a background drag, not the day's driver.

### S4_ETF_TAPE: scored 0 → **CORRECT (confirm-only)**

The morning explicitly refused to copy Friday's −0.96% rel into S4 as a forecast (08-28 lesson). Reality: XLP fell again, so a *copy* would have "worked" — but the morning's discipline was still right, because the *reason* it fell was the live macro rotation, not momentum persistence. **Scoring 0 and letting S0 carry it was methodologically correct.**

### The engine vs the analyst

The morning's **prose conclusion was "down/mild"** — and it explicitly warned: *"If v2 tape_anchor tries up off ES +1.35%, that is the 09-17/09-18 error class — reject; trust factors."*

The **pipeline output** printed `predicted_direction: flat, predicted_magnitude_band: flat` with `total_score: -1.24`.

So the analyst was right and the engine was wrong — **again, in the same direction as 09-17 and 09-18.** The `index_carry: 3.218` (general 12.871) leg pulled the total up from the leading sum of −3.5 to −1.24, and the `sector_rs_veto_applied: True` with `sector_rs_tape: {d1: 1.44, w1: 0.52}` appears to have further dampened the negative. The result: a −3.5 leading sum got flattened to "flat."

**This is the third consecutive session where the engine's index-carry/tape-anchor machinery fought a correct negative sector read.** The morning even flagged it: "09-17/09-18 bind the engine path: do not accept v2 up/mild from ES tape_anchor + index_carry."

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count audit (morning's own):**
- Oil counted **once** (S1 relief, not S0 haven, not a second S0 negative) — **held**.
- Risk-on rotation counted **once** in S0, residual only in S1 — **held**.
- Friday's smash not copied into S2/S4 — **held**.
- Nested sleeves (WMT/COST/KR/ADM/PEP) not driving the ETF call — **held**.

The morning's same-shock discipline was clean. **No double-count error.**

**Knowable-at-open test: YES.**

Everything needed to call this was on the board before the bell:
- ES +1.35% / NQ +2.12% (77 bp lead) — a strong anti-FTS board.
- PM XLP −0.65% vs XLK +0.98%, XLE −1.29% — XLP bottom-of-book among non-energy.
- VIX contango, oil offered, Asia/Europe green.
- XLP 1m rel −3.61% (deep laggard), 5d/1m outflows.

The morning *had* all of this and *said* "down/mild." The information was sufficient. **The failure was in the score-to-output translation, not in the information set.**

**The one genuinely hard part:** the *magnitude*. Calling −1.09% absolute / −2.64% relative from a −0.65% PM print requires extrapolating that a risk-on day *accelerates* the rotation rather than letting XLP mean-revert. The morning capped at "mild" partly on the oil-relief dampener and the nested WMT/COST bid. In hindsight, **on a +1.55% SPY day, the rotation-out force dominates any single-session input-cost relief** — the dampener logic was too generous. That's a defensible miss, but it's the second-order lesson.

---

## 4. Outliers inside the sector

- **XLP itself is the outlier:** −1.09% on a day SPY +1.55% is a −2.64% relative print — a ~2.6 sigma-ish relative move for a low-beta staples ETF. This is the kind of dispersion that only happens on strong risk-on rotation days.
- **Energy (XLE −1.29% PM) was the only sector worse than XLP** — and that's oil-driven, a different mechanism. So XLP was effectively **worst-of-book ex-energy**, exactly as the morning's PM board showed.
- **Nested sleeves diverged from the parent:** WMT/COST/KR/ADM were bid while the ETF fell. This is the "split book" the morning described — and it's a reminder that XLP's cap-weighted structure (PG, KO, PEP, COST, WMT heavy) can be dragged by the mega-cap staples complex even when discount/grocery sleeves hold. **PEP −3% on 09-18** (nested, already in Friday's close) was a lingering drag.
- **No single-name blowup** drove the ETF — this was a broad, macro-driven de-rating of the defensive factor, not an idiosyncratic shock.

---

## 5. Verdict and lessons

**The morning analyst was right; the engine was wrong.** The prose called down/mild, correctly identified the risk-on rotation as the dominant object, correctly capped oil relief, correctly refused to let nested sleeves drive the call, and explicitly warned against the exact engine error that then occurred. The pipeline flattened a −3.5 leading sum to "flat" via index_carry (+3.218) and the sector-RS veto.

**Three consecutive sessions (09-17, 09-18, 09-21) now show the same failure mode:** when the sector read is negative but the broad tape is risk-on, `index_carry` / `tape_anchor` pulls the sector call toward flat/up. On 09-17 it produced a dir HIT by luck (19 bp); on 09-18 and 09-21 it produced dir MISSes. **The engine's index-carry leg is systematically fighting correct negative defensive reads in risk-on regimes.**

**Actionable:** the `sector_rs_veto` and `index_carry` weights need review for defensive sectors in risk-on regimes — the veto appears to be *dampening* correct negative signals rather than protecting against false ones. The morning's own "reject the engine, trust factors" instruction should be promoted from prose to a scored override when PM sign and factor sign agree against the engine.

**Magnitude lesson:** on a ≥+1.5% SPY day, do not let single-session input-cost relief (oil) or nested sleeve strength cap a defensive sector at "mild." Rotation-out force dominates.

---

OUTCOME_BEGIN
SECTOR: Consumer Defensive
ETF: XLP
ETF_PCT: -1.087
SPY_PCT: 1.552
REL_PCT: -2.639
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Risk-on rotation out of defensives on a chip-led +1.55% SPY day; XLP as funding source, with oil-relief and nested sleeve strength unable to offset.
KEY_INTERACTION: Oil input-cost relief (S1, capped) and nested WMT/COST/KR bid were correctly not allowed to offset the S0 rotation object — but the engine's index_carry (+3.218) flattened a -3.5 leading sum to "flat."
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Analyst prose correct (down/mild, rotation named, oil capped, sleeves excluded); engine output wrong (flat/flat) — third straight session where index_carry/tape_anchor fought a correct negative defensive read in a risk-on regime.
OUTCOME_END