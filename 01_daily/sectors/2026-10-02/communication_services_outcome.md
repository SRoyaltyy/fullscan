# Sector Outcome — Communication Services — 2026-10-02

Actuals: {'etf': 'XLC', 'pct': 0.3092562582685421, 'spy_pct': 0.742154083513169, 'rel': -0.43289782524462694, 'open': 110.30999755859375, 'close': 110.27999877929688, 'source': 'yf_download'}

# Sector Post-Session Review — Communication Services (XLC) — 2026-10-02

## 0. FACTS

**Channel 1 (deterministic actuals):**
- XLC: **+0.309%** (open 110.31 → close 110.28; note the ETF closed *below* its open — the gain is entirely an overnight gap from the prior close, not intraday accumulation)
- SPY: **+0.742%**
- Relative: **−0.433%** (XLC underperformed SPY)
- Path: XLC gapped up ~+0.13% premarket, opened at 110.31, then **faded to 110.28 by the close** — a flat-to-down intraday drift against a rising tape.

**Context:** SPY +0.74% on a soft-jobs rally (Nasdaq intraday high, Nvidia-led). XLC captured less than half of SPY's move and gave back its open. This is a **mild up in absolute terms, but a relative loss** — the fourth consecutive session of XLC lagging SPY on a 1d basis.

**Morning prediction:** up / mild, total_score 4.631, engine v2, tape_anchor 3.54 (NQ +0.68% / ES +0.50%), index_carry 1.091, llm_overlay 0.0. The **factor card was all-zero (S0–S4 = 0)** with multiplier 0.75 and confidence 0.46; the **engine overrode the card** via the tape anchor to mint an up/mild call.

---

## 1. WHAT DROVE THE SECTOR

**Primary driver: the soft NFP print → dovish rates path → broad risk-on, with XLC as a lagging participant, not a leader.**

The September jobs report came in cooler than expected, cutting Fed hike odds and sparking a broad advance. This is the **10-01-style two-sided rates object resolving to the dovish side** — exactly the scenario the morning card flagged as a trap ("a soft print is the 10-01-style two-sided rates trap: path dovish, level still high").

Taxonomy mapping:
- **Risk-on tape / equity beta expansion — HIT (realized).** SPY +0.74%, Nasdaq intraday high. The morning grid scored this MIXED at 0.55; it resolved as a genuine risk-on session.
- **Sector rotation out of communication services — HIT (confirmed).** The morning grid scored this HIT at 0.65 on the 1w rel −3.14% history. Today's −0.43% rel **extends** that rotation. This was the single most predictive cell on the card.
- **Real yields falling — the missing leg.** The morning card correctly noted DFII10 2.93 was "elevated and still rising on every FRED horizon" and that the all-zero-up residual rule "needs yields falling." The soft NFP is precisely the mechanism that *should* have started that easing — but XLC's failure to convert it into relative strength says the **duration tax level (DGS10 5.29, DFII10 2.93) remained binding even as the path turned dovish.** The level, not the path, governed XLC.

**Secondary:** META/GOOGL were modestly green premarket (+0.46–0.70%) but the ETF print was a noise tick. The two-name book participated in the beta rally but did not lead it — consistent with the morning read that "dual anchors are modestly green together; the ETF print is a noise tick, not 09-21 participation."

---

## 2. AUDIT OF MORNING S0–S4 READS

**S0 (Shared Macro) = 0 — VERDICT: CORRECT, and the reasoning was vindicated.**
The card refused to score the two-sided rates object as +1 (per 10-01) and refused to score it −1 (per 08-21, which needs real yields *easing*, not shown). Net zero. Reality: the dovish path did lift the broad tape, but XLC's relative lag confirms the **level** was the binding constraint. A signed S0 in either direction would have been wrong for the *sector* even though the *index* rallied. **The "one rates object" discipline held.**

**S1 (Sector Factors) = 0 — VERDICT: CORRECT.**
The card collapsed ad/AI/Gemini/Reels into one carried thesis (08-11) and refused to let leftover ad/AI be the sole plus cell (09-22). No same-morning revenue print existed. Reality: no ad-monetization catalyst drove XLC today; the sector moved on macro beta alone. The antitrust overhang (Google ad-tech trial, NM Meta penalties) was correctly treated as carried, not a session shock — and indeed did not produce a structural-remedy selloff. **S1 = 0 was right.**

**S2 (Breadth) = 0 — VERDICT: CORRECT.**
The card explicitly refused to map XLK +0.78% or NQ +0.68% onto XLC breadth (08-27 / 09-10) and refused to feed leftover 1d rel into S2 (09-11). Reality: XLC underperformed despite a green index sleeve — the absence from the PM board was a genuine non-participation tell, not a data gap. **S2 = 0 was right.**

**S3 (Flows/Positioning) = 0 — VERDICT: CORRECT.**
No same-day flow catalyst; prior-month inflows correctly discounted as not same-day support (08-11). Reality: no flow-driven move. **S3 = 0 was right.**

**S4 (ETF Tape) = 0 — VERDICT: CORRECT.**
The card refused to reuse leftover 1d rel −1.11% as a same-print signal (09-11) and treated the multi-horizon lag as history only (08-28). Reality: the lag **persisted** (−0.43% rel today), but the card was right not to *mint* a down call from it — the correct output was flat, and the correct *outcome* was a mild up that still lagged. **S4 = 0 was right.**

**Overall card verdict: the all-zero factor card was the correct read.** The sector delivered a mild absolute gain (matching the "up/mild" direction) but the card's *substance* — that XLC would not convert index beta into relative strength — was fully vindicated by the −0.43% rel.

---

## 3. INTERACTIONS / DOUBLE-COUNT / KNOWABLE-AT-OPEN

**The critical failure was the engine, not the card.** The factor card emitted **leading_sum = 0.0, divergence_flagged = False**, yet the pipeline produced **predicted_direction = up, total_score = 4.631** via `tape_anchor 3.54` (NQ +0.68% / ES +0.50%) and `index_carry 1.091`. This is precisely the failure mode the card warned against:

> "If the engine's tape_anchor tries to lift this off NQ/XLK, **trust the factor card (flat)** — XLC is not on the board (09-16 / 09-18 / two-name S4=0 lesson)."

The engine **did not trust the card.** It converted NQ/ES green into an XLC up call — the exact 08-27 / 09-10 / 09-16 prohibition. The card's own self-audit flagged this: "do not let ES/NQ +0.50/+0.68 or XLK +0.78 mint up."

**Double-count check:** The card correctly identified one rates object (level vs path vs NFP) and one leftover ad/AI object. No double-count occurred in the card. The engine's error was a **cross-object contamination** — importing index beta (NQ/ES) as a sector factor, which is the same category of error as mapping XLK onto XLC.

**Knowable-at-open test:** The soft NFP was **unprinted at the morning cutoff** (08:30 ET binary, size_gate applied). The card was right to refuse to pre-assign it. However, the *direction* of the resolution (dovish → risk-on) was the higher-probability branch given cooler PCE and Williams/Jefferson patience already in the tape. So a **mild up was partially knowable** — but the card's refusal to sign it was defensible given the 10-01 trap and the 9-for-10 direction-miss record. The **relative underperformance was NOT knowable at open** — it required seeing that XLC would fail to convert the dovish path into relative strength, which only the session could reveal.

**Verdict: KNOWABLE_AT_OPEN = partially.** Direction (mild up) was the modal branch; the relative lag was not.

---

## 4. OUTLIERS INSIDE THE SECTOR

- **XLC closed below its open (110.31 → 110.28).** The entire +0.31% gain was an overnight gap. Intraday, the sector was a *seller* into a +0.74% SPY tape. This is the signature of a **distribution/lagging sleeve**, not a leadership sleeve — consistent with the "sector rotation out of communication services" HIT cell.
- **Two-name book divergence:** META/GOOGL were green premarket but the ETF faded. This suggests the mid/small-cap and telecom components (NFLX, DIS, T, VZ, TMUS) dragged, or the mega-caps faded intraday. The morning card's note that "telecom weight is limited as a thesis" and "MAP HEAT Telecom flat" held — no telecom catalyst emerged.
- **No idiosyncratic single-name shock** drove the ETF; the move was pure macro beta with a negative alpha residual.

---

## 5. LESSONS

1. **The all-zero factor card was correct; the engine's tape_anchor override was the error.** This is now the **second consecutive session** (10-01 dir MISS, 10-02 relative MISS) where the engine minted a signed call the card did not support. The 09-16/09-18 "no NQ-ES overlay" lesson needs to be **promoted from a card-level note to an engine-level hard gate**: when `leading_sum = 0` and `divergence_flagged = False`, the tape_anchor must not produce a signed direction.
2. **The "sector rotation out of communication services" HIT cell (0.65) was the highest-value signal on the card.** It correctly predicted the relative lag. Consider weighting RS-rotation cells more heavily in the leading sum for this sector.
3. **The rates *level* vs *path* distinction is now empirically confirmed for XLC.** A dovish path (soft NFP) did not relieve the duration tax because DFII10 2.93 / DGS10 5.29 remained elevated. XLC needs the *level* to fall, not just the path to turn — this should be a standing rule.
4. **Direction accuracy ≠ relative accuracy.** The card's "up/mild" direction was technically correct (+0.31%), but the sector still lost to SPY. The scoring rubric should track **relative** hits separately for a two-name duration book that structurally lags in beta rallies.

---

OUTCOME_BEGIN
SECTOR: Communication Services
ETF: XLC
ETF_PCT: 0.309
SPY_PCT: 0.742
REL_PCT: -0.433
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Soft NFP → dovish rates path → broad risk-on; XLC participated as a lagging sleeve, failing to convert index beta into relative strength as the elevated real-yield level (DFII10 2.93) stayed binding.
KEY_INTERACTION: Engine tape_anchor (NQ +0.68% / ES +0.50%) overrode an all-zero factor card to mint up/mild — the exact 08-27/09-10/09-16 NQ-ES-onto-XLC error the card warned against; card was right, engine was wrong.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Factor card (S0–S4 = 0) was correct and its relative-lag thesis fully vindicated; the engine's tape_anchor override produced a directionally-right but substantively-misleading signed call.
OUTCOME_END