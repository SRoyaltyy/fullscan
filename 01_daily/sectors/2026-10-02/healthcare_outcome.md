# Sector Outcome — Healthcare — 2026-10-02

Actuals: {'etf': 'XLV', 'pct': -0.03008607260888141, 'spy_pct': 0.742154083513169, 'rel': -0.7722401561220504, 'open': 166.5449981689453, 'close': 166.14999389648438, 'source': 'yf_download'}

# Sector Post-Session Review — Healthcare / XLV — 2026-10-02

## 0. FACTS

**CLAIM:** XLV closed **−0.03%** on 2026-10-02, opening $166.545 and closing $166.150.
**URL:** https://markets.ohlcx.com/xlv (Daily Performance Report)
**PUBLISHED:** 2026-10-02
**QUOTE:** "The day started with XLV ETF opening at $166.48 and closing at $166.11… highest price observed at $167.02 and the lowest at $165.27… a negative trading day for XLV ETF."
**SUMMARY:** Deterministic actuals give ETF_PCT **−0.0301%**, OPEN **166.545**, CLOSE **166.150**. The independent OHLCX report (open $166.48 / close $166.11 / high $167.02 / low $165.27) corroborates both the sign and the flat magnitude. The small open/close deltas vs the injected numbers are vendor rounding; direction and band agree.

**CLAIM:** SPY closed **+0.74%** on the same session.
**URL:** (injected Channel 1 actuals)
**PUBLISHED:** 2026-10-02
**SUMMARY:** SPY_PCT **+0.7422%**. This is a *strong* index day — not the modest +0.2–0.5% futures tape the morning card was anchored to.

**Relative:** REL_PCT = −0.0301 − 0.7422 = **−0.772%**. XLV was **flat in absolute terms while the market ripped ~0.75%** — a clean, unambiguous **relative underperformance** on a risk-on day.

**Path:** Open $166.545 → high $167.02 (early) → low $165.27 → close $166.150. The ETF **faded from the open**, spent the session below its opening print, and closed near the lower-middle of a $1.75 range. There was no late-session rescue; the close is **below the open** by ~0.24%. So the shape is: *green-ish open, steady bleed, flat-to-down close* — the opposite of the "tag-along green" the morning card assumed.

**ACTUAL_DIRECTION:** flat (signally negative, magnitude flat)
**ACTUAL_MAGNITUDE:** flat (|−0.03%| is a rounding-error move; the *relative* miss is the real story)

---

## 1. What drove the sector today

The dominant fact is a **decoupling**: SPY +0.74%, XLV −0.03%. On a day when the index had a strong up-print, healthcare simply **did not participate**. That is the signature of a **sector-specific relative drag**, not a macro beta event — because if this were macro/beta, XLV (beta ~0.65) would have captured roughly +0.4–0.5% of the SPY move and closed clearly green.

Taxonomy-aligned reads:

- **Sector rotation OUT of healthcare (WATCH, 0.50, 2026-10-01)** — this is the factor that actually paid. The morning card carried it as a WATCH but declined to sign it, because it deferred to the live green PM print. The session resolved the ambiguity in favor of the **rotation-out** read: money that was in HC went to the risk-on complex (NQ-led tech, industrials) and HC was left flat.
- **Risk-on tape / equity beta expansion (WATCH, 0.55)** — fired at the *index* level (SPY +0.74%) but **failed to transmit** to XLV. This is the key diagnostic: the beta channel was open and XLV didn't take it.
- **Real yields rising (WATCH, 0.60, as-of 09-30)** — DGS10 5.29 / DFII10 2.93, both up on the week and month. Long-duration healthcare (biotech, life-sciences tools) is the most rate-sensitive defensive sleeve; a strong-tape day with yields still elevated is exactly the environment where HC **lags a risk-on index** rather than leading it.
- **Drug pricing policy relief (WATCH, 0.45)** — the Axios GLOBE/MFN Part B demo shrink (~$440M/5yr) was a *mild residual relief* item, and it did **not** produce a re-rate. Consistent with the morning's own call that it was "not a mega-cap re-rate."
- **Biotech risk-off / funding winter (WATCH, 0.40)** — FDA user-fee submission stall during the funding lapse remained a live XBI/small-cap overhang; it did not become an XLV spine, but it capped the high-beta end.

**PRIMARY_DRIVER:** Sector-specific relative drag — XLV failed to capture a strong SPY up-day (rotation out of healthcare + rate-sensitive duration sleeve lagging a risk-on tape), leaving the ETF flat while the index rose ~0.75%.

---

## 2. Audit of morning S0–S4 reads against reality

The morning card scored **S0=S1=S2=S3=S4 = 0**, leading_sum = 0, and explicitly invoked the **09-23 empty-spine gate** ("if S0–S4 net = 0 and S4 = 0… trust this factor card (flat)"). The engine nonetheless minted **up/mild** off `index_carry` (1.091) and tape_anchor (1.506, ES +0.50% / PM:XLV +0.23%).

**Verdict on the factor card: directionally RIGHT, magnitude RIGHT, but for a reason it under-weighted.**

- **S0 (shared macro) = 0 — PARTIAL MISS.** The card correctly refused to rewrite S0 as a duration bid, and correctly refused to map XLK/NQ into S0=+1. But it also **failed to sign the beta channel at all**. The reality is that S0 *should* have been a small **positive** for a beta-capture sector on a +0.74% SPY day — and the fact that XLV got **zero** of it is the entire story. The card treated "modest NQ-led risk-on" as neutral; in fact the index delivered a *strong* day and HC's non-participation is the signal. S0=0 was defensible pre-open but understated the beta that was on offer.
- **S1 (sector factors) = 0 — CORRECT.** No CMS-rate HIT (April finalization stale), IRA paid Sep 30, no same-morning Rx smash, no FDA cluster. The Axios demo shrink was correctly sized as residual. **S1=0 holds.**
- **S2 (breadth) = 0 — CORRECT, and vindicated.** The card refused to copy Thursday's −1.50% rel into S2 and refused to zero it against a "worst-on-board" parent. Reality: XLV was **not** worst-on-board (it was flat, not down hard), and breadth did not expand. S2=0 is right.
- **S3 (flows) = 0 — CORRECT.** 1m rel −3.35% is the opposite of a crowded long; no forced selling, no volume-spike chase. S3=0 holds.
- **S4 (ETF tape) = 0 — CORRECT, and this is the card's best call.** The card explicitly warned that putting S4<0 would make leftover tape the only signed factor (the 09-22 leak), and that S4>0 would invent a bounce. **S4=0 was exactly right** — the tape neither confirmed a bounce nor mandated a down. The 09-23 empty-spine gate **should have governed**, and it pointed to **flat** — which is precisely what happened.

**MORNING_READ_VERDICT:** The factor card's **flat** call was correct on both direction and magnitude; the engine's **up/mild** was wrong on direction. The card's error was *not* in its S-scores but in its **confidence in the flat read** — it should have been more assertive that a beta-capture sector on a strong-tape day with all relative horizons red was a **relative-lag setup**, not a neutral one.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count check:** The card was disciplined here. It counted oil-offered **once** (not again as HC rotation), refused to restack the NFP yield dip into both S0 and S1, and kept nested MAP HEAT (Plans/Facilities OVERRIDE) out of the XLV parent. **No double-count detected.** The one *under*-count: the card treated "modest NQ-led risk-on" as a neutral S0 input when the realized index move (+0.74%) was materially stronger than the futures panel implied — so the beta that was *available* to XLV was larger than the card assumed, making the non-capture more diagnostic.

**Knowable-at-open test:** **PARTIALLY knowable.**
- *Knowable at open:* All four relative horizons were red (1d −1.50%, 3d −2.74%, 1w −1.74%, 1m −3.35%). The 08-13 leftover-RS leadership was absent. Real yields were up on week/month. The sector had just been smashed −1.32% on a green SPY tape the prior day. **A sector with four red relative horizons and no RS leadership, on a risk-on day, is a relative-lag candidate — that was knowable.**
- *Not knowable at open:* The *magnitude* of the SPY move (+0.74% vs the +0.2–0.5% futures panel). The card anchored to ES +0.50% / NQ +0.68%; the index delivered more. But this cuts *against* the up/mild call, not for it — a stronger index day with XLV flat is a *worse* relative outcome, and the card's own beta check ("SPY/ES ~+0.5% × XLV beta ~0.65 ≈ +0.3% gross — flat band") already flagged that even the *expected* beta capture was marginal. The realized outcome (zero capture) is the same family of read, just worse.

**KEY_INTERACTION:** The card's own beta arithmetic (+0.3% gross expected) and its 09-23 empty-spine gate (flat) were **both pointing at flat**, while the engine's `index_carry`/tape_anchor overrode them to up/mild. The interaction that decided the day: **a beta-capture sector that fails to capture beta on a strong-tape day is a relative-lag signal, and the card had the ingredients to say so but deferred to the engine.**

---

## 4. Outliers inside the sector

- **The ETF itself is the outlier:** XLV −0.03% vs SPY +0.74% is a **−0.77% relative gap** on a day with no sector-specific negative catalyst. That gap is the entire post-session story and it is a *breadth/participation* failure, not a single-name event.
- **No single-name driver:** The morning card correctly nested AMGN/ABT/BMY/REGN/CAH/CI/BSX/LLY as single-name or T+n. Nothing in the session produced a mega-cap Rx smash or an FDA cluster that would explain the gap — reinforcing that the drag was **structural (rotation + duration)**, not idiosyncratic.
- **XBI sleeve:** The biotech end remained the high-beta, rate-sensitive overhang (FDA submission stall + real yields up). It did not lead XLV up, consistent with the card's refusal to promote the XBI sleeve.
- **Defensive sleeve (XLP):** The morning board showed staples (+0.60%) outrunning healthcare (+0.23%) pre-open. On a strong risk-on day, the *defensive* complex lagging the index is normal — but XLV lagging by −0.77% while XLP's relative position was already known is the tell that HC was being **sold as a funding source** for the risk-on trade, not held as a haven.

---

## OUTCOME_BEGIN
SECTOR: Healthcare
ETF: XLV
ETF_PCT: -0.03
SPY_PCT: 0.74
REL_PCT: -0.77
ACTUAL_DIRECTION: flat
ACTUAL_MAGNITUDE: flat
PRIMARY_DRIVER: Sector-specific relative drag — XLV failed to capture a strong SPY up-day (rotation out of healthcare + rate-sensitive duration sleeve lagging a risk-on tape), closing flat while the index rose ~0.75%.
KEY_INTERACTION: The card's own beta arithmetic (+0.3% gross) and the 09-23 empty-spine gate both pointed to flat, but the engine's index_carry/tape_anchor overrode them to up/mild; a beta-capture sector failing to capture beta on a strong-tape day is a relative-lag signal the card had the ingredients to sign but deferred on.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Factor card's flat call was correct on direction and magnitude (S1–S4 all validated); the engine's up/mild was wrong on direction — the card's only error was insufficient conviction that four red relative horizons + no RS leadership on a risk-on day = relative-lag setup, not neutral.
OUTCOME_END