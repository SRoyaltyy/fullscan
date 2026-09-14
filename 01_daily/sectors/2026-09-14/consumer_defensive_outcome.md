# Sector Outcome — Consumer Defensive — 2026-09-14

Actuals: {'etf': 'XLP', 'pct': 1.2473026502584972, 'spy_pct': -0.446162221482016, 'rel': 1.6934648717405132, 'open': 84.37999725341797, 'close': 84.41999816894531, 'source': 'yf_download'}

# Sector Post-Session Review — Consumer Defensive (XLP) — 2026-09-14

## 0. FACTS

**Channel 1 actuals (deterministic, injected):**

| Metric | Value |
|---|---|
| XLP % change | **+1.2473%** |
| SPY % change | **−0.4462%** |
| Relative (XLP − SPY) | **+1.6935%** |
| XLP open | 84.3800 |
| XLP close | 84.4200 |

**Path note (important, and it changes the read):** XLP opened at 84.38 and closed at 84.42 — a **+0.05% open-to-close drift**. The entire +1.25% session gain was **gap**, not intraday trend. The premarket print of **+0.61%** (Channel 1, morning) understated the eventual gap; the ETF gapped up and then went **flat-to-marginally-higher all day**. This is the signature of a **one-shot defensive rotation at the open** that was *not* extended by intraday accumulation. It matters for the audit: the morning call was directionally right, but the *shape* it implied (a live, building FTS bid) is only half-true — the bid was priced in the first minutes and then held, not compounded.

**Cross-check on SPY:** the injected SPY −0.4462% is consistent with a red tape; the search result showing SPY 759.00 → 762.80 is a *different* date's row (the snippet's "Sep 14, 2026" line is the header of a history table, and 762.80 as a close is inconsistent with a −0.45% down day against a 09-11 SP500 print of 7,656.98). I treat the **injected deterministic actuals as authoritative** and do not use the scraped SPY row.

**Regime realized:** risk-off, tech-led, exactly as the morning panel described — but with a **much larger defensive rotation than the morning tape implied**.

---

## 1. What drove the sector today

**Primary driver: a genuine, broad flight-to-safety rotation into staples on a tech-led risk-off day, amplified by XLP's low-beta/under-owned positioning.**

The taxonomy-aligned decomposition:

- **Risk-off tape / flight to safety — HIT, and it was the whole trade.** SPY −0.45% with XLP +1.25% is a **+1.69% relative** move, which is a *large* one-day relative print for a low-beta defensive. The morning panel's own framing (NQ lagging ES by ~93bp, VIX backwardated at 1.135, Kospi −3.26%, oil >$100 Brent) described precisely the regime in which this happens. The regime read was correct; the **magnitude of the rotation was under-modeled**.
- **Sector rotation into defensives — HIT, live and confirmed.** The premarket board (XLP +0.61% best of eleven, XLK −1.95%, XLI −1.13%) was the leading indicator, and it *understated* the close. The rotation was real and it broadened.
- **Input cost spike without pricing power — HIT as a drag, but it lost the day.** Oil at $102–107 was a genuine freight/packaging headwind, and it is the reason the *absolute* move was +1.25% rather than something larger. It did not prevent the relative bid.
- **Real yields rising / duration headwind — HIT as a cap, not a reversal.** 30Y at 5.37 / 10Y at 4.95 / DFII10 +0.09 1d is a real headwind for a bond-proxy. The morning call used this to cap the absolute at "flat-to-mildly-up." **That cap was too tight** — the FTS bid overwhelmed the duration drag on the day.
- **Low-beta leadership inside the sector — HIT.** A +1.25% ETF move with a flat intraday path is consistent with broad, low-beta, high-weight participation (COST/WMT/PG/KO/PM), not a single-name event. The morning's KO +0.6% color was directionally right.

**What did *not* drive it:** no fresh staples earnings, no fresh food-crash print (CPB T+7), no same-day macro binary (CPI was 09-11, FOMC is 09-16). The move was **regime-driven, not idiosyncratic** — which is exactly what the morning thesis said it would be. The error was in the **size**, not the **sign**.

---

## 2. Audit of morning S0–S4 reads against reality

I use the **morning numbers as written**, not post-close rewrites.

### S0_SHARED_MACRO: morning +0.5 → **under-scored**

The morning S0 captured the right *object* (risk-off + NQ lag + oil >$100 = theoretical relative FTS bid) but explicitly **capped it**: "the FTS bid is a *relative* positive, not an absolute one," and "do not upgrade to absolute up (08-18 utilities)." The realized tape shows the FTS bid was **strong enough to produce a large absolute up move** (+1.25%) on a day when SPY fell. The 08-18 template (rising long-end + risk-off → relative outperformance / flat-to-marginally-positive absolute) **understated the absolute** this time.

**Verdict: direction right, magnitude under-scored.** The morning's own divergence check said "the tape confirms the relative bid and is *positive* on absolute" — and then the prose walked that back to "flat-to-mildly-up." The tape was telling the truth; the prose over-applied the duration cap.

### S1_SECTOR_FACTORS: morning −0.5 → **correctly negative, correctly not dominant**

The morning scored S1 negative on the oil/input-cost leg and the carried food-crash weight. Both were real. But the morning's own self-audit said the food-crash drag "reverts to a carried structural weight (≈ half)" and "does not zero the FTS bid." **That was the right call** — S1's −0.5 was a drag that did not flip the sign. The realized +1.25% confirms the drag was a *cap*, not a *reversal*. **HIT on the read, correctly weighted.**

### S2_BREADTH: morning +0.5 → **correct, and the caveat was right**

The morning scored S2 +0.5 on the sector-level rotation but explicitly refused to score it higher on the ETF's own premarket move alone ("single-stale-print rule"). The realized broad +1.25% with a flat intraday path is consistent with **broad participation**, so the +0.5 was directionally correct and the refusal to over-score was prudent. **HIT.**

### S3_FLOWS_POSITIONING: morning +0.5 → **correct, and this was the best read of the morning**

The morning's key insight: "the *prior* week's flow was **out of defensives into growth**, which is exactly the positioning that gets unwound on a tech-led risk-off day. **Not crowded long** (1m rel −0.94%, RSI ~34, below the 50-day) — so there is **no crowded-long unwind fuel** against XLP." That is precisely what happened: an under-owned, washed-out defensive got a clean bid with no positioning overhang. **HIT — the strongest analytical call in the morning set.**

### S4_ETF_TAPE: morning +0.5 → **correct, and the 08-28 discipline paid off**

The morning used the 1d anti-FTS signature (XLP +0.35% vs SPY +0.85% on the benign-CPI day) as *description*, not forecast, explicitly citing the 08-28 lesson ("does not forecast a second lag day"). The realized tape **reversed** that lag day. **HIT — the refusal to extrapolate the lag was the right application of the lesson.**

### Aggregate

| Component | Morning | Realized verdict |
|---|---|---|
| S0_SHARED_MACRO | +0.5 | **Under-scored** (direction right, magnitude capped too tight) |
| S1_SECTOR_FACTORS | −0.5 | Correct (drag, not reversal) |
| S2_BREADTH | +0.5 | Correct |
| S3_FLOWS_POSITIONING | +0.5 | Correct (best read) |
| S4_ETF_TAPE | +0.5 | Correct (08-28 discipline worked) |

**Leading sum +1.5 → flat/mild. Realized: up, notable.** The **direction was a HIT; the magnitude band was a MISS** (mild predicted, notable realized). This is the mirror image of the 09-11 session, where the direction hit and the *relative* missed. Here the direction hit and the *magnitude* missed on the upside.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count audit (morning):** The morning explicitly counted the Hormuz oil spike **once** (S0 risk-off/FTS object) and folded its input-cost leg into S1 **without** re-scoring the same barrel as a second S0 defensive bid. It also counted the Warsh hawkish repricing once (S0 duration overlay). **This discipline was correct and should be preserved** — the realized tape does not suggest any hidden double-count inflated the score. If anything, the morning *under*-counted the FTS bid by capping it.

**The one interaction the morning got wrong:** it treated "risk-off + NQ lag + oil >$100" as producing a **relative** bid with a **capped absolute**, when in fact the combination produced a **large absolute** bid. The missing piece: when the rotation is *into* an under-owned, washed-out sector (S3's read) on a day with **no crowded-long fuel**, the absolute move is not capped by the duration headwind — the duration headwind is a *slow* drag that loses to a *fast* rotation. The morning had all the ingredients (S3's under-owned read, S0's live FTS bid) but did not let them sum to a larger absolute.

**Knowable-at-open test:** **YES — substantially knowable.** The premarket board (XLP +0.61% best of eleven, XLK −1.95%) was live and public before the open. The VIX backwardation, the NQ-lag, the oil spike, and the under-owned positioning were all in the morning panel. The realized +1.25% was a **larger version of the same trade the premarket tape was already showing**. The morning's error was not missing information — it was **discounting the premarket signal** in favor of the duration cap. The honest verdict: **knowable at open = yes; the morning chose to cap it.**

---

## 4. Outliers inside the sector

I have no constituent-level breadth print for 09-14 (the morning flagged this too, and it remains unscored). What can be said from the ETF-level evidence:

- **The move was broad, not a single-name carry.** A +1.25% ETF move with a **flat intraday path** (84.38 → 84.42) is the signature of a **gap-up on broad participation** — a single-name event would typically show intraday drift as the name was repriced. The morning's KO +0.6% color and the sector-level rotation read are consistent with COST/WMT/PG/KO/PM all participating.
- **No outlier reversal.** There is no evidence of a constituent spiking and fading, which would have shown as a higher open and a lower close. The close ≈ open means the bid **held**.
- **Caveat:** without a constituent breadth print, I will not claim a specific name drove the move. The evidence supports "broad, low-beta participation," which is what the morning predicted.

---

## 5. Lessons for the next Consumer Defensive session

1. **When S3 says "under-owned, no crowded-long fuel" AND S0 says "live FTS bid," let the absolute run.** The morning's duration cap (08-18 template) is the right *default*, but it should be **relaxed** when the sector is washed-out and under-owned. The 08-18 template assumes a *crowded* bond-proxy; XLP on 09-14 was not crowded.
2. **The premarket sector board is a leading indicator, not a ceiling.** XLP +0.61% premarket → +1.25% close. The morning treated the premarket print as *confirmation* and then capped the absolute below it. When the premarket print is the **best of eleven** on a red tape, the close is more likely to *exceed* it than to fade to flat.
3. **Preserve the 08-28 discipline (don't extrapolate the lag) and the 09-10 cap (food-crash = one session, needs fresh print).** Both worked today. The food-crash drag correctly reverted to a carried weight and did not zero the FTS bid.
4. **The 09-11 relative-miss lesson and today's magnitude-miss are two sides of the same coin:** the morning is good at *direction* and *relative sign*, and tends to **under-model the size** of regime-driven sector rotations. Consider widening the magnitude band when the premarket board shows a >1.5% spread between the best and worst sector.

---

OUTCOME_BEGIN
SECTOR: Consumer Defensive
ETF: XLP
ETF_PCT: 1.2473
SPY_PCT: -0.4462
REL_PCT: 1.6935
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Broad flight-to-safety rotation into an under-owned, washed-out low-beta defensive on a tech-led risk-off day (NQ lagging ES, VIX backwardated, oil >$100 Brent); the bid gapped in at the open and held flat intraday.
KEY_INTERACTION: The morning correctly identified the live FTS bid (S0) and the under-owned positioning (S3) but capped the absolute with the 08-18 duration template — the duration headwind is a slow drag that lost to a fast rotation, so the two positive reads should have summed to a larger absolute.
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Direction HIT (up), magnitude band MISS (mild predicted vs notable realized); S1/S2/S3/S4 reads correct, S0 under-scored on magnitude — the premarket board (XLP +0.61% best of eleven) was the true leading signal and the morning discounted it.
OUTCOME_END