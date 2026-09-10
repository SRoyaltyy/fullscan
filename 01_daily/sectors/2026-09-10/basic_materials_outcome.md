# Sector Outcome — Basic Materials — 2026-09-10

Actuals: {'etf': 'XLB', 'pct': -1.2259215325893469, 'spy_pct': -0.5994238166152965, 'rel': -0.6264977159740504, 'open': 50.7599983215332, 'close': 50.7599983215332, 'source': 'yf_download'}

# Sector Post-Session Review — Basic Materials (XLB) — 2026-09-10

## 0. FACTS

**CLAIM:** XLB closed at 50.76 on 2026-09-10, down 1.23% on the session.
**URL:** https://finance.yahoo.com/quote/XLB/ ; https://www.marketwatch.com/investing/fund/xlb
**PUBLISHED:** 2026-09-10
**QUOTE:** "XLB Previous Close 51.39 Open 50.76" (Yahoo); "Real time quote $50.67 −0.72 −1.40% Previous Close $51.39" (MarketWatch, 2:27pm EDT)
**SUMMARY:** XLB opened at 50.76 (already −1.23% from the 51.39 prior close) and the deterministic feed shows OPEN = CLOSE = 50.76. The tape gapped down at the open and never recovered — a full-session, no-bounce down day. The intraday range (Robinhood: $50.58–$51.35) shows the high was set early and the low came later, i.e. persistent selling pressure, not a morning flush-and-recover.

**CLAIM:** SPY closed −0.60% on 2026-09-10.
**URL:** (deterministic feed, injected)
**PUBLISHED:** 2026-09-10
**SUMMARY:** Broad market down modestly; XLB down roughly twice as much.

**Relative return:** XLB −1.23% vs SPY −0.60% → **rel −0.63%**. This is a genuine underperformance day, and it is *larger* than the 1d rel of −0.59% that was on the tape through 09-09 — the sector's relative bleed continued and slightly accelerated.

**Path:** Gap-down open (50.76 vs 51.39 prior close), high $51.35 early, low $50.58, close 50.76. Net: opened at the low end of the day's range, briefly tried to reclaim, failed, closed near the open. No intraday reversal. This is a **trend-down day**, not a whipsaw.

**Actual direction:** down. **Actual magnitude:** notable (|−1.23%| with rel −0.63% and a full-session trend-down path — this clears the "mild" band).

---

## 1. What drove the sector

The morning thesis was a **second consecutive fresh-kinetic Hormuz/oil escalation day** with the 8/18 metals-co-move floor ban firing. The post-close evidence says that thesis was **directionally correct and the driver set was right**, but the *magnitude* was understated — and the reason is that the metals collapse was **far worse than the morning read assumed**.

**CLAIM:** Copper fell to $6.44/lb on 2026-09-10, down 5.36% from the previous day.
**URL:** https://tradingeconomics.com/commodity/copper
**PUBLISHED:** 2026-09-10
**QUOTE:** "Copper fell to 6.44 USD/Lbs on September 10, 2026, down 5.36% from the previous day. Over the past month, Copper's price has fallen 2.62%, but it is still 39.67% higher than a year ago."
**SUMMARY:** This is the single most important fact of the session. The morning read had copper at **−2.89%** (to $6.681). It closed at **−5.36%** (to $6.44). The industrial-metals collapse **nearly doubled in severity** after the open. The morning's "clean HIT" on the collapse sub-channel was not just right — it was *understated by roughly 2.5 percentage points*.

**CLAIM:** A second source confirms copper at $6.44/lb, down ~4.91% over 24h.
**URL:** https://metalcharts.org/copper-price-per-ounce ; https://iscrapapp.com/metals/bare-bright-copper/
**PUBLISHED:** 2026-09-10
**SUMMARY:** Cross-source confirmation of the ~$6.44 print. (One outlier source, metalcharts' main page, showed $6.78 "up 0.22%" — that is a stale/rolling quote and is contradicted by both TradingEconomics and the scrap-price feed; treat the $6.44 / −5% figure as the session truth.)

**Driver taxonomy (aligned to the morning's own factor list):**

| Factor | Morning read | Reality | Verdict |
|---|---|---|---|
| Industrial metal price collapse | copper −2.89% | copper **−5.36%** | HIT, understated |
| Oil/Hormuz risk-off overlay | Brent >$100, 2nd session | persisted | HIT |
| Real yields rising / hawkish Fed | DGS10 4.80, Warsh hawkish | persisted | HIT |
| Risk-off tape | ES +0.11%, NQ −0.17% | SPY −0.60% | HIT (SPY weaker than the ES read implied) |
| Gold/silver monetary surge | GC −0.56%, SI −2.43% | faded | MISS (correctly called as a miss) |
| USD spike | DXY −0.02% | no spike | MISS (correctly called) |
| China demand shock | T-1, 49.8 NBS | no fresh print | HIT (structural, not same-morning) |

The **primary driver** was the industrial-metals leg of the complex, and specifically the *acceleration* of the copper collapse intraday. The chemicals-heavy book (LIN/SHW/ECL ~40–50% of XLB) faced the oil-feedstock cost squeeze the morning note flagged, and the copper-miner sleeve (~10–15%) got hit by a 5%+ copper move. Both sleeves negative simultaneously = the "no defensive pocket" condition the 09-09 lesson described, and it played out exactly.

---

## 2. Audit of morning S0–S4 reads against reality

**Critical discipline note:** I am auditing the *morning numbers as written*, not rewriting them with post-close knowledge.

### S0 = −1 (shared macro) — **VERDICT: correct, arguably one notch light**

The morning read correctly identified the oil/Iran escalation + hawkish Fed + risk-off + backwardation as a negative map for a cyclical. It explicitly refused to score −2 because "ES is only +0.11%, DXY is not a spike, no same-morning China print, CPI is tomorrow." That reasoning was sound *at the open*. In reality SPY closed −0.60% — weaker than the +0.11% ES read implied — so the risk-off leg was a touch stronger than the morning assumed. But −1 was defensible and the direction was right. **No error of substance.** If anything, the morning's caution about not over-scoring S0 was the correct instinct; the miss was elsewhere.

### S1 = −2 (sector factors) — **VERDICT: correct, and the 09-09 binding rule paid off**

This is the cleanest call of the session. The morning note invoked the **09-09 composition/magnitude lesson** — "when the 8/18 metals-co-move floor ban fires, there is NO defensive pocket inside XLB; if all four S1 sub-channels align negative with zero offset, score S1=−2 and S2=−1, not −1/0." It then applied exactly that: chemicals oil-cost drag + copper collapse + gold/silver fade + China contraction, zero offset → **S1 = −2**.

Reality: copper collapsed 5.36%, the chemicals sleeve faced the oil squeeze, gold/silver did not provide the 8/14 offset (correctly ruled OFF), and China's structural weakness persisted. **All four sub-channels were negative. S1 = −2 was exactly right.** The binding rule from 09-09 was the single most valuable piece of memory in this run.

### S2 = −1 (breadth) — **VERDICT: correct**

The morning read said: don't copy yesterday's lag as fresh confirmation (8/28), but the *live same-morning* metals collapse IS same-morning confirmation of broad-based weakness. That distinction was correct and well-drawn. The intraday path (gap down, brief reclaim to $51.35, then fade to $50.58 and close at the open) is consistent with uniformly negative breadth — no sleeve led a recovery. **S2 = −1 correct.**

### S3 = 0 (flows) — **VERDICT: correct / unverifiable**

~1m net outflows, not a washout, not a volume spike. No evidence in the post-close data to contradict this. **S3 = 0 stands.**

### S4 = 0 (tape, confirmation only) — **VERDICT: correct application, but the cap cost magnitude**

The morning read correctly applied the **8/27 S4-cap**: 1d rel −0.59% < 0.5% threshold → S4 cannot be a ± confirmation. It also correctly refused to import the prior-day lag as fresh (09-04). **S4 = 0 was the right *rule application*.**

But here is the honest audit finding: the 1d rel of −0.59% was *just barely* under the 0.5%... no — it was **over** 0.5% in absolute terms (−0.59%). The cap rule as written ("1d rel −0.59% <0.5% → S4 cannot be a ± confirmation") appears to have been applied with a sign/threshold ambiguity. If the rule is "|1d rel| < 0.5% → no confirmation," then −0.59% *clears* the threshold and S4 could have been −1. This is a **rule-interpretation question worth flagging**, not a clear error — but it is the most likely place where a point was left on the table.

### Reconciliation audit

Morning: (−1 + −2 + −1 + 0 + 0) × 0.9 = **−3.6 → down/mild**.
Actual: down/**notable**.

The **direction was a HIT**. The **magnitude band was a MISS** — actual was notable, predicted mild. This is the *second consecutive* magnitude miss in the same direction (09-09 was also dir HIT / mag MISS, actual notable vs predicted mild). That is now a **pattern**, not noise.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count check:** The morning note was careful here — it counted Hormuz/oil **once** in S0 as the risk-off overlay and explicitly refused to *also* credit copper as a positive floor (8/18 ban). That is correct non-double-counting. The oil cost-squeeze on chemicals was counted in S1 as a sector factor, which is a *different transmission channel* (input cost) than the S0 risk-off overlay — legitimate, not double-counted.

**The real interaction the morning under-weighted:** the **copper collapse and the oil squeeze are not independent** — both are expressions of the same global risk-off/strong-dollar-real-yields impulse hitting a cyclical commodity complex. When they fire *together and accelerate*, the ETF's two largest sleeves (chemicals ~40–50%, miners ~10–15%) both bleed, and there is no offset. The morning note *identified* this ("no defensive pocket") but scored the *magnitude* as if the moves were static at the morning print. They were not — copper went from −2.89% to −5.36%.

**Knowable-at-open test:** Was the notable magnitude knowable at the open?
- The **gap-down open** (50.76 vs 51.39, −1.23% at the bell) was **knowable at the open** and was itself already at the *notable* threshold. The morning note predicted "mild" while the ETF was *already trading at −1.23% pre-fill*. This is the crux: **the open itself falsified the mild band before the session began.**
- The copper acceleration to −5.36% was **not** fully knowable at the open (it developed intraday), but the *direction and the no-offset structure* were.
- **Verdict: PARTIALLY knowable at open.** The gap-down alone should have pushed the magnitude band to at least "notable," independent of the intraday copper acceleration.

**This is the key lesson:** when XLB gaps down >1% at the open on a risk-off/metals-collapse day, the "mild" band is already dead. The morning note's magnitude discipline ("rolling mag discipline favors mild on modest |score|") was applied *against* a tape that was already showing notable. The DO-INSTEAD instruction ("shrink confidence on modest |score| when magnitude historically misses") was followed — but it shrank confidence in the *wrong direction*: it should have *raised* the magnitude band given the gap.

---

## 4. Outliers inside the sector

**CLAIM:** One source (Perplexity finance) described XLB as having "closed essentially flat as strength in copper and gold miners" and "near 52-week highs."
**URL:** https://www.perplexity.ai/finance/XLB
**PUBLISHED:** undated / rolling
**SUMMARY:** This is a **stale or hallucinated summary** — it directly contradicts the deterministic actuals (−1.23%), the Yahoo/MarketWatch prints, and the copper collapse. It likely reflects an *earlier* session (XLB was near highs in late August per the Moomoo 09-08 print of 51.94). **Discard as an outlier.** Flagging it because it is exactly the kind of AI-generated "flat close" narrative that could corrupt a memory index if ingested.

**Genuine intraday outlier:** The early high of $51.35 (Robinhood range) vs the close of 50.76 — XLB briefly traded *green-ish* relative to the open before fading. That early bid was the last gasp of the "copper squeeze" narrative from 09-08; it failed within the session. No single-name outlier (e.g., APD's already-traded beat) rescued the tape — consistent with the morning's correct call that APD was "a single-name positive, not an XLB-wide thrust."

**Cross-source outlier:** metalcharts' main page showed copper "$6.78, up 0.22%" — contradicted by TradingEconomics ($6.44, −5.36%) and the scrap feed. Stale rolling quote; discard.

---

## 5. Verdict and lessons

**Direction: HIT.** The morning call of "down" was correct, well-reasoned, and the driver set (oil/Hormuz risk-off + metals co-move collapse + no defensive pocket) was exactly what drove the tape.

**Magnitude: MISS (second consecutive).** Predicted mild, actual notable. The miss was *structural*, not random: the morning note applied magnitude discipline against a tape that had **already gapped to −1.23% at the open**. The gap itself was the tell.

**Rule that worked:** the **09-09 composition/magnitude binding rule** (all four S1 sub-channels negative + zero offset → S1=−2, S2=−1). This was the highest-value memory in the run and it fired cleanly.

**Rule to flag:** the **8/27 S4-cap** interpretation. 1d rel −0.59% is *above* the 0.5% threshold in absolute terms; the cap may have been mis-applied, costing a confirmation point.

**New lesson to write:** *When XLB gaps down ≥1% at the open on a risk-off + metals-collapse day, the "mild" magnitude band is already falsified at the bell. Do not apply rolling-magnitude discipline to shrink the band when the opening print itself is already in the notable range. The gap is the magnitude signal.*

**Memory hygiene:** flag the Perplexity "flat close / near highs" summary as a stale outlier — do not let it enter the index.

---

OUTCOME_BEGIN
SECTOR: Basic Materials
ETF: XLB
ETF_PCT: -1.23
SPY_PCT: -0.60
REL_PCT: -0.63
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Accelerating industrial-metals collapse (copper −5.36% intraday, vs −2.89% at the morning print) combined with the oil-feedstock cost squeeze on the chemicals-heavy book — both XLB sleeves negative simultaneously, no defensive pocket.
KEY_INTERACTION: Copper collapse and oil cost-squeeze are two expressions of the same risk-off/real-yields impulse; firing together they hit chemicals (~40–50%) and miners (~10–15%) at once, eliminating any offset — the morning note identified the structure but scored the magnitude as if the moves were static.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Direction HIT and driver set correct (09-09 binding rule fired cleanly, S1=−2/S2=−1 exactly right); magnitude MISS (predicted mild, actual notable) because the −1.23% gap-down open had already falsified the mild band before the session began — second consecutive dir-HIT/mag-MISS.
OUTCOME_END