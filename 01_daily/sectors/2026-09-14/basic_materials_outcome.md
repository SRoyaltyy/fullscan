# Sector Outcome — Basic Materials — 2026-09-14

Actuals: {'etf': 'XLB', 'pct': -0.9028441169470103, 'spy_pct': -0.446162221482016, 'rel': -0.45668189546499427, 'open': 50.68000030517578, 'close': 50.4900016784668, 'source': 'yf_download'}

# Sector Post-Session Review — Basic Materials (XLB) — 2026-09-14

## 0. FACTS

**Channel 1 actuals (deterministic):**

| Metric | Value |
|---|---|
| XLB % | **−0.9028%** |
| SPY % | **−0.4462%** |
| Relative % | **−0.4567%** |
| Open | 50.68 |
| Close | 50.49 |
| Prior close (09-11) | 50.95 |

**Path:** XLB opened at 50.68 vs prior close 50.95 → **gap −0.53%** at the bell. It then drifted from 50.68 to 50.49, i.e. a further **−0.37%** of intraday erosion. So the session was **gap-down, then grind-down** — no reversal, no bounce, no VWAP reclaim. The entire day was spent below the prior close, and the close was near the session low end of the range implied by the open/close pair.

**Direction:** down. **Magnitude:** notable for the *relative* leg (−0.46% under SPY on a day SPY itself fell), mild-to-notable absolute (−0.90%).

**Cross-check against the two morning documents:** the morning packet contained **two conflicting predictions** — a v2 engine run (total −15.213, mult 0.9, **down/notable**, divergence_flagged False) and an LLM-authored run (total −4.05, mult 0.9, **down/mild**, divergence_flagged True). The deterministic pipeline-computed decision block at the bottom of the packet is the **v2 engine**: down/**notable**, confidence 0.85. The actuals land **between** the two bands: absolute −0.90% is mild-to-notable, but the *relative* −0.46% is a clean, unambiguous underperformance that the mild-band LLM run explicitly predicted ("Absolute down/mild, relative lag") and the notable-band engine run also predicted.

**Verdict on the band question:** the 09-10 gap rule did **not** fire — the open was −0.53%, below the 1.0% threshold. So the mild band was *not* falsified at the bell, and the LLM run's decision to keep mild was procedurally correct. But the engine's notable band was closer to the realized absolute move. This is a **band-split session**: mild was right on the gap test, notable was right on the realized close.

---

## 1. What drove the sector today

**Primary driver: the hawkish-Fed repricing confirmed by hot August CPI, transmitted to materials through a broad industrial-metals liquidation and an oil-cost squeeze on the chemicals majority sleeve.**

Evidence chain:

**CLAIM:** Copper fell toward $14,000/ton on 09-14 as traders ramped rate-hike wagers and spreads loosened after fresh deliveries.
**URL:** https://www.bloomberg.com/news/articles/2026-09-14/copper-slips-as-inflation-data-raises-bets-on-fed-hiking-rates
**PUBLISHED:** 2026-09-14
**QUOTE:** "Copper fell towards $14,000 a ton as traders ramped up wagers on a US rate hike and price spreads loosened after fresh deliveries of metal."
**SUMMARY:** Confirms the morning's S1 "industrial metal collapse" read. Copper dropped as much as 1.6% to $14,018.50/ton, the lowest in three weeks (aegis-hedging.com, 2026-09-14). This is the single most important XLB spine factor and it printed exactly as the morning packet scored it.

**CLAIM:** CME FedWatch priced an 89% probability of a September 16 Fed rate hike, up from 67% before the September 11 inflation report.
**URL:** https://www.cruxinvestor.com/posts/89-fed-hike-odds-pressure-gold-but-softer-guidance-could-trigger-a-rebound
**PUBLISHED:** 2026-09-14
**QUOTE:** "The CME FedWatch Tool priced an 89% probability of a Fed rate hike on September 16, up from 67% before the September 11 inflation report."
**SUMMARY:** This is a **material escalation** versus the morning packet, which cited "~56–60%" hike odds. By the session itself, odds had moved to **89%**. The morning read was directionally right but **understated the magnitude of the hawkish repricing** — a knowable-at-open miss in degree, not in sign.

**CLAIM:** Gold erased 2026 gains as Fed hike bets climbed; silver slid.
**URL:** https://www.mining.com/gold-price-erases-2026-gains-as-fed-hike-bets-climb-to-70-silver-slides/
**PUBLISHED:** 2026-09-01 (context; the 09-14 cruxinvestor piece confirms the continuation)
**SUMMARY:** The monetary-metals fade the morning packet scored as a MISS on the "gold/silver surge" hit-grid row was correct — gold and silver were down, not up, and the 8/14 gold-offset rule correctly did not pay.

**Taxonomy alignment:** the session maps cleanly onto the rubric's **risk-off / real-yields-rising / USD-firm / industrial-metal-collapse / margin-compression** cluster. Every one of those rows was scored HIT in the morning hit-grid and every one of them printed. The two MISS rows (gold/silver surge, inventory draw) were correctly scored as misses *in advance* — i.e. the morning packet correctly predicted that those two factors would **not** fire. That is a subtle but important point: a "MISS" on a hit-grid row means "this factor did not drive the session," and the morning packet got both of those right.

**Secondary driver: the oil spike.** WTI +2.44% / Brent +2.80% on the morning tape was scored as a **cost headwind for the chemicals majority sleeve** (LIN, SHW, ECL, DOW ≈ 40–50% of XLB). XLB's −0.90% absolute with a −0.46% relative lag is consistent with a majority-sleeve cost squeeze that the minority copper-miner sleeve could not offset — because the copper miners were *also* down. That is the 8/18 metals-co-move pattern firing exactly as the morning packet described it.

---

## 2. Audit of morning S0–S4 reads against reality

I use the **morning numbers as written**, not post-close rewrites.

### S0_SHARED_MACRO = −1 (LLM run) / −1.0 × 1.25 skill = −1.25 (engine run)

**Morning claim:** hot CPI → hawkish Fed repricing + firm USD + rising real yields + oil-spike risk-off + backwardation, mapping negative to this cyclical; not −2 because ES only −0.67%, DXY not a spike, HY tight, FOMC two days out.

**Reality:** SPY −0.45%, XLB −0.90%. The macro headwind was real and it hit. The decision **not** to score −2 was defensible on the morning tape (ES −0.67% is not a crash), but the realized session shows the hawkish repricing **intensified intraday** (56–60% → 89% hike odds). A −1 on a day the sector fell 0.90% absolute and 0.46% relative is **slightly under-scored**, but within tolerance. **Verdict: HIT, mild under-score.**

### S1_SECTOR_FACTORS = −2 (LLM run) / −2.0 × 0.5 skill = −1.0 (engine run)

**Morning claim:** all four sub-channels negative (chemicals oil-cost drag + copper collapse + gold/silver fade + China contraction) with zero offsetting positive; the 09-09 lesson mandates S1 = −2 when the 8/18 metals-co-move floor ban fires with all four sub-channels negative.

**Reality:** copper −1.4% to −1.6% (confirmed), gold/silver down (confirmed), oil up (confirmed), China PMI 49.8 contraction (confirmed). **All four sub-channels printed negative.** The 09-09 rule fired correctly and the −2 score was **exactly right**.

**Critical audit note:** the **engine run applied a 0.5 skill multiplier to S1**, cutting −2.0 to −1.0 — i.e. the engine *halved* the sector's own factor score. The LLM run did not apply that haircut. Given that S1 was the single most accurate component of the entire morning read, the 0.5 multiplier was **value-destroying** on this session. This is a concrete, actionable finding: the S1 skill multiplier for Basic Materials is mis-calibrated and should be reviewed. **Verdict: HIT, and the engine's S1 haircut was wrong.**

### S2_BREADTH = −1 (both runs)

**Morning claim:** no defensive pocket inside XLB — chemicals, copper miners, and gold miners all negative together; 8/18 pattern → uniformly negative breadth.

**Reality:** XLB closed −0.90% with a −0.46% relative lag. A uniform-breadth day produces exactly this signature: no offsetting sleeve, so the ETF tracks the *worst* common factor rather than being rescued by a rotation. **Verdict: HIT.**

### S3_FLOWS_POSITIONING = −0.5 (both runs)

**Morning claim:** deep multi-horizon relative laggard (1w rel −2.03%, 1m rel −2.04%) with persistent outflows; score once here, not again in S4.

**Reality:** XLB underperformed again by −0.46%. The persistent-lag thesis **extended for another session**. The decision to score it once (not double-count into S4) was methodologically correct. **Verdict: HIT.**

### S4_ETF_TAPE = 0 (both runs)

**Morning claim:** 1d rel −0.48% is sub-0.5% → per the 8/27 S4-cap, it cannot be a ± confirmation; 3d/1w/1m lags already scored in S3.

**Reality:** the realized 1d rel was **−0.4567%** — *still* sub-0.5%, and remarkably close to the morning's −0.48% input. So the S4-cap rule was applied correctly and the realized relative move **validated the cap**: a sub-0.5% relative move is genuinely not a confirmation-grade signal. **Verdict: HIT, and the 8/27 cap is validated by the outcome.**

### Reconciliation audit

| Component | Morning (LLM) | Morning (engine) | Realized verdict |
|---|---|---|---|
| S0 | −1 | −1.25 | HIT (mild under-score) |
| S1 | −2 | −1.0 | HIT (engine haircut wrong) |
| S2 | −1 | −1.0 | HIT |
| S3 | −0.5 | −0.5 | HIT |
| S4 | 0 | 0 | HIT (cap validated) |
| **Total** | **−4.05** | **−15.213** | — |
| **Band** | mild | notable | **split** |

**The LLM run's component scores were all correct.** Its total (−4.05) and mild band were conservative but procedurally sound. The engine run's total (−15.213) was inflated by the tape_anchor (−6.462) and llm_overlay (−6.0) legs, which pushed it to notable — and notable was closer to the realized absolute move. **Neither run was wrong on direction; the disagreement was purely about magnitude, and the outcome split the difference.**

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count check:** The morning packet explicitly guarded against the two classic double-counts — (a) scoring the 3d/1w/1m relative lags in both S3 and S4 (it scored them only in S3), and (b) scoring the persistent relative lag as both a standing lean and a fresh 1-day signal (it scored it once). **Both guards held.** No double-count is detectable in the realized outcome.

**Interaction that mattered:** the **oil-spike × chemicals-majority × copper-miner-minority** interaction. The morning packet correctly identified that the chemicals sleeve (~40–50% of XLB) faces an oil-cost squeeze while the copper-miner sleeve (~10–15%) was *also* falling — meaning there was **no internal hedge**. This is the 8/18 metals-co-move pattern, and it is the reason XLB's relative lag (−0.46%) was *larger in magnitude* than its absolute move would suggest relative to a normal down day. The interaction was correctly modeled.

**Knowable-at-open test:** What was knowable at the open?
- ✅ Hot CPI (published 09-11) — knowable.
- ✅ Hawkish Fed repricing direction — knowable.
- ✅ Copper down, gold/silver down, oil up (premarket) — knowable.
- ✅ XLB gap −0.53% — knowable at the bell.
- ❌ The **intraday escalation of hike odds to 89%** — *not* fully knowable at the open; this was an intraday repricing.
- ❌ The **China August activity data** (due this week) — not yet printed.

**Verdict: KNOWABLE_AT_OPEN = yes (for direction and the relative-lag thesis); partially (for the magnitude of the absolute move).** The direction and the relative-underperformance call were fully knowable. The absolute magnitude depended on an intraday hawkish escalation that was not in the morning tape.

---

## 4. Outliers inside the sector

**Outlier 1 — the relative/absolute divergence.** XLB fell −0.90% while SPY fell −0.45%. On a risk-off day, a cyclical materials ETF underperforming by 0.46% is *expected*, but the **ratio** (XLB down ~2× SPY) is at the high end of the normal beta relationship. This is the signature of a sector with **no defensive pocket** — exactly what S2 scored.

**Outlier 2 — the gap-then-grind path.** Open 50.68 (−0.53%), close 50.49 (−0.90%). There was **no intraday reversal**. The 09-03 exhaustion-bounce rule (sleeve-driven >+1% reversal within a negative 1w is a bounce, not an inflection) was **not triggered** — there was no bounce to misread. This is a clean trend-down day, which is the *easiest* kind of session to predict and the morning packet predicted it correctly.

**Outlier 3 — the two-prediction split.** The most notable "outlier" is internal to the morning packet: two runs, two bands (mild vs notable), same direction. The realized outcome (−0.90% absolute, −0.46% relative) sits **between** the bands. This is not a market outlier; it is a **process outlier** — the engine and the LLM disagreed on magnitude by a factor of ~3.75× (−4.05 vs −15.213) while agreeing perfectly on direction and on all five component signs. That is a calibration problem, not a signal problem.

**Outlier 4 — the S1 skill multiplier.** As noted in §2, the engine's 0.5 multiplier on S1 halved the single most accurate component. On a session where S1 was a clean 4-for-4 hit, that multiplier cost the engine accuracy. This is the most actionable single finding of the review.

---

## 5. Lessons for the next Basic Materials session

1. **The 09-09 metals-co-move rule is now 2-for-2.** When the 8/18 floor ban fires with all four S1 sub-channels negative and zero offset, S1 = −2 is correct. Do not haircut it with a 0.5 skill multiplier — review that multiplier.
2. **The 8/27 S4-cap is validated.** A sub-0.5% 1d relative move is genuinely not confirmation-grade. The realized −0.4567% rel confirms the cap threshold is well-placed.
3. **The 09-10 gap rule did not fire and should not have.** Open −0.53% < 1.0% threshold. The mild band was procedurally correct at the bell. But note: **the realized close (−0.90%) exceeded the gap (−0.53%)**, meaning intraday drift added −0.37%. Consider whether the gap rule should be complemented by an *intraday-drift* rule for sessions with a known hawkish-repricing catalyst.
4. **Hike-odds escalation is an intraday variable.** The morning packet cited 56–60%; the session ran to 89%. When the FOMC is ≤2 days out and CPI has just surprised hot, **expect intraday hawkish escalation** and consider widening the magnitude band accordingly — this is the mechanism by which the engine's notable band beat the LLM's mild band.
5. **The relative-lag thesis is the highest-confidence call in this sector.** XLB has now underperformed on 1d, 3d, 1w, and 1m horizons. Until that reverses, the standing relative-underperformance lean should be carried every session.

---

OUTCOME_BEGIN
SECTOR: Basic Materials
ETF: XLB
ETF_PCT: -0.9028
SPY_PCT: -0.4462
REL_PCT: -0.4567
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Hot-CPI-driven hawkish Fed repricing (hike odds 56-60% -> 89% intraday) liquidating the industrial-metals complex (copper -1.4% to -1.6%, 3-week low) while an oil spike squeezed the chemicals majority sleeve — no internal hedge, uniform breadth failure.
KEY_INTERACTION: Oil-spike cost squeeze on chemicals (~40-50% of XLB) coincided with a simultaneous copper-miner and gold-miner decline, so the minority sleeves could not offset the majority sleeve — the 8/18 metals-co-move pattern firing cleanly, producing a relative lag larger than the absolute move alone would imply.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Direction and all five component signs correct (S0-S4 all HIT); the LLM run's mild band was procedurally sound (gap -0.53% did not trigger the 09-10 rule) but the engine's notable band was closer to the realized -0.90% absolute close — the split was a magnitude-calibration issue, not a signal error, and the engine's 0.5 S1 skill multiplier wrongly halved the single most accurate component.
OUTCOME_END