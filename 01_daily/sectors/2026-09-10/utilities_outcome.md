# Sector Outcome — Utilities — 2026-09-10

Actuals: {'etf': 'XLU', 'pct': -0.9781047563519718, 'spy_pct': -0.5994238166152965, 'rel': -0.37868093973667527, 'open': 42.9900016784668, 'close': 42.52000045776367, 'source': 'yf_download'}

# Sector Post-Session Review — Utilities (XLU) — 2026-09-10

## 0. FACTS

| Item | Value |
|---|---|
| XLU % | **−0.978%** |
| SPY % | **−0.599%** |
| Relative % | **−0.379%** |
| Open | 42.99 |
| Close | 42.52 |
| Actual direction | **down** |
| Actual magnitude | **mild** (sub-1%, no gap-and-trend) |
| Predicted | down / mild, total_score −4.725, conf 0.55, regime risk_off |

Path: XLU opened at 42.99 and closed at 42.52 — a **monotone-ish grind lower**, no opening gap to fade, no intraday reversal. The whole session was a slow bleed, which is the signature of a **rates/duration tape**, not a headline shock. Note the premarket quote in the search results: XLU was indicated **+0.72% at $43.25 before hours** (MarketWatch, Sep 10 2026 5:10 a.m. EDT). So the ETF gave up a positive premarket indication and sold off through the day — the fade happened *after* the open, i.e. it was driven by the session's live rates event, not by anything knowable at 09:30.

**Direction: HIT. Magnitude: HIT (mild). Relative: MISS — XLU underperformed SPY by 38 bp, whereas the morning read explicitly allowed for relative resilience.**

That last point is the single most important grading fact. The morning call was *"down/mild absolute with possible relative resilience."* The absolute half was right; the relative half was wrong. XLU did not get a defensive bid — it was sold *harder* than the index.

---

## 1. What actually drove the sector

**Primary driver: a failed long-end auction that pushed yields sharply higher, hitting the bond-proxy complex directly.**

Evidence:

> CLAIM: The 10-year Treasury yield rose to 4.96% on September 10, 2026, up 0.12 percentage points (12 bp) from the previous session.
> URL: https://tradingeconomics.com/united-states/government-bond-yield
> PUBLISHED: 2026-09-10
> QUOTE: "The yield on US 10 Year Note Bond Yield rose to 4.96% on September 10, 2026, marking a 0.12 percentage points increase from the previous session."
> SUMMARY: A 12 bp single-session jump in the 10Y — from ~4.80% (the morning's live level) to 4.96% — is a large one-day duration shock and lands squarely on utilities.

> CLAIM: Treasury yields surged after a poor 30-year auction, and a new buyback operation failed to calm the market.
> URL: https://www.morningstar.com/news/marketwatch/20260910191/treasury-yields-surge-after-poor-30-year-auction-and-new-buyback-operation-fails-to-calm-market
> PUBLISHED: 2026-09-10 11:37 ET
> QUOTE: "Treasury yields surge after poor 30-year auction and new buyback operation fails to calm market."
> SUMMARY: The live supply event was the long-end auction, and it went badly. This is the mechanism that turned a mildly risk-off, mixed-futures open into a rates-led selloff in bond proxies.

> CLAIM: The 30-year bond auction showed coverage of 2.39, a low-end result, with the high yield awarded at 5.216%.
> URL: https://www.cmegroup.com/education/events/econoday/692701
> PUBLISHED: 2026-09-10
> QUOTE: "30-year bond auction show coverage at 2.39, a low-end result. The high yield was awarded at 5.216 percent."
> SUMMARY: Weak coverage at the long end = the market demanding more term premium. Utilities, as the longest-duration equity sector, are the cleanest short in that regime.

Taxonomy mapping (rubric-aligned):

- **Rates rising (bond-proxy selloff): HIT — dominant.** The morning grid already scored this HIT at 0.75. It was correct and it was the engine.
- **Real yields rising: HIT.** Consistent with a term-premium/supply shock rather than a growth-driven nominal move.
- **Risk-off tape / flight to safety: MISS in the form that mattered.** This is the key taxonomy correction. The morning scored it PARTIAL (0.5) on the theory that a defensive bid would give XLU *relative* support. It did not. In a **rates-led** risk-off, utilities are not the safe haven — they are the funding source. Cash and front-end bills are the haven; long-duration equity proxies get sold.
- **Data-center load growth / power demand: STALE, correctly ignored.** No same-session order. Correct call.
- **Favorable rate case (Duke Energy Florida): correctly not promoted.** Single-name, and it did not move the ETF. The 08-28 rule held.

So the driver decomposition is clean: **one shock — the long-end supply/term-premium shock — transmitted through the duration channel into XLU.** No idiosyncratic utility news, no regulatory smash, no AI-power headline. Pure macro-duration.

---

## 2. Audit of morning S0–S4 reads against reality

**S0_SHARED_MACRO = −1. Verdict: CORRECT, and arguably under-weighted.**
The morning identified the live oil shock (Brent $102.08) → inflation expectations → long-end up → duration headwind. That chain was right in direction. What the morning did *not* anticipate was the **magnitude of the long-end move** — it had 10Y at 4.80% and treated the auction as "a supply event, not a scored binary." In reality the auction was the day's dominant price-setting event and took the 10Y to 4.96%. The morning's own note — *"10Y Treasury auction is the live supply event (can push the long end)"* — was the correct flag, but it was filed as a non-scored caveat rather than as the primary risk. That is a **calibration miss, not a directional miss**.

**S1_SECTOR_FACTORS = −1. Verdict: CORRECT.**
"Rates rising (bond-proxy selloff)" was the dominant fresh factor and it hit. The morning correctly refused to let the AI-power structural story override a 1d rate tape (08-12 rule), and correctly refused to pay the "rates falling" bid (08-21 rule). The 09-09 lesson — *a static-shock cushion is mean-reversion fuel, not a flat override* — was applied correctly: the oil shock was not escalating, so no flat/up license was granted. Good discipline.

**S2_BREADTH = 0. Verdict: CORRECT, and the reasoning was the best part of the morning note.**
The morning explicitly flagged the tension: 1d rel −0.71% but 3d/1w/1m rel all positive, and ruled that the medium-term relative tape was *confirmation of relative resilience, not an absolute-up signal* (08-13 rule). That was exactly right. The medium-term relative strength did **not** protect XLU today. Breadth was genuinely neutral-to-negative and 0 was the honest score.

**S3_FLOWS_POSITIONING = 0. Verdict: CORRECT / unverifiable.**
No same-day flow evidence either way. Neutral was right. No penalty.

**S4_ETF_TAPE = −0.5. Verdict: CORRECT in sign, but the *interpretation* embedded in the divergence check was wrong.**
The morning used S4 as "mixed-to-negative" and concluded *"no strong divergence — factors and near-term tape agree on a mild-down absolute with possible relative resilience."* The sign was right. The **"possible relative resilience"** clause was the error. The 1d rel print of −0.71% going into the session was the freshest and most informative tape signal, and it was pointing at relative *weakness*, not resilience. The morning let the 3d/1w/1m positive relative tape dilute a 1d signal that was telling the truth.

**Multiplier 0.9 / confidence 0.55. Verdict: reasonable.** The realized move (−0.98%) landed inside the mild band. The 0.9 multiplier did not cost anything material.

**Net S0–S4 audit: 4 of 5 component scores directionally correct; the composite direction and magnitude were both right. The failure was localized to the relative-return sub-call, which was not a scored component but was stated in the prose and in the divergence check.**

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count check: PASS.** The morning counted the oil shock once (S1) and the rates/duration channel once (S0), and explicitly stated the separation. That was the right structure. Today's actual driver — the auction-driven long-end move — is a *distinct* shock from the oil channel, and it arrived intraday. So the morning did not double-count; if anything it **under-counted** by treating the auction as unscored.

**Interaction the morning got wrong: the sign of the "defensive bid" interaction.**
The morning's operative frame was 08-18: *"risk-off + rising long-end → relative beat / flat-to-negative absolute."* Today falsified the "relative beat" half of that frame. The interaction that actually obtained was:

> rising long-end (auction failure) + mildly risk-off tape → **absolute down AND relative down** for XLU.

The reason: when the risk-off is *caused by* the long end, utilities are the transmission channel, not the beneficiary. The 08-18 frame likely worked in a regime where the risk-off was equity-idiosyncratic and rates were rising for growth reasons. Today the risk-off *was* the rates move. That is a meaningful regime distinction the morning did not draw.

**Knowable-at-open test: PARTIALLY.**
- The *direction* was knowable at open: live 10Y 4.80%, 30Y 5.25%, bond futures red, oil $102, VIX backwardated. All pointed down. **Direction was fully knowable.**
- The *magnitude* was knowable at open: mild. Nothing in the premarket panel suggested a severe move. **Magnitude was knowable.**
- The *relative underperformance* was **partially** knowable: the 1d rel −0.71% print was in hand and was the correct tell. The morning had the right data and drew the wrong inference from it by letting longer-horizon relative strength override the freshest signal.
- The *specific catalyst* (auction failure) was **not** knowable at open — but the morning had already identified the auction as the live event and simply declined to score it. That is a process gap, not an information gap.

---

## 4. Outliers inside the sector

No single-name outlier is visible in the ETF-level data, and none is needed to explain the move: a −0.98% XLU session on a 12 bp 10Y jump is fully explained by beta to the long end. The Duke Energy Florida rate-lower filing was correctly assessed as non-driving (08-28 rule held — no promotion of a single-name regulatory item). The IPP/AI-power names (CEG/VST type) were correctly excluded from the ETF call; if anything they would have been a *drag* on a rates day, consistent with the ETF underperforming SPY. No outlier requires a separate explanation.

---

## 5. Lessons for the book

1. **When the risk-off is rates-led, utilities do not get a relative bid — they are the funding source.** The 08-18 "relative beat" frame needs a regime qualifier: it applies when rates rise for *growth* reasons alongside an equity-idiosyncratic risk-off. When the long end is the *cause* of the risk-off (supply/term-premium shock), XLU goes down *and* underperforms. Add this as a conditional branch.
2. **A flagged-but-unscored catalyst is a scoring error waiting to happen.** The morning wrote "10Y Treasury auction is the live supply event (can push the long end)" and then assigned it zero weight. If a same-day event is identified as capable of moving the dominant factor, it belongs in the score, not in a caveat.
3. **The freshest relative-tape print should dominate longer-horizon relative strength for a 1d call.** The 1d rel −0.71% was right; the 3d/1w/1m positive rel was not protective. The 08-13 rule ("S2/S4 confirmation only") was applied to *scores* but not to the *prose conclusion*, which still smuggled in "possible relative resilience."
4. **Direction and magnitude process is working.** Down/mild was correct on both axes, and the 09-09 static-shock-cushion veto was applied correctly. Do not over-correct the core; fix the relative sub-call and the unscored-catalyst gap.

---

OUTCOME_BEGIN
SECTOR: Utilities
ETF: XLU
ETF_PCT: -0.978
SPY_PCT: -0.599
REL_PCT: -0.379
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Failed 30Y auction (coverage 2.39, high yield 5.216%) drove 10Y from ~4.80% to 4.96% (+12bp), a duration shock to the bond-proxy complex
KEY_INTERACTION: Rates-led risk-off — XLU was the transmission channel, not the haven, so it fell AND underperformed SPY instead of getting the expected defensive relative bid
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Direction and magnitude correct (down/mild); relative-resilience clause wrong — XLU underperformed by 38bp, and the flagged-but-unscored 10Y auction was the actual driver
OUTCOME_END