# Sector Outcome — Consumer Cyclical — 2026-09-21

Actuals: {'etf': 'XLY', 'pct': 1.062776112896624, 'spy_pct': 1.5518133737258744, 'rel': -0.48903726082925036, 'open': 111.54000091552734, 'close': 112.20999908447266, 'source': 'yf_download'}

# Sector Post-Session Review — Consumer Cyclical (XLY) — 2026-09-21

## 0. FACTS

**CLAIM:** XLY closed at $112.21, +1.06% on the session, from an open of $111.54 and a prior close of $111.03.
**URL:** https://www.marketwatch.com/investing/fund/xly (and stockscan.io price history)
**PUBLISHED:** 2026-09-21
**QUOTE:** "the latest closing stock price as of September 21, 2026, is $112.18" (intraday snapshot; deterministic feed gives $112.21)
**SUMMARY:** XLY's own tape was a solid up day — the ETF gained just over 1%.

**CLAIM:** SPY gained ~1.55% on the session; the S&P 500 climbed ~1.5% and the Nasdaq gained ~2.3% for its first record close since June.
**URL:** https://www.cnbc.com/2026/09/20/stock-market-today-live-updates.html · https://finance.yahoo.com/markets/live/stock-market-today-monday-september-21-dow-sp-500-nasdaq-080214605.html
**PUBLISHED:** 2026-09-21
**QUOTE:** "The S&P 500 climbed 1.5%, while the Nasdaq Composite gained 2.3% for its first record close since June. The Dow Jones Industrial Average added 368 points, or 0.7%."
**SUMMARY:** A broad risk-on session led by tech/growth; the Dow lagged (+0.6–0.7%).

**Deterministic actuals (authoritative):**
- XLY: **+1.0628%** (open 111.54 → close 112.21)
- SPY: **+1.5518%**
- **Relative: −0.4890%** (XLY lagged SPY by ~49 bp)
- Path: opened +0.46% above prior close, closed +1.06% — a **gap-up that extended modestly**, not a fade.

**ACTUAL_DIRECTION:** up. **ACTUAL_MAGNITUDE:** notable in absolute terms (+1.06%), but **mild-to-notable underperformance** relative to SPY.

---

## 1. What drove the sector today

The dominant driver was **shared macro / equity beta**, not sector-specific consumer news. The session was a broad risk-on melt-up: S&P +1.5%, Nasdaq +2.3% to a record, Dow +0.7%. XLY's +1.06% is best explained as **beta participation with a lag**, consistent with its mega-cap growth sleeve (AMZN ~23–25%, TSLA ~17–20%) catching part of the tech-led bid while its broad-book holdings (retail, apparel, autos, restaurants) dragged.

Taxonomy-aligned factors that fired:
- **Risk-on tape / equity beta expansion — PARTIAL (0.45).** XLY rose, but underperformed the index it trades against. Beta was present but incomplete.
- **Large-cap leadership inside sector — HIT (0.60).** The mega-cap sleeve (AMZN/TSLA) was the engine; the equal-weight broad book lagged.
- **Real yields falling — HIT (0.70).** DFII10 −7 bp 1d was a genuine duration tailwind for the growth sleeve, and it showed up.
- **Sector rotation out of discretionary — HIT (0.70), structural.** The 1m rel −5.66% laggard status persisted; today's −0.49% rel is a continuation of that pattern, not a break.

The oil-relief thesis (WTI −1.59%, RBOB −0.54%) was **directionally correct as a consumer tailwind** but did not translate into XLY outperformance — it helped the tape broadly (energy down, everything else up) rather than XLY specifically.

---

## 2. Audit of morning S0–S4 reads against reality

**S0_SHARED_MACRO = 0 → verdict: MISS (should have been +1).**
This is the central error. The morning card correctly identified: oil offered, real yields dipping 1d, VIX low/contango, Asia +1.04%, Europe +0.95%, Finviz futures modestly green, XLK leading PM. It then **refused to sign S0 positive** because (a) Finviz ES +0.20% was inside ±0.5%, (b) the 08-27 NVDA/XLK-map "ban on S0=+1" fired, and (c) Friday's 1d rel was negative. The result: a flat call on a day SPY rose 1.55%.

The 08-27 ban was **misapplied**. That lesson exists to stop mapping a *non-holdings* tech impulse (ASML/AI-capex) into a consumer-cyclical +1. But today's driver was not ASML — it was a **broad, index-wide risk-on session** (Nasdaq record close, S&P +1.5%) in which XLY's own mega-cap holdings (AMZN, TSLA) were green premarket. The ban should have blocked *sector-specific* +1 attribution, not *shared-macro* +1. The morning conflated the two.

The "Finviz < +0.5%" gate also failed as a filter: the discarded yfinance anchor (ES +1.35% / NQ +2.12%) was **closer to the truth** than the Finviz snapshot the card chose to trust. The card explicitly noted the "discarded-anchor split" and picked the wrong side.

**S1_SECTOR_FACTORS = 0 → verdict: PARTIAL MISS (should have been +1).**
The card netted spend/auto/travel/claims positives against carried confidence/credit negatives and nested MAP HEAT weakness. But it **under-weighted the live positives**: August retail sales +1.2% (control +1.4%, 12/13 categories), claims 196k, SAAR 16.8M, real-yield relief. The nested-heat negatives (footwear, apparel, dept stores, gambling) were **stale color**, not same-session prints. On a risk-on day, carried negatives should not have fully offset live positives. Net should have been +1.

**S2_BREADTH = 0 → verdict: CORRECT in sign, but the card drew the wrong conclusion.**
The card correctly read "quality/mega-cap bid, not healthy cyclical breadth." That was right — and it was exactly the reason XLY would **underperform** SPY. The card used this to justify flat, but the correct inference was: *mega-cap-led up day → XLY rises but lags SPY*. The breadth read was accurate; its implication was mis-signed.

**S3_FLOWS_POSITIONING = 0 → verdict: CORRECT.**
No fresh flow print; the +$615M 09-17 inflow was properly treated as stale. No error.

**S4_ETF_TAPE = 0 → verdict: CORRECT (with a caveat).**
Friday's −0.45% 1d rel was correctly not restacked. The card's instinct that "live PM green does not confirm Friday's lag" was right — XLY did rise. But the card failed to note that **live PM green also did not confirm flat**; it was a mild positive signal that got zeroed out.

**Aggregate:** The card produced an all-zero leading sum on a day the sector rose >1%. The **direction was wrong** (flat vs up), and the **relative call was wrong in the opposite way** (the card's structural-lag thesis was right, but it expressed it as flat rather than "up but lagging").

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count check:** The card was disciplined here — oil was kept out of both S0 and S1, FOMC was not restacked, Friday's 1d rel was not copied into S2/S3/S4. No double-count error. The problem was **under-counting**, not over-counting.

**Knowable-at-open test — the decisive question:** Was "XLY up, lagging SPY" knowable at the open?
**YES.** At the open the card had:
- Finviz futures green across ES/NQ/RTY/DJIA
- Asia +1.04%, Europe +0.95%
- VIX 14.98, contango
- Real yields −7 bp 1d
- Oil offered (WTI −1.59%)
- XLY PM +0.12%, AMZN +0.9%, TSLA +1.4–1.6%
- XLK leading PM +0.98%

Every one of these is a **risk-on, up-tape** signal. The card saw them all and chose flat because the *magnitude* gates (Finviz < +0.5%, 08-27 ban) were set too tight. The information was sufficient to call **up/mild with XLY lagging**; the card's own S2 breadth read (mega-cap bid, weak broad book) was the exact template for that call.

**The 09-17 lesson was inverted.** The card cited "09-17 all-zero→mild-up does NOT fire: needs live ES/NQ ≥ +0.5%." But the *spirit* of that lesson is that an all-zero card plus a green tape should not be forced to flat when the tape is clearly directional. Here the tape was directional (Nasdaq record), and the card used the letter of the gate to suppress the signal.

---

## 4. Outliers inside the sector

- **Mega-cap sleeve (AMZN, TSLA):** The clear outperformers and the reason XLY rose at all. TSLA PM +1.4–1.6% and AMZN +0.9% premarket translated into the ETF's gain. This is the "large-cap leadership" HIT.
- **Broad book (retail, apparel, footwear, dept stores, gambling):** The laggards. The nested MAP HEAT (NKE/DECK downgrades, KSS, internet retail mixed) was the drag that produced the −0.49% relative gap. This is the "sector breadth failure / ETF up, names flat" PARTIAL.
- **Auto parts (AZO) up, HI/dealerships flat:** Consistent with the mixed broad book.
- **The relative gap itself (−0.49%) is the outlier of the day:** XLY participated in a +1.5% index day but captured only ~68% of it. That is the signature of a **concentrated, mega-cap-led ETF in a broad rally** — exactly what the morning breadth read described.

---

## 5. Verdict and lessons

**MORNING_READ_VERDICT:** Direction MISS (flat vs +1.06%); the relative-lag thesis was correct but expressed as flat instead of "up, lagging." The all-zero card suppressed a knowable risk-on signal.

**Primary error category:** **Gate over-tightening / lesson misapplication.** The 08-27 XLK-map ban and the Finviz <+0.5% gate were designed for *sector-specific* attribution and *mean-shift* detection respectively; both were applied to a *shared-macro* risk-on session where they did not belong. The card also trusted the weaker of two futures feeds.

**DO-INSTEAD for next time:**
1. When the *index-level* tape is clearly directional (Nasdaq record, S&P +1.5%, Asia/Europe green, VIX contango), **sign S0 positive** even if the single Finviz snapshot is inside ±0.5% — use the cross-asset confirmation, not one number.
2. Distinguish **sector-specific impulse bans** (08-27: don't map ASML into XLY) from **shared-macro risk-on** (which *should* lift XLY via beta). The ban blocks the former, not the latter.
3. When S2 reads "mega-cap bid, weak broad book," the correct output is **"up but lagging SPY"** — not flat. Breadth quality determines *relative*, not *absolute*, direction on a risk-on day.
4. Do not let carried/stale negatives (confidence, credit, nested heat) fully offset **live** positives (retail sales, claims, real-yield relief) on a risk-on session.

**OUTCOME_BEGIN**
SECTOR: Consumer Cyclical
ETF: XLY
ETF_PCT: 1.0628
SPY_PCT: 1.5518
REL_PCT: -0.4890
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Broad risk-on melt-up (Nasdaq record close, S&P +1.5%) lifting XLY's mega-cap growth sleeve (AMZN/TSLA) while the broad book lagged
KEY_INTERACTION: Mega-cap leadership (AMZN/TSLA) drove the absolute gain; weak broad-book breadth (retail/apparel/dept stores) produced the −0.49% relative lag — the exact template the morning S2 read described but mis-signed
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Direction MISS — flat called on a +1.06% day; the relative-lag thesis was correct but should have been expressed as "up, lagging SPY," not flat; 08-27 XLK-ban and Finviz <+0.5% gate were misapplied to a shared-macro risk-on session
OUTCOME_END