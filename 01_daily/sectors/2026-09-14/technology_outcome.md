# Sector Outcome — Technology — 2026-09-14

Actuals: {'etf': 'XLK', 'pct': -1.8063619239750195, 'spy_pct': -0.446162221482016, 'rel': -1.3601997024930035, 'open': 183.0, 'close': 184.27999877929688, 'source': 'yf_download'}

# Sector Post-Session Review — Technology (XLK) — 2026-09-14

## 0. FACTS

**Channel 1 (deterministic actuals):**

| Metric | Value |
|---|---|
| XLK % change | **−1.806%** |
| SPY % change | **−0.446%** |
| Relative (XLK − SPY) | **−1.360%** |
| XLK open | 183.00 |
| XLK close | 184.28 |

**Path (from search, corroborating):** XLK opened at 183.00 — already gapped down hard from the prior close of 187.67 (a −2.49% gap on the open). It then **recovered** through the session to close at 184.28, i.e. **+0.70% off the open**. So the day was: gap-down open → intraday grind higher → still a notable net loss. The morning premarket print of −1.95% was *worse* than the eventual close of −1.81%, and the open (−2.49% vs prior close) was the session low. This is a **fade-the-gap-up day**, not a trend-down day.

**Index context (search):**
- CLAIM: Nasdaq Composite fell ~1.17–1.3%; S&P 500 fell ~0.75%; Dow slipped ~0.28%.
- URL: https://www.thestreet.com/stock-market-today/stock-market-today-dow-jones-sp-500-nasdaq-updates-sept-14-2026
- PUBLISHED: 2026-09-14
- QUOTE: "The S&P 500 fell 0.75%, the Dow Jones Industrial Average slipped 0.28% and the Nasdaq Composite lost 1.17%"
- SUMMARY: Tech-led decline, but broad indices pared losses into the close.

- CLAIM: Losses were pared as the 10-year Treasury yield reversed after touching 5%.
- URL: https://www.investopedia.com/stock-market-today-dow-jones-s-and-p-500-09142026-12120379
- PUBLISHED: 2026-09-14
- QUOTE: "Major U.S. stock indexes pared tech-led losses Monday as the 10-year Treasury yield reversed course after touching 5% for the first time in..."
- SUMMARY: The intraday recovery in XLK is directly attributable to the 10Y yield backing off the 5% level — a rates-driven bounce, not a tech-fundamentals-driven one.

- CLAIM: An "AI warning" spooked tech traders.
- URL: https://finance.yahoo.com/markets/live/stock-market-today-monday-september-14-dow-sp-500-nasdaq-080559558.html
- PUBLISHED: 2026-09-14
- QUOTE: "Stock market today: Dow, S&P 500, Nasdaq pare losses after AI warning spooks tech traders, 10-year yield hits 5%."
- SUMMARY: A named AI-related warning was the proximate tech-specific catalyst, layered on the oil/yields macro.

**Direction:** down. **Magnitude:** notable (XLK −1.81%, rel −1.36% — well outside flat, short of severe).

---

## 1. WHAT DROVE THE SECTOR

Taxonomy-aligned decomposition:

**(a) Shared macro — the dominant driver, and it was real.** The re-escalating oil/supply shock (WTI >$102, Brent >$107) plus the hawkish Warsh repricing pushed the 10Y to **5.00%** intraday. That is the single largest input. XLK is the longest-duration sector in the S&P, so a 5% 10Y print is a direct multiple-compression event. This is the 08-10 Hormuz configuration firing exactly as the morning read said it would.

**(b) Sector-specific — the AI warning.** The Yahoo headline names an "AI warning" as the tech-specific spook. This is the live same-session negative that the morning read did *not* have in hand (it had AAPL PT cut, ASML PT cut, APH −6.5%, Kospi −3.26% — all consistent, but the AI-warning headline is the sharper version of the same transmission). The Kospi −3.26% memory/semi tell was the correct leading indicator; the AI warning is the US-session realization of it.

**(c) Rates reversal → intraday recovery.** The 10Y touching 5% and then reversing is the mechanical cause of the +0.70% open-to-close recovery. This is important: **the sector's own fundamentals did not improve intraday; the discount rate stopped rising.** That is a fragile, macro-dependent bounce.

**(d) Breadth/leadership.** XLK underperformed SPY by 1.36% — the worst-relative major sector on a down day. High-beta hardware led down, exactly as S2 predicted.

**(e) Flows/positioning.** The crowded-long semi complex (JPMorgan crowding ~99%) unwound. The 1w rel +2.06% leadership was, as the morning read argued, unwind fuel rather than a shield. Confirmed.

---

## 2. AUDIT OF MORNING S0–S4 READS

**S0_SHARED_MACRO = −2 (full weight).** **HIT.** Oil spiking, real yields up, VIX backwardation, NQ −1.60% — all present, all fired. The 09-10 crowded-long-fuel precondition was correctly judged *re-inverted to present* (not zeroed). The 09-11 inversion rule was correctly *not* applied. This was the highest-conviction, best-calibrated read of the morning. The 10Y hitting 5% is the confirmation.

**S1_SECTOR_FACTORS = −1.** **HIT, arguably understated.** The morning read deliberately held S1 at −1 (not −2) on the reasoning that "the spine is structurally intact, not a kill." That was defensible ex ante, but the actual AI-warning headline plus Kospi −3.26% plus APH −6.5% plus dual mega-cap PT cuts (AAPL, ASML) is a fuller sector-specific negative than −1 implies. The read *named every one of these* but chose not to aggregate them to −2. In hindsight S1 = −2 was available. This is the one place the morning read left points on the table — but note it left them on the table in the *conservative* direction (it did not over-claim), which is the right error to make.

**S2_BREADTH = −1.** **HIT.** XLK −1.81% vs SPY −0.45%, rel −1.36%. High-beta hardware led down. The "not ETF-up/names-flat" call was correct — this was broad, leadership-led weakness.

**S3_FLOWS_POSITIONING = −1.** **HIT.** Crowded long unwound. Note the pipeline applied a **skill multiplier of 0.0** to S3 (zeroing its contribution to the deterministic score) while the LLM overlay kept it at −1. The outcome validates the −1 read; the 0.0 multiplier was, on this day, a mild drag on the deterministic engine's accuracy. Worth flagging for the multiplier audit.

**S4_ETF_TAPE = −1.** **HIT.** Premarket −1.95% → close −1.81%. The tape leg was accurate and slightly conservative (actual was marginally better than premarket).

**Magnitude band: predicted SEVERE, actual NOTABLE.** **MISS.** This is the material error. The morning read's own self-audit explicitly reasoned: *"S1 = −1 (not −2) — the spine is structurally intact, not a kill. So severe is not fully justified; down/notable is the right band."* The narrative reached **notable**. The deterministic engine then output **severe** (total_score −19.586, confidence 0.85). **The engine overrode the analyst's own correctly-reasoned magnitude conclusion.** The analyst was right; the engine was wrong. This is the single most important finding of the review.

**Direction: predicted down, actual down.** **HIT.**

---

## 3. INTERACTIONS / DOUBLE-COUNT / KNOWABLE-AT-OPEN

**Double-count check.** The morning read was disciplined here: it explicitly refused to count hyperscaler capex + foundry + HBM as three spines (counted once, as stale-positive), and it counted the shared macro regime once. The S0 = −2 / S1 = −1 / S2 = −1 / S3 = −1 / S4 = −1 structure does not obviously double-count — S0 is the macro regime, S1 is the live sector transmission, S2 is breadth, S3 is positioning, S4 is the tape. These are genuinely distinct axes. **No material double-count found.**

**However — a soft interaction.** S0 (macro: oil + yields) and S1 (sector: AI warning, Kospi, PT cuts) are *causally linked*: the AI warning and the semi selloff are partly the same risk-off impulse that drives the macro leg. The read treated them as additive (−2 + −1 = −3), but they share a common root. If anything this argues the *combined* negative was slightly overstated, which is consistent with the actual coming in at notable rather than severe. The interaction was directionally benign but the additive treatment contributed to the magnitude overshoot.

**Knowable-at-open test.** The direction was fully knowable at open: NQ −1.60%, XLK premarket −1.95%, oil spiking, 10Y pressing 5%, Kospi −3.26%. **Direction: knowable = yes.** The magnitude was *partially* knowable: the gap-down open was visible, but the **intraday rates reversal** (10Y touching 5% then backing off) was not knowable at open and is precisely what converted a severe-looking day into a notable one. **Magnitude: knowable = partially.** The morning read's narrative correctly identified this uncertainty (it flagged the multi-timeframe relative leadership as the one offset); the engine's 0.85 confidence did not.

**The critical asymmetry:** the analyst wrote "notable," the engine wrote "severe," and the analyst was right. The engine's tape_anchor (−10.836, driven by PM:XLK −1.95% and NQ −1.59%) mechanically extrapolated the premarket gap to the close. But premarket gaps in high-beta tech on rates-driven risk-off days **frequently fade** when the rates impulse reverses — which is exactly what happened. The anchor over-weighted the premarket print.

---

## 4. OUTLIERS INSIDE THE SECTOR

- **The gap-and-fade itself is the outlier.** XLK opened −2.49% vs prior close and closed −1.81%, a +0.70% open-to-close recovery. On a day when the 10Y hit 5%, the long-duration sector *outperformed its own open*. That is a rates-reversal artifact, not a tech-strength signal.
- **ALAB +12% on S&P 500 inclusion speculation** — a single-name mechanical positive, correctly excluded from the sector call. It did not move the ETF.
- **APH −6.5%** (Fabrinet weakness + rising yields) — the worst large-cap hardware name, consistent with the AI-warning transmission.
- **AAPL PT cut (BofA, $380→$370)** and **ASML PT cut (MS, €1,700)** — both named per the 09-09 rule, both market-negative, both consistent with the outcome. Neither was a sector driver on its own, as the read correctly judged.
- **Kospi −3.26%** — the single best leading indicator in the morning packet. It telegraphed the US semi weakness with high fidelity.

---

## 5. VERDICT

The morning read was **directionally correct and analytically disciplined**, and its *narrative* magnitude call (notable) was **exactly right**. The failure was mechanical: the deterministic engine escalated to **severe** and stamped 0.85 confidence, overriding the analyst's own correctly-reasoned "severe is not fully justified" conclusion. The premarket tape anchor over-extrapolated a gap that the intraday rates reversal then faded.

**Actionable for the engine:** when the analyst's narrative magnitude band and the deterministic band disagree, and the analyst's reasoning explicitly cites a *known offset* (here: multi-timeframe relative leadership + the possibility of a rates reversal), the engine should not override to the more extreme band at high confidence. The 09-10 "unwind fuel not a shield" lesson was correct for *direction* but was over-applied to *magnitude*.

OUTCOME_BEGIN
SECTOR: Technology
ETF: XLK
ETF_PCT: -1.806
SPY_PCT: -0.446
REL_PCT: -1.360
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Re-escalating oil/supply shock + hawkish Warsh repricing drove the 10Y to 5.00%, compressing long-duration tech multiples; an "AI warning" headline plus Kospi −3.26% semi weakness supplied the sector-specific leg.
KEY_INTERACTION: S0 (macro: oil/yields) and S1 (sector: AI warning/semi selloff) share a common risk-off root but were scored additively; the 10Y touching 5% then reversing caused the +0.70% open-to-close fade that converted severe into notable.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Direction HIT and narrative magnitude (notable) HIT — but the deterministic engine escalated to severe at 0.85 confidence, overriding the analyst's own correctly-reasoned "severe not justified" conclusion; the premarket tape anchor over-extrapolated a gap that the intraday rates reversal faded.
OUTCOME_END