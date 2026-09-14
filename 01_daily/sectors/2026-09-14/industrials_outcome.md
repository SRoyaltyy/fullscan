# Sector Outcome — Industrials — 2026-09-14

Actuals: {'etf': 'XLI', 'pct': -1.4155610086009407, 'spy_pct': -0.446162221482016, 'rel': -0.9693987871189247, 'open': 168.6699981689453, 'close': 169.92999267578125, 'source': 'yf_download'}

# Sector Post-Session Review — Industrials (XLI) — 2026-09-14

## 0. FACTS

**Channel 1 actuals (deterministic):**
- XLI: **−1.4156%** (open 168.67 → close 169.93; note the ETF opened *below* the prior close and closed *above* its open — the loss was largely a gap-down that partially recovered intraday)
- SPY: **−0.4462%**
- Relative: **−0.9694%** (XLI underperformed SPY by ~97bp)
- Actual direction: **down**; actual magnitude: **notable** (≈1.4% single-session decline, ~2.2× SPY's move, and a ~1.0% relative underperformance)

**Cross-checks from search:**
- CLAIM: XLI closed ~$169.81–170.08 on 09/14/2026, down ~1.33% on the day. URL: https://markets.businessinsider.com/etfs/state-street-industrial-select-sector-spdr-etf-us81369y7040 ; https://stockscan.io/stocks/XLI/price-history. PUBLISHED: 2026-09-14. QUOTE: "170.08 −2.29 −1.33% 09/14/2026." SUMMARY: Confirms a notable down day for XLI, consistent with the deterministic −1.42% (small vendor differences in close/print).
- CLAIM: "Tech and industrials are particularly hard-hit" on 09/14/2026; S&P 500 −0.65%, only 233 holdings advancing. URL: https://www.thestreet.com/stock-market-today/stock-market-today-dow-jones-sp-500-nasdaq-updates-sept-14-2026. PUBLISHED: 2026-09-14. QUOTE: "The S&P 500 (−0.65%) is off over half a percentage point... Tech and industrials are particularly hard-hit after a turbulent weekend of AI and energy market news." SUMMARY: Industrials were explicitly named as a hard-hit sector — a sector-specific, not merely beta, decline.
- CLAIM: "The decline was led by Caterpillar." URL: https://tradingeconomics.com/united-states/stock-market. PUBLISHED: 2026-09-14. QUOTE: "The decline was led by Caterpillar." SUMMARY: The single largest XLI constituent (CAT) led the index lower — a concentrated, name-level driver inside the sector.
- CLAIM: 10Y Treasury yield hit 5% (highest since 2023); oil prices rose. URL: https://www.investopedia.com/stock-market-today-dow-jones-s-and-p-500-09142026-12120379. PUBLISHED: 2026-09-14. QUOTE: "Chip Stocks Drop, Software Jumps as Indexes Close Lower; Oil Prices Rise, 10-Year Yield Hits 5%." SUMMARY: The morning's two macro theses — rising real/long yields and an oil spike — both materialized and persisted into the close.
- CLAIM: Indexes closed lower but well off worst levels; Nasdaq dropped >1% intraday. URL: https://investrade.com/market-review-september-14-2026/. PUBLISHED: 2026-09-14. QUOTE: "U.S. stocks finish lower, but well off their worst levels with AI names/semis hitting the Nasdaq." SUMMARY: The session was a risk-off day that faded into the close — consistent with XLI's gap-down-then-recover path.

**Path:** XLI gapped down (open 168.67 vs prior close ~172.37 per Clearank) and recovered ~0.75% off the open into the close, but never reclaimed the prior close. The damage was front-loaded; the afternoon was a partial bounce, not a further slide.

---

## 1. What drove the sector

The morning thesis was directionally correct and the taxonomy it used was largely the right one. The drivers, in order of explanatory power:

**(a) Rates / real-yield backup — the dominant shared macro driver.** The 10Y hit 5% intraday (highest since 2023), and the morning's DFII10 +0.09 1d / +0.12 1m read was the persistent-real-yield story. Industrials are long-duration cyclicals with heavy capex financing needs; a 5-handle 10Y is a direct multiple and cost-of-capital headwind. This was scored in S0 and it paid.

**(b) Oil supply shock — the sector-specific cost channel.** WTI +2.4% / Brent +2.8% on a live supply shock is a direct fuel-cost and input-cost headwind for the transport/manufacturer sleeve. This was the clearest *sector-specific* live negative in the morning read, and it transmitted.

**(c) Caterpillar-led single-name drag.** Trading Economics explicitly attributes the index decline to CAT. This is the most important *new* information relative to the morning read: the morning self-audit asserted "no single ticker carries it" and "GEV/ETN/AME do not drive the ETF call." That was wrong in outcome — the largest constituent led the decline. The morning read treated the sector as a diffuse macro transmission; in reality it was partly a concentrated CAT move.

**(d) Industrial-metals fade.** Copper −1.44%, aluminum −1.91% — the morning read flagged this as a global-growth/industrial-demand negative read-through to machinery/electrical-equipment. That channel was live and consistent with the CAT-led decline.

**(e) Tech/growth-led risk-off (NQ −1.59% premarket).** The morning read correctly noted this was a tech-led selloff, "not a broad cyclical smash." The outcome partially vindicates that: XLI's −1.42% was worse than SPY's −0.45%, so it was *not* merely beta — but the tech-led framing understated how much industrials would be hit as a second-order duration/cyclical casualty.

**Taxonomy alignment:** The morning's factor taxonomy (risk-off tape, real yields rising, USD strengthening, construction slowdown, ISM expansion, rotation out of industrials) was the right lens. The miss was not in *which* factors — it was in *how much* they would bite, and in the failure to anticipate the CAT concentration.

---

## 2. Audit of morning S0–S4 reads against reality

**S0 = −1 (shared macro): CORRECT SIGN, UNDERSTATED MAGNITUDE.** The morning read called risk-off, oil-spiking, hawkish-repricing, real yields rising, VIX backwardation, EPU spike. All of that materialized and persisted (10Y hit 5%, oil rose, indexes closed lower). The morning self-audit explicitly declined to go to −2, citing "VIX is not a panic print, credit is tight, 10Y–SPX corr only −0.248." That reasoning was defensible at the open but the −1 was too light given the *persistence* of the real-yield backup across 1d/1w/1m that the morning read itself flagged. The read identified the right driver and then discounted it.

**S1 = −1 (sector factors, capped): CORRECT SIGN, CORRECT CHANNEL, WRONG ON CONCENTRATION.** The fuel-cost/metals transmission was the right sector-specific negative and it paid. But the morning read's insistence that "no single ticker carries it" and that GEV/ETN/AME don't drive the call was falsified by CAT leading the decline. The cap at −1 (rather than −2) was justified by the "don't over-stack one shock" discipline, but the sector-specific channel was stronger than −1 implied.

**S2 = 0 (breadth): DEFENSIBLE, SLIGHTLY LIGHT.** The morning read scored the laggard once (in S4) per the 09-04 laggard-shield rule and cited no fresh same-day breadth data. The outcome — XLI underperforming SPY by ~97bp with only 233 S&P holdings advancing — suggests breadth was worse than "0" implied, but the morning read had no same-day breadth print to work with, so scoring it 0 was procedurally correct. The *information* was knowable at the open only via the premarket XLI −1.13% vs ES −0.66% gap, which the morning read *did* note but did not convert into a breadth/relative score.

**S3 = 0 (flows): CORRECT.** No flow data; not a crowded long; rotation out of industrials. The outcome (industrials hard-hit) is consistent with continued rotation *out*, but there was no knowable flow signal at the open. 0 was right.

**S4 = 0 (ETF tape): THE KEY ERROR.** The morning read scored S4 = 0, explicitly reasoning that the freshest 1d rel print (+0.21%) was *positive* and therefore the tape did *not* confirm a down move, invoking the 09-10 decay rule (deep-oversold laggard → prior-day 1d rel is a decaying signal). This was the single most consequential misjudgment. The 1m rel of −6.21% was a *persistent* medium-term laggard signal, and the morning read let a single positive 1d print (which the 09-10 rule itself says to *discount*) talk it out of scoring the laggard. The laggard fact was real and it reasserted. S4 should have been −1, not 0.

**Net audit:** S0 sign right / magnitude light; S1 sign right / concentration wrong; S2 defensible; S3 right; S4 wrong (should have been −1). The directional call was correct; the magnitude call was too light; and the confidence was too low relative to the strength of the negative factors.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count check:** The morning read was disciplined here — oil counted once in S0, hawkish repricing once in S0, laggard once in S4. No double-counting error. The problem was the opposite: the laggard was scored *zero* times (S4 = 0), not twice.

**Interaction the morning read missed:** The oil spike and the real-yield backup are not independent for industrials — they compound. Higher oil raises input costs *and* higher real yields raise the discount rate on the same capex-heavy cash flows. The morning read treated them as two separate −1s in S0 and S1 but did not recognize that their *interaction* on a long-duration cyclical is super-additive. That interaction is the most likely explanation for why XLI (−1.42%) so badly lagged SPY (−0.45%).

**Knowable-at-open test:** The decisive inputs were all knowable at the open:
- XLI premarket −1.13% vs ES −0.66% — the sector was *already* gapping down harder than the index. The morning read noted this number but did not let it drive the relative/magnitude call.
- Oil +2.4–2.8% — knowable, and the morning read correctly identified it as a direct cost headwind.
- 10Y/DFII10 rising across 1d/1w/1m — knowable, and the morning read correctly identified the persistence.
- CAT is the largest XLI constituent — knowable, and the morning read explicitly dismissed single-name concentration.

So the *information* to call down/notable was available at the open. The morning read had it and chose down/mild with confidence 0.52. **Knowable_at_open: yes** (for direction and for a notable-magnitude call).

**The binding DO-INSTEAD rule was misapplied.** The morning read invoked "when score fights tape, cut conviction / prefer flat/mild" because the 1d rel was +0.21%. But the 09-10 decay rule — which the morning read itself cited — says a deep-oversold laggard's prior-day 1d rel is a *decaying* signal, i.e., it should be *discounted*, not used as a reason to cut conviction. The morning read used the decaying signal as a dampener, which is exactly backwards. This is the cleanest process error of the session.

---

## 4. Outliers inside the sector

- **Caterpillar (CAT):** The dominant outlier and the single-name driver of the index decline (Trading Economics: "The decline was led by Caterpillar"). The morning read's "no single ticker carries it" was falsified. CAT is the largest XLI weight and its move was the concentrated expression of the rates + metals + capex-duration thesis.
- **AI-power complex (GEV/ETN):** The morning read warned "do not use GEV/ETN as a floor" and that warning was correct — the AI-power complex sold with the tech tape (NQ −1.59% premarket, XLK −1.95%). The morning read's refusal to treat GEV/ETN as a cushion was a *good* call.
- **Aerospace & defense:** The morning read called this MIXED and declined to let a stale defense narrative cancel the oil headwind. No evidence of a defense-led offset in the outcome; the MIXED call was reasonable.
- **Freight/trucking/rail:** The morning read called this the "clearest live negative" via fuel costs. Consistent with the outcome; no evidence of a freight-led offset.

The outlier structure confirms the morning read's *sector-internal* taxonomy was sound — it just under-weighted the largest constituent and over-weighted the diversifying effect of the rest of the book.

---

## 5. Verdict

The morning call was **directionally correct (down)** but **magnitude-light (mild vs actual notable)** and **confidence-too-low (0.52)**. The pipeline's deterministic engine actually called **down/notable with confidence 0.85** — and the pipeline was *right* on magnitude while the LLM overlay talked it down to mild. This is the mirror image of the 09-09/09-08 flattening errors: here the LLM overlay *under*-called a correct negative signal, using a decaying 1d rel print as a dampener when the rule it cited said to discount that print.

The two process errors:
1. **S4 = 0 instead of −1** — the persistent 1m laggard (−6.21%) was real and reasserted; the positive 1d rel was a decaying signal that should have been discounted, not used to zero out the laggard score.
2. **Magnitude band capped at mild** — the premarket XLI −1.13% vs ES −0.66% gap, the persistent real-yield backup, and the oil spike's interaction with duration on a capex-heavy cyclical all pointed to notable, not mild.

The morning read's *factor identification* was excellent — it named the right drivers (rates, oil, metals, rotation, laggard) and correctly refused to use GEV/ETN as a floor. The failure was in *calibration*: it identified strong negatives and then systematically discounted them.

OUTCOME_BEGIN
SECTOR: Industrials
ETF: XLI
ETF_PCT: -1.4156
SPY_PCT: -0.4462
REL_PCT: -0.9694
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Persistent real-yield backup (10Y to 5%) + oil supply shock compounding on a capex-heavy cyclical, with Caterpillar leading the index decline
KEY_INTERACTION: Oil cost shock and real-yield backup are super-additive on long-duration industrials — the morning read scored them as two separate −1s and missed the compounding, which explains XLI's ~97bp underperformance vs SPY
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Direction correct (down), magnitude under-called (mild vs actual notable) and confidence too low (0.52) — S4 wrongly zeroed the persistent 1m laggard by treating a decaying 1d rel print as a dampener, and the premarket XLI −1.13% vs ES −0.66% gap was noted but not converted into the relative/magnitude call
OUTCOME_END