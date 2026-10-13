# News Judge — 2026-10-13

### IMPORTANT NEWS (my ranking)

1. **Strait of Hormuz tanker attacked; Iran targeting US-UAE oil shuttle; Houthi missiles hit Riyadh 3rd day** — fresh kinetic oil-supply escalation with energy shipping/insurance costs surging; this is the session's primary macro risk driver. (risk)
2. **Fed minutes: another hike likely this year; officials hold off this month, CPI + Powell comments this week** — unresolved hawkish rates binary gating risk appetite; treat as one rates cluster. (rates)
3. **US yield surge triggers mortgage hedging, adds to bond-market pressure; bank stocks mired in correction before earnings** — yields are the transmission channel hitting duration, financials, small-caps. (rates)
4. **Gold drops >$100 on hawkish Fed comments; Agnico Eagle falls** — rescue: gold flush is a real-rates/risk-signal, hits miners basket, not noise. (rates)
5. **Bank earnings season begins with yields surging into prints (BNY raising prime to 7.00%)** — financials sector catalyst, tests whether NIM beats offset duration pain. (sector_fundamental)
6. **Amgen Phase 3 dazodalibep success in Sjögren's; Jefferies raises target** — large-cap biotech positive readout with XBI/IBB sympathy. (sector_fundamental)
7. **Boeing 737 MAX 10 certification halted by FAA; 31% of undelivered 737 order book at risk** — Industrials sector weight, supply-chain certification risk. (sector_fundamental)
8. **Microsoft deploying gigawatts of in-house AI chips; Nvidia China share eroding to Huawei** — AI capex/substitution pressure on NVDA-adjacent semi basket. (substitution)

Set is not thin: usable macro (oil/Fed) plus FINVIZ elevated digests give enough to rank 8.

**RULES_APPLIED:** fresh-overnight-kinetic-oil-supply-escalation (Hormuz increment — apply B1=−3 / cap magnitude by futures confirmation when pipeline runs); a-fresh-overnight-kinetic-oil-supply-escalation-iran-hormuz (force mild band unless B6 confirms ≥0.5% down). No CPI/FOMC binary directional gate applies yet beyond #2's unresolved status.

---

### STEP 1 — FRAMEWORK SCORES

1. Hormuz/Iran/Houthi escalation
   - keep | us_relevance: high — oil supply shock + shipping/insurance costs hit broad risk appetite
   - channel: risk | geography: us_supply_chain | severity: session | horizon: 1d-1w
   - action_object: spx (SPX beta; secondarily XLE, OIH) | polarity: bearish — kinetic supply disruption, oil shipping costs surging
   - confidence: 0.8

2. Fed hike expectations + CPI/Powell pending
   - keep | us_relevance: high — direct rate-path pricing into binary week
   - channel: rates | geography: us_domestic | severity: session | horizon: 1d
   - action_object: spx | polarity: hawkish — minutes flag another hike; persistence inflation narrative
   - confidence: 0.75

3. Yield surge / mortgage hedging / bank correction
   - keep | us_relevance: high — 10Y backup is the cross-asset driver of the week
   - channel: rates | geography: us_domestic | severity: session | horizon: 1d-1w
   - action_object: sector_etf (XLF; KRE, duration-heavy baskets) | polarity: bearish — hedging flows add mechanical selling pressure
   - confidence: 0.7

4. Gold −$100 on hawkish Fed; AEM down
   - keep (rescued) | us_relevance: medium — real-rates flush, miners basket + risk-sentiment tell
   - channel: rates | geography: global_priced | severity: session | horizon: 1d
   - action_object: basket (GDX/gold miners) | polarity: bearish — hawkish real-rate repricing
   - confidence: 0.7

5. Bank earnings begin / BNY prime +25bp
   - conditional | us_relevance: high — NIM support vs credit/duration worry; earnings gate the sector
   - channel: sector_fundamental | geography: us_domestic | severity: session | horizon: 1d-1w
   - action_object: sector_etf (XLF, KRE) | polarity: mixed — higher prime rate helps margins, yield surge hurts
   - confidence: 0.6

6. Amgen dazodalibep Phase 3 success
   - keep | us_relevance: medium — large-cap biotech readout with sector sympathy
   - channel: sector_fundamental | geography: us_domestic | severity: session | horizon: 1d
   - action_object: basket (IBB/XBI large-cap immunology) | polarity: bullish — positive late-stage data
   - confidence: 0.65

7. Boeing MAX 10 certification halted
   - keep | us_relevance: high — Industrials heavyweight, order-book risk
   - channel: sector_fundamental | geography: us_domestic | severity: session | horizon: 1w-1m
   - action_object: single_name (BA) + XLI sympathy | polarity: bearish — certification delay hits deliveries/cash flow
   - confidence: 0.7

8. Microsoft in-house AI chips at scale / NVDA China erosion
   - single_name (carries substitution force) | us_relevance: medium — custom-silicon substitution pressure on semi basket
   - channel: substitution | geography: us_supply_chain | severity: session | horizon: 1w-1m
   - action_object: basket (SOXX/NVDA-adjacent) | polarity: mixed — capex intact but share/margin risk for NVDA
   - confidence: 0.55

### STEP 2 — INTERACTIONS
- **Fed hawkish + yield surge + gold flush** → ONE rates cluster; do not double-count across items 2/3/4. Net hawkish risk-off, CPI is the resolver.
- **Oil shock + hawkish Fed + earnings start** → stagflationary tape: cyclicals/energy favored, duration and small-caps penalized; don't buy IWM on rate-hike hopes.
- **Bank correction + bank earnings start** → XLF is a coin-flip into prints; no directional lean.
- **AI power/semis + NVDA China erosion** → semis mixed; do not short SOXX on substitution alone while AI capex intact.

### STEP 3 — RECLASSIFY AUDIT
**RESCUED (from noise/single-name):**
- Gold −$100 / AEM — real-rates signal, miners basket (keep)
- "US yield surge triggers mortgage hedging" — core rates transmission (keep)
- "Bank stocks mired in correction as yields surge before earnings" — financials sector driver (conditional)
- Amgen Phase 3 — sector sympathy force beyond single name (keep)

**DROPPED (from usable):**
- All "Powell" obituary/sports/debate noise under topic_fed — parser name-matching failure, zero market relevance
- Ronaldo / Saudi missile-alert scarf — color, not market driver
- Dollar near 18-month high — already captured inside rates cluster
- BNY prime rate — folded into bank-earnings item, not standalone

### STEP 4 — B1 INJECT

NEWS_JUDGE: n=8 rescued=4
MACRO rates: [hawkish] Fed minutes flag another hike; yields surging, gold −$100, CPI/Powell binary pending (session/1d)
MACRO risk: [bearish] Fresh Hormuz tanker attack + Houthi Riyadh missiles; shipping/insurance costs surge (session/1d-1w)
MACRO interaction: [bearish] Oil shock + hawkish Fed = stagflationary tape; mild-band cap unless futures confirm ≥0.5% down
SECTOR financials: [mixed] Yields surge into bank earnings; BNY prime to 7.00% — NIM vs duration tug-of-war (XLF/KRE)
SECTOR healthcare: [bullish] AMGN dazodalibep Phase 3 win; IBB/XBI sympathy (basket)
SECTOR industrials: [bearish] Boeing MAX 10 cert halted by FAA; 31% of undelivered order book at risk (BA/XLI)
SECTOR semis: [mixed] MSFT gigawatt in-house AI chips + NVDA China share loss to Huawei (substitution, not short trigger)
WATCH: rates cluster dominates; do not double-count hawkish signals across Fed/yields/gold.

---

NEWS_PARSE_BEGIN
IMPORTANT_COUNT: 8
TOP_ITEMS:
- Strait of Hormuz tanker attack / Houthi Riyadh missiles | keep | channel=risk | severity=session | horizon=1d-1w | object=spx:SPX beta; XLE secondarily | pol=bearish | conf=0.8
- Fed minutes another hike + CPI/Powell binary | keep | channel=rates | severity=session | horizon=1d | object=spx:SPX | pol=hawkish | conf=0.75
- Yield surge / mortgage hedging / bank correction | keep | channel=rates | severity=session | horizon=1d-1w | object=sector_etf:XLF,KRE | pol=bearish | conf=0.7
- Gold −$100 on hawkish Fed (AEM) | keep | channel=rates | severity=session | horizon=1d | object=basket:GDX gold miners | pol=bearish | conf=0.7
- Bank earnings begin / BNY prime +25bp | conditional | channel=sector_fundamental | severity=session | horizon=1d-1w | object=sector_etf:XLF | pol=mixed | conf=0.6
- AMGN dazodalibep Phase 3 success | keep | channel=sector_fundamental | severity=session | horizon=1d | object=basket:IBB/XBI | pol=bullish | conf=0.65
- Boeing MAX 10 certification halted | keep | channel=sector_fundamental | severity=session | horizon=1w-1m | object=single_name:BA/XLI | pol=bearish | conf=0.7
- MSFT in-house AI chips / NVDA China erosion | single_name | channel=substitution | severity=session | horizon=1w-1m | object=basket:SOXX/NVDA | pol=mixed | conf=0.55
INTERACTIONS: Fed+yields+gold = one rates cluster (no double-count); oil shock + hawkish Fed = stagflationary, energy/cyclicals favored over duration/small-caps; bank correction + earnings start = XLF no lean; AI capex intact so substitution not a short trigger on SOXX
RESCUED_FROM_NOISE: Gold −$100 hawkish real-rate flush (AEM); US yield surge mortgage hedging; Bank stocks mired in correction before earnings; AMGN Phase 3 dazodalibep
DROPPED_FROM_USABLE: Powell name-match noise (obits/sports/debates); Ronaldo scarf color; Dollar at 18-month high (folded into rates cluster); BNY prime hike (folded into bank earnings)
B1_INJECT:
NEWS_JUDGE: n=8 rescued=4
MACRO rates: [hawkish] Fed minutes flag another hike; yields surging, gold −$100, CPI/Powell binary pending (session/1d)
MACRO risk: [bearish] Fresh Hormuz tanker attack + Houthi Riyadh missiles; shipping/insurance costs surge (session/1d-1w)
MACRO interaction: [bearish] Oil shock + hawkish Fed = stagflationary tape; mild-band cap unless futures confirm ≥0.5% down
SECTOR financials: [mixed] Yields surge into bank earnings; BNY prime to 7.00% — NIM vs duration tug-of-war (XLF/KRE)
SECTOR healthcare: [bullish] AMGN dazodalibep Phase 3 win; IBB/XBI sympathy (basket)
SECTOR industrials: [bearish] Boeing MAX 10 cert halted by FAA; 31% of undelivered order book at risk (BA/XLI)
SECTOR semis: [mixed] MSFT gigawatt in-house AI chips + NVDA China share loss to Huawei (substitution, not short trigger)
WATCH: rates cluster dominates; do not double-count hawkish signals across Fed/yields/gold.
NEWS_PARSE_END
