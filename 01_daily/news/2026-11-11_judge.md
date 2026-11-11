# News Judge — 2026-11-11

### IMPORTANT NEWS (my ranking)
1. **Hot October CPI and Waller’s half-point hike debate cement December hike odds** — dominant rates/risk shock; lifts hike odds, 10Y near 5%, pressures SPX beta. Channel: rates.
2. **Nvidia selloff deepens on AI jitters and Jensen Huang scrutiny** — AI/semis leadership risk; spills into QQQ/SMH and crowded AI basket. Channel: risk.
3. **Caterpillar reportedly wins $3.4B Anthropic gas-turbine order for AI data centers** — AI-power capex signal for industrials/energy, not pure single-name color. Channel: sector_fundamental.
4. **Constellation raises 2024 EPS guidance again on expanded Amazon/Google nuclear deals** — supports AI-power IPP/utility earnings narrative. Channel: sector_fundamental.
5. **Boeing to cut 5% of commercial airplane workforce as it slows production increases** — industrials production/sentiment negative. Channel: sector_fundamental.
6. **Investors buy the dip in chip ETFs as inflows top $44B** — partial semis sentiment offset to Nvidia selloff. Channel: sentiment.
7. **CoreWeave boosts annual forecast easing AI slowdown concerns** — rescued AI-infra demand signal; offsets chip weakness only partially. Channel: sentiment.
8. **Wells Fargo upgrades US AI infrastructure view after chip selloff** — rescued sector-sentiment stabilizer, not an all-clear. Channel: sentiment.

### STEP 1 — FRAMEWORK SCORE
- **Hot CPI/Waller hike debate** | keep | us_relevance: high — direct SPX discount-rate and Fed-path shock | channel: rates | geography: us_domestic | severity: regime | horizon: 1d-1w | action: spx:SPX beta/10Y | polarity: hawkish — hot CPI plus Waller half-point talk lift hike odds | conf: 0.90
- **Nvidia selloff/AI jitters** | keep | us_relevance: high — AI leadership is index and semis beta | channel: risk | geography: us_domestic | severity: session | horizon: 1d-1w | action: sector_etf:SMH/QQQ | polarity: bearish — selling pressure in AI bellwether | conf: 0.85
- **CAT/Anthropic turbine order** | keep | us_relevance: medium-high — AI data-center power capex read-through | channel: sector_fundamental | geography: us_domestic | severity: session | horizon: 1w-1m | action: sector_etf:XLI/XLE | polarity: bullish — large AI-power contract supports capex | conf: 0.75
- **CEG nuclear guidance** | keep | us_relevance: medium-high — AI-power utility earnings signal | channel: sector_fundamental | geography: us_domestic | severity: session | horizon: 1w-1m | action: sector_etf:XLU | polarity: bullish — expanded nuclear PPAs lift guidance | conf: 0.75
- **Boeing workforce cut** | conditional | us_relevance: medium — large industrial single-name with sector read-through | channel: sector_fundamental | geography: us_domestic | severity: session | horizon: 1w | action: sector_etf:XLI | polarity: bearish — production slowdown signal | conf: 0.70
- **Chip ETF dip-buying inflows** | conditional | us_relevance: medium — semis sentiment stabilizer, not fundamental | channel: sentiment | geography: us_domestic | severity: session | horizon: 1d-1w | action: basket:chip ETFs | polarity: bullish — inflows show dip demand despite Nvidia | conf: 0.60
- **CoreWeave forecast boost** | conditional | us_relevance: medium — AI-infra demand signal | channel: sentiment | geography: us_domestic | severity: session | horizon: 1d-1w | action: basket:AI-infra | polarity: bullish — forecast raise eases slowdown fear | conf: 0.65
- **Wells Fargo AI-infra upgrade** | conditional | us_relevance: medium — analyst sentiment after chip selloff | channel: sentiment | geography: us_domestic | severity: session | horizon: 1d-1w | action: basket:AI-infra | polarity: bullish — upgrade supports AI-infra view | conf: 0.60

### STEP 2 — INTERACTIONS
Hot CPI/Waller + Nvidia selloff = one rates/AI risk-off cluster; do not buy software/semis on dovish-rate hope. AI-power demand (CAT turbines, CEG nuclear) + Nvidia/AI jitters = AI capex mixed; utilities/power supported, semis not all-clear. CoreWeave/Wells/chip ETF inflows offset Nvidia selloff only partially; semis sentiment mixed, do not double-count positives. Fed path + hot CPI = one rates cluster, not separate signals.

### STEP 3 — RECLASSIFY AUDIT
**DROPPED_FROM_USABLE:** duplicate CPI/Fed headlines, foreign macro/rates spillovers, crypto CPI item, and non-US equity items — no incremental US equity/sector force beyond the rates cluster. **RESCUED_FROM_NOISE:** CoreWeave forecast boost and Wells Fargo AI-infra upgrade — both are AI-infra sentiment stabilizers after the chip selloff, so they belong in conditional sector context, not noise.

**RULES_APPLIED: none** — no fresh kinetic oil shock, no unresolved same-morning voting Fed binary, and CPI/Waller are already printing rather than pending at the open.

NEWS_PARSE_BEGIN
IMPORTANT_COUNT: 8
TOP_ITEMS:
- Hot October CPI and Waller half-point hike debate cement December hike odds | keep=keep | channel=rates | severity=regime | horizon=1d-1w | object=spx:SPX beta | pol=hawkish | conf=0.90
- Nvidia selloff deepens on AI jitters and Jensen Huang scrutiny | keep=keep | channel=risk | severity=session | horizon=1d-1w | object=sector_etf:SMH/QQQ | pol=bearish | conf=0.85
- Caterpillar reportedly wins $3.4B Anthropic gas-turbine order for AI data centers | keep=keep | channel=sector_fundamental | severity=session | horizon=1w-1m | object=sector_etf:XLI/XLE | pol=bullish | conf=0.75
- Constellation raises 2024 EPS guidance again on expanded Amazon/Google nuclear deals | keep=keep | channel=sector_fundamental | severity=session | horizon=1w-1m | object=sector_etf:XLU | pol=bullish | conf=0.75
- Boeing to cut 5% of commercial airplane workforce as it slows production increases | keep=conditional | channel=sector_fundamental | severity=session | horizon=1w | object=sector_etf:XLI | pol=bearish | conf=0.70
- Investors buy the dip in chip ETFs as inflows top $44B | keep=conditional | channel=sentiment | severity=session | horizon=1d-1w | object=basket:chip ETFs | pol=bullish | conf=0.60
- CoreWeave boosts annual forecast easing AI slowdown concerns | keep=conditional | channel=sentiment | severity=session | horizon=1d-1w | object=basket:AI-infra | pol=bullish | conf=0.65
- Wells Fargo upgrades US AI infrastructure view after chip selloff | keep=conditional | channel=sentiment | severity=session | horizon=1d-1w | object=basket:AI-infra | pol=bullish | conf=0.60
INTERACTIONS: Hot CPI/Waller + Nvidia selloff = rates/AI risk-off cluster; AI-power demand (CAT turbines, CEG nuclear) + Nvidia/AI jitters = AI capex mixed; CoreWeave/Wells/chip ETF inflows offset Nvidia selloff only partially; Fed path + hot CPI = one rates cluster
RESCUED_FROM_NOISE: CoreWeave boosts annual forecast easing AI slowdown concerns; Wells Fargo upgrades US AI infrastructure view after chip selloff
DROPPED_FROM_USABLE: Canadian dollar hits near four-week low as US CPI data boosts greenback; Canada reimposes sanctions on 11 Iranian entities for contributing to nuclear activities; Faltering Philippine Growth Spurs Calls for Rate Hold State Aid; KPMG Chief Economist Predicts Hot PCE Report After CPI; Food Gas Prices Push October CPI Higher; China's October crude oil imports rise to highest level since March; Indian shares inch higher to close at 8-month high as crude oil drops; Bitcoin Trades Near $73K as Hot CPI Fuels Fed Rate Hike Fears
B1_INJECT:
NEWS_JUDGE: n=8 rescued=2
MACRO rates: [pol] Hot CPI + Waller half-point debate lift Dec hike odds, 10Y near 5% (regime/1d-1w)
SECTOR semis/AI: [pol] Nvidia selloff/deepening AI jitters weigh QQQ/SMH (sector_etf:SMH/QQQ)
SECTOR industrials/energy: [pol] CAT $3.4B Anthropic gas-turbine order supports AI-power capex (sector_etf:XLI/XLE)
SECTOR utilities: [pol] CEG raises EPS on Amazon/Google nuclear deals supports AI-power IPPs (sector_etf:XLU)
SECTOR industrials: [pol] Boeing 5% commercial workforce cut signals production slowdown (sector_etf:XLI)
SECTOR semis: [pol] Chip ETF dip-buying inflows top $44B, partial offset to Nvidia selloff (basket:chip ETFs)
SECTOR AI-infra: [pol] CoreWeave raises forecast, Wells Fargo upgrades AI-infra view offset chip selloff (basket:AI-infra)
INTERACTION: Hot CPI/Waller + Nvidia selloff = rates/AI risk-off cluster; AI-power positives offset only partly; do not double-count AI-infra upgrades as all-clear.
WATCH: Hawkish CPI repricing can dominate single-name positives; semis remain headline-sensitive.
NEWS_PARSE_END

