# News Judge — 2026-10-20

### IMPORTANT NEWS (my ranking)
1. **Hawkish Fed comments lift October hike odds; gold drops >$100; CPI still needs “some work”** — direct rates/risk-appetite repricing; outranks single-name color. *Channel: rates*
2. **Asian LNG prices jump to highest since 2022 on tanker strikes** — energy/supply shock with cross-asset risk read-through. *Channel: risk*
3. **Nasdaq 100 has best day in nearly a month as AI rout cools; Big Tech earnings could validate AI** — key risk-sentiment and index-beta driver. *Channel: sentiment*
4. **Amazon says its AI chips will work with popular coding language, challenging Nvidia** — semis/AI competitive signal for sector baskets. *Channel: sector_fundamental*
5. **Boeing 737 MAX 10 certification halted by FAA over software glitch; 31% of undelivered order book at risk** — clear industrials sector fundamental hit. *Channel: sector_fundamental*
6. **Wells Fargo says earnings off to weak start as most fall short** — earnings-breadth caution for financials/SPX sentiment. *Channel: sector_fundamental*
7. **AbbVie FDA approval of JUVMO/tavapadon for Parkinson’s** — healthcare catalyst with possible basket sympathy. *Channel: sector_fundamental*
8. **Amgen positive Phase 3 dazodalibep data in Sjögren’s; Jefferies raises PT** — biotech/healthcare sentiment support beyond single name. *Channel: sector_fundamental*

### STEP 1 — FRAMEWORK SCORE
- **Hawkish Fed / gold drop / CPI “some work”** — keep; us_relevance high (US rates/risk appetite); channel rates; geography us_domestic; severity regime; horizon 1d-1w; action spx:SPX beta; polarity hawkish; why: higher hike odds pressure duration and risk appetite; confidence 0.80.
- **Asian LNG jump on tanker strikes** — keep; us_relevance medium (global energy/supply risk); channel risk; geography global_priced; severity session; horizon 1d-1w; action sector_etf:XLE; polarity mixed; why: supports energy equities but adds global risk; confidence 0.70.
- **Nasdaq best day / AI rout cools / Big Tech earnings** — keep; us_relevance high (NQ/SPX beta); channel sentiment; geography us_domestic; severity session; horizon 1d-1w; action basket:AI mega-cap/NQ; polarity bullish; why: AI-led risk appetite recovery; confidence 0.72.
- **Amazon AI chips challenge Nvidia** — conditional; us_relevance high (semis/AI); channel sector_fundamental; geography us_supply_chain; severity session; horizon 1w; action sector_etf:SMH; polarity mixed; why: AI demand positive but Nvidia competition negative; confidence 0.66.
- **Boeing 737 MAX 10 FAA halt** — keep; us_relevance medium (industrials sector); channel sector_fundamental; geography us_domestic; severity session; horizon 1w-1m; action sector_etf:ITA; polarity bearish; why: certification delay and order-book risk; confidence 0.80.
- **Wells Fargo weak earnings start** — conditional; us_relevance medium (financials/earnings breadth); channel sector_fundamental; geography us_domestic; severity session; horizon 1d-1w; action sector_etf:XLF; polarity bearish; why: earnings caution pressures financials/sentiment; confidence 0.60.
- **AbbVie FDA approval JUVMO** — conditional; us_relevance medium (healthcare basket); channel sector_fundamental; geography us_domestic; severity session; horizon 1w; action basket:healthcare/biotech; polarity bullish; why: FDA approval supports healthcare sentiment; confidence 0.65.
- **Amgen Phase 3 positive data** — conditional; us_relevance medium (biotech/healthcare); channel sector_fundamental; geography us_domestic; severity session; horizon 1w; action basket:healthcare/biotech; polarity bullish; why: positive Phase 3 can lift immunology/biotech sympathy; confidence 0.64.

RULES_APPLIED: none — no futures snapshot or confirmed pending CPI/NFP/FOMC binary is supplied, so the standing FOMC/CPI and oil-shock gates are not mechanically triggered.

### STEP 2 — INTERACTIONS
- AI sentiment bounce + Amazon AI-chip challenge → semis/AI mixed; do not chase broad AI beta without Nvidia reaction.
- Amgen Phase 3 success + AbbVie FDA approval → Healthcare basket sympathy, not only the two single names.

### STEP 3 — RECLASSIFY AUDIT
- **Dropped from usable:** Powell obituary/roster cluster, Mavericks/Dwight Powell items, China nuclear-sub item, Nextdc Melbourne data-center power-delay item — false-positive mechanical tags or non-US-equity single-name noise.
- **Rescued from noise:** Asian LNG tanker-strike price spike — genuine global energy/supply risk; AI-stock bounce/CoreWeave upgrade — relevant to AI risk-sentiment recovery.

### STEP 4 — B1 / SECTOR INJECT
NEWS_JUDGE: n=8 rescued=2
MACRO rates: hawkish Fed comments and CPI “some work” lift Oct hike odds; gold -$100 (regime/1d-1w)
MACRO energy: Asian LNG highest since 2022 on tanker strikes (session/1d-1w)
MACRO risk: Nasdaq best day as AI rout cools; Big Tech earnings validation ahead (session/1d-1w)
SECTOR semis/AI: Amazon AI chips challenge Nvidia; AI bounce vs competitive headline (SMH mixed)
SECTOR industrials: Boeing 737 MAX 10 FAA halt risks 31% undelivered order book (ITA/BA)
SECTOR financials: Wells Fargo says earnings weak start; most fall short (XLF)
SECTOR healthcare: AbbVie FDA approval + Amgen Phase 3 positive → XLV/biotech sympathy
INTERACTION: AI sentiment bounce + Amazon AI-chip challenge = semis/AI mixed; healthcare catalysts support XLV basket, not only single names
WATCH: mechanical usable set polluted by Powell obituary noise; no futures provided

NEWS_PARSE_BEGIN
IMPORTANT_COUNT: 8
TOP_ITEMS:
- Hawkish Fed comments lift October hike odds and gold drops over $100 while CPI needs some work | keep=keep | channel=rates | severity=regime | horizon=1d-1w | object=spx:SPX beta | pol=hawkish | conf=0.80
- Asian LNG prices jump to highest since 2022 on tanker strikes | keep=keep | channel=risk | severity=session | horizon=1d-1w | object=sector_etf:XLE | pol=mixed | conf=0.70
- Nasdaq 100 best day in nearly a month as AI rout cools and Big Tech earnings could validate AI | keep=keep | channel=sentiment | severity=session | horizon=1d-1w | object=basket:AI mega-cap/NQ | pol=bullish | conf=0.72
- Amazon says AI chips will work with popular coding language challenging Nvidia | keep=conditional | channel=sector_fundamental | severity=session | horizon=1w | object=sector_etf:SMH | pol=mixed | conf=0.66
- Boeing 737 MAX 10 certification halted by FAA over software glitch risking 31% of undelivered order book | keep=keep | channel=sector_fundamental | severity=session | horizon=1w-1m | object=sector_etf:ITA | pol=bearish | conf=0.80
- Wells Fargo says earnings off to weak start as most fall short | keep=conditional | channel=sector_fundamental | severity=session | horizon=1d-1w | object=sector_etf:XLF | pol=bearish | conf=0.60
- AbbVie FDA approval of JUVMO tavapadon for Parkinson's disease | keep=conditional | channel=sector_fundamental | severity=session | horizon=1w | object=basket:healthcare/biotech | pol=bullish | conf=0.65
- Amgen positive Phase 3 dazodalibep data in Sjogren's disease and Jefferies raises price target | keep=conditional | channel=sector_fundamental | severity=session | horizon=1w | object=basket:healthcare/biotech | pol=bullish | conf=0.64
INTERACTIONS: AI sentiment bounce + Amazon AI-chip challenge → semis/AI mixed; Amgen Phase 3 success + AbbVie approval → Healthcare basket sympathy, not only names
RESCUED_FROM_NOISE: Asian LNG Prices Jump to Highest Since 2022 on Tanker Strikes; AI stocks bounce back after bruising selloff, CoreWeave gets Goldman upgrade
DROPPED_FROM_USABLE: Mavericks Waive Toronto Native Dwight Powell Before Roster Deadline; Powell obituary cluster (20+ items); China Reveals Advanced Nuclear Sub Was Used to Test-Fire Missile; Nextdc Flags Earnings Risk on Melbourne Data Center Power Delay
B1_INJECT:
NEWS_JUDGE: n=8 rescued=2
MACRO rates: hawkish Fed comments and CPI “some work” lift Oct hike odds; gold -$100 (regime/1d-1w)
MACRO energy: Asian LNG highest since 2022 on tanker strikes (session/1d-1w)
MACRO risk: Nasdaq best day as AI rout cools; Big Tech earnings validation ahead (session/1d-1w)
SECTOR semis/AI: Amazon AI chips challenge Nvidia; AI bounce vs competitive headline (SMH mixed)
SECTOR industrials: Boeing 737 MAX 10 FAA halt risks 31% undelivered order book (ITA/BA)
SECTOR financials: Wells Fargo says earnings weak start; most fall short (XLF)
SECTOR healthcare: AbbVie FDA approval + Amgen Phase 3 positive → XLV/biotech sympathy
INTERACTION: AI sentiment bounce + Amazon AI-chip challenge = semis/AI mixed; healthcare catalysts support XLV basket, not only single names
WATCH: mechanical usable set polluted by Powell obituary noise; no futures provided
NEWS_PARSE_END
