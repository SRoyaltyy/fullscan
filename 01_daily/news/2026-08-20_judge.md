# News Judge — 2026-08-20

### IMPORTANT NEWS (my ranking)

1. **Long-term Treasury yields hit 19-year high; bond market shrugging off hesitant Fed** — This is the dominant macro force for the session, driving risk-off in long-duration assets and rotation into value/financials. It outranks everything else because it sets the rate spine for all sectors.
2. **Fed minutes show September rate hike still on the table; officials say hike necessary if inflation doesn't cool** — This is the hawkish policy anchor reinforcing the yield spike. It's a rates cluster with the bond market story, not separate news.
3. **Dow futures drop 400 points as Treasury yields reverse Bessent-driven decline; Walmart tumbles** — This is the live market reaction to the rates move, confirming risk-off at the open. Walmart's drop is a sector-fundamental signal for consumer staples/discretionary.
4. **Adobe shares fall after-hours as Figma's earnings highlight surging AI costs, triggering broader software-sector selloff** — This is a fresh, index-relevant catalyst for the tech sector. It hits the crowded AI/software trade and compounds the yield-driven pressure on long-duration tech.
5. **Analog Devices record Q3, beats, guides above on AI data center demand** — This is a positive counterweight in semis, showing AI demand is real but bifurcated. It prevents a blanket short on the whole semi complex.
6. **Gold price surge on softer data and FOMC anticipation drives AU up 8.5%** — This is a monetary-metals bid that can act as a hedge and a rotation target on risk-off days. It's a sector-fundamental signal for materials, but not a broad equity driver.
7. **US debt hits $40 trillion: Who does Washington owe and why does it matter?** — This is a structural backdrop story feeding the term-premium/ supply narrative behind the long-end yield spike. It's context, not a fresh catalyst, but it explains the persistence of the move.
8. **Target earnings, Canada tariffs, American Airlines' seatback screens and more in Morning Squawk** — This is a scheduled-event reminder. Target earnings are a key consumer read for the day, and Canada tariffs are a potential sector-policy overhang.

---

### STEP 1 — FRAMEWORK SCORE

**1. Long-term Treasury yields hit 19-year high; bond market shrugging off hesitant Fed**
- **keep**: keep
- **us_relevance**: high — Directly sets the discount rate for all US equities, especially long-duration growth.
- **channel**: rates
- **geography**: us_domestic
- **severity**: regime
- **horizon**: 1w
- **action_object**: spx
- **action_object_detail**: SPX beta, long-duration tech (XLK), rate-sensitive (XLRE, XLU)
- **polarity**: bearish
- **polarity_why**: Higher long-end yields compress multiples on growth and duration-sensitive assets, pressuring the broad index.
- **confidence**: 0.9

**2. Fed minutes show September rate hike still on the table; officials say hike necessary if inflation doesn't cool**
- **keep**: keep
- **us_relevance**: high — This is the policy anchor for the entire rates complex.
- **channel**: rates
- **geography**: us_domestic
- **severity**: regime
- **horizon**: 1w
- **action_object**: spx
- **action_object_detail**: SPX beta, rate-sensitive sectors
- **polarity**: hawkish
- **polarity_why**: Reinforces the higher-for-longer narrative, supporting the yield spike and pressuring equities.
- **confidence**: 0.85

**3. Dow futures drop 400 points as Treasury yields reverse Bessent-driven decline; Walmart tumbles**
- **keep**: keep
- **us_relevance**: high — This is the live market confirmation of the rates-driven risk-off.
- **channel**: risk
- **geography**: us_domestic
- **severity**: session
- **horizon**: 1d
- **action_object**: spx
- **action_object_detail**: SPX, consumer staples (XLP), consumer discretionary (XLY)
- **polarity**: bearish
- **polarity_why**: Negative futures and a key consumer bellwether (WMT) tumbling signal a risk-off open and potential consumer weakness.
- **confidence**: 0.8

**4. Adobe shares fall after-hours as Figma's earnings highlight surging AI costs, triggering broader software-sector selloff**
- **keep**: keep
- **us_relevance**: high — This is a fresh, index-relevant catalyst for the tech sector, hitting the crowded AI trade.
- **channel**: sector_fundamental
- **geography**: us_domestic
- **severity**: session
- **horizon**: 1d-1w
- **action_object**: sector_etf
- **action_object_detail**: XLK, IGV (software)
- **polarity**: bearish
- **polarity_why**: Surging AI costs without commensurate revenue is a negative read for high-multiple software names, triggering a sector selloff.
- **confidence**: 0.75

**5. Analog Devices record Q3, beats, guides above on AI data center demand**
- **keep**: keep
- **us_relevance**: high — A positive counterweight in semis, showing AI demand is real and bifurcated.
- **channel**: sector_fundamental
- **geography**: us_supply_chain
- **severity**: session
- **horizon**: 1d-1w
- **action_object**: sector_etf
- **action_object_detail**: SMH, XLK
- **polarity**: bullish
- **polarity_why**: Strong AI data center demand is a positive for the semiconductor complex, offsetting some of the software-led weakness.
- **confidence**: 0.7

**6. Gold price surge on softer data and FOMC anticipation drives AU up 8.5%**
- **keep**: conditional
- **us_relevance**: medium — A monetary-metals bid is a hedge and rotation target, but not a broad equity driver.
- **channel**: substitution
- **geography**: global_priced
- **severity**: session
- **horizon**: 1d-1w
- **action_object**: sector_etf
- **action_object_detail**: GDX, XLB (materials)
- **polarity**: bullish
- **polarity_why**: Gold's surge is a flight-to-safety and a hedge against inflation/currency debasement, benefiting gold miners.
- **confidence**: 0.65

**7. US debt hits $40 trillion: Who does Washington owe and why does it matter?**
- **keep**: conditional
- **us_relevance**: medium — This is structural context feeding the term-premium/supply narrative, not a fresh catalyst.
- **channel**: rates
- **geography**: us_domestic
- **severity**: regime
- **horizon**: 1m+
- **action_object**: none
- **action_object_detail**: none
- **polarity**: bearish
- **polarity_why**: The debt level is a backdrop for higher term premiums and long-end yields, but it's not a same-day driver.
- **confidence**: 0.5

**8. Target earnings, Canada tariffs, American Airlines' seatback screens and more in Morning Squawk**
- **keep**: conditional
- **us_relevance**: high — Target earnings are a key consumer read; Canada tariffs are a potential sector-policy overhang.
- **channel**: sector_fundamental
- **geography**: us_domestic
- **severity**: session
- **horizon**: 1d
- **action_object**: sector_etf
- **action_object_detail**: XLY, XLP
- **polarity**: mixed
- **polarity_why**: Earnings could be a positive or negative catalyst for consumer sectors; tariffs are a negative for specific industrials.
- **confidence**: 0.6

---

### STEP 2 — INTERACTIONS

- **Fed path + weak labor → treat as ONE rates cluster**: The Fed minutes and the long-end yield spike are the same rates cluster. Do not double-count them as separate bearish factors.
- **Yields down (buybacks / auction / dovish) + cyclicals/small-caps → risk-on breadth support**: This is the inverse. Today, yields are UP, so this interaction is inverted: yields up + long-duration tech = risk-off pressure.
- **AI chip demand + semi/solar tariff → semis mixed; do not double-count**: ADI's strong AI demand is a positive for semis, but the software selloff (ADBE) and yield pressure are negatives. The semi complex is mixed, not uniformly bearish.
- **Single-name biotech Phase 3 success + sector sympathy → Healthcare basket, not only the name**: No such catalyst is present today. This interaction does not fire.
- **SaaS multiple compression + weak labor → do not buy software on dovish-rates hope**: This fires. The ADBE-led software selloff is a multiple compression event. Do not buy software on hopes of a dovish Fed, as the minutes are hawkish.

---

### STEP 3 — RECLASSIFY AUDIT

**DROPPED_FROM_USABLE:**
- **Hypersonic missile startup Castelion raises $1 billion** — This is a single-name story with no immediate sector or macro force for the session. It's noise for the equity risk-appetite ranking.

**RESCUED_FROM_NOISE:**
- **US debt hits $40 trillion** — This is not noise. It's a structural backdrop feeding the term-premium/supply narrative behind the long-end yield spike. It's context, not a fresh catalyst, but it explains the persistence of the move.
- **Conditions are ripe for a market 'accident,' but surging bond yields alone won't cause it** — This is a sentiment/positioning read that corroborates the risk-off tape. It's a useful context flag, not a primary driver.

---

### STEP 4 — B1 / SECTOR INJECT

```
NEWS_JUDGE: n=8 rescued=2
MACRO rates: [bearish] Long-end yields at 19-yr high, hawkish Fed minutes reinforce higher-for-longer (regime/1w)
MACRO risk: [bearish] Dow futures -400 on yield reversal, WMT tumbles, risk-off open (session/1d)
SECTOR tech: [bearish] ADBE-led software selloff on AI costs, compounds yield pressure on XLK (sector_etf)
SECTOR semis: [mixed] ADI beats on AI DC demand, but software weakness and yields cap upside (sector_etf)
SECTOR materials: [bullish] Gold surge on FOMC anticipation, AU +8.5%, monetary-metals bid (sector_etf)
INTERACTION: Rates cluster (Fed minutes + yield spike) is ONE bearish driver; do not double-count.
WATCH: Target earnings and Canada tariffs are scheduled events that could shift consumer/industrial sectors intraday.
```

---

NEWS_PARSE_BEGIN
IMPORTANT_COUNT: 8
TOP_ITEMS:
- Long-term Treasury yields hit 19-year high; bond market shrugging off hesitant Fed | keep=keep | channel=rates | severity=regime | horizon=1w | object=spx:SPX beta, long-duration tech | pol=bearish | conf=0.9
- Fed minutes show September rate hike still on the table; officials say hike necessary if inflation doesn't cool | keep=keep | channel=rates | severity=regime | horizon=1w | object=spx:SPX beta, rate-sensitive sectors | pol=hawkish | conf=0.85
- Dow futures drop 400 points as Treasury yields reverse Bessent-driven decline; Walmart tumbles | keep=keep | channel=risk | severity=session | horizon=1d | object=spx:SPX, XLP, XLY | pol=bearish | conf=0.8
- Adobe shares fall after-hours as Figma's earnings highlight surging AI costs, triggering broader software-sector selloff | keep=keep | channel=sector_fundamental | severity=session | horizon=1d-1w | object=sector_etf:XLK, IGV | pol=bearish | conf=0.75
- Analog Devices record Q3, beats, guides above on AI data center demand | keep=keep | channel=sector_fundamental | severity=session | horizon=1d-1w | object=sector_etf:SMH, XLK | pol=bullish | conf=0.7
- Gold price surge on softer data and FOMC anticipation drives AU up 8.5% | keep=conditional | channel=substitution | severity=session | horizon=1d-1w | object=sector_etf:GDX, XLB | pol=bullish | conf=0.65
- US debt hits $40 trillion: Who does Washington owe and why does it matter? | keep=conditional | channel=rates | severity=regime | horizon=1m+ | object=none:none | pol=bearish | conf=0.5
- Target earnings, Canada tariffs, American Airlines' seatback screens and more in Morning Squawk | keep=conditional | channel=sector_fundamental | severity=session | horizon=1d | object=sector_etf:XLY, XLP | pol=mixed | conf=0.6
INTERACTIONS: Rates cluster (Fed minutes + yield spike) is ONE bearish driver; do not double-count; SaaS multiple compression + hawkish Fed = do not buy software on dovish hopes
RESCUED_FROM_NOISE: US debt hits $40 trillion; Conditions are ripe for a market 'accident'
DROPPED_FROM_USABLE: Hypersonic missile startup Castelion raises $1 billion
B1_INJECT:
NEWS_JUDGE: n=8 rescued=2
MACRO rates: [bearish] Long-end yields at 19-yr high, hawkish Fed minutes reinforce higher-for-longer (regime/1w)
MACRO risk: [bearish] Dow futures -400 on yield reversal, WMT tumbles, risk-off open (session/1d)
SECTOR tech: [bearish] ADBE-led software selloff on AI costs, compounds yield pressure on XLK (sector_etf)
SECTOR semis: [mixed] ADI beats on AI DC demand, but software weakness and yields cap upside (sector_etf)
SECTOR materials: [bullish] Gold surge on FOMC anticipation, AU +8.5%, monetary-metals bid (sector_etf)
INTERACTION: Rates cluster (Fed minutes + yield spike) is ONE bearish driver; do not double-count.
WATCH: Target earnings and Canada tariffs are scheduled events that could shift consumer/industrial sectors intraday.
NEWS_PARSE_END
