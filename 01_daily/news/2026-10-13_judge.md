# News Judge — 2026-10-13

### IMPORTANT NEWS (my ranking)

1. **Houthi/Iran escalation: Riyadh missile attacks day 3, tanker attacked in Strait of Hormuz** — fresh kinetic oil-supply increment with energy freight/insurance costs surging; direct risk-appetite and energy-channel driver. Channel: risk
2. **Hawkish Fed comments boost October hike expectations; gold drops >$100; yields surge with mortgage hedging pressure** — the dominant rates cluster into this week's CPI + Powell; hits gold, banks, small caps, duration. Channel: rates
3. **OPEC cuts 2026 oil demand growth forecast to 400k bpd** — demand-side counterweight to the supply shock; caps XLE upside, mixed for energy. Channel: sector_fundamental
4. **EU weighs oil/gas windfall profit tax as energy prices soar** — policy overhang on European energy majors, mild substitution/read-across for US energy sentiment. Channel: sector_policy
5. **Boeing 737 MAX 10 certification halted by FAA, 31% of undelivered order book at risk** — Industrials sector weight and supply-chain read-across. Channel: sector_fundamental
6. **Bank stocks mired in correction as yields surge before earnings; bank earnings begin this week** — rates + earnings binary for XLF. Channel: rates
7. **Microsoft plans huge in-house AI chip deployment / Cisco–Nvidia data-center bottleneck coverage** — semis/AI-infra basket narrative (custom silicon substitution vs NVDA). Channel: substitution
8. **Gold drops >$100 on hawkish Fed (AEM digest)** — sentiment/rates proxy for Materials and the crowded gold trade. Channel: sentiment

The usable set is macro-thin (geopolitics + Fed) and padded with topic_fed noise; most sector color lives in the FINVIZ digest.

---

**STEP 1 — FRAMEWORK SCORE**

1. Hormuz/Riyadh escalation — keep | us_relevance: high (oil supply, freight/insurance, futures risk premium) | channel: risk | geography: us_supply_chain | severity: session | horizon: 1d-1w | action_object: spx | detail: SPX beta via risk premium; XLE second-order | polarity: bearish | why: fresh kinetic supply shock, standing lessons confirm down/mild scoring on confirming tape | conf: 0.8
2. Hawkish Fed + yield surge + CPI/Powell pending — keep | high (10Y backup, mortgage hedging pressure, hike odds into binary) | rates | us_domestic | session | 1d | spx (with XLF, IWM side effects) | hawkish | why: rising discount rate + unresolved CPI/Powell binary suppresses multiple expansion | conf: 0.75
3. OPEC demand cut — conditional | medium (global-priced but caps US energy upside) | sector_fundamental | global_priced | session | 1w | sector_etf: XLE | mixed | why: bullish for price, bearish for demand narrative | conf: 0.6
4. EU windfall tax on oil/gas — conditional | low (foreign policy; mild US sentiment read-across) | sector_policy | global_priced | 1w-1m | sector_etf: XLE (sentiment only) | bearish | why: policy-risk template for energy | conf: 0.5
5. Boeing MAX 10 halt — keep | medium (Industrials heavyweight, order-book risk) | sector_fundamental | us_domestic | session | 1w-1m | single_name: BA (with XLI drag) | bearish | why: FAA halt hits delivery revenue and supplier chain | conf: 0.7
6. Bank correction pre-earnings — conditional | medium (XLF weight, earnings kicking off) | rates | us_domestic | session | 1d-1w | sector_etf: XLF | bearish | why: yield surge + NII uncertainty into prints | conf: 0.6
7. Microsoft custom AI chips at scale — keep | medium (crowded AI-infra basket; substitution vs NVDA/AVGO) | substitution | us_domestic | session | 1w | basket: semis/AI-infra (NVDA, AVGO hyperscaler custom-silicon debate) | mixed | why: capex-save bullish for MSFT, bearish for merchant-silicon share narrative | conf: 0.6
8. Gold >$100 drop on hawkish Fed — conditional | low-medium (sentiment proxy; crowded gold trade unwinding) | sentiment | global_priced | 1d | basket: gold miners (GDX/AEM) | bearish | why: real-rate-driven unwind, not idiosyncratic | conf: 0.6

**STEP 2 — INTERACTIONS**
- **Hawkish Fed/yields + Hormuz oil = one risk cluster**: oil supply shock colliding with rising hike odds into CPI — do not score as separate offsetting forces; net risk-off with XLE the only hedge leg.
- **Yields up + banks pre-earnings + gold unwind**: same rates channel across XLF, GDX, IWM — don't double-count as three bearish signals; treat as one hawkish-rates sleeve.
- **MSFT custom chips + Cisco/Nvidia bottleneck coverage**: substitution pressure on merchant silicon; semis mixed, not directional.

**STEP 3 — RECLASSIFY AUDIT**
RESCUED from noise: "US yield surge triggers mortgage hedging" (rates, merges into item 2); "Bank Stocks Mired in Correction as Yields Surge" (XLF, item 6); "EU looking at oil and gas windfall profit tax" (sector_policy, item 4); "AEM/gold drop $100" from digest (sentiment, item 8); Boeing FAA halt from digest (item 5).
DROPPED from usable: the entire topic_fed obituary/sports Powell padding (~20 items) — keyword collision, zero market content; Ronaldo scarf item — noise; China sanctions on Paraguay/Belize ministers — no US equity transmission; Anduril cheaper missile — single-name color, no sector force. Also dropped single_name bucket: Burry bull-trap clickbait, TikTok/Chew AI-hype warnings, Norway Xandr fine, Tesla robotaxi probe.

RULES_APPLIED: fresh-overnight-kinetic-oil-supply-escalation (Hormuz increment — cap magnitude at mild if futures flat); a-scheduled-high-impact-macro (CPI/Powell binary pending — no pre-signed directional lean).

**STEP 4 — B1 / SECTOR INJECT**

NEWS_JUDGE: n=8 rescued=5
MACRO rates: [hawkish] Hike odds building into CPI/Powell; yields surging, mortgage hedging pressure, gold −$100 (session/1d)
MACRO risk: [bearish] Fresh Hormuz tanker attack + 3rd day Riyadh missiles; freight/insurance costs surging (session/1d-1w)
MACRO energy: [mixed] OPEC cuts 2026 demand growth to 400k bpd — caps XLE upside despite supply shock (1w)
SECTOR energy: [mixed] Supply shock vs demand cut vs EU windfall-tax risk (XLE)
SECTOR industrials: [bearish] Boeing MAX 10 certification halted by FAA; 31% undelivered order book at risk (BA, XLI drag)
SECTOR financials: [bearish] Banks correcting into earnings on yield surge; XLF + NII binary this week (XLF)
SECTOR semis: [mixed] MSFT scaling custom AI chips — merchant-silicon substitution pressure vs NVDA/AVGO (basket)
INTERACTION: Hawkish-rates cluster (yields+gold+banks) is one sleeve; oil shock + hike odds = net risk-off, XLE only hedge
WATCH: CPI/Powell binary unresolved — no pre-signed directional lean; cap magnitude if futures flat.

NEWS_PARSE_BEGIN
IMPORTANT_COUNT: 8
TOP_ITEMS:
- Hormuz tanker attack + Riyadh missiles day 3 | keep | channel=risk | severity=session | horizon=1d-1w | object=spx:SPX beta, XLE hedge leg | pol=bearish | conf=0.8
- Hawkish Fed comments, October hike odds, yield surge + mortgage hedging | keep | channel=rates | severity=session | horizon=1d | object=spx:SPX duration/beta, XLF/IWM side effects | pol=hawkish | conf=0.75
- OPEC cuts 2026 demand growth to 400k bpd | conditional | channel=sector_fundamental | severity=session | horizon=1w | object=sector_etf:XLE | pol=mixed | conf=0.6
- EU weighs oil/gas windfall profit tax | conditional | channel=sector_policy | severity=signal horizon=1w-1m | object=sector_etf:XLE sentiment | pol=bearish | conf=0.5
- Boeing 737 MAX 10 certification halted by FAA | keep | channel=sector_fundamental | severity=session | horizon=1w-1m | object=single_name:BA + XLI drag | pol=bearish | conf=0.7
- Bank stocks in correction as yields surge pre-earnings | conditional | channel=rates | severity=session | horizon=1d-1w | object=sector_etf:XLF | pol=bearish | conf=0.6
- Microsoft huge custom AI chip deployment | keep | channel=substitution | severity=session | horizon=1w | object=basket:semis/AI-infra (NVDA/AVGO) | pol=mixed | conf=0.6
- Gold drops >$100 on hawkish Fed (AEM) | conditional | channel=sentiment | severity=session | horizon=1d | object=basket:GDX/AEM gold miners | pol=bearish | conf=0.6
INTERACTIONS: hawkish-yields+gold+banks = one rates sleeve; oil shock + hike odds = net risk-off with XLE hedge; MSFT custom chips + Cisco/Nvidia bottleneck = semis mixed
RESCUED_FROM_NOISE: US yield surge triggers mortgage hedging; Bank Stocks Mired in Correction as Yields Surge; EU looking at oil and gas windfall profit tax; AEM/gold drops $100 on hawkish Fed; Boeing MAX 10 FAA halt
DROPPED_FROM_USABLE: topic_fed Powell obituary/sports padding; Ronaldo Saudi scarf item; China sanctions on Paraguay/Belize ministers; Anduril cheaper missile; Burry bull-trap clickbait; Tesla robotaxi probe; Norway Xandr fine
B1_INJECT:
NEWS_JUDGE: n=8 rescued=5
MACRO rates: [hawkish] Hike odds building into CPI/Powell; yields surging, gold −$100 (session/1d)
MACRO risk: [bearish] Fresh Hormuz tanker attack + 3rd day Riyadh missiles; freight costs surging (session/1d-1w)
MACRO energy: [mixed] OPEC cuts 2026 demand growth to 400k bpd (1w)
SECTOR energy: [mixed] Supply shock vs demand cut vs EU windfall-tax risk (XLE)
SECTOR industrials: [bearish] Boeing MAX 10 certification halted; 31% order book at risk (BA, XLI)
SECTOR financials: [bearish] Banks correcting into earnings on yield surge (XLF)
SECTOR semis: [mixed] MSFT custom AI chip scale-up; merchant-silicon substitution pressure (NVDA/AVGO basket)
INTERACTION: Hawkish-rates cluster = one sleeve; oil shock + hike odds = net risk-off, XLE only hedge
WATCH: CPI/Powell binary pending — no pre-signed lean; cap magnitude if futures flat
NEWS_PARSE_END
