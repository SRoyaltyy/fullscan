# Stock book — 2026-08-31

_Generated 2026-08-31T19:02:44.871995-04:00_

This file is the **human read** of one run. CSV/JSON next to it are the machine files.

## How today's action is built

**1d uses a decision lattice.** Evidence is evaluated on its own merits before any numeric rank:

1. **Market gate** — raw general factor scoreboard + risk state sets exposure. An extreme confirmed red day closes ordinary longs.
2. **Parent / child route** — sector tape/essay and independent industry/theme absolute + relative strength decide where.
3. **Company route** — News Judge adjudicates; actions, Finviz digest and dossiers form one deduplicated direct-event decision.
4. **Setup / flow gate** — intrinsic AB + join structure, peer RS, price/gap and time-aware relative volume decide whether now.
5. **Rank inside the lane** — standard, group-leader or catalyst. mid_opp cannot grant permission.

The existing red/yellow/green source graph remains visible. Its digest, judge and catalyst cells are now populated before selection. 🔵 / 🚨 / ⚪, Cond, region and featured fades remain gates. A second six-domain row prevents duplicate headlines from voting three times. Longer horizons remain on the legacy weighted rank while the 1d lattice is validated.

## Today's regime

- Weather risk: **off**
- General predict (same-day): -0.47 down (present)
- Stand-down: **YES — no BUY** — HARD_RED: general down score=-5.85; good=+0.5 vs bad=-7.0; risk=off; red pillars=5; no confirmed direct-company exception
- Sector predicts this date: 10/11 (ok)
- News tickers in play: 118
- AB coverage: 2551 names · peer RS: 2400
- Universe after liquidity: 2685
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 105

## All-green BUY / SELL

- Stand-down: **no BUY.** HARD_RED: general down score=-5.85; good=+0.5 vs bad=-7.0; risk=off; red pillars=5; no confirmed direct-company exception
- Pile still computed (134 liquid green of 2685) but is not used to force names into a no-win open.
- SELL still ranks on full weights.

## Decision lattice — gate → route → rank

The weighted score is now a tie-breaker inside an eligible lane. It cannot average away a market, group, company, or setup veto.

### MARKET: 🔴 HARD_RED

- HARD_RED: general down score=-5.85; good=+0.5 vs bad=-7.0; risk=off; red pillars=5
- Allowed long lanes: **catalyst_exception** · max slots 2 · size ×0.25
- Bull evidence: sentiment +0.50 points
- Bear evidence: overnight catalysts -3.00 points; rates / Fed -2.00 points; oil / dollar -1.00 points; volatility -0.75 points; futures -0.25 points

Decision domains: **MKT · parent · child · company · setup · flow**. Measured parent/child tape is kept separate from the LLM essay; direct company events must be price-confirmed on a hard-red day.

### Bull decisions (eligible or closest blocked cases)

| # | Ticker | Domains | Lane | Company / group | Decision |
|---:|--------|---------|------|-----------------|----------|
| 1 | **CRM** | 🔴🔴🟢🟢🟢🟡 | blocked | direct high digest (same-day): Salesforce beats Q2 guidance, raises FY27 outlook as analysts lift price targets after 'narrative-changing' results; Software - Application +0.4% d1 / +4.2% 1w / +3.0% vs parent | BLOCK BUY — HARD_RED market closes ordinary/group longs; parent sector RED; direct catalyst lacks price confirmation / market=HARD_RED; parent=RED; child=GREEN/rel=GREEN; company=GREEN(0.88); setup=GREEN; flow=YELLOW |
| 2 | **AMGN** | 🔴🔴🔴🟢🟢🔴 | blocked | direct high digest (same-day): Amgen says Repatha cut all-cause mortality 20% in high-risk adults in Phase 3 VESALIUS-CV primary prevention analysis; Drug Manufacturers - General -0.4% d1 / -3.2% 1w / -1.0% vs parent | BLOCK BUY — HARD_RED market closes ordinary/group longs; parent sector RED; child industry/theme RED; flow RED; direct catalyst lacks price confirmation; v2 domain region red / market=HARD_RED; parent=RED; child=RED/rel=YELLOW; company=GREEN(0.82); setup=GREEN; flow=RED |
| 3 | **AON** | 🔴🟢🟢🟢🟢🟡 | blocked | direct high digest (same-day): Aon names Nadin Virani interim CFO, reaffirms 2026 guidance, launches Sidecar X transactional risk platform with $200 million capacity; Insurance Brokers +1.7% d1 / +0.4% 1w / -0.5% vs parent | BLOCK BUY — HARD_RED market closes ordinary/group longs; direct catalyst lacks price confirmation / market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=GREEN(0.72); setup=GREEN; flow=YELLOW |
| 4 | **MPC** | 🔴🟢🟢🟡🟢🟢 | blocked | direct normal digest (stale/undated): Mizuho raises Marathon Petroleum price target to $304 from $284, keeps Neutral after stronger-than-expected Q2 results; Oil & Gas Refining & Marketing +1.6% d1 / +1.0% 1w / +3.1% vs parent | BLOCK BUY — HARD_RED market closes ordinary/group longs / market=HARD_RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.30); setup=GREEN; flow=GREEN |
| 5 | **BNS** | 🔴🟢🟢🟡🟢🟢 | blocked | direct high digest (stale/undated): Bank of Nova Scotia posts record Q3 2026 EPS $2.28 as National Bank upgrades to Outperform, lifts target to C$142; Banks - Diversified +0.9% d1 / +1.4% 1w / +0.5% vs parent | BLOCK BUY — HARD_RED market closes ordinary/group longs; direct catalyst lacks price confirmation / market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.48); setup=GREEN; flow=GREEN |
| 6 | **BMO** | 🔴🟢🟢🟡🟢🟢 | blocked | direct high digest (stale/undated): Bank of Montreal beats fiscal Q3 2026 estimates with non-GAAP EPS $2.86, revenue $7.2B, announces new share buyback plan and higher dividend; Banks - Diversified +0.9% d1 / +1.4% 1w / +0.5% vs parent | BLOCK BUY — HARD_RED market closes ordinary/group longs / market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.48); setup=GREEN; flow=GREEN |
| 7 | **BKR** | 🔴🟢🟢🟡🟢🟡 | blocked | direct high digest (stale/undated): Baker Hughes wins multi-year Kuwait Oil Company contract as key technology collaborator for Ahmadi Innovation Valley; Oil & Gas Equipment & Services +0.9% d1 / +1.6% 1w / +3.7% vs parent | BLOCK BUY — HARD_RED market closes ordinary/group longs; direct catalyst lacks price confirmation / market=HARD_RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.48); setup=GREEN; flow=YELLOW |
| 8 | **ING** | 🔴🟢🟢🟡🟢🟢 | blocked | direct high digest (stale/undated): ING Groep NV ADR reports fiscal Q2 2026 results with non-GAAP EPS $0.77 (+21% YoY) and revenue $7.2B (+10% YoY), beats EPS and revenue estimates; Banks - Diversified +0.9% d1 / +1.4% 1w / +0.5% vs parent | BLOCK BUY — HARD_RED market closes ordinary/group longs; direct catalyst lacks price confirmation / market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.48); setup=GREEN; flow=GREEN |
| 9 | **RES** | 🔴🟢🟢🟡🟢🟢 | blocked | no direct company event; Oil & Gas Equipment & Services +0.9% d1 / +1.6% 1w / +3.7% vs parent | BLOCK BUY — HARD_RED market closes ordinary/group longs / market=HARD_RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 10 | **DINO** | 🔴🟢🟢🟡🟢🟢 | blocked | no direct company event; Oil & Gas Refining & Marketing +1.6% d1 / +1.0% 1w / +3.1% vs parent | BLOCK BUY — HARD_RED market closes ordinary/group longs / market=HARD_RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 11 | **NOV** | 🔴🟢🟢🟡🟢🟢 | blocked | no direct company event; Oil & Gas Equipment & Services +0.9% d1 / +1.6% 1w / +3.7% vs parent | BLOCK BUY — HARD_RED market closes ordinary/group longs / market=HARD_RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 12 | **SLB** | 🔴🟢🟢🟡🟢🟢 | blocked | no direct company event; Oil & Gas Equipment & Services +0.9% d1 / +1.6% 1w / +3.7% vs parent | BLOCK BUY — HARD_RED market closes ordinary/group longs / market=HARD_RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 13 | **HAL** | 🔴🟢🟢🟡🟢🟢 | blocked | no direct company event; Oil & Gas Equipment & Services +0.9% d1 / +1.6% 1w / +3.7% vs parent | BLOCK BUY — HARD_RED market closes ordinary/group longs / market=HARD_RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 14 | **BBVA** | 🔴🟢🟢🟡🟢🟡 | blocked | direct high digest (stale/undated): BBVA posts record Q2 2026 net profit, upgrades Mexico and South America guidance, announces extraordinary €2 billion share buyback program; Banks - Diversified +0.9% d1 / +1.4% 1w / +0.5% vs parent | BLOCK BUY — HARD_RED market closes ordinary/group longs / market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.48); setup=GREEN; flow=YELLOW |
| 15 | **PBF** | 🔴🟢🟢🟡🟢🟢 | blocked | no direct company event; Oil & Gas Refining & Marketing +1.6% d1 / +1.0% 1w / +3.1% vs parent | BLOCK BUY — HARD_RED market closes ordinary/group longs / market=HARD_RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **MNSO** | 🔴🔴🔴🟡🔴🔴 | Specialty Retail | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -5.2% |
| 2 | **BEPC** | 🔴🔴🔴🟡🔴🔴 | Utilities - Renewable | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.0% |
| 3 | **DKS** | 🔴🔴🔴🟡🔴🟡 | Specialty Retail | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -5.2% |
| 4 | **JKS** | 🔴🔴🔴🟡🔴🟡 | Solar | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -4.5% |
| 5 | **RGTI** | 🔴🔴🔴🟡🔴🔴 | Computer Hardware | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 6 | **AIIO** | 🔴🔴🔴🟡🔴🔴 | Auto Manufacturers | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 7 | **CSIQ** | 🔴🔴🔴🟡🔴🟡 | Solar | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -4.5% |
| 8 | **ORBS** | 🔴🔴🔴🟡🔴🔴 | Packaging & Containers | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 9 | **FCEL** | 🔴🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 10 | **ATOM** | 🔴🔴🔴🟡🔴🟡 | Semiconductor Equipment & Materials | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -6.0% |
| 11 | **NNBR** | 🔴🔴🔴🟡🔴🔴 | Conglomerates | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 12 | **DHC** | 🔴🔴🔴🟡🔴🔴 | REIT - Healthcare Facilities | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 13 | **VPG** | 🔴🔴🔴🟡🔴🟡 | Scientific & Technical Instruments | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -3.3% |
| 14 | **ARKO** | 🔴🔴🔴🟡🔴🟡 | Specialty Retail | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -5.2% |
| 15 | **NPWR** | 🔴🔴🔴🟡🔴🔴 | Specialty Industrial Machinery | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **finviz_tape** (36 captains, 15 industries → s_heat).
- Board file: `01_daily/map_heat/2026-08-31_map_heat.json` · generated 2026-08-31T03:48:25.433368-04:00

### Sector RS vs same-day LLM essay

| Sector | Finviz 1d | Finviz 1w | LLM 1d | Tape vs essay |
|--------|----------:|----------:|-------:|---------------|
| Basic Materials | -1.5% | -1.2% | -0.55 |  |
| Communication Services | +1.4% | +1.3% | — |  |
| Consumer Cyclical | +1.4% | -0.6% | -0.50 |  |
| Consumer Defensive | +0.6% | -0.3% | +0.28 |  |
| Energy | +0.1% | -2.1% | +0.53 | essay UP, tape DOWN |
| Financial | +0.3% | +0.9% | +0.25 |  |
| Healthcare | -0.8% | -2.2% | -0.55 |  |
| Industrials | -1.0% | -1.5% | -0.28 |  |
| Real Estate | -0.4% | -1.3% | -0.55 |  |
| Technology | -1.4% | +1.2% | -0.28 | essay DOWN, tape UP |
| Utilities | -1.1% | -0.3% | -0.28 |  |

### Industry heat (1w vs parent)

**HOT**

- **Infrastructure Operations** (Industrials) -1.0% 1d · +8.8% 1w · vs parent +10.3% · —
- **Software - Infrastructure** (Technology) +0.2% 1d · +5.3% 1w · vs parent +4.1% · MSFT, PLTR, ZETA, QLYS
- **Software - Application** (Technology) +0.4% 1d · +4.2% 1w · vs parent +3.0% · CRM, UBER, FROG, TTAN
- **Thermal Coal** (Energy) -0.6% 1d · +4.1% 1w · vs parent +6.3% · CNR, BTU
- **Coking Coal** (Basic Materials) -1.2% 1d · +3.5% 1w · vs parent +4.7% · HCC, AMR
- **Consumer Electronics** (Technology) +1.7% 1d · +3.4% 1w · vs parent +2.1% · AAPL, SONO
- **Pharmaceutical Retailers** (Healthcare) -1.7% 1d · +3.1% 1w · vs parent +5.3% · —
- **Publishing** (Communication Services) +0.1% 1d · +3.0% 1w · vs parent +1.7% · WLY, TDAY

**COLD**

- **Textile Manufacturing** (Consumer Cyclical) -1.6% 1d · -7.8% 1w · vs parent -7.2% · AIN
- **Engineering & Construction** (Industrials) -3.2% 1d · -5.8% 1w · vs parent -4.4% · PWR, FIX, ACA, FLR
- **Specialty Retail** (Consumer Cyclical) -1.1% 1d · -5.8% 1w · vs parent -5.2% · CASY, WSM, RH, ASO
- **Chemicals** (Basic Materials) +0.2% 1d · -5.1% 1w · vs parent -3.9% · DOW, HUN, REX
- **Semiconductor Equipment & Materials** (Technology) -4.0% 1d · -4.8% 1w · vs parent -6.0% · LRCX, AMAT, ACMR, KLIC
- **Utilities - Renewable** (Utilities) -3.3% 1d · -4.3% 1w · vs parent -4.0% · ORA, FLNC
- **Trucking** (Industrials) -1.3% 1d · -4.1% 1w · vs parent -2.6% · ODFL, RXO, ARCB
- **Rental & Leasing Services** (Industrials) -0.8% 1d · -4.1% 1w · vs parent -2.6% · URI, GATX, HRI

### Overrides (child 1w residual ≥ 3pp)

| Action | Industry | 1w | Parent 1w | Gap | Captains |
|--------|----------|---:|----------:|----:|----------|
| OVERRIDE | Infrastructure Operations | +8.8% | -1.5% | +10.3% | — |
| SPLIT | Textile Manufacturing | -7.8% | -0.6% | -7.2% | AIN |
| OVERRIDE | Thermal Coal | +4.1% | -2.1% | +6.3% | CNR, BTU |
| OVERRIDE | Semiconductor Equipment & Materials | -4.8% | +1.2% | -6.0% | LRCX, AMAT, ACMR, KLIC |
| OVERRIDE | Pharmaceutical Retailers | +3.1% | -2.2% | +5.3% | — |
| SPLIT | Specialty Retail | -5.8% | -0.6% | -5.2% | CASY, WSM, RH, ASO |
| OVERRIDE | Medical Distribution | +2.7% | -2.2% | +4.9% | MCK, COR |
| OVERRIDE | Coking Coal | +3.5% | -1.2% | +4.7% | HCC, AMR |
| OVERRIDE | Solar | -3.2% | +1.2% | -4.5% | FSLR, RUN, SHLS |
| SPLIT | Engineering & Construction | -5.8% | -1.5% | -4.4% | PWR, FIX, ACA, FLR |
| SPLIT | Software - Infrastructure | +5.3% | +1.2% | +4.1% | MSFT, PLTR, ZETA, QLYS |
| SPLIT | Utilities - Renewable | -4.3% | -0.3% | -4.0% | ORA, FLNC |
| OVERRIDE | Electronic Gaming & Multimedia | -2.7% | +1.3% | -4.0% | TTWO |
| SPLIT | Chemicals | -5.1% | -1.2% | -3.9% | DOW, HUN, REX |
| OVERRIDE | Steel | +2.6% | -1.2% | +3.8% | NUE, STLD, WS, NWPX |

### Theme join (sub-sector vs GICS parent)

- **Energy Traditional** — Oil / Majors: -1.5% 1w vs parent -2.1% → AGREE; Oil E&P: -3.6% 1w vs parent -2.1% → AGREE; Oil Services: +1.6% 1w vs parent -2.1% → **DIVERGE**; Nuclear: -1.4% 1w vs parent -2.1% → AGREE
- **Commodities Energy** — Uranium: -2.9% 1w vs parent -1.7% → AGREE; Oil (commodity): -3.7% 1w vs parent -1.7% → AGREE
- **Energy Renewable** — Solar: -3.2% 1w vs parent -0.4% → AGREE; Renewable utilities: -4.3% 1w vs parent -0.4% → AGREE
- **Commodities Metals** — Gold: -3.1% 1w vs parent -1.2% → AGREE; Silver: +1.0% 1w vs parent -1.2% → **DIVERGE**; Copper: -1.7% 1w vs parent -1.2% → AGREE; Other precious: -2.0% 1w vs parent -1.2% → AGREE
- **Semiconductors** — Semis: -0.3% 1w vs parent +1.2% → **DIVERGE**; Semi equipment: -4.8% 1w vs parent +1.2% → **DIVERGE**
- **Artificial Intelligence** — AI compute / semis: -0.3% 1w vs parent +1.2% → **DIVERGE**; Software infra: +5.3% 1w vs parent +1.2% → AGREE
- **Defense & Aerospace** — Aero / defense: -0.2% 1w vs parent -1.5% → AGREE

### Theme ETF tape (biggest |1w| moves)

| Theme | 1d | 1w | Leaders |
|-------|---:|---:|---------|
| Space Exploration & Technology | -2.3% | -5.0% | NASA, ARKX, UFO |
| Industrials | -1.3% | -2.5% | XLI, ITA, AIRR |
| Future Mobility Production & Tech | -0.7% | -2.2% | DRIV, ROKT, IDRV |
| Healthcare | -1.5% | -2.2% | XLV, VHT, XBI |
| Fintech | -4.9% | -1.6% | BLOK, ARKF, BITQ |
| Materials | -3.4% | -1.5% | GDX, GDXJ, XLB |
| Natural Resources | -0.6% | -1.5% | GUNR, GNR, PHO |
| Cannabis Based Businesses | +3.3% | +1.2% | MSOS, MJ, CNBS |
| Real Estate | -0.6% | -1.2% | VNQ, SCHH, XLRE |
| Consumer Discretionary | +0.8% | -1.1% | XLY, VCR, TSLL |
| Real Assets | -0.7% | -1.1% | ABLD, VRAI, CSRA |
| Communication Services | +1.1% | +1.0% | XLC, VOX, FCOM |

## Inputs this run — every resource

If a row says **missing**, that layer scored 0 today. If it says **found**, it moved the rank.

| Resource | This run | Where it lands in the score |
|----------|----------|-----------------------------|
| Finviz Elite export | **found** | liquidity + labels + AB proxy + digest |
| Labels / membership | **found** | join + mid_opp + earnings/range |
| Weather (tape + FRED/DXY/VIX) | **found** | join × weather |
| Channel 1 raw | **found** | via weather |
| Join ranked universe | **found** | s_join |
| News parse + actions | **found** | s_news |
| News judge | **found** | s_news ticker tilts |
| Finviz daily digest | **found** | s_news company headlines |
| General predict | **found** | s_general × beta |
| Sector LLM essays | **found** | s_sector (0 if essays missing) |
| AB checklist + P01–P04 | **found** | s_ab |
| Peer RS | **found** | s_peer |
| Ticker checklist (rebound) | **found** | rebound_floor (dated file, else latest — can be stale) |
| Event scanner | **found** | sector tilt + weather |
| Finviz map heat (industry RS / themes) | **found** | industry residual + theme tape → s_heat when research is gone |
| Map heat captain research | **missing / not in ranker** | Grok captain essays (strict morning_refresh; else Finviz tape) |
| Catalyst overlays | **missing / not in ranker** | not in ranker — separate chart workflow |
| Insider / politician flow | **missing / not in ranker** | no daily file in repo |
| Industry predict | **found** | not scored (ad-hoc only) |
| Learnings / mutable policy | **found** | next predict prompt, not a ticker score |

### Sector LLM bias (1d) — 0 / empty means that essay was not run today

| Sector | bias |
|--------|------|
| Basic Materials | -0.55 |
| Healthcare | -0.55 |
| Real Estate | -0.55 |
| Energy | +0.53 |
| Consumer Cyclical | -0.50 |
| Consumer Defensive | +0.28 |
| Industrials | -0.28 |
| Technology | -0.28 |
| Utilities | -0.28 |
| Financial | +0.25 |

### How much each predictor is trusted (graded hit rate)

| Topic | hit rate | n | weight |
|-------|----------|---|--------|
| general | 48% | 21 | ×0.85 |
| sector:Basic Materials | 58% | 12 | ×1.00 |
| sector:Communication Services | 25% | 12 | ×0.50 |
| sector:Consumer Cyclical | 58% | 12 | ×1.00 |
| sector:Consumer Defensive | 42% | 12 | ×0.50 |
| sector:Energy | 50% | 12 | ×0.85 |
| sector:Financial | 33% | 12 | ×0.50 |
| sector:Healthcare | 78% | 9 | ×1.00 |
| sector:Industrials | 25% | 12 | ×0.50 |
| sector:Real Estate | 58% | 12 | ×1.00 |
| sector:Technology | 42% | 12 | ×0.50 |
| sector:Utilities | 42% | 12 | ×0.50 |

## Horizon weights — book_policy.json v5

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.12 | 0.10 | 0.08 | 0.25 | 0.25 | 0.20 | additive |
| 3d | 0.16 | 0.14 | 0.08 | 0.16 | 0.26 | 0.20 | additive |
| 1w | 0.18 | 0.16 | 0.08 | 0.10 | 0.28 | 0.20 | additive |
| 2w | 0.20 | 0.18 | 0.08 | 0.06 | 0.28 | 0.20 | additive |
| 1m | 0.22 | 0.20 | 0.08 | 0.00 | 0.30 | 0.20 | additive |

## 1d BUY — why these names

_HARD_RED: general down score=-5.85; good=+0.5 vs bad=-7.0; risk=off; red pillars=5; no confirmed direct-company exception_


## 1d AVOID — bottom of the same rank

- **MNSO** (mid, Consumer Cyclical, $2.9B) score -0.422. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -5.2%
- **BEPC** (mid, Utilities, $5.9B) score -0.463. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.0%
- **DKS** (large, Consumer Cyclical, $12.2B) score -0.447. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -5.2%
- **JKS** (small, Technology, $701M) score -0.348. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -4.5%
- **RGTI** (mid, Technology, $5.2B) score -0.249. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow
- **AIIO** (small, Consumer Cyclical, $381M) score -0.545. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow
- **CSIQ** (small, Technology, $890M) score -0.236. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -4.5%
- **ORBS** (small, Consumer Cyclical, $344M) score -0.425. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow
- **FCEL** (small, Industrials, $1.4B) score -0.511. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow
- **ATOM** (micro, Technology, $166M) score -0.354. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -6.0%
- **NNBR** (micro, Industrials, $277M) score -0.459. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow
- **DHC** (small, Real Estate, $1.8B) score -0.534. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow
- **VPG** (small, Technology, $850M) score -0.288. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -3.3%
- **ARKO** (small, Consumer Cyclical, $499M) score -0.337. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -5.2%
- **NPWR** (micro, Industrials, $161M) score -0.401. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow
- **EVGO** (small, Consumer Cyclical, $437M) score -0.523. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -5.2%
- **COHU** (mid, Technology, $2.3B) score -0.100. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -6.0%
- **ADTN** (small, Technology, $620M) score -0.251. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow
- **TRT** (micro, Technology, $104M) score -0.379. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -6.0%
- **MTZ** (large, Industrials, $19.3B) score -0.420. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -4.4%
- **VLRS** (small, Industrials, $755M) score -0.427. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow
- **DQ** (small, Technology, $946M) score -0.131. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -4.5%
- **CAMT** (mid, Technology, $6.4B) score -0.093. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -6.0%
- **APG** (large, Industrials, $17.3B) score -0.328. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -4.4%
- **JBLU** (small, Industrials, $1.8B) score -0.333. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow

## 3d BUY (compact — same names, different weights)

_HARD_RED: general down score=-5.85; good=+0.5 vs bad=-7.0; risk=off; red pillars=5; no confirmed direct-company exception_


## 1w BUY (compact — same names, different weights)

_HARD_RED: general down score=-5.85; good=+0.5 vs bad=-7.0; risk=off; red pillars=5; no confirmed direct-company exception_


## 2w BUY (compact — same names, different weights)

_HARD_RED: general down score=-5.85; good=+0.5 vs bad=-7.0; risk=off; red pillars=5; no confirmed direct-company exception_


## 1m BUY — why these names

_HARD_RED: general down score=-5.85; good=+0.5 vs bad=-7.0; risk=off; red pillars=5; no confirmed direct-company exception_


## 1m AVOID — bottom of the same rank

- **HPP** (small, Real Estate, $729M) score -0.704. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **REAX** (small, Real Estate, $464M) score -0.690. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OPEN** (mid, Real Estate, $3.2B) score -0.688. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **DHC** (small, Real Estate, $1.8B) score -0.683. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **CMTG** (micro, Real Estate, $229M) score -0.672. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LUNR** (mid, Industrials, $2.7B) score -0.666. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BKSY** (small, Industrials, $970M) score -0.650. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SPIR** (small, Industrials, $528M) score -0.646. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RKLB** (large, Industrials, $38.2B) score -0.645. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RCAT** (small, Industrials, $1.3B) score -0.644. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **EH** (micro, Industrials, $259M) score -0.639. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FIP** (small, Industrials, $422M) score -0.629. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **BEPC** (mid, Utilities, $5.9B) score -0.622. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SHMD** (micro, Industrials, $207M) score -0.616. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **EVTL** (micro, Industrials, $118M) score -0.603. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TLN** (large, Utilities, $14.3B) score -0.603. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**
- **FCEL** (small, Industrials, $1.4B) score -0.603. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **MAC** (mid, Real Estate, $7.0B) score -0.602. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LWLG** (small, Basic Materials, $845M) score -0.601. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RDW** (mid, Industrials, $2.7B) score -0.595. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **JBGS** (small, Real Estate, $901M) score -0.594. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **STWD** (mid, Real Estate, $5.9B) score -0.594. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BIPC** (mid, Utilities, $5.3B) score -0.592. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FLNC** (mid, Utilities, $2.0B) score -0.591. this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PCT** (small, Industrials, $1.3B) score -0.587. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-08-31_stock_book.md`
- Machine table: `data/stock_book/2026-08-31_stock_book.csv`
- Machine book: `data/stock_book/2026-08-31_stock_book.json`
- Join rank: `data/join/2026-08-31_ranked.csv`
- Weather: `01_daily/weather/2026-08-31_weather.md`
- AB enrich: `data/ab_checklist/2026-08-31_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-08-31_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-08-31_map_heat.md`
