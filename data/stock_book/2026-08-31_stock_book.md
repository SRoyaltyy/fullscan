# Stock book — 2026-08-31

_Generated 2026-08-31T23:24:27.785157-04:00_

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
- Stand-down: **no** — 111 names qualified through catalyst_exception,probable (111 probable)
- Sector predicts this date: 10/11 (ok)
- News tickers in play: 118
- AB coverage: 2551 names · peer RS: 2400
- Universe after liquidity: 2685
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 105

## All-green BUY / SELL

- Mode: **green_pile** · SELL **core_weights_ex_green**
- Pile: **126** liquid all-green names (need ≥ 8) of 2685
- Core fired: join=yes, AB=yes, peer=yes
- pile 126 ≥ 8 liquid all-green names — BUY 15 from the pile by green_rank (no opp); SELL is core weights on the non-green remainder

## Decision lattice — gate → route → rank

The weighted score is now a tie-breaker inside an eligible lane. It cannot average away a market, group, company, or setup veto.

### MARKET: 🔴 HARD_RED

- HARD_RED: general down score=-5.85; good=+0.5 vs bad=-7.0; risk=off; red pillars=5
- Allowed long lanes: **catalyst_exception, probable** · max slots 10 · size ×0.25
- Bull evidence: sentiment +0.50 points
- Bear evidence: overnight catalysts -3.00 points; rates / Fed -2.00 points; oil / dollar -1.00 points; volatility -0.75 points; futures -0.25 points

Decision domains: **MKT · parent · child · company · setup · flow**. Measured parent/child tape is kept separate from the LLM essay; direct company events must be price-confirmed on a hard-red day.

### Bull decisions (eligible or closest blocked cases)

| # | Ticker | Domains | Lane | Company / group | Decision |
|---:|--------|---------|------|-----------------|----------|
| 1 | **CRM** | 🔴🔴🟢🟢🟢🟡 | probable | direct high digest (same-day): Salesforce beats Q2 guidance, raises FY27 outlook as analysts lift price targets after 'narrative-changing' results; Software - Application +0.4% d1 / +4.2% 1w / +3.0% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: company news fresh (0.88); child/theme outperform +4.2% 1w / +3.0% rel; lookback 🔵 blue — market=HARD_RED; parent=RED; child=GREEN/rel=GREEN; company=GREEN(0.88); setup=GREEN; flow=YELLOW; lookback=🔵,Cond green |
| 2 | **AMGN** | 🔴🔴🔴🟢🟢🔴 | probable | direct high digest (same-day): Amgen says Repatha cut all-cause mortality 20% in high-risk adults in Phase 3 VESALIUS-CV primary prevention analysis; Drug Manufacturers - General -0.4% d1 / -3.2% 1w / -1.0% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: company news fresh (0.82) — market=HARD_RED; parent=RED; child=RED/rel=YELLOW; company=GREEN(0.82); setup=GREEN; flow=RED; lookback=🔵 |
| 3 | **AON** | 🔴🟢🟢🟢🟢🟡 | probable | direct high digest (same-day): Aon names Nadin Virani interim CFO, reaffirms 2026 guidance, launches Sidecar X transactional risk platform with $200 million capacity; Insurance Brokers +1.7% d1 / +0.4% 1w / -0.5% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: company news fresh (0.72); lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=GREEN(0.72); setup=GREEN; flow=YELLOW; lookback=🔵,Cond green |
| 4 | **MPC** | 🔴🟢🟢🟡🟢🟢 | probable | direct normal digest (stale/undated): Mizuho raises Marathon Petroleum price target to $304 from $284, keeps Neutral after stronger-than-expected Q2 results; Oil & Gas Refining & Marketing +1.6% d1 / +1.0% 1w / +3.1% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +1.0% 1w / +3.1% rel; lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.30); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 5 | **BMO** | 🔴🟢🟢🟡🟢🟢 | probable | direct high digest (stale/undated): Bank of Montreal beats fiscal Q3 2026 estimates with non-GAAP EPS $2.86, revenue $7.2B, announces new share buyback plan and higher dividend; Banks - Diversified +0.9% d1 / +1.4% 1w / +0.5% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.48); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 6 | **CM** | 🔴🟢🟢🟡🟢🟡 | probable | direct high digest (stale/undated): Canadian Imperial Bank Of Commerce reports fiscal Q3 2026 results with non-GAAP EPS $1.97 (+25% YoY) and revenue $6.0B (+14% YoY), beats EPS and revenue estimates; Banks - Diversified +0.9% d1 / +1.4% 1w / +0.5% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.48); setup=GREEN; flow=YELLOW; lookback=🔵,Cond green |
| 7 | **AMZN** | 🔴🔴🟢🟡🟢🟡 | probable | direct normal digest (same-day): Evercore ISI's Mahaney raises Amazon 12‑month price target to $355 from $315, reiterates Buy on stronger retail trends; Internet Retail +3.5% d1 / +2.4% 1w / +3.0% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +2.4% 1w / +3.0% rel; lookback 🔵 blue — market=HARD_RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.42); setup=GREEN; flow=YELLOW; lookback=🔵,Cond green |
| 8 | **CVE** | 🔴🟢🟡🟡🟢🟢 | probable | direct high digest (stale/undated): Cenovus Energy Q2 2026 non-GAAP EPS $1.08 misses estimates, revenue $14.7B beats, company raises full-year production guidance; Oil & Gas Integrated +0.2% d1 / -3.9% 1w / -1.8% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=YELLOW/rel=YELLOW; company=YELLOW(0.48); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 9 | **LIN** | 🔴🔴🟢🟡🟢🟢 | probable | direct high digest (stale/undated): Linde posts record Q2 EPS, raises 2026 EPS guidance floor as project backlog grows about $1B to $8.1B; Specialty Chemicals +0.6% d1 / +0.1% 1w / +1.3% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=RED; child=GREEN/rel=YELLOW; company=YELLOW(0.48); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 10 | **CVX** | 🔴🟢🟡🟡🟢🟢 | probable | basket/action net=+4.20; context only, not a company catalyst; Oil & Gas Integrated +0.2% d1 / -3.9% 1w / -1.8% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=YELLOW/rel=YELLOW; company=YELLOW(0.28); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 11 | **RES** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Oil & Gas Equipment & Services +0.9% d1 / +1.6% 1w / +3.7% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +1.6% 1w / +3.7% rel — market=HARD_RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 12 | **APD** | 🔴🔴🟢🟡🟢🟢 | probable | direct high digest (stale/undated): Air Products beats fiscal Q3 2026 EPS with non-GAAP $3.47, raises FY26 EPS outlook, takes $2.9B clean energy exit charge; Specialty Chemicals +0.6% d1 / +0.1% 1w / +1.3% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=RED; child=GREEN/rel=YELLOW; company=YELLOW(0.48); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 13 | **DINO** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Oil & Gas Refining & Marketing +1.6% d1 / +1.0% 1w / +3.1% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +1.0% 1w / +3.1% rel — market=HARD_RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 14 | **NOV** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Oil & Gas Equipment & Services +0.9% d1 / +1.6% 1w / +3.7% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +1.6% 1w / +3.7% rel — market=HARD_RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 15 | **SLB** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Oil & Gas Equipment & Services +0.9% d1 / +1.6% 1w / +3.7% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +1.6% 1w / +3.7% rel — market=HARD_RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **MNSO** | 🔴🔴🔴🟡🔴🔴 | Specialty Retail | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -5.2% |
| 2 | **BEPC** | 🔴🔴🔴🟡🔴🔴 | Utilities - Renewable | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.0% |
| 3 | **DKS** | 🔴🔴🔴🟡🔴🟡 | Specialty Retail | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -5.2% |
| 4 | **AIIO** | 🔴🔴🔴🟡🔴🔴 | Auto Manufacturers | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 5 | **ORBS** | 🔴🔴🔴🟡🔴🔴 | Packaging & Containers | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 6 | **CSIQ** | 🔴🔴🔴🟡🔴🟡 | Solar | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -4.5% |
| 7 | **JKS** | 🔴🔴🔴🟡🔴🟡 | Solar | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -4.5% |
| 8 | **FCEL** | 🔴🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 9 | **NNBR** | 🔴🔴🔴🟡🔴🔴 | Conglomerates | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 10 | **DHC** | 🔴🔴🔴🟡🔴🔴 | REIT - Healthcare Facilities | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 11 | **ARKO** | 🔴🔴🔴🟡🔴🟡 | Specialty Retail | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -5.2% |
| 12 | **NPWR** | 🔴🔴🔴🟡🔴🔴 | Specialty Industrial Machinery | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 13 | **EVGO** | 🔴🔴🔴🟡🔴🟡 | Specialty Retail | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -5.2% |
| 14 | **RGTI** | 🔴🔴🔴🟡🔴🔴 | Computer Hardware | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 15 | **MTZ** | 🔴🔴🔴🟡🔴🟡 | Engineering & Construction | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -4.4% |

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
| Learnings / mutable policy | **missing / not in ranker** | next predict prompt, not a ticker score |

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

### 1. CRM · $209.0B mega · Technology

**1d score +0.337**

**CRM** is a liquid **mega-cap** Technology name (Software - Application) at $209.0B, ADV ~15585k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.88 | +0.106 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | -0.08 | -0.008 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.23 | -0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.46 | +0.115 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.95 | +0.239 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.97 | +0.194 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.34 | -0.340 | liquid small/mid, room to run |
| **1d total** | | | **+0.337** | |

### 2. AON · $75.4B large · Financial

**1d score +0.342**

**AON** is a liquid **large-cap** Financial name (Insurance Brokers) at $75.4B, ADV ~1393k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.91 | +0.110 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.45 | +0.045 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.76 | +0.190 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.01 | +0.003 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1d total** | | | **+0.342** | |

### 3. MPC · $105.2B large · Energy

**1d score +0.427**

**MPC** is a liquid **large-cap** Energy name (Oil & Gas Refining & Marketing) at $105.2B, ADV ~2365k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.97 | +0.117 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.33 | +0.033 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.31 | +0.077 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.93 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.17 | +0.034 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.11 | -0.110 | liquid small/mid, room to run |
| **1d total** | | | **+0.427** | |

### 4. BMO · $122.7B large · Financial

**1d score +0.289**

**BMO** is a liquid **large-cap** Financial name (Banks - Diversified) at $122.7B, ADV ~798k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.60 | +0.072 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.45 | +0.045 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.23 | -0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.31 | +0.077 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.10 | +0.021 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.289** | |

### 5. AMGN · $233.0B mega · Healthcare

**1d score +0.083**

**AMGN** is a liquid **mega-cap** Healthcare name (Drug Manufacturers - General) at $233.0B, ADV ~2690k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Today's **news/judge** is a headwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.96 | +0.116 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | -0.55 | -0.055 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | -0.31 | -0.077 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.36 | +0.090 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | -0.17 | -0.035 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1d total** | | | **+0.083** | |

### 6. CM · $105.9B large · Financial

**1d score +0.262**

**CM** is a liquid **large-cap** Financial name (Banks - Diversified) at $105.9B, ADV ~1345k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.64 | +0.076 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.45 | +0.045 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.23 | -0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.11 | +0.027 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.76 | +0.190 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | -0.29 | -0.058 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1d total** | | | **+0.262** | |

### 7. AMZN · $2859.0B mega · Consumer Cyclical

**1d score +0.332**

**AMZN** is a liquid **mega-cap** Consumer Cyclical name (Internet Retail) at $2859.0B, ADV ~49156k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Today's **news/judge** is a tailwind for this ticker.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.20 | +0.024 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | -0.70 | -0.070 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.47 | -0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.31 | +0.077 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.93 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.28 | +0.057 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1d total** | | | **+0.332** | |

### 8. CVE · $60.3B large · Energy

**1d score +0.266**

**CVE** is a liquid **large-cap** Energy name (Oil & Gas Integrated) at $60.3B, ADV ~7666k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **miss**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.33 | +0.033 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.31 | +0.077 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.46 | +0.116 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.24 | +0.047 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.266** | |

### 9. LIN · $225.7B mega · Basic Materials

**1d score +0.338**

**LIN** is a liquid **mega-cap** Basic Materials name (Specialty Chemicals) at $225.7B, ADV ~2305k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | -0.75 | -0.075 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.31 | +0.077 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.64 | +0.159 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.07 | +0.014 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1d total** | | | **+0.338** | |

### 10. CVX · $406.8B mega · Energy

**1d score +0.292**

**CVX** is a liquid **mega-cap** Energy name (Oil & Gas Integrated) at $406.8B, ADV ~8454k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.98 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.33 | +0.033 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.69 | +0.171 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.31 | +0.063 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.34 | -0.340 | liquid small/mid, room to run |
| **1d total** | | | **+0.292** | |


## 1d AVOID — bottom of the same rank

- **MNSO** (mid, Consumer Cyclical, $2.9B) score -0.437. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -5.2%
- **BEPC** (mid, Utilities, $5.9B) score -0.463. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.0%
- **DKS** (large, Consumer Cyclical, $12.2B) score -0.449. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -5.2%
- **AIIO** (small, Consumer Cyclical, $381M) score -0.550. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow
- **ORBS** (small, Consumer Cyclical, $344M) score -0.427. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow
- **CSIQ** (small, Technology, $890M) score -0.230. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -4.5%
- **JKS** (small, Technology, $701M) score -0.316. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -4.5%
- **FCEL** (small, Industrials, $1.4B) score -0.512. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow
- **NNBR** (micro, Industrials, $277M) score -0.460. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow
- **DHC** (small, Real Estate, $1.8B) score -0.535. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | RES | +0.697 | small | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 2 | NOV | +0.688 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 3 | PBF | +0.694 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | WTTR | +0.630 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 5 | BMO | +0.310 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 6 | VOD | +0.148 | large | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | RES | +0.726 | small | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 2 | NOV | +0.719 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 3 | PBF | +0.727 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | WTTR | +0.663 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 5 | BMO | +0.332 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 6 | VOD | +0.170 | large | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | RES | +0.674 | small | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 2 | NOV | +0.678 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 3 | PBF | +0.676 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | WTTR | +0.623 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 5 | BMO | +0.313 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 6 | VOD | +0.187 | large | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up** |

## 1m BUY — why these names

### 1. RES · $1.5B small · Energy

**1m score +0.788**

**RES** is a liquid **small-cap** Energy name (Oil & Gas Equipment & Services) at $1.5B, ADV ~1549k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.90 | +0.197 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.22 | +0.045 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.59 | +0.117 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.788** | |

### 2. NOV · $7.5B mid · Energy

**1m score +0.793**

**NOV** is a liquid **mid-cap** Energy name (Oil & Gas Equipment & Services) at $7.5B, ADV ~3822k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.83 | +0.183 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.22 | +0.045 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.55 | +0.110 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.793** | |

### 3. PBF · $8.5B mid · Energy

**1m score +0.794**

**PBF** is a liquid **mid-cap** Energy name (Oil & Gas Refining & Marketing) at $8.5B, ADV ~2884k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.90 | +0.199 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.22 | +0.045 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.96 | +0.289 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.30 | +0.061 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.794** | |

### 4. WTTR · $2.6B mid · Energy

**1m score +0.739**

**WTTR** is a liquid **mid-cap** Energy name (Oil & Gas Equipment & Services) at $2.6B, ADV ~1730k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.90 | +0.198 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.22 | +0.045 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.16 | +0.032 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.739** | |

### 5. BMO · $122.7B large · Financial

**1m score +0.327**

**BMO** is a liquid **large-cap** Financial name (Banks - Diversified) at $122.7B, ADV ~798k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.60 | +0.132 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.31 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.10 | +0.021 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1m total** | | | **+0.327** | |

### 6. VOD · $37.9B large · Communication Services

**1m score +0.208**

**VOD** is a liquid **large-cap** Communication Services name (Telecom Services) at $37.9B, ADV ~4001k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.59 | +0.129 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.46 | +0.139 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.30 | +0.060 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1m total** | | | **+0.208** | |


## 1m AVOID — bottom of the same rank

- **HPP** (small, Real Estate, $729M) score -0.706. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **REAX** (small, Real Estate, $464M) score -0.692. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OPEN** (mid, Real Estate, $3.2B) score -0.688. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **DHC** (small, Real Estate, $1.8B) score -0.684. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **CMTG** (micro, Real Estate, $229M) score -0.679. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LUNR** (mid, Industrials, $2.7B) score -0.666. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **EH** (micro, Industrials, $259M) score -0.657. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BKSY** (small, Industrials, $970M) score -0.650. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RKLB** (large, Industrials, $38.2B) score -0.647. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SPIR** (small, Industrials, $528M) score -0.646. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RCAT** (small, Industrials, $1.3B) score -0.644. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FIP** (small, Industrials, $422M) score -0.635. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **SHMD** (micro, Industrials, $207M) score -0.625. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BEPC** (mid, Utilities, $5.9B) score -0.622. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **EVTL** (micro, Industrials, $118M) score -0.620. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **JBGS** (small, Real Estate, $901M) score -0.605. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **MAC** (mid, Real Estate, $7.0B) score -0.604. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TLN** (large, Utilities, $14.3B) score -0.604. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**
- **FCEL** (small, Industrials, $1.4B) score -0.604. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **LWLG** (small, Basic Materials, $845M) score -0.603. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **KODK** (small, Industrials, $885M) score -0.603. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **RDW** (mid, Industrials, $2.7B) score -0.597. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **STWD** (mid, Real Estate, $5.9B) score -0.597. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BIPC** (mid, Utilities, $5.3B) score -0.593. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FLNC** (mid, Utilities, $2.0B) score -0.591. this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-08-31_stock_book.md`
- Machine table: `data/stock_book/2026-08-31_stock_book.csv`
- Machine book: `data/stock_book/2026-08-31_stock_book.json`
- Join rank: `data/join/2026-08-31_ranked.csv`
- Weather: `01_daily/weather/2026-08-31_weather.md`
- AB enrich: `data/ab_checklist/2026-08-31_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-08-31_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-08-31_map_heat.md`
