# Stock book — 2026-09-14

_Generated 2026-09-14T05:55:22.921265-04:00_

This file is the **human read** of one run. CSV/JSON next to it are the machine files.

## How today's action is built

**BUY is the green pile when it is thick enough** (every horizon, including 1d). A name is all-green when join / general / AB / peer are each ≥ +0.05, sector and news are yellow or missing (not red), and Finviz relvol is not in (0, 0.7) when printed. 1d still requires lattice `bull_eligible` so a hard-red market can empty the sleeve. SELL is core weights on the non-green remainder. The lattice is the thin-pile fallback and still writes the watch list:

1. **Market gate** — raw general factor scoreboard + risk state sets exposure. An extreme confirmed red day closes ordinary longs.
2. **Parent / child route** — sector tape/essay and independent industry/theme absolute + relative strength decide where.
3. **Company route** — News Judge adjudicates; actions, Finviz digest and dossiers form one deduplicated direct-event decision.
4. **Setup / flow gate** — intrinsic AB + join structure, peer RS, price/gap and time-aware relative volume decide whether now.
5. **Rank inside the lane** — standard, group-leader or catalyst. mid_opp cannot grant permission.

The existing red/yellow/green source graph remains visible. Its digest, judge and catalyst cells are now populated before selection. 🔵 / 🚨 / ⚪, Cond, region and featured fades remain gates. A second six-domain row prevents duplicate headlines from voting three times. Longer horizons use the same pile; they do not wait on a separate 1d lattice experiment.

## Today's regime

- Weather risk: **off**
- General predict (same-day): -0.72 down (present)
- Stand-down: **no** — 39 names qualified through catalyst_exception,probable (39 probable)
- Sector predicts this date: 10/11 (ok)
- News tickers in play: 96
- AB coverage: 1930 names · peer RS: 1827
- Universe after liquidity: 2057
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 43

## All-green BUY / SELL

- Mode: **weighted_fallback** · SELL **core_weights**
- Pile: **0** liquid all-green names (need ≥ 8) of 2057
- Core fired: join=yes, AB=yes, peer=yes
- pile 0 < 8 liquid all-green names. Fallback weighted walk; SELL stays on core

## Decision lattice — gate → route → rank

The weighted score is now a tie-breaker inside an eligible lane. It cannot average away a market, group, company, or setup veto.

### MARKET: 🔴 HARD_RED

- HARD_RED: general down score=-11.00; good=+0.0 vs bad=-12.0; risk=off; red pillars=6
- Allowed long lanes: **catalyst_exception, probable** · max slots 10 · size ×0.25
- Bear evidence: overnight catalysts -6.00 points; rates / Fed -3.00 points; global sessions -1.00 points; oil / dollar -1.00 points; volatility -0.75 points; futures -0.25 points

Decision domains: **MKT · parent · child · company · setup · flow**. Measured parent/child tape is kept separate from the LLM essay; direct company events must be price-confirmed on a hard-red day.

### Bull decisions (eligible or closest blocked cases)

| # | Ticker | Domains | Lane | Company / group | Decision |
|---:|--------|---------|------|-----------------|----------|
| 1 | **CENX** | 🔴🔴🟢🟡🟢🔴 | blocked | usable dossier Strong Bullish conv=67; Aluminum +2.9% d1 / +4.1% 1w / +4.4% vs parent | BLOCK BUY — HARD_RED: no company / child-outperform / lookback clock; parent sector RED; flow RED; direct catalyst lacks price confirmation; legacy Cond red / market=HARD_RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.80); setup=GREEN; flow=RED |
| 2 | **ORCL** | 🔴🔴🔴🟢🟢🔴 | blocked | direct high digest (same-day): Oracle posts Q1 revenue up 30%, OCI up 121%, raises FY27 guidance to at least $90B revenue; Software - Infrastructure -0.7% d1 / -3.1% 1w / -4.4% vs parent | BLOCK BUY — HARD_RED: no company / child-outperform / lookback clock; parent sector RED; child industry/theme RED; flow RED; direct catalyst lacks price confirmation; 🚨 alarm; featured fade; legacy Cond red; legacy region red; v2 domain region red / market=HARD_RED; parent=RED; child=RED/rel=RED; company=GREEN(0.72); setup=GREEN; flow=RED |
| 3 | **SM** | 🔴🟢🟢🟡🟢🟢 | probable | basket/action net=+6.80; context only, not a company catalyst; Oil & Gas E&P +0.9% d1 / +1.2% 1w / -0.4% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.40); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 4 | **CVE** | 🔴🟢🟢🟡🟢🟢 | probable | direct high digest (stale/undated): Cenovus Energy Q2 2026 non-GAAP EPS $1.08 misses estimates, revenue $14.7B beats, company raises full-year production guidance; Oil & Gas Integrated +1.6% d1 / +2.0% 1w / +0.4% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.48); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 5 | **FANG** | 🔴🟢🟢🟡🟢🟢 | probable | basket/action net=+6.80; context only, not a company catalyst; Oil & Gas E&P +0.9% d1 / +1.2% 1w / -0.4% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.40); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 6 | **KEYS** | 🔴🔴🟢🟡🟢🟢 | probable | direct high digest (stale/undated): Keysight posts record Q3 with EPS $3.07 (+78% YoY), revenue $1.846B (+36% YoY), orders +56%, raises outlook amid supply constraints; Scientific & Technical Instruments +1.5% d1 / +1.6% 1w / +0.3% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=RED; child=GREEN/rel=YELLOW; company=YELLOW(0.48); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 7 | **CNQ** | 🔴🟢🟢🟡🟢🟡 | probable | direct high digest (stale/undated): Canadian Natural Resources posts record Q2 2026 results with EPS $1.58, raises 2026 production guidance and returns about $4B to shareholders; Oil & Gas E&P +0.9% d1 / +1.2% 1w / -0.4% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.48); setup=GREEN; flow=YELLOW; lookback=🔵,Cond green |
| 8 | **DEC** | 🔴🟢🟢🟡🟢🟡 | probable | basket/action net=+4.76; context only, not a company catalyst; Oil & Gas Integrated +1.6% d1 / +2.0% 1w / +0.4% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.32); setup=GREEN; flow=YELLOW; lookback=🔵,Cond green |
| 9 | **CVI** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Oil & Gas Refining & Marketing +2.4% d1 / +5.9% 1w / +4.3% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +5.9% 1w / +4.3% rel — market=HARD_RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 10 | **APA** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Oil & Gas E&P +0.9% d1 / +1.2% 1w / -0.4% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 11 | **PBF** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Oil & Gas Refining & Marketing +2.4% d1 / +5.9% 1w / +4.3% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +5.9% 1w / +4.3% rel — market=HARD_RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 12 | **VLO** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Oil & Gas Refining & Marketing +2.4% d1 / +5.9% 1w / +4.3% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +5.9% 1w / +4.3% rel — market=HARD_RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 13 | **DK** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Oil & Gas Refining & Marketing +2.4% d1 / +5.9% 1w / +4.3% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +5.9% 1w / +4.3% rel — market=HARD_RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 14 | **BG** | 🔴🟡🟢🟡🟢🟢 | probable | no direct company event; Farm Products +1.5% d1 / +2.5% 1w / +3.5% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +2.5% 1w / +3.5% rel — market=HARD_RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 15 | **ADM** | 🔴🟡🟢🟡🟢🟢 | probable | no direct company event; Farm Products +1.5% d1 / +2.5% 1w / +3.5% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +2.5% 1w / +3.5% rel — market=HARD_RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **TNDM** | 🔴🔴🔴🟡🔴🔴 | Medical Devices | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.2% |
| 2 | **SERV** | 🔴🔴🔴🟡🔴🔴 | Integrated Freight & Logistics | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.6% |
| 3 | **GRRR** | 🔴🔴🔴🟡🔴🔴 | Software - Infrastructure | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.4% |
| 4 | **AHCO** | 🔴🔴🔴🟡🔴🔴 | Medical Devices | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.2% |
| 5 | **ARQQ** | 🔴🔴🔴🟡🔴🔴 | Software - Infrastructure | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.4% |
| 6 | **CIFR** | 🔴🔴🔴🟡🔴🔴 | Information Technology Services | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 7 | **TDTH** | 🔴🔴🔴🟡🔴🔴 | Information Technology Services | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 8 | **BTDR** | 🔴🔴🔴🟡🔴🔴 | Software - Application | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -8.3% |
| 9 | **SLQT** | 🔴🔴🔴🟡🔴🔴 | Insurance Brokers | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.9% |
| 10 | **OWL** | 🔴🔴🔴🟡🔴🔴 | Asset Management | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.2% |
| 11 | **WRD** | 🔴🔴🔴🟡🔴🔴 | Software - Application | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -8.3% |
| 12 | **BBAI** | 🔴🔴🔴🟡🔴🔴 | Information Technology Services | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 13 | **SPIR** | 🔴🔴🔴🟡🔴🔴 | Specialty Business Services | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.6% |
| 14 | **QTRX** | 🔴🔴🔴🟡🔴🔴 | Medical Devices | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.2% |
| 15 | **PACB** | 🔴🔴🔴🟡🔴🔴 | Medical Devices | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.2% |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **captain_research** (257 captains, 10 industries → s_heat).
- Board file: `01_daily/map_heat/2026-09-14_map_heat.json` · generated 2026-09-09T01:59:54.403331-04:00

### Sector RS vs same-day LLM essay

| Sector | Finviz 1d | Finviz 1w | LLM 1d | Tape vs essay |
|--------|----------:|----------:|-------:|---------------|
| Basic Materials | +0.0% | -0.3% | -0.85 |  |
| Communication Services | -0.3% | +0.7% | — |  |
| Consumer Cyclical | -0.7% | -1.8% | -0.85 |  |
| Consumer Defensive | -0.6% | -0.9% | +0.27 |  |
| Energy | +1.2% | +1.6% | +0.42 |  |
| Financial | -1.2% | +0.1% | -0.27 |  |
| Healthcare | -2.5% | -1.8% | -0.85 |  |
| Industrials | +0.3% | +1.1% | -0.42 | essay DOWN, tape UP |
| Real Estate | -0.2% | -0.5% | -0.44 |  |
| Technology | +0.2% | +1.3% | -0.42 | essay DOWN, tape UP |
| Utilities | +0.9% | +2.7% | -0.35 | essay DOWN, tape UP |

### Industry heat (1w vs parent)

**HOT**

- **Electrical Equipment & Parts** (Industrials) +4.5% 1d · +13.6% 1w · vs parent +12.5% · VRT, HUBB, ENS, ATKR
- **Textile Manufacturing** (Consumer Cyclical) -0.5% 1d · +10.5% 1w · vs parent +12.4% · AIN
- **Utilities - Independent Power Producers** (Utilities) +0.9% 1d · +8.9% 1w · vs parent +6.2% · CEG, VST, HNRG
- **Computer Hardware** (Technology) +1.8% 1d · +8.4% 1w · vs parent +7.0% · DELL, SNDK, QBTS, RGTI
- **Oil & Gas Refining & Marketing** (Energy) +2.4% 1d · +5.9% 1w · vs parent +4.3% · MPC, VLO, PBF, DK
- **Solar** (Technology) +3.6% 1d · +5.6% 1w · vs parent +4.3% · FSLR, RUN, SHLS
- **Electronic Components** (Technology) +2.1% 1d · +5.2% 1w · vs parent +3.9% · APH, GLW, PLXS, BELFB
- **Semiconductor Equipment & Materials** (Technology) +3.3% 1d · +5.0% 1w · vs parent +3.7% · LRCX, AMAT, ACMR, KLIC

**COLD**

- **Staffing & Employment Services** (Industrials) -5.6% 1d · -8.1% 1w · vs parent -9.1% · KFY, TNET
- **Consulting Services** (Industrials) -3.4% 1d · -7.5% 1w · vs parent -8.5% · VRSK, EFX, HURN, ICFI
- **Software - Application** (Technology) -3.5% 1d · -7.0% 1w · vs parent -8.3% · CRM, UBER, FROG, IDCC
- **Travel Services** (Consumer Cyclical) -3.5% 1d · -6.0% 1w · vs parent -4.2% · BKNG, ABNB, GBTG, LIND
- **Real Estate - Development** (Real Estate) -2.9% 1d · -5.2% 1w · vs parent -4.6% · CCS
- **Medical Devices** (Healthcare) -3.5% 1d · -5.0% 1w · vs parent -3.2% · ABT, MDT, GKOS, TXG
- **Broadcasting** (Communication Services) -3.1% 1d · -4.6% 1w · vs parent -5.3% · NMAX, FUBO
- **Residential Construction** (Consumer Cyclical) -3.4% 1d · -4.4% 1w · vs parent -2.6% · DHI, PHM, IBP, SKY

### Overrides (child 1w residual ≥ 3pp)

| Action | Industry | 1w | Parent 1w | Gap | Captains |
|--------|----------|---:|----------:|----:|----------|
| SPLIT | Electrical Equipment & Parts | +13.6% | +1.1% | +12.5% | VRT, HUBB, ENS, ATKR |
| OVERRIDE | Textile Manufacturing | +10.5% | -1.8% | +12.4% | AIN |
| OVERRIDE | Staffing & Employment Services | -8.1% | +1.1% | -9.1% | KFY, TNET |
| OVERRIDE | Consulting Services | -7.5% | +1.1% | -8.5% | VRSK, EFX, HURN, ICFI |
| OVERRIDE | Software - Application | -7.0% | +1.3% | -8.3% | CRM, UBER, FROG, IDCC |
| SPLIT | Computer Hardware | +8.4% | +1.3% | +7.0% | DELL, SNDK, QBTS, RGTI |
| OVERRIDE | Pharmaceutical Retailers | +5.0% | -1.8% | +6.7% | — |
| SPLIT | Utilities - Independent Power Producers | +8.9% | +2.7% | +6.2% | CEG, VST, HNRG |
| OVERRIDE | Broadcasting | -4.6% | +0.7% | -5.3% | NMAX, FUBO |
| OVERRIDE | Paper & Paper Products | +4.8% | -0.3% | +5.1% | SLVM |
| OVERRIDE | Information Technology Services | -3.4% | +1.3% | -4.7% | IBM, ACN, CIFR, PENG |
| OVERRIDE | Department Stores | +2.9% | -1.8% | +4.7% | KSS |
| SPLIT | Real Estate - Development | -5.2% | -0.5% | -4.6% | CCS |
| OVERRIDE | Aluminum | +4.1% | -0.3% | +4.4% | CENX, CSTM |
| OVERRIDE | Software - Infrastructure | -3.1% | +1.3% | -4.4% | MSFT, ORCL, ZETA, QLYS |

### Theme join (sub-sector vs GICS parent)

- **Energy Traditional** — Oil / Majors: +1.1% 1w vs parent +1.6% → AGREE; Oil E&P: +1.2% 1w vs parent +1.6% → AGREE; Oil Services: -0.5% 1w vs parent +1.6% → **DIVERGE**; Nuclear: +5.7% 1w vs parent +1.6% → AGREE
- **Commodities Energy** — Uranium: +2.4% 1w vs parent +0.6% → AGREE; Oil (commodity): +1.6% 1w vs parent +0.6% → AGREE
- **Energy Renewable** — Solar: +5.6% 1w vs parent +1.9% → AGREE; Renewable utilities: +4.2% 1w vs parent +1.9% → AGREE
- **Commodities Metals** — Gold: -0.3% 1w vs parent -0.3% → AGREE; Silver: +1.3% 1w vs parent -0.3% → **DIVERGE**; Copper: +0.9% 1w vs parent -0.3% → **DIVERGE**; Other precious: +2.2% 1w vs parent -0.3% → **DIVERGE**
- **Semiconductors** — Semis: +4.2% 1w vs parent +1.3% → AGREE; Semi equipment: +5.0% 1w vs parent +1.3% → AGREE
- **Artificial Intelligence** — AI compute / semis: +4.2% 1w vs parent +1.3% → AGREE; Software infra: -3.1% 1w vs parent +1.3% → **DIVERGE**
- **Defense & Aerospace** — Aero / defense: +1.3% 1w vs parent +1.1% → AGREE

### Theme ETF tape (biggest |1w| moves)

| Theme | 1d | 1w | Leaders |
|-------|---:|---:|---------|
| Fintech | +0.0% | +5.2% | BLOK, ARKF, BITQ |
| Agri-business | +0.0% | +4.7% | MOO, VEGI, FTAG |
| Consumer Discretionary | +0.0% | -2.1% | XLY, VCR, TSLL |
| Energy | +0.0% | +1.6% | XLE, AMLP, VDE |
| Consumer Staples | +0.0% | -1.2% | XLP, VDC, FSTA |
| Utilities | +0.0% | +1.2% | XLU, VPU, FUTY |
| Industrials | +0.0% | -1.1% | XLI, ITA, AIRR |
| Real Estate | +0.0% | -1.0% | VNQ, SCHH, XLRE |
| Technology | +0.0% | +0.8% | VGT, XLK, SMH |
| Cannabis Based Businesses | +0.0% | +0.8% | MSOS, MJ, CNBS |
| Natural Resources | +0.0% | +0.8% | GUNR, GNR, PHO |
| Materials | +0.0% | -0.6% | GDX, GDXJ, XLB |

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
| Map heat captain research | **found** | Grok captain essays (strict morning_refresh; else Finviz tape) |
| Catalyst overlays | **missing / not in ranker** | not in ranker — separate chart workflow |
| Insider / politician flow | **missing / not in ranker** | no daily file in repo |
| Industry predict | **found** | not scored (ad-hoc only) |
| Learnings / mutable policy | **missing / not in ranker** | next predict prompt, not a ticker score |

### Sector LLM bias (1d) — 0 / empty means that essay was not run today

| Sector | bias |
|--------|------|
| Basic Materials | -0.85 |
| Consumer Cyclical | -0.85 |
| Healthcare | -0.85 |
| Real Estate | -0.44 |
| Energy | +0.42 |
| Industrials | -0.42 |
| Technology | -0.42 |
| Utilities | -0.35 |
| Financial | -0.27 |
| Consumer Defensive | +0.27 |

### How much each predictor is trusted (graded hit rate)

| Topic | hit rate | n | weight |
|-------|----------|---|--------|
| general | 48% | 29 | ×0.85 |
| sector:Basic Materials | 56% | 18 | ×1.00 |
| sector:Communication Services | 28% | 18 | ×0.50 |
| sector:Consumer Cyclical | 61% | 18 | ×1.00 |
| sector:Consumer Defensive | 44% | 18 | ×0.50 |
| sector:Energy | 44% | 18 | ×0.50 |
| sector:Financial | 39% | 18 | ×0.50 |
| sector:Healthcare | 60% | 15 | ×1.00 |
| sector:Industrials | 28% | 18 | ×0.50 |
| sector:Real Estate | 50% | 18 | ×0.85 |
| sector:Technology | 41% | 17 | ×0.50 |
| sector:Utilities | 41% | 17 | ×0.50 |

## Horizon weights — book_policy.json v13

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.12 | 0.10 | 0.08 | 0.25 | 0.25 | 0.20 | additive |
| 3d | 0.16 | 0.14 | 0.08 | 0.16 | 0.26 | 0.20 | additive |
| 1w | 0.18 | 0.16 | 0.08 | 0.10 | 0.28 | 0.20 | additive |
| 2w | 0.20 | 0.18 | 0.08 | 0.06 | 0.28 | 0.20 | additive |
| 1m | 0.22 | 0.20 | 0.08 | 0.00 | 0.30 | 0.20 | additive |

## 1d BUY — why these names

### 1. CVE · $62.4B large · Energy

**1d score +0.309**

**CVE** is a liquid **large-cap** Energy name (Oil & Gas Integrated) at $62.4B, ADV ~7356k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **miss**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.95 | +0.114 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.22 | +0.022 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.11 | -0.009 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.31 | +0.077 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.88 | +0.220 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.02 | +0.003 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.309** | |


## 1d AVOID — bottom of the same rank

- **TNDM** (small, Healthcare, $1.2B) score -0.623. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.2%
- **SERV** (small, Industrials, $382M) score -0.605. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.6%
- **GRRR** (small, Technology, $360M) score -0.262. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.4%
- **AHCO** (small, Healthcare, $823M) score -0.593. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.2%
- **ARQQ** (small, Technology, $340M) score -0.255. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.4%
- **CIFR** (mid, Technology, $6.6B) score -0.376. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.7%
- **TDTH** (small, Technology, $1.8B) score -0.301. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.7%
- **BTDR** (mid, Technology, $3.1B) score -0.280. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -8.3%
- **SLQT** (micro, Financial, $88M) score -0.572. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.9%
- **OWL** (large, Financial, $16.4B) score -0.197. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.2%

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | DK | +0.587 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | CVE | +0.339 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | BG | +0.331 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 4 | NVT | +0.249 | large | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | AEIS | +0.041 | large | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | DK | +0.612 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | BG | +0.370 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 3 | CVE | +0.362 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | NVT | +0.272 | large | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | AEIS | +0.038 | large | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | DK | +0.623 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | BG | +0.396 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 3 | CVE | +0.379 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | NVT | +0.314 | large | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | AEIS | +0.059 | large | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1m BUY — why these names

### 1. DK · $4.7B mid · Energy

**1m score +0.648**

**DK** is a liquid **mid-cap** Energy name (Oil & Gas Refining & Marketing) at $4.7B, ADV ~1455k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.07 | +0.015 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.22 | +0.045 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.93 | +0.278 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.20 | +0.041 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.07 | +0.070 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.648** | |

### 2. BG · $23.8B large · Consumer Defensive

**1m score +0.434**

**BG** is a liquid **large-cap** Consumer Defensive name (Farm Products) at $23.8B, ADV ~1537k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.96 | +0.211 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.07 | +0.013 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.44 | +0.088 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1m total** | | | **+0.434** | |

### 3. CVE · $62.4B large · Energy

**1m score +0.402**

**CVE** is a liquid **large-cap** Energy name (Oil & Gas Integrated) at $62.4B, ADV ~7356k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **miss**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.95 | +0.209 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.22 | +0.045 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.31 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.02 | +0.003 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1m total** | | | **+0.402** | |

### 4. NVT · $25.1B large · Industrials

**1m score +0.334**

**NVT** is a liquid **large-cap** Industrials name (Electrical Equipment & Parts) at $25.1B, ADV ~2283k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.87 | +0.191 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.62 | -0.125 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.20 | +0.040 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.334** | |

### 5. AEIS · $11.1B large · Industrials

**1m score +0.053**

**AEIS** is a liquid **large-cap** Industrials name (Electrical Equipment & Parts) at $11.1B, ADV ~754k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.17 | -0.037 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.62 | -0.125 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.46 | +0.139 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.38 | +0.076 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.053** | |


## 1m AVOID — bottom of the same rank

- **SGML** (small, Basic Materials, $1.1B) score -0.891. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **METC** (small, Basic Materials, $649M) score -0.883. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FUN** (small, Consumer Cyclical, $1.4B) score -0.870. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **TMC** (small, Basic Materials, $1.7B) score -0.858. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CWH** (small, Consumer Cyclical, $621M) score -0.853. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BEAM** (mid, Healthcare, $2.4B) score -0.846. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ALMS** (small, Healthcare, $1.2B) score -0.843. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ASPI** (small, Basic Materials, $511M) score -0.841. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **DYN** (mid, Healthcare, $3.4B) score -0.808. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ACH** (micro, Healthcare, $83M) score -0.803. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LAR** (small, Basic Materials, $935M) score -0.800. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SNDX** (small, Healthcare, $1.6B) score -0.798. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BHVN** (small, Healthcare, $1.9B) score -0.798. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **WHR** (mid, Consumer Cyclical, $2.3B) score -0.794. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CRML** (small, Basic Materials, $917M) score -0.793. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CTMX** (small, Healthcare, $717M) score -0.792. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TNDM** (small, Healthcare, $1.2B) score -0.787. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OPEN** (mid, Real Estate, $2.6B) score -0.787. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NAMS** (mid, Healthcare, $2.7B) score -0.784. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ORGO** (micro, Healthcare, $192M) score -0.781. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **STLA** (large, Consumer Cyclical, $15.3B) score -0.780. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LODE** (micro, Basic Materials, $214M) score -0.780. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **XPOF** (micro, Consumer Cyclical, $202M) score -0.777. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OCGN** (small, Healthcare, $353M) score -0.776. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LVWR** (micro, Consumer Cyclical, $234M) score -0.774. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-09-14_stock_book.md`
- Machine table: `data/stock_book/2026-09-14_stock_book.csv`
- Machine book: `data/stock_book/2026-09-14_stock_book.json`
- Join rank: `data/join/2026-09-14_ranked.csv`
- Weather: `01_daily/weather/2026-09-14_weather.md`
- AB enrich: `data/ab_checklist/2026-09-14_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-09-14_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-09-14_map_heat.md`
