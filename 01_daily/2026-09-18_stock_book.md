# Stock book — 2026-09-18

_Generated 2026-09-19T23:19:15.630956-04:00_

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

- Weather risk: **on**
- General predict (same-day): +0.59 up (present)
- Stand-down: **no** — 433 names qualified through standard,group_leader,catalyst (79 probable)
- Sector predicts this date: 11/11 (ok)
- News tickers in play: 65
- AB coverage: 1897 names · peer RS: 1862
- Universe after liquidity: 2087
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 34

## All-green BUY / SELL

- Mode: **green_pile** · SELL **core_weights_ex_green**
- Pile: **429** liquid all-green names (need ≥ 8) of 2087
- Core fired: join=yes, AB=yes, peer=yes
- pile 429 ≥ 8 liquid all-green names — BUY 15 from the pile by green_rank (no opp); SELL is core weights on the non-green remainder

## Decision lattice — gate → route → rank

The weighted score is now a tie-breaker inside an eligible lane. It cannot average away a market, group, company, or setup veto.

### MARKET: 🟢 GREEN

- GREEN: general up score=+4.86; good=+2.2 vs bad=-4.0; risk=on; red pillars=1
- Allowed long lanes: **standard, group_leader, catalyst** · max slots 15 · size ×1.00
- Bull evidence: oil / dollar +1.00 points; futures +0.25 points
- Bear evidence: rates / Fed -3.00 points

Decision domains: **MKT · parent · child · company · setup · flow**. Measured parent/child tape is kept separate from the LLM essay; direct company events must be price-confirmed on a hard-red day.

### Bull decisions (eligible or closest blocked cases)

| # | Ticker | Domains | Lane | Company / group | Decision |
|---:|--------|---------|------|-----------------|----------|
| 1 | **LLY** | 🟢🟢🟡🟢🟢🟢 | catalyst | direct high digest (same-day): U.S. FDA grants full approval to Eli Lilly's Inluriyo plus Verzenio for ER+, HER2-, ESR1-mutated advanced breast cancer; Drug Manufacturers - General -0.1% d1 / +2.3% 1w / +0.8% vs parent | BUY CATALYST — market=GREEN; parent=GREEN; child=YELLOW/rel=YELLOW; company=GREEN(0.72); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 2 | **GEV** | 🟢🟡🟡🟢🟡🟢 | catalyst | direct high digest (same-day): GE Vernova settles over $300 million Vineyard Wind payment dispute, withdraws contract-termination notice and legal claims on Massachusetts offshore project; Specialty Industrial Machinery +1.3% d1 / -1.2% 1w / +0.1% vs parent | BUY CATALYST — market=GREEN; parent=YELLOW; child=YELLOW/rel=YELLOW; company=GREEN(0.72); setup=YELLOW; flow=GREEN; lookback=🔵,⚪,Cond green |
| 3 | **AMGN** | 🟢🟢🟡🟢🟢🟢 | catalyst | direct high digest (same-day): Amgen gets FDA approval to update IMDELLTRA label to reduce monitoring for first two ES-SCLC doses; Drug Manufacturers - General -0.1% d1 / +2.3% 1w / +0.8% vs parent | BUY CATALYST — market=GREEN; parent=GREEN; child=YELLOW/rel=YELLOW; company=GREEN(0.72); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 4 | **SB** | 🟢🟡🟢🟡🟢🟢 | group_leader | no direct company event; Marine Shipping +0.1% d1 / +2.4% 1w / +3.7% vs parent | BUY GROUP_LEADER — market=GREEN; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 5 | **DSX** | 🟢🟡🟢🟡🟢🟢 | group_leader | no direct company event; Marine Shipping +0.1% d1 / +2.4% 1w / +3.7% vs parent | BUY GROUP_LEADER — market=GREEN; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 6 | **ECO** | 🟢🟡🟢🟡🟢🟢 | group_leader | no direct company event; Marine Shipping +0.1% d1 / +2.4% 1w / +3.7% vs parent | BUY GROUP_LEADER — market=GREEN; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 7 | **SFL** | 🟢🟡🟢🟡🟢🟡 | group_leader | no direct company event; Marine Shipping +0.1% d1 / +2.4% 1w / +3.7% vs parent | BUY GROUP_LEADER — market=GREEN; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,⚪,Cond green |
| 8 | **QCOM** | 🟢🟢🟢🟡🟢🟢 | standard | direct normal digest (same-day): RBC Capital Markets raises Qualcomm price target to $180 from $160, keeps Sector Perform on up-to-$60B AWS AI data-center chip deal; Semiconductors +1.9% d1 / +1.5% 1w / +0.7% vs parent | BUY STANDARD — market=GREEN; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.42); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 9 | **SBLK** | 🟢🟡🟢🟡🟢🟢 | group_leader | no direct company event; Marine Shipping +0.1% d1 / +2.4% 1w / +3.7% vs parent | BUY GROUP_LEADER — market=GREEN; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 10 | **ZIM** | 🟢🟡🟢🟡🟢🟡 | group_leader | no direct company event; Marine Shipping +0.1% d1 / +2.4% 1w / +3.7% vs parent | BUY GROUP_LEADER — market=GREEN; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,⚪,Cond green |
| 11 | **ADI** | 🟢🟢🟢🟡🟢🟢 | standard | basket/action net=+5.50; context only, not a company catalyst; Semiconductors +1.9% d1 / +1.5% 1w / +0.7% vs parent | BUY STANDARD — market=GREEN; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.37); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 12 | **NVDA** | 🟢🟢🟢🟡🟢🟢 | standard | basket/action net=+4.00; context only, not a company catalyst; Semiconductors +1.9% d1 / +1.5% 1w / +0.7% vs parent | BUY STANDARD — market=GREEN; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.27); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 13 | **SVM** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Silver +0.6% d1 / +1.1% 1w / +3.1% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 14 | **AYA** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Silver +0.6% d1 / +1.1% 1w / +3.1% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 15 | **MRVL** | 🟢🟢🟢🟡🟢🟢 | standard | no direct company event; Semiconductors +1.9% d1 / +1.5% 1w / +0.7% vs parent | BUY STANDARD — market=GREEN; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **METC** | 🟢🔴🔴🟡🔴🔴 | Coking Coal | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -10.1% |
| 2 | **LDI** | 🟢🔴🔴🟡🔴🔴 | Mortgage Finance | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -4.5% |
| 3 | **EU** | 🟢🔴🔴🟡🔴🔴 | Uranium | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -3.8% |
| 4 | **TTI** | 🟢🔴🔴🟡🔴🔴 | Oil & Gas Equipment & Services | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -3.9% |
| 5 | **UUUU** | 🟢🔴🔴🟡🔴🔴 | Uranium | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -3.8% |
| 6 | **RKT** | 🟢🔴🔴🟡🔴🔴 | Mortgage Finance | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -4.5% |
| 7 | **BTU** | 🟢🔴🔴🟡🔴🔴 | Thermal Coal | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -4.5% |
| 8 | **HAL** | 🟢🔴🔴🟡🔴🔴 | Oil & Gas Equipment & Services | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -3.9% |
| 9 | **BETR** | 🟢🔴🔴🟡🔴🔴 | Mortgage Finance | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -4.5% |
| 10 | **URG** | 🟢🔴🔴🟡🔴🔴 | Uranium | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -3.8% |
| 11 | **AA** | 🟢🔴🔴🟡🔴🔴 | Aluminum | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 12 | **VALE** | 🟢🔴🔴🟡🔴🔴 | Other Industrial Metals & Mining | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow |
| 13 | **NRG** | 🟢🔴🔴🟡🔴🔴 | Utilities - Independent Power Producers | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -5.1% |
| 14 | **PFSI** | 🟢🔴🔴🟡🔴🔴 | Mortgage Finance | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -4.5% |
| 15 | **PUMP** | 🟢🔴🔴🟡🔴🟢 | Oil & Gas Equipment & Services | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -3.9% |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **captain_research** (287 captains, 6 industries → s_heat).
- Board file: `01_daily/map_heat/2026-09-18_map_heat.json` · generated 2026-09-19T01:48:06.648688-04:00

### Sector RS vs same-day LLM essay

| Sector | Finviz 1d | Finviz 1w | LLM 1d | Tape vs essay |
|--------|----------:|----------:|-------:|---------------|
| Basic Materials | -0.9% | -2.0% | +0.00 | essay flat, tape moving |
| Communication Services | -0.7% | +0.8% | +0.32 |  |
| Consumer Cyclical | -0.1% | -1.6% | +0.00 | essay flat, tape moving |
| Consumer Defensive | -0.6% | -0.7% | +0.20 |  |
| Energy | -0.5% | -1.6% | -0.55 |  |
| Financial | -0.1% | -2.2% | +0.00 | essay flat, tape moving |
| Healthcare | -0.4% | +1.6% | +0.51 |  |
| Industrials | -0.0% | -1.3% | +0.00 |  |
| Real Estate | -0.9% | -2.3% | +0.00 | essay flat, tape moving |
| Technology | +0.8% | +0.8% | +0.00 |  |
| Utilities | -1.3% | -2.7% | +0.00 | essay flat, tape moving |

### Industry heat (1w vs parent)

**HOT**

- **Diagnostics & Research** (Healthcare) -0.8% 1d · +6.4% 1w · vs parent +4.8% · TMO, DHR, RDNT, ADPT
- **Oil & Gas Refining & Marketing** (Energy) -0.0% 1d · +5.5% 1w · vs parent +7.0% · MPC, VLO, PBF, CVI
- **Health Information Services** (Healthcare) -1.1% 1d · +3.9% 1w · vs parent +2.3% · VEEV, BTSG, HQY
- **Infrastructure Operations** (Industrials) +0.0% 1d · +2.9% 1w · vs parent +4.1% · —
- **Internet Content & Information** (Communication Services) -0.4% 1d · +2.7% 1w · vs parent +1.9% · GOOGL, GOOG, RUM, CARG
- **Computer Hardware** (Technology) +3.1% 1d · +2.5% 1w · vs parent +1.7% · DELL, SNDK, QBTS, RGTI
- **Marine Shipping** (Industrials) +0.1% 1d · +2.4% 1w · vs parent +3.7% · MATX, SFL
- **Publishing** (Communication Services) -0.8% 1d · +2.4% 1w · vs parent +1.6% · WLY, TDAY

**COLD**

- **Coking Coal** (Basic Materials) -4.5% 1d · -12.0% 1w · vs parent -10.1% · HCC, AMR
- **Gambling** (Consumer Cyclical) -2.8% 1d · -9.4% 1w · vs parent -7.8% · SGHC, RSI
- **Business Equipment & Supplies** (Industrials) -0.7% 1d · -8.9% 1w · vs parent -7.6% · CXT, XRX
- **Broadcasting** (Communication Services) -2.7% 1d · -7.8% 1w · vs parent -8.5% · NMAX, FUBO
- **Utilities - Independent Power Producers** (Utilities) -2.5% 1d · -7.8% 1w · vs parent -5.1% · CEG, VST, HNRG
- **Aluminum** (Basic Materials) -4.5% 1d · -7.4% 1w · vs parent -5.4% · CENX, CSTM
- **Mortgage Finance** (Financial) -2.0% 1d · -6.7% 1w · vs parent -4.5% · PFSI, WD
- **Auto & Truck Dealerships** (Consumer Cyclical) -1.4% 1d · -6.7% 1w · vs parent -5.1% · CVNA, RUSHA, OPLN

### Overrides (child 1w residual ≥ 3pp)

| Action | Industry | 1w | Parent 1w | Gap | Captains |
|--------|----------|---:|----------:|----:|----------|
| SPLIT | Coking Coal | -12.0% | -2.0% | -10.1% | HCC, AMR |
| OVERRIDE | Broadcasting | -7.8% | +0.8% | -8.5% | NMAX, FUBO |
| SPLIT | Gambling | -9.4% | -1.6% | -7.8% | SGHC, RSI |
| SPLIT | Business Equipment & Supplies | -8.9% | -1.3% | -7.6% | CXT, XRX |
| OVERRIDE | Electronic Components | -6.6% | +0.8% | -7.4% | APH, GLW, PLXS, BELFA |
| OVERRIDE | Oil & Gas Refining & Marketing | +5.5% | -1.6% | +7.0% | MPC, VLO, PBF, CVI |
| OVERRIDE | Telecom Services | -4.7% | +0.8% | -5.4% | VZ, TMUS, LUMN, TDS |
| SPLIT | Aluminum | -7.4% | -2.0% | -5.4% | CENX, CSTM |
| OVERRIDE | Solar | -4.4% | +0.8% | -5.2% | FSLR, RUN, SHLS |
| SPLIT | Utilities - Independent Power Producers | -7.8% | -2.7% | -5.1% | CEG, VST, HNRG |
| SPLIT | Auto & Truck Dealerships | -6.7% | -1.6% | -5.1% | CVNA, RUSHA, OPLN |
| OVERRIDE | Entertainment | -4.2% | +0.8% | -5.0% | NFLX, DIS, SPHR, CNK |
| SPLIT | Diagnostics & Research | +6.4% | +1.6% | +4.8% | TMO, DHR, RDNT, ADPT |
| OVERRIDE | Advertising Agencies | -4.0% | +0.8% | -4.7% | APP, OMC, MGNI, DV |
| SPLIT | Mortgage Finance | -6.7% | -2.2% | -4.5% | PFSI, WD |

### Theme join (sub-sector vs GICS parent)

- **Energy Traditional** — Oil / Majors: -1.1% 1w vs parent -1.6% → AGREE; Oil E&P: -3.9% 1w vs parent -1.6% → AGREE; Oil Services: -5.5% 1w vs parent -1.6% → AGREE; Nuclear: -6.5% 1w vs parent -1.6% → AGREE
- **Commodities Energy** — Uranium: -5.3% 1w vs parent -1.8% → AGREE; Oil (commodity): -2.8% 1w vs parent -1.8% → AGREE
- **Energy Renewable** — Solar: -4.4% 1w vs parent -1.1% → AGREE; Renewable utilities: -0.5% 1w vs parent -1.1% → AGREE
- **Commodities Metals** — Gold: -1.6% 1w vs parent -2.0% → AGREE; Silver: +1.1% 1w vs parent -2.0% → **DIVERGE**; Copper: +0.7% 1w vs parent -2.0% → **DIVERGE**; Other precious: -3.0% 1w vs parent -2.0% → AGREE
- **Semiconductors** — Semis: +1.5% 1w vs parent +0.8% → AGREE; Semi equipment: -2.1% 1w vs parent +0.8% → **DIVERGE**
- **Artificial Intelligence** — AI compute / semis: +1.5% 1w vs parent +0.8% → AGREE; Software infra: +1.4% 1w vs parent +0.8% → AGREE
- **Defense & Aerospace** — Aero / defense: -0.6% 1w vs parent -1.3% → AGREE

### Theme ETF tape (biggest |1w| moves)

| Theme | 1d | 1w | Leaders |
|-------|---:|---:|---------|
| Utilities | -1.3% | -2.7% | XLU, VPU, FUTY |
| Cannabis Based Businesses | -1.4% | +2.6% | MSOS, MJ, CNBS |
| Fintech | +4.8% | +2.1% | BLOK, ARKF, DAPP |
| Materials | -0.8% | -2.1% | GDX, GDXJ, XLB |
| Battery and Energy Storage | -0.7% | -2.0% | LIT, BATT, IBAT |
| Real Estate | -1.0% | -2.0% | VNQ, SCHH, XLRE |
| Energy | -0.4% | -1.9% | XLE, AMLP, VDE |
| Agri-business | -1.4% | -1.9% | MOO, VEGI, KROP |
| Future Mobility Production & Tech | -0.8% | -1.9% | DRIV, IDRV, ROKT |
| Natural Resources | -1.1% | -1.7% | GUNR, GNR, PHO |
| Real Assets | -0.6% | -1.6% | ABLD, CSRA, VRAI |
| Industrials | -0.5% | -1.5% | XLI, ITA, AIRR |

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
| Learnings / mutable policy | **found** | next predict prompt, not a ticker score |

### Sector LLM bias (1d) — 0 / empty means that essay was not run today

| Sector | bias |
|--------|------|
| Energy | -0.55 |
| Healthcare | +0.51 |
| Communication Services | +0.32 |
| Consumer Defensive | +0.20 |
| Basic Materials | +0.00 |
| Consumer Cyclical | +0.00 |
| Financial | +0.00 |
| Industrials | +0.00 |
| Real Estate | +0.00 |
| Technology | +0.00 |
| Utilities | +0.00 |

### How much each predictor is trusted (graded hit rate)

| Topic | hit rate | n | weight |
|-------|----------|---|--------|
| general | 53% | 34 | ×0.85 |
| sector:Basic Materials | 48% | 23 | ×0.85 |
| sector:Communication Services | 23% | 22 | ×0.50 |
| sector:Consumer Cyclical | 52% | 23 | ×0.85 |
| sector:Consumer Defensive | 44% | 23 | ×0.50 |
| sector:Energy | 52% | 23 | ×0.85 |
| sector:Financial | 48% | 23 | ×0.85 |
| sector:Healthcare | 50% | 20 | ×0.85 |
| sector:Industrials | 35% | 23 | ×0.50 |
| sector:Real Estate | 48% | 23 | ×0.85 |
| sector:Technology | 38% | 21 | ×0.50 |
| sector:Utilities | 38% | 21 | ×0.50 |

## Horizon weights — book_policy.json v15

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.12 | 0.10 | 0.08 | 0.25 | 0.25 | 0.20 | additive |
| 3d | 0.16 | 0.14 | 0.08 | 0.16 | 0.26 | 0.20 | additive |
| 1w | 0.18 | 0.16 | 0.08 | 0.10 | 0.28 | 0.20 | additive |
| 2w | 0.20 | 0.18 | 0.08 | 0.06 | 0.28 | 0.20 | additive |
| 1m | 0.22 | 0.20 | 0.08 | 0.00 | 0.30 | 0.20 | additive |

## 1d BUY — why these names

### 1. VICR · $10.3B mid · Technology

**1d score +0.846**

**VICR** is a liquid **mid-cap** Technology name (Electronic Components) at $10.3B, ADV ~872k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.59 | +0.047 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.99 | +0.198 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.846** | |

### 2. RBRK · $22.1B large · Technology

**1d score +0.439**

**RBRK** is a liquid **large-cap** Technology name (Software - Infrastructure) at $22.1B, ADV ~3407k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.29 | +0.024 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.92 | +0.185 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.439** | |

### 3. ECO · $3.3B mid · Industrials

**1d score +0.779**

**ECO** is a liquid **mid-cap** Industrials name (Marine Shipping) at $3.3B, ADV ~551k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.98 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.16 | +0.016 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.09 | +0.007 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.93 | +0.185 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.779** | |

### 4. DELL · $361.2B mega · Technology

**1d score +0.313**

**DELL** is a liquid **mega-cap** Technology name (Computer Hardware) at $361.2B, ADV ~7832k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.59 | +0.047 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.87 | +0.173 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.03 | -0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.28 | -0.280 | liquid small/mid, room to run |
| **1d total** | | | **+0.313** | |

### 5. FIVN · $2.4B mid · Technology

**1d score +0.766**

**FIVN** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $2.4B, ADV ~2581k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.59 | +0.047 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.89 | +0.178 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.766** | |

### 6. SDGR · $2.2B mid · Healthcare

**1d score +0.798**

**SDGR** is a liquid **mid-cap** Healthcare name (Health Information Services) at $2.2B, ADV ~1356k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extreme**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.91 | +0.109 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.51 | +0.051 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.59 | +0.047 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.76 | +0.190 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +1.00 | +0.200 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.798** | |

### 7. GNRC · $12.2B large · Industrials

**1d score +0.639**

**GNRC** is a liquid **large-cap** Industrials name (Specialty Industrial Machinery) at $12.2B, ADV ~1171k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.96 | +0.115 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.16 | +0.016 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.59 | +0.047 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.76 | +0.190 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.95 | +0.190 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1d total** | | | **+0.639** | |

### 8. ILMN · $36.2B large · Healthcare

**1d score +0.487**

**ILMN** is a liquid **large-cap** Healthcare name (Diagnostics & Research) at $36.2B, ADV ~2019k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.93 | +0.112 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.51 | +0.051 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.59 | +0.047 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.93 | +0.186 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.11 | -0.110 | liquid small/mid, room to run |
| **1d total** | | | **+0.487** | |

### 9. WAY · $4.9B mid · Healthcare

**1d score +0.760**

**WAY** is a liquid **mid-cap** Healthcare name (Health Information Services) at $4.9B, ADV ~2748k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.89 | +0.107 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.51 | +0.051 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.09 | +0.007 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.96 | +0.192 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.760** | |

### 10. XMTR · $5.6B mid · Industrials

**1d score +0.748**

**XMTR** is a liquid **mid-cap** Industrials name (Industrial Distribution) at $5.6B, ADV ~723k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.98 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.16 | +0.016 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.29 | +0.024 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.70 | +0.176 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.82 | +0.164 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.748** | |

### 11. BRKR · $9.4B mid · Healthcare

**1d score +0.800**

**BRKR** is a liquid **mid-cap** Healthcare name (Medical Devices) at $9.4B, ADV ~2229k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.80 | +0.096 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.51 | +0.051 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.29 | +0.024 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.89 | +0.177 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.800** | |

### 12. SFL · $2.1B mid · Industrials

**1d score +0.730**

**SFL** is a liquid **mid-cap** Industrials name (Marine Shipping) at $2.1B, ADV ~1478k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.97 | +0.116 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.16 | +0.016 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.09 | +0.007 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.50 | +0.101 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.730** | |

### 13. GLBE · $6.4B mid · Consumer Cyclical

**1d score +0.702**

**GLBE** is a liquid **mid-cap** Consumer Cyclical name (Internet Retail) at $6.4B, ADV ~1475k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.95 | +0.114 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.08 | +0.008 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.29 | +0.024 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.52 | +0.104 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.702** | |

### 14. M · $5.7B mid · Consumer Cyclical

**1d score +0.720**

**M** is a liquid **mid-cap** Consumer Cyclical name (Department Stores) at $5.7B, ADV ~5609k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.91 | +0.109 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.08 | +0.008 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.59 | +0.047 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.76 | +0.190 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.58 | +0.115 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.720** | |


## 1d AVOID — bottom of the same rank

- **SOC** (small, Energy, $875M) score -0.447. SELL/AVOID — market=GREEN; red domains=parent,child,setup
- **EU** (micro, Energy, $174M) score -0.405. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -3.8%
- **UUUU** (mid, Energy, $3.1B) score -0.397. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -3.8%
- **PUMP** (small, Energy, $1.3B) score -0.396. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -3.9%
- **GS** (mega, Financial, $274.3B) score -0.386. SELL/AVOID — market=GREEN; red domains=parent,flow
- **MS** (mega, Financial, $318.2B) score -0.376. NO BEAR — market=GREEN; red domains=parent
- **WFRD** (mid, Energy, $6.0B) score -0.364. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -3.9%
- **BTU** (mid, Energy, $3.1B) score -0.339. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -4.5%
- **URG** (small, Energy, $473M) score -0.325. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -3.8%
- **TTI** (small, Energy, $900M) score -0.314. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -3.9%
- **AR** (large, Energy, $10.7B) score -0.307. SELL/AVOID — market=GREEN; red domains=parent,child,flow
- **XPRO** (small, Energy, $1.8B) score -0.302. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -3.9%
- **HP** (mid, Energy, $4.1B) score -0.297. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow
- **KGS** (mid, Energy, $5.9B) score -0.287. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -3.9%
- **C** (mega, Financial, $221.0B) score -0.287. SELL/AVOID — market=GREEN; red domains=parent,child,flow
- **VALE** (large, Basic Materials, $57.9B) score -0.278. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow
- **CBOE** (large, Financial, $28.5B) score -0.262. SELL/AVOID — market=GREEN; red domains=parent,setup
- **LDI** (micro, Financial, $236M) score -0.260. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -4.5%
- **CRK** (mid, Energy, $3.8B) score -0.258. SELL/AVOID — market=GREEN; red domains=parent,child,flow
- **CTVA** (large, Basic Materials, $53.7B) score -0.256. SELL/AVOID — market=GREEN; red domains=parent,child,flow
- **WTI** (small, Energy, $580M) score -0.254. SELL/AVOID — market=GREEN; red domains=parent,child,flow
- **OSG** (micro, Financial, $203M) score -0.253. SELL/AVOID — market=GREEN; red domains=parent,child,setup
- **BHP** (mega, Basic Materials, $219.3B) score -0.248. SELL/AVOID — market=GREEN; red domains=parent,child
- **JEF** (large, Financial, $11.0B) score -0.246. SELL/AVOID — market=GREEN; red domains=parent,setup
- **UEC** (mid, Energy, $4.9B) score -0.238. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -3.8%

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | VICR | +0.892 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 2 | RBRK | +0.490 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | ECO | +0.831 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | DELL | +0.359 | mega | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | FIVN | +0.812 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | SDGR | +0.852 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | GNRC | +0.681 | large | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 8 | ILMN | +0.543 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | WAY | +0.822 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 10 | XMTR | +0.796 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 11 | BRKR | +0.855 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | SFL | +0.783 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | GLBE | +0.747 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 14 | M | +0.758 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | VICR | +0.895 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 2 | RBRK | +0.512 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | ECO | +0.865 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | DELL | +0.362 | mega | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | FIVN | +0.814 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | SDGR | +0.858 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | GNRC | +0.682 | large | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 8 | ILMN | +0.551 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | WAY | +0.861 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 10 | XMTR | +0.814 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 11 | BRKR | +0.879 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | SFL | +0.817 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | GLBE | +0.765 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 14 | M | +0.755 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | VICR | +0.884 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 2 | RBRK | +0.518 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | ECO | +0.882 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | DELL | +0.351 | mega | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | FIVN | +0.803 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | SDGR | +0.851 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | GNRC | +0.669 | large | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 8 | ILMN | +0.544 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | WAY | +0.883 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 10 | XMTR | +0.819 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 11 | BRKR | +0.888 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | SFL | +0.834 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | GLBE | +0.768 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 14 | M | +0.740 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 1m BUY — why these names

### 1. VICR · $10.3B mid · Technology

**1m score +0.922**

**VICR** is a liquid **mid-cap** Technology name (Electronic Components) at $10.3B, ADV ~872k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.47 | -0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.99 | +0.198 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.922** | |

### 2. RBRK · $22.1B large · Technology

**1m score +0.558**

**RBRK** is a liquid **large-cap** Technology name (Software - Infrastructure) at $22.1B, ADV ~3407k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.23 | -0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.92 | +0.185 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1m total** | | | **+0.558** | |

### 3. ECO · $3.3B mid · Industrials

**1m score +0.921**

**ECO** is a liquid **mid-cap** Industrials name (Marine Shipping) at $3.3B, ADV ~551k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.98 | +0.216 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.16 | +0.032 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.93 | +0.185 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.921** | |

### 4. DELL · $361.2B mega · Technology

**1m score +0.389**

**DELL** is a liquid **mega-cap** Technology name (Computer Hardware) at $361.2B, ADV ~7832k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.47 | -0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.87 | +0.173 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.03 | -0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.28 | -0.280 | liquid small/mid, room to run |
| **1m total** | | | **+0.389** | |

### 5. FIVN · $2.4B mid · Technology

**1m score +0.841**

**FIVN** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $2.4B, ADV ~2581k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.47 | -0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.89 | +0.178 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.841** | |

### 6. SDGR · $2.2B mid · Healthcare

**1m score +0.893**

**SDGR** is a liquid **mid-cap** Healthcare name (Health Information Services) at $2.2B, ADV ~1356k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extreme**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.91 | +0.201 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.51 | +0.101 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.47 | -0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +1.00 | +0.200 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.893** | |

### 7. GNRC · $12.2B large · Industrials

**1m score +0.704**

**GNRC** is a liquid **large-cap** Industrials name (Specialty Industrial Machinery) at $12.2B, ADV ~1171k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.96 | +0.211 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.16 | +0.032 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.47 | -0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.95 | +0.190 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1m total** | | | **+0.704** | |

### 8. ILMN · $36.2B large · Healthcare

**1m score +0.587**

**ILMN** is a liquid **large-cap** Healthcare name (Diagnostics & Research) at $36.2B, ADV ~2019k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.93 | +0.204 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.51 | +0.101 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.47 | -0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.93 | +0.186 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.11 | -0.110 | liquid small/mid, room to run |
| **1m total** | | | **+0.587** | |

### 9. WAY · $4.9B mid · Healthcare

**1m score +0.927**

**WAY** is a liquid **mid-cap** Healthcare name (Health Information Services) at $4.9B, ADV ~2748k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.89 | +0.196 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.51 | +0.101 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.96 | +0.192 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.927** | |

### 10. XMTR · $5.6B mid · Industrials

**1m score +0.855**

**XMTR** is a liquid **mid-cap** Industrials name (Industrial Distribution) at $5.6B, ADV ~723k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.98 | +0.216 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.16 | +0.032 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.23 | -0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.211 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.82 | +0.164 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.855** | |

### 11. BRKR · $9.4B mid · Healthcare

**1m score +0.929**

**BRKR** is a liquid **mid-cap** Healthcare name (Medical Devices) at $9.4B, ADV ~2229k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.80 | +0.176 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.51 | +0.101 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.23 | -0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.89 | +0.177 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.929** | |

### 12. SFL · $2.1B mid · Industrials

**1m score +0.873**

**SFL** is a liquid **mid-cap** Industrials name (Marine Shipping) at $2.1B, ADV ~1478k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.97 | +0.213 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.16 | +0.032 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.50 | +0.101 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.873** | |

### 13. GLBE · $6.4B mid · Consumer Cyclical

**1m score +0.804**

**GLBE** is a liquid **mid-cap** Consumer Cyclical name (Internet Retail) at $6.4B, ADV ~1475k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.95 | +0.209 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.08 | +0.016 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.23 | -0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.52 | +0.104 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.804** | |

### 14. M · $5.7B mid · Consumer Cyclical

**1m score +0.773**

**M** is a liquid **mid-cap** Consumer Cyclical name (Department Stores) at $5.7B, ADV ~5609k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.91 | +0.201 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.08 | +0.016 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.47 | -0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.58 | +0.115 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.773** | |


## 1m AVOID — bottom of the same rank

- **EU** (micro, Energy, $174M) score -0.638. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **UUUU** (mid, Energy, $3.1B) score -0.623. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SOC** (small, Energy, $875M) score -0.615. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PUMP** (small, Energy, $1.3B) score -0.599. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **WFRD** (mid, Energy, $6.0B) score -0.558. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GS** (mega, Financial, $274.3B) score -0.528. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **URG** (small, Energy, $473M) score -0.527. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **MS** (mega, Financial, $318.2B) score -0.512. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BTU** (mid, Energy, $3.1B) score -0.498. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TTI** (small, Energy, $900M) score -0.493. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **XPRO** (small, Energy, $1.8B) score -0.491. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LDI** (micro, Financial, $236M) score -0.484. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **UEC** (mid, Energy, $4.9B) score -0.467. this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **KGS** (mid, Energy, $5.9B) score -0.466. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **JEF** (large, Financial, $11.0B) score -0.446. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AR** (large, Energy, $10.7B) score -0.441. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **HP** (mid, Energy, $4.1B) score -0.433. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OSG** (micro, Financial, $203M) score -0.433. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **C** (mega, Financial, $221.0B) score -0.418. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **VALE** (large, Basic Materials, $57.9B) score -0.418. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **CRC** (mid, Energy, $4.8B) score -0.417. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AROC** (mid, Energy, $5.5B) score -0.407. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **HAL** (large, Energy, $28.0B) score -0.399. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CBOE** (large, Financial, $28.5B) score -0.396. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **IVZ** (large, Financial, $13.5B) score -0.386. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-09-18_stock_book.md`
- Machine table: `data/stock_book/2026-09-18_stock_book.csv`
- Machine book: `data/stock_book/2026-09-18_stock_book.json`
- Join rank: `data/join/2026-09-18_ranked.csv`
- Weather: `01_daily/weather/2026-09-18_weather.md`
- AB enrich: `data/ab_checklist/2026-09-18_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-09-18_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-09-18_map_heat.md`
