# Stock book — 2026-09-09

_Generated 2026-09-09T06:25:59.612178-04:00_

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

- Weather risk: **unknown**
- General predict (same-day): +0.00  (MISSING → 0)
- Stand-down: **no** — 387 names qualified through standard,group_leader,catalyst (23 probable)
- Sector predicts this date: 0/11 (missing → sector layer is 0; Finviz week tape still sits in join)
- News tickers in play: 113
- AB coverage: 1941 names · peer RS: 1833
- Universe after liquidity: 2062
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 62

## All-green BUY / SELL

- Mode: **weighted_fallback** · SELL **core_weights**
- Pile: **0** liquid all-green names (need ≥ 8) of 2062
- Core fired: join=yes, AB=yes, peer=yes
- pile 0 < 8 liquid all-green names. Fallback weighted walk; SELL stays on core

## Decision lattice — gate → route → rank

The weighted score is now a tie-breaker inside an eligible lane. It cannot average away a market, group, company, or setup veto.

### MARKET: 🟡 YELLOW

- YELLOW: general flat score=+0.00; good=+0.0 vs bad=+0.0; risk=unknown; red pillars=0
- Allowed long lanes: **standard, group_leader, catalyst** · max slots 8 · size ×0.60

Decision domains: **MKT · parent · child · company · setup · flow**. Measured parent/child tape is kept separate from the LLM essay; direct company events must be price-confirmed on a hard-red day.

### Bull decisions (eligible or closest blocked cases)

| # | Ticker | Domains | Lane | Company / group | Decision |
|---:|--------|---------|------|-----------------|----------|
| 1 | **CRM** | 🟡🟡🔴🟢🟢🟢 | catalyst | direct high digest (same-day): Salesforce beats Q2 guidance, raises FY27 outlook as analysts lift price targets after 'narrative-changing' results; Software - Application -3.5% d1 / -7.0% 1w / -8.3% vs parent | BUY CATALYST — market=YELLOW; parent=YELLOW; child=RED/rel=RED; company=GREEN(0.78); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 2 | **NVT** | 🟡🟡🟢🟡🟢🟢 | group_leader | basket/action net=+3.51; context only, not a company catalyst; Electrical Equipment & Parts +4.5% d1 / +13.6% 1w / +12.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.23); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 3 | **FORM** | 🟡🟡🟢🟡🟢🟢 | group_leader | basket/action net=+1.80; context only, not a company catalyst; Semiconductor Equipment & Materials +3.3% d1 / +5.0% 1w / +3.7% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.12); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 4 | **VLO** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Oil & Gas Refining & Marketing +2.4% d1 / +5.9% 1w / +4.3% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 5 | **DINO** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Oil & Gas Refining & Marketing +2.4% d1 / +5.9% 1w / +4.3% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 6 | **CEG** | 🟡🟢🟢🟡🟢🟡 | group_leader | basket/action net=+3.51; context only, not a company catalyst; Utilities - Independent Power Producers +0.9% d1 / +8.9% 1w / +6.2% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.23); setup=GREEN; flow=YELLOW; lookback=🔵,⚪,Cond green |
| 7 | **UGP** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Oil & Gas Refining & Marketing +2.4% d1 / +5.9% 1w / +4.3% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 8 | **TER** | 🟡🟡🟢🟡🟢🟢 | group_leader | basket/action net=+1.80; context only, not a company catalyst; Semiconductor Equipment & Materials +3.3% d1 / +5.0% 1w / +3.7% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.12); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 9 | **BE** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Electrical Equipment & Parts +4.5% d1 / +13.6% 1w / +12.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 10 | **MPC** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Oil & Gas Refining & Marketing +2.4% d1 / +5.9% 1w / +4.3% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 11 | **PARR** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Oil & Gas Refining & Marketing +2.4% d1 / +5.9% 1w / +4.3% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 12 | **VRT** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Electrical Equipment & Parts +4.5% d1 / +13.6% 1w / +12.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 13 | **PBF** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Oil & Gas Refining & Marketing +2.4% d1 / +5.9% 1w / +4.3% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 14 | **RUN** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Solar +3.6% d1 / +5.6% 1w / +4.3% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 15 | **LRCX** | 🟡🟡🟢🟡🟢🟢 | group_leader | basket/action net=+1.80; context only, not a company catalyst; Semiconductor Equipment & Materials +3.3% d1 / +5.0% 1w / +3.7% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.12); setup=GREEN; flow=GREEN; lookback=🔵,⚪ |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **BGS** | 🟡🔴🔴🟡🔴🔴 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.3% |
| 2 | **SMPL** | 🟡🔴🔴🟡🔴🔴 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.3% |
| 3 | **BRBR** | 🟡🔴🔴🟡🔴🟡 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.3% |
| 4 | **GIS** | 🟡🔴🔴🟡🔴🟡 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.3% |
| 5 | **CPB** | 🟡🔴🔴🟡🔴🟡 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.3% |
| 6 | **FLO** | 🟡🔴🔴🟡🔴🟡 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.3% |
| 7 | **INFY** | 🟡🟡🔴🟡🔴🔴 | Information Technology Services | SELL/AVOID — market=YELLOW; red domains=child,setup,flow; child lags parent -4.7% |
| 8 | **HRL** | 🟡🔴🔴🟡🔴🟢 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.3% |
| 9 | **JBS** | 🟡🔴🔴🟡🔴🟡 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.3% |
| 10 | **WEST** | 🟡🔴🔴🟡🔴🟢 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.3% |
| 11 | **ENHA** | 🟡🔴🔴🟡🔴🟡 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.3% |
| 12 | **XHG** | 🟡🟡🔴🟡🔴🔴 | Insurance Brokers | SELL/AVOID — market=YELLOW; red domains=child,setup,flow; child lags parent -3.9% |
| 13 | **LUCD** | 🟡🔴🔴🟡🔴🟡 | Medical Devices | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.2% |
| 14 | **TRIP** | 🟡🔴🔴🟡🔴🟢 | Travel Services | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -4.2% |
| 15 | **HUBG** | 🟡🟡🔴🟡🔴🟡 | Integrated Freight & Logistics | SELL/AVOID — market=YELLOW; red domains=child,setup; child lags parent -3.6% |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **captain_research** (250 captains, 11 industries → s_heat).
- Board file: `01_daily/map_heat/2026-09-09_map_heat.json` · generated 2026-09-09T01:59:54.403331-04:00

### Sector RS vs same-day LLM essay

| Sector | Finviz 1d | Finviz 1w | LLM 1d | Tape vs essay |
|--------|----------:|----------:|-------:|---------------|
| Basic Materials | +0.0% | -0.3% | — |  |
| Communication Services | -0.3% | +0.7% | — |  |
| Consumer Cyclical | -0.7% | -1.8% | — |  |
| Consumer Defensive | -0.6% | -0.9% | — |  |
| Energy | +1.2% | +1.6% | — |  |
| Financial | -1.2% | +0.1% | — |  |
| Healthcare | -2.5% | -1.8% | — |  |
| Industrials | +0.3% | +1.1% | — |  |
| Real Estate | -0.2% | -0.5% | — |  |
| Technology | +0.2% | +1.3% | — |  |
| Utilities | +0.9% | +2.7% | — |  |

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
| Channel 1 raw | **missing / not in ranker** | via weather |
| Join ranked universe | **found** | s_join |
| News parse + actions | **found** | s_news |
| News judge | **found** | s_news ticker tilts |
| Finviz daily digest | **found** | s_news company headlines |
| General predict | **missing / not in ranker** | s_general × beta |
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
| — | none today |

### How much each predictor is trusted (graded hit rate)

| Topic | hit rate | n | weight |
|-------|----------|---|--------|
| general | 46% | 26 | ×0.85 |
| sector:Basic Materials | 53% | 15 | ×0.85 |
| sector:Communication Services | 27% | 15 | ×0.50 |
| sector:Consumer Cyclical | 60% | 15 | ×1.00 |
| sector:Consumer Defensive | 40% | 15 | ×0.50 |
| sector:Energy | 47% | 15 | ×0.85 |
| sector:Financial | 33% | 15 | ×0.50 |
| sector:Healthcare | 58% | 12 | ×1.00 |
| sector:Industrials | 20% | 15 | ×0.50 |
| sector:Real Estate | 47% | 15 | ×0.85 |
| sector:Technology | 36% | 14 | ×0.50 |
| sector:Utilities | 36% | 14 | ×0.50 |

## Horizon weights — book_policy.json v11 · renormalized (absent: sector, general)

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.15 | 0.00 | 0.00 | 0.30 | 0.30 | 0.24 | additive |
| 3d | 0.21 | 0.00 | 0.00 | 0.21 | 0.33 | 0.26 | additive |
| 1w | 0.24 | 0.00 | 0.00 | 0.13 | 0.37 | 0.26 | additive |
| 2w | 0.27 | 0.00 | 0.00 | 0.08 | 0.38 | 0.27 | additive |
| 1m | 0.31 | 0.00 | 0.00 | 0.00 | 0.42 | 0.28 | additive |

## 1d BUY — why these names

### 1. NVT · $26.7B large · Industrials

**1d score +0.702**

**NVT** is a liquid **large-cap** Industrials name (Electrical Equipment & Parts) at $26.7B, ADV ~2283k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.90 | +0.131 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.08 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.61 | +0.185 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.94 | +0.287 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.90 | +0.219 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.702** | |

### 2. DINO · $19.5B large · Energy

**1d score +0.316**

**DINO** is a liquid **large-cap** Energy name (Oil & Gas Refining & Marketing) at $19.5B, ADV ~2641k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.32 | +0.047 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.96 | +0.294 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.39 | +0.095 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.316** | |

### 3. UGP · $8.0B mid · Energy

**1d score +0.795**

**UGP** is a liquid **mid-cap** Energy name (Oil & Gas Refining & Marketing) at $8.0B, ADV ~3582k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.47 | +0.068 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.93 | +0.282 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.80 | +0.195 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.795** | |


## 1d AVOID — bottom of the same rank

- **BGS** (micro, Consumer Defensive, $262M) score -0.287. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.3%
- **SMPL** (small, Consumer Defensive, $947M) score -0.182. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.3%
- **BRBR** (small, Consumer Defensive, $1.1B) score -0.143. SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.3%
- **GIS** (large, Consumer Defensive, $20.1B) score -0.438. SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.3%
- **CPB** (mid, Consumer Defensive, $6.5B) score -0.091. SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.3%
- **FLO** (small, Consumer Defensive, $1.3B) score -0.234. SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.3%
- **INFY** (large, Technology, $44.2B) score -0.363. SELL/AVOID — market=YELLOW; red domains=child,setup,flow; child lags parent -4.7%
- **HRL** (large, Consumer Defensive, $11.7B) score -0.110. SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.3%

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | SM | +0.950 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 2 | ARLO | +0.923 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | MRX | +0.912 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | GEO | +0.900 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | UROY | +0.894 | small | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | WTTR | +0.864 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | UGP | +0.859 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | CECO | +0.855 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | CXW | +0.848 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 10 | CIG | +0.829 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | BDC | +0.819 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | QRVO | +0.814 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | PIPR | +0.813 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 14 | NU | +0.794 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 15 | ACMR | +0.782 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 16 | COHU | +0.776 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 17 | ORIC | +0.766 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 18 | SNEX | +0.765 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 19 | GGB | +0.761 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 20 | GPRE | +0.759 | small | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 21 | CLOV | +0.758 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the Finviz industry was **advancing** |
| 22 | CWEN | +0.701 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 23 | NRGV | +0.697 | small | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 24 | EVER | +0.692 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 25 | TDS | +0.678 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | ARLO | +0.989 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | MRX | +0.979 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | GEO | +0.963 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | UROY | +0.950 | small | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | SM | +0.944 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 6 | WTTR | +0.920 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | UGP | +0.912 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | CECO | +0.908 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | CXW | +0.904 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 10 | PIPR | +0.877 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | BDC | +0.876 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | CIG | +0.876 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | NU | +0.838 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 14 | ORIC | +0.826 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 15 | COHU | +0.825 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 16 | SNEX | +0.824 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 17 | QRVO | +0.824 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 18 | ACMR | +0.816 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 19 | CLOV | +0.813 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the Finviz industry was **advancing** |
| 20 | GGB | +0.809 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 21 | GPRE | +0.802 | small | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 22 | CWEN | +0.744 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 23 | EVER | +0.736 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 24 | SGHC | +0.726 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 25 | NRGV | +0.724 | small | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | ARLO | +1.033 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | MRX | +1.026 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | GEO | +1.004 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | UROY | +0.988 | small | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | WTTR | +0.954 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | CECO | +0.944 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | UGP | +0.942 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | SM | +0.938 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 9 | CXW | +0.937 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 10 | PIPR | +0.921 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | BDC | +0.911 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | CIG | +0.901 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | ORIC | +0.869 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 14 | NU | +0.868 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 15 | MIAX | +0.866 | mid | Financial | the Finviz industry was **advancing** |
| 16 | COHU | +0.858 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 17 | CLOV | +0.854 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the Finviz industry was **advancing** |
| 18 | ACMR | +0.836 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 19 | AVT | +0.836 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 20 | GGB | +0.834 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 21 | GPRE | +0.821 | small | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 22 | CWEN | +0.763 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 23 | EVER | +0.759 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 24 | SGHC | +0.757 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 25 | NRGV | +0.738 | small | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 1m BUY — why these names

### 1. ARLO · $1.5B small · Industrials

**1m score +1.106**

**ARLO** is a liquid **small-cap** Industrials name (Building Products & Equipment) at $1.5B, ADV ~1281k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.90 | +0.274 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.08 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.93 | +0.386 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.71 | +0.197 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.05 | +0.049 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.106** | |

### 2. MRX · $5.5B mid · Financial

**1m score +1.101**

**MRX** is a liquid **mid-cap** Financial name (Capital Markets) at $5.5B, ADV ~836k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.93 | +0.386 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.59 | +0.164 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.101** | |

### 3. GEO · $4.2B mid · Industrials

**1m score +1.074**

**GEO** is a liquid **mid-cap** Industrials name (Security & Protection Services) at $4.2B, ADV ~2077k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.84 | +0.256 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.08 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.94 | +0.392 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.53 | +0.147 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.074** | |

### 4. UROY · $1.8B small · Energy

**1m score +1.051**

**UROY** is a liquid **small-cap** Energy name (Uranium) at $1.8B, ADV ~2858k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.70 | +0.215 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.81 | +0.337 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.89 | +0.249 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.051** | |

### 5. WTTR · $2.7B mid · Energy

**1m score +1.016**

**WTTR** is a liquid **mid-cap** Energy name (Oil & Gas Equipment & Services) at $2.7B, ADV ~1723k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.56 | +0.173 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.97 | +0.405 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.68 | +0.188 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.016** | |

### 6. CECO · $4.6B mid · Industrials

**1m score +1.003**

**CECO** is a liquid **mid-cap** Industrials name (Pollution & Treatment Controls) at $4.6B, ADV ~867k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.66 | +0.201 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.08 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.76 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.84 | +0.234 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.003** | |

### 7. UGP · $8.0B mid · Energy

**1m score +1.000**

**UGP** is a liquid **mid-cap** Energy name (Oil & Gas Refining & Marketing) at $8.0B, ADV ~3582k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.47 | +0.142 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.93 | +0.386 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.80 | +0.222 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.000** | |

### 8. CXW · $3.5B mid · Industrials

**1m score +0.999**

**CXW** is a liquid **mid-cap** Industrials name (Security & Protection Services) at $3.5B, ADV ~1470k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.59 | +0.179 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.08 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.93 | +0.386 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.66 | +0.184 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.999** | |

### 9. CRK · $4.5B mid · Energy

**1m score +0.994**

**CRK** is a liquid **mid-cap** Energy name (Oil & Gas E&P) at $4.5B, ADV ~2551k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.80 | +0.244 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.85 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.53 | +0.146 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.994** | |

### 10. PIPR · $5.5B mid · Financial

**1m score +0.992**

**PIPR** is a liquid **mid-cap** Financial name (Capital Markets) at $5.5B, ADV ~578k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.98 | +0.299 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.93 | +0.386 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.21 | +0.058 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.992** | |

### 11. BDC · $4.8B mid · Technology

**1m score +0.975**

**BDC** is a liquid **mid-cap** Technology name (Communication Equipment) at $4.8B, ADV ~512k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.61 | +0.186 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.96 | +0.402 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.67 | +0.187 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.975** | |

### 12. CIG · $4.3B mid · Utilities

**1m score +0.952**

**CIG** is a liquid **mid-cap** Utilities name (Utilities - Regulated Electric) at $4.3B, ADV ~5730k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.29 | +0.089 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.91 | +0.377 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.85 | +0.236 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.952** | |

### 13. MIAX · $4.3B mid · Financial

**1m score +0.936**

**MIAX** is a liquid **mid-cap** Financial name (Capital Markets) at $4.3B, ADV ~1839k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.98 | +0.301 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.93 | +0.386 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.936** | |

### 14. ORIC · $1.4B small · Healthcare

**1m score +0.935**

**ORIC** is a liquid **small-cap** Healthcare name (Biotechnology) at $1.4B, ADV ~1550k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **miss**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.92 | +0.280 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.76 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.60 | +0.167 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.12 | +0.120 | liquid small/mid, room to run |
| **1m total** | | | **+0.935** | |

### 15. NU · $74.2B large · Financial

**1m score +0.916**

**NU** is a liquid **large-cap** Financial name (Banks - Regional) at $74.2B, ADV ~75892k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.31 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.61 | +0.168 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1m total** | | | **+0.916** | |

### 16. CLOV · $2.3B mid · Healthcare

**1m score +0.914**

**CLOV** is a liquid **mid-cap** Healthcare name (Healthcare Plans) at $2.3B, ADV ~5851k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.95 | +0.289 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.64 | +0.265 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.40 | +0.111 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.914** | |

### 17. COHU · $2.5B mid · Technology

**1m score +0.913**

**COHU** is a liquid **mid-cap** Technology name (Semiconductor Equipment & Materials) at $2.5B, ADV ~1184k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.60 | +0.183 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.70 | +0.293 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.85 | +0.237 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.913** | |

### 18. AVT · $7.5B mid · Technology

**1m score +0.901**

**AVT** is a liquid **mid-cap** Technology name (Electronics & Computer Distribution) at $7.5B, ADV ~1224k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.74 | +0.225 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.97 | +0.405 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.25 | +0.071 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.901** | |

### 19. GGB · $6.3B mid · Basic Materials

**1m score +0.887**

**GGB** is a liquid **mid-cap** Basic Materials name (Steel) at $6.3B, ADV ~14337k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.37 | +0.112 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.91 | +0.377 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.60 | +0.167 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.030 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.887** | |

### 20. KOPN · $847M small · Technology

**1m score +0.882**

**KOPN** is a liquid **small-cap** Technology name (Electronic Components) at $847M, ADV ~4895k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.25 | +0.075 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.96 | +0.402 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.74 | +0.206 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.882** | |

### 21. GPRE · $1.1B small · Basic Materials

**1m score +0.868**

**GPRE** is a liquid **small-cap** Basic Materials name (Chemicals) at $1.1B, ADV ~1491k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.16 | +0.050 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.95 | +0.398 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.61 | +0.171 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.868** | |

### 22. SGHC · $7.1B mid · Consumer Cyclical

**1m score +0.812**

**SGHC** is a liquid **mid-cap** Consumer Cyclical name (Gambling) at $7.1B, ADV ~2837k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.62 | +0.189 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.81 | +0.337 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.30 | +0.084 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.05 | -0.049 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.812** | |

### 23. CWEN · $6.8B mid · Utilities

**1m score +0.810**

**CWEN** is a liquid **mid-cap** Utilities name (Utilities - Renewable) at $6.8B, ADV ~1338k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.23 | +0.070 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.93 | +0.386 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.37 | +0.104 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.810** | |

### 24. EVER · $880M small · Communication Services

**1m score +0.807**

**EVER** is a liquid **small-cap** Communication Services name (Internet Content & Information) at $880M, ADV ~665k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.37 | +0.112 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.85 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.33 | +0.091 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.807** | |

### 25. NRGV · $775M small · Utilities

**1m score +0.768**

**NRGV** is a liquid **small-cap** Utilities name (Utilities - Renewable) at $775M, ADV ~5289k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.03 | +0.009 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.55 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +1.00 | +0.277 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.768** | |


## 1m AVOID — bottom of the same rank

- **DVLT** (micro, Technology, $209M) score -0.692. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **SSTK** (micro, Communication Services, $193M) score -0.688. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GIS** (large, Consumer Defensive, $20.1B) score -0.674. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BHR** (micro, Real Estate, $126M) score -0.668. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BZFD** (micro, Communication Services, $90M) score -0.665. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LODE** (micro, Basic Materials, $234M) score -0.646. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **ZVIA** (micro, Consumer Defensive, $104M) score -0.622. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PFGC** (large, Consumer Defensive, $15.2B) score -0.621. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SGML** (small, Basic Materials, $1.2B) score -0.617. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **CMTG** (micro, Real Estate, $217M) score -0.594. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **HUBG** (mid, Industrials, $2.2B) score -0.589. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **COLD** (mid, Real Estate, $4.0B) score -0.588. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NN** (mid, Technology, $2.6B) score -0.586. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **JKS** (small, Technology, $647M) score -0.586. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **XHR** (small, Real Estate, $1.6B) score -0.583. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AMCX** (small, Communication Services, $498M) score -0.579. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CPNG** (large, Consumer Cyclical, $26.9B) score -0.577. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SMRT** (micro, Technology, $247M) score -0.568. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **INFY** (large, Technology, $44.2B) score -0.567. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CLVT** (small, Technology, $1.2B) score -0.564. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **XPOF** (micro, Consumer Cyclical, $240M) score -0.551. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CNDT** (micro, Technology, $257M) score -0.550. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BGS** (micro, Consumer Defensive, $262M) score -0.548. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LAES** (small, Technology, $571M) score -0.535. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **KULR** (micro, Technology, $117M) score -0.524. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**

## Files for this run

- This rationale: `01_daily/2026-09-09_stock_book.md`
- Machine table: `data/stock_book/2026-09-09_stock_book.csv`
- Machine book: `data/stock_book/2026-09-09_stock_book.json`
- Join rank: `data/join/2026-09-09_ranked.csv`
- Weather: `01_daily/weather/2026-09-09_weather.md`
- AB enrich: `data/ab_checklist/2026-09-09_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-09-09_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-09-09_map_heat.md`
