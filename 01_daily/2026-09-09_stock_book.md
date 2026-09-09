# Stock book — 2026-09-09

_Generated 2026-09-09T09:00:47.965396-04:00_

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
- Stand-down: **no** — 359 names qualified through standard,group_leader,catalyst (66 probable)
- Sector predicts this date: 10/11 (ok)
- News tickers in play: 112
- AB coverage: 1941 names · peer RS: 1833
- Universe after liquidity: 2062
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 63

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
| 1 | **BE** | 🟡🟡🟢🟡🟢🟢 | group_leader | direct normal digest (stale/undated): Clear Street lifts Bloom Energy target to $330 on expected S&P 500 demand as UBS raises target to $325; Electrical Equipment & Parts +4.5% d1 / +13.6% 1w / +12.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.30); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 2 | **NVT** | 🟡🟡🟢🟡🟢🟢 | group_leader | basket/action net=+3.51; context only, not a company catalyst; Electrical Equipment & Parts +4.5% d1 / +13.6% 1w / +12.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.23); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 3 | **FORM** | 🟡🟡🟢🟡🟢🟢 | group_leader | basket/action net=+1.80; context only, not a company catalyst; Semiconductor Equipment & Materials +3.3% d1 / +5.0% 1w / +3.7% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.12); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 4 | **VLO** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Oil & Gas Refining & Marketing +2.4% d1 / +5.9% 1w / +4.3% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 5 | **DINO** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Oil & Gas Refining & Marketing +2.4% d1 / +5.9% 1w / +4.3% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 6 | **CEG** | 🟡🟢🟢🟡🟢🟡 | group_leader | basket/action net=+3.51; context only, not a company catalyst; Utilities - Independent Power Producers +0.9% d1 / +8.9% 1w / +6.2% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.23); setup=GREEN; flow=YELLOW; lookback=🔵,⚪,Cond green |
| 7 | **UGP** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Oil & Gas Refining & Marketing +2.4% d1 / +5.9% 1w / +4.3% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 8 | **CVI** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Oil & Gas Refining & Marketing +2.4% d1 / +5.9% 1w / +4.3% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 9 | **TER** | 🟡🟡🟢🟡🟢🟢 | group_leader | basket/action net=+1.80; context only, not a company catalyst; Semiconductor Equipment & Materials +3.3% d1 / +5.0% 1w / +3.7% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.12); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 10 | **MPC** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Oil & Gas Refining & Marketing +2.4% d1 / +5.9% 1w / +4.3% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 11 | **AEHR** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Semiconductor Equipment & Materials +3.3% d1 / +5.0% 1w / +3.7% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 12 | **PARR** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Oil & Gas Refining & Marketing +2.4% d1 / +5.9% 1w / +4.3% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 13 | **Q** | 🟡🟡🟢🟡🟢🟢 | group_leader | basket/action net=+1.80; context only, not a company catalyst; Semiconductor Equipment & Materials +3.3% d1 / +5.0% 1w / +3.7% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.12); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 14 | **VRT** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Electrical Equipment & Parts +4.5% d1 / +13.6% 1w / +12.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 15 | **PBF** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Oil & Gas Refining & Marketing +2.4% d1 / +5.9% 1w / +4.3% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **BGS** | 🟡🔴🔴🟡🔴🔴 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.3% |
| 2 | **XHG** | 🟡🔴🔴🟡🔴🔴 | Insurance Brokers | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.9% |
| 3 | **METC** | 🟡🔴🔴🟡🔴🟡 | Coking Coal | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.4% |
| 4 | **SMPL** | 🟡🔴🔴🟡🔴🔴 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.3% |
| 5 | **NG** | 🟡🔴🔴🟡🔴🔴 | Gold | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 6 | **BRBR** | 🟡🔴🔴🟡🔴🟡 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.3% |
| 7 | **BRO** | 🟡🔴🔴🟡🔴🔴 | Insurance Brokers | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.9% |
| 8 | **GIS** | 🟡🔴🔴🟡🔴🟡 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.3% |
| 9 | **CPB** | 🟡🔴🔴🟡🔴🟡 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.3% |
| 10 | **FLO** | 🟡🔴🔴🟡🔴🟡 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.3% |
| 11 | **INFY** | 🟡🟡🔴🟡🔴🔴 | Information Technology Services | SELL/AVOID — market=YELLOW; red domains=child,setup,flow; child lags parent -4.7% |
| 12 | **TRIP** | 🟡🔴🔴🟡🔴🟢 | Travel Services | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -4.2% |
| 13 | **HRL** | 🟡🔴🔴🟡🔴🟢 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.3% |
| 14 | **JBS** | 🟡🔴🔴🟡🔴🟡 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.3% |
| 15 | **LUCD** | 🟡🔴🔴🟡🔴🟡 | Medical Devices | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.2% |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **captain_research** (250 captains, 11 industries → s_heat).
- Board file: `01_daily/map_heat/2026-09-09_map_heat.json` · generated 2026-09-09T01:59:54.403331-04:00

### Sector RS vs same-day LLM essay

| Sector | Finviz 1d | Finviz 1w | LLM 1d | Tape vs essay |
|--------|----------:|----------:|-------:|---------------|
| Basic Materials | +0.0% | -0.3% | -0.47 |  |
| Communication Services | -0.3% | +0.7% | +0.00 |  |
| Consumer Cyclical | -0.7% | -1.8% | -0.60 |  |
| Consumer Defensive | -0.6% | -0.9% | -0.28 |  |
| Energy | +1.2% | +1.6% | +0.47 |  |
| Financial | -1.2% | +0.1% | -0.25 |  |
| Healthcare | -2.5% | -1.8% | -0.55 |  |
| Industrials | +0.3% | +1.1% | +0.00 |  |
| Real Estate | -0.2% | -0.5% | — |  |
| Technology | +0.2% | +1.3% | +0.00 |  |
| Utilities | +0.9% | +2.7% | +0.00 | essay flat, tape moving |

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
| Consumer Cyclical | -0.60 |
| Healthcare | -0.55 |
| Basic Materials | -0.47 |
| Energy | +0.47 |
| Consumer Defensive | -0.28 |
| Financial | -0.25 |
| Communication Services | +0.00 |
| Industrials | +0.00 |
| Technology | +0.00 |
| Utilities | +0.00 |

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

## Horizon weights — book_policy.json v11 · renormalized (absent: general)

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.13 | 0.11 | 0.00 | 0.27 | 0.27 | 0.22 | additive |
| 3d | 0.17 | 0.15 | 0.00 | 0.17 | 0.28 | 0.22 | additive |
| 1w | 0.20 | 0.17 | 0.00 | 0.11 | 0.30 | 0.22 | additive |
| 2w | 0.22 | 0.20 | 0.00 | 0.07 | 0.30 | 0.22 | additive |
| 1m | 0.24 | 0.22 | 0.00 | 0.00 | 0.33 | 0.22 | additive |

## 1d BUY — why these names

### 1. NVT · $26.7B large · Industrials

**1d score +0.608**

**NVT** is a liquid **large-cap** Industrials name (Electrical Equipment & Parts) at $26.7B, ADV ~2283k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.86 | +0.113 | does this *kind* of stock fit today's regime? |
| sector predict | 0.11 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.27 | +0.61 | +0.165 | headlines + news-judge ticker tilts |
| AB checklist | 0.27 | +0.94 | +0.256 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.90 | +0.195 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.608** | |

### 2. DINO · $19.5B large · Energy

**1d score +0.379**

**DINO** is a liquid **large-cap** Energy name (Oil & Gas Refining & Marketing) at $19.5B, ADV ~2641k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.95 | +0.123 | does this *kind* of stock fit today's regime? |
| sector predict | 0.11 | +0.27 | +0.029 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.27 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.27 | +0.96 | +0.262 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.39 | +0.085 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.379** | |

### 3. UGP · $8.0B mid · Energy

**1d score +0.829**

**UGP** is a liquid **mid-cap** Energy name (Oil & Gas Refining & Marketing) at $8.0B, ADV ~3582k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.96 | +0.125 | does this *kind* of stock fit today's regime? |
| sector predict | 0.11 | +0.27 | +0.029 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.27 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.27 | +0.93 | +0.251 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.80 | +0.174 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.829** | |

### 4. CVI · $4.6B mid · Energy

**1d score +0.838**

**CVI** is a liquid **mid-cap** Energy name (Oil & Gas Refining & Marketing) at $4.6B, ADV ~1031k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.89 | +0.116 | does this *kind* of stock fit today's regime? |
| sector predict | 0.11 | +0.27 | +0.029 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.27 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.27 | +0.97 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.70 | +0.152 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.838** | |


## 1d AVOID — bottom of the same rank

- **BGS** (micro, Consumer Defensive, $262M) score -0.416. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.3%
- **XHG** (micro, Financial, $141M) score -0.382. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.9%
- **METC** (small, Basic Materials, $723M) score -0.556. SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.4%
- **SMPL** (small, Consumer Defensive, $947M) score -0.370. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.3%
- **NG** (mid, Basic Materials, $3.5B) score -0.355. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow
- **BRBR** (small, Consumer Defensive, $1.1B) score -0.326. SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.3%
- **BRO** (large, Financial, $22.6B) score -0.237. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.9%
- **GIS** (large, Consumer Defensive, $20.1B) score -0.489. SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.3%

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | SM | +0.893 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 2 | CVI | +0.893 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | UGP | +0.886 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | UROY | +0.877 | small | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | ARLO | +0.815 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | GEO | +0.799 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | QRVO | +0.763 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | CECO | +0.759 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | CXW | +0.755 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 10 | BDC | +0.753 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | CIG | +0.748 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | ACMR | +0.718 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | COHU | +0.716 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 14 | EVER | +0.660 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | TDS | +0.659 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 16 | NRGV | +0.644 | small | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 17 | CWEN | +0.642 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 18 | ZG | +0.577 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 19 | MRX | +0.560 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 20 | XP | +0.557 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 21 | NU | +0.552 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 22 | ENIC | +0.543 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 23 | PIPR | +0.474 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 24 | ORIC | +0.467 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 25 | BZ | +0.443 | mid | Communication Services | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | CVI | +0.931 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | UGP | +0.925 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | WTTR | +0.914 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | UROY | +0.914 | small | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | ARLO | +0.854 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | GEO | +0.837 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | BDC | +0.834 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | QRVO | +0.808 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | COHU | +0.792 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | KOPN | +0.790 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | CECO | +0.790 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | CXW | +0.788 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 13 | CIG | +0.775 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 14 | EVER | +0.735 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | TDS | +0.721 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 16 | CWEN | +0.668 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 17 | NRGV | +0.659 | small | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 18 | ZG | +0.640 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 19 | MRX | +0.596 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 20 | XP | +0.589 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 21 | NU | +0.587 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 22 | ENIC | +0.560 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 23 | BZ | +0.513 | mid | Communication Services | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 24 | PIPR | +0.509 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 25 | ORIC | +0.499 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | CVI | +0.879 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | UGP | +0.875 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | ARLO | +0.873 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | WTTR | +0.864 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | UROY | +0.864 | small | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | BDC | +0.857 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | GEO | +0.854 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | CIG | +0.831 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | COHU | +0.815 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | KOPN | +0.806 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | QRVO | +0.806 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | CECO | +0.804 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | CXW | +0.800 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 14 | EVER | +0.753 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | TDS | +0.733 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 16 | CWEN | +0.723 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 17 | NRGV | +0.710 | small | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 18 | MRX | +0.658 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 19 | ZG | +0.655 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 20 | XP | +0.650 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 21 | NU | +0.648 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 22 | ENIC | +0.607 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 23 | ORIC | +0.605 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 24 | PIPR | +0.570 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 25 | BZ | +0.542 | mid | Communication Services | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1m BUY — why these names

### 1. CVI · $4.6B mid · Energy

**1m score +0.915**

**CVI** is a liquid **mid-cap** Energy name (Oil & Gas Refining & Marketing) at $4.6B, ADV ~1031k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.89 | +0.212 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | -0.20 | -0.043 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.97 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.70 | +0.152 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.915** | |

### 2. UGP · $8.0B mid · Energy

**1m score +0.911**

**UGP** is a liquid **mid-cap** Energy name (Oil & Gas Refining & Marketing) at $8.0B, ADV ~3582k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.96 | +0.229 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | -0.20 | -0.043 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.93 | +0.302 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.80 | +0.174 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.911** | |

### 3. BDC · $4.8B mid · Technology

**1m score +0.906**

**BDC** is a liquid **mid-cap** Technology name (Communication Equipment) at $4.8B, ADV ~512k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.59 | +0.142 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.48 | +0.103 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.96 | +0.314 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.67 | +0.146 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.906** | |

### 4. WTTR · $2.7B mid · Energy

**1m score +0.902**

**WTTR** is a liquid **mid-cap** Energy name (Oil & Gas Equipment & Services) at $2.7B, ADV ~1723k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.97 | +0.231 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | -0.20 | -0.043 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.97 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.68 | +0.147 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.902** | |

### 5. UROY · $1.8B small · Energy

**1m score +0.899**

**UROY** is a liquid **small-cap** Energy name (Uranium) at $1.8B, ADV ~2858k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.98 | +0.234 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | -0.20 | -0.043 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.81 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.89 | +0.194 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.899** | |

### 6. ARLO · $1.5B small · Industrials

**1m score +0.868**

**ARLO** is a liquid **small-cap** Industrials name (Building Products & Equipment) at $1.5B, ADV ~1281k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.86 | +0.206 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | -0.20 | -0.043 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.93 | +0.302 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.71 | +0.154 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.05 | +0.049 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.868** | |

### 7. CIG · $4.3B mid · Utilities

**1m score +0.863**

**CIG** is a liquid **mid-cap** Utilities name (Utilities - Regulated Electric) at $4.3B, ADV ~5730k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.33 | +0.079 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.25 | +0.054 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.91 | +0.295 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.85 | +0.184 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.863** | |

### 8. COHU · $2.5B mid · Technology

**1m score +0.858**

**COHU** is a liquid **mid-cap** Technology name (Semiconductor Equipment & Materials) at $2.5B, ADV ~1184k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.58 | +0.140 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.48 | +0.103 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.70 | +0.230 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.85 | +0.185 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.858** | |

### 9. GEO · $4.2B mid · Industrials

**1m score +0.849**

**GEO** is a liquid **mid-cap** Industrials name (Security & Protection Services) at $4.2B, ADV ~2077k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.80 | +0.192 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | -0.20 | -0.043 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.94 | +0.307 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.53 | +0.115 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.849** | |

### 10. KOPN · $847M small · Technology

**1m score +0.849**

**KOPN** is a liquid **small-cap** Technology name (Electronic Components) at $847M, ADV ~4895k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.29 | +0.070 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.48 | +0.103 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.96 | +0.314 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.74 | +0.161 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.849** | |

### 11. AVT · $7.5B mid · Technology

**1m score +0.845**

**AVT** is a liquid **mid-cap** Technology name (Electronics & Computer Distribution) at $7.5B, ADV ~1224k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.71 | +0.169 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.48 | +0.103 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.97 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.25 | +0.055 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.845** | |

### 12. CECO · $4.6B mid · Industrials

**1m score +0.790**

**CECO** is a liquid **mid-cap** Industrials name (Pollution & Treatment Controls) at $4.6B, ADV ~867k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.64 | +0.153 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | -0.20 | -0.043 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.76 | +0.248 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.84 | +0.183 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.790** | |

### 13. EVER · $880M small · Communication Services

**1m score +0.790**

**EVER** is a liquid **small-cap** Communication Services name (Internet Content & Information) at $880M, ADV ~665k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.39 | +0.094 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.45 | +0.098 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.85 | +0.277 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.33 | +0.071 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.790** | |

### 14. CXW · $3.5B mid · Industrials

**1m score +0.790**

**CXW** is a liquid **mid-cap** Industrials name (Security & Protection Services) at $3.5B, ADV ~1470k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.58 | +0.138 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | -0.20 | -0.043 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.93 | +0.302 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.66 | +0.144 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.790** | |

### 15. TDS · $4.1B mid · Communication Services

**1m score +0.758**

**TDS** is a liquid **mid-cap** Communication Services name (Telecom Services) at $4.1B, ADV ~1151k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.12 | +0.028 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.45 | +0.098 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.55 | +0.181 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.92 | +0.201 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.758** | |

### 16. CWEN · $6.8B mid · Utilities

**1m score +0.754**

**CWEN** is a liquid **mid-cap** Utilities name (Utilities - Renewable) at $6.8B, ADV ~1338k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.28 | +0.067 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.25 | +0.054 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.93 | +0.302 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.37 | +0.081 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.754** | |

### 17. NRGV · $775M small · Utilities

**1m score +0.730**

**NRGV** is a liquid **small-cap** Utilities name (Utilities - Renewable) at $775M, ADV ~5289k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.12 | +0.028 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.25 | +0.054 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.55 | +0.181 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +1.00 | +0.217 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.730** | |

### 18. MRX · $5.5B mid · Financial

**1m score +0.697**

**MRX** is a liquid **mid-cap** Financial name (Capital Markets) at $5.5B, ADV ~836k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.98 | +0.235 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | -0.08 | -0.017 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.93 | +0.302 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.59 | +0.128 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.697** | |

### 19. NU · $74.2B large · Financial

**1m score +0.687**

**NU** is a liquid **large-cap** Financial name (Banks - Regional) at $74.2B, ADV ~75892k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.99 | +0.236 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | -0.08 | -0.017 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.88 | +0.287 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.61 | +0.132 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.687** | |

### 20. XP · $10.3B large · Financial

**1m score +0.686**

**XP** is a liquid **large-cap** Financial name (Capital Markets) at $10.3B, ADV ~5322k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **extended**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.97 | +0.231 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | -0.08 | -0.017 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.76 | +0.248 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.80 | +0.173 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.686** | |

### 21. ZG · $7.9B mid · Communication Services

**1m score +0.680**

**ZG** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $7.9B, ADV ~1262k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.23 | +0.054 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.45 | +0.098 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.46 | +0.151 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.58 | +0.127 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.680** | |

### 22. ORIC · $1.4B small · Healthcare

**1m score +0.641**

**ORIC** is a liquid **small-cap** Healthcare name (Biotechnology) at $1.4B, ADV ~1550k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **miss**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.89 | +0.212 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.76 | +0.248 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.60 | +0.131 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.641** | |

### 23. ENIC · $6.3B mid · Utilities

**1m score +0.629**

**ENIC** is a liquid **mid-cap** Utilities name (Utilities - Renewable) at $6.3B, ADV ~592k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | -0.09 | -0.022 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.25 | +0.054 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.88 | +0.287 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.51 | +0.111 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.629** | |

### 24. PIPR · $5.5B mid · Financial

**1m score +0.610**

**PIPR** is a liquid **mid-cap** Financial name (Capital Markets) at $5.5B, ADV ~578k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.96 | +0.230 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | -0.08 | -0.017 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.93 | +0.302 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.21 | +0.046 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.610** | |

### 25. BZ · $6.7B mid · Communication Services

**1m score +0.574**

**BZ** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $6.7B, ADV ~4662k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.90 | +0.214 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.45 | +0.098 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.12 | +0.041 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | -0.13 | -0.028 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.574** | |


## 1m AVOID — bottom of the same rank

- **SGML** (small, Basic Materials, $1.2B) score -0.689. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **METC** (small, Basic Materials, $723M) score -0.667. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LODE** (micro, Basic Materials, $234M) score -0.647. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **CPNG** (large, Consumer Cyclical, $26.9B) score -0.622. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **XPOF** (micro, Consumer Cyclical, $240M) score -0.619. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **XPEV** (mid, Consumer Cyclical, $8.5B) score -0.612. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **TROX** (small, Basic Materials, $767M) score -0.602. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**
- **ZVIA** (micro, Consumer Defensive, $104M) score -0.599. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GIS** (large, Consumer Defensive, $20.1B) score -0.596. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ROL** (large, Consumer Cyclical, $16.9B) score -0.586. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **HGV** (mid, Consumer Cyclical, $3.2B) score -0.585. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BIRK** (mid, Consumer Cyclical, $5.8B) score -0.580. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **IAUX** (small, Basic Materials, $1.5B) score -0.576. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **OI** (small, Consumer Cyclical, $1.0B) score -0.565. this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **MVST** (micro, Consumer Cyclical, $268M) score -0.554. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **PACK** (small, Consumer Cyclical, $369M) score -0.539. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **WHR** (mid, Consumer Cyclical, $2.5B) score -0.533. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **DFH** (small, Consumer Cyclical, $1.2B) score -0.530. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BGS** (micro, Consumer Defensive, $262M) score -0.529. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **KLC** (small, Consumer Defensive, $306M) score -0.523. the Finviz industry was **down**
- **FLO** (small, Consumer Defensive, $1.3B) score -0.520. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LKQ** (mid, Consumer Cyclical, $6.3B) score -0.519. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**
- **PENN** (mid, Consumer Cyclical, $2.3B) score -0.510. this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **STLA** (large, Consumer Cyclical, $15.7B) score -0.504. this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **WH** (mid, Consumer Cyclical, $5.2B) score -0.501. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-09-09_stock_book.md`
- Machine table: `data/stock_book/2026-09-09_stock_book.csv`
- Machine book: `data/stock_book/2026-09-09_stock_book.json`
- Join rank: `data/join/2026-09-09_ranked.csv`
- Weather: `01_daily/weather/2026-09-09_weather.md`
- AB enrich: `data/ab_checklist/2026-09-09_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-09-09_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-09-09_map_heat.md`
