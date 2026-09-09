# Stock book — 2026-09-09

_Generated 2026-09-09T11:14:28.044936-04:00_

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
- General predict (same-day): -0.55 down (present)
- Stand-down: **no** — 78 names qualified through group_leader,catalyst,probable (71 probable)
- Sector predicts this date: 11/11 (ok)
- News tickers in play: 112
- AB coverage: 1938 names · peer RS: 1834
- Universe after liquidity: 2065
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 64

## All-green BUY / SELL

- Mode: **weighted_fallback** · SELL **core_weights**
- Pile: **0** liquid all-green names (need ≥ 8) of 2065
- Core fired: join=yes, AB=yes, peer=yes
- pile 0 < 8 liquid all-green names. Fallback weighted walk; SELL stays on core

## Decision lattice — gate → route → rank

The weighted score is now a tie-breaker inside an eligible lane. It cannot average away a market, group, company, or setup veto.

### MARKET: 🔴 RED

- RED: general down score=-13.95; good=+0.5 vs bad=-16.0; risk=unknown; red pillars=6
- Allowed long lanes: **group_leader, catalyst, probable** · max slots 8 · size ×0.35
- Bull evidence: sentiment +0.50 points
- Bear evidence: overnight catalysts -9.00 points; rates / Fed -3.00 points; global sessions -2.00 points; oil / dollar -1.00 points; volatility -0.75 points; futures -0.25 points

Decision domains: **MKT · parent · child · company · setup · flow**. Measured parent/child tape is kept separate from the LLM essay; direct company events must be price-confirmed on a hard-red day.

### Bull decisions (eligible or closest blocked cases)

| # | Ticker | Domains | Lane | Company / group | Decision |
|---:|--------|---------|------|-----------------|----------|
| 1 | **DINO** | 🔴🟢🟢🟡🟢🟢 | group_leader | no direct company event; Oil & Gas Refining & Marketing +2.4% d1 / +5.9% 1w / +4.3% vs parent | BUY GROUP_LEADER — market=RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 2 | **BG** | 🔴🔴🟢🟡🟢🟢 | group_leader | no direct company event; Farm Products +1.5% d1 / +2.5% 1w / +3.5% vs parent | BUY GROUP_LEADER — market=RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵 |
| 3 | **AEHR** | 🔴🟡🟢🟡🟢🟢 | group_leader | no direct company event; Semiconductor Equipment & Materials +3.3% d1 / +5.0% 1w / +3.7% vs parent | BUY GROUP_LEADER — market=RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 4 | **CEG** | 🔴🟢🟢🟡🟢🔴 | probable | basket/action net=+3.51; context only, not a company catalyst; Utilities - Independent Power Producers +0.9% d1 / +8.9% 1w / +6.2% vs parent | BUY PROBABLE — most-probable long on RED (size ×0.35); clocks: lookback 🔵 blue — market=RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.23); setup=GREEN; flow=RED; lookback=🔵,Cond green |
| 5 | **SANM** | 🔴🟡🟢🟡🟢🟢 | group_leader | no direct company event; Electronic Components +2.1% d1 / +5.2% 1w / +3.9% vs parent | BUY GROUP_LEADER — market=RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 6 | **NXT** | 🔴🟡🟢🟡🟢🟢 | group_leader | no direct company event; Solar +3.6% d1 / +5.6% 1w / +4.3% vs parent | BUY GROUP_LEADER — market=RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 7 | **DK** | 🔴🟢🟢🟡🟢🟡 | group_leader | no direct company event; Oil & Gas Refining & Marketing +2.4% d1 / +5.9% 1w / +4.3% vs parent | BUY GROUP_LEADER — market=RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=Cond green |
| 8 | **ETN** | 🔴🟡🟢🟡🟢🔴 | probable | direct normal digest (stale/undated): Eaton shares rise after UBS upgrades the stock to Buy from Neutral and raises its price target to $515, citing strong sales growth and expected margin improvement.; Specialty Industrial Machinery +0.9% d1 / +1.9% 1w / +0.9% vs parent | BUY PROBABLE — most-probable long on RED (size ×0.35); clocks: lookback 🔵 blue — market=RED; parent=YELLOW; child=GREEN/rel=YELLOW; company=YELLOW(0.30); setup=GREEN; flow=RED; lookback=🔵,Cond green |
| 9 | **CIG** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Utilities - Regulated Electric +0.9% d1 / +1.7% 1w / -1.0% vs parent | BUY PROBABLE — most-probable long on RED (size ×0.35); clocks: lookback 🔵 blue — market=RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 10 | **SM** | 🔴🟢🟢🟡🟢🔴 | probable | basket/action net=+7.36; context only, not a company catalyst; Oil & Gas E&P +0.9% d1 / +1.2% 1w / -0.4% vs parent | BUY PROBABLE — most-probable long on RED (size ×0.35); clocks: lookback 🔵 blue — market=RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.40); setup=GREEN; flow=RED; lookback=🔵,Cond green |
| 11 | **ATRO** | 🔴🟡🟢🟡🟢🟢 | probable | no direct company event; Aerospace & Defense +0.8% d1 / +1.3% 1w / +0.2% vs parent | BUY PROBABLE — most-probable long on RED (size ×0.35); clocks: lookback 🔵 blue — market=RED; parent=YELLOW; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 12 | **EMBJ** | 🔴🟡🟢🟡🟢🟢 | probable | no direct company event; Aerospace & Defense +0.8% d1 / +1.3% 1w / +0.2% vs parent | BUY PROBABLE — most-probable long on RED (size ×0.35); clocks: lookback 🔵 blue — market=RED; parent=YELLOW; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 13 | **FANG** | 🔴🟢🟢🟡🟢🔴 | probable | basket/action net=+7.36; context only, not a company catalyst; Oil & Gas E&P +0.9% d1 / +1.2% 1w / -0.4% vs parent | BUY PROBABLE — most-probable long on RED (size ×0.35); clocks: lookback 🔵 blue — market=RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.40); setup=GREEN; flow=RED; lookback=🔵,Cond green |
| 14 | **NVT** | 🔴🟡🟢🟡🟢🔴 | probable | basket/action net=+3.51; context only, not a company catalyst; Electrical Equipment & Parts +4.5% d1 / +13.6% 1w / +12.5% vs parent | BUY PROBABLE — most-probable long on RED (size ×0.35); clocks: lookback 🔵 blue — market=RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.23); setup=GREEN; flow=RED; lookback=🔵,Cond green |
| 15 | **FLEX** | 🔴🟡🟢🟡🟢🟡 | group_leader | no direct company event; Electronic Components +2.1% d1 / +5.2% 1w / +3.9% vs parent | BUY GROUP_LEADER — market=RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **CPB** | 🔴🔴🔴🟡🔴🔴 | Packaged Foods | SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -3.3% |
| 2 | **METC** | 🔴🔴🔴🟡🔴🔴 | Coking Coal | SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -3.4% |
| 3 | **BGS** | 🔴🔴🔴🟡🔴🔴 | Packaged Foods | SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -3.3% |
| 4 | **RZLV** | 🔴🟡🔴🟡🔴🔴 | Software - Infrastructure | SELL/AVOID — market=RED; red domains=child,setup,flow; child lags parent -4.4% |
| 5 | **SPIR** | 🔴🟡🔴🟡🔴🔴 | Specialty Business Services | SELL/AVOID — market=RED; red domains=child,setup,flow; child lags parent -3.6% |
| 6 | **CLPT** | 🔴🔴🔴🟡🔴🔴 | Medical Devices | SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -3.2% |
| 7 | **SLQT** | 🔴🔴🔴🟡🔴🔴 | Insurance Brokers | SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -3.9% |
| 8 | **BYND** | 🔴🔴🔴🟡🟢🔴 | Packaged Foods | SELL/AVOID — market=RED; red domains=parent,child,flow; child lags parent -3.3% |
| 9 | **APLD** | 🔴🟡🔴🟡🔴🔴 | Information Technology Services | SELL/AVOID — market=RED; red domains=child,setup,flow; child lags parent -4.7% |
| 10 | **DVLT** | 🔴🟡🔴🟡🔴🔴 | Software - Infrastructure | SELL/AVOID — market=RED; red domains=child,setup,flow; child lags parent -4.4% |
| 11 | **XHG** | 🔴🔴🔴🟡🔴🔴 | Insurance Brokers | SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -3.9% |
| 12 | **WHR** | 🔴🔴🔴🟡🔴🔴 | Furnishings, Fixtures & Appliances | SELL/AVOID — market=RED; red domains=parent,child,setup,flow |
| 13 | **FLO** | 🔴🔴🔴🟡🔴🟡 | Packaged Foods | SELL/AVOID — market=RED; red domains=parent,child,setup; child lags parent -3.3% |
| 14 | **AORT** | 🔴🔴🔴🟡🔴🔴 | Medical Devices | SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -3.2% |
| 15 | **GIS** | 🔴🔴🔴🟡🔴🟡 | Packaged Foods | SELL/AVOID — market=RED; red domains=parent,child,setup; child lags parent -3.3% |

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
| Financial | -1.2% | +0.1% | -0.28 |  |
| Healthcare | -2.5% | -1.8% | -0.55 |  |
| Industrials | +0.3% | +1.1% | +0.00 |  |
| Real Estate | -0.2% | -0.5% | -0.47 |  |
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
| Consumer Cyclical | -0.60 |
| Healthcare | -0.55 |
| Basic Materials | -0.47 |
| Energy | +0.47 |
| Real Estate | -0.47 |
| Consumer Defensive | -0.28 |
| Financial | -0.28 |
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

## Horizon weights — book_policy.json v11

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.12 | 0.10 | 0.08 | 0.25 | 0.25 | 0.20 | additive |
| 3d | 0.16 | 0.14 | 0.08 | 0.16 | 0.26 | 0.20 | additive |
| 1w | 0.18 | 0.16 | 0.08 | 0.10 | 0.28 | 0.20 | additive |
| 2w | 0.20 | 0.18 | 0.08 | 0.06 | 0.28 | 0.20 | additive |
| 1m | 0.22 | 0.20 | 0.08 | 0.00 | 0.30 | 0.20 | additive |

## 1d BUY — why these names

### 1. DINO · $19.0B large · Energy

**1d score +0.194**

**DINO** is a liquid **large-cap** Energy name (Oil & Gas Refining & Marketing) at $19.0B, ADV ~2632k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.45 | +0.054 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.27 | +0.027 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.08 | -0.007 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.96 | +0.241 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.24 | +0.048 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.194** | |

### 2. BG · $23.0B large · Consumer Defensive

**1d score +0.133**

**BG** is a liquid **large-cap** Consumer Defensive name (Farm Products) at $23.0B, ADV ~1516k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather are a **headwind** (sector stamp or hostile tape).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | -0.77 | -0.092 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | -0.48 | -0.048 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.08 | -0.007 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.94 | +0.235 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.08 | +0.016 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1d total** | | | **+0.133** | |

### 3. AEHR · $2.9B mid · Technology

**1d score +0.505**

**AEHR** is a liquid **mid-cap** Technology name (Semiconductor Equipment & Materials) at $2.9B, ADV ~2576k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | -0.15 | -0.018 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.55 | -0.044 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.76 | +0.190 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.78 | +0.157 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.505** | |

### 4. SANM · $10.9B large · Technology

**1d score +0.281**

**SANM** is a liquid **large-cap** Technology name (Electronic Components) at $10.9B, ADV ~1010k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.34 | +0.041 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.55 | -0.044 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.88 | +0.220 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.07 | +0.014 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1d total** | | | **+0.281** | |

### 5. NXT · $12.8B large · Technology

**1d score +0.314**

**NXT** is a liquid **large-cap** Technology name (Solar) at $12.8B, ADV ~2991k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.45 | +0.054 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.55 | -0.044 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.55 | +0.139 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.17 | +0.035 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.11 | +0.110 | liquid small/mid, room to run |
| **1d total** | | | **+0.314** | |

### 6. DK · $4.5B mid · Energy

**1d score +0.396**

**DK** is a liquid **mid-cap** Energy name (Oil & Gas Refining & Marketing) at $4.5B, ADV ~1365k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.55 | +0.066 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.27 | +0.027 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.08 | -0.007 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.93 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | -0.61 | -0.122 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.396** | |


## 1d AVOID — bottom of the same rank

- **CPB** (mid, Consumer Defensive, $6.3B) score -0.387. SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -3.3%
- **METC** (small, Basic Materials, $759M) score -0.504. SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -3.4%
- **BGS** (micro, Consumer Defensive, $267M) score -0.430. SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -3.3%
- **RZLV** (small, Technology, $901M) score -0.222. SELL/AVOID — market=RED; red domains=child,setup,flow; child lags parent -4.4%
- **SPIR** (small, Industrials, $475M) score -0.358. SELL/AVOID — market=RED; red domains=child,setup,flow; child lags parent -3.6%
- **CLPT** (small, Healthcare, $427M) score -0.239. SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -3.2%
- **SLQT** (micro, Financial, $92M) score -0.506. SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -3.9%
- **BYND** (micro, Consumer Defensive, $201M) score -0.276. SELL/AVOID — market=RED; red domains=parent,child,flow; child lags parent -3.3%

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | MIR | +0.684 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 2 | UGP | +0.679 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | BKV | +0.639 | mid | Energy | the Finviz industry was **advancing** |
| 4 | ATRO | +0.628 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | TDS | +0.612 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | MGY | +0.602 | mid | Energy | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | CIG | +0.579 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | CHRD | +0.577 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | MTCH | +0.537 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | GEO | +0.522 | mid | Industrials | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 11 | XIFR | +0.519 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | AEHR | +0.518 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | EVER | +0.503 | small | Communication Services | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | TPC | +0.493 | mid | Industrials | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 15 | IRDM | +0.471 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 16 | MWH | +0.405 | mid | Utilities | the Finviz industry was **advancing** |
| 17 | IMSR | +0.383 | small | Utilities | the Finviz industry was **advancing** |
| 18 | NXT | +0.349 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 19 | HLI | +0.338 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 20 | SANM | +0.315 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 21 | SUZ | +0.305 | large | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 22 | JEF | +0.287 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 23 | MEOH | +0.236 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 24 | NEXA | +0.098 | small | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | UGP | +0.715 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | MIR | +0.707 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 3 | BKV | +0.680 | mid | Energy | the Finviz industry was **advancing** |
| 4 | ATRO | +0.651 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | MGY | +0.640 | mid | Energy | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | TDS | +0.633 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | CHRD | +0.611 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | CIG | +0.599 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | AEHR | +0.578 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | MTCH | +0.567 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 11 | GEO | +0.551 | mid | Industrials | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 12 | XIFR | +0.535 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | EVER | +0.531 | small | Communication Services | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | TPC | +0.524 | mid | Industrials | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 15 | IRDM | +0.485 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 16 | MWH | +0.420 | mid | Utilities | the Finviz industry was **advancing** |
| 17 | NXT | +0.416 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 18 | IMSR | +0.399 | small | Utilities | the Finviz industry was **advancing** |
| 19 | SANM | +0.387 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 20 | HLI | +0.365 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 21 | SUZ | +0.319 | large | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 22 | JEF | +0.313 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 23 | MEOH | +0.246 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 24 | NEXA | +0.099 | small | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | UGP | +0.738 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | MIR | +0.731 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 3 | BKV | +0.720 | mid | Energy | the Finviz industry was **advancing** |
| 4 | ATRO | +0.677 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | MGY | +0.665 | mid | Energy | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | CIG | +0.650 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | TDS | +0.643 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | CHRD | +0.638 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | AEHR | +0.621 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | MTCH | +0.620 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 11 | XIFR | +0.596 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | TPC | +0.572 | mid | Industrials | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 13 | GEO | +0.568 | mid | Industrials | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 14 | EVER | +0.548 | small | Communication Services | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | IRDM | +0.505 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 16 | MWH | +0.480 | mid | Utilities | the Finviz industry was **advancing** |
| 17 | IMSR | +0.478 | small | Utilities | the Finviz industry was **advancing** |
| 18 | NXT | +0.472 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 19 | SANM | +0.440 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 20 | HLI | +0.438 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 21 | JEF | +0.405 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 22 | SUZ | +0.323 | large | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 23 | MEOH | +0.254 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 24 | NEXA | +0.109 | small | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |

## 1m BUY — why these names

### 1. UGP · $7.8B mid · Energy

**1m score +0.773**

**UGP** is a liquid **mid-cap** Energy name (Oil & Gas Refining & Marketing) at $7.8B, ADV ~3585k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.58 | +0.127 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.27 | +0.054 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.93 | +0.278 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.58 | +0.115 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.773** | |

### 2. BKV · $2.9B mid · Energy

**1m score +0.760**

**BKV** is a liquid **mid-cap** Energy name (Oil & Gas E&P) at $2.9B, ADV ~889k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.79 | +0.174 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.27 | +0.054 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.94 | +0.282 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.760** | |

### 3. MIR · $4.2B mid · Industrials

**1m score +0.752**

**MIR** is a liquid **mid-cap** Industrials name (Specialty Industrial Machinery) at $4.2B, ADV ~4028k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.43 | +0.094 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.97 | +0.194 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.752** | |

### 4. MGY · $6.4B mid · Energy

**1m score +0.703**

**MGY** is a liquid **mid-cap** Energy name (Oil & Gas E&P) at $6.4B, ADV ~4389k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.70 | +0.153 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.27 | +0.054 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.93 | +0.278 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | -0.08 | -0.016 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.02 | -0.016 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.703** | |

### 5. ATRO · $3.3B mid · Industrials

**1m score +0.698**

**ATRO** is a liquid **mid-cap** Industrials name (Aerospace & Defense) at $3.3B, ADV ~720k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.55 | +0.122 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.211 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.78 | +0.155 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.698** | |

### 6. CIG · $4.1B mid · Utilities

**1m score +0.675**

**CIG** is a liquid **mid-cap** Utilities name (Utilities - Regulated Electric) at $4.1B, ADV ~5869k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.04 | +0.008 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.25 | +0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.93 | +0.278 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.69 | +0.139 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.675** | |

### 7. CHRD · $8.3B mid · Energy

**1m score +0.671**

**CHRD** is a liquid **mid-cap** Energy name (Oil & Gas E&P) at $8.3B, ADV ~786k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.82 | +0.181 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.27 | +0.054 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.55 | +0.166 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.10 | +0.021 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.671** | |

### 8. TDS · $4.1B mid · Communication Services

**1m score +0.664**

**TDS** is a liquid **mid-cap** Communication Services name (Telecom Services) at $4.1B, ADV ~1127k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.02 | +0.004 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.88 | +0.177 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.664** | |

### 9. AEHR · $2.9B mid · Technology

**1m score +0.647**

**AEHR** is a liquid **mid-cap** Technology name (Semiconductor Equipment & Materials) at $2.9B, ADV ~2576k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.15 | -0.033 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.48 | +0.095 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.78 | +0.157 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.647** | |

### 10. MTCH · $9.5B mid · Communication Services

**1m score +0.647**

**MTCH** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $9.5B, ADV ~3299k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.58 | +0.127 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.55 | +0.166 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.32 | +0.063 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.647** | |

### 11. XIFR · $2.3B mid · Utilities

**1m score +0.616**

**XIFR** is a liquid **mid-cap** Utilities name (Utilities - Renewable) at $2.3B, ADV ~914k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.15 | -0.033 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.25 | +0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.39 | +0.078 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.616** | |

### 12. TPC · $4.6B mid · Industrials

**1m score +0.600**

**TPC** is a liquid **mid-cap** Industrials name (Engineering & Construction) at $4.6B, ADV ~704k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.73 | +0.160 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | -0.12 | -0.025 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.600** | |

### 13. GEO · $4.1B mid · Industrials

**1m score +0.596**

**GEO** is a liquid **mid-cap** Industrials name (Security & Protection Services) at $4.1B, ADV ~2095k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.76 | +0.167 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | -0.32 | -0.063 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.596** | |

### 14. EVER · $874M small · Communication Services

**1m score +0.576**

**EVER** is a liquid **small-cap** Communication Services name (Internet Content & Information) at $874M, ADV ~671k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.36 | +0.078 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | -0.23 | -0.047 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.576** | |

### 15. IRDM · $5.2B mid · Communication Services

**1m score +0.517**

**IRDM** is a liquid **mid-cap** Communication Services name (Telecom Services) at $5.2B, ADV ~1998k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big miss**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.15 | -0.033 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.55 | +0.166 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.47 | +0.093 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.517** | |

### 16. NXT · $12.8B large · Technology

**1m score +0.506**

**NXT** is a liquid **large-cap** Technology name (Solar) at $12.8B, ADV ~2991k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.45 | +0.100 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.48 | +0.095 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.55 | +0.166 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.17 | +0.035 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.11 | +0.110 | liquid small/mid, room to run |
| **1m total** | | | **+0.506** | |

### 17. MWH · $5.1B mid · Utilities

**1m score +0.499**

**MWH** is a liquid **mid-cap** Utilities name (Utilities - Renewable) at $5.1B, ADV ~2005k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.04 | +0.008 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.25 | +0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.64 | +0.191 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.499** | |

### 18. IMSR · $563M small · Utilities

**1m score +0.495**

**IMSR** is a liquid **small-cap** Utilities name (Utilities - Regulated Electric) at $563M, ADV ~1849k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.15 | -0.033 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.25 | +0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.495** | |

### 19. SANM · $10.9B large · Technology

**1m score +0.478**

**SANM** is a liquid **large-cap** Technology name (Electronic Components) at $10.9B, ADV ~1010k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.34 | +0.075 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.48 | +0.095 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.07 | +0.014 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1m total** | | | **+0.478** | |

### 20. HLI · $9.6B mid · Financial

**1m score +0.465**

**HLI** is a liquid **mid-cap** Financial name (Capital Markets) at $9.6B, ADV ~853k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **miss**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.92 | +0.201 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.64 | +0.191 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.32 | +0.063 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.465** | |

### 21. JEF · $12.7B large · Financial

**1m score +0.429**

**JEF** is a liquid **large-cap** Financial name (Capital Markets) at $12.7B, ADV ~2096k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **miss**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.93 | +0.205 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.46 | +0.139 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.38 | +0.075 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.429** | |

### 22. SUZ · $11.0B large · Basic Materials

**1m score +0.336**

**SUZ** is a liquid **large-cap** Basic Materials name (Paper & Paper Products) at $11.0B, ADV ~3217k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.40 | +0.088 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.47 | -0.094 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.211 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.65 | +0.130 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.336** | |

### 23. MEOH · $4.5B mid · Basic Materials

**1m score +0.263**

**MEOH** is a liquid **mid-cap** Basic Materials name (Chemicals) at $4.5B, ADV ~871k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.58 | +0.127 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.47 | -0.094 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.36 | +0.108 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.36 | +0.072 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.263** | |

### 24. NEXA · $1.8B small · Basic Materials

**1m score +0.108**

**NEXA** is a liquid **small-cap** Basic Materials name (Other Industrial Metals & Mining) at $1.8B, ADV ~751k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **miss**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.05 | +0.012 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.47 | -0.094 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.36 | +0.108 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.16 | +0.032 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.108** | |


## 1m AVOID — bottom of the same rank

- **XPOF** (micro, Consumer Cyclical, $242M) score -0.765. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LODE** (micro, Basic Materials, $236M) score -0.715. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FUN** (small, Consumer Cyclical, $1.6B) score -0.699. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **XHR** (small, Real Estate, $1.7B) score -0.689. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NUS** (micro, Consumer Defensive, $236M) score -0.674. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **PACK** (small, Consumer Cyclical, $389M) score -0.670. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **WHR** (mid, Consumer Cyclical, $2.5B) score -0.664. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **AIIO** (micro, Consumer Cyclical, $220M) score -0.656. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OPEN** (mid, Real Estate, $3.0B) score -0.653. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**
- **CPNG** (large, Consumer Cyclical, $27.5B) score -0.651. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SGML** (small, Basic Materials, $1.3B) score -0.648. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BHR** (micro, Real Estate, $130M) score -0.646. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GIS** (large, Consumer Defensive, $20.4B) score -0.643. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TMC** (small, Basic Materials, $1.9B) score -0.635. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **DFH** (small, Consumer Cyclical, $1.2B) score -0.634. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TROX** (small, Basic Materials, $768M) score -0.630. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**
- **COLD** (mid, Real Estate, $4.1B) score -0.625. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OI** (small, Consumer Cyclical, $1.1B) score -0.614. this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NAK** (small, Basic Materials, $832M) score -0.614. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **BGS** (micro, Consumer Defensive, $267M) score -0.609. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **METC** (small, Basic Materials, $759M) score -0.606. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TMQ** (small, Basic Materials, $579M) score -0.601. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **KLC** (small, Consumer Defensive, $316M) score -0.587. the Finviz industry was **down**
- **JBGS** (small, Real Estate, $873M) score -0.584. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ENHA** (small, Consumer Defensive, $628M) score -0.581. the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-09-09_stock_book.md`
- Machine table: `data/stock_book/2026-09-09_stock_book.csv`
- Machine book: `data/stock_book/2026-09-09_stock_book.json`
- Join rank: `data/join/2026-09-09_ranked.csv`
- Weather: `01_daily/weather/2026-09-09_weather.md`
- AB enrich: `data/ab_checklist/2026-09-09_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-09-09_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-09-09_map_heat.md`
