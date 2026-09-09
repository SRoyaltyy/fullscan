# Stock book — 2026-09-09

_Generated 2026-09-09T10:39:00.712447-04:00_

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
- Stand-down: **no** — 217 names qualified through standard,group_leader,catalyst (136 probable)
- Sector predicts this date: 10/11 (ok)
- News tickers in play: 112
- AB coverage: 1958 names · peer RS: 1836
- Universe after liquidity: 2067
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 63

## All-green BUY / SELL

- Mode: **weighted_fallback** · SELL **core_weights**
- Pile: **0** liquid all-green names (need ≥ 8) of 2067
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
| 1 | **MSTR** | 🟡🟡🔴🟢🟡🟢 | catalyst | direct high digest (same-day): Strategy Inc shares fall after 8-K reveals doubling of preferred buyback authorization to $2 billion and no new Bitcoin purchases for a second straight week.; Software - Application -3.5% d1 / -7.0% 1w / -8.3% vs parent | BUY CATALYST — market=YELLOW; parent=YELLOW; child=RED/rel=RED; company=GREEN(0.72); setup=YELLOW; flow=GREEN |
| 2 | **ABT** | 🟡🔴🔴🟢🟢🔴 | probable | direct high digest (same-day): Abbott receives FDA approval for TactiFlex Duo dual-energy ablation catheter to treat complex atrial fibrillation, expanding its U.S. pulsed field ablation portfolio; Medical Devices -3.5% d1 / -5.0% 1w / -3.2% vs parent | BUY PROBABLE — most-probable long on YELLOW (size ×0.60); clocks: company news fresh (0.72) — market=YELLOW; parent=RED; child=RED/rel=RED; company=GREEN(0.72); setup=GREEN; flow=RED; lookback=Cond green |
| 3 | **UGP** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Oil & Gas Refining & Marketing +2.4% d1 / +5.9% 1w / +4.3% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 4 | **TER** | 🟡🟡🟢🟡🟢🟢 | group_leader | basket/action net=+1.80; context only, not a company catalyst; Semiconductor Equipment & Materials +3.3% d1 / +5.0% 1w / +3.7% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.12); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 5 | **VLO** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Oil & Gas Refining & Marketing +2.4% d1 / +5.9% 1w / +4.3% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 6 | **NVT** | 🟡🟡🟢🟡🟢🟡 | group_leader | basket/action net=+3.51; context only, not a company catalyst; Electrical Equipment & Parts +4.5% d1 / +13.6% 1w / +12.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.23); setup=GREEN; flow=YELLOW; lookback=🔵,⚪,Cond green |
| 7 | **BG** | 🟡🔴🟢🟡🟢🟢 | group_leader | no direct company event; Farm Products +1.5% d1 / +2.5% 1w / +3.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 8 | **GPRK** | 🟡🟢🟢🟡🟢🟢 | standard | no direct company event; Oil & Gas E&P +0.9% d1 / +1.2% 1w / -0.4% vs parent | BUY STANDARD — market=YELLOW; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 9 | **CWEN** | 🟡🟢🟢🟡🟢🟢 | standard | no direct company event; Utilities - Renewable +1.9% d1 / +4.2% 1w / +1.4% vs parent | BUY STANDARD — market=YELLOW; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 10 | **TALO** | 🟡🟢🟢🟡🟢🟢 | standard | no direct company event; Oil & Gas E&P +0.9% d1 / +1.2% 1w / -0.4% vs parent | BUY STANDARD — market=YELLOW; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 11 | **BE** | 🟡🟡🟢🟡🟢🔴 | probable | direct normal digest (same-day): Clear Street lifts Bloom Energy target to $330 on expected S&P 500 demand as UBS raises target to $325; Electrical Equipment & Parts +4.5% d1 / +13.6% 1w / +12.5% vs parent | BUY PROBABLE — most-probable long on YELLOW (size ×0.60); clocks: lookback 🔵 blue — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.42); setup=GREEN; flow=RED; lookback=🔵,Cond green |
| 12 | **HAYW** | 🟡🟡🟢🟡🟢🟡 | group_leader | no direct company event; Electrical Equipment & Parts +4.5% d1 / +13.6% 1w / +12.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,Cond green |
| 13 | **AEHR** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Semiconductor Equipment & Materials +3.3% d1 / +5.0% 1w / +3.7% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 14 | **QRVO** | 🟡🟡🟢🟡🟢🟡 | standard | basket/action net=+2.52; context only, not a company catalyst; Semiconductors +1.0% d1 / +4.2% 1w / +2.9% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=GREEN/rel=YELLOW; company=YELLOW(0.17); setup=GREEN; flow=YELLOW; lookback=🔵,⚪,Cond green |
| 15 | **CVE** | 🟡🟢🟢🟡🟢🔴 | probable | direct high digest (stale/undated): Cenovus Energy Q2 2026 non-GAAP EPS $1.08 misses estimates, revenue $14.7B beats, company raises full-year production guidance; Oil & Gas Integrated +1.6% d1 / +2.0% 1w / +0.4% vs parent | BUY PROBABLE — most-probable long on YELLOW (size ×0.60); clocks: lookback 🔵 blue — market=YELLOW; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.48); setup=GREEN; flow=RED; lookback=🔵,Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **METC** | 🟡🔴🔴🟡🔴🔴 | Coking Coal | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.4% |
| 2 | **CPB** | 🟡🔴🔴🟡🔴🔴 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.3% |
| 3 | **GIS** | 🟡🔴🔴🟡🔴🔴 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.3% |
| 4 | **FLO** | 🟡🔴🔴🟡🔴🔴 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.3% |
| 5 | **TRIP** | 🟡🔴🔴🟡🔴🔴 | Travel Services | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.2% |
| 6 | **HRL** | 🟡🔴🔴🟡🔴🔴 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.3% |
| 7 | **XHG** | 🟡🔴🔴🟡🔴🔴 | Insurance Brokers | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.9% |
| 8 | **ENHA** | 🟡🔴🔴🟡🔴🔴 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.3% |
| 9 | **CCL** | 🟡🔴🔴🟡🔴🔴 | Travel Services | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.2% |
| 10 | **SMPL** | 🟡🔴🔴🟡🔴🔴 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.3% |
| 11 | **ZBH** | 🟡🔴🔴🟡🔴🔴 | Medical Devices | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.2% |
| 12 | **BKNG** | 🟡🔴🔴🟡🟡🔴 | Travel Services | SELL/AVOID — market=YELLOW; red domains=parent,child,flow; child lags parent -4.2% |
| 13 | **NG** | 🟡🔴🔴🟡🔴🔴 | Gold | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 14 | **NN** | 🟡🟡🔴🟡🔴🔴 | Software - Infrastructure | SELL/AVOID — market=YELLOW; red domains=child,setup,flow; child lags parent -4.4% |
| 15 | **AORT** | 🟡🔴🔴🟡🔴🔴 | Medical Devices | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.2% |

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

### 1. MSTR · $53.9B large · Technology

**1d score +0.303**

**MSTR** is a liquid **large-cap** Technology name (Software - Application) at $53.9B, ADV ~23386k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big miss**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | -0.13 | -0.017 | does this *kind* of stock fit today's regime? |
| sector predict | 0.11 | +0.20 | +0.022 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.27 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.27 | +0.00 | +0.000 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.96 | +0.210 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.02 | -0.021 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.11 | +0.110 | liquid small/mid, room to run |
| **1d total** | | | **+0.303** | |

### 2. UGP · $8.1B mid · Energy

**1d score +0.786**

**UGP** is a liquid **mid-cap** Energy name (Oil & Gas Refining & Marketing) at $8.1B, ADV ~3582k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.97 | +0.127 | does this *kind* of stock fit today's regime? |
| sector predict | 0.11 | +0.27 | +0.029 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.27 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.27 | +0.93 | +0.251 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.59 | +0.129 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.786** | |

### 3. TER · $57.1B large · Technology

**1d score +0.516**

**TER** is a liquid **large-cap** Technology name (Semiconductor Equipment & Materials) at $57.1B, ADV ~3899k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.65 | +0.085 | does this *kind* of stock fit today's regime? |
| sector predict | 0.11 | +0.20 | +0.022 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.27 | +0.35 | +0.094 | headlines + news-judge ticker tilts |
| AB checklist | 0.27 | +0.96 | +0.262 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.25 | +0.054 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1d total** | | | **+0.516** | |

### 4. VLO · $111.7B large · Energy

**1d score +0.465**

**VLO** is a liquid **large-cap** Energy name (Oil & Gas Refining & Marketing) at $111.7B, ADV ~2845k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.97 | +0.126 | does this *kind* of stock fit today's regime? |
| sector predict | 0.11 | +0.27 | +0.029 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.27 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.27 | +0.93 | +0.251 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.32 | +0.069 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.05 | +0.049 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.11 | -0.110 | liquid small/mid, room to run |
| **1d total** | | | **+0.465** | |

### 5. NVT · $25.9B large · Industrials

**1d score +0.681**

**NVT** is a liquid **large-cap** Industrials name (Electrical Equipment & Parts) at $25.9B, ADV ~2283k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.89 | +0.115 | does this *kind* of stock fit today's regime? |
| sector predict | 0.11 | -0.20 | -0.022 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.27 | +0.61 | +0.165 | headlines + news-judge ticker tilts |
| AB checklist | 0.27 | +0.94 | +0.256 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.77 | +0.167 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1d total** | | | **+0.681** | |

### 6. BG · $23.8B large · Consumer Defensive

**1d score +0.220**

**BG** is a liquid **large-cap** Consumer Defensive name (Farm Products) at $23.8B, ADV ~1537k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather are a **headwind** (sector stamp or hostile tape).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | -0.69 | -0.090 | does this *kind* of stock fit today's regime? |
| sector predict | 0.11 | -0.28 | -0.030 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.27 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.27 | +0.94 | +0.256 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.26 | +0.056 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1d total** | | | **+0.220** | |

### 7. GPRK · $792M small · Energy

**1d score +0.731**

**GPRK** is a liquid **small-cap** Energy name (Oil & Gas E&P) at $792M, ADV ~606k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.65 | +0.085 | does this *kind* of stock fit today's regime? |
| sector predict | 0.11 | +0.27 | +0.029 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.27 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.27 | +0.88 | +0.239 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.77 | +0.168 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.16 | +0.160 | liquid small/mid, room to run |
| **1d total** | | | **+0.731** | |

### 8. CWEN · $6.8B mid · Utilities

**1d score +0.653**

**CWEN** is a liquid **mid-cap** Utilities name (Utilities - Renewable) at $6.8B, ADV ~1338k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.28 | +0.037 | does this *kind* of stock fit today's regime? |
| sector predict | 0.11 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.27 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.27 | +0.93 | +0.251 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.53 | +0.115 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.653** | |


## 1d AVOID — bottom of the same rank

- **METC** (small, Basic Materials, $717M) score -0.565. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.4%
- **CPB** (mid, Consumer Defensive, $6.5B) score -0.347. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.3%
- **GIS** (large, Consumer Defensive, $20.2B) score -0.486. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.3%
- **FLO** (small, Consumer Defensive, $1.3B) score -0.324. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.3%
- **TRIP** (small, Consumer Cyclical, $1.1B) score -0.276. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.2%
- **HRL** (large, Consumer Defensive, $11.7B) score -0.212. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.3%
- **XHG** (micro, Financial, $141M) score -0.375. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.9%
- **ENHA** (small, Consumer Defensive, $605M) score -0.347. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.3%

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | UGP | +0.843 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | WTTR | +0.797 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | GPRK | +0.774 | small | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | GEO | +0.768 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | ARLO | +0.766 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | CMBT | +0.759 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | QRVO | +0.756 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | CIG | +0.718 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | CECO | +0.716 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | CMC | +0.677 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | CWEN | +0.676 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | TDS | +0.669 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | XIFR | +0.578 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 14 | XP | +0.573 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 15 | AEHR | +0.562 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 16 | ENIC | +0.538 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 17 | TER | +0.530 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 18 | NOK | +0.509 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 19 | BMRN | +0.443 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 20 | MIAX | +0.442 | mid | Financial | the Finviz industry was **advancing** |
| 21 | SNEX | +0.435 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 22 | HAPN | +0.425 | mid | Financial | the Finviz industry was **advancing** |
| 23 | ORIC | +0.418 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 24 | BZ | +0.388 | mid | Communication Services | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 25 | MTCH | +0.378 | mid | Communication Services | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | UGP | +0.882 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | WTTR | +0.837 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | GPRK | +0.805 | small | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | GEO | +0.801 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | ARLO | +0.800 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | CMBT | +0.799 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | QRVO | +0.795 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | CIG | +0.746 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | CECO | +0.741 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | TDS | +0.731 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | CMC | +0.705 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | CWEN | +0.702 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | AEHR | +0.623 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 14 | XP | +0.607 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 15 | XIFR | +0.596 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 16 | TER | +0.590 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 17 | NOK | +0.580 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 18 | ENIC | +0.556 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 19 | BMRN | +0.482 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 20 | MIAX | +0.480 | mid | Financial | the Finviz industry was **advancing** |
| 21 | SNEX | +0.469 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 22 | HAPN | +0.461 | mid | Financial | the Finviz industry was **advancing** |
| 23 | BZ | +0.453 | mid | Communication Services | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 24 | ORIC | +0.451 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 25 | MTCH | +0.442 | mid | Communication Services | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | UGP | +0.832 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | ARLO | +0.815 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | GEO | +0.815 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | CIG | +0.804 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | WTTR | +0.787 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | QRVO | +0.787 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | CWEN | +0.757 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | CMBT | +0.749 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | CECO | +0.749 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | GPRK | +0.748 | small | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | TDS | +0.739 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | CMC | +0.713 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | XP | +0.670 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 14 | XIFR | +0.643 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 15 | AEHR | +0.630 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 16 | ENIC | +0.603 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 17 | TER | +0.599 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 18 | NOK | +0.596 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 19 | BMRN | +0.590 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 20 | ORIC | +0.557 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 21 | MIAX | +0.543 | mid | Financial | the Finviz industry was **advancing** |
| 22 | SNEX | +0.531 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 23 | HAPN | +0.523 | mid | Financial | the Finviz industry was **advancing** |
| 24 | BZ | +0.478 | mid | Communication Services | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 25 | MTCH | +0.460 | mid | Communication Services | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |

## 1m BUY — why these names

### 1. UGP · $8.1B mid · Energy

**1m score +0.869**

**UGP** is a liquid **mid-cap** Energy name (Oil & Gas Refining & Marketing) at $8.1B, ADV ~3582k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.97 | +0.232 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | -0.20 | -0.043 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.93 | +0.302 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.59 | +0.129 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.869** | |

### 2. CIG · $4.2B mid · Utilities

**1m score +0.838**

**CIG** is a liquid **mid-cap** Utilities name (Utilities - Regulated Electric) at $4.2B, ADV ~5730k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.41 | +0.098 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.25 | +0.054 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.91 | +0.295 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.65 | +0.141 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.838** | |

### 3. WTTR · $2.7B mid · Energy

**1m score +0.825**

**WTTR** is a liquid **mid-cap** Energy name (Oil & Gas Equipment & Services) at $2.7B, ADV ~1723k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.97 | +0.231 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | -0.20 | -0.043 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.97 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.32 | +0.070 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.825** | |

### 4. ARLO · $1.5B small · Industrials

**1m score +0.806**

**ARLO** is a liquid **small-cap** Industrials name (Building Products & Equipment) at $1.5B, ADV ~1281k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.87 | +0.207 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | -0.40 | -0.087 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.93 | +0.302 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.62 | +0.135 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.05 | +0.049 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.806** | |

### 5. GEO · $4.2B mid · Industrials

**1m score +0.805**

**GEO** is a liquid **mid-cap** Industrials name (Security & Protection Services) at $4.2B, ADV ~2077k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.81 | +0.193 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | -0.40 | -0.087 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.94 | +0.307 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.52 | +0.114 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.805** | |

### 6. QRVO · $9.1B mid · Technology

**1m score +0.794**

**QRVO** is a liquid **mid-cap** Technology name (Semiconductors) at $9.1B, ADV ~1190k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.12 | +0.029 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.48 | +0.103 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.47 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.88 | +0.287 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.57 | +0.124 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.794** | |

### 7. CWEN · $6.8B mid · Utilities

**1m score +0.789**

**CWEN** is a liquid **mid-cap** Utilities name (Utilities - Renewable) at $6.8B, ADV ~1338k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.28 | +0.068 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.25 | +0.054 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.93 | +0.302 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.53 | +0.115 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.789** | |

### 8. CMBT · $5.6B mid · Energy

**1m score +0.787**

**CMBT** is a liquid **mid-cap** Energy name (Oil & Gas Midstream) at $5.6B, ADV ~1059k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.98 | +0.235 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | -0.20 | -0.043 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.95 | +0.311 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.16 | +0.034 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.787** | |

### 9. TRMD · $3.6B mid · Energy

**1m score +0.781**

**TRMD** is a liquid **mid-cap** Energy name (Oil & Gas Midstream) at $3.6B, ADV ~733k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.95 | +0.227 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | -0.20 | -0.043 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.88 | +0.287 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.28 | +0.060 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.781** | |

### 10. TDS · $4.1B mid · Communication Services

**1m score +0.762**

**TDS** is a liquid **mid-cap** Communication Services name (Telecom Services) at $4.1B, ADV ~1151k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.12 | +0.029 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.25 | +0.054 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.70 | +0.230 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.92 | +0.199 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.762** | |

### 11. CECO · $4.7B mid · Industrials

**1m score +0.730**

**CECO** is a liquid **mid-cap** Industrials name (Pollution & Treatment Controls) at $4.7B, ADV ~867k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.57 | +0.136 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | -0.40 | -0.087 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.76 | +0.248 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.84 | +0.182 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.730** | |

### 12. XP · $10.3B large · Financial

**1m score +0.707**

**XP** is a liquid **large-cap** Financial name (Capital Markets) at $10.3B, ADV ~5322k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **extended**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.97 | +0.232 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.76 | +0.248 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.82 | +0.177 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.707** | |

### 13. CMC · $7.9B mid · Industrials

**1m score +0.698**

**CMC** is a liquid **mid-cap** Industrials name (Metal Fabrication) at $7.9B, ADV ~1248k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.58 | +0.138 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | -0.40 | -0.087 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.93 | +0.302 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.36 | +0.079 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.016 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.698** | |

### 14. XIFR · $2.4B mid · Utilities

**1m score +0.667**

**XIFR** is a liquid **mid-cap** Utilities name (Utilities - Renewable) at $2.4B, ADV ~909k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | -0.06 | -0.015 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.25 | +0.054 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.88 | +0.287 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.41 | +0.090 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.667** | |

### 15. AEHR · $2.9B mid · Technology

**1m score +0.658**

**AEHR** is a liquid **mid-cap** Technology name (Semiconductor Equipment & Materials) at $2.9B, ADV ~2613k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | -0.15 | -0.036 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.48 | +0.103 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.76 | +0.248 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.66 | +0.143 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.658** | |

### 16. NOK · $61.2B large · Technology

**1m score +0.634**

**NOK** is a liquid **large-cap** Technology name (Communication Equipment) at $61.2B, ADV ~88151k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.28 | +0.068 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.48 | +0.103 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.76 | +0.248 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.85 | +0.184 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1m total** | | | **+0.634** | |

### 17. BMRN · $12.5B large · Healthcare

**1m score +0.632**

**BMRN** is a liquid **large-cap** Healthcare name (Biotechnology) at $12.5B, ADV ~1968k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.98 | +0.234 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.95 | +0.311 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.17 | +0.037 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.632** | |

### 18. TER · $57.1B large · Technology

**1m score +0.627**

**TER** is a liquid **large-cap** Technology name (Semiconductor Equipment & Materials) at $57.1B, ADV ~3899k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.65 | +0.155 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.48 | +0.103 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.35 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.96 | +0.314 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.25 | +0.054 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1m total** | | | **+0.627** | |

### 19. ENIC · $6.3B mid · Utilities

**1m score +0.625**

**ENIC** is a liquid **mid-cap** Utilities name (Utilities - Renewable) at $6.3B, ADV ~592k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | -0.09 | -0.022 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.25 | +0.054 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.88 | +0.287 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.48 | +0.105 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.625** | |

### 20. ORIC · $1.3B small · Healthcare

**1m score +0.593**

**ORIC** is a liquid **small-cap** Healthcare name (Biotechnology) at $1.3B, ADV ~1550k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **miss**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.90 | +0.216 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.76 | +0.248 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.36 | +0.079 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.593** | |

### 21. MIAX · $4.3B mid · Financial

**1m score +0.584**

**MIAX** is a liquid **mid-cap** Financial name (Capital Markets) at $4.3B, ADV ~1839k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.97 | +0.232 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.93 | +0.302 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.584** | |

### 22. SNEX · $8.3B mid · Financial

**1m score +0.568**

**SNEX** is a liquid **mid-cap** Financial name (Capital Markets) at $8.3B, ADV ~1247k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.95 | +0.227 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.76 | +0.248 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.20 | +0.043 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.568** | |

### 23. HAPN · $2.0B mid · Financial

**1m score +0.562**

**HAPN** is a liquid **mid-cap** Financial name (Banks - Regional) at $2.0B, ADV ~1764k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.94 | +0.226 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.88 | +0.287 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.562** | |

### 24. BZ · $6.6B mid · Communication Services

**1m score +0.506**

**BZ** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $6.6B, ADV ~4662k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.90 | +0.214 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.25 | +0.054 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.12 | +0.041 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | -0.25 | -0.054 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.506** | |

### 25. MTCH · $9.4B mid · Communication Services

**1m score +0.486**

**MTCH** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $9.4B, ADV ~3319k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.58 | +0.138 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.25 | +0.054 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.36 | +0.117 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | -0.34 | -0.074 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.486** | |


## 1m AVOID — bottom of the same rank

- **SGML** (small, Basic Materials, $1.2B) score -0.739. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **CPNG** (large, Consumer Cyclical, $26.7B) score -0.683. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **METC** (small, Basic Materials, $717M) score -0.676. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **XPOF** (micro, Consumer Cyclical, $235M) score -0.670. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LODE** (micro, Basic Materials, $234M) score -0.649. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **GIS** (large, Consumer Defensive, $20.2B) score -0.593. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PACK** (small, Consumer Cyclical, $364M) score -0.592. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BIRK** (mid, Consumer Cyclical, $5.8B) score -0.582. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **XPEV** (mid, Consumer Cyclical, $8.4B) score -0.574. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TROX** (small, Basic Materials, $767M) score -0.563. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **EAT** (mid, Consumer Cyclical, $9.1B) score -0.560. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **STLA** (large, Consumer Cyclical, $15.5B) score -0.560. this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **MVST** (micro, Consumer Cyclical, $263M) score -0.555. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **HGV** (mid, Consumer Cyclical, $3.1B) score -0.545. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OI** (small, Consumer Cyclical, $1.1B) score -0.540. this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AIIO** (micro, Consumer Cyclical, $233M) score -0.537. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **ROL** (large, Consumer Cyclical, $17.1B) score -0.532. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **KLC** (small, Consumer Defensive, $302M) score -0.523. the Finviz industry was **down**
- **CWH** (small, Consumer Cyclical, $672M) score -0.516. this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**
- **DKS** (large, Consumer Cyclical, $11.7B) score -0.516. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **DASH** (large, Consumer Cyclical, $86.6B) score -0.512. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ECX** (small, Consumer Cyclical, $357M) score -0.510. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**
- **TMC** (small, Basic Materials, $2.0B) score -0.505. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **NAK** (small, Basic Materials, $860M) score -0.499. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **NG** (mid, Basic Materials, $3.5B) score -0.496. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**

## Files for this run

- This rationale: `01_daily/2026-09-09_stock_book.md`
- Machine table: `data/stock_book/2026-09-09_stock_book.csv`
- Machine book: `data/stock_book/2026-09-09_stock_book.json`
- Join rank: `data/join/2026-09-09_ranked.csv`
- Weather: `01_daily/weather/2026-09-09_weather.md`
- AB enrich: `data/ab_checklist/2026-09-09_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-09-09_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-09-09_map_heat.md`
