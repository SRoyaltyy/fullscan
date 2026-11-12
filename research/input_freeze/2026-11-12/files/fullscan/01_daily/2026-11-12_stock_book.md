# Stock book — 2026-11-12

_Generated 2026-11-12T09:19:18.595051-05:00_

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
- General predict (same-day): -0.67 down (present)
- Stand-down: **no** — 37 names qualified through group_leader,catalyst,probable (6 probable)
- Sector predicts this date: 0/11 (missing → sector layer is 0; Finviz week tape still sits in join)
- News tickers in play: 120
- AB coverage: 2097 names · peer RS: 1983
- Universe after liquidity: 2222
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 87

## All-green BUY / SELL

- Mode: **weighted_fallback** · SELL **core_weights**
- Pile: **0** liquid all-green names (need ≥ 8) of 2222
- Core fired: join=yes, AB=yes, peer=yes
- pile 0 < 8 liquid all-green names. Fallback weighted walk; SELL stays on core

## Decision lattice — gate → route → rank

The weighted score is now a tie-breaker inside an eligible lane. It cannot average away a market, group, company, or setup veto.

### MARKET: 🔴 RED

- RED: general down score=-7.16; good=+1.5 vs bad=-7.2; risk=off; red pillars=2
- Allowed long lanes: **group_leader, catalyst, probable** · max slots 8 · size ×0.35
- Bull evidence: oil / dollar +0.50 points
- Bear evidence: overnight catalysts -6.00 points; futures -0.25 points

Decision domains: **MKT · parent · child · company · setup · flow**. Measured parent/child tape is kept separate from the LLM essay; direct company events must be price-confirmed on a hard-red day.

### Bull decisions (eligible or closest blocked cases)

| # | Ticker | Domains | Lane | Company / group | Decision |
|---:|--------|---------|------|-----------------|----------|
| 1 | **NBIS** | 🔴🔴🟢🟡🟢🟢 | group_leader | direct high digest (stale/undated): CoreWeave raises 2026 revenue outlook to $13 billion–$13.3 billion, a development seen as positive for Nebius Group; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.48); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 2 | **BB** | 🔴🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 3 | **SONO** | 🔴🔴🟢🟡🟢🟢 | group_leader | no direct company event; Consumer Electronics +0.3% d1 / +4.0% 1w / +6.0% vs parent | BUY GROUP_LEADER — market=RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 4 | **PLTR** | 🔴🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 5 | **CHKP** | 🔴🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 6 | **GMRS** | 🔴🟡🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 7 | **RPD** | 🔴🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 8 | **PRGS** | 🔴🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 9 | **WEX** | 🔴🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 10 | **ORCL** | 🔴🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 11 | **PATH** | 🔴🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 12 | **IOT** | 🔴🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 13 | **SNPS** | 🔴🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 14 | **PENG** | 🔴🔴🟢🟡🟢🟢 | group_leader | no direct company event; Information Technology Services +2.9% d1 / +1.9% 1w / +3.9% vs parent | BUY GROUP_LEADER — market=RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 15 | **SABR** | 🔴🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **DDD** | 🔴🔴🔴🟡🔴🔴 | Computer Hardware | SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -3.1% |
| 2 | **QUBT** | 🔴🔴🔴🟡🔴🔴 | Computer Hardware | SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -3.1% |
| 3 | **LTBR** | 🔴🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 4 | **SOC** | 🔴🟡🔴🟡🔴🔴 | Oil & Gas Drilling | SELL/AVOID — market=RED; red domains=child,setup,flow; child lags parent -5.4% |
| 5 | **UNIT** | 🔴🔴🔴🟡🔴🔴 | REIT - Specialty | SELL/AVOID — market=RED; red domains=parent,child,setup,flow |
| 6 | **LWLG** | 🔴🔴🔴🟡🔴🔴 | Specialty Chemicals | SELL/AVOID — market=RED; red domains=parent,child,setup,flow |
| 7 | **HYLN** | 🔴🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 8 | **FLNC** | 🔴🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=RED; red domains=parent,child,setup; child lags parent -4.7% |
| 9 | **ABR** | 🔴🔴🔴🟡🔴🔴 | REIT - Mortgage | SELL/AVOID — market=RED; red domains=parent,child,setup,flow |
| 10 | **FMC** | 🔴🔴🔴🟡🔴🔴 | Agricultural Inputs | SELL/AVOID — market=RED; red domains=parent,child,setup,flow |
| 11 | **ACDC** | 🔴🟡🔴🟡🔴🔴 | Oil & Gas Equipment & Services | SELL/AVOID — market=RED; red domains=child,setup,flow; child lags parent -7.7% |
| 12 | **EU** | 🔴🟡🔴🟡🔴🔴 | Uranium | SELL/AVOID — market=RED; red domains=child,setup,flow; child lags parent -10.3% |
| 13 | **HPP** | 🔴🔴🔴🟡🔴🔴 | REIT - Office | SELL/AVOID — market=RED; red domains=parent,child,setup,flow |
| 14 | **URG** | 🔴🟡🔴🟡🔴🔴 | Uranium | SELL/AVOID — market=RED; red domains=child,setup,flow; child lags parent -10.3% |
| 15 | **JBI** | 🔴🔴🔴🟡🔴🔴 | Building Products & Equipment | SELL/AVOID — market=RED; red domains=parent,child,setup,flow |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **finviz_tape** (40 captains, 15 industries → s_heat).
- Board file: `01_daily/map_heat/2026-11-12_map_heat.json` · generated 2026-11-12T04:47:14.383012-05:00

### Sector RS vs same-day LLM essay

| Sector | Finviz 1d | Finviz 1w | LLM 1d | Tape vs essay |
|--------|----------:|----------:|-------:|---------------|
| Basic Materials | -2.1% | -4.8% | — |  |
| Communication Services | +2.7% | +3.5% | — |  |
| Consumer Cyclical | -0.4% | -2.0% | — |  |
| Consumer Defensive | +1.4% | +0.5% | — |  |
| Energy | -0.8% | +1.0% | — |  |
| Financial | -0.4% | -1.8% | — |  |
| Healthcare | +1.4% | -2.5% | — |  |
| Industrials | -1.6% | -2.7% | — |  |
| Real Estate | -0.6% | -2.1% | — |  |
| Technology | -2.0% | -2.1% | — |  |
| Utilities | -1.5% | -3.2% | — |  |

### Industry heat (1w vs parent)

**HOT**

- **Tobacco** (Consumer Defensive) +2.4% 1d · +5.0% 1w · vs parent +4.5% · PM, MO, TPB, UVV
- **Internet Content & Information** (Communication Services) +3.0% 1d · +4.2% 1w · vs parent +0.8% · GOOGL, GOOG, RUM, CARG
- **Electronic Gaming & Multimedia** (Communication Services) +4.6% 1d · +4.1% 1w · vs parent +0.6% · TTWO
- **Consumer Electronics** (Technology) +0.3% 1d · +4.0% 1w · vs parent +6.0% · AAPL, SONO, GPRO
- **Oil & Gas Integrated** (Energy) -0.5% 1d · +3.3% 1w · vs parent +2.3% · XOM, CVX, DEC
- **Medical Care Facilities** (Healthcare) +0.6% 1d · +3.1% 1w · vs parent +5.6% · HCA, DVA, PACS, LFST
- **Advertising Agencies** (Communication Services) +2.9% 1d · +2.7% 1w · vs parent -0.8% · APP, OMC, MGNI, STGW
- **Publishing** (Communication Services) +3.8% 1d · +2.5% 1w · vs parent -1.0% · WLY, TDAY

**COLD**

- **Coking Coal** (Basic Materials) -3.9% 1d · -10.5% 1w · vs parent -5.7% · HCC, AMR
- **Utilities - Independent Power Producers** (Utilities) -6.0% 1d · -9.5% 1w · vs parent -6.3% · CEG, VST, HNRG
- **Metal Fabrication** (Industrials) -4.1% 1d · -9.4% 1w · vs parent -6.7% · CMC, GPGI
- **Uranium** (Energy) -3.5% 1d · -9.4% 1w · vs parent -10.3% · UEC, UUUU
- **Silver** (Basic Materials) -3.6% 1d · -9.3% 1w · vs parent -4.5% · —
- **Semiconductor Equipment & Materials** (Technology) -7.6% 1d · -8.4% 1w · vs parent -6.3% · LRCX, AMAT, ACMR, KLIC
- **Aluminum** (Basic Materials) -3.6% 1d · -7.5% 1w · vs parent -2.7% · CENX, CSTM
- **Recreational Vehicles** (Consumer Cyclical) -0.2% 1d · -7.4% 1w · vs parent -5.5% · PII, HOG

### Overrides (child 1w residual ≥ 3pp)

| Action | Industry | 1w | Parent 1w | Gap | Captains |
|--------|----------|---:|----------:|----:|----------|
| OVERRIDE | Uranium | -9.4% | +1.0% | -10.3% | UEC, UUUU |
| OVERRIDE | Oil & Gas Equipment & Services | -6.8% | +1.0% | -7.7% | SLB, BKR, KGS, WHD |
| SPLIT | Metal Fabrication | -9.4% | -2.7% | -6.7% | CMC, GPGI |
| SPLIT | Semiconductor Equipment & Materials | -8.4% | -2.1% | -6.3% | LRCX, AMAT, ACMR, KLIC |
| SPLIT | Utilities - Independent Power Producers | -9.5% | -3.2% | -6.3% | CEG, VST, HNRG |
| OVERRIDE | Consumer Electronics | +4.0% | -2.1% | +6.0% | AAPL, SONO, GPRO |
| SPLIT | Coking Coal | -10.5% | -4.8% | -5.7% | HCC, AMR |
| OVERRIDE | Medical Care Facilities | +3.1% | -2.5% | +5.6% | HCA, DVA, PACS, LFST |
| SPLIT | Recreational Vehicles | -7.4% | -2.0% | -5.5% | PII, HOG |
| OVERRIDE | Oil & Gas Drilling | -4.4% | +1.0% | -5.4% | NE, RIG |
| SPLIT | Electrical Equipment & Parts | -7.4% | -2.7% | -4.7% | VRT, HUBB, ENS, ATKR |
| SPLIT | Silver | -9.3% | -4.8% | -4.5% | — |
| SPLIT | Tobacco | +5.0% | +0.5% | +4.5% | PM, MO, TPB, UVV |
| OVERRIDE | Thermal Coal | -3.4% | +1.0% | -4.4% | CNR, BTU |
| SPLIT | Utilities - Renewable | -7.3% | -3.2% | -4.1% | ORA, FLNC |

### Theme join (sub-sector vs GICS parent)

- **Energy Traditional** — Oil / Majors: +0.9% 1w vs parent +1.0% → AGREE; Oil E&P: +1.5% 1w vs parent +1.0% → AGREE; Oil Services: -6.8% 1w vs parent +1.0% → **DIVERGE**; Nuclear: -9.4% 1w vs parent +1.0% → **DIVERGE**
- **Commodities Energy** — Uranium: -9.4% 1w vs parent -1.9% → AGREE; Oil (commodity): +2.4% 1w vs parent -1.9% → **DIVERGE**
- **Energy Renewable** — Solar: -0.4% 1w vs parent -1.4% → AGREE; Renewable utilities: -7.3% 1w vs parent -1.4% → AGREE
- **Commodities Metals** — Gold: -5.0% 1w vs parent -4.8% → AGREE; Silver: -9.3% 1w vs parent -4.8% → AGREE; Copper: -5.0% 1w vs parent -4.8% → AGREE; Other precious: -6.6% 1w vs parent -4.8% → AGREE
- **Semiconductors** — Semis: -4.8% 1w vs parent -2.1% → AGREE; Semi equipment: -8.4% 1w vs parent -2.1% → AGREE
- **Artificial Intelligence** — AI compute / semis: -4.8% 1w vs parent -2.1% → AGREE; Software infra: +1.4% 1w vs parent -2.1% → **DIVERGE**
- **Defense & Aerospace** — Aero / defense: -2.0% 1w vs parent -2.7% → AGREE

### Theme ETF tape (biggest |1w| moves)

| Theme | 1d | 1w | Leaders |
|-------|---:|---:|---------|
| Materials | -2.9% | -6.5% | GDX, GDXJ, XLB |
| Battery and Energy Storage | -2.4% | -4.4% | LIT, BATT, IBAT |
| Future Mobility Production & Tech | -1.6% | -3.5% | DRIV, ROKT, IDRV |
| Technology | -2.9% | -3.4% | VGT, XLK, SMH |
| Robotics & Automation | -2.1% | -3.3% | BAI, AIQ, QTUM |
| Industrials | -1.5% | -3.0% | XLI, ITA, AIRR |
| Communication Services | +2.4% | +3.0% | XLC, VOX, FCOM |
| Utilities | -1.4% | -3.0% | XLU, VPU, FUTY |
| Fintech | +0.2% | -2.8% | BLOK, ARKF, BITQ |
| Healthcare | +1.4% | -2.5% | XLV, VHT, XBI |
| Agri-business | -0.1% | -2.4% | MOO, VEGI, KROP |
| Green Investing | -1.5% | -2.3% | NLR, USCL, SPYX |

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
| Sector LLM essays | **missing / not in ranker** | s_sector (0 if essays missing) |
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
| — | none today |

### How much each predictor is trusted (graded hit rate)

| Topic | hit rate | n | weight |
|-------|----------|---|--------|
| general | 55% | 64 | ×0.85 |
| sector:Basic Materials | 41% | 34 | ×0.50 |
| sector:Communication Services | 21% | 33 | ×0.50 |
| sector:Consumer Cyclical | 44% | 34 | ×0.50 |
| sector:Consumer Defensive | 52% | 31 | ×0.85 |
| sector:Energy | 53% | 32 | ×0.85 |
| sector:Financial | 48% | 31 | ×0.85 |
| sector:Healthcare | 46% | 28 | ×0.85 |
| sector:Industrials | 36% | 31 | ×0.50 |
| sector:Real Estate | 52% | 31 | ×0.85 |
| sector:Technology | 36% | 31 | ×0.50 |
| sector:Utilities | 40% | 30 | ×0.50 |

## Horizon weights — book_policy.json v20 · renormalized (absent: sector)

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.13 | 0.00 | 0.09 | 0.28 | 0.28 | 0.22 | additive |
| 3d | 0.19 | 0.00 | 0.09 | 0.19 | 0.30 | 0.23 | additive |
| 1w | 0.21 | 0.00 | 0.10 | 0.12 | 0.33 | 0.24 | additive |
| 2w | 0.24 | 0.00 | 0.10 | 0.07 | 0.34 | 0.24 | additive |
| 1m | 0.28 | 0.00 | 0.10 | 0.00 | 0.38 | 0.25 | additive |

## 1d BUY — why these names

### 1. NBIS · $42.1B large · Technology

**1d score +0.357**

**NBIS** is a liquid **large-cap** Technology name (Software - Infrastructure) at $42.1B, ADV ~20764k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **washed**. Last earnings were a **big beat**. AB/peer context: the Finviz industry was **down**. Today's **news/judge** is a tailwind for this ticker.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | -0.01 | -0.001 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | -0.67 | -0.059 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.31 | +0.086 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.76 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.07 | +0.070 | liquid small/mid, room to run |
| **1d total** | | | **+0.357** | |

### 2. BB · $5.5B mid · Technology

**1d score +0.695**

**BB** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $5.5B, ADV ~13793k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.90 | +0.120 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | -0.67 | -0.059 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.85 | +0.236 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.89 | +0.199 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.695** | |

### 3. SONO · $2.3B mid · Technology

**1d score +0.745**

**SONO** is a liquid **mid-cap** Technology name (Consumer Electronics) at $2.3B, ADV ~1682k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.67 | +0.090 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | -0.67 | -0.059 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.94 | +0.262 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.91 | +0.203 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.05 | +0.050 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.745** | |

### 4. GMRS · $635M small · Healthcare

**1d score +0.308**

**GMRS** is a liquid **small-cap** Healthcare name (Medical Care Facilities) at $635M, ADV ~1009k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.10 | +0.014 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | -0.27 | -0.024 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.36 | +0.100 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.019 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.308** | |


## 1d AVOID — bottom of the same rank

- **DDD** (small, Technology, $301M) score -0.392. SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -3.1%
- **QUBT** (small, Technology, $1.4B) score -0.294. SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -3.1%
- **LTBR** (micro, Industrials, $174M) score -0.340. SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -4.7%
- **SOC** (micro, Energy, $155M) score -0.397. SELL/AVOID — market=RED; red domains=child,setup,flow; child lags parent -5.4%
- **UNIT** (small, Real Estate, $988M) score -0.456. SELL/AVOID — market=RED; red domains=parent,child,setup,flow
- **LWLG** (small, Basic Materials, $547M) score -0.363. SELL/AVOID — market=RED; red domains=parent,child,setup,flow
- **HYLN** (small, Industrials, $496M) score -0.172. SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -4.7%
- **FLNC** (small, Industrials, $938M) score -0.510. SELL/AVOID — market=RED; red domains=parent,child,setup; child lags parent -4.7%

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | SONO | +0.829 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 2 | BB | +0.789 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | DVN | +0.693 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | WEX | +0.639 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | SLDB | +0.580 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 6 | STVN | +0.531 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 7 | IVA | +0.472 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 8 | PENG | +0.397 | mid | Technology | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 9 | GMRS | +0.329 | small | Healthcare | the Finviz industry was **down** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | SONO | +0.885 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 2 | BB | +0.848 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | WEX | +0.689 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | DVN | +0.684 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | SLDB | +0.607 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 6 | STVN | +0.558 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 7 | IVA | +0.488 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 8 | DBX | +0.431 | mid | Technology | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 9 | GMRS | +0.344 | small | Healthcare | the Finviz industry was **down** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | SONO | +0.959 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 2 | BB | +0.927 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | WEX | +0.740 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | DVN | +0.680 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | SLDB | +0.666 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 6 | STVN | +0.596 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 7 | IVA | +0.510 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 8 | PENG | +0.478 | mid | Technology | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 9 | GMRS | +0.366 | small | Healthcare | the Finviz industry was **down** |

## 1m BUY — why these names

### 1. SONO · $2.3B mid · Technology

**1m score +1.055**

**SONO** is a liquid **mid-cap** Technology name (Consumer Electronics) at $2.3B, ADV ~1682k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.67 | +0.185 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.38 | +0.038 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.94 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.91 | +0.229 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.05 | +0.050 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.055** | |

### 2. BB · $5.5B mid · Technology

**1m score +1.028**

**BB** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $5.5B, ADV ~13793k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.90 | +0.248 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.38 | +0.038 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.85 | +0.318 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.89 | +0.224 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.028** | |

### 3. WEX · $6.0B mid · Technology

**1m score +0.813**

**WEX** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $6.0B, ADV ~576k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.74 | +0.205 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.19 | +0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.85 | +0.318 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.28 | +0.071 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.813** | |

### 4. SLDB · $848M small · Healthcare

**1m score +0.730**

**SLDB** is a liquid **small-cap** Healthcare name (Biotechnology) at $848M, ADV ~1227k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.37 | +0.102 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.38 | +0.038 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.24 | +0.092 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.99 | +0.248 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.730** | |

### 5. DVN · $51.2B large · Energy

**1m score +0.675**

**DVN** is a liquid **large-cap** Energy name (Oil & Gas E&P) at $51.2B, ADV ~10955k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.70 | +0.192 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.06 | +0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.88 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.93 | +0.347 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.20 | +0.050 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1m total** | | | **+0.675** | |

### 6. STVN · $3.8B mid · Healthcare

**1m score +0.643**

**STVN** is a liquid **mid-cap** Healthcare name (Medical Instruments & Supplies) at $3.8B, ADV ~602k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **washed**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.41 | +0.112 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.19 | +0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.36 | +0.134 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.51 | +0.127 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.643** | |

### 7. PENG · $3.0B mid · Technology

**1m score +0.545**

**PENG** is a liquid **mid-cap** Technology name (Information Technology Services) at $3.0B, ADV ~2899k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **washed**. Last earnings were a **big beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.17 | +0.047 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.38 | +0.038 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.70 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | -0.02 | -0.005 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.545** | |

### 8. IVA · $959M small · Healthcare

**1m score +0.532**

**IVA** is a liquid **small-cap** Healthcare name (Biotechnology) at $959M, ADV ~2096k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.41 | +0.112 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.06 | +0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.00 | +0.000 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.66 | +0.165 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.532** | |

### 9. GMRS · $635M small · Healthcare

**1m score +0.397**

**GMRS** is a liquid **small-cap** Healthcare name (Medical Care Facilities) at $635M, ADV ~1009k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.10 | +0.028 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.15 | +0.015 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.36 | +0.134 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.019 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.397** | |


## 1m AVOID — bottom of the same rank

- **URG** (micro, Energy, $260M) score -0.675. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **ABR** (micro, Real Estate, $256M) score -0.670. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FLNC** (small, Industrials, $938M) score -0.669. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **DKNG** (large, Consumer Cyclical, $17.5B) score -0.664. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**
- **NUS** (micro, Consumer Defensive, $189M) score -0.661. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **STLA** (large, Consumer Cyclical, $11.9B) score -0.654. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BETR** (micro, Financial, $292M) score -0.641. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TRIP** (small, Consumer Cyclical, $880M) score -0.632. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **FMC** (small, Basic Materials, $879M) score -0.627. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **HIMS** (mid, Healthcare, $4.9B) score -0.625. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OPEN** (small, Real Estate, $1.3B) score -0.622. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LDI** (micro, Financial, $160M) score -0.621. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SOC** (micro, Energy, $155M) score -0.612. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **UNIT** (small, Real Estate, $988M) score -0.611. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GT** (small, Consumer Cyclical, $1.2B) score -0.607. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BKD** (mid, Healthcare, $2.2B) score -0.604. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GRPN** (small, Communication Services, $513M) score -0.604. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **HPP** (small, Real Estate, $479M) score -0.600. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CWH** (small, Consumer Cyclical, $461M) score -0.600. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **WHR** (small, Consumer Cyclical, $2.0B) score -0.596. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CHRS** (micro, Healthcare, $90M) score -0.592. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RC** (micro, Real Estate, $149M) score -0.592. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **VITL** (small, Consumer Defensive, $407M) score -0.591. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LINE** (mid, Real Estate, $8.1B) score -0.588. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **IRTC** (mid, Healthcare, $3.0B) score -0.587. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-11-12_stock_book.md`
- Machine table: `data/stock_book/2026-11-12_stock_book.csv`
- Machine book: `data/stock_book/2026-11-12_stock_book.json`
- Join rank: `data/join/2026-11-12_ranked.csv`
- Weather: `01_daily/weather/2026-11-12_weather.md`
- AB enrich: `data/ab_checklist/2026-11-12_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-11-12_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-11-12_map_heat.md`
