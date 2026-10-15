# Stock book — 2026-10-15

_Generated 2026-10-15T08:02:43.704458-04:00_

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
- Stand-down: **no** — 384 names qualified through standard,group_leader,catalyst (72 probable)
- Sector predicts this date: 0/11 (missing → sector layer is 0; Finviz week tape still sits in join)
- News tickers in play: 106
- AB coverage: 1955 names · peer RS: 1804
- Universe after liquidity: 2035
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 46

## All-green BUY / SELL

- Mode: **weighted_fallback** · SELL **core_weights**
- Pile: **0** liquid all-green names (need ≥ 8) of 2035
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
| 1 | **HITI** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Pharmaceutical Retailers +3.6% d1 / +1.0% 1w / +3.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 2 | **CON** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 3 | **SGRY** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 4 | **AMN** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 5 | **ASTH** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 6 | **MD** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 7 | **PACS** | 🟡🟡🟢🟡🟢🟡 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,⚪,Cond green |
| 8 | **NTAP** | 🟡🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 9 | **MQ** | 🟡🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 10 | **IT** | 🟡🔴🟢🟡🟢🟢 | group_leader | no direct company event; Information Technology Services +2.9% d1 / +1.9% 1w / +3.9% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 11 | **BAND** | 🟡🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 12 | **AVPT** | 🟡🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 13 | **BB** | 🟡🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 14 | **PANW** | 🟡🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 15 | **TWLO** | 🟡🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **NEOV** | 🟡🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 2 | **OKLO** | 🟡🔴🔴🟡🔴🔴 | Utilities - Independent Power Producers | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -6.3% |
| 3 | **FCEL** | 🟡🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 4 | **NNDM** | 🟡🔴🔴🟡🔴🔴 | Computer Hardware | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.1% |
| 5 | **POWL** | 🟡🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 6 | **EOSE** | 🟡🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -4.7% |
| 7 | **TE** | 🟡🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -4.7% |
| 8 | **NRGV** | 🟡🔴🔴🟡🔴🟡 | Utilities - Renewable | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -4.1% |
| 9 | **INDI** | 🟡🔴🔴🟡🔴🔴 | Semiconductors | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 10 | **HNRG** | 🟡🔴🔴🟡🔴🟡 | Utilities - Independent Power Producers | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -6.3% |
| 11 | **SKYX** | 🟡🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -4.7% |
| 12 | **AMSC** | 🟡🔴🔴🟡🔴🔴 | Specialty Industrial Machinery | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 13 | **PLUG** | 🟡🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -4.7% |
| 14 | **CRML** | 🟡🔴🔴🟡🔴🔴 | Other Industrial Metals & Mining | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 15 | **LUNR** | 🟡🔴🔴🟡🔴🔴 | Aerospace & Defense | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **captain_research** (178 captains, 5 industries → s_heat).
- Board file: `01_daily/map_heat/2026-10-15_map_heat.json` · generated 2026-10-15T06:37:37.848810-04:00

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
| general | 51% | 47 | ×0.85 |
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

## Horizon weights — book_policy.json v15 · renormalized (absent: sector, general)

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.15 | 0.00 | 0.00 | 0.30 | 0.30 | 0.24 | additive |
| 3d | 0.21 | 0.00 | 0.00 | 0.21 | 0.33 | 0.26 | additive |
| 1w | 0.24 | 0.00 | 0.00 | 0.13 | 0.37 | 0.26 | additive |
| 2w | 0.27 | 0.00 | 0.00 | 0.08 | 0.38 | 0.27 | additive |
| 1m | 0.31 | 0.00 | 0.00 | 0.00 | 0.42 | 0.28 | additive |

## 1d BUY — why these names

### 1. HITI · $250M micro · Healthcare

**1d score +0.557**

**HITI** is a liquid **micro-cap** Healthcare name (Pharmaceutical Retailers) at $250M, ADV ~541k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.91 | +0.134 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.36 | +0.109 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.76 | +0.185 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.08 | +0.080 | liquid small/mid, room to run |
| **1d total** | | | **+0.557** | |

### 2. CON · $4.5B mid · Healthcare

**1d score +0.788**

**CON** is a liquid **mid-cap** Healthcare name (Medical Care Facilities) at $4.5B, ADV ~942k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.99 | +0.144 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.247 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.52 | +0.126 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.021 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.788** | |

### 3. SGRY · $1.9B small · Healthcare

**1d score +0.731**

**SGRY** is a liquid **small-cap** Healthcare name (Medical Care Facilities) at $1.9B, ADV ~2126k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.98 | +0.144 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.46 | +0.141 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.72 | +0.175 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.021 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.731** | |

### 4. AMN · $1.3B small · Healthcare

**1d score +0.762**

**AMN** is a liquid **small-cap** Healthcare name (Medical Care Facilities) at $1.3B, ADV ~702k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.99 | +0.144 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.276 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.29 | +0.071 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.021 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.762** | |

### 5. ASTH · $1.7B small · Healthcare

**1d score +0.704**

**ASTH** is a liquid **small-cap** Healthcare name (Medical Care Facilities) at $1.7B, ADV ~523k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.99 | +0.144 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.46 | +0.141 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.61 | +0.148 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.021 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.704** | |

### 6. MD · $2.1B mid · Healthcare

**1d score +0.699**

**MD** is a liquid **mid-cap** Healthcare name (Medical Care Facilities) at $2.1B, ADV ~774k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.97 | +0.141 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.268 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.08 | +0.019 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.021 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.699** | |

### 7. PACS · $6.8B mid · Healthcare

**1d score +0.892**

**PACS** is a liquid **mid-cap** Healthcare name (Medical Care Facilities) at $6.8B, ADV ~830k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.99 | +0.144 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.268 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.74 | +0.181 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.05 | +0.049 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.892** | |

### 8. NTAP · $40.2B large · Technology

**1d score +0.096**

**NTAP** is a liquid **large-cap** Technology name (Software - Infrastructure) at $40.2B, ADV ~2546k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | -0.01 | -0.002 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.36 | +0.109 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.65 | +0.159 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.096** | |


## 1d AVOID — bottom of the same rank

- **NEOV** (micro, Industrials, $133M) score -0.385. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.7%
- **OKLO** (mid, Utilities, $6.9B) score -0.267. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -6.3%
- **FCEL** (small, Industrials, $1.3B) score -0.216. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.7%
- **NNDM** (small, Technology, $324M) score -0.133. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.1%
- **POWL** (mid, Industrials, $7.1B) score -0.079. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.7%
- **EOSE** (small, Industrials, $1.1B) score -0.340. SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -4.7%
- **TE** (small, Industrials, $1.1B) score -0.296. SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -4.7%
- **NRGV** (small, Utilities, $736M) score -0.247. SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -4.1%

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | CDNA | +1.000 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | PACS | +0.984 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | BLFS | +0.935 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 4 | CBRL | +0.897 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | CON | +0.875 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 6 | SHOO | +0.856 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | CAKE | +0.855 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 8 | SIG | +0.850 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 9 | AVPT | +0.744 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | SLDE | +0.730 | mid | Financial | the Finviz industry was **down** |
| 11 | OLLI | +0.699 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | PDFS | +0.698 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 13 | RRC | +0.694 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | EZPW | +0.680 | small | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | CVLT | +0.672 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 16 | LAUR | +0.671 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 17 | YOU | +0.666 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 18 | SPNT | +0.658 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 19 | AVO | +0.649 | small | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 20 | TAL | +0.637 | mid | Consumer Defensive | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 21 | CNK | +0.612 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 22 | SM | +0.599 | mid | Energy | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 23 | LNC | +0.592 | mid | Financial | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 24 | CTOS | +0.579 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 25 | WLY | +0.565 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | CDNA | +1.072 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | PACS | +1.052 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | BLFS | +0.999 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 4 | CBRL | +0.950 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | CON | +0.939 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 6 | SHOO | +0.923 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | CAKE | +0.911 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 8 | SIG | +0.908 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 9 | SLDE | +0.790 | mid | Financial | the Finviz industry was **down** |
| 10 | AVPT | +0.789 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 11 | OLLI | +0.731 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | EZPW | +0.727 | small | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 13 | PDFS | +0.725 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 14 | CVLT | +0.720 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 15 | SPNT | +0.710 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 16 | LAUR | +0.703 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 17 | PAYS | +0.695 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 18 | TAL | +0.684 | mid | Consumer Defensive | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 19 | AVO | +0.678 | small | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 20 | RRC | +0.653 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 21 | CNK | +0.644 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 22 | LNC | +0.636 | mid | Financial | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 23 | CTOS | +0.607 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 24 | WLY | +0.593 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 25 | SM | +0.549 | mid | Energy | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | CDNA | +1.121 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | PACS | +1.099 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | BLFS | +1.046 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 4 | CON | +0.983 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | CBRL | +0.982 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 6 | SHOO | +0.969 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | FIGS | +0.950 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | CAKE | +0.950 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 9 | SLDE | +0.831 | mid | Financial | the Finviz industry was **down** |
| 10 | AVPT | +0.820 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 11 | EZPW | +0.763 | small | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 12 | SPNT | +0.747 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 13 | CVLT | +0.744 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 14 | OLLI | +0.740 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 15 | PDFS | +0.733 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 16 | LAUR | +0.723 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 17 | PAYS | +0.714 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 18 | TAL | +0.711 | mid | Consumer Defensive | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 19 | AVO | +0.685 | small | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 20 | LNC | +0.667 | mid | Financial | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 21 | CNK | +0.652 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 22 | CTOS | +0.612 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 23 | RRC | +0.611 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 24 | WLY | +0.599 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 25 | PEB | +0.534 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 1m BUY — why these names

### 1. CDNA · $3.4B mid · Healthcare

**1m score +1.199**

**CDNA** is a liquid **mid-cap** Healthcare name (Diagnostics & Research) at $3.4B, ADV ~1185k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.96 | +0.402 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.88 | +0.246 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.199** | |

### 2. PACS · $6.8B mid · Healthcare

**1m score +1.173**

**PACS** is a liquid **mid-cap** Healthcare name (Medical Care Facilities) at $6.8B, ADV ~830k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.74 | +0.206 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.05 | +0.049 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.173** | |

### 3. BLFS · $1.9B small · Healthcare

**1m score +1.117**

**BLFS** is a liquid **small-cap** Healthcare name (Medical Instruments & Supplies) at $1.9B, ADV ~1082k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.76 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.89 | +0.248 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.117** | |

### 4. WAY · $4.9B mid · Healthcare

**1m score +1.055**

**WAY** is a liquid **mid-cap** Healthcare name (Health Information Services) at $4.9B, ADV ~2551k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.96 | +0.402 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.37 | +0.102 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.055** | |

### 5. SHOO · $3.3B mid · Consumer Cyclical

**1m score +1.043**

**SHOO** is a liquid **mid-cap** Consumer Cyclical name (Footwear & Accessories) at $3.3B, ADV ~1034k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.98 | +0.300 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.94 | +0.392 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.47 | +0.129 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.03 | -0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.043** | |

### 6. CBRL · $1.2B small · Consumer Cyclical

**1m score +1.040**

**CBRL** is a liquid **small-cap** Consumer Cyclical name (Restaurants) at $1.2B, ADV ~961k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.48 | +0.148 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.99 | +0.275 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.040** | |

### 7. FIGS · $2.3B mid · Consumer Cyclical

**1m score +1.022**

**FIGS** is a liquid **mid-cap** Consumer Cyclical name (Apparel Manufacturing) at $2.3B, ADV ~2935k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.96 | +0.293 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.91 | +0.377 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.37 | +0.102 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.022** | |

### 8. SIG · $3.9B mid · Consumer Cyclical

**1m score +1.013**

**SIG** is a liquid **mid-cap** Consumer Cyclical name (Luxury Goods) at $3.9B, ADV ~831k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.91 | +0.277 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.76 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.43 | +0.120 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.05 | +0.049 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.013** | |

### 9. SLDE · $2.7B mid · Financial

**1m score +0.898**

**SLDE** is a liquid **mid-cap** Financial name (Insurance - Property & Casualty) at $2.7B, ADV ~1233k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.96 | +0.294 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.85 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.898** | |

### 10. AVPT · $2.9B mid · Technology

**1m score +0.871**

**AVPT** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $2.9B, ADV ~2025k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.61 | +0.185 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.64 | +0.265 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.61 | +0.171 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.871** | |

### 11. EZPW · $1.9B small · Financial

**1m score +0.815**

**EZPW** is a liquid **small-cap** Financial name (Credit Services) at $1.9B, ADV ~974k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.88 | +0.267 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.46 | +0.193 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.38 | +0.105 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.815** | |

### 12. SPNT · $2.9B mid · Financial

**1m score +0.804**

**SPNT** is a liquid **mid-cap** Financial name (Insurance - Reinsurance) at $2.9B, ADV ~801k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.93 | +0.283 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.64 | +0.265 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.02 | +0.006 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.804** | |

### 13. CVLT · $6.1B mid · Technology

**1m score +0.797**

**CVLT** is a liquid **mid-cap** Technology name (Software - Application) at $6.1B, ADV ~618k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.43 | +0.133 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.96 | +0.402 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.04 | +0.012 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.797** | |

### 14. OLLI · $5.1B mid · Consumer Defensive

**1m score +0.775**

**OLLI** is a liquid **mid-cap** Consumer Defensive name (Discount Stores) at $5.1B, ADV ~1889k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | -0.15 | -0.046 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.91 | +0.377 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.70 | +0.193 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.775** | |

### 15. TAL · $4.0B mid · Consumer Defensive

**1m score +0.762**

**TAL** is a liquid **mid-cap** Consumer Defensive name (Education & Training Services) at $4.0B, ADV ~4615k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.58 | +0.176 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.81 | +0.337 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | -0.00 | -0.001 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.762** | |

### 16. PDFS · $2.4B mid · Technology

**1m score +0.762**

**PDFS** is a liquid **mid-cap** Technology name (Software - Application) at $2.4B, ADV ~603k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | -0.15 | -0.046 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.70 | +0.293 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.95 | +0.265 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.762** | |

### 17. LAUR · $5.2B mid · Consumer Defensive

**1m score +0.759**

**LAUR** is a liquid **mid-cap** Consumer Defensive name (Education & Training Services) at $5.2B, ADV ~1003k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.27 | +0.082 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.55 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.71 | +0.196 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.759** | |

### 18. PAYS · $718M small · Technology

**1m score +0.758**

**PAYS** is a liquid **small-cap** Technology name (Software - Infrastructure) at $718M, ADV ~691k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.25 | +0.077 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.23 | +0.065 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.758** | |

### 19. CHEF · $4.6B mid · Consumer Defensive

**1m score +0.719**

**CHEF** is a liquid **mid-cap** Consumer Defensive name (Food Distribution) at $4.6B, ADV ~639k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.18 | +0.054 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.91 | +0.377 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.19 | +0.053 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.02 | -0.016 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.719** | |

### 20. LNC · $7.8B mid · Financial

**1m score +0.716**

**LNC** is a liquid **mid-cap** Financial name (Insurance - Life) at $7.8B, ADV ~1970k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.78 | +0.237 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.55 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | -0.01 | -0.003 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.716** | |

### 21. CNK · $4.4B mid · Communication Services

**1m score +0.686**

**CNK** is a liquid **mid-cap** Communication Services name (Entertainment) at $4.4B, ADV ~2438k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | -0.15 | -0.046 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.93 | +0.386 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.53 | +0.146 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.686** | |

### 22. CTOS · $2.1B mid · Industrials

**1m score +0.643**

**CTOS** is a liquid **mid-cap** Industrials name (Rental & Leasing Services) at $2.1B, ADV ~1010k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | -0.15 | -0.046 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.26 | +0.072 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.643** | |

### 23. WLY · $2.4B mid · Communication Services

**1m score +0.629**

**WLY** is a liquid **mid-cap** Communication Services name (Publishing) at $2.4B, ADV ~509k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | -0.15 | -0.046 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.85 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.38 | +0.105 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.016 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.629** | |

### 24. RRC · $8.8B mid · Energy

**1m score +0.566**

**RRC** is a liquid **mid-cap** Energy name (Oil & Gas E&P) at $8.8B, ADV ~2768k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Today's **news/judge** is a tailwind for this ticker.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | -0.15 | -0.046 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.90 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.85 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.03 | +0.008 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.566** | |

### 25. PEB · $2.1B mid · Real Estate

**1m score +0.559**

**PEB** is a liquid **mid-cap** Real Estate name (REIT - Hotel & Motel) at $2.1B, ADV ~2157k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | -0.15 | -0.046 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.76 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.13 | +0.037 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.559** | |


## 1m AVOID — bottom of the same rank

- **IBM** (mega, Technology, $208.7B) score -0.717. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SOC** (small, Energy, $702M) score -0.664. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **BIDU** (large, Communication Services, $23.9B) score -0.659. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RBLX** (large, Communication Services, $30.1B) score -0.652. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TMC** (small, Basic Materials, $1.7B) score -0.650. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ASPI** (small, Basic Materials, $402M) score -0.647. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PRM** (mid, Basic Materials, $4.6B) score -0.643. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **VSAT** (large, Technology, $10.0B) score -0.641. this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PCT** (small, Industrials, $936M) score -0.636. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RC** (micro, Real Estate, $218M) score -0.622. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ACDC** (small, Energy, $818M) score -0.621. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **USAS** (small, Basic Materials, $1.5B) score -0.609. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NEOV** (micro, Industrials, $133M) score -0.605. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NN** (mid, Technology, $2.4B) score -0.605. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **EOSE** (small, Industrials, $1.1B) score -0.604. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CMTG** (micro, Real Estate, $179M) score -0.602. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SMR** (mid, Industrials, $3.4B) score -0.600. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GOGO** (small, Communication Services, $301M) score -0.599. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **UAMY** (small, Basic Materials, $648M) score -0.598. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BAK** (micro, Basic Materials, $257M) score -0.596. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **VNET** (small, Technology, $1.7B) score -0.593. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LBTYK** (mid, Communication Services, $3.2B) score -0.590. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TDTH** (small, Technology, $1.3B) score -0.590. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BORR** (small, Energy, $1.2B) score -0.586. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SSTK** (micro, Communication Services, $150M) score -0.580. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-10-15_stock_book.md`
- Machine table: `data/stock_book/2026-10-15_stock_book.csv`
- Machine book: `data/stock_book/2026-10-15_stock_book.json`
- Join rank: `data/join/2026-10-15_ranked.csv`
- Weather: `01_daily/weather/2026-10-15_weather.md`
- AB enrich: `data/ab_checklist/2026-10-15_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-10-15_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-10-15_map_heat.md`
