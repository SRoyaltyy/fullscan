# Stock book — 2026-09-21

_Generated 2026-09-21T06:53:58.226140-04:00_

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
- General predict (same-day): +0.72 up (present)
- Stand-down: **no** — 364 names qualified through standard,group_leader,catalyst (9 probable)
- Sector predicts this date: 11/11 (ok)
- News tickers in play: 85
- AB coverage: 1957 names · peer RS: 1834
- Universe after liquidity: 2069
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 47

## All-green BUY / SELL

- Mode: **green_pile** · SELL **core_weights_ex_green**
- Pile: **78** liquid all-green names (need ≥ 8) of 2069
- Core fired: join=yes, AB=yes, peer=yes
- pile 78 ≥ 8 liquid all-green names — BUY 15 from the pile by green_rank (no opp); SELL is core weights on the non-green remainder

## Decision lattice — gate → route → rank

The weighted score is now a tie-breaker inside an eligible lane. It cannot average away a market, group, company, or setup veto.

### MARKET: 🟢 GREEN

- GREEN: general up score=+12.87; good=+7.0 vs bad=-1.0; risk=on; red pillars=1
- Allowed long lanes: **standard, group_leader, catalyst** · max slots 15 · size ×1.00
- Bull evidence: overnight catalysts +3.00 points; global sessions +2.00 points; oil / dollar +1.00 points; volatility +0.75 points; futures +0.25 points
- Bear evidence: rates / Fed -1.00 points

Decision domains: **MKT · parent · child · company · setup · flow**. Measured parent/child tape is kept separate from the LLM essay; direct company events must be price-confirmed on a hard-red day.

### Bull decisions (eligible or closest blocked cases)

| # | Ticker | Domains | Lane | Company / group | Decision |
|---:|--------|---------|------|-----------------|----------|
| 1 | **GEV** | 🟢🔴🔴🟢🟡🟢 | catalyst | direct high digest (same-day): GE Vernova settles over $300 million Vineyard Wind payment dispute, withdraws contract-termination notice and legal claims on Massachusetts offshore project; Specialty Industrial Machinery -4.2% d1 / -4.2% 1w / -1.5% vs parent | BUY CATALYST — market=GREEN; parent=RED; child=RED/rel=YELLOW; company=GREEN(0.72); setup=YELLOW; flow=GREEN; lookback=Cond green |
| 2 | **CON** | 🟢🟢🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 3 | **OPCH** | 🟢🟢🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 4 | **HITI** | 🟢🟢🟢🟡🟢🟢 | group_leader | no direct company event; Pharmaceutical Retailers +3.6% d1 / +1.0% 1w / +3.5% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 5 | **AMN** | 🟢🟢🟢🟡🟢🟡 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,⚪,Cond green |
| 6 | **ORCL** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 7 | **BB** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 8 | **AVAH** | 🟢🟢🟢🟡🟢🟡 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,Cond green |
| 9 | **THC** | 🟢🟢🟢🟡🟢🟡 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,Cond green |
| 10 | **LFST** | 🟢🟢🟢🟡🟢🟡 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,Cond green |
| 11 | **ACHC** | 🟢🟢🟢🟡🟢🟡 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,⚪,Cond green |
| 12 | **MD** | 🟢🟢🟢🟡🟢🟡 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,Cond green |
| 13 | **PACS** | 🟢🟢🟢🟡🟢🟡 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,Cond green |
| 14 | **MGNI** | 🟢🟢🟢🟡🟢🟢 | standard | no direct company event; Advertising Agencies +2.9% d1 / +2.7% 1w / -0.8% vs parent | BUY STANDARD — market=GREEN; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 15 | **HCSG** | 🟢🟢🟢🟡🟢🟡 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **HP** | 🟢🔴🔴🟡🔴🔴 | Oil & Gas Drilling | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 2 | **NE** | 🟢🔴🔴🟡🔴🔴 | Oil & Gas Drilling | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 3 | **SOC** | 🟢🔴🔴🟡🔴🔴 | Oil & Gas Drilling | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 4 | **BAK** | 🟢🔴🔴🟡🔴🔴 | Chemicals | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow |
| 5 | **BTU** | 🟢🔴🔴🟡🔴🟡 | Thermal Coal | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -4.4% |
| 6 | **NNDM** | 🟢🔴🔴🟡🔴🔴 | Computer Hardware | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -3.1% |
| 7 | **URG** | 🟢🔴🔴🟡🔴🟡 | Uranium | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -10.3% |
| 8 | **HAL** | 🟢🔴🔴🟡🔴🟡 | Oil & Gas Equipment & Services | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -7.7% |
| 9 | **GORO** | 🟢🔴🔴🟡🔴🔴 | Gold | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow |
| 10 | **UEC** | 🟢🔴🔴🟡🔴🟡 | Uranium | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -10.3% |
| 11 | **EU** | 🟢🔴🔴🟡🔴🟡 | Uranium | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -10.3% |
| 12 | **UUUU** | 🟢🔴🔴🟡🔴🟡 | Uranium | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -10.3% |
| 13 | **JKS** | 🟢🔴🔴🟡🔴🔴 | Solar | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow |
| 14 | **WFRD** | 🟢🔴🔴🟡🔴🟡 | Oil & Gas Equipment & Services | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -7.7% |
| 15 | **TTI** | 🟢🔴🔴🟡🔴🟡 | Oil & Gas Equipment & Services | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -7.7% |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **captain_research** (274 captains, 6 industries → s_heat).
- Board file: `01_daily/map_heat/2026-09-21_map_heat.json` · generated 2026-09-21T04:18:55.645178-04:00

### Sector RS vs same-day LLM essay

| Sector | Finviz 1d | Finviz 1w | LLM 1d | Tape vs essay |
|--------|----------:|----------:|-------:|---------------|
| Basic Materials | -2.1% | -4.8% | +0.00 | essay flat, tape moving |
| Communication Services | +2.7% | +3.5% | +0.33 |  |
| Consumer Cyclical | -0.4% | -2.0% | +0.00 | essay flat, tape moving |
| Consumer Defensive | +1.4% | +0.5% | +0.00 |  |
| Energy | -0.8% | +1.0% | -0.55 |  |
| Financial | -0.4% | -1.8% | +0.00 | essay flat, tape moving |
| Healthcare | +1.4% | -2.5% | +0.44 | essay UP, tape DOWN |
| Industrials | -1.6% | -2.7% | +0.00 | essay flat, tape moving |
| Real Estate | -0.6% | -2.1% | +0.00 | essay flat, tape moving |
| Technology | -2.0% | -2.1% | +0.00 | essay flat, tape moving |
| Utilities | -1.5% | -3.2% | -0.28 |  |

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
| Energy | -0.55 |
| Healthcare | +0.44 |
| Communication Services | +0.33 |
| Utilities | -0.28 |
| Basic Materials | +0.00 |
| Consumer Cyclical | +0.00 |
| Consumer Defensive | +0.00 |
| Financial | +0.00 |
| Industrials | +0.00 |
| Real Estate | +0.00 |
| Technology | +0.00 |

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

### 1. PGEN · $2.8B mid · Healthcare

**1d score +0.838**

**PGEN** is a liquid **mid-cap** Healthcare name (Biotechnology) at $2.8B, ADV ~5099k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.44 | +0.044 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.36 | +0.029 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.97 | +0.194 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.838** | |

### 2. MGNI · $3.7B mid · Communication Services

**1d score +0.879**

**MGNI** is a liquid **mid-cap** Communication Services name (Advertising Agencies) at $3.7B, ADV ~2662k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.33 | +0.033 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.72 | +0.058 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.90 | +0.180 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.879** | |

### 3. IOVA · $4.7B mid · Healthcare

**1d score +0.825**

**IOVA** is a liquid **mid-cap** Healthcare name (Biotechnology) at $4.7B, ADV ~16698k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extreme**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.95 | +0.114 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.44 | +0.044 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.36 | +0.029 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.93 | +0.186 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.825** | |

### 4. NEOG · $2.8B mid · Healthcare

**1d score +0.857**

**NEOG** is a liquid **mid-cap** Healthcare name (Medical Devices) at $2.8B, ADV ~2173k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.96 | +0.115 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.44 | +0.044 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.72 | +0.058 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.93 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.79 | +0.158 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.857** | |

### 5. HAE · $4.9B mid · Healthcare

**1d score +0.805**

**HAE** is a liquid **mid-cap** Healthcare name (Medical Devices) at $4.9B, ADV ~739k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.95 | +0.114 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.44 | +0.044 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.11 | +0.009 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.91 | +0.226 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.81 | +0.161 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.805** | |

### 6. ZD · $2.0B small · Communication Services

**1d score +0.746**

**ZD** is a liquid **small-cap** Communication Services name (Advertising Agencies) at $2.0B, ADV ~670k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.91 | +0.110 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.33 | +0.033 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.36 | +0.029 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.76 | +0.190 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.67 | +0.134 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.746** | |

### 7. IMAX · $2.9B mid · Communication Services

**1d score +0.728**

**IMAX** is a liquid **mid-cap** Communication Services name (Entertainment) at $2.9B, ADV ~1263k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.95 | +0.114 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.33 | +0.033 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.11 | +0.009 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.93 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.46 | +0.092 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.728** | |

### 8. CNK · $4.1B mid · Communication Services

**1d score +0.755**

**CNK** is a liquid **mid-cap** Communication Services name (Entertainment) at $4.1B, ADV ~2388k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.77 | +0.092 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.33 | +0.033 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.36 | +0.029 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.91 | +0.226 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.49 | +0.098 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.755** | |

### 9. CHEF · $4.5B mid · Consumer Defensive

**1d score +0.654**

**CHEF** is a liquid **mid-cap** Consumer Defensive name (Food Distribution) at $4.5B, ADV ~652k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.91 | +0.109 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.72 | +0.058 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.70 | +0.176 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.30 | +0.061 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.654** | |

### 10. DAR · $10.3B large · Consumer Defensive

**1d score +0.263**

**DAR** is a liquid **large-cap** Consumer Defensive name (Packaged Foods) at $10.3B, ADV ~2466k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.73 | +0.088 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.36 | +0.029 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.76 | +0.190 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.38 | +0.075 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.263** | |

### 11. SJM · $13.8B large · Consumer Defensive

**1d score +0.183**

**SJM** is a liquid **large-cap** Consumer Defensive name (Packaged Foods) at $13.8B, ADV ~1432k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.64 | +0.077 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.11 | +0.009 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.46 | +0.116 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.76 | +0.152 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.183** | |

### 12. TAL · $4.0B mid · Consumer Defensive

**1d score +0.572**

**TAL** is a liquid **mid-cap** Consumer Defensive name (Education & Training Services) at $4.0B, ADV ~4776k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.63 | +0.075 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.11 | +0.009 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.70 | +0.176 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.31 | +0.062 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.572** | |


## 1d AVOID — bottom of the same rank

- **BTU** (mid, Energy, $3.1B) score -0.554. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -4.4%
- **UEC** (mid, Energy, $5.0B) score -0.437. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -10.3%
- **FLNC** (small, Utilities, $1.4B) score -0.430. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -4.1%
- **HP** (mid, Energy, $4.1B) score -0.408. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -5.4%
- **EU** (micro, Energy, $174M) score -0.391. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -10.3%
- **SOC** (small, Energy, $885M) score -0.381. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -5.4%
- **GLND** (micro, Energy, $125M) score -0.365. SELL/AVOID — market=GREEN; red domains=parent,setup
- **CRC** (mid, Energy, $4.8B) score -0.335. SELL/AVOID — market=GREEN; red domains=parent,setup
- **AESI** (small, Energy, $1.7B) score -0.331. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -7.7%
- **AROC** (mid, Energy, $5.5B) score -0.331. SELL/AVOID — market=GREEN; red domains=parent,child; child lags parent -7.7%
- **FRVO** (mid, Utilities, $5.1B) score -0.328. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -4.1%
- **UUUU** (mid, Energy, $3.2B) score -0.314. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -10.3%
- **RES** (small, Energy, $1.3B) score -0.311. SELL/AVOID — market=GREEN; red domains=parent,child; child lags parent -7.7%
- **DVLT** (micro, Technology, $150M) score -0.305. SELL/AVOID — market=GREEN; red domains=parent,setup
- **VTS** (small, Energy, $737M) score -0.298. SELL/AVOID — market=GREEN; red domains=parent,setup
- **BIPC** (mid, Utilities, $5.0B) score -0.295. SELL/AVOID — market=GREEN; red domains=parent,child,setup
- **SAFX** (micro, Utilities, $203M) score -0.293. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -4.1%
- **URG** (small, Energy, $477M) score -0.283. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -10.3%
- **BAK** (micro, Basic Materials, $280M) score -0.282. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow
- **LU** (small, Financial, $1.0B) score -0.265. NO BEAR — market=GREEN; red domains=setup
- **NEXT** (small, Energy, $1.8B) score -0.264. SELL/AVOID — market=GREEN; red domains=parent,child; child lags parent -7.7%
- **BGS** (micro, Consumer Defensive, $237M) score -0.260. NO BEAR — market=GREEN; red domains=setup; child lags parent -3.5%
- **NE** (mid, Energy, $6.9B) score -0.258. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -5.4%
- **TTI** (small, Energy, $909M) score -0.255. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -7.7%
- **PTEN** (mid, Energy, $4.5B) score -0.248. SELL/AVOID — market=GREEN; red domains=parent,child; child lags parent -5.4%

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | PGEN | +0.893 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 2 | MGNI | +0.919 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | IOVA | +0.879 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | NEOG | +0.901 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 5 | HAE | +0.867 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 6 | ZD | +0.793 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 7 | IMAX | +0.785 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 8 | CNK | +0.798 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 9 | CHEF | +0.677 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | DAR | +0.290 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 11 | SJM | +0.210 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 12 | TAL | +0.601 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | PGEN | +0.919 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 2 | MGNI | +0.925 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | IOVA | +0.904 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | NEOG | +0.910 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 5 | HAE | +0.907 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 6 | ZD | +0.814 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 7 | IMAX | +0.824 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 8 | CNK | +0.819 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 9 | CHEF | +0.672 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | DAR | +0.301 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 11 | SJM | +0.227 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 12 | TAL | +0.622 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | PGEN | +0.929 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 2 | MGNI | +0.914 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | IOVA | +0.913 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | NEOG | +0.901 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 5 | HAE | +0.929 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 6 | ZD | +0.820 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 7 | IMAX | +0.844 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 8 | CNK | +0.822 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 9 | CHEF | +0.652 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | DAR | +0.297 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 11 | SJM | +0.234 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 12 | TAL | +0.629 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1m BUY — why these names

### 1. PGEN · $2.8B mid · Healthcare

**1m score +0.972**

**PGEN** is a liquid **mid-cap** Healthcare name (Biotechnology) at $2.8B, ADV ~5099k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.44 | +0.089 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.25 | -0.020 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.97 | +0.194 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.972** | |

### 2. MGNI · $3.7B mid · Communication Services

**1m score +0.955**

**MGNI** is a liquid **mid-cap** Communication Services name (Advertising Agencies) at $3.7B, ADV ~2662k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.33 | +0.065 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.49 | -0.039 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.90 | +0.180 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.955** | |

### 3. IOVA · $4.7B mid · Healthcare

**1m score +0.956**

**IOVA** is a liquid **mid-cap** Healthcare name (Biotechnology) at $4.7B, ADV ~16698k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extreme**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.95 | +0.208 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.44 | +0.089 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.25 | -0.020 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.93 | +0.186 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.956** | |

### 4. NEOG · $2.8B mid · Healthcare

**1m score +0.945**

**NEOG** is a liquid **mid-cap** Healthcare name (Medical Devices) at $2.8B, ADV ~2173k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.96 | +0.210 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.44 | +0.089 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.49 | -0.039 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.93 | +0.278 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.79 | +0.158 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.945** | |

### 5. HAE · $4.9B mid · Healthcare

**1m score +0.975**

**HAE** is a liquid **mid-cap** Healthcare name (Medical Devices) at $4.9B, ADV ~739k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.95 | +0.210 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.44 | +0.089 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.81 | +0.161 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.975** | |

### 6. ZD · $2.0B small · Communication Services

**1m score +0.859**

**ZD** is a liquid **small-cap** Communication Services name (Advertising Agencies) at $2.0B, ADV ~670k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.91 | +0.201 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.33 | +0.065 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.25 | -0.020 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.67 | +0.134 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.859** | |

### 7. IMAX · $2.9B mid · Communication Services

**1m score +0.887**

**IMAX** is a liquid **mid-cap** Communication Services name (Entertainment) at $2.9B, ADV ~1263k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.95 | +0.209 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.33 | +0.065 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.93 | +0.278 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.46 | +0.092 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.887** | |

### 8. CNK · $4.1B mid · Communication Services

**1m score +0.861**

**CNK** is a liquid **mid-cap** Communication Services name (Entertainment) at $4.1B, ADV ~2388k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.77 | +0.169 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.33 | +0.065 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.25 | -0.020 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.49 | +0.098 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.861** | |

### 9. CHEF · $4.5B mid · Consumer Defensive

**1m score +0.683**

**CHEF** is a liquid **mid-cap** Consumer Defensive name (Food Distribution) at $4.5B, ADV ~652k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.91 | +0.200 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.49 | -0.039 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.211 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.30 | +0.061 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.683** | |

### 10. DAR · $10.3B large · Consumer Defensive

**1m score +0.326**

**DAR** is a liquid **large-cap** Consumer Defensive name (Packaged Foods) at $10.3B, ADV ~2466k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.73 | +0.162 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.25 | -0.020 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.38 | +0.075 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1m total** | | | **+0.326** | |

### 11. SJM · $13.8B large · Consumer Defensive

**1m score +0.256**

**SJM** is a liquid **large-cap** Consumer Defensive name (Packaged Foods) at $13.8B, ADV ~1432k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.64 | +0.141 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.46 | +0.139 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.76 | +0.152 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1m total** | | | **+0.256** | |

### 12. TAL · $4.0B mid · Consumer Defensive

**1m score +0.655**

**TAL** is a liquid **mid-cap** Consumer Defensive name (Education & Training Services) at $4.0B, ADV ~4776k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.63 | +0.138 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.211 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.31 | +0.062 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.655** | |


## 1m AVOID — bottom of the same rank

- **BTU** (mid, Energy, $3.1B) score -0.769. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **UEC** (mid, Energy, $5.0B) score -0.728. this name **lagged its own correlated peers** this week; the peer basket itself was **up**
- **EU** (micro, Energy, $174M) score -0.679. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide)
- **HP** (mid, Energy, $4.1B) score -0.604. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FLNC** (small, Utilities, $1.4B) score -0.593. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SOC** (small, Energy, $885M) score -0.582. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **UUUU** (mid, Energy, $3.2B) score -0.579. this name **lagged its own correlated peers** this week; the peer basket itself was **up**
- **GLND** (micro, Energy, $125M) score -0.572. the Finviz industry was **down**
- **CRC** (mid, Energy, $4.8B) score -0.569. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AESI** (small, Energy, $1.7B) score -0.562. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BIPC** (mid, Utilities, $5.0B) score -0.551. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AROC** (mid, Energy, $5.5B) score -0.550. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FRVO** (mid, Utilities, $5.1B) score -0.525. the Finviz industry was **down**
- **BAK** (micro, Basic Materials, $280M) score -0.525. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **URG** (small, Energy, $477M) score -0.488. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide)
- **VTS** (small, Energy, $737M) score -0.485. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NE** (mid, Energy, $6.9B) score -0.473. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TTI** (small, Energy, $909M) score -0.467. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NEXT** (small, Energy, $1.8B) score -0.450. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RES** (small, Energy, $1.3B) score -0.448. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SAFX** (micro, Utilities, $203M) score -0.436. the Finviz industry was **down**
- **WFRD** (mid, Energy, $5.9B) score -0.432. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LU** (small, Financial, $1.0B) score -0.418. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NUS** (micro, Consumer Defensive, $223M) score -0.413. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SUPV** (small, Financial, $613M) score -0.409. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-09-21_stock_book.md`
- Machine table: `data/stock_book/2026-09-21_stock_book.csv`
- Machine book: `data/stock_book/2026-09-21_stock_book.json`
- Join rank: `data/join/2026-09-21_ranked.csv`
- Weather: `01_daily/weather/2026-09-21_weather.md`
- AB enrich: `data/ab_checklist/2026-09-21_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-09-21_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-09-21_map_heat.md`
