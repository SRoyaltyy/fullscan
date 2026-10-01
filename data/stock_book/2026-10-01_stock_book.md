# Stock book — 2026-10-01

_Generated 2026-10-01T17:31:32.703197-04:00_

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
- General predict (same-day): -0.47 down (present)
- Stand-down: **no** — 41 names qualified through group_leader,catalyst,probable (24 probable)
- Sector predicts this date: 11/11 (ok)
- News tickers in play: 113
- AB coverage: 1906 names · peer RS: 1804
- Universe after liquidity: 2035
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 58

## All-green BUY / SELL

- Mode: **weighted_fallback** · SELL **core_weights**
- Pile: **0** liquid all-green names (need ≥ 8) of 2035
- Core fired: join=yes, AB=yes, peer=yes
- pile 0 < 8 liquid all-green names. Fallback weighted walk; SELL stays on core

## Decision lattice — gate → route → rank

The weighted score is now a tie-breaker inside an eligible lane. It cannot average away a market, group, company, or setup veto.

### MARKET: 🔴 RED

- RED: general down score=-1.34; good=+2.2 vs bad=-4.5; risk=off; red pillars=3
- Allowed long lanes: **group_leader, catalyst, probable** · max slots 8 · size ×0.35
- Bull evidence: futures +0.25 points
- Bear evidence: global sessions -1.00 points; rates / Fed -1.00 points; oil / dollar -0.50 points

Decision domains: **MKT · parent · child · company · setup · flow**. Measured parent/child tape is kept separate from the LLM essay; direct company events must be price-confirmed on a hard-red day.

### Bull decisions (eligible or closest blocked cases)

| # | Ticker | Domains | Lane | Company / group | Decision |
|---:|--------|---------|------|-----------------|----------|
| 1 | **BKR** | 🔴🔴🔴🟢🟢🟢 | probable | usable dossier Bullish conv=28; Oil & Gas Equipment & Services -4.4% d1 / -6.8% 1w / -7.7% vs parent | BUY PROBABLE — most-probable long on RED (size ×0.35); clocks: company news fresh (0.80) — market=RED; parent=RED; child=RED/rel=RED; company=GREEN(0.80); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 2 | **CNR** | 🔴🔴🔴🟢🟡🟢 | probable | usable dossier Bullish conv=21; Thermal Coal -1.8% d1 / -3.4% 1w / -4.4% vs parent | BUY PROBABLE — most-probable long on RED (size ×0.35); clocks: company news fresh (0.80) — market=RED; parent=RED; child=RED/rel=RED; company=GREEN(0.80); setup=YELLOW; flow=GREEN; lookback=🔵,Cond green |
| 3 | **BAND** | 🔴🟡🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 4 | **NTAP** | 🔴🟡🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 5 | **OKTA** | 🔴🟡🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 6 | **BB** | 🔴🟡🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 7 | **EPAM** | 🔴🟡🟢🟡🟢🟢 | group_leader | no direct company event; Information Technology Services +2.9% d1 / +1.9% 1w / +3.9% vs parent | BUY GROUP_LEADER — market=RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 8 | **NTSK** | 🔴🟡🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 9 | **FIS** | 🔴🟡🟢🟡🟢🟢 | group_leader | no direct company event; Information Technology Services +2.9% d1 / +1.9% 1w / +3.9% vs parent | BUY GROUP_LEADER — market=RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 10 | **NBIS** | 🔴🟡🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 11 | **PENG** | 🔴🟡🟢🟡🟢🟡 | group_leader | no direct company event; Information Technology Services +2.9% d1 / +1.9% 1w / +3.9% vs parent | BUY GROUP_LEADER — market=RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=Cond green |
| 12 | **BLZE** | 🔴🟡🟢🟡🟢🟡 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=Cond green |
| 13 | **TENB** | 🔴🟡🟢🟡🟢🟡 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=Cond green |
| 14 | **GDS** | 🔴🟡🟢🟡🟢🟡 | group_leader | no direct company event; Information Technology Services +2.9% d1 / +1.9% 1w / +3.9% vs parent | BUY GROUP_LEADER — market=RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=Cond green |
| 15 | **VRSN** | 🔴🟡🟢🟡🟢🟡 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **NEOV** | 🔴🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 2 | **FCEL** | 🔴🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 3 | **OKLO** | 🔴🔴🔴🟡🔴🔴 | Utilities - Independent Power Producers | SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -6.3% |
| 4 | **EOSE** | 🔴🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=RED; red domains=parent,child,setup; child lags parent -4.7% |
| 5 | **POWL** | 🔴🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 6 | **TE** | 🔴🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=RED; red domains=parent,child,setup; child lags parent -4.7% |
| 7 | **SKYX** | 🔴🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=RED; red domains=parent,child,setup; child lags parent -4.7% |
| 8 | **AMSC** | 🔴🔴🔴🟡🔴🔴 | Specialty Industrial Machinery | SELL/AVOID — market=RED; red domains=parent,child,setup,flow |
| 9 | **ACDC** | 🔴🔴🔴🟡🔴🟡 | Oil & Gas Equipment & Services | SELL/AVOID — market=RED; red domains=parent,child,setup; child lags parent -7.7% |
| 10 | **METC** | 🔴🔴🔴🟡🔴🟡 | Coking Coal | SELL/AVOID — market=RED; red domains=parent,child,setup; child lags parent -5.7% |
| 11 | **PLUG** | 🔴🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=RED; red domains=parent,child,setup; child lags parent -4.7% |
| 12 | **LUNR** | 🔴🔴🔴🟡🔴🔴 | Aerospace & Defense | SELL/AVOID — market=RED; red domains=parent,child,setup,flow |
| 13 | **MNTS** | 🔴🔴🔴🟡🔴🔴 | Aerospace & Defense | SELL/AVOID — market=RED; red domains=parent,child,setup,flow |
| 14 | **AESI** | 🔴🔴🔴🟡🔴🟡 | Oil & Gas Equipment & Services | SELL/AVOID — market=RED; red domains=parent,child,setup; child lags parent -7.7% |
| 15 | **SOC** | 🔴🔴🔴🟡🔴🟡 | Oil & Gas Drilling | SELL/AVOID — market=RED; red domains=parent,child,setup; child lags parent -5.4% |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **captain_research** (280 captains, 6 industries → s_heat).
- Board file: `01_daily/map_heat/2026-10-01_map_heat.json` · generated 2026-10-01T05:41:52.261525-04:00

### Sector RS vs same-day LLM essay

| Sector | Finviz 1d | Finviz 1w | LLM 1d | Tape vs essay |
|--------|----------:|----------:|-------:|---------------|
| Basic Materials | -2.1% | -4.8% | -0.37 |  |
| Communication Services | +2.7% | +3.5% | +0.40 |  |
| Consumer Cyclical | -0.4% | -2.0% | -0.66 |  |
| Consumer Defensive | +1.4% | +0.5% | -0.51 |  |
| Energy | -0.8% | +1.0% | -0.42 |  |
| Financial | -0.4% | -1.8% | -0.35 |  |
| Healthcare | +1.4% | -2.5% | -0.56 |  |
| Industrials | -1.6% | -2.7% | -0.21 |  |
| Real Estate | -0.6% | -2.1% | -0.63 |  |
| Technology | -2.0% | -2.1% | +0.35 | essay UP, tape DOWN |
| Utilities | -1.5% | -3.2% | -0.22 |  |

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
| Consumer Cyclical | -0.66 |
| Real Estate | -0.63 |
| Healthcare | -0.56 |
| Consumer Defensive | -0.51 |
| Energy | -0.42 |
| Communication Services | +0.40 |
| Basic Materials | -0.37 |
| Financial | -0.35 |
| Technology | +0.35 |
| Utilities | -0.22 |
| Industrials | -0.21 |

### How much each predictor is trusted (graded hit rate)

| Topic | hit rate | n | weight |
|-------|----------|---|--------|
| general | 50% | 42 | ×0.85 |
| sector:Basic Materials | 47% | 30 | ×0.85 |
| sector:Communication Services | 21% | 29 | ×0.50 |
| sector:Consumer Cyclical | 47% | 30 | ×0.85 |
| sector:Consumer Defensive | 48% | 29 | ×0.85 |
| sector:Energy | 53% | 30 | ×0.85 |
| sector:Financial | 45% | 29 | ×0.50 |
| sector:Healthcare | 50% | 26 | ×0.85 |
| sector:Industrials | 34% | 29 | ×0.50 |
| sector:Real Estate | 55% | 29 | ×1.00 |
| sector:Technology | 39% | 28 | ×0.50 |
| sector:Utilities | 41% | 27 | ×0.50 |

## Horizon weights — book_policy.json v15

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.12 | 0.10 | 0.08 | 0.25 | 0.25 | 0.20 | additive |
| 3d | 0.16 | 0.14 | 0.08 | 0.16 | 0.26 | 0.20 | additive |
| 1w | 0.18 | 0.16 | 0.08 | 0.10 | 0.28 | 0.20 | additive |
| 2w | 0.20 | 0.18 | 0.08 | 0.06 | 0.28 | 0.20 | additive |
| 1m | 0.22 | 0.20 | 0.08 | 0.00 | 0.30 | 0.20 | additive |

## 1d BUY — why these names

### 1. BKR · $56.6B large · Energy

**1d score +0.133**

**BKR** is a liquid **large-cap** Energy name (Oil & Gas Equipment & Services) at $56.6B, ADV ~8443k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.68 | +0.082 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | -0.42 | -0.042 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.24 | -0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.10 | +0.025 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.36 | +0.090 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.09 | +0.018 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.07 | -0.070 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1d total** | | | **+0.133** | |

### 2. CNR · $4.4B mid · Energy

**1d score +0.261**

**CNR** is a liquid **mid-cap** Energy name (Thermal Coal) at $4.4B, ADV ~536k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.73 | +0.088 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | -0.42 | -0.042 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.09 | +0.022 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.64 | +0.159 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.09 | +0.017 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.03 | -0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1d total** | | | **+0.261** | |

### 3. NTAP · $40.2B large · Technology

**1d score +0.368**

**NTAP** is a liquid **large-cap** Technology name (Software - Infrastructure) at $40.2B, ADV ~2546k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.55 | +0.055 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.47 | -0.038 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.65 | +0.130 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.11 | -0.110 | liquid small/mid, room to run |
| **1d total** | | | **+0.368** | |

### 4. BAND · $2.0B small · Technology

**1d score +0.692**

**BAND** is a liquid **small-cap** Technology name (Software - Infrastructure) at $2.0B, ADV ~1056k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.93 | +0.111 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.55 | +0.055 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.47 | -0.038 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.88 | +0.220 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.72 | +0.143 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.692** | |

### 5. NTSK · $7.4B mid · Technology

**1d score +0.474**

**NTSK** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $7.4B, ADV ~5491k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.98 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.55 | +0.055 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.47 | -0.038 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.55 | +0.139 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.474** | |


## 1d AVOID — bottom of the same rank

- **NEOV** (micro, Industrials, $133M) score -0.241. SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -4.7%
- **FCEL** (small, Industrials, $1.3B) score -0.248. SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -4.7%
- **OKLO** (mid, Utilities, $6.9B) score -0.181. SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -6.3%
- **EOSE** (small, Industrials, $1.1B) score -0.311. SELL/AVOID — market=RED; red domains=parent,child,setup; child lags parent -4.7%
- **POWL** (mid, Industrials, $7.1B) score -0.089. SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -4.7%
- **TE** (small, Industrials, $1.1B) score -0.302. SELL/AVOID — market=RED; red domains=parent,child,setup; child lags parent -4.7%
- **SKYX** (micro, Industrials, $158M) score -0.294. SELL/AVOID — market=RED; red domains=parent,child,setup; child lags parent -4.7%
- **AMSC** (small, Industrials, $1.4B) score -0.145. SELL/AVOID — market=RED; red domains=parent,child,setup,flow

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | MQ | +0.826 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 2 | AVPT | +0.808 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | BAND | +0.797 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | SONO | +0.753 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | CNR | +0.277 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 6 | BKR | +0.157 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | MQ | +0.857 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 2 | AVPT | +0.843 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | BAND | +0.814 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | SONO | +0.771 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | CNR | +0.286 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 6 | BKR | +0.148 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | MQ | +0.887 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 2 | AVPT | +0.874 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | BAND | +0.843 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | SONO | +0.802 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | CNR | +0.288 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 6 | BKR | +0.149 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1m BUY — why these names

### 1. MQ · $1.8B small · Technology

**1m score +0.935**

**MQ** is a liquid **small-cap** Technology name (Software - Infrastructure) at $1.8B, ADV ~971k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.95 | +0.210 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.55 | +0.110 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.17 | -0.014 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.93 | +0.186 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.935** | |

### 2. AVPT · $2.9B mid · Technology

**1m score +0.926**

**AVPT** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $2.9B, ADV ~2025k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.55 | +0.110 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.17 | -0.014 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.96 | +0.289 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.61 | +0.123 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.926** | |

### 3. BAND · $2.0B small · Technology

**1m score +0.894**

**BAND** is a liquid **small-cap** Technology name (Software - Infrastructure) at $2.0B, ADV ~1056k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.93 | +0.204 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.55 | +0.110 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.34 | -0.027 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.72 | +0.143 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.894** | |

### 4. SONO · $2.0B mid · Technology

**1m score +0.854**

**SONO** is a liquid **mid-cap** Technology name (Consumer Electronics) at $2.0B, ADV ~1940k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.98 | +0.216 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.55 | +0.110 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.34 | -0.027 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.27 | +0.055 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.854** | |

### 5. CNR · $4.4B mid · Energy

**1m score +0.302**

**CNR** is a liquid **mid-cap** Energy name (Thermal Coal) at $4.4B, ADV ~536k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.73 | +0.161 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.42 | -0.084 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.05 | -0.004 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.09 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.64 | +0.191 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.09 | +0.017 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.03 | -0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.302** | |

### 6. BKR · $56.6B large · Energy

**1m score +0.157**

**BKR** is a liquid **large-cap** Energy name (Oil & Gas Equipment & Services) at $56.6B, ADV ~8443k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.68 | +0.150 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.42 | -0.084 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.17 | -0.014 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.10 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.36 | +0.108 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.09 | +0.018 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.07 | -0.070 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.157** | |


## 1m AVOID — bottom of the same rank

- **SUPV** (small, Financial, $530M) score -0.761. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TMC** (small, Basic Materials, $1.7B) score -0.754. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RC** (micro, Real Estate, $218M) score -0.749. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AUR** (large, Consumer Cyclical, $10.7B) score -0.747. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ASPI** (small, Basic Materials, $402M) score -0.734. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LDI** (micro, Financial, $216M) score -0.729. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BAK** (micro, Basic Materials, $257M) score -0.729. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PRM** (mid, Basic Materials, $4.6B) score -0.728. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CMTG** (micro, Real Estate, $179M) score -0.727. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BETR** (micro, Financial, $192M) score -0.725. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LWLG** (small, Basic Materials, $771M) score -0.723. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OPEN** (mid, Real Estate, $2.4B) score -0.710. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CWH** (small, Consumer Cyclical, $534M) score -0.704. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LCID** (small, Consumer Cyclical, $1.5B) score -0.702. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OI** (small, Consumer Cyclical, $891M) score -0.696. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **STEP** (mid, Financial, $5.2B) score -0.689. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **DHC** (small, Real Estate, $1.9B) score -0.681. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FLUT** (large, Consumer Cyclical, $13.3B) score -0.678. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **UAMY** (small, Basic Materials, $648M) score -0.674. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ACDC** (small, Energy, $818M) score -0.674. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BORR** (small, Energy, $1.2B) score -0.669. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **METC** (small, Basic Materials, $518M) score -0.665. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **HIVE** (small, Financial, $834M) score -0.665. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GT** (small, Consumer Cyclical, $1.4B) score -0.660. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **MBC** (small, Consumer Cyclical, $1.4B) score -0.658. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**

## Files for this run

- This rationale: `01_daily/2026-10-01_stock_book.md`
- Machine table: `data/stock_book/2026-10-01_stock_book.csv`
- Machine book: `data/stock_book/2026-10-01_stock_book.json`
- Join rank: `data/join/2026-10-01_ranked.csv`
- Weather: `01_daily/weather/2026-10-01_weather.md`
- AB enrich: `data/ab_checklist/2026-10-01_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-10-01_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-10-01_map_heat.md`
