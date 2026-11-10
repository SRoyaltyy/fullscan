# Stock book — 2026-11-10

_Generated 2026-11-10T09:13:10.817916-05:00_

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
- General predict (same-day): +0.66 up (present)
- Stand-down: **no** — 558 names qualified through standard,group_leader,catalyst (47 probable)
- Sector predicts this date: 11/11 (ok)
- News tickers in play: 128
- AB coverage: 2173 names · peer RS: 2030
- Universe after liquidity: 2269
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 94

## All-green BUY / SELL

- Mode: **green_pile** · SELL **core_weights_ex_green**
- Pile: **474** liquid all-green names (need ≥ 8) of 2269
- Core fired: join=yes, AB=yes, peer=yes
- pile 474 ≥ 8 liquid all-green names — BUY 15 from the pile by green_rank (no opp); SELL is core weights on the non-green remainder

## Decision lattice — gate → route → rank

The weighted score is now a tie-breaker inside an eligible lane. It cannot average away a market, group, company, or setup veto.

### MARKET: 🟢 GREEN

- GREEN: general up score=+6.88; good=+6.0 vs bad=-2.0; risk=on; red pillars=1
- Allowed long lanes: **standard, group_leader, catalyst** · max slots 15 · size ×1.00
- Bull evidence: overnight catalysts +3.00 points; global sessions +2.00 points; oil / dollar +1.00 points
- Bear evidence: rates / Fed -2.00 points

Decision domains: **MKT · parent · child · company · setup · flow**. Measured parent/child tape is kept separate from the LLM essay; direct company events must be price-confirmed on a hard-red day.

### Bull decisions (eligible or closest blocked cases)

| # | Ticker | Domains | Lane | Company / group | Decision |
|---:|--------|---------|------|-----------------|----------|
| 1 | **CON** | 🟢🟢🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 2 | **PACS** | 🟢🟢🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 3 | **SGRY** | 🟢🟢🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 4 | **ACHC** | 🟢🟢🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 5 | **LFST** | 🟢🟢🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 6 | **UHS** | 🟢🟢🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 7 | **HCA** | 🟢🟢🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 8 | **GRDN** | 🟢🟢🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 9 | **THC** | 🟢🟢🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 10 | **BAH** | 🟢🟡🟢🟡🟢🟢 | group_leader | no direct company event; Consulting Services +3.9% d1 / +0.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 11 | **ARDT** | 🟢🟢🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=⚪,Cond green |
| 12 | **FMS** | 🟢🟢🟢🟡🟢🟡 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,⚪,Cond green |
| 13 | **YEXT** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 14 | **BR** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Information Technology Services +2.9% d1 / +1.9% 1w / +3.9% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 15 | **DXC** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Information Technology Services +2.9% d1 / +1.9% 1w / +3.9% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **RIG** | 🟢🔴🔴🟡🔴🔴 | Oil & Gas Drilling | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 2 | **BORR** | 🟢🔴🔴🟡🔴🔴 | Oil & Gas Drilling | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 3 | **RES** | 🟢🔴🔴🟡🔴🔴 | Oil & Gas Equipment & Services | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -7.7% |
| 4 | **SOC** | 🟢🔴🔴🟡🔴🔴 | Oil & Gas Drilling | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 5 | **AESI** | 🟢🔴🔴🟡🔴🟢 | Oil & Gas Equipment & Services | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -7.7% |
| 6 | **BTU** | 🟢🔴🔴🟡🔴🟡 | Thermal Coal | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -4.4% |
| 7 | **VAL** | 🟢🔴🔴🟡🔴🔴 | Oil & Gas Drilling | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 8 | **DDD** | 🟢🔴🔴🟡🔴🟢 | Computer Hardware | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -3.1% |
| 9 | **ACDC** | 🟢🔴🔴🟡🔴🟢 | Oil & Gas Equipment & Services | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -7.7% |
| 10 | **XPRO** | 🟢🔴🔴🟡🔴🟢 | Oil & Gas Equipment & Services | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -7.7% |
| 11 | **UEC** | 🟢🔴🔴🟡🔴🟡 | Uranium | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -10.3% |
| 12 | **EU** | 🟢🔴🔴🟡🔴🟡 | Uranium | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -10.3% |
| 13 | **SDRL** | 🟢🔴🔴🟡🔴🟡 | Oil & Gas Drilling | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -5.4% |
| 14 | **PAA** | 🟢🔴🔴🟡🔴🔴 | Oil & Gas Midstream | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow |
| 15 | **SEDG** | 🟢🔴🔴🟡🔴🔴 | Solar | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **finviz_tape** (40 captains, 15 industries → s_heat).
- Board file: `01_daily/map_heat/2026-11-10_map_heat.json` · generated 2026-11-10T04:39:58.802207-05:00

### Sector RS vs same-day LLM essay

| Sector | Finviz 1d | Finviz 1w | LLM 1d | Tape vs essay |
|--------|----------:|----------:|-------:|---------------|
| Basic Materials | -2.1% | -4.8% | +0.28 | essay UP, tape DOWN |
| Communication Services | +2.7% | +3.5% | +0.33 |  |
| Consumer Cyclical | -0.4% | -2.0% | +0.38 | essay UP, tape DOWN |
| Consumer Defensive | +1.4% | +0.5% | -0.44 |  |
| Energy | -0.8% | +1.0% | -0.72 |  |
| Financial | -0.4% | -1.8% | +0.62 | essay UP, tape DOWN |
| Healthcare | +1.4% | -2.5% | +0.64 | essay UP, tape DOWN |
| Industrials | -1.6% | -2.7% | +0.29 | essay UP, tape DOWN |
| Real Estate | -0.6% | -2.1% | +0.68 | essay UP, tape DOWN |
| Technology | -2.0% | -2.1% | -0.38 |  |
| Utilities | -1.5% | -3.2% | +0.21 | essay UP, tape DOWN |

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
| Energy | -0.72 |
| Real Estate | +0.68 |
| Healthcare | +0.64 |
| Financial | +0.62 |
| Consumer Defensive | -0.44 |
| Technology | -0.38 |
| Consumer Cyclical | +0.38 |
| Communication Services | +0.33 |
| Industrials | +0.29 |
| Basic Materials | +0.28 |
| Utilities | +0.21 |

### How much each predictor is trusted (graded hit rate)

| Topic | hit rate | n | weight |
|-------|----------|---|--------|
| general | 55% | 62 | ×0.85 |
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

## Horizon weights — book_policy.json v19

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.12 | 0.10 | 0.08 | 0.25 | 0.25 | 0.20 | additive |
| 3d | 0.16 | 0.14 | 0.08 | 0.16 | 0.26 | 0.20 | additive |
| 1w | 0.18 | 0.16 | 0.08 | 0.10 | 0.28 | 0.20 | additive |
| 2w | 0.20 | 0.18 | 0.08 | 0.06 | 0.28 | 0.20 | additive |
| 1m | 0.22 | 0.20 | 0.08 | 0.00 | 0.30 | 0.20 | additive |

## 1d BUY — why these names

### 1. MGNI · $4.2B mid · Communication Services

**1d score +0.889**

**MGNI** is a liquid **mid-cap** Communication Services name (Advertising Agencies) at $4.2B, ADV ~2390k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.33 | +0.033 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.66 | +0.053 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.94 | +0.235 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +1.00 | +0.200 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.889** | |

### 2. TMDX · $3.3B mid · Healthcare

**1d score +0.912**

**TMDX** is a liquid **mid-cap** Healthcare name (Medical Devices) at $3.3B, ADV ~860k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.64 | +0.064 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.66 | +0.053 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.91 | +0.226 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +1.00 | +0.200 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.912** | |

### 3. ETON · $1.9B small · Healthcare

**1d score +0.889**

**ETON** is a liquid **small-cap** Healthcare name (Drug Manufacturers - Specialty & Generic) at $1.9B, ADV ~578k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.95 | +0.114 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.64 | +0.064 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.33 | +0.026 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.94 | +0.235 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +1.00 | +0.200 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.889** | |

### 4. WRBY · $3.7B mid · Healthcare

**1d score +0.910**

**WRBY** is a liquid **mid-cap** Healthcare name (Medical Instruments & Supplies) at $3.7B, ADV ~3131k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.98 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.64 | +0.064 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.66 | +0.053 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.91 | +0.226 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +1.00 | +0.199 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.910** | |

### 5. ADPT · $4.9B mid · Healthcare

**1d score +0.858**

**ADPT** is a liquid **mid-cap** Healthcare name (Diagnostics & Research) at $4.9B, ADV ~2180k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.98 | +0.117 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.64 | +0.064 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.66 | +0.053 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.91 | +0.226 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.99 | +0.198 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.858** | |

### 6. RBLX · $36.9B large · Communication Services

**1d score +0.685**

**RBLX** is a liquid **large-cap** Communication Services name (Electronic Gaming & Multimedia) at $36.9B, ADV ~11435k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.97 | +0.117 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.33 | +0.033 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.66 | +0.053 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.91 | +0.226 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.93 | +0.187 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.07 | +0.070 | liquid small/mid, room to run |
| **1d total** | | | **+0.685** | |

### 7. SG · $1.6B small · Consumer Cyclical

**1d score +0.827**

**SG** is a liquid **small-cap** Consumer Cyclical name (Restaurants) at $1.6B, ADV ~5330k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extreme**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.90 | +0.108 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.46 | +0.046 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.66 | +0.053 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.97 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.89 | +0.178 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.827** | |

### 8. SEZL · $5.1B mid · Financial

**1d score +0.857**

**SEZL** is a liquid **mid-cap** Financial name (Credit Services) at $5.1B, ADV ~621k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.42 | +0.042 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.66 | +0.053 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.96 | +0.192 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.857** | |

### 9. ASO · $4.4B mid · Consumer Cyclical

**1d score +0.835**

**ASO** is a liquid **mid-cap** Consumer Cyclical name (Specialty Retail) at $4.4B, ADV ~1723k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.96 | +0.115 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.46 | +0.046 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.33 | +0.026 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.93 | +0.186 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.835** | |

### 10. ABNB · $112.6B large · Consumer Cyclical

**1d score +0.496**

**ABNB** is a liquid **large-cap** Consumer Cyclical name (Travel Services) at $112.6B, ADV ~4889k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.94 | +0.112 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.46 | +0.046 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.33 | +0.026 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.31 | +0.077 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.93 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.86 | +0.173 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.496** | |

### 11. EVER · $1.0B small · Communication Services

**1d score +0.671**

**EVER** is a liquid **small-cap** Communication Services name (Internet Content & Information) at $1.0B, ADV ~727k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.92 | +0.110 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.33 | +0.033 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.10 | +0.008 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.99 | +0.198 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.12 | +0.120 | liquid small/mid, room to run |
| **1d total** | | | **+0.671** | |

### 12. VVV · $4.6B mid · Consumer Cyclical

**1d score +0.832**

**VVV** is a liquid **mid-cap** Consumer Cyclical name (Auto & Truck Dealerships) at $4.6B, ADV ~2565k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.95 | +0.114 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.46 | +0.046 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.33 | +0.026 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.92 | +0.184 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.832** | |

### 13. RDDT · $19.5B large · Communication Services

**1d score +0.691**

**RDDT** is a liquid **large-cap** Communication Services name (Internet Content & Information) at $19.5B, ADV ~6276k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **washed**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.33 | +0.033 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.66 | +0.053 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.88 | +0.175 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.11 | +0.110 | liquid small/mid, room to run |
| **1d total** | | | **+0.691** | |

### 14. RKT · $43.1B large · Financial

**1d score +0.710**

**RKT** is a liquid **large-cap** Financial name (Mortgage Finance) at $43.1B, ADV ~34750k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.98 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.42 | +0.042 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.66 | +0.053 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.88 | +0.220 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.78 | +0.157 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.07 | +0.070 | liquid small/mid, room to run |
| **1d total** | | | **+0.710** | |

### 15. COMP · $8.3B mid · Real Estate

**1d score +0.831**

**COMP** is a liquid **mid-cap** Real Estate name (Real Estate Services) at $8.3B, ADV ~13712k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.98 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.48 | +0.048 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.66 | +0.053 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.70 | +0.176 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.93 | +0.187 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.831** | |

### 16. COLD · $4.6B mid · Real Estate

**1d score +0.812**

**COLD** is a liquid **mid-cap** Real Estate name (REIT - Industrial) at $4.6B, ADV ~3557k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.90 | +0.108 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.48 | +0.048 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.33 | +0.026 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.88 | +0.220 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.80 | +0.160 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.812** | |

### 17. FG · $3.3B mid · Financial

**1d score +0.807**

**FG** is a liquid **mid-cap** Financial name (Insurance - Life) at $3.3B, ADV ~673k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **extended**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.77 | +0.092 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.42 | +0.042 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.33 | +0.026 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.97 | +0.194 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.807** | |

### 18. GXO · $6.3B mid · Industrials

**1d score +0.797**

**GXO** is a liquid **mid-cap** Industrials name (Integrated Freight & Logistics) at $6.3B, ADV ~1308k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.96 | +0.115 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.37 | +0.037 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.66 | +0.053 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.70 | +0.176 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.84 | +0.167 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.797** | |

### 19. XHR · $2.0B mid · Real Estate

**1d score +0.769**

**XHR** is a liquid **mid-cap** Real Estate name (REIT - Hotel & Motel) at $2.0B, ADV ~846k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide). Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.92 | +0.111 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.48 | +0.048 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.33 | +0.026 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.61 | +0.122 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.769** | |

### 20. BKSY · $978M small · Industrials

**1d score +0.760**

**BKSY** is a liquid **small-cap** Industrials name (Specialty Business Services) at $978M, ADV ~966k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.88 | +0.105 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.37 | +0.037 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.66 | +0.053 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.46 | +0.116 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +1.00 | +0.200 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.760** | |

### 21. FA · $4.0B mid · Industrials

**1d score +0.740**

**FA** is a liquid **mid-cap** Industrials name (Specialty Business Services) at $4.0B, ADV ~1852k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.92 | +0.111 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.37 | +0.037 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.33 | +0.026 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.57 | +0.114 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.740** | |

### 22. VOYA · $9.3B mid · Financial

**1d score +0.742**

**VOYA** is a liquid **mid-cap** Financial name (Financial Conglomerates) at $9.3B, ADV ~1013k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.90 | +0.108 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.42 | +0.042 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.33 | +0.026 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.52 | +0.104 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.742** | |

### 23. SBLK · $3.0B mid · Industrials

**1d score +0.676**

**SBLK** is a liquid **mid-cap** Industrials name (Marine Shipping) at $3.0B, ADV ~1606k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.88 | +0.106 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.37 | +0.037 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.33 | +0.026 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.76 | +0.190 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.34 | +0.067 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.676** | |

### 24. CWK · $3.4B mid · Real Estate

**1d score +0.691**

**CWK** is a liquid **mid-cap** Real Estate name (Real Estate Services) at $3.4B, ADV ~2537k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.93 | +0.112 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.48 | +0.048 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.66 | +0.053 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.08 | +0.017 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.691** | |


## 1d AVOID — bottom of the same rank

- **XPRO** (small, Energy, $1.5B) score -0.562. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -7.7%
- **KOS** (small, Energy, $1.2B) score -0.553. SELL/AVOID — market=GREEN; red domains=parent,setup,flow
- **ACDC** (small, Energy, $623M) score -0.528. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -7.7%
- **BTU** (mid, Energy, $2.1B) score -0.525. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -4.4%
- **VET** (small, Energy, $1.6B) score -0.516. SELL/AVOID — market=GREEN; red domains=parent,setup,flow
- **AESI** (small, Energy, $694M) score -0.506. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -7.7%
- **IEP** (mid, Energy, $3.7B) score -0.496. SELL/AVOID — market=GREEN; red domains=parent,setup
- **SOC** (micro, Energy, $169M) score -0.488. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -5.4%
- **RES** (small, Energy, $1.2B) score -0.457. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -7.7%
- **BORR** (small, Energy, $993M) score -0.451. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -5.4%
- **SFM** (mid, Consumer Defensive, $4.6B) score -0.444. SELL/AVOID — market=GREEN; red domains=setup,flow
- **NUS** (micro, Consumer Defensive, $208M) score -0.435. NO BEAR — market=GREEN; red domains=setup
- **INGR** (mid, Consumer Defensive, $5.9B) score -0.385. NO BEAR — market=GREEN; red domains=setup; child lags parent -3.5%
- **ZS** (large, Technology, $32.5B) score -0.383. SELL/AVOID — market=GREEN; red domains=parent,setup
- **VSAT** (large, Technology, $10.2B) score -0.383. SELL/AVOID — market=GREEN; red domains=parent,child,setup
- **SDRL** (mid, Energy, $2.6B) score -0.370. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -5.4%
- **DELL** (mega, Technology, $220.2B) score -0.360. SELL/AVOID — market=GREEN; red domains=parent,child; child lags parent -3.1%
- **CNDT** (micro, Technology, $216M) score -0.344. SELL/AVOID — market=GREEN; red domains=parent,setup,flow
- **RIG** (mid, Energy, $5.8B) score -0.339. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -5.4%
- **PAA** (large, Energy, $16.7B) score -0.320. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow
- **MNR** (small, Energy, $1.6B) score -0.313. SELL/AVOID — market=GREEN; red domains=parent,setup
- **EU** (micro, Energy, $136M) score -0.308. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -10.3%
- **GTE** (small, Energy, $339M) score -0.306. SELL/AVOID — market=GREEN; red domains=parent,setup
- **PTEN** (mid, Energy, $3.7B) score -0.304. SELL/AVOID — market=GREEN; red domains=parent,child,flow; child lags parent -5.4%
- **BLND** (micro, Technology, $233M) score -0.300. SELL/AVOID — market=GREEN; red domains=parent,setup,flow

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | MGNI | +0.890 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | TMDX | +0.881 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | ETON | +0.865 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 4 | WRBY | +0.878 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | ADPT | +0.827 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | RBLX | +0.686 | large | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | SG | +0.864 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | SEZL | +0.873 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 9 | ASO | +0.881 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | ABNB | +0.514 | large | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | EVER | +0.681 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 12 | VVV | +0.878 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 13 | RDDT | +0.691 | large | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 14 | RKT | +0.726 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 15 | COMP | +0.848 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 16 | COLD | +0.836 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 17 | FG | +0.821 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 18 | GXO | +0.830 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 19 | XHR | +0.792 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide) |
| 20 | BKSY | +0.787 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 21 | FA | +0.780 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 22 | VOYA | +0.763 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 23 | SBLK | +0.714 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 24 | CWK | +0.708 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | MGNI | +0.925 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | TMDX | +0.969 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | ETON | +0.955 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 4 | WRBY | +0.967 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | ADPT | +0.916 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | RBLX | +0.720 | large | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | SG | +0.902 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | SEZL | +0.916 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 9 | ASO | +0.919 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | ABNB | +0.535 | large | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | EVER | +0.715 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 12 | VVV | +0.916 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 13 | RDDT | +0.723 | large | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 14 | RKT | +0.770 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 15 | COMP | +0.813 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 16 | COLD | +0.804 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 17 | FG | +0.862 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 18 | GXO | +0.866 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 19 | XHR | +0.760 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide) |
| 20 | BKSY | +0.816 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 21 | FA | +0.819 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 22 | VOYA | +0.806 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 23 | SBLK | +0.751 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 24 | CWK | +0.674 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | MGNI | +0.910 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | TMDX | +0.962 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | ETON | +0.964 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 4 | WRBY | +0.960 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | ADPT | +0.908 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | RBLX | +0.705 | large | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | SG | +0.888 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | SEZL | +0.906 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 9 | ASO | +0.924 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | ABNB | +0.527 | large | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | EVER | +0.728 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 12 | VVV | +0.921 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 13 | RDDT | +0.709 | large | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 14 | RKT | +0.761 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 15 | COMP | +0.794 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 16 | COLD | +0.801 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 17 | FG | +0.865 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 18 | GXO | +0.852 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 19 | XHR | +0.758 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide) |
| 20 | BKSY | +0.801 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 21 | FA | +0.821 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 22 | VOYA | +0.812 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 23 | SBLK | +0.753 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 24 | CWK | +0.655 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |

## 1m BUY — why these names

### 1. MGNI · $4.2B mid · Communication Services

**1m score +0.980**

**MGNI** is a liquid **mid-cap** Communication Services name (Advertising Agencies) at $4.2B, ADV ~2390k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.38 | +0.031 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.94 | +0.282 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +1.00 | +0.200 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.980** | |

### 2. TMDX · $3.3B mid · Healthcare

**1m score +1.046**

**TMDX** is a liquid **mid-cap** Healthcare name (Medical Devices) at $3.3B, ADV ~860k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.38 | +0.077 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.38 | +0.031 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +1.00 | +0.200 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.046** | |

### 3. ETON · $1.9B small · Healthcare

**1m score +1.032**

**ETON** is a liquid **small-cap** Healthcare name (Drug Manufacturers - Specialty & Generic) at $1.9B, ADV ~578k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.95 | +0.209 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.38 | +0.077 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.19 | +0.015 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.94 | +0.282 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +1.00 | +0.200 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.032** | |

### 4. WRBY · $3.7B mid · Healthcare

**1m score +1.043**

**WRBY** is a liquid **mid-cap** Healthcare name (Medical Instruments & Supplies) at $3.7B, ADV ~3131k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.98 | +0.215 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.38 | +0.077 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.38 | +0.031 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +1.00 | +0.199 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.043** | |

### 5. ADPT · $4.9B mid · Healthcare

**1m score +0.992**

**ADPT** is a liquid **mid-cap** Healthcare name (Diagnostics & Research) at $4.9B, ADV ~2180k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.98 | +0.215 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.38 | +0.077 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.38 | +0.031 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.99 | +0.198 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.992** | |

### 6. RBLX · $36.9B large · Communication Services

**1m score +0.773**

**RBLX** is a liquid **large-cap** Communication Services name (Electronic Gaming & Multimedia) at $36.9B, ADV ~11435k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.97 | +0.214 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.38 | +0.031 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.93 | +0.187 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.07 | +0.070 | liquid small/mid, room to run |
| **1m total** | | | **+0.773** | |

### 7. SG · $1.6B small · Consumer Cyclical

**1m score +0.958**

**SG** is a liquid **small-cap** Consumer Cyclical name (Restaurants) at $1.6B, ADV ~5330k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extreme**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.90 | +0.197 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.31 | +0.061 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.38 | +0.031 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.97 | +0.292 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.89 | +0.178 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.958** | |

### 8. SEZL · $5.1B mid · Financial

**1m score +0.986**

**SEZL** is a liquid **mid-cap** Financial name (Credit Services) at $5.1B, ADV ~621k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.27 | +0.054 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.38 | +0.031 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.96 | +0.192 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.986** | |

### 9. ASO · $4.4B mid · Consumer Cyclical

**1m score +0.977**

**ASO** is a liquid **mid-cap** Consumer Cyclical name (Specialty Retail) at $4.4B, ADV ~1723k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.96 | +0.211 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.31 | +0.061 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.19 | +0.015 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.93 | +0.186 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.977** | |

### 10. ABNB · $112.6B large · Consumer Cyclical

**1m score +0.562**

**ABNB** is a liquid **large-cap** Consumer Cyclical name (Travel Services) at $112.6B, ADV ~4889k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.94 | +0.206 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.31 | +0.061 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.19 | +0.015 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.31 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.93 | +0.278 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.86 | +0.173 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1m total** | | | **+0.562** | |

### 11. EVER · $1.0B small · Communication Services

**1m score +0.768**

**EVER** is a liquid **small-cap** Communication Services name (Internet Content & Information) at $1.0B, ADV ~727k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.92 | +0.202 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.06 | +0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.99 | +0.198 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.12 | +0.120 | liquid small/mid, room to run |
| **1m total** | | | **+0.768** | |

### 12. VVV · $4.6B mid · Consumer Cyclical

**1m score +0.974**

**VVV** is a liquid **mid-cap** Consumer Cyclical name (Auto & Truck Dealerships) at $4.6B, ADV ~2565k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.95 | +0.209 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.31 | +0.061 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.19 | +0.015 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.92 | +0.184 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.974** | |

### 13. RDDT · $19.5B large · Communication Services

**1m score +0.776**

**RDDT** is a liquid **large-cap** Communication Services name (Internet Content & Information) at $19.5B, ADV ~6276k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **washed**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.38 | +0.031 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.88 | +0.175 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.11 | +0.110 | liquid small/mid, room to run |
| **1m total** | | | **+0.776** | |

### 14. RKT · $43.1B large · Financial

**1m score +0.841**

**RKT** is a liquid **large-cap** Financial name (Mortgage Finance) at $43.1B, ADV ~34750k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.98 | +0.216 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.27 | +0.054 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.38 | +0.031 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.78 | +0.157 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.07 | +0.070 | liquid small/mid, room to run |
| **1m total** | | | **+0.841** | |

### 15. COMP · $8.3B mid · Real Estate

**1m score +0.854**

**COMP** is a liquid **mid-cap** Real Estate name (Real Estate Services) at $8.3B, ADV ~13712k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.98 | +0.216 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.38 | +0.031 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.211 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.93 | +0.187 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.854** | |

### 16. COLD · $4.6B mid · Real Estate

**1m score +0.848**

**COLD** is a liquid **mid-cap** Real Estate name (REIT - Industrial) at $4.6B, ADV ~3557k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.90 | +0.198 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.19 | +0.015 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.80 | +0.160 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.848** | |

### 17. FG · $3.3B mid · Financial

**1m score +0.924**

**FG** is a liquid **mid-cap** Financial name (Insurance - Life) at $3.3B, ADV ~673k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **extended**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.77 | +0.169 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.27 | +0.054 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.19 | +0.015 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.97 | +0.194 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.924** | |

### 18. GXO · $6.3B mid · Industrials

**1m score +0.921**

**GXO** is a liquid **mid-cap** Industrials name (Integrated Freight & Logistics) at $6.3B, ADV ~1308k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.96 | +0.211 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.26 | +0.051 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.38 | +0.031 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.211 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.84 | +0.167 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.921** | |

### 19. XHR · $2.0B mid · Real Estate

**1m score +0.804**

**XHR** is a liquid **mid-cap** Real Estate name (REIT - Hotel & Motel) at $2.0B, ADV ~846k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide). Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.92 | +0.203 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.19 | +0.015 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.61 | +0.122 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.804** | |

### 20. BKSY · $978M small · Industrials

**1m score +0.863**

**BKSY** is a liquid **small-cap** Industrials name (Specialty Business Services) at $978M, ADV ~966k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.88 | +0.193 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.26 | +0.051 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.38 | +0.031 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.46 | +0.139 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +1.00 | +0.200 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.863** | |

### 21. FA · $4.0B mid · Industrials

**1m score +0.876**

**FA** is a liquid **mid-cap** Industrials name (Specialty Business Services) at $4.0B, ADV ~1852k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.92 | +0.203 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.26 | +0.051 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.19 | +0.015 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.57 | +0.114 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.876** | |

### 22. VOYA · $9.3B mid · Financial

**1m score +0.875**

**VOYA** is a liquid **mid-cap** Financial name (Financial Conglomerates) at $9.3B, ADV ~1013k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.90 | +0.198 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.27 | +0.054 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.19 | +0.015 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.52 | +0.104 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.875** | |

### 23. SBLK · $3.0B mid · Industrials

**1m score +0.806**

**SBLK** is a liquid **mid-cap** Industrials name (Marine Shipping) at $3.0B, ADV ~1606k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.88 | +0.194 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.26 | +0.051 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.19 | +0.015 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.34 | +0.067 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.806** | |

### 24. CWK · $3.4B mid · Real Estate

**1m score +0.717**

**CWK** is a liquid **mid-cap** Real Estate name (Real Estate Services) at $3.4B, ADV ~2537k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.93 | +0.205 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.38 | +0.031 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.08 | +0.017 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.717** | |


## 1m AVOID — bottom of the same rank

- **XPRO** (small, Energy, $1.5B) score -0.646. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **KOS** (small, Energy, $1.2B) score -0.645. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ACDC** (small, Energy, $623M) score -0.613. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AESI** (small, Energy, $694M) score -0.587. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **VET** (small, Energy, $1.6B) score -0.585. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BTU** (mid, Energy, $2.1B) score -0.578. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **IEP** (mid, Energy, $3.7B) score -0.547. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **RES** (small, Energy, $1.2B) score -0.541. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BORR** (small, Energy, $993M) score -0.535. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NUS** (micro, Consumer Defensive, $208M) score -0.518. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **SFM** (mid, Consumer Defensive, $4.6B) score -0.483. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SOC** (micro, Energy, $169M) score -0.480. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ZS** (large, Technology, $32.5B) score -0.459. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SDRL** (mid, Energy, $2.6B) score -0.457. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **VSAT** (large, Technology, $10.2B) score -0.453. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **CNDT** (micro, Technology, $216M) score -0.447. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **INGR** (mid, Consumer Defensive, $5.9B) score -0.446. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **FMC** (small, Basic Materials, $842M) score -0.411. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **RIG** (mid, Energy, $5.8B) score -0.392. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BLND** (micro, Technology, $233M) score -0.380. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **GTE** (small, Energy, $339M) score -0.372. this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **HPK** (small, Energy, $855M) score -0.370. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PAA** (large, Energy, $16.7B) score -0.368. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **DELL** (mega, Technology, $220.2B) score -0.363. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **EU** (micro, Energy, $136M) score -0.363. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**

## Files for this run

- This rationale: `01_daily/2026-11-10_stock_book.md`
- Machine table: `data/stock_book/2026-11-10_stock_book.csv`
- Machine book: `data/stock_book/2026-11-10_stock_book.json`
- Join rank: `data/join/2026-11-10_ranked.csv`
- Weather: `01_daily/weather/2026-11-10_weather.md`
- AB enrich: `data/ab_checklist/2026-11-10_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-11-10_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-11-10_map_heat.md`
