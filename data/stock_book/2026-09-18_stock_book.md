# Stock book — 2026-09-18

_Generated 2026-09-18T18:40:58.835476-04:00_

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
- Stand-down: **no** — 407 names qualified through standard,group_leader,catalyst (26 probable)
- Sector predicts this date: 11/11 (ok)
- News tickers in play: 69
- AB coverage: 1907 names · peer RS: 1869
- Universe after liquidity: 2096
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 35

## All-green BUY / SELL

- Mode: **green_pile** · SELL **core_weights_ex_green**
- Pile: **347** liquid all-green names (need ≥ 8) of 2096
- Core fired: join=yes, AB=yes, peer=yes
- pile 347 ≥ 8 liquid all-green names — BUY 15 from the pile by green_rank (no opp); SELL is core weights on the non-green remainder

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
| 1 | **CMPS** | 🟢🟢🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 2 | **CRWD** | 🟢🔴🟢🟡🟢🟢 | group_leader | direct normal digest (stale/undated): Bernstein raises CrowdStrike price target to $132 from $119 on stronger cybersecurity demand signals and improved sentiment; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.30); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 3 | **RBRK** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 4 | **BLZE** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 5 | **RXT** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 6 | **OPCH** | 🟢🟢🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=⚪,Cond green |
| 7 | **PRTH** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 8 | **CNDT** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Information Technology Services +2.9% d1 / +1.9% 1w / +3.9% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 9 | **VUZI** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Consumer Electronics +0.3% d1 / +4.0% 1w / +6.0% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 10 | **SABR** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 11 | **RPAY** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 12 | **ACHC** | 🟢🟢🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=⚪,Cond green |
| 13 | **CON** | 🟢🟢🟢🟡🟢🟡 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=⚪,Cond green |
| 14 | **AMN** | 🟢🟢🟢🟡🟢🟡 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=⚪,Cond green |
| 15 | **AVAH** | 🟢🟢🟢🟡🟢🟡 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=⚪,Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **SOC** | 🟢🔴🔴🟡🔴🔴 | Oil & Gas Drilling | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 2 | **METC** | 🟢🔴🔴🟡🔴🔴 | Coking Coal | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -5.7% |
| 3 | **TTI** | 🟢🔴🔴🟡🔴🔴 | Oil & Gas Equipment & Services | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -7.7% |
| 4 | **HAL** | 🟢🔴🔴🟡🔴🔴 | Oil & Gas Equipment & Services | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -7.7% |
| 5 | **FLNC** | 🟢🔴🔴🟡🔴🔴 | Utilities - Renewable | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -4.1% |
| 6 | **SGML** | 🟢🔴🔴🟡🔴🔴 | Other Industrial Metals & Mining | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow |
| 7 | **EU** | 🟢🔴🔴🟡🔴🟢 | Uranium | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -10.3% |
| 8 | **NRG** | 🟢🔴🔴🟡🔴🔴 | Utilities - Independent Power Producers | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -6.3% |
| 9 | **PUMP** | 🟢🔴🔴🟡🔴🟡 | Oil & Gas Equipment & Services | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -7.7% |
| 10 | **WFRD** | 🟢🔴🔴🟡🔴🟡 | Oil & Gas Equipment & Services | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -7.7% |
| 11 | **RES** | 🟢🔴🔴🟡🔴🔴 | Oil & Gas Equipment & Services | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -7.7% |
| 12 | **HUN** | 🟢🔴🔴🟡🔴🔴 | Chemicals | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow |
| 13 | **UUUU** | 🟢🔴🔴🟡🔴🟡 | Uranium | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -10.3% |
| 14 | **HDSN** | 🟢🔴🔴🟡🔴🔴 | Specialty Chemicals | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow |
| 15 | **BTU** | 🟢🔴🔴🟡🔴🟡 | Thermal Coal | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -4.4% |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **captain_research** (287 captains, 6 industries → s_heat).
- Board file: `01_daily/map_heat/2026-09-18_map_heat.json` · generated 2026-09-18T04:09:36.036248-04:00

### Sector RS vs same-day LLM essay

| Sector | Finviz 1d | Finviz 1w | LLM 1d | Tape vs essay |
|--------|----------:|----------:|-------:|---------------|
| Basic Materials | -2.1% | -4.8% | +0.00 | essay flat, tape moving |
| Communication Services | +2.7% | +3.5% | +0.32 |  |
| Consumer Cyclical | -0.4% | -2.0% | +0.00 | essay flat, tape moving |
| Consumer Defensive | +1.4% | +0.5% | +0.20 |  |
| Energy | -0.8% | +1.0% | -0.55 |  |
| Financial | -0.4% | -1.8% | +0.00 | essay flat, tape moving |
| Healthcare | +1.4% | -2.5% | +0.51 | essay UP, tape DOWN |
| Industrials | -1.6% | -2.7% | +0.00 | essay flat, tape moving |
| Real Estate | -0.6% | -2.1% | +0.00 | essay flat, tape moving |
| Technology | -2.0% | -2.1% | +0.00 | essay flat, tape moving |
| Utilities | -1.5% | -3.2% | +0.00 | essay flat, tape moving |

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

### 1. RBRK · $22.4B large · Technology

**1d score +0.439**

**RBRK** is a liquid **large-cap** Technology name (Software - Infrastructure) at $22.4B, ADV ~3387k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

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

### 2. FIVN · $2.6B mid · Technology

**1d score +0.766**

**FIVN** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $2.6B, ADV ~2583k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

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

### 3. SDGR · $2.3B mid · Healthcare

**1d score +0.798**

**SDGR** is a liquid **mid-cap** Healthcare name (Health Information Services) at $2.3B, ADV ~1237k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extreme**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

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

### 4. ILMN · $37.0B large · Healthcare

**1d score +0.487**

**ILMN** is a liquid **large-cap** Healthcare name (Diagnostics & Research) at $37.0B, ADV ~1986k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

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

### 5. ZS · $32.2B large · Technology

**1d score +0.605**

**ZS** is a liquid **large-cap** Technology name (Software - Infrastructure) at $32.2B, ADV ~3006k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.97 | +0.116 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.29 | +0.024 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.87 | +0.173 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.07 | +0.070 | liquid small/mid, room to run |
| **1d total** | | | **+0.605** | |

### 6. ARQT · $3.3B mid · Healthcare

**1d score +0.779**

**ARQT** is a liquid **mid-cap** Healthcare name (Biotechnology) at $3.3B, ADV ~1829k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.97 | +0.116 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.51 | +0.051 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.59 | +0.047 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.70 | +0.176 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.94 | +0.189 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.779** | |

### 7. CROX · $5.9B mid · Consumer Cyclical

**1d score +0.740**

**CROX** is a liquid **mid-cap** Consumer Cyclical name (Footwear & Accessories) at $5.9B, ADV ~1040k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.92 | +0.110 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.08 | +0.008 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.59 | +0.047 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.76 | +0.190 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.92 | +0.185 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.740** | |

### 8. TEM · $14.5B large · Healthcare

**1d score +0.539**

**TEM** is a liquid **large-cap** Healthcare name (Health Information Services) at $14.5B, ADV ~6305k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **extreme**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.84 | +0.101 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.51 | +0.051 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.59 | +0.047 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.76 | +0.190 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +1.00 | +0.200 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1d total** | | | **+0.539** | |

### 9. SONO · $1.9B small · Technology

**1d score +0.769**

**SONO** is a liquid **small-cap** Technology name (Consumer Electronics) at $1.9B, ADV ~1976k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.98 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.59 | +0.047 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.76 | +0.153 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.769** | |

### 10. MGNI · $3.7B mid · Communication Services

**1d score +0.717**

**MGNI** is a liquid **mid-cap** Communication Services name (Advertising Agencies) at $3.7B, ADV ~2669k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.32 | +0.032 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.59 | +0.047 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.64 | +0.159 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.80 | +0.161 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.717** | |

### 11. LIND · $1.8B small · Consumer Cyclical

**1d score +0.675**

**LIND** is a liquid **small-cap** Consumer Cyclical name (Travel Services) at $1.8B, ADV ~923k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.91 | +0.109 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.08 | +0.008 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.59 | +0.047 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.55 | +0.139 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.86 | +0.173 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.675** | |

### 12. M · $5.7B mid · Consumer Cyclical

**1d score +0.670**

**M** is a liquid **mid-cap** Consumer Cyclical name (Department Stores) at $5.7B, ADV ~5648k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

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
| **1d total** | | | **+0.670** | |

### 13. CARG · $3.0B mid · Communication Services

**1d score +0.683**

**CARG** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $3.0B, ADV ~1089k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.90 | +0.108 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.32 | +0.032 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.29 | +0.024 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.46 | +0.116 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.88 | +0.175 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.683** | |

### 14. ZD · $2.0B mid · Communication Services

**1d score +0.652**

**ZD** is a liquid **mid-cap** Communication Services name (Advertising Agencies) at $2.0B, ADV ~646k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.97 | +0.116 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.32 | +0.032 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.29 | +0.024 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.64 | +0.159 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.60 | +0.121 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.652** | |

### 15. RUSHA · $5.9B mid · Consumer Cyclical

**1d score +0.618**

**RUSHA** is a liquid **mid-cap** Consumer Cyclical name (Auto & Truck Dealerships) at $5.9B, ADV ~729k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.90 | +0.108 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.08 | +0.008 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.29 | +0.024 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.55 | +0.139 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.70 | +0.140 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.618** | |

### 16. NXDR · $959M small · Communication Services

**1d score +0.690**

**NXDR** is a liquid **small-cap** Communication Services name (Internet Content & Information) at $959M, ADV ~2427k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.62 | +0.075 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.32 | +0.032 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.59 | +0.047 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.70 | +0.176 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.80 | +0.161 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.690** | |

### 17. CYRX · $850M small · Industrials

**1d score +0.598**

**CYRX** is a liquid **small-cap** Industrials name (Integrated Freight & Logistics) at $850M, ADV ~628k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.96 | +0.115 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.16 | +0.016 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.59 | +0.047 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.64 | +0.159 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.45 | +0.091 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.12 | +0.120 | liquid small/mid, room to run |
| **1d total** | | | **+0.598** | |

### 18. COCO · $3.2B mid · Consumer Defensive

**1d score +0.669**

**COCO** is a liquid **mid-cap** Consumer Defensive name (Beverages - Non-Alcoholic) at $3.2B, ADV ~1204k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.42 | +0.050 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.09 | +0.007 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.76 | +0.190 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.85 | +0.171 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.669** | |

### 19. CHEF · $4.5B mid · Consumer Defensive

**1d score +0.599**

**CHEF** is a liquid **mid-cap** Consumer Defensive name (Food Distribution) at $4.5B, ADV ~650k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.94 | +0.113 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.59 | +0.047 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.64 | +0.159 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.40 | +0.080 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.599** | |

### 20. OLLI · $4.5B mid · Consumer Defensive

**1d score +0.651**

**OLLI** is a liquid **mid-cap** Consumer Defensive name (Discount Stores) at $4.5B, ADV ~1947k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.54 | +0.065 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.09 | +0.007 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.58 | +0.116 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.651** | |

### 21. PBI · $2.4B mid · Industrials

**1d score +0.619**

**PBI** is a liquid **mid-cap** Industrials name (Integrated Freight & Logistics) at $2.4B, ADV ~2013k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.97 | +0.116 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.16 | +0.016 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.59 | +0.047 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.55 | +0.139 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.26 | +0.052 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.619** | |

### 22. RXO · $3.3B mid · Industrials

**1d score +0.642**

**RXO** is a liquid **mid-cap** Industrials name (Trucking) at $3.3B, ADV ~1784k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.84 | +0.101 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.16 | +0.016 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.59 | +0.047 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.36 | +0.090 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.55 | +0.110 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.642** | |

### 23. KODK · $954M small · Industrials

**1d score +0.612**

**KODK** is a liquid **small-cap** Industrials name (Specialty Business Services) at $954M, ADV ~894k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.41 | +0.050 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.16 | +0.016 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.59 | +0.047 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.64 | +0.159 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.45 | +0.091 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.612** | |

### 24. FIZZ · $3.0B mid · Consumer Defensive

**1d score +0.494**

**FIZZ** is a liquid **mid-cap** Consumer Defensive name (Beverages - Non-Alcoholic) at $3.0B, ADV ~507k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **miss**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.19 | +0.023 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.29 | +0.024 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.64 | +0.159 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.19 | +0.039 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.494** | |


## 1d AVOID — bottom of the same rank

- **SOC** (small, Energy, $835M) score -0.447. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -5.4%
- **EU** (micro, Energy, $189M) score -0.405. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -10.3%
- **UUUU** (mid, Energy, $3.2B) score -0.397. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -10.3%
- **PUMP** (small, Energy, $1.3B) score -0.396. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -7.7%
- **GS** (mega, Financial, $277.0B) score -0.386. NO BEAR — market=GREEN; red domains=child
- **MS** (mega, Financial, $319.6B) score -0.376. NO BEAR — market=GREEN; red domains=child
- **WFRD** (mid, Energy, $6.0B) score -0.364. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -7.7%
- **BTU** (mid, Energy, $3.3B) score -0.339. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -4.4%
- **URG** (small, Energy, $485M) score -0.325. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -10.3%
- **TTI** (small, Energy, $911M) score -0.314. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -7.7%
- **AR** (large, Energy, $10.9B) score -0.307. NO BEAR — market=GREEN; red domains=parent
- **XPRO** (small, Energy, $1.8B) score -0.302. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -7.7%
- **HP** (mid, Energy, $4.1B) score -0.297. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -5.4%
- **KGS** (mid, Energy, $5.9B) score -0.287. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -7.7%
- **C** (mega, Financial, $222.6B) score -0.287. SELL/AVOID — market=GREEN; red domains=child
- **VALE** (large, Basic Materials, $61.6B) score -0.278. SELL/AVOID — market=GREEN; red domains=parent,child,setup
- **CBOE** (large, Financial, $27.8B) score -0.262. SELL/AVOID — market=GREEN; red domains=setup,flow
- **LDI** (micro, Financial, $244M) score -0.260. NO BEAR — market=GREEN; red domains=setup
- **CRK** (mid, Energy, $3.8B) score -0.258. SELL/AVOID — market=GREEN; red domains=parent,flow
- **CTVA** (large, Basic Materials, $54.4B) score -0.256. SELL/AVOID — market=GREEN; red domains=parent,child
- **WTI** (small, Energy, $592M) score -0.254. SELL/AVOID — market=GREEN; red domains=parent,flow
- **OSG** (micro, Financial, $201M) score -0.253. SELL/AVOID — market=GREEN; red domains=setup,flow
- **BHP** (mega, Basic Materials, $220.0B) score -0.248. SELL/AVOID — market=GREEN; red domains=parent,child
- **JEF** (large, Financial, $11.0B) score -0.246. SELL/AVOID — market=GREEN; red domains=child,setup
- **UEC** (mid, Energy, $5.0B) score -0.238. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -10.3%

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | RBRK | +0.490 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | FIVN | +0.812 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | SDGR | +0.852 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | ILMN | +0.543 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | ZS | +0.655 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | ARQT | +0.835 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 7 | CROX | +0.778 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | TEM | +0.591 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | SONO | +0.814 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 10 | MGNI | +0.766 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 11 | LIND | +0.710 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 12 | M | +0.708 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | CARG | +0.731 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | ZD | +0.705 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 15 | RUSHA | +0.657 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 16 | NXDR | +0.725 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 17 | CYRX | +0.639 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 18 | COCO | +0.692 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 19 | CHEF | +0.633 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 20 | OLLI | +0.679 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 21 | PBI | +0.660 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 22 | RXO | +0.675 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 23 | KODK | +0.632 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 24 | FIZZ | +0.503 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | RBRK | +0.512 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | FIVN | +0.814 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | SDGR | +0.858 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | ILMN | +0.551 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | ZS | +0.676 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | ARQT | +0.841 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 7 | CROX | +0.776 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | TEM | +0.595 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | SONO | +0.817 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 10 | MGNI | +0.768 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 11 | LIND | +0.704 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 12 | M | +0.705 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | CARG | +0.746 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | ZD | +0.724 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 15 | RUSHA | +0.669 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 16 | NXDR | +0.721 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 17 | CYRX | +0.637 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 18 | COCO | +0.710 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 19 | CHEF | +0.627 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 20 | OLLI | +0.702 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 21 | PBI | +0.656 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 22 | RXO | +0.665 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 23 | KODK | +0.618 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 24 | FIZZ | +0.501 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | RBRK | +0.518 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | FIVN | +0.803 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | SDGR | +0.851 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | ILMN | +0.544 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | ZS | +0.682 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | ARQT | +0.835 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 7 | CROX | +0.760 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | TEM | +0.587 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | SONO | +0.805 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 10 | MGNI | +0.759 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 11 | LIND | +0.688 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 12 | M | +0.690 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | CARG | +0.753 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | ZD | +0.732 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 15 | RUSHA | +0.671 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 16 | NXDR | +0.704 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 17 | CYRX | +0.624 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 18 | COCO | +0.713 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 19 | CHEF | +0.611 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 20 | OLLI | +0.707 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 21 | PBI | +0.644 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 22 | RXO | +0.650 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 23 | KODK | +0.594 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 24 | FIZZ | +0.487 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 1m BUY — why these names

### 1. RBRK · $22.4B large · Technology

**1m score +0.558**

**RBRK** is a liquid **large-cap** Technology name (Software - Infrastructure) at $22.4B, ADV ~3387k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

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

### 2. FIVN · $2.6B mid · Technology

**1m score +0.841**

**FIVN** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $2.6B, ADV ~2583k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

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

### 3. SDGR · $2.3B mid · Healthcare

**1m score +0.893**

**SDGR** is a liquid **mid-cap** Healthcare name (Health Information Services) at $2.3B, ADV ~1237k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extreme**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

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

### 4. ILMN · $37.0B large · Healthcare

**1m score +0.587**

**ILMN** is a liquid **large-cap** Healthcare name (Diagnostics & Research) at $37.0B, ADV ~1986k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

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

### 5. ZS · $32.2B large · Technology

**1m score +0.720**

**ZS** is a liquid **large-cap** Technology name (Software - Infrastructure) at $32.2B, ADV ~3006k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.97 | +0.213 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.23 | -0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.87 | +0.173 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.07 | +0.070 | liquid small/mid, room to run |
| **1m total** | | | **+0.720** | |

### 6. ARQT · $3.3B mid · Healthcare

**1m score +0.876**

**ARQT** is a liquid **mid-cap** Healthcare name (Biotechnology) at $3.3B, ADV ~1829k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.97 | +0.213 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.51 | +0.101 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.47 | -0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.211 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.94 | +0.189 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.876** | |

### 7. CROX · $5.9B mid · Consumer Cyclical

**1m score +0.794**

**CROX** is a liquid **mid-cap** Consumer Cyclical name (Footwear & Accessories) at $5.9B, ADV ~1040k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.92 | +0.202 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.08 | +0.016 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.47 | -0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.92 | +0.185 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.794** | |

### 8. TEM · $14.5B large · Healthcare

**1m score +0.627**

**TEM** is a liquid **large-cap** Healthcare name (Health Information Services) at $14.5B, ADV ~6305k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **extreme**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.84 | +0.185 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.51 | +0.101 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.47 | -0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +1.00 | +0.200 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1m total** | | | **+0.627** | |

### 9. SONO · $1.9B small · Technology

**1m score +0.843**

**SONO** is a liquid **small-cap** Technology name (Consumer Electronics) at $1.9B, ADV ~1976k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.98 | +0.216 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.47 | -0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.76 | +0.153 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.843** | |

### 10. MGNI · $3.7B mid · Communication Services

**1m score +0.795**

**MGNI** is a liquid **mid-cap** Communication Services name (Advertising Agencies) at $3.7B, ADV ~2669k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.32 | +0.064 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.47 | -0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.64 | +0.191 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.80 | +0.161 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.795** | |

### 11. LIND · $1.8B small · Consumer Cyclical

**1m score +0.717**

**LIND** is a liquid **small-cap** Consumer Cyclical name (Travel Services) at $1.8B, ADV ~923k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.91 | +0.199 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.08 | +0.016 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.47 | -0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.55 | +0.166 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.86 | +0.173 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.717** | |

### 12. M · $5.7B mid · Consumer Cyclical

**1m score +0.723**

**M** is a liquid **mid-cap** Consumer Cyclical name (Department Stores) at $5.7B, ADV ~5648k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

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
| **1m total** | | | **+0.723** | |

### 13. CARG · $3.0B mid · Communication Services

**1m score +0.786**

**CARG** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $3.0B, ADV ~1089k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.90 | +0.198 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.32 | +0.064 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.23 | -0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.46 | +0.139 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.88 | +0.175 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.786** | |

### 14. ZD · $2.0B mid · Communication Services

**1m score +0.770**

**ZD** is a liquid **mid-cap** Communication Services name (Advertising Agencies) at $2.0B, ADV ~646k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.97 | +0.213 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.32 | +0.064 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.23 | -0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.64 | +0.191 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.60 | +0.121 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.770** | |

### 15. RUSHA · $5.9B mid · Consumer Cyclical

**1m score +0.701**

**RUSHA** is a liquid **mid-cap** Consumer Cyclical name (Auto & Truck Dealerships) at $5.9B, ADV ~729k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.90 | +0.197 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.08 | +0.016 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.23 | -0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.55 | +0.166 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.70 | +0.140 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.701** | |

### 16. NXDR · $959M small · Communication Services

**1m score +0.735**

**NXDR** is a liquid **small-cap** Communication Services name (Internet Content & Information) at $959M, ADV ~2427k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.62 | +0.137 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.32 | +0.064 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.47 | -0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.211 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.80 | +0.161 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.735** | |

### 17. CYRX · $850M small · Industrials

**1m score +0.657**

**CYRX** is a liquid **small-cap** Industrials name (Integrated Freight & Logistics) at $850M, ADV ~628k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.96 | +0.211 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.16 | +0.032 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.47 | -0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.64 | +0.191 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.45 | +0.091 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.12 | +0.120 | liquid small/mid, room to run |
| **1m total** | | | **+0.657** | |

### 18. COCO · $3.2B mid · Consumer Defensive

**1m score +0.736**

**COCO** is a liquid **mid-cap** Consumer Defensive name (Beverages - Non-Alcoholic) at $3.2B, ADV ~1204k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.42 | +0.092 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.001 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.85 | +0.171 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.736** | |

### 19. CHEF · $4.5B mid · Consumer Defensive

**1m score +0.641**

**CHEF** is a liquid **mid-cap** Consumer Defensive name (Food Distribution) at $4.5B, ADV ~650k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.94 | +0.207 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.001 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.47 | -0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.64 | +0.191 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.40 | +0.080 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.641** | |

### 20. OLLI · $4.5B mid · Consumer Defensive

**1m score +0.735**

**OLLI** is a liquid **mid-cap** Consumer Defensive name (Discount Stores) at $4.5B, ADV ~1947k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.54 | +0.119 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.001 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.58 | +0.116 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.735** | |

### 21. PBI · $2.4B mid · Industrials

**1m score +0.675**

**PBI** is a liquid **mid-cap** Industrials name (Integrated Freight & Logistics) at $2.4B, ADV ~2013k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.97 | +0.213 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.16 | +0.032 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.47 | -0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.55 | +0.166 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.26 | +0.052 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.675** | |

### 22. RXO · $3.3B mid · Industrials

**1m score +0.675**

**RXO** is a liquid **mid-cap** Industrials name (Trucking) at $3.3B, ADV ~1784k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.84 | +0.184 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.16 | +0.032 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.47 | -0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.36 | +0.108 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.55 | +0.110 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.675** | |

### 23. KODK · $954M small · Industrials

**1m score +0.617**

**KODK** is a liquid **small-cap** Industrials name (Specialty Business Services) at $954M, ADV ~894k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.41 | +0.091 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.16 | +0.032 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.47 | -0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.64 | +0.191 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.45 | +0.091 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.617** | |

### 24. FIZZ · $3.0B mid · Consumer Defensive

**1m score +0.503**

**FIZZ** is a liquid **mid-cap** Consumer Defensive name (Beverages - Non-Alcoholic) at $3.0B, ADV ~507k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **miss**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.19 | +0.042 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.001 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.23 | -0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.64 | +0.191 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.19 | +0.039 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.503** | |


## 1m AVOID — bottom of the same rank

- **EU** (micro, Energy, $189M) score -0.638. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **UUUU** (mid, Energy, $3.2B) score -0.623. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SOC** (small, Energy, $835M) score -0.615. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PUMP** (small, Energy, $1.3B) score -0.599. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **WFRD** (mid, Energy, $6.0B) score -0.558. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GS** (mega, Financial, $277.0B) score -0.528. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **URG** (small, Energy, $485M) score -0.527. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **MS** (mega, Financial, $319.6B) score -0.512. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BTU** (mid, Energy, $3.3B) score -0.498. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TTI** (small, Energy, $911M) score -0.493. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **XPRO** (small, Energy, $1.8B) score -0.491. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LDI** (micro, Financial, $244M) score -0.484. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **UEC** (mid, Energy, $5.0B) score -0.467. this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **KGS** (mid, Energy, $5.9B) score -0.466. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **JEF** (large, Financial, $11.0B) score -0.446. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AR** (large, Energy, $10.9B) score -0.441. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **HP** (mid, Energy, $4.1B) score -0.433. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OSG** (micro, Financial, $201M) score -0.433. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LODE** (micro, Basic Materials, $200M) score -0.432. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **C** (mega, Financial, $222.6B) score -0.418. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **VALE** (large, Basic Materials, $61.6B) score -0.418. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **CRC** (mid, Energy, $4.8B) score -0.417. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AROC** (mid, Energy, $5.5B) score -0.407. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **HAL** (large, Energy, $28.4B) score -0.399. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CBOE** (large, Financial, $27.8B) score -0.396. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-09-18_stock_book.md`
- Machine table: `data/stock_book/2026-09-18_stock_book.csv`
- Machine book: `data/stock_book/2026-09-18_stock_book.json`
- Join rank: `data/join/2026-09-18_ranked.csv`
- Weather: `01_daily/weather/2026-09-18_weather.md`
- AB enrich: `data/ab_checklist/2026-09-18_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-09-18_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-09-18_map_heat.md`
