# Stock book — 2026-09-17

_Generated 2026-09-17T23:52:58.664421-04:00_

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
- General predict (same-day): +0.68 up (present)
- Stand-down: **no** — 359 names qualified through standard,group_leader,catalyst (54 probable)
- Sector predicts this date: 11/11 (ok)
- News tickers in play: 90
- AB coverage: 1993 names · peer RS: 1869
- Universe after liquidity: 2096
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 45

## All-green BUY / SELL

- Mode: **green_pile** · SELL **core_weights_ex_green**
- Pile: **263** liquid all-green names (need ≥ 8) of 2096
- Core fired: join=yes, AB=yes, peer=yes
- pile 263 ≥ 8 liquid all-green names — BUY 15 from the pile by green_rank (no opp); SELL is core weights on the non-green remainder

## Decision lattice — gate → route → rank

The weighted score is now a tie-breaker inside an eligible lane. It cannot average away a market, group, company, or setup veto.

### MARKET: 🟢 GREEN

- GREEN: general up score=+7.38; good=+0.8 vs bad=-3.0; risk=on; red pillars=1
- Allowed long lanes: **standard, group_leader, catalyst** · max slots 15 · size ×1.00
- Bull evidence: oil / dollar +0.50 points; futures +0.25 points
- Bear evidence: rates / Fed -3.00 points

Decision domains: **MKT · parent · child · company · setup · flow**. Measured parent/child tape is kept separate from the LLM essay; direct company events must be price-confirmed on a hard-red day.

### Bull decisions (eligible or closest blocked cases)

| # | Ticker | Domains | Lane | Company / group | Decision |
|---:|--------|---------|------|-----------------|----------|
| 1 | **AMGN** | 🟢🟢🟡🟢🟢🟡 | catalyst | direct high digest (same-day): Amgen gets FDA approval to update IMDELLTRA label to reduce monitoring for first two ES-SCLC doses; Drug Manufacturers - General +1.4% d1 / -3.1% 1w / -0.7% vs parent | BUY CATALYST — market=GREEN; parent=GREEN; child=YELLOW/rel=YELLOW; company=GREEN(0.72); setup=GREEN; flow=YELLOW; lookback=🔵,Cond green |
| 2 | **GEV** | 🟢🔴🔴🟢🟡🟢 | catalyst | direct high digest (same-day): GE Vernova settles over $300 million Vineyard Wind payment dispute, withdraws contract-termination notice and legal claims on Massachusetts offshore project; Specialty Industrial Machinery -4.2% d1 / -4.2% 1w / -1.5% vs parent | BUY CATALYST — market=GREEN; parent=RED; child=RED/rel=YELLOW; company=GREEN(0.72); setup=YELLOW; flow=GREEN; lookback=🔵,Cond green |
| 3 | **CMI** | 🟢🔴🔴🟢🔴🔴 | blocked | direct high digest (same-day): Cummins wins largest-ever BESS contract to supply storage systems for major U.S. data center project; Specialty Industrial Machinery -4.2% d1 / -4.2% 1w / -1.5% vs parent | BLOCK BUY — parent sector RED; child industry/theme RED; setup RED; flow RED; v2 domain region red / market=GREEN; parent=RED; child=RED/rel=YELLOW; company=GREEN(0.72); setup=RED; flow=RED |
| 4 | **OPCH** | 🟢🟢🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 5 | **PLTR** | 🟢🔴🟢🟡🟢🟢 | group_leader | direct normal digest (same-day): UBS raises Palantir price target to $250 from $220, reiterates Buy rating on robust AI software demand; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.42); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 6 | **NBIS** | 🟢🔴🟢🟡🟢🟢 | group_leader | direct normal digest (same-day): Nebius raises NVIDIA GPU prices on strong AI demand, surging 8.6% premarket; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.42); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 7 | **AMN** | 🟢🟢🟢🟡🟢🟡 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,⚪,Cond green |
| 8 | **ACHC** | 🟢🟢🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 9 | **CRWD** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 10 | **SABR** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 11 | **SONO** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Consumer Electronics +0.3% d1 / +4.0% 1w / +6.0% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 12 | **TASK** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Information Technology Services +2.9% d1 / +1.9% 1w / +3.9% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 13 | **NTNX** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 14 | **APPN** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 15 | **OKTA** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **FLNC** | 🟢🔴🔴🟡🔴🔴 | Utilities - Renewable | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -4.1% |
| 2 | **NRG** | 🟢🔴🔴🟡🔴🔴 | Utilities - Independent Power Producers | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -6.3% |
| 3 | **SOC** | 🟢🟢🔴🟡🔴🔴 | Oil & Gas Drilling | SELL/AVOID — market=GREEN; red domains=child,setup,flow; child lags parent -5.4% |
| 4 | **METC** | 🟢🔴🔴🟡🔴🔴 | Coking Coal | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -5.7% |
| 5 | **TTI** | 🟢🟢🔴🟡🔴🔴 | Oil & Gas Equipment & Services | SELL/AVOID — market=GREEN; red domains=child,setup,flow; child lags parent -7.7% |
| 6 | **SGML** | 🟢🔴🔴🟡🔴🔴 | Other Industrial Metals & Mining | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow |
| 7 | **LTBR** | 🟢🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -4.7% |
| 8 | **TLN** | 🟢🔴🔴🟡🔴🟡 | Utilities - Independent Power Producers | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -6.3% |
| 9 | **HAL** | 🟢🟢🔴🟡🔴🔴 | Oil & Gas Equipment & Services | SELL/AVOID — market=GREEN; red domains=child,setup,flow; child lags parent -7.7% |
| 10 | **HDSN** | 🟢🔴🔴🟡🔴🔴 | Specialty Chemicals | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow |
| 11 | **HE** | 🟢🔴🔴🟡🔴🔴 | Utilities - Regulated Electric | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow |
| 12 | **JELD** | 🟢🔴🔴🟡🔴🔴 | Building Products & Equipment | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow |
| 13 | **FMC** | 🟢🔴🔴🟡🔴🔴 | Agricultural Inputs | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow |
| 14 | **FIP** | 🟢🔴🔴🟡🔴🔴 | Conglomerates | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow |
| 15 | **DGXX** | 🟢🔴🔴🟡🔴🟢 | Utilities - Independent Power Producers | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -6.3% |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **captain_research** (203 captains, 5 industries → s_heat).
- Board file: `01_daily/map_heat/2026-09-17_map_heat.json` · generated 2026-09-17T04:10:17.053880-04:00

### Sector RS vs same-day LLM essay

| Sector | Finviz 1d | Finviz 1w | LLM 1d | Tape vs essay |
|--------|----------:|----------:|-------:|---------------|
| Basic Materials | -2.1% | -4.8% | +0.00 | essay flat, tape moving |
| Communication Services | +2.7% | +3.5% | +0.33 |  |
| Consumer Cyclical | -0.4% | -2.0% | +0.00 | essay flat, tape moving |
| Consumer Defensive | +1.4% | +0.5% | +0.37 |  |
| Energy | -0.8% | +1.0% | +0.35 |  |
| Financial | -0.4% | -1.8% | +0.00 | essay flat, tape moving |
| Healthcare | +1.4% | -2.5% | +0.50 | essay UP, tape DOWN |
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
| Healthcare | +0.50 |
| Consumer Defensive | +0.37 |
| Energy | +0.35 |
| Communication Services | +0.33 |
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
| general | 52% | 33 | ×0.85 |
| sector:Basic Materials | 50% | 22 | ×0.85 |
| sector:Communication Services | 24% | 21 | ×0.50 |
| sector:Consumer Cyclical | 55% | 22 | ×0.85 |
| sector:Consumer Defensive | 46% | 22 | ×0.85 |
| sector:Energy | 50% | 22 | ×0.85 |
| sector:Financial | 46% | 22 | ×0.85 |
| sector:Healthcare | 53% | 19 | ×0.85 |
| sector:Industrials | 36% | 22 | ×0.50 |
| sector:Real Estate | 50% | 22 | ×0.85 |
| sector:Technology | 40% | 20 | ×0.50 |
| sector:Utilities | 40% | 20 | ×0.50 |

## Horizon weights — book_policy.json v15

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.12 | 0.10 | 0.08 | 0.25 | 0.25 | 0.20 | additive |
| 3d | 0.16 | 0.14 | 0.08 | 0.16 | 0.26 | 0.20 | additive |
| 1w | 0.18 | 0.16 | 0.08 | 0.10 | 0.28 | 0.20 | additive |
| 2w | 0.20 | 0.18 | 0.08 | 0.06 | 0.28 | 0.20 | additive |
| 1m | 0.22 | 0.20 | 0.08 | 0.00 | 0.30 | 0.20 | additive |

## 1d BUY — why these names

### 1. ILMN · $37.0B large · Healthcare

**1d score +0.591**

**ILMN** is a liquid **large-cap** Healthcare name (Diagnostics & Research) at $37.0B, ADV ~1986k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.98 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.50 | +0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.68 | +0.054 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.97 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.93 | +0.186 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.11 | -0.110 | liquid small/mid, room to run |
| **1d total** | | | **+0.591** | |

### 2. IOVA · $4.5B mid · Healthcare

**1d score +0.817**

**IOVA** is a liquid **mid-cap** Healthcare name (Biotechnology) at $4.5B, ADV ~16505k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extreme**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.96 | +0.115 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.50 | +0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.34 | +0.027 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.91 | +0.226 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.99 | +0.198 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.817** | |

### 3. PGEN · $2.8B mid · Healthcare

**1d score +0.860**

**PGEN** is a liquid **mid-cap** Healthcare name (Biotechnology) at $2.8B, ADV ~4934k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.98 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.50 | +0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.34 | +0.027 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.88 | +0.220 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.97 | +0.194 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.860** | |

### 4. FIVN · $2.6B mid · Technology

**1d score +0.806**

**FIVN** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $2.6B, ADV ~2583k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.98 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.68 | +0.054 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.94 | +0.235 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.89 | +0.178 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.806** | |

### 5. ATRC · $3.0B mid · Healthcare

**1d score +0.846**

**ATRC** is a liquid **mid-cap** Healthcare name (Medical Instruments & Supplies) at $3.0B, ADV ~968k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.96 | +0.115 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.50 | +0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.34 | +0.027 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.93 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.86 | +0.172 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.846** | |

### 6. ASAN · $2.3B mid · Technology

**1d score +0.805**

**ASAN** is a liquid **mid-cap** Technology name (Software - Application) at $2.3B, ADV ~5755k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.91 | +0.109 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.34 | +0.027 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.93 | +0.187 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.805** | |

### 7. MGNI · $3.7B mid · Communication Services

**1d score +0.877**

**MGNI** is a liquid **mid-cap** Communication Services name (Advertising Agencies) at $3.7B, ADV ~2669k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.33 | +0.033 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.68 | +0.054 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.80 | +0.161 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.05 | +0.049 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.877** | |

### 8. CRWD · $251.6B mega · Technology

**1d score +0.265**

**CRWD** is a liquid **mega-cap** Technology name (Software - Infrastructure) at $251.6B, ADV ~10127k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.97 | +0.116 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.34 | +0.027 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.97 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.69 | +0.139 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.28 | -0.280 | liquid small/mid, room to run |
| **1d total** | | | **+0.265** | |

### 9. BBY · $19.8B large · Consumer Cyclical

**1d score +0.373**

**BBY** is a liquid **large-cap** Consumer Cyclical name (Specialty Retail) at $19.8B, ADV ~3809k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.95 | +0.114 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.34 | +0.027 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.93 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.75 | +0.151 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.373** | |

### 10. SONO · $1.9B small · Technology

**1d score +0.747**

**SONO** is a liquid **small-cap** Technology name (Consumer Electronics) at $1.9B, ADV ~1976k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.98 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.68 | +0.054 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.76 | +0.153 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.747** | |

### 11. CROX · $5.9B mid · Consumer Cyclical

**1d score +0.789**

**CROX** is a liquid **mid-cap** Consumer Cyclical name (Footwear & Accessories) at $5.9B, ADV ~1040k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.74 | +0.089 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.68 | +0.054 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.76 | +0.190 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.92 | +0.185 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.789** | |

### 12. ZD · $2.0B mid · Communication Services

**1d score +0.702**

**ZD** is a liquid **mid-cap** Communication Services name (Advertising Agencies) at $2.0B, ADV ~646k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.92 | +0.110 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.33 | +0.033 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.34 | +0.027 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.60 | +0.121 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.702** | |

### 13. GME · $11.5B large · Consumer Cyclical

**1d score +0.579**

**GME** is a liquid **large-cap** Consumer Cyclical name (Specialty Retail) at $11.5B, ADV ~6045k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **extended**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.87 | +0.104 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.68 | +0.054 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.55 | +0.139 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.91 | +0.182 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1d total** | | | **+0.579** | |

### 14. CARG · $3.0B mid · Communication Services

**1d score +0.722**

**CARG** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $3.0B, ADV ~1089k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.69 | +0.083 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.33 | +0.033 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.34 | +0.027 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.70 | +0.176 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.88 | +0.175 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.722** | |

### 15. M · $5.7B mid · Consumer Cyclical

**1d score +0.752**

**M** is a liquid **mid-cap** Consumer Cyclical name (Department Stores) at $5.7B, ADV ~5648k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.73 | +0.087 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.68 | +0.054 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.91 | +0.226 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.58 | +0.115 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.752** | |

### 16. CHEF · $4.5B mid · Consumer Defensive

**1d score +0.702**

**CHEF** is a liquid **mid-cap** Consumer Defensive name (Food Distribution) at $4.5B, ADV ~650k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.83 | +0.099 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.17 | +0.017 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.68 | +0.054 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.40 | +0.080 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.702** | |

### 17. NXDR · $959M small · Communication Services

**1d score +0.584**

**NXDR** is a liquid **small-cap** Communication Services name (Internet Content & Information) at $959M, ADV ~2427k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.18 | +0.021 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.33 | +0.033 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.68 | +0.054 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.46 | +0.116 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.80 | +0.161 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.584** | |


## 1d AVOID — bottom of the same rank

- **TLN** (large, Utilities, $14.1B) score -0.291. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -6.3%
- **OSG** (micro, Financial, $201M) score -0.287. SELL/AVOID — market=GREEN; red domains=setup,flow
- **CDZI** (micro, Industrials, $285M) score -0.272. SELL/AVOID — market=GREEN; red domains=parent,setup,flow
- **EQ** (micro, Healthcare, $120M) score -0.271. NO BEAR — market=GREEN; red domains=setup
- **DVLT** (micro, Technology, $157M) score -0.256. SELL/AVOID — market=GREEN; red domains=parent,setup,flow
- **MIST** (micro, Healthcare, $114M) score -0.248. NO BEAR — market=GREEN; red domains=setup
- **LDI** (micro, Financial, $244M) score -0.248. NO BEAR — market=GREEN; red domains=setup
- **SEER** (micro, Healthcare, $106M) score -0.241. NO BEAR — market=GREEN; red domains=setup
- **LODE** (micro, Basic Materials, $200M) score -0.239. SELL/AVOID — market=GREEN; red domains=parent,child,setup
- **GETY** (micro, Communication Services, $97M) score -0.235. NO BEAR — market=GREEN; red domains=setup
- **NUS** (micro, Consumer Defensive, $224M) score -0.232. NO BEAR — market=GREEN; red domains=setup
- **NAUT** (micro, Healthcare, $124M) score -0.219. NO BEAR — market=GREEN; red domains=setup
- **JKS** (small, Technology, $541M) score -0.214. SELL/AVOID — market=GREEN; red domains=parent,child,setup
- **CMTG** (micro, Real Estate, $206M) score -0.214. SELL/AVOID — market=GREEN; red domains=parent,child,setup
- **BGS** (micro, Consumer Defensive, $247M) score -0.211. SELL/AVOID — market=GREEN; red domains=setup,flow; child lags parent -3.5%
- **BTU** (mid, Energy, $3.3B) score -0.198. SELL/AVOID — market=GREEN; red domains=child,setup; child lags parent -4.4%
- **ROIV** (large, Healthcare, $28.9B) score -0.195. NO BEAR — market=GREEN; red domains=setup
- **EU** (micro, Energy, $189M) score -0.195. SELL/AVOID — market=GREEN; red domains=child,setup; child lags parent -10.3%
- **STLA** (large, Consumer Cyclical, $14.6B) score -0.194. NO BEAR — market=GREEN; red domains=setup
- **ELDN** (micro, Healthcare, $215M) score -0.193. SELL/AVOID — market=GREEN; red domains=setup,flow
- **NAK** (small, Basic Materials, $714M) score -0.190. SELL/AVOID — market=GREEN; red domains=parent,child,setup
- **IVVD** (micro, Healthcare, $220M) score -0.187. SELL/AVOID — market=GREEN; red domains=setup,flow
- **SLQT** (micro, Financial, $83M) score -0.185. SELL/AVOID — market=GREEN; red domains=setup,flow
- **FMC** (small, Basic Materials, $1.3B) score -0.184. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow
- **ORGO** (micro, Healthcare, $175M) score -0.184. SELL/AVOID — market=GREEN; red domains=setup,flow

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | ILMN | +0.606 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | IOVA | +0.858 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 3 | PGEN | +0.901 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 4 | FIVN | +0.808 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | ATRC | +0.887 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 6 | ASAN | +0.831 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | MGNI | +0.884 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 8 | CRWD | +0.294 | mega | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | BBY | +0.401 | large | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | SONO | +0.749 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 11 | CROX | +0.780 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | ZD | +0.733 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 13 | GME | +0.573 | large | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | CARG | +0.743 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | M | +0.745 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 16 | CHEF | +0.696 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 17 | NXDR | +0.554 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | ILMN | +0.620 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | IOVA | +0.887 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 3 | PGEN | +0.931 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 4 | FIVN | +0.816 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | ATRC | +0.917 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 6 | ASAN | +0.852 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | MGNI | +0.892 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 8 | CRWD | +0.319 | mega | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | BBY | +0.425 | large | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | SONO | +0.753 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 11 | CROX | +0.779 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | ZD | +0.757 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 13 | GME | +0.570 | large | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | CARG | +0.759 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | M | +0.746 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 16 | CHEF | +0.696 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 17 | NXDR | +0.538 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | ILMN | +0.651 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | IOVA | +0.917 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 3 | PGEN | +0.961 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 4 | FIVN | +0.841 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | ATRC | +0.947 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 6 | ASAN | +0.875 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | MGNI | +0.919 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 8 | CRWD | +0.343 | mega | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | BBY | +0.449 | large | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | SONO | +0.778 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 11 | CROX | +0.799 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | ZD | +0.783 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 13 | GME | +0.593 | large | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | CARG | +0.780 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | M | +0.766 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 16 | CHEF | +0.718 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 17 | NXDR | +0.550 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |

## 1m BUY — why these names

### 1. ILMN · $37.0B large · Healthcare

**1m score +0.702**

**ILMN** is a liquid **large-cap** Healthcare name (Diagnostics & Research) at $37.0B, ADV ~1986k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.98 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.50 | +0.101 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.41 | -0.033 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.97 | +0.292 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.93 | +0.186 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.11 | -0.110 | liquid small/mid, room to run |
| **1m total** | | | **+0.702** | |

### 2. IOVA · $4.5B mid · Healthcare

**1m score +0.965**

**IOVA** is a liquid **mid-cap** Healthcare name (Biotechnology) at $4.5B, ADV ~16505k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extreme**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.96 | +0.211 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.50 | +0.101 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.20 | -0.016 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.99 | +0.198 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.965** | |

### 3. PGEN · $2.8B mid · Healthcare

**1m score +1.009**

**PGEN** is a liquid **mid-cap** Healthcare name (Biotechnology) at $2.8B, ADV ~4934k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.98 | +0.216 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.50 | +0.101 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.20 | -0.016 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.97 | +0.194 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.009** | |

### 4. FIVN · $2.6B mid · Technology

**1m score +0.885**

**FIVN** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $2.6B, ADV ~2583k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.98 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.41 | -0.033 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.94 | +0.282 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.89 | +0.178 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.885** | |

### 5. ATRC · $3.0B mid · Healthcare

**1m score +0.995**

**ATRC** is a liquid **mid-cap** Healthcare name (Medical Instruments & Supplies) at $3.0B, ADV ~968k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.96 | +0.212 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.50 | +0.101 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.20 | -0.016 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.93 | +0.278 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.86 | +0.172 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.995** | |

### 6. ASAN · $2.3B mid · Technology

**1m score +0.915**

**ASAN** is a liquid **mid-cap** Technology name (Software - Application) at $2.3B, ADV ~5755k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.91 | +0.200 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.20 | -0.016 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.93 | +0.187 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.915** | |

### 7. MGNI · $3.7B mid · Communication Services

**1m score +0.964**

**MGNI** is a liquid **mid-cap** Communication Services name (Advertising Agencies) at $3.7B, ADV ~2669k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.33 | +0.065 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.41 | -0.033 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.80 | +0.161 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.05 | +0.049 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.964** | |

### 8. CRWD · $251.6B mega · Technology

**1m score +0.387**

**CRWD** is a liquid **mega-cap** Technology name (Software - Infrastructure) at $251.6B, ADV ~10127k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.97 | +0.213 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.20 | -0.016 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.97 | +0.292 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.69 | +0.139 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.28 | -0.280 | liquid small/mid, room to run |
| **1m total** | | | **+0.387** | |

### 9. BBY · $19.8B large · Consumer Cyclical

**1m score +0.491**

**BBY** is a liquid **large-cap** Consumer Cyclical name (Specialty Retail) at $19.8B, ADV ~3809k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.95 | +0.209 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.20 | -0.016 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.93 | +0.278 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.75 | +0.151 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1m total** | | | **+0.491** | |

### 10. SONO · $1.9B small · Technology

**1m score +0.819**

**SONO** is a liquid **small-cap** Technology name (Consumer Electronics) at $1.9B, ADV ~1976k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.98 | +0.216 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.41 | -0.033 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.76 | +0.153 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.819** | |

### 11. CROX · $5.9B mid · Consumer Cyclical

**1m score +0.834**

**CROX** is a liquid **mid-cap** Consumer Cyclical name (Footwear & Accessories) at $5.9B, ADV ~1040k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.74 | +0.164 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.41 | -0.033 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.92 | +0.185 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.834** | |

### 12. ZD · $2.0B mid · Communication Services

**1m score +0.825**

**ZD** is a liquid **mid-cap** Communication Services name (Advertising Agencies) at $2.0B, ADV ~646k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.92 | +0.201 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.33 | +0.065 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.20 | -0.016 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.60 | +0.121 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.825** | |

### 13. GME · $11.5B large · Consumer Cyclical

**1m score +0.627**

**GME** is a liquid **large-cap** Consumer Cyclical name (Specialty Retail) at $11.5B, ADV ~6045k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **extended**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.87 | +0.191 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.41 | -0.033 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.55 | +0.166 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.91 | +0.182 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1m total** | | | **+0.627** | |

### 14. CARG · $3.0B mid · Communication Services

**1m score +0.815**

**CARG** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $3.0B, ADV ~1089k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.69 | +0.152 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.33 | +0.065 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.20 | -0.016 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.211 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.88 | +0.175 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.815** | |

### 15. M · $5.7B mid · Consumer Cyclical

**1m score +0.804**

**M** is a liquid **mid-cap** Consumer Cyclical name (Department Stores) at $5.7B, ADV ~5648k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.73 | +0.160 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.41 | -0.033 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.58 | +0.115 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.804** | |

### 16. CHEF · $4.5B mid · Consumer Defensive

**1m score +0.755**

**CHEF** is a liquid **mid-cap** Consumer Defensive name (Food Distribution) at $4.5B, ADV ~650k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.83 | +0.182 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.17 | +0.033 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.41 | -0.033 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.40 | +0.080 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.755** | |

### 17. NXDR · $959M small · Communication Services

**1m score +0.570**

**NXDR** is a liquid **small-cap** Communication Services name (Internet Content & Information) at $959M, ADV ~2427k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.18 | +0.039 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.33 | +0.065 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.41 | -0.033 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.46 | +0.139 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.80 | +0.161 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.570** | |


## 1m AVOID — bottom of the same rank

- **TLN** (large, Utilities, $14.1B) score -0.525. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CDZI** (micro, Industrials, $285M) score -0.480. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OSG** (micro, Financial, $201M) score -0.465. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LDI** (micro, Financial, $244M) score -0.456. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LODE** (micro, Basic Materials, $200M) score -0.434. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **EU** (micro, Energy, $189M) score -0.403. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CMTG** (micro, Real Estate, $206M) score -0.400. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GETY** (micro, Communication Services, $97M) score -0.398. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **SUPV** (small, Financial, $638M) score -0.397. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **EQ** (micro, Healthcare, $120M) score -0.394. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NUS** (micro, Consumer Defensive, $224M) score -0.393. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide)
- **FLNC** (small, Utilities, $1.4B) score -0.369. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SEER** (micro, Healthcare, $106M) score -0.365. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **ABAT** (small, Industrials, $319M) score -0.360. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **SLQT** (micro, Financial, $83M) score -0.360. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **MIST** (micro, Healthcare, $114M) score -0.358. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **STLA** (large, Consumer Cyclical, $14.6B) score -0.358. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **LPL** (mid, Technology, $3.0B) score -0.349. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LU** (small, Financial, $989M) score -0.343. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BTU** (mid, Energy, $3.3B) score -0.340. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NAK** (small, Basic Materials, $714M) score -0.333. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BHR** (micro, Real Estate, $122M) score -0.328. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **JKS** (small, Technology, $541M) score -0.326. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **DVLT** (micro, Technology, $157M) score -0.321. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **BGS** (micro, Consumer Defensive, $247M) score -0.320. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-09-17_stock_book.md`
- Machine table: `data/stock_book/2026-09-17_stock_book.csv`
- Machine book: `data/stock_book/2026-09-17_stock_book.json`
- Join rank: `data/join/2026-09-17_ranked.csv`
- Weather: `01_daily/weather/2026-09-17_weather.md`
- AB enrich: `data/ab_checklist/2026-09-17_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-09-17_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-09-17_map_heat.md`
