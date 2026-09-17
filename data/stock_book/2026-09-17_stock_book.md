# Stock book — 2026-09-17

_Generated 2026-09-17T06:50:31.999494-04:00_

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
- Stand-down: **no** — 415 names qualified through standard,group_leader,catalyst (47 probable)
- Sector predicts this date: 11/11 (ok)
- News tickers in play: 90
- AB coverage: 1966 names · peer RS: 1850
- Universe after liquidity: 2077
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 45

## All-green BUY / SELL

- Mode: **green_pile** · SELL **core_weights_ex_green**
- Pile: **181** liquid all-green names (need ≥ 8) of 2077
- Core fired: join=yes, AB=yes, peer=yes
- pile 181 ≥ 8 liquid all-green names — BUY 15 from the pile by green_rank (no opp); SELL is core weights on the non-green remainder

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
| 1 | **GEV** | 🟢🔴🔴🟢🟡🟢 | catalyst | direct high digest (same-day): GE Vernova settles over $300 million Vineyard Wind payment dispute, withdraws contract-termination notice and legal claims on Massachusetts offshore project; Specialty Industrial Machinery -4.2% d1 / -4.2% 1w / -1.5% vs parent | BUY CATALYST — market=GREEN; parent=RED; child=RED/rel=YELLOW; company=GREEN(0.72); setup=YELLOW; flow=GREEN; lookback=🔵,Cond green |
| 2 | **OPCH** | 🟢🟢🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 3 | **PLTR** | 🟢🔴🟢🟡🟢🟢 | group_leader | direct normal digest (same-day): UBS raises Palantir price target to $250 from $220, reiterates Buy rating on robust AI software demand; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.42); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 4 | **NBIS** | 🟢🔴🟢🟡🟢🟢 | group_leader | direct normal digest (same-day): Nebius raises NVIDIA GPU prices on strong AI demand, surging 8.6% premarket; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.42); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 5 | **HCSG** | 🟢🟢🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 6 | **ASTH** | 🟢🟢🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 7 | **HITI** | 🟢🟢🟢🟡🟢🟢 | group_leader | no direct company event; Pharmaceutical Retailers +3.6% d1 / +1.0% 1w / +3.5% vs parent | BUY GROUP_LEADER — market=GREEN; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 8 | **SABR** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 9 | **IOT** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 10 | **CRWD** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 11 | **PGY** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 12 | **RPD** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 13 | **OKTA** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 14 | **IT** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Information Technology Services +2.9% d1 / +1.9% 1w / +3.9% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 15 | **RELY** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **FLNC** | 🟢🔴🔴🟡🔴🔴 | Utilities - Renewable | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -4.1% |
| 2 | **ATOM** | 🟢🔴🔴🟡🔴🔴 | Semiconductor Equipment & Materials | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -6.3% |
| 3 | **NNDM** | 🟢🔴🔴🟡🔴🔴 | Computer Hardware | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -3.1% |
| 4 | **JKS** | 🟢🔴🔴🟡🔴🔴 | Solar | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow |
| 5 | **OKLO** | 🟢🔴🔴🟡🔴🟡 | Utilities - Independent Power Producers | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -6.3% |
| 6 | **TLN** | 🟢🔴🔴🟡🔴🟡 | Utilities - Independent Power Producers | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -6.3% |
| 7 | **LTBR** | 🟢🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -4.7% |
| 8 | **ELVA** | 🟢🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -4.7% |
| 9 | **NRG** | 🟢🔴🔴🟡🔴🟢 | Utilities - Independent Power Producers | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -6.3% |
| 10 | **VLN** | 🟢🔴🔴🟡🔴🔴 | Semiconductors | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow |
| 11 | **METC** | 🟢🔴🔴🟡🔴🟡 | Coking Coal | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -5.7% |
| 12 | **DGXX** | 🟢🔴🔴🟡🔴🟢 | Utilities - Independent Power Producers | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -6.3% |
| 13 | **FRVO** | 🟢🔴🔴🟡🔴🟢 | Utilities - Renewable | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -4.1% |
| 14 | **EU** | 🟢🟢🔴🟡🔴🟡 | Uranium | SELL/AVOID — market=GREEN; red domains=child,setup; child lags parent -10.3% |
| 15 | **HNRG** | 🟢🔴🔴🟡🔴🟡 | Utilities - Independent Power Producers | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -6.3% |

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
| Consumer Defensive | +1.4% | +0.5% | +0.22 |  |
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
| Learnings / mutable policy | **missing / not in ranker** | next predict prompt, not a ticker score |

### Sector LLM bias (1d) — 0 / empty means that essay was not run today

| Sector | bias |
|--------|------|
| Healthcare | +0.50 |
| Energy | +0.35 |
| Communication Services | +0.33 |
| Consumer Defensive | +0.22 |
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
| general | 50% | 32 | ×0.85 |
| sector:Basic Materials | 52% | 21 | ×0.85 |
| sector:Communication Services | 25% | 20 | ×0.50 |
| sector:Consumer Cyclical | 57% | 21 | ×1.00 |
| sector:Consumer Defensive | 43% | 21 | ×0.50 |
| sector:Energy | 48% | 21 | ×0.85 |
| sector:Financial | 43% | 21 | ×0.50 |
| sector:Healthcare | 50% | 18 | ×0.85 |
| sector:Industrials | 38% | 21 | ×0.50 |
| sector:Real Estate | 52% | 21 | ×0.85 |
| sector:Technology | 42% | 19 | ×0.50 |
| sector:Utilities | 42% | 19 | ×0.50 |

## Horizon weights — book_policy.json v14

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.12 | 0.10 | 0.08 | 0.25 | 0.25 | 0.20 | additive |
| 3d | 0.16 | 0.14 | 0.08 | 0.16 | 0.26 | 0.20 | additive |
| 1w | 0.18 | 0.16 | 0.08 | 0.10 | 0.28 | 0.20 | additive |
| 2w | 0.20 | 0.18 | 0.08 | 0.06 | 0.28 | 0.20 | additive |
| 1m | 0.22 | 0.20 | 0.08 | 0.00 | 0.30 | 0.20 | additive |

## 1d BUY — why these names

### 1. FIVN · $2.6B mid · Technology

**1d score +0.801**

**FIVN** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $2.6B, ADV ~2583k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.98 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.68 | +0.054 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.91 | +0.226 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.91 | +0.183 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.801** | |

### 2. RVTY · $16.5B large · Healthcare

**1d score +0.497**

**RVTY** is a liquid **large-cap** Healthcare name (Diagnostics & Research) at $16.5B, ADV ~1618k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.88 | +0.106 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.50 | +0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.34 | +0.027 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.93 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.96 | +0.192 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.11 | -0.110 | liquid small/mid, room to run |
| **1d total** | | | **+0.497** | |

### 3. WAY · $5.1B mid · Healthcare

**1d score +0.779**

**WAY** is a liquid **mid-cap** Healthcare name (Health Information Services) at $5.1B, ADV ~2791k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.95 | +0.114 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.50 | +0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.10 | +0.008 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.88 | +0.220 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.93 | +0.187 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.779** | |

### 4. IT · $12.1B large · Technology

**1d score +0.578**

**IT** is a liquid **large-cap** Technology name (Information Technology Services) at $12.1B, ADV ~1410k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.98 | +0.117 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.34 | +0.027 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.91 | +0.182 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1d total** | | | **+0.578** | |

### 5. IOT · $24.3B large · Technology

**1d score +0.516**

**IOT** is a liquid **large-cap** Technology name (Software - Infrastructure) at $24.3B, ADV ~6242k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.98 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.68 | +0.054 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.93 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.71 | +0.143 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1d total** | | | **+0.516** | |

### 6. CARG · $3.1B mid · Communication Services

**1d score +0.775**

**CARG** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $3.1B, ADV ~1089k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.87 | +0.104 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.33 | +0.033 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.34 | +0.027 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.90 | +0.180 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.775** | |

### 7. ATRC · $2.9B mid · Healthcare

**1d score +0.759**

**ATRC** is a liquid **mid-cap** Healthcare name (Medical Instruments & Supplies) at $2.9B, ADV ~968k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.94 | +0.113 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.50 | +0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.34 | +0.027 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.95 | +0.239 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.65 | +0.129 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.759** | |

### 8. AMRX · $5.8B mid · Healthcare

**1d score +0.797**

**AMRX** is a liquid **mid-cap** Healthcare name (Drug Manufacturers - Specialty & Generic) at $5.8B, ADV ~1969k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.96 | +0.115 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.50 | +0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.34 | +0.027 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.88 | +0.220 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.67 | +0.134 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.797** | |

### 9. FRSH · $3.3B mid · Technology

**1d score +0.705**

**FRSH** is a liquid **mid-cap** Technology name (Software - Application) at $3.3B, ADV ~13938k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.98 | +0.117 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.34 | +0.027 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.88 | +0.220 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.60 | +0.121 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.705** | |

### 10. M · $5.8B mid · Consumer Cyclical

**1d score +0.782**

**M** is a liquid **mid-cap** Consumer Cyclical name (Department Stores) at $5.8B, ADV ~5648k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.69 | +0.082 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.68 | +0.054 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.82 | +0.164 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.782** | |

### 11. LIND · $1.8B small · Consumer Cyclical

**1d score +0.760**

**LIND** is a liquid **small-cap** Consumer Cyclical name (Travel Services) at $1.8B, ADV ~923k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.89 | +0.107 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.68 | +0.054 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.76 | +0.190 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.69 | +0.139 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.760** | |

### 12. NYT · $11.5B large · Communication Services

**1d score +0.518**

**NYT** is a liquid **large-cap** Communication Services name (Publishing) at $11.5B, ADV ~2014k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.77 | +0.093 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.33 | +0.033 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.34 | +0.027 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.62 | +0.124 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1d total** | | | **+0.518** | |

### 13. IMAX · $2.9B mid · Communication Services

**1d score +0.652**

**IMAX** is a liquid **mid-cap** Communication Services name (Entertainment) at $2.9B, ADV ~1259k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.97 | +0.116 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.33 | +0.033 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.10 | +0.008 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.95 | +0.239 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.29 | +0.057 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.652** | |

### 14. ZD · $2.0B mid · Communication Services

**1d score +0.641**

**ZD** is a liquid **mid-cap** Communication Services name (Advertising Agencies) at $2.0B, ADV ~646k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.95 | +0.113 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.33 | +0.033 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.34 | +0.027 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.28 | +0.056 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.641** | |

### 15. TILE · $2.0B mid · Consumer Cyclical

**1d score +0.699**

**TILE** is a liquid **mid-cap** Consumer Cyclical name (Furnishings, Fixtures & Appliances) at $2.0B, ADV ~501k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.95 | +0.113 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.68 | +0.054 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.29 | +0.059 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.699** | |

### 16. FTDR · $5.5B mid · Consumer Cyclical

**1d score +0.626**

**FTDR** is a liquid **mid-cap** Consumer Cyclical name (Personal Services) at $5.5B, ADV ~569k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.94 | +0.113 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.68 | +0.054 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.21 | +0.043 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.02 | -0.016 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.626** | |

### 17. TAL · $4.0B mid · Consumer Defensive

**1d score +0.584**

**TAL** is a liquid **mid-cap** Consumer Defensive name (Education & Training Services) at $4.0B, ADV ~4842k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.47 | +0.057 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.02 | +0.002 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.10 | +0.008 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.91 | +0.226 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.46 | +0.091 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.584** | |


## 1d AVOID — bottom of the same rank

- **TLN** (large, Utilities, $14.0B) score -0.306. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -6.3%
- **JKS** (small, Technology, $501M) score -0.279. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow
- **CMTG** (micro, Real Estate, $203M) score -0.265. SELL/AVOID — market=GREEN; red domains=parent,child,setup
- **DVLT** (micro, Technology, $168M) score -0.254. SELL/AVOID — market=GREEN; red domains=parent,setup
- **MIST** (micro, Healthcare, $112M) score -0.246. NO BEAR — market=GREEN; red domains=setup
- **NUS** (micro, Consumer Defensive, $223M) score -0.239. NO BEAR — market=GREEN; red domains=setup
- **GETY** (micro, Communication Services, $95M) score -0.228. SELL/AVOID — market=GREEN; red domains=setup,flow
- **TLSA** (micro, Healthcare, $99M) score -0.226. NO BEAR — market=GREEN; red domains=setup
- **CDZI** (small, Industrials, $301M) score -0.225. SELL/AVOID — market=GREEN; red domains=parent,setup
- **LODE** (micro, Basic Materials, $189M) score -0.224. SELL/AVOID — market=GREEN; red domains=parent,child,setup
- **EU** (micro, Energy, $180M) score -0.223. SELL/AVOID — market=GREEN; red domains=child,setup; child lags parent -10.3%
- **STLA** (large, Consumer Cyclical, $14.7B) score -0.222. NO BEAR — market=GREEN; red domains=setup
- **AIRJ** (micro, Industrials, $299M) score -0.217. SELL/AVOID — market=GREEN; red domains=parent,child,setup
- **SGML** (small, Basic Materials, $1.0B) score -0.211. SELL/AVOID — market=GREEN; red domains=parent,child,setup
- **OSG** (micro, Financial, $213M) score -0.209. NO BEAR — market=GREEN; red domains=setup
- **FLNC** (small, Utilities, $1.3B) score -0.208. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -4.1%
- **RVMD** (large, Healthcare, $42.2B) score -0.206. NO BEAR — market=GREEN; red domains=setup
- **LDI** (micro, Financial, $238M) score -0.190. NO BEAR — market=GREEN; red domains=setup
- **ABAT** (small, Industrials, $314M) score -0.188. SELL/AVOID — market=GREEN; red domains=parent,setup
- **NAUT** (micro, Healthcare, $124M) score -0.184. NO BEAR — market=GREEN; red domains=setup
- **ELDN** (micro, Healthcare, $204M) score -0.183. SELL/AVOID — market=GREEN; red domains=setup,flow
- **BGS** (micro, Consumer Defensive, $255M) score -0.183. NO BEAR — market=GREEN; red domains=setup; child lags parent -3.5%
- **LAB** (micro, Healthcare, $278M) score -0.182. NO BEAR — market=GREEN; red domains=setup
- **AREC** (micro, Industrials, $214M) score -0.177. SELL/AVOID — market=GREEN; red domains=parent,setup
- **NAK** (small, Basic Materials, $720M) score -0.174. SELL/AVOID — market=GREEN; red domains=parent,child,setup

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | FIVN | +0.803 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 2 | RVTY | +0.534 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | WAY | +0.838 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | IT | +0.606 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | IOT | +0.518 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 6 | CARG | +0.804 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 7 | ATRC | +0.799 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | AMRX | +0.838 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 9 | FRSH | +0.734 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | M | +0.772 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 11 | LIND | +0.757 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | NYT | +0.543 | large | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 13 | IMAX | +0.705 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 14 | ZD | +0.673 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 15 | TILE | +0.698 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 16 | FTDR | +0.626 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 17 | TAL | +0.604 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | FIVN | +0.810 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 2 | RVTY | +0.563 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | WAY | +0.880 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | IT | +0.628 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | IOT | +0.525 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 6 | CARG | +0.826 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 7 | ATRC | +0.829 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | AMRX | +0.867 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 9 | FRSH | +0.757 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | M | +0.772 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 11 | LIND | +0.759 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | NYT | +0.565 | large | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 13 | IMAX | +0.745 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 14 | ZD | +0.698 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 15 | TILE | +0.702 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 16 | FTDR | +0.630 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 17 | TAL | +0.627 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | FIVN | +0.835 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 2 | RVTY | +0.591 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | WAY | +0.909 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | IT | +0.652 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | IOT | +0.550 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 6 | CARG | +0.851 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 7 | ATRC | +0.859 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | AMRX | +0.897 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 9 | FRSH | +0.781 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | M | +0.791 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 11 | LIND | +0.782 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | NYT | +0.587 | large | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 13 | IMAX | +0.771 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 14 | ZD | +0.724 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 15 | TILE | +0.726 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 16 | FTDR | +0.655 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 17 | TAL | +0.637 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 1m BUY — why these names

### 1. FIVN · $2.6B mid · Technology

**1m score +0.878**

**FIVN** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $2.6B, ADV ~2583k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.98 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.41 | -0.033 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.91 | +0.183 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.878** | |

### 2. RVTY · $16.5B large · Healthcare

**1m score +0.638**

**RVTY** is a liquid **large-cap** Healthcare name (Diagnostics & Research) at $16.5B, ADV ~1618k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.88 | +0.194 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.50 | +0.101 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.20 | -0.016 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.93 | +0.278 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.96 | +0.192 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.11 | -0.110 | liquid small/mid, room to run |
| **1m total** | | | **+0.638** | |

### 3. WAY · $5.1B mid · Healthcare

**1m score +0.956**

**WAY** is a liquid **mid-cap** Healthcare name (Health Information Services) at $5.1B, ADV ~2791k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.95 | +0.209 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.50 | +0.101 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.93 | +0.187 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.956** | |

### 4. IT · $12.1B large · Technology

**1m score +0.693**

**IT** is a liquid **large-cap** Technology name (Information Technology Services) at $12.1B, ADV ~1410k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.98 | +0.215 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.20 | -0.016 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.91 | +0.182 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1m total** | | | **+0.693** | |

### 5. IOT · $24.3B large · Technology

**1m score +0.594**

**IOT** is a liquid **large-cap** Technology name (Software - Infrastructure) at $24.3B, ADV ~6242k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.98 | +0.216 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.41 | -0.033 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.93 | +0.278 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.71 | +0.143 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1m total** | | | **+0.594** | |

### 6. CARG · $3.1B mid · Communication Services

**1m score +0.891**

**CARG** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $3.1B, ADV ~1089k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.87 | +0.192 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.33 | +0.065 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.20 | -0.016 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.90 | +0.180 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.891** | |

### 7. ATRC · $2.9B mid · Healthcare

**1m score +0.908**

**ATRC** is a liquid **mid-cap** Healthcare name (Medical Instruments & Supplies) at $2.9B, ADV ~968k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.94 | +0.207 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.50 | +0.101 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.20 | -0.016 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.95 | +0.286 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.65 | +0.129 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.908** | |

### 8. AMRX · $5.8B mid · Healthcare

**1m score +0.944**

**AMRX** is a liquid **mid-cap** Healthcare name (Drug Manufacturers - Specialty & Generic) at $5.8B, ADV ~1969k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.96 | +0.211 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.50 | +0.101 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.20 | -0.016 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.67 | +0.134 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.944** | |

### 9. FRSH · $3.3B mid · Technology

**1m score +0.823**

**FRSH** is a liquid **mid-cap** Technology name (Software - Application) at $3.3B, ADV ~13938k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.98 | +0.215 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.20 | -0.016 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.60 | +0.121 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.823** | |

### 10. M · $5.8B mid · Consumer Cyclical

**1m score +0.827**

**M** is a liquid **mid-cap** Consumer Cyclical name (Department Stores) at $5.8B, ADV ~5648k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.69 | +0.151 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.41 | -0.033 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.82 | +0.164 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.827** | |

### 11. LIND · $1.8B small · Consumer Cyclical

**1m score +0.821**

**LIND** is a liquid **small-cap** Consumer Cyclical name (Travel Services) at $1.8B, ADV ~923k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.89 | +0.196 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.41 | -0.033 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.69 | +0.139 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.821** | |

### 12. NYT · $11.5B large · Communication Services

**1m score +0.627**

**NYT** is a liquid **large-cap** Communication Services name (Publishing) at $11.5B, ADV ~2014k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.77 | +0.170 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.33 | +0.065 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.20 | -0.016 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.62 | +0.124 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1m total** | | | **+0.627** | |

### 13. IMAX · $2.9B mid · Communication Services

**1m score +0.816**

**IMAX** is a liquid **mid-cap** Communication Services name (Entertainment) at $2.9B, ADV ~1259k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.97 | +0.212 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.33 | +0.065 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.95 | +0.286 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.29 | +0.057 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.816** | |

### 14. ZD · $2.0B mid · Communication Services

**1m score +0.767**

**ZD** is a liquid **mid-cap** Communication Services name (Advertising Agencies) at $2.0B, ADV ~646k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.95 | +0.208 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.33 | +0.065 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.20 | -0.016 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.28 | +0.056 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.767** | |

### 15. TILE · $2.0B mid · Consumer Cyclical

**1m score +0.767**

**TILE** is a liquid **mid-cap** Consumer Cyclical name (Furnishings, Fixtures & Appliances) at $2.0B, ADV ~501k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.95 | +0.208 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.41 | -0.033 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.29 | +0.059 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.767** | |

### 16. FTDR · $5.5B mid · Consumer Cyclical

**1m score +0.696**

**FTDR** is a liquid **mid-cap** Consumer Cyclical name (Personal Services) at $5.5B, ADV ~569k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.94 | +0.207 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.41 | -0.033 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.21 | +0.043 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.02 | -0.016 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.696** | |

### 17. TAL · $4.0B mid · Consumer Defensive

**1m score +0.665**

**TAL** is a liquid **mid-cap** Consumer Defensive name (Education & Training Services) at $4.0B, ADV ~4842k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.47 | +0.104 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.02 | +0.003 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.46 | +0.091 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.665** | |


## 1m AVOID — bottom of the same rank

- **TLN** (large, Utilities, $14.0B) score -0.555. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CMTG** (micro, Real Estate, $203M) score -0.459. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CDZI** (small, Industrials, $301M) score -0.437. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **EU** (micro, Energy, $180M) score -0.430. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **STLA** (large, Consumer Cyclical, $14.7B) score -0.419. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **FLNC** (small, Utilities, $1.3B) score -0.415. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NUS** (micro, Consumer Defensive, $223M) score -0.414. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **SUPV** (small, Financial, $638M) score -0.411. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LDI** (micro, Financial, $238M) score -0.410. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **JKS** (small, Technology, $501M) score -0.399. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GETY** (micro, Communication Services, $95M) score -0.397. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **LODE** (micro, Basic Materials, $189M) score -0.386. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ABAT** (small, Industrials, $314M) score -0.377. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **VLRS** (small, Industrials, $726M) score -0.376. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OSG** (micro, Financial, $213M) score -0.373. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **MIST** (micro, Healthcare, $112M) score -0.360. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BETR** (micro, Financial, $235M) score -0.356. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ACDC** (small, Energy, $881M) score -0.353. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AIRJ** (micro, Industrials, $299M) score -0.350. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **SGML** (small, Basic Materials, $1.0B) score -0.348. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **JELD** (micro, Industrials, $166M) score -0.341. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BHR** (micro, Real Estate, $124M) score -0.338. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AREC** (micro, Industrials, $214M) score -0.333. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LPL** (mid, Technology, $3.0B) score -0.327. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RC** (micro, Real Estate, $261M) score -0.327. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-09-17_stock_book.md`
- Machine table: `data/stock_book/2026-09-17_stock_book.csv`
- Machine book: `data/stock_book/2026-09-17_stock_book.json`
- Join rank: `data/join/2026-09-17_ranked.csv`
- Weather: `01_daily/weather/2026-09-17_weather.md`
- AB enrich: `data/ab_checklist/2026-09-17_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-09-17_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-09-17_map_heat.md`
