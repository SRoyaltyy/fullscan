# Stock book — 2026-09-04

_Generated 2026-09-04T08:54:27.308108-04:00_

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
- General predict (same-day): +0.42 up (present)
- Stand-down: **no** — 432 names qualified through standard,group_leader,catalyst (68 probable)
- Sector predicts this date: 11/11 (ok)
- News tickers in play: 70
- AB coverage: 1970 names · peer RS: 1857
- Universe after liquidity: 2091
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 33

## All-green BUY / SELL

- Mode: **green_pile** · SELL **core_weights_ex_green**
- Pile: **115** liquid all-green names (need ≥ 8) of 2091
- Core fired: join=yes, AB=yes, peer=yes
- pile 115 ≥ 8 liquid all-green names — BUY 15 from the pile by green_rank (no opp); SELL is core weights on the non-green remainder

## Decision lattice — gate → route → rank

The weighted score is now a tie-breaker inside an eligible lane. It cannot average away a market, group, company, or setup veto.

### MARKET: 🟡 YELLOW

- YELLOW: general up score=+2.25; good=+2.5 vs bad=+0.0; risk=off; red pillars=0
- Allowed long lanes: **standard, group_leader, catalyst** · max slots 8 · size ×0.60
- Bull evidence: global sessions +1.00 points; volatility +0.75 points; sentiment +0.50 points; futures +0.25 points

Decision domains: **MKT · parent · child · company · setup · flow**. Measured parent/child tape is kept separate from the LLM essay; direct company events must be price-confirmed on a hard-red day.

### Bull decisions (eligible or closest blocked cases)

| # | Ticker | Domains | Lane | Company / group | Decision |
|---:|--------|---------|------|-----------------|----------|
| 1 | **WAY** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Health Information Services +0.8% d1 / +3.9% 1w / +4.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 2 | **TXG** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Health Information Services +0.8% d1 / +3.9% 1w / +4.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 3 | **HQY** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Health Information Services +0.8% d1 / +3.9% 1w / +4.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 4 | **HNGE** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Health Information Services +0.8% d1 / +3.9% 1w / +4.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 5 | **HTFL** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Health Information Services +0.8% d1 / +3.9% 1w / +4.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 6 | **BTSG** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Health Information Services +0.8% d1 / +3.9% 1w / +4.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 7 | **NU** | 🟡🟢🟢🟡🟢🟢 | standard | direct high digest (stale/undated): Nu Holdings posts record Q2 2026 net income topping $1.1B with EPS $0.20, 33% ROE, authorizes $1B buyback and beats estimates; Banks - Regional +2.2% d1 / +0.6% 1w / +1.1% vs parent | BUY STANDARD — market=YELLOW; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.48); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 8 | **CNH** | 🟡🔴🟢🟡🟢🟢 | group_leader | no direct company event; Farm & Heavy Construction Machinery +2.2% d1 / +0.4% 1w / +3.9% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 9 | **OMDA** | 🟡🟢🟢🟡🟢🟡 | group_leader | no direct company event; Health Information Services +0.8% d1 / +3.9% 1w / +4.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,⚪,Cond green |
| 10 | **GDRX** | 🟡🟢🟢🟡🟢🟡 | group_leader | no direct company event; Health Information Services +0.8% d1 / +3.9% 1w / +4.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,Cond green |
| 11 | **TEM** | 🟡🟢🟢🟡🟢🟡 | group_leader | no direct company event; Health Information Services +0.8% d1 / +3.9% 1w / +4.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,Cond green |
| 12 | **CAT** | 🟡🔴🟢🟡🟢🟢 | group_leader | no direct company event; Farm & Heavy Construction Machinery +2.2% d1 / +0.4% 1w / +3.9% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 13 | **HAFN** | 🟡🔴🟢🟡🟢🟢 | group_leader | no direct company event; Marine Shipping +1.7% d1 / +4.6% 1w / +8.2% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 14 | **XRX** | 🟡🔴🟢🟡🟢🟢 | group_leader | no direct company event; Business Equipment & Supplies +1.1% d1 / +2.4% 1w / +6.0% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵 |
| 15 | **MCK** | 🟡🟢🟢🟡🟢🟡 | group_leader | no direct company event; Medical Distribution +2.0% d1 / +2.8% 1w / +3.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=⚪,Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **IE** | 🟡🔴🟡🟡🔴🔴 | Copper | SELL/AVOID — market=YELLOW; red domains=parent,setup,flow; child lags parent -3.4% |
| 2 | **SATL** | 🟡🔴🔴🟡🔴🔴 | Aerospace & Defense | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 3 | **BKSY** | 🟡🔴🔴🟡🔴🔴 | Specialty Business Services | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 4 | **PACK** | 🟡🔴🟡🟡🔴🔴 | Packaging & Containers | SELL/AVOID — market=YELLOW; red domains=parent,setup,flow; child lags parent -3.4% |
| 5 | **UNFI** | 🟡🔴🔴🟡🔴🔴 | Food Distribution | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 6 | **COLD** | 🟡🔴🔴🟡🔴🔴 | REIT - Industrial | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 7 | **VFC** | 🟡🔴🔴🟡🔴🔴 | Apparel Manufacturing | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.6% |
| 8 | **SIDU** | 🟡🔴🔴🟡🔴🔴 | Aerospace & Defense | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 9 | **TNL** | 🟡🔴🟡🟡🔴🔴 | Travel Services | SELL/AVOID — market=YELLOW; red domains=parent,setup,flow; child lags parent -3.4% |
| 10 | **GIII** | 🟡🔴🔴🟡🟡🔴 | Apparel Manufacturing | SELL/AVOID — market=YELLOW; red domains=parent,child,flow; child lags parent -3.6% |
| 11 | **TRIP** | 🟡🔴🟡🟡🔴🔴 | Travel Services | SELL/AVOID — market=YELLOW; red domains=parent,setup,flow; child lags parent -3.4% |
| 12 | **REAX** | 🟡🔴🔴🟡🔴🟡 | Real Estate Services | SELL/AVOID — market=YELLOW; red domains=parent,child,setup |
| 13 | **LUNR** | 🟡🔴🔴🟡🔴🔴 | Aerospace & Defense | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 14 | **TMC** | 🟡🔴🟡🟡🔴🔴 | Other Industrial Metals & Mining | SELL/AVOID — market=YELLOW; red domains=parent,setup,flow |
| 15 | **RZLV** | 🟡🟡🔴🟡🔴🔴 | Software - Infrastructure | SELL/AVOID — market=YELLOW; red domains=child,setup,flow |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **captain_research** (295 captains, 13 industries → s_heat).
- Board file: `01_daily/map_heat/2026-09-04_map_heat.json` · generated 2026-09-03T01:47:39.371074-04:00

### Sector RS vs same-day LLM essay

| Sector | Finviz 1d | Finviz 1w | LLM 1d | Tape vs essay |
|--------|----------:|----------:|-------:|---------------|
| Basic Materials | +1.8% | -2.2% | -0.55 |  |
| Communication Services | +1.2% | -0.2% | +0.00 |  |
| Consumer Cyclical | +0.3% | -1.9% | -0.50 |  |
| Consumer Defensive | +0.2% | -0.7% | -0.25 |  |
| Energy | +0.2% | +3.8% | +0.00 | essay flat, tape moving |
| Financial | +1.1% | -0.5% | +0.25 |  |
| Healthcare | +0.9% | -0.7% | +0.50 |  |
| Industrials | +0.0% | -3.5% | -0.25 |  |
| Real Estate | -0.5% | -2.8% | +0.00 | essay flat, tape moving |
| Technology | +0.4% | +1.0% | +0.00 |  |
| Utilities | +0.2% | -1.9% | +0.00 | essay flat, tape moving |

### Industry heat (1w vs parent)

**HOT**

- **Oil & Gas Drilling** (Energy) +3.6% 1d · +10.0% 1w · vs parent +6.2% · NE, RIG
- **Agricultural Inputs** (Basic Materials) +2.9% 1d · +8.4% 1w · vs parent +10.6% · CTVA, CF, FMC, IPI
- **Oil & Gas Equipment & Services** (Energy) +1.5% 1d · +6.1% 1w · vs parent +2.3% · SLB, BKR, KGS, WHD
- **Oil & Gas Refining & Marketing** (Energy) +1.1% 1d · +5.9% 1w · vs parent +2.1% · MPC, VLO, PBF, DK
- **Coking Coal** (Basic Materials) +1.7% 1d · +5.3% 1w · vs parent +7.5% · HCC, AMR
- **Farm Products** (Consumer Defensive) +1.1% 1d · +4.8% 1w · vs parent +5.5% · ADM, BG, CALM, DMC
- **Oil & Gas Integrated** (Energy) -0.2% 1d · +4.6% 1w · vs parent +0.8% · XOM, CVX, DEC
- **Marine Shipping** (Industrials) +1.7% 1d · +4.6% 1w · vs parent +8.2% · MATX, CMRE

**COLD**

- **Uranium** (Energy) +0.6% 1d · -9.9% 1w · vs parent -13.7% · UEC, UUUU
- **Textile Manufacturing** (Consumer Cyclical) +7.1% 1d · -6.9% 1w · vs parent -5.0% · AIN
- **Semiconductor Equipment & Materials** (Technology) +0.4% 1d · -6.2% 1w · vs parent -7.2% · LRCX, AMAT, ACMR, KLIC
- **Rental & Leasing Services** (Industrials) -0.3% 1d · -6.2% 1w · vs parent -2.6% · URI, GATX, HRI
- **Railroads** (Industrials) -0.0% 1d · -6.1% 1w · vs parent -2.6% · UNP, CSX, TRN, GBX
- **Lodging** (Consumer Cyclical) +0.2% 1d · -6.1% 1w · vs parent -4.2% · MAR, HLT
- **Real Estate - Development** (Real Estate) -1.2% 1d · -5.7% 1w · vs parent -2.8% · CCS
- **Copper** (Basic Materials) +1.4% 1d · -5.6% 1w · vs parent -3.4% · FCX, IE

### Overrides (child 1w residual ≥ 3pp)

| Action | Industry | 1w | Parent 1w | Gap | Captains |
|--------|----------|---:|----------:|----:|----------|
| OVERRIDE | Uranium | -9.9% | +3.8% | -13.7% | UEC, UUUU |
| OVERRIDE | Agricultural Inputs | +8.4% | -2.2% | +10.6% | CTVA, CF, FMC, IPI |
| OVERRIDE | Marine Shipping | +4.6% | -3.5% | +8.2% | MATX, CMRE |
| OVERRIDE | Coking Coal | +5.3% | -2.2% | +7.5% | HCC, AMR |
| OVERRIDE | Semiconductor Equipment & Materials | -6.2% | +1.0% | -7.2% | LRCX, AMAT, ACMR, KLIC |
| SPLIT | Oil & Gas Drilling | +10.0% | +3.8% | +6.2% | NE, RIG |
| OVERRIDE | Business Equipment & Supplies | +2.4% | -3.5% | +6.0% | XRX |
| OVERRIDE | Steel | +3.6% | -2.2% | +5.8% | NUE, STLD, WS, NWPX |
| OVERRIDE | Chemicals | +3.3% | -2.2% | +5.5% | DOW, HUN, REX |
| OVERRIDE | Farm Products | +4.8% | -0.7% | +5.5% | ADM, BG, CALM, DMC |
| OVERRIDE | Scientific & Technical Instruments | -4.3% | +1.0% | -5.3% | KEYS, GRMN, ESE, NOVT |
| OVERRIDE | Aluminum | +3.0% | -2.2% | +5.2% | CENX, CSTM |
| SPLIT | Textile Manufacturing | -6.9% | -1.9% | -5.0% | AIN |
| OVERRIDE | Communication Equipment | -3.8% | +1.0% | -4.8% | CSCO, MSI, VSAT, BDC |
| OVERRIDE | Electronic Components | -3.6% | +1.0% | -4.6% | APH, GLW, PLXS, BELFA |

### Theme join (sub-sector vs GICS parent)

- **Energy Traditional** — Oil / Majors: +2.8% 1w vs parent +3.8% → AGREE; Oil E&P: +4.0% 1w vs parent +3.8% → AGREE; Oil Services: +6.1% 1w vs parent +3.8% → AGREE; Nuclear: -4.2% 1w vs parent +3.8% → **DIVERGE**
- **Commodities Energy** — Uranium: -9.9% 1w vs parent +0.8% → **DIVERGE**; Oil (commodity): +4.3% 1w vs parent +0.8% → AGREE
- **Energy Renewable** — Solar: -3.5% 1w vs parent +0.9% → **DIVERGE**; Renewable utilities: -1.8% 1w vs parent +0.9% → **DIVERGE**
- **Commodities Metals** — Gold: -5.5% 1w vs parent -2.2% → AGREE; Silver: +0.5% 1w vs parent -2.2% → **DIVERGE**; Copper: -5.6% 1w vs parent -2.2% → AGREE; Other precious: -1.7% 1w vs parent -2.2% → AGREE
- **Semiconductors** — Semis: +2.5% 1w vs parent +1.0% → AGREE; Semi equipment: -6.2% 1w vs parent +1.0% → **DIVERGE**
- **Artificial Intelligence** — AI compute / semis: +2.5% 1w vs parent +1.0% → AGREE; Software infra: -0.5% 1w vs parent +1.0% → **DIVERGE**
- **Defense & Aerospace** — Aero / defense: -3.3% 1w vs parent -3.5% → AGREE

### Theme ETF tape (biggest |1w| moves)

| Theme | 1d | 1w | Leaders |
|-------|---:|---:|---------|
| Agri-business | +1.9% | +5.5% | MOO, VEGI, FTAG |
| Industrials | +0.2% | -4.3% | XLI, ITA, AIRR |
| Materials | +1.7% | -4.1% | GDX, GDXJ, XLB |
| Space Exploration & Technology | +0.9% | -3.1% | NASA, ARKX, UFO |
| Fintech | +1.5% | -3.0% | BLOK, ARKF, BITQ |
| Energy | +0.2% | +2.8% | XLE, AMLP, VDE |
| Real Estate | -0.2% | -2.7% | VNQ, SCHH, XLRE |
| Infrastructure | +0.0% | -2.6% | PAVE, GRID, IGF |
| Consumer Discretionary | +0.2% | -2.5% | XLY, VCR, TSLL |
| Robotics & Automation | -0.0% | -2.2% | BAI, AIQ, QTUM |
| Utilities | +0.2% | -2.2% | XLU, VPU, FUTY |
| Cannabis Based Businesses | +0.8% | +2.1% | MSOS, MJ, CNBS |

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
| Basic Materials | -0.55 |
| Consumer Cyclical | -0.50 |
| Healthcare | +0.50 |
| Consumer Defensive | -0.25 |
| Financial | +0.25 |
| Industrials | -0.25 |
| Communication Services | +0.00 |
| Energy | +0.00 |
| Real Estate | +0.00 |
| Technology | +0.00 |
| Utilities | +0.00 |

### How much each predictor is trusted (graded hit rate)

| Topic | hit rate | n | weight |
|-------|----------|---|--------|
| general | 46% | 24 | ×0.85 |
| sector:Basic Materials | 58% | 12 | ×1.00 |
| sector:Communication Services | 25% | 12 | ×0.50 |
| sector:Consumer Cyclical | 58% | 12 | ×1.00 |
| sector:Consumer Defensive | 42% | 12 | ×0.50 |
| sector:Energy | 50% | 12 | ×0.85 |
| sector:Financial | 33% | 12 | ×0.50 |
| sector:Healthcare | 78% | 9 | ×1.00 |
| sector:Industrials | 25% | 12 | ×0.50 |
| sector:Real Estate | 58% | 12 | ×1.00 |
| sector:Technology | 42% | 12 | ×0.50 |
| sector:Utilities | 42% | 12 | ×0.50 |

## Horizon weights — book_policy.json v11

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.12 | 0.10 | 0.08 | 0.25 | 0.25 | 0.20 | additive |
| 3d | 0.16 | 0.14 | 0.08 | 0.16 | 0.26 | 0.20 | additive |
| 1w | 0.18 | 0.16 | 0.08 | 0.10 | 0.28 | 0.20 | additive |
| 2w | 0.20 | 0.18 | 0.08 | 0.06 | 0.28 | 0.20 | additive |
| 1m | 0.22 | 0.20 | 0.08 | 0.00 | 0.30 | 0.20 | additive |

## 1d BUY — why these names

### 1. HOOD · $112.1B large · Financial

**1d score +0.584**

**HOOD** is a liquid **large-cap** Financial name (Capital Markets) at $112.1B, ADV ~24437k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.05 | +0.005 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.42 | +0.034 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.96 | +0.241 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.93 | +0.186 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1d total** | | | **+0.584** | |

### 2. XP · $10.3B large · Financial

**1d score +0.558**

**XP** is a liquid **large-cap** Financial name (Capital Markets) at $10.3B, ADV ~5294k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **extended**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.98 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.05 | +0.005 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.21 | +0.017 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.97 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.88 | +0.176 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1d total** | | | **+0.558** | |

### 3. ASND · $17.8B large · Healthcare

**1d score +0.412**

**ASND** is a liquid **large-cap** Healthcare name (Biotechnology) at $17.8B, ADV ~651k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.50 | +0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.06 | +0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.88 | +0.220 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.69 | +0.139 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.412** | |

### 4. NVAX · $1.7B small · Healthcare

**1d score +0.830**

**NVAX** is a liquid **small-cap** Healthcare name (Biotechnology) at $1.7B, ADV ~4245k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.75 | +0.090 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.50 | +0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.42 | +0.034 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.88 | +0.220 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.93 | +0.185 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.830** | |

### 5. OSCR · $10.0B mid · Healthcare

**1d score +0.838**

**OSCR** is a liquid **mid-cap** Healthcare name (Healthcare Plans) at $10.0B, ADV ~5937k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.98 | +0.117 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.50 | +0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.42 | +0.034 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.91 | +0.226 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.66 | +0.133 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.838** | |

### 6. ATRC · $2.7B mid · Healthcare

**1d score +0.783**

**ATRC** is a liquid **mid-cap** Healthcare name (Medical Instruments & Supplies) at $2.7B, ADV ~889k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.96 | +0.115 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.50 | +0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.21 | +0.017 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.88 | +0.220 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.65 | +0.131 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.783** | |

### 7. DUOL · $7.4B mid · Technology

**1d score +0.722**

**DUOL** is a liquid **mid-cap** Technology name (Software - Application) at $7.4B, ADV ~1316k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.69 | +0.083 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.21 | +0.017 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.91 | +0.226 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.88 | +0.176 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.722** | |

### 8. MRX · $5.4B mid · Financial

**1d score +0.717**

**MRX** is a liquid **mid-cap** Financial name (Capital Markets) at $5.4B, ADV ~819k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.05 | +0.005 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.06 | +0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.95 | +0.239 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.50 | +0.100 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.717** | |

### 9. NU · $75.7B large · Financial

**1d score +0.617**

**NU** is a liquid **large-cap** Financial name (Banks - Regional) at $75.7B, ADV ~75982k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.05 | +0.005 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.21 | +0.017 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.31 | +0.077 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.70 | +0.176 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.71 | +0.143 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1d total** | | | **+0.617** | |

### 10. VNT · $4.5B mid · Technology

**1d score +0.641**

**VNT** is a liquid **mid-cap** Technology name (Scientific & Technical Instruments) at $4.5B, ADV ~1598k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.71 | +0.085 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.21 | +0.017 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.93 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.50 | +0.099 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.01 | -0.012 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.641** | |

### 11. FUBO · $1.3B small · Communication Services

**1d score +0.663**

**FUBO** is a liquid **small-cap** Communication Services name (Broadcasting) at $1.3B, ADV ~1495k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.09 | +0.011 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.42 | +0.034 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.91 | +0.226 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.94 | +0.188 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.02 | -0.016 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.663** | |

### 12. SONO · $1.9B small · Technology

**1d score +0.679**

**SONO** is a liquid **small-cap** Technology name (Consumer Electronics) at $1.9B, ADV ~1916k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.48 | +0.057 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.42 | +0.034 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.58 | +0.117 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.05 | +0.049 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.679** | |

### 13. STGW · $2.2B mid · Communication Services

**1d score +0.577**

**STGW** is a liquid **mid-cap** Communication Services name (Advertising Agencies) at $2.2B, ADV ~1388k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.77 | +0.092 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.21 | +0.017 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.10 | +0.020 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.016 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.577** | |

### 14. NCNO · $2.5B mid · Technology

**1d score +0.521**

**NCNO** is a liquid **mid-cap** Technology name (Software - Application) at $2.5B, ADV ~2976k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.65 | +0.078 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.06 | +0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.64 | +0.159 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.30 | +0.059 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.521** | |

### 15. MTCH · $9.7B mid · Communication Services

**1d score +0.534**

**MTCH** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $9.7B, ADV ~3304k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.72 | +0.087 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.42 | +0.034 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.70 | +0.176 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.09 | +0.018 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.534** | |

### 16. UPWK · $1.1B small · Communication Services

**1d score +0.505**

**UPWK** is a liquid **small-cap** Communication Services name (Internet Content & Information) at $1.1B, ADV ~3453k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.25 | +0.030 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.21 | +0.017 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.70 | +0.176 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.31 | +0.062 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.505** | |


## 1d AVOID — bottom of the same rank

- **FUN** (small, Consumer Cyclical, $1.5B) score -0.540. SELL/AVOID — market=YELLOW; red domains=parent,setup,flow
- **PACK** (small, Consumer Cyclical, $376M) score -0.534. SELL/AVOID — market=YELLOW; red domains=parent,setup,flow; child lags parent -3.4%
- **TMC** (small, Basic Materials, $1.9B) score -0.530. SELL/AVOID — market=YELLOW; red domains=parent,setup,flow
- **LODE** (micro, Basic Materials, $245M) score -0.526. SELL/AVOID — market=YELLOW; red domains=parent,setup,flow
- **FCEL** (small, Industrials, $1.2B) score -0.523. SELL/AVOID — market=YELLOW; red domains=parent,setup,flow
- **SPIR** (small, Industrials, $483M) score -0.521. SELL/AVOID — market=YELLOW; red domains=parent,child,setup
- **UNFI** (mid, Consumer Defensive, $2.6B) score -0.520. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow
- **MNSO** (mid, Consumer Cyclical, $2.8B) score -0.512. SELL/AVOID — market=YELLOW; red domains=parent,setup
- **AIIO** (small, Consumer Cyclical, $301M) score -0.506. SELL/AVOID — market=YELLOW; red domains=parent,setup
- **TROX** (small, Basic Materials, $779M) score -0.506. SELL/AVOID — market=YELLOW; red domains=parent,setup,flow
- **METC** (small, Basic Materials, $714M) score -0.506. SELL/AVOID — market=YELLOW; red domains=parent,setup,flow
- **SES** (micro, Consumer Cyclical, $188M) score -0.496. SELL/AVOID — market=YELLOW; red domains=parent,setup
- **LCID** (small, Consumer Cyclical, $1.8B) score -0.490. SELL/AVOID — market=YELLOW; red domains=parent,setup,flow
- **NG** (mid, Basic Materials, $3.7B) score -0.486. SELL/AVOID — market=YELLOW; red domains=parent,setup; child lags parent -3.3%
- **ASPN** (small, Basic Materials, $397M) score -0.478. SELL/AVOID — market=YELLOW; red domains=parent,setup,flow
- **IE** (small, Basic Materials, $1.6B) score -0.476. SELL/AVOID — market=YELLOW; red domains=parent,setup,flow; child lags parent -3.4%
- **LWLG** (small, Basic Materials, $789M) score -0.470. SELL/AVOID — market=YELLOW; red domains=parent,setup
- **NIO** (mid, Consumer Cyclical, $9.0B) score -0.462. SELL/AVOID — market=YELLOW; red domains=parent,setup,flow
- **BKSY** (small, Industrials, $844M) score -0.459. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow
- **EVTL** (micro, Industrials, $103M) score -0.448. SELL/AVOID — market=YELLOW; red domains=parent,child,setup
- **RCAT** (small, Industrials, $1.3B) score -0.448. SELL/AVOID — market=YELLOW; red domains=parent,child,setup
- **OEC** (small, Basic Materials, $337M) score -0.445. SELL/AVOID — market=YELLOW; red domains=parent,setup,flow
- **PCT** (small, Industrials, $1.3B) score -0.438. SELL/AVOID — market=YELLOW; red domains=parent,setup
- **MBC** (small, Consumer Cyclical, $1.7B) score -0.435. SELL/AVOID — market=YELLOW; red domains=parent,setup
- **PPTA** (mid, Basic Materials, $3.1B) score -0.434. SELL/AVOID — market=YELLOW; red domains=parent,setup

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | HOOD | +0.601 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | XP | +0.592 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | ASND | +0.405 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 4 | NVAX | +0.785 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 5 | OSCR | +0.803 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | ATRC | +0.763 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 7 | DUOL | +0.750 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 8 | MRX | +0.763 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | NU | +0.620 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 10 | VNT | +0.669 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 11 | FUBO | +0.650 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | SONO | +0.680 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | STGW | +0.607 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | NCNO | +0.556 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | MTCH | +0.544 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 16 | UPWK | +0.513 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | HOOD | +0.673 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | XP | +0.647 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | ASND | +0.448 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 4 | NVAX | +0.851 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 5 | OSCR | +0.874 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | ATRC | +0.817 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 7 | DUOL | +0.843 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 8 | MRX | +0.806 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | NU | +0.652 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 10 | VNT | +0.763 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 11 | FUBO | +0.748 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | SONO | +0.784 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | STGW | +0.700 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | NCNO | +0.631 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | MTCH | +0.651 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 16 | UPWK | +0.593 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | HOOD | +0.617 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | XP | +0.608 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | ASND | +0.462 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 4 | NVAX | +0.832 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 5 | OSCR | +0.860 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | ATRC | +0.819 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 7 | DUOL | +0.849 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 8 | MRX | +0.779 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | NU | +0.600 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 10 | VNT | +0.769 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 11 | FUBO | +0.724 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | SONO | +0.769 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | STGW | +0.708 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | NCNO | +0.648 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | MTCH | +0.640 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 16 | UPWK | +0.590 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |

## 1m BUY — why these names

### 1. HOOD · $112.1B large · Financial

**1m score +0.652**

**HOOD** is a liquid **large-cap** Financial name (Capital Markets) at $112.1B, ADV ~24437k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.96 | +0.289 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.93 | +0.186 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1m total** | | | **+0.652** | |

### 2. XP · $10.3B large · Financial

**1m score +0.643**

**XP** is a liquid **large-cap** Financial name (Capital Markets) at $10.3B, ADV ~5294k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **extended**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.98 | +0.216 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.97 | +0.292 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.88 | +0.176 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1m total** | | | **+0.643** | |

### 3. ASND · $17.8B large · Healthcare

**1m score +0.500**

**ASND** is a liquid **large-cap** Healthcare name (Biotechnology) at $17.8B, ADV ~651k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.69 | +0.139 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1m total** | | | **+0.500** | |

### 4. NVAX · $1.7B small · Healthcare

**1m score +0.865**

**NVAX** is a liquid **small-cap** Healthcare name (Biotechnology) at $1.7B, ADV ~4245k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.75 | +0.166 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.93 | +0.185 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.865** | |

### 5. OSCR · $10.0B mid · Healthcare

**1m score +0.898**

**OSCR** is a liquid **mid-cap** Healthcare name (Healthcare Plans) at $10.0B, ADV ~5937k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.98 | +0.215 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.66 | +0.133 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.898** | |

### 6. ATRC · $2.7B mid · Healthcare

**1m score +0.856**

**ATRC** is a liquid **mid-cap** Healthcare name (Medical Instruments & Supplies) at $2.7B, ADV ~889k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.96 | +0.211 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.65 | +0.131 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.856** | |

### 7. DUOL · $7.4B mid · Technology

**1m score +0.890**

**DUOL** is a liquid **mid-cap** Technology name (Software - Application) at $7.4B, ADV ~1316k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.69 | +0.153 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.45 | +0.090 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.88 | +0.176 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.890** | |

### 8. MRX · $5.4B mid · Financial

**1m score +0.813**

**MRX** is a liquid **mid-cap** Financial name (Capital Markets) at $5.4B, ADV ~819k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.95 | +0.286 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.50 | +0.100 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.813** | |

### 9. NU · $75.7B large · Financial

**1m score +0.611**

**NU** is a liquid **large-cap** Financial name (Banks - Regional) at $75.7B, ADV ~75982k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.31 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.211 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.71 | +0.143 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1m total** | | | **+0.611** | |

### 10. VNT · $4.5B mid · Technology

**1m score +0.811**

**VNT** is a liquid **mid-cap** Technology name (Scientific & Technical Instruments) at $4.5B, ADV ~1598k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.71 | +0.156 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.45 | +0.090 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.93 | +0.278 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.50 | +0.099 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.01 | -0.012 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.811** | |

### 11. FUBO · $1.3B small · Communication Services

**1m score +0.753**

**FUBO** is a liquid **small-cap** Communication Services name (Broadcasting) at $1.3B, ADV ~1495k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.09 | +0.019 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.45 | +0.090 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.94 | +0.188 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.02 | -0.016 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.753** | |

### 12. SONO · $1.9B small · Technology

**1m score +0.804**

**SONO** is a liquid **small-cap** Technology name (Consumer Electronics) at $1.9B, ADV ~1916k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.48 | +0.105 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.45 | +0.090 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.58 | +0.117 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.05 | +0.049 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.804** | |

### 13. STGW · $2.2B mid · Communication Services

**1m score +0.749**

**STGW** is a liquid **mid-cap** Communication Services name (Advertising Agencies) at $2.2B, ADV ~1388k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.77 | +0.168 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.45 | +0.090 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.10 | +0.020 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.016 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.749** | |

### 14. NCNO · $2.5B mid · Technology

**1m score +0.683**

**NCNO** is a liquid **mid-cap** Technology name (Software - Application) at $2.5B, ADV ~2976k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.65 | +0.143 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.45 | +0.090 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.64 | +0.191 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.30 | +0.059 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.683** | |

### 15. MTCH · $9.7B mid · Communication Services

**1m score +0.678**

**MTCH** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $9.7B, ADV ~3304k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.72 | +0.159 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.45 | +0.090 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.211 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.09 | +0.018 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.678** | |

### 16. UPWK · $1.1B small · Communication Services

**1m score +0.618**

**UPWK** is a liquid **small-cap** Communication Services name (Internet Content & Information) at $1.1B, ADV ~3453k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.25 | +0.055 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.45 | +0.090 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.211 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.31 | +0.062 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.618** | |


## 1m AVOID — bottom of the same rank

- **PACK** (small, Consumer Cyclical, $376M) score -0.766. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FUN** (small, Consumer Cyclical, $1.5B) score -0.761. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AIIO** (small, Consumer Cyclical, $301M) score -0.725. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LCID** (small, Consumer Cyclical, $1.8B) score -0.715. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SES** (micro, Consumer Cyclical, $188M) score -0.708. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**
- **MNSO** (mid, Consumer Cyclical, $2.8B) score -0.687. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **SPIR** (small, Industrials, $483M) score -0.682. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **MBC** (small, Consumer Cyclical, $1.7B) score -0.678. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FCEL** (small, Industrials, $1.2B) score -0.671. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TMC** (small, Basic Materials, $1.9B) score -0.665. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LODE** (micro, Basic Materials, $245M) score -0.663. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CPNG** (large, Consumer Cyclical, $27.9B) score -0.660. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **UNFI** (mid, Consumer Defensive, $2.6B) score -0.650. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **METC** (small, Basic Materials, $714M) score -0.645. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PENN** (mid, Consumer Cyclical, $2.3B) score -0.640. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **TROX** (small, Basic Materials, $779M) score -0.625. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**
- **AZI** (micro, Consumer Cyclical, $86M) score -0.623. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PRKS** (small, Consumer Cyclical, $1.8B) score -0.621. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **WH** (mid, Consumer Cyclical, $5.3B) score -0.617. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ASPN** (small, Basic Materials, $397M) score -0.611. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LWLG** (small, Basic Materials, $789M) score -0.610. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NIO** (mid, Consumer Cyclical, $9.0B) score -0.607. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **MVST** (micro, Consumer Cyclical, $269M) score -0.606. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **RCAT** (small, Industrials, $1.3B) score -0.601. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BKSY** (small, Industrials, $844M) score -0.600. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-09-04_stock_book.md`
- Machine table: `data/stock_book/2026-09-04_stock_book.csv`
- Machine book: `data/stock_book/2026-09-04_stock_book.json`
- Join rank: `data/join/2026-09-04_ranked.csv`
- Weather: `01_daily/weather/2026-09-04_weather.md`
- AB enrich: `data/ab_checklist/2026-09-04_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-09-04_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-09-04_map_heat.md`
