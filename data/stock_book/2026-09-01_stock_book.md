# Stock book — 2026-09-01

_Generated 2026-09-01T09:29:43.760137-04:00_

This file is the **human read** of one run. CSV/JSON next to it are the machine files.

## How today's action is built

**1d uses a decision lattice.** Evidence is evaluated on its own merits before any numeric rank:

1. **Market gate** — raw general factor scoreboard + risk state sets exposure. An extreme confirmed red day closes ordinary longs.
2. **Parent / child route** — sector tape/essay and independent industry/theme absolute + relative strength decide where.
3. **Company route** — News Judge adjudicates; actions, Finviz digest and dossiers form one deduplicated direct-event decision.
4. **Setup / flow gate** — intrinsic AB + join structure, peer RS, price/gap and time-aware relative volume decide whether now.
5. **Rank inside the lane** — standard, group-leader or catalyst. mid_opp cannot grant permission.

The existing red/yellow/green source graph remains visible. Its digest, judge and catalyst cells are now populated before selection. 🔵 / 🚨 / ⚪, Cond, region and featured fades remain gates. A second six-domain row prevents duplicate headlines from voting three times. Longer horizons remain on the legacy weighted rank while the 1d lattice is validated.

## Today's regime

- Weather risk: **off**
- General predict (same-day): -0.47 down (present)
- Stand-down: **no** — 239 names qualified through catalyst_exception,probable (239 probable)
- Sector predicts this date: 11/11 (ok)
- News tickers in play: 115
- AB coverage: 2517 names · peer RS: 2397
- Universe after liquidity: 2683
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 104

## All-green BUY / SELL

- Mode: **green_pile** · SELL **core_weights_ex_green**
- Pile: **53** liquid all-green names (need ≥ 8) of 2683
- Core fired: join=yes, AB=yes, peer=yes
- pile 53 ≥ 8 liquid all-green names — BUY 15 from the pile by green_rank (no opp); SELL is core weights on the non-green remainder

## Decision lattice — gate → route → rank

The weighted score is now a tie-breaker inside an eligible lane. It cannot average away a market, group, company, or setup veto.

### MARKET: 🔴 HARD_RED

- HARD_RED: general down score=-6.30; good=+0.5 vs bad=-7.5; risk=off; red pillars=6
- Allowed long lanes: **catalyst_exception, probable** · max slots 10 · size ×0.25
- Bull evidence: sentiment +0.50 points
- Bear evidence: overnight catalysts -3.00 points; rates / Fed -2.00 points; global sessions -1.00 points; volatility -0.75 points; oil / dollar -0.50 points; futures -0.25 points

Decision domains: **MKT · parent · child · company · setup · flow**. Measured parent/child tape is kept separate from the LLM essay; direct company events must be price-confirmed on a hard-red day.

### Bull decisions (eligible or closest blocked cases)

| # | Ticker | Domains | Lane | Company / group | Decision |
|---:|--------|---------|------|-----------------|----------|
| 1 | **CRM** | 🔴🟡🟡🟢🟢🟡 | probable | direct high digest (same-day): Salesforce beats Q2 guidance, raises FY27 outlook as analysts lift price targets after 'narrative-changing' results; Software - Application -0.5% d1 / +3.9% 1w / +0.7% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: company news fresh (0.78) — market=HARD_RED; parent=YELLOW; child=YELLOW/rel=YELLOW; company=GREEN(0.78); setup=GREEN; flow=YELLOW; lookback=Cond green |
| 2 | **KMI** | 🔴🟢🟢🟡🟢🟢 | probable | direct high digest (stale/undated): KMI rebounds on pipeline project momentum and raised 2026 guidance; Oil & Gas Midstream +1.2% d1 / +2.0% 1w / +1.5% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.48); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 3 | **FTI** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Oil & Gas Equipment & Services +2.9% d1 / +5.6% 1w / +5.0% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +5.6% 1w / +5.0% rel; lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 4 | **CNR** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Thermal Coal +1.4% d1 / +4.7% 1w / +4.1% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +4.7% 1w / +4.1% rel; lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 5 | **DK** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Oil & Gas Refining & Marketing +1.5% d1 / +3.1% 1w / +2.6% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 6 | **INVX** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Oil & Gas Equipment & Services +2.9% d1 / +5.6% 1w / +5.0% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +5.6% 1w / +5.0% rel; lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 7 | **OXY** | 🔴🟢🟡🟡🟢🟢 | probable | basket/action net=+6.00; context only, not a company catalyst; Oil & Gas E&P +1.5% d1 / -0.7% 1w / -1.3% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=YELLOW/rel=YELLOW; company=YELLOW(0.40); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 8 | **DOCS** | 🔴🔴🟢🟡🟢🟢 | probable | no direct company event; Health Information Services +1.6% d1 / +4.5% 1w / +6.9% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +4.5% 1w / +6.9% rel; lookback 🔵 blue — market=HARD_RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 9 | **DHT** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Oil & Gas Midstream +1.2% d1 / +2.0% 1w / +1.5% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 10 | **LNG** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Oil & Gas Midstream +1.2% d1 / +2.0% 1w / +1.5% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 11 | **PAGP** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Oil & Gas Midstream +1.2% d1 / +2.0% 1w / +1.5% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 12 | **WAY** | 🔴🔴🟢🟡🟢🟢 | probable | no direct company event; Health Information Services +1.6% d1 / +4.5% 1w / +6.9% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +4.5% 1w / +6.9% rel; lookback 🔵 blue — market=HARD_RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 13 | **SDRL** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Oil & Gas Drilling +1.1% d1 / +2.0% 1w / +1.5% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 14 | **VEEV** | 🔴🔴🟢🟡🟢🟢 | probable | no direct company event; Health Information Services +1.6% d1 / +4.5% 1w / +6.9% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +4.5% 1w / +6.9% rel; lookback 🔵 blue — market=HARD_RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 15 | **HMC** | 🔴🔴🟢🟡🟢🟢 | probable | no direct company event; Auto Manufacturers +4.1% d1 / +3.9% 1w / +5.1% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +3.9% 1w / +5.1% rel; lookback 🔵 blue — market=HARD_RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **JBLU** | 🔴🔴🔴🟡🔴🔴 | Airlines | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.3% |
| 2 | **RKT** | 🔴🔴🔴🟡🔴🔴 | Mortgage Finance | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.8% |
| 3 | **ARKO** | 🔴🔴🔴🟡🔴🔴 | Specialty Retail | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -5.9% |
| 4 | **CSIQ** | 🔴🟡🔴🟡🔴🔴 | Solar | SELL/AVOID — market=HARD_RED; red domains=child,setup,flow; child lags parent -6.4% |
| 5 | **OPEN** | 🔴🔴🔴🟡🔴🔴 | Real Estate Services | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 6 | **LWLG** | 🔴🔴🔴🟡🔴🔴 | Specialty Chemicals | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 7 | **SLQT** | 🔴🔴🔴🟡🔴🟡 | Insurance Brokers | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -4.4% |
| 8 | **SMR** | 🔴🔴🔴🟡🔴🔴 | Specialty Industrial Machinery | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 9 | **PLTK** | 🔴🔴🔴🟡🔴🔴 | Electronic Gaming & Multimedia | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 10 | **CRML** | 🔴🔴🔴🟡🔴🔴 | Other Industrial Metals & Mining | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 11 | **ORBS** | 🔴🔴🔴🟡🔴🟡 | Packaging & Containers | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -3.3% |
| 12 | **HYMC** | 🔴🔴🔴🟡🔴🔴 | Gold | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 13 | **DQ** | 🔴🟡🔴🟡🔴🔴 | Solar | SELL/AVOID — market=HARD_RED; red domains=child,setup,flow; child lags parent -6.4% |
| 14 | **GNRC** | 🔴🔴🔴🟡🔴🔴 | Specialty Industrial Machinery | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 15 | **NPWR** | 🔴🔴🔴🟡🔴🔴 | Specialty Industrial Machinery | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **finviz_tape** (44 captains, 15 industries → s_heat).
- Board file: `01_daily/map_heat/2026-09-01_map_heat.json` · generated 2026-09-01T02:16:10.519913-04:00

### Sector RS vs same-day LLM essay

| Sector | Finviz 1d | Finviz 1w | LLM 1d | Tape vs essay |
|--------|----------:|----------:|-------:|---------------|
| Basic Materials | -0.8% | -2.1% | -0.55 |  |
| Communication Services | -1.5% | -1.1% | -0.26 |  |
| Consumer Cyclical | -0.8% | -1.2% | -0.56 |  |
| Consumer Defensive | -0.3% | -2.2% | +0.00 | essay flat, tape moving |
| Energy | +1.8% | +0.6% | +0.48 |  |
| Financial | -0.5% | -0.4% | -0.26 |  |
| Healthcare | -0.4% | -2.5% | -0.52 |  |
| Industrials | -0.7% | -1.2% | -0.28 |  |
| Real Estate | -0.9% | -2.7% | -0.55 |  |
| Technology | +0.2% | +3.2% | -0.28 | essay DOWN, tape UP |
| Utilities | -0.9% | -2.0% | -0.26 |  |

### Industry heat (1w vs parent)

**HOT**

- **Infrastructure Operations** (Industrials) +2.4% 1d · +5.9% 1w · vs parent +7.1% · —
- **Coking Coal** (Basic Materials) +2.4% 1d · +5.8% 1w · vs parent +8.0% · HCC, AMR
- **Oil & Gas Equipment & Services** (Energy) +2.9% 1d · +5.6% 1w · vs parent +5.0% · SLB, BKR, KGS, WHD
- **Software - Infrastructure** (Technology) -0.4% 1d · +5.2% 1w · vs parent +2.0% · MSFT, PLTR, ZETA, QLYS
- **Thermal Coal** (Energy) +1.4% 1d · +4.7% 1w · vs parent +4.1% · CNR, BTU
- **Health Information Services** (Healthcare) +1.6% 1d · +4.5% 1w · vs parent +6.9% · VEEV, BTSG, TXG
- **Auto Manufacturers** (Consumer Cyclical) +4.1% 1d · +3.9% 1w · vs parent +5.1% · TSLA, GM, LCID, LVWR
- **Semiconductors** (Technology) +1.0% 1d · +3.9% 1w · vs parent +0.7% · NVDA, AVGO, SLAB, MXL

**COLD**

- **Textile Manufacturing** (Consumer Cyclical) -4.6% 1d · -10.8% 1w · vs parent -9.6% · AIN
- **Specialty Retail** (Consumer Cyclical) -0.1% 1d · -7.1% 1w · vs parent -5.9% · CASY, WSM, RH, ASO
- **Resorts & Casinos** (Consumer Cyclical) -2.8% 1d · -6.2% 1w · vs parent -5.0% · LVS, MGM, RRR, VAC
- **Travel Services** (Consumer Cyclical) -3.2% 1d · -6.1% 1w · vs parent -4.8% · BKNG, ABNB, GBTG, LIND
- **Luxury Goods** (Consumer Cyclical) -1.7% 1d · -5.8% 1w · vs parent -4.6% · TPR, SIG, CPRI
- **Gold** (Basic Materials) -1.3% 1d · -5.1% 1w · vs parent -3.0% · NEM, SSRM, NG
- **Apparel Manufacturing** (Consumer Cyclical) -1.7% 1d · -5.1% 1w · vs parent -3.9% · RL, ZGN, KTB
- **Beverages - Wineries & Distilleries** (Consumer Defensive) -1.4% 1d · -4.9% 1w · vs parent -2.7% · BF-B

### Overrides (child 1w residual ≥ 3pp)

| Action | Industry | 1w | Parent 1w | Gap | Captains |
|--------|----------|---:|----------:|----:|----------|
| SPLIT | Textile Manufacturing | -10.8% | -1.2% | -9.6% | AIN |
| OVERRIDE | Coking Coal | +5.8% | -2.1% | +8.0% | HCC, AMR |
| OVERRIDE | Infrastructure Operations | +5.9% | -1.2% | +7.1% | — |
| OVERRIDE | Health Information Services | +4.5% | -2.5% | +6.9% | VEEV, BTSG, TXG |
| OVERRIDE | Semiconductor Equipment & Materials | -3.5% | +3.2% | -6.7% | LRCX, AMAT, ACMR, KLIC |
| OVERRIDE | Solar | -3.2% | +3.2% | -6.4% | FSLR, RUN, SHLS |
| SPLIT | Specialty Retail | -7.1% | -1.2% | -5.9% | CASY, WSM, RH, ASO |
| OVERRIDE | Pharmaceutical Retailers | +2.9% | -2.5% | +5.4% | — |
| OVERRIDE | Auto Manufacturers | +3.9% | -1.2% | +5.1% | TSLA, GM, LCID, LVWR |
| SPLIT | Oil & Gas Equipment & Services | +5.6% | +0.6% | +5.0% | SLB, BKR, KGS, WHD |
| SPLIT | Resorts & Casinos | -6.2% | -1.2% | -5.0% | LVS, MGM, RRR, VAC |
| SPLIT | Travel Services | -6.1% | -1.2% | -4.8% | BKNG, ABNB, GBTG, LIND |
| SPLIT | Luxury Goods | -5.8% | -1.2% | -4.6% | TPR, SIG, CPRI |
| SPLIT | Insurance Brokers | -4.8% | -0.4% | -4.4% | MRSH, AON, NP, BWIN |
| OVERRIDE | Steel | +2.2% | -2.1% | +4.3% | NUE, STLD, WS, NWPX |

### Theme join (sub-sector vs GICS parent)

- **Energy Traditional** — Oil / Majors: +0.7% 1w vs parent +0.6% → AGREE; Oil E&P: -0.7% 1w vs parent +0.6% → **DIVERGE**; Oil Services: +5.6% 1w vs parent +0.6% → AGREE; Nuclear: -1.5% 1w vs parent +0.6% → **DIVERGE**
- **Commodities Energy** — Uranium: -3.3% 1w vs parent -0.8% → AGREE; Oil (commodity): -0.7% 1w vs parent -0.8% → AGREE
- **Energy Renewable** — Solar: -3.2% 1w vs parent +0.6% → **DIVERGE**; Renewable utilities: -1.7% 1w vs parent +0.6% → **DIVERGE**
- **Commodities Metals** — Gold: -5.1% 1w vs parent -2.1% → AGREE; Silver: +0.2% 1w vs parent -2.1% → **DIVERGE**; Copper: -2.6% 1w vs parent -2.1% → AGREE; Other precious: -3.6% 1w vs parent -2.1% → AGREE
- **Semiconductors** — Semis: +3.9% 1w vs parent +3.2% → AGREE; Semi equipment: -3.5% 1w vs parent +3.2% → **DIVERGE**
- **Artificial Intelligence** — AI compute / semis: +3.9% 1w vs parent +3.2% → AGREE; Software infra: +5.2% 1w vs parent +3.2% → AGREE
- **Defense & Aerospace** — Aero / defense: +0.8% 1w vs parent -1.2% → **DIVERGE**

### Theme ETF tape (biggest |1w| moves)

| Theme | 1d | 1w | Leaders |
|-------|---:|---:|---------|
| Consumer Staples | -0.6% | -2.6% | XLP, VDC, FSTA |
| Real Estate | -0.7% | -2.5% | VNQ, SCHH, XLRE |
| Materials | -0.8% | -2.3% | GDX, GDXJ, XLB |
| Space Exploration & Technology | -0.6% | -2.2% | NASA, ARKX, UFO |
| Healthcare | -0.3% | -2.2% | XLV, VHT, XBI |
| Robotics & Automation | +0.1% | +2.0% | BAI, AIQ, QTUM |
| Industrials | -1.0% | -2.0% | XLI, ITA, AIRR |
| Consumer Discretionary | -0.9% | -2.0% | XLY, VCR, TSLL |
| Technology | +0.2% | +1.9% | VGT, XLK, SMH |
| Utilities | -0.8% | -1.7% | XLU, VPU, FUTY |
| Cannabis Based Businesses | -0.8% | +1.6% | MSOS, MJ, CNBS |
| Metaverse and Web3 | -0.1% | +1.3% | METV, FMET, GAMR |

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
| Consumer Cyclical | -0.56 |
| Basic Materials | -0.55 |
| Real Estate | -0.55 |
| Healthcare | -0.52 |
| Energy | +0.48 |
| Industrials | -0.28 |
| Technology | -0.28 |
| Communication Services | -0.26 |
| Financial | -0.26 |
| Utilities | -0.26 |
| Consumer Defensive | +0.00 |

### How much each predictor is trusted (graded hit rate)

| Topic | hit rate | n | weight |
|-------|----------|---|--------|
| general | 48% | 21 | ×0.85 |
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

## Horizon weights — book_policy.json v6

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.12 | 0.10 | 0.08 | 0.25 | 0.25 | 0.20 | additive |
| 3d | 0.16 | 0.14 | 0.08 | 0.16 | 0.26 | 0.20 | additive |
| 1w | 0.18 | 0.16 | 0.08 | 0.10 | 0.28 | 0.20 | additive |
| 2w | 0.20 | 0.18 | 0.08 | 0.06 | 0.28 | 0.20 | additive |
| 1m | 0.22 | 0.20 | 0.08 | 0.00 | 0.30 | 0.20 | additive |

## 1d BUY — why these names

### 1. CRM · $210.3B mega · Technology

**1d score +0.169**

**CRM** is a liquid **mega-cap** Technology name (Software - Application) at $210.3B, ADV ~15405k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | -0.14 | -0.016 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | -0.08 | -0.008 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.23 | -0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.46 | +0.115 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.95 | +0.239 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.99 | +0.199 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.34 | -0.340 | liquid small/mid, room to run |
| **1d total** | | | **+0.169** | |

### 2. KMI · $72.7B large · Energy

**1d score +0.453**

**KMI** is a liquid **large-cap** Energy name (Oil & Gas Midstream) at $72.7B, ADV ~10129k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.28 | +0.028 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.95 | +0.239 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.37 | +0.074 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1d total** | | | **+0.453** | |

### 3. FTI · $31.2B large · Energy

**1d score +0.319**

**FTI** is a liquid **large-cap** Energy name (Oil & Gas Equipment & Services) at $31.2B, ADV ~3768k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.28 | +0.028 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.96 | +0.241 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.20 | +0.040 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.017 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.319** | |

### 4. CNR · $5.1B mid · Energy

**1d score +0.666**

**CNR** is a liquid **mid-cap** Energy name (Thermal Coal) at $5.1B, ADV ~610k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.98 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.28 | +0.028 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.93 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.22 | +0.045 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.666** | |

### 5. DK · $4.6B mid · Energy

**1d score +0.748**

**DK** is a liquid **mid-cap** Energy name (Oil & Gas Refining & Marketing) at $4.6B, ADV ~1315k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.96 | +0.115 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.28 | +0.028 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.95 | +0.239 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.61 | +0.123 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.748** | |

### 6. INVX · $2.1B mid · Energy

**1d score +0.634**

**INVX** is a liquid **mid-cap** Energy name (Oil & Gas Equipment & Services) at $2.1B, ADV ~554k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.94 | +0.113 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.28 | +0.028 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.23 | -0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.91 | +0.226 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.09 | +0.018 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.017 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.634** | |

### 7. OXY · $60.9B large · Energy

**1d score +0.642**

**OXY** is a liquid **large-cap** Energy name (Oil & Gas E&P) at $60.9B, ADV ~8961k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.28 | +0.028 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.83 | +0.208 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.94 | +0.235 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.29 | +0.058 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1d total** | | | **+0.642** | |

### 8. DOCS · $4.8B mid · Healthcare

**1d score +0.492**

**DOCS** is a liquid **mid-cap** Healthcare name (Health Information Services) at $4.8B, ADV ~4695k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.93 | +0.111 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | -0.52 | -0.052 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.23 | -0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.83 | +0.166 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.023 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1d total** | | | **+0.492** | |

### 9. DHT · $3.2B mid · Energy

**1d score +0.705**

**DHT** is a liquid **mid-cap** Energy name (Oil & Gas Midstream) at $3.2B, ADV ~3268k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.28 | +0.028 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.95 | +0.239 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.38 | +0.076 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.705** | |

### 10. LNG · $60.8B large · Energy

**1d score +0.314**

**LNG** is a liquid **large-cap** Energy name (Oil & Gas Midstream) at $60.8B, ADV ~1908k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.98 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.28 | +0.028 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.88 | +0.220 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.37 | +0.074 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.314** | |


## 1d AVOID — bottom of the same rank

- **JBLU** (small, Industrials, $1.7B) score -0.449. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.3%
- **RKT** (large, Financial, $37.2B) score -0.272. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.8%
- **ARKO** (small, Consumer Cyclical, $505M) score -0.183. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -5.9%
- **CSIQ** (small, Technology, $832M) score -0.366. SELL/AVOID — market=HARD_RED; red domains=child,setup,flow; child lags parent -6.4%
- **OPEN** (mid, Real Estate, $3.1B) score -0.614. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow
- **LWLG** (small, Basic Materials, $820M) score -0.581. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow
- **SLQT** (micro, Financial, $99M) score -0.538. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -4.4%
- **SMR** (mid, Industrials, $3.9B) score -0.362. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow
- **PLTK** (small, Communication Services, $854M) score -0.439. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow
- **CRML** (small, Basic Materials, $1.0B) score -0.578. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | DK | +0.806 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | KMI | +0.512 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | BTE | +0.760 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 4 | MTDR | +0.759 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | DK | +0.846 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | KMI | +0.552 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | BTE | +0.800 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 4 | MTDR | +0.799 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | DK | +0.790 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | KMI | +0.497 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | BTE | +0.745 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 4 | MTDR | +0.744 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 1m BUY — why these names

### 1. DK · $4.6B mid · Energy

**1m score +0.915**

**DK** is a liquid **mid-cap** Energy name (Oil & Gas Refining & Marketing) at $4.6B, ADV ~1315k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.96 | +0.211 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.22 | +0.045 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.95 | +0.286 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.61 | +0.123 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.915** | |

### 2. KMI · $72.7B large · Energy

**1m score +0.622**

**KMI** is a liquid **large-cap** Energy name (Oil & Gas Midstream) at $72.7B, ADV ~10129k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.22 | +0.045 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.95 | +0.286 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.37 | +0.074 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1m total** | | | **+0.622** | |

### 3. BTE · $3.4B mid · Energy

**1m score +0.869**

**BTE** is a liquid **mid-cap** Energy name (Oil & Gas E&P) at $3.4B, ADV ~17324k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.97 | +0.213 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.22 | +0.045 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.94 | +0.282 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.39 | +0.078 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.869** | |

### 4. MTDR · $7.3B mid · Energy

**1m score +0.869**

**MTDR** is a liquid **mid-cap** Energy name (Oil & Gas E&P) at $7.3B, ADV ~1958k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.22 | +0.045 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.95 | +0.286 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.35 | +0.071 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.869** | |


## 1m AVOID — bottom of the same rank

- **OPEN** (mid, Real Estate, $3.1B) score -0.768. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **REAX** (small, Real Estate, $424M) score -0.725. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **HPP** (small, Real Estate, $697M) score -0.704. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CMTG** (micro, Real Estate, $224M) score -0.692. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NRGV** (small, Utilities, $627M) score -0.678. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RCAT** (small, Industrials, $1.3B) score -0.671. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **MBC** (small, Consumer Cyclical, $1.7B) score -0.655. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LUNR** (mid, Industrials, $2.6B) score -0.651. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LWLG** (small, Basic Materials, $820M) score -0.649. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OKLO** (mid, Utilities, $7.4B) score -0.648. this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**
- **DHC** (small, Real Estate, $1.8B) score -0.648. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CRML** (small, Basic Materials, $1.0B) score -0.648. this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PLAY** (small, Communication Services, $312M) score -0.642. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RDW** (mid, Industrials, $2.6B) score -0.641. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SATL** (small, Industrials, $735M) score -0.628. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **XHR** (small, Real Estate, $1.7B) score -0.626. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RKLB** (large, Industrials, $37.6B) score -0.622. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BKSY** (small, Industrials, $950M) score -0.620. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FIP** (small, Industrials, $411M) score -0.617. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FCEL** (small, Industrials, $1.4B) score -0.617. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **FUN** (small, Consumer Cyclical, $1.6B) score -0.616. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **EOSE** (small, Industrials, $1.2B) score -0.613. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TE** (small, Industrials, $1.3B) score -0.610. this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SPIR** (small, Industrials, $528M) score -0.606. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RC** (micro, Real Estate, $292M) score -0.605. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-09-01_stock_book.md`
- Machine table: `data/stock_book/2026-09-01_stock_book.csv`
- Machine book: `data/stock_book/2026-09-01_stock_book.json`
- Join rank: `data/join/2026-09-01_ranked.csv`
- Weather: `01_daily/weather/2026-09-01_weather.md`
- AB enrich: `data/ab_checklist/2026-09-01_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-09-01_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-09-01_map_heat.md`
