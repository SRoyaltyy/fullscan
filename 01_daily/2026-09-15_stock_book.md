# Stock book — 2026-09-15

_Generated 2026-09-15T14:12:57.034453-04:00_

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
- General predict (same-day): -0.46 down (present)
- Stand-down: **no** — 248 names qualified through catalyst_exception,probable (248 probable)
- Sector predicts this date: 11/11 (ok)
- News tickers in play: 95
- AB coverage: 1952 names · peer RS: 1847
- Universe after liquidity: 2077
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 41

## All-green BUY / SELL

- Mode: **weighted_fallback** · SELL **core_weights**
- Pile: **0** liquid all-green names (need ≥ 8) of 2077
- Core fired: join=yes, AB=yes, peer=yes
- pile 0 < 8 liquid all-green names. Fallback weighted walk; SELL stays on core

## Decision lattice — gate → route → rank

The weighted score is now a tie-breaker inside an eligible lane. It cannot average away a market, group, company, or setup veto.

### MARKET: 🔴 HARD_RED

- HARD_RED: general down score=-3.65; good=+0.0 vs bad=-6.5; risk=off; red pillars=4
- Allowed long lanes: **catalyst_exception, probable** · max slots 10 · size ×0.25
- Bear evidence: overnight catalysts -3.00 points; rates / Fed -2.00 points; global sessions -1.00 points; oil / dollar -0.50 points

Decision domains: **MKT · parent · child · company · setup · flow**. Measured parent/child tape is kept separate from the LLM essay; direct company events must be price-confirmed on a hard-red day.

### Bull decisions (eligible or closest blocked cases)

| # | Ticker | Domains | Lane | Company / group | Decision |
|---:|--------|---------|------|-----------------|----------|
| 1 | **NMAX** | 🔴🟢🟡🟡🟢🟡 | probable | usable dossier Strong Bullish conv=70; Broadcasting +0.6% d1 / -1.1% 1w / -4.6% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=YELLOW/rel=RED; company=YELLOW(0.80); setup=GREEN; flow=YELLOW; lookback=🔵 |
| 2 | **CDNS** | 🔴🔴🟡🟢🟢🟡 | probable | direct high digest (same-day): Cadence beats Q2 2026 estimates with EPS $2.11, revenue $1.6B, signs major Intel deal underpinning raised 2026 AI-driven outlook; Software - Application +3.8% d1 / -1.1% 1w / +0.9% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: company news fresh (0.72); lookback 🔵 blue — market=HARD_RED; parent=RED; child=YELLOW/rel=YELLOW; company=GREEN(0.72); setup=GREEN; flow=YELLOW; lookback=🔵 |
| 3 | **AMGN** | 🔴🔴🟡🟢🟢🔴 | probable | direct high digest (same-day): Amgen gets FDA approval to update IMDELLTRA label to reduce monitoring for first two ES-SCLC doses; Drug Manufacturers - General +1.4% d1 / -3.1% 1w / -0.7% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: company news fresh (0.72); lookback 🔵 blue — market=HARD_RED; parent=RED; child=YELLOW/rel=YELLOW; company=GREEN(0.72); setup=GREEN; flow=RED; lookback=🔵 |
| 4 | **MPC** | 🔴🟢🟡🟢🟢🟡 | blocked | usable dossier Bullish conv=34; Oil & Gas Refining & Marketing -1.1% d1 / +1.9% 1w / +0.9% vs parent | BLOCK BUY — HARD_RED: no company / child-outperform / lookback clock; 🚨 alarm; featured fade / market=HARD_RED; parent=GREEN; child=YELLOW/rel=YELLOW; company=GREEN(0.80); setup=GREEN; flow=YELLOW |
| 5 | **CSTM** | 🔴🔴🔴🟢🟡🟡 | blocked | usable dossier Bullish conv=26; Aluminum -3.6% d1 / -7.5% 1w / -2.7% vs parent | BLOCK BUY — HARD_RED: no company / child-outperform / lookback clock; parent sector RED; child industry/theme RED; setup YELLOW; 🚨 alarm; featured fade; legacy Cond red; legacy region red; v2 domain region red / market=HARD_RED; parent=RED; child=RED/rel=YELLOW; company=GREEN(0.80); setup=YELLOW; flow=YELLOW |
| 6 | **DBX** | 🔴🔴🟢🟡🟢🟢 | probable | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +1.4% 1w / +3.4% rel; lookback 🔵 blue — market=HARD_RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 7 | **FFIV** | 🔴🔴🟢🟡🟢🟢 | probable | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +1.4% 1w / +3.4% rel; lookback 🔵 blue — market=HARD_RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 8 | **BJ** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Discount Stores +1.8% d1 / +0.8% 1w / +0.3% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 9 | **SAIC** | 🔴🔴🟢🟡🟢🟢 | probable | no direct company event; Information Technology Services +2.9% d1 / +1.9% 1w / +3.9% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +1.9% 1w / +3.9% rel; lookback 🔵 blue — market=HARD_RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 10 | **APPN** | 🔴🔴🟢🟡🟢🟢 | probable | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +1.4% 1w / +3.4% rel; lookback 🔵 blue — market=HARD_RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 11 | **BAH** | 🔴🔴🟢🟡🟢🟢 | probable | no direct company event; Consulting Services +3.9% d1 / +0.6% 1w / +3.4% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +0.6% 1w / +3.4% rel; lookback 🔵 blue — market=HARD_RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 12 | **META** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Internet Content & Information +3.0% d1 / +4.2% 1w / +0.8% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 13 | **SNAP** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Internet Content & Information +3.0% d1 / +4.2% 1w / +0.8% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 14 | **CRWD** | 🔴🔴🟢🟡🟢🟢 | probable | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +1.4% 1w / +3.4% rel; lookback 🔵 blue — market=HARD_RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 15 | **TWLO** | 🔴🔴🟢🟡🟢🟢 | probable | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +1.4% 1w / +3.4% rel; lookback 🔵 blue — market=HARD_RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **METC** | 🔴🔴🔴🟡🔴🔴 | Coking Coal | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -5.7% |
| 2 | **FLNC** | 🔴🔴🔴🟡🔴🔴 | Utilities - Renewable | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.1% |
| 3 | **HNRG** | 🔴🔴🔴🟡🔴🔴 | Utilities - Independent Power Producers | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -6.3% |
| 4 | **OKLO** | 🔴🔴🔴🟡🔴🔴 | Utilities - Independent Power Producers | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -6.3% |
| 5 | **TLN** | 🔴🔴🔴🟡🔴🔴 | Utilities - Independent Power Producers | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -6.3% |
| 6 | **NRG** | 🔴🔴🔴🟡🔴🔴 | Utilities - Independent Power Producers | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -6.3% |
| 7 | **FCEL** | 🔴🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 8 | **LVWR** | 🔴🔴🔴🟡🔴🔴 | Recreational Vehicles | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -5.5% |
| 9 | **TE** | 🔴🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 10 | **AG** | 🔴🔴🔴🟡🔴🔴 | Silver | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.5% |
| 11 | **FUN** | 🔴🔴🔴🟡🔴🔴 | Leisure | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 12 | **QBTS** | 🔴🔴🔴🟡🔴🔴 | Computer Hardware | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.1% |
| 13 | **DPRO** | 🔴🔴🔴🟡🔴🔴 | Computer Hardware | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.1% |
| 14 | **ASPI** | 🔴🔴🔴🟡🔴🔴 | Specialty Chemicals | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 15 | **PRKS** | 🔴🔴🔴🟡🔴🔴 | Leisure | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **captain_research** (257 captains, 10 industries → s_heat).
- Board file: `01_daily/map_heat/2026-09-15_map_heat.json` · generated 2026-09-15T04:25:19.101684-04:00

### Sector RS vs same-day LLM essay

| Sector | Finviz 1d | Finviz 1w | LLM 1d | Tape vs essay |
|--------|----------:|----------:|-------:|---------------|
| Basic Materials | -2.1% | -4.8% | -0.65 |  |
| Communication Services | +2.7% | +3.5% | +0.00 | essay flat, tape moving |
| Consumer Cyclical | -0.4% | -2.0% | -0.63 |  |
| Consumer Defensive | +1.4% | +0.5% | +0.00 |  |
| Energy | -0.8% | +1.0% | +0.33 |  |
| Financial | -0.4% | -1.8% | -0.28 |  |
| Healthcare | +1.4% | -2.5% | -0.52 |  |
| Industrials | -1.6% | -2.7% | -0.21 |  |
| Real Estate | -0.6% | -2.1% | -0.55 |  |
| Technology | -2.0% | -2.1% | -0.22 |  |
| Utilities | -1.5% | -3.2% | -0.33 |  |

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
| Basic Materials | -0.65 |
| Consumer Cyclical | -0.63 |
| Real Estate | -0.55 |
| Healthcare | -0.52 |
| Energy | +0.33 |
| Utilities | -0.33 |
| Financial | -0.28 |
| Technology | -0.22 |
| Industrials | -0.21 |
| Communication Services | +0.00 |
| Consumer Defensive | +0.00 |

### How much each predictor is trusted (graded hit rate)

| Topic | hit rate | n | weight |
|-------|----------|---|--------|
| general | 50% | 30 | ×0.85 |
| sector:Basic Materials | 58% | 19 | ×1.00 |
| sector:Communication Services | 28% | 18 | ×0.50 |
| sector:Consumer Cyclical | 58% | 19 | ×1.00 |
| sector:Consumer Defensive | 47% | 19 | ×0.85 |
| sector:Energy | 42% | 19 | ×0.50 |
| sector:Financial | 42% | 19 | ×0.50 |
| sector:Healthcare | 56% | 16 | ×1.00 |
| sector:Industrials | 32% | 19 | ×0.50 |
| sector:Real Estate | 53% | 19 | ×0.85 |
| sector:Technology | 44% | 18 | ×0.50 |
| sector:Utilities | 44% | 18 | ×0.50 |

## Horizon weights — book_policy.json v13

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.12 | 0.10 | 0.08 | 0.25 | 0.25 | 0.20 | additive |
| 3d | 0.16 | 0.14 | 0.08 | 0.16 | 0.26 | 0.20 | additive |
| 1w | 0.18 | 0.16 | 0.08 | 0.10 | 0.28 | 0.20 | additive |
| 2w | 0.20 | 0.18 | 0.08 | 0.06 | 0.28 | 0.20 | additive |
| 1m | 0.22 | 0.20 | 0.08 | 0.00 | 0.30 | 0.20 | additive |

## 1d BUY — why these names

### 1. DBX · $8.2B mid · Technology

**1d score +0.670**

**DBX** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $8.2B, ADV ~3961k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.84 | +0.101 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | -0.02 | -0.002 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.64 | +0.159 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.84 | +0.168 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.670** | |

### 2. APPN · $2.8B mid · Technology

**1d score +0.621**

**APPN** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $2.8B, ADV ~879k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.95 | +0.114 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | -0.02 | -0.002 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.23 | -0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.46 | +0.116 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.81 | +0.162 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.621** | |

### 3. META · $1707.7B mega · Communication Services

**1d score +0.100**

**META** is a liquid **mega-cap** Communication Services name (Internet Content & Information) at $1707.7B, ADV ~18279k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **miss**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | -0.04 | -0.004 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.23 | -0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.46 | +0.116 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.74 | +0.148 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.14 | -0.140 | liquid small/mid, room to run |
| **1d total** | | | **+0.100** | |

### 4. SNAP · $10.0B mid · Communication Services

**1d score +0.476**

**SNAP** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $10.0B, ADV ~40299k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | -0.15 | -0.018 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.23 | -0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.46 | +0.116 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.74 | +0.147 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.476** | |


## 1d AVOID — bottom of the same rank

- **METC** (small, Basic Materials, $580M) score -0.649. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -5.7%
- **FLNC** (small, Utilities, $1.7B) score -0.602. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.1%
- **HNRG** (small, Utilities, $664M) score -0.197. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -6.3%
- **OKLO** (mid, Utilities, $6.6B) score -0.466. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -6.3%
- **TLN** (large, Utilities, $13.6B) score -0.605. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -6.3%
- **NRG** (large, Utilities, $22.5B) score -0.369. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -6.3%
- **FCEL** (small, Industrials, $1.2B) score -0.528. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.7%
- **LVWR** (micro, Consumer Cyclical, $222M) score -0.551. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -5.5%
- **TE** (small, Industrials, $1.2B) score -0.399. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.7%
- **AG** (mid, Basic Materials, $9.0B) score -0.475. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.5%

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | S | +0.731 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 2 | DBX | +0.710 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 3 | RPD | +0.699 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 4 | DSGX | +0.692 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 5 | ECO | +0.552 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | ICLR | +0.482 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 7 | WAY | +0.482 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 8 | SNAP | +0.474 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 9 | RVTY | +0.459 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | CLOV | +0.442 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | NMAX | +0.436 | small | Communication Services | the Finviz industry was **advancing** |
| 12 | RDDT | +0.376 | large | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 13 | NXDR | +0.372 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 14 | COCO | +0.347 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 15 | BWIN | +0.227 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 16 | AFRM | +0.201 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 17 | HAFN | +0.189 | mid | Industrials | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 18 | PRG | +0.175 | small | Industrials | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 19 | SIG | +0.089 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | S | +0.763 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 2 | DBX | +0.739 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 3 | DSGX | +0.730 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 4 | RPD | +0.729 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 5 | ECO | +0.581 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | ICLR | +0.506 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 7 | WAY | +0.505 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 8 | RVTY | +0.484 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | SNAP | +0.482 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 10 | CLOV | +0.468 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | NMAX | +0.450 | small | Communication Services | the Finviz industry was **advancing** |
| 12 | RDDT | +0.392 | large | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 13 | ANGX | +0.381 | small | Communication Services | the Finviz industry was **advancing** |
| 14 | COCO | +0.351 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 15 | BWIN | +0.229 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 16 | AFRM | +0.214 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 17 | HAFN | +0.204 | mid | Industrials | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 18 | PRG | +0.200 | small | Industrials | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 19 | SIG | +0.075 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | S | +0.786 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 2 | RPD | +0.763 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 3 | DBX | +0.761 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 4 | DSGX | +0.754 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 5 | ECO | +0.598 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | ICLR | +0.531 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 7 | WAY | +0.520 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 8 | CLOV | +0.512 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | RVTY | +0.508 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | SNAP | +0.496 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 11 | NMAX | +0.482 | small | Communication Services | the Finviz industry was **advancing** |
| 12 | RDDT | +0.429 | large | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 13 | NXDR | +0.408 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 14 | COCO | +0.351 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 15 | AFRM | +0.285 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 16 | BWIN | +0.279 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 17 | PRG | +0.243 | small | Industrials | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 18 | HAFN | +0.217 | mid | Industrials | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 19 | SIG | +0.081 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1m BUY — why these names

### 1. S · $7.9B mid · Technology

**1m score +0.813**

**S** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $7.9B, ADV ~7407k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.92 | +0.203 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.02 | -0.004 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.211 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.79 | +0.159 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.813** | |

### 2. DSGX · $6.8B mid · Technology

**1m score +0.787**

**DSGX** is a liquid **mid-cap** Technology name (Software - Application) at $6.8B, ADV ~593k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.95 | +0.210 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.02 | -0.004 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.96 | +0.289 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.34 | +0.068 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.02 | -0.021 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.787** | |

### 3. DBX · $8.2B mid · Technology

**1m score +0.785**

**DBX** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $8.2B, ADV ~3961k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.84 | +0.186 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.02 | -0.004 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.64 | +0.191 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.84 | +0.168 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.785** | |

### 4. RPD · $834M small · Technology

**1m score +0.776**

**RPD** is a liquid **small-cap** Technology name (Software - Infrastructure) at $834M, ADV ~2438k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.83 | +0.183 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.02 | -0.004 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.20 | -0.016 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.64 | +0.191 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.86 | +0.173 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.776** | |

### 5. ECO · $3.2B mid · Industrials

**1m score +0.622**

**ECO** is a liquid **mid-cap** Industrials name (Marine Shipping) at $3.2B, ADV ~519k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.98 | +0.215 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.41 | -0.082 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.90 | +0.180 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.622** | |

### 6. WAY · $5.1B mid · Healthcare

**1m score +0.539**

**WAY** is a liquid **mid-cap** Healthcare name (Health Information Services) at $5.1B, ADV ~2692k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.98 | +0.216 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.52 | -0.104 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.211 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.85 | +0.170 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.539** | |

### 7. ICLR · $13.2B large · Healthcare

**1m score +0.538**

**ICLR** is a liquid **large-cap** Healthcare name (Diagnostics & Research) at $13.2B, ADV ~821k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.85 | +0.187 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.52 | -0.104 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.20 | -0.016 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.83 | +0.167 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.538** | |

### 8. RVTY · $15.3B large · Healthcare

**1m score +0.516**

**RVTY** is a liquid **large-cap** Healthcare name (Diagnostics & Research) at $15.3B, ADV ~1541k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.82 | +0.180 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.52 | -0.104 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.20 | -0.016 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.67 | +0.134 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.516** | |

### 9. CLOV · $2.5B mid · Healthcare

**1m score +0.503**

**CLOV** is a liquid **mid-cap** Healthcare name (Healthcare Plans) at $2.5B, ADV ~5096k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.94 | +0.207 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.52 | -0.104 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.41 | -0.033 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.77 | +0.154 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.503** | |

### 10. SNAP · $10.0B mid · Communication Services

**1m score +0.486**

**SNAP** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $10.0B, ADV ~40299k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.15 | -0.033 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.20 | -0.016 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.46 | +0.139 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.74 | +0.147 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.486** | |

### 11. NMAX · $1.4B small · Communication Services

**1m score +0.462**

**NMAX** is a liquid **small-cap** Communication Services name (Broadcasting) at $1.4B, ADV ~1131k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.15 | -0.033 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.41 | -0.033 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.05 | +0.049 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.462** | |

### 12. RDDT · $30.7B large · Communication Services

**1m score +0.410**

**RDDT** is a liquid **large-cap** Communication Services name (Internet Content & Information) at $30.7B, ADV ~5745k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.07 | +0.015 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.41 | -0.033 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.64 | +0.191 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.59 | +0.117 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.07 | +0.070 | liquid small/mid, room to run |
| **1m total** | | | **+0.410** | |

### 13. WMG · $15.1B large · Communication Services

**1m score +0.395**

**WMG** is a liquid **large-cap** Communication Services name (Entertainment) at $15.1B, ADV ~2056k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.30 | +0.066 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.41 | -0.033 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.19 | +0.039 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1m total** | | | **+0.395** | |

### 14. COCO · $3.1B mid · Consumer Defensive

**1m score +0.351**

**COCO** is a liquid **mid-cap** Consumer Defensive name (Beverages - Non-Alcoholic) at $3.1B, ADV ~1167k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.05 | -0.012 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.46 | +0.139 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.34 | +0.069 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.351** | |

### 15. BWIN · $4.5B mid · Financial

**1m score +0.266**

**BWIN** is a liquid **mid-cap** Financial name (Insurance Brokers) at $4.5B, ADV ~1767k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.11 | -0.023 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.20 | -0.016 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.46 | +0.139 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.79 | +0.157 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.266** | |

### 16. PRG · $1.5B small · Industrials

**1m score +0.234**

**PRG** is a liquid **small-cap** Industrials name (Rental & Leasing Services) at $1.5B, ADV ~571k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.80 | +0.176 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.41 | -0.082 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.41 | -0.033 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | -0.53 | -0.106 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.234** | |

### 17. HAFN · $4.7B mid · Industrials

**1m score +0.228**

**HAFN** is a liquid **mid-cap** Industrials name (Marine Shipping) at $4.7B, ADV ~1598k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **miss**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.81 | +0.178 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.41 | -0.082 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.36 | +0.108 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | -0.11 | -0.021 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.228** | |

### 18. SIG · $3.9B mid · Consumer Cyclical

**1m score +0.050**

**SIG** is a liquid **mid-cap** Consumer Cyclical name (Luxury Goods) at $3.9B, ADV ~851k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.22 | +0.048 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.83 | -0.167 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.20 | -0.016 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | -0.12 | -0.037 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +1.00 | +0.199 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.03 | -0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.050** | |


## 1m AVOID — bottom of the same rank

- **METC** (small, Basic Materials, $580M) score -0.874. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FUN** (small, Consumer Cyclical, $1.3B) score -0.857. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TMC** (small, Basic Materials, $1.6B) score -0.840. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ASPI** (small, Basic Materials, $471M) score -0.834. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OPEN** (mid, Real Estate, $2.6B) score -0.813. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **UAMY** (small, Basic Materials, $682M) score -0.800. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FLNC** (small, Utilities, $1.7B) score -0.795. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LODE** (micro, Basic Materials, $193M) score -0.792. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SGML** (small, Basic Materials, $1.0B) score -0.791. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TLN** (large, Utilities, $13.6B) score -0.791. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LWLG** (small, Basic Materials, $720M) score -0.787. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ORBS** (small, Consumer Cyclical, $383M) score -0.776. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NG** (mid, Basic Materials, $3.1B) score -0.768. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AIIO** (micro, Consumer Cyclical, $186M) score -0.748. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CWH** (small, Consumer Cyclical, $614M) score -0.748. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LVWR** (micro, Consumer Cyclical, $222M) score -0.745. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LI** (mid, Consumer Cyclical, $9.4B) score -0.744. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **HYMC** (small, Basic Materials, $1.8B) score -0.737. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LAR** (small, Basic Materials, $905M) score -0.736. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SVV** (small, Consumer Cyclical, $1.4B) score -0.734. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **USAR** (mid, Basic Materials, $5.7B) score -0.733. the Finviz industry was **down**
- **LCID** (small, Consumer Cyclical, $1.6B) score -0.732. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NB** (small, Basic Materials, $513M) score -0.730. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TROX** (small, Basic Materials, $705M) score -0.730. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LKQ** (mid, Consumer Cyclical, $6.0B) score -0.726. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-09-15_stock_book.md`
- Machine table: `data/stock_book/2026-09-15_stock_book.csv`
- Machine book: `data/stock_book/2026-09-15_stock_book.json`
- Join rank: `data/join/2026-09-15_ranked.csv`
- Weather: `01_daily/weather/2026-09-15_weather.md`
- AB enrich: `data/ab_checklist/2026-09-15_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-09-15_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-09-15_map_heat.md`
