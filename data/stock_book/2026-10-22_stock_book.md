# Stock book — 2026-10-22

_Generated 2026-10-22T20:14:49.809063-04:00_

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
- General predict (same-day): -0.72 down (present)
- Stand-down: **no** — 1 names qualified through catalyst_exception,probable (1 probable)
- Sector predicts this date: 0/11 (missing → sector layer is 0; Finviz week tape still sits in join)
- News tickers in play: 161
- AB coverage: 1920 names · peer RS: 1804
- Universe after liquidity: 2035
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 83

## All-green BUY / SELL

- Mode: **weighted_fallback** · SELL **core_weights**
- Pile: **0** liquid all-green names (need ≥ 8) of 2035
- Core fired: join=yes, AB=yes, peer=yes
- pile 0 < 8 liquid all-green names. Fallback weighted walk; SELL stays on core

## Decision lattice — gate → route → rank

The weighted score is now a tie-breaker inside an eligible lane. It cannot average away a market, group, company, or setup veto.

### MARKET: 🔴 HARD_RED

- HARD_RED: general down score=-14.31; good=+1.0 vs bad=-16.8; risk=off; red pillars=5
- Allowed long lanes: **catalyst_exception, probable** · max slots 10 · size ×0.25
- Bull evidence: rates / Fed +1.00 points
- Bear evidence: overnight catalysts -9.00 points; global sessions -6.00 points; oil / dollar -1.00 points; sentiment -0.50 points; futures -0.25 points

Decision domains: **MKT · parent · child · company · setup · flow**. Measured parent/child tape is kept separate from the LLM essay; direct company events must be price-confirmed on a hard-red day.

### Bull decisions (eligible or closest blocked cases)

| # | Ticker | Domains | Lane | Company / group | Decision |
|---:|--------|---------|------|-----------------|----------|
| 1 | **NET** | 🔴🔴🟢🟡🟢🟢 | probable | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +1.4% 1w / +3.4% rel — market=HARD_RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 2 | **HITI** | 🔴🟡🟢🟡🟢🟢 | blocked | no direct company event; Pharmaceutical Retailers +3.6% d1 / +1.0% 1w / +3.5% vs parent | BLOCK BUY — HARD_RED: no company / child-outperform / lookback clock; v2 domain alarm / market=HARD_RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 3 | **OKTA** | 🔴🔴🟢🟡🟢🟢 | blocked | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BLOCK BUY — HARD_RED: no company / child-outperform / lookback clock; parent sector RED; v2 domain alarm / market=HARD_RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 4 | **IMAX** | 🔴🟢🟢🟡🟢🟢 | blocked | no direct company event; Entertainment +2.5% d1 / +2.0% 1w / -1.5% vs parent | BLOCK BUY — HARD_RED: no company / child-outperform / lookback clock; v2 domain alarm / market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 5 | **PACS** | 🔴🟡🟢🟡🟢🟡 | blocked | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BLOCK BUY — HARD_RED: no company / child-outperform / lookback clock; v2 domain alarm / market=HARD_RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW |
| 6 | **OLLI** | 🔴🟢🟢🟡🟢🟢 | blocked | no direct company event; Discount Stores +1.8% d1 / +0.8% 1w / +0.3% vs parent | BLOCK BUY — HARD_RED: no company / child-outperform / lookback clock; v2 domain alarm / market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 7 | **BJ** | 🔴🟢🟢🟡🟢🟢 | blocked | no direct company event; Discount Stores +1.8% d1 / +0.8% 1w / +0.3% vs parent | BLOCK BUY — HARD_RED: no company / child-outperform / lookback clock; v2 domain alarm / market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 8 | **SPOT** | 🔴🟢🟢🟡🟢🟢 | blocked | no direct company event; Internet Content & Information +3.0% d1 / +4.2% 1w / +0.8% vs parent | BLOCK BUY — HARD_RED: no company / child-outperform / lookback clock; v2 domain alarm / market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 9 | **WLY** | 🔴🟢🟢🟡🟢🟢 | blocked | no direct company event; Publishing +3.8% d1 / +2.5% 1w / -1.0% vs parent | BLOCK BUY — HARD_RED: no company / child-outperform / lookback clock; v2 domain alarm / market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 10 | **ATHM** | 🔴🟢🟢🟡🟢🟢 | blocked | no direct company event; Internet Content & Information +3.0% d1 / +4.2% 1w / +0.8% vs parent | BLOCK BUY — HARD_RED: no company / child-outperform / lookback clock; v2 domain alarm / market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 11 | **GDYN** | 🔴🔴🟢🟡🟢🟢 | blocked | no direct company event; Information Technology Services +2.9% d1 / +1.9% 1w / +3.9% vs parent | BLOCK BUY — HARD_RED: no company / child-outperform / lookback clock; parent sector RED; v2 domain alarm / market=HARD_RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 12 | **KSPI** | 🔴🔴🟢🟡🟢🟢 | blocked | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BLOCK BUY — HARD_RED: no company / child-outperform / lookback clock; parent sector RED; v2 domain alarm / market=HARD_RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 13 | **TMUS** | 🔴🟢🟢🟡🟢🟢 | blocked | no direct company event; Telecom Services +0.6% d1 / +0.7% 1w / -2.7% vs parent | BLOCK BUY — HARD_RED: no company / child-outperform / lookback clock / market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 14 | **UHS** | 🔴🟡🟢🟡🟡🟢 | blocked | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BLOCK BUY — HARD_RED: no company / child-outperform / lookback clock; setup YELLOW; v2 domain alarm / market=HARD_RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=YELLOW; flow=GREEN |
| 15 | **BLSH** | 🔴🔴🟢🟡🟢🟢 | blocked | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BLOCK BUY — HARD_RED: no company / child-outperform / lookback clock; parent sector RED; v2 domain alarm / market=HARD_RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **NEOV** | 🔴🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 2 | **OKLO** | 🔴🔴🔴🟡🔴🔴 | Utilities - Independent Power Producers | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -6.3% |
| 3 | **FCEL** | 🔴🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 4 | **EOSE** | 🔴🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -4.7% |
| 5 | **NRGV** | 🔴🔴🔴🟡🔴🟡 | Utilities - Renewable | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -4.1% |
| 6 | **INDI** | 🔴🔴🔴🟡🔴🔴 | Semiconductors | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 7 | **TE** | 🔴🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -4.7% |
| 8 | **SKYX** | 🔴🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -4.7% |
| 9 | **NNDM** | 🔴🔴🔴🟡🔴🔴 | Computer Hardware | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.1% |
| 10 | **LUNR** | 🔴🔴🔴🟡🔴🔴 | Aerospace & Defense | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 11 | **AMSC** | 🔴🔴🔴🟡🔴🔴 | Specialty Industrial Machinery | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 12 | **METC** | 🔴🔴🔴🟡🔴🟡 | Coking Coal | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -5.7% |
| 13 | **HNRG** | 🔴🔴🔴🟡🔴🟡 | Utilities - Independent Power Producers | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -6.3% |
| 14 | **PLUG** | 🔴🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -4.7% |
| 15 | **MTZ** | 🔴🔴🔴🟡🔴🔴 | Engineering & Construction | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **finviz_tape** (40 captains, 15 industries → s_heat).
- Board file: `01_daily/map_heat/2026-10-22_map_heat.json` · generated 2026-10-22T04:32:02.795318-04:00

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
| General predict | **found** | s_general × beta |
| Sector LLM essays | **found** | s_sector (0 if essays missing) |
| AB checklist + P01–P04 | **found** | s_ab |
| Peer RS | **found** | s_peer |
| Ticker checklist (rebound) | **found** | rebound_floor (dated file, else latest — can be stale) |
| Event scanner | **found** | sector tilt + weather |
| Finviz map heat (industry RS / themes) | **found** | industry residual + theme tape → s_heat when research is gone |
| Map heat captain research | **missing / not in ranker** | Grok captain essays (strict morning_refresh; else Finviz tape) |
| Catalyst overlays | **missing / not in ranker** | not in ranker — separate chart workflow |
| Insider / politician flow | **missing / not in ranker** | no daily file in repo |
| Industry predict | **found** | not scored (ad-hoc only) |
| Learnings / mutable policy | **found** | next predict prompt, not a ticker score |

### Sector LLM bias (1d) — 0 / empty means that essay was not run today

| Sector | bias |
|--------|------|
| — | none today |

### How much each predictor is trusted (graded hit rate)

| Topic | hit rate | n | weight |
|-------|----------|---|--------|
| general | 52% | 52 | ×0.85 |
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

## Horizon weights — book_policy.json v15 · renormalized (absent: sector)

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.13 | 0.00 | 0.09 | 0.28 | 0.28 | 0.22 | additive |
| 3d | 0.19 | 0.00 | 0.09 | 0.19 | 0.30 | 0.23 | additive |
| 1w | 0.21 | 0.00 | 0.10 | 0.12 | 0.33 | 0.24 | additive |
| 2w | 0.24 | 0.00 | 0.10 | 0.07 | 0.34 | 0.24 | additive |
| 1m | 0.28 | 0.00 | 0.10 | 0.00 | 0.38 | 0.25 | additive |

## 1d BUY — why these names

### 1. NET · $125.7B large · Technology

**1d score -0.170**

**NET** is a liquid **large-cap** Technology name (Software - Infrastructure) at $125.7B, ADV ~3410k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather are a **headwind** (sector stamp or hostile tape).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | -0.51 | -0.068 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | -0.72 | -0.064 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.36 | +0.100 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.15 | +0.033 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **-0.170** | |


## 1d AVOID — bottom of the same rank

- **NEOV** (micro, Industrials, $133M) score -0.391. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.7%
- **OKLO** (mid, Utilities, $6.9B) score -0.181. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -6.3%
- **FCEL** (small, Industrials, $1.3B) score -0.268. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.7%
- **EOSE** (small, Industrials, $1.1B) score -0.409. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -4.7%
- **NRGV** (small, Utilities, $736M) score -0.323. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -4.1%
- **INDI** (small, Technology, $668M) score -0.348. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow
- **TE** (small, Industrials, $1.1B) score -0.386. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -4.7%
- **SKYX** (micro, Industrials, $158M) score -0.423. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -4.7%
- **NNDM** (small, Technology, $324M) score -0.169. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.1%
- **LUNR** (mid, Industrials, $3.5B) score -0.366. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | NET | -0.176 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | NET | -0.169 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | NET | -0.136 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1m BUY — why these names

### 1. NET · $125.7B large · Technology

**1m score -0.139**

**NET** is a liquid **large-cap** Technology name (Software - Infrastructure) at $125.7B, ADV ~3410k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather are a **headwind** (sector stamp or hostile tape).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | -0.51 | -0.141 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.36 | +0.134 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.15 | +0.037 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1m total** | | | **-0.139** | |


## 1m AVOID — bottom of the same rank

- **RC** (micro, Real Estate, $218M) score -0.653. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SOC** (small, Energy, $702M) score -0.647. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **PCT** (small, Industrials, $936M) score -0.643. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BAK** (micro, Basic Materials, $257M) score -0.631. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AIRS** (micro, Healthcare, $138M) score -0.627. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NRXP** (micro, Healthcare, $126M) score -0.603. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PLAY** (micro, Communication Services, $223M) score -0.602. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LFMD** (micro, Healthcare, $141M) score -0.601. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **IMNM** (mid, Healthcare, $2.5B) score -0.596. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AXSM** (large, Healthcare, $10.2B) score -0.589. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ROIV** (large, Healthcare, $26.5B) score -0.587. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PRM** (mid, Basic Materials, $4.6B) score -0.587. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ACDC** (small, Energy, $818M) score -0.581. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **EOSE** (small, Industrials, $1.1B) score -0.578. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **MIST** (micro, Healthcare, $99M) score -0.577. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BORR** (small, Energy, $1.2B) score -0.576. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CMTG** (micro, Real Estate, $179M) score -0.575. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TMC** (small, Basic Materials, $1.7B) score -0.574. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LDI** (micro, Financial, $216M) score -0.568. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ALMS** (small, Healthcare, $953M) score -0.568. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **SKYX** (micro, Industrials, $158M) score -0.564. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **PYXS** (micro, Healthcare, $242M) score -0.562. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NEOV** (micro, Industrials, $133M) score -0.559. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ASPI** (small, Basic Materials, $402M) score -0.559. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TE** (small, Industrials, $1.1B) score -0.556. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-10-22_stock_book.md`
- Machine table: `data/stock_book/2026-10-22_stock_book.csv`
- Machine book: `data/stock_book/2026-10-22_stock_book.json`
- Join rank: `data/join/2026-10-22_ranked.csv`
- Weather: `01_daily/weather/2026-10-22_weather.md`
- AB enrich: `data/ab_checklist/2026-10-22_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-10-22_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-10-22_map_heat.md`
