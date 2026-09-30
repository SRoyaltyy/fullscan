# Stock book — 2026-09-30

_Generated 2026-09-30T12:15:36.867685-04:00_

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
- General predict (same-day): +0.49 up (present)
- Stand-down: **no** — 263 names qualified through standard,group_leader,catalyst (17 probable)
- Sector predicts this date: 0/11 (missing → sector layer is 0; Finviz week tape still sits in join)
- News tickers in play: 89
- AB coverage: 1893 names · peer RS: 1804
- Universe after liquidity: 2035
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 42

## All-green BUY / SELL

- Mode: **green_pile** · SELL **core_weights_ex_green**
- Pile: **121** liquid all-green names (need ≥ 8) of 2035
- Core fired: join=yes, AB=yes, peer=yes
- pile 121 ≥ 8 liquid all-green names — BUY 15 from the pile by green_rank (no opp); SELL is core weights on the non-green remainder

## Decision lattice — gate → route → rank

The weighted score is now a tie-breaker inside an eligible lane. It cannot average away a market, group, company, or setup veto.

### MARKET: 🟡 YELLOW

- YELLOW: general up score=+2.01; good=+0.0 vs bad=+0.0; risk=off; red pillars=0
- Allowed long lanes: **standard, group_leader, catalyst** · max slots 8 · size ×0.60

Decision domains: **MKT · parent · child · company · setup · flow**. Measured parent/child tape is kept separate from the LLM essay; direct company events must be price-confirmed on a hard-red day.

### Bull decisions (eligible or closest blocked cases)

| # | Ticker | Domains | Lane | Company / group | Decision |
|---:|--------|---------|------|-----------------|----------|
| 1 | **BKR** | 🟡🟡🔴🟡🟢🟢 | blocked | usable dossier Strong Bullish conv=100; Oil & Gas Equipment & Services -4.4% d1 / -6.8% 1w / -7.7% vs parent | BLOCK BUY — child industry/theme RED; direct catalyst lacks price confirmation / market=YELLOW; parent=YELLOW; child=RED/rel=RED; company=YELLOW(1.00); setup=GREEN; flow=GREEN |
| 2 | **KGS** | 🟡🟡🔴🟡🔴🟡 | blocked | usable dossier Strong Bullish conv=89; Oil & Gas Equipment & Services -4.4% d1 / -6.8% 1w / -7.7% vs parent | BLOCK BUY — child industry/theme RED; setup RED; direct catalyst lacks price confirmation; legacy region red; v2 domain region red / market=YELLOW; parent=YELLOW; child=RED/rel=RED; company=YELLOW(0.89); setup=RED; flow=YELLOW |
| 3 | **SLB** | 🟡🟡🔴🟢🟢🔴 | blocked | usable dossier Bullish conv=35; Oil & Gas Equipment & Services -4.4% d1 / -6.8% 1w / -7.7% vs parent | BLOCK BUY — child industry/theme RED; flow RED; direct catalyst lacks price confirmation; legacy Cond red / market=YELLOW; parent=YELLOW; child=RED/rel=RED; company=GREEN(0.80); setup=GREEN; flow=RED |
| 4 | **SSRM** | 🟡🔴🔴🟡🟢🟡 | blocked | usable dossier Strong Bullish conv=73; Gold -2.8% d1 / -5.0% 1w / -0.2% vs parent | BLOCK BUY — parent sector RED; child industry/theme RED; direct catalyst lacks price confirmation / market=YELLOW; parent=RED; child=RED/rel=YELLOW; company=YELLOW(0.80); setup=GREEN; flow=YELLOW |
| 5 | **RIG** | 🟡🟡🔴🟡🟡🟢 | blocked | usable dossier Strong Bullish conv=51; Oil & Gas Drilling -3.3% d1 / -4.4% 1w / -5.4% vs parent | BLOCK BUY — child industry/theme RED; setup YELLOW; direct catalyst lacks price confirmation / market=YELLOW; parent=YELLOW; child=RED/rel=RED; company=YELLOW(0.80); setup=YELLOW; flow=GREEN |
| 6 | **AMN** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 7 | **AVPT** | 🟡🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 8 | **PLTR** | 🟡🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 9 | **MD** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 10 | **RELY** | 🟡🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 11 | **FTNT** | 🟡🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 12 | **RBRK** | 🟡🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 13 | **BB** | 🟡🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵 |
| 14 | **EPAM** | 🟡🔴🟢🟡🟢🟢 | group_leader | no direct company event; Information Technology Services +2.9% d1 / +1.9% 1w / +3.9% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵 |
| 15 | **HITI** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Pharmaceutical Retailers +3.6% d1 / +1.0% 1w / +3.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **OKLO** | 🟡🔴🔴🟡🔴🔴 | Utilities - Independent Power Producers | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -6.3% |
| 2 | **NEOV** | 🟡🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 3 | **NRGV** | 🟡🔴🔴🟡🔴🟡 | Utilities - Renewable | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -4.1% |
| 4 | **INDI** | 🟡🔴🔴🟡🔴🔴 | Semiconductors | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 5 | **NNDM** | 🟡🔴🔴🟡🔴🔴 | Computer Hardware | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.1% |
| 6 | **HNRG** | 🟡🔴🔴🟡🔴🟡 | Utilities - Independent Power Producers | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -6.3% |
| 7 | **METC** | 🟡🔴🔴🟡🔴🟡 | Coking Coal | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -5.7% |
| 8 | **FCEL** | 🟡🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 9 | **AMPG** | 🟡🔴🔴🟡🔴🔴 | Communication Equipment | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 10 | **NE** | 🟡🟡🔴🔴🔴🟡 | Oil & Gas Drilling | SELL/AVOID — market=YELLOW; red domains=child,company,setup; child lags parent -5.4% |
| 11 | **EXK** | 🟡🔴🔴🟡🔴🟡 | Silver | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -4.5% |
| 12 | **LPTH** | 🟡🔴🔴🟡🔴🔴 | Electronic Components | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 13 | **NRG** | 🟡🔴🔴🟡🔴🟡 | Utilities - Independent Power Producers | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -6.3% |
| 14 | **QUBT** | 🟡🔴🔴🟡🔴🟡 | Computer Hardware | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.1% |
| 15 | **POWL** | 🟡🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.7% |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **captain_research** (282 captains, 6 industries → s_heat).
- Board file: `01_daily/map_heat/2026-09-30_map_heat.json` · generated 2026-09-30T04:22:14.006127-04:00

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
| Map heat captain research | **found** | Grok captain essays (strict morning_refresh; else Finviz tape) |
| Catalyst overlays | **missing / not in ranker** | not in ranker — separate chart workflow |
| Insider / politician flow | **missing / not in ranker** | no daily file in repo |
| Industry predict | **found** | not scored (ad-hoc only) |
| Learnings / mutable policy | **missing / not in ranker** | next predict prompt, not a ticker score |

### Sector LLM bias (1d) — 0 / empty means that essay was not run today

| Sector | bias |
|--------|------|
| — | none today |

### How much each predictor is trusted (graded hit rate)

| Topic | hit rate | n | weight |
|-------|----------|---|--------|
| general | 51% | 41 | ×0.85 |
| sector:Basic Materials | 45% | 29 | ×0.50 |
| sector:Communication Services | 21% | 28 | ×0.50 |
| sector:Consumer Cyclical | 48% | 29 | ×0.85 |
| sector:Consumer Defensive | 46% | 28 | ×0.85 |
| sector:Energy | 55% | 29 | ×1.00 |
| sector:Financial | 46% | 28 | ×0.85 |
| sector:Healthcare | 48% | 25 | ×0.85 |
| sector:Industrials | 36% | 28 | ×0.50 |
| sector:Real Estate | 54% | 28 | ×0.85 |
| sector:Technology | 37% | 27 | ×0.50 |
| sector:Utilities | 42% | 26 | ×0.50 |

## Horizon weights — book_policy.json v15 · renormalized (absent: sector)

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.13 | 0.00 | 0.09 | 0.28 | 0.28 | 0.22 | additive |
| 3d | 0.19 | 0.00 | 0.09 | 0.19 | 0.30 | 0.23 | additive |
| 1w | 0.21 | 0.00 | 0.10 | 0.12 | 0.33 | 0.24 | additive |
| 2w | 0.24 | 0.00 | 0.10 | 0.07 | 0.34 | 0.24 | additive |
| 1m | 0.28 | 0.00 | 0.10 | 0.00 | 0.38 | 0.25 | additive |

## 1d BUY — why these names

### 1. SN · $25.6B large · Consumer Cyclical

**1d score +0.407**

**SN** is a liquid **large-cap** Consumer Cyclical name (Furnishings, Fixtures & Appliances) at $25.6B, ADV ~1827k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.91 | +0.121 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.25 | +0.022 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.93 | +0.257 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.80 | +0.177 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.407** | |

### 2. MHK · $7.5B mid · Consumer Cyclical

**1d score +0.769**

**MHK** is a liquid **mid-cap** Consumer Cyclical name (Furnishings, Fixtures & Appliances) at $7.5B, ADV ~952k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.87 | +0.116 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.25 | +0.022 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.76 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.76 | +0.169 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.769** | |

### 3. FIGS · $2.3B mid · Consumer Cyclical

**1d score +0.701**

**FIGS** is a liquid **mid-cap** Consumer Cyclical name (Apparel Manufacturing) at $2.3B, ADV ~2935k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.96 | +0.127 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.25 | +0.022 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.97 | +0.270 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.37 | +0.081 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.701** | |

### 4. CAKE · $5.2B mid · Consumer Cyclical

**1d score +0.702**

**CAKE** is a liquid **mid-cap** Consumer Cyclical name (Restaurants) at $5.2B, ADV ~1635k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.79 | +0.106 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.25 | +0.022 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.76 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.73 | +0.163 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.702** | |

### 5. AVPT · $2.9B mid · Technology

**1d score +0.749**

**AVPT** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $2.9B, ADV ~2025k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.57 | +0.075 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.25 | +0.022 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.95 | +0.265 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.61 | +0.136 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.749** | |

### 6. CORT · $12.5B large · Healthcare

**1d score +0.272**

**CORT** is a liquid **large-cap** Healthcare name (Biotechnology) at $12.5B, ADV ~1309k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.48 | +0.064 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.07 | +0.007 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.81 | +0.225 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.66 | +0.147 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.272** | |

### 7. CDNA · $3.4B mid · Healthcare

**1d score +0.719**

**CDNA** is a liquid **mid-cap** Healthcare name (Diagnostics & Research) at $3.4B, ADV ~1185k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.10 | +0.014 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.49 | +0.044 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.95 | +0.265 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.88 | +0.197 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.719** | |

### 8. ETON · $1.7B small · Healthcare

**1d score +0.661**

**ETON** is a liquid **small-cap** Healthcare name (Drug Manufacturers - Specialty & Generic) at $1.7B, ADV ~559k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.30 | +0.041 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.25 | +0.022 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.93 | +0.257 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.63 | +0.141 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.661** | |

### 9. IT · $11.7B large · Technology

**1d score +0.476**

**IT** is a liquid **large-cap** Technology name (Information Technology Services) at $11.7B, ADV ~1306k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.35 | +0.046 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.25 | +0.022 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.81 | +0.225 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.69 | +0.153 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1d total** | | | **+0.476** | |

### 10. ADMA · $2.3B mid · Healthcare

**1d score +0.659**

**ADMA** is a liquid **mid-cap** Healthcare name (Biotechnology) at $2.3B, ADV ~2634k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.46 | +0.061 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.07 | +0.007 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.93 | +0.257 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.38 | +0.084 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.659** | |

### 11. RXO · $3.3B mid · Industrials

**1d score +0.601**

**RXO** is a liquid **mid-cap** Industrials name (Trucking) at $3.3B, ADV ~1754k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.94 | +0.125 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.49 | +0.044 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.12 | +0.035 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.54 | +0.119 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.601** | |

### 12. VLO · $112.2B large · Energy

**1d score +0.251**

**VLO** is a liquid **large-cap** Energy name (Oil & Gas Refining & Marketing) at $112.2B, ADV ~3037k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.17 | +0.022 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.07 | +0.007 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.85 | +0.236 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.48 | +0.107 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.05 | +0.049 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.251** | |

### 13. PAYS · $718M small · Technology

**1d score +0.575**

**PAYS** is a liquid **small-cap** Technology name (Software - Infrastructure) at $718M, ADV ~691k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.33 | +0.044 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.25 | +0.022 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.93 | +0.257 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.23 | +0.052 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.575** | |

### 14. GDYN · $617M small · Technology

**1d score +0.530**

**GDYN** is a liquid **small-cap** Technology name (Information Technology Services) at $617M, ADV ~1686k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.20 | +0.026 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.25 | +0.022 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.81 | +0.225 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.26 | +0.057 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.530** | |


## 1d AVOID — bottom of the same rank

- **ROIV** (large, Healthcare, $26.5B) score -0.433. NO BEAR — market=YELLOW; red domains=flow
- **RBLX** (large, Communication Services, $30.1B) score -0.407. NO BEAR — market=YELLOW; red domains=setup
- **VSAT** (large, Technology, $10.0B) score -0.384. SELL/AVOID — market=YELLOW; red domains=parent,child,setup
- **BIDU** (large, Communication Services, $23.9B) score -0.376. SELL/AVOID — market=YELLOW; red domains=setup,flow
- **SOC** (small, Energy, $702M) score -0.370. SELL/AVOID — market=YELLOW; red domains=child,setup; child lags parent -5.4%
- **AXSM** (large, Healthcare, $10.2B) score -0.355. NO BEAR — market=YELLOW; red domains=setup
- **GH** (large, Healthcare, $23.8B) score -0.352. NO BEAR — market=YELLOW; red domains=setup
- **BAK** (micro, Basic Materials, $257M) score -0.347. SELL/AVOID — market=YELLOW; red domains=parent,child,setup
- **CMTG** (micro, Real Estate, $179M) score -0.342. SELL/AVOID — market=YELLOW; red domains=parent,child,setup
- **SKHY** (mega, Technology, $1345.1B) score -0.341. SELL/AVOID — market=YELLOW; red domains=parent,child
- **GOGO** (small, Communication Services, $301M) score -0.340. NO BEAR — market=YELLOW; red domains=setup
- **RC** (micro, Real Estate, $218M) score -0.335. SELL/AVOID — market=YELLOW; red domains=parent,child,setup
- **LFMD** (micro, Healthcare, $141M) score -0.332. NO BEAR — market=YELLOW; red domains=setup
- **NRXP** (micro, Healthcare, $126M) score -0.330. NO BEAR — market=YELLOW; red domains=setup
- **AIRS** (micro, Healthcare, $138M) score -0.329. NO BEAR — market=YELLOW; red domains=setup
- **VNET** (small, Technology, $1.7B) score -0.326. SELL/AVOID — market=YELLOW; red domains=parent,setup
- **PRM** (mid, Basic Materials, $4.6B) score -0.323. SELL/AVOID — market=YELLOW; red domains=parent,child,setup
- **WVE** (small, Healthcare, $749M) score -0.322. NO BEAR — market=YELLOW; red domains=setup
- **TMC** (small, Basic Materials, $1.7B) score -0.322. SELL/AVOID — market=YELLOW; red domains=parent,child,setup
- **INTC** (mega, Technology, $615.5B) score -0.322. SELL/AVOID — market=YELLOW; red domains=parent,child
- **TIGO** (large, Communication Services, $15.0B) score -0.322. NO BEAR — market=YELLOW
- **BORR** (small, Energy, $1.2B) score -0.321. SELL/AVOID — market=YELLOW; red domains=child,setup; child lags parent -5.4%
- **PYXS** (micro, Healthcare, $242M) score -0.318. NO BEAR — market=YELLOW; red domains=setup
- **SSP** (micro, Communication Services, $249M) score -0.317. NO BEAR — market=YELLOW; red domains=setup; child lags parent -4.6%
- **WRAP** (micro, Technology, $83M) score -0.314. SELL/AVOID — market=YELLOW; red domains=parent,child,setup

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | SN | +0.464 | large | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 2 | MHK | +0.820 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 3 | FIGS | +0.757 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | CAKE | +0.749 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | AVPT | +0.786 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 6 | CORT | +0.318 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 7 | CDNA | +0.713 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | ETON | +0.684 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 9 | IT | +0.499 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | ADMA | +0.703 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 11 | RXO | +0.615 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 12 | VLO | +0.279 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 13 | PAYS | +0.596 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | GDYN | +0.541 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | SN | +0.503 | large | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 2 | MHK | +0.852 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 3 | FIGS | +0.796 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | CAKE | +0.778 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | AVPT | +0.815 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 6 | CORT | +0.354 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 7 | CDNA | +0.710 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | ETON | +0.705 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 9 | IT | +0.518 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | ADMA | +0.741 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 11 | RXO | +0.608 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 12 | VLO | +0.306 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 13 | PAYS | +0.615 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | GDYN | +0.553 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | SN | +0.542 | large | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 2 | MHK | +0.889 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 3 | FIGS | +0.834 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | CAKE | +0.813 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | AVPT | +0.844 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 6 | CORT | +0.379 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 7 | CDNA | +0.727 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | ETON | +0.725 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 9 | IT | +0.539 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | ADMA | +0.764 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 11 | RXO | +0.641 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 12 | VLO | +0.321 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 13 | PAYS | +0.634 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | GDYN | +0.568 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1m BUY — why these names

### 1. SN · $25.6B large · Consumer Cyclical

**1m score +0.626**

**SN** is a liquid **large-cap** Consumer Cyclical name (Furnishings, Fixtures & Appliances) at $25.6B, ADV ~1827k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.91 | +0.250 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.93 | +0.347 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.80 | +0.199 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1m total** | | | **+0.626** | |

### 2. MHK · $7.5B mid · Consumer Cyclical

**1m score +0.966**

**MHK** is a liquid **mid-cap** Consumer Cyclical name (Furnishings, Fixtures & Appliances) at $7.5B, ADV ~952k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.87 | +0.240 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.76 | +0.286 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.76 | +0.191 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.966** | |

### 3. FIGS · $2.3B mid · Consumer Cyclical

**1m score +0.919**

**FIGS** is a liquid **mid-cap** Consumer Cyclical name (Apparel Manufacturing) at $2.3B, ADV ~2935k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.96 | +0.263 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.97 | +0.364 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.37 | +0.091 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.919** | |

### 4. CAKE · $5.2B mid · Consumer Cyclical

**1m score +0.887**

**CAKE** is a liquid **mid-cap** Consumer Cyclical name (Restaurants) at $5.2B, ADV ~1635k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.79 | +0.218 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.76 | +0.286 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.73 | +0.184 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.887** | |

### 5. AVPT · $2.9B mid · Technology

**1m score +0.917**

**AVPT** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $2.9B, ADV ~2025k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.57 | +0.155 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.95 | +0.358 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.61 | +0.154 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.917** | |

### 6. CORT · $12.5B large · Healthcare

**1m score +0.431**

**CORT** is a liquid **large-cap** Healthcare name (Biotechnology) at $12.5B, ADV ~1309k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.48 | +0.133 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.81 | +0.303 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.66 | +0.165 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1m total** | | | **+0.431** | |

### 7. CDNA · $3.4B mid · Healthcare

**1m score +0.807**

**CDNA** is a liquid **mid-cap** Healthcare name (Diagnostics & Research) at $3.4B, ADV ~1185k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.10 | +0.028 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.95 | +0.358 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.88 | +0.221 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.807** | |

### 8. ETON · $1.7B small · Healthcare

**1m score +0.789**

**ETON** is a liquid **small-cap** Healthcare name (Drug Manufacturers - Specialty & Generic) at $1.7B, ADV ~559k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.30 | +0.084 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.93 | +0.347 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.63 | +0.159 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.789** | |

### 9. IT · $11.7B large · Technology

**1m score +0.601**

**IT** is a liquid **large-cap** Technology name (Information Technology Services) at $11.7B, ADV ~1306k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.35 | +0.096 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.81 | +0.303 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.69 | +0.172 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1m total** | | | **+0.601** | |

### 10. ADMA · $2.3B mid · Healthcare

**1m score +0.818**

**ADMA** is a liquid **mid-cap** Healthcare name (Biotechnology) at $2.3B, ADV ~2634k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.46 | +0.126 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.93 | +0.347 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.38 | +0.095 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.818** | |

### 11. RXO · $3.3B mid · Industrials

**1m score +0.717**

**RXO** is a liquid **mid-cap** Industrials name (Trucking) at $3.3B, ADV ~1754k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.94 | +0.259 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.12 | +0.047 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.54 | +0.134 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.717** | |

### 12. VLO · $112.2B large · Energy

**1m score +0.364**

**VLO** is a liquid **large-cap** Energy name (Oil & Gas Refining & Marketing) at $112.2B, ADV ~3037k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.17 | +0.046 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.85 | +0.318 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.48 | +0.121 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.05 | +0.049 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1m total** | | | **+0.364** | |

### 13. PAYS · $718M small · Technology

**1m score +0.697**

**PAYS** is a liquid **small-cap** Technology name (Software - Infrastructure) at $718M, ADV ~691k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.33 | +0.092 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.93 | +0.347 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.23 | +0.058 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.697** | |

### 14. GDYN · $617M small · Technology

**1m score +0.622**

**GDYN** is a liquid **small-cap** Technology name (Information Technology Services) at $617M, ADV ~1686k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.20 | +0.054 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.81 | +0.303 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.26 | +0.065 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.622** | |


## 1m AVOID — bottom of the same rank

- **RBLX** (large, Communication Services, $30.1B) score -0.689. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **VSAT** (large, Technology, $10.0B) score -0.646. this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BIDU** (large, Communication Services, $23.9B) score -0.621. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PRM** (mid, Basic Materials, $4.6B) score -0.619. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TMC** (small, Basic Materials, $1.7B) score -0.617. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SOC** (small, Energy, $702M) score -0.615. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **ROIV** (large, Healthcare, $26.5B) score -0.609. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AXSM** (large, Healthcare, $10.2B) score -0.607. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BAK** (micro, Basic Materials, $257M) score -0.601. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LFMD** (micro, Healthcare, $141M) score -0.600. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AIRS** (micro, Healthcare, $138M) score -0.593. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RC** (micro, Real Estate, $218M) score -0.591. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GOGO** (small, Communication Services, $301M) score -0.590. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CMTG** (micro, Real Estate, $179M) score -0.588. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BORR** (small, Energy, $1.2B) score -0.587. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PLAY** (micro, Communication Services, $223M) score -0.585. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ASPI** (small, Basic Materials, $402M) score -0.585. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NRXP** (micro, Healthcare, $126M) score -0.574. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **VNET** (small, Technology, $1.7B) score -0.568. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **EYPT** (micro, Healthcare, $287M) score -0.562. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **IMNM** (mid, Healthcare, $2.5B) score -0.561. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ANGI** (micro, Communication Services, $207M) score -0.556. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ARDX** (small, Healthcare, $860M) score -0.553. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **ACDC** (small, Energy, $818M) score -0.553. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PYXS** (micro, Healthcare, $242M) score -0.545. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-09-30_stock_book.md`
- Machine table: `data/stock_book/2026-09-30_stock_book.csv`
- Machine book: `data/stock_book/2026-09-30_stock_book.json`
- Join rank: `data/join/2026-09-30_ranked.csv`
- Weather: `01_daily/weather/2026-09-30_weather.md`
- AB enrich: `data/ab_checklist/2026-09-30_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-09-30_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-09-30_map_heat.md`
