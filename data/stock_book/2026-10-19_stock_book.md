# Stock book — 2026-10-19

_Generated 2026-10-19T12:47:20.448326-04:00_

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
- General predict (same-day): +0.50 up (present)
- Stand-down: **no** — 289 names qualified through standard,group_leader,catalyst (12 probable)
- Sector predicts this date: 0/11 (missing → sector layer is 0; Finviz week tape still sits in join)
- News tickers in play: 149
- AB coverage: 1907 names · peer RS: 1804
- Universe after liquidity: 2035
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 65

## All-green BUY / SELL

- Mode: **green_pile** · SELL **core_weights_ex_green**
- Pile: **143** liquid all-green names (need ≥ 8) of 2035
- Core fired: join=yes, AB=yes, peer=yes
- pile 143 ≥ 8 liquid all-green names — BUY 15 from the pile by green_rank (no opp); SELL is core weights on the non-green remainder

## Decision lattice — gate → route → rank

The weighted score is now a tie-breaker inside an eligible lane. It cannot average away a market, group, company, or setup veto.

### MARKET: 🟡 YELLOW

- YELLOW: general up score=+2.12; good=+1.8 vs bad=-1.0; risk=off; red pillars=1
- Allowed long lanes: **standard, group_leader, catalyst** · max slots 8 · size ×0.60
- Bull evidence: rates / Fed +1.00 points; oil / dollar +0.50 points; futures +0.25 points
- Bear evidence: global sessions -1.00 points

Decision domains: **MKT · parent · child · company · setup · flow**. Measured parent/child tape is kept separate from the LLM essay; direct company events must be price-confirmed on a hard-red day.

### Bull decisions (eligible or closest blocked cases)

| # | Ticker | Domains | Lane | Company / group | Decision |
|---:|--------|---------|------|-----------------|----------|
| 1 | **META** | 🟡🟢🟢🔴🟢🟡 | blocked | usable dossier Bearish conv=21; Internet Content & Information +3.0% d1 / +4.2% 1w / +0.8% vs parent | BLOCK BUY — company evidence RED; direct catalyst lacks price confirmation / market=YELLOW; parent=GREEN; child=GREEN/rel=YELLOW; company=RED(0.80); setup=GREEN; flow=YELLOW |
| 2 | **HITI** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Pharmaceutical Retailers +3.6% d1 / +1.0% 1w / +3.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 3 | **CON** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 4 | **AMN** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 5 | **PACS** | 🟡🟡🟢🟡🟢🟡 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,⚪ |
| 6 | **ASTH** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 7 | **PLTR** | 🟡🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 8 | **MD** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 9 | **PANW** | 🟡🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 10 | **AVAH** | 🟡🟡🟢🟡🟢🟡 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,Cond green |
| 11 | **NBIS** | 🟡🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵 |
| 12 | **PENG** | 🟡🔴🟢🟡🟢🟡 | group_leader | no direct company event; Information Technology Services +2.9% d1 / +1.9% 1w / +3.9% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,Cond green |
| 13 | **PRCH** | 🟡🟡🟢🟡🟢🟢 | standard | no direct company event; Insurance - Property & Casualty +1.2% d1 / +0.3% 1w / +2.1% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 14 | **SNPS** | 🟡🔴🟢🟡🟢🟡 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,Cond green |
| 15 | **NTAP** | 🟡🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **OKLO** | 🟡🔴🔴🟡🔴🔴 | Utilities - Independent Power Producers | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -6.3% |
| 2 | **NEOV** | 🟡🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 3 | **FCEL** | 🟡🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 4 | **NNDM** | 🟡🔴🔴🟡🔴🔴 | Computer Hardware | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.1% |
| 5 | **EOSE** | 🟡🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -4.7% |
| 6 | **POWL** | 🟡🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 7 | **TE** | 🟡🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -4.7% |
| 8 | **INDI** | 🟡🔴🔴🟡🔴🔴 | Semiconductors | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 9 | **NRGV** | 🟡🔴🔴🟡🔴🟡 | Utilities - Renewable | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -4.1% |
| 10 | **HNRG** | 🟡🔴🔴🟡🔴🟡 | Utilities - Independent Power Producers | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -6.3% |
| 11 | **PLUG** | 🟡🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -4.7% |
| 12 | **LUNR** | 🟡🔴🔴🟡🔴🔴 | Aerospace & Defense | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 13 | **METC** | 🟡🔴🔴🟡🔴🟡 | Coking Coal | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -5.7% |
| 14 | **SKYX** | 🟡🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -4.7% |
| 15 | **SIDU** | 🟡🔴🔴🟡🔴🔴 | Aerospace & Defense | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **captain_research** (259 captains, 5 industries → s_heat).
- Board file: `01_daily/map_heat/2026-10-19_map_heat.json` · generated 2026-10-19T04:22:36.211258-04:00

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
| general | 50% | 48 | ×0.85 |
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

### 1. SN · $25.6B large · Consumer Cyclical

**1d score +0.412**

**SN** is a liquid **large-cap** Consumer Cyclical name (Furnishings, Fixtures & Appliances) at $25.6B, ADV ~1827k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.99 | +0.132 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.25 | +0.022 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.91 | +0.251 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.80 | +0.177 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.412** | |

### 2. CBRL · $1.2B small · Consumer Cyclical

**1d score +0.793**

**CBRL** is a liquid **small-cap** Consumer Cyclical name (Restaurants) at $1.2B, ADV ~961k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.74 | +0.099 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.25 | +0.022 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.91 | +0.251 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.99 | +0.220 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.793** | |

### 3. PRCH · $1.9B small · Financial

**1d score +0.814**

**PRCH** is a liquid **small-cap** Financial name (Insurance - Property & Casualty) at $1.9B, ADV ~1644k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.98 | +0.131 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.50 | +0.044 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.70 | +0.196 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.87 | +0.194 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.814** | |

### 4. CAKE · $5.2B mid · Consumer Cyclical

**1d score +0.734**

**CAKE** is a liquid **mid-cap** Consumer Cyclical name (Restaurants) at $5.2B, ADV ~1635k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.93 | +0.124 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.25 | +0.022 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.81 | +0.225 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.73 | +0.163 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.734** | |

### 5. SHOO · $3.3B mid · Consumer Cyclical

**1d score +0.718**

**SHOO** is a liquid **mid-cap** Consumer Cyclical name (Footwear & Accessories) at $3.3B, ADV ~1034k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.99 | +0.132 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.25 | +0.022 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.94 | +0.262 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.47 | +0.103 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.718** | |

### 6. VEEV · $45.1B large · Healthcare

**1d score +0.397**

**VEEV** is a liquid **large-cap** Healthcare name (Health Information Services) at $45.1B, ADV ~1817k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.66 | +0.088 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.25 | +0.022 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.85 | +0.236 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.87 | +0.193 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.397** | |

### 7. CDNA · $3.4B mid · Healthcare

**1d score +0.769**

**CDNA** is a liquid **mid-cap** Healthcare name (Diagnostics & Research) at $3.4B, ADV ~1185k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.50 | +0.066 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.50 | +0.044 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.94 | +0.262 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.88 | +0.197 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.769** | |

### 8. ARQT · $3.5B mid · Healthcare

**1d score +0.731**

**ARQT** is a liquid **mid-cap** Healthcare name (Biotechnology) at $3.5B, ADV ~2136k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.79 | +0.105 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.50 | +0.044 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.85 | +0.236 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.66 | +0.146 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.731** | |

### 9. AVPT · $2.9B mid · Technology

**1d score +0.704**

**AVPT** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $2.9B, ADV ~2025k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.82 | +0.110 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.25 | +0.022 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.85 | +0.236 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.61 | +0.136 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.704** | |

### 10. ETON · $1.7B small · Healthcare

**1d score +0.669**

**ETON** is a liquid **small-cap** Healthcare name (Drug Manufacturers - Specialty & Generic) at $1.7B, ADV ~559k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.82 | +0.110 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.25 | +0.022 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.70 | +0.196 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.63 | +0.141 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.669** | |

### 11. MQ · $1.8B small · Technology

**1d score +0.679**

**MQ** is a liquid **small-cap** Technology name (Software - Infrastructure) at $1.8B, ADV ~971k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.19 | +0.025 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.25 | +0.022 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.81 | +0.225 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.93 | +0.207 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.679** | |

### 12. LAUR · $5.2B mid · Consumer Defensive

**1d score +0.629**

**LAUR** is a liquid **mid-cap** Consumer Defensive name (Education & Training Services) at $5.2B, ADV ~1003k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.58 | +0.077 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.07 | +0.007 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.55 | +0.154 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.71 | +0.157 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.02 | -0.016 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.629** | |

### 13. TOST · $17.7B large · Technology

**1d score +0.496**

**TOST** is a liquid **large-cap** Technology name (Software - Infrastructure) at $17.7B, ADV ~10515k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.31 | +0.041 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.50 | +0.044 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.85 | +0.236 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.65 | +0.145 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1d total** | | | **+0.496** | |

### 14. TRU · $13.4B large · Financial

**1d score +0.465**

**TRU** is a liquid **large-cap** Financial name (Financial Data & Stock Exchanges) at $13.4B, ADV ~2270k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.87 | +0.116 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.50 | +0.044 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.70 | +0.196 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.18 | +0.039 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.07 | +0.070 | liquid small/mid, room to run |
| **1d total** | | | **+0.465** | |

### 15. JXN · $8.9B mid · Financial

**1d score +0.528**

**JXN** is a liquid **mid-cap** Financial name (Insurance - Life) at $8.9B, ADV ~700k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.98 | +0.131 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.50 | +0.044 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.36 | +0.100 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.24 | +0.053 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.528** | |

### 16. CHEF · $4.6B mid · Consumer Defensive

**1d score +0.632**

**CHEF** is a liquid **mid-cap** Consumer Defensive name (Food Distribution) at $4.6B, ADV ~639k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.50 | +0.066 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.50 | +0.044 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.88 | +0.244 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.19 | +0.043 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.02 | -0.016 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.632** | |

### 17. ABX · $804M small · Financial

**1d score +0.522**

**ABX** is a liquid **small-cap** Financial name (Insurance - Life) at $804M, ADV ~621k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.78 | +0.104 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.07 | +0.007 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.64 | +0.176 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.16 | +0.035 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.522** | |

### 18. AVO · $1.1B small · Consumer Defensive

**1d score +0.619**

**AVO** is a liquid **small-cap** Consumer Defensive name (Food Distribution) at $1.1B, ADV ~881k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.08 | +0.010 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.07 | +0.007 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.81 | +0.225 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.57 | +0.127 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.619** | |

### 19. SONO · $2.0B mid · Technology

**1d score +0.567**

**SONO** is a liquid **mid-cap** Technology name (Consumer Electronics) at $2.0B, ADV ~1940k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.28 | +0.037 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.50 | +0.044 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.81 | +0.225 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.27 | +0.061 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.567** | |

### 20. EDU · $8.7B mid · Consumer Defensive

**1d score +0.545**

**EDU** is a liquid **mid-cap** Consumer Defensive name (Education & Training Services) at $8.7B, ADV ~763k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **miss**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.15 | +0.020 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | +0.07 | +0.007 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.64 | +0.176 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.41 | +0.091 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.545** | |


## 1d AVOID — bottom of the same rank

- **ROIV** (large, Healthcare, $26.5B) score -0.608. SELL/AVOID — market=YELLOW; red domains=setup,flow
- **ARE** (mid, Real Estate, $8.5B) score -0.454. SELL/AVOID — market=YELLOW; red domains=parent,child,setup
- **BNTX** (large, Healthcare, $24.6B) score -0.397. SELL/AVOID — market=YELLOW; red domains=setup,flow
- **GH** (large, Healthcare, $23.8B) score -0.389. NO BEAR — market=YELLOW; red domains=setup
- **VSAT** (large, Technology, $10.0B) score -0.366. SELL/AVOID — market=YELLOW; red domains=parent,child,setup
- **IFRX** (micro, Healthcare, $268M) score -0.363. NO BEAR — market=YELLOW; red domains=setup
- **TYRA** (small, Healthcare, $1.5B) score -0.361. NO BEAR — market=YELLOW; red domains=setup
- **WVE** (small, Healthcare, $749M) score -0.359. NO BEAR — market=YELLOW; red domains=setup
- **BAK** (micro, Basic Materials, $257M) score -0.356. SELL/AVOID — market=YELLOW; red domains=parent,child,setup
- **ALK** (mid, Industrials, $4.4B) score -0.350. SELL/AVOID — market=YELLOW; red domains=parent,setup
- **AXSM** (large, Healthcare, $10.2B) score -0.349. NO BEAR — market=YELLOW; red domains=setup
- **NEOV** (micro, Industrials, $133M) score -0.345. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.7%
- **CGEM** (small, Healthcare, $962M) score -0.345. NO BEAR — market=YELLOW; red domains=setup
- **PYXS** (micro, Healthcare, $242M) score -0.335. NO BEAR — market=YELLOW; red domains=setup
- **LXRX** (small, Healthcare, $845M) score -0.334. SELL/AVOID — market=YELLOW; red domains=setup,flow
- **AVTX** (small, Healthcare, $815M) score -0.331. NO BEAR — market=YELLOW; red domains=setup
- **AIRS** (micro, Healthcare, $138M) score -0.326. NO BEAR — market=YELLOW; red domains=setup
- **CHRS** (micro, Healthcare, $179M) score -0.324. NO BEAR — market=YELLOW; red domains=setup
- **VNET** (small, Technology, $1.7B) score -0.321. SELL/AVOID — market=YELLOW; red domains=parent,setup
- **MLYS** (mid, Healthcare, $2.3B) score -0.321. NO BEAR — market=YELLOW; red domains=setup
- **INTC** (mega, Technology, $615.5B) score -0.320. SELL/AVOID — market=YELLOW; red domains=parent,child
- **PCT** (small, Industrials, $936M) score -0.318. SELL/AVOID — market=YELLOW; red domains=parent,setup
- **NN** (mid, Technology, $2.4B) score -0.318. SELL/AVOID — market=YELLOW; red domains=parent,setup
- **MBOT** (micro, Healthcare, $84M) score -0.313. NO BEAR — market=YELLOW; red domains=setup
- **IMNM** (mid, Healthcare, $2.5B) score -0.313. NO BEAR — market=YELLOW; red domains=setup

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | SN | +0.494 | large | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 2 | CBRL | +0.864 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | PRCH | +0.891 | small | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | CAKE | +0.811 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | SHOO | +0.798 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | VEEV | +0.461 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 7 | CDNA | +0.826 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | ARQT | +0.800 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 9 | AVPT | +0.774 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | ETON | +0.735 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 11 | MQ | +0.718 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 12 | LAUR | +0.680 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 13 | TOST | +0.540 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | TRU | +0.530 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | JXN | +0.590 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 16 | CHEF | +0.681 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 17 | ABX | +0.580 | small | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 18 | AVO | +0.649 | small | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 19 | SONO | +0.603 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 20 | EDU | +0.572 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | SN | +0.533 | large | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 2 | CBRL | +0.897 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | PRCH | +0.902 | small | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | CAKE | +0.844 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | SHOO | +0.836 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | VEEV | +0.489 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 7 | CDNA | +0.831 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | ARQT | +0.809 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 9 | AVPT | +0.806 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | ETON | +0.762 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 11 | MQ | +0.731 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 12 | LAUR | +0.711 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 13 | TOST | +0.535 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | TRU | +0.534 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | JXN | +0.587 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 16 | CHEF | +0.680 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 17 | ABX | +0.616 | small | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 18 | AVO | +0.673 | small | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 19 | SONO | +0.594 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 20 | EDU | +0.592 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | SN | +0.554 | large | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 2 | CBRL | +0.912 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | PRCH | +0.900 | small | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | CAKE | +0.862 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | SHOO | +0.855 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | VEEV | +0.500 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 7 | CDNA | +0.817 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | ARQT | +0.801 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 9 | AVPT | +0.820 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | ETON | +0.775 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 11 | MQ | +0.728 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 12 | LAUR | +0.730 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 13 | TOST | +0.513 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | TRU | +0.525 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | JXN | +0.578 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 16 | CHEF | +0.661 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 17 | ABX | +0.638 | small | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 18 | AVO | +0.678 | small | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 19 | SONO | +0.569 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 20 | EDU | +0.598 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1m BUY — why these names

### 1. SN · $25.6B large · Consumer Cyclical

**1m score +0.640**

**SN** is a liquid **large-cap** Consumer Cyclical name (Furnishings, Fixtures & Appliances) at $25.6B, ADV ~1827k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.99 | +0.271 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.91 | +0.339 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.80 | +0.199 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1m total** | | | **+0.640** | |

### 2. CBRL · $1.2B small · Consumer Cyclical

**1m score +0.992**

**CBRL** is a liquid **small-cap** Consumer Cyclical name (Restaurants) at $1.2B, ADV ~961k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.74 | +0.205 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.91 | +0.339 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.99 | +0.248 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.992** | |

### 3. PRCH · $1.9B small · Financial

**1m score +1.001**

**PRCH** is a liquid **small-cap** Financial name (Insurance - Property & Casualty) at $1.9B, ADV ~1644k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.98 | +0.269 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.70 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.87 | +0.218 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.001** | |

### 4. CAKE · $5.2B mid · Consumer Cyclical

**1m score +0.943**

**CAKE** is a liquid **mid-cap** Consumer Cyclical name (Restaurants) at $5.2B, ADV ~1635k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.93 | +0.256 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.81 | +0.303 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.73 | +0.184 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.943** | |

### 5. SHOO · $3.3B mid · Consumer Cyclical

**1m score +0.941**

**SHOO** is a liquid **mid-cap** Consumer Cyclical name (Footwear & Accessories) at $3.3B, ADV ~1034k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.99 | +0.271 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.94 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.47 | +0.116 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.941** | |

### 6. VEEV · $45.1B large · Healthcare

**1m score +0.575**

**VEEV** is a liquid **large-cap** Healthcare name (Health Information Services) at $45.1B, ADV ~1817k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.66 | +0.181 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.85 | +0.318 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.87 | +0.218 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1m total** | | | **+0.575** | |

### 7. CDNA · $3.4B mid · Healthcare

**1m score +0.911**

**CDNA** is a liquid **mid-cap** Healthcare name (Diagnostics & Research) at $3.4B, ADV ~1185k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.50 | +0.137 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.94 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.88 | +0.221 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.911** | |

### 8. ARQT · $3.5B mid · Healthcare

**1m score +0.900**

**ARQT** is a liquid **mid-cap** Healthcare name (Biotechnology) at $3.5B, ADV ~2136k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.79 | +0.218 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.85 | +0.318 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.66 | +0.164 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.900** | |

### 9. AVPT · $2.9B mid · Technology

**1m score +0.898**

**AVPT** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $2.9B, ADV ~2025k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.82 | +0.227 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.85 | +0.318 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.61 | +0.154 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.898** | |

### 10. ETON · $1.7B small · Healthcare

**1m score +0.849**

**ETON** is a liquid **small-cap** Healthcare name (Drug Manufacturers - Specialty & Generic) at $1.7B, ADV ~559k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.82 | +0.227 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.70 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.63 | +0.159 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.849** | |

### 11. MQ · $1.8B small · Technology

**1m score +0.787**

**MQ** is a liquid **small-cap** Technology name (Software - Infrastructure) at $1.8B, ADV ~971k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.19 | +0.051 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.81 | +0.303 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.93 | +0.233 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.787** | |

### 12. LAUR · $5.2B mid · Consumer Defensive

**1m score +0.777**

**LAUR** is a liquid **mid-cap** Consumer Defensive name (Education & Training Services) at $5.2B, ADV ~1003k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.58 | +0.159 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.55 | +0.208 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.71 | +0.176 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.02 | -0.016 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.777** | |

### 13. TOST · $17.7B large · Technology

**1m score +0.597**

**TOST** is a liquid **large-cap** Technology name (Software - Infrastructure) at $17.7B, ADV ~10515k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.31 | +0.085 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.85 | +0.318 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.65 | +0.163 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1m total** | | | **+0.597** | |

### 14. TRU · $13.4B large · Financial

**1m score +0.618**

**TRU** is a liquid **large-cap** Financial name (Financial Data & Stock Exchanges) at $13.4B, ADV ~2270k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.87 | +0.240 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.70 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.18 | +0.044 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.07 | +0.070 | liquid small/mid, room to run |
| **1m total** | | | **+0.618** | |

### 15. JXN · $8.9B mid · Financial

**1m score +0.664**

**JXN** is a liquid **mid-cap** Financial name (Insurance - Life) at $8.9B, ADV ~700k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.98 | +0.270 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.36 | +0.134 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.24 | +0.059 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.664** | |

### 16. CHEF · $4.6B mid · Consumer Defensive

**1m score +0.749**

**CHEF** is a liquid **mid-cap** Consumer Defensive name (Food Distribution) at $4.6B, ADV ~639k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.50 | +0.137 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.88 | +0.330 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.19 | +0.048 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.02 | -0.016 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.749** | |

### 17. ABX · $804M small · Financial

**1m score +0.691**

**ABX** is a liquid **small-cap** Financial name (Insurance - Life) at $804M, ADV ~621k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.78 | +0.214 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.64 | +0.238 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.16 | +0.039 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.691** | |

### 18. AVO · $1.1B small · Consumer Defensive

**1m score +0.718**

**AVO** is a liquid **small-cap** Consumer Defensive name (Food Distribution) at $1.1B, ADV ~881k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.08 | +0.021 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.81 | +0.303 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.57 | +0.143 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.718** | |

### 19. SONO · $2.0B mid · Technology

**1m score +0.648**

**SONO** is a liquid **mid-cap** Technology name (Consumer Electronics) at $2.0B, ADV ~1940k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.28 | +0.076 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.81 | +0.303 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.27 | +0.069 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.648** | |

### 20. EDU · $8.7B mid · Consumer Defensive

**1m score +0.632**

**EDU** is a liquid **mid-cap** Consumer Defensive name (Education & Training Services) at $8.7B, ADV ~763k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **miss**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.15 | +0.041 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.64 | +0.238 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.41 | +0.103 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.632** | |


## 1m AVOID — bottom of the same rank

- **ROIV** (large, Healthcare, $26.5B) score -0.859. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BNTX** (large, Healthcare, $24.6B) score -0.650. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **VSAT** (large, Technology, $10.0B) score -0.627. this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BAK** (micro, Basic Materials, $257M) score -0.618. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TYRA** (small, Healthcare, $1.5B) score -0.610. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **WVE** (small, Healthcare, $749M) score -0.606. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **IMNM** (mid, Healthcare, $2.5B) score -0.604. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PRM** (mid, Basic Materials, $4.6B) score -0.593. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AXSM** (large, Healthcare, $10.2B) score -0.591. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PCT** (small, Industrials, $936M) score -0.590. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **IFRX** (micro, Healthcare, $268M) score -0.587. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NN** (mid, Technology, $2.4B) score -0.584. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AVTX** (small, Healthcare, $815M) score -0.577. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **LXRX** (small, Healthcare, $845M) score -0.576. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AIRS** (micro, Healthcare, $138M) score -0.573. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CGEM** (small, Healthcare, $962M) score -0.569. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **MLYS** (mid, Healthcare, $2.3B) score -0.559. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **VNET** (small, Technology, $1.7B) score -0.558. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CRDF** (micro, Healthcare, $93M) score -0.550. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TMC** (small, Basic Materials, $1.7B) score -0.547. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ASPI** (small, Basic Materials, $402M) score -0.541. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CHRS** (micro, Healthcare, $179M) score -0.535. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **BIDU** (large, Communication Services, $23.9B) score -0.534. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ALMS** (small, Healthcare, $953M) score -0.533. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **PYXS** (micro, Healthcare, $242M) score -0.533. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-10-19_stock_book.md`
- Machine table: `data/stock_book/2026-10-19_stock_book.csv`
- Machine book: `data/stock_book/2026-10-19_stock_book.json`
- Join rank: `data/join/2026-10-19_ranked.csv`
- Weather: `01_daily/weather/2026-10-19_weather.md`
- AB enrich: `data/ab_checklist/2026-10-19_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-10-19_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-10-19_map_heat.md`
