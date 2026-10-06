# Stock book — 2026-10-06

_Generated 2026-10-06T16:44:54.252662-04:00_

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
- General predict (same-day): +0.00  (MISSING → 0)
- Stand-down: **no** — 470 names qualified through standard,group_leader,catalyst (117 probable)
- Sector predicts this date: 1/11 (ok)
- News tickers in play: 152
- AB coverage: 1927 names · peer RS: 1804
- Universe after liquidity: 2034
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 81

## All-green BUY / SELL

- Mode: **weighted_fallback** · SELL **core_weights**
- Pile: **0** liquid all-green names (need ≥ 8) of 2034
- Core fired: join=yes, AB=yes, peer=yes
- pile 0 < 8 liquid all-green names. Fallback weighted walk; SELL stays on core

## Decision lattice — gate → route → rank

The weighted score is now a tie-breaker inside an eligible lane. It cannot average away a market, group, company, or setup veto.

### MARKET: 🟢 GREEN

- GREEN: general up score=+2.62; good=+0.0 vs bad=+0.0; risk=on; red pillars=0
- Allowed long lanes: **standard, group_leader, catalyst** · max slots 15 · size ×1.00

Decision domains: **MKT · parent · child · company · setup · flow**. Measured parent/child tape is kept separate from the LLM essay; direct company events must be price-confirmed on a hard-red day.

### Bull decisions (eligible or closest blocked cases)

| # | Ticker | Domains | Lane | Company / group | Decision |
|---:|--------|---------|------|-----------------|----------|
| 1 | **HITI** | 🟢🟡🟢🟡🟢🟢 | group_leader | no direct company event; Pharmaceutical Retailers +3.6% d1 / +1.0% 1w / +3.5% vs parent | BUY GROUP_LEADER — market=GREEN; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 2 | **CON** | 🟢🟡🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 3 | **ASTH** | 🟢🟡🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 4 | **NTAP** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 5 | **AMN** | 🟢🟡🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 6 | **BAND** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 7 | **MQ** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 8 | **OKTA** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 9 | **AVPT** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 10 | **PACS** | 🟢🟡🟢🟡🟢🟡 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,Cond green |
| 11 | **IT** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Information Technology Services +2.9% d1 / +1.9% 1w / +3.9% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 12 | **MD** | 🟢🟡🟢🟡🟢🟢 | group_leader | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BUY GROUP_LEADER — market=GREEN; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 13 | **CRWD** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 14 | **TOST** | 🟢🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 15 | **ACN** | 🟢🔴🟢🟡🟢🟡 | group_leader | direct high digest (stale/undated): Accenture beats Q4 revenue guidance with 7% growth, sets FY27 revenue outlook for 3–6% local-currency growth; Information Technology Services +2.9% d1 / +1.9% 1w / +3.9% vs parent | BUY GROUP_LEADER — market=GREEN; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.48); setup=GREEN; flow=YELLOW; lookback=🔵,Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **NEOV** | 🟢🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 2 | **OKLO** | 🟢🔴🔴🟡🔴🔴 | Utilities - Independent Power Producers | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -6.3% |
| 3 | **FCEL** | 🟢🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 4 | **EOSE** | 🟢🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -4.7% |
| 5 | **ENVX** | 🟢🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 6 | **TE** | 🟢🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -4.7% |
| 7 | **SKYX** | 🟢🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -4.7% |
| 8 | **NNDM** | 🟢🔴🔴🟡🔴🔴 | Computer Hardware | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -3.1% |
| 9 | **MTZ** | 🟢🔴🔴🟡🔴🔴 | Engineering & Construction | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow |
| 10 | **LUNR** | 🟢🔴🔴🟡🔴🔴 | Aerospace & Defense | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow |
| 11 | **METC** | 🟢🔴🔴🟡🔴🟡 | Coking Coal | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -5.7% |
| 12 | **AMSC** | 🟢🔴🔴🟡🔴🔴 | Specialty Industrial Machinery | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow |
| 13 | **PLUG** | 🟢🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -4.7% |
| 14 | **INDI** | 🟢🔴🔴🟡🔴🔴 | Semiconductors | SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow |
| 15 | **NRG** | 🟢🔴🔴🟡🔴🟡 | Utilities - Independent Power Producers | SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -6.3% |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **captain_research** (281 captains, 6 industries → s_heat).
- Board file: `01_daily/map_heat/2026-10-06_map_heat.json` · generated 2026-10-06T04:22:36.554946-04:00

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
| Technology | -2.0% | -2.1% | +0.00 | essay flat, tape moving |
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
| Learnings / mutable policy | **found** | next predict prompt, not a ticker score |

### Sector LLM bias (1d) — 0 / empty means that essay was not run today

| Sector | bias |
|--------|------|
| Technology | +0.00 |

### How much each predictor is trusted (graded hit rate)

| Topic | hit rate | n | weight |
|-------|----------|---|--------|
| general | 50% | 44 | ×0.85 |
| sector:Basic Materials | 45% | 31 | ×0.85 |
| sector:Communication Services | 23% | 30 | ×0.50 |
| sector:Consumer Cyclical | 45% | 31 | ×0.85 |
| sector:Consumer Defensive | 50% | 30 | ×0.85 |
| sector:Energy | 52% | 31 | ×0.85 |
| sector:Financial | 47% | 30 | ×0.85 |
| sector:Healthcare | 48% | 27 | ×0.85 |
| sector:Industrials | 33% | 30 | ×0.50 |
| sector:Real Estate | 53% | 30 | ×0.85 |
| sector:Technology | 37% | 30 | ×0.50 |
| sector:Utilities | 39% | 28 | ×0.50 |

## Horizon weights — book_policy.json v15 · renormalized (absent: general)

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.13 | 0.11 | 0.00 | 0.27 | 0.27 | 0.22 | additive |
| 3d | 0.17 | 0.15 | 0.00 | 0.17 | 0.28 | 0.22 | additive |
| 1w | 0.20 | 0.17 | 0.00 | 0.11 | 0.30 | 0.22 | additive |
| 2w | 0.22 | 0.20 | 0.00 | 0.07 | 0.30 | 0.22 | additive |
| 1m | 0.24 | 0.22 | 0.00 | 0.00 | 0.33 | 0.22 | additive |

## 1d BUY — why these names

### 1. HITI · $250M micro · Healthcare

**1d score +0.374**

**HITI** is a liquid **micro-cap** Healthcare name (Pharmaceutical Retailers) at $250M, ADV ~541k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | -0.16 | -0.021 | does this *kind* of stock fit today's regime? |
| sector predict | 0.11 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.27 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.27 | +0.55 | +0.151 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.76 | +0.164 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.08 | +0.080 | liquid small/mid, room to run |
| **1d total** | | | **+0.374** | |

### 2. CON · $4.5B mid · Healthcare

**1d score +0.561**

**CON** is a liquid **mid-cap** Healthcare name (Medical Care Facilities) at $4.5B, ADV ~942k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | -0.09 | -0.011 | does this *kind* of stock fit today's regime? |
| sector predict | 0.11 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.27 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.27 | +0.88 | +0.239 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.52 | +0.112 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.021 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.561** | |

### 3. ASTH · $1.7B small · Healthcare

**1d score +0.575**

**ASTH** is a liquid **small-cap** Healthcare name (Medical Care Facilities) at $1.7B, ADV ~523k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | -0.15 | -0.020 | does this *kind* of stock fit today's regime? |
| sector predict | 0.11 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.27 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.27 | +0.70 | +0.191 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.61 | +0.132 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.021 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.575** | |

### 4. NTAP · $40.2B large · Technology

**1d score +0.402**

**NTAP** is a liquid **large-cap** Technology name (Software - Infrastructure) at $40.2B, ADV ~2546k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.92 | +0.120 | does this *kind* of stock fit today's regime? |
| sector predict | 0.11 | +0.20 | +0.022 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.27 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.27 | +0.88 | +0.239 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.65 | +0.142 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.402** | |

### 5. AMN · $1.3B small · Healthcare

**1d score +0.545**

**AMN** is a liquid **small-cap** Healthcare name (Medical Care Facilities) at $1.3B, ADV ~702k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | -0.15 | -0.020 | does this *kind* of stock fit today's regime? |
| sector predict | 0.11 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.27 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.27 | +0.85 | +0.230 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.29 | +0.063 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.021 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.545** | |

### 6. BAND · $2.0B mid · Technology

**1d score +0.734**

**BAND** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $2.0B, ADV ~1056k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.76 | +0.099 | does this *kind* of stock fit today's regime? |
| sector predict | 0.11 | +0.20 | +0.022 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.27 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.27 | +0.76 | +0.207 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.72 | +0.156 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.734** | |

### 7. MQ · $1.8B small · Technology

**1d score +0.791**

**MQ** is a liquid **small-cap** Technology name (Software - Infrastructure) at $1.8B, ADV ~971k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.97 | +0.126 | does this *kind* of stock fit today's regime? |
| sector predict | 0.11 | +0.20 | +0.022 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.27 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.27 | +0.70 | +0.191 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.93 | +0.202 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.791** | |

### 8. AVPT · $2.9B mid · Technology

**1d score +0.790**

**AVPT** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $2.9B, ADV ~2025k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.99 | +0.129 | does this *kind* of stock fit today's regime? |
| sector predict | 0.11 | +0.20 | +0.022 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.27 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.27 | +0.94 | +0.256 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.61 | +0.134 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.790** | |

### 9. PACS · $6.8B mid · Healthcare

**1d score +0.680**

**PACS** is a liquid **mid-cap** Healthcare name (Medical Care Facilities) at $6.8B, ADV ~830k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | -0.15 | -0.020 | does this *kind* of stock fit today's regime? |
| sector predict | 0.11 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.27 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.27 | +0.88 | +0.239 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.74 | +0.161 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.05 | +0.049 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.680** | |

### 10. IT · $11.7B large · Technology

**1d score +0.446**

**IT** is a liquid **large-cap** Technology name (Information Technology Services) at $11.7B, ADV ~1306k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.99 | +0.129 | does this *kind* of stock fit today's regime? |
| sector predict | 0.11 | +0.20 | +0.022 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.27 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.27 | +0.24 | +0.067 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.69 | +0.149 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1d total** | | | **+0.446** | |

### 11. MD · $2.1B mid · Healthcare

**1d score +0.475**

**MD** is a liquid **mid-cap** Healthcare name (Medical Care Facilities) at $2.1B, ADV ~774k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | -0.15 | -0.020 | does this *kind* of stock fit today's regime? |
| sector predict | 0.11 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.27 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.27 | +0.76 | +0.207 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.08 | +0.017 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.021 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.475** | |

### 12. TOST · $17.7B large · Technology

**1d score +0.600**

**TOST** is a liquid **large-cap** Technology name (Software - Infrastructure) at $17.7B, ADV ~10515k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.96 | +0.126 | does this *kind* of stock fit today's regime? |
| sector predict | 0.11 | +0.20 | +0.022 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.27 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.27 | +0.85 | +0.230 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.65 | +0.142 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1d total** | | | **+0.600** | |

### 13. ACN · $107.0B large · Technology

**1d score +0.219**

**ACN** is a liquid **large-cap** Technology name (Information Technology Services) at $107.0B, ADV ~6614k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.95 | +0.124 | does this *kind* of stock fit today's regime? |
| sector predict | 0.11 | +0.20 | +0.022 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.27 | +0.31 | +0.084 | headlines + news-judge ticker tilts |
| AB checklist | 0.27 | -0.24 | -0.067 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | -0.24 | -0.052 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1d total** | | | **+0.219** | |


## 1d AVOID — bottom of the same rank

- **NEOV** (micro, Industrials, $133M) score -0.270. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -4.7%
- **OKLO** (mid, Utilities, $6.9B) score -0.200. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -6.3%
- **FCEL** (small, Industrials, $1.3B) score -0.122. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -4.7%
- **EOSE** (small, Industrials, $1.1B) score -0.254. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -4.7%
- **ENVX** (small, Industrials, $562M) score -0.097. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -4.7%
- **TE** (small, Industrials, $1.1B) score -0.236. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -4.7%
- **SKYX** (micro, Industrials, $158M) score -0.262. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -4.7%
- **NNDM** (small, Technology, $324M) score +0.114. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow; child lags parent -3.1%
- **MTZ** (large, Industrials, $16.3B) score +0.019. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow
- **LUNR** (mid, Industrials, $3.5B) score -0.201. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow
- **METC** (small, Basic Materials, $518M) score -0.208. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -5.7%
- **AMSC** (small, Industrials, $1.4B) score -0.012. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow
- **PLUG** (mid, Industrials, $2.6B) score -0.125. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -4.7%
- **INDI** (small, Technology, $668M) score -0.136. SELL/AVOID — market=GREEN; red domains=parent,child,setup,flow
- **NRG** (large, Utilities, $20.4B) score -0.086. SELL/AVOID — market=GREEN; red domains=parent,child,setup; child lags parent -6.3%

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | PDFS | +0.890 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 2 | CBRL | +0.888 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | AVPT | +0.852 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | MQ | +0.850 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | SONO | +0.812 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 6 | CDNA | +0.789 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | BLFS | +0.780 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 8 | PRCH | +0.771 | small | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 9 | FIGS | +0.742 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | SIG | +0.735 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 11 | CAKE | +0.735 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 12 | ETON | +0.715 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 13 | ADMA | +0.701 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 14 | SM | +0.677 | mid | Energy | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | EZPW | +0.669 | small | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 16 | JXN | +0.641 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 17 | SPNT | +0.641 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 18 | TBBB | +0.633 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 19 | IMAX | +0.569 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 20 | COP | +0.567 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 21 | TAL | +0.558 | mid | Consumer Defensive | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 22 | WLY | +0.540 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 23 | RRC | +0.535 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 24 | SB | +0.534 | small | Industrials | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 25 | LAUR | +0.522 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | PDFS | +0.933 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 2 | CBRL | +0.928 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | AVPT | +0.898 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | MQ | +0.890 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | SONO | +0.859 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 6 | CDNA | +0.820 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | BLFS | +0.809 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 8 | PRCH | +0.802 | small | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 9 | FIGS | +0.782 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | ARHS | +0.774 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 11 | SIG | +0.772 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 12 | ETON | +0.742 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 13 | ADMA | +0.734 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 14 | EZPW | +0.700 | small | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | SPNT | +0.674 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 16 | JXN | +0.674 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 17 | TBBB | +0.646 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 18 | SM | +0.638 | mid | Energy | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 19 | TAL | +0.585 | mid | Consumer Defensive | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 20 | IMAX | +0.582 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 21 | SB | +0.565 | small | Industrials | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 22 | ASC | +0.549 | small | Industrials | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 23 | WLY | +0.549 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 24 | COP | +0.539 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 25 | LAUR | +0.537 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | PDFS | +0.958 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 2 | CBRL | +0.948 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | AVPT | +0.924 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | MQ | +0.916 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | SONO | +0.885 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 6 | CDNA | +0.830 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | BLFS | +0.819 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 8 | PRCH | +0.818 | small | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 9 | FIGS | +0.805 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | ARHS | +0.796 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 11 | SIG | +0.795 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 12 | ETON | +0.749 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 13 | ADMA | +0.748 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 14 | EZPW | +0.715 | small | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | JXN | +0.691 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 16 | SPNT | +0.690 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 17 | TBBB | +0.643 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 18 | SM | +0.601 | mid | Energy | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 19 | TAL | +0.594 | mid | Consumer Defensive | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 20 | IMAX | +0.580 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 21 | SB | +0.578 | small | Industrials | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 22 | ASC | +0.561 | small | Industrials | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 23 | WLY | +0.541 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 24 | LAUR | +0.537 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 25 | EQPT | +0.515 | mid | Industrials | the Finviz industry was **down** |

## 1m BUY — why these names

### 1. PDFS · $2.4B mid · Technology

**1m score +1.001**

**PDFS** is a liquid **mid-cap** Technology name (Software - Application) at $2.4B, ADV ~603k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.94 | +0.224 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.20 | +0.043 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.85 | +0.277 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.95 | +0.207 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.001** | |

### 2. CBRL · $1.2B small · Consumer Cyclical

**1m score +0.989**

**CBRL** is a liquid **small-cap** Consumer Cyclical name (Restaurants) at $1.2B, ADV ~961k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.85 | +0.204 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.08 | +0.017 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.93 | +0.302 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.99 | +0.215 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.989** | |

### 3. AVPT · $2.9B mid · Technology

**1m score +0.970**

**AVPT** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $2.9B, ADV ~2025k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.99 | +0.236 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.20 | +0.043 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.94 | +0.307 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.61 | +0.134 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.970** | |

### 4. MQ · $1.8B small · Technology

**1m score +0.956**

**MQ** is a liquid **small-cap** Technology name (Software - Infrastructure) at $1.8B, ADV ~971k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.97 | +0.231 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.20 | +0.043 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.70 | +0.230 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.93 | +0.202 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.956** | |

### 5. SONO · $2.0B mid · Technology

**1m score +0.931**

**SONO** is a liquid **mid-cap** Technology name (Consumer Electronics) at $2.0B, ADV ~1940k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.99 | +0.236 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.20 | +0.043 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.96 | +0.314 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.27 | +0.060 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.931** | |

### 6. CDNA · $3.4B mid · Healthcare

**1m score +0.861**

**CDNA** is a liquid **mid-cap** Healthcare name (Diagnostics & Research) at $3.4B, ADV ~1185k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.49 | +0.117 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.93 | +0.302 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.88 | +0.192 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.861** | |

### 7. PRCH · $1.9B small · Financial

**1m score +0.849**

**PRCH** is a liquid **small-cap** Financial name (Insurance - Property & Casualty) at $1.9B, ADV ~1644k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.94 | +0.224 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | -0.20 | -0.043 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.70 | +0.230 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.87 | +0.189 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.849** | |

### 8. BLFS · $1.9B small · Healthcare

**1m score +0.849**

**BLFS** is a liquid **small-cap** Healthcare name (Medical Instruments & Supplies) at $1.9B, ADV ~1082k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.46 | +0.110 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.91 | +0.295 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.89 | +0.194 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.849** | |

### 9. FIGS · $2.3B mid · Consumer Cyclical

**1m score +0.846**

**FIGS** is a liquid **mid-cap** Consumer Cyclical name (Apparel Manufacturing) at $2.3B, ADV ~2935k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.98 | +0.235 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.08 | +0.017 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.81 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.37 | +0.079 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.846** | |

### 10. ARHS · $1.4B small · Consumer Cyclical

**1m score +0.838**

**ARHS** is a liquid **small-cap** Consumer Cyclical name (Specialty Retail) at $1.4B, ADV ~1341k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.93 | +0.223 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.08 | +0.017 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.91 | +0.295 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.24 | +0.052 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.838** | |

### 11. SIG · $3.9B mid · Consumer Cyclical

**1m score +0.832**

**SIG** is a liquid **mid-cap** Consumer Cyclical name (Luxury Goods) at $3.9B, ADV ~831k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.99 | +0.236 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.08 | +0.017 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.64 | +0.207 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.43 | +0.094 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.832** | |

### 12. ADMA · $2.3B mid · Healthcare

**1m score +0.782**

**ADMA** is a liquid **mid-cap** Healthcare name (Biotechnology) at $2.3B, ADV ~2634k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.65 | +0.154 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.91 | +0.295 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.38 | +0.083 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.782** | |

### 13. ETON · $1.7B small · Healthcare

**1m score +0.777**

**ETON** is a liquid **small-cap** Healthcare name (Drug Manufacturers - Specialty & Generic) at $1.7B, ADV ~559k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.31 | +0.074 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.96 | +0.314 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.63 | +0.138 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.777** | |

### 14. EZPW · $1.9B small · Financial

**1m score +0.746**

**EZPW** is a liquid **small-cap** Financial name (Credit Services) at $1.9B, ADV ~974k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.87 | +0.209 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | -0.20 | -0.043 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.76 | +0.248 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.38 | +0.082 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.746** | |

### 15. SPNT · $2.9B mid · Financial

**1m score +0.723**

**SPNT** is a liquid **mid-cap** Financial name (Insurance - Reinsurance) at $2.9B, ADV ~801k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.92 | +0.220 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | -0.20 | -0.043 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.81 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.02 | +0.005 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.723** | |

### 16. JXN · $8.9B mid · Financial

**1m score +0.723**

**JXN** is a liquid **mid-cap** Financial name (Insurance - Life) at $8.9B, ADV ~700k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.98 | +0.235 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | -0.20 | -0.043 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.70 | +0.230 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.24 | +0.052 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.723** | |

### 17. TBBB · $6.1B mid · Consumer Defensive

**1m score +0.656**

**TBBB** is a liquid **mid-cap** Consumer Defensive name (Discount Stores) at $6.1B, ADV ~669k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | -0.15 | -0.036 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.76 | +0.248 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.89 | +0.194 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.656** | |

### 18. TAL · $4.0B mid · Consumer Defensive

**1m score +0.621**

**TAL** is a liquid **mid-cap** Consumer Defensive name (Education & Training Services) at $4.0B, ADV ~4615k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.40 | +0.095 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.85 | +0.277 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | -0.00 | -0.001 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.621** | |

### 19. SB · $939M small · Industrials

**1m score +0.609**

**SB** is a liquid **small-cap** Industrials name (Marine Shipping) at $939M, ADV ~1102k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.41 | +0.099 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.20 | +0.043 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.81 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | -0.22 | -0.047 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.609** | |

### 20. IMAX · $3.1B mid · Communication Services

**1m score +0.593**

**IMAX** is a liquid **mid-cap** Communication Services name (Entertainment) at $3.1B, ADV ~1238k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.07 | +0.017 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | -0.20 | -0.043 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.76 | +0.248 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.56 | +0.122 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.593** | |

### 21. ASC · $741M small · Industrials

**1m score +0.589**

**ASC** is a liquid **small-cap** Industrials name (Marine Shipping) at $741M, ADV ~614k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.36 | +0.087 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.20 | +0.043 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.70 | +0.230 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | -0.10 | -0.021 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.589** | |

### 22. KEX · $6.9B mid · Industrials

**1m score +0.577**

**KEX** is a liquid **mid-cap** Industrials name (Marine Shipping) at $6.9B, ADV ~521k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Today's **news/judge** is a headwind for this ticker.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | -0.01 | -0.002 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.20 | +0.043 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | -0.68 | -0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.85 | +0.277 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.04 | +0.009 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.577** | |

### 23. SM · $8.0B mid · Energy

**1m score +0.562**

**SM** is a liquid **mid-cap** Energy name (Oil & Gas E&P) at $8.0B, ADV ~3886k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Today's **news/judge** is a tailwind for this ticker.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.26 | +0.062 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | -0.20 | -0.043 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.90 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.85 | +0.277 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | -0.05 | -0.011 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.562** | |

### 24. LAUR · $5.2B mid · Consumer Defensive

**1m score +0.552**

**LAUR** is a liquid **mid-cap** Consumer Defensive name (Education & Training Services) at $5.2B, ADV ~1003k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | +0.03 | +0.007 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.64 | +0.207 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.71 | +0.153 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.02 | -0.016 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.552** | |

### 25. WLY · $2.4B mid · Communication Services

**1m score +0.550**

**WLY** is a liquid **mid-cap** Communication Services name (Publishing) at $2.4B, ADV ~509k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.24 | -0.15 | -0.036 | does this *kind* of stock fit today's regime? |
| sector predict | 0.22 | -0.20 | -0.043 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.33 | +0.76 | +0.248 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.38 | +0.082 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.05 | +0.049 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.550** | |


## 1m AVOID — bottom of the same rank

- **ROIV** (large, Healthcare, $26.5B) score -0.551. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SOC** (small, Energy, $702M) score -0.523. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **GOGO** (micro, Communication Services, $301M) score -0.517. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NFLX** (mega, Communication Services, $287.7B) score -0.497. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BIDU** (large, Communication Services, $23.9B) score -0.494. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **IBRX** (large, Healthcare, $9.0B) score -0.480. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **BAK** (micro, Basic Materials, $257M) score -0.471. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AIRS** (micro, Healthcare, $138M) score -0.464. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BORR** (small, Energy, $1.2B) score -0.457. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NRXP** (micro, Healthcare, $126M) score -0.455. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ASPI** (small, Basic Materials, $402M) score -0.448. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SSP** (micro, Communication Services, $249M) score -0.448. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **IMNM** (mid, Healthcare, $2.5B) score -0.447. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LBTYK** (mid, Communication Services, $3.2B) score -0.444. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TYRA** (small, Healthcare, $1.5B) score -0.444. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PLAY** (micro, Communication Services, $223M) score -0.443. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PRM** (mid, Basic Materials, $4.6B) score -0.437. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TMC** (small, Basic Materials, $1.7B) score -0.436. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SBS** (large, Utilities, $18.0B) score -0.434. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ANGI** (micro, Communication Services, $207M) score -0.432. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BHVN** (small, Healthcare, $2.1B) score -0.424. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **LFMD** (micro, Healthcare, $141M) score -0.419. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SSTK** (micro, Communication Services, $150M) score -0.418. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **HUT** (large, Financial, $11.6B) score -0.417. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LXRX** (small, Healthcare, $845M) score -0.414. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-10-06_stock_book.md`
- Machine table: `data/stock_book/2026-10-06_stock_book.csv`
- Machine book: `data/stock_book/2026-10-06_stock_book.json`
- Join rank: `data/join/2026-10-06_ranked.csv`
- Weather: `01_daily/weather/2026-10-06_weather.md`
- AB enrich: `data/ab_checklist/2026-10-06_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-10-06_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-10-06_map_heat.md`
