# Stock book — 2026-10-28

_Generated 2026-10-28T08:19:55.411969-04:00_

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

- Weather risk: **unknown**
- General predict (same-day): +0.00  (MISSING → 0)
- Stand-down: **no** — 39 names qualified through standard,group_leader,catalyst (1 probable)
- Sector predicts this date: 0/11 (missing → sector layer is 0; Finviz week tape still sits in join)
- News tickers in play: 46
- AB coverage: 1915 names · peer RS: 1804
- Universe after liquidity: 2035
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 14

## All-green BUY / SELL

- Mode: **weighted_fallback** · SELL **core_weights**
- Pile: **0** liquid all-green names (need ≥ 8) of 2035
- Core fired: join=yes, AB=yes, peer=yes
- pile 0 < 8 liquid all-green names. Fallback weighted walk; SELL stays on core

## Decision lattice — gate → route → rank

The weighted score is now a tie-breaker inside an eligible lane. It cannot average away a market, group, company, or setup veto.

### MARKET: 🟡 YELLOW

- YELLOW: general flat score=+0.00; good=+0.0 vs bad=+0.0; risk=unknown; red pillars=0
- Allowed long lanes: **standard, group_leader, catalyst** · max slots 8 · size ×0.60

Decision domains: **MKT · parent · child · company · setup · flow**. Measured parent/child tape is kept separate from the LLM essay; direct company events must be price-confirmed on a hard-red day.

### Bull decisions (eligible or closest blocked cases)

| # | Ticker | Domains | Lane | Company / group | Decision |
|---:|--------|---------|------|-----------------|----------|
| 1 | **HITI** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Pharmaceutical Retailers +3.6% d1 / +1.0% 1w / +3.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=⚪,Cond green |
| 2 | **UPXI** | 🟡🟢🟢🟡🟢🟢 | standard | no direct company event; Internet Content & Information +3.0% d1 / +4.2% 1w / +0.8% vs parent | BUY STANDARD — market=YELLOW; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 3 | **XNDU** | 🟡🔴🟢🟡🟢🟢 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 4 | **INFQ** | 🟡🔴🟢🟡🟢🟡 | group_leader | no direct company event; Software - Infrastructure +2.8% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW |
| 5 | **ADCT** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Drug Manufacturers - Specialty & Generic +1.7% d1 / -0.7% 1w / +1.8% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=⚪ |
| 6 | **AUTL** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Biotechnology +0.6% d1 / -3.2% 1w / -0.8% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=⚪ |
| 7 | **SIRI** | 🟡🟢🟢🟡🟢🟡 | standard | no direct company event; Entertainment +2.5% d1 / +2.0% 1w / -1.5% vs parent | BUY STANDARD — market=YELLOW; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=Cond green |
| 8 | **FDMT** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Biotechnology +0.6% d1 / -3.2% 1w / -0.8% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=⚪ |
| 9 | **ETD** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Furnishings, Fixtures & Appliances +2.4% d1 / -4.0% 1w / -2.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=⚪ |
| 10 | **DSGN** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Biotechnology +0.6% d1 / -3.2% 1w / -0.8% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 11 | **GENB** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Biotechnology +0.6% d1 / -3.2% 1w / -0.8% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 12 | **SRRK** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Biotechnology +0.6% d1 / -3.2% 1w / -0.8% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 13 | **TWST** | 🟡🟡🟡🟡🟢🟡 | standard | no direct company event; Biotechnology +0.6% d1 / -3.2% 1w / -0.8% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=YELLOW |
| 14 | **IONS** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Biotechnology +0.6% d1 / -3.2% 1w / -0.8% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 15 | **QTTB** | 🟡🟡🟡🟡🟢🟡 | standard | no direct company event; Biotechnology +0.6% d1 / -3.2% 1w / -0.8% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=YELLOW |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **OKLO** | 🟡🔴🔴🟡🔴🔴 | Utilities - Independent Power Producers | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -6.3% |
| 2 | **NEOV** | 🟡🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 3 | **FCEL** | 🟡🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 4 | **POWL** | 🟡🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 5 | **NNDM** | 🟡🔴🔴🟡🔴🔴 | Computer Hardware | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.1% |
| 6 | **EOSE** | 🟡🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -4.7% |
| 7 | **TE** | 🟡🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -4.7% |
| 8 | **LUNR** | 🟡🔴🔴🟡🔴🔴 | Aerospace & Defense | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 9 | **METC** | 🟡🔴🔴🟡🔴🟡 | Coking Coal | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -5.7% |
| 10 | **PLUG** | 🟡🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -4.7% |
| 11 | **SKYX** | 🟡🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -4.7% |
| 12 | **SIDU** | 🟡🔴🔴🟡🔴🔴 | Aerospace & Defense | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 13 | **LPTH** | 🟡🔴🔴🟡🔴🔴 | Electronic Components | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 14 | **QUBT** | 🟡🔴🔴🟡🔴🟡 | Computer Hardware | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.1% |
| 15 | **AMPG** | 🟡🔴🔴🟡🔴🔴 | Communication Equipment | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **finviz_tape** (40 captains, 15 industries → s_heat).
- Board file: `01_daily/map_heat/2026-10-28_map_heat.json` · generated 2026-10-28T04:38:34.802468-04:00

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
| News parse + actions | **missing / not in ranker** | s_news |
| News judge | **found** | s_news ticker tilts |
| Finviz daily digest | **found** | s_news company headlines |
| General predict | **missing / not in ranker** | s_general × beta |
| Sector LLM essays | **missing / not in ranker** | s_sector (0 if essays missing) |
| AB checklist + P01–P04 | **found** | s_ab |
| Peer RS | **found** | s_peer |
| Ticker checklist (rebound) | **found** | rebound_floor (dated file, else latest — can be stale) |
| Event scanner | **found** | sector tilt + weather |
| Finviz map heat (industry RS / themes) | **found** | industry residual + theme tape → s_heat when research is gone |
| Map heat captain research | **missing / not in ranker** | Grok captain essays (strict morning_refresh; else Finviz tape) |
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
| general | 51% | 53 | ×0.85 |
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

## Horizon weights — book_policy.json v17 · renormalized (absent: sector, general)

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.15 | 0.00 | 0.00 | 0.30 | 0.30 | 0.24 | additive |
| 3d | 0.21 | 0.00 | 0.00 | 0.21 | 0.33 | 0.26 | additive |
| 1w | 0.24 | 0.00 | 0.00 | 0.13 | 0.37 | 0.26 | additive |
| 2w | 0.27 | 0.00 | 0.00 | 0.08 | 0.38 | 0.27 | additive |
| 1m | 0.31 | 0.00 | 0.00 | 0.00 | 0.42 | 0.28 | additive |

## 1d BUY — why these names

### 1. HITI · $250M micro · Healthcare

**1d score +0.492**

**HITI** is a liquid **micro-cap** Healthcare name (Pharmaceutical Retailers) at $250M, ADV ~541k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.40 | +0.058 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.55 | +0.169 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.76 | +0.185 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.08 | +0.080 | liquid small/mid, room to run |
| **1d total** | | | **+0.492** | |

### 2. ADCT · $161M micro · Healthcare

**1d score +0.614**

**ADCT** is a liquid **micro-cap** Healthcare name (Drug Manufacturers - Specialty & Generic) at $161M, ADV ~1396k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.17 | +0.024 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.215 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.88 | +0.215 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.16 | +0.160 | liquid small/mid, room to run |
| **1d total** | | | **+0.614** | |

### 3. AUTL · $495M small · Healthcare

**1d score +0.383**

**AUTL** is a liquid **small-cap** Healthcare name (Biotechnology) at $495M, ADV ~2028k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.20 | +0.029 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.12 | +0.038 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.48 | +0.116 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.383** | |

### 4. SIRI · $8.7B mid · Communication Services

**1d score -0.106**

**SIRI** is a liquid **mid-cap** Communication Services name (Entertainment) at $8.7B, ADV ~4070k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **miss**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather are a **headwind** (sector stamp or hostile tape).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | -0.74 | -0.109 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | -0.24 | -0.075 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | -0.50 | -0.123 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **-0.106** | |

### 5. FDMT · $804M small · Healthcare

**1d score +0.280**

**FDMT** is a liquid **small-cap** Healthcare name (Biotechnology) at $804M, ADV ~932k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | -0.02 | -0.003 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.00 | +0.000 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.34 | +0.082 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.280** | |


## 1d AVOID — bottom of the same rank

- **OKLO** (mid, Utilities, $6.9B) score -0.180. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -6.3%
- **NEOV** (micro, Industrials, $133M) score -0.382. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.7%
- **FCEL** (small, Industrials, $1.3B) score -0.260. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.7%
- **POWL** (mid, Industrials, $7.1B) score -0.130. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.7%
- **NNDM** (small, Technology, $324M) score -0.095. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.1%
- **EOSE** (small, Industrials, $1.1B) score -0.320. SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -4.7%
- **TE** (small, Industrials, $1.1B) score -0.245. SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -4.7%
- **LUNR** (mid, Industrials, $3.5B) score -0.257. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | ETD | +0.582 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | AUTL | +0.404 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | SRRK | +0.369 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | BRKR | +0.363 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 5 | NEO | +0.339 | mid | Healthcare | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | VET | +0.233 | small | Energy | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 7 | SMWB | +0.132 | small | Technology | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 8 | SIRI | -0.163 | mid | Communication Services | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | ETD | +0.630 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | AUTL | +0.418 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | BRKR | +0.378 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 4 | SRRK | +0.369 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | NEO | +0.358 | mid | Healthcare | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | VET | +0.236 | small | Energy | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 7 | SMWB | +0.135 | small | Technology | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 8 | SIRI | -0.199 | mid | Communication Services | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | ETD | +0.665 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | AUTL | +0.429 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | BRKR | +0.378 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 4 | SRRK | +0.370 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | NEO | +0.368 | mid | Healthcare | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | VET | +0.233 | small | Energy | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 7 | SMWB | +0.136 | small | Technology | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 8 | SIRI | -0.230 | mid | Communication Services | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1m BUY — why these names

### 1. ETD · $534M small · Consumer Cyclical

**1m score +0.718**

**ETD** is a liquid **small-cap** Consumer Cyclical name (Furnishings, Fixtures & Appliances) at $534M, ADV ~597k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.87 | +0.266 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.55 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.07 | +0.020 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.718** | |

### 2. AUTL · $495M small · Healthcare

**1m score +0.444**

**AUTL** is a liquid **small-cap** Healthcare name (Biotechnology) at $495M, ADV ~2028k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.20 | +0.061 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.12 | +0.052 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.48 | +0.132 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.444** | |

### 3. BRKR · $9.6B mid · Healthcare

**1m score +0.395**

**BRKR** is a liquid **mid-cap** Healthcare name (Medical Devices) at $9.6B, ADV ~2237k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | -0.15 | -0.046 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.55 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.03 | +0.009 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.395** | |

### 4. NEO · $2.5B mid · Healthcare

**1m score +0.388**

**NEO** is a liquid **mid-cap** Healthcare name (Diagnostics & Research) at $2.5B, ADV ~2360k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.21 | +0.064 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.36 | +0.149 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | -0.09 | -0.025 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.388** | |

### 5. SRRK · $6.0B mid · Healthcare

**1m score +0.370**

**SRRK** is a liquid **mid-cap** Healthcare name (Biotechnology) at $6.0B, ADV ~1465k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | -0.15 | -0.046 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.00 | +0.000 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.78 | +0.216 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.370** | |

### 6. VET · $1.8B small · Energy

**1m score +0.237**

**VET** is a liquid **small-cap** Energy name (Oil & Gas E&P) at $1.8B, ADV ~1438k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | -0.15 | -0.046 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.24 | +0.102 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | -0.07 | -0.019 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.237** | |

### 7. SMWB · $665M small · Technology

**1m score +0.140**

**SMWB** is a liquid **small-cap** Technology name (Software - Application) at $665M, ADV ~888k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.13 | +0.040 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.12 | +0.052 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | -0.73 | -0.202 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.140** | |

### 8. SIRI · $8.7B mid · Communication Services

**1m score -0.269**

**SIRI** is a liquid **mid-cap** Communication Services name (Entertainment) at $8.7B, ADV ~4070k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **miss**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather are a **headwind** (sector stamp or hostile tape).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | -0.74 | -0.228 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | -0.24 | -0.102 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | -0.50 | -0.140 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **-0.269** | |


## 1m AVOID — bottom of the same rank

- **ROIV** (large, Healthcare, $26.5B) score -0.918. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SOC** (small, Energy, $702M) score -0.726. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **BNTX** (large, Healthcare, $24.6B) score -0.711. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **WVE** (small, Healthcare, $749M) score -0.708. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **EYPT** (micro, Healthcare, $287M) score -0.706. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **VNET** (small, Technology, $1.7B) score -0.692. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ALMS** (small, Healthcare, $953M) score -0.690. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **AXSM** (large, Healthcare, $10.2B) score -0.680. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ASPI** (small, Basic Materials, $402M) score -0.668. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CRVS** (small, Healthcare, $1.0B) score -0.654. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CNXC** (small, Technology, $1.5B) score -0.652. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BIDU** (large, Communication Services, $23.9B) score -0.650. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SSTK** (micro, Communication Services, $150M) score -0.644. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LFMD** (micro, Healthcare, $141M) score -0.638. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LBTYK** (mid, Communication Services, $3.2B) score -0.636. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ANGI** (micro, Communication Services, $207M) score -0.635. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ARE** (mid, Real Estate, $8.5B) score -0.623. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LILAK** (small, Communication Services, $1.6B) score -0.623. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PRM** (mid, Basic Materials, $4.6B) score -0.613. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **IMNM** (mid, Healthcare, $2.5B) score -0.612. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ARDX** (small, Healthcare, $860M) score -0.609. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **TDTH** (small, Technology, $1.3B) score -0.606. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **MIDD** (mid, Industrials, $4.8B) score -0.604. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **GOGO** (small, Communication Services, $301M) score -0.601. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CGEM** (small, Healthcare, $962M) score -0.601. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-10-28_stock_book.md`
- Machine table: `data/stock_book/2026-10-28_stock_book.csv`
- Machine book: `data/stock_book/2026-10-28_stock_book.json`
- Join rank: `data/join/2026-10-28_ranked.csv`
- Weather: `01_daily/weather/2026-10-28_weather.md`
- AB enrich: `data/ab_checklist/2026-10-28_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-10-28_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-10-28_map_heat.md`
