# Stock book — 2026-10-07

_Generated 2026-10-07T06:05:44.829807-04:00_

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
- General predict (same-day): -0.60 down (present)
- Stand-down: **no** — 2 names qualified through group_leader,catalyst,probable (2 probable)
- Sector predicts this date: 0/11 (missing → sector layer is 0; Finviz week tape still sits in join)
- News tickers in play: 156
- AB coverage: 1932 names · peer RS: 1804
- Universe after liquidity: 2033
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 82

## All-green BUY / SELL

- Mode: **weighted_fallback** · SELL **core_weights**
- Pile: **0** liquid all-green names (need ≥ 8) of 2033
- Core fired: join=yes, AB=yes, peer=yes
- pile 0 < 8 liquid all-green names. Fallback weighted walk; SELL stays on core

## Decision lattice — gate → route → rank

The weighted score is now a tie-breaker inside an eligible lane. It cannot average away a market, group, company, or setup veto.

### MARKET: 🔴 RED

- RED: general down score=-5.21; good=+0.0 vs bad=+0.0; risk=off; red pillars=0
- Allowed long lanes: **group_leader, catalyst, probable** · max slots 8 · size ×0.35

Decision domains: **MKT · parent · child · company · setup · flow**. Measured parent/child tape is kept separate from the LLM essay; direct company events must be price-confirmed on a hard-red day.

### Bull decisions (eligible or closest blocked cases)

| # | Ticker | Domains | Lane | Company / group | Decision |
|---:|--------|---------|------|-----------------|----------|
| 1 | **WH** | 🔴🟡🟡🟡🟢🟢 | probable | basket/action net=+2.24; context only, not a company catalyst; Lodging +1.3% d1 / -0.6% 1w / +1.4% vs parent | BUY PROBABLE — most-probable long on RED (size ×0.35); clocks: lookback 🔵 blue — market=RED; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.15); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 2 | **SIRI** | 🔴🟢🟢🟡🟢🟡 | probable | no direct company event; Entertainment +2.5% d1 / +2.0% 1w / -1.5% vs parent | BUY PROBABLE — most-probable long on RED (size ×0.35); clocks: lookback 🔵 blue — market=RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,Cond green |
| 3 | **META** | 🔴🟢🟢🟡🟢🟡 | blocked | direct normal digest (stale/undated): Wells Fargo raises Meta Platforms price target to $1,000 from $796, reiterates Overweight on strong Muse AI product cycle upside; Internet Content & Information +3.0% d1 / +4.2% 1w / +0.8% vs parent | BLOCK BUY — RED market: no confirmed catalyst, group leader, or probable clock; direct catalyst lacks price confirmation; v2 domain alarm / market=RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.30); setup=GREEN; flow=YELLOW |
| 4 | **HITI** | 🔴🟡🟢🟡🟢🟢 | blocked | no direct company event; Pharmaceutical Retailers +3.6% d1 / +1.0% 1w / +3.5% vs parent | BLOCK BUY — RED market: no confirmed catalyst, group leader, or probable clock; v2 domain alarm / market=RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 5 | **CON** | 🔴🟡🟢🟡🟢🟢 | blocked | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BLOCK BUY — RED market: no confirmed catalyst, group leader, or probable clock; v2 domain alarm / market=RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 6 | **OLLI** | 🔴🟢🟢🟡🟢🟢 | blocked | no direct company event; Discount Stores +1.8% d1 / +0.8% 1w / +0.3% vs parent | BLOCK BUY — RED market: no confirmed catalyst, group leader, or probable clock; v2 domain alarm / market=RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 7 | **BJ** | 🔴🟢🟢🟡🟢🟢 | blocked | no direct company event; Discount Stores +1.8% d1 / +0.8% 1w / +0.3% vs parent | BLOCK BUY — RED market: no confirmed catalyst, group leader, or probable clock; v2 domain alarm / market=RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 8 | **MD** | 🔴🟡🟢🟡🟢🟢 | blocked | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BLOCK BUY — RED market: no confirmed catalyst, group leader, or probable clock; v2 domain alarm / market=RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 9 | **WLY** | 🔴🟢🟢🟡🟢🟢 | blocked | no direct company event; Publishing +3.8% d1 / +2.5% 1w / -1.0% vs parent | BLOCK BUY — RED market: no confirmed catalyst, group leader, or probable clock; v2 domain alarm / market=RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 10 | **ATHM** | 🔴🟢🟢🟡🟢🟢 | blocked | no direct company event; Internet Content & Information +3.0% d1 / +4.2% 1w / +0.8% vs parent | BLOCK BUY — RED market: no confirmed catalyst, group leader, or probable clock; v2 domain alarm / market=RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 11 | **UHS** | 🔴🟡🟢🟡🟡🟢 | blocked | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BLOCK BUY — RED market: no confirmed catalyst, group leader, or probable clock; setup YELLOW; v2 domain alarm / market=RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=YELLOW; flow=GREEN |
| 12 | **CNK** | 🔴🟢🟢🟡🟢🟢 | blocked | no direct company event; Entertainment +2.5% d1 / +2.0% 1w / -1.5% vs parent | BLOCK BUY — RED market: no confirmed catalyst, group leader, or probable clock; v2 domain alarm / market=RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN |
| 13 | **OPCH** | 🔴🟡🟢🟡🟢🟡 | blocked | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BLOCK BUY — no lane qualified / market=RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW |
| 14 | **AVAH** | 🔴🟡🟢🟡🟢🟡 | blocked | no direct company event; Medical Care Facilities +0.6% d1 / +3.1% 1w / +5.6% vs parent | BLOCK BUY — RED market: no confirmed catalyst, group leader, or probable clock; v2 domain alarm / market=RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW |
| 15 | **SPOT** | 🔴🟢🟢🟡🟡🟢 | blocked | no direct company event; Internet Content & Information +3.0% d1 / +4.2% 1w / +0.8% vs parent | BLOCK BUY — RED market: no confirmed catalyst, group leader, or probable clock; setup YELLOW; v2 domain alarm / market=RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=YELLOW; flow=GREEN |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **NEOV** | 🔴🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 2 | **OKLO** | 🔴🔴🔴🟡🔴🔴 | Utilities - Independent Power Producers | SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -6.3% |
| 3 | **FCEL** | 🔴🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 4 | **EOSE** | 🔴🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=RED; red domains=parent,child,setup; child lags parent -4.7% |
| 5 | **ENVX** | 🔴🔴🔴🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -4.7% |
| 6 | **INDI** | 🔴🔴🔴🟡🔴🔴 | Semiconductors | SELL/AVOID — market=RED; red domains=parent,child,setup,flow |
| 7 | **TE** | 🔴🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=RED; red domains=parent,child,setup; child lags parent -4.7% |
| 8 | **SKYX** | 🔴🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=RED; red domains=parent,child,setup; child lags parent -4.7% |
| 9 | **NNDM** | 🔴🔴🔴🟡🔴🔴 | Computer Hardware | SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -3.1% |
| 10 | **MTZ** | 🔴🔴🔴🟡🔴🔴 | Engineering & Construction | SELL/AVOID — market=RED; red domains=parent,child,setup,flow |
| 11 | **LUNR** | 🔴🔴🔴🟡🔴🔴 | Aerospace & Defense | SELL/AVOID — market=RED; red domains=parent,child,setup,flow |
| 12 | **METC** | 🔴🔴🔴🟡🔴🟡 | Coking Coal | SELL/AVOID — market=RED; red domains=parent,child,setup; child lags parent -5.7% |
| 13 | **PLUG** | 🔴🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=RED; red domains=parent,child,setup; child lags parent -4.7% |
| 14 | **MNTS** | 🔴🔴🔴🟡🔴🔴 | Aerospace & Defense | SELL/AVOID — market=RED; red domains=parent,child,setup,flow |
| 15 | **HYLN** | 🔴🔴🔴🟡🔴🟡 | Electrical Equipment & Parts | SELL/AVOID — market=RED; red domains=parent,child,setup; child lags parent -4.7% |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **captain_research** (277 captains, 6 industries → s_heat).
- Board file: `01_daily/map_heat/2026-10-07_map_heat.json` · generated 2026-10-07T04:22:15.456945-04:00

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

## Horizon weights — book_policy.json v15 · renormalized (absent: sector)

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.13 | 0.00 | 0.09 | 0.28 | 0.28 | 0.22 | additive |
| 3d | 0.19 | 0.00 | 0.09 | 0.19 | 0.30 | 0.23 | additive |
| 1w | 0.21 | 0.00 | 0.10 | 0.12 | 0.33 | 0.24 | additive |
| 2w | 0.24 | 0.00 | 0.10 | 0.07 | 0.34 | 0.24 | additive |
| 1m | 0.28 | 0.00 | 0.10 | 0.00 | 0.38 | 0.25 | additive |

## 1d BUY — why these names

### 1. WH · $5.3B mid · Consumer Cyclical

**1d score +0.715**

**WH** is a liquid **mid-cap** Consumer Cyclical name (Lodging) at $5.3B, ADV ~1298k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.93 | +0.125 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.08 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | -0.09 | -0.008 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.42 | +0.117 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | +0.55 | +0.154 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | +0.35 | +0.077 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.715** | |

### 2. SIRI · $8.7B mid · Communication Services

**1d score +0.091**

**SIRI** is a liquid **mid-cap** Communication Services name (Entertainment) at $8.7B, ADV ~4070k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **miss**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.13 | +0.11 | +0.014 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.09 | -0.30 | -0.027 | same-day SPX call × this stock's beta |
| news / judge | 0.28 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.28 | -0.12 | -0.035 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.22 | -0.50 | -0.112 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.091** | |


## 1d AVOID — bottom of the same rank

- **NEOV** (micro, Industrials, $133M) score -0.418. SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -4.7%
- **OKLO** (mid, Utilities, $6.9B) score -0.120. SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -6.3%
- **FCEL** (small, Industrials, $1.3B) score -0.221. SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -4.7%
- **EOSE** (small, Industrials, $1.1B) score -0.315. SELL/AVOID — market=RED; red domains=parent,child,setup; child lags parent -4.7%
- **ENVX** (small, Industrials, $562M) score -0.202. SELL/AVOID — market=RED; red domains=parent,child,setup,flow; child lags parent -4.7%
- **INDI** (small, Technology, $668M) score -0.263. SELL/AVOID — market=RED; red domains=parent,child,setup,flow
- **TE** (small, Industrials, $1.1B) score -0.373. SELL/AVOID — market=RED; red domains=parent,child,setup; child lags parent -4.7%
- **SKYX** (micro, Industrials, $158M) score -0.368. SELL/AVOID — market=RED; red domains=parent,child,setup; child lags parent -4.7%

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | WH | +0.751 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | SIRI | +0.115 | mid | Communication Services | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | WH | +0.768 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | SIRI | +0.112 | mid | Communication Services | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | WH | +0.778 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | SIRI | +0.094 | mid | Communication Services | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1m BUY — why these names

### 1. WH · $5.3B mid · Consumer Cyclical

**1m score +0.797**

**WH** is a liquid **mid-cap** Consumer Cyclical name (Lodging) at $5.3B, ADV ~1298k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.93 | +0.257 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.08 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | -0.05 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.42 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | +0.55 | +0.208 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | +0.35 | +0.087 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.797** | |

### 2. SIRI · $8.7B mid · Communication Services

**1m score +0.091**

**SIRI** is a liquid **mid-cap** Communication Services name (Entertainment) at $8.7B, ADV ~4070k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **miss**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.28 | +0.11 | +0.029 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.10 | -0.16 | -0.016 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.38 | -0.12 | -0.047 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.25 | -0.50 | -0.126 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.091** | |


## 1m AVOID — bottom of the same rank

- **GH** (large, Healthcare, $23.8B) score -0.705. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **ABSI** (small, Healthcare, $1.6B) score -0.697. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NRXP** (micro, Healthcare, $126M) score -0.695. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PCT** (small, Industrials, $936M) score -0.664. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BAK** (micro, Basic Materials, $257M) score -0.656. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RC** (micro, Real Estate, $218M) score -0.650. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **IMNM** (mid, Healthcare, $2.5B) score -0.646. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ASPI** (small, Basic Materials, $402M) score -0.645. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AIRS** (micro, Healthcare, $138M) score -0.637. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PAL** (micro, Industrials, $99M) score -0.628. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TMC** (small, Basic Materials, $1.7B) score -0.621. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PRM** (mid, Basic Materials, $4.6B) score -0.621. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LFMD** (micro, Healthcare, $141M) score -0.618. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RVMD** (large, Healthcare, $43.5B) score -0.617. this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **CMTG** (micro, Real Estate, $179M) score -0.616. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LWLG** (small, Basic Materials, $771M) score -0.613. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **WVE** (small, Healthcare, $749M) score -0.611. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ALMS** (small, Healthcare, $953M) score -0.609. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **BHVN** (small, Healthcare, $2.1B) score -0.609. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **ZNTL** (micro, Healthcare, $243M) score -0.607. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **LDI** (micro, Financial, $216M) score -0.606. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PLAY** (micro, Communication Services, $223M) score -0.606. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LAB** (micro, Healthcare, $286M) score -0.603. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **TNYA** (micro, Healthcare, $130M) score -0.602. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LXEO** (micro, Healthcare, $258M) score -0.601. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-10-07_stock_book.md`
- Machine table: `data/stock_book/2026-10-07_stock_book.csv`
- Machine book: `data/stock_book/2026-10-07_stock_book.json`
- Join rank: `data/join/2026-10-07_ranked.csv`
- Weather: `01_daily/weather/2026-10-07_weather.md`
- AB enrich: `data/ab_checklist/2026-10-07_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-10-07_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-10-07_map_heat.md`
