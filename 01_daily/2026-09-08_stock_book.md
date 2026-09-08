# Stock book — 2026-09-08

_Generated 2026-09-08T10:53:03.061770-04:00_

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
- Stand-down: **no** — 361 names qualified through standard,group_leader,catalyst (18 probable)
- Sector predicts this date: 0/11 (missing → sector layer is 0; Finviz week tape still sits in join)
- News tickers in play: 49
- AB coverage: 1944 names · peer RS: 1834
- Universe after liquidity: 2065
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 21

## All-green BUY / SELL

- Mode: **weighted_fallback** · SELL **core_weights**
- Pile: **0** liquid all-green names (need ≥ 8) of 2065
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
| 1 | **CEG** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Utilities - Independent Power Producers +4.5% d1 / +7.5% 1w / +6.7% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 2 | **BE** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Electrical Equipment & Parts +4.0% d1 / +8.3% 1w / +8.2% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 3 | **ANET** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Computer Hardware +4.5% d1 / +7.2% 1w / +5.8% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 4 | **CMCO** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Farm & Heavy Construction Machinery +1.2% d1 / +4.3% 1w / +4.2% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 5 | **P** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Computer Hardware +4.5% d1 / +7.2% 1w / +5.8% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 6 | **SNDK** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Computer Hardware +4.5% d1 / +7.2% 1w / +5.8% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 7 | **CRSR** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Computer Hardware +4.5% d1 / +7.2% 1w / +5.8% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 8 | **WDC** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Computer Hardware +4.5% d1 / +7.2% 1w / +5.8% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 9 | **DELL** | 🟡🟢🟢🟡🟢🟡 | group_leader | no direct company event; Computer Hardware +4.5% d1 / +7.2% 1w / +5.8% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,⚪,Cond green |
| 10 | **OSK** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Farm & Heavy Construction Machinery +1.2% d1 / +4.3% 1w / +4.2% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 11 | **HPQ** | 🟡🟢🟢🟡🟢🟡 | group_leader | no direct company event; Computer Hardware +4.5% d1 / +7.2% 1w / +5.8% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,⚪,Cond green |
| 12 | **ZIM** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Marine Shipping +1.3% d1 / +4.0% 1w / +3.9% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 13 | **SMCI** | 🟡🟢🟢🟡🟢🟡 | group_leader | no direct company event; Computer Hardware +4.5% d1 / +7.2% 1w / +5.8% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,⚪,Cond green |
| 14 | **DE** | 🟡🟡🟢🟡🟢🟡 | group_leader | no direct company event; Farm & Heavy Construction Machinery +1.2% d1 / +4.3% 1w / +4.2% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,⚪,Cond green |
| 15 | **BLBD** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Farm & Heavy Construction Machinery +1.2% d1 / +4.3% 1w / +4.2% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **CPB** | 🟡🔴🔴🟡🔴🔴 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.5% |
| 2 | **IE** | 🟡🔴🔴🟡🔴🟡 | Copper | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.7% |
| 3 | **SGML** | 🟡🔴🔴🟡🔴🔴 | Other Industrial Metals & Mining | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 4 | **TMC** | 🟡🔴🔴🟡🔴🔴 | Other Industrial Metals & Mining | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 5 | **HYMC** | 🟡🔴🔴🟡🔴🔴 | Gold | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 6 | **COLD** | 🟡🔴🔴🟡🔴🔴 | REIT - Industrial | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 7 | **BYND** | 🟡🔴🔴🟡🟢🔴 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,flow; child lags parent -3.5% |
| 8 | **OPEN** | 🟡🔴🔴🟡🔴🔴 | Real Estate Services | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 9 | **FLO** | 🟡🔴🔴🟡🔴🟡 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.5% |
| 10 | **DJT** | 🟡🔴🔴🟡🔴🔴 | Internet Content & Information | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 11 | **FVRR** | 🟡🔴🔴🟡🔴🔴 | Internet Content & Information | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 12 | **DHC** | 🟡🔴🔴🟡🔴🔴 | REIT - Healthcare Facilities | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 13 | **BIDU** | 🟡🔴🔴🟡🔴🔴 | Internet Content & Information | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 14 | **GETY** | 🟡🔴🔴🟡🔴🔴 | Internet Content & Information | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 15 | **BGS** | 🟡🔴🔴🟡🔴🟡 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.5% |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **captain_research** (269 captains, 13 industries → s_heat).
- Board file: `01_daily/map_heat/2026-09-08_map_heat.json` · generated 2026-09-08T01:58:38.299618-04:00

### Sector RS vs same-day LLM essay

| Sector | Finviz 1d | Finviz 1w | LLM 1d | Tape vs essay |
|--------|----------:|----------:|-------:|---------------|
| Basic Materials | -0.7% | -1.1% | — |  |
| Communication Services | -0.9% | -0.5% | — |  |
| Consumer Cyclical | -1.1% | -1.9% | — |  |
| Consumer Defensive | -0.9% | -0.6% | — |  |
| Energy | -0.8% | +2.3% | — |  |
| Financial | -0.6% | +0.8% | — |  |
| Healthcare | -0.9% | +0.3% | — |  |
| Industrials | +0.4% | +0.1% | — |  |
| Real Estate | -0.6% | -1.2% | — |  |
| Technology | +0.8% | +1.4% | — |  |
| Utilities | +0.1% | +0.8% | — |  |

### Industry heat (1w vs parent)

**HOT**

- **Electrical Equipment & Parts** (Industrials) +4.0% 1d · +8.3% 1w · vs parent +8.2% · VRT, HUBB, ENS, ATKR
- **Utilities - Independent Power Producers** (Utilities) +4.5% 1d · +7.5% 1w · vs parent +6.7% · CEG, VST, HNRG
- **Computer Hardware** (Technology) +4.5% 1d · +7.2% 1w · vs parent +5.8% · DELL, SNDK, QBTS, RGTI
- **Department Stores** (Consumer Cyclical) +1.3% 1d · +6.0% 1w · vs parent +8.0% · KSS
- **Agricultural Inputs** (Basic Materials) -1.1% 1d · +6.0% 1w · vs parent +7.2% · CTVA, CF, FMC, IPI
- **Textile Manufacturing** (Consumer Cyclical) -0.1% 1d · +5.9% 1w · vs parent +7.8% · AIN
- **Oil & Gas Refining & Marketing** (Energy) +0.1% 1d · +4.9% 1w · vs parent +2.6% · MPC, VLO, PBF, CVI
- **Steel** (Basic Materials) +0.4% 1d · +4.5% 1w · vs parent +5.6% · NUE, STLD, WS, NWPX

**COLD**

- **Travel Services** (Consumer Cyclical) -0.8% 1d · -5.7% 1w · vs parent -3.8% · BKNG, ABNB, GBTG, LIND
- **Consulting Services** (Industrials) -3.5% 1d · -4.9% 1w · vs parent -5.0% · VRSK, EFX, HURN, ICFI
- **Copper** (Basic Materials) -0.1% 1d · -4.8% 1w · vs parent -3.7% · FCX, IE
- **Railroads** (Industrials) +0.4% 1d · -4.2% 1w · vs parent -4.3% · UNP, CSX, TRN, GBX
- **Insurance Brokers** (Financial) -1.4% 1d · -4.2% 1w · vs parent -5.0% · MRSH, AON, NP, ARX
- **Software - Application** (Technology) -2.7% 1d · -4.1% 1w · vs parent -5.4% · CRM, UBER, FROG, IDCC
- **Packaged Foods** (Consumer Defensive) -1.3% 1d · -4.1% 1w · vs parent -3.5% · KHC, GIS, MZTI, CENTA
- **Lodging** (Consumer Cyclical) -0.4% 1d · -3.7% 1w · vs parent -1.8% · MAR, HLT

### Overrides (child 1w residual ≥ 3pp)

| Action | Industry | 1w | Parent 1w | Gap | Captains |
|--------|----------|---:|----------:|----:|----------|
| SPLIT | Electrical Equipment & Parts | +8.3% | +0.1% | +8.2% | VRT, HUBB, ENS, ATKR |
| OVERRIDE | Department Stores | +6.0% | -1.9% | +8.0% | KSS |
| OVERRIDE | Textile Manufacturing | +5.9% | -1.9% | +7.8% | AIN |
| OVERRIDE | Agricultural Inputs | +6.0% | -1.1% | +7.2% | CTVA, CF, FMC, IPI |
| SPLIT | Utilities - Independent Power Producers | +7.5% | +0.8% | +6.7% | CEG, VST, HNRG |
| SPLIT | Computer Hardware | +7.2% | +1.4% | +5.8% | DELL, SNDK, QBTS, RGTI |
| OVERRIDE | Steel | +4.5% | -1.1% | +5.6% | NUE, STLD, WS, NWPX |
| OVERRIDE | Software - Application | -4.1% | +1.4% | -5.4% | CRM, UBER, FROG, IDCC |
| OVERRIDE | Insurance Brokers | -4.2% | +0.8% | -5.0% | MRSH, AON, NP, ARX |
| OVERRIDE | Consulting Services | -4.9% | +0.1% | -5.0% | VRSK, EFX, HURN, ICFI |
| OVERRIDE | Specialty Retail | +2.5% | -1.9% | +4.4% | CASY, WSM, RH, ASO |
| OVERRIDE | Railroads | -4.2% | +0.1% | -4.3% | UNP, CSX, TRN, GBX |
| SPLIT | Farm & Heavy Construction Machinery | +4.3% | +0.1% | +4.2% | CAT, DE, FSS, TEX |
| OVERRIDE | Auto Parts | +2.2% | -1.9% | +4.2% | ORLY, AZO, GTX, ATMU |
| OVERRIDE | Software - Infrastructure | -2.8% | +1.4% | -4.1% | MSFT, ORCL, ZETA, QLYS |

### Theme join (sub-sector vs GICS parent)

- **Energy Traditional** — Oil / Majors: +1.8% 1w vs parent +2.3% → AGREE; Oil E&P: +1.8% 1w vs parent +2.3% → AGREE; Oil Services: +2.3% 1w vs parent +2.3% → AGREE; Nuclear: +3.8% 1w vs parent +2.3% → AGREE
- **Commodities Energy** — Uranium: +0.1% 1w vs parent +0.6% → AGREE; Oil (commodity): +2.2% 1w vs parent +0.6% → AGREE
- **Energy Renewable** — Solar: -0.5% 1w vs parent +1.5% → **DIVERGE**; Renewable utilities: +2.3% 1w vs parent +1.5% → AGREE
- **Commodities Metals** — Gold: -0.6% 1w vs parent -1.1% → AGREE; Silver: +0.8% 1w vs parent -1.1% → **DIVERGE**; Copper: -4.8% 1w vs parent -1.1% → AGREE; Other precious: +0.5% 1w vs parent -1.1% → **DIVERGE**
- **Semiconductors** — Semis: +4.3% 1w vs parent +1.4% → AGREE; Semi equipment: +1.4% 1w vs parent +1.4% → AGREE
- **Artificial Intelligence** — AI compute / semis: +4.3% 1w vs parent +1.4% → AGREE; Software infra: -2.8% 1w vs parent +1.4% → **DIVERGE**
- **Defense & Aerospace** — Aero / defense: -0.2% 1w vs parent +0.1% → **DIVERGE**

### Theme ETF tape (biggest |1w| moves)

| Theme | 1d | 1w | Leaders |
|-------|---:|---:|---------|
| Fintech | -0.2% | +5.7% | BLOK, ARKF, BITQ |
| Agri-business | -0.3% | +4.7% | MOO, VEGI, FTAG |
| Consumer Discretionary | -0.5% | -2.1% | XLY, VCR, TSLL |
| Energy | -0.3% | +2.1% | XLE, AMLP, VDE |
| Industrials | +0.5% | -1.3% | XLI, ITA, AIRR |
| Consumer Staples | -0.8% | -1.2% | XLP, VDC, FSTA |
| Materials | -0.7% | -1.1% | GDX, GDXJ, XLB |
| Real Estate | -0.3% | -1.1% | VNQ, SCHH, XLRE |
| Technology | +1.1% | +0.9% | VGT, XLK, SMH |
| Utilities | +0.2% | +0.9% | XLU, VPU, FUTY |
| Cannabis Based Businesses | +2.0% | +0.8% | MSOS, MJ, CNBS |
| Financials | -0.8% | +0.5% | XLF, VFH, KBWB |

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
| general | 44% | 25 | ×0.50 |
| sector:Basic Materials | 57% | 14 | ×1.00 |
| sector:Communication Services | 21% | 14 | ×0.50 |
| sector:Consumer Cyclical | 57% | 14 | ×1.00 |
| sector:Consumer Defensive | 43% | 14 | ×0.50 |
| sector:Energy | 43% | 14 | ×0.50 |
| sector:Financial | 29% | 14 | ×0.50 |
| sector:Healthcare | 64% | 11 | ×1.00 |
| sector:Industrials | 21% | 14 | ×0.50 |
| sector:Real Estate | 50% | 14 | ×0.85 |
| sector:Technology | 36% | 14 | ×0.50 |
| sector:Utilities | 36% | 14 | ×0.50 |

## Horizon weights — book_policy.json v11 · renormalized (absent: sector, general)

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.15 | 0.00 | 0.00 | 0.30 | 0.30 | 0.24 | additive |
| 3d | 0.21 | 0.00 | 0.00 | 0.21 | 0.33 | 0.26 | additive |
| 1w | 0.24 | 0.00 | 0.00 | 0.13 | 0.37 | 0.26 | additive |
| 2w | 0.27 | 0.00 | 0.00 | 0.08 | 0.38 | 0.27 | additive |
| 1m | 0.31 | 0.00 | 0.00 | 0.00 | 0.42 | 0.28 | additive |

## 1d BUY — why these names

### 1. CMCO · $547M small · Industrials

**1d score +0.676**

**CMCO** is a liquid **small-cap** Industrials name (Farm & Heavy Construction Machinery) at $547M, ADV ~666k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.49 | +0.072 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.215 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.57 | +0.139 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.676** | |

### 2. CRSR · $1.4B small · Technology

**1d score +0.664**

**CRSR** is a liquid **small-cap** Technology name (Computer Hardware) at $1.4B, ADV ~1914k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.35 | +0.052 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.268 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.38 | +0.093 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.664** | |


## 1d AVOID — bottom of the same rank

- **CPB** (mid, Consumer Defensive, $6.3B) score +0.184. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -3.5%
- **IE** (small, Basic Materials, $1.6B) score -0.268. SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.7%
- **SGML** (small, Basic Materials, $1.3B) score -0.328. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow
- **TMC** (small, Basic Materials, $1.9B) score -0.291. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow
- **HYMC** (mid, Basic Materials, $2.1B) score -0.111. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow
- **COLD** (mid, Real Estate, $4.1B) score -0.304. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow
- **BYND** (micro, Consumer Defensive, $201M) score -0.080. SELL/AVOID — market=YELLOW; red domains=parent,child,flow; child lags parent -3.5%
- **OPEN** (mid, Real Estate, $3.0B) score -0.304. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | CABA | +0.933 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 2 | HRMY | +0.896 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | VOR | +0.876 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | LPG | +0.875 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | UGP | +0.852 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | XP | +0.850 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 7 | WT | +0.807 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 8 | CVI | +0.795 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | SNEX | +0.794 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 10 | AVAH | +0.789 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | BGC | +0.764 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 12 | NOV | +0.759 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | OSK | +0.758 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 14 | CMCO | +0.732 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 15 | CRSR | +0.714 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 16 | LEA | +0.711 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 17 | MIR | +0.672 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 18 | ZIM | +0.663 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 19 | GGB | +0.638 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 20 | SID | +0.600 | small | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 21 | VGNT | +0.577 | mid | Consumer Cyclical | the Finviz industry was **advancing** |
| 22 | QRVO | +0.572 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 23 | SBH | +0.567 | small | Consumer Cyclical | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 24 | SONO | +0.556 | small | Technology | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 25 | TEL | +0.526 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | CABA | +1.000 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 2 | HRMY | +0.962 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | LPG | +0.942 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | VOR | +0.939 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | XP | +0.918 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 6 | UGP | +0.914 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | WT | +0.873 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 8 | SNEX | +0.859 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 9 | CVI | +0.858 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | AVAH | +0.855 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | BGC | +0.829 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 12 | NOV | +0.817 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | OSK | +0.810 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 14 | CMCO | +0.776 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 15 | LEA | +0.760 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 16 | CRSR | +0.759 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 17 | MIR | +0.706 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 18 | ZIM | +0.690 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 19 | GGB | +0.676 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 20 | VGNT | +0.621 | mid | Consumer Cyclical | the Finviz industry was **advancing** |
| 21 | SID | +0.618 | small | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 22 | SBH | +0.610 | small | Consumer Cyclical | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 23 | QRVO | +0.600 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 24 | SONO | +0.585 | small | Technology | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 25 | TEL | +0.572 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | CABA | +1.047 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 2 | HRMY | +1.009 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | LPG | +0.988 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | VOR | +0.984 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | XP | +0.964 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 6 | UGP | +0.958 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | WT | +0.918 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 8 | SNEX | +0.903 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 9 | CVI | +0.900 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | AVAH | +0.898 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | BGC | +0.873 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 12 | NOV | +0.859 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | OSK | +0.841 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 14 | CMCO | +0.804 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 15 | LEA | +0.789 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 16 | CRSR | +0.782 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 17 | MIR | +0.728 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 18 | FSS | +0.710 | mid | Industrials | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 19 | GGB | +0.695 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 20 | VGNT | +0.642 | mid | Consumer Cyclical | the Finviz industry was **advancing** |
| 21 | SBH | +0.633 | small | Consumer Cyclical | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 22 | SID | +0.624 | small | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 23 | QRVO | +0.608 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 24 | TEL | +0.596 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 25 | SONO | +0.595 | small | Technology | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 1m BUY — why these names

### 1. CABA · $660M small · Healthcare

**1m score +1.121**

**CABA** is a liquid **small-cap** Healthcare name (Biotechnology) at $660M, ADV ~4816k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.95 | +0.290 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.85 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +1.00 | +0.277 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.121** | |

### 2. HRMY · $2.6B mid · Healthcare

**1m score +1.081**

**HRMY** is a liquid **mid-cap** Healthcare name (Biotechnology) at $2.6B, ADV ~811k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.81 | +0.337 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.87 | +0.243 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.081** | |

### 3. LPG · $2.4B mid · Energy

**1m score +1.062**

**LPG** is a liquid **mid-cap** Energy name (Oil & Gas Midstream) at $2.4B, ADV ~666k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.70 | +0.194 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.062** | |

### 4. VOR · $1.5B small · Healthcare

**1m score +1.053**

**VOR** is a liquid **small-cap** Healthcare name (Biotechnology) at $1.5B, ADV ~1142k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big miss**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.91 | +0.278 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.81 | +0.337 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.86 | +0.238 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.053** | |

### 5. XP · $9.8B mid · Financial

**1m score +1.040**

**XP** is a liquid **mid-cap** Financial name (Capital Markets) at $9.8B, ADV ~5327k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.08 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.96 | +0.402 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.49 | +0.137 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.040** | |

### 6. UGP · $7.8B mid · Energy

**1m score +1.026**

**UGP** is a liquid **mid-cap** Energy name (Oil & Gas Refining & Marketing) at $7.8B, ADV ~3585k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.98 | +0.299 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.76 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.58 | +0.160 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.026** | |

### 7. WT · $3.8B mid · Financial

**1m score +0.989**

**WT** is a liquid **mid-cap** Financial name (Asset Management) at $3.8B, ADV ~2807k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.08 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.44 | +0.121 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.989** | |

### 8. SNEX · $8.4B mid · Financial

**1m score +0.974**

**SNEX** is a liquid **mid-cap** Financial name (Capital Markets) at $8.4B, ADV ~1244k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.96 | +0.294 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.08 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.91 | +0.377 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.37 | +0.104 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.974** | |

### 9. AVAH · $3.0B mid · Healthcare

**1m score +0.970**

**AVAH** is a liquid **mid-cap** Healthcare name (Medical Care Facilities) at $3.0B, ADV ~2662k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.96 | +0.402 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.06 | +0.017 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.970** | |

### 10. CVI · $4.5B mid · Energy

**1m score +0.970**

**CVI** is a liquid **mid-cap** Energy name (Oil & Gas Refining & Marketing) at $4.5B, ADV ~1016k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.93 | +0.283 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.93 | +0.386 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.18 | +0.051 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.970** | |

### 11. BGC · $5.8B mid · Financial

**1m score +0.944**

**BGC** is a liquid **mid-cap** Financial name (Capital Markets) at $5.8B, ADV ~3076k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.97 | +0.297 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.08 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.93 | +0.386 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.22 | +0.061 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.944** | |

### 12. NOV · $7.6B mid · Energy

**1m score +0.923**

**NOV** is a liquid **mid-cap** Energy name (Oil & Gas Equipment & Services) at $7.6B, ADV ~3710k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.96 | +0.292 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.76 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.23 | +0.064 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.923** | |

### 13. OSK · $9.7B mid · Industrials

**1m score +0.899**

**OSK** is a liquid **mid-cap** Industrials name (Farm & Heavy Construction Machinery) at $9.7B, ADV ~709k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.58 | +0.177 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.91 | +0.377 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.34 | +0.095 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.899** | |

### 14. CMCO · $547M small · Industrials

**1m score +0.852**

**CMCO** is a liquid **small-cap** Industrials name (Farm & Heavy Construction Machinery) at $547M, ADV ~666k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.49 | +0.151 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.70 | +0.293 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.57 | +0.158 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.852** | |

### 15. LEA · $6.6B mid · Consumer Cyclical

**1m score +0.843**

**LEA** is a liquid **mid-cap** Consumer Cyclical name (Auto Parts) at $6.6B, ADV ~645k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.48 | +0.147 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.47 | +0.129 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.843** | |

### 16. CRSR · $1.4B small · Technology

**1m score +0.831**

**CRSR** is a liquid **small-cap** Technology name (Computer Hardware) at $1.4B, ADV ~1914k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.35 | +0.108 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.38 | +0.106 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.831** | |

### 17. MIR · $4.2B mid · Industrials

**1m score +0.765**

**MIR** is a liquid **mid-cap** Industrials name (Specialty Industrial Machinery) at $4.2B, ADV ~4028k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.34 | +0.104 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.46 | +0.193 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.97 | +0.269 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.765** | |

### 18. FSS · $7.3B mid · Industrials

**1m score +0.758**

**FSS** is a liquid **mid-cap** Industrials name (Farm & Heavy Construction Machinery) at $7.3B, ADV ~511k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.40 | +0.122 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.91 | +0.377 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | -0.14 | -0.040 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.05 | +0.049 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.758** | |

### 19. GGB · $6.1B mid · Basic Materials

**1m score +0.737**

**GGB** is a liquid **mid-cap** Basic Materials name (Steel) at $6.1B, ADV ~14522k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.26 | +0.080 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.76 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.39 | +0.109 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.030 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.737** | |

### 20. VGNT · $3.6B mid · Consumer Cyclical

**1m score +0.690**

**VGNT** is a liquid **mid-cap** Consumer Cyclical name (Auto Parts) at $3.6B, ADV ~1199k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.37 | +0.113 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.91 | +0.377 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.690** | |

### 21. SBH · $1.5B small · Consumer Cyclical

**1m score +0.680**

**SBH** is a liquid **small-cap** Consumer Cyclical name (Specialty Retail) at $1.5B, ADV ~1400k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.43 | +0.130 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.85 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | -0.01 | -0.003 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.680** | |

### 22. TEL · $59.9B large · Technology

**1m score +0.646**

**TEL** is a liquid **large-cap** Technology name (Electronic Components) at $59.9B, ADV ~2423k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.40 | +0.122 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.91 | +0.377 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.17 | +0.048 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.02 | -0.021 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.07 | +0.070 | liquid small/mid, room to run |
| **1m total** | | | **+0.646** | |

### 23. SID · $1.6B small · Basic Materials

**1m score +0.644**

**SID** is a liquid **small-cap** Basic Materials name (Steel) at $1.6B, ADV ~4155k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | -0.15 | -0.046 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.46 | +0.193 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.96 | +0.267 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.030 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.644** | |

### 24. QRVO · $9.0B mid · Technology

**1m score +0.638**

**QRVO** is a liquid **mid-cap** Technology name (Semiconductors) at $9.0B, ADV ~1194k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | -0.09 | -0.027 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.76 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.53 | +0.148 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.638** | |

### 25. SONO · $1.8B small · Technology

**1m score +0.627**

**SONO** is a liquid **small-cap** Technology name (Consumer Electronics) at $1.8B, ADV ~1924k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.08 | +0.025 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.76 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | -0.05 | -0.014 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.05 | +0.049 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.627** | |


## 1m AVOID — bottom of the same rank

- **LODE** (micro, Basic Materials, $236M) score -0.732. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SPIR** (small, Industrials, $475M) score -0.675. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **IBM** (mega, Technology, $219.2B) score -0.670. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **BZFD** (micro, Communication Services, $91M) score -0.644. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RGP** (micro, Industrials, $139M) score -0.642. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **APLD** (mid, Technology, $7.7B) score -0.642. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **RZLV** (small, Technology, $901M) score -0.636. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **DVLT** (micro, Technology, $200M) score -0.636. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **BIDU** (large, Communication Services, $25.6B) score -0.628. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FCEL** (small, Industrials, $1.2B) score -0.612. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **CPNG** (large, Consumer Cyclical, $27.5B) score -0.606. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **QH** (small, Technology, $418M) score -0.605. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TROX** (small, Basic Materials, $768M) score -0.603. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**
- **NUS** (micro, Consumer Defensive, $236M) score -0.601. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **XHR** (small, Real Estate, $1.7B) score -0.597. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **EVTL** (micro, Industrials, $103M) score -0.590. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PCT** (small, Industrials, $1.3B) score -0.588. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide)
- **AIIO** (micro, Consumer Cyclical, $220M) score -0.581. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NN** (mid, Technology, $2.6B) score -0.579. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **OPEN** (mid, Real Estate, $3.0B) score -0.577. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**
- **SGML** (small, Basic Materials, $1.3B) score -0.575. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GETY** (micro, Communication Services, $98M) score -0.571. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FUN** (small, Consumer Cyclical, $1.6B) score -0.569. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LAES** (small, Technology, $555M) score -0.567. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **COLD** (mid, Real Estate, $4.1B) score -0.567. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-09-08_stock_book.md`
- Machine table: `data/stock_book/2026-09-08_stock_book.csv`
- Machine book: `data/stock_book/2026-09-08_stock_book.json`
- Join rank: `data/join/2026-09-08_ranked.csv`
- Weather: `01_daily/weather/2026-09-08_weather.md`
- AB enrich: `data/ab_checklist/2026-09-08_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-09-08_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-09-08_map_heat.md`
