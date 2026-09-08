# Stock book — 2026-09-08

_Generated 2026-09-08T11:44:20.168139-04:00_

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
- General predict (same-day): -0.33 down (present)
- Stand-down: **no** — 49 names qualified through catalyst_exception,probable (49 probable)
- Sector predicts this date: 9/11 (ok)
- News tickers in play: 49
- AB coverage: 1959 names · peer RS: 1834
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

### MARKET: 🔴 HARD_RED

- HARD_RED: general down score=-11.47; good=+0.5 vs bad=-13.2; risk=off; red pillars=4
- Allowed long lanes: **catalyst_exception, probable** · max slots 10 · size ×0.25
- Bull evidence: sentiment +0.50 points
- Bear evidence: overnight catalysts -9.00 points; rates / Fed -3.00 points; oil / dollar -1.00 points; futures -0.25 points

Decision domains: **MKT · parent · child · company · setup · flow**. Measured parent/child tape is kept separate from the LLM essay; direct company events must be price-confirmed on a hard-red day.

### Bull decisions (eligible or closest blocked cases)

| # | Ticker | Domains | Lane | Company / group | Decision |
|---:|--------|---------|------|-----------------|----------|
| 1 | **ANET** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Computer Hardware +4.5% d1 / +7.2% 1w / +5.8% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +7.2% 1w / +5.8% rel; lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 2 | **WDC** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Computer Hardware +4.5% d1 / +7.2% 1w / +5.8% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +7.2% 1w / +5.8% rel; lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 3 | **ACMR** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Semiconductor Equipment & Materials +4.9% d1 / +1.4% 1w / +0.0% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 4 | **TER** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Semiconductor Equipment & Materials +4.9% d1 / +1.4% 1w / +0.0% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 5 | **STM** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Semiconductors +2.6% d1 / +4.3% 1w / +2.9% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 6 | **ARM** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Semiconductors +2.6% d1 / +4.3% 1w / +2.9% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 7 | **KLIC** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Semiconductor Equipment & Materials +4.9% d1 / +1.4% 1w / +0.0% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 8 | **ORA** | 🔴🟡🟢🟡🟢🟢 | probable | no direct company event; Utilities - Renewable +1.9% d1 / +2.3% 1w / +1.4% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=YELLOW; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 9 | **KN** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Electronic Components +2.2% d1 / +3.4% 1w / +2.0% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 10 | **ZIM** | 🔴🟡🟢🟡🟢🟢 | probable | no direct company event; Marine Shipping +1.3% d1 / +4.0% 1w / +3.9% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +4.0% 1w / +3.9% rel — market=HARD_RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 11 | **SB** | 🔴🟡🟢🟡🟢🟢 | probable | no direct company event; Marine Shipping +1.3% d1 / +4.0% 1w / +3.9% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +4.0% 1w / +3.9% rel — market=HARD_RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 12 | **CEG** | 🔴🟡🟢🟡🟢🟢 | probable | no direct company event; Utilities - Independent Power Producers +4.5% d1 / +7.5% 1w / +6.7% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +7.5% 1w / +6.7% rel — market=HARD_RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 13 | **FN** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Electronic Components +2.2% d1 / +3.4% 1w / +2.0% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 14 | **BE** | 🔴🟡🟢🟡🟢🟢 | probable | no direct company event; Electrical Equipment & Parts +4.0% d1 / +8.3% 1w / +8.2% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +8.3% 1w / +8.2% rel — market=HARD_RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 15 | **MTSI** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Semiconductors +2.6% d1 / +4.3% 1w / +2.9% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **SLQT** | 🔴🔴🔴🟡🔴🔴 | Insurance Brokers | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -5.0% |
| 2 | **CPB** | 🔴🔴🔴🟡🔴🔴 | Packaged Foods | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.5% |
| 3 | **BYND** | 🔴🔴🔴🟡🟢🔴 | Packaged Foods | SELL/AVOID — market=HARD_RED; red domains=parent,child,flow; child lags parent -3.5% |
| 4 | **DJT** | 🔴🔴🔴🟡🔴🔴 | Internet Content & Information | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 5 | **TMC** | 🔴🔴🔴🟡🔴🔴 | Other Industrial Metals & Mining | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 6 | **IE** | 🔴🔴🔴🟡🔴🟡 | Copper | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -3.7% |
| 7 | **COLD** | 🔴🔴🔴🟡🔴🔴 | REIT - Industrial | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 8 | **SGML** | 🔴🔴🔴🟡🔴🔴 | Other Industrial Metals & Mining | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 9 | **FLO** | 🔴🔴🔴🟡🔴🟡 | Packaged Foods | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -3.5% |
| 10 | **OPEN** | 🔴🔴🔴🟡🔴🔴 | Real Estate Services | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 11 | **TNL** | 🔴🔴🔴🟡🔴🟡 | Travel Services | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -3.8% |
| 12 | **GETY** | 🔴🔴🔴🟡🔴🔴 | Internet Content & Information | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 13 | **DHC** | 🔴🔴🔴🟡🔴🔴 | REIT - Healthcare Facilities | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 14 | **FVRR** | 🔴🔴🔴🟡🔴🔴 | Internet Content & Information | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |
| 15 | **DKNG** | 🔴🔴🔴🟡🔴🔴 | Gambling | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **captain_research** (269 captains, 13 industries → s_heat).
- Board file: `01_daily/map_heat/2026-09-08_map_heat.json` · generated 2026-09-08T01:58:38.299618-04:00

### Sector RS vs same-day LLM essay

| Sector | Finviz 1d | Finviz 1w | LLM 1d | Tape vs essay |
|--------|----------:|----------:|-------:|---------------|
| Basic Materials | -0.7% | -1.1% | +0.00 |  |
| Communication Services | -0.9% | -0.5% | -0.28 |  |
| Consumer Cyclical | -1.1% | -1.9% | -0.60 |  |
| Consumer Defensive | -0.9% | -0.6% | +0.00 |  |
| Energy | -0.8% | +2.3% | +0.29 |  |
| Financial | -0.6% | +0.8% | -0.25 |  |
| Healthcare | -0.9% | +0.3% | +0.00 |  |
| Industrials | +0.4% | +0.1% | +0.00 |  |
| Real Estate | -0.6% | -1.2% | -0.47 |  |
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
| Consumer Cyclical | -0.60 |
| Real Estate | -0.47 |
| Energy | +0.29 |
| Communication Services | -0.28 |
| Financial | -0.25 |
| Basic Materials | +0.00 |
| Consumer Defensive | +0.00 |
| Healthcare | +0.00 |
| Industrials | +0.00 |

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

## Horizon weights — book_policy.json v11

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.12 | 0.10 | 0.08 | 0.25 | 0.25 | 0.20 | additive |
| 3d | 0.16 | 0.14 | 0.08 | 0.16 | 0.26 | 0.20 | additive |
| 1w | 0.18 | 0.16 | 0.08 | 0.10 | 0.28 | 0.20 | additive |
| 2w | 0.20 | 0.18 | 0.08 | 0.06 | 0.28 | 0.20 | additive |
| 1m | 0.22 | 0.20 | 0.08 | 0.00 | 0.30 | 0.20 | additive |

## 1d BUY — why these names

### 1. ZIM · $3.6B mid · Industrials

**1d score +0.512**

**ZIM** is a liquid **mid-cap** Industrials name (Marine Shipping) at $3.6B, ADV ~1410k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | -0.15 | -0.018 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.16 | -0.013 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.76 | +0.190 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.66 | +0.132 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.021 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.512** | |


## 1d AVOID — bottom of the same rank

- **SLQT** (micro, Financial, $92M) score -0.419. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -5.0%
- **CPB** (mid, Consumer Defensive, $6.3B) score +0.183. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.5%
- **BYND** (micro, Consumer Defensive, $201M) score -0.106. SELL/AVOID — market=HARD_RED; red domains=parent,child,flow; child lags parent -3.5%
- **DJT** (mid, Communication Services, $2.5B) score -0.124. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow
- **TMC** (small, Basic Materials, $1.9B) score -0.213. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow
- **IE** (small, Basic Materials, $1.6B) score -0.165. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -3.7%
- **COLD** (mid, Real Estate, $4.1B) score -0.480. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow
- **SGML** (small, Basic Materials, $1.3B) score -0.201. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow
- **FLO** (small, Consumer Defensive, $1.3B) score -0.119. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup; child lags parent -3.5%
- **OPEN** (mid, Real Estate, $3.0B) score -0.490. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | GGB | +0.534 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | SID | +0.513 | small | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | CRSR | +0.511 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 4 | SUZ | +0.495 | large | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | ZIM | +0.481 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | BLBD | +0.472 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | OSK | +0.452 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 8 | TWI | +0.432 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | CLF | +0.406 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | NOK | +0.303 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 11 | LEA | +0.182 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | BWA | +0.178 | large | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | BBY | +0.163 | large | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 14 | M | +0.025 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | GGB | +0.555 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | CRSR | +0.534 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 3 | ZIM | +0.529 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | SUZ | +0.523 | large | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | BLBD | +0.521 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | SID | +0.521 | small | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | OSK | +0.506 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 8 | TWI | +0.477 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | CLF | +0.412 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | NOK | +0.325 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 11 | LEA | +0.190 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | BWA | +0.189 | large | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | BBY | +0.161 | large | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 14 | M | +0.029 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | GGB | +0.560 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | CRSR | +0.539 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 3 | SUZ | +0.537 | large | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | ZIM | +0.527 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | BLBD | +0.520 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | SID | +0.516 | small | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | OSK | +0.506 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 8 | TWI | +0.476 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | CLF | +0.407 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | NOK | +0.344 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 11 | BWA | +0.265 | large | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | LEA | +0.264 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | BBY | +0.226 | large | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 14 | M | +0.104 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 1m BUY — why these names

### 1. GGB · $6.1B mid · Basic Materials

**1m score +0.598**

**GGB** is a liquid **mid-cap** Basic Materials name (Steel) at $6.1B, ADV ~14522k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.34 | +0.075 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.39 | +0.079 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.030 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.598** | |

### 2. CRSR · $1.4B small · Technology

**1m score +0.579**

**CRSR** is a liquid **small-cap** Technology name (Computer Hardware) at $1.4B, ADV ~1914k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.04 | -0.009 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.38 | +0.077 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.579** | |

### 3. SUZ · $11.0B large · Basic Materials

**1m score +0.568**

**SUZ** is a liquid **large-cap** Basic Materials name (Paper & Paper Products) at $11.0B, ADV ~3217k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.89 | +0.197 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.211 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.65 | +0.130 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.07 | +0.070 | liquid small/mid, room to run |
| **1m total** | | | **+0.568** | |

### 4. BLBD · $2.0B mid · Industrials

**1m score +0.550**

**BLBD** is a liquid **mid-cap** Industrials name (Farm & Heavy Construction Machinery) at $2.0B, ADV ~514k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.15 | -0.033 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.77 | +0.155 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.550** | |

### 5. ZIM · $3.6B mid · Industrials

**1m score +0.548**

**ZIM** is a liquid **mid-cap** Industrials name (Marine Shipping) at $3.6B, ADV ~1410k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.15 | -0.033 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.66 | +0.132 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.021 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.548** | |

### 6. SID · $1.6B small · Basic Materials

**1m score +0.540**

**SID** is a liquid **small-cap** Basic Materials name (Steel) at $1.6B, ADV ~4155k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.15 | -0.033 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.64 | +0.191 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.96 | +0.192 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.030 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.540** | |

### 7. OSK · $9.7B mid · Industrials

**1m score +0.533**

**OSK** is a liquid **mid-cap** Industrials name (Farm & Heavy Construction Machinery) at $9.7B, ADV ~709k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.03 | -0.007 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.34 | +0.068 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.533** | |

### 8. TWI · $523M small · Industrials

**1m score +0.502**

**TWI** is a liquid **small-cap** Industrials name (Farm & Heavy Construction Machinery) at $523M, ADV ~549k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.15 | -0.033 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.55 | +0.166 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.84 | +0.169 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.502** | |

### 9. CLF · $7.1B mid · Basic Materials

**1m score +0.429**

**CLF** is a liquid **mid-cap** Basic Materials name (Steel) at $7.1B, ADV ~20157k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.15 | -0.033 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.55 | +0.166 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.53 | +0.105 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.030 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.429** | |

### 10. NOK · $58.1B large · Technology

**1m score +0.374**

**NOK** is a liquid **large-cap** Technology name (Communication Equipment) at $58.1B, ADV ~89491k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.70 | +0.155 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.12 | +0.037 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.41 | +0.082 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.02 | -0.021 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1m total** | | | **+0.374** | |

### 11. BWA · $13.7B large · Consumer Cyclical

**1m score +0.287**

**BWA** is a liquid **large-cap** Consumer Cyclical name (Auto Parts) at $13.7B, ADV ~2409k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.07 | -0.016 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.95 | +0.286 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.28 | +0.057 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.287** | |

### 12. LEA · $6.6B mid · Consumer Cyclical

**1m score +0.284**

**LEA** is a liquid **mid-cap** Consumer Cyclical name (Auto Parts) at $6.6B, ADV ~645k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.15 | -0.033 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.47 | +0.093 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.284** | |

### 13. BBY · $18.7B large · Consumer Cyclical

**1m score +0.235**

**BBY** is a liquid **large-cap** Consumer Cyclical name (Specialty Retail) at $18.7B, ADV ~3818k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather are a **headwind** (sector stamp or hostile tape).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.58 | -0.127 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.87 | +0.174 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.235** | |

### 14. M · $6.0B mid · Consumer Cyclical

**1m score +0.126**

**M** is a liquid **mid-cap** Consumer Cyclical name (Department Stores) at $6.0B, ADV ~5451k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.15 | -0.033 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.55 | +0.166 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.16 | +0.032 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.126** | |


## 1m AVOID — bottom of the same rank

- **AIIO** (micro, Consumer Cyclical, $220M) score -0.619. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PACK** (small, Consumer Cyclical, $389M) score -0.608. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FUN** (small, Consumer Cyclical, $1.6B) score -0.605. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CPNG** (large, Consumer Cyclical, $27.5B) score -0.601. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **XHR** (small, Real Estate, $1.7B) score -0.580. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OPEN** (mid, Real Estate, $3.0B) score -0.563. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**
- **COLD** (mid, Real Estate, $4.1B) score -0.560. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LCID** (small, Consumer Cyclical, $1.8B) score -0.557. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **MBC** (small, Consumer Cyclical, $1.7B) score -0.545. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **XPOF** (micro, Consumer Cyclical, $242M) score -0.534. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **MVST** (micro, Consumer Cyclical, $268M) score -0.533. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **NIO** (mid, Consumer Cyclical, $8.8B) score -0.521. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PRKS** (small, Consumer Cyclical, $1.8B) score -0.511. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NXH** (small, Consumer Cyclical, $375M) score -0.499. the Finviz industry was **down**
- **WHR** (mid, Consumer Cyclical, $2.5B) score -0.485. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **LODE** (micro, Basic Materials, $236M) score -0.480. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **HGV** (mid, Consumer Cyclical, $3.3B) score -0.477. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LDI** (small, Financial, $319M) score -0.475. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **AZI** (micro, Consumer Cyclical, $87M) score -0.471. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**
- **CLSK** (mid, Financial, $3.2B) score -0.466. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **BHR** (micro, Real Estate, $130M) score -0.459. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **DFH** (small, Consumer Cyclical, $1.2B) score -0.457. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **XPEV** (mid, Consumer Cyclical, $8.5B) score -0.456. this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ABTC** (small, Financial, $585M) score -0.453. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **FCEL** (small, Industrials, $1.2B) score -0.451. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**

## Files for this run

- This rationale: `01_daily/2026-09-08_stock_book.md`
- Machine table: `data/stock_book/2026-09-08_stock_book.csv`
- Machine book: `data/stock_book/2026-09-08_stock_book.json`
- Join rank: `data/join/2026-09-08_ranked.csv`
- Weather: `01_daily/weather/2026-09-08_weather.md`
- AB enrich: `data/ab_checklist/2026-09-08_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-09-08_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-09-08_map_heat.md`
