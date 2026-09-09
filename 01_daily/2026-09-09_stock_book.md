# Stock book — 2026-09-09

_Generated 2026-09-09T13:18:18.041623-04:00_

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
- General predict (same-day): -0.55 down (present)
- Stand-down: **no** — 68 names qualified through catalyst_exception,probable (68 probable)
- Sector predicts this date: 11/11 (ok)
- News tickers in play: 48
- AB coverage: 1938 names · peer RS: 1834
- Universe after liquidity: 2065
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 22

## All-green BUY / SELL

- Mode: **weighted_fallback** · SELL **core_weights**
- Pile: **0** liquid all-green names (need ≥ 8) of 2065
- Core fired: join=yes, AB=yes, peer=yes
- pile 0 < 8 liquid all-green names. Fallback weighted walk; SELL stays on core

## Decision lattice — gate → route → rank

The weighted score is now a tie-breaker inside an eligible lane. It cannot average away a market, group, company, or setup veto.

### MARKET: 🔴 HARD_RED

- HARD_RED: general down score=-13.95; good=+0.5 vs bad=-16.0; risk=off; red pillars=6
- Allowed long lanes: **catalyst_exception, probable** · max slots 10 · size ×0.25
- Bull evidence: sentiment +0.50 points
- Bear evidence: overnight catalysts -9.00 points; rates / Fed -3.00 points; global sessions -2.00 points; oil / dollar -1.00 points; volatility -0.75 points; futures -0.25 points

Decision domains: **MKT · parent · child · company · setup · flow**. Measured parent/child tape is kept separate from the LLM essay; direct company events must be price-confirmed on a hard-red day.

### Bull decisions (eligible or closest blocked cases)

| # | Ticker | Domains | Lane | Company / group | Decision |
|---:|--------|---------|------|-----------------|----------|
| 1 | **ETN** | 🔴🟡🟢🟡🟢🔴 | probable | direct normal digest (stale/undated): Eaton shares rise after UBS upgrades the stock to Buy from Neutral and raises its price target to $515, citing strong sales growth and expected margin improvement.; Specialty Industrial Machinery +0.9% d1 / +1.9% 1w / +0.9% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=YELLOW; child=GREEN/rel=YELLOW; company=YELLOW(0.30); setup=GREEN; flow=RED; lookback=🔵,Cond green |
| 2 | **CIG** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Utilities - Regulated Electric +0.9% d1 / +1.7% 1w / -1.0% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 3 | **EMBJ** | 🔴🟡🟢🟡🟢🟢 | probable | no direct company event; Aerospace & Defense +0.8% d1 / +1.3% 1w / +0.2% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=YELLOW; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 4 | **ATRO** | 🔴🟡🟢🟡🟢🟢 | probable | no direct company event; Aerospace & Defense +0.8% d1 / +1.3% 1w / +0.2% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=YELLOW; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 5 | **MIR** | 🔴🟡🟢🟡🟢🟢 | probable | no direct company event; Specialty Industrial Machinery +0.9% d1 / +1.9% 1w / +0.9% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=YELLOW; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 6 | **NXE** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Uranium +1.4% d1 / +2.4% 1w / +0.8% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 7 | **BG** | 🔴🔴🟢🟡🟢🟢 | probable | no direct company event; Farm Products +1.5% d1 / +2.5% 1w / +3.5% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +2.5% 1w / +3.5% rel; lookback 🔵 blue — market=HARD_RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 8 | **AR** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Oil & Gas E&P +0.9% d1 / +1.2% 1w / -0.4% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 9 | **ITT** | 🔴🟡🟢🟡🟢🟢 | probable | no direct company event; Specialty Industrial Machinery +0.9% d1 / +1.9% 1w / +0.9% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=YELLOW; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 10 | **XIFR** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Utilities - Renewable +1.9% d1 / +4.2% 1w / +1.4% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 11 | **EC** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Oil & Gas Integrated +1.6% d1 / +2.0% 1w / +0.4% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 12 | **VFF** | 🔴🔴🟢🟡🟢🟢 | probable | no direct company event; Farm Products +1.5% d1 / +2.5% 1w / +3.5% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +2.5% 1w / +3.5% rel; lookback 🔵 blue — market=HARD_RED; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 13 | **AMPY** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Oil & Gas E&P +0.9% d1 / +1.2% 1w / -0.4% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 14 | **DINO** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Oil & Gas Refining & Marketing +2.4% d1 / +5.9% 1w / +4.3% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +5.9% 1w / +4.3% rel — market=HARD_RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=Cond green |
| 15 | **IMSR** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Utilities - Regulated Electric +0.9% d1 / +1.7% 1w / -1.0% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **METC** | 🔴🔴🔴🟡🔴🔴 | Coking Coal | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.4% |
| 2 | **SLQT** | 🔴🔴🔴🟡🔴🔴 | Insurance Brokers | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.9% |
| 3 | **CPB** | 🔴🔴🔴🟡🔴🔴 | Packaged Foods | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.3% |
| 4 | **COIN** | 🔴🔴🔴🟡🔴🔴 | Financial Data & Stock Exchanges | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.2% |
| 5 | **SPIR** | 🔴🟡🔴🟡🔴🔴 | Specialty Business Services | SELL/AVOID — market=HARD_RED; red domains=child,setup,flow; child lags parent -3.6% |
| 6 | **RZLV** | 🔴🟡🔴🟡🔴🔴 | Software - Infrastructure | SELL/AVOID — market=HARD_RED; red domains=child,setup,flow; child lags parent -4.4% |
| 7 | **CLPT** | 🔴🔴🔴🟡🔴🔴 | Medical Devices | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.2% |
| 8 | **BGS** | 🔴🔴🔴🟡🔴🔴 | Packaged Foods | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.3% |
| 9 | **APLD** | 🔴🟡🔴🟡🔴🔴 | Information Technology Services | SELL/AVOID — market=HARD_RED; red domains=child,setup,flow; child lags parent -4.7% |
| 10 | **STEP** | 🔴🔴🔴🟡🔴🔴 | Asset Management | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.2% |
| 11 | **BYND** | 🔴🔴🔴🟡🟢🔴 | Packaged Foods | SELL/AVOID — market=HARD_RED; red domains=parent,child,flow; child lags parent -3.3% |
| 12 | **INV** | 🔴🔴🔴🟡🔴🔴 | Asset Management | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.2% |
| 13 | **AORT** | 🔴🔴🔴🟡🔴🔴 | Medical Devices | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.2% |
| 14 | **OXSQ** | 🔴🔴🔴🟡🔴🔴 | Asset Management | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.2% |
| 15 | **CCL** | 🔴🔴🔴🟡🔴🔴 | Travel Services | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.2% |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **captain_research** (250 captains, 11 industries → s_heat).
- Board file: `01_daily/map_heat/2026-09-09_map_heat.json` · generated 2026-09-09T01:59:54.403331-04:00

### Sector RS vs same-day LLM essay

| Sector | Finviz 1d | Finviz 1w | LLM 1d | Tape vs essay |
|--------|----------:|----------:|-------:|---------------|
| Basic Materials | +0.0% | -0.3% | -0.47 |  |
| Communication Services | -0.3% | +0.7% | +0.00 |  |
| Consumer Cyclical | -0.7% | -1.8% | -0.60 |  |
| Consumer Defensive | -0.6% | -0.9% | -0.28 |  |
| Energy | +1.2% | +1.6% | +0.47 |  |
| Financial | -1.2% | +0.1% | -0.28 |  |
| Healthcare | -2.5% | -1.8% | -0.55 |  |
| Industrials | +0.3% | +1.1% | +0.00 |  |
| Real Estate | -0.2% | -0.5% | -0.47 |  |
| Technology | +0.2% | +1.3% | +0.00 |  |
| Utilities | +0.9% | +2.7% | +0.00 | essay flat, tape moving |

### Industry heat (1w vs parent)

**HOT**

- **Electrical Equipment & Parts** (Industrials) +4.5% 1d · +13.6% 1w · vs parent +12.5% · VRT, HUBB, ENS, ATKR
- **Textile Manufacturing** (Consumer Cyclical) -0.5% 1d · +10.5% 1w · vs parent +12.4% · AIN
- **Utilities - Independent Power Producers** (Utilities) +0.9% 1d · +8.9% 1w · vs parent +6.2% · CEG, VST, HNRG
- **Computer Hardware** (Technology) +1.8% 1d · +8.4% 1w · vs parent +7.0% · DELL, SNDK, QBTS, RGTI
- **Oil & Gas Refining & Marketing** (Energy) +2.4% 1d · +5.9% 1w · vs parent +4.3% · MPC, VLO, PBF, DK
- **Solar** (Technology) +3.6% 1d · +5.6% 1w · vs parent +4.3% · FSLR, RUN, SHLS
- **Electronic Components** (Technology) +2.1% 1d · +5.2% 1w · vs parent +3.9% · APH, GLW, PLXS, BELFB
- **Semiconductor Equipment & Materials** (Technology) +3.3% 1d · +5.0% 1w · vs parent +3.7% · LRCX, AMAT, ACMR, KLIC

**COLD**

- **Staffing & Employment Services** (Industrials) -5.6% 1d · -8.1% 1w · vs parent -9.1% · KFY, TNET
- **Consulting Services** (Industrials) -3.4% 1d · -7.5% 1w · vs parent -8.5% · VRSK, EFX, HURN, ICFI
- **Software - Application** (Technology) -3.5% 1d · -7.0% 1w · vs parent -8.3% · CRM, UBER, FROG, IDCC
- **Travel Services** (Consumer Cyclical) -3.5% 1d · -6.0% 1w · vs parent -4.2% · BKNG, ABNB, GBTG, LIND
- **Real Estate - Development** (Real Estate) -2.9% 1d · -5.2% 1w · vs parent -4.6% · CCS
- **Medical Devices** (Healthcare) -3.5% 1d · -5.0% 1w · vs parent -3.2% · ABT, MDT, GKOS, TXG
- **Broadcasting** (Communication Services) -3.1% 1d · -4.6% 1w · vs parent -5.3% · NMAX, FUBO
- **Residential Construction** (Consumer Cyclical) -3.4% 1d · -4.4% 1w · vs parent -2.6% · DHI, PHM, IBP, SKY

### Overrides (child 1w residual ≥ 3pp)

| Action | Industry | 1w | Parent 1w | Gap | Captains |
|--------|----------|---:|----------:|----:|----------|
| SPLIT | Electrical Equipment & Parts | +13.6% | +1.1% | +12.5% | VRT, HUBB, ENS, ATKR |
| OVERRIDE | Textile Manufacturing | +10.5% | -1.8% | +12.4% | AIN |
| OVERRIDE | Staffing & Employment Services | -8.1% | +1.1% | -9.1% | KFY, TNET |
| OVERRIDE | Consulting Services | -7.5% | +1.1% | -8.5% | VRSK, EFX, HURN, ICFI |
| OVERRIDE | Software - Application | -7.0% | +1.3% | -8.3% | CRM, UBER, FROG, IDCC |
| SPLIT | Computer Hardware | +8.4% | +1.3% | +7.0% | DELL, SNDK, QBTS, RGTI |
| OVERRIDE | Pharmaceutical Retailers | +5.0% | -1.8% | +6.7% | — |
| SPLIT | Utilities - Independent Power Producers | +8.9% | +2.7% | +6.2% | CEG, VST, HNRG |
| OVERRIDE | Broadcasting | -4.6% | +0.7% | -5.3% | NMAX, FUBO |
| OVERRIDE | Paper & Paper Products | +4.8% | -0.3% | +5.1% | SLVM |
| OVERRIDE | Information Technology Services | -3.4% | +1.3% | -4.7% | IBM, ACN, CIFR, PENG |
| OVERRIDE | Department Stores | +2.9% | -1.8% | +4.7% | KSS |
| SPLIT | Real Estate - Development | -5.2% | -0.5% | -4.6% | CCS |
| OVERRIDE | Aluminum | +4.1% | -0.3% | +4.4% | CENX, CSTM |
| OVERRIDE | Software - Infrastructure | -3.1% | +1.3% | -4.4% | MSFT, ORCL, ZETA, QLYS |

### Theme join (sub-sector vs GICS parent)

- **Energy Traditional** — Oil / Majors: +1.1% 1w vs parent +1.6% → AGREE; Oil E&P: +1.2% 1w vs parent +1.6% → AGREE; Oil Services: -0.5% 1w vs parent +1.6% → **DIVERGE**; Nuclear: +5.7% 1w vs parent +1.6% → AGREE
- **Commodities Energy** — Uranium: +2.4% 1w vs parent +0.6% → AGREE; Oil (commodity): +1.6% 1w vs parent +0.6% → AGREE
- **Energy Renewable** — Solar: +5.6% 1w vs parent +1.9% → AGREE; Renewable utilities: +4.2% 1w vs parent +1.9% → AGREE
- **Commodities Metals** — Gold: -0.3% 1w vs parent -0.3% → AGREE; Silver: +1.3% 1w vs parent -0.3% → **DIVERGE**; Copper: +0.9% 1w vs parent -0.3% → **DIVERGE**; Other precious: +2.2% 1w vs parent -0.3% → **DIVERGE**
- **Semiconductors** — Semis: +4.2% 1w vs parent +1.3% → AGREE; Semi equipment: +5.0% 1w vs parent +1.3% → AGREE
- **Artificial Intelligence** — AI compute / semis: +4.2% 1w vs parent +1.3% → AGREE; Software infra: -3.1% 1w vs parent +1.3% → **DIVERGE**
- **Defense & Aerospace** — Aero / defense: +1.3% 1w vs parent +1.1% → AGREE

### Theme ETF tape (biggest |1w| moves)

| Theme | 1d | 1w | Leaders |
|-------|---:|---:|---------|
| Fintech | +0.0% | +5.2% | BLOK, ARKF, BITQ |
| Agri-business | +0.0% | +4.7% | MOO, VEGI, FTAG |
| Consumer Discretionary | +0.0% | -2.1% | XLY, VCR, TSLL |
| Energy | +0.0% | +1.6% | XLE, AMLP, VDE |
| Consumer Staples | +0.0% | -1.2% | XLP, VDC, FSTA |
| Utilities | +0.0% | +1.2% | XLU, VPU, FUTY |
| Industrials | +0.0% | -1.1% | XLI, ITA, AIRR |
| Real Estate | +0.0% | -1.0% | VNQ, SCHH, XLRE |
| Technology | +0.0% | +0.8% | VGT, XLK, SMH |
| Cannabis Based Businesses | +0.0% | +0.8% | MSOS, MJ, CNBS |
| Natural Resources | +0.0% | +0.8% | GUNR, GNR, PHO |
| Materials | +0.0% | -0.6% | GDX, GDXJ, XLB |

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
| Healthcare | -0.55 |
| Basic Materials | -0.47 |
| Energy | +0.47 |
| Real Estate | -0.47 |
| Consumer Defensive | -0.28 |
| Financial | -0.28 |
| Communication Services | +0.00 |
| Industrials | +0.00 |
| Technology | +0.00 |
| Utilities | +0.00 |

### How much each predictor is trusted (graded hit rate)

| Topic | hit rate | n | weight |
|-------|----------|---|--------|
| general | 46% | 26 | ×0.85 |
| sector:Basic Materials | 53% | 15 | ×0.85 |
| sector:Communication Services | 27% | 15 | ×0.50 |
| sector:Consumer Cyclical | 60% | 15 | ×1.00 |
| sector:Consumer Defensive | 40% | 15 | ×0.50 |
| sector:Energy | 47% | 15 | ×0.85 |
| sector:Financial | 33% | 15 | ×0.50 |
| sector:Healthcare | 58% | 12 | ×1.00 |
| sector:Industrials | 20% | 15 | ×0.50 |
| sector:Real Estate | 47% | 15 | ×0.85 |
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

### 1. CIG · $4.1B mid · Utilities

**1d score +0.724**

**CIG** is a liquid **mid-cap** Utilities name (Utilities - Regulated Electric) at $4.1B, ADV ~5869k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.92 | +0.111 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.08 | -0.007 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.93 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.69 | +0.139 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.724** | |

### 2. ATRO · $3.3B mid · Industrials

**1d score +0.521**

**ATRO** is a liquid **mid-cap** Industrials name (Aerospace & Defense) at $3.3B, ADV ~720k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | -0.15 | -0.018 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | -0.20 | -0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.28 | -0.022 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.70 | +0.176 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.78 | +0.155 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.521** | |

### 3. EMBJ · $13.3B large · Industrials

**1d score +0.130**

**EMBJ** is a liquid **large-cap** Industrials name (Aerospace & Defense) at $13.3B, ADV ~1189k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.38 | +0.046 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | -0.20 | -0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.55 | -0.044 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.46 | +0.116 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.77 | +0.153 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.130** | |

### 4. MIR · $4.2B mid · Industrials

**1d score +0.596**

**MIR** is a liquid **mid-cap** Industrials name (Specialty Industrial Machinery) at $4.2B, ADV ~4028k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | -0.15 | -0.018 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | -0.20 | -0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.28 | -0.022 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.97 | +0.194 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.596** | |

### 5. NXE · $7.2B mid · Energy

**1d score +0.514**

**NXE** is a liquid **mid-cap** Energy name (Uranium) at $7.2B, ADV ~5294k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.95 | +0.114 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.27 | +0.027 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.55 | -0.044 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.36 | +0.090 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.39 | +0.078 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.514** | |

### 6. BG · $23.0B large · Consumer Defensive

**1d score +0.385**

**BG** is a liquid **large-cap** Consumer Defensive name (Farm Products) at $23.0B, ADV ~1516k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.92 | +0.110 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | -0.48 | -0.048 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.08 | -0.007 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.94 | +0.235 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.08 | +0.016 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1d total** | | | **+0.385** | |

### 7. AR · $12.4B large · Energy

**1d score +0.388**

**AR** is a liquid **large-cap** Energy name (Oil & Gas E&P) at $12.4B, ADV ~4394k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **miss**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.27 | +0.027 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.08 | -0.007 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.70 | +0.176 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.37 | +0.073 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1d total** | | | **+0.388** | |

### 8. ITT · $18.4B large · Industrials

**1d score +0.367**

**ITT** is a liquid **large-cap** Industrials name (Specialty Industrial Machinery) at $18.4B, ADV ~921k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.44 | +0.053 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | -0.20 | -0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.28 | -0.022 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.88 | +0.220 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.28 | +0.056 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1d total** | | | **+0.367** | |

### 9. XIFR · $2.3B mid · Utilities

**1d score +0.561**

**XIFR** is a liquid **mid-cap** Utilities name (Utilities - Renewable) at $2.3B, ADV ~914k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.25 | +0.030 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.28 | -0.022 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.91 | +0.226 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.39 | +0.078 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.561** | |


## 1d AVOID — bottom of the same rank

- **METC** (small, Basic Materials, $759M) score -0.549. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.4%
- **SLQT** (micro, Financial, $92M) score -0.611. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.9%
- **CPB** (mid, Consumer Defensive, $6.3B) score -0.336. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.3%
- **COIN** (large, Financial, $47.8B) score -0.479. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.2%
- **SPIR** (small, Industrials, $475M) score -0.367. SELL/AVOID — market=HARD_RED; red domains=child,setup,flow; child lags parent -3.6%
- **RZLV** (small, Technology, $901M) score -0.225. SELL/AVOID — market=HARD_RED; red domains=child,setup,flow; child lags parent -4.4%
- **CLPT** (small, Healthcare, $427M) score -0.329. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.2%
- **BGS** (micro, Consumer Defensive, $267M) score -0.382. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.3%
- **APLD** (mid, Technology, $7.7B) score -0.098. SELL/AVOID — market=HARD_RED; red domains=child,setup,flow; child lags parent -4.7%
- **STEP** (mid, Financial, $6.1B) score -0.389. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.2%

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | TDS | +0.805 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | CIG | +0.771 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | UGP | +0.744 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | BKV | +0.670 | mid | Energy | the Finviz industry was **advancing** |
| 5 | CVI | +0.659 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | XPRO | +0.650 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | MIR | +0.592 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 8 | XIFR | +0.582 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | EVER | +0.580 | small | Communication Services | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | PCG | +0.554 | large | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | MTCH | +0.547 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 12 | KYIV | +0.544 | mid | Communication Services | the Finviz industry was **advancing** |
| 13 | AEHR | +0.518 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 14 | ATRO | +0.516 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | GEO | +0.458 | mid | Industrials | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 16 | BG | +0.413 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 17 | CECO | +0.411 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up** |
| 18 | MWH | +0.394 | mid | Utilities | the Finviz industry was **advancing** |
| 19 | SANM | +0.375 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 20 | NXT | +0.358 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 21 | PRMB | +0.254 | mid | Consumer Defensive | the Finviz industry was **down** |
| 22 | MEOH | +0.206 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 23 | HLI | +0.149 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | TDS | +0.844 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | CIG | +0.808 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | UGP | +0.787 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | BKV | +0.715 | mid | Energy | the Finviz industry was **advancing** |
| 5 | CVI | +0.701 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | MGY | +0.692 | mid | Energy | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | EVER | +0.618 | small | Communication Services | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 8 | XIFR | +0.607 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | MIR | +0.604 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 10 | PCG | +0.582 | large | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | MTCH | +0.579 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 12 | AEHR | +0.578 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | KYIV | +0.577 | mid | Communication Services | the Finviz industry was **advancing** |
| 14 | ATRO | +0.525 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | GEO | +0.478 | mid | Industrials | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 16 | SANM | +0.455 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 17 | BG | +0.441 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 18 | NXT | +0.427 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 19 | CECO | +0.421 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up** |
| 20 | MWH | +0.408 | mid | Utilities | the Finviz industry was **advancing** |
| 21 | PRMB | +0.272 | mid | Consumer Defensive | the Finviz industry was **down** |
| 22 | MEOH | +0.212 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 23 | HLI | +0.158 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | CIG | +0.878 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | TDS | +0.872 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | UGP | +0.818 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | BKV | +0.759 | mid | Energy | the Finviz industry was **advancing** |
| 5 | CVI | +0.741 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | XPRO | +0.733 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | XIFR | +0.675 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | EVER | +0.645 | small | Communication Services | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 9 | PCG | +0.644 | large | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | MTCH | +0.633 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 11 | AEHR | +0.621 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | KYIV | +0.615 | mid | Communication Services | the Finviz industry was **advancing** |
| 13 | MIR | +0.615 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 14 | ATRO | +0.536 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | SANM | +0.516 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 16 | GEO | +0.487 | mid | Industrials | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 17 | NXT | +0.483 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 18 | IMSR | +0.478 | small | Utilities | the Finviz industry was **advancing** |
| 19 | BG | +0.455 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 20 | CECO | +0.452 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up** |
| 21 | PRMB | +0.282 | mid | Consumer Defensive | the Finviz industry was **down** |
| 22 | MEOH | +0.216 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 23 | HLI | +0.214 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 1m BUY — why these names

### 1. CIG · $4.1B mid · Utilities

**1m score +0.919**

**CIG** is a liquid **mid-cap** Utilities name (Utilities - Regulated Electric) at $4.1B, ADV ~5869k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.92 | +0.203 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.25 | +0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.93 | +0.278 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.69 | +0.139 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.919** | |

### 2. TDS · $4.1B mid · Communication Services

**1m score +0.911**

**TDS** is a liquid **mid-cap** Communication Services name (Telecom Services) at $4.1B, ADV ~1127k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.91 | +0.201 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.88 | +0.177 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.911** | |

### 3. UGP · $7.8B mid · Energy

**1m score +0.861**

**UGP** is a liquid **mid-cap** Energy name (Oil & Gas Refining & Marketing) at $7.8B, ADV ~3585k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.98 | +0.215 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.27 | +0.054 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.93 | +0.278 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.58 | +0.115 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.861** | |

### 4. BKV · $2.9B mid · Energy

**1m score +0.803**

**BKV** is a liquid **mid-cap** Energy name (Oil & Gas E&P) at $2.9B, ADV ~889k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.27 | +0.054 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.94 | +0.282 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.803** | |

### 5. CVI · $4.5B mid · Energy

**1m score +0.782**

**CVI** is a liquid **mid-cap** Energy name (Oil & Gas Refining & Marketing) at $4.5B, ADV ~1016k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.78 | +0.172 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.27 | +0.054 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.97 | +0.292 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.18 | +0.037 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.782** | |

### 6. XPRO · $2.0B mid · Energy

**1m score +0.772**

**XPRO** is a liquid **mid-cap** Energy name (Oil & Gas Equipment & Services) at $2.0B, ADV ~1092k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **miss**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.90 | +0.197 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.27 | +0.054 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.14 | +0.029 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.772** | |

### 7. XIFR · $2.3B mid · Utilities

**1m score +0.703**

**XIFR** is a liquid **mid-cap** Utilities name (Utilities - Renewable) at $2.3B, ADV ~914k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.25 | +0.054 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.25 | +0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.39 | +0.078 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.703** | |

### 8. EVER · $874M small · Communication Services

**1m score +0.682**

**EVER** is a liquid **small-cap** Communication Services name (Internet Content & Information) at $874M, ADV ~671k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.84 | +0.185 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | -0.23 | -0.047 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.682** | |

### 9. PCG · $31.3B large · Utilities

**1m score +0.676**

**PCG** is a liquid **large-cap** Utilities name (Utilities - Regulated Electric) at $31.3B, ADV ~29122k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.54 | +0.119 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.25 | +0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.66 | +0.133 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.07 | +0.070 | liquid small/mid, room to run |
| **1m total** | | | **+0.676** | |

### 10. MTCH · $9.5B mid · Communication Services

**1m score +0.661**

**MTCH** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $9.5B, ADV ~3299k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.64 | +0.141 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.55 | +0.166 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.32 | +0.063 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.661** | |

### 11. AEHR · $2.9B mid · Technology

**1m score +0.647**

**AEHR** is a liquid **mid-cap** Technology name (Semiconductor Equipment & Materials) at $2.9B, ADV ~2576k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.15 | -0.033 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.48 | +0.095 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.78 | +0.157 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.647** | |

### 12. KYIV · $3.3B mid · Communication Services

**1m score +0.647**

**KYIV** is a liquid **mid-cap** Communication Services name (Telecom Services) at $3.3B, ADV ~529k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **miss**. AB/peer context: the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.76 | +0.167 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.64 | +0.191 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.647** | |

### 13. MIR · $4.2B mid · Industrials

**1m score +0.625**

**MIR** is a liquid **mid-cap** Industrials name (Specialty Industrial Machinery) at $4.2B, ADV ~4028k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.15 | -0.033 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.97 | +0.194 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.625** | |

### 14. SANM · $10.9B large · Technology

**1m score +0.562**

**SANM** is a liquid **large-cap** Technology name (Electronic Components) at $10.9B, ADV ~1010k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.72 | +0.159 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.48 | +0.095 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.07 | +0.014 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1m total** | | | **+0.562** | |

### 15. ATRO · $3.3B mid · Industrials

**1m score +0.543**

**ATRO** is a liquid **mid-cap** Industrials name (Aerospace & Defense) at $3.3B, ADV ~720k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.15 | -0.033 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.211 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.78 | +0.155 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.543** | |

### 16. NXT · $12.8B large · Technology

**1m score +0.519**

**NXT** is a liquid **large-cap** Technology name (Solar) at $12.8B, ADV ~2991k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.51 | +0.112 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.48 | +0.095 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.55 | +0.166 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.17 | +0.035 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.11 | +0.110 | liquid small/mid, room to run |
| **1m total** | | | **+0.519** | |

### 17. GEO · $4.1B mid · Industrials

**1m score +0.507**

**GEO** is a liquid **mid-cap** Industrials name (Security & Protection Services) at $4.1B, ADV ~2095k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.35 | +0.078 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | -0.32 | -0.063 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.507** | |

### 18. IMSR · $563M small · Utilities

**1m score +0.495**

**IMSR** is a liquid **small-cap** Utilities name (Utilities - Regulated Electric) at $563M, ADV ~1849k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.15 | -0.033 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.25 | +0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.495** | |

### 19. BG · $23.0B large · Consumer Defensive

**1m score +0.483**

**BG** is a liquid **large-cap** Consumer Defensive name (Farm Products) at $23.0B, ADV ~1516k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.92 | +0.202 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.48 | -0.095 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.94 | +0.282 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.08 | +0.016 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.483** | |

### 20. CECO · $4.3B mid · Industrials

**1m score +0.459**

**CECO** is a liquid **mid-cap** Industrials name (Pollution & Treatment Controls) at $4.3B, ADV ~871k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.15 | -0.033 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.211 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.35 | +0.071 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.459** | |

### 21. PRMB · $8.0B mid · Consumer Defensive

**1m score +0.299**

**PRMB** is a liquid **mid-cap** Consumer Defensive name (Beverages - Non-Alcoholic) at $8.0B, ADV ~3710k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.70 | +0.154 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.48 | -0.095 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.64 | +0.191 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.299** | |

### 22. HLI · $9.6B mid · Financial

**1m score +0.224**

**HLI** is a liquid **mid-cap** Financial name (Capital Markets) at $9.6B, ADV ~853k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **miss**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.05 | +0.010 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.64 | +0.191 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.32 | +0.063 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.224** | |

### 23. MEOH · $4.5B mid · Basic Materials

**1m score +0.222**

**MEOH** is a liquid **mid-cap** Basic Materials name (Chemicals) at $4.5B, ADV ~871k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.39 | +0.086 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.47 | -0.094 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.36 | +0.108 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.36 | +0.072 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.00 | +0.000 | liquid small/mid, room to run |
| **1m total** | | | **+0.222** | |


## 1m AVOID — bottom of the same rank

- **XPOF** (micro, Consumer Cyclical, $242M) score -0.774. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FUN** (small, Consumer Cyclical, $1.6B) score -0.714. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LODE** (micro, Basic Materials, $236M) score -0.712. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CPNG** (large, Consumer Cyclical, $27.5B) score -0.709. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AIIO** (micro, Consumer Cyclical, $220M) score -0.706. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PACK** (small, Consumer Cyclical, $389M) score -0.694. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SLQT** (micro, Financial, $92M) score -0.690. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **WHR** (mid, Consumer Cyclical, $2.5B) score -0.688. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **METC** (small, Basic Materials, $759M) score -0.688. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TMC** (small, Basic Materials, $1.9B) score -0.685. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **XHR** (small, Real Estate, $1.7B) score -0.682. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SGML** (small, Basic Materials, $1.3B) score -0.668. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TROX** (small, Basic Materials, $768M) score -0.659. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**
- **DFH** (small, Consumer Cyclical, $1.2B) score -0.655. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OPEN** (mid, Real Estate, $3.0B) score -0.653. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**
- **HGV** (mid, Consumer Cyclical, $3.3B) score -0.647. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OI** (small, Consumer Cyclical, $1.1B) score -0.641. this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **IAUX** (small, Basic Materials, $1.5B) score -0.636. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LCID** (small, Consumer Cyclical, $1.8B) score -0.629. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **MLCO** (small, Consumer Cyclical, $2.0B) score -0.628. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NG** (mid, Basic Materials, $3.5B) score -0.627. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **DKNG** (large, Consumer Cyclical, $21.3B) score -0.625. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **BHR** (micro, Real Estate, $130M) score -0.621. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **COLD** (mid, Real Estate, $4.1B) score -0.619. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NAK** (small, Basic Materials, $832M) score -0.603. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-09-09_stock_book.md`
- Machine table: `data/stock_book/2026-09-09_stock_book.csv`
- Machine book: `data/stock_book/2026-09-09_stock_book.json`
- Join rank: `data/join/2026-09-09_ranked.csv`
- Weather: `01_daily/weather/2026-09-09_weather.md`
- AB enrich: `data/ab_checklist/2026-09-09_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-09-09_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-09-09_map_heat.md`
