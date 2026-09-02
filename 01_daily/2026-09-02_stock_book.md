# Stock book — 2026-09-02

_Generated 2026-09-02T13:21:08.628340-04:00_

This file is the **human read** of one run. CSV/JSON next to it are the machine files.

## How today's action is built

**1d uses a decision lattice.** Evidence is evaluated on its own merits before any numeric rank:

1. **Market gate** — raw general factor scoreboard + risk state sets exposure. An extreme confirmed red day closes ordinary longs.
2. **Parent / child route** — sector tape/essay and independent industry/theme absolute + relative strength decide where.
3. **Company route** — News Judge adjudicates; actions, Finviz digest and dossiers form one deduplicated direct-event decision.
4. **Setup / flow gate** — intrinsic AB + join structure, peer RS, price/gap and time-aware relative volume decide whether now.
5. **Rank inside the lane** — standard, group-leader or catalyst. mid_opp cannot grant permission.

The existing red/yellow/green source graph remains visible. Its digest, judge and catalyst cells are now populated before selection. 🔵 / 🚨 / ⚪, Cond, region and featured fades remain gates. A second six-domain row prevents duplicate headlines from voting three times. Longer horizons remain on the legacy weighted rank while the 1d lattice is validated.

## Today's regime

- Weather risk: **off**
- General predict (same-day): -0.44 down (present)
- Stand-down: **no** — 325 names qualified through catalyst_exception,probable (325 probable)
- Sector predicts this date: 9/11 (ok)
- News tickers in play: 117
- AB coverage: 2487 names · peer RS: 2393
- Universe after liquidity: 2683
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 104

## All-green BUY / SELL

- Mode: **green_pile** · SELL **core_weights_ex_green**
- Pile: **138** liquid all-green names (need ≥ 8) of 2683
- Core fired: join=yes, AB=yes, peer=yes
- pile 138 ≥ 8 liquid all-green names — BUY 15 from the pile by green_rank (no opp); SELL is core weights on the non-green remainder

## Decision lattice — gate → route → rank

The weighted score is now a tie-breaker inside an eligible lane. It cannot average away a market, group, company, or setup veto.

### MARKET: 🔴 HARD_RED

- HARD_RED: general down score=-3.83; good=+0.5 vs bad=-4.8; risk=off; red pillars=3
- Allowed long lanes: **catalyst_exception, probable** · max slots 10 · size ×0.25
- Bull evidence: sentiment +0.50 points
- Bear evidence: global sessions -2.00 points; rates / Fed -2.00 points; volatility -0.75 points

Decision domains: **MKT · parent · child · company · setup · flow**. Measured parent/child tape is kept separate from the LLM essay; direct company events must be price-confirmed on a hard-red day.

### Bull decisions (eligible or closest blocked cases)

| # | Ticker | Domains | Lane | Company / group | Decision |
|---:|--------|---------|------|-----------------|----------|
| 1 | **CVS** | 🔴🟡🟢🟡🟢🟢 | probable | direct high digest (stale/undated): CVS Health beats Q2 estimates, raises 2026 EPS and cash flow guidance, pre-guides 2027 amid PBM headwinds; Healthcare Plans +2.4% d1 / +1.1% 1w / +3.6% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +1.1% 1w / +3.6% rel; lookback 🔵 blue — market=HARD_RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.48); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 2 | **CVE** | 🔴🟢🟢🟡🟢🟢 | probable | direct high digest (stale/undated): Cenovus Energy Q2 2026 non-GAAP EPS $1.08 misses estimates, revenue $14.7B beats, company raises full-year production guidance; Oil & Gas Integrated +2.6% d1 / +4.0% 1w / +0.2% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.48); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 3 | **CNQ** | 🔴🟢🟢🟡🟢🟢 | probable | direct high digest (stale/undated): Canadian Natural Resources posts record Q2 2026 results with EPS $1.58, raises 2026 production guidance and returns about $4B to shareholders; Oil & Gas E&P +2.3% d1 / +3.6% 1w / -0.2% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.48); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 4 | **COR** | 🔴🟡🟢🟡🟢🟢 | probable | direct high digest (stale/undated): Cencora reaffirms fiscal 2026 adjusted EPS guidance despite Walgreens prescription volume shift; Medical Distribution +2.1% d1 / +0.4% 1w / +2.9% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=YELLOW; child=GREEN/rel=YELLOW; company=YELLOW(0.48); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 5 | **BG** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Farm Products +3.0% d1 / +4.3% 1w / +5.6% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +4.3% 1w / +5.6% rel; lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 6 | **PBF** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Oil & Gas Refining & Marketing +1.8% d1 / +7.0% 1w / +3.2% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +7.0% 1w / +3.2% rel; lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 7 | **ADM** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Farm Products +3.0% d1 / +4.3% 1w / +5.6% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +4.3% 1w / +5.6% rel; lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 8 | **OXY** | 🔴🟢🟢🟡🟢🟢 | probable | basket/action net=+6.00; context only, not a company catalyst; Oil & Gas E&P +2.3% d1 / +3.6% 1w / -0.2% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.40); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 9 | **EOG** | 🔴🟢🟢🟡🟢🟡 | probable | direct high digest (stale/undated): EOG posts record EPS and free cash flow, reiterates 2026 guidance and highlights strong UAE unconventional exploration results; Oil & Gas E&P +2.3% d1 / +3.6% 1w / -0.2% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.54); setup=GREEN; flow=YELLOW; lookback=🔵,Cond green |
| 10 | **CVX** | 🔴🟢🟢🟡🟢🟢 | probable | basket/action net=+4.20; context only, not a company catalyst; Oil & Gas Integrated +2.6% d1 / +4.0% 1w / +0.2% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.28); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 11 | **DEC** | 🔴🟢🟢🟡🟢🟢 | probable | basket/action net=+4.20; context only, not a company catalyst; Oil & Gas Integrated +2.6% d1 / +4.0% 1w / +0.2% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.28); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 12 | **AAPL** | 🔴🟡🟢🟡🟢🟢 | probable | no direct company event; Consumer Electronics +2.6% d1 / +4.8% 1w / +4.0% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +4.8% 1w / +4.0% rel; lookback 🔵 blue — market=HARD_RED; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 13 | **DK** | 🔴🟢🟢🟡🟢🟢 | probable | no direct company event; Oil & Gas Refining & Marketing +1.8% d1 / +7.0% 1w / +3.2% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: child/theme outperform +7.0% 1w / +3.2% rel; lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 14 | **COP** | 🔴🟢🟢🟡🟢🟡 | probable | basket/action net=+6.00; context only, not a company catalyst; Oil & Gas E&P +2.3% d1 / +3.6% 1w / -0.2% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.40); setup=GREEN; flow=YELLOW; lookback=🔵,Cond green |
| 15 | **RRC** | 🔴🟢🟢🟡🟢🟡 | probable | basket/action net=+6.00; context only, not a company catalyst; Oil & Gas E&P +2.3% d1 / +3.6% 1w / -0.2% vs parent | BUY PROBABLE — most-probable long on HARD_RED (size ×0.25); clocks: lookback 🔵 blue — market=HARD_RED; parent=GREEN; child=GREEN/rel=YELLOW; company=YELLOW(0.40); setup=GREEN; flow=YELLOW; lookback=🔵,Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **IP** | 🔴🔴🔴🟡🔴🔴 | Packaging & Containers | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.0% |
| 2 | **PACK** | 🔴🔴🔴🟡🔴🔴 | Packaging & Containers | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.0% |
| 3 | **HYMC** | 🔴🔴🔴🟡🔴🔴 | Gold | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -5.5% |
| 4 | **PENN** | 🔴🔴🔴🟡🔴🔴 | Resorts & Casinos | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.3% |
| 5 | **DFH** | 🔴🔴🔴🟡🔴🔴 | Residential Construction | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.0% |
| 6 | **KBH** | 🔴🔴🔴🟡🔴🔴 | Residential Construction | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.0% |
| 7 | **MTN** | 🔴🔴🔴🟡🔴🔴 | Resorts & Casinos | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.3% |
| 8 | **IE** | 🔴🔴🔴🟡🔴🔴 | Copper | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.6% |
| 9 | **MLCO** | 🔴🔴🔴🟡🔴🔴 | Resorts & Casinos | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.3% |
| 10 | **OI** | 🔴🔴🔴🟡🔴🔴 | Packaging & Containers | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.0% |
| 11 | **LOW** | 🔴🔴🔴🟡🔴🔴 | Home Improvement Retail | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.0% |
| 12 | **XHR** | 🔴🔴🔴🟡🔴🔴 | REIT - Hotel & Motel | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.0% |
| 13 | **LVS** | 🔴🔴🔴🟡🔴🔴 | Resorts & Casinos | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.3% |
| 14 | **HGV** | 🔴🔴🔴🟡🔴🔴 | Resorts & Casinos | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.3% |
| 15 | **HD** | 🔴🔴🔴🟡🔴🔴 | Home Improvement Retail | SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.0% |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **finviz_tape** (47 captains, 15 industries → s_heat).
- Board file: `01_daily/map_heat/2026-09-02_map_heat.json` · generated 2026-09-02T01:49:12.710218-04:00

### Sector RS vs same-day LLM essay

| Sector | Finviz 1d | Finviz 1w | LLM 1d | Tape vs essay |
|--------|----------:|----------:|-------:|---------------|
| Basic Materials | -1.9% | -5.2% | +0.00 | essay flat, tape moving |
| Communication Services | -0.6% | -2.2% | +0.00 | essay flat, tape moving |
| Consumer Cyclical | -1.8% | -2.8% | -0.52 |  |
| Consumer Defensive | +0.2% | -1.3% | +0.26 | essay UP, tape DOWN |
| Energy | +1.7% | +3.8% | +0.44 |  |
| Financial | -0.9% | -1.7% | +0.00 | essay flat, tape moving |
| Healthcare | +0.6% | -2.5% | +0.00 | essay flat, tape moving |
| Industrials | -1.4% | -2.6% | +0.00 | essay flat, tape moving |
| Real Estate | -0.1% | -3.0% | -0.52 |  |
| Technology | -1.2% | +0.9% | — |  |
| Utilities | +0.7% | -1.8% | — |  |

### Industry heat (1w vs parent)

**HOT**

- **Oil & Gas Refining & Marketing** (Energy) +1.8% 1d · +7.0% 1w · vs parent +3.2% · MPC, VLO, PBF, DK
- **Agricultural Inputs** (Basic Materials) +3.2% 1d · +5.3% 1w · vs parent +10.5% · CTVA, CF, FMC, IPI
- **Oil & Gas Equipment & Services** (Energy) -1.2% 1d · +5.0% 1w · vs parent +1.1% · SLB, BKR, KGS, WHD
- **Consumer Electronics** (Technology) +2.6% 1d · +4.8% 1w · vs parent +4.0% · AAPL, SONO, GPRO
- **Oil & Gas Drilling** (Energy) +2.0% 1d · +4.7% 1w · vs parent +0.9% · NE, RIG
- **Farm Products** (Consumer Defensive) +3.0% 1d · +4.3% 1w · vs parent +5.6% · ADM, BG, CALM, DMC
- **Oil & Gas Integrated** (Energy) +2.6% 1d · +4.0% 1w · vs parent +0.2% · XOM, CVX, DEC
- **Coking Coal** (Basic Materials) -1.5% 1d · +3.8% 1w · vs parent +8.9% · HCC, AMR

**COLD**

- **Textile Manufacturing** (Consumer Cyclical) -1.8% 1d · -11.9% 1w · vs parent -9.2% · AIN
- **Uranium** (Energy) -3.3% 1d · -10.8% 1w · vs parent -14.6% · UEC, UUUU
- **Gold** (Basic Materials) -3.8% 1d · -10.7% 1w · vs parent -5.5% · NEM, SSRM, NG
- **Other Precious Metals & Mining** (Basic Materials) -3.3% 1d · -8.8% 1w · vs parent -3.6% · PPTA, ELE
- **Copper** (Basic Materials) -3.8% 1d · -8.7% 1w · vs parent -3.6% · FCX, IE
- **Airlines** (Industrials) -2.4% 1d · -8.5% 1w · vs parent -5.9% · DAL, UAL, SKYW, ALGT
- **Mortgage Finance** (Financial) -2.8% 1d · -8.1% 1w · vs parent -6.4% · PFSI, WD
- **Travel Services** (Consumer Cyclical) -1.7% 1d · -7.9% 1w · vs parent -5.1% · BKNG, ABNB, GBTG, LIND

### Overrides (child 1w residual ≥ 3pp)

| Action | Industry | 1w | Parent 1w | Gap | Captains |
|--------|----------|---:|----------:|----:|----------|
| OVERRIDE | Uranium | -10.8% | +3.8% | -14.6% | UEC, UUUU |
| OVERRIDE | Agricultural Inputs | +5.3% | -5.2% | +10.5% | CTVA, CF, FMC, IPI |
| SPLIT | Textile Manufacturing | -11.9% | -2.8% | -9.2% | AIN |
| OVERRIDE | Coking Coal | +3.8% | -5.2% | +8.9% | HCC, AMR |
| OVERRIDE | Semiconductor Equipment & Materials | -6.7% | +0.9% | -7.5% | LRCX, AMAT, ACMR, KLIC |
| SPLIT | Mortgage Finance | -8.1% | -1.7% | -6.4% | PFSI, WD |
| SPLIT | Airlines | -8.5% | -2.6% | -5.9% | DAL, UAL, SKYW, ALGT |
| OVERRIDE | Health Information Services | +3.3% | -2.5% | +5.8% | VEEV, BTSG, HQY |
| OVERRIDE | Farm Products | +4.3% | -1.3% | +5.6% | ADM, BG, CALM, DMC |
| SPLIT | Gold | -10.7% | -5.2% | -5.5% | NEM, SSRM, NG |
| SPLIT | Travel Services | -7.9% | -2.8% | -5.1% | BKNG, ABNB, GBTG, LIND |
| OVERRIDE | Solar | -3.5% | +0.9% | -4.4% | FSLR, RUN, SHLS |
| SPLIT | Resorts & Casinos | -7.1% | -2.8% | -4.3% | LVS, MGM, RRR, VAC |
| OVERRIDE | Scientific & Technical Instruments | -3.4% | +0.9% | -4.3% | KEYS, COHR, ESE, NOVT |
| SPLIT | Consumer Electronics | +4.8% | +0.9% | +4.0% | AAPL, SONO, GPRO |

### Theme join (sub-sector vs GICS parent)

- **Energy Traditional** — Oil / Majors: +3.5% 1w vs parent +3.8% → AGREE; Oil E&P: +3.6% 1w vs parent +3.8% → AGREE; Oil Services: +5.0% 1w vs parent +3.8% → AGREE; Nuclear: -6.0% 1w vs parent +3.8% → **DIVERGE**
- **Commodities Energy** — Uranium: -10.8% 1w vs parent -0.7% → AGREE; Oil (commodity): +3.8% 1w vs parent -0.7% → **DIVERGE**
- **Energy Renewable** — Solar: -3.5% 1w vs parent +1.0% → **DIVERGE**; Renewable utilities: -2.6% 1w vs parent +1.0% → **DIVERGE**
- **Commodities Metals** — Gold: -10.7% 1w vs parent -5.2% → AGREE; Silver: -6.5% 1w vs parent -5.2% → AGREE; Copper: -8.7% 1w vs parent -5.2% → AGREE; Other precious: -8.8% 1w vs parent -5.2% → AGREE
- **Semiconductors** — Semis: +0.5% 1w vs parent +0.9% → AGREE; Semi equipment: -6.7% 1w vs parent +0.9% → **DIVERGE**
- **Artificial Intelligence** — AI compute / semis: +0.5% 1w vs parent +0.9% → AGREE; Software infra: +2.1% 1w vs parent +0.9% → AGREE
- **Defense & Aerospace** — Aero / defense: -1.3% 1w vs parent -2.6% → AGREE

### Theme ETF tape (biggest |1w| moves)

| Theme | 1d | 1w | Leaders |
|-------|---:|---:|---------|
| Materials | -3.4% | -7.5% | GDX, GDXJ, XLB |
| Fintech | -3.2% | -6.0% | BLOK, ARKF, BITQ |
| Space Exploration & Technology | -2.2% | -4.3% | NASA, ARKX, UFO |
| Industrials | -2.1% | -4.2% | XLI, ITA, AIRR |
| Consumer Discretionary | -2.0% | -3.5% | XLY, VCR, TSLL |
| Agri-business | +2.0% | +3.5% | MOO, VEGI, FTAG |
| Energy | +0.9% | +3.2% | XLE, AMLP, VDE |
| Healthcare | +0.2% | -2.9% | XLV, VHT, XBI |
| Real Estate | -0.4% | -2.9% | VNQ, SCHH, XLRE |
| Battery and Energy Storage | -1.8% | -2.8% | LIT, BATT, IBAT |
| Demographic & Lifestyle Trends | -1.5% | -2.7% | FFOX, FFND, BUZZ |
| Future Mobility Production & Tech | -1.8% | -2.5% | DRIV, ROKT, IDRV |

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
| Learnings / mutable policy | **missing / not in ranker** | next predict prompt, not a ticker score |

### Sector LLM bias (1d) — 0 / empty means that essay was not run today

| Sector | bias |
|--------|------|
| Consumer Cyclical | -0.52 |
| Real Estate | -0.52 |
| Energy | +0.44 |
| Consumer Defensive | +0.26 |
| Basic Materials | +0.00 |
| Communication Services | +0.00 |
| Financial | +0.00 |
| Healthcare | +0.00 |
| Industrials | +0.00 |

### How much each predictor is trusted (graded hit rate)

| Topic | hit rate | n | weight |
|-------|----------|---|--------|
| general | 50% | 22 | ×0.85 |
| sector:Basic Materials | 58% | 12 | ×1.00 |
| sector:Communication Services | 25% | 12 | ×0.50 |
| sector:Consumer Cyclical | 58% | 12 | ×1.00 |
| sector:Consumer Defensive | 42% | 12 | ×0.50 |
| sector:Energy | 50% | 12 | ×0.85 |
| sector:Financial | 33% | 12 | ×0.50 |
| sector:Healthcare | 78% | 9 | ×1.00 |
| sector:Industrials | 25% | 12 | ×0.50 |
| sector:Real Estate | 58% | 12 | ×1.00 |
| sector:Technology | 42% | 12 | ×0.50 |
| sector:Utilities | 42% | 12 | ×0.50 |

## Horizon weights — book_policy.json v8

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.12 | 0.10 | 0.08 | 0.25 | 0.25 | 0.20 | additive |
| 3d | 0.16 | 0.14 | 0.08 | 0.16 | 0.26 | 0.20 | additive |
| 1w | 0.18 | 0.16 | 0.08 | 0.10 | 0.28 | 0.20 | additive |
| 2w | 0.20 | 0.18 | 0.08 | 0.06 | 0.28 | 0.20 | additive |
| 1m | 0.22 | 0.20 | 0.08 | 0.00 | 0.30 | 0.20 | additive |

## 1d BUY — why these names

### 1. CVS · $124.8B large · Healthcare

**1d score +0.523**

**CVS** is a liquid **large-cap** Healthcare name (Healthcare Plans) at $124.8B, ADV ~7967k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.31 | +0.077 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.93 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.51 | +0.101 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1d total** | | | **+0.523** | |

### 2. CVE · $61.1B large · Energy

**1d score +0.360**

**CVE** is a liquid **large-cap** Energy name (Oil & Gas Integrated) at $61.1B, ADV ~7599k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **miss**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.73 | +0.087 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.24 | +0.024 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.31 | +0.077 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.88 | +0.220 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.38 | +0.077 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.360** | |

### 3. CNQ · $106.8B large · Energy

**1d score +0.377**

**CNQ** is a liquid **large-cap** Energy name (Oil & Gas E&P) at $106.8B, ADV ~7902k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.84 | +0.101 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.24 | +0.024 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.31 | +0.077 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.95 | +0.239 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.00 | +0.001 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.11 | -0.110 | liquid small/mid, room to run |
| **1d total** | | | **+0.377** | |

### 4. COR · $63.2B large · Healthcare

**1d score +0.373**

**COR** is a liquid **large-cap** Healthcare name (Medical Distribution) at $63.2B, ADV ~1457k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.24 | +0.048 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1d total** | | | **+0.373** | |

### 5. BG · $23.3B large · Consumer Defensive

**1d score +0.568**

**BG** is a liquid **large-cap** Consumer Defensive name (Farm Products) at $23.3B, ADV ~1555k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.78 | +0.093 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.26 | +0.026 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.96 | +0.241 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.81 | +0.163 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.05 | +0.050 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1d total** | | | **+0.568** | |

### 6. PBF · $8.9B mid · Energy

**1d score +0.733**

**PBF** is a liquid **mid-cap** Energy name (Oil & Gas Refining & Marketing) at $8.9B, ADV ~2886k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.80 | +0.096 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.24 | +0.024 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.94 | +0.235 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.66 | +0.132 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.733** | |

### 7. ADM · $40.7B large · Consumer Defensive

**1d score +0.383**

**ADM** is a liquid **large-cap** Consumer Defensive name (Farm Products) at $40.7B, ADV ~3654k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.67 | +0.080 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.26 | +0.026 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.75 | +0.150 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.05 | +0.050 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.383** | |

### 8. OXY · $60.9B large · Energy

**1d score +0.505**

**OXY** is a liquid **large-cap** Energy name (Oil & Gas E&P) at $60.9B, ADV ~8961k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.89 | +0.106 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.24 | +0.024 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.83 | +0.208 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.91 | +0.226 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | -0.03 | -0.005 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1d total** | | | **+0.505** | |

### 9. EOG · $77.8B large · Energy

**1d score +0.394**

**EOG** is a liquid **large-cap** Energy name (Oil & Gas E&P) at $77.8B, ADV ~3331k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.91 | +0.109 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.24 | +0.024 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.91 | +0.227 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.91 | +0.226 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | -0.33 | -0.067 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.394** | |

### 10. CVX · $417.0B mega · Energy

**1d score +0.278**

**CVX** is a liquid **mega-cap** Energy name (Oil & Gas Integrated) at $417.0B, ADV ~8425k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.80 | +0.096 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.24 | +0.024 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.69 | +0.171 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.93 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.25 | +0.050 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.34 | -0.340 | liquid small/mid, room to run |
| **1d total** | | | **+0.278** | |


## 1d AVOID — bottom of the same rank

- **IP** (large, Consumer Cyclical, $19.3B) score -0.479. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.0%
- **PACK** (small, Consumer Cyclical, $383M) score -0.559. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.0%
- **HYMC** (mid, Basic Materials, $2.0B) score -0.138. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -5.5%
- **PENN** (mid, Consumer Cyclical, $2.2B) score -0.550. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.3%
- **DFH** (small, Consumer Cyclical, $1.2B) score -0.486. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.0%
- **KBH** (mid, Consumer Cyclical, $3.2B) score -0.397. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.0%
- **MTN** (mid, Consumer Cyclical, $4.7B) score -0.265. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.3%
- **IE** (small, Basic Materials, $1.6B) score -0.265. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.6%
- **MLCO** (small, Consumer Cyclical, $1.9B) score -0.329. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -4.3%
- **OI** (small, Consumer Cyclical, $1.1B) score -0.455. SELL/AVOID — market=HARD_RED; red domains=parent,child,setup,flow; child lags parent -3.0%

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | PBR-A | +0.465 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | PBR | +0.462 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | PCRX | +0.773 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | HRMY | +0.758 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | DG | +0.609 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 6 | BG | +0.582 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 7 | TAL | +0.771 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 8 | VSTM | +0.742 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 9 | PBH | +0.752 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | PBF | +0.786 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | CRK | +0.758 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | CHEF | +0.699 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 13 | RNW | +0.620 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | NJR | +0.562 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 15 | ORA | +0.490 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | PBR-A | +0.505 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | PBR | +0.500 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | PCRX | +0.809 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | HRMY | +0.796 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | DG | +0.604 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 6 | BG | +0.578 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 7 | TAL | +0.769 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 8 | VSTM | +0.776 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 9 | PBH | +0.789 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | PBF | +0.824 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | CRK | +0.792 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | CHEF | +0.695 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 13 | RNW | +0.656 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | NJR | +0.590 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 15 | ORA | +0.510 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | PBR-A | +0.447 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | PBR | +0.442 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | PCRX | +0.829 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | HRMY | +0.816 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | DG | +0.618 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 6 | BG | +0.590 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 7 | TAL | +0.785 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 8 | VSTM | +0.796 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 9 | PBH | +0.809 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | PBF | +0.766 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | CRK | +0.734 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | CHEF | +0.711 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 13 | RNW | +0.674 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | NJR | +0.601 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 15 | ORA | +0.519 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1m BUY — why these names

### 1. PBR-A · $49.6B large · Energy

**1m score +0.565**

**PBR-A** is a liquid **large-cap** Energy name (Oil & Gas Integrated) at $49.6B, ADV ~7529k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.87 | +0.192 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.22 | +0.045 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.96 | +0.289 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.82 | +0.164 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1m total** | | | **+0.565** | |

### 2. PBR · $75.7B large · Energy

**1m score +0.559**

**PBR** is a liquid **large-cap** Energy name (Oil & Gas Integrated) at $75.7B, ADV ~16058k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.83 | +0.183 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.22 | +0.045 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.95 | +0.286 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.85 | +0.170 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1m total** | | | **+0.559** | |

### 3. PCRX · $1.1B small · Healthcare

**1m score +0.866**

**PCRX** is a liquid **small-cap** Healthcare name (Drug Manufacturers - Specialty & Generic) at $1.1B, ADV ~547k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.75 | +0.149 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.866** | |

### 4. HRMY · $2.4B mid · Healthcare

**1m score +0.855**

**HRMY** is a liquid **mid-cap** Healthcare name (Biotechnology) at $2.4B, ADV ~807k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.19 | -0.015 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.69 | +0.139 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.855** | |

### 5. DG · $28.9B large · Consumer Defensive

**1m score +0.646**

**DG** is a liquid **large-cap** Consumer Defensive name (Discount Stores) at $28.9B, ADV ~2833k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.82 | +0.181 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.25 | -0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.93 | +0.278 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.81 | +0.162 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1m total** | | | **+0.646** | |

### 6. BG · $23.3B large · Consumer Defensive

**1m score +0.618**

**BG** is a liquid **large-cap** Consumer Defensive name (Farm Products) at $23.3B, ADV ~1555k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.78 | +0.171 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.25 | -0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.96 | +0.289 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.81 | +0.163 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.05 | +0.050 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1m total** | | | **+0.618** | |

### 7. TAL · $4.1B mid · Consumer Defensive

**1m score +0.817**

**TAL** is a liquid **mid-cap** Consumer Defensive name (Education & Training Services) at $4.1B, ADV ~4260k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.95 | +0.208 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.25 | -0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.94 | +0.282 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.65 | +0.130 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.817** | |

### 8. VSTM · $689M small · Healthcare

**1m score +0.830**

**VSTM** is a liquid **small-cap** Healthcare name (Biotechnology) at $689M, ADV ~2443k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.211 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.78 | +0.157 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.830** | |

### 9. PBH · $2.5B mid · Healthcare

**1m score +0.847**

**PBH** is a liquid **mid-cap** Healthcare name (Drug Manufacturers - Specialty & Generic) at $2.5B, ADV ~592k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.98 | +0.216 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.61 | +0.121 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.847** | |

### 10. PBF · $8.9B mid · Energy

**1m score +0.882**

**PBF** is a liquid **mid-cap** Energy name (Oil & Gas Refining & Marketing) at $8.9B, ADV ~2886k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.80 | +0.176 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.22 | +0.045 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.94 | +0.282 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.66 | +0.132 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.882** | |

### 11. CRK · $4.7B mid · Energy

**1m score +0.847**

**CRK** is a liquid **mid-cap** Energy name (Oil & Gas E&P) at $4.7B, ADV ~2410k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.85 | +0.187 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.22 | +0.045 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.211 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.79 | +0.158 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.847** | |

### 12. CHEF · $4.7B mid · Consumer Defensive

**1m score +0.741**

**CHEF** is a liquid **mid-cap** Consumer Defensive name (Food Distribution) at $4.7B, ADV ~614k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.86 | +0.190 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.25 | -0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.38 | -0.031 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.59 | +0.118 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.741** | |

### 13. RNW · $2.5B mid · Utilities

**1m score +0.710**

**RNW** is a liquid **mid-cap** Utilities name (Utilities - Renewable) at $2.5B, ADV ~2077k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.88 | +0.193 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.19 | -0.015 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.09 | +0.019 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.710** | |

### 14. NJR · $5.5B mid · Utilities

**1m score +0.629**

**NJR** is a liquid **mid-cap** Utilities name (Utilities - Regulated Gas) at $5.5B, ADV ~679k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.53 | +0.116 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.06 | +0.013 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.629** | |

### 15. ORA · $6.6B mid · Utilities

**1m score +0.539**

**ORA** is a liquid **mid-cap** Utilities name (Utilities - Renewable) at $6.6B, ADV ~845k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.40 | +0.088 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.19 | -0.015 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.55 | +0.166 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.25 | +0.050 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.539** | |


## 1m AVOID — bottom of the same rank

- **FUN** (small, Consumer Cyclical, $1.5B) score -0.811. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LCID** (small, Consumer Cyclical, $1.8B) score -0.806. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **MBC** (small, Consumer Cyclical, $1.6B) score -0.805. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AIIO** (small, Consumer Cyclical, $347M) score -0.785. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OPEN** (mid, Real Estate, $3.0B) score -0.781. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PENN** (mid, Consumer Cyclical, $2.2B) score -0.753. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PACK** (small, Consumer Cyclical, $383M) score -0.740. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **DKNG** (large, Consumer Cyclical, $20.8B) score -0.736. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RC** (micro, Real Estate, $279M) score -0.720. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CMTG** (micro, Real Estate, $207M) score -0.712. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **WHR** (mid, Consumer Cyclical, $2.5B) score -0.707. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LVWR** (micro, Consumer Cyclical, $234M) score -0.698. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TDUP** (small, Consumer Cyclical, $331M) score -0.694. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **EVGO** (small, Consumer Cyclical, $399M) score -0.693. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **W** (large, Consumer Cyclical, $12.9B) score -0.684. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **DHC** (small, Real Estate, $1.8B) score -0.683. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AUR** (large, Consumer Cyclical, $10.9B) score -0.675. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **REAX** (small, Real Estate, $396M) score -0.674. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **DFH** (small, Consumer Cyclical, $1.2B) score -0.669. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **MNRO** (small, Consumer Cyclical, $389M) score -0.668. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OI** (small, Consumer Cyclical, $1.1B) score -0.662. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CAVA** (mid, Consumer Cyclical, $7.1B) score -0.660. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **IP** (large, Consumer Cyclical, $19.3B) score -0.658. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **COLD** (mid, Real Estate, $4.2B) score -0.650. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LKQ** (mid, Consumer Cyclical, $6.3B) score -0.644. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-09-02_stock_book.md`
- Machine table: `data/stock_book/2026-09-02_stock_book.csv`
- Machine book: `data/stock_book/2026-09-02_stock_book.json`
- Join rank: `data/join/2026-09-02_ranked.csv`
- Weather: `01_daily/weather/2026-09-02_weather.md`
- AB enrich: `data/ab_checklist/2026-09-02_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-09-02_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-09-02_map_heat.md`
