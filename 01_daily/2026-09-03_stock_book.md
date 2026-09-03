# Stock book — 2026-09-03

_Generated 2026-09-03T15:48:39.278594-04:00_

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
- General predict (same-day): +0.00 flat (present)
- Stand-down: **no** — 841 names qualified through standard,group_leader,catalyst (215 probable)
- Sector predicts this date: 11/11 (ok)
- News tickers in play: 72
- AB coverage: 1980 names · peer RS: 1859
- Universe after liquidity: 2095
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 33

## All-green BUY / SELL

- Mode: **green_pile** · SELL **core_weights_ex_green**
- Pile: **188** liquid all-green names (need ≥ 8) of 2095
- Core fired: join=yes, AB=yes, peer=yes
- pile 188 ≥ 8 liquid all-green names — BUY 15 from the pile by green_rank (no opp); SELL is core weights on the non-green remainder

## Decision lattice — gate → route → rank

The weighted score is now a tie-breaker inside an eligible lane. It cannot average away a market, group, company, or setup veto.

### MARKET: 🟡 YELLOW

- YELLOW: general flat score=-0.90; good=+1.0 vs bad=-2.0; risk=off; red pillars=1
- Allowed long lanes: **standard, group_leader, catalyst** · max slots 8 · size ×0.60
- Bull evidence: sentiment +0.50 points; oil / dollar +0.50 points
- Bear evidence: rates / Fed -2.00 points

Decision domains: **MKT · parent · child · company · setup · flow**. Measured parent/child tape is kept separate from the LLM essay; direct company events must be price-confirmed on a hard-red day.

### Bull decisions (eligible or closest blocked cases)

| # | Ticker | Domains | Lane | Company / group | Decision |
|---:|--------|---------|------|-----------------|----------|
| 1 | **AVGO** | 🟡🟡🟢🟢🟡🟢 | catalyst | direct high digest (same-day): Broadcom posts record AI-driven Q3, beats estimates with EPS $3.32, revenue $29.6B, raises FY26 AI outlook and doubles 2027–28 guide; Semiconductors +1.6% d1 / +2.5% 1w / +1.6% vs parent | BUY CATALYST — market=YELLOW; parent=YELLOW; child=GREEN/rel=YELLOW; company=GREEN(0.88); setup=YELLOW; flow=GREEN; lookback=🔵,⚪,Cond green |
| 2 | **HPE** | 🟡🟡🟡🟢🟢🟢 | catalyst | direct high digest (same-day): HPE posts record fiscal Q3 2026 with EPS $1.11, revenue $12.2B, raises FY26–27 outlook on AI, networking demand; Communication Equipment +0.1% d1 / -3.8% 1w / -4.8% vs parent | BUY CATALYST — market=YELLOW; parent=YELLOW; child=YELLOW/rel=RED; company=GREEN(0.72); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 3 | **CEG** | 🟡🟡🟢🟡🟢🟢 | group_leader | direct high digest (stale/undated): Constellation lifts 2024 adjusted EPS guidance to $11.50–$12.50 after Q2 beat and agrees to sell 606MW Brazos plant for $860M; Utilities - Independent Power Producers +3.2% d1 / +1.4% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.48); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 4 | **ADM** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Farm Products +1.1% d1 / +4.8% 1w / +5.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 5 | **VFF** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Farm Products +1.1% d1 / +4.8% 1w / +5.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 6 | **VEEV** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Health Information Services +0.8% d1 / +3.9% 1w / +4.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 7 | **VAL** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Oil & Gas Drilling +3.6% d1 / +10.0% 1w / +6.2% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 8 | **WAY** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Health Information Services +0.8% d1 / +3.9% 1w / +4.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 9 | **RIG** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Oil & Gas Drilling +3.6% d1 / +10.0% 1w / +6.2% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 10 | **MCK** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Medical Distribution +2.0% d1 / +2.8% 1w / +3.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 11 | **DOCS** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Health Information Services +0.8% d1 / +3.9% 1w / +4.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 12 | **SUZ** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Paper & Paper Products +3.2% d1 / +1.6% 1w / +3.8% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 13 | **BG** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Farm Products +1.1% d1 / +4.8% 1w / +5.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 14 | **GGB** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Steel +3.6% d1 / +3.6% 1w / +5.8% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 15 | **CENX** | 🟡🟢🟢🟡🟢🟢 | group_leader | no direct company event; Aluminum +0.1% d1 / +3.0% 1w / +5.2% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=GREEN; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **SPIR** | 🟡🔴🔴🟡🔴🔴 | Specialty Business Services | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 2 | **SATL** | 🟡🔴🔴🟡🔴🔴 | Aerospace & Defense | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 3 | **FWRD** | 🟡🔴🔴🟡🔴🔴 | Integrated Freight & Logistics | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 4 | **FIP** | 🟡🔴🔴🟡🔴🔴 | Conglomerates | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 5 | **PACK** | 🟡🔴🟡🟡🔴🔴 | Packaging & Containers | SELL/AVOID — market=YELLOW; red domains=parent,setup,flow; child lags parent -3.4% |
| 6 | **HUBG** | 🟡🔴🔴🟡🔴🔴 | Integrated Freight & Logistics | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 7 | **EVTL** | 🟡🔴🔴🟡🔴🔴 | Aerospace & Defense | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 8 | **RCAT** | 🟡🔴🔴🟡🔴🔴 | Aerospace & Defense | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 9 | **COLD** | 🟡🔴🔴🟡🔴🔴 | REIT - Industrial | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 10 | **FND** | 🟡🔴🔴🟡🔴🟡 | Home Improvement Retail | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.1% |
| 11 | **VFC** | 🟡🔴🔴🟡🔴🟢 | Apparel Manufacturing | SELL/AVOID — market=YELLOW; red domains=parent,child,setup; child lags parent -3.6% |
| 12 | **RZLV** | 🟡🟡🔴🟡🔴🔴 | Software - Infrastructure | SELL/AVOID — market=YELLOW; red domains=child,setup,flow |
| 13 | **CWK** | 🟡🔴🔴🟡🔴🔴 | Real Estate Services | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 14 | **GIII** | 🟡🔴🔴🟡🟡🔴 | Apparel Manufacturing | SELL/AVOID — market=YELLOW; red domains=parent,child,flow; child lags parent -3.6% |
| 15 | **LPTH** | 🟡🟡🔴🟡🔴🟢 | Electronic Components | SELL/AVOID — market=YELLOW; red domains=child,setup; child lags parent -4.6% |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **captain_research** (262 captains, 15 industries → s_heat).
- Board file: `01_daily/map_heat/2026-09-03_map_heat.json` · generated 2026-09-03T01:47:39.371074-04:00

### Sector RS vs same-day LLM essay

| Sector | Finviz 1d | Finviz 1w | LLM 1d | Tape vs essay |
|--------|----------:|----------:|-------:|---------------|
| Basic Materials | +1.8% | -2.2% | +0.55 | essay UP, tape DOWN |
| Communication Services | +1.2% | -0.2% | +0.00 |  |
| Consumer Cyclical | +0.3% | -1.9% | -0.52 |  |
| Consumer Defensive | +0.2% | -0.7% | +0.26 |  |
| Energy | +0.2% | +3.8% | +0.00 | essay flat, tape moving |
| Financial | +1.1% | -0.5% | +0.00 |  |
| Healthcare | +0.9% | -0.7% | +0.00 |  |
| Industrials | +0.0% | -3.5% | +0.00 | essay flat, tape moving |
| Real Estate | -0.5% | -2.8% | +0.00 | essay flat, tape moving |
| Technology | +0.4% | +1.0% | +0.00 |  |
| Utilities | +0.2% | -1.9% | +0.00 | essay flat, tape moving |

### Industry heat (1w vs parent)

**HOT**

- **Oil & Gas Drilling** (Energy) +3.6% 1d · +10.0% 1w · vs parent +6.2% · NE, RIG
- **Agricultural Inputs** (Basic Materials) +2.9% 1d · +8.4% 1w · vs parent +10.6% · CTVA, CF, FMC, IPI
- **Oil & Gas Equipment & Services** (Energy) +1.5% 1d · +6.1% 1w · vs parent +2.3% · SLB, BKR, KGS, WHD
- **Oil & Gas Refining & Marketing** (Energy) +1.1% 1d · +5.9% 1w · vs parent +2.1% · MPC, VLO, PBF, DK
- **Coking Coal** (Basic Materials) +1.7% 1d · +5.3% 1w · vs parent +7.5% · HCC, AMR
- **Farm Products** (Consumer Defensive) +1.1% 1d · +4.8% 1w · vs parent +5.5% · ADM, BG, CALM, DMC
- **Oil & Gas Integrated** (Energy) -0.2% 1d · +4.6% 1w · vs parent +0.8% · XOM, CVX, DEC
- **Marine Shipping** (Industrials) +1.7% 1d · +4.6% 1w · vs parent +8.2% · MATX, CMRE

**COLD**

- **Uranium** (Energy) +0.6% 1d · -9.9% 1w · vs parent -13.7% · UEC, UUUU
- **Textile Manufacturing** (Consumer Cyclical) +7.1% 1d · -6.9% 1w · vs parent -5.0% · AIN
- **Semiconductor Equipment & Materials** (Technology) +0.4% 1d · -6.2% 1w · vs parent -7.2% · LRCX, AMAT, ACMR, KLIC
- **Rental & Leasing Services** (Industrials) -0.3% 1d · -6.2% 1w · vs parent -2.6% · URI, GATX, HRI
- **Railroads** (Industrials) -0.0% 1d · -6.1% 1w · vs parent -2.6% · UNP, CSX, TRN, GBX
- **Lodging** (Consumer Cyclical) +0.2% 1d · -6.1% 1w · vs parent -4.2% · MAR, HLT
- **Real Estate - Development** (Real Estate) -1.2% 1d · -5.7% 1w · vs parent -2.8% · CCS
- **Copper** (Basic Materials) +1.4% 1d · -5.6% 1w · vs parent -3.4% · FCX, IE

### Overrides (child 1w residual ≥ 3pp)

| Action | Industry | 1w | Parent 1w | Gap | Captains |
|--------|----------|---:|----------:|----:|----------|
| OVERRIDE | Uranium | -9.9% | +3.8% | -13.7% | UEC, UUUU |
| OVERRIDE | Agricultural Inputs | +8.4% | -2.2% | +10.6% | CTVA, CF, FMC, IPI |
| OVERRIDE | Marine Shipping | +4.6% | -3.5% | +8.2% | MATX, CMRE |
| OVERRIDE | Coking Coal | +5.3% | -2.2% | +7.5% | HCC, AMR |
| OVERRIDE | Semiconductor Equipment & Materials | -6.2% | +1.0% | -7.2% | LRCX, AMAT, ACMR, KLIC |
| SPLIT | Oil & Gas Drilling | +10.0% | +3.8% | +6.2% | NE, RIG |
| OVERRIDE | Business Equipment & Supplies | +2.4% | -3.5% | +6.0% | XRX |
| OVERRIDE | Steel | +3.6% | -2.2% | +5.8% | NUE, STLD, WS, NWPX |
| OVERRIDE | Chemicals | +3.3% | -2.2% | +5.5% | DOW, HUN, REX |
| OVERRIDE | Farm Products | +4.8% | -0.7% | +5.5% | ADM, BG, CALM, DMC |
| OVERRIDE | Scientific & Technical Instruments | -4.3% | +1.0% | -5.3% | KEYS, GRMN, ESE, NOVT |
| OVERRIDE | Aluminum | +3.0% | -2.2% | +5.2% | CENX, CSTM |
| SPLIT | Textile Manufacturing | -6.9% | -1.9% | -5.0% | AIN |
| OVERRIDE | Communication Equipment | -3.8% | +1.0% | -4.8% | CSCO, MSI, VSAT, BDC |
| OVERRIDE | Electronic Components | -3.6% | +1.0% | -4.6% | APH, GLW, PLXS, BELFA |

### Theme join (sub-sector vs GICS parent)

- **Energy Traditional** — Oil / Majors: +2.8% 1w vs parent +3.8% → AGREE; Oil E&P: +4.0% 1w vs parent +3.8% → AGREE; Oil Services: +6.1% 1w vs parent +3.8% → AGREE; Nuclear: -4.2% 1w vs parent +3.8% → **DIVERGE**
- **Commodities Energy** — Uranium: -9.9% 1w vs parent +0.8% → **DIVERGE**; Oil (commodity): +4.3% 1w vs parent +0.8% → AGREE
- **Energy Renewable** — Solar: -3.5% 1w vs parent +0.9% → **DIVERGE**; Renewable utilities: -1.8% 1w vs parent +0.9% → **DIVERGE**
- **Commodities Metals** — Gold: -5.5% 1w vs parent -2.2% → AGREE; Silver: +0.5% 1w vs parent -2.2% → **DIVERGE**; Copper: -5.6% 1w vs parent -2.2% → AGREE; Other precious: -1.7% 1w vs parent -2.2% → AGREE
- **Semiconductors** — Semis: +2.5% 1w vs parent +1.0% → AGREE; Semi equipment: -6.2% 1w vs parent +1.0% → **DIVERGE**
- **Artificial Intelligence** — AI compute / semis: +2.5% 1w vs parent +1.0% → AGREE; Software infra: -0.5% 1w vs parent +1.0% → **DIVERGE**
- **Defense & Aerospace** — Aero / defense: -3.3% 1w vs parent -3.5% → AGREE

### Theme ETF tape (biggest |1w| moves)

| Theme | 1d | 1w | Leaders |
|-------|---:|---:|---------|
| Agri-business | +1.9% | +5.5% | MOO, VEGI, FTAG |
| Industrials | +0.2% | -4.3% | XLI, ITA, AIRR |
| Materials | +1.7% | -4.1% | GDX, GDXJ, XLB |
| Space Exploration & Technology | +0.9% | -3.1% | NASA, ARKX, UFO |
| Fintech | +1.5% | -3.0% | BLOK, ARKF, BITQ |
| Energy | +0.2% | +2.8% | XLE, AMLP, VDE |
| Real Estate | -0.2% | -2.7% | VNQ, SCHH, XLRE |
| Infrastructure | +0.0% | -2.6% | PAVE, GRID, IGF |
| Consumer Discretionary | +0.2% | -2.5% | XLY, VCR, TSLL |
| Robotics & Automation | -0.0% | -2.2% | BAI, AIQ, QTUM |
| Utilities | +0.2% | -2.2% | XLU, VPU, FUTY |
| Cannabis Based Businesses | +0.8% | +2.1% | MSOS, MJ, CNBS |

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
| Basic Materials | +0.55 |
| Consumer Cyclical | -0.52 |
| Consumer Defensive | +0.26 |
| Communication Services | +0.00 |
| Energy | +0.00 |
| Financial | +0.00 |
| Healthcare | +0.00 |
| Industrials | +0.00 |
| Real Estate | +0.00 |
| Technology | +0.00 |
| Utilities | +0.00 |

### How much each predictor is trusted (graded hit rate)

| Topic | hit rate | n | weight |
|-------|----------|---|--------|
| general | 48% | 23 | ×0.85 |
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

## Horizon weights — book_policy.json v10

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.12 | 0.10 | 0.08 | 0.25 | 0.25 | 0.20 | additive |
| 3d | 0.16 | 0.14 | 0.08 | 0.16 | 0.26 | 0.20 | additive |
| 1w | 0.18 | 0.16 | 0.08 | 0.10 | 0.28 | 0.20 | additive |
| 2w | 0.20 | 0.18 | 0.08 | 0.06 | 0.28 | 0.20 | additive |
| 1m | 0.22 | 0.20 | 0.08 | 0.00 | 0.30 | 0.20 | additive |

## 1d BUY — why these names

### 1. AVGO · $1747.2B mega · Technology

**1d score +0.423**

**AVGO** is a liquid **mega-cap** Technology name (Semiconductors) at $1747.2B, ADV ~25575k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Today's **news/judge** is a tailwind for this ticker.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | -0.02 | -0.003 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.55 | +0.138 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.88 | +0.220 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.69 | +0.138 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.14 | -0.140 | liquid small/mid, room to run |
| **1d total** | | | **+0.423** | |

### 2. HPE · $68.6B large · Technology

**1d score +0.099**

**HPE** is a liquid **large-cap** Technology name (Communication Equipment) at $68.6B, ADV ~22919k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Today's **news/judge** is a tailwind for this ticker.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.10 | +0.012 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.20 | +0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.31 | +0.077 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.46 | +0.116 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | -0.57 | -0.114 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.01 | -0.012 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1d total** | | | **+0.099** | |

### 3. CEG · $102.8B large · Utilities

**1d score +0.568**

**CEG** is a liquid **large-cap** Utilities name (Utilities - Independent Power Producers) at $102.8B, ADV ~3231k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.72 | +0.086 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.31 | +0.077 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.70 | +0.176 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.54 | +0.108 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.07 | +0.070 | liquid small/mid, room to run |
| **1d total** | | | **+0.568** | |

### 4. ADM · $41.1B large · Consumer Defensive

**1d score +0.387**

**ADM** is a liquid **large-cap** Consumer Defensive name (Farm Products) at $41.1B, ADV ~3670k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.79 | +0.095 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.26 | +0.026 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.78 | +0.157 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.387** | |

### 5. VFF · $353M small · Consumer Defensive

**1d score +0.644**

**VFF** is a liquid **small-cap** Consumer Defensive name (Farm Products) at $353M, ADV ~1062k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.19 | +0.023 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.26 | +0.026 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.91 | +0.226 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.49 | +0.097 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.021 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.644** | |

### 6. VEEV · $45.5B large · Healthcare

**1d score +0.454**

**VEEV** is a liquid **large-cap** Healthcare name (Health Information Services) at $45.5B, ADV ~2180k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.94 | +0.235 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.85 | +0.171 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.05 | +0.049 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.454** | |

### 7. VAL · $6.4B mid · Energy

**1d score +0.499**

**VAL** is a liquid **mid-cap** Energy name (Oil & Gas Drilling) at $6.4B, ADV ~980k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.07 | +0.009 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | -0.20 | -0.020 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.91 | +0.226 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.17 | +0.034 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.499** | |

### 8. WAY · $4.9B mid · Healthcare

**1d score +0.778**

**WAY** is a liquid **mid-cap** Healthcare name (Health Information Services) at $4.9B, ADV ~2731k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.88 | +0.220 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.84 | +0.168 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.021 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.778** | |


## 1d AVOID — bottom of the same rank

- **SPIR** (small, Industrials, $484M) score -0.267. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow
- **SATL** (small, Industrials, $727M) score -0.138. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow
- **FWRD** (small, Industrials, $540M) score -0.127. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow
- **FIP** (small, Industrials, $399M) score -0.208. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow
- **PACK** (small, Consumer Cyclical, $379M) score -0.475. SELL/AVOID — market=YELLOW; red domains=parent,setup,flow; child lags parent -3.4%
- **HUBG** (mid, Industrials, $2.2B) score -0.158. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow
- **EVTL** (micro, Industrials, $100M) score -0.202. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow
- **RCAT** (small, Industrials, $1.3B) score -0.114. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | ATRC | +0.818 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 2 | HRMY | +0.791 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 3 | CABA | +0.776 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | CRM | +0.285 | mega | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 5 | RVTY | +0.402 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 6 | MGNI | +0.768 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 7 | SONO | +0.807 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 8 | DUOL | +0.703 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 9 | NCNO | +0.754 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | FUBO | +0.764 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 11 | ADM | +0.401 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 12 | SBLK | +0.706 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | UPWK | +0.729 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | DG | +0.547 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | TDS | +0.692 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 16 | OSK | +0.603 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 17 | EXPO | +0.584 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 18 | SB | +0.612 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 19 | NWL | +0.434 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 20 | OLLI | +0.431 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | ATRC | +0.840 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 2 | HRMY | +0.812 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 3 | CABA | +0.774 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | CRM | +0.280 | mega | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 5 | RVTY | +0.422 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 6 | MGNI | +0.737 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 7 | SONO | +0.810 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 8 | DUOL | +0.718 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 9 | NCNO | +0.784 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | FUBO | +0.727 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 11 | ADM | +0.389 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 12 | SBLK | +0.736 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | UPWK | +0.713 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | DG | +0.532 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | TDS | +0.681 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 16 | OSK | +0.614 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 17 | EXPO | +0.608 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 18 | SB | +0.617 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 19 | NWL | +0.389 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 20 | OLLI | +0.399 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | ATRC | +0.858 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 2 | HRMY | +0.831 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 3 | CABA | +0.792 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | CRM | +0.325 | mega | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 5 | RVTY | +0.440 | large | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 6 | MGNI | +0.786 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 7 | SONO | +0.872 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 8 | DUOL | +0.778 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 9 | NCNO | +0.847 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | FUBO | +0.772 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 11 | ADM | +0.443 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 12 | SBLK | +0.797 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | UPWK | +0.762 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | DG | +0.580 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | TDS | +0.728 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 16 | OSK | +0.669 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 17 | EXPO | +0.668 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 18 | SB | +0.664 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 19 | NWL | +0.431 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 20 | OLLI | +0.439 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1m BUY — why these names

### 1. ATRC · $2.7B mid · Healthcare

**1m score +0.995**

**ATRC** is a liquid **mid-cap** Healthcare name (Medical Instruments & Supplies) at $2.7B, ADV ~879k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.48 | +0.096 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.21 | -0.017 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.95 | +0.286 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.81 | +0.163 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.995** | |

### 2. HRMY · $2.5B mid · Healthcare

**1m score +0.967**

**HRMY** is a liquid **mid-cap** Healthcare name (Biotechnology) at $2.5B, ADV ~810k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.48 | +0.096 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.21 | -0.017 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.93 | +0.278 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.71 | +0.143 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.967** | |

### 3. CABA · $605M small · Healthcare

**1m score +0.925**

**CABA** is a liquid **small-cap** Healthcare name (Biotechnology) at $605M, ADV ~4684k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.48 | +0.096 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.42 | -0.034 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.211 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.93 | +0.185 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.925** | |

### 4. CRM · $211.5B mega · Technology

**1m score +0.343**

**CRM** is a liquid **mega-cap** Technology name (Software - Application) at $211.5B, ADV ~15222k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.63 | +0.140 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.21 | -0.017 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.31 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.99 | +0.199 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.34 | -0.340 | liquid small/mid, room to run |
| **1m total** | | | **+0.343** | |

### 5. RVTY · $14.6B large · Healthcare

**1m score +0.575**

**RVTY** is a liquid **large-cap** Healthcare name (Diagnostics & Research) at $14.6B, ADV ~1483k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.98 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.48 | +0.096 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.21 | -0.017 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.63 | +0.126 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.11 | -0.110 | liquid small/mid, room to run |
| **1m total** | | | **+0.575** | |

### 6. MGNI · $3.6B mid · Communication Services

**1m score +0.827**

**MGNI** is a liquid **mid-cap** Communication Services name (Advertising Agencies) at $3.6B, ADV ~2851k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.91 | +0.199 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.42 | -0.034 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.95 | +0.286 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.63 | +0.125 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.827** | |

### 7. SONO · $1.9B small · Technology

**1m score +0.917**

**SONO** is a liquid **small-cap** Technology name (Consumer Electronics) at $1.9B, ADV ~1933k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.92 | +0.202 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.42 | -0.034 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.97 | +0.292 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.59 | +0.118 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.05 | +0.049 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.917** | |

### 8. DUOL · $7.4B mid · Technology

**1m score +0.816**

**DUOL** is a liquid **mid-cap** Technology name (Software - Application) at $7.4B, ADV ~1323k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.77 | +0.170 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.21 | -0.017 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.90 | +0.180 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.816** | |

### 9. NCNO · $2.4B mid · Technology

**1m score +0.886**

**NCNO** is a liquid **mid-cap** Technology name (Software - Application) at $2.4B, ADV ~3019k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.86 | +0.190 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.20 | +0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.74 | +0.147 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.886** | |

### 10. FUBO · $1.2B small · Communication Services

**1m score +0.807**

**FUBO** is a liquid **small-cap** Communication Services name (Broadcasting) at $1.2B, ADV ~1486k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.69 | +0.152 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.42 | -0.034 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.88 | +0.175 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.807** | |

### 11. ADM · $41.1B large · Consumer Defensive

**1m score +0.426**

**ADM** is a liquid **large-cap** Consumer Defensive name (Farm Products) at $41.1B, ADV ~3670k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.79 | +0.173 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.25 | -0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.78 | +0.157 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.03 | +0.028 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1m total** | | | **+0.426** | |

### 12. SBLK · $3.5B mid · Industrials

**1m score +0.788**

**SBLK** is a liquid **mid-cap** Industrials name (Marine Shipping) at $3.5B, ADV ~1337k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.96 | +0.212 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.24 | -0.048 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.96 | +0.289 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.39 | +0.078 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.01 | +0.012 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.788** | |

### 13. UPWK · $1.1B small · Communication Services

**1m score +0.799**

**UPWK** is a liquid **small-cap** Communication Services name (Internet Content & Information) at $1.1B, ADV ~3475k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.84 | +0.184 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.21 | -0.017 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.55 | +0.110 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.799** | |

### 14. DG · $28.9B large · Consumer Defensive

**1m score +0.560**

**DG** is a liquid **large-cap** Consumer Defensive name (Discount Stores) at $28.9B, ADV ~2782k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.50 | +0.110 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.25 | -0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.94 | +0.282 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.71 | +0.142 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1m total** | | | **+0.560** | |

### 15. TDS · $3.9B mid · Communication Services

**1m score +0.757**

**TDS** is a liquid **mid-cap** Communication Services name (Telecom Services) at $3.9B, ADV ~1103k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.70 | +0.154 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.211 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.73 | +0.146 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.757** | |

### 16. OSK · $9.4B mid · Industrials

**1m score +0.655**

**OSK** is a liquid **mid-cap** Industrials name (Farm & Heavy Construction Machinery) at $9.4B, ADV ~727k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.74 | +0.163 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.24 | -0.048 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.21 | -0.017 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.26 | +0.053 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.655** | |

### 17. EXPO · $3.3B mid · Industrials

**1m score +0.653**

**EXPO** is a liquid **mid-cap** Industrials name (Engineering & Construction) at $3.3B, ADV ~538k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.92 | +0.201 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.24 | -0.048 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.211 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.22 | +0.043 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.653** | |

### 18. SB · $926M small · Industrials

**1m score +0.644**

**SB** is a liquid **small-cap** Industrials name (Marine Shipping) at $926M, ADV ~851k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.32 | +0.071 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.24 | -0.048 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.21 | -0.017 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.97 | +0.292 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.42 | +0.085 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.01 | +0.012 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.644** | |

### 19. NWL · $2.5B mid · Consumer Defensive

**1m score +0.394**

**NWL** is a liquid **mid-cap** Consumer Defensive name (Household & Personal Products) at $2.5B, ADV ~11249k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.21 | +0.046 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.25 | -0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.21 | -0.017 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.36 | +0.108 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.29 | +0.057 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.394** | |

### 20. OLLI · $4.5B mid · Consumer Defensive

**1m score +0.401**

**OLLI** is a liquid **mid-cap** Consumer Defensive name (Discount Stores) at $4.5B, ADV ~2006k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.11 | +0.025 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.25 | -0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.06 | -0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.46 | +0.139 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.22 | +0.043 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.401** | |


## 1m AVOID — bottom of the same rank

- **XPOF** (micro, Consumer Cyclical, $250M) score -0.672. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LVWR** (micro, Consumer Cyclical, $246M) score -0.625. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide)
- **PENN** (mid, Consumer Cyclical, $2.3B) score -0.618. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PACK** (small, Consumer Cyclical, $379M) score -0.582. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **EVGO** (small, Consumer Cyclical, $393M) score -0.557. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AZI** (micro, Consumer Cyclical, $86M) score -0.535. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LCID** (small, Consumer Cyclical, $1.9B) score -0.531. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide)
- **AIIO** (micro, Consumer Cyclical, $287M) score -0.522. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide)
- **XPEV** (mid, Consumer Cyclical, $8.7B) score -0.521. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide)
- **FUN** (small, Consumer Cyclical, $1.6B) score -0.520. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **UNFI** (mid, Consumer Defensive, $2.7B) score -0.495. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LODE** (micro, Basic Materials, $247M) score -0.494. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BIPC** (mid, Utilities, $5.0B) score -0.492. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TRON** (small, Consumer Cyclical, $735M) score -0.488. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **MVST** (micro, Consumer Cyclical, $267M) score -0.487. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **SPIR** (small, Industrials, $484M) score -0.487. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OI** (small, Consumer Cyclical, $1.1B) score -0.480. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CMTG** (micro, Real Estate, $214M) score -0.479. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GETY** (micro, Communication Services, $101M) score -0.471. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **FLNC** (small, Utilities, $1.9B) score -0.468. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **MBC** (small, Consumer Cyclical, $1.7B) score -0.466. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SVV** (small, Consumer Cyclical, $1.6B) score -0.460. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **IE** (small, Basic Materials, $1.6B) score -0.459. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NXH** (small, Consumer Cyclical, $365M) score -0.448. the Finviz industry was **down**
- **LKQ** (mid, Consumer Cyclical, $6.3B) score -0.447. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-09-03_stock_book.md`
- Machine table: `data/stock_book/2026-09-03_stock_book.csv`
- Machine book: `data/stock_book/2026-09-03_stock_book.json`
- Join rank: `data/join/2026-09-03_ranked.csv`
- Weather: `01_daily/weather/2026-09-03_weather.md`
- AB enrich: `data/ab_checklist/2026-09-03_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-09-03_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-09-03_map_heat.md`
