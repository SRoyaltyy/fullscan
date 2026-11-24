# Stock book — 2026-11-24

_Generated 2026-11-24T09:23:33.494499-05:00_

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
- Stand-down: **no** — 288 names qualified through standard,group_leader,catalyst (40 probable)
- Sector predicts this date: 0/11 (missing → sector layer is 0; Finviz week tape still sits in join)
- News tickers in play: 107
- AB coverage: 2091 names · peer RS: 1987
- Universe after liquidity: 2231
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 70

## All-green BUY / SELL

- Mode: **weighted_fallback** · SELL **core_weights**
- Pile: **0** liquid all-green names (need ≥ 8) of 2231
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
| 1 | **A** | 🟡🟡🟢🟢🟢🟢 | catalyst | direct high digest (same-day): Agilent beats fiscal Q4 guidance, expands margins and forecasts FY27 revenue of $8.00B-$8.15B with 5%-7% core growth; Diagnostics & Research +1.1% d1 / +1.6% 1w / +3.4% vs parent | BUY CATALYST — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=GREEN(0.72); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 2 | **CMBT** | 🟡🟡🟡🟢🟢🟢 | catalyst | direct high digest (same-day): CMB.Tech NV beats fiscal Q3 2026 estimates with non-GAAP EPS $1.29 and revenue $802.9M, up 77% YoY; Oil & Gas Midstream +0.1% d1 / -3.5% 1w / +2.0% vs parent | BUY CATALYST — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=GREEN(0.72); setup=GREEN; flow=GREEN; lookback=🔵,⚪ |
| 3 | **HPE** | 🟡🔴🔴🟢🟡🟡 | catalyst | direct high digest (same-day): HPE posts record FY26, guides FY27 revenue and EPS growth of 27–35% as rising AI mix moderates margins; Communication Equipment -3.0% d1 / -2.4% 1w / +0.7% vs parent | BUY CATALYST — market=YELLOW; parent=RED; child=RED/rel=YELLOW; company=GREEN(0.72); setup=YELLOW; flow=YELLOW; lookback=🔵 |
| 4 | **CRWD** | 🟡🔴🔴🟢🟢🟡 | catalyst | direct high digest (same-day): CrowdStrike agrees to acquire Zscaler for $43.9B in stock, posts record Q3 and raises FY27 net new ARR growth guidance to ~45%; Software - Infrastructure -0.7% d1 / -1.6% 1w / +1.5% vs parent | BUY CATALYST — market=YELLOW; parent=RED; child=RED/rel=YELLOW; company=GREEN(0.72); setup=GREEN; flow=YELLOW |
| 5 | **GMAB** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Biotechnology +0.8% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 6 | **SRRK** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Biotechnology +0.8% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 7 | **RZLT** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Biotechnology +0.8% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 8 | **VCYT** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Diagnostics & Research +1.1% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 9 | **RKT** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Mortgage Finance +0.7% d1 / +4.2% 1w / +6.7% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 10 | **VCEL** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Biotechnology +0.8% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 11 | **ADMA** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Biotechnology +0.8% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 12 | **ZYME** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Biotechnology +0.8% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 13 | **QTTB** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Biotechnology +0.8% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 14 | **TGTX** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Biotechnology +0.8% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 15 | **SPNT** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Insurance - Reinsurance +0.8% d1 / +0.5% 1w / +3.0% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **CSIQ** | 🟡🔴🔴🟡🔴🔴 | Solar | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.6% |
| 2 | **BTDR** | 🟡🔴🔴🟡🔴🔴 | Software - Application | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 3 | **DKNG** | 🟡🔴🔴🟡🔴🔴 | Gambling | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -7.5% |
| 4 | **IE** | 🟡🔴🔴🟡🔴🔴 | Copper | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -6.6% |
| 5 | **MSTR** | 🟡🔴🔴🟡🔴🔴 | Software - Application | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 6 | **HYMC** | 🟡🔴🔴🟡🔴🔴 | Gold | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.0% |
| 7 | **WRD** | 🟡🔴🔴🟡🔴🔴 | Software - Application | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 8 | **HTZ** | 🟡🔴🔴🟡🔴🔴 | Rental & Leasing Services | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 9 | **CDE** | 🟡🔴🔴🟡🔴🔴 | Gold | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.0% |
| 10 | **ACHR** | 🟡🔴🔴🟡🔴🔴 | Aerospace & Defense | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 11 | **CRML** | 🟡🔴🔴🟡🔴🔴 | Other Industrial Metals & Mining | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 12 | **FMC** | 🟡🔴🔴🟡🔴🔴 | Agricultural Inputs | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 13 | **QBTS** | 🟡🔴🔴🟡🔴🔴 | Computer Hardware | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 14 | **RUM** | 🟡🔴🔴🟡🔴🔴 | Internet Content & Information | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |
| 15 | **RKLB** | 🟡🔴🔴🟡🔴🔴 | Aerospace & Defense | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **finviz_tape** (37 captains, 15 industries → s_heat).
- Board file: `01_daily/map_heat/2026-11-24_map_heat.json` · generated 2026-11-24T06:02:55.728806-05:00

### Sector RS vs same-day LLM essay

| Sector | Finviz 1d | Finviz 1w | LLM 1d | Tape vs essay |
|--------|----------:|----------:|-------:|---------------|
| Basic Materials | -1.5% | -5.6% | — |  |
| Communication Services | -0.7% | -1.8% | — |  |
| Consumer Cyclical | -0.6% | -0.8% | — |  |
| Consumer Defensive | +0.7% | -0.7% | — |  |
| Energy | +0.3% | -5.5% | — |  |
| Financial | +0.2% | -2.5% | — |  |
| Healthcare | +0.8% | -1.8% | — |  |
| Industrials | -0.8% | -2.7% | — |  |
| Real Estate | +0.2% | -1.1% | — |  |
| Technology | -2.3% | -3.1% | — |  |
| Utilities | -0.1% | -0.8% | — |  |

### Industry heat (1w vs parent)

**HOT**

- **Mortgage Finance** (Financial) +0.7% 1d · +4.2% 1w · vs parent +6.7% · PFSI, WD
- **Metal Fabrication** (Industrials) +2.7% 1d · +3.8% 1w · vs parent +6.5% · CMC, GPGI
- **Apparel Retail** (Consumer Cyclical) +0.2% 1d · +3.7% 1w · vs parent +4.6% · TJX, ROST, VSXY, URBN
- **Furnishings, Fixtures & Appliances** (Consumer Cyclical) +0.7% 1d · +3.6% 1w · vs parent +4.5% · ALH, HNI
- **Real Estate - Development** (Real Estate) +1.8% 1d · +2.9% 1w · vs parent +4.1% · CCS
- **Home Improvement Retail** (Consumer Cyclical) -0.0% 1d · +2.2% 1w · vs parent +3.1% · HD, LOW
- **Residential Construction** (Consumer Cyclical) +0.7% 1d · +2.2% 1w · vs parent +3.0% · DHI, PHM, IBP, CVCO
- **Building Materials** (Basic Materials) +0.1% 1d · +1.9% 1w · vs parent +7.6% · CRH, MLM, USLM, KNF

**COLD**

- **Shell Companies** (Financial) -26.5% 1d · -26.4% 1w · vs parent -24.0% · —
- **Silver** (Basic Materials) -2.0% 1d · -13.9% 1w · vs parent -8.2% · —
- **Other Precious Metals & Mining** (Basic Materials) -2.4% 1d · -12.9% 1w · vs parent -7.3% · PPTA, MUX
- **Copper** (Basic Materials) -4.0% 1d · -12.2% 1w · vs parent -6.6% · FCX, IE
- **Healthcare Plans** (Healthcare) +0.5% 1d · -11.5% 1w · vs parent -9.7% · UNH, CVS, OSCR, PGNY
- **Gold** (Basic Materials) -1.6% 1d · -9.6% 1w · vs parent -4.0% · NEM, SSRM, NG
- **Oil & Gas Refining & Marketing** (Energy) -0.3% 1d · -9.2% 1w · vs parent -3.7% · MPC, PSX, PBF, DK
- **Solar** (Technology) -1.2% 1d · -8.7% 1w · vs parent -5.6% · FSLR, RUN, SHLS

### Overrides (child 1w residual ≥ 3pp)

| Action | Industry | 1w | Parent 1w | Gap | Captains |
|--------|----------|---:|----------:|----:|----------|
| SPLIT | Shell Companies | -26.4% | -2.5% | -24.0% | — |
| SPLIT | Healthcare Plans | -11.5% | -1.8% | -9.7% | UNH, CVS, OSCR, PGNY |
| SPLIT | Silver | -13.9% | -5.6% | -8.2% | — |
| SPLIT | Gambling | -8.3% | -0.8% | -7.5% | SGHC, RSI |
| SPLIT | Other Precious Metals & Mining | -12.9% | -5.6% | -7.3% | PPTA, MUX |
| OVERRIDE | Mortgage Finance | +4.2% | -2.5% | +6.7% | PFSI, WD |
| SPLIT | Copper | -12.2% | -5.6% | -6.6% | FCX, IE |
| OVERRIDE | Metal Fabrication | +3.8% | -2.7% | +6.5% | CMC, GPGI |
| SPLIT | Solar | -8.7% | -3.1% | -5.6% | FSLR, RUN, SHLS |
| SPLIT | Software - Application | -8.4% | -3.1% | -5.4% | CRM, UBER, FROG, TTAN |
| SPLIT | Health Information Services | -6.8% | -1.8% | -5.0% | VEEV, BTSG, HNGE |
| SPLIT | Advertising Agencies | -6.4% | -1.8% | -4.6% | APP, OMC, LFTO, MGNI |
| OVERRIDE | Apparel Retail | +3.7% | -0.8% | +4.6% | TJX, ROST, VSXY, URBN |
| OVERRIDE | Furnishings, Fixtures & Appliances | +3.6% | -0.8% | +4.5% | ALH, HNI |
| SPLIT | Utilities - Independent Power Producers | -4.8% | -0.8% | -4.1% | CEG, VST, HNRG |

### Theme join (sub-sector vs GICS parent)

- **Energy Traditional** — Oil / Majors: -4.6% 1w vs parent -5.5% → AGREE; Oil E&P: -5.6% 1w vs parent -5.5% → AGREE; Oil Services: -5.2% 1w vs parent -5.5% → AGREE; Nuclear: -5.5% 1w vs parent -5.5% → AGREE
- **Commodities Energy** — Uranium: -6.3% 1w vs parent -5.5% → AGREE; Oil (commodity): -5.7% 1w vs parent -5.5% → AGREE
- **Energy Renewable** — Solar: -8.7% 1w vs parent -3.1% → AGREE; Renewable utilities: -4.7% 1w vs parent -3.1% → AGREE
- **Commodities Metals** — Gold: -9.6% 1w vs parent -5.6% → AGREE; Silver: -13.9% 1w vs parent -5.6% → AGREE; Copper: -12.2% 1w vs parent -5.6% → AGREE; Other precious: -12.9% 1w vs parent -5.6% → AGREE
- **Semiconductors** — Semis: -3.8% 1w vs parent -3.1% → AGREE; Semi equipment: -3.3% 1w vs parent -3.1% → AGREE
- **Artificial Intelligence** — AI compute / semis: -3.8% 1w vs parent -3.1% → AGREE; Software infra: -1.6% 1w vs parent -3.1% → AGREE
- **Defense & Aerospace** — Aero / defense: -4.3% 1w vs parent -2.7% → AGREE

### Theme ETF tape (biggest |1w| moves)

| Theme | 1d | 1w | Leaders |
|-------|---:|---:|---------|
| Cannabis Based Businesses | +0.0% | -11.2% | MSOS, MJ, CNBS |
| Materials | +0.0% | -7.1% | GDX, XLB, GDXJ |
| Space Exploration & Technology | +0.0% | -6.6% | NASA, ARKX, UFO |
| Energy | +0.0% | -5.9% | XLE, AMLP, VDE |
| Fintech | +0.0% | -5.9% | ARKF, BLOK, BITQ |
| Battery and Energy Storage | +0.0% | -5.6% | LIT, BATT, IBAT |
| Natural Resources | +0.0% | -5.4% | GUNR, GNR, PHO |
| Financials | +0.0% | -4.7% | XLF, VFH, KBWB |
| Future Mobility Production & Tech | +0.0% | -4.1% | DRIV, ROKT, IDRV |
| Agri-business | +0.0% | -4.0% | MOO, VEGI, KROP |
| Technology | +0.0% | -3.8% | VGT, XLK, SMH |
| Industrials | +0.0% | -3.8% | XLI, ITA, VIS |

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
| general | 55% | 64 | ×0.85 |
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

## Horizon weights — book_policy.json v20 · renormalized (absent: sector, general)

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.15 | 0.00 | 0.00 | 0.30 | 0.30 | 0.24 | additive |
| 3d | 0.21 | 0.00 | 0.00 | 0.21 | 0.33 | 0.26 | additive |
| 1w | 0.24 | 0.00 | 0.00 | 0.13 | 0.37 | 0.26 | additive |
| 2w | 0.27 | 0.00 | 0.00 | 0.08 | 0.38 | 0.27 | additive |
| 1m | 0.31 | 0.00 | 0.00 | 0.00 | 0.42 | 0.28 | additive |

## 1d BUY — why these names

### 1. SRRK · $6.2B mid · Healthcare

**1d score +0.744**

**SRRK** is a liquid **mid-cap** Healthcare name (Biotechnology) at $6.2B, ADV ~1861k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.47 | +0.069 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.64 | +0.194 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.95 | +0.231 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.744** | |

### 2. RZLT · $420M small · Healthcare

**1d score +0.723**

**RZLT** is a liquid **small-cap** Healthcare name (Biotechnology) at $420M, ADV ~1677k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.84 | +0.122 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.36 | +0.109 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.99 | +0.242 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.723** | |


## 1d AVOID — bottom of the same rank

- **CSIQ** (small, Technology, $382M) score -0.450. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.6%
- **BTDR** (small, Technology, $898M) score -0.423. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4%
- **DKNG** (large, Consumer Cyclical, $14.0B) score -0.673. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -7.5%
- **IE** (small, Basic Materials, $881M) score -0.460. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -6.6%
- **MSTR** (large, Technology, $34.1B) score -0.447. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4%
- **HYMC** (small, Basic Materials, $1.2B) score -0.336. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.0%
- **WRD** (small, Technology, $916M) score -0.345. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4%
- **HTZ** (small, Industrials, $397M) score -0.462. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | VCEL | +0.872 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 2 | ADMA | +0.870 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | CODI | +0.866 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | PGNY | +0.864 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | PBH | +0.862 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 6 | SPNT | +0.816 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 7 | TNK | +0.815 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 8 | AVNT | +0.793 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 9 | ARLO | +0.751 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 10 | AEO | +0.750 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 11 | PRCH | +0.740 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 12 | VLY | +0.713 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 13 | FA | +0.695 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | CRI | +0.668 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the Finviz industry was **down** |
| 15 | NP | +0.667 | mid | Financial | the Finviz industry was **down** |
| 16 | SMA | +0.664 | mid | Real Estate | the Finviz industry was **down** |
| 17 | ALH | +0.659 | mid | Consumer Cyclical | the Finviz industry was **down** |
| 18 | ALGT | +0.641 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 19 | HNI | +0.637 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 20 | ELPC | +0.624 | mid | Utilities | the Finviz industry was **down** |
| 21 | RLJ | +0.602 | small | Real Estate | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 22 | MEOH | +0.599 | mid | Basic Materials | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 23 | GPRK | +0.570 | small | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 24 | VNOM | +0.550 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 25 | COP | +0.544 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | ADMA | +0.935 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 2 | VCEL | +0.934 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | PGNY | +0.931 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | PBH | +0.925 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | CODI | +0.920 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 6 | SPNT | +0.878 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 7 | TNK | +0.867 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 8 | AVNT | +0.842 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 9 | AEO | +0.799 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | ARLO | +0.793 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 11 | PRCH | +0.787 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 12 | VLY | +0.763 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 13 | ALGT | +0.756 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | FA | +0.744 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | CRI | +0.719 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the Finviz industry was **down** |
| 16 | SMA | +0.718 | mid | Real Estate | the Finviz industry was **down** |
| 17 | NP | +0.717 | mid | Financial | the Finviz industry was **down** |
| 18 | ALH | +0.711 | mid | Consumer Cyclical | the Finviz industry was **down** |
| 19 | HNI | +0.683 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 20 | ELPC | +0.673 | mid | Utilities | the Finviz industry was **down** |
| 21 | RLJ | +0.651 | small | Real Estate | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 22 | MEOH | +0.644 | mid | Basic Materials | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 23 | GPRK | +0.603 | small | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 24 | VNOM | +0.599 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 25 | SOLS | +0.597 | large | Basic Materials | the Finviz industry was **down** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | ADMA | +0.980 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 2 | VCEL | +0.979 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | PGNY | +0.978 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | PBH | +0.967 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | CODI | +0.962 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 6 | SPNT | +0.922 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 7 | TNK | +0.908 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 8 | AVNT | +0.876 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 9 | ALGT | +0.832 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | AEO | +0.831 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 11 | ARLO | +0.813 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 12 | PRCH | +0.813 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 13 | VLY | +0.795 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | FA | +0.778 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | SMA | +0.757 | mid | Real Estate | the Finviz industry was **down** |
| 16 | ALH | +0.750 | mid | Consumer Cyclical | the Finviz industry was **down** |
| 17 | NP | +0.748 | mid | Financial | the Finviz industry was **down** |
| 18 | CRI | +0.747 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the Finviz industry was **down** |
| 19 | HNI | +0.720 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 20 | ELPC | +0.709 | mid | Utilities | the Finviz industry was **down** |
| 21 | RLJ | +0.681 | small | Real Estate | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 22 | MEOH | +0.679 | mid | Basic Materials | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 23 | SOLS | +0.636 | large | Basic Materials | the Finviz industry was **down** |
| 24 | GPRK | +0.635 | small | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 25 | VNOM | +0.632 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1m BUY — why these names

### 1. PGNY · $2.6B mid · Healthcare

**1m score +1.052**

**PGNY** is a liquid **mid-cap** Healthcare name (Healthcare Plans) at $2.6B, ADV ~1104k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.95 | +0.291 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.88 | +0.244 | this week vs its correlated basket |
| map heat / captains | 1.00 | -0.05 | -0.050 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.052** | |

### 2. ADMA · $3.0B mid · Healthcare

**1m score +1.051**

**ADMA** is a liquid **mid-cap** Healthcare name (Biotechnology) at $3.0B, ADV ~3226k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.98 | +0.299 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.85 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.53 | +0.148 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.051** | |

### 3. VCEL · $3.0B mid · Healthcare

**1m score +1.048**

**VCEL** is a liquid **mid-cap** Healthcare name (Biotechnology) at $3.0B, ADV ~665k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.76 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.64 | +0.179 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.048** | |

### 4. TFX · $6.5B mid · Healthcare

**1m score +1.038**

**TFX** is a liquid **mid-cap** Healthcare name (Medical Instruments & Supplies) at $6.5B, ADV ~788k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.95 | +0.291 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.65 | +0.180 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.038** | |

### 5. CODI · $1.0B small · Industrials

**1m score +1.021**

**CODI** is a liquid **small-cap** Industrials name (Conglomerates) at $1.0B, ADV ~613k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.87 | +0.266 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.55 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.98 | +0.273 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.021** | |

### 6. SPNT · $3.1B mid · Financial

**1m score +0.990**

**SPNT** is a liquid **mid-cap** Financial name (Insurance - Reinsurance) at $3.1B, ADV ~943k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.81 | +0.337 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.37 | +0.102 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.990** | |

### 7. TNK · $3.1B mid · Energy

**1m score +0.966**

**TNK** is a liquid **mid-cap** Energy name (Oil & Gas Midstream) at $3.1B, ADV ~505k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment. Checklist marks it as a **rebound-from-own-lows** candidate.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.95 | +0.291 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.55 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.41 | +0.114 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| rebound floor | add | +0.08 | +0.080 | tape at own-history low |
| **1m total** | | | **+0.966** | |

### 8. ALGT · $2.7B mid · Industrials

**1m score +0.959**

**ALGT** is a liquid **mid-cap** Industrials name (Airlines) at $2.7B, ADV ~513k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Today's **news/judge** is a headwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.70 | +0.213 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | -0.77 | -0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.91 | +0.377 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.61 | +0.168 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.959** | |

### 9. AVNT · $4.1B mid · Basic Materials

**1m score +0.930**

**AVNT** is a liquid **mid-cap** Basic Materials name (Specialty Chemicals) at $4.1B, ADV ~769k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.64 | +0.195 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.70 | +0.293 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.69 | +0.192 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.930** | |

### 10. AEO · $3.7B mid · Consumer Cyclical

**1m score +0.885**

**AEO** is a liquid **mid-cap** Consumer Cyclical name (Apparel Retail) at $3.7B, ADV ~5646k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.61 | +0.185 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.70 | +0.293 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.69 | +0.191 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.015 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.885** | |

### 11. PRCH · $2.4B mid · Financial

**1m score +0.864**

**PRCH** is a liquid **mid-cap** Financial name (Insurance - Property & Casualty) at $2.4B, ADV ~1682k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.37 | +0.112 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.67 | +0.186 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.864** | |

### 12. ARLO · $1.4B small · Industrials

**1m score +0.860**

**ARLO** is a liquid **small-cap** Industrials name (Building Products & Equipment) at $1.4B, ADV ~1769k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.22 | +0.067 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.63 | +0.176 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.860** | |

### 13. VLY · $7.6B mid · Financial

**1m score +0.849**

**VLY** is a liquid **mid-cap** Financial name (Banks - Regional) at $7.6B, ADV ~6968k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.67 | +0.204 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.76 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.28 | +0.078 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.849** | |

### 14. FA · $3.6B mid · Industrials

**1m score +0.832**

**FA** is a liquid **mid-cap** Industrials name (Specialty Business Services) at $3.6B, ADV ~1930k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.78 | +0.239 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.64 | +0.265 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.28 | +0.079 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.832** | |

### 15. SMA · $2.3B mid · Real Estate

**1m score +0.816**

**SMA** is a liquid **mid-cap** Real Estate name (REIT - Industrial) at $2.3B, ADV ~892k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.64 | +0.265 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.816** | |

### 16. RDN · $4.7B mid · Financial

**1m score +0.809**

**RDN** is a liquid **mid-cap** Financial name (Insurance - Specialty) at $4.7B, ADV ~1468k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.76 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | -0.03 | -0.010 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.809** | |

### 17. ALH · $5.1B mid · Consumer Cyclical

**1m score +0.808**

**ALH** is a liquid **mid-cap** Consumer Cyclical name (Furnishings, Fixtures & Appliances) at $5.1B, ADV ~1180k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.96 | +0.293 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.64 | +0.265 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.05 | +0.050 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.808** | |

### 18. CRI · $1.3B small · Consumer Cyclical

**1m score +0.803**

**CRI** is a liquid **small-cap** Consumer Cyclical name (Apparel Retail) at $1.3B, ADV ~1413k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.55 | +0.169 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.91 | +0.377 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.15 | +0.041 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.015 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.803** | |

### 19. HNI · $3.6B mid · Consumer Cyclical

**1m score +0.772**

**HNI** is a liquid **mid-cap** Consumer Cyclical name (Furnishings, Fixtures & Appliances) at $3.6B, ADV ~535k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.92 | +0.282 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.46 | +0.193 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.17 | +0.047 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.05 | +0.050 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.772** | |

### 20. ELPC · $7.5B mid · Utilities

**1m score +0.763**

**ELPC** is a liquid **mid-cap** Utilities name (Utilities - Diversified) at $7.5B, ADV ~675k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **washed**. Last earnings were a **beat**. AB/peer context: the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.92 | +0.282 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.55 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.763** | |

### 21. RLJ · $1.9B small · Real Estate

**1m score +0.734**

**RLJ** is a liquid **small-cap** Real Estate name (REIT - Hotel & Motel) at $1.9B, ADV ~1860k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.72 | +0.221 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.76 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | -0.19 | -0.054 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.734** | |

### 22. MEOH · $4.6B mid · Basic Materials

**1m score +0.729**

**MEOH** is a liquid **mid-cap** Basic Materials name (Chemicals) at $4.6B, ADV ~766k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment. Checklist marks it as a **rebound-from-own-lows** candidate.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.98 | +0.300 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.46 | +0.193 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | -0.34 | -0.094 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| rebound floor | add | +0.08 | +0.080 | tape at own-history low |
| **1m total** | | | **+0.729** | |

### 23. SOLS · $10.1B large · Basic Materials

**1m score +0.699**

**SOLS** is a liquid **large-cap** Basic Materials name (Specialty Chemicals) at $10.1B, ADV ~2585k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.92 | +0.282 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.81 | +0.337 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1m total** | | | **+0.699** | |

### 24. VNOM · $14.2B large · Energy

**1m score +0.686**

**VNOM** is a liquid **large-cap** Energy name (Oil & Gas Midstream) at $14.2B, ADV ~2014k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.77 | +0.235 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.64 | +0.265 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.24 | +0.066 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.07 | +0.070 | liquid small/mid, room to run |
| **1m total** | | | **+0.686** | |

### 25. LTC · $2.4B mid · Real Estate

**1m score +0.682**

**LTC** is a liquid **mid-cap** Real Estate name (REIT - Healthcare Facilities) at $2.4B, ADV ~642k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.96 | +0.294 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.55 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | -0.16 | -0.043 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.682** | |


## 1m AVOID — bottom of the same rank

- **DKNG** (large, Consumer Cyclical, $14.0B) score -0.872. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FLUT** (large, Consumer Cyclical, $11.7B) score -0.853. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RCAT** (micro, Industrials, $294M) score -0.812. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RKLB** (large, Industrials, $30.4B) score -0.782. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GPRO** (micro, Technology, $106M) score -0.754. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **HTZ** (small, Industrials, $397M) score -0.754. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **NCMI** (micro, Communication Services, $141M) score -0.750. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PLAY** (micro, Communication Services, $215M) score -0.749. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **IE** (small, Basic Materials, $881M) score -0.749. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **VOD** (large, Communication Services, $31.0B) score -0.746. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FMC** (small, Basic Materials, $636M) score -0.745. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ASTS** (large, Technology, $9.7B) score -0.740. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CSIQ** (small, Technology, $382M) score -0.731. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **KOS** (small, Energy, $953M) score -0.727. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **HIMS** (mid, Healthcare, $3.5B) score -0.713. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GT** (small, Consumer Cyclical, $1.1B) score -0.712. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **QXO** (large, Industrials, $11.5B) score -0.708. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ACHR** (mid, Industrials, $2.4B) score -0.706. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CRML** (small, Basic Materials, $585M) score -0.702. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **QBTS** (mid, Technology, $2.7B) score -0.697. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ALIT** (micro, Technology, $240M) score -0.693. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BTDR** (small, Technology, $898M) score -0.692. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **WHR** (small, Consumer Cyclical, $1.7B) score -0.691. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **UNIT** (small, Real Estate, $845M) score -0.691. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **WSM** (large, Consumer Cyclical, $24.8B) score -0.688. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-11-24_stock_book.md`
- Machine table: `data/stock_book/2026-11-24_stock_book.csv`
- Machine book: `data/stock_book/2026-11-24_stock_book.json`
- Join rank: `data/join/2026-11-24_ranked.csv`
- Weather: `01_daily/weather/2026-11-24_weather.md`
- AB enrich: `data/ab_checklist/2026-11-24_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-11-24_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-11-24_map_heat.md`
