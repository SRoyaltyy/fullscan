# Stock book — 2026-11-25

_Generated 2026-11-25T15:38:37.995600-05:00_

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
- Stand-down: **no** — 290 names qualified through standard,group_leader,catalyst (47 probable)
- Sector predicts this date: 0/11 (missing → sector layer is 0; Finviz week tape still sits in join)
- News tickers in play: 155
- AB coverage: 2081 names · peer RS: 1992
- Universe after liquidity: 2237
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 119

## All-green BUY / SELL

- Mode: **weighted_fallback** · SELL **core_weights**
- Pile: **0** liquid all-green names (need ≥ 8) of 2237
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
| 1 | **BBIO** | 🟡🟡🟢🟡🟢🟢 | group_leader | direct high digest (stale/undated): BridgeBio receives traditional FDA approval for Welgemda for limb-girdle muscular dystrophy type 2I/R9; Biotechnology +0.8% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.48); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 2 | **ANF** | 🟡🔴🟢🟡🟢🟢 | group_leader | direct high digest (stale/undated): Abercrombie beats fiscal Q3 2027 estimates with EPS of $3.82 and revenue of $1.44B, raises 2026 outlook; Apparel Retail +0.2% d1 / +3.7% 1w / +4.6% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.48); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 3 | **VCEL** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Biotechnology +0.8% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 4 | **EWTX** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Biotechnology +0.8% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 5 | **PLX** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Biotechnology +0.8% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 6 | **TGTX** | 🟡🟡🟢🟡🟢🟢 | group_leader | digest is non-directional: FDA accepts TG Therapeutics’ application for single-dose BRIUMVI initiation; Biotechnology +0.8% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 7 | **CDNA** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Diagnostics & Research +1.1% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 8 | **IDXX** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Diagnostics & Research +1.1% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 9 | **RYTM** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Biotechnology +0.8% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 10 | **CGON** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Biotechnology +0.8% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 11 | **ALEC** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Biotechnology +0.8% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 12 | **WAT** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Diagnostics & Research +1.1% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 13 | **DHR** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Diagnostics & Research +1.1% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 14 | **MRNA** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Biotechnology +0.8% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 15 | **REPL** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Biotechnology +0.8% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **CSIQ** | 🟡🔴🔴🟡🔴🔴 | Solar | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.6% |
| 2 | **BTDR** | 🟡🔴🔴🟡🔴🔴 | Software - Application | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 3 | **MSTR** | 🟡🔴🔴🟡🔴🔴 | Software - Application | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 4 | **WRD** | 🟡🔴🔴🟡🔴🔴 | Software - Application | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 5 | **NCMI** | 🟡🔴🔴🟡🔴🔴 | Advertising Agencies | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.6% |
| 6 | **ALIT** | 🟡🔴🔴🟡🔴🔴 | Software - Application | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 7 | **EXFY** | 🟡🔴🔴🟡🔴🔴 | Software - Application | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 8 | **TOYO** | 🟡🔴🔴🟡🔴🔴 | Solar | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.6% |
| 9 | **MUX** | 🟡🔴🔴🟡🔴🔴 | Other Precious Metals & Mining | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -7.3% |
| 10 | **BLND** | 🟡🔴🔴🟡🔴🔴 | Software - Application | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 11 | **GRAB** | 🟡🔴🔴🟡🔴🔴 | Software - Application | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 12 | **AIOT** | 🟡🔴🔴🟡🔴🔴 | Software - Application | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 13 | **HYMC** | 🟡🔴🔴🟡🔴🔴 | Gold | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.0% |
| 14 | **RBBN** | 🟡🔴🔴🟡🔴🔴 | Software - Application | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 15 | **RYDE** | 🟡🔴🔴🟡🔴🔴 | Software - Application | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4% |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **finviz_tape** (37 captains, 15 industries → s_heat).
- Board file: `01_daily/map_heat/2026-11-25_map_heat.json` · generated 2026-11-25T05:09:58.312210-05:00

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

### 1. BBIO · $16.7B large · Healthcare

**1d score +0.418**

**BBIO** is a liquid **large-cap** Healthcare name (Biotechnology) at $16.7B, ADV ~2635k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.95 | +0.139 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.46 | +0.141 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.57 | +0.139 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1d total** | | | **+0.418** | |

### 2. ANF · $7.1B mid · Consumer Cyclical

**1d score +0.853**

**ANF** is a liquid **mid-cap** Consumer Cyclical name (Apparel Retail) at $7.1B, ADV ~1581k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.19 | +0.028 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.31 | +0.094 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.247 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.89 | +0.218 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.015 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.853** | |

### 3. VCEL · $3.1B mid · Healthcare

**1d score +0.831**

**VCEL** is a liquid **mid-cap** Healthcare name (Biotechnology) at $3.1B, ADV ~665k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.97 | +0.142 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.259 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.74 | +0.180 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.831** | |

### 4. EWTX · $5.1B mid · Healthcare

**1d score +0.746**

**EWTX** is a liquid **mid-cap** Healthcare name (Biotechnology) at $5.1B, ADV ~1529k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.98 | +0.144 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.215 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.57 | +0.138 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.746** | |

### 5. PLX · $267M micro · Healthcare

**1d score +0.453**

**PLX** is a liquid **micro-cap** Healthcare name (Biotechnology) at $267M, ADV ~903k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.89 | +0.131 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.268 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.51 | +0.124 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.12 | -0.120 | liquid small/mid, room to run |
| **1d total** | | | **+0.453** | |

### 6. TGTX · $10.6B large · Healthcare

**1d score +0.477**

**TGTX** is a liquid **large-cap** Healthcare name (Biotechnology) at $10.6B, ADV ~1851k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.88 | +0.129 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.215 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.79 | +0.194 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.11 | -0.110 | liquid small/mid, room to run |
| **1d total** | | | **+0.477** | |

### 7. CDNA · $4.1B mid · Healthcare

**1d score +0.708**

**CDNA** is a liquid **mid-cap** Healthcare name (Diagnostics & Research) at $4.1B, ADV ~1297k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.91 | +0.133 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.247 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.32 | +0.079 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.708** | |

### 8. IDXX · $56.1B large · Healthcare

**1d score +0.451**

**IDXX** is a liquid **large-cap** Healthcare name (Diagnostics & Research) at $56.1B, ADV ~557k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.80 | +0.117 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.259 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.31 | +0.075 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1d total** | | | **+0.451** | |


## 1d AVOID — bottom of the same rank

- **CSIQ** (small, Technology, $329M) score -0.403. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.6%
- **BTDR** (small, Technology, $768M) score -0.362. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4%
- **MSTR** (large, Technology, $32.8B) score -0.516. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4%
- **WRD** (small, Technology, $864M) score -0.390. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4%
- **NCMI** (micro, Communication Services, $145M) score -0.494. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.6%
- **ALIT** (micro, Technology, $254M) score -0.434. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4%
- **EXFY** (micro, Technology, $129M) score -0.473. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4%
- **TOYO** (micro, Technology, $156M) score -0.264. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.6%

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | VCEL | +0.922 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 2 | PBH | +0.889 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide) |
| 3 | SRRK | +0.885 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | ANF | +0.867 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | PRCH | +0.858 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | ETON | +0.854 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide) |
| 7 | PTON | +0.842 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 8 | CSGP | +0.825 | large | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 9 | AEO | +0.817 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | SPNT | +0.810 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 11 | CRI | +0.793 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | SLDE | +0.774 | mid | Financial | the Finviz industry was **advancing** |
| 13 | SEZL | +0.712 | mid | Financial | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | LTC | +0.711 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 15 | HLF | +0.676 | small | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 16 | SMA | +0.656 | mid | Real Estate | the Finviz industry was **advancing** |
| 17 | HIW | +0.644 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 18 | DLTR | +0.538 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 19 | OLLI | +0.516 | mid | Consumer Defensive | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 20 | EXE | +0.510 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 21 | DG | +0.503 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 22 | CHRD | +0.465 | mid | Energy | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 23 | DHT | +0.438 | mid | Energy | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 24 | RYZ | +0.425 | small | Industrials | the Finviz industry was **down** |
| 25 | KNTK | +0.405 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | VCEL | +0.988 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 2 | PBH | +0.957 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide) |
| 3 | SRRK | +0.948 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | PRCH | +0.921 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | ETON | +0.919 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide) |
| 6 | PTON | +0.896 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 7 | ANF | +0.885 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | AEO | +0.880 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | SPNT | +0.875 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 10 | CSGP | +0.864 | large | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 11 | CRI | +0.859 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | SLDE | +0.839 | mid | Financial | the Finviz industry was **advancing** |
| 13 | SEZL | +0.770 | mid | Financial | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | LTC | +0.769 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 15 | SMA | +0.714 | mid | Real Estate | the Finviz industry was **advancing** |
| 16 | HLF | +0.713 | small | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 17 | HIW | +0.689 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 18 | OLLI | +0.565 | mid | Consumer Defensive | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 19 | DLTR | +0.555 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 20 | DG | +0.554 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 21 | EXE | +0.549 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 22 | DHT | +0.466 | mid | Energy | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 23 | RYZ | +0.445 | small | Industrials | the Finviz industry was **down** |
| 24 | KNTK | +0.421 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 25 | CHRD | +0.417 | mid | Energy | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | VCEL | +1.034 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 2 | PBH | +1.004 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide) |
| 3 | SRRK | +0.991 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | ETON | +0.965 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide) |
| 5 | PRCH | +0.963 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | PTON | +0.930 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 7 | AEO | +0.923 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | SPNT | +0.920 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 9 | CRI | +0.903 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | CSGP | +0.893 | large | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 11 | ATAT | +0.891 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 12 | SLDE | +0.881 | mid | Financial | the Finviz industry was **advancing** |
| 13 | LTC | +0.810 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 14 | SEZL | +0.809 | mid | Financial | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | SMA | +0.755 | mid | Real Estate | the Finviz industry was **advancing** |
| 16 | HLF | +0.732 | small | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 17 | HIW | +0.725 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 18 | OLLI | +0.593 | mid | Consumer Defensive | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 19 | DG | +0.585 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 20 | EXE | +0.576 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 21 | DLTR | +0.556 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 22 | DHT | +0.492 | mid | Energy | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 23 | RYZ | +0.447 | small | Industrials | the Finviz industry was **down** |
| 24 | KNTK | +0.423 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 25 | CHRD | +0.384 | mid | Energy | this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1m BUY — why these names

### 1. VCEL · $3.1B mid · Healthcare

**1m score +1.106**

**VCEL** is a liquid **mid-cap** Healthcare name (Biotechnology) at $3.1B, ADV ~665k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.97 | +0.298 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.85 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.74 | +0.205 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.106** | |

### 2. PBH · $2.4B mid · Healthcare

**1m score +1.079**

**PBH** is a liquid **mid-cap** Healthcare name (Drug Manufacturers - Specialty & Generic) at $2.4B, ADV ~583k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide). Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.97 | +0.295 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.94 | +0.392 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.69 | +0.192 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.079** | |

### 3. SRRK · $6.5B mid · Healthcare

**1m score +1.060**

**SRRK** is a liquid **mid-cap** Healthcare name (Biotechnology) at $6.5B, ADV ~1861k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.84 | +0.258 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.85 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.90 | +0.249 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.060** | |

### 4. ETON · $1.9B small · Healthcare

**1m score +1.038**

**ETON** is a liquid **small-cap** Healthcare name (Drug Manufacturers - Specialty & Generic) at $1.9B, ADV ~568k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide). Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.85 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.66 | +0.183 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.038** | |

### 5. PRCH · $2.5B mid · Financial

**1m score +1.032**

**PRCH** is a liquid **mid-cap** Financial name (Insurance - Property & Casualty) at $2.5B, ADV ~1682k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.80 | +0.246 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.94 | +0.392 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.70 | +0.195 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.032** | |

### 6. AEO · $3.8B mid · Consumer Cyclical

**1m score +0.993**

**AEO** is a liquid **mid-cap** Consumer Cyclical name (Apparel Retail) at $3.8B, ADV ~5646k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.93 | +0.285 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.45 | +0.126 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.015 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.993** | |

### 7. SPNT · $3.1B mid · Financial

**1m score +0.992**

**SPNT** is a liquid **mid-cap** Financial name (Insurance - Reinsurance) at $3.1B, ADV ~943k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.45 | +0.124 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.992** | |

### 8. PTON · $2.5B mid · Consumer Cyclical

**1m score +0.989**

**PTON** is a liquid **mid-cap** Consumer Cyclical name (Leisure) at $2.5B, ADV ~9135k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.63 | +0.193 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.81 | +0.337 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.75 | +0.209 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.989** | |

### 9. CRI · $1.4B small · Consumer Cyclical

**1m score +0.976**

**CRI** is a liquid **small-cap** Consumer Cyclical name (Apparel Retail) at $1.4B, ADV ~1413k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.97 | +0.297 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.95 | +0.398 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.24 | +0.066 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.015 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.976** | |

### 10. ATAT · $5.4B mid · Consumer Cyclical

**1m score +0.956**

**ATAT** is a liquid **mid-cap** Consumer Cyclical name (Lodging) at $5.4B, ADV ~918k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.70 | +0.293 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.40 | +0.111 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.956** | |

### 11. SLDE · $2.9B mid · Financial

**1m score +0.953**

**SLDE** is a liquid **mid-cap** Financial name (Insurance - Property & Casualty) at $2.9B, ADV ~1385k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.96 | +0.402 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.953** | |

### 12. CSGP · $13.1B large · Real Estate

**1m score +0.935**

**CSGP** is a liquid **large-cap** Real Estate name (Real Estate Services) at $13.1B, ADV ~6221k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.98 | +0.299 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.31 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.76 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.57 | +0.159 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.11 | +0.110 | liquid small/mid, room to run |
| **1m total** | | | **+0.935** | |

### 13. LTC · $2.5B mid · Real Estate

**1m score +0.874**

**LTC** is a liquid **mid-cap** Real Estate name (REIT - Healthcare Facilities) at $2.5B, ADV ~642k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.76 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.02 | +0.006 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.874** | |

### 14. SEZL · $4.9B mid · Financial

**1m score +0.873**

**SEZL** is a liquid **mid-cap** Financial name (Credit Services) at $4.9B, ADV ~522k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.91 | +0.278 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.85 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | -0.03 | -0.008 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.873** | |

### 15. SMA · $2.3B mid · Real Estate

**1m score +0.819**

**SMA** is a liquid **mid-cap** Real Estate name (REIT - Industrial) at $2.3B, ADV ~892k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.76 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.819** | |

### 16. HIW · $3.4B mid · Real Estate

**1m score +0.775**

**HIW** is a liquid **mid-cap** Real Estate name (REIT - Office) at $3.4B, ADV ~1142k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.88 | +0.268 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.46 | +0.193 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.23 | +0.065 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.775** | |

### 17. HLF · $1.7B small · Consumer Defensive

**1m score +0.772**

**HLF** is a liquid **small-cap** Consumer Defensive name (Packaged Foods) at $1.7B, ADV ~1582k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.20 | +0.060 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.70 | +0.293 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.78 | +0.218 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.772** | |

### 18. OLLI · $5.6B mid · Consumer Defensive

**1m score +0.647**

**OLLI** is a liquid **mid-cap** Consumer Defensive name (Discount Stores) at $5.6B, ADV ~1686k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.67 | +0.206 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | -0.45 | -0.125 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.647** | |

### 19. DG · $30.6B large · Consumer Defensive

**1m score +0.641**

**DG** is a liquid **large-cap** Consumer Defensive name (Discount Stores) at $30.6B, ADV ~2360k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.59 | +0.179 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.85 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.39 | +0.109 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1m total** | | | **+0.641** | |

### 20. EXE · $19.0B large · Energy

**1m score +0.619**

**EXE** is a liquid **large-cap** Energy name (Oil & Gas E&P) at $19.0B, ADV ~3135k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.59 | +0.179 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.55 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.17 | +0.048 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.11 | +0.110 | liquid small/mid, room to run |
| **1m total** | | | **+0.619** | |

### 21. DLTR · $29.0B large · Consumer Defensive

**1m score +0.575**

**DLTR** is a liquid **large-cap** Consumer Defensive name (Discount Stores) at $29.0B, ADV ~2451k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.07 | +0.020 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.31 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.91 | +0.377 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.85 | +0.237 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.11 | -0.110 | liquid small/mid, room to run |
| **1m total** | | | **+0.575** | |

### 22. DHT · $3.4B mid · Energy

**1m score +0.523**

**DHT** is a liquid **mid-cap** Energy name (Oil & Gas Midstream) at $3.4B, ADV ~4742k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.75 | +0.228 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.12 | +0.052 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | -0.02 | -0.007 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.523** | |

### 23. RYZ · $1.4B small · Industrials

**1m score +0.469**

**RYZ** is a liquid **small-cap** Industrials name (Metal Fabrication) at $1.4B, ADV ~508k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | -0.15 | -0.046 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.70 | +0.293 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.021 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.469** | |

### 24. KNTK · $8.8B mid · Energy

**1m score +0.440**

**KNTK** is a liquid **mid-cap** Energy name (Oil & Gas Midstream) at $8.8B, ADV ~1221k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big miss**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | -0.15 | -0.046 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.55 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.20 | +0.055 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.440** | |

### 25. TK · $1.2B small · Energy

**1m score +0.348**

**TK** is a liquid **small-cap** Energy name (Oil & Gas Midstream) at $1.2B, ADV ~508k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.09 | +0.028 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.12 | +0.052 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.24 | +0.068 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.348** | |


## 1m AVOID — bottom of the same rank

- **RCAT** (micro, Industrials, $264M) score -0.800. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RKLB** (large, Industrials, $29.1B) score -0.796. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **MSTR** (large, Technology, $32.8B) score -0.785. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **VOD** (large, Communication Services, $31.1B) score -0.781. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **NCMI** (micro, Communication Services, $145M) score -0.773. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **KOS** (small, Energy, $941M) score -0.758. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **HTZ** (small, Industrials, $368M) score -0.755. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **PLAY** (micro, Communication Services, $206M) score -0.752. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **DKNG** (large, Consumer Cyclical, $14.3B) score -0.748. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **CNDT** (micro, Technology, $144M) score -0.745. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **FMC** (small, Basic Materials, $649M) score -0.742. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SID** (small, Basic Materials, $672M) score -0.725. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **QBTS** (mid, Technology, $2.6B) score -0.720. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ASTS** (mid, Technology, $9.2B) score -0.714. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **EXFY** (micro, Technology, $129M) score -0.714. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RUM** (small, Communication Services, $1.9B) score -0.713. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GPRO** (micro, Technology, $111M) score -0.706. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **RGTI** (mid, Technology, $2.6B) score -0.706. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **WOLF** (small, Technology, $975M) score -0.706. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **KLXE** (micro, Energy, $96M) score -0.703. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ACHR** (mid, Industrials, $2.3B) score -0.703. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BBAI** (small, Technology, $741M) score -0.696. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **DJT** (small, Communication Services, $1.0B) score -0.695. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **EH** (micro, Industrials, $175M) score -0.691. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RZLV** (small, Technology, $361M) score -0.690. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-11-25_stock_book.md`
- Machine table: `data/stock_book/2026-11-25_stock_book.csv`
- Machine book: `data/stock_book/2026-11-25_stock_book.json`
- Join rank: `data/join/2026-11-25_ranked.csv`
- Weather: `01_daily/weather/2026-11-25_weather.md`
- AB enrich: `data/ab_checklist/2026-11-25_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-11-25_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-11-25_map_heat.md`
