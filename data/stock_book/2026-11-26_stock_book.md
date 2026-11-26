# Stock book — 2026-11-26

_Generated 2026-11-26T14:56:06.181023-05:00_

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
- Stand-down: **no** — 190 names qualified through standard,group_leader,catalyst (41 probable)
- Sector predicts this date: 0/11 (missing → sector layer is 0; Finviz week tape still sits in join)
- News tickers in play: 159
- AB coverage: 2100 names · peer RS: 1988
- Universe after liquidity: 2240
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 123

## All-green BUY / SELL

- Mode: **weighted_fallback** · SELL **core_weights**
- Pile: **0** liquid all-green names (need ≥ 8) of 2240
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
| 1 | **RKT** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Mortgage Finance +0.7% d1 / +4.2% 1w / +6.7% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 2 | **ALNY** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Biotechnology +0.8% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 3 | **AURA** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Biotechnology +0.8% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪ |
| 4 | **EWTX** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Biotechnology +0.8% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=⚪,Cond green |
| 5 | **ADMA** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Biotechnology +0.8% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=⚪,Cond green |
| 6 | **RZLT** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Biotechnology +0.8% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=⚪,Cond green |
| 7 | **ALEC** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Biotechnology +0.8% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=⚪,Cond green |
| 8 | **XNCR** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Biotechnology +0.8% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=⚪,Cond green |
| 9 | **PGEN** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Biotechnology +0.8% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=⚪,Cond green |
| 10 | **IDXX** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Diagnostics & Research +1.1% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=⚪,Cond green |
| 11 | **SPNT** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Insurance - Reinsurance +0.8% d1 / +0.5% 1w / +3.0% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=⚪,Cond green |
| 12 | **TMO** | 🟡🟡🟢🟡🟢🟡 | group_leader | digest is non-directional: Thermo Fisher Scientific announced FDA clearance for a first-of-its-kind test to help clinicians monitor multiple myeloma.; Diagnostics & Research +1.1% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=YELLOW; lookback=🔵,⚪ |
| 13 | **MHK** | 🟡🔴🟢🟡🟢🟢 | group_leader | no direct company event; Furnishings, Fixtures & Appliances +0.7% d1 / +3.6% 1w / +4.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,Cond green |
| 14 | **UNCY** | 🟡🟡🟢🟡🟢🟢 | group_leader | no direct company event; Biotechnology +0.8% d1 / +1.6% 1w / +3.4% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=YELLOW; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=⚪,Cond green |
| 15 | **RYZ** | 🟡🔴🟢🟡🟢🟢 | group_leader | no direct company event; Metal Fabrication +2.7% d1 / +3.8% 1w / +6.5% vs parent | BUY GROUP_LEADER — market=YELLOW; parent=RED; child=GREEN/rel=GREEN; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵 |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **CSIQ** | 🟡🔴🔴🟡🔴🔴 | Solar | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.6% |
| 2 | **MSTR** | 🟡🔴🔴🟡🔴🔴 | Software - Application | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 3 | **RYDE** | 🟡🔴🔴🟡🔴🔴 | Software - Application | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 4 | **WRD** | 🟡🔴🔴🟡🔴🔴 | Software - Application | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 5 | **HYMC** | 🟡🔴🔴🟡🔴🔴 | Gold | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.0% |
| 6 | **BTDR** | 🟡🔴🔴🟡🔴🔴 | Software - Application | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 7 | **GORO** | 🟡🔴🔴🟡🔴🔴 | Gold | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.0% |
| 8 | **MUX** | 🟡🔴🔴🟡🔴🔴 | Other Precious Metals & Mining | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -7.3% |
| 9 | **EXFY** | 🟡🔴🔴🟡🔴🔴 | Software - Application | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 10 | **TOYO** | 🟡🔴🔴🟡🔴🔴 | Solar | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.6% |
| 11 | **BLND** | 🟡🔴🔴🟡🔴🔴 | Software - Application | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 12 | **NEWP** | 🟡🔴🔴🟡🔴🔴 | Silver | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -8.2% |
| 13 | **PLG** | 🟡🔴🔴🟡🔴🔴 | Other Precious Metals & Mining | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -7.3% |
| 14 | **ALIT** | 🟡🔴🔴🟡🔴🔴 | Software - Application | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4% |
| 15 | **ENPH** | 🟡🔴🔴🟡🔴🔴 | Solar | SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.6% |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **finviz_tape** (37 captains, 15 industries → s_heat).
- Board file: `01_daily/map_heat/2026-11-26_map_heat.json` · generated 2026-11-26T05:10:22.998401-05:00

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

### 1. RKT · $43.4B large · Financial

**1d score +0.700**

**RKT** is a liquid **large-cap** Financial name (Mortgage Finance) at $43.4B, ADV ~35429k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.90 | +0.132 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.259 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.69 | +0.167 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.022 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.07 | +0.070 | liquid small/mid, room to run |
| **1d total** | | | **+0.700** | |

### 2. ALNY · $22.1B large · Healthcare

**1d score +0.607**

**ALNY** is a liquid **large-cap** Healthcare name (Biotechnology) at $22.1B, ADV ~2118k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **washed**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.99 | +0.144 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.55 | +0.169 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.55 | +0.133 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.11 | +0.110 | liquid small/mid, room to run |
| **1d total** | | | **+0.607** | |

### 3. AURA · $807M small · Healthcare

**1d score +0.588**

**AURA** is a liquid **small-cap** Healthcare name (Biotechnology) at $807M, ADV ~842k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.78 | +0.114 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.215 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.04 | +0.009 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.588** | |

### 4. EWTX · $5.2B mid · Healthcare

**1d score +0.748**

**EWTX** is a liquid **mid-cap** Healthcare name (Biotechnology) at $5.2B, ADV ~1536k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.98 | +0.143 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.215 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.78 | +0.190 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.748** | |

### 5. ADMA · $3.2B mid · Healthcare

**1d score +0.722**

**ADMA** is a liquid **mid-cap** Healthcare name (Biotechnology) at $3.2B, ADV ~3241k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.99 | +0.144 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.93 | +0.282 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.39 | +0.096 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.722** | |

### 6. RZLT · $443M small · Healthcare

**1d score +0.660**

**RZLT** is a liquid **small-cap** Healthcare name (Biotechnology) at $443M, ADV ~1686k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.74 | +0.109 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.215 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.56 | +0.137 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.660** | |

### 7. ALEC · $406M small · Healthcare

**1d score +0.676**

**ALEC** is a liquid **small-cap** Healthcare name (Biotechnology) at $406M, ADV ~3375k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.89 | +0.131 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.94 | +0.287 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.24 | +0.058 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.676** | |

### 8. XNCR · $2.9B mid · Healthcare

**1d score +0.688**

**XNCR** is a liquid **mid-cap** Healthcare name (Biotechnology) at $2.9B, ADV ~1561k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.95 | +0.139 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.55 | +0.169 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.74 | +0.180 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.688** | |


## 1d AVOID — bottom of the same rank

- **CSIQ** (small, Technology, $370M) score -0.453. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.6%
- **MSTR** (large, Technology, $33.8B) score -0.461. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4%
- **RYDE** (micro, Technology, $117M) score -0.481. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4%
- **WRD** (small, Technology, $878M) score -0.380. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4%
- **HYMC** (small, Basic Materials, $1.3B) score -0.309. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.0%
- **BTDR** (small, Technology, $842M) score -0.346. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -5.4%
- **GORO** (small, Basic Materials, $461M) score -0.316. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -4.0%
- **MUX** (small, Basic Materials, $578M) score -0.345. SELL/AVOID — market=YELLOW; red domains=parent,child,setup,flow; child lags parent -7.3%

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | ARDT | +0.958 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | EMBC | +0.896 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up** |
| 3 | ETON | +0.854 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 4 | LFST | +0.853 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | CSGP | +0.845 | large | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 6 | RKT | +0.786 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | SPNT | +0.766 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | CMBT | +0.764 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | ATAT | +0.753 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | DHT | +0.748 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | CWK | +0.709 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 12 | NAT | +0.703 | small | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | HGTY | +0.688 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 14 | FG | +0.669 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 15 | HLF | +0.652 | small | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 16 | AMC | +0.644 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 17 | OXY | +0.640 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 18 | SMA | +0.636 | mid | Real Estate | the Finviz industry was **advancing** |
| 19 | AVNT | +0.621 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 20 | INN | +0.608 | small | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 21 | CODI | +0.584 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 22 | MKC | +0.569 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 23 | NWL | +0.542 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 24 | NOMD | +0.528 | small | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 25 | VFC | +0.518 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | ARDT | +1.027 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | EMBC | +0.956 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up** |
| 3 | LFST | +0.921 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | ETON | +0.919 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 5 | CSGP | +0.886 | large | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 6 | RKT | +0.849 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | SPNT | +0.831 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | CMBT | +0.825 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | DHT | +0.806 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | ATAT | +0.805 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 11 | CWK | +0.760 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 12 | NAT | +0.758 | small | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | HGTY | +0.748 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 14 | FG | +0.721 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 15 | EXE | +0.698 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 16 | SMA | +0.692 | mid | Real Estate | the Finviz industry was **advancing** |
| 17 | HLF | +0.690 | small | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 18 | AMC | +0.669 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 19 | INN | +0.655 | small | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 20 | AVNT | +0.651 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 21 | CODI | +0.611 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 22 | MKC | +0.601 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 23 | NWL | +0.562 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 24 | NOMD | +0.554 | small | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 25 | VFC | +0.546 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | ARDT | +1.074 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | EMBC | +0.996 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up** |
| 3 | LFST | +0.967 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | ETON | +0.964 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 5 | CSGP | +0.917 | large | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 6 | RKT | +0.893 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | SPNT | +0.875 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | CMBT | +0.868 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | DHT | +0.848 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | ATAT | +0.842 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 11 | NAT | +0.799 | small | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | CWK | +0.797 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 13 | HGTY | +0.790 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 14 | FG | +0.758 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 15 | EXE | +0.740 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 16 | SMA | +0.732 | mid | Real Estate | the Finviz industry was **advancing** |
| 17 | HLF | +0.710 | small | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 18 | INN | +0.684 | small | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 19 | AMC | +0.676 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 20 | AVNT | +0.663 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 21 | CODI | +0.624 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 22 | MKC | +0.615 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 23 | NWL | +0.567 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 24 | NOMD | +0.563 | small | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 25 | VFC | +0.552 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |

## 1m BUY — why these names

### 1. ARDT · $2.2B mid · Healthcare

**1m score +1.150**

**ARDT** is a liquid **mid-cap** Healthcare name (Medical Care Facilities) at $2.2B, ADV ~740k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.96 | +0.293 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.95 | +0.398 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.75 | +0.209 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.150** | |

### 2. EMBC · $449M small · Healthcare

**1m score +1.063**

**EMBC** is a liquid **small-cap** Healthcare name (Medical Instruments & Supplies) at $449M, ADV ~1099k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.72 | +0.219 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +1.00 | +0.277 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.063** | |

### 3. LFST · $5.5B mid · Healthcare

**1m score +1.041**

**LFST** is a liquid **mid-cap** Healthcare name (Medical Care Facilities) at $5.5B, ADV ~5301k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.98 | +0.299 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.93 | +0.386 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.56 | +0.156 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.041** | |

### 4. ETON · $1.9B small · Healthcare

**1m score +1.035**

**ETON** is a liquid **small-cap** Healthcare name (Drug Manufacturers - Specialty & Generic) at $1.9B, ADV ~567k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.42 | +0.117 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.035** | |

### 5. RKT · $43.4B large · Financial

**1m score +0.962**

**RKT** is a liquid **large-cap** Financial name (Mortgage Finance) at $43.4B, ADV ~35429k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.90 | +0.276 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.85 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.69 | +0.191 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.02 | +0.022 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.07 | +0.070 | liquid small/mid, room to run |
| **1m total** | | | **+0.962** | |

### 6. CSGP · $13.7B large · Real Estate

**1m score +0.962**

**CSGP** is a liquid **large-cap** Real Estate name (Real Estate Services) at $13.7B, ADV ~6234k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.98 | +0.300 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.31 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.76 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.84 | +0.234 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.11 | +0.110 | liquid small/mid, room to run |
| **1m total** | | | **+0.962** | |

### 7. SPNT · $3.1B mid · Financial

**1m score +0.947**

**SPNT** is a liquid **mid-cap** Financial name (Insurance - Reinsurance) at $3.1B, ADV ~948k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.93 | +0.386 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.22 | +0.060 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.947** | |

### 8. CMBT · $5.1B mid · Energy

**1m score +0.936**

**CMBT** is a liquid **mid-cap** Energy name (Oil & Gas Midstream) at $5.1B, ADV ~1141k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.76 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.42 | +0.117 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.936** | |

### 9. DHT · $3.5B mid · Energy

**1m score +0.911**

**DHT** is a liquid **mid-cap** Energy name (Oil & Gas Midstream) at $3.5B, ADV ~4764k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.70 | +0.293 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.24 | +0.066 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.911** | |

### 10. ATAT · $5.5B mid · Consumer Cyclical

**1m score +0.900**

**ATAT** is a liquid **mid-cap** Consumer Cyclical name (Lodging) at $5.5B, ADV ~917k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.79 | +0.243 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.70 | +0.293 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.41 | +0.114 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.900** | |

### 11. NAT · $1.4B small · Energy

**1m score +0.860**

**NAT** is a liquid **small-cap** Energy name (Oil & Gas Midstream) at $1.4B, ADV ~4825k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.95 | +0.291 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.64 | +0.265 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.37 | +0.104 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.860** | |

### 12. HGTY · $5.7B mid · Financial

**1m score +0.856**

**HGTY** is a liquid **mid-cap** Financial name (Insurance - Property & Casualty) at $5.7B, ADV ~696k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.97 | +0.297 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.85 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.02 | +0.006 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.856** | |

### 13. CWK · $3.3B mid · Real Estate

**1m score +0.854**

**CWK** is a liquid **mid-cap** Real Estate name (Real Estate Services) at $3.3B, ADV ~2663k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.81 | +0.247 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.64 | +0.265 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.51 | +0.143 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.854** | |

### 14. FG · $3.0B mid · Financial

**1m score +0.815**

**FG** is a liquid **mid-cap** Financial name (Insurance - Life) at $3.0B, ADV ~681k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.85 | +0.260 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.64 | +0.265 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.32 | +0.090 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.815** | |

### 15. EXE · $20.0B large · Energy

**1m score +0.806**

**EXE** is a liquid **large-cap** Energy name (Oil & Gas E&P) at $20.0B, ADV ~3124k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.98 | +0.301 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.76 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.28 | +0.078 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.11 | +0.110 | liquid small/mid, room to run |
| **1m total** | | | **+0.806** | |

### 16. SMA · $2.4B mid · Real Estate

**1m score +0.793**

**SMA** is a liquid **mid-cap** Real Estate name (REIT - Industrial) at $2.4B, ADV ~899k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.98 | +0.300 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.70 | +0.293 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.793** | |

### 17. HLF · $1.8B small · Consumer Defensive

**1m score +0.752**

**HLF** is a liquid **small-cap** Consumer Defensive name (Packaged Foods) at $1.8B, ADV ~1583k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.27 | +0.082 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.70 | +0.293 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.63 | +0.176 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.752** | |

### 18. INN · $872M small · Real Estate

**1m score +0.735**

**INN** is a liquid **small-cap** Real Estate name (REIT - Hotel & Motel) at $872M, ADV ~1090k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.65 | +0.197 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.70 | +0.293 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.16 | +0.045 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.735** | |

### 19. AMC · $2.9B mid · Communication Services

**1m score +0.704**

**AMC** is a liquid **mid-cap** Communication Services name (Entertainment) at $2.9B, ADV ~25011k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | -0.15 | -0.046 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.70 | +0.293 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.74 | +0.206 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.704** | |

### 20. AVNT · $4.2B mid · Basic Materials

**1m score +0.696**

**AVNT** is a liquid **mid-cap** Basic Materials name (Specialty Chemicals) at $4.2B, ADV ~766k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.07 | +0.020 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.70 | +0.293 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.48 | +0.133 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.696** | |

### 21. DG · $30.7B large · Consumer Defensive

**1m score +0.654**

**DG** is a liquid **large-cap** Consumer Defensive name (Discount Stores) at $30.7B, ADV ~2375k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.60 | +0.184 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.37 | +0.103 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1m total** | | | **+0.654** | |

### 22. CODI · $1.0B small · Industrials

**1m score +0.653**

**CODI** is a liquid **small-cap** Industrials name (Conglomerates) at $1.0B, ADV ~617k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.07 | +0.020 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.55 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.72 | +0.201 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.653** | |

### 23. NOMD · $1.7B small · Consumer Defensive

**1m score +0.591**

**NOMD** is a liquid **small-cap** Consumer Defensive name (Packaged Foods) at $1.7B, ADV ~1197k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.05 | +0.015 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.64 | +0.265 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.22 | +0.061 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.591** | |

### 24. NWL · $2.5B mid · Consumer Defensive

**1m score +0.589**

**NWL** is a liquid **mid-cap** Consumer Defensive name (Household & Personal Products) at $2.5B, ADV ~5937k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | -0.15 | -0.046 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.64 | +0.265 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.43 | +0.120 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.589** | |

### 25. VFC · $8.5B mid · Consumer Cyclical

**1m score +0.582**

**VFC** is a liquid **mid-cap** Consumer Cyclical name (Apparel Manufacturing) at $8.5B, ADV ~9344k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | -0.15 | -0.046 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.91 | +0.377 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.18 | +0.051 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.582** | |


## 1m AVOID — bottom of the same rank

- **VOD** (large, Communication Services, $31.2B) score -0.817. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **RCAT** (micro, Industrials, $269M) score -0.795. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RKLB** (large, Industrials, $29.7B) score -0.788. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AMBA** (small, Technology, $1.6B) score -0.770. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AMZN** (mega, Consumer Cyclical, $2643.1B) score -0.769. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **HTZ** (small, Industrials, $375M) score -0.753. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **PLAY** (micro, Communication Services, $206M) score -0.748. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CSIQ** (small, Technology, $370M) score -0.734. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ECX** (micro, Consumer Cyclical, $209M) score -0.725. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LCID** (small, Consumer Cyclical, $1.0B) score -0.723. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RYDE** (micro, Technology, $117M) score -0.716. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **WOLF** (small, Technology, $961M) score -0.715. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CNDT** (micro, Technology, $142M) score -0.714. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **RUM** (small, Communication Services, $1.7B) score -0.712. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TE** (small, Industrials, $331M) score -0.711. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **MSTR** (large, Technology, $33.8B) score -0.708. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **FMC** (small, Basic Materials, $638M) score -0.706. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **FLUT** (large, Consumer Cyclical, $12.3B) score -0.705. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CRML** (small, Basic Materials, $552M) score -0.705. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PACK** (micro, Consumer Cyclical, $245M) score -0.704. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **GOOGL** (mega, Communication Services, $3324.7B) score -0.702. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **UAMY** (small, Basic Materials, $330M) score -0.700. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NEOV** (micro, Industrials, $108M) score -0.698. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GOOG** (mega, Communication Services, $3328.4B) score -0.696. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **EH** (micro, Industrials, $195M) score -0.694. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-11-26_stock_book.md`
- Machine table: `data/stock_book/2026-11-26_stock_book.csv`
- Machine book: `data/stock_book/2026-11-26_stock_book.json`
- Join rank: `data/join/2026-11-26_ranked.csv`
- Weather: `01_daily/weather/2026-11-26_weather.md`
- AB enrich: `data/ab_checklist/2026-11-26_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-11-26_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-11-26_map_heat.md`
