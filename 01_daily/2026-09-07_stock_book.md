# Stock book — 2026-09-07

_Generated 2026-09-07T05:32:04.437552-04:00_

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
- Stand-down: **no** — 331 names qualified through standard,group_leader,catalyst (11 probable)
- Sector predicts this date: 0/11 (missing → sector layer is 0; Finviz week tape still sits in join)
- News tickers in play: 48
- AB coverage: 1966 names · peer RS: 1852
- Universe after liquidity: 2088
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 20

## All-green BUY / SELL

- Mode: **weighted_fallback** · SELL **core_weights**
- Pile: **0** liquid all-green names (need ≥ 8) of 2088
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
| 1 | **JCI** | 🟡🟡🟡🟡🟢🟢 | standard | direct high digest (stale/undated): Zacks efficiency screen and post-Q3 earnings momentum drive 5.25% JCI surge; Building Products & Equipment +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.48); setup=GREEN; flow=GREEN; lookback=🔵,⚪ |
| 2 | **ALAB** | 🟡🟡🟡🟡🟢🟢 | standard | direct normal digest (stale/undated): S&P 500 inclusion speculation fuels Astera Labs 12% intraday surge; Semiconductors +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.30); setup=GREEN; flow=GREEN; lookback=🔵,⚪ |
| 3 | **CRWD** | 🟡🟡🟡🟡🟢🟡 | standard | direct high digest (stale/undated): Analyst price target hikes on Fal.Con 2026 guidance drive CRWD 5.7% surge; Software - Infrastructure +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.48); setup=GREEN; flow=YELLOW; lookback=🔵,⚪ |
| 4 | **BKR** | 🟡🟡🟡🟡🟢🟡 | standard | direct high digest (stale/undated): Baker Hughes wins significant bp contract for vessel-based offshore stimulation and production enhancement in UK North Sea; Oil & Gas Equipment & Services +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.48); setup=GREEN; flow=YELLOW; lookback=🔵,⚪ |
| 5 | **KEYS** | 🟡🟡🟡🟡🟢🔴 | probable | direct high digest (stale/undated): Keysight posts record Q3 with EPS $3.07 (+78% YoY), revenue $1.846B (+36% YoY), orders +56%, raises outlook amid supply constraints; Scientific & Technical Instruments +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY PROBABLE — most-probable long on YELLOW (size ×0.60); clocks: lookback 🔵 blue — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.48); setup=GREEN; flow=RED; lookback=🔵,Cond green |
| 6 | **MOS** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Agricultural Inputs +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪ |
| 7 | **HPE** | 🟡🟡🟡🟡🟢🟡 | standard | direct normal digest (stale/undated): HPE posts record fiscal Q3 2026, raises FY26â27 outlook and expands Oracle AI data center networking collaboration with equity warrants; Communication Equipment +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.30); setup=GREEN; flow=YELLOW; lookback=🔵,⚪ |
| 8 | **CRM** | 🟡🟡🟡🟡🟢🔴 | probable | direct high digest (stale/undated): Salesforce beats Q2 guidance, raises FY27 outlook as analysts lift price targets after 'narrative-changing' results; Software - Application +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY PROBABLE — most-probable long on YELLOW (size ×0.60); clocks: lookback 🔵 blue — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.48); setup=GREEN; flow=RED; lookback=🔵,Cond green |
| 9 | **DOCU** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Software - Application +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=⚪,Cond green |
| 10 | **MRX** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Capital Markets +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=⚪,Cond green |
| 11 | **CNH** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Farm & Heavy Construction Machinery +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=⚪,Cond green |
| 12 | **WWW** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Footwear & Accessories +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=⚪,Cond green |
| 13 | **MRVL** | 🟡🟡🟡🟡🟢🔴 | probable | direct normal digest (stale/undated): Marvell sharply raises FY27âFY28 outlook on accelerating AI data center and custom demand; Semiconductors +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY PROBABLE — most-probable long on YELLOW (size ×0.60); clocks: lookback 🔵 blue — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.30); setup=GREEN; flow=RED; lookback=🔵,Cond green |
| 14 | **FIVE** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Specialty Retail +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=⚪,Cond green |
| 15 | **BMEA** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Biotechnology +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=⚪,Cond green |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **FCEL** | 🟡🟡🟡🟡🔴🔴 | Electrical Equipment & Parts | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 2 | **DQ** | 🟡🟡🟡🟡🔴🔴 | Solar | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 3 | **REAX** | 🟡🟡🟡🟡🔴🔴 | Real Estate Services | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 4 | **SPIR** | 🟡🟡🟡🟡🔴🔴 | Specialty Business Services | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 5 | **TROX** | 🟡🟡🟡🟡🔴🔴 | Chemicals | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 6 | **FLO** | 🟡🟡🟡🟡🔴🔴 | Packaged Foods | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 7 | **JKS** | 🟡🟡🟡🟡🔴🔴 | Solar | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 8 | **CLX** | 🟡🟡🟡🟡🔴🔴 | Household & Personal Products | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 9 | **BZFD** | 🟡🟡🟡🟡🔴🔴 | Internet Content & Information | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 10 | **IE** | 🟡🟡🟡🟡🔴🔴 | Copper | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 11 | **LODE** | 🟡🟡🟡🟡🔴🔴 | Other Precious Metals & Mining | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 12 | **OPTX** | 🟡🟡🟡🟡🔴🔴 | Electronic Components | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 13 | **DVLT** | 🟡🟡🟡🟡🔴🔴 | Software - Infrastructure | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 14 | **XPOF** | 🟡🟡🟡🟡🔴🔴 | Leisure | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 15 | **AIIO** | 🟡🟡🟡🟡🔴🔴 | Auto Manufacturers | SELL/AVOID — market=YELLOW; red domains=setup,flow |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **none** (0 captains, 0 industries → s_heat).
- Board file: `01_daily/map_heat/2026-09-07_map_heat.json`

_map_heat.json missing — no industry/theme tape today._

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
| Finviz map heat (industry RS / themes) | **missing / not in ranker** | industry residual + theme tape → s_heat when research is gone |
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

### 1. JCI · $87.8B large · Industrials

**1d score +0.431**

**JCI** is a liquid **large-cap** Industrials name (Building Products & Equipment) at $87.8B, ADV ~3912k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.27 | +0.040 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.31 | +0.094 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.46 | +0.141 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.64 | +0.156 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1d total** | | | **+0.431** | |

### 2. ALAB · $53.9B large · Technology

**1d score +0.621**

**ALAB** is a liquid **large-cap** Technology name (Semiconductors) at $53.9B, ADV ~4913k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.62 | +0.091 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.31 | +0.094 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.215 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.58 | +0.141 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1d total** | | | **+0.621** | |

### 3. CRWD · $218.2B mega · Technology

**1d score +0.102**

**CRWD** is a liquid **mega-cap** Technology name (Software - Infrastructure) at $218.2B, ADV ~10602k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.43 | +0.063 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.31 | +0.094 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.36 | +0.109 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.51 | +0.125 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.34 | -0.340 | liquid small/mid, room to run |
| **1d total** | | | **+0.102** | |

### 4. BKR · $63.0B large · Energy

**1d score +0.425**

**BKR** is a liquid **large-cap** Energy name (Oil & Gas Equipment & Services) at $63.0B, ADV ~8219k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.98 | +0.144 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.276 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.02 | +0.005 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1d total** | | | **+0.425** | |

### 5. MOS · $8.2B mid · Basic Materials

**1d score +0.536**

**MOS** is a liquid **mid-cap** Basic Materials name (Agricultural Inputs) at $8.2B, ADV ~8697k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | -0.02 | -0.003 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.55 | +0.169 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.49 | +0.120 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.536** | |

### 6. HPE · $68.9B large · Technology

**1d score +0.231**

**HPE** is a liquid **large-cap** Technology name (Communication Equipment) at $68.9B, ADV ~21711k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.69 | +0.102 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.31 | +0.094 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.12 | +0.038 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | -0.01 | -0.003 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1d total** | | | **+0.231** | |


## 1d AVOID — bottom of the same rank

- **FCEL** (small, Industrials, $1.2B) score -0.412. SELL/AVOID — market=YELLOW; red domains=setup,flow
- **DQ** (small, Technology, $833M) score -0.251. SELL/AVOID — market=YELLOW; red domains=setup,flow
- **REAX** (small, Real Estate, $409M) score -0.234. SELL/AVOID — market=YELLOW; red domains=setup,flow
- **SPIR** (small, Industrials, $469M) score -0.405. SELL/AVOID — market=YELLOW; red domains=setup,flow
- **TROX** (small, Basic Materials, $768M) score -0.361. SELL/AVOID — market=YELLOW; red domains=setup,flow
- **FLO** (small, Consumer Defensive, $1.3B) score -0.255. SELL/AVOID — market=YELLOW; red domains=setup,flow
- **JKS** (small, Technology, $602M) score -0.401. SELL/AVOID — market=YELLOW; red domains=setup,flow
- **CLX** (large, Consumer Defensive, $11.3B) score +0.033. SELL/AVOID — market=YELLOW; red domains=setup,flow

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | CABA | +0.915 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 2 | MRX | +0.901 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | NVAX | +0.888 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | ALT | +0.860 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | UGP | +0.858 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | LPG | +0.857 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | CRK | +0.855 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | ATRC | +0.838 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 9 | MTDR | +0.812 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | LNC | +0.795 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | BFH | +0.793 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | XP | +0.784 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | DUOL | +0.750 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 14 | ARLO | +0.748 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 15 | VNT | +0.745 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 16 | CXW | +0.737 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 17 | FA | +0.732 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 18 | AVT | +0.729 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 19 | WWW | +0.728 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 20 | SBLK | +0.727 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 21 | LEA | +0.713 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 22 | PAR | +0.679 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 23 | MATV | +0.668 | small | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 24 | MHK | +0.659 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 25 | SGHC | +0.650 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | CABA | +0.982 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 2 | MRX | +0.971 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | NVAX | +0.953 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | ALT | +0.925 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | LPG | +0.923 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | CRK | +0.922 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | UGP | +0.921 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | ATRC | +0.902 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 9 | MTDR | +0.878 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | LNC | +0.859 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | BFH | +0.858 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | XP | +0.855 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | DUOL | +0.805 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 14 | VNT | +0.800 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 15 | ARLO | +0.798 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 16 | CXW | +0.789 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 17 | SBLK | +0.787 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 18 | FA | +0.784 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 19 | AVT | +0.783 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 20 | WWW | +0.778 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 21 | LEA | +0.763 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 22 | PAR | +0.726 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 23 | MATV | +0.703 | small | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 24 | MHK | +0.698 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 25 | SGHC | +0.695 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | CABA | +1.029 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 2 | MRX | +1.018 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | NVAX | +0.997 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | ALT | +0.971 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | LPG | +0.969 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | CRK | +0.968 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | UGP | +0.967 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | ATRC | +0.947 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 9 | MTDR | +0.923 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | LNC | +0.904 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | XP | +0.903 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | BFH | +0.902 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | DUOL | +0.838 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 14 | ARLO | +0.837 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 15 | VNT | +0.833 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 16 | SBLK | +0.823 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 17 | FA | +0.821 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 18 | CXW | +0.819 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 19 | AVT | +0.817 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 20 | WWW | +0.807 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 21 | LEA | +0.791 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 22 | PAR | +0.756 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 23 | SGHC | +0.723 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 24 | STGW | +0.719 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 25 | MHK | +0.717 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1m BUY — why these names

### 1. CABA · $588M small · Healthcare

**1m score +1.103**

**CABA** is a liquid **small-cap** Healthcare name (Biotechnology) at $588M, ADV ~4799k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.97 | +0.296 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.85 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.91 | +0.253 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.103** | |

### 2. MRX · $5.6B mid · Financial

**1m score +1.095**

**MRX** is a liquid **mid-cap** Financial name (Capital Markets) at $5.6B, ADV ~830k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.301 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.95 | +0.398 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.71 | +0.196 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.095** | |

### 3. NVAX · $1.7B small · Healthcare

**1m score +1.069**

**NVAX** is a liquid **small-cap** Healthcare name (Biotechnology) at $1.7B, ADV ~4294k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.86 | +0.263 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.93 | +0.386 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.79 | +0.221 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.069** | |

### 4. LPG · $2.4B mid · Energy

**1m score +1.043**

**LPG** is a liquid **mid-cap** Energy name (Oil & Gas Midstream) at $2.4B, ADV ~663k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.301 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.63 | +0.175 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.043** | |

### 5. ALT · $665M small · Healthcare

**1m score +1.042**

**ALT** is a liquid **small-cap** Healthcare name (Biotechnology) at $665M, ADV ~4416k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.96 | +0.294 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.85 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.70 | +0.195 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.042** | |

### 6. CRK · $4.5B mid · Energy

**1m score +1.042**

**CRK** is a liquid **mid-cap** Energy name (Oil & Gas E&P) at $4.5B, ADV ~2582k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.91 | +0.377 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.59 | +0.163 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.042** | |

### 7. UGP · $7.8B mid · Energy

**1m score +1.037**

**UGP** is a liquid **mid-cap** Energy name (Oil & Gas Refining & Marketing) at $7.8B, ADV ~3553k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.98 | +0.300 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.76 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.79 | +0.219 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.037** | |

### 8. ATRC · $2.6B mid · Healthcare

**1m score +1.016**

**ATRC** is a liquid **mid-cap** Healthcare name (Medical Instruments & Supplies) at $2.6B, ADV ~898k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.96 | +0.293 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.81 | +0.337 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.67 | +0.187 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.016** | |

### 9. MTDR · $7.3B mid · Energy

**1m score +0.996**

**MTDR** is a liquid **mid-cap** Energy name (Oil & Gas E&P) at $7.3B, ADV ~1932k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.91 | +0.377 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.42 | +0.117 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.996** | |

### 10. XP · $10.3B large · Financial

**1m score +0.982**

**XP** is a liquid **large-cap** Financial name (Capital Markets) at $10.3B, ADV ~5328k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **extended**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.97 | +0.297 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.97 | +0.405 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.90 | +0.250 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1m total** | | | **+0.982** | |

### 11. LNC · $8.7B mid · Financial

**1m score +0.975**

**LNC** is a liquid **mid-cap** Financial name (Insurance - Life) at $8.7B, ADV ~1908k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.97 | +0.298 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.40 | +0.110 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.975** | |

### 12. BFH · $4.3B mid · Financial

**1m score +0.974**

**BFH** is a liquid **mid-cap** Financial name (Credit Services) at $4.3B, ADV ~615k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.38 | +0.106 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.974** | |

### 13. DUOL · $7.2B mid · Technology

**1m score +0.898**

**DUOL** is a liquid **mid-cap** Technology name (Software - Application) at $7.2B, ADV ~1309k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.62 | +0.190 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.91 | +0.377 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.47 | +0.131 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.898** | |

### 14. VNT · $4.5B mid · Technology

**1m score +0.894**

**VNT** is a liquid **mid-cap** Technology name (Scientific & Technical Instruments) at $4.5B, ADV ~1591k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.62 | +0.190 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.94 | +0.392 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.40 | +0.112 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.894** | |

### 15. ARLO · $1.5B small · Industrials

**1m score +0.892**

**ARLO** is a liquid **small-cap** Industrials name (Building Products & Equipment) at $1.5B, ADV ~1249k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.86 | +0.264 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.46 | +0.193 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.85 | +0.235 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.892** | |

### 16. SBLK · $3.6B mid · Industrials

**1m score +0.888**

**SBLK** is a liquid **mid-cap** Industrials name (Marine Shipping) at $3.6B, ADV ~1359k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.75 | +0.229 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.98 | +0.408 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.19 | +0.052 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.888** | |

### 17. FA · $3.7B mid · Industrials

**1m score +0.879**

**FA** is a liquid **mid-cap** Industrials name (Specialty Business Services) at $3.7B, ADV ~2155k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.76 | +0.233 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.70 | +0.293 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.55 | +0.153 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.879** | |

### 18. AVT · $7.6B mid · Technology

**1m score +0.876**

**AVT** is a liquid **mid-cap** Technology name (Electronics & Computer Distribution) at $7.6B, ADV ~1253k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.68 | +0.207 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.85 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.42 | +0.116 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.876** | |

### 19. CXW · $3.4B mid · Industrials

**1m score +0.876**

**CXW** is a liquid **mid-cap** Industrials name (Security & Protection Services) at $3.4B, ADV ~1463k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.51 | +0.157 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.93 | +0.386 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.48 | +0.133 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.876** | |

### 20. WWW · $1.7B small · Consumer Cyclical

**1m score +0.863**

**WWW** is a liquid **small-cap** Consumer Cyclical name (Footwear & Accessories) at $1.7B, ADV ~856k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.49 | +0.150 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.91 | +0.377 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.49 | +0.136 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.863** | |

### 21. LEA · $6.6B mid · Consumer Cyclical

**1m score +0.846**

**LEA** is a liquid **mid-cap** Consumer Cyclical name (Auto Parts) at $6.6B, ADV ~646k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.49 | +0.150 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.47 | +0.129 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.846** | |

### 22. PAR · $818M small · Technology

**1m score +0.809**

**PAR** is a liquid **small-cap** Technology name (Software - Application) at $818M, ADV ~1026k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.57 | +0.175 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.76 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.42 | +0.117 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.809** | |

### 23. SGHC · $7.1B mid · Consumer Cyclical

**1m score +0.772**

**SGHC** is a liquid **mid-cap** Consumer Cyclical name (Gambling) at $7.1B, ADV ~2857k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.55 | +0.168 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.70 | +0.293 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.40 | +0.111 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.772** | |

### 24. STGW · $2.1B mid · Communication Services

**1m score +0.769**

**STGW** is a liquid **mid-cap** Communication Services name (Advertising Agencies) at $2.1B, ADV ~1381k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.45 | +0.139 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.85 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.28 | +0.077 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.769** | |

### 25. ANF · $6.6B mid · Consumer Cyclical

**1m score +0.768**

**ANF** is a liquid **mid-cap** Consumer Cyclical name (Apparel Retail) at $6.6B, ADV ~1516k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.76 | +0.231 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | -0.11 | -0.030 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.768** | |


## 1m AVOID — bottom of the same rank

- **LODE** (micro, Basic Materials, $236M) score -0.724. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SPIR** (small, Industrials, $469M) score -0.679. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FCEL** (small, Industrials, $1.2B) score -0.671. this name **lagged its own correlated peers** this week; the peer basket itself was **up**
- **JKS** (small, Technology, $602M) score -0.654. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **APLD** (mid, Technology, $7.7B) score -0.650. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **IBM** (mega, Technology, $221.3B) score -0.629. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **RGP** (micro, Industrials, $139M) score -0.629. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RZLV** (small, Technology, $881M) score -0.628. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BZFD** (micro, Communication Services, $91M) score -0.624. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OPEN** (mid, Real Estate, $3.1B) score -0.620. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **DVLT** (micro, Technology, $194M) score -0.618. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **TROX** (small, Basic Materials, $768M) score -0.611. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**
- **FLNC** (small, Utilities, $1.5B) score -0.609. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **BGS** (micro, Consumer Defensive, $267M) score -0.609. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CPNG** (large, Consumer Cyclical, $27.5B) score -0.600. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **UNFI** (mid, Consumer Defensive, $2.7B) score -0.598. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **EVTL** (micro, Industrials, $103M) score -0.592. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GETY** (micro, Communication Services, $100M) score -0.584. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **COLD** (mid, Real Estate, $4.1B) score -0.579. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CAN** (micro, Technology, $253M) score -0.573. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **XHR** (small, Real Estate, $1.7B) score -0.572. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LAES** (small, Technology, $557M) score -0.569. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **GCTS** (micro, Technology, $163M) score -0.566. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **SRFM** (micro, Industrials, $83M) score -0.564. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **ZVIA** (micro, Consumer Defensive, $99M) score -0.557. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-09-07_stock_book.md`
- Machine table: `data/stock_book/2026-09-07_stock_book.csv`
- Machine book: `data/stock_book/2026-09-07_stock_book.json`
- Join rank: `data/join/2026-09-07_ranked.csv`
- Weather: `01_daily/weather/2026-09-07_weather.md`
- AB enrich: `data/ab_checklist/2026-09-07_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-09-07_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-09-07_map_heat.md`
