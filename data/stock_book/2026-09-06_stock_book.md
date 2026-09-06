# Stock book — 2026-09-06

_Generated 2026-09-06T14:37:43.974237-04:00_

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
- Stand-down: **no** — 267 names qualified through standard,group_leader,catalyst (59 probable)
- Sector predicts this date: 0/11 (missing → sector layer is 0; Finviz week tape still sits in join)
- News tickers in play: 2
- AB coverage: 1970 names · peer RS: 1851
- Universe after liquidity: 2088
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 0

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
| 1 | **WWW** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Footwear & Accessories +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 2 | **FIVE** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Specialty Retail +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 3 | **CMBT** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Oil & Gas Midstream +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 4 | **ORCL** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Software - Infrastructure +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪ |
| 5 | **VYX** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Information Technology Services +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪ |
| 6 | **WCC** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Industrial Distribution +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪ |
| 7 | **KNX** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Trucking +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪ |
| 8 | **MHK** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Furnishings, Fixtures & Appliances +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪ |
| 9 | **JCI** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Building Products & Equipment +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪ |
| 10 | **BBY** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Specialty Retail +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪ |
| 11 | **FA** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Specialty Business Services +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪ |
| 12 | **ETN** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Specialty Industrial Machinery +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪ |
| 13 | **EAT** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Restaurants +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪ |
| 14 | **HPQ** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Computer Hardware +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪ |
| 15 | **TH** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Specialty Business Services +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪ |

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
- Board file: `01_daily/map_heat/2026-09-06_map_heat.json`

_map_heat.json missing — no industry/theme tape today._

## Inputs this run — every resource

If a row says **missing**, that layer scored 0 today. If it says **found**, it moved the rank.

| Resource | This run | Where it lands in the score |
|----------|----------|-----------------------------|
| Finviz Elite export | **found** | liquidity + labels + AB proxy + digest |
| Labels / membership | **found** | join + mid_opp + earnings/range |
| Weather (tape + FRED/DXY/VIX) | **found** | join × weather |
| Channel 1 raw | **missing / not in ranker** | via weather |
| Join ranked universe | **found** | s_join |
| News parse + actions | **missing / not in ranker** | s_news |
| News judge | **found** | s_news ticker tilts |
| Finviz daily digest | **missing / not in ranker** | s_news company headlines |
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

### 1. WWW · $1.7B small · Consumer Cyclical

**1d score +0.717**

**WWW** is a liquid **small-cap** Consumer Cyclical name (Footwear & Accessories) at $1.7B, ADV ~856k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.49 | +0.072 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.276 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.49 | +0.119 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.717** | |

### 2. FIVE · $13.9B large · Consumer Cyclical

**1d score +0.299**

**FIVE** is a liquid **large-cap** Consumer Cyclical name (Specialty Retail) at $13.9B, ADV ~1236k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.81 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.247 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.22 | +0.054 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.299** | |

### 3. CMBT · $5.7B mid · Energy

**1d score +0.666**

**CMBT** is a liquid **mid-cap** Energy name (Oil & Gas Midstream) at $5.7B, ADV ~1060k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.99 | +0.144 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.247 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.10 | +0.025 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.666** | |

### 4. ORCL · $457.4B mega · Technology

**1d score +0.498**

**ORCL** is a liquid **mega-cap** Technology name (Software - Infrastructure) at $457.4B, ADV ~31387k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.61 | +0.090 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.232 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.76 | +0.186 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.06 | -0.060 | liquid small/mid, room to run |
| **1d total** | | | **+0.498** | |

### 5. VYX · $1.3B small · Technology

**1d score +0.675**

**VYX** is a liquid **small-cap** Technology name (Information Technology Services) at $1.3B, ADV ~2407k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.18 | +0.027 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.247 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.62 | +0.151 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.675** | |

### 6. WCC · $17.1B large · Industrials

**1d score +0.347**

**WCC** is a liquid **large-cap** Industrials name (Industrial Distribution) at $17.1B, ADV ~629k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.54 | +0.079 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.247 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.58 | +0.142 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.347** | |

### 7. KNX · $11.7B large · Industrials

**1d score +0.460**

**KNX** is a liquid **large-cap** Industrials name (Trucking) at $11.7B, ADV ~3846k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.27 | +0.040 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.93 | +0.282 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.57 | +0.138 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1d total** | | | **+0.460** | |

### 8. MHK · $8.1B mid · Consumer Cyclical

**1d score +0.616**

**MHK** is a liquid **mid-cap** Consumer Cyclical name (Furnishings, Fixtures & Appliances) at $8.1B, ADV ~863k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.21 | +0.031 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.247 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.57 | +0.138 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.616** | |


## 1d AVOID — bottom of the same rank

- **FCEL** (small, Industrials, $1.2B) score -0.423. SELL/AVOID — market=YELLOW; red domains=setup,flow
- **DQ** (small, Technology, $833M) score -0.252. SELL/AVOID — market=YELLOW; red domains=setup,flow
- **REAX** (small, Real Estate, $409M) score -0.184. SELL/AVOID — market=YELLOW; red domains=setup,flow
- **SPIR** (small, Industrials, $487M) score -0.397. SELL/AVOID — market=YELLOW; red domains=setup,flow
- **TROX** (small, Basic Materials, $768M) score -0.361. SELL/AVOID — market=YELLOW; red domains=setup,flow
- **FLO** (small, Consumer Defensive, $1.3B) score -0.255. SELL/AVOID — market=YELLOW; red domains=setup,flow
- **JKS** (small, Technology, $602M) score -0.401. SELL/AVOID — market=YELLOW; red domains=setup,flow
- **CLX** (large, Consumer Defensive, $11.3B) score +0.033. SELL/AVOID — market=YELLOW; red domains=setup,flow

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | ALT | +0.860 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | CRK | +0.836 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | GPRK | +0.793 | small | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | FA | +0.782 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | WWW | +0.778 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 6 | CXW | +0.772 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 7 | CDNA | +0.770 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 8 | AVT | +0.766 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | ARLO | +0.764 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | LEA | +0.763 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | WT | +0.757 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 12 | CMBT | +0.748 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | CLYM | +0.746 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 14 | VNT | +0.745 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 15 | PACS | +0.730 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 16 | SBLK | +0.727 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 17 | INSW | +0.722 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 18 | MATV | +0.719 | small | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 19 | VYX | +0.716 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 20 | GDDY | +0.712 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 21 | EAT | +0.692 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 22 | ANF | +0.671 | mid | Consumer Cyclical | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 23 | ABR | +0.635 | small | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 24 | GSM | +0.615 | small | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 25 | ALM | +0.607 | mid | Basic Materials | the Finviz industry was **down** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | ALT | +0.925 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | CRK | +0.901 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | GPRK | +0.843 | small | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | CDNA | +0.836 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 5 | FA | +0.835 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 6 | WWW | +0.829 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 7 | CXW | +0.823 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 8 | WT | +0.821 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 9 | AVT | +0.819 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | LEA | +0.813 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | ARLO | +0.810 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 12 | CMBT | +0.808 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | CLYM | +0.802 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 14 | VNT | +0.800 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 15 | PACS | +0.792 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 16 | SBLK | +0.787 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 17 | INSW | +0.786 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 18 | GDDY | +0.769 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 19 | PAR | +0.755 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 20 | MATV | +0.753 | small | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 21 | EAT | +0.729 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 22 | ANF | +0.725 | mid | Consumer Cyclical | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 23 | ABR | +0.667 | small | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 24 | ALM | +0.654 | mid | Basic Materials | the Finviz industry was **down** |
| 25 | GSM | +0.633 | small | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | ALT | +0.971 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | CRK | +0.947 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | CDNA | +0.880 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 4 | GPRK | +0.878 | small | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | FA | +0.871 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 6 | WT | +0.865 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 7 | WWW | +0.858 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 8 | AVT | +0.852 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | CXW | +0.852 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | CMBT | +0.850 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | ARLO | +0.848 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 12 | CLYM | +0.844 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | LEA | +0.842 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 14 | VNT | +0.834 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 15 | PACS | +0.834 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 16 | INSW | +0.829 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 17 | SBLK | +0.823 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 18 | GDDY | +0.806 | large | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 19 | PAR | +0.785 | small | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 20 | MATV | +0.764 | small | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 21 | ANF | +0.758 | mid | Consumer Cyclical | this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 22 | EAT | +0.748 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 23 | ALM | +0.687 | mid | Basic Materials | the Finviz industry was **down** |
| 24 | ABR | +0.674 | small | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 25 | QNST | +0.653 | small | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1m BUY — why these names

### 1. ALT · $665M small · Healthcare

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

### 2. CRK · $4.5B mid · Energy

**1m score +1.018**

**CRK** is a liquid **mid-cap** Energy name (Oil & Gas E&P) at $4.5B, ADV ~2582k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.85 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.59 | +0.163 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.018** | |

### 3. CDNA · $2.6B mid · Healthcare

**1m score +0.953**

**CDNA** is a liquid **mid-cap** Healthcare name (Diagnostics & Research) at $2.6B, ADV ~1121k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.95 | +0.398 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.19 | +0.054 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.953** | |

### 4. WT · $3.8B mid · Financial

**1m score +0.935**

**WT** is a liquid **mid-cap** Financial name (Asset Management) at $3.8B, ADV ~2819k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.301 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.24 | +0.067 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.935** | |

### 5. GPRK · $750M small · Energy

**1m score +0.932**

**GPRK** is a liquid **small-cap** Energy name (Oil & Gas E&P) at $750M, ADV ~526k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.65 | +0.199 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.64 | +0.265 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.97 | +0.268 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.932** | |

### 6. FA · $3.7B mid · Industrials

**1m score +0.929**

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
| **1m total** | | | **+0.929** | |

### 7. CMBT · $5.7B mid · Energy

**1m score +0.917**

**CMBT** is a liquid **mid-cap** Energy name (Oil & Gas Midstream) at $5.7B, ADV ~1060k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extended**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.81 | +0.337 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.10 | +0.028 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.917** | |

### 8. WWW · $1.7B small · Consumer Cyclical

**1m score +0.913**

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
| **1m total** | | | **+0.913** | |

### 9. AVT · $7.6B mid · Technology

**1m score +0.910**

**AVT** is a liquid **mid-cap** Technology name (Electronics & Computer Distribution) at $7.6B, ADV ~1253k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.68 | +0.207 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.81 | +0.337 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.42 | +0.116 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.910** | |

### 10. CXW · $3.4B mid · Industrials

**1m score +0.908**

**CXW** is a liquid **mid-cap** Industrials name (Security & Protection Services) at $3.4B, ADV ~1463k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.52 | +0.158 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.48 | +0.133 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.908** | |

### 11. CLYM · $891M small · Healthcare

**1m score +0.906**

**CLYM** is a liquid **small-cap** Healthcare name (Biotechnology) at $891M, ADV ~1428k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.95 | +0.289 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.64 | +0.265 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.55 | +0.152 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.906** | |

### 12. PACS · $6.7B mid · Healthcare

**1m score +0.902**

**PACS** is a liquid **mid-cap** Healthcare name (Medical Care Facilities) at $6.7B, ADV ~873k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.94 | +0.287 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.91 | +0.377 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.14 | +0.038 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.902** | |

### 13. INSW · $5.2B mid · Energy

**1m score +0.899**

**INSW** is a liquid **mid-cap** Energy name (Oil & Gas Midstream) at $5.2B, ADV ~559k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.301 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.93 | +0.386 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.04 | +0.012 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.899** | |

### 14. ARLO · $1.5B small · Industrials

**1m score +0.899**

**ARLO** is a liquid **small-cap** Industrials name (Building Products & Equipment) at $1.5B, ADV ~1249k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.86 | +0.264 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.36 | +0.149 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.85 | +0.235 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.899** | |

### 15. LEA · $6.6B mid · Consumer Cyclical

**1m score +0.896**

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
| **1m total** | | | **+0.896** | |

### 16. VNT · $4.5B mid · Technology

**1m score +0.895**

**VNT** is a liquid **mid-cap** Technology name (Scientific & Technical Instruments) at $4.5B, ADV ~1591k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.62 | +0.191 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.94 | +0.392 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.40 | +0.112 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.895** | |

### 17. SBLK · $3.6B mid · Industrials

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

### 18. GDDY · $12.8B large · Technology

**1m score +0.868**

**GDDY** is a liquid **large-cap** Technology name (Software - Infrastructure) at $12.8B, ADV ~2197k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.71 | +0.218 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.85 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.64 | +0.177 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.07 | +0.070 | liquid small/mid, room to run |
| **1m total** | | | **+0.868** | |

### 19. PAR · $818M small · Technology

**1m score +0.835**

**PAR** is a liquid **small-cap** Technology name (Software - Application) at $818M, ADV ~1026k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.57 | +0.175 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.70 | +0.293 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.42 | +0.117 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.835** | |

### 20. ANF · $6.7B mid · Consumer Cyclical

**1m score +0.818**

**ANF** is a liquid **mid-cap** Consumer Cyclical name (Apparel Retail) at $6.7B, ADV ~1516k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

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
| **1m total** | | | **+0.818** | |

### 21. MATV · $699M small · Basic Materials

**1m score +0.801**

**MATV** is a liquid **small-cap** Basic Materials name (Specialty Chemicals) at $699M, ADV ~570k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | -0.08 | -0.024 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.91 | +0.377 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.71 | +0.198 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.801** | |

### 22. EAT · $9.6B mid · Consumer Cyclical

**1m score +0.789**

**EAT** is a liquid **mid-cap** Consumer Cyclical name (Restaurants) at $9.6B, ADV ~1013k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.24 | +0.075 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.70 | +0.293 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.61 | +0.171 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.789** | |

### 23. ALM · $5.1B mid · Basic Materials

**1m score +0.738**

**ALM** is a liquid **mid-cap** Basic Materials name (Other Industrial Metals & Mining) at $5.1B, ADV ~6580k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.84 | +0.257 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.20 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.55 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.738** | |

### 24. ABR · $979M small · Real Estate

**1m score +0.709**

**ABR** is a liquid **small-cap** Real Estate name (REIT - Mortgage) at $979M, ADV ~3890k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | -0.15 | -0.046 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.98 | +0.408 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.35 | +0.097 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.709** | |

### 25. QNST · $1.1B small · Communication Services

**1m score +0.703**

**QNST** is a liquid **small-cap** Communication Services name (Advertising Agencies) at $1.1B, ADV ~845k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.70 | +0.213 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.64 | +0.265 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.09 | +0.025 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.703** | |


## 1m AVOID — bottom of the same rank

- **LODE** (micro, Basic Materials, $236M) score -0.724. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FCEL** (small, Industrials, $1.2B) score -0.687. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **SPIR** (small, Industrials, $487M) score -0.668. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **JKS** (small, Technology, $602M) score -0.654. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **APLD** (mid, Technology, $7.7B) score -0.650. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **IBM** (mega, Technology, $221.3B) score -0.629. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **RGP** (micro, Industrials, $139M) score -0.629. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RZLV** (small, Technology, $881M) score -0.628. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BZFD** (micro, Communication Services, $91M) score -0.624. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OPEN** (mid, Real Estate, $3.1B) score -0.620. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TROX** (small, Basic Materials, $768M) score -0.611. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**
- **CPNG** (large, Consumer Cyclical, $27.5B) score -0.600. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FLNC** (small, Utilities, $1.9B) score -0.592. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **GETY** (micro, Communication Services, $100M) score -0.584. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **COLD** (mid, Real Estate, $4.1B) score -0.579. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CAN** (micro, Technology, $253M) score -0.573. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **XHR** (small, Real Estate, $1.7B) score -0.572. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LAES** (small, Technology, $557M) score -0.569. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **DVLT** (micro, Technology, $194M) score -0.568. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **GCTS** (micro, Technology, $163M) score -0.566. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **SRFM** (micro, Industrials, $83M) score -0.564. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **BGS** (micro, Consumer Defensive, $267M) score -0.559. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **QH** (small, Technology, $418M) score -0.551. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **UNFI** (mid, Consumer Defensive, $2.7B) score -0.548. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **NUS** (micro, Consumer Defensive, $236M) score -0.547. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-09-06_stock_book.md`
- Machine table: `data/stock_book/2026-09-06_stock_book.csv`
- Machine book: `data/stock_book/2026-09-06_stock_book.json`
- Join rank: `data/join/2026-09-06_ranked.csv`
- Weather: `01_daily/weather/2026-09-06_weather.md`
- AB enrich: `data/ab_checklist/2026-09-06_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-09-06_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-09-06_map_heat.md`
