# Stock book — 2026-09-27

_Generated 2026-09-27T15:59:50.926370-04:00_

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
- Stand-down: **no** — 473 names qualified through standard,group_leader,catalyst (147 probable)
- Sector predicts this date: 0/11 (missing → sector layer is 0; Finviz week tape still sits in join)
- News tickers in play: 50
- AB coverage: 1960 names · peer RS: 1839
- Universe after liquidity: 2080
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 31

## All-green BUY / SELL

- Mode: **weighted_fallback** · SELL **core_weights**
- Pile: **0** liquid all-green names (need ≥ 8) of 2080
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
| 1 | **OPRT** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Credit Services +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 2 | **CRDO** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Semiconductors +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 3 | **VIAV** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Communication Equipment +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 4 | **AAON** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Building Products & Equipment +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 5 | **QMCO** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Computer Hardware +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 6 | **C** | 🟡🟡🟡🟡🟢🟢 | standard | basket/action net=+1.78; context only, not a company catalyst; Banks - Diversified +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.12); setup=GREEN; flow=GREEN; lookback=🔵,⚪ |
| 7 | **RSKD** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Software - Application +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 8 | **FIGS** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Apparel Manufacturing +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪,Cond green |
| 9 | **AXTI** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Semiconductor Equipment & Materials +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪ |
| 10 | **ALAB** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Semiconductors +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪ |
| 11 | **VICR** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Electronic Components +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪ |
| 12 | **SMCI** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Computer Hardware +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪ |
| 13 | **CRCT** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Computer Hardware +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪ |
| 14 | **ARHS** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Specialty Retail +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪ |
| 15 | **SBH** | 🟡🟡🟡🟡🟢🟢 | standard | no direct company event; Specialty Retail +0.0% d1 / +0.0% 1w / +0.0% vs parent | BUY STANDARD — market=YELLOW; parent=YELLOW; child=YELLOW/rel=YELLOW; company=YELLOW(0.00); setup=GREEN; flow=GREEN; lookback=🔵,⚪ |

### Bear decisions

| # | Ticker | Domains | Industry | Decision |
|---:|--------|---------|----------|----------|
| 1 | **SOC** | 🟡🟡🟡🟡🔴🔴 | Oil & Gas Drilling | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 2 | **TYRA** | 🟡🟡🟡🟡🔴🔴 | Biotechnology | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 3 | **ACDC** | 🟡🟡🟡🟡🔴🔴 | Oil & Gas Equipment & Services | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 4 | **WVE** | 🟡🟡🟡🟡🔴🔴 | Biotechnology | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 5 | **AVTX** | 🟡🟡🟡🟡🔴🔴 | Biotechnology | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 6 | **ALMS** | 🟡🟡🟡🟡🔴🔴 | Biotechnology | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 7 | **AIRS** | 🟡🟡🟡🟡🔴🔴 | Medical Care Facilities | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 8 | **PROK** | 🟡🟡🟡🟡🔴🔴 | Biotechnology | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 9 | **PRCT** | 🟡🟡🟡🟡🔴🔴 | Medical Devices | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 10 | **LXRX** | 🟡🟡🟡🟡🔴🔴 | Biotechnology | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 11 | **IMNM** | 🟡🟡🟡🟡🔴🔴 | Biotechnology | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 12 | **KGS** | 🟡🟡🟡🟡🔴🔴 | Oil & Gas Equipment & Services | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 13 | **CGEM** | 🟡🟡🟡🟡🔴🔴 | Biotechnology | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 14 | **DNTH** | 🟡🟡🟡🟡🔴🔴 | Biotechnology | SELL/AVOID — market=YELLOW; red domains=setup,flow |
| 15 | **OLMA** | 🟡🟡🟡🟡🔴🔴 | Biotechnology | SELL/AVOID — market=YELLOW; red domains=setup,flow |

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **none** (0 captains, 0 industries → s_heat).
- Board file: `01_daily/map_heat/2026-09-27_map_heat.json`

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
| News parse + actions | **found** | s_news |
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
| general | 51% | 39 | ×0.85 |
| sector:Basic Materials | 43% | 28 | ×0.50 |
| sector:Communication Services | 22% | 27 | ×0.50 |
| sector:Consumer Cyclical | 46% | 28 | ×0.85 |
| sector:Consumer Defensive | 48% | 27 | ×0.85 |
| sector:Energy | 57% | 28 | ×1.00 |
| sector:Financial | 46% | 28 | ×0.85 |
| sector:Healthcare | 48% | 25 | ×0.85 |
| sector:Industrials | 36% | 28 | ×0.50 |
| sector:Real Estate | 54% | 28 | ×0.85 |
| sector:Technology | 35% | 26 | ×0.50 |
| sector:Utilities | 42% | 26 | ×0.50 |

## Horizon weights — book_policy.json v15 · renormalized (absent: sector, general)

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.15 | 0.00 | 0.00 | 0.30 | 0.30 | 0.24 | additive |
| 3d | 0.21 | 0.00 | 0.00 | 0.21 | 0.33 | 0.26 | additive |
| 1w | 0.24 | 0.00 | 0.00 | 0.13 | 0.37 | 0.26 | additive |
| 2w | 0.27 | 0.00 | 0.00 | 0.08 | 0.38 | 0.27 | additive |
| 1m | 0.31 | 0.00 | 0.00 | 0.00 | 0.42 | 0.28 | additive |

## 1d BUY — why these names

### 1. CRDO · $39.7B large · Technology

**1d score +0.751**

**CRDO** is a liquid **large-cap** Technology name (Semiconductors) at $39.7B, ADV ~7313k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.99 | +0.144 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.94 | +0.287 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.98 | +0.239 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1d total** | | | **+0.751** | |

### 2. OPRT · $393M small · Financial

**1d score +0.661**

**OPRT** is a liquid **small-cap** Financial name (Credit Services) at $393M, ADV ~516k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.05 | +0.008 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.215 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.77 | +0.189 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.661** | |

### 3. VIAV · $10.0B large · Technology

**1d score +0.724**

**VIAV** is a liquid **large-cap** Technology name (Communication Equipment) at $10.0B, ADV ~5364k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.98 | +0.144 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.268 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.95 | +0.233 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1d total** | | | **+0.724** | |

### 4. AAON · $7.3B mid · Industrials

**1d score +0.848**

**AAON** is a liquid **mid-cap** Industrials name (Building Products & Equipment) at $7.3B, ADV ~1147k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.84 | +0.122 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.259 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.89 | +0.217 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.848** | |

### 5. QMCO · $1.2B small · Technology

**1d score +0.848**

**QMCO** is a liquid **small-cap** Technology name (Computer Hardware) at $1.2B, ADV ~1556k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **extreme**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.93 | +0.137 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.259 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +1.00 | +0.243 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.16 | +0.160 | liquid small/mid, room to run |
| **1d total** | | | **+0.848** | |

### 6. C · $225.2B mega · Financial

**1d score +0.362**

**C** is a liquid **mega-cap** Financial name (Banks - Diversified) at $225.2B, ADV ~10801k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.72 | +0.106 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.34 | +0.104 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.215 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.44 | +0.108 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.22 | -0.220 | liquid small/mid, room to run |
| **1d total** | | | **+0.362** | |

### 7. RSKD · $1.0B small · Technology

**1d score +0.831**

**RSKD** is a liquid **small-cap** Technology name (Software - Application) at $1.0B, ADV ~1576k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.98 | +0.144 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.64 | +0.194 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +1.00 | +0.243 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.831** | |

### 8. FIGS · $2.2B mid · Consumer Cyclical

**1d score +0.692**

**FIGS** is a liquid **mid-cap** Consumer Cyclical name (Apparel Manufacturing) at $2.2B, ADV ~2974k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.88 | +0.129 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.98 | +0.300 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.05 | +0.013 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.692** | |


## 1d AVOID — bottom of the same rank

- **SOC** (small, Energy, $744M) score -0.390. SELL/AVOID — market=YELLOW; red domains=setup,flow
- **TYRA** (small, Healthcare, $1.5B) score -0.385. SELL/AVOID — market=YELLOW; red domains=setup,flow
- **ACDC** (small, Energy, $839M) score -0.383. SELL/AVOID — market=YELLOW; red domains=setup,flow
- **WVE** (small, Healthcare, $745M) score -0.413. SELL/AVOID — market=YELLOW; red domains=setup,flow
- **AVTX** (small, Healthcare, $830M) score -0.221. SELL/AVOID — market=YELLOW; red domains=setup,flow
- **ALMS** (small, Healthcare, $948M) score -0.407. SELL/AVOID — market=YELLOW; red domains=setup,flow
- **AIRS** (micro, Healthcare, $141M) score -0.465. SELL/AVOID — market=YELLOW; red domains=setup,flow
- **PROK** (small, Healthcare, $484M) score -0.428. SELL/AVOID — market=YELLOW; red domains=setup,flow

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | OUST | +0.990 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | CCC | +0.974 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | ARHS | +0.973 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | SONO | +0.971 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 5 | VECO | +0.956 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | AAON | +0.932 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | SHOO | +0.905 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | HNI | +0.856 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 9 | CBRL | +0.839 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 10 | LAUR | +0.825 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 11 | SWBI | +0.785 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 12 | VCTR | +0.782 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 13 | GTES | +0.779 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 14 | RRC | +0.737 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | WMS | +0.723 | large | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 16 | SPNT | +0.722 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 17 | WRBY | +0.711 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 18 | PURR | +0.698 | mid | Financial | the Finviz industry was **down** |
| 19 | JXN | +0.683 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 20 | CRML | +0.604 | small | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 21 | ETON | +0.584 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 22 | MKC | +0.584 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 23 | INSW | +0.573 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 24 | PBF | +0.570 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 25 | AVNT | +0.565 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | OUST | +1.058 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | CCC | +1.043 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | SONO | +1.042 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 4 | ARHS | +1.036 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | VECO | +1.026 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | AAON | +0.995 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | SHOO | +0.970 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | HNI | +0.918 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 9 | LAUR | +0.886 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | CBRL | +0.884 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 11 | VCTR | +0.843 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 12 | GTES | +0.840 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | SWBI | +0.833 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | WMS | +0.779 | large | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 15 | SPNT | +0.773 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 16 | PURR | +0.755 | mid | Financial | the Finviz industry was **down** |
| 17 | WRBY | +0.744 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 18 | JXN | +0.732 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 19 | RRC | +0.696 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 20 | MKC | +0.631 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 21 | CRML | +0.626 | small | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 22 | ETON | +0.615 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 23 | INSW | +0.603 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 24 | PBF | +0.598 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 25 | AVNT | +0.595 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | OUST | +1.107 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | SONO | +1.092 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 3 | CCC | +1.091 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | ARHS | +1.077 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | VECO | +1.073 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | AAON | +1.038 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | SHOO | +1.014 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | HNI | +0.960 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 9 | LAUR | +0.928 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | SBH | +0.918 | small | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | VCTR | +0.883 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 12 | GTES | +0.881 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | SWBI | +0.864 | small | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | WMS | +0.812 | large | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 15 | SPNT | +0.810 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 16 | PURR | +0.795 | mid | Financial | the Finviz industry was **down** |
| 17 | JXN | +0.769 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 18 | WRBY | +0.755 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 19 | MKC | +0.662 | large | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 20 | RRC | +0.656 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 21 | CRML | +0.633 | small | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 22 | ETON | +0.623 | small | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 23 | INSW | +0.612 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 24 | PBF | +0.603 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 25 | AVNT | +0.600 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 1m BUY — why these names

### 1. OUST · $3.2B mid · Technology

**1m score +1.181**

**OUST** is a liquid **mid-cap** Technology name (Electronic Components) at $3.2B, ADV ~3318k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.85 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.99 | +0.276 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.181** | |

### 2. SONO · $2.1B mid · Technology

**1m score +1.170**

**SONO** is a liquid **mid-cap** Technology name (Consumer Electronics) at $2.1B, ADV ~1973k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.95 | +0.398 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.98 | +0.271 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.170** | |

### 3. CCC · $4.1B mid · Technology

**1m score +1.168**

**CCC** is a liquid **mid-cap** Technology name (Software - Application) at $4.1B, ADV ~10787k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.93 | +0.386 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.83 | +0.231 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.168** | |

### 4. VECO · $3.0B mid · Technology

**1m score +1.150**

**VECO** is a liquid **mid-cap** Technology name (Semiconductor Equipment & Materials) at $3.0B, ADV ~909k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.98 | +0.301 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.96 | +0.402 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.71 | +0.198 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.150** | |

### 5. ARHS · $1.4B small · Consumer Cyclical

**1m score +1.147**

**ARHS** is a liquid **small-cap** Consumer Cyclical name (Specialty Retail) at $1.4B, ADV ~1354k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.73 | +0.224 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.95 | +0.398 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.99 | +0.276 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.147** | |

### 6. AAON · $7.3B mid · Industrials

**1m score +1.106**

**AAON** is a liquid **mid-cap** Industrials name (Building Products & Equipment) at $7.3B, ADV ~1147k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.84 | +0.256 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.85 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.89 | +0.247 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.106** | |

### 7. SHOO · $3.3B mid · Consumer Cyclical

**1m score +1.086**

**SHOO** is a liquid **mid-cap** Consumer Cyclical name (Footwear & Accessories) at $3.3B, ADV ~1097k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.91 | +0.277 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.91 | +0.377 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.65 | +0.182 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.086** | |

### 8. HNI · $3.4B mid · Consumer Cyclical

**1m score +1.029**

**HNI** is a liquid **mid-cap** Consumer Cyclical name (Furnishings, Fixtures & Appliances) at $3.4B, ADV ~621k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.86 | +0.261 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.93 | +0.386 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.47 | +0.132 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+1.029** | |

### 9. LAUR · $5.2B mid · Consumer Defensive

**1m score +0.995**

**LAUR** is a liquid **mid-cap** Consumer Defensive name (Education & Training Services) at $5.2B, ADV ~1048k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.86 | +0.261 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.85 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.65 | +0.180 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.995** | |

### 10. SBH · $1.6B small · Consumer Cyclical

**1m score +0.986**

**SBH** is a liquid **small-cap** Consumer Cyclical name (Specialty Retail) at $1.6B, ADV ~1349k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.79 | +0.242 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.93 | +0.386 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.68 | +0.188 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.12 | +0.120 | liquid small/mid, room to run |
| **1m total** | | | **+0.986** | |

### 11. VCTR · $6.7B mid · Financial

**1m score +0.950**

**VCTR** is a liquid **mid-cap** Financial name (Asset Management) at $6.7B, ADV ~624k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.89 | +0.273 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.22 | +0.060 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.950** | |

### 12. GTES · $6.8B mid · Industrials

**1m score +0.948**

**GTES** is a liquid **mid-cap** Industrials name (Specialty Industrial Machinery) at $6.8B, ADV ~1930k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.91 | +0.279 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.19 | +0.053 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.948** | |

### 13. SWBI · $639M small · Industrials

**1m score +0.917**

**SWBI** is a liquid **small-cap** Industrials name (Aerospace & Defense) at $639M, ADV ~576k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.57 | +0.175 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.70 | +0.293 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.71 | +0.198 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.917** | |

### 14. BE · $85.0B large · Industrials

**1m score +0.877**

**BE** is a liquid **large-cap** Industrials name (Electrical Equipment & Parts) at $85.0B, ADV ~16239k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide). Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.86 | +0.261 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.20 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.85 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.94 | +0.262 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.05 | -0.050 | liquid small/mid, room to run |
| **1m total** | | | **+0.877** | |

### 15. SPNT · $2.9B mid · Financial

**1m score +0.867**

**SPNT** is a liquid **mid-cap** Financial name (Insurance - Reinsurance) at $2.9B, ADV ~831k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.79 | +0.242 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.64 | +0.265 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.58 | +0.160 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.867** | |

### 16. PURR · $3.1B mid · Financial

**1m score +0.857**

**PURR** is a liquid **mid-cap** Financial name (Capital Markets) at $3.1B, ADV ~17622k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.95 | +0.290 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.76 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.857** | |

### 17. JXN · $9.0B mid · Financial

**1m score +0.823**

**JXN** is a liquid **mid-cap** Financial name (Insurance - Life) at $9.0B, ADV ~698k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.87 | +0.267 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.55 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.27 | +0.075 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.823** | |

### 18. WRBY · $3.3B mid · Healthcare

**1m score +0.790**

**WRBY** is a liquid **mid-cap** Healthcare name (Medical Instruments & Supplies) at $3.3B, ADV ~2768k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | -0.15 | -0.046 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.97 | +0.270 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.790** | |

### 19. MKC · $12.9B large · Consumer Defensive

**1m score +0.714**

**MKC** is a liquid **large-cap** Consumer Defensive name (Packaged Foods) at $12.9B, ADV ~3291k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.66 | +0.203 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.70 | +0.293 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.21 | +0.057 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.11 | +0.110 | liquid small/mid, room to run |
| **1m total** | | | **+0.714** | |

### 20. ETON · $1.6B small · Healthcare

**1m score +0.657**

**ETON** is a liquid **small-cap** Healthcare name (Drug Manufacturers - Specialty & Generic) at $1.6B, ADV ~567k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | -0.15 | -0.046 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.96 | +0.402 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.36 | +0.101 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.657** | |

### 21. CRML · $1.2B small · Basic Materials

**1m score +0.656**

**CRML** is a liquid **small-cap** Basic Materials name (Other Industrial Metals & Mining) at $1.2B, ADV ~7770k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | -0.15 | -0.046 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.55 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.98 | +0.271 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.656** | |

### 22. INSW · $5.2B mid · Energy

**1m score +0.644**

**INSW** is a liquid **mid-cap** Energy name (Oil & Gas Midstream) at $5.2B, ADV ~629k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | -0.04 | -0.011 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.85 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.19 | +0.052 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.644** | |

### 23. PBF · $8.7B mid · Energy

**1m score +0.633**

**PBF** is a liquid **mid-cap** Energy name (Oil & Gas Refining & Marketing) at $8.7B, ADV ~3394k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | -0.15 | -0.046 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.22 | +0.062 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.633** | |

### 24. AVNT · $3.8B mid · Basic Materials

**1m score +0.632**

**AVNT** is a liquid **mid-cap** Basic Materials name (Specialty Chemicals) at $3.8B, ADV ~839k shares/day. Setup: tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | -0.15 | -0.046 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.95 | +0.398 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.11 | +0.031 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.632** | |

### 25. BJ · $11.8B large · Consumer Defensive

**1m score +0.630**

**BJ** is a liquid **large-cap** Consumer Defensive name (Discount Stores) at $11.8B, ADV ~1753k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.58 | +0.179 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.98 | +0.408 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.05 | +0.013 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.03 | +0.030 | liquid small/mid, room to run |
| **1m total** | | | **+0.630** | |


## 1m AVOID — bottom of the same rank

- **ROIV** (large, Healthcare, $25.9B) score -0.872. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PYXS** (micro, Healthcare, $239M) score -0.762. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NRXP** (micro, Healthcare, $130M) score -0.740. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AIRS** (micro, Healthcare, $141M) score -0.736. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AXSM** (large, Healthcare, $10.2B) score -0.720. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LFMD** (micro, Healthcare, $143M) score -0.705. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **LAB** (micro, Healthcare, $290M) score -0.704. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **PROK** (small, Healthcare, $484M) score -0.691. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ALMS** (small, Healthcare, $948M) score -0.680. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **WVE** (small, Healthcare, $745M) score -0.680. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SOC** (small, Energy, $744M) score -0.679. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ACDC** (small, Energy, $839M) score -0.670. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LXRX** (small, Healthcare, $799M) score -0.664. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **MIST** (micro, Healthcare, $98M) score -0.656. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **IMNM** (mid, Healthcare, $2.4B) score -0.649. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **MGNX** (micro, Healthcare, $238M) score -0.647. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **TNYA** (micro, Healthcare, $137M) score -0.643. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TYRA** (small, Healthcare, $1.5B) score -0.642. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **APA** (large, Energy, $15.0B) score -0.637. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **STLA** (large, Consumer Cyclical, $13.3B) score -0.635. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SEER** (micro, Healthcare, $106M) score -0.635. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **LYB** (large, Basic Materials, $18.8B) score -0.633. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**
- **CHRS** (micro, Healthcare, $175M) score -0.632. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **TJGC** (micro, Communication Services, $265M) score -0.626. the Finviz industry was **down**
- **PUMP** (small, Energy, $1.2B) score -0.625. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-09-27_stock_book.md`
- Machine table: `data/stock_book/2026-09-27_stock_book.csv`
- Machine book: `data/stock_book/2026-09-27_stock_book.json`
- Join rank: `data/join/2026-09-27_ranked.csv`
- Weather: `01_daily/weather/2026-09-27_weather.md`
- AB enrich: `data/ab_checklist/2026-09-27_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-09-27_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-09-27_map_heat.md`
