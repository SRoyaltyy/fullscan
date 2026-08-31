# Stock book — 2026-08-31

_Generated 2026-08-31T11:29:17.371678-04:00_

This file is the **human read** of one run. CSV/JSON next to it are the machine files.

## How today's rank is built

Every liquid name ($80M+ mcap, 500k+ ADV) gets six signals, then a mid-cap opportunity add-on:

1. **Join × weather** — do this *kind* of stock (sector, size, trend, leverage, earnings) fit today's tape?
2. **Sector / general predict** — same-day LLM calls only. Missing file = 0, never yesterday's leftover.
3. **News / judge** — headlines plus the news-judge ticker list (AU/ADBE-style).
4. **AB checklist** — structure score + P01–P04 (beats peers? peers up? industry up? sector board up?).
5. **Peer RS** — this week's return vs that name's own correlated basket. Kills XLE clones.
6. **Mid-cap opportunity** — extra points for liquid small/mid ($400M–$20B) that are not jammed at the 52-week high. Micros skipped. Max 4 large/mega.

**All-green BUY.** A name is green when join, AB, and peer are all ≥ +0.05, sector and news are not red, and relvol is not dead (< 0.7). General is a market-wide SPX stamp — a modest red general does not empty the pile. A hard-red general (≤ −0.25) is a veto unless the same-day sector call is green. Event-scanner sector tilt is clipped to ±0.20 so it cannot invert the day's essay. mid_opp is capped at +0.20 and zeroed on hard sector-red names. BUY also drops LAG+peer-losers and printed dead volume. If ≥ 8 liquid green names survive the $400M / 4-per-sector / 3-per-industry / 4 large-mega caps, BUY 15 is filled from that pile by green_rank (no opp). If the pile is thinner (usually AB or peers all zeros), the ranker keeps the weighted walk under the same vetoes. **SELL always ranks on core weights** and never shorts a green name when the pile is used.

**1d** leans on news + AB + peers. **1m** drops news and leans on AB + peers + join.
A sector headline (e.g. ADBE) cannot zero a mid-cap that just beat earnings or is leading its own peers.

## Today's regime

- Weather risk: **off**
- General predict (same-day): -0.47 (present)
- Sector predicts this date: 10/11 (ok)
- News tickers in play: 120
- AB coverage: 2551 names · peer RS: 2400
- Universe after liquidity: 2685
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 105

## All-green BUY / SELL

- Mode: **green_pile** · SELL **core_weights_ex_green**
- Pile: **93** liquid all-green names (need ≥ 8) of 2685
- Core fired: join=yes, AB=yes, peer=yes
- pile 93 ≥ 8 liquid all-green names — BUY 15 from the pile by green_rank (no opp); SELL is core weights on the non-green remainder

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
| Map heat research | **missing / not in ranker** | s_heat nested override + captains |
| Catalyst overlays | **missing / not in ranker** | not in ranker — separate chart workflow |
| Insider / politician flow | **missing / not in ranker** | no daily file in repo |
| Industry predict | **found** | not scored (ad-hoc only) |
| Learnings / mutable policy | **missing / not in ranker** | next predict prompt, not a ticker score |

### Sector LLM bias (1d) — 0 / empty means that essay was not run today

| Sector | bias |
|--------|------|
| Basic Materials | -0.55 |
| Real Estate | -0.55 |
| Consumer Cyclical | -0.55 |
| Healthcare | -0.50 |
| Energy | +0.47 |
| Industrials | -0.28 |
| Technology | -0.28 |
| Utilities | -0.28 |
| Financial | +0.25 |
| Consumer Defensive | +0.00 |

### How much each predictor is trusted (graded hit rate)

| Topic | hit rate | n | weight |
|-------|----------|---|--------|
| general | 48% | 21 | ×0.85 |
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

## Horizon weights — book_policy.json v5

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.12 | 0.10 | 0.08 | 0.25 | 0.25 | 0.20 | additive |
| 3d | 0.16 | 0.14 | 0.08 | 0.16 | 0.26 | 0.20 | additive |
| 1w | 0.18 | 0.16 | 0.08 | 0.10 | 0.28 | 0.20 | additive |
| 2w | 0.20 | 0.18 | 0.08 | 0.06 | 0.28 | 0.20 | additive |
| 1m | 0.22 | 0.20 | 0.08 | 0.00 | 0.30 | 0.20 | additive |

## 1d BUY — why these names

### 1. TRGP · $70.9B large · Energy

**1d score +0.392**

**TRGP** is a liquid **large-cap** Energy name (Oil & Gas Midstream) at $70.9B, ADV ~1250k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.97 | +0.117 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.27 | +0.027 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.76 | +0.190 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.87 | +0.174 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.11 | -0.110 | liquid small/mid, room to run |
| **1d total** | | | **+0.392** | |

### 2. BZ · $7.0B mid · Communication Services

**1d score +0.594**

**BZ** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $7.0B, ADV ~4412k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.97 | +0.116 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.36 | +0.090 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.97 | +0.193 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.594** | |

### 3. CHRD · $8.3B mid · Energy

**1d score +0.634**

**CHRD** is a liquid **mid-cap** Energy name (Oil & Gas E&P) at $8.3B, ADV ~795k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.27 | +0.027 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.70 | +0.176 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.59 | +0.118 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.634** | |

### 4. SM · $8.9B mid · Energy

**1d score +0.852**

**SM** is a liquid **mid-cap** Energy name (Oil & Gas E&P) at $8.9B, ADV ~3820k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.98 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.27 | +0.027 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.83 | +0.208 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.91 | +0.226 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.39 | +0.078 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.852** | |

### 5. NMR · $29.9B large · Financial

**1d score +0.264**

**NMR** is a liquid **large-cap** Financial name (Capital Markets) at $29.9B, ADV ~1078k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.83 | +0.099 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.05 | +0.005 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.91 | +0.226 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.54 | +0.109 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.264** | |

### 6. XP · $9.1B mid · Financial

**1d score +0.619**

**XP** is a liquid **mid-cap** Financial name (Capital Markets) at $9.1B, ADV ~5263k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.80 | +0.095 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.05 | +0.005 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.23 | -0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.91 | +0.226 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.55 | +0.111 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.619** | |

### 7. RES · $1.5B small · Energy

**1d score +0.636**

**RES** is a liquid **small-cap** Energy name (Oil & Gas Equipment & Services) at $1.5B, ADV ~1549k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.90 | +0.108 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.27 | +0.027 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.76 | +0.190 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.59 | +0.117 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.636** | |

### 8. BNS · $113.9B large · Financial

**1d score +0.325**

**BNS** is a liquid **large-cap** Financial name (Banks - Diversified) at $113.9B, ADV ~2564k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.74 | +0.089 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.05 | +0.005 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.23 | -0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.31 | +0.077 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.91 | +0.226 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.58 | +0.116 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.325** | |

### 9. SPNT · $2.8B mid · Financial

**1d score +0.603**

**SPNT** is a liquid **mid-cap** Financial name (Insurance - Reinsurance) at $2.8B, ADV ~744k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.93 | +0.112 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.05 | +0.005 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.91 | +0.226 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.33 | +0.065 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.603** | |

### 10. PSO · $10.0B large · Communication Services

**1d score +0.229**

**PSO** is a liquid **large-cap** Communication Services name (Publishing) at $10.0B, ADV ~770k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.78 | +0.094 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.95 | +0.239 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.36 | +0.072 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1d total** | | | **+0.229** | |

### 11. ATHM · $2.6B mid · Communication Services

**1d score +0.576**

**ATHM** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $2.6B, ADV ~844k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.87 | +0.105 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.32 | +0.065 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.576** | |

### 12. WLY · $2.7B mid · Communication Services

**1d score +0.590**

**WLY** is a liquid **mid-cap** Communication Services name (Publishing) at $2.7B, ADV ~515k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.68 | +0.081 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | -0.07 | -0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.91 | +0.226 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.44 | +0.088 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1d total** | | | **+0.590** | |


## 1d AVOID — bottom of the same rank


## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | TRGP | +0.449 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | BZ | +0.636 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | CHRD | +0.691 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | SM | +0.836 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | NMR | +0.273 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 6 | XP | +0.627 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | RES | +0.691 | small | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 8 | BNS | +0.303 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | SPNT | +0.616 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 10 | PSO | +0.270 | large | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 11 | ATHM | +0.619 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 12 | WLY | +0.626 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | TRGP | +0.488 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | BZ | +0.668 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | CHRD | +0.729 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | SM | +0.828 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | NMR | +0.343 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 6 | XP | +0.709 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | RES | +0.728 | small | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 8 | BNS | +0.365 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | SPNT | +0.688 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 10 | PSO | +0.311 | large | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 11 | ATHM | +0.659 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 12 | WLY | +0.663 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | TRGP | +0.436 | large | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | BZ | +0.687 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | CHRD | +0.676 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 4 | SM | +0.742 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | NMR | +0.321 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 6 | XP | +0.687 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | RES | +0.674 | small | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 8 | BNS | +0.330 | large | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | SPNT | +0.669 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 10 | PSO | +0.326 | large | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 11 | ATHM | +0.677 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 12 | WLY | +0.676 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 1m BUY — why these names

### 1. TRGP · $70.9B large · Energy

**1m score +0.548**

**TRGP** is a liquid **large-cap** Energy name (Oil & Gas Midstream) at $70.9B, ADV ~1250k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.97 | +0.214 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.21 | +0.042 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.87 | +0.174 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.11 | -0.110 | liquid small/mid, room to run |
| **1m total** | | | **+0.548** | |

### 2. BZ · $7.0B mid · Communication Services

**1m score +0.714**

**BZ** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $7.0B, ADV ~4412k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.97 | +0.213 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.36 | +0.108 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.97 | +0.193 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.714** | |

### 3. CHRD · $8.3B mid · Energy

**1m score +0.788**

**CHRD** is a liquid **mid-cap** Energy name (Oil & Gas E&P) at $8.3B, ADV ~795k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.21 | +0.042 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.211 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.59 | +0.118 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.788** | |

### 4. SM · $8.9B mid · Energy

**1m score +0.807**

**SM** is a liquid **mid-cap** Energy name (Oil & Gas E&P) at $8.9B, ADV ~3820k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.98 | +0.215 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.21 | +0.042 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.83 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.39 | +0.078 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.807** | |

### 5. NMR · $29.9B large · Financial

**1m score +0.352**

**NMR** is a liquid **large-cap** Financial name (Capital Markets) at $29.9B, ADV ~1078k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.83 | +0.182 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.54 | +0.109 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1m total** | | | **+0.352** | |

### 6. XP · $9.1B mid · Financial

**1m score +0.717**

**XP** is a liquid **mid-cap** Financial name (Capital Markets) at $9.1B, ADV ~5263k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.80 | +0.175 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.55 | +0.111 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.717** | |

### 7. RES · $1.5B small · Energy

**1m score +0.785**

**RES** is a liquid **small-cap** Energy name (Oil & Gas Equipment & Services) at $1.5B, ADV ~1549k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.90 | +0.197 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.21 | +0.042 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.59 | +0.117 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.785** | |

### 8. BNS · $113.9B large · Financial

**1m score +0.340**

**BNS** is a liquid **large-cap** Financial name (Banks - Diversified) at $113.9B, ADV ~2564k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.74 | +0.163 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.31 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.58 | +0.116 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1m total** | | | **+0.340** | |

### 9. SPNT · $2.8B mid · Financial

**1m score +0.702**

**SPNT** is a liquid **mid-cap** Financial name (Insurance - Reinsurance) at $2.8B, ADV ~744k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.93 | +0.205 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | -0.20 | -0.040 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.33 | +0.065 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.702** | |

### 10. PSO · $10.0B large · Communication Services

**1m score +0.361**

**PSO** is a liquid **large-cap** Communication Services name (Publishing) at $10.0B, ADV ~770k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.78 | +0.173 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.95 | +0.286 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.36 | +0.072 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | -0.17 | -0.170 | liquid small/mid, room to run |
| **1m total** | | | **+0.361** | |

### 11. ATHM · $2.6B mid · Communication Services

**1m score +0.711**

**ATHM** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $2.6B, ADV ~844k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.87 | +0.192 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.32 | +0.065 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.711** | |

### 12. WLY · $2.7B mid · Communication Services

**1m score +0.708**

**WLY** is a liquid **mid-cap** Communication Services name (Publishing) at $2.7B, ADV ~515k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment.

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.68 | +0.149 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.44 | +0.088 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.20 | +0.200 | liquid small/mid, room to run |
| **1m total** | | | **+0.708** | |


## 1m AVOID — bottom of the same rank


## Files for this run

- This rationale: `01_daily/2026-08-31_stock_book.md`
- Machine table: `data/stock_book/2026-08-31_stock_book.csv`
- Machine book: `data/stock_book/2026-08-31_stock_book.json`
- Join rank: `data/join/2026-08-31_ranked.csv`
- Weather: `01_daily/weather/2026-08-31_weather.md`
- AB enrich: `data/ab_checklist/2026-08-31_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-08-31_peer_rs.md`
