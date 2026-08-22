# Stock book — 2026-08-21

_Generated 2026-08-22T03:38:49.879333-04:00_

This file is the **human read** of one run. CSV/JSON next to it are the machine files.

## How today's rank is built

Every liquid name ($80M+ mcap, 500k+ ADV) gets six signals, then a mid-cap opportunity add-on:

1. **Join × weather** — do this *kind* of stock (sector, size, trend, leverage, earnings) fit today's tape?
2. **Sector / general predict** — same-day LLM calls only. Missing file = 0, never yesterday's leftover.
3. **News / judge** — headlines plus the news-judge ticker list (AU/ADBE-style).
4. **AB checklist** — structure score + P01–P04 (beats peers? peers up? industry up? sector board up?).
5. **Peer RS** — this week's return vs that name's own correlated basket. Kills XLE clones.
6. **Mid-cap opportunity** — extra points for liquid small/mid ($400M–$20B) that are not jammed at the 52-week high. Micros skipped. Max 4 large/mega.

**1d** leans on news + AB + peers. **1m** drops news and leans on AB + peers + join.
A sector headline (e.g. ADBE) cannot zero a mid-cap that just beat earnings or is leading its own peers.

## Today's regime

- Weather risk: **off**
- General predict (same-day): +0.47 (present)
- Sector predicts this date: 11/11 (ok)
- News tickers in play: 93
- AB coverage: 2562 names · peer RS: 2420
- Universe after liquidity: 2707
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 81

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
| Catalyst overlays | **missing / not in ranker** | not in ranker — separate chart workflow |
| Insider / politician flow | **missing / not in ranker** | no daily file in repo |
| Industry predict | **found** | not scored (ad-hoc only) |
| Learnings / mutable policy | **found** | next predict prompt, not a ticker score |

### Sector LLM bias (1d) — 0 / empty means that essay was not run today

| Sector | bias |
|--------|------|
| Basic Materials | +0.65 |
| Energy | +0.65 |
| Healthcare | +0.60 |
| Real Estate | +0.60 |
| Consumer Cyclical | -0.55 |
| Financial | +0.51 |
| Consumer Defensive | -0.47 |
| Technology | -0.47 |
| Utilities | +0.47 |
| Industrials | -0.28 |
| Communication Services | -0.25 |

### How much each predictor is trusted (graded hit rate)

| Topic | hit rate | n | weight |
|-------|----------|---|--------|
| general | 53% | 19 | ×0.85 |
| sector:Basic Materials | 62% | 8 | ×1.00 |
| sector:Communication Services | 38% | 8 | ×0.50 |
| sector:Consumer Cyclical | 75% | 8 | ×1.00 |
| sector:Consumer Defensive | 50% | 8 | ×0.85 |
| sector:Energy | 62% | 8 | ×1.00 |
| sector:Financial | 50% | 8 | ×0.85 |
| sector:Healthcare | 75% | 8 | ×1.00 |
| sector:Industrials | 25% | 8 | ×0.50 |
| sector:Real Estate | 62% | 8 | ×1.00 |
| sector:Technology | 50% | 8 | ×0.85 |
| sector:Utilities | 50% | 8 | ×0.85 |

## Horizon weights

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.12 | 0.10 | 0.08 | 0.25 | 0.25 | 0.20 | additive |
| 3d | 0.16 | 0.14 | 0.08 | 0.16 | 0.26 | 0.20 | additive |
| 1w | 0.18 | 0.16 | 0.08 | 0.10 | 0.28 | 0.20 | additive |
| 2w | 0.20 | 0.18 | 0.08 | 0.06 | 0.28 | 0.20 | additive |
| 1m | 0.22 | 0.20 | 0.08 | 0.00 | 0.30 | 0.20 | additive |

## 1d BUY — why these names

### 1. MOS · $7.8B mid · Basic Materials

**1d score +1.146**

**MOS** is a liquid **mid-cap** Basic Materials name (Agricultural Inputs) at $7.8B, ADV ~8684k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.84 | +0.101 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.65 | +0.065 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.23 | +0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.64 | +0.159 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.81 | +0.162 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1d total** | | | **+1.146** | |

### 2. OCUL · $2.5B mid · Healthcare

**1d score +1.135**

**OCUL** is a liquid **mid-cap** Healthcare name (Biotechnology) at $2.5B, ADV ~2710k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.91 | +0.109 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.60 | +0.060 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.23 | +0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.76 | +0.190 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.79 | +0.157 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.60 | +0.600 | liquid small/mid, room to run |
| **1d total** | | | **+1.135** | |

### 3. GSHD · $2.6B mid · Financial

**1d score +1.134**

**GSHD** is a liquid **mid-cap** Financial name (Insurance Brokers) at $2.6B, ADV ~569k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.91 | +0.109 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.75 | +0.075 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.47 | +0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.96 | +0.241 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.76 | +0.151 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1d total** | | | **+1.134** | |

### 4. DUOL · $6.9B mid · Technology

**1d score +1.133**

**DUOL** is a liquid **mid-cap** Technology name (Software - Application) at $6.9B, ADV ~1343k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | -0.15 | -0.018 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.41 | +0.041 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.23 | +0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.95 | +0.239 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.86 | +0.173 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.68 | +0.680 | liquid small/mid, room to run |
| **1d total** | | | **+1.133** | |

### 5. CRSP · $6.0B mid · Healthcare

**1d score +1.133**

**CRSP** is a liquid **mid-cap** Healthcare name (Biotechnology) at $6.0B, ADV ~1672k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.83 | +0.099 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.60 | +0.060 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.47 | +0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.67 | +0.134 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.60 | +0.600 | liquid small/mid, room to run |
| **1d total** | | | **+1.133** | |

### 6. XP · $8.7B mid · Financial

**1d score +1.124**

**XP** is a liquid **mid-cap** Financial name (Capital Markets) at $8.7B, ADV ~5329k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.87 | +0.105 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.75 | +0.075 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.23 | +0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.96 | +0.241 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.82 | +0.164 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1d total** | | | **+1.124** | |

### 7. OPCH · $3.6B mid · Healthcare

**1d score +1.104**

**OPCH** is a liquid **mid-cap** Healthcare name (Medical Care Facilities) at $3.6B, ADV ~2582k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.93 | +0.112 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.60 | +0.060 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.07 | +0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.96 | +0.241 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.22 | +0.045 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1d total** | | | **+1.104** | |

### 8. WGS · $2.5B mid · Healthcare

**1d score +1.094**

**WGS** is a liquid **mid-cap** Healthcare name (Diagnostics & Research) at $2.5B, ADV ~939k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.82 | +0.099 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.60 | +0.060 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.47 | +0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.88 | +0.220 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.19 | +0.038 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1d total** | | | **+1.094** | |

### 9. HLNE · $5.8B mid · Financial

**1d score +1.085**

**HLNE** is a liquid **mid-cap** Financial name (Asset Management) at $5.8B, ADV ~906k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.95 | +0.114 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.75 | +0.075 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.23 | +0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.33 | +0.065 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.60 | +0.600 | liquid small/mid, room to run |
| **1d total** | | | **+1.085** | |

### 10. FIGR · $8.7B mid · Financial

**1d score +1.074**

**FIGR** is a liquid **mid-cap** Financial name (Capital Markets) at $8.7B, ADV ~3935k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.94 | +0.113 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.75 | +0.075 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.19 | +0.015 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.93 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.00 | +0.000 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1d total** | | | **+1.074** | |

### 11. EPAM · $5.8B mid · Technology

**1d score +1.073**

**EPAM** is a liquid **mid-cap** Technology name (Information Technology Services) at $5.8B, ADV ~1903k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | -0.15 | -0.018 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.41 | +0.041 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.47 | +0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.91 | +0.226 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.73 | +0.146 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1d total** | | | **+1.073** | |

### 12. CALX · $2.5B mid · Technology

**1d score +1.072**

**CALX** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $2.5B, ADV ~1177k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | -0.15 | -0.018 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.41 | +0.041 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.23 | +0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.76 | +0.190 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.80 | +0.159 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.68 | +0.680 | liquid small/mid, room to run |
| **1d total** | | | **+1.072** | |

### 13. NHI · $3.6B mid · Real Estate

**1d score +1.035**

**NHI** is a liquid **mid-cap** Real Estate name (REIT - Healthcare Facilities) at $3.6B, ADV ~600k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.89 | +0.106 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.60 | +0.060 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.07 | +0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.05 | +0.011 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1d total** | | | **+1.035** | |

### 14. ZG · $8.2B mid · Communication Services

**1d score +1.034**

**ZG** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $8.2B, ADV ~1331k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.38 | +0.046 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | -0.25 | -0.025 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.47 | +0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.55 | +0.139 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.78 | +0.157 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.68 | +0.680 | liquid small/mid, room to run |
| **1d total** | | | **+1.034** | |

### 15. VNT · $4.5B mid · Technology

**1d score +1.019**

**VNT** is a liquid **mid-cap** Technology name (Scientific & Technical Instruments) at $4.5B, ADV ~1726k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | -0.15 | -0.018 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.41 | +0.041 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.23 | +0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.88 | +0.220 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.58 | +0.117 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1d total** | | | **+1.019** | |

### 16. CE · $5.2B mid · Basic Materials

**1d score +1.014**

**CE** is a liquid **mid-cap** Basic Materials name (Chemicals) at $5.2B, ADV ~1797k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.82 | +0.098 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.65 | +0.065 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.07 | +0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.46 | +0.116 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.45 | +0.089 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1d total** | | | **+1.014** | |

### 17. FBRT · $692M small · Real Estate

**1d score +0.996**

**FBRT** is a liquid **small-cap** Real Estate name (REIT - Mortgage) at $692M, ADV ~1242k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.79 | +0.095 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.60 | +0.060 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.23 | +0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.94 | +0.235 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.33 | +0.066 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1d total** | | | **+0.996** | |

### 18. NMRK · $2.9B mid · Real Estate

**1d score +0.991**

**NMRK** is a liquid **mid-cap** Real Estate name (Real Estate Services) at $2.9B, ADV ~1375k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.92 | +0.111 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.60 | +0.060 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.47 | +0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.94 | +0.235 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.34 | +0.068 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.48 | +0.480 | liquid small/mid, room to run |
| **1d total** | | | **+0.991** | |

### 19. ABR · $977M small · Real Estate

**1d score +0.982**

**ABR** is a liquid **small-cap** Real Estate name (REIT - Mortgage) at $977M, ADV ~4176k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.61 | +0.073 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.60 | +0.060 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.23 | +0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.93 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.39 | +0.079 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1d total** | | | **+0.982** | |

### 20. GPK · $3.6B mid · Consumer Cyclical

**1d score +0.978**

**GPK** is a liquid **mid-cap** Consumer Cyclical name (Packaging & Containers) at $3.6B, ADV ~5576k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | -0.15 | -0.018 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | -0.31 | -0.031 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.07 | +0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.95 | +0.239 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.51 | +0.103 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.68 | +0.680 | liquid small/mid, room to run |
| **1d total** | | | **+0.978** | |

### 21. NFG · $7.9B mid · Energy

**1d score +0.966**

**NFG** is a liquid **mid-cap** Energy name (Oil & Gas Integrated) at $7.9B, ADV ~710k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.92 | +0.111 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | -0.15 | -0.015 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.07 | +0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.64 | +0.159 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.33 | +0.066 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1d total** | | | **+0.966** | |

### 22. BZ · $6.3B mid · Communication Services

**1d score +0.959**

**BZ** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $6.3B, ADV ~3748k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.69 | +0.083 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | -0.25 | -0.025 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.07 | +0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.64 | +0.159 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.48 | +0.097 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1d total** | | | **+0.959** | |

### 23. BFAM · $3.6B mid · Consumer Cyclical

**1d score +0.958**

**BFAM** is a liquid **mid-cap** Consumer Cyclical name (Personal Services) at $3.6B, ADV ~952k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | -0.15 | -0.018 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | -0.31 | -0.031 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.23 | +0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.76 | +0.190 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.59 | +0.117 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.68 | +0.680 | liquid small/mid, room to run |
| **1d total** | | | **+0.958** | |

### 24. Z · $8.2B mid · Communication Services

**1d score +0.956**

**Z** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $8.2B, ADV ~4390k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.48 | +0.058 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | -0.25 | -0.025 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.47 | +0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.36 | +0.090 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.58 | +0.116 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.68 | +0.680 | liquid small/mid, room to run |
| **1d total** | | | **+0.956** | |

### 25. DNN · $3.2B mid · Energy

**1d score +0.955**

**DNN** is a liquid **mid-cap** Energy name (Uranium) at $3.2B, ADV ~24257k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.79 | +0.095 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | -0.15 | -0.015 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.23 | +0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.70 | +0.176 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.81 | +0.161 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1d total** | | | **+0.955** | |


## 1d AVOID — bottom of the same rank

- **INTC** (mega, Technology, $476.3B) score -0.588. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GEV** (mega, Industrials, $256.4B) score -0.526. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ARM** (mega, Technology, $262.7B) score -0.446. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BWEN** (micro, Industrials, $105M) score -0.439. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BE** (large, Industrials, $58.8B) score -0.431. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **VOD** (large, Communication Services, $36.8B) score -0.423. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **MDB** (large, Technology, $34.6B) score -0.397. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **VSAT** (large, Technology, $10.3B) score -0.382. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LYV** (large, Communication Services, $42.9B) score -0.376. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **BIDU** (large, Communication Services, $26.2B) score -0.373. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OC** (large, Industrials, $11.8B) score -0.370. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AUR** (large, Consumer Cyclical, $12.5B) score -0.367. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **GSAT** (large, Communication Services, $10.7B) score -0.360. this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **VRT** (large, Industrials, $100.4B) score -0.347. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SES** (micro, Consumer Cyclical, $197M) score -0.340. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **AREC** (micro, Industrials, $283M) score -0.331. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **CODI** (small, Industrials, $888M) score -0.330. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CRWD** (large, Technology, $194.4B) score -0.326. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SMTC** (large, Technology, $11.6B) score -0.325. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GETY** (micro, Communication Services, $112M) score -0.324. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **CRS** (large, Industrials, $24.2B) score -0.321. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TSEM** (large, Technology, $25.2B) score -0.316. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **MOD** (large, Consumer Cyclical, $10.5B) score -0.312. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **CMI** (large, Industrials, $81.7B) score -0.312. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **BGS** (micro, Consumer Defensive, $276M) score -0.311. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | MOS | +1.203 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | GSHD | +1.201 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | OCUL | +1.195 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | XP | +1.191 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 5 | CRSP | +1.187 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | OPCH | +1.167 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 7 | DUOL | +1.158 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | HLNE | +1.153 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 9 | WGS | +1.150 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | FIGR | +1.144 | mid | Financial | the Finviz industry was **advancing** |
| 11 | NHI | +1.102 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | EPAM | +1.095 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 13 | CALX | +1.094 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | ZG | +1.076 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 15 | CE | +1.070 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 16 | FBRT | +1.059 | small | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 17 | NMRK | +1.058 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 18 | VNT | +1.042 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 19 | SMA | +1.039 | mid | Real Estate | the Finviz industry was **down** |
| 20 | BZ | +1.017 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 21 | SOLS | +1.003 | mid | Basic Materials | the Finviz industry was **down** |
| 22 | Z | +1.000 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 23 | NFG | +0.996 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 24 | ALM | +0.993 | mid | Basic Materials | the Finviz industry was **advancing** |
| 25 | ELF | +0.984 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | GSHD | +1.253 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | XP | +1.242 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 3 | OCUL | +1.239 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | MOS | +1.237 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | CRSP | +1.231 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | OPCH | +1.216 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 7 | HLNE | +1.203 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 8 | FIGR | +1.195 | mid | Financial | the Finviz industry was **advancing** |
| 9 | WGS | +1.195 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | DUOL | +1.183 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | NHI | +1.141 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | EPAM | +1.119 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 13 | CALX | +1.115 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | NMRK | +1.099 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 15 | CE | +1.099 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 16 | FBRT | +1.098 | small | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 17 | SMA | +1.077 | mid | Real Estate | the Finviz industry was **down** |
| 18 | VNT | +1.066 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 19 | ZG | +1.055 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 20 | SOLS | +1.034 | mid | Basic Materials | the Finviz industry was **down** |
| 21 | ERO | +1.032 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 22 | NFG | +1.023 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 23 | DNN | +1.005 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 24 | BZ | +1.004 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 25 | ELF | +0.999 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | GSHD | +1.274 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 2 | XP | +1.264 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 3 | DUOL | +1.264 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | OCUL | +1.258 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | MOS | +1.254 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | CRSP | +1.246 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | OPCH | +1.236 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 8 | HLNE | +1.227 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 9 | FIGR | +1.219 | mid | Financial | the Finviz industry was **advancing** |
| 10 | WGS | +1.210 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | EPAM | +1.198 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 12 | CALX | +1.196 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 13 | VNT | +1.147 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 14 | CE | +1.117 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 15 | ZG | +1.099 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 16 | NHI | +1.070 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 17 | GPK | +1.067 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 18 | BZ | +1.057 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 19 | ERO | +1.051 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 20 | SOLS | +1.049 | mid | Basic Materials | the Finviz industry was **down** |
| 21 | BFAM | +1.039 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 22 | NMRK | +1.026 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 23 | FBRT | +1.024 | small | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 24 | Z | +1.023 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 25 | NFG | +1.019 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |

## 1m BUY — why these names

### 1. XP · $8.7B mid · Financial

**1m score +1.299**

**XP** is a liquid **mid-cap** Financial name (Capital Markets) at $8.7B, ADV ~5329k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.87 | +0.192 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.67 | +0.133 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.96 | +0.289 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.82 | +0.164 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1m total** | | | **+1.299** | |

### 2. GSHD · $2.6B mid · Financial

**1m score +1.294**

**GSHD** is a liquid **mid-cap** Financial name (Insurance Brokers) at $2.6B, ADV ~569k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.91 | +0.201 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.67 | +0.133 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.96 | +0.289 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.76 | +0.151 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1m total** | | | **+1.294** | |

### 3. OCUL · $2.5B mid · Healthcare

**1m score +1.286**

**OCUL** is a liquid **mid-cap** Healthcare name (Biotechnology) at $2.5B, ADV ~2710k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.91 | +0.200 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.50 | +0.100 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.79 | +0.157 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.60 | +0.600 | liquid small/mid, room to run |
| **1m total** | | | **+1.286** | |

### 4. DUOL · $6.9B mid · Technology

**1m score +1.282**

**DUOL** is a liquid **mid-cap** Technology name (Software - Application) at $6.9B, ADV ~1343k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.15 | -0.033 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.88 | +0.176 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.95 | +0.286 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.86 | +0.173 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.68 | +0.680 | liquid small/mid, room to run |
| **1m total** | | | **+1.282** | |

### 5. OPCH · $3.6B mid · Healthcare

**1m score +1.280**

**OPCH** is a liquid **mid-cap** Healthcare name (Medical Care Facilities) at $3.6B, ADV ~2582k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.93 | +0.205 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.50 | +0.100 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.96 | +0.289 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.22 | +0.045 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1m total** | | | **+1.280** | |

### 6. MOS · $7.8B mid · Basic Materials

**1m score +1.278**

**MOS** is a liquid **mid-cap** Basic Materials name (Agricultural Inputs) at $7.8B, ADV ~8684k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.84 | +0.185 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.50 | +0.100 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.64 | +0.191 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.81 | +0.162 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1m total** | | | **+1.278** | |

### 7. HLNE · $5.8B mid · Financial

**1m score +1.261**

**HLNE** is a liquid **mid-cap** Financial name (Asset Management) at $5.8B, ADV ~906k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.95 | +0.209 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.67 | +0.133 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.33 | +0.065 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.60 | +0.600 | liquid small/mid, room to run |
| **1m total** | | | **+1.261** | |

### 8. CRSP · $6.0B mid · Healthcare

**1m score +1.258**

**CRSP** is a liquid **mid-cap** Healthcare name (Biotechnology) at $6.0B, ADV ~1672k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.83 | +0.182 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.50 | +0.100 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.67 | +0.134 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.60 | +0.600 | liquid small/mid, room to run |
| **1m total** | | | **+1.258** | |

### 9. FIGR · $8.7B mid · Financial

**1m score +1.258**

**FIGR** is a liquid **mid-cap** Financial name (Capital Markets) at $8.7B, ADV ~3935k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.94 | +0.207 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.67 | +0.133 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.93 | +0.278 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.00 | +0.000 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1m total** | | | **+1.258** | |

### 10. INSP · $1.8B small · Healthcare

**1m score +1.240**

**INSP** is a liquid **small-cap** Healthcare name (Medical Devices) at $1.8B, ADV ~959k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.92 | +0.203 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.50 | +0.100 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.87 | +0.174 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1m total** | | | **+1.240** | |

### 11. CALX · $2.5B mid · Technology

**1m score +1.211**

**CALX** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $2.5B, ADV ~1177k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.15 | -0.033 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.88 | +0.176 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.80 | +0.159 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.68 | +0.680 | liquid small/mid, room to run |
| **1m total** | | | **+1.211** | |

### 12. EPAM · $5.8B mid · Technology

**1m score +1.201**

**EPAM** is a liquid **mid-cap** Technology name (Information Technology Services) at $5.8B, ADV ~1903k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.15 | -0.033 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.88 | +0.176 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.91 | +0.272 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.73 | +0.146 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1m total** | | | **+1.201** | |

### 13. GPK · $3.6B mid · Consumer Cyclical

**1m score +1.184**

**GPK** is a liquid **mid-cap** Consumer Cyclical name (Packaging & Containers) at $3.6B, ADV ~5576k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.15 | -0.033 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.74 | +0.148 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.95 | +0.286 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.51 | +0.103 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.68 | +0.680 | liquid small/mid, room to run |
| **1m total** | | | **+1.184** | |

### 14. VNT · $4.5B mid · Technology

**1m score +1.164**

**VNT** is a liquid **mid-cap** Technology name (Scientific & Technical Instruments) at $4.5B, ADV ~1726k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.15 | -0.033 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.88 | +0.176 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.264 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.58 | +0.117 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1m total** | | | **+1.164** | |

### 15. CE · $5.2B mid · Basic Materials

**1m score +1.148**

**CE** is a liquid **mid-cap** Basic Materials name (Chemicals) at $5.2B, ADV ~1797k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.82 | +0.180 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.50 | +0.100 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.46 | +0.139 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.45 | +0.089 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1m total** | | | **+1.148** | |

### 16. BFAM · $3.6B mid · Consumer Cyclical

**1m score +1.141**

**BFAM** is a liquid **mid-cap** Consumer Cyclical name (Personal Services) at $3.6B, ADV ~952k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.15 | -0.033 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.74 | +0.148 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.59 | +0.117 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.68 | +0.680 | liquid small/mid, room to run |
| **1m total** | | | **+1.141** | |

### 17. ZG · $8.2B mid · Communication Services

**1m score +1.137**

**ZG** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $8.2B, ADV ~1331k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.38 | +0.084 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.25 | +0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.55 | +0.166 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.78 | +0.157 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.68 | +0.680 | liquid small/mid, room to run |
| **1m total** | | | **+1.137** | |

### 18. BZ · $6.3B mid · Communication Services

**1m score +1.129**

**BZ** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $6.3B, ADV ~3748k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.69 | +0.152 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.25 | +0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.64 | +0.191 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.48 | +0.097 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1m total** | | | **+1.129** | |

### 19. NHI · $3.6B mid · Real Estate

**1m score +1.100**

**NHI** is a liquid **mid-cap** Real Estate name (REIT - Healthcare Facilities) at $3.6B, ADV ~600k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.89 | +0.195 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.05 | +0.011 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1m total** | | | **+1.100** | |

### 20. ERO · $4.0B mid · Basic Materials

**1m score +1.085**

**ERO** is a liquid **mid-cap** Basic Materials name (Copper) at $4.0B, ADV ~1214k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.96 | +0.211 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.50 | +0.100 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.98 | +0.293 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.40 | +0.080 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.40 | +0.400 | liquid small/mid, room to run |
| **1m total** | | | **+1.085** | |

### 21. SOLS · $9.0B mid · Basic Materials

**1m score +1.073**

**SOLS** is a liquid **mid-cap** Basic Materials name (Specialty Chemicals) at $9.0B, ADV ~2950k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.76 | +0.167 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.50 | +0.100 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.55 | +0.166 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.00 | +0.000 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1m total** | | | **+1.073** | |

### 22. BKE · $2.2B mid · Consumer Cyclical

**1m score +1.073**

**BKE** is a liquid **mid-cap** Consumer Cyclical name (Apparel Retail) at $2.2B, ADV ~506k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.03 | -0.007 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.74 | +0.148 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.11 | +0.023 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.68 | +0.680 | liquid small/mid, room to run |
| **1m total** | | | **+1.073** | |

### 23. PLNT · $4.1B mid · Consumer Cyclical

**1m score +1.066**

**PLNT** is a liquid **mid-cap** Consumer Cyclical name (Leisure) at $4.1B, ADV ~1975k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | -0.15 | -0.033 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.74 | +0.148 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.81 | +0.162 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.56 | +0.560 | liquid small/mid, room to run |
| **1m total** | | | **+1.066** | |

### 24. Z · $8.2B mid · Communication Services

**1m score +1.059**

**Z** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $8.2B, ADV ~4390k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.48 | +0.106 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.25 | +0.050 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.36 | +0.108 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.58 | +0.116 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.68 | +0.680 | liquid small/mid, room to run |
| **1m total** | | | **+1.059** | |

### 25. FBRT · $692M small · Real Estate

**1m score +1.043**

**FBRT** is a liquid **small-cap** Real Estate name (REIT - Mortgage) at $692M, ADV ~1242k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.79 | +0.175 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.94 | +0.282 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.33 | +0.066 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1m total** | | | **+1.043** | |


## 1m AVOID — bottom of the same rank

- **GEV** (mega, Industrials, $256.4B) score -0.612. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BWEN** (micro, Industrials, $105M) score -0.564. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **INTC** (mega, Technology, $476.3B) score -0.543. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OC** (large, Industrials, $11.8B) score -0.480. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BE** (large, Industrials, $58.8B) score -0.474. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **VRT** (large, Industrials, $100.4B) score -0.463. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CODI** (small, Industrials, $888M) score -0.456. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GSAT** (large, Communication Services, $10.7B) score -0.443. this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AREC** (micro, Industrials, $283M) score -0.431. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **RKLB** (large, Industrials, $44.1B) score -0.426. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NFE** (micro, Energy, $84M) score -0.422. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**
- **MTZ** (large, Industrials, $21.3B) score -0.421. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GNRC** (large, Industrials, $12.1B) score -0.420. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TLN** (large, Utilities, $15.3B) score -0.415. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **VSAT** (large, Technology, $10.3B) score -0.414. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **VOD** (large, Communication Services, $36.8B) score -0.414. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **BIDU** (large, Communication Services, $26.2B) score -0.408. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CMI** (large, Industrials, $81.7B) score -0.407. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **CRS** (large, Industrials, $24.2B) score -0.404. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GETY** (micro, Communication Services, $112M) score -0.390. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **MDB** (large, Technology, $34.6B) score -0.389. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BGS** (micro, Consumer Defensive, $276M) score -0.371. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **BYRN** (micro, Industrials, $84M) score -0.371. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NVT** (large, Industrials, $24.8B) score -0.362. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **QXO** (large, Industrials, $14.3B) score -0.361. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-08-21_stock_book.md`
- Machine table: `data/stock_book/2026-08-21_stock_book.csv`
- Machine book: `data/stock_book/2026-08-21_stock_book.json`
- Join rank: `data/join/2026-08-21_ranked.csv`
- Weather: `01_daily/weather/2026-08-21_weather.md`
- AB enrich: `data/ab_checklist/2026-08-21_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-08-21_peer_rs.md`
