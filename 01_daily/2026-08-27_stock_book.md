# Stock book — 2026-08-27

_Generated 2026-08-27T01:21:44.466736-04:00_

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

- Weather risk: **unknown**
- General predict (same-day): +0.00 (MISSING → 0)
- Sector predicts this date: 0/11 (missing → sector layer is 0; Finviz week tape still sits in join)
- News tickers in play: 128
- AB coverage: 2552 names · peer RS: 2406
- Universe after liquidity: 2698
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 111

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
| Map heat research | **missing / not in ranker** | s_heat nested override + captains |
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
| general | 50% | 20 | ×0.85 |
| sector:Basic Materials | 70% | 10 | ×1.00 |
| sector:Communication Services | 30% | 10 | ×0.50 |
| sector:Consumer Cyclical | 70% | 10 | ×1.00 |
| sector:Consumer Defensive | 40% | 10 | ×0.50 |
| sector:Energy | 50% | 10 | ×0.85 |
| sector:Financial | 40% | 10 | ×0.50 |
| sector:Healthcare | 75% | 8 | ×1.00 |
| sector:Industrials | 20% | 10 | ×0.50 |
| sector:Real Estate | 60% | 10 | ×1.00 |
| sector:Technology | 40% | 10 | ×0.50 |
| sector:Utilities | 40% | 10 | ×0.50 |

## Horizon weights — book_policy.json v2 · renormalized (absent: sector, general)

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.15 | 0.00 | 0.00 | 0.30 | 0.30 | 0.24 | additive |
| 3d | 0.21 | 0.00 | 0.00 | 0.21 | 0.33 | 0.26 | additive |
| 1w | 0.24 | 0.00 | 0.00 | 0.13 | 0.37 | 0.26 | additive |
| 2w | 0.27 | 0.00 | 0.00 | 0.08 | 0.38 | 0.27 | additive |
| 1m | 0.31 | 0.00 | 0.00 | 0.00 | 0.42 | 0.28 | additive |

## 1d BUY — why these names

### 1. RRC · $9.7B mid · Energy

**1d score +1.389**

**RRC** is a liquid **mid-cap** Energy name (Oil & Gas E&P) at $9.7B, ADV ~3041k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.98 | +0.144 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.24 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.88 | +0.267 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.232 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.60 | +0.145 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.60 | +0.600 | liquid small/mid, room to run |
| **1d total** | | | **+1.389** | |

### 2. CRK · $4.3B mid · Energy

**1d score +1.221**

**CRK** is a liquid **mid-cap** Energy name (Oil & Gas E&P) at $4.3B, ADV ~2450k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.97 | +0.142 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.24 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.64 | +0.194 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.84 | +0.204 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.68 | +0.680 | liquid small/mid, room to run |
| **1d total** | | | **+1.221** | |

### 3. ACMR · $5.5B mid · Technology

**1d score +1.203**

**ACMR** is a liquid **mid-cap** Technology name (Semiconductor Equipment & Materials) at $5.5B, ADV ~1447k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.95 | +0.139 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.40 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.35 | +0.105 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.232 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.52 | +0.126 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.60 | +0.600 | liquid small/mid, room to run |
| **1d total** | | | **+1.203** | |

### 4. MOS · $7.7B mid · Basic Materials

**1d score +1.179**

**MOS** is a liquid **mid-cap** Basic Materials name (Agricultural Inputs) at $7.7B, ADV ~8758k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.99 | +0.144 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.32 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.215 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.74 | +0.180 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1d total** | | | **+1.179** | |

### 5. ELF · $6.3B mid · Consumer Defensive

**1d score +1.162**

**ELF** is a liquid **mid-cap** Consumer Defensive name (Household & Personal Products) at $6.3B, ADV ~3121k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.92 | +0.134 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.24 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.247 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.74 | +0.181 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.60 | +0.600 | liquid small/mid, room to run |
| **1d total** | | | **+1.162** | |

### 6. EPAM · $5.6B mid · Technology

**1d score +1.153**

**EPAM** is a liquid **mid-cap** Technology name (Information Technology Services) at $5.6B, ADV ~1882k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.84 | +0.123 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.40 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.94 | +0.287 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.42 | +0.104 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1d total** | | | **+1.153** | |

### 7. CXT · $2.9B mid · Industrials

**1d score +1.146**

**CXT** is a liquid **mid-cap** Industrials name (Specialty Industrial Machinery) at $2.9B, ADV ~892k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.94 | +0.137 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.56 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.95 | +0.291 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.48 | +0.118 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.60 | +0.600 | liquid small/mid, room to run |
| **1d total** | | | **+1.146** | |

### 8. XP · $9.2B mid · Financial

**1d score +1.136**

**XP** is a liquid **mid-cap** Financial name (Capital Markets) at $9.2B, ADV ~5293k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.95 | +0.139 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.24 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.98 | +0.298 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.74 | +0.179 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1d total** | | | **+1.136** | |

### 9. MNDY · $3.9B mid · Technology

**1d score +1.127**

**MNDY** is a liquid **mid-cap** Technology name (Software - Application) at $3.9B, ADV ~1578k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.98 | +0.143 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.40 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.215 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.36 | +0.089 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.68 | +0.680 | liquid small/mid, room to run |
| **1d total** | | | **+1.127** | |

### 10. VNT · $4.6B mid · Technology

**1d score +1.113**

**VNT** is a liquid **mid-cap** Technology name (Scientific & Technical Instruments) at $4.6B, ADV ~1693k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.85 | +0.124 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.40 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.94 | +0.287 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.25 | +0.062 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1d total** | | | **+1.113** | |

### 11. BFAM · $3.6B mid · Consumer Cyclical

**1d score +1.102**

**BFAM** is a liquid **mid-cap** Consumer Cyclical name (Personal Services) at $3.6B, ADV ~935k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.84 | +0.123 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.80 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.259 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.33 | +0.081 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1d total** | | | **+1.102** | |

### 12. SLI · $644M small · Basic Materials

**1d score +1.100**

**SLI** is a liquid **small-cap** Basic Materials name (Other Industrial Metals & Mining) at $644M, ADV ~1765k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.84 | +0.122 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.32 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.247 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.86 | +0.211 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1d total** | | | **+1.100** | |

### 13. SFM · $8.1B mid · Consumer Defensive

**1d score +1.097**

**SFM** is a liquid **mid-cap** Consumer Defensive name (Grocery Stores) at $8.1B, ADV ~2017k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.65 | +0.095 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.24 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.95 | +0.291 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.79 | +0.192 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1d total** | | | **+1.097** | |

### 14. DEC · $1.0B small · Energy

**1d score +1.095**

**DEC** is a liquid **small-cap** Energy name (Oil & Gas Integrated) at $1.0B, ADV ~1034k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.66 | +0.097 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.24 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.74 | +0.226 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.55 | +0.169 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.67 | +0.164 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.44 | +0.440 | liquid small/mid, room to run |
| **1d total** | | | **+1.095** | |

### 15. LW · $7.6B mid · Consumer Defensive

**1d score +1.090**

**LW** is a liquid **mid-cap** Consumer Defensive name (Packaged Foods) at $7.6B, ADV ~1578k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.59 | +0.086 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.24 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.88 | +0.268 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.56 | +0.136 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.60 | +0.600 | liquid small/mid, room to run |
| **1d total** | | | **+1.090** | |

### 16. ICL · $7.3B mid · Basic Materials

**1d score +1.082**

**ICL** is a liquid **mid-cap** Basic Materials name (Agricultural Inputs) at $7.3B, ADV ~1309k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.73 | +0.107 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.32 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.94 | +0.287 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.36 | +0.088 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.60 | +0.600 | liquid small/mid, room to run |
| **1d total** | | | **+1.082** | |

### 17. WT · $3.8B mid · Financial

**1d score +1.079**

**WT** is a liquid **mid-cap** Financial name (Asset Management) at $3.8B, ADV ~2799k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.97 | +0.141 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.24 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.96 | +0.294 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.75 | +0.184 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.46 | +0.460 | liquid small/mid, room to run |
| **1d total** | | | **+1.079** | |

### 18. ALHC · $2.9B mid · Healthcare

**1d score +1.069**

**ALHC** is a liquid **mid-cap** Healthcare name (Healthcare Plans) at $2.9B, ADV ~5649k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | -0.15 | -0.022 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.247 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.67 | +0.164 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.68 | +0.680 | liquid small/mid, room to run |
| **1d total** | | | **+1.069** | |

### 19. KBR · $4.8B mid · Industrials

**1d score +1.069**

**KBR** is a liquid **mid-cap** Industrials name (Engineering & Construction) at $4.8B, ADV ~1736k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.57 | +0.084 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.56 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.95 | +0.291 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.22 | +0.054 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1d total** | | | **+1.069** | |

### 20. TIGR · $990M small · Financial

**1d score +1.067**

**TIGR** is a liquid **small-cap** Financial name (Capital Markets) at $990M, ADV ~3135k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.18 | +0.027 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.24 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.97 | +0.296 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.92 | +0.224 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1d total** | | | **+1.067** | |

### 21. Z · $7.9B mid · Communication Services

**1d score +1.062**

**Z** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $7.9B, ADV ~4334k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.91 | +0.132 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.55 | +0.169 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.33 | +0.081 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.68 | +0.680 | liquid small/mid, room to run |
| **1d total** | | | **+1.062** | |

### 22. BXSL · $5.8B mid · Financial

**1d score +1.055**

**BXSL** is a liquid **mid-cap** Financial name (Asset Management) at $5.8B, ADV ~1642k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.66 | +0.097 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.24 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.94 | +0.287 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.13 | +0.031 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1d total** | | | **+1.055** | |

### 23. OSK · $9.7B mid · Industrials

**1d score +1.054**

**OSK** is a liquid **mid-cap** Industrials name (Farm & Heavy Construction Machinery) at $9.7B, ADV ~748k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.96 | +0.140 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.56 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.95 | +0.291 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.42 | +0.103 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1d total** | | | **+1.054** | |

### 24. GPK · $3.4B mid · Consumer Cyclical

**1d score +1.054**

**GPK** is a liquid **mid-cap** Consumer Cyclical name (Packaging & Containers) at $3.4B, ADV ~5540k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.57 | +0.084 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.80 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.95 | +0.291 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | -0.00 | -0.001 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.68 | +0.680 | liquid small/mid, room to run |
| **1d total** | | | **+1.054** | |

### 25. DNN · $3.3B mid · Energy

**1d score +1.054**

**DNN** is a liquid **mid-cap** Energy name (Uranium) at $3.3B, ADV ~25228k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.15 | +0.71 | +0.104 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.24 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.30 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.215 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.24 | +0.88 | +0.215 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1d total** | | | **+1.054** | |


## 1d AVOID — bottom of the same rank

- **QTRX** (micro, Healthcare, $127M) score -0.461. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **NN** (mid, Technology, $2.6B) score -0.096. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GLUE** (small, Healthcare, $1.2B) score -0.288. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CTMX** (small, Healthcare, $723M) score -0.199. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **AZI** (micro, Consumer Cyclical, $88M) score -0.439. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ACH** (micro, Healthcare, $94M) score -0.436. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**
- **EYPT** (small, Healthcare, $430M) score -0.175. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **HUMA** (micro, Healthcare, $189M) score -0.414. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **PUMP** (small, Energy, $1.3B) score -0.252. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OTLK** (micro, Healthcare, $152M) score -0.404. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **IHRT** (small, Communication Services, $443M) score -0.163. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **IVVD** (micro, Healthcare, $266M) score -0.402. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AEVA** (small, Technology, $1.1B) score -0.201. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AXSM** (large, Healthcare, $10.9B) score -0.601. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SPRY** (small, Healthcare, $600M) score -0.151. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **ORGO** (micro, Healthcare, $225M) score -0.391. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**
- **GPCR** (mid, Healthcare, $3.5B) score -0.070. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FLNC** (mid, Utilities, $2.0B) score +0.011. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ANNX** (small, Healthcare, $946M) score -0.228. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **THRY** (micro, Technology, $84M) score -0.387. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **YRD** (micro, Financial, $92M) score -0.384. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ARDX** (small, Healthcare, $954M) score -0.142. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **MRNA** (large, Healthcare, $59.7B) score -0.711. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TSHA** (small, Healthcare, $2.0B) score -0.298. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LU** (small, Financial, $1.1B) score -0.137. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | RRC | +1.388 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 2 | CRK | +1.306 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | MOS | +1.266 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | ACMR | +1.252 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 5 | ELF | +1.248 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 6 | EPAM | +1.235 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 7 | CXT | +1.234 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 8 | XP | +1.229 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | MNDY | +1.209 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | VNT | +1.193 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up** |
| 11 | SLI | +1.183 | small | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | BFAM | +1.180 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 13 | WT | +1.172 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 14 | SFM | +1.172 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 15 | LW | +1.157 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 16 | ICL | +1.156 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 17 | OSK | +1.143 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 18 | TFPM | +1.138 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 19 | FIGR | +1.136 | mid | Financial | the Finviz industry was **advancing** |
| 20 | Z | +1.136 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 21 | KBR | +1.133 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 22 | HLNE | +1.132 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 23 | DNN | +1.127 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 24 | BRSL | +1.126 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 25 | ZG | +1.118 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | RRC | +1.386 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 2 | CRK | +1.365 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | MOS | +1.327 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | ELF | +1.310 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 5 | CXT | +1.301 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 6 | XP | +1.298 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | EPAM | +1.297 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 8 | ACMR | +1.287 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 9 | MNDY | +1.267 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | VNT | +1.255 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up** |
| 11 | SLI | +1.244 | small | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | WT | +1.242 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | BFAM | +1.238 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 14 | SFM | +1.232 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 15 | ICL | +1.215 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 16 | OSK | +1.210 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 17 | LW | +1.210 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 18 | ERO | +1.204 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 19 | FIGR | +1.198 | mid | Financial | the Finviz industry was **advancing** |
| 20 | HLNE | +1.196 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 21 | Z | +1.186 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 22 | KBR | +1.186 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 23 | DNN | +1.180 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 24 | BRSL | +1.178 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 25 | ZG | +1.171 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | CRK | +1.410 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 2 | RRC | +1.387 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 3 | MOS | +1.373 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | ELF | +1.354 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 5 | CXT | +1.345 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down** |
| 6 | XP | +1.345 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | EPAM | +1.338 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 8 | ACMR | +1.313 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 9 | MNDY | +1.310 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 10 | VNT | +1.294 | mid | Technology | this name **beat most of its own correlated peers** this week; the peer basket itself was **up** |
| 11 | WT | +1.289 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 12 | SLI | +1.286 | small | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | BFAM | +1.277 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 14 | SFM | +1.269 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 15 | OSK | +1.255 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 16 | ERO | +1.253 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 17 | ICL | +1.251 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 18 | LW | +1.242 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 19 | FIGR | +1.240 | mid | Financial | the Finviz industry was **advancing** |
| 20 | HLNE | +1.238 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 21 | Z | +1.224 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 22 | DNN | +1.218 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 23 | KBR | +1.216 | mid | Industrials | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 24 | BRSL | +1.216 | mid | Consumer Cyclical | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 25 | BZ | +1.212 | mid | Communication Services | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |

## 1m BUY — why these names

### 1. CRK · $4.3B mid · Energy

**1m score +1.475**

**CRK** is a liquid **mid-cap** Energy name (Oil & Gas E&P) at $4.3B, ADV ~2450k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.97 | +0.298 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.24 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.64 | +0.265 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.84 | +0.233 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.68 | +0.680 | liquid small/mid, room to run |
| **1m total** | | | **+1.475** | |

### 2. MOS · $7.7B mid · Basic Materials

**1m score +1.440**

**MOS** is a liquid **mid-cap** Basic Materials name (Agricultural Inputs) at $7.7B, ADV ~8758k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.301 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.32 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.70 | +0.293 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.74 | +0.205 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1m total** | | | **+1.440** | |

### 3. ELF · $6.3B mid · Consumer Defensive

**1m score +1.423**

**ELF** is a liquid **mid-cap** Consumer Defensive name (Household & Personal Products) at $6.3B, ADV ~3121k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.92 | +0.280 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.24 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.81 | +0.337 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.74 | +0.206 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.60 | +0.600 | liquid small/mid, room to run |
| **1m total** | | | **+1.423** | |

### 4. XP · $9.2B mid · Financial

**1m score +1.421**

**XP** is a liquid **mid-cap** Financial name (Capital Markets) at $9.2B, ADV ~5293k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.95 | +0.289 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.24 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.98 | +0.408 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.74 | +0.204 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1m total** | | | **+1.421** | |

### 5. CXT · $2.9B mid · Industrials

**1m score +1.418**

**CXT** is a liquid **mid-cap** Industrials name (Specialty Industrial Machinery) at $2.9B, ADV ~892k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.94 | +0.286 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.56 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.95 | +0.398 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.48 | +0.135 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.60 | +0.600 | liquid small/mid, room to run |
| **1m total** | | | **+1.418** | |

### 6. EPAM · $5.6B mid · Technology

**1m score +1.407**

**EPAM** is a liquid **mid-cap** Technology name (Information Technology Services) at $5.6B, ADV ~1882k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.84 | +0.256 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.40 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.94 | +0.392 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.42 | +0.118 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1m total** | | | **+1.407** | |

### 7. RRC · $9.7B mid · Energy

**1m score +1.384**

**RRC** is a liquid **mid-cap** Energy name (Oil & Gas E&P) at $9.7B, ADV ~3041k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Today's **news/judge** is a tailwind for this ticker. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.98 | +0.301 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.24 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.88 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.76 | +0.317 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.60 | +0.166 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.60 | +0.600 | liquid small/mid, room to run |
| **1m total** | | | **+1.384** | |

### 8. MNDY · $3.9B mid · Technology

**1m score +1.374**

**MNDY** is a liquid **mid-cap** Technology name (Software - Application) at $3.9B, ADV ~1578k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.98 | +0.300 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.40 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.70 | +0.293 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.36 | +0.101 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.68 | +0.680 | liquid small/mid, room to run |
| **1m total** | | | **+1.374** | |

### 9. WT · $3.8B mid · Financial

**1m score +1.366**

**WT** is a liquid **mid-cap** Financial name (Asset Management) at $3.8B, ADV ~2799k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.97 | +0.295 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.24 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.96 | +0.402 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.75 | +0.209 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.46 | +0.460 | liquid small/mid, room to run |
| **1m total** | | | **+1.366** | |

### 10. VNT · $4.6B mid · Technology

**1m score +1.362**

**VNT** is a liquid **mid-cap** Technology name (Scientific & Technical Instruments) at $4.6B, ADV ~1693k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.85 | +0.260 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.40 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.94 | +0.392 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.25 | +0.070 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1m total** | | | **+1.362** | |

### 11. PGY · $1.9B small · Technology

**1m score +1.360**

**PGY** is a liquid **small-cap** Technology name (Software - Infrastructure) at $1.9B, ADV ~3523k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.40 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.81 | +0.337 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.87 | +0.242 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.48 | +0.480 | liquid small/mid, room to run |
| **1m total** | | | **+1.360** | |

### 12. SLI · $644M small · Basic Materials

**1m score +1.353**

**SLI** is a liquid **small-cap** Basic Materials name (Other Industrial Metals & Mining) at $644M, ADV ~1765k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.84 | +0.255 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.32 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.81 | +0.337 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.86 | +0.240 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1m total** | | | **+1.353** | |

### 13. BFAM · $3.6B mid · Consumer Cyclical

**1m score +1.342**

**BFAM** is a liquid **mid-cap** Consumer Cyclical name (Personal Services) at $3.6B, ADV ~935k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.84 | +0.256 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.80 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.85 | +0.353 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.33 | +0.092 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1m total** | | | **+1.342** | |

### 14. SFM · $8.1B mid · Consumer Defensive

**1m score +1.334**

**SFM** is a liquid **mid-cap** Consumer Defensive name (Grocery Stores) at $8.1B, ADV ~2017k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.65 | +0.198 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.24 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.95 | +0.398 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.79 | +0.218 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1m total** | | | **+1.334** | |

### 15. ERO · $4.1B mid · Basic Materials

**1m score +1.331**

**ERO** is a liquid **mid-cap** Basic Materials name (Copper) at $4.1B, ADV ~1293k shares/day. Setup: already at the **top** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.32 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.98 | +0.410 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.79 | +0.220 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.40 | +0.400 | liquid small/mid, room to run |
| **1m total** | | | **+1.331** | |

### 16. OSK · $9.7B mid · Industrials

**1m score +1.328**

**OSK** is a liquid **mid-cap** Industrials name (Farm & Heavy Construction Machinery) at $9.7B, ADV ~748k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.96 | +0.293 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.56 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.95 | +0.398 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.42 | +0.117 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1m total** | | | **+1.328** | |

### 17. TFPM · $7.2B mid · Basic Materials

**1m score +1.319**

**TFPM** is a liquid **mid-cap** Basic Materials name (Other Precious Metals & Mining) at $7.2B, ADV ~647k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.32 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.96 | +0.402 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.06 | +0.016 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.60 | +0.600 | liquid small/mid, room to run |
| **1m total** | | | **+1.319** | |

### 18. FIGR · $8.3B mid · Financial

**1m score +1.308**

**FIGR** is a liquid **mid-cap** Financial name (Capital Markets) at $8.3B, ADV ~3958k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.99 | +0.302 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.24 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.00 | +0.000 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1m total** | | | **+1.308** | |

### 19. HLNE · $5.8B mid · Financial

**1m score +1.308**

**HLNE** is a liquid **mid-cap** Financial name (Asset Management) at $5.8B, ADV ~883k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.96 | +0.294 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.24 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.93 | +0.386 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.10 | +0.028 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.60 | +0.600 | liquid small/mid, room to run |
| **1m total** | | | **+1.308** | |

### 20. LW · $7.6B mid · Consumer Defensive

**1m score +1.301**

**LW** is a liquid **mid-cap** Consumer Defensive name (Packaged Foods) at $7.6B, ADV ~1578k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.59 | +0.179 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.24 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.88 | +0.367 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.56 | +0.155 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.60 | +0.600 | liquid small/mid, room to run |
| **1m total** | | | **+1.301** | |

### 21. Z · $7.9B mid · Communication Services

**1m score +1.280**

**Z** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $7.9B, ADV ~4334k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.91 | +0.277 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.55 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.33 | +0.092 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.68 | +0.680 | liquid small/mid, room to run |
| **1m total** | | | **+1.280** | |

### 22. DNN · $3.3B mid · Energy

**1m score +1.276**

**DNN** is a liquid **mid-cap** Energy name (Uranium) at $3.3B, ADV ~25228k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.71 | +0.218 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.24 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.70 | +0.293 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.88 | +0.245 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1m total** | | | **+1.276** | |

### 23. KBR · $4.8B mid · Industrials

**1m score +1.275**

**KBR** is a liquid **mid-cap** Industrials name (Engineering & Construction) at $4.8B, ADV ~1736k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.57 | +0.175 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.56 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.95 | +0.398 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.22 | +0.062 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1m total** | | | **+1.275** | |

### 24. BRSL · $2.1B mid · Consumer Cyclical

**1m score +1.274**

**BRSL** is a liquid **mid-cap** Consumer Cyclical name (Gambling) at $2.1B, ADV ~1720k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.90 | +0.275 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | -0.80 | -0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.64 | +0.265 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.19 | +0.053 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.68 | +0.680 | liquid small/mid, room to run |
| **1m total** | | | **+1.274** | |

### 25. BZ · $7.4B mid · Communication Services

**1m score +1.274**

**BZ** is a liquid **mid-cap** Communication Services name (Internet Content & Information) at $7.4B, ADV ~3876k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.31 | +0.94 | +0.289 | does this *kind* of stock fit today's regime? |
| sector predict | 0.00 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.00 | +0.00 | +0.000 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.42 | +0.55 | +0.231 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.28 | +0.99 | +0.274 | this week vs its correlated basket |
| map heat / captains | 1.00 | +0.00 | +0.000 | nested OVERRIDE + captain research (additive) |
| mid-cap opportunity | add | +0.48 | +0.480 | liquid small/mid, room to run |
| **1m total** | | | **+1.274** | |


## 1m AVOID — bottom of the same rank

- **QTRX** (micro, Healthcare, $127M) score -0.735. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **GLUE** (small, Healthcare, $1.2B) score -0.574. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NN** (mid, Technology, $2.6B) score -0.372. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **CTMX** (small, Healthcare, $723M) score -0.480. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **ACH** (micro, Healthcare, $94M) score -0.712. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**
- **AZI** (micro, Consumer Cyclical, $88M) score -0.694. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **EYPT** (small, Healthcare, $430M) score -0.434. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OTLK** (micro, Healthcare, $152M) score -0.670. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **HUMA** (micro, Healthcare, $189M) score -0.669. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **AXSM** (large, Healthcare, $10.9B) score -0.875. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SPRY** (small, Healthcare, $600M) score -0.423. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **IVVD** (micro, Healthcare, $266M) score -0.654. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ORGO** (micro, Healthcare, $225M) score -0.652. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**
- **GPCR** (mid, Healthcare, $3.5B) score -0.332. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ANNX** (small, Healthcare, $946M) score -0.491. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PUMP** (small, Energy, $1.3B) score -0.490. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ARDX** (small, Healthcare, $954M) score -0.409. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TSHA** (small, Healthcare, $2.0B) score -0.566. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FLNC** (mid, Utilities, $2.0B) score -0.240. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **NAMS** (mid, Healthcare, $3.3B) score -0.236. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **MRNA** (large, Healthcare, $59.7B) score -0.962. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **IBIO** (micro, Healthcare, $87M) score -0.631. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **VNDA** (small, Healthcare, $323M) score -0.390. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **YRD** (micro, Financial, $92M) score -0.626. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TU** (large, Communication Services, $15.4B) score -0.676. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**

## Files for this run

- This rationale: `01_daily/2026-08-27_stock_book.md`
- Machine table: `data/stock_book/2026-08-27_stock_book.csv`
- Machine book: `data/stock_book/2026-08-27_stock_book.json`
- Join rank: `data/join/2026-08-27_ranked.csv`
- Weather: `01_daily/weather/2026-08-27_weather.md`
- AB enrich: `data/ab_checklist/2026-08-27_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-08-27_peer_rs.md`
