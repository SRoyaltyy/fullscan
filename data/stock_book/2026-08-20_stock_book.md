# Stock book — 2026-08-20

_Generated 2026-08-21T02:02:49.748453-04:00_

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
- Sector predicts this date: 0/11 (missing → sector layer is 0; Finviz week tape still sits in join)
- News tickers in play: 38
- AB coverage: 2425 names · peer RS: 2415
- Universe after liquidity: 2707
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega

### Sector LLM bias (1d) — 0 means that essay was not run today

| Sector | bias |
|--------|------|
| — | none today |

### How much each predictor is trusted (graded hit rate)

| Topic | hit rate | n | weight |
|-------|----------|---|--------|
| general | 50% | 18 | ×0.85 |
| sector:Basic Materials | 57% | 7 | ×1.00 |
| sector:Communication Services | 43% | 7 | ×0.50 |
| sector:Consumer Cyclical | 86% | 7 | ×1.00 |
| sector:Consumer Defensive | 57% | 7 | ×1.00 |
| sector:Energy | 71% | 7 | ×1.00 |
| sector:Financial | 43% | 7 | ×0.50 |
| sector:Healthcare | 71% | 7 | ×1.00 |
| sector:Industrials | 29% | 7 | ×0.50 |
| sector:Real Estate | 71% | 7 | ×1.00 |
| sector:Technology | 57% | 7 | ×1.00 |
| sector:Utilities | 57% | 7 | ×1.00 |

## Horizon weights

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.12 | 0.10 | 0.08 | 0.25 | 0.25 | 0.20 | additive |
| 3d | 0.16 | 0.14 | 0.08 | 0.16 | 0.26 | 0.20 | additive |
| 1w | 0.18 | 0.16 | 0.08 | 0.10 | 0.28 | 0.20 | additive |
| 2w | 0.20 | 0.18 | 0.08 | 0.06 | 0.28 | 0.20 | additive |
| 1m | 0.22 | 0.20 | 0.08 | 0.00 | 0.30 | 0.20 | additive |

## 1d BUY — why these names

### 1. ELF · $5.9B mid · Consumer Defensive

**1d score +1.123**

**ELF** is a liquid **mid-cap** Consumer Defensive name (Household & Personal Products) at $5.9B, ADV ~3458k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment. Checklist marks it as a **rebound-from-own-lows** candidate. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.87 | +0.104 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.47 | +0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.55 | +0.139 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.81 | +0.163 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.60 | +0.600 | liquid small/mid, room to run |
| rebound floor | add | +0.08 | +0.080 | tape at own-history low |
| **1d total** | | | **+1.123** | |

### 2. MOS · $7.3B mid · Basic Materials

**1d score +1.066**

**MOS** is a liquid **mid-cap** Basic Materials name (Agricultural Inputs) at $7.3B, ADV ~8610k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Checklist marks it as a **rebound-from-own-lows** candidate. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.98 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.23 | +0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.64 | +0.159 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.05 | +0.011 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.68 | +0.680 | liquid small/mid, room to run |
| rebound floor | add | +0.08 | +0.080 | tape at own-history low |
| **1d total** | | | **+1.066** | |

### 3. AUPH · $2.3B mid · Healthcare

**1d score +1.024**

**AUPH** is a liquid **mid-cap** Healthcare name (Biotechnology) at $2.3B, ADV ~1464k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.93 | +0.112 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.47 | +0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.76 | +0.153 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1d total** | | | **+1.024** | |

### 4. CE · $5.1B mid · Basic Materials

**1d score +1.020**

**CE** is a liquid **mid-cap** Basic Materials name (Chemicals) at $5.1B, ADV ~1819k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.95 | +0.114 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.07 | +0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.64 | +0.159 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.51 | +0.102 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1d total** | | | **+1.020** | |

### 5. OCUL · $2.5B mid · Healthcare

**1d score +1.019**

**OCUL** is a liquid **mid-cap** Healthcare name (Biotechnology) at $2.5B, ADV ~2732k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.54 | +0.065 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.23 | +0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.70 | +0.176 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.80 | +0.159 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.60 | +0.600 | liquid small/mid, room to run |
| **1d total** | | | **+1.019** | |

### 6. WRBY · $3.4B mid · Healthcare

**1d score +1.005**

**WRBY** is a liquid **mid-cap** Healthcare name (Medical Instruments & Supplies) at $3.4B, ADV ~3027k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.90 | +0.108 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.47 | +0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.76 | +0.190 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.75 | +0.149 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1d total** | | | **+1.005** | |

### 7. CELH · $8.2B mid · Consumer Defensive

**1d score +1.000**

**CELH** is a liquid **mid-cap** Consumer Defensive name (Beverages - Non-Alcoholic) at $8.2B, ADV ~10245k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **miss**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.73 | +0.088 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.23 | +0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.64 | +0.159 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.87 | +0.174 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.56 | +0.560 | liquid small/mid, room to run |
| **1d total** | | | **+1.000** | |

### 8. NHI · $3.7B mid · Real Estate

**1d score +0.995**

**NHI** is a liquid **mid-cap** Real Estate name (REIT - Healthcare Facilities) at $3.7B, ADV ~603k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.75 | +0.090 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.07 | +0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.28 | +0.056 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1d total** | | | **+0.995** | |

### 9. IRTC · $4.2B mid · Healthcare

**1d score +0.994**

**IRTC** is a liquid **mid-cap** Healthcare name (Medical Devices) at $4.2B, ADV ~611k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.82 | +0.098 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.47 | +0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.46 | +0.116 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.51 | +0.103 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1d total** | | | **+0.994** | |

### 10. NFG · $8.0B mid · Energy

**1d score +0.989**

**NFG** is a liquid **mid-cap** Energy name (Oil & Gas Integrated) at $8.0B, ADV ~724k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.07 | +0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.27 | +0.053 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.60 | +0.600 | liquid small/mid, room to run |
| **1d total** | | | **+0.989** | |

### 11. MTDR · $7.3B mid · Energy

**1d score +0.989**

**MTDR** is a liquid **mid-cap** Energy name (Oil & Gas E&P) at $7.3B, ADV ~1977k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.99 | +0.118 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.07 | +0.006 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.85 | +0.212 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.66 | +0.133 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1d total** | | | **+0.989** | |

### 12. EPAM · $5.6B mid · Technology

**1d score +0.975**

**EPAM** is a liquid **mid-cap** Technology name (Information Technology Services) at $5.6B, ADV ~1913k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | -0.15 | -0.018 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.47 | +0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.46 | +0.116 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.80 | +0.160 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.68 | +0.680 | liquid small/mid, room to run |
| **1d total** | | | **+0.975** | |

### 13. GSHD · $2.5B mid · Financial

**1d score +0.964**

**GSHD** is a liquid **mid-cap** Financial name (Insurance Brokers) at $2.5B, ADV ~567k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.91 | +0.110 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.47 | +0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.81 | +0.202 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.47 | +0.095 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1d total** | | | **+0.964** | |

### 14. BOOT · $4.9B mid · Consumer Cyclical

**1d score +0.947**

**BOOT** is a liquid **mid-cap** Consumer Cyclical name (Apparel Retail) at $4.9B, ADV ~718k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.21 | +0.025 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.47 | +0.037 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.55 | +0.139 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.73 | +0.146 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.60 | +0.600 | liquid small/mid, room to run |
| **1d total** | | | **+0.947** | |

### 15. CALX · $2.5B mid · Technology

**1d score +0.946**

**CALX** is a liquid **mid-cap** Technology name (Software - Infrastructure) at $2.5B, ADV ~1182k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.12 | +0.01 | +0.001 | does this *kind* of stock fit today's regime? |
| sector predict | 0.10 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.23 | +0.019 | same-day SPX call × this stock's beta |
| news / judge | 0.25 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.25 | +0.36 | +0.090 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.78 | +0.157 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.68 | +0.680 | liquid small/mid, room to run |
| **1d total** | | | **+0.946** | |


## 1d AVOID — bottom of the same rank

- **GEV** (mega, Industrials, $254.6B) score -0.507. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AVGO** (mega, Technology, $1729.5B) score -0.424. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **DELL** (mega, Technology, $281.3B) score -0.410. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **INTC** (mega, Technology, $480.3B) score -0.408. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **META** (mega, Communication Services, $1385.4B) score -0.375. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **MDB** (large, Technology, $34.0B) score -0.366. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PANW** (mega, Technology, $287.2B) score -0.358. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SNOW** (large, Technology, $110.9B) score -0.346. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ARM** (mega, Technology, $262.8B) score -0.344. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AUR** (large, Consumer Cyclical, $12.2B) score -0.330. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BWEN** (micro, Industrials, $104M) score -0.325. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RY** (mega, Financial, $283.2B) score -0.323. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **WMT** (mega, Consumer Defensive, $819.7B) score -0.312. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BIDU** (large, Communication Services, $25.4B) score -0.312. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **VSAT** (large, Technology, $10.2B) score -0.308. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## 3d BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | ELF | +1.126 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 2 | MOS | +1.093 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | CE | +1.059 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | AUPH | +1.032 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | NFG | +1.031 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | MTDR | +1.031 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | OCUL | +1.029 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | NHI | +1.028 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | CELH | +1.017 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | WRBY | +1.012 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 11 | IRTC | +0.994 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 12 | SM | +0.982 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | OGS | +0.980 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 14 | TALO | +0.976 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 15 | GSHD | +0.971 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 1w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | ELF | +1.154 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 2 | MOS | +1.125 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | CE | +1.090 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | NFG | +1.068 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | MTDR | +1.068 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 6 | AUPH | +1.067 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | NHI | +1.059 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | OCUL | +1.054 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | WRBY | +1.045 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 10 | CELH | +1.044 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | IRTC | +1.019 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 12 | SM | +1.018 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | OGS | +1.013 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 14 | TALO | +1.011 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 15 | GSHD | +1.006 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 2w BUY (compact — same names, different weights)

| # | Ticker | Score | Size | Sector | Why in short |
|---|--------|------:|------|--------|--------------|
| 1 | ELF | +1.206 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 2 | MOS | +1.162 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 3 | AUPH | +1.119 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 4 | CE | +1.114 | mid | Basic Materials | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 5 | WRBY | +1.097 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing** |
| 6 | NFG | +1.093 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 7 | MTDR | +1.093 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 8 | OCUL | +1.082 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 9 | NHI | +1.079 | mid | Real Estate | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 10 | CELH | +1.075 | mid | Consumer Defensive | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 11 | IRTC | +1.070 | mid | Healthcare | this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down** |
| 12 | GSHD | +1.058 | mid | Financial | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 13 | SM | +1.043 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 14 | TALO | +1.036 | mid | Energy | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |
| 15 | OGS | +1.035 | mid | Utilities | this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing** |

## 1m BUY — why these names

### 1. ELF · $5.9B mid · Consumer Defensive

**1m score +1.234**

**ELF** is a liquid **mid-cap** Consumer Defensive name (Household & Personal Products) at $5.9B, ADV ~3458k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment. Checklist marks it as a **rebound-from-own-lows** candidate. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.87 | +0.191 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.42 | +0.034 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.55 | +0.166 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.81 | +0.163 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.60 | +0.600 | liquid small/mid, room to run |
| rebound floor | add | +0.08 | +0.080 | tape at own-history low |
| **1m total** | | | **+1.234** | |

### 2. MOS · $7.3B mid · Basic Materials

**1m score +1.194**

**MOS** is a liquid **mid-cap** Basic Materials name (Agricultural Inputs) at $7.3B, ADV ~8610k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Checklist marks it as a **rebound-from-own-lows** candidate. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.98 | +0.216 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.21 | +0.017 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.64 | +0.191 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.05 | +0.011 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.68 | +0.680 | liquid small/mid, room to run |
| rebound floor | add | +0.08 | +0.080 | tape at own-history low |
| **1m total** | | | **+1.194** | |

### 3. AUPH · $2.3B mid · Healthcare

**1m score +1.154**

**AUPH** is a liquid **mid-cap** Healthcare name (Biotechnology) at $2.3B, ADV ~1464k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.93 | +0.205 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.42 | +0.034 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.76 | +0.153 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1m total** | | | **+1.154** | |

### 4. CE · $5.1B mid · Basic Materials

**1m score +1.146**

**CE** is a liquid **mid-cap** Basic Materials name (Chemicals) at $5.1B, ADV ~1819k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.95 | +0.208 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.06 | +0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.64 | +0.191 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.51 | +0.102 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1m total** | | | **+1.146** | |

### 5. WRBY · $3.4B mid · Healthcare

**1m score +1.130**

**WRBY** is a liquid **mid-cap** Healthcare name (Medical Instruments & Supplies) at $3.4B, ADV ~3027k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.90 | +0.198 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.42 | +0.034 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.75 | +0.149 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1m total** | | | **+1.130** | |

### 6. NFG · $8.0B mid · Energy

**1m score +1.130**

**NFG** is a liquid **mid-cap** Energy name (Oil & Gas Integrated) at $8.0B, ADV ~724k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.06 | +0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.27 | +0.053 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.60 | +0.600 | liquid small/mid, room to run |
| **1m total** | | | **+1.130** | |

### 7. MTDR · $7.3B mid · Energy

**1m score +1.129**

**MTDR** is a liquid **mid-cap** Energy name (Oil & Gas E&P) at $7.3B, ADV ~1977k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.06 | +0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.66 | +0.133 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1m total** | | | **+1.129** | |

### 8. NHI · $3.7B mid · Real Estate

**1m score +1.110**

**NHI** is a liquid **mid-cap** Real Estate name (REIT - Healthcare Facilities) at $3.7B, ADV ~603k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **downtrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.75 | +0.166 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.06 | +0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.28 | +0.056 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1m total** | | | **+1.110** | |

### 9. OCUL · $2.5B mid · Healthcare

**1m score +1.107**

**OCUL** is a liquid **mid-cap** Healthcare name (Biotechnology) at $2.5B, ADV ~2732k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.54 | +0.119 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.21 | +0.017 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.70 | +0.211 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.80 | +0.159 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.60 | +0.600 | liquid small/mid, room to run |
| **1m total** | | | **+1.107** | |

### 10. CELH · $8.2B mid · Consumer Defensive

**1m score +1.103**

**CELH** is a liquid **mid-cap** Consumer Defensive name (Beverages - Non-Alcoholic) at $8.2B, ADV ~10245k shares/day. Setup: still in the **deep low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **miss**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.73 | +0.161 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.21 | +0.017 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.64 | +0.191 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.87 | +0.174 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.56 | +0.560 | liquid small/mid, room to run |
| **1m total** | | | **+1.103** | |

### 11. IRTC · $4.2B mid · Healthcare

**1m score +1.095**

**IRTC** is a liquid **mid-cap** Healthcare name (Medical Devices) at $4.2B, ADV ~611k shares/day. Setup: still in the **low** of its 52-week range (room left), tape is **mixed** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.82 | +0.180 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.42 | +0.034 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.46 | +0.139 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.51 | +0.103 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.64 | +0.640 | liquid small/mid, room to run |
| **1m total** | | | **+1.095** | |

### 12. GSHD · $2.5B mid · Financial

**1m score +1.093**

**GSHD** is a liquid **mid-cap** Financial name (Insurance Brokers) at $2.5B, ADV ~567k shares/day. Setup: tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.91 | +0.201 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.42 | +0.034 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.47 | +0.095 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.52 | +0.520 | liquid small/mid, room to run |
| **1m total** | | | **+1.093** | |

### 13. SM · $9.0B mid · Energy

**1m score +1.079**

**SM** is a liquid **mid-cap** Energy name (Oil & Gas E&P) at $9.0B, ADV ~3926k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.06 | +0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.81 | +0.243 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.77 | +0.154 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.46 | +0.460 | liquid small/mid, room to run |
| **1m total** | | | **+1.079** | |

### 14. TALO · $3.0B mid · Energy

**1m score +1.071**

**TALO** is a liquid **mid-cap** Energy name (Oil & Gas E&P) at $3.0B, ADV ~1993k shares/day. Setup: already at the **breakout** of the 52-week range (less upside left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.99 | +0.217 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.06 | +0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.76 | +0.228 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.80 | +0.160 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.46 | +0.460 | liquid small/mid, room to run |
| **1m total** | | | **+1.071** | |

### 15. OGS · $5.2B mid · Utilities

**1m score +1.068**

**OGS** is a liquid **mid-cap** Utilities name (Utilities - Regulated Gas) at $5.2B, ADV ~643k shares/day. Setup: still in the **mid** of its 52-week range (room left), tape is **uptrend** (50/200DMA), extension **neutral**. Last earnings were a **big beat**. AB/peer context: this name **beat most of its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **advancing**. Labels × today's weather **fit** this environment. Opportunity tilt applies: this is the **BB-class** bucket (liquid small/mid, not a mega clone).

| Layer | Weight | Signal | Contribution | Means |
|-------|-------:|-------:|-------------:|-------|
| join × weather | 0.22 | +0.81 | +0.178 | does this *kind* of stock fit today's regime? |
| sector predict | 0.20 | +0.00 | +0.000 | same-day sector LLM, 0 if that file is missing |
| general predict | 0.08 | +0.06 | +0.005 | same-day SPX call × this stock's beta |
| news / judge | 0.00 | +0.00 | +0.000 | headlines + news-judge ticker tilts |
| AB checklist | 0.30 | +0.85 | +0.254 | structure + P01–P04 peer/industry/sector |
| peer RS | 0.20 | +0.15 | +0.031 | this week vs its correlated basket |
| mid-cap opportunity | add | +0.60 | +0.600 | liquid small/mid, room to run |
| **1m total** | | | **+1.068** | |


## 1m AVOID — bottom of the same rank

- **GEV** (mega, Industrials, $254.6B) score -0.609. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AVGO** (mega, Technology, $1729.5B) score -0.516. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **INTC** (mega, Technology, $480.3B) score -0.506. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **META** (mega, Communication Services, $1385.4B) score -0.475. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AUR** (large, Consumer Cyclical, $12.2B) score -0.453. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BIDU** (large, Communication Services, $25.4B) score -0.436. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **MDB** (large, Technology, $34.0B) score -0.431. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AMPG** (micro, Technology, $103M) score -0.430. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **VSAT** (large, Technology, $10.2B) score -0.423. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **TIGO** (large, Communication Services, $15.3B) score -0.421. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BWEN** (micro, Industrials, $104M) score -0.420. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **ARM** (mega, Technology, $262.8B) score -0.417. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **DELL** (mega, Technology, $281.3B) score -0.416. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PANW** (mega, Technology, $287.2B) score -0.399. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RKLB** (large, Industrials, $43.1B) score -0.393. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-08-20_stock_book.md`
- Machine table: `data/stock_book/2026-08-20_stock_book.csv`
- Machine book: `data/stock_book/2026-08-20_stock_book.json`
- Join rank: `data/join/2026-08-20_ranked.csv`
- Weather: `01_daily/weather/2026-08-20_weather.md`
- AB enrich: `data/ab_checklist/2026-08-20_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-08-20_peer_rs.md`
