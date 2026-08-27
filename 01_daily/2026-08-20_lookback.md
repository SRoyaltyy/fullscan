# Book lookback — 2026-08-20

_Generated 2026-08-27T06:04:46.547908-04:00_

Question: **on this trading day, before 09:30 ET, what did the pipeline
that feeds the Stock Book Ranker know about a name — and for names that
then went up 1d/2d/3d/1w, did ANY input fire?**

Honest timing: the ranker file itself is usually written **after the open**.
News / judge / digest / events / map-heat / predicts / catalyst are the
pre-09:30 packet. Join, AB, peers, weather may land later the same day
and still moved that day's book.

Classes: **in_buy_book** = ranker picked it · **outweighed** = something
fired but the name was not in a buy book · **gated_out** = micro / <$400M
· **blind** = no ticker-specific signal (news, AB, peers, heat, digest,
catalyst, volume spike).

## Inputs present for this date

| Resource | Found | Lands in |
|----------|-------|----------|
| Finviz Elite export | yes | liquidity + labels + AB proxy + digest |
| Labels / membership | yes | join + mid_opp + earnings/range |
| Weather (tape + FRED/DXY/VIX) | yes | join × weather |
| Channel 1 raw | yes | via weather |
| Join ranked universe | yes | s_join |
| News parse + actions | yes | s_news |
| News judge | yes | s_news ticker tilts |
| Finviz daily digest | yes | s_news company headlines |
| General predict | yes | s_general × beta |
| Sector LLM essays | yes | s_sector (0 if essays missing) |
| AB checklist + P01–P04 | yes | s_ab |
| Peer RS | yes | s_peer |
| Ticker checklist (rebound) | yes | rebound_floor (dated file, else latest — can be stale) |
| Event scanner | yes | sector tilt + weather |
| Map heat research | NO | s_heat nested override + captains |
| Catalyst overlays | NO | not in ranker — separate chart workflow |
| Insider / politician flow | NO | no daily file in repo |
| Industry predict | yes | not scored (ad-hoc only) |
| Learnings / mutable policy | yes | next predict prompt, not a ticker score |
| Catalyst dossiers | NO | pre-open layer 3; merged into news actions |
| Map heat tables | NO | pre-open overlay / post-close tables |
| Captain research | NO | s_heat captains — missing → bootstrap stub |
| Stock book CSV | yes | ranker snapshot (usually written after the open) |

## Requested tickers

### SLS · mid · Healthcare

**class: `outweighed`** · in universe: True · buy books: —

| 1d | 2d | 3d | 1w |
|----|----|----|----|
| +15.4% | n/a | n/a | n/a |

| Layer | Signal | When it lands |
|-------|-------:|---------------|
| join × weather | +0.45 | after open (join) |
| sector predict | +0.00 | pre-09:30 |
| general predict | +0.47 | pre-09:30 |
| news / judge | +0.00 | pre-09:30 |
| AB checklist | +0.64 | afternoon |
| peer RS | +0.95 | afternoon |
| map heat | +0.00 | post-close + morning delta |
| mid-opp | +0.28 | labels |

Ranker reasons: `join=+0.45; gen1d=+0.47; ab=+0.64; LEAD,peers↑,ind↑; peer=+0.95; mid_opp=+0.28`

**What fired before the ranker:**

- Finviz tape change=None relvol=0.88 spike=False (finviz_2026-08-20.csv)

### HOOD · large · Financial

**class: `outweighed`** · in universe: True · buy books: —

| 1d | 2d | 3d | 1w |
|----|----|----|----|
| +13.7% | n/a | n/a | n/a |

| Layer | Signal | When it lands |
|-------|-------:|---------------|
| join × weather | +0.90 | after open (join) |
| sector predict | +0.00 | pre-09:30 |
| general predict | +0.47 | pre-09:30 |
| news / judge | +0.00 | pre-09:30 |
| AB checklist | +0.36 | afternoon |
| peer RS | -0.64 | afternoon |
| map heat | +0.00 | post-close + morning delta |
| mid-opp | +0.07 | labels |

Ranker reasons: `join=+0.90; gen1d=+0.47; ab=+0.36; LAG,peers↑,ind↓; peer=-0.64; mid_opp=+0.07`

**What fired before the ranker:**

- Finviz tape change=None relvol=1.36 spike=False (finviz_2026-08-20.csv)

### ARCT · small · Healthcare

**class: `gated_out`** · in universe: True · buy books: —

| 1d | 2d | 3d | 1w |
|----|----|----|----|
| +22.4% | n/a | n/a | n/a |

| Layer | Signal | When it lands |
|-------|-------:|---------------|
| join × weather | +0.04 | after open (join) |
| sector predict | +0.00 | pre-09:30 |
| general predict | +0.47 | pre-09:30 |
| news / judge | +0.00 | pre-09:30 |
| AB checklist | +0.64 | afternoon |
| peer RS | +1.00 | afternoon |
| map heat | +0.00 | post-close + morning delta |
| mid-opp | +0.40 | labels |

Ranker reasons: `gen1d=+0.47; ab=+0.64; LEAD,peers↑,ind↑; peer=+1.00; mid_opp=+0.40`

**What fired before the ranker:**

- Finviz tape change=None relvol=4.53 spike=True (finviz_2026-08-20.csv)

### CAN · micro · Technology

**class: `gated_out`** · in universe: True · buy books: —

| 1d | 2d | 3d | 1w |
|----|----|----|----|
| +27.2% | n/a | n/a | n/a |

| Layer | Signal | When it lands |
|-------|-------:|---------------|
| join × weather | -0.63 | after open (join) |
| sector predict | +0.40 | pre-09:30 |
| general predict | +0.47 | pre-09:30 |
| news / judge | +0.00 | pre-09:30 |
| AB checklist | +0.24 | afternoon |
| peer RS | +1.00 | afternoon |
| map heat | +0.00 | post-close + morning delta |
| mid-opp | +0.16 | labels |

Ranker reasons: `join=-0.63; sector1d=+0.40; gen1d=+0.47; ab=+0.24; LEAD,peers↑,ind↓; peer=+1.00; mid_opp=+0.16`

**What fired before the ranker:**

- Finviz tape change=None relvol=11.27 spike=True (finviz_2026-08-20.csv)

### IQMX · small · Technology

**class: `blind`** · in universe: True · buy books: —

| 1d | 2d | 3d | 1w |
|----|----|----|----|
| +14.8% | n/a | n/a | n/a |

| Layer | Signal | When it lands |
|-------|-------:|---------------|
| join × weather | -0.77 | after open (join) |
| sector predict | +0.40 | pre-09:30 |
| general predict | +0.07 | pre-09:30 |
| news / judge | +0.00 | pre-09:30 |
| AB checklist | +0.00 | afternoon |
| peer RS | +0.00 | afternoon |
| map heat | +0.00 | post-close + morning delta |
| mid-opp | +0.40 | labels |

Ranker reasons: `join=-0.77; sector1d=+0.40; gen1d=+0.07; mid_opp=+0.40`

**What fired before the ranker:**

- Finviz tape change=None relvol=0.2 spike=False (finviz_2026-08-20.csv)

### HQ · small · Technology

**class: `blind`** · in universe: True · buy books: —

| 1d | 2d | 3d | 1w |
|----|----|----|----|
| +14.5% | n/a | n/a | n/a |

| Layer | Signal | When it lands |
|-------|-------:|---------------|
| join × weather | -0.83 | after open (join) |
| sector predict | +0.40 | pre-09:30 |
| general predict | +0.07 | pre-09:30 |
| news / judge | +0.00 | pre-09:30 |
| AB checklist | +0.00 | afternoon |
| peer RS | +0.00 | afternoon |
| map heat | +0.00 | post-close + morning delta |
| mid-opp | +0.40 | labels |

Ranker reasons: `join=-0.83; sector1d=+0.40; gen1d=+0.07; mid_opp=+0.40`

**What fired before the ranker:**

- Finviz tape change=None relvol=0.32 spike=False (finviz_2026-08-20.csv)

### ASST · small · Financial

**class: `outweighed`** · in universe: True · buy books: —

| 1d | 2d | 3d | 1w |
|----|----|----|----|
| +13.0% | n/a | n/a | n/a |

| Layer | Signal | When it lands |
|-------|-------:|---------------|
| join × weather | +0.30 | after open (join) |
| sector predict | +0.00 | pre-09:30 |
| general predict | +0.47 | pre-09:30 |
| news / judge | +0.00 | pre-09:30 |
| AB checklist | +0.00 | afternoon |
| peer RS | +1.00 | afternoon |
| map heat | +0.00 | post-close + morning delta |
| mid-opp | +0.40 | labels |

Ranker reasons: `join=+0.30; gen1d=+0.47; peer=+1.00; mid_opp=+0.40`

**What fired before the ranker:**

- Finviz tape change=None relvol=2.94 spike=True (finviz_2026-08-20.csv)

### UEC · mid · Energy

**class: `outweighed`** · in universe: True · buy books: —

| 1d | 2d | 3d | 1w |
|----|----|----|----|
| +14.4% | n/a | n/a | n/a |

| Layer | Signal | When it lands |
|-------|-------:|---------------|
| join × weather | +0.91 | after open (join) |
| sector predict | -0.80 | pre-09:30 |
| general predict | +0.23 | pre-09:30 |
| news / judge | +0.00 | pre-09:30 |
| AB checklist | -0.12 | afternoon |
| peer RS | +0.18 | afternoon |
| map heat | +0.00 | post-close + morning delta |
| mid-opp | +0.56 | labels |

Ranker reasons: `join=+0.91; sector1d=-0.80; gen1d=+0.23; ab=-0.12; LEAD,peers↓,ind↓; peer=+0.18; mid_opp=+0.56`

**What fired before the ranker:**

- Finviz tape change=None relvol=0.52 spike=False (finviz_2026-08-20.csv)

### PROK · small · Healthcare

**class: `outweighed`** · in universe: True · buy books: —

| 1d | 2d | 3d | 1w |
|----|----|----|----|
| +13.4% | n/a | n/a | n/a |

| Layer | Signal | When it lands |
|-------|-------:|---------------|
| join × weather | -0.10 | after open (join) |
| sector predict | +0.00 | pre-09:30 |
| general predict | +0.47 | pre-09:30 |
| news / judge | +0.00 | pre-09:30 |
| AB checklist | +0.46 | afternoon |
| peer RS | +0.22 | afternoon |
| map heat | +0.00 | post-close + morning delta |
| mid-opp | +0.40 | labels |

Ranker reasons: `gen1d=+0.47; ab=+0.46; LEAD,peers↑,ind↑; peer=+0.22; mid_opp=+0.40`

**What fired before the ranker:**

- Finviz tape change=None relvol=1.62 spike=True (finviz_2026-08-20.csv)

### CRML · small · Basic Materials

**class: `outweighed`** · in universe: True · buy books: —

| 1d | 2d | 3d | 1w |
|----|----|----|----|
| +22.6% | n/a | n/a | n/a |

| Layer | Signal | When it lands |
|-------|-------:|---------------|
| join × weather | +0.87 | after open (join) |
| sector predict | +0.00 | pre-09:30 |
| general predict | +0.47 | pre-09:30 |
| news / judge | +0.00 | pre-09:30 |
| AB checklist | -0.36 | afternoon |
| peer RS | -0.96 | afternoon |
| map heat | +0.00 | post-close + morning delta |
| mid-opp | +0.40 | labels |

Ranker reasons: `join=+0.87; gen1d=+0.47; ab=-0.36; LAG,peers↑,ind↓; peer=-0.96; mid_opp=+0.40`

**What fired before the ranker:**

- Finviz tape change=None relvol=1.29 spike=False (finviz_2026-08-20.csv)

### USDE · micro · Financial

**class: `gated_out`** · in universe: True · buy books: —

| 1d | 2d | 3d | 1w |
|----|----|----|----|
| +85.2% | n/a | n/a | n/a |

| Layer | Signal | When it lands |
|-------|-------:|---------------|
| join × weather | -0.26 | after open (join) |
| sector predict | +0.00 | pre-09:30 |
| general predict | +0.07 | pre-09:30 |
| news / judge | +0.00 | pre-09:30 |
| AB checklist | -0.12 | afternoon |
| peer RS | +0.00 | afternoon |
| map heat | +0.00 | post-close + morning delta |
| mid-opp | +0.16 | labels |

Ranker reasons: `join=-0.26; gen1d=+0.07; ab=-0.12; ind↓; mid_opp=+0.16`

**What fired before the ranker:**

- Finviz tape change=None relvol=2.35 spike=True (finviz_2026-08-20.csv)

### BKKT · small · Technology

**class: `gated_out`** · in universe: True · buy books: —

| 1d | 2d | 3d | 1w |
|----|----|----|----|
| +14.5% | n/a | n/a | n/a |

| Layer | Signal | When it lands |
|-------|-------:|---------------|
| join × weather | -0.15 | after open (join) |
| sector predict | +0.40 | pre-09:30 |
| general predict | +0.47 | pre-09:30 |
| news / judge | +0.00 | pre-09:30 |
| AB checklist | +0.24 | afternoon |
| peer RS | -0.08 | afternoon |
| map heat | +0.00 | post-close + morning delta |
| mid-opp | +0.52 | labels |

Ranker reasons: `join=-0.15; sector1d=+0.40; gen1d=+0.47; ab=+0.24; LAG,peers↑,ind↓; peer=-0.08; mid_opp=+0.52`

**What fired before the ranker:**

- Finviz tape change=None relvol=3.29 spike=True (finviz_2026-08-20.csv)


## Winners — did the ranker see *something*?

### 1d (next 1 session(s))

20 names ≥ threshold · 0 already in a buy book · **2 blind**.

| Ticker | fwd% | class | size | sector | what fired |
|--------|------|-------|------|--------|------------|
| USDE | +85.2 | gated_out | micro | Financial | s_ab=-0.12, relvol=2.35 |
| CAN | +27.2 | gated_out | micro | Technology | s_ab=+0.24, s_peer=+1.00, relvol=11.27 |
| CRML | +22.6 | outweighed | small | Basic Materials | s_ab=-0.36, s_peer=-0.96 |
| ARCT | +22.4 | gated_out | small | Healthcare | s_ab=+0.64, s_peer=+1.00, relvol=4.53 |
| TMC | +20.6 | outweighed | small | Basic Materials | s_ab=-0.24, s_peer=-0.97 |
| CYPH | +19.3 | gated_out | micro | Healthcare | s_ab=+0.76, s_peer=+1.00, relvol=2.92 |
| ELMT | +19.3 | outweighed | small | Industrials | s_ab=+0.46 |
| SLS | +15.4 | outweighed | mid | Healthcare | s_ab=+0.64, s_peer=+0.95 |
| AIFC | +15.2 | gated_out | micro | Technology | s_ab=+0.12, relvol=4.11 |
| IQMX | +14.8 | blind | small | Technology | — |
| TII | +14.6 | gated_out | micro | Basic Materials | s_ab=+0.24 |
| HQ | +14.5 | blind | small | Technology | — |
| BKKT | +14.5 | gated_out | small | Technology | s_ab=+0.24, s_peer=-0.08, relvol=3.29 |
| UEC | +14.4 | outweighed | mid | Energy | s_ab=-0.12, s_peer=+0.18 |
| EU | +14.3 | gated_out | micro | Energy | s_ab=-0.64, s_peer=-0.70 |
| LAR | +14.2 | outweighed | small | Basic Materials | s_ab=-0.55, s_peer=-0.76 |
| GUTS | +13.9 | gated_out | micro | Healthcare | s_ab=+0.36, s_peer=-0.54 |
| HOOD | +13.7 | outweighed | large | Financial | s_ab=+0.36, s_peer=-0.64 |
| PROK | +13.4 | outweighed | small | Healthcare | s_ab=+0.46, s_peer=+0.22, relvol=1.62 |
| ASST | +13.0 | outweighed | small | Financial | s_peer=+1.00, relvol=2.94 |

### 2d (next 2 session(s))

_No realized winners at this gain threshold (or prices not in yet)._

### 3d (next 3 session(s))

_No realized winners at this gain threshold (or prices not in yet)._

### 1w (next 5 session(s))

_No realized winners at this gain threshold (or prices not in yet)._

## Files

- `data/stock_book/2026-08-20_lookback.json`
- `01_daily/2026-08-20_lookback.md`
- `03_scoreboard/BOOK_LOOKBACK.md` (this report, latest run)

