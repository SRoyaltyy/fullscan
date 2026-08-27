# Book lookback — 2026-08-20

_Generated 2026-08-27T06:57:28.151400-04:00_

Winner bar: **5% per session** (1d ≥ 5% · 2d ≥ 10% · 3d ≥ 15% · 1w ≥ 25%).

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

Boxes: 🟢 good (helped / fired bullish) · 🟡 neutral (present, flat) · 🔴 bad (fired against the name) · ⬛ missing (that day's file was not there).

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

🟢join 🟡sect 🟢gen 🟡news 🟡dig 🟡jdg 🟢AB 🟢peer ⬛heat 🟡vol ⬛cat 🟡buy

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

🟢join 🟡sect 🟢gen 🟡news 🟡dig 🟡jdg 🟢AB 🔴peer ⬛heat 🟡vol ⬛cat 🟡buy

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

🟡join 🟡sect 🟢gen 🟡news 🟡dig 🟡jdg 🟢AB 🟢peer ⬛heat 🟢vol ⬛cat 🟡buy

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

🔴join 🟢sect 🟢gen 🟡news 🟡dig 🟡jdg 🟢AB 🟢peer ⬛heat 🟢vol ⬛cat 🟡buy

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

🔴join 🟢sect 🟢gen 🟡news 🟡dig 🟡jdg 🟡AB 🟡peer ⬛heat 🔴vol ⬛cat 🟡buy

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

🔴join 🟢sect 🟢gen 🟡news 🟡dig 🟡jdg 🟡AB 🟡peer ⬛heat 🔴vol ⬛cat 🟡buy

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

### ELF · mid · Consumer Defensive

**class: `in_buy_book`** · in universe: True · buy books: 1d, 3d, 1w, 2w, 1m

🟢join 🟡sect 🟢gen 🟡news 🟡dig 🟡jdg 🟢AB 🟢peer ⬛heat 🔴vol ⬛cat 🟢buy

| 1d | 2d | 3d | 1w |
|----|----|----|----|
| +3.5% | n/a | n/a | n/a |

| Layer | Signal | When it lands |
|-------|-------:|---------------|
| join × weather | +0.87 | after open (join) |
| sector predict | +0.00 | pre-09:30 |
| general predict | +0.47 | pre-09:30 |
| news / judge | +0.00 | pre-09:30 |
| AB checklist | +0.56 | afternoon |
| peer RS | +0.81 | afternoon |
| map heat | +0.00 | post-close + morning delta |
| mid-opp | +0.60 | labels |

Ranker reasons: `join=+0.87; gen1d=+0.47; ab=+0.55; LEAD,peers↓,ind↓; peer=+0.81; mid_opp=+0.60; rebound_floor`

**What fired before the ranker:**

- Finviz tape change=None relvol=0.49 spike=False (finviz_2026-08-20.csv)

### MOS · mid · Basic Materials

**class: `in_buy_book`** · in universe: True · buy books: 1d, 3d, 1w, 2w, 1m

🟢join 🟡sect 🟢gen 🟡news 🟡dig 🟡jdg 🟢AB 🟢peer ⬛heat 🟢vol ⬛cat 🟢buy

| 1d | 2d | 3d | 1w |
|----|----|----|----|
| +4.5% | n/a | n/a | n/a |

| Layer | Signal | When it lands |
|-------|-------:|---------------|
| join × weather | +0.98 | after open (join) |
| sector predict | +0.00 | pre-09:30 |
| general predict | +0.23 | pre-09:30 |
| news / judge | +0.00 | pre-09:30 |
| AB checklist | +0.64 | afternoon |
| peer RS | +0.05 | afternoon |
| map heat | +0.00 | post-close + morning delta |
| mid-opp | +0.68 | labels |

Ranker reasons: `join=+0.98; gen1d=+0.23; ab=+0.64; LEAD,peers↑,ind↑; peer=+0.05; mid_opp=+0.68; rebound_floor`

**What fired before the ranker:**

- Finviz tape change=None relvol=1.59 spike=True (finviz_2026-08-20.csv)

### INTC · mega · Technology

**class: `outweighed`** · in universe: True · buy books: —

🔴join 🟢sect 🟢gen 🔴news 🟢dig 🟡jdg 🔴AB 🔴peer ⬛heat 🟡vol ⬛cat 🟡buy

| 1d | 2d | 3d | 1w |
|----|----|----|----|
| -2.2% | n/a | n/a | n/a |

| Layer | Signal | When it lands |
|-------|-------:|---------------|
| join × weather | -0.89 | after open (join) |
| sector predict | +0.40 | pre-09:30 |
| general predict | +0.47 | pre-09:30 |
| news / judge | -0.31 | pre-09:30 |
| AB checklist | -0.12 | afternoon |
| peer RS | -0.84 | afternoon |
| map heat | +0.00 | post-close + morning delta |
| mid-opp | -0.14 | labels |

Ranker reasons: `join=-0.88; sector1d=+0.40; gen1d=+0.47; news=-0.31; ev=finviz_digest; ab=-0.12; LAG,peers↓,ind↓; peer=-0.84`

**What fired before the ranker:**

- Finviz digest: Intel shares fall about 7% after UBS cuts its price target to $112 from $121 while maintaining a Neutral rating.
- Finviz tape change=None relvol=0.94 spike=False (finviz_2026-08-20.csv)

### GEV · mega · Industrials

**class: `outweighed`** · in universe: True · buy books: —

🔴join 🔴sect 🟢gen 🟡news 🟡dig 🟡jdg 🔴AB 🔴peer ⬛heat 🔴vol ⬛cat 🟡buy

| 1d | 2d | 3d | 1w |
|----|----|----|----|
| -0.9% | n/a | n/a | n/a |

| Layer | Signal | When it lands |
|-------|-------:|---------------|
| join × weather | -0.89 | after open (join) |
| sector predict | -0.24 | pre-09:30 |
| general predict | +0.23 | pre-09:30 |
| news / judge | +0.00 | pre-09:30 |
| AB checklist | -0.24 | afternoon |
| peer RS | -0.69 | afternoon |
| map heat | +0.00 | post-close + morning delta |
| mid-opp | -0.22 | labels |

Ranker reasons: `join=-0.88; sector1d=-0.24; gen1d=+0.23; ab=-0.24; LAG,peers↓,ind↓; peer=-0.69`

**What fired before the ranker:**

- Finviz tape change=None relvol=0.66 spike=False (finviz_2026-08-20.csv)

### FIGR · mid · Financial

**class: `in_buy_book`** · in universe: True · buy books: 1d, 3d, 1w, 2w, 1m

🟢join 🟡sect 🟢gen 🟡news 🟡dig 🟡jdg 🟢AB 🟡peer ⬛heat 🟡vol ⬛cat 🟢buy

| 1d | 2d | 3d | 1w |
|----|----|----|----|
| +8.4% | n/a | n/a | n/a |

| Layer | Signal | When it lands |
|-------|-------:|---------------|
| join × weather | +0.89 | after open (join) |
| sector predict | +0.00 | pre-09:30 |
| general predict | +0.19 | pre-09:30 |
| news / judge | +0.00 | pre-09:30 |
| AB checklist | +0.56 | afternoon |
| peer RS | +0.00 | afternoon |
| map heat | +0.00 | post-close + morning delta |
| mid-opp | +0.68 | labels |

Ranker reasons: `join=+0.89; gen1d=+0.19; ab=+0.55; ind↓; mid_opp=+0.68`

**What fired before the ranker:**

- Finviz tape change=None relvol=0.93 spike=False (finviz_2026-08-20.csv)


## Winners — did the ranker see *something*?

### 1d (next 1 session(s), bar 5%)

20 names ≥ 5% · 0 already in a buy book · **2 blind**.

| Ticker | fwd | join | sect | gen | news | dig | jdg | AB | peer | heat | vol | cat | buy |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| USDE | +85.2% | 🔴 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🟡 | ⬛ | 🟢 | ⬛ | 🟡 |
| CAN | +27.2% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟡 |
| CRML | +22.6% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| ARCT | +22.4% | 🟡 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟡 |
| TMC | +20.6% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| CYPH | +19.3% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟡 |
| ELMT | +19.3% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟡 | ⬛ | 🔴 | ⬛ | 🟡 |
| SLS | +15.4% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟡 | ⬛ | 🟡 |
| AIFC | +15.2% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟡 | ⬛ | 🟢 | ⬛ | 🟡 |
| IQMX | +14.8% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟡 | 🟡 | ⬛ | 🔴 | ⬛ | 🟡 |
| TII | +14.6% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟡 | ⬛ | 🔴 | ⬛ | 🟡 |
| HQ | +14.5% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟡 | 🟡 | ⬛ | 🔴 | ⬛ | 🟡 |
| BKKT | +14.5% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🟢 | ⬛ | 🟡 |
| UEC | +14.4% | 🟢 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🟢 | ⬛ | 🔴 | ⬛ | 🟡 |
| EU | +14.3% | 🟢 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| LAR | +14.2% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| GUTS | +13.9% | 🔴 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| HOOD | +13.7% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| PROK | +13.4% | 🔴 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟡 |
| ASST | +13.0% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟡 | 🟢 | ⬛ | 🟢 | ⬛ | 🟡 |

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

### 2d (next 2 session(s), bar 10%)

_No realized winners at this gain threshold (or prices not in yet)._

### 3d (next 3 session(s), bar 15%)

_No realized winners at this gain threshold (or prices not in yet)._

### 1w (next 5 session(s), bar 25%)

_No realized winners at this gain threshold (or prices not in yet)._

## Book picks — scores that day

What the ranker actually put in buy/sell, with every layer score and the same color boxes.
`book` is the combined ranker score. 1d/2d/3d/1w are realized close-to-close after the signal.
1w uses the last available close if a full 5 sessions are not in yet.

### 1d book

**BUY** (15)

| # | Ticker | book | 1d | 2d | 3d | 1w | join | sect | gen | news | dig | jdg | AB | peer | heat | vol | cat | buy |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | ELF | +1.123 | +3.5% | +7.6% | +7.4% | +8.6% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| 2 | MOS | +1.066 | +4.5% | +2.8% | +3.9% | +3.5% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟢 |
| 3 | AUPH | +1.024 | -3.6% | -4.0% | -3.0% | -4.2% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟢 |
| 4 | CE | +1.020 | -0.4% | -3.7% | -5.2% | -4.8% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| 5 | OCUL | +1.019 | -0.1% | -2.8% | -1.9% | -2.9% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| 6 | EPAM | +1.015 | +3.4% | +4.2% | +4.8% | +2.4% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| 7 | WRBY | +1.005 | -0.1% | -4.5% | -5.3% | -7.2% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| 8 | CELH | +1.000 | +2.5% | +7.7% | +8.6% | +8.3% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| 9 | IRTC | +0.994 | +0.4% | -2.1% | -3.8% | -6.9% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| 10 | CALX | +0.986 | -0.6% | -1.4% | -1.3% | -5.0% | 🟡 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| 11 | NHI | +0.971 | -0.7% | -1.1% | -0.8% | -0.7% | 🟢 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| 12 | GSHD | +0.964 | +2.6% | +3.2% | +2.3% | +0.9% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| 13 | OGS | +0.945 | -3.1% | -1.3% | -1.2% | -0.4% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| 14 | FIGR | +0.941 | +8.4% | +7.2% | +14.1% | +2.8% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟡 | ⬛ | 🟡 | ⬛ | 🟢 |
| 15 | HLNE | +0.938 | +1.2% | +1.1% | +2.7% | +1.2% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |

| Ticker | size | sector | reasons |
|--------|------|--------|---------|
| ELF | mid | Consumer Defensive | `join=+0.87; gen1d=+0.47; ab=+0.55; LEAD,peers↓,ind↓; peer=+0.81; mid_opp=+0.60; rebound_floor` |
| MOS | mid | Basic Materials | `join=+0.98; gen1d=+0.23; ab=+0.64; LEAD,peers↑,ind↑; peer=+0.05; mid_opp=+0.68; rebound_floor` |
| AUPH | mid | Healthcare | `join=+0.93; gen1d=+0.47; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.76; mid_opp=+0.52` |
| CE | mid | Basic Materials | `join=+0.95; gen1d=+0.07; ab=+0.64; LEAD,peers↑,ind↑; peer=+0.51; mid_opp=+0.64` |
| OCUL | mid | Healthcare | `join=+0.54; gen1d=+0.23; ab=+0.70; LEAD,peers↑,ind↑; peer=+0.80; mid_opp=+0.60` |
| EPAM | mid | Technology | `join=-0.15; sector1d=+0.40; gen1d=+0.47; ab=+0.46; LEAD,peers↓,ind↓; peer=+0.80; mid_opp=+0.68` |
| WRBY | mid | Healthcare | `join=+0.90; gen1d=+0.47; ab=+0.76; LEAD,peers↓,ind↑; peer=+0.75; mid_opp=+0.52` |
| CELH | mid | Consumer Defensive | `join=+0.73; gen1d=+0.23; ab=+0.64; LEAD,peers↑,ind↑; peer=+0.87; mid_opp=+0.56` |
| IRTC | mid | Healthcare | `join=+0.82; gen1d=+0.47; ab=+0.46; LEAD,peers↓,ind↓; peer=+0.51; mid_opp=+0.64` |
| CALX | mid | Technology | `sector1d=+0.40; gen1d=+0.23; ab=+0.36; LEAD,peers↓,ind↓; peer=+0.78; mid_opp=+0.68` |
| NHI | mid | Real Estate | `join=+0.75; sector1d=-0.24; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.28; mid_opp=+0.64` |
| GSHD | mid | Financial | `join=+0.91; gen1d=+0.47; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.47; mid_opp=+0.52` |
| OGS | mid | Utilities | `join=+0.81; gen1d=+0.07; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.15; mid_opp=+0.60` |
| FIGR | mid | Financial | `join=+0.89; gen1d=+0.19; ab=+0.55; ind↓; mid_opp=+0.68` |
| HLNE | mid | Financial | `join=+0.95; gen1d=+0.23; ab=+0.46; LEAD,peers↓,ind↓; peer=+0.25; mid_opp=+0.64` |

**SELL** (15)

| # | Ticker | book | 1d | 2d | 3d | 1w | join | sect | gen | news | dig | jdg | AB | peer | heat | vol | cat | buy |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | GEV | -0.531 | -0.9% | -2.5% | -4.1% | -1.3% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| 2 | INTC | -0.446 | -2.2% | -5.3% | -5.0% | -4.2% | 🔴 | 🟢 | 🟢 | 🔴 | 🟢 | 🟡 | 🔴 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| 3 | AUR | -0.410 | +2.0% | -7.0% | -5.7% | -5.9% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| 4 | ARM | -0.381 | -3.0% | -4.8% | -3.6% | +0.1% | 🔴 | 🟢 | 🟢 | 🔴 | 🟢 | 🟡 | 🟡 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| 5 | META | -0.375 | +0.8% | +2.4% | +4.4% | +5.5% | 🔴 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| 6 | SW | -0.371 | +2.5% | +1.0% | +2.6% | +2.8% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| 7 | DELL | -0.370 | +1.7% | -0.4% | +3.9% | +6.7% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| 8 | BWEN | -0.349 | -1.1% | -2.4% | -2.9% | -2.7% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| 9 | PKG | -0.340 | +1.3% | -0.3% | -1.2% | -1.1% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟡 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| 10 | MDB | -0.326 | +2.5% | -4.2% | -3.7% | -3.4% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟡 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| 11 | RY | -0.323 | +0.0% | -0.6% | +0.9% | +1.0% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| 12 | PANW | -0.318 | +2.4% | +0.4% | -2.8% | -2.9% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| 13 | WMT | -0.312 | +0.1% | +2.8% | +1.7% | +0.7% | 🟢 | 🟡 | 🟢 | 🔴 | 🟡 | 🔴 | 🔴 | 🔴 | ⬛ | 🟢 | ⬛ | 🟡 |
| 14 | BIDU | -0.312 | +1.4% | +0.2% | +1.6% | +1.4% | 🔴 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟢 | ⬛ | 🟡 |
| 15 | SES | -0.308 | +6.1% | +6.1% | +7.1% | +5.3% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟢 | ⬛ | 🟡 |

| Ticker | size | sector | reasons |
|--------|------|--------|---------|
| GEV | mega | Industrials | `join=-0.88; sector1d=-0.24; gen1d=+0.23; ab=-0.24; LAG,peers↓,ind↓; peer=-0.69` |
| INTC | mega | Technology | `join=-0.88; sector1d=+0.40; gen1d=+0.47; news=-0.31; ev=finviz_digest; ab=-0.12; LAG,peers↓,ind↓; peer=-0.84` |
| AUR | large | Consumer Cyclical | `join=-0.96; sector1d=-0.80; gen1d=+0.47; ab=-0.46; LAG,peers↓,ind↓; peer=-0.83` |
| ARM | mega | Technology | `join=-0.70; sector1d=+0.40; gen1d=+0.47; news=-0.31; ev=finviz_digest; peer=-0.79` |
| META | mega | Communication Services | `join=-0.80; gen1d=+0.23; ab=-0.36; LAG,peers↓,ind↓; peer=-0.74` |
| SW | large | Consumer Cyclical | `join=-0.61; sector1d=-0.80; gen1d=+0.23; ab=-0.12; LAG,peers↓,ind↓; peer=-0.18` |
| DELL | mega | Technology | `join=-0.15; sector1d=+0.40; gen1d=+0.47; ab=+0.24; LAG,peers↓,ind↓; peer=-0.75` |
| BWEN | micro | Industrials | `join=-0.74; sector1d=-0.24; gen1d=+0.47; ab=-0.36; LAG,peers↓,ind↓; peer=-0.92` |
| PKG | large | Consumer Cyclical | `join=-0.63; sector1d=-0.80; gen1d=+0.23; peer=-0.16` |
| MDB | large | Technology | `join=-0.61; sector1d=+0.40; gen1d=+0.47; peer=-0.80` |
| RY | mega | Financial | `join=+0.59; gen1d=+0.07; ab=-0.12; LAG,peers↓,ind↓; peer=-0.14` |
| PANW | mega | Technology | `join=-0.45; sector1d=+0.40; gen1d=+0.23; ab=+0.12; LAG,peers↓,ind↓; peer=-0.06` |
| WMT | mega | Consumer Defensive | `join=+0.13; gen1d=+0.07; news=-0.29; ev=news_judge; ab=-0.12; LAG,peers↓,ind↓; peer=-0.85` |
| BIDU | large | Communication Services | `join=-0.95; gen1d=+0.07; ab=-0.55; LAG,peers↓,ind↓; peer=-0.87; mid_opp=+0.11` |
| SES | micro | Consumer Cyclical | `join=-0.71; sector1d=-0.80; gen1d=+0.23; ab=-0.55; LAG,peers↓,ind↓; peer=-0.91; mid_opp=+0.16` |

## Same 1d rippers — later sessions

Not a new hunt. These are the ≥5% next-day names, with 2d/3d/1w filled from live prices.

| Ticker | class | 1d | 2d | 3d | 1w |
|--------|-------|----|----|----|----|
| USDE | gated_out | +85.2% | +56.8% | +51.5% | +49.5% |
| CAN | gated_out | +27.2% | +29.0% | +49.5% | +42.6% |
| CRML | outweighed | +22.6% | +15.0% | +39.7% | +37.8% |
| ARCT | gated_out | +22.4% | +30.5% | +40.5% | +44.0% |
| TMC | outweighed | +20.6% | +19.3% | +24.9% | +26.2% |
| CYPH | gated_out | +19.3% | +41.2% | +37.8% | +37.0% |
| ELMT | outweighed | +19.3% | +5.9% | +6.5% | +7.7% |
| SLS | outweighed | +15.4% | +6.9% | +5.2% | +2.5% |
| AIFC | gated_out | +15.2% | +11.9% | +8.5% | +0.0% |
| IQMX | blind | +14.8% | +7.8% | +10.5% | +13.5% |
| TII | gated_out | +14.6% | +15.8% | +19.1% | +17.5% |
| HQ | blind | +14.5% | +11.7% | +11.4% | +1.4% |
| BKKT | gated_out | +14.5% | +9.1% | +15.1% | +9.7% |
| UEC | outweighed | +14.4% | +12.2% | +19.0% | +17.8% |
| EU | gated_out | +14.3% | +18.1% | +31.4% | +30.5% |
| LAR | outweighed | +14.2% | +12.0% | +12.4% | +12.2% |
| GUTS | gated_out | +13.9% | +9.5% | +14.4% | +7.6% |
| HOOD | outweighed | +13.7% | +9.0% | +17.9% | +14.1% |
| PROK | outweighed | +13.4% | +13.4% | +21.8% | +22.5% |
| ASST | outweighed | +13.0% | +22.3% | +32.6% | +33.3% |

## Files

- `data/stock_book/2026-08-20_lookback.json`
- `01_daily/2026-08-20_lookback.md`
- `03_scoreboard/BOOK_LOOKBACK.md` (this report, latest run)

