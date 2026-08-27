# Book lookback — 2026-08-20

_Generated 2026-08-27T07:50:38.380749-04:00_

Winner bar: **3% per session** (1d ≥ 3% · 2d ≥ 6% · 3d ≥ 9% · 1w ≥ 15%).

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

### 1d (next 1 session(s), bar 3%)

80 names ≥ 3% · 1 already in a buy book · **3 blind**.

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
| ALOY | +12.9% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🟡 | ⬛ | 🟡 | ⬛ | 🟡 |
| USAR | +12.6% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🟡 | ⬛ | 🟡 | ⬛ | 🟡 |
| OI | +12.5% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| INFQ | +12.5% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🟡 | ⬛ | 🔴 | ⬛ | 🟡 |
| EZPW | +12.0% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟡 |
| DEFT | +11.7% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟡 | ⬛ | 🟡 | ⬛ | 🟡 |
| DK | +11.6% | 🟢 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| SGML | +11.6% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| RGTI | +11.5% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| DNN | +11.5% | 🟢 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟡 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| VIRT | +11.0% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟡 |
| BLMN | +11.0% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟡 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| LPTH | +10.8% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🟢 | ⬛ | 🔴 | ⬛ | 🟡 |
| JELD | +10.6% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟡 | ⬛ | 🟡 |
| ABAT | +10.3% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| CVI | +10.3% | 🟢 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟡 |
| EOSE | +10.1% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟢 | ⬛ | 🟡 |
| SID | +10.1% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🟢 | ⬛ | 🔴 | ⬛ | 🟡 |
| SENS | +10.1% | 🟡 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟡 | 🟢 | ⬛ | 🟡 | ⬛ | 🟡 |
| BCAR | +9.9% | 🟡 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🟡 | ⬛ | 🔴 | ⬛ | 🟡 |
| FSLY | +9.9% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟡 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| PACB | +9.8% | 🟡 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟡 | ⬛ | 🟡 |
| FUTU | +9.7% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟡 |
| QUBT | +9.6% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| IE | +9.5% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| NEXA | +9.4% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| ERO | +9.4% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| CHGG | +9.3% | 🔴 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| QMLS | +9.3% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟡 | 🟡 | ⬛ | 🟡 | ⬛ | 🟡 |
| MB | +9.3% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🟡 | ⬛ | 🔴 | ⬛ | 🟡 |
| SLI | +9.2% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| RZLT | +9.2% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟡 |
| ABUS | +9.2% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟡 |
| MP | +9.1% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟡 | 🟡 | ⬛ | 🟡 | ⬛ | 🟡 |
| TMQ | +9.1% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| PARR | +9.1% | 🟢 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| TEM | +9.1% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟡 |
| BTDR | +9.0% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟡 |
| UUUU | +8.9% | 🟢 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| BGC | +8.9% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟡 | ⬛ | 🔴 | ⬛ | 🟡 |
| PSNL | +8.9% | 🔴 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟡 |
| MRNA | +8.9% | 🔴 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟡 |
| ACDC | +8.8% | 🟡 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| NMG | +8.7% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| SCCO | +8.7% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| EROC | +8.6% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟡 | ⬛ | 🟡 | ⬛ | 🟡 |
| QMCO | +8.6% | 🟡 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| AXGN | +8.5% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| QBTS | +8.5% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| FIGR | +8.4% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟡 | ⬛ | 🟡 | ⬛ | 🟢 |
| CNH | +8.4% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟡 |
| XNDU | +8.3% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟡 | ⬛ | 🔴 | ⬛ | 🟡 |
| TRLV | +8.3% | 🟡 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟡 | ⬛ | 🟡 | ⬛ | 🟡 |
| CSAN | +8.3% | 🟢 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟡 | 🔴 | ⬛ | 🟢 | ⬛ | 🟡 |
| ONT | +8.2% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🟡 | ⬛ | 🔴 | ⬛ | 🟡 |
| COIN | +8.2% | 🔴 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟡 |
| DCH | +8.2% | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟡 | ⬛ | 🔴 | ⬛ | 🟡 |
| NB | +8.2% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| IONQ | +8.0% | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| AGEN | +8.0% | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟡 |

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
| ALOY | +12.9 | outweighed | small | Basic Materials | s_ab=-0.24 |
| USAR | +12.6 | outweighed | mid | Basic Materials | s_ab=-0.36 |
| OI | +12.5 | outweighed | small | Consumer Cyclical | s_ab=-0.70, s_peer=-0.68 |
| INFQ | +12.5 | outweighed | mid | Technology | s_ab=-0.12 |
| EZPW | +12.0 | outweighed | small | Financial | s_ab=+0.36, s_peer=+0.35 |
| DEFT | +11.7 | gated_out | micro | Financial | s_ab=+0.46 |
| DK | +11.6 | outweighed | mid | Energy | s_ab=+0.64, s_peer=-0.77 |
| SGML | +11.6 | outweighed | small | Basic Materials | s_ab=-0.36, s_peer=-0.65 |
| RGTI | +11.5 | outweighed | mid | Technology | s_ab=-0.46, s_peer=-0.89 |
| DNN | +11.5 | outweighed | mid | Energy | s_peer=-0.21 |
| VIRT | +11.0 | outweighed | mid | Financial | s_ab=+0.64, s_peer=+0.50, relvol=3.82 |
| BLMN | +11.0 | outweighed | small | Consumer Cyclical | s_peer=-0.09 |
| LPTH | +10.8 | outweighed | small | Technology | s_ab=-0.24, s_peer=+0.62 |
| JELD | +10.6 | gated_out | micro | Industrials | s_ab=+0.12, s_peer=+0.69 |
| ABAT | +10.3 | gated_out | small | Industrials | s_ab=-0.36, s_peer=-0.76 |
| CVI | +10.3 | outweighed | mid | Energy | s_ab=+0.70, s_peer=+0.10 |
| EOSE | +10.1 | outweighed | small | Industrials | s_ab=-0.70, s_peer=-0.95, relvol=1.5 |
| SID | +10.1 | outweighed | small | Basic Materials | s_ab=-0.36, s_peer=+0.74 |
| SENS | +10.1 | outweighed | small | Healthcare | s_peer=+0.97 |
| BCAR | +9.9 | gated_out | micro | Financial | s_ab=-0.12 |
| FSLY | +9.9 | outweighed | mid | Technology | s_peer=-0.98 |
| PACB | +9.8 | gated_out | small | Healthcare | s_ab=+0.24, s_peer=+0.42 |
| FUTU | +9.7 | outweighed | large | Financial | s_ab=+0.55, s_peer=+0.79, relvol=2.22 |
| QUBT | +9.6 | outweighed | small | Technology | s_ab=-0.55, s_peer=-0.44 |
| IE | +9.5 | outweighed | small | Basic Materials | s_ab=+0.12, s_peer=-0.63 |
| NEXA | +9.4 | outweighed | small | Basic Materials | s_ab=-0.12, s_peer=-0.65 |
| ERO | +9.4 | outweighed | mid | Basic Materials | s_ab=+0.70, s_peer=-0.35 |
| CHGG | +9.3 | gated_out | micro | Consumer Defensive | s_ab=-0.46, s_peer=-0.63 |
| QMLS | +9.3 | gated_out | micro | Technology | — |
| MB | +9.3 | gated_out | micro | Consumer Cyclical | s_ab=-0.36 |
| SLI | +9.2 | outweighed | small | Basic Materials | s_ab=+0.24, s_peer=-0.36 |
| RZLT | +9.2 | outweighed | small | Healthcare | s_ab=+0.76, s_peer=+0.17 |
| ABUS | +9.2 | outweighed | small | Healthcare | s_ab=+0.70, s_peer=+0.09 |
| MP | +9.1 | blind | mid | Basic Materials | — |
| TMQ | +9.1 | outweighed | small | Basic Materials | s_ab=-0.24, s_peer=-0.88 |
| PARR | +9.1 | outweighed | mid | Energy | s_ab=+0.64, s_peer=-0.94 |
| TEM | +9.1 | outweighed | large | Healthcare | s_ab=+0.55, s_peer=+0.96, relvol=5.44 |
| BTDR | +9.0 | outweighed | mid | Technology | s_ab=+0.24, s_peer=+0.95, relvol=2.78 |
| UUUU | +8.9 | outweighed | mid | Energy | s_ab=-0.46, s_peer=-0.34 |
| BGC | +8.9 | outweighed | mid | Financial | s_ab=+0.46 |
| PSNL | +8.9 | outweighed | small | Healthcare | s_ab=+0.46, s_peer=+0.81, relvol=6.42 |
| MRNA | +8.9 | outweighed | large | Healthcare | s_ab=+0.46, s_peer=+1.00, relvol=13.48 |
| ACDC | +8.8 | outweighed | small | Energy | s_ab=-0.64, s_peer=-0.98 |
| NMG | +8.7 | outweighed | small | Basic Materials | s_ab=+0.24, s_peer=-0.92 |
| SCCO | +8.7 | outweighed | large | Basic Materials | s_ab=+0.46, s_peer=-0.34 |
| EROC | +8.6 | outweighed | mid | Industrials | s_ab=+0.46 |
| QMCO | +8.6 | outweighed | small | Technology | s_ab=+0.12, s_peer=-0.92 |
| AXGN | +8.5 | outweighed | mid | Healthcare | s_ab=+0.12, s_peer=-0.63 |
| QBTS | +8.5 | outweighed | mid | Technology | s_ab=-0.46, s_peer=-0.72 |
| FIGR | +8.4 | in_buy_book | mid | Financial | s_ab=+0.55 |
| CNH | +8.4 | outweighed | large | Industrials | s_ab=+0.55, s_peer=+0.57, relvol=2.61 |
| XNDU | +8.3 | outweighed | mid | Technology | s_ab=+0.12 |
| TRLV | +8.3 | outweighed | small | Healthcare | s_ab=+0.36 |
| CSAN | +8.3 | outweighed | mid | Energy | s_peer=-0.41, relvol=2.92 |
| ONT | +8.2 | outweighed | small | Industrials | s_ab=-0.55 |
| COIN | +8.2 | outweighed | large | Financial | s_ab=+0.46, s_peer=+0.85, relvol=3.1 |
| DCH | +8.2 | outweighed | small | Consumer Cyclical | s_ab=+0.24 |
| NB | +8.2 | outweighed | small | Basic Materials | s_ab=+0.12, s_peer=-0.99 |
| IONQ | +8.0 | outweighed | large | Technology | s_ab=-0.55, s_peer=-0.36 |
| AGEN | +8.0 | gated_out | small | Healthcare | s_ab=+0.70, s_peer=+0.65 |

### 2d (next 2 session(s), bar 6%)

_No realized winners at this gain threshold (or prices not in yet)._

### 3d (next 3 session(s), bar 9%)

_No realized winners at this gain threshold (or prices not in yet)._

### 1w (next 5 session(s), bar 15%)

_No realized winners at this gain threshold (or prices not in yet)._

## Book picks — scores that day

What the ranker actually put in buy/sell, with every layer score and the same color boxes.
`book` is the combined ranker score. 1d/2d/3d/1w are realized close-to-close after the signal.
1w uses the last available close if a full 5 sessions are not in yet.

### 1d book

**BUY** (15)

| # | Ticker | book | join | sect | gen | news | AB | peer | heat | 1d | 2d | 3d | 1w |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | ELF | +1.123 | +0.87 | +0.00 | +0.47 | +0.00 | +0.56 | +0.81 | +0.00 | +3.5% | +7.6% | +7.4% | +8.6% |
| 2 | MOS | +1.066 | +0.98 | +0.00 | +0.23 | +0.00 | +0.64 | +0.05 | +0.00 | +4.5% | +2.8% | +3.9% | +3.5% |
| 3 | AUPH | +1.024 | +0.93 | +0.00 | +0.47 | +0.00 | +0.81 | +0.76 | +0.00 | -3.6% | -4.0% | -3.0% | -4.2% |
| 4 | CE | +1.020 | +0.95 | +0.00 | +0.07 | +0.00 | +0.64 | +0.51 | +0.00 | -0.4% | -3.7% | -5.2% | -4.8% |
| 5 | OCUL | +1.019 | +0.54 | +0.00 | +0.23 | +0.00 | +0.70 | +0.80 | +0.00 | -0.1% | -2.8% | -1.9% | -2.9% |
| 6 | EPAM | +1.015 | -0.15 | +0.40 | +0.47 | +0.00 | +0.46 | +0.80 | +0.00 | +3.4% | +4.2% | +4.8% | +2.4% |
| 7 | WRBY | +1.005 | +0.90 | +0.00 | +0.47 | +0.00 | +0.76 | +0.75 | +0.00 | -0.1% | -4.5% | -5.3% | -7.2% |
| 8 | CELH | +1.000 | +0.73 | +0.00 | +0.23 | +0.00 | +0.64 | +0.87 | +0.00 | +2.5% | +7.7% | +8.6% | +8.3% |
| 9 | IRTC | +0.994 | +0.82 | +0.00 | +0.47 | +0.00 | +0.46 | +0.51 | +0.00 | +0.4% | -2.1% | -3.8% | -6.9% |
| 10 | CALX | +0.986 | +0.01 | +0.40 | +0.23 | +0.00 | +0.36 | +0.79 | +0.00 | -0.6% | -1.4% | -1.3% | -5.0% |
| 11 | NHI | +0.971 | +0.75 | -0.24 | +0.07 | +0.00 | +0.81 | +0.28 | +0.00 | -0.7% | -1.1% | -0.8% | -0.7% |
| 12 | GSHD | +0.964 | +0.91 | +0.00 | +0.47 | +0.00 | +0.81 | +0.47 | +0.00 | +2.6% | +3.2% | +2.3% | +0.9% |
| 13 | OGS | +0.945 | +0.81 | +0.00 | +0.07 | +0.00 | +0.85 | +0.15 | +0.00 | -3.1% | -1.3% | -1.2% | -0.4% |
| 14 | FIGR | +0.941 | +0.89 | +0.00 | +0.19 | +0.00 | +0.56 | +0.00 | +0.00 | +8.4% | +7.2% | +14.1% | +2.8% |
| 15 | HLNE | +0.938 | +0.94 | +0.00 | +0.23 | +0.00 | +0.46 | +0.25 | +0.00 | +1.2% | +1.1% | +2.7% | +1.2% |

| Ticker | join | sect | gen | news | dig | jdg | AB | peer | heat | vol | cat | buy |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| ELF | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| MOS | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟢 |
| AUPH | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🟢 | ⬛ | 🟢 |
| CE | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| OCUL | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| EPAM | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| WRBY | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| CELH | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| IRTC | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| CALX | 🟡 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| NHI | 🟢 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| GSHD | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| OGS | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |
| FIGR | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟡 | ⬛ | 🟡 | ⬛ | 🟢 |
| HLNE | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🟢 | ⬛ | 🔴 | ⬛ | 🟢 |

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

| # | Ticker | book | join | sect | gen | news | AB | peer | heat | 1d | 2d | 3d | 1w |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | GEV | -0.531 | -0.89 | -0.24 | +0.23 | +0.00 | -0.24 | -0.69 | +0.00 | -0.9% | -2.5% | -4.1% | -1.3% |
| 2 | INTC | -0.446 | -0.89 | +0.40 | +0.47 | -0.31 | -0.12 | -0.84 | +0.00 | -2.2% | -5.3% | -5.0% | -4.2% |
| 3 | AUR | -0.410 | -0.96 | -0.80 | +0.47 | +0.00 | -0.46 | -0.83 | +0.00 | +2.0% | -7.0% | -5.7% | -5.9% |
| 4 | ARM | -0.381 | -0.70 | +0.40 | +0.47 | -0.31 | +0.00 | -0.79 | +0.00 | -3.0% | -4.8% | -3.6% | +0.1% |
| 5 | META | -0.375 | -0.80 | +0.00 | +0.23 | +0.00 | -0.36 | -0.74 | +0.00 | +0.8% | +2.4% | +4.4% | +5.5% |
| 6 | SW | -0.371 | -0.61 | -0.80 | +0.23 | +0.00 | -0.12 | -0.18 | +0.00 | +2.5% | +1.0% | +2.6% | +2.8% |
| 7 | DELL | -0.370 | -0.15 | +0.40 | +0.47 | +0.00 | +0.24 | -0.75 | +0.00 | +1.7% | -0.4% | +3.9% | +6.7% |
| 8 | BWEN | -0.349 | -0.74 | -0.24 | +0.47 | +0.00 | -0.36 | -0.92 | +0.00 | -1.1% | -2.4% | -2.9% | -2.7% |
| 9 | PKG | -0.340 | -0.63 | -0.80 | +0.23 | +0.00 | +0.00 | -0.16 | +0.00 | +1.3% | -0.3% | -1.2% | -1.1% |
| 10 | MDB | -0.326 | -0.61 | +0.40 | +0.47 | +0.00 | +0.00 | -0.80 | +0.00 | +2.5% | -4.2% | -3.7% | -3.4% |
| 11 | RY | -0.323 | +0.59 | +0.00 | +0.07 | +0.00 | -0.12 | -0.14 | +0.00 | +0.0% | -0.6% | +0.9% | +1.0% |
| 12 | PANW | -0.318 | -0.46 | +0.40 | +0.23 | +0.00 | +0.12 | -0.06 | +0.00 | +2.4% | +0.4% | -2.8% | -2.9% |
| 13 | WMT | -0.312 | +0.13 | +0.00 | +0.07 | -0.29 | -0.12 | -0.85 | +0.00 | +0.1% | +2.8% | +1.7% | +0.7% |
| 14 | BIDU | -0.312 | -0.95 | +0.00 | +0.07 | +0.00 | -0.56 | -0.87 | +0.00 | +1.4% | +0.2% | +1.6% | +1.4% |
| 15 | SES | -0.308 | -0.71 | -0.80 | +0.23 | +0.00 | -0.56 | -0.91 | +0.00 | +6.1% | +6.1% | +7.1% | +5.3% |

| Ticker | join | sect | gen | news | dig | jdg | AB | peer | heat | vol | cat | buy |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| GEV | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| INTC | 🔴 | 🟢 | 🟢 | 🔴 | 🟢 | 🟡 | 🔴 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| AUR | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| ARM | 🔴 | 🟢 | 🟢 | 🔴 | 🟢 | 🟡 | 🟡 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| META | 🔴 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟡 | ⬛ | 🟡 |
| SW | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| DELL | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| BWEN | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| PKG | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🟡 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| MDB | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟡 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| RY | 🟢 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| PANW | 🔴 | 🟢 | 🟢 | 🟡 | 🟡 | 🟡 | 🟢 | 🔴 | ⬛ | 🔴 | ⬛ | 🟡 |
| WMT | 🟢 | 🟡 | 🟢 | 🔴 | 🟡 | 🔴 | 🔴 | 🔴 | ⬛ | 🟢 | ⬛ | 🟡 |
| BIDU | 🔴 | 🟡 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟢 | ⬛ | 🟡 |
| SES | 🔴 | 🔴 | 🟢 | 🟡 | 🟡 | 🟡 | 🔴 | 🔴 | ⬛ | 🟢 | ⬛ | 🟡 |

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

## Same 1d movers — book score + later sessions

`book` is that day's ranker score (`score_1d` in the CSV). Buys started around +0.94.
A ripper with book +0.40 was seen and ranked too low. A ripper with book — was not scored.

| Ticker | class | book | join | sect | gen | news | AB | peer | heat | 1d | 2d | 3d | 1w |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| USDE | gated_out | +0.103 | -0.26 | +0.00 | +0.07 | +0.00 | -0.12 | +0.00 | +0.00 | +85.2% | +56.8% | +51.5% | +49.5% |
| CAN | gated_out | +0.423 | -0.63 | +0.40 | +0.47 | +0.00 | +0.24 | +1.00 | +0.00 | +27.2% | +29.0% | +49.5% | +42.6% |
| CRML | outweighed | +0.260 | +0.87 | +0.00 | +0.47 | +0.00 | -0.36 | -0.96 | +0.00 | +22.6% | +15.0% | +39.7% | +37.8% |
| ARCT | gated_out | +0.800 | +0.04 | +0.00 | +0.47 | +0.00 | +0.64 | +1.00 | +0.00 | +22.4% | +30.5% | +40.5% | +44.0% |
| TMC | outweighed | +0.359 | +0.81 | +0.00 | +0.47 | +0.00 | -0.24 | -0.97 | +0.00 | +20.6% | +19.3% | +24.9% | +26.2% |
| CYPH | gated_out | +0.605 | +0.08 | +0.00 | +0.07 | +0.00 | +0.76 | +1.00 | +0.00 | +19.3% | +41.2% | +37.8% | +37.0% |
| ELMT | outweighed | +0.535 | -0.10 | -0.24 | +0.19 | +0.00 | +0.46 | +0.00 | +0.00 | +19.3% | +5.9% | +6.5% | +7.7% |
| SLS | outweighed | +0.720 | +0.45 | +0.00 | +0.47 | +0.00 | +0.64 | +0.95 | +0.00 | +15.4% | +6.9% | +5.2% | +2.5% |
| AIFC | gated_out | +0.178 | -0.75 | +0.40 | +0.47 | +0.00 | +0.12 | +0.00 | +0.00 | +15.2% | +11.9% | +8.5% | +0.0% |
| IQMX | blind | +0.353 | -0.77 | +0.40 | +0.07 | +0.00 | +0.00 | +0.00 | +0.00 | +14.8% | +7.8% | +10.5% | +13.5% |
| TII | gated_out | +0.292 | +0.88 | +0.00 | +0.07 | +0.00 | +0.24 | +0.00 | +0.00 | +14.6% | +15.8% | +19.1% | +17.5% |
| HQ | blind | +0.346 | -0.83 | +0.40 | +0.07 | +0.00 | +0.00 | +0.00 | +0.00 | +14.5% | +11.7% | +11.4% | +1.4% |
| BKKT | gated_out | +0.625 | -0.15 | +0.40 | +0.47 | +0.00 | +0.24 | -0.08 | +0.00 | +14.5% | +9.1% | +15.1% | +9.7% |
| UEC | outweighed | +0.613 | +0.91 | -0.80 | +0.23 | +0.00 | -0.12 | +0.18 | +0.00 | +14.4% | +12.2% | +19.0% | +17.8% |
| EU | gated_out | -0.130 | +0.43 | -0.80 | +0.47 | +0.00 | -0.64 | -0.70 | +0.00 | +14.3% | +18.1% | +31.4% | +30.5% |
| LAR | outweighed | +0.209 | +0.84 | +0.00 | +0.47 | +0.00 | -0.56 | -0.76 | +0.00 | +14.2% | +12.0% | +12.4% | +12.2% |
| GUTS | gated_out | +0.156 | -0.19 | +0.00 | +0.47 | +0.00 | +0.36 | -0.54 | +0.00 | +13.9% | +9.5% | +14.4% | +7.6% |
| HOOD | outweighed | +0.178 | +0.90 | +0.00 | +0.47 | +0.00 | +0.36 | -0.64 | +0.00 | +13.7% | +9.0% | +17.9% | +14.1% |
| PROK | outweighed | +0.585 | -0.10 | +0.00 | +0.47 | +0.00 | +0.46 | +0.22 | +0.00 | +13.4% | +13.4% | +21.8% | +22.5% |
| ASST | outweighed | +0.674 | +0.30 | +0.00 | +0.47 | +0.00 | +0.00 | +1.00 | +0.00 | +13.0% | +22.3% | +32.6% | +33.3% |
| ALOY | outweighed | +0.420 | +0.96 | +0.00 | +0.07 | +0.00 | -0.24 | +0.00 | +0.00 | +12.9% | +0.2% | +9.8% | +4.3% |
| USAR | outweighed | +0.605 | +0.81 | +0.00 | +0.47 | +0.00 | -0.36 | +0.00 | +0.00 | +12.6% | +6.9% | +13.4% | +10.6% |
| OI | outweighed | -0.103 | -0.97 | -0.80 | +0.07 | +0.00 | -0.70 | -0.68 | +0.00 | +12.5% | +13.6% | +17.7% | +16.6% |
| INFQ | outweighed | +0.504 | -0.85 | +0.40 | +0.47 | +0.00 | -0.12 | +0.00 | +0.00 | +12.5% | +4.0% | +10.2% | +7.8% |
| EZPW | outweighed | +0.699 | +0.79 | +0.00 | +0.07 | +0.00 | +0.36 | +0.35 | +0.00 | +12.0% | +21.9% | +22.5% | +17.9% |
| DEFT | gated_out | +0.354 | +0.34 | +0.00 | +0.47 | +0.00 | +0.46 | +0.00 | +0.00 | +11.7% | +20.4% | +17.3% | +14.4% |
| DK | outweighed | +0.449 | +0.99 | -0.80 | +0.07 | +0.00 | +0.64 | -0.77 | +0.00 | +11.6% | +6.3% | +5.5% | +10.6% |
| SGML | outweighed | +0.232 | +0.71 | +0.00 | +0.07 | +0.00 | -0.36 | -0.65 | +0.00 | +11.6% | +13.8% | +14.9% | +13.7% |
| RGTI | outweighed | +0.229 | -0.96 | +0.40 | +0.47 | +0.00 | -0.46 | -0.89 | +0.00 | +11.5% | +1.9% | +5.5% | -0.8% |
| DNN | outweighed | +0.694 | +0.98 | -0.80 | +0.23 | +0.00 | +0.00 | -0.21 | +0.00 | +11.5% | +12.7% | +18.5% | +15.6% |
| VIRT | outweighed | +0.772 | +0.90 | +0.00 | +0.07 | +0.00 | +0.64 | +0.50 | +0.00 | +11.0% | +8.3% | +6.5% | +7.4% |
| BLMN | outweighed | +0.262 | -0.15 | -0.80 | +0.23 | +0.00 | +0.00 | -0.10 | +0.00 | +11.0% | +9.2% | +5.3% | +6.4% |
| LPTH | outweighed | +0.443 | -0.15 | +0.40 | +0.47 | +0.00 | -0.24 | +0.62 | +0.00 | +10.8% | +1.6% | +5.9% | +1.2% |
| JELD | gated_out | +0.272 | -0.59 | -0.24 | +0.47 | +0.00 | +0.12 | +0.69 | +0.00 | +10.6% | +19.7% | +15.4% | +16.4% |
| ABAT | gated_out | +0.116 | -0.97 | -0.24 | +0.23 | +0.00 | -0.36 | -0.76 | +0.00 | +10.3% | +3.0% | +11.2% | +11.6% |
| CVI | outweighed | +0.774 | +0.99 | -0.80 | +0.23 | +0.00 | +0.70 | +0.10 | +0.00 | +10.3% | +6.1% | +5.8% | +11.7% |
| EOSE | outweighed | -0.070 | -0.98 | -0.24 | +0.47 | +0.00 | -0.70 | -0.95 | +0.00 | +10.1% | +0.0% | +2.3% | -3.5% |
| SID | outweighed | +0.666 | +0.41 | +0.00 | +0.47 | +0.00 | -0.36 | +0.74 | +0.00 | +10.1% | +15.7% | +14.6% | +15.7% |
| SENS | outweighed | +0.455 | +0.01 | +0.00 | +0.23 | +0.00 | +0.00 | +0.97 | +0.00 | +10.1% | +6.3% | +7.9% | +5.9% |
| BCAR | gated_out | +0.144 | -0.03 | +0.00 | +0.23 | +0.00 | -0.12 | +0.00 | +0.00 | +9.9% | +7.4% | +6.2% | +11.2% |
| FSLY | outweighed | +0.433 | -0.15 | +0.40 | +0.07 | +0.00 | +0.00 | -0.97 | +0.00 | +9.9% | +1.0% | +1.3% | +2.5% |
| PACB | gated_out | +0.584 | +0.01 | +0.00 | +0.47 | +0.00 | +0.24 | +0.42 | +0.00 | +9.8% | +4.1% | +23.6% | +26.8% |
| FUTU | outweighed | +0.393 | +0.17 | +0.00 | +0.07 | +0.00 | +0.56 | +0.79 | +0.00 | +9.7% | +2.7% | +11.4% | +13.0% |
| QUBT | outweighed | +0.135 | -0.97 | +0.40 | +0.47 | +0.00 | -0.56 | -0.43 | +0.00 | +9.6% | +1.2% | +3.9% | +2.6% |
| IE | outweighed | +0.433 | +0.91 | +0.00 | +0.23 | +0.00 | +0.12 | -0.63 | +0.00 | +9.5% | +9.4% | +15.7% | +15.9% |
| NEXA | outweighed | +0.216 | +0.99 | +0.00 | +0.23 | +0.00 | -0.12 | -0.65 | +0.00 | +9.4% | +8.2% | +12.4% | +10.7% |
| ERO | outweighed | +0.723 | +0.99 | +0.00 | +0.23 | +0.00 | +0.70 | -0.35 | +0.00 | +9.4% | +7.0% | +12.2% | +8.9% |
| CHGG | gated_out | -0.057 | -0.10 | +0.00 | +0.47 | +0.00 | -0.46 | -0.63 | +0.00 | +9.3% | +6.7% | +5.3% | +5.3% |
| QMLS | gated_out | +0.153 | -0.51 | +0.40 | +0.19 | +0.00 | +0.00 | +0.00 | +0.00 | +9.3% | +9.9% | +17.0% | +13.8% |
| MB | gated_out | -0.073 | -0.84 | -0.80 | +0.47 | +0.00 | -0.36 | +0.00 | +0.00 | +9.3% | -1.5% | +3.2% | +2.6% |
| SLI | outweighed | +0.646 | +0.83 | +0.00 | +0.47 | +0.00 | +0.24 | -0.36 | +0.00 | +9.2% | +7.0% | +13.2% | +15.0% |
| RZLT | outweighed | +0.760 | +0.41 | +0.00 | +0.07 | +0.00 | +0.76 | +0.17 | +0.00 | +9.2% | +7.5% | +7.5% | +8.2% |
| ABUS | outweighed | +0.512 | +0.60 | +0.00 | +0.07 | +0.00 | +0.70 | +0.09 | +0.00 | +9.2% | +9.0% | +9.0% | +8.8% |
| MP | blind | +0.675 | +0.98 | +0.00 | +0.47 | +0.00 | +0.00 | +0.00 | +0.00 | +9.1% | +4.3% | +9.3% | +7.8% |
| TMQ | outweighed | +0.281 | +0.67 | +0.00 | +0.47 | +0.00 | -0.24 | -0.88 | +0.00 | +9.1% | +3.6% | +5.8% | +2.5% |
| PARR | outweighed | +0.415 | +0.99 | -0.80 | +0.07 | +0.00 | +0.64 | -0.94 | +0.00 | +9.1% | +2.6% | +1.9% | +4.6% |
| TEM | outweighed | +0.572 | +0.78 | +0.00 | +0.47 | +0.00 | +0.56 | +0.96 | +0.00 | +9.1% | -0.7% | +3.1% | +2.8% |
| BTDR | outweighed | +0.870 | -0.15 | +0.40 | +0.47 | +0.00 | +0.24 | +0.95 | +0.00 | +9.0% | +4.6% | +8.2% | +2.3% |
| UUUU | outweighed | +0.409 | +0.96 | -0.80 | +0.47 | +0.00 | -0.46 | -0.34 | +0.00 | +8.9% | +7.0% | +14.9% | +12.9% |
| BGC | outweighed | +0.690 | +0.59 | +0.00 | +0.23 | +0.00 | +0.46 | +0.03 | +0.00 | +8.9% | +9.8% | +8.5% | +9.4% |
| PSNL | outweighed | +0.488 | -0.06 | +0.00 | +0.47 | +0.00 | +0.46 | +0.81 | +0.00 | +8.9% | +1.1% | +2.2% | +1.2% |
| MRNA | outweighed | +0.278 | -0.06 | +0.00 | +0.23 | +0.00 | +0.46 | +1.00 | +0.00 | +8.9% | +4.2% | +19.1% | +12.3% |
| ACDC | outweighed | -0.042 | -0.04 | -0.80 | +0.47 | +0.00 | -0.64 | -0.98 | +0.00 | +8.8% | +8.6% | +5.2% | +4.0% |
| NMG | outweighed | +0.507 | +0.75 | +0.00 | +0.23 | +0.00 | +0.24 | -0.92 | +0.00 | +8.7% | +5.1% | +7.2% | +7.2% |
| SCCO | outweighed | +0.014 | +0.98 | +0.00 | +0.23 | +0.00 | +0.46 | -0.34 | +0.00 | +8.7% | +7.8% | +10.6% | +7.6% |
| EROC | outweighed | +0.688 | -0.15 | -0.24 | +0.19 | +0.00 | +0.46 | +0.00 | +0.00 | +8.6% | +0.8% | -0.5% | -0.1% |
| QMCO | outweighed | +0.201 | -0.03 | +0.40 | +0.47 | +0.00 | +0.12 | -0.92 | +0.00 | +8.6% | +5.2% | +9.3% | +13.3% |
| AXGN | outweighed | +0.291 | +0.72 | +0.00 | +0.23 | +0.00 | +0.12 | -0.63 | +0.00 | +8.5% | +6.0% | +7.5% | +11.1% |
| QBTS | outweighed | +0.262 | -0.97 | +0.40 | +0.47 | +0.00 | -0.46 | -0.72 | +0.00 | +8.5% | -0.6% | +2.9% | -6.9% |
| FIGR | in_buy_book | +0.941 | +0.89 | +0.00 | +0.19 | +0.00 | +0.56 | +0.00 | +0.00 | +8.4% | +7.2% | +14.1% | +2.8% |
| CNH | outweighed | +0.238 | -0.32 | -0.24 | +0.23 | +0.00 | +0.56 | +0.57 | +0.00 | +8.4% | +7.4% | +5.6% | +6.1% |
| XNDU | outweighed | +0.770 | -0.15 | +0.40 | +0.47 | +0.00 | +0.12 | +0.00 | +0.00 | +8.3% | +3.4% | +5.0% | +1.5% |
| TRLV | outweighed | +0.362 | -0.04 | +0.00 | +0.47 | +0.00 | +0.36 | +0.00 | +0.00 | +8.3% | +8.7% | +10.4% | +11.5% |
| CSAN | outweighed | +0.456 | +0.44 | -0.80 | +0.07 | +0.00 | +0.00 | -0.41 | +0.00 | +8.3% | +5.5% | +11.4% | +11.0% |
| ONT | outweighed | +0.156 | -0.99 | -0.24 | +0.47 | +0.00 | -0.56 | +0.00 | +0.00 | +8.2% | +4.6% | +9.1% | +13.1% |
| COIN | outweighed | +0.362 | -0.60 | +0.00 | +0.47 | +0.00 | +0.46 | +0.85 | +0.00 | +8.2% | +4.1% | +8.6% | +5.5% |
| DCH | outweighed | +0.561 | -0.15 | -0.80 | +0.47 | +0.00 | +0.24 | +0.00 | +0.00 | +8.2% | +1.6% | +1.1% | +0.6% |
| NB | outweighed | +0.555 | +0.97 | +0.00 | +0.07 | +0.00 | +0.12 | -0.99 | +0.00 | +8.2% | -0.9% | +4.2% | +0.9% |
| IONQ | outweighed | -0.178 | -0.96 | +0.40 | +0.47 | +0.00 | -0.56 | -0.36 | +0.00 | +8.0% | -1.1% | +1.2% | -3.6% |
| AGEN | gated_out | +0.396 | +0.11 | +0.00 | +0.47 | +0.00 | +0.70 | +0.65 | +0.00 | +8.0% | +2.4% | +11.6% | +10.2% |

## Files

- `data/stock_book/2026-08-20_lookback.json`
- `01_daily/2026-08-20_lookback.md`
- `03_scoreboard/BOOK_LOOKBACK.md` (this report, latest run)

