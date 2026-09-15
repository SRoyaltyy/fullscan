# Sector Prediction — Basic Materials — 2026-09-15

- ETF: **XLB**
- rubric: `00_grounding/sectors/basic_materials.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-7.604** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**
- engine: v2 · tape_anchor **-0.05** (ES +0.51%, HG -0.76%, GC -1.01%, DX +0.25%, PM:XLB +0.03%) · index_carry **-1.554** (general -6.215) · llm_overlay **-6.0** (raw -6.75)

## Channel 1 sector ETF tape

```
ETF XLB vs SPY (yfinance, through 2026-09-14):
  1d: XLB -0.90% | SPY -0.45% | rel -0.46%
  3d: XLB -1.75% | SPY -0.20% | rel -1.55%
  1w: XLB -3.72% | SPY -1.21% | rel -2.51%
  1m: XLB -3.48% | SPY -2.19% | rel -1.29%
```

MEMORY_CONFIRM: Sector Basic Materials / XLB only. Memory index unavailable this run (embedding metadata missing) — used injected scoreboard/lessons only. Rolling last-10 dir=0.5 mag=0.5 (n=10); last graded 2026-09-14 down/notable vs XLB −0.90% / SPY −0.45% / rel −0.46% (dir HIT, mag HIT). Active XLB rules checked: **09-14 S1-skill-multiplier lesson** — do not let a sub-1.0 multiplier haircut a rule-capped channel while anchor legs set the band; report both bands when component arithmetic and anchor total disagree >2×. **09-11 pre-binary-tape rule** — separate the unknowable binary (FOMC 09-16) from the knowable pre-binary tape; score a modest S0 lean in the futures direction rather than zeroing everything, and carry an explicit relative-underperformance lean when 1w/1m rel are persistently negative. **09-10 gap-at-open rule** — if |open vs prior close| ≥ 1.0%, mild is falsified at the bell. **09-09 composition/magnitude rule** — when the 8/18 metals-co-move floor ban fires with all four S1 sub-channels negative and zero offset, score S1=−2, S2=−1. **8/18 metals-as-floor ban** — do NOT use copper/gold as a floor on oil-shock risk-off days. **8/14 gold-offset** — score the monetary bid only if gold/silver are green (today they are NOT: GC −1.01%, SI −1.47%). **8/25 composition/transmission** — chemicals ~40–50% of XLB vs copper miners ~10–15%; NQ>>ES is not a materials green light. **8/27 S4-cap** — 1d rel <0.5% cannot be a ± confirmation. **8/28 leftover-S2/S4 down-mandate** — S4 confirms only the session being predicted. **09-03 exhaustion-bounce** — sleeve-driven >+1% reversal within a negative 1w is a bounce, not an inflection. **09-04 T-1-lag** — do not copy a prior-day lag into S4 as fresh. No open experiment for `sector_basic_materials`. DO-INSTEAD: keep direction; shrink confidence on modest |score| when magnitude historically misses.

---

# Sector Prediction — Basic Materials — 2026-09-15

- ETF: **XLB**
- rubric: `00_grounding/sectors/basic_materials.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **−4.05** (mult 0.9)
- regime: risk_off
- divergence_flagged: **True**

## Channel 1 sector ETF tape

```
ETF XLB vs SPY (yfinance, through 2026-09-14):
  1d: XLB -0.90% | SPY -0.45% | rel -0.46%
  3d: XLB -1.75% | SPY -0.20% | rel -1.55%
  1w: XLB -3.72% | SPY -1.21% | rel -2.51%
  1m: XLB -3.48% | SPY -2.19% | rel -1.29%
```

## Analysis — XLB, session of 2026-09-15

This is a **Tuesday risk-off open into an FOMC-eve stack**, with the metals complex in a **broad, deepening collapse** and a **fresh same-morning China hard-data miss**. It is not a copper-squeeze day and not a leftover-chemicals fade. Channel 1 tape through 09-14 is decisively negative across every horizon: 1d rel **−0.46%**, 3d **−1.55%**, 1w **−2.51%**, 1m **−1.29%**.

### 1. Shared macro as it hits materials (S0)

The dominant live driver is the **FOMC binary (09-15/16)** wrapped in a **hawkish repricing that has already happened**. Per Channel 2 search: CME FedWatch puts the September 16 hike at **~56–85%** depending on the feed (Yahoo/CNBC ~56% six days ago; goldsilver.com today cites **83–85%** on Fed-funds futures after hot August PPI). Either way the market is pricing a **hike**, not a cut — the opposite of the "gold surge on Fed rate-cut bets" headline in the News Judge, which is a **stale/contradicted** framing. The live tape confirms the hawkish read: **DXY +0.25% to 99.36**, **DFII10 2.60 (+0.05 1d, +0.18 1w, +0.18 1m)**, **DGS10 4.96 (+0.19 1w, +0.28 1m)**, **DGS30 5.35**. Real yields are grinding higher into the meeting.

Live tape (Channel 1, do not re-derive): **ES −0.54%, NQ −0.62%, Russell −0.73%, DJIA −0.71%** — a **broad, uniform risk-off**, not the tech-led NQ>>ES rotation of 09-14. Reuters (09-15): *"Wall St futures slip as rising oil, Treasury yields compound AI anxiety."* **VIX 17.56 (+0.46 1d, +1.84 1w)** with **VIX/VIX3M 1.123 backwardation** — stress building. **USEPUINDXD 395.54 (+189.93 1d)** — policy uncertainty spiking into the FOMC. **HY OAS 2.65** still tight. **Asia composite −0.66%** (Hang Seng −1.0%, Kospi −0.85%, ASX −0.88%, Shanghai −0.54%); **Europe −0.44%**.

The materials-specific overlay is the **8/18 metals-co-move floor ban firing cleanly**: oil is **spiking** (WTI **$103.79 +2.37%**, Brent **$108.11 +2.31%**, heating oil +3.09%, gasoil +2.68%), and the **entire metals complex is co-moving DOWN with equities** — copper **−0.76%** to $6.359, gold **−1.01%**, silver **−1.47%**, platinum **−1.67%**, palladium **−1.85%**, aluminum **−0.03%**, iron ore **−0.48%**. This is risk-asset liquidation, not a hedge. Do **not** score S0 as risk_on because oil is up.

**S0 = −1.** Hawkish FOMC-eve repricing + firm USD + rising real yields + oil-spike risk-off + backwardation map negative to this cyclical. Not −2: ES is only −0.54%, DXY is firm not a spike, HY is tight, and the FOMC binary itself is unknowable — but the **pre-binary tape is knowable and it is red** (09-11 rule).

### 2. Spine + secondary (S1)

**Industrial metals — COLLAPSE, deepening.** Copper **−0.76%** to $6.359 (COMEX), and per Channel 2 the LME has broken **below $14,000/t to a three-week low**, now **−6.2% from last week's record high**. The driver is **two-fold and both negative**: (a) **rising LME inventories** easing supply tightness (Economic Times / Business Recorder, 09-14: *"Copper sinks to 3-week low as dollar firms, LME stocks rise"*), and (b) the **White House copper tariff plan stalling** on affordability concerns (Reuters 09-10, Mining.com.au 09-11) — which *removes* the tariff-driven squeeze premium that carried the record run. Spine "surge" **off**; spine "collapse" is a **clean HIT**. Spine "inventory draw" is **inverted** — LME stocks are **rising**, which is the opposite HIT.

**China demand — FRESH same-morning hard-data miss.** This is the key change from 09-14. NBS released August activity at 02:00 GMT **today (09-15)**: **industrial output +5.2%** (beat, up from 4.5%), but **retail sales +0.4%** (miss) and **Jan–Aug fixed-asset investment −7.2% YoY** (deepening from −6.7% in Jan–Jul), with the **property slump deepening** (CNBC, Reuters, Xinhua 09-15). This is a **live, same-morning China demand-shock print** — the 8/17-style hard-data miss that the prior sessions only had as T-1/T-2 context. It confirms the **China demand shock / property stress** HIT at full weight, and it is **not** a rebound. The industrial-output beat is a genuine offset but it is a *supply-side* beat (Beijing itself warns of "supply-demand imbalance") — it does not rescue the demand channel that drives copper and chemicals.

**Monetary metals — FADE.** GC **−1.01%** to $4,307.7, SI **−1.47%** to $63.205. 8/14 does **not** pay. The News Judge's "gold surge on Fed rate-cut bets lifts Barrick +8.2%" is **contradicted by the live tape** — gold is down ~1% and the Fed is pricing a *hike*. Per the 09-14 lesson, do not let a stale headline override the live Channel 1 print. MAP HEAT Gold is **dir=down** with breadth **0.073 — the worst on the board** — confirming the gold sleeve is a drag, not a floor.

**Oil-up is a cost headwind for processors, not an XLB squeeze.** Count Hormuz/oil once in S0 as the risk-off overlay; do not also credit copper as a positive floor (8/18). The chemicals-heavy book (LIN ~13%, SHW, ECL — ~40–50% combined) faces a direct oil feedstock/energy cost squeeze with WTI >$103. MAP HEAT Chemicals is **flat/low-conviction** ("merger-arb and macro-oil tape, not a demand tape"), which is a *relative* cushion but not a positive.

**Supply disruption / tariffs — now a NEGATIVE, not a positive.** The DRC concentrate ban and Section 232 copper remain on the books, but the **live catalyst is the tariff plan STALLING** — which removes the squeeze premium. This flips the "critical-minerals policy / domestic tariff support" HIT from positive to negative for the miner sleeve. APD's Q3 beat/raise is **already traded** (09-14) and carries a $2.9B clean-energy exit charge — a single-name positive, not an XLB-wide thrust.

**S1 = −2.** Per the 09-09 lesson: all four sub-channels align negative — **chemicals oil-cost drag + copper collapse (LME <$14k, −6.2% from record, stocks rising) + gold/silver fade + fresh China demand/property miss** — with **zero offsetting positive** anywhere in the book. The "not a collapse" cap does **not** apply: copper is in a genuine multi-session collapse and the minority sleeve that could have provided offset is also negative.

### 3. Breadth (S2)

MAP HEAT SPLIT is **uniformly negative-to-flat** across the nested books: **Building Materials dir=down** (breadth 0.176, −2.33% d1), **Coking Coal dir=down** (both captains −4.5% on the week, 0.0 breadth), **Gold dir=down** (breadth 0.073, worst on board), **Lumber dir=down** (breadth 0.167), **Other Industrial Metals dir=down** (rare-earth complex gapped down on China summit threat), **Chemicals dir=flat** (breadth 0.188). The only positives are **Copper dir=up** (FCX/IE, breadth 0.875, +5.09% d1, EXIM financing catalyst) and **Aluminum OVERRIDE dir=up** (+2.85% d1, but explicitly *"entirely in RUT names the Materials ETF underweights"*) and **Other Precious Metals dir=up** (juniors, +2.24% w1). Per the 09-09 lesson, when the 8/18 metals-co-move pattern fires there is **no defensive pocket** inside XLB — and here the negatives dominate the nested board. The Copper nested long is real but is a **minority sleeve (~10–15%)** and is fighting a −0.76% live copper print and rising LME stocks; the Aluminum override is explicitly **not in the ETF**. **S2 = −1.**

### 4. Flows / positioning (S3)

XLB AUM ~$8.55B (09-09); no fresh flow print available this morning. The 1w rel **−2.51%** and 1m rel **−1.29%** describe a sector being **sold relative to SPY** on a persistent basis, but there is no washout/volume-spike evidence and no crowding extreme. **S3 = 0.**

### 5. Tape (S4, confirmation only)

1d rel **−0.46%** is **below the 0.5% threshold**, so per the 8/27 S4-cap it **cannot be a ± confirmation**. The 3d rel **−1.55%** and 1w rel **−2.51%** are the persistent relative-underperformance lean (09-11 rule), but they are multi-horizon descriptors, not same-day tape. **S4 = 0.**

### Reconciliation and divergence flag

Component arithmetic: S0 −1, S1 −2, S2 −1, S3 0, S4 0 = **−4.0**; × mult 0.9 = **−3.6** → **down/mild**. The engine's anchor legs (tape_anchor driven by ES −0.54%, HG −0.76%, GC −1.01%, DX +0.25%; plus index_carry from the general run) will push the deterministic total materially more negative — the same **>2× band disagreement** flagged in the 09-14 lesson. Per that lesson I report **both**: the component arithmetic supports **down/mild**, the anchor-driven total supports **down/notable**. I am **not** letting the anchor mechanically set the band, and I am **not** applying a sub-1.0 haircut to the rule-capped S1 = −2. **Divergence flagged: True.**

**Magnitude check (09-10 gap rule):** XLB premarket is **+0.03%** vs prev close — **no ≥1% gap**, so the mild band is **not** falsified at the bell. This is the key difference from 09-10. The mild band stands on the gap test, and the rolling-magnitude prior (mag accuracy 0.5, two consecutive mag misses on the *understated* side) argues against inflating to notable without a same-morning print. **Band = mild**, with the divergence flag carrying the notable risk.

**Direction:** down. The 09-11 rule requires a modest S0 lean in the *futures* direction (red) rather than zeroing everything, and the persistent 1w/1m relative lag carries an explicit **relative-underperformance lean** — XLB should lag SPY on this tape.

---

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -2
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.62
REGIME: risk_off
DIVERGENCE_FLAGGED: True
SECTOR: Basic Materials
ETF: XLB
PREDICTED_DIRECTION: down
PREDICTED_MAGNITUDE_BAND: mild
TOTAL_SCORE: -3.6
HORIZON_3D: down
HORIZON_1W: down
HORIZON_2W: flat
HORIZON_1M: flat
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-off tape / flight to safety|HIT|0.85|2026-09-15|https://www.reuters.com/business/wall-st-futures-slip-rising-oil-treasury-yields-compound-ai-anxiety-2026-09-15/
Real yields rising|HIT|0.75|2026-09-15|https://goldsilver.com/industry-news/article/gold-price-outlook-september-2026/
USD strengthening|HIT|0.65|2026-09-15|https://www.usagold.com/daily-precious-metals-market-report-september-14-2026/
Industrial metal price collapse|HIT|0.85|2026-09-15|https://economictimes.indiatimes.com/markets/us-stocks/wall-street-guide/copper-hits-three-week-low-as-inventories-rise-and-dollar-strengthens/articleshow/134245729.cms
China demand shock / property stress|HIT|0.80|2026-09-15|https://www.cnbc.com/2026/09/15/china-august-retail-sales-industrial-output-investment-exports-.html
Inventory draw (LME/exchange stocks down)|MISS|0.70|2026-09-15|https://www.brecorder.com/news/40439432/copper-sinks-to-3-week-low-as-dollar-firms-lme-stocks-rise
Gold/silver price surge (monetary metals)|MISS|0.80|2026-09-15|https://www.usagold.com/daily-precious-metals-market-report-september-14-2026/
Critical-minerals policy / domestic tariff support|MISS|0.70|2026-09-15|https://news.google.com/rss/articles/CBMivwFBVV95cUxNNURGbmRQdjJaanFqS0VBQUVyTjZCNkZyeTBrV2NVcWtGLVRQdk5jaG9pYU5FejR3REpqdHJSQWM0NlRUSVdnclJOR0RiWEpUNlV1ZXRUU1RPSTFlNF90elZIZWFxbFhDYm1nUlFjQ3l0UjRMbGstTmItU3BiOVZDYi0tRUxPelM0V1J6TmhVTnJJQkdxdUFBemtkdWxTdUNScndiZlNiQm1rQXRsMlh6ZFhxNnRmemFLN2ZfMnk2UQ
Margin compression / cost inflation without pricing power|HIT|0.60|2026-09-15|https://www.iea.org/reports/oil-market-report-september-2026
Sector rotation out of materials|HIT|0.70|2026-09-15|https://www.thedesperatetrader.com/sector-performance
Sector breadth failure (ETF up, names flat)|HIT|0.60|2026-09-15|https://www.stockbase.com/market-data/sectors
Sector ETF outflow / volume dry-up|PARTIAL|0.45|2026-09-15|https://etfdb.com/etf/XLB/
Crowded long (extreme relative performance + valuation)|MISS|0.55|2026-09-15|https://www.tipranks.com/etf/xlb
Supply disruption (mine/export ban)|PARTIAL|0.40|2026-09-15|https://kamoacap.com/lmecmecoppertarriff/
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -2.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': -10.0, 'divergence_flagged': False, 'total_score': -7.604, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.65, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': -0.0084, 'score': -0.05, 'legs': [{'leg': 'ES', 'pct': 0.51, 'w': 0.6}, {'leg': 'HG', 'pct': -0.76, 'w': 0.3}, {'leg': 'GC', 'pct': -1.01, 'w': 0.1}, {'leg': 'DX', 'pct': 0.25, 'w': -0.3}, {'leg': 'PM:XLB', 'pct': 0.03, 'w': 0.7}]}, 'overlay_score': -6.0, 'overlay_raw': -6.75, 'index_carry': -1.554, 'general_total': -6.215, 'skill_multipliers': {'S0_SHARED_MACRO': 1.25, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 1.25, 'S4_ETF_TAPE': 0.0}, 'llm_confidence': 0.62, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
