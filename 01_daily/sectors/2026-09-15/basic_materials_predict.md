# Sector Prediction — Basic Materials — 2026-09-15

- ETF: **XLB**
- rubric: `00_grounding/sectors/basic_materials.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-3.723** (mult 0.9)
- regime: risk_off
- divergence_flagged: **True**
- engine: v2 · tape_anchor **2.993** (ES +0.80%, HG -0.76%, GC -1.01%, DX +0.25%, PM:XLB +0.68%) · index_carry **-0.716** (general -2.865) · llm_overlay **-6.0** (raw -6.75)

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

**Industrial metals — COLLAPSE, deepening.** Copper **−0.76%** to $6.359 (COMEX), and per Channel 2 the LME has broken **below $14,000/t to a three-week low**, now **−6.2% from last week's record high**. The driver is **two-fold and both negative**: (a) hawkish Fed repricing lifting the dollar and real yields, and (b) **easing LME tightness** — TradingView (09-15): *"Copper steadies near three-week low amid easing LME tightness"*; The Edge Malaysia (09-15): *"Copper holds near US$14,000 as new deliveries signal supply relief"*; Economic Times (09-11): *"Copper price hits three-week low as inventories rise and dollar strengthens."* Spine "surge" **off**; spine "collapse" is a **clean HIT**. Spine "inventory draw" is **inverted** — new deliveries are rebuilding exchange stocks, which is the *supply-glut* side of the spine, not the draw.

**China demand — FRESH SAME-MORNING HARD-DATA MISS.** NBS released August activity at 02:00 GMT today (09-15). Per Channel 2: **industrial output +5.2% y/y** (beat, vs 4.8% cons, up from 4.5% July) — the one bright spot; **retail sales +0.4% y/y** (miss, slowing from 0.6%); **fixed-asset investment −7.2% y/y for Jan–Aug** (contraction, accelerating). CNBC (09-15): *"China's August retail sales miss forecast while investment..."*; Cryptobriefing: *"retail sales, industrial production, and fixed-asset investment all missing analyst expectations."* This is the **8/17 China-miss pattern** — a same-morning hard-data miss on the demand side (retail + FAI) with the industrial beat being a supply-side offset that does **not** help materials demand. **China demand shock / property stress** is a **HIT**, and it is **live today**, not T-1.

**Monetary metals — FADE.** GC **−1.01%** to $4,307.7, SI **−1.47%** to $63.205. XTB (09-15): *"Gold at five-week lows — what stands behind the decline?"* 8/14 does **not** pay. The News Judge's "gold surge on Fed rate-cut bets" headline (B: Barrick +8.21%) is **stale and contradicted** by the live tape — do not score it.

**Oil-up is a cost headwind for processors, not an XLB squeeze.** Count Hormuz/oil once in S0 as the risk-off overlay; do not also credit copper as a positive floor (8/18). The chemicals-heavy book (LIN ~13%, SHW, ECL — ~40–50% combined) faces a direct oil feedstock/energy cost squeeze at WTI $103.79 / Brent $108.11. **Margin compression / cost inflation without pricing power** is a **HIT** for the majority sleeve.

**Supply disruption / tariffs — stale.** DRC concentrate ban and Section 232 copper remain on the books; not a same-open catalyst. BHP ADRs fell on copper retreating from records amid US tariff uncertainty (Finviz digest) — a miner-sleeve drag, already in the copper HIT.

**S1 = −2.** Per the 09-09 lesson: all four sub-channels align negative (chemicals oil-cost drag + copper collapse + gold/silver fade + China demand shock) with **zero offsetting positive** anywhere in the book. The "not a collapse" cap does **not** apply — copper is at a three-week low and −6.2% off the record, LME tightness is *easing* (supply relief), and the minority sleeve that could have provided offset (copper miners) is itself negative.

### 3. Breadth (S2)

The 09-09/09-10 uniform-negative breadth pattern is **re-firing**. MAP HEAT shows the parent book split with **no defensive pocket**: Chemicals **flat** (breadth 0.188, "merger-arb and macro-oil tape, not a demand tape"), Building Materials **down** (breadth 0.176, −2.33% d1), Coking Coal **down** (both captains −4.5% w/w, 0.0 breadth), Gold **down** (breadth **0.073 — worst on the board**), Lumber **down** (breadth 0.167), Other Industrial Metals **down** (rare-earth complex gapped down on China summit threat). The only positives are **nested child books the parent ETF underweights**: Aluminum OVERRIDE (+2.85% d1, "no SPX captain, entirely in RUT names the Materials ETF underweights"), Copper HEAT (FCX/IE, but that is a *nested* long vs a *flat* parent and is contradicted by the live COMEX −0.76%), and Other Precious Metals (+2.24% w1, juniors financing). Per the MAP HEAT instruction, **nested overrides do not average into the parent ETF**.

**S2 = −1.** Breadth is uniformly negative across the parent's actual sleeves (chemicals flat, building materials down, gold worst-on-board, coking coal down, lumber down, rare earths down) with no defensive pocket — the 09-09 rule's S2=−1 condition is met.

### 4. Flows / positioning (S3)

XLB ~1m net outflows from prior logs (~−$180M range). Not a washout, not a volume spike. The 09-14 lesson's S3 skill multiplier was raised to 1.25 (n=9, hit=0.667) — but there is **no fresh flow print** today to justify a non-zero score. **S3 = 0.**

### 5. Tape (S4, confirmation only)

1d rel **−0.46%** is **below the 0.5% threshold** — per the 8/27 S4-cap, this **cannot** be a ± confirmation. The 3d rel **−1.55%** and 1w rel **−2.51%** are decisive, but per the 09-04 T-1-lag rule, a prior-day lag is not fresh confirmation of *this* session. **S4 = 0.**

### Reconciliation

Total = (−1 + −2 + −1 + 0 + 0) = **−4.0** × 0.9 = **−3.6** → **down/mild**.

**Divergence flag: TRUE.** The engine's tape_anchor leg is **−0.05** (ES +0.51%, HG −0.76%, GC −1.01%, DX +0.25%, PM:XLB +0.03%) — essentially flat, because the premarket ES is *green* (+0.51%) while the metals are red. The engine's index_carry is **−1.554** (general −6.215) and llm_overlay **−6.0** (raw −6.75), producing an engine total of **−7.604** → down/mild. The analyst's component arithmetic (−3.6) and the engine total (−7.604) **agree on every sign** but disagree on magnitude by >2×. Per the 09-14 lesson, report both bands and state which leg drives the divergence: here it is the **llm_overlay (−6.0)**, not the sector factors. The sector factors (S1=−2, S2=−1) are the calibrated read; the overlay is the amplifier. Both resolve to **down/mild**, so the band is stable — but the divergence is flagged because the anchor leg is *flat* while the overlay is *extreme*, which is the same structural mismatch the 09-14 lesson identified.

**Gap check (09-10 rule):** XLB premarket **+0.68%** vs prev close — a **positive** gap, not a ≥1% down-gap. The 09-10 rule does **not** fire (it is a down-gap rule). But note the tension: XLB is indicated **+0.68%** while the metals complex is uniformly red and China just missed. That positive premarket print is the **single biggest risk to this call** — it suggests the market is reading the China industrial-output beat (+5.2%) and the nested Aluminum/Copper HEAT as an XLB positive, or that XLB is being carried by the chemicals sleeve's oil-cost pass-through. If XLB opens +0.68% and holds, the down call is wrong on direction. I am keeping down because (a) the 1w/1m relative tape is persistently negative (−2.51% / −1.29%), (b) the 8/18 metals-co-move ban is firing cleanly, (c) the China demand-side miss is live and same-morning, and (d) the FOMC-eve hawkish repricing is a knowable red pre-binary tape. But confidence is cut for the conflicting premarket print.

**Confidence: 0.55.** Direction is supported by four aligned negative channels, but the +0.68% XLB premarket indication directly conflicts with the thesis, and the FOMC binary is unknowable. Per DO-INSTEAD, shrink confidence on modest |score| when magnitude historically misses.

**Multiplier: 0.9.** Rolling mag accuracy 0.5 — do not manufacture notable.

### HORIZON_3D / 1W / 2W / 1M

- **HORIZON_3D:** down. The FOMC (09-16) resolves the hawkish repricing one way or the other; if it hikes, the metals complex takes another leg down; if it holds, a relief bounce is possible but the China demand miss and easing LME tightness cap it. Net: down/mild.
- **HORIZON_1W:** down. The 1w rel is −2.51% and the spine (copper collapse + China demand shock + inventory rebuild) is intact-negative. No catalyst in the next week reverses the composition-weighted drag.
- **HORIZON_2W:** flat-to-down. Two weeks out, the FOMC is past and the market re-focuses on China stimulus response. If Beijing responds to the FAI −7.2% with credit/property measures, the China HIT could flip; absent that, the downtrend persists.
- **HORIZON_1M:** flat. The 1m rel is −1.29% (less negative than 1w), suggesting the sector is in a downtrend but not accelerating. Mean-reversion risk rises as the sector gets stretched to the downside (1w rel −2.51%).

---

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -2
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: risk_off
DIVERGENCE_FLAGGED: true
TOTAL_SCORE: -3.6
PREDICTED_DIRECTION: down
PREDICTED_MAGNITUDE_BAND: mild
HORIZON_3D: down
HORIZON_1W: down
HORIZON_2W: flat
HORIZON_1M: flat
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-off tape / flight to safety|HIT|0.85|2026-09-15|https://www.reuters.com/world/china/chinas-factory-output-growth-quickens-in-august-retail-sales-slow-2026-09-15/
Real yields rising|HIT|0.80|2026-09-15|
USD strengthening|HIT|0.70|2026-09-15|
Industrial metal price collapse|HIT|0.90|2026-09-15|https://news.google.com/rss/articles/CBMiywFBVV95cUxQNTNJbnc3ZzJYQnItNGZKMjlVeTM3d3I5MlotSFRlWmplM0ttSkxHZzlXSEowYm5KamNSd3l6UGl1ZUNDZldzTEV4ZHRzaFVEWUl2SWdkWGRXM004SkxvRHhhVEJ3cG02bHJwcnd4RjRCZ0MxMEM2dXR5Q2JwVVYtNjRyQWFoZHcyTEo5U0pzQU9VaUVSamZzVmtxMmstcFdhdnNpLTA4RWk1WU9aOUs5Tmd1V1J1TUxTYVZtMExfeXZpQ1B2c0RVTGZRbw
China demand shock / property stress|HIT|0.85|2026-09-15|https://www.cnbc.com/2026/09/15/china-august-retail-sales-industrial-output-investment-exports-.html
Supply glut / new capacity online|HIT|0.70|2026-09-15|https://news.google.com/rss/articles/CBMiUEFVX3lxTE9oTl9LWUJHcjdVR1FHUWZyVFVZTDdXdnNUVnVPUVRaZUJ4SUN3NTdfQWF3VXRqRXFFc2VKMHJNYXY1VXhQSVlUWDdqZGphU1dI
Margin compression / cost inflation without pricing power|HIT|0.65|2026-09-15|
Gold/silver price surge (monetary metals)|MISS|0.85|2026-09-15|https://news.google.com/rss/articles/CBMivAFBVV95cUxNcTg0ZTVXNlpmUkx1dU5Gc0JDWU1HX3VBU3d5R0lXemdtSTlBa3BVeUVqNDJhRnVEazVLUVRTakYxOURQS3dYTEtpekhscUNuZVNPWWExOFYyUzhldzZId0lEaHV2LW5wVndNaDJVbGg5MUp5d1IwUDlCVjBvTFo0MkhVeHI2NnFPdy1zVnVnaTZzTkxadXhWb0FlRE1BMERjM3EwR25McXFNQ1VibXFTbGlLNDY2YVZDakM1UA
Industrial metal price surge (copper/aluminum/iron ore)|MISS|0.85|2026-09-15|
Inventory draw (LME/exchange stocks down)|MISS|0.75|2026-09-15|https://news.google.com/rss/articles/CBMiUEFVX3lxTE9oTl9LWUJHcjdVR1FHUWZyVFVZTDdXdnNUVnVPUVRaZUJ4SUN3NTdfQWF3VXRqRXFFc2VKMHJNYXY1VXhQSVlUWDdqZGphU1dI
China PMI / property demand rebound|MISS|0.80|2026-09-15|https://www.reuters.com/world/china/chinas-factory-output-growth-quickens-in-august-retail-sales-slow-2026-09-15/
Sector breadth failure (ETF up, names flat)|PARTIAL|0.50|2026-09-15|
Sector rotation out of materials|PARTIAL|0.55|2026-09-15|
Sector ETF outflow / volume dry-up|PARTIAL|0.45|2026-09-15|
Critical-minerals policy / domestic tariff support|PARTIAL|0.40|2026-09-15|
Supply disruption (mine/export ban)|PARTIAL|0.35|2026-09-15|
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -2.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': -10.0, 'divergence_flagged': True, 'total_score': -3.723, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.549, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.4988, 'score': 2.993, 'legs': [{'leg': 'ES', 'pct': 0.8, 'w': 0.6}, {'leg': 'HG', 'pct': -0.76, 'w': 0.3}, {'leg': 'GC', 'pct': -1.01, 'w': 0.1}, {'leg': 'DX', 'pct': 0.25, 'w': -0.3}, {'leg': 'PM:XLB', 'pct': 0.68, 'w': 0.7}]}, 'overlay_score': -6.0, 'overlay_raw': -6.75, 'index_carry': -0.716, 'general_total': -2.865, 'skill_multipliers': {'S0_SHARED_MACRO': 1.25, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 1.25, 'S4_ETF_TAPE': 0.0}, 'llm_confidence': 0.55, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
