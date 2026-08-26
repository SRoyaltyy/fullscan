# Sector Prediction — Basic Materials — 2026-08-26

- ETF: **XLB**
- rubric: `00_grounding/sectors/basic_materials.md`
- predicted_direction: **up**
- predicted_magnitude_band: **flat**
- total_score: **2.0** (mult 0.8)
- regime: mixed
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLB vs SPY (yfinance, through 2026-08-25):
  1d: XLB +0.00% | SPY +0.32% | rel -0.32%
  3d: XLB +2.21% | SPY +0.43% | rel +1.78%
  1w: XLB +3.48% | SPY -0.20% | rel +3.68%
  1m: XLB +4.26% | SPY +3.63% | rel +0.63%
```

MEMORY_CONFIRM: Sector Basic Materials / XLB only. Scoreboard n=9 graded, dir=0.667, mag=0.444. Last graded 2026-08-21 was up/severe and HIT (+2.14% / rel +1.73%); 2026-08-24 up/mild and 2026-08-25 up/mild are ungraded (8/25 reflect flagged a composition/transmission miss — predicted up/mild, actual flat). Active XLB rules checked: temper-severe does **not** fire (1d rel -0.32% is <0.5% but there is **no** active Hormuz/oil>$90 risk-off tape — CL -2.5%, BZ -3.94%, oil collapsing); 8/17 China-miss + flat-futures severe ban does **not** fire (no same-morning China print; August NBS PMI ~31 Aug); 8/18 metals-co-move floor ban does **not** fire (oil is falling, not a supply squeeze); 8/14 gold-offset **does** apply (score the monetary-metals bid; do not ignore it); 8/21 "keep pipeline severe if no temper" does **not** apply — this is a post-run follow-through with a **negative** 1d tape, not a fresh squeeze day; **8/25 composition/transmission lesson is now active** — when NQ >> ES (tech-led risk-on) and XLB 1d rel <0.5% with mixed breadth and two-sided S1, emit flat/flat-mild, not up/mild. No open experiment for `sector_basic_materials`. Memory index search unavailable; used injected sector scoreboard/lessons only.

## Analysis — XLB, session of 2026-08-26

This is a **Wednesday follow-through** after Friday's materials blowout and two consecutive flat/mild sessions (Mon +0.07%, Tue 0.0%). Channel 1 tape is **no longer confirming**: 1d rel **-0.32%** (XLB flat, SPY +0.32%), 3d **+1.78%**, 1w **+3.68%**, 1m **+0.63%**. The 1d print has **turned negative** — the sector is stalling after its run. This is the exact setup the 8/25 composition/transmission lesson targets: a tech-led risk-on tape with a flat-to-negative XLB 1d tape and two-sided sector factors.

### 1. Shared macro as it hits materials
Premarket is **flat-to-mildly-negative, tech-led**: ES **-0.07%**, NQ **-0.15%** (NQ still above ES on the day's leadership but both negative). Asia composite **+0.47%** (broadly green), Europe **+0.18%**. VIX **15.69** (calm) but VIX/VIX3M **1.014 — backwardation** (a small caution). Fear & Greed **58.6** (Greed).

The dominant scheduled catalyst is **July core PCE due today (8:30 ET)** — the Fed's preferred inflation gauge. News Judge ranks it #1. The market is positioned for a cool print; the asymmetric risk is a hot surprise. Fed's Collins hawkish comments + regional directors seeking a hike (News Judge #2) raise the stakes. This is a **two-sided binary event** for the whole tape, and for a cyclical sector like materials a hot print is a clear negative (higher real yields, USD firming, risk-off).

Offsets that map to this sector:
- **Oil is down hard** (CL **-2.5%**, BZ **-3.94%**). News Judge: Iran sanctions are copy, not a Hormuz supply shock. The 8/18 "oil-shock liquidates the whole commodity complex" pattern is **off**. Falling crude is **cost-input relief** for chemicals/processors, not a metals squeeze.
- **Gold is the live cross-asset tell** (GC=F **+0.89%**, spot near 3-month high ~$4,650). That is a materials-positive substitution/hedge, **not** an SPX risk-on mandate — and **gold does not cancel China**.
- **USD**: DXY **+0.1%** 1d vs **-2.33%** 1m. Not a USD spike vs the complex.
- **Real yields**: DFII10 **2.38, -0.02 1d, -0.06 1w** — easing. Mild positive for gold/duration.
- **Calendar:** July **PCE today 8:30 ET** is the load-bearing event. Encode as **session event risk**, not "no macro print." Do not score S0 negative merely because the print exists (two-sided), but do not build an up call into a binary hawkish-risk event with flat futures.

**S0 = 0 (mixed).** Flat futures + PCE binary risk + hawkish Fed pushback keep this from being a risk-on cyclical tape; falling oil + easing real yields + gold keep it from being a hard risk-off overlay for XLB.

### 2. Spine + secondary (S1)
**Industrial metals — partial, not a surge day.** COMEX copper ~**$6.60-6.64/lb** (firm/elevated, off the record). LME copper stocks have **rebuilt** to ~**240kt** from ~205kt mid-August — the acute tightness has eased. Aluminum ~$3,200/t (soft). Iron ore still ~$95/t on China property. Spine "surge" is **partial**; spine "inventory draw" is **off**.

**China demand — still the industrial offset.** July NBS mfg PMI **49.2**, construction **47.0**. August official PMI not out until ~31 Aug. Property/F AI still draining traditional copper demand. **Do not let gold cancel this.**

**Supply disruption — stale/narrow.** DRC concentrate ban still on the books but BMI/Fitch: concentrates are a small share vs DRC cathode. Not a same-day shock.

**Monetary metals (different driver) — HIT.** Channel 1 gold **+0.89%**; silver elevated. News Judge ranks the gold/miners bid (AU +8.5%, Barrick +8.2%) as a primary materials input. Per 8/14: this is a **positive S1 offset**, not a dampener. It is **not** permission to emit up/severe from gold alone.

**Policy/tariffs — carried HIT.** Section 232 copper/semi-finished still in force. Not a fresh open catalyst.

**S1 = +1.** Gold + still-elevated copper, minus China/property, minus the inventory rebuild, minus stale DRC. Capped well below +2/+3 so gold cannot wash out the industrial breakdown.

### 3. Breadth
Premarket top weights are **mixed, not expanding**: LIN ~flat, FCX ~flat, NEM ~-0.5%, SHW ~+0.5%. That is large-cap/chemicals vs miner split, not a fresh % names-up thrust. Friday's FCX/NEM/AU leadership is **already in the 3d/1w tape** — scoring it again as today's breadth would double-count the same reversal (the follow-through lesson forbids this).

**S2 = 0.**

### 4. Flows / positioning
XLB ~$8.7B. Recent 1m net flows roughly flat-to-slightly-negative despite the price run. 1w/1m relative is extended enough for **crowding risk** (1w rel +3.68%), not a forced-selling signal. No fresh inflow/volume spike today.

**S3 = 0.**

### 5. Tape (confirmation only)
Channel 1 1d rel **-0.32%** is a **-1** (negative) confirmation — the sector is **not** confirming the up thesis today. 3d/1w relative strength is already traded. Per the 8/25 lesson, do not derive direction from 1w/3d relative strength that is already in the price.

**S4 = -1.** Leading sum (0+1+0+0 = +1) and tape (-1) **disagree** — a leading-vs-tape divergence. Trust the tape over the modest factor stack on a PCE day with flat futures. This is a flat-to-mild call, not up.

### Self-audit
- **Lens:** XLB environment, not SPX, not FCX/NEM single-name.
- **Band:** After +2.14% Friday and two flat sessions, negative 1d tape, PCE binary risk, copper tightness easing, China still contracting → **flat/mild**, not up.
- **Skew:** Gold ≠ industrial metals. Stated separately; gold does not cancel China.
- **Same-shock:** PCE counted once in S0 as event risk; gold counted once in S1; oil counted once as cost-input relief. No double-count.
- **Single-ticker:** AU/Barrick gold surge is a monetary-metals sub-theme, not the XLB ETF call. It does not drive the sector ETF on a flat tape.

**Divergence flagged:** leading sum (+1) vs tape (-1) disagree. Trust the tape and the PCE binary risk → **flat/mild**, low confidence.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -1
MULTIPLIER: 0.8
CONFIDENCE: 0.45
REGIME: mixed
HORIZON_3D: flat:mild:0.45
HORIZON_1W: up:mild:0.5
HORIZON_2W: up:mild:0.5
HORIZON_1M: up:notable:0.5
SECTOR_SCORES_END

HIT_GRID_BEGIN
Gold/silver price surge (monetary metals)|HIT|0.7|2026-08-26|GC=F +0.89%, AU +8.5%, Barrick +8.2% on softer data/FOMC anticipation
Industrial metal price surge (copper/aluminum/iron ore)|PARTIAL|0.5|2026-08-26|Copper firm ~$6.60/lb but off record; LME stocks rebuilt to ~240kt
China demand shock / property stress|HIT|0.6|2026-08-26|July NBS mfg PMI 49.2, construction 47.0; property/F AI draining copper demand
Inventory draw (LME/exchange stocks down)|MISS|0.6|2026-08-26|LME copper stocks rebuilt to ~240kt from ~205kt mid-August
Supply disruption (mine/export ban)|PARTIAL|0.4|2026-08-26|DRC concentrate ban on books but narrow vs cathode exports
Critical-minerals policy / domestic tariff support|HIT|0.5|2026-08-26|Section 232 copper/semi-finished still in force (carried)
Risk-on tape / equity beta expansion|MISS|0.5|2026-08-26|ES -0.07%, NQ -0.15%; flat futures, tech-led, not materials-led
Sector breadth failure (ETF up, names flat)|PARTIAL|0.5|2026-08-26|XLB 1d flat vs SPY +0.32% (rel -0.32%); premarket LIN/FCX/NEM mixed
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.8, 'leading_sum': 3.0, 'divergence_flagged': False, 'total_score': 2.0, 'predicted_direction': 'up', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.45, 'regime': 'mixed'}
```
