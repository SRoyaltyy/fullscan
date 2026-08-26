# Sector Prediction — Energy — 2026-08-26

- ETF: **XLE**
- rubric: `00_grounding/sectors/energy.md`
- predicted_direction: **down**
- predicted_magnitude_band: **notable**
- total_score: **-10.0** (mult 1.0)
- regime: mixed
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLE vs SPY (yfinance, through 2026-08-25):
  1d: XLE -1.66% | SPY +0.32% | rel -1.98%
  3d: XLE -2.65% | SPY +0.43% | rel -3.09%
  1w: XLE -2.54% | SPY -0.20% | rel -2.34%
  1m: XLE +6.34% | SPY +3.63% | rel +2.71%
```

MEMORY_CONFIRM: Sector Energy (XLE) — n=9 graded, dir=0.556 mag=0.333; last graded 08-25 down/notable vs XLE −1.66% (dir HIT, mag HIT per reflect, though scoreboard recorded a false None/None miss). 08-24 down/notable still ungraded. Applied: 08-11 live-oil verify — Channel 1 CL=F −2.5% / BZ=F −3.94% matches live WTI/Brent down sharply; 08-14 green-oil escalation does **not** fire (oil is DOWN); 08-12/08-13 stale-run cap does **not** fire (1w rel −2.34% is negative, not a >+4% cushion); 08-21 oil-up/XLE-down decoupling is inverted today (oil and ETF aligned lower); 08-24/08-25 oil-down spine pattern is the operative template. Open Energy experiment: keep direction, shrink confidence after mag misses. Memory index unavailable; used injected sector scoreboard/lessons only.

## Energy / XLE — 2026-08-26

This is a **third consecutive oil-down, premium-not-transmitting** session. Crude is the load-bearing spine and it is breaking down hard (CL −2.5%, BZ −3.94%). The dominant calendar catalyst today is **July core PCE** (8:30 ET) plus the **EIA weekly** print — both two-sided but with a hawkish Fed backdrop (Collins "hike may be needed," regional directors sought hike). Do not run the 08-14 squeeze playbook.

### Channel 2

**1. Shared macro as it hits energy.** Broad tape is roughly flat-to-mildly-soft: ES −0.07%, NQ −0.15%, Asia +0.47%, Europe +0.18%, VIX 15.69 (+0.24), Fear & Greed 58.6. DXY +0.1% 1d is a mild commodity headwind (1m still −2.33%). DFII10 2.38, −0.02 1d — real yields easing slightly, secondary vs oil. The dominant calendar is **today**: July core PCE + EIA weekly (week ending 8/21). News Judge is explicit: Iran sanctions/Hormuz copy is live **while oil is falling**, so Hormuz-up Energy lessons do not fire. For this cyclical, a flat/soft tape is not a tailwind; the oil spine dominates. **S0 muted at 0.**

**2. Spine (S1).** Count the oil shock **once**.
- **Crude collapse (live-verified):** CL=F −2.5%, BZ=F −3.94%. Same sign as Channel 1 — 08-11 check passes. This is the third straight down session for the barrel.
- **Inventory build (carried):** EIA week ending 8/14 +4.4M bbl to 428.8M (third consecutive build; prior week +17.4M). **Next print today (8/26)** — a fresh catalyst that could confirm or reverse.
- **OPEC+ adding barrels:** Aug +188 kb/d; Sep another ~188 kb/d completing the 2023 voluntary-cut rollback.
- **Demand destruction (official, carried):** IEA Aug OMR 2026 demand **−1.6 mb/d**; OPEC trimmed 2026 growth to ~0.6 mb/d.
- **Geo premium not transmitting:** Iran blacklisted tankers; Hormuz transits near zero. That is **not** an oil-up HIT today. Score as fade/non-confirmation — **do not** also score Crude oil price surge.
- **Cracks still extreme** (diesel crack elevated) — **refiner offset only**; dampen for whole XLE. Do not let VLO/MPC carry the ETF while crude breaks down.
- **Nat gas ~$2.7** — no surge; N/A for oil-weighted XLE.

Net S1 = **−2**. Not −3: same oil print is not triple-counted; cracks still cushion refiners; PCE/EIA today are two-sided and could reverse the oil sign.

**3. Breadth.** XLE 1d rel −1.98%, 3d rel −3.09%, 1w rel −2.34% — the ETF is now **underperforming SPY across all near timeframes**, a decisive reversal from the Hormuz leadership. XOM/CVX/COP all red with the ETF. Leadership is failing, not expanding.

**4. Flows / crowding.** Energy-sector ETFs have seen sustained outflows (~$4B over ~65 days, largest streak since mid-2025). XLE AUM ~$39B. YTD ~+40% is still a **crowded long** unwinding after the Hormuz run. Rotation is **out of energy** into NQ/risk-on and gold (GC=F +0.89%).

**5. Catalysts.** No fresh XLE-wide earnings. **July core PCE (8:30 ET)** is the dominant two-sided catalyst — a hot print would pressure cyclicals further; a cool print could relieve. **EIA weekly (today)** is a fresh inventory catalyst. Neither flips the current oil-down sign at the open.

### Scoring logic

S0 is **0**: flat/soft tape, mild USD headwind, real yields easing slightly. Not −1 (no panic liquidation; VIX ~15.7). Not +1 (would fight the oil spine).

S1 is the oil spine, netted once: crude down + inventory build + OPEC+ add + IEA demand cut, minus a damped crack offset. Cap the geo headline because oil is not confirming. **S1 = −2.**

S2/S3/S4 all confirm the fade: internals red across XOM/CVX/COP, outflows/crowding after the run, 1d rel −1.98% and 3d rel −3.09%. **No leading-vs-tape divergence** — oil and the ETF are aligned lower. Trust factors.

Magnitude: oil drop is large (CL −2.5%, BZ −3.94%) and XLE is already underperforming on 1d/3d/1w. But PCE/EIA today are two-sided and could reverse the oil sign; cracks still bid refiners; Energy mag hit-rate is 0.333. Multiplier **1.0**. Confidence **0.58** (keep direction, shrink conviction per the Energy experiment).

Regime **mixed**: SPX beta is flat; the Energy call is the oil spine, not SPX.

Self-audit: lens = XLE/oil, not SPX; no same-shock triple-count; refiners not allowed to drive the ETF; no single-ticker call; PCE/EIA treated as two-sided event risk, not a one-way negative.

---

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: -2
S2_BREADTH: -1
S3_FLOWS_POSITIONING: -1
S4_ETF_TAPE: -1
MULTIPLIER: 1.0
CONFIDENCE: 0.58
REGIME: mixed
HORIZON_3D: down:mild:0.55
HORIZON_1W: down:mild:0.52
HORIZON_2W: flat:mild:0.48
HORIZON_1M: up:mild:0.50
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|MISS|0.70|2026-08-26|channel1 ES -0.07% / NQ -0.15%
Risk-off tape / flight to safety|PARTIAL|0.55|2026-08-26|channel1 VIX 15.69, F&G 58.6
Real yields falling|PARTIAL|0.55|2026-08-24|channel1 DFII10 2.38, 1d -0.02
USD strengthening|HIT|0.55|2026-08-26|channel1 DXY +0.1% 1d
Crude oil price surge (WTI/Brent)|MISS|0.85|2026-08-26|https://www.investing.com/commodities/crude-oil
Crude price collapse|HIT|0.85|2026-08-26|https://www.investing.com/commodities/crude-oil
Geopolitical supply risk premium|PARTIAL|0.55|2026-08-26|https://www.reuters.com/world/middle-east/iran-warns-vessels-violating-hormuz-transit-rules-fines-detention-2026-08-24/
Inventory build|HIT|0.85|2026-08-19|https://www.eia.gov/petroleum/supply/weekly/
Inventory draw (EIA crude/products)|MISS|0.85|2026-08-19|https://www.eia.gov/petroleum/supply/weekly/
OPEC+ production increase / quota break|HIT|0.70|2026-08-26|https://www.reuters.com/markets/commodities/opec-raises-output-august-2026/
Demand destruction (recession/China weak)|HIT|0.70|2026-08-13|https://www.iea.org/reports/oil-market-report-august-2026
Crack spread / refining margin expansion|PARTIAL|0.55|2026-08-26|channel1 diesel crack elevated
Sector ETF outflow / volume dry-up|HIT|0.70|2026-08-26|https://www.etf.com/sections/etf-flows
Crowded long (extreme relative performance + valuation)|HIT|0.65|2026-08-26|channel1 XLE 1m rel +2.71%, YTD ~+40%
Sector rotation out of energy|HIT|0.70|2026-08-26|channel1 XLE 1d/3d/1w rel all negative
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': -2.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': -1.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 1.0, 'leading_sum': -8.0, 'divergence_flagged': False, 'total_score': -10.0, 'predicted_direction': 'down', 'predicted_magnitude_band': 'notable', 'confidence_score': 0.58, 'regime': 'mixed'}
```
