# Sector Prediction — Consumer Cyclical — 2026-08-31

- ETF: **XLY**
- rubric: `00_grounding/sectors/consumer_cyclical.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-4.95** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLY vs SPY (yfinance, through 2026-08-27):
  1d: XLY -1.09% | SPY +0.66% | rel -1.75%
  3d: XLY -2.05% | SPY +1.00% | rel -3.05%
  1w: XLY -0.69% | SPY +1.11% | rel -1.80%
  1m: XLY +3.83% | SPY +5.71% | rel -1.88%
```

MEMORY_CONFIRM: Memory index paused (embedding metadata missing); used injected Consumer Cyclical scoreboard + standing lessons only. Rolling dir=0.6 / mag=0.3 (n=10). Last graded: 2026-08-28 down/mild vs XLY +1.15% / SPY −0.23% / rel +1.37% (dir MISS). Scope DO-INSTEAD after that loss: when score sign conflicts with tape/breadth, prefer flat/mild. Active THIS-scope lessons: (1) **08-28 inherited-lag lesson — FIRES**: S0=0, stale S1, do not triple-count a completed prior-session lag into S2/S3/S4; prefer flat/mild unless a live premarket AMZN/TSLA/HD breakdown is confirmed. (2) **08-27 NVDA/XLK-futures map — FIRES as a ban on S0=+1 / 08-21-up**: leftover AI impulse is a non-holdings print; XLY 1d/3d/1w/1m rel already red; map S0 from AMZN/TSLA/HD, let S4 go negative, default down/mild or down/flat. (3) **08-21 reversal — does NOT fire** (ES=F −0.3%, NQ=F −0.26%, not ≥ +0.3%). (4) **08-11 oil-shock — does NOT fire** (CL +3.75% 1d but Brent −0.45%; mixed, not a clean Hormuz squeeze; WTI $85.03). (5) **08-18 severe-cap** — ceiling only; no same-morning mega-cap breakdown that authorizes severe. (6) **08-25 sector-owned print** — no same-day consumer print today (jobs week ahead). (7) **08-17 notable-on-stale-spine** — does not force notable: July retail is 17 days old; retailer-earnings week is over.

# Consumer Cyclical (XLY) — 2026-08-31

Object is the **near-session XLY environment**, not SPX and not a stock pick. XLY remains AMZN ~24% + TSLA ~16% + HD ~5.5% (~46% combined). Score **broad consumer health**, not a single name.

## Channel 1 (used as given)

- Tape: XLY vs SPY **1d rel −1.75%**, **3d −3.05%**, **1w −1.80%**, **1m −1.88%**. Absolute 1d: XLY −1.09% vs SPY +0.66%. All four windows lag — a **multi-horizon relative fade**.
- Macro: VIX 15.22 (+0.71d); VIX/VIX3M 0.996 (bare backwardation); DGS10 4.67 / DGS30 5.19 (as-of 08-27); **DFII10 2.34, 1d 0 / 1w −0.01 / 1m −0.07**; HY 2.63 (−0.04d); EPU 131 (−141d, sharp drop); **CL=F +3.75% 1d / BZ=F −0.45% 1d** (mixed crude); WTI $85.03 (+1.97%); gold −0.81%; DXY −0.19%d / −0.45% 1m; **ES=F −0.3% / NQ=F −0.26%** (mildly negative); Asia +0.02% (Kospi −1.34% idiosyncratic); Europe +0.35%; 10Y–SPX corr +0.26.

## Channel 2

**1. Shared macro as it hits THIS sector (S0)**
The dominant macro driver is **Warsh/Jackson Hole hawkish repricing** (News Judge #1/#2): September hike odds ~coin flip, yields rising, long-duration/growth hit hardest. This is the **live rate spine** for the growth-heavy AMZN/TSLA sleeve. Real yields are **flat 1d** (DFII10 0), not a fresh squeeze, but the hawkish Fed signal is the session's regime driver.

Futures are **mildly negative** (ES −0.3%, NQ −0.26%) — not an 08-21 recovery tape. Oil is **mixed** (CL +3.75% but Brent −0.45%; WTI $85.03 +1.97% on Hormuz terms headline) — a live geopolitical/oil supply-risk headline (Iran naming Hormuz terms) that can flip the tape risk-off. Per 08-11, an active geopolitical/oil supply shock should make S0 more negative for Consumer Cyclical. However, the crude move is mixed (CL up, Brent down) and not a clean $90+ squeeze, so this is a **risk-off overlay, not a full 08-11 trigger**.

Per 08-27, do not map green futures onto XLY when the impulse is non-holdings — here futures are **red**, so the reverse applies: do not assume XLY follows a tech-led fade either (08-28: XLK fade → AMZN/XLY relative bid). The hawkish Fed + rising yields is a **negative** for the growth-heavy basket. **S0 = −1** (hawkish rate spine + geopolitical oil overlay, capped at −1 not −2 because crude is mixed and no clean Hormuz squeeze).

**2. Spine + secondary (S1)**
- **Retail miss / traffic down — HIT, stale:** July retail sales **−0.6% m/m** vs +0.1% (08-14); first drop in nine months.
- **Consumer confidence collapse — HIT, stale:** Conference Board Aug **89.4** (Expectations 68.2, seven-month low, 08-25); UMich final **51.7** (08-28). Both soft, both stale.
- **Employment / wage support — HIT:** claims **203k** (week of 08-22), 4-wk avg ~205.5k. No claims print today (jobs week ahead).
- **Credit tightening / delinquency rise — HIT, carried:** TransUnion Q2 bankcard 90+ DPD **2.26%** (+9 bp YoY).
- **Jobless claims / unemployment spike — checked, nothing material.**
- **Gasoline spike crushing discretionary — MIXED:** CL +3.75% 1d on Hormuz terms headline; WTI $85.03. This is a **live energy-cost risk** — the 08-11 lesson says set the live energy-cost factor negative when oil is spiking. But Brent is −0.45% and the move is not a clean $90+ squeeze. Score as a **mild negative**, not a hard kill.
- **Auto SAAR / inventories — HIT:** Cox Aug SAAR ~**16.3M**, resilient affluent/cash buyer.
- **Travel / hotel RevPAR beat — HIT:** STR week of 08-22 RevPAR **+4.4% YoY** to $106.12 (19th straight up week).
- **Retail sales / card spend upside — miss.** Consumer confidence jump — miss. Credit easing — checked, nothing material.
- Same-morning color: **AMZN** — Evercore ISI raises PT to $355 from $315, reiterates Buy on stronger retail trends (positive, single-name, carried from 08-28). **ABNB** — Bernstein/Evercore PT raises (carried). No fresh AMZN/TSLA/HD earnings print knowable at the open.

**Net S1 = −1.** Stale soft-consumer cluster (retail miss, sentiment, credit) is the backdrop. Live oil uptick on Hormuz terms is a mild negative. Labor + auto + RevPAR + AMZN PT raise offset. Per 08-28, do not let a stale spine set direction on its own — but here the live hawkish-Fed + oil overlay is the direction driver, not the stale spine.

**3. Breadth / leadership (S2)**
Premarket: AMZN/TSLA/HD not confirmed breaking down (no live premarket breakdown in the packet). XLY is a **multi-horizon relative laggard** (1d/3d/1w/1m all red). Per 08-28, do not triple-count the completed prior-session lag into S2. No fresh % names expansion or confirmed mega-cap breakdown. **S2 = 0** (do not re-vote yesterday's lag).

**4. Flows / positioning (S3)**
XLY **+$111M 1-month**, **+$739M 3-month** inflows (carried from prior runs) — positive recent flow signal. 1m rel −1.88% is now a **relative laggard** after the fade, not a crowded long. No fresh same-day creation spike. **S3 = 0** (carried flows, not a 1-day lid).

**5. Earnings / policy**
No XLY-bellwether earnings this morning. **Jobs week ahead** (NFP Friday) — a two-sided macro binary, not a same-morning print. **Warsh/Jackson Hole** is the live policy driver (hawkish). **Hormuz terms** headline is live geopolitical/oil risk. No fresh consumer print today. **Checked, nothing material** that should flip S1 on its own.

## Self-audit
- **Lens:** high-beta cyclical into a hawkish-Fed + geopolitical-oil overlay, with XLY a multi-horizon relative laggard.
- **Band:** mild/flat ceiling. Mag hit-rate 0.3. 08-18 forbids severe while AMZN/TSLA are not confirmed breaking down. 08-28 forbids triple-counting the completed lag.
- **Same-shock double-count:** The hawkish-Fed rate spine is counted once in S0; the stale consumer fundamentals in S1; the oil uptick is a live S1 factor (not double-counted in S0). Not double-weighted.
- **Single-ticker:** AMZN PT raise is single-name, not a sector driver. Do not let it set direction.
- **Divergence:** Leading factors (hawkish Fed + oil overlay + stale soft consumer) are negative; the tape is also negative (multi-horizon lag). No divergence — factors and tape agree on a down bias, but magnitude is capped at mild given no confirmed mega-cap breakdown and the 08-28 lesson against extrapolating the completed lag.

## Divergence Note
No leading-vs-tape divergence — both point down. Per the 08-28 lesson, do not triple-count the completed prior-session lag into S2/S3/S4. The direction driver here is the **live hawkish-Fed + geopolitical-oil overlay** (S0) plus the stale soft-consumer spine (S1), not the inherited tape. Magnitude capped at mild (no confirmed AMZN/TSLA/HD breakdown, no clean Hormuz squeeze, mag hit-rate 0.3).

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -1
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: risk_off
TOTAL_SCORE: -2.7
PREDICTED_DIRECTION: down
PREDICTED_MAGNITUDE_BAND: mild
HORIZON_3D: down:mild:0.5
HORIZON_1W: down:mild:0.5
HORIZON_2W: flat:mild:0.4
HORIZON_1M: flat:mild:0.4
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-off tape / flight to safety|HIT|0.6|2026-08-31|Warsh hawkish + Hormuz terms headline
Real yields rising|HIT|0.5|2026-08-31|Hawkish Fed repricing, DFII10 flat 1d but rate spine negative
Retail miss / traffic down|HIT|0.6|2026-08-14|July retail sales -0.6% m/m
Consumer confidence collapse|HIT|0.6|2026-08-25|CB 89.4 / Expectations 68.2; UMich final 51.7
Credit tightening / delinquency rise|HIT|0.5|2026-08-27|TransUnion Q2 bankcard 90+ DPD 2.26%
Gasoline spike crushing discretionary|HIT|0.5|2026-08-31|CL +3.75% on Hormuz terms; WTI $85.03
Employment / wage support for discretionary|HIT|0.5|2026-08-27|Claims 203k, 4-wk avg ~205.5k
Auto SAAR / dealer inventory healthy|HIT|0.5|2026-08-27|Cox Aug SAAR ~16.3M
Travel / hotel RevPAR beat|HIT|0.5|2026-08-27|STR week RevPAR +4.4% YoY
Sector breadth failure (ETF up, names flat)|MISS|0.0|2026-08-31|No confirmed mega-cap breakdown; do not triple-count lag
Sector ETF inflow / relative volume spike|MISS|0.0|2026-08-31|Carried 1m/3m inflows, no fresh same-day spike
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -5.0, 'divergence_flagged': False, 'total_score': -4.95, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.55, 'regime': 'risk_off'}
```
