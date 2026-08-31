# Sector Prediction — Consumer Cyclical — 2026-08-31

- ETF: **XLY**
- rubric: `00_grounding/sectors/consumer_cyclical.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-4.5** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLY vs SPY (yfinance, through 2026-08-31):
  1d: XLY +0.82% | SPY -0.68% | rel +1.50%
  3d: XLY -0.95% | SPY -0.01% | rel -0.94%
  1w: XLY -1.01% | SPY +0.02% | rel -1.03%
  1m: XLY +3.95% | SPY +3.26% | rel +0.69%
```

MEMORY_CONFIRM: Memory index paused (embedding metadata missing); used injected Consumer Cyclical scoreboard + standing lessons only. Rolling dir=0.6 / mag=0.3 (n=10). Last graded: 2026-08-28 down/mild vs XLY +1.15% / SPY −0.23% / rel +1.37% (dir MISS). Scope DO-INSTEAD after that loss: when score sign conflicts with tape/breadth, prefer flat/mild. Active THIS-scope lessons: (1) **08-28 inherited-lag lesson — FIRES**: S0=0, stale S1, do not triple-count a completed prior-session lag into S2/S3/S4; prefer flat/mild unless a live premarket AMZN/TSLA/HD breakdown is confirmed. (2) **08-27 NVDA/XLK-futures map — FIRES as a ban on S0=+1 / 08-21-up**: leftover AI impulse is a non-holdings print; XLY 1d/3d/1w/1m rel already red; map S0 from AMZN/TSLA/HD, let S4 go negative, default down/mild or down/flat. (3) **08-21 reversal — does NOT fire** (ES=F −0.45%, NQ=F −0.17%, not ≥ +0.3%). (4) **08-11 oil-shock — does NOT fire cleanly** (CL +2.58% 1d but BZ −1.3%; WTI $85.03; Hormuz terms headline is live but crude is mixed, not a clean $90+ squeeze). (5) **08-18 severe-cap** — ceiling only; no same-morning mega-cap breakdown that authorizes severe. (6) **08-25 sector-owned print** — no same-day consumer print today (jobs week ahead). (7) **08-17 notable-on-stale-spine** — does not force notable: July retail is 17 days old; retailer-earnings week is over.

# Consumer Cyclical (XLY) — 2026-08-31

Object is the **near-session XLY environment**, not SPX and not a stock pick. XLY remains AMZN ~24% + TSLA ~16% + HD ~5.5% (~46% combined). Score **broad consumer health**, not a single name.

## Channel 1 (used as given)

- Tape: XLY vs SPY **1d rel +1.50%** (XLY +0.82% vs SPY −0.68%), **3d −0.94%**, **1w −1.03%**, **1m +0.69%**. Note the **1d flip to positive relative** — this is the first green 1d rel after a multi-horizon fade. Absolute 1d: XLY +0.82% vs SPY −0.68%.
- Macro: VIX 15.15 (+0.64d); VIX/VIX3M 0.856 (contango); DGS10 4.67 / DGS30 5.19 (as-of 08-27); **DFII10 2.34, 1d 0 / 1w −0.01 / 1m −0.07**; HY 2.60 (−0.03d); **CL=F +2.58% 1d / BZ=F −1.3% 1d** (mixed crude); WTI $85.03 (+1.97%); gold −0.81%; DXY −0.19%d / −0.58% 1m; **ES=F −0.45% / NQ=F −0.17%** (mildly negative); Asia +0.23% (Kospi +1.53% idiosyncratic); Europe −0.27%; F&G 58.2 Greed; 10Y–SPX corr −0.538 (strongly negative).

## Channel 2

**1. Shared macro as it hits THIS sector (S0)**
The dominant macro driver is **Warsh/Jackson Hole hawkish repricing** (News Judge #1): September hike odds ~coin flip, yields rising, long-duration/growth hit hardest. This is the **live rate spine** for the growth-heavy AMZN/TSLA sleeve. Real yields are **flat 1d** (DFII10 0), not a fresh squeeze, but the hawkish Fed signal is the session's regime driver.

Futures are **mildly negative** (ES −0.45%, NQ −0.17%) — not an 08-21 recovery tape. Oil is **mixed** (CL +2.58% but BZ −1.3%; WTI $85.03 on Hormuz terms headline) — a live geopolitical/oil supply-risk headline (Iran naming Hormuz terms) that can flip the tape risk-off. Per 08-11, an active geopolitical/oil supply shock should make S0 more negative for Consumer Cyclical. However, the crude move is mixed (CL up, Brent down) and not a clean $90+ squeeze, so this is a **risk-off overlay, not a full 08-11 trigger**.

Per 08-27, do not map green futures onto XLY when the impulse is non-holdings — here futures are **red**, so the reverse applies: do not assume XLY follows a tech-led fade either (08-28: XLK fade → AMZN/XLY relative bid). The hawkish Fed + rising yields is a **negative** for the growth-heavy basket, but the **1d tape already flipped positive relative** (+1.50%), suggesting AMZN/TSLA are participating. **S0 = −1** (hawkish rate spine + geopolitical oil overlay, capped at −1 not −2 because crude is mixed and no clean Hormuz squeeze).

**2. Spine + secondary (S1)**
- **Retail miss / traffic down — HIT, stale:** July retail sales **−0.6% m/m** vs +0.1% (08-14); first drop in nine months.
- **Consumer confidence collapse — HIT, stale:** Conference Board Aug **89.4** (Expectations 68.2, seven-month low, 08-25); UMich final **51.7** (08-28). Both soft, both stale.
- **Employment / wage support — HIT:** claims **203k** (week of 08-22), 4-wk avg ~205.5k. No claims print today (jobs week ahead).
- **Credit tightening / delinquency rise — HIT, carried:** TransUnion Q2 bankcard 90+ DPD **2.26%** (+9 bp YoY).
- **Jobless claims / unemployment spike — checked, nothing material.**
- **Gasoline spike crushing discretionary — MIXED:** CL +2.58% 1d on Hormuz terms headline; WTI $85.03. This is a **live energy-cost risk** — the 08-11 lesson says set the live energy-cost factor negative when oil is spiking. But Brent is −1.3% and the move is not a clean $90+ squeeze. Score as a **mild negative**, not a hard kill.
- **Auto SAAR / inventories — HIT:** Cox Aug SAAR ~**16.3M**, resilient affluent/cash buyer.
- **Travel / hotel RevPAR beat — HIT:** STR week of 08-22 RevPAR **+4.4% YoY** to $106.12 (19th straight up week).
- **Retail sales / card spend upside — miss.** Consumer confidence jump — miss. Credit easing — checked, nothing material.
- Same-morning color: **AMZN** — Evercore ISI raises PT to $355 from $315, reiterates Buy on stronger retail trends (positive, single-name, carried from 08-28). **ABNB** — Bernstein/Evercore PT raises (carried). No fresh AMZN/TSLA/HD earnings print knowable at the open.

**Net S1 = −1.** Stale soft-consumer cluster (retail miss, sentiment, credit) is the backdrop. Live oil uptick on Hormuz terms is a mild negative. Labor + auto + RevPAR + AMZN PT raise offset. Per 08-28, do not let a stale spine set direction on its own — but here the live hawkish-Fed + oil overlay is the session driver, and the 1d tape flip is the key new input.

**3. Breadth / leadership (S2)**
The **1d relative flip to +1.50%** is the key signal — XLY outperformed SPY on a down day, suggesting **AMZN/TSLA/HD are participating** (not ETF-only carry of one name). This is a **high-beta leadership** day (XLY up while SPY down). However, the 3d/1w/1m windows still lag, so this is a **1d bounce in a multi-horizon laggard**, not a durable breadth expansion. Large-cap leadership persists by construction. **S2 = 0** (1d flip is real but not yet a % names expansion; 3d/1w/1m still red).

**4. Flows / positioning (S3)**
XLY **+$111M 1-month**, **+$739M 3-month** inflows (ainvest, carried) — positive recent flow signal. YTD still −$778M (structural rotation out of discretionary over the year). 1m rel +0.69% is no longer extended after the fade. No fresh same-day creation spike. **S3 = 0** (carried flows, no fresh signal).

**5. Earnings / policy**
No XLY-bellwether earnings this morning. **Jobs week ahead** (NFP Friday) — a forward catalyst, not today's driver. **Warsh Jackson Hole 10:00 ET** — the live two-sided policy binary. Salesforce beat (software) is a non-holdings catalyst. **Checked, nothing material** that should flip S1 on its own.

## Self-audit

- **Lens:** high-beta cyclical into a hawkish-Fed + geopolitical-oil overlay, with a **1d relative flip** as the key new input.
- **Band:** mild ceiling. Mag hit-rate 0.3. 08-18 forbids severe while AMZN/TSLA are not broadly breaking down.
- **Same-shock double-count:** The hawkish-Fed + oil overlay is counted once in S0 (macro); the consumer fundamentals (retail miss, sentiment, credit) are counted in S1. Not double-weighted.
- **Single-ticker:** AMZN PT raise is one name; the 1d flip is broad (XLY +0.82% vs SPY −0.68%), so this is not single-ticker domination.
- **Divergence:** Leading factors (negative consumer spine + hawkish macro) fight the **1d tape flip** (positive relative). Per 08-28, do not triple-count a completed prior-session lag — but here the 1d tape has **flipped positive**, so the inherited-lag lesson does not fire as a down mandate. The 08-27 map (don't map NVDA into XLY) is a ban on S0=+1, not a license to call down when the tape has flipped.

## Divergence Note
Leading factors (negative consumer spine + hawkish macro) fight the **1d tape flip** (XLY +1.50% rel). Per the scope DO-INSTEAD ("when score sign conflicts with tape/breadth, prefer flat/mild"), and given the 1d flip is the freshest signal, I'll lean **flat/mild** rather than a confident down call. The hawkish-Fed + oil overlay is real but the tape is showing XLY resilience (AMZN/TSLA participating). This is a **flat-to-mild** call with modest confidence.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.5
REGIME: mixed
TOTAL_SCORE: -1.8
PREDICTED_DIRECTION: flat
PREDICTED_MAGNITUDE_BAND: mild
HORIZON_3D: flat:mild:0.5
HORIZON_1W: flat:mild:0.5
HORIZON_2W: up:mild:0.5
HORIZON_1M: up:mild:0.55
SECTOR_SCORES_END

HIT_GRID_BEGIN
Retail miss / traffic down|HIT|0.8|2026-08-14|Census July retail sales -0.6% m/m
Consumer confidence collapse|HIT|0.8|2026-08-25|Conference Board 89.4 / Expectations 68.2
Employment / wage support for discretionary|HIT|0.7|2026-08-27|Claims 203k, 4-wk avg ~205.5k
Credit tightening / delinquency rise|HIT|0.7|2026-08-28|TransUnion Q2 bankcard 90+ DPD 2.26%
Auto SAAR / dealer inventory healthy|HIT|0.6|2026-08-28|Cox Aug SAAR ~16.3M
Travel / hotel RevPAR beat|HIT|0.6|2026-08-28|STR RevPAR +4.4% YoY
Gasoline spike crushing discretionary|HIT|0.5|2026-08-31|CL +2.58% on Hormuz terms; WTI $85.03
Risk-off tape / flight to safety|HIT|0.6|2026-08-31|Hawkish Warsh + Hormuz terms headline
Real yields rising|HIT|0.5|2026-08-31|Hawkish Fed repricing, hike odds coin flip
Sector rotation into discretionary|HIT|0.5|2026-08-31|1d rel flip +1.50% (XLY +0.82% vs SPY -0.68%)
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': -5.0, 'divergence_flagged': False, 'total_score': -4.5, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.5, 'regime': 'mixed'}
```
