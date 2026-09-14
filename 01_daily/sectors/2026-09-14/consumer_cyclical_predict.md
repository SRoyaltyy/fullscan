# Sector Prediction — Consumer Cyclical — 2026-09-14

- ETF: **XLY**
- rubric: `00_grounding/sectors/consumer_cyclical.md`
- predicted_direction: **down**
- predicted_magnitude_band: **notable**
- total_score: **-13.101** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**
- engine: v2 · tape_anchor **-4.35** (ES -0.66%, ER2 -0.27%, NQ -1.59%, PM:XLY -0.65%) · index_carry **-2.751** (general -11.002) · llm_overlay **-6.0** (raw -7.2)

## Channel 1 sector ETF tape

```
ETF XLY vs SPY (yfinance, through 2026-09-11):
  1d: XLY +0.89% | SPY +0.85% | rel +0.04%
  3d: XLY -0.90% | SPY -0.22% | rel -0.69%
  1w: XLY -3.01% | SPY -1.15% | rel -1.86%
  1m: XLY -4.18% | SPY -1.06% | rel -3.12%
```

MEMORY_CONFIRM: Memory index paused (embedding metadata missing); used injected Consumer Cyclical scoreboard + standing THIS-scope lessons only. Rolling dir=0.5 / mag=0.2 (n=10); last graded 2026-09-11 down/flat vs XLY +0.89% / SPY +0.85% / rel +0.04% (dir MISS, mag MISS). Active THIS-scope lessons: (1) **09-11 prose/component consistency + two-component double-count lesson — FIRES HARD**: when futures confirm ≥+0.5% and the sector's live inputs align, S0 must not be scored negative; when a sector's live sign flips (oil spike→relief), S1 must be ≥0; if S0 and S1 both express the same object negatively, zero one; if the self-audit says "flat," the component sum must reconcile to flat. (2) **09-10 triple-count lesson — FIRES**: S0/S1/S4 must not all express the same macro object; when futures are flat/mixed the "cap at mild" must BIND; when the ETF is oversold (RSI <35, below 50-day, 1m rel ≤ −4%) a ≥3rd-session continuation is a mean-reversion setup, not momentum. (3) **08-11 oil-shock — FIRES HARD**: live crude spiking (WTI $102.29 +2.44%, Brent $107.33 +2.80%, RBOB +2.73%) on a fresh kinetic increment; S0 dominant and more negative for Consumer Cyclical; do NOT carry stale gas relief. (4) **08-21 reversal — does NOT fire** (ES −0.66%, NQ −1.59%, both negative; oil spiking). (5) **08-27 NVDA/XLK-map — FIRES as a ban on S0=+1** (moot, futures red; NQ −1.59% is the weak leg on a non-XLY AI/semis impulse). (6) **08-28 inherited-lag — does NOT fire** (S0 not 0; live oil shock). (7) **08-18 severe-cap** — ceiling; AMZN/TSLA/HD not confirmed breaking premarket in a way that authorizes severe. (8) **08-25 sector-owned print — does NOT fire** (no same-day consumer print; FOMC next week). (9) **08-17 notable-on-stale-spine — does NOT force notable** (July retail ~31d old). (10) **09-03/09-04 pending-macro asymmetry — does NOT fire** (no high-impact US macro print today; the driver is the oil shock + Warsh repricing). Scope experiment: keep direction, shrink confidence on modest |score|.

# Consumer Cyclical (XLY) — 2026-09-14

Object is the **near-session XLY environment**, not SPX and not a stock pick. XLY remains AMZN ~24% + TSLA ~17% + HD ~5.4% (~46% combined). Score **broad consumer health**, not a single name.

## Channel 1 (used as given)

- Tape: XLY vs SPY **1d rel +0.04%** (XLY +0.89% / SPY +0.85%), **3d −0.69%**, **1w −1.86%**, **1m −3.12%**. The 1d relative print is flat — XLY moved with SPY on Friday's CPI-in-line relief rally. 1w/1m remain deeply negative.
- Macro: **VIX 17.67** (1d +1.83, 1w +2.37); **VIX/VIX3M 1.135 — BACKWARDATION** (stress). DGS10 **4.95** (+0.12d / +0.16w / +0.25 1m); DGS30 **5.37** (+0.09d / +0.10w / +0.13 1m); **DFII10 2.55** (+0.09d / +0.10w / +0.12 1m — real yields rising hard). HY 2.70 (−0.01d). **5-day corr 10Y vs SPX −0.248** (weakly negative — yields less dominant than last week). **CL=F +3.05% / BZ=F +3.43%**; Finviz **WTI $102.29 (+2.44%) / Brent $107.33 (+2.80%)**; **RBOB +2.73%**. DXY 99.245 (+0.41%d / −0.39% 1m). **ES −0.66% / NQ −1.59%** (red, NQ the weak leg). Asia composite **−0.72%** (Kospi −3.26% idiosyncratic, Nikkei −0.81%); Europe −0.35%. Gold −1.26%, Silver −2.17%, Copper −1.44%. **XLY premarket −0.65%** vs XLE +1.50%, XLP +0.61%, XLRE +0.48%, XLU +0.33%, XLF +0.33%, XLI −1.13%, XLK −1.95%.

## Channel 2

**1. Shared macro as it hits THIS sector (S0)**
This is a **live, escalating geopolitical/oil supply shock** — the fourth session of the same regime, and the most severe crude print yet. Finviz confirms **WTI $102.29 (+2.44%)**, Brent **$107.33 (+2.80%)**, RBOB **+2.73%**, heating oil +2.67%, gasoil +2.35%. Channel 1 crude agrees (CL +3.05%, BZ +3.43%). Per 08-11, S0 is the dominant score and **more negative** for Consumer Cyclical: gasoline transmission + discretionary demand destruction. The 08-11 lesson is explicit — "treat S0 as the dominant score and make it more negative for Consumer Cyclical (e.g., S0 = −2). Set the live energy-cost factor negative when oil is spiking rather than carrying stale 'gas relief.'"

The rates channel reinforces it. News Judge #1 (**Warsh Jackson Hole → September hike odds up; gold −3%**) and #2 (**rising yields = key risk for Russell 2000 / small caps**) are the same rates object: real yields **rising hard** (DFII10 +0.09d / +0.10w / +0.12 1m), DGS10 +0.25 1m, DGS30 +0.13 1m. Rising long-end yields are a direct duration headwind for the AMZN/TSLA growth sleeve (macro map: real yields up − for this growth-heavy basket). VIX/VIX3M **backwardation at 1.135** is a genuine stress tell, and VIX +1.83d / +2.37w confirms it.

Futures are **red** (ES −0.66%, NQ −1.59%) — not an 08-21 recovery tape. Per 08-27, NQ being the weak leg on a non-XLY AI/semis impulse (Kospi −3.26%, XLK −1.95%, APH −6.5% on Fabrinet weakness) must **not** be mapped into S0=+1 for XLY — but the reverse also holds: the NQ weakness is exogenous to XLY's book, so it should not be triple-counted as a consumer-specific negative either. XLY premarket −0.65% is roughly SPY-beta, not a sector-specific breakdown.

**S0 = −2.** Live oil shock (WTI >$102, Brent >$107) + hawkish Fed/rising real yields + backwardated VIX. This is the dominant driver. Not −3: futures are not confirming a ≥1% broad crash (ES −0.66%), and the shock is a continuation of the 09-08→09-11 regime rather than a brand-new overnight kinetic increment. Per the 09-10 lesson, S0/S1/S4 must not all express the same oil object — so S1 will carry the *transmission channel* only, and S4 will be scored on the tape, not restacked.

**2. Spine + secondary (S1)**
- **Gasoline spike crushing discretionary — HIT, LIVE:** WTI $102.29, Brent $107.33, RBOB +2.73%. The pump will rise further from the ~$4.14 level. This is the fresh spine hit and the transmission channel of the S0 oil object.
- **Retail sales / card spend upside — miss.** July Census **−0.6% m/m** vs +0.1% (08-14) remains the last hard spend print. August retail sales due mid-Sept.
- **Retail miss / traffic down — HIT, stale.** Same 08-14 print; 08-17 does not force notable from it alone.
- **Consumer confidence jump — miss.**
- **Consumer confidence collapse — HIT, stale:** Conference Board Aug **89.4** / Expectations **68.2** (08-25); UMich final **51.7** (08-28).
- **Employment / wage support — HIT, cooling:** claims **203k** (week of 08-22); ADP Aug **+38k** (slowest since January). NFP Friday was hot (+162k vs ~120k) — hawkish, not a wage-support positive for discretionary.
- **Jobless claims / unemployment spike — checked, nothing material.**
- **Credit tightening / delinquency rise — HIT, carried:** TransUnion Q2 bankcard 90+ **2.26%**.
- **Credit conditions easing — checked, nothing material.**
- **Auto SAAR / dealer inventory healthy — HIT:** Cox Aug SAAR ~**16.3–16.8M**, resilient affluent/cash buyer.
- **Travel / hotel RevPAR beat — HIT, cooling:** STR week of 08-22 RevPAR **+4.4% YoY** (streak intact, weakest since early May).
- **Sector rotation out of discretionary — HIT, structural:** 1w/1m rel deeply negative (−1.86%/−3.12%); YTD staples still lead. XLP +0.61% premarket vs XLY −0.65% is the live rotation tell.
- Same-morning color: **no fresh AMZN/TSLA/HD earnings.** AAPL PT cut to $370 (BofA, iPhone 18 margin pressure) is a consumer-tech sentiment negative but AAPL is not an XLY holding. ADBE record Q3 is XLK. No XLY-weight catalyst.

**Net S1 = −1.** Live gasoline spike is the fresh spine hit (the transmission channel of the S0 oil object, counted once); stale soft-consumer cluster (retail miss, confidence, credit) stays in the tape but does not get 08-17's −3 stack. Auto/travel resilience is a partial offset, not a flip.

**3. Breadth / leadership (S2)**
XLY is mega-cap concentrated (AMZN+TSLA ~46%). Multi-horizon lag (1w −1.86%, 1m −3.12%). Premarket shows XLY −0.65% roughly in line with SPY beta, not a sector-specific breakdown. No fresh same-day constituent/breadth data. Per the 08-28 inherited-lag lesson and the 09-10 triple-count lesson, the completed 1w/1m lag must **not** be restacked here. **S2 = 0.**

**4. Flows / positioning (S3)**
XLY has been a 1m laggard (−3.12% rel). Trailing outflows likely, but no fresh same-day flow print. Per 08-28, trailing outflows are not a 1-day lid. **S3 = 0.**

**5. ETF tape (S4)**
XLY 1d rel **+0.04%** (flat), 3d −0.69%, 1w −1.86%, 1m −3.12%. The freshest 1d print is **flat**, not confirming a fresh down leg. Per the 09-10 lesson, S4 must not restack the same oil object already in S0/S1. The 1d flat print is the honest tape read. **S4 = 0.**

**Total: −2 −1 +0 +0 +0 = −3.0 × 0.9 = −2.7 → down/mild.**

**Divergence check:** S0/S1 (leading factors) are negative; S4 (tape) is flat. This is a mild divergence — the factors point down, the tape is neutral. Per the shared method, trust factors over tape, but the flat tape + the 09-10 lesson (oversold ETF, ≥3rd-session continuation, flat futures cap) argue for capping magnitude at **mild**, not notable. XLY is oversold (1m rel −3.12%, below 50-day), so the multi-horizon lag is a mean-reversion setup, not momentum confirmation.

**Self-audit:** (a) Lens = near-session XLY environment, not SPX. (b) Band = mild, capped by the flat 1d tape and the 09-10 continuation/oversold rule. (c) Skew = down, driven by the live oil shock. (d) Same-shock double-count = the oil object is scored once in S0 (macro overlay) and once in S1 (gasoline transmission channel) — the 09-10 lesson permits this separation but forbids a third vote in S4; S4 is set to 0. (e) Single-ticker = no single name drives the call; AMZN/TSLA/HD are not confirmed breaking premarket.

**Confidence: 0.55.** Direction is well-supported by the live oil shock and hawkish rates; magnitude is uncertain given the flat 1d tape and the oversold mean-reversion setup.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -2
S1_SECTOR_FACTORS: -1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: risk_off
SECTOR: Consumer Cyclical
ETF: XLY
PREDICTED_DIRECTION: down
PREDICTED_MAGNITUDE_BAND: mild
TOTAL_SCORE: -2.7
DIVERGENCE_FLAGGED: True
SECTOR_SCORES_END

HIT_GRID_BEGIN
Gasoline spike crushing discretionary|HIT|0.85|2026-09-14|https://finviz.com/futures
Retail miss / traffic down|HIT_STALE|0.7|2026-08-14|https://www.census.gov/retail
Consumer confidence collapse|HIT_STALE|0.7|2026-08-25|https://www.conference-board.org
Credit tightening / delinquency rise|HIT_CARRIED|0.6|2026-08-11|https://www.transunion.com
Employment / wage support for discretionary|HIT|0.6|2026-08-27|https://www.dol.gov/ui/data.pdf
Auto SAAR / dealer inventory healthy|HIT|0.6|2026-09-02|https://www.coxautoinc.com
Travel / hotel RevPAR beat|HIT|0.55|2026-08-28|https://str.com
Sector rotation out of discretionary|HIT|0.7|2026-09-14|https://finviz.com
Real yields rising|HIT|0.8|2026-09-10|https://fred.stlouisfed.org/series/DFII10
Risk-off tape / flight to safety|HIT|0.7|2026-09-14|https://finviz.com/futures
Retail sales / card spend upside|MISS|0.7|2026-08-14|https://www.census.gov/retail
Consumer confidence jump|MISS|0.7|2026-08-25|https://www.conference-board.org
Credit conditions easing for consumers|MISS|0.6|2026-09-14|https://fred.stlouisfed.org
Jobless claims / unemployment spike|MISS|0.6|2026-08-27|https://www.dol.gov/ui/data.pdf
Sector breadth expansion (% names up)|MISS|0.5|2026-09-14|https://finviz.com
Sector ETF inflow / relative volume spike|MISS|0.5|2026-09-14|https://finviz.com
HORIZON_3D: down/mild — oil shock + hawkish rates persist; XLY oversold, mean-reversion risk caps downside
HORIZON_1W: down/mild — FOMC next week is the swing; a dovish hold relieves the duration headwind
HORIZON_2W: flat/mild — oversold XLY with 1m rel −3.12% is a mean-reversion setup if oil retreats
HORIZON_1M: flat — structural rotation out of discretionary vs staples persists; no fresh consumer-spend catalyst until August retail sales
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -2.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': -7.0, 'divergence_flagged': False, 'total_score': -13.101, 'predicted_direction': 'down', 'predicted_magnitude_band': 'notable', 'confidence_score': 0.85, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': -0.725, 'score': -4.35, 'legs': [{'leg': 'ES', 'pct': -0.66, 'w': 0.8}, {'leg': 'ER2', 'pct': -0.27, 'w': 0.2}, {'leg': 'NQ', 'pct': -1.59, 'w': 0.2}, {'leg': 'PM:XLY', 'pct': -0.65, 'w': 0.7}]}, 'overlay_score': -6.0, 'overlay_raw': -7.2, 'index_carry': -2.751, 'general_total': -11.002, 'skill_multipliers': {'S0_SHARED_MACRO': 1.25, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.25}, 'llm_confidence': 0.55}
```
