# Sector Prediction — Utilities — 2026-08-31

- ETF: **XLU**
- rubric: `00_grounding/sectors/utilities.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-6.75** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLU vs SPY (yfinance, through 2026-08-31):
  1d: XLU -2.32% | SPY -0.70% | rel -1.61%
  3d: XLU -2.61% | SPY -0.03% | rel -2.58%
  1w: XLU -1.38% | SPY -0.01% | rel -1.37%
  1m: XLU -5.55% | SPY +3.23% | rel -8.79%
```

MEMORY_CONFIRM: Utilities/XLU only — rolling dir=0.4 / mag=0.4 (n=10); 08-28 down/mild vs XLU −1.04% / SPY −0.23% / rel −0.82% (dir HIT, mag MISS — hawkish Warsh resolution + PCG overlay, not knowable at open). Applied: 08-27 (PCE already out, mega-cap AI/software public, no fresh XLU catalyst → relative lag / flat-to-down absolute; do not pay carried easing in S0 and S1); 08-17 (no fresh catalyst → relative, not absolute); 08-13 (S2/S4 confirmation only); 08-12 (do not let AI-power inflate a 1d band); 08-14 (calendar: no 8:30 CPI/PCE/NFP today; Warsh already delivered 8/28); 08-21 (do not score easing off stale FRED — live 10Y not falling); 08-18 (bond-proxy + elevated long-end → flat-to-negative absolute); 08-11 (does not flip up: yields not easing, tape not inflecting); 08-25 (does not force flat: S1 and S4 are not both neutral — S1 is negative on rates-rising + rotation-away, S4 is decisive red). Memory index unavailable this run; used injected sector logs.

## Utilities (XLU) — 2026-08-31

Object is the **near-session XLU environment**, not SPX and not a stock pick.

### Channel 1 (trusted, not re-derived)

XLU vs SPY through 2026-08-31: **1d −2.32% / −0.70% (rel −1.61%)**; **3d −2.61% / −0.03% (rel −2.58%)**; **1w −1.38% / −0.01% (rel −1.37%)**; **1m −5.55% / +3.23% (rel −8.79%)**.

Macro panel: VIX **15.15** (+0.64); DGS10 **4.67** as of **2026-08-27** (+1 bp 1d / −2 bp 1w / +0 bp 1m); DGS30 **5.19** (+1 bp 1d); DFII10 **2.34** (flat 1d, −1 bp 1w, −7 bp 1m); HY 2.60 tight; EPU **311.48** (+194.51 1d — big spike); CL=F **+2.58%** / BZ=F **−1.3%** (WTI up sharply, Brent down — mixed); GC=F +0.11%; DXY +0.27% 1d; **ES=F −0.45% / NQ=F −0.17%** premarket (negative); Asia +0.23% (Kospi +1.53%, Shanghai +1.13%); Europe −0.27%; F&G 58.2 Greed; 5-day 10Y–SPX corr **−0.538**.

**Calendar (08-14 / 08-27):** Warsh Jackson Hole **already delivered 8/28** (hawkish, Sept hike odds ~55–57%). No 8:30 CPI/PCE/NFP today. Next week is **Jobs Week** (NFP). Today is a light calendar — the hawkish Fed spine is **carried**, not a fresh same-session impulse.

**Live curve (08-21 / 08-25 duration check):** 10Y ~**4.71%** (FT), 30Y ~**5.19%**. Sticky/higher, still in the long-end stress zone. **Not** a second-day easing impulse.

### Channel 2

**1. Shared macro → this sector.** The dominant driver is the **carried hawkish Warsh spine** (Sept hike odds ~55–57%, News Judge #1) — a direct long-duration headwind for bond-proxy utilities. Live 10Y ~4.71% is sticky/higher, not easing. **Iran/Hormuz reopening terms** (News Judge #3) is a live geopolitical/oil supply-risk headline — but oil is **mixed** (WTI +2.58%, Brent −1.3%), and this is a **two-sided** risk: it can flip the tape risk-off (a defensive bid for utilities) OR feed inflation (a rate headwind). Futures are **negative** (ES −0.45%, NQ −0.17%). EPU spiked +194 — elevated policy uncertainty. For a bond-proxy defensive, the hawkish Fed + sticky long-end is a **negative** S0; the Iran risk-off is a partial defensive offset but oil-up is inflationary. Net: **S0 = −1**.

**2. Spine / secondary.**
- **Data-center / power demand:** structural HIT, **stale**. No fresh same-day load catalyst. Rubric: do **not** let the multi-year AI-power story override a 1d rate tape without a fresh XLU catalyst.
- **Rates falling (bond-proxy bid):** **MISS**. Live 10Y ~4.71% not falling.
- **Rates rising (bond-proxy selloff):** **HIT**. Warsh hawkish, Sept hike odds ~57%, 10Y sticky ~4.71%, 30Y ~5.19%.
- **Risk-on rotation away from utilities:** **HIT**. Salesforce/software rally (ADBE +6%) is a fresh tech catalyst; Morningstar/DJ noted utilities down on rotation into tech on Nvidia's outlook.
- **Nuclear / gas generation policy support:** structural HIT, stale.
- **Grid CapEx approval / recovery:** structural HIT, stale.
- **Load growth disappointment / adverse rate case:** carried (WoodMac/Texas/Ohio, AEP/Oklahoma), not fresh. Single-name must not drive the ETF call.
Net: **Rates rising (bond-proxy selloff) + risk-on rotation away** are the dominant fresh factors. Structural positives are stale. **S1 = −1**.

**3. Breadth.** 1d rel −1.61%, 3d rel −2.58%, 1w rel −1.37%, 1m rel −8.79% (all negative). The 8/28 tape was broad (NEE/DUK/SO down with XLU; CEG did not save the ETF). No durable breadth expansion. **S2 = −1**.

**4. Flows.** Prior logs noted modest flows (5d ~+$45M, 1m ~+$62M through late Aug). No confirmed same-day inflow spike. De-risked on 1m (rel −8.79%). No confirmed inflow reversal. **S3 = 0** (neutral).

**5. Catalysts.** Warsh hawkish is **carried** (delivered 8/28). Iran/Hormuz is a live two-sided geopolitical/oil headline. Salesforce/software rally is a fresh tech catalyst (rotation away from utilities). No fresh XLU-wide rate-order/load print. **No fresh XLU catalyst.**

### Lessons → scores

**08-27 is the veto:** PCE out, mega-cap AI/software public, no fresh XLU catalyst → **relative lag / flat-to-down absolute**. Do not mint S0/S1 = +1 from carried easing. Today NQ is **not** leading ES (it's slightly negative), which only **weakens** another tech-rip day — it does **not** authorize up.

**08-18:** bond-proxy + elevated long-end → **flat-to-negative absolute**. Supports down.

**08-21:** do not score easing off stale FRED — live 10Y ~4.71% is not falling. No easing impulse.

**08-25 does not force flat:** S1 is not 0 (rates-rising + rotation-away are negative) and S4 is decisive red across 1d/3d/1w/1m.

**08-11 does not flip up:** yields are not easing; tape is not inflecting.

**08-12:** AI-power stays a dampener; band **mild**.

**08-14:** no 8:30 regime-flip print today; Warsh already delivered.

**Divergence:** leading (S0 −1 + S1 −1 + S2 −1 + S3 0 = −3) and S4 (−1) **agree**. No divergence.

**Self-audit:** rate lens over AI narrative on a 1d horizon; band capped at mild (no notable — this is not a repeat of the 8/28 hawkish-resolution smash, which was not knowable at open); no same-shock double-count of yields in S0 and S1 (both scored once); Iran/oil is two-sided, not a one-way FTS bid; Salesforce/software rotation is a fresh tech catalyst but not an XLU holding. Policy: last XLU losses were up-calls vs negative tape — cut conviction, prefer flat/mild; today is a down call with confirming tape, so direction is supported.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -1
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -1
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: mixed
HORIZON_3D: down:mild:0.5
HORIZON_1W: down:mild:0.45
HORIZON_2W: down:mild:0.42
HORIZON_1M: down:mild:0.4
SECTOR_SCORES_END

HIT_GRID_BEGIN
Rates rising (bond-proxy selloff)|HIT|0.8|2026-08-31|Warsh hawkish carried (Sept hike odds ~57%), live 10Y ~4.71% sticky
Risk-on rotation away from utilities|HIT|0.7|2026-08-31|Salesforce/software rally (ADBE +6%) fresh tech catalyst; Morningstar/DJ utilities down on tech rotation
Risk-off tape / flight to safety|PARTIAL|0.4|2026-08-31|Iran/Hormuz reopening terms live but oil mixed (WTI +2.6%, Brent −1.3%); two-sided, not a clean FTS bid
Data-center load growth / power demand upside|STALE|0.5|2026-08-31|Structural HIT but no fresh same-day XLU catalyst; do not override 1d rate tape
Sector breadth failure (ETF up, names flat)|HIT|0.6|2026-08-31|1d/3d/1w/1m rel all negative; broad utility decline
Sector ETF outflow / volume dry-up|NEUTRAL|0.4|2026-08-31|No confirmed same-day inflow spike; de-risked on 1m
HIT_GRID_END

**Direction: DOWN / Magnitude: MILD** — The carried hawkish Warsh spine + sticky long-end + risk-on rotation away from utilities (Salesforce/software) dominate the absolute close. The Iran/Hormuz headline is a live two-sided risk that could add a defensive bid, but oil-up is inflationary and futures are negative, so it does not flip the call. No fresh XLU catalyst, no easing impulse, and the 1d/3d/1w/1m relative tape is decisively negative. The 08-27 veto (relative lag / flat-to-down absolute) and 08-18 (bond-proxy + elevated long-end → flat-to-negative absolute) both support down. Band capped at mild — this is not a repeat of the 8/28 hawkish-resolution smash, which was not knowable at open.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -7.0, 'divergence_flagged': False, 'total_score': -6.75, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.55, 'regime': 'mixed'}
```
