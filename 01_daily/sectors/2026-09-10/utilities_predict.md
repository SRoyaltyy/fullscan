# Sector Prediction — Utilities — 2026-09-10

- ETF: **XLU**
- rubric: `00_grounding/sectors/utilities.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-4.725** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLU vs SPY (yfinance, through 2026-09-09):
  1d: XLU -1.17% | SPY -0.46% | rel -0.71%
  3d: XLU -0.21% | SPY -1.39% | rel +1.18%
  1w: XLU +0.89% | SPY +0.08% | rel +0.81%
  1m: XLU -0.44% | SPY -1.38% | rel +0.93%
```

MEMORY_CONFIRM: Utilities/XLU only — rolling dir=0.4 / mag=0.4 (n=10); last graded 09-09 flat/flat vs XLU −1.17% / SPY −0.46% / rel −0.71% (dir MISS, mag MISS — static-shock cushion reverted, rates dominated). Applied: 09-09 (cushion built on a STATIC shock = mean-reversion fuel, not a flat override; score S1 negative when the shock is not escalating); 09-08 (oil at elevated $90+ = inflation/duration unwind, not an FTS bid); 08-18 (risk-off + rising long-end → relative beat / flat-to-negative absolute; do not mint absolute up); 08-21 (live curve, not stale FRED 1d); 08-27 (do not pay carried easing in S0 and S1; no fresh XLU catalyst → relative lag); 08-25 (S0=S1=0 → do not manufacture down from carried lag); 08-12 (AI-power is a 1d dampener, not a band engine); 08-13 (S2/S4 confirmation only); 08-28 (do not promote a single-name regulatory item into S1); 09-03/09-04 (widen band when a high-impact catalyst is live; calendar scan must include all employment/rates releases). Open experiment: require one extra confirming source in the dominant bucket before full weight — used live 10Y/30Y + oil + VIX backwardation. Memory index unavailable this run; used injected sector logs + Channel 1 + web search.

## Utilities (XLU) — 2026-09-10

Object is the **near-session XLU environment**, not SPX and not a stock pick.

### Channel 1 (trusted, not re-derived)

XLU vs SPY through 2026-09-09: **1d −1.17% / −0.46% (rel −0.71%)**; **3d −0.21% / −1.39% (rel +1.18%)**; **1w +0.89% / +0.08% (rel +0.81%)**; **1m −0.44% / −1.38% (rel +0.93%)**.

Macro panel: VIX **16.51** (+0.05 1d, **+2.19 1w**) with **VIX/VIX3M 1.079 — backwardation**; DGS10 **4.80** (as of 09-08; +2 bp 1d, **+5 bp 1w, +15 bp 1m**); DGS30 **5.25** (+1 bp 1d, flat 1w, +6 bp 1m); DFII10 **2.43** (flat 1d, −1 bp 1w, +3 bp 1m); HY 2.67 tight; EPU 263.53 (−202.84 1d, +56.58 1m); **CL=F +1.38% / BZ=F +0.76%** (WTI $97.44, **Brent $102.08**); GC=F −0.56% / Silver −2.43% / Copper −2.89%; DXY 98.53 (−0.02%); **ES=F +0.11% / NQ=F −0.17%**; Asia composite **−0.56%** (Nikkei +0.2%, HangSeng −1.27%, ASX −1.03%); Europe composite **−0.06%**; 5-day 10Y–SPX corr **−0.969** (strongly negative); F&G unavailable.

**Live curve:** 10Y ~**4.80%** (20-month high zone), 30Y ~**5.25%** (19-year high zone). **Sticky-high, not easing.** Bond futures are **red** (10Y note −0.12%, 30Y bond −0.32%) — yields grinding higher, not a duration-relief impulse.

**Calendar:** No 8:30 CPI/PCE/NFP today. **10Y Treasury auction** is the live supply event (can push the long end). No FOMC. The dominant driver is the **live oil shock (Brent >$102) + hawkish Fed repricing**, not a scheduled print.

### Channel 2

**1. Shared macro → this sector.** The dominant driver is the **live oil shock** — Brent **$102.08**, WTI **$97.44**, oil crossing $101 on Iran-war escalation (News Judge #1). This is a **stagflation impulse**: oil → inflation expectations → long-end yields up (10Y 4.80%, 30Y 5.25%, both in multi-decade stress zones) → a **direct duration headwind** for bond-proxy utilities. The 09-08 lesson applies: at **elevated** oil ($90+), the oil spike is an **inflation/duration negative**, not a flight-to-safety bid. The 08-18 frame (risk-off + rising long-end → relative beat / flat-to-negative absolute) is the operative one: XLU can **outperform SPY relatively** on a defensive bid while **falling in absolute terms** if the long end keeps grinding. Futures are **mixed** (ES +0.11%, NQ −0.17%), Europe flat, Asia red, VIX backwardated (16.51, +2.19 1w) — a mildly risk-off, rates-pressured tape. **S0 = −1** (rates/duration headwind dominant; the defensive bid is relative-only, not an absolute-up license).

**2. Spine / secondary.**
- **Data-center load growth / power demand upside:** structural HIT, **stale** for a 1d call. No fresh same-day XLU-wide catalyst. Rubric: do **not** let the multi-year AI-power story override a 1d rate tape.
- **Rates falling (bond-proxy bid):** **MISS**. Live 10Y 4.80% / 30Y 5.25%, bond futures red. Not falling.
- **Rates rising (bond-proxy selloff):** **HIT**. Oil $102 → inflation → long-end up; 10Y +15 bp 1m, 30Y +6 bp 1m. One shock, counted once.
- **Risk-on rotation away from utilities:** **MISS**. Tape is mildly risk-off (NQ red, Asia red, VIX backwardated), not a tech-led rip.
- **Risk-off tape / flight to safety:** **PARTIAL (relative)**. VIX backwardation + oil shock is a defensive bid **relative to SPY**, but VIX 16.5 is not a >20 flight-to-quality spike, and long-end is rising — so relative, not absolute.
- **Nuclear / grid CapEx / favorable ROE:** structural, no same-session order.
- **Adverse rate case / load-growth disappointment / regulatory smash:** carried (WoodMac/Texas/Ohio). **Duke Energy Florida filed to LOWER customer rates** (News Judge #8) — a **favorable** regulatory item, but single-name and not an ETF driver (08-28 rule). Net neutral.
Net: **Rates rising (bond-proxy selloff)** is the dominant fresh factor; the defensive bid is relative-only. **S1 = −1**.

**3. Breadth.** 1d rel −0.71% (yesterday's fade), but 3d rel +1.18%, 1w rel +0.81%, 1m rel +0.93% — the medium-term relative tape is **positive** (XLU has been a relative winner through the oil shock). No durable breadth expansion today; no live premarket breakdown. Mixed. **S2 = 0**.

**4. Flows.** Prior logs noted modest flows (5d ~+$131M, 1m ~+$22M through early Sept). No confirmed same-day inflow spike or outflow lid. **S3 = 0**.

**5. Catalysts.** Oil/Iran escalation (macro, counted in S0/S1). Duke Energy Florida rate-lower filing (single-name, favorable, not an ETF driver). 10Y auction (supply event, can push long end). No fresh XLU-wide rate-order/load print. **No fresh XLU ETF catalyst.**

### Lessons → scores

**09-09 is the operative veto:** the +1.41% cushion from 09-08 was built on a **static** shock and reverted; a static-shock cushion is **mean-reversion fuel**, not a flat override. Today the oil shock is **not escalating** (Brent +0.85%, WTI +1.44% — holding, not a fresh kinetic increment), so the cushion logic does **not** authorize flat/up. Score S1 negative.

**09-08:** oil at elevated $90+ = inflation/duration unwind, not an FTS bid. Supports down.

**08-18:** risk-off + rising long-end → relative beat / flat-to-negative absolute. Supports down/mild (relative beat possible).

**08-21:** live 10Y 4.80% is **rising**, not easing. No duration relief.

**08-25 does not force flat:** S1 is not 0 (rates-rising is a live HIT) and S4 is not neutral (1d rel −0.71%).

**08-27:** no fresh XLU catalyst → relative lag / flat-to-down absolute. Supports down.

**08-12:** AI-power is a 1d dampener, not a band engine — caps magnitude at mild, does not flip direction.

**08-13:** S2/S4 confirmation only — the positive 3d/1w/1m relative tape is confirmation of relative resilience, not an absolute-up signal.

**09-03/09-04:** no high-impact same-day macro print pending (no 8:30 CPI/PCE/NFP), so the "widen band" rule does not fire; the 10Y auction is a supply event, not a scored binary.

**Self-audit:** lens = bond-proxy duration + oil-shock inflation; band = mild (|score| modest, mag accuracy 0.4); skew = down; same-shock double-count check = oil shock counted once in S1, not again in S0 (S0 is the rates/duration channel); single-ticker check = Duke FL rate filing and any IPP (CEG/VST) do **not** drive the ETF call.

**Divergence:** leading factors (S0 −1, S1 −1) sum negative; tape confirmation (S4) is mixed-to-negative (1d rel −0.71%, but 3d/1w/1m positive). No strong divergence — factors and near-term tape agree on a mild-down absolute with possible relative resilience.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -0.5
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: risk_off
PREDICTED_DIRECTION: down
PREDICTED_MAGNITUDE_BAND: mild
TOTAL_SCORE: -3.15
DIVERGENCE_FLAGGED: False
HORIZON_3D: down/mild
HORIZON_1W: flat/mild
HORIZON_2W: flat/mild
HORIZON_1M: flat/mild
SECTOR_SCORES_END

HIT_GRID_BEGIN
Rates rising (bond-proxy selloff)|HIT|0.75|2026-09-10|https://www.cnbc.com/2026/09/09/treasury-yields.html
Real yields rising|HIT|0.6|2026-09-10|https://fred.stlouisfed.org/series/DFII10
Risk-off tape / flight to safety|PARTIAL|0.5|2026-09-10|https://www.reuters.com/markets/
Data-center load growth / power demand upside|STALE|0.4|2026-09-10|https://www.utilitydive.com/
Rates falling (bond-proxy bid)|MISS|0.8|2026-09-10|https://fred.stlouisfed.org/series/DGS10
Risk-on rotation away from utilities|MISS|0.6|2026-09-10|https://www.morningstar.com/
Favorable rate case / allowed ROE|PARTIAL|0.4|2026-09-10|https://www.duke-energy.com/
Sector rotation into utilities|PARTIAL|0.4|2026-09-10|https://www.reuters.com/markets/
Sector ETF inflow / relative volume spike|NEUTRAL|0.4|2026-09-10|https://www.etfdb.com/etf/XLU/
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -0.5}, 'multiplier': 0.9, 'leading_sum': -5.0, 'divergence_flagged': False, 'total_score': -4.725, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.55, 'regime': 'risk_off'}
```
