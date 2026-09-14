# Sector Prediction — Utilities — 2026-09-14

- ETF: **XLU**
- rubric: `00_grounding/sectors/utilities.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-7.559** (mult 0.9)
- regime: risk_off
- divergence_flagged: **True**
- engine: v2 · tape_anchor **1.192** (ES -0.66%, ZN +0.12%, PM:XLU +0.33%) · index_carry **-2.751** (general -11.002) · llm_overlay **-6.0** (raw -7.65)

## Channel 1 sector ETF tape

```
ETF XLU vs SPY (yfinance, through 2026-09-11):
  1d: XLU -0.31% | SPY +0.85% | rel -1.16%
  3d: XLU -2.44% | SPY -0.22% | rel -2.22%
  1w: XLU -1.49% | SPY -1.15% | rel -0.34%
  1m: XLU -3.31% | SPY -1.06% | rel -2.25%
```

MEMORY_CONFIRM: Utilities/XLU only — rolling dir=0.412 / mag=0.412 (n=17); last graded 09-11 down/flat vs XLU −0.31% / SPY +0.85% / rel −1.16% (dir HIT, mag HIT by band letter, but the −1.16% relative lag was unflagged — 09-11 lesson: a pending high-impact macro binary is ASYMMETRIC-negative for a defensive bond-proxy; both branches hurt; risk-on inputs are headwinds, not cushions). Applied: 09-11 (test each branch sign; S0=−1 not 0; act on the 09-10 gate, don't cap it); 09-10 (gate the 08-18 relative-beat frame on VIX ≥ ~20 or explicit FTS — VIX 17.67 backwardated 1.135 → rising long end = relative-LAG signal; score a live long-end auction in S1); 09-09 (a 1d cushion built on a STATIC shock is mean-reversion fuel, not a flat override); 09-08 (oil at elevated $90+ = inflation/duration unwind, not an FTS bid); 08-18 (risk-off + rising long end → relative beat / flat-to-negative absolute; do not mint absolute up); 08-21 (live curve, not stale FRED 1d); 08-27 (do not pay carried easing in S0 and S1; no fresh XLU catalyst → relative lag); 08-25 (S0=S1=0 → do not manufacture down from carried lag — NOT applicable today, S0/S1 are negative); 08-12 (AI-power is a 1d dampener, not a band engine); 08-13 (S2/S4 confirmation only); 08-28 (do not promote a single-name regulatory item into S1). Open experiment: require one extra confirming source in the dominant bucket before full weight — used live 10Y/30Y + real yield + oil + VIX backwardation + XLU premarket. Memory index unavailable this run; used injected sector logs + Channel 1 + web search.

## Utilities (XLU) — 2026-09-14

Object is the **near-session XLU environment**, not SPX and not a stock pick.

### Channel 1 (trusted, not re-derived)

XLU vs SPY through 2026-09-11: **1d −0.31% / +0.85% (rel −1.16%)**; **3d −2.44% / −0.22% (rel −2.22%)**; **1w −1.49% / −1.15% (rel −0.34%)**; **1m −3.31% / −1.06% (rel −2.25%)**. Every horizon is negative — the 09-11 "relative lag" is now a **multi-horizon lag**, not a one-day artifact.

Macro panel: VIX **17.67** (+1.83 1d, +2.37 1w) with **VIX/VIX3M 1.135 — backwardation**; DGS10 **4.95** (as of 09-10; **+12 bp 1d, +16 bp 1w, +25 bp 1m**); DGS30 **5.37** (+9 bp 1d, +10 bp 1w, +13 bp 1m); DFII10 **2.55** (**+9 bp 1d, +10 bp 1w, +12 bp 1m**); HY OAS 2.70 (tight, −1 bp 1d); EPU **725.88** (+451 1d — a massive policy-uncertainty spike); RRP +$4.55B 1w; **CL=F +3.05% / BZ=F +3.43%** (WTI $102.29, Brent $107.33 — oil **spiking**, not offering); GC=F −0.33% / Silver −2.17% / Copper −1.44%; DXY **+0.45%** (99.245); **ES=F −0.66% / NQ=F −1.59%** premarket (decisively red, NQ leading down); **XLU premarket +0.33%** (the only green defensive alongside XLP +0.61%, XLRE +0.48%); Asia composite **−0.72%** (Kospi −3.26%, Nikkei −0.81%); Europe **−0.35%**; 5-day 10Y–SPX corr **−0.248**.

**Live curve:** 10Y **4.95%** (new high zone, +25 bp 1m), 30Y **5.37%**, real 10Y **2.55%** (+12 bp 1m). **Rising across every tenor and every window.** Bond futures are marginally green (10Y note +0.12%) — a tiny stabilization inside a violent backup, not a duration-relief impulse.

**Calendar:** No 8:30 CPI/PCE/NFP today (News Judge: "set is thin on hard macro data"). The dominant driver is the **carried Warsh hawkish repricing** (Sept hike odds up, gold −3%) plus a **fresh oil spike** (Brent $107). **FOMC is next week** — a live, unresolved, two-sided policy binary sitting just ahead.

### Channel 2

**1. Shared macro → this sector.** Two live negatives stack on a bond proxy:
- **Rates:** 10Y 4.95% / 30Y 5.37% / real 2.55%, all rising on every window; Warsh's Jackson Hole hawkishness lifted September hike odds (News Judge #1, confidence 0.85). This is a **direct duration headwind** — the classical bond-proxy selloff channel.
- **Oil:** WTI $102.29 / Brent $107.33, **+3% on the day** — a fresh inflation impulse feeding the long end. Per 09-08, at elevated oil ($90+) this is an **inflation/duration negative**, not a flight-to-safety bid.
- **Tape:** ES −0.66% / NQ −1.59%, Asia red, Europe red, VIX backwardated at 17.67. This is a **risk-off tape with a rising long end** — the 08-18 setup. But the **09-10 gate** requires VIX ≥ ~20 or an explicit FTS impulse before granting a relative-beat claim; VIX 17.67 backwardated **fails that gate**, so the rising long end is a **relative-LAG** signal for XLU, not a cushion.
- **The one genuine offset:** XLU premarket **+0.33%** (green while ES/NQ are red) — a nascent defensive bid. But per 09-11, for a defensive bond-proxy the risk-on/risk-off inputs must be sign-tested: a *risk-off* tape is a relative positive, yet the **rates + oil** channels dominate the absolute, and the 09-09 lesson says a defensive bid built on a **static** shock is mean-reversion fuel. Oil is *escalating* (+3%), which is the one condition that could sustain a fresh defensive bid — but it is simultaneously the inflation channel that pushes yields higher. Net: **S0 = −1** (rates + oil + backwardated VIX dominate; the premarket green is a relative, not absolute, tell).

**2. Spine / secondary.**
- **Data-center load growth / power demand upside:** structural HIT, **stale** for a 1d call. No fresh same-day XLU-wide catalyst. Rubric: do **not** let the multi-year AI-power story override a 1d rate tape.
- **Rates falling (bond-proxy bid):** **MISS**. 10Y 4.95% / 30Y 5.37% / real 2.55%, all rising.
- **Rates rising (bond-proxy selloff):** **HIT**. One shock (Warsh hawkish + oil spike), counted once.
- **Risk-on rotation away from utilities:** **MISS**. Tape is risk-off (NQ −1.59%, Kospi −3.26%), not a tech-led rip.
- **Risk-off tape / flight to safety:** **PARTIAL (relative)**. VIX backwardated + red futures is a defensive bid *relative to SPY* — consistent with XLU premarket +0.33% — but VIX 17.67 < 20 fails the 09-10 deep-risk-off gate, so it is relative-only, not an absolute-up license.
- **Nuclear / gas generation policy support:** structural HIT, stale.
- **Grid CapEx approval / recovery:** structural HIT, stale.
- **Adverse rate case / load-growth disappointment / regulatory smash:** carried (WoodMac/Texas/Ohio). Not fresh. Single-name must not drive the ETF (08-28 rule).
Net: **Rates rising (bond-proxy selloff)** is the dominant fresh factor; the defensive bid is relative-only. **S1 = −1.5** (rates HIT at full weight + oil-inflation transmission; the relative defensive bid is a partial offset, not a positive).

**3. Breadth.** 1d rel −1.16%, 3d rel −2.22%, 1w rel −0.34%, 1m rel −2.25% — **all negative**, a multi-horizon lag. No durable breadth expansion. Premarket XLU +0.33% is a single-name-level green, not a broad constituent expansion. **S2 = −1**.

**4. Flows.** No confirmed same-day inflow spike or outflow lid in Channel 1. 1m rel −2.25% is de-risked, not crowded-long. **S3 = 0**.

**5. Catalysts.** Warsh hawkish is **carried** (delivered 8/28, still the dominant rates object). Oil spike is **fresh** (+3%, Brent $107). **FOMC next week** is a live unresolved binary — per 09-11, a pending high-impact policy event is **asymmetric-negative** for a defensive bond-proxy (hawkish → rates up → XLU down; dovish → risk-on rotation away from defensives). No fresh XLU-wide rate-order/load print. **No fresh XLU catalyst.**

### Lessons → scores

**09-11 is the operative lesson:** a pending high-impact macro binary is **asymmetric-negative** for a defensive bond-proxy — both branches hurt. FOMC next week + a live hawkish repricing means the binary is not symmetric. Score S0 = −1 (not 0), weight the **live 1d/1m relative tape** over stale positives in S2, and emit down with a relative-lag clause.

**09-10 gate fires:** VIX 17.67 < 20 and backwardated (1.135) → a rising long end is a **relative-LAG** signal for XLU, not a cushion. Do not write a relative-beat clause.

**09-09:** the defensive bid is built on an **escalating** oil shock (+3%), which is the one condition that could sustain it — but that same escalation is the inflation channel pushing yields higher, so it does not flip the absolute call.

**08-18:** risk-off + rising long end → relative beat / flat-to-negative absolute; do not mint absolute up. Supports down.

**08-21:** live 10Y 4.95% is **rising**, not easing. No easing impulse.

**08-25 does not force flat:** S0 and S1 are both negative (not 0), and S4 is decisively red across 1d/3d/1w/1m.

**08-12:** AI-power is a 1d dampener, not a band engine — do not let it inflate the band.

### Magnitude

|score| is modest (Σ ≈ −3.5 × 0.9 ≈ −3.2). Rolling mag accuracy is 0.412 — shrink the band on modest |score|. The 09-11 sibling lesson says raise to mild when the asymmetry is clear; the 09-10 lesson says cap at mild when VIX <20 and the tape is not confirming a notable move. XLU premarket +0.33% is a **relative** green that argues against a notable absolute down. **Band = mild.**

### Direction

Down. Rates rising (10Y 4.95% / 30Y 5.37% / real 2.55%, all windows), oil spiking (+3%, Brent $107), VIX backwardated, multi-horizon relative lag, and an asymmetric-negative FOMC binary next week. The premarket green is a relative defensive bid, not an absolute-up license (09-10 gate fails). **Down/mild with a relative-lag clause.**

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -1.5
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -0.5
MULTIPLIER: 0.9
CONFIDENCE: 0.62
REGIME: risk_off
SECTOR_SCORES_END

HIT_GRID_BEGIN
Rates rising (bond-proxy selloff)|HIT|0.85|2026-09-14|https://www.federalreserve.gov/
Real yields rising|HIT|0.80|2026-09-14|https://fred.stlouisfed.org/series/DFII10
Risk-off tape / flight to safety|PARTIAL|0.55|2026-09-14|https://www.cnbc.com/
Data-center load growth / power demand upside|STALE|0.40|2026-09-14|https://www.utilitydive.com/
Rates falling (bond-proxy bid)|MISS|0.85|2026-09-14|https://fred.stlouisfed.org/series/DGS10
Risk-on rotation away from utilities|MISS|0.60|2026-09-14|https://www.reuters.com/
Sector breadth failure (ETF up, names flat)|HIT|0.55|2026-09-14|https://finance.yahoo.com/quote/XLU
Sector rotation out of utilities|HIT|0.55|2026-09-14|https://finance.yahoo.com/quote/XLU
Nuclear / gas generation policy support|STALE|0.35|2026-09-14|https://www.eia.gov/
Grid CapEx approval / recovery|STALE|0.35|2026-09-14|https://www.utilitydive.com/
Adverse rate case|STALE|0.30|2026-09-14|https://www.utilitydive.com/
Load growth disappointment|STALE|0.30|2026-09-14|https://www.utilitydive.com/
Sector ETF inflow / relative volume spike|NEUTRAL|0.30|2026-09-14|https://www.etfdb.com/etf/XLU/
Crowded long (extreme relative performance + valuation)|MISS|0.55|2026-09-14|https://finance.yahoo.com/quote/XLU
HIT_GRID_END

HORIZON_3D: down/mild — rates backup (10Y 4.95%, +25 bp 1m) and oil spike persist; XLU relative lag continues unless FOMC resolves dovish.
HORIZON_1W: down/mild — FOMC next week is the swing; a dovish resolution would rotate away from defensives (still negative), a hawkish one compounds the duration headwind.
HORIZON_2W: flat/down — the multi-decade long-end stress zone (30Y 5.37%) caps bond-proxy upside; AI-power structural bid is a slow offset, not a 2w driver.
HORIZON_1M: flat — 1m rel −2.25% is de-risked; a rate-peak would let the structural load-growth narrative reassert, but not before the FOMC clears.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.5, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -0.5}, 'multiplier': 0.9, 'leading_sum': -8.5, 'divergence_flagged': True, 'total_score': -7.559, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.702, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.1986, 'score': 1.192, 'legs': [{'leg': 'ES', 'pct': -0.66, 'w': 0.3}, {'leg': 'ZN', 'pct': 0.12, 'w': 0.75}, {'leg': 'PM:XLU', 'pct': 0.33, 'w': 0.7}]}, 'overlay_score': -6.0, 'overlay_raw': -7.65, 'index_carry': -2.751, 'general_total': -11.002, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.62}
```
