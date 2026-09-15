# Sector Prediction — Utilities — 2026-09-15

- ETF: **XLU**
- rubric: `00_grounding/sectors/utilities.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-6.107** (mult 1.0)
- regime: risk_off
- divergence_flagged: **True**
- engine: v2 · tape_anchor **0.609** (ES +0.80%, ZN -0.46%, PM:XLU +0.19%) · index_carry **-0.716** (general -2.865) · llm_overlay **-6.0** (raw -7.625)

## Channel 1 sector ETF tape

```
ETF XLU vs SPY (yfinance, through 2026-09-14):
  1d: XLU -1.34% | SPY -0.45% | rel -0.90%
  3d: XLU -2.61% | SPY -0.20% | rel -2.41%
  1w: XLU -2.92% | SPY -1.21% | rel -1.72%
  1m: XLU -5.04% | SPY -2.19% | rel -2.86%
```

MEMORY_CONFIRM: Utilities/XLU only — rolling dir=0.412 / mag=0.412 (n=17); last graded 09-14 down/mild vs XLU −1.34% / SPY −0.45% / rel −0.90% (dir HIT, mag MISS — S4 under-scored at −0.5 despite a decisively red multi-horizon tape; overlay shrank the deterministic engine ~30% when it should have shrunk ≤15%). Applied: 09-14 (when 1d/3d/1w/1m rel all negative AND freshest 1d rel ≤ −1%, S4 ≥ −1.0; limit overlay shrink to ~15% when divergence_flagged and tape is unidirectional); 09-11 (a pending high-impact macro binary is ASYMMETRIC-negative for a defensive bond-proxy — test each branch sign; risk-on inputs are headwinds, not cushions); 09-10 (gate the 08-18 relative-beat frame on VIX ≥ ~20 or explicit FTS; VIX 17.05 backwardated 1.093 → rising long end = relative-LAG signal; score a live long-end auction in S1); 09-09 (a cushion built on a STATIC shock is mean-reversion fuel); 09-08 (oil at elevated $90+ = inflation/duration unwind, not an FTS bid); 08-18 (risk-off + rising long end → relative beat / flat-to-negative absolute; do not mint absolute up); 08-21 (live curve, not stale FRED 1d); 08-27 (do not pay carried easing in S0 and S1); 08-25 (S0=S1=0 → do not manufacture down — NOT applicable, S0/S1 negative); 08-12 (AI-power is a 1d dampener); 08-13 (S2/S4 confirmation only); 08-28 (do not promote a single-name regulatory item into S1). Open experiment: require one extra confirming source in the dominant bucket before full weight — used live 10Y/30Y + real yield + oil + VIX backwardation + XLU premarket. Memory index unavailable this run; used injected sector logs + Channel 1 + web search.

---

# Utilities (XLU) — 2026-09-15

Object is the **near-session XLU environment**, not SPX and not a stock pick.

## Channel 1 (trusted, not re-derived)

XLU vs SPY through 2026-09-14: **1d −1.34% / −0.45% (rel −0.90%)**; **3d −2.61% / −0.20% (rel −2.41%)**; **1w −2.92% / −1.21% (rel −1.72%)**; **1m −5.04% / −2.19% (rel −2.86%)**. **Every horizon is negative and the lag is widening** — a multi-horizon relative lag, not a one-day artifact. Per the 09-14 lesson, this tape earns **S4 = −1.0**, not −0.5.

Macro panel: VIX **17.05** (−0.05 1d, +1.33 1w) with **VIX/VIX3M 1.093 — backwardation**; DGS10 **4.96** (as of 09-11; +1 bp 1d, **+19 bp 1w, +28 bp 1m**); DGS30 **5.35** (+10 bp 1w, +11 bp 1m); DFII10 **2.60** (**+5 bp 1d, +18 bp 1w, +18 bp 1m**); HY OAS 2.65 (tight, −5 bp 1d); EPU **395.54** (+189.93 1d — policy-uncertainty spike); **CL=F +1.4% / BZ=F −3.88%** (WTI $103.79, Brent $108.11 — oil **spiking**, not offering); GC=F −0.63%; DXY **+0.12%** (99.36); **ES=F −0.54% / NQ=F −0.62%** premarket (red, NQ marginally leading down); **XLU premarket +0.19%** (green while ES/NQ red — the least-bad of the defensives alongside XLRE +0.03%, XLV +0.07%); Asia composite **−0.66%**; Europe **−0.18%**; 5-day 10Y–SPX corr **−0.107**.

**Live curve:** 10Y **4.96%** — and per CNBC/FT/Bloomberg (09-15), the 10Y **rose to its highest since 2007**, touching **5%** intraday. 30Y **5.35%**, real 10Y **2.60%**. **Rising across every tenor and every window.** Bond futures are **decisively red** (10Y note −0.46%, 30Y bond −0.93%, Ultra Bond −1.09%) — a violent backup, not a stabilization.

**Calendar:** No 8:30 CPI/PCE/NFP today. **FOMC decision is TOMORROW, September 16** (Warsh's rate decision) — a live, unresolved, high-impact policy binary sitting one session ahead. CME FedWatch hike odds are reported in a wide band (~56% per CNBC/Yahoo, up to ~91% per one monitor) — the direction of the surprise is **not knowable at the snapshot**. Retail sales also due this week.

## Channel 2

**1. Shared macro → this sector.** Three live negatives stack on a bond proxy:
- **Rates:** 10Y 4.96% (highest since 2007, touched 5%), 30Y 5.35%, real 2.60% — all rising on every window, and the long end is the **cause** of the risk-off (oil/supply/term-premium shock), not a growth-driven rise. Per the 08-18 regime qualifier, when the long end is the *cause* of the risk-off, the bond-proxy sector is the **transmission channel, not the haven** — it falls AND underperforms.
- **Oil:** WTI $103.79 / Brent $108.11, **+2.3–2.6% on the day** — a fresh inflation impulse feeding the long end. Per 09-08, at elevated oil ($90+) this is an **inflation/duration negative**, not a flight-to-safety bid.
- **Tape:** ES −0.54% / NQ −0.62%, Asia red, Europe red, VIX backwardated at 17.05. This is a **risk-off tape with a rising long end** — the 08-18 setup. But the **09-10 gate** requires VIX ≥ ~20 or an explicit FTS impulse before granting a relative-beat claim; VIX 17.05 backwardated **fails that gate**, so the rising long end is a **relative-LAG** signal for XLU, not a cushion.
- **The one genuine offset:** XLU premarket **+0.19%** (green while ES/NQ are red) — a nascent defensive bid. But per 09-11, for a defensive bond-proxy the risk-on/risk-off inputs must be sign-tested: a *risk-off* tape is a relative positive, yet the **rates + oil** channels dominate the absolute, and the 09-09 lesson says a defensive bid built on a **static** shock is mean-reversion fuel. Oil is *escalating* (+2.4%), which is the one condition that could sustain a fresh defensive bid — but it is simultaneously the inflation channel pushing yields higher. Net: **S0 = −1** (rates + oil + backwardated VIX dominate; the premarket green is a relative, not absolute, tell).

**2. Spine / secondary.**
- **Data-center load growth / power demand upside:** structural HIT, **stale** for a 1d call. No fresh same-day XLU-wide catalyst. Rubric: do **not** let the multi-year AI-power story override a 1d rate tape.
- **Rates falling (bond-proxy bid):** **MISS**. 10Y 4.96% (touched 5%), 30Y 5.35%, bond futures decisively red. Not falling.
- **Rates rising (bond-proxy selloff):** **HIT**. 10Y highest since 2007, +28 bp 1m; oil $104 → inflation → long-end up. One shock, counted once.
- **Risk-on rotation away from utilities:** **MISS**. Tape is risk-off (ES/NQ red, Asia red, Europe red), not a tech-led rip.
- **Risk-off tape / flight to safety:** **PARTIAL (relative)**. VIX backwardation + oil shock is a defensive bid **relative to SPY**, but VIX 17.05 is not a >20 flight-to-quality spike, and the long end is rising — so relative, not absolute.
- **Nuclear / grid CapEx / favorable ROE:** structural, no same-session order.
- **Adverse rate case / load-growth disappointment / regulatory smash:** carried (WoodMac/Texas/Ohio). Not fresh. Single-name must not drive the ETF (08-28 rule).
Net: **Rates rising (bond-proxy selloff)** is the dominant fresh factor; the defensive bid is relative-only. **S1 = −1.5**.

**3. Breadth.** 1d rel −0.90%, 3d rel −2.41%, 1w rel −1.72%, 1m rel −2.86% — **every horizon negative and widening**. MAP HEAT: Regulated Electric **lags** parent (vs_parent −1.01, NEE and SO both red); Regulated Gas breadth 0.0, vs_parent −1.55 (weakest pocket); Water captains do not confirm (AWK red, AWR −1.5, CWT −0.89); Diversified is tape drift, not a story (SRE +5.55 on ceremony, AES flat); the only SPLIT is **IPP** (VST +10.4 / CEG +9.43 w1, breadth 0.444) — but IPP is **nested, not the XLU parent** (08-28 rule: do not let a nested pocket drive the ETF call). The regulated rate-base core — the bulk of XLU — is failing. **S2 = −1**.

**4. Flows.** Prior logs noted modest flows (5d ~+$131M, 1m ~+$22M through early Sept). No confirmed same-day inflow spike or outflow lid. 1m rel −2.86% is de-risked, not a crowded-long unwind that forces a bounce. **S3 = 0**.

**5. Catalysts.** No fresh XLU-wide rate-order/load print. **FOMC tomorrow** is the live unresolved binary — per 09-11, for a defensive bond-proxy a pending high-impact macro binary is **asymmetric-negative** (in-line/hold → risk-on rotation away from defensives; hawkish → rates up → bond-proxy down). Both branches hurt. The 10Y-through-5% headline is the same rates shock already counted in S0/S1 — do not pay it a third time in S4.

### Lessons → scores

**09-14 is the operative lesson:** the tape is negative on every horizon with a clean −0.90% 1d lag → **S4 = −1.0**, and the overlay must not shrink the deterministic engine by ~30% when the tape is unidirectional. **09-11:** the FOMC binary is asymmetric-negative → S0 = −1, not 0; risk-on inputs (green futures) are headwinds, not cushions — but here futures are *red*, so the defensive bid is the only offset and it is relative-only. **09-10 gate:** VIX 17.05 backwardated < 20 → rising long end = relative-LAG, not cushion. **09-09:** the premarket green is a nascent bid on an *escalating* oil shock — the one condition that could sustain it — but it is simultaneously the inflation channel; net it does not flip the absolute. **08-18:** long end is the *cause* of the risk-off → XLU is the transmission channel, not the haven.

**Divergence check:** leading factor sum (S0 −1, S1 −1.5, S2 −1, S3 0) = −3.5 vs tape confirmation S4 = −1.0 — **aligned, no divergence**. The premarket green (+0.19%) is the only counter-signal and it is a relative, not absolute, tell.

---

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1.0
S1_SECTOR_FACTORS: -1.5
S2_BREADTH: -1.0
S3_FLOWS_POSITIONING: 0.0
S4_ETF_TAPE: -1.0
MULTIPLIER: 1.0
CONFIDENCE: 0.68
REGIME: risk_off
SECTOR_SCORES_END

HIT_GRID_BEGIN
Rates rising (bond-proxy selloff)|HIT|0.85|2026-09-15|https://www.cnbc.com/2026/09/15/treasury-yields.html
Real yields rising|HIT|0.80|2026-09-15|https://fred.stlouisfed.org/series/DFII10
Risk-off tape / flight to safety|PARTIAL|0.55|2026-09-15|https://www.reuters.com/markets/
Data-center load growth / power demand upside|STALE|0.50|2026-09-15|https://www.utilitydive.com/
Rates falling (bond-proxy bid)|MISS|0.85|2026-09-15|https://fred.stlouisfed.org/series/DGS10
Risk-on rotation away from utilities|MISS|0.60|2026-09-15|https://www.reuters.com/markets/
Sector breadth failure (ETF up, names flat)|HIT|0.65|2026-09-15|https://finviz.com/map.ashx
Sector rotation out of utilities|HIT|0.65|2026-09-15|https://finviz.com/map.ashx
Sector ETF outflow / volume dry-up|NEUTRAL|0.40|2026-09-15|https://www.etfdb.com/etf/XLU/
Adverse rate case|CARRIED|0.40|2026-09-15|https://www.utilitydive.com/
HIT_GRID_END

HORIZON_3D: down — the 3d rel −2.41% and widening, with the 10Y at a 2007 high and FOMC tomorrow; the rate-base core (NEE/SO red, regulated gas weakest) has no bid. A hawkish FOMC extends the lag; even a hold leaves the long end elevated and rotation away from defensives intact.
HORIZON_1W: down — 1w rel −1.72%, 1m rel −2.86%; the duration shock is the cause of the risk-off, so XLU is the transmission channel, not the haven. Only a genuine flight-to-quality (VIX ≥ 20) or a dovish FOMC surprise flips this.
HORIZON_2W: down-to-flat — two weeks out the AI/data-center load-growth structural bid and any dovish repricing could stabilize XLU, but the multi-decade long-end stress zone caps the upside until the 10Y/30Y roll over.
HORIZON_1M: flat — the 1m rel −2.86% is already a deep de-rating; the structural power-demand narrative plus a de-risked positioning base make a mean-reversion bounce plausible once the rate shock exhausts, but not before the long end peaks.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.5, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 1.0, 'leading_sum': -8.5, 'divergence_flagged': True, 'total_score': -6.107, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.644, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.1015, 'score': 0.609, 'legs': [{'leg': 'ES', 'pct': 0.8, 'w': 0.3}, {'leg': 'ZN', 'pct': -0.46, 'w': 0.75}, {'leg': 'PM:XLU', 'pct': 0.19, 'w': 0.7}]}, 'overlay_score': -6.0, 'overlay_raw': -7.625, 'index_carry': -0.716, 'general_total': -2.865, 'skill_multipliers': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 1.25, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.25}, 'llm_confidence': 0.68, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
