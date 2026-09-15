# Sector Prediction — Utilities — 2026-09-15

- ETF: **XLU**
- rubric: `00_grounding/sectors/utilities.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-6.716** (mult 1.0)
- regime: risk_off
- divergence_flagged: **True**
- engine: v2 · tape_anchor **0.381** (ES +0.30%, ZN -0.46%, PM:XLU +0.20%) · index_carry **-1.097** (general -4.389) · llm_overlay **-6.0** (raw -7.0)

## Channel 1 sector ETF tape

```
ETF XLU vs SPY (yfinance, through 2026-09-15):
  1d: XLU -0.71% | SPY -0.46% | rel -0.24%
  3d: XLU -2.34% | SPY -0.06% | rel -2.28%
  1w: XLU -4.43% | SPY -1.12% | rel -3.31%
  1m: XLU -6.29% | SPY -2.44% | rel -3.84%
```

MEMORY_CONFIRM: Utilities/XLU only — rolling dir=0.412 / mag=0.412 (n=17); last graded 09-14 down/mild vs XLU −1.34% / SPY −0.45% / rel −0.90% (dir HIT, mag MISS — S4 under-scored at −0.5 despite a decisively red multi-horizon tape; overlay shrank the deterministic engine ~30% when it should have shrunk ≤15%). Applied: 09-14 (when 1d/3d/1w/1m rel all negative AND freshest 1d rel ≤ −1%, S4 ≥ −1.0; limit overlay shrink to ~15% when divergence_flagged and tape is unidirectional); 09-11 (a pending high-impact macro binary is ASYMMETRIC-negative for a defensive bond-proxy — test each branch sign; risk-on inputs are headwinds, not cushions); 09-10 (gate the 08-18 relative-beat frame on VIX ≥ ~20 or explicit FTS; VIX 17.75 → rising long end = relative-LAG signal; score a live long-end auction in S1); 09-09 (a cushion built on a STATIC shock is mean-reversion fuel); 09-08 (oil at elevated $90+ = inflation/duration unwind, not an FTS bid); 08-18 (risk-off + rising long end → relative beat / flat-to-negative absolute; do not mint absolute up — and when the long end is the CAUSE of the risk-off, the bond proxy is the transmission channel, not the haven); 08-21 (live curve, not stale FRED 1d); 08-27 (do not pay carried easing in S0 and S1); 08-25 (S0=S1=0 → do not manufacture down — NOT applicable, S0/S1 negative); 08-12 (AI-power is a 1d dampener); 08-13 (S2/S4 confirmation only); 08-28 (do not promote a single-name regulatory item into S1). Open experiment: require one extra confirming source in the dominant bucket before full weight — used live 10Y/30Y + real yield + oil + VIX term structure + XLU premarket + MAP HEAT. Memory index unavailable this run; used injected sector logs + Channel 1 + web search.

---

# Utilities (XLU) — 2026-09-15

Object is the **near-session XLU environment**, not SPX and not a stock pick.

## Channel 1 (trusted, not re-derived)

XLU vs SPY through 2026-09-15: **1d −0.71% / −0.46% (rel −0.24%)**; **3d −2.34% / −0.06% (rel −2.28%)**; **1w −4.43% / −1.12% (rel −3.31%)**; **1m −6.29% / −2.44% (rel −3.84%)**. Every horizon is negative and the lag is **widening** (1w rel −3.31%, 1m rel −3.84%). The freshest 1d rel (−0.24%) is sub-gate, but the multi-horizon tape is decisively red — per the 09-14 lesson this earns **S4 = −1.0**, not −0.5.

Macro panel: VIX **17.75** (+0.65 1d, +2.03 1w) with **VIX/VIX3M 0.901 — CONTANGO** (a regime change from the backwardation of 09-10 through 09-14); DGS10 **4.96** (as of 09-11; +1 bp 1d, **+19 bp 1w, +28 bp 1m**); DGS30 **5.35** (+10 bp 1w, +11 bp 1m); DFII10 **2.60** (**+5 bp 1d, +18 bp 1w, +18 bp 1m**); HY OAS 2.71 (+6 bp 1d — creeping, not blowing out); EPU 215.48 (−202.54 1d); **CL=F +3.24% / BZ=F −2.34%** (WTI $103.79, Brent $108.11 — oil at war-premium levels); GC=F −0.78%; DXY **+0.25%** (99.36); **ES=F −0.54% / NQ=F −0.62% / RTY −0.73% / DJIA −0.71%** premarket (decisively red, broad); **XLU premarket +0.20%** (green while ES/NQ red — the least-bad of the defensives alongside XLV +0.04%, XLRE −0.24%); Asia composite **−1.1%** (Kospi −3.26%, HangSeng −1.0%); Europe **−0.39%**; 5-day 10Y–SPX corr **−0.172**.

**Live curve:** 10Y **4.96%** — and per CNBC/FT/Bloomberg (09-15), the 10Y **breached 5%**, its highest since 2007. 30Y **5.35%**, real 10Y **2.60%**. **Rising across every tenor and every window.** Bond futures are **decisively red** (10Y note −0.46%, 30Y bond −0.93%, Ultra Bond −1.09%) — a violent backup, not a stabilization.

**Calendar:** No 8:30 CPI/PCE/NFP today. **FOMC decision is TOMORROW, September 16** (Warsh) — a live, unresolved, high-impact policy binary one session ahead. CME FedWatch hike odds ~**56%** (CNBC/Yahoo), with Kalshi ~57% / Polymarket ~49% — the direction of the surprise is **not knowable at the snapshot**. Retail sales also due this week.

## Channel 2

**1. Shared macro → this sector.** Three live negatives stack on a bond proxy:
- **Rates:** 10Y 4.96% (breached 5%, highest since 2007), 30Y 5.35%, real 2.60% — rising on every window. Per the **08-18 regime qualifier**, when the long end is the *cause* of the risk-off (supply/term-premium/oil shock), the bond-proxy sector is the **transmission channel, not the haven** — it falls AND underperforms. That is precisely today's configuration.
- **Oil:** WTI $103.79 / Brent $108.11, **+2.3–3.2% on the day** — a fresh inflation impulse feeding the long end. Per 09-08, at elevated oil ($90+) this is an **inflation/duration negative**, not a flight-to-safety bid.
- **Tape:** ES −0.54% / NQ −0.62% / RTY −0.73%, Asia red, Europe red. This is a **risk-off tape with a rising long end** — the 08-18 setup. The **09-10 gate** requires VIX ≥ ~20 or an explicit FTS impulse before granting a relative-beat claim; VIX 17.75 **fails that gate**, so the rising long end is a **relative-LAG** signal for XLU, not a cushion. Note the VIX term structure has flipped to **contango (0.901)** — less acute stress than the backwardated 09-10→09-14 window, which further weakens any FTS claim.
- **The one genuine offset:** XLU premarket **+0.20%** (green while ES/NQ are red) — a nascent defensive bid, identical in character to 09-14's +0.33% which was correctly discounted as relative-only. Per 09-11, for a defensive bond-proxy the risk-on/risk-off inputs must be sign-tested: a *risk-off* tape is a relative positive, yet the **rates + oil** channels dominate the absolute, and per 09-09 a defensive bid built on a **static** shock is mean-reversion fuel. Oil is escalating (+2.4%), the one condition that could sustain a fresh defensive bid — but it is simultaneously the inflation channel pushing yields higher.
- **FOMC tomorrow:** per 09-11, a pending high-impact binary is **asymmetric-negative** for a defensive bond-proxy — a hawkish hold/hike → rates up → bond proxy down; a dovish surprise → risk-on rotation away from defensives. Both branches are negative-to-neutral. Do **not** pre-score the hawkish branch (News Judge rule), but do **not** treat the binary as symmetric-positive either.

Net: **S0 = −1** (rates + oil + real yields rising dominate; the premarket green is a relative, not absolute, tell; the FOMC binary is unresolved so no −2).

**2. Spine / secondary.**
- **Data-center load growth / power demand upside:** structural HIT, **stale** for a 1d call. Search surfaced **"XLU's AI Power Story Crumbles as Texas Freezes Data-Center Demand"** (247wallst, 09-07) and **Reuters "Texas' halt on powering data centers reflects US reckoning over 'ghost' demand"** (09-01) — a **narrative headwind**, not a fresh same-day catalyst. Rubric: do **not** let the multi-year AI-power story override a 1d rate tape; and per the 09-14 lesson, do not let a stale structural positive soften the tape.
- **Rates falling (bond-proxy bid):** **MISS**. 10Y 4.96% (breached 5%), 30Y 5.35%, bond futures decisively red. Not falling.
- **Rates rising (bond-proxy selloff):** **HIT**. 10Y +28 bp 1m, 30Y +11 bp 1m, real +18 bp 1m; 10Y breached 5% today. One shock, counted once.
- **Risk-on rotation away from utilities:** **MISS**. Tape is risk-off (all four futures red, Asia red, Europe red), not a tech-led rip.
- **Risk-off tape / flight to safety:** **PARTIAL (relative only)**. Red futures + oil shock is a defensive bid *relative to SPY*, but VIX 17.75 in **contango** is not a >20 flight-to-quality spike, and the long end is rising — so relative, not absolute. Per 08-18 qualifier, the long end is the cause, so XLU is the transmission channel.
- **Nuclear / gas generation policy support:** structural HIT, stale. No same-session order.
- **Grid CapEx approval / recovery:** structural HIT, stale. Deloitte M&A midyear (09-14) and Morningstar outlook (09-02) are multi-year, not 1d.
- **Adverse rate case / load-growth disappointment / regulatory smash:** carried (Pennsylvania PUC ratemaking/ROE/curtailment hearing "amid growing concern about the balance" — Utility Dive, 4 days ago; FERC ROE precedents). Not fresh today. Single-name must not drive the ETF (08-28 rule).
- **MAP HEAT (nested override beats parent):** Diversified **dir=up conv=low** — "tape drift, not a story" (SRE +5.55 w1 on ceremony, AES flat). Regulated Electric **flat conv=low** — lags parent (vs_parent −1.01), NEE and SO both red. Regulated Gas **flat conv=low** — weakest pocket (breadth 0.0, vs_parent −1.55). Regulated Water **flat conv=low** — captains red (AWK red, AWR −1.5, CWT −0.89). IPP is the only real heat (VST +10.4 / CEG +9.43 w1, breadth 0.444) but is a **SPLIT** — power, not rate-base — and must **not** be averaged into the parent ETF. Net: the regulated core of XLU has **no bid**; the only heat is in a nested pocket that is not the ETF.

Net: **Rates rising (bond-proxy selloff)** is the dominant fresh factor; the defensive bid is relative-only; the AI-power narrative is a headwind, not a support. **S1 = −1.5**.

**3. Breadth.** MAP HEAT shows **breadth failure inside the sector**: Regulated Electric (NEE, SO red), Regulated Gas (breadth 0.0), Regulated Water (captains red) — the three largest XLU pockets are all flat-to-red with no confirming captains. Only the IPP split has heat, and it is not the ETF. XLU premarket +0.20% is a thin, ETF-level green with no constituent confirmation. **S2 = −0.5**.

**4. Flows.** No confirmed same-day inflow spike. The 1w rel −3.31% / 1m rel −3.84% is a persistent multi-week relative lag — distribution, not accumulation. Prior logs noted modest flows (5d ~+$131M, 1m ~+$22M through early Sept) that have not reversed the lag. No washout-inflow evidence. **S3 = −0.5**.

**5. Catalysts.** No fresh XLU-wide rate-order/load print today. FOMC tomorrow is the dominant unresolved binary (do not pre-score). Oil/Iran/Hormuz is the live macro driver. Texas data-center demand freeze is a **narrative headwind** to the AI-power thesis. Pennsylvania PUC ratemaking/ROE/curtailment is a live regulatory theme but not a same-session ETF driver.

## Lessons → scores

**09-14 is the operative lesson:** all four horizons negative + widening lag → **S4 = −1.0**, and limit overlay shrink to ~15% when divergence_flagged with a unidirectional tape. The deterministic engine's negative lean is closer to right than a shrunken overlay.

**09-10 gate fails:** VIX 17.75 < 20, and now in **contango** — the rising long end is a **relative-LAG** signal, not a cushion. No relative-beat clause.

**08-18 qualifier fires:** the long end is the *cause* of the risk-off (oil/supply/term-premium), so XLU is the transmission channel — falls AND underperforms.

**09-11 asymmetry:** the pending FOMC binary is negative-to-neutral on both branches for a defensive bond-proxy. Do not call flat into it; do not treat the premarket green as a cushion.

**08-25 does not apply** (S0/S1 are negative, not zero). **08-12** caps AI-power as a dampener only. **08-13** keeps S2/S4 as confirmation. **08-28** keeps single-name regulatory items out of S1.

**Divergence:** the leading factor sum (S0 −1, S1 −1.5, S2 −0.5, S3 −0.5 = −3.5) fights the tape confirmation (S4 −1.0 is confirming, not fighting) — but the **XLU premarket +0.20% green vs red futures** is the one input that fights the down call. Per the 09-14 lesson, that premarket green is a **relative-only** tell already discounted in S0/S1 and must **not** be used a second time to cap magnitude. Flag divergence = True; shrink overlay ≤15%.

**Band:** |score| ≈ −4.5 with mult 1.0 → **down / mild**, at the upper edge of mild. Not notable: the 1d rel is sub-gate (−0.24%), VIX is in contango (not a stress spike), and the FOMC binary tomorrow argues against a trend-day conviction. But the multi-horizon lag is severe enough that a notable print is plausible — hence confidence 0.68, not higher.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1.0
S1_SECTOR_FACTORS: -1.5
S2_BREADTH: -0.5
S3_FLOWS_POSITIONING: -0.5
S4_ETF_TAPE: -1.0
MULTIPLIER: 1.0
CONFIDENCE: 0.68
REGIME: risk_off
DIVERGENCE_FLAGGED: True
PREDICTED_DIRECTION: down
PREDICTED_MAGNITUDE_BAND: mild
RELATIVE_CLAUSE: relative lag vs SPY expected (VIX <20 and in contango fails the 09-10 gate; long end is the cause of the risk-off → XLU is the transmission channel, not the haven)
SECTOR_SCORES_END

HIT_GRID_BEGIN
Rates rising (bond-proxy selloff)|HIT|0.90|2026-09-15|https://www.cmegroup.com/markets/interest-rates/cme-fedwatch-tool.html
Real yields rising|HIT|0.85|2026-09-15|https://stockanalysis.com/etf/xlu/
Rates falling (bond-proxy bid)|MISS|0.90|2026-09-15|https://stockanalysis.com/etf/xlu/
Risk-off tape / flight to safety|PARTIAL|0.60|2026-09-15|https://news.google.com/rss/articles/CBMirwFBVV95cUxNcXlwLTZ3U29UZXBTS3BWSWh6R1BGQWxWcDdGZWR3cFpMUEUzeUIzcEsySHFITFR4emFONjgzLWtSNGNJMTUxcW1rSGFfZ1VBZDVDa21aUVY0WlFoRUlDZFlKakI1WTREdGszUkRCNjlrSUxEcVlab01PZTZoTUY0WnUtbEFOUk95b3VCWlRDOERON0EyLWhORk9pS296RThEWFpHOW5USnBZRl91bE5V
Risk-on rotation away from utilities|MISS|0.70|2026-09-15|https://stockanalysis.com/etf/xlu/
Sector breadth failure (ETF up, names flat)|HIT|0.65|2026-09-15|https://news.google.com/rss/articles/CBMisgFBVV95cUxNZ0JyalpHbDFtQVp4SV9vaWtRNERPMnd2d29RamYzZzhBZmhqV21seHQ2LVJOaHZ1RlRGcm9UVnJ4ZTdBbFZoUDViM1g1OGg0ZlJndi0zV2NsaXdrNUpDdW0xTTBwdEp6UVV1cWFwN2FoUEtUbW10bUlKRHpITEVrcG9tcjY2bjc4aVZ3T2o3dzFBTk9rWlpGQjhZd04zeGE4T1k1b3p6Q2VaX3pNb1g4Z01R
Sector rotation out of utilities|HIT|0.70|2026-09-15|https://stockanalysis.com/etf/xlu/
Sector ETF outflow / volume dry-up|PARTIAL|0.50|2026-09-15|https://stockanalysis.com/etf/xlu/
Data-center load growth / power demand upside|MISS|0.60|2026-09-15|https://news.google.com/rss/articles/CBMisgFBVV95cUxNZ0JyalpHbDFtQVp4SV9vaWtRNERPMnd2d29RamYzZzhBZmhqV21seHQ2LVJOaHZ1RlRGcm9UVnJ4ZTdBbFZoUDViM1g1OGg0ZlJndi0zV2NsaXdrNUpDdW0xTTBwdEp6UVV1cWFwN2FoUEtUbW10bUlKRHpITEVrcG9tcjY2bjc4aVZ3T2o3dzFBTk9rWlpGQjhZd04zeGE4T1k1b3p6Q2VaX3pNb1g4Z01R
Adverse rate case|PARTIAL|0.45|2026-09-15|https://www.utilitydive.com/news/pennsylvania-puc-to-consider-rates-and-curtailment-as-data-centers-grow/830174/
Nuclear / gas generation policy support|NEUTRAL|0.40|2026-09-15|https://news.google.com/rss/articles/CBMingFBVV95cUxNQ21ieEpvTjFPYUxuZzZiTFpWSlRONHBFVlJwdDMzRVdXS2xGREUyd1ZqdGhaNjRnMV9WaGd4T1VJUXhuTzdMYXNubjNVNm9DdWE2VElmN2dVRWxXT1ZMMzNqalpUYnZaM2haOU1XUXRXOE9OUjZPLXRzOFBSSTVMQ3JZd3ZiQkdLekJPZnctc1FqNFdWZmhIbGk1M29ZQQ
Grid CapEx approval / recovery|NEUTRAL|0.40|2026-09-15|https://news.google.com/rss/articles/CBMizwFBVV95cUxQSFBXRHVMOVpjU1NoT0oxRVhlaTZyRFhlUTBZbGZhcDNXcE1pNFFPUThPRXpSQ3V4ZUIxSVcwU3ZTZVBtYkY1NFVvY0VQR0dSeDNiTlpQTkZjcXk5TzFmU1FRcXJiN3k0M2hTNDRZUk5NaWVSTm1OX2RDSDBOUTdEYTUtY3B2emdQaWdrODdFQXRNakVQVXpWcVJGTVl0bGpTTld4T3BuT0F1RXpLZE1RcUdTTWVuaGNhOU1yMHFYR0dVbVVVVUhPNDdfa3RCOXM
Crowded long (extreme relative performance + valuation)|MISS|0.70|2026-09-15|https://stockanalysis.com/etf/xlu/
Low-beta leadership inside sector|HIT|0.55|2026-09-15|https://stockanalysis.com/etf/xlu/
HORIZON_3D|down|mild|0.62|2026-09-15
HORIZON_1W|down|mild|0.60|2026-09-15
HORIZON_2W|down|mild|0.55|2026-09-15
HORIZON_1M|down|notable|0.52|2026-09-15
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.5, 'S2_BREADTH': -0.5, 'S3_FLOWS_POSITIONING': -0.5, 'S4_ETF_TAPE': -1.0}, 'multiplier': 1.0, 'leading_sum': -7.5, 'divergence_flagged': True, 'total_score': -6.716, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.65, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.0635, 'score': 0.381, 'legs': [{'leg': 'ES', 'pct': 0.3, 'w': 0.3}, {'leg': 'ZN', 'pct': -0.46, 'w': 0.75}, {'leg': 'PM:XLU', 'pct': 0.2, 'w': 0.7}]}, 'overlay_score': -6.0, 'overlay_raw': -7.0, 'index_carry': -1.097, 'general_total': -4.389, 'skill_multipliers': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 1.25, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.25}, 'llm_confidence': 0.68, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
