# Sector Prediction — Utilities — 2026-09-15

- ETF: **XLU**
- rubric: `00_grounding/sectors/utilities.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-8.152** (mult 0.95)
- regime: risk_off
- divergence_flagged: **False**
- engine: v2 · tape_anchor **-0.598** (ES +0.51%, ZN -0.46%, PM:XLU -0.06%) · index_carry **-1.554** (general -6.215) · llm_overlay **-6.0** (raw -6.294)

## Channel 1 sector ETF tape

```
ETF XLU vs SPY (yfinance, through 2026-09-14):
  1d: XLU -1.34% | SPY -0.45% | rel -0.90%
  3d: XLU -2.61% | SPY -0.20% | rel -2.41%
  1w: XLU -2.92% | SPY -1.21% | rel -1.72%
  1m: XLU -5.04% | SPY -2.19% | rel -2.86%
```

I now have a complete picture. Let me compile the analysis.

**MEMORY_CONFIRM: Utilities/XLU only** — rolling dir=0.412 / mag=0.412 (n=17); last graded 09-14 down/mild vs XLU −1.34% / SPY −0.45% / rel −0.90% (dir HIT, mag MISS — S4 under-scored at −0.5 despite a decisively red multi-horizon tape; overlay shrank the deterministic engine ~30% when it should have shrunk ≤15%). Applied: 09-14 (when 1d/3d/1w/1m rel all negative AND freshest 1d rel ≤ −1%, S4 ≥ −1.0; limit overlay shrink to ~15% when divergence_flagged and tape is unidirectional); 09-11 (a pending high-impact macro binary is ASYMMETRIC-negative for a defensive bond-proxy — test each branch sign; risk-on inputs are headwinds, not cushions); 09-10 (gate the 08-18 relative-beat frame on VIX ≥ ~20 or explicit FTS; VIX 17.56 backwardated 1.123 → rising long end = relative-LAG signal); 09-09 (a cushion built on a STATIC shock is mean-reversion fuel); 09-08 (oil at elevated $90+ = inflation/duration unwind, not an FTS bid); 08-18 (risk-off + rising long end → relative beat / flat-to-negative absolute; do not mint absolute up); 08-21 (live curve, not stale FRED 1d); 08-27 (do not pay carried easing in S0 and S1); 08-25 (S0=S1=0 → do not manufacture down — NOT applicable, S0/S1 negative); 08-12 (AI-power is a 1d dampener); 08-13 (S2/S4 confirmation only); 08-28 (do not promote a single-name regulatory item into S1). Open experiment: require one extra confirming source in the dominant bucket before full weight — used live 10Y/30Y + real yield + oil + VIX backwardation + XLU premarket. Memory index unavailable this run; used injected sector logs + Channel 1 + web search.

---

# Utilities (XLU) — 2026-09-15

Object is the **near-session XLU environment**, not SPX and not a stock pick.

## Channel 1 (trusted, not re-derived)

XLU vs SPY through 2026-09-14: **1d −1.34% / −0.45% (rel −0.90%)**; **3d −2.61% / −0.20% (rel −2.41%)**; **1w −2.92% / −1.21% (rel −1.72%)**; **1m −5.04% / −2.19% (rel −2.86%)**. **Every horizon is negative and the lag is widening** — this is a multi-horizon relative lag, not a one-day artifact. Per the 09-14 lesson, this tape earns S4 = −1.0, not −0.5.

Macro panel: VIX **17.56** (+0.46 1d, +1.84 1w) with **VIX/VIX3M 1.123 — backwardation**; DGS10 **4.96** (as of 09-11; +1 bp 1d, **+19 bp 1w, +28 bp 1m**); DGS30 **5.35** (+10 bp 1w, +11 bp 1m); DFII10 **2.60** (**+5 bp 1d, +18 bp 1w, +18 bp 1m**); HY OAS 2.65 (tight, −5 bp 1d); EPU **395.54** (+189.93 1d — policy-uncertainty spike); **CL=F +2.55% / BZ=F −2.81%** (WTI $103.79, Brent $108.11 — oil **spiking**, not offering); GC=F −0.99%; DXY **+0.16%** (99.36); **ES=F −0.54% / NQ=F −0.62%** premarket (red, NQ marginally leading down); **XLU premarket −0.06%** (essentially flat, the least-bad of the defensives alongside XLP −0.19%, XLV −0.13%); Asia composite **−0.66%**; Europe **−0.44%**; 5-day 10Y–SPX corr **−0.107**.

**Live curve:** 10Y **4.96%** — and per CNBC/FT/Bloomberg (09-15), the 10Y **rose to its highest since 2007**, touching **5%** intraday. 30Y **5.35%**, real 10Y **2.60%**. **Rising across every tenor and every window.** Bond futures are **decisively red** (10Y note −0.46%, 30Y bond −0.93%, Ultra Bond −1.09%) — a violent backup, not a stabilization.

**Calendar:** No 8:30 CPI/PCE/NFP today. **FOMC decision is TOMORROW, September 16** (Warsh's rate decision) — a live, unresolved, high-impact policy binary sitting one session ahead. CME FedWatch hike odds are reported in a wide band (~56% per CNBC/Yahoo, up to ~91% per one monitor) — the direction of the surprise is **not knowable at the snapshot**. Retail sales also due this week.

## Channel 2

**1. Shared macro → this sector.** Three live negatives stack on a bond proxy:
- **Rates:** 10Y 4.96% (highest since 2007, touched 5%), 30Y 5.35%, real 2.60% — all rising on every window, and the long end is the **cause** of the risk-off (oil/supply/term-premium shock), not a growth-driven rise. Per the 08-18 regime qualifier, when the long end is the *cause* of the risk-off, the bond-proxy sector is the **transmission channel, not the haven** — it falls AND underperforms.
- **Oil:** WTI $103.79 / Brent $108.11, **+2.3–2.6% on the day** — a fresh inflation impulse feeding the long end. Per 09-08, at elevated oil ($90+) this is an **inflation/duration negative**, not a flight-to-safety bid.
- **Tape:** ES −0.54% / NQ −0.62%, Asia red, Europe red, VIX backwardated at 17.56. This is a **risk-off tape with a rising long end** — the 08-18 setup. But the **09-10 gate** requires VIX ≥ ~20 or an explicit FTS impulse before granting a relative-beat claim; VIX 17.56 backwardated **fails that gate**, so the rising long end is a **relative-LAG** signal for XLU, not a cushion.
- **The one genuine offset:** XLU premarket **−0.06%** (flat while ES/NQ are red) — a nascent defensive bid. But per 09-11, for a defensive bond-proxy the risk-on/risk-off inputs must be sign-tested: a *risk-off* tape is a relative positive, yet the **rates + oil** channels dominate the absolute, and the 09-09 lesson says a defensive bid built on a **static** shock is mean-reversion fuel. Oil is *escalating* (+2.5%), which is the one condition that could sustain a fresh defensive bid — but it is simultaneously the inflation channel pushing yields higher. Net: **S0 = −1** (rates + oil + backwardated VIX dominate; the premarket flat is a relative, not absolute, tell).

**2. Spine / secondary.**
- **Data-center load growth / power demand upside:** structural HIT, **stale** for a 1d call. MAP HEAT: IPP (VST +10.4 / CEG +9.43 w1) is the real SPLIT, but that is **IPP, not XLU regulated-electric**; regulated electric lags parent (vs_parent −1.01), NEE and SO both red. Rubric: do **not** let the multi-year AI-power story override a 1d rate tape. Note the 09-07 24/7 Wall St. piece "XLU's AI Power Story Crumbles as Texas Freezes Data-Center Demand" — a narrative headwind, not a fresh same-day catalyst.
- **Rates falling (bond-proxy bid):** **MISS**. 10Y 4.96% (highest since 2007), 30Y 5.35%, bond futures decisively red. Not falling.
- **Rates rising (bond-proxy selloff):** **HIT**. 10Y +28 bp 1m, 30Y +11 bp 1m, real +18 bp 1m; oil $108 → inflation → long end up. One shock, counted once.
- **Risk-on rotation away from utilities:** **MISS**. Tape is risk-off (ES/NQ red, Asia red, Europe red), not a tech-led rip.
- **Risk-off tape / flight to safety:** **PARTIAL (relative)**. VIX backwardation + oil shock is a defensive bid **relative to SPY**, but VIX 17.56 is not a >20 flight-to-quality spike, and the long end is rising — so relative, not absolute.
- **Nuclear / grid CapEx / favorable ROE:** structural, no same-session order. DTE gas rate hike (09-15) is single-name, not an ETF driver (08-28 rule).
- **Adverse rate case / load-growth disappointment / regulatory smash:** carried (WoodMac/Texas/Ohio). Not fresh.
Net: **Rates rising (bond-proxy selloff)** is the dominant fresh factor; the defensive bid is relative-only. **S1 = −1.5**.

**3. Breadth.** 1d rel −0.90%, 3d rel −2.41%, 1w rel −1.72%, 1m rel −2.86% — **all negative and widening**. MAP HEAT: diversified is "tape drift, not a story" (SRE +5.55 on ceremony, AES flat); regulated electric lags parent with NEE/SO red; regulated gas is the weakest pocket (breadth 0.0, vs_parent −1.55); water captains do not confirm (AWK red, AWR −1.5, CWT −0.89). Only IPP shows real heat, and IPP is **not** the XLU regulated-electric book. No durable breadth expansion; no live premarket breakdown either (XLU −0.06%). **S2 = −0.5** (breadth failure inside the regulated pockets, partially offset by the flat premarket).

**4. Flows.** Prior logs noted modest flows (5d ~+$131M, 1m ~+$22M through early Sept). Seeking Alpha (09-06): "Utilities flash oversold signal as Treasury yields dim dividend appeal" — a washout setup, but no confirmed same-day inflow spike or outflow lid. **S3 = 0**.

**5. Catalysts.** Oil/Iran escalation (Brent $108) is the live macro driver. **FOMC tomorrow (09-16)** is the dominant unresolved binary — per 09-11, for a defensive bond-proxy this binary is **asymmetric-negative**: a hike → rates up → bond-proxy down; a hold/dovish surprise → risk-on rotation away from defensives. Both branches hurt. No fresh XLU-wide rate-order/load print today. DTE gas rate hike is single-name.

## Lessons → scores

**09-14 is the binding lesson on S4:** the tape is negative on every horizon (1d/3d/1w/1m rel all < 0) and the freshest 1d rel is −0.90% — just under the ≥1% floor, but the multi-horizon unidirectionality plus the widening lag earns **S4 = −1.0**. The overlay must shrink the deterministic engine by **no more than ~15%** when divergence_flagged and the tape is unidirectional.

**09-11 is the binding lesson on S0/S1:** a pending high-impact macro binary (FOMC tomorrow) is **asymmetric-negative** for a defensive bond-proxy — test each branch sign; risk-on inputs (green futures, oil-offering) are headwinds, not cushions. Here futures are red and oil is *spiking*, so the asymmetry is even cleaner: both the rates channel and the rotation channel point down.

**09-10 gate:** VIX 17.56 backwardated (1.123) fails the ≥20 FTS gate → the rising long end is a **relative-LAG** signal, not a cushion. Act on it (raise conviction), don't cap it.

**08-18 regime qualifier:** the long end is the *cause* of the risk-off (oil/supply shock), so XLU is the transmission channel, not the haven — it falls AND underperforms.

**08-25 does NOT apply** — S0 and S1 are both negative, so this is not a "manufacture down from carried lag" case.

**Divergence flag:** the premarket flat (−0.06%) is a relative tell that fights the leading-factor sum. Per method, trust factors over tape — but flag it and keep the band at mild (not notable) given the low-beta instrument and the unresolved FOMC binary one session ahead.

## Verdict

Direction **down**, magnitude **mild**. The rates channel (10Y highest since 2007, real yields +18 bp 1m, bond futures decisively red), the oil/inflation impulse (Brent $108), the multi-horizon relative lag (all four horizons negative and widening), and the asymmetric-negative FOMC binary all point down. The premarket flat is a relative-only tell and the FOMC binary caps conviction — so mild, not notable.

```
SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1.0
S1_SECTOR_FACTORS: -1.5
S2_BREADTH: -0.5
S3_FLOWS_POSITIONING: 0.0
S4_ETF_TAPE: -1.0
MULTIPLIER: 0.95
CONFIDENCE: 0.68
REGIME: risk_off
DIVERGENCE_FLAGGED: True
PREDICTED_DIRECTION: down
PREDICTED_MAGNITUDE_BAND: mild
HORIZON_3D: down
HORIZON_1W: down
HORIZON_2W: flat
HORIZON_1M: flat
SECTOR_SCORES_END
```

```
HIT_GRID_BEGIN
Rates rising (bond-proxy selloff)|HIT|0.85|2026-09-15|https://www.cnbc.com/2026/09/15/10-year-treasury-yield-rises-to-highest-since-2007.html
Real yields rising|HIT|0.80|2026-09-15|https://www.cnbc.com/2026/09/15/10-year-treasury-yield-rises-to-highest-since-2007.html
Risk-off tape / flight to safety|PARTIAL|0.60|2026-09-15|https://www.reuters.com/world/asia-pacific/global-markets-global-markets-2026-09-15/
Rates falling (bond-proxy bid)|MISS|0.85|2026-09-15|https://www.cnbc.com/2026/09/15/10-year-treasury-yield-rises-to-highest-since-2007.html
Risk-on rotation away from utilities|MISS|0.65|2026-09-15|https://www.reuters.com/world/asia-pacific/global-markets-global-markets-2026-09-15/
Sector breadth failure (ETF up, names flat)|HIT|0.55|2026-09-15|https://www.bloomberg.com/news/articles/2026-09-14/asian-stocks-to-fall-on-ai-key-us-yield-tops-5-markets-wrap
Data-center load growth / power demand upside|PARTIAL|0.45|2026-09-15|https://news.google.com/rss/articles/CBMisgFBVV95cUxNZ0JyalpHbDFtQVp4SV9vaWtRNERPMnd2d29RamYzZzhBZmhqV21seHQ2LVJOaHZ1RlRGcm9UVnJ4ZTdBbFZoUDViM1g1OGg0ZlJndi0zV2NsaXdrNUpDdW0xTTBwdEp6UVV1cWFwN2FoUEtUbW10bUlKRHpITEVrcG9tcjY2bjc4aVZ3T2o3dzFBTk9rWlpGQjhZd04zeGE4T1k1b3p6Q2VaX3pNb1g4Z01R
Sector rotation out of utilities|HIT|0.60|2026-09-15|https://seekingalpha.com/news/4640528-utilities-flash-oversold-signal-as-treasury-yields-dim-dividend-appeal
Adverse rate case|PARTIAL|0.40|2026-09-15|https://news.google.com/rss/articles/CBMixwFBVV95cUxPcm56OW8xemlnSVZEY1Q2MjRjU0dUZ1M1NUZCamxsOEVidVdyTXZPOFowdHdBekppc2RDVEpMQXNJb2N6Mm9SZzduQV9ZT2xjMjJxMFBYOVJPV0hCbTJkQVE0TGdlR01RblV4OXpSUXQ3eVk4TTAtUjRWZ1h2VE5KUmkwSlVNR2h0Mm0tbkk4UzluRzVuTG1Gb3hPVm5XUURSTWp3VnhqdmRIUVhZNXhuRHltTTNOUmxscHJpWHprMW5UUWpnRWZv
HIT_GRID_END
```

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.5, 'S2_BREADTH': -0.5, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.95, 'leading_sum': -7.5, 'divergence_flagged': False, 'total_score': -8.152, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.65, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': -0.0996, 'score': -0.598, 'legs': [{'leg': 'ES', 'pct': 0.51, 'w': 0.3}, {'leg': 'ZN', 'pct': -0.46, 'w': 0.75}, {'leg': 'PM:XLU', 'pct': -0.06, 'w': 0.7}]}, 'overlay_score': -6.0, 'overlay_raw': -6.294, 'index_carry': -1.554, 'general_total': -6.215, 'skill_multipliers': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 1.25, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.25}, 'llm_confidence': 0.68, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
