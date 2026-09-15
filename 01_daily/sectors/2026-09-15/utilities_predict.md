# Sector Prediction — Utilities — 2026-09-15

- ETF: **XLU**
- rubric: `00_grounding/sectors/utilities.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-6.556** (mult 1.0)
- regime: risk_off
- divergence_flagged: **True**
- engine: v2 · tape_anchor **0.403** (ES +0.34%, ZN -0.46%, PM:XLU +0.20%) · index_carry **-0.959** (general -3.836) · llm_overlay **-6.0** (raw -9.875)

## Channel 1 sector ETF tape

```
ETF XLU vs SPY (yfinance, through 2026-09-15):
  1d: XLU -1.18% | SPY -0.45% | rel -0.73%
  3d: XLU -2.81% | SPY -0.05% | rel -2.76%
  1w: XLU -4.89% | SPY -1.11% | rel -3.78%
  1m: XLU -6.74% | SPY -2.43% | rel -4.30%
```

I have enough to score. Compiling now.

**MEMORY_CONFIRM: Utilities/XLU only** — rolling dir=0.412 / mag=0.412 (n=17); last graded 09-14 down/mild vs XLU −1.34% / SPY −0.45% / rel −0.90% (dir HIT, mag MISS — S4 under-scored at −0.5 despite a decisively red multi-horizon tape; overlay shrank the deterministic engine ~30% when it should have shrunk ≤15%). Applied: 09-14 (when 1d/3d/1w/1m rel all negative AND freshest 1d rel ≤ −1%, S4 ≥ −1.0; limit overlay shrink to ~15% when divergence_flagged and tape is unidirectional); 09-11 (a pending high-impact macro binary is ASYMMETRIC-negative for a defensive bond-proxy — test each branch sign; risk-on inputs are headwinds, not cushions); 09-10 (gate the 08-18 relative-beat frame on VIX ≥ ~20 or explicit FTS; VIX 17.49 → rising long end = relative-LAG signal; score a live long-end auction in S1); 09-09 (a cushion built on a STATIC shock is mean-reversion fuel); 09-08 (oil at elevated $90+ = inflation/duration unwind, not an FTS bid); 08-18 (risk-off + rising long end → relative beat / flat-to-negative absolute; when the long end is the CAUSE of the risk-off, the bond proxy is the transmission channel, not the haven); 08-21 (live curve, not stale FRED 1d); 08-27 (do not pay carried easing in S0 and S1); 08-25 (S0=S1=0 → do not manufacture down — NOT applicable, S0/S1 negative); 08-12 (AI-power is a 1d dampener); 08-13 (S2/S4 confirmation only); 08-28 (do not promote a single-name regulatory item into S1). Open experiment: require one extra confirming source in the dominant bucket before full weight — used live 10Y/30Y + real yield + oil + VIX term structure + XLU premarket + MAP HEAT. Memory index unavailable this run; used injected sector logs + Channel 1 + web search.

---

# Utilities (XLU) — 2026-09-15

Object is the **near-session XLU environment**, not SPX and not a stock pick.

## Channel 1 (trusted, not re-derived)

XLU vs SPY through 2026-09-15: **1d −1.18% / −0.45% (rel −0.73%)**; **3d −2.81% / −0.05% (rel −2.76%)**; **1w −4.89% / −1.11% (rel −3.78%)**; **1m −6.74% / −2.43% (rel −4.30%)**. Every horizon is negative and the lag is **widening** (1w rel −3.78%, 1m rel −4.30%). The freshest 1d rel (−0.73%) is sub-gate for the 09-14 ≥1% floor, but the multi-horizon tape is decisively red and unidirectional — per the 09-14 lesson this earns **S4 = −1.0**, not −0.5.

Macro panel: VIX **17.49** (+0.39 1d, +1.77 1w) with **VIX/VIX3M 0.897 — CONTANGO** (a regime change from the backwardation of 09-10→09-14); DGS10 **4.96** (as of 09-11; +1 bp 1d, **+19 bp 1w, +28 bp 1m**) — and per Channel 2, the 10Y **breached 5.00–5.02%** on 09-15, highest since 2007; DGS30 **5.35** (+10 bp 1w, +11 bp 1m); DFII10 **2.60** (**+5 bp 1d, +18 bp 1w, +18 bp 1m**); HY OAS 2.71 (+6 bp 1d — creeping, not blowing out); EPU 215.48 (−202.54 1d); **CL=F +4.27% / BZ=F +2.70%** (WTI $103.79, Brent $108.11 — war-premium levels); GC=F −0.30%; DXY **+0.17%** (99.36); **ES=F −0.54% / NQ=F −0.62% / RTY −0.73% / DJIA −0.71%** premarket (decisively red, broad); **XLU premarket +0.20%** (green while ES/NQ red — the least-bad of the defensives alongside XLV +0.04%, XLRE −0.24%); Asia composite **−0.72%** (Kospi −3.26%, Nikkei −0.81%); Europe **−0.31%**; 5-day 10Y–SPX corr **−0.151**.

**Live curve:** 10Y **4.96% → breached 5.00–5.02% intraday** (highest since 2007 per Euronews/Morningstar/Vantage), 30Y **5.35%**, real 10Y **2.60%**. **Rising across every tenor and every window.** Bond futures are **decisively red** (10Y note −0.46%, 30Y bond −0.93%, Ultra Bond −1.09%) — a violent backup, not a stabilization.

**Calendar:** No 8:30 CPI/PCE/NFP today. **FOMC decision is TOMORROW, September 16** (Warsh) — a live, unresolved, high-impact policy binary one session ahead. CME FedWatch hike odds ~**56%** (CNBC/Yahoo), Kalshi ~57% / Polymarket ~49% — the direction of the surprise is **not knowable at the snapshot**.

## Channel 2

**1. Shared macro → this sector.** Three live negatives stack on a bond proxy:
- **Rates:** 10Y 4.96% (breached 5%, highest since 2007), 30Y 5.35%, real 2.60% — rising on every window. Per the **08-18 regime qualifier**, when the long end is the *cause* of the risk-off (supply/term-premium/oil shock), the bond-proxy sector is the **transmission channel, not the haven** — it falls AND underperforms. That is precisely today's configuration.
- **Oil:** WTI $103.79 / Brent $108.11, **+2.3–4.3% on the day** — a fresh inflation impulse feeding the long end (News Judge #4: US–Iran tanker war, Hormuz traffic impaired). Per 09-08, at elevated oil ($90+) this is an **inflation/duration negative**, not a flight-to-safety bid.
- **Tape:** ES −0.54% / NQ −0.62% / RTY −0.73%, Asia red, Europe red. This is a **risk-off tape with a rising long end** — the 08-18 setup. The **09-10 gate** requires VIX ≥ ~20 or an explicit FTS impulse before granting a relative-beat claim; VIX 17.49 **fails that gate**, so the rising long end is a **relative-LAG** signal for XLU, not a cushion. The VIX term structure has flipped to **contango (0.897)** — less acute stress than the backwardated 09-10→09-14 window, which further weakens any FTS claim.
- **The one genuine offset:** XLU premarket **+0.20%** (green while ES/NQ are red) — a nascent defensive bid, identical in character to 09-14's +0.33% which was correctly discounted as relative-only. Per 09-11, for a defensive bond-proxy the risk-on/risk-off inputs must be sign-tested: a *risk-off* tape is a relative positive, yet the **rates + oil** channels dominate the absolute, and the 09-09 lesson says a defensive bid built on a **static** shock is mean-reversion fuel. Oil is *escalating* (+4.3%), which is the one condition that could sustain a fresh defensive bid — but it is simultaneously the inflation channel pushing yields higher. Net: **S0 = −1** (rates + oil + sub-gate VIX dominate; the premarket green is a relative, not absolute, tell).

**2. Spine / secondary.**
- **Data-center load growth / power demand upside:** structural HIT, **stale** for a 1d call. Search confirms only evergreen coverage (simplywall.st 09-15 "3 AI Infrastructure Stocks for Data Center And Power Demand"; 24/7 Wall St 09-11; Morningstar 09-02 "US Utilities Outlook: AI Power Demand Meets Grid Investment"). No fresh same-day XLU-wide catalyst. Rubric: do **not** let the multi-year AI-power story override a 1d rate tape.
- **Rates falling (bond-proxy bid):** **MISS**. 10Y 4.96%→5.00%+, 30Y 5.35%, real 2.60%, bond futures decisively red. Not falling.
- **Rates rising (bond-proxy selloff):** **HIT**. 10Y +28 bp 1m / +19 bp 1w, 30Y +11 bp 1m, real +18 bp 1m; 10Y breached 5% (highest since 2007). One shock, counted once.
- **Risk-on rotation away from utilities:** **MISS**. Tape is risk-off (ES/NQ/RTY red, Asia red, Europe red), not a tech-led rip. (News Judge #2/#6: chip weakness + AI-hardware→software rotation is *inside* tech, not a rotation into utilities.)
- **Risk-off tape / flight to safety:** **PARTIAL (relative only)**. Red futures + oil shock is a defensive bid *relative to SPY*, but VIX 17.49 in **contango** is not a >20 flight-to-quality spike, and the long end is rising — so relative, not absolute. Per 09-10 gate, this does **not** authorize a relative-beat claim.
- **Nuclear / gas generation policy support:** structural HIT, stale. No same-session order.
- **Grid CapEx approval / recovery:** structural HIT, stale. Search returned only evergreen regulator-dilemma coverage (utilitydive, S&P Global rate-request record) — no fresh same-day order.
- **Adverse rate case / load-growth disappointment / regulatory smash:** carried (WoodMac/Texas/Ohio). Not fresh. Single-name must not drive the ETF (08-28 rule). Duke Energy "valuation debate" (ad-hoc-news 09-15) is single-name and not an ETF driver.
- **Sector rotation out of utilities:** **HIT (carried)**. 1w rel −3.78% / 1m rel −4.30% is a sustained rotation-out, confirmed by the "utilities flash oversold signal as Treasury yields dim dividend appeal" (Seeking Alpha 09-06) and "dramatic reversal as rising Treasury yields erode the appeal of their dividends" theme.
Net: **Rates rising (bond-proxy selloff) + Sector rotation out of utilities** are the dominant fresh factors; the defensive bid is relative-only and sub-gate. **S1 = −2**.

**3. Breadth.** MAP HEAT is decisive and **splits the parent**: Regulated Electric (NEE/SO both red, vs_parent −1.01) = **flat/low**; Regulated Gas (breadth 0.0, vs_parent −1.55) = **weakest pocket**; Regulated Water (captains AWK red, AWR −1.5, CWT −0.89) = **captains do not confirm**; Diversified (SRE +5.55 on ceremony, AES flat) = **tape drift, not a story**; Renewable (ORA bid, FLNC lagging) = low conviction. Only **IPP** (VST +10.4 / CEG +9.43 w1, breadth 0.444) shows heat — and per the 08-28 rule, IPP is **nested, not XLU confirm** (CEG/VST are not the regulated rate-base book). The regulated core — which is the bulk of XLU — has **no bid**. Breadth failure inside the ETF. **S2 = −1**.

**4. Flows.** No confirmed same-day inflow spike or outflow lid in search (only evergreen "early recovery as AI power demand grows" 09-09 and "unexpected AI infrastructure trade" 03-02). The 1m rel −4.30% and the "oversold signal as yields dim dividend appeal" (09-06) describe a **de-risking / rotation-out** regime, not a washout reversal with confirmed inflows. Modest, unconfirmed. **S3 = −0.5**.

**5. Catalysts.** No fresh XLU-wide rate-order/load print. FOMC tomorrow (Warsh) is the dominant binary — per 09-11, for a defensive bond-proxy **both branches are negative-to-neutral** (hike → rates up → bond proxy down; hold/dovish → risk-on rotation away from defensives), so the binary is **asymmetric-negative**, not symmetric. Do not pre-score the direction of the surprise, but do not treat it as a cushion either. Oil/Hormuz escalation is the live overlay.

## Lessons → scores

**09-14 is the binding lesson:** S4 must be ≥ −1.0 when the tape is negative on every horizon and unidirectional; the overlay must not shrink the deterministic engine by ~30% when divergence_flagged and the tape is unidirectional. I am scoring S4 = −1.0 and keeping the overlay shrink modest.

**09-11:** the pending FOMC binary is asymmetric-negative for a defensive bond-proxy — both branches hurt. Risk-on inputs (green futures, oil-offering) are headwinds, not cushions. Today futures are **red**, so the "risk-on inputs" clause is moot; the asymmetry still holds.

**09-10 gate:** VIX 17.49 < 20 and in contango → the rising long end is a **relative-LAG** signal, not a cushion. No relative-beat clause.

**08-18 qualifier:** the long end is the *cause* of the risk-off (oil/supply/term-premium) → XLU is the **transmission channel, not the haven**. Falls AND underperforms.

**08-25 does not apply** (S0/S1 are negative, not zero). **08-12** caps AI-power as a 1d dampener. **08-13** keeps S2/S4 as confirmation only. **08-28** keeps Duke/single-name out of S1.

## Divergence check

Leading factor sum (S0 −1, S1 −2, S2 −1, S3 −0.5) = **−4.5**, strongly negative. Tape confirmation S4 = −1.0, also negative — **no divergence** between factors and tape; they agree. The only counter-signal is the premarket XLU +0.20% green, which is a **relative-only** tell already discounted in S0/S1 and must not be used a second time to cap magnitude (09-14 lesson). Regime: **risk_off**.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -2
S2_BREADTH: -1
S3_FLOWS_POSITIONING: -0.5
S4_ETF_TAPE: -1.0
MULTIPLIER: 1.0
CONFIDENCE: 0.68
REGIME: risk_off
DIVERGENCE_FLAGGED: False
PREDICTED_DIRECTION: down
PREDICTED_MAGNITUDE_BAND: mild
HORIZON_3D: down
HORIZON_1W: down
HORIZON_2W: down
HORIZON_1M: down
SECTOR_SCORES_END

HIT_GRID_BEGIN
Rates rising (bond-proxy selloff)|HIT|0.85|2026-09-15|https://www.euronews.com/business/2026/09/15/us-10-year-treasury-yield-breaches-5-as-global-bond-sell-off-deepens
Sector rotation out of utilities|HIT|0.75|2026-09-15|https://seekingalpha.com/news/4640528-utilities-flash-oversold-signal-as-treasury-yields-dim-dividend-appeal
Risk-off tape / flight to safety|PARTIAL|0.55|2026-09-15|https://www.vantagemarkets.com/market-analysis/us10y-treasury-yield-tops-5-percent-september-2026/
Data-center load growth / power demand upside|HIT|0.40|2026-09-15|https://news.google.com/rss/articles/CBMi1gFBVV95cUxOQ2xwRDRDWnJ0SzI2dDc4TllMc3V0YUdsRjc5MVRYZDA1cDhjamtkX0c1YkZZNjRXV2lCZ2JrS0ItbGdoZng3YXBXUWlVdVczLXpZV1lUeXVQLTQ2NGt6N2hfc0JNYVdTbGdHcVhPSTFPOEs4NjR0ZkRqVXZEUzRSN01mSXFaU1dtano5ZGlha1VYdXhPbk0tTHU3Q1VqdjZKSEJGY2l1ZXU5OVZLbjgzbmhxSHRhQjV6bWIydHlWRWJ5V3JKOTZQa3UtdFl4WVN1eEtYcXhB0gHWAUFVX3lxTE5DbHBENENacnRLMjZ0NzhOWUxzdXRhR2xGNzkxVFhkMDVwOGNqa2RfRzViRlk2NFdXaUJnYmtLQi1sZ2hmeDdhcFdRaVV1VzMtellXWVR5dVAtNDY0a3o3aF9zQk1hV1NsZ0dxWE9JMU84Szg2NHRmRGpVdkRTNFI3TWZJcVpTV21qejlkaWFrVVh1eE9uTS1MdTdDVWp2NkpIQkZjaXVldTk5VktuODNuaHFIdGFCNXptYjJ0eVZFYnlXcko5NlBrdS10WXhZU3V4S1hxeEE?oc=5
Rates falling (bond-proxy bid)|MISS|0.85|2026-09-15|https://tradingeconomics.com/united-states/government-bond-yield
Risk-on rotation away from utilities|MISS|0.60|2026-09-15|https://www.vantagemarkets.com/market-analysis/us10y-treasury-yield-tops-5-percent-september-2026/
Sector breadth failure (ETF up, names flat)|HIT|0.55|2026-09-15|https://news.google.com/rss/articles/CBMiwgFBVV95cUxObGtEZVVkd0dRZHgyOTF5TlZSaTRPcHJjQ0RMUW1xTjdPX29kMzZ3S1VZNHVNZGNNaWQ4MUhxTUZfcGJuZ21veTVoQk81OHdndDlvenhla01seFZCOTJPTTgxcmN6SDc3MWwyeTg1TXVwa1NWajRVWDBDTjVucTRFX3JpN0hmODZhU2F3dy1kQ1o2QUQyNzB3LWxycHFobVhUYTcxTjc0YmV0WWJDNy14UUdiWDVQV2x6cF80NWo3dW5NZw?oc=5
Adverse rate case|NEUTRAL|0.35|2026-09-15|https://news.google.com/rss/articles/CBMi6AFBVV95cUxQNV9mYzBZSEhwdW0zRl9TUHFjTndFZDdfbjlsSVFxVDNLVkt6WVhqUGVBejJpLWFwaF9JU1ZUejZWTUs0X0YyOXVXNmF2UFdLLVlaQ045ZUx3aEx4Q2VGUENfZThKYldPVzZheFBVY3ZZYjJLN1o1MkUwMHVwSHlnUUJfNktCdm44SE5Jc2o4Z2JJM2duSElnM1hiRUd1Vk0zQW9QQ0RKbVpyNFJiZldIT2NXWE0zcnRnNGRVNDRNREk2c3otamp6dTFhckFoaUVSb1Q2S3pKTEd4UkxydG11ZjkzQW9ieGtW?oc=5
HIT_GRID_END

**Bottom line:** XLU enters 2026-09-15 as a bond proxy in the worst possible configuration — a 10Y that just breached 5% (highest since 2007), a real yield at 2.60% and rising on every window, oil at war-premium levels feeding the long end, a risk-off tape where the long end is the *cause* (so XLU is the transmission channel, not the haven), VIX sub-20 and in contango (failing the 09-10 relative-beat gate), and a regulated core with no breadth bid (MAP HEAT: NEE/SO red, gas/water captains unconfirmed; only nested IPP shows heat). The premarket +0.20% green is a relative-only tell, already discounted. Call: **down / mild**, with the explicit relative-lag clause — XLU should fall in absolute terms and underperform SPY. FOMC tomorrow is an asymmetric-negative binary (both branches hurt a defensive bond proxy), which caps conviction at 0.68 rather than higher.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -2.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': -0.5, 'S4_ETF_TAPE': -1.0}, 'multiplier': 1.0, 'leading_sum': -10.0, 'divergence_flagged': True, 'total_score': -6.556, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.65, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.0671, 'score': 0.403, 'legs': [{'leg': 'ES', 'pct': 0.34, 'w': 0.3}, {'leg': 'ZN', 'pct': -0.46, 'w': 0.75}, {'leg': 'PM:XLU', 'pct': 0.2, 'w': 0.7}]}, 'overlay_score': -6.0, 'overlay_raw': -9.875, 'index_carry': -0.959, 'general_total': -3.836, 'skill_multipliers': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 1.25, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.25}, 'llm_confidence': 0.68, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
