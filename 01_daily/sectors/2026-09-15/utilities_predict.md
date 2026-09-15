# Sector Prediction — Utilities — 2026-09-15

- ETF: **XLU**
- rubric: `00_grounding/sectors/utilities.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-6.499** (mult 1.0)
- regime: risk_off
- divergence_flagged: **True**
- engine: v2 · tape_anchor **0.413** (ES +0.36%, ZN -0.46%, PM:XLU +0.20%) · index_carry **-0.912** (general -3.65) · llm_overlay **-6.0** (raw -9.875)

## Channel 1 sector ETF tape

```
ETF XLU vs SPY (yfinance, through 2026-09-15):
  1d: XLU -0.79% | SPY -0.48% | rel -0.31%
  3d: XLU -2.42% | SPY -0.08% | rel -2.35%
  1w: XLU -4.51% | SPY -1.14% | rel -3.37%
  1m: XLU -6.36% | SPY -2.46% | rel -3.91%
```

I have enough to score. Let me compile.

**MEMORY_CONFIRM: Utilities/XLU only** — rolling dir=0.412 / mag=0.412 (n=17). Last graded 09-14 down/mild vs XLU −1.34% / SPY −0.45% / rel −0.90% (dir HIT, mag MISS — S4 under-scored at −0.5 despite a decisively red multi-horizon tape; overlay shrank the deterministic engine ~30% when it should have shrunk ≤15%). Applied: 09-14 (when 1d/3d/1w/1m rel all negative AND freshest 1d rel ≤ −1%, S4 ≥ −1.0; limit overlay shrink to ~15% when divergence_flagged and tape is unidirectional); 09-11 (a pending high-impact macro binary is ASYMMETRIC-negative for a defensive bond-proxy — test each branch sign; risk-on inputs are headwinds, not cushions); 09-10 (gate the 08-18 relative-beat frame on VIX ≥ ~20 or explicit FTS; VIX 17.55 → rising long end = relative-LAG signal; score a live long-end auction in S1); 09-09 (a cushion built on a STATIC shock is mean-reversion fuel); 09-08 (oil at elevated $90+ = inflation/duration unwind, not an FTS bid); 08-18 (risk-off + rising long end → relative beat / flat-to-negative absolute; when the long end is the CAUSE of the risk-off, the bond proxy is the transmission channel, not the haven); 08-21 (live curve, not stale FRED 1d); 08-27 (do not pay carried easing in S0 and S1); 08-25 (S0=S1=0 → do not manufacture down — NOT applicable, S0/S1 negative); 08-12 (AI-power is a 1d dampener); 08-13 (S2/S4 confirmation only); 08-28 (do not promote a single-name regulatory item into S1). Open experiment: require one extra confirming source in the dominant bucket before full weight — used live 10Y/30Y + real yield + oil + VIX term structure + XLU premarket + MAP HEAT. Memory index unavailable this run; used injected sector logs + Channel 1 + web search.

---

# Utilities (XLU) — 2026-09-15

Object is the **near-session XLU environment**, not SPX and not a stock pick.

## Channel 1 (trusted, not re-derived)

XLU vs SPY through 2026-09-15: **1d −0.79% / −0.48% (rel −0.31%)**; **3d −2.42% / −0.08% (rel −2.35%)**; **1w −4.51% / −1.14% (rel −3.37%)**; **1m −6.36% / −2.46% (rel −3.91%)**. Every horizon is negative and the lag is **widening** (1w rel −3.37%, 1m rel −3.91%). The freshest 1d rel (−0.31%) is sub-gate, but the multi-horizon tape is decisively red — per the 09-14 lesson this earns **S4 = −1.0**, not −0.5.

Macro panel: VIX **17.55** (+0.45 1d, +1.83 1w) with **VIX/VIX3M 0.898 — CONTANGO** (a regime change from the backwardation of 09-10→09-14); DGS10 **4.96** (as of 09-11; +1 bp 1d, **+19 bp 1w, +28 bp 1m**); DGS30 **5.35** (+10 bp 1w, +11 bp 1m); DFII10 **2.60** (**+5 bp 1d, +18 bp 1w, +18 bp 1m**); HY OAS 2.71 (+6 bp 1d — creeping, not blowing out); EPU 215.48 (−202.54 1d); **CL=F +5.05% / BZ=F +3.30%** (WTI $103.79, Brent $108.11 — oil at war-premium levels); GC=F −0.38%; DXY **+0.17%** (99.36); **ES=F −0.54% / NQ=F −0.62% / RTY −0.73% / DJIA −0.71%** premarket (decisively red, broad); **XLU premarket +0.20%** (green while ES/NQ red — the least-bad of the defensives alongside XLV +0.04%, XLRE −0.24%); Asia composite **−0.72%** (Kospi −3.26%, Nikkei −0.81%); Europe **−0.31%**; 5-day 10Y–SPX corr **−0.178**.

**Live curve:** 10Y **4.96%** — and per CNBC/Reuters/Bloomberg (09-15), the 10Y **breached 5%** (5.011–5.041% intraday), its highest since 2007. 30Y **5.35%**, real 10Y **2.60%**. **Rising across every tenor and every window.** Bond futures are **decisively red** (10Y note −0.46%, 30Y bond −0.93%, Ultra Bond −1.09%) — a violent backup, not a stabilization.

**Calendar:** No 8:30 CPI/PCE/NFP today. **FOMC decision is TOMORROW, September 16** (Warsh) — a live, unresolved, high-impact policy binary one session ahead. CME FedWatch hike odds ~**56%** (CNBC/Yahoo), with Kalshi ~57% / Polymarket ~49% — the direction of the surprise is **not knowable at the snapshot**.

## Channel 2

**1. Shared macro → this sector.** Three live negatives stack on a bond proxy:
- **Rates:** 10Y 4.96% (breached 5%, highest since 2007), 30Y 5.35%, real 2.60% — rising on every window. Per the **08-18 regime qualifier**, when the long end is the *cause* of the risk-off (supply/term-premium/oil shock), the bond-proxy sector is the **transmission channel, not the haven** — it falls AND underperforms. That is precisely today's configuration.
- **Oil:** WTI $103.79 / Brent $108.11, **+2.3–5.1% on the day** — a fresh inflation impulse feeding the long end. Per 09-08, at elevated oil ($90+) this is an **inflation/duration negative**, not a flight-to-safety bid.
- **Tape:** ES −0.54% / NQ −0.62% / RTY −0.73%, Asia red, Europe red. This is a **risk-off tape with a rising long end** — the 08-18 setup. The **09-10 gate** requires VIX ≥ ~20 or an explicit FTS impulse before granting a relative-beat claim; VIX 17.55 **fails that gate**, so the rising long end is a **relative-LAG** signal for XLU, not a cushion. Note the VIX term structure has flipped to **contango (0.898)** — less acute stress than the backwardated 09-10→09-14 window, which further weakens any FTS claim.
- **The one genuine offset:** XLU premarket **+0.20%** (green while ES/NQ are red) — a nascent defensive bid, identical in character to 09-14's +0.33% which was correctly discounted as relative-only. Per 09-11, for a defensive bond-proxy the risk-on/risk-off inputs must be sign-tested: a *risk-off* tape is a relative positive, yet the **rates + oil** channels dominate the absolute, and the 09-09 lesson says a defensive bid built on a **static** shock is mean-reversion fuel. Oil is *escalating* (+5%), which is the one condition that could sustain a fresh defensive bid — but it is simultaneously the inflation channel that pushes yields higher. Net: **S0 = −1** (rates + oil + contango VIX dominate; the premarket green is a relative, not absolute, tell).

**2. Spine / secondary.**
- **Data-center load growth / power demand upside:** structural HIT, **stale** for a 1d call. Search confirms the narrative is live (AEP raised guidance again on 69 GW of new demand, 09-13; "AI Data Centers Need Enormous Amounts of Power: These 5 Dividend Stocks Provide It," 09-11; Morningstar "AI Power Demand Meets Grid Investment," 09-02) — but there is **no fresh same-session XLU-wide catalyst**. Rubric: do **not** let the multi-year AI-power story override a 1d rate tape.
- **Rates falling (bond-proxy bid):** **MISS**. 10Y 4.96% (breached 5%), 30Y 5.35%, bond futures decisively red. Not falling.
- **Rates rising (bond-proxy selloff):** **HIT**. 10Y +28 bp 1m, real +18 bp 1m, 10Y through 5% — the classical duration channel, live and escalating. One shock, counted once.
- **Risk-on rotation away from utilities:** **MISS**. Tape is risk-off (ES/NQ/RTY red, Asia red, Europe red), not a tech-led rip.
- **Risk-off tape / flight to safety:** **PARTIAL (relative only)**. Red futures + oil shock is a defensive bid **relative to SPY**, but VIX 17.55 in **contango** is not a >20 flight-to-quality spike, and the long end is rising — so relative, not absolute.
- **Nuclear / gas generation policy support:** structural HIT, stale. New US power emissions rules (09-14) are a two-sided regulatory item, not a same-session XLU-wide order.
- **Grid CapEx approval / recovery:** structural HIT, stale.
- **Adverse rate case / load-growth disappointment / regulatory smash:** carried (WoodMac/Texas/Ohio). Not fresh. Single-name must not drive the ETF (08-28 rule).
- **Sector rotation out of utilities:** **HIT (carried)**. 1w rel −3.37% / 1m rel −3.91% is a persistent rotation-out, confirmed by the 247wallst "From Bond Proxy to Battleground: Why Utilities Are the Worst Hiding Spot in 2026" theme and ASGI's "Utilities/Industrials Cooled Off" NAV decline.
Net: **Rates rising (bond-proxy selloff) + sector rotation out of utilities** are the dominant fresh factors; the defensive bid is relative-only. **S1 = −2**.

**3. Breadth.** MAP HEAT is decisive and **splits the parent**: Regulated Electric **lags** parent (vs_parent −1.01, NEE and SO both red on the day — "no rate-base bid here"); Regulated Gas is the **weakest pocket** (breadth 0.0, vs_parent −1.55); Regulated Water captains do not confirm (AWK red, AWR −1.5, CWT −0.89); Diversified is "tape drift, not a story" (SRE +5.55 w1 on ceremony, AES flat). The **only** heat is IPP (VST +10.4 / CEG +9.43 w1 vs parent, breadth 0.444) — but per the 08-28 rule and the MAP HEAT instruction, **IPP is a nested SPLIT, not an XLU confirm** (CEG/VST are not XLU drivers). The regulated core — which *is* XLU — is red. **S2 = −1**.

**4. Flows.** No confirmed same-day inflow spike or outflow lid in search. Prior logs noted modest flows (5d ~+$131M, 1m ~+$22M through early Sept). The 1m rel −3.91% is de-risked, not a crowded-long unwind that forces a bounce. **S3 = −0.5** (mild negative: persistent rotation-out, no confirmed inflow reversal).

**5. Catalysts.** No fresh XLU-wide rate-order/load print today. **FOMC tomorrow (09-16)** is the dominant unresolved binary — per 09-11, for a defensive bond-proxy **both branches are negative-to-neutral** (hawkish → rates up → bond proxy down; dovish → risk-on rotation away from defensives), so the binary is **asymmetric-negative**, not symmetric. Do not pre-score the direction, but do not treat it as a cushion either.

### Lessons → scores

**09-14 is the binding lesson on S4:** 1d/3d/1w/1m rel all negative with a widening lag → **S4 = −1.0**, and the overlay must shrink the deterministic engine by **≤ ~15%** (not ~30%) when divergence is flagged and the tape is unidirectional. The 09-14 mag MISS was exactly this error; I will not repeat it.

**09-11 is the binding lesson on the binary:** FOMC tomorrow is asymmetric-negative for a defensive bond-proxy. Both branches hurt. Risk-on inputs (green futures, oil-offering) are **headwinds, not cushions** — and today futures are *red*, so there is not even a risk-on offset to mis-sign.

**09-10 gate:** VIX 17.55 < 20 and now in **contango** → the rising long end is a **relative-LAG** signal, not a relative cushion. No relative-beat clause.

**08-18 regime qualifier:** the long end is the *cause* of the risk-off (5% 10Y, oil shock) → XLU is the **transmission channel, not the haven**. Falls AND underperforms.

**08-12:** AI-power is a 1d dampener, not a band engine — it does not offset a 5% 10Y.

**08-25 does NOT apply** (S0/S1 are negative, not zero).

### Divergence check

Leading factors (S0 −1, S1 −2, S2 −1, S3 −0.5) sum to **−4.5**; S4 tape confirmation is **−1.0** — **aligned, not divergent**. The only counter-signal is XLU premarket +0.20% (relative-only, already discounted in S0/S1 per 09-14). No same-shock double-count: the rates shock is paid once in S0 (macro) and once in S1 (the sector's own duration transmission, confirmed by the multi-horizon relative lag) — these are distinct channels, not the same fact twice. Single-ticker check: CEG/VST IPP heat does **not** drive the XLU call (nested SPLIT, 08-28 rule).

### Verdict

**Down / mild.** Direction is well-supported: 5% 10Y (highest since 2007), real yields +18 bp 1m, oil +5%, red broad futures, contango VIX, and a widening multi-horizon relative lag with the regulated core (the actual XLU) red. Magnitude is capped at **mild** because (a) XLU premarket is green (+0.20%) — a genuine relative defensive bid, (b) VIX 17.55 is not a >20 stress spike, (c) the FOMC binary tomorrow is unresolved and can reverse the rates cluster in one session, and (d) XLU is already −6.36% 1m, so much of the duration damage is priced. Per the 09-14 lesson I will **not** let the overlay shrink the deterministic engine below ~15%, and I will **not** cap the band below mild — the realized 09-14 move (−1.34%) shows this instrument can print notable on a red multi-horizon tape, so the band sits at the **upper edge of mild**.

```
SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -2
S2_BREADTH: -1
S3_FLOWS_POSITIONING: -0.5
S4_ETF_TAPE: -1
MULTIPLIER: 1.0
CONFIDENCE: 0.68
REGIME: risk_off
SECTOR_SCORES_END
```

```
HIT_GRID_BEGIN
Rates rising (bond-proxy selloff)|HIT|0.90|2026-09-15|https://www.reuters.com/world/asia-pacific/bond-selloff-drives-us-benchmark-beyond-5-stocks-rattled-2026-09-15/
Real yields rising|HIT|0.85|2026-09-15|https://www.cnbc.com/2026/09/15/treasury-yields-stocks-investors.html
Sector rotation out of utilities|HIT|0.75|2026-09-15|https://news.google.com/rss/articles/CBMiwAFBVV95cUxQV0dMelgweTVKYkFvR1Y3a2FwRmVHZ1RMWDllU0tadnlUZHJkVU9jNjRQNGcxUlVobWx0QW9MN1k1cUdSam1vcFVYSWJTQ1ctUUF3dmhDUnhuZ1J0TE5EV2Y4c0FnbmNENFFtS1Nsa01YeVVndW9Fdzh1U19vczNKMjBMUXhVR082MHdTdDdkUERVYWxTdVNKUmNLaEZVcm04UzY2Q082S1UtQzZsck0zeXdVcVAyQURkYnctODhXN00
Rates falling (bond-proxy bid)|MISS|0.90|2026-09-15|https://www.kitco.com/news/off-the-wire/2026-09-15/bond-selloff-drives-us-benchmark-beyond-5-stocks-rattled
Risk-on rotation away from utilities|MISS|0.70|2026-09-15|https://www.cnbc.com/2026/09/15/treasury-yields-stocks-investors.html
Risk-off tape / flight to safety|PARTIAL|0.55|2026-09-15|https://www.euronews.com/business/2026/09/15/us-10-year-treasury-yield-breaches-5-as-global-bond-sell-off-deepens
Data-center load growth / power demand upside|HIT|0.60|2026-09-15|https://news.google.com/rss/articles/CBMirgFBVV95cUxPNHpjQklZa2k0bFZmaGJUSFd3TU5SWFZ1SHo3SXBVb1hLelItUzFQQkstU2lacUJMQWJSSHVtVW9pT2E1bkxjT2tNbWxRVkhqLTQzLXRucHRPOUdzVV9NMnZ2YUJ6eUtkcFlvVGt4TW9fTTQxRnZEV0NaMGtjR3hoTTRUaVZRU2lpRUdZQUdOQTJMaTFkT1otWDgxQWdsdGNqZGRfNFlQNkRuUENXY1E
Sector breadth failure (ETF up, names flat)|MISS|0.60|2026-09-15|https://news.google.com/rss/articles/CBMimwFBVV95cUxPVDFBbVdHdldUZlFkeHRBcXhWTHdLQmlwY1IwUmdoMjdSYmNWcUgtbDNSN3NrWGZpSGxZQlM5MHhzUEZCcWFXTjZWaUFtNlNSeGRzNG81TFJHQjlWS0c1TVphaG9yRVk0b1Rsb2lzNGMxVXNoTWgyVUlZU3h0ZF9SZG8zUEp0X3VTeF9aMURzdktJTUZfQloxY3o3cw
Crowded long (extreme relative performance + valuation)|MISS|0.65|2026-09-15|https://news.google.com/rss/articles/CBMiqAFBVV95cUxPVGVJeEFncDZRSWtXb0YxT2lKQ1Z4MTdPZDNNTUpORjdlVnh4OGxKeHdzNUNPWDN6T3JYempDZGJROU5OVmJmejREOUo4YnFlM2gySHg3WDd0U09JSVNEVG1udXNPZHgzYkhydUVGM19lSU5qdGdrT1dmcmJxU0ppTlJiV2l1SzFSUTUzOUw2am1DRXUtVUtoeVFZemVXcVJYZEtyczRiYmQ
HORIZON_3D: down
HORIZON_1W: down
HORIZON_2W: down
HORIZON_1M: flat
HIT_GRID_END
```

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -2.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': -0.5, 'S4_ETF_TAPE': -1.0}, 'multiplier': 1.0, 'leading_sum': -10.0, 'divergence_flagged': True, 'total_score': -6.499, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.65, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.0689, 'score': 0.413, 'legs': [{'leg': 'ES', 'pct': 0.36, 'w': 0.3}, {'leg': 'ZN', 'pct': -0.46, 'w': 0.75}, {'leg': 'PM:XLU', 'pct': 0.2, 'w': 0.7}]}, 'overlay_score': -6.0, 'overlay_raw': -9.875, 'index_carry': -0.912, 'general_total': -3.65, 'skill_multipliers': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 1.25, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.25}, 'llm_confidence': 0.68, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
