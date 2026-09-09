# Sector Prediction — Utilities — 2026-09-09

- ETF: **XLU**
- rubric: `00_grounding/sectors/utilities.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **0.45** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLU vs SPY (yfinance, through 2026-09-08):
  1d: XLU +0.86% | SPY -0.55% | rel +1.41%
  3d: XLU +1.83% | SPY +0.10% | rel +1.72%
  1w: XLU +2.89% | SPY -0.14% | rel +3.03%
  1m: XLU -0.37% | SPY -0.94% | rel +0.58%
```

MEMORY_CONFIRM: Utilities/XLU only — rolling dir=0.4 / mag=0.4 (n=10). Last graded: 09-04 flat/flat vs XLU +0.12% / SPY −0.39% / rel +0.51% (dir MISS, mag HIT — hot jobs report was the dominant driver, not knowable at open as a calendar miss). 09-08 down/mild vs XLU +0.86% / SPY −0.55% / rel +1.41% (dir MISS, mag MISS — the defensive bid overwhelmed the rate spine; the 1d relative cushion was the single most predictive input). Applied: 08-27 (PCE out, mega-cap AI/software public, no fresh XLU catalyst → relative lag / flat-to-down absolute; do not pay carried easing in S0 and S1); 08-18 (bond-proxy + elevated long-end → flat-to-negative absolute; risk-off + rising yields → relative beat, not absolute up); 08-21 (live curve, not stale FRED 1d); 08-25 (S0=S1=0 → do not manufacture down from carried lag); 08-17 (no same-day miss printed, no fresh yield rally → no absolute up); 08-11 (does not flip up: yields not easing, tape not inflecting); 08-12 (AI-power is a 1d dampener); 08-13 (S2/S4 confirmation only); 08-14 (calendar: no 8:30 CPI/PCE/NFP today; ISM Services 10:00 ET two-sided); 08-28 (do not pre-score a 10:00 print into notable-down; CEG/VST IPP are not XLU ETF drivers); 09-03 (two-sided macro catalyst → widen magnitude band, not flat); 09-04 (calendar scan must include employment releases); 09-08 (1d relative cushion ≥ +0.4% + prior-session outperformance → direction override toward flat, not magnitude cap only). Memory index unavailable this run; used injected sector logs + web search.

## Utilities (XLU) — 2026-09-09

Object is the **near-session XLU environment**, not SPX and not a stock pick.

### Channel 1 (trusted, not re-derived)

XLU vs SPY through 2026-09-08: **1d +0.86% / −0.55% (rel +1.41%)**; **3d +1.83% / +0.10% (rel +1.72%)**; **1w +2.89% / −0.14% (rel +3.03%)**; **1m −0.37% / −0.94% (rel +0.58%)**.

Macro panel: VIX elevated (risk-off); DGS10 **~4.8%+** (20-month high per search, +13 bp 1w); DGS30 **~5.27%** (19-year high); DFII10 **~2.45%** (+11 bp 1w); HY tight; EPU elevated; **CL=F +2.4% to $100.29 / Brent >$100** (WTI crossed $100 for first time since July); DXY firm; **ES=F negative / NQ=F negative** (futures slip per WSJ/Yahoo); Asia mixed; Europe **−0.7%** (Reuters: European stocks drop on fresh Gulf attacks); F&G Greed (stale); 5-day 10Y–SPX corr negative.

**Live curve (08-21 / 08-25 duration check):** 10Y ~**4.8%+** (20-month high), 30Y ~**5.27%** (19-year high). **Rising**, not easing. Oil at **$100+** is a fresh inflation shock feeding the long-end.

**Calendar (08-14 / 08-27 / 09-04):** No 8:30 CPI/PCE/NFP today. **MBA mortgage applications 7:00 ET** (already printed, not a regime-flip). **10-year Treasury auction** today (supply event — can move long-end). No FOMC. The dominant driver is the **live oil shock + hawkish Fed repricing**, not a scheduled print.

### Channel 2

**1. Shared macro → this sector.** The dominant driver is the **live oil shock** (WTI >$100, Brent >$100) from **fresh Middle East strikes** (US military struck five Iranian tankers after Iran fired ballistic missiles at a US Navy warship; Houthis hit Saudi energy facilities). This is a **stagflation shock**: oil → inflation expectations → long-end yields up (10Y 20-month high, 30Y 19-year high) → rate-sensitive bond-proxy utilities face a **direct duration headwind**. BUT: oil at $100+ is also a **risk-off trigger** that can drive flight-to-safety into defensives. The 09-08 lesson (when oil is already elevated $90+ and the sector is crowded-long, oil spike = inflation/duration negative, not a defensive bid) applies — but XLU is **not** crowded-long anymore (1m rel +0.58%, de-risked after the August smash). The 08-18 lesson (risk-off + rising yields → relative beat / flat-to-negative absolute) is the operative frame: XLU can **outperform SPY** on the defensive bid while **falling in absolute terms** if long-end yields keep rising. Futures are **negative** (ES/NQ slip), Europe red, oil $100+. This is a **risk-off tape with rising long-end yields** — the exact 08-18 setup. **S0 = 0** (mixed: defensive bid vs duration headwind; do not score S0=+1 from FTS, do not score S0=−2 from rates — the 08-18 frame says relative beat, flat-to-negative absolute).

**2. Spine / secondary.**
- **Data-center load growth / power demand upside:** structural HIT, **stale** for a 1d call. Search: "Utilities stocks show early recovery as AI power demand grows" (Sept 8) — but no fresh same-day XLU-wide catalyst. Rubric: do **not** let the multi-year AI-power story override a 1d rate tape without a fresh XLU catalyst.
- **Rates falling (bond-proxy bid):** **MISS**. Live 10Y ~4.8%+ (20-month high), 30Y ~5.27% (19-year high). **Rising**, not falling.
- **Rates rising (bond-proxy selloff):** **HIT**. Oil $100+ → inflation → long-end yields up. One shock, counted once.
- **Risk-on rotation away from utilities:** **MISS**. Tape is risk-off (futures negative, Europe red), not a tech-led rotation.
- **Risk-off tape / flight to safety:** **HIT** (relative). Oil $100+, Middle East strikes, futures negative — this is a defensive bid for utilities **relative to SPY**.
- **Nuclear / gas generation policy support:** structural HIT, stale.
- **Grid CapEx approval / recovery:** structural HIT, stale.
- **Load growth disappointment / adverse rate case:** carried, not fresh.
Net: **Rates rising (bond-proxy selloff) + Risk-off tape / flight to safety (relative)** are the dominant fresh factors. The rates headwind and the defensive bid are **opposing forces** — the 08-18 frame says the defensive bid wins **relatively** but the rates headwind caps **absolute** upside. **S1 = 0** (net neutral: rates-rising negative offset by risk-off defensive bid).

**3. Breadth.** 1d rel +1.41%, 3d rel +1.72%, 1w rel +3.03% — **strong positive relative tape** after the August smash. 1m rel +0.58% — now positive, de-risked. The 09-08 lesson (1d relative cushion ≥ +0.4% + prior-session outperformance → direction override toward flat) applies: XLU has a **large pre-existing 1d relative cushion** (+1.41%) going into a risk-off session. This cushion is a **direction signal toward flat**, not just a magnitude cap. **S2 = 0** (mixed — positive relative tape but no confirmed breadth expansion; MAP HEAT shows IPP up, regulated electric down).

**4. Flows.** Search: XLU closed +0.86% at $43.45 on 9/8, RSI 44 neutral, below 50-day average $44.39. No confirmed same-day inflow spike. 1m rel +0.58% is de-risked, not crowded-long. **S3 = 0** (neutral).

**5. Catalysts.** Oil $100+ (fresh Middle East strikes) is the dominant driver. 10-year Treasury auction today (supply event). No fresh XLU-wide rate-order/load print. MAP HEAT: IPP (CEG/VST) up, regulated electric down — the IPP bid is **not** XLU confirm (CEG/VST are IPP, not the regulated-electric core of XLU).

### Lessons → scores

**09-08 is the key lesson:** When the 1d relative cushion is ≥ +0.4% AND the prior session confirmed XLU outperforming SPY on a risk-off day, the cushion must be scored as a **direction override toward flat**, not just a magnitude cap. XLU has a **+1.41% 1d relative cushion** — the largest in the recent tape. The negative rate spine (oil $100+, long-end yields up) is **structural**, not a fresh same-day shock to XLU specifically. The cushion should **downgrade S0/S1 contribution** — cap combined S0+S1 at −1 total (not −2) when the cushion exceeds the expected same-day downside transmission.

**08-18 is the veto on up:** risk-off + rising 10Y → **relative beat / flat-to-negative absolute**. XLU can outperform SPY while closing flat-to-slightly-down in absolute terms. Do **not** mint S0/S1 = +1 from the defensive bid.

**08-27 does not fire as a down mandate:** PCE is out, but the tape is **risk-off** (not an NQ-led tech rip). The lag default does not apply.

**08-25 does not force flat:** S1 is not 0 (rates-rising is a real negative), but the 09-08 cushion override applies.

**08-11 does not flip up:** yields are **rising** (oil $100+), not easing. Tape is not inflecting positive in a way that overrides the rate spine.

**08-12:** AI-power stays a dampener; band **mild**.

**08-14 / 09-03 / 09-04:** No scheduled high-impact macro print today (MBA already printed, 10Y auction is supply, not data). The oil shock is the live catalyst.

**Divergence:** Leading (S0 0 + S1 0 + S2 0 + S3 0 = 0) and S4 (+1) **disagree** — the tape is positive (defensive bid) but the leading factors are neutral (rates headwind offsets). This is a **divergence**: the positive relative tape is the defensive bid, but the rates headwind caps absolute upside. Per 09-08, the cushion points to **flat**, not up.

### Final call

The setup is a **live oil shock** (WTI >$100, Brent >$100) with **rising long-end yields** (10Y 20-month high, 30Y 19-year high) on a **risk-off tape** (futures negative, Europe red). For bond-proxy utilities, this is the **08-18 frame**: risk-off + rising yields → **relative beat / flat-to-negative absolute**. XLU has a **large 1d relative cushion** (+1.41%) from the prior session's defensive bid — per 09-08, this cushion is a **direction override toward flat**, not just a magnitude cap. The defensive bid (oil shock, risk-off) and the duration headwind (long-end yields up) are **opposing forces** that net to **flat-to-mild-down absolute** with **relative outperformance**. The 10-year Treasury auction today is a supply event that could push long-end yields higher intraday — a risk to the downside. **Direction: flat. Magnitude: mild.** Confidence: moderate (the oil shock is live and could escalate, but the rates headwind caps absolute upside).

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 0
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 1
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: risk_off
DIVERGENCE_FLAGGED: True
PREDICTED_DIRECTION: flat
PREDICTED_MAGNITUDE_BAND: mild
TOTAL_SCORE: 0.9
HORIZON_3D: flat:mild:0.5
HORIZON_1W: flat:mild:0.5
HORIZON_2W: up:mild:0.5
HORIZON_1M: up:mild:0.5
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-off tape / flight to safety|HIT|0.7|2026-09-09|Oil >$100, Middle East strikes, futures negative — defensive bid for utilities relative to SPY
Rates rising (bond-proxy selloff)|HIT|0.7|2026-09-09|Oil $100+ → inflation → long-end yields up (10Y 20-month high, 30Y 19-year high)
Data-center load growth / power demand upside|HIT|0.5|2026-09-08|Structural, stale for 1d call — "Utilities stocks show early recovery as AI power demand grows"
Sector rotation into utilities|PARTIAL|0.4|2026-09-09|Defensive bid on oil shock, but IPP (CEG/VST) leads, not regulated-electric XLU core
Rates falling (bond-proxy bid)|MISS|0.0|2026-09-09|Live 10Y ~4.8%+ rising, not falling
Risk-on rotation away from utilities|MISS|0.0|2026-09-09|Tape is risk-off, not tech-led rotation
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 1.0}, 'multiplier': 0.9, 'leading_sum': 0.0, 'divergence_flagged': False, 'total_score': 0.45, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.55, 'regime': 'risk_off', 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
