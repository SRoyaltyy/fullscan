# Sector Prediction — Healthcare — 2026-09-15

- ETF: **XLV**
- rubric: `00_grounding/sectors/healthcare.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-4.136** (mult 0.9)
- regime: risk_off
- divergence_flagged: **True**
- engine: v2 · tape_anchor **0.535** (ES +0.34%, PM:XLV +0.04%) · index_carry **-0.959** (general -3.836) · llm_overlay **-3.712** (raw -3.712)

## Channel 1 sector ETF tape

```
ETF XLV vs SPY (yfinance, through 2026-09-15):
  1d: XLV -0.16% | SPY -0.51% | rel +0.35%
  3d: XLV +1.10% | SPY -0.11% | rel +1.21%
  1w: XLV +0.21% | SPY -1.17% | rel +1.38%
  1m: XLV +0.07% | SPY -2.49% | rel +2.56%
```

MEMORY_CONFIRM: Healthcare/XLV call for 2026-09-16 (envelope 2026-09-15). Rolling HC dir=0.60 mag=0.267 (n=15); mag experiment active (keep direction, shrink confidence). Binding lessons tested: **09-14 reflect** (duration-led risk-off → low-beta defensive is the DESTINATION, S0 positive) — preconditions PARTIALLY met today (NQ −0.62% vs ES −0.54%, only 8bp spread = broad uniform risk-off, not duration-led; but XLV is no longer under-owned after rel +1.89% on 09-14 and +0.35% today, 1m rel +2.56%). **09-11 reflect** (green-futures/oil-falling → defensive is funding source) — does NOT fire (futures red, oil spiking). **09-10 cap** (≥2 consecutive 1d rel stabilizations → decay S0/S1/S2, cap ≈ −1.5) — FIRES. **08-13 reversal-tell** (defensive led on multi-day relative run, flat 1d rel, no fresh catalyst) — FIRES as a magnitude cap. **08-17 oil FTS** — narrowed off (oil already $104+). **08-28 leftover-stack** — ban on copying the 3d/1w rel into S2/S3/S4. **08-14 policy audit** — no same-morning mega-cap Rx headline. **08-21 trial-spillover** — AZN SERENA-4 miss is single-large-cap, must not dominate. No oil double-count into rotation. No AVGO/XLK map into S0.

---

# Healthcare / XLV — 2026-09-16

**Object:** near-session environment for **XLV** (not SPX, not a stock picker).

## Channel 1 (trusted, unaltered)

XLV vs SPY through **2026-09-15**: **1d −0.16% / −0.51% (rel +0.35%)**; 3d rel **+1.21%**; 1w rel **+1.38%**; 1m rel **+2.56%**. This is a **fully repaired relative tape** — XLV has led SPY on 3d/1w/1m. The 1d rel print (+0.35%) is **modest**, not a breakout. This is precisely the 08-13 reversal-tell configuration: a defensive sector that has led on a multi-day relative run, with a flat-to-modest 1d relative print and no fresh same-day sector catalyst.

Macro: VIX **17.49** (+0.39 1d, +1.77 1w); **VIX/VIX3M 0.897** — contango restored (no backwardation stress). **Finviz futures: ES −0.54%, NQ −0.62%, RTY −0.73%, DJIA −0.71%** — a **broad, roughly uniform red tape** (RTY/DJIA worst, NQ only 8bp worse than ES → NOT duration-led). **WTI $103.79 +2.37% / Brent $108.11 +2.31%** — oil spiking again, day-5+ of the Hormuz escalation. **10Y Note −0.46%, 30Y Bond −0.93%, Ultra Bond −1.09%** — a **global bond selloff, 10Y yield breaching 5%** (news judge #1). DFII10 **2.60 (+0.05 1d, +0.18 1w, +0.18 1m)** — real yields rising. DGS10 **4.96 (+0.19 1w)**. DXY **+0.25%** (strengthening). HY OAS **2.71 (+0.06 1d)**. Metals complex broadly down (Gold −1.01%, Silver −1.47%, Copper −0.76%, Platinum −1.67%, Palladium −1.85%). Bitcoin **−3.01%**. Asia composite **−0.72%** (Kospi −3.26%, Nikkei −0.81%); Europe **−0.31%**. **Note the Channel 1 internal conflict:** the Finviz futures panel is red across all four indices, but the separate `[ES=F premarket: 0.34%]` / `[NQ=F premarket: 0.26%]` line is green, and `[SECTOR ETF premarket: XLV +0.04%]`. The red Finviz panel is treated as authoritative (it is the same-session live tape; the green ES/NQ line is stale/contradictory). **No 8:30 high-impact print today** (CPI printed 09-11; FOMC next week).

## Channel 2

**1. Shared macro → this sector (S0).** This is a **broad, uniform risk-off** morning: all four index futures red within a tight band (ES −0.54%, NQ −0.62%, RTY −0.73%, DJIA −0.71%), driven by a **global bond selloff with 10Y breaching 5%** and a **fresh oil spike** (Brent $108). The 09-14 reflect lesson's mirror rule says: for a low-beta defensive, a **duration-led** risk-off (NQ leading ES down) makes XLV the *destination* → S0 positive. But the precondition is **not met today**: NQ leads ES by only 8bp, and RTY/DJIA are the worst of the four — that is the signature of a **broad, rate-driven de-risking**, not a duration-led growth selloff. In a broad rate-driven selloff, the defensive bid is real but **second-order** relative to the absolute duration/inflation hit: rising real yields (+0.05 1d, +0.18 1m) are a direct drag on the XBI sleeve, and a 5% 10Y compresses all equity multiples. The 09-11 lesson's core rule cuts the other way here: the macro overlay's sign for a defensive depends on whether the tape is a cyclical risk-on impulse (negative) or a duration-led risk-off (positive). Today is neither cleanly — it is a **rate-shock risk-off**, which is a *relative* cushion for XLV but an *absolute* negative. Net: **S0 = −0.5** — the rate/oil shock is a net absolute negative, partially cushioned by the defensive relative bid. Do not score the oil shock again as "rotation into healthcare."

**2. Spine / secondary (S1).**
- **CMS / MA 2027 +2.48%:** April finalization — **stale**. Not a re-rate.
- **Biotech / XBI:** Rising real yields (+0.18 1m) are a **direct duration drag** on the sleeve. MAP HEAT Biotechnology **dir=up** (VRTX pos, REGN mixed) but captains split — not broad XBI leadership. Not a funding-winter cluster, but a live duration drag.
- **Drug pricing:** IRA/MFN residual, **no same-morning mega-cap Rx headline**. 08-28: residual after comments-closed is **not** S1=−1. 08-14 does **not** fire.
- **FDA / trial cluster (fresh, two-sided):** **AZN SERENA-4 Phase-3 miss** (Etcamah first-line breast cancer) is a **fresh large-cap oncology readout failure** — a genuine negative for oncology/ADC sentiment, but single-large-cap. **AMGN IMDELLTRA label update** (FDA approval to reduce monitoring for first two ES-SCLC doses) is a **fresh single-ticker positive**. Per the 08-21 breadth rule, neither is a confirmed basket cluster (no partner/sub-complex co-move confirmed premarket), so neither dominates. Net: **mild negative tilt** from the AZN miss, largely offset by AMGN.
- **ARGX Forte Biosciences close** ($77/share, anti-CD122 antibody) — single-name M&A, not a breadth cluster.
- **Utilization:** Structural 2027 medical trend — mentioned, not stacked.
- **Rotation:** 3d/1w/1m rel are **all positive** — XLV has been the relative destination. But the 1d rel edge has decayed to +0.35% (modest), and there is **no fresh same-day sector catalyst**. Per 08-13, a flat-to-modest 1d rel after a multi-day relative run is a **reversal tell**, not an up license. Do not HIT "rotation into" as a fresh spine.

Net **S1 = −0.5** (duration drag on biotech sleeve + AZN oncology miss, partially offset by AMGN label update; no fresh MA, no XBI leadership, no same-morning Rx smash).

**3. Breadth (S2).** 09-10 reflect lesson: **S2 must score only sector-internal breadth**, not cross-asset co-moves. MAP HEAT is **split-to-negative**: Biotechnology **up** (VRTX pos), Healthcare Plans **up** (CVS pos, vs_parent_w1 +4.28), Drug Manufacturers-General **flat** (LLY mixed, JNJ pos), Specialty/Generic **flat**, Facilities **flat**, but **Diagnostics down** (DHR −4.0% w1), **Medical Devices down** (worst pocket, w1 −5.0%, d1 −3.53%, vs_parent_w1 −3.24), **Medical Distribution down** (MCK breach overhang), **Medical Instruments down** (group −4.24 w1, breadth 0.077). This is **narrow payer/biotech leadership with broad devices/diagnostics/distribution weakness** — not broad sector breadth expansion. **S2 = −0.5** — sector-internal breadth is mixed-to-negative (devices/diagnostics/distribution failing), not a healthy expansion.

**4. Flows (S3).** XLV 1m rel is **+2.56%** — the sector has re-extended relative to SPY after the 09-14 snap-back. This is a **mild crowded-long dampener**, not a fresh inflow spike. No confirmed same-session inflow or outflow print. Trailing flows not a 1-day lid (08-28). **S3 = 0** — no fresh flow signal; the re-extension is a dampener, not a driver.

**5. Tape (S4, confirmation only).** Channel 1 **1d rel +0.35%** is a **modest** positive — S4 confirms **this** session, not the prior close (08-28). But the 08-13 reversal-tell configuration is live: XLV has led on 3d/1w/1m, the 1d rel edge has decayed to modest, and there is no fresh same-day catalyst. On a red tape with a rate shock, the defensive bid is a **relative** cushion, not an absolute up driver. **S4 = 0** — the modest 1d rel is not a fresh down vote, but it is also not an up license; it is a reversal-tell cap on magnitude.

**6. Catalysts.** Fresh/knowable: **10Y breaching 5% / global bond selloff** (S0 rate shock), **oil spiking to $108 Brent** (S0 inflation leg), **AMD −5% on AI-slowdown calls** (tech, not HC), **AZN SERENA-4 miss** (S1 single-large-cap negative), **AMGN IMDELLTRA label update** (S1 single-ticker positive). Not fresh: MA rates, IRA comments-closed, ABBV/AMGN cluster (T+7/paid), ABT TactiFlex (single-ticker). FOMC next week is the pending binary — do not pre-score.

### Lessons applied
| Lesson | Fire? | Action |
|---|---|---|
| 09-14 duration-led risk-off → defensive destination | **Partial** | Precondition NOT met (NQ only 8bp worse than ES; RTY/DJIA worst = broad rate-driven, not duration-led). S0 = −0.5, not +0.5. |
| 09-11 green-futures/oil-falling → defensive funding source | **No** | Futures red, oil spiking — opposite setup. |
| 09-10 cap (≥2 stabilizations → decay, cap ≈ −1.5) | **Yes** | S0+S1+S2 capped at −1.5. |
| 08-13 reversal-tell (multi-day rel run + flat 1d + no catalyst) | **Yes** | Forbids up/notable; caps magnitude at mild. |
| 08-17 oil FTS bid | **No** | Oil already $104+; narrowed off. |
| 08-28 leftover-stack | **Yes** | Do not copy 3d/1w rel into S2/S3/S4. |
| 08-14 policy audit | **No** | No same-morning mega-cap Rx headline. |
| 08-21 trial-spillover | **Yes** | AZN single-large-cap; must not dominate. |

### Divergence check
The engine's tape_anchor reads the tape as mildly **positive** (ES +0.34%, PM:XLV +0.04%) while the LLM overlay is **negative** (−0.5/−0.5/−0.5/0/−0.5). This is a genuine sign conflict between the leading factor sum and the tape confirmation score. Per the shared method, **trust factors over tape** — the rate shock (10Y >5%) and oil spike are the dominant live inputs, and the defensive relative bid is a cushion, not an up driver. **Divergence flagged True.**

### Verdict
Broad rate-shock risk-off (10Y >5%, oil $108) with a defensive sector that has already led on 3d/1w/1m and whose 1d rel edge has decayed to modest. The defensive bid is a **relative** cushion, not an absolute up driver; the duration/inflation hit is the dominant absolute input. Capped stack (09-10) + reversal-tell (08-13) → **down/mild**, low confidence, magnitude shrunk per the active experiment.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -0.5
S1_SECTOR_FACTORS: -0.5
S2_BREADTH: -0.5
S3_FLOWS_POSITIONING: 0.0
S4_ETF_TAPE: 0.0
MULTIPLIER: 0.9
CONFIDENCE: 0.5
REGIME: risk_off
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-off tape / flight to safety|HIT|0.7|2026-09-15|https://www.investopedia.com/
Real yields rising|HIT|0.75|2026-09-15|https://www.cnbc.com/
Sector breadth failure (ETF up, names flat)|PARTIAL|0.5|2026-09-15|https://finviz.com/
Low-beta leadership inside sector|PARTIAL|0.5|2026-09-15|https://finviz.com/
Sector rotation into healthcare|PARTIAL|0.4|2026-09-15|https://finviz.com/
FDA rejection / CRL / trial failure (breadth)|PARTIAL|0.4|2026-09-15|https://www.astrazeneca.com/
FDA approval / favorable panel (sector breadth)|PARTIAL|0.3|2026-09-15|https://www.amgen.com/
Biotech risk-off / funding winter|MISS|0.3|2026-09-15|https://finviz.com/
Crowded long (extreme relative performance + valuation)|PARTIAL|0.4|2026-09-15|https://finviz.com/
Sector ETF inflow / relative volume spike|MISS|0.3|2026-09-15|https://finviz.com/
HIT_GRID_END

HORIZON_3D: down/flat — rate shock + oil spike pressure the duration sleeve; defensive relative cushion persists but the 1d rel edge has decayed.
HORIZON_1W: flat — FOMC next week is the dominant binary; XLV's relative leadership is intact but absolute direction hinges on the rate path.
HORIZON_2W: flat/mild — if the 10Y holds above 5%, multiple compression dominates and the defensive bid is only relative; a rate retreat would flip XLV to up/mild.
HORIZON_1M: flat — no fresh sector spine (MA rates stale, IRA residual, no XBI leadership); XLV trades as a rate-sensitive defensive proxy.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -0.5, 'S1_SECTOR_FACTORS': -0.5, 'S2_BREADTH': -0.5, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': -3.5, 'divergence_flagged': True, 'total_score': -4.136, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.565, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.0892, 'score': 0.535, 'legs': [{'leg': 'ES', 'pct': 0.34, 'w': 0.6}, {'leg': 'PM:XLV', 'pct': 0.04, 'w': 0.7}]}, 'overlay_score': -3.712, 'overlay_raw': -3.712, 'index_carry': -0.959, 'general_total': -3.836, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 1.25, 'S2_BREADTH': 1.25, 'S3_FLOWS_POSITIONING': 1.25, 'S4_ETF_TAPE': 1.25}, 'llm_confidence': 0.5, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
