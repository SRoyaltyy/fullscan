# Sector Prediction — Healthcare — 2026-09-24

- ETF: **XLV**
- rubric: `00_grounding/sectors/healthcare.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-4.286** (mult 0.8)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **-2.371** (ES -0.64%, PM:XLV -0.40%) · index_carry **-1.915** (general -7.659) · llm_overlay **0.0** (raw 0.0)

## Channel 1 sector ETF tape

```
ETF XLV vs SPY (yfinance, through 2026-09-23):
  1d: XLV -0.12% | SPY -0.74% | rel +0.61%
  3d: XLV +0.38% | SPY +0.93% | rel -0.56%
  1w: XLV +1.07% | SPY +1.63% | rel -0.56%
  1m: XLV -2.96% | SPY +0.52% | rel -3.48%
```

MEMORY_CONFIRM: Healthcare/XLV 2026-09-24. Memory index unavailable this run (embedding metadata missing). Rolling HC dir=0.3 mag=0.5 (n=10); last-30 dir=0.478 mag=0.304 (n=23). Last graded **09-23 up/mild vs actual −0.124% (dir MISS, mag MISS)** — the 09-23 reflect lesson is the binding one: **when leading_sum = 0 and divergence = false, the engine must emit flat/flat, not a signed direction off index_carry/tape_anchor.** Open experiment **applies** as prefer flat/mild + shrink confidence when factor sign fights tape. Calendar: **Thu cash session**; **no 8:30 CPI/NFP**; **FOMC+SEP+presser PRINTED 09-16** (paid, T+6) — not today's binary; **IRA cycle-3 final offers Sep 30**, not today. **09-23 empty-spine gate FIRES** (S0–S4 net ≈ 0, no divergence → flat/flat, suppress index_carry). **09-22 emit-cap FIRES** (paid FOMC + empty S1 + mixed tape → flat/flat, not leftover-PM down/mild and not ES-carry up). **09-18 complement FIRES as up-cap** (PM:XLV **−0.40% ≤ 0**, not already-mild). **09-17 keep-up does NOT fire** (PM not ~+0.4%). **09-16 force-flat does NOT fire** (FOMC paid). **09-21 beta-arithmetic does NOT fire** (ES=F **−0.64% / NQ=F −1.09%**, not ≥+1%). **09-11 funding-source does NOT fire at full weight** (needs confirming futures ≥+0.5%; today futures are RED). **08-13 reversal-tell does NOT fire** (Channel 1 3d/1w/1m rel all negative, not leftover leadership). **09-14 destination PARTIALLY fires** (NQ −1.09% leading ES −0.64% down by 45bp — near the ≥50bp duration-led threshold; XLV is a low-beta defensive that is under-owned after a deep 1m rel −3.48% lag → relative cushion, but the absolute is a red tape). **09-10 decay cap does NOT fire** (1d rel +0.61%, not |rel|≤0.15% stabilization). **08-17 oil FTS narrowed off** (WTI/Brent still ~$104/$108, offered off elevated levels). **08-14 policy audit: no same-morning mega-cap Rx headline** (MFN 50-state is 09-18 residual; IRA final offers Sep 30). **08-11 MA cut stale** (April 2027 +2.48%). **08-21 / AMGN dazodalibep Phase-3 is a fresh large-cap biotech readout — sector-relevant but single-large-cap; must not dominate XLV.** **08-28 leftover-stack: do not copy 3d/1w/1m rel into S2/S3/S4.** No oil double-count into rotation. No AVGO/XLK/ASML map into S0. Nested MAP HEAT OVERRIDE (Facilities) not averaged into XLV. Fear & Greed **58.2 is stale (2026-08-27)** — ignored. FedWatch not scrapable. size_gate=True.

# Healthcare / XLV — 2026-09-24

**Object:** near-session environment for **XLV** (not SPX, not a stock picker).

## Channel 1 (trusted, unaltered)

XLV vs SPY through **2026-09-23**: **1d −0.12% / −0.74% (rel +0.61%)**; 3d rel **−0.56%**; 1w rel **−0.56%**; 1m rel **−3.48%**. Wednesday's absolute was a **mild lag** on a red SPY tape — the relative print (+0.61%) is a **defensive cushion**, not leadership. 3d/1w are modestly negative; 1m is a **deep relative lag** — crowded-long fuel is gone (the opposite of early-September extension). 08-13 leftover-RS leadership is **absent**.

Macro: VIX **16.44** (+1.26 1d, −1.27 1w); **VIX/VIX3M 0.908 contango** (no backwardation stress, but the ratio is creeping up). **Finviz futures: ES +0.20%, NQ +0.41%, RTY +0.08%, DJIA +0.11%** — modest green, NQ leading. Separate **`[ES=F −0.64%] / [NQ=F −1.09%]`** is the same conflict class as 09-15…09-23; **both panels disagree on sign today** — Finviz modest green vs ES=F/NQ=F red. **WTI $104.16 −1.59% / Brent $107.67 −1.02% / CL=F +1.86% / BZ=F +2.40%** — oil **mixed/offered off elevated levels**. DFII10 **2.63** (as-of 09-22: +0.01 1d, +0.01 1w, **+0.23 1m**) — real yields **sticky/up on the month**, not a same-session print. DGS10 **4.96**, DGS30 **5.29**. DXY **+0.13%** 1d, **+2.25% 1m** — USD firming. HY OAS **2.68**. Asia **−0.09%** (split: Nikkei +0.76%, Kospi +1.04%, Shanghai −1.22%), Europe **−0.40%**. **PM: XLV −0.40% vs XLK −1.51%, XLE +1.11%, XLP +0.41%, XLU +0.10%** — healthcare is **offered with tech**, while staples/utilities hold the defensive sleeve. **FOMC is paid (09-16).** **size_gate=True.** 5-day 10Y–SPX corr **−0.826** (strongly negative — yields driving equities).

## Channel 2

**1. Shared macro → this sector (S0).** Pre-open tape is **mixed-to-negative, not a regime day**. The dominant macro driver (News Judge #1/#2) is **Warsh signaling rate HIKES may be needed** — a hawkish regime shift that repriced the curve and left Wall Street lower. That is a **direct duration headwind for the XBI sleeve** and a multiple-compression force on all equities. But the *live* futures panel is split: Finviz modest green (NQ≥ES) vs ES=F **−0.64% / NQ=F −1.09%** red. The 09-14 destination rule **partially fires**: NQ is leading ES down by ~45bp (near the ≥50bp duration-led threshold), and XLV is a low-beta defensive that is **under-owned** after a deep 1m rel −3.48% lag — that configuration gives XLV a **relative cushion**. But the absolute is a red tape with a hawkish rate overhang, and PM:XLV **−0.40%** is offered with tech, not bid as a haven (staples/utilities hold that sleeve). Oil-offered is **not** a duration tailwind for XBI (09-11 inversion still binds) and **not** 08-17 FTS (oil still elevated). Real-yield sticky (+0.23 1m DFII10) is a second-order XBI-sleeve drag — **do not restack** into S0 as a fresh shock (09-10 same-shock). Live PM red **forbids a fat absolute-up** (09-18: paid FOMC + PM ≤ 0 closes 09-17's keep-up) and **forbids converting the same flat-to-red print into down/mild** (09-22: 09-18 is an emit-cap, **not** a down mandate). **S0 = −0.3** — small hawkish-rate/duration lean, **not** a large negative implying a notable absolute smash. Do not score oil-falling as "rotation into healthcare."

**2. Spine / secondary (S1).**
- **CMS / MA 2027 +2.48%:** April finalization — **stale**. Sept Part D NAMBA / landscape / Star cutpoints is process. **Checked, nothing material** as a live MA-upside HIT.
- **Biotech / XBI:** **AMGN Phase-3 dazodalibep positive in systemic Sjögren's; Jefferies PT to $410** (News Judge #4, Finviz digest) is a **fresh large-cap biotech late-stage readout** — a genuine positive sector_fundamental signal for immunology/large-cap biotech sentiment. Per the 08-21 breadth rule, a single-large-cap readout is **not** a confirmed basket cluster (no partner/sub-complex co-move confirmed premarket), so it **must not dominate XLV**. It is a **mild positive tilt**, not a spine. No XBI leadership confirmed; MAP HEAT Biotechnology was **flat** (VRTX mixed, REGN neg) as of 09-22. Not a funding-winter cluster.
- **Drug pricing:** IRA cycle-3 final offers **Sep 30**, not today. MFN 50-state Medicaid / GENEROUS is **09-18 residual**. **No same-morning mega-cap Rx headline.** 08-14 does **not** fire.
- **FDA / trials:** Summer CMC CRL cluster (Unicycive / Elevar / Achieve / etc.) is **small-cap / paid**, not an XLV basket. **ARGX Forte close** is paid. **BSX Citi downgrade to Neutral, PT cut to $50** (pulsed field ablation / LAAC concerns) is **single-name medtech**. **CI Jefferies downgrade to Hold, PT cut to $307** (EviCore sale / Centene PDP shrinkage) is **single-name managed care**. **CAH CEO stock sale** is single-name. **Must not dominate XLV.**
- **Utilization:** CI downgrade cites Centene PDP shrinkage / consensus-too-high — **single-name**, not a same-morning insurer smash. MAP HEAT Plans was nested.
- **Rotation:** 3d/1w/1m **out** of healthcare is **already paid** (08-28). Today's live tell is **rotation risk out** on a hawkish-rate tape — that lives in **S0**, not a second S1 hit.

Net **S1 = 0** (AMGN readout is a mild single-large-cap positive, offset by the hawkish-rate duration drag on the sleeve; no fresh MA, no XBI leadership, no same-morning Rx smash).

**3. Breadth (S2).** Sector-internal only (09-10). No confirmed premarket XLV mega-cap breakdown; the AMGN readout is a **positive** single-name, not a breadth failure. PM:XLV **−0.40%** is offered with tech but staples/utilities hold the defensive sleeve — this is **rotation within defensives**, not a healthcare-internal breadth failure. Do not copy 3d/1w/1m rel into S2. **S2 = 0**.

**4. Flows (S3).** XLV 1m rel is **−3.48%** — the crowded-long extension is fully unwound (no longer an accelerant, no fresh inflow bid). Trailing flows not a 1-day lid (08-28). **S3 = 0**.

**5. Tape (S4, confirmation only).** Channel 1 **1d rel +0.61%** is a **defensive cushion** on a red SPY tape — S4 confirms **relative** resilience, not absolute direction. 3d/1w rel are modestly negative; 1m is a deep lag. The 1d rel is not a |rel|≤0.15% stabilization (09-10 decay cap off). **S4 = 0** — the relative cushion is real but does not license an absolute up call on a red, hawkish-rate tape.

## Divergence / reconciliation

Leading factor sum (S0 −0.3 + S1 0 + S2 0 + S3 0 + S4 0) ≈ **−0.3** — net-non-positive, near zero. The engine's `index_carry`/`tape_anchor` legs are **not** a healthcare factor and must not mint a signed direction off a near-zero card (09-23 empty-spine gate; 09-22 emit-cap). Live PM:XLV **−0.40%** is red, so the 09-18 up-cap binds and the 09-17 keep-up does not fire. The correct official call is **flat/flat** — the card is near-zero, the tape is mixed, and the only live sector catalyst (AMGN) is a single-large-cap positive that must not dominate. **Divergence flagged: True** (engine carry vs near-zero factor card).

## Self-audit

- **Lens:** XLV near-session environment, not SPX, not a stock picker. ✔
- **Band:** flat — |score| ≈ 0.3, size_gate applied, PM red forbids up/mild, 09-22 forbids down/mild. ✔
- **Skew:** symmetric — no fat up (PM red, hawkish rate overhang) and no fat down (defensive relative cushion, AMGN positive, under-owned). ✔
- **Same-shock double-count:** hawkish-rate/duration drag scored once in S0; not restacked into S1 (XBI sleeve) or S2. ✔
- **Single-ticker:** AMGN dazodalibep, BSX Citi, CI Jefferies, CAH sale all capped — none drives the XLV call. ✔

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -0.3
S1_SECTOR_FACTORS: 0.0
S2_BREADTH: 0.0
S3_FLOWS_POSITIONING: 0.0
S4_ETF_TAPE: 0.0
MULTIPLIER: 0.8
CONFIDENCE: 0.4
REGIME: mixed
DIVERGENCE_FLAGGED: True
SELF_AUDIT: lens=XLV near-session; band=flat (|score|~0.3, size_gate, PM red forbids up/mild, 09-22 forbids down/mild); skew=symmetric; same_shock=hawkish-rate drag scored once in S0 only; single_ticker=AMGN/BSX/CI/CAH capped, none drives XLV
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|MISS|0.35|2026-09-24|
Risk-off tape / flight to safety|PARTIAL|0.45|2026-09-24|
Real yields rising|PARTIAL|0.5|2026-09-24|
Real yields falling|MISS|0.4|2026-09-24|
USD strengthening|PARTIAL|0.4|2026-09-24|
USD weakening|MISS|0.35|2026-09-24|
Sector breadth expansion (% names up)|MISS|0.4|2026-09-24|
Sector breadth failure (ETF up, names flat)|MISS|0.4|2026-09-24|
Large-cap leadership inside sector|PARTIAL|0.4|2026-09-24|
Small/mid leadership inside sector|MISS|0.4|2026-09-24|
High-beta leadership inside sector|MISS|0.4|2026-09-24|
Low-beta leadership inside sector|PARTIAL|0.45|2026-09-24|
Sector ETF inflow / relative volume spike|MISS|0.35|2026-09-24|
Sector ETF outflow / volume dry-up|MISS|0.35|2026-09-24|
Crowded long (extreme relative performance + valuation)|MISS|0.5|2026-09-24|
Index rebalance / inclusion tailwind|MISS|0.3|2026-09-24|
Index exclusion / forced selling|MISS|0.3|2026-09-24|
FDA approval / favorable panel (sector breadth)|MISS|0.4|2026-09-24|
Positive late-stage trial readout (breadth)|PARTIAL|0.5|2026-09-24|https://www.finviz.com/
CMS / Medicare Advantage rate upside|MISS|0.5|2026-09-24|
Biotech risk-on / XBI leadership|PARTIAL|0.4|2026-09-24|
Drug pricing policy relief|MISS|0.45|2026-09-24|
FDA rejection / CRL / trial failure (breadth)|MISS|0.45|2026-09-24|
Medicare rate cut / reimbursement pressure|MISS|0.45|2026-09-24|
Drug pricing crackdown / IRA expansion risk|MISS|0.4|2026-09-24|
Biotech risk-off / funding winter|MISS|0.45|2026-09-24|
Utilization spike hurting insurers|MISS|0.4|2026-09-24|
Sector rotation into healthcare|MISS|0.4|2026-09-24|
Sector rotation out of healthcare|PARTIAL|0.4|2026-09-24|
HORIZON_3D|flat|0.4|2026-09-24|
HORIZON_1W|flat|0.35|2026-09-24|
HORIZON_2W|flat|0.3|2026-09-24|
HORIZON_1M|down_mild|0.35|2026-09-24|
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -0.3, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.8, 'leading_sum': -0.6, 'divergence_flagged': False, 'total_score': -4.286, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.671, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': -0.3952, 'score': -2.371, 'legs': [{'leg': 'ES', 'pct': -0.64, 'w': 0.6}, {'leg': 'PM:XLV', 'pct': -0.4, 'w': 0.7}]}, 'overlay_score': 0.0, 'overlay_raw': 0.0, 'index_carry': -1.915, 'general_total': -7.659, 'skill_multipliers': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 1.25, 'S2_BREADTH': 1.25, 'S3_FLOWS_POSITIONING': 1.25, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.4}
```
