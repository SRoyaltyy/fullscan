# Sector Prediction — Financial — 2026-09-15

- ETF: **XLF**
- rubric: `00_grounding/sectors/financial.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-3.429** (mult 0.9)
- regime: risk_off
- divergence_flagged: **True**
- engine: v2 · tape_anchor **0.593** (ES +0.30%, ZN -0.46%, PM:XLF -0.08%) · index_carry **-1.097** (general -4.389) · llm_overlay **-2.925** (raw -2.925)

## Channel 1 sector ETF tape

```
ETF XLF vs SPY (yfinance, through 2026-09-15):
  1d: XLF -0.87% | SPY -0.51% | rel -0.36%
  3d: XLF -0.59% | SPY -0.11% | rel -0.48%
  1w: XLF -1.34% | SPY -1.17% | rel -0.17%
  1m: XLF -2.79% | SPY -2.49% | rel -0.30%
```

MEMORY_CONFIRM: Financial scoreboard used. Rolling accuracy last 10: dir=0.4 mag=0.5 (n=10); last 30: dir=0.421 mag=0.316 (n=19). Last graded: 09-08 down/flat vs XLF −1.38%/rel −0.83% (dir HIT, mag MISS); 09-09 down/mild vs −0.42% (HIT/HIT); 09-10 down/mild vs −0.33%/rel +0.27% (HIT/HIT); 09-11 flat/flat vs +0.67%/rel −0.18% (dir MISS, mag MISS); 09-14 down/mild vs −0.384%/rel +0.062% (HIT/HIT). Binding lessons applied: (1) **09-10 Financial** — with 1d rel flat (|rel| < ~0.15%), credit tight, futures mixed, do NOT score S1 as an independent negative on the macro narrative alone; the sector-specific transmission must be confirmed by the sector's own relative tape. (2) **09-11 Financial** — on a pending high-impact binary, do not score S4 on a sub-gate stale rel print and then resolve the manufactured divergence toward the benign branch. (3) **09-14 Financial (validated, promoted to standing rule)** — a sub-gate premarket relative bid vs red cyclicals is a downside *cap*, not an absolute-up license; resolve leading-vs-tape divergence toward the live macro overlay, cap magnitude at flat/mild. (4) **09-08/09-09** — oil >$100 + long-end stress → S0=−2, S1=−0.5, no "value shield" (absolute-direction lesson, not a relative-underperformance stack). (5) **08-28** — do not triple-count a completed lag into S2/S3/S4. (6) **08-17** — long-end steepener ≠ NIM+. (7) **08-18** — two-sided long-end rotation fires only when 1d rel ≥ +0.4% live at open (today −0.36%, off). (8) **08-21** — one band / mag temper (rolling mag 0.5). (9) **08-11** — geo/oil live + flat S4 → no absolute up, mult ≤1.0. Open experiment (sector_financial): prefer flat/mild when sign fights tape. Today's key change: **oil re-spiking (WTI $103.79 +2.37%, Brent $108.11 +2.31%), futures RED (ES −0.54%, NQ −0.62%, RTY −0.73%, DJIA −0.71%), long end selling off hard (30Y bond futures −0.93%, Ultra Bond −1.09%), and XLF premarket −0.08%** — the 09-14 configuration is intact and intensifying, with a fresh sector-specific negative (BAC CEO soft Q3 outlook, −5%).

---

## XLF — 2026-09-15 (near-session)

Object is the **XLF absolute** environment, not SPX and not a stock picker. Channel 1 numbers are taken as given.

### Channel 1 (trusted, not re-derived)

- **XLF vs SPY (through 09-15):** 1d −0.87% / rel **−0.36%**; 3d rel **−0.48%**; 1w rel **−0.17%**; 1m rel **−0.30%**. 1d relative is **modestly negative** (below the 08-18 ≥ +0.4% gate, and now on the *negative* side of the 09-10 sub-gate band); 3d also red; 1w/1m mildly red.
- Macro: **VIX 17.75 (+0.65 1d, +2.03 1w)**; VIX/VIX3M **0.901** (contango — not panic). **DGS30 5.35 / DGS10 4.96** (stress-zone long end); DFII10 2.60 (+0.05 1d, +0.18 1w) — **real yields rising**. **HY OAS 2.71** (1d +0.06, 1w +0.03, 1m +0.04) — **tight but creeping wider**. **ES −0.54%, NQ −0.62%, RTY −0.73%, DJIA −0.71%** — broad red futures. **WTI $103.79 (+2.37%), Brent $108.11 (+2.31%)** — oil re-spiking above $100 for a second session. **XLF premarket −0.08%** (vs XLI +0.81%, XLB +0.38%, XLU +0.20%, XLE +0.14%, XLK +0.11%, XLV +0.04%, XLY −0.13%, XLRE −0.24%, XLP −0.33%, XLC −0.63%) — XLF is **not** the green cyclical today; the value-rotation tell of 09-14 has faded. **USEPUINDXD 215.48 (−202.54 1d)** — policy-uncertainty spike unwinding. **5-day corr 10Y vs SPX −0.172** (weak). Asia composite **−1.1%** (Kospi −3.26%, Hang Seng −1.0%); Europe **−0.39%**. DXY +0.25%.

### Channel 2

**1. Shared macro → this sector (curve & credit > equity beta)**

The regime is a **broad risk-off with an oil re-spike and a long-end selloff**. Unlike 09-14 (tech-led, NQ −1.59% vs ES −0.66%, XLF green premarket), today the futures are **uniformly red** (ES −0.54%, NQ −0.62%, RTY −0.73%, DJIA −0.71%) — no tech-specific rotation shape, and **XLF premarket is −0.08%**, i.e. the value-rotation bid that cushioned 09-14 is **absent**. The long end is selling off hard (30Y bond futures −0.93%, Ultra Bond −1.09%, 10Y note −0.46%) with **real yields rising** (DFII10 +0.05 1d, +0.18 1w) — per 08-17 that is a **headwind, not NIM+**. Oil is re-spiking (WTI +2.37%, Brent +2.31%) — the 09-08/09-09 stagflation-shock channel is live again. Credit is **tight but creeping wider** (HY 2.71, +0.06 1d) — that tempers but does not eliminate. **No 8:30 high-impact US print today**; the Fed meeting is next week. Net S0: **negative** — red futures + oil re-spike + long-end selloff + real-yield rise, with no offsetting value-rotation bid.

**2. Spine (mandatory)**

| Spine | Read |
|---|---|
| 2s10s steepening | **Not NIM+.** 30Y 5.35% / 10Y 4.96% = 08-17 **bear / long-end** steepener, long end backing up hard today (30Y futures −0.93%). Counted in S0 context only, not S1+. |
| Credit spreads | **Tight but creeping wider** (HY 2.71, +0.06 1d, +0.03 1w, +0.04 1m) — a mild negative, not a blowout. |
| NII/NIM | FDIC Q2 NIM 3.32% — **carried**, not a same-morning print. |
| Credit quality | Q2 card/CRE DQ mixed-to-stable. **Not a spike.** |
| CRE / funding | CRE overhang carried (regionals). No deposit-flight headline. |

**3. Secondary — this is where today differs from 09-14**

The Finviz digest carries a **fresh, sector-specific negative**: **BAC CEO's soft Q3 outlook at the Barclays conference drove a 5% plunge**, and **BNY raised expense-growth guidance to 6–7%**. This is the first hard read on Q3 NII/credit into the quarter, and it is **soft**. Per the 09-10 lesson, a sector-specific negative earns an S1 score **only if the sector's own relative tape confirms it** — and today it does: XLF 1d rel **−0.36%** (negative, not flat), 3d rel **−0.48%** (negative), and XLF premarket **−0.08%** while XLI/XLB/XLU/XLE/XLK are all green. That is a **confirmed sector-specific underperformance channel**, not a phantom double-count of S0. So S1 earns a modest negative.

MAP HEAT (nested, beats parent ETF): **Asset Management dir=down** (BX neg, BCRED redemption caps), **Capital Markets dir=down** (MS neg — crash warning; GS neg — hawkish Fed revision), **Credit Services dir=down** (V/MA red, weekly residual −2.12%), **Financial Data & Exchanges dir=down** (SPGI/CME pos but red daily tape), **P&C Insurance dir=down** (breadth 0.045, both captains red). Only **Banks-Regional dir=up** (low conv, breadth 0.074, catalyst-free) and **Insurance-Life dir=up** (low conv) are constructive. Net: **broadly soft breadth inside financials**, with the two "up" pockets explicitly low-conviction and catalyst-free. This is not ETF-only carry — it is a genuine internal deterioration.

**4. Breadth / leadership**

1d rel **−0.36%** (negative), 3d rel **−0.48%** (negative), 1w rel **−0.17%** (mildly negative), 1m rel **−0.30%** (mildly negative). All four horizons are now negative — the first time since 09-08. The 1d tape is the freshest signal and it is **negative**, consistent with the 09-10 lesson's requirement that the sector-specific channel be confirmed by the sector's own relative tape. No live premarket BKX/XLF breakdown *crash*, but a confirmed modest underperformance.

**5. Flows / positioning**

XLF trailing outflows; the broader tape shows **CTA selling and continued US equity ETF outflows** (Wells Fargo, 09-11). Not a crowded long (1m rel −0.30%). No fresh inflow spike. S3 = 0 — trailing flows are not a 1-day lid (08-28).

**6. Catalysts**

**No 8:30 high-impact US print today.** The Fed meeting is next week (news judge: FOMC/SEP/Warsh is an unresolved binary — do not pre-score). The live catalysts are: (a) the **10Y breaching 5%** / global bond selloff, (b) the **oil re-spike** (Brent $108), (c) the **BAC CEO soft Q3 outlook** (sector-specific), and (d) **chipmaker weakness / AMD −5%** (an XLK object — do not map into XLF S0).

### Lessons applied (not restacked)

- **09-10 Financial:** S1 negative is **confirmed** today by the sector's own relative tape (1d rel −0.36%, 3d rel −0.48%) — this is the exact condition the lesson requires. Not a phantom double-count.
- **09-11 Financial:** no pending same-day binary (FOMC is next week, not today); S4 is scored on a **negative** 1d rel, not a sub-gate positive one. No manufactured divergence toward a benign branch.
- **09-14 Financial (standing rule):** the sub-gate premarket tell is a **cap**, not a license. Today XLF premarket is −0.08% (not green), so there is no relative bid to cap against — the cap simply binds at mild.
- **09-08/09-09:** oil >$100 + long-end stress → S0 negative, no "value shield." Applied, but **not** escalated to −2 because credit is tight (HY 2.71) and futures are red-but-not-crashing.
- **08-28:** do not triple-count the 3d/1w lag into S2/S3/S4. S2 modest, S3 = 0.
- **08-17:** long-end steepener scored as a **headwind in S0**, not NIM+.
- **08-18:** **off** (1d rel −0.36%, not ≥ +0.4%).
- **08-11:** geo/oil live; S4 negative → no absolute up, mult ≤ 1.0.
- **08-21 mag:** one band; rolling mag 0.5 → **mild**, not notable.

### Self-audit

Lens = XLF, not SPX. Band = **mild** (mag record, tight credit tempering, no crash tape). Oil/yields counted **once** in S0; the S1 negative is a **distinct, tape-confirmed** sector-specific channel (BAC Q3 outlook + broad internal breadth deterioration), not a re-score of S0. MAP HEAT nested overrides noted but not allowed to flip the macro-driven down call. BAC/BNY/MS/GS must not drive the ETF alone — but they are corroborated by the ETF's own negative relative tape, which is the required confirmation. Leading sum (S0–S3) and S4 are **same sign** (soft down) → no divergence. 09-14's hit (down/mild) and the 09-08 magnitude miss (flat vs notable) both argue for **mild**, not flat and not notable.

---

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -0.5
S2_BREADTH: -0.5
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -0.5
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: risk_off
HORIZON_3D: down:mild:0.52
HORIZON_1W: down:mild:0.48
HORIZON_2W: flat:mild:0.42
HORIZON_1M: flat:mild:0.40
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-off tape / flight to safety|HIT|0.80|2026-09-15|Channel 1 ES -0.54% NQ -0.62% RTY -0.73% DJIA -0.71%
Real yields rising|HIT|0.75|2026-09-15|DFII10 2.60 +0.05 1d +0.18 1w
Yield curve steepening (NIM tailwind)|MISS|0.70|2026-09-15|30Y 5.35/10Y 4.96 bear steepener, 30Y futures -0.93% — headwind not NIM+
Credit spreads tightening|MISS|0.65|2026-09-15|HY OAS 2.71 +0.06 1d +0.03 1w — creeping wider
Credit spreads blowing out|MISS|0.70|2026-09-15|HY 2.71 still tight, no blowout
Bank NII / NIM beat|MISS|0.70|2026-09-15|BAC CEO soft Q3 outlook at Barclays, -5%; BNY expense guidance raised to 6-7%
Sector rotation out of financials|HIT|0.65|2026-09-15|XLF 1d rel -0.36%, 3d rel -0.48%, all four horizons negative
Sector breadth failure (ETF up, names flat)|MISS|0.60|2026-09-15|XLF premarket -0.08% while XLI +0.81% XLB +0.38% XLU +0.20% XLE +0.14% XLK +0.11%
Large-cap leadership inside sector|MISS|0.55|2026-09-15|MAP HEAT: money-center flat-to-lower, BAC -5%
Sector ETF outflow / volume dry-up|HIT|0.55|2026-09-15|CTA selling + continued US equity ETF outflows (Wells Fargo 09-11)
CRE concentration stress|NEUTRAL|0.50|2026-09-15|Carried overhang, no fresh headline
Deposit flight / funding stress|NEUTRAL|0.55|2026-09-15|No fresh headline; SOFR-IORB -0.03
Charge-off / delinquency spike|NEUTRAL|0.55|2026-09-15|Q2 card/CRE DQ mixed-to-stable, no spike
Capital markets / IB / trading surge|MISS|0.60|2026-09-15|MAP HEAT Capital Markets dir=down, MS neg GS neg
Regional bank stress easing|NEUTRAL|0.50|2026-09-15|MAP HEAT Banks-Regional dir=up but low conv, breadth 0.074
USD strengthening|NEUTRAL|0.50|2026-09-15|DXY +0.25% — mild, not a driver
Crowded long (extreme relative performance + valuation)|MISS|0.60|2026-09-15|1m rel -0.30%, not crowded
Index rebalance / inclusion tailwind|NEUTRAL|0.45|2026-09-15|SPGI rebalance flows noted, not XLF-specific
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -0.5, 'S2_BREADTH': -0.5, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -0.5}, 'multiplier': 0.9, 'leading_sum': -4.5, 'divergence_flagged': True, 'total_score': -3.429, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.537, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.0988, 'score': 0.593, 'legs': [{'leg': 'ES', 'pct': 0.3, 'w': 0.8}, {'leg': 'ZN', 'pct': -0.46, 'w': -0.6}, {'leg': 'PM:XLF', 'pct': -0.08, 'w': 0.7}]}, 'overlay_score': -2.925, 'overlay_raw': -2.925, 'index_carry': -1.097, 'general_total': -4.389, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 0.5, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 1.25}, 'llm_confidence': 0.55, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
