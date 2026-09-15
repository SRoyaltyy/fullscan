# Sector Prediction — Financial — 2026-09-15

- ETF: **XLF**
- rubric: `00_grounding/sectors/financial.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-4.063** (mult 0.9)
- regime: risk_off
- divergence_flagged: **True**
- engine: v2 · tape_anchor **0.416** (ES +0.44%, ZN -0.46%, PM:XLF -0.17%) · index_carry **-1.554** (general -6.215) · llm_overlay **-2.925** (raw -2.925)

## Channel 1 sector ETF tape

```
ETF XLF vs SPY (yfinance, through 2026-09-14):
  1d: XLF -0.38% | SPY -0.45% | rel +0.06%
  3d: XLF -0.05% | SPY -0.20% | rel +0.15%
  1w: XLF -1.84% | SPY -1.21% | rel -0.63%
  1m: XLF -2.11% | SPY -2.19% | rel +0.07%
```

MEMORY_CONFIRM: Financial scoreboard used. Rolling accuracy last 10: dir=0.4 mag=0.5 (n=10); last 30: dir=0.421 mag=0.316 (n=19). Last graded: 09-08 down/flat vs XLF −1.38%/rel −0.83% (dir HIT, mag MISS); 09-09 down/mild vs −0.42% (HIT/HIT); 09-10 down/mild vs −0.33%/rel +0.27% (HIT/HIT); 09-11 flat/flat vs +0.67%/rel −0.18% (dir MISS, mag MISS); 09-14 down/mild vs −0.384%/rel +0.062% (HIT/HIT). Binding lessons: (1) **09-10 Financial** — when 1d rel is flat (|rel| < ~0.15%), credit tight, futures mixed, do NOT score S1 as an independent negative on the macro narrative alone; the sector-specific transmission channel must be confirmed by the sector's own relative tape. (2) **09-11 Financial** — on a pending high-impact binary, do not score S4 on a sub-gate stale rel print and then resolve the manufactured divergence toward the benign branch; hold S0 at a small skew. (3) **09-14 Financial (validated, promoted to standing rule)** — sub-gate premarket relative bid vs red cyclicals is a downside *cap*, not an absolute-up license; resolve leading-vs-tape divergence toward the live macro overlay, cap magnitude at flat/mild. (4) **09-08/09-09** — oil >$100 + long-end stress → S0=−2, S1=−0.5, no "value shield" (absolute-direction lesson, not a relative-underperformance stack). (5) **08-28** — do not triple-count a completed lag into S2/S3/S4. (6) **08-17** — long-end steepener ≠ NIM+. (7) **08-18** — two-sided long-end rotation fires only when 1d rel ≥ +0.4% live at open (today +0.06%, off). (8) **08-21** — one band / mag temper (rolling mag 0.5). (9) **08-11** — geo/oil live + flat S4 → no absolute up, mult ≤1.0. Open experiment (sector_financial): prefer flat/mild when sign fights tape. Today's key change: **oil re-spiking (WTI $103.73 +2.24%, Brent $108.03 +2.21%), futures RED (ES −0.57%, NQ −0.66%), long end selling off hard (30Y bond futures −0.91%, Ultra Bond −1.06%), and XLF premarket −0.17%** — the 09-14 configuration is intact and intensifying, with a fresh sector-specific negative (BAC CEO soft Q3 outlook).

---

## XLF — 2026-09-15 (near-session)

Object is the **XLF absolute** environment, not SPX and not a stock picker. Channel 1 numbers are taken as given.

### Channel 1 (trusted, not re-derived)
- **XLF vs SPY (through 09-14):** 1d −0.38% / rel **+0.06%**; 3d rel **+0.15%**; 1w rel **−0.63%**; 1m rel **+0.07%**. 1d relative is **flat/sub-gate** (well below the 08-18 ≥ +0.4% gate); 3d flat; 1w still the lag; 1m flat.
- Macro: **VIX 17.82 (+0.72 1d, +2.1 1w)**; VIX/VIX3M **1.139 backwardation** (stress). **DGS30 5.35 / DGS10 4.96** (stress-zone long end); DFII10 2.60 (+0.05 1d, +0.18 1w) — **real yields rising**. **HY OAS 2.65** (1d −0.05, 1w −0.03, 1m −0.06) — **tight and tightening**, no blowout. **ES −0.57%, NQ −0.66%, RTY −0.77%, DJIA −0.78%** — broad red futures. **WTI $103.73 (+2.24%), Brent $108.03 (+2.21%)** — oil re-spiking above $100 for a second session. **XLF premarket −0.17%** (vs XLE +0.18%, XLB +0.03%, XLU +0.08%, XLK −0.65%) — XLF is *not* the green cyclical today; the value-rotation tell of 09-14 has faded. **USEPUINDXD 395.54 (+189.93 1d)** — economic-policy-uncertainty spike. **5-day corr 10Y vs SPX −0.107** (weak). Asia composite **−0.66%**; Europe **−0.87%**. DXY +0.27%.

### Channel 2

**1. Shared macro → this sector (curve & credit > equity beta)**
The regime is a **broad risk-off with an oil re-spike and a long-end selloff**. Unlike 09-14 (tech-led, NQ −1.59% vs ES −0.66%, XLF green premarket), today the futures are **uniformly red** (ES −0.57%, NQ −0.66%, RTY −0.77%, DJIA −0.78%) — no tech-specific rotation shape, and **XLF premarket is −0.17%**, i.e. the value-rotation bid that cushioned 09-14 is **absent**. The long end is selling off hard (30Y bond futures −0.91%, Ultra Bond −1.06%, 10Y note −0.46%) with **real yields rising** (DFII10 +0.05 1d, +0.18 1w) — per 08-17 that is a **headwind, not NIM+**. Oil is re-spiking (WTI +2.24%, Brent +2.21%) — the 09-08/09-09 stagflation-shock channel is live again. Credit is **tight and tightening** (HY 2.65, −0.05 1d) — that tempers but does not eliminate. **No 8:30 high-impact US print today**; the Fed meeting is next week. Net S0: **negative** — red futures + oil re-spike + long-end selloff + real-yield rise, with no offsetting value-rotation bid.

**2. Spine (mandatory)**
| Spine | Read |
|---|---|
| 2s10s steepening | **Not NIM+.** 30Y 5.35% / 10Y 4.96% = 08-17 **bear / long-end** steepener, long end backing up hard today (30Y futures −0.91%). Counted in S0 context only, not S1+. |
| Credit spreads | **Tight and tightening** (HY 2.65, −0.05 1d, −0.03 1w, −0.06 1m) — a genuine positive, but not a same-day catalyst. |
| NII/NIM | FDIC Q2 NIM 3.32% — **carried**, not a same-morning print. |
| Credit quality | Q2 card/CRE DQ mixed-to-stable. **Not a spike.** |
| CRE / funding | CRE overhang carried (regionals). No deposit-flight headline. |

**3. Secondary — this is where today differs from 09-14**
The Finviz digest carries a **fresh, sector-specific negative**: **BAC CEO's soft Q3 outlook at the Barclays conference drove a 5% plunge**, and **BNY raised expense-growth guidance to 6–7%**. This is the first hard read on Q3 NII/credit into the quarter, and it is **soft**. Per the 09-10 lesson, an S1 negative requires the sector's own tape to confirm the transmission — and here it does: **XLF premarket −0.17%** while XLE/XLB/XLU are green, i.e. XLF is *underperforming* the cyclical complex on a day when it should be a value beneficiary. That is the sector-specific confirmation the 09-10 lesson demands. The positive offsets are **foreign/Canadian banks** (BBVA record Q2 + €2B buyback, BCS strong H1 + £1bn buyback, BNS record Q3 EPS) — **not XLF money-center drivers**; do not map into S1. **AJG +5.4%** on a GS AI-productivity call is a single insurance broker, not the ETF. **AON USI merger filing** is M&A noise. IB/trading "fee boom" is stale Q2.

**4. Breadth / leadership**
1d rel **+0.06%** (flat/sub-gate), 3d rel **+0.15%** (flat), 1w rel **−0.63%** (the lag), 1m rel **+0.07%** (flat). The freshest tape is flat, and the premarket is **negative** — no broad participation bid. The BAC guidance item is a **large-cap leadership negative** (BAC is a top XLF holding), which is the opposite of the "large-cap leadership inside sector" positive.

**5. Flows / positioning**
XLF trailing outflows. Not a crowded long (1m rel +0.07%). No fresh inflow spike. No index-rebalance item.

**6. Catalysts**
No 8:30 high-impact US print today. **BAC CEO soft Q3 outlook** is the live sector-specific catalyst. **BNY expense guidance raise** is a secondary negative. The oil re-spike + long-end selloff + real-yield rise is the macro wrapper. Fed meeting next week.

### Lessons applied (not restacked)
- **09-10 Financial:** S1 negative is justified **only** because the sector's own tape confirms — XLF premarket −0.17% while peer cyclicals are green. This is the confirmation the lesson requires; it is not a narrative-only S1.
- **09-11 Financial:** S4 is **not** scored on the sub-gate 1d rel (+0.06%); it is scored on the **live premarket −0.17%**, which is a same-day signal, not a stale print. No manufactured divergence.
- **09-14 Financial (standing rule):** the sub-gate premarket relative bid is a downside *cap*, not an up license — and today there is **no** relative bid at all, so the cap is moot and the macro overlay governs.
- **09-08/09-09:** oil >$100 + long-end stress → S0 negative, no "value shield." Credit tight tempers magnitude.
- **08-28:** do not triple-count the 1w lag into S2/S3/S4. S2 modest, S3=0.
- **08-17:** long-end steepener scored as a **headwind in S0**, not NIM+.
- **08-18:** **off** (1d rel +0.06%, not ≥ +0.4%).
- **08-11:** geo/oil live; S4 negative → no absolute up, mult ≤1.0.
- **08-21 mag:** one band; rolling mag 0.5 → **mild**, not notable.

### Self-audit
Lens = XLF, not SPX. Band = **mild** (mag record 0.5, tight credit tempering, futures red but not a crash tape). Oil/yields counted **once** in S0; the S1 negative rests on the **BAC guidance + XLF premarket underperformance**, not on the macro narrative (09-10 compliant). BBVA/BCS/BNS/AJG/AON must not drive the ETF. Leading sum (S0–S3) and S4 are same sign (soft down) → **no divergence**. 08-21 green-futures ban-on-down is **off** (ES −0.57%). The 09-14 standing rule is satisfied: no sub-gate rel was used to manufacture a positive.

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
Risk-off tape / flight to safety|HIT|0.80|2026-09-15|Channel 1 ES -0.57% NQ -0.66% RTY -0.77% DJIA -0.78%
Real yields rising|HIT|0.75|2026-09-15|Channel 1 DFII10 2.60 +0.05 1d +0.18 1w
Yield curve steepening (NIM tailwind)|MISS|0.70|2026-09-15|30Y 5.35/10Y 4.96 bear-long-end steepener; 30Y futures -0.91% — headwind not NIM+
Credit spreads tightening|HIT|0.65|2026-09-15|Channel 1 HY OAS 2.65 -0.05 1d -0.03 1w -0.06 1m
Bank NII / NIM beat|MISS|0.70|2026-09-15|Finviz: BAC CEO soft Q3 outlook drove 5% plunge; BNY raised expense guidance to 6-7%
Credit quality stable or improving|NEUTRAL|0.50|2026-09-15|Q2 card/CRE DQ mixed-to-stable, no fresh spike
Capital markets / IB / trading surge|MISS|0.60|2026-09-15|IB fee boom stale Q2; no fresh money-center capital-markets catalyst
Sector rotation out of financials|HIT|0.60|2026-09-15|XLF premarket -0.17% vs XLE +0.18% XLB +0.03% XLU +0.08%
Sector breadth failure (ETF up, names flat)|MISS|0.55|2026-09-15|XLF premarket negative, not ETF-up/names-flat
Large-cap leadership inside sector|MISS|0.65|2026-09-15|BAC (top holding) -5% on soft Q3 guidance — large-cap leadership negative
Sector ETF outflow / volume dry-up|NEUTRAL|0.45|2026-09-15|Trailing XLF outflows, no fresh same-day flow print
Crowded long (extreme relative performance + valuation)|MISS|0.60|2026-09-15|1m rel +0.07% — not crowded
CRE concentration stress|NEUTRAL|0.45|2026-09-15|CRE overhang carried, no fresh headline
Deposit flight / funding stress|MISS|0.60|2026-09-15|No deposit-flight headline; SOFR-IORB -0.03 benign
Charge-off / delinquency spike|MISS|0.60|2026-09-15|No fresh delinquency spike
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -0.5, 'S2_BREADTH': -0.5, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -0.5}, 'multiplier': 0.9, 'leading_sum': -4.5, 'divergence_flagged': True, 'total_score': -4.063, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.563, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.0694, 'score': 0.416, 'legs': [{'leg': 'ES', 'pct': 0.44, 'w': 0.8}, {'leg': 'ZN', 'pct': -0.46, 'w': -0.6}, {'leg': 'PM:XLF', 'pct': -0.17, 'w': 0.7}]}, 'overlay_score': -2.925, 'overlay_raw': -2.925, 'index_carry': -1.554, 'general_total': -6.215, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 0.5, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 1.25}, 'llm_confidence': 0.55}
```
