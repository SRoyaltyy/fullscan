# Sector Prediction — Financial — 2026-09-15

- ETF: **XLF**
- rubric: `00_grounding/sectors/financial.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-3.834** (mult 0.9)
- regime: risk_off
- divergence_flagged: **True**
- engine: v2 · tape_anchor **0.679** (ES +0.36%, ZN -0.46%, PM:XLF -0.08%) · index_carry **-0.912** (general -3.65) · llm_overlay **-3.6** (raw -3.6)

## Channel 1 sector ETF tape

```
ETF XLF vs SPY (yfinance, through 2026-09-15):
  1d: XLF -0.73% | SPY -0.51% | rel -0.22%
  3d: XLF -0.45% | SPY -0.11% | rel -0.34%
  1w: XLF -1.20% | SPY -1.17% | rel -0.02%
  1m: XLF -2.66% | SPY -2.49% | rel -0.16%
```

MEMORY_CONFIRM: Financial scoreboard used. Rolling accuracy last 10: dir=0.4 mag=0.5 (n=10); last 30: dir=0.421 mag=0.316 (n=19). Last graded: 09-08 down/flat vs XLF −1.38%/rel −0.83% (dir HIT, mag MISS); 09-09 down/mild vs −0.42% (HIT/HIT); 09-10 down/mild vs −0.33%/rel +0.27% (HIT/HIT); 09-11 flat/flat vs +0.67%/rel −0.18% (dir MISS, mag MISS); 09-14 down/mild vs −0.384%/rel +0.062% (HIT/HIT). Binding lessons applied: (1) **09-10 Financial** — with 1d rel flat (|rel| < ~0.15%), credit tight, futures mixed, do NOT score S1 as an independent negative on the macro narrative alone; the sector-specific transmission must be confirmed by the sector's own relative tape. (2) **09-11 Financial** — on a pending high-impact binary, do not score S4 on a sub-gate stale rel print and then resolve the manufactured divergence toward the benign branch. (3) **09-14 Financial (validated, promoted to standing rule)** — a sub-gate premarket relative bid vs red cyclicals is a downside *cap*, not an absolute-up license; resolve leading-vs-tape divergence toward the live macro overlay, cap magnitude at flat/mild. (4) **09-08/09-09** — oil >$100 + long-end stress → S0=−2, S1=−0.5, no "value shield" (absolute-direction lesson, not a relative-underperformance stack). (5) **08-28** — do not triple-count a completed lag into S2/S3/S4. (6) **08-17** — long-end steepener ≠ NIM+. (7) **08-18** — two-sided long-end rotation fires only when 1d rel ≥ +0.4% live at open (today −0.22%, off). (8) **08-21** — one band / mag temper (rolling mag 0.5). (9) **08-11** — geo/oil live + flat S4 → no absolute up, mult ≤1.0. Open experiment (sector_financial): prefer flat/mild when sign fights tape. Today's key change: **oil re-spiking (WTI $103.79 +2.37%, Brent $108.11 +2.31%), futures RED (ES −0.54%, NQ −0.62%, RTY −0.73%, DJIA −0.71%), long end selling off hard (30Y bond futures −0.93%, Ultra Bond −1.09%), and XLF premarket −0.08%** — the 09-14 configuration is intact and intensifying, with a fresh sector-specific negative (BAC CEO soft Q3 outlook, −5%).

---

## XLF — 2026-09-15 (near-session)

Object is the **XLF absolute** environment, not SPX and not a stock picker. Channel 1 numbers are taken as given.

### Channel 1 (trusted, not re-derived)

- **XLF vs SPY (through 09-15):** 1d −0.73% / rel **−0.22%**; 3d rel **−0.34%**; 1w rel **−0.02%**; 1m rel **−0.16%**. 1d relative is **modestly negative** (below the 08-18 ≥ +0.4% gate, and on the *negative* side of the 09-10 sub-gate band); 3d also red; 1w/1m mildly red.
- Macro: **VIX 17.55 (+0.45 1d, +1.83 1w)**; VIX/VIX3M **0.898** (contango — not panic). **DGS30 5.35 / DGS10 4.96** (stress-zone long end); DFII10 2.60 (+0.05 1d, +0.18 1w) — **real yields rising**. **HY OAS 2.71** (1d +0.06, 1w +0.03, 1m +0.04) — **tight but creeping wider**. **ES −0.54%, NQ −0.62%, RTY −0.73%, DJIA −0.71%** — broad red futures. **WTI $103.79 (+2.37%), Brent $108.11 (+2.31%)** — oil re-spiking above $100 for a second session. **XLF premarket −0.08%** (vs XLI +0.81%, XLB +0.38%, XLU +0.20%, XLE +0.14%, XLK +0.11%, XLV +0.04%, XLY −0.13%, XLRE −0.24%, XLP −0.33%, XLC −0.63%) — XLF is **not** the green cyclical today; the value-rotation tell of 09-14 has faded. **USEPUINDXD 215.48 (−202.54 1d)** — policy-uncertainty spike unwinding. **5-day corr 10Y vs SPX −0.178** (weak). Asia composite **−0.72%** (Kospi −3.26%, Nikkei −0.81%); Europe **−0.31%**. DXY +0.25%.

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

The Finviz digest carries a **fresh, sector-specific negative**: **BAC CEO's soft Q3 outlook at the Barclays conference drove a 5% plunge**, and **BNY raised expense-growth guidance to 6–7%**. This is the first hard, same-morning, XLF-constituent-level negative in the recent sequence — it is *not* a foreign-bank print (BBVA/BCS/BNS are not XLF drivers) and *not* stale Q2 fee-boom color. BAC is a top-5 XLF holding; a −5% move in a money-center captain on guidance is a genuine S1 input. **MAP HEAT** confirms the internal picture is deteriorating: **Capital Markets dir=down** (MS crash warning + GS hawkish Fed revision — both SPX captains negative), **Credit Services dir=down** (V/MA both red, weekly residual −2.12%), **Asset Management dir=down** (BX private-credit redemption overhang), **P&C Insurance dir=down** (breadth 0.045). The only constructive pockets are **Banks-Regional dir=up (low conv, thin breadth 0.074)** and **Financial Data/Exchanges dir=down but with positive captains (SPGI/CME)**. Net: **broadly negative internal breadth**, with the money-center captain (BAC) breaking on guidance.

**4. Breadth / leadership**

1d rel **−0.22%** (modestly negative), 3d rel **−0.34%** (red), 1w rel **−0.02%** (flat), 1m rel **−0.16%** (mildly red). The freshest 1d print is negative and the 3d is negative — this is the **09-10 confirmation condition** (a negative 1d rel *does* confirm the sector-specific transmission channel), so S1 is permitted a modest negative today, unlike 09-10/09-14 where the rel was flat/sub-gate. MAP HEAT shows no broad participation bid — the constructive pockets (regionals, exchanges) are thin and low-conviction.

**5. Flows / positioning**

XLF trailing outflows. Not a crowded long (1m rel −0.16%). No fresh inflow spike. No forced-selling/rebalance event identified.

**6. Catalysts**

**No 8:30 high-impact US print today.** The dominant scheduled binary is **next week's FOMC/SEP/Warsh press conference** (news judge item 3) — unresolved, do not pre-score. **CPI core surprise locking a September hike** (news judge item 2) is the regime backdrop, already printed. **10Y at/above 5%** (news judge item 1) is the live rates shock. **BAC CEO soft Q3 outlook** (news judge item 7) is the fresh sector-specific catalyst. **US-Iran tanker war / Hormuz impaired / Brent $106–108** (news judge item 5) is the oil/stagflation overlay.

### Lessons applied (not restacked)

- **09-10 Financial:** 1d rel is **−0.22%** (negative, not flat) → the sector-specific transmission channel **is** confirmed by the tape today, so S1 may carry a modest negative. This is the *inverse* of the 09-10 configuration.
- **09-11 Financial:** no same-day high-impact binary pending (FOMC is next week) → the sub-gate-S4 trap does not apply; S4 is scored on the live 1d rel, which is negative.
- **09-14 Financial (standing rule):** the premarket relative tell is a *cap*, not an up-license; XLF premarket −0.08% is not a green-cyclical bid → no absolute-up license. Resolve divergence toward the live macro overlay, cap magnitude at mild.
- **09-08/09-09:** oil >$100 + long-end stress → S0 negative, no "value shield." But per 09-10, S1 is only scored negative when the sector's own rel tape confirms — today it does (−0.22% 1d, −0.34% 3d), so S1 = −0.5 is justified.
- **08-28:** do not triple-count the 3d/1w lag into S2/S3/S4. S2 modest, S3 = 0.
- **08-17:** long-end steepener scored as a **headwind in S0**, not NIM+.
- **08-18:** **off** (1d rel −0.22%, not ≥ +0.4%).
- **08-11:** geo/oil live; S4 negative → no absolute up, mult ≤ 1.0.
- **08-21 mag:** one band; rolling mag 0.5 → **mild**, not notable.

### Self-audit

Lens = XLF, not SPX. Band = **mild** (mag record 0.5, tight-but-creeping credit tempering, no 8:30 print). Oil/yields counted **once** in S0; S1 carries the **sector-specific** channel (BAC guidance break + negative 1d/3d rel tape), which is *confirmed* by the tape per the 09-10 rule — not a phantom double-count. BAC is a top-5 holding and a money-center captain, so it is a legitimate ETF-level input, not a single-ticker driver; but the call does not rest on BAC alone — MAP HEAT shows broad internal weakness (cap mkts, credit services, asset mgmt, P&C all down). Leading sum (S0–S3 = −2.5) vs S4 = −0.5 → same sign, **no divergence**. 08-21 green-futures ban-on-down is **off** (ES −0.54%). FOMC next week is event risk in confidence/regime language, not a directional input.

---

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -1
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
Credit spreads tightening|MISS|0.65|2026-09-15|HY OAS 2.71 +0.06 1d, creeping wider
Credit spreads blowing out|MISS|0.60|2026-09-15|HY 2.71 tight, no blowout
Sector rotation out of financials|HIT|0.60|2026-09-15|XLF premarket -0.08% vs XLI +0.81%; 1d rel -0.22%, 3d rel -0.34%
Sector breadth failure (ETF up, names flat)|HIT|0.55|2026-09-15|MAP HEAT: cap mkts/credit svcs/asset mgmt/P&C all dir=down
Large-cap leadership inside sector|MISS|0.55|2026-09-15|BAC CEO soft Q3 outlook -5%; money-center captain breaking
Sector ETF outflow / volume dry-up|HIT|0.50|2026-09-15|XLF trailing outflows, no fresh inflow spike
CRE concentration stress|NEUTRAL|0.40|2026-09-15|Overhang carried, no fresh headline
Charge-off / delinquency spike|MISS|0.45|2026-09-15|Q2 card/CRE DQ mixed-to-stable
Deposit flight / funding stress|MISS|0.45|2026-09-15|No deposit-flight headline
Bank NII / NIM beat|MISS|0.50|2026-09-15|FDIC Q2 NIM 3.32% carried, not a same-morning print
Capital markets / IB / trading surge|MISS|0.55|2026-09-15|MAP HEAT Capital Markets dir=down (MS crash warning, GS hawkish revision)
Regional bank stress easing|NEUTRAL|0.40|2026-09-15|Banks-Regional dir=up but low conv, breadth 0.074
USD strengthening|HIT|0.55|2026-09-15|DXY +0.25% 1d
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': -0.5, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -0.5}, 'multiplier': 0.9, 'leading_sum': -6.0, 'divergence_flagged': True, 'total_score': -3.834, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.553, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.1132, 'score': 0.679, 'legs': [{'leg': 'ES', 'pct': 0.36, 'w': 0.8}, {'leg': 'ZN', 'pct': -0.46, 'w': -0.6}, {'leg': 'PM:XLF', 'pct': -0.08, 'w': 0.7}]}, 'overlay_score': -3.6, 'overlay_raw': -3.6, 'index_carry': -0.912, 'general_total': -3.65, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 0.5, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 1.25}, 'llm_confidence': 0.55, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
