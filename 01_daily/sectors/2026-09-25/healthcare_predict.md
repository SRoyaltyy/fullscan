# Sector Prediction — Healthcare — 2026-09-25

- news_mode: **on**
- ETF: **XLV**
- rubric: `00_grounding/sectors/healthcare.md`
- predicted_direction: **up**
- predicted_magnitude_band: **mild**
- total_score: **0.936** (mult 0.8)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **0.26** (ES +0.28%, PM:XLV -0.01%) · index_carry **0.676** (general 2.706) · llm_overlay **0.0** (raw 0.0)

## Channel 1 sector ETF tape

```
ETF XLV vs SPY (yfinance, through 2026-09-24):
  1d: XLV +0.63% | SPY -0.08% | rel +0.72%
  3d: XLV +1.26% | SPY +0.72% | rel +0.54%
  1w: XLV +1.64% | SPY +1.99% | rel -0.35%
  1m: XLV -2.39% | SPY +0.74% | rel -3.13%
```

MEMORY_CONFIRM: Healthcare/XLV 2026-09-25. Memory index unavailable this run (embedding metadata missing). Rolling HC dir=0.2 mag=0.5 (n=10); last-30 dir=0.458 mag=0.333 (n=24). Last graded **09-24 down/mild vs actual +0.634% (dir MISS, mag HIT)** — the binding lesson is the **09-23/09-24 empty-spine family**: when the sector factor card nets ≈0 (or near-zero) with no divergence, the engine must NOT mint a signed direction off `index_carry`/`tape_anchor`; the card's flat must win. Open experiment **applies** as prefer flat/mild + shrink confidence when factor sign fights tape. Calendar: **Fri cash session**; **no 8:30 CPI/NFP**; **FOMC+SEP+presser PRINTED 09-16** (paid, T+7) — not today's binary; **IRA cycle-3 final offers Sep 30**, not today. **09-23 empty-spine gate FIRES** (S0–S4 net ≈ 0, no divergence → flat/flat, suppress index_carry). **09-22 emit-cap FIRES** (paid FOMC + empty S1 + mixed tape → flat/flat, not leftover-PM down/mild and not ES-carry up). **09-18 complement FIRES as up-cap** (PM:XLV **−0.01% ≤ 0**, not already-mild). **09-17 keep-up does NOT fire** (PM not ~+0.4%). **09-16 force-flat does NOT fire** (FOMC paid). **09-21 beta-arithmetic does NOT fire** (ES=F **+0.28% / NQ=F +0.57%**, not ≥+1%). **09-11 funding-source does NOT fire at full weight** (needs confirming futures ≥+0.5% AND a pending binary; today Finviz ES +0.20% is not a rip). **08-13 reversal-tell does NOT fire** (Channel 1 3d/1w/1m rel are **not** uniformly leftover leadership: 3d +0.54%, 1w −0.35%, 1m −3.13%). **09-14 destination does NOT fire** (NQ leading ES *up*, not down ≥50bp). **09-10 decay cap does NOT fire** (1d rel +0.72%, not |rel|≤0.15% stabilization). **09-15 S0~0 complement does NOT fire** (not broad uniform risk-off). **08-17 oil FTS narrowed off** (WTI/Brent still ~$104/$108, offered off elevated levels). **08-14 policy audit: no same-morning mega-cap Rx headline** (MFN 50-state is 09-18 residual; IRA final offers Sep 30). **08-11 MA cut stale** (April 2027 +2.48%). **08-21 / AMGN dazodalibep Phase-3 (Jefferies PT $410) is a fresh large-cap biotech readout — sector-relevant but single-large-cap; must not dominate XLV.** **08-28 leftover-stack: do not copy 3d/1w/1m rel into S2/S3/S4.** No oil double-count into rotation. No AVGO/XLK/ASML map into S0. Nested MAP HEAT OVERRIDE (Facilities) not averaged into XLV. Fear & Greed **58.2 is stale (2026-08-27)** — ignored. FedWatch not scrapable. size_gate=True.

# Healthcare / XLV — 2026-09-25

**Object:** near-session environment for **XLV** (not SPX, not a stock picker).

## Channel 1 (trusted, unaltered)

XLV vs SPY through **2026-09-24**: **1d +0.63% / −0.08% (rel +0.72%)**; 3d rel **+0.54%**; 1w rel **−0.35%**; 1m rel **−3.13%**. Thursday's absolute was a **defensive outperformance on a red SPY tape** — the relative print (+0.72%) is a genuine cushion, and it is the *first* positive 1d rel after a run of negative ones. But 1w is still modestly negative and 1m is a **deep relative lag** — crowded-long fuel is gone (the opposite of early-September extension). 08-13 leftover-RS leadership is **absent** (3d/1w/1m are not uniformly green).

Macro: VIX **15.38** (−0.29 1d, −0.06 1w); **VIX/VIX3M 0.835 contango** (no backwardation stress). **Finviz futures: ES +0.20%, NQ +0.41%, RTY +0.08%, DJIA +0.11%** — modest green, NQ leading. Separate **`[ES=F +0.28%] / [NQ=F +0.57%]`** — both panels agree on sign and on NQ≥ES, and both agree this is **not** a ≥+1% rip. **WTI $104.16 −1.59% / Brent $107.67 −1.02% / CL=F −1.71% / BZ=F −7.41%** — oil **offered off elevated levels**. **DGS10 5.11 (+0.15 1d, +0.41 1m) / DGS30 5.40 (+0.11 1d)** — long yields **spiking**; **DFII10 2.76 (+0.13 1d, +0.38 1m)** — real yields **rising hard**. DXY **−0.20% 1d, +2.2% 1m** — USD firming on the month. HY OAS **2.73 (+0.05 1d)**. **5-day corr 10Y vs SPX −0.958** — yields are *the* equity driver. Asia composite **−0.06%** (split: Nikkei +1.3%, Kospi +1.04%, Hang Seng −1.01%, Shanghai −1.22%); Europe **+0.64%**. **PM: XLV −0.01% vs XLK +0.79%, XLE −0.99%, XLI −0.38%, XLU +0.30%, XLP −0.12%** — healthcare is **flat with the board**, not bid as a haven and not smashed as funding. **FOMC is paid (09-16).** **size_gate=True.**

## Channel 2

**1. Shared macro → this sector (S0).** Pre-open tape is **mixed, not a regime day**. The dominant macro driver (News Judge #1/#2/#3) is a **hawkish rate shock**: 10Y tops **5.2%**, Williams says another hike by year-end is "reasonable," Warsh JH lifted September hike odds, gold slid >3%. That is a **direct duration headwind for the XBI sleeve** and a multiple-compression force on all equities — but it is **already printed** (the 10Y spike is in Channel 1's DGS10 5.11 / DFII10 2.76, and the futures panel is *green*, not red). The live futures panel is **modestly green with NQ≥ES** — a weak 09-11 funding-source shape, but 09-22 already falsified scoring that as S0 −0.3 when futures are inside ±0.5% and the binary is paid. Oil-offered is **not** a duration tailwind for XBI (09-11 inversion still binds) and **not** 08-17 FTS (oil still elevated). It is **not** 09-14 destination (NQ is leading ES *up*, not down ≥50bp). Real-yield rising (+0.13 1d, +0.38 1m DFII10) is a **second-order XBI-sleeve drag** — do **not** restack it into S0 as a fresh shock (09-10 same-shock; the 10Y spike is the same impulse as the news-judge rates cluster). Live PM **−0.01%** forbids a fat absolute-up from Finviz ES +0.20% (09-18: paid FOMC + PM ≤ 0 closes 09-17's keep-up) and forbids converting the same flat print into down/mild (09-22: 09-18 is an emit-cap, **not** a down mandate). **S0 = 0**. Do not score oil-falling as "rotation into healthcare." Do not map the rates cluster into a second S0 hit — it is already in the tape.

**2. Spine / secondary (S1).**
- **CMS / MA 2027 +2.48%:** April finalization — **stale**. Sept Part D NAMBA / landscape / Star cutpoints is process. **Checked, nothing material** as a live MA-upside HIT.
- **Biotech / XBI:** MAP HEAT Biotechnology **dir=flat conv=low** (VRTX mixed, REGN neg, CYTK/KRYS none) — "a stock-picker tape, not a sector bid." **AMGN dazodalibep Phase-3 positive** (Jefferies PT → $410) is a **fresh large-cap biotech readout** — sector-relevant, but single-large-cap; per 08-21 it must **not** dominate XLV. Not a funding-winter cluster. Duration sleeve is a minority XLV weight — do not restack rising real yields into S1 (09-10 same-shock). **Not XBI leadership.**
- **Drug pricing:** IRA cycle-3 final offers **Sep 30**, not today. MFN 50-state Medicaid / GENEROUS is **09-18 residual**. **No same-morning mega-cap Rx headline.** 08-14 does **not** fire.
- **FDA / trials:** No fresh sector-wide approval/CRL cluster in the digest. ARGX Forte close is **paid**. Small-cap CMC CRL cluster is **not an XLV basket**. **Must not dominate.**
- **Devices / plans / distribution:** **BSX Citi Neutral (09-17)** single-name; **CI Jefferies Hold** single-name; **CAH CEO stock sale** is a Barron's color item, not a fundamental smash. MAP HEAT Plans **dir=up conv=medium** (breadth 0.909, CLOV +19.86% w1, OSCR +4.84%) and OVERRIDE Facilities **dir=up conv=high** (HCA +5.06%, LFST +5.11%) are **nested** — do not average into the parent ETF.
- **Rotation:** 3d/1w/1m **into** healthcare is **not** a clean carried bid (1w −0.35%, 1m −3.13%). Today's live tell is a **flat PM with a modest tech-led green tape** — that lives in **S0**, not a second S1 hit.

Net **S1 = 0**.

**3. Breadth (S2).** Sector-internal only (09-10). MAP HEAT is **split-to-positive**: Diagnostics **up/med**, Specialty&Generic **up/med**, Health-IT **up/med**, Plans **up/med**, Devices **up/med**, Distribution **up/med**, Instruments **up/med**, Facilities **OVERRIDE up/high**; Biotech **flat/low**, Drug Manufacturers-General **flat/low**. That is a **broad, quiet nested bid** the parent ETF understates — but it is **nested sub-industry heat, not confirmed XLV-wide breadth expansion**, and the parent PM is flat. Do not copy 3d/1w/1m RS into S2. **S2 = 0** (mild positive tilt noted, not scored, because the parent tape does not confirm).

**4. Flows (S3).** No fresh ETF flow/volume data in the packet; no crowding signal (1m rel −3.13% means the crowded-long fuel is gone, so the 09-04 unwind amplifier does **not** apply). **S3 = 0**.

**5. ETF tape (S4, confirmation only).** 1d rel **+0.72%** is a **positive** confirmation — the first clean defensive-outperformance print after a run of negative rel. But it is a *single* day against a still-negative 1w/1m, and PM is flat. Per 08-28, do not stack it. **S4 = +0.5** (confirmation only, not a thesis).

## Divergence / self-audit

- **Lens:** near-session environment for XLV, not SPX, not a stock picker. ✔
- **Band:** leading sum ≈ +0.5 (S4 only) → **flat/flat** is the honest card; the 09-23/09-24 empty-spine gate says do not let `index_carry`/`tape_anchor` mint a signed call off a ≈0 sector card. ✔
- **Skew:** the temptation is to read the green futures + positive 1d rel as an up call. That is exactly the 09-18/09-23/09-24 failure — ES-carry up on a flat PM. Rejected. ✔
- **Same-shock double-count:** the rates cluster (10Y 5.2%, Williams, Warsh, gold −3%) is scored **once** in S0 as context, not restacked into S1 (XBI duration) or S2. ✔
- **Single-ticker:** AMGN dazodalibep, BSX Citi, CI Jefferies, CAH insider sale, ARGX Forte — none drives the XLV call. ✔
- **Divergence flag:** the LLM card (flat) and the engine's likely ES-carry (up/mild) **disagree** → flag it; trust the factor card over tape.

**Call: flat / flat.** The sector factor card nets ≈0 (S0 0, S1 0, S2 0, S3 0, S4 +0.5 confirmation-only), PM:XLV is flat, the tape is a modest tech-led green that makes healthcare a funding source rather than a destination, and the 09-23/09-24 empty-spine gate forbids minting a signed direction off index_carry. Low confidence given the split MAP HEAT nested bid and the positive 1d rel cushion.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 0
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0.5
MULTIPLIER: 0.8
CONFIDENCE: 0.35
REGIME: mixed
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|HIT|0.55|2026-09-25|https://www.finviz.com/futures.ashx
Risk-off tape / flight to safety|MISS|0.60|2026-09-25|https://www.finviz.com/futures.ashx
Real yields rising|HIT|0.75|2026-09-25|https://fred.stlouisfed.org/series/DFII10
Real yields falling|MISS|0.70|2026-09-25|https://fred.stlouisfed.org/series/DFII10
USD strengthening|MISS|0.55|2026-09-25|https://www.finviz.com/futures.ashx
USD weakening|HIT|0.50|2026-09-25|https://www.finviz.com/futures.ashx
Sector breadth expansion (% names up)|HIT|0.45|2026-09-25|https://www.finviz.com/screener.ashx
Sector breadth failure (ETF up, names flat)|MISS|0.40|2026-09-25|https://www.finviz.com/screener.ashx
Large-cap leadership inside sector|HIT|0.45|2026-09-25|https://www.finviz.com/screener.ashx
Small/mid leadership inside sector|MISS|0.40|2026-09-25|https://www.finviz.com/screener.ashx
High-beta leadership inside sector|MISS|0.45|2026-09-25|https://www.finviz.com/screener.ashx
Low-beta leadership inside sector|HIT|0.45|2026-09-25|https://www.finviz.com/screener.ashx
Sector ETF inflow / relative volume spike|MISS|0.35|2026-09-25|https://www.finviz.com/etf.ashx?t=XLV
Sector ETF outflow / volume dry-up|MISS|0.35|2026-09-25|https://www.finviz.com/etf.ashx?t=XLV
Crowded long (extreme relative performance + valuation)|MISS|0.55|2026-09-25|https://www.finviz.com/etf.ashx?t=XLV
Index rebalance / inclusion tailwind|MISS|0.30|2026-09-25|https://www.finviz.com/etf.ashx?t=XLV
Index exclusion / forced selling|MISS|0.30|2026-09-25|https://www.finviz.com/etf.ashx?t=XLV
FDA approval / favorable panel (sector breadth)|MISS|0.40|2026-09-25|https://www.fda.gov/drugs
Positive late-stage trial readout (breadth)|HIT|0.45|2026-09-25|https://www.finviz.com/quote.ashx?t=AMGN
CMS / Medicare Advantage rate upside|MISS|0.35|2026-09-25|https://www.cms.gov/medicare
Biotech risk-on / XBI leadership|MISS|0.50|2026-09-25|https://www.finviz.com/etf.ashx?t=XBI
Drug pricing policy relief|MISS|0.35|2026-09-25|https://www.cms.gov/medicare
FDA rejection / CRL / trial failure (breadth)|MISS|0.40|2026-09-25|https://www.fda.gov/drugs
Medicare rate cut / reimbursement pressure|MISS|0.35|2026-09-25|https://www.cms.gov/medicare
Drug pricing crackdown / IRA expansion risk|MISS|0.40|2026-09-25|https://www.cms.gov/medicare
Biotech risk-off / funding winter|MISS|0.45|2026-09-25|https://www.finviz.com/etf.ashx?t=XBI
Utilization spike hurting insurers|MISS|0.35|2026-09-25|https://www.finviz.com/screener.ashx
Sector rotation into healthcare|MISS|0.45|2026-09-25|https://www.finviz.com/etf.ashx?t=XLV
Sector rotation out of healthcare|HIT|0.45|2026-09-25|https://www.finviz.com/etf.ashx?t=XLV
HORIZON_3D|flat|0.35|2026-09-25|https://www.finviz.com/etf.ashx?t=XLV
HORIZON_1W|flat|0.35|2026-09-25|https://www.finviz.com/etf.ashx?t=XLV
HORIZON_2W|down|0.30|2026-09-25|https://www.finviz.com/etf.ashx?t=XLV
HORIZON_1M|down|0.30|2026-09-25|https://www.finviz.com/etf.ashx?t=XLV
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.5}, 'multiplier': 0.8, 'leading_sum': 0.0, 'divergence_flagged': False, 'total_score': 0.936, 'predicted_direction': 'up', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.537, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.0434, 'score': 0.26, 'legs': [{'leg': 'ES', 'pct': 0.28, 'w': 0.6}, {'leg': 'PM:XLV', 'pct': -0.01, 'w': 0.7}]}, 'overlay_score': 0.0, 'overlay_raw': 0.0, 'index_carry': 0.676, 'general_total': 2.706, 'skill_multipliers': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 1.25, 'S2_BREADTH': 1.25, 'S3_FLOWS_POSITIONING': 1.25, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.35, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
