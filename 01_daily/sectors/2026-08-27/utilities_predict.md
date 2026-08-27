# Sector Prediction — Utilities — 2026-08-27

- ETF: **XLU**
- rubric: `00_grounding/sectors/utilities.md`
- predicted_direction: **up**
- predicted_magnitude_band: **notable**
- total_score: **7.5** (mult 1.0)
- regime: mixed
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLU vs SPY (yfinance, through 2026-08-26):
  1d: XLU +0.46% | SPY +0.02% | rel +0.44%
  3d: XLU +1.73% | SPY +0.05% | rel +1.68%
  1w: XLU -1.16% | SPY -0.39% | rel -0.77%
  1m: XLU -4.42% | SPY +3.40% | rel -7.82%
```

MEMORY_CONFIRM: Reviewed prior runs for sector Utilities — last run 2026-08-26 predicted down/mild, actual XLU +0.46% (dir MISS); 2026-08-25 predicted down/mild, actual +0.21% (dir MISS); 2026-08-24 predicted down/mild, actual pending (ungraded); 2026-08-21 predicted up/flat, actual -2.28% (dir MISS). Rolling accuracy dir=0.4, mag=0.5 (n=10). Standing active lessons: (1) 08-11 — don't mechanically continue a down call when the yield driver is easing and tape is inflecting positive; (2) 08-12 — cap magnitude to mild when risk-on tech-led tape + same-day sector-narrative headwind; (3) 08-13 — treat S2/S4 as absolute confirmation only, allow XLU to lag SPY; (4) 08-14 — scan the economic calendar for 8:30 ET high-impact releases that could flip a growth-led tape into defensive rotation; (5) 08-17 — a carried defensive rotation with no fresh same-day catalyst is a RELATIVE bid, not an ABSOLUTE bid; (6) 08-18 — when a bond-proxy sector has both a risk-off defensive bid AND rising 10Y/long-end yields, default to relative outperformance / flat-to-negative absolute; (7) 08-21 — do not score "yields easing" off stale FRED; live tape + hawkish-Fed overhang cap S0/S1; (8) 08-25 — when S0/S1 both neutral and only carried S2/S3 negatives, prefer flat over down on a risk-on tape; do not manufacture a down call from carried negatives alone. The 08-25 lesson is the most directly relevant to today's setup.

---

## Utilities (XLU) — Sector Environment Analysis — 2026-08-27

Object is the **near-session XLU environment**, not SPX and not a stock pick.

### Channel 1 (trusted, not re-derived)

XLU vs SPY: **1d +0.46% / +0.02% (rel +0.44%)**; **3d +1.73% / +0.05% (rel +1.68%)**; **1w −1.16% / −0.39% (rel −0.77%)**; **1m −4.42% / +3.40% (rel −7.82%)**.

Macro panel: VIX 15.21 (−0.24, low); DGS10 **4.64** / DGS30 **5.17** as of **2026-08-25** (both −6 bp that print); DFII10 **2.32** (1d −6 bp, 1w −9 bp, 1m −12 bp — real yields easing on all horizons); HY 2.70 (tight); EPU **−117** to 230 (big drop); CL −1.11% / BZ −2.11%; gold +1.45%; DXY −0.03% 1d / −1.64% 1m; **ES +0.31% / NQ +0.55%** premarket (risk-on); Asia +0.1% (Kospi +1.26%, Shanghai +0.84%); Europe +0.13%; F&G 56.8 (Greed); 5-day 10Y–SPX corr **−0.292**.

**Recency check (08-21 lesson):** The FRED yield prints are **Monday (8/25)**. Tuesday's live tape was a mild risk-on day (XLU +0.46%, SPY +0.02%). This morning's live quotes show 10Y ~4.64%, 30Y ~5.17% — **easing on 1d/1w/1m across nominal AND real yields**, with real yields down 12 bp on the month. This is a **confirmed easing regime**, not a stale read. The hawkish-Fed overhang (Collins, regional directors) is live but the tape is not confirming a rate-hike repricing — yields are actually falling.

### Channel 2

**1. Shared macro → this sector.** The dominant scheduled catalyst is **July core PCE due today** (binary, two-sided). The market is positioned for a cool print (gold +1.45%, yields easing, real yields down 12 bp 1m). A cool/in-line PCE would relieve the duration headwind and lift bond-proxy utilities; a hot print is the asymmetric risk. The **08-14 lesson** applies: scan the calendar for a regime-flip catalyst — PCE is exactly that. The **08-12 REIT/utilities lesson** also applies: when CPI/PCE is imminent for a long-duration sector, treat it as two-sided; with easing expectations visible (real yields down 1m), do not default S0 negative. Oil is **down** (CL −1.11%, BZ −2.11%) — no Hormuz supply squeeze, so no fresh inflation-via-oil override. EPU dropped sharply (−117) — de-escalation/calm signal.

**2. Spine / secondary.**
- **Data-center / power demand:** structural HIT, **stale**. No fresh same-day load catalyst. Carried WoodMac/Texas/Nvidia-Ohio skepticism still sits on the narrative. **Do not** let the multi-year AI-power story drive a 1d call without a fresh catalyst.
- **Rates falling (bond-proxy bid):** **HIT** — 10Y −6 bp, 30Y −6 bp, real −6 bp on 1d; real −12 bp on 1m. This is the key driver today, and it is **confirmed** (not stale).
- **Real yields falling:** **HIT** (DFII10 −6 bp 1d, −9 bp 1w, −12 bp 1m).
- **Rates rising:** not firing (yields easing).
- **Risk-on rotation away:** not firing (NQ/ES positive but modest; not a strong growth-led rotation).
- **Nuclear / grid CapEx:** structural HIT (stale).
- **Rate cases:** no fresh favorable/adverse order.
- **Load-growth disappointment / AEP miss:** carried, not fresh. Single-name must not drive the ETF call.

**3. Breadth.** 1d rel +0.44%, 3d rel +1.68% (both positive — durable inflection), 1w rel −0.77%, 1m rel −7.82% (deep underperformance). The 1d/3d relative outperformance is a positive inflection after the prior week's smash. This is the **08-11 pattern**: driver (yields) easing + 1d/3d tape inflecting positive → do not mechanically continue a down call.

**4. Flows.** Prior logs noted 5d −$190M, 1m −$236M outflows (through ~8/21). No confirmed same-day inflow spike in the search. De-risked on 1m (rel −7.82%), not a crowded-long extreme. Neutral-to-slightly-negative flow read.

**5. Catalysts.** **PCE today** is the load-bearing event (two-sided, with easing expectations visible). No fresh XLU-wide earnings/rate-order win. AEP PT cut is carried.

### Lessons → scores

**08-25 lesson (most relevant):** When S0/S1 are both neutral and only carried S2/S3 negatives remain, prefer flat over down on a risk-on tape. Today S0/S1 are **not** neutral — the easing-yield driver is confirmed and live (real yields down 12 bp 1m), so S0/S1 should be **positive**, not neutral. This is the key difference from 08-25/08-26.

**08-11 lesson:** Driver (yields) easing + 1d/3d tape inflecting positive → do not mechanically continue a down call. This fires today.

**08-14 lesson:** PCE is a scheduled high-impact release that could flip the tape. With easing expectations visible, score the bond-proxy bid as a live positive input, not neutral.

**08-18 lesson:** Risk-off + rising yields → relative beat / flat-to-negative absolute. Today is the **opposite**: yields are **falling** and the tape is risk-on. The 08-18 trigger does not fire.

**08-21 lesson:** Do not score "yields easing" off stale FRED. Today's easing is **confirmed** (real yields down 12 bp 1m, live quotes easing). The lesson does not cap S0/S1 here.

**08-12 lesson:** Cap magnitude to mild when risk-on tech-led tape + same-day sector-narrative headwind. Today the tape is mildly risk-on (ES +0.31%, NQ +0.55%) but not a strong growth-led rotation, and there is no fresh sector-narrative headwind. The PCE is two-sided. Cap magnitude at mild.

**Divergence:** Leading factors (easing yields, positive 1d/3d tape, real-yield relief) lean **up**; the 1w/1m relative tape is still negative (carried). The 1d/3d positive inflection is the leading signal. **Flag mild divergence** — trust the easing-yield factor over the carried 1w/1m underperformance.

**Self-audit:** Rate lens over AI narrative on a 1d horizon; band capped at mild (PCE is binary, two-sided); no same-shock double-count of yields in S0 and S1 (both reflect the confirmed easing but are distinct channels — macro vs sector bond-proxy); AEP single-name does not drive the ETF call. Policy: last three XLU losses were **up** calls vs a negative tape — but today the tape is positive and the driver is easing, so an up call is justified; keep magnitude mild.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 1
S1_SECTOR_FACTORS: 1
S2_BREADTH: 1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 1
MULTIPLIER: 1.0
CONFIDENCE: 0.55
REGIME: mixed
HORIZON_3D: up:mild:0.52
HORIZON_1W: up:mild:0.48
HORIZON_2W: flat:mild:0.45
HORIZON_1M: flat:mild:0.42
SECTOR_SCORES_END

HIT_GRID_BEGIN
Rates falling (bond-proxy bid)|HIT|0.7|2026-08-27|Confirmed easing: 10Y −6bp, 30Y −6bp, real −6bp 1d; real −12bp 1m
Real yields falling|HIT|0.7|2026-08-27|DFII10 −6bp 1d, −9bp 1w, −12bp 1m
Data-center load growth / power demand upside|HIT|0.5|2026-08-27|Structural, stale; no fresh same-day catalyst
Nuclear / gas generation policy support|HIT|0.5|2026-08-27|Structural, intact
Grid CapEx approval / recovery|HIT|0.5|2026-08-27|Structural, intact
Sector breadth expansion (% names up)|HIT|0.5|2026-08-27|1d rel +0.44%, 3d rel +1.68% positive inflection
Load growth disappointment|MISS|0.4|2026-08-27|Carried (Texas/Nvidia-Ohio), not fresh
Sector ETF outflow / volume dry-up|MISS|0.4|2026-08-27|5d −$190M, 1m −$236M carried outflows
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 1.0}, 'multiplier': 1.0, 'leading_sum': 7.0, 'divergence_flagged': False, 'total_score': 7.5, 'predicted_direction': 'up', 'predicted_magnitude_band': 'notable', 'confidence_score': 0.55, 'regime': 'mixed'}
```
