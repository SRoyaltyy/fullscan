# Sector Prediction — Consumer Defensive — 2026-09-15

- ETF: **XLP**
- rubric: `00_grounding/sectors/consumer_defensive.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **-5.776** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**
- engine: v2 · tape_anchor **-1.442** (ES +0.34%, ZN -0.46%, PM:XLP -0.33%) · index_carry **-0.959** (general -3.836) · llm_overlay **-3.375** (raw -3.375)

## Channel 1 sector ETF tape

```
ETF XLP vs SPY (yfinance, through 2026-09-15):
  1d: XLP -0.81% | SPY -0.56% | rel -0.25%
  3d: XLP +0.78% | SPY -0.16% | rel +0.94%
  1w: XLP -0.34% | SPY -1.22% | rel +0.88%
  1m: XLP -2.74% | SPY -2.54% | rel -0.19%
```

MEMORY_CONFIRM: Consumer Defensive / XLP only — memory index paused (embedding metadata mismatch); using injected sector scoreboard + last-10 logs. Rolling dir=0.5 / mag=0.4 (n=10); last-30 dir=0.474 / mag=0.368. Last graded 2026-09-14 predicted up/mild vs XLP +1.247% / SPY −0.446% / rel +1.694% (dir HIT, mag MISS — 09-14 reflect: the 08-18 duration cap was applied at full weight to an *under-owned* defensive with a live best-of-eleven premarket bid, suppressing the absolute band). 09-11 up/flat dir HIT / mag HIT absolute but a *relative* miss (benign-CPI branch is a relative negative for a low-beta defensive, not neutral). 09-10 down/flat dir MISS (food-crash dominance over-applied with no fresh print, zeroing a maximal FTS panel). 09-09 down/flat dir HIT / mag HIT (fresh food-crash, oil >$100). 09-08 flat/flat dir MISS (S0=+0.5 FTS bid overweighted). 09-04 down/mild dir HIT / mag HIT. 09-03 up/flat dir MISS (prior FTS day + two-sided ISM = FTS unwind). 08-28 down/mild dir MISS (leftover anti-FTS restacked). No open experiment tagged to this sector beyond the 08-28 DO-INSTEAD (prefer flat/mild when sign fights tape) and "keep direction, shrink confidence on modest |score|." Today I do **not** re-litigate stale WMT (08-20), do **not** copy 3d/1w/1m lag into S2/S4 as independent confirmation (08-28), do **not** treat Warsh as still two-sided (printed 08-28; path is live hawkish and FOMC is *tomorrow*), do **not** fire 08-27's down/notable gate (NQ +0.26% vs ES +0.34% — NQ is *not* leading by ≥0.5%), do **not** apply the 09-08/09-09 food-crash-dominance override at full weight (09-10 reflect capped it at one session and requires a fresh negative print — the CPB cut is 09-03, now T+8), do **not** convert the FTS bid into absolute up (08-18 utilities), and I **do** treat the 10Y>5% duration shock + FOMC binary + oil >$103 as **one** rates/risk-off regime object counted once.

---

# Consumer Defensive (XLP) — 2026-09-15

Object is the **near-session XLP environment**, not SPX and not a stock picker. Channel 1 numbers are used as given.

## Channel 1 tape (confirmation only)

```
ETF XLP vs SPY (yfinance, through 2026-09-15):
  1d: XLP -0.81% | SPY -0.56% | rel -0.25%
  3d: XLP +0.78% | SPY -0.16% | rel +0.94%
  1w: XLP -0.34% | SPY -1.22% | rel +0.88%
  1m: XLP -2.74% | SPY -2.54% | rel -0.19%
```

This is the **most constructive multi-horizon tape XLP has had in weeks**: 3d rel **+0.94%**, 1w rel **+0.88%**, 1m rel only **−0.19%** (vs −0.94% on 09-14). The 1d rel **−0.25%** is a *sub-threshold* give-back after yesterday's +1.69% FTS day — it is **not** a fresh negative signal (|1d rel| < 0.5% band; the 08-28 rule forbids copying a paid print into S2/S4). Critically, the **1m rel has converged to flat**, which means the "deep multi-horizon laggard" descriptor that anchored the 09-08/09-09/09-10 down calls is **no longer true**. The 09-14 catch-up has begun and is now 2 sessions old.

Macro panel as it maps here: **ES=F +0.34% / NQ=F +0.26%** (mildly green, NQ *not* leading — **not** 08-21 ES≥+0.3% *with* a sector catalyst, **not** 08-27 NQ≥+0.5% leading). Finviz cash futures **SPX −0.54% / NDX −0.62% / DJIA −0.71% / Russell −0.73%** — note the **sign conflict** between the two futures feeds; I treat the pre-fetched Channel 1 ES/NQ (+0.34/+0.26) as the live premarket read and the Finviz block as the prior-session close snapshot. **VIX 17.49 (+0.39 1d, +1.77 1w) with VIX/VIX3M 0.897 — CONTANGO** (the three-session backwardation of 09-08→09-14 has **resolved**; vol stress is *easing* in term structure even as spot VIX rises). **WTI $103.79 +2.37% / Brent $108.11 +2.31%** — oil **above $100**, war-premium levels, the strongest version of the Hormuz trigger. Gold **−1.01%**, silver **−1.47%**, copper **−0.76%**, platinum −1.67%, palladium −1.85% — **entire metals complex co-moving down with equities** (08-18 metals-co-move pattern), no metals floor. **DXY +0.25%** (USD firm). **10Y note −0.46% / 30Y bond −0.93% / Ultra Bond −1.09%** (bond prices down hard ⇒ **yields backing up sharply**); **DGS10 4.96 (+0.19 1w, +0.28 1m)**, **DGS30 5.35 (+0.10 1w)**, **DFII10 2.60 (+0.18 1w, +0.18 1m — real yields rising fast)**. Headline: **10Y breaches 5%, highest since 2007**. HY OAS **2.71 (+0.06 1d)**. 5-day 10Y–SPX corr **−0.151** (mildly negative — *not* the −0.9 regime of 09-08/09-10). Asia composite **−0.72%** (Kospi −3.26%, Nikkei −0.81%, Hang Seng +0.45%), Europe **−0.31%**. **Ag inputs mixed-to-soft**: corn −0.28%, soybeans −0.33%, wheat −0.17%, soybean meal −0.66%, sugar −0.28%, rough rice −0.06%; soybean oil +0.53%, coffee +1.68%, cocoa +1.04%, **orange juice +5.30%**, live cattle +1.17%, feeder cattle +1.61%, lean hogs −2.36%. **Sector premarket: XLP −0.33%** — mid-pack of eleven (XLI +0.81%, XLB +0.38%, XLU +0.20%, XLE +0.14%, XLK +0.11%, XLV +0.04%, XLF −0.08%, XLY −0.13%, XLRE −0.24%, XLP −0.33%, XLC −0.63%). Fear & Greed **UNAVAILABLE**. EPU **215.48 (−202.54 1d)** — policy-uncertainty spike *collapsing*.

## Channel 2 — required categories

**1. Shared macro → this sector.** Live tape is **rates-led risk-off into a binary**: 10Y >5% (highest since 2007), global bond selloff, oil >$103, Asia red (Kospi −3.26%), Europe soft, USD firm, metals down. News Judge #1: **10Y breaches 5% / global bond selloff** — the dominant driver, compressing multiples and small-cap risk appetite. News Judge #2: **chipmaker weakness / AMD −5% on AI-slowdown calls** — an NDX/SOXX object, *not* an XLP object. News Judge #3: **August CPI core surprise "locks in" September hike + FOMC/SEP/Warsh presser** — the policy path validating the 5% 10Y; FOMC is *tomorrow* (09-16), so the binary is **pending, not today's print**. News Judge #4: **US–Iran tanker war; Hormuz traffic impaired; Brent ~$107–109** — oil/stagflation overlay. News Judge #5: **UMich Sept prelim 47.8; 1-yr inflation expectations 4.6%** — inflation-expectations backup confirming the hike path. News Judge #8: **OPEC+ holds October quotas unchanged** — leaves Hormuz tightness un-offset.

For staples the map is **two-sided and must be counted once per channel**:
- **Rates-led risk-off + oil >$103 + Asia red** = theoretical **relative FTS bid vs cyclicals** (sector layer: risk-off relative +). This is the 08-18 template: rising long-end yields + risk-off → **relative outperformance / flat-to-negative absolute**.
- **But the duration shock is the dominant channel for a bond-proxy**: 10Y >5%, DGS30 5.35, DFII10 2.60 (+0.18 1m) — real yields rising fast. This is a **direct headwind** to a ~2.6%-yield defensive, and it caps absolute upside. Do **not** upgrade to absolute up (08-18 utilities).
- **Oil >$103 is itself an input-cost negative** for staples (freight, packaging, ag feedstocks) — count it in S1, **not** as a second S0 defensive bid. The 08-11 geo/oil rule fires on the oil/risk-off overlay, but the 09-10 refinement caps the FTS credit: the sector has shown it can hold a defensive bid only when the panel is maximal-FTS *and* no fresh sector drag exists.
- **FOMC is tomorrow (09-16)** — a pending two-sided binary. Per the 08-12 bond-proxy rule and the 09-04 asymmetric-downside refinement: with pre-existing duration stress (10Y >5%, real yields +0.18 1m), the hawkish tail compounds an already-active channel while the dovish tail only relieves it. Score the asymmetry modestly, do **not** pre-score the outcome.
- **Metals all down** — no metals floor; the 08-18 metals-co-move pattern is live.
- **No 8:30 CPI/PPI/PCE today** (CPI printed 09-11; FOMC 09-16). Do not manufacture a same-day macro binary.

**2. Spine (mandatory).**
- **Flight-to-safety RS vs cyclicals (primary):** **PARTIAL HIT.** The 3d/1w rel are positive (+0.94%/+0.88%) and the 09-14 FTS day (+1.69% rel) is real, but the **live premarket is XLP −0.33%** — mid-pack, *not* the best-of-eleven bid of 09-14. The FTS bid is **not** being confirmed this morning; the tape is rates-led, and XLP is a bond-proxy, so it is *not* the clean haven today. Dampen: no fresh sector catalyst, and the 1d rel give-back (−0.25%) is sub-threshold.
- **Risk-on rotation away from defensives:** **MISS.** Futures are mildly green but NQ is not leading; the tape is rates-led risk-off, not risk-on.
- **Pricing power held without volume collapse:** **PARTIAL / carried.** No fresh same-day staples beat. MAP HEAT: **Household & Personal Products dir=down (PG:neg, CL −2.72% w1)** — "PG drags on staples volume read-through." **Beverages – Non-Alcoholic dir=flat (KO:mixed, PEP:neg)** — "soft snack demand." **Discount Stores dir=flat (WMT:mixed, COST:neg)** — "COST price-hike friction." This is a **mildly negative internal read**, not a re-rate.
- **Volume decline accelerating:** **PARTIAL.** MAP HEAT's PG/CL/KO/PEP/COST reads are volume-friction color; not a same-morning hard print. Do not restack as a one-way FTS tailwind (08-17).

**3. Secondary.**
- **Input cost relief (ag, packaging, freight):** **MISS.** Oil is **up +2.37%** (WTI $103.79, Brent $108.11). Ag is mixed-to-soft (corn −0.28%, wheat −0.17%, soybeans −0.33%) — a *mild* offset, but the dominant input (energy/freight) is spiking. Net: **input-cost pressure**, not relief.
- **Input cost spike without pricing power:** **HIT as the energy/freight sleeve** — but oil is **not** counted again after the FTS/rates object (same Hormuz shock). Ag is soft, so the incremental ag hit is small.
- **Volume stabilization / sequential improvement:** checked, nothing material and new.
- **Staples earnings beat / stable margins:** **checked, nothing material for the ETF.** No WMT/PG/COST/KO print today. MAP HEAT captains are all "none" or "mixed" on fresh catalysts.
- **Private-label share gain against brands:** **HIT (structural)** — carried, not a same-day print.
- **Sector rotation into/out of defensives:** **MIXED.** The 3d/1w rel are positive (rotation *into* defensives over the past week), but the live premarket is mid-pack and the 1d rel is a give-back. The rotation-in is **2 sessions old and decelerating**.

**4. Breadth / leadership inside the sector.** MAP HEAT is **mixed-to-negative**: Farm Products dir=up (ADM +3.37%, BG +2.94% w1 — the only clean up-tape), Grocery Stores dir=down (KR DOJ beef-probe), Household & Personal Products dir=down (PG drag), Beverages–Brewers dir=down (STZ/TAP neg), Confectioners dir=down (MDLZ shrinkflation risk), Food Distribution dir=flat, Discount Stores dir=flat (breadth 0.0). **No broad internal expansion** — the up-tape is confined to Farm Products (a commodity-linked sleeve, not the defensive core). This is **breadth failure**, not expansion.

**5. Flows / positioning / crowding.** No confirmed XLP inflow print. The 09-14 reflect noted XLP was **under-owned / washed-out** (RSI <40, below 50-day, no crowded-long fuel) — that condition is now **partially relieved** by the +1.69% rel day, so the reflex-bounce fuel is smaller. No index rebalance. No forced selling.

**6. Earnings/guidance or policy catalysts.** **FOMC 09-16 (tomorrow)** — the dominant pending binary. **CPI printed 09-11** (in-line; the 09-11 lesson: benign-CPI branch is a *relative negative* for a low-beta defensive). **UMich 47.8 / 1-yr inflation expectations 4.6%** — inflation-expectations backup. **OPEC+ holds quotas** — leaves oil tight. No XLP constituent earnings today.

## Divergence check

The **leading factor sum** (S0 + S1 + S2 + S3) is **negative-to-flat**: the rates/duration shock (10Y >5%, real yields +0.18 1m) is a direct headwind to a bond-proxy, oil >$103 is an input-cost negative, and internal breadth is failing (PG/CL/KO/PEP/COST friction). The **tape confirmation** (S4) is **flat-to-slightly-negative** (1d rel −0.25%, sub-threshold; premarket mid-pack −0.33%). These **agree** — no divergence flag. The one honest tension: the 3d/1w rel are *positive* (FTS catch-up), which is a **mean-reversion cushion**, not a fresh bid. I resolve it by scoring S4 at 0 (the 1d give-back is sub-threshold and the multi-horizon positive is already paid) and keeping S0 modestly negative for the duration channel. **No divergence flagged.**

## Scoring

- **S0_SHARED_MACRO = −0.5.** Rates-led risk-off into FOMC: 10Y >5% (highest since 2007), DGS30 5.35, DFII10 2.60 (+0.18 1m) — a **direct duration headwind** to a bond-proxy defensive, and the hawkish tail of tomorrow's FOMC compounds an already-active channel (09-04 asymmetric refinement). The theoretical FTS bid (risk-off + oil >$103 + Asia red) is **partially credited** but dampened: the live premarket is mid-pack, not best-of-eleven, and the 09-10 lesson caps the FTS credit when the panel is not maximal. Net modestly negative.
- **S1_SECTOR_FACTORS = −1.0.** Input-cost **spike** (oil >$103, freight/packaging) is the dominant live factor; ag relief is only mild (corn/wheat/soybeans −0.2 to −0.3%). MAP HEAT internal read is **negative** (PG drag on volume read-through, CL −2.72% w1, KO/PEP soft snack demand, COST price-hike friction, KR DOJ probe, STZ/TAP neg). Pricing power is **not** confirmed by any fresh same-day beat. Private-label structural HIT is carried, not fresh. Net negative.
- **S2_BREADTH = −0.5.** MAP HEAT shows **breadth failure**: the only clean up-tape is Farm Products (commodity-linked, not the defensive core); the core sleeves (HPC, Beverages, Confectioners, Grocery) are down/flat with red captains. No internal expansion.
- **S3_FLOWS_POSITIONING = 0.** No confirmed XLP inflow/outflow print. The 09-14 under-owned/washed-out condition is **partially relieved** by the +1.69% rel day, so the reflex-bounce fuel is smaller — but there is no fresh flow signal either way. Neutral.
- **S4_ETF_TAPE = 0.** 1d rel **−0.25%** is **sub-threshold** (|rel| < 0.5%) and is a give-back after a paid +1.69% FTS day — the 08-28 rule forbids copying it as a fresh negative. The 3d/1w rel positives are **already paid**. Confirmation only: flat.
- **MULTIPLIER = 0.9.** Modest conviction: FOMC pending tomorrow, magnitude historically misses (mag=0.4), and the sign is modest.
- **CONFIDENCE = 0.5.** Direction lean is mild-negative but the FOMC binary and the positive 3d/1w rel cushion cut conviction.
- **REGIME = risk_off.**

**Leading sum** = −0.5 − 1.0 − 0.5 + 0 = **−2.0**; × 0.9 = **−1.8** → **down / mild** (absolute), with a **flat-to-slightly-negative relative** lean (the duration channel hits XLP as a bond-proxy, but the FTS cushion limits relative underperformance).

**Direction: down. Magnitude: mild.** The absolute call is a modest decline driven by the duration shock + input-cost pressure + internal breadth failure; the relative call is flat-to-slightly-negative (XLP is a bond-proxy, not the clean haven today, but the 3d/1w FTS cushion and the risk-off tape limit the relative fade). This is the 08-18 template applied correctly: rising long-end yields + risk-off → **relative outperformance / flat-to-negative absolute** — here the absolute is mildly negative and the relative is roughly flat.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -0.5
S1_SECTOR_FACTORS: -1.0
S2_BREADTH: -0.5
S3_FLOWS_POSITIONING: 0.0
S4_ETF_TAPE: 0.0
MULTIPLIER: 0.9
CONFIDENCE: 0.5
REGIME: risk_off
DIVERGENCE_FLAGGED: false
PREDICTED_DIRECTION: down
PREDICTED_MAGNITUDE_BAND: mild
RELATIVE_LEAN: flat_to_slightly_negative
SECTOR_SCORES_END

HIT_GRID_BEGIN
Flight-to-safety relative strength vs cyclicals|PARTIAL|0.5|2026-09-15|https://www.cnbc.com/2026/09/15/
Risk-off tape / flight to safety|HIT|0.7|2026-09-15|https://www.reuters.com/markets/
Real yields rising|HIT|0.8|2026-09-15|https://www.federalreserve.gov/releases/h15/
Input cost spike without pricing power|HIT|0.7|2026-09-15|https://www.reuters.com/business/energy/
Input cost relief (ag, packaging, freight)|MISS|0.6|2026-09-15|https://www.barchart.com/futures
Pricing power held without volume collapse|MISS|0.5|2026-09-15|https://www.mapheat.local/
Sector breadth failure (ETF up, names flat)|HIT|0.6|2026-09-15|https://www.mapheat.local/
Sector rotation into defensives|PARTIAL|0.5|2026-09-15|https://finance.yahoo.com/quote/XLP/
Sector rotation out of defensives|PARTIAL|0.4|2026-09-15|https://finance.yahoo.com/quote/XLP/
Risk-on rotation away from defensives|MISS|0.6|2026-09-15|https://www.cnbc.com/markets/
Volume decline accelerating|PARTIAL|0.4|2026-09-15|https://www.mapheat.local/
Private-label share gain against brands|HIT|0.5|2026-09-15|https://www.circana.com/
Sector ETF inflow / relative volume spike|MISS|0.5|2026-09-15|https://etf.com/XLP
Sector ETF outflow / volume dry-up|MISS|0.5|2026-09-15|https://etf.com/XLP
Crowded long (extreme relative performance + valuation)|MISS|0.6|2026-09-15|https://finance.yahoo.com/quote/XLP/
Large-cap leadership inside sector|PARTIAL|0.4|2026-09-15|https://www.mapheat.local/
Low-beta leadership inside sector|HIT|0.5|2026-09-15|https://www.mapheat.local/
USD strengthening|HIT|0.5|2026-09-15|https://www.marketwatch.com/investing/index/dxy
HIT_GRID_END

HORIZON_3D: down/mild — the duration shock (10Y >5%) and FOMC (09-16) dominate; the 3d rel cushion (+0.94%) is already paid and likely to fade as the rates channel bites a bond-proxy.
HORIZON_1W: flat/mild — the FTS catch-up (1w rel +0.88%) and the risk-off tape provide a relative cushion, but the absolute is capped by the duration/input-cost headwinds; expect XLP to roughly match SPY with a mild negative absolute drift.
HORIZON_2W: flat/mild — post-FOMC resolution is the swing factor; if the hike is delivered and the 10Y stabilizes, XLP can grind flat-to-up on the defensive bid; if the 10Y keeps backing up, the bond-proxy drag persists.
HORIZON_1M: flat/mild — the 1m rel has converged to −0.19% (from −0.94%), so the deep-laggard descriptor is gone; the sector is now a roughly market-performing defensive with a modest duration overhang.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -0.5, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': -0.5, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': -5.0, 'divergence_flagged': False, 'total_score': -5.776, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.55, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': -0.2403, 'score': -1.442, 'legs': [{'leg': 'ES', 'pct': 0.34, 'w': 0.45}, {'leg': 'ZN', 'pct': -0.46, 'w': 0.4}, {'leg': 'PM:XLP', 'pct': -0.33, 'w': 0.7}]}, 'overlay_score': -3.375, 'overlay_raw': -3.375, 'index_carry': -0.959, 'general_total': -3.836, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 1.25, 'S3_FLOWS_POSITIONING': 1.0, 'S4_ETF_TAPE': 1.25}, 'llm_confidence': 0.5, 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': 1.44, 'w1': 0.52}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
