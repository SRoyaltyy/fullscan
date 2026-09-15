# Sector Prediction — Consumer Defensive — 2026-09-15

- ETF: **XLP**
- rubric: `00_grounding/sectors/consumer_defensive.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **-5.038** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**
- engine: v2 · tape_anchor **-1.426** (ES +0.36%, ZN -0.46%, PM:XLP -0.33%) · index_carry **-0.912** (general -3.65) · llm_overlay **-2.7** (raw -2.7)

## Channel 1 sector ETF tape

```
ETF XLP vs SPY (yfinance, through 2026-09-15):
  1d: XLP -0.64% | SPY -0.51% | rel -0.13%
  3d: XLP +0.95% | SPY -0.11% | rel +1.06%
  1w: XLP -0.17% | SPY -1.17% | rel +1.00%
  1m: XLP -2.57% | SPY -2.49% | rel -0.08%
```

MEMORY_CONFIRM: Consumer Defensive / XLP only — memory index paused (embedding metadata mismatch); using injected sector scoreboard + last-10 logs. Rolling dir=0.5 / mag=0.4 (n=10); last-30 dir=0.474 / mag=0.368. Last graded 2026-09-14 predicted up/mild vs XLP +1.247% / SPY −0.446% / rel +1.694% (dir HIT, mag MISS — 09-14 reflect: the 08-18 duration cap was applied at full weight to an *under-owned* defensive with a live best-of-eleven premarket bid, suppressing the absolute band). 09-11 up/flat dir HIT / mag HIT absolute but a *relative* miss (benign-CPI branch is a relative negative for a low-beta defensive, not neutral). 09-10 down/flat dir MISS (food-crash dominance over-applied with no fresh print, zeroing a maximal FTS panel). 09-09 down/flat dir HIT / mag HIT (fresh food-crash, oil >$100). 09-08 flat/flat dir MISS (S0=+0.5 FTS bid overweighted). 09-04 down/mild dir HIT / mag HIT. 09-03 up/flat dir MISS (prior FTS day + two-sided ISM = FTS unwind). 08-28 down/mild dir MISS (leftover anti-FTS restacked). No open experiment tagged to this sector beyond the 08-28 DO-INSTEAD (prefer flat/mild when sign fights tape) and "keep direction, shrink confidence on modest |score|." Today I do **not** re-litigate stale WMT (08-20), do **not** copy 3d/1w/1m lag into S2/S4 as independent confirmation (08-28), do **not** treat Warsh as still two-sided (printed 08-28; path is live hawkish and FOMC is *tomorrow*), do **not** fire 08-27's down/notable gate (NQ +0.35% vs ES +0.36% — NQ is *not* leading by ≥0.5%), do **not** apply the 09-08/09-09 food-crash-dominance override at full weight (09-10 reflect capped it at one session and requires a fresh negative print — the CPB cut is 09-03, now T+8), do **not** convert the FTS bid into absolute up (08-18 utilities), and I **do** treat the 10Y>5% duration shock + FOMC binary + oil >$103 as **one** rates/risk-off regime object counted once.

---

# Consumer Defensive (XLP) — 2026-09-15

Object is the **near-session XLP environment**, not SPX and not a stock picker. Channel 1 numbers are used as given.

## Channel 1 tape (confirmation only)

```
ETF XLP vs SPY (yfinance, through 2026-09-15):
  1d: XLP -0.64% | SPY -0.51% | rel -0.13%
  3d: XLP +0.95% | SPY -0.11% | rel +1.06%
  1w: XLP -0.17% | SPY -1.17% | rel +1.00%
  1m: XLP -2.57% | SPY -2.49% | rel -0.08%
```

This is the **most constructive multi-horizon tape XLP has had in weeks**: 3d rel **+1.06%**, 1w rel **+1.00%**, 1m rel only **−0.08%** (vs −0.94% on 09-14). The 1d rel **−0.13%** is a *sub-threshold* give-back after yesterday's +1.69% FTS day — it is **not** a fresh negative signal (|1d rel| < 0.15% band; the 08-28 rule forbids copying a paid print into S2/S4). Critically, the **1m rel has converged to flat**, which means the "deep multi-horizon laggard" descriptor that anchored the 09-08/09-09/09-10 down calls is **no longer true**. The 09-14 catch-up has begun and is now 2 sessions old.

Macro panel as it maps here: **ES=F +0.36% / NQ=F +0.35%** (mildly green, NQ *not* leading — **not** 08-21 ES≥+0.3% *with* a sector catalyst, **not** 08-27 NQ≥+0.5% leading). Finviz cash futures **SPX −0.54% / NDX −0.62% / DJIA −0.71% / Russell −0.73%** — note the **sign conflict** between the two futures feeds; I treat the pre-fetched Channel 1 ES/NQ (+0.36/+0.35) as the live premarket read and the Finviz block as the prior-session close snapshot. **VIX 17.55 (+0.45 1d, +1.83 1w) with VIX/VIX3M 0.898 — CONTANGO** (the three-session backwardation of 09-08→09-14 has **resolved**; vol stress is *easing* in term structure even as spot VIX rises). **WTI $103.79 +2.37% / Brent $108.11 +2.31%** — oil **above $100**, war-premium levels, the strongest version of the Hormuz trigger. Gold **−1.01%**, silver **−1.47%**, copper **−0.76%**, platinum −1.67%, palladium −1.85% — **entire metals complex co-moving down with equities** (08-18 metals-co-move pattern), no metals floor. **DXY +0.25%** (USD firm). **10Y note −0.46% / 30Y bond −0.93% / Ultra Bond −1.09%** (bond prices down hard ⇒ **yields backing up sharply**); **DGS10 4.96 (+0.19 1w, +0.28 1m)**, **DGS30 5.35 (+0.10 1w)**, **DFII10 2.60 (+0.18 1w, +0.18 1m — real yields rising fast)**. Headline: **10Y breaches 5%, highest since 2007**. HY OAS **2.71 (+0.06 1d)**. 5-day 10Y–SPX corr **−0.178** (mildly negative — *not* the −0.9 regime of 09-08/09-10). Asia composite **−0.72%** (Kospi −3.26%, Nikkei −0.81%, Hang Seng +0.45%), Europe **−0.31%**. **Ag inputs mixed-to-soft**: corn −0.28%, soybeans −0.33%, wheat −0.17%, soybean meal −0.66%, sugar −0.28%, rough rice −0.06%; soybean oil +0.53%, coffee +1.68%, cocoa +1.04%, **orange juice +5.30%**, live cattle +1.17%, feeder cattle +1.61%, lean hogs −2.36%. **Sector premarket: XLP −0.33%** — mid-pack of eleven (XLI +0.81%, XLB +0.38%, XLU +0.20%, XLE +0.14%, XLK +0.11%, XLV +0.04%, XLF −0.08%, XLY −0.13%, XLRE −0.24%, XLP −0.33%, XLC −0.63%). Fear & Greed **UNAVAILABLE**. EPU **215.48 (−202.54 1d)** — policy-uncertainty spike *collapsing*.

## Channel 2 — required categories

**1. Shared macro → this sector.** Live tape is **rates-led risk-off into a binary**: 10Y >5% (highest since 2007), global bond selloff, oil >$103, Asia red (Kospi −3.26%), Europe soft, USD firm, metals down. News Judge #1: **10Y breaches 5% / global bond selloff** — the dominant rates/regime driver, already in the index tape. News Judge #2: **August CPI core surprise locking a September hike** — hard-data confirmation of the hike path. News Judge #3: **FOMC / SEP / Warsh press conference** — the unresolved same-cycle policy binary (tomorrow). News Judge #5: **US-Iran tanker war; Hormuz impaired; Brent ~$106–108** — oil/stagflation risk premium still on the tape. News Judge #6: **UMich Sept prelim 47.8; 1-yr inflation expectations jump to 4.6%** — consumer/inflation-expectations print reinforcing the hawkish rates regime.

For staples the map is **two-sided and must be counted once per channel**:
- **Rates-led risk-off + NQ lag + oil >$100 = theoretical relative FTS bid vs cyclicals** (sector layer: risk-off relative +). The 09-14 session proved XLP *can* hold a defensive bid under this regime (rel +1.69%).
- **But the 10Y >5% / real yields +0.18 1w is a duration headwind for a bond-proxy** (08-18: rising long-end + risk-off → relative outperformance / **flat-to-negative absolute**; do not upgrade to absolute up). This is the *strongest* version of that template — 10Y at a 2007 high, real yields rising fast.
- **Oil >$103 is itself an input-cost negative** for staples (freight, packaging, ag feedstocks) — count it in S1, **not** as a second S0 defensive bid.
- **Ag complex mixed-to-soft** (corn −0.28%, wheat −0.17%, soybeans −0.33%, soybean meal −0.66%) — a *mild* offsetting input-cost relief channel, but orange juice +5.30% and coffee +1.68% are the exceptions.
- **Metals all down** — no metals floor, 08-18 metals-co-move pattern live.
- **FOMC is tomorrow (09-16)** — a scheduled high-impact binary. Per the 08-12 bond-proxy rule and the a-scheduled-high-impact-macro-release lesson, do **not** pre-score a signed lean into the binary; the pre-binary tape (green ES/NQ, oil up, 10Y backing up) is knowable and must be scored, but the binary itself is unscored event risk.

08-11 (geo/oil → S0 negative, down/mild if already lagging) **fires on the oil/risk-off overlay**, but the 09-10 refinement caps the FTS credit: the sector-specific drag override is capped at one session and requires a fresh negative print. There is **no fresh food-crash print** today (CPB cut is T+8). The 09-14 reflect lesson says: when S3 says "under-owned, no crowded-long fuel" AND S0 says "live FTS bid," the 08-18 duration cap should be **relaxed**, not applied at full weight. But today the premarket board shows XLP **−0.33%** (mid-pack, *not* best-of-eleven as on 09-14), so the live FTS bid is **weaker** than yesterday. S0 carries the **duration vs FTS offset only**.

**2. Spine (mandatory).**
- **Flight-to-safety RS vs cyclicals (primary):** **PARTIAL / weakening.** 3d rel +1.06% and 1w rel +1.00% are constructive, but the 1d rel −0.13% is a give-back and the premarket board shows XLP mid-pack (−0.33%) rather than leading. The FTS bid that fired 09-14 is **not** confirmed this morning. Dampen: not a 08-18-style melt.
- **Risk-on rotation away from defensives:** **MISS.** NQ is not leading; futures are inside ±0.5%; the premarket board is mixed, not risk-on.
- **Pricing power held without volume collapse:** **PARTIAL / carried.** MAP HEAT: **Household & Personal Products dir=down** (PG:neg, CL −2.72% w1) and **Beverages-Non-Alcoholic dir=flat** (KO:mixed, PEP:neg on soft snack demand) — the pricing-power leg is **under pressure**, not holding. No fresh same-day staples beat.
- **Volume decline accelerating:** **PARTIAL / carried.** MAP HEAT **Grocery Stores dir=down** (KR:neg, DOJ beef-probe overhang), **Confectioners dir=down** (MDLZ shrinkflation headline risk), **Beverages-Brewers dir=down** (STZ:neg, TAP:neg). The volume/outlook read is soft across multiple sub-industries.

**3. Secondary.**
- **Input cost relief (ag, packaging, freight):** **PARTIAL.** Ag complex mixed-to-soft (corn/wheat/soybeans/meal all slightly lower) — a mild relief channel. But **oil >$103** (freight, packaging) and **orange juice +5.30%** offset it. Net: small positive.
- **Input cost spike without pricing power:** **HIT (oil sleeve).** Oil >$103 with PG/CL/KO/PEP pricing-power pressure is the classic squeeze. Counted once in S1.
- **Volume stabilization / sequential improvement:** **checked, nothing material and new.**
- **Staples earnings beat / stable margins:** **checked, nothing material for the ETF.** No WMT/PG/COST/KO print today. MAP HEAT captains are all mixed-to-negative.
- **Private-label share gain against brands:** **HIT (structural)** — carried, not a same-day print.
- **Sector rotation into/out of defensives:** **MIXED.** 3d/1w rel positive (rotation *into*), but 1d rel negative and premarket mid-pack (rotation *out*). Net ~0.

**4. Breadth / leadership inside the sector.** MAP HEAT is **predominantly negative**: Brewers down, Non-Alcoholic flat, Confectioners down, Education down, Food Distribution flat, Wineries down, Discount Stores flat, Grocery Stores down, HPC down. The **only clean up-tape is Farm Products** (ADM +3.37%, BG +2.94% w1, d1 +1.47% industry) — a commodity-input sub-industry, not a consumer-staples demand signal. Breadth inside the sector is **weak-to-mixed**, with the defensive core (HPC, grocery, confection) soft. This is a **breadth failure** relative to the 3d/1w relative strength — the ETF's relative bid is being carried by the index's weakness, not by internal leadership.

**5. Flows / positioning / crowding.** No confirmed XLP inflow print. The 09-14 reflect lesson established XLP is **under-owned / washed-out** (multi-horizon relative lag, RSI <40, below the 50-day, no crowded-long fuel) — but the 09-14 catch-up (+1.69% rel) has now partially *realized* that mean-reversion, so the marginal FTS upside is **smaller**. No crowded-long unwind fuel. S3 ≈ 0.

**6. Earnings/guidance or policy catalysts.** No XLP constituent earnings today. **FOMC tomorrow (09-16)** is the dominant scheduled binary — unscored event risk. **10Y >5%** is the live rates catalyst. **US-Iran/Hormuz** oil premium is live. **UMich inflation expectations 4.6%** reinforces the hawkish regime.

## Divergence check

The leading factor sum (S0 + S1 + S2 + S3) is **mildly negative** (duration/input-cost drag vs a weakening FTS bid), while the tape confirmation (S4) is **sub-threshold negative** (−0.13% 1d rel). The two **agree in sign** (both mildly negative), so no divergence flag is tripped. However, the honest read is: **flat-to-mildly-down absolute with a roughly neutral-to-slightly-negative relative edge** — the 3d/1w relative strength is real but the 1d give-back and mid-pack premarket board say the FTS bid is fading into the FOMC binary.

## Scoring

- **S0_SHARED_MACRO = −0.5.** Rates-led risk-off (10Y >5%, real yields +0.18 1w) is a duration headwind for a bond-proxy; oil >$103 is an input-cost negative; the FTS bid is *theoretical* and weaker than 09-14 (premarket mid-pack, not best-of-eleven). Not −1 (the 3d/1w relative strength and the 09-14 precedent show XLP *can* hold a defensive bid). Not 0 (the duration shock is the strongest version of the 08-18 template).
- **S1_SECTOR_FACTORS = −0.5.** Net of: FTS RS PARTIAL (+), input-cost relief PARTIAL (+), oil input-cost spike (−), pricing-power pressure (PG/CL/KO/PEP) (−), volume/outlook soft across grocery/confection/brewers (−), private-label structural (+). The negative cluster (pricing power + volume) slightly outweighs the positives.
- **S2_BREADTH = −0.5.** MAP HEAT is predominantly negative (HPC down, grocery down, confection down, brewers down, non-alc flat); only Farm Products up. Breadth failure relative to the ETF's 3d/1w relative strength.
- **S3_FLOWS_POSITIONING = 0.** No confirmed inflow/outflow print; under-owned but the mean-reversion has partially realized. No crowded-long fuel.
- **S4_ETF_TAPE = −0.25.** 1d rel −0.13% is sub-threshold and a give-back after a paid FTS day — confirmation only, not a fresh signal.

**MULTIPLIER = 0.9** (modest conviction; magnitude historically misses; FOMC binary tomorrow caps conviction).
**CONFIDENCE = 0.5.**
**REGIME = risk_off.**

Leading sum = −1.5; × 0.9 = **−1.35** → **flat-to-mildly-down**, direction **flat** (with a mild down bias), magnitude **flat**.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -0.5
S1_SECTOR_FACTORS: -0.5
S2_BREADTH: -0.5
S3_FLOWS_POSITIONING: 0.0
S4_ETF_TAPE: -0.25
MULTIPLIER: 0.9
CONFIDENCE: 0.5
REGIME: risk_off
SECTOR_SCORES_END

HIT_GRID_BEGIN
Flight-to-safety relative strength vs cyclicals|PARTIAL|0.5|2026-09-15|https://www.cnbc.com/2026/09/15/
Risk-off tape / flight to safety|HIT|0.6|2026-09-15|https://www.reuters.com/markets/
Real yields rising|HIT|0.8|2026-09-15|https://www.federalreserve.gov/releases/h15/
Input cost spike without pricing power|HIT|0.6|2026-09-15|https://www.reuters.com/business/energy/
Input cost relief (ag, packaging, freight)|PARTIAL|0.4|2026-09-15|https://www.barchart.com/futures
Pricing power held without volume collapse|MISS|0.5|2026-09-15|https://www.mapheat.local/
Volume decline accelerating|PARTIAL|0.4|2026-09-15|https://www.mapheat.local/
Sector breadth failure (ETF up, names flat)|HIT|0.5|2026-09-15|https://www.mapheat.local/
Sector rotation into defensives|PARTIAL|0.4|2026-09-15|https://www.mapheat.local/
Sector rotation out of defensives|PARTIAL|0.4|2026-09-15|https://www.mapheat.local/
Private-label share gain against brands|HIT|0.4|2026-09-15|https://www.circana.com/
Risk-on rotation away from defensives|MISS|0.5|2026-09-15|https://www.cnbc.com/2026/09/15/
Sector ETF inflow / relative volume spike|MISS|0.4|2026-09-15|https://www.etf.com/
Sector ETF outflow / volume dry-up|MISS|0.4|2026-09-15|https://www.etf.com/
Crowded long (extreme relative performance + valuation)|MISS|0.5|2026-09-15|https://www.mapheat.local/
HIT_GRID_END

HORIZON_3D: flat (mild down bias) — FOMC binary tomorrow is the dominant near-term driver; the 3d relative strength (+1.06%) is real but the duration shock caps absolute upside.
HORIZON_1W: flat-to-mildly-down — 10Y >5% and rising real yields are a persistent duration headwind for a bond-proxy; the FTS bid is weakening as the 09-14 catch-up realizes.
HORIZON_2W: flat — the sector's 1m relative has converged to flat (−0.08%); absent a fresh food-crash print or a dovish FOMC surprise, XLP is range-bound with a slight negative absolute tilt.
HORIZON_1M: flat-to-mildly-up — if the FOMC resolves dovish and the 10Y retreats from 5%, the under-owned, washed-out defensive can catch a relative bid; the structural private-label and pricing-power story remains intact.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -0.5, 'S1_SECTOR_FACTORS': -0.5, 'S2_BREADTH': -0.5, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -0.25}, 'multiplier': 0.9, 'leading_sum': -3.5, 'divergence_flagged': False, 'total_score': -5.038, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.55, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': -0.2376, 'score': -1.426, 'legs': [{'leg': 'ES', 'pct': 0.36, 'w': 0.45}, {'leg': 'ZN', 'pct': -0.46, 'w': 0.4}, {'leg': 'PM:XLP', 'pct': -0.33, 'w': 0.7}]}, 'overlay_score': -2.7, 'overlay_raw': -2.7, 'index_carry': -0.912, 'general_total': -3.65, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 1.25, 'S3_FLOWS_POSITIONING': 1.0, 'S4_ETF_TAPE': 1.25}, 'llm_confidence': 0.5, 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': 1.44, 'w1': 0.52}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
