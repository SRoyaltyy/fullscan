# Sector Prediction — Industrials — 2026-09-14

- ETF: **XLI**
- rubric: `00_grounding/sectors/industrials.md`
- predicted_direction: **down**
- predicted_magnitude_band: **notable**
- total_score: **-11.954** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**
- engine: v2 · tape_anchor **-6.053** (ES -0.66%, ER2 -0.27%, HG -1.44%, PM:XLI -1.13%) · index_carry **-2.751** (general -11.002) · llm_overlay **-3.15** (raw -3.15)

## Channel 1 sector ETF tape

```
ETF XLI vs SPY (yfinance, through 2026-09-11):
  1d: XLI +1.07% | SPY +0.85% | rel +0.21%
  3d: XLI -1.18% | SPY -0.22% | rel -0.96%
  1w: XLI -1.25% | SPY -1.15% | rel -0.11%
  1m: XLI -7.27% | SPY -1.06% | rel -6.21%
```

MEMORY_CONFIRM: Memory index unavailable this run (embedding metadata mismatch); used injected Industrials scoreboard + 08-11..09-11 sector logs. Rolling dir=0.278 / mag=0.111 (n=18); last 10 dir=0.3 / mag=0.1. Last graded 09-11: predicted up/mild, actual XLI +1.067% / SPY +0.852% / rel +0.215% — **dir HIT, mag HIT**. Prior: 09-10 down/mild vs −0.72% (dir HIT, mag HIT), 09-09 flat/flat vs −1.51% (dir MISS — pipeline flat-overrode a correct narrative down call), 09-08 flat/flat vs −0.485% (dir MISS, same flattening error), 09-04 down/flat vs +0.41% (dir MISS, laggard-shield), 09-03 flat/flat vs +1.03% (dir MISS, ISM Services beat). **Governing today: 09-11 (NONE) — pending binary + unanimous flow confirmation → treat the binary as NEUTRAL, not a dampener; do not cap S0 solely because the event is pending. 09-10 (NONE) — deep-oversold laggard (RSI<30, 1m rel ≤ −5%) means the prior-day 1d rel is a DECAYING signal; keep direction, temper the S4 relative-magnitude weight. 09-09 (A) — when the tape CONFIRMS the negative score, do NOT let sector_rs_veto/calendar_size_gate flatten the narrative's directional call. 09-04 laggard-shield — score the laggard ONCE, not in both S2 and S4. 08-27 — 1w/1m laggard → forbid up on non-holdings AHR. 08-18 — cap S1 at 0/+1, don't use GEV/ETN as a cushion. 08-11/08-12 supply-shock cap — verify live oil sign.** DO-INSTEAD: when score fights tape, cut conviction / prefer flat/mild — **BINDING today**: the tape (1d rel +0.21% positive, 3d rel −0.96%, 1m rel −6.21%) is MIXED, not confirming, and the leading factors are negative while the 1d tape is positive. Open experiment (sector_industrials): keep direction, shrink confidence on modest |score| given mag=0.111.

## XLI near-session environment (not an SPX call)

Object is the **Sep 14 cash session for XLI**, not SPX and not a stock pick. Channel 1 numbers are used as given.

### 1. Shared macro as it hits Industrials — S0 = −1
This is a **risk-off, oil-spiking, hawkish-repricing tape** — the mirror image of 09-11's risk-on bounce.

- **Oil is UP hard on a live supply shock.** Channel 1: `CL=F +3.05% 1d`, `BZ=F +3.43% 1d`; Finviz WTI **$102.29 (+2.44%)**, Brent **$107.33 (+2.80%)**. This is the **08-11/08-12 trigger**, not the 08-13 trigger: the live session change is a **supply-driven crude spike**, not a demand/risk slide. For XLI, a +2.4% to +2.8% crude move is a **direct cost headwind** for transports, airlines, trucking, and manufacturers. Do **not** call oil flat.
- **Futures independently confirm risk-off.** Channel 1: ES **−0.66%**, NQ **−1.59%**, RTY **−0.27%**, DJIA **−0.15%**. The 08-21 reversal gate (ES/NQ ≥ +0.3%) is **OFF**. NQ is leading the decline (−1.59% vs ES −0.66%) — a tech/growth-led selloff, not a broad cyclical smash, but XLI premarket is **−1.13%**, worse than ES.
- **Globals mixed-to-negative.** Asia composite **−0.72%** (Nikkei −0.81%, **Kospi −3.26%**, Hang Seng +0.45%). Europe **−0.35%** (FTSE +0.54%, DAX −0.55%, CAC −0.58%, EuroStoxx50 −0.80%). Per 08-03, do not let a single Asia outlier set direction, but here Europe and US futures both lean negative — the negative read is confirmed, not outlier-driven.
- **Rates: real yields rising, long end in the stress zone.** Channel 1: DGS30 **5.37** (+0.09 1d, +0.10 1w, +0.13 1m), DGS10 **4.95** (+0.12 1d, +0.16 1w, +0.25 1m), DFII10 **2.55** (+0.09 1d, +0.10 1w, +0.12 1m). Note the 1m moves are large — this is a **persistent** real-yield backup, not a one-day wiggle. 5-day 10Y–SPX corr **−0.248** (negative but far less extreme than the −0.969 of 09-10 — the yield-equity link is currently weak, so do not over-weight it as a same-session driver).
- **Fed path hawkish and re-armed.** News Judge #1: Warsh's Jackson Hole comments lifted September hike odds; gold slid >3%. Channel 1 confirms: **Gold −1.26%, Silver −2.17%**, USD **+0.41%**. This is a genuine hawkish repricing, and it is the dominant rates/regime driver in the set. It is **not** fully stale — the gold/silver/USD confirmation is live in today's tape.
- **VIX 17.67 (+1.83 1d, +2.37 1w) with VIX/VIX3M 1.135 — BACKWARDATION.** VX futures **+5.69%**. HY OAS **2.70** (tight, +0.05 1w). EPU **725.88** (+451 1d) — a violent policy-uncertainty spike. Not a credit-stress crash, but a genuine risk-off overlay on a cyclical.
- **Copper −1.44%, aluminum −1.91%, iron ore −0.67%** — a broad industrial-metals fade, a direct read-through to machinery/electrical-equipment demand expectations.

**S0 = −1, regime risk_off.** Not −2: VIX is not a panic print, credit is tight (HY 2.70), no hard-data miss, and the 10Y–SPX corr is only −0.248. Not 0: oil is confirmed up +2.4–2.8% on a live supply shock, futures are ≤ −0.66%, real yields are rising across 1d/1w/1m, and the hawkish repricing is live-confirmed by gold/silver/USD. Oil counted **once here**, not again in S1.

### 2. Spine + secondary — S1 = −1 (capped)
**No fresh same-morning industrials print in hand.** August ISM manufacturing already printed **09-01**: PMI **54.6** (expansion, 8th month) vs July 55.6; new orders **53.7** (−3.0 pts). Still **expansion** — **not** an ISM-contraction HIT, but **slowing**. ISM Services printed **09-03** (55.4). Both in the tape. 08-18/08-27: **cap S1 at 0/+1** on the positive side; +2 is forbidden without same-morning confirmation.

- **Grid / electrical equipment backlog (AI power) — HIT, carried, but under pressure today.** GEV ~$176B RPO / 116 GW gas book remains structural. Finviz digest: **AME** completed the $5.0B Indicor acquisition (stale M&A, 08-26). But 08-18: **not** a downside cushion — and today the AI-power complex is being sold with the tech/growth tape (NQ −1.59%, XLK premarket −1.95%). Do not use GEV/ETN as a floor.
- **Aerospace & defense — MIXED.** The Iran/Houthi escalation is a defense-order narrative, but defense names have been volatile on this conflict. No fresh award today. Do **not** cancel the oil-cost headwind with a stale defense narrative.
- **Freight / trucking / rail — NEGATIVE-leaning, live.** Oil **+2.4% to +2.8%** is a direct **fuel-cost headwind** for trucking/air freight. Cass trucking still soft. This is the clearest live negative inside the book.
- **Construction slowdown — HIT, carried.** Manufacturing construction off the 2025 peak; AI/nonres is the offset, not a broad build boom.
- **Copper −1.44% / aluminum −1.91%** — a **global-growth/industrial-demand negative**, a direct read-through to electrical equipment/machinery demand expectations.
- **Reshoring / industrial policy — no fresh catalyst today.**

Net: carried ISM expansion (slowing) + structural grid vs **live oil-cost headwind + industrial-metals fade + mixed freight + no fresh same-morning confirmation**. The live inputs are negative-leaning. **S1 = −1** (capped; the negative is the live oil/metals transmission, not a fresh sector print).

### 3. Breadth — S2 = 0
XLI is a **deep medium-term laggard** (1m rel **−6.21%**), but the **1d tape is positive** (rel **+0.21%**) after 09-11's bounce. Per **09-04 laggard-shield** and **09-10 decay**, score the laggard **ONCE** — and I am scoring it in S4, not here. There is **no fresh same-day constituent/breadth data** available (no premarket breadth print for XLI names beyond the ETF itself). Per the standing rule that a single trailing rel print may anchor **at most one** component, and per 09-04's explicit instruction not to double-count the laggard in S2 and S4, **S2 = 0**.

### 4. Flows — S3 = 0
No flow data returned this morning. Checked, nothing material. Not a crowded long (1m rel **−6.21%**). Rotation has been out of industrials into tech/AI-power, and today's tape is a tech-led selloff, which is not a rotation *into* industrials. **S3 = 0.**

### 5. ETF tape (confirmation only) — S4 = 0
Channel 1 through 09-11: 1d rel **+0.21%** (positive), 3d rel **−0.96%**, 1w rel **−0.11%**, 1m rel **−6.21%**. The **freshest** print (1d) is **positive** — the sector bounced with the market on 09-11. Per **09-10 decay** (deep-oversold laggard, RSI<30, 1m rel ≤ −5% → the prior-day 1d rel is a decaying signal, not a level signal) and per the rule that S4 is **confirmation only**, the mixed tape does **not** confirm a strong down move. The laggard fact is scored **once**, here, as a **mild negative** — but the positive 1d rel and the mixed 3d/1w mean this is **not** a decisive negative tape. **S4 = 0** (the laggard is captured in the S0/S1 negative lean and the mixed tape does not add an independent confirmation point).

### 6. Catalysts / calendar
- **Warsh Jackson Hole hawkish repricing** — dominant rates/regime driver, live-confirmed by gold −1.26% / silver −2.17% / USD +0.41%.
- **Oil +2.4–2.8% supply spike** — direct cost headwind for the transport/manufacturer sleeve.
- **No CPI/NFP/FOMC print today** (News Judge: "Set is thin on hard macro data"). The dominant driver is the **carried Warsh repricing**, not a pending binary. This means the **09-11 "pending binary = neutral" rule does NOT apply** — there is no binary to be neutral about, and the flow evidence (futures −0.66%/−1.59%, oil up, real yields up) is **unanimously negative**, not unanimous-positive.
- **AME/Indicor $5.0B close** — stale M&A (08-26), not a same-session catalyst.
- **AAPL PT cut (BofA, $370)** — index-relevant sentiment, not an XLI spine.
- **SpaceX Nasdaq-100 re-weighting** — mechanical flow event for QQQ, not XLI.

### Self-audit
- **Lens:** cyclical; oil and rates scored in S0 only, not re-counted in S1 as a second macro vote (the S1 negative is the *sector-specific* fuel-cost/metals transmission, which is a distinct channel — but I have kept it at −1, not −2, to avoid over-stacking one shock).
- **Band:** **mild**, not notable. Futures are ≤ −0.66% but not a crash; VIX is not a panic print; credit is tight; the 10Y–SPX corr is only −0.248; and the sector's own 1d tape is **positive**. Per the flat-futures/magnitude discipline and the rolling mag=0.111, a modest |score| with a mixed tape caps at mild.
- **Skew:** GEV/ETN/AME do not drive the ETF call; no single ticker carries it.
- **Same-shock:** the oil spike is counted **once** in S0; the hawkish repricing is counted **once** in S0; the laggard is counted **once** in S4.
- **Single-ticker:** no single name drives the sector call.
- **08-27:** 1w/1m laggard → **forbid up**. Applied — direction is not up.
- **09-09:** the tape does **not** decisively confirm the negative score (1d rel is **positive**), so the 09-09 "emit the directional call, don't flatten" correction is **not** triggered in its strong form. But the leading factors (S0 −1, S1 −1) are negative and the futures confirm risk-off, so a **down** call is warranted rather than flat.
- **DO-INSTEAD (binding):** score sign (negative) **conflicts** with the freshest sector tape (1d rel +0.21% positive). Per the rule, **cut conviction** — hence **down/mild**, not down/notable, and confidence held at moderate-low.

**Divergence:** Leading factors (S0 −1 oil/rates risk-off, S1 −1 fuel-cost/metals transmission) point **down**, and futures confirm (ES −0.66%, NQ −1.59%). But the sector's own freshest tape (1d rel **+0.21%**) is **positive** and the 3d/1w rel are only mildly negative. This is a **genuine divergence** — the macro overlay is negative while the sector's own recent relative tape is not confirming. Per DO-INSTEAD, this cuts conviction and caps the band at **mild**. Direction stays **down** because the live, knowable-at-open inputs (oil +2.4–2.8%, futures −0.66%/−1.59%, real yields rising, XLI premarket −1.13%) are negative and the 08-21 reversal gate is off.

**Final call: down / mild.** Σ(S0..S4) = −1 + −1 + 0 + 0 + 0 = **−2.0**; × multiplier **0.9** = **−1.8** → down/mild. Confidence **0.52** (modest |score|, mixed tape, rolling mag=0.111, divergence flagged).

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.52
REGIME: risk_off
DIVERGENCE_FLAGGED: True
PREDICTED_DIRECTION: down
PREDICTED_MAGNITUDE_BAND: mild
TOTAL_SCORE: -1.8
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-off tape / flight to safety|HIT|0.75|2026-09-14|https://www.finviz.com/futures
Real yields rising|HIT|0.7|2026-09-14|https://fred.stlouisfed.org/series/DFII10
USD strengthening|HIT|0.6|2026-09-14|https://www.finviz.com/futures
Sector breadth failure (ETF up, names flat)|MISS|0.4|2026-09-14|
Sector ETF outflow / volume dry-up|UNKNOWN|0.3|2026-09-14|
Grid / electrical equipment backlog (AI power)|HIT|0.6|2026-09-14|https://www.finviz.com/news
Aerospace & defense order / budget upside|MISS|0.4|2026-09-14|
Freight / trucking / rail volume recovery|MISS|0.5|2026-09-14|
Construction slowdown|HIT|0.5|2026-09-14|
ISM manufacturing / new orders expansion|HIT|0.5|2026-09-14|https://www.ismworld.org/
Sector rotation out of industrials|HIT|0.5|2026-09-14|
Crowded long (extreme relative performance + valuation)|MISS|0.6|2026-09-14|
HORIZON_3D: down|mild|0.5|2026-09-17
HORIZON_1W: down|mild|0.45|2026-09-21
HORIZON_2W: flat|mild|0.4|2026-09-28
HORIZON_1M: flat|mild|0.4|2026-10-14
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': -5.0, 'divergence_flagged': False, 'total_score': -11.954, 'predicted_direction': 'down', 'predicted_magnitude_band': 'notable', 'confidence_score': 0.85, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': -1.0088, 'score': -6.053, 'legs': [{'leg': 'ES', 'pct': -0.66, 'w': 0.8}, {'leg': 'ER2', 'pct': -0.27, 'w': 0.2}, {'leg': 'HG', 'pct': -1.44, 'w': 0.1}, {'leg': 'PM:XLI', 'pct': -1.13, 'w': 0.7}]}, 'overlay_score': -3.15, 'overlay_raw': -3.15, 'index_carry': -2.751, 'general_total': -11.002, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 0.5}, 'llm_confidence': 0.52}
```
