# Sector Prediction — Industrials — 2026-09-25

- news_mode: **on**
- ETF: **XLI**
- rubric: `00_grounding/sectors/industrials.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-5.994** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**
- engine: v2 · tape_anchor **-1.045** (ES +0.28%, ER2 +0.08%, HG +0.66%, PM:XLI -0.38%) · index_carry **0.676** (general 2.706) · llm_overlay **-5.625** (raw -5.625)

## Channel 1 sector ETF tape

```
ETF XLI vs SPY (yfinance, through 2026-09-24):
  1d: XLI -0.75% | SPY -0.08% | rel -0.66%
  3d: XLI -0.27% | SPY +0.72% | rel -1.00%
  1w: XLI +0.34% | SPY +1.99% | rel -1.65%
  1m: XLI -5.43% | SPY +0.74% | rel -6.16%
```

MEMORY_CONFIRM: Memory index unavailable this run (embedding metadata mismatch; `openclaw memory status --index` / `openclaw memory index --force` would rebuild). Used the injected Industrials scoreboard + 08-11..09-24 sector logs + in-prompt active-lesson pack only. Rolling dir=0.6 / mag=0.7 (n=10); last 30 dir=0.37 / mag=0.333 (n=27). Last graded 09-24: predicted down/mild vs XLI −0.7466% / SPY −0.08% / rel −0.66% — **dir HIT, mag HIT** (gap-down that stuck; the LLM overlay's flat/flat + beat-SPY lean was the miss). Prior: 09-23 flat/flat vs +0.0706% / SPY −0.7202% / rel +0.7908% (dir HIT, mag HIT, relative MISS unexpressed), 09-22 down/mild vs +0.171% (dir MISS, mag MISS — PM:XLI −0.75% died at the cash open), 09-21 flat/flat vs +0.112% / SPY +1.55% / rel −1.44% (dir MISS, mag HIT), 09-18 flat/flat vs +0.438% (dir MISS, mag MISS), 09-17 flat/flat vs +0.178% (dir MISS, mag HIT), 09-16 flat/flat HIT, 09-15 down/mild HIT, 09-14 down/notable HIT, 09-11 up/mild HIT, 09-10 down/mild HIT. **Governing today: 09-24 (A) — score-vs-tape flatten needs a cash-open tape or a worse-than-index PM gap that is still the open, NOT a flat PM quote; when |ES|/|NQ| are outside the 09-22 mixed ±0.5% band, derive absolute direction from the ES/NQ sign and do not let PM:XLI ≈ 0 or a 1–2 day rel bounce veto S0; 1m rel ≤ −5% stays a CONDITION, not a beat-SPY coupon; relative lean requires the index to be sold in the complex XLI is supposed to shield against (AI/duration/growth) — energy leadership is not that complex. 09-23 (C) — record an explicit RELATIVE-OUTPERFORMANCE lean when 1m rel ≤ −5% AND rotation-out CARRIED AND index leadership concentrated in a non-sector complex AND PM flat-to-slightly-negative, while keeping absolute flat. 09-22 (A) — mixed T+1 with |ES|/|NQ| inside ±0.5% is NOT a 09-21 tape; do not mint down/mild from MAP HEAT + 1m lag + a PM quote. 09-21 (A) — derive direction from ES/NQ sign, breadth from all four. 09-17/09-16 keep-flat on an unsigned post-paid-FOMC card. 08-27 — 1w/1m laggard forbids up. 08-18 — cap S1 at 0/+1; GEV/FIX/VRT must not raise or sink the ETF. 08-11/08-12 supply-shock cap — verify live oil sign. 09-16 — oil-down ≠ S1 trucking relief. 09-14 S4=−1 — needs live oil/duration + worse-than-index PM gap. 09-11 — four-index ≥+0.5% unanimous gate. 09-09 — emit-down needs a live supply shock with confirming negative tape. 09-10/09-04 — score the lag once, as a condition. Fed-speaker lesson — same-session voting-Fed remarks while hike odds are contested ⇒ keep S0 directionally 0, cut confidence, mark unresolved-policy event day.** DO-INSTEAD (sector_industrials, 09-21/09-22 losses): "when score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild" — **NOT binding as a flatten today**: the leading factors are net-negative (10Y >5.2%, real yields +0.13 1d, ES/NQ sleeve mixed-to-green but XLI PM −0.38% is the worst cyclical on the board) and the sector's own tape is negative on 3d/1w/1m — score and tape AGREE negative. Open experiment (`sector_industrials`): keep direction, shrink confidence on modest |score| — **applied**.

## XLI near-session environment (not an SPX call)

Object is the **Sep 25 cash session for XLI**, not SPX and not a stock pick. Channel 1 numbers used as given (tape through **2026-09-24**; do not re-derive). FOMC/SEP/Warsh **paid 09-16**. **Durable goods (Aug advance) prints 8:30 ET TODAY** — the sector's own spine print, live and two-sided. No CPI/NFP/FOMC binary.

### 1. Shared macro as it hits Industrials — S0 = −1

This is a **yields-spiking, hawkish-repricing, duration-shock tape** — a continuation of 09-24's regime, now with the 10Y through 5.2% and the real yield at 2.76.

- **The rate shock is live and confirmed, not stale.** News Judge #1 (ranked dominant): "US 10Y tops 5.2%; Treasury yields keep spiking as ES/NQ futures ease." #2: "NY Fed Williams: another hike by year-end is 'reasonable'; officials not done." #3: "Warsh/Jackson Hole lift September hike odds; gold slides >3%." Channel 1 confirms the transmission is current: **DGS10 5.11 (+0.15 1d, +0.41 1m)**, **DGS30 5.40 (+0.11 1d, +0.17 1m)**, **DFII10 2.76 (+0.13 1d, +0.38 1m)**. The 1m moves are large — this is a **persistent** real-yield backup, not a one-day wiggle. **5-day 10Y–SPX corr −0.958** — the yield-equity link is at its most extreme in the recent window, so the rate shock IS the same-session driver. This is a **regime** item (severity: regime, horizon 1w–1m).
- **Futures: the two sources disagree in sign, and the operator tape is green.** Channel 1 `ES=F premarket +0.28% / NQ=F premarket +0.57% vs prev close`. Finviz operator tape: S&P **+0.20%**, Nasdaq **+0.41%**, RTY **+0.08%**, DJIA **+0.11%**. Same conflict-handling as 09-15..09-24 — **do not re-derive**; I treat Finviz as the live operator quote page and the ES=F/NQ=F sleeve as the overnight gap. Both are now **green**, so per 09-21/09-22 the ES/NQ **sign** is positive — but the magnitude is **inside the 09-22 mixed ±0.5% band** on ES (+0.20% Finviz / +0.28% sleeve) and only marginally outside on NQ (+0.41% / +0.57%). 08-21's ES/NQ ≥ +0.3% gate is **partial** (NQ only). 09-11's unanimous ≥ +0.5% across all four is **off**. RTY +0.08% is not a cyclical bid. **An index rebound is not an XLI participation certificate** — and XLI's own PM is **−0.38%**, the worst cyclical on the board.
- **XLI is the worst cyclical on the injected PM board.** Channel 1: **PM:XLI −0.38%** vs XLK +0.79% / XLU +0.30% / XLF +0.09% / XLV −0.01% / XLP −0.12% / XLE −0.99% / ES Finviz +0.20%. That is a knowable-at-open **~58 bp** relative gap vs Finviz ES and ~66 bp vs the ES=F sleeve. 09-14's "convert the worse-than-index gap" clause fires on the *gap*; 09-14's oil/duration co-trigger **does** fire today (real yields +0.13 1d, 10Y >5.2%, corr −0.958) — so this is the **full 09-14 stack**, unlike 09-22/09-23 where the oil/duration leg was off.
- **Oil is DOWN on both reads — a cost LEVEL, not a squeeze.** Channel 1: `CL=F −1.71% 1d`, `BZ=F −7.41% 1d`; Finviz WTI **$104.16 (−1.59%)**, Brent **$107.67 (−1.02%)**. Absolute Brent is still elevated — a **cost LEVEL** for transports/manufacturers, not a same-session supply shock. News Judge: **no fresh kinetic/oil increment**. 08-11/08-12 **does not fire**. 08-13: Hormuz/tanker is the stale leg; live change is a pullback. Count oil **once, here**. Do **not** treat oil-down as trucking/air S1 relief (09-16), and do **not** treat ~$104 Finviz as a live squeeze.
- **Globals mixed, USD soft, vol low.** Asia composite **−0.06%** (Nikkei +1.3%, Kospi +1.04%, Hang Seng −1.01%, Shanghai −1.22%, ASX −0.43%). Europe **+0.64%** (FTSE +0.5%, DAX +0.85%, CAC +0.33%, EuroStoxx50 +0.88%). DXY **−0.02% / −0.2% 1d** — a mild exporter tailwind, not enough for S0 = +1. VIX **15.38 (−0.29 1d)** with VIX/VIX3M **0.835** (contango). HY OAS **2.73** (tight, +0.05 1d). EPU **99.37** (−17.5 1d, −151.93 1w) — policy uncertainty *falling*, not a stress spike. Not a credit-stress crash, but a genuine duration shock on a capex-heavy cyclical.
- **Fed path is contested and live later this morning.** CME-implied hike odds are contested (Williams says another hike "reasonable"; Warsh JH lifted September odds). Per the Fed-speaker lesson: do **not** encode the hike as fully paid; keep S0 **directionally 0 from the policy binary**; cut confidence; mark an unresolved-policy event day. But the *rate level and 1d change* are knowable at the open and they are hawkish — that is the S0 read, not the binary.

**S0 = −1, regime risk_off.** Not −2: VIX is 15.38 (low), credit is tight (HY 2.73), EPU is falling, futures are green, and there is no hard-data miss yet. Not 0: real yields are rising across 1d/1w/1m (+0.13/+0.08/+0.38), the 10Y is through 5.2%, the 10Y–SPX corr is −0.958 (the rate shock IS the driver), and XLI's own PM is the worst cyclical on the board. Oil counted **once here**, and it is a *level* cost, not a same-session shock.

### 2. Spine + secondary — S1 = −1 (capped)

**Durable goods (Aug advance) prints 8:30 ET TODAY — the sector's own spine print, live and unscored until it prints.** Do **not** pre-score a beat or a miss.

- **Spine status: ISM manufacturing is in expansion but decelerating.** August ISM manufacturing printed **09-01**: PMI **54.6** vs July 55.6; new orders **53.7** (−3.0 pts). Still **expansion** — **not** an ISM-contraction HIT, but **slowing**. ISM Services printed **09-03** (55.4). Both in the tape. 08-18/08-27: **cap S1 at 0/+1** on the positive side; +2 is forbidden without same-morning confirmation. Durable goods is the fresh print that could confirm or falsify the deceleration — it is **unscored**.
- **MAP HEAT is decisively negative and broad-based inside the sector.** Nested overrides beat the parent ETF:
  - **SPLIT Electrical Equipment & Parts dir=down conv=high** — VRT's **−15.4% week** is the whole story: AI-cooling de-rate, breadth **0.109**. This is the 08-18 "GEV/FIX/VRT must not raise the ETF" clause firing in the *negative* direction — the AI-power/grid sleeve is de-rating, not cushioning.
  - **HEAT Engineering & Construction dir=down conv=medium** — PWR −4.4%, FIX post-earnings fade: "the AI-datacenter build trade is de-rating, not breaking."
  - **HEAT Conglomerates dir=down conv=medium** — HON carries the GE downgrade read-through; MMM index-drag.
  - **HEAT Aerospace & Defense dir=down conv=medium** — GE PT cut drags; RTX engine milestone is the only positive, so group stays net down. (08-18: do not cancel ISM weakness with a single defense award.)
  - **HEAT Building Products & Equipment dir=down conv=medium** — HVAC captains TT and JCI both down hard with no offsetting news.
  - **HEAT Farm & Heavy Construction Machinery dir=down conv=medium** — CAT **−4.2%** drags; DE holds green (two-captain split).
  - **HEAT Business Equipment & Supplies dir=down conv=low**; **Airports & Air Services dir=down conv=low**; **Airlines dir=flat conv=low**.
  - **Only positive: HEAT Consulting Services dir=up conv=medium** (VRSK, HURN, ICFI green; breadth 0.727) — a low-beta, non-cyclical sleeve, not the ETF's cyclical core.
- **Single-name color:** **AME completes $5.0B all-cash acquisition of Indicor Instrumentation** (Industrials digest) — a large-cap consolidator deploying cash, mildly constructive for the electrical/instrumentation complex but not an ETF-level driver. **BE (Bloom Energy): Oracle remains committed to 2.4 GW Project Jupiter despite force majeure** — AI-power demand intact, but BE is not an XLI top-weight and 08-18 forbids using it as a cushion.
- **Net:** the spine is decelerating (ISM new orders −3.0), the fresh spine print is unprinted, and the nested MAP HEAT is down across **eight of ten** sub-industries with the AI-power/grid sleeve (the sector's structural bull case) actively de-rating. **S1 = −1**, capped at −1 (not −2) because ISM is still in expansion and durable goods is unprinted.

### 3. Breadth / leadership inside the sector — S2 = −1

- **Breadth is failing, not expanding.** MAP HEAT shows 8/10 sub-industries down; the SPLIT Electrical Equipment breadth is **0.109** (near-total failure). The only green sleeve is Consulting Services (breadth 0.727) — a low-beta, non-cyclical corner.
- **Leadership is negative and concentrated in the de-rating complex.** VRT −15.4% w1, PWR −4.4%, CAT −4.2%, HON negative, TT/JCI down hard. This is **large-cap cyclical leadership to the downside**, not a healthy rotation.
- **Per 09-21/09-23:** "index rallies, my sector doesn't" is a signed relative signal. Here the index is *mildly* green (ES +0.20% Finviz) while XLI PM is **−0.38%** and the sector's own 3d/1w/1m rel are all negative (−1.00% / −1.65% / −6.16%). The sector is **not participating** in the green index tape — that is a signed negative relative weight, not a zero.
- **S2 = −1.** Not −2: the ETF itself is only −0.38% PM and the 1d rel (−0.66%) is a single-session print, not a collapse. Not 0: breadth is failing across the sector's cyclical core and the sector is absent from the green index tape.

### 4. Flows / positioning / crowding — S3 = −1

- **XLI is a deep multi-horizon relative laggard:** 1m rel **−6.16%**, 1w rel **−1.65%**, 3d rel **−1.00%**, 1d rel **−0.66%**. Per 09-04/09-10, score the laggard **ONCE** — I score it here in S3 as a positioning/flow condition, and I do **not** re-score it in S4 as a second copy.
- **The AI-power/grid sleeve is de-rating, which is a positioning unwind, not a fundamental break.** VRT −15.4% w1 with breadth 0.109 is a crowded-long unwind in the sector's most-crowded structural theme. Per the crowded-long lesson: a complex that has *just* de-risked into an easing macro overlay is a reflex-bounce setup — but the macro overlay here is **not** easing (real yields +0.13 1d, 10Y >5.2%), so the reflex-bounce precondition is **absent**. Score the unwind as a near-term flow negative.
- **No evidence of XLI ETF inflow or a relative-volume spike** in the injected data; the sector is being *sold* relative to the index, not accumulated.
- **S3 = −1.** Not −2: no forced-selling/index-exclusion event, HY OAS is tight (2.73), and the laggard status is a *condition* (09-10/09-22), not an accelerating flow. Not 0: the AI-power unwind is a live positioning negative and the multi-horizon lag is real.

### 5. ETF tape — S4 = −1 (confirmation only)

- **Channel 1 relative tape (through 09-24):** 1d rel **−0.66%**, 3d rel **−1.00%**, 1w rel **−1.65%**, 1m rel **−6.16%** — negative on **every** horizon.
- **PM:XLI −0.38%** is the worst cyclical on the board vs a green index tape — a knowable-at-open relative gap of ~58 bp vs Finviz ES.
- **Per 09-04/09-10/09-22:** the 1m lag is scored **once** (in S3), so S4 is scored on the **fresh same-session tape facts** only: the negative 1d rel (−0.66%), the negative 3d rel (−1.00%), and the worse-than-index PM gap. These are independent of the S3 lag score.
- **S4 = −1.** Confirmation only — it does not drive the thesis, but it does **confirm** the negative factor card rather than fight it. This is the key distinction from 09-22 (where the PM gap was the *only* negative and died at the open) and from 09-24 (where the LLM overlay wrongly flattened against a confirming tape).

### 6. Earnings / guidance / policy catalysts

- **Durable goods (Aug advance) 8:30 ET** — the sector's own spine print, live and unscored. A miss would confirm the ISM new-orders deceleration; a beat would falsify it. Do **not** pre-score.
- **Fed speakers:** Williams (year-end hike "reasonable"), Warsh JH hike-odds — **already printed**, not an unresolved same-morning binary. Per the Fed-speaker lesson, keep S0 directionally 0 *from the binary* and cut confidence; the rate *level* is the S0 read.
- **AME $5.0B Indicor acquisition** — large-cap consolidator M&A, mildly constructive for instrumentation, not ETF-level.
- **BE/Oracle Project Jupiter 2.4 GW** — AI-power demand intact, but BE is not an XLI driver (08-18).
- **No CPI/NFP/FOMC binary today.**

### Divergence check

Leading factor sum (S0 −1, S1 −1, S2 −1, S3 −1, S4 −1) = **−5** → net negative. Tape confirmation (S4) = **−1** → **confirms** the negative score. **No divergence.** This is the opposite of 09-22 (score negative, tape flat-to-positive → flatten) and 09-24 (score negative, LLM overlay flattened against a confirming tape). Per the 09-09 correction and the 09-24 lesson: when the tape **confirms** the negative score, emit the directional call — do **not** flatten.

### Self-audit

- **Lens:** XLI near-session environment, not SPX, not a stock pick. ✓
- **Band:** down/mild — the rate shock is live and confirmed, the sector's own tape is negative on every horizon, and the PM gap is worse-than-index. Not notable: VIX is low (15.38), credit is tight, futures are green, and durable goods is unprinted (a beat could flip the spine). Not flat: the full 09-14 stack (live duration shock + worse-than-index PM gap) is present, and the tape confirms. ✓
- **Skew:** the AI-power/grid sleeve is de-rating (VRT −15.4% w1) — this is the sector's structural bull case unwinding, which is a genuine negative, not a single-ticker event. ✓
- **Same-shock double-count:** oil counted **once** in S0 (as a cost level, not a squeeze); the 1m lag scored **once** in S3, not re-scored in S4; the rate shock counted **once** in S0, not re-scored in S1. ✓
- **Single-ticker:** CAT −4.2% and VRT −15.4% are MAP HEAT captains, not the ETF thesis — the breadth failure (8/10 sub-industries down) is the ETF-level driver. ✓
- **Open experiment:** keep direction, shrink confidence on modest |score| — applied (confidence 0.55). ✓

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -1
S2_BREADTH: -1
S3_FLOWS_POSITIONING: -1
S4_ETF_TAPE: -1
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: risk_off
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-off tape / flight to safety|HIT|0.70|2026-09-25|https://www.cnbc.com/2026/09/24/10-year-treasury-yield.html
Real yields rising|HIT|0.80|2026-09-25|https://www.morningstar.com/markets/global-bond-selloff-extends
USD weakening|PARTIAL|0.45|2026-09-25|https://www.finviz.com/
Sector breadth failure (ETF up, names flat)|HIT|0.75|2026-09-25|https://www.finviz.com/
Large-cap leadership inside sector|HIT|0.60|2026-09-25|https://www.finviz.com/
Sector ETF outflow / volume dry-up|PARTIAL|0.50|2026-09-25|https://www.finviz.com/
ISM manufacturing / new orders expansion|PARTIAL|0.55|2026-09-25|https://www.ismworld.org/
Durable goods / CapEx upside|MISS|0.50|2026-09-25|https://www.census.gov/economic-indicators/
Grid / electrical equipment backlog (AI power)|MISS|0.65|2026-09-25|https://www.finviz.com/
Aerospace & defense order / budget upside|MISS|0.55|2026-09-25|https://www.finviz.com/
Freight / trucking / rail volume recovery|MISS|0.50|2026-09-25|https://www.finviz.com/
CapEx cuts / order cancellation|PARTIAL|0.50|2026-09-25|https://www.finviz.com/
Construction slowdown|HIT|0.60|2026-09-25|https://www.finviz.com/
Sector rotation out of industrials|HIT|0.65|2026-09-25|https://www.finviz.com/
HORIZON_3D|down|0.55|2026-09-25|https://www.finviz.com/
HORIZON_1W|down|0.55|2026-09-25|https://www.finviz.com/
HORIZON_2W|down|0.50|2026-09-25|https://www.finviz.com/
HORIZON_1M|down|0.50|2026-09-25|https://www.finviz.com/
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': -1.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -7.0, 'divergence_flagged': False, 'total_score': -5.994, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.65, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': -0.1742, 'score': -1.045, 'legs': [{'leg': 'ES', 'pct': 0.28, 'w': 0.8}, {'leg': 'ER2', 'pct': 0.08, 'w': 0.2}, {'leg': 'HG', 'pct': 0.66, 'w': 0.1}, {'leg': 'PM:XLI', 'pct': -0.38, 'w': 0.7}]}, 'overlay_score': -5.625, 'overlay_raw': -5.625, 'index_carry': 0.676, 'general_total': 2.706, 'skill_multipliers': {'S0_SHARED_MACRO': 1.25, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 0.5}, 'llm_confidence': 0.55, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
