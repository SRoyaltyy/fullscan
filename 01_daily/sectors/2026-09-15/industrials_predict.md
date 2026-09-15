# Sector Prediction — Industrials — 2026-09-15

- ETF: **XLI**
- rubric: `00_grounding/sectors/industrials.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-0.813** (mult 0.9)
- regime: risk_off
- divergence_flagged: **True**
- engine: v2 · tape_anchor **3.434** (ES +0.30%, ER2 -0.73%, HG -0.76%, PM:XLI +0.81%) · index_carry **-1.097** (general -4.389) · llm_overlay **-3.15** (raw -3.15)

## Channel 1 sector ETF tape

```
ETF XLI vs SPY (yfinance, through 2026-09-15):
  1d: XLI -0.74% | SPY -0.46% | rel -0.28%
  3d: XLI -1.10% | SPY -0.06% | rel -1.04%
  1w: XLI -3.29% | SPY -1.12% | rel -2.17%
  1m: XLI -9.56% | SPY -2.44% | rel -7.12%
```

MEMORY_CONFIRM: Memory index unavailable this run (embedding metadata mismatch); used injected Industrials scoreboard + 08-11..09-15 sector logs. Rolling dir=0.4 / mag=0.2 (n=10); last 30 dir=0.316 / mag=0.158 (n=19). Last graded 09-14: predicted down/notable, actual XLI −1.4156% / SPY −0.4462% / rel −0.9694% — **dir HIT, mag HIT** (pipeline down/notable; the LLM overlay's down/mild was the magnitude-light miss). Prior: 09-11 up/mild vs +1.067% (dir HIT, mag HIT), 09-10 down/mild vs −0.72% (dir HIT, mag HIT), 09-09 flat/flat vs −1.51% (dir MISS — pipeline flat-overrode a correct narrative down call), 09-08 flat/flat vs −0.485% (dir MISS, same flattening error), 09-04 down/flat vs +0.41% (dir MISS, laggard-shield), 09-03 flat/flat vs +1.03% (dir MISS, ISM Services beat). **Governing today: 09-14 (A-category) — when the 09-10 decay rule applies (deep-oversold laggard, 1m rel ≤ −5%), the prior-day 1d rel is a DECAYING signal to be DISCOUNTED, not a dampener used to zero the laggard; score the persistent 1m laggard ONCE in S4 as −1; and convert a premarket sector gap materially worse than the index futures gap into the S4/magnitude call rather than leaving it in prose. 09-11 (NONE) — pending binary + unanimous flow confirmation → treat the binary as NEUTRAL, not a dampener. 09-10 (NONE) — keep direction, temper the S4 relative-magnitude weight. 09-09 (A) — when the tape CONFIRMS the negative score, do NOT let sector_rs_veto/calendar_size_gate flatten the narrative's directional call. 09-04 laggard-shield — score the laggard ONCE, not in both S2 and S4. 08-27 — 1w/1m laggard → forbid up on non-holdings AHR. 08-18 — cap S1 at 0/+1, don't use GEV/ETN as a cushion. 08-11/08-12 supply-shock cap — verify live oil sign.** DO-INSTEAD: when score fights tape, cut conviction / prefer flat/mild — **NOT binding today**: the tape (1d rel −0.28%, 3d rel −1.04%, 1w rel −2.17%, 1m rel −7.12%) CONFIRMS the negative score on every horizon, so the 09-09 correction applies (emit the directional call, don't flatten). Open experiment (sector_industrials): keep direction, shrink confidence on modest |score| given mag=0.158.

## XLI near-session environment (not an SPX call)

Object is the **Sep 15 cash session for XLI**, not SPX and not a stock pick. Channel 1 numbers are used as given.

### 1. Shared macro as it hits Industrials — S0 = −1
This is a **risk-off, oil-spiking, yield-breakout tape** — a continuation of 09-14's regime, now with a 10Y >5% headline.

- **Oil is UP hard on a live supply shock.** Channel 1: `CL=F +3.24% 1d`; Finviz WTI **$103.79 (+2.37%)**, Brent **$108.11 (+2.31%)**, heating oil **+3.09%**, gasoil **+2.68%**, RBOB **+1.92%** — the whole distillate complex is bid. This is the **08-11/08-12 trigger**, not the 08-13 trigger: the live session change is a **supply-driven crude spike**, not a demand/risk slide. For XLI, a +2.3–3.2% crude move is a **direct cost headwind** for transports, airlines, trucking, and manufacturers. Do **not** call oil flat. (The `BZ=F −2.34% 1d` line is the prior-close sleeve and conflicts with live Finviz Brent +2.31% and WTI +2.37% — per the 08-11/08-12 rule, verify the oil sign from live evidence; the live tape is unambiguously up.)
- **Futures independently confirm risk-off.** Channel 1 Finviz: S&P **−0.54%**, Nasdaq **−0.62%**, Russell **−0.73%**, Dow **−0.71%**. The 08-21 reversal gate (ES/NQ ≥ +0.3%) is **OFF**. (The `ES=F premarket +0.3% / NQ=F +0.35%` lines are stale/conflicting with the Finviz futures tape; the Finviz tape is the live read and it is uniformly red.)
- **Globals negative.** Asia composite **−1.1%** (Kospi −3.26%, Hang Seng −1.0%, Nikkei −0.81%, Shanghai −0.54%). Europe **−0.39%** (FTSE −0.4%, DAX −0.21%, CAC −0.43%, EuroStoxx50 −0.5%). Both sleeves lean negative — the negative read is confirmed, not outlier-driven.
- **Rates: 10Y breaches 5%, long end selling off live.** Channel 1: DGS30 **5.35**, DGS10 **4.96** (+0.19 1w, +0.28 1m), DFII10 **2.60** (+0.18 1w, +0.18 1m). Live futures: 10Y Note **−0.46%**, 30Y Bond **−0.93%**, Ultra Bond **−1.09%** (prices down = yields up) — the long end is selling off *today*, live. News Judge #1: "10-year Treasury yield breaches 5% — global bond selloff." 5-day 10Y–SPX corr **−0.172** (weakly negative — the yield-equity link is currently weak, so do not over-weight it as a same-session driver, but the live long-end selloff is a genuine duration drag).
- **Fed path: FOMC/SEP/Warsh is a PENDING binary.** News Judge #2: FOMC decision, SEP/dot plot, Warsh press conference — "unresolved policy binary that can reprice the whole hike path; outranks paid color until it prints." News Judge #3: August CPI core 0.3% MoM locks in September hike. Per the 09-11 lesson, a pending binary with **unanimous flow confirmation** is neutral, not a dampener — but here the flow is **not** unanimous (futures red, oil up, yields up), so the binary is a genuine two-sided event risk. Do **not** pre-score hawkish or dovish. Treat the Fed path as **mixed/contested**.
- **VIX 17.75 (+0.65 1d, +2.03 1w) with VIX/VIX3M 0.901 — contango (not backwardation).** VX futures **+3.85%**. HY OAS **2.71** (tight, +0.06 1d). EPU **215.48** (−202.54 1d — a violent *drop* in policy uncertainty, likely a data artifact). Not a credit-stress crash, but a genuine risk-off overlay on a cyclical.
- **Copper −0.76%, aluminum −0.03%, iron ore −0.48%, platinum −1.67%, palladium −1.85%** — a broad industrial-metals fade, a direct read-through to machinery/electrical-equipment demand expectations. USD **+0.25%** (mild strengthening — a headwind for exporters/commodity-linked names).

**S0 = −1, regime risk_off.** Not −2: VIX is not a panic print (contango), credit is tight (HY 2.71), no hard-data miss, and the 10Y–SPX corr is only −0.172. Not 0: oil is confirmed up +2.3–3.2% on a live supply shock, futures are ≤ −0.54%, real yields are rising across 1d/1w/1m, and the 10Y has breached 5%. Oil counted **once here**, not again in S1.

### 2. Spine + secondary — S1 = −1 (capped)
**No fresh same-morning industrials print in hand.** August ISM manufacturing already printed **09-01**: PMI **54.6** (expansion, 8th month) vs July 55.6; new orders **53.7** (−3.0 pts). Still **expansion** — **not** an ISM-contraction HIT, but **slowing**. ISM Services printed **09-03** (55.4). Both in the tape. 08-18/08-27: **cap S1 at 0/+1** on the positive side; +2 is forbidden without same-morning confirmation.

- **Grid / electrical equipment backlog (AI power) — HIT, live.** Finviz digest: **BE (Bloom Energy)** — Mizuho raises PT to **$351** from $242, reiterates Outperform on stronger pricing and demand signals. MAP HEAT: **Electrical Equipment & Parts dir=up conv=medium** (VRT:pos, HUBB:pos — SPX data-center/grid names bid; SPLIT is SPX-led, not broad). **Engineering & Construction dir=up conv=medium** (PWR:pos, FIX:pos, FLR:pos — backlogs and FLR's $5B award confirm the nested long). GEV ~$176B RPO / 116 GW gas book remains structural. **But 08-18: not a downside cushion and not a same-session raise** — this is a genuine positive, scored once.
- **Aerospace & defense — MIXED/soft.** MAP HEAT: **Aerospace & Defense dir=flat conv=low** — "SPX captains soft (GE deal drag, RTX w1 −4.1%); RUT names quiet — no clean A&D direction." Do **not** cancel ISM (expansion) with one award, and do **not** treat geo as a fresh defense-order HIT.
- **Freight / trucking / rail — NEGATIVE-leaning.** Oil >$103 is a direct **fuel-cost headwind** for trucking/air freight. MAP HEAT: **Airlines dir=down conv=low** (DAL:neg, UAL:neg — Barclays target cuts; regionals hold on cheap jet fuel). Cass trucking still soft. Not a same-morning recovery HIT.
- **Construction slowdown — HIT, carried.** Manufacturing construction off the 2025 peak; AI/nonres is the offset, not a broad build boom. MAP HEAT: **Building Products & Equipment dir=up conv=medium** (TT:pos, JCI:pos on Carrier beat-and-raise read-through) — a partial offset.
- **Conglomerates — NEGATIVE.** MAP HEAT: **Conglomerates dir=down conv=medium** (MMM:neg, HON:neg both red on the week). **Consulting Services OVERRIDE dir=down conv=medium** (VRSK:neg, EFX:neg — w1 −7.47% vs parent, EFX hit by VantageScore ruling).
- **Farm & Heavy Construction Machinery — flat.** MAP HEAT: **dir=flat conv=low** (CAT:none, DE:none — "w1 strength is tape, not news; breadth 0.115 says the move is two mega-caps, not the industry").
- **AME** completed $5.0B Indicor acquisition (stale M&A, 08-26) — not a same-session catalyst.

Net: carried ISM expansion (slowing) + structural grid/AI-power + building-products offset vs oil-cost headwind + conglomerate weakness + consulting override + metals fade. **S1 = −1** (capped; no fresh same-morning confirmation, and the negatives — oil cost, conglomerates, consulting — are live and sector-specific).

### 3. Breadth — S2 = −1
XLI is a **deep, persistent laggard**. Channel 1 through 09-15: 1d rel **−0.28%**, 3d **−1.04%**, 1w **−2.17%**, 1m **−7.12%**. MAP HEAT confirms the internal split: Electrical Equipment is **SPX-led, not broad**; Farm & Heavy Machinery breadth **0.115** (two mega-caps, not the industry); Conglomerates and Consulting are red. Leadership is narrow (VRT/HUBB/PWR/FIX), not % of names expanding. Per 09-04, score the lag **once** — here in S2 (breadth failure), with S4 carrying the tape confirmation separately as the 09-14 correction requires.

### 4. Flows — S3 = 0
No fresh XLI flow print returned this morning (ETFdb/flow data not in Channel 1). Not a crowded long (1m rel **−7.12%** — deeply de-risked). Rotation has been out of industrials into tech/AI-power-adjacent names. **S3 = 0** — checked, nothing material.

### 5. ETF tape (confirmation only) — S4 = −1
Channel 1 through 09-15: 1d rel **−0.28%**, 3d **−1.04%**, 1w **−2.17%**, 1m **−7.12%**. Decisive negative on **every** horizon. Per the 09-14 correction: the persistent 1m laggard is the **level** signal and is scored **once** here as −1; the prior-day 1d rel (−0.28%) is a decaying signal to be discounted, not a dampener. The premarket XLI **+0.81%** vs ES **+0.3%** (stale sleeve) is not a relative positive — the live Finviz futures are red and XLI's own multi-horizon relative tape is negative on all four windows. Confirmation of underperformance, not an independent second thesis.

### 6. Catalysts / calendar
- **FOMC decision + SEP/dot plot + Warsh press conference** — the dominant pending binary (News Judge #2). Two-sided; do not pre-score.
- **10Y breaches 5% / global bond selloff** — live duration shock (News Judge #1).
- **August CPI core 0.3% MoM** — paid inflation print locking in a September hike (News Judge #3).
- **Chipmaker weakness / AMD −5% premarket** — an equity-sector shock (News Judge #4), but it is **XLK**, not XLI beta (08-27: do not map a non-holdings mega-cap move into XLI S0).
- **US-Iran tanker war; Hormuz impaired, Brent ~$107** — oil at war-premium levels (News Judge #6). Fresh-kinetic B1=−3 lessons **do not fire** (no same-overnight increment, futures not confirming ≥0.5% down), but the level is a growth/inflation tax on this book.
- **UMich Sept prelim 47.8; 1-yr inflation expectations 4.6%** — validates the hawkish rates cluster (News Judge #7).

### Self-audit
- **Lens:** cyclical; rates/oil counted once in S0, not re-counted in S1.
- **Band:** **mild**, not notable — futures are red but not ≤ −1%, no fresh hard-data miss, and the FOMC binary is pending (two-sided). The 09-14 pipeline called notable on a −1.42% day; today's setup is similar but the pending FOMC caps conviction.
- **Skew:** VRT/HUBB/PWR/FIX do not drive the ETF call (MAP HEAT: SPLIT is SPX-led, not broad).
- **Same-shock:** oil/yields counted once in S0; the laggard counted once in S2 and once in S4 (per 09-14, the level signal is scored in S4; breadth failure is a distinct fact).
- **Single-ticker:** BE PT raise and AME M&A do not carry the sector.
- **08-27:** 1w/1m laggard → forbid up. Applied.
- **09-09/09-14:** tape confirms the negative score on every horizon → emit the directional call, do not flatten; score the persistent laggard in S4 as −1.

**Divergence:** Leading factors (S0 −1 oil/yields, S1 −1 cost/conglomerates, S2 −1 breadth failure, S4 −1 tape) sum negative, and the tape confirms on every horizon — **no divergence**. The pipeline's `divergence_flagged=True` reflects the stale `ES=F +0.8%` sleeve fighting the live red Finviz futures; per the 08-11/08-12 rule, the live tape governs. Direction: **down**.

**Final call: down / mild.** Σ(S0..S4) = −4, ×mult 0.9 = −3.6. Direction down; magnitude mild (pending FOMC binary + no fresh hard-data miss caps the band below notable).

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -1
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -1
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: risk_off
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-off tape / flight to safety|HIT|0.85|2026-09-15|https://www.finviz.com/futures
Real yields rising|HIT|0.80|2026-09-15|https://www.cnbc.com/2026/09/15/treasury-yields.html
USD strengthening|HIT|0.55|2026-09-15|https://www.finviz.com/futures
Sector breadth failure (ETF up, names flat)|HIT|0.60|2026-09-15|https://www.finviz.com/map
Sector ETF outflow / volume dry-up|MISS|0.40|2026-09-15|
Grid / electrical equipment backlog (AI power)|HIT|0.70|2026-09-15|https://www.finviz.com/quote/BE
Aerospace & defense order / budget upside|MISS|0.45|2026-09-15|
Freight / trucking / rail volume recovery|MISS|0.50|2026-09-15|
Construction slowdown|HIT|0.55|2026-09-15|
ISM manufacturing / new orders expansion|PARTIAL|0.50|2026-09-01|https://www.ismworld.org/
Sector rotation out of industrials|HIT|0.60|2026-09-15|
Crowded long (extreme relative performance + valuation)|MISS|0.30|2026-09-15|
HORIZON_3D|down|0.55|2026-09-15|
HORIZON_1W|down|0.55|2026-09-15|
HORIZON_2W|flat|0.40|2026-09-15|
HORIZON_1M|flat|0.40|2026-09-15|
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -7.0, 'divergence_flagged': True, 'total_score': -0.813, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.433, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.5724, 'score': 3.434, 'legs': [{'leg': 'ES', 'pct': 0.3, 'w': 0.8}, {'leg': 'ER2', 'pct': -0.73, 'w': 0.2}, {'leg': 'HG', 'pct': -0.76, 'w': 0.1}, {'leg': 'PM:XLI', 'pct': 0.81, 'w': 0.7}]}, 'overlay_score': -3.15, 'overlay_raw': -3.15, 'index_carry': -1.097, 'general_total': -4.389, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 0.5}, 'llm_confidence': 0.55, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
