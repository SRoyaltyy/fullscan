# Sector Prediction — Industrials — 2026-09-15

- ETF: **XLI**
- rubric: `00_grounding/sectors/industrials.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-0.541** (mult 0.9)
- regime: risk_off
- divergence_flagged: **True**
- engine: v2 · tape_anchor **3.521** (ES +0.36%, ER2 -0.73%, HG -0.76%, PM:XLI +0.81%) · index_carry **-0.912** (general -3.65) · llm_overlay **-3.15** (raw -3.15)

## Channel 1 sector ETF tape

```
ETF XLI vs SPY (yfinance, through 2026-09-15):
  1d: XLI -0.45% | SPY -0.49% | rel +0.04%
  3d: XLI -0.81% | SPY -0.09% | rel -0.72%
  1w: XLI -3.01% | SPY -1.15% | rel -1.86%
  1m: XLI -9.30% | SPY -2.47% | rel -6.83%
```

MEMORY_CONFIRM: Memory index unavailable this run (embedding metadata mismatch); used injected Industrials scoreboard + 08-11..09-15 sector logs. Rolling dir=0.4 / mag=0.2 (n=10); last 30 dir=0.316 / mag=0.158 (n=19). Last graded 09-14: predicted down/notable, actual XLI −1.4156% / SPY −0.4462% / rel −0.9694% — **dir HIT, mag HIT** (pipeline down/notable; the LLM overlay's down/mild was the magnitude-light miss). Prior: 09-11 up/mild vs +1.067% (dir HIT, mag HIT), 09-10 down/mild vs −0.72% (dir HIT, mag HIT), 09-09 flat/flat vs −1.51% (dir MISS — pipeline flat-overrode a correct narrative down call), 09-08 flat/flat vs −0.485% (dir MISS, same flattening error), 09-04 down/flat vs +0.41% (dir MISS, laggard-shield), 09-03 flat/flat vs +1.03% (dir MISS, ISM Services beat). **Governing today: 09-14 (A-category) — when the 09-10 decay rule applies (deep-oversold laggard, 1m rel ≤ −5%), the prior-day 1d rel is a DECAYING signal to be DISCOUNTED, not a dampener used to zero the laggard; score the persistent 1m laggard ONCE in S4 as −1; and convert a premarket sector gap materially worse than the index futures gap into the S4/magnitude call rather than leaving it in prose. 09-11 (NONE) — pending binary + unanimous flow confirmation → treat the binary as NEUTRAL, not a dampener; but pending binary + NON-unanimous flow → genuine two-sided event risk. 09-10 (NONE) — keep direction, temper the S4 relative-magnitude weight. 09-09 (A) — when the tape CONFIRMS the negative score, do NOT let sector_rs_veto/calendar_size_gate flatten the narrative's directional call. 09-04 laggard-shield — score the laggard ONCE, not in both S2 and S4. 08-27 — 1w/1m laggard → forbid up on non-holdings AHR. 08-18 — cap S1 at 0/+1, don't use GEV/ETN as a cushion. 08-11/08-12 supply-shock cap — verify live oil sign.** DO-INSTEAD: when score fights tape, cut conviction / prefer flat/mild — **NOT binding today**: the tape (1d rel +0.04%, 3d rel −0.72%, 1w rel −1.86%, 1m rel −6.83%) is negative on 3d/1w/1m and only flat on 1d, so the 09-09 correction applies (emit the directional call, don't flatten). Open experiment (sector_industrials): keep direction, shrink confidence on modest |score| given mag=0.158.

## XLI near-session environment (not an SPX call)

Object is the **Sep 15 cash session for XLI**, not SPX and not a stock pick. Channel 1 numbers are used as given.

### 1. Shared macro as it hits Industrials — S0 = −1
This is a **risk-off, oil-spiking, yield-breakout tape** — a continuation of 09-14's regime, now with a 10Y ≥5% headline and a live FOMC binary.

- **Oil is UP hard on a live supply shock.** Channel 1: `CL=F +5.05% 1d`, `BZ=F +3.3% 1d`; Finviz WTI **$103.79 (+2.37%)**, Brent **$108.11 (+2.31%)**, heating oil **+3.09%**, gasoil **+2.68%**, RBOB **+1.92%** — the whole distillate complex is bid. This is the **08-11/08-12 trigger**, not the 08-13 trigger: the live session change is a **supply-driven crude spike**, not a demand/risk slide. For XLI, a +2.3–5.1% crude move is a **direct cost headwind** for transports, airlines, trucking, and manufacturers. Do **not** call oil flat.
- **Futures independently confirm risk-off.** Channel 1 Finviz: S&P **−0.54%**, Nasdaq **−0.62%**, Russell **−0.73%**, Dow **−0.71%**. The 08-21 reversal gate (ES/NQ ≥ +0.3%) is **OFF**. (The `ES=F premarket +0.36% / NQ=F +0.35%` lines conflict with the Finviz futures tape; the Finviz tape is the live read and it is uniformly red — I use the red tape and note the conflict.)
- **Globals negative.** Asia composite **−0.72%** (Kospi −3.26%, Nikkei −0.81%, Shanghai −0.07%, Hang Seng +0.45%). Europe **−0.31%** (FTSE −0.37%, DAX −0.15%, CAC −0.34%, EuroStoxx50 −0.38%). Both sleeves lean negative — the negative read is confirmed, not outlier-driven.
- **Rates: 10Y breaches 5%, long end selling off live.** Channel 1: DGS30 **5.35**, DGS10 **4.96** (+0.19 1w, +0.28 1m), DFII10 **2.60** (+0.18 1w, +0.18 1m). Live futures: 10Y Note **−0.46%**, 30Y Bond **−0.93%**, Ultra Bond **−1.09%** (prices down = yields up) — the long end is selling off *today*, live. News Judge #1: "10-year Treasury yield breaches 5% — global bond selloff." 5-day 10Y–SPX corr **−0.178** (weakly negative — the yield-equity link is currently weak, so do not over-weight it as a same-session driver, but the live long-end selloff is a genuine duration drag).
- **Fed path: FOMC/SEP/Warsh is a PENDING binary.** News Judge #2: FOMC decision, SEP/dot plot, Warsh press conference — "unresolved policy binary that can reprice the whole hike path; outranks paid color until it prints." News Judge #3: August CPI core 0.3% MoM locks in September hike. Per the 09-11 lesson, a pending binary with **unanimous flow confirmation** is neutral, not a dampener — but here the flow is **not** unanimous (futures red, oil up, yields up), so the binary is a genuine two-sided event risk. Do **not** pre-score hawkish or dovish. Treat the Fed path as **mixed/contested**.
- **VIX 17.55 (+0.45 1d, +1.83 1w) with VIX/VIX3M 0.898 — contango (not backwardation).** VX futures **+3.85%**. HY OAS **2.71** (tight, +0.06 1d). EPU **215.48** (−202.54 1d — a violent *drop* in policy uncertainty, likely a data artifact). Not a credit-stress crash, but a genuine risk-off overlay on a cyclical.
- **USD +0.25%**, gold **−1.01%**, silver **−1.47%**, copper **−0.76%**, platinum **−1.67%**, palladium **−1.85%** — a broad industrial-metals fade plus a firm dollar, a direct read-through to machinery/electrical-equipment demand expectations.

**S0 = −1, regime risk_off.** Not −2: VIX is not a panic print, credit is tight (HY 2.71), no hard-data miss, and the 10Y–SPX corr is only −0.178. Not 0: oil is confirmed up +2.3–5.1% on a live supply shock, futures are ≤ −0.54%, real yields are rising across 1d/1w/1m, and the long end is selling off live. Oil counted **once here**, not again in S1.

### 2. Spine + secondary — S1 = −1 (capped)
**No fresh same-morning industrials print in hand.** August ISM manufacturing already printed **09-01**: PMI **54.6** (expansion, 8th month) vs July 55.6; new orders **53.7** (−3.0 pts). Still **expansion** — **not** an ISM-contraction HIT, but **slowing**. ISM Services printed **09-03** (55.4). Both in the tape. 08-18/08-27: **cap S1 at 0/+1** on the positive side; +2 is forbidden without same-morning confirmation.

- **Grid / electrical equipment backlog (AI power) — HIT, live but split.** MAP HEAT **SPLIT Electrical Equipment & Parts dir=up conv=medium** (VRT:pos, HUBB:pos; RUT ENS/ATKR flat — "SPX-led, not broad"). Finviz: **BE (Bloom Energy)** target raised to **$351** from $242 (Mizuho, Outperform) on stronger pricing/demand. GEV ~$176B RPO / 116 GW gas book remains structural. **But 08-18: not a downside cushion and not a same-session raise** — on an oil-shock/rates-breakout day, GEV/ETN/VRT can still roll. Score once, positive, but capped.
- **Aerospace & defense — MIXED/soft.** MAP HEAT **Aerospace & Defense dir=flat conv=low** (GE:mixed, RTX:pos but w1 −4.1%; "no clean A&D direction"). The Iran/Hormuz escalation is a defense-order narrative, but defense names have been volatile on this conflict. Do **not** cancel ISM (expansion) with one award, and do **not** treat geo as a fresh defense-order HIT.
- **Freight / trucking / rail — NEGATIVE-leaning.** Oil +2.3–5.1% is a direct **fuel-cost headwind** for trucking/air freight. MAP HEAT **Airlines dir=down conv=low** (DAL:neg, UAL:neg — Barclays target cuts). Cass trucking still soft. Not a recovery HIT.
- **Construction slowdown — HIT, carried.** Manufacturing construction off the 2025 peak; AI/nonres is the offset, not a broad build boom.
- **Conglomerates — NEGATIVE.** MAP HEAT **Conglomerates dir=down conv=medium** (MMM:neg, HON:neg both red on the week). **Consulting Services OVERRIDE dir=down conv=medium** (VRSK:neg, EFX:neg; w1 −7.47% vs parent — nested short).
- **Farm & Heavy Machinery — flat.** MAP HEAT **dir=flat conv=low** (CAT/DE w1 strength is tape, not news; breadth 0.115 says the move is two mega-caps, not the industry).
- **Engineering & Construction — POSITIVE.** MAP HEAT **dir=up conv=medium** (PWR:pos, FIX:pos, FLR:pos — backlogs + FLR $5B award). **Building Products dir=up conv=medium** (TT:pos, JCI:pos on Carrier beat-and-raise read-through).
- **AME** completed the $5.0B Indicor acquisition (stale M&A, 08-26) — not a fresh catalyst.
- **Copper −0.76%, aluminum −0.03%, iron ore −0.48%** — a broad industrial-metals fade, a direct read-through to machinery/electrical-equipment demand expectations.

Net: carried ISM expansion (slowing) + structural grid/E&C/building-products positives vs oil-cost headwind + conglomerate/consulting weakness + metals fade + mixed A&D. **S1 = −1** (capped; the negatives — oil-cost, conglomerates, consulting override, metals — outweigh the split positives on a risk-off day, and no fresh same-morning confirmation exists to raise it).

### 3. Breadth — S2 = −1
XLI is a **deep multi-horizon laggard**. Channel 1 through 09-15: 1d rel **+0.04%** (flat), 3d **−0.72%**, 1w **−1.86%**, 1m **−6.83%**. MAP HEAT shows the sector is **internally split, not expanding**: Electrical Equipment is SPX-led not broad, Conglomerates down, Consulting OVERRIDE down, Airlines down, Farm & Heavy flat (breadth 0.115 — two mega-caps, not the industry). This is **breadth failure**, not expansion. Score the lag **once** here (09-04 discipline: do not double-count into S4).

### 4. Flows — S3 = 0
No fresh XLI flow print returned this morning (ETFdb/flow data not in Channel 1). Not a crowded long (1m rel **−6.83%** — deeply de-risked). No index rebalance/exclusion event identified. **S3 = 0** — checked, nothing material.

### 5. ETF tape (confirmation only) — S4 = −1
Channel 1 through 09-15: 1d rel **+0.04%** (flat), 3d **−0.72%**, 1w **−1.86%**, 1m **−6.83%**. Per the **09-14 correction**, the persistent 1m laggard is the **level signal** and must be scored **once** as a mild-to-moderate negative (S4 = −1), NOT zeroed by treating the freshest 1d rel (+0.04%) as a dampener. The 09-10 decay rule says the prior-day 1d rel is a *decaying* signal to be **discounted**, not used to cut conviction. Also note the premarket gap: **PM:XLI +0.81%** vs ES +0.36% — XLI is gapping *up* relative to futures premarket, which is a mild positive offset to the persistent lag; I convert it into a **tempering of the S4 magnitude** (S4 = −1, not −2) rather than leaving it in prose. Confirmation of underperformance, not an independent second thesis.

### 6. Catalysts / calendar
- **FOMC decision + SEP/dot plot + Warsh press conference** — the dominant same-cycle binary, **pending**, two-sided. Do not pre-score hawkish/dovish.
- **10Y ≥5% global bond selloff** — live, in the tape (News Judge #1).
- **August CPI core 0.3% MoM** — locks in September hike (News Judge #2), already paid.
- **US-Iran tanker war / Hormuz impaired / Brent ~$106–108** — live oil risk premium (News Judge #5).
- **UMich Sept prelim 47.8; 1-yr inflation expectations 4.6%** — reinforces the hawkish rates regime (News Judge #6).
- **BAC CEO soft Q3 outlook, shares ~5% lower** — XLF/credit force, not XLI (News Judge #7).
- **AMD −5% premarket on AI-slowdown call; ADBE record Q3** — hardware-vs-software rotation, XLK, not XLI (News Judge #4, #8).

### Self-audit
- **Lens:** cyclical; rates/oil counted once in S0, not re-counted in S1.
- **Band:** **mild**, not notable — the tape is only flat on 1d, futures are red but not ≤ −1%, and the FOMC binary is pending (two-sided). Per the 09-03 lesson, a pending high-impact print means the band must be at least **mild**, not flat.
- **Skew:** GEV/BE/VRT do not drive the ETF call (08-18).
- **Same-shock:** oil counted once (S0); the persistent laggard counted once (S2), not double-counted in S4 (09-04).
- **Single-ticker:** CAT/DE w1 strength is two mega-caps, not the sector (MAP HEAT breadth 0.115) — not used to lift the call.
- **08-27:** 1w/1m laggard → forbid up. Applied.
- **09-14:** persistent 1m laggard scored once in S4 as −1; premarket gap converted into the call. Applied.
- **09-09:** tape confirms the negative score on 3d/1w/1m → emit the directional call, do not flatten. Applied.

**Divergence:** Leading factors (S0 −1 oil/rates, S1 −1, S2 −1, S4 −1) point down, but the 1d tape is flat (+0.04%) and XLI is gapping *up* premarket (+0.81% vs ES +0.36%). This is a **mild divergence** — the freshest tape is not confirming a strong down move. Per DO-INSTEAD, cut conviction / prefer **down/mild** (not down/notable). The pipeline's `divergence_flagged: True` is consistent with this read.

**Final call: down / mild.** Direction down (persistent laggard + risk-off macro + oil/rates headwinds), magnitude mild (flat 1d tape, positive premarket gap, pending two-sided FOMC binary). Confidence reduced to **0.50** per the open experiment (modest |score|, mag historically 0.158).

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -1
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -1
MULTIPLIER: 0.9
CONFIDENCE: 0.50
REGIME: risk_off
DIVERGENCE_FLAG: true
PREDICTED_DIRECTION: down
PREDICTED_MAGNITUDE_BAND: mild
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-off tape / flight to safety|HIT|0.80|2026-09-15|https://www.finviz.com/futures
Real yields rising|HIT|0.75|2026-09-15|https://www.finviz.com/rates
USD strengthening|HIT|0.60|2026-09-15|https://www.finviz.com/currencies
Sector breadth failure (ETF up, names flat)|HIT|0.65|2026-09-15|https://www.finviz.com/map
Large-cap leadership inside sector|HIT|0.55|2026-09-15|https://www.finviz.com/map
Sector ETF outflow / volume dry-up|PARTIAL|0.35|2026-09-15|
Grid / electrical equipment backlog (AI power)|HIT|0.60|2026-09-15|https://www.finviz.com/news
Aerospace & defense order / budget upside|PARTIAL|0.35|2026-09-15|
Freight / trucking / rail volume recovery|MISS|0.30|2026-09-15|
ISM manufacturing / new orders expansion|PARTIAL|0.45|2026-09-15|
Construction slowdown|HIT|0.55|2026-09-15|
CapEx cuts / order cancellation|PARTIAL|0.35|2026-09-15|
Sector rotation out of industrials|HIT|0.60|2026-09-15|
Crowded long (extreme relative performance + valuation)|MISS|0.20|2026-09-15|
HORIZON_3D: down|mild|0.45
HORIZON_1W: down|mild|0.42
HORIZON_2W: down|mild|0.40
HORIZON_1M: down|notable|0.38
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -7.0, 'divergence_flagged': True, 'total_score': -0.541, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.422, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.5868, 'score': 3.521, 'legs': [{'leg': 'ES', 'pct': 0.36, 'w': 0.8}, {'leg': 'ER2', 'pct': -0.73, 'w': 0.2}, {'leg': 'HG', 'pct': -0.76, 'w': 0.1}, {'leg': 'PM:XLI', 'pct': 0.81, 'w': 0.7}]}, 'overlay_score': -3.15, 'overlay_raw': -3.15, 'index_carry': -0.912, 'general_total': -3.65, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 0.5}, 'llm_confidence': 0.5, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
