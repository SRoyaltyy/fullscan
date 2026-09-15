# Sector Prediction — Industrials — 2026-09-15

- ETF: **XLI**
- rubric: `00_grounding/sectors/industrials.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-0.617** (mult 0.9)
- regime: risk_off
- divergence_flagged: **True**
- engine: v2 · tape_anchor **3.492** (ES +0.34%, ER2 -0.73%, HG -0.76%, PM:XLI +0.81%) · index_carry **-0.959** (general -3.836) · llm_overlay **-3.15** (raw -3.15)

## Channel 1 sector ETF tape

```
ETF XLI vs SPY (yfinance, through 2026-09-15):
  1d: XLI -0.71% | SPY -0.49% | rel -0.22%
  3d: XLI -1.07% | SPY -0.09% | rel -0.98%
  1w: XLI -3.27% | SPY -1.15% | rel -2.11%
  1m: XLI -9.54% | SPY -2.48% | rel -7.06%
```

I have enough to cover all Channel 2 categories. Let me compile.

MEMORY_CONFIRM: Memory index unavailable this run (embedding metadata mismatch); used injected Industrials scoreboard + 08-11..09-15 sector logs. Rolling dir=0.4 / mag=0.2 (n=10); last 30 dir=0.316 / mag=0.158 (n=19). Last graded 09-14: predicted down/notable, actual XLI −1.4156% / SPY −0.4462% / rel −0.9694% — **dir HIT, mag HIT**. Prior: 09-11 up/mild vs +1.067% (dir HIT, mag HIT), 09-10 down/mild vs −0.72% (dir HIT, mag HIT), 09-09 flat/flat vs −1.51% (dir MISS — pipeline flat-overrode a correct narrative down call), 09-08 flat/flat vs −0.485% (dir MISS, same flattening error), 09-04 down/flat vs +0.41% (dir MISS, laggard-shield), 09-03 flat/flat vs +1.03% (dir MISS, ISM Services beat). **Governing today: 09-14 (A-category) — when the 09-10 decay rule applies (deep-oversold laggard, 1m rel ≤ −5%), the prior-day 1d rel is a DECAYING signal to be DISCOUNTED, not a dampener used to zero the laggard; score the persistent 1m laggard ONCE in S4 as −1; and convert a premarket sector gap materially worse than the index futures gap into the S4/magnitude call rather than leaving it in prose. 09-11 (NONE) — pending binary + unanimous flow confirmation → treat the binary as NEUTRAL, not a dampener; but pending binary + NON-unanimous flow → genuine two-sided event risk. 09-10 (NONE) — keep direction, temper the S4 relative-magnitude weight. 09-09 (A) — when the tape CONFIRMS the negative score, do NOT let sector_rs_veto/calendar_size_gate flatten the narrative's directional call. 09-04 laggard-shield — score the laggard ONCE, not in both S2 and S4. 08-27 — 1w/1m laggard → forbid up on non-holdings AHR. 08-18 — cap S1 at 0/+1, don't use GEV/ETN as a cushion. 08-11/08-12 supply-shock cap — verify live oil sign.** DO-INSTEAD: when score fights tape, cut conviction / prefer flat/mild — **NOT binding today**: the tape (1d rel −0.22%, 3d rel −0.98%, 1w rel −2.11%, 1m rel −7.06%) is negative on every horizon, so the 09-09 correction applies (emit the directional call, don't flatten). Open experiment (sector_industrials): keep direction, shrink confidence on modest |score| given mag=0.158.

## XLI near-session environment (not an SPX call)

Object is the **Sep 15 cash session for XLI**, not SPX and not a stock pick. Channel 1 numbers are used as given.

### 1. Shared macro as it hits Industrials — S0 = −1
This is a **risk-off, oil-spiking, 10Y-breakout tape with a live FOMC binary** — a continuation of 09-14's regime, now with a 5% 10Y headline.

- **Oil is UP hard on a live supply shock.** Channel 1: `CL=F +4.27% 1d`, `BZ=F +2.7% 1d`; Finviz WTI **$103.79 (+2.37%)**, Brent **$108.11 (+2.31%)**, heating oil **+3.09%**, gasoil **+2.68%**, RBOB **+1.92%** — the whole distillate complex is bid. News Judge #4: "US–Iran tanker war; Hormuz traffic impaired; Brent ~$107–109." This is the **08-11/08-12 trigger**, not the 08-13 trigger: the live session change is a **supply-driven crude spike**, not a demand/risk slide. For XLI, a +2.3–4.3% crude move is a **direct cost headwind** for transports, airlines, trucking, and manufacturers. Do **not** call oil flat.
- **Futures independently confirm risk-off.** Channel 1 Finviz: S&P **−0.54%**, Nasdaq **−0.62%**, Russell **−0.73%**, Dow **−0.71%**. The 08-21 reversal gate (ES/NQ ≥ +0.3%) is **OFF**. (The `ES=F premarket +0.34% / NQ=F +0.26%` lines conflict with the Finviz futures tape; the Finviz tape is the live read and it is uniformly red — I use the red tape and note the conflict, consistent with 09-15's own prior handling.)
- **Globals negative.** Asia composite **−0.72%** (Kospi −3.26%, Nikkei −0.81%, Shanghai −0.07%, Hang Seng +0.45%). Europe **−0.31%** (FTSE −0.37%, DAX −0.15%, CAC −0.34%, EuroStoxx50 −0.38%). Both sleeves lean negative — the negative read is confirmed, not outlier-driven.
- **Rates: 10Y breaches 5%, long end selling off live.** Channel 1: DGS30 **5.35**, DGS10 **4.96** (+0.19 1w, +0.28 1m), DFII10 **2.60** (+0.18 1w, +0.18 1m). Live futures: 10Y Note **−0.46%**, 30Y Bond **−0.93%**, Ultra Bond **−1.09%** (prices down = yields up) — the long end is selling off *today*, live. News Judge #1: "10-year Treasury yield breaches 5% — global bond selloff." 5-day 10Y–SPX corr **−0.151** (weakly negative — the yield-equity link is currently weak, so do not over-weight it as a same-session driver, but the live long-end selloff is a genuine duration drag).
- **Fed path: FOMC/SEP/Warsh is a PENDING binary.** News Judge #3: FOMC decision, SEP/dot plot, Warsh press conference — "unresolved policy binary that can reprice the whole hike path; outranks paid color until it prints." Channel 2: September hike odds have surged (Yahoo Finance: "FOMC September 2026 Odds of Rate Hike Surge Over 60%"; centralbank.watch shows 91% hike odds). Per the 09-11 lesson, a pending binary with **unanimous flow confirmation** is neutral, not a dampener — but here the flow is **not** unanimous (futures red, oil up, yields up), so the binary is a genuine two-sided event risk. Do **not** pre-score hawkish or dovish. Treat the Fed path as **mixed/contested**.
- **VIX 17.49 (+0.39 1d, +1.77 1w) with VIX/VIX3M 0.897 — contango (not backwardation).** VX futures **+3.85%**. HY OAS **2.71** (tight, +0.06 1d). EPU **215.48** (−202.54 1d — a violent *drop* in policy uncertainty, likely a data artifact). Not a credit-stress crash, but a genuine risk-off overlay on a cyclical.
- **USD +0.25%**, gold **−1.01%**, silver **−1.47%**, copper **−0.76%**, platinum **−1.67%**, palladium **−1.85%** — a broad industrial-metals fade, a direct read-through to machinery/electrical-equipment demand expectations.

**S0 = −1, regime risk_off.** Not −2: VIX is not a panic print, credit is tight (HY 2.71), no hard-data miss, and the 10Y–SPX corr is only −0.151. Not 0: oil is confirmed up +2.3–4.3% on a live supply shock, futures are ≤ −0.54%, real yields are rising across 1d/1w/1m, and the hawkish repricing is live-confirmed by gold/silver/USD. Oil counted **once here**, not again in S1.

### 2. Spine + secondary — S1 = −1 (capped)
**No fresh same-morning industrials print in hand.** August ISM manufacturing already printed **09-01**: PMI **54.6** (expansion, 8th month) vs July 55.6; new orders **53.7** (−3.0 pts). Still **expansion** — **not** an ISM-contraction HIT, but **slowing**. ISM Services printed **09-03** (55.4). Both in the tape. 08-18/08-27: **cap S1 at 0/+1** on the positive side; +2 is forbidden without same-morning confirmation.

- **Grid / electrical equipment backlog (AI power) — HIT, carried, but not a cushion.** Channel 2 confirms: GEV **$176B backlog**, **116 GW** gas equipment backlog/slot reservations, **>$5B** 2026 YTD Electrification data-center orders; Eaton Electrical Americas orders **+41%**, Electrical Global backlog **+103%**. MAP HEAT: Electrical Equipment & Parts **dir=up** (VRT, HUBB bid), Engineering & Construction **dir=up** (PWR/FIX backlogs, FLR $5B award), Building Products **dir=up** (TT/JCI on Carrier beat-and-raise). This is a genuine structural positive. **But 08-18: not a downside cushion and not a same-session raise** on an oil-shock/yield-breakout day — GEV/ETN can still roll. Scored once, as a partial offset.
- **Aerospace & defense — MIXED.** Channel 2: Boeing awarded a **$131.2B ceiling** IDIQ F-15 Eagle Crest contract (08-24) — a **ceiling**, not obligated, and already traded (08-24 faded BA). Sept 14 DLA contract is a **$16.2M** door-launcher award — immaterial. MAP HEAT: Aerospace & Defense **dir=flat, conv=low** (GE mixed — "GE deal drag", RTX w1 −4.1%). Do **not** cancel ISM (expansion) with one award, and do **not** treat geo as a fresh defense-order HIT.
- **Freight / trucking / rail — MIXED, negative-leaning.** Channel 2: Cass/AAR — "freight volumes remain mixed entering August… improving selectively"; "manufacturing rebound showing up in rail traffic"; stored cars declining (more equipment in service). Modest rail positive, trucking still soft. Oil +2.3–4.3% is a **direct fuel-cost headwind** for trucking/air freight. Not a same-morning recovery HIT.
- **Construction slowdown — HIT, carried.** Manufacturing construction off the 2025 peak; AI/nonres is the offset, not a broad build boom.
- **Copper −0.76%, aluminum −0.03%, iron ore −0.48%** — a broad industrial-metals fade, a direct read-through to machinery/electrical-equipment demand expectations.
- **Conglomerates — negative.** MAP HEAT: Conglomerates **dir=down, conv=medium** (MMM neg, HON neg, both red on the week). Consulting Services **OVERRIDE dir=down** (VRSK −5%, EFX VantageScore ruling, w1 −7.47% vs parent). Airlines **dir=down** (DAL/UAL Barclays target cuts). Farm & Heavy Machinery **dir=flat, conv=low** — CAT/DE w1 strength is "tape, not news; breadth 0.115 says the move is two mega-caps, not the industry."

Net: carried ISM expansion (slowing) + structural grid/AI-power + modest rail vs oil-cost headwind + metals fade + conglomerate/consulting/airline weakness + no fresh same-morning confirmation. **S1 = −1** (capped; the negatives are live and broad-based, the positives are carried/structural and explicitly not a cushion per 08-18).

### 3. Breadth — S2 = −1
XLI is a **deep, persistent laggard**. Channel 1 through 09-15: 1d rel **−0.22%**, 3d **−0.98%**, 1w **−2.11%**, 1m **−7.06%**. Negative on **every** horizon. MAP HEAT confirms the internal picture is not broad: Farm & Heavy Machinery breadth **0.115** ("two mega-caps, not the industry"), Conglomerates down, Consulting down, Airlines down. The only up-nested groups (Electrical Equipment, E&C, Building Products) are **SPX-led, not broad** ("SPLIT is SPX-led, not broad"). Score the lag **once** here (09-04 single-count discipline).

### 4. Flows — S3 = 0
Channel 2 flow search returned nothing material on XLI specifically (ETFdb/flow prints not returned this morning). Broader context: BofA clients bought US stocks at the 6th-fastest weekly pace since 2008 (09-09) — a market-wide inflow, not XLI-specific. Not a crowded long (1m rel **−7.06%**). **S3 = 0** — checked, nothing material.

### 5. ETF tape (confirmation only) — S4 = −1
Channel 1 through 09-15: 1d rel **−0.22%**, 3d **−0.98%**, 1w **−2.11%**, 1m **−7.06%**. Decisive multi-horizon underperformance. Per the **09-14 correction**, the persistent 1m laggard is the **level** signal and is scored **once** here as −1; the freshest 1d rel (−0.22%) is a **decaying** signal (09-10) and is **discounted, not used as a dampener**. Also per 09-14: the premarket sector gap is **materially better** than the index futures gap today (PM:XLI **+0.81%** vs ES +0.34% / Finviz S&P −0.54%) — this is a **positive** knowable-at-open relative signal and it **tempers** the magnitude band (it does not flip direction, since the Finviz futures tape is uniformly red and the sector's own multi-horizon tape is negative).

### 6. Catalysts / calendar
- **FOMC decision + SEP/dot plot + Warsh presser** — the dominant pending binary (News Judge #3). Two-sided; do not pre-score.
- **10Y >5% / global bond selloff** — live duration shock (News Judge #1).
- **US–Iran tanker war / Hormuz impaired / Brent ~$107–109** — live oil supply shock (News Judge #4).
- **AMD −5% on AI-slowdown calls; chipmaker weakness** — NDX/SOXX driver, not an XLI spine (News Judge #2).
- **UMich Sept prelim 47.8; 1-yr inflation expectations 4.6%** — inflation-expectations backup (News Judge #5).
- **AME $5.0B Indicor acquisition completed** — stale M&A (08-26), not a same-session catalyst.
- **BE (Bloom Energy)** target raised to $351 (Mizuho) — AI-power positive inside the electrical-equipment sleeve, but a single-name analyst action, not an ETF driver.

### Self-audit
- **Lens:** cyclical; rates and oil counted **once** in S0, not re-counted in S1.
- **Band:** **mild**, not notable — the premarket XLI gap (+0.81%) is *better* than the index futures gap, the 1d rel is only −0.22%, and the FOMC binary is unresolved. The 09-14 correction says convert the premarket gap into the call; here it argues for **tempering** magnitude, not extending it.
- **Skew:** GEV/ETN/BE do not drive the ETF call (08-18).
- **Same-shock:** oil counted once (S0); the 1m laggard counted once (S4, not S2+S4).
- **Single-ticker:** no single name drives the sector call.
- **08-27:** 1w/1m laggard → forbid up. Applied (no up call).
- **09-09:** tape confirms the negative lean on 3d/1w/1m → emit the directional call, do not flatten. Applied.
- **09-11:** pending binary + **non-unanimous** flow → genuine two-sided event risk, not neutral. Applied (confidence reduced).

**Divergence:** Leading factors (S0 −1 oil/yields/FOMC, S1 −1, S2 −1, S4 −1) sum negative, but the **premarket XLI gap (+0.81%) is positive and better than the index futures gap**, and the 1d rel is only −0.22%. This is a **genuine divergence** — the tape is not confirming a large down move. Per DO-INSTEAD, cut conviction / prefer **mild** over notable. Direction stays **down** (multi-horizon tape negative, macro overlay negative, 08-27 forbids up), magnitude capped at **mild**.

**Final call: down / mild.** Σ(S0..S4) = −1 + −1 + −1 + 0 + −1 = **−4.0**; ×mult 0.9 = **−3.6**. Direction down, band mild (divergence-flagged, confidence reduced for the pending FOMC binary and the positive premarket gap).

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -1
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -1
MULTIPLIER: 0.9
CONFIDENCE: 0.5
REGIME: risk_off
DIVERGENCE_FLAGGED: True
SECTOR_ETF: XLI
PREDICTED_DIRECTION: down
PREDICTED_MAGNITUDE_BAND: mild
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-off tape / flight to safety|HIT|0.80|2026-09-15|https://www.financialcontent.com
Real yields rising|HIT|0.82|2026-09-15|https://finance.yahoo.com/economy/policy/articles/fomc-september-2026-odds-rate-163505675.html
USD strengthening|HIT|0.60|2026-09-15|https://www.financialcontent.com
Sector breadth failure (ETF up, names flat)|HIT|0.65|2026-09-15|https://www.aar.org/rail-industry-overview/
Large-cap leadership inside sector|HIT|0.60|2026-09-15|https://www.aar.org/rail-industry-overview/
Sector ETF outflow / volume dry-up|PARTIAL|0.35|2026-09-15|https://seekingalpha.com
Crowded long (extreme relative performance + valuation)|NO|0.70|2026-09-15|https://www.cnbc.com/2026/07/26/
ISM manufacturing / new orders expansion|PARTIAL|0.55|2026-09-01|https://www.prnewswire.com/news-releases/manufacturing-pmi-at-54-6-august-2026-ism-manufacturing-pmi-report-302544000.html
Durable goods / CapEx upside|PARTIAL|0.45|2026-08-26|https://www.census.gov/manufacturing/m3/adv/
Grid / electrical equipment backlog (AI power)|HIT|0.85|2026-09-02|https://greentechlead.com/power/global-power-equipment-boom-2026-ge-vernova-siemens-energy-eaton-and-schneider-ride-ai-infrastructure-supercycle-55129
Aerospace & defense order / budget upside|PARTIAL|0.40|2026-08-24|https://www.reuters.com/business/aerospace-defense/boeing-awarded-contract-with-ceiling-value-1312-billion-f-15-program-2026-08-24/
Freight / trucking / rail volume recovery|PARTIAL|0.40|2026-08-28|https://www.actresearch.net/resources/blog/trucking-industry-forecast-for-2026
Reshoring / industrial policy funding|PARTIAL|0.35|2026-09-10|https://roboticstomorrow.com
ISM contraction|NO|0.75|2026-09-01|https://www.prnewswire.com/news-releases/manufacturing-pmi-at-54-6-august-2026-ism-manufacturing-pmi-report-302544000.html
CapEx cuts / order cancellation|NO|0.60|2026-09-15|https://www.financialcontent.com
Freight recession|PARTIAL|0.45|2026-08-28|https://www.actresearch.net/resources/blog/trucking-industry-forecast-for-2026
Construction slowdown|HIT|0.60|2026-09-01|https://www.census.gov/construction/c30/
Sector rotation out of industrials|HIT|0.65|2026-09-15|https://seekingalpha.com
HORIZON_3D: down|mild|0.50|2026-09-18
HORIZON_1W: down|mild|0.45|2026-09-22
HORIZON_2W: down|mild|0.40|2026-09-29
HORIZON_1M: flat|mild|0.35|2026-10-15
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -7.0, 'divergence_flagged': True, 'total_score': -0.617, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.425, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.582, 'score': 3.492, 'legs': [{'leg': 'ES', 'pct': 0.34, 'w': 0.8}, {'leg': 'ER2', 'pct': -0.73, 'w': 0.2}, {'leg': 'HG', 'pct': -0.76, 'w': 0.1}, {'leg': 'PM:XLI', 'pct': 0.81, 'w': 0.7}]}, 'overlay_score': -3.15, 'overlay_raw': -3.15, 'index_carry': -0.959, 'general_total': -3.836, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 0.5}, 'llm_confidence': 0.5, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
