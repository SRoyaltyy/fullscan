# Sector Prediction — Industrials — 2026-09-15

- ETF: **XLI**
- rubric: `00_grounding/sectors/industrials.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-5.881** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**
- engine: v2 · tape_anchor **-1.177** (ES +0.51%, ER2 -0.73%, HG -0.76%, PM:XLI -0.36%) · index_carry **-1.554** (general -6.215) · llm_overlay **-3.15** (raw -3.15)

## Channel 1 sector ETF tape

```
ETF XLI vs SPY (yfinance, through 2026-09-14):
  1d: XLI -1.42% | SPY -0.45% | rel -0.97%
  3d: XLI -1.08% | SPY -0.20% | rel -0.88%
  1w: XLI -3.05% | SPY -1.21% | rel -1.84%
  1m: XLI -8.54% | SPY -2.19% | rel -6.35%
```

MEMORY_CONFIRM: Memory index unavailable this run (embedding metadata mismatch); used injected Industrials scoreboard + 08-11..09-14 sector logs. Rolling dir=0.4 / mag=0.2 (n=10); last 30 dir=0.316 / mag=0.158 (n=19). Last graded 09-14: predicted down/notable, actual XLI −1.4156% / SPY −0.4462% / rel −0.9694% — **dir HIT, mag HIT** (pipeline down/notable; LLM overlay down/mild was the magnitude-light miss). Prior: 09-11 up/mild vs +1.067% (dir HIT, mag HIT), 09-10 down/mild vs −0.72% (dir HIT, mag HIT), 09-09 flat/flat vs −1.51% (dir MISS — pipeline flat-overrode a correct narrative down call), 09-08 flat/flat vs −0.485% (dir MISS, same flattening error), 09-04 down/flat vs +0.41% (dir MISS, laggard-shield), 09-03 flat/flat vs +1.03% (dir MISS, ISM Services beat). **Governing today: 09-14 (A-category) — when the 09-10 decay rule applies (deep-oversold laggard, 1m rel ≤ −5%), the prior-day 1d rel is a DECAYING signal to be DISCOUNTED, not a dampener used to zero the laggard; score the persistent 1m laggard ONCE in S4 as −1; and convert a premarket sector gap materially worse than the index futures gap into the S4/magnitude call rather than leaving it in prose. 09-11 (NONE) — pending binary + unanimous flow confirmation → treat the binary as NEUTRAL, not a dampener. 09-10 (NONE) — keep direction, temper the S4 relative-magnitude weight. 09-09 (A) — when the tape CONFIRMS the negative score, do NOT let sector_rs_veto/calendar_size_gate flatten the narrative's directional call. 09-04 laggard-shield — score the laggard ONCE, not in both S2 and S4. 08-27 — 1w/1m laggard → forbid up on non-holdings AHR. 08-18 — cap S1 at 0/+1, don't use GEV/ETN as a cushion. 08-11/08-12 supply-shock cap — verify live oil sign.** DO-INSTEAD: when score fights tape, cut conviction / prefer flat/mild — **NOT binding today**: the tape (1d rel −0.97%, 3d rel −0.88%, 1w rel −1.84%, 1m rel −6.35%) CONFIRMS the negative score on every horizon, so the 09-09 correction applies (emit the directional call, don't flatten). Open experiment (sector_industrials): keep direction, shrink confidence on modest |score| given mag=0.158.

## XLI near-session environment (not an SPX call)

Object is the **Sep 15 cash session for XLI**, not SPX and not a stock pick. Channel 1 numbers are used as given.

### 1. Shared macro as it hits Industrials — S0 = −1
This is a **risk-off, oil-spiking, yield-backing-up tape** — a continuation of 09-14's regime, not a reversal.

- **Oil is UP hard on a live supply shock.** Channel 1: `CL=F +2.55% 1d`; Finviz WTI **$103.79 (+2.37%)**, Brent **$108.11 (+2.31%)**, heating oil **+3.09%**, gasoil **+2.68%**, RBOB **+1.92%** — the whole distillate complex is bid. This is the **08-11/08-12 trigger**, not the 08-13 trigger: the live session change is a **supply-driven crude spike**, not a demand/risk slide. For XLI, a +2.3–2.6% crude move is a **direct cost headwind** for transports, airlines, trucking, and manufacturers. Do **not** call oil flat. (Note the `BZ=F −2.81% 1d` line is the prior-close sleeve and conflicts with the live Finviz Brent +2.31% and the WTI +2.55% — per the 08-11/08-12 rule, verify the oil sign from live evidence; the live tape is unambiguously up.)
- **Futures independently confirm risk-off.** Channel 1: ES **−0.54%**, NQ **−0.62%**, RTY **−0.73%**, DJIA **−0.71%**. The 08-21 reversal gate (ES/NQ ≥ +0.3%) is **OFF**. Note the `ES=F premarket +0.51% / NQ=F +0.64%` line is stale/conflicting with the Finviz futures tape; the Finviz tape (S&P −0.54%, Nasdaq −0.62%, Russell −0.73%, Dow −0.71%) is the live read and it is uniformly red. **XLI premarket −0.36%** — worse than ES −0.54%? No: XLI −0.36% is *better* than ES −0.54% on this snapshot, but the sector's own 1d/3d/1w/1m relative tape is negative on every horizon, so the premarket gap is not a relative positive.
- **Globals negative.** Asia composite **−0.66%** (Hang Seng −1.0%, Kospi −0.85%, ASX −0.88%, Shanghai −0.54%, Nikkei −0.01%). Europe **−0.44%** (FTSE −0.46%, DAX −0.42%, CAC −0.47%, EuroStoxx50 −0.41%). Both sleeves lean negative — the negative read is confirmed, not outlier-driven.
- **Rates: real yields rising, long end in the stress zone.** Channel 1: DGS30 **5.35**, DGS10 **4.96** (+0.19 1w, +0.28 1m), DFII10 **2.60** (+0.18 1w, +0.18 1m). The 1m moves are large — a **persistent** real-yield backup, not a one-day wiggle. Live futures: 10Y Note **−0.46%**, 30Y Bond **−0.93%**, Ultra Bond **−1.09%** (prices down = yields up) — the long end is selling off *today*, live. 5-day 10Y–SPX corr **−0.107** (weakly negative — the yield-equity link is currently weak, so do not over-weight it as a same-session driver, but the live long-end selloff is a genuine duration drag).
- **Fed path: the news judge reads DOVISH (gold surge on rate-cut bets), which conflicts with the hawkish Warsh repricing in the 09-14 tape.** News Judge #3: "Gold surge on Fed rate-cut bets lifts Barrick +8.2% — a regime-level signal that the market is pricing easing, not hikes." But Channel 1 live: **Gold −1.01%, Silver −1.47%, USD +0.25%** — gold is *down* today, not up. The Barrick +8.2% headline is a prior-session move. Do not one-way score the Fed path; the live metals/USD tape is mildly hawkish-leaning (USD up, gold down), and the long end is selling off. Treat the Fed path as **contested and unresolved** — do not encode it as fully paid in either direction.
- **VIX 17.56 (+0.46 1d, +1.84 1w) with VIX/VIX3M 1.123 — BACKWARDATION.** VX futures **+3.85%**. HY OAS **2.65** (tight, −0.05 1d). EPU **395.54** (+189.93 1d) — a violent policy-uncertainty spike. Not a credit-stress crash, but a genuine risk-off overlay on a cyclical.
- **Copper −0.76%, aluminum −0.03%, iron ore −0.48%, platinum −1.67%, palladium −1.85%** — a broad industrial-metals fade, a direct read-through to machinery/electrical-equipment demand expectations. News Judge #8: copper retreats from record highs on US refined-copper tariff uncertainty.

**S0 = −1, regime risk_off.** Not −2: VIX is not a panic print, credit is tight (HY 2.65), no hard-data miss, and the 10Y–SPX corr is only −0.107. Not 0: oil is confirmed up +2.3–2.6% on a live supply shock, futures are ≤ −0.54% across all four indices, real yields are rising across 1w/1m, and the long end is selling off live. Oil counted **once here**, not again in S1.

### 2. Spine + secondary — S1 = −1 (capped)
**No fresh same-morning industrials print in hand.** August ISM manufacturing already printed **09-01**: PMI **54.6** (expansion, 8th month) vs July 55.6; new orders **53.7** (−3.0 pts). Still **expansion** — **not** an ISM-contraction HIT, but **slowing**. ISM Services printed **09-03** (55.4). Both in the tape. 08-18/08-27: **cap S1 at 0/+1** on the positive side; +2 is forbidden without same-morning confirmation. Today the net is negative, so the cap is not the binding constraint.

- **Grid / electrical equipment backlog (AI power) — HIT, live and broadening.** News Judge #7: "GS AI-productivity call fuels AJG +5.4%; **FIX +11% on AI data-center backlog** — shows the AI trade is broadening into services/industrials, not just chips — a breadth signal." MAP HEAT: **Electrical Equipment & Parts dir=up conv=medium** (VRT:pos, HUBB:pos — SPX data-center/grid names bid); **Engineering & Construction dir=up conv=medium** (PWR:pos, FIX:pos, FLR:pos — "PWR/FIX backlogs and FLR's $5B award confirm the nested long"). Finviz: **BE (Bloom Energy)** target raised to **$351** from $242 (Mizuho), reiterate Outperform on stronger pricing and demand. This is a genuine, dated AI-power positive inside XLI's electrical-equipment/E&C sleeve. **But 08-18: not a downside cushion and not a same-session raise** — scored once, and it does not offset the macro drag.
- **Aerospace & defense — MIXED/flat.** MAP HEAT: **Aerospace & Defense dir=flat conv=low** — "SPX captains soft (GE deal drag, RTX w1 −4.1%); RUT names quiet — no clean A&D direction." Do **not** cancel ISM (expansion) with one award, and do **not** treat geo as a fresh defense-order HIT.
- **Freight / trucking / rail — NEGATIVE-leaning.** Oil >$103 is a direct **fuel-cost headwind** for trucking/air freight. MAP HEAT: **Airlines dir=down conv=low** (DAL:neg, UAL:neg — "SPX majors get Barclays target cuts and bleed"). Cass trucking still soft. Not a same-morning recovery HIT.
- **Construction slowdown — HIT, carried.** Manufacturing construction off the 2025 peak; AI/nonres is the offset, not a broad build boom.
- **Conglomerates — NEGATIVE.** MAP HEAT: **Conglomerates dir=down conv=medium** (MMM:neg, HON:neg — "SPX conglomerates MMM/HON both red on the week"). **Consulting Services OVERRIDE dir=down conv=medium** (VRSK:neg, EFX:neg — "w1 −7.47% vs parent, EFX hit by VantageScore ruling, VRSK −5% — nested short"). **Farm & Heavy Construction Machinery dir=flat conv=low** (CAT/DE w1 strength is tape, not news; breadth 0.115 says the move is two mega-caps, not the industry).
- **AME** completed the $5.0B Indicor acquisition — **stale M&A** (08-26), not a same-session HIT.

Net: carried ISM expansion (slowing) + structural/live grid-AI-power positive vs oil-cost headwind + conglomerate/consulting weakness + metals fade + mixed freight. **S1 = −1** (capped; no fresh same-morning confirmation, and the negative secondary factors — oil cost, conglomerates, consulting, metals — outweigh the grid positive).

### 3. Breadth — S2 = −1
XLI is a **deep, persistent laggard**. Channel 1 through 09-14: 1d rel **−0.97%**, 3d **−0.88%**, 1w **−1.84%**, 1m **−6.35%**. Negative on **every** horizon. MAP HEAT confirms the internal split: the AI-power/E&C sleeve is bid (VRT/HUBB/PWR/FIX), but the **conglomerates (MMM/HON) and consulting (VRSK/EFX) are red**, and the Farm & Heavy Construction Machinery breadth is **0.115** — "the move is two mega-caps, not the industry." That is **breadth failure**: a narrow AI-power carry masking a broad industrial lag. Score the lag **once** here (per 09-04/09-14: do not double-count the same laggard fact in both S2 and S4 — see S4 below).

### 4. Flows — S3 = 0
No fresh XLI flow print returned this morning (Channel 1 has no flow line; web search returned nothing material on XLI fund flows). Not a crowded long (1m rel **−6.35%**). Rotation has been out of industrials into tech/software (News Judge #1: chips→software rotation). **S3 = 0** — checked, nothing material.

### 5. ETF tape (confirmation only) — S4 = −1
Channel 1 through 09-14: 1d rel **−0.97%**, 3d **−0.88%**, 1w **−1.84%**, 1m **−6.35%**. Decisive negative across **all** horizons. Per the **09-14 correction**, the persistent 1m laggard is the level signal and must be scored **once** as a mild-to-moderate negative — and the prior-day 1d rel (−0.97%) is *not* a decaying signal here (it is negative and confirming, not a positive print to discount). Per 09-04/09-14, the laggard fact is scored **once**; I place it in **S4** (the tape component) and keep **S2** as the *breadth-failure* read (narrow AI-power carry vs broad industrial lag) — these are distinct facts, not a double-count. **S4 = −1.**

### 6. Catalysts / calendar
- **No scheduled high-impact US macro print identified for today** (no CPI/NFP/FOMC in the injected calendar; the 09-11 CPI already printed in line). Per the 09-03 lesson, a pending two-sided print would force the band to at least mild — but absent a pending print, the band is set by the component sum.
- **Dominant cross-sector driver: AI-pacing rotation (chips down, software up)** — News Judge #1. This is **XLK/IGV beta, not XLI beta** (08-27: do not map a non-holdings mega-cap/tech rotation into XLI S0). It does, however, confirm a **risk-off-for-cyclicals** tape.
- **Treasury yield pressure + chipmaker weakness drag indices** — News Judge #2, the macro wrapper. Yields are the transmission channel.
- **Copper retreat on US refined-copper tariff uncertainty** — News Judge #8, a genuine materials/industrial-demand negative.
- **BAC CEO soft Q3 outlook (−5%)** — News Judge #6, a financials item, not XLI.

### Self-audit
- **Lens:** cyclical; rates/oil counted once in S0, not re-counted in S1.
- **Band:** **mild**, not notable — futures are red but not ≤ −1%, no fresh hard-data miss, VIX is not a panic print, and the 10Y–SPX corr is weak (−0.107). The 09-14 pipeline called notable on a −1.42% day; today's setup is similar but the futures gap is smaller (ES −0.54% vs 09-14's −0.66%) and there is no fresh single-name smash (CAT was the 09-14 leader; today MAP HEAT says CAT/DE strength is "tape, not news").
- **Skew:** GEV/VRT/HUBB/PWR/FIX do **not** drive the ETF call (08-18: not a downside cushion).
- **Same-shock:** oil counted once (S0); the persistent laggard counted once (S4); breadth failure is a distinct fact (S2).
- **Single-ticker:** no single name drives the sector call.
- **08-27:** 1w/1m laggard → **forbid up**; applied (call is down).
- **09-09:** tape confirms the negative score → emit the directional call, do not flatten; applied.
- **09-14:** score the persistent laggard once in S4; convert the premarket gap into the call; applied.

**Divergence:** Leading factors (S0 −1, S1 −1, S2 −1, S4 −1) and the tape (negative on every horizon) **agree** — no divergence to flag. The only cross-current is the live AI-power/grid positive (S1 partial offset) and the weak 10Y–SPX corr, both of which argue for **mild**, not notable.

**Final call: down / mild.** Σ(S0..S4) = −4; ×mult 0.9 = −3.6. Direction down, magnitude mild (futures red but not ≤ −1%, no fresh hard-data miss, VIX not a panic print, weak yield-equity corr). Confidence 0.55 (open experiment: shrink confidence on modest |score|; mag accuracy is only 0.158).

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
Risk-off tape / flight to safety|HIT|0.75|2026-09-15|https://www.finviz.com/futures
Real yields rising|HIT|0.65|2026-09-15|https://fred.stlouisfed.org/series/DFII10
USD strengthening|HIT|0.55|2026-09-15|https://www.finviz.com/futures
Sector breadth failure (ETF up, names flat)|HIT|0.6|2026-09-15|https://www.finviz.com/map
Grid / electrical equipment backlog (AI power)|HIT|0.7|2026-09-15|https://www.finviz.com/news
Freight / trucking / rail volume recovery|MISS|0.5|2026-09-15|https://www.finviz.com/news
Construction slowdown|HIT|0.5|2026-09-15|https://www.census.gov/construction/c30/c30index.html
Sector rotation out of industrials|HIT|0.6|2026-09-15|https://www.finviz.com/news
ISM manufacturing / new orders expansion|PARTIAL|0.5|2026-09-01|https://www.ismworld.org/supply-management-news-and-reports/reports/ism-report-on-business/
Aerospace & defense order / budget upside|MISS|0.4|2026-09-15|https://www.finviz.com/map
Crowded long (extreme relative performance + valuation)|MISS|0.6|2026-09-15|https://www.finviz.com/etf/XLI
Sector ETF inflow / relative volume spike|MISS|0.4|2026-09-15|https://www.etf.com/XLI
HIT_GRID_END

HORIZON_3D: down/mild — the persistent 1m lag (−6.35%) plus a live oil supply shock and rising real yields keep the relative trend negative; the AI-power/grid sleeve is the only offset.
HORIZON_1W: down/mild — no fresh industrial catalyst in the calendar; the sector remains a funding source for the chips→software rotation, and oil >$103 is a cost headwind for transports.
HORIZON_2W: flat/mild — mean-reversion risk rises as the 1m lag approaches −8.5% and RSI is deeply oversold; a stabilizing 1d rel would be the first tell.
HORIZON_1M: flat/mild — the structural grid/AI-power backlog and reshoring policy support a base, but the ISM new-orders slowdown and construction drag cap upside until a fresh spine print confirms.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -7.0, 'divergence_flagged': False, 'total_score': -5.881, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.65, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': -0.1962, 'score': -1.177, 'legs': [{'leg': 'ES', 'pct': 0.51, 'w': 0.8}, {'leg': 'ER2', 'pct': -0.73, 'w': 0.2}, {'leg': 'HG', 'pct': -0.76, 'w': 0.1}, {'leg': 'PM:XLI', 'pct': -0.36, 'w': 0.7}]}, 'overlay_score': -3.15, 'overlay_raw': -3.15, 'index_carry': -1.554, 'general_total': -6.215, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 0.5}, 'llm_confidence': 0.55, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
