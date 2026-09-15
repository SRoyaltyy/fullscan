# Sector Prediction — Industrials — 2026-09-15

- ETF: **XLI**
- rubric: `00_grounding/sectors/industrials.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-3.324** (mult 0.9)
- regime: risk_off
- divergence_flagged: **True**
- engine: v2 · tape_anchor **0.542** (ES +0.80%, ER2 -0.73%, HG -0.76%, PM:XLI -0.05%) · index_carry **-0.716** (general -2.865) · llm_overlay **-3.15** (raw -3.15)

## Channel 1 sector ETF tape

```
ETF XLI vs SPY (yfinance, through 2026-09-14):
  1d: XLI -1.42% | SPY -0.45% | rel -0.97%
  3d: XLI -1.08% | SPY -0.20% | rel -0.88%
  1w: XLI -3.05% | SPY -1.21% | rel -1.84%
  1m: XLI -8.54% | SPY -2.19% | rel -6.35%
```

MEMORY_CONFIRM: Memory index unavailable this run (embedding metadata mismatch); used injected Industrials scoreboard + 08-11..09-15 sector logs. Rolling dir=0.4 / mag=0.2 (n=10); last 30 dir=0.316 / mag=0.158 (n=19). Last graded 09-14: predicted down/notable, actual XLI −1.4156% / SPY −0.4462% / rel −0.9694% — **dir HIT, mag HIT** (pipeline down/notable; the LLM overlay's down/mild was the magnitude-light miss). Prior: 09-11 up/mild vs +1.067% (dir HIT, mag HIT), 09-10 down/mild vs −0.72% (dir HIT, mag HIT), 09-09 flat/flat vs −1.51% (dir MISS — pipeline flat-overrode a correct narrative down call), 09-08 flat/flat vs −0.485% (dir MISS, same flattening error), 09-04 down/flat vs +0.41% (dir MISS, laggard-shield), 09-03 flat/flat vs +1.03% (dir MISS, ISM Services beat). **Governing today: 09-14 (A-category) — when the 09-10 decay rule applies (deep-oversold laggard, 1m rel ≤ −5%), the prior-day 1d rel is a DECAYING signal to be DISCOUNTED, not a dampener used to zero the laggard; score the persistent 1m laggard ONCE in S4 as −1; and convert a premarket sector gap materially worse than the index futures gap into the S4/magnitude call rather than leaving it in prose. 09-11 (NONE) — pending binary + unanimous flow confirmation → treat the binary as NEUTRAL, not a dampener. 09-10 (NONE) — keep direction, temper the S4 relative-magnitude weight. 09-09 (A) — when the tape CONFIRMS the negative score, do NOT let sector_rs_veto/calendar_size_gate flatten the narrative's directional call. 09-04 laggard-shield — score the laggard ONCE, not in both S2 and S4. 08-27 — 1w/1m laggard → forbid up on non-holdings AHR. 08-18 — cap S1 at 0/+1, don't use GEV/ETN as a cushion. 08-11/08-12 supply-shock cap — verify live oil sign.** DO-INSTEAD: when score fights tape, cut conviction / prefer flat/mild — **NOT binding today**: the tape (1d rel −0.97%, 3d rel −0.88%, 1w rel −1.84%, 1m rel −6.35%) CONFIRMS the negative score on every horizon, so the 09-09 correction applies (emit the directional call, don't flatten). Open experiment (sector_industrials): keep direction, shrink confidence on modest |score| given mag=0.158.

## XLI near-session environment (not an SPX call)

Object is the **Sep 15 cash session for XLI**, not SPX and not a stock pick. Channel 1 numbers are used as given.

### 1. Shared macro as it hits Industrials — S0 = −1
This is a **risk-off, oil-spiking, yield-backing-up tape** — a continuation of 09-14's regime, not a reversal.

- **Oil is UP hard on a live supply shock.** Channel 1: `CL=F +1.4% 1d`; Finviz WTI **$103.79 (+2.37%)**, Brent **$108.11 (+2.31%)**, heating oil **+3.09%**, gasoil **+2.68%**, RBOB **+1.92%** — the whole distillate complex is bid. This is the **08-11/08-12 trigger**, not the 08-13 trigger: the live session change is a **supply-driven crude spike**, not a demand/risk slide. For XLI, a +2.3–2.6% crude move is a **direct cost headwind** for transports, airlines, trucking, and manufacturers. Do **not** call oil flat. (Note the `BZ=F −3.88% 1d` line is the prior-close sleeve and conflicts with the live Finviz Brent +2.31% and the WTI +2.55% — per the 08-11/08-12 rule, verify the oil sign from live evidence; the live tape is unambiguously up.)
- **Futures independently confirm risk-off.** Channel 1: ES **−0.54%**, NQ **−0.62%**, RTY **−0.73%**, DJIA **−0.71%**. The 08-21 reversal gate (ES/NQ ≥ +0.3%) is **OFF**. Note the `ES=F premarket +0.8% / NQ=F +0.95%` line is stale/conflicting with the Finviz futures tape; the Finviz tape (S&P −0.54%, Nasdaq −0.62%, Russell −0.73%, Dow −0.71%) is the live read and it is uniformly red. **XLI premarket −0.05%** — better than ES −0.54% on this snapshot, but the sector's own 1d/3d/1w/1m relative tape is negative on every horizon, so the premarket gap is not a relative positive.
- **Globals negative.** Asia composite **−0.66%** (Hang Seng −1.0%, Kospi −0.85%, ASX −0.88%, Shanghai −0.54%, Nikkei −0.01%). Europe **−0.18%** (FTSE −0.3%, DAX −0.09%, CAC −0.21%, EuroStoxx50 −0.12%). Both sleeves lean negative — the negative read is confirmed, not outlier-driven.
- **Rates: real yields rising, long end in the stress zone.** Channel 1: DGS30 **5.35**, DGS10 **4.96** (+0.19 1w, +0.28 1m), DFII10 **2.60** (+0.18 1w, +0.18 1m). The 1m moves are large — a **persistent** real-yield backup, not a one-day wiggle. Live futures: 10Y Note **−0.46%**, 30Y Bond **−0.93%**, Ultra Bond **−1.09%** (prices down = yields up) — the long end is selling off *today*, live. 5-day 10Y–SPX corr **−0.107** (weakly negative — the yield-equity link is currently weak, so do not over-weight it as a same-session driver, but the live long-end selloff is a genuine duration drag).
- **Fed path: the news judge reads DOVISH (gold surge on rate-cut bets), which conflicts with the hawkish Warsh repricing in the 09-14 tape.** News Judge #3: "Gold surge on Fed rate-cut bets lifts Barrick +8.2% — a regime-level signal that the market is pricing easing, not hikes." But Channel 1 live: **Gold −1.01%, Silver −1.47%, USD +0.25%** — gold is *down* today, not up. The Barrick +8.2% headline is a prior-session move. Do not one-way score the Fed path; the live metals/USD tape is mildly hawkish-leaning (USD up, gold down), and the long end is selling off. Treat the Fed path as **mixed/contested** — do not score it as a dovish tailwind.
- **VIX 17.05 (−0.05 1d, +1.33 1w) with VIX/VIX3M 1.093 — BACKWARDATION.** VX futures **+3.85%**. HY OAS **2.65** (tight, −0.05 1d). EPU **395.54** (+189.93 1d) — a violent policy-uncertainty spike. Not a credit-stress crash, but a genuine risk-off overlay on a cyclical.
- **Copper −0.76%, aluminum −0.03%, iron ore −0.48%, platinum −1.67%, palladium −1.85%** — a broad industrial-metals fade, a direct read-through to machinery/electrical-equipment demand expectations.

**S0 = −1, regime risk_off.** Not −2: VIX is not a panic print, credit is tight (HY 2.65), no hard-data miss, and the 10Y–SPX corr is only −0.107. Not 0: oil is confirmed up +2.3–2.6% on a live supply shock, futures are ≤ −0.54%, real yields are rising across 1d/1w/1m, and the long end is selling off live. Oil counted **once here**, not again in S1.

### 2. Spine + secondary — S1 = −1 (capped)
**No fresh same-morning industrials print in hand.** August ISM manufacturing already printed **09-01**: PMI **54.6** (expansion, 8th month) vs July 55.6; new orders **53.7** (−3.0 pts). Still **expansion** — **not** an ISM-contraction HIT, but **slowing**. ISM Services printed **09-03** (55.4). Both in the tape. 08-18/08-27: **cap S1 at 0/+1** on the positive side; +2 is forbidden without same-morning confirmation.

- **Grid / electrical equipment backlog (AI power) — HIT, carried, but SPLIT.** MAP HEAT: Electrical Equipment & Parts **dir=up conv=medium** (VRT:pos, HUBB:pos) — but explicitly flagged **SPLIT is SPX-led, not broad** (RUT ENS/ATKR flat). Engineering & Construction **dir=up conv=medium** (PWR:pos, FIX:pos, FLR:pos). Building Products **dir=up conv=medium** (TT:pos, JCI:pos on Carrier beat-and-raise read-through). Finviz: **BE (Bloom Energy)** target raised to **$351** from $242 (Mizuho, Outperform) on stronger pricing/demand. These are genuine positives inside XLI's electrical/grid sleeve. **But 08-18: not a downside cushion and not a same-session raise** — on an oil-shock, long-end-selling-off day, GEV/ETN/VRT can still roll, and the SPLIT flag says the bid is narrow.
- **Aerospace & defense — MIXED/soft.** MAP HEAT: **dir=flat conv=low** — "SPX captains soft (GE deal drag, RTX w1 −4.1%); RUT names quiet — no clean A&D direction." Do **not** treat geo as a fresh defense-order HIT; the sector's own captains are not confirming.
- **Freight / trucking / rail — NEGATIVE-leaning.** Oil **+2.3–2.6%** is a direct **fuel-cost headwind** for trucking/air freight. MAP HEAT Airlines **dir=down conv=low** (DAL:neg, UAL:neg — Barclays target cuts). Not a recovery HIT.
- **Conglomerates — NEGATIVE.** MAP HEAT: **dir=down conv=medium** (MMM:neg, HON:neg both red on the week). Consulting Services **OVERRIDE dir=down conv=medium** (w1 −7.47% vs parent; EFX hit by VantageScore ruling, VRSK −5%).
- **Farm & Heavy Construction Machinery — flat, breadth 0.115.** MAP HEAT explicitly: "CAT/DE w1 strength is tape, not news; **breadth 0.115 says the move is two mega-caps, not the industry**." This is a direct falsifier of any "CAT carries XLI up" thesis.
- **Construction slowdown — HIT, carried.** Manufacturing construction off the 2025 peak; AI/nonres is the offset, not a broad build boom.
- **AME** completed the $5.0B Indicor acquisition — **stale M&A** (08-26), not a same-session catalyst.

Net: carried ISM expansion (slowing) + narrow SPX-led grid bid vs oil-cost headwind + conglomerate weakness + consulting override + metals fade + mixed freight. **S1 = −1** (capped; the negatives are live and broad, the positives are narrow and carried).

### 3. Breadth — S2 = −1
XLI is a **deep, persistent laggard**. Channel 1 through 09-14: 1d rel **−0.97%**, 3d **−0.88%**, 1w **−1.84%**, 1m **−6.35%**. MAP HEAT confirms the internal picture is **not** broad expansion: Farm & Heavy Machinery breadth **0.115** (two mega-caps, not the industry), Electrical Equipment explicitly **SPLIT/SPX-led**, Conglomerates **down**, Consulting **override down**, Airlines **down**. Score the lag **once** here (09-04 discipline: do not double-count into S4).

### 4. Flows — S3 = 0
Checked XLI flows via search — **nothing material returned** (only generic ETF profile pages and a July CNBC piece on industrials valuation richness). No fresh inflow/outflow print, no volume spike, no index-rebalance event. Not a crowded long (1m rel **−6.35%**). **S3 = 0.**

### 5. ETF tape (confirmation only) — S4 = −1
Channel 1 through 09-14: 1d rel **−0.97%**, 3d **−0.88%**, 1w **−1.84%**, 1m **−6.35%**. **Negative on every horizon** — this is a confirming tape, not a mixed one. Per the **09-14 correction**, the persistent 1m laggard is the *level* signal and is scored **once** here as −1; the 09-10 decay rule tempers the *relative-magnitude expectation*, it does **not** zero the laggard. The premarket XLI −0.05% vs ES −0.54% gap is *not* materially worse than the index gap today, so the 09-14 "convert the premarket gap" clause does not add a second negative — the −1 stands on the multi-horizon tape alone.

### 6. Catalysts / calendar
- **10Y breaches 5% / global bond selloff** (News Judge #1) — core duration shock; bearish for capex-heavy cyclicals via discount rate and financing cost.
- **FOMC decision, SEP/dot plot, Warsh press conference** (News Judge #2) — unresolved policy binary; per 09-11, a pending binary with **non-unanimous** flow (futures red, not green) is a genuine dampener, not a neutral. Do not pre-score the outcome.
- **August CPI core surprise locked in the September hike** (News Judge #3) — already-printed hawkish hard data; already in the tape, do not double-count.
- **Chipmaker weakness / AI-slowdown (AMD −5% premarket, APH −6.5%, Asia memory)** (News Judge #4) — NDX/SOXX driver, not an XLI spine; do not map it into XLI S0 (08-27 family).
- **US–Iran tanker war; Brent ~$107, Hormuz traffic impaired** (News Judge #5) — the live oil supply shock, counted once in S0.
- **BAC CEO soft Q3 outlook, shares −5%** (News Judge #7) — XLF beta, not XLI.
- **August PCE** (News Judge #8) — confirmatory, same rates cluster.

### Self-audit
- **Lens:** cyclical; rates and oil counted **once** in S0, not re-counted in S1.
- **Band:** **mild**, not notable — futures are red but only −0.54% to −0.73% (not ≤ −1%), VIX is not a panic print, credit is tight, and the FOMC binary is unresolved. The 09-14 pipeline's down/notable was justified by a −1.13% premarket XLI gap; today's premarket XLI is only −0.05%, so notable is not earned.
- **Skew:** GEV/VRT/HUBB/BE do **not** drive the ETF call (08-18); CAT/DE strength is explicitly two-mega-cap tape, not breadth (MAP HEAT breadth 0.115).
- **Same-shock double-count:** oil counted once (S0); the persistent laggard counted once (S2), with S4 standing on the multi-horizon tape rather than restacking the same fact.
- **Single-ticker:** no single name (CAT, GEV, BA, BE) drives the sector call.
- **08-27:** 1w/1m laggard → **forbid up**. Applied — no up branch considered.
- **09-09:** tape confirms negative on all horizons → **emit the directional call, do not flatten**. Applied.
- **09-14:** score the persistent laggard once in S4 as −1; do not use the decay rule as a dampener to zero it. Applied.
- **09-11:** pending binary + **non-unanimous** flow → binary is a dampener (cap S0 at −1, not −2). Applied.

**Divergence:** Leading factors (S0 −1 oil/yields, S1 −1 broad internal weakness, S2 −1 laggard) and the tape (S4 −1, negative on every horizon) **agree**. No divergence flag. The only tension is the FOMC binary, which is handled by capping magnitude at mild rather than by flattening direction.

**Final call: down / mild.** Σ(S0..S4) = −1 −1 −1 +0 −1 = **−4.0**; × mult 0.9 = **−3.6**. Direction down, band mild (FOMC pending + futures only −0.5% to −0.7% caps the band below notable). Confidence 0.55 — direction is well-supported by a confirming tape on every horizon, but the unresolved FOMC/SEP binary and the weak 10Y–SPX correlation (−0.107) argue against high conviction.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -1
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -1
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: risk_off
DIVERGENCE_FLAGGED: False
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-off tape / flight to safety|HIT|0.80|2026-09-15|https://www.finviz.com/futures
Real yields rising|HIT|0.75|2026-09-15|https://fred.stlouisfed.org/series/DFII10
USD strengthening|HIT|0.60|2026-09-15|https://www.finviz.com/futures
Sector breadth failure (ETF up, names flat)|HIT|0.70|2026-09-15|https://www.finviz.com/map
Large-cap leadership inside sector|HIT|0.55|2026-09-15|https://www.finviz.com/map
Sector ETF outflow / volume dry-up|MISS|0.30|2026-09-15|
Grid / electrical equipment backlog (AI power)|HIT|0.65|2026-09-15|https://www.finviz.com/news
Aerospace & defense order / budget upside|MISS|0.35|2026-09-15|https://www.finviz.com/map
Freight / trucking / rail volume recovery|MISS|0.40|2026-09-15|https://www.finviz.com/map
ISM manufacturing / new orders expansion|HIT|0.60|2026-09-01|https://www.prnewswire.com/news-releases/manufacturing-pmi-at-54-6-august-2026-ism-manufacturing-pmi-report-302543210.html
Construction slowdown|HIT|0.55|2026-09-15|
Sector rotation out of industrials|HIT|0.65|2026-09-15|https://www.finviz.com/map
Crowded long (extreme relative performance + valuation)|MISS|0.30|2026-09-15|
HORIZON_3D|down|0.55|2026-09-18|
HORIZON_1W|down|0.50|2026-09-22|
HORIZON_2W|flat|0.40|2026-09-29|
HORIZON_1M|flat|0.35|2026-10-15|
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -7.0, 'divergence_flagged': True, 'total_score': -3.324, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.533, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.0904, 'score': 0.542, 'legs': [{'leg': 'ES', 'pct': 0.8, 'w': 0.8}, {'leg': 'ER2', 'pct': -0.73, 'w': 0.2}, {'leg': 'HG', 'pct': -0.76, 'w': 0.1}, {'leg': 'PM:XLI', 'pct': -0.05, 'w': 0.7}]}, 'overlay_score': -3.15, 'overlay_raw': -3.15, 'index_carry': -0.716, 'general_total': -2.865, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 0.5}, 'llm_confidence': 0.55, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
