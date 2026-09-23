# Sector Prediction — Industrials — 2026-09-23

- ETF: **XLI**
- rubric: `00_grounding/sectors/industrials.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **0.68** (mult 0.8)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **0.107** (ES +0.03%, ER2 +0.08%, HG +0.66%, PM:XLI -0.02%) · index_carry **0.573** (general 2.293) · llm_overlay **0.0** (raw 0.0)

## Channel 1 sector ETF tape

```
ETF XLI vs SPY (yfinance, through 2026-09-21):
  1d: XLI +0.40% | SPY +1.55% | rel -1.15%
  3d: XLI +1.02% | SPY +2.83% | rel -1.81%
  1w: XLI +0.30% | SPY +1.91% | rel -1.61%
  1m: XLI -5.19% | SPY +1.68% | rel -6.87%
```

MEMORY_CONFIRM: Memory index unavailable this run (embedding metadata mismatch; `openclaw memory status --index` or `openclaw memory index --force` would rebuild). Used injected Industrials scoreboard + 08-11..09-22 sector logs only. Rolling dir=0.5 / mag=0.6 (n=10); last 30 dir=0.32 / mag=0.28 (n=25). Last graded 09-22: predicted down/mild vs XLI +0.171% / SPY −0.016% / rel +0.186% — **dir MISS, mag MISS** (PM:XLI −0.75% died at the cash open). 09-21 flat/flat vs +0.112% / SPY +1.55% / rel −1.44% (dir MISS, mag HIT). 09-18/09-17 flat/flat dir MISSes on tiny leftover-gap prints. **Governing today: 09-22 (A)** — mixed T+1, S0/S1 correctly 0, |ES|/|NQ| inside ~±0.5% is **not** a 09-21 “index rallies, my sector doesn’t” tape; do **not** mint down/mild from MAP HEAT + 1m lag + a PM quote; 1m rel ≤ −5% is a condition; convert-the-gap needs the **full** 09-14 stack (live oil/duration **plus** a worse-than-index gap that is still the open). **09-21** relative-nonparticipation **OFF** (index is not rallying). **09-17/09-16 keep-flat** on an unsigned post-paid-FOMC card **ON**. **08-27** 1w/1m laggard **forbids up**. **08-18** cap S1 at 0/+1; GEV/FIX/VRT **must not** raise or sink the ETF. **08-11/08-12** **off** (live oil down; Hormuz is a **level**, not a same-session kinetic increment). **09-16** oil-down ≠ S1 trucking. **09-14 S4=−1** **OFF** (oil/duration off; PM:XLI **−0.02%**, not a smash gap). **09-15** better-than-index fade **OFF**. **09-11** four-index ≥+0.5% **OFF**. **09-09** emit-down **OFF**. **09-10/09-04** score the lag **once**, as a condition, not a down print. **Fed-speaker** — Governor Barr 10:05 ET (voting; housing/outlook) while Oct hike odds ~54/46 contested; keep S0 **directionally 0**, cut confidence; do **not** restack paid 09-16 FOMC/Warsh. Open experiment (`sector_industrials` DO-INSTEAD flatten when score fights tape): **applied as keep-flat**, not as a down call. Checklist: (1) flatten-experiment applied; (2) missing factor that flipped 09-22 was signing S2/S4 down without S0/S1 or a live up-index — not repeated; (3) oil / paid FOMC / 1m lag / PM each counted once; (4) S0 vs S1 both 0 — no fresh spine print, leftover AI/grid not restacked.

## XLI near-session environment (not an SPX call)

Object is the **Sep 23 cash session for XLI**, not SPX and not a stock pick. Channel 1 numbers used as given (tape through **2026-09-21**; do not re-derive). FOMC/SEP/Warsh **paid 09-16**. **S&P Global flash PMI 9:45 ET** and **Governor Barr 10:05 ET** are live two-sided and **unscored until print**. Durable goods (Aug advance) is **Sep 25**, not today.

### 1. Shared macro as it hits Industrials — S0 = 0

This is a **mixed, post-paid-FOMC, oil-offered, chips-led pause** — not 09-14/09-15’s oil-up/yields-up smash, not 09-11’s unanimous ≥+0.5% de-risking bounce, and **not** Monday’s AI-beta session restacked as XLI beta.

- **Operator futures vs overnight sleeve — do not re-derive.** Finviz: ES **+0.20%**, NQ **+0.41%**, RTY **+0.08%**, DJIA **+0.11%**. Channel 1 `ES=F +0.03% / NQ=F −0.10%` vs prior close is **inside ±0.5%** on both legs (09-22 template). 08-21 ES/NQ ≥ +0.3% is **partial** (Finviz NQ only). 09-11 four-index ≥ +0.5% is **off**. RTY +0.08% is not a cyclical bid. Per 09-21/09-22: derive *direction* from ES/NQ **sign** (here mixed/flat), *breadth* from all four (narrow). An index rebound is **not** an XLI participation certificate — and this morning there is barely an index rebound.
- **XLI is flat on the injected PM board, not the smash gap.** Channel 1: **PM:XLI −0.02%** vs XLE +0.21% / XLY +0.10% / XLF +0.09% / XLK −0.05% / ES Finviz +0.20%. That is **~0–22 bp** vs the index, not 09-14’s −95 bp relative gap and not 09-22’s −0.75% quote. 09-14 convert-the-gap **does not fire**.
- **Oil is DOWN, not a squeeze.** Finviz WTI **$104.16 (−1.59%)**, Brent **$107.67 (−1.02%)**; `CL=F −4.97% / BZ=F −3.68%`. News Judge #1/#4: Nasdaq record + surprise crude build / oil cools. Channel 2: Brent slipped on Hormuz-reopening *talk* after a multi-session losing streak; transits remain impaired — a **cost LEVEL**, not a same-session kinetic increment. 08-11/08-12 **off**. 08-13: tanker/Hormuz is the stale leg. Count oil **once, here**. Do **not** treat oil-down as trucking/air S1 relief (09-16), and do **not** treat ~$104 Finviz / cooler live crude as a live squeeze.
- **Rates: 1d change is tiny relief, level is still high.** DGS10 **4.96 (−0.05 1d)**, DGS30 **5.29 (−0.05)**, DFII10 **2.62 (−0.06)**. Live Finviz: 10Y note **−0.03%**, 30Y **−0.06%**. Not 09-15’s long-end washout. Real-yield *level* is a condition; the *1d change* is too small to score “real yields falling” as a cyclical tailwind. 5-day 10Y–SPX corr **−0.79** is a condition, not a second S0 shock. DXY **+0.38% 1d** is a mild exporter headwind, not enough for S0 = −1.
- **Fed path is contested and live later this morning.** CME-implied Oct hike **~54.2%** vs hold **~45.8%** (Channel 2; Channel 1 FedWatch not scrapable). **Governor Barr 10:05 ET** (voting; “Economic Outlook and Housing,” Chicago Fed). Per the Fed-speaker lesson: do **not** encode the hike as fully paid; keep S0 **directionally 0**; cut confidence; mark an unresolved-policy event day. News Judge #3 (Warsh JH / gold −3%) is **prior-session / paid 09-16 spine** — do **not** restack.
- **Globals mixed, vol easing, credit tight.** Asia composite **+0.33%**, Europe **−0.25%** (DAX −0.56%). VIX **14.21 (−0.66 1d)** with VIX/VIX3M **0.786** (deep contango). HY OAS **2.66** (tight). Copper **+0.66%**, aluminum **+1.10%** — metals do **not** confirm a growth scare. News Judge #1 (Nasdaq record / chips / oil cool) is **XLK/QQQ beta**, not an industrials participation certificate.
- **Calendar:** no CPI/NFP/FOMC. Flash PMI 9:45 ET is two-sided and **unprinted at this snapshot** — do **not** pre-score a beat or miss. 09-16: on an all-zero industrials card, 09-03 is **path variance**, not a mandate to emit mild. Durable goods is Friday.

**S0 = 0, regime mixed.** Not −1: oil confirmed down, no live yield spike, no vol shock, ES/NQ mixed-flat, 09-22 forbids minting down from this tape. Not +1: four-index confirmation fails, Europe red, 08-13 blocks treating oil-down as a cyclical green light, 1w/1m XLI is a laggard, Barr unprinted. Oil counted **once here**.

### 2. Spine + secondary — S1 = 0 (capped)

**No fresh same-morning industrials hard print.** August ISM manufacturing already printed **09-01**: PMI **54.6** vs July 55.6; new orders **53.7 (−3.0 pts)**. Still **expansion** — **not** an ISM-contraction HIT, but **slowing**. Next ISM is **Oct 1**. 08-18/08-27: **cap S1 at 0/+1**; +2 is forbidden without same-morning confirmation.

- **ISM manufacturing / new orders expansion — CARRIED, not live.** Expansion eighth month, slowing. Do not re-score as a same-session raise.
- **Durable goods / CapEx — UNPRINTED.** August advance **Sep 25**. July +1.1% already in the tape. Do not pre-score Friday.
- **Grid / electrical equipment backlog (AI power) — CARRIED / SPLIT, not a same-session raise.** GEV ~$176B RPO / 116 GW gas book and FIX ~$14.1B backlog are structural. News Judge #8 (FIX +11% AI data-center) and Finviz AME $5.0B Indicor close are **already-traded**. MAP HEAT **Electrical Equipment & Parts = SPLIT down, conv=high** (VRT −7.65% / −15.4% w1, HUBB −3.91%, breadth 0.109). 08-18: GEV/grid/VRT **must not** raise the ETF — and a nested SPLIT-down sleeve must **not** be averaged into a parent down call either. Semi-independent of ISM, but **not** today’s S1 +1.
- **Aerospace & defense — MIXED, not a HIT.** GE F414 logistics (~$2.9B ceiling, early Sep) and small 09-22 Boeing/Raytheon mods are IDIQ/ceiling noise. Jefferies cut BA PT to $265. MAP HEAT A&D **dir=down** (RTX −3.1%, captains split). Do **not** cancel ISM slowing with one award.
- **Freight / trucking / rail — CARRIED inflection, not same-morning.** Cass August shipments **+2.1% y/y** (first y/y gain since Jan 2023) is last month’s print. 09-16: do **not** map WTI/Brent down onto trucking as S1 relief. MAP HEAT Airlines **split** (majors cut, regionals bid on jet fuel). Not a recovery HIT today.
- **Construction slowdown — CARRIED.** Dodge Aug starts **−24.8%** MoM (manufacturing starts **−80.8%** MoM) released ~Sep 21; housing starts 1.275M already printed 09-17; manufacturing construction ~21–22% below year-ago. AI/nonres (Amazon/Clarksville DCs) is the offset, not a broad build boom. Carried, not a same-morning print.
- **Reshoring / industrial policy — checked, nothing material** as a same-morning funding HIT.
- **Rotation into industrials — MISS.** News Judge leadership is chips/Nasdaq; XLI 1m rel **−6.87%**.

Net: carried ISM expansion (slowing) + structural grid vs carried construction cooldown + mixed A&D + no same-morning spine. **S1 = 0**. Not +1: 08-18 forbids using GEV/FIX as a cushion, electrical sleeve is SPLIT-down, no same-morning confirmation. Not −1: ISM is still expansion, Cass is not a freight recession, do not let Dodge T-2 or VRT set the ETF.

### 3. Breadth / leadership — S2 = 0

MAP HEAT nested: Aerospace & Defense down/medium; Building Products worst breadth (0.216); Conglomerates down; Farm/heavy machinery **splitting** (CAT backlog vs tape); Electrical **SPLIT down**; **only clean up-tape is Consulting Services** (VRSK/HURN/ICFI) — that sleeve does not drive XLI. 09-22: MAP HEAT / %above-20dma is a **condition**, not a same-session participation failure, **unless names are failing a live up-index**. ES/NQ are mixed-flat, so the 09-21 “index rallies, my sector doesn’t” clause is **off**. 09-04: do **not** copy the 1m lag into S2. **S2 = 0**.

### 4. Flows / positioning — S3 = 0

ETF Channel (Sep 22): XLI **~$178.4M WoW outflow** (−0.6% shares outstanding, 181.226M → 180.176M). Finviz 1M flows **−3.58%**. Chaikin 20d cited negative. This is **modest distribution already in yesterday’s session**, not a same-open volume spike, and not a crowded-long unwind (1m rel −6.87% is the opposite). 09-22 keep S3 = 0 on this mixed T+1 card. Engine weight on S3 is already ×0.5. **S3 = 0**.

### 5. ETF tape confirmation — S4 = 0

Channel 1 (through **09-21**, unaltered): 1d rel **−1.15%**, 3d **−1.81%**, 1w **−1.61%**, 1m **−6.87%**. Live PM:XLI **−0.02%**. Scoreboard 09-22 close was **rel +0.19%** — the injected 1d lag is a leftover window, not this morning’s open. 09-10: prior 1d rel on a deep laggard is a **decaying** signal. 09-22: 1m ≤ −5% is a **condition**, not a down print; a PM quote is not the cash open. 09-14 full stack **off**. S4 is confirmation only and must not be the thesis. **S4 = 0**.

### Horizons (condition, not a same-day call)

- **HORIZON_3D:** still a relative laggard on the injected 3d (−1.81% vs SPY); no fresh 3-day mean-reversion catalyst at the open → **flat**.
- **HORIZON_1W:** 1w rel −1.61% + chips leadership = rotation-out **condition**; needs a spine print (flash PMI hold or Friday durables) to change → **lag / flat**.
- **HORIZON_2W:** post-FOMC digestion, oil cooling from the $107+ spike but still an elevated cost **level**; Oct hike still a coin toss → **mixed**.
- **HORIZON_1M:** 1m rel −6.87% is the 09-04/09-10 **condition**, not a forecast of another down day → **laggard_condition**.

### Self-audit

- **Lens:** XLI cash session, not SPX, not CAT/GEV/VRT/FIX stock-picking.
- **Band:** unsigned card → pipeline owns totals; lessons cap this at **flat/flat**, not down/mild (09-22) and not up (08-27).
- **Skew:** none. Flash PMI / Barr are two-sided, unscored.
- **Same-shock double-count:** oil once in S0; 1m lag not in S2 and S4; Warsh/FOMC not restacked; FIX/GEV not in S1 and S2.
- **Single-ticker:** VRT SPLIT, FIX +11%, CAT backlog, BA PT cut — none drive the ETF.
- **Divergence:** leading S0–S3 = 0 vs S4 = 0 — **no fight**. DO-INSTEAD flatten is applied as keep-flat, not as a down call. 09-21 relative clause stays off because the index is not rallying.

**Path qualifier:** if flash PMI (9:45) or Barr (10:05) prints a one-sided surprise, that is **after this snapshot** — variance around flat, not a pre-scored mild. PM:XLI −0.02% is not a gap-and-fade setup.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 0
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.8
CONFIDENCE: 0.38
REGIME: mixed
HORIZON_3D: 0
HORIZON_1W: 0
HORIZON_2W: 0
HORIZON_1M: 0
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|PARTIAL|0.45|2026-09-23|https://realinvestmentadvice.com/resources/blog/dow-slides-while-nasdaq-rallies-omen-or-rotation/
Risk-off tape / flight to safety|MISS|0.70|2026-09-23|https://zh.tradingeconomics.com/vix:ind
Real yields rising|MISS|0.65|2026-09-21|Channel 1 DFII10 2.62 (−0.06 1d)
Real yields falling|PARTIAL|0.40|2026-09-21|Channel 1 DGS10 4.96 (−0.05 1d)
USD strengthening|PARTIAL|0.50|2026-09-23|Channel 1 DXY +0.38% 1d
USD weakening|MISS|0.60|2026-09-23|Channel 1 Finviz USD −0.02%
Sector breadth expansion (% names up)|MISS|0.70|2026-09-23|MAP HEAT nested (building products 0.216; electrical 0.109)
Sector breadth failure (ETF up, names flat)|MISS|0.55|2026-09-23|PM:XLI −0.02% (ETF not up)
Large-cap leadership inside sector|PARTIAL|0.45|2026-09-23|MAP HEAT captains split (CAT mixed, RTX neg, VRT neg)
Small/mid leadership inside sector|MISS|0.60|2026-09-23|MAP HEAT RUT names silent / TEX −7.2% w1
High-beta leadership inside sector|MISS|0.65|2026-09-23|Electrical SPLIT down (VRT −7.65%)
Low-beta leadership inside sector|PARTIAL|0.40|2026-09-23|Consulting Services only clean up-tape (VRSK/HURN)
Sector ETF inflow / relative volume spike|MISS|0.70|2026-09-22|https://www.nasdaq.com/articles/notable-etf-outflow-detected-xli-ge-unp-etn
Sector ETF outflow / volume dry-up|PARTIAL|0.55|2026-09-22|https://www.nasdaq.com/articles/notable-etf-outflow-detected-xli-ge-unp-etn
Crowded long (extreme relative performance + valuation)|MISS|0.75|2026-09-21|Channel 1 1m rel −6.87%
Index rebalance / inclusion tailwind|MISS|0.50|2026-09-23|checked, nothing material
Index exclusion / forced selling|MISS|0.50|2026-09-23|checked, nothing material
ISM manufacturing / new orders expansion|CARRIED|0.70|2026-09-01|https://www.prnewswire.com/news-releases/manufacturing-pmi-at-54-6-august-2026-ism-manufacturing-pmi-report-302865127.html
Durable goods / CapEx upside|MISS|0.60|2026-09-23|https://www.census.gov/manufacturing/m3/release_schedule.html
Grid / electrical equipment backlog (AI power)|CARRIED|0.65|2026-09-23|https://pulse2.com/ge-vernova-backlog-reaches-176-billion-as-power-and-electrification-demand-accelerates/
Aerospace & defense order / budget upside|PARTIAL|0.40|2026-09-23|https://breakingdefense.com/2026/09/ge-wins-deals-for-f-a-18-engine-parts-hypersonic-test/
Freight / trucking / rail volume recovery|CARRIED|0.50|2026-09-23|https://www.freightwaves.com/news/cass-tl-rates-jump-11-in-august-freight-shipments-turn-positive
Reshoring / industrial policy funding|MISS|0.50|2026-09-23|checked, nothing material
ISM contraction|MISS|0.80|2026-09-01|https://www.prnewswire.com/news-releases/manufacturing-pmi-at-54-6-august-2026-ism-manufacturing-pmi-report-302865127.html
CapEx cuts / order cancellation|MISS|0.55|2026-09-23|checked, nothing material
Freight recession|MISS|0.60|2026-09-23|https://www.thetrucker.com/trucking-news/business/cass-freight-index-longest-freight-downturn-on-record-as-measured-y-y-ended-in-august
Construction slowdown|CARRIED|0.60|2026-09-21|https://www.constructiondive.com/news/construction-groundbreakings-plunge-August-2026/830940/
Sector rotation into industrials|MISS|0.70|2026-09-23|https://www.etf.com/tools/etf-comparison/xli-vs-spy
Sector rotation out of industrials|CARRIED|0.60|2026-09-23|https://www.investing.com/analysis/dow-slides-while-nasdaq-rallies-omen-or-rotation-200688136
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- ISM manufacturing PMI durable goods orders September 2026
- XLI industrials ETF flows positioning breadth September 23 2026
- GE Vernova grid backlog AI power electrical equipment September 2026
- US freight trucking rail volumes Cass Freight Index September 2026
- CME FedWatch October 2026 rate hike odds September 23
- US economic calendar September 23 2026 Fed speakers industrial production
- S&P Global flash PMI US manufacturing September 23 2026
- Governor Michael Barr speech September 23 2026 housing Fed
- aerospace defense orders Boeing RTX GE September 2026
- US construction spending housing starts manufacturing construction slowdown September 2026
- XLI vs SPY rotation industrials lagging AI chips September 2026
- oil prices Hormuz Iran tanker war September 23 2026
- FIX Comfort Systems AI data center backlog industrials September 2026
- S&P Global US flash PMI September 23 2026 9:45 manufacturing services
- XLI ETF shares outstanding outflow GE UNP ETN September 22 2026
- risk on equity breadth September 23 2026 VIX industrials lagging Nasdaq
- X search: XLI industrials ETF premarket breadth CAT GE Vernova freight defense September 23 2026 (2026-09-21 to 2026-09-23)
- Fetches: nasdaq.com XLI outflow article; ISM Aug 2026 PR; Construction Dive Dodge starts

**Key sources and facts taken**
- ISM / PR Newswire (2026-09-01): Manufacturing PMI 54.6 (Jul 55.6); new orders 53.7 (−3.0); 8th expansion month. https://www.prnewswire.com/news-releases/manufacturing-pmi-at-54-6-august-2026-ism-manufacturing-pmi-report-302865127.html
- Census durable-goods calendar: August advance due **2026-09-25**; July orders +1.1% to $339.3B. https://www.census.gov/manufacturing/m3/release_schedule.html / https://tradingeconomics.com/united-states/durable-goods-orders
- ETF Channel / Nasdaq (2026-09-22 11:05am EDT): XLI WoW outflow ~$178.4M, shares 181.226M → 180.176M (−0.6%). https://www.nasdaq.com/articles/notable-etf-outflow-detected-xli-ge-unp-etn
- Pulse2 / GEV backlog: ~$176B total; electrification equipment ~$40–42B; gas slots 116 GW (Q2 2026, carried). https://pulse2.com/ge-vernova-backlog-reaches-176-billion-as-power-and-electrification-demand-accelerates/
- FreightWaves / Cass August 2026: shipments +2.1% y/y (first since Jan 2023); TL linehaul +11.3% y/y. https://www.freightwaves.com/news/cass-tl-rates-jump-11-in-august-freight-shipments-turn-positive
- Phemex / CME FedWatch (~2026-09-23): Oct meeting ~54.2% +25 bp vs 45.8% hold. https://phemex.com/news/article/cme-fedwatch-542-probability-of-rate-hike-at-october-meeting-97512
- CME Econoday / S&P Global: US flash PMI scheduled 9:45 ET 2026-09-23; mfg consensus ~53.6, services ~56.0; **unprinted at this snapshot**. https://www.cmegroup.com/education/events/econoday/671311
- Forth.news / Chicago Fed: Governor Barr 10:05 ET 2026-09-23, “Economic Outlook and Housing.” https://www.chicagofed.org/events/2026/housing-affordability-community-development-summit
- Construction Dive (2026-09-21): Dodge August starts −24.8% MoM to $1.34T SAAR; manufacturing starts −80.8% MoM; YTD starts still +15.2%. https://www.constructiondive.com/news/construction-groundbreakings-plunge-August-2026/830940/
- CNBCTV18 (~2026-09-22): Brent below ~$98–99 on Hormuz-reopening talk; fifth down session; transits still impaired. https://www.cnbctv18.com/market/commodities/crude-oil-prices-drop-below-usd-99-iran-hormuz-reopening-offer-us-agreement-reports-19995778.htm
- Breaking Defense (~2026-09-02): GE F414 PBL up to ~$2.9B through 2031 (already-traded ceiling). https://breakingdefense.com/2026/09/ge-wins-deals-for-f-a-18-engine-parts-hypersonic-test/
- Comfort Systems Q2 2026: backlog ~$14.1B, tech/data-center 58% of H1 revenue (carried; News Judge FIX +11% already-traded). https://investors.comfortsystemsusa.com/node/18631/html
- Channel 1 (injected, unaltered): VIX 14.21, VIX/VIX3M 0.786; Finviz ES +0.20% / NQ +0.41% / RTY +0.08% / DJIA +0.11%; ES=F +0.03% / NQ=F −0.10%; PM:XLI −0.02%; CL=F −4.97% / BZ=F −3.68%; Asia +0.33% / Europe −0.25%; XLI vs SPY through 2026-09-21: 1d rel −1.15%, 3d −1.81%, 1w −1.61%, 1m −6.87%.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.8, 'leading_sum': 0.0, 'divergence_flagged': False, 'total_score': 0.68, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.527, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.0178, 'score': 0.107, 'legs': [{'leg': 'ES', 'pct': 0.03, 'w': 0.8}, {'leg': 'ER2', 'pct': 0.08, 'w': 0.2}, {'leg': 'HG', 'pct': 0.66, 'w': 0.1}, {'leg': 'PM:XLI', 'pct': -0.02, 'w': 0.7}]}, 'overlay_score': 0.0, 'overlay_raw': 0.0, 'index_carry': 0.573, 'general_total': 2.293, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 0.5}, 'llm_confidence': 0.38, 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': -1.65, 'w1': -2.74}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
