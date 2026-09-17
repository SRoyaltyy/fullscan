# Sector Prediction — Industrials — 2026-09-17

- ETF: **XLI**
- rubric: `00_grounding/sectors/industrials.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **6.262** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **4.416** (ES +1.71%, ER2 +0.08%, HG +0.66%, PM:XLI +0.43%) · index_carry **1.846** (general 7.383) · llm_overlay **0.0** (raw 0.0)

## Channel 1 sector ETF tape

```
ETF XLI vs SPY (yfinance, through 2026-09-16):
  1d: XLI -0.08% | SPY -0.44% | rel +0.36%
  3d: XLI -2.12% | SPY -1.34% | rel -0.78%
  1w: XLI -1.79% | SPY -1.10% | rel -0.70%
  1m: XLI -9.45% | SPY -2.41% | rel -7.04%
```

MEMORY_CONFIRM: Memory index unavailable this run (embedding metadata mismatch; `openclaw memory status --index` or `openclaw memory index --force` would rebuild). Used injected Industrials scoreboard + 08-11..09-16 sector logs only. Rolling dir=0.6 / mag=0.4 (n=10); last 30 dir=0.381 / mag=0.238 (n=21). Last graded 09-16: predicted flat/flat, actual XLI −0.083% / SPY −0.441% / rel +0.358% — **dir HIT, mag HIT**. Prior: 09-15 down/mild HIT, 09-14 down/notable HIT, 09-11 up/mild HIT, 09-10 down/mild HIT; 09-08/09-09 flat/flat MISSes on oil-shock days. **Governing today: 09-16 (NONE) — all-zero card, oil down, incomplete four-index confirmation, 08-27 forbids up → keep close-to-close flat/flat; 09-03 is path variance around an unprinted print, not a mandate to emit mild with no directional lean; do not map crude-down onto trucking as S1 relief.** 09-15 better-than-index PM-gap fade **OFF** (no live oil/duration shock). 09-14 S4=−1 persistent-lag **OFF** (needs live oil/duration plus a *worse*-than-index PM gap; PM:XLI +0.43% vs Finviz ES +0.20%). 09-11 unanimous ≥+0.5% across all four **OFF** (Finviz ES +0.20% / NQ +0.41% / RTY +0.08% / DJIA +0.11%). 09-09 emit-down **OFF** (1d rel **+0.36%**, oil **down**). 09-10 decay — 1d rel is a sub-gate/positive print; 1m rel −7.04% is a CONDITION. 09-04 score the laggard **once**. 08-27 — 1w/1m laggard **forbids up**. 08-21 reversal **partial** (NQ +0.41% ≥ +0.3%, ES +0.20% not). 08-18 — cap S1 at 0/+1; GEV/grid **not** a cushion. 08-11/08-12 **does not fire** (live oil **down**). 08-13 — Hormuz/tanker-rate headline is the stale leg; session oil change is down. Fed-speaker/FOMC lesson — **FOMC+SEP+Warsh printed 09-16**; do **not** restack as today’s binary. Open experiment (`sector_industrials`): keep direction, shrink confidence on modest |score| — **applied**. DO-INSTEAD “score fights tape → flat/mild”: leading factors ~0 vs still-negative 3d/1w/1m tape — **binding as a flatten, not as a down call**. Checklist: (1) open experiment applied; (2) no missing factor that would flip 09-16’s HIT; (3) oil/FOMC/lag each counted once; (4) S0 vs S1 both 0 — FOMC paid, no fresh spine print.

## XLI near-session environment (not an SPX call)

Object is the **Sep 17 cash session for XLI**, not SPX and not a stock pick. Channel 1 numbers used as given. FOMC/SEP/Warsh **already printed 09-16** (paid). **8:30 ET claims / housing starts / Philly Fed** are live two-sided and **unscored until print**.

### 1. Shared macro as it hits Industrials — S0 = 0

This is a **post-FOMC relief/pause tape**, not 09-14/09-15’s oil-up/yields-up smash and not 09-11’s unanimous +0.5% de-risking bounce.

- **FOMC is paid, not pending.** Official statement 09-16 14:00 ET: +25 bp to **3.75–4.00%**, 12–0, inflation “elevated,” CapEx called “robust.” Warsh/dots (another hike clustered by year-end) hit **yesterday’s** close (SPY −0.44%, XLI −0.08%, rel **+0.36%**). News Judge #1–2 (warm August retail +1.2% / Warsh hike-odds) are **prior-session**. Do **not** restack as a second hawkish S0 shock. Do **not** encode the residual path as a same-morning unsigned binary — the print happened.
- **Futures are green but not independently confirming a cyclical bid.** Channel 1 Finviz: ES **+0.20%**, NQ **+0.41%**, RTY **+0.08%**, DJIA **+0.11%**. The `ES=F +1.71% / NQ=F +2.10% vs prev close` sleeve is the same sign, larger overnight gap (Reuters: hike “lifts a long-standing overhang”). **Do not re-derive**; Finviz is the live quote-page tape (same conflict-handling as 09-15/09-16). 08-21’s ES/NQ ≥ +0.3% gate is **partial** (NQ only). 09-11’s unanimous ≥ +0.5% *across all four* is **off**. RTY +0.08% is not a cyclical bid. XLK PM **+1.28%** vs XLI PM **+0.43%** — the bounce is **tech-led**, not industrials-led.
- **Oil is DOWN, not a fresh squeeze.** Channel 1: `CL=F −0.98%`, `BZ=F −1.30%`; Finviz WTI **$104.16 (−1.59%)**, Brent **$107.67 (−1.02%)**. Absolute level is still elevated — a **cost LEVEL** for transports/manufacturers, not a same-session supply shock. Channel 2: crude eased on Saudi extra cargoes / inventory build even as Hormuz transits stay impaired and VLCC rates stay extreme. 08-11/08-12 **does not fire**. 08-13: tanker-rate/Hormuz headline is the stale leg; live change is a pullback. Count oil **once, here**. Do **not** treat oil-down as a full cyclical tailwind, and do **not** treat $104 oil as a live squeeze.
- **Rates: level still high, session change is not a backup.** DGS10 **5.00** / DGS30 **5.36** / DFII10 **2.62** (through 09-15). Live Finviz: 10Y note **−0.03%**, 30Y **−0.06%** — tiny price dip, not 09-15’s long-end washout. 5-day 10Y–SPX corr **−0.109** (weak). Real-yield *level* is a condition; the *1d change* is not a second S0 shock.
- **Globals mixed, USD soft, vol easing.** Asia composite **−0.03%**, Europe **+0.45%**. DXY **−0.13% 1d**. VIX **16.04 (−1.67 1d)** with VIX/VIX3M **0.813** (deep contango). HY OAS **2.76** (tight, +0.05 1d). Copper **+0.66%**, aluminum **+1.10%** — metals do **not** confirm a growth scare this morning.
- **Calendar is two-sided at 8:30 ET, not scored.** Claims (~207–210k vs 206k), August housing starts (~1.32M vs 1.239M), **Philly Fed** (consensus ~30 vs August **47.4**). Philly Fed is the XLI-relevant print; do **not** pre-score a miss or a beat. 09-03 still means path variance can be wide around 8:30. 09-16 still means that is **not** a license to emit mild when the factor card has no directional lean.

**S0 = 0, regime mixed.** Not −1: oil is confirmed down, futures are green, no live yield spike, FOMC already absorbed (XLI *outperformed* SPY on the print day). Not +1: four-index confirmation fails, 08-13 blocks treating oil-down as a cyclical green light, NQ/XLK lead the gap, 1w/1m XLI is a laggard. Oil counted **once here**.

### 2. Spine + secondary — S1 = 0 (capped)

**No fresh same-morning industrials hard print.** August ISM manufacturing already printed **09-01**: PMI **54.6** vs July 55.6; new orders **53.7** (−3.0 pts). Still **expansion** — **not** an ISM-contraction HIT, but **slowing**. Both in the tape. 08-18/08-27: **cap S1 at 0/+1**; +2 is forbidden without same-morning confirmation. Philly Fed is the live manufacturing read and is **unprinted**.

- **Grid / electrical equipment backlog (AI power) — HIT, carried, not an ETF raise.** GEV RPO ~**$176B**; CEO 09-16 said **$200B “very early” 2027**; GEV ~+5% on 09-16 **did not lift XLI** (XLI −0.08%). MAP HEAT **SPLIT Electrical Equipment dir=down** (VRT AI-power complex being sold; ATKR HSR is a nested takeout, not an XLI electrical long). Nested OVERRIDE beats the parent. 08-18: **not** a downside cushion and **not** a same-session raise.
- **Aerospace & defense — MIXED, do not cancel the book with awards.** Boeing MDA GMD **$3.48B** (09-16) and KC-46 **$13.4B ceiling** (09-11, no funds obligated at award). Korean Air **103-aircraft** order is commercial BA, already traded 09-16. MAP HEAT A&D **dir=down / low conv** (RTX NASAMS vs GE aftermarket downgrade). Do **not** cancel ISM (expansion, not weak) with one award.
- **Freight / trucking / rail — not a same-morning recovery HIT.** Cass August shipments **+2.1% y/y** (first y/y gain since Jan 2023) is **stale** (~09-14). 09-16: do **not** map WTI/Brent down onto trucking/air as S1 relief. MAP HEAT Airlines **dir=flat**.
- **Construction slowdown — HIT, carried, residential vs AI/nonres split.** July construction spending **−0.5%**; July starts **1.239M**. August starts **pending 8:30**. MAP HEAT Building Products **dir=down** (−5% week, 0.22 breadth).
- **Reshoring / industrial policy — checked, nothing material** same-morning.
- **Rotation — out of industrials into tech this morning, not in.** XLK PM **+1.28%** vs XLI **+0.43%**. MAP HEAT machinery/E&C/electrical nested **down vs XLI**.

Net: carried ISM expansion (slowing) + structural grid vs nested electrical sale + carried construction drag + mixed freight. **S1 = 0** (capped; no fresh same-morning confirmation; GEV/BA do not drive the ETF).

### 3. Breadth — S2 = 0

MAP HEAT nested groups are mostly **dir=down / low conv** (machinery week-down vs XLI, E&C red, building products −5% week, A&D mixed). Electrical is a **medium-conv SPLIT down**. Consulting is up and **XLI will miss that bid**. That is large-cap / residual-weak color, not a same-morning % names print.

Top weights are **green in PM** (CAT ~+1.8%, GE ~+1.5%, RTX ~+0.7%, HON ~+0.4%) — do **not** let CAT/GE set the ETF call, and do **not** treat low-conv nested red as a full S2 = −1 without a second confirming source.

Persistent 1m rel **−7.04%** is a **CONDITION**, not a same-day forecast (standing laggard rule). 09-04: score the lag **once**. 09-16: 09-14’s S4 = −1 branch is **off**. If the lag is not a forward factor today, it does not get a second home in S2. **S2 = 0.**

### 4. Flows / positioning — S3 = 0

Channel 2: ETF.com ~**−$212M** XLI redemption on 09-15; ~**−$1.13B** 1m / ~**−$121M** 5d. Volume 09-16 ~8.9M vs ~6.9M avg — **not** a dry-up. Not a crowded long (1m rel −7%). Lagged creations/redemptions are the laggard’s positioning shadow, not a same-session flow shock. **S3 = 0** (engine already half-weights this bucket). Not “nothing material” — noted and **not re-stacked** into S2/S4.

### 5. ETF tape (confirmation only) — S4 = 0

Channel 1 through 09-16:

- 1d: XLI **−0.08%** | SPY **−0.44%** | rel **+0.36%**
- 3d: rel **−0.78%**
- 1w: rel **−0.70%**
- 1m: rel **−7.04%**

1d rel is **positive** (stabilizing after FOMC). 3d/1w/1m remain a laggard. 09-09 needs **negative 1d confirmation** to emit down — **absent**. 09-14 wants S4 = −1 for the 1m lag only with live oil/duration **and** a worse-than-index PM gap — **both absent** (oil down; PM:XLI **+0.43%** vs Finviz ES **+0.20%**, *better* than the index). 09-15 fade rule **OFF**. Independent same-session tape fact for S4: **none**. Lag not double-counted. **S4 = 0.**

### 6. Catalysts / calendar

- **Paid:** FOMC +25 bp / hawkish dots / Warsh (09-16); August retail +1.2%; GEV $200B color; Boeing GMD/KC-46 ceilings; Cass August inflection.
- **Live unprinted (8:30 ET):** claims, housing starts/permits, **Philly Fed (~30 vs 47.4)**. Two-sided. Do not pre-score.
- **Not XLI spine:** ADBE, ASML, BAC, APH — News Judge #6–8.

### Self-audit

- Lens: cyclical; rates/oil only in S0; ISM/CapEx in S1.
- Band: **flat**, not mild — 09-16 binds when the card is unsigned; 09-03 is path variance around 8:30, not a close-to-close mild mandate. size_gate=True.
- Skew: GEV/BA/CAT do not drive the ETF call. Nested electrical **sold**.
- Same-shock: FOMC once (paid, not restacked); oil once in S0; lag once (as condition, not S2+S4).
- Single-ticker: CAT/GE PM green ignored as ETF direction.
- **08-27 forbids up** (1w rel −0.70%, 1m rel −7.04%).
- **Divergence:** leading sum **0** vs 1d rel **+0.36%** (no fight). 3d/1w/1m still lag — that forbids **up**, it does not mint **down** without a confirming 1d tape or a live oil/duration stack.

**Final:** component card is **all-zero**. Pipeline owns totals/direction/magnitude. Narrative reconciliation: **flat/flat** (08-27 blocks up; 09-09/09-14/09-15 down-branches off; 09-16 all-zero + incomplete futures confirmation keeps close-to-close flat). Confidence **shrunk** per open experiment.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 0
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.50
REGIME: mixed
HORIZON_3D: flat — FOMC paid; 8:30 Philly Fed/claims/starts two-sided; oil offered but still >$100; no signed lean
HORIZON_1W: mixed_to_lag — 1w rel −0.70% and real-yield *level* still high; needs ISM/Philly confirmation to stop lagging XLK
HORIZON_2W: mixed — grid/AI-power backlog intact vs hawkish dots / CapEx cost of capital
HORIZON_1M: lag_persists — 1m rel −7.04% is the condition until a fresh manufacturing/CapEx impulse; not a same-day forecast
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|PARTIAL|0.45|2026-09-17|https://www.reuters.com/business/wall-st-futures-rise-fed-rate-hike-lifts-long-standing-overhang-2026-09-17/
Risk-off tape / flight to safety|MISS|0.70|2026-09-17|Channel 1 VIX 16.04 −1.67, VIX/VIX3M 0.813
Real yields rising|PARTIAL|0.55|2026-09-15|Channel 1 DFII10 2.62, +0.19 1w / +0.21 1m; 1d only +0.02
Real yields falling|MISS|0.60|2026-09-17|Channel 1 Finviz 10Y −0.03% / 30Y −0.06% (not a relief dump)
USD strengthening|MISS|0.65|2026-09-17|Channel 1 DXY 1d −0.13%
USD weakening|PARTIAL|0.40|2026-09-17|Channel 1 DXY −0.13%; Finviz USD −0.02%
Sector breadth expansion (% names up)|MISS|0.55|2026-09-17|MAP HEAT nested mostly dir=down vs XLI
Sector breadth failure (ETF up, names flat)|PARTIAL|0.45|2026-09-17|MAP HEAT electrical/E&C/machinery nested down; XLI PM +0.43%
Large-cap leadership inside sector|HIT|0.60|2026-09-17|CAT/GE PM green vs nested residual red
Small/mid leadership inside sector|MISS|0.55|2026-09-17|MAP HEAT RTY +0.08%; nested small/mid not leading
High-beta leadership inside sector|MISS|0.50|2026-09-17|XLK PM +1.28% leads; XLI not the high-beta sleeve
Low-beta leadership inside sector|MISS|0.50|2026-09-17|Not a defensive bid morning
Sector ETF inflow / relative volume spike|MISS|0.55|2026-09-16|https://www.etf.com/sections/daily-etf-flows/daily-etf-flows-iei-sheds-assets
Sector ETF outflow / volume dry-up|PARTIAL|0.50|2026-09-16|XLI ~−$212M 09-15; 1m ~−$1.13B; volume not dry
Crowded long (extreme relative performance + valuation)|MISS|0.75|2026-09-16|Channel 1 1m rel −7.04%
Index rebalance / inclusion tailwind|MISS|0.40|2026-09-17|checked, nothing material
Index exclusion / forced selling|MISS|0.40|2026-09-17|checked, nothing material
ISM manufacturing / new orders expansion|PARTIAL|0.70|2026-09-01|https://www.prnewswire.com/news-releases/manufacturing-pmi-at-54-6-august-2026-ism-manufacturing-pmi-report-302865127.html
Durable goods / CapEx upside|MISS|0.45|2026-09-17|no fresh same-morning durables print
Grid / electrical equipment backlog (AI power)|HIT|0.70|2026-09-16|https://www.investors.com/news/gev-stock-ge-vernova-expects-200-billion-backlog-very-soon/
Aerospace & defense order / budget upside|PARTIAL|0.50|2026-09-16|https://www.afcea.org/signal-media/boeing-wins-3-billion-ground-based-midcourse-defense-contract
Freight / trucking / rail volume recovery|PARTIAL|0.45|2026-09-14|https://www.freightwaves.com/news/cass-tl-rates-jump-11-in-august-freight-shipments-turn-positive
Reshoring / industrial policy funding|MISS|0.40|2026-09-17|checked, nothing material
ISM contraction|MISS|0.75|2026-09-01|August PMI 54.6 still expansion
CapEx cuts / order cancellation|MISS|0.45|2026-09-17|checked, nothing material
Freight recession|MISS|0.50|2026-09-14|Cass August shipments +2.1% y/y (stale, not same-morning)
Construction slowdown|HIT|0.55|2026-09-01|https://www.census.gov/construction/c30/pdf/release.pdf
Sector rotation into industrials|MISS|0.60|2026-09-17|Channel 1 XLI PM +0.43% vs XLK +1.28%
Sector rotation out of industrials|PARTIAL|0.50|2026-09-17|MAP HEAT nested down vs XLI; 1w/1m rel still negative
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- FOMC decision September 17 2026 Warsh rate hike
- US retail sales September 2026 FOMC
- ISM manufacturing durable goods CapEx September 2026
- XLI industrials ETF flows premarket September 17 2026
- oil prices Hormuz tanker rates September 17 2026
- GE Vernova grid backlog electrical equipment September 2026
- US economic calendar September 17 2026 claims Philly Fed
- freight trucking rail Cass September 2026
- Boeing defense orders aerospace September 2026
- XLI constituents premarket CAT GE RTX HON September 17 2026
- CME FedWatch September 2026 rate hike odds after FOMC
- sector rotation industrials vs technology September 17 2026
- US housing starts construction spending September 2026
- Philly Fed manufacturing index September 2026 forecast
- XLI ETF volume flows State Street industrials September 2026
- stocks futures September 17 2026 after Fed hike Warsh
- X search: XLI industrials FOMC Warsh oil premarket September 17 2026 (2026-09-16..2026-09-17)
- web_fetch: https://www.federalreserve.gov/newsevents/pressreleases/monetary20260916a.htm
- web_fetch: https://www.nytimes.com/2026/09/17/business/economy/fed-interest-rates-warsh.html (403)

**Key sources (title + URL + timestamp where available) and facts taken**

1. **Federal Reserve FOMC statement** — https://www.federalreserve.gov/newsevents/pressreleases/monetary20260916a.htm — fetched 2026-09-17T10:02:15Z. +25 bp to 3.75–4.00%, 12–0, activity solid, CapEx robust, inflation elevated. **Used:** FOMC paid 09-16; do not restack.
2. **NYT / CNBC / Axios via search** — https://www.nytimes.com/2026/09/17/business/economy/fed-interest-rates-warsh.html ; https://www.cnbc.com/2026/09/16/here-are-five-key-takeaways-from-wednesdays-fed-rate-hike.html — Warsh hawkish, dots cluster another hike, equity selloff 09-16. **Used:** residual path is paid color, not today’s unsigned binary.
3. **Census / Reuters retail sales** — https://www.census.gov/retail/sales.html ; https://www.reuters.com/business/retail-consumer/us-retail-sales-rebound-sharply-august-2026-09-16/ — August sales $773.9B, +1.2% m/m, control +1.4%. **Used:** News Judge #1 is prior-session.
4. **ISM August PMI** — https://www.prnewswire.com/news-releases/manufacturing-pmi-at-54-6-august-2026-ism-manufacturing-pmi-report-302865127.html — PMI 54.6. **Used:** spine still expansion, not contraction; stale vs 09-17.
5. **Reuters futures 09-17** — https://www.reuters.com/business/wall-st-futures-rise-fed-rate-hike-lifts-long-standing-overhang-2026-09-17/ — futures up as hike lifts overhang; oil retreats. **Used:** relief-bounce framing; still not four-index ≥+0.5% on Finviz.
6. **Economic Times oil 09-17** — https://m.economictimes.com/markets/commodities/news/oil-price-today-september-17-crude-oil-falls-below-105-even-as-middle-east-tensions-simmer-heres-why/articleshow/134299659.cms — crude below $105 on extra Saudi cargoes; Hormuz still impaired. **Used:** 08-11/12 off; 08-13 stale-headline / live oil-down.
7. **Reuters Hormuz transits** — https://www.reuters.com/world/middle-east/number-ships-transiting-strait-hormuz-falls-three-wednesday-data-shows-2026-09-17/ — transits still tiny. **Used:** structural geo LEVEL, not today’s squeeze increment.
8. **Investor’s Business Daily GEV** — https://www.investors.com/news/gev-stock-ge-vernova-expects-200-billion-backlog-very-soon/ — CEO $200B backlog early 2027; GEV +5% 09-16. **Used:** grid HIT carried; 08-18 not an XLI raise (XLI still −0.08% that day).
9. **TipRanks / TradingCharts calendar** — https://www.tipranks.com/calendars/economic ; https://forex.tradingcharts.com/economic_calendar/2026-09-17.html?code=USD — 8:30 claims, housing starts, Philly Fed ~30 vs 47.4. **Used:** two-sided, unscored.
10. **FreightWaves Cass August** — https://www.freightwaves.com/news/cass-tl-rates-jump-11-in-august-freight-shipments-turn-positive — shipments +2.1% y/y, first since Jan 2023. **Used:** stale freight recovery, not same-morning S1.
11. **AFCEA / Morningstar Boeing** — https://www.afcea.org/signal-media/boeing-wins-3-billion-ground-based-midcourse-defense-contract ; https://www.morningstar.com/news/dow-jones/202609117562/pentagon-awards-boeing-134-billion-air-force-contract-modification — GMD $3.48B; KC-46 $13.4B *ceiling*. **Used:** awards exist; do not cancel book; ceilings ≠ obligated.
12. **ETF.com daily flows** — https://www.etf.com/sections/daily-etf-flows/daily-etf-flows-iei-sheds-assets — XLI ~−$212M 09-15. **Used:** lagged outflow, not same-session S3 shock.
13. **Census construction** — https://www.census.gov/construction/c30/pdf/release.pdf — July spending −0.5%. **Used:** carried construction slowdown.
14. **Channel 1 (injected, unaltered)** — VIX 16.04; Finviz futures ES +0.20% / NQ +0.41% / RTY +0.08% / DJIA +0.11%; WTI −1.59% / Brent −1.02%; XLI vs SPY 1d rel +0.36%, 1m rel −7.04%; XLI PM +0.43% vs XLK +1.28%.
15. **X search 09-16..09-17** — no XLI-specific premarket thread; FOMC/Warsh hawkish color already in 09-16 tape. **Used:** “checked, nothing material” for live XLI-only chatter.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': 0.0, 'divergence_flagged': False, 'total_score': 6.262, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.55, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.736, 'score': 4.416, 'legs': [{'leg': 'ES', 'pct': 1.71, 'w': 0.8}, {'leg': 'ER2', 'pct': 0.08, 'w': 0.2}, {'leg': 'HG', 'pct': 0.66, 'w': 0.1}, {'leg': 'PM:XLI', 'pct': 0.43, 'w': 0.7}]}, 'overlay_score': 0.0, 'overlay_raw': 0.0, 'index_carry': 1.846, 'general_total': 7.383, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 0.5, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 0.5}, 'llm_confidence': 0.5, 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': -1.65, 'w1': -2.74}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
