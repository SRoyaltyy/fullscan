# Sector Prediction — Industrials — 2026-09-22

- ETF: **XLI**
- rubric: `00_grounding/sectors/industrials.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-4.127** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **-3.103** (ES -0.07%, ER2 +0.08%, HG +0.66%, PM:XLI -0.75%) · index_carry **-0.124** (general -0.497) · llm_overlay **-0.9** (raw -0.9)

## Channel 1 sector ETF tape

```
ETF XLI vs SPY (yfinance, through 2026-09-21):
  1d: XLI +0.40% | SPY +1.55% | rel -1.15%
  3d: XLI +1.02% | SPY +2.83% | rel -1.81%
  1w: XLI +0.30% | SPY +1.91% | rel -1.61%
  1m: XLI -5.19% | SPY +1.68% | rel -6.87%
```

MEMORY_CONFIRM: Memory index unavailable this run (embedding metadata mismatch). Used injected Industrials scoreboard + 08-11..09-21 sector logs only. Rolling dir=0.5 / mag=0.6 (n=10); last 30 dir=0.333 / mag=0.292 (n=24). Last graded 09-21: predicted flat/flat vs XLI +0.112% / SPY +1.552% / rel −1.440% — dir MISS, mag HIT (absolute flat, relative collapse). 09-18 flat/flat MISS (+0.438%). 09-17 flat/flat MISS (+0.178%, gap-and-fade). 09-16 flat/flat HIT. 09-15 down/mild HIT. 09-14 down/notable HIT. Governing today: **09-21 (A)** — “index rallies, my sector doesn’t” is a signed relative signal, not three zeros; derive *direction* from ES/NQ sign, *breadth* from all four; XLK-led / XLI absent-or-red on the PM board / 1w/1m rel ≤ −1% must carry explicit negative relative weight. **09-17 keep-flat** applies only to an *all-zero* post-paid-FOMC card — it does **not** license re-zeroing S2/S4 after 09-21. **09-16** — do not map crude-down onto trucking as S1 relief; 09-03 is not a mandate to emit mild with no lean. **09-14 S4=−1** full stack needs live oil/duration **plus** a worse-than-index PM gap — oil/duration **OFF**, worse PM gap **ON** (PM:XLI −0.75%). **09-15** better-than-index fade **OFF** (PM is worse, not better). **09-11** unanimous ≥+0.5% across all four **OFF**. **09-09** emit-down **OFF** (no live oil-supply shock). **09-10** decay — 1m rel −6.87% is a CONDITION; yesterday’s 1d rel −1.15% is a fresh relative miss, not a sub-gate decay print. **09-04** score the SPY-relative lag once in S4; S2 is internals (MAP HEAT / % above 20-dma), not a second copy of 1m rel. **08-27** 1w/1m laggard **forbids up**. **08-18** cap S1 at 0/+1; GEV/grid/FIX/VRT **must not** raise the ETF. **08-11/08-12** **does not fire** (live oil down; inventory ≠ kinetic). **08-13** Hormuz is the stale leg. **Fed-speaker lesson** — Williams 10:05 ET + Vice Chair Jefferson 10:20 ET (Treasury Market Conference) are **same-session voting-Fed remarks** while Oct hike odds remain contested (~55–58%); keep S0 directionally 0, cut confidence, mark unresolved-policy event day; do **not** restack paid 09-16 FOMC/Warsh. Open experiment (`sector_industrials` DO-INSTEAD flatten when score fights tape): **not binding** — leading factors and Channel 1 tape **agree** negative. Checklist: (1) no flatten-experiment applies; (2) missing factor that flipped 09-21 was the unsigned relative headwind — signed today; (3) oil / paid FOMC / 1m lag / PM gap each counted once; (4) S0 vs S1 both 0 — no fresh spine print, leftover AI not restacked as cyclical beta.

## XLI near-session environment (not an SPX call)

Object is the **Sep 22 cash session for XLI**, not SPX and not a stock pick. Channel 1 numbers used as given.

### 1. Shared macro as it hits Industrials — S0 = 0

This is **not** 09-14/09-15’s oil-up/yields-up smash and **not** 09-11’s unanimous ≥+0.5% de-risking bounce. It is also **not** Monday’s AI-beta session restacked: News Judge ranks the Nasdaq/AMD $1T pop as a **prior-close leftover**, with no overnight AI-infra bellwether beat.

- **Operator futures vs overnight sleeve — do not re-derive.** Finviz: ES **+0.20%**, NQ **+0.41%**, RTY **+0.08%**, DJIA **+0.11%**. Channel 1 `ES=F −0.07% / NQ=F −0.07%` vs prev close is the same-sign-conflict pattern as 09-15/16/17, except the overnight sleeve is now **flat-to-red**, not a leftover gap. 08-21 ES/NQ ≥ +0.3% is **partial** (Finviz NQ only). 09-11 four-index ≥ +0.5% is **off**. RTY +0.08% is not a cyclical bid. Per 09-21: ES/NQ *sign* is mixed (Finviz green, ES=F red); all-four *breadth* is narrow. An index rebound is **not** an XLI participation certificate.
- **XLI is the worst name on the injected PM board.** Channel 1: **PM:XLI −0.75%** vs XLP +0.32% / XLC +0.21% / XLK −0.18% / ES Finviz +0.20%. That is a knowable-at-open **~95 bp** relative gap vs Finviz ES and ~68 bp vs ES=F. 09-14’s “convert the worse-than-index gap” clause fires on the *gap*; 09-14’s oil/duration co-trigger does **not**.
- **Oil is DOWN, not a squeeze.** Finviz WTI **$104.16 (−1.59%)**, Brent **$107.67 (−1.02%)**; `CL=F −4.78% / BZ=F −0.63%`. News Judge #4: surprise crude inventory build (DVN), **not** a Hormuz kinetic increment. Absolute Brent still elevated — a **cost LEVEL** for transports/manufacturers, not a same-session supply shock. 08-11/08-12 **off**. 08-13: tanker/Hormuz is the stale leg. Count oil **once, here**. Do **not** treat oil-down as trucking/air S1 relief (09-16).
- **Rates: level still high, session change is not a backup.** DGS10 **5.01** / DGS30 **5.34** / DFII10 **2.68** (through 09-18). Live Finviz: 10Y note **−0.03%**, 30Y **−0.06%**. 5-day 10Y–SPX corr **−0.79** (through 09-21) is a *condition*, not a second S0 shock. Real-yield *1d change* is not 09-15’s long-end washout and is too small to score “real yields falling” as a cyclical tailwind.
- **Fed path is contested and live later this morning.** CME-implied Oct hike ~55–58% vs hold ~42–44% (last clean prints 09-17/18; Channel 1 FedWatch not scrapable). **Williams 10:05 ET** and **Vice Chair Jefferson 10:20 ET** at the NY Fed Treasury Market Conference (discount window / Treasury functioning). Per the Fed-speaker lesson: do **not** encode the hike as fully paid; keep S0 **directionally 0**; cut confidence; mark an unresolved-policy event day. Topics are market-structure, not SEP/dots — do **not** inflate magnitude via 09-03.
- **Globals/vol/USD:** Asia composite **+0.41%**, Europe **+0.07%**. VIX **14.88**, VIX/VIX3M **0.823** (deep contango). HY OAS **2.68** (tight). DXY **+0.06%** / Finviz USD **−0.02%** — flat. Copper Finviz **+0.66%** vs News Judge copper-tariff retreat (BHP) — mixed industrial metals, not a growth-scare smash.
- **Monday AI/Nasdaq leftover is paid.** Do not restack AMD $1T / ASML EUV / optics as today’s industrials beta. That impulse already printed as SPY **+1.55%** vs XLI **+0.40%** (rel **−1.15%**).

**S0 = 0, regime mixed.** Not −1: no live oil squeeze, no risk-off vol, futures not independently red. Not +1: leftover AI is stale, four-index confirm fails, XLI PM is the worst on the board, oil-down is not a cyclical green light, voting-Fed remarks are unprinted. Oil counted **once here**.

### 2. Spine + secondary — S1 = 0 (capped)

**No fresh same-morning industrials hard print.** August ISM already printed **09-01**: PMI **54.6** (8th expansion month) vs July 55.6; new orders **53.7** (−3.0 pts). Still **expansion** — **not** an ISM-contraction HIT, but **slowing**. September ISM is **Oct 1**. 08-18/08-27: **cap S1 at 0/+1**; +2 forbidden without same-morning confirmation. GEV/ETN/VRT/FIX **cannot** raise the ETF.

- **ISM manufacturing / new orders expansion — CARRIED, slowing.** Electrical Equipment was one of the stronger August ISM industries; that is **not** today’s tape.
- **Durable goods / CapEx — MIXED, carried.** July durables **+1.1%** (aircraft-led). August IP: total **unchanged**, **manufacturing −0.3%** (first decline of the year) — already in the 09-18 tape. August durables due ~**Sep 25**.
- **Grid / electrical equipment backlog (AI power) — CARRIED structural, live tape is a SPLIT.** GEV ~$176B RPO / 116 GW gas book remains multi-year. News Judge #5 clusters LITE/CIEN/FIX as AI-infra. Channel 2 could **not** confirm a same-morning FIX “+11% backlog” print — latest FIX backlog is **Q2 $14.06B** (Jul 23). MAP HEAT **Electrical Equipment & Parts = SPLIT, conv=high**: VRT **−15.4% w1**, breadth **0.109**. 08-18: electrical SPLIT ≠ ETF raise; VRT must not drive XLI.
- **Aerospace & defense — CARRIED, not a same-morning HIT.** RTX Tomahawk/SM-3 ceilings and BA/RTX B-52 work are August awards. This morning: Jefferies **cuts BA PT to $265**; MAP HEAT Aerospace & Defense **dir=down, conv=low**. Do **not** cancel ISM slowing with one award.
- **Freight — CARRIED inflection, not a same-morning recovery HIT.** Cass August shipments **+2.1% y/y** (first y/y gain since Jan 2023, ~Sep 14 release). Oil-down is **not** S1 trucking relief (09-16). MAP HEAT Airlines **flat/low**.
- **Construction slowdown — CARRIED HIT.** August housing starts **1.275M** (−2.6% m/m, printed 09-17); manufacturing construction still off the 2025 peak; AI/nonres is the offset, not a broad build boom. MAP HEAT Building Products **dir=down, conv=medium** (HVAC captains −3% to −5.7% d1).
- **Reshoring / CHIPS — residual, not XLI spine.** IBM/Anderon $1B quantum foundry is News Judge’s **last** policy line. Do not import SMH/SOXX (ASML EUV) into XLI.

Net: slowing-but-expanding ISM + structural grid vs electrical SPLIT + construction drag + no same-morning spine print. **S1 = 0.** Not −1: ISM is still expansion; VRT is a single-ticker unwind. Not +1: 08-18 forbids using GEV/FIX as a raise on a SPLIT electrical tape.

### 3. Breadth / leadership — S2 = −1

This is the 09-21 hole, signed.

- MAP HEAT (nested, do not average into the parent): Aerospace down, Building Products down, Conglomerates down, Electrical **SPLIT/down**, E&C down, Farm & Heavy Machinery down. **Only Consulting is up** (VRSK/ICFI). That is not sector expansion.
- BreadthMarket / Barchart: **~24–25% of industrials above the 20-dma** (Barchart S&P Industrials 20-dma **25%**, 50-dma **14%** as of 09-22). 09-21’s ~16% above 20-dma has not healed.
- Leadership: high-beta electrical is the **unwind** (VRT), not the bid. RTY **+0.08%** is not small/mid cyclical leadership. Large-cap captains (CAT/GE/RTX/HON) are mixed-to-soft on the week, not a quality bid that lifts the ETF.
- 09-21 relative clause: leftover XLK/AI beta vs XLI **PM −0.75%** (worst on the board) is **rotation out of industrials**, not a delayed catch-up.

**S2 = −1.** Internals, not a second copy of 1m SPY-rel (that lives in S4). Not −2: consulting/E&C captains are not a washout crash.

### 4. Flows / positioning — S3 = 0

- ETF.com/ETFdb: XLI **−$212M** (09-15), **5d ~−$213M**, **1m ~−$1.05B**; 1y still positive. AUM ~$30–32B. Real **outflow**, but **lagged** — not a same-morning relative-volume spike.
- Not a crowded long (1m rel **−6.87%** is the opposite).
- Rotation-out is already in S2. Policy S3 sign-hit is weak (×0.5). Do not stack the same rotation into S3.

**S3 = 0.** Checked; nothing material as a *same-session* flow impulse. Carried 1m outflows noted, not re-scored.

### 5. ETF tape confirmation — S4 = −1

Channel 1 (do not alter):

- 1d: XLI **+0.40%** | SPY **+1.55%** | rel **−1.15%**
- 3d: rel **−1.81%**
- 1w: rel **−1.61%**
- 1m: rel **−6.87%**

All four horizons **negative**. Independent same-session fact (09-04 exception / 09-14 convert-the-gap): **PM:XLI −0.75%** vs Finviz ES **+0.20%** / ES=F **−0.07%**. 09-10 decay does **not** zero this: yesterday’s −1.15% 1d rel is a **fresh** relative miss continuing 09-21’s −1.44 pp, not a stretched-oversold shock-day decay. Persistent 1m ≤ −5% is scored **once here**, not again as S2.

**S4 = −1** (confirmation only; not the main thesis). Thesis is internals + non-participation. Tape **confirms**.

### Horizons (context, not a second score)

| Horizon | XLI vs SPY rel (Ch1) | Read |
|---|---|---|
| **3D** | **−1.81%** | Relative lag **accelerating** through the AI-beta session |
| **1W** | **−1.61%** | 08-27 forbid-up still on |
| **2W** | n/a in Ch1; 1w+1m both red | No mean-reversion evidence in the injected tape |
| **1M** | **−6.87%** | Deep laggard **CONDITION** (09-04/09-10); needs a fresh catalyst to flip — none at the open |

### Self-audit

- **Lens:** XLI cash session, not SPX, not VRT/FIX/GEV stock-picking.
- **Band:** Pipeline owns totals. Leading sum S0..S4 = **−2** before multiplier. size_gate=True. Fed speakers cut conviction. 08-27 **forbids up**.
- **Skew:** Cyclical; leftover risk-on is **XLK**, not XLI. Polarity of “risk-on tape” for this book this morning is **not** positive.
- **Same-shock double-count:** Oil once in S0. 1m SPY-rel once in S4. MAP HEAT internals in S2. Grid not re-used as S1 raise. Paid FOMC not restacked.
- **Single-ticker:** VRT SPLIT and FIX/BA headlines **must not** drive the ETF call (08-18).
- **Divergence:** Leading factors (S2+S4) and tape confirmation **agree** negative. **No leading-vs-tape fight.** 09-21 correction: do **not** flatten this back to all-zero. 09-17 keep-flat does not apply to a signed relative card. 09-09 oil-shock emit-down remains **off** — this is a **relative-underperformance** card in a mixed index tape, not a supply-shock smash.

**Path qualifier:** PM:XLI −0.75% is already a gap. If the cash session merely holds that gap, the move is **gap-and-hold down**, not an industrials trend day (09-11 inverse). Williams/Jefferson are two-sided for *rates path*, not a license to widen the band.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 0
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -1
MULTIPLIER: 0.9
CONFIDENCE: 0.48
REGIME: mixed
HORIZON_3D: -1
HORIZON_1W: -1
HORIZON_2W: -1
HORIZON_1M: -1
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|PARTIAL|0.40|2026-09-22|https://www.tipranks.com/news/u-s-stock-futures-hold-steady-after-sp-500-rally
Risk-off tape / flight to safety|MISS|0.70|2026-09-22|Channel 1 VIX 14.88, VIX/VIX3M 0.823
Real yields rising|MISS|0.60|2026-09-22|Finviz 10Y note -0.03%; DGS10 5.01 is Friday level
Real yields falling|PARTIAL|0.40|2026-09-22|tiny note bid only; DFII10 2.68 still elevated
USD strengthening|MISS|0.55|2026-09-22|DXY +0.06% / Finviz USD -0.02%
USD weakening|MISS|0.55|2026-09-22|flat dollar
Sector breadth expansion (% names up)|MISS|0.75|2026-09-22|https://www.barchart.com/stocks/market-performance
Sector breadth failure (ETF up, names flat)|PARTIAL|0.55|2026-09-22|PM:XLI already red; MAP HEAT industries down — participation failure vs SPY, not ETF-up/names-flat
Large-cap leadership inside sector|PARTIAL|0.45|2026-09-22|MAP HEAT CAT/GE/RTX mixed-to-down
Small/mid leadership inside sector|MISS|0.60|2026-09-22|Finviz RTY +0.08%
High-beta leadership inside sector|MISS|0.70|2026-09-22|MAP HEAT Electrical SPLIT, VRT -15.4% w1
Low-beta leadership inside sector|MISS|0.50|2026-09-22|not a defensive bid inside XLI
Sector ETF inflow / relative volume spike|MISS|0.65|2026-09-22|checked, nothing material
Sector ETF outflow / volume dry-up|CARRIED|0.60|2026-09-19|https://etfdb.com/etf/XLI/
Crowded long (extreme relative performance + valuation)|MISS|0.80|2026-09-21|1m rel -6.87%
Index rebalance / inclusion tailwind|MISS|0.80|2026-09-22|checked, nothing material
Index exclusion / forced selling|MISS|0.80|2026-09-22|checked, nothing material
ISM manufacturing / new orders expansion|CARRIED|0.70|2026-09-01|https://www.prnewswire.com/news-releases/manufacturing-pmi-at-54-6-august-2026-ism-manufacturing-pmi-report-302865127.html
Durable goods / CapEx upside|CARRIED|0.50|2026-09-18|https://www.reuters.com/business/us-manufacturing-output-falls-august-after-rising-seven-straight-months-2026-09-18/
Grid / electrical equipment backlog (AI power)|CARRIED|0.65|2026-09-16|https://pulse2.com/ge-vernova-backlog-reaches-176-billion-as-power-and-electrification-demand-accelerates/
Aerospace & defense order / budget upside|CARRIED|0.45|2026-09-22|https://www.govconwire.com/?s=RTX
Freight / trucking / rail volume recovery|CARRIED|0.50|2026-09-14|https://www.freightwaves.com/news/cass-tl-rates-jump-11-in-august-freight-shipments-turn-positive
Reshoring / industrial policy funding|CARRIED|0.35|2026-09-22|News Judge IBM/Anderon residual — not XLI spine
ISM contraction|MISS|0.80|2026-09-01|PMI 54.6 still expansion
CapEx cuts / order cancellation|MISS|0.55|2026-09-22|no same-morning cancellation print
Freight recession|MISS|0.60|2026-09-14|Cass Aug shipments +2.1% y/y inflection
Construction slowdown|CARRIED|0.60|2026-09-17|https://www.census.gov/construction/nrc/pdf/newresconst_202608.pdf
Sector rotation into industrials|MISS|0.75|2026-09-22|PM:XLI worst on board
Sector rotation out of industrials|HIT|0.70|2026-09-22|Channel 1 PM:XLI -0.75%; 1d rel -1.15%; MAP HEAT mostly down
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- memory_search: Industrials XLI sector prediction lessons ISM grid defense freight (index disabled)
- web_search: ISM manufacturing PMI September 2026 durable goods industrial production
- web_search: XLI industrials ETF premarket September 22 2026 flows breadth CAT GE Vernova
- web_search: US stock futures September 22 2026 Treasury yields oil industrials
- web_search: GE Vernova grid backlog electrical equipment VRT Hubbell industrials 2026
- web_search: Cass Freight Index trucking rail volumes September 2026
- web_search: Fed speakers calendar September 22 2026 Goolsbee Bowman
- web_search: aerospace defense orders Boeing RTX budget September 2026
- web_search: Comfort Systems FIX +11% backlog news September 2026
- web_search: Vertiv VRT stock drop September 2026 electrical equipment
- web_search: XLI ETF fund flows September 2026
- web_search: CME FedWatch October 2026 rate hike odds September 22
- web_search: US housing starts construction spending industrials September 2026
- web_search: reshoring CHIPS Act industrial policy funding September 2026
- web_search: Philip Jefferson John Williams Fed speech September 22 2026
- web_search: industrials sector breadth percent stocks above 20 day moving average September 22 2026
- web_fetch: ISM August 2026 PR Newswire; NY Fed Treasury Market Conference agenda; Tipranks futures (403)
- x_search: XLI industrials ETF premarket CAT GE Vernova Vertiv September 22 2026 (2026-09-21 to 2026-09-22)

**Key sources (title + URL + timestamp where available) and facts taken**

1. ISM August 2026 Manufacturing PMI report — https://www.prnewswire.com/news-releases/manufacturing-pmi-at-54-6-august-2026-ism-manufacturing-pmi-report-302865127.html — fetched 2026-09-22T10:28Z. PMI 54.6 (−1.0 vs July 55.6); New Orders 53.7 (−3.0); Production 58.3; Prices 71.1; Backlog 51.8. September report due Oct 1.
2. NAM / Fed G.17 — https://nam.org/manufacturing-output-declines-0-3-in-august-as-industrial-production-holds-steady/ and https://www.reuters.com/business/us-manufacturing-output-falls-august-after-rising-seven-straight-months-2026-09-18/ — August IP unchanged; manufacturing −0.3%; cap-u 76.3%.
3. Trading Economics durables — https://tradingeconomics.com/united-states/durable-goods-orders — July +1.1% to $339.3B; August due ~Sep 25.
4. Census housing starts (Aug, released Sep 17) — https://www.census.gov/construction/nrc/pdf/newresconst_202608.pdf — 1.275M SAAR, −2.6% m/m.
5. Tipranks / Yahoo live — https://www.tipranks.com/news/u-s-stock-futures-hold-steady-after-sp-500-rally — 09-22 futures little changed after 09-21 AI rally (SPX ~+1.5%, Nasdaq ~+2.3%); industrials lagged Monday.
6. NY Fed Treasury Market Conference — https://www.newyorkfed.org/newsevents/events/markets/2026/0922-2026 — fetched 2026-09-22T10:28Z. Williams remarks 10:05; Jefferson 10:20 ET. Agenda: discount window / Treasury functioning, not FOMC/SEP.
7. Fed calendar — https://www.federalreserve.gov/newsevents/2026-september.htm — no Goolsbee/Bowman on 09-22; Goolsbee spoke 09-21 (non-voting alternate).
8. CME FedWatch secondary — https://news.bitcoin.com/finance/feds-next-rate-move-has-traders-staring-at-a-coin-toss/ — Oct meeting ~55–58% hike / ~42–44% hold as of 09-17/18.
9. Pulse2 / GEV backlog — https://pulse2.com/ge-vernova-backlog-reaches-176-billion-as-power-and-electrification-demand-accelerates/ — $176B backlog, Electrification book, data-center orders >$5B H1; carried, not same-morning.
10. FreightWaves Cass — https://www.freightwaves.com/news/cass-tl-rates-jump-11-in-august-freight-shipments-turn-positive — Aug shipments +2.1% y/y, first since Jan 2023; September Cass not out.
11. ETFdb / ETF.com flows — https://etfdb.com/etf/XLI/ and https://www.etf.com/sections/daily-etf-flows/daily-etf-flows-iei-sheds-assets — 09-15 −$212M; 5d ~−$213M; 1m ~−$1.05B.
12. Barchart breadth — https://www.barchart.com/stocks/market-performance — S&P Industrials 20-dma 25%, 50-dma 14% on 09-22.
13. VRT tape — https://ts2.tech/en/vertiv-stock-drops-9-6-and-erases-82-of-its-post-deal-rally/ — mid-Sep acquisition-skepticism drop; MAP HEAT w1 −15.4%.
14. FIX backlog — https://investors.comfortsystemsusa.com/node/18631/html — Q2 $14.06B (Jun 30); **no confirmed Sep 22 +11% backlog print**.
15. GovConWire / Bloomberg RTX — https://www.govconwire.com/?s=RTX and https://www.bloomberg.com/news/articles/2026-08-17/rtx-wins-22-9-billion-navy-missile-contract-amid-supply-strain — August Tomahawk/SM-3 awards; not a same-morning XLI catalyst. Jefferies BA PT cut in Finviz digest.
16. Channel 1 injected panel (pipeline, 2026-09-22) — VIX 14.88; Finviz ES/NQ/RTY/DJIA +0.20/+0.41/+0.08/+0.11; ES=F/NQ=F −0.07/−0.07; PM:XLI −0.75%; CL=F −4.78%; XLI vs SPY rel 1d/3d/1w/1m −1.15/−1.81/−1.61/−6.87%.

**Not used as same-morning S1:** ASML 2027 EUV (SMH, not XLI); AMD $1T leftover; IBM quantum foundry (News Judge last); AME Indicor close (stale).

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -2.0, 'divergence_flagged': False, 'total_score': -4.127, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.65, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': -0.5172, 'score': -3.103, 'legs': [{'leg': 'ES', 'pct': -0.07, 'w': 0.8}, {'leg': 'ER2', 'pct': 0.08, 'w': 0.2}, {'leg': 'HG', 'pct': 0.66, 'w': 0.1}, {'leg': 'PM:XLI', 'pct': -0.75, 'w': 0.7}]}, 'overlay_score': -0.9, 'overlay_raw': -0.9, 'index_carry': -0.124, 'general_total': -0.497, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 0.5, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 0.5}, 'llm_confidence': 0.48, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
