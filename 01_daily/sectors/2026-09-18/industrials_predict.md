# Sector Prediction — Industrials — 2026-09-18

- ETF: **XLI**
- rubric: `00_grounding/sectors/industrials.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **4.138** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **2.923** (ES +1.14%, ER2 +0.08%, HG +0.66%, PM:XLI +0.27%) · index_carry **1.215** (general 4.861) · llm_overlay **0.0** (raw 0.0)

## Channel 1 sector ETF tape

```
ETF XLI vs SPY (yfinance, through 2026-09-17):
  1d: XLI +0.18% | SPY +1.13% | rel -0.96%
  3d: XLI -0.54% | SPY +0.23% | rel -0.77%
  1w: XLI -0.90% | SPY +0.63% | rel -1.53%
  1m: XLI -7.93% | SPY -0.63% | rel -7.30%
```

MEMORY_CONFIRM: Memory index unavailable this run (embedding metadata mismatch; `openclaw memory status --index` or `openclaw memory index --force` would rebuild). Used injected Industrials scoreboard + 08-11..09-17 sector logs only. Rolling dir=0.5 / mag=0.5 (n=10); last 30 dir=0.364 / mag=0.273 (n=22). Last graded 09-17: predicted flat/flat, actual XLI +0.178% / SPY +1.134% / rel −0.956% — **dir MISS, mag HIT** (gap-and-fade from 171.51 → 169.01). Prior: 09-16 flat/flat HIT, 09-15 down/mild HIT, 09-14 down/notable HIT, 09-11 up/mild HIT, 09-10 down/mild HIT; 09-08/09-09 flat/flat MISSes on oil-shock days. **Governing today: 09-17 (NONE/D) — unsigned post-paid-FOMC XLI card, 1w/1m lag forbids up, oil offered, operator futures not unanimous ≥+0.5% across ES/NQ/RTY/DJIA, tech-led PM, large overnight ES-vs-cash sleeve → keep close-to-close flat/flat; do not promote the overnight ES gap into up, and do not promote yesterday’s post-close relative lag (−0.96%) into down.** 09-16 — all-zero card + oil down + incomplete four-index confirm → flat/flat; 09-03 is path variance around an unprinted print, not a mandate to emit mild with no directional lean; do not map crude-down onto trucking as S1 relief. 09-15 better-than-index PM-gap fade **OFF** (no live oil/duration shock). 09-14 S4=−1 persistent-lag **OFF** (needs live oil/duration plus a *worse*-than-index PM gap; PM:XLI +0.27% vs Finviz ES +0.20%). 09-11 unanimous ≥+0.5% across all four **OFF**. 09-09 emit-down **OFF** (oil **down**, futures green; 09-09 needs a live supply shock with confirming negative tape). 09-10 decay — 1m rel −7.30% is a CONDITION; do not use yesterday’s 1d rel as acceleration. 09-04 score the laggard **once**. 08-27 — 1w/1m laggard **forbids up**. 08-21 reversal **partial** (NQ +0.41% ≥ +0.3%, ES +0.20% not). 08-18 — cap S1 at 0/+1; GEV/grid **not** a cushion; VRT does not drive the ETF. 08-11/08-12 **does not fire** (live oil **down**). 08-13 — Hormuz/tanker headline is the stale leg; session oil change is down; do **not** treat oil-down as a cyclical green light. Fed-speaker lesson — Bowman (stress testing) + Schmid on calendar; Oct hike odds contested (~45–50%); keep S0 directionally 0 and cut confidence; do **not** restack paid 09-16 FOMC/Warsh. Open experiment (`sector_industrials`): keep direction, shrink confidence on modest |score| — **applied**. DO-INSTEAD 09-17 loss: score vs still-negative 3d/1w/1m tape → **binding as a flatten, not as a down call**. Checklist: (1) open experiment applied (shrink confidence); (2) no missing factor that would flip 09-17’s keep-flat (falsifier needs industrials-led breadth or four-index confirm — both absent at the open); (3) oil / paid FOMC / 1m lag each counted once; (4) S0 vs S1 both 0 — FOMC paid, no fresh spine print, IP unprinted.

## XLI near-session environment (not an SPX call)

Object is the **Sep 18 cash session for XLI**, not SPX and not a stock pick. Channel 1 numbers used as given.

### 1. Shared macro as it hits Industrials — S0 = 0

This is a **post-FOMC, oil-offered, tech-led pause**, not 09-14/09-15’s oil-up/yields-up smash and not 09-11’s unanimous +0.5% de-risking bounce. Same unsigned shape as 09-16/09-17.

- **FOMC is paid, not pending.** 09-16: +25 bp to 3.75–4.00%, 12–0. News Judge #1–2 (Warsh JH hike-odds / gold −3%; IWM + BNY prime +25 bp) are **prior-session / same hawkish spine already in yesterday’s close**. Do **not** restack as a second S0 shock. October hike odds ~45–50% are **contested**, not a same-morning binary to pre-score.
- **Futures are green but not independently confirming a cyclical bid.** Channel 1 Finviz: ES **+0.20%**, NQ **+0.41%**, RTY **+0.08%**, DJIA **+0.11%**. The `ES=F +1.14% / NQ=F +1.50% vs prev close` sleeve is the same sign, larger overnight gap. **Do not re-derive** (09-17). Finviz is the live operator tape. 08-21’s ES/NQ ≥ +0.3% gate is **partial** (NQ only). 09-11’s unanimous ≥ +0.5% *across all four* is **off**. RTY +0.08% is not a cyclical bid. Sector PM: **XLK +0.60% vs XLI +0.27%** — the bounce is **tech-led**, not industrials-led.
- **Oil is DOWN, not a fresh squeeze.** Finviz WTI **$104.16 (−1.59%)**, Brent **$107.67 (−1.02%)**; `CL=F −6.31% / BZ=F −6.17%` is the multi-session 1d sleeve (same conflict-handling as 09-15/16/17). Absolute level is still elevated — a **cost LEVEL** for transports/manufacturers, not a same-session supply shock. News Judge #3: oil drop / inventory optics allay inflation while hitting E&Ps. Channel 2: crude extending a 3-day pullback even with stale Houthi/Saudi headlines. 08-11/08-12 **does not fire**. 08-13: tanker/Hormuz is the stale leg; live change is a pullback. Count oil **once, here**. Do **not** treat oil-down as a full cyclical tailwind, and do **not** treat ~$104 oil as a live squeeze.
- **Rates: level still high, session change is not a backup.** DGS10 **5.01** / DGS30 **5.35** / DFII10 **2.68** (+0.06 1d, +0.22 1w). Live Finviz: 10Y note **−0.03%**, 30Y **−0.06%** — tiny price dip, not 09-15’s long-end washout. Real-yield *level* is a condition; the *1d change* is not a second S0 shock. 5-day 10Y–SPX corr **−0.437** (moderate, not −0.97).
- **Globals mixed, USD flat-to-firm, vol easing.** Asia composite **+1.11%** (Kospi +2.66%, Nikkei +1.38%). Europe **−0.46%** (FTSE −0.53%, DAX −0.41%, CAC −0.56%). Do not let Asia-only green set a cyclical bid when Europe is red and US operator futures are sub-gate. DXY **+0.12% 1d**. VIX **15.22** with VIX/VIX3M **0.82** (deep contango). HY OAS **2.70** (tight). Copper Finviz **+0.66%** vs News Judge copper-retreat-from-records — metals **mixed**, not a growth-scare smash.
- **Calendar is two-sided and unscored.** 9:15 ET **Industrial Production / cap-util** (consensus ~+0.3% / 76.4%) is the XLI-relevant print — do **not** pre-score a miss or a beat. 10:00 LEI is secondary. Bowman (stress-testing, London) + Schmid are on the calendar with **contested** October hike odds: Fed-speaker lesson → keep S0 **directionally 0**, cut confidence, mark as unresolved-policy overlay. Not a Chair/FOMC path binary.

**S0 = 0, regime mixed.** Not −1: oil is confirmed down, futures are green, no live yield spike, no kinetic increment, FOMC paid. Not +1: four-index confirmation fails, Europe red, 08-13 blocks treating oil-down as a cyclical green light, 1w/1m XLI is a laggard, IP unprinted, Fed speakers unresolved. Oil counted **once here**.

### 2. Spine + secondary — S1 = 0 (capped)

**No fresh same-morning industrials hard print in hand.** August ISM manufacturing already printed **09-01**: PMI **54.6** vs July 55.6; new orders **53.7** (−3.0 pts). Still **expansion** — **not** an ISM-contraction HIT, but **slowing**. September ISM is **Oct 1**. Durable goods: July **+1.1%** is stale; August advance is **Sep 25**. 08-18/08-27: **cap S1 at 0/+1**; +2 is forbidden without same-morning confirmation. 09-16: do **not** map WTI/Brent down onto trucking/air as S1 relief.

- **Grid / electrical equipment backlog (AI power) — HIT, carried, internally splitting.** GEV ~$176B RPO / 116 GW gas book, CEO still talking $200B early-2027 — structural, semi-independent of ISM. **MAP HEAT SPLIT confirmed:** VRT **−15.38% w1** on AI-thermal timeline doubt, electrical-equipment breadth 0.109, while ATKR is flat — the group is **breaking, not dipping**. News Judge #7: FIX backlog vs APH −6.5% — do not count “AI demand” as one-way. 08-18: GEV/ETN are **not** a downside cushion and **not** a same-session raise. VRT is a single-ticker/sleeve — **must not drive the ETF call**. Net grid = wash, not +1.
- **Aerospace & defense — MIXED, already-traded.** Boeing KC-46 FMS **ceiling** raise (not obligated cash), Korean Air 103-jet order **09-16** (in yesterday’s tape), MQ-25 LRIP **09-17**. MAP HEAT A&D dir=**down** (GE capital-deal overhang, RTX valuation). Do **not** cancel ISM (expansion, not weak) with one award, and do **not** treat IDIQ ceilings as a same-morning spine HIT.
- **Freight / trucking / rail — MIXED.** Cass August shipments **+2.1% YoY** (first gain after a 42-month slump) is a mid-month inflection, already in the tape — not a same-morning recovery HIT. AAR week ending Sep 12: rail **−3.7% YoY**. Oil-down is **not** scored again here (09-16).
- **Construction slowdown — HIT, carried.** August housing starts **1.275M (−2.6% m/m)** printed **09-17**; multifamily **−22.5%**. Single-family +7.6% is the split, not a broad build boom. AI/nonres remains the offset. In the tape; not a same-morning raise.
- **Reshoring / industrial policy — checked, nothing material** same-morning.
- **ISM contraction / CapEx cuts — NOT in play.** ISM still >50; no fresh order-cancellation print.

Net: carried ISM expansion (slowing) + structural grid that is **internally splitting** vs carried construction drag + mixed freight + already-traded A&D ceilings. **S1 = 0** (capped; no fresh same-morning confirmation; GEV/VRT/BA do not set the ETF).

### 3. Breadth — S2 = 0

XLI is a **deep multi-horizon laggard** (1m rel **−7.30%**). That is a **CONDITION**, not a same-day forecast (09-04 / 09-10 / 09-17). Score it **once**, and not as a signed down factor today: 09-14’s S4=−1 stack is **off**, and 09-17 **forbids** promoting yesterday’s post-close rel (−0.96%) into a down call.

MAP HEAT: Aerospace, building products, conglomerates, electrical (SPLIT), airlines, E&C, machinery all **dir=down**; only Consulting is up (VRSK product launch — must not drive XLI). 09-17 close: only **~7%** of S&P industrials above the 20-day SMA. That is the same underperformance already in 1w/1m, not an independent same-morning breadth event. Leadership is **not** expanding inside the sector; it is also **not** a fresh catalyst. **S2 = 0.**

### 4. Flows / positioning — S3 = 0

Channel 2: XLI **−$212.4M** net redemptions on **Sep 15** (~0.69% of AUM); ~**−$1.09B** over 1 month. That is the flow twin of the 1m lag, not a same-morning volume spike. Not a crowded long (1m rel −7.30%). 1y flows still net positive. No independent forced-flow / rebalance print this morning. Score the lag **once** (as condition); do not restack as S3. **S3 = 0.**

### 5. ETF tape (confirmation only) — S4 = 0

Channel 1 through 09-17: 1d rel **−0.96%**, 3d **−0.77%**, 1w **−1.53%**, 1m **−7.30%**. Decisive medium-term lag. **S4 is confirmation only, never the main thesis.** 09-17: yesterday’s −0.96% rel was **path** (tech-led SPY +1.13%, XLI faded a leftover gap) — do **not** promote it into down. 09-14 S4=−1 required live oil/duration **plus** a worse-than-index PM gap — **both absent** (oil down; PM:XLI **+0.27%** vs Finviz ES **+0.20%**). 09-10: even if the 1d rel is read, it is a decaying relative-beta signal on a deep laggard, not acceleration. Independent same-session tape fact (fresh gap vs ES, volume spike, intraday reversal) is **not** present. **S4 = 0.**

### 6. Catalysts / calendar

- **9:15 ET Industrial Production / cap-util** — two-sided, **unscored** until print. 09-16/09-17: this is path variance, not a license to lift an all-zero card to mild.
- **10:00 LEI** — secondary.
- **Bowman / Schmid** — unresolved-policy overlay; S0 stays 0; confidence down.
- **FOMC/Warsh** — paid 09-16; do not restack.
- **ASML EUV / BAC / copper tariffs** — not XLI spine (XLK / XLF / XLB).

### Self-audit

- **Lens:** cyclical. Rates/oil only in S0. Grid/VRT/construction only in S1. Lag not double-counted in S2 and S4.
- **Band:** **flat**, not mild. 09-03 does not inflate magnitude on an unsigned card (09-16). size_gate is on.
- **Skew:** VRT, GEV, CAT, BA, VRSK do **not** drive the ETF call.
- **Same-shock:** oil once in S0; paid FOMC not restacked; 1m lag scored as condition, not as S2+S4.
- **Single-ticker:** electrical SPLIT noted; cannot set S1 to −1 or +1.

**Divergence:** Leading S0–S3 = 0 vs still-negative 3d/1w/1m relative tape. **Flagged.** Trust **factors over tape** → do not let the lag write a down call. DO-INSTEAD 09-17: prefer **flat**. 08-27 forbids **up**. 09-17 falsifier (industrials-led breadth or four-index ≥+0.5%) is **not** met at the open.

**Horizons:** 3d flat (unsigned near-session, IP two-sided). 1w/2w/1m still a relative laggard until a spine print or a true cyclical four-index bid — that is a condition, not today’s signed call.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 0
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.40
REGIME: mixed
HORIZON_3D: flat
HORIZON_1W: down
HORIZON_2W: down
HORIZON_1M: down
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|PARTIAL|0.55|2026-09-18|https://www.morningstar.com/news/dow-jones/202609181836/stock-futures-rise-as-oil-extends-losses-treasury-yields-rise
Risk-off tape / flight to safety|ABSENT|0.70|2026-09-18|https://www.morningstar.com/news/dow-jones/202609181836/stock-futures-rise-as-oil-extends-losses-treasury-yields-rise
Real yields rising|PARTIAL|0.60|2026-09-16|Channel 1 DFII10 2.68 (+0.06 1d / +0.22 1w)
Real yields falling|ABSENT|0.65|2026-09-18|Channel 1 10Y note -0.03% / 30Y -0.06%
USD strengthening|PARTIAL|0.50|2026-09-18|Channel 1 DXY +0.12% 1d
USD weakening|ABSENT|0.55|2026-09-18|Channel 1 DXY +0.12%
Sector breadth expansion (% names up)|ABSENT|0.75|2026-09-17|https://breadthmarket.com/
Sector breadth failure (ETF up, names flat)|HIT|0.70|2026-09-17|Channel 1 1d XLI +0.18% vs SPY +1.13% (path; not re-scored)
Large-cap leadership inside sector|PARTIAL|0.55|2026-09-18|MAP HEAT electrical SPLIT / CAT drag
Small/mid leadership inside sector|ABSENT|0.60|2026-09-18|MAP HEAT RTY captains mixed-to-down
High-beta leadership inside sector|ABSENT|0.60|2026-09-18|XLK PM +0.60% vs XLI +0.27%
Low-beta leadership inside sector|ABSENT|0.50|2026-09-18|checked, nothing material
Sector ETF inflow / relative volume spike|ABSENT|0.70|2026-09-15|https://www.etf.com/sections/daily-etf-flows/daily-etf-flows-iei-sheds-assets
Sector ETF outflow / volume dry-up|PARTIAL|0.65|2026-09-15|https://www.etf.com/sections/daily-etf-flows/daily-etf-flows-iei-sheds-assets
Crowded long (extreme relative performance + valuation)|ABSENT|0.75|2026-09-17|Channel 1 1m rel -7.30%
Index rebalance / inclusion tailwind|ABSENT|0.50|2026-09-18|checked, nothing material
Index exclusion / forced selling|ABSENT|0.50|2026-09-18|checked, nothing material
ISM manufacturing / new orders expansion|PARTIAL|0.80|2026-09-01|https://www.prnewswire.com/news-releases/manufacturing-pmi-at-54-6-august-2026-ism-manufacturing-pmi-report-302865127.html
Durable goods / CapEx upside|PARTIAL|0.55|2026-08|https://tradingeconomics.com/united-states/durable-goods-orders
Grid / electrical equipment backlog (AI power)|HIT|0.75|2026-09-16|https://247wallst.com/investing/2026/09/16/ge-vernova-climbs-5-as-ceo-sees-backlog-hitting-200b-early-eaton-and-quanta-services-edge-higher/
Aerospace & defense order / budget upside|PARTIAL|0.55|2026-09-17|https://thedefensepost.com/2026/09/17/us-stingray-drone-tanker/
Freight / trucking / rail volume recovery|PARTIAL|0.60|2026-09-14|https://www.freightwaves.com/news/cass-tl-rates-jump-11-in-august-freight-shipments-turn-positive
Reshoring / industrial policy funding|ABSENT|0.50|2026-09-18|checked, nothing material
ISM contraction|ABSENT|0.85|2026-09-01|https://www.prnewswire.com/news-releases/manufacturing-pmi-at-54-6-august-2026-ism-manufacturing-pmi-report-302865127.html
CapEx cuts / order cancellation|ABSENT|0.55|2026-09-18|checked, nothing material
Freight recession|ABSENT|0.55|2026-09-14|https://www.freightwaves.com/news/cass-tl-rates-jump-11-in-august-freight-shipments-turn-positive
Construction slowdown|HIT|0.70|2026-09-17|https://www.census.gov/construction/nrc/current/index.html
Sector rotation into industrials|ABSENT|0.70|2026-09-18|Channel 1 1m rel -7.30%; XLK leads PM
Sector rotation out of industrials|PARTIAL|0.60|2026-09-15|https://www.etf.com/sections/daily-etf-flows/daily-etf-flows-iei-sheds-assets
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- ISM manufacturing PMI durable goods orders September 2026
- XLI industrials ETF flows positioning September 2026
- GE Vernova grid backlog AI power electrical equipment VRT September 2026
- US freight trucking rail volumes Cass Freight Index September 2026
- Philly Fed housing starts construction industrials September 17 2026
- oil prices WTI Brent inventory build September 18 2026 industrials
- Fed Warsh FOMC rate hike odds September 18 2026
- US economic calendar September 18 2026 Fed speaker
- Boeing defense orders aerospace budget September 2026
- housing starts August 2026 construction slowdown industrials
- XLI vs SPY relative performance breadth industrials stocks September 17 2026
- Vertiv VRT electrical equipment selloff September 2026
- risk on risk off stock market September 18 2026 premarket
- US industrial production August 2026 forecast September 18
- XLI ETF daily flows September 15 2026 -212 million
- X search: XLI industrials premarket oil yields September 18 2026 (from 2026-09-17 to 2026-09-18)
- web_fetch: ISM August 2026 PR Newswire release

**Key sources (title + URL + timestamp/facts taken)**

1. ISM August 2026 Manufacturing PMI report — https://www.prnewswire.com/news-releases/manufacturing-pmi-at-54-6-august-2026-ism-manufacturing-pmi-report-302865127.html — fetched 2026-09-18. PMI 54.6 (8th expansion month), new orders 53.7 (−3.0 pts), production 58.3, employment 51.2, prices 71.1. September ISM due Oct 1.
2. Trading Economics durable goods — https://tradingeconomics.com/united-states/durable-goods-orders — July 2026 +1.1% to $339.3B; August advance due Sep 25.
3. ETF.com daily flows — https://www.etf.com/sections/daily-etf-flows/daily-etf-flows-iei-sheds-assets — Sep 15 XLI −$212.42M (~0.69% AUM).
4. ETFDB XLI profile — https://etfdb.com/etf/XLI/ — ~−$1.09B 1-month net flows.
5. 24/7 Wall St GEV backlog — https://247wallst.com/investing/2026/09/16/ge-vernova-climbs-5-as-ceo-sees-backlog-hitting-200b-early-eaton-and-quanta-services-edge-higher/ — 2026-09-16: ~$176B backlog, CEO $200B early-2027.
6. FreightWaves Cass August — https://www.freightwaves.com/news/cass-tl-rates-jump-11-in-august-freight-shipments-turn-positive — shipments +2.1% YoY (first after 42-month slump); TL linehaul +11.3% YoY.
7. FreightWaves rail — https://www.freightwaves.com/news/rail-freight-slides-in-rare-off-week — week ending Sep 12 total rail −3.7% YoY.
8. Morningstar / Philly Fed Sep 17 — https://www.morningstar.com/news/dow-jones/202609174437/philadelphia-area-manufacturing-activity-kept-climbing-in-september — Philly Fed 37.8 vs 47.4 prior, beat ~30 consensus; paid 09-17.
9. Census housing starts — https://www.census.gov/construction/nrc/current/index.html — August starts 1.275M SAAR, −2.6% m/m; multifamily −22.5%; permits 1.394M −2.7%.
10. Morningstar futures 09-18 — https://www.morningstar.com/news/dow-jones/202609181836/stock-futures-rise-as-oil-extends-losses-treasury-yields-rise — futures green, oil extending losses, yields mixed-to-up.
11. FinanceFeeds Fed odds — https://financefeeds.com/will-the-fed-raise-interest-rates-again-october-odds-45/ — Sep hike paid +25 bp to 3.75–4.00%; Oct ~45–50%.
12. TipRanks / calendar — https://www.tipranks.com/calendars/economic — Sep 18: 9:15 Industrial Production; Bowman + Schmid speeches.
13. Fed G.17 / IP preview — https://www.morningstar.com/news/dow-jones/202609174814/industrial-production-on-tap-data-week-ahead — IP consensus +0.3% MoM, cap-util 76.4%.
14. Defense Post MQ-25 — https://thedefensepost.com/2026/09/17/us-stingray-drone-tanker/ — 2026-09-17 $562M LRIP.
15. GovConWire KC-46 — https://www.govconwire.com/articles/boeing-13-4b-air-force-kc-46-fms-contract-modification — $13.4B **ceiling** increase, no immediate obligation.
16. BreadthMarket — https://breadthmarket.com/ — ~7.23% of S&P industrials above 20-day SMA as of Sep 17 close.
17. Channel 1 (pipeline, do not alter) — VIX 15.22 / ratio 0.82; Finviz ES +0.20% NQ +0.41% RTY +0.08% DJIA +0.11%; WTI −1.59% Brent −1.02%; XLI PM +0.27% XLK +0.60%; Asia +1.11% Europe −0.46%; DGS10 5.01 DFII10 2.68; XLI vs SPY 1d/3d/1w/1m rel −0.96/−0.77/−1.53/−7.30%.

**Facts used vs discarded**
- Used: operator Finviz futures (not the ES=F +1.14% cash sleeve); oil **down** as S0 cost-level; ISM 54.6 carried; GEV backlog carried but VRT split offsets; Cass inflection mixed with rail −3.7%; starts 1.275M construction drag carried; XLI Sep 15 outflow as flow twin of 1m lag; IP/Bowman unprinted.
- Discarded / not re-derived: News Judge Warsh/gold as a **today** shock (paid); CL=F −6.31% as a same-session squeeze (multi-day sleeve; Finviz −1.59% is the live quote); oil-down as S1 trucking relief; Boeing IDIQ ceilings as obligated awards; ASML/BAC as XLI spine; Asia +1.11% as a cyclical bid against Europe −0.46% and sub-gate RTY.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': 0.0, 'divergence_flagged': False, 'total_score': 4.138, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.55, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.4872, 'score': 2.923, 'legs': [{'leg': 'ES', 'pct': 1.14, 'w': 0.8}, {'leg': 'ER2', 'pct': 0.08, 'w': 0.2}, {'leg': 'HG', 'pct': 0.66, 'w': 0.1}, {'leg': 'PM:XLI', 'pct': 0.27, 'w': 0.7}]}, 'overlay_score': 0.0, 'overlay_raw': 0.0, 'index_carry': 1.215, 'general_total': 4.861, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 0.5, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 0.5}, 'llm_confidence': 0.4, 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': -1.65, 'w1': -2.74}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
