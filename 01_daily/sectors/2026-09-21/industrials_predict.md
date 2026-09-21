# Sector Prediction — Industrials — 2026-09-21

- ETF: **XLI**
- rubric: `00_grounding/sectors/industrials.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **10.19** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **6.972** (ES +1.35%, ER2 +0.08%, HG +0.66%) · index_carry **3.218** (general 12.871) · llm_overlay **0.0** (raw 0.0)

## Channel 1 sector ETF tape

```
ETF XLI vs SPY (yfinance, through 2026-09-18):
  1d: XLI +0.44% | SPY +0.13% | rel +0.31%
  3d: XLI +0.53% | SPY +0.82% | rel -0.28%
  1w: XLI -1.52% | SPY -0.09% | rel -1.43%
  1m: XLI -6.71% | SPY -0.71% | rel -5.99%
```

MEMORY_CONFIRM: Memory index unavailable this run (embedding metadata mismatch; `openclaw memory status --index` or `openclaw memory index --force` would rebuild). Used injected Industrials scoreboard + 08-11..09-18 sector logs only. Rolling dir=0.5 / mag=0.5 (n=10); last 30 dir=0.348 / mag=0.261 (n=23). Last graded 09-18: predicted flat/flat vs XLI +0.438% / SPY −0.119% / rel +0.557% — **dir MISS, mag MISS** (third path after a flat open; 09-17 keep-flat applied, no new signed rule). 09-17 flat/flat vs +0.178% (dir MISS, mag HIT, gap-and-fade). 09-16 flat/flat HIT. 09-15 down/mild HIT. 09-14 down/notable HIT. **Governing today: 09-17 (NONE/D) — unsigned post-paid-FOMC XLI card, 1w/1m lag forbids up, oil offered, operator futures not unanimous ≥+0.5% across ES/NQ/RTY/DJIA, tech-led PM, large overnight ES-vs-cash sleeve → keep close-to-close flat/flat; do not promote the overnight ES gap into up, and do not promote leftover 1w/1m lag into down.** 09-16 — all-zero card + oil down + incomplete four-index confirm → flat/flat; 09-03 is path variance around an unprinted *high-impact* print, not a mandate to emit mild with no directional lean; do not map crude-down onto trucking as S1 relief. 09-18 reflection — keep-flat stands; a late electrical/CAT squeeze vs soft SPY is not a license to lift direction. 09-15 better-than-index PM-gap fade **OFF** (no live oil/duration shock). 09-14 S4=−1 persistent-lag **OFF** (needs live oil/duration plus a *worse*-than-index PM gap; XLI is absent from the injected PM board). 09-11 unanimous ≥+0.5% across all four **OFF** (Finviz ES +0.20% / NQ +0.41% / RTY +0.08% / DJIA +0.11%). 09-09 emit-down **OFF** (oil **down**, futures green, 1d rel **+0.31%**). 09-10 decay — 1m rel −5.99% is a CONDITION; Friday’s +0.31% 1d rel is not acceleration. 09-04 score the laggard **once**. 08-27 — 1w/1m laggard **forbids up**. 08-21 reversal **partial** (NQ only). 08-18 — cap S1 at 0/+1; GEV/grid **not** a cushion; VRT/electrical SPLIT does not drive the ETF. 08-11/08-12 **does not fire** (live oil **down**). 08-13 — Hormuz/tanker is the stale leg; session oil change is down; do **not** treat oil-down as a cyclical green light. Fed-speaker lesson — Goolsbee (Chicago) is on the calendar but is a **2026 non-voting alternate**; October hike odds contested (~55–58%); keep S0 directionally 0; do **not** restack paid 09-16 FOMC/Warsh. Open experiment (`sector_industrials`): shrink confidence on modest |score| — **applied**. DO-INSTEAD 09-17/09-18 loss: leftover 3d/1w/1m RS vs unsigned card → **binding as a flatten, not as a down call**. Checklist: (1) open experiment applied (shrink confidence); (2) no missing factor that would flip 09-17/09-18’s keep-flat without breaking those HITs; (3) oil / paid FOMC / 1m lag each counted once; (4) S0 vs S1 both 0 — FOMC paid, no fresh spine print.

## XLI near-session environment (not an SPX call)

Object is the **Sep 21 cash session for XLI**, not SPX and not a stock pick. Channel 1 numbers used as given. Monday after Friday 09-18 close. **No CPI/NFP/FOMC binary.** CFNAI is secondary and **unscored until print**. Goolsbee remarks are **unprinted / non-voting**.

### 1. Shared macro as it hits Industrials — S0 = 0

Same unsigned shape as 09-16/09-17/09-18: **post-paid-FOMC, oil-offered, tech-led pause**. Not 09-14/09-15’s oil-up/yields-up smash and not 09-11’s unanimous +0.5% de-risking bounce.

- **FOMC is paid, not pending.** 09-16: +25 bp to 3.75–4.00%, 12–0. News Judge #1–3 (Dow’s worst week / Warsh JH hike-odds / gold −3% / IWM vs yields) are **prior-session / already in Friday’s close**. Do **not** restack as a second S0 shock. October hike odds ~55–58% vs hold ~42–45% are **contested**, not a same-morning binary to pre-score.
- **Operator futures are green but not independently confirming a cyclical bid.** Channel 1 Finviz: ES **+0.20%**, NQ **+0.41%**, RTY **+0.08%**, DJIA **+0.11%**. The `ES=F +1.35% / NQ=F +2.12% vs prev close` sleeve is the same sign, larger overnight gap. **Do not re-derive** (09-17). Finviz is the live operator tape. 08-21’s ES/NQ ≥ +0.3% gate is **partial** (NQ only). 09-11’s unanimous ≥ +0.5% *across all four* is **off**. RTY +0.08% is not a cyclical bid. Injected sector PM: **XLK +0.98%** vs XLE −1.29% / XLP −0.65% / XLU −0.63%; **XLI is not on the board** — the bounce is **tech-led**, not industrials-led. An index rebound is not a participation certificate (09-17 / 09-16 XLC cousin).
- **Oil is DOWN, not a fresh squeeze.** Finviz WTI **$104.16 (−1.59%)**, Brent **$107.67 (−1.02%)**; `CL=F −5.94% / BZ=F −5.76%` is the multi-session 1d sleeve (same conflict-handling as 09-15/16/17/18). Channel 2: crude sliding on diplomacy/UN headlines and inventory optics; Hormuz remains a **level** risk, not a same-session kinetic increment. News Judge: **no fresh kinetic oil increment**. 08-11/08-12 **does not fire**. 08-13: tanker/Hormuz is the stale leg; live change is a pullback. Count oil **once, here**. Do **not** treat oil-down as a full cyclical tailwind, and do **not** treat ~$104 Finviz / ~$94 live WTI as a live squeeze.
- **Rates: 1d change is relief, level is still high.** DGS10 **4.94 (−0.07 1d)**, DGS30 **5.29 (−0.06)**, DFII10 **2.61 (−0.07)**. Live Finviz: 10Y note **−0.03%**, 30Y **−0.06%** — tiny, not 09-15’s long-end washout. Real-yield *level* is a condition; the *1d change* is not a second S0 shock. For this cyclical, rates are **secondary vs ISM/CapEx**. 5-day 10Y–SPX corr **−0.592** (moderate). USD flat (Finviz −0.02%; DXY +0.08% 1d).
- **Globals constructive, vol calm.** Asia composite **+1.04%**, Europe **+0.95%**. VIX **14.98** with VIX/VIX3M **0.821** (deep contango). HY OAS **2.70** (tight). Copper **+0.66%**, aluminum **+1.10%** — metals do **not** confirm a growth scare. EPU **342** is a level spike already in Friday’s print; do not restack.
- **Calendar is light.** No industrial production today (G.17 printed 09-18: IP 0.0%, manufacturing −0.3% — **paid**). CFNAI is not a 09-03 high-impact binary. Goolsbee is **non-voting**; do not encode the October path as paid, and do not one-way score hawkish/dovish.

**S0 = 0, regime mixed.** Not −1: oil is confirmed down, futures are green, no live yield spike, no kinetic increment. Not +1: four-index confirmation fails, 08-13 blocks treating oil-down as a cyclical green light, 1w/1m XLI is a laggard, bounce is XLK-led. Oil counted **once here**.

### 2. Spine + secondary — S1 = 0 (capped)

**No fresh same-morning industrials hard print.** August ISM manufacturing already printed **09-01**: PMI **54.6** vs July 55.6; new orders **53.7 (−3.0 pts)**. Still **expansion** — **not** an ISM-contraction HIT, but **slowing**. Next ISM is Oct 1. August durable-goods advance is **Sep 25**, not today. 08-18/08-27: **cap S1 at 0/+1**; +2 is forbidden without same-morning confirmation.

- **Grid / electrical equipment backlog (AI power) — CARRIED, not a same-session raise.** GEV ~$176B RPO / 116 GW gas book remains structural. MAP HEAT **SPLIT Electrical Equipment dir=down conv=high** (VRT −15.4% w1, breadth 0.109). 08-18: GEV/grid is **not** a downside cushion and **not** an ETF raise; VRT does **not** drive XLI. Nested SPLIT beats the parent *for that sleeve only* — do not average it into a sector-wide S1 debit, and do not cancel ISM expansion with VRT.
- **Aerospace & defense — no fresh award.** MAP HEAT A&D dir=down conv=low (GE CPP is 2027 revenue; RTX quiet). Do **not** cancel ISM with one award.
- **Freight / trucking / rail — CARRIED inflection, not this morning.** Cass August shipments **+2.1% y/y** (first gain after 42 months of declines) is already in the tape. 09-16: do **not** map WTI/Brent down onto trucking/air as S1 relief.
- **Construction slowdown — CARRIED.** Housing starts **1.275M (−2.6%)** printed 09-17; August manufacturing IP **−0.3%** printed 09-18. AI/nonres vs residential split still holds; this is not a same-morning print.
- **E&C (PWR/FIX) MAP HEAT up** is nested AI-grid color — do not let two names set the ETF (08-18).
- **AME $5.0B Indicor** is stale M&A in the Finviz digest, already traded.
- **Reshoring / industrial policy — checked, nothing material.**

Net: carried ISM expansion (slowing) vs carried construction/IP softness vs electrical SPLIT vs structural grid. **S1 = 0.**

### 3. Breadth / leadership — S2 = 0

MAP HEAT internals are **weak and split**: building products down (TT/JCI), machinery down (CAT), conglomerates down, electrical SPLIT down; consulting and E&C up. Channel 2: only ~16% of industrials above the 20-day SMA as of Friday — leftover weak participation, not a same-morning breadth event.

1m rel **−5.99%** is a **CONDITION** (09-04 / 09-10), not a same-day forecast. 09-04: score the lag **once**. 09-17/09-18: leftover 3d/1w/1m RS is a **flatten**, not a down call. Friday 1d rel was **+0.31%** — the tape is not confirming down. No live XLI print on the injected PM board, so this is not a same-session “ETF up / names flat” failure.

**S2 = 0.** Weak internals noted in prose; not double-counted into S4.

### 4. Flows / positioning — S3 = 0

Channel 2: XLI saw **~$212M outflow on Sep 15**, ~**−$189M** over 5 days and ~**−$976M** over 1 month — multi-day redemptions, not a same-session volume spike or forced flow. This is the opposite of a crowded long (1m rel −6%). No index-rebalance headline. **S3 = 0** (checked; nothing material same-morning).

### 5. ETF tape confirmation — S4 = 0

Channel 1 (through 09-18, do not alter):

- 1d: XLI **+0.44%** | SPY **+0.13%** | rel **+0.31%**
- 3d: rel **−0.28%**
- 1w: rel **−1.43%**
- 1m: rel **−5.99%**

Confirmation only. 1d is a small **positive** — does **not** confirm down (09-09 off). 1w/1m lag does **not** confirm up (08-27). 09-14 S4=−1 **off** (no live oil/duration stack; no worse-than-index PM gap). 09-10: do not read Friday’s +31 bp rel as acceleration. **S4 = 0.**

**Ex-div path qualifier (not a score):** XLI ex-date is **today**, $0.455 (~27 bp vs ~$170). That is mechanical close-to-close noise, not a factor. Do not encode it in S1/S4.

### Self-audit

- **Lens:** XLI session environment, not SPX, not CAT/VRT/GEV stock-pick.
- **Band:** unsigned card → **flat**; 09-03 mild-mandate **off** (no high-impact binary).
- **Skew:** none. Overnight ES sleeve discarded (09-17).
- **Same-shock double-count:** oil once in S0; paid FOMC once; 1m lag once (as condition, not S2+S4).
- **Single-ticker:** VRT SPLIT / GEV backlog / FIX-PWR / CAT d1 do **not** set the ETF (08-18).
- **Divergence:** leading S0–S3 = 0 vs S4 = 0 — **no leading-vs-tape fight**. Leftover 1w/1m lag vs unsigned card is the 09-17 flatten, not a down override.
- **08-27 forbid-up:** ON. **09-09 emit-down:** OFF.

Leading arithmetic: 0+0+0+0+0 = **0** × 0.9 = **0**. Official close-to-close: **flat / flat**. Confidence shrunk on modest |score| and a non-voting speaker still on the tape.

### Horizons (context, not the session call)

- **HORIZON_3D:** Mixed. Oil-offered cost relief vs still-high real-yield *level* and a contested October hike. No spine print until durable goods (09-25). Expect range, not a trend day cluster, unless a kinetic oil increment returns.
- **HORIZON_1W:** Laggard mean-reversion only if four-index confirmation and industrials-led breadth appear together; 08-27 still caps chasing upside while 1m rel ≤ −5% and XLK leads.
- **HORIZON_2W:** Spine still expansion-not-contraction (ISM 54.6 / new orders 53.7) but slowing; construction/IP softness is the offset. Grid backlog is multi-quarter, not a 2-week ETF driver.
- **HORIZON_1M:** Deep relative lag (−6%) is a condition for a later catch-up **if** ISM/CapEx hold and oil stays offered; it is not a same-day long. Rotation remains **out of** XLI vs XLK on the 1m window.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 0
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.45
REGIME: mixed
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|PARTIAL|0.45|2026-09-21|Channel 1 Finviz ES +0.20% / NQ +0.41% / RTY +0.08% / DJIA +0.11%; XLK PM +0.98%; four-index ≥+0.5% fail
Risk-off tape / flight to safety|MISS|0.70|2026-09-21|VIX 14.98, VIX/VIX3M 0.821 contango, HY OAS 2.70, futures green
Real yields rising|MISS|0.75|2026-09-21|DFII10 2.61 (−0.07 1d); DGS10 4.94 (−0.07)
Real yields falling|PARTIAL|0.55|2026-09-21|1d real-yield dip is relief, not a recession-scare bid; level still high
USD strengthening|MISS|0.60|2026-09-21|Finviz USD −0.02%; DXY +0.08% 1d — flat
USD weakening|MISS|0.60|2026-09-21|no material USD slide
Sector breadth expansion (% names up)|MISS|0.70|2026-09-21|MAP HEAT mostly down; ~16% of industrials above 20-dma as of 09-18
Sector breadth failure (ETF up, names flat)|CARRIED|0.40|2026-09-18|Friday XLI +0.44% with weak internals; not a live same-session ETF-up event (XLI off PM board)
Large-cap leadership inside sector|PARTIAL|0.40|2026-09-21|quality/index drift (MMM/HON) without a captain catalyst
Small/mid leadership inside sector|MISS|0.55|2026-09-21|RTY +0.08%; no small-cap industrial bid
High-beta leadership inside sector|MISS|0.65|2026-09-21|Electrical SPLIT down (VRT); not a high-beta chase
Low-beta leadership inside sector|MISS|0.50|2026-09-21|not a defensive-leadership tape inside XLI
Sector ETF inflow / relative volume spike|MISS|0.55|2026-09-15|https://www.etf.com/sections/daily-etf-flows/daily-etf-flows-iei-sheds-assets
Sector ETF outflow / volume dry-up|CARRIED|0.55|2026-09-15|XLI ~−$212M on 09-15; ~−$189M 5d / ~−$976M 1m — multi-day, not same-session
Crowded long (extreme relative performance + valuation)|MISS|0.80|2026-09-21|1m rel −5.99% is a laggard, not a crowded long
Index rebalance / inclusion tailwind|CHECKED_EMPTY|0.50|2026-09-21|checked, nothing material
Index exclusion / forced selling|CHECKED_EMPTY|0.50|2026-09-21|checked, nothing material
ISM manufacturing / new orders expansion|CARRIED|0.85|2026-09-01|https://www.prnewswire.com/news-releases/manufacturing-pmi-at-54-6-august-2026-ism-manufacturing-pmi-report-302865127.html
Durable goods / CapEx upside|CARRIED|0.50|2026-09-21|July +1.1% already paid; August advance due 09-25 — not today
Grid / electrical equipment backlog (AI power)|CARRIED|0.70|2026-09-16|https://pulse2.com/ge-vernova-backlog-reaches-176-billion-as-power-and-electrification-demand-accelerates/
Aerospace & defense order / budget upside|MISS|0.55|2026-09-21|MAP HEAT A&D down/low; GE CPP is 2027 revenue — no fresh award
Freight / trucking / rail volume recovery|CARRIED|0.60|2026-09-21|https://www.freightwaves.com/news/cass-tl-rates-jump-11-in-august-freight-shipments-turn-positive
Reshoring / industrial policy funding|CHECKED_EMPTY|0.50|2026-09-21|checked, nothing material
ISM contraction|MISS|0.85|2026-09-01|PMI 54.6 / new orders 53.7 — expansion, slowing
CapEx cuts / order cancellation|CHECKED_EMPTY|0.50|2026-09-21|checked, nothing material same-morning
Freight recession|MISS|0.60|2026-09-21|Cass August shipments first y/y gain after 42 months
Construction slowdown|CARRIED|0.70|2026-09-17|https://www.census.gov/construction/nrc/pdf/newresconst_202608.pdf
Sector rotation into industrials|MISS|0.70|2026-09-21|1m XLI vs XLK still lagging; XLK PM +0.98% leads
Sector rotation out of industrials|CARRIED|0.60|2026-09-21|1w rel −1.43% / 1m rel −5.99%; leftover, not a same-morning flow shock
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- web_search: `ISM manufacturing PMI durable goods orders September 2026`
- web_search: `XLI industrials ETF flows premarket September 21 2026`
- web_search: `US economic calendar September 21 2026 Fed speaker industrial production`
- web_search: `GE Vernova grid backlog electrical equipment VRT CAT freight trucking September 2026`
- web_search: `oil prices WTI Brent Hormuz September 21 2026`
- web_search: `Cass Freight Index trucking rail volumes September 2026`
- web_search: `CME FedWatch September October 2026 rate hike odds Warsh`
- web_search: `Austan Goolsbee speech September 21 2026 Fed`
- web_search: `XLI stock premarket September 21 2026 industrials breadth`
- web_search: `US industrial production August 2026 housing starts construction`
- web_search: `2026 FOMC voting members Chicago Fed Goolsbee`
- web_search: `XLI ETF dividend ex-date September 21 2026`
- web_search: `industrials sector rotation XLI vs XLK September 2026`
- web_fetch: `https://www.prnewswire.com/news-releases/manufacturing-pmi-at-54-6-august-2026-ism-manufacturing-pmi-report-302865127.html`
- web_fetch: TipRanks 09-21 futures piece (403 / blocked)
- x_search: `XLI industrials ETF premarket oil yields CAT VRT GE Vernova September 21 2026` (2026-09-18..2026-09-21)
- memory_search: Industrials/XLI (index disabled)

**Key sources (title + URL + timestamp where available) and facts taken**

1. ISM Manufacturing PMI August 2026 (PR Newswire / ISM) — https://www.prnewswire.com/news-releases/manufacturing-pmi-at-54-6-august-2026-ism-manufacturing-pmi-report-302865127.html — fetched 2026-09-21T10:23Z. **Facts:** PMI 54.6 (−1.0 vs July 55.6), 8th expansion month; New Orders 53.7 (−3.0); Production 58.3; Prices 71.1; Backlog 51.8. Next ISM ~Oct 1.
2. Census durable-goods schedule / Trading Economics — https://www.census.gov/manufacturing/m3/prel/pdf/s-i-o.pdf ; https://tradingeconomics.com/united-states/durable-goods-orders. **Facts:** July durables +1.1%; August advance expected **Sep 25, 2026**.
3. Scotiabank / FedRateCalc September 2026 calendar — https://www.scotiabank.com/ca/en/about/economics/economics-publications/post.other-publications.calendar-of-economic-release-dates.calendar-of-economic-release-dates--september-2026-.html. **Facts:** Sep 21 light US calendar; no IP/CPI/NFP/FOMC; CFNAI secondary.
4. Chicago Fed speaking calendar — https://www.chicagofed.org/utilities/about-us/office-of-the-president/office-of-the-president-speaking. **Facts:** Goolsbee OMFIF London discussion 09-21 5:30 a.m. CT; no transcript in hand at snapshot.
5. Federal Reserve FOMC roster — https://federalreserve.gov/monetarypolicy/fomc.htm. **Facts:** Goolsbee is a **2026 alternate / non-voter**; Cleveland votes in 2026.
6. CME FedWatch / Bitcoin.com / MacroOdds — https://news.bitcoin.com/finance/feds-next-rate-move-has-traders-staring-at-a-coin-toss/ ; https://macroodds.com/blog/fed-rate-hike-odds-october-2026. **Facts:** Oct 27–28 hike ~55–58% vs hold ~42–45% as of Sep 18–20; Sep 16 hike already paid.
7. ETF.com daily flows — https://www.etf.com/sections/daily-etf-flows/daily-etf-flows-iei-sheds-assets ; ETFdb XLI — https://etfdb.com/etf/XLI/. **Facts:** XLI ~−$212M on Sep 15; ~−$189M 5-day / ~−$976M 1-month.
8. GE Vernova backlog — https://pulse2.com/ge-vernova-backlog-reaches-176-billion-as-power-and-electrification-demand-accelerates/ ; 24/7 Wall St 2026-09-16 — https://247wallst.com/investing/2026/09/16/ge-vernova-climbs-5-as-ceo-sees-backlog-hitting-200b-early-eaton-and-quanta-services-edge-higher/. **Facts:** ~$176B backlog; 116 GW gas book; structural, not a same-morning print.
9. Cass / FreightWaves August 2026 — https://www.freightwaves.com/news/cass-tl-rates-jump-11-in-august-freight-shipments-turn-positive ; https://www.cassinfo.com/freight-audit-payment/cass-transportation-indexes/august-2026. **Facts:** Shipments 1.038, +2.1% y/y first gain after 42 months; TL linehaul +11.3% y/y.
10. Oil tape (Channel 2, not used to overwrite Channel 1) — Tribune/Investing.com/Trading Economics. **Facts:** live WTI ~$93–94 / Brent ~$101–102 sliding on diplomacy headlines; Hormuz still a level risk. Channel 1 Finviz WTI $104.16 (−1.59%) / Brent $107.67 (−1.02%) remains the trusted panel.
11. Industrial production / housing — Haver / Census — https://www.haver.com/articles/u-s-industrial-production-flat-in-august-manufacturing-ip-down ; https://www.census.gov/construction/nrc/pdf/newresconst_202608.pdf. **Facts:** Aug IP 0.0%, manufacturing −0.3% (printed 09-18); starts 1.275M −2.6% (printed 09-17).
12. Tradesmith / DividendInvestor — https://tradesmith.com/stockdata/XLI:NYSE ; https://www.dividendinvestor.com/dividend-news/20260918/state-street-industrial-select-sector-spdr-etf-select-sector-spdr-trust-nyse-xli-declared-a-dividend-of-$0.4550-per-share/. **Facts:** XLI ex-div **09-21** $0.455; unofficial PM quote ~$170.25 +0.29% (not in Channel 1 PM board).
13. BreadthMarket — https://breadthmarket.com/. **Facts:** 13/83 industrials (~15.7%) above 20-dma as of 09-18.
14. PortfoliosLab XLI vs XLK — https://portfolioslab.com/tools/stock-comparison/XLI/XLK. **Facts:** 1m XLI ~−6.7% vs XLK ~+3.3% — rotation still **out of** industrials vs tech.
15. X/Twitter search 09-18..09-21 — CAT/XLI 200-dma test chatter; no same-morning XLI catalyst.

**Channel 2 coverage:** (1) shared macro as it hits XLI — covered; (2) spine ISM/CapEx — covered, no same-morning print; (3) secondary grid/A&D/freight/reshoring/construction/rotation — covered; (4) breadth — covered; (5) flows — covered (multi-day outflows, no same-session spike); (6) earnings/guidance/policy — Goolsbee non-voting unprinted; CFNAI secondary unprinted; no XLI-holding AHR.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': 0.0, 'divergence_flagged': False, 'total_score': 10.19, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.55, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 1.162, 'score': 6.972, 'legs': [{'leg': 'ES', 'pct': 1.35, 'w': 0.8}, {'leg': 'ER2', 'pct': 0.08, 'w': 0.2}, {'leg': 'HG', 'pct': 0.66, 'w': 0.1}]}, 'overlay_score': 0.0, 'overlay_raw': 0.0, 'index_carry': 3.218, 'general_total': 12.871, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 0.5, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 0.5}, 'llm_confidence': 0.45, 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': -1.65, 'w1': -2.74}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
