# Sector Prediction — Basic Materials — 2026-09-02

- ETF: **XLB**
- rubric: `00_grounding/sectors/basic_materials.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **0.0** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLB vs SPY (yfinance, through 2026-09-01):
  1d: XLB -1.18% | SPY -0.69% | rel -0.49%
  3d: XLB -2.98% | SPY -0.56% | rel -2.42%
  1w: XLB -2.82% | SPY -0.22% | rel -2.60%
  1m: XLB +3.25% | SPY +1.97% | rel +1.28%
```

MEMORY_CONFIRM: Sector Basic Materials / XLB only. Memory index unavailable this run (embedding metadata missing) — used injected scoreboard/lessons only. Rolling last-10 dir=0.5 mag=0.6 (n=10). Last graded: 08-28 down/flat vs XLB −0.09% / rel +0.13% (dir MISS, mag HIT); 08-27 up/flat vs −0.82% / rel −1.48% (dir+mag MISS); 08-31 and 09-01 down/mild still ungraded. Active XLB rules checked: temper-severe does **not** fire (not building severe; 1d rel −0.49% <0.5% but live oil is only +0.2–0.4%, not a fresh Hormuz squeeze); 8/17 China-miss + flat-futures severe ban **off** (no same-morning China print; NBS is T-2 8/31; ES is flat not a China-miss overlay); 8/18 metals-as-floor **partial/off as a down mandate** (Brent still ~$95 / WTI ~$90.5, but News Judge Hormuz is **mixed** reopen-terms vs stalemate, oil is **not** independently confirming, ES **+0.06%** — do **not** use copper/gold as a floor **and** do **not** treat leftover oil>$90 as a fresh liquidation); 8/14 gold-offset **OFF** (live gold −0.92%, silver −1.73%, 3-day slide ~6%, not green); 8/21 keep-severe **OFF**; 8/25 composition/transmission **OFF as NQ>>ES up-ban** (NQ is *weaker* than ES, not a tech-led green light) but the chemicals-heavy transmission discount still forbids passing a miner fade into an XLB down call without LIN/SHW confirmation; 8/27 S4-cap **ON** (1d rel −0.49% <0.5% → S4 cannot be ± confirmation); **8/28 leftover-S2/S4 down-mandate is the binding rule** — S0 and S1 net-zero, do **not** copy Tuesday’s −1.18% / −0.49% rel into S4, do **not** treat “don’t emit up” as a license to emit down. No open experiment for `sector_basic_materials`. DO-INSTEAD: prefer flat/mild when score sign conflicts with tape/breadth; shrink confidence on modest |score|. ISM Manufacturing is T-1 (09-01, 54.6). ADP **+38k** is today’s 8:15 print (already out). PCE/Warsh are T+ sessions, not today’s 8:30.

## Analysis — XLB, session of 2026-09-02

This is a **Wednesday leftover after Tuesday’s risk-off / metals fade**, not a copper-squeeze day and not a fresh Hormuz-oil liquidation. Channel 1 tape through 09-01 is already soft (1d rel **−0.49%**, 3d **−2.42%**, 1w **−2.60%**, 1m still **+1.28%**) but that tape is **Tuesday’s close**. Per 8/28, it does not forecast today by itself.

What *is* live this morning: ES **+0.06%**, NQ **−0.12%** (Finviz cash futures SPX −0.23% / NQ −0.54%), Asia composite **−1.77%** (Kospi −3.99% / Nikkei −2.85% / Shanghai −0.97%), Europe **~−0.12%**, copper **$6.5405 (−0.91%)**, gold **$4,355.6 (−0.92%)**, silver **−1.73%**, WTI **$90.47 (+0.22%)**, Brent **$94.97 (+0.38%)**, USD **+0.15%**.

### 1. Shared macro as it hits materials (S0)

Risk-on does **not** map here, and neither does a fresh 09-01-style kinetic overlay.

- US futures are **flat**, not independently weak: ES **+0.06%** vs NQ **−0.12%**. NQ is softer than ES (duration/growth), which is **not** the 8/25 “tech-only green light” and **not** the 09-01 NQ ≤ −0.5% confirmation. 08-21 reversal checklist (ES/NQ ≥ +0.3%) is **off**.
- Asia **−1.77%** is a real cyclical headwind, but Kospi −3.99% is the outlier; Shanghai −0.97% / Hang Seng −0.07% is **not** a same-morning China hard-data miss. Do not import a chip-led Asia crash as materials regime confirmation.
- **Warsh / September hike coin-flip** is News Judge #1 and still the rates spine, but it is **T+** (Jackson Hole 8/28). Gold’s 3-day slide is the transmission, not a same-morning speech. Count it once; do not ding S0 and S1 as two hawkish objects.
- **Hormuz** is News Judge #2 with **mixed** polarity: stalemate vs Iran “terms to reopen.” The 09-01 kinetic increment **does not fire**. Live oil is only **+0.22% / +0.38%** after Tuesday’s CL **−1.16%** 1d column. Oil is still **>$90** (processor cost, not a squeeze). 8/18 co-move floor is a **ban on using metals as a hedge**, not a mandate to keep S0 at −1 when oil is not confirming and ES is flat.
- **USD** +0.15% / DXY +0.07% 1d is **not** a USD spike vs the complex (1m DXY still −0.22%).
- **Real yields:** FRED DFII10 **2.44 (+2 bp 1d as of 8/31)** is the prior-close table. Live Finviz 10Y note **−0.06%** / 30Y **−0.23%** is a small same-morning bond bid — do **not** treat the 8/31 1d column as today’s open curve (08-25 live-rate lesson).
- **Calendar:** ADP **+38k** (8:15 ET) vs ~47k, manufacturing **−17k** — already printed, modest miss, two-sided (growth vs hike-odds). Factory orders **10:00 ET** still pending — do not pre-score. ISM Manufacturing **54.6** (vs 55.2 cons / 55.6 prior) printed **yesterday**. VIX **16.09** with VIX/VIX3M **1.053 backwardation** is caution, not panic. HY OAS **2.63** still tight.

**S0 = 0.** Not +1 (no materials-led risk-on; NQ is not a cyclical bid). Not −1 (ES flat, Hormuz mixed, oil not independently confirming, Warsh already paid, do not ding merely because oil is still >$90). Asia red and ADP manufacturing −17k are acknowledged once here.

### 2. Spine + secondary (S1)

**Industrial metals — fade, not surge, not collapse.** Live COMEX **$6.54 (−0.91%)**; 09-01 settle **$6.5065 (−1.32%)**. LME cash still elevated (~**$14,396/t** vs 3m ~**$14,215**, backwardation ~**$180/t**) with warehouse stocks **~233.5 kt** after the mid-August rebuild from ~205 kt to ~240 kt. Tightness without a same-morning squeeze. Spine “surge” **off**. Spine “collapse” **not** a clean HIT.

**Inventory draw — off as a HIT.** Modest draw from the rebuild peak is not a restoration of the ultra-tight regime.

**China demand — still the industrial offset, not a rebound.** August NBS mfg **49.8** (still <50), construction **46.9**. Property FAI still draining traditional copper; grid/EV/AI is the structural floor. **Do not let gold cancel this — and today gold is not even green.** This is T-2, not an 8/17 same-morning miss.

**Monetary metals — fade, not surge.** Live gold **−0.92%** near ~$4,330 after a ~**6% three-day slide**; silver **−1.73%**. News Judge #3 is explicit: this is hawkish real-rate transmission, not a geo safe-haven bid. 8/14 does **not** pay. Per 8/28, a live gold fade is an **S1 sleeve**, not a reason to keep stale chemicals scores or to net S1 to −1 without LIN/SHW confirmation. NEM (~8% of XLB) cannot drive the ETF.

**Supply disruption / tariffs — stale.** DRC concentrate ban and Section 232 copper remain on the books; not a same-open catalyst. APD’s Q3 beat/charge is **already traded**.

**Oil-up is a cost headwind for processors, not an XLB squeeze.** Count Hormuz/oil once in S0 as mixed/non-confirming; do not also credit copper as a positive floor (8/18).

**S1 = 0.** Residual LME backwardation vs China/property still <50 vs gold/copper fade nets to zero. Gold is an 8/14 sleeve (off when red), not a −1 wash. Cap well below ±2. Do not let gold strength (absent) cancel China, and do not let gold weakness cancel the chemicals book.

### 3. Breadth (S2)

8/28: do **not** copy Tuesday’s metals/chemicals lag into S2. Same-morning chemicals confirmation is required for S2 = −1. Premarket LIN/SHW are **mixed-to-flat**; miner snapshots are split (NEM indicated firmer in one print while gold is still red; FCX not leading). That is composition (chemicals-heavy book buffering a miner fade), not a fresh % names-down thrust.

**S2 = 0.**

### 4. Flows / positioning (S3)

XLB AUM ~$8.7–8.8B. Tracked 1m net flows about **−$171M**, 5d about **−$69M** — mild outflow, not a volume spike and not a washout. One ~+$90M print around 09-01 is not a persistent bid. Trailing unit outflows are not a 1-day lid.

**S3 = 0.**

### 5. Tape (S4, confirmation only)

1d rel **−0.49%** is Tuesday’s close, **<0.5%**, and already paid. 8/27: S4 cannot be a ± confirmation from a sub-0.5% print. 8/28: S4 confirms only the session being predicted. Premarket XLB snapshots around **$52.05–$52.28** vs 09-01 close **$52.07** are a bounce path, not a confirmed breakdown.

**S4 = 0.** Leading sum (0+0+0+0 = 0) and tape (0) **agree**. No leading-vs-tape fight. Residual after 8/25/8/27 “do not emit up” is **flat**, not down.

### Self-audit

- **Lens:** XLB environment, not SPX, not NEM/FCX single-name. Amgen Ph3 and ADI upgrades are not this book.
- **Band:** After Tuesday −1.18%, flat ES, mixed Hormuz, gold fade as a sleeve only → **mild/flat**, not notable/severe. Mag accuracy 0.6 argues for shrinking confidence, not upgrading the band.
- **Skew:** Gold fade ≠ industrial-metals collapse. China still <50 is stated separately; gold does not cancel it.
- **Same-shock:** Warsh/hike odds counted once (S0 mixed). Gold slide is the transmission sleeve, not a second −1. Oil>$90 counted once as non-confirming, not restacked as a processor smash.
- **Single-ticker:** APD already traded; do not let NEM/AU drive the ETF call.
- **8/28 vs 09-01:** Tuesday’s down/mild was live kinetic + NQ ≤ −0.5% + oil +2.6%. Those legs are **off** this morning. Copying S2/S4 would repeat the 08-28 miss.
- **Pipeline reconcile:** Σ(S0..S4) = 0 × 0.9 = 0. Narrative and components both **flat**. Do not let a leftover negative tape rewrite the block to down.

**REGIME = mixed.** Not risk_off (ES flat, oil not spiking, Hormuz mixed). Not risk_on (Asia red, gold/copper red, NQ softer, VIX backwardation).

---

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 0
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.52
REGIME: mixed
HORIZON_3D: down:mild:0.55
HORIZON_1W: down:mild:0.52
HORIZON_2W: flat:mild:0.48
HORIZON_1M: up:mild:0.50
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|MISS|0.72|2026-09-02|https://finviz.com/futures.ashx
Risk-off tape / flight to safety|WATCH|0.58|2026-09-02|https://finviz.com/futures.ashx
Real yields rising|WATCH|0.55|2026-08-31|https://fred.stlouisfed.org/series/DFII10
Real yields falling|MISS|0.50|2026-09-02|https://finviz.com/futures.ashx
USD strengthening|WATCH|0.52|2026-09-02|https://finviz.com/futures.ashx
USD weakening|MISS|0.55|2026-09-02|https://finviz.com/futures.ashx
Sector breadth expansion (% names up)|MISS|0.60|2026-09-02|https://tradesmith.com/stockdata/XLB:NYSE
Sector breadth failure (ETF up, names flat)|MISS|0.55|2026-09-02|https://tradesmith.com/stockdata/XLB:NYSE
Large-cap leadership inside sector|WATCH|0.50|2026-09-02|https://tradesmith.com/stockdata/XLB:NYSE/holdings
Small/mid leadership inside sector|MISS|0.45|2026-09-02|https://tradesmith.com/stockdata/XLB:NYSE
High-beta leadership inside sector|MISS|0.55|2026-09-02|https://finviz.com/futures.ashx
Low-beta leadership inside sector|WATCH|0.48|2026-09-02|https://tradesmith.com/stockdata/XLB:NYSE
Sector ETF inflow / relative volume spike|MISS|0.58|2026-09-02|https://etfdb.com/etf/XLB/
Sector ETF outflow / volume dry-up|WATCH|0.55|2026-09-02|https://etfdb.com/etf/XLB/
Crowded long (extreme relative performance + valuation)|MISS|0.60|2026-09-02|
Index rebalance / inclusion tailwind|MISS|0.40|2026-09-02|
Index exclusion / forced selling|MISS|0.40|2026-09-02|
Industrial metal price surge (copper/aluminum/iron ore)|MISS|0.78|2026-09-02|https://www.morningstar.com/news/dow-jones/202609016174/comex-copper-settles-132-lower-at-65065-data-talk
Gold/silver price surge (monetary metals)|MISS|0.82|2026-09-02|https://www.businesstoday.com.my/2026/09/02/gold-holds-near-us4330-after-nearly-6-three-day-slide/
China PMI / property demand rebound|MISS|0.80|2026-08-31|https://www.stats.gov.cn/sj/zxfbhjd/202608/t20260831_1965154.html
Inventory draw (LME/exchange stocks down)|WATCH|0.50|2026-09-01|https://thevaultreport.com/lme/copper
Supply disruption (mine/export ban)|MISS|0.45|2026-09-02|
Critical-minerals policy / domestic tariff support|WATCH|0.40|2026-09-02|
Industrial metal price collapse|MISS|0.70|2026-09-02|https://www.morningstar.com/news/dow-jones/202609016174/comex-copper-settles-132-lower-at-65065-data-talk
China demand shock / property stress|HIT|0.72|2026-08-31|https://www.stats.gov.cn/sj/zxfbhjd/202608/t20260831_1965154.html
USD spike vs commodity complex|MISS|0.65|2026-09-02|https://finviz.com/futures.ashx
Supply glut / new capacity online|MISS|0.40|2026-09-02|
Margin compression / cost inflation without pricing power|WATCH|0.52|2026-09-02|https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/august/
Sector rotation into materials|MISS|0.70|2026-09-01|
Sector rotation out of materials|WATCH|0.62|2026-09-01|
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- memory_search: Basic Materials XLB sector prediction lessons copper gold China (index unavailable)
- web_search: copper LME price inventory stocks September 2 2026
- web_search: gold silver price today September 2026 Warsh Fed
- web_search: China PMI property copper demand September 2026
- web_search: XLB materials stocks LIN FCX NEM SHW premarket September 2 2026
- web_search: US economic calendar September 2 2026 8:30 ISM ADP
- web_search: ISM Manufacturing PMI August 2026 September 1 result
- web_search: Hormuz Iran oil WTI Brent September 2 2026 reopen terms
- web_search: XLB ETF flows holdings LIN NEM FCX September 2026
- web_search: copper price COMEX today September 2 2026
- web_search: ADP employment August 2026 38000
- web_search: XLB premarket September 2 2026 Linde Sherwin Williams Freeport Newmont
- x_search: XLB copper gold Linde Freeport Newmont materials stocks today September 2 2026 (2026-09-01 to 2026-09-02)
- web_fetch: Business Today gold article (403); Reuters oil article (401)

**Key sources and facts used**
- Channel 1 (injected, unaltered): ES +0.06% / NQ −0.12%; Finviz WTI $90.47 (+0.22%), Brent $94.97 (+0.38%), gold $4,355.6 (−0.92%), silver −1.73%, copper $6.5405 (−0.91%), USD +0.15%; Asia composite −1.77%; XLB vs SPY 1d −1.18% / rel −0.49%, 3d rel −2.42%, 1w rel −2.60%, 1m rel +1.28%; VIX 16.09, VIX/VIX3M 1.053; DFII10 2.44 as of 2026-08-31.
- News Judge 2026-09-02: Warsh hike-odds #1; Hormuz mixed (stalemate vs reopen terms) #2; gold >3% slide #3; 09-01 kinetic increment does not fire.
- Westmetall / Vault Report / search compile: LME Cu cash ~$14,395.50, 3m ~$14,215, backwardation ~$180/t; stocks ~233.5 kt as of 2026-09-01 (https://www.westmetall.com/ ; https://thevaultreport.com/lme/copper).
- Morningstar/Dow Jones: COMEX copper 2026-09-01 settle $6.5065, −1.32% (https://www.morningstar.com/news/dow-jones/202609016174/comex-copper-settles-132-lower-at-65065-data-talk).
- Business Today / Moneycontrol: gold near $4,330 after ~6% three-day slide on Warsh/hike odds (https://www.businesstoday.com.my/2026/09/02/gold-holds-near-us4330-after-nearly-6-three-day-slide/).
- NBS / stats.gov.cn: August mfg PMI 49.8, construction 46.9, released 2026-08-31 (https://www.stats.gov.cn/sj/zxfbhjd/202608/t20260831_1965154.html).
- ISM: August Manufacturing PMI 54.6 vs 55.6 July / 55.2 cons, released 2026-09-01 (https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/august/).
- ADP: private payrolls +38,000 in August, manufacturing −17,000, released 2026-09-02 8:15 ET (https://mediacenter.adp.com/2026-09-02-ADP-National-Employment-Report-Private-Sector-Employment-Increased-by-38,000-Jobs-in-August).
- Scotiabank calendar: 2026-09-02 ADP 8:15; factory orders 10:00; Beige Book 14:00; ISM Services 2026-09-03 (https://www.scotiabank.com/ca/en/about/economics/economics-publications/post.other-publications.calendar-of-economic-release-dates.calendar-of-economic-release-dates--september-2026-.html).
- ETFDB / Tradesmith: XLB AUM ~$8.7–8.8B; LIN ~13%, NEM ~7.9%, FCX ~6.3%, SHW ~4.9%; 1m flows ~−$171M, 5d ~−$69M (https://etfdb.com/etf/XLB/ ; https://tradesmith.com/stockdata/XLB:NYSE).
- Premarket snapshots (Tradesmith / StockMarketWatch): XLB ~$52.05–$52.28 vs 09-01 close $52.07; LIN/SHW mixed-to-flat (https://tradesmith.com/stockdata/XLB:NYSE).
- Hormuz: News Judge mixed reopen-terms; search also surfaced conflicting “fresh strikes / oil up ~1%” copy (Reuters fetch blocked). Scored as **mixed / not independently confirming** because Channel 1 live oil is only +0.2–0.4% and ES is flat — did not promote to a 09-01 kinetic HIT.

Checked, nothing material beyond the above for: index rebalance/exclusion, fresh mine/export bans, critical-minerals tariff increment, supply glut, or confirmed XLB inflow spike.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': 0.0, 'divergence_flagged': False, 'total_score': 0.0, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.52, 'regime': 'mixed'}
```
