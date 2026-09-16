# Sector Prediction — Basic Materials — 2026-09-16

- ETF: **XLB**
- rubric: `00_grounding/sectors/basic_materials.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **9.667** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **5.868** (ES +1.14%, HG +0.66%, GC +0.90%, DX -0.02%) · index_carry **1.324** (general 5.297) · llm_overlay **2.475** (raw 2.475)

## Channel 1 sector ETF tape

```
ETF XLB vs SPY (yfinance, through 2026-09-15):
  1d: XLB +0.48% | SPY -0.46% | rel +0.93%
  3d: XLB -0.06% | SPY -0.06% | rel -0.00%
  1w: XLB -2.33% | SPY -1.12% | rel -1.21%
  1m: XLB -3.44% | SPY -2.44% | rel -1.00%
```

MEMORY_CONFIRM: Sector Basic Materials / XLB only. Memory index unavailable this run (embedding metadata missing) — used injected scoreboard/lessons only. Rolling last-10 dir=0.4 mag=0.5 (n=10); last graded 2026-09-15 down/mild vs XLB +0.475% / SPY −0.46% / rel +0.93% (dir MISS, mag HIT). No open experiment for `sector_basic_materials`. Active XLB rules checked: **09-15 nested-bid / don’t-flip-down is ON as a process rule, OFF as a copper-HEAT trigger** — score a live bid in exactly one of S1 or S4; PM:XLB is **flat 0.00%** (not green) and MAP HEAT Copper is **down**, so the 09-15 copper-child-book misfire does **not** replay; the live bid today is oil-offered chemicals relief + green gold/silver. **09-11 pre-binary-tape** — FOMC 14:00 ET is the unknowable binary; do not zero the knowable tape; Finviz four-index is **not** all ≥ +0.5% (ES +0.20 / NQ +0.41 / RTY +0.08 / DJIA +0.11) so the hard four-index up-gate is **OFF**, but ES=F **+1.14%** / NQ=F **+1.50%** vs prior close are independently green — modest S0 lean, not a materials +1. **09-10 gap-at-open OFF** (XLB PM **$50.73 / 0.00%**). **09-09 S1=−2 / S2=−1 OFF** — 8/18 co-move is **not** firing (oil offered, metals green); offset is not zero. **8/18 metals-as-floor OFF**. **8/14 gold-offset ON** (GC +0.90% / +1.36% 1d, SI +1.96%). **8/25 up-ban OFF** (1d rel **+0.93% > 0.5%**; NQ only modestly > ES). **8/27 S4-cap OFF** (1d rel confirmation-eligible) but **09-03 / 09-15 double-count** → do not also pay S4. **8/28 leftover-down OFF**. **09-04 T-1-lag** — China 09-15 hard data is a *level* already in Asia (Shanghai +0.71%, Hang Seng +0.19%), not a US-open re-acceleration. DO-INSTEAD: prefer flat/mild if score sign fights tape; keep direction and shrink confidence on modest |score|.

## Analysis — XLB, session of 2026-09-16 (FOMC)

This is an **FOMC-day pre-binary session** after yesterday’s chemicals/relative bounce, **not** a copper-squeeze day and **not** a fresh Hormuz liquidation. Channel 1 tape through 09-15: 1d XLB **+0.48%** / SPY **−0.46%** / rel **+0.93%**, 3d rel **0.00%**, 1w rel **−1.21%**, 1m rel **−1.00%**. XLB premarket is **unchanged at $50.73**. News Judge: no kinetic/oil increment; hike-priced tape is T-1; gold “surge on cuts” is the stale/conflicting line.

### 1. Shared macro as it hits materials (S0)

Knowable pre-binary tape (do not pre-score the 14:00 ET statement / SEP / Warsh presser):

- **Futures mixed-green, not a four-index thrust.** Finviz: SPX **+0.20%**, NQ **+0.41%**, RTY **+0.08%**, DJIA **+0.11%**. ES=F **+1.14%** / NQ=F **+1.50%** vs prior close are the overnight bounce from Monday–Tuesday’s selloff — score the sign, do not treat NQ>ES as an XLB green light (8/25).
- **Oil offered, 8/18 OFF.** WTI **−1.59%** to $104.16, Brent **−1.02%** to $107.67, CL=F **−2.36%** 1d. API crude **+7.1 mb** vs ~−1.6 mb expected. Hormuz remains a *level* (Brent still >$100) but the live increment is inventory-led easing, not a fresh squeeze. Majority-sleeve chemicals get **feedstock relief**, not a cost shock.
- **USD / real yields:** DXY **−0.02%** / 1d **+0.02%** — not a spike. DFII10 **2.60 (1d 0.00)**; live 10Y note **−0.03%**, 30Y **−0.06%**. Hawkish path is **priced (~92–95% 25 bp hike)** — a *level* already in Monday–Tuesday’s close, not a same-morning real-yield smash.
- **Asia +0.65% / Europe +0.45%.** VIX **16.98 (−0.22)** with VIX/VIX3M **0.877 contango**. HY OAS **2.71** still tight. 5-day 10Y–SPX corr **−0.155** (no longer the −0.97 stress tape).
- **China 09-15 print is T-1 / absorbed.** IP **+5.2%** beat vs property FAI **−19.9%** / home prices still down. Hang Seng/Shanghai are **green**; 09-15 rule: do not re-open that as a US-open shock.

**S0 = +0.5.** Modest lean with the green ES/NQ pre-binary tape and oil-offered chemicals overlay. Not +1: FOMC binary pending, Finviz four-index fails ≥ +0.5%, NQ>ES is tech-tilted, hike is already priced. Not −1: that was yesterday’s miss — using the hawkish *level* to force down against a non-red materials tape.

### 2. Spine + secondary (S1)

**Industrial metals — bounce, not surge, not collapse.** COMEX copper **$6.489 (+0.66%)**; aluminum **+1.10%**; iron ore **−0.14%**; steel HRC **−0.16%**. LME cash ~**$14,043–14,065/t** after a three-week low near **$13,926** on 09-15, still **well off** the ~**$14,858** record. Spine “surge” **OFF**. Spine “collapse” **OFF** this morning (that was 09-10/09-14).

**Inventory draw — inverted (glut HIT).** LME copper stocks **~249.2–254.2 kt**, **+~20% in 30 days** (242.9 kt → 249.2 kt, further daily builds). Cash-3M squeeze has **unwound toward flat/small contango**. This is the inverse of the spine’s inventory-draw HIT.

**China demand — still contraction, not a rebound; do not let gold cancel it.** NBS mfg PMI **49.8**, property FAI **−19.9%**, new-home prices **−0.17% m/m**, copper imports a **six-year low**. T-1, Asia green, so it is a **carried industrial offset**, not a fresh miss.

**Monetary metals — 8/14 ON.** Gold **+0.90%** (GC=F **+1.36%**), silver **+1.96%**, platinum **+0.61%**, palladium **+1.64%**. Reuters: gold firmer on softer dollar/oil into the Fed — continuation, not a squeeze. NEM ~8% of XLB is a **sleeve**, not the book.

**Chemicals majority sleeve — oil-offered cost relief (the composition math).** LIN ~13%, SHW ~4.8%, ECL ~4.8%, APD ~4.7%. WTI/Brent offered is a **direct, knowable** majority-sleeve positive — the inverse of 09-08/09-09. MAP HEAT Chemicals **dir=up / medium** (HUN/REX) is a **child-book**; do **not** lift the parent off HUN/REX. LIN PM is **flat-to-slightly down**. Count oil-relief once as XLB-weight, not as a nested-captain thrust.

**Copper nested HEAT — down.** MAP HEAT Copper **dir=down / medium**, FCX:neg on stalled **Section 232 refined-copper** (decision still ~09-28). FCX PM **+1.04%** vs HEAT down = mixed minority sleeve (~6%). 09-08 composition: minority cannot dominate majority. Today the majority is the **positive**.

**S1 = +1.** Majority-sleeve oil relief + 8/14 gold/silver, net of LME glut + China property + copper-HEAT down. Capped at +1: no metal surge, gold does **not** cancel China, HUN/REX is not LIN/SHW. Not −2: 09-09 requires zero offset and 8/18 co-move — both absent. Not 0: 09-15 forbids zero-counting a live majority-sleeve bid.

### 3. Breadth (S2)

HEAT is a **split**, not a thrust: Chemicals **up** vs Copper/Gold/Aluminum/Ag **down**; Building Materials **flat**. 09-15 cash leadership was LIN/SHW/ECL (chemicals/paint), not a miner melt-up. Same-morning: XLB **flat**, LIN **flat**, FCX **+1%**. No % names-up expansion and no 8/18 uniform washout.

**S2 = 0.** Prior-day 1d rel is reserved for (and then zeroed in) S4 — do not also pay S2 (single-print rule).

### 4. Flows / positioning (S3)

XLB ~**−$169M** 1m net outflow; 1y still positive. No volume spike, no washout, no crowded-long extreme after the 1w/1m lag.

**S3 = 0.**

### 5. Tape (S4, confirmation only)

1d rel **+0.93%** is confirmation-eligible, but it is **yesterday’s chemicals-led bounce** — the same object already in S1. 1w **−1.21%** / 1m **−1.00%** still lag. 09-03: a sleeve/chemicals bounce inside a negative 1w is not an inflection. 09-15: nested/live bid in **one** channel.

**S4 = 0.**

### Horizons (relative XLB vs SPY, not a second 1d call)

- **3D:** flat/choppy — FOMC 1d binary plus leftover metals mean-reversion; 3d rel already **0.00%**.
- **1W:** down/lag — 1w rel **−1.21%** with China/inventory still HIT; expect lag even if the cash session is green.
- **2W:** mixed — hawkish SEP path vs oil-offered chemicals; unresolved Section 232 (~09-28).
- **1M:** down/lag — 1m rel **−1.00%**, property FAI **−19.9%**, LME rebuild; needs a China/property turn that is not on the tape.

### Reconciliation / self-audit

Leading sum **S0+S1+S2+S3 = +1.5** vs **S4 = 0** — same sign, **no divergence**. Trust factors over tape; tape is confirmation-only and was zeroed to avoid double-count.

- **Lens:** XLB book (chemicals majority + NEM sleeve), not SPX, not FCX, not HUN/REX.
- **Band:** FOMC binary + no ≥1% gap → **mild cap**; rolling mag discipline may shrink further. 09-10 does not force notable.
- **Skew:** majority chemicals **+** (oil offered) vs minority copper HEAT **−**; gold is an 8/14 sleeve, not a China cancel.
- **Same-shock:** oil counted in S1 (chemicals transmission), not again in S0; hawkish Fed is S0 *level*, not restacked as a metals collapse; China T-1 not restacked as a US-open shock.
- **Single-ticker:** APD beat is already traded; BHP/FCX tariff is minority.

Relative lean (09-11): **absolute modest up/flat-mild, lagging 1w/1m**. Do not emit down against oil-offered + green metals + non-red PM after the 09-15 sign miss.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0.5
S1_SECTOR_FACTORS: 1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.42
REGIME: mixed
HORIZON_3D: flat
HORIZON_1W: down
HORIZON_2W: mixed
HORIZON_1M: down
DIVERGENCE: false
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|PARTIAL|0.55|2026-09-16|https://www.tipranks.com/news/stock-futures-rise-ahead-of-feds-interest-rate-decision
Risk-off tape / flight to safety|OFF|0.70|2026-09-16|https://www.tipranks.com/news/stock-futures-rise-ahead-of-feds-interest-rate-decision
Real yields rising|OFF|0.62|2026-09-16|https://www.bloomberg.com/news/newsletters/2026-09-16/bond-traders-are-convinced-the-fed-will-hike-interest-rates
Real yields falling|OFF|0.55|2026-09-16|https://www.bloomberg.com/news/newsletters/2026-09-16/bond-traders-are-convinced-the-fed-will-hike-interest-rates
USD strengthening|OFF|0.70|2026-09-16|https://www.marketscreener.com/news/gold-rises-on-easing-dollar-oil-as-fed-rate-verdict-looms-ce785bd2da8df120
USD weakening|PARTIAL|0.50|2026-09-16|https://www.marketscreener.com/news/gold-rises-on-easing-dollar-oil-as-fed-rate-verdict-looms-ce785bd2da8df120
Sector breadth expansion (% names up)|OFF|0.58|2026-09-16|https://breadthmarket.com/
Sector breadth failure (ETF up, names flat)|OFF|0.55|2026-09-16|https://www.benzinga.com/etfs/sector-etfs/26/09/61789034/leading-and-lagging-sectors-september-15-2026
Large-cap leadership inside sector|PARTIAL|0.52|2026-09-15|https://finance.yahoo.com/quotes/LIN,SHW,NEM,CRH,ECL,APD,FCX,CTVA,VMC,MLM/
Small/mid leadership inside sector|OFF|0.45|2026-09-16|https://insiderstreet.ai/etfs/XLB
High-beta leadership inside sector|OFF|0.50|2026-09-16|https://insiderstreet.ai/etfs/XLB
Low-beta leadership inside sector|PARTIAL|0.50|2026-09-15|https://finance.yahoo.com/quotes/LIN,SHW,NEM,CRH,ECL,APD,FCX,CTVA,VMC,MLM/
Sector ETF inflow / relative volume spike|OFF|0.60|2026-09-16|https://etfdb.com/etf/XLB
Sector ETF outflow / volume dry-up|PARTIAL|0.55|2026-09-16|https://etfdb.com/etf/XLB
Crowded long (extreme relative performance + valuation)|OFF|0.65|2026-09-16|https://insiderstreet.ai/etfs/XLB
Index rebalance / inclusion tailwind|OFF|0.40|2026-09-16|checked, nothing material
Index exclusion / forced selling|OFF|0.40|2026-09-16|checked, nothing material
Industrial metal price surge (copper/aluminum/iron ore)|OFF|0.72|2026-09-16|https://mining.com.au/copper-price-hits-lowest-in-nearly-a-month-as-lme-stocks-rise/
Gold/silver price surge (monetary metals)|HIT|0.70|2026-09-16|https://www.reuters.com/world/india/gold-muted-investors-brace-fed-rate-decision-2026-09-16/
China PMI / property demand rebound|OFF|0.78|2026-09-15|https://www.stats.gov.cn/english/PressRelease/202609/t20260915_1965305.html
Inventory draw (LME/exchange stocks down)|OFF|0.80|2026-09-16|https://www.westmetall.com/en/markdaten.php
Supply disruption (mine/export ban)|OFF|0.55|2026-09-16|https://www.cnbc.com/2026/09/16/oil-falls-as-us-crude-inventories-rise-despite-saudi-supply-concerns.html
Critical-minerals policy / domestic tariff support|PARTIAL|0.58|2026-09-16|https://www.trefis.com/stock/fcx/articles/615048/why-did-freeport-mcmoran-stock-drop-on-doubts-over-a-tariff-it-would-gain-from/2026-09-11
Industrial metal price collapse|OFF|0.68|2026-09-16|https://www.brecorder.com/news/40439627
China demand shock / property stress|HIT|0.74|2026-09-15|https://www.bloomberg.com/news/articles/2026-09-15/china-home-price-slump-persists-as-focus-turns-to-policy-support
USD spike vs commodity complex|OFF|0.75|2026-09-16|https://www.marketscreener.com/news/gold-rises-on-easing-dollar-oil-as-fed-rate-verdict-looms-ce785bd2da8df120
Supply glut / new capacity online|HIT|0.76|2026-09-16|https://mining.com.au/copper-price-hits-lowest-in-nearly-a-month-as-lme-stocks-rise/
Margin compression / cost inflation without pricing power|OFF|0.66|2026-09-16|https://www.cnbc.com/2026/09/16/oil-falls-as-us-crude-inventories-rise-despite-saudi-supply-concerns.html
Sector rotation into materials|PARTIAL|0.50|2026-09-15|https://www.benzinga.com/etfs/sector-etfs/26/09/61789034/leading-and-lagging-sectors-september-15-2026
Sector rotation out of materials|PARTIAL|0.52|2026-09-16|https://insiderstreet.ai/etfs/XLB
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- web_search: FOMC September 16 2026 Fed decision rate hike odds premarket
- web_search: copper price LME inventory China PMI property September 2026
- web_search: XLB materials stocks premarket gold silver copper oil September 16 2026
- web_search: XLB ETF flows holdings LIN SHW FCX NEM breadth September 2026
- web_search: gold silver price Fed decision September 16 2026
- web_search: oil WTI Brent inventory Hormuz Iran September 16 2026
- web_search: Section 232 copper tariff Freeport FCX XLB September 2026
- web_search: XLB constituents LIN SHW ECL FCX NEM premarket September 16 2026
- web_search: China August 2026 industrial production property FAI copper demand
- web_search: LME copper stocks warehouse 254000 September 16 2026
- web_search: XLB premarket open September 16 2026 Materials Select Sector
- web_search: CME FedWatch September 2026 25bp hike probability September 16
- web_search: risk on rotation materials chemicals vs miners breadth September 16 2026
- x_search: copper LME gold XLB FOMC materials sector September 16 2026 (from 2026-09-15 to 2026-09-16)
- web_fetch: Reuters futures / gold URLs (blocked 401 / JS wall)
- memory_search: Basic Materials XLB lessons (index disabled)

**Key sources and facts used**

| Source | URL | Timestamp / as-of | Facts taken |
|---|---|---|---|
| TipRanks / futures | https://www.tipranks.com/news/stock-futures-rise-ahead-of-feds-interest-rate-decision | 2026-09-16 | Premarket futures modestly green into FOMC; hike odds ~92%+ |
| Bloomberg newsletter | https://www.bloomberg.com/news/newsletters/2026-09-16/bond-traders-are-convinced-the-fed-will-hike-interest-rates | 2026-09-16 | ~94% hike probability; bond market treating 25 bp as base case |
| Fed meeting schedule | https://fedratecalc.com/fomc-meeting-schedule/september-2026/ | 2026-09-16 | Decision 14:00 ET, Warsh presser 14:30 ET, SEP/dots |
| Reuters gold | https://www.reuters.com/world/india/gold-muted-investors-brace-fed-rate-decision-2026-09-16/ | 2026-09-16 | Gold firmer/muted into Fed; hike ~90%+ priced |
| MarketScreener gold | https://www.marketscreener.com/news/gold-rises-on-easing-dollar-oil-as-fed-rate-verdict-looms-ce785bd2da8df120 | 2026-09-16 | Gold bid on easier dollar and lower oil |
| MarketWatch XLB | https://www.marketwatch.com/investing/fund/xlb/download-data | 2026-09-16 ~04:00 ET | XLB PM $50.73, **0.00%** vs 09-15 close |
| Business Insider premarket | https://markets.businessinsider.com/premarket | 2026-09-16 | Gold/silver up, WTI down, XLB flat |
| Westmetall / LME | https://www.westmetall.com/en/markdaten.php | 2026-09-15/16 | LME Cu stocks **249,225 t** (builds; ~+20% / 30d) |
| mining.com.au | https://mining.com.au/copper-price-hits-lowest-in-nearly-a-month-as-lme-stocks-rise/ | ~2026-09-15 | Cu near 1-month low then recover; inventory rebuild |
| BRecorder LME | https://www.brecorder.com/news/40439627 | 2026-09-16 | LME cash ~$14,043–14,044/t, 3m ~$14,065 |
| NBS / Stats.gov | https://www.stats.gov.cn/english/PressRelease/202609/t20260915_1965305.html | 2026-09-15 | IP +5.2%; property FAI −19.9%; sales floor −12.1% |
| Bloomberg China homes | https://www.bloomberg.com/news/articles/2026-09-15/china-home-price-slump-persists-as-focus-turns-to-policy-support | 2026-09-15 | New homes −0.17% m/m; resale −0.31% m/m |
| CNBC oil | https://www.cnbc.com/2026/09/16/oil-falls-as-us-crude-inventories-rise-despite-saudi-supply-concerns.html | 2026-09-16 | Oil offered on API **+7.1 mb** crude build; Hormuz still a background risk |
| Trefis FCX | https://www.trefis.com/stock/fcx/articles/615048/why-did-freeport-mcmoran-stock-drop-on-doubts-over-a-tariff-it-would-gain-from/2026-09-11 | 2026-09-11 | Section 232 refined-Cu uncertainty; FCX sold off prior week |
| ETFdb / InsiderStreet | https://etfdb.com/etf/XLB ; https://insiderstreet.ai/etfs/XLB | mid-Sep 2026 | LIN ~13%, NEM ~8%, FCX ~6%, SHW ~4.8%; ~−$169M 1m flows |
| Yahoo quotes | https://finance.yahoo.com/quotes/LIN,SHW,NEM,CRH,ECL,APD,FCX,CTVA,VMC,MLM/ | 2026-09-15 close | LIN +1.43%, SHW +2.69%, ECL +2.51%, FCX +0.85% |
| X posts | https://x.com/LibertyLynx/status/2099706614295191717 | 2026-09-15 | LME stocks +16.9% to 242.9 kt; backwardation unwound |

Channel 1 numbers were used as injected (not re-derived): VIX 16.98 / ratio 0.877; Finviz futures; ES=F +1.14% / NQ=F +1.50%; WTI −1.59% / Brent −1.02%; gold +0.90% / silver +1.96% / copper +0.66% / aluminum +1.10%; USD −0.02%; Asia +0.65% / Europe +0.45%; XLB vs SPY 1d/3d/1w/1m rels; DFII10 2.60.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.5, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': 4.0, 'divergence_flagged': False, 'total_score': 9.667, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.55, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.978, 'score': 5.868, 'legs': [{'leg': 'ES', 'pct': 1.14, 'w': 0.6}, {'leg': 'HG', 'pct': 0.66, 'w': 0.3}, {'leg': 'GC', 'pct': 0.9, 'w': 0.1}, {'leg': 'DX', 'pct': -0.02, 'w': -0.3}]}, 'overlay_score': 2.475, 'overlay_raw': 2.475, 'index_carry': 1.324, 'general_total': 5.297, 'skill_multipliers': {'S0_SHARED_MACRO': 1.25, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 0.5, 'S3_FLOWS_POSITIONING': 1.25, 'S4_ETF_TAPE': 0.0}, 'llm_confidence': 0.42, 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': -2.08, 'w1': -4.78}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
