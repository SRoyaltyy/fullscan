# Sector Prediction — Basic Materials — 2026-09-01

- ETF: **XLB**
- rubric: `00_grounding/sectors/basic_materials.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-4.5** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLB vs SPY (yfinance, through 2026-08-31):
  1d: XLB -1.01% | SPY -0.53% | rel -0.49%
  3d: XLB -1.66% | SPY +0.15% | rel -1.81%
  1w: XLB -1.59% | SPY +0.17% | rel -1.76%
  1m: XLB +2.03% | SPY +3.42% | rel -1.39%
```

MEMORY_CONFIRM: Sector Basic Materials / XLB only. Memory index unavailable this run (embedding metadata missing) — used injected scoreboard/lessons only. Rolling last-10 dir=0.5 mag=0.6 (n=10). Last graded: 08-28 down/flat vs XLB −0.09% / rel +0.13% (dir MISS, mag HIT); 08-27 up/flat vs −0.82% / rel −1.48% (dir+mag MISS); 08-26 up/flat HIT; 08-21 up/severe HIT. Active XLB rules checked: temper-severe **partial** (1d rel −0.49% <0.5%, China still <50, live Hormuz/oil>$90 — S0 ≤0, no severe); 8/17 China-miss+flat-futures severe ban **does not fully fire** (NBS PMI is T-1 8/31, not a same-morning Sep-1 print; futures are independently weak, not flat); 8/18 metals-co-move **ON** (Hormuz live, WTI +2.59% / Brent ~$92, copper and gold already red with equities — do not use metals as a floor); 8/14 gold-offset **OFF** (GC −1.31%, SI −2.49%, not green); 8/21 keep-severe **OFF**; 8/25 composition/transmission **OFF as an up-ban trigger** (NQ is *weaker* than ES, not NQ>>ES risk-on); 8/27 S4-cap **ON** (1d rel <0.5% → S4 cannot be a + confirmation); 8/28 leftover-S2/S4 down-mandate **does not bind** because S0 and S1 are live-negative, not net-zero. No open experiment for `sector_basic_materials`. PCE/Warsh are T+ sessions (8/26–8/28), not today’s 8:30. **ISM Manufacturing (August) is scheduled 10:00 ET today** — two-sided sector-owned print, consensus ~55.2 vs July 55.6; do not pre-score the print.

## Analysis — XLB, session of 2026-09-01

This is a **Tuesday risk-off open into a live oil/Hormuz overlay**, not a copper-squeeze day and not an 8/28 leftover-chemicals fade. Channel 1 tape through 8/31 is already soft (1d rel **−0.49%**, 3d **−1.81%**, 1w **−1.76%**, 1m **−1.39%**) but that tape is **Friday’s close**. Per 8/28, it does not forecast today by itself. What *is* live this morning is the macro/metals complex: ES **−0.53%**, NQ **−1.01%**, Europe **−0.69%**, copper **−1.32%**, gold **−1.31%**, silver **−2.49%**, WTI **+2.59%** / Brent **+1.89%** to ~$92.

### 1. Shared macro as it hits materials (S0)

Risk-on does **not** map here. NQ is weaker than ES — duration/growth selling, not a materials-led tape and not the 8/25 “tech-only green light.” Real yields are **up** (DFII10 **2.42, +8 bp 1d**); 10Y **4.73 (+6 bp)**; DXY **+0.16%** (not a USD spike vs the complex). VIX **15.81** with VIX/VIX3M **1.036 backwardation**. News Judge #1 (Warsh hawkish follow-through / Sep hike coin-flip) is the printed rates spine, not a two-sided speech still pending.

The materials-specific overlay is News Judge #2: **Hormuz still live** (IRGC control / US blockade). Channel 1 oil is **green and >$90**. That is the 8/18 setup: geopolitical/oil risk-off, industrial and monetary metals **co-move down with equities**, not a hedge. Do not score S0 as risk_on because oil is up.

China NBS mfg PMI **49.8** (8/31, +0.6 from 49.2) is **still contraction**; construction **46.9**. That is T-1, not today’s 8:30, so it is not an 8/17 same-morning hard-data miss. It keeps the China map from being a rebound.

**ISM 10:00 ET** is the same-session binary for this book (chemicals/industrial). Consensus slightly below the 55.6 prior but still expansion. Encode as event risk that **caps magnitude**, not as a pre-scored miss, and not as “no macro print.”

**S0 = −1.** Risk-off + rising real yields + Hormuz/oil>$90 map negative to this cyclical. Not −2: ES is only ~−0.5%, ISM is two-sided, DXY is not a spike, and the China print is already in Friday’s tape.

### 2. Spine + secondary (S1)

**Industrial metals — fade, not surge, not collapse.** Channel 1 copper **$6.599 (−1.32%)**. LME cash still elevated (~$14,535/t vs 3m ~$14,365, backwardation intact) but that is **tightness without a same-morning squeeze**. Spine “surge” is **off**. Spine “collapse” is **not** a clean HIT either.

**Inventory draw — off.** LME Cu stocks ~**234 kt** (8/28) after the mid-August rebuild from ~205 kt to ~240 kt. One-day draws do not restore the ultra-tight regime.

**China demand — still the industrial offset, not a rebound.** August NBS 49.8 still <50; construction 46.9; second-hand home prices still falling even as a 100-city new-home index ticked +0.15% MoM on mix. **Do not let gold cancel this — and today gold is not even green.**

**Monetary metals — fade.** GC **−1.31%**, SI **−2.49%**. 8/14 does not pay. NEM indicated softer premarket; that is the gold sleeve, not an XLB-wide chemicals smash.

**Supply disruption / tariffs — stale.** DRC concentrate ban and Section 232 copper remain on the books; not a same-open catalyst. APD’s Q3 beat/charge is **already traded** (late July).

**Oil-up is a cost headwind for processors, not an XLB squeeze.** Count Hormuz/oil once in S0 as the risk-off overlay; do not also credit copper as a positive floor (8/18).

**S1 = −1.** Copper/gold down + China still <50, net of residual backwardation. Capped well below −2/−3: not a metal collapse, PMI improved vs July, ISM unprinted. Gold is an 8/14 sleeve, not a second independent −1.

### 3. Breadth (S2)

8/28: do **not** copy Friday’s LIN/SHW/ECL lag into S2. Same-morning chemicals confirmation is required for S2 = −1. LIN is ~flat premarket; the live weakness is the **miner/metals sleeve** (copper/gold, NEM/FCX), which is already in S1. That is composition (chemicals-heavy book buffering a miner fade), not a fresh % names-down thrust across the ETF.

**S2 = 0.**

### 4. Flows / positioning (S3)

XLB ~1m net flows about **−$171M**, 5d about **−$69M** — mild outflow, not a volume spike and not a washout. Trailing unit outflows are not a 1-day lid.

**S3 = 0.** (Do not restack the same rotation already described by S0/S1.)

### 5. Tape (S4, confirmation only)

Channel 1 1d rel **−0.49%** is modest (<0.5%). 8/27: cap S4 at 0 on a sub-0.5% tape. 8/28: S4 confirms **this** session, not Friday’s −1.01%. Do not re-vote the prior close as today’s down confirmation.

**S4 = 0.** Leading sum (−1−1+0+0 = −2) vs tape 0 is a **magnitude/confirmation divergence**, not a direction fight. Trust factors (live oil/geo + metals down) over leftover tape. Do not let S4=0 flip the call to flat the way 8/28 forbade flipping leftover red into down when S0=S1=0 — here S0 and S1 are not zero.

### Self-audit
- **Lens:** XLB environment, not SPX, not NEM/FCX single-name. Chemicals (LIN) are the book; miners are a sleeve.
- **Band:** Down **mild**, not notable/severe. ISM 10:00 is two-sided; LIN not confirming smash; 8/18 co-move argues against a metals floor but does not by itself justify notable. Rolling mag 0.6 / dir 0.5 → shrink conviction on a modest |sum|.
- **Skew:** Gold weakness does **not** cancel China; both sleeves are negative. Oil-up ≠ copper squeeze.
- **Same-shock:** Hormuz/oil counted in S0; copper/gold transmission in S1 once. Not also a magnitude floor.
- **Single-ticker:** APD stale; NEM must not drive the ETF call.

Lean (pipeline owns the official band): **down / mild**, regime **risk_off**, multiplier **0.9**. Residual after 8/25–8/27 “do not emit up” is **not** 8/28’s flat default, because this morning’s metals/oil tape is live.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: risk_off
HORIZON_3D: down:mild:0.55
HORIZON_1W: down:mild:0.52
HORIZON_2W: flat:mild:0.48
HORIZON_1M: flat:mild:0.50
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|MISS|0.80|2026-09-01|https://www.reuters.com/business/wall-st-futures-kick-off-september-under-pressure-yields-oil-prices-rise-2026-09-01/
Risk-off tape / flight to safety|HIT|0.82|2026-09-01|https://www.reuters.com/business/wall-st-futures-kick-off-september-under-pressure-yields-oil-prices-rise-2026-09-01/
Real yields rising|HIT|0.85|2026-08-28|Channel 1 DFII10 2.42 +0.08 1d
Real yields falling|MISS|0.85|2026-08-28|Channel 1 DFII10 +0.08 1d
USD strengthening|HIT|0.55|2026-09-01|Channel 1 DXY +0.16% 1d (not a spike)
USD weakening|MISS|0.70|2026-09-01|Channel 1 DXY +0.16% 1d
Sector breadth expansion (% names up)|MISS|0.60|2026-09-01|checked, nothing material
Sector breadth failure (ETF up, names flat)|MISS|0.60|2026-09-01|ETF already red 8/31; not an ETF-up/names-flat day
Large-cap leadership inside sector|HIT|0.50|2026-09-01|LIN ~flat vs miner softness — quality/chemicals buffer, not a bull thrust
Small/mid leadership inside sector|MISS|0.50|2026-09-01|checked, nothing material
High-beta leadership inside sector|MISS|0.65|2026-09-01|FCX/NEM indicated softer with Cu/Au
Low-beta leadership inside sector|HIT|0.50|2026-09-01|chemicals/gases holding vs miners
Sector ETF inflow / relative volume spike|MISS|0.70|2026-08-28|https://etfdb.com/etf/XLB/
Sector ETF outflow / volume dry-up|HIT|0.60|2026-08-28|https://etfdb.com/etf/XLB/
Crowded long (extreme relative performance + valuation)|MISS|0.55|2026-08-31|1m rel already −1.39%; run has faded
Index rebalance / inclusion tailwind|MISS|0.40|2026-09-01|checked, nothing material
Index exclusion / forced selling|MISS|0.40|2026-09-01|checked, nothing material
Industrial metal price surge (copper/aluminum/iron ore)|MISS|0.85|2026-09-01|Channel 1 copper −1.32%
Gold/silver price surge (monetary metals)|MISS|0.88|2026-09-01|Channel 1 gold −1.31% / silver −2.49%
China PMI / property demand rebound|MISS|0.80|2026-08-31|https://english.www.gov.cn/archive/statistics/202608/31/content_WS6a952339c6d00ca5f9a0ce12.html
Inventory draw (LME/exchange stocks down)|MISS|0.70|2026-08-28|https://thevaultreport.com/lme/copper
Supply disruption (mine/export ban)|MISS|0.55|2026-09-01|DRC/Section 232 carried, not same-open
Critical-minerals policy / domestic tariff support|MISS|0.50|2026-09-01|carried Section 232; checked, nothing material today
Industrial metal price collapse|MISS|0.60|2026-09-01|Cu −1.32% is a fade, still backwardated/elevated
China demand shock / property stress|HIT|0.72|2026-08-31|https://www.cnbc.com/2026/08/31/china-pmi-august-economy-slowdown.html
USD spike vs commodity complex|MISS|0.70|2026-09-01|DXY +0.16% only
Supply glut / new capacity online|MISS|0.45|2026-09-01|checked, nothing material
Margin compression / cost inflation without pricing power|HIT|0.55|2026-09-01|WTI +2.59% cost impulse into chemicals/processors
Sector rotation into materials|MISS|0.75|2026-08-31|Channel 1 1d/3d/1w/1m rel all negative
Sector rotation out of materials|HIT|0.70|2026-08-31|Channel 1 XLB rel −0.49% 1d / −1.76% 1w
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- China August 2026 official manufacturing PMI property
- ISM Manufacturing PMI September 2026 release
- ISM Manufacturing PMI August 2026 release September 1
- economic calendar September 1 2026 ISM manufacturing
- copper price LME inventory September 1 2026
- copper COMEX price today September 1 2026 down
- LME copper stocks August 31 2026 westmetall
- gold silver price today September 2026
- Hormuz Iran oil blockade September 2026
- XLB premarket LIN FCX NEM SHW ECL September 1 2026
- XLB LIN SHW NEM FCX stock price September 1 2026
- XLB ETF flows August 2026 materials sector
- China property August 2026 new home prices PMI construction
- Air Products APD earnings XLB September 2026
- web_fetch Reuters futures wrap (JS wall / 401)

**Key sources and facts used**
- Channel 1 (injected, unaltered): ES −0.53%, NQ −1.01%; WTI +2.59% to $88.01, Brent +1.89% to $92.25; Cu −1.32% to $6.599; Au −1.31%; Ag −2.49%; DXY +0.16%; DFII10 2.42 +0.08; VIX 15.81, VIX/VIX3M 1.036; XLB vs SPY through 2026-08-31: 1d rel −0.49%, 3d −1.81%, 1w −1.76%, 1m −1.39%.
- NBS / English.gov.cn / CNBC (2026-08-31): China official mfg PMI 49.8 (from 49.2); production 50.4, new orders 50.6; non-manufacturing 49.0; construction 46.9. https://english.www.gov.cn/archive/statistics/202608/31/content_WS6a952339c6d00ca5f9a0ce12.html ; https://www.cnbc.com/2026/08/31/china-pmi-august-economy-slowdown.html
- Financecalendar / ISM calendar: August ISM Manufacturing PMI due **2026-09-01 10:00 ET**; July was 55.6; consensus ~55.2. https://www.financecalendar.com/us-ism-manufacturing-pmi/ ; https://www.ismworld.org/supply-management-news-and-reports/reports/rob-report-calendar/
- Reuters (2026-09-01): Wall St futures kick off September under pressure as yields and oil rise. https://www.reuters.com/business/wall-st-futures-kick-off-september-under-pressure-yields-oil-prices-rise-2026-09-01/
- Vault Report / Westmetall context: LME Cu stocks ~234.3 kt as of 2026-08-28 after mid-August rebuild. https://thevaultreport.com/lme/copper
- ETFDB: XLB ~1m net flows −$171M, 5d −$69M (as of ~8/28). https://etfdb.com/etf/XLB/
- News Judge 2026-09-01: Warsh hawkish follow-through (Sep hike coin-flip); Hormuz IRGC/US blockade still live.
- El País / Kpler (late Aug 2026): Hormuz still restricted, not normalized. https://english.elpais.com/international/2026-08-28/the-strait-of-hormuz-after-six-months-of-war-blockades-drones-and-the-shadow-fleet.html
- Premarket color: LIN ~flat (~$489.4); FCX indicated softer (~$75.10 vs $75.74); NEM indicated softer. Chemicals not confirming a same-morning smash.
- APD: Q3 already out 2026-07-30; not a Sep-1 catalyst. https://www.prnewswire.com/news-releases/air-products-reports-fiscal-2026-third-quarter-results-302838883.html

**Checked, nothing material**
- Fresh DRC/export-ban escalation this morning
- Fresh Section 232 / critical-minerals same-open catalyst
- XLB inflow spike / index rebalance
- Gold/silver bid (opposite: both down)
- China property *rebound* (construction PMI still 46.9; second-hand prices still falling)

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': -5.0, 'divergence_flagged': False, 'total_score': -4.5, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.55, 'regime': 'risk_off'}
```
