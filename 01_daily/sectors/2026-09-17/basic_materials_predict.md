# Sector Prediction — Basic Materials — 2026-09-17

- ETF: **XLB**
- rubric: `00_grounding/sectors/basic_materials.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **4.936** (mult 0.85)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **3.09** (ES +1.71%, HG +0.66%, GC +0.90%, DX -0.02%, PM:XLB +0.17%) · index_carry **1.846** (general 7.383) · llm_overlay **0.0** (raw 0.0)

## Channel 1 sector ETF tape

```
ETF XLB vs SPY (yfinance, through 2026-09-16):
  1d: XLB -0.73% | SPY -0.44% | rel -0.29%
  3d: XLB -1.16% | SPY -1.34% | rel +0.18%
  1w: XLB -2.00% | SPY -1.10% | rel -0.91%
  1m: XLB -3.60% | SPY -2.41% | rel -1.19%
```

MEMORY_CONFIRM: Sector Basic Materials / XLB only. Memory index unavailable this run (embedding metadata missing) — used injected scoreboard/lessons only. Rolling last-10 dir=0.4 mag=0.5 (n=10); last graded 2026-09-16 flat/flat vs XLB −0.73% / SPY −0.44% / rel −0.29% (dir MISS, mag MISS). No open experiment for `sector_basic_materials`. Active XLB rules checked: **8/28 S0=S1=0 residual-is-flat is BINDING** — no fresh China print, no metal surge/collapse, oil offered not a live Hormuz squeeze; leftover 1w/1m lag is not a down mandate. **09-16 FOMC-transmission lesson is ON as process, OFF as a down trigger** — hike/SEP/Warsh already printed into Wednesday’s close; do not re-score the binary, do not assume close≈open, and do not pay oil-relief + gold as cash-XLB transmission after that miss. **09-15 nested-bid ON as process, OFF as copper-HEAT trigger** — PM:XLB **+0.17%** is only modestly green; MAP HEAT Copper is **down**; do not flip down against a non-red parent PM, and do not invent a nested bid. **09-11 four-index ≥+0.5% up-gate OFF** (Finviz SPX +0.20 / NQ +0.41 / RTY +0.08 / DJIA +0.11). **09-10 gap-at-open OFF** (XLB PM **+0.17%** << 1%). **09-09 S1=−2 / S2=−1 OFF** (8/18 co-move not firing; offset not zero). **8/18 metals-as-floor OFF**. **8/14 gold-offset ON as a sleeve** (Finviz GC **+0.90%**, SI **+1.96%**) — not a book bid after yesterday’s NEM miss. **8/25 up-ban ON as transmission discount** (NQ=F **+2.1%** vs ES=F **+1.71%**, XLK PM **+1.28%** vs XLB **+0.17%**; 1d rel **−0.29% < 0.5%** — cannot emit up). **8/27 S4-cap ON** (1d rel −0.29%). **09-04 T-1-lag ON** — do not copy Wednesday’s FOMC dump into S4. **09-03 exhaustion-bounce OFF** (no >+1% sleeve reversal this morning). DO-INSTEAD: prefer flat/mild when score sign fights tape; keep direction and shrink confidence on modest |score|.

## Analysis — XLB, session of 2026-09-17 (T+1 after FOMC)

This is a **Thursday rebound attempt after Wednesday’s hawkish FOMC dump**, not a copper-squeeze day and not a fresh kinetic oil shock. Channel 1 tape through 09-16: 1d XLB **−0.73%** / SPY **−0.44%** / rel **−0.29%**, 3d rel **+0.18%**, 1w rel **−0.91%**, 1m rel **−1.19%**. XLB premarket **+0.17%**. The 14:00 ET statement / SEP / Warsh presser is **T-1 and already in the cash close** — do not pre-score it, and do not treat the hawkish *level* as a same-open smash.

### 1. Shared macro as it hits materials (S0)

Knowable this morning (Channel 1, do not re-derive):

- **FOMC is printed, not pending.** 25 bp hike to 3.75–4.00%, unanimous 12–0, first hike since 2023; SEP **16/18** another hike by year-end. That is Wednesday’s object. News Judge #1 (warm retail into FOMC) and #2 (Warsh JH / gold −3%) are **paid**.
- **Futures are a tech-led bounce, not a materials thrust.** Finviz four-index **fails** ≥ +0.5%. ES=F **+1.71%** / NQ=F **+2.10%** vs prior close is the overnight repair of Wednesday’s selloff. XLK PM **+1.28%** vs XLB **+0.17%** is **8/25 transmission**: NQ > ES is not an XLB green light.
- **Oil offered, 8/18 OFF.** WTI **−1.59%** to $104.16, Brent **−1.02%** to $107.67, CL=F **−0.98%**. Bloomberg: Brent extended Wednesday’s drop as Saudi East-West pipeline restoration and Trump “war ends soon” talk eased the supply scare. Tanker rates remain a *level*; News Judge correctly said overnight kinetic rules **do not fire**. Finviz “oil surges” is **stale vs Channel 1**. Count feedstock relief in S1, not a second S0 plus.
- **USD / real yields:** DXY **−0.02%** / 1d **−0.13%** — not a spike. DFII10 **2.62** is an elevated *level* (09-15 print); live 10Y note **−0.03%**, 30Y **−0.06%** — yields paring, not a same-morning smash. 5-day 10Y–SPX corr **−0.109**.
- **VIX 16.04 (−1.67)** with VIX/VIX3M **0.813 contango**. HY OAS **2.76** still contained. Asia composite **−0.03%** (Hang Seng **−0.44%**, Shanghai **−0.41%**); Europe **+0.45%**.

**S0 = 0.** Not +1: four-index gate off, bounce is XLK-led, Asia not confirming China, hawkish path still the cyclical overlay. Not −1: oil offered, USD not spiking, futures not red, FOMC already in yesterday’s print — using the hike *level* to force down is the 09-15 error.

### 2. Spine + secondary (S1)

**Industrial metals — bounce, not surge, not collapse.** COMEX copper **$6.489 (+0.66%)**; aluminum **+1.10%**; iron ore **−0.14%**; steel HRC **−0.16%**. LME cash ~**$14,200–14,230/t** after the three-week low near **$13,926**. Spine “surge” **OFF**. Spine “collapse” **OFF** (that was 09-10/09-14). MAP HEAT Copper **down** (FCX sold off with stalled cathode tariffs) — do not average a nested long into the parent.

**Inventory draw — inverted (glut HIT).** LME copper stocks **~254.2 kt**, **+~20% in 30 days**; cash-3M squeeze **unwound to flat/slight contango**. Inverse of the spine’s inventory-draw HIT.

**China demand — still contraction, not a rebound; do not let gold cancel it.** NBS mfg PMI **49.8** (<50), construction **46.9**, property FAI still ~**−19–20% YoY**. T-2, not a same-morning miss. Yangshan import premium **~$118/t** (highest in ~4 years) is a **physical restocking** signal after the dip — a partial industrial offset, **not** a PMI/property rebound HIT. Hang Seng/Shanghai are **red**, so it is not a US-open China impulse.

**Monetary metals — 8/14 sleeve ON, book bid OFF.** Finviz gold **+0.90%**, silver **+1.96%** (live). GC=F **−0.73% 1d** is Wednesday’s already-printed dump — do not restack it. NEM ~8% of XLB; Gold HEAT residual is still **down**. Yesterday gold futures were green in the morning note and cash XLB still died. Score the sleeve, do not promote S1.

**Chemicals majority sleeve — oil-offered cost relief, continuation not confirmed.** LIN ~13%, SHW ~4.8%, ECL/APD in the 40–50% processor book. This is the composition math (09-08), but it is the **same narrative that failed to transmit yesterday** with PM:XLB flat and LIN unconfirmed. Chemicals HEAT **up** is **HUN/REX nested**, not LIN/SHW — do not average child-books into XLB. Haircut the relief to **neutral**, not +1.

**Supply disruption / tariffs — stale.** DRC concentrate ban and Section 232 refined-copper uncertainty remain on the books; Copper HEAT says the tariff tape is a **drag**, not support. Hormuz is oil, not a metals mine outage.

**S1 = 0.** Oil-relief + gold sleeve + copper bounce are offset by China contraction + LME glut + Copper HEAT down + failed T-1 transmission. Not +1 (repeats 09-16). Not −1 (no collapse, 8/18 off, 09-09 zero-offset mandate off). Gold does **not** cancel China.

### 3. Breadth (S2)

HEAT is a **split, not a thrust**: Copper/Gold/Aluminum/Ag/Building Materials **down**; Chemicals **up** only in nested HUN/REX; Lumber the only clean green group. Parent PM **+0.17%** with XLK **+1.28%** is large-cap/tech leadership **outside** the book, not sector breadth expansion. No same-morning % names-up board. Do not copy Wednesday’s lag into S2 (8/28, 09-04). 09-09 S2=−1 requires 8/18 uniform co-move — **off**.

**S2 = 0.**

### 4. Flows / positioning (S3)

XLB ~**−$127M to −$169M** 1m net, ~**−$157M** 5d — residual outflows, not a same-open volume spike or washout. 1m rel **−1.19%** is a laggard, not a crowded long. No rebalance print.

**S3 = 0.**

### 5. Tape (S4, confirmation only)

1d rel **−0.29% < 0.5%** → **8/27 S4-cap** → cannot be ± confirmation. Wednesday’s −0.73% is T-1 (09-04). PM +0.17% is not a breakout.

**S4 = 0.**

### Reconciliation / self-audit

Leading sum **S0–S3 = 0**; S4 = 0. **No leading-vs-tape fight.** 8/25 forbids **up**. 8/28 forbids **down** when S0=S1=0 with no fresh China/metal/oil HIT. 09-15 forbids flipping down against a non-red PM. 09-16’s down/mild lean was for an *unprinted* 14:00 beta dump — that trigger is **off**. Residual is **flat/flat**. Lens is XLB, not SPX and not FCX/NEM/HUN. Band stays flat (gap **+0.17%**, size_gate on). Same-shock: FOMC counted once as T-1; oil counted in S1 as non-event continuation, not in S0. Single-ticker nested HEAT not driving the ETF.

**Horizons (relative to SPY, not a same-session call):** 3D mixed/choppy after the FOMC print (3d rel already +0.18%). 1W/2W still a lagging cyclical while the hike path is the regime and China/property stays <50. 1M structural lag until inventory tightness or a real China rebound returns — gold strength does not rewrite that.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 0
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.85
CONFIDENCE: 0.44
REGIME: mixed
HORIZON_3D: 0
HORIZON_1W: -0.5
HORIZON_2W: -0.5
HORIZON_1M: -1
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|PARTIAL|0.55|2026-09-17|https://www.swissinfo.ch/eng/stock-futures-rebound%2c-bonds-pare-losses-after-fed%3a-markets-wrap/92071875
Risk-off tape / flight to safety|OFF|0.70|2026-09-17|https://www.swissinfo.ch/eng/stock-futures-rebound%2c-bonds-pare-losses-after-fed%3a-markets-wrap/92071875
Real yields rising|OFF|0.60|2026-09-17|https://www.swissinfo.ch/eng/stock-futures-rebound%2c-bonds-pare-losses-after-fed%3a-markets-wrap/92071875
Real yields falling|PARTIAL|0.45|2026-09-17|https://www.swissinfo.ch/eng/stock-futures-rebound%2c-bonds-pare-losses-after-fed%3a-markets-wrap/92071875
USD strengthening|OFF|0.65|2026-09-17|https://www.swissinfo.ch/eng/stock-futures-rebound%2c-bonds-pare-losses-after-fed%3a-markets-wrap/92071875
USD weakening|OFF|0.55|2026-09-17|https://www.swissinfo.ch/eng/stock-futures-rebound%2c-bonds-pare-losses-after-fed%3a-markets-wrap/92071875
Sector breadth expansion (% names up)|OFF|0.60|2026-09-17|
Sector breadth failure (ETF up, names flat)|OFF|0.50|2026-09-17|
Large-cap leadership inside sector|OFF|0.50|2026-09-17|
Small/mid leadership inside sector|OFF|0.50|2026-09-17|
High-beta leadership inside sector|OFF|0.55|2026-09-17|
Low-beta leadership inside sector|OFF|0.50|2026-09-17|
Sector ETF inflow / relative volume spike|OFF|0.55|2026-09-17|https://etfdb.com/etf/XLB
Sector ETF outflow / volume dry-up|PARTIAL|0.50|2026-09-17|https://etfdb.com/etf/XLB
Crowded long (extreme relative performance + valuation)|OFF|0.70|2026-09-17|
Index rebalance / inclusion tailwind|OFF|0.80|2026-09-17|
Index exclusion / forced selling|OFF|0.80|2026-09-17|
Industrial metal price surge (copper/aluminum/iron ore)|OFF|0.70|2026-09-17|https://www.marketscreener.com/news/copper-rises-as-improving-china-demand-counters-fed-rate-hike-ce785bd3d98ef52c
Gold/silver price surge (monetary metals)|PARTIAL|0.60|2026-09-17|https://www.swissinfo.ch/eng/stock-futures-rebound%2c-bonds-pare-losses-after-fed%3a-markets-wrap/92071875
China PMI / property demand rebound|OFF|0.75|2026-09-17|https://www.stats.gov.cn/english/PressRelease/202609/t20260901_1965170.html
Inventory draw (LME/exchange stocks down)|OFF|0.80|2026-09-17|https://mining.com.au/copper-price-hits-lowest-in-nearly-a-month-as-lme-stocks-rise/
Supply disruption (mine/export ban)|OFF|0.65|2026-09-17|
Critical-minerals policy / domestic tariff support|OFF|0.60|2026-09-17|
Industrial metal price collapse|OFF|0.70|2026-09-17|https://www.marketscreener.com/news/copper-rises-as-improving-china-demand-counters-fed-rate-hike-ce785bd3d98ef52c
China demand shock / property stress|HIT|0.70|2026-09-17|https://www.stats.gov.cn/english/PressRelease/202609/t20260901_1965170.html
USD spike vs commodity complex|OFF|0.75|2026-09-17|
Supply glut / new capacity online|HIT|0.70|2026-09-17|https://mining.com.au/copper-price-hits-lowest-in-nearly-a-month-as-lme-stocks-rise/
Margin compression / cost inflation without pricing power|OFF|0.60|2026-09-17|
Sector rotation into materials|OFF|0.65|2026-09-17|
Sector rotation out of materials|PARTIAL|0.55|2026-09-17|
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- web_search: `FOMC September 16 2026 decision rate hike Warsh SEP`
- web_search: `copper price LME inventory September 17 2026`
- web_search: `XLB premarket stocks LIN SHW FCX NEM September 17 2026`
- web_search: `gold silver price September 17 2026 after Fed hike`
- web_search: `China PMI property copper demand September 2026`
- web_search: `XLB ETF flows holdings LIN SHW NEM FCX September 2026`
- web_search: `stock market futures September 17 2026 after Fed rate hike Warsh`
- web_search: `LME copper stocks backwardation September 2026`
- web_search: `copper rises China demand Fed rate hike September 16 2026`
- web_search: `XLB LIN SHW NEM FCX premarket September 17 2026 after Fed`
- web_search: `Hormuz oil tanker rates September 17 2026`
- web_search: `XLB ETF inflow outflow September 2026`
- web_fetch: `https://www.federalreserve.gov/newsevents/pressreleases/monetary20260916a.htm`
- web_fetch: `https://www.swissinfo.ch/eng/stock-futures-rebound%2c-bonds-pare-losses-after-fed%3a-markets-wrap/92071875`
- x_search: `XLB copper gold materials stocks premarket September 17 2026 after FOMC` (2026-09-16 to 2026-09-17)
- memory_search: Basic Materials XLB lessons (index unavailable)

**Key sources and facts taken**
- Federal Reserve FOMC statement, 2026-09-16 14:00 ET — https://www.federalreserve.gov/newsevents/pressreleases/monetary20260916a.htm — 25 bp hike to 3.75–4.00%, 12–0 vote; inflation elevated; action to support return to 2%.
- CNBC / NYT / Axios via search — SEP 16/18 another hike; first hike since 2023 under Warsh; hike effective 2026-09-17.
- Bloomberg via Swissinfo, 2026-09-17 07:57 — https://www.swissinfo.ch/eng/stock-futures-rebound%2c-bonds-pare-losses-after-fed%3a-markets-wrap/92071875 — ES futures rebound ~0.4–0.5% after Wednesday’s low; 2Y/10Y yields −2 to −3 bp; dollar held prior gains; Brent −0.9% near $104.90 on pipeline restoration / Trump “war ends soon”; spot gold +0.8% to ~$4,299 after three down days; “credibility relief trade.”
- MarketScreener / Reuters-line, 2026-09-16 — https://www.marketscreener.com/news/copper-rises-as-improving-china-demand-counters-fed-rate-hike-ce785bd3d98ef52c — LME 3M copper ~+1.1% to ~$14,231; Yangshan premium +7% to $118/t, ~4-year high.
- mining.com.au — https://mining.com.au/copper-price-hits-lowest-in-nearly-a-month-as-lme-stocks-rise/ — LME stocks rebuilt, tightness eased, prices off records.
- Westmetall / vault report via search — LME Cu cash ~$14,201–14,227 vs 3M ~$14,232 (flat/slight contango); stocks ~254.15–254.2 kt on 09-16 vs 249.2 kt on 09-15.
- NBS China, 2026-09-01 — https://www.stats.gov.cn/english/PressRelease/202609/t20260901_1965170.html — August mfg PMI 49.8; construction 46.9.
- ETFdb / ETF Action via search — XLB ~1m net −$127M to −$169M; 5d ~−$157M; AUM ~$8.1–8.6B; LIN ~13%, NEM ~8%, FCX ~6%, SHW ~4.8%.
- MarketWatch via search — XLB PM ~$50.47, ~+0.21% vs 09-16 close $50.36 (aligns with Channel 1 XLB PM +0.17%).
- Channel 1 (trusted, not altered) — Finviz futures, ES/NQ vs prior close, XLB vs SPY tape, VIX 16.04 / ratio 0.813, oil/gold/copper/USD/notes, Asia −0.03%, Europe +0.45%, sector PM board.
- MAP HEAT (injected) — Copper/Gold down; Chemicals up only nested HUN/REX; size_gate True.
- News Judge (injected) — FOMC binary was yesterday; kinetic Hormuz overnight rules do not fire; oil-surge headline vs inventory-build contradiction.

**Checked, nothing material**
- Same-morning China hard-data print (09-17): none.
- XLB constituent-level premarket quotes for LIN/SHW/FCX/NEM: not available as a clean board.
- Fresh mine/export-ban increment: none.
- Index rebalance / forced inclusion-exclusion: none.
- Fear & Greed / CME FedWatch scrape: Channel 1 unavailable; post-hike path taken from SEP/press coverage instead.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.85, 'leading_sum': 0.0, 'divergence_flagged': False, 'total_score': 4.936, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.55, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.515, 'score': 3.09, 'legs': [{'leg': 'ES', 'pct': 1.71, 'w': 0.6}, {'leg': 'HG', 'pct': 0.66, 'w': 0.3}, {'leg': 'GC', 'pct': 0.9, 'w': 0.1}, {'leg': 'DX', 'pct': -0.02, 'w': -0.3}, {'leg': 'PM:XLB', 'pct': 0.17, 'w': 0.7}]}, 'overlay_score': 0.0, 'overlay_raw': 0.0, 'index_carry': 1.846, 'general_total': 7.383, 'skill_multipliers': {'S0_SHARED_MACRO': 1.25, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 0.5, 'S3_FLOWS_POSITIONING': 1.25, 'S4_ETF_TAPE': 0.0}, 'llm_confidence': 0.44, 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': -2.08, 'w1': -4.78}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
