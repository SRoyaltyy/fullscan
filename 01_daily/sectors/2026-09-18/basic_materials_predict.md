# Sector Prediction — Basic Materials — 2026-09-18

- ETF: **XLB**
- rubric: `00_grounding/sectors/basic_materials.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **6.18** (mult 0.85)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **3.902** (ES +1.14%, HG +0.66%, GC +0.90%, DX -0.02%, PM:XLB +0.51%) · index_carry **1.215** (general 4.861) · llm_overlay **1.062** (raw 1.062)

## Channel 1 sector ETF tape

```
ETF XLB vs SPY (yfinance, through 2026-09-17):
  1d: XLB +0.69% | SPY +1.13% | rel -0.44%
  3d: XLB +0.44% | SPY +0.23% | rel +0.21%
  1w: XLB -0.10% | SPY +0.63% | rel -0.73%
  1m: XLB -2.07% | SPY -0.63% | rel -1.43%
```

MEMORY_CONFIRM: Sector Basic Materials / XLB only. Memory index unavailable this run (embedding metadata missing) — used injected scoreboard/lessons only. Rolling last-10 dir=0.4 mag=0.4 (n=10); last-30 dir=0.5 mag=0.455 (n=22); last graded 2026-09-17 flat/flat vs XLB +0.695% / SPY +1.134% / rel −0.439% (dir MISS, mag MISS). No open experiment for `sector_basic_materials`. Active XLB rules checked: **09-17 all-zero residual is BINDING** — S0–S3 may not collapse to flat when ES/NQ are ≥ +0.5% (ideally ≥ +1%), yields are paring, and XLB PM is green; residual is mild-up with relative lag, not flat; 8/28 residual-is-flat is conditioned on a *neutral* index tape; 8/27 S4-cap is a conviction cap (no confirmed-up), not a level cap. **09-16 FOMC-transmission ON as process, OFF as a down trigger** — hike/SEP/Warsh is T+2 and already in Wednesday’s close / Thursday’s repair; do not re-score the binary; do not overpay oil-relief + gold as cash-XLB transmission. **09-15 nested-bid ON as process, OFF as copper-HEAT trigger** — PM:XLB **+0.51%** is green (do not flip down); MAP HEAT Copper is **down** (FCX tariff stall) so do not invent a nested copper bid; score any live bid in exactly one of S1 or S4. **09-11 four-index ≥ +0.5% up-gate OFF** (Finviz SPX +0.20 / NQ +0.41 / RTY +0.08 / DJIA +0.11) but ES=F **+1.14%** / NQ=F **+1.50%** vs prior close are independently green — modest S0 lean, not a materials +2. **09-10 gap-at-open OFF** (XLB PM **+0.51%** << 1%). **09-09 S1=−2 / S2=−1 OFF** (8/18 co-move not firing; offset not zero). **8/18 metals-as-floor OFF** (oil offered, not a Hormuz squeeze into equity risk-off). **8/14 gold-offset ON as a sleeve** (GC **+0.90%**, SI **+1.96%**) — not a book bid after the 09-16 NEM miss. **8/25 up-ban ON as transmission/conviction discount** (NQ > ES; 1d rel **−0.44% < 0.5%** — cannot emit confirmed-up). **8/27 S4-cap ON**. **8/28 leftover-down OFF**. **09-04 T-1-lag ON** — do not copy Thursday’s +0.69% / −0.44% rel into S4. **09-03 exhaustion-bounce OFF** (no >+1% sleeve reversal this morning). **China/gold split ON** — do not let gold cancel China/industrial metals. DO-INSTEAD (last three BM losses): when score sign fights tape/breadth, cut conviction; prefer flat/mild. size_gate=True.

## Analysis — XLB, session of 2026-09-18 (Friday, T+2 after FOMC)

This is a **Friday continuation of Thursday’s post-FOMC repair**, not a copper-squeeze day and not a fresh kinetic oil shock. Channel 1 tape through 09-17: 1d XLB **+0.69%** / SPY **+1.13%** / rel **−0.44%**, 3d rel **+0.21%**, 1w rel **−0.73%**, 1m rel **−1.43%**. XLB premarket **+0.51%**. The 09-16 statement / SEP / Warsh presser is **already in the cash close** — do not pre-score it, and do not treat the hawkish *level* as a same-open smash (09-15 / 09-16). Repeating yesterday’s all-zero → flat/flat card is the named 09-17 miss.

### 1. Shared macro as it hits materials (S0)

Knowable this morning (Channel 1, do not re-derive):

- **No pending CPI/NFP/FOMC binary.** News Judge correctly applied none of those gates. Warsh JH / gold −3% is a *symptom of a paid path*, not a same-morning Chair shock. Finviz Barrick “gold surge on cut bets” is **stale vs live GC +0.90%**.
- **Index tape is green vs prior close, modest on the Finviz board.** ES=F **+1.14%**, NQ=F **+1.50%** — 09-17 condition (a) fires. Finviz four-index **fails** ≥ +0.5% (SPX +0.20 / NQ +0.41 / RTY +0.08 / DJIA +0.11), so this is **not** a four-index thrust and **not** an 8/25 materials green light. NQ > ES remains a tech tilt; today the gap is smaller than yesterday’s XLK +1.28% vs XLB +0.17%. XLB PM **+0.51%** is actually keeping up with XLK PM **+0.60%**.
- **Oil offered, 8/18 OFF.** WTI **−1.59%** to $104.16, Brent **−1.02%** to $107.67, CL=F **−6.31%** 1d, BZ=F **−6.17%**. News Judge #3: cheaper oil allays inflation optics and hits E&Ps — for *this* cyclical that is feedstock relief, not a metals-complex wipeout. Hormuz remains a *level* (Brent still >$100); the live increment is down. Count chemicals relief in S1, not a second S0 plus.
- **USD / real yields.** Finviz USD **−0.02%**; DXY 1d **+0.12%** — not a spike vs the complex. DFII10 **2.68 (+0.06 1d, +0.22 1w)** is still an elevated *level*. Live notes are slightly bid (10Y **−0.03%**, 30Y **−0.06%**) — modest duration relief, not a real-yield smash. 5-day 10Y–SPX corr **−0.437** (no longer the −0.97 stress tape).
- **VIX 15.22 (−0.22)** with VIX/VIX3M **0.82 contango**. HY OAS **2.70** contained. EPU **106.54** sharply off the 09-14 spike. Asia composite **+1.11%** (Kospi +2.66%, Shanghai +0.94%, Hang Seng +0.60%); **Europe −0.46%** is the offset.

**S0 = +0.5.** Modest lean with independently green ES/NQ, oil offered, calm VIX, Asia green, and a green parent PM — the 09-17 / 09-11 knowable-tape rule. Not +1: four-index gate off, Europe red, real yields still elevated, hawkish path is the cyclical overlay, NQ > ES is not an XLB thrust. Not 0: that was yesterday’s miss — zeroing a ≥ +1% ES/NQ continuation with a green XLB PM. Not −1: oil is down, no kinetic increment, no same-morning China miss, USD is not spiking.

### 2. Spine + secondary (S1)

**Industrial metals — bounce, not surge, not collapse.** COMEX copper **$6.489 (+0.66%)**; aluminum **+1.10%**; iron ore **−0.14%**; steel HRC **−0.16%**. LME cash **$14,400.50**, 3M **$14,409** — **near-flat** after the squeeze unwound; still well off the ~$14,850–14,858 record. Spine “surge” **OFF**. Spine “collapse” **OFF** (that was 09-10 / 09-14). MAP HEAT Copper **down** (FCX tariff stall) — do **not** average a nested long into the parent, and do not treat +0.66% as a copper-squeeze HIT.

**Inventory draw — inverted (glut HIT).** LME copper stocks **255.9 kt** as of 09-17 (**+0.69% 24h, +9.1% 7d, +8.4% 30d**). Cash–3M **near flat** (~$8.50). Inverse of the spine’s inventory-draw HIT. SHFE tightness / Yangshan premium **~$118–121/t** (highest since late 2022) and SHFE stocks ~**54,780 t** are a **China physical restock** signal — not an LME draw, and **not** a PMI/property rebound.

**China demand — still contraction, not a rebound; do not let gold cancel it.** August NBS mfg **49.8** (<50), construction **46.9**, property FAI still ~**−19–20% YoY**. No same-morning China print. Hang Seng/Shanghai are **green**, so it is a carried industrial offset, not a US-open miss. Taxonomy rebound stays **OFF**.

**Monetary metals — 8/14 sleeve ON, book bid OFF.** Gold **+0.90%**, silver **+1.96%**, platinum **+0.61%**, palladium **+1.64%**. Score the firm metals bid as a **sleeve** (NEM ~8%), not a book bid — 09-16 already showed gold futures ≠ cash XLB. MAP HEAT Gold is **down** on residual after the hike; NEM’s RBC hike does not rewrite that nested tape.

**Chemicals majority sleeve — oil-offered cost relief, transmission haircut.** LIN ~13%, SHW ~4.8%, ECL/APD still the chemicals-heavy book (~40–50% combined). WTI/Brent offered is majority-sleeve feedstock relief (09-08 composition math, inverted). MAP HEAT Chemicals is **flat** (DOW mixed, HUN pos) — not a LIN/SHW thrust. Per 09-16, do **not** pay oil-relief + gold as if they already printed in cash XLB.

**Tariffs / policy — stalled, not a same-open support HIT.** News Judge #6 + MAP HEAT: copper has retreated from records on **Section 232 refined-copper uncertainty**; White House plan stalled on affordability; FCX is the live negative captain. Critical-minerals support is **not** firing this morning. BHP ADR weakness is a single-name/miner sleeve, not an XLB-wide thrust.

**S1 = 0.** Gold sleeve + oil-offered chemicals relief + modest Cu/Al bounce are **offset** by China/property contraction + LME inventory rebuild + Copper/Gold HEAT-down + stalled cathode tariff. Not +1: no spine surge, China still <50, glut HIT, nested copper bid absent, 09-16 transmission haircut. Not −1: 8/18 is off, metals are green, oil is offering the majority sleeve, gold is green (8/14 sleeve). Gold does **not** cancel China.

### 3. Breadth (S2)

Same-morning nested tape is **heavy vs parent**: Copper/Gold/Aluminum/Ag-inputs/Building Materials/Other industrials all **down**; Chemicals **flat**; Lumber the only d1-green pocket. That is large-cap / parent-PM carry, not % names expansion. Do **not** average nested HEAT into the parent ETF, and do **not** fire 09-09 S2=−1 (that required 8/18 uniform co-move with zero offset). 8/28: do not copy Thursday’s lag into S2.

**S2 = 0.** Not +1 (HEAT is not expanding). Not −1 (chemicals majority is flat, not down; parent PM is green; 8/18 off).

### 4. Flows / positioning (S3)

XLB ~**−$127M to −$169M** 1m net, ~**−$157M** 5d — rotation into tech, not a washout and not a volume spike. 1m rel **−1.43%** is a laggard, not a crowded long. 09-15: a book that has already de-risked into an easing oil overlay should not take another S3 down-ding.

**S3 = 0.**

### 5. Tape (S4, confirmation only)

1d rel **−0.44% < 0.5%** → **8/27 S4-cap ON** → **S4 = 0**. Thursday’s print is T-1 (09-04); do not copy it. PM:XLB **+0.51%** is the live parent bid — scored as S0 participation, not also as S4 (09-15: exactly one of S1 or S4). 8/27 is a **conviction cap** (cannot emit confirmed-up), not a license to flatten a modest leading lean (09-17).

### Reconciliation / self-audit

Leading S0–S3 = **+0.5**. S4 = 0. No leading-vs-tape *sign* fight (S4 is capped, not negative). Trust factors over tape if they fight — they don’t.

- **Lens:** XLB ETF environment, not SPX, not FCX/NEM stock-picking.
- **Band:** mild, not notable — size_gate=True, |PM gap| 0.51% < 1% (09-10 off), modest |leading sum|, rolling mag 0.4.
- **Skew:** chemicals-heavy book gets oil relief; copper HEAT-down must not drive the ETF call.
- **Same-shock:** oil counted once (S1 chemicals relief); gold counted once (S1 sleeve); FOMC path not re-scored.
- **8/25:** relative lag is the expression (1w **−0.73%**, 1m **−1.43%**). Mild-up-with-lag ≠ confirmed-up.
- **DO-INSTEAD:** nested HEAT vs parent PM is a breadth conflict → **cut conviction**, keep mild.

**Expression the pipeline should not flatten:** absolute **up / mild**, **lagging vs SPY**. Friday continuation after Thursday’s gap-and-fade ($50.90 → $50.71) is a choppy mild, not a trend day.

HORIZON_3D: mild-up lagging (3d rel already +0.21%; no new spine).
HORIZON_1W: lag persists (1w rel −0.73%) unless China/property or LME drain inflects.
HORIZON_2W: mixed; hawkish path + LME rebuild vs oil-offered chemicals and physical China restock.
HORIZON_1M: structural lag (1m rel −1.43%) until PMI/property or inventory-draw spine fires.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0.5
S1_SECTOR_FACTORS: 0
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.85
CONFIDENCE: 0.48
REGIME: mixed
HORIZON_3D: up_mild_lag
HORIZON_1W: lag
HORIZON_2W: mixed
HORIZON_1M: lag
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|PARTIAL|0.62|2026-09-18|https://markets.businessinsider.com/premarket
Risk-off tape / flight to safety|OFF|0.70|2026-09-18|https://markets.businessinsider.com/premarket
Real yields rising|PARTIAL|0.64|2026-09-16|https://cryptobriefing.com/us-tips-auction-19b-highest-yield-2008/
Real yields falling|PARTIAL|0.55|2026-09-18|https://markets.businessinsider.com/premarket
USD strengthening|OFF|0.58|2026-09-18|https://www.fxstreet.com/news/united-states-dollar-index-trades-firmly-above-10000-while-approaching-to-weekend-202609180344
USD weakening|OFF|0.55|2026-09-18|https://markets.businessinsider.com/premarket
Sector breadth expansion (% names up)|OFF|0.66|2026-09-18|
Sector breadth failure (ETF up, names flat)|PARTIAL|0.60|2026-09-18|
Large-cap leadership inside sector|HIT|0.58|2026-09-18|https://www.tradingview.com/symbols/AMEX-XLB/holdings/
Small/mid leadership inside sector|OFF|0.55|2026-09-18|
High-beta leadership inside sector|OFF|0.52|2026-09-18|
Low-beta leadership inside sector|PARTIAL|0.50|2026-09-18|
Sector ETF inflow / relative volume spike|OFF|0.63|2026-09-18|https://etfdb.com/etf/XLB
Sector ETF outflow / volume dry-up|PARTIAL|0.62|2026-09-18|https://etfdb.com/etf/XLB
Crowded long (extreme relative performance + valuation)|OFF|0.70|2026-09-18|
Index rebalance / inclusion tailwind|OFF|0.40|2026-09-18|
Index exclusion / forced selling|OFF|0.40|2026-09-18|
Industrial metal price surge (copper/aluminum/iron ore)|OFF|0.72|2026-09-18|https://thevaultreport.com/lme/copper
Gold/silver price surge (monetary metals)|PARTIAL|0.68|2026-09-18|https://www.kitco.com/news/article/2026-09-16/gold-price-buckles-under-warshs-inflation-fixation-fed-hikes-rates
China PMI / property demand rebound|OFF|0.78|2026-09-01|https://www.stats.gov.cn/english/PressRelease/202609/t20260901_1965170.html
Inventory draw (LME/exchange stocks down)|OFF|0.80|2026-09-17|https://thevaultreport.com/lme/copper
Supply disruption (mine/export ban)|OFF|0.55|2026-09-18|
Critical-minerals policy / domestic tariff support|OFF|0.66|2026-09-18|https://www.lse.co.uk/news/white-house-copper-tariff-plan-stalls-amid-affordability-concerns-sources-say-k10120hh9mrpre6.html
Industrial metal price collapse|OFF|0.74|2026-09-18|https://thevaultreport.com/lme/copper
China demand shock / property stress|HIT|0.74|2026-09-01|https://www.focus-economics.com/countries/china/news/pmi/china-pmi-02-09-2026-manufacturing-and-non-manufacturing-pmis-stay-soft-in-august/
USD spike vs commodity complex|OFF|0.70|2026-09-18|
Supply glut / new capacity online|HIT|0.76|2026-09-17|https://thevaultreport.com/lme/copper
Margin compression / cost inflation without pricing power|OFF|0.60|2026-09-18|
Sector rotation into materials|OFF|0.62|2026-09-18|https://www.etfaction.com/tech-sector-and-ultrashort-bonds-draw-steady-inflows/
Sector rotation out of materials|PARTIAL|0.60|2026-09-18|https://etfdb.com/etf/XLB
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- memory_search: Basic Materials XLB sector prediction lessons copper China gold chemicals (index unavailable)
- web_search: copper price LME inventory stocks September 18 2026
- web_search: China PMI property copper demand September 2026
- web_search: gold silver price Fed Warsh XLB materials September 18 2026
- web_search: XLB ETF flows holdings LIN SHW copper tariff Section 232
- web_search: US stock futures risk on VIX real yields DXY September 18 2026
- web_search: XLB premarket breadth LIN SHW NEM FCX September 18 2026
- web_search: LME copper stocks 255900 backwardation contango September 2026
- web_search: oil prices Hormuz Brent WTI inventory build September 18 2026
- web_search: XLB ETF inflow outflow September 2026 materials sector rotation
- web_search: Section 232 copper tariff FCX BHP September 2026
- web_search: China Yangshan copper premium SHFE inventory September 2026
- web_search: XLB LIN Sherwin Williams chemicals oil feedstock September 18 2026
- web_search: materials sector ETF XLB vs SPY Friday September 18 2026 premarket
- x_search: XLB copper gold LME stocks China property materials sector September 18 2026 (2026-09-16 to 2026-09-18)
- web_fetch: https://thevaultreport.com/lme/copper
- web_fetch: https://www.tipranks.com/news/u-s-stock-futures-edge-higher-after-thursdays-rally (403)

**Key sources and facts taken**

1. **The Vault Report — LME Copper** — https://thevaultreport.com/lme/copper — as of 2026-09-17: warehouse stock **255.9 kt** (+0.69% 24h, +9.1% 7d, +8.4% 30d); cash **$14,400.50**, 3M **$14,409**, cash vs 3M **near flat**. Used for inventory-rebuild HIT and surge/collapse OFF.
2. **NBS / FocusEconomics — China August PMI** — https://www.stats.gov.cn/english/PressRelease/202609/t20260901_1965170.html — mfg **49.8**, non-mfg **49.0**, construction **46.9**. Used for China rebound OFF / property stress HIT.
3. **ADMIS / mining.com.au / Finimize — Yangshan / SHFE** — https://www.admis.com/chinese-demand-is-back-for-copper/ ; https://mining.com.au/copper-price-rallies-as-chinese-buyers-return/ — Yangshan premium ~**$118–121/t**, SHFE stocks ~**54,780 t**. Used as physical restock, **not** a PMI rebound HIT.
4. **Kitco — Warsh / gold** — https://www.kitco.com/news/article/2026-09-16/gold-price-buckles-under-warshs-inflation-fixation-fed-hikes-rates — 09-16 hike to 3.75–4.00% and hawkish presser. Treated as **T+2 paid path**, not a same-morning shock; live Channel 1 gold is green.
5. **TradingView / TipRanks — XLB holdings** — https://www.tradingview.com/symbols/AMEX-XLB/holdings/ — LIN ~13%, NEM ~8%, FCX ~6%, SHW ~4.8%. Used for composition weighting.
6. **ETFDB / ETF Action — flows** — https://etfdb.com/etf/XLB ; https://www.etfaction.com/tech-sector-and-ultrashort-bonds-draw-steady-inflows/ — ~**−$127M to −$169M** 1m, ~**−$157M** 5d. Used for S3=0 / rotation-out PARTIAL.
7. **LSE / Finimize — Section 232 stall** — https://www.lse.co.uk/news/white-house-copper-tariff-plan-stalls-amid-affordability-concerns-sources-say-k10120hh9mrpre6.html — refined-copper tariff stalled. Used to keep critical-minerals HIT **OFF** and align with MAP HEAT FCX neg.
8. **Business Insider / Yahoo live — 09-18 futures** — https://markets.businessinsider.com/premarket ; https://finance.yahoo.com/markets/live/stock-market-today-friday-september-18-dow-sp-500-nasdaq-080504071.html — modestly higher futures after Thursday’s rally; VIX mid-teens. Cross-checked against Channel 1 (ES +1.14% / NQ +1.50% / Finviz four-index modest).
9. **Sunday Guardian / CNBC — oil 09-18** — https://www.cnbc.com/2026/09/18/oil-prices-today-brent-wti-saudi-arabia-houthi.html — Brent/WTI extending a multi-day decline despite residual Hormuz/Houthi headlines. Used to keep 8/18 **OFF**.
10. **X/MetalBenderHQ** — https://x.com/MetalBenderHQ/status/2100540923465629855 — Yangshan **$121/t**; LME builds concentrated in New Orleans while Asia/Europe stocks fell. Used as color on mixed inventory geography, not as an LME-draw HIT.

Channel 1 numbers (VIX, futures, metals, XLB/SPY relatives, XLB PM +0.51%) were **not** re-derived. Nested MAP HEAT was **not** averaged into the parent.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.5, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.85, 'leading_sum': 1.0, 'divergence_flagged': False, 'total_score': 6.18, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.55, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.6504, 'score': 3.902, 'legs': [{'leg': 'ES', 'pct': 1.14, 'w': 0.6}, {'leg': 'HG', 'pct': 0.66, 'w': 0.3}, {'leg': 'GC', 'pct': 0.9, 'w': 0.1}, {'leg': 'DX', 'pct': -0.02, 'w': -0.3}, {'leg': 'PM:XLB', 'pct': 0.51, 'w': 0.7}]}, 'overlay_score': 1.062, 'overlay_raw': 1.062, 'index_carry': 1.215, 'general_total': 4.861, 'skill_multipliers': {'S0_SHARED_MACRO': 1.25, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 0.5, 'S3_FLOWS_POSITIONING': 1.25, 'S4_ETF_TAPE': 0.0}, 'llm_confidence': 0.48, 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': -2.08, 'w1': -4.78}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
