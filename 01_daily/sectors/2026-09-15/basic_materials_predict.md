# Sector Prediction — Basic Materials — 2026-09-15

- ETF: **XLB**
- rubric: `00_grounding/sectors/basic_materials.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-5.904** (mult 0.9)
- regime: risk_off
- divergence_flagged: **True**
- engine: v2 · tape_anchor **1.193** (ES +0.30%, HG -0.76%, GC -1.01%, DX +0.25%, PM:XLB +0.38%) · index_carry **-1.097** (general -4.389) · llm_overlay **-6.0** (raw -6.75)

## Channel 1 sector ETF tape

```
ETF XLB vs SPY (yfinance, through 2026-09-15):
  1d: XLB -0.44% | SPY -0.52% | rel +0.08%
  3d: XLB -0.97% | SPY -0.12% | rel -0.85%
  1w: XLB -3.22% | SPY -1.18% | rel -2.04%
  1m: XLB -4.32% | SPY -2.50% | rel -1.82%
```

I have sufficient Channel 2 coverage. Writing the analysis.

---

MEMORY_CONFIRM: Sector Basic Materials / XLB only. Memory index unavailable this run (embedding metadata missing) — used injected scoreboard/lessons only. Rolling last-10 dir=0.5 mag=0.5 (n=10); last graded 2026-09-14 down/notable vs XLB −0.90% / SPY −0.45% / rel −0.46% (dir HIT, mag HIT). Active XLB rules checked: **09-14 S1-skill-multiplier lesson** — do not let a sub-1.0 multiplier haircut a rule-capped channel while anchor legs set the band; report both bands when component arithmetic and anchor total disagree >2×. **09-11 pre-binary-tape rule** — separate the unknowable binary (FOMC 09-16) from the knowable pre-binary tape; score a modest S0 lean in the futures direction rather than zeroing everything, and carry an explicit relative-underperformance lean when 1w/1m rel are persistently negative. **09-10 gap-at-open rule** — if |open vs prior close| ≥ 1.0%, mild is falsified at the bell (must check the XLB open; premarket XLB is **+0.38%**, so this rule does **not** fire). **09-09 composition/magnitude rule** — when the 8/18 metals-co-move floor ban fires with all four S1 sub-channels negative and zero offset, score S1=−2, S2=−1. **8/18 metals-as-floor ban** — do NOT use copper/gold as a floor on oil-shock risk-off days. **8/14 gold-offset** — score the monetary bid only if gold/silver are green (today they are NOT: GC −1.01%, SI −1.47%). **8/25 composition/transmission** — chemicals ~40–50% of XLB vs copper miners ~10–15%; NQ>>ES is not a materials green light (today NQ −0.62% ≈ ES −0.54%, so no tech-led green light either). **8/27 S4-cap** — 1d rel +0.08% <0.5% cannot be a ± confirmation. **8/28 leftover-S2/S4 down-mandate** — S4 confirms only the session being predicted. **09-03 exhaustion-bounce** — sleeve-driven >+1% reversal within a negative 1w is a bounce, not an inflection. **09-04 T-1-lag** — do not copy a prior-day lag into S4 as fresh. No open experiment for `sector_basic_materials`. DO-INSTEAD: keep direction; shrink confidence on modest |score| when magnitude historically misses.

## Analysis — XLB, session of 2026-09-15

This is a **Tuesday risk-off open into an FOMC-eve stack**, with a **fresh same-morning China hard-data miss**, a **10Y yield breaking 5%**, and the metals complex in a **broad, deepening collapse**. It is not a copper-squeeze day and not a leftover-chemicals fade. Channel 1 tape through 09-14 is decisively negative across every horizon: 1d rel **+0.08%**, 3d **−0.85%**, 1w **−2.04%**, 1m **−1.82%**.

### 1. Shared macro as it hits materials (S0)

The dominant live driver is the **rates/duration shock into the FOMC binary**. Per Channel 2: the **10Y breached 5.03%** (highest since 2007), 30Y above 5.38%, on a global bond selloff; **CME hike odds for the 09-16 meeting are ~70–85%** depending on feed (goldsilver.com ~85–91% on Fed-funds futures; Pomegra cites 70%). Either way the market is pricing a **hike**, not a cut — the "gold surge on Fed rate-cut bets" line in the Finviz digest (B/Barrick +8.21%) is **stale/contradicted** framing; the live gold tape is **−1.01%** at a five-week low.

Live tape (Channel 1, do not re-derive): **ES −0.54%, NQ −0.62%, Russell −0.73%, DJIA −0.71%** — a **broad, uniform risk-off**, not a tech-led NQ>>ES rotation. **VIX 17.75 (+0.65 1d, +2.03 1w)** with **VIX/VIX3M 0.901** (contango — stress elevated but not backwardated). **DXY +0.25% to 99.36** — firm, not a spike. **DFII10 2.60 (+0.05 1d, +0.18 1w/1m)**, **DGS10 4.96**, **DGS30 5.35** — real yields grinding higher. **HY OAS 2.71 (+0.06 1d)** — still tight but widening. **Asia composite −1.1%** (Kospi −3.26% the outlier, Hang Seng −1.0%, Shanghai −0.54%); **Europe −0.39%**.

The materials-specific overlay is the **8/18 metals-co-move floor ban firing cleanly**: oil is **spiking** (WTI **$103.79 +2.37%**, Brent **$108.11 +2.31%**, heating oil +3.09%, gasoil +2.68%), and the **entire metals complex is co-moving DOWN with equities** — copper **−0.76%** to $6.359, gold **−1.01%**, silver **−1.47%**, platinum **−1.67%**, palladium **−1.85%**, aluminum **−0.03%**, iron ore **−0.48%**. This is risk-asset liquidation, not a hedge. Do **not** score S0 as risk_on because oil is up.

**S0 = −1.** Rates shock (10Y >5%) + hawkish FOMC-eve repricing + firm USD + rising real yields + oil-spike risk-off map negative to this cyclical. Not −2: ES is only −0.54%, DXY is firm not a spike, HY is tight, and the FOMC binary itself is unknowable — but the **pre-binary tape is knowable and it is red** (09-11 rule).

### 2. Spine + secondary (S1)

**Industrial metals — COLLAPSE, deepening.** Copper **−0.76%** to $6.359 (COMEX); per Channel 2 the LME has broken **below $14,000/t to a three-week low** (~$14,065 3M), now **−6.2% from last week's record high** ($14,533). The drivers are **two-fold and both negative**: (a) **LME inventories rising** — 242.9k mt as of 09-14, **+18.5% in 30 days** — which is a **supply-glut/inventory-rebuild HIT**, the direct inverse of the spine's "inventory draw"; (b) **US Section 232 refined-copper tariff uncertainty** (50% tariff on cathode/refined copper in force) rattling the market. Spine "surge" **off**; spine "collapse" is a **clean HIT**; spine "inventory draw" is **inverted to a rebuild**.

**China demand — FRESH same-morning hard-data miss.** Per Channel 2, the NBS August activity data released **10:00 Beijing time today (09-15)** showed **retail sales missing**, **fixed-asset investment −7.2% YTD (deeper slump)**, **property investment −19.9%**, with industrial output the lone beat (+5.2% y/y). This is a **same-morning China demand shock / property stress HIT** — not T-1, not stale. This is the first session in weeks where the China channel is a *live* negative rather than a carried one.

**Monetary metals — FADE.** GC **−1.01%** to $4,307.7 (five-week low), SI **−1.47%** to $63.205. 8/14 does **not** pay. MAP HEAT Gold dir=down, breadth **0.073** — the worst on the board, driven by a rates repricing, not a mining story. NEM neg.

**Oil-up is a cost headwind for processors, not an XLB squeeze.** Count Hormuz/oil once in S0 as the risk-off overlay; do not also credit copper as a positive floor (8/18). The chemicals-heavy book (LIN ~13%, SHW, ECL — ~40–50% combined) faces a direct oil feedstock/energy cost squeeze at $104 WTI. MAP HEAT Chemicals dir=flat conv=low, breadth 0.188 — "a merger-arb and macro-oil tape, not a demand tape."

**Supply disruption / tariffs — mixed-to-negative.** The DRC concentrate ban remains on the books, but the live tariff story is **Section 232 refined-copper tariff uncertainty suppressing US copper demand** — a demand-side negative, not a supply-side positive. APD's Q3 beat/raise is **already traded** and carries a $2.9B clean-energy exit charge — a single-name positive, not an XLB-wide thrust.

**S1 = −2.** Per the 09-09 lesson: all four sub-channels align negative (chemicals oil-cost drag + copper collapse with inventory rebuild + gold/silver fade + **fresh same-morning China miss**) with **zero offsetting positive** anywhere in the book. The "not a collapse" cap does **not** apply — copper is −6.2% off record with LME stocks +18.5% in 30 days, and the minority sleeve that could have provided offset (gold miners) is also negative.

### 3. Breadth (S2)

Per the 09-09 lesson, when the 8/18 metals-co-move pattern fires there is **no defensive pocket inside XLB** — chemicals, industrial metals, and gold miners all decline together. MAP HEAT confirms uniform weakness: Building Materials dir=down (breadth 0.176), Coking Coal dir=down (breadth 0.0), Gold dir=down (breadth 0.073), Other Industrial Metals dir=down, Lumber dir=down (breadth 0.167). The only positives are **nested child books the parent ETF underweights**: Aluminum OVERRIDE (+2.85% d1, entirely RUT names), Copper HEAT dir=up (FCX/IE, +5.09% d1 — but this is a *nested* read that conflicts with the live COMEX/LME collapse and the parent's own tape), and Other Precious Metals juniors. Per the MAP HEAT instruction, **do not average nested overrides into the parent ETF**.

**S2 = −1.** Uniform breadth failure across the parent's actual sleeves; the positives are nested child books, not XLB-wide.

### 4. Flows / positioning (S3)

XLB carries a persistent multi-week relative lag (1w rel −2.04%, 1m rel −1.82%) with no washout volume spike. No fresh flow print available this run. Not a crowded long (the sector has been *underperforming*), not a forced-selling event. **S3 = 0.**

### 5. Tape (S4, confirmation only)

1d rel **+0.08%** — sub-0.5%, so per the 8/27 S4-cap it **cannot** be a ± confirmation. The prior-day −0.46% rel is T-1 and per the 09-04 rule must not be copied in as fresh. **S4 = 0.**

### Reconciliation and band

Component arithmetic: S0 −1, S1 −2, S2 −1, S3 0, S4 0 → **−4.0** × mult 0.9 = **−3.6 → down/mild**.

**Divergence flag: TRUE.** The engine's tape_anchor leg is **+2.993** (ES +0.80%, HG −0.76%, GC −1.01%, DX +0.25%, PMIXLB +0.68%) — the anchor is reading the *premarket* XLB +0.38% and ES green as a positive, which fights the entire leading-factor sum. Per the shared method, **trust the factors over the tape**: the premarket XLB +0.38% is a gap-attempt into a red futures tape (ES −0.54% per the live Finviz futures line, which conflicts with the anchor's ES +0.80% — the anchor is stale). The 09-14 lesson also warns that anchor legs mechanically set the band; here the anchor is *positive* while the factors are decisively negative, so I explicitly do **not** let it lift the band.

**Band: mild.** The 09-10 gap-at-open rule does **not** fire (XLB premarket +0.38%, not a ≥1% gap). Rolling-magnitude discipline (last-10 mag=0.5) plus the modest |score| and the two-sided FOMC binary favor **mild** over notable. Note the honest tension: the last two sessions both resolved at notable magnitude (−1.06%, −1.23%) on similar setups, and the 09-10 lesson warns against shrinking the band against a tape that has already printed the larger move — but today's premarket is *green*, not gapped down, so the falsification condition is absent. I hold **mild** with reduced confidence.

**Direction: down.** The leading-factor sum is decisively negative (fresh China miss + copper collapse + inventory rebuild + gold fade + oil cost squeeze + rates shock), and the only counter-evidence is a stale/conflicting anchor and a sub-0.5% 1d rel that cannot confirm either way.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -2
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: risk_off
DIVERGENCE_FLAGGED: true
PREDICTED_DIRECTION: down
PREDICTED_MAGNITUDE_BAND: mild
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-off tape / flight to safety|HIT|0.85|2026-09-15|https://www.tipranks.com/news/stock-market-today-september-15-futures-slip-as-treasury-yields-cross-5-rate-hike-looms
Real yields rising|HIT|0.80|2026-09-15|https://propfirmscan.com/news/us-10-year-yield-tops-5-rattling-global-stocks
USD strengthening|HIT|0.60|2026-09-15|https://www.vantagemarkets.com/market-analysis/us10y-treasury-yield-tops-5-percent-september-2026/
Industrial metal price collapse|HIT|0.85|2026-09-15|https://www.brecorder.com/news/40439432/copper-sinks-to-3-week-low-as-dollar-firms-lme-stocks-rise
China demand shock / property stress|HIT|0.80|2026-09-15|https://www.cnbc.com/2026/09/15/china-august-retail-sales-industrial-output-investment-exports-.html
Supply glut / new capacity online|HIT|0.70|2026-09-15|https://thevaultreport.com/lme/copper
Gold/silver price surge (monetary metals)|MISS|0.85|2026-09-15|https://www.vantagemarkets.com/en/market-news/gold-near-4300-oil-rate-hike-bets-september-15-2026/
Inventory draw (LME/exchange stocks down)|MISS (inverted — rebuild)|0.75|2026-09-15|https://thevaultreport.com/lme/copper
Margin compression / cost inflation without pricing power|HIT|0.60|2026-09-15|https://www.tipranks.com/news/stock-market-today-september-15-futures-slip-as-treasury-yields-cross-5-rate-hike-looms
Sector breadth failure (ETF up, names flat)|HIT|0.65|2026-09-15|https://investinglive.com/stocks/stock-market-rotation-crowded-tech-improving-materials/
Sector rotation out of materials|HIT|0.60|2026-09-15|https://investinglive.com/stocks/stock-market-rotation-crowded-tech-improving-materials/
Critical-minerals policy / domestic tariff support|MIXED (Section 232 refined-copper tariff uncertainty = demand-side negative)|0.55|2026-09-15|https://www.tariffstool.com/guides/adjust-imports-of-copper-into-the-us
Industrial metal price surge (copper/aluminum/iron ore)|MISS|0.80|2026-09-15|https://www.indiaipo.in/news/detail/copper-price-hits-three-week-low-as-inventories-rise-and-dollar-strengthens
China PMI / property demand rebound|MISS|0.85|2026-09-15|https://www.reuters.com/world/china/chinas-factory-output-growth-quickens-in-august-retail-sales-slow-2026-09-15/
Sector ETF inflow / relative volume spike|checked, nothing material|0.40|2026-09-15|
Crowded long (extreme relative performance + valuation)|checked, nothing material (sector is a relative laggard, not crowded long)|0.50|2026-09-15|
Index rebalance / inclusion tailwind|checked, nothing material|0.40|2026-09-15|
Supply disruption (mine/export ban)|checked, nothing material (DRC ban stale; live tariff story is demand-side)|0.50|2026-09-15|
HIT_GRID_END

HORIZON_3D: down — the rates shock + fresh China miss + copper inventory rebuild are multi-session drivers; the FOMC print (09-16) is the swing factor and could produce a relief bounce if the hike is priced-and-done, but the sector's own spine (China property −19.9%, LME stocks +18.5%/30d) stays negative regardless.
HORIZON_1W: down — 1w rel is already −2.04% and the fundamental spine (China demand, copper inventory rebuild, oil cost squeeze on chemicals) has no positive offset; expect continued relative underperformance vs SPY.
HORIZON_2W: down-to-flat — mean-reversion risk rises after a −6.2% copper drawdown and a 1m rel of −1.82%; a post-FOMC relief rally in oversold miners is plausible, but the China/property channel caps the upside.
HORIZON_1M: flat-to-down — the sector is a deep multi-horizon relative laggard (1m rel −1.82%) with a deteriorating fundamental spine; without a China stimulus turn or a copper inventory draw, the path of least resistance remains lower, but valuation/positioning washout argues against extrapolating the decline.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -2.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': -10.0, 'divergence_flagged': True, 'total_score': -5.904, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.636, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.1988, 'score': 1.193, 'legs': [{'leg': 'ES', 'pct': 0.3, 'w': 0.6}, {'leg': 'HG', 'pct': -0.76, 'w': 0.3}, {'leg': 'GC', 'pct': -1.01, 'w': 0.1}, {'leg': 'DX', 'pct': 0.25, 'w': -0.3}, {'leg': 'PM:XLB', 'pct': 0.38, 'w': 0.7}]}, 'overlay_score': -6.0, 'overlay_raw': -6.75, 'index_carry': -1.097, 'general_total': -4.389, 'skill_multipliers': {'S0_SHARED_MACRO': 1.25, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 1.25, 'S4_ETF_TAPE': 0.0}, 'llm_confidence': 0.55, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
