# Sector Prediction — Basic Materials — 2026-09-15

- ETF: **XLB**
- rubric: `00_grounding/sectors/basic_materials.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-5.723** (mult 0.9)
- regime: risk_off
- divergence_flagged: **True**
- engine: v2 · tape_anchor **1.236** (ES +0.34%, HG -0.76%, GC -1.01%, DX +0.25%, PM:XLB +0.38%) · index_carry **-0.959** (general -3.836) · llm_overlay **-6.0** (raw -6.75)

## Channel 1 sector ETF tape

```
ETF XLB vs SPY (yfinance, through 2026-09-15):
  1d: XLB +0.24% | SPY -0.53% | rel +0.77%
  3d: XLB -0.30% | SPY -0.13% | rel -0.16%
  1w: XLB -2.56% | SPY -1.19% | rel -1.37%
  1m: XLB -3.67% | SPY -2.51% | rel -1.16%
```

I have full Channel 2 coverage. Writing the analysis.

---

MEMORY_CONFIRM: Sector Basic Materials / XLB only. Memory index unavailable this run (embedding metadata missing) — used injected scoreboard/lessons only. Rolling last-10 dir=0.5 mag=0.5 (n=10); last graded 2026-09-14 down/notable vs XLB −0.90% / SPY −0.45% / rel −0.46% (dir HIT, mag HIT). Active XLB rules checked: **09-14 S1-skill-multiplier lesson** — do not let a sub-1.0 multiplier haircut a rule-capped channel while anchor legs set the band; report both bands when component arithmetic and anchor total disagree >2×. **09-11 pre-binary-tape rule** — separate the unknowable binary (FOMC 09-16) from the knowable pre-binary tape; score a modest S0 lean in the futures direction rather than zeroing everything, and carry an explicit relative-underperformance lean when 1w/1m rel are persistently negative. **09-10 gap-at-open rule** — if |open vs prior close| ≥ 1.0%, mild is falsified at the bell (premarket XLB is **+0.38%**, so this rule does **not** fire). **09-09 composition/magnitude rule** — when the 8/18 metals-co-move floor ban fires with all four S1 sub-channels negative and zero offset, score S1=−2, S2=−1. **8/18 metals-as-floor ban** — do NOT use copper/gold as a floor on oil-shock risk-off days. **8/14 gold-offset** — score the monetary bid only if gold/silver are green (today they are NOT: GC −1.01%, SI −1.47%). **8/25 composition/transmission** — chemicals ~40–50% of XLB vs copper miners ~10–15%; NQ>>ES is not a materials green light (today NQ −0.62% ≈ ES −0.54%, no tech-led green light). **8/27 S4-cap** — 1d rel +0.77% is confirmation-eligible, but see the double-count caution below. **8/28 leftover-S2/S4 down-mandate** — S4 confirms only the session being predicted. **09-03 exhaustion-bounce** — sleeve-driven >+1% reversal within a negative 1w is a bounce, not an inflection. **09-04 T-1-lag** — do not copy a prior-day lag into S4 as fresh. No open experiment for `sector_basic_materials`. DO-INSTEAD: keep direction; shrink confidence on modest |score| when magnitude historically misses.

## Analysis — XLB, session of 2026-09-15

This is a **Tuesday risk-off open into an FOMC-eve stack**, with a **fresh same-morning China hard-data miss**, a **10Y yield breaking 5%**, and the metals complex in a **broad, deepening collapse**. It is not a copper-squeeze day and not a leftover-chemicals fade. Channel 1 tape through 09-14 is decisively negative across every horizon: 1d rel **+0.08%**, 3d **−0.85%**, 1w **−2.04%**, 1m **−1.82%**.

### 1. Shared macro as it hits materials (S0)

The dominant live driver is the **rates/duration shock into the FOMC binary**. Per Channel 2: the **10Y breached 5.03%** (highest since 2007), 30Y above 5.38%, on a global bond selloff; **CME hike odds for the 09-16 meeting are ~70–85%** depending on feed (goldsilver.com ~85–91% on Fed-funds futures; Pomegra cites 70%). Either way the market is pricing a **hike**, not a cut — the "gold surge on Fed rate-cut bets" line in the Finviz digest (B/Barrick +8.21%) is **stale/contradicted** framing; the live gold tape is **−1.01%** at a five-week low.

Live tape (Channel 1, do not re-derive): **ES −0.54%, NQ −0.62%, Russell −0.73%, DJIA −0.71%** — a **broad, uniform risk-off**, not a tech-led NQ>>ES rotation. **VIX 17.75 (+0.65 1d, +2.03 1w)** with **VIX/VIX3M 0.901** (contango — stress elevated but not backwardated). **DXY +0.25% to 99.36** — firm, not a spike. **DFII10 2.60 (+0.05 1d, +0.18 1w/1m)**, **DGS10 4.96**, **DGS30 5.35** — real yields grinding higher. **HY OAS 2.71 (+0.06 1d)** — still tight but widening. **Asia composite −1.1%** (Kospi −3.26% the outlier, Hang Seng −1.0%, Shanghai −0.54%); **Europe −0.39%**.

The materials-specific overlay is the **8/18 metals-co-move floor ban firing cleanly**: oil is **spiking** (WTI **$103.79 +2.37%**, Brent **$108.11 +2.31%**, heating oil +3.09%, gasoil +2.68%), and the **entire metals complex is co-moving DOWN with equities** — copper **−0.76%** to $6.359, gold **−1.01%**, silver **−1.47%**, platinum **−1.67%**, palladium **−1.85%**, aluminum **−0.03%**, iron ore **−0.48%**. This is risk-asset liquidation, not a hedge. Do **not** score S0 as risk_on because oil is up.

**S0 = −1.** Rates shock (10Y >5%) + hawkish FOMC-eve repricing + firm USD + rising real yields + oil-spike risk-off map negative to this cyclical. Not −2: ES is only −0.54%, DXY is firm not a spike, HY is tight, and the FOMC binary itself is unknowable — but the **pre-binary tape is knowable and it is red** (09-11 rule).

### 2. Spine + secondary (S1)

**Industrial metals — COLLAPSE, deepening.** Copper **−0.76%** to $6.359 (COMEX); per Channel 2 the LME has broken **below $14,000/t to a three-week low** (~$14,065 3M), now **−6.2% from last week's record high** ($14,533). The drivers are **two-fold and both negative**: (a) **LME inventories rising** — 242.9k mt as of 09-14, **+18.5% in 30 days** — which is a **supply-glut/inventory-rebuild HIT**, the direct inverse of the spine's "inventory draw"; (b) **US Section 232 refined-copper tariff uncertainty** (50% tariff on cathode/refined copper in force) rattling the market. Spine "surge" **off**; spine "collapse" is a **clean HIT**; spine "inventory draw" is **inverted to a rebuild**.

**China demand — FRESH SAME-MORNING HARD-DATA MISS.** This is the key new input today. NBS released August activity data at 10:00 Beijing (22:00 ET Monday, i.e. the Asia session that just closed): **retail sales +0.4% y/y vs 0.8% consensus** (missed, third consecutive decline, 11th percentile of 24-month range), **fixed-asset investment −7.2% y/y** (deepening contraction), **real-estate development investment −19.9%** (worse than July's −19.2%), while **industrial output +5.2%** beat. This is a **confirmed China demand shock / property stress HIT** — not a T-1 stale print, and not a rebound. Per the 8/17 rule, a same-morning China hard-data miss with a risk-off overlay is a **severe-ban trigger** for an up call; here it reinforces the down lean. **Do not let gold cancel this — and today gold is not even green.**

**Monetary metals — FADE.** GC **−1.01%** to $4,307.7 (five-week low), SI **−1.47%** to $63.205. 8/14 does **not** pay. The driver is the **rates repricing** (hike odds ~85–92%), not a mining story — MAP HEAT Gold breadth is **0.073, the worst on the board**.

**Oil-up is a cost headwind for processors, not an XLB squeeze.** Count Hormuz/oil once in S0 as the risk-off overlay; do not also credit copper as a positive floor (8/18). The chemicals-heavy book (LIN ~13%, SHW, ECL — ~40–50% combined) faces a direct oil feedstock/energy cost squeeze — **margin compression HIT**. MAP HEAT Chemicals is **flat/low-conviction** (merger-arb + macro-oil tape, breadth 0.188, no fresh company facts) — i.e. no pricing-power offset.

**Supply disruption / tariffs — mixed-to-negative.** Section 232 refined-copper tariff **uncertainty** is a live negative for the copper sleeve (BHP ADRs fell on copper retreating from records amid tariff uncertainty). DRC concentrate ban remains on the books but is stale. **Critical-minerals policy** is a partial positive for domestic producers but is not a same-open catalyst.

**MAP HEAT nested overrides (do not average into the parent):** Copper dir=**up** (FCX:pos, IE:pos, breadth 0.875, +5.09% d1, $1.1bn EXIM financing) and Aluminum dir=**up** (CENX/CSTM, +2.85% d1, entirely RUT names the ETF underweights) are **nested child-book longs** — they are **not** a license to lift the parent ETF, and the parent's own tape (copper −0.76% live) contradicts the nested read. Gold dir=**down** (breadth 0.073), Coking Coal dir=**down**, Building Materials dir=**down** (breadth 0.176, −2.33% d1), Other Industrial Metals dir=**down** (rare-earth gapped down on China summit threat). Net nested read is **negative-to-mixed**, with the two positives in names XLB underweights.

**S1 = −2.** Per the 09-09 lesson: all four sub-channels align negative (chemicals oil-cost drag + copper collapse + gold/silver fade + **fresh China hard-data miss**) with **zero offsetting positive** in the ETF's actual book. The "not a collapse" cap does **not** apply — copper is down ~6% from its record, LME stocks are rebuilding +18.5%/30d, and the minority sleeve that could have provided offset (copper miners) is itself fading on tariff uncertainty. The nested Copper/Aluminum "up" reads are RUT-heavy child books, not XLB weight.

### 3. Breadth (S2)

Per the 09-09 lesson, when the 8/18 metals-co-move pattern fires there is **no defensive pocket inside XLB** — chemicals, industrial metals, and gold miners all decline together. MAP HEAT confirms: Gold breadth **0.073**, Building Materials **0.176**, Chemicals **0.188**, Coking Coal **0.0** — uniformly weak. The only positive breadth pockets (Copper 0.875, Aluminum) are **nested child books in RUT names the ETF underweights**, which is precisely the composition trap the 09-08 lesson warns about. This is a **breadth failure**, not a compositional split.

**S2 = −1.**

### 4. Flows / positioning (S3)

XLB has carried persistent 1m net outflows (~−$180M range from prior logs) and a **1m rel of −1.82%** — a multi-week relative laggard. Not a washout capitulation, not a volume spike. The 09-14 lesson's "crowded-long unwind is bullish" reflex does **not** apply: there is no crowded long here — the sector has been de-risking for two weeks into a *tightening* macro overlay (hike odds rising), which is the opposite of the easing-overlay precondition. **S3 = 0.**

### 5. Tape (S4, confirmation only)

1d rel **+0.77%** (XLB +0.24% vs SPY −0.53%) is **confirmation-eligible** (>0.5%), but per the 09-08 double-count lesson, that relative strength is **attributable to the same nested copper/aluminum child-book bid already scored in S1** — and the parent's live premarket tape is **+0.38%** while copper is **−0.76%** and the whole complex is red. The 1d rel is a **stale prior-close print** (09-14 close), and per the 09-04 T-1-lag rule it must not be copied as fresh downside *or* fresh upside confirmation. Score it **0** to avoid double-counting the nested bid.

**S4 = 0.**

### Reconciliation and divergence

Leading-factor sum (S0 −1, S1 −2, S2 −1, S3 0) = **−4.0**, weighted by the engine's multipliers (S0 ×1.0, S1 ×0.5, S2 ×1.0, S3 ×1.25) → **−1.0 −1.0 −1.0 + 0 = −3.0**; with S4 = 0 and mult 0.9 → **−2.7 → down/mild**.

**Divergence flag: TRUE.** The engine's tape_anchor leg is **+1.193** (ES +0.30%, PM:XLB +0.38%) — i.e. the anchor is *positive* while every leading factor is negative, and the llm_overlay is −6.0. Per the 09-14 lesson, when the anchor leg and the component arithmetic disagree on sign/magnitude, **trust the factors over the tape** and report both bands. The anchor's positive sign is driven by the premarket XLB +0.38% print, which is itself the nested copper/aluminum child-book bid (RUT names XLB underweights) — not a parent-ETF signal. The honest read is **down/mild**, with the caveat that the premarket +0.38% is a real same-morning observation that caps conviction and could produce a flat-to-mildly-down open.

**Direction: down. Magnitude: mild.** Not notable: the premarket XLB print is **+0.38%** (no ≥1% gap, so the 09-10 gap rule does not fire), the FOMC binary is unknowable and caps the band, and the nested Copper/Aluminum child books are genuinely bid. Confidence reduced for the pending binary and the anchor/factor sign conflict.

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
Risk-off tape / flight to safety|HIT|0.85|2026-09-15|https://www.benzinga.com/etfs/sector-etfs/26/09/61789034/leading-and-lagging-sectors-september-15-2026
Real yields rising|HIT|0.80|2026-09-15|https://coinalertnews.com/news/2026/09/15/fed-rate-hike-odds-gold
USD strengthening|HIT|0.60|2026-09-15|https://www.vantagemarkets.com/en/market-news/gold-near-4300-oil-rate-hike-bets-september-15-2026/
Industrial metal price collapse|HIT|0.85|2026-09-15|https://www.riotimesonline.com/copper-markets-latam-tuesday-september-15-2026/
China demand shock / property stress|HIT|0.90|2026-09-15|https://www.cnbc.com/2026/09/15/china-august-retail-sales-industrial-output-investment-exports-.html
Supply glut / new capacity online|HIT|0.75|2026-09-15|https://thevaultreport.com/lme/copper
Gold/silver price surge (monetary metals)|MISS|0.85|2026-09-15|https://www.vantagemarkets.com/en/market-news/gold-near-4300-oil-rate-hike-bets-september-15-2026/
Inventory draw (LME/exchange stocks down)|MISS (inverted — stocks +18.5%/30d)|0.80|2026-09-15|https://thevaultreport.com/lme/copper
Margin compression / cost inflation without pricing power|HIT|0.65|2026-09-15|https://www.globalcitybullion.com/gold-and-silver-brace-for-crucial-2026-september-fed-decision/
Critical-minerals policy / domestic tariff support|PARTIAL|0.50|2026-09-15|https://discoveryalert.com/news/copper-prices-record-crash-tariff-september-2026/
Supply disruption (mine/export ban)|PARTIAL (stale)|0.40|2026-09-15|https://discoveryalert.com/news/lme-copper-record-tariff-arbitrage-september-2026/
Sector breadth failure (ETF up, names flat)|HIT|0.70|2026-09-15|https://www.benzinga.com/etfs/sector-etfs/26/09/61789034/leading-and-lagging-sectors-september-15-2026
Sector rotation out of materials|HIT|0.70|2026-09-15|https://www.benzinga.com/etfs/sector-etfs/26/09/61789034/leading-and-lagging-sectors-september-15-2026
Sector ETF outflow / volume dry-up|PARTIAL|0.45|2026-09-15|https://www.etf.com/etfanalytics/etf-fund-flows-tool
Industrial metal price surge (copper/aluminum/iron ore)|MISS (nested child-book only)|0.60|2026-09-15|https://tradingeconomics.com/commodity/copper
China PMI / property demand rebound|MISS|0.85|2026-09-15|https://www.cnbc.com/2026/09/15/china-august-retail-sales-industrial-output-investment-exports-.html
HORIZON_3D: down — China hard-data miss + LME inventory rebuild + FOMC hike repricing are multi-day drivers; nested copper/aluminum child books are the only offset and are RUT-weighted.
HORIZON_1W: down — 1w rel −2.04% and 1m rel −1.82% describe a persistent relative downtrend; no fresh positive catalyst in the ETF's actual book.
HORIZON_2W: down-to-flat — if the FOMC resolves dovish or the Section 232 refined-copper tariff decision lands as a domestic-producer positive, the copper sleeve could re-rate; watch LME stocks for a draw reversal.
HORIZON_1M: flat-to-down — 1m rel −1.82% with a supply-glut/inventory-rebuild regime and a confirmed China property contraction; a genuine turn requires either a China credit/property headline or an LME inventory draw.
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -2.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': -10.0, 'divergence_flagged': True, 'total_score': -5.723, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.629, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.206, 'score': 1.236, 'legs': [{'leg': 'ES', 'pct': 0.34, 'w': 0.6}, {'leg': 'HG', 'pct': -0.76, 'w': 0.3}, {'leg': 'GC', 'pct': -1.01, 'w': 0.1}, {'leg': 'DX', 'pct': 0.25, 'w': -0.3}, {'leg': 'PM:XLB', 'pct': 0.38, 'w': 0.7}]}, 'overlay_score': -6.0, 'overlay_raw': -6.75, 'index_carry': -0.959, 'general_total': -3.836, 'skill_multipliers': {'S0_SHARED_MACRO': 1.25, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 1.25, 'S4_ETF_TAPE': 0.0}, 'llm_confidence': 0.55, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
