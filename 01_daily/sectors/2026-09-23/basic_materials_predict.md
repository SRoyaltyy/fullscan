# Sector Prediction — Basic Materials — 2026-09-23

- ETF: **XLB**
- rubric: `00_grounding/sectors/basic_materials.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **2.494** (mult 0.85)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **0.646** (ES +0.03%, HG +0.66%, GC +0.90%, DX -0.02%, PM:XLB +0.02%) · index_carry **0.573** (general 2.293) · llm_overlay **1.275** (raw 1.275)

## Channel 1 sector ETF tape

```
ETF XLB vs SPY (yfinance, through 2026-09-21):
  1d: XLB -0.10% | SPY +1.55% | rel -1.65%
  3d: XLB -0.84% | SPY +2.83% | rel -3.67%
  1w: XLB -1.09% | SPY +1.91% | rel -3.00%
  1m: XLB -4.73% | SPY +1.68% | rel -6.41%
```

MEMORY_CONFIRM: Sector Basic Materials / XLB only. Memory index unavailable this run (embedding metadata missing) — used injected scoreboard/lessons only. Rolling last-10 dir=0.3 mag=0.2 (n=10); last-30 dir=0.44 mag=0.4 (n=25); last graded 2026-09-22 down/mild vs XLB **+1.65%** / rel **+1.67%** (dir MISS, mag MISS). No open experiment for `sector_basic_materials`. Active XLB rules checked: **09-22 Cu-tightness vs flat-index is BINDING** — |ES|,|NQ| < 0.5% and copper continuation/cancelled-warrant tightness must not be signed **down**; pay the spine in S1; cap conviction not direction; leftover HEAT-down / 1d-rel hole is not a down mandate on a dead index. **09-18 HEAT-down→down/mild is OFF as a down trigger** — industrial spine is independently green/tight; ES/NQ are not a green lagging-cyclical tape. **09-21 RS-veto triad OFF** (PM:XLB **+0.02%** not red; ES/NQ not ≥ +1%); do not *create* a down overlay against a live spine. **09-17 residual-mild-up OFF** (needs ES/NQ ≥ +0.5% and a held green PM). **09-16 oil+gold cash-transmission haircut ON as process, OFF as a Cu ban** — FOMC is T+7 and printed. **09-15 nested-bid ON as process, OFF as copper-HEAT long** — do not invent a nested bid; do not zero-count Channel 1 HG. **09-11 four-index ≥ +0.5% OFF**. **09-10 gap-at-open OFF** (|PM| **0.02%**). **09-09 / 8/18 OFF** (oil offered, metals not co-moving down). **8/14 gold sleeve ON** (Finviz GC **+0.90%**, SI **+1.96%**) — not a book bid; **China/gold split ON**. **8/25 / 8/27** remain a **confirmed-up ban** / S4 conviction cap, not a signed-down mandate. **8/17 / commodity-vs-flat-futures:** cap **severe**, not direction. **09-04 / 8/28:** do not copy Monday’s **−1.65%** rel into S4. DO-INSTEAD (last three BM losses): when factor sign fights leftover tape/HEAT, **cut conviction; prefer flat/mild** — do **not** flip back to down. size_gate=True.

## Analysis — XLB, session of 2026-09-23

This is a **Wednesday digestion session after Tuesday’s copper-transmission bounce**, not a Hormuz liquidation and not a same-morning China print. Channel 1 tape through **2026-09-21** is still a deep relative hole (1d rel **−1.65%**, 3d **−3.67%**, 1w **−3.00%**, 1m **−6.41%**). That tape is **T-1 leftover**. Live this morning: **ES=F +0.03%**, **NQ=F −0.10%**, **PM:XLB +0.02%**. Repeating 09-22’s down/mild against a live tight copper spine on a dead index is the named miss.

### 1. Shared macro as it hits materials (S0)

Knowable this morning (Channel 1, do not re-derive):

- **No pending CPI/NFP/FOMC binary.** News Judge applied none of those gates. Warsh JH / gold −3% / hike-odds (News Judge #3) is a **paid path level** (09-16 hike already in last week’s close), not a same-open Chair shock. Finviz Barrick “gold surge on cut bets” is **stale vs live GC +0.90%** and vs GC=F **−0.57%** 1d.
- **Index tape is dead-flat, not a thrust.** Finviz four-index **fails** ≥ +0.5% (SPX +0.20 / NQ +0.41 / RTY +0.08 / DJIA +0.11). ES/NQ vs prior close are **inside ±0.1%**. Nasdaq record / chips / oil-cool (News Judge #1) is **T-1 leftover growth beta**. **8/25:** NQ leftover is not an XLB green light. **09-17 condition (a) is off.**
- **Oil offered, 8/18 OFF.** WTI **−1.59%** to $104.16, Brent **−1.02%**, CL=F **−4.97%** 1d. News Judge #4 (surprise crude build / DVN) is an energy-factor hit, not a metals squeeze. Hormuz remains a *level*; the live increment is down. Count feedstock relief in S1 with the **09-16 haircut**, not a second S0 plus.
- **USD / real yields.** Finviz USD **−0.02%**; DXY 1d **+0.38%** / 1m **+2.04%** — firm, **not** a spike vs the complex. DFII10 **2.62 (−0.06 1d)** is an elevated *level* with a modest dip. Live notes slightly bid (10Y **−0.03%**, 30Y **−0.06%**). 5-day 10Y–SPX corr **−0.79**.
- **VIX 14.21** with VIX/VIX3M **0.786 contango**. HY OAS **2.66** contained. Asia composite **+0.33%** but **Hang Seng −0.83% / Shanghai −0.34%** (no China impulse). Europe **−0.25%**.
- **8/17 copper-vs-risk-off cap:** futures are flat, not a true risk-off + same-morning China miss. Cap **severe**, not a flat/defensive mandate.

**S0 = 0.** Not +1: four-index off, ES/NQ flat, Europe red, China equities red, leftover AI is a funding rotation **away** from this cyclical. Not −1: oil offered, USD not spiking, no kinetic increment, no same-morning China miss, futures not red, VIX calm.

### 2. Spine + secondary (S1)

**Industrial metals — continuation/tightness HIT, not collapse.** Channel 1: copper **$6.489 (+0.66%)**, aluminum **+1.10%**, iron ore **−0.14%**, steel HRC **−0.16%**. Channel 2 (09-22 close, still the live physical story): LME 3M into **~$14,745–14,766/t**, within ~1% of the **$14,875** Sep-10 record; sixth straight up day; cash-3M **backwardation ~$62/t** (from an **$86/t contango** a week earlier). 09-23 London tape is only a **mild profit-take** (~−0.3% toward $14,700) against a firmer dollar — not a collapse HIT. Iron/steel are **not** in the surge; composition stays chemicals-heavy.

**Inventory — available-metal tightness HIT, not a total-tonnes glut.** LME cancelled warrants **122,150 t (48% of ~255,100 t on-warrant)** → **~133,725 t** actually available; SHFE stocks **−70% since early June**; Shanghai cathode **43,900 t** (lowest since 2023); Yangshan premium near **4-year highs**. Comex still bloated (**~696 kt**, tariff-distorted). **09-22 miss:** scoring headline LME tonnes as glut while cancelled warrants / SHFE draws were the live tightness. Do **not** repeat.

**China demand — still contraction, not a rebound; do not let copper or gold cancel it.** NBS mfg **49.8**, construction **46.9**, property FAI **~−19.9% YoY**, new-home prices still down. Physical restock into Mid-Autumn / National Day holidays is **spot tightness**, not a PMI/property rebound HIT. Hang Seng/Shanghai are **red**, so it is not a US-open China impulse. Carried industrial offset only.

**Monetary metals — 8/14 sleeve ON, book bid OFF.** Finviz gold **+0.90%**, silver **+1.96%**. GC=F **−0.57%** 1d and NEM premarket **~−1.8%** mean the cash miner is **not** confirming. NEM ~8% of XLB is a sleeve. Do **not** let gold cancel China.

**Tariffs / News Judge #7 — uncertainty, not a support HIT.** Section 232 **refined-copper** decision still stalled; BHP ADRs already digested the “off records on tariff uncertainty” line. That is a **miner-sleeve overhang**, not critical-minerals domestic support.

**Chemicals majority sleeve — oil-offered cost relief, haircut.** LIN ~12–13%, SHW/ECL in the 40–50% chemicals/process book. 09-16: do **not** pay oil-down + gold as cash-XLB. LIN **did** print **+1.62%** on 09-22, so some transmission happened; it is **not** a same-open chemicals thrust today.

**S1 = +1.** Net of spine tightness/continuation versus carried China/property + tariff stall + incomplete iron/steel + 8/25 composition. Not +2/+3: chemicals are the book, 8/17 caps severe on copper into flat futures, nested FCX/NEM PM is soft. Not 0/−1: that was yesterday’s miss — refusing to pay green Cu on a dead index.

### 3. Breadth (S2)

Injected MAP HEAT is still **majority-down** (Cu, Al, Au, ag-inputs, building materials, coking coal, other metals/precious; chemicals **flat**). Nested OVERRIDE says do not average those captains into the parent. **Live same-morning:** PM:XLB **+0.02%**, FCX premarket **~−1%**, NEM **~−1.8%**, LIN ~unchanged — digestion, not expansion.

Do **not** copy Monday HEAT as Wednesday’s book (09-22 error). Do **not** copy Tuesday’s FCX **+3.03%** / LIN **+1.62%** / XLB **+1.65%** into today’s S2 (T-1). No live %advance confirmation this morning.

**S2 = 0.** Not −1 (stale HEAT after a transmitted bounce). Not +1 (no same-morning expansion; nested miners fading).

### 4. Flows / positioning (S3)

XLB ~$8.1–8.2B. ETFdb-style prints: **5d ~−$53M**, **1m ~−$238M**, week of Sep-11 **~$167M** outflow. 1m rel **−6.41%** is **washout**, not a crowded long. No same-morning volume spike in Channel 1. Engine weight on S3 is already haircut.

**S3 = 0.**

### 5. ETF tape confirmation (S4)

Channel 1 through **09-21** only: 1d rel **−1.65%**. **8/27 / 09-04 / 8/28:** S4 confirms **this** session, not Monday’s hole and not Tuesday’s already-printed **+1.65%**. PM **+0.02%** is non-information (09-18: PM ∈ (0,1%) with nested names not holding is not a bid).

**S4 = 0.**

### Horizons (relative vs SPY, expression only)

- **3D:** leftover Channel 1 hole plus Tuesday bounce — mixed, not a new trend day.
- **1W:** still a relative laggard into a tech-led tape.
- **2W:** FOMC/Hormuz scar, partial copper repair — lagging cyclical.
- **1M:** structural underperformance (**−6.41%** rel) until chemicals participate with metals.

### Self-audit

- **Lens:** XLB environment, not SPX, not FCX/NEM single-name.
- **Band/conviction:** size_gate on; last-10 mag **0.2**; factor sum modest — **multiplier 0.85**, **confidence 0.42**. 8/25–8/27 forbid **confirmed-up**; 8/17 forbids **severe**.
- **Skew:** copper miners ~10–15% vs chemicals ~40–50% — S1 capped at **+1**.
- **Same-shock double-count:** oil once (S0 offered / S1 haircut relief, not two pluses). China once (S1 offset). Tightness in **S1 only**, not S4.
- **Divergence:** leading S0–S3 = **+1** vs S4 **0** and leftover 1d/1w/1m rel still deeply negative. **Flag it. Trust the live spine over leftover tape** (09-22). Do not let RS-veto/size_gate flatten a **down** that is not on this card, and do not manufacture down from leftover rel.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.85
CONFIDENCE: 0.42
REGIME: mixed
DIVERGENCE: 1
HORIZON_3D: mixed
HORIZON_1W: lag
HORIZON_2W: lag
HORIZON_1M: lag
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|OFF|0.55|2026-09-23|Channel 1 ES +0.03% / NQ -0.10%; Nasdaq record is T-1 leftover
Risk-off tape / flight to safety|OFF|0.70|2026-09-23|VIX 14.21 contango 0.786; oil offered; no kinetic increment
Real yields rising|OFF|0.60|2026-09-23|https://fred.stlouisfed.org/series/DFII10
Real yields falling|OFF|0.50|2026-09-23|DFII10 2.62 is an elevated level, 1d -0.06 only
USD strengthening|HIT|0.45|2026-09-23|Channel 1 DXY 1d +0.38% / 1m +2.04%; not a spike
USD weakening|OFF|0.70|2026-09-23|Finviz USD -0.02%; DXY firm
Sector breadth expansion (% names up)|OFF|0.55|2026-09-23|PM:XLB +0.02%; FCX/NEM PM soft
Sector breadth failure (ETF up, names flat)|OFF|0.60|2026-09-23|Parent not up this morning
Large-cap leadership inside sector|HIT|0.40|2026-09-23|LIN remains the weight; chemicals HEAT flat
Small/mid leadership inside sector|OFF|0.50|2026-09-23|checked, nothing material
High-beta leadership inside sector|OFF|0.50|2026-09-23|nested Cu/Al HEAT still listed down vs parent
Low-beta leadership inside sector|OFF|0.45|2026-09-23|not a defensive regime for this cyclical
Sector ETF inflow / relative volume spike|OFF|0.55|2026-09-23|https://etfdb.com/etf/XLB/
Sector ETF outflow / volume dry-up|HIT|0.50|2026-09-23|https://etfdb.com/etf/XLB/
Crowded long (extreme relative performance + valuation)|OFF|0.75|2026-09-23|1m rel -6.41% is washout not crowding
Index rebalance / inclusion tailwind|OFF|0.80|2026-09-23|checked, nothing material
Index exclusion / forced selling|OFF|0.80|2026-09-23|checked, nothing material
Industrial metal price surge (copper/aluminum/iron ore)|HIT|0.72|2026-09-23|https://www.mining.com/copper-price-closes-in-on-new-record-as-shanghai-london-warehouses-empty-out/
Gold/silver price surge (monetary metals)|HIT|0.50|2026-09-23|Channel 1 Gold +0.90% / Silver +1.96%; GC=F -0.57% and NEM PM fade dampen
China PMI / property demand rebound|OFF|0.80|2026-09-23|https://www.stats.gov.cn/sj/zxfbhjd/202608/t20260831_1965154.html
Inventory draw (LME/exchange stocks down)|HIT|0.70|2026-09-23|https://www.mining.com/copper-price-closes-in-on-new-record-as-shanghai-london-warehouses-empty-out/
Supply disruption (mine/export ban)|OFF|0.55|2026-09-23|Grasberg/Kamoa cuts are structural/stale, not same-open
Critical-minerals policy / domestic tariff support|OFF|0.65|2026-09-23|https://www.reuters.com/world/us/white-house-copper-tariff-plan-stalls-amid-affordability-concerns-sources-say-2026-09-10/
Industrial metal price collapse|OFF|0.80|2026-09-23|Channel 1 HG +0.66% / Al +1.10%; 09-23 LME only ~-0.3%
China demand shock / property stress|HIT|0.60|2026-09-23|carried NBS 49.8 / construction 46.9 / property FAI ~-19.9%; not a same-morning miss
USD spike vs commodity complex|OFF|0.70|2026-09-23|DXY +0.38% is firm not a spike
Supply glut / new capacity online|OFF|0.65|2026-09-23|headline LME tonnes elevated but cancelled warrants 48% / SHFE draws invert glut
Margin compression / cost inflation without pricing power|OFF|0.55|2026-09-23|oil offered is feedstock relief for chemicals
Sector rotation into materials|OFF|0.50|2026-09-23|09-22 bounce is T-1; this morning PM flat
Sector rotation out of materials|OFF|0.50|2026-09-23|do not copy Monday rel hole into today
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- memory_search: Basic Materials XLB sector prediction lessons copper China PMI; XLB copper inventory LME gold silver 2026-09 (index unavailable)
- web_search: copper price LME inventory stocks September 23 2026
- web_search: China PMI property copper demand September 2026
- web_search: XLB ETF premarket flows breadth copper gold silver September 23 2026
- web_search: gold silver price Fed Warsh copper tariff BHP September 2026
- web_search: copper rises sixth day China tightness cancelled warrants LME September 22 2026
- web_search: XLB holdings LIN NEM FCX SHW premarket September 23 2026
- web_search: Section 232 refined copper tariff White House September 2026
- web_search: XLB ETF inflows outflows positioning September 2026
- web_search: XLB constituents performance LIN NEM FCX SHW ECL NUE September 22 23 2026
- web_search: real yields TIPS 10 year DFII10 September 23 2026
- web_search: China property FAI home prices copper imports September 2026
- web_search: risk on Nasdaq record oil cools materials sector rotation September 23 2026
- x_search: Latest copper LME inventory cancelled warrants XLB materials tape September 23 2026 (from 2026-09-20 to 2026-09-23)
- web_fetch: https://www.mining.com/copper-price-closes-in-on-new-record-as-shanghai-london-warehouses-empty-out/
- web_fetch failed (403): brecorder 40440856; etfdb.com/etf/XLB/

**Key sources (title + URL + timestamp/facts taken)**

1. **Mining.com — “Copper price closes in on new record as Shanghai, London warehouses empty out”** — https://www.mining.com/copper-price-closes-in-on-new-record-as-shanghai-london-warehouses-empty-out/ — fetched 2026-09-23T11:22:31Z. Facts: sixth straight Cu up day; LME 3M ~$14,766/t vs $14,875 peak Sep-10; Comex Dec as high as $6.8710; SHFE stocks −70% since early June; Shanghai cathode 43,900 t (lowest since 2023); cash-3M backwardation $62/t; cancelled warrants 122,150 t = 48% of 255,100 t on-warrant, 133,725 t available; Comex 696,204 t; FCX +1.8% Tue morning in that piece; Grasberg + Kamoa ~600 kt supply hit.

2. **Bloomberg (via search) — copper sixth day on China tightness** — https://www.bloomberg.com/news/articles/2026-09-22/copper-rises-for-a-sixth-day-toward-record-on-china-tightness — 2026-09-22. Facts: China physical tightness, pre-holiday restock, SHFE/Yangshan support.

3. **Business Recorder / LME official (via search)** — https://www.brecorder.com/news/40440711 and https://www.brecorder.com/news/40440856 — 2026-09-23. Facts: LME cash ~$14,787–14,788; 3M ~$14,725–14,730; later ~$14,700 (−0.3%) on profit-taking / firmer dollar; stocks ~254.25–254.3 kt as of Sep-22.

4. **NBS China PMI** — https://www.stats.gov.cn/sj/zxfbhjd/202608/t20260831_1965154.html — August 2026 print (Sep PMI not out). Facts: NBS mfg 49.8; construction 46.9; not a rebound HIT.

5. **Reuters China August factory PMI** — https://www.reuters.com/world/asia-pacific/chinas-august-factory-activity-picks-up-demand-improves-pmi-shows-2026-09-01/ — Caixin/S&P 51.5 vs official contraction.

6. **Reuters — White House copper tariff plan stalls** — https://www.reuters.com/world/us/white-house-copper-tariff-plan-stalls-amid-affordability-concerns-sources-say-2026-09-10/ — mid-Sep 2026. Facts: refined-copper 232 not finalized; News Judge #7 / BHP overhang still live as uncertainty, not a support HIT.

7. **ETFdb XLB** — https://etfdb.com/etf/XLB/ — as of ~Sep-19 2026 via search. Facts: AUM ~$8.1–8.2B; 5d −$53M; 1m −$238M; 3m +$85M.

8. **Seeking Alpha weekly ETF flows** — https://seekingalpha.com/news/4643112-weekly-etfs-eight-of-11-sectors-record-outflows-financial-sector-leads-inflows — week ended ~Sep-11: XLB ~$167M outflow.

9. **FRED DFII10** — https://fred.stlouisfed.org/series/DFII10 — 2.62% as of 2026-09-21 (Channel 1 matches).

10. **TradeSmith / holdings tape** — https://tradesmith.com/stockdata/XLB:NYSE/historical-data — XLB Sep-22 close $50.53 **+1.65%**; some Sep-23 premarket prints ~$50.27 (not used as Channel 1; Channel 1 PM:XLB **+0.02%** trusted). FCX Sep-22 **$74.35 +3.03%**; LIN **$464.91 +1.62%**; NEM Sep-23 PM ~$125 (−1.8%).

11. **Business Standard — Nasdaq record, oil cools** — https://www.business-standard.com/markets/news/nasdaq-hits-record-high-as-oil-prices-fall-on-improved-west-asia-flows-126092300062_1.html — 2026-09-23. Facts: Nasdaq record on chips; oil cooling; rotation not uniform risk-on for cyclicals.

12. **X posts (cancelled warrants)** — https://x.com/Fantastic_MrSTX/status/2102398287903097226 , https://x.com/Traderlv1/status/2102436600592691672 — ~Sep-22: ~45–48% cancelled; available 133,725 t; contango-to-backwardation flip.

**Channel 1 numbers used as injected (not re-derived):** VIX 14.21; ES +0.03% / NQ −0.10%; XLB PM +0.02%; HG +0.66%; Al +1.10%; Au +0.90%; Ag +1.96%; WTI −1.59%; DXY +0.38% 1d; XLB vs SPY through 2026-09-21 1d/3d/1w/1m rel −1.65/−3.67/−3.00/−6.41%.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.85, 'leading_sum': 3.0, 'divergence_flagged': False, 'total_score': 2.494, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.55, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.1076, 'score': 0.646, 'legs': [{'leg': 'ES', 'pct': 0.03, 'w': 0.6}, {'leg': 'HG', 'pct': 0.66, 'w': 0.3}, {'leg': 'GC', 'pct': 0.9, 'w': 0.1}, {'leg': 'DX', 'pct': -0.02, 'w': -0.3}, {'leg': 'PM:XLB', 'pct': 0.02, 'w': 0.7}]}, 'overlay_score': 1.275, 'overlay_raw': 1.275, 'index_carry': 0.573, 'general_total': 2.293, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 0.5, 'S3_FLOWS_POSITIONING': 1.25, 'S4_ETF_TAPE': 0.0}, 'llm_confidence': 0.42, 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': -2.08, 'w1': -4.78}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
