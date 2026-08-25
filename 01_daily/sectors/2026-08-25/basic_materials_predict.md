# Sector Prediction — Basic Materials — 2026-08-25

- ETF: **XLB**
- rubric: `00_grounding/sectors/basic_materials.md`
- predicted_direction: **up**
- predicted_magnitude_band: **mild**
- total_score: **4.5** (mult 0.9)
- regime: risk_on
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLB vs SPY (yfinance, through 2026-08-24):
  1d: XLB +0.07% | SPY -0.29% | rel +0.37%
  3d: XLB +2.02% | SPY -0.73% | rel +2.75%
  1w: XLB +2.57% | SPY -1.19% | rel +3.76%
  1m: XLB +4.53% | SPY +3.32% | rel +1.20%
```

MEMORY_CONFIRM: Sector Basic Materials / XLB only. Scoreboard n=8 graded, dir=0.625 mag=0.375. Last graded 2026-08-21 was up/severe and HIT (+2.14% / rel +1.73%); 2026-08-24 up/mild is still ungraded. Active XLB rules checked: temper-severe does **not** fully fire (1d rel +0.37% is <0.5% and China PMI contraction is live, but there is **no** active Hormuz/oil>$90 risk-off tape — CL −3.06%, BZ −4.54%); 8/17 China-miss + flat-futures severe ban does **not** fire (ES +0.44% / NQ +0.92%; no same-morning China print; August NBS PMI ~31 Aug); 8/18 metals-co-move floor ban does **not** fire (oil is collapsing, not a supply squeeze); 8/14 gold-offset **does** apply (score the monetary-metals bid; do not ignore it); 8/21 “keep pipeline severe if no temper” does **not** apply — this is a post-run follow-through with a modest 1d tape, not a fresh squeeze day. No open experiment for `sector_basic_materials`. Memory index search was unavailable (embedding-index metadata missing); used the injected sector scoreboard/lessons only.

## Analysis — XLB, session of 2026-08-25

This is a **Tuesday follow-through** after Friday’s materials blowout and Monday’s already-mild tape, not a new copper-squeeze day. Channel 1 is still **directionally constructive** (1d rel **+0.37%**, 3d **+2.75%**, 1w **+3.76%**, 1m **+1.20%**), but the 1d print is **modest (<0.5%)**. That sets a **small up bias**. It does **not** set notable/severe: the 1w relative run is already traded, futures confirmation is tech-led rather than materials-led, and tomorrow’s PCE is the week’s load-bearing event.

### 1. Shared macro as it hits materials
Premarket is **risk-on, Nasdaq-led**: ES **+0.44%**, NQ **+0.92%**, Asia composite **+0.41%**, Europe **+0.41%**. VIX **15.77** (calm) but VIX/VIX3M just into **backwardation (1.002)** — a small caution, not a panic. Fear & Greed **55.9** (Greed). That is a **mild cyclical tailwind** for XLB’s industrial/chemical sleeve, not a materials-specific green light.

Offsets / two-sided items that map here:
- **Oil is down hard** (CL **−3.06%**, BZ **−4.54%**). News Judge is explicit: Iran sanctions are copy, not a Hormuz supply shock. The 8/18 “oil-shock liquidates the whole commodity complex” pattern is **off**. Falling crude is **cost-input relief** for chemicals/processors, not a metals squeeze.
- **Gold is the live cross-asset tell** (GC=F **+1.08%**, spot still near a 3-month high ~$4,630–$4,650). That is a materials-positive substitution/hedge, **not** an SPX risk-on mandate — and **gold does not cancel China**.
- **USD**: DXY **+0.01%** 1d vs **−2.46%** 1m. Not a USD spike vs the complex.
- **Real yields**: DFII10 **2.40, +0.05 1d** — a 1d uptick. Mild duration/gold headwind, not a materials collapse.
- **Calendar:** Today is **not** an 8:30 CPI/PPI/NFP day. Knowable prints: Case-Shiller / FHFA, New Home Sales, Conference Board Consumer Confidence (10:00 ET), Richmond Fed, Barkin. **July PCE is tomorrow (8/26 8:30 ET)**; Jackson Hole **8/27–29**. Encode PCE as **week-horizon event risk**, not “no macro print,” and do not score S0 negative merely because the print exists.

**S0 = +1.** Risk-on maps mildly positive to industrial metals; oil-down is not an XLB shock; gold/USD are supportive. Capped at +1 by the 1d real-yield uptick, VIX backwardation, NQ-over-ES leadership, and tomorrow’s two-sided PCE.

### 2. Spine + secondary (S1)
**Industrial metals — partial, not a surge day.** COMEX copper ~**$6.63–$6.64/lb** (+~0.5–0.6% vs prior). LME cash (24 Aug) **$14,344/t**, 3m **$14,245/t**. That is **firm/elevated**, not an 8/17-style record backwardation squeeze. Aluminum ~**$3,201/t** (soft on the day). Iron ore still ~**$95/t** on China property. Spine “surge” is **partial**.

**Inventory draw is OFF.** LME copper stocks **240,250t** (24 Aug) vs **238,575t** (21 Aug) and ~**205kt** mid-August — a **rebuild**, not a draw. Acute tightness has eased.

**China demand — still the industrial offset.** July NBS mfg PMI **49.2**, construction **47.0**. August official PMI not out until ~31 Aug. Property/F AI still draining traditional copper (wiring, construction). EV/grid/AI copper is the structural floor, not a same-morning rebound. **Do not let gold cancel this.**

**Supply disruption — stale/narrow.** DRC concentrate ban (order 29 Jun, reported 6 Aug) is still on the books. BMI/Fitch: concentrates are a small share vs DRC cathode; **not** a same-day shock.

**Monetary metals (different driver) — HIT.** Channel 1 gold **+1.08%**; silver still elevated ~$68. News Judge ranks the 3-month-high gold/miners bid as a primary materials input. Per 8/14: this is a **positive S1 offset**, not a dampener. It is **not** permission to emit severe from gold alone.

**Policy/tariffs — carried HIT.** Section 232 copper/semi-finished still in force; BIS comment window on extra derivatives runs through **27 Aug**. Not a fresh open catalyst.

**S1 = +1.** Gold + still-elevated copper, minus China/property, minus the inventory rebuild, minus stale DRC. Capped well below +2/+3 so gold cannot wash out the industrial breakdown.

### 3. Breadth
Monday’s 1d was ETF-only modest green. Premarket top weights are **mixed, not expanding**: LIN ~−0.3%, FCX ~flat, NEM ~−0.8%, SHW ~+1%. That is large-cap/chemicals vs miner split, not a fresh % names-up thrust. Friday’s FCX/NEM/AU leadership is **already in the 3d/1w tape** — scoring it again as today’s breadth would double-count the same reversal (the follow-through failure mode). APD’s Q3 beat/guide-up is **stale** (30 Jul), not a same-session catalyst.

**S2 = 0.**

### 4. Flows / positioning
XLB AUM ~$8.7B. Recent **5d ~−$94M**, **1m ~−$11M** despite the price run; 3m still +$1.1B so this is not a washout. 1w rel **+3.76%** is extended enough for **crowding risk**, not forced selling. No confirmed same-day inflow spike.

**S3 = 0.**

### 5. Tape (confirmation only)
Channel 1 1d rel **+0.37%** is a real **relative** beat vs SPY −0.29%, but it is **modest (<0.5%)**. Use it as weak directional confirmation, **not** a second magnitude engine. 3d/1w strength is Friday–Monday’s already-traded move.

**S4 = 0.** Leading sum (+1+1+0+0 = **+2**) and tape (**0**) **do not fight** — tape is just non-confirming on magnitude. No leading-vs-tape divergence. Trust the modest factor stack; do not let S4 or the 1w relative run upgrade the band.

### Self-audit
- **Lens:** XLB environment, not SPX, not FCX/NEM/AU single-name. Gold is a sector secondary factor, counted once in S1.
- **Band:** After a +3.8% 1w relative run, 1d rel <0.5%, copper tightness easing, China still contracting, PCE tomorrow → **mild/flat, not notable/severe.** Temper-severe’s *full* geo/oil clause is off, but the modest 1d tape still forbids building severe from structural metals.
- **Skew:** Gold ≠ industrial metals. Stated separately; gold does not cancel China.
- **Same-shock:** Iran/sanctions counted once in S0 as **non-squeeze** (oil down). Not re-used as an S1 supply HIT.
- **Single-ticker:** APD stale; SHW premarket not enough to drive the ETF; NEM fade is not a sector thesis.

Component arithmetic the pipeline must honor: **(+1)+(+1)+(0)+(0)+(0) = +2 × 0.9 = 1.8**. If a later block prints notable/severe from these same components, treat that as a tool mismatch and keep the call in the **mild/flat** neighborhood.

```
SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 1
S1_SECTOR_FACTORS: 1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.52
REGIME: risk_on
HORIZON_3D: flat:mild:0.46
HORIZON_1W: up:mild:0.44
HORIZON_2W: up:mild:0.45
HORIZON_1M: up:mild:0.50
SECTOR_SCORES_END
```

```
HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|HIT|0.72|2026-08-25|channel1 ES +0.44% / NQ +0.92%; Asia/Europe +0.41%
Risk-off tape / flight to safety|MISS|0.70|2026-08-25|oil down; futures green; VIX 15.77 — not a risk-off overlay
Real yields rising|HIT|0.68|2026-08-25|channel1 DFII10 2.40 +0.05 1d (1w/1m still slightly lower)
Real yields falling|MISS|0.65|2026-08-25|1d real yield uptick; not a decline day
USD strengthening|MISS|0.70|2026-08-25|channel1 DXY +0.01% 1d / −2.46% 1m
USD weakening|PARTIAL|0.62|2026-08-25|1m USD still weak; 1d flat — not a fresh impulse
Sector breadth expansion (% names up)|MISS|0.58|2026-08-25|premarket LIN/FCX/NEM mixed-to-red; SHW green only
Sector breadth failure (ETF up, names flat)|PARTIAL|0.55|2026-08-25|Mon 1d XLB only +0.07%; leadership not broad chemicals
Large-cap leadership inside sector|HIT|0.60|2026-08-25|LIN/NEM/FCX still the book; Friday miner leadership already traded
Small/mid leadership inside sector|MISS|0.55|2026-08-25|checked, nothing material
High-beta leadership inside sector|PARTIAL|0.58|2026-08-21|Friday FCX/NEM/AU rip is stale for Tuesday open
Low-beta leadership inside sector|MISS|0.55|2026-08-25|checked, nothing material
Sector ETF inflow / relative volume spike|MISS|0.66|2026-08-22|etfdb XLB 5d ~−$94M / 1m ~−$11M
Sector ETF outflow / volume dry-up|PARTIAL|0.60|2026-08-22|modest near-term outflow, not a washout (3m still +$1.1B)
Crowded long (extreme relative performance + valuation)|PARTIAL|0.58|2026-08-25|1w rel +3.76% extended; not a forced-unwind signal
Index rebalance / inclusion tailwind|MISS|0.50|2026-08-25|checked, nothing material
Index exclusion / forced selling|MISS|0.50|2026-08-25|checked, nothing material
Industrial metal price surge (copper/aluminum/iron ore)|PARTIAL|0.70|2026-08-25|COMEX Cu ~$6.63–6.64; Al soft; Fe ore ~$95
Gold/silver price surge (monetary metals)|HIT|0.78|2026-08-25|channel1 GC=F +1.08%; spot still near 3-month high
China PMI / property demand rebound|MISS|0.74|2026-08-25|July NBS 49.2 / construction 47.0; Aug PMI not out
Inventory draw (LME/exchange stocks down)|MISS|0.76|2026-08-24|westmetall LME Cu stocks 240,250t vs ~205kt mid-Aug (rebuild)
Supply disruption (mine/export ban)|PARTIAL|0.62|2026-08-06|DRC concentrate ban still on books; BMI: narrow vs cathode
Critical-minerals policy / domestic tariff support|HIT|0.64|2026-08-06|Section 232 Cu still live; BIS derivative comments due 8/27
Industrial metal price collapse|MISS|0.72|2026-08-25|copper firm/elevated, not collapsing
China demand shock / property stress|HIT|0.73|2026-08-25|StoneX/NBS: property still draining traditional Cu demand
USD spike vs commodity complex|MISS|0.70|2026-08-25|DXY flat 1d, weak 1m
Supply glut / new capacity online|PARTIAL|0.58|2026-08-24|LME deliveries rebuilt stocks; not a new-mine glut headline
Margin compression / cost inflation without pricing power|MISS|0.52|2026-08-25|checked, nothing material (APD print is 7/30 stale)
Sector rotation into materials|PARTIAL|0.60|2026-08-25|3d/1w rel strong; 1d only +0.37% — follow-through, not a new rotation day
Sector rotation out of materials|MISS|0.60|2026-08-25|relative tape still positive vs SPY
HIT_GRID_END
```

## RESEARCH APPENDIX

**Queries run**
- web_search: `copper price LME inventory August 25 2026` (freshness=day)
- web_search: `gold silver price today August 25 2026 miners` (day)
- web_search: `China PMI property copper demand August 2026` (week)
- web_search: `XLB materials ETF flows breadth leadership August 2026` (week)
- web_search: `Air Products APD earnings XLB materials August 25 2026` (week)
- web_search: `US economic calendar August 25 2026 PCE Jackson Hole` (week)
- web_search: `aluminum iron ore price LME August 25 2026` (day)
- web_search: `DRC Congo copper concentrate export ban August 2026` (week)
- web_search: `copper COMEX price today August 25 2026` (day)
- web_search: `critical minerals tariffs copper Section 232 August 2026` (week)
- web_search: `XLB premarket LIN FCX NEM SHW APD August 25 2026` (day)
- web_search: `gold price 3 month high August 25 2026 miners XLB` (day)
- web_fetch: `https://tradingeconomics.com/commodity/copper` (403)
- web_fetch: `https://www.westmetall.com/en/markdaten.php?action=table&field=LME_Cu_cash`
- x_search: `XLB materials copper gold miners premarket August 25 2026` (timed out)
- memory_search: Basic Materials XLB lessons (unavailable — index metadata missing)

**Key sources and facts used**
- Westmetall LME copper table — https://www.westmetall.com/en/markdaten.php?action=table&field=LME_Cu_cash — fetched 2026-08-25 ~12:36 UTC. Search synthesis: 24 Aug cash **$14,344/t**, 3m **$14,245/t**, stocks **240,250t** (up from 238,575t on 21 Aug / ~205kt mid-month).
- Trading Economics / COMEX recap — https://tradingeconomics.com/commodity/copper ; https://www.marketwatch.com/investing/future/hg00/charts — 25 Aug COMEX ~**$6.63–$6.64/lb**, modestly green.
- Kitco / gold trackers — https://www.kitco.com/charts/gold — 25 Aug spot ~**$4,630–$4,650**; Channel 1 GC=F **+1.08%** is the authoritative 1d print.
- NBS / CNBC / StoneX — July 2026 NBS mfg PMI **49.2**, construction **47.0**; property still suppressing traditional copper demand. August official PMI not yet released.
- ETFDB XLB — https://etfdb.com/etf/XLB/ — ~22 Aug: 5d **−$94M**, 1m **−$11M**, 3m **+$1.12B**, AUM ~$8.7B.
- Reuters DRC ban — https://www.reuters.com/world/africa/congo-bans-exports-copper-cobalt-concentrates-official-order-says-2026-08-06/ — concentrate export ban still in force; BMI: no material balance-sheet impact vs cathode.
- CRS / Federal Register Section 232 — copper semi-finished tariffs still live; 6 Aug BIS request for comments on extra derivatives (due 27 Aug).
- Air Products IR — https://www.airproducts.com/company/news-center/2026/07/0730-air-products-fiscal-2026-third-quarter-earnings — Q3 print **30 Jul**, not a 25 Aug catalyst.
- Scotiabank / BEA calendars — 25 Aug: Case-Shiller, New Home Sales, Consumer Confidence, Richmond Fed; **PCE 26 Aug 8:30 ET**; Jackson Hole **27–29 Aug**.
- Channel 1 panel (injected, unaltered) — VIX 15.77, DFII10 2.40 +0.05 1d, CL −3.06%, BZ −4.54%, GC +1.08%, DXY +0.01% / −2.46% 1m, ES +0.44%, NQ +0.92%, XLB 1d rel **+0.37%**.
- News Judge 2026-08-25 — PCE/Fed speech is the week’s dominant event; oil-down means Iran is **not** an 8/14 energy escalate; gold/miners bid is live for materials.

**Not used as fresh positives:** APD 7/30 beat; Friday AU/Barrick +8% rip; DRC ban as if new today; any XLB premarket +1.6% print that conflicted with mixed LIN/FCX/NEM quotes (treated as light-volume/unreliable).

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': 5.0, 'divergence_flagged': False, 'total_score': 4.5, 'predicted_direction': 'up', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.52, 'regime': 'risk_on'}
```
