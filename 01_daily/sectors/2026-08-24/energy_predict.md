# Sector Prediction — Energy — 2026-08-24

- ETF: **XLE**
- rubric: `00_grounding/sectors/energy.md`
- predicted_direction: **down**
- predicted_magnitude_band: **notable**
- total_score: **-10.8** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLE vs SPY (yfinance, through 2026-08-21):
  1d: XLE -0.17% | SPY +0.41% | rel -0.58%
  3d: XLE -0.06% | SPY -0.23% | rel +0.16%
  1w: XLE +2.79% | SPY -1.37% | rel +4.16%
  1m: XLE +7.17% | SPY +3.73% | rel +3.44%
```

MEMORY_CONFIRM: Energy/XLE n=8 graded, dir=0.625 mag=0.375; last graded 08-21 was up/notable vs XLE −0.17% (dir MISS). Applied: live-oil verify (08-11) — Channel 1 CL −1.75% / BZ −1.31% matches live WTI ~$85.3 (−~2%) and Brent ~$91–93 (−1.3–2%); 08-14 green-oil escalation does **not** fire; 08-12/08-13 stale-run + negative oil/inventory/demand spine **does** fire (1w rel +4.16% > +4%, 1d rel −0.58% not confirming); 08-21 overbought decoupling is context (last session already failed to transmit oil-up into XLE) but today the oil sign itself is down, so factors and tape agree. Open Energy experiment: keep direction, shrink confidence after magnitude misses. Memory index unavailable this run; used injected sector scoreboard/lessons only.

## Energy / XLE — 2026-08-24

This is an **oil-down, premium-not-transmitting** session, not a fresh Hormuz squeeze. Channel 1 and live quotes agree: crude is lower, XLE already lagged Friday, and the Iran tape is sanctions/positioning — not a same-morning supply shock that is lifting the barrel.

### Channel 2

**1. Shared macro as it hits energy.** Mild risk-off overlay: ES −0.18%, NQ −0.58%, Asia composite −1.17% (Kospi −3.12%), Europe −0.13%, VIX 15.91 (+0.78), gold +2.14%, EPU +60. USD +0.13% 1d is a small commodity headwind; 1m DXY still −2.5%. Real yields (DFII10 2.35) are flat 1d / slightly easier 1w — secondary vs oil. Dominant calendar is **two-sided and not energy-specific**: PCE Wed 8/26 and Warsh’s first Jackson Hole keynote Fri 8/28, with a hawkish-hold prior. News Judge is explicit: Iran sanctions are risk-off geopolitics **while oil is falling**, so the Hormuz-up Energy lessons do not fire.

**2. Spine (S1).**  
- **Crude down (live-verified):** WTI ~$85.30–85.40 vs prior ~$87.06; Brent off ~1.3–2% toward the low $90s. Same sign as CL=F −1.75% / BZ=F −1.31%. This is the load-bearing factor.  
- **Inventory build:** EIA week ending 8/14 +4.4M bbl to ~428.8M; prior week +17.4M. Next print **Wed 8/26**.  
- **OPEC+ adding barrels:** August +188 kb/d, fifth monthly restoration step.  
- **Demand destruction (carried official):** IEA Aug OMR −1.6 mb/d 2026 demand; OPEC also cut 2026 demand-growth to ~0.58 mb/d.  
- **Geo premium not transmitting:** Iran tanker blacklist / “toughest” sanctions / Bessent “economic D-Day” are live headlines, but the **oil sign is down** (profit-taking into the announcement, not a squeeze). Count this as premium fade / non-confirmation — **do not** also score it as a crude-surge HIT.  
- **Cracks still extreme** (3-2-1 ~$70; diesel cracks near records) — refiner offset only; dampen for whole XLE.  
- **Nat gas ~$2.83** — not a surge; N/A for the oil-weighted ETF.

**3. Breadth.** Premarket XLE ~$63.40 (−0.3% to −0.5%); XOM and CVX both red with the ETF. Not a mega-name carry. After Friday’s relative lag, leadership is stalling, not expanding.

**4. Flows / crowding.** XLE ~$42B AUM; ~−$243M 1m, +$92M 5d. Sector ETFs have been leaking after the Hormuz run. YTD leadership + 1w rel +4.16% = **crowded long / profit-taking**, not fresh inflow confirmation.

**5. Catalysts.** No fresh XLE-wide earnings. Same-day Iran sanctions announcement is **two-sided** (could re-tighten later) but is **not** an oil-up catalyst at the open. EIA + PCE midweek add event risk; they do not flip the current oil sign.

### Scoring logic

S0 is mildly negative for this cyclical: risk-off tape, firmer USD, duration/inflation event risk. Not −2 — VIX is still ~16 and this is not a panic liquidation.

S1 is the oil spine, netted once: crude down + inventory build + OPEC+ add + IEA demand cut, minus a damped crack offset. Cap the geo headline because oil is not confirming. **S1 = −2.** Do not triple-count the same oil print.

S2/S3/S4 all confirm the fade: weak internal tape, outflows/crowding after the run, 1d rel −0.58%. **No leading-vs-tape divergence** — Friday already showed oil-up / XLE-down; today oil and the ETF are aligned lower.

Magnitude stays **mild**, not notable: 08-13 showed a large prior relative run cushions **absolute** XLE even when oil falls; cracks still bid refiners; sanctions later today are two-sided. Multiplier **0.9**. Confidence **0.56** (Energy mag hit-rate 0.375; do not over-size).

Regime **mixed**: broad tape is soft, but the Energy call is the oil spine, not SPX beta.

---

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -2
S2_BREADTH: -1
S3_FLOWS_POSITIONING: -1
S4_ETF_TAPE: -1
MULTIPLIER: 0.9
CONFIDENCE: 0.56
REGIME: mixed
HORIZON_3D: down:mild:0.58
HORIZON_1W: down:mild:0.50
HORIZON_2W: flat:mild:0.45
HORIZON_1M: up:mild:0.48
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-off tape / flight to safety|HIT|0.70|2026-08-24|https://www.nytimes.com/2026/08/24/business/oil-prices-bonds-stocks.html
USD strengthening|HIT|0.55|2026-08-24|channel1 DXY +0.13% 1d
Real yields falling|MISS|0.40|2026-08-20|channel1 DFII10 2.35, 1d 0.0 / 1w -0.04
Crude oil price surge (WTI/Brent)|MISS|0.85|2026-08-24|https://www.investing.com/commodities/crude-oil
Crude price collapse|HIT|0.80|2026-08-24|https://www.investing.com/commodities/crude-oil
Geopolitical supply risk premium|PARTIAL|0.55|2026-08-24|https://www.reuters.com/world/middle-east/iran-warns-vessels-violating-hormuz-transit-rules-fines-detention-2026-08-24/
Inventory build|HIT|0.85|2026-08-19|https://www.eia.gov/petroleum/supply/weekly/
Inventory draw (EIA crude/products)|MISS|0.85|2026-08-19|https://www.eia.gov/petroleum/supply/weekly/
OPEC+ production increase / quota break|HIT|0.75|2026-08-24|https://www.enerdata.net/publications/daily-energy-news/opec-confirms-188-kbd-production-increase-august-2026.html
OPEC+ cut / supply discipline|MISS|0.75|2026-08-24|https://www.enerdata.net/publications/daily-energy-news/opec-confirms-188-kbd-production-increase-august-2026.html
Demand destruction (recession/China weak)|HIT|0.70|2026-08-12|https://www.iea.org/reports/oil-market-report-august-2026
Crack spread / refining margin expansion|HIT|0.70|2026-08-22|https://rbnenergy.com/market-data/3-2-1-crack-spread
Natural gas price surge|MISS|0.60|2026-08-24|https://tradingeconomics.com/commodity/natural-gas
Sector breadth failure (ETF up, names flat)|MISS|0.55|2026-08-24|premarket XLE/XOM/CVX all down together
Large-cap leadership inside sector|HIT|0.60|2026-08-24|XOM/CVX leading the ETF lower
Sector ETF outflow / volume dry-up|HIT|0.60|2026-08-20|https://etfdb.com/etf/XLE/
Crowded long (extreme relative performance + valuation)|HIT|0.65|2026-08-21|1w rel +4.16%; YTD leadership; 08-21 RSI-extension lesson
Sector rotation out of energy|HIT|0.65|2026-08-21|channel1 1d rel -0.58%
Sector rotation into energy|MISS|0.65|2026-08-21|channel1 1d rel -0.58%
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- WTI Brent crude oil price today August 24 2026
- EIA weekly crude oil inventory report August 2026
- OPEC+ production policy August 2026 oil supply
- Iran sanctions oil Hormuz tanker August 24 2026
- XLE energy ETF flows positioning August 2026
- crack spread refining margins August 2026
- natural gas price Henry Hub August 24 2026
- XLE premarket XOM CVX COP energy stocks August 24 2026
- oil prices fall Iran sanctions Jackson Hole Warsh August 24 2026
- IEA OPEC oil demand forecast August 2026
- Jackson Hole Warsh speech date August 2026 economic calendar PCE CPI
- x_search: oil WTI Brent XLE energy stocks down August 24 2026 Iran sanctions (timed out)
- web_fetch Yahoo oil-falls article (failed); Benzinga sector page (403)

**Sources and facts used**
- Investing.com / MarketWatch / Trading Economics (2026-08-24): WTI ~$85.30–85.40, ~−2% from ~$87.06; Brent ~$91–93, ~−1.5–2%. Used for live oil-sign verify.
- NYT / QZ / Yahoo Finance (2026-08-24): oil down into Bessent Iran-sanctions announcement and Warsh/JH week; profit-taking after a >5% weekly oil gain. Used to classify geo as non-transmitting, not a squeeze.
- Reuters (2026-08-24): Iran blacklists 45 tankers / Hormuz transit threats. Used as geo headline present but oil-down.
- EIA WPSR (released 2026-08-19, week ending 2026-08-14): crude +4.4M bbl; prior week +17.4M; next release 2026-08-26.
- Enerdata / World Oil: OPEC+ Aug +188 kb/d restoration.
- IEA OMR 2026-08-12: 2026 demand −1.6 mb/d; OPEC Aug MOMR demand-growth cut to ~580 kb/d.
- RBN / 24/7 Wall St: 3-2-1 crack ~$69.61 (8/22); diesel cracks near/above $100 earlier in the week. Refiner offset only.
- Trading Economics / Markets Insider: Henry Hub ~$2.83 — no gas surge.
- ETFDB / SSGA (~8/19–8/20): XLE 1m −$243M, 5d +$92M, AUM ~$41.7B.
- MarketWatch / World Energy News (2026-08-24 premarket): XLE ~$63.40 red; XOM/CVX also red.
- Kansas City Fed / BEA calendars: Warsh JH Fri 2026-08-28 10:00 ET; PCE Wed 2026-08-26 8:30 ET.
- Channel 1 (unaltered): CL −1.75%, BZ −1.31%, ES −0.18%, NQ −0.58%, DXY +0.13% 1d, VIX 15.91, XLE 1d/3d/1w/1m rel −0.58% / +0.16% / +4.16% / +3.44%.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -2.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': -1.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -10.0, 'divergence_flagged': False, 'total_score': -10.8, 'predicted_direction': 'down', 'predicted_magnitude_band': 'notable', 'confidence_score': 0.56, 'regime': 'mixed'}
```
