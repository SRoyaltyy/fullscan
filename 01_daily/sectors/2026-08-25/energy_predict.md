# Sector Prediction — Energy — 2026-08-25

- ETF: **XLE**
- rubric: `00_grounding/sectors/energy.md`
- predicted_direction: **down**
- predicted_magnitude_band: **notable**
- total_score: **-10.0** (mult 1.0)
- regime: mixed
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLE vs SPY (yfinance, through 2026-08-24):
  1d: XLE -0.83% | SPY -0.29% | rel -0.54%
  3d: XLE -0.74% | SPY -0.73% | rel -0.01%
  1w: XLE +0.85% | SPY -1.19% | rel +2.04%
  1m: XLE +5.85% | SPY +3.32% | rel +2.53%
```

MEMORY_CONFIRM: Energy/XLE n=8 graded, dir=0.625 mag=0.375; last graded 08-21 up/notable vs XLE −0.17% (dir MISS); 08-24 down/notable still ungraded. Applied: 08-11 live-oil verify — Channel 1 CL −3.06% / BZ −4.54% matches live WTI ~$82.35 (−3.13%) and Brent ~$88–89.5 (−~2.5–3.5%); 08-14 green-oil escalation does **not** fire; 08-12 stale-run cap does **not** fully fire (1w rel +2.04% < +4%) but 1d rel −0.54% plus premarket XLE ~−1.33% **do** confirm the oil-down spine; 08-21 oil-up/XLE-down decoupling is inverted today (oil and the ETF are aligned lower). Open Energy experiment: keep direction, shrink confidence after mag misses. Memory index unavailable this run; used injected sector scoreboard/lessons only.

## Energy / XLE — 2026-08-25

This is an **oil-down, premium-not-transmitting** session against a **risk-on equity tape**. Crude is the load-bearing spine. Iran/Hormuz copy is still in the news, but the barrel is falling and XLE is already lagging — do not run the 08-14 squeeze playbook.

### Channel 2

**1. Shared macro as it hits energy.** Broad tape is risk-on, not energy’s friend today: ES +0.44%, NQ +0.92%, Asia +0.41%, Europe +0.41%, VIX 15.77 (−0.08), Fear & Greed 55.9. That is tech/beta leadership, not a commodity bid. DXY 1d +0.01% (flat; 1m still −2.46%). DFII10 2.40, +0.05 1d — secondary vs oil. Gold +1.08% is a monetary-metal bid, not an XLE tailwind. Dominant calendar is **tomorrow**: July PCE + EIA weekly (week ending 8/21) on Wed 8/26. News Judge is explicit: Iran sanctions are live in copy **while oil is falling**, so Hormuz-up Energy lessons do not fire. For this cyclical, risk-on is only a mild offset; it does not flip the oil spine. **S0 muted at 0.**

**2. Spine (S1).** Count the oil shock **once**.
- **Crude collapse (live-verified):** CL=F −3.06%, BZ=F −4.54%; Yahoo CL Oct ’26 $82.35 (−3.13%). WTI ~$82–83, Brent ~$88–89.5. Same sign as Channel 1 — 08-11 check passes.
- **Inventory build (carried):** EIA week ending 8/14 +4.4M bbl to 428.8M (third consecutive build; prior week +17.4M). Next print **Wed 8/26**.
- **OPEC+ adding barrels:** Aug +188 kb/d; Sep another ~188 kb/d completing the 2023 voluntary-cut rollback.
- **Demand destruction (official, carried):** IEA Aug OMR 2026 demand **−1.6 mb/d**; OPEC trimmed 2026 growth to ~0.6 mb/d.
- **Geo premium not transmitting:** Iran blacklisted 45 tankers; Hormuz outbound transits reported at zero on 8/24. That is **not** an oil-up HIT today. News Judge: tape brushed aside Bessent/“economic D-Day.” Score as fade/non-confirmation — **do not** also score Crude oil price surge.
- **Cracks still extreme** (diesel crack ~$100 mid-Aug; 3-2-1 still elevated) — **refiner offset only**; dampen for whole XLE. Do not let VLO/MPC carry the ETF call while crude is breaking down.
- **Nat gas ~$2.72–2.74**, down ~2% — no surge; N/A for oil-weighted XLE.

Net S1 = **−2**. Not −3: same oil print is not triple-counted; cracks still cushion refiners; geo can re-tighten later.

**3. Breadth.** Premarket XLE **62.29 (−1.33%)**. XOM ~−1.5%, CVX ~−1.0%, COP ~−1.4% — large-caps down with the ETF, not mega-name carry. 1d rel −0.54%, 3d rel ~0. Leadership stalling, not expanding.

**4. Flows / crowding.** Energy-sector ETFs ~$4B outflows over ~65 days (largest streak since mid-2025). XLE AUM ~$39B. YTD ~+41–44% is still a **crowded long** after the Hormuz run; 1w rel +2.04% is leftover leadership, not fresh inflow. Rotation today is **out of energy** into NQ/risk-on.

**5. Catalysts.** No fresh XLE-wide earnings. Same-day Iran/Hormuz headlines are **two-sided later**, **not** an oil-up catalyst at the open. PCE + EIA tomorrow add event risk; they do not flip the current oil sign.

### Scoring logic

S0 is **0**: risk-on equity beta is a mild cyclical plus, but energy is being sold as oil drops while Nasdaq leads. Not −1 (that was 08-24’s soft futures tape). Not +1 (would fight the oil spine).

S1 is the oil spine, netted once: crude down + inventory build + OPEC+ add + IEA demand cut, minus a damped crack offset. Cap the geo headline because oil is not confirming. **S1 = −2.**

S2/S3/S4 all confirm the fade: internals red across XOM/CVX/COP, outflows/crowding after the run, 1d rel −0.54% and premarket −1.33%. **No leading-vs-tape divergence** — 08-21 was oil-up / XLE-down; today oil and the ETF are aligned lower. Trust factors.

Magnitude: oil drop is larger than 08-24’s −1.7%, and the 1w relative cushion is only +2.04% (not the +6% 08-13 sponge). Premarket already ~−1.3% is transmission, not a stale tape. Still: cracks bid refiners; PCE/EIA tomorrow are two-sided; Energy mag hit-rate 0.375. Multiplier **1.0**. Confidence **0.58** (keep direction, shrink conviction per the Energy experiment).

Regime **mixed**: SPX beta is risk-on; the Energy call is the oil spine, not SPX.

Self-audit: lens = XLE/oil, not SPX; no same-shock triple-count; refiners not allowed to drive the ETF; no single-ticker call.

---

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: -2
S2_BREADTH: -1
S3_FLOWS_POSITIONING: -1
S4_ETF_TAPE: -1
MULTIPLIER: 1.0
CONFIDENCE: 0.58
REGIME: mixed
HORIZON_3D: down:mild:0.55
HORIZON_1W: down:mild:0.52
HORIZON_2W: flat:mild:0.48
HORIZON_1M: up:mild:0.50
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|HIT|0.75|2026-08-25|channel1 ES +0.44% / NQ +0.92%
Risk-off tape / flight to safety|MISS|0.70|2026-08-25|channel1 VIX 15.77, F&G 55.9
Real yields rising|PARTIAL|0.55|2026-08-21|channel1 DFII10 2.40, 1d +0.05
Real yields falling|MISS|0.50|2026-08-21|channel1 DFII10 1d +0.05 / 1w -0.01
USD strengthening|MISS|0.60|2026-08-25|channel1 DXY 1d +0.01%
USD weakening|PARTIAL|0.55|2026-08-25|channel1 DXY 1m -2.46%, 1d flat
Sector breadth expansion (% names up)|MISS|0.70|2026-08-25|https://finance.yahoo.com/quote/XLE
Sector breadth failure (ETF up, names flat)|MISS|0.65|2026-08-25|https://finance.yahoo.com/quote/XLE
Large-cap leadership inside sector|HIT|0.70|2026-08-25|https://finance.yahoo.com/quote/XOM
Small/mid leadership inside sector|MISS|0.40|2026-08-25|checked, nothing material
High-beta leadership inside sector|MISS|0.45|2026-08-25|checked, nothing material
Low-beta leadership inside sector|MISS|0.40|2026-08-25|checked, nothing material
Sector ETF inflow / relative volume spike|MISS|0.55|2026-08-25|https://cryptobriefing.com/us-energy-etf-outflows-4-billion/
Sector ETF outflow / volume dry-up|HIT|0.60|2026-08-25|https://cryptobriefing.com/us-energy-etf-outflows-4-billion/
Crowded long (extreme relative performance + valuation)|HIT|0.65|2026-08-25|https://finance.yahoo.com/quote/XLE
Index rebalance / inclusion tailwind|MISS|0.30|2026-08-25|checked, nothing material
Index exclusion / forced selling|MISS|0.30|2026-08-25|checked, nothing material
Crude oil price surge (WTI/Brent)|MISS|0.90|2026-08-25|https://finance.yahoo.com/quote/CL%3DF/
Natural gas price surge|MISS|0.75|2026-08-25|https://tradingeconomics.com/commodity/natural-gas
Inventory draw (EIA crude/products)|MISS|0.85|2026-08-19|https://www.eia.gov/petroleum/supply/weekly/
OPEC+ cut / supply discipline|MISS|0.80|2026-08-02|https://www.opec.org/pr-detail/1854611-2-august-2026.html
Crack spread / refining margin expansion|HIT|0.70|2026-08-17|https://www.reuters.com/business/energy/us-diesel-crack-surpasses-100-barrel-first-time-supply-disruptions-2026-08-17/
Geopolitical supply risk premium|PARTIAL|0.55|2026-08-24|https://www.reuters.com/world/middle-east/iran-warns-vessels-violating-hormuz-transit-rules-fines-detention-2026-08-24/
Crude price collapse|HIT|0.90|2026-08-25|https://finance.yahoo.com/quote/CL%3DF/
OPEC+ production increase / quota break|HIT|0.80|2026-08-02|https://www.opec.org/pr-detail/1854611-2-august-2026.html
Demand destruction (recession/China weak)|HIT|0.70|2026-08-13|https://www.iea.org/reports/oil-market-report-august-2026
Inventory build|HIT|0.85|2026-08-19|https://www.eia.gov/petroleum/supply/weekly/
Crack spread collapse|MISS|0.65|2026-08-17|https://www.reuters.com/business/energy/us-diesel-crack-surpasses-100-barrel-first-time-supply-disruptions-2026-08-17/
Sector rotation into energy|MISS|0.65|2026-08-25|channel1 XLE 1d rel -0.54%, NQ +0.92%
Sector rotation out of energy|HIT|0.70|2026-08-25|channel1 XLE 1d rel -0.54%, NQ +0.92%
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- WTI Brent crude oil price today August 25 2026
- EIA weekly petroleum status report crude inventories August 2026
- OPEC+ production policy August 2026 oil supply
- XLE energy ETF flows inflows outflows August 2026
- Iran Hormuz oil sanctions tanker attacks August 25 2026
- crack spread refining margins diesel gasoline August 2026
- XLE XOM CVX COP energy stocks premarket August 25 2026
- natural gas price Henry Hub August 25 2026
- IEA OPEC oil demand forecast 2026 demand destruction
- oil prices drop Iran sanctions Bessent August 25 2026
- sector rotation energy lagging technology August 25 2026 XLE
- economic calendar August 25 26 2026 PCE EIA crude inventories
- web_fetch https://straitofhormuz.report/oil
- web_fetch https://finance.yahoo.com/quote/XLE
- x_search WTI Brent crude oil price drop August 25 2026 XLE (timed out)

**Key sources and facts taken**
- Yahoo Finance XLE / CL=F (fetched 2026-08-25 ~08:58 ET): XLE last 63.11 (−0.83% 8/24 close), premarket **62.29 (−1.33%)**; Crude Oil Oct ’26 **$82.35 (−3.13%)**; ES +0.36%, NQ +0.74% on the quote page. https://finance.yahoo.com/quote/XLE
- Channel 1 (injected, not altered): CL=F −3.06%, BZ=F −4.54%; XLE 1d −0.83% vs SPY −0.29% (rel −0.54%).
- FT/Trading Economics/Business Insider snapshots (search, 2026-08-25): WTI ~$82–83, Brent ~$88–89.5, both ~−2.5–3.5% on the day. https://markets.businessinsider.com/commodities/oil-prices
- EIA WPSR week ending 8/14 (released 8/19): crude **+4.405M bbl to 428.8M**; next report 8/26 for week ending 8/21. https://www.eia.gov/petroleum/supply/weekly/
- OPEC 2 Aug 2026 PR: ~188 kb/d increase for September, completing the 2023 voluntary-cut rollback; August already +188 kb/d. https://www.opec.org/pr-detail/1854611-2-august-2026.html
- Reuters 17 Aug 2026: US diesel crack record **$102.20/bbl**. https://www.reuters.com/business/energy/us-diesel-crack-surpasses-100-barrel-first-time-supply-disruptions-2026-08-17/
- Reuters 24 Aug 2026: Iran blacklisted 45 tankers for Hormuz transit-rule violations. https://www.reuters.com/world/middle-east/iran-warns-vessels-violating-hormuz-transit-rules-fines-detention-2026-08-24/
- NYT 24 Aug 2026: oil’s largest one-day drop in three weeks as Bessent sanctions were faded (Brent ~$92, WTI ~$85 that session). https://www.nytimes.com/2026/08/24/business/oil-prices-bonds-stocks.html
- IEA Oil Market Report August 2026: 2026 demand **−1.6 mb/d**. https://www.iea.org/reports/oil-market-report-august-2026
- CryptoBriefing / SSGA flow context: energy ETFs ~**$4B outflows** over ~65 days into mid-August. https://cryptobriefing.com/us-energy-etf-outflows-4-billion/
- Trading Economics 25 Aug 2026: Henry Hub nat gas **~$2.72**, ~−2.2% d/d. https://tradingeconomics.com/commodity/natural-gas
- Calendar: no PCE/EIA today; **PCE + EIA weekly Wed 8/26**. https://www.eia.gov/petroleum/supply/weekly/

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': -2.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': -1.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 1.0, 'leading_sum': -8.0, 'divergence_flagged': False, 'total_score': -10.0, 'predicted_direction': 'down', 'predicted_magnitude_band': 'notable', 'confidence_score': 0.58, 'regime': 'mixed'}
```
