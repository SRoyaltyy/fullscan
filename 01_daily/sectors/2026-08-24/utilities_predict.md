# Sector Prediction — Utilities — 2026-08-24

- ETF: **XLU**
- rubric: `00_grounding/sectors/utilities.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-3.15** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLU vs SPY (yfinance, through 2026-08-24):
  1d: XLU +0.27% | SPY -0.35% | rel +0.62%
  3d: XLU -2.58% | SPY -0.78% | rel -1.79%
  1w: XLU -2.93% | SPY -1.25% | rel -1.68%
  1m: XLU -7.36% | SPY +3.26% | rel -10.62%
```

MEMORY_CONFIRM: Injected Utilities scoreboard/lessons used (memory index unavailable). Last graded: 08-21 up/flat vs XLU −2.28% (dir MISS). Rolling dir=0.5 / mag=0.375 (n=8). Active XLU rules applied: 08-11 (no mechanical down-extend only if yields *and* tape are inflecting — they are not); 08-12 (don’t let AI-power inflate magnitude); 08-13 (S2/S4 = absolute confirmation only); 08-14 (calendar scan: no high-impact 8:30 regime-flip today); 08-17 (carried defensive bid is relative, not absolute); 08-18 (risk-off + elevated long-end → relative beat / flat-to-negative absolute); 08-21 (do not score “yields easing” off stale FRED; live tape + hawkish-Fed overhang cap S0/S1). Open experiment: extra confirming source before a full-weight lean — used live 10Y/30Y + ETF outflows + 3d/1w/1m tape.

## Utilities (XLU) — 2026-08-24

Object is the **near-session XLU environment**, not SPX and not a stock pick.

### Channel 1 (trusted, not re-derived)

XLU vs SPY: **1d +0.27% / −0.35% (rel +0.62%)**; **3d −2.58% / −0.78% (rel −1.79%)**; **1w −2.93% / −1.25% (rel −1.68%)**; **1m −7.36% / +3.26% (rel −10.62%)**.

Macro panel: VIX 15.91 (+0.78, slight VIX/VIX3M backwardation); official DGS10 **4.69** / DGS30 **5.23** as of **2026-08-20** (both +4 bp that print); DFII10 2.35 (1d flat, 1w −4 bp); HY 2.75 slightly wider; EPU **+60** to 248; CL −1.75% / BZ −1.31%; gold +2.14%; DXY +0.13% 1d; **ES −0.18% / NQ −0.58%**; Asia **−1.17%** (Kospi −3.12%); Europe −0.13%; F&G 55.2; 5-day 10Y–SPX corr **−0.21**.

**08-21 recency check:** those FRED yield prints are **Thursday**. Friday’s live tape was a long-end smash (XLU −2.28%, 10Y ~4.74%, 30Y ~5.27%). This morning’s live quotes are **10Y ~4.71%**, **30Y ~5.24–5.28%** — a few bp of pre-Jackson-Hole bid, **not** a confirmed easing regime, and still above the official 4.69 / near-cycle-high long end.

### Channel 2

**1. Shared macro → this sector.** Mild risk-off (NQ/Asia weaker than ES, VIX up, EPU spike, gold bid) is a *relative* utility bid. The **rate spine still dominates the absolute close**: 30Y remains in the mid-5.20s, last week’s index driver was elevated long-end yields, and News Judge #1–3 is a **hawkish Warsh Jackson Hole (Fri 8/28) + inflation-week** binary. News Judge #4 (Treasuries bid into the speech) forbids treating the hawkish prior as locked, and the two-sided JH/inflation rule forbids a one-way S0. Oil is **down** on sanctions headlines — **not** a Hormuz supply squeeze, so no fresh inflation-via-oil override. Calendar: **no CPI today**; CFNAI July **−0.08** (below trend, low-impact). Not an 08-14 retail-sales-style regime flip.

**2. Spine / secondary.**  
- **Data-center / power demand:** structural HIT, **stale**. No fresh same-day load catalyst. Carried WoodMac / Texas / Nvidia-Ohio skepticism still sits on the narrative. **Do not** let the multi-year AI-power story override a 1d rate tape.  
- **Rates falling:** **not a HIT**. Live 10Y 4.71 vs Friday 4.74 is a pre-event dip, not relief.  
- **Rates rising:** **PARTIAL** (level still punitive; impulse not repeating this morning).  
- **Risk-on rotation away:** not firing (NQ/Kospi weak).  
- **Nuclear / grid CapEx:** structural HIT (We Energies–NextEra Point Beach PPA into 2050s; Duke hearings; ~$1.3T IOU capex 2026–30). Not a same-session ETF impulse.  
- **Rate cases:** CMP seeking ROE 9.35% → 9.8% (unresolved). Not a favorable-order HIT.  
- **Load-growth disappointment / AEP miss:** carried, not fresh. Single-name must not drive the ETF call.  
- **Rotation into/out:** 1d relative in; 3d/1w/1m **out**.

**3. Breadth.** Friday’s −2.3% was **broad** (NEE −1.6%, DUK −2.3%, SO −2.7%). Today’s +0.27% / +0.62% rel is a bounce after that smash, not expansion. 3d/1w/1m relative tape is a **failure**.

**4. Flows.** ETFDB through ~8/21: **5d −$190M, 1m −$236M**. No confirmed same-day inflow spike. De-risked on 1m (rel −10.6%), not a crowded-long extreme that forces a bounce today.

**5. Catalysts.** Warsh JH **Friday** is the week’s load-bearing event (two-sided). Today is a **light** calendar. No fresh XLU-wide earnings/rate-order win.

### Lessons → scores

08-18 + 08-17: defensive bid + still-elevated 10Y/30Y → **relative outperformance, flat-to-negative absolute**. Do not upgrade to absolute up because 1d rel is green or because Iran headlines exist (oil is falling).  
08-21: do **not** score S0/S1 positive on “rates falling.”  
08-11 does **not** flip this to up: the yield driver is not durably easing and 3d/1w tape is not inflecting.  
08-12: AI-power stays a dampener, not a magnitude engine.  
08-14: CFNAI is not a high-impact flip print.

**Divergence:** leading factors (breadth + outflows, muted macro/spine) lean **down/flat**; 1d rel **+0.62%** is a relative bounce. **Flag it. Trust factors over tape.** Relative bid can leave XLU less red than SPY and still not green.

**Self-audit:** rate lens over AI narrative on a 1d horizon; band capped (no notable — this is not a repeat of Friday’s +5 bp smash); no same-shock double-count of yields in S0 and S1 (both held at 0); AEP/NEE 13Fs and the AEP miss do not drive the ETF call. Policy: last three XLU losses were **up** calls vs a negative tape — cut conviction, prefer flat/mild.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 0
S2_BREADTH: -1
S3_FLOWS_POSITIONING: -1
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.52
REGIME: mixed
HORIZON_3D: down:mild:0.48
HORIZON_1W: down:mild:0.45
HORIZON_2W: down:mild:0.42
HORIZON_1M: flat:mild:0.40
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|MISS|0.72|2026-08-24|https://www.thestreet.com/stock-market-today/stock-market-today-dow-jones-sp-500-nasdaq-updates-aug-24-2026
Risk-off tape / flight to safety|PARTIAL|0.68|2026-08-24|https://www.thestreet.com/stock-market-today/stock-market-today-dow-jones-sp-500-nasdaq-updates-aug-24-2026
Real yields rising|MISS|0.60|2026-08-24|
Real yields falling|MISS|0.62|2026-08-24|
USD strengthening|PARTIAL|0.55|2026-08-24|
USD weakening|MISS|0.55|2026-08-24|
Sector breadth expansion (% names up)|MISS|0.74|2026-08-24|https://finance.yahoo.com/quote/XLU/history/
Sector breadth failure (ETF up, names flat)|PARTIAL|0.58|2026-08-24|https://finance.yahoo.com/quote/XLU/history/
Large-cap leadership inside sector|MISS|0.70|2026-08-21|https://finance.yahoo.com/quote/NEE/history/
Small/mid leadership inside sector|checked, nothing material|0.40|2026-08-24|
High-beta leadership inside sector|MISS|0.65|2026-08-24|
Low-beta leadership inside sector|PARTIAL|0.60|2026-08-24|
Sector ETF inflow / relative volume spike|MISS|0.78|2026-08-21|https://etfdb.com/etf/XLU/
Sector ETF outflow / volume dry-up|HIT|0.78|2026-08-21|https://etfdb.com/etf/XLU/
Crowded long (extreme relative performance + valuation)|MISS|0.70|2026-08-24|
Index rebalance / inclusion tailwind|checked, nothing material|0.35|2026-08-24|
Index exclusion / forced selling|checked, nothing material|0.35|2026-08-24|
Data-center load growth / power demand upside|PARTIAL|0.70|2026-08-24|https://247wallst.com/investing/2026/07/06/3-utility-etfs-to-buy-now-as-ai-data-centers-trigger-a-1970s-scale-power-buildout/
Rates falling (bond-proxy bid)|MISS|0.72|2026-08-24|https://www.cnbc.com/quotes/US10Y
Favorable rate case / allowed ROE|MISS|0.62|2026-08-24|https://www.pressherald.com/?p=7708573
Nuclear / gas generation policy support|HIT|0.68|2026-08-20|https://www.wpr.org/energy/we-energies-buy-power-from-point-beach-nuclear-plant
Grid CapEx approval / recovery|HIT|0.66|2026-04-01|https://www.spglobal.com/market-intelligence/en/news-insights/research/2026/04/surging-energy-demand-puts-us-utility-capex-forecast-near-1-3t-in-2026-30
Rates rising (bond-proxy selloff)|PARTIAL|0.74|2026-08-24|https://www.reuters.com/business/bond-market-anxiety-raises-stakes-warshs-debut-jackson-hole-speech-2026-08-24/
Adverse rate case|checked, nothing material|0.45|2026-08-24|
Load growth disappointment|PARTIAL|0.60|2026-08-24|
Regulatory disallowance / project cancel|checked, nothing material|0.40|2026-08-24|
Risk-on rotation away from utilities|MISS|0.70|2026-08-24|
Sector rotation into utilities|PARTIAL|0.58|2026-08-24|
Sector rotation out of utilities|HIT|0.76|2026-08-24|
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- memory_search: Utilities XLU sector prediction lessons yields Jackson Hole (index unavailable)
- web_search: 10 year Treasury yield today August 24 2026
- web_search: Jackson Hole Warsh Fed speech August 2026 utilities bond yields
- web_search: XLU utilities ETF news data center power demand August 2026
- web_search: US economic calendar August 24 2026 CPI inflation Jackson Hole
- web_search: US 10 year 30 year Treasury yield live August 24 2026
- web_search: XLU ETF inflows outflows August 2026 utilities rotation
- web_search: utility rate case nuclear grid capex news August 2026
- web_search: AEP NextEra Duke Constellation stock news August 24 2026
- web_search: Iran sanctions oil prices August 24 2026 risk off stocks
- web_search: Chicago Fed National Activity Index August 24 2026
- web_search: utilities sector stocks breadth XLU NEE DUK SO CEG August 24 2026
- web_fetch: Reuters JH / Trading Economics / ETFDB (JS/403 blocked)
- x_search: XLU utilities yields Jackson Hole Warsh (timed out)

**Key sources (facts taken)**
- Trading Economics / CNBC / MarketWatch (search wrap, 2026-08-24): US 10Y ~**4.71%** (eased ~3 bp vs Friday ~4.74%); 30Y ~**5.24–5.28%**. https://tradingeconomics.com/united-states/government-bond-yield · https://www.cnbc.com/quotes/US10Y
- Reuters (2026-08-24): Warsh **first JH keynote Fri Aug 28**; bond-market anxiety; inflation above target; 30Y near multi-decade highs; hike/higher-for-longer prior. https://www.reuters.com/business/bond-market-anxiety-raises-stakes-warshs-debut-jackson-hole-speech-2026-08-24/
- Bloomberg (2026-08-23/24, via search): long-end selloff risk without clear Warsh guidance; Treasuries bid with Bessent/Warsh due. https://www.bloomberg.com/news/articles/2026-08-24/treasuries-gain-with-bessent-and-warsh-due-to-set-direction
- Scotiabank / TS2 / BLS calendars: **Aug 24 light** — no CPI; next CPI Sep 11; JH Aug 27–29. https://www.scotiabank.com/ca/en/about/economics/economics-publications/post.other-publications.calendar-of-economic-release-dates.calendar-of-economic-release-dates--august-2026-.html
- Trading Economics / FRED: CFNAI July **−0.08** released Aug 24 (below trend). https://tradingeconomics.com/united-states/chicago-fed-national-activity-index
- ETFDB (~Aug 21): XLU **5d −$189.56M**, **1m −$235.62M**. https://etfdb.com/etf/XLU/
- Yahoo/MarketWatch histories: XLU Fri close **$42.77 (−2.28%)**; NEE −1.62%, DUK −2.31%, SO −2.72%. https://finance.yahoo.com/quote/XLU/history/
- Portland Press Herald (2026-08-24): CMP rate case, ROE ask **9.8% vs 9.35%**. https://www.pressherald.com/?p=7708573
- WPR (2026-08-20): We Energies–NextEra Point Beach nuclear PPA through **2050/2053**. https://www.wpr.org/energy/we-energies-buy-power-from-point-beach-nuclear-plant
- S&P Global: IOU capex ~**$1.3T 2026–30**. https://www.spglobal.com/market-intelligence/en/news-insights/research/2026/04/surging-energy-demand-puts-us-utility-capex-forecast-near-1-3t-in-2026-30
- NYT / The Street / Reuters energy (2026-08-24): Iran sanctions headlines; **WTI ~$85, down ~1.7–2.3%** — not a live squeeze. https://www.nytimes.com/2026/08/24/business/oil-prices-bonds-stocks.html
- 24/7 Wall St / Utility Dive: AI data-center power demand **structural**, no Aug 24 incremental XLU catalyst. https://247wallst.com/investing/2026/07/06/3-utility-etfs-to-buy-now-as-ai-data-centers-trigger-a-1970s-scale-power-buildout/
- Finviz digest: AEP Q2 miss / PT cuts — **stale**, already in the 08-21 tape.

**Checked, nothing material:** same-day XLU-wide rate-order win; index rebalance; fresh load-growth print; Hormuz tanker/closure escalation with oil up.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': -1.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': -2.0, 'divergence_flagged': False, 'total_score': -3.15, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.52, 'regime': 'mixed'}
```
