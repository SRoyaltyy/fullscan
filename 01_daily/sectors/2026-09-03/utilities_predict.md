# Sector Prediction — Utilities — 2026-09-03

- ETF: **XLU**
- rubric: `00_grounding/sectors/utilities.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **0.0** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLU vs SPY (yfinance, through 2026-09-02):
  1d: XLU +0.26% | SPY +0.44% | rel -0.19%
  3d: XLU -1.18% | SPY -0.77% | rel -0.41%
  1w: XLU -1.48% | SPY -0.10% | rel -1.38%
  1m: XLU -3.81% | SPY +0.99% | rel -4.80%
```

MEMORY_CONFIRM: Utilities/XLU only — memory index unavailable this run (used injected sector logs + Channel 1). Rolling dir=0.4 / mag=0.4 (n=10). Last graded: 08-28 down/mild vs XLU −1.04% / SPY −0.23% (dir HIT, mag MISS — hawkish Warsh not knowable at open). 08-27 up/notable vs −0.76% (dir MISS). 08-31 and 09-01 down/mild still ungraded. Applied: 08-14 (calendar: 8:30 claims + 10:00 ISM Services are two-sided, not a scored FTS HIT); 08-21 (live curve, not FRED 9/1 1d — 10Y ~4.78–4.79 / 30Y ~5.25, not a second-day easing impulse); 08-25 (S0=S1=0 → do not manufacture down from carried 1w/1m lag); 08-27 (AVGO already public AHR 9/2, but NQ +0.16 vs ES +0.13 is not an NQ-led rip — lag default does not fire as a down mandate); 08-17 (no same-day miss printed, no fresh yield rally → no absolute up); 08-18 (not on: tape is not risk-off while long-end rises); 08-11 (does not flip up: 1d rel −0.19%, yields not independently verified falling); 08-12 (AI-power is a 1d dampener); 08-13 (S2/S4 confirmation only); 08-28 (do not pre-score claims/ISM into notable-down; CEG/NEE vote are not ETF drivers). Open experiment: extra confirm before full-weight — live 10Y + CME hike odds + Channel 1 DFII10. |score| small → keep mild/flat.

## Utilities (XLU) — 2026-09-03

Object is the **near-session XLU environment**, not SPX and not a stock pick.

### Channel 1 (trusted, not re-derived)

XLU vs SPY through 2026-09-02: **1d +0.26% / +0.44% (rel −0.19%)**; **3d −1.18% / −0.77% (rel −0.41%)**; **1w −1.48% / −0.10% (rel −1.38%)**; **1m −3.81% / +0.99% (rel −4.80%)**.

Macro: VIX **15.18** (calm; VIX/VIX3M **0.999**); DGS10 **4.79** as of **2026-09-01** (+4 bp 1d / +15 bp 1w); DGS30 **5.27** (+2 bp 1d / +10 bp 1w); DFII10 **2.44** (0 bp 1d, **+12 bp 1w**); HY 2.65 tight; EPU 167 (−104 1d); CL=F −0.26% / Finviz WTI **−1.19%** / Brent **−1.07%**; DXY −0.35%; **ES=F +0.13% / NQ=F +0.16%**; Asia +0.04%; Europe +0.19%; F&G 58.2 Greed (stale 8/27); 5-day 10Y–SPX corr **−0.795**. Bond futures: 10Y note **+0.17%**, 30Y **+0.38%**.

**Live curve (08-21 / 08-25 — not the 9/1 1d column as “today’s easing”):** 10Y ~**4.78–4.79%** (GuruFocus 4.78% 9/3; DJ 9/2 close 4.793%); 30Y ~**5.25%**. That is a **1–2 bp stabilization** inside the long-end stress zone, not a verified second-day easing impulse. Do **not** score S0/S1 from carried 9/1 backup, and do **not** mint duration relief from the futures tick.

**Calendar (08-14 / 08-27):** **8:30 ET** initial jobless claims (cons. ~205k vs 203k prior), goods/services trade, Q2 productivity final. **10:00 ET** ISM Services (cons. ~54.2 vs 54.1). Claims are a real 8:30 — never “no macro print.” Outcome is **two-sided** at 09:30: a hot miss can FTS-bid the bond-proxy; a cool print can keep the mild risk-on tape and leave XLU lagging. Do **not** pre-score either branch.

### Channel 2

**1. Shared macro → this sector.** Classical map is **real/nominal yields**. Live 10Y/30Y/TIPS are **sticky-high, not falling**. Overlay: ES/NQ are **flat-green inside ±0.5%**, not an 08-27 NQ≥+0.5% anti-FTS rip and not risk-off/FTS (VIX 15.2, oil **down**, no VIX>20). News Judge “futures lower to start September” is **stale vs Channel 1** — trust ES +0.13% / NQ +0.16%. Hawkish Fed (divided hold, 3 hike votes, Sep hike odds ~**60–70%** per WSJ 9/2) is the **carried** rates object, already in 4.79/5.27 — counted **once**, not again as a fresh smash. Oil-slide is **not** an 08-11 Hormuz FTS positive; it is the 08-25 inflation→yield channel, and it only **forbids forcing down**, it does **not** authorize up. Gold’s Channel 1 bounce (+1.5% / GC +2.7%) confirms the morning is not a long-end blowout. Net **S0 = 0**.

**2. Spine / secondary.**
- **Data-center load / power demand:** structural HIT, **stale** for a 1d call (SO 17 GW contracted large-load, Duke equity-funded capex, NEE/Dominion vote **today 9:00 ET**). Rubric: do **not** let the multi-year AI-power story override a 1d rate tape without a fresh XLU-wide catalyst. MAP HEAT: CEG/VST nested pos is **IPP, not XLU confirm**; regulated-electric NEE/SO news sits inside a week-lag tape.
- **Rates falling (bond-proxy bid):** **MISS**. Live 10Y ~4.78–4.79 / 30Y ~5.25. Tiny bond-future dip is noise at this level (08-21).
- **Rates rising (bond-proxy selloff):** **PARTIAL / carried**. 1w DGS10 +15 bp and hike odds ~70% are already in the price; this morning the curve is not independently rising. Do **not** HIT and do **not** double-count with S0.
- **Risk-on rotation away from utilities:** **MISS**. AVGO beat-and-raise is **already public** (9/2 AHR) with a mixed AHR fade on a light Q4 print; NQ is not leading. Goolsbee AI-spend warning is XLK, not an XLU rotation HIT.
- **Nuclear / grid CapEx / favorable ROE:** structural, no same-session order.
- **Adverse rate case / load-growth disappointment / regulatory smash:** carried (WoodMac/Texas/Ohio, Fitch “deteriorating” June). Not fresh. Single-name must not drive the ETF (08-28 PCG rule; AES takeout is diversified nested, size-gated).
Net **S1 = 0**.

**3. Breadth.** Tuesday’s 1d was **flat-lag** (rel −0.19%), not a smash. Premarket: XLU ~**$42.67 flat**, NEE ~+0.3%, DUK flat, SO mixed, CEG modest green after **+3.47%** 9/2 — CEG must **not** set the ETF. MAP HEAT mixed (gas residual up, regulated electric week-lag, IPP not confirming). Do **not** copy 1w/1m lag into S2. **S2 = 0**.

**4. Flows.** ETFdb ~9/2: 5d **+$131M**, 1m **+$22M**. Modest, not a relative-volume spike, not a 1-day lid. 1m rel −4.80% is de-risked, not a crowded-long unwind. **S3 = 0**.

**5. Catalysts.** No fresh XLU-wide rate-order/load print. Claims/ISM remain two-sided. NEE–Dominion shareholder votes are **single-ticker event risk**, not an S1 spine. AVGO is non-holdings and already traded.

### Lessons → scores

**08-25 is the veto on manufacturing down:** S0=0 and S1=0; leftover 1w/1m underperformance is not a fresh sector negative.

**08-11 does not flip up:** 1d rel is **−0.19%**, not outperformance; yields are not independently verified falling.

**08-27 lag default does not fire as a down mandate:** AVGO is public, but NQ is **not** leading ES by a meaningful margin (both ~+0.15%). Ban on minting **up** from carried easing still holds.

**08-18 does not fire:** this is not risk-off + rising 10Y at the open.

**08-14:** claims are knowable event risk. A miss *could* FTS-bid XLU; that is why S0 is not −1 from “prior risk-on.” It is **not** a scored +1 until it prints.

**08-12 / 08-13:** AI-power stays a dampener; S2/S4 are confirmation only. Allow XLU to lag SPY even if the absolute print is flat.

**Divergence:** leading (S0 0 + S1 0 + S2 0 + S3 0 = 0) and S4 (0) **agree**. No flag. Trust factors over tape — here both are a pause.

**Self-audit:** duration and hawkish Fed counted **once**; 9/1 FRED backup not re-paid as live rising; CEG/NEE/AES cannot drive XLU; band stays **flat/mild** (rolling mag 0.4, |Σ|<4). Implied arithmetic: (0)×0.9 = **0 → flat / flat**. Claims 8:30 is the only path that can still mint a mild defensive bid **after** the print — not before.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 0
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.52
REGIME: mixed
DIVERGENCE_FLAGGED: false
HORIZON_3D: down:mild:0.55
HORIZON_1W: down:mild:0.58
HORIZON_2W: down:mild:0.56
HORIZON_1M: down:mild:0.50
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|MISS|0.62|2026-09-03|Channel 1 ES +0.13% / NQ +0.16% (flat, not expansion)
Risk-off tape / flight to safety|MISS|0.70|2026-09-03|VIX 15.18, oil down, no VIX>20 FTS
Real yields rising|PARTIAL|0.64|2026-09-01|https://fred.stlouisfed.org/series/dfii10
Real yields falling|MISS|0.66|2026-09-03|DFII10 2.44, 1d 0.0; live 10Y not falling
USD strengthening|MISS|0.68|2026-09-03|Channel 1 DXY −0.35%
USD weakening|HIT|0.60|2026-09-03|Channel 1 DXY −0.35% / USD −0.31%
Sector breadth expansion (% names up)|MISS|0.58|2026-09-03|XLU premarket flat; NEE/DUK mixed
Sector breadth failure (ETF up, names flat)|MISS|0.55|2026-09-03|1d XLU only +0.26%, not an ETF-only squeeze
Large-cap leadership inside sector|PARTIAL|0.50|2026-09-02|CEG +3.47% 9/2 — single-name, not ETF driver
Small/mid leadership inside sector|MISS|0.45|2026-09-03|checked, nothing material
High-beta leadership inside sector|MISS|0.50|2026-09-03|IPP heat not confirming XLU
Low-beta leadership inside sector|PARTIAL|0.48|2026-09-03|regulated names flat-to-soft vs CEG
Sector ETF inflow / relative volume spike|PARTIAL|0.58|2026-09-02|https://etfdb.com/etf/XLU/
Sector ETF outflow / volume dry-up|MISS|0.58|2026-09-02|5d +$131M / 1m +$22M
Crowded long (extreme relative performance + valuation)|MISS|0.60|2026-09-02|1m rel −4.80% de-risked
Index rebalance / inclusion tailwind|MISS|0.40|2026-09-03|checked, nothing material
Index exclusion / forced selling|MISS|0.40|2026-09-03|checked, nothing material
Data-center load growth / power demand upside|HIT|0.70|2026-09-03|https://www.utilitydive.com/news/southern-co-contracted-large-load-data-centers/826919/
Rates falling (bond-proxy bid)|MISS|0.72|2026-09-03|https://www.gurufocus.com/economic_indicators/37/10-year-treasury-yield
Favorable rate case / allowed ROE|MISS|0.45|2026-09-03|checked, nothing material
Nuclear / gas generation policy support|PARTIAL|0.50|2026-09-03|structural, no same-session order
Grid CapEx approval / recovery|PARTIAL|0.52|2026-09-03|https://www.utilitydive.com/news/duke-energy-to-issue-10b-in-equity-to-capture-once-in-a-generation-gro/827039/
Rates rising (bond-proxy selloff)|PARTIAL|0.68|2026-09-02|https://www.wsj.com/livecoverage/stock-market-today-dow-sp-500-nasdaq-09-02-2026/card/odds-of-fed-rate-hike-in-september-hit-70--PQ04igZVrpQ5Hy4h87rp
Adverse rate case|MISS|0.45|2026-09-03|checked, nothing material
Load growth disappointment|MISS|0.48|2026-09-03|carried WoodMac/Texas — not fresh
Regulatory disallowance / project cancel|MISS|0.45|2026-09-03|checked, nothing material
Risk-on rotation away from utilities|MISS|0.60|2026-09-03|NQ not leading; AVGO already traded
Sector rotation into utilities|MISS|0.58|2026-09-02|1d rel −0.19%
Sector rotation out of utilities|PARTIAL|0.60|2026-09-02|1w rel −1.38% / 1m rel −4.80% already paid
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- US economic calendar September 3 2026 jobless claims 8:30
- US 10 year Treasury yield today September 2026
- 30 year Treasury yield September 3 2026
- TIPS 10 year real yield DFII10 September 2026
- CME FedWatch September 2026 rate hike odds Warsh
- XLU utilities stocks news rate case data center power demand September 2026
- XLU premarket NextEra Duke Southern Constellation September 3 2026
- XLU ETF flows inflows outflows September 2026
- ISM Services PMI September 3 2026 calendar
- initial jobless claims forecast September 3 2026 consensus
- utilities sector rotation yields September 2026 XLU
- Broadcom earnings utilities rotation XLU September 3 2026
- NextEra Duke Southern news September 2 3 2026
- Fetches: gurufocus.com 10Y (403 ASN ban); etfdb.com/etf/XLU/ (403 challenge)

**Key sources (title + URL + timestamp/facts used)**
- GuruFocus 10-Year Treasury Yield — https://www.gurufocus.com/economic_indicators/37/10-year-treasury-yield — ~4.78% as of 2026-09-03 (search snippet; page fetch blocked).
- Dow Jones / Morningstar — “10-Year Treasury Yield Falls to 4.793%” — https://www.morningstar.com/news/dow-jones/202609027515/10-year-treasury-yield-falls-to-4793-data-talk — 9/2 3 p.m. ET close 4.793% (−0.2 bp).
- GuruFocus 30-Year Yield — https://www.gurufocus.com/economic_indicators/111/30-year-yield — ~5.25% as of 2026-09-03.
- FRED DFII10 — https://fred.stlouisfed.org/series/dfii10 — 2.44% on 2026-09-01 (1d 0.0; 8/27 was 2.34%).
- WSJ live coverage 2026-09-02 — https://www.wsj.com/livecoverage/stock-market-today-dow-sp-500-nasdaq-09-02-2026/card/odds-of-fed-rate-hike-in-september-hit-70--PQ04igZVrpQ5Hy4h87rp — Sep hike odds ~70% (from ~37% a week earlier).
- YCharts / Investing / Myfxbook calendars — claims 8:30 ET 2026-09-03, cons. ~205k vs 203k prior.
- ISM calendar — https://www.ismworld.org/supply-management-news-and-reports/reports/rob-report-calendar/ — Services PMI 10:00 ET 2026-09-03, cons. ~54.2 vs 54.1.
- ETFdb XLU — https://etfdb.com/etf/XLU/ — 5d +$130.86M, 1m +$22.28M as of ~Sep 2 (page fetch blocked; used search extract).
- CNBC Broadcom Q3 — https://www.cnbc.com/2026/09/02/broadcom-avgo-q3-earnings-report-2026.html — 9/2 AHR: rev $29.6B, EPS $3.32, AI $16.7B; Q4 guide $34.8B slightly light; mixed AHR.
- Utility Dive Southern large-load — https://www.utilitydive.com/news/southern-co-contracted-large-load-data-centers/826919/ — 17 GW contracted (stale structural).
- Utility Dive Duke equity — https://www.utilitydive.com/news/duke-energy-to-issue-10b-in-equity-to-capture-once-in-a-generation-gro/827039/ — ~$10B equity 2027–30 (dilution/capex, not a 1d bid).
- MarketWatch premarket snapshots (NEE/DUK/XLU) — XLU ~$42.67 flat; NEE ~+0.26%; DUK flat; CEG modest green after +3.47% 9/2.
- Channel 1 panel (injected, not altered) — ES/NQ, XLU/SPY relatives, DGS10/30, DFII10, oil, DXY, VIX.

**Facts taken / not taken**
- Taken: 8:30 claims exist; live 10Y ~4.78–4.79 and 30Y ~5.25; hike odds ~60–70% carried; ES/NQ flat-green; XLU 1d rel −0.19%; modest XLU inflows; AVGO already public; AI-power structural but stale; oil down this morning.
- Not taken as 1d XLU drivers: News Judge “futures lower” (conflicts with Channel 1); gold −3% Warsh print (conflicts with Channel 1 gold +1.5%); CEG 9/2 +3.47%; NEE–Dominion vote; Duke $10B equity; SO 17 GW load; RSS Fed-cut duplicates.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': 0.0, 'divergence_flagged': False, 'total_score': 0.0, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.52, 'regime': 'mixed', 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'high-impact Finviz economic calendar'}
```
