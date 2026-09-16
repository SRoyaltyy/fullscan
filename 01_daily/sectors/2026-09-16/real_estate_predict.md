# Sector Prediction — Real Estate — 2026-09-16

- ETF: **XLRE**
- rubric: `00_grounding/sectors/real_estate.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **2.52** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **1.196** (ES +1.14%, ZN -0.03%, PM:XLRE +0.05%) · index_carry **1.324** (general 5.297) · llm_overlay **0.0** (raw 0.0)

## Channel 1 sector ETF tape

```
ETF XLRE vs SPY (yfinance, through 2026-09-15):
  1d: XLRE -0.12% | SPY -0.46% | rel +0.34%
  3d: XLRE +0.05% | SPY -0.06% | rel +0.10%
  1w: XLRE -1.89% | SPY -1.12% | rel -0.77%
  1m: XLRE -4.86% | SPY -2.44% | rel -2.42%
```

MEMORY_CONFIRM: Memory index unavailable this run (embedding metadata mismatch) — used injected Real Estate scoreboard, standing REIT lessons, and last-10 XLRE logs only. Last graded: 2026-09-15 down/mild vs XLRE −0.116% / SPY −0.46% / rel +0.34% (dir HIT, mag MISS — actual flat; stacked duration). 2026-09-14 down/mild (dir HIT, mag HIT). 2026-09-11 down/flat vs +0.859% (dir MISS — pure beta). Rolling dir=0.5 mag=0.4 (n=10); last-30 dir=0.55 mag=0.35 (n=20). Open `sector_real_estate` experiment **applies**: keep direction, shrink confidence on modest |score|. Applied: **(1) 08-27** — 08-25 is a ban on forcing down, not an up license; 30Y in stress zone ⇒ cap S0/S1 at 0; do not double-count one rate/oil object; do not pad S1 with always-on DC/industrial; green/NQ beta ≠ REIT duration relief. **(2) 08-25** — live curve must be independently verified falling before a down-force is lifted; today’s 10Y is off the 5.04% high but Finviz 10Y note −0.03% / 30Y bond −0.06% is **not** a verified second-day decline. **(3) 08-21** — DGS30 **5.34** is still the multi-decade stress zone; a 1–2 bp tick is not relief. **(4) 08-17/08-18** smash branch **OFF** (no live long-end rip at the open). **(5) 08-11** spike branch **OFF** (WTI −1.59% / Brent −1.02% — oil slide, level still >$100). **(6) 08-12** — FOMC 14:00 ET + retail sales 08:30 ET stay two-sided; do not one-way hawkish-score S0. **(7) 09-04** asymmetric-downside **does not fully fire** — that rule needs a live unresolved rates-backup open, not a post-5% pullback into a priced hike. **(8) 09-08** cushion override **does NOT fire** (1d rel **+0.34%** < +0.4% gate). **(9) 09-11** **does fire** — S0 genuinely mixed, oil offered, live curve not rising; a down call needs a LIVE negative; stale 1w/1m lag must not be scored into S2 **and** S4; already-priced hike (~92–93%) = 0. **(10) 09-14** — XLRE premarket **+0.05%** is unconfirmed in this ETF; may not set sign or offset S0. **(11) 09-15** flatten-mag / de-correlate rule is for a *telegraphed live level smash* with sub-gate green rel — yesterday’s 5% print, not this open. **(12) 09-03** — FOMC is high-impact two-sided: do not treat the binary as a flat-band cap; shrink confidence instead. **(13) 08-14** reconcile Σ×mult.

## Real Estate (XLRE) — 2026-09-16

### Channel 1 (used as given, not re-derived)

Rates through **2026-09-14**: DGS10 **4.97** (1d **+0.01** / 1w **+0.19** / 1m **+0.34**), DGS30 **5.34** (1d **−0.01** / 1w **+0.10** / 1m **+0.13**), DFII10 **2.60** (1d **0.0** / 1w **+0.17** / 1m **+0.21**) — **real yields up on 1w/1m**. That 1d column is **Monday’s close**, not this open. VIX **16.98** (1d −0.22, 1w +0.52), VIX/VIX3M **0.877 — contango** (stress term structure has normalized vs 09-11/09-14 backwardation). Finviz: ES **+0.20%**, NQ **+0.41%**, Russell **+0.08%**, DJIA **+0.11%**. Channel 1 also prints **ES=F +1.14% / NQ=F +1.50% vs prev close** — overnight bounce from Tuesday’s red cash close, not a second independent tape. Asia **+0.65%**, Europe **+0.45%**. **Oil DOWN**: Finviz WTI **$104.16 (−1.59%)**, Brent **$107.67 (−1.02%)**; CL=F **−2.36%** / BZ=F **−1.50%** — slide, still >$100 as a *level*. Gold **+0.90%** (GC=F +1.36%). DXY **−0.02%** (flat). **10Y note −0.03%**, **30Y bond −0.06%**, Ultra Bond **−0.06%** (prices slightly down = **yields ~flat-to-+1 bp this morning**). HY OAS **2.71**. 5-day 10Y–SPX corr **−0.155** (weak). EPU **215.48** (collapsed vs last week).

**Sector premarket vs prev close: XLRE +0.05%** — mid-pack (XLK +0.65%, XLU +0.24%, XLV +0.20%; XLE −0.56%). **No 09-14-style defensive rotation bid.** Per 09-14 this print is **unconfirmed** and is scored **0** — it does not set sign and is not an S0 offset.

XLRE vs SPY through **2026-09-15**: 1d **−0.12 / −0.46 / rel +0.34**; 3d rel **+0.10**; 1w rel **−0.77**; 1m rel **−2.42**. **1d/3d are a sub-gate defensive relative bid; 1w/1m remain laggards.** 09-08 gate missed by 6 bp. Confirmation mix, not duration relief.

### Channel 2

**1. Shared macro as it hits REITs.** This is an **FOMC-day mixed tape**, not a rates-backup session and not a flight-to-safety bid into REITs. Live CNBC: 10Y **~4.96%** at 07:38 ET after a Tuesday high of **5.041%** (19-year high); later prints ~**4.99%**. 30Y **~5.35–5.36%** vs FRED **5.34** — still **≥5.15% stress**. Finviz long-end prices are **slightly down** (yields +1 bp-ish). That is **stabilization off a round-number high, not relief** (08-21) and **not a smash** (08-18 OFF). CME FedWatch **~92–93%** for a 25 bp hike to 3.75–4.00% at **14:00 ET**; Warsh JH hike repricing is **already printed** (T+weeks, News Judge #2). **08-12 / 09-11:** the *hike* is priced = **0**; the *dots / presser path* is two-sided and unprinted = **do not one-way S0**. Retail sales **08:30 ET** (cons. ~+0.8% / +0.5% ex-auto) is a second two-sided binary, also unprinted. Oil is **offered** (WTI −1.59%) — 08-11 spike OFF; 08-25 forbids booking the slide as duration relief without a verified falling curve. Gold **+0.90%** is a mild real-rate/FTS tell, not enough to flip a 5.34% 30Y. NQ leading ES is **XLK/AI beta**, not REIT duration relief (08-27). Risk-on futures do **not** make S0 positive for a bond-proxy (defensives are a funding source on a green-beta open). **S0 = 0.**

**2. Spine (count the rate object once; S0 is the map, not a second copy).**
- Rates falling / REIT duration relief: **MISS.** Live dip off 5.04% is real; 08-21 says it is not relief while 30Y ~5.35.
- Rates rising / REIT selloff: **MISS at the open.** Tuesday’s 5% print is yesterday. Live curve is flat-to-1 bp up, not ripping. 08-25 forbids treating the 9/14 +19 bp 1w column as today’s tape.
- Real yields rising: **HIT on 1w/1m** (DFII10 +17 / +21 bp) — **same duration channel**, not a second independent shock. Already inside the S0 cap.

**3. Secondary.**
- Data-center REIT demand / rent upside: **HIT, stale / single-name.** EQIX Q2 guide already raised; AI-spend wobble into 9/15, modest premarket bounce. **08-27: not a same-day up vote. EQIX must not define XLRE.** MAP HEAT specialty = none.
- Industrial REIT occupancy / rent growth: **HIT, stale.** PLD quality sleeve; **PLD $1.07 ex-div today** is mechanical, not a fundamental up/down vote. MAP HEAT industrial = none.
- Refinancing window / cap-rate compression: **MISS.** 30Y **5.34**; no compression.
- Office vacancy / mark-to-market: **HIT, small sleeve.** CBRE Q2 vacancy **~18.3%** (down 30 bp, still stressed). Office ~1% of XLRE (BXP). Do not let office set the ETF. MAP HEAT office = none.
- Refinancing wall: **HIT, structural.** ~$875B 2026 CRE maturities; not a same-morning print.
- Sector rotation into REITs: **MISS** (premarket +0.05%, not a bid).
- Sector rotation out of real estate: **structural on 1w/1m price** — see S3/S4 rules; **do not dump into S1.**

**S1 = 0.** Cap with S0 while 30Y is in the stress zone and the open change is noise. Do not pad with DC/industrial. Do not restack the 1w real-yield backup on top of S0.

**4. Breadth / leadership.** MAP HEAT: **every REIT sleeve flat / conv=low / captains=none.** No same-morning % names expanding. WELL/EQIX/PLD **must not** define XLRE. 1d rel **+0.34%** is a single trailing cash print — per the single-print rule it may anchor **at most one** component. It is used in **S4**, so **S2 = 0**. Stale 1w/1m lag is a structural descriptor, not a same-day breadth fail (09-11: “no cushion ≠ headwind”).

**5. Flows / positioning.** ETFdb/Trefis: XLRE **~−$286M 1m**, real-estate ETFs **largest category outflows (~−$654M / 1m)** into bonds/cash. That is **demand-soft over a month**, not a same-morning volume spike, not a crowded-long unwind (1m rel **−2.42%** is the opposite of extreme relative outperformance). Unconfirmed washout, not a live forced flow. **S3 = 0** (checked; nothing material same-day).

**6. Earnings / policy catalysts.** **FOMC 14:00 ET + SEP/dots + Warsh presser 14:30 ET** is the session’s dominant binary. Modal 25 bp hike is **priced**; path is **not**. Retail sales 08:30 is mid-tier vs FOMC, still two-sided. No XLRE-constituent earnings this morning. Nested HEAT overrides: **none**.

### Self-audit
- **Lens:** duration / real yields for XLRE, not SPX beta. Green ES/NQ is XLK, not REIT relief.
- **Band:** leading sum is ~0; pipeline owns totals. 09-03 says FOMC is mag-expansion risk — encoded as **lower confidence / 0.9 mult**, not as a signed pre-score.
- **Skew:** priced hike vs unpriced dots. Do not one-way the hawkish branch; do not one-way the dovish branch.
- **Same-shock double-count:** oil slide + modest yield pullback + gold up = **one** easing-ish open, counted once as “no live negative,” **not** as two S0/S1 positives. Warsh + 93% hike odds = **one** already-priced object = 0.
- **Single-ticker:** EQIX / WELL / PLD barred from the ETF call. MAP HEAT empty.
- **Divergence:** leading (S0–S3) = 0 vs S4 = +0.5. Tape does **not** fight factors; factors do not fight tape. **divergence_flagged = false.** Trust factors (flat) over a sub-gate relative bid.
- **09-11 vs 09-04:** live tape is **not** a rates-backup; down is forbidden without a live negative. **09-08** does not flip to a cushion override. **09-14** premarket ban respected. **09-15** de-correlate/flatten-mag does not fire (no live 10Y smash this morning).

### HORIZON_3D
FOMC path is the 72h object. Hawkish dots/presser → duration offered, XLRE lags; dovish surprise → the first clean duration bid in weeks. 3d rel is already **+0.10%** — a failed test of that bid is the risk, not a fresh 1d smash. **flat / two-sided, mild.**

### HORIZON_1W
DGS10 **+19 bp / 1w**, DFII10 **+17 bp / 1w**, XLRE 1w rel **−0.77%**. Unless the long end actually breaks lower after the decision, the 1w lag persists. **down / mild unless dots ease.**

### HORIZON_2W
30Y **5.34** + CRE maturity wall + no cap-rate compression. Structural, not a 2w mean-reversion license. **down / mild.**

### HORIZON_1M
1m rel **−2.42%**, DFII10 **+21 bp / 1m**. Bond-proxy stays a laggard until real yields roll over. DC/industrial occupancy is a 1w–1m dispersion sleeve, **not** an XLRE 1m up vote. **down / mild.**

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 0
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0.5
MULTIPLIER: 0.9
CONFIDENCE: 0.45
REGIME: mixed
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|PARTIAL|0.55|2026-09-16|https://www.tipranks.com/news/stock-market-today-september-16-futures-rise-ahead-of-fed-rate-decision
Risk-off tape / flight to safety|MISS|0.60|2026-09-16|https://www.tipranks.com/news/stock-market-today-september-16-futures-rise-ahead-of-fed-rate-decision
Real yields rising|HIT|0.75|2026-09-14|https://fred.stlouisfed.org/series/dfii10
Real yields falling|MISS|0.70|2026-09-14|https://fred.stlouisfed.org/series/dfii10
USD strengthening|MISS|0.65|2026-09-16|Channel 1 DXY −0.02%
USD weakening|MISS|0.65|2026-09-16|Channel 1 DXY −0.02%
Sector breadth expansion (% names up)|MISS|0.55|2026-09-16|MAP HEAT all sleeves none
Sector breadth failure (ETF up, names flat)|MISS|0.50|2026-09-16|MAP HEAT all sleeves none
Large-cap leadership inside sector|MISS|0.45|2026-09-16|MAP HEAT captains none
Small/mid leadership inside sector|MISS|0.45|2026-09-16|MAP HEAT captains none
High-beta leadership inside sector|MISS|0.45|2026-09-16|MAP HEAT captains none
Low-beta leadership inside sector|PARTIAL|0.40|2026-09-15|Channel 1 1d rel +0.34% vs SPY
Sector ETF inflow / relative volume spike|MISS|0.60|2026-09-15|https://etfdb.com/etf/XLRE/
Sector ETF outflow / volume dry-up|HIT|0.65|2026-09-15|https://etfdb.com/etf/XLRE/
Crowded long (extreme relative performance + valuation)|MISS|0.70|2026-09-15|Channel 1 1m rel −2.42%
Index rebalance / inclusion tailwind|MISS|0.40|2026-09-16|checked, nothing material
Index exclusion / forced selling|MISS|0.40|2026-09-16|checked, nothing material
Rates falling / REIT duration relief|MISS|0.70|2026-09-16|https://www.cnbc.com/2026/09/16/fed-meeting-today-live-updates.html
Data-center REIT demand / rent upside|HIT|0.55|2026-09-16|https://investor.equinix.com/sec-filings/all-sec-filings/content/0001101239-26-000145/0001101239-26-000145.pdf
Industrial REIT occupancy / rent growth|HIT|0.50|2026-09-16|https://finance.yahoo.com/quote/PLD/
Refinancing window opening|MISS|0.65|2026-09-16|https://www.cnbc.com/quotes/US10Y,US30Y
Cap-rate compression|MISS|0.65|2026-09-16|https://tradingeconomics.com/united-states/30-year-bond-yield
Rates rising / REIT selloff|MISS|0.65|2026-09-16|https://www.cnbc.com/2026/09/16/fed-meeting-today-live-updates.html
Office vacancy / mark-to-market stress|HIT|0.60|2026-09-16|https://www.cbre.com/insights/figures/q2-2026-us-office-market-report
Refinancing wall stress|HIT|0.60|2026-09-16|https://www.sterlingassetgroup.com/insights/the-great-refinancing-wall
Cap-rate expansion|PARTIAL|0.45|2026-09-16|https://tradingeconomics.com/united-states/30-year-bond-yield
Sector rotation into REITs|MISS|0.60|2026-09-16|Channel 1 XLRE PM +0.05%
Sector rotation out of real estate|PARTIAL|0.50|2026-09-15|https://www.trefis.com/data/etfs/USRT
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- US 10 year Treasury yield 30 year yield today September 16 2026
- FOMC meeting September 16 2026 rate decision FedWatch hike odds
- XLRE REIT real estate sector premarket flows Equinix Prologis Welltower September 16 2026
- CNBC US 10 year yield 30 year yield live September 16 2026
- stock futures S&P 500 Nasdaq premarket September 16 2026 Fed decision
- REIT XLRE office vacancy data center Equinix industrial Prologis refinancing 2026
- XLRE ETF flows fund flows real estate sector rotation September 2026
- real yields TIPS 10 year DFII10 September 16 2026
- August retail sales September 16 2026 8:30 ET forecast
- Equinix stock premarket September 16 2026 AI data center
- risk on risk off market breadth September 16 2026 REIT rotation
- X search: 10 year Treasury yield 30 year FOMC REIT XLRE September 16 2026 (2026-09-15 to 2026-09-16)
- Fetched: https://www.cnbc.com/2026/09/16/fed-meeting-today-live-updates.html
- Fetched: https://etfdb.com/etf/XLRE/ (403 — unused)

**Key sources and facts taken**
- CNBC Fed live blog (fetched 2026-09-16 ~12:21 UTC): hike >90% / FedWatch; 10Y Tuesday high **5.041%**; 10Y **~4.96%** as of 07:38 ET; decision 14:00 ET; Warsh JH + CPI + oil >$100 as the hike setup. https://www.cnbc.com/2026/09/16/fed-meeting-today-live-updates.html
- CNBC US10Y/US30Y quotes: 10Y **4.99%** (−0.6 bp), 30Y **5.356%** (−0.7 bp) on 9/16. https://www.cnbc.com/quotes/US10Y,US30Y
- Fox Business / QZ / FedWatch wrap: **~92–93%** 25 bp hike to 3.75–4.00%. https://www.foxbusiness.com/economy/stubborn-inflation-sets-stage-federal-reserve-hike-interest-rates
- TipRanks / MarketScreener: futures modestly green into the decision; oil easing but still elevated. https://www.tipranks.com/news/stock-market-today-september-16-futures-rise-ahead-of-fed-rate-decision
- FRED DFII10: **2.60%** as of 2026-09-14; +17 bp 1w / +21 bp 1m (Channel 1 match). https://fred.stlouisfed.org/series/dfii10
- ETFdb / Trefis: XLRE **~−$286M 1m** flows; real-estate ETFs **~−$654M 1m** largest category outflows. https://etfdb.com/etf/XLRE/ · https://www.trefis.com/data/etfs/USRT
- CBRE Q2 2026 office: vacancy **~18.3%**, −30 bp q/q. https://www.cbre.com/insights/figures/q2-2026-us-office-market-report
- CRE maturity wall ~**$875B** 2026. https://www.sterlingassetgroup.com/insights/the-great-refinancing-wall
- EQIX: Q2 2026 guide raise / AI-spend wobble into 9/15; modest 9/16 premarket bounce — **single-name, not XLRE**. https://investor.equinix.com/sec-filings/all-sec-filings/content/0001101239-26-000145/0001101239-26-000145.pdf
- PLD: industrial sleeve; **ex-div $1.07 on 9/16** (mechanical). https://finance.yahoo.com/quote/PLD/
- Retail sales consensus **+0.8% / +0.5% ex-auto**, 08:30 ET 9/16, unprinted at this snapshot. https://www.marketscreener.com/news/august-us-retail-sales-expected-to-rise-by-0-8-up-0-5-excluding-motor-vehicles-ce785bddd18af624
- X (9/15–9/16): 10Y tagged **5.00–5.04%**, 30Y **~5.39%** on Tuesday; FOMC 9/16 14:00 ET; ~92–95% hike odds. Used as color, not as a live open curve (CNBC/Channel 1 preferred).

**Not used / empty**
- Fear & Greed: Channel 1 UNAVAILABLE.
- CME FedWatch scrape: Channel 1 not scrapable; filled via Channel 2 (~92–93%).
- MAP HEAT: all REIT captains **none** — no nested override.
- News Judge: no XLRE/REIT line; rates/Warsh already-priced; ASML/ADBE are not this sector.
- Memory search: disabled (embedding metadata mismatch).

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.5}, 'multiplier': 0.9, 'leading_sum': 0.0, 'divergence_flagged': False, 'total_score': 2.52, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.55, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.1993, 'score': 1.196, 'legs': [{'leg': 'ES', 'pct': 1.14, 'w': 0.5}, {'leg': 'ZN', 'pct': -0.03, 'w': 0.75}, {'leg': 'PM:XLRE', 'pct': 0.05, 'w': 0.7}]}, 'overlay_score': 0.0, 'overlay_raw': 0.0, 'index_carry': 1.324, 'general_total': 5.297, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.45, 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': -0.56, 'w1': -2.06}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
