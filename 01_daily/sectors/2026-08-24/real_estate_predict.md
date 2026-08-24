# Sector Prediction — Real Estate — 2026-08-24

- ETF: **XLRE**
- rubric: `00_grounding/sectors/real_estate.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **-0.9** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLRE vs SPY (yfinance, through 2026-08-24):
  1d: XLRE +0.58% | SPY -0.37% | rel +0.95%
  3d: XLRE +0.78% | SPY -0.81% | rel +1.58%
  1w: XLRE +1.14% | SPY -1.27% | rel +2.41%
  1m: XLRE -1.33% | SPY +3.24% | rel -4.57%
```

MEMORY_CONFIRM: Memory index is unavailable this run (embedding metadata mismatch), so I used the injected Real Estate scoreboard, standing lessons, and last-10 XLRE logs only. Last graded: 2026-08-21 up/notable vs actual 0.0% (dir MISS). Rolling dir=0.625 / mag=0.25 (n=8). Applied: 08-21 level-vs-change (a 3–4 bp dip is not duration relief while 30Y sits near a 19-year high); 08-17 live-rate check (yields are not rising at the open, so do not force a down spine); 08-18 (relative bid is a magnitude cap, not an absolute up); 08-12 / News Judge two-sided rule (Warsh + PCE this week, do not one-way score duration); 08-11 geo/oil does **not** fire (CL/BZ down, not a Hormuz squeeze); 08-14 reconcile band to Σ×mult. No open `sector_real_estate` experiment changes the scores.

## Real Estate (XLRE) — 2026-08-24

### Channel 1 (used as given)
Rates: DGS10 4.69 (1d +0.04 / 1w +0.06), DGS30 5.23 (1d +0.04 / 1m +0.08), DFII10 2.35 (1d 0 / 1w −0.04 / 1m −0.04). VIX 15.91 (+0.78) with VIX/VIX3M ~1.00 backwardation. ES −0.18%, NQ −0.58%. Asia −1.17% (Kospi −3.12%), Europe −0.13%. Oil CL −1.75% / BZ −1.31%. Gold +2.14%. DXY +0.13% 1d / −2.5% 1m. EPU 248 (+60 1d). F&G 55.2. 10Y–SPX 5d corr −0.21.

XLRE vs SPY: 1d +0.58 / −0.37 / **rel +0.95**; 3d rel **+1.58**; 1w rel **+2.41**; 1m rel **−4.57**. Short-term defensive outperformance, still a 1m laggard.

### Channel 2

**1. Shared macro as it hits REITs.** This is not a clean risk-on tape. Futures are soft, Asia is red, VIX is up, gold is bid. It is also not a 08-11/08-18 style oil-shock risk-off: crude is lower on sanctions/hike-odds, not a chokepoint squeeze. The load-bearing event is **Warsh’s first Jackson Hole speech Friday (Aug 28) into PCE (~Aug 26)**. News Judge ranks that as two-sided. Live tape: 10Y ~4.70–4.71% (−3 to −4 bp), 30Y ~5.23% (−4 bp) after a CNBC report that Treasury may use the ~$1T TGA to fund buybacks. That is a **same-morning yield dip at a still-extreme long-end level**, not a regime break. No 8:30 high-impact print today (CFNAI only). For REITs, risk-on usually leaves the sector lagging; today’s tape is the opposite (relative bid vs a soft SPY), but the **30Y still near 2007/19-year highs** is the duration constraint. S0 stays mixed, not a duration-relief plus.

**2. Spine (do not double-count in S0 and S1).**
- Rates falling / REIT duration relief: **not a clean HIT**. A 4 bp morning dip after last week’s long-end equity hit is the exact 08-21 false-positive. 30Y 5.23% is still a stress zone.
- Rates rising / REIT selloff: **not firing at the open** (live yields down). 08-17’s “yields rising → force down” trigger is off.
- Real yields: Channel 1 1d flat, 1w/1m slightly lower. Helpful backdrop, not a fresh impulse.

**3. Secondary.**
- Data-center demand / rent upside: **HIT, stale**. EQIX/DLR raised 2026 guides in July (record bookings, DLR cash renewals +25%). Structural, not a same-day catalyst. Will not let EQIX/DLR or WELL set the ETF call.
- Industrial occupancy / rent growth: **HIT, stale**. PLD Q2 occupancy ~95–95.5%, cash rent change +22%, SSNOI +8.5%.
- Refinancing window / cap-rate compression: **not opening**. Long-end still ~5.23–5.25%; no compression signal.
- Office vacancy / mark-to-market: **HIT**. National vacancy still ~17–19%; office remains the stressed sleeve.
- Refinancing wall: **HIT**. ~$875B–$1.5T CRE/multifamily maturities clustered in 2026 at 6–7% reset rates.
- Rotation into REITs: **price-only**. 1w rel +2.41% vs SPY, but XLRE **5d ~−$173M / 1m ~−$112M outflows** (3m still positive). Flows contradict the relative tape.

**4. Breadth / leadership.** Healthcare/senior (WELL ~11%) and data-center/industrial large-caps are carrying XLRE. Roughly a third of REIT names in an uptrend; office still dead money. That is **large-cap / low-beta leadership**, not breadth expansion. Single-name rule: WELL cannot define XLRE.

**5. Flows / crowding.** Near-term ETF outflows into a +2.4% 1w relative run = unconfirmed rotation. Not a crowded long on 1m (still −4.6% rel).

**6. Earnings / policy.** No fresh REIT print this morning. Policy catalyst is **Warsh + PCE later this week** — two-sided, not scored as already hawkish or already dovish.

### Self-audit
- Lens: duration/rates for a bond-proxy sector, not an SPX beta call.
- Band: factors net ~0; do not emit notable off last week’s relative bounce.
- Skew: hawkish JH prior vs TGA/buyback bid — two-sided, not one-way down.
- Same-shock: the 4 bp dip is counted once as “not durable relief,” not again as S1 duration HIT and S4 thesis.
- Single-ticker: WELL/EQIX/DLR are dispersion, not the ETF call.

### Divergence
Leading (S0+S1+S2+S3) = **−1**. Tape S4 = **+1**. **Flagged.** Trust factors over tape: last week’s relative bid is a **magnitude cushion / relative note**, not an absolute up call (08-18, 08-21). Futures are not confirming a ≥0.5% risk-on follow-through.

**Call implication for the pipeline:** Σ = 0 × 0.9 = **0 → flat / flat**. Absolute XLRE can still print a few tenths either way; the environment is a contained pre-JH standoff, not a duration-relief rally and not a fresh long-end shock.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 0
S2_BREADTH: 0
S3_FLOWS_POSITIONING: -1
S4_ETF_TAPE: 1
MULTIPLIER: 0.9
CONFIDENCE: 0.52
REGIME: mixed
HORIZON_3D: flat:mild:0.50
HORIZON_1W: down:mild:0.48
HORIZON_2W: down:mild:0.45
HORIZON_1M: down:mild:0.50
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|MISS|0.70|2026-08-24|channel1 ES -0.18% NQ -0.58% Asia -1.17%
Risk-off tape / flight to safety|PARTIAL|0.55|2026-08-24|https://www.cnbc.com/2026/08/24/treasury-yields-warsh-jackson-hole.html
Real yields rising|MISS|0.75|2026-08-20|channel1 DFII10 2.35 1d +0.0 1w -0.04
Real yields falling|PARTIAL|0.60|2026-08-20|channel1 DFII10 1w/1m -0.04; 1d flat
USD strengthening|PARTIAL|0.45|2026-08-24|channel1 DXY +0.13% 1d
USD weakening|MISS|0.55|2026-08-24|1d DXY up; 1m still -2.5% is stale
Sector breadth expansion (% names up)|MISS|0.65|2026-08-24|https://pro.stockalarm.io/market/breadth
Sector breadth failure (ETF up, names flat)|PARTIAL|0.55|2026-08-24|XLRE rel bid vs concentrated WELL/EQIX/PLD
Large-cap leadership inside sector|HIT|0.70|2026-08-24|https://www.ssga.com/us/en/intermediary/etfs/state-street-real-estate-select-sector-spdr-etf-xlre
Small/mid leadership inside sector|MISS|0.60|2026-08-24|large-cap/low-beta carry
High-beta leadership inside sector|MISS|0.60|2026-08-24|defensive/low-beta sleeve leading
Low-beta leadership inside sector|HIT|0.65|2026-08-24|channel1 XLRE 1w rel +2.41% vs SPY
Sector ETF inflow / relative volume spike|MISS|0.70|2026-08-21|https://etfdb.com/etf/XLRE
Sector ETF outflow / volume dry-up|HIT|0.70|2026-08-21|https://etfdb.com/etf/XLRE
Crowded long (extreme relative performance + valuation)|MISS|0.60|2026-08-24|1m rel still -4.57%
Index rebalance / inclusion tailwind|MISS|0.40|2026-08-24|checked, nothing material
Index exclusion / forced selling|MISS|0.40|2026-08-24|checked, nothing material
Rates falling / REIT duration relief|MISS|0.72|2026-08-24|https://www.cnbc.com/quotes/US10Y
Data-center REIT demand / rent upside|HIT|0.70|2026-07-29|https://investor.equinix.com/news-events/press-releases/detail/1114/equinix-reports-second-quarter-results-raises-2026
Industrial REIT occupancy / rent growth|HIT|0.68|2026-07|https://www.prnewswire.com/news-releases/prologis-reports-second-quarter-2026-results-302827171.html
Refinancing window opening|MISS|0.65|2026-08-24|30Y still ~5.23%
Cap-rate compression|MISS|0.60|2026-08-24|long-end still at multi-year highs
Rates rising / REIT selloff|MISS|0.70|2026-08-24|live 10Y/30Y -3 to -4 bp at open
Office vacancy / mark-to-market stress|HIT|0.70|2026-06|https://www.yardimatrix.com/blog/us-office-market-outlook-june-2026/
Refinancing wall stress|HIT|0.68|2026-08|https://mmgrea.com/2026-cre-refinancing-wall/
Cap-rate expansion|PARTIAL|0.50|2026-08-24|level still tight; no fresh same-day widening
Sector rotation into REITs|PARTIAL|0.55|2026-08-24|price rel yes; ETF flows no
Sector rotation out of real estate|MISS|0.60|2026-08-24|1d/3d/1w XLRE beating SPY
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- memory_search: Real Estate XLRE REIT duration yields Jackson Hole prediction lessons (index disabled)
- web_search: 10 year 30 year Treasury yield today August 24 2026
- web_search: TIPS real yield 10 year DFII10 August 2026
- web_search: XLRE REIT stocks today real estate sector August 2026
- web_search: Jackson Hole 2026 Warsh speech date inflation data calendar
- web_search: data center REIT Equinix Digital Realty demand August 2026
- web_search: office vacancy REIT refinancing wall 2026
- web_search: XLRE ETF flows inflows outflows August 2026
- web_search: REIT sector breadth Welltower Prologis performance August 2026
- web_search: US 10 year yield live CNBC Treasuries bid Jackson Hole August 24 2026
- web_search: US economic calendar August 24 2026 8:30 ET releases
- web_search: industrial REIT occupancy rent growth Prologis 2026
- web_search: REIT rotation defensive bond proxy August 2026 XLRE
- web_fetch: https://www.cnbc.com/2026/08/24/treasury-yields-warsh-jackson-hole.html
- x_search: XLRE REITs Treasury yields Jackson Hole August 24 2026 (timed out)

**Key sources and facts used**
- Channel 1 panel (injected, unaltered): DFII10 2.35 (1d 0 / 1w −0.04 / 1m −0.04); DGS10 4.69; DGS30 5.23; ES −0.18%; NQ −0.58%; CL −1.75%; BZ −1.31%; GC +2.14%; XLRE/SPY rel +0.95% / +1.58% / +2.41% / −4.57%.
- CNBC, 2026-08-24, 4:14 AM ET, updated ~2h before fetch (https://www.cnbc.com/2026/08/24/treasury-yields-warsh-jackson-hole.html): 10Y −4 bp to 4.7%; 30Y −4 bp to 5.23% after report Treasury could use ~$1T TGA for buybacks; Warsh JH Friday; PCE this week.
- CNBC US10Y quote snapshot ~8:22 AM ET 2026-08-24 (https://www.cnbc.com/quotes/US10Y): ~4.708%, −0.03 from 4.738%; 30Y ~5.239%.
- Trading Economics / Treasury H.15 (https://tradingeconomics.com/united-states/government-bond-yield; Treasury daily curve): official EOD through Aug 21 ~10Y 4.74% / 30Y 5.27%; live ~4.71% / 5.24–5.25%.
- FRED DFII10 (https://fred.stlouisfed.org/graph/?g=1hKmD): 2.35% as of 2026-08-20 (2.41% on Aug 18, 2.44% on Aug 17).
- XTB / Kansas City Fed / Coindesk week-ahead: JH Aug 27–29; Warsh keynote Fri Aug 28 ~10:00 ET; PCE around Aug 26.
- Trading Economics calendar: Aug 24 8:30 ET is CFNAI, not CPI/NFP/PCE.
- Equinix Q2 PR 2026-07-29 (https://investor.equinix.com/news-events/press-releases/detail/1114/equinix-reports-second-quarter-results-raises-2026): raised 2026 growth; record $424M annualized bookings.
- Digital Realty Q2 (https://investor.digitalrealty.com/news-releases/news-release-details/digital-realty-reports-second-quarter-2026-results): record backlog; cash renewal spreads +25.4%.
- Prologis Q2 PR (https://www.prnewswire.com/news-releases/prologis-reports-second-quarter-2026-results-302827171.html): occupancy ~95–95.5%; cash rent +22.3%; cash SSNOI +8.5%.
- Yardi Matrix Jun 2026 office outlook (https://www.yardimatrix.com/blog/us-office-market-outlook-june-2026/): US office vacancy ~17.6% (May 2026).
- MMG / Reed Smith CRE maturity notes: ~$875B+ US CRE/multifamily maturing 2026; broader wall estimates >$1.5T.
- ETFdb XLRE (~Aug 21) (https://etfdb.com/etf/XLRE): 5d ~−$173M, 1m ~−$112M, 3m ~+$364M.
- SSGA XLRE holdings: WELL ~11%, PLD ~9%, EQIX ~7% — concentration reminder.
- News Judge 2026-08-24 (injected): Warsh+inflation two-sided; hawkish prior; last week long-end hit; TGA/yield dip forbids locking hawkish; Iran sanctions with oil down ≠ Hormuz squeeze.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': -1.0, 'S4_ETF_TAPE': 1.0}, 'multiplier': 0.9, 'leading_sum': 0.0, 'divergence_flagged': False, 'total_score': -0.9, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.52, 'regime': 'mixed'}
```
