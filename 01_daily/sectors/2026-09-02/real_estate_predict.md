# Sector Prediction — Real Estate — 2026-09-02

- ETF: **XLRE**
- rubric: `00_grounding/sectors/real_estate.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-5.85** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLRE vs SPY (yfinance, through 2026-09-01):
  1d: XLRE -0.16% | SPY -0.69% | rel +0.53%
  3d: XLRE -1.39% | SPY -1.21% | rel -0.18%
  1w: XLRE -2.91% | SPY -0.54% | rel -2.37%
  1m: XLRE -2.52% | SPY +0.54% | rel -3.07%
```

MEMORY_CONFIRM: Memory index unavailable this run (embedding metadata mismatch) — used injected Real Estate scoreboard, standing REIT lessons, and last-10 XLRE logs only. Last graded: 2026-08-28 down/mild vs XLRE −0.403% / SPY −0.227% / rel −0.176% (dir HIT, mag HIT). 08-31 and 09-01 down/mild still ungraded. Rolling dir=0.5 mag=0.5 (n=10). Applied: **08-25** live-curve (CNBC US10Y ~4.81 vs FRED 4.75 — **rising**, oil-slide “don’t force down” **off**); **08-27** (08-25 is not an up license; 30Y still in the stress zone; do not pad S1 with always-on DC/industrial; leftover NQ beta ≠ duration relief); **08-21** level-vs-change (DGS30 5.25 / 30Y futures −0.23% is not relief); **08-17/08-18** live long-end + hawkish path → absolute **down**, 1d relative bid is a **mag cap only**; **08-11** geo/oil does **not** add a second S0 hit (News Judge: today’s Hormuz is stalemate/reopen-terms, not a fresh tanker increment; live WTI/Brent only +0.2–0.4%); **08-12** two-sided labor (ADP 38k already out) does **not** flip S0 — no easing tell on the live curve; **08-28** do not import leftover-S2/S4 down-bans, but also do not restack 1w lag on top of a **+0.53% 1d rel** cushion; **08-14** reconcile to signed Σ. Open `sector_real_estate` experiment: keep direction, shrink confidence; 1d absolute (−0.16%) does not fight the down lean.

## Real Estate (XLRE) — 2026-09-02

### Channel 1 (used as given, not re-derived)
Rates through **2026-08-31**: DGS10 **4.75** (1d **+0.02** / 1w **+0.05**), DGS30 **5.25** (1d **+0.03** / 1w **+0.02**), DFII10 **2.44** (1d **+0.02** / 1w **+0.06** / 1m **−0.03**) — **real yields up on 1d/1w**. VIX **16.09** (1d −0.25) with VIX/VIX3M **1.053 backwardation**. ES=F **+0.06%**, NQ=F **−0.12%**; Finviz ES **−0.23%** / NQ **−0.54%**. Asia composite **−1.77%** (Kospi −3.99%, Nikkei −2.85%). Europe **−0.12%**. Finviz WTI **$90.47 (+0.22%)**, Brent **$94.97 (+0.38%)**; Channel 1 CL=F **−1.16%** / BZ=F **−1.01%** is the **prior-close** column — live oil is firm, not an 08-25 slide. Gold **−0.92%**. DXY **+0.15%**. 10Y note **−0.06%**, **30Y bond −0.23%**. HY OAS **2.63**. 10Y–SPX 5d corr **−0.661**.

XLRE vs SPY through **2026-09-01**: 1d **−0.16 / −0.69 / rel +0.53**; 3d rel **−0.18**; 1w rel **−2.37**; 1m rel **−3.07**. **1d is a defensive relative cushion; 1w/1m remain laggards.** Confirmation mix, not a duration-relief thesis.

### Channel 2

**1. Shared macro as it hits REITs.** This is a duration overlay, not a flight-to-safety bid into REITs. Live 10Y (CNBC) **~4.81%** (intraday high 4.814%) vs FRED 4.75 — **verified rising**, so 08-25’s falling-curve branch is **off**. 30Y still **5.25** in the multi-decade stress zone; 30Y futures **−0.23%**. Warsh (8/28) is **already hawkish**: CME FedWatch **~66%** Sep hike (not a two-sided speech still in the future — 08-12 parking **off**). Hormuz is **live as a stalemate/reopen-terms binary**, not a same-morning tanker increment (News Judge). Oil holds **~$90 / ~$95** with only a +0.2–0.4% live tick — inflation overlay, **not** an 08-11 spike to re-score on top of rates. Gold is **down**; no bond-proxy FTS. ADP **+38k vs ~47k** (8:15 ET, already out) is two-sided for duration, but **2Y note −0.01% and 10Y still backing up** — no 08-12 easing tell, so do not flip S0. Factory Orders 10:00 / Beige Book 14:00 are mid-tier, not CPI. Green-futures reversal **does not fire** (ES inside ±0.3%). NQ softer than ES is XLK beta, not REIT relief (08-27).

**2. Spine (count the rate shock once in S1; S0 is the regime map, not a second copy of the same backup).**
- Rates falling / REIT duration relief: **MISS**. Live curve is up; 30Y ≥5.15%.
- Rates rising / REIT selloff: **HIT**. Live 10Y ~4.81, 30Y 5.25, 30Y futures −0.23%, hike odds ~66%.
- Real yields rising: **HIT** (DFII10 +2 bp 1d / +6 bp 1w). Same duration channel as the nominal backup — **not** a second independent shock.

**3. Secondary.**
- Data-center REIT demand / rent upside: **HIT, stale**. EQIX/DLR guides already raised in July; EQIX **−1.83%** on 9/1. Equinix Fabric One (same-morning product note) is **single-name/DC sleeve**, not an XLRE duration vote. **08-27: not a same-day up vote.** EQIX/DLR must not define XLRE.
- Industrial REIT occupancy / rent growth: **HIT, stale** (PLD quality sleeve). Same rule. PLD ~flat-to-down 9/1.
- Refinancing window / cap-rate compression: **MISS**. Long end ~5.25%; no compression.
- Office vacancy / mark-to-market: **HIT, small sleeve**. CBRE Q2 vacancy **18.3%** (down 30 bp — modest heal, still stressed). Office ~1% of XLRE (BXP). Do not let office set the ETF.
- Refinancing wall: **HIT, structural**. MBA **~$875B** 2026 CRE/multifamily maturities; CBRE **~$131B** office funding gap. Not a same-morning print.
- Sector rotation out of real estate: **HIT on 1w/1m price** (see S2/S4). Do not also dump it into S1.

**4. Breadth / leadership.** 9/1 cash: **WELL +1.77%**, EQIX **−1.83%**, PLD ~**−0.2%**, AMT **+0.18%**, XLRE **−0.16%**. WELL (top weight) did **not** carry the ETF — the opposite of a WELL-defines-XLRE error. That is **large-cap idiosyncratic**, not breadth expansion. 1d rel **+0.53%** is a defensive cushion vs SPY, not % names expanding. WELL cannot set the call.

**5. Flows / positioning.** ETFdb: XLRE **5d −$173M**, **1m −$112M**, 3m still **+$364M**. Near-term outflow into a −2.4% 1w / −3.1% 1m relative laggard = unconfirmed washout, **not** a crowded long. Not a same-day volume spike; trailing 5d is demand-soft, not a 1-day lid. Score once in S3, lightly.

**6. Earnings / policy.** No fresh REIT print that moves the book. Dominant objects are **already-printed Warsh hawkish path** + **live long-end backup**. ADP is the only same-morning hard print — two-sided, **not transmitting** to lower yields. ISM Services is **tomorrow**; CPI **Sep 11**; FOMC **Sep 15–16**. Calendar-check passed: no 8:30 CPI today.

### Self-audit
- Lens: duration/rates for a bond-proxy, not an SPX beta call.
- Band: factors net modestly negative; **do not emit notable** (ES flat, not an 08-18 30Y-smash + sharply negative futures analog).
- Skew: soft ADP vs hawkish path — labor is two-sided; live curve still up, so no S0 flip to 0/+.
- Same-shock: the 10Y backup is the S1 spine; S0 is the hawkish/Asia/NQ-soft regime map; S4 is **not** a third copy. Oil is **not** re-scored on top of rates.
- Single-ticker: WELL’s +1.77% and EQIX Fabric One **do not** define XLRE.

### Divergence
Leading (S0+S1+S2+S3) = **−3**. Tape S4 = **0**. **Not flagged.** 1d rel **+0.53%** is the 08-18 **magnitude cushion / relative note**, not an absolute up signal. Trust the rate spine over the one-day relative bid. Absolute XLRE can still print a few tenths either way; the environment is a contained post-Warsh duration grind, not a crash and not duration relief.

**Call implication for the pipeline:** Σ = (−1−1+0−1+0) × 0.9 = **−2.7 → down / mild**. Do not promote to notable. Do not flatten to flat solely because of the 1d relative cushion (absolute 1d was still −0.16%).

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: -1
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.52
REGIME: mixed
HORIZON_3D: down:mild:0.52
HORIZON_1W: down:mild:0.54
HORIZON_2W: down:mild:0.50
HORIZON_1M: down:mild:0.48
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|MISS|0.72|2026-09-02|https://www.cnbc.com/2026/09/02/private-payrolls-rose-by-38000-in-august-fewer-than-expected-adp-reports.html
Risk-off tape / flight to safety|MISS|0.58|2026-09-02|https://www.cnbc.com/2026/09/02/bond-yields-treasurys-inflation.html
Real yields rising|HIT|0.80|2026-09-02|https://fred.stlouisfed.org/series/DFII10
Real yields falling|MISS|0.80|2026-09-02|https://fred.stlouisfed.org/series/DFII10
USD strengthening|HIT|0.55|2026-09-02|https://www.cnbc.com/quotes/DXY
USD weakening|MISS|0.55|2026-09-02|https://www.cnbc.com/quotes/DXY
Sector breadth expansion (% names up)|MISS|0.70|2026-09-01|https://www.cnbc.com/quotes/XLRE
Sector breadth failure (ETF up, names flat)|MISS|0.65|2026-09-01|https://www.cnbc.com/quotes/XLRE
Large-cap leadership inside sector|HIT|0.62|2026-09-01|https://www.cnbc.com/quotes/WELL
Small/mid leadership inside sector|MISS|0.60|2026-09-01|https://www.cnbc.com/quotes/XLRE
High-beta leadership inside sector|MISS|0.60|2026-09-01|https://www.gurufocus.com/stock/EQIX/article
Low-beta leadership inside sector|HIT|0.58|2026-09-01|https://www.cnbc.com/quotes/WELL
Sector ETF inflow / relative volume spike|MISS|0.70|2026-09-02|https://etfdb.com/etf/XLRE
Sector ETF outflow / volume dry-up|HIT|0.68|2026-09-02|https://etfdb.com/etf/XLRE
Crowded long (extreme relative performance + valuation)|MISS|0.75|2026-09-01|
Index rebalance / inclusion tailwind|MISS|0.50|2026-09-02|
Index exclusion / forced selling|MISS|0.50|2026-09-02|
Rates falling / REIT duration relief|MISS|0.85|2026-09-02|https://www.cnbc.com/2026/09/02/bond-yields-treasurys-inflation.html
Data-center REIT demand / rent upside|HIT|0.60|2026-09-02|https://za.investing.com/news/stock-market-news/mizuho-sees-upside-for-data-center-reits-amid-state-pushback-93CH-4449751
Industrial REIT occupancy / rent growth|HIT|0.55|2026-09-01|https://za.investing.com/equities/prologis
Refinancing window opening|MISS|0.70|2026-09-02|https://www.cbre.com/insights/figures/q2-2026-us-office-market-report
Cap-rate compression|MISS|0.70|2026-09-02|https://www.cbre.com/insights/figures/q2-2026-us-office-market-report
Rates rising / REIT selloff|HIT|0.82|2026-09-02|https://www.cnbc.com/2026/09/02/bond-yields-treasurys-inflation.html
Office vacancy / mark-to-market stress|HIT|0.60|2026-09-02|https://www.cbre.com/insights/figures/q2-2026-us-office-market-report
Refinancing wall stress|HIT|0.62|2026-09-02|https://www.bloomberg.com/news/articles/2026-02-09/property-debt-s-maturity-wall-eases-as-875-billion-comes-due
Cap-rate expansion|HIT|0.55|2026-09-02|https://www.cbre.com/insights/figures/q2-2026-us-office-market-report
Sector rotation into REITs|MISS|0.70|2026-09-01|
Sector rotation out of real estate|HIT|0.70|2026-09-01|
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- US 10 year Treasury yield 30 year TIPS real yield September 2 2026
- XLRE REIT stocks today real estate sector September 2 2026
- economic calendar September 2 2026 CPI PPI ISM Fed
- Kevin Warsh Jackson Hole September hike odds 2026 Treasury yields
- ADP employment August 2026 38k jobs report September 2
- Hormuz Iran oil tanker September 2 2026 Brent WTI
- XLRE ETF flows WELL PLD EQIX AMT premarket September 2 2026
- office vacancy REIT refinancing wall 2026 CBRE
- US 10 year yield today CNBC 4.80 September 2 2026
- data center REIT Equinix Digital Realty September 2026
- XLRE WELL Prologis Equinix American Tower stock today
- CME FedWatch September 2026 hike probability September 2
- site:etfdb.com XLRE fund flows
- Fetches: CNBC US10Y, CNBC US30Y (quote pages unusable), CNBC ADP article

**Key sources (title + URL + timestamp / as-of)**
- CNBC — Private payrolls rose by 38,000 in August, fewer than expected, ADP reports — https://www.cnbc.com/2026/09/02/private-payrolls-rose-by-38000-in-august-fewer-than-expected-adp-reports.html — 2026-09-02
- ADP Media Center — Private-Sector Employment Increased by 38,000 Jobs in August — https://mediacenter.adp.com/2026-09-02-ADP-National-Employment-Report-Private-Sector-Employment-Increased-by-38,000-Jobs-in-August — 2026-09-02
- CNBC — Bond yields / Treasurys (US10Y ~4.81%) — https://www.cnbc.com/2026/09/02/bond-yields-treasurys-inflation.html — 2026-09-02
- Fool — Odds of Sept rate hike doubled after Warsh — https://www.fool.com/investing/2026/09/01/odds-sept-rate-hike-doubled-fed-chair-kevin-warsh/ — 2026-09-01
- Reuters commentary — Warsh scored an easy win at Jackson Hole — https://www.reuters.com/commentary/reuters-open-interest/warsh-scored-an-easy-win-jackson-hole-hard-work-starts-now-2026-09-01/ — 2026-09-01
- Politico — Warsh Jackson Hole speech — https://www.politico.com/news/2026/08/28/warsh-speech-jackson-hole-fed-rates-01053899 — 2026-08-28
- The National — Oil surges above $95 on renewed US-Iran fighting — https://www.thenationalnews.com/business/energy/2026/09/02/oil-surges-above-95-on-renewed-us-iran-fighting-and-hormuz-disruption-concerns/ — 2026-09-02
- World Oil — Two oil tankers reportedly struck (Sep 1) — https://worldoil.com/news/2026/9/1/two-oil-tankers-reportedly-struck-as-strait-of-hormuz-hostilities-resume/ — 2026-09-01
- NYT — Ships / oil / Hormuz — https://www.nytimes.com/2026/09/01/business/ships-oil-hormuz-iran-war.html — 2026-09-01
- CBRE Q2 2026 US Office Market Report — https://www.cbre.com/insights/figures/q2-2026-us-office-market-report — Q2 2026
- Bloomberg — $875B CRE maturity wall — https://www.bloomberg.com/news/articles/2026-02-09/property-debt-s-maturity-wall-eases-as-875-billion-comes-due — 2026
- ETFdb XLRE — https://etfdb.com/etf/XLRE — flows as listed
- CNBC XLRE quote — https://www.cnbc.com/quotes/XLRE — through 2026-09-01 close
- FedRateCalc / TradingCharts economic calendar Sep 2 2026 — no CPI/PPI/ISM today; ADP 8:15, Factory Orders 10:00, Beige Book 14:00
- Mizuho / data-center REITs — https://za.investing.com/news/stock-market-news/mizuho-sees-upside-for-data-center-reits-amid-state-pushback-93CH-4449751 — early Sep 2026

**Facts taken**
- Live 10Y ~4.80–4.81% (CNBC 4.81, high 4.814%) vs Channel 1 FRED 4.75 — curve **rising**, not an 08-25 second-day decline.
- 30Y FRED 5.25; 30Y futures −0.23% — still in the stress zone.
- DFII10 2.44, +2 bp 1d / +6 bp 1w — real yields rising on the short horizons that dominate REITs.
- Sep hike odds ~66% (FedWatch snapshot Sep 2); Warsh already printed hawkish 8/28.
- ADP +38k vs ~47k consensus, slowest since Jan 2026; pay still +3.2% base / +4.7% gross — soft jobs, **not** a transmitted yield rally.
- No CPI/PPI/ISM on Sep 2; Factory Orders 10:00, Beige Book 14:00, NFP Friday.
- Live oil: Finviz WTI $90.47 (+0.22%), Brent $94.97 (+0.38%). Sep 1 tanker strikes are **T-1**; today’s News Judge object is stalemate/reopen-terms.
- XLRE 9/1 close $44.04 (−0.16%); WELL +1.77%, EQIX −1.83%, PLD ~−0.2%, AMT +0.18%.
- XLRE flows: 5d −$173M, 1m −$112M, 3m +$364M.
- CBRE office vacancy 18.3% (−30 bp q/q); MBA ~$875B 2026 CRE maturities; office funding gap ~$131B — structural, not same-morning.
- DC/industrial demand intact (Mizuho Outperform, raised 2026 guides) but **stale / sleeve** — EQIX down 1.83% on 9/1, so not an ETF up vote.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': -1.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': -5.0, 'divergence_flagged': False, 'total_score': -5.85, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.52, 'regime': 'mixed'}
```
