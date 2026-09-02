# Sector Prediction — Industrials — 2026-09-02

- ETF: **XLI**
- rubric: `00_grounding/sectors/industrials.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **0.0** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLI vs SPY (yfinance, through 2026-09-01):
  1d: XLI -1.37% | SPY -0.69% | rel -0.68%
  3d: XLI -4.22% | SPY -0.56% | rel -3.66%
  1w: XLI -3.50% | SPY -0.22% | rel -3.28%
  1m: XLI -3.95% | SPY +1.97% | rel -5.93%
```

MEMORY_CONFIRM: Memory index unavailable this run (embedding metadata mismatch; `openclaw memory index --force` would rebuild). Used injected Industrials scoreboard + 08-11..09-01 sector logs only. Rolling dir=0.2 / mag=0.1 (n=10); last 30 dir=0.25 / mag=0.083 (n=12). Last graded 08-28: narrative down/mild vs pipeline down/flat, actual XLI −0.93% (dir HIT, mag MISS on pipeline flat). 08-31 and 09-01 still ungraded (09-01 was down/mild into XLI −1.37% / rel −0.68%). Governing today: 08-11/08-12 (verify live oil; no severe-up; S0 ≤ 0 only if oil is confirmed up/risk-off — live WTI/Brent are only +0.2–0.4%, not a new squeeze); 09-01 kinetic Hormuz increment does **not** fire (stalemate / reopen-terms, not a fresh tanker strike); 08-13 does **not** fire (oil is not a demand-side dump); 08-18 (do not hold S1 at +2 on ISM; do not use GEV/ETN as a cushion; reconcile narrative vs components); 08-21 reversal **off** (ES +0.06%, NQ −0.12% — not ≥ +0.3%); 08-25 needs a *fresh* single-name smash plus all-timeframe lag to force down:mild — SPEEA is stale, CAT/GEV/BA are not breaking premarket; 08-27 (1w/1m laggard → forbid up; cap S1 at 0/+1); 08-28 Industrials (do **not** import XLF leftover-S2/S4 down-bans as a *new* factor lesson, but also do **not** restack a completed 09-01 smash into S2+S3+S4 when S0=S1=0 and ES is flat). DO-INSTEAD: when score fights tape, cut conviction / prefer flat/mild. Open experiment: keep direction, shrink confidence on modest |score| — **applied** (Σ ≈ 0).

## XLI near-session environment (not an SPX call)

### 1. Shared macro as it hits Industrials — S0 = 0
This is a **mixed/pause tape**, not 09-01’s confirmed cyclical risk-off.

- **Futures do not independently confirm.** Channel 1: ES **+0.06%**, NQ **−0.12%**. Finviz cash futures are a bit softer (SPX −0.23% / NQ −0.54%) but still inside a flat band. 08-21’s ES/NQ ≥ +0.3% bounce gate is **off**. 08-10-style flat-futures rule: do not let a moderate headline stack manufacture a mild-down without futures confirmation.
- **Oil: live-verify, do not trust the 1d CL column.** Channel 1 `CL=F −1.16% / BZ=F −1.01%` is the prior-close sleeve. Live Finviz: WTI **$90.47 (+0.22%)**, Brent **$94.97 (+0.38%)**. Web tape is ~$90 / ~$95, not a fresh +2% squeeze. News Judge: US–Iran **stalemate / Hormuz reopen terms** — mixed, and the 09-01 kinetic increment is explicitly **not** live. Elevated **level** is a cost headwind for transports/manufacturers; the **session change** is not a new S0 shock. Count oil **once**, here, as non-confirming.
- **Asia is the red sleeve, not a license for S0 = −1.** Composite **−1.77%** (Nikkei −2.85%, Kospi **−3.99%**). Europe **−0.12%**. 08-03: do not let a single Asia crash set direction when Europe + US futures do not confirm.
- **Rates: do not score the prior-close 1d column as live.** FRED through 08-31: DGS10 4.75 (+2 bp), DFII10 2.44 (+2 bp), 30Y 5.25. Live Finviz: 10Y note **−0.06%**, 30Y **−0.23%** — a small open easing, not a second-day real-yield spike. Warsh/hike-odds repricing is **already paid** (08-28 → 09-01). Do not double-count it.
- **ADP is out (8:15 ET), not pending.** Private payrolls **+38k vs ~47k**, manufacturing **−17k**. Soft growth print, not a crash; HY OAS still **2.63** (tight); VIX **16.09** with mild backwardation. Bad-news-good for duration is **not** an industrials tailwind. Do not one-way score S0 from ADP (that belongs as a sector-employment footnote under S1, and it is not enough to flip the spine).
- Calendar still open: **Factory orders 10:00 ET** (consensus ~+0.6% after June −0.3%) and Beige Book 14:00 — two-sided, **unscored until print**.

**S0 = 0, regime mixed.** Not −1: futures are not ≤ −0.5%, oil is not independently spiking, Warsh is stale, Asia is an outlier. Not +1: NQ is soft, 1w/1m XLI is a laggard, no cyclical risk-on. 08-27 still forbids mapping leftover XLK/NVDA beta into XLI S0 = +1.

### 2. Spine + secondary — S1 = 0 (capped)
**August ISM already printed 09-01 (10:00 ET) and is in yesterday’s tape.** PMI **54.6** vs ~55.2 expected vs July 55.6; new orders **53.7** (down 3.0 pts from 56.7). Still **expansion** (8th month) — not an ISM-contraction HIT. New orders cooled; prices 71.1 still hot. XLI **already paid** this (09-01 −1.37% / rel −0.68%). Do **not** pre-score it again as today’s binary, and do **not** keep S1 at +2 on a slowing survey (08-18/08-27).

- **Durable goods / CapEx:** July durables already +1.1% / core +0.2% (08-26). Today’s factory-orders revision is **pending** — two-sided, not a HIT.
- **Grid / AI power — HIT, carried.** GEV ~$176B RPO / 116 GW gas book remains structural. Premarket GEV ~flat, ETN ~flat-to-soft. 08-18: **not** a downside cushion.
- **Aerospace & defense — MIXED / stale.** F-15 **ceiling** already faded. SPEEA rejected/authorized in August; talks resume **Sep 8**; earliest strike **Oct 6**. Premarket BA **~+0.7%**. Do not cancel ISM with one award; do not treat geo as a fresh defense-order HIT.
- **Freight — MIXED.** AAR rail/intermodal still expanding on lagged prints; Cass July shipments **−4.8% y/y** (volume recession in trucking, rates up on capacity). Not a same-morning recovery HIT.
- **Construction slowdown — HIT, already printed 09-01.** Census: July SAAR **$2,157.6B**, **−0.5% m/m**, **−3.8% y/y**; residential −1.3% m/m, private nonres +0.4%. AI/nonres is the offset, not a broad build boom. In the tape; not a second S1 vote today.
- **ADP manufacturing −17k** is the only **fresh** same-morning industrial-employment miss. It offsets leftover ISM-expansion credit. It is **not** ISM contraction and does not get its own taxonomy HIT.

Net: slowing-but-still-expansion ISM vs construction drag vs mixed freight vs carried grid vs **no fresh same-morning confirmation**. **S1 = 0.** +2 is forbidden. −1 would require ISM <50, a fresh CapEx cut, or a live hard-data miss that is not already in yesterday’s close.

### 3. Breadth — S2 = 0
XLI is still a **1w/1m laggard** (rel −3.28% / −5.93%). That is **history**, scored in the 09-01 session. Premarket CAT ~flat, GEV ~flat, ETN slight red, BA slight green, XLI ~**−0.19%** on low volume — **not** a live large-cap breakdown. Do not copy 09-01 internals into S2 as an independent down vote (the leftover-tape error that 08-28 XLF/XLY/XLP punished). Mega-name AI-power/defense carry is **not** expanding % of names. **S2 = 0.**

### 4. Flows — S3 = 0
ETFdb: ~**+$111–151M** over 1m vs **5-day ~−$433M** (including the 08-27 ~−$370M day). Not a crowded long (1m rel **−5.93%**). Trailing unit outflows are **not** a 1-day lid. No same-morning inflow spike. Rotation **out of industrials** is the multi-day relative tape, counted at most as a HIT_GRID label, **not** restacked in S3. **S3 = 0.**

### 5. ETF tape (confirmation only) — S4 = 0
Channel 1 through **09-01**: 1d rel **−0.68%**, 3d **−3.66%**, 1w **−3.28%**, 1m **−5.93%**. Decisive **prior** lag. S4 confirms **this** session, not the prior close. Premarket XLI is inside a flat band; ES is unconfirmed. Do not re-vote yesterday’s 1d rel as a full S4 down. **S4 = 0.** Relative lag remains a **relative** note (XLI can still lag SPY on a flat absolute day) — it is not an absolute-down forecast.

### 6. Earnings / policy
No index-wide industrials earnings cluster at the open. AME/Indicor is **08-26 stale M&A**. ADSK/ADI/APH are **not** XLI spines. Factory orders 10:00 is the sector-owned print still outstanding — consensus **above** prior, so **no** 08-25-style pre-score downside tilt. Beige Book is anecdotal, two-sided.

### Self-audit
- **Lens:** cyclical; ISM/CapEx outrank duration. Rates used only as a non-event in S0 (live notes slightly easier, not a fresh DFII shock).
- **Band:** Σ(S0..S4) = **0** × 0.9 = **0** → **flat / flat**. Mag accuracy 0.1 forbids notable. Narrative and components **agree** (08-18/08-28 reconcile). Do not let a pipeline rewrite this to down/flat or up/flat.
- **Skew:** GEV/BA/CAT do not drive the ETF call. Premarket BA green is single-ticker, not a defense-order HIT.
- **Same-shock:** Hormuz/oil counted once in S0 as **non-confirming**. Not re-used in S1. Warsh counted **zero** (already paid). ISM counted as **carried expansion, not today’s print**.
- **Divergence:** leading sum **0** vs S4 **0**. Prior 1w/1m lag would fight a *down* call if we copied it into S4; we did not. **divergence_flagged = False.** Trust factors over leftover tape.
- **08-27 up-ban:** still on (1w/1m laggard). Residual is **flat**, not down, because S0=S1=0 and the bounce gate is off but so is a fresh hard-data/oil confirmation.
- **Open experiment:** modest |score| → confidence **0.52**, not 0.7.

Checked Channel 2 categories: breadth/leadership, FTS/defensive rotation, real yields both ways, USD, XLI flows, ISM/durables/grid/defense/freight/reshoring, construction, ADP/factory-orders calendar. Empty sleeves stated as such.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 0
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.52
REGIME: mixed
DIVERGENCE_FLAGGED: False
HORIZON_3D: down:mild:0.55
HORIZON_1W: down:mild:0.52
HORIZON_2W: flat:mild:0.48
HORIZON_1M: flat:mild:0.45
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|MISS|0.70|2026-09-02|https://finance.yahoo.com/markets/stocks/articles/stock-market-news-sep-2-095800348.html
Risk-off tape / flight to safety|MISS|0.62|2026-09-02|https://finance.yahoo.com/markets/stocks/articles/stock-market-news-sep-2-095800348.html
Real yields rising|MISS|0.65|2026-09-02|https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/august/
Real yields falling|MISS|0.58|2026-09-02|https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/august/
USD strengthening|MISS|0.55|2026-09-02|https://finance.yahoo.com/markets/stocks/articles/stock-market-news-sep-2-095800348.html
USD weakening|MISS|0.55|2026-09-02|https://finance.yahoo.com/markets/stocks/articles/stock-market-news-sep-2-095800348.html
Sector breadth expansion (% names up)|MISS|0.70|2026-09-02|https://www.benzinga.com/etfs/sector-etfs/26/09/61546291/leading-and-lagging-sectors-september-1-2026
Sector breadth failure (ETF up, names flat)|MISS|0.60|2026-09-02|https://www.benzinga.com/etfs/sector-etfs/26/09/61546291/leading-and-lagging-sectors-september-1-2026
Large-cap leadership inside sector|MISS|0.58|2026-09-02|https://public.com/stocks/cat/pre-market
Small/mid leadership inside sector|MISS|0.50|2026-09-02|https://www.thetrading.tools/sector-health
High-beta leadership inside sector|MISS|0.55|2026-09-02|https://www.thetrading.tools/sector-health
Low-beta leadership inside sector|MISS|0.50|2026-09-02|https://www.thetrading.tools/sector-health
Sector ETF inflow / relative volume spike|MISS|0.60|2026-09-02|https://etfdb.com/etf/XLI/
Sector ETF outflow / volume dry-up|MIXED|0.55|2026-09-02|https://etfdb.com/etf/XLI/
Crowded long (extreme relative performance + valuation)|MISS|0.70|2026-09-02|https://etfdb.com/etf/XLI/
Index rebalance / inclusion tailwind|MISS|0.40|2026-09-02|https://www.ssga.com/us/en/intermediary/etfs/state-street-industrial-select-sector-spdr-etf-xli
Index exclusion / forced selling|MISS|0.40|2026-09-02|https://www.ssga.com/us/en/intermediary/etfs/state-street-industrial-select-sector-spdr-etf-xli
ISM manufacturing / new orders expansion|HIT|0.80|2026-09-01|https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/august/
Durable goods / CapEx upside|MISS|0.55|2026-09-02|https://tradingeconomics.com/united-states/factory-orders
Grid / electrical equipment backlog (AI power)|HIT|0.70|2026-09-02|https://www.ssga.com/us/en/individual/insights/sector-market-perspectives-q3-2026
Aerospace & defense order / budget upside|MISS|0.60|2026-09-02|https://www.seattletimes.com/business/boeing-aerospace/boeing-speea-will-resume-negotiations-sept-8/
Freight / trucking / rail volume recovery|MIXED|0.58|2026-09-02|https://www.aar.org/rail-industry-overview/
Reshoring / industrial policy funding|MISS|0.45|2026-09-02|https://www.ssga.com/us/en/individual/insights/sector-market-perspectives-q3-2026
ISM contraction|MISS|0.85|2026-09-01|https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/august/
CapEx cuts / order cancellation|MISS|0.55|2026-09-02|https://tradingeconomics.com/united-states/factory-orders
Freight recession|MISS|0.55|2026-09-02|https://finance.yahoo.com/markets/commodities/articles/truckload-linehaul-rates-rip-higher-133252088.html
Construction slowdown|HIT|0.78|2026-09-01|https://www.census.gov/construction/c30/current/
Sector rotation into industrials|MISS|0.70|2026-09-02|https://www.benzinga.com/etfs/sector-etfs/26/09/61546291/leading-and-lagging-sectors-september-1-2026
Sector rotation out of industrials|HIT|0.66|2026-09-02|https://www.benzinga.com/etfs/sector-etfs/26/09/61546291/leading-and-lagging-sectors-september-1-2026
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- ISM manufacturing August 2026 PMI new orders
- Brent crude oil price Hormuz Iran September 2 2026
- US economic calendar September 2 2026 ISM construction durable goods
- XLI industrials ETF flows GE Vernova Boeing Caterpillar September 2026
- US construction spending July 2026 Census
- GE Vernova Eaton Caterpillar Boeing stock premarket September 2 2026
- ADP employment August 2026
- XLI ETF premarket September 2 2026 industrials sector
- freight rail trucking Cass AAR September 2026
- WTI crude oil live price September 2 2026
- US factory orders July 2026 forecast
- August 2026 ISM manufacturing PMI 54.6 industrials stocks reaction
- sector rotation industrials XLI September 2 2026
- Boeing SPEEA strike September 2026
- X search: XLI industrials GE Vernova Caterpillar Boeing oil Hormuz September 2 2026
- web_fetch: https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/august/ (extraction failed)

**Key sources and facts taken**
- ISM, Aug 2026 PMI (released 2026-09-01): PMI 54.6 (vs 55.6 July, ~55.2 cons); new orders 53.7 (−3.0); production 58.3; employment 51.2; prices 71.1. Still expansion, not contraction. https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/august/
- Census construction (CB26-140, 2026-09-01): July SAAR $2,157.6B, −0.5% m/m, −3.8% y/y; residential −1.3% m/m; private nonres +0.4%. https://www.census.gov/construction/c30/current/
- ADP (2026-09-02 8:15 ET): +38k private jobs vs ~47k; manufacturing −17k. https://www.cnbc.com/2026/09/02/private-payrolls-rose-by-38000-in-august-fewer-than-expected-adp-reports.html
- Calendar 2026-09-02: factory orders 10:00 ET (cons ~+0.6%); Beige Book 14:00; ISM/construction already 09-01. https://www.scotiabank.com/ca/en/about/economics/economics-publications/post.other-publications.calendar-of-economic-release-dates.calendar-of-economic-release-dates--september-2026-.html
- Live oil ~WTI $90 / Brent $95, small green; Hormuz as stalemate/reopen-terms not a fresh kinetic print. Channel 1 Finviz WTI +0.22% / Brent +0.38%; web range $89–$95.
- XLI premarket ~$172.41 (−0.19%) vs 09-01 close $172.73 (−1.37%). https://www.marketwatch.com/investing/fund/xli/charts
- Premarket leaders: CAT ~flat, GEV ~flat, ETN slight red, BA ~+0.7%. public.com / stockanalysis snapshots.
- XLI flows: 1m ~+$111–151M, 5d ~−$433M. https://etfdb.com/etf/XLI/
- SPEEA: no active strike; talks 09-08; expire 10-06. https://www.seattletimes.com/business/boeing-aerospace/boeing-speea-will-resume-negotiations-sept-8/
- Cass July shipments −4.8% y/y; rail/intermodal still the firmer leg (AAR lagged). Freight MIXED, not a freight-recession HIT.
- 09-01 tape: industrials lagged, energy led on the oil spike; ISM described as noise vs oil. https://www.benzinga.com/markets/market-summary/26/09/61554095/crude-oil-surges-3-ism-manufacturing-pmi-falls-in-august
- X: no incremental GEV/CAT/BA–Hormuz link on 09-02; 09-01 was the oil/yield risk-off session already in the cash close.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': 0.0, 'divergence_flagged': False, 'total_score': 0.0, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.52, 'regime': 'mixed'}
```
