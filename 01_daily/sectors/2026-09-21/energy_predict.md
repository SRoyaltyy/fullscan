# Sector Prediction — Energy — 2026-09-21

- ETF: **XLE**
- rubric: `00_grounding/sectors/energy.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-6.269** (mult 0.85)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **-5.237** (CL -1.59%, QA -1.02%, ES +1.35%, PM:XLE -1.29%) · index_carry **3.218** (general 12.871) · llm_overlay **-4.25** (raw -4.25)

## Channel 1 sector ETF tape

```
ETF XLE vs SPY (yfinance, through 2026-09-18):
  1d: XLE -0.26% | SPY +0.13% | rel -0.39%
  3d: XLE -2.46% | SPY +0.82% | rel -3.27%
  1w: XLE -1.27% | SPY -0.09% | rel -1.18%
  1m: XLE +1.15% | SPY -0.71% | rel +1.86%
```

MEMORY_CONFIRM: Energy/XLE — memory index unavailable this run (used injected scoreboard/lessons only). Last-10 dir=0.6 mag=0.4 (n=10); last-30 dir=0.522 mag=0.348 (n=23); last graded 09-18 down/mild vs XLE −0.264% (dir HIT, mag MISS — actual flat). Open sector_energy experiment **does** apply: keep direction, shrink confidence after mag misses. Applied: **08-11 live-oil verify FIRES** — Channel 1 CL=F −5.94% / BZ=F −5.76% / Finviz WTI $104.16 (−1.59%) / Brent $107.67 (−1.02%) is a leftover 09-16 column and **conflicts** with live quotes (WTI ~$98 / Brent ~$101.7–101.8, ~−2%); live sign **DOWN**, increment ~2%, **not** a collapse. **08-14 green-oil does NOT fire.** **09-15 physical-increment notable license does NOT fire** (oil offered; Hormuz throughput recovering). **09-14 backwardation debit does NOT fire** (VIX/VIX3M **0.821 contango**). **09-11 pending-binary flatten does NOT fire** (FOMC/SEP **already printed 09-16**; no CPI/NFP today). **09-10 crowded-long does NOT fire** (1m rel **+1.86%** ≪ +8%). **09-04 S1+S4 aligned-negative does NOT fire** off prior-close (1d rel **−0.39%**, not ≤ −1.5%). **09-17 leftover-S4 gate FIRES as a non-force**: do **not** reuse 09-16’s −2.44% / 3d −3.27%; **but** today’s PM:XLE **−1.29%** with XOM/CVX red **is** live extension, so 09-17’s “allow mixed/up when PM is flat” is **OFF**. **09-03 emit-signed-down FIRES**: live offered barrel + S4=0 because prior 1d is near-flat, not because PM is bid. **09-18 band refinement FIRES toward mild** (oil extends ~2% **and** PM ≤ −1%; not the |PM|<1% flat preference). **09-08/09-09 mag-discipline FIRES** (mag 0.35–0.40 ≤ 0.4; oil not >5%; XLE PM not >2% up). size_gate=True. EIA already in 09-16’s tape (next WPSR **Sep 23**). Open experiment: no extra confirming-source test for this scope.

# Energy / XLE — 2026-09-21

This is a **Monday continuation of the oil-premium fade**, not a fresh sector_shock and not a collapse. Friday’s object (offered barrel ~1–2%, XLE −0.26% / rel −0.39%, East-West workaround hopes) **already printed**. Overnight/weekend the **same fade got a fresh increment**: CENTCOM said Hormuz oil/LNG shipments hit a **six-month high**, Trump said he is open to meeting Iran’s president at UNGA, and crude is **down a fourth session** (Brent >2% to ~$101.8, WTI ~$98). XLE is **red in premarket (−1.29%)** — the clear laggard vs XLK +0.98% / XLC +0.59% — and the majors are **with the ETF**. Count the oil-down + geo-premium-fade cluster **once**. Do **not** treat Channel 1 CL=F −5.94% as today’s smash. Do **not** flip to up on a tech-led risk-on tape energy is not in.

## Channel 2

**1. Shared macro as it hits energy.** Tape is **risk-on beta, not a commodity bid**: Channel 1 ES=F **+1.35%** / NQ=F **+2.12%**; Finviz ES/NQ/RTY/DJIA all green (+0.20% / +0.41% / +0.08% / +0.11%). Asia composite **+1.04%**, Europe **+0.95%**. DJ Newswires 04:58 ET: global stocks rally, yields slip, **oil’s fall is the sentiment bid**. **VIX 14.98** with VIX/VIX3M **0.821 — contango**, so the 09-14 full-weight de-risking debit is **off**. USD is a mild producer headwind (DXY **+0.08%** 1d; Finviz USD −0.02%). DFII10 **2.61, −0.07 1d / +0.06 1w** — real yields are easing overnight, **secondary vs oil**. News Judge #1–#3 (Dow worst week / Warsh hike odds / yields vs IWM) are **already-printed duration/beta**, not an XLE spine. Cheaper oil is SPX-positive inflation optics; that can leak beta **only if PM is participating**. It is not: **XLE −1.29% vs XLK +0.98%**. 09-11’s “don’t score S0 negative on green futures” still binds (no pending data-binary), but 09-17 forbids treating tech-led ES as an energy bid. **S0 = 0.**

**2. Spine (S1).** One cluster: live offered barrel **plus** geo-premium fade. Do not triple-count oil + Hormuz recovery + leftover inventory.
- **Crude: offered ~2%, not a surge, not a collapse.** Live-verified (08-11): DJ/Morningstar Brent **>2% to ~$101.79**, WTI **~$98**; Economic Times fourth down day, Brent ~$101.70 / WTI ~$98.20. Channel 1 Finviz $104.16 / CL=F −5.94% is **rejected** as the live increment (same stale column as 09-17/09-18). Still ~$98 / ~$102 — a dip, not 08-25’s smash and not 09-15’s +2.3% surge.
- **Geo premium still present, not transmitting — fading, with a weekend increment.** Hormuz shipments **six-month high** (Adm. Cooper, 19 Sep); Trump open to a Pezeshkian meeting at UNGA; East-West half-capacity-in-days already in Friday’s tape. Houthi Red Sea threats remain, but **oil is falling**, so this is fade/non-confirmation. **Do not** score Crude oil price surge. **Do not** score a fresh Geopolitical supply risk premium HIT. 09-15’s physical-increment license is **off**. News Judge: **no fresh kinetic oil increment**.
- **Inventory: already printed.** EIA week ended 9/11 (released **9/16**): crude **−0.64 Mb** vs ~−1.4 to −1.6 Mb expected to **423.4 Mb**. Next WPSR **Wed Sep 23, 10:30 ET — unprinted, two-sided**. News Judge #4 / DVN −5.6% is leftover 09-16. Do **not** date it as today’s HIT.
- **OPEC+ (carried offset):** Sep **+188 kb/d** completed the 2023 voluntary-cut rollback; **6 Sep** meeting **held October unchanged**. Next core-group meeting **Oct 4**. Not a cut, not a quota break.
- **Demand destruction (carried official):** Sinopec research arm still sees 2026 China demand down; September imports recovering m/m but **well below** last year. Offset only; not today’s HIT.
- **Cracks:** 3-2-1 still elevated (~$69–72); diesel historically extreme. **Refiner sleeve only.** Today HO **+0.18%**, RBOB **−0.54%**, gasoil **−0.26%** — products are **not** a clean squeeze that can drive whole XLE. MAP HEAT Refining dir=up (MPC/VLO) is **nested** — dampen; do not let VLO/MPC set the ETF (09-18’s refiner cushion is why Friday was flat, not a license to flip today).
- **Nat gas $2.902 (−0.51%)** — no surge; N/A for oil-weighted XLE.
- **MAP HEAT nested:** OFS/Drilling/Coal/Uranium **OVERRIDE down**; E&P/Integrated/Refining residual **up** on quiet/stale captains. Nested overrides **do not average into XLE**. Live PM (XOM ~−0.8%, CVX ~−0.5%) **overrides** the stale integrated “pos” tag.

Net **S1 = −1**. Live oil sign is down once, with weekend Hormuz/diplomacy fade inside the same cluster. Not −2: not a collapse, 1w/1m not extended, increment ~2% not a smash, residual geo/cracks still a floor. Not 0: WTI ~−2% with PM confirmation is a real offered barrel and 08-14’s green-oil license is off. Not +1: 08-12 forbids treating the same geo run as a fresh surge.

**3. Breadth.** Channel 1 tape: XLE 1d rel **−0.39%** (through 09-18) — leftover, **sub-threshold**, do **not** copy 3d rel **−3.27%** into S2 (09-17 leftover-S4; 09-10 no double-count). **Live** breadth is the signal: PM:XLE **−1.29%** is the sector board’s worst print; XOM/CVX red with the ETF; XLK/XLC green. That is constituent-confirmed participation in the oil-down, not ETF-only carry and not 09-14’s “single-ETF gap relabeled as breadth.” Nested refiner bid does **not** rescue the parent. **S2 = −1.**

**4. Flows / positioning.** Recent creations/redemptions lean outflow (~$157M week of 9/17; ~$422M print around 9/20). 1m rel **+1.86%** is **not** crowded (09-10 off). Live rotation is **out of** energy vs a green XLK tape — the flow expression of S1, not a second spine. **S3 = 0.**

**5. Catalysts.** No fresh XLE-wide earnings. **EIA WPSR Wed 9/23** is the next hard energy print — two-sided, **not** today’s. UNGA / optional Trump–Pezeshkian / Trump–Xi Thursday are **two-sided geo binaries**, not pre-scored. **XLE ex-div today ($0.3803, ~0.59% of Friday’s $64.31)** is a mechanical cash-print drag, not a taxonomy HIT. Warsh/hike-odds are leftover SPX duration.

### Scoring logic

S0 muted: risk-on futures + contango vol + overnight yield ease are a cyclical overlay that is **not transmitting** to energy; 09-11 forbids scoring that overlay negative, 09-17 forbids scoring it as an energy bid.

S1 is the live oil-offered + geo-fade cluster, netted once. S2 is live XOM/CVX/XLE participation. S4 is Channel 1 confirmation only: 1d rel **−0.39%** (not ≤ −1.5%), 3d **−3.27%** leftover, 1w **−1.18%**, 1m **+1.86%**. Prior-close tape is a **neutral starting point**, not a counter-signal to a live offered barrel (09-03).

**No leading-vs-tape divergence** — factors lean down and live PM confirms down. Trust factors over green ES. Do not manufacture a 09-11 flatten (wrong precondition).

Magnitude discipline: mag hit-rate **0.40 / 0.35** and size_gate=True cap **notable → mild**. 09-18’s flat preference does **not** bind: oil extends ~2% **and** PM:XLE **−1.29% ≤ −1%**. Ex-div is a ~60 bp mechanical, not a notable license. Do **not** emit severe/notable.

Knowable-at-open: every bullish residual (cracks, nested refiners, leftover E&P heat, green ES) is either nested or not transmitting; the only same-week energy print (EIA) is **Wednesday**. Base case is fade continuation, not a bounce.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: -1
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.85
CONFIDENCE: 0.48
REGIME: mixed
DIVERGENCE_FLAGGED: False
HORIZON_3D: oil-fade still the object into Wed EIA (two-sided); XLE follows the barrel unless a fresh Hormuz/kinetic increment re-prices
HORIZON_1W: UNGA diplomacy (optional Trump–Pezeshkian; Trump–Xi Thu) is a two-sided geo binary; premium-fade base case unless talks fail
HORIZON_2W: next OPEC+ core meeting Oct 4; China demand still structurally soft — lid on re-squeeze unless physical chokepoint re-tightens
HORIZON_1M: 1m rel only +1.86% after 09-16 de-risk; not crowded; crude still elevated vs pre-crisis so fade can persist without a smash
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|PARTIAL|0.72|2026-09-21|https://www.morningstar.com/news/dow-jones/202609211129/global-stocks-rally-yields-lower-as-oil-prices-extend-falls
Risk-off tape / flight to safety|MISS|0.78|2026-09-21|https://www.morningstar.com/news/dow-jones/202609211129/global-stocks-rally-yields-lower-as-oil-prices-extend-falls
Real yields rising|MISS|0.62|2026-09-21|Channel 1 DFII10 2.61 (−0.07 1d)
Real yields falling|PARTIAL|0.55|2026-09-21|Channel 1 DFII10 −0.07 1d / +0.06 1w
USD strengthening|PARTIAL|0.52|2026-09-21|Channel 1 DXY +0.08% 1d
USD weakening|MISS|0.60|2026-09-21|Channel 1 DXY +0.08% 1d
Sector breadth expansion (% names up)|MISS|0.70|2026-09-21|Channel 1 PM:XLE −1.29%; XOM/CVX red
Sector breadth failure (ETF up, names flat)|MISS|0.70|2026-09-21|ETF is down, not up
Large-cap leadership inside sector|PARTIAL|0.58|2026-09-21|XOM/CVX participating in the decline
Small/mid leadership inside sector|MISS|0.45|2026-09-21|MAP HEAT nested; not parent
High-beta leadership inside sector|MISS|0.55|2026-09-21|XLK +0.98% vs XLE −1.29%
Low-beta leadership inside sector|MISS|0.50|2026-09-21|checked, nothing material for XLE
Sector ETF inflow / relative volume spike|MISS|0.55|2026-09-21|https://www.nasdaq.com/articles/xle-mpc-psx-vlo-etf-outflow-alert-0
Sector ETF outflow / volume dry-up|PARTIAL|0.55|2026-09-21|https://www.etfaction.com/buffer-etfs-and-value-rotation-drive-flows/
Crowded long (extreme relative performance + valuation)|MISS|0.70|2026-09-21|Channel 1 1m rel +1.86%
Index rebalance / inclusion tailwind|MISS|0.40|2026-09-21|checked, nothing material
Index exclusion / forced selling|MISS|0.40|2026-09-21|checked, nothing material
Crude oil price surge (WTI/Brent)|MISS|0.80|2026-09-21|https://www.morningstar.com/news/dow-jones/202609211129/global-stocks-rally-yields-lower-as-oil-prices-extend-falls
Natural gas price surge|MISS|0.75|2026-09-21|Channel 1 NG −0.51%
Inventory draw (EIA crude/products)|MISS|0.70|2026-09-21|https://www.eia.gov/petroleum/supply/weekly/
OPEC+ cut / supply discipline|MISS|0.65|2026-09-21|https://www.ogj.com/general-interest/economics-markets/news/55403398/opec-holds-october-production-targets-steady-as-focus-shifts-to-2027-quotas
Crack spread / refining margin expansion|PARTIAL|0.60|2026-09-21|https://rbnenergy.com/market-data/3-2-1-crack-spread
Geopolitical supply risk premium|MISS|0.72|2026-09-21|https://www.bloomberg.com/news/articles/2026-09-19/hormuz-oil-shipments-hit-six-month-high-us-commander-says
Crude price collapse|MISS|0.70|2026-09-21|live WTI ~$98 / Brent ~$101.8 (~−2%, not >5%)
OPEC+ production increase / quota break|PARTIAL|0.50|2026-09-21|https://www.ogj.com/general-interest/economics-markets/news/55403398/opec-holds-october-production-targets-steady-as-focus-shifts-to-2027-quotas
Demand destruction (recession/China weak)|PARTIAL|0.50|2026-09-21|https://www.reuters.com/business/energy/china-oil-demand-fall-89-2026-sinopec-research-says-2026-09-09/
Inventory build|MISS|0.65|2026-09-21|https://www.eia.gov/petroleum/supply/weekly/
Crack spread collapse|MISS|0.60|2026-09-21|3-2-1 still ~$69–72; nested only
Sector rotation into energy|MISS|0.75|2026-09-21|Channel 1 PM:XLE −1.29% vs XLK +0.98%
Sector rotation out of energy|HIT|0.72|2026-09-21|Channel 1 PM board; https://www.morningstar.com/news/dow-jones/202609211129/global-stocks-rally-yields-lower-as-oil-prices-extend-falls
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- memory_search: Energy XLE sector prediction lessons oil crude 2026-09 (index unavailable)
- web_search: WTI Brent crude oil price today September 21 2026
- web_search: oil news OPEC EIA inventory Hormuz Saudi pipeline September 21 2026
- web_search: XLE ETF premarket energy stocks XOM CVX COP September 21 2026
- web_search: risk on equity futures VIX yields dollar September 21 2026
- x_search: WTI Brent crude oil price and energy stocks XLE today September 21 2026 (2026-09-19 to 2026-09-21)
- web_search: XLE energy ETF flows positioning September 2026
- web_search: crack spread diesel gasoline refining margins September 2026
- web_search: XOM CVX COP premarket September 21 2026 energy stocks down
- web_search: Strait of Hormuz oil shipments six-month high September 2026
- web_search: WTI crude oil price September 21 2026 live $98 OR $94
- web_search: XLE ex-dividend September 21 2026 $0.3803
- web_search: OPEC+ October production meeting oil supply September 2026
- web_search: energy sector breadth XOM CVX MPC VLO premarket Monday September 21
- web_search: EIA weekly petroleum status report September 23 2026 preview API
- web_search: China oil demand 2026 September weak OR strong
- web_search: XLE premarket -1.29% September 21 2026 energy only red sector
- web_fetch: Economic Times oil-price-today Sep 21 (title only; body blocked)
- web_fetch: Morningstar/Dow Jones “Global Stocks Rally, Yields Lower as Oil Prices Extend Falls”
- web_fetch attempts: Trading Economics Brent, Reuters Hormuz, MarketWatch CL.1 (403/401)

**Key sources (title + URL + timestamp where available) and facts taken**
- Morningstar / Dow Jones Newswires, “Global Stocks Rally, Yields Lower as Oil Prices Extend Falls” — https://www.morningstar.com/news/dow-jones/202609211129/global-stocks-rally-yields-lower-as-oil-prices-extend-falls — **September 21, 2026 04:58 ET (08:58 GMT)**. Facts: DJIA/S&P futures +0.6%, Nasdaq futures +0.8%; Asia/Europe green; Brent **>2% to ~$101.79**, WTI **~$98**; Hormuz tanker transits + Trump open to meeting Pezeshkian at UNGA; 10Y **4.973% (−2.4 bp)**; Houthi still targeting Red Sea routes.
- Economic Times, “Oil Price Today (September 21): Crude oil falls for 4th day, below $102” — https://economictimes.indiatimes.com/markets/commodities/news/oil-price-today-september-21-crude-oil-falls-for-4th-day-below-102-whats-triggering-the-fall/articleshow/134378118.cms — **2026-09-21**. Facts used via search extract: fourth down session; Brent ~$101.70 (−2.1% / −$2.16); WTI ~$98.20; Saudi shipment-recovery hopes.
- Bloomberg via search, Hormuz six-month high — https://www.bloomberg.com/news/articles/2026-09-19/hormuz-oil-shipments-hit-six-month-high-us-commander-says — **2026-09-19**. Fact: Adm. Brad Cooper / CENTCOM: crude/LNG shipments through Hormuz over prior two weeks at a six-month high.
- Reuters (search extract; page 401 on fetch) — https://www.reuters.com/world/middle-east/vessels-trickle-through-strait-hormuz-mideast-tension-persists-2026-09-21/ — **2026-09-21**. Fact: visible AIS traffic still thin vs pre-war; tension persists (used as dampener, not a fresh kinetic HIT).
- WSJ/MarketWatch via search — https://www.wsj.com/market-data/quotes/futures/CRUDE%20OIL%20-%20ELECTRONIC — **2026-09-21**. Fact: front-month Oct WTI ~$98.19–$98.65 (~−2%); some Nov/continuous quotes ~$94 (contract split; live sign still DOWN).
- DividendInvestor — https://www.dividendinvestor.com/dividend-news/20260918/state-street-energy-select-sector-spdr-etf-select-sector-spdr-trust-nyse-xle-declared-a-dividend-of-$0.3803-per-share/ — **ex-date 2026-09-21**. Fact: XLE $0.3803 dividend, record 9/21, payable 9/23.
- OGJ / Reuters OPEC+ — https://www.ogj.com/general-interest/economics-markets/news/55403398/opec-holds-october-production-targets-steady-as-focus-shifts-to-2027-quotas — **2026-09-06**. Fact: October quotas unchanged; next meeting Oct 4; Sep +188 kb/d completed voluntary-cut rollback.
- EIA WPSR page — https://www.eia.gov/petroleum/supply/weekly/ — next release **Wed 2026-09-23 10:30 ET**; prior week ended 9/11 already printed 9/16.
- RBN / oilpriceapi crack spread (search) — 3-2-1 ~$69–72 on/around 9/19–9/21; diesel cracks still extreme — **refiner sleeve only**.
- ETF flow notes — Nasdaq outflow alert / ETFaction ~$157M and ~$422M prints mid/late Sep — used as PARTIAL outflow, not crowded-long.
- Public.com / stockanalysis premarket — XOM ~$162.25 (~−0.8%), CVX ~$208.38 (~−0.5%), XLE ~$63.50 (~−1.26%) vs Friday close $64.31.
- Sinopec/Reuters China demand (9 Sep) — 2026 China oil demand seen down; September imports still weak y/y — **carried offset only**.
- Channel 1 injected panel (do not alter): VIX 14.98, VIX/VIX3M 0.821, ES=F +1.35%, NQ=F +2.12%, PM:XLE −1.29%, XLE 1d/3d/1w/1m rel −0.39% / −3.27% / −1.18% / +1.86%.

**Rejected / not used as live oil**
- Channel 1 CL=F −5.94%, BZ=F −5.76%, Finviz WTI $104.16 / Brent $107.67 — leftover 09-16 column (same conflict as 09-17/09-18).
- One WorldEnergyNews extract citing Brent ~$82 / WTI ~$79 and XOM −6% on a “US-Iran agreement” — **inconsistent** with DJ Newswires $101.8/$98 and Channel 1 PM:XLE −1.29%; treated as a conflicting/wrong-level story, not the load-bearing print.
- X posts citing WTI $96–$100 / Brent $99–$103 — noisy, sign still DOWN, not used as the primary quote.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.85, 'leading_sum': -5.0, 'divergence_flagged': False, 'total_score': -6.269, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.65, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': -0.8728, 'score': -5.237, 'legs': [{'leg': 'CL', 'pct': -1.59, 'w': 0.35}, {'leg': 'QA', 'pct': -1.02, 'w': 0.15}, {'leg': 'ES', 'pct': 1.35, 'w': 0.6}, {'leg': 'PM:XLE', 'pct': -1.29, 'w': 0.7}]}, 'overlay_score': -4.25, 'overlay_raw': -4.25, 'index_carry': 3.218, 'general_total': 12.871, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.48, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
