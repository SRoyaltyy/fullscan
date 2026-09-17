# Sector Prediction — Utilities — 2026-09-17

- ETF: **XLU**
- rubric: `00_grounding/sectors/utilities.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **1.751** (mult 0.9)
- regime: risk_on
- divergence_flagged: **True**
- engine: v2 · tape_anchor **2.605** (ES +1.71%, ZN -0.03%, PM:XLU +0.41%) · index_carry **1.846** (general 7.383) · llm_overlay **-2.7** (raw -2.7)

## Channel 1 sector ETF tape

```
ETF XLU vs SPY (yfinance, through 2026-09-16):
  1d: XLU +0.00% | SPY -0.44% | rel +0.44%
  3d: XLU -2.52% | SPY -1.34% | rel -1.18%
  1w: XLU -3.77% | SPY -1.10% | rel -2.68%
  1m: XLU -6.47% | SPY -2.41% | rel -4.06%
```

MEMORY_CONFIRM: Utilities/XLU only — memory index paused this run (`openclaw memory status --index` / `openclaw memory index --force`); used injected sector logs + Channel 1 + Channel 2. Rolling dir=0.4 / mag=0.3 (n=10); last 30 dir=0.421 / mag=0.368 (n=19). Last graded: 09-16 down/mild vs XLU 0.00% / SPY −0.44% / rel +0.44% (dir MISS — AM risk-on/rotation-away signed the close through an unprinted FOMC leftover). 09-15 down/mild still ungraded. Applied: 09-16 (FOMC leftover flatten does **not** bind today — the decision printed; do **not** restack the hike as a fresh smash; do **not** let trailing 1w/1m lag pay another down close); 09-11 (risk-on inputs are headwinds for a defensive, not cushions; claims is **not** a fully one-sided CPI-class binary); 09-10 (VIX 16.04 / VIX3M 0.813 contango fails VIX≥20 FTS gate → no 08-18 relative-beat); 09-09 (1d rel +0.44% is **not** a kinetic/oil defensive spike — no cushion-as-flat override); 09-08 (oil still elevated, **offering** today = inflation channel fading, not FTS); 08-28 (Warsh already public — no notable-down minted from a pre-statement hawkish branch); 08-27 (NQ leads ES, mega-cap/AI already public → relative lag / flat-to-down absolute unless a fresh same-session yield impulse — there is none); 08-25 (S0=0 but S1≠0, so the “don’t manufacture down from carried lag” gate does not fully bind); 08-21 (live 10Y ~4.99, not FRED 09-15 +3 bp as “today’s move”); 08-13 (one trailing rel print does not pay S2 and S4); 08-12 (AI-power is a 1d dampener); 08-14/09-04 (calendar: 8:30 claims + housing + Philly Fed, not a light day). Open experiment (09-16 loss): extra confirm before full weight in the dominant bucket — used XLK PM + NQ-vs-ES + Europe for rotation. Scope do-instead: cut conviction vs 1d-rel conflict; keep mild. Same-shock: carried yields/FOMC in S0 only (and not HIT); rotation-away in S1 only.

# Utilities (XLU) — 2026-09-17

Object is the **near-session XLU environment**, not SPX and not a stock pick.

## Channel 1 (trusted, not re-derived)

XLU vs SPY through 2026-09-16: **1d +0.00% / −0.44% (rel +0.44%)**; **3d −2.52% / −1.34% (rel −1.18%)**; **1w −3.77% / −1.10% (rel −2.68%)**; **1m −6.47% / −2.41% (rel −4.06%)**. Freshest 1d is a **relative beat on a down SPY day** (XLU unchanged). 3d/1w/1m remain a **widening lag**. Horizons: **3d lag**, **1w lag**, **2w lag** (no independent 2w print; 1w and 1m are both deep red), **1m lag**.

Macro: VIX **16.04** (−1.67 1d, −1.8 1w), **VIX/VIX3M 0.813 — CONTANGO**; DGS10 **5.0** as of 09-15 (+3 bp 1d, **+20 bp 1w, +32 bp 1m**); DGS30 **5.36** (+2 bp 1d, +11 bp 1w, +11 bp 1m); DFII10 **2.62** (+2 bp 1d, **+19 bp 1w, +21 bp 1m**); HY 2.76 (+5 bp 1d, still tight); EPU 147.41 (−45 1d); **CL=F −0.98% / BZ=F −1.3%** (Finviz WTI **−1.59%** / Brent **−1.02%**); DXY **−0.13%**; **ES=F +1.71% / NQ=F +2.10%** vs prior close (Finviz live still green, NQ leads: ES +0.20%, NQ +0.41%, RTY +0.08%, DJIA +0.11%); **XLU PM +0.41%** vs **XLK +1.28%**, XLY +0.61%, XLF +0.43%, XLI +0.43%, XLP +0.18%, XLRE +0.37%; Asia **−0.03%**; Europe **+0.45%**; 5-day 10Y–SPX corr **−0.109**. Bond futures: 10Y note **−0.03%**, 30Y **−0.06%** — a tiny backup, not a smash and not relief.

**Live curve (08-21):** 10Y **~4.99%** (CountryEconomy/GuruFocus; Investing ~4.992–4.993%) vs FRED 09-15 **5.00%**. That is **stabilization inside the stress zone**, not a scored easing impulse and not a fresh backup. Do **not** pay Tuesday’s 5% breach or Wednesday’s FOMC twice.

**Calendar (08-14 / 09-04 / 09-11):** **8:30 ET initial jobless claims** (cons. ~208k vs 206k prior) **plus housing starts, permits, Philly Fed** — unprinted at this snapshot (18:30 GMT+8 = 06:30 ET). Not CPI/NFP/FOMC-class. Branch test for a bond-proxy: in-line/strong labor → risk-on continuation / rotation-away (negative-to-neutral); weak labor → possible duration bid (the one XLU-positive leg). **Not fully asymmetric-negative** — do **not** apply the 09-11 S0=−1 CPI template to claims. Keep as event risk; do not pre-score either branch.

**FOMC (already printed 09-16):** Unanimous **+25 bp to 3.75–4.00%**; 16/18 dots another 2026 hike; 2Y +~7 bp, 10Y only +~1–2 bp to ~5.00–5.02%; XLU closed **unchanged**. Hawkish Warsh is **in the price**. CME FedWatch now **Oct hold ~59.5% / +25 bp ~40%**. Do not restack.

## Channel 2

**1. Shared macro → this sector.** Classical map is **real/nominal yields**; AI load is structural offset only.

- **Pre-print tape is risk-on, NQ-led:** ES +1.71% / NQ +2.10% vs prior close; Finviz live still NQ-leads; Europe +0.45%; VIX 16.04 in deep contango; XLK PM +1.28% vs XLU +0.41%. Per 09-11, for a defensive those are **headwinds, not cushions**. Per 08-27, NQ leading with Adobe/ASML already public defaults XLU to **relative lag / flat-to-down absolute** unless a fresh same-session yield impulse appears. It has not: 10Y ~4.99%, bond futures flat-red.
- **Carried hawkish FOMC / sticky real yields:** DFII10 2.62, 10Y still ~5%. **Already expressed** in yesterday’s 0% XLU close and the 1w/1m lag. Live curve is **not independently ripping**. Count once in S0 as **carried, not a fresh HIT**.
- **Oil offering from war-premium levels** (WTI ~$104, −1.0/−1.6%): 09-08 says elevated oil is an inflation/duration negative when **rising**; today it is **fading**, which forbids a fresh rates smash from oil and does **not** mint FTS.
- **09-10 gate:** VIX 16.04 < 20 and **contango 0.813** — no FTS. 08-18 relative-beat frame is **off**.
- **Claims 8:30:** two-sided for XLU; not scored.

**S0 = 0.** Sticky real yields are carried, not a live impulse; FOMC is paid; claims unresolved and not one-sided; oil-offering forbids a rates smash. Risk-on is mapped in S1 as rotation, not stacked here. Extra-confirm experiment does **not** promote S0 to −1 without a live duration impulse.

**2. Spine / secondary.**
- **Data-center load growth / power demand upside:** structural HIT, **stale** for a 1d call (SO 17 GW contracted large-load; Duke equity-funded capex; 247wallst 09-07 Texas interconnection freeze is **carried** load-growth disappointment, not this morning). Rubric: do **not** let the multi-year AI-power story override a 1d rate/rotation tape without a fresh XLU-wide catalyst. 08-12 dampener only.
- **Rates falling (bond-proxy bid):** **MISS**. Live 10Y ~4.99 / 30Y ~5.36. Tiny 1 bp dip is noise at this level (08-21).
- **Rates rising (bond-proxy selloff):** **PARTIAL / carried**. 1w DGS10 +20 bp / DFII10 +19 bp already in the 1w/1m tape and in yesterday’s close. This morning the curve is **not** independently rising. Do **not** HIT and do **not** double-count with S0.
- **Risk-on rotation away from utilities:** **HIT**. NQ leads ES on both tapes; XLK PM +1.28% vs XLU +0.41%; XLY +0.61%. Extra confirms (open experiment): Europe +0.45%, VIX −1.67, oil offered. Count **once** (not also as a separate “sector rotation out” stack).
- **Risk-off / FTS / rotation into utilities:** **MISS**.
- **Nuclear / gas / grid CapEx / favorable ROE:** structural (Reuters 09-16 Westinghouse/AP1000; DOE Duane Arnold loan) — **stale** for 1d. MAP HEAT captains all **none**, size_gate on.
- **Adverse rate case / regulatory smash:** checked, nothing material this morning. CEG PM ~+1.5% is **IPP nested**, not an XLU driver (08-28).

Net **S1 = −1** (live rotation-away at full single-factor weight with extra confirms; rates-rising not re-HIT; AI-power not allowed to offset a 1d rotation tape).

**3. Breadth.** 1d rel **+0.44%** is a relative beat, not a smash. Premarket: XLU +0.41% with the tape, **lagging XLK**; NEE/SO/DUK modest green; CEG IPP must not set the ETF. MAP HEAT all **flat/none**. No independent constituent expansion. Stale 3d/1w/1m lag is a **descriptor**, not a same-day breadth print (rate-proxy lesson: do not score 1w/1m into S2). One 1d rel print is reserved for S4 mixing, so S2 does not also eat it. **S2 = 0**.

**4. Flows.** ETFdb ~09-16: 5d **+$17M**, 1m **+$65M**, 3m +$406M. Modest, not a relative-volume spike, not a dry-up. 1m rel **−4.06%** = **not crowded-long**. **S3 = 0**.

**5. ETF tape (confirmation only).** Freshest 1d rel **+0.44%** vs still-red 3d/1w/1m. 09-14’s S4=−1 floor needs **all horizons negative and |1d rel|≥~1%** — **does not fire** (1d is positive). Do not mint S4=+1 from a 0% absolute / SPY-down relative beat. Mixed → **S4 = 0**.

**Divergence:** leading S1 (−1) fights S4 (0) and the 1d relative cushion. **Flag it; trust factors over tape.** 09-16 do-instead: cut conviction, keep mild — the 1d tape conflict is real, but the live factor is rotation, not a restacked FOMC.

**Self-audit:** lens = XLU 1d environment, not SPX. Band capped mild (unprinted claims; |S| modest; 09-16 loss pattern). Skew = defensive on risk-on = negative, not FTS. Same-shock: FOMC/yields counted in S0 as **0 / carried**, rotation only in S1. Single-ticker (CEG) does not drive the ETF. 09-09 cushion override **off** (not a kinetic spike). 08-18 beat **off** (VIX 16, contango).

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: -1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: risk_on
DIVERGENCE: true
HORIZON_3D: lag
HORIZON_1W: lag
HORIZON_2W: lag
HORIZON_1M: lag
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|HIT|0.82|2026-09-17|Channel 1 ES +1.71% / NQ +2.10%; Finviz NQ +0.41% vs ES +0.20%
Risk-off tape / flight to safety|MISS|0.85|2026-09-17|VIX 16.04, VIX/VIX3M 0.813 contango
Real yields rising|PARTIAL|0.70|2026-09-15|https://fred.stlouisfed.org — DFII10 2.62, +19 bp 1w / +21 bp 1m, not a live AM impulse
Real yields falling|MISS|0.78|2026-09-17|live 10Y ~4.99%, not easing
USD strengthening|MISS|0.70|2026-09-17|Channel 1 DXY −0.13%
USD weakening|PARTIAL|0.50|2026-09-17|DXY −0.13% — not a utilities driver
Sector breadth expansion (% names up)|MISS|0.60|2026-09-17|MAP HEAT captains none; regulated names only modest green
Sector breadth failure (ETF up, names flat)|MISS|0.55|2026-09-17|XLU PM +0.41% with the tape, not an ETF-up/names-flat divergence
Large-cap leadership inside sector|PARTIAL|0.50|2026-09-17|NEE/SO/DUK modest; CEG IPP nested
Small/mid leadership inside sector|MISS|0.50|2026-09-17|checked, nothing material
High-beta leadership inside sector|PARTIAL|0.55|2026-09-17|CEG PM ~+1.5% is IPP, size-gated, not XLU
Low-beta leadership inside sector|MISS|0.55|2026-09-17|no defensive leadership vs XLK
Sector ETF inflow / relative volume spike|MISS|0.62|2026-09-16|https://etfdb.com/etf/XLU/ — 5d +$17.13M, not a spike
Sector ETF outflow / volume dry-up|MISS|0.62|2026-09-16|https://etfdb.com/etf/XLU/ — 5d/1m still net positive
Crowded long (extreme relative performance + valuation)|MISS|0.72|2026-09-16|1m rel −4.06%, de-risked
Index rebalance / inclusion tailwind|MISS|0.50|2026-09-17|checked, nothing material
Index exclusion / forced selling|MISS|0.50|2026-09-17|checked, nothing material
Data-center load growth / power demand upside|HIT|0.65|2026-09-17|https://www.utilitydive.com/news/southern-co-contracted-large-load-data-centers/826919/ — structural/stale for 1d
Rates falling (bond-proxy bid)|MISS|0.80|2026-09-17|https://countryeconomy.com/bonds/usa — 10Y ~4.99%, not falling
Favorable rate case / allowed ROE|MISS|0.50|2026-09-17|checked, nothing material
Nuclear / gas generation policy support|HIT|0.55|2026-09-16|https://www.reuters.com/business/energy/us-bets-heavy-westinghouse-fleet-cost-hurdles-loom--reeii-2026-09-16/ — structural/stale
Grid CapEx approval / recovery|HIT|0.50|2026-09-17|structural capex supercycle, no same-session order
Rates rising (bond-proxy selloff)|PARTIAL|0.70|2026-09-15|Channel 1 DGS10 5.0 / +20 bp 1w — carried, not live
Adverse rate case|MISS|0.50|2026-09-17|checked, nothing material
Load growth disappointment|PARTIAL|0.55|2026-09-07|https://247wallst.com/investing/etf/2026/09/07/xlus-ai-power-story-crumbles-as-texas-freezes-data-center-demand/ — carried, not fresh
Regulatory disallowance / project cancel|MISS|0.50|2026-09-17|checked, nothing material
Risk-on rotation away from utilities|HIT|0.78|2026-09-17|XLK PM +1.28% vs XLU +0.41%; NQ leads ES
Sector rotation into utilities|MISS|0.78|2026-09-17|XLU lagging XLK/XLY on the PM board
Sector rotation out of utilities|HIT|0.70|2026-09-17|same object as rotation-away — scored once in S1
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- memory_search: Utilities XLU sector prediction lessons rates FOMC (index unavailable)
- web_search: US 10 year treasury yield today September 17 2026
- web_search: jobless claims September 17 2026 calendar economic data
- web_search: XLU utilities stocks premarket September 17 2026
- web_search: FOMC Warsh September 16 2026 hike utilities bond yields
- web_search: CME FedWatch rate hike odds September 17 2026
- web_search: utilities sector rotation XLU NEE Duke Southern data center power demand September 2026
- web_search: XLU ETF flows inflows outflows September 2026
- web_search: 10 year treasury yield live 4.99 September 17 2026 after FOMC
- web_search: utility rate case grid capex nuclear policy September 2026
- web_search: initial jobless claims actual September 17 2026 8:30
- web_search: risk on Nasdaq futures utilities lag defensive rotation September 17 2026
- web_search: NEE SO DUK CEG stock price September 17 2026 premarket
- web_search: Philadelphia Fed manufacturing index housing starts September 17 2026
- web_search: "utilities" OR XLU lag OR underperform Nasdaq OR XLK September 17 2026
- x_search: XLU utilities 10 year yield FOMC Warsh September 17 2026 (2026-09-16 to 2026-09-17)
- web_fetch: https://www.cnbc.com/2026/09/16/here-are-five-key-takeaways-from-wednesdays-fed-rate-hike.html
- web_fetch: https://etfdb.com/etf/XLU/ (403)

**Key sources and facts taken**
- Channel 1 panel (injected, 2026-09-17): VIX 16.04 / VIX3M 0.813; DGS10 5.0; DFII10 2.62; ES +1.71% / NQ +2.10% vs prior close; Finviz ES +0.20% / NQ +0.41%; XLU PM +0.41% vs XLK +1.28%; XLU vs SPY 1d rel +0.44%, 3d −1.18%, 1w −2.68%, 1m −4.06%; WTI −1.59% / Brent −1.02%; 10Y note −0.03% / 30Y −0.06%.
- CountryEconomy / GuruFocus (2026-09-17): 10Y **4.99%**, −1 bp from ~5.00%. https://countryeconomy.com/bonds/usa ; https://www.gurufocus.com/economic_indicators/37/10-year-treasury-yield
- MacroRadar / Myfxbook / TradingEconomics (pre-8:30 ET 2026-09-17): claims **scheduled 8:30 ET**, cons. ~208k vs 206k prior; **actual not out**. Also housing starts, permits, Philly Fed 8:30. https://www.macroradar.io/initial-jobless-claims
- CNBC (2026-09-16): unanimous +25 bp to 3.75–4.00%; 16/18 another 2026 hike; Dow −631; 2Y +>7 bp; hawkish Warsh presser. https://www.cnbc.com/2026/09/16/here-are-five-key-takeaways-from-wednesdays-fed-rate-hike.html
- Reuters (2026-09-16): Warsh attributes high long-end yields to growth, hyperscaler capex, geopolitics. https://www.reuters.com/markets/us/feds-warsh-lays-out-forces-driving-up-bond-yields-2026-09-16/
- Odaily/TechFlow citing CME FedWatch (post-hike): Oct hold **59.5%**, +25 bp **40.1%**.
- ETFdb (as of ~2026-09-16): XLU 5d **+$17.13M**, 1m **+$64.78M**, 3m +$406M. https://etfdb.com/etf/XLU/
- Utility Dive: Southern contracted large-load **17 GW**; Duke $10B equity for growth — structural, not 1d. https://www.utilitydive.com/news/southern-co-contracted-large-load-data-centers/826919/
- 24/7 Wall St (2026-09-07): Texas data-center interconnection freeze / load-growth disappointment — **carried**. https://247wallst.com/investing/etf/2026/09/07/xlus-ai-power-story-crumbles-as-texas-freezes-data-center-demand/
- Reuters (2026-09-16): Westinghouse AP1000 / DOE nuclear financing — structural. https://www.reuters.com/business/energy/us-bets-heavy-westinghouse-fleet-cost-hurdles-loom--reeii-2026-09-16/
- MarketScreener / Morningstar (2026-09-16): “Utilities flat on defensive bias”; futures green as hike overhang lifts. https://www.morningstar.com/news/dow-jones/202609168323/utilities-flat-on-defensive-bias-utilities-roundup
- Premarket names (2026-09-17 AM): NEE ~+0.3–0.9%, SO ~flat/+0.2%, DUK ~+0.08%, CEG ~+1.5–1.7% (IPP).
- X/Twitter (09-16/17): 10Y ~5.003% into the FOMC close; 2Y +6.5 bp — consistent with Channel 1, not a fresh 09-17 smash.

**Checked, nothing material:** same-session XLU-wide rate-case outcome; index rebalance; ETF outflow spike; fresh kinetic Hormuz increment (News Judge: tanker-rate spike is not a confirmed new kinetic; inventory build argues the other way); 2-week independent relative print separate from 1w/1m lag.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': -3.0, 'divergence_flagged': True, 'total_score': 1.751, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.47, 'regime': 'risk_on', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.4342, 'score': 2.605, 'legs': [{'leg': 'ES', 'pct': 1.71, 'w': 0.3}, {'leg': 'ZN', 'pct': -0.03, 'w': 0.75}, {'leg': 'PM:XLU', 'pct': 0.41, 'w': 0.7}]}, 'overlay_score': -2.7, 'overlay_raw': -2.7, 'index_carry': 1.846, 'general_total': 7.383, 'skill_multipliers': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.55, 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': -1.5, 'w1': -3.18}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
