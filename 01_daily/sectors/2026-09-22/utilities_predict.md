# Sector Prediction — Utilities — 2026-09-22

- ETF: **XLU**
- rubric: `00_grounding/sectors/utilities.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-0.286** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **-0.162** (ES -0.07%, ZN -0.03%, PM:XLU -0.02%) · index_carry **-0.124** (general -0.497) · llm_overlay **0.0** (raw 0.0)

## Channel 1 sector ETF tape

```
ETF XLU vs SPY (yfinance, through 2026-09-21):
  1d: XLU -0.34% | SPY +1.55% | rel -1.89%
  3d: XLU -0.87% | SPY +2.83% | rel -3.71%
  1w: XLU -2.06% | SPY +1.91% | rel -3.97%
  1m: XLU -6.42% | SPY +1.68% | rel -8.10%
```

MEMORY_CONFIRM: Utilities/XLU only — memory index paused (`openclaw memory status --index` / `openclaw memory index --force`); used injected sector logs + Channel 1 + Channel 2. Rolling dir=0.4 / mag=0.2 (n=10); last 30 dir=0.409 / mag=0.318 (n=22). Last graded: 09-21 down/mild vs XLU −1.09% / SPY +1.55% / rel −2.65% (dir HIT, mag MISS — S0 zeroed by the extra-confirm *floor*; rotation was the close). 09-18 flat/flat vs −1.42% (dir MISS). 09-17 flat/flat vs +0.90% (dir MISS). Applied: 09-21 (S0 −0.5/−1 only if ≥0.5% rip + PM red vs a leading peer + 4-horizon lag — **does not bind**: Finviz ES +0.20%/NQ +0.41% under the rip gate, vs-close ES/NQ **−0.07%**, PM:XLU −0.02% vs XLK **−0.18%** not a lag vs a leader; extra-confirm is a *ceiling*, not a license to re-HIT); 09-18 (don’t treat missing AM smash-confirm as all-clear *and* don’t re-HIT Friday’s paid backup; leftover after a paid down-twin is two-sided — **slot is now several sessions old**; do not restack 09-18 or 09-21); 09-16 (do not restack the paid hike; AM leftover risk-on alone does not sign the close; do **not** let trailing 1d/1w lag pay another down close as the thesis); 09-17 (rotation-away is relative, not an absolute ceiling); 09-14 (**binds for S4 only**: 1d/3d/1w/1m rel all < 0 **and** |1d rel| 1.89% ≥ ~1% → S4 = −1.0 confirmation); 09-11 (no CPI/NFP/FOMC today — do **not** apply the both-branches S0=−1 template to Richmond Fed / Barkin; leftover Nasdaq is **not** a live risk-on rip); 09-10 (VIX 14.88 / VIX3M 0.823 contango fails VIX≥20 FTS gate → no 08-18 relative-beat; 2Y auction is **not** a long-end supply HIT); 09-09 (1d rel −1.89% is not a ≥0.4% cushion); 09-08 (oil **offering** from war-premium = inflation channel fading, not FTS); 08-28 (do **not** promote IPP CEG/VST SPLIT, SO/Google nuclear, or NEE/Dominion into S1); 08-27 (AI/ASML/AMD already public; live NQ lead is modest and vs-close is flat — relative lag is the *descriptor*, not a fresh smash); 08-25 (**binds**: S0=S1=0 → do not manufacture down from carried S2/S3); 08-21 (live 10Y **4.97%** unchanged vs 09-21 4.96%, not FRED 09-18 5.01 as “today’s move”); 08-13 (one trailing rel print does not pay S2 **and** S4 — S4 takes the 09-14 floor; S2 stays 0); 08-12 (AI-power / data-center CapEx is a 1d dampener, not a band engine). Open experiment: extra confirm before full weight — **no extra confirm for a fresh rates smash** (ZN −0.03%, 10Y unchanged) **and none for a second easing leg**. Scope do-instead (09-21 win): shrink confidence on modest |score|. Same-shock: sticky ~5% long end counted in S0 as **carried/not HIT**; not re-HIT in S1. Ex-div **already printed 09-21** (~$0.30) — do not restack.

# Utilities (XLU) — 2026-09-22

Object is the **near-session XLU environment**, not SPX and not a stock pick.

## Channel 1 (trusted, not re-derived)

XLU vs SPY through 2026-09-21: **1d −0.34% / +1.55% (rel −1.89%)**; **3d −0.87% / +2.83% (rel −3.71%)**; **1w −2.06% / +1.91% (rel −3.97%)**; **1m −6.42% / +1.68% (rel −8.10%)**. Freshest 1d is a **clean lag on a strong up-SPY day** — already expressed Monday. Do **not** smuggle a relative-beat clause. Do **not** let 1w/1m pay a second down close (09-16).

**HORIZON_3D:** lag (−3.71% rel). **HORIZON_1W:** lag (−3.97% rel). **HORIZON_2W:** lag (no independent 2w print; 1w and 1m both deep red). **HORIZON_1M:** deep lag (−8.10% rel). Structural descriptor, not a same-session catalyst.

Macro: VIX **14.88** (+0.01 1d, −2.32 1w), **VIX/VIX3M 0.823 — CONTANGO** (no stress); DGS10 **5.01** as of 09-18 (+7 bp 1d, +5 bp 1w, **+36 bp 1m**); DGS30 **5.34** (+5 bp 1d, −1 bp 1w, +15 bp 1m); DFII10 **2.68** (+7 bp 1d, +8 bp 1w, **+33 bp 1m**); HY OAS 2.68 (tight); EPU 202.77; **CL=F −4.78% / BZ=F −0.63%** vs Finviz WTI **−1.59%** / Brent **−1.02%** (both tapes **offering**); DXY ~flat (+0.06% 1d); **ES=F −0.07% / NQ=F −0.07%** vs prior close vs Finviz live **ES +0.20% / NQ +0.41% / RTY +0.08% / DJIA +0.11%** (NQ leads the *live* tape modestly; vs-close is **flat**, not a ≥0.5% rip); **XLU PM −0.02%** vs XLK **−0.18%**, XLP **+0.32%**, XLI **−0.75%**, XLE **−0.45%**, XLF **−0.29%**, XLV **−0.27%**, XLC **+0.21%** — XLU is **mid-pack / non-haven**, not the funding source of a live rip (XLP is the only green defensive); Asia **+0.41%**; Europe **+0.07%**; 5-day 10Y–SPX corr **−0.79**. Bond futures: 10Y note **−0.03%**, 30Y **−0.06%**, Ultra Bond **−0.06%** — a **tiny backup**, not a smash and not relief.

**Live curve (08-21):** CountryEconomy **10Y 4.97% on 09/22 vs 4.96% on 09/21 (unchanged)**; cluster ~4.93–4.97% (MacroMicro/GuruFocus). 30Y live **~5.27–5.29%** vs FRED 09-18 5.34%. This is **stabilization inside the ~5% stress zone**, not a scored easing impulse and not a fresh backup. Do **not** pay FRED 09-18 5.01%, Wednesday’s FOMC, Friday’s 5.00%, or Monday’s rotation twice.

**Calendar (08-14 / 09-04 / 09-11):** **No 8:30 CPI/PCE/NFP. No FOMC** (printed 09-16). **No long-end auction.** **$69B 2-Year note** at 1:00 ET is **front-end supply** — 09-10’s S1 auction rule is 10Y/30Y, not 2Y. Richmond Fed manufacturing ~10:00 ET and Barkin 13:00 ET are **not** CPI-class; branch test is two-sided (strong → relative lag; weak → possible duration bid). Do **not** apply the 09-11 S0=−1 template. Fed-speaker leftover is event risk → lower confidence, **not** a signed call. XLU ex-div **was yesterday**.

## Channel 2

**1. Shared macro → this sector.** Classical map is **real/nominal yields**; AI load is a structural offset only.

- **This morning is not Monday’s rip.** News Judge #1 (Nasdaq AI pop / AMD $1T) is explicitly a **prior-close leftover**, not an overnight utilities catalyst. Vs-close ES/NQ **−0.07%**. Live Finviz is modest (NQ +0.41% < 0.5% gate). The **09-21 S0=−0.5/−1 trigger fails**.
- **Sticky long end, not a live smash.** 10Y 4.97% unchanged; ZN −0.03%. Extra-confirm gate **caps** smash weight — it does **not** force S0 to a down HIT, and it does **not** mint S0+. 09-18: missing AM confirm ≠ all-clear, but re-HITing Friday/Monday is forbidden.
- **Oil offering** from elevated levels: 09-08 says that is the inflation channel **fading**, not FTS. For a defensive it is a mild risk-on input, not a duration bid.
- **09-10 gate:** VIX 14.88 < 20 and **contango** — no FTS. Sticky ~5% long end remains a **relative-lag descriptor**, not an 08-18 relative beat.
- **Gold +0.90% vs APH/rising-yields cluster:** mixed duration, not a clean cut-bet impulse for XLU.
- **PM board is the tell:** cyclicals are **red** (XLI −0.75%, XLE −0.45%), XLK **−0.18%**, XLU **−0.02%**, XLP **+0.32%**. That is **not** a funding-source rotation this morning.

**S0 = 0.** Mixed: leftover growth-beta vs no live rip, no live rates smash, oil offering, curve unchanged. Extra confirms in the dominant bucket: **none**. Do not HIT Barkin/2Y. Do not pay Monday’s −1.89% rel twice.

**2. Spine / secondary.**
- **Data-center load growth / power demand upside:** structural HIT, **stale** for a 1d call. Moody’s ~$110B generation, House Ratepayer Protection Act (09-16), SO/Google Vogtle–Hatch uprates (09-21) are **not** a same-session XLU-wide impulse. Rubric: do **not** let the multi-year AI-power story override a 1d rate tape. 08-12 dampener only.
- **Rates falling (bond-proxy bid):** **MISS**. Live 10Y 4.97% / 30Y ~5.28%, bond futures tiny red.
- **Rates rising (bond-proxy selloff):** **CARRIED / not a fresh HIT**. Stress-zone long end is already in the 1w/1m tape. Open-experiment extra-confirm **fails**.
- **Risk-on rotation away from utilities:** **MISS as a live HIT**. Leftover Nasdaq is yesterday; this morning’s sector board has cyclicals red and XLU flat. 09-17: rotation is relative, not an absolute ceiling.
- **Risk-off tape / flight to safety:** **MISS**. VIX 14.88 contango, no FTS.
- **Nuclear / gas generation policy support:** SO/Google nuclear (09-21) — single-name, 08-28. Stale for the ETF.
- **Grid CapEx / favorable ROE:** structural / nested (AES Ohio, water rate-recovery). MAP HEAT parent **Diversified = flat**. Do not average water **up** or IPP **SPLIT down** into XLU.
- **Adverse rate case / load-growth disappointment / regulatory smash:** carried (Texas interconnection, Duke equity needs). Not a same-session ETF driver.
- **Sector rotation into/out of utilities:** out **already printed Monday**. Not a fresh HIT today.

Net: no live spine HIT. **S1 = 0.**

**3. Breadth.** MAP HEAT: Diversified tape still red, regulated-electric breadth **0.098** (group not bid), IPP breadth **0.0** (SPLIT — do not promote). Captains NEE/SO tagged pos in heat, tape not. 08-13: do not pay the lag in S2 **and** S4. **S2 = 0.**

**4. Flows / positioning.** Checked: no confirmed same-day XLU inflow spike or outflow lid; 09-21 volume ~in-line (~21M). 1m rel −8.10% is the opposite of a crowded long. **S3 = 0.** (checked, nothing material)

**5. Catalysts.** No CPI/FOMC. 2Y auction ≠ long-end. Barkin two-sided. Ex-div paid. SO/Google and NEE/Dominion are **not** XLU 1d engines (08-28). size_gate=True.

## Self-audit
- **Lens:** XLU near-session, not SPX, not NEE/SO/CEG.
- **Band:** size_gate on; 08-12 caps notable from AI-power; |leading S0–S3| = 0.
- **Skew:** not manufacturing up from AI load; not manufacturing down from leftover lag as the thesis.
- **Same-shock:** rates once in S0 (carried, not HIT); rotation not double-counted in S1.
- **Single-ticker:** IPP SPLIT / SO nuclear / NEE merger **must not** drive the ETF.
- **Divergence:** leading S0–S3 = **0** vs S4 = **−1**. Flag it. **Trust factors over tape** → residual is **flat**, not a signed down from confirmation.

Open experiment (this scope): extra-confirm withheld full S0/S1 weight (applied). Milder-band experiment when |score|<4: applicable; confidence shrunk (mag hit-rate 0.2).

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 0
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -1
MULTIPLIER: 0.9
CONFIDENCE: 0.52
REGIME: mixed
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|PARTIAL|0.45|2026-09-22|https://www.benzinga.com/quote/XLU
Risk-off tape / flight to safety|MISS|0.80|2026-09-22|channel1
Real yields rising|CARRIED|0.70|2026-09-18|channel1
Real yields falling|MISS|0.75|2026-09-22|https://countryeconomy.com/bonds/usa
USD strengthening|MISS|0.70|2026-09-22|channel1
USD weakening|MISS|0.70|2026-09-22|channel1
Sector breadth expansion (% names up)|MISS|0.75|2026-09-22|map_heat
Sector breadth failure (ETF up, names flat)|MISS|0.70|2026-09-22|map_heat
Large-cap leadership inside sector|PARTIAL|0.50|2026-09-22|map_heat
Small/mid leadership inside sector|MISS|0.60|2026-09-22|map_heat
High-beta leadership inside sector|MISS|0.65|2026-09-22|map_heat
Low-beta leadership inside sector|PARTIAL|0.45|2026-09-22|channel1
Sector ETF inflow / relative volume spike|MISS|0.55|2026-09-22|web_search
Sector ETF outflow / volume dry-up|MISS|0.55|2026-09-22|web_search
Crowded long (extreme relative performance + valuation)|MISS|0.80|2026-09-21|channel1
Index rebalance / inclusion tailwind|MISS|0.85|2026-09-21|channel1
Index exclusion / forced selling|MISS|0.85|2026-09-21|channel1
Data-center load growth / power demand upside|STALE|0.70|2026-09-22|https://www.energyconnects.com/news/renewables/2026/september/us-ai-boom-needs-110-billion-of-new-power-plants-moody-s-says/
Rates falling (bond-proxy bid)|MISS|0.80|2026-09-22|https://countryeconomy.com/bonds/usa
Favorable rate case / allowed ROE|PARTIAL|0.40|2026-09-22|map_heat
Nuclear / gas generation policy support|STALE|0.60|2026-09-21|https://www.reuters.com/legal/litigation/southern-co-unit-signs-deal-with-google-add-nuclear-capacity-2026-09-21/
Grid CapEx approval / recovery|STALE|0.55|2026-09-22|https://www.utilitydive.com/news/2026-q2-roundup-utilities-emphasize-project-execution-ratepayer-protec/827997/
Rates rising (bond-proxy selloff)|CARRIED|0.70|2026-09-22|https://www.morningstar.com/news/dow-jones/202609217076/utilities-down-as-treasury-yields-hover-around-multiyear-highs-utilities-roundup
Adverse rate case|MISS|0.60|2026-09-22|web_search
Load growth disappointment|CARRIED|0.45|2026-09-07|https://247wallst.com/investing/etf/2026/09/07/xlus-ai-power-story-crumbles-as-texas-freezes-data-center-demand/
Regulatory disallowance / project cancel|MISS|0.60|2026-09-22|web_search
Risk-on rotation away from utilities|CARRIED|0.65|2026-09-21|channel1
Sector rotation into utilities|MISS|0.70|2026-09-22|channel1
Sector rotation out of utilities|CARRIED|0.70|2026-09-21|channel1
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- US 10 year Treasury yield today September 22 2026
- XLU utilities ETF premarket news flows September 22 2026
- economic calendar September 22 2026 US Treasury auction CPI FOMC
- data center power demand utilities rate case nuclear grid CapEx September 2026
- CME FedWatch September 2026 October hike odds
- XLU ETF flows volume breadth NEE SO DUK September 22 2026
- US 30 year Treasury yield September 22 2026
- Richmond Fed manufacturing Williams Jefferson Barkin September 22 2026
- utilities sector rotation out of XLU risk on September 2026
- XLU dividend ex-date September 2026
- utilities stocks September 22 2026 NextEra Duke Southern news
- US 2 year note auction September 22 2026 Treasury
- X search: XLU utilities ETF vs SPY rotation yields 10-year today September 22 2026 (2026-09-21 to 2026-09-22)
- web_fetch: https://countryeconomy.com/bonds/usa

**Key sources (title + URL + timestamp / as-of)**
- CountryEconomy US 10Y — https://countryeconomy.com/bonds/usa — fetched 2026-09-22T10:52:46Z — **4.97% on 09/22, unchanged vs 4.96% on 09/21** (vs 5.00% on 09/18).
- GuruFocus / MacroMicro / Trading Economics 10Y cluster — ~4.93–4.95% live quotes vs prior close ~4.96%.
- GuruFocus / Trading Economics 30Y — ~5.27–5.29% on 2026-09-22.
- TreasuryDirect 2Y announcement — https://www.treasurydirect.gov/instit/annceresult/press/preanre/2026/A_20260917_7.pdf — **$69B 2-year notes, auction 2026-09-22**, competitive close 1:00 PM ET (front-end, not 10Y/30Y).
- FedRateCalc / Yahoo FOMC calendar — no CPI or FOMC on 09-22; FOMC printed 09-16; next CPI 10-14.
- TipRanks / Richmond Fed media advisory — Richmond Fed manufacturing ~10:00 ET; Barkin speech 1:00 PM ET Baltimore (two-sided).
- MacroOdds / prediction markets — Oct FOMC **~55–58% hike / ~42–44% hold** (carried post-09-16).
- Morningstar/Dow Jones utilities roundup — https://www.morningstar.com/news/dow-jones/202609217076/utilities-down-as-treasury-yields-hover-around-multiyear-highs-utilities-roundup — utilities down as yields hover near multiyear highs (09-21 tape, not a fresh 09-22 smash).
- Reuters — Southern Co unit / Google nuclear uprates — https://www.reuters.com/legal/litigation/southern-co-unit-signs-deal-with-google-add-nuclear-capacity-2026-09-21/ — **09-21 single-name**, not XLU-wide 1d.
- Energy Connects / Moody’s — https://www.energyconnects.com/news/renewables/2026/september/us-ai-boom-needs-110-billion-of-new-power-plants-moody-s-says/ — structural AI-power CapEx, stale for 1d.
- Utility Dive Q2 roundup — https://www.utilitydive.com/news/2026-q2-roundup-utilities-emphasize-project-execution-ratepayer-protec/827997/ — execution / ratepayer protection, not a same-session ETF impulse.
- Slickcharts / MarketBeat — XLU ex-div **2026-09-21**, pay 09-23, ~$0.3004 (already printed).
- Tradesmith / MarketWatch — 09-21 XLU close ~$40.66, volume ~21M; NEE −1.04%, DUK −0.67%.
- X search 09-21→09-22 — **checked, nothing material** on XLU vs SPY / 10Y rotation.

**Facts taken**
- Live 10Y **unchanged at 4.97%** (not FRED 5.01 “today”).
- No CPI/FOMC; 2Y auction only; Barkin/Richmond Fed two-sided.
- XLU PM **−0.02%**, XLK **−0.18%**, cyclicals red on the Channel 1 sector board.
- 4-horizon XLU/SPY rel all negative; |1d rel| **1.89%**.
- Oil offering; VIX 14.88 contango; Oct hike odds ~mid-50s carried.
- AI-power / nuclear / CapEx items are **structural or single-name**, not a fresh XLU 1d catalyst.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': 0.0, 'divergence_flagged': False, 'total_score': -0.286, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.511, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': -0.027, 'score': -0.162, 'legs': [{'leg': 'ES', 'pct': -0.07, 'w': 0.3}, {'leg': 'ZN', 'pct': -0.03, 'w': 0.75}, {'leg': 'PM:XLU', 'pct': -0.02, 'w': 0.7}]}, 'overlay_score': 0.0, 'overlay_raw': 0.0, 'index_carry': -0.124, 'general_total': -0.497, 'skill_multipliers': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.52, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
