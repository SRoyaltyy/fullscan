# Sector Prediction — Utilities — 2026-09-21

- ETF: **XLU**
- rubric: `00_grounding/sectors/utilities.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-1.439** (mult 0.9)
- regime: risk_on
- divergence_flagged: **False**
- engine: v2 · tape_anchor **-1.957** (ES +1.35%, ZN -0.03%, PM:XLU -0.63%) · index_carry **3.218** (general 12.871) · llm_overlay **-2.7** (raw -2.7)

## Channel 1 sector ETF tape

```
ETF XLU vs SPY (yfinance, through 2026-09-18):
  1d: XLU -1.42% | SPY +0.13% | rel -1.54%
  3d: XLU -0.53% | SPY +0.82% | rel -1.35%
  1w: XLU -3.04% | SPY -0.09% | rel -2.95%
  1m: XLU -6.63% | SPY -0.71% | rel -5.92%
```

MEMORY_CONFIRM: Utilities/XLU only — memory index paused (`openclaw memory status --index` / `openclaw memory index --force`); used injected sector logs + Channel 1 + Channel 2. Rolling dir=0.4 / mag=0.2 (n=10); last 30 dir=0.381 / mag=0.333 (n=21). Last graded: 09-18 flat/flat vs XLU −1.42% / SPY −0.12% / rel −1.30% (dir MISS — post-relief reversal slot collapsed to S0=0 all-clear). 09-17 flat/flat vs +0.90% (dir MISS). 09-16 down/mild vs 0.0% (dir MISS). Applied: 09-18 (keep extra-confirm gate on FULL smash weight; do **not** restack Friday’s already-printed +5–6 bp backup / −1.42% close as a second smash; leftover after a paid down-twin is two-sided again, not an all-clear **and** not a license to re-HIT rates); 09-17 (leave a duration slot; rotation-away is relative, not an absolute ceiling; prefer size-gated mild over flat/flat); 09-16 (do not restack the paid 09-16 hike; AM risk-on alone does not sign the close; do not let trailing 1w/1m lag pay another down close); 09-14 (**binds**: 1d/3d/1w/1m rel all < 0 **and** |1d rel| 1.54% ≥ ~1% → S4 ≥ −1.0); 09-11 (risk-on inputs are headwinds for a defensive, not cushions; **no** CPI-class binary today — do **not** apply the S0=−1 both-branches template to CFNAI/Goolsbee); 09-10 (VIX 14.98 / VIX3M 0.821 contango fails VIX≥20 FTS gate → no 08-18 relative-beat; sticky long end = relative-LAG); 09-09 (1d rel −1.54% is not a ≥0.4% cushion — no flat override); 09-08 (oil **offering** from war-premium = inflation channel fading, not FTS); 08-28 (do **not** promote IPP CEG/VST or Duke’s NC turbine deny into S1); 08-27 (NQ leads ES, ASML/AI already public → relative lag / flat-to-down absolute unless a fresh same-session yield impulse — **partial**: live 10Y ~4.95% vs Friday ~5.00%, not a smash and not a 09-17-style −6 bp grind already in the PM print); 08-25 (S1 ≠ 0, so the “don’t manufacture down from carried S2/S3” gate does not fully bind); 08-21 (live 10Y ~4.95%, not FRED 09-17 4.94 as “today’s move”); 08-13 (one trailing rel print does not pay S2 **and** S4 — S4 takes the 09-14 floor; S2 stays 0); 08-12 (AI-power is a 1d dampener, not a band engine). Open experiment (09-16/09-17/09-18 losses): extra confirm before full weight in the dominant bucket — used NQ-vs-ES + XLK vs XLU PM + Asia/Europe for rotation; live 10Y/30Y + ZN/ZB for duration; **no extra confirm for a fresh rates smash** (curve is easing off Friday, bond futures only tiny red). Scope do-instead: tape **agrees** with the negative S1 (no sign conflict → do not flatten from that rule). Same-shock: paid FOMC + Friday backup counted in S4 tape only, not re-HIT in S0/S1; rotation-away in S1 only.

---

# Utilities (XLU) — 2026-09-21

Object is the **near-session XLU environment**, not SPX and not a stock pick.

## Channel 1 (trusted, not re-derived)

XLU vs SPY through 2026-09-18: **1d −1.42% / +0.13% (rel −1.54%)**; **3d −0.53% / +0.82% (rel −1.35%)**; **1w −3.04% / −0.09% (rel −2.95%)**; **1m −6.63% / −0.71% (rel −5.92%)**. Freshest 1d is a **clean lag on a flat-to-up SPY day**. Do **not** smuggle a relative-beat clause. Do **not** let 1w/1m soften or harden the 1d print into a second smash — they are a structural descriptor (09-16).

**HORIZON_3D:** lag (−1.35% rel). **HORIZON_1W:** lag (−2.95% rel). **HORIZON_2W:** lag (no independent 2w print; 1w and 1m both deep red). **HORIZON_1M:** deep lag (−5.92% rel).

Macro: VIX **14.98** (+0.17 1d, −2.12 1w), **VIX/VIX3M 0.821 — CONTANGO** (no stress); DGS10 **4.94** as of 09-17 (−7 bp 1d, −1 bp 1w, **+23 bp 1m**); DGS30 **5.29** (−6 bp 1d, −8 bp 1w, +1 bp 1m); DFII10 **2.61** (−7 bp 1d, **+6 bp 1w, +20 bp 1m**); HY OAS 2.70 (tight); EPU 342.15 (+163 1d — carried uncertainty spike, not a same-session XLU catalyst); **CL=F −5.94% / BZ=F −5.76%** vs Finviz WTI **$104.16 −1.59%** / Brent **$107.67 −1.02%** (both tapes **offering** from war-premium); DXY ~flat (+0.08% 1d); **ES=F +1.35% / NQ=F +2.12%** vs prior close vs Finviz live **ES +0.20% / NQ +0.41% / RTY +0.08% / DJIA +0.11%** (NQ leads either feed; vs-close tape is a **≥0.5% rip**, live tape is modest); **XLU PM −0.63%** vs XLK **+0.98%**, XLC +0.59%, XLP −0.65%, XLRE −0.05%, XLV −0.30%, XLE −1.29% — XLU is **non-haven**, tied-worst among defensives with XLP; Asia **+1.04%**; Europe **+0.95%**; 5-day 10Y–SPX corr **−0.592**. Bond futures: 10Y note **−0.03%**, 30Y **−0.06%**, Ultra Bond **−0.06%** — a **tiny backup**, not a smash and not relief.

**Live curve (08-21):** CountryEconomy **10Y 4.95% on 09/21 vs 5.00% on 09/18 (−5 bp)**; cluster ~4.95–4.96% (GuruFocus/Investing). FRED 09-17 **4.94%**. Friday’s close was the **paid backup** (10Y +5–6 bp to ~5.00%, XLU −1.42%). This morning is a **partial retrace of Friday**, not a fresh rip and not a 09-17-style duration-up already showing in the XLU print. Do **not** pay FRED 09-17 4.94%, Wednesday’s FOMC, or Friday’s 5.00% twice.

**Calendar (08-14 / 09-04 / 09-11):** **Monday 09-21 is light.** No 8:30 CPI/PCE/NFP. No FOMC (printed 09-16). **No long-end auction** (13w/26w bills only; 2Y/5Y/7Y later this week). **S&P 500 rebalance effective today: zero utilities add/delete** (BE/ILMN/Everpure in; TAP/TTD/BLDR out). **CFNAI + Goolsbee** are not CPI-class; Goolsbee is two-sided event risk (do not mint direction). **XLU ex-dividend today (~$0.30, ~0.73% of Friday’s $41.10 close)** — mechanical price-drop bias, **not** a sector-factor HIT.

**MAP HEAT (nested, do not average into parent):** Diversified / regulated electric / gas = **flat**; water up (not an XLU driver); **IPP SPLIT down (CEG/VST) and renewables SPLIT down — 08-28: do not let IPP drive the ETF.** `size_gate=True`.

## Channel 2

**1. Shared macro → this sector.** Classical map is **real/nominal yields**; AI load is a structural offset only.

- **Tape is risk-on, NQ-led.** ES +1.35% / NQ +2.12% vs prior close; Asia +1.04%; Europe +0.95%; XLK PM +0.98% vs XLU −0.63%; VIX 14.98 in deep contango. Per 09-11, for a defensive those are **headwinds, not cushions**. Per the defensive-as-funding-source rule, S0 is **0 to slightly negative, not positive** — capital is a **source**, not a destination. Do **not** apply the 09-11 S0=−1 CPI both-branches template: there is **no** high-impact binary.
- **FOMC is paid (09-16).** Unanimous +25 bp to 3.75–4.00%; Oct hike odds ~57%. Per 09-16, do **not** restack the hike.
- **Friday’s rates smash is paid.** 10Y ~5.00%, captains NEE/SO/DUK ~−1%, XLU −1.42% / rel −1.54%. Per 09-18, that was the down-twin after 09-17’s −6 bp relief. **Do not re-HIT it.** Live 10Y **~4.95% (−5 bp vs Friday)** with ZN only −0.03% is **not** extra confirm for a second smash. It **is** a modest duration slot (09-17), but XLU PM is **red** (and partly ex-div), so it does **not** license S0+.
- **Oil offering hard** from $104+ (CL −5.9% / WTI −1.6%). 09-08: elevated oil is an inflation/duration negative when **rising**; today it is **fading** — forbids a fresh oil→yields smash and does **not** mint FTS. For a defensive, oil-offering is another risk-on input.
- **09-10 gate:** VIX 14.98 < 20 and **contango** — no FTS. Sticky ~5% zone is a **relative-lag** signal, not an 08-18 relative beat. When the long end was the cause of last week’s risk-off, XLU was the transmission channel; that damage is **already in Friday’s 1d and the 1w/1m tape**.

**S0 = 0.** Mixed: risk-on mapping vs modest live ease off a paid Friday backup; FOMC paid; oil offering; no binary. Extra-confirm gate **blocks** full smash weight. Do **not** score S0+ from the 5 bp retrace while XLU PM is non-haven. Do **not** score S0−1 from carried real yields / paid hike.

**2. Spine / secondary.**
- **Data-center load growth / power demand upside:** structural HIT, **stale** for a 1d call. Capex/load pipelines (Duke $103B, SO $81B, AEP, SO standby-power) are multi-month. Rubric: do **not** let the multi-year AI-power story override a 1d rate/rotation tape without a fresh XLU-wide catalyst. Dampener only (08-12).
- **Rates falling (bond-proxy bid):** **PARTIAL.** Live 10Y 4.95 vs Friday 5.00. Not independently ripping lower on the bond-futures tape. Not a full HIT.
- **Rates rising (bond-proxy selloff):** **MISS as fresh.** Friday’s backup is in the price. Live curve is **not** independently rising. Do **not** HIT and do **not** double-count with S0.
- **Risk-on rotation away from utilities:** **HIT.** NQ leads, XLK PM +0.98% vs XLU −0.63%, XLP also red, VIX contango, Asia/Europe green. Extra confirms in the dominant bucket (open experiment). Count **once** here, not again as “sector rotation out.”
- **Risk-off tape / flight to safety:** **MISS.**
- **Favorable rate case / allowed ROE:** stale/mixed; not a same-session ETF driver.
- **Nuclear / gas generation policy support / Grid CapEx:** structural, stale.
- **Adverse rate case / Regulatory disallowance:** Duke NC commission denied a $584M gas turbine near an Amazon data-center site (~09-18) — **single-name**, 08-28 forbids promoting it into S1. NEE/Dominion merger concessions are the same class.
- **Load growth disappointment:** checked, nothing material XLU-wide today (queue-vetting stories are multi-month).
- **Index rebalance / inclusion tailwind:** **MISS** (no utilities add/delete).
- **IPP SPLIT (CEG/VST):** MAP HEAT down/high conv — **do not drive XLU**.

Net: **live rotation-away** is the only fresh full-weight spine; rates-falling is a partial offset; AI-power does not override. **S1 = −1.**

**3. Breadth.** MAP HEAT parent sleeves (diversified / regulated electric / gas) are **flat**, not a clean expansion or a clean smash. Friday’s captains moved together (~−1%); that is already in S4. No live premarket breadth expansion. Do **not** score stale 1w/1m lag into S2 (08-13 / 09-16). **S2 = 0.**

**4. Flows / positioning.** ETFDB through ~09-18: 5d **+$71M**, 1m **+$126M**, 3m +$407M — modest inflows, not a relative-volume spike, not a crowded long (1m rel −5.92% is the opposite). No confirmed same-day outflow lid. **S3 = 0.**

**5. Catalysts.** Light Monday. Ex-div is mechanical (~0.73% price bias; total-return can be flat while the printed close looks down/mild). Goolsbee two-sided. Rebalance is a non-event for this ETF. No fresh XLU-wide earnings/guidance.

**Self-audit:** Lens = 1d XLU, not SPX. Band capped mild (`size_gate`, |leading| modest, 08-12). Skew = relative lag on NQ-led risk-on; no 08-18 beat. Same-shock: Friday yields / FOMC not paid twice. Single-ticker (DUK turbine, CEG/VST, NEE deal) not driving the ETF. AI-power not used as a 1d override.

**Divergence:** Leading S0–S3 net **−1**; S4 **−1**. Same sign — **no fight**. Trust factors; tape confirms. Do not let ES/NQ tape_anchor mint up while PM:XLU is the worst/tied-worst defensive (index is not a participation certificate).

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: -1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -1
MULTIPLIER: 0.9
CONFIDENCE: 0.58
REGIME: risk_on
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|HIT|0.80|2026-09-21|https://www.reuters.com/business/wall-st-futures-rise-ai-stocks-gain-oil-prices-slide-2026-09-21/
Risk-off tape / flight to safety|MISS|0.75|2026-09-21|https://www.reuters.com/business/wall-st-futures-rise-ai-stocks-gain-oil-prices-slide-2026-09-21/
Real yields rising|PARTIAL|0.55|2026-09-21|Channel 1 DFII10 2.61 as of 2026-09-17 (+6 bp 1w / +20 bp 1m; 1d −7 bp — carried, not a fresh 1d rip)
Real yields falling|PARTIAL|0.55|2026-09-21|https://countryeconomy.com/bonds/usa
USD strengthening|MISS|0.60|2026-09-21|Channel 1 DXY 1d +0.08%
USD weakening|MISS|0.60|2026-09-21|Channel 1 DXY 1d +0.08%
Sector breadth expansion (% names up)|MISS|0.55|2026-09-21|MAP HEAT diversified/regulated electric/gas dir=flat
Sector breadth failure (ETF up, names flat)|MISS|0.60|2026-09-21|Channel 1 XLU 1d −1.42% (ETF not up)
Large-cap leadership inside sector|PARTIAL|0.50|2026-09-21|MAP HEAT NEE:mixed, SO:pos, captains ~−1% on 09-18
Small/mid leadership inside sector|MISS|0.50|2026-09-21|MAP HEAT parent sleeves flat; IPP/renewables SPLIT not XLU
High-beta leadership inside sector|MISS|0.55|2026-09-21|MAP HEAT IPP SPLIT down (CEG/VST) — not XLU driver
Low-beta leadership inside sector|PARTIAL|0.50|2026-09-21|MAP HEAT regulated sleeves flat / XLU PM non-haven
Sector ETF inflow / relative volume spike|MISS|0.55|2026-09-21|https://etfdb.com/etf/XLU/
Sector ETF outflow / volume dry-up|MISS|0.55|2026-09-21|https://etfdb.com/etf/XLU/
Crowded long (extreme relative performance + valuation)|MISS|0.70|2026-09-21|Channel 1 1m rel −5.92%
Index rebalance / inclusion tailwind|MISS|0.80|2026-09-21|https://press.spglobal.com/2026-09-04-Bloom-Energy,-Illumina,-and-Everpure-Set-to-Join-S-P-500-Others-to-Join-S-P-100,-S-P-MidCap-400,-and-S-P-SmallCap-600
Index exclusion / forced selling|MISS|0.80|2026-09-21|https://press.spglobal.com/2026-09-04-Bloom-Energy,-Illumina,-and-Everpure-Set-to-Join-S-P-500-Others-to-Join-S-P-100,-S-P-MidCap-400,-and-S-P-SmallCap-600
Data-center load growth / power demand upside|HIT|0.60|2026-09-21|https://www.morningstar.com/economy/utilities-stocks-plunge-yet-outlook-remains-positive-with-data-center-boom
Rates falling (bond-proxy bid)|PARTIAL|0.60|2026-09-21|https://countryeconomy.com/bonds/usa
Favorable rate case / allowed ROE|MISS|0.45|2026-09-21|checked, nothing material XLU-wide
Nuclear / gas generation policy support|MISS|0.40|2026-09-21|checked, nothing material
Grid CapEx approval / recovery|PARTIAL|0.45|2026-09-21|https://www.utilitydive.com/news/103-billion-duke-claims-largest-spending-plan-of-any-regulated-us-utility/812047/
Rates rising (bond-proxy selloff)|MISS|0.65|2026-09-21|https://countryeconomy.com/bonds/usa
Adverse rate case|MISS|0.50|2026-09-21|https://www.wral.com/news/state/duke-energy-gas-turbine-amazon-data-center-september-2026/
Load growth disappointment|MISS|0.45|2026-09-21|checked, nothing material
Regulatory disallowance / project cancel|PARTIAL|0.50|2026-09-21|https://www.wral.com/news/state/duke-energy-gas-turbine-amazon-data-center-september-2026/
Risk-on rotation away from utilities|HIT|0.78|2026-09-21|https://www.reuters.com/business/wall-st-futures-rise-ai-stocks-gain-oil-prices-slide-2026-09-21/
Sector rotation into utilities|MISS|0.70|2026-09-21|Channel 1 XLU PM −0.63% vs XLK +0.98%
Sector rotation out of utilities|HIT|0.70|2026-09-21|Channel 1 XLU vs SPY 1d rel −1.54%; PM XLU −0.63% vs XLK +0.98%
HIT_GRID_END

---

## RESEARCH APPENDIX

**Queries run**
- memory_search: Utilities XLU sector prediction lessons duration yields 2026-09-21 (index unavailable)
- web_search: 10 year Treasury yield today September 21 2026
- web_search: XLU utilities ETF premarket September 21 2026 rotation yields
- web_search: US Treasury auction calendar September 21 2026 10-year 30-year
- web_search: S&P 500 rebalance September 21 2026 utilities XLU
- web_search: XLU ex-dividend date September 21 2026
- web_search: US economic calendar Monday September 21 2026
- web_search: utilities sector rotation data center power demand September 2026
- web_search: XLU ETF flows inflows outflows September 2026
- web_search: utility rate case news NextEra Duke Southern September 2026
- web_search: 10 year Treasury yield live September 21 2026 4.96
- web_search: CME FedWatch October 2026 hike odds September 21
- web_search: XLU dividend amount September 2026 State Street SPDR
- web_search: utilities stocks Monday September 21 2026 NEE DUK SO XLU
- web_search: risk on rotation away from utilities defensives September 21 2026
- web_fetch: https://countryeconomy.com/bonds/usa
- x_search: XLU utilities ETF premarket yields 10Y today September 21 2026 (2026-09-18..2026-09-21)

**Key sources (title + URL + timestamp / as-of)**
- CountryEconomy US 10Y — https://countryeconomy.com/bonds/usa — fetched 2026-09-21T10:44Z. Facts: 09/21 4.95% (−0.05); 09/18 5.00% (+0.06); 09/17 4.94% (−0.06).
- Reuters futures/AI/oil — https://www.reuters.com/business/wall-st-futures-rise-ai-stocks-gain-oil-prices-slide-2026-09-21/ — 2026-09-21. Facts: Wall St futures rise, AI stocks bid, oil slides (risk-on, not FTS).
- Morningstar utilities roundup — https://www.morningstar.com/news/dow-jones/202609187441/utilities-down-as-treasury-yields-test-multiyear-highs-utilities-roundup — 2026-09-18. Facts: utilities down as yields test multiyear highs; NEE/DUK/SO ~−1% Friday.
- S&P DJI rebalance — https://press.spglobal.com/2026-09-04-Bloom-Energy,-Illumina,-and-Everpure-Set-to-Join-S-P-500-Others-to-Join-S-P-100,-S-P-MidCap-400,-and-S-P-SmallCap-600 — 2026-09-04, effective 2026-09-21. Facts: no utilities add/delete.
- Treasury auction calendar (Timsun / Fiscal Data) — https://timsun.net/rates/auctions — as of mid/late Sep 2026. Facts: 09-21 is 13w/26w bills only; no 10Y/30Y; next coupons 09-22+.
- ETFDB XLU flows — https://etfdb.com/etf/XLU/ — ~Sep 18 2026. Facts: 5d +$71.35M, 1m +$125.87M, 3m +$407.23M.
- Scotiabank Sep 2026 calendar — https://www.scotiabank.com/ca/en/about/economics/economics-publications/post.other-publications.calendar-of-economic-release-dates.calendar-of-economic-release-dates--september-2026-.html — Facts: 09-21 listed as no major US releases; CFNAI / Goolsbee / bill auctions only.
- XLU ex-div — https://stockevents.app/en/stock/XLU/dividends — Facts: ex-date 2026-09-21, ~$0.30, payable ~09-23.
- WRAL Duke NC turbine deny — https://www.wral.com/news/state/duke-energy-gas-turbine-amazon-data-center-september-2026/ — ~Sep 18 2026. Facts: NCUC denied $584M gas turbine near Amazon data-center site (single-name; not scored into S1).
- Utility Dive Duke capex — https://www.utilitydive.com/news/103-billion-duke-claims-largest-spending-plan-of-any-regulated-us-utility/812047/ — Facts: Duke five-year capex $103B (structural, stale for 1d).
- Morningstar AI-power utilities — https://www.morningstar.com/economy/utilities-stocks-plunge-yet-outlook-remains-positive-with-data-center-boom — Facts: structural data-center load still intact after the plunge (1d dampener only).
- CME FedWatch / secondary — https://growbeansprout.com/tools/fedwatch — ~Sep 21 2026. Facts: Oct 25 bp hike odds ~57%.
- Channel 1 panel (injected, not altered) — 2026-09-21 premarket: VIX 14.98 / ratio 0.821; ES +1.35% / NQ +2.12%; XLU PM −0.63%; XLU vs SPY 1d rel −1.54%; CL=F −5.94%.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -3.0, 'divergence_flagged': False, 'total_score': -1.439, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.558, 'regime': 'risk_on', 'engine': 'v2', 'anchor': {'available': True, 'pct': -0.3262, 'score': -1.957, 'legs': [{'leg': 'ES', 'pct': 1.35, 'w': 0.3}, {'leg': 'ZN', 'pct': -0.03, 'w': 0.75}, {'leg': 'PM:XLU', 'pct': -0.63, 'w': 0.7}]}, 'overlay_score': -2.7, 'overlay_raw': -2.7, 'index_carry': 3.218, 'general_total': 12.871, 'skill_multipliers': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.58, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
