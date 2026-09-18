# Sector Prediction — Utilities — 2026-09-08

- ETF: **XLU**
- rubric: `00_grounding/sectors/utilities.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **-2.158** (mult 0.9)
- regime: mixed
- divergence_flagged: **True**
- engine: v2 · tape_anchor **2.961** (ES +2.02%, ZN -0.15%) · index_carry **-2.869** (general -11.475) · llm_overlay **-2.25** (raw -2.25)

## Channel 1 sector ETF tape

```
ETF XLU vs SPY (yfinance, through 2026-09-17):
  1d: XLU +0.90% | SPY +1.13% | rel -0.24%
  3d: XLU -0.31% | SPY +0.23% | rel -0.54%
  1w: XLU -1.95% | SPY +0.63% | rel -2.58%
  1m: XLU -5.29% | SPY -0.63% | rel -4.66%
```

I have enough. Let me compile.

**MEMORY_CONFIRM: Utilities/XLU only** — memory index paused this run (`openclaw memory status --index`); used injected sector logs + Channel 1 + Channel 2. Rolling dir=0.4 / mag=0.3 (n=10); last 30 dir=0.421 / mag=0.368 (n=19). Last graded: 09-17 flat/flat vs XLU +0.90% / SPY +1.13% / rel −0.24% (dir MISS — the post-FOMC duration slot was left unfilled; S1 rotation-away was treated as an absolute ceiling and S0=0 was read as "duration-up is closed today"). Applied: 09-17 (leave a two-sided duration slot after a paid FOMC; do not treat rotation-away as an absolute ceiling; prefer flat/mild over flat/flat); 09-16 (do not restack the paid hike as a fresh smash; do not let trailing 1w/1m lag pay another down close); 09-14 (when 1d/3d/1w/1m rel all negative AND freshest 1d rel ≤ −1%, S4 ≥ −1.0 — **does not bind today**, freshest 1d rel is −0.24%); 09-11 (risk-on inputs are headwinds for a defensive, not cushions — but the 09-17 lesson qualifies this: headwind ≠ absolute ceiling); 09-10 (VIX 15.44 / VIX3M 0.832 contango fails the VIX≥20 FTS gate → no 08-18 relative-beat claim); 09-09 (1d rel −0.24% is not a cushion — no cushion-as-flat override); 09-08 (oil **offering** hard, third day = inflation channel fading, not FTS); 08-28 (Warsh already public — no notable-down minted from a paid hawkish branch); 08-27 (NQ leads ES, mega-cap/AI already public → relative lag / flat-to-down absolute unless a fresh same-session yield impulse — **one is now live**: 10Y −6bp to 4.94% on 09-17); 08-25 (S0/S1 not both 0 — gate does not bind); 08-21 (live 10Y ~4.94–5.01, not FRED 09-16 5.01 as "today's move"); 08-13 (one trailing rel print does not pay S2 and S4); 08-12 (AI-power is a 1d dampener, not a band engine). Open experiment (09-16/09-17 losses): extra confirm before full weight in the dominant bucket — used live 10Y/30Y/real yield + bond futures + oil + VIX term structure + XLU PM + MAP HEAT. Scope do-instead: cut conviction vs 1d-rel conflict; keep mild. Same-shock: carried yields/FOMC in S0 only; rotation-away in S1 only.

---

# Utilities (XLU) — 2026-09-18

Object is the **near-session XLU environment**, not SPX and not a stock pick.

## Channel 1 (trusted, not re-derived)

XLU vs SPY through 2026-09-17: **1d +0.90% / +1.13% (rel −0.24%)**; **3d −0.31% / +0.23% (rel −0.54%)**; **1w −1.95% / +0.63% (rel −2.58%)**; **1m −5.29% / −0.63% (rel −4.66%)**. Freshest 1d is a **mild lag on a strong up day** — XLU participated (+0.90%) but underperformed. 1w/1m remain a deep, widening lag. Horizons: **3d mild lag**, **1w lag**, **2w lag** (no independent 2w print; 1w and 1m both red), **1m deep lag**.

Macro: VIX **15.44** (−2.27 1d, −2.4 1w), **VIX/VIX3M 0.832 — CONTANGO** (deep; no stress); DGS10 **5.01** as of 09-16 (+1 bp 1d, **+18 bp 1w, +29 bp 1m**); DGS30 **5.35** (−1 bp 1d, +7 bp 1w, +4 bp 1m); DFII10 **2.68** (**+6 bp 1d, +22 bp 1w, +24 bp 1m**); HY OAS 2.70 (−6 bp 1d, tight); EPU 106.54 (−149.78 1d, −385 1w — policy uncertainty collapsing); RRP 0.276 (−5.1 1d); **CL=F −1.24% / BZ=F −1.76%** (Finviz WTI **$94.40 +3.18%** / Brent **$99.10 +2.16%** — note the conflict: the Finviz futures tape shows crude *up* on the day while the yfinance 1d column shows crude *down*; the Bloomberg 09-18 headline says oil **dropped for a third day** as supply concerns eased and diplomacy resumed); DXY **98.915 (−0.24%)**; **ES=F +2.02% / NQ=F +2.66%** vs prior close (Finviz live futures **red**: ES −0.41%, NQ −0.22%, RTY −0.63%, DJIA −0.91% — a large divergence between the two futures feeds); **Nikkei −1.06%** (Finviz) vs **Asia composite +0.98%** (Channel 1 final: Nikkei +0.82%, Kospi +2.48%, Shanghai +1.02%); **Europe +0.84%** (FTSE +1.19%, DAX +0.70%); 5-day 10Y–SPX corr **−0.437**. Bond futures: 10Y note **−0.15%**, 30Y **−0.46%**, Ultra Bond **−0.51%** — a **backup**, not relief.

**Live curve (08-21):** 10Y **~4.94%** after the 09-17 session (Investopedia: "the 10-year Treasury yield sank back" below 5% as tech led); FRED 09-16 prints **5.01%**. So the live curve is **easing off a 19-year-high overshoot**, not ripping. Do **not** pay Wednesday's 5.01% or the FOMC twice.

**Calendar (08-14 / 09-04 / 09-11):** **Friday 09-18 is September triple/quadruple witching** — index futures, index options, stock options, and single-stock futures expire simultaneously, with the S&P quarterly rebalance effective at the close. This is a **mechanical-flow day**, not a macro-binary day. No 8:30 CPI/PCE/NFP. No FOMC (printed 09-16). No live long-end auction identified.

## Channel 2

**1. Shared macro → this sector.** The classical map is **real/nominal yields**; AI load is a structural offset only.

- **The FOMC is paid, and the first post-hike session was a duration-relief day.** The Fed hiked 25 bp to 3.75–4.00% (unanimous, first hike since 2023), telegraphed another before year-end, and Warsh flagged inflation risks. The market's reaction on 09-17 was **not** a bond-proxy smash: the 10Y **sank back below 5%**, oil fell a third day, and tech led a broad rally (SPY +1.13%). Per 09-16, do **not** restack the hike as a fresh smash — it is in the price.
- **The 09-17 lesson's duration slot is now partially filled.** The 09-17 miss was that S0=0 was read as "duration-up is closed today" with no slot for a mixed 8:30 to ease a 5% overshoot. That easing **printed** on 09-17 (10Y −6 bp to ~4.94%). But it printed **with** a risk-on tech-led tape, so XLU captured only +0.90% and **lagged** (−0.24% rel). The duration bid is real but it is being **out-competed** by rotation into high-beta.
- **Pre-tape is risk-on, NQ-led.** ES +2.02% / NQ +2.66% vs prior close (Channel 1); Finviz live futures are red but NQ still leads ES on both feeds. Europe green, Asia green. VIX 15.44 in deep contango. Per 09-11, for a defensive these are **headwinds, not cushions** — but per 09-17, a headwind is **not an absolute ceiling**.
- **Oil is offering** (Bloomberg: third down day, supply concerns easing, diplomacy shaping the US–Iran war). Per 09-08, elevated oil is an inflation/duration negative when **rising**; today it is **fading**, which forbids a fresh rates smash from oil and does **not** mint FTS. For a defensive, oil-offering is another risk-on input.
- **09-10 gate:** VIX 15.44 < 20 and **deep contango (0.832)** — no FTS. The easing long end is therefore **not** an 08-18 relative-beat claim; it is a mild absolute tailwind that the rotation-away channel largely offsets.

Net: **S0 = 0** — genuinely two-sided. The duration channel is a mild **positive** (10Y easing off a 19-year-high overshoot, oil offering, real yields still high but no longer accelerating); the risk-on/rotation channel is a mild **negative**. Do not score +1 from the easing (it is a 1-session pullback inside a stress zone, and 08-11 requires easing on the AM tape, not a noise dip); do not score −1 from the paid hike (09-16 forbids restacking).

**2. Spine / secondary.**
- **Data-center load growth / power demand upside:** structural HIT, **stale** for a 1d call. MAP HEAT shows the IPP complex as the standout (CEG +3.5%, VST +3.9%, breadth 0.889, conv=high) — but per the rubric and 08-28, **IPP is nested, not XLU confirm**; CEG/VST must not set the ETF. Regulated Electric (NEE/SO) is **down/low conv**, Regulated Gas **down/low conv**, Diversified **down/low conv**, Water **up/medium**, Renewable **flat/low**. The parent ETF is **weak**; the only strength is in the nested IPP sleeve.
- **Rates falling (bond-proxy bid):** **PARTIAL**. 10Y eased ~6 bp to ~4.94% on 09-17 and bond futures are red today (a backup), so this is a **1-session pullback inside a 19-year-high stress zone**, not a durable easing impulse. Score partial, not full.
- **Rates rising (bond-proxy selloff):** **PARTIAL / carried**. 1w DGS10 +18 bp, 1m +29 bp, DFII10 +22 bp 1w — already in the 1w/1m lag. Today the curve is **not** independently rising. Do **not** HIT and do **not** double-count with S0.
- **Risk-on rotation away from utilities:** **HIT**. ES/NQ strongly green, NQ leading, Europe/Asia green, VIX deep contango, XLK leading PM. This is the dominant fresh negative and it is what produced yesterday's −0.24% rel.
- **Risk-off tape / flight to safety:** **MISS**. VIX 15.44, deep contango, futures green — no FTS impulse.
- **Nuclear / gas generation policy support:** structural HIT, stale (DOE $1.9B loan to restart Duane Arnold, 09-08 — a week old).
- **Grid CapEx approval / recovery:** structural HIT, stale.
- **Favorable rate case / allowed ROE:** no same-session order. Halcyon tracker shows authorized ROEs trending **downward** (AII, July) — a slow structural headwind, not a 1d input.
- **Adverse rate case / load-growth disappointment / regulatory disallowance:** carried (WoodMac/Texas/Ohio; TXNM equity offering pressuring sentiment per MAP HEAT). Not fresh. Single-name must not drive the ETF (08-28).
- **Index rebalance / inclusion tailwind:** **live today** — S&P quarterly rebalance effective at the 09-18 close, on triple witching. Mechanical, one-day noise; per the taxonomy, [+] mechanical demand but [−] one-day noise. Score as a small S3 consideration, not an S1 factor.

Net: **Risk-on rotation away (HIT) + partial rates-falling (PARTIAL)** roughly offset; the structural positives are stale. **S1 = −0.5** (rotation-away is the freshest, most decisive sector-level input; the rates-falling partial is real but sub-durable and already partly paid in yesterday's +0.90%).

**3. Breadth.** 1d rel −0.24% (mild lag on a strong day), 3d rel −0.54%, 1w rel −2.58%, 1m rel −4.66%. MAP HEAT breadth is **split and mostly weak**: Regulated Electric w1 −2.73%, Regulated Gas breadth 0.312, Diversified weak, only IPP (0.889) and Water strong. XLU is **not** a broad-based bid — it is a weak parent with one hot nested sleeve. No durable breadth expansion. **S2 = −0.5**.

**4. Flows / positioning.** XLU trades **$41.32** (09-17) with **RSI 26 — oversold**, below its 50-day ($43.97). Flows: **+$124M net buying over the past month (~0.6% of AUM)** while price is **−4.3% YTD** — money is arriving into weakness, which is a mild washout/accumulation tell, not a demand spike. No relative-volume spike. The oversold RSI is a **mean-reversion setup** but not a same-day catalyst. **S3 = 0** (mild positive from oversold + steady inflows, offset by the absence of any flow reversal signal; S3 weight is ×0.5 in the engine).

**5. Catalysts.** Triple witching + S&P quarterly rebalance at the close (mechanical, two-sided). No utility-specific earnings or regulatory binary today. Oil's third down day (inflation channel easing). Post-FOMC digestion with the 10Y back below 5%.

## Divergence check

The leading factor sum (S0 0, S1 −0.5, S2 −0.5, S3 0) is **mildly negative**, while the tape confirmation (S4) is **mildly negative** (−0.24% rel, but XLU was **up** +0.90% absolute). These **agree in sign** — no divergence to flag. The genuine tension is **internal to S0**: the duration channel says up, the rotation channel says down, and they net to zero. That is a **two-sided macro**, not a leading-vs-tape fight.

**S4_ETF_TAPE = −0.5.** Confirmation only. The freshest 1d rel is −0.24% (mild lag), and the multi-horizon tape is negative on 3d/1w/1m. The 09-14 ≥1% 1d-rel floor does **not** fire (freshest 1d rel is −0.24%), so S4 stays at −0.5, not −1.0. Do not let the trailing 1w/1m lag pay twice (08-13).

## Self-audit

- **Lens:** near-session XLU environment, not SPX, not a stock pick. ✓
- **Band:** |net| is small; the scope do-instead says shrink confidence on modest |score| and keep mild. ✓
- **Skew:** the 09-17 lesson warns against treating rotation-away as an **absolute ceiling**; the 09-11 lesson warns against treating risk-on as a **cushion**. Both are honored by netting S0 to 0 and keeping S1 at −0.5 rather than −2. ✓
- **Same-shock double-count:** the paid FOMC/hike is counted **once**, in S0 as carried context, and explicitly **not** HIT in S1. The 1w/1m yield backup is counted **once**, in S2/S4 as tape, not again in S1. ✓
- **Single-ticker:** CEG/VST (IPP, +3.5%/+3.9%) are **nested overrides** and must not drive the XLU call; the parent is weak. ✓
- **Open experiment (extra confirm in dominant bucket):** the dominant bucket is rotation-away; confirmed by ES/NQ green with NQ leading, Europe/Asia green, VIX deep contango, XLK PM leading, and MAP HEAT showing regulated sub-sleeves weak. ✓

## Verdict

XLU enters a **post-FOMC, risk-on, NQ-led** session with the long end **easing off a 19-year-high overshoot** (a mild duration tailwind) but with **rotation-away dominating the sector's own tape** (1d rel −0.24%, 1w −2.58%, 1m −4.66%) and a **weak, split internal breadth** (only the nested IPP sleeve is hot). The two macro channels net to zero; the sector-specific channel is mildly negative. Triple witching adds mechanical two-sided noise. Net score is small and negative → **flat with a mild-down skew**, i.e. **flat/flat** with a relative-lag clause, not a directional down call (08-25 spirit: do not manufacture a directional down from a small net negative with no fresh decisive sector-level catalyst).

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: -0.5
S2_BREADTH: -0.5
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -0.5
MULTIPLIER: 0.9
CONFIDENCE: 0.5
REGIME: mixed
DIVERGENCE_FLAGGED: false
PREDICTED_DIRECTION: flat
PREDICTED_MAGNITUDE_BAND: flat
HORIZON_3D: down
HORIZON_1W: down
HORIZON_2W: down
HORIZON_1M: down
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|HIT|0.75|2026-09-18|https://www.investopedia.com/stock-market-today-dow-jones-s-and-p-500-09172026-12125532
Risk-off tape / flight to safety|MISS|0.80|2026-09-18|https://www.schwab.com/learn/story/stock-market-update-open
Real yields rising|PARTIAL|0.60|2026-09-18|https://fred.stlouisfed.org/series/DFII10
Real yields falling|PARTIAL|0.55|2026-09-18|https://www.investopedia.com/stock-market-today-dow-jones-s-and-p-500-09172026-12125532
USD weakening|HIT|0.55|2026-09-18|https://tradingeconomics.com/united-states/government-bond-yield
Sector breadth expansion (% names up)|MISS|0.65|2026-09-18|https://clearank.com/etf/utilities-xlu/
Sector breadth failure (ETF up, names flat)|HIT|0.60|2026-09-18|https://clearank.com/etf/utilities-xlu/
Large-cap leadership inside sector|MISS|0.50|2026-09-18|https://clearank.com/etf/utilities-xlu/
Low-beta leadership inside sector|HIT|0.55|2026-09-18|https://clearank.com/etf/utilities-xlu/
Sector ETF inflow / relative volume spike|PARTIAL|0.50|2026-09-18|https://stockanalysis.com/etf/xlu/
Sector ETF outflow / volume dry-up|MISS|0.50|2026-09-18|https://stockanalysis.com/etf/xlu/
Crowded long (extreme relative performance + valuation)|MISS|0.70|2026-09-18|https://clearank.com/etf/utilities-xlu/
Index rebalance / inclusion tailwind|HIT|0.55|2026-09-18|https://news.google.com/rss/articles/CBMie0FVX3lxTFBpSG4zaHdGbmJUX3dsUFVLUXp4LXJYd1pHNUl5eW45MWFNRjBidmJjWmxnN0hULU8wQ3N5RUp1bzdRT2VZZVdpdm9lTlBjbG1tNktpb0tjS0lzRG5rTTd6Y2R3d3VFU3VpZ1Z1NDliUWRyUEVpRzhoLVdLVQ
Data-center load growth / power demand upside|PARTIAL|0.60|2026-09-18|https://www.spglobal.com/market-intelligence/en/news-insights/research/2026/06/ai-data-center-power-demand-grid-constraints-energy-resilience
Rates falling (bond-proxy bid)|PARTIAL|0.55|2026-09-18|https://www.investopedia.com/stock-market-today-dow-jones-s-and-p-500-09172026-12125532
Nuclear / gas generation policy support|PARTIAL|0.50|2026-09-18|https://newsroom.nexteraenergy.com/news-releases
Grid CapEx approval / recovery|PARTIAL|0.45|2026-09-18|https://www.ferc.gov/
Favorable rate case / allowed ROE|MISS|0.55|2026-09-18|https://www.aii.org/new-aii-report-finds-utility-roes-have-trended-downward/
Rates rising (bond-proxy selloff)|PARTIAL|0.60|2026-09-18|https://fred.stlouisfed.org/series/DGS10
Adverse rate case|MISS|0.45|2026-09-18|https://halcyon.io/rate-case-tracker
Load growth disappointment|MISS|0.45|2026-09-18|https://www.spglobal.com/market-intelligence/en/news-insights/research/2026/06/ai-data-center-power-demand-grid-constraints-energy-resilience
Regulatory disallowance / project cancel|MISS|0.45|2026-09-18|https://halcyon.io/rate-case-tracker
Risk-on rotation away from utilities|HIT|0.70|2026-09-18|https://www.schwab.com/learn/story/stock-market-update-open
Sector rotation into utilities|MISS|0.60|2026-09-18|https://clearank.com/etf/utilities-xlu/
Sector rotation out of utilities|HIT|0.65|2026-09-18|https://clearank.com/etf/utilities-xlu/
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': -0.5, 'S2_BREADTH': -0.5, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -0.5}, 'multiplier': 0.9, 'leading_sum': -2.5, 'divergence_flagged': True, 'total_score': -2.158, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.486, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.4935, 'score': 2.961, 'legs': [{'leg': 'ES', 'pct': 2.02, 'w': 0.3}, {'leg': 'ZN', 'pct': -0.15, 'w': 0.75}]}, 'overlay_score': -2.25, 'overlay_raw': -2.25, 'index_carry': -2.869, 'general_total': -11.475, 'skill_multipliers': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.5, 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': 0.15, 'w1': 0.84}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
