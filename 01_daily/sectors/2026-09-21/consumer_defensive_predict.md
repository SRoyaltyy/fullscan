# Sector Prediction — Consumer Defensive — 2026-09-21

- ETF: **XLP**
- rubric: `00_grounding/sectors/consumer_defensive.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **-1.24** (mult 0.8)
- regime: risk_on
- divergence_flagged: **False**
- engine: v2 · tape_anchor **-1.658** (ES +1.35%, ZN -0.03%, PM:XLP -0.65%) · index_carry **3.218** (general 12.871) · llm_overlay **-2.8** (raw -2.8)

## Channel 1 sector ETF tape

```
ETF XLP vs SPY (yfinance, through 2026-09-18):
  1d: XLP -0.83% | SPY +0.13% | rel -0.96%
  3d: XLP -1.11% | SPY +0.82% | rel -1.93%
  1w: XLP -0.70% | SPY -0.09% | rel -0.60%
  1m: XLP -4.32% | SPY -0.71% | rel -3.61%
```

MEMORY_CONFIRM: Consumer Defensive / XLP only — memory index paused (embedding metadata mismatch); using injected sector scoreboard + last-10 logs, not MEMORY.md. Rolling dir=0.5 / mag=0.3 (n=10); last-30 dir=0.435 / mag=0.348 (n=23). Last graded 2026-09-18 predicted up/mild vs XLP −0.826% / SPY −0.119% / rel −0.707% (dir MISS, mag HIT): net-negative S0–S4 + non-haven PM ≤0, v2 tape_anchor/index_carry still printed up. 09-17 up/mild vs +0.192% / rel −0.942% (dir HIT on 19 bp, mag MISS) — same engine path, PM was small-green leftover beta. 09-16/09-15 flat/flat were leftover-RS veto misses. 09-14 up/mild dir HIT / mag MISS. No open experiment tagged to this sector (utilities/news only). Today I do **not** re-litigate stale WMT (08-20 / nested HEAT up is a sleeve, not the ETF), do **not** copy Friday’s paid 1d rel −0.96% or 3d/1w/1m lag into S2+S4 (08-28), do **not** apply food-crash dominance at full weight (CPB cut 09-03, T+18, no fresh print — 09-10 one-session cap), do **not** convert a theoretical FTS bid into absolute up (08-18; PM is anti-haven), do **not** accept ES tape_anchor as an XLP-up certificate (09-17/09-18; PM is already −0.65%, not flat-band leftover beta), do **not** restack the paid 09-16 FOMC / 09-15 10Y>5% smash, and I **do** treat the live NQ-led risk-on tape (ES +1.35% / NQ +2.12%, 77 bp lead, oil offered, VIX contango) as **one** anti-FTS / rotation-out object counted once in S0, with oil’s input-cost leg capped in S1.

# Consumer Defensive (XLP) — 2026-09-21

Object is the **near-session XLP environment**, not SPX and not a stock picker. Channel 1 numbers are used as given.

## Channel 1 tape (confirmation only)

```
ETF XLP vs SPY (yfinance, through 2026-09-18):
  1d: XLP -0.83% | SPY +0.13% | rel -0.96%
  3d: XLP -1.11% | SPY +0.82% | rel -1.93%
  1w: XLP -0.70% | SPY -0.09% | rel -0.60%
  1m: XLP -4.32% | SPY -0.71% | rel -3.61%
```

Friday’s print is an **already-paid anti-FTS / funding-source day**: −83 bp absolute vs SPY +13 bp (rel **−0.96%**). That is the 09-18 lesson realized — not leftover beta, a down day. S4 may describe it; it does **not** forecast a second down day by itself (08-28). Multi-horizon is a **deep laggard**: 3d rel **−1.93%**, 1w **−0.60%**, 1m **−3.61%**. The 09-14 “under-owned + best-of-eleven FTS widen” does **not** fire: this morning’s board is the opposite of a haven bid.

Live board that *is* knowable at the open: **ES=F +1.35% / NQ=F +2.12%** (both ≥ +0.5%; NQ leads by **~77 bp** — this *does* clear the 50 bp NQ-lead threshold used as a notable *gate*, not as an automatic notable print). Finviz cash futures SPX **+0.20% / NDX +0.41%** are the same sign, smaller print — trust Channel 1, do not average. **Sector PM: XLP −0.65%** vs **XLK +0.98%**, **XLC +0.59%**, XLF 0.00%, XLRE −0.05%, XLB −0.28%, XLV −0.30%, **XLU −0.63%**, **XLE −1.29%**. That is **not a haven print**. It is bottom-of-book among non-energy (tied with XLU). Absolute red is **non-participation / funding source**, already inside the mild-down band — not the 09-17 flat-band leftover-beta case (PM ≲ +0.3%) and not the 09-18 PM ≤ 0 *flat* case (PM −0.01%).

Macro panel as it maps here: **VIX 14.98 (+0.17 1d, −2.12 1w) / VIX3M 18.24 / ratio 0.821 CONTANGO** — no vol-FTS. **CL=F −5.94% / BZ=F −5.76%**; Finviz WTI **−1.59% / Brent −1.02%** — oil still war-premium *level*, live *sign* is a hard offer (08-11 spike rule **off**). Gold mixed (Finviz **+0.90%** vs GC=F **−0.77%**) — **not** a staples floor. DXY **+0.08% 1d** / Finviz USD **−0.02%** (flat). **DGS10 4.94 / DGS30 5.29 / DFII10 2.61** (real yield **−7 bp 1d** as of 09-17, **+6 bp 1w / +20 bp 1m**) — duration still in the stress zone; **not** a fresh 10Y>5% break (09-15 smash is paid) and **not** a live easing bid this morning (10Y note **−0.03%**, 30Y **−0.06%** = bond prices marginally down). HY OAS **2.70** (tight). 5-day 10Y–SPX corr **−0.592** (not the −0.9 FTS regime). Asia **+1.04%**, Europe **+0.95%**. Fear & Greed **58.2 Greed is stale (2026-08-27) — unused**. EPU **342.15 (+163 1d)** — uncertainty up, not a staples catalyst by itself. `size_gate=True`. Ag mixed: corn/soy/wheat **+0.6 to +0.9%**, coffee **−2.36%**, sugar **−1.17%**, cocoa **−1.66%**.

## Channel 2 — required categories

**1. Shared macro → this sector.** Live tape is **session-1 of a new week, still NQ-led risk-on**: ES/NQ both ≥ +1% with NQ leading by 77 bp, Asia and Europe green, VIX contango, oil offering hard, XLK lead. News Judge #1 (Dow worst week / yields+oil) and #2/#3 (Warsh JH / hike path / IWM) are **paid as of 09-16 FOMC** — first hike under Warsh to 3.75–4.00%; October FedWatch ~57–58% another 25 bp is path, not today’s binary. News Judge RULES_APPLIED: **none** (no pending CPI/NFP/FOMC, no unresolved Chair appearance, no fresh kinetic oil increment, no overnight mega-cap AHR). News Judge #4 (crude build / DVN) is the live *sign* on crude — XLE hit, **input-cost relief** for staples, **not** a Hormuz FTS bid. #5/#6 ASML/AI-infra are XLK objects. Calendar: **no major US 8:30**; CFNAI / Goolsbee are sub-directional. X search 09-18→09-21: **checked, nothing material** for a same-morning XLP flow print.

For staples the map is **one object**:
- Risk-on / equity-beta expansion is **[−] defensives** (amp/damp). 09-11: “no FTS bid” is a **relative negative**, not S0=0. Named-headwind rule: this rotation **must** be scored, not merely narrated.
- PM XLP **−0.65%** vs XLK/XLY leaders → **zero FTS credit** (09-15). 08-18 relative-outperformance is **off**.
- Oil offering is **input-cost relief** (S1, capped) and **removes** the Hormuz FTS trigger. Do **not** score oil as a defensive bid in S0, and do **not** also score it as a second S0 negative (same-shock audit).
- NQ lead **77 bp** with NQ **+2.12%** is a stronger anti-FTS board than 09-17/09-18 (those were ~36–39 bp). It licenses a **signed down** call; it does **not** auto-upgrade magnitude to notable without XLP tape confirmation (PM is mild-down, not ≥0.75%). 08-27 down/notable was a mega-cap-AH + NQ≥+0.5% template — **no overnight mega-cap print today**, so do not fire notable off index beta.
- 09-17/09-18 bind the engine path: **do not accept v2 up/mild from ES tape_anchor + index_carry**. PM is already red in the mild band — leftover-beta up/flat is closed.

S0 carries the **risk-on rotation overlay only** → **−1**. Not −2 (no fresh mega-cap shock, oil’s live sign is S1 relief not a second macro hit, duration is carried stress not a fresh 1d break). Not 0 (naming a relative headwind without scoring it is banned).

**2. Spine (mandatory).**
- **Flight-to-safety RS vs cyclicals (primary):** **MISS live.** XLP PM **−0.65%** vs XLY ~**+0.12%** and XLK **+0.98%**. Friday 1d rel **−0.96%** already paid. YTD XLP-vs-XLY outperformance is a *long-horizon* fact, not this session.
- **Risk-on rotation away from defensives:** **HIT.** Counted in S0 as the regime object. Residual sector-factor lean only in S1 (same-shock audit — do not restack a second full HIT).
- **Pricing power held without volume collapse:** **MISS / no fresh HIT.** Circana/PLMA: H1 2026 food & beverage volume **flat**, grocery units still sliding into late August; growth is price/mix.
- **Volume decline accelerating / elasticity break:** **no fresh same-morning print.** Structural drag only, not a new 09-21 data point.

**3. Secondary (taxonomy).**
- **Input cost relief (ag, packaging, freight):** **PARTIAL HIT.** Crude offered hard (CL **−5.94%**). Ag is **not** confirming (corn/soy/wheat green). Cap at ~**+0.2** for a single-session *relative* outcome (09-11: gross-margin relief cannot outrun same-day rotation).
- **Volume stabilization sequential:** **MISS.**
- **Staples earnings beat stable margins:** **no fresh print.** KO $10B US capex is 09-15 (stale). WMT GS-conference inflation commentary is 09-18 (paid; nested HEAT still *up* for discount stores — sleeve, not parent). PEP **−3%** on 09-18 is nested, already in Friday’s close.
- **Input cost spike without pricing power:** **MISS** (oil offering).
- **Private-label share gain against brands:** **HIT structural** (record ~24% F&B value share, H1 2026) — carried, not a fresh morning print; half-weight.
- **Sector rotation into/out of defensives:** out = the S0 object; do not double-count.

**4. Breadth / leadership inside the sector.** MAP HEAT is a **split book**, not parent confirmation: Discount Stores **up** (WMT/COST), Farm Products **up** (ADM), Grocery **up** (KR) vs Brewers/Wineries **down**, Non-Alcoholic **split** (KO pos / PEP neg), Household Products **flat** (PG/CL). Nested OVERRIDE/SPLIT **beats the parent** for those sleeves and **must not be averaged into XLP**. Live parent PM **−0.65%** with mixed captains is **not** breadth expansion and is **not** “ETF up, names flat.” Large-cap quality bid in WMT/COST is a *sleeve*, not an XLP-up certificate.

**5. Flows / positioning / crowding.** ETFDB/ETF Action: XLP **5-day ~−$325M**, **1-month ~−$251M**, 1-year still deeply negative. Near-term demand is soft. This is **not** a crowded long (1m rel **−3.61%** is washed, not extended). 09-14 magnitude-widen needs a live FTS + best-of-eleven PM — **absent**. Outflows are not a forced-selling event; do not pile S3 to −2. Washout is a later setup, not a same-session bounce without a haven print.

**6. Earnings / guidance / policy.** FOMC **paid**. No 09-21 staples print. No high-impact 8:30. Policy path (hawkish Warsh, ~coin-toss October hike) is duration *context*, not a same-session binary. Index rebalance / inclusion: **checked, nothing material**.

## Self-audit

- **Lens:** near-session XLP absolute environment with an explicit **negative relative lean** vs SPY/XLK/XLY. Not an SPX call, not WMT/PEP stock-picking.
- **Band:** PM already **−0.65%** (mild). `size_gate=True`. Mag hit-rate 0.3 last 10. Oil-relief dampener + nested WMT/COST bid + no 8:30 binary → **cap notable**. Do **not** import NQ +2.12% into XLP magnitude.
- **Skew:** last-10 dir 0.5; 09-18 DO-INSTEAD (cut conviction when sign fights tape) does **not** fire — factors and live PM **agree down**. 09-17 win: keep direction, shrink confidence.
- **Same-shock:** oil counted **once** (S1 relief, not S0 haven and not a second S0 negative). Risk-on rotation counted **once** in S0. Friday smash not copied into S2/S4.
- **Single-ticker:** WMT/COST/KR/ADM nested up and PEP nested down **do not drive** the ETF call.
- **Divergence:** leading S0–S3 **−1.5** vs S4 **0** (paid tape, confirm-only). Live PM **−0.65%** agrees with factors. **No leading-vs-tape fight.** If v2 tape_anchor tries **up** off ES +1.35%, that is the 09-17/09-18 error class — **reject**; trust factors. Official lean is **down/mild**, not up, not flat.

Open experiment this scope: **no**. Food-crash full-weight override: **off** (no fresh print). 08-21 green-PM bounce after anti-FTS smash: **off** (PM is red). Maximal-FTS catch-up (09-10): **off** (contango, NQ lead, oil offered).

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -0.5
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.8
CONFIDENCE: 0.55
REGIME: risk_on
HORIZON_3D: down
HORIZON_1W: down
HORIZON_2W: mixed
HORIZON_1M: down
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|HIT|0.85|2026-09-21|channel1 ES+1.35% NQ+2.12%
Risk-off tape / flight to safety|MISS|0.80|2026-09-21|VIX/VIX3M 0.821 contango; XLP PM -0.65%
Real yields rising|PARTIAL|0.55|2026-09-17|DFII10 2.61; 1w +0.06 / 1m +0.20; 1d -0.07 paid
Real yields falling|MISS|0.60|2026-09-21|10Y note -0.03%; not a live easing bid
USD strengthening|MISS|0.50|2026-09-21|DXY +0.08% 1d / Finviz USD -0.02% — flat
USD weakening|MISS|0.50|2026-09-21|checked, nothing material
Sector breadth expansion (% names up)|MISS|0.70|2026-09-21|parent PM -0.65%; HEAT split not expansion
Sector breadth failure (ETF up, names flat)|MISS|0.70|2026-09-21|ETF is down, not up-on-narrow-leadership
Large-cap leadership inside sector|PARTIAL|0.55|2026-09-21|WMT/COST nested bid; PG/CL flat; PEP nested lag
Small/mid leadership inside sector|MISS|0.50|2026-09-21|checked, nothing material
High-beta leadership inside sector|MISS|0.55|2026-09-21|XLK/NQ lead is outside the sector
Low-beta leadership inside sector|MISS|0.70|2026-09-21|XLP/XLU both red; no defensive regime
Sector ETF inflow / relative volume spike|MISS|0.75|2026-09-18|https://etfdb.com/etf/XLP/
Sector ETF outflow / volume dry-up|HIT|0.70|2026-09-18|https://etfdb.com/etf/XLP/
Crowded long (extreme relative performance + valuation)|MISS|0.75|2026-09-18|1m rel -3.61% washed, not extended
Index rebalance / inclusion tailwind|MISS|0.50|2026-09-21|checked, nothing material
Index exclusion / forced selling|MISS|0.50|2026-09-21|checked, nothing material
Flight-to-safety relative strength vs cyclicals|MISS|0.85|2026-09-21|XLP PM -0.65% vs XLY +0.12% / XLK +0.98%
Input cost relief (ag, packaging, freight)|PARTIAL|0.70|2026-09-21|CL=F -5.94%; ag corn/soy/wheat green
Pricing power held without volume collapse|MISS|0.65|2026-09-21|https://foodindustryexecutive.com/2026/09/24-of-food-and-beverage-dollars-now-go-to-private-label-which-of-your-skus-will-survive/
Volume stabilization / sequential improvement|MISS|0.65|2026-09-21|https://www.supermarketnews.com/grocery-categories/grocery-unit-sales-slide-in-august-but-frozen-foods-and-shelf-stable-seafood-buck-trend
Staples earnings beat stable margins|MISS|0.50|2026-09-21|no fresh 09-21 print
Volume decline accelerating|PARTIAL|0.50|2026-09-21|structural grocery-unit slide; no new morning print
Elasticity break (price up, volume down hard)|MISS|0.50|2026-09-21|checked, nothing material
Input cost spike without pricing power|MISS|0.75|2026-09-21|oil offering; 08-11 spike off
Risk-on rotation away from defensives|HIT|0.85|2026-09-21|counted in S0; residual only in S1
Private-label share gain against brands|HIT|0.60|2026-09-21|https://foodindustryexecutive.com/2026/09/24-of-food-and-beverage-dollars-now-go-to-private-label-which-of-your-skus-will-survive/
Sector rotation into defensives|MISS|0.80|2026-09-21|PM worst-of-non-energy
Sector rotation out of defensives|HIT|0.85|2026-09-21|same object as risk-on rotation; not double-counted in S1
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- web_search: `XLP consumer staples ETF premarket September 21 2026`
- web_search: `consumer staples sector news PG WMT KO PEP COST September 2026`
- web_search: `CME FedWatch September 2026 rate hike odds Warsh`
- web_search: `risk on rotation defensives consumer staples lagging Nasdaq rally September 2026`
- web_search: `XLP ETF flows inflows outflows September 2026`
- web_search: `XLP vs XLY relative performance consumer staples vs discretionary September 21 2026`
- web_search: `consumer staples packaged food volume margins private label September 2026`
- web_search: `oil prices WTI Brent drop input costs consumer staples September 21 2026`
- web_search: `US stock futures Nasdaq lead S&P Monday September 21 2026`
- web_search: `economic calendar Monday September 21 2026 US releases`
- web_search: `XLY premarket September 21 2026 consumer discretionary`
- web_search: `consumer staples ETF XLP lagging Monday futures Nasdaq 2026-09-21`
- x_search: `XLP consumer staples ETF premarket lagging risk-on Nasdaq Monday September 21 2026 flows rotation` (from 2026-09-18 to 2026-09-21)
- web_fetch: `https://etfdb.com/etf/XLP/` (403)
- web_fetch: `https://www.reuters.com/business/coca-cola-invest-10-billion-us-infrastructure-by-2030-2026-09-15/` (401)
- memory_search: paused (index metadata mismatch)

**Key sources and facts taken**

- Tradesmith XLP historical — https://tradesmith.com/stockdata/XLP:NYSE/historical-data — ~2026-09-21 05:11 ET: XLP PM **$82.19, −0.74%** vs 09-18 close $82.80 (aligns with Channel 1 PM −0.65%).
- Tradesmith XLY — https://tradesmith.com/stockdata/XLY:NYSE — ~2026-09-21 ~04:00 ET: XLY PM **$111.17, +0.12%** vs 09-18 close $111.03.
- TipRanks / Reuters (via search) — WMT 09-18 GS conference: inflation eased then may re-accelerate 2H; KO ~09-15 **$10B US infrastructure 2026–2030** (https://www.reuters.com/business/coca-cola-invest-10-billion-us-infrastructure-by-2030-2026-09-15/); PEP **~−3% on 09-18** (https://247wallst.com/investing/2026/09-18/pepsico-falls-3-while-consumer-staples-hold-firm-keurig-dr-pepper-eases-coca-cola-barely-budges/).
- Axios — https://www.axios.com/2026/09/16/fed-rates-warsh-trump — 09-16 FOMC: Warsh Fed **+25 bp to 3.75–4.00%** (paid). Bitcoin.com FedWatch wrap: next meeting **~57–58% odds another 25 bp** (path, not today’s binary).
- FXStreet / Gate / InvestingLive — post-hike **risk-on rebound**, staples lag, oil offered (https://www.fxstreet.com/analysis/risk-on-rebound-oil-slides-tields-ease-and-growth-names-come-roaring-back-202609181058; https://investinglive.com/stocks/stock-sector-rotation-with-the-fed-decision-healthcare-attracts-fresh-interest-as-consumer-staples-lose-support/).
- ETFDB — https://etfdb.com/etf/XLP/ — as of ~09-18: **5d −$325M, 1m −$251M, 1y −$2.76B** net flows.
- Food Industry Executive / Circana-PLMA — https://foodindustryexecutive.com/2026/09/24-of-food-and-beverage-dollars-now-go-to-private-label-which-of-your-skus-will-survive/ — private label **~24% F&B value share**; H1 volume **flat**, national-brand units down.
- Supermarket News — https://www.supermarketnews.com/grocery-categories/grocery-unit-sales-slide-in-august-but-frozen-foods-and-shelf-stable-seafood-buck-trend — grocery units **~−2.1%** in five weeks ended 2026-08-30.
- Scotiabank calendar — https://www.scotiabank.com/ca/en/about/economics/economics-publications/post.other-publications.calendar-of-economic-release-dates.calendar-of-economic-release-dates--september-2026-.html — **no major US releases** 2026-09-21 (CFNAI / Goolsbee only).
- Bloomberg/Yahoo via search — Monday futures **Nasdaq leading S&P**; oil down; US-China talks color (index object, not XLP).
- X (baalhadid 09-18) — https://x.com/baalhadid/status/2100942750933815493 — XLP flagged laggard ~−0.6% Friday; **no material 09-21 XLP flow posts**.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -0.5, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.8, 'leading_sum': -3.5, 'divergence_flagged': False, 'total_score': -1.24, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.55, 'regime': 'risk_on', 'engine': 'v2', 'anchor': {'available': True, 'pct': -0.2763, 'score': -1.658, 'legs': [{'leg': 'ES', 'pct': 1.35, 'w': 0.45}, {'leg': 'ZN', 'pct': -0.03, 'w': 0.4}, {'leg': 'PM:XLP', 'pct': -0.65, 'w': 0.7}]}, 'overlay_score': -2.8, 'overlay_raw': -2.8, 'index_carry': 3.218, 'general_total': 12.871, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.25, 'S3_FLOWS_POSITIONING': 1.0, 'S4_ETF_TAPE': 1.25}, 'llm_confidence': 0.55, 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': 1.44, 'w1': 0.52}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
