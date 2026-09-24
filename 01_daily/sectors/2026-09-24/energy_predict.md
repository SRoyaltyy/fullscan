# Sector Prediction — Energy — 2026-09-24

- ETF: **XLE**
- rubric: `00_grounding/sectors/energy.md`
- predicted_direction: **up**
- predicted_magnitude_band: **mild**
- total_score: **6.779** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **2.694** (CL -1.59%, QA -1.02%, ES -0.64%, PM:XLE +1.11%) · index_carry **-1.915** (general -7.659) · llm_overlay **6.0** (raw 7.538)

## Channel 1 sector ETF tape

```
ETF XLE vs SPY (yfinance, through 2026-09-23):
  1d: XLE +0.96% | SPY -0.72% | rel +1.68%
  3d: XLE -2.44% | SPY +0.80% | rel -3.24%
  1w: XLE -2.01% | SPY +2.08% | rel -4.09%
  1m: XLE -0.59% | SPY +0.82% | rel -1.40%
```

MEMORY_CONFIRM: Sector Energy (XLE) — memory index unavailable this run (used injected scoreboard/lessons only). Last-10 dir=0.6 mag=0.4 (n=10); last-30 dir=0.538 mag=0.346 (n=26); last graded 09-23 down/mild vs XLE +0.955% (dir MISS, mag HIT). Applied: **09-23 Energy lesson FIRES** (commodity down + index flat-to-soft + ETF green premarket + sector leading the premarket board → rotation bid; do NOT sign S1 from the commodity alone; a green premarket in a defensive/real-asset sector on a flat-to-soft index is a SIGN signal, not a magnitude cap; "trust factors over tape" must be conditional — when the tape divergence is in the direction of a coherent rotation setup, trust the tape; if the narrative says "flag it," the pipeline must emit divergence_flagged True). **09-21 Energy lesson FIRES** (sector ETF the clear laggard on a green tape → S0 negative, green tape is the funding mechanism for rotation OUT; do not use "residual geo/cracks floor" as a cap when the card itself calls the premium fading; when PM ≤ −1% AND barrel offered AND tape risk-on, lift mild→notable). **09-18 band refinement FIRES toward mild** (|PM| < 1%, oil increment sub-1.5%, 1m not crowded). **09-17 leftover-S4 gate FIRES** (do not reuse prior-close 1d rel as live S4; PM is the live extension test). **09-03 emit-signed-down FIRES** (live barrel sign sets direction when PM confirms). **08-11 live-oil verify FIRES** — Channel 1 Finviz WTI $104.16 (−1.59%) / Brent $107.67 (−1.02%) is the **stale 09-16 column**; CL=F +1.86% / BZ=F +2.4% 1d is the live increment and is **UP**; independent live (Convex/TradingEconomics 09-23) WTI **$92.71 +3.18%**, Brent surging 09-24 (TradingKey). Sign **UP**. **08-14 green-oil FIRES** (oil green >1.8%, live supply-risk headlines). **09-15 physical-increment notable license: PARTIAL** (no confirmed ≥2%-of-global-supply outage; US–Iran kinetic headlines are live but the increment is a rebound, not a new outage). **09-14 backwardation debit does NOT fire** (VIX/VIX3M **0.908 contango**). **09-11 pending-binary flatten does NOT fire** (no CPI/NFP/FOMC today; but ES=F **−0.64%** / NQ=F **−1.09%** are RED, so its protective clause is moot). **09-10 crowded-long does NOT fire** (1m rel **−1.40%** ≪ +8%; the multi-week run has fully unwound). **09-04 S1+S4 aligned-negative does NOT fire** (1d rel **+1.68%**, not ≤ −1.5%). Open sector_energy DO-INSTEAD: keep direction, shrink confidence on modest |score| when magnitude historically misses.

---

# Energy / XLE — 2026-09-24

This is the **first genuine regime flip in seven sessions**. For six straight sessions (09-16 → 09-23) the book was "offered barrel + geo-premium fade → down/mild," and it went 5-for-6 on direction. This morning **every load-bearing input has inverted**: the barrel is **UP** (CL=F +1.86%, BZ=F +2.4%, WTI $92.71 +3.18% on 09-23), XLE is the **top premarket sector at +1.11%** while XLK is **−1.51%** and NQ futures are **−1.09%**, and the driver is a **fresh US–Iran kinetic escalation** (Saudi/Houthi strikes, Trump "hopefully nearing end of Iran war"). The trap today is the mirror of yesterday's: **do not carry the six-session down/mild prior into a session where the sector's own object is green and the sector is the board's leader.** But the second trap is 09-15's: **do not let a green barrel license notable when the broad tape is risk-off and the increment is a rebound, not a confirmed physical outage.**

## Channel 2

**1. Shared macro as it hits energy.** The tape is **risk-off, and energy is the rotation destination**: ES=F **−0.64%**, NQ=F **−1.09%**; Finviz ES/NQ/RTY/DJIA **+0.20% / +0.41% / +0.08% / +0.11%** are the **stale prior-close column**, not the live impulse. Asia composite **−0.09%** (Nikkei +0.76%, Kospi +1.04%, Shanghai −1.22%, ASX −0.72%), Europe **−0.40%** (DAX −0.52%, CAC −0.56%). **VIX 16.44 (+1.26 1d) with VIX/VIX3M 0.908 — contango**, so the 09-14 full-weight de-risking debit is **off**; but VIX up 1.26 on the day is a live risk-off impulse. **USD is a non-event** (DXY +0.13% 1d, Finviz USD −0.02%) — no commodity headwind. **Real yields rising** (DFII10 2.63, +0.01 1d / +0.23 1m; DGS10 4.96, +0.22 1m; DGS30 5.29) — secondary vs oil, but a multiple headwind. 5-day 10Y-SPX corr **−0.826** — duration is the dominant SPX transmission today. News Judge #1–#2 (**Warsh signals hikes may be needed; yields spike; Wall Street ends lower**) is the **dominant macro cluster** and it is **bearish SPX beta, not bearish energy** — in fact the Finviz digest headline reads *"Tech Giants Falter as Treasury Yields Surge to 2007 Levels, Fueling Energy Sector Rally."* That is the 09-21 lesson's rotation mechanism, **inverted in energy's favor**: the green/red tape is the funding source, and today the funding flows **INTO** energy, not out. **S0 = +0.5** (mild positive: risk-off rotation into the real-asset sleeve, no USD headwind, no backwardation debit; not +1 because rising real yields and a −1.09% NQ tape are a genuine multiple headwind that caps extension).

**2. Spine (S1).** One cluster: live crude rebound **plus** the same US–Iran/Hormuz supply-risk premium. Count it **once**.
- **Crude: UP, live-verified.** CL=F **+1.86%** 1d, BZ=F **+2.4%** 1d; independent live (Convex/TradingEconomics, 09-23) WTI **$92.71, +3.18% d/d, +9.06% 1m, +42.65% y/y**; TradingKey (09-24 08:20 GMT) **"Brent (UKOIL) Surges on Sep 24."** Channel 1 Finviz **$104.16 / $107.67 is the stale 09-16 column** and is **rejected** as the live level (08-11). Sign **UP**, increment **~1.9–2.4%** — a real surge, not a sub-1% tick, and the first green barrel print since 09-15.
- **Geo premium: live and transmitting again.** News Judge: **US–Iran tensions, Saudi/Houthi strikes**; CNBC (09-16) "Trump says U.S. 'hopefully' nearing end of Iran war amid Saudi, Houthi strikes"; Gulf News (09-24) **"Asia's oil imports rebound — but still 13% below pre-war levels."** The 09-21/09-22/09-23 fade narrative (Hormuz six-month-high flows, UNGA diplomacy, East-West workaround) has **stalled** — the Express Tribune (09-24 07:28 GMT) still runs *"Oil prices fall as Iran says it is open to diplomacy,"* so the diplomacy branch is **live and two-sided**, but the **price is rising**, so the premium is transmitting. **08-14 FIRES**: green oil + current supply-risk headlines → oil spine dominates. Do **not** score a separate Crude-surge HIT on top of geo.
- **Inventory: two-sided, unprinted.** API week ending 9/18 was crude **+1.786 Mb** / Cushing **+2.082 Mb** (a build lean, already in 09-23's tape); WSJ survey expected EIA crude **−0.5 Mb**. **Today's WPSR 10:30 ET is unprinted.** Pluang (09-24 04:56 GMT) frames the tape as *"Oil prices dip after sharp rebound amid US-Iran tensions and rising US crude stocks"* — i.e. the build is the **counterweight** to the rebound, not today's HIT. Do **not** date it as an Inventory-build HIT; do **not** ignore it.
- **OPEC+ (carried offset):** Sep **+188 kb/d** completed the 2023 voluntary-cut rollback; 6 Sep meeting **held October unchanged**; next core-group meeting **Oct 4**. Not a cut, not a quota break.
- **Demand destruction (carried):** IEA 2026 bearish vs OPEC constructive; hawkish Warsh is a medium-horizon demand lid. Offset only.
- **Cracks:** HO **+0.18%**, RBOB **−0.54%**, gasoil **−0.26%** — products are **NOT** bid with crude. This is the important nuance: the crude move is a **supply-risk premium**, not a demand/crack squeeze. **Refiner sleeve only**; do not let VLO/MPC set the ETF, and do not score Crack-spread expansion.
- **Nat gas $2.902 (−0.51%)** — no surge; N/A for oil-weighted XLE.
- **Coal Newcastle −1.53%, Uranium 0.00%** — no thermal/nuclear sympathy.

Net **S1 = +2**. Not +3: the same oil/Hormuz shock counted once, the increment is a **rebound** (~2%) not a confirmed ≥2%-of-global-supply physical outage (09-15's license is only PARTIAL), products are **not** confirming, and the diplomacy branch is live and two-sided. Not +1: 08-14 forbids capping S1 when oil is green >1.8% with live supply-risk headlines, and the sector is the board's leader.

**3. Breadth.** Channel 1 tape: XLE 1d rel **+1.68%** (XLE +0.96% vs SPY −0.72% on 09-23 — the sector **outperformed** on a red tape). 3d rel **−3.24%**, 1w rel **−4.09%**, 1m rel **−1.40%** — the multi-week relative bleed is intact but the **1d has flipped green**. **Premarket breadth is the live signal**: XLE **+1.11%** is the **top sector** vs XLK **−1.51%**, XLC −0.71%, XLV −0.40%, XLF −0.07%, XLY −0.05%, XLI −0.00%, XLU +0.10%, XLP +0.41%. That is a genuine same-morning breadth expansion into energy on a red tape — **but** per the 09-14 lesson, a single ETF's premarket print is **not** breadth unless constituent-confirmed. I have **no** constituent-level confirmation (no XOM/CVX/COP premarket prints in Channel 1). So: **S2 = +0.5** (partial credit — the sector is the board leader on a red tape, which is a real rotation tell, but I cannot verify intra-sector participation and I must not relabel the ETF's own gap as breadth).

**4. Flows / positioning.** XLE carries the multi-week outflow hangover (~$4B over ~65 days). 1m rel **−1.40%** — the crowded-long condition is **fully unwound** (09-10 does not fire). The 09-23 lesson's rotation-bid branch is **live again today** and now has a **fundamental** driver (green barrel) rather than only a tape tell. **S3 = +0.5** (washout + rotation-in setup; not +1 because the outflow hangover is a real near-term demand drag and I have no flow data confirming an inflow).

**5. Catalysts.** **EIA WPSR 10:30 ET** is the only same-session hard energy print — two-sided (API build lean vs WSJ −0.5 Mb expectation). **US–Iran kinetic headlines** are the live two-sided catalyst (escalation = up, diplomacy = fade). **Warsh hawkish / yields spike** is the dominant macro binary and is **energy-supportive via rotation** but **beta-negative**. No XLE-wide earnings. **Trump–Xi summit** (Finviz) is a cross-asset overhang, not an energy spine.

### Scoring logic

S0 is **mildly positive, not zero**: the 09-21 lesson says a green tape is a funding mechanism for rotation **out of** the laggard — today energy is the **destination**, not the laggard, so the sign flips. But I keep it at +0.5, not +1, because NQ −1.09% and rising real yields are a genuine multiple headwind and 09-11's "green futures = tailwind" clause is **moot** (futures are red).

S1 is the live green barrel counted **once** with the geo premium. The 08-11 live-oil verify is the load-bearing check today: Channel 1's Finviz $104.16 is a **stale 09-16 column** and would have produced a **wrong-sign** S1 if trusted. The live sign is **UP**.

S2 gets **partial** credit only — the sector is the board leader, but I have no constituent confirmation and I will not double-count the ETF's own premarket print (09-14).

S3 is a **mild positive** — the crowded-long condition has fully unwound and the rotation bid is live, but the outflow hangover caps it.

S4 is **confirmation only**: 1d rel **+1.68%** is a genuine prior-session relative outperformance on a red tape, which **confirms** the rotation read. Per the 09-17 leftover-S4 gate, I do **not** reuse it as the main thesis — the live PM (+1.11%) is the same-session extension test, and it **passes**. **S4 = +1.**

**Divergence check.** Leading factor sum (S0 +0.5, S1 +2, S2 +0.5, S3 +0.5) = **+3.5**, and the tape confirmation (S4 +1, PM +1.11%) **agrees in sign**. **No divergence.** This is the cleanest alignment in the recent sample — and it is the **opposite** of yesterday's setup, where the card's own inputs leaned bullish-rotation and it netted S1 negative. I am explicitly **not** repeating that error.

**Magnitude discipline.** Mag hit-rate is **0.346–0.40** (<0.4), which per 09-08/09-09 caps magnitude **unless** oil >5% **or** XLE futures >2% **or** a confirmed physical supply increment ≥~2% of global supply is live. Today: oil **+1.86%/+2.4%** (not >5%), XLE PM **+1.11%** (not >2%), and the physical-increment license is only **PARTIAL** (kinetic headlines, no confirmed outage). **None of the three escalators fire.** The 09-18 band refinement also points to **mild** (|PM| ~1.1%, oil increment sub-2.5%, 1m not crowded). **Band = mild.** I explicitly **reject** notable: the 09-15 lesson's notable license required a **confirmed physical supply increment**, and today's move is a **rebound** off a six-session fade with **products not confirming** (HO +0.18%, RBOB −0.54%) and a **live diplomacy branch** (Iran open to talks). A rebound-plus-premium inside a risk-off tape with NQ −1.09% is a **mild** setup, not a trend day.

**Direction = up.** S1 is a live, verified green barrel; S4 confirms; PM is the board leader; the crowded-long condition has unwound. The 09-03 emit-signed-down rule has a symmetric counterpart here: when the live barrel sign is **up** and PM **confirms**, emit the signed direction rather than flat.

**Confidence = 0.52.** Direction signals are aligned (S1 + S4 + PM all green), which argues higher; but (a) the magnitude hit-rate is poor, (b) the EIA WPSR at 10:30 is a genuine two-sided binary that can flip the barrel, (c) the diplomacy branch is live and two-sided, and (d) the broad tape is risk-off with NQ −1.09%, which historically caps energy extension near +1–1.5%. The open sector_energy experiment says **keep direction, shrink confidence** — I keep direction and set confidence modest.

**Multiplier = 0.9.** Slightly below 1.0: the S1 cluster is a single shock counted once, the physical-increment license is only partial, and the tape is risk-off. Not 0.85 (that was the six-session fade multiplier); not 1.0 (the increment is a rebound, not a confirmed outage).

**Regime = mixed.** Risk-off broad tape (ES −0.64%, NQ −1.09%, VIX +1.26) with a **sector-specific supply-risk shock** that is green. Per the 08-10/09-15 guidance, this is a **sector_shock inside a risk-off tape** — but because the tape is risk-off and the sector is the rotation destination, "mixed" is the honest label rather than a clean risk_on.

### What would falsify this call

- **XLE closes down** despite a green barrel + board-leading PM → the rotation read was wrong and the risk-off tape dominated (would validate the 09-11 protective clause outside its precondition).
- **XLE closes >2%** → the mild cap was too tight and the 09-15 physical-increment license should have been granted on kinetic headlines alone.
- **EIA WPSR prints a large build at 10:30** and the barrel reverses → the S1 +2 was a morning-only artifact.

### HIT_GRID

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0.5
S1_SECTOR_FACTORS: 2
S2_BREADTH: 0.5
S3_FLOWS_POSITIONING: 0.5
S4_ETF_TAPE: 1
MULTIPLIER: 0.9
CONFIDENCE: 0.52
REGIME: mixed
DIVERGENCE_FLAGGED: False
SECTOR_SCORES_END

HIT_GRID_BEGIN
Crude oil price surge (WTI/Brent)|HIT|0.85|2026-09-24|https://convextrade.com/today/oil-price
Geopolitical supply risk premium|HIT|0.70|2026-09-24|https://www.cnbc.com/quotes/@CL.1
Sector rotation into energy|HIT|0.65|2026-09-24|https://news.google.com/rss/articles/CBMivAFBVV95cUxPTnh3bzRWY21EdFh2T3VaS0x6UW93bE5Md0NySC11S1dFM0RNUm1DVWYtdmVGNmwyak9aMXhLLVF1aVRDNG5QZFhEOGMxSWJCLXZYc25nT1NReVJNNFJObVNqY21GRjdUUVBfTlc5engtQ1ZlZjI1bld6UkFBd3Zqa0h5QlhSaDVRY2piNE1YS1B1NTl4R0ZaVnE1SkdWWTIxcGRDa29NSW1qOGd0ZUlNQ3VmaDZqNEpiZWQ4UQ
Sector ETF inflow / relative volume spike|PARTIAL|0.40|2026-09-24|
Sector breadth expansion (% names up)|PARTIAL|0.45|2026-09-24|
Risk-off tape / flight to safety|HIT|0.70|2026-09-24|
Real yields rising|HIT|0.60|2026-09-24|
USD strengthening|MISS|0.55|2026-09-24|
Crowded long (extreme relative performance + valuation)|MISS|0.80|2026-09-24|
Inventory build|PARTIAL|0.40|2026-09-24|
Crack spread / refining margin expansion|MISS|0.65|2026-09-24|
Natural gas price surge|MISS|0.80|2026-09-24|
OPEC+ cut / supply discipline|MISS|0.75|2026-09-24|
Demand destruction (recession/China weak)|PARTIAL|0.35|2026-09-24|
HORIZON_3D|up|0.50|2026-09-24|
HORIZON_1W|up|0.45|2026-09-24|
HORIZON_2W|flat|0.40|2026-09-24|
HORIZON_1M|flat|0.40|2026-09-24|
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.5, 'S1_SECTOR_FACTORS': 2.0, 'S2_BREADTH': 0.5, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'multiplier': 0.9, 'leading_sum': 8.0, 'divergence_flagged': False, 'total_score': 6.779, 'predicted_direction': 'up', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.771, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.4489, 'score': 2.694, 'legs': [{'leg': 'CL', 'pct': -1.59, 'w': 0.35}, {'leg': 'QA', 'pct': -1.02, 'w': 0.15}, {'leg': 'ES', 'pct': -0.64, 'w': 0.6}, {'leg': 'PM:XLE', 'pct': 1.11, 'w': 0.7}]}, 'overlay_score': 6.0, 'overlay_raw': 7.538, 'index_carry': -1.915, 'general_total': -7.659, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.52}
```
