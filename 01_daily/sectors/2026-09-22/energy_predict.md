# Sector Prediction — Energy — 2026-09-22

- ETF: **XLE**
- rubric: `00_grounding/sectors/energy.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-5.917** (mult 0.85)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **-3.243** (CL -1.59%, QA -1.02%, ES -0.07%, PM:XLE -0.45%) · index_carry **-0.124** (general -0.497) · llm_overlay **-2.55** (raw -2.55)

## Channel 1 sector ETF tape

```
ETF XLE vs SPY (yfinance, through 2026-09-21):
  1d: XLE -2.30% | SPY +1.55% | rel -3.85%
  3d: XLE -1.87% | SPY +2.83% | rel -4.71%
  1w: XLE -2.63% | SPY +1.91% | rel -4.54%
  1m: XLE -1.44% | SPY +1.68% | rel -3.12%
```

MEMORY_CONFIRM: Sector Energy (XLE) — memory index unavailable this run (used injected scoreboard/lessons only). Last-10 dir=0.7 mag=0.4 (n=10); last-30 dir=0.542 mag=0.333 (n=24); last graded 09-21 down/mild vs XLE −2.91% / rel −4.46% (dir HIT, mag MISS). Open sector_energy experiment **does** apply: keep direction, shrink confidence after mag misses. Applied: **08-11 live-oil verify FIRES** — Channel 1 Finviz WTI $104.16 (−1.59%) / Brent $107.67 (−1.02%) is a leftover 09-16 column; CL=F −4.78% is the **expiring Oct contract** (rolls today) and is **rejected** as the live increment. Independent live (Oilprice.com ~10:00 UTC): WTI **$90.72 (−1.79%)**, Brent **$99.12 (−1.22%)**; HO **−1.53%**, RBOB **−1.03%**. Sign **DOWN**, increment **sub-2%**, not a collapse. Overnight short-covering bounce (Oilprice 00:30 CDT WTI $93.38 / Brent $101.69) **faded by the open**. **08-14 green-oil does NOT fire.** **09-15 physical-increment notable license does NOT fire** (oil offered; Hormuz throughput recovering). **09-14 backwardation debit does NOT fire** (VIX/VIX3M **0.823 contango**). **09-11 pending-binary flatten does NOT fire** (no CPI/NFP/FOMC; live ES=F/NQ=F **−0.07%**, not ≥ +0.5%). **09-10 crowded-long does NOT fire** (1m rel **−3.12%** ≪ +8%; already de-risked). **09-17 leftover-S4 gate FIRES**: do **not** reuse 09-21 1d rel **−3.85%** as live S4; PM:XLE **−0.45%** is **not** smash extension (not ≤ −1%). **09-03 emit-signed-down FIRES**: live offered barrel + S4=0 because prior 1d is leftover, not because PM is bid. **09-18 band refinement FIRES toward flat/mild** (|PM| < 1%, oil ~1–2%, 1m not crowded, refiners nested cushion). **09-21 S0-laggard notable lift does NOT fire** (PM not ≤ −1%; live index not green ≥ +1%). **09-08/09-09 mag-discipline + size_gate=True FIRES**. News Judge #4 inventory/DVN is **09-16 leftover** — do not restack. EIA WPSR **Wed Sep 23** unprinted.

# Energy / XLE — 2026-09-22

This is a **fifth-session oil-premium fade**, not a fresh sector_shock and not a collapse sequel to Monday. Yesterday’s object (Hormuz recovery + diplomacy hopes, XLE **−2.30% / rel −3.85%**) **already printed**. This morning the **active** barrel is still **offered ~1.2–1.8%**, XLE is only **mildly red in premarket (−0.45%)**, and live ES/NQ are **flat (−0.07%)** — not Monday’s risk-on rotation tape. Count the oil-down + geo-fade cluster **once**. Do **not** treat Channel 1 CL=F −4.78% or Finviz $104 as today’s smash. Do **not** rerun 09-21’s notable-down hindsight. Do **not** flip to up on leftover Nasdaq/AI beta energy is not in.

## Channel 2

**1. Shared macro as it hits energy.** Live tape is **flat, not a commodity bid and not a 09-21 risk-on funding squeeze**: ES=F **−0.07%**, NQ=F **−0.07%**. Finviz ES/NQ/RTY/DJIA leftover greens (+0.20% / +0.41% / +0.08% / +0.11%) are **not** the live impulse. Asia composite **+0.41%**, Europe **+0.07%**. **VIX 14.88** with VIX/VIX3M **0.823 — contango**, so the 09-14 full-weight de-risking debit is **off**. USD is a non-event (DXY **+0.06%** 1d; Finviz USD −0.02%). DFII10 **2.68, +0.07 1d** is a Friday leftover, **secondary vs oil**. News Judge #1–#3 (Nasdaq/AI leftover, gold vs yields, ASML EUV) are **SPX/QQQ growth-beta**, not an XLE spine. Cheaper oil is SPX-positive inflation optics; that can leak beta **only if PM is participating**. It is not: **XLE −0.45% vs XLP +0.32% / XLC +0.21%**, but **XLI −0.75%** is worse — energy is **a** laggard, not **the** clear laggard on a ≥+1% green tape. 09-21’s “score S0 negative when the sector is the clear laggard on a green tape” **does not meet its precondition**. 09-11’s “don’t score S0 negative on green futures” is **moot** (futures are flat). **S0 = 0.**

**2. Spine (S1).** One cluster: live offered barrel **plus** geo-premium fade. Do not triple-count oil + Hormuz recovery + leftover inventory.
- **Crude: offered ~1.2–1.8%, not a surge, not a collapse.** Live-verified (08-11): Oilprice.com WTI **$90.72 (−1.79%)**, Brent **$99.12 (−1.22%)**; products **offered with** crude (HO **−1.53%**, RBOB **−1.03%**, gasoil leftover −0.26%). Channel 1 Finviz $104.16 / CL=F −4.78% is **rejected** (stale column + Oct expiry/roll). Overnight bounce to WTI $93.38 / Brent $101.69 (Oilprice, 00:30 CDT; Reuters via KCM “short-covering”) **did not hold**. Still ~$91 / ~$99 — a dip, not 08-25’s smash and not 09-15’s +2.3% surge.
- **Geo premium still present, not transmitting — fading, no fresh kinetic increment.** Hormuz shipments **six-month high** (Adm. Cooper, 19 Sep) is **already in Monday’s tape**. UNGA/Iran-talks headline is the **same fade narrative**, not a new outage. East-West remains damaged; Saudi Hormuz exports recovering. Oil is **falling**, so this is fade/non-confirmation. **Do not** score Crude oil price surge. **Do not** score a fresh Geopolitical supply risk premium HIT. 09-15’s physical-increment license is **off**. News Judge: **no fresh kinetic oil increment**.
- **Inventory: leftover, not today’s HIT.** News Judge #4 / DVN −5.6% / API **+7.1 Mb** is the **09-16** print. Last EIA (week ended 9/11, released 9/16): crude **−0.64 Mb** vs ~−1.4 to −1.6 Mb expected to **423.4 Mb**. **Tonight’s API** (week ended 9/18) and **Wed Sep 23 WPSR 10:30 ET** are **unprinted, two-sided**. Do **not** date either as today’s HIT.
- **OPEC+ (carried offset):** Sep **+188 kb/d** completed the 2023 voluntary-cut rollback; **6 Sep** meeting **held October unchanged**. Next core-group meeting **Oct 4**. Not a cut, not a quota break.
- **Demand destruction (carried official):** Sinopec research arm still sees 2026 China demand down (~0.6 mb/d). Medium-horizon lid, not a 1d HIT.
- **Cracks:** 3-2-1 still extreme (~$69–73 as of 9/19–9/22). **Refiner sleeve only.** Today HO/RBOB are **offered with crude** — not a clean squeeze that can drive whole XLE. MAP HEAT Refining dir=up (MPC/VLO) is **nested** — dampen; do not let VLO/MPC set the ETF.
- **Nat gas $2.834 (−0.07%)** — no surge; N/A for oil-weighted XLE.
- **MAP HEAT nested:** OFS/Drilling/Coal/Uranium **OVERRIDE down**; E&P/Integrated residual **up** on leftover w1 (refining-margin squeeze, not crude strength). Nested overrides **do not average into XLE**.

Net **S1 = −1**. Live oil sign is down once. Not −2: increment is sub-2%, not a collapse, PM is not extending Monday’s smash, and 09-21’s “lift S1 to −2” required PM:XLE ≤ −1% plus a live ~2%+ offered barrel **transmitting**. Not 0: WTI −1.79% is a real offered barrel and 08-14’s green-oil license is off. Not +1: the overnight bounce faded; do not score a dead cat as a surge.

**3. Breadth.** Channel 1 1d rel **−3.85%** is **yesterday’s** smash — 09-17 forbids copying it into S2. Live PM:XLE **−0.45%** with XOM ~−0.45%; not constituent-confirmed expansion and not a smash extension. MAP HEAT is a **split** (refiners/integrated leftover green vs OFS/drilling override down). Do **not** copy 3d/1w/1m lag (−4.71% / −4.54% / −3.12%) into S2. **S2 = 0.**

**4. Flows / positioning.** 1m rel **−3.12%** — **not** crowded; 09-10 is **off**. Short-term XLE outflows (~$157M week of 9/17; ~$0.9B 1m) are a hangover **already in** the 09-16→09-21 de-risk, not a fresh 1-day lid. 09-21 rotation-out **already printed**. Do not restack as a same-session S3 debit, and do not flip S3 positive for a washout bounce while the barrel is still offered. **S3 = 0.**

**5. Catalysts.** No fresh XLE-wide earnings. **API tonight / EIA Wed 10:30 ET** are the only near-session hard energy prints — **two-sided, unprinted**. OPEC+ next **Oct 4**. UNGA/Iran talks are a **two-sided fade binary**, not a confirmed deal. G7 Houthi statement is color, not a new supply shock.

### Scoring logic

S0 muted: live futures are flat; 09-21’s rotation-out debit needs a green ≥+1% tape and a clear laggard PM ≤ −1%. Neither holds.

S1 is the live offered barrel counted once with the same geo-fade. Incremental print is sub-2%. 09-21’s “don’t use fading geo as a floor to keep S1 at −1” was tied to PM ≤ −1% transmission; that gate is **off**.

S2/S4 both **0**: do not double-count Monday’s −3.85% rel as breadth **and** tape. Combined breadth+tape contribution = 0 (09-10/09-17).

S3 = 0: de-risked, not crowded.

**09-03** keeps **direction down** (live oil offered, S4=0 because prior 1d is leftover, PM is not bid). **09-18** caps the **band**: oil only ~1–2%, |PM| < 1%, refiners nested cushion, 1m not crowded → prefer **flat**, mild only if oil extends or PM ≤ −1%. **09-08/09-09 + size_gate** forbid notable. Open experiment: **keep direction, shrink confidence** (mag 0.40 last-10; 09-16/09-18/09-21 all mag misses).

**Divergence:** leading sum is mildly negative (S1=−1, rest 0); S4=0 is leftover, not a counter-tape. **No leading-vs-tape fight.** Trust factors. Do not flatten direction to up on leftover AI beta.

**Knowable-at-open test:** bullish items (overnight WTI bounce, leftover cracks, nested refiners) are either **faded** or **sleeve-only**. Forward items (API/EIA, UNGA talks) are **two-sided**. Base case is **continuation of the oil-fade at a smaller increment**, not a bounce day and not a smash day.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: -1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.85
CONFIDENCE: 0.40
REGIME: mixed
HORIZON_3D: down
HORIZON_1W: down
HORIZON_2W: mixed
HORIZON_1M: mixed
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|MISS|0.55|2026-09-22|n/a
Risk-off tape / flight to safety|MISS|0.60|2026-09-22|n/a
Real yields rising|PARTIAL|0.45|2026-09-18|n/a
Real yields falling|MISS|0.55|2026-09-22|n/a
USD strengthening|MISS|0.50|2026-09-22|n/a
USD weakening|MISS|0.50|2026-09-22|n/a
Sector breadth expansion (% names up)|MISS|0.60|2026-09-22|n/a
Sector breadth failure (ETF up, names flat)|MISS|0.65|2026-09-22|n/a
Large-cap leadership inside sector|PARTIAL|0.45|2026-09-22|n/a
Small/mid leadership inside sector|MISS|0.50|2026-09-22|n/a
High-beta leadership inside sector|MISS|0.55|2026-09-22|n/a
Low-beta leadership inside sector|MISS|0.50|2026-09-22|n/a
Sector ETF inflow / relative volume spike|MISS|0.55|2026-09-22|https://etfdb.com/etf/XLE/
Sector ETF outflow / volume dry-up|PARTIAL|0.50|2026-09-17|https://www.nasdaq.com/articles/xle-mpc-psx-vlo-etf-outflow-alert-0
Crowded long (extreme relative performance + valuation)|MISS|0.70|2026-09-22|n/a
Index rebalance / inclusion tailwind|MISS|0.70|2026-09-22|n/a
Index exclusion / forced selling|MISS|0.70|2026-09-22|n/a
Crude oil price surge (WTI/Brent)|MISS|0.80|2026-09-22|https://oilprice.com/oil-price-charts/
Natural gas price surge|MISS|0.75|2026-09-22|https://oilprice.com/oil-price-charts/
Inventory draw (EIA crude/products)|MISS|0.70|2026-09-16|https://www.eia.gov/petroleum/supply/weekly/
OPEC+ cut / supply discipline|MISS|0.70|2026-09-06|https://www.reuters.com/business/energy/opec-set-keep-oil-output-policy-unchanged-sunday-sources-say-2026-09-06/
Crack spread / refining margin expansion|PARTIAL|0.55|2026-09-22|https://rbnenergy.com/market-data/3-2-1-crack-spread
Geopolitical supply risk premium|MISS|0.65|2026-09-22|https://worldoil.com/news/2026/9/19/strait-of-hormuz-oil-shipments-hit-six-month-high-u-s-commander-says/
Crude price collapse|MISS|0.70|2026-09-22|https://oilprice.com/oil-price-charts/
OPEC+ production increase / quota break|MISS|0.65|2026-09-06|https://www.opec.org/pr-detail/1835613-6-september-2026.html
Demand destruction (recession/China weak)|PARTIAL|0.45|2026-09-09|https://www.reuters.com/business/energy/china-oil-demand-fall-89-2026-sinopec-research-says-2026-09-09/
Inventory build|MISS|0.60|2026-09-22|n/a
Crack spread collapse|MISS|0.60|2026-09-22|https://oilprice.com/oil-price-charts/
Sector rotation into energy|MISS|0.70|2026-09-22|n/a
Sector rotation out of energy|PARTIAL|0.55|2026-09-21|n/a
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- WTI crude oil price today September 22 2026
- Brent crude oil price live September 22 2026
- XLE energy ETF premarket September 22 2026
- EIA crude inventory build oil market September 2026
- oil prices September 22 2026 WTI Brent Hormuz Iran
- XOM CVX COP stock premarket September 22 2026
- OPEC+ October production meeting oil 2026
- XLE ETF flows energy sector rotation September 2026
- WTI crude oil futures price now site:marketwatch.com OR site:cnbc.com OR site:reuters.com
- crude oil prices fall OR rise Tuesday September 22 2026
- crack spread diesel gasoline refining margins September 22 2026
- Hormuz oil shipments six-month high Iran talks UNGA September 22 2026
- XLE stock quote September 22 2026
- natural gas price Henry Hub September 22 2026
- China oil demand Sinopec September 2026
- XLE -0.45% premarket energy stocks Tuesday September 22
- API crude inventory estimate week ending September 18 2026
- X search: WTI crude oil price today September 22 2026 live levels and energy ETF XLE premarket

**Key sources (title + URL + timestamp where available) and facts taken**

1. **Oilprice.com oil price charts** — https://oilprice.com/oil-price-charts/ — fetched 2026-09-22 ~09:59–10:00 UTC. **Facts:** WTI **90.72 (−1.65 / −1.79%)**; Brent **99.12 (−1.22 / −1.22%)**; nat gas **2.834 (−0.07%)**; gasoline **3.434 (−1.03%)**; heating oil **4.815 (−1.53%)**. Used as the **authoritative live oil sign** (DOWN, sub-2%).
2. **Oilprice.com homepage** — https://oilprice.com/ — fetched 2026-09-22 ~10:00 UTC. **Facts:** same live ticks; headline “Oil Prices Reverse Course as Traders Watch US-Iran Diplomacy” (4h) vs later live offered print — bounce faded.
3. **Irina Slav, Oilprice.com, “Oil Prices Reverse Course…”** — https://oilprice.com/Latest-Energy-News/World-News/Oil-Prices-Reverse-Course-as-Traders-Watch-US-Iran-Diplomacy.html — Sep 22, 2026, 12:30 AM CDT. **Facts:** overnight bounce Brent **$101.69**, WTI **$93.38**; KCM/Reuters: “typical short-covering bounce… not a fundamental shift”; ING: speculative Brent longs up 16,904 w/e Sep 15 to 282,657 (highest since May); G7 called on Iran to stop arming Houthis.
4. **Economic Times** — https://economictimes.indiatimes.com/markets/commodities/news/oil-price-today-september-22-crude-oil-snaps-4-day-fall-hovers-near-101-on-potential-us-iran-talk-whats-next/articleshow/134400423.cms — Sep 22. **Facts:** narrative of snapping a 4-day fall / Brent near $101 on US-Iran talk hopes. Treated as the **overnight bounce headline**, overridden by later live Oilprice ticks.
5. **World Oil / CENTCOM Hormuz** — https://worldoil.com/news/2026/9/19/strait-of-hormuz-oil-shipments-hit-six-month-high-u-s-commander-says/ — Sep 19, 2026. **Facts:** Adm. Cooper: Hormuz oil/LNG shipments hit a **six-month high**; already in Monday’s tape.
6. **Reuters vessels/Hormuz** — https://www.reuters.com/world/middle-east/vessels-trickle-through-strait-hormuz-mideast-tension-persists-2026-09-21/ — Sep 21. **Facts:** traffic still well below pre-war; Saudi flows via Hormuz rising vs earlier lows.
7. **Reuters OPEC+** — https://www.reuters.com/business/energy/opec-set-keep-oil-output-policy-unchanged-sunday-sources-say-2026-09-06/ plus OPEC PR https://www.opec.org/pr-detail/1835613-6-september-2026.html — Sep 6. **Facts:** core OPEC+ **held October unchanged**; next meeting **Oct 4**; Sep +188 kb/d completed 2023 voluntary-cut rollback.
8. **EIA / inventory trackers** — https://www.eia.gov/petroleum/supply/weekly/ ; https://thevaultreport.com/oil ; Trading Economics crude stocks. **Facts:** week ended Sep 11 (released Sep 16) commercial crude **−0.64 Mb to 423.4 Mb** (smaller than ~−1.6 Mb expected); gasoline/distillates built; **next WPSR Sep 23**. API week ended Sep 11 was **+7.1 Mb** (09-16 leftover). API week ended Sep 18 **unprinted** at this snapshot (Tue evening print).
9. **RBN / OilPriceAPI cracks** — https://rbnenergy.com/market-data/3-2-1-crack-spread ; https://www.oilpriceapi.com/crack-spread . **Facts:** 3-2-1 still elevated (~$69.28 on Sep 19; ~$73 WTI-based on some Sep 22 snapshots). Nested refiner tailwind only; today’s HO/RBOB are offered with crude.
10. **ETFDB / Nasdaq outflow alert** — https://etfdb.com/etf/XLE/ ; https://www.nasdaq.com/articles/xle-mpc-psx-vlo-etf-outflow-alert-0 . **Facts:** ~$157M outflow week ending ~Sep 17; ~$0.9B 1m outflows; 1y flows still positive. Not a fresh same-session flow shock.
11. **Reuters/Bloomberg Sinopec** — https://www.reuters.com/business/energy/china-oil-demand-fall-89-2026-sinopec-research-says-2026-09-09/ — Sep 9. **Facts:** China 2026 oil demand seen down ~0.6 mb/d — carried demand lid, not today’s HIT.
12. **Channel 1 injected tape (trusted numbers, not re-derived):** XLE vs SPY through 2026-09-21: 1d **−2.30% / rel −3.85%**; 3d rel **−4.71%**; 1w rel **−4.54%**; 1m rel **−3.12%**. PM board: **XLE −0.45%**; ES=F/NQ=F **−0.07%**; VIX **14.88**, VIX/VIX3M **0.823**.
13. **X search (xAI, 2026-09-21 to 2026-09-22):** trader scans of WTI in the low-to-mid $90s and XLE indicated lower — corroborates offered crude, not used as a primary quote.

**Conflicts resolved:** Channel 1 Finviz WTI $104.16 and CL=F −4.78% **conflict** with live Oilprice WTI $90.72 / Brent $99.12. Per 08-11, **live independent quotes win**. Oct WTI expiry today explains the −4.8% contract print; do not score it as a new collapse. Overnight “snaps 4-day fall” headlines describe a **faded bounce**, not the 05:58 ET / 09:58 UTC decision tape.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.85, 'leading_sum': -3.0, 'divergence_flagged': False, 'total_score': -5.917, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.65, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': -0.5405, 'score': -3.243, 'legs': [{'leg': 'CL', 'pct': -1.59, 'w': 0.35}, {'leg': 'QA', 'pct': -1.02, 'w': 0.15}, {'leg': 'ES', 'pct': -0.07, 'w': 0.6}, {'leg': 'PM:XLE', 'pct': -0.45, 'w': 0.7}]}, 'overlay_score': -2.55, 'overlay_raw': -2.55, 'index_carry': -0.124, 'general_total': -0.497, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.4, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
