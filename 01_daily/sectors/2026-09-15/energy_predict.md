# Sector Prediction — Energy — 2026-09-15

- ETF: **XLE**
- rubric: `00_grounding/sectors/energy.md`
- predicted_direction: **up**
- predicted_magnitude_band: **mild**
- total_score: **7.35** (mult 0.85)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **3.029** (CL +2.37%, QA +2.31%, ES +0.30%, PM:XLE +0.14%) · index_carry **-1.097** (general -4.389) · llm_overlay **5.419** (raw 5.419)

## Channel 1 sector ETF tape

```
ETF XLE vs SPY (yfinance, through 2026-09-15):
  1d: XLE +1.52% | SPY -0.49% | rel +2.00%
  3d: XLE +0.89% | SPY -0.09% | rel +0.98%
  1w: XLE +1.14% | SPY -1.15% | rel +2.29%
  1m: XLE +5.81% | SPY -2.47% | rel +8.28%
```

MEMORY_CONFIRM: Sector Energy (XLE) — memory index unavailable this run (used injected scoreboard/lessons only). Last-10 dir=0.3 mag=0.3 (n=10); last graded 09-14 up/mild vs XLE −0.936% (dir MISS, mag MISS). Applied: **09-14 Energy lesson FIRES** (VIX/VIX3M backwardation = de-risking regime → do NOT mute S0 for a high-beta sector; premarket breadth in a single ETF is not breadth → S2=0 unless constituent-confirmed; a late >2.4% oil spike is fuel already spent → discount S1 for fade; honor the divergence flag; 1m rel ≥ +7.5% + outflow hangover = crowded; do not triple-count the ETF's own premarket print across anchor + S2 + S4). **09-10 Energy lesson FIRES** (record-close sequence + 1m rel ≥ +8% = live crowded-long; do not score S2 and S4 both positive off the same prior-close Channel 1 series; pipeline must honor the prose magnitude cap). **09-08/09-09 magnitude-discipline lesson FIRES** (mag hit-rate ~0.3 <0.4 → cap magnitude unless oil >5% or XLE futures >2%). **09-11 Energy lesson FIRES** (do not assign S0 a negative sign for a beta sector on green futures; a ~2–3% offered barrel is a dip, not a break — it sets RELATIVE sign, not absolute; if the divergence flag fires, FLATTEN the absolute call). **08-11 live-oil verify FIRES** — Channel 1 CL=F +3.24% 1d / BZ=F −2.34% 1d is internally split and is the prior-close column; live premarket Finviz WTI $103.79 (+2.37%) / Brent $108.11 (+2.31%) is the session sign (UP), independently confirmed by Reuters/NDTV Profit (Brent ~$107, WTI ~$103, Saudi East-West pipeline offline after fresh attacks). **08-14 green-oil + live Hormuz FIRES** (oil green >2.3%, Brent >$108, fresh physical supply increment). **08-12 stale-run cap does NOT fire** (1w rel +2.29%, not >+4%). **09-03/09-04 exhaustion-after-extended-run does NOT fire** (oil is UP, not offered). Open sector_energy DO-INSTEAD: score sign vs tape conflict → prefer flat/mild, cut conviction.

## Energy / XLE — 2026-09-15

This is the **second consecutive instance of the exact setup that burned 09-14**: a sector_shock oil surge inside a broad risk-off tape, with VIX/VIX3M in backwardation. Crude is bid (WTI $103.79 +2.37%, Brent $108.11 +2.31%, gasoil +2.68%, HO +3.09%, RBOB +1.92% — the whole barrel complex is green), and XLE is one of only two green sector ETFs in the premarket (+0.14% vs XLI +0.81%, XLB +0.38%, XLU +0.20%, XLK +0.11%, XLV +0.04%, XLF −0.08%, XLY −0.13%, XLRE −0.24%, XLP −0.33%, XLC −0.63%). But ES −0.54%, NQ −0.62%, Russell −0.73%, DJIA −0.71%, VIX 17.75 with VIX/VIX3M **0.901** (contango, NOT backwardation), USD +0.25%, real yields +0.05 1d / +0.18 1w. The 09-14 lesson is the load-bearing correction: do not let the ETF's own premarket gap masquerade as breadth, and do not triple-count the ETF's own print across anchor + S2 + S4.

### Channel 2

**1. Shared macro as it hits energy.** Equity tape is **risk-off, not a commodity bid from beta**: ES −0.54%, NQ −0.62%, Russell −0.73%, DJIA −0.71%. Asia composite −1.1% (Kospi −3.26%, Hang Seng −1.0%, Nikkei −0.81%), Europe −0.39% (DAX −0.21%, CAC −0.43%, EuroStoxx −0.5%). **VIX 17.75 (+0.65 1d, +2.03 1w) with VIX/VIX3M 0.901 — contango, NOT backwardation.** This is the key differentiator from 09-14 (which had 1.135 backwardation): the vol term structure is *not* signaling acute de-risking, so the 09-14 "full-weight negative S0" clause fires at reduced weight, not full. **USD strengthening** (DXY +0.16% 1d, USD 99.36 +0.25%) is a mild commodity headwind. **Real yields rising** (DFII10 2.60, +0.05 1d / +0.18 1w / +0.18 1m; DGS10 4.96 +0.19 1w; DGS30 5.35) — secondary vs oil but a multiple headwind. 5-day 10Y-SPX corr −0.172 (weak, so duration is not the dominant transmission today). News Judge #1 (10Y breaches 5%, global bond selloff), #2 (FOMC/SEP/Warsh — unresolved binary, do not pre-score), #3 (Aug CPI core 0.3% locks Sept hike) are the day's dominant SPX-beta drivers and all are **negative for broad beta, neutral-to-negative for energy's multiple**. News Judge #6 (US-Iran tanker war, Hormuz impaired, Brent ~$107) is the sector's own object and is **live**. Per 08-10 keep S0 muted *under a pure sector_shock*, but the 09-14 lesson overrides that mute when red tape + firm USD + rising real yields are themselves a driver. **S0 = −0.5** (not −1: contango, not backwardation, so the acute de-risking tell is absent).

**2. Spine (S1).** One cluster: live crude surge **plus** the same Hormuz premium — but with a **fresh physical supply increment**.
- **Crude surge (live-verified):** Finviz WTI $103.79 (+2.37%), Brent $108.11 (+2.31%); products bid with crude (HO +3.09%, gasoil +2.68%, RBOB +1.92%). Channel 1 CL=F +3.24% 1d agrees on sign; BZ=F −2.34% 1d is the prior-close column and is **superseded** by the live +2.31% print (08-11: do not use the stale column as the session sign). This is a **strong surge**, the third >2% print in five sessions.
- **Geo premium live, with a fresh physical increment:** live search confirms **"Oil prices climb as attacks, pipeline outage deepen Saudi supply fears"** (Reuters, 15 Sep) and **"Brent crude near $107 as Saudi pipeline shutdown keeps supply risks in focus"** (NDTV Profit, 15 Sep) — the kingdom's **East-West Pipeline is offline** after fresh attacks. A pipeline outage is a *new* physical supply increment on top of the Hormuz/US–Iran narrative — this is not the same shock re-scored. 08-14 FIRES: green oil + current supply-risk headlines → oil spine dominates. Do **not** also score a separate crude-surge HIT on top of geo.
- **Inventory:** last EIA (week ending 9/4) — next crude WPSR ~Sep 17. Stale/mixed; not a live draw or a live collapse. Do not date it as today's HIT.
- **OPEC+ (carried offset):** Sep +188 kb/d completed the 2023 voluntary-cut rollback; Sep 6 meeting held October unchanged. Not a cut.
- **Demand destruction (carried official):** IEA 2026 −1.6 mb/d vs OPEC ~+0.6 mb/d. Offset only.
- **Cracks:** diesel/gasoil still extreme; products bid with crude — a refiner tailwind, and MAP HEAT's **Refining & Marketing OVERRIDE (dir=up, conv=high, +5.86% w1, +4.26 vs parent, VLO at 52-wk highs)** confirms it. But **refiner sleeve only**; do not let VLO/MPC drive XLE.
- **Nat gas $2.893 (−0.07%)** — no surge; N/A for oil-weighted XLE.
- **MAP HEAT nested:** E&P dir=up (COP +2.83% w1, breadth 0.449 improving); Integrated dir=up (XOM/CVX both green, only sub-sector beating parent); Midstream dir=up (M&A wave at $100 oil); Drilling dir=flat (NE/RIG neg); Equipment & Services dir=down (SLB −4.13% w1, breadth 0.26); Thermal Coal dir=down; Uranium dir=down (stale tag, both RUT captains red). Net nested read is **positive but split** — the up-tagged sub-sectors (E&P, Integrated, Refining, Midstream) are the XLE-heavy ones, so the parent gets a genuine constituent-confirmed bid.

Net **S1 = +2**. Not +3 (same oil/Hormuz shock counted once, though the pipeline outage is a fresh increment). Not +1 (08-14 forbids capping S1 when oil is green >2.3% and the chokepoint headline is live).

**3. Breadth.** Channel 1 tape: XLE 1d rel **+2.00%** (XLE +1.52% vs SPY −0.49% on 09-14 — the sector *led* the prior session's red tape). 3d rel +0.98%, 1w rel +2.29%, 1m rel +8.28%. **Premarket breadth is the live signal**: XLE +0.14% is one of only two green sector ETFs, but per the 09-14 lesson a single ETF's own premarket print is **not** breadth — it is the ETF's own gap relabeled. The **constituent-confirmed** breadth evidence is MAP HEAT: Integrated 0.75 breadth (XOM/CVX green), E&P 0.449 improving, Refining OVERRIDE with VLO/PBF/DK confirming. That is genuine intra-sector participation, not ETF-only carry. **S2 = +1** (constituent-confirmed, not the ETF's own print).

**4. Flows / positioning.** XLE still has the multi-week outflow hangover (~$4B over ~65 days). 1m rel **+8.28%** is leftover leadership and, per the 09-10 lesson, 1m rel ≥ +8% **is** the crowded-long condition on its own — do not gate it on 1w rel >+5%. But 1w rel is only +2.29% and the 09-14 record-close sequence has already been broken (XLE −0.94% on 09-14), so the crowding is partially de-risked. Net **S3 = −0.5** (residual crowded-long unwind risk, dampened because the prior session already de-risked).

**5. Catalysts.** **FOMC/SEP/Warsh today** is the dominant unresolved binary — two-sided, not the energy spine; do not pre-score hawkish. **EIA crude WPSR ~Sep 17** (not today). No fresh XLE-wide earnings. The Saudi East-West pipeline outage is the load-bearing same-day catalyst.

### Scoring logic

S0 = −0.5: red tape + firm USD + rising real yields are a genuine headwind for a high-beta cyclical, but VIX/VIX3M is in **contango (0.901)**, not backwardation — the acute de-risking tell from 09-14 is absent, so the 09-14 full-weight clause fires at reduced weight.

S1 = +2: the live oil/Hormuz cluster with a fresh physical increment (Saudi pipeline offline), netted once, with carried OPEC+/IEA as dampeners only.

S2 = +1: **constituent-confirmed** via MAP HEAT (Integrated 0.75 breadth, E&P improving, Refining OVERRIDE) — not the ETF's own premarket print (09-14 lesson).

S3 = −0.5: residual crowded-long unwind risk (1m rel +8.28%), dampened because 09-14 already de-risked.

S4 = 0: Channel 1 1d rel +2.00% is the **prior** session's tape (09-14), already in the price. Per the 09-14 lesson, do not re-score the ETF's own print as fresh confirmation. The live premarket XLE +0.14% is the anchor's object, not a second vote.

**Divergence check:** leading factors (S0 −0.5, S1 +2, S2 +1, S3 −0.5) net **+2.0**; tape confirmation (S4 = 0, prior-session rel +2.00% but already priced) is neutral. Factors lean up, tape is neutral-to-mildly-confirming. **No material divergence** — but the 09-14 lesson's warning stands: a gap-up on a red tape with a late oil spike is fade-prone. The contango (vs 09-14's backwardation) is the reason I do not flatten the absolute call.

**Magnitude discipline:** 09-08/09-09 lesson — mag hit-rate ~0.3 (<0.4) + risk-off tape → cap at **mild** unless oil >5% or XLE futures >2%. Oil is +2.37% (not >5%), XLE premarket +0.14% (not >2%). **Cap at mild.** Multiplier **0.85** (keep direction, shrink the lever after consecutive mag misses). Confidence **0.52** (cut conviction per sector_energy DO-INSTEAD: score sign vs tape conflict → prefer flat/mild).

Regime **mixed**: sector_shock oil surge (S1 +2) fighting a risk-off broad tape (S0 −0.5) with a live FOMC binary.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -0.5
S1_SECTOR_FACTORS: 2
S2_BREADTH: 1
S3_FLOWS_POSITIONING: -0.5
S4_ETF_TAPE: 0
MULTIPLIER: 0.85
CONFIDENCE: 0.52
REGIME: mixed
SECTOR_SCORES_END

HIT_GRID_BEGIN
Crude oil price surge (WTI/Brent)|HIT|0.85|2026-09-15|https://www.reuters.com/business/energy/
Geopolitical supply risk premium|HIT|0.80|2026-09-15|https://www.ndtvprofit.com/markets/
Sector breadth expansion (% names up)|HIT|0.60|2026-09-15|MAP HEAT: Integrated 0.75 breadth, E&P 0.449 improving
Crack spread / refining margin expansion|HIT|0.70|2026-09-15|MAP HEAT OVERRIDE: Refining +5.86% w1, VLO 52-wk highs
Risk-off tape / flight to safety|HIT|0.65|2026-09-15|ES -0.54%, NQ -0.62%, Russell -0.73%
Real yields rising|HIT|0.60|2026-09-15|DFII10 2.60 +0.05 1d / +0.18 1w
USD strengthening|HIT|0.55|2026-09-15|DXY +0.16% 1d, USD 99.36 +0.25%
Crowded long (extreme relative performance + valuation)|PARTIAL|0.50|2026-09-15|1m rel +8.28%, outflow hangover
Sector ETF outflow / volume dry-up|PARTIAL|0.45|2026-09-15|~$4B over ~65 days
Inventory draw (EIA crude/products)|MISS|0.30|2026-09-15|Last EIA wk ending 9/4; next ~9/17
OPEC+ cut / supply discipline|MISS|0.25|2026-09-15|Sep +188 kb/d rollback complete; Oct held
Demand destruction (recession/China weak)|MISS|0.30|2026-09-15|IEA 2026 -1.6 mb/d carried offset
Natural gas price surge|MISS|0.20|2026-09-15|NG $2.893 -0.07%
Crude price collapse|MISS|0.15|2026-09-15|WTI +2.37% live
Inventory build|MISS|0.20|2026-09-15|No live build print
OPEC+ production increase / quota break|MISS|0.20|2026-09-15|No fresh quota break
Crack spread collapse|MISS|0.15|2026-09-15|HO +3.09%, gasoil +2.68%
Sector rotation out of energy|MISS|0.35|2026-09-15|XLE one of two green sector ETFs premarket
HIT_GRID_END

HORIZON_3D: up/mild — the Saudi pipeline outage is a fresh physical supply increment that should hold the barrel bid for 2–3 sessions; XLE's constituent-confirmed breadth (Integrated/E&P/Refining) supports a modest grind higher, but the FOMC binary and 1m rel +8.28% crowding cap extension.
HORIZON_1W: flat-to-up/mild — the oil spine is intact but the crowded-long condition (1m rel +8.28%, outflow hangover) plus a hawkish rates regime (10Y >5%, Sept hike locked) is a multiple headwind; expect the sector to outperform SPY on relative but struggle for large absolute gains.
HORIZON_2W: flat — mean-reversion risk rises as the war-premium narrative ages; absent a fresh kinetic increment, the barrel tends to fade half its spike (as on 09-14), and XLE's 1m relative leadership is already extended.
HORIZON_1M: flat-to-down/mild — the 09-10/09-14 pattern (record-close sequence + multi-week run + outflow hangover) historically resolves in a crowded-long unwind once the marginal oil increment stalls; a hawkish FOMC/SEP path is an additional headwind for the sector's multiple.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -0.5, 'S1_SECTOR_FACTORS': 2.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': -0.5, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.85, 'leading_sum': 7.0, 'divergence_flagged': False, 'total_score': 7.35, 'predicted_direction': 'up', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.65, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.5048, 'score': 3.029, 'legs': [{'leg': 'CL', 'pct': 2.37, 'w': 0.35}, {'leg': 'QA', 'pct': 2.31, 'w': 0.15}, {'leg': 'ES', 'pct': 0.3, 'w': 0.6}, {'leg': 'PM:XLE', 'pct': 0.14, 'w': 0.7}]}, 'overlay_score': 5.419, 'overlay_raw': 5.419, 'index_carry': -1.097, 'general_total': -4.389, 'skill_multipliers': {'S0_SHARED_MACRO': 1.25, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.52, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
