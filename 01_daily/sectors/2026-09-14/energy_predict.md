# Sector Prediction — Energy — 2026-09-14

- ETF: **XLE**
- rubric: `00_grounding/sectors/energy.md`
- predicted_direction: **up**
- predicted_magnitude_band: **mild**
- total_score: **11.129** (mult 0.9)
- regime: sector_shock
- divergence_flagged: **False**
- engine: v2 · tape_anchor **7.88** (CL +2.44%, QA +2.80%, ES -0.66%, PM:XLE +1.50%) · index_carry **-2.751** (general -11.002) · llm_overlay **6.0** (raw 7.2)

## Channel 1 sector ETF tape

```
ETF XLE vs SPY (yfinance, through 2026-09-11):
  1d: XLE +0.32% | SPY +0.85% | rel -0.53%
  3d: XLE +0.57% | SPY -0.22% | rel +0.79%
  1w: XLE +0.80% | SPY -1.15% | rel +1.95%
  1m: XLE +6.73% | SPY -1.06% | rel +7.80%
```

MEMORY_CONFIRM: Sector Energy (XLE) — memory index unavailable this run (used injected scoreboard/lessons only). Last-10 dir=0.3 mag=0.3 (n=10); last graded 09-11 down/mild vs XLE +0.32% (dir MISS, mag HIT). Applied: **09-11 Energy lesson FIRES** (CPI-day risk-on: do not assign S0 a negative sign for a beta sector on green futures; a ~2–3% offered barrel is a dip, not a break — it sets RELATIVE sign, not absolute; if the divergence flag fires, FLATTEN the absolute call). **09-10 Energy lesson FIRES** (record-close sequence + 1m rel ≥ +8% = live crowded-long; do not score S2 and S4 both positive off the same prior-close Channel 1 series; pipeline must honor the prose magnitude cap). **09-08/09-09 magnitude-discipline lesson FIRES** (mag hit-rate ~0.3 <0.4 → cap magnitude unless oil >5% or XLE futures >2%). **08-11 live-oil verify FIRES** — Channel 1 CL=F +3.05% / BZ=F +3.43% matches Finviz WTI $102.29 (+2.44%) / Brent $107.33 (+2.80%), sign **UP**. **08-14 green-oil + live Hormuz FIRES** (oil green >2.4%, Brent >$107). **08-12 stale-run cap does NOT fire** (1w rel +1.95%, not >+4%). **09-03/09-04 exhaustion-after-extended-run does NOT fire** (oil is UP, not offered). Open sector_energy DO-INSTEAD: score sign vs tape conflict → prefer flat/mild, cut conviction.

## Energy / XLE — 2026-09-14

This is a **sector_shock oil-surge session inside a broad risk-off tape**. Crude is ripping (WTI $102.29 +2.44%, Brent $107.33 +2.80%, gasoil +2.35%, RBOB +2.73%, HO +2.67% — the whole barrel complex is bid), and XLE is the **only green sector in the premarket** (+1.50% vs XLK −1.95%, XLI −1.13%, XLY −0.65%). But ES −0.66% / NQ −1.59%, VIX 17.67 with VIX/VIX3M **1.135 backwardation**, and the 09-11 lesson just taught that a green-futures CPI day can flip a down call. Today the futures are RED and the sector's own object is UP — the two channels point the same way for XLE's absolute print. Count the oil shock **once**.

### Channel 2

**1. Shared macro as it hits energy.** Equity tape is **risk-off, not a commodity bid from beta**: ES −0.66%, NQ −1.59%, Russell −0.27%, DJIA −0.15%. Asia composite −0.72% (Kospi −3.26%, Nikkei −0.81%), Europe −0.35% (DAX −0.55%, CAC −0.58%). VIX 17.67 (+1.83 1d, +2.37 1w) with VIX/VIX3M **1.135 backwardation** — a live vol bid. **USD strengthening** (DXY +0.45% 1d, USD 99.245 +0.41%) is a mild commodity headwind. **Real yields rising** (DFII10 2.55, +0.09 1d / +0.10 1w / +0.12 1m; DGS10 4.95 +0.12 1d; DGS30 5.37) — secondary vs oil, but a multiple headwind. 5-day 10Y-SPX corr −0.248 (weak). USEPUINDXD 725.88 (+451 1d) — policy uncertainty spiking. News Judge #1: **Warsh Jackson Hole → Sept hike odds up, gold −3%** — already printed, SPX duration, not an XLE down spine when the barrel is bid. News Judge #3: **"Oil retreats; S&P/Dow break four-day losing streak"** is the *prior* session's object — today oil is UP +2.4%, so that line is stale for the energy sign. Per 08-10, keep S0 muted under sector_shock. **S0 = 0.**

**2. Spine (S1).** One cluster: live crude surge **plus** the same Hormuz premium.
- **Crude surge (live-verified):** CL=F +3.05%, BZ=F +3.43%; Finviz WTI $102.29 (+2.44%), Brent $107.33 (+2.80%). Products bid **with** crude (HO +2.67%, RBOB +2.73%, gasoil +2.35%). 08-11 passes; Channel 1 and live sign agree. This is a **strong surge**, not a sub-1% tick — and it is the first >2.4% print since 09-08.
- **Geo premium still live, not faded:** Hormuz/US–Iran copy remains the load-bearing catalyst; oil is **rising**, so this is live transmission — 08-14 FIRES: green oil + current supply-risk headlines → oil spine dominates. Do **not** also score a separate crude-surge HIT on top of geo.
- **Inventory:** last EIA (week ending 8/28) crude −4.5 Mb; **today's WPSR is the live print** (10:30 ET, unprinted at snapshot). Two-sided. Do not date the stale draw as today's HIT.
- **OPEC+ (carried offset):** Sep +188 kb/d completed the 2023 voluntary-cut rollback; Sep 6 meeting held October unchanged. Not a cut.
- **Demand destruction (carried official):** IEA 2026 −1.6 mb/d vs OPEC ~+0.6 mb/d. Offset only.
- **Cracks:** diesel/gasoil still extreme; products bid with crude — a refiner tailwind, but **refiner sleeve only**; do not let VLO/MPC drive XLE.
- **Nat gas $2.903 (+2.54%)** — a mild gas bid, N/A for oil-weighted XLE.
- **BKR** [Energy] −6.5% on Chart acquisition margin drags (Finviz digest) — single-name, do not set S1.

Net **S1 = +2**. Not +3 (same oil/Hormuz shock counted once). Not +1 (08-14 forbids capping S1 when oil is green >2.4% and the chokepoint headline is live).

**3. Breadth.** Channel 1 tape: XLE 1d rel **−0.53%** (XLE +0.32% vs SPY +0.85% on 09-11 — the sector *lagged* the prior session's risk-on rip). 3d rel +0.79%, 1w rel +1.95%, 1m rel +7.80%. **Premarket breadth is the live signal**: XLE +1.50% is the **only green sector** vs XLK −1.95%, XLI −1.13%, XLY −0.65%, XLF +0.33%, XLP +0.61%, XLRE +0.48%, XLU +0.33%. That is a genuine same-morning breadth expansion into energy on a red tape — not ETF-only carry. **S2 = +1.**

**4. Flows / positioning.** XLE still has the multi-week outflow hangover (~$4B over ~65 days). 1m rel **+7.80%** is leftover leadership, but 1w rel is only **+1.95%** — NOT the 08-21 RSI>70 / 1w rel >+5% crowded-long unwind trigger, and the 09-10 record-close sequence has now been interrupted by the 09-11 fade. Live rotation is **into** energy vs a red SPY tape. Net **S3 = 0** — do not restack S2 as a flow HIT, and do not treat trailing outflows as a 1-day lid against a live oil surge.

**5. Catalysts.** No fresh XLE-wide earnings. **EIA crude WPSR 10:30 ET** is the only same-session energy print — two-sided (last was a −4.5 Mb draw). No CPI/NFP/FOMC today (News Judge: "set is thin on hard macro data"). Warsh's hawkish repricing is carried, not fresh. Hormuz remains the load-bearing catalyst.

### Scoring logic

S0 muted: risk-off equities + firming USD/real yields are a cyclical overlay, not a veto when oil is the sector's own shock. **Critically, the 09-11 sign error is NOT repeated** — today futures are RED, so there is no green-futures tailwind to mis-sign; the risk-off tape is a *relative* tailwind for energy (the only green sector) and a mild *absolute* headwind that the oil surge dominates.

S1 is the oil/Hormuz cluster, netted once, with carried OPEC+/IEA as dampeners only.

S2 is live premarket breadth (XLE the only green sector). S4 is Channel 1 confirmation only: 1d rel **−0.53%** (prior-session lag — do NOT score this positive), 3d +0.79%, 1w +1.95%, 1m +7.80%. The 1d tape does **not** confirm; the premarket tape does. Per the 09-10 lesson, do not score S2 and S4 both positive off the same series — S4 = 0.

**Divergence:** S1 (oil surge, strongly positive) vs S4 (1d rel −0.53%, prior-session lag). The premarket tape (+1.50%, only green sector) resolves the tension in favor of the factors. **Divergence flagged = True** (leading factors up, prior-close tape flat-to-negative), but the live premarket tape confirms the factors — trust factors over the stale 1d print.

Magnitude discipline: 08-14 allows **notable** when oil is green >2.4% and Hormuz is live. But the 09-08/09-09 magnitude-discipline lesson fires (mag hit-rate ~0.3 <0.4) and the broad tape is risk-off (ES −0.66%, NQ −1.59%) — historically the risk-off tape caps energy extension near +1–1.5%. Oil is up +2.4–2.8%, not >5%, and XLE premarket is +1.50%, not >2%. **Cap at mild.** Multiplier **0.9** (keep direction, shrink the lever after mag misses). Confidence **0.55**.

Regime **sector_shock**: SPY/ES red, XLE the only green sector, oil the load-bearing driver.

**Direction: up. Magnitude: mild.**

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 2
S2_BREADTH: 1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: sector_shock
DIVERGENCE_FLAGGED: True
PREDICTED_DIRECTION: up
PREDICTED_MAGNITUDE_BAND: mild
TOTAL_SCORE: 2.7
SECTOR_SCORES_END

HIT_GRID_BEGIN
Crude oil price surge (WTI/Brent)|HIT|0.90|2026-09-14|https://www.tradingview.com/symbols/NYMEX-CL1!/
Geopolitical supply risk premium|HIT|0.75|2026-09-14|https://www.reuters.com/markets/commodities/
Sector breadth expansion (% names up)|HIT|0.70|2026-09-14|https://finviz.com/sectors.ashx
Risk-off tape / flight to safety|HIT|0.65|2026-09-14|https://finviz.com/futures.ashx
USD strengthening|HIT|0.60|2026-09-14|https://finviz.com/futures.ashx
Real yields rising|HIT|0.60|2026-09-14|https://fred.stlouisfed.org/series/DFII10
Crack spread / refining margin expansion|HIT|0.55|2026-09-14|https://finviz.com/futures.ashx
Crowded long (extreme relative performance + valuation)|MISS|0.45|2026-09-14|https://finviz.com/etf.ashx?t=XLE
Sector ETF outflow / volume dry-up|MISS|0.40|2026-09-14|https://www.etf.com/XLE
Inventory draw (EIA crude/products)|UNRESOLVED|0.50|2026-09-14|https://www.eia.gov/petroleum/supply/weekly/
OPEC+ production increase / quota break|MISS|0.35|2026-09-14|https://www.opec.org/
Demand destruction (recession/China weak)|MISS|0.35|2026-09-14|https://www.iea.org/reports/oil-market-report
HORIZON_3D: up/mild — oil surge persists into EIA print; watch whether the risk-off tape caps extension or the barrel breaks >$105 WTI.
HORIZON_1W: up/mild — Hormuz premium still live; 1m rel +7.80% is crowded but 1w rel +1.95% is not extended; a >5% oil break would justify notable.
HORIZON_2W: flat/mild — if the geopolitical premium fades (reopen terms) the crowded 1m run unwinds; the 09-10 record-close fade is the template.
HORIZON_1M: flat — 1m rel +7.80% is leftover leadership; without a fresh kinetic step-change the sector mean-reverts toward SPY.
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 2.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': 8.0, 'divergence_flagged': False, 'total_score': 11.129, 'predicted_direction': 'up', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.85, 'regime': 'sector_shock', 'engine': 'v2', 'anchor': {'available': True, 'pct': 1.3134, 'score': 7.88, 'legs': [{'leg': 'CL', 'pct': 2.44, 'w': 0.35}, {'leg': 'QA', 'pct': 2.8, 'w': 0.15}, {'leg': 'ES', 'pct': -0.66, 'w': 0.6}, {'leg': 'PM:XLE', 'pct': 1.5, 'w': 0.7}]}, 'overlay_score': 6.0, 'overlay_raw': 7.2, 'index_carry': -2.751, 'general_total': -11.002, 'skill_multipliers': {'S0_SHARED_MACRO': 1.25, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.55}
```
