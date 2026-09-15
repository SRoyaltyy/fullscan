# Sector Prediction — Energy — 2026-09-15

- ETF: **XLE**
- rubric: `00_grounding/sectors/energy.md`
- predicted_direction: **up**
- predicted_magnitude_band: **mild**
- total_score: **5.832** (mult 0.85)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **3.072** (CL +2.37%, QA +2.31%, ES +0.34%, PM:XLE +0.14%) · index_carry **-0.959** (general -3.836) · llm_overlay **3.719** (raw 3.719)

## Channel 1 sector ETF tape

```
ETF XLE vs SPY (yfinance, through 2026-09-15):
  1d: XLE +1.96% | SPY -0.56% | rel +2.52%
  3d: XLE +1.33% | SPY -0.16% | rel +1.49%
  1w: XLE +1.58% | SPY -1.22% | rel +2.80%
  1m: XLE +6.28% | SPY -2.54% | rel +8.81%
```

I have enough to score. Let me finalize.

MEMORY_CONFIRM: Sector Energy (XLE) — memory index unavailable this run (used injected scoreboard/lessons only). Last-10 dir=0.3 mag=0.3 (n=10); last graded 09-14 up/mild vs XLE −0.936% (dir MISS, mag MISS). Applied: **09-14 Energy lesson FIRES** (VIX/VIX3M backwardation = de-risking regime → do NOT mute S0 for a high-beta sector; premarket breadth in a single ETF is not breadth → S2=0 unless constituent-confirmed; a late >2.4% oil spike is fuel already spent → discount S1 for fade; honor the divergence flag; 1m rel ≥ +7.5% + outflow hangover = crowded; do not triple-count the ETF's own premarket print across anchor + S2 + S4). **09-10 Energy lesson FIRES** (record-close sequence + 1m rel ≥ +8% = live crowded-long; do not score S2 and S4 both positive off the same prior-close Channel 1 series; pipeline must honor the prose magnitude cap). **09-08/09-09 magnitude-discipline lesson FIRES** (mag hit-rate ~0.3 <0.4 → cap magnitude unless oil >5% or XLE futures >2%). **09-11 Energy lesson FIRES** (do not assign S0 a negative sign for a beta sector on green futures; a ~2–3% offered barrel is a dip, not a break — it sets RELATIVE sign, not absolute; if the divergence flag fires, FLATTEN the absolute call). **08-11 live-oil verify FIRES** — Channel 1 CL=F +4.27% 1d / BZ=F +2.70% 1d agrees on sign with live Finviz WTI $103.79 (+2.37%) / Brent $108.11 (+2.31%), independently confirmed by Reuters ("Saudi pipeline outage threatens loss of 4% of global oil supply"), EnergyNow (WTI $103.53 +2.11%), NDTV Profit (Brent ~$107), Daily Sabah ("Oil surges as attacks, pipeline outage worsen Saudi supply woes"). **08-14 green-oil + live Hormuz FIRES** (oil green >2.3%, Brent >$108, fresh physical supply increment — East-West pipeline offline). **08-12 stale-run cap does NOT fire** (1w rel +2.80%, not >+4%). **09-03/09-04 exhaustion-after-extended-run does NOT fire** (oil is UP, not offered). Open sector_energy DO-INSTEAD: score sign vs tape conflict → prefer flat/mild, cut conviction.

---

# Energy / XLE — 2026-09-15

This is the **fourth consecutive instance of the same setup**: a sector_shock oil surge inside a broad risk-off tape. The microstructure differs from 09-14 in the one way that matters — **VIX/VIX3M is 0.897 (contango), not backwardation** — so the acute de-risking tell that justified full-weight negative S0 on 09-14 is absent. But the 09-14 lesson's *other* clauses (ETF's own premarket gap ≠ breadth; don't triple-count the ETF print; late-spike fade risk) all still bind, and the 09-11 lesson's protective clause (green futures → don't score S0 negative) does **not** apply because futures are RED (ES −0.54%, NQ −0.62%, Russell −0.73%, DJIA −0.71%).

## Channel 2

**1. Shared macro as it hits energy.** Equity tape is **risk-off, not a commodity bid from beta**: ES −0.54%, NQ −0.62%, Russell −0.73%, DJIA −0.71%. Asia composite −0.72% (Kospi −3.26%, Nikkei −0.81%, Hang Seng +0.45%), Europe −0.31% (DAX −0.15%, CAC −0.34%, EuroStoxx −0.38%). **VIX 17.49 (+0.39 1d, +1.77 1w) with VIX/VIX3M 0.897 — contango, NOT backwardation.** This is the key differentiator from 09-14 (1.135 backwardation): the vol term structure is *not* signaling acute de-risking, so the 09-14 "full-weight negative S0" clause fires at **reduced** weight. **USD strengthening** (DXY +0.17% 1d, USD 99.36 +0.25%) is a mild commodity headwind. **Real yields rising** (DFII10 2.60, +0.05 1d / +0.18 1w / +0.18 1m; DGS10 4.96 +0.19 1w; DGS30 5.35) — secondary vs oil but a multiple headwind. 5-day 10Y-SPX corr −0.151 (weak, so duration is not the dominant transmission today). News Judge #1 (10Y breaches 5%, global bond selloff), #2 (chipmaker weakness, AMD −5%), #3 (Aug CPI core locks Sept hike; FOMC/SEP/Warsh unresolved binary — do not pre-score) are the day's dominant SPX-beta drivers and are **negative for broad beta, neutral-to-negative for energy's multiple**. News Judge #4 (US–Iran tanker war, Hormuz impaired, Brent ~$107–109) is the sector's own object and is **live**. Per 08-10 keep S0 muted *under a pure sector_shock*, but the 09-14 lesson overrides that mute when red tape + firm USD + rising real yields are themselves a driver. **S0 = −0.5** (not −1: contango, not backwardation, so the acute de-risking tell is absent).

**2. Spine (S1).** One cluster: live crude surge **plus** the same Hormuz premium — but with a **fresh physical supply increment**.
- **Crude surge (live-verified):** Finviz WTI $103.79 (+2.37%), Brent $108.11 (+2.31%); products bid with crude (HO +3.09%, gasoil +2.68%, RBOB +1.92%). Channel 1 CL=F +4.27% 1d agrees on sign; BZ=F +2.70% 1d agrees. This is a **strong surge**, the fourth >2% print in six sessions.
- **Geo premium live, with a fresh physical increment:** live search confirms **"Saudi pipeline outage threatens loss of 4% of global oil supply"** (Reuters, 14 Sep), **"Satellite images show extent of damage to major Saudi pipeline"** (Guardian, 14 Sep), **"Oil rises above $108 as attacks, pipeline outage deepen Saudi supply concerns"** (15 Sep), **"East-West pipeline shutdown narrows Saudi oil export routes amid Hormuz constraints"** (Anadolu, 15 Sep). This is a **step-change beyond the 09-08→09-14 Hormuz narrative** — a 4%-of-global-supply physical outage with satellite-confirmed damage. 08-14 FIRES strongly: green oil + current supply-risk headlines → oil spine dominates. Do **not** also score a separate crude-surge HIT on top of geo.
- **Inventory:** EIA week ending 9/4 (released ~9/10) showed a **crude draw** (Rigzone, 11 Sep: "USA Crude Oil Stocks Drop Week on Week"; API showed crude ease + gasoline draw). Next crude WPSR is **today 10:30 ET — unprinted, two-sided**. Do not date the stale draw as today's HIT.
- **OPEC+ (carried offset):** Sep 6 meeting **held October quotas unchanged** (Reuters, 6 Sep) — leaves the Hormuz/pipeline tightness **un-offset**. Not a cut, but not a bearish increment either.
- **Demand destruction (carried official):** IEA 2026 −1.6 mb/d vs OPEC ~+0.6 mb/d. Offset only.
- **Cracks:** 3-2-1 at **$56.82** (15 Sep close) with distillate crack **$107.02** — refining margins still extreme; products bid with crude. **Refiner sleeve only** — MAP HEAT OVERRIDE Refining & Marketing dir=up conv=high (MPC/VLO pos, +5.86% w1, +4.26 vs parent). Do not let VLO/MPC drive the whole ETF.
- **Nat gas $2.893 (−0.07%)** — no surge; N/A for oil-weighted XLE.
- **MAP HEAT:** Integrated dir=up conv=medium (XOM/CVX both pos, 0.75 breadth — the only sub-sector beating parent); E&P dir=up conv=medium (COP +2.83% w1); Services dir=down (SLB −4.13% w1); Drilling dir=flat; Coal dir=down; Uranium dir=down (do not average into XLE).

Net **S1 = +2**. Not +3 (same oil/Hormuz/pipeline shock counted once). Not +1 (08-14 forbids capping S1 when oil is green >2.3% and the chokepoint headline is live — and this is a fresh physical increment, not a stale continuation).

**3. Breadth.** Channel 1 tape: XLE 1d rel **+2.52%** (XLE +1.96% vs SPY −0.56% on 09-14 — the sector *led* the prior session's risk-off tape). 3d rel +1.49%, 1w rel +2.80%, 1m rel +8.81%. **Premarket breadth is the live signal**: XLE +0.14% is one of only two green sector ETFs (XLI +0.81%, XLB +0.38%, XLU +0.20%, XLK +0.11%, XLV +0.04%, XLF −0.08%, XLY −0.13%, XLRE −0.24%, XLP −0.33%, XLC −0.63%). **But per the 09-14 lesson, a single ETF's own premarket print is NOT breadth** — it is the ETF's own gap relabeled, and the anchor already carries PM:XLE at weight 0.7. MAP HEAT gives genuine constituent confirmation (XOM/CVX green, COP +2.83% w1, refining OVERRIDE with VLO at 52-wk highs), which is *some* independent evidence — but it is the same oil shock, not a separate breadth expansion. **S2 = 0** (not +1: no independent constituent-level breadth expansion beyond the oil factor already in S1; do not double-count the ETF's own print).

**4. Flows / positioning.** XLE still has the multi-week outflow hangover (~$4B over ~65 days). 1m rel **+8.81%** plus the recent record-close sequence **IS the crowded-long condition** — do not gate it on 1w rel >+5% (09-10 lesson). With the barrel up a fourth consecutive session and the ETF's own premarket print only +0.14% (i.e., **not** confirming the +2.37% crude move), this is a fade-risk setup. **S3 = −0.5** (crowded-long unwind risk).

**5. Catalysts.** **EIA crude WPSR today 10:30 ET** is the only same-session energy print — two-sided. **FOMC/SEP/Warsh** is the dominant unresolved binary (do not pre-score). No fresh XLE-wide earnings. Pipeline repair timeline is the load-bearing catalyst.

### Scoring logic

S0 = −0.5: red tape + firm USD + rising real yields are a genuine headwind for a high-beta sector, but contango (not backwardation) means the acute de-risking tell is absent — so reduced, not full, weight.

S1 = +2: the oil/Hormuz/pipeline cluster, netted once, with a **fresh physical supply increment** (4% of global supply offline, satellite-confirmed). Carried OPEC+/IEA as dampeners only.

S2 = 0: the ETF's own premarket print is not breadth (09-14 lesson); MAP HEAT constituent confirmation is the same oil shock already in S1.

S3 = −0.5: crowded 1m rel +8.81% + outflow hangover + ETF not confirming the crude move.

S4 = 0: Channel 1 1d rel +2.52% is the **prior session's** tape (already in the price); the current session's premarket XLE +0.14% is the live read and it is **not** confirming a fresh up day. Per the 09-14 lesson, do not score the prior-close series as live forward confirmation.

**Divergence check:** leading factor sum (S0 −0.5, S1 +2, S2 0, S3 −0.5 = **+1.0**) vs tape confirmation (S4 = 0, premarket XLE +0.14% vs crude +2.37%). **Divergence IS present** — factors lean up, tape is flat. Per the 09-11 lesson, a fired divergence flag **FLATTENS the absolute call** toward flat/mild rather than keeping the up sign at reduced conviction. This is the single most important correction from 09-14, where the flag was identified in prose but the pipeline emitted up/mild anyway and XLE gapped-and-faded −0.94%.

**Magnitude discipline:** 09-08/09-09 lesson — mag hit-rate ~0.3 (<0.4) + risk-off tape → cap at mild unless oil >5% or XLE futures >2%. Oil is +2.37% (not >5%), XLE premarket +0.14% (not >2%). **Cap at mild.** Do not emit notable.

**Direction:** The oil spine is genuinely strong and fresh (4% of global supply), which argues up. But the tape is not confirming (XLE +0.14% vs crude +2.37%), the sector is crowded (1m rel +8.81%), the broad tape is red, and the divergence flag fires. Net: **up/mild** with low confidence — the direction leans up on the fresh physical supply increment, but the flat tape + divergence flag means this is a coin-flip between up/mild and flat/mild. Multiplier **0.85** (cut conviction after four consecutive Energy misses). Confidence **0.42**.

Regime **mixed** (sector_shock oil-up inside a risk-off broad tape, with contango rather than backwardation).

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -0.5
S1_SECTOR_FACTORS: 2
S2_BREADTH: 0
S3_FLOWS_POSITIONING: -0.5
S4_ETF_TAPE: 0
MULTIPLIER: 0.85
CONFIDENCE: 0.42
REGIME: mixed
DIVERGENCE_FLAGGED: True
PREDICTED_DIRECTION: up
PREDICTED_MAGNITUDE_BAND: mild
SECTOR_SCORES_END

HIT_GRID_BEGIN
Crude oil price surge (WTI/Brent)|HIT|0.85|2026-09-15|https://energynow.com/2026/09/oil-climbs-as-saudi-pipeline-remains-offline-and-hormuz-traffic-slumps/
Geopolitical supply risk premium|HIT|0.85|2026-09-15|https://www.reuters.com/business/energy/saudi-pipeline-outage-threatens-loss-4-global-oil-supply-2026-09-14/
OPEC+ cut / supply discipline|PARTIAL|0.60|2026-09-15|https://www.reuters.com/business/energy/opec-keeps-oil-output-policy-unchanged-october-2026-09-06/
Crack spread / refining margin expansion|HIT|0.70|2026-09-15|https://worldoilmonitor.com/crack-spread
Inventory draw (EIA crude/products)|PARTIAL|0.45|2026-09-15|https://www.rigzone.com/news/usa_crude_oil_stocks_drop_week_on_week-11-sep-2026/
Risk-off tape / flight to safety|HIT|0.75|2026-09-15|https://www.cnbc.com/2026/09/15/stock-market-today-live-updates.html
Real yields rising|HIT|0.70|2026-09-15|https://www.cnbc.com/2026/09/15/treasury-yields-10-year-breaches-5percent.html
USD strengthening|HIT|0.65|2026-09-15|https://finviz.com/futures.ashx
Crowded long (extreme relative performance + valuation)|HIT|0.60|2026-09-15|https://finviz.com/etf.ashx?t=XLE
Sector breadth expansion (% names up)|MISS|0.55|2026-09-15|https://finviz.com/etf.ashx?t=XLE
Large-cap leadership inside sector|PARTIAL|0.55|2026-09-15|https://finviz.com/etf.ashx?t=XLE
Natural gas price surge|MISS|0.80|2026-09-15|https://finviz.com/futures.ashx
Demand destruction (recession/China weak)|PARTIAL|0.40|2026-09-15|https://www.iea.org/reports/oil-market-report
Sector rotation into energy|PARTIAL|0.50|2026-09-15|https://finviz.com/etf.ashx?t=XLE
HORIZON_3D: up/mild — fresh physical supply increment (4% of global supply) has multi-day persistence; watch pipeline repair timeline and EIA prints.
HORIZON_1W: up/mild — supply-shock premium likely holds while East-West pipeline is offline; fade risk rises if repair headlines appear.
HORIZON_2W: flat/mild — geopolitical premium historically fades fast (AMP/DAMP: "temporary premium fades fast"); crowded 1m rel +8.81% + outflow hangover caps extension.
HORIZON_1M: flat/mild — mean-reversion risk on crowded positioning once the supply shock resolves; structural demand-destruction offset (IEA −1.6 mb/d) reasserts.
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -0.5, 'S1_SECTOR_FACTORS': 2.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': -0.5, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.85, 'leading_sum': 5.0, 'divergence_flagged': False, 'total_score': 5.832, 'predicted_direction': 'up', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.65, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.512, 'score': 3.072, 'legs': [{'leg': 'CL', 'pct': 2.37, 'w': 0.35}, {'leg': 'QA', 'pct': 2.31, 'w': 0.15}, {'leg': 'ES', 'pct': 0.34, 'w': 0.6}, {'leg': 'PM:XLE', 'pct': 0.14, 'w': 0.7}]}, 'overlay_score': 3.719, 'overlay_raw': 3.719, 'index_carry': -0.959, 'general_total': -3.836, 'skill_multipliers': {'S0_SHARED_MACRO': 1.25, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.42, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
