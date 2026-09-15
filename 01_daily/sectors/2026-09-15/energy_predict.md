# Sector Prediction — Energy — 2026-09-15

- ETF: **XLE**
- rubric: `00_grounding/sectors/energy.md`
- predicted_direction: **up**
- predicted_magnitude_band: **mild**
- total_score: **6.75** (mult 0.85)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **3.094** (CL +2.37%, QA +2.31%, ES +0.36%, PM:XLE +0.14%) · index_carry **-0.912** (general -3.65) · llm_overlay **4.569** (raw 4.569)

## Channel 1 sector ETF tape

```
ETF XLE vs SPY (yfinance, through 2026-09-15):
  1d: XLE +2.21% | SPY -0.50% | rel +2.71%
  3d: XLE +1.58% | SPY -0.10% | rel +1.68%
  1w: XLE +1.83% | SPY -1.16% | rel +2.99%
  1m: XLE +6.53% | SPY -2.48% | rel +9.02%
```

MEMORY_CONFIRM: Sector Energy (XLE) — memory index unavailable this run (used injected scoreboard/lessons only). Last-10 dir=0.3 mag=0.3 (n=10); last graded 09-14 up/mild vs XLE −0.936% (dir MISS, mag MISS). Applied: **09-14 Energy lesson FIRES** (VIX/VIX3M backwardation = de-risking regime → do NOT mute S0 for a high-beta sector; premarket breadth in a single ETF is not breadth → S2=0 unless constituent-confirmed; a late >2.4% oil spike is fuel already spent → discount S1 for fade; honor the divergence flag; 1m rel ≥ +7.5% + outflow hangover = crowded; do not triple-count the ETF's own premarket print across anchor + S2 + S4). **09-10 Energy lesson FIRES** (record-close sequence + 1m rel ≥ +8% = live crowded-long; do not score S2 and S4 both positive off the same prior-close Channel 1 series; pipeline must honor the prose magnitude cap). **09-08/09-09 magnitude-discipline lesson FIRES** (mag hit-rate ~0.3 <0.4 → cap magnitude unless oil >5% or XLE futures >2%). **09-11 Energy lesson FIRES** (do not assign S0 a negative sign for a beta sector on green futures; a ~2–3% offered barrel is a dip, not a break — it sets RELATIVE sign, not absolute; if the divergence flag fires, FLATTEN the absolute call). **08-11 live-oil verify FIRES** — Channel 1 CL=F +5.05% 1d / BZ=F +3.30% 1d is internally split and is the prior-close column; live premarket Finviz WTI $103.79 (+2.37%) / Brent $108.11 (+2.31%) is the session sign (UP), independently confirmed by Reuters ("Oil jumps nearly $3 as Saudi export halt, Libya outages stoke supply fears"), EnergyNow ("Oil Climbs as Saudi Pipeline Remains Offline and Hormuz Traffic Slumps"), and FXEmpire ("Saudi Pipeline Outage and China Draws Keep Buyers in Control"). **08-14 green-oil + live Hormuz FIRES** (oil green >2.3%, Brent >$108, fresh physical supply increment). **08-12 stale-run cap does NOT fire** (1w rel +2.99%, not >+4%). **09-03/09-04 exhaustion-after-extended-run does NOT fire** (oil is UP, not offered). Open sector_energy DO-INSTEAD: score sign vs tape conflict → prefer flat/mild, cut conviction.

---

# Energy / XLE — 2026-09-15

This is the **third consecutive instance of the setup that burned 09-14 and 09-11**: a sector_shock oil surge inside a broad risk-off tape. But the microstructure differs from 09-14 in the one way that matters — **VIX/VIX3M is 0.898 (contango), not backwardation**, so the acute de-risking tell that justified full-weight negative S0 on 09-14 is absent. Crude is bid (WTI $103.79 +2.37%, Brent $108.11 +2.31%, gasoil +2.68%, HO +3.09%, RBOB +1.92% — the whole barrel complex is green), and XLE is one of only two green sector ETFs in the premarket (+0.14% vs XLI +0.81%, XLB +0.38%, XLU +0.20%, XLK +0.11%, XLV +0.04%, XLF −0.08%, XLY −0.13%, XLRE −0.24%, XLP −0.33%, XLC −0.63%). But ES −0.54%, NQ −0.62%, Russell −0.73%, DJIA −0.71%, USD +0.25%, real yields +0.05 1d / +0.18 1w. The 09-14 lesson is the load-bearing correction: do not let the ETF's own premarket gap masquerade as breadth, and do not triple-count the ETF's own print across anchor + S2 + S4.

## Channel 2

**1. Shared macro as it hits energy.** Equity tape is **risk-off, not a commodity bid from beta**: ES −0.54%, NQ −0.62%, Russell −0.73%, DJIA −0.71%. Asia composite −0.72% (Kospi −3.26%, Nikkei −0.81%, Hang Seng +0.45%), Europe −0.31% (DAX −0.15%, CAC −0.34%, EuroStoxx −0.38%). **VIX 17.55 (+0.45 1d, +1.83 1w) with VIX/VIX3M 0.898 — contango, NOT backwardation.** This is the key differentiator from 09-14 (which had 1.135 backwardation): the vol term structure is *not* signaling acute de-risking, so the 09-14 "full-weight negative S0" clause fires at reduced weight, not full. **USD strengthening** (DXY +0.17% 1d, USD 99.36 +0.25%) is a mild commodity headwind. **Real yields rising** (DFII10 2.60, +0.05 1d / +0.18 1w / +0.18 1m; DGS10 4.96 +0.19 1w; DGS30 5.35) — secondary vs oil but a multiple headwind. 5-day 10Y-SPX corr −0.178 (weak, so duration is not the dominant transmission today). News Judge #1 (10Y breaches 5%, global bond selloff), #2 (Aug CPI core 0.3% locks Sept hike), #3 (FOMC/SEP/Warsh — unresolved binary, do not pre-score) are the day's dominant SPX-beta drivers and all are **negative for broad beta, neutral-to-negative for energy's multiple**. News Judge #5 (US-Iran tanker war, Hormuz impaired, Brent ~$106–108) is the sector's own object and is **live**. Per 08-10 keep S0 muted *under a pure sector_shock*, but the 09-14 lesson overrides that mute when red tape + firm USD + rising real yields are themselves a driver. **S0 = −0.5** (not −1: contango, not backwardation, so the acute de-risking tell is absent).

**2. Spine (S1).** One cluster: live crude surge **plus** the same Hormuz premium — but with a **fresh physical supply increment**.
- **Crude surge (live-verified):** Finviz WTI $103.79 (+2.37%), Brent $108.11 (+2.31%); products bid with crude (HO +3.09%, gasoil +2.68%, RBOB +1.92%). Channel 1 CL=F +5.05% 1d agrees on sign; BZ=F +3.30% 1d agrees. This is a **strong surge**, the third >2% print in five sessions.
- **Geo premium live, with a fresh physical increment:** live search confirms **"Oil jumps nearly $3 as Saudi export halt, Libya outages stoke supply fears"** (Reuters, 15 Sep), **"Oil Climbs as Saudi Pipeline Remains Offline and Hormuz Traffic Slumps"** (EnergyNow, 15 Sep), and **"Attacks Knock Saudi Arabia East-West Pipeline Offline, Driving Up Brent And WTI Prices"** (NewsCord, 15 Sep). A pipeline outage plus Libya outages is a *new* physical supply increment on top of the Hormuz/US–Iran narrative — this is not merely a day-4 continuation. 08-14 FIRES: green oil + current supply-risk headlines → oil spine dominates. Do **not** also score a separate crude-surge HIT on top of geo.
- **Inventory:** EIA week ending 8/28 was crude −4.5 Mb; the 09-11 WPSR (Rigzone, 11 Sep) showed a further **week-on-week crude draw**. FXEmpire (15 Sep) cites "China Draws" as a supporting bid. Next crude WPSR is **tomorrow (09-16)** — unprinted at snapshot. Do not date the stale draw as today's HIT.
- **OPEC+ (carried offset):** Reuters (06 Sep) — **OPEC+ keeps oil output policy unchanged for October**; Astana Times confirms focus shifts to 2027. Not a cut, not an increase. Neutral offset.
- **Demand destruction (carried official):** IEA 2026 −1.6 mb/d vs OPEC ~+0.6 mb/d. Offset only.
- **Cracks:** diesel/gasoil still extreme; products bid with crude — a refiner tailwind, and MAP HEAT's **Refining & Marketing OVERRIDE (dir=up, conv=high, +5.86% w1, +4.26 vs parent, VLO at 52-wk highs)** confirms it. But **refiner sleeve only**; do not let VLO/MPC drive XLE.
- **Nat gas $2.893 (−0.07%)** — no surge; N/A for oil-weighted XLE.
- **Uranium $89.9 (−0.11%)** — MAP HEAT dir=down; must not be averaged into XLE.

Net **S1 = +2**. Not +3 (same oil/Hormuz shock counted once, though the Saudi pipeline outage is a genuine fresh increment). Not +1 (08-14 forbids capping S1 when oil is green >2.3% and the chokepoint headline is live).

**3. Breadth.** Channel 1 tape: XLE 1d rel **+2.71%** (XLE +2.21% vs SPY −0.50% on 09-14 — the sector *led* the prior session's risk-off tape). 3d rel +1.68%, 1w rel +2.99%, 1m rel +9.02%. **Per the 09-14 lesson, the ETF's own premarket print (+0.14%) is NOT breadth** — it is the same object as the anchor (PM:XLE +0.14%) and S4. MAP HEAT gives the constituent-level read: **Integrated dir=up conv=medium (XOM:pos, CVX:pos, "the only Energy sub-sector beating parent (+0.37) with 0.75 breadth")** and **E&P dir=up conv=medium (COP:pos, breadth 0.449 improving)**. That is genuine intra-sector participation in the two largest XLE sleeves. But Services is dir=down (SLB −4.13% w1, breadth 0.26) and Drilling dir=down. Net: two of the four major sleeves are participating, two are not. **S2 = +0.5** (not +1 — the 09-14 lesson caps this, and the ETF's own print is excluded).

**4. Flows / positioning.** XLE still has the multi-week outflow hangover (~$4B over ~65 days). 1m rel **+9.02%** is leftover leadership, and per the 09-10/09-14 lessons, **1m rel ≥ +7.5% with an outflow hangover IS the crowded-long condition** — do not require the full +8% / 1w rel >+5% trigger. However, 1w rel is only **+2.99%** and the 09-14 gap-and-fade has already partially de-risked the crowd (XLE −0.94% yesterday). Per the "binding lesson precondition absent" rule, a complex that has just de-risked into a fresh physical supply increment is not the same crowded-long unwind setup. **S3 = −0.5** (crowded-long unwind risk, damped for the prior-day de-risk).

**5. Catalysts.** **FOMC/SEP/Warsh press conference is the dominant binary today** — two-sided, not the energy spine; do not pre-score. **EIA crude WPSR is tomorrow (09-16)**, not today. No fresh XLE-wide earnings. The Saudi East-West pipeline outage is the load-bearing same-day catalyst and it is **physical, not rhetorical** — that is the difference from 09-14's "late spike."

## Scoring logic

S0 = −0.5: red tape + firm USD + rising real yields are a genuine headwind for a high-beta sector, but **contango (0.898), not backwardation**, means the acute de-risking tell is absent — so this is a half-weight negative, not the full-weight negative the 09-14 lesson prescribes for backwardation.

S1 = +2: the oil/Hormuz cluster netted once, with the Saudi pipeline outage as a fresh physical increment. 08-14 forbids capping below +2 when oil is green >2.3% and the chokepoint headline is live.

S2 = +0.5: constituent-confirmed participation (Integrated 0.75 breadth, E&P 0.449 improving) but Services/Drilling lagging, and the ETF's own premarket print is explicitly excluded per 09-14.

S3 = −0.5: crowded 1m rel +9.02% with outflow hangover, damped because the 09-14 fade already partially de-risked the crowd.

S4 = 0: **the 09-14 lesson's core correction.** The 1d rel +2.71% is the prior session's tape, and the ETF's own premarket print is already in the anchor. Do not score it again.

**Divergence check:** factors (S1 +2) vs tape (S4 0, S0 −0.5) — the leading factor sum is positive while the tape confirmation is neutral-to-negative. Per the 09-11 rule, a fired divergence flag **flattens the absolute call**. But here the divergence is *mild* (S4 = 0, not negative) and the sector's own object is genuinely surging on a fresh physical increment. The 09-11 rule's precondition was a *dip* (~2–3% offered barrel); today the barrel is **up** >2.3% with a pipeline offline. So the divergence is flagged but does not force a flat call — it caps magnitude and cuts conviction.

**Magnitude discipline:** 09-08/09-09 lesson — mag hit-rate ~0.3 (<0.4) + risk-off tape → cap at **mild** unless oil >5% or XLE futures >2%. Oil is +2.37% (not >5%), XLE premarket +0.14% (not >2%). **Cap at mild.** The 09-10 lesson also requires the pipeline to honor the prose cap.

**Direction:** up. The sector's own object is surging on a fresh physical supply increment, the 1d tape confirms transmission (+2.71% rel), and the vol term structure is not signaling acute de-risking. The 09-14 lesson's protective clauses fire at reduced weight, not full.

**Multiplier 0.85** (cut from 1.0): three consecutive Energy direction misses, mag hit-rate 0.3, and a live FOMC binary. **Confidence 0.52.**

**Regime: mixed** — sector_shock oil surge inside a risk-off broad tape, with the vol term structure in contango rather than backwardation.

## HORIZON_3D / 1W / 2W / 1M

- **3D:** up/mild. The Saudi pipeline outage is a physical supply event with multi-day repair timelines; Brent >$108 with Hormuz impaired keeps the bid alive. Risk: FOMC hawkish surprise compresses the multiple.
- **1W:** up/mild-to-notable. If the pipeline stays offline and EIA (09-16) confirms a draw, the barrel complex has room. Crowded 1m rel +9.02% is the cap.
- **2W:** flat-to-up. The 09-10 record-close sequence and the outflow hangover argue for mean reversion once the physical increment is priced. Watch for a Hormuz reopen headline — that is the fade binary.
- **1M:** flat. 1m rel +9.02% is already extended; the sector needs a *new* physical increment each week to sustain. Absent one, the crowded-long unwind risk dominates.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -0.5
S1_SECTOR_FACTORS: 2
S2_BREADTH: 0.5
S3_FLOWS_POSITIONING: -0.5
S4_ETF_TAPE: 0
MULTIPLIER: 0.85
CONFIDENCE: 0.52
REGIME: mixed
DIVERGENCE_FLAGGED: true
SECTOR_SCORES_END

HIT_GRID_BEGIN
Crude oil price surge (WTI/Brent)|HIT|0.90|2026-09-15|https://news.google.com/rss/articles/CBMiwwFBVV95cUxNVGZNVlpqdGZqMGlsa1lSR2IyWU0tbmd2R1ZkY2o5VDNlb0VsMEdhbWlZVzJBLXJ0WU9MTm13WnN2R0M0Slg4TzFkb1VSYVRxQXE5ejQ5eTNRbGcxdUVSNTFKYlk2WmxYbkdPMTJIUEZJRVdBUmFKVHVCamlsUzB2NFJLbkNGbkJFSjR3TTlhNF9vT2g4MXk0MjlzOHhVaWNGX3U3RGlUS0JzekpCb0VBbU8zMGdUbFpIaGlCVHlwcEtoR0U
Geopolitical supply risk premium|HIT|0.85|2026-09-15|https://news.google.com/rss/articles/CBMiowFBVV95cUxPSHpBdnVEWUtWOUcwdTZEbDlVSnZBQUlxWHFqWWUxR1FLU0o3eExqbWJwOENvMXA0bjViMk5xZTEyS1JKMWh2WjV1UmFxenZ1SW9MNkUzVVZneG9mSEE2SFRONDNLcG43blU2MjhfNk5kM09xNDJnRC1MVnJrZVJmSlpablM2NzhXRjlrcGVJS2hHOXhZLTUzSTk0aUptY2hoVDhn
Inventory draw (EIA crude/products)|HIT|0.55|2026-09-11|https://news.google.com/rss/articles/CBMimwFBVV95cUxOR09DM1I3anBpRWhyLUZxLUpfVThKbjBLdlNiU1dURzd0TzVrWC14OHJMQVNGNUF4UDhKUXRKei1PdVNFVXRNdGtNcC14QXVxcll3X0p5WFV0ZFJfTE9nSVJ1TlU5bGFZaXZPSDl2RXNYM0dlZ0piVGoyU3B6aXNFWTBRU0JlYVhQbFlqdjk2Wkw5SFI5VHp6eEoyUQ
OPEC+ cut / supply discipline|MISS|0.70|2026-09-06|https://news.google.com/rss/articles/CBMisgFBVV95cUxPWEdjS0pVMy1JLXZFN0V6ZFhjdjNOOXpjRnUtYXdYZEdjbzNaR2R6TGU4UTRDVEdLazJuMS12eHp6d0hWQjI0ZzdhRTUtbVBqR3NhSGRES0UxbG1TZ29QX1hjaWZFNWs1RWNWV1E0azdPVDNIR2x4TGJYMzdDZ3YtVUN1VzZEam5Tb3pXYy1TejN0VFMxYjI1U3ByeFZDZmZ1UnFYX053R3N3YzhST3dSWGdB
Crack spread / refining margin expansion|HIT|0.75|2026-09-10|https://news.google.com/rss/articles/CBMixgFBVV95cUxPZm5naGU4Y3RrQW9SYTJUYURJVHlQRGFWa08tMl9sZ1podi0xWTdqWEI4dk53UkZ4UUJvcUJyMHpJYVRCOUNiVXZNVUdjN1I1anlGYjdVSjZmemRDX2g4MjhUNHF2eU55Z2ZYeUd0eDU2RGw1azRwa2xka1ZPcEhWWXo5c0pyRXJINTk1U1NocFRQRWxxaEIxWUtINTI3NjlQRlhoSFpTaEhHdXl0cThFR3M1bDRMYkRIWjdfY1F5WXJheHBmUmc
Sector rotation into energy|HIT|0.60|2026-09-15|
Crowded long (extreme relative performance + valuation)|HIT|0.65|2026-09-15|
Risk-off tape / flight to safety|HIT|0.70|2026-09-15|
Real yields rising|HIT|0.65|2026-09-15|
USD strengthening|HIT|0.60|2026-09-15|
Sector breadth expansion (% names up)|PARTIAL|0.50|2026-09-15|
Sector ETF outflow / volume dry-up|HIT|0.55|2026-09-15|
Natural gas price surge|MISS|0.85|2026-09-15|
Crude price collapse|MISS|0.90|2026-09-15|
Demand destruction (recession/China weak)|MISS|0.60|2026-09-15|
Inventory build|MISS|0.70|2026-09-15|
Crack spread collapse|MISS|0.80|2026-09-15|
OPEC+ production increase / quota break|MISS|0.75|2026-09-06|
Sector rotation out of energy|MISS|0.60|2026-09-15|
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -0.5, 'S1_SECTOR_FACTORS': 2.0, 'S2_BREADTH': 0.5, 'S3_FLOWS_POSITIONING': -0.5, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.85, 'leading_sum': 6.0, 'divergence_flagged': False, 'total_score': 6.75, 'predicted_direction': 'up', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.65, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.5156, 'score': 3.094, 'legs': [{'leg': 'CL', 'pct': 2.37, 'w': 0.35}, {'leg': 'QA', 'pct': 2.31, 'w': 0.15}, {'leg': 'ES', 'pct': 0.36, 'w': 0.6}, {'leg': 'PM:XLE', 'pct': 0.14, 'w': 0.7}]}, 'overlay_score': 4.569, 'overlay_raw': 4.569, 'index_carry': -0.912, 'general_total': -3.65, 'skill_multipliers': {'S0_SHARED_MACRO': 1.25, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.52, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
