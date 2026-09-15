# Sector Prediction — Energy — 2026-09-15

- ETF: **XLE**
- rubric: `00_grounding/sectors/energy.md`
- predicted_direction: **up**
- predicted_magnitude_band: **mild**
- total_score: **6.779** (mult 0.85)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **3.989** (CL +2.37%, QA +2.31%, ES +0.80%, PM:XLE +0.24%) · index_carry **-0.716** (general -2.865) · llm_overlay **3.506** (raw 3.506)

## Channel 1 sector ETF tape

```
ETF XLE vs SPY (yfinance, through 2026-09-14):
  1d: XLE -0.94% | SPY -0.45% | rel -0.49%
  3d: XLE -1.19% | SPY -0.20% | rel -0.99%
  1w: XLE +0.73% | SPY -1.21% | rel +1.94%
  1m: XLE +5.68% | SPY -2.19% | rel +7.87%
```

MEMORY_CONFIRM: Sector Energy (XLE) — memory index unavailable this run (used injected scoreboard/lessons only). Last-10 dir=0.3 mag=0.3 (n=10); last graded 09-14 up/mild vs XLE −0.936% (dir MISS, mag MISS). Applied: **09-14 Energy lesson FIRES** (VIX/VIX3M backwardation = de-risking regime → do NOT mute S0 for a high-beta sector; premarket breadth in a single ETF is not breadth → S2=0 unless constituent-confirmed; a late >2.4% oil spike is fuel already spent → discount S1 for fade; honor the divergence flag; 1m rel ≥ +7.5% + outflow hangover = crowded; do not triple-count the ETF's own premarket print across anchor + S2 + S4). **09-10 Energy lesson FIRES** (record-close sequence + 1m rel ≥ +8% = live crowded-long; do not score S2 and S4 both positive off the same prior-close Channel 1 series; pipeline must honor the prose magnitude cap). **09-08/09-09 magnitude-discipline lesson FIRES** (mag hit-rate ~0.3 <0.4 → cap magnitude unless oil >5% or XLE futures >2%). **09-11 Energy lesson FIRES** (do not assign S0 a negative sign for a beta sector on green futures; a ~2–3% offered barrel is a dip, not a break — it sets RELATIVE sign, not absolute; if the divergence flag fires, FLATTEN the absolute call). **08-11 live-oil verify FIRES** — Channel 1 CL=F +1.4% 1d / BZ=F −3.88% 1d is internally split and is the prior-close column; live premarket Finviz WTI $103.79 (+2.37%) / Brent $108.11 (+2.31%) is the session sign (UP), independently confirmed by Reuters/NDTV Profit/CNBC-TV18 (Brent ~$107, WTI ~$103, Saudi East-West pipeline offline after fresh attacks). **08-14 green-oil + live Hormuz FIRES** (oil green >2.3%, Brent >$108, fresh physical supply increment). **08-12 stale-run cap does NOT fire** (1w rel +1.94%, not >+4%). **09-03/09-04 exhaustion-after-extended-run does NOT fire** (oil is UP, not offered). Open sector_energy DO-INSTEAD: score sign vs tape conflict → prefer flat/mild, cut conviction.

## Energy / XLE — 2026-09-15

This is the **second consecutive instance of the exact setup that burned 09-14**: a sector_shock oil surge inside a broad risk-off tape, with VIX/VIX3M in backwardation. Crude is bid (WTI $103.79 +2.37%, Brent $108.11 +2.31%, gasoil +2.68%, HO +3.09%, RBOB +1.92% — the whole barrel complex is green), and XLE is one of only two green sector ETFs in the premarket (+0.24% vs XLK +0.05%, XLF −0.24%, XLC −0.63%). But ES −0.54%, NQ −0.62%, Russell −0.73%, DJIA −0.71%, VIX 17.05 with VIX/VIX3M **1.093 backwardation**, USD +0.25%, real yields +0.05 1d / +0.18 1w. The 09-14 lesson is the load-bearing correction: when backwardation is live, do not mute S0 for a high-beta sector, and do not let the ETF's own premarket gap masquerade as breadth.

### Channel 2

**1. Shared macro as it hits energy.** Equity tape is **risk-off, not a commodity bid from beta**: ES −0.54%, NQ −0.62%, Russell −0.73%, DJIA −0.71%. Asia composite −0.66% (Hang Seng −1.0%, Kospi −0.85%, ASX −0.88%), Europe −0.18% (FTSE −0.3%, DAX −0.09%, CAC −0.21%). VIX 17.05 (−0.05 1d, +1.33 1w) with VIX/VIX3M **1.093 backwardation** — a live de-risking regime, and per the 09-14 lesson that is a **full-weight negative for a high-beta cyclical**, not a mute. **USD strengthening** (DXY +0.12% 1d, USD 99.36 +0.25%) is a mild commodity headwind. **Real yields rising** (DFII10 2.60, +0.05 1d / +0.18 1w / +0.18 1m; DGS10 4.96 +0.19 1w; DGS30 5.35) — secondary vs oil but a multiple headwind. 5-day 10Y-SPX corr −0.107 (weak, so duration is not the dominant transmission today). USEPUINDXD 395.54 (+189.93 1d) — policy uncertainty spiking. News Judge #1 (10Y breaches 5%, global bond selloff) and #2 (FOMC/SEP/Warsh) are the day's dominant SPX-beta drivers and both are **negative for broad beta, neutral-to-negative for energy's multiple**. News Judge #5 (US-Iran tanker war, Brent ~$107, Hormuz impaired) is the sector's own object and is **live and escalating**. Per 08-10 keep S0 muted *under a pure sector_shock*, but the 09-14 lesson overrides that mute when backwardation + red tape + firm USD + rising real yields are themselves the dominant driver. **S0 = −1.**

**2. Spine (S1).** One cluster: live crude surge **plus** the same Hormuz premium — but this is a **fresh physical supply increment**, not a stale continuation.
- **Crude surge (live-verified):** Finviz WTI $103.79 (+2.37%), Brent $108.11 (+2.31%); products bid with crude (HO +3.09%, gasoil +2.68%, RBOB +1.92%). Channel 1 CL=F +1.4% 1d agrees on sign; BZ=F −3.88% 1d is the prior-close column and is **superseded** by the live +2.31% print (08-11: do not use the stale column as the session sign). This is a **strong surge**, the third >2% print in five sessions.
- **Geo premium live and escalating, not faded:** live search confirms **"Oil prices climb as attacks, pipeline outage deepen Saudi supply fears"** (Reuters, 15 Sep) and **"Brent crude near $107 as Saudi pipeline shutdown keeps supply risks in focus"** (NDTV Profit, 15 Sep) — the kingdom's **East-West Pipeline is offline** after fresh attacks. A pipeline outage is a *new* physical supply increment on top of the Hormuz/US–Iran narrative — this is not the same shock re-scored. 08-14 FIRES: green oil + current supply-risk headlines → oil spine dominates. Do **not** also score a separate crude-surge HIT on top of geo.
- **Inventory:** last EIA (week ending 9/4, released ~9/10) showed **crude stocks falling week-on-week** with fuel inventories rising on strong refining (BOE Report/Rigzone, 11 Sep). Next crude WPSR is **not today**. Stale/mixed — not a live draw or a live collapse. Do not date it as today's HIT.
- **OPEC+ (carried offset):** Sep +188 kb/d completed the 2023 voluntary-cut rollback; Sep 6 meeting held October unchanged. Not a cut.
- **Demand destruction (carried official):** IEA 2026 −1.6 mb/d vs OPEC ~+0.6 mb/d. Offset only.
- **Cracks:** HO +3.09%, gasoil +2.68% — products bid with crude, a refiner tailwind, but **refiner sleeve only**; do not let VLO/MPC drive XLE. MAP HEAT OVERRIDE Refining & Marketing dir=up conv=high (+5.86% w1, +4.26 vs parent, VLO at 52-wk highs) — nested, do not average into the parent ETF.
- **Nat gas $2.893 (−0.07%)** — no surge; N/A for oil-weighted XLE.
- **MAP HEAT nested:** E&P dir=up conv=medium (COP +2.83% w1, breadth 0.449 improving) and Integrated dir=up conv=medium (XOM/CVX both green, only sub-sector beating parent) — these are the cleanest nested longs and support the parent. Drilling dir=flat, Equipment & Services dir=down (SLB −4.13% w1), Thermal Coal dir=down, Uranium dir=down — do not average these into XLE.

Net **S1 = +2**. Not +3 (same oil/Hormuz shock counted once, though the pipeline outage is a fresh physical increment). Not +1 (08-14 forbids capping S1 when oil is green >2.3% and the chokepoint headline is live).

**3. Breadth.** Channel 1 tape: XLE 1d rel **−0.49%** (XLE −0.94% vs SPY −0.45% on 09-14 — the sector *lagged* the prior session's tape). 3d rel −0.99%, 1w rel +1.94%, 1m rel +7.87%. **Premarket breadth is the live signal**: XLE +0.24% is one of only two green sector ETFs vs XLK +0.05%, XLF −0.24%, XLC −0.63%, XLP −0.43%, XLY −0.13%. But per the 09-14 lesson, **premarket breadth in a single ETF is not breadth — it is the ETF's own gap relabeled**. MAP HEAT gives genuine intra-sector constituent participation (Integrated XOM/CVX both green, E&P COP +2.83% w1, breadth 0.449 improving), which is the constituent confirmation the lesson requires. That earns a modest positive, not a full +1. **S2 = +0.5.**

**4. Flows / positioning.** XLE still has the multi-week outflow hangover (~$4B over ~65 days). 1m rel **+7.87%** plus the recent record-close sequence **IS the crowded-long condition** — do not gate it on 1w rel >+5% (09-10 lesson). With the barrel bid but the 1d/3d tape negative and the broad tape risk-off, this is a fade-risk setup. **S3 = −0.5** (crowded-long unwind risk).

**5. Catalysts.** **FOMC/SEP/Warsh** is the dominant scheduled binary — two-sided, not the energy spine. **10Y through 5%** is a live multiple headwind. No crude EIA today. No fresh XLE-wide earnings. Saudi pipeline outage + Hormuz remain the load-bearing catalysts.

### Scoring logic

S0 is **−1**, not muted: the 09-14 lesson overrides the 08-10 sector_shock mute when backwardation + red tape + firm USD + rising real yields are the dominant driver. This is the single most important correction versus 09-14.

S1 is the oil/Hormuz/pipeline cluster, netted once, with carried OPEC+/IEA as dampeners only. The pipeline outage is a fresh physical increment, so 08-14's license holds — but the 09-14 lesson says a late, already-gapped oil spike is fuel partly spent, so I do not escalate to +3.

S2 is **+0.5**, not +1: constituent confirmation (Integrated/E&P green) earns a modest positive, but the ETF's own premarket print is not breadth and must not be double-counted against the anchor.

S3 is **−0.5**: 1m rel +7.87% + outflow hangover = crowded, and the 1d/3d tape is negative.

S4 is Channel 1 confirmation only: 1d rel **−0.49%**, 3d **−0.99%**, 1w **+1.94%**, 1m **+7.87%**. The 1d/3d tape says the prior session's oil bid **did not transmit** — XLE lagged. That is a **negative** tape confirmation, not a positive one. **S4 = −0.5.**

**Divergence flag: TRUE.** Leading factors (S1 +2) fight the tape confirmation (S4 −0.5, 1d/3d rel negative). Per the 09-11 rule, a fired divergence flag **FLATTENS the absolute call** — this is exactly the mechanism that would have converted 09-14's MISS into a HIT. The leading sum is positive but the tape is not confirming, and the shared macro is a full-weight negative.

Magnitude discipline: 09-08/09-09 lesson caps at **mild** (mag hit-rate ~0.3 <0.4, risk-off tape, no >5% oil move and no XLE futures >2%). The 09-14 lesson adds: a gap-up + de-risking vol regime + crowded 1m rel + fading-late oil favors flat-to-down. Multiplier **0.85** (keep direction, shrink the lever after consecutive mag misses). Confidence **0.42** — low, because the divergence flag is fired and the two channels disagree.

Regime **mixed**: the sector's own object is bid but the broad tape is risk-off and the vol regime is de-risking.

**Direction: up/mild, low conviction, divergence flagged.** The oil spine is real and fresh (pipeline outage), which justifies the up lean; but S0 −1, S3 −0.5, S4 −0.5, and the fired divergence flag mean this is a **flat-to-mild-up** call, not a notable one. If the tape confirms at the open (XLE holding green while ES stabilizes), the up lean holds; if XLE fades the gap like 09-14, the flat/down side is live. I am explicitly not repeating 09-14's error of letting the ETF's own premarket gap carry S2 at full weight.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: 2
S2_BREADTH: 0.5
S3_FLOWS_POSITIONING: -0.5
S4_ETF_TAPE: -0.5
MULTIPLIER: 0.85
CONFIDENCE: 0.42
REGIME: mixed
DIVERGENCE_FLAGGED: true
SECTOR_SCORES_END

HIT_GRID_BEGIN
Crude oil price surge (WTI/Brent)|HIT|0.85|2026-09-15|https://www.reuters.com/business/energy/oil-prices-rise-saudi-pipeline-outage-fresh-attacks-raise-supply-concerns-2026-09-15/
Geopolitical supply risk premium|HIT|0.80|2026-09-15|https://www.ndtvprofit.com/markets/oil-prices-on-september-15-brent-crude-near-107-as-saudi-pipeline-shutdown-keeps-supply-risks-in-focus-12047165
Risk-off tape / flight to safety|HIT|0.75|2026-09-15|
Real yields rising|HIT|0.70|2026-09-15|
USD strengthening|HIT|0.65|2026-09-15|
Crowded long (extreme relative performance + valuation)|HIT|0.60|2026-09-15|
Sector breadth expansion (% names up)|PARTIAL|0.45|2026-09-15|
Sector ETF outflow / volume dry-up|PARTIAL|0.50|2026-09-15|
Inventory draw (EIA crude/products)|MISS|0.30|2026-09-15|https://news.google.com/rss/articles/CBMipwFBVV95cUxNWEgyU1J6XzNzX0NJMGNJWWsycDN5YV9oa2lGclZ3LUx0eEc5RGZMUmxwRXBzNEVUTmhLQlhrX05rNFR1c0JYMmh2U0ZFd0hIRXhmcE1NcFoyUFJXVXItS3NtNDRES2F6VDFyVGJqWjJQOFVneEkzdzhha2ZoWlJkeHY2cV90Mm5tQlMwa0laZ0R5T0xRbHgxQUtjLTVJMjQ0cFpUN2FGTdIBrAFBVV95cUxPSnFGWFJJWlVBcDBzZFNZZ3dfcWlzUXJLaHdad2lFLU1rTHIxbW16c2MtaDI5VHRVcnVEZlV1MkRJeXNFMzlPNGFCYUlCazFNakF6Z2dmOE5qSTVSVS0tN1Y1SlZmTEFFT0xYT0pLS1ZSMGdjQUJ2OUpreVhnbGdhVVlrYmtDbC1oTEhXR0ZieTlxVVNIUEloZ3ZXRllLOTlyV3dfUzBjblFMUTFU
OPEC+ cut / supply discipline|MISS|0.25|2026-09-15|
Natural gas price surge|MISS|0.20|2026-09-15|
Crack spread / refining margin expansion|PARTIAL|0.55|2026-09-15|
Sector rotation into energy|PARTIAL|0.45|2026-09-15|
HORIZON_3D|up|0.40|2026-09-15|
HORIZON_1W|flat|0.35|2026-09-15|
HORIZON_2W|flat|0.30|2026-09-15|
HORIZON_1M|up|0.35|2026-09-15|
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': 2.0, 'S2_BREADTH': 0.5, 'S3_FLOWS_POSITIONING': -0.5, 'S4_ETF_TAPE': -0.5}, 'multiplier': 0.85, 'leading_sum': 5.0, 'divergence_flagged': False, 'total_score': 6.779, 'predicted_direction': 'up', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.65, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.6648, 'score': 3.989, 'legs': [{'leg': 'CL', 'pct': 2.37, 'w': 0.35}, {'leg': 'QA', 'pct': 2.31, 'w': 0.15}, {'leg': 'ES', 'pct': 0.8, 'w': 0.6}, {'leg': 'PM:XLE', 'pct': 0.24, 'w': 0.7}]}, 'overlay_score': 3.506, 'overlay_raw': 3.506, 'index_carry': -0.716, 'general_total': -2.865, 'skill_multipliers': {'S0_SHARED_MACRO': 1.25, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.42, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
