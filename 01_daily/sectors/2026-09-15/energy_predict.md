# Sector Prediction — Energy — 2026-09-15

- ETF: **XLE**
- rubric: `00_grounding/sectors/energy.md`
- predicted_direction: **up**
- predicted_magnitude_band: **mild**
- total_score: **4.023** (mult 0.85)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **3.239** (CL +2.24%, QA +2.21%, ES +0.44%, PM:XLE +0.18%) · index_carry **-1.554** (general -6.215) · llm_overlay **2.337** (raw 2.337)

## Channel 1 sector ETF tape

```
ETF XLE vs SPY (yfinance, through 2026-09-14):
  1d: XLE -0.94% | SPY -0.45% | rel -0.49%
  3d: XLE -1.19% | SPY -0.20% | rel -0.99%
  1w: XLE +0.73% | SPY -1.21% | rel +1.94%
  1m: XLE +5.68% | SPY -2.19% | rel +7.87%
```

MEMORY_CONFIRM: Sector Energy (XLE) — memory index unavailable this run (used injected scoreboard/lessons only). Last-10 dir=0.3 mag=0.3 (n=10); last graded 09-14 up/mild vs XLE −0.936% (dir MISS, mag MISS). Applied: **09-14 Energy lesson FIRES** (VIX/VIX3M backwardation = de-risking regime → do NOT mute S0 for a high-beta sector; premarket breadth in a single ETF is not breadth → S2=0 unless constituent-confirmed; a late >2.4% oil spike is fuel already spent → discount S1 for fade; honor the divergence flag; 1m rel ≥ +7.5% + outflow hangover = crowded; do not triple-count the ETF's own premarket print across anchor + S2 + S4). **09-10 Energy lesson FIRES** (record-close sequence + 1m rel ≥ +8% = live crowded-long; do not score S2 and S4 both positive off the same prior-close Channel 1 series; pipeline must honor the prose magnitude cap). **09-08/09-09 magnitude-discipline lesson FIRES** (mag hit-rate ~0.3 <0.4 → cap magnitude unless oil >5% or XLE futures >2%). **09-11 Energy lesson FIRES** (do not assign S0 a negative sign for a beta sector on green futures; a ~2–3% offered barrel is a dip, not a break — it sets RELATIVE sign, not absolute; if the divergence flag fires, FLATTEN the absolute call). **08-11 live-oil verify FIRES** — Channel 1 CL=F +2.0% 1d / BZ=F −2.84% 1d is internally split and is the prior-close column; live premarket Finviz WTI $103.73 (+2.24%) / Brent $108.03 (+2.21%) is the session sign (UP). **08-14 green-oil + live Hormuz FIRES** (oil green >2.2%, Brent >$108). **08-12 stale-run cap does NOT fire** (1w rel +1.94%, not >+4%). **09-03/09-04 exhaustion-after-extended-run does NOT fire** (oil is UP, not offered). Open sector_energy DO-INSTEAD: score sign vs tape conflict → prefer flat/mild, cut conviction.

## Energy / XLE — 2026-09-15

This is a **sector_shock oil-surge session inside a broad risk-off tape**, and it is the *second consecutive* instance of the exact setup that burned 09-14. Crude is bid (WTI $103.73 +2.24%, Brent $108.03 +2.21%, gasoil +2.57%, HO +3.01%, RBOB +1.73% — the whole barrel complex is green), and XLE is one of only two green sector ETFs in the premarket (+0.18% vs XLK −0.65%, XLF −0.17%). But ES −0.57%, NQ −0.66%, Russell −0.77%, DJIA −0.78%, VIX 17.82 with VIX/VIX3M **1.139 backwardation**, USD +0.27%, real yields +0.05 1d / +0.18 1w. The 09-14 lesson says: when backwardation is live, do not mute S0 for a high-beta sector, and do not let the ETF's own premarket gap masquerade as breadth. That is the load-bearing correction today.

### Channel 2

**1. Shared macro as it hits energy.** Equity tape is **risk-off, not a commodity bid from beta**: ES −0.57%, NQ −0.66%, Russell −0.77%, DJIA −0.78%. Asia composite −0.66% (Hang Seng −1.0%, Kospi −0.85%, ASX −0.88%), Europe −0.87% (FTSE −0.87%, DAX −0.86%, CAC −0.89%). VIX 17.82 (+0.72 1d, +2.1 1w) with VIX/VIX3M **1.139 backwardation** — a live de-risking regime, and per the 09-14 lesson that is a **full-weight negative for a high-beta cyclical**, not a mute. **USD strengthening** (DXY +0.15% 1d, USD 99.36 +0.27%) is a mild commodity headwind. **Real yields rising** (DFII10 2.60, +0.05 1d / +0.18 1w / +0.18 1m; DGS10 4.96 +0.19 1w; DGS30 5.35) — secondary vs oil but a multiple headwind. 5-day 10Y-SPX corr −0.107 (weak, so duration is not the dominant transmission today). USEPUINDXD 395.54 (+189.93 1d) — policy uncertainty spiking. News Judge #2 (Treasury yield pressure + chipmaker weakness drag indices) and #1 (AI-pacing rotation chips→software) are the day's dominant SPX-beta drivers and both are **negative for broad beta, neutral-to-negative for energy's multiple**. News Judge #3 (gold surge on Fed rate-cut bets) is a dovish rates signal — mildly supportive of the commodity complex, but gold itself is **−1.08%** on the live tape, so the "Fed cut" read is not confirming today. Per 08-10 keep S0 muted *under a pure sector_shock*, but the 09-14 lesson overrides that mute when backwardation + red tape + firm USD + rising real yields are themselves the dominant driver. **S0 = −1.**

**2. Spine (S1).** One cluster: live crude surge **plus** the same Hormuz premium — but this is a **fresh supply increment**, not a stale continuation.
- **Crude surge (live-verified):** Finviz WTI $103.73 (+2.24%), Brent $108.03 (+2.21%); products bid with crude (HO +3.01%, gasoil +2.57%, RBOB +1.73%). Channel 1 CL=F +2.0% 1d agrees on sign; BZ=F −2.84% 1d is the prior-close column and is **superseded** by the live +2.21% print (08-11: do not use the stale column as the session sign). This is a **strong surge**, the second >2% print in three sessions.
- **Geo premium live and escalating, not faded:** live search confirms **"Brent crude rises as Saudi pipeline outage, fresh attacks raise supply concerns"** (Times of India, 15 Sep 04:13 GMT) and **"Crude Oil Price Forecast: Brent Nears $110 Amid Saudi Pipeline Outage"** (TradingKey, 15 Sep 07:09 GMT), plus **"Oil Prices Rise to $107 as Saudi Pipeline Faces Outage"** (HDFC Sky, 15 Sep 04:42 GMT). A **Saudi pipeline outage** is a *new* physical supply increment on top of the Hormuz/US–Iran narrative — this is not the same shock re-scored. 08-14 FIRES: green oil + current supply-risk headlines → oil spine dominates. Do **not** also score a separate crude-surge HIT on top of geo.
- **Inventory:** last EIA (week ending 8/28) crude −4.5 Mb; the 09-10/09-11 WPSR prints are already in the tape. No fresh crude WPSR today. Do not date a stale draw as today's HIT.
- **OPEC+ (carried offset):** Sep +188 kb/d completed the 2023 voluntary-cut rollback; **Sep 7 meeting held October quotas unchanged** (Economic Times, 07 Sep) — a hold, not a cut, and not a fresh increase. Offset only.
- **Demand destruction (carried official):** IEA 2026 −1.6 mb/d vs OPEC ~+0.6 mb/d. Offset only.
- **Cracks:** HO +3.01% / gasoil +2.57% / RBOB +1.73% — products bid **with** crude, a genuine refining-margin tailwind, but **refiner sleeve only**; do not let VLO/MPC drive the ETF.
- **Nat gas $2.894 (−0.07%)** — no surge; N/A for oil-weighted XLE.
- **Copper −0.89%, gold −1.08%, silver −1.63%, platinum −1.87%** — the metals complex is **not** a floor today; do not import a broad commodity-bid cushion into XLE. This is an **oil-specific** bid, which is exactly what the energy spine should be.

Net **S1 = +2**. Not +3 (the oil/Hormuz/Saudi cluster is counted once; the Saudi pipeline outage is a fresh increment but it is the same barrel complex). Not +1 (08-14 forbids capping S1 when oil is green >2.2% and the chokepoint/supply headline is live). **But** per the 09-14 lesson, a late multi-day oil spike into a de-risking vol regime carries fade risk — I hold S1 at +2 rather than +3 for that reason, and I do not escalate magnitude on it.

**3. Breadth.** Channel 1 tape: XLE 1d rel **−0.49%** (XLE −0.94% vs SPY −0.45% on 09-14 — the sector *lagged* on the prior session despite oil being up). 3d rel −0.99%, 1w rel +1.94%, 1m rel +7.87%. **Premarket:** XLE +0.18% is one of only two green sector ETFs (XLB +0.03%, XLU +0.08%, XLF −0.17%, XLK −0.65%). Per the **09-14 lesson, a single ETF's own premarket print is NOT breadth** — it is the ETF's own gap relabeled, and it is already carried by the tape anchor. I have no constituent-level confirmation (no XOM/CVX/COP premarket prints in Channel 1, no MAP HEAT research block this run). **S2 = 0.**

**4. Flows / positioning.** XLE still carries the multi-week outflow hangover (~$4B over ~65 days). 1m rel **+7.87%** plus the recent record-close sequence **is the crowded-long condition** — the 09-14 lesson explicitly says do not require the full +8% / 1w rel >+5% trigger. With the barrel bid but the 1d/3d relative tape **negative** (−0.49% / −0.99%) and the vol regime de-risking, this is a fade-risk setup, not a fresh accumulation. **S3 = −1.**

**5. Catalysts.** No fresh XLE-wide earnings in the digest. **No crude EIA today** (WPSR is Wednesdays; today is Tuesday). The dominant scheduled items are the AI-pacing rotation and Treasury-yield pressure (News Judge #1/#2) — both broad-beta, not energy-spine. The **Saudi pipeline outage** is the load-bearing same-day energy catalyst. Hormuz reopen-terms remain a fade binary.

### Scoring logic

S0 is **−1, not 0**: the 09-14 lesson is explicit that VIX/VIX3M backwardation (1.139) is a de-risking regime and high-beta cyclicals get sold regardless of their commodity. Red futures across all four indices, firm USD, and rising real yields reinforce it. The 08-10 "mute S0 under sector_shock" rule is overridden when the shared macro is itself the dominant driver — which it was on 09-14 and is again today.

S1 is the live oil/Saudi-outage/Hormuz cluster, netted once at **+2**, held below +3 for late-spike fade risk.

S2 is **0** — the 09-14 lesson forbids scoring the ETF's own premarket gap as breadth, and there is no constituent confirmation.

S3 is **−1** — crowded 1m rel +7.87% with a negative 1d/3d relative tape and an outflow hangover.

S4 is **0** — the prior-session 1d rel is **−0.49%** (mildly negative, sub-threshold), and per the 09-14 lesson I must not re-score the ETF's own premarket print (already in the anchor) as tape confirmation. The 1d rel is not decisively red (|rel| < 1.5%), so it does not earn a −1 either.

**Divergence check:** leading factor sum (S0 −1, S1 +2, S2 0, S3 −1) = **0** against a tape confirmation of **0** — no divergence. But the *internal* tension is real: the sector's own object is up >2% while the shared macro is de-risking. Per the 09-11 lesson, when the divergence flag fires the correct response is to **flatten the absolute call** — and here the arithmetic already lands near zero. That is the honest read: the oil bid and the de-risking tape are roughly offsetting for XLE's absolute print.

**Magnitude discipline:** Energy mag hit-rate is ~0.3 (<0.4). Oil is up +2.24% (not >5%) and XLE premarket is +0.18% (nowhere near >2%). Per the 09-08/09-09 lesson, **cap at mild**. Do not emit notable. Do not emit severe.

**Direction:** With S0 −1 and S3 −1 offsetting S1 +2, the net is ~0. The 09-14 lesson's most important operational point is that the *same* setup (oil up >2%, XLE the only green sector, backwardation, crowded 1m rel) produced a **−0.94% gap-and-fade** one session ago. The 09-11 lesson says a ~2–3% oil move sets the *relative* sign, not the absolute. And the sector_energy DO-INSTEAD says: when score sign conflicts with tape/breadth, cut conviction and prefer flat/mild. I therefore call **flat** with **mild** magnitude — explicitly not up, because the 09-14 falsifier (oil up >2.4% + backwardation + crowded → XLE closed −0.94%) is the most recent and most directly analogous evidence in memory, and because the 1d/3d relative tape is negative going in.

**Multiplier 0.85** (cut conviction: two consecutive direction misses, mag hit-rate 0.3, and a live internal tension between the oil bid and the de-risking tape). **Confidence 0.42.** **Regime: mixed** (sector_shock oil bid vs broad risk-off/de-risking tape — neither dominates cleanly).

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: 2
S2_BREADTH: 0
S3_FLOWS_POSITIONING: -1
S4_ETF_TAPE: 0
MULTIPLIER: 0.85
CONFIDENCE: 0.42
REGIME: mixed
SECTOR_SCORES_END

HIT_GRID_BEGIN
Crude oil price surge (WTI/Brent)|HIT|0.80|2026-09-15|https://news.google.com/rss/articles/CBMilAJBVV95cUxOYm9UVmdLQnFUUVJ1cVllSVd4dVhmSmJpWm1CNkNoV2dHdlBPemxVUERkNHY5TzRyLVVOSVgzQ1g1Yy1rdC1QRzRRMzc3bERPUV9hZlFRbDBjbUxWVldMZTlreERScUdLN09wcUtRa09aTWFKclMyZFVkbzVRdFJ4VTJyd3hTcjZJWlFOVlhwVkRNZDlrWkJNMkhBbHZwYTkwV3ZsSlNvcE4zSktvZVVBVk5xbEZuX294TGY3UnJnQkRmQk53Wk1uclRUSWxaTUNTQkd5dDVOaG9Zd01RWnBvY3pWYXAwMndXOE95czcybDYxRl9Eb1VwcElFcUktTnY2aWZ0ODl2ZVFHOFZ0NlBlbDlxT2bSAZoCQVVfeXFMUExNdFMzVWRMMXMwa3VPSGNiV2RMN3pHWndVUHVDcGUxZW9DZTZNT0ptTEdRbWpzbHhGUF9qZUpxS19RYmhtNXZSOWE4QjE4Z2JHQWNYcnh4aU9VTEllVW41MW91T0RlQjlfRXlQOG1Zd0xMenh3NjdabUgtWFdaZTlUY1NqRkRoVVVETHFRRzZWN2RHVmZPVDZMWXU0dTRHRV9hejgzemN3cnhEVXRLZHoxNTRXbW9DTWxocmhyY0dJdFN6MldmRWZnT2NqWmNpQllXeGp5MTBqaGdtUHBLdndIUGZTOXNXemNiUjlva0cxMnR4UUpqOTR5YWV5Nlk3TnUyS3NYaWx5c05GVU9XNkJkbzVDNURhTzhB
Geopolitical supply risk premium|HIT|0.75|2026-09-15|https://news.google.com/rss/articles/CBMiywFBVV95cUxNMkI3aVBvT25QeDhCN2NCajVqUk5td3Rycjl5WXVJanZNc0VTcXB1NDZQdS1jV1hWbGRNd1pDTkQ4TC0tRDl2ZXR6WE5VN2l5YmtKYzFhU1JEZzZvUWlWSldVSlJVQlJWSzNYdy1xem5USVZZMllEWFhqUHpMNnZ4U1pIeU1UYjQyYWcydG9lb29HU2dPVEIzaHYtQWRYOTU2S0wwU3EyQ1NicWtZSG9fYzFxTll3dm5UQ0JiSG1YdFpJb0NuSVB3QV9VNA
Crack spread / refining margin expansion|HIT|0.60|2026-09-15|
Risk-off tape / flight to safety|HIT|0.70|2026-09-15|
Real yields rising|HIT|0.65|2026-09-15|
USD strengthening|HIT|0.60|2026-09-15|
Crowded long (extreme relative performance + valuation)|HIT|0.60|2026-09-15|
Sector breadth failure (ETF up, names flat)|HIT|0.45|2026-09-15|
OPEC+ production increase / quota break|MISS|0.55|2026-09-15|https://cfo.economictimes.indiatimes.com/news/opec-keeps-october-oil-output-quota-unchanged-from-september-levels/133859563
Inventory draw (EIA crude/products)|MISS|0.50|2026-09-15|
Natural gas price surge|MISS|0.70|2026-09-15|
Sector ETF inflow / relative volume spike|MISS|0.45|2026-09-15|
Sector breadth expansion (% names up)|MISS|0.50|2026-09-15|
HORIZON_3D|flat|0.40|2026-09-15|
HORIZON_1W|up|0.38|2026-09-15|
HORIZON_2W|up|0.35|2026-09-15|
HORIZON_1M|up|0.40|2026-09-15|
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': 2.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': -1.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.85, 'leading_sum': 4.0, 'divergence_flagged': False, 'total_score': 4.023, 'predicted_direction': 'up', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.661, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.5399, 'score': 3.239, 'legs': [{'leg': 'CL', 'pct': 2.24, 'w': 0.35}, {'leg': 'QA', 'pct': 2.21, 'w': 0.15}, {'leg': 'ES', 'pct': 0.44, 'w': 0.6}, {'leg': 'PM:XLE', 'pct': 0.18, 'w': 0.7}]}, 'overlay_score': 2.337, 'overlay_raw': 2.337, 'index_carry': -1.554, 'general_total': -6.215, 'skill_multipliers': {'S0_SHARED_MACRO': 1.25, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.42}
```
