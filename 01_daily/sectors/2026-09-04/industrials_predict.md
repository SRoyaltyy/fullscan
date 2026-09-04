# Sector Prediction — Industrials — 2026-09-04

- ETF: **XLI**
- rubric: `00_grounding/sectors/industrials.md`
- predicted_direction: **down**
- predicted_magnitude_band: **flat**
- total_score: **-1.35** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLI vs SPY (yfinance, through 2026-09-03):
  1d: XLI +1.03% | SPY +1.05% | rel -0.02%
  3d: XLI -0.33% | SPY +0.80% | rel -1.12%
  1w: XLI -2.37% | SPY +0.27% | rel -2.64%
  1m: XLI -6.33% | SPY +0.44% | rel -6.77%
```

No flow data returned. I'll note flows as checked/nothing material. Let me finalize.

MEMORY_CONFIRM: Reviewed prior runs for sector Industrials. Rolling dir=0.2 / mag=0.1 (n=10); last 30 dir=0.25 / mag=0.083 (n=12). Last graded 08-28: narrative down/mild vs pipeline down/flat, actual XLI −0.93% (dir HIT, mag MISS on pipeline flat). Governing today: 08-27 (1w/1m laggard + already-public non-holdings mega-cap AHR → **forbid up**, prefer flat or down:mild); 08-18 (cap S1 at 0/+1, don't use GEV/ETN as cushion); 08-21 reversal **off** (ES +0.09% not ≥ +0.3%); 08-28 (emit narrative band, don't import XLF down-bans). DO-INSTEAD: when score fights tape, cut conviction / prefer flat/mild.

## XLI near-session environment (not an SPX call)

### 1. Shared macro as it hits Industrials — S0 = −1
The dominant macro driver today is a **hawkish Fed repricing**, not a benign relief tape.

- **Fed held rates, 3 dissent for hike; Warsh signals September hike risk.** This is the core risk-appetite driver. Gold slid **>3%** on Warsh's Jackson Hole comments boosting September hike bets. Hike odds are rising. For a cyclical like Industrials, this is a **negative overlay** — tighter financial conditions + 30Y at 5.27% (stress zone) + 5-day 10Y–SPX corr **−0.943** (strongly negative: rising yields crush equities).
- **Futures are green but not confirming a cyclical bid.** ES **+0.09%**, NQ **+0.46%** (NQ leading ES by +0.37%, not ≥ +0.5%). Asia composite **+0.84%** (Nikkei +1.26%, Kospi +1.64%), Europe **−0.11%**. The prior session (09-03) was a broad relief rally on "calmer yields as rate-hike bets wane" — but that relief is now being **challenged** by the Warsh-hawkish repricing. This is a reversal-risk setup.
- **Oil is down** (CL −0.78%, BZ −0.49%) — mild cost relief for transports/manufacturers, not a Hormuz squeeze (08-11/08-12 do not fire). Not a cyclical tailwind (08-13).
- **Rates:** DGS10 4.79 / DGS30 5.27 / DFII10 2.45 (prior-close 09-02). 30Y in stress zone. Real yields elevated.
- **USD flat** (DXY +0.16%). VIX 14.2 (low), Fear & Greed 58.2 (Greed).

**S0 = −1, regime risk_off-leaning.** Not −2: futures are green, oil is down, no fresh hard-data miss, VIX is low. Not 0: the hawkish Fed repricing + 30Y stress + strongly negative yield-equity corr is a genuine negative overlay for a cyclical laggard. The green futures are a partial offset but the relief rally is being challenged.

### 2. Spine + secondary — S1 = +1 (capped)
**No fresh same-morning industrials print today.** August ISM manufacturing already printed **09-01**: PMI **54.6** (expansion, 8th month) vs July 55.6; new orders **53.7** (−3.0 pts). Still expansion — **not** an ISM-contraction HIT, but **slowing**. ISM Services printed **09-03** (55.4). Both in the tape. 08-18/08-27: cap S1 at 0/+1.

- **Grid / AI power — HIT, structural.** GEV nuclear/grid deals, backlog growing (Microsoft/Meta multi-decade nuclear deals, White House bulk-power emergency). Structural, semi-independent of ISM. In the tape. 08-18: not a downside cushion.
- **Aerospace & defense — HIT, fresh-ish.** Boeing near an Ethiopian Airlines cargo order (09-02), MAX orders, defense backlog. But SPEEA labor uncertainty is an overhang. Do not cancel ISM (which is expansion, not weak) with one award.
- **Freight — MIXED.** Cass trucking still declining; rail/data-center freight demand stronger. Not a same-morning recovery HIT.
- **Construction slowdown — HIT, carried.** Manufacturing construction off the 2025 peak; AI/nonres is the offset.

Net: carried ISM expansion (slowing) + structural grid + defense order flow vs construction drag + mixed freight. **S1 = +1** (grid + defense are genuine positives, but ISM is slowing and no fresh same-morning print; cap at +1).

### 3. Breadth — S2 = −1
XLI is a **deep laggard**. Channel 1 through 09-03: 1d rel **−0.02%** (flat after relief rally), 3d **−1.12%**, 1w **−2.64%**, 1m **−6.77%**. Leadership is large-cap AI-power/defense carry (GEV/BA), not % of names expanding. Score the lag **once** here.

### 4. Flows — S3 = 0
Checked, nothing material returned on XLI flows this morning. Rotation has been out of industrials into tech. Not a crowded long (1m rel −6.77%). **S3 = 0.**

### 5. ETF tape (confirmation only) — S4 = −1
Channel 1 through 09-03: 1d rel **−0.02%** (flat), 3d **−1.12%**, 1w **−2.64%**, 1m **−6.77%**. Decisive medium-term lag. 1d flat after the relief rally — XLI participated but did NOT lead. Confirmation of underperformance, not an independent second thesis.

### 6. Catalysts / calendar
- **Fed held rates, 3 dissent for hike; Warsh signals September hike risk** — dominant macro driver (hawkish).
- Gold slid >3% on Warsh comments — regime signal.
- Prior session (09-03) best day in a month on calmer yields — being challenged.
- AVGO 09-02 AHR is non-holdings XLK beta, not an XLI spine (08-27).

### Self-audit
- Lens: cyclical; rates only in S0, not re-counted in S1.
- Band: **mild**, not notable (futures green, no fresh hard-data miss, hawkish Fed already partly priced).
- Skew: GEV/BA do not drive the ETF call.
- Same-shock: hawkish Fed counted once in S0.
- 08-27: 1w/1m laggard + non-holdings AHR → forbid up, prefer flat/down:mild. Applied.

**Divergence:** Leading factors (S0 −1 hawkish Fed, S2/S4 lag) point down, but the 1d tape is flat (−0.02%) and futures are green. This is a mild divergence — the tape is not confirming a strong down move. Per DO-INSTEAD, cut conviction / prefer flat/mild.

**Final:** Down/mild at low-moderate confidence. The hawkish Fed repricing + deep laggard status argue down, but green futures and a flat 1d tape cap magnitude at mild and argue for flat. I'll emit **down/mild** at confidence 0.5, acknowledging the flat-futures cap.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: 1
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -1
MULTIPLIER: 0.9
CONFIDENCE: 0.5
REGIME: risk_off
TOTAL_SCORE: -1.8
PREDICTED_DIRECTION: down
PREDICTED_MAGNITUDE_BAND: mild
HORIZON_3D: down:mild:0.5
HORIZON_1W: down:mild:0.5
HORIZON_2W: flat:mild:0.4
HORIZON_1M: flat:mild:0.4
SECTOR_SCORES_END

HIT_GRID_BEGIN
ISM manufacturing / new orders expansion|carried (slowing, PMI 54.6 printed 09-01)|medium|2026-09-01|https://www.ismworld.org
Grid / electrical equipment backlog (AI power)|HIT structural (GEV nuclear/grid deals, MSFT/META nuclear)|high|2026-09-03|https://www.reuters.com
Aerospace & defense order / budget upside|HIT fresh-ish (Boeing Ethiopian cargo order, MAX orders)|medium|2026-09-02|https://www.reuters.com
Freight / trucking / rail volume recovery|MIXED (Cass trucking soft, rail/data-center stronger)|medium|2026-09-01|checked, nothing material
Construction slowdown|HIT carried (manufacturing construction off 2025 peak)|medium|2026-09-01|checked
Sector rotation out of industrials|HIT (1w rel -2.64%, 1m rel -6.77% laggard)|high|2026-09-03|Channel 1 tape
Risk-off tape / flight to safety|HIT (hawkish Fed repricing, gold -3%, 30Y stress)|high|2026-09-04|News Judge
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -1.0, 'divergence_flagged': False, 'total_score': -1.35, 'predicted_direction': 'down', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.5, 'regime': 'risk_off', 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
