# Sector Prediction — Industrials — 2026-09-09

- ETF: **XLI**
- rubric: `00_grounding/sectors/industrials.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **-4.05** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLI vs SPY (yfinance, through 2026-09-09):
  1d: XLI -1.04% | SPY -0.39% | rel -0.64%
  3d: XLI -1.12% | SPY -1.32% | rel +0.21%
  1w: XLI -0.07% | SPY +0.15% | rel -0.22%
  1m: XLI -6.50% | SPY -1.31% | rel -5.19%
```

I have enough context. Let me compile the analysis for Industrials/XLI for 2026-09-09.

Key facts for today (2026-09-09):
- **Oil has breached $100** (WTI above $100, first time since July) on continued Middle East conflict — this is a **live, escalating supply shock**, not a stale headline. The 09-08 Houthi attack on Saudi energy facilities has escalated further.
- **Futures muted/down pre-market** — "Wall St futures muted as oil tops $100", "US Stock Index Futures Down Pre-Market... Oil Breaches $100". This is a risk-off overlay.
- **CPI due this week** (key inflation data) — the binary.
- **XLI tape**: 1d rel −0.64% (negative), 3d rel +0.21%, 1w rel −0.22%, 1m rel −5.19%. Deep medium-term laggard.
- **Rates**: 30Y in stress zone (~5.25%), real yields elevated, 5-day 10Y-SPX corr strongly negative.
- **Fed hike odds contested** — Warsh hawkish, Waller muddied.
- **MAP HEAT**: Airlines up (oil drop? but oil is up...), Building Products up (Carrier raise), Electrical Equipment up (VRT/HUBB AI power), E&C up (PWR/FIX data center). Conglomerates down, Consulting down (EFX regulatory).

Wait — the MAP HEAT shows Airlines dir=up citing "Oil drop" but oil is breaching $100 today. That MAP HEAT may be from a prior snapshot. Let me note that discrepancy. The Airlines "oil drop" captain read conflicts with today's oil >$100. I should weight the live oil breach over the stale MAP HEAT airline read.

Let me finalize the analysis.

MEMORY_CONFIRM: Reviewed prior runs for sector Industrials. Rolling dir=0.2 / mag=0.1 (n=10); last 30 dir=0.25 / mag=0.083 (n=12). Last graded 08-28: narrative down/mild vs pipeline down/flat, actual XLI −0.93% (dir HIT, mag MISS on pipeline flat). 09-01 down/mild, 09-02 flat/flat, 09-03 flat/flat (missed +1.03% on ISM Services beat), 09-04 down/flat (missed +0.41% up on laggard-shield), 09-08 flat/flat (pipeline −3.6 vs narrative down/mild, actual XLI −0.485% — dir MISS on flat, mag MISS). Governing today: **09-08 lesson** (reconcile narrative vs pipeline; when score fights tape with positive 1d/3d rel, prefer flat/mild); **09-04 laggard-shield** (deep laggard + hawkish macro shock → laggards can be relatively shielded; score laggard once, not in both S2/S4); **08-27** (1w/1m laggard → forbid up on non-holdings AHR); **08-18** (cap S1 at 0/+1, don't use GEV/ETN as cushion); **08-11/08-12 supply-shock cap** (verify live oil). DO-INSTEAD: when score fights tape, cut conviction / prefer flat/mild.

## XLI near-session environment (not an SPX call)

### 1. Shared macro as it hits Industrials — S0 = −1
This is a **live, escalating geopolitical/oil supply-shock day** for a cyclical sector — the 09-08 shock has escalated, not faded.

- **Oil has breached $100** (WTI above $100, first time since July) on continued Middle East conflict. Reuters (09-08): "Oil prices rise to six-week highs on worsening Middle East conflict." TradingKey (09-09): "Oil Breaches $100." This is the **08-11/08-12 trigger**: a live Hormuz/oil supply shock, now escalated. Do **not** call oil flat. For XLI, the oil spike is a **cost/stagflation headwind** for transports/manufacturers.
- **Futures muted/down pre-market.** "Wall St futures muted as oil tops $100" and "US Stock Index Futures Down Pre-Market." This is a risk-off overlay, not a bounce. 08-21's ES/NQ ≥ +0.3% reversal gate is **off**.
- **CPI due this week** (key inflation data) — the binary. Oil >$100 adds an inflation impulse that supports the hawkish side of the Fed path.
- **Rates elevated, 30Y in stress zone.** DGS10 ~4.77 / DGS30 ~5.25 / DFII10 ~2.42 (prior-close). 5-day 10Y–SPX corr strongly negative. Real yields elevated. This is a duration/cyclical drag.
- **Fed hike odds contested.** Warsh hawkish (September hike risk), Waller muddied the outlook (09-04). Do not one-way score hawkish, but the oil spike supports the hawkish side.
- **XLI tape is a deep medium-term laggard.** 1d rel −0.64%, 3d rel +0.21%, 1w rel −0.22%, 1m rel −5.19%.

**S0 = −1, regime risk_off.** Not −2: VIX is not a panic print (contango), credit is tight, no hard-data miss. Not 0: oil is confirmed up >$100 on a live, escalating supply shock, futures are down pre-market. Oil counted **once here**, not again in S1.

### 2. Spine + secondary — S1 = 0 (capped)
**No fresh same-morning industrials print in hand.** August ISM manufacturing already printed **09-01**: PMI **54.6** (expansion, 8th month) vs July 55.6; new orders **53.7** (−3.0 pts). Still **expansion** — **not** an ISM-contraction HIT, but **slowing**. ISM Services printed **09-03** (55.4). Both in the tape. 08-18/08-27: **cap S1 at 0/+1**; +2 is forbidden without same-morning confirmation.

- **Grid / AI power — HIT, carried.** GEV ~$176B RPO / 116 GW gas book, Q2 orders +88% organic. MAP HEAT confirms Electrical Equipment dir=up (VRT +9% week, HUBB confirms grid spend), E&C dir=up (PWR/FIX data center/grid demand), Building Products dir=up (Carrier raise, JCI leads). This is a **live AI-power bid** within industrials. But 08-18: **not** a downside cushion and **not** a same-session raise on an oil-shock day — GEV/ETN can still roll.
- **Aerospace & defense — MIXED.** Boeing–SPEEA talks resumed (constructive, not a strike). Defense backlog intact. The Iran/Houthi escalation is a defense-order narrative but defense names have been volatile on this conflict. Do **not** cancel ISM (expansion) with one award.
- **Freight — MIXED.** Cass trucking still soft; rail carloads +5.5% y/y. Oil >$100 is a **cost headwind** for airlines/trucking (MAP HEAT Airlines "oil drop" read is **stale** — oil is breaching $100 today, not dropping).
- **Construction slowdown — HIT, carried.** Manufacturing construction off the 2025 peak; AI/nonres is the offset.

Net: carried ISM expansion (slowing) + live AI-power bid (VRT/HUBB/PWR/FIX) vs construction drag + mixed freight + **oil-cost overlay** + no fresh same-morning confirmation. **S1 = 0** (carried positives are in the tape; the AI-power bid is real but does not justify +2 on an oil-shock day).

### 3. Breadth — S2 = −1
XLI is a **deep medium-term laggard**. Channel 1 through 09-09: 1d rel **−0.64%**, 3d **+0.21%**, 1w **−0.22%**, 1m **−5.19%**. Seeking Alpha (09-04): "Industrial stocks face broader weakness as bear-market breadth hits 2025 high — 45% of stocks are in bear markets despite XLI staying elevated." This is a **breadth failure** — the ETF is carried by large-cap AI-power/defense names (GEV, ETN, CAT) while the majority of industrial names are in downtrends. Score the lag **once** here (09-04: do not double-count in S2 and S4).

### 4. Flows — S3 = 0
Checked, nothing material returned on XLI flows this morning. Rotation has been out of industrials into tech/AI-power. Not a crowded long (1m rel −5.19%). **S3 = 0.**

### 5. ETF tape (confirmation only) — S4 = −1
Channel 1 through 09-09: 1d rel **−0.64%** (negative), 3d **+0.21%**, 1w **−0.22%**, 1m **−5.19%**. Decisive medium-term lag. 1d is negative today (XLI −1.04% vs SPY −0.39%). Confirmation of underperformance on the oil-shock day, not an independent second thesis. Score the lag **once** (09-04).

### 6. Catalysts / calendar
- **Oil >$100 on escalating Middle East conflict** — dominant macro driver (live supply shock).
- **CPI due this week** — the binary.
- **Fed hike odds contested** — Warsh hawkish, Waller muddied.
- **MAP HEAT**: AI-power bid live (VRT/HUBB/PWR/FIX up), but Airlines "oil drop" read is stale (oil is breaching $100).
- Prior session (09-08) was the oil-shock day that hit XLI −0.48% / rel +0.06%.

### Self-audit
- Lens: cyclical; rates only in S0, not re-counted in S1.
- Band: **mild**, not notable (VIX not panic, credit tight, no hard-data miss; but oil >$100 is a genuine risk-off overlay).
- Skew: GEV/BA do not drive the ETF call.
- Same-shock: oil shock counted once in S0.
- 08-27: 1w/1m laggard + non-holdings AHR → forbid up. Applied.
- 09-04 laggard-shield: deep laggard + hawkish macro shock → laggards can be relatively shielded. But today the shock is an **oil supply shock** (cost headwind for industrials), not a pure hawkish-Fed shock where laggards are shielded. The 09-04 shield applied to a hot-jobs/Fed-repricing shock; an oil-cost shock hits transports/manufacturers directly. So the shield is weaker today.
- 09-08 reconcile: narrative and pipeline must agree. I'll emit one consistent call.

**Divergence:** Leading factors (S0 −1 oil shock, S2/S4 lag) point down, but the 3d rel is +0.21% (mild positive) and the AI-power bid (VRT/HUBB/PWR/FIX) is live. This is a mild divergence — the tape is not fully confirming a strong down move. Per DO-INSTEAD, cut conviction / prefer flat/mild.

**Final call:** Down/mild. The oil >$100 supply shock is a genuine cost/stagflation headwind for a cyclical laggard, and the 1d tape is already negative (−0.64% rel). But the AI-power bid and 3d positive rel cap magnitude at mild, and the deep-laggard shield (09-04) suggests XLI may not underperform as much as a pure cyclical read implies. Down/mild is the honest call.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: 0
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -1
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: risk_off
TOTAL_SCORE: -2.7
PREDICTED_DIRECTION: down
PREDICTED_MAGNITUDE_BAND: mild
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-off tape / flight to safety|HIT|0.8|2026-09-09|Oil >$100 on escalating Middle East conflict; futures down pre-market
Real yields rising|HIT|0.7|2026-09-09|30Y in stress zone ~5.25%, real yields elevated, 5-day 10Y-SPX corr strongly negative
Sector breadth failure (ETF up, names flat)|HIT|0.7|2026-09-09|45% of industrial names in bear markets despite XLI carried by large-cap AI-power/defense
Grid / electrical equipment backlog (AI power)|HIT|0.7|2026-09-09|GEV $176B RPO; MAP HEAT VRT/HUBB/PWR/FIX bid on AI power/grid demand
Construction slowdown|HIT|0.6|2026-09-09|Manufacturing construction off 2025 peak; AI/nonres is the offset
Sector rotation out of industrials|HIT|0.6|2026-09-09|1m rel -5.19% deep laggard; rotation into tech/AI-power
ISM manufacturing / new orders expansion|HIT|0.6|2026-09-01|Aug ISM 54.6 expansion (8th month) but slowing; new orders 53.7 (-3.0 pts)
Aerospace & defense order / budget upside|PARTIAL|0.5|2026-09-09|Boeing-SPEEA talks constructive; defense backlog intact but names volatile on conflict
HIT_GRID_END

HORIZON_3D: down/mild
HORIZON_1W: down/mild
HORIZON_2W: flat/mild
HORIZON_1M: flat/mild

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -4.0, 'divergence_flagged': False, 'total_score': -4.05, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.55, 'regime': 'risk_off', 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': 0.26, 'w1': 1.06}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
