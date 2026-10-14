# Sector Prediction — Basic Materials — 2026-10-14

- news_mode: **on**
- ETF: **XLB**
- rubric: `00_grounding/sectors/basic_materials.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **7.7** (mult 0.85)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **4.924** (ES +0.18%, HG +0.66%, GC +0.90%, DX -0.02%, PM:XLB +1.00%) · index_carry **-0.411** (general -1.645) · llm_overlay **3.188** (raw 3.188)

## Channel 1 sector ETF tape

```
ETF XLB vs SPY (yfinance, through 2026-10-13):
  1d: XLB +0.57% | SPY -0.01% | rel +0.58%
  3d: XLB +0.91% | SPY +0.53% | rel +0.39%
  1w: XLB -0.02% | SPY -0.14% | rel +0.12%
  1m: XLB -1.07% | SPY +2.51% | rel -3.58%
```

MEMORY_CONFIRM: Sector **Basic Materials / XLB only**. Rolling last-10 dir=0.3 mag=0.4 (n=10); last-30 dir=0.4 mag=0.467 (n=30). Last graded **2026-10-08 down/mild vs XLB +0.592% (dir MISS)** — recurring failure is signing down on stale levels while the live tape resolves up. Scope DO-INSTEAD: **cut conviction when sign fights sector tape; prefer flat/mild on conflict** (10-02, 10-08 losses). Today the sign **agrees** with the sector tape. Active rules checked: **10-02 unprinted-binary rule partially ON** — CPI tonight is the dominant unsigned binary → cap magnitude at mild, no confirmed-up; on green pre-open tape with an unprinted binary, do not emit signed down. **8/25 / 8/27 S4 caps ON** (1d rel +0.58% is real but PM +1.00% is the leading signal, not confirmation of severity). **Hormuz/oil co-move lesson OFF today** — oil is *offered* (WTI −1.59%), no kinetic escalation increment; geopolitics easing per News Judge #7. **China/gold split** — gold flush (GC=F −0.34%) is a sleeve headwind only; it does not cancel the industrial spine.

## Analysis — XLB, session of 2026-10-14 (Wednesday cash, pre-CPI)

**The live tape has flipped in XLB's favor.** Channel 1 rel: 1d **+0.58%**, 3d **+0.39%**, 1w **+0.12%** — three consecutive horizons positive for the first time in this month-long lag, and **XLB is the #1 premarket sector at +1.00%** (vs ES +0.18%). This is exactly the setup where the 10-08/10-13 cards lost: signing down against a resolving sector tape. The stale 1m rel (−3.58%) is T-1 leftover — scored once at most, not three times (10-02 lesson).

**Spine (S1):** Copper firm (+0.66%, LME ~$14.7k, **229.7k mt stocks, down 2.1% in 30 days — 18 weeks of draw**), aluminum +1.10%, platinum/palladium green. Iron ore/HRC/Newcastle mildly soft — the complex is *firm-not-surging*, a level not a squeeze. China September **exports/imports beat expectations** (News Judge #8) — a genuine industrial-demand positive, distinct from the stale PMI level. Gold/silver sleeve is split: silver +1.96% but the **gold −$100 flush on hawkish Fed comments is the live increment** (AEM down) — sleeve negative, not a book bid.

**Macro (S0):** Mildly green index tape (ES +0.18%, NQ −0.06%, Europe +0.6%), Shanghai −0.89% red. Real yields elevated (DFII10 2.91, +0.36 on month) but 10Y *steading* pre-CPI, 5-day 10Y–SPX corr +0.69 (yields falling = stocks rising regime lately). USD firm on month (+2.59%) but flat today. Dominant binary: **CPI tonight, consensus core +0.3 accelerating on energy — hawkish skew** → this is the cap on severity, not a signed pre-open headwind.

**Flows/breadth (S2/S3):** XLB PM +1.00% leads the sector board; rotation into materials/commodity-adjacent names is live (XLE +0.45%, XLF +0.41%). MAP HEAT nested captains all failed to land cards — no breadth confirmation, so S2 stays modest. No fresh flow evidence found.

Resolution: leading factors net positive and **aligned with** the tape (unlike 10-08/10-13) → **up/mild, low confidence**, capped by the CPI binary and 8/27 conviction rules. Regime mixed.

HIT_GRID_BEGIN
```
Risk-on tape / equity beta expansion|PARTIAL|0.55|2026-10-14|ES +0.18%, Europe +0.6%, NQ flat
Risk-off tape / flight to safety|MISS|0.6|2026-10-14|VIX 15.2, contango 0.846
Real yields rising|PARTIAL|0.5|2026-10-09|DFII10 2.91 +0.36 1m, but 10Y steadying
Real yields falling|MISS|0.6|2026-10-14|channel1
USD strengthening|PARTIAL|0.5|2026-10-14|DXY +2.59% 1m, flat 1d
Sector breadth expansion (% names up)|PARTIAL|0.5|2026-10-14|XLB #1 PM +1.00%; HEAT captains no cards
Sector breadth failure (ETF up, names flat)|MISS|0.6|2026-10-14|rel 1d/3d/1w all green
Large-cap leadership inside sector|PARTIAL|0.5|2026-10-14|no single-name driver found
Sector ETF inflow / relative volume spike|PARTIAL|0.5|2026-10-14|PM +1.00% leads board; no flow data
Industrial metal price surge (copper/aluminum/iron ore)|PARTIAL|0.6|2026-10-14|Cu +0.66%, Al +1.10%; ore/HRC soft
Gold/silver price surge (monetary metals)|PARTIAL|0.6|2026-10-14|SI +1.96% but gold -$100 flush (GC=F -0.34%)
China PMI / property demand rebound|PARTIAL|0.55|2026-10-01|PMI 50.1 stale level; exports/imports beat fresh
Inventory draw (LME/exchange stocks down)|HIT|0.65|2026-10-13|LME Cu 229.7k mt, -2.1% 30d, 18-wk draw
Supply disruption (mine/export ban)|MISS|0.6|2026-10-14|checked, nothing material
Critical-minerals policy / domestic tariff support|MISS|0.5|2026-10-14|checked, nothing material
Industrial metal price collapse|MISS|0.7|2026-10-14|no collapse; Cu firm
China demand shock / property stress|MISS|0.6|2026-10-14|exports beat, no fresh miss
USD spike vs commodity complex|MISS|0.6|2026-10-14|DXY flat 1d
Margin compression / cost inflation without pricing power|MISS|0.4|2026-10-14|no fresh print
Sector rotation into materials|PARTIAL|0.55|2026-10-14|XLB/XLE/XLF PM green vs XLK -0.26%
```
HIT_GRID_END

SECTOR_SCORES_BEGIN
```
S0_SHARED_MACRO: 0.5
S1_SECTOR_FACTORS: 1
S2_BREADTH: 0.5
S3_FLOWS_POSITIONING: 0.5
S4_ETF_TAPE: 0.5
MULTIPLIER: 0.85
CONFIDENCE: 0.4
REGIME: mixed
HORIZON_3D: up/mild
HORIZON_1W: up/mild
HORIZON_2W: flat/mixed
HORIZON_1M: flat/mixed
```
SECTOR_SCORES_END

Divergence: none — leading factor sum (+2.5) agrees with S4/PM (+0.5; XLB premarket +1.00% leads board). CPI binary tonight and 8/25/8/27 caps hold the band at mild; sign follows the resolving sector tape this time, not the stale 1m hole.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.5, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 0.5, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 0.5}, 'multiplier': 0.85, 'leading_sum': 5.0, 'divergence_flagged': False, 'total_score': 7.7, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.55, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.8206, 'score': 4.924, 'legs': [{'leg': 'ES', 'pct': 0.18, 'w': 0.6}, {'leg': 'HG', 'pct': 0.66, 'w': 0.3}, {'leg': 'GC', 'pct': 0.9, 'w': 0.1}, {'leg': 'DX', 'pct': -0.02, 'w': -0.3}, {'leg': 'PM:XLB', 'pct': 1.0, 'w': 0.7}]}, 'overlay_score': 3.188, 'overlay_raw': 3.188, 'index_carry': -0.411, 'general_total': -1.645, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 0.5, 'S3_FLOWS_POSITIONING': 1.0, 'S4_ETF_TAPE': 0.0}, 'llm_confidence': 0.4, 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': -2.08, 'w1': -4.78}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
