# Sector Prediction — Real Estate — 2026-10-13

- news_mode: **on**
- ETF: **XLRE**
- rubric: `00_grounding/sectors/real_estate.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **1.462** (mult 0.85)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **3.154** (ES -0.09%, ZN -0.03%, PM:XLRE +0.78%) · index_carry **-1.692** (general -6.767) · llm_overlay **0.0** (raw 0.0)

## Channel 1 sector ETF tape

```
ETF XLRE vs SPY (yfinance, through 2026-10-12):
  1d: XLRE +0.75% | SPY -0.06% | rel +0.81%
  3d: XLRE +3.33% | SPY +0.11% | rel +3.21%
  1w: XLRE +3.07% | SPY +0.42% | rel +2.65%
  1m: XLRE -2.64% | SPY +2.06% | rel -4.70%
```

MEMORY_CONFIRM: Reviewed sector Real Estate (XLRE) memory only. Last graded: 2026-10-08 down/mild pending; 2026-10-02 flat/flat vs +0.295% (dir MISS — stale multi-horizon lag illegally imported into S4; 10-02 lesson: **S4 is a same-day tape read only**, lag belongs in HORIZON columns). 2026-10-01 down/mild dir+mag HIT. Rolling dir=0.5 mag=0.4 (n=10). Open experiment applies: 10-02 loss → when score sign conflicts with tape/breadth, cut conviction, prefer flat/mild. 09-25 binds: rate LEVEL once in S0, IMPULSE only on a fresh 1d step. 09-14: XLRE on the PM board today (+0.78%), so the absence rule doesn't bind. Channel 2: checked — nothing material beyond the News Judge set (hawkish Fed minutes, Hormuz oil escalation, yield surge/mortgage hedging, CPI/Powell pending this week).

---

## Real Estate (XLRE) — 2026-10-13

### Channel 1 (used as given)

- **Relative tape (through 10-12):** 1d rel **+0.81%**, 3d **+3.21%**, 1w **+2.65%**, 1m **−4.70%**. XLRE has had a sharp 3-session bounce — the 1m lag is a HORIZON descriptor only.
- **PM board:** XLRE **+0.78%** — best on the board with XLP (+0.98%) inside a red tech tape (XLK −0.97%, NQ=F −0.73%). This is a classic **defensive-relative bid**, exactly the REIT polarity inside risk-off.
- **Rates:** DGS10 5.22 (1d **−0.06**), DGS30 5.60 (1d **−0.07**), DFII10 2.87 (1d **−0.05**) — **fresh 1d easing impulse** (small, but it's a live falling step, not just a level). 1m still up ~+0.40 = stress-zone LEVEL. Note rates data is dated 10-08 (Columbus-week gap); live curve unverified independently today — flag.
- **Tape:** ES −0.09% (inside ±0.5%), NQ −0.73% (outside down — tech-led, not REIT-led). VIX 15.42, contango. Oil CL=F −1.25% / BZ=F −1.38% — **offered despite the Hormuz headline**; the News Judge's fresh kinetic increment is partially not confirmed in live futures (stale board agreement), so score once, damped.
- **Map Heat:** all sub-sector captains flat/low — no single property type dominates; EQIX/DLR/WELL must not define the call. ✓

### Channel 2 synthesis

1. **Macro → REITs:** Two live fights: (a) hawkish Fed minutes (16/18 wanting another hike) + CPI/Powell this week = unresolved negative binary skew; (b) *live* long-end easing at the margin + mortgage-hedging yield surge being absorbed. Real yields 1m still rising = duration headwind persists, but the 1d impulse is the freshest incremental input and it's down.
2. **Spine:** LEVEL scored once (S0, negative skew, not full −1 — long end actually eased 1d); IMPULSE = small relief, scored once in S1. No restack of the 1m lag anywhere.
3. **Secondary:** office/mortgage credit stress (BXMT $1B office loan) persists but is stale; data-center split; no fresh open-board REIT-specific catalyst.
4. **Breadth/leadership:** XLRE leading the board PM — low-beta defensive leadership inside a red tech tape; +0.81% 1d cushion satisfies the ≥ +0.4% cushion lesson (scored as positive S4/direction input, magnitude-capped by the red NQ tape).
5. **Flows:** no fresh ETF flow data; 3d +3.21% bounce suggests short-covering/rotation into defensives, but that's now a partially paid move — mild extension risk into CPI.
6. **Catalysts:** CPI + Powell this week = two-sided calendar → cap magnitude, no notable either way.

**Divergence flag:** leading factors mildly positive (cushion + defensive bid + easing 1d impulse) vs the hawkish binary calendar and red NQ. Per the experiment rule: cut conviction, prefer flat with up-skew rather than forcing up.

HIT_GRID_BEGIN
label|status|confidence|date|url
Risk-off tape / flight to safety|HIT|0.6|2026-10-13|
Real yields falling|PARTIAL (1d impulse only)|0.4|2026-10-08|
Real yields rising|PARTIAL (1m level)|0.5|2026-10-08|
Rates falling / REIT duration relief|PARTIAL (fresh 1d step)|0.4|2026-10-13|
Large-cap leadership inside sector|CONTEXT|0.4|2026-10-13|
Low-beta leadership inside sector|HIT (XLRE best PM board)|0.6|2026-10-13|
Sector ETF inflow / relative volume spike|PARTIAL (3d bounce)|0.4|2026-10-12|
Rates rising / REIT selloff|OFFSET (calendar skew)|0.4|2026-10-13|
Office vacancy / mark-to-market stress|PARTIAL (stale BXMT/office)|0.4|2026-10-08|
Sector rotation out of real estate|ABSENT (1w rel positive)|0.5|2026-10-12|
HIT_GRID_END

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -0.5
S1_SECTOR_FACTORS: 0.0
S2_BREADTH: 0.5
S3_FLOWS_POSITIONING: 0.0
S4_ETF_TAPE: 0.5
MULTIPLIER: 0.85
CONFIDENCE: 0.45
REGIME: mixed
PREDICTED_DIRECTION: flat
PREDICTED_MAGNITUDE_BAND: flat
HORIZON_3D: up-skew, relative laggard fading into CPI week
HORIZON_1W: mixed; CPI binary dominates, 1m real-yield level still negative
HORIZON_2W: capped both ways by Fed path uncertainty
HORIZON_1M: down-skew; 1m rel −4.70% + real yields +0.41 not repaired
DIVERGENCE_FLAGGED: true
SECTOR_SCORES_END

Self-audit: lens = sector ETF environment, not SPX/stock picker; band flat honors the mild-cap discipline into an unsigned CPI binary; skew symmetric (hawkish calendar vs defensive cushion) with up-skew noted; the Hormuz oil shock scored once in S0 and damped by live oil offered; no single ticker (WELL/EQIX/BXMT) drives the XLRE call — sub-sector heat is uniformly flat/low-confidence.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -0.5, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': 0.5, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.5}, 'multiplier': 0.85, 'leading_sum': 0.0, 'divergence_flagged': False, 'total_score': 1.462, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.55, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.5257, 'score': 3.154, 'legs': [{'leg': 'ES', 'pct': -0.09, 'w': 0.5}, {'leg': 'ZN', 'pct': -0.03, 'w': 0.75}, {'leg': 'PM:XLRE', 'pct': 0.78, 'w': 0.7}]}, 'overlay_score': 0.0, 'overlay_raw': 0.0, 'index_carry': -1.692, 'general_total': -6.767, 'skill_multipliers': {'S0_SHARED_MACRO': 1.25, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.25, 'S3_FLOWS_POSITIONING': 1.25, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.45, 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': -0.56, 'w1': -2.06}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
