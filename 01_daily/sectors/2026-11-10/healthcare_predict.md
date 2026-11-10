# Sector Prediction — Healthcare — 2026-11-10

- news_mode: **on**
- ETF: **XLV**
- rubric: `00_grounding/sectors/healthcare.md`
- predicted_direction: **up**
- predicted_magnitude_band: **mild**
- total_score: **6.41** (mult 0.8)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **2.489** (ES +0.01%, PM:XLV +0.59%) · index_carry **1.721** (general 6.883) · llm_overlay **2.2** (raw 2.2)

## Channel 1 sector ETF tape

```
ETF XLV vs SPY (yfinance, through 2026-11-09):
  1d: XLV +1.91% | SPY +2.37% | rel -0.46%
  3d: XLV -0.70% | SPY -1.84% | rel +1.14%
  1w: XLV +2.02% | SPY +0.08% | rel +1.94%
  1m: XLV +10.26% | SPY -3.32% | rel +13.58%
```

MEMORY_CONFIRM: Sector:Healthcare scoped. Relevant actives applied: (1) `a-sector-has-led-spy-on-a-defensive-relative-strength-rotati` — XLV carries 3d/1w/1m leadership (+1.14/+1.94/+13.58 rel) but 1d rel is -0.46%, a reversal tell to respect on a post-selloff rebound day; (2) `a-healthcare-xlv-call-after-a-strong-multi-week-relative-run` — strong carried RS does not license an absolute up call without a fresh same-day positive catalyst; (3) `a-defensive-relative-strength-healthcare-call-is-already-cor` — policy audit before setting S1 (done: IRA codification 11-06 and utilization pressure are slow-burn overhangs, not fresh same-day shocks); (4) scope do-instead `sector_healthcare` loss pattern — when score sign conflicts with tape, cut conviction, prefer flat/mild. No open utilities experiments apply to this scope. No factor double-count: MA rate final (+2.48%, April) and IRA negotiation discounts (in force from 2026) are regime context, not fresh 11-10 catalysts.

## Analysis

Channel 1 is exact and trusted. XLV premarket +0.09% vs SPY-futures ES +0.09% / NQ -0.2%; VIX 20.05; 10Y futures flat; USD flat. The tape is a mild post-AI-selloff rebound, not a clean tech-led melt-up (NQ actually lags ES), which softens the pure "defensive give-back" read but does not erase it.

Channel 2 coverage, category by category:
- Shared macro as it hits healthcare: elevated VIX (20) and a green futures tape pull modestly against low-beta defensives on a flight-from-safety unwind, but healthcare is not a pure bond proxy here — it has an idiosyncratic rotation bid. Mild negative S0, not a regime break.
- Sector spine: no fresh CMS/MA shock (2027 final rate +2.48% locked since April; no new cut) — spine neutral on rates. Biotech risk-on is live: XBI hit a 52-week high 11-01 and rebounded +2.57%/+3.08% after the 11-05 dip, with XLV +1.91% on 11-09 — XBI leadership is a genuine positive breadth factor, not one-ticker noise. Drug-pricing: CMS codified the IRA negotiation framework 11-06, a slow-burn pharma overhang rather than a same-day crackdown. Utilization: insurers still flag elevated medical cost/utilization into 2027 (limited-network MA shifts), a persistent managed-care drag but already known. M&A active (Thermo/Clario $8.9B, McKesson/Option Care $5.8B) — supportive but not a one-day sector driver.
- Breadth/leadership: 11-09 was broad (XLV +1.91%, XBI +3.08%), so breadth expanded on the rebound; premarket 11-10 is thin at +0.09%, i.e. consolidation rather than expansion.
- Flows/positioning: rotation into healthcare is real (+10.26% 1m, ~32% since Jan 2025, ~9pp ahead of SPY) and money-retention during the AI drawdown was strong — but that same run makes the book crowded-long after a strong multi-week move.
- Tape confirmation: S4 is negative (-0.46% 1d rel) even though the 3d/1w/1m relative record is outstanding. This is the exact carried-RS reversal signature flagged in memory.

Divergence: leading factor sum is modestly positive (+0.5) while tape confirmation is negative (-0.5) — flagged below; factors are trusted over tape, but the conflict plus the terrible magnitude record (0.321 last 30) and dir record (0.4 last 10) argues for a flat/mild call at low confidence, not an up call. Single-ticker items (BDX cautious FY27 guide, BSX outlook cut to negative, CAH post-earnings drop) are deliberately not allowed to drive the XLV sector score.

HIT_GRID_BEGIN
label|status|confidence|date|url
Sector rotation into healthcare|HIT|0.70|2026-11-10|https://www.odaily.news
Biotech risk-on / XBI leadership|HIT|0.65|2026-11-10|https://zrsa.zacks.com
Sector breadth expansion (% names up)|PARTIAL|0.50|2026-11-09|https://www.paraborsa.net
Crowded long (extreme relative performance + valuation)|HIT|0.55|2026-11-07|https://timesbrasil.com.br
Drug pricing crackdown / IRA expansion risk|WATCH|0.45|2026-11-06|https://www.fiercepharma.com
Utilization spike hurting insurers|WATCH|0.50|2026-10-03|https://www.htfmarketintelligence.com
CMS / Medicare Advantage rate upside|CHECKED, NOTHING MATERIAL|0.40|2026-04-06|https://www.cms.gov
Risk-on tape / equity beta expansion|WATCH|0.40|2026-11-10|https://www.paraborsa.net
HIT_GRID_END

SECTOR_SCORES_BEGIN
SECTOR: Healthcare
ETF: XLV
REGIME: mixed
MULTIPLIER: 0.8
S0_SHARED_MACRO: -0.5
S1_SECTOR_FACTORS: 0.5
S2_BREADTH: 0.5
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -0.5
DIVERGENCE_NOTE: Leading S0-S3 sum mildly positive (+0.5) vs negative 1d tape confirmation (-0.46 rel); factors trusted but conflict + poor magnitude record caps call at flat/mild, low confidence.
CONFIDENCE: 0.4
HORIZON_3D: flat:mild:0.35
HORIZON_1W: up:mild:0.40
HORIZON_2W: up:mild:0.40
HORIZON_1M: up:mild:0.45
SECTOR_SCORES_END


---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -0.5, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 0.5, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -0.5}, 'multiplier': 0.8, 'leading_sum': 1.5, 'divergence_flagged': False, 'total_score': 6.41, 'predicted_direction': 'up', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.756, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.4148, 'score': 2.489, 'legs': [{'leg': 'ES', 'pct': 0.01, 'w': 0.6}, {'leg': 'PM:XLV', 'pct': 0.59, 'w': 0.7}]}, 'overlay_score': 2.2, 'overlay_raw': 2.2, 'index_carry': 1.721, 'general_total': 6.883, 'skill_multipliers': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.25, 'S3_FLOWS_POSITIONING': 1.25, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.4}
```
