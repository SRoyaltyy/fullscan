# Sector Prediction — Consumer Cyclical — 2026-11-10

- news_mode: **on**
- ETF: **XLY**
- rubric: `00_grounding/sectors/consumer_cyclical.md`
- predicted_direction: **up**
- predicted_magnitude_band: **mild**
- total_score: **6.369** (mult 1.0)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **0.148** (ES +0.00%, ER2 +0.08%, NQ -0.37%, PM:XLY +0.06%) · index_carry **1.721** (general 6.883) · llm_overlay **4.5** (raw 4.5)

## Channel 1 sector ETF tape

```
ETF XLY vs SPY (yfinance, through 2026-11-09):
  1d: XLY +2.92% | SPY +2.37% | rel +0.55%
  3d: XLY -1.08% | SPY -1.84% | rel +0.76%
  1w: XLY +2.41% | SPY +0.08% | rel +2.33%
  1m: XLY +7.23% | SPY -3.32% | rel +10.55%
```

MEMORY_CONFIRM: Consumer Cyclical scope — checked open experiments (none apply to XLY today); reviewed missing-factor risks from recent losses (tape-vs-score conflicts, credit/claims misreads); confirmed no double-counting of confidence/claims as separate from employment support; S0 macro mapped to discretionary (oil down, risk-on futures, but inflation/AI-sell-off overhang); S1 spine and secondary factors reconciled against live data; S2 breadth concentration caveat (AMZN+TSLA ~40%) flagged; S3 outflows vs strong tape divergence noted; S4 tape used as confirmation only. No prior sector logs — first run.

# Consumer Cyclical (XLY) — Near-Session Environment, 2026-11-10

## Channel 2 coverage summary

**Shared macro regime as it hits this sector.** The pre-fetched tape is modestly risk-on (S&P futures +0.20%, Nasdaq +0.41%), but the news layer flags a live AI-led correction narrative and a hot-inflation/Fed-hike-overhang that compresses multiples for growth-heavy discretionary names. WTI is down 1.59% and RBOB gasoline down 0.54%, a direct tailwind for discretionary spending power. USD is flat. The regime is mixed — supportive at the margin via lower energy costs and a green futures tape, but capped by rate/AI duration risk that disproportionately hits XLY's top holdings.

**Sector spine factors.**
- Retail sales/card spend: September retail sales were strong (+0.8% MoM, +6.3% YoY), and Redbook weekly sales remain robust (~10.4% growth). The next official retail sales print is 2026-11-17 — not a session catalyst today, but the underlying trend is positive.
- Employment/wage support: Initial jobless claims fell to 189,000, matching a 57-year low. This is a hard positive for discretionary spending capacity, though some commentary flags weak September sentiment and holiday-quarter exposure for discretionary retailers.
- Credit tightening/delinquency rise: Multiple November 2026 sources report rising credit card delinquencies across states and broadening household credit strain (Urban Institute, MerchantSeys). This is a genuine negative offset and a key risk to the subprime-consumer-facing portion of the sector.

**Secondary factors.**
- Consumer confidence jump: University of Michigan preliminary November sentiment rose to 51.0 from 48.0, beating consensus of 48.0 — a fresh, material positive.
- Auto SAAR: October SAAR expected ~15.6–16.1M, essentially flat to slightly up, with healthy dealer inventory dynamics. Mild positive.
- Travel/hotel RevPAR beat: Marriott raised 2026 RevPAR growth guidance to 4.5%–5.0% after Q3 RevPAR +6.5%, citing resilient travel demand. Positive for the travel/leisure sleeve.
- Gasoline spike: Not active — oil is down, which is a tailwind, not a crush.
- Rotation: XLY's 1-week relative performance (+2.33% vs SPY) and 1-month (+10.55% vs SPY) indicate the sector has been a rotation beneficiary, though recent ETF outflows suggest some profit-taking.

**Breadth/leadership inside the sector.** XLY is structurally mega-cap heavy: AMZN ~21.3% and TSLA ~18.8%, together ~40% of the fund. The 1-month relative outperformance is large enough that breadth likely expanded beyond the top two, but the concentration caveat remains critical: a single mega-cap shock can dominate the ETF outcome, so breadth reads are lower-confidence.

**Flows/positioning.** XLY saw a net outflow of $235.4M on Nov 5 and roughly −$243M over the past month. The strong tape plus outflows creates a divergence: price momentum is positive, but positioning is not accumulating. Crowding risk is moderate given the 1-month relative performance extreme (+10.55%), which raises mean-reversion risk if the AI/rate narrative intensifies.

**Earnings/guidance or policy catalysts.** No same-session mega-cap earnings for XLY's top holdings. ABNB received a Mizuho top-pick upgrade/PT raise to $245 — a mild positive sector catalyst. The dominant cross-sector catalysts are macro: AI sell-off risk, hot inflation/Fed path, and Nvidia-related export/legal risk that transmits to the growth-heavy sleeve of XLY.

## Divergence and self-audit

The leading factor sum (S0 + S1) is positive (+3), and the tape confirmation (S4) is positive (+1), so direction is not in conflict. However, S3 flows are negative (−1), creating a factors-vs-positioning divergence: trust the fundamental factors over the flow signal per method, but flag the outflows and crowding risk as a confidence dampener. The single-ticker (AMZN/TSLA) driver risk is explicitly acknowledged and does not drive the sector call — the score reflects broad consumer health plus the concentrated-ETF caveat.

## HIT_GRID

HIT_GRID_BEGIN
label|status|confidence|date|url
Retail sales / card spend upside|HIT|0.75|2026-11-10|https://www.fxblue.com/calendar/retail-sales-mom-usd
Employment / wage support for discretionary|HIT|0.80|2026-11-10|https://finance.yahoo.com/news/america-just-matched-57-year-employment-record-why-workers-still-worried-140000000.html
Consumer confidence jump|HIT|0.70|2026-11-10|https://www.sca.isr.umich.edu/
Credit tightening / delinquency rise|HIT|0.65|2026-11-10|https://tradersunion.com/news/financial-news/show/urban-institute-analysis-shows-credit-card-and-student-loan-delinquencies-strain-us-households/
Auto SAAR / dealer inventory healthy|HIT|0.60|2026-11-10|https://www.coxautoinc.com/insights/cox-automotive-forecast-october-2026/
Travel / hotel RevPAR beat|HIT|0.70|2026-11-10|https://www.reuters.com/business/marriott-lifts-annual-room-revenue-growth-forecast-resilient-travel-demand-2026-11-02/
Sector breadth failure (ETF up, names flat)|NOT_MET|0.55|2026-11-10|https://www.ainvest.com/news/xly-just-fired-rare-panic-signal-200-day-isnt-bottom-call-2026-10-29/
Sector ETF outflow / volume dry-up|HIT|0.60|2026-11-10|https://au.finance.yahoo.com/news/state-street-consumer-discretionary-spdr-xly-sold-amazon-leads-nov-5-2026-11-09/
Crowded long (extreme relative performance + valuation)|PARTIAL|0.50|2026-11-10|https://www.etfpro.ai/xly
Risk-on tape / equity beta expansion|PARTIAL|0.55|2026-11-10|Channel 1 futures tape
Gasoline spike crushing discretionary|NOT_MET|0.80|2026-11-10|Channel 1 WTI/gasoline tape
Sector rotation into discretionary|HIT|0.60|2026-11-10|Channel 1 XLY vs SPY relative returns
HIT_GRID_END

## SECTOR_SCORES

SECTOR_SCORES_BEGIN
SECTOR: Consumer Cyclical
ETF: XLY
REGIME: mixed
MULTIPLIER: 1.0
S0_SHARED_MACRO: 1
S1_SECTOR_FACTORS: 2
S2_BREADTH: 1
S3_FLOWS_POSITIONING: -1
S4_ETF_TAPE: 1
DIVERGENCE_NOTE: Strong fundamentals (confidence, claims, retail trend, travel) and positive tape vs. negative ETF outflows and rising delinquencies; trust factors over flows but cut confidence for the S3 divergence and mega-cap concentration risk.
CONFIDENCE: 0.60
HORIZON_3D: up:mild:0.60
HORIZON_1W: up:mild:0.55
HORIZON_2W: up:mild:0.50
HORIZON_1M: up:mild:0.45
SECTOR_SCORES_END


---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 2.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': -1.0, 'S4_ETF_TAPE': 1.0}, 'multiplier': 1.0, 'leading_sum': 10.0, 'divergence_flagged': False, 'total_score': 6.369, 'predicted_direction': 'up', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.755, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.0246, 'score': 0.148, 'legs': [{'leg': 'ES', 'pct': 0.0, 'w': 0.8}, {'leg': 'ER2', 'pct': 0.08, 'w': 0.2}, {'leg': 'NQ', 'pct': -0.37, 'w': 0.2}, {'leg': 'PM:XLY', 'pct': 0.06, 'w': 0.7}]}, 'overlay_score': 4.5, 'overlay_raw': 4.5, 'index_carry': 1.721, 'general_total': 6.883, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 0.5, 'S3_FLOWS_POSITIONING': 1.0, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.6}
```
