# Sector Prediction — Real Estate — 2026-11-10

- news_mode: **on**
- ETF: **XLRE**
- rubric: `00_grounding/sectors/real_estate.md`
- predicted_direction: **up**
- predicted_magnitude_band: **mild**
- total_score: **7.461** (mult 1.0)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **1.49** (ES +0.02%, ZN -0.03%, PM:XLRE +0.36%) · index_carry **1.721** (general 6.883) · llm_overlay **4.25** (raw 4.25)

## Channel 1 sector ETF tape

```
ETF XLRE vs SPY (yfinance, through 2026-11-09):
  1d: XLRE +1.45% | SPY +2.37% | rel -0.91%
  3d: XLRE -1.06% | SPY -1.84% | rel +0.78%
  1w: XLRE +2.29% | SPY +0.08% | rel +2.21%
  1m: XLRE +7.28% | SPY -3.32% | rel +10.61%
```

MEMORY_CONFIRM: Sector-scoped memory checked. Applying (1) a-long-duration-rate-sensitive-sector-reits-has-been-lagging — CPI was the 08:30 ET binary and is now paid; the curve is flat-to-lower, so S0 is not defaulted to −1; (2) the no-double-count rule — the "rates rising" spine is not stacked in both S0 and S1; (3) DO NOT let Welltower or a single data-center name define the XLRE call — breadth is scored on the 52-name guidance slate, not one ticker; (4) a-rate-sensitive-bond-proxy... "no cushion ≠ headwind" — the negative 1d relative is a beta-day artifact, not a same-day negative signal; (5) sector_real_estate DO-INSTEAD — score sign agrees with the premarket tape, so keep direction but shrink confidence on a modest |score|. No open experiment for sector_real_estate is live.

## Channel 2 coverage

Shared macro as it hits REITs. The operative fact is that the October CPI (released 08:30 ET today, one day early because of the Veterans Day bond-market closure) is behind us, and the long end did not break: 10Y around 4.859%–4.882% (roughly flat, −0.2bp on Tradeweb), 30Y slightly lower, 2Y/5Y marginally higher. That is a flat curve, not a duration shock — but it is also not relief. The structural drag is intact: 10Y TIPS real yield ~2.63%, up ~0.69pp since 2 January, and December hike odds are priced above 80% with Waller explicitly flagging 25bp and a possible 50bp if core runs 0.3% m/m. Oil at $104 but −1.6% on the day is mildly disinflationary at the margin; gold +0.90% and silver +1.96% suggest a residual inflation-hedge bid; USD flat. Equity tape is a low-beta digestion session (ES +0.09%, NQ −0.2%, Russell +0.08%), which is the regime in which REITs are least penalized.

Sector spine. Rates falling / REIT duration relief: neutral, not a HIT — the curve is flat, so no duration tailwind is being delivered. Rates rising / REIT selloff: not triggered intraday. Real yields rising: HIT on the 1m horizon and remains the sector's dominant valuation headwind.

Secondary factors. This is where the card turns constructive and it is broad, not single-name: the Q3 REIT slate shows an 85% guidance-raise rate (44 of 52 raisers, 6 maintained, only 2 cuts) with a perfect slate of increases across eight property sectors — including eight office raises and eight net-lease raises. Industrial: Prologis at 95.6% occupancy, net effective rent growth 38%, market vacancy down to 7.1% and a third consecutive quarterly rent increase — industrial occupancy/rent growth is a clean HIT. Data-center: rents +63% over five years, BTIG calling a multi-decade supercycle on DLR/EQIX, and the AI power-demand headlines (CAT turbines for Anthropic, CEG nuclear deals) reinforce the derivative demand narrative without making any one name the thesis. Office vacancy / mark-to-market stress remains a real structural negative, but it is a slow-burn descriptor rather than a fresh same-day shock, and office guidance raises argue against a fresh leg down today. Refinancing wall / cap-rate expansion: partial risk with 10Y near 4.86% and hike odds rising, but no live cap-rate event in the tape.

Breadth. Genuine expansion inside the sector: the raise rate is spread across residential (UDR, ESS), retail (FRT), industrial, net lease and office — this is not mega-name carry, and it is the single strongest argument against a down call.

Flows/positioning. XLRE has taken +$191M net inflows over the past month (~2.3% of AUM; a second source shows +$141M/+1.8%), institutional holders +44 net, so demand is real. But the offset is crowding: XLRE +7.28% absolute and +10.61% relative to SPY over one month is an extreme relative move, and there was a ~$175M weekly outflow print. Positioning is a support, not a fresh accelerant — hence S3 flat.

Tape. 1d rel −0.91% (XLRE +1.45% vs SPY +2.37%) is exactly the macro map: a big equity-beta day leaves REITs lagging. It is confirmation-only and must not be read as a same-day negative. The fresher, more relevant signals point the other way: 3d rel +0.78%, 1w rel +2.21%, 1m rel +10.61%, and this morning XLRE is +0.36% premarket versus ES +0.09% and SPY futures +0.20% — REITs are third-best of the eleven sector proxies on a modest-risk morning.

Divergence. The weighted factor sum is positive while the 1d relative tape is negative. Per method, trust factors over tape: the 1d lag is a beta artifact of yesterday's +2.37% SPY session, not a fresh REIT-specific shock. The genuine counter-risk is the opposite one — a crowded 1m relative extension into a hawkish December FOMC.

HIT_GRID_BEGIN
label|status|confidence|date|url
Real yields rising|HIT|0.72|2026-11-10|https://th.tokenpost.com
Rates falling / REIT duration relief|NEUTRAL|0.55|2026-11-10|https://sa.marketscreener.com
Rates rising / REIT selloff|MISS|0.60|2026-11-10|https://es.marketscreener.com
Industrial REIT occupancy / rent growth|HIT|0.80|2026-11-10|https://in.investing.com
Data-center REIT demand / rent upside|HIT|0.70|2026-11-10|https://in.investing.com
Office vacancy / mark-to-market stress|PARTIAL|0.60|2026-11-10|https://simplywall.st
Refinancing wall stress|PARTIAL|0.45|2026-11-10|https://247wallst.com
Cap-rate expansion|PARTIAL|0.50|2026-11-10|https://www.edgen.tech
Sector breadth expansion (% names up)|HIT|0.78|2026-11-10|https://seekingalpha.com
Sector ETF inflow / relative volume spike|HIT|0.65|2026-11-10|https://www.etfpro.ai
Crowded long (extreme relative performance + valuation)|PARTIAL|0.62|2026-11-10|https://www.etfpro.ai
Risk-on tape / equity beta expansion|PARTIAL|0.55|2026-11-10|https://ch.zonebourse.com
Sector rotation into REITs|HIT|0.66|2026-11-10|https://www.perplexity.ai
HIT_GRID_END

SECTOR_SCORES_BEGIN
SECTOR: Real Estate
ETF: XLRE
REGIME: mixed
MULTIPLIER: 1
S0_SHARED_MACRO: -0.5
S1_SECTOR_FACTORS: 1
S2_BREADTH: 1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0.5
DIVERGENCE_NOTE: Weighted factor sum positive (+1.5) and agrees with XLRE premarket +0.36% vs ES +0.09%, but 1d rel −0.91% is a beta-day artifact, not a same-day negative; real risk is crowded 1m rel +10.61% into a hawkish December FOMC.
CONFIDENCE: 0.55
HORIZON_3D: up:mild:0.52
HORIZON_1W: flat:mild:0.45
HORIZON_2W: flat:mild:0.42
HORIZON_1M: flat:mild:0.40
SECTOR_SCORES_END


---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -0.5, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.5}, 'multiplier': 1.0, 'leading_sum': 4.0, 'divergence_flagged': False, 'total_score': 7.461, 'predicted_direction': 'up', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.798, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.2482, 'score': 1.49, 'legs': [{'leg': 'ES', 'pct': 0.02, 'w': 0.5}, {'leg': 'ZN', 'pct': -0.03, 'w': 0.75}, {'leg': 'PM:XLRE', 'pct': 0.36, 'w': 0.7}]}, 'overlay_score': 4.25, 'overlay_raw': 4.25, 'index_carry': 1.721, 'general_total': 6.883, 'skill_multipliers': {'S0_SHARED_MACRO': 1.25, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.25, 'S3_FLOWS_POSITIONING': 1.25, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.55}
```
