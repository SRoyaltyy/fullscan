# Sector Prediction — Energy — 2026-11-10

- news_mode: **on**
- ETF: **XLE**
- rubric: `00_grounding/sectors/energy.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-9.084** (mult 1.0)
- regime: risk_off
- divergence_flagged: **False**
- engine: v2 · tape_anchor **-4.805** (CL -1.59%, QA -1.02%, ES -0.00%, PM:XLE -0.84%) · index_carry **1.721** (general 6.883) · llm_overlay **-6.0** (raw -8.75)

## Channel 1 sector ETF tape

```
ETF XLE vs SPY (yfinance, through 2026-11-09):
  1d: XLE +0.52% | SPY +2.37% | rel -1.85%
  3d: XLE -2.49% | SPY -1.84% | rel -0.65%
  1w: XLE -7.90% | SPY +0.08% | rel -7.97%
  1m: XLE -4.82% | SPY -3.32% | rel -1.50%
```

MEMORY_CONFIRM: Energy scope — checked open experiments (none directly applicable today); checked missing-factor risk (geopolitical premium fade was the dominant missing factor in prior up-calls — now correctly reversed); no double-counting of the same Iran shock across multiple HITs; S0 macro vs S1 sector factors reconciled (S1 dominates today, S0 neutral); no single-ticker carry (XOM/CVX concentration noted but the call is sector-wide oil-driven, not a single-name story).

## Energy (XLE) — 2026-11-10

The dominant fact is a sector-specific crude collapse, not a broad macro shock. Brent fell 5.85% Monday to $92.57 and extended below $91 this morning; WTI is near $85, down another 1.6–1.9% on the session. The driver is a systematic unwind of the Iran war-risk premium after Trump signaled further strikes are unnecessary and Qatar-mediated talks progressed. This is day two-to-three of the same de-escalation shock, not a fresh headline, but the incremental barrel print is still decisively negative — WTI is making new lows since late August.

The supply side reinforces the bear case. OPEC+ kept December quotas unchanged, but that is neutral-to-bearish because October OPEC output already rose 1.38 mb/d to 22.87 mb/d, roughly 90% of pre-war levels, led by Saudi Arabia, Kuwait and Iraq. The IEA's 2027 surplus framework remains the medium-term anchor, and the prompt Brent spread has narrowed to ~$2.60 from ~$4, signaling easing physical tightness. Diesel and gasoline cracks are still elevated versus history, but they are easing from records and are a refiner sub-story that must not carry the whole XLE call.

The offset is exhaustion. XLE already fell roughly 12% last week — the worst single-week sector dispersion of 2026 — and Monday actually closed +0.52% with 13 of 19 tracked energy names advancing. That is a tentative washout signal. But today's premarket XLE -0.60% against WTI -1.6% suggests the relief bounce is not extending while the barrel is still offered. The 1d relative tape is -1.85%, 1w -7.97%, and 1m -1.50%, so tape confirmation is squarely negative.

Breadth is the ambiguous channel. Monday's advance was broad enough to avoid a "breadth failure" label, but it was a one-day counter-trend bounce inside a violent downtrend. Flows show a classic crowded-long unwind: +$1.6B of inflows over the prior month into a sector up ~40% YTD, followed by the -12% week and a $171M one-sided liquidation on Nov 2. Positioning is now less crowded than a week ago, which limits further forced-selling magnitude, but it does not create a buy signal while crude is still falling.

Macro is neutral. ES +0.20% / NQ +0.41% is mildly risk-on, but energy is idiosyncratically risk-off. USD is flat-to-slightly weaker, which would normally help commodities, but the war-premium unwind overwhelms that. VIX at 20 is elevated but not panic. Natural gas +4.6% is irrelevant to XLE's oil-major concentration (XOM 24%, CVX 18%, COP 7%).

S1 and S4 align negative, so there is no factor-vs-tape divergence to flag. The call is down, but magnitude discipline matters: Energy's rolling magnitude hit-rate is only 0.367 over 30 and 0.4 over 10, and the sector is already deeply oversold. A -0.60% premarket gap is a mild-band signal, not a notable one. The direction is high-confidence; the magnitude is deliberately capped at mild.

HIT_GRID_BEGIN
label|status|confidence|date|url
Crude price collapse|HIT|0.90|2026-11-10|https://www-preview.morningstar.com/news/dow-jones/202611102803/oil-prices-extend-decline-after-trump-signals-restraint-on-iran-update
Geopolitical supply risk premium|HIT (fade)|0.88|2026-11-10|https://energyscan.engie.com/daily-news/oil-and-gas-slide-as-the-iran-war-premium-fades/
OPEC+ production increase / quota break|HIT|0.82|2026-11-10|https://oilprice.com/Latest-Energy-News/World-News/Saudi-Pipeline-Attacked-as-OPEC-Output-Climbs-to-90-of-Pre-War-Levels.html
Demand destruction (recession/China weak)|PARTIAL|0.55|2026-11-10|https://www.argusmedia.com/en/news/2669612-iea-cuts-2026-demand-forecast-sees-huge-2027-surplus
Sector ETF outflow / volume dry-up|HIT|0.72|2026-11-10|https://www.gurufocus.com/news/9169025/what-state-street-energy-select-sector-spdr-etf-xle-sold-exxon-leads-on-monday
Crowded long (extreme relative performance + valuation)|HIT (unwind)|0.78|2026-11-10|https://premarketdaily.com/2026/11/07/one-week-rewrote-the-entire-sector-hierarchy-heres-what-comes-next/
Sector rotation out of energy|HIT|0.80|2026-11-10|https://premarketdaily.com/2026/11/07/one-week-rewrote-the-entire-sector-hierarchy-heres-what-comes-next/
Sector breadth failure (ETF up, names flat)|WATCH|0.40|2026-11-10|https://stakeandpaper.com/news/week-opens-with-producers-bullion-miners-and-uranium-names-each-following-their-own-script
HIT_GRID_END

SECTOR_SCORES_BEGIN
SECTOR: Energy
ETF: XLE
REGIME: risk_off
MULTIPLIER: 1.0
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: -2
S2_BREADTH: -1
S3_FLOWS_POSITIONING: -1
S4_ETF_TAPE: -1
DIVERGENCE_NOTE: none — S1 crude collapse and S4 relative tape both negative; no factor-vs-tape conflict.
CONFIDENCE: 0.65
HORIZON_3D: down:mild:0.62
HORIZON_1W: down:mild:0.55
HORIZON_2W: flat:mild:0.45
HORIZON_1M: flat:mild:0.40
SECTOR_SCORES_END


---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': -2.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': -1.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 1.0, 'leading_sum': -8.0, 'divergence_flagged': False, 'total_score': -9.084, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.85, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': -0.8009, 'score': -4.805, 'legs': [{'leg': 'CL', 'pct': -1.59, 'w': 0.35}, {'leg': 'QA', 'pct': -1.02, 'w': 0.15}, {'leg': 'ES', 'pct': -0.0, 'w': 0.6}, {'leg': 'PM:XLE', 'pct': -0.84, 'w': 0.7}]}, 'overlay_score': -6.0, 'overlay_raw': -8.75, 'index_carry': 1.721, 'general_total': 6.883, 'skill_multipliers': {'S0_SHARED_MACRO': 0.5, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.65}
```
