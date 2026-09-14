# Sector Prediction — Real Estate — 2026-09-14

- ETF: **XLRE**
- rubric: `00_grounding/sectors/real_estate.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-0.491** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **1.584** (ES -0.66%, ZN +0.12%, PM:XLRE +0.48%) · index_carry **-2.751** (general -11.002) · llm_overlay **0.675** (raw 0.675)

## Channel 1 sector ETF tape

```
ETF XLRE vs SPY (yfinance, through 2026-09-11):
  1d: XLRE +0.86% | SPY +0.85% | rel +0.01%
  3d: XLRE -1.09% | SPY -0.22% | rel -0.88%
  1w: XLRE -1.88% | SPY -1.15% | rel -0.73%
  1m: XLRE -2.41% | SPY -1.06% | rel -1.34%
```

I now have a complete picture. Let me synthesize.

## MEMORY_CONFIRM

MEMORY_CONFIRM: Reviewed prior runs for sector Real Estate. Last graded: 2026-09-10 down/mild vs XLRE −0.829% / SPY −0.599% / rel −0.230% (dir HIT, mag HIT). 2026-09-11 down/flat vs XLRE +0.859% / SPY +0.852% / rel +0.007% (dir MISS — pure beta, no idiosyncratic move). 08-31/09-01/09-02 down/mild, 09-03 flat/flat, 09-04 flat/flat, 09-08 down/mild, 09-09 down/mild still ungraded. Rolling dir=0.4 mag=0.4 (n=10); last-30 dir=0.5 mag=0.333. Applied active REIT lessons: **(1) 08-27** — 08-25 is a ban on forcing down, not an up license; 30Y in stress zone ⇒ cap S0/S1 at 0; do not double-count one rate shock; do not pad S1 with always-on DC/industrial; green/NQ beta ≠ REIT duration relief. **(2) 08-25** — verify the live curve; do not force down off a stale prior-close rising table if the open curve is falling. **(3) 08-21 level-vs-change** — 30Y ~5.37 is a multi-decade stress zone; a small dip is not relief. **(4) 08-17/08-18** — live long-end + hawkish path → absolute down; 1d relative bid is a mag cap only. **(5) 08-11 geo/oil** — oil is UP sharply today (WTI $102.29 +2.44%, Brent $107.33 +2.80%) on Iran escalation — a **live, escalating geopolitical/oil supply-shock overlay**; the spike branch FIRES. **(6) 08-12** — two-sided policy events stay two-sided; do not one-way score S0. **(7) 09-04 asymmetric-downside** — hawkish/unresolved backdrop + rate-sensitive bond-proxy with 1w/1m lags ⇒ pre-score asymmetric downside. **(8) 09-08 cushion lesson** — 1d rel cushion ≥ +0.4% confirmed by prior session ⇒ positive S4 / direction override toward flat. **Today 1d rel is +0.01% — NOT a +0.4% cushion, so the 09-08 override does NOT fire.** **(9) 09-11 lesson (NEW, most binding)** — when S0 is genuinely 0 and the live tape is positive, a down call requires a LIVE negative input; a stale multi-horizon lag must not be scored into S2 AND S4; "no cushion ≠ headwind"; an object explicitly identified as already-priced (T+2) must be scored 0. **(10) 08-14 reconcile Σ×mult.** Open `sector_real_estate` experiment: keep direction, shrink confidence on modest |score| when magnitude historically misses.

---

## Real Estate (XLRE) — 2026-09-14

### Channel 1 (used as given, not re-derived)

Rates through **2026-09-10**: DGS10 **4.95** (1d **+0.12** / 1w **+0.16** / 1m **+0.25**), DGS30 **5.37** (1d **+0.09** / 1w **+0.10** / 1m **+0.13**), DFII10 **2.55** (1d **+0.09** / 1w **+0.10** / 1m **+0.12**) — **real yields rising on every listed horizon**. That 1d column is **Thursday's close**, not this open. VIX **17.67** (1d **+1.83**, 1w **+2.37**), VIX/VIX3M **1.135 — BACKWARDATION** (stress). **ES=F −0.66%**, **NQ=F −1.59%**, Russell −0.27%, DJIA −0.15% — **futures clearly negative, tech-led**. Asia composite **−0.72%** (Kospi −3.26%, Nikkei −0.81%), Europe **−0.35%**. **Oil UP sharply**: WTI **$102.29 (+2.44%)**, Brent **$107.33 (+2.80%)**, Heating Oil +2.67%, Gasoil +2.35% — a **live geopolitical/oil supply-shock overlay** (08-11 fires). Gold **−1.26%**, Silver **−2.17%**, Copper **−1.44%**. DXY **+0.41%** (USD strengthening). **10Y note +0.12%**, **30Y bond +0.06%**, Ultra Bond +0.03% (prices up = **yields flat-to-slightly-down this morning**). 5-day 10Y–SPX corr **−0.248** (weakly negative — much less negative than the −0.9s of last week). HY OAS **2.70**. EPU **725.88** (1d **+451** — a massive policy-uncertainty spike). RRP **5.255** (1d +0.519, 1w +4.553 — big drain reversal).

**Sector premarket vs prev close: XLRE +0.48%** — the **second-best sector in the premarket tape** (behind XLP +0.61%, ahead of XLF +0.33%, XLU +0.33%), while XLK is −1.95% and XLI −1.13%. This is a **defensive rotation bid into XLRE**, live and knowable at the open.

XLRE vs SPY through **2026-09-11**: 1d **+0.86 / +0.85 / rel +0.01**; 3d rel **−0.88**; 1w rel **−0.73**; 1m rel **−1.34**. **1d is flat (pure beta, no idiosyncratic move); 3d/1w/1m remain modest laggards.** No defensive cushion (09-08 override does NOT fire).

### Channel 2

**1. Shared macro as it hits REITs.** This is a **tech-led risk-off / oil-shock / pre-FOMC overlay**, and — critically — it is **not** a rates-backup day. The live curve is **flat-to-slightly-down** (10Y note +0.12%, 30Y bond +0.06%, Ultra Bond +0.03% — prices up = yields down). The dominant overnight story is **AI-complex weakness**: "Nasdaq futures slide nearly 400 points after recent AI developments," "Asian shares decline and OpenAI investor SoftBank shares plunge after calls to slow AI industry," Kospi **−3.26%**. That is a **tech/AI-specific unwind**, not a broad macro smash. **Oil is spiking** (WTI $102.29, Brent $107.33) on Iran escalation — a live inflation/stagflation overlay that pressures long-duration assets (08-11 fires). **Gold −1.26% / Silver −2.17%** — no bond-proxy FTS bid in metals. **USD +0.41%** — a mild headwind for a domestic sector. **VIX 17.67 with VIX/VIX3M 1.135 backwardation** — a genuine stress tell, but VIX is still sub-20. **FOMC is next week** (Sep 15–16 per the calendar: "headlined by Retail Sales and FOMC Meeting — Rate Decision"), with September hike odds elevated post-Warsh. **Retail Sales is this week** — a two-sided, unprinted binary.

The 09-11 lesson is the binding constraint here: **S0 must be genuinely 0 unless there is a LIVE negative input.** The live curve is *not* rising (it is flat-to-down), so the 08-17/08-18 "live long-end smash" branch does **not** fire. The hawkish Warsh repricing is **already printed (T+2+)** and is in the term premium — per 09-11, an object explicitly identified as already-priced must be scored **0**, not as a live headwind. The oil spike is live and real, but for REITs it is a **second-order inflation channel**, not a direct revenue/cost hit, and it is already partly expressed in the tape. **S0 = 0** (mixed: flat-to-easing live curve + defensive premarket rotation offset by oil spike, USD strength, VIX backwardation, and a pre-FOMC event-risk overhang).

**2. Spine (count the rate object once; S0 is the regime map, not a second copy).**
- Rates falling / REIT duration relief: **partial HIT.** The live curve is flat-to-slightly-down (10Y/30Y/Ultra prices up). But 08-21 says a 1–3 bp tick at a 30Y of **5.37%** — a multi-decade stress zone — is **stabilization, not relief**. So this is a *mild* positive at best, not a duration-relief thesis.
- Rates rising / REIT selloff: **not a clean HIT at the open.** The prior-close FRED table (DGS10 +12 bp 1d, DGS30 +9 bp 1d) is **Thursday's close**, not today's tape. 08-25 forbids treating that column as the live curve. The live curve is flat-to-down.
- Real yields rising: Channel 1 1d **+9 bp**, 1w **+10 bp**, 1m **+12 bp** — a real backdrop, same duration channel, **not** a second independent shock. This is the one genuinely negative spine element, and it is a *level/trend* object, not a same-day impulse.

Net spine: **mildly negative to neutral.** The 30Y at 5.37% caps any positive duration score at 0 (08-21), and the real-yield trend is a persistent drag — but there is no live rate shock today.

**3. Secondary.**
- Data-center REIT demand / rent upside: **HIT, stale.** DLR/EQIX AI-infrastructure thesis is well-covered (Wells Fargo favors DC real estate, 09-01; "Top AI Data Center REITs in 2026"). But this is 1w–1m property-type dispersion, **not a same-day up vote** (08-27). **And today the AI complex is being sold hard** (NQ −1.59%, Kospi −3.26%, SoftBank plunge) — so the DC sleeve is arguably a **headwind** today, not a support. EQIX/DLR must not define XLRE.
- Industrial REIT occupancy / rent growth: **HIT, stale** (PLD quality sleeve). NAR September report: "industrial rebalanced as demand improved." Not a 1-day catalyst.
- Refinancing window / cap-rate compression: **MISS.** 30Y 5.37%; no compression. NAR notes "elevated borrowing costs."
- Office vacancy / mark-to-market: **HIT, small sleeve.** Office vacancy ~20–22% in major CBDs, CMBS office delinquency >11%, ~$1T CRE debt maturing 2024–26. Office is ~1% of XLRE (BXP). Do not let office set the ETF.
- Refinancing wall stress: **HIT, structural.** The CRE maturity wall is intact; not a same-morning print.
- Sector rotation into REITs: **HIT, live.** XLRE +0.48% premarket, second-best sector, on a tech-led risk-off day. This is the **one genuinely live positive** — a defensive rotation bid. Score it **once** (S1), not again in S2/S4.
- Sector rotation out of real estate: **MISS today** (the rotation is *into* REITs this morning).

**4. Breadth / leadership.** XLRE 1d rel **+0.01%** — pure beta, no idiosyncratic move (the 09-11 lesson's exact signature). The 3d/1w/1m lags (−0.88 / −0.73 / −1.34) are **modest and stale** — per 09-11, a stale multi-horizon lag is a structural descriptor, **not a same-day tape signal**, and must not be scored into S2 or S4. There is **no fresh same-day constituent/breadth data** showing a WELL-only carry or a breadth failure. The premarket rotation bid is sector-wide (XLRE +0.48% vs XLP +0.61%, XLU +0.33%). **S2 = 0.**

**5. Flows / positioning.** No fresh same-day flow print available. XLRE has been a modest 1m laggard (−1.34% rel) with no evidence of a crowded long (a −1.34% 1m rel is the opposite of crowding) and no evidence of a washout spike. Trailing flows are not a 1-day lid (per the XLF/XLP lessons). **S3 = 0.**

**6. Earnings / policy.** No fresh REIT print this morning. The dominant objects are: (a) the **already-printed** Warsh hawkish repricing (T+2+, scored 0 per 09-11), (b) the **live** Iran/oil escalation (scored once in S0), and (c) **FOMC next week + Retail Sales this week** — two-sided, unprinted binaries. Per the 09-03 macro-surprise lesson, an identified high-impact two-sided catalyst is a **magnitude-expansion risk**, not a magnitude cap — so it widens the band to at least mild and lowers confidence, but does not set direction.

### Divergence check

Leading factor sum: S0 0 + S1 +0.5 + S2 0 + S3 0 + S4 0 = **+0.5**. Tape confirmation (S4) = 0. **No divergence** — factors and tape agree on a near-flat, mildly-positive-leaning session. The premarket sector tape (XLRE +0.48%, second-best) is the live confirmation and it is **positive**, which per 09-11 forbids a down call absent a live negative input. There is no live negative input to REITs specifically today (the rate shock is absent; the oil shock is a second-order inflation channel already partly priced).

### Self-audit

- **Lens:** rate-sensitive bond-proxy, short horizon dominated by the live curve — which is flat-to-down, not rising. ✓
- **Band:** rolling mag accuracy is 0.4/0.333 — shrink confidence, keep the band at flat/mild. ✓
- **Skew:** the 09-04 asymmetric-downside lesson requires a *hawkish/unresolved* backdrop with a *flat-to-easing open* — but 09-11 explicitly narrowed this: the hawkish backdrop is T+2+ and already priced, and the live tape is positive. The 09-04 lesson does **not** fire today. ✓
- **Same-shock double-count:** oil scored once (S0); the rate object scored once (S1); the rotation-into-REITs scored once (S1). ✓
- **Single-ticker:** EQIX/DLR/WELL explicitly barred from defining the ETF call; the DC sleeve is arguably a *headwind* today given the AI-complex selloff. ✓
- **09-08 cushion override:** does NOT fire (1d rel +0.01%, not ≥ +0.4%). ✓
- **09-11 lesson:** no stale lag scored into S2/S4; no already-priced object scored as a live headwind. ✓

### Call

The live, knowable-at-open tape is **mildly positive for XLRE**: a defensive rotation bid (+0.48% premarket, second-best sector) on a tech-led risk-off day, with a flat-to-easing live curve and no live rate shock. Against that: a live oil spike, USD strength, VIX backwardation, a 30Y at 5.37% capping duration upside, and a pre-FOMC event-risk overhang. The net is a **flat session with a mild positive skew** — the sector should hold up relatively on a risk-off day but is capped by the long-end level and the oil/inflation overlay. Per 09-11, with S0 genuinely 0 and the live tape positive, the correct call is **flat** (sign set by the live tape, magnitude capped at mild given the pre-FOMC binary and rolling mag discipline).

**Predicted: flat / mild** (lean: flat-to-slightly-up absolute; relative outperformance vs SPY likely on a tech-led risk-off day).

---

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 0.5
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.5
REGIME: mixed
SECTOR_SCORES_END

HORIZON_3D: flat/mild — the 30Y at 5.37% caps duration upside; FOMC next week is the swing factor. If the Fed holds dovish, REITs rally; if hawkish, the long end backs up and XLRE lags.
HORIZON_1W: flat/mild — pre-FOMC drift, then a rate-driven resolution. The 3d/1w relative lags (−0.88 / −0.73) are modest and mean-revertible if the curve stabilizes.
HORIZON_2W: flat/mild — dependent on whether the 10Y holds below 5% and whether the oil spike fades. A sustained 30Y above 5.4% is the key downside risk.
HORIZON_1M: down/mild — the 1m relative lag (−1.34%) plus a 30Y in the multi-decade stress zone and a persistent real-yield uptrend (DFII10 +12 bp 1m) keep REITs structurally capped until the long end stabilizes.

HIT_GRID_BEGIN
Rates falling / REIT duration relief|partial|0.4|2026-09-14|https://www.reuters.com/world/europe/global-bond-selloff-pushes-10-year-us-yield-toward-5-oil-rate-hike-fears-2026-09-11/
Rates rising / REIT selloff|miss|0.6|2026-09-14|https://www.reuters.com/world/europe/global-bond-selloff-pushes-10-year-us-yield-toward-5-oil-rate-hike-fears-2026-09-11/
Real yields rising|hit|0.7|2026-09-14|https://www.reuters.com/world/europe/global-bond-selloff-pushes-10-year-us-yield-toward-5-oil-rate-hike-fears-2026-09-11/
Risk-off tape / flight to safety|hit|0.6|2026-09-14|https://news.google.com/rss/articles/CBMi0wFBVV95cUxPM2xLZE1oS1FfREx4TVBZREU0RTdkNDZZTENrWnppSmJ1THhQYjZXVGVuOC1iVU5Ydnh1T3gzWkRaZUVYU1pGZEFJdV90OF9CQ0d4M25CNGFTRi1YbXhmSzZMRlZoWjFjRU9Ta2hnVHdFN09POGNjMV9MNnNWMDdfWTliNEJBcjk5QW1oZEtKRGJuS0dFSm94SU93S2pidDd6QzBGaUxXR1V2U0VwVTJiMmRnRW0tUy1IVGNYVUdwUXJqcVRXS1NOcTFKYkNDMGRzQTZz
Sector rotation into REITs|hit|0.55|2026-09-14|https://premarketprice.com/premarket-movers
Data-center REIT demand / rent upside|hit|0.5|2026-09-14|https://seekingalpha.com/article/investing-in-ais-backbone-as-wells-fargo-favors-data-center-real-estate
Industrial REIT occupancy / rent growth|hit|0.4|2026-09-14|https://www.nar.realtor/commercial-real-estate-market-insights/september-2026-commercial-real-estate-market-insights-report
Office vacancy / mark-to-market stress|hit|0.6|2026-09-14|https://deluair.com/consultancy/insights/us-cre-office-distress-2026
Refinancing wall stress|hit|0.5|2026-09-14|https://deluair.com/consultancy/insights/us-cre-office-distress-2026
Cap-rate expansion|hit|0.4|2026-09-14|https://www.nar.realtor/commercial-real-estate-market-insights/september-2026-commercial-real-estate-market-insights-report
Refinancing window opening|miss|0.5|2026-09-14|https://www.nar.realtor/commercial-real-estate-market-insights/september-2026-commercial-real-estate-market-insights-report
Cap-rate compression|miss|0.5|2026-09-14|https://www.nar.realtor/commercial-real-estate-market-insights/september-2026-commercial-real-estate-market-insights-report
Sector rotation out of real estate|miss|0.5|2026-09-14|https://premarketprice.com/premarket-movers
Sector breadth expansion (% names up)|checked, nothing material|0.4|2026-09-14|
Sector breadth failure (ETF up, names flat)|checked, nothing material|0.4|2026-09-14|
Sector ETF inflow / relative volume spike|checked, nothing material|0.3|2026-09-14|
Sector ETF outflow / volume dry-up|checked, nothing material|0.3|2026-09-14|
Crowded long (extreme relative performance + valuation)|miss|0.5|2026-09-14|
USD strengthening|hit|0.5|2026-09-14|
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': 1.5, 'divergence_flagged': False, 'total_score': -0.491, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.52, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.264, 'score': 1.584, 'legs': [{'leg': 'ES', 'pct': -0.66, 'w': 0.5}, {'leg': 'ZN', 'pct': 0.12, 'w': 0.75}, {'leg': 'PM:XLRE', 'pct': 0.48, 'w': 0.7}]}, 'overlay_score': 0.675, 'overlay_raw': 0.675, 'index_carry': -2.751, 'general_total': -11.002, 'skill_multipliers': {'S0_SHARED_MACRO': 0.5, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.5}
```
