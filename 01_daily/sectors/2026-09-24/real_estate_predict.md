# Sector Prediction — Real Estate — 2026-09-24

- ETF: **XLRE**
- rubric: `00_grounding/sectors/real_estate.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-9.97** (mult 0.85)
- regime: risk_off
- divergence_flagged: **False**
- engine: v2 · tape_anchor **-2.055** (ES -0.64%, ZN -0.03%) · index_carry **-1.915** (general -7.659) · llm_overlay **-6.0** (raw -6.906)

## Channel 1 sector ETF tape

```
ETF XLRE vs SPY (yfinance, through 2026-09-23):
  1d: XLRE -1.76% | SPY -0.74% | rel -1.03%
  3d: XLRE -1.74% | SPY +0.93% | rel -2.67%
  1w: XLRE -2.04% | SPY +1.63% | rel -3.67%
  1m: XLRE -6.41% | SPY +0.52% | rel -6.93%
```

MEMORY_CONFIRM: Reviewed prior runs for sector Real Estate (XLRE) only. Last graded: 2026-09-23 down/mild vs XLRE −1.761% / SPY −0.720% / rel −1.041% (dir HIT, mag MISS — actual notable; unsigned S0 under-extended a stress-zone duration skew). 2026-09-22 down/mild vs −0.211% (dir HIT, mag MISS — actual flat; unsigned S0 + rotation-out over-extended the band). 2026-09-21 flat/flat vs +0.141% / rel −1.411% (dir MISS, mag HIT — index_carry flattened a negative leading sum). 2026-09-18 flat/flat vs −0.955% (dir MISS, mag MISS — T+2 unsigned card vs same-session 10Y through 5%). Rolling dir=0.5 mag=0.3 (n=10); last-30 dir=0.5 mag=0.308 (n=26). Open `sector_real_estate` experiment **applies**: 09-22/09-23 wins → keep direction, shrink confidence on modest |score|; 09-18/09-21 losses → when score sign conflicts with tape/breadth, cut conviction. Methodology: (1) experiment applies; (2) 09-23 miss was **band under-extension** from scoring a stress-zone skew as S0=0 — the newest binding lesson; (3) do not restack paid FOMC/Warsh or Monday's AI gap into S0 and S1; (4) S0 is the regime map, S1 the spine — count the rate object once. Applied: **(1) 08-27** — 08-25 is a ban on forcing down, not an up license; 30Y in stress zone ⇒ cap S0/S1 at 0; do not double-count one rate/oil object; do not pad S1 with always-on DC/industrial; leftover NQ/AI ≠ REIT duration relief. **(2) 08-25** — live curve must be independently verified; Finviz 10Y note −0.03% / 30Y bond −0.06% is not relief. **(3) 08-21** — DGS30 **5.29** / live ~5.32 still ≥5.15% stress; a 1–2 bp tick is not relief. **(4) 08-17/08-18** smash **OFF** (no live long-end rip at the open). **(5) 08-11** spike **OFF** (Channel 1 WTI −1.59% / Brent −1.02%; CL=F +1.86% / BZ=F +2.40% is an intraday bounce off a slide, not a Hormuz shock). **(6) 08-12** — FOMC+SEP printed (T+6); Warsh's fresh hike signal is a **new** hawkish increment, not the paid path — score it once, in S0. **(7) 09-04** asymmetric-downside **fires on the skew branch** per the 09-23 lesson — stress-zone 30Y + hawkish Warsh hike signal + two-sided calendar. **(8) 09-08** cushion **does NOT fire** (1d rel **−1.03%**, not ≥ +0.4%). **(9) 09-11** **does fire on the no-force-down branch** — but its precondition (positive live tape) is **NOT met**: ES=F **−0.64%**, NQ=F **−1.09%**, XLK **−1.51%** — the live tape is negative, so a down call has a live negative input. **(10) 09-14** — XLRE **absent from the sector PM board**; unconfirmed; may not set sign or offset S0. **(11) 09-15** flatten-mag is for a telegraphed live 5% smash with sub-gate **green** rel — not this open (1d rel already red). **(12) 09-16** mag-expansion **does not fire** (binary paid). **(13) 09-17** keep-flatten — do not promote leftover index beta into up. **(14) 09-18** joint down-gate **does NOT fire** — 1d rel ≲ −0.5% is present, but the open 10Y is a **flat hold under 5%**, not a failed round-number break. **(15) 09-21** — every-horizon relative lag + non-participation vs growth is the MACRO MAP object; do not let index_carry erase the relative-skew expression. **(16) 09-22** — unsigned S0 + rotation-out only + ES/NQ inside ±0.5% ⇒ down/flat, not down/mild. **Today ES/NQ are OUTSIDE ±0.5% (negative), so the 09-22 flat-cap does NOT bind.** **(17) 09-23 (most binding)** — a stress-zone long-end yield + re-accelerating oil + two-sided macro calendar is a **negative skew**, not S0=0; "not relief" ≠ neutral for a pure-duration sector; the band must be allowed to expand on the negative side. **(18) 08-14** reconcile Σ×mult.

---

## Real Estate (XLRE) — 2026-09-24

### Channel 1 (used as given, not re-derived)

Rates through **2026-09-22**: DGS10 **4.96** (1d **0.0** / 1w **−0.04** / 1m **+0.22**), DGS30 **5.29** (1d **0.0** / 1w **−0.07** / 1m **+0.02**), DFII10 **2.63** (1d **+0.01** / 1w **+0.01** / 1m **+0.23**) — **real yields still up on 1m**. That 1d column is **Tuesday's close**, not this open. VIX **16.44** (1d **+1.26**, 1w **−1.27**), VIX/VIX3M **0.908 — contango** (term structure normal, but spot VIX rising 1d). Finviz: ES **+0.20%**, NQ **+0.41%**, Russell **+0.08%**, DJIA **+0.11%**. Channel 1 also prints **ES=F −0.64% / NQ=F −1.09% vs prev close** — a **negative** overnight tape, tech-led, the opposite of the 09-16/09-17/09-18/09-21 leftover-green pattern. Asia composite **−0.09%** (Nikkei +0.76%, Kospi +1.04%, Shanghai −1.22%, ASX −0.72%), Europe **−0.40%** (DAX −0.52%, CAC −0.56%). **Oil mixed**: Finviz WTI **$104.16 (−1.59%)**, Brent **$107.67 (−1.02%)**; CL=F **+1.86%** / BZ=F **+2.40%** — an intraday bounce off a slide, level still >$100. Gold **+0.90%** (GC=F −0.68%), Silver **+1.96%**, Copper **+0.66%**. DXY **−0.02%** Finviz / **+0.13% 1d** / **+2.25% 1m** (USD firm on the month). **10Y note −0.03%**, **5Y note −0.01%**, **2Y note +0.01%**, **30Y bond −0.06%**, Ultra Bond **−0.06%** (Finviz prices slightly down = **yields ~flat-to-+1 bp on that board**). HY OAS **2.68** (+0.02 1d). 5-day 10Y–SPX corr **−0.826** (strongly negative — the rate channel is the live equity driver). EPU **114.08** (1d −78.97, 1w −240.03 — policy uncertainty collapsing). RRP **0.461**.

**Sector premarket vs prev close:** XLRE **not on the board**. Peers: XLE **+1.11%**, XLP **+0.41%**, XLU **+0.10%**, XLI **−0.00%**, XLY **−0.05%**, XLF **−0.07%**, XLV **−0.40%**, XLC **−0.71%**, XLK **−1.51%**. **Not** a 09-14-style second-best defensive rotation bid into REITs. Per 09-14 this absence is **unconfirmed** and is scored **0** — it does not set sign and is not an S0 offset. Leftover index beta is **not** a participation certificate.

XLRE vs SPY through **2026-09-23**: 1d **−1.76 / −0.74 / rel −1.03**; 3d rel **−2.67**; 1w rel **−3.67**; 1m rel **−6.93**. **Every horizon is a relative laggard, and the lags are widening** (1m −6.93% is the deepest in the last-10 log). No defensive cushion (09-08 override does NOT fire). This is a **hard funding-source / rotation-out** configuration, not duration relief.

MAP HEAT: **split, not a parent vote** — Hotel / Residential nested **up**; Office / Mortgage / Specialty (EQIX) nested **down**; Industrial / Diversified / Healthcare / Retail **flat**. `size_gate=True`. Do not let WELL, EQIX, PLD, or BXP define XLRE.

### Channel 2

**1. Shared macro as it hits REITs.** This is a **hawkish-repricing / rates-backup / risk-off tape**, and it is the **opposite** of the 09-16→09-21 leftover-green digestion pattern. The News Judge's #1 and #2 items are the dominant driver: **"Fed Chair Warsh signals rate HIKES may be needed; September hike back on the table"** (channel: rates, severity: regime, confidence 0.9) and **"Treasury yields spike / 10Y backup on Warsh; Wall Street ends lower"** (channel: rates, severity: session, confidence 0.85). The Finviz digest corroborates: *"S&P 500, Dow, Nasdaq Drop As Yields Spike Amid Calls For More Rate Hikes."* This is a **fresh hawkish increment** — not the already-printed 09-16 FOMC+SEP path (which is T+6 and scored 0 per 08-12/09-11). Warsh's explicit hike signal is a **new** object that lifts the front end and the term premium, and it is the live transmission channel (5-day 10Y–SPX corr **−0.826**).

For REITs this is the **worst possible macro map**: a pure-duration sector, already the deepest multi-horizon relative laggard (1m rel −6.93%), facing a fresh hawkish repricing with the 30Y still at **5.29%** — inside the multi-decade stress zone (08-21). The live curve is **flat-to-+1 bp** on the Finviz board, which is **not relief** (08-25) and **not a smash** (08-18 OFF) — but per the **09-23 lesson**, "not relief" at a stress-zone yield with a fresh hawkish catalyst and a two-sided calendar is a **negative skew, not S0=0**. The 09-23 lesson is explicit: *"When a pure-duration sector sits at a stress-zone yield with oil oscillating near a round number and a two-sided macro calendar, 'not relief' must be scored as a NEGATIVE SKEW, not as zero. The absence of a positive catalyst in a fragile-rate regime is itself a negative input for REITs."* Today adds a **fresh hawkish catalyst** (Warsh hike signal) on top of the stress-zone yield — a stronger negative skew than 09-23's. **S0 = −1.**

**2. Spine (count the rate object once; S0 is the regime map, not a second copy of the same backup).**
- **Rates rising / REIT selloff: HIT (partial).** The live curve is flat-to-+1 bp, not a verified rip — but the **hawkish regime shift** (Warsh hike signal) is a live, same-morning rate-path object that pressures the duration sleeve. Per 09-23, the stress-zone yield + fresh hawkish catalyst is a negative skew. Score **−1**.
- **Real yields rising: HIT (structural).** DFII10 **2.63**, up **+0.23 on 1m** — the operative duration horizon for a daily REIT call (08-11). Score **−0.5**.
- **Rates falling / REIT duration relief: MISS.** No verified falling curve; Finviz long-end prices slightly down = yields flat-to-+1 bp. Not relief (08-25).
- **Data-center REIT demand / rent upside: MISS.** No fresh DC catalyst; the AI-complex is **weak** today (NQ=F −1.09%, XLK −1.51%), which is a **headwind** to the EQIX/DLR sleeve, not a support (09-14 lesson: the DC sleeve is a headwind on an AI-unwind day).
- **Industrial REIT occupancy / rent growth: MISS.** No fresh industrial catalyst.
- **Office vacancy / mark-to-market stress: MISS (structural, not same-day).** No fresh office news; do not pad.
- **Refinancing window opening / cap-rate compression: MISS.** A hawkish repricing **closes** the refinancing window and pressures cap rates — the opposite sign.
- **Refinancing wall stress / cap-rate expansion: HIT (mild).** A fresh hawkish repricing at a stress-zone long end is a cap-rate-expansion / refinancing-cost headwind. Score **−0.5**.
- **Sector rotation out of real estate: HIT.** Every-horizon relative lag, widening (1m rel −6.93%), with the sector absent from the PM board while XLE/XLP lead. Score **−0.5**.

**S1 = −2.5** (rates-rising −1, real-yields-rising −0.5, cap-rate-expansion −0.5, rotation-out −0.5).

**3. Breadth / leadership inside the sector.** No same-day breadth data is available pre-open (XLRE absent from the PM board). The structural read is a **hard multi-horizon laggard** with no cushion and no leadership tell. Per 09-11, a stale multi-horizon lag must not be scored into **both** S2 and S4 — I score it **once** in S2 as a breadth/participation failure, and leave S4 to the confirmation-only tape. **S2 = −1.**

**4. Flows / positioning / crowding.** No fresh flow data. The sector is **not crowded long** (it is a laggard, not an extended winner) — the crowded-long mean-reversion risk does not apply. But the persistent relative outflow / non-participation is a near-term demand negative. **S3 = −0.5.**

**5. Earnings / guidance / policy catalysts.** No XLRE-specific earnings or policy catalyst in the News Judge or Finviz digest. The dominant catalyst is **macro** (Warsh hawkish), already scored in S0. The AI-complex weakness (ASML sold out is a **positive** for semis but the tape is **down** — NQ −1.09%, XLK −1.51%, APH −6.5% on Fabrinet weakness) is a **headwind** to the DC sleeve, not a support. No new REIT-specific catalyst. **Checked, nothing material beyond the macro map.**

**6. ETF tape (confirmation only).** 1d rel **−1.03%**, 3d rel **−2.67%**, 1w rel **−3.67%**, 1m rel **−6.93%** — uniformly negative, widening. This **confirms** the negative factor card; it does not drive it. **S4 = −1.**

### Divergence check
Leading factor sum (S0 −1, S1 −2.5, S2 −1, S3 −0.5) = **−5.0**; tape confirmation S4 = **−1**. **No divergence** — factors and tape agree on the negative direction. The 09-11 "positive live tape forbids down" clause is **OFF** (ES=F −0.64%, NQ=F −1.09% — the live tape is negative). The 09-22 flat-cap is **OFF** (ES/NQ outside ±0.5%). The 09-23 lesson **fires** (stress-zone yield + hawkish catalyst + two-sided calendar = negative skew, band allowed to expand).

### Self-audit
- **Lens:** pure-duration REIT sector, rate channel dominant (5-day corr −0.826). Correct.
- **Band:** |leading sum| ≈ 5.0 × mult 0.85 ≈ −4.3 → **down/mild**. The 09-23 lesson permits expansion to mild on the negative side; notable would require a verified long-end smash at the open, which is **not** present (Finviz flat-to-+1 bp). Cap at **mild**.
- **Skew:** negative (stress-zone 30Y + fresh hawkish Warsh + widening relative lag). Correct per 09-23.
- **Same-shock double-count:** the Warsh hawkish object is scored **once** in S0 (regime map); the rate-rising spine in S1 is the **live curve** object, not a second copy of Warsh. Flagged and mitigated.
- **Single-ticker:** EQIX/DLR/WELL/PLD/BXP do **not** drive the call — the call is macro/duration-driven, and the DC sleeve is scored as a **headwind** (AI-complex weak), not a support.

---

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -2.5
S2_BREADTH: -1
S3_FLOWS_POSITIONING: -0.5
S4_ETF_TAPE: -1
MULTIPLIER: 0.85
CONFIDENCE: 0.6
REGIME: risk_off
SECTOR_DIRECTION: down
SECTOR_MAGNITUDE_BAND: mild
DIVERGENCE_FLAGGED: False
HORIZON_3D: down/mild
HORIZON_1W: down/mild
HORIZON_2W: down/mild
HORIZON_1M: down/notable
SECTOR_SCORES_END

HIT_GRID_BEGIN
Rates rising / REIT selloff|HIT|0.7|2026-09-24|https://www.finnewsnetwork.com.au/archives/finance_news_network3921234.html
Real yields rising|HIT|0.7|2026-09-24|https://fred.stlouisfed.org/series/DFII10
Cap-rate expansion|HIT|0.55|2026-09-24|https://www.reuters.com/markets/rates-bonds/
Refinancing wall stress|HIT|0.5|2026-09-24|https://www.reuters.com/markets/rates-bonds/
Sector rotation out of real estate|HIT|0.65|2026-09-24|https://finviz.com/screener.ashx
Risk-off tape / flight to safety|HIT|0.6|2026-09-24|https://finviz.com/futures.ashx
Sector breadth failure (ETF up, names flat)|HIT|0.5|2026-09-24|https://finviz.com/screener.ashx
Sector ETF outflow / volume dry-up|HIT|0.5|2026-09-24|https://finviz.com/screener.ashx
Rates falling / REIT duration relief|MISS|0.7|2026-09-24|https://finviz.com/futures.ashx
Data-center REIT demand / rent upside|MISS|0.6|2026-09-24|https://finviz.com/screener.ashx
Industrial REIT occupancy / rent growth|MISS|0.6|2026-09-24|https://finviz.com/screener.ashx
Office vacancy / mark-to-market stress|MISS|0.5|2026-09-24|https://finviz.com/screener.ashx
Refinancing window opening|MISS|0.6|2026-09-24|https://www.reuters.com/markets/rates-bonds/
Cap-rate compression|MISS|0.6|2026-09-24|https://www.reuters.com/markets/rates-bonds/
Sector rotation into REITs|MISS|0.6|2026-09-24|https://finviz.com/screener.ashx
Crowded long (extreme relative performance + valuation)|MISS|0.6|2026-09-24|https://finviz.com/screener.ashx
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -2.5, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': -0.5, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.85, 'leading_sum': -11.5, 'divergence_flagged': False, 'total_score': -9.97, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.85, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': -0.3425, 'score': -2.055, 'legs': [{'leg': 'ES', 'pct': -0.64, 'w': 0.5}, {'leg': 'ZN', 'pct': -0.03, 'w': 0.75}]}, 'overlay_score': -6.0, 'overlay_raw': -6.906, 'index_carry': -1.915, 'general_total': -7.659, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 1.0}, 'llm_confidence': 0.6}
```
