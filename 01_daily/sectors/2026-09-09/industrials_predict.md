# Sector Prediction — Industrials — 2026-09-09

- ETF: **XLI**
- rubric: `00_grounding/sectors/industrials.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **-1.8** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLI vs SPY (yfinance, through 2026-09-08):
  1d: XLI -0.48% | SPY -0.55% | rel +0.06%
  3d: XLI +0.95% | SPY +0.10% | rel +0.84%
  1w: XLI -0.41% | SPY -0.14% | rel -0.26%
  1m: XLI -5.81% | SPY -0.94% | rel -4.87%
```

I have enough context. Let me compile the analysis for Industrials/XLI for 2026-09-09.

MEMORY_CONFIRM: Reviewed prior runs for sector Industrials. Rolling dir=0.2 / mag=0.1 (n=10); last 30 dir=0.25 / mag=0.083 (n=12). Last graded 08-28: narrative down/mild vs pipeline down/flat, actual XLI −0.93% (dir HIT, mag MISS on pipeline flat). 09-01 down/mild, 09-02 flat/flat, 09-03 flat/flat (missed +1.03% on ISM Services beat), 09-04 down/flat (missed +0.41% up on laggard-shield), 09-08 flat/flat (pipeline −3.6 vs narrative down/mild, actual XLI −0.485% — dir MISS on flat, mag MISS). Governing today: **09-08 lesson** (reconcile narrative vs pipeline; when score fights tape with positive 1d/3d rel, prefer flat/mild); **09-04 laggard-shield** (deep laggard + hawkish macro shock → laggards can be relatively shielded; score laggard once, not in both S2/S4); **08-27** (1w/1m laggard → forbid up on non-holdings AHR); **08-18** (cap S1 at 0/+1, don't use GEV/ETN as cushion); **08-11/08-12 supply-shock cap** (verify live oil). DO-INSTEAD: when score fights tape, cut conviction / prefer flat/mild.

## XLI near-session environment (not an SPX call)

### 1. Shared macro as it hits Industrials — S0 = 0
This is a **mixed/pause tape** for a cyclical, not a confirmed risk-off and not a risk-on bid.

- **Futures do not independently confirm direction.** Channel 1 is unavailable this run (do not invent precise levels). The prior session (09-08) was a geopolitical/oil supply-shock day (Houthi attacks on Saudi energy, WTI +3.18% to $94.40, Brent +2.16% to $99.10) that hit XLI −0.48% / rel +0.06%. Today the oil shock is **≥1 session old** — the 09-08 kinetic increment is not a fresh same-morning shock unless oil re-spikes. 08-11/08-12 supply-shock cap applies only while oil is live-up; verify the live barrel before scoring S0 negative.
- **Oil level is still elevated (~$94–99)** — a **cost/stagflation headwind** for transports/manufacturers even if the session change is flat. This is a structural overlay, not a same-session S0 shock.
- **Rates elevated, 30Y in stress zone.** DGS10 ~4.77 / DGS30 ~5.25 / DFII10 ~2.42 (prior-close). 5-day 10Y–SPX corr strongly negative. Real yields elevated. This is a duration/cyclical drag but **secondary to ISM/CapEx** for this book.
- **Fed hike odds contested.** Warsh hawkish (September hike risk), Waller muddied the outlook (09-04). CPI due this week is the binary. Do not one-way score hawkish.
- **XLI tape is a medium-term laggard that has bounced over the last 3 days.** 1d rel +0.06%, 3d rel +0.84%, 1w rel −0.26%, 1m rel −4.87%. The 3d bounce is real but the 1m lag is deep.

**S0 = 0, regime mixed.** Not −1: the oil shock is ≥1 session old (verify live), no fresh hard-data miss, futures not confirmed ≤ −0.5%. Not +1: NQ not confirmed leading, 1m XLI is a deep laggard, no cyclical risk-on. 08-27 still forbids mapping leftover XLK/NVDA beta into XLI S0 = +1.

### 2. Spine + secondary — S1 = 0 (capped)
**No fresh same-morning industrials print in hand.** August ISM manufacturing already printed **09-01**: PMI **54.6** (expansion, 8th month) vs July 55.6; new orders **53.7** (−3.0 pts). Still **expansion** — **not** an ISM-contraction HIT, but **slowing**. ISM Services printed **09-03** (55.4). Both in the tape. 08-18/08-27: **cap S1 at 0/+1**; +2 is forbidden without same-morning confirmation.

- **Grid / AI power — HIT, carried.** GEV ~$176B RPO / 116 GW gas book, Q2 orders +88% organic, data-center electrification orders in Q1 exceeded all of 2025. Greentechlead (09-02): GE Vernova, Siemens Energy, Eaton, Schneider all expanding capacity on AI-infrastructure supercycle. **Structural and semi-independent of ISM.** But 08-18: **not** a downside cushion and **not** a same-session raise. On a post-oil-shock tape, GEV/ETN can still roll.
- **Aerospace & defense — MIXED.** Boeing–SPEEA talks **resumed today (09-08 update: "pleased to get back to the bargaining table")** — constructive, not a strike. Contract expires Oct 6, earliest strike Oct 7. Defense backlog intact. Do **not** cancel ISM (expansion) with one award; do not treat the Iran/Houthi escalation as a fresh defense-order HIT (defense names have been volatile on this conflict).
- **Freight — MIXED.** Cass July shipments still soft (−4.8% y/y); rail carloads +5.5% y/y through early 2026 (14 of 20 categories up). Not a same-morning recovery HIT.
- **Construction slowdown — HIT, carried.** Manufacturing construction off the 2025 peak; AI/nonres is the offset, not a broad build boom.

Net: carried ISM expansion (slowing) + structural grid + constructive Boeing talks vs construction drag + mixed freight + oil-cost overlay. **S1 = 0** (no fresh same-morning confirmation; carried positives are in the tape).

### 3. Breadth — S2 = −1
XLI is a **deep medium-term laggard**. Channel 1 through 09-08: 1d rel **+0.06%** (flat), 3d **+0.84%**, 1w **−0.26%**, 1m **−4.87%**. Seeking Alpha (09-04): "Industrial stocks face broader weakness as bear-market breadth hits 2025 high — 45% of stocks are in bear markets despite XLI staying elevated." This is a **breadth failure** — the ETF is carried by large-cap AI-power/defense names (GEV, ETN, CAT) while the majority of industrial names are in downtrends. Score the lag **once** here (09-04: do not double-count in S2 and S4).

### 4. Flows — S3 = 0
Checked, nothing material returned on XLI flows this morning. Rotation has been out of industrials into tech/AI-power. Not a crowded long (1m rel −4.87%). **S3 = 0.**

### 5. ETF tape (confirmation only) — S4 = 0
Channel 1 through 09-08: 1d rel **+0.06%** (flat), 3d **+0.84%**, 1w **−0.26%**, 1m **−4.87%**. The 1d tape is **flat** — XLI matched SPY on the oil-shock day. The 3d bounce (+0.84%) is real but the 1m lag is deep. 09-04: score the laggard **once** (in S2), not again in S4. S4 = 0 (flat 1d tape is not confirming continued underperformance).

### 6. Catalysts / calendar
- **CPI due this week** — the binary. Fed hike odds contested (Warsh hawkish, Waller muddied).
- **Boeing–SPEEA talks resumed today** (constructive, not a strike).
- **Oil level elevated (~$94–99)** on Houthi/Hormuz — cost headwind, ≥1 session old.
- ISM manufacturing (09-01) and ISM Services (09-03) already in the tape.

### Self-audit
- Lens: cyclical; rates only in S0, not re-counted in S1.
- Band: **flat/mild**, not notable (no fresh hard-data miss, oil shock ≥1 session old, contested Fed path).
- Skew: GEV/BA do not drive the ETF call.
- Same-shock: oil counted once in S0, not re-counted in S1.
- 09-04: laggard scored once (S2), not double-counted in S4.
- 09-08: reconcile narrative vs pipeline — Σ(S0..S4) = −1 × 0.9 = −0.9 → flat/flat.

**Divergence:** Leading factors (S2 laggard) point mildly down, but the 1d tape is flat (+0.06%) and the 3d bounce (+0.84%) is positive. Per DO-INSTEAD and 09-08 lesson, prefer flat/mild when score fights tape. The laggard-shield (09-04) applies: on a hawkish/oil-shock tape, already-de-risked laggards can be relatively shielded.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 0
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.5
REGIME: mixed
TOTAL_SCORE: -0.9
PREDICTED_DIRECTION: flat
PREDICTED_MAGNITUDE_BAND: flat
HORIZON_3D: flat:mild:0.5
HORIZON_1W: flat:mild:0.45
HORIZON_2W: flat:mild:0.4
HORIZON_1M: down:mild:0.4
SECTOR_SCORES_END

HIT_GRID_BEGIN
ISM manufacturing / new orders expansion|HIT (carried, slowing — PMI 54.6, new orders 53.7, printed 09-01)|0.7|2026-09-01|https://www.prnewswire.com
Grid / electrical equipment backlog (AI power)|HIT (structural — GEV $176B RPO, Q2 orders +88%, AI supercycle)|0.8|2026-09-02|https://greentechlead.com
Aerospace & defense order / budget upside|MIXED (Boeing-SPEEA talks resumed constructively; defense backlog intact)|0.6|2026-09-08|https://www.boeing.com
Sector breadth failure (ETF up, names flat)|HIT (45% of industrial stocks in bear markets despite XLI elevated)|0.7|2026-09-04|https://seekingalpha.com
Construction slowdown|HIT (carried — manufacturing construction off 2025 peak)|0.6|2026-09-01|
Freight / trucking / rail volume recovery|MIXED (rail +5.5% y/y, Cass trucking still soft)|0.5|2026-09-02|
Risk-off tape / flight to safety|PARTIAL (oil shock ≥1 session old, elevated ~$94-99)|0.5|2026-09-08|
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': -2.0, 'divergence_flagged': False, 'total_score': -1.8, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.5, 'regime': 'mixed', 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': 0.26, 'w1': 1.06}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
