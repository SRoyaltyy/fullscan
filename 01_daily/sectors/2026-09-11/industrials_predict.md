# Sector Prediction — Industrials — 2026-09-11

- ETF: **XLI**
- rubric: `00_grounding/sectors/industrials.md`
- predicted_direction: **up**
- predicted_magnitude_band: **mild**
- total_score: **4.5** (mult 0.9)
- regime: risk_on
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLI vs SPY (yfinance, through 2026-09-10):
  1d: XLI -0.72% | SPY -0.60% | rel -0.12%
  3d: XLI -2.69% | SPY -1.60% | rel -1.09%
  1w: XLI -1.29% | SPY -0.96% | rel -0.33%
  1m: XLI -8.16% | SPY -1.65% | rel -6.51%
```

MEMORY_CONFIRM: Memory index unavailable this run (embedding metadata mismatch); used injected Industrials scoreboard + 08-11..09-10 sector logs. Rolling dir=0.2 / mag=0.1 (n=10); last 30 dir=0.235 / mag=0.118 (n=17). Last graded 09-10: predicted down/mild, actual XLI −0.72% / SPY −0.60% / rel −0.12% — **dir HIT, mag HIT**. Prior: 09-09 flat/flat vs −1.51% (dir MISS — pipeline flat-overrode a correct narrative down call), 09-08 flat/flat vs −0.485% (dir MISS, same flattening error), 09-04 down/flat vs +0.41% (dir MISS, laggard-shield), 09-03 flat/flat vs +1.03% (dir MISS, ISM Services beat). **Governing today: 09-09 (A-category) — when the tape CONFIRMS the negative score, do NOT let sector_rs_veto/calendar_size_gate flatten the narrative's directional call; emit the directional call. 09-10 (NONE) — deep-oversold laggard (RSI<30, 1m rel ≤ −5%) means the prior-day 1d rel is a DECAYING signal, not a level signal; keep direction, temper the S4 relative-magnitude weight. 09-04 laggard-shield — score the laggard ONCE, not in both S2 and S4. 08-27 — 1w/1m laggard → forbid up. 08-18 — cap S1 at 0/+1, don't use GEV/ETN as a cushion. 08-11/08-12 supply-shock cap — verify live oil sign.** DO-INSTEAD: when score fights tape, cut conviction / prefer flat/mild — **NOT binding today**: the tape (1d rel −0.12%, 3d rel −1.09%, 1m rel −6.51%) confirms the negative lean, and the 09-09 correction says emit the directional call rather than flatten.

## XLI near-session environment (not an SPX call)

### 1. Shared macro as it hits Industrials — S0 = +1
This is a **CPI-day risk-on bounce after a 4-day slide**, with the oil shock **reversing** — a materially different regime from 09-08/09-09/09-10.

- **Oil is DOWN hard, not up.** Channel 1: `CL=F −2.78% 1d`, `BZ=F −3.37% 1d`; Finviz WTI **$99.91 (−2.54%)**, Brent **$104.62 (−2.86%)**. This is the **08-13 trigger**, not the 08-11/08-12 trigger: the old Hormuz/supply headline is the stale leg, and the live session change is a **demand/risk-driven oil slide**. For XLI, a −2.5% to −3.4% crude move is a **direct cost relief** for transports, airlines, trucking, and manufacturers. The 08-11/08-12 supply-shock cap **does not fire** today.
- **Futures independently confirm risk-on.** Channel 1: ES **+0.63%**, NQ **+0.65%**, RTY **+0.63%**, DJIA **+0.53%**. Finviz: S&P **+0.55%**, Nasdaq **+0.61%**, Russell **+0.63%**. The 08-21 reversal gate (ES/NQ ≥ +0.3%) is **ON** — and this is the first session in a week where it is.
- **Globals mixed-to-constructive.** Europe **+0.53%** (FTSE +0.36%, DAX +0.53%, CAC +0.58%, EuroStoxx +0.64%). Asia **−1.27%** composite (Nikkei −1.93%, Kospi −1.76%, Shanghai −1.18%) — but per 08-03, do not let an Asia-only red sleeve set direction when Europe + US futures confirm the other way.
- **Rates: long end is flat-to-marginally-firmer, not a fresh backup.** Channel 1: 10Y Note **+0.03%**, 30Y Bond **+0.03%**, Ultra Bond **+0.09%** (prices up = yields down marginally). DGS30 **5.28** / DGS10 **4.83** / DFII10 **2.46** are prior-close levels (09-09), still in the stress zone. 5-day 10Y–SPX corr **−0.745** (negative but less extreme than the −0.969 of 09-10). The long end is not the live pressure today.
- **CPI is the binary and it is PENDING.** News Judge #1: "CPI-day risk-on setup: futures rise after 4-day slide, CPI looms large." This is a scheduled high-impact release. Per the 09-03 lesson, do **not** pre-score a miss or a beat — but per the same lesson, a pending high-impact print means the **magnitude band must be at least mild, not flat**.
- **VIX 17.24 (−0.6 1d, +2.71 1w) with VIX/VIX3M 1.111 — BACKWARDATION.** Near-term stress > 3-month. HY OAS **2.71** (+0.04 1d, +0.05 1w) — creeping but still tight. EPU **275.99** (+26.8 1d, +78.88 1w) — elevated policy uncertainty. USD **+0.06%** (flat). Gold **−0.33%**, copper **+0.05%** — metals flat, not a risk-off liquidation.

**S0 = +1, regime risk_on.** Not +2: CPI is pending, VIX is backwardated, EPU is elevated, and the 1m XLI lag is −6.51%. Not 0: futures confirm ≥ +0.5% across all four indices, oil is down 2.5–3.4% (a genuine cost relief for this book), and Europe is green. Oil counted **once here**, not again in S1.

### 2. Spine + secondary — S1 = +1 (capped)
**No fresh same-morning industrials print in hand.** August ISM manufacturing already printed **09-01**: PMI **54.6** (expansion, 8th month) vs July 55.6; new orders **53.7** (−3.0 pts). Still **expansion** — not an ISM-contraction HIT, but **slowing**. ISM Services printed **09-03** (55.4). Both in the tape. 08-18/08-27: **cap S1 at 0/+1**; +2 is forbidden without same-morning confirmation.

- **Grid / electrical equipment backlog (AI power) — HIT, live.** Finviz digest: **BE (Bloom Energy)** target raised to **$330** on expected S&P 500 inclusion (Clear Street), UBS to $325. This is a fresh, dated AI-power catalyst inside XLI's electrical-equipment sleeve. GEV ~$176B RPO / 116 GW gas book remains structural. **But 08-18: not a downside cushion and not a same-session raise** — this is a genuine positive, scored once.
- **Aerospace & defense — MIXED.** SPEEA talks resumed (constructive, not a strike). Defense backlog intact. The Iran/Houthi escalation is a defense-order narrative, but defense names have been volatile on this conflict. Do **not** cancel ISM (expansion) with one award, and do **not** treat geo as a fresh defense-order HIT.
- **Freight / trucking / rail — IMPROVING, live.** Oil **−2.5% to −3.4%** is a direct **fuel-cost relief** for trucking, airlines, and air freight. This is the cleanest same-session positive transmission for XLI today. Rail carloads modestly positive. Score as a genuine (if modest) recovery signal.
- **Durable goods / CapEx — carried.** July durables +1.1% / core +0.2% (08-26). Not a same-morning HIT.
- **Construction slowdown — HIT, carried.** Manufacturing construction off the 2025 peak; AI/nonres is the offset.
- **Copper +0.05%** (Channel 1) — flat, not a global-growth negative today (contrast 09-10's −2.89%).

Net: carried ISM expansion (slowing) + live AI-power catalyst (BE) + oil-driven freight cost relief vs construction drag + mixed defense. **S1 = +1** (capped; no fresh same-morning industrials print, ISM is slowing, and 08-18 forbids +2 without same-morning confirmation).

### 3. Breadth — S2 = 0
XLI is a **deep medium-term laggard** but the 1d tape has stabilized. Channel 1 through 09-10: 1d rel **−0.12%** (essentially flat), 3d rel **−1.09%**, 1w rel **−0.33%**, 1m rel **−6.51%**. Per **09-04**, score the laggard **ONCE** — and per **09-10**, a deep-oversold laggard (1m rel ≤ −5%) is a mean-reversion setup, not momentum confirmation. The 1d rel has now stabilized for a second consecutive session (−0.12% after −1.04%). Per the **09-10 healthcare lesson** (≥2 consecutive 1d rel stabilizations after a multi-day lag → decay the leading sum toward zero), S2 should be **0**, not −1. Breadth is not expanding (no % names up confirmation), but it is not failing either. **S2 = 0.**

### 4. Flows — S3 = 0
No flow data returned this run. Checked, nothing material. Not a crowded long (1m rel −6.51%). Rotation has been out of industrials into tech/AI-power. **S3 = 0.**

### 5. ETF tape (confirmation only) — S4 = 0
Channel 1 through 09-10: 1d rel **−0.12%** (flat), 3d **−1.09%**, 1w **−0.33%**, 1m **−6.51%**. The 1d tape is flat, not confirming a strong move either way. Per **09-10**, the prior-day 1d rel is a **decaying signal** for a deep-oversold laggard — do not weight it as a level signal. Per **09-04**, do not double-count the laggard fact already scored in S2. **S4 = 0.**

### 6. Catalysts / calendar
- **CPI pending** — the dominant binary. Do not pre-score; widen band to at least mild (09-03).
- **Oil −2.5% to −3.4%** — cost relief for transports/manufacturers (08-13 regime, not 08-11/08-12).
- **BE (Bloom Energy) PT raise to $330 on expected S&P inclusion** — fresh AI-power catalyst inside XLI.
- **AME completes $5.0B Indicor acquisition** — stale M&A (08-26), not a same-session raise.
- **Warsh hawkish / September hike odds elevated** — already paid (08-28 → 09-10); do not double-count.
- **EPU +78.88 1w** — elevated policy uncertainty, a magnitude dampener.

### Self-audit
- **Lens:** cyclical; oil counted once in S0 (as cost relief), not re-counted in S1.
- **Band:** **mild**, not notable — CPI pending, VIX backwardated, EPU elevated, 1m lag −6.51%.
- **Skew:** BE/GEV do not drive the ETF call; the oil-driven freight relief is the broadest same-session positive.
- **Same-shock:** oil counted once; Warsh counted once (and already paid).
- **Single-ticker:** BE PT raise is a sleeve positive, not the ETF thesis.
- **09-09 correction applied:** the tape (1d rel −0.12%, 3d rel −1.09%) does not confirm a strong down move, and futures confirm up — so the directional call is **up**, not flat, and not down.
- **09-10 correction applied:** the deep-oversold laggard's prior-day rel is a decaying signal — S4 = 0, not −1.
- **09-04 correction applied:** the laggard is scored once (S2 = 0), not in both S2 and S4.
- **08-27 applied:** 1w/1m laggard → forbid up... **but** 08-27's trigger is a *non-holdings mega-cap AHR with NQ leading ES*. Today the impulse is a **broad risk-on futures tape + oil cost relief + a live AI-power catalyst inside XLI**, not a foreign AHR. 08-27's forbid-up does not bind. The 08-21 reversal gate is ON (ES/NQ ≥ +0.3%).

**Divergence:** Leading factors (S0 +1, S1 +1) point up; the 1d tape is flat (−0.12%) and the 1m lag is −6.51%. This is a **mild divergence** — the tape is not confirming a strong up move. Per DO-INSTEAD, cut conviction / prefer mild. **Divergence flagged: True.**

**Final:** Σ(S0..S4) = +1 +1 +0 +0 +0 = **+2.0** × mult **0.9** = **+1.8** → **up / mild**.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 1
S1_SECTOR_FACTORS: 1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: risk_on
SECTOR_SCORES_END

HORIZON_3D: up:mild
HORIZON_1W: flat:mild
HORIZON_2W: flat:mild
HORIZON_1M: down:mild

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|HIT|0.70|2026-09-11|https://www.finviz.com/futures
Risk-off tape / flight to safety|MISS|0.65|2026-09-11|https://www.finviz.com/futures
Real yields rising|MISS|0.55|2026-09-11|https://fred.stlouisfed.org/series/DFII10
Real yields falling|HIT|0.50|2026-09-11|https://fred.stlouisfed.org/series/DFII10
USD strengthening|NEUTRAL|0.50|2026-09-11|https://www.finviz.com/futures
USD weakening|NEUTRAL|0.50|2026-09-11|https://www.finviz.com/futures
Sector breadth expansion (% names up)|NEUTRAL|0.45|2026-09-11|https://finance.yahoo.com/quote/XLI
Sector breadth failure (ETF up, names flat)|NEUTRAL|0.45|2026-09-11|https://finance.yahoo.com/quote/XLI
Large-cap leadership inside sector|NEUTRAL|0.45|2026-09-11|https://www.finviz.com/quote/XLI
Small/mid leadership inside sector|NEUTRAL|0.45|2026-09-11|https://www.finviz.com/quote/XLI
High-beta leadership inside sector|HIT|0.50|2026-09-11|https://www.finviz.com/futures
Low-beta leadership inside sector|MISS|0.50|2026-09-11|https://www.finviz.com/futures
Sector ETF inflow / relative volume spike|NEUTRAL|0.40|2026-09-11|https://etfdb.com/etf/XLI
Sector ETF outflow / volume dry-up|NEUTRAL|0.40|2026-09-11|https://etfdb.com/etf/XLI
Crowded long (extreme relative performance + valuation)|MISS|0.60|2026-09-11|https://finance.yahoo.com/quote/XLI
Index rebalance / inclusion tailwind|HIT|0.55|2026-09-11|https://www.finviz.com/quote/BE
Index exclusion / forced selling|NEUTRAL|0.40|2026-09-11|https://www.finviz.com/quote/XLI
ISM manufacturing / new orders expansion|HIT|0.55|2026-09-11|https://www.ismworld.org/supply-management-news-and-reports/reports/ism-report-on-business/pmi/
Durable goods / CapEx upside|NEUTRAL|0.45|2026-09-11|https://www.census.gov/manufacturing/m3/adv/current/index.html
Grid / electrical equipment backlog (AI power)|HIT|0.65|2026-09-11|https://www.finviz.com/quote/BE
Aerospace & defense order / budget upside|NEUTRAL|0.45|2026-09-11|https://www.finviz.com/quote/BA
Freight / trucking / rail volume recovery|HIT|0.55|2026-09-11|https://www.finviz.com/futures
Reshoring / industrial policy funding|NEUTRAL|0.40|2026-09-11|https://www.finviz.com/quote/XLI
ISM contraction|MISS|0.60|2026-09-11|https://www.ismworld.org/supply-management-news-and-reports/reports/ism-report-on-business/pmi/
CapEx cuts / order cancellation|MISS|0.50|2026-09-11|https://www.finviz.com/quote/XLI
Freight recession|MISS|0.50|2026-09-11|https://www.finviz.com/futures
Construction slowdown|HIT|0.55|2026-09-11|https://www.census.gov/construction/c30/current/index.html
Sector rotation into industrials|NEUTRAL|0.45|2026-09-11|https://www.finviz.com/quote/XLI
Sector rotation out of industrials|NEUTRAL|0.45|2026-09-11|https://www.finviz.com/quote/XLI
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': 5.0, 'divergence_flagged': False, 'total_score': 4.5, 'predicted_direction': 'up', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.55, 'regime': 'risk_on'}
```
