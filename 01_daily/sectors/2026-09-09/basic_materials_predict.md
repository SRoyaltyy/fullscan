# Sector Prediction — Basic Materials — 2026-09-09

- ETF: **XLB**
- rubric: `00_grounding/sectors/basic_materials.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-4.5** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLB vs SPY (yfinance, through 2026-09-08):
  1d: XLB -0.95% | SPY -0.55% | rel -0.40%
  3d: XLB -1.91% | SPY +0.10% | rel -2.01%
  1w: XLB -1.42% | SPY -0.14% | rel -1.28%
  1m: XLB -1.74% | SPY -0.94% | rel -0.80%
```

I have enough context. Let me synthesize the analysis. Today is 2026-09-09 (Wednesday), a follow-through session after the 09-08 Hormuz/oil risk-off shock. Key facts:

- **Channel 1 tape through 09-08**: 1d XLB −0.95% / SPY −0.55% / rel −0.40%; 3d rel −2.01%; 1w rel −1.28%; 1m rel −0.80%. Decisively negative across all timeframes.
- **09-08 was a risk-off Hormuz/oil shock** (WTI +3.18% to $94.4, Brent +2.16% to $99.1 near $100). Copper hit records but XLB fell −0.95% because the chemicals-heavy book (LIN/SHW/ECL ~40-50%) was dragged by oil cost squeeze while copper miners (FCX ~10-15%) couldn't offset.
- **CPI is Friday 09-11** (not today). Today 09-09 has no 8:30 high-impact print.
- **Gold ~$4,400** (fading on hawkish Fed), silver ~$66 (steady). Copper ~$6.69-6.77 (firm/records).
- **Hormuz/oil still elevated** (~$96 Brent) — the geopolitical overlay persists.
- **China**: August NBS mfg PMI 49.8 (still <50), construction 46.9 — contraction persists.

Let me write the analysis.

MEMORY_CONFIRM: Sector Basic Materials / XLB only. Rolling last-10 dir=0.5 mag=0.7 (n=10); last graded 2026-09-08 flat/flat vs XLB −0.95% / SPY −0.55% / rel −0.40% (dir MISS, mag MISS). Memory index unavailable this run — used injected scoreboard/lessons only. Active XLB rules checked: **8/18 metals-as-floor** (oil spiking on Hormuz into equity risk-off — do NOT use copper/gold as a floor); **8/14 gold-offset** (gold is NOT green — fading ~$4,400); **8/25 composition/transmission** (chemicals-heavy book); **8/27 S4-cap** (1d rel −0.40% <0.5% → S4 cannot be ± confirmation); **8/28 leftover-S2/S4 down-mandate** (S0/S1 live, not net-zero); **09-03 exhaustion-bounce** (sector in mean-reversion chop, 1w rel still negative); **09-04 T-1-lag** (do not copy prior-day lag into S4); **09-08 composition-weighting** (chemicals ~40-50% of XLB vs copper miners ~10-15% — a minority-sleeve copper positive cannot offset a majority-sleeve chemicals cost headwind). No open experiment for `sector_basic_materials`. DO-INSTEAD: prefer flat/mild when score sign conflicts with tape/breadth.

## Analysis — XLB, session of 2026-09-09

This is a **Wednesday follow-through after Tuesday's Hormuz/oil risk-off shock**, not a copper-squeeze day and not a fresh kinetic escalation. Channel 1 tape through 09-08 is **decisively negative across all timeframes**: 1d rel **−0.40%**, 3d **−2.01%**, 1w **−1.28%**, 1m **−0.80%**. Tuesday's session was the live test of the 09-08 composition lesson: copper hit records (FCX +5.35%) but XLB fell **−0.95%** because the chemicals-heavy book (LIN/SHW/ECL ~40-50%) was dragged by the oil cost squeeze. That lesson is now **the binding frame** for today.

### 1. Shared macro as it hits materials (S0)

The Hormuz/oil overlay **persists** but is **not a fresh kinetic increment** today. Brent is still elevated (~$96, near $100) after Tuesday's +2.16% surge, but the escalation is now **T-1** — the shock has already transmitted through Tuesday's close. Per 09-04 T-1-lag, do not re-score Tuesday's oil spike as a fresh same-morning liquidation.

The hawkish Fed backdrop persists: gold fading (~$4,400, down from the $4,600+ peak), silver steady ~$66, real yields elevated (10Y ~4.77%, 30Y ~5.25% stress zone). **CPI is Friday 09-11** — not today's 8:30. No high-impact print today.

Offsets: copper is **firm** (~$6.69-6.77, at/near records on tariff/supply + AI data-center demand). USD is not spiking. But the equity tape is still digesting the oil shock and hawkish Fed.

**S0 = −1.** Persistent Hormuz/oil + hawkish Fed + elevated real yields map negative to this cyclical. Not −2: no fresh kinetic increment, copper firm, CPI is 2 days out (not a same-morning binary), oil is not re-spiking today.

### 2. Spine + secondary (S1)

**Industrial metals — firm, at records.** Copper ~$6.69-6.77/lb, at/near records on tariff/supply + AI data-center demand. Spine "surge" is **partial HIT** — but per 09-08 composition-weighting, this is a **minority sleeve** (~10-15% copper miners in XLB).

**Chemicals — the dominant sleeve, still under oil-cost pressure.** LIN ~13%, SHW, ECL ~40-50% combined. Oil at ~$96 is a **direct feedstock/energy cost headwind** for processors. MAP HEAT Chemicals dir=down (DOW oil-cost drag outweighs HUN/REX positives). This is the **majority-sleeve negative** that dictates the ETF outcome.

**Monetary metals — fade.** Gold ~$4,400 (down from peak), silver ~$66 steady. 8/14 does not pay (gold not green). NEM/gold miners are the exposed sleeve.

**China demand — still the offset.** August NBS mfg PMI **49.8** (still <50), construction **46.9**. Still contractionary, not a rebound.

**S1 = −1.** Chemicals oil-cost drag (majority sleeve) + China contraction + gold fade, net of copper firmness (minority sleeve). Per 09-08, the copper positive cannot offset the chemicals negative. Capped below −2: copper is genuinely firm, not a collapse.

### 3. Breadth (S2)

Compositional split persists: copper miners (FCX) firm on record copper, chemicals (LIN/SHW/ECL) under oil-cost pressure, gold miners (NEM) fade. Not a clean breadth expansion or failure. Per 8/28, do not copy yesterday's lag into S2. **S2 = 0.**

### 4. Flows / positioning (S3)

XLB mild outflows from prior logs (~−$180M 1m range). Not a washout, not a volume spike. **S3 = 0.**

### 5. Tape (S4, confirmation only)

1d rel **−0.40%** is **sub-0.5%** — per 8/27 S4-cap, this cannot be a ± confirmation. It is a modest negative, not decisive. **S4 = 0.**

### Reconciliation

Total = (−1 + −1 + 0 + 0 + 0) × 0.9 = **−1.8** → **down/flat** to **down/mild**.

Given the persistent Hormuz/oil overlay + hawkish Fed + chemicals cost drag (majority sleeve), direction is **down**. But magnitude is capped at **flat/mild** because: (a) copper is firm (minority sleeve buffer), (b) no fresh kinetic escalation today, (c) CPI is 2 days out, (d) 1d rel is only −0.40% (sub-threshold), (e) rolling mag discipline favors mild. I'll emit **down/flat** — the composition buffer (copper firm + chemicals not collapsing) prevents a notable decline, and the sub-0.5% tape does not confirm a decisive move.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: risk_off
TOTAL_SCORE: -1.8
PREDICTED_DIRECTION: down
PREDICTED_MAGNITUDE_BAND: flat
HORIZON_3D: down:mild:0.5
HORIZON_1W: down:mild:0.5
HORIZON_2W: flat:mild:0.45
HORIZON_1M: flat:mild:0.4
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-off tape / flight to safety|HIT|0.7|2026-09-09|Hormuz/oil overlay persists, hawkish Fed
Real yields rising|HIT|0.6|2026-09-09|10Y ~4.77%, 30Y ~5.25% stress zone
Industrial metal price surge (copper/aluminum/iron ore)|HIT|0.6|2026-09-09|Copper ~$6.69-6.77 at/near records
China demand shock / property stress|HIT|0.6|2026-09-09|NBS mfg 49.8, construction 46.9 still contraction
Margin compression / cost inflation without pricing power|HIT|0.6|2026-09-09|Oil ~$96 cost headwind for chemicals-heavy XLB
Gold/silver price surge (monetary metals)|MISS|0.7|2026-09-09|Gold fading ~$4,400, not green
Sector rotation out of materials|HIT|0.5|2026-09-09|Chemicals under oil-cost drag, 3d/1w rel negative
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': -5.0, 'divergence_flagged': False, 'total_score': -4.5, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.55, 'regime': 'risk_off', 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
