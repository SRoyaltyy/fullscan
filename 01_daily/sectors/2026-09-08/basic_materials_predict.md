# Sector Prediction — Basic Materials — 2026-09-08

- ETF: **XLB**
- rubric: `00_grounding/sectors/basic_materials.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **1.125** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLB vs SPY (yfinance, through 2026-09-08):
  1d: XLB +0.21% | SPY -0.35% | rel +0.56%
  3d: XLB -0.76% | SPY +0.30% | rel -1.06%
  1w: XLB -0.27% | SPY +0.06% | rel -0.32%
  1m: XLB -0.59% | SPY -0.75% | rel +0.16%
```

I have sufficient context. Let me finalize the analysis and scores.

**Summary of the call:** This is a **risk-off session driven by a fresh, escalating Hormuz/oil supply shock** (WTI +3.18% to $94.4, Brent +2.16% to $99.1 near $100, US-Iran strikes intensifying). Broad equity tape is decisively risk-off (ES −0.44%, DJIA −0.91%, Russell −0.63%). However, copper is at records and GREEN (+1.31%), USD is weakening (−0.37%), and XLB is already outperforming SPY (+0.56% rel on the day). The tension: risk-off equity tape + hawkish Fed backdrop (real yields elevated) vs copper/commodity bid + USD weakness.

Per 8/18, on geopolitical/oil risk-off days metals can co-move with equities — but today copper is genuinely green, so that pattern is not cleanly firing. The chemicals-heavy XLB book faces an oil cost headwind while copper miners get support. Net: offsetting forces → flat.

MEMORY_CONFIRM: Sector Basic Materials / XLB only. Rolling last-10 dir=0.5 mag=0.7 (n=10). Active XLB rules checked: temper-severe does not fire (not building severe); 8/17 China-miss severe ban off (no same-morning China print); **8/18 metals-as-floor is the key rule** — oil spiking on Hormuz into equity risk-off, do NOT use copper/gold as a floor; but copper is genuinely green today (+1.31%), so the co-move-down pattern is not cleanly firing; 8/14 gold-offset OFF (GC −0.84%); 8/25 composition/transmission OFF as up-ban (NQ −0.16% vs ES −0.44%, NQ not >> ES); 8/27 S4-cap OFF (1d rel +0.56% confirmation-eligible); 8/28 leftover-S2/S4 down-mandate OFF (S0/S1 live); 09-03 exhaustion-bounce rule applies (sector in mean-reversion chop); 09-04 T-1-lag rule applies (do not copy prior-day lag into S4). No open experiment for `sector_basic_materials`. DO-INSTEAD: prefer flat/mild when score sign conflicts with tape/breadth.

## Analysis — XLB, session of 2026-09-08

This is a **Tuesday risk-off session driven by a fresh, escalating Hormuz/oil supply shock**, not a copper-squeeze day and not a leftover-chemicals fade. Channel 1 tape through 09-08: 1d XLB **+0.21%** / SPY **−0.35%** / rel **+0.56%**, 3d rel **−1.06%**, 1w **−0.32%**, 1m **+0.16%**. The 1d print is **modestly positive** — XLB is outperforming SPY on a risk-off day, driven by copper strength.

### 1. Shared macro as it hits materials (S0)

The dominant live driver is the **escalating Hormuz/oil supply shock**: WTI **+3.18%** to $94.4, Brent **+2.16%** to $99.1 (near $100), US-Iran strikes intensifying in the Strait of Hormuz, oil at six-week highs, fourth straight day up. This is a **fresh kinetic increment**, not leftover. Broad equity tape is **decisively risk-off**: ES **−0.44%**, NQ **−0.16%**, Russell **−0.63%**, DJIA **−0.91%**. Asia composite **−0.48%**, Europe **−0.14%**.

The hawkish Fed backdrop persists (real yields elevated: DFII10 **2.42**, 10Y **4.77**, 30Y **5.25**), but the oil spike is now the more immediate inflation driver. VIX **15.49** (elevating, +0.19 1d), VIX/VIX3M **0.851**.

Offsets that map to this sector:
- **Copper is GREEN +1.31%** to $6.77/lb, at/near record highs on tariff/supply concerns + AI data-center demand. This is a genuine commodity bid, not co-moving down with equities.
- **USD weakening** (DXY **−0.37%** 1d) — commodity tailwind.
- **Gold −0.84%**, silver −0.39% — monetary metals fading on hawkish Fed, not green.

Per 8/18, on geopolitical/oil risk-off days, metals can co-move with equities. But today copper is genuinely green, so that co-move-down pattern is **not cleanly firing**. The oil spike is a **cost headwind for the chemicals-heavy XLB book** (LIN/SHW/ECL processors), not an XLB-wide squeeze.

**S0 = −1.** Risk-off equity tape (ES −0.44%, DJIA −0.91%) + hawkish Fed backdrop map negative to this cyclical. Not −2: copper is green, USD weakening, oil spike is a commodity-positive for the complex even as it's a cost headwind for processors.

### 2. Spine + secondary (S1)

**Industrial metals — SURGE, at records.** COMEX copper **+1.31%** to $6.77/lb; LME record $14,533 on tariff/supply concerns + AI data-center demand. Spine "surge" is **HIT** — this is a genuine copper bid, not a fade.

**Inventory — tight.** LME copper stocks still low after the mid-August rebuild; record backwardation intact. Spine "inventory draw" is **partial HIT**.

**China demand — still the offset.** August NBS mfg PMI **49.8** (still <50, improved from 49.2), construction **46.9**. Still contractionary, not a rebound. **Do not let copper cancel China.**

**Monetary metals — FADE.** GC **−0.84%**, SI **−0.39%**. 8/14 does not pay (gold not green). NEM/gold miners are the exposed sleeve.

**Oil spike = cost headwind for chemicals.** The chemicals-heavy XLB book (LIN ~13%, SHW, ECL) faces higher feedstock/energy costs from the oil surge. This is a **margin compression** pressure, not an XLB-wide metals squeeze.

**S1 = +1.** Copper at records + inventory tightness, net of China contraction + gold fade + oil cost headwind for chemicals. Capped below +2: China still <50, gold fading, oil is a cost not a metals squeeze.

### 3. Breadth (S2)

Compositional split: copper miners (FCX) benefit from record copper, chemicals (LIN/SHW/ECL) face oil cost headwinds, gold miners (NEM) fade. Not a clean breadth expansion or failure. **S2 = 0.**

### 4. Flows / positioning (S3)

XLB mild outflows from prior logs (~−$180M 1m range). Not a washout, not a volume spike. **S3 = 0.**

### 5. Tape (S4, confirmation only)

1d rel **+0.56%** — modestly positive, confirmation-eligible (XLB outperforming SPY on a risk-off day via copper strength). **S4 = +0.5.**

### Reconciliation

Total = (−1 + 1 + 0 + 0 + 0.5) × 0.9 = +0.45 → **flat/flat** with a mild up tilt. The risk-off equity tape (S0 −1) is offset by the copper surge + XLB outperformance (S1 +1, S4 +0.5). Given the broad risk-off tape is decisive but copper is genuinely green and XLB is already outperforming, the net is a **flat** day — the offsetting forces (risk-off vs copper/commodity bid) cancel.

I'll emit **flat/flat** — the copper surge supports XLB on a risk-off day, but the oil cost headwind on chemicals + China contraction + hawkish Fed cap any upside. The 8/18 lesson warns against using copper as a floor on oil-shock days, but copper is genuinely green today, so I won't force a down call.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: 1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0.5
MULTIPLIER: 0.9
CONFIDENCE: 0.5
REGIME: risk_off
TOTAL_SCORE: 0.45
PREDICTED_DIRECTION: flat
PREDICTED_MAGNITUDE_BAND: flat
HORIZON_3D: flat:mild:0.5
HORIZON_1W: flat:mild:0.5
HORIZON_2W: flat:mild:0.5
HORIZON_1M: flat:mild:0.5
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-off tape / flight to safety|HIT|0.7|2026-09-08|ES −0.44%, DJIA −0.91%, Russell −0.63% on Hormuz/oil shock
Industrial metal price surge (copper/aluminum/iron ore)|HIT|0.7|2026-09-08|COMEX copper +1.31% to $6.77/lb, LME record $14,533
USD weakening|HIT|0.6|2026-09-08|DXY −0.37% 1d
China demand shock / property stress|HIT|0.6|2026-09-08|August NBS mfg PMI 49.8 still <50, construction 46.9
Gold/silver price surge (monetary metals)|MISS|0.7|2026-09-08|GC −0.84%, SI −0.39%, fading on hawkish Fed
Margin compression / cost inflation without pricing power|HIT|0.5|2026-09-08|Oil +3.18% WTI cost headwind for chemicals-heavy XLB book
Sector ETF inflow / relative volume spike|MISS|0.6|2026-09-08|XLB mild outflows, no volume spike
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.5}, 'multiplier': 0.9, 'leading_sum': 1.0, 'divergence_flagged': False, 'total_score': 1.125, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.5, 'regime': 'risk_off', 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': -0.73, 'w1': -1.11}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
