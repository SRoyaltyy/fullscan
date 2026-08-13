# Sector Outcome — Utilities — 2026-08-13

Actuals: {'etf': 'XLU', 'pct': 0.4562061182557864, 'spy_pct': 0.6977455651952402, 'rel': -0.24153944693945384, 'open': 43.88999938964844, 'close': 44.040000915527344}

I have enough information to execute the post-session review. Let me compile the analysis.

Key facts from the actuals:
- ETF_PCT: +0.456% (XLU up)
- SPY_PCT: +0.698% (SPY up)
- REL_PCT: -0.242% (XLU underperformed SPY)
- Open 43.89, Close 44.04

The morning prediction was **up/mild**. Actual direction was **up** (XLU +0.46%), so direction HIT. Magnitude: +0.46% is mild (well within mild band). So magnitude also HIT.

However, the relative return was NEGATIVE (-0.24%) — XLU underperformed SPY. The morning read assumed XLU would show relative outperformance (1d rel +0.23% going into the day, 3d rel +0.63% turning positive). Instead, XLU lagged SPY by 0.24%.

Key drivers from research:
- PPI (Aug 13) came in soft/cooling — core PPI +0.2% M/M below consensus, Y/Y cooled to 4.2% from 4.7%. This was another softer inflation report.
- 10Y yield eased to ~4.615-4.68%.
- Risk-on tech-led tape continued — tech outperformance continued per ZeroHedge headline "Tech outperformance continues with yields pressured by soft PPI."
- The soft PPI pressured yields (good for bond-proxy utilities) but the tape was tech-led risk-on, which rotated capital into growth and away from defensives.

So the primary driver: soft PPI pressured yields (supportive of utilities) but the risk-on tech-led tape dominated, causing XLU to underperform SPY. The bond-proxy bid was real but capped by the rotation into growth/tech.

Let me write the review.

---

OUTCOME_BEGIN
SECTOR: Utilities
ETF: XLU
ETF_PCT: 0.456
SPY_PCT: 0.698
REL_PCT: -0.242
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Soft July PPI (core +0.2% M/M, Y/Y cooled to 4.2%) pressured yields and supported the bond-proxy bid, but the risk-on tech-led tape rotated capital into growth, capping utilities' relative performance.
KEY_INTERACTION: Yield relief (positive for bond-proxy) vs. risk-on tech-led rotation (negative for defensives) — the rotation won on a relative basis, leaving XLU positive in absolute terms but -0.24% behind SPY.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Direction HIT (up) and magnitude HIT (mild), but the relative-performance assumption was wrong — the morning read expected XLU to hold modest relative outperformance (+0.23% 1d, +0.63% 3d), whereas it actually underperformed SPY by -0.24%.
OUTCOME_END

---

## Sector Post-Session Review — Utilities (XLU) — 2026-08-13

### STEP 0: FACTS
- **XLU:** +0.456% (open 43.89 → close 44.04)
- **SPY:** +0.698%
- **Relative:** **-0.242%** (XLU underperformed SPY)
- **Path:** Opened 43.89, closed 44.04 — modest grind higher, roughly in line with the mild band.

### STEP 1: What drove the sector today

The dominant macro event was the **July PPI release (Aug 13)**, which came in soft/cooling:
- Core PPI +0.2% M/M, below the 0.3% consensus and slowing from prior 0.4%.
- Y/Y cooled to 4.2% from 4.7%, in line with expectations.
- Headline PPI unchanged M/M, +4.7% Y/Y (down from 5.5%).
- Bloomberg: "US wholesale inflation cools as war-driven energy..." — another softer-than-forecast report.

This pressured yields (10Y eased to ~4.615-4.68%), which is **supportive of the bond-proxy utilities bid** — the exact channel the morning read flagged (S0/S1 rates-falling).

However, the **broad tape was risk-on and tech-led** — ZeroHedge headline: "Tech outperformance continues with yields pressured by soft PPI." Stocks rose near record highs, Nasdaq leading. This is the exact condition the 08-12 Utilities lesson warned about: money rotating into growth/tech, capping the defensive bid.

Net effect: XLU rose in absolute terms (+0.46%) on the yield relief, but **underperformed SPY by -0.24%** because the risk-on rotation into tech/growth dominated the tape. The bond-proxy bid was real but secondary to the growth bid.

**Evidence:**
- CLAIM: July PPI came in soft (core +0.2% M/M below consensus, Y/Y cooled to 4.2% from 4.7%).
  URL: https://www.zerohedge.com/markets/tech-outperformance-continues-yields-pressured-soft-ppi-13th-august-2026
  PUBLISHED: 2026-08-13
  QUOTE: "Core PPI rose 0.2% M/M, below the 0.3% consensus and slowing from the prior 0.4% increase, while the Y/Y rate cooled to 4.2% from 4.7%."
- CLAIM: 10Y yield eased to ~4.615-4.68% on the soft PPI.
  URL: https://www.tradingview.com/symbols/TVC-US10Y/
  PUBLISHED: 2026-08-13
  QUOTE: "The current yield of United States 10 Year Government Bonds is 4.615%."
- CLAIM: Tech outperformance continued on the soft PPI (risk-on tech-led tape).
  URL: https://www.zerohedge.com/markets/tech-outperformance-continues-yields-pressured-soft-ppi-13th-august-2026
  PUBLISHED: 2026-08-13
  QUOTE: "Tech outperformance continues with yields pressured by soft PPI."

### STEP 2: Audit morning S0–S4 reads against reality

| Component | Morning read | Reality | Verdict |
|---|---|---|---|
| **S0_SHARED_MACRO (0)** | Neutral — yield relief positive but risk-on tech-led tape caps it | Correct. Soft PPI pressured yields (positive) but tech-led risk-on dominated (negative). Net neutral was right. | **HIT** |
| **S1_SECTOR_FACTORS (+1)** | Positive but capped — rates-falling + structural policy, dampened by Wood Mackenzie/Texas headwinds | Correct. The bond-proxy bid was real but capped by rotation. The load-growth disappointment signals (Wood Mackenzie, Texas audit) remained active. | **HIT** |
| **S2_BREADTH (+1)** | Mild positive inflection — 3d rel +0.63% turning positive, 1d rel +0.23% modest | **MISS (direction of relative move)**. The morning read assumed the 3d positive inflection would continue. Instead, XLU **underperformed** SPY by -0.24% on the day. The relative leadership did not hold. | **MISS** |
| **S3_FLOWS_POSITIONING (0)** | No confirmed inflows, deeply oversold | Correct — no confirmed flow reversal. | **HIT** |
| **S4_ETF_TAPE (+1)** | Mild confirmation — 1d rel +0.23%, 3d rel +0.63% | **MISS (relative)**. The tape did NOT confirm relative outperformance — XLU lagged SPY. Absolute move was positive but relative was negative. | **MISS** |

**Net:** Direction (up) and magnitude (mild) were both correct. But the **relative-performance read was wrong** — the morning assumed XLU would hold modest relative outperformance, whereas it actually underperformed SPY. The S2 and S4 relative reads were the weak points.

### STEP 3: Interactions / double-count / knowable-at-open test

**Interaction:** The key interaction was **yield relief (positive for bond-proxy) vs. risk-on tech-led rotation (negative for defensives)**. The morning read correctly identified this tension and scored S0 neutral. But it underestimated how strongly the tech-led rotation would dominate on a relative basis. The soft PPI pressured yields, which *should* have helped utilities, but the same soft PPI also fueled the risk-on tech rally — and the tech bid won the relative contest.

**Double-count check:** The morning read kept rates-falling (S0/S1) and load-growth disappointment (S1) as distinct channels. This was correct — no double-count. The Wood Mackenzie/Texas headwind was a separate negative channel that legitimately dampened S1.

**Knowable-at-open test:** The direction (up) and magnitude (mild) were knowable at open — the soft PPI was a scheduled release, and the yield-relief channel was well-established from the prior session. However, the **relative underperformance** was NOT clearly knowable at open. The morning read had XLU 1d rel +0.23% and 3d rel +0.63% going into the day, which suggested the sector was inflecting positively. The risk-on tech-led tape was known, but the degree to which it would dominate the relative contest was uncertain. Verdict: **partially** knowable.

### STEP 4: Outliers inside the sector

No single-name outlier drove the sector move. The move was broad-based — XLU +0.46% in line with the mild band, consistent with a broad bond-proxy bid capped by rotation. No evidence of a specific utility stock (e.g., a data-center-exposed name like VST, CEG, or a regulated utility) driving the day. The sector call was broad-based, as the morning read correctly noted.

---

### Summary

The morning read got the **direction right (up)** and the **magnitude right (mild)** — both HITs. The core thesis (yield relief from soft CPI/PPI supporting the bond-proxy bid, capped by risk-on tech-led rotation) was correct. The soft PPI confirmed the yield-relief channel.

The **weak point** was the relative-performance read. The morning assumed XLU would hold modest relative outperformance (1d +0.23%, 3d +0.63% going in), but it actually **underperformed SPY by -0.24%**. The risk-on tech-led rotation was stronger on a relative basis than the morning read anticipated. This is a lesson worth noting: when the broad tape is risk-on with tech leading, even a yield-relief catalyst for bond-proxy defensives may not translate into relative outperformance — the rotation into growth can dominate the relative contest even while the absolute move is positive.