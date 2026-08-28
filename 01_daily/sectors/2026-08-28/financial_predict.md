# Sector Prediction — Financial — 2026-08-28

- ETF: **XLF**
- rubric: `00_grounding/sectors/financial.md`
- predicted_direction: **down**
- predicted_magnitude_band: **flat**
- total_score: **-2.925** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLF vs SPY (yfinance, through 2026-08-27):
  1d: XLF -0.65% | SPY +0.66% | rel -1.31%
  3d: XLF -0.58% | SPY +1.00% | rel -1.58%
  1w: XLF +1.63% | SPY +1.11% | rel +0.52%
  1m: XLF +2.12% | SPY +5.71% | rel -3.59%
```

MEMORY_CONFIRM: Injected Financial scoreboard used (memory index unavailable). Last 10: dir=0.3 mag=0.2; 08-26 up/mild vs −0.09% MISS; 08-27 up/mild vs −0.65%/rel −1.31% MISS. Binding 08-27 XLF rule: non-holdings NVDA already public + NQ-led tape = rotation-*out* of banks, not an up license; do not triple-count a 3d/1w streak; if S4 is not confirming up, do not emit absolute up. 08-17: do not treat long-end-driven 2s10s as NIM+. 08-18 two-sided long-end rotation does **not** fire (1d rel −1.31%, not ≥+0.4%). 08-21 mag-temper: one band, mild cap. PCE is 08-26 (stale); today’s binary is Warsh JH 10:00 ET, not an 8:30 print. 08-21 green-futures ban-on-down does **not** fire (ES 0.0%, NQ −0.19%).

## XLF — 2026-08-28 (near-session)

Object is **XLF absolute environment**, not SPX and not a stock pick. Channel 1 tape is taken as given.

### Channel 1 (trusted, not re-derived)
- **XLF vs SPY:** 1d −0.65% / rel **−1.31%**; 3d rel **−1.58%**; 1w rel +0.52%; 1m rel **−3.59%**.
- Macro: VIX 14.51 (calm); HY OAS 2.67 and still **tightening**; 10Y 4.66 (+2 bp on the table), 30Y **5.18** (still a stress-zone long end); DFII10 2.34 (+2 bp); ES **0.0%**, NQ **−0.19%**; CL −0.8% / BZ −1.91%; Asia −0.15%, Europe **−0.72%**; F&G 58.2 Greed.
- Live curve check (not just the prior-close table): 10Y ~**4.68%** overnight, 2s10s ~**+47 bp**. Steepening is **long-end / term-premium**, not a short-end-easing NIM gift.

### Channel 2

**1. Shared macro → this sector (curve & credit > equity beta)**  
Not a credit risk-off day (HY tight, VIX 14.5). Also **not** a financials risk-on day: ES flat, NQ slightly red, Europe soft. Sticky July core PCE 3.3% and the hike-repricing are **already in** (08-26/27); do not score them as today’s 8:30. **Warsh Jackson Hole 10:00 ET** is the live two-sided policy binary (first JH as Chair; markets ~33–40% Sep hike). Oil is **down**, so the Hormuz-style risk-off overlay does **not** fire. 08-27 leftover: NVDA/CRM are **not** XLF holdings; do not map residual AI beta into S0+.

**2. Spine (mandatory)**  
| Spine | Read |  
|---|---|  
| 2s10s steepening | **Not a NIM tailwind.** 30Y 5.18 / 10Y ~4.68 is the 08-17 long-end headwind, not bull-steepener. Not inverted either. |  
| Credit spreads | **Tightening** (HY 2.67, −3 bp 1d / −17 bp 1m). No blowout. |  
| NII/NIM | FDIC Q2 NIM 3.32% (+1 bp QoQ) — **carried**, not a same-morning print. |  
| Credit quality | Bank card DQ 2.85% Q2 slightly better; 90+ still elevated. **Mixed, not a spike.** |  
| CRE / funding | CRE DQ 1.53% Q2, modestly easier; no deposit-flight headline. |  

**3. Secondary**  
Live factor is **rotation out of financials** (08-27: only tech of 11 S&P sectors up; XLF −0.65% vs SPY +0.66%; BKX −0.56%). IB/trading “fee boom” is **stale Q2** — do not treat as structural NIM. Idiosyncratic: ALL KBW downgrade (already traded), AON CFO (08-17), Victory/First Eagle $7B AM deal (not an XLF driver). JPM vs GS GSIB fight is noise, not a sector shock.

**4. Breadth / leadership**  
1d and 3d relative are **decisively red**. 1w residual +0.52% is the leftover streak, not live leadership. Large-caps (JPM/BKX) participated in the lag — not ETF-only. 1m rel −3.59% = cyclical laggard vs SPY.

**5. Flows / crowding**  
XLF **outflows**: ~−$494M (08-24), ~−$247M (08-25); Trefis ~**−$2B** past month. July inflows have reversed. That is de-allocation after an extended bid, not a fresh inflow spike.

**6. Catalysts**  
No 8:30 high-impact US print. **Warsh 10:00 ET**, Chicago PMI 9:45, UMich final 10:00. Two-sided policy event **caps magnitude**; it does not authorize an up call into a red 1d/3d tape.

### Lessons applied (not restacked)
- **08-27 Financial:** NVDA already public; do **not** call up off a broken streak. Today S4 is **negative**, not flat → absolute **down**, band **mild**.
- **08-17:** long-end steepener ≠ NIM+. Not scored + in S0 and S1.
- **08-18:** 1d rel −1.31% fails the ≥+0.4% rotation-into-banks trigger.
- **08-21 mag:** one band; rolling mag 0.2 → **mild**, not notable.
- Rotation/fade scored **once** in S2, half in S3, S4 confirmation only.

### Self-audit
Lens = XLF, not SPX. Band = mild (Warsh binary + mag record). No same-shock double-count of PCE or NVDA. ALL/AON/VCTR must not drive the ETF call. Leading (S0–S3) and S4 are **same sign** (soft down) → no divergence; tape confirms factors.

---

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 0
S2_BREADTH: -1
S3_FLOWS_POSITIONING: -0.5
S4_ETF_TAPE: -1
MULTIPLIER: 0.9
CONFIDENCE: 0.52
REGIME: mixed
HORIZON_3D: down:mild:0.48
HORIZON_1W: down:mild:0.50
HORIZON_2W: flat:mild:0.42
HORIZON_1M: flat:mild:0.40
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|MISS|0.70|2026-08-28|https://www.reuters.com/business/anxious-investors-hope-clarity-warshs-fed-plan-jackson-hole-2026-08-26/
Risk-off tape / flight to safety|MISS|0.65|2026-08-28|https://www.reuters.com/business/anxious-investors-hope-clarity-warshs-fed-plan-jackson-hole-2026-08-26/
Real yields rising|HIT|0.55|2026-08-28|https://fred.stlouisfed.org/series/DFII10
Real yields falling|MISS|0.55|2026-08-28|https://fred.stlouisfed.org/series/DFII10
USD strengthening|MISS|0.50|2026-08-28|Channel 1 DXY 1d -0.02%
USD weakening|MISS|0.50|2026-08-28|Channel 1 DXY 1d -0.02%
Sector breadth expansion (% names up)|MISS|0.80|2026-08-28|https://www.thetrading.tools/sector-performance
Sector breadth failure (ETF up, names flat)|MISS|0.70|2026-08-28|https://www.thetrading.tools/sector-performance
Large-cap leadership inside sector|MISS|0.70|2026-08-28|https://www.marketwatch.com/investing/index/bkx/download-data
Small/mid leadership inside sector|MISS|0.55|2026-08-28|https://www.thetrading.tools/sector-performance
High-beta leadership inside sector|MISS|0.60|2026-08-28|https://www.thetrading.tools/sector-performance
Low-beta leadership inside sector|MISS|0.45|2026-08-28|https://www.thetrading.tools/sector-performance
Sector ETF inflow / relative volume spike|MISS|0.75|2026-08-28|https://www.etf.com/sections/daily-etf-flows/daily-etf-flows-soxl-inflows-total-520m
Sector ETF outflow / volume dry-up|HIT|0.75|2026-08-28|https://www.etf.com/sections/daily-etf-flows/daily-etf-flows-tlt-gains
Crowded long (extreme relative performance + valuation)|HIT|0.55|2026-08-28|https://www.trefis.com/data/etfs/XLF
Index rebalance / inclusion tailwind|MISS|0.40|2026-08-28|checked, nothing material
Index exclusion / forced selling|MISS|0.40|2026-08-28|checked, nothing material
Yield curve steepening (NIM tailwind)|MISS|0.80|2026-08-28|https://fred.stlouisfed.org/series/DGS30
Credit spreads tightening|HIT|0.85|2026-08-28|https://fred.stlouisfed.org/series/BAMLH0A0HYM2
Bank NII / NIM beat|HIT|0.45|2026-08-25|https://www.bloomberg.com/news/articles/2026-08-25/us-banking-profits-improved-as-lending-expanded-fdic-reports
Credit quality stable or improving|HIT|0.50|2026-08-28|https://fred.stlouisfed.org/series/DRCCLACBS
Regional bank stress easing|HIT|0.45|2026-08-28|https://fred.stlouisfed.org/series/DRCRELEXFACBS
Capital markets / IB / trading surge|MISS|0.55|2026-08-28|stale Q2; no same-morning IB print
Credit spreads blowing out|MISS|0.85|2026-08-28|https://www.gurufocus.com/economic_indicators/5735/bofa-us-high-yield-index-optionadjusted-spread
Charge-off / delinquency spike|MISS|0.55|2026-08-28|https://fred.stlouisfed.org/series/DRCCLACBS
CRE concentration stress|MISS|0.55|2026-08-28|https://www.mba.org/news-and-research/newsroom/news/2026/07/30/delinquency-rates-for-commercial-properties-decreased-in-the-second-quarter-of-2026
Deposit flight / funding stress|MISS|0.70|2026-08-28|checked, nothing material
Yield curve inversion / flattening hurting NIM|MISS|0.70|2026-08-28|https://yieldcurvestoday.com/
Sector rotation into financials|MISS|0.80|2026-08-28|https://www.thetrading.tools/sector-performance
Sector rotation out of financials|HIT|0.80|2026-08-28|https://www.thetrading.tools/sector-performance
HIT_GRID_END

---

## RESEARCH APPENDIX

**Queries run**
- Jackson Hole Warsh speech Friday August 28 2026 Fed hike odds yields
- XLF financials banks premarket August 28 2026 KBW JPM Goldman
- US 2s10s yield curve 10 year 30 year Treasury August 28 2026
- high yield credit spreads HY IG banks NIM CRE delinquencies August 2026
- XLF ETF flows inflows outflows August 2026 financials rotation
- economic calendar August 28 2026 Jackson Hole Warsh 8:30 data
- KBW bank index XLF vs SPY August 27 2026 sector rotation financials lag tech
- Allstate downgrade Aon CFO Victory Capital First Eagle banks earnings August 2026
- live 10 year Treasury yield August 28 2026 premarket
- consumer credit card delinquencies banks CRE stress August 2026
- memory_search: Financial XLF sector prediction lessons 2026-08-27 2026-08-28 (index unavailable)

**Sources and facts used**

1. Forth / KC Fed — Warsh JH keynote **Fri 2026-08-28 10:00 ET**, prepared text, no Q&A. https://www.forth.news/lists/fed/CeEdnSxqxTWpfD1xiNQyK  
2. Reuters 2026-08-26 — ~33–40% Sep hike odds; 10Y ~4.6–4.7%; Warsh “not constrained by market prices.” https://www.reuters.com/business/anxious-investors-hope-clarity-warshs-fed-plan-jackson-hole-2026-08-26/  
3. Reuters 2026-08-27 — sticky inflation / whether inflation is still the problem. https://www.reuters.com/business/feds-warsh-faces-challenge-whether-inflation-is-problem-or-not-2026-08-27/  
4. Axios 2026-08-27 — Warsh prefers framework over near-term guidance. https://www.axios.com/2026/08/27/warsh-guidance-jackson-hole  
5. ChartExchange / MarketWatch — XLF close **$57.89, −0.635%** (08-27); BKX **186.64, −0.56%**. https://chartexchange.com/symbol/nyse-xlf/ ; https://www.marketwatch.com/investing/index/bkx/download-data  
6. thetrading.tools — 08-27 XLK **+3.16%**, XLF **−0.65%**, SPY **+0.66%**. https://www.thetrading.tools/sector-performance  
7. Yields: 2Y ~4.19–4.23%, 10Y ~4.66–4.68%, 30Y ~5.18–5.19%, 2s10s ~+47 bp; live 10Y ~4.68% on 08-28. https://fred.stlouisfed.org/series/DGS10 ; https://cn.investing.com/rates-bonds/u.s.-10-year-bond-yield-historical-data  
8. HY OAS **2.67%** (08-26). https://www.gurufocus.com/economic_indicators/5735/bofa-us-high-yield-index-optionadjusted-spread  
9. Bloomberg/FDIC 2026-08-25 — Q2 NIM **3.32%**, NI **$90.1B**. https://www.bloomberg.com/news/articles/2026-08-25/us-banking-profits-improved-as-lending-expanded-fdic-reports  
10. FRED CRE DQ **1.53%** Q2; MBA CMBS DQ **4.82%** (down QoQ). https://fred.stlouisfed.org/series/DRCRELEXFACBS  
11. FRED card DQ **2.85%** Q2 (bank metric). https://fred.stlouisfed.org/series/DRCCLACBS  
12. ETF.com — XLF **−$494M** (08-24), **−$247M** (08-25); Trefis ~**−$2.0B** past month. https://www.etf.com/sections/daily-etf-flows/daily-etf-flows-tlt-gains ; https://www.trefis.com/data/etfs/XLF  
13. Investing.com calendar — **no 8:30** high-impact US print 08-28; Chicago PMI 9:45; UMich + payroll benchmark 10:00. https://sslecal2.investing.com/  
14. Reuters 2026-08-27 — JPM/BAC vs GS/MS GSIB capital fight. https://www.reuters.com/legal/transactional/wall-st-banks-turn-each-other-capital-fight-nears-endgame-2026-08-27/  
15. KBW/MarketScreener — ALL downgrade to Underperform, PT $250. https://www.marketscreener.com/news/keefe-bruyette-woods-downgrades-allstate-to-underperform-from-market-perform-adjusts-pt-to-250-f-ce7859d3d98ff023  
16. Bloomberg 2026-08-26 — Victory Capital / First Eagle ~$7B. https://www.bloomberg.com/news/articles/2026-08-26/victory-capital-agrees-to-buy-first-eagle-in-7-billion-deal  

**Not used as same-morning XLF drivers:** NVDA 08-26 AHR (non-holding, already traded 08-27); July PCE (08-26); Q2 IB/trading haul; ALL/AON/VCTR single-names.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': -0.5, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -2.0, 'divergence_flagged': False, 'total_score': -2.925, 'predicted_direction': 'down', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.52, 'regime': 'mixed'}
```
