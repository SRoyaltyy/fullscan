# Sector Outcome — Real Estate — 2026-09-18

Actuals: {'etf': 'XLRE', 'pct': -0.9548203552039447, 'spy_pct': -0.11932509489422927, 'rel': -0.8354952603097154, 'open': 42.59000015258789, 'close': 42.529998779296875, 'source': 'yf_download'}

Memory search is paused (embedding index metadata mismatch). Review uses the injected morning XLRE card, Channel 1 actuals, and live sources — not a post-close rewrite of the 04:45 ET curve.

## 0. Facts

XLRE **−0.955%**, SPY **−0.119%**, relative **−0.835%**. Cash path: open **42.59** → close **42.53** (prior close ~**42.94**). Almost the entire loss was the **gap**; from the cash open the ETF only leaked ~**0.14%**. Direction **down**, magnitude **notable** (~1%).

Morning call was **flat / flat** (S0–S2–S4 = 0, S3 = −0.5, mixed, 09-17 keep-flatten). Premarket XLRE **+0.12%** did **not** survive into the cash open.

**CLAIM:** 10Y went from a flat 4.951% pre-open print to back above 5% in cash hours.  
**URL:** https://www.cnbc.com/2026/09/18/treasury-yields-fed-rates-volatile-week.html  
**PUBLISHED:** 2026-09-18 (fetched 2026-09-18T21:38:25Z)  
**QUOTE:** “The yield on the benchmark 10-year Treasury note climbed back above the 5% level, rising more than 5 basis points to 5.006%. … 2-year … 4.76% … 30-year … 5.331%.”  
**SUMMARY:** Same-session backup vs the morning CNBC 04:45 ET snapshot (10Y **4.951% flat**, 30Y **5.286%, −1 bp**).

**CLAIM:** Equity REITs sold off with the curve, not with SPX beta.  
**URL:** https://x.com/HoyaCapital/status/2101045323808358849  
**PUBLISHED:** 2026-09-18, 8:26 PM  
**QUOTE:** “S&P 500: +0.1% … Equity REITs: −1.0% … Housing Index: −1.1% … 2-Year: 4.75% (+9 bps) 10-Year: 5.00% (+7 bps) 30-Year: 5.33% (+5 bps)”  
**SUMMARY:** Parent duration tape, housing weaker than XLRE; matches Channel 1 XLRE −0.95% / rel −0.84%.

**CLAIM:** Broad tape was mixed, not a crash; majority of names still fell as 10Y tagged 5.00%.  
**URL:** https://wtop.com/national/2026/09/how-major-us-stock-indexes-fared-friday-9-18-2026/  
**PUBLISHED:** 2026-09-18  
**QUOTE:** “The S&P 500 rose 0.2% Friday. The Dow … slipped 0.2%, and the Nasdaq … added 0.4%. The majority of stocks on Wall Street fell … 10-year Treasury climbed to 5.00%.”  
**SUMMARY:** NQ/XLK beta ≠ REIT bid. SPY actual **−0.12%** (injected) vs cash SPX **+0.2%** — use Channel 1 for scoring.

**CLAIM:** 09:15 ET industrial production was a miss, not a REIT spine print.  
**URL:** https://seekingalpha.com/news/4644334-industrial-production-growth-stalls-in-august-falling-short-of-expectations  
**PUBLISHED:** 2026-09-18, 9:17 AM ET  
**QUOTE:** “U.S. industrial production growth came in at 0.0% M/M in August, missing the +0.3% consensus … Manufacturing production slipped 0.3% M/M”  
**SUMMARY:** Two-sided growth print; not the duration object.

## 1. What drove the sector

Taxonomy: **rates rising / REIT duration selloff** — the S1 spine the morning marked **MISS at 04:45** because the live curve was flat-to-−1 bp. In cash hours that spine **HIT**: 10Y **+5 to +7 bp** through 5%, 30Y **+3 to +5 bp** to **~5.33%**, still ≥5.15% stress. No refinancing window, no cap-rate compression, no FTS bid into REITs.

Secondary, not the parent: oil still **offered** (Hoya crude **$99.29, −2.6%**) — 08-11 spike stayed off; the slide did **not** buy duration relief once the curve backed up (08-25). FOMC/Warsh/Goldman-October remained **one already-printed path** (T+2); they were not a second shock. IP 0.0% / Bowman stress-test speech were two-sided and not the REIT spine.

Relative: XLRE vs SPY **−0.84%**, almost a carbon copy of Thursday’s **−0.83%**. Defensives were the **funding source** on a mixed/Nasdaq-green tape — the MACRO MAP object, now with a live curve.

## 2. Audit of morning S0–S4 (use morning numbers)

| Sleeve | Morning | Reality | Verdict |
|---|---|---|---|
| **S0 = 0** | T+2 digestion, live curve not rising, leftover ES=F +1.14% not an up vote, Finviz ES +0.20% not the 09-11 ≥+0.5% branch | Cash session **was** a rates-backup; SPY flat-to-red, NQ green | **Correct at the open snapshot, incomplete for the session.** Unsigned S0 assumed the 4.951%/5.286% print would hold. It didn’t. |
| **S1 = 0** | Rates-falling MISS; rates-rising MISS at open; real-yield 1w/1m backdrop not a second copy; EQIX/PLD/office nested only | Duration **did** sell; WELL (nested *up* in MAP HEAT) led **down** | **Open-true, session-false.** Spine should have been “curve still in stress, unsigned until live backup” with 09-04 skew **armed as a watch**, not fully off. |
| **S2 = 0** | Property-type split, not parent breadth | Equity REITs **−1.0%**, housing **−1.1%** — coordinated duration, not WELL-vs-EQIX dispersion | **Missed the conversion from nested split → parent down-breadth.** |
| **S3 = −0.5** | 5d −$123.5M / 1m −$315M leak, not a crowded unwind | Right **sign**, too small vs −0.95% | **Direction HIT, size too timid.** |
| **S4 = 0** | 09-14 PM +0.12% unused; 09-11 forbids restacking 1d rel −0.83%; trust factors over tape | Cash **gapped** to 42.59 (~**−0.8%** vs 42.94); PM was stale | **Right not to score PM as up. Wrong that S4 stayed 0 after treating confirmation as empty.** The live tape object at the cash open was the **gap**, not Thursday’s rel. |

**09-17 keep-flatten:** Did what it was written to do — blocked promoting leftover ES=F +1.14% into **up**. It also blocked **down** because 09-11 demanded a *live* negative at the open. The live negative arrived as (a) the **cash gap** vs a +0.12% PM and (b) the **intraday 10Y re-break of 5%**. Flatten was process-compliant and still a **dir/mag miss**.

**09-04 asymmetric-downside:** Morning said it does not fully fire (curve not rising; binary paid). Fair at 04:45. The week-high **5.041%** plus 30Y still in the stress zone was the reason a **flat/mild-down skew** was more honest than unsigned flat.

## 3. Interactions / double-count / knowable-at-open

- **One rate object:** FOMC + Warsh + Goldman-October + DFII10 1w were correctly counted **once** as mixed S0 = 0. Do **not** restack them as the Friday driver. Friday’s incremental object is the **same-session curve backup**, which was **not** on the 04:45 board.
- **Oil vs duration:** Oil slide ≠ relief. 08-25 held. No double-count.
- **NQ green ≠ REIT bid:** 08-27 held. Interaction is **beta funding out of bond proxies**, not a second macro.
- **Lag vs factors:** Divergence flag (leading sum ~0 vs 1d rel −0.83%) was the right soft spot. “Trust factors over tape” then **zeroed S4** instead of treating the lag as **confirmation of a still-stressed duration overlay**. That is the process error — not double-counting the lag into S2 **and** S4, but **refusing to let confirmation exist at all**.
- **Knowable at open:** **Partially.** Knowable: 30Y ≥5.15%, 10Y already tagged 5.04% this week, no duration relief, PM unconfirmed, defensives as funding source, structural rel lag. **Not** knowable at 04:45: +5–7 bp through 5%, WELL −1.5%, cash gap vs PM.

## 4. Outliers inside the sector

Morning MAP HEAT: healthcare / hotel / residential **up**; office / mortgage / specialty (EQIX) **down**; industrial **flat**. `size_gate=True`.

| Name | ~XLRE wt | Day | vs parent | Note |
|---|---|---|---|---|
| **WELL** | ~11.6% | **~−1.46%** | worse | Nested *up* captain **failed**; largest weight dragged the ETF |
| **PLD** | ~9.0% | **~−0.25%** | better | Industrial quality held; MAP HEAT flat was closer |
| **EQIX** | ~7.1% | **~−0.45%** | better | Nested *down* overstated the same-day hit |
| **AMT** | ~5.8% | **~−0.90%** | in line | Duration, not a special |
| **BXP** | small | **~−0.5%** | better | Office nested down did **not** set XLRE (size_gate held) |

**CLAIM:** WELL closed $228.87, −1.46%.  
**URL:** https://www.financecharts.com/stocks/WELL/summary/price  
**PUBLISHED:** as-of 2026-09-18 close  
**QUOTE:** Close $228.87, prior $232.26, −1.46%.  
**SUMMARY:** Healthcare bid in the morning nested heat inverted; WELL was the parent outlier to the downside.

Housing index **−1.1%** (Hoya) also inverted the residential nested-up read. Dispersion was **quality industrial / data-center holding up vs healthcare/housing duration**, not the morning split.

---

OUTCOME_BEGIN
SECTOR: Real Estate
ETF: XLRE
ETF_PCT: -0.9548
SPY_PCT: -0.1193
REL_PCT: -0.8355
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Same-session long-end backup (10Y back through 5%, ~+5–7 bp; 30Y to ~5.33%) hitting REIT duration after a T+2 unsigned open.
KEY_INTERACTION: Mixed/Nasdaq-green beta funded out of rate-sensitives; XLRE lagged SPY ~0.84% for a second session — duration + relative lag, not a restacked FOMC copy.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: 09-11/09-17 flatten was open-snapshot compliant (live curve not rising, PM unused) but missed the cash gap and the 10Y re-break of 5%; unsigned S0/S1 underweighted a still-stressed 30Y.
OUTCOME_END

---

## RESEARCH APPENDIX

**Queries run**
- memory_search: Real Estate XLRE sector prediction lessons rates duration 2026-09 *(index unavailable)*
- web_search: XLRE real estate ETF September 18 2026 performance REITs Treasury yields
- web_search: US 10 year 30 year Treasury yield September 18 2026 XLRE REIT
- web_search: S&P 500 SPY close September 18 2026 stock market industrial production
- web_search: Welltower Prologis Equinix American Tower BXP REIT stock September 18 2026
- web_search: US industrial production August 2026 Bowman Fed September 18
- web_search: site:stocknear.com XLRE history September 2026
- web_search: Hoya Capital REIT daily recap September 18 2026 10-year yield
- web_search: US 10-year Treasury yield close September 18 2026 5.006
- web_search: Welltower WELL stock price September 18 2026 close percent
- web_search: XLRE holdings WELL PLD EQIX AMT percentage September 2026
- web_search: Prologis PLD Equinix EQIX American Tower AMT Boston Properties BXP September 18 2026 percent change
- web_search: Michelle Bowman stress testing speech September 18 2026 rates
- x_search: XLRE REIT real estate sector Treasury yields September 18 2026 (2026-09-18 to 2026-09-19)
- web_fetch: https://www.cnbc.com/2026/09/18/treasury-yields-fed-rates-volatile-week.html
- web_fetch: https://wtop.com/national/2026/09/how-major-us-stock-indexes-fared-friday-9-18-2026/
- web_fetch: https://x.com/HoyaCapital/status/2101045323808358849
- web_fetch: https://seekingalpha.com/news/4644334-industrial-production-growth-stalls-in-august-falling-short-of-expectations
- web_fetch: https://stocknear.com/etf/XLRE/history *(403)*
- web_fetch: https://www.reuters.com/business/us-manufacturing-output-falls-august-after-rising-seven-straight-months-2026-09-18/ *(401)*
- web_fetch: https://www.marketscreener.com/news/us-treasury-closing-levels-ce785adadc81f525 *(403)*
- web_fetch: https://www.financecharts.com/etfs/XLRE/summary/price *(403/captcha)*
- read: `/home/gha/.openclaw/workspace/00_grounding/sectors/real_estate.md` *(ENOENT)*

**Key sources (title + URL + timestamp / as-of)**
- Injected Channel 1 actuals — XLRE **−0.9548%**, SPY **−0.1193%**, rel **−0.8355%**, O/C **42.59 / 42.53** — 2026-09-18 session
- Morning XLRE prediction card — 2026-09-18 pre-open: flat/flat, S0=0, live 10Y **4.951%**, 30Y **5.286% (−1 bp)**, PM XLRE **+0.12%**
- CNBC — “Treasury yields move higher as volatile week wraps up” — https://www.cnbc.com/2026/09/18/treasury-yields-fed-rates-volatile-week.html — fetched 2026-09-18T21:38:25Z — 10Y **5.006% (+>5 bp)**, 2Y **4.76%**, 30Y **5.331%**
- Hoya Capital — Daily REITBeat Late Edition — https://x.com/HoyaCapital/status/2101045323808358849 — 2026-09-18 8:26 PM — Equity REITs **−1.0%**, Housing **−1.1%**, 10Y **5.00% (+7 bp)**, 30Y **5.33% (+5 bp)**
- WTOP — How major US stock indexes fared Friday 9/18/2026 — https://wtop.com/national/2026/09/how-major-us-stock-indexes-fared-friday-9-18-2026/ — fetched 2026-09-18T21:28:52Z — SPX **+0.2%** to 7,650.50, Nasdaq **+0.4%**, Dow **−0.2%**, 10Y **5.00%**
- Seeking Alpha — Industrial production stalls in August — https://seekingalpha.com/news/4644334-industrial-production-growth-stalls-in-august-falling-short-of-expectations — 2026-09-18 9:17 AM ET — IP **0.0%** vs **+0.3%**, manufacturing **−0.3%**
- FinanceCharts / search — WELL close **$228.87 (−1.46%)** — https://www.financecharts.com/stocks/WELL/summary/price
- Stocknear (via search; direct fetch 403) — XLRE Sep 17 close **$42.94**; Sep 18 ~**$42.52 (−0.98%)** — https://stocknear.com/etf/XLRE/history
- Fed Bowman remarks (via search) — stress-testing speech, not a rates signal — https://www.federalreserve.gov/newsevents/speech/bowman20260918a.htm — 2026-09-18
- TradingView/TradeSmith (via search) — XLRE weights WELL ~11.6%, PLD ~9%, EQIX ~7%, AMT ~5.8%

**Facts taken**
- Channel 1 actuals are the scoring tape (not cash SPX +0.2%).
- Morning 04:45 ET curve was **flat**; cash curve **backed up through 5% / 5.33%**.
- XLRE path was a **gap-down then flat-to-leak**, not a green-open smash.
- Relative lag **−0.84%** rhymed with Thursday **−0.83%**.
- Nested heat inverted: WELL/housing worse, EQIX/PLD better.
- IP miss and Bowman speech were **not** the parent driver.
- Oil remained offered; duration still sold — 08-25 holds.
- FOMC/Warsh/Goldman stayed one printed path; Friday’s incremental object was the **live curve**.