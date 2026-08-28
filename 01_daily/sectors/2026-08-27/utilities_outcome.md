# Sector Outcome — Utilities — 2026-08-27

Actuals: {'etf': 'XLU', 'pct': -0.7584418043843133, 'spy_pct': 0.6552786111251541, 'rel': -1.4137204155094674, 'open': 43.220001220703125, 'close': 43.18000030517578}

Memory search is paused (index was built with a different embedding setup). This review uses the injected morning note, deterministic actuals, and live sources.

OUTCOME_BEGIN
SECTOR: Utilities
ETF: XLU
ETF_PCT: -0.7584
SPY_PCT: 0.6553
REL_PCT: -1.4137
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Tech-led risk-on rotation after overnight NVDA/CRM prints; the bond-proxy/easing bid did not fire.
KEY_INTERACTION: Growth tape (SPY +0.66%) crowded out duration-sensitive utilities while S0/S1 double-counted the same stale yield-easing fact and treated PCE as a same-day event after it had already printed.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Direction and magnitude miss — scored a live easing/PCE up-bid on a post-PCE, post-mega-cap-earnings morning that was actually a defensive-lag day.
OUTCOME_END

## 0. Facts

Trusted Channel 1 actuals for **2026-08-27**:

| | |
|---|---|
| XLU | **−0.76%** (open 43.22 → close 43.18) |
| SPY | **+0.66%** |
| Relative | **−1.41%** |
| Path | Prior close implied ~43.51; **gapped down** to 43.22 (−0.67% vs prior), then grinded: high ~43.33 / low ~42.92 / close 43.18. Weak from the open, not a late-day collapse. |

**Predicted:** up / notable (score 7.5, conf 0.55).  
**Actual:** down / mild. **Direction miss. Magnitude miss** (notable vs mild). Absolute move is a garden-variety down day; the **relative** −1.41% is the sharper miss.

## 1. What drove the sector

Taxonomy, in order:

1. **Risk-on rotation away (S0 / “rates falling” inverse)** — **HIT, dominant.** NVDA and CRM reported **after the 8/26 close**. On 8/27 NVDA ~**+8.7%**, CRM ~**+22.6%**, software/tech led, XLK ~**+3.3%**, SPY **+0.66%**. Dow Jones utilities roundup: power producers fell as traders **rotated out of defensives into tech** on Nvidia’s outlook. That is the 08-12 pattern the morning said was **not** firing.

2. **Rates falling / bond-proxy bid (S1)** — **MISS on the day.** Morning treated 10Y ~4.64 / real-yield easing as a **confirmed live easing regime**. Session close: 10Y **~4.67%** (slight backup vs the morning 4.64 print, not a fresh −6 bp impulse). No same-day duration tailwind.

3. **PCE as same-day two-sided catalyst (S0)** — **calendar error.** July PCE was released **Wednesday 8/26 8:30 ET**, not Thursday 8/27. Print: headline PCE **+0.2% m/m / +3.7% y/y** (both 0.1 pp above consensus); core **+0.2% / +3.3% in line**. Income +0.4%, spending +0.2%. That was a **mildly hawkish-to-in-line** print already in the rear-view, not a cool surprise still ahead.

4. **Data-center / AI-power (S1 structural)** — **stale, as morning said.** Did not drive the ETF. If anything, NVDA strength pulled **capital toward chips/software**, not regulated utilities.

5. **Breadth / tape (S2/S4)** — 1d/3d relative bounce was **carried**, then reversed. Session internals were **broadly red** among rate-sensitive names.

**Primary driver in one line:** growth-led rotation after overnight AI/software earnings, with no fresh easing to offset it.

## 2. Audit of morning S0–S4 (use morning numbers, do not rewrite them)

Morning scores: S0 **+1**, S1 **+1**, S2 **+1**, S3 **0**, S4 **+1**, mult 1.0 → pipeline **up / notable**. Analyst write-up wanted magnitude **capped at mild**; pipeline still printed notable.

| Sleeve | Morning | Reality 8/27 | Verdict |
|---|---|---|---|
| **S0 shared macro** | +1 on easing yields, “PCE today” with cool expectations, oil down, EPU drop, “not a strong growth-led rotation” (ES +0.31% / NQ +0.55% premarket) | PCE **already out** (headline slightly hot, core in-line). Overnight NVDA/CRM made it a **strong** tech tape. 10Y did not ease further. | **Too positive.** Should have been **0 or −1** (risk-on away from defensives). |
| **S1 sector factors** | +1 on rates/real-yields falling as **the** key driver; AI-power correctly marked stale | Bond-proxy bid **did not print**. Yields ~flat-to-up. No rate-case/load catalyst. | **Too positive.** Same yield fact as S0. |
| **S2 breadth** | +1 on 1d rel +0.44% / 3d rel +1.68% “durable inflection” | That was a **2-day bounce after a smash**, not same-day expansion. 8/27 names mostly down. | **Overstated.** 08-13: S2 is confirmation only. |
| **S3 flows** | 0 (carried 5d −$190M / 1m −$236M, no inflow spike) | No evidence of a same-day inflow rescue. Volume ~16M, not a squeeze. | **OK.** |
| **S4 ETF tape** | +1 on 1d/3d; 1w −1.16% / 1m −4.42% still ugly | Leading 1d/3d inflection **failed** the next session. 1w/1m underperformance was the durable tape. | **Overstated.** Internal “mild divergence” was the right instinct; pipeline `divergence_flagged: False` buried it. |

**Lessons vs reality:**

- **08-11** (don’t keep calling down when yields ease and tape inflects): morning applied it. Wrong day — the inflection was a **2-day bounce**, not a new regime.
- **08-12** (cap mild on tech-led tape + sector headwind): morning **explicitly dismissed** it (“mildly risk-on, not strong”). Overnight NVDA/CRM **was** the strong growth-led tape. This was the lesson that should have dominated.
- **08-14** (scan 8:30 ET high-impact): they scanned, then **put PCE on the wrong day**.
- **08-17** (carried defensive bid with no fresh catalyst = relative, not absolute): **should have fired**. No fresh XLU catalyst.
- **08-18** (risk-off + rising yields → relative beat / flat-to-neg absolute): trigger did not fire (tape was risk-on). Correctly not applied.
- **08-21** (don’t score easing off stale FRED): they argued easing was “confirmed.” Even if 8/25 FRED was real, **it was not the 8/27 impulse**.
- **08-25** (don’t manufacture down from carried negatives when S0/S1 are neutral): they used this to **justify flipping S0/S1 to +1**. That over-corrected the last three **up-call losses** into another up call on a day whose live driver was tech.

## 3. Interactions / double-count / knowable-at-open

**Double-count:** S0 +1 and S1 +1 were the **same** “yields easing / real yields −12 bp 1m” shock, labeled “macro vs bond-proxy.” Morning self-audit claimed they were distinct. They were not. One easing fact cannot pay twice.

**PCE vs tech:** Morning made PCE the load-bearing **same-day** binary and treated NVDA/CRM as a modest premarket risk-on. By the open of 8/27, PCE was **yesterday’s** print and NVDA/CRM were **overnight**. Those two facts **interact**: in-line/slightly hot PCE does not deliver a duration bid, and a mega-cap AI/software melt-up **pulls** away from bond proxies. Scoring both as net **up** for XLU was incoherent.

**Knowable at open: partially.**

- **Knowable:** PCE dated 8/26, not 8/27. NVDA and CRM already reported. No fresh utility rate-order/load print. 1w/1m XLU still deeply negative vs SPY. 08-12/08-17 were on the books.
- **Not fully knowable:** modest ES/NQ premarket (+0.31% / +0.55%) did not advertise XLK +3% / NVDA +9% / CRM +23%. Rotation **intensity** was a same-day surprise; rotation **direction** was not.

A correct open stance was **flat-to-down / mild**, not **up / notable**. Pipeline magnitude (notable) was an extra error on top of the direction error.

## 4. Outliers inside the sector

Regulated / rate-sensitive names tracked XLU lower; the AI-power/nuclear name did **not**:

- NEE ~**−0.89%**
- DUK ~**−0.86%**
- SO ~**−0.79%**
- PEG ~**−0.90%**
- XEL ~**−0.67%**
- AEP ~**−0.53%**
- **CEG ~+1.03%** (outlier)

That split is the tell: **duration/rotation**, not a sector-wide power-demand bid. CEG’s up day did not save XLU because NEE/SO/DUK dominate the ETF. AEP was not the driver (correctly not used as an ETF call in the morning).

---

### Evidence

CLAIM: July PCE released 8/26, not 8/27; headline +0.2% m/m / +3.7% y/y; core +0.2% / +3.3%.  
URL: https://www.bea.gov/news/2026/personal-income-and-outlays-july-2026  
PUBLISHED: 2026-08-26 (BEA “released today”; next release 2026-09-30)  
QUOTE: “From the preceding month, the PCE price index for July increased 0.2 percent. Excluding food and energy, the PCE price index also increased 0.2 percent. From the same month one year ago, the PCE price index for July increased 3.7 percent. Excluding food and energy, the PCE price index increased 3.3 percent.”  
SUMMARY: Official print. Calendar puts this on Wednesday, not the Thursday session under review.

CLAIM: Headline PCE 0.1 pp hot vs consensus; core in line; futures pulled back, yields higher on the print.  
URL: https://www.cnbc.com/2026/08/26/feds-preferred-inflation-gauge-shows-core-prices-rose-3point3percent-annually-in-july.html  
PUBLISHED: 2026-08-26  
QUOTE: “increased a seasonally adjusted 0.2% for the month, putting the annual inflation rate at 3.7%… Both were 0.1 percentage point above the Dow Jones consensus. But stripping out volatile food and energy costs, core PCE posted respective gains of 0.2% and 3.3%, in line with forecasts… Stock market futures pulled back a bit after the report while Treasury yields were higher.”  
SUMMARY: Not the cool PCE the morning was positioned for; already digested before 8/27 cash open.

CLAIM: Utilities fell on rotation into tech after Nvidia’s outlook.  
URL: https://www.morningstar.com/news/dow-jones/2026082710813/utilities-down-on-rotation-into-tech-utilities-roundup  
PUBLISHED: 2026-08-27 18:03 ET  
QUOTE: “Shares of power producers fell as traders rotated out of defensive sectors back into the high-risk tech sector due to Nvidia's outlook for robust growth.”  
SUMMARY: Same-day sector tape explanation matching XLU −0.76% vs SPY +0.66%.

CLAIM: Salesforce +22% on 8/27 after 8/26 earnings; software complex bid.  
URL: https://www.cnbc.com/2026/08/27/salesforce-stock-soars-on-track-for-second-best-day-ever.html  
PUBLISHED: 2026-08-27  
QUOTE: “Shares of Salesforce jumped 22% Thursday after the company reported a beat on second-quarter earnings and announced an expanded partnership with artificial intelligence startup Anthropic… The iShares Expanded Tech-Software ETF climbed roughly 5%.”  
SUMMARY: Overnight software shock that made the 8/27 tape growth-led, not “modest risk-on.”

CLAIM: Salesforce reported 8/26, not 8/27.  
URL: https://www.salesforce.com/news/press-releases/2026/08/26/fy27-q2-earnings/  
PUBLISHED: 2026-08-26  
QUOTE: “SAN FRANCISCO, Calif. – August 26, 2026 – Salesforce (NYSE: CRM)… announced results for its second quarter fiscal 2027 ended July 31, 2026.”  
SUMMARY: Knowable before Thursday’s open.

CLAIM: XLU close-to-close −0.76% on 8/27 with open 43.22 / close 43.18.  
URL: (deterministic Channel 1 actuals injected into this run)  
PUBLISHED: 2026-08-27 session  
QUOTE: ETF_PCT −0.7584; SPY_PCT +0.6553; REL_PCT −1.4137; OPEN 43.22; CLOSE 43.18  
SUMMARY: Absolute mild down, relative notable lag.

---

**Standing lesson to add:** On a morning after mega-cap earnings **and** a PCE print that already happened, do not score S0/S1 from **carried yield easing**. If ES/NQ are green and NVDA/CRM already reported, default utilities to **relative lag / flat-to-down absolute**, and **do not let the pipeline promote mild → notable**. Also: **verify the economic calendar date** — “PCE today” was yesterday.

---

## RESEARCH APPENDIX

**Queries run**
- web_search: `XLU utilities ETF August 27 2026 performance PCE`
- web_search: `July 2026 core PCE release August 27 2026 result utilities stocks`
- web_search: `10 year treasury yield August 27 2026 PCE reaction`
- web_search: `July 2026 personal income and outlays PCE BEA release date`
- web_search: `XLU vs SPY August 27 2026 sector performance utilities lag growth`
- web_search: `NEE DUK SO AEP CEG XEL PEG August 27 2026 stock performance`
- web_search: `"core PCE" July 2026 3.3% August 26 OR August 27`
- web_search: `US 10-year yield August 27 2026 close 4.67`
- web_search: `S&P 500 sector performance August 27 2026 utilities technology`
- web_search: `BEA personal income outlays July 2026 released August 26 8:30`
- web_search: `Salesforce stock August 27 2026 earnings surge SPY`
- web_search: `XLU volume August 27 2026 utilities ETF lag tech rotation`
- web_search: `utilities down on rotation into tech August 27 2026 Morningstar Dow Jones`
- web_search: `CEG Constellation Energy August 27 2026 close percentage`
- web_search: `Nvidia earnings August 26 2026 stock August 27 2026`
- web_search: `AEP SO DUK stock percent change August 27 2026`
- x_search: `XLU utilities sector August 27 2026 PCE yields why down vs SPY` (2026-08-27 to 2026-08-28)
- x_search: `utilities XLU down rotation tech Nvidia Salesforce August 27 2026` (2026-08-27 to 2026-08-28)
- memory_search: Utilities/XLU 2026-08-27 (failed — index metadata missing)

**Key sources**
- BEA, *Personal Income and Outlays, July 2026* — https://www.bea.gov/news/2026/personal-income-and-outlays-july-2026 — fetched 2026-08-28T00:27Z — headline/core PCE +0.2% m/m; +3.7% / +3.3% y/y; income +0.4%; PCE spending +0.2%.
- CNBC, *Fed’s preferred inflation gauge…* — https://www.cnbc.com/2026/08/26/feds-preferred-inflation-gauge-shows-core-prices-rose-3point3percent-annually-in-july.html — 2026-08-26 — headline 0.1 pp hot; core in line; yields up / futures down on the print; dated Wednesday.
- Morningstar/Dow Jones, *Utilities Down on Rotation Into Tech — Utilities Roundup* — https://www.morningstar.com/news/dow-jones/2026082710813/utilities-down-on-rotation-into-tech-utilities-roundup — 2026-08-27 18:03 ET — defensive-to-tech rotation on Nvidia outlook.
- CNBC, *Salesforce rockets 22%…* — https://www.cnbc.com/2026/08/27/salesforce-stock-soars-on-track-for-second-best-day-ever.html — 2026-08-27 — CRM +22% Thursday; IGV ~+5%.
- Salesforce IR PR — https://www.salesforce.com/news/press-releases/2026/08/26/fy27-q2-earnings/ — 2026-08-26 — Q2 FY27 results dated Wednesday.
- Nvidia IR (via search citation) — https://nvidianews.nvidia.com/news/nvidia-announces-financial-results-for-second-quarter-fiscal-2027 — Q2 FY27 after close 8/26; 8/27 follow-through ~+8.7–9%.
- GuruFocus / CountryEconomy / Slickcharts (search) — 10Y close **4.67%** on 2026-08-27 vs morning ~4.64%.
- Stock history aggregators (stockanalysis / stocknear) — NEE −0.89%, DUK ~−0.86%, SO ~−0.79%, AEP ~−0.53%, XEL −0.67%, PEG −0.90%, CEG +1.03%.
- Channel 1 actuals (injected) — XLU −0.7584%, SPY +0.6553%, rel −1.4137%.

**Not used as primary:** Zacks/Yahoo history fetches (bot-wall / 404); FRED DGS10 page timed out; memory index unavailable.