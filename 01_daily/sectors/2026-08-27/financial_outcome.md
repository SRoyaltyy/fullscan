# Sector Outcome — Financial — 2026-08-27

Actuals: {'etf': 'XLF', 'pct': -0.6522438454611534, 'spy_pct': 0.6552786111251541, 'rel': -1.3075224565863075, 'open': 57.90999984741211, 'close': 57.880001068115234}

Memory search is paused (index metadata missing; `openclaw memory index --force` would rebuild it). Review uses the injected morning note, deterministic actuals, and live sources.

## 0. Facts

| | |
|---|---|
| **XLF** | **−0.652%** (open 57.91 → close 57.88) |
| **SPY** | **+0.655%** |
| **Relative** | **−1.308%** |
| **Path** | Open-to-close was essentially flat (−0.03 pts). The loss vs prior close was a gap/overnight fade, then a dead session while SPY rallied. |
| **Direction / mag** | **down / mild** (absolute ~0.65%; relative underperformance was larger) |

Morning call: **up / mild**, total_score 5.175 (pipeline) vs 2.25 in the SECTOR_SCORES block, regime mixed, S4 = 0.0, multiplier 0.9.

---

## 1. What drove Financials today

**Primary driver was not PCE and not a NIM/yield shock.** It was a **tech/AI leadership snapback** that reversed the morning’s “AI trade unravels → rotate into banks” factor.

- **Nvidia** reported Q2 FY27 after the close on **Aug 26**: revenue **$96.2B** (+106% y/y), Q3 guide **$108B ±2%**. Premarket Aug 27 NVDA **+7.2%**; chips lifted Nasdaq futures.
- **Salesforce** also reported Aug 26 AMC; CRM **+22%** on Aug 27 (second-best day on record per CNBC), IGV ~+5%. That is enough to lift SPY without banks.
- **Yields barely moved** on Thursday: 10y **4.672%** (<1 bp), 30y **5.19%**, 2y **4.232%**. This was not a long-end spike day that should have crushed (or boosted) XLF via duration/NIM.
- **July PCE was Wednesday, Aug 26**, not Thursday. Headline **+0.2% m/m / 3.7% y/y** (0.1 ppt above DJ consensus); **core +0.2% / 3.3% in line**. Futures pulled back and yields rose *Wednesday*. By Thursday cash open, PCE was stale; Jackson Hole / Warsh Friday was the next macro event.

**Taxonomy mapping**

| Factor | Morning | Reality 8/27 |
|---|---|---|
| Sector rotation into financials / “AI unravels” | HIT high | **Inverted** — AI re-asserted; XLF lagged SPY by 1.31% |
| Capital markets / IB / trading surge | HIT high | Structural, not a 1-day tape driver |
| Yield-curve / NIM tailwind | HIT medium | Yields **unchanged**; no fresh NIM impulse |
| Risk-on / equity beta | HIT medium | Risk-on was **tech-idiosyncratic**, not beta that lifts XLF |
| Credit spreads / charge-offs / CRE | mixed/partial | No evidence they *caused* today’s print |
| Crowded long / failed high | “not crowded” | XLF tagged highs then failed; relative mean-reversion |

**Outliers inside the sector:** BAC ~**−1.7%** (worse than XLF); JPM ~**−0.4% to −0.6%**; BRK.B modestly red. Lending-heavy names weaker than the ETF; no single bank-specific blow-up found. Weakness was **broad relative underperformance**, not one name.

---

## 2. Audit of morning S0–S4 (use morning numbers, do not rewrite them)

**S0_SHARED_MACRO +0.5 — too constructive, and PCE timing was wrong.**  
Morning treated PCE as *both* “due today 8:30 ET” *and* “already in-line relief.” BEA printed **Wednesday**. Headline was slightly hot; core in-line. Thursday macro was Jackson Hole positioning + jobless claims 203k, not a fresh PCE binary. Mapping “futures up / oil down / PCE relief → banks up” failed because the equity bid was NVDA/CRM, not broad beta.

**S1_SECTOR_FACTORS +1.0 — the miss.**  
The high-weight HIT was rotation out of AI into financials. Overnight NVDA/CRM made that factor **two-sided at the open**. Leaving S1 at +1.0 after a mega-cap AI beat is converting a *theme* into a *day* call. Offsets (consumer credit, CRE, NIM fade) were real but not what printed.

**S2_BREADTH +0.5 — stale.**  
3d rel +1.31% / 1w rel +1.74% were used as if they would persist. 1d rel was already **−0.11%** and 1m rel **−2.26%**. Today extended the 1d/1m lag, not the 3d/1w win streak.

**S3_FLOWS_POSITIONING +0.5 — vulnerable, not a tailwind.**  
“Record highs / 11-week streak / not crowded” described *why a reversal could be sharp*, not why the day should be up. Once AI leadership returned, that positioning was fuel for relative selling.

**S4_ETF_TAPE 0.0 — the honest score, then ignored.**  
Standing lesson in the morning note: *S4 flat/neutral → do not convert structural support into an absolute up call.* Official block still emitted `PREDICTED_DIRECTION: up`. Pipeline score 5.175 vs handwritten 2.25 shows the same leak: leading-sum inflation vs the tempered narrative.

**Multiplier 0.9 / mixed / mild cap — partially right for the wrong event.**  
Capping magnitude at mild was correct. Discounting for a “scheduled PCE day” was a day late. The live overnight event was **NVDA/CRM**, not PCE.

---

## 3. Interactions / double-count / knowable-at-open

**Double-count:** The same 3d/1w relative tape was scored in **S1 (rotation HIT), S2 (breadth), and S3 (flows)**. One fact, three pluses.

**PCE double-use:** Counted as live two-sided (multiplier, mixed regime, confidence 0.5) *and* as already-in-line relief (S0 +0.5). Those cannot both be true on Aug 27.

**Knowable at open (yes):** NVDA and CRM prints were out; PCE was already Wednesday; 10y had stopped easing; XLF 1d tape was flat; the AI-to-banks rotation was the crowded narrative.

**Not knowable:** CRM +22% / NVDA follow-through size, SPY +0.66% vs XLF −0.65%, BAC −1.7%.

**KNOWABLE_AT_OPEN: partially** — a down-or-lag day for XLF vs a tech squeeze was a live overnight binary; an *absolute up* call was not justified by S4=0.

---

## 4. Evidence

CLAIM: July PCE released Aug 26: headline +0.2% m/m / 3.7% y/y (0.1 ppt above DJ consensus); core +0.2% / 3.3% in line; income +0.4%, spending +0.2%.  
URL: https://www.cnbc.com/2026/08/26/feds-preferred-inflation-gauge-shows-core-prices-rose-3point3percent-annually-in-july.html  
PUBLISHED: 2026-08-26  
QUOTE: “increased a seasonally adjusted 0.2% for the month, putting the annual inflation rate at 3.7%… Both were 0.1 percentage point above the Dow Jones consensus… core PCE posted respective gains of 0.2% and 3.3%, in line with forecasts… Stock market futures pulled back a bit after the report while Treasury yields were higher.”  
SUMMARY: PCE was Wednesday’s event; headline slightly hot, core in-line; not Thursday’s catalyst.

CLAIM: BEA official July 2026 PIO numbers.  
URL: https://www.bea.gov/news/2026/personal-income-and-outlays-july-2026  
PUBLISHED: 2026-08-26  
QUOTE: “From the preceding month, the PCE price index for July increased 0.2 percent. Excluding food and energy, the PCE price index also increased 0.2 percent… From the same month one year ago, the PCE price index for July increased 3.7 percent… excluding food and energy… 3.3 percent.”  
SUMMARY: Primary source confirms CNBC; next release Sep 30.

CLAIM: Thursday yields little changed into Jackson Hole.  
URL: https://www.cnbc.com/2026/08/27/us-bonds-us10y-jackson-hold.html  
PUBLISHED: 2026-08-27  
QUOTE: “The benchmark 10-year Treasury note was up less than 1 basis point at 4.672%. The 30-year… 5.19%… 2-year… 4.232%.”  
SUMMARY: No Thursday yield shock to explain XLF.

CLAIM: Slightly hot Wednesday inflation lifted hike odds, then faded Thursday; claims 203k.  
URL: https://www.cnbc.com/2026/08/27/dollar-near-eight-day-high-as-us-data-lifts-fed-hike-bets.html  
PUBLISHED: 2026-08-27  
QUOTE: “Data on Wednesday showed inflation rose more than expected in July… briefly lifting expectations for a September rate hike… to over 40%. But expectations… slipped back to 34.1% on Thursday.”  
SUMMARY: PCE’s hawkish impulse was Wednesday; Thursday was Warsh-watch, not a banks-specific macro dump.

CLAIM: Nvidia Q2 FY27 beat and guide.  
URL: https://nvidianews.nvidia.com/news/nvidia-announces-financial-results-for-second-quarter-fiscal-2027  
PUBLISHED: 2026-08-26  
QUOTE: “revenue… $96.2 billion, up 18% from the previous quarter and up 106% from a year ago… Revenue is expected to be $108.0 billion, plus or minus 2%.”  
SUMMARY: Overnight AI-demand confirmation that could reverse the financials-rotation theme.

CLAIM: NVDA +7%+ premarket Aug 27, chips/Nasdaq futures bid.  
URL: https://www.forbes.com/sites/siladityaray/2026/08/27/nvidia-soars-7-in-premarket-as-it-leads-upswing-in-chip-stocks-after-strong-earnings/  
PUBLISHED: 2026-08-27  
QUOTE: “Nvidia’s stock price rose to $224.83 per share, up more than 7.2% from Wednesday’s close… Nasdaq Futures… rose 1.10%… S&P 500 futures climbed around 0.5%.”  
SUMMARY: Knowable-at-open tech squeeze vs financials.

CLAIM: Salesforce +22% Aug 27 after Q2 beat / Anthropic.  
URL: https://www.cnbc.com/2026/08/27/salesforce-stock-soars-on-track-for-second-best-day-ever.html  
PUBLISHED: 2026-08-27  
QUOTE: “Shares of Salesforce jumped 22% Thursday… second-best day ever… iShares Expanded Tech-Software ETF climbed roughly 5%.”  
SUMMARY: Second mega-cap software impulse lifting SPY without XLF.

CLAIM: Salesforce official print Aug 26.  
URL: https://www.salesforce.com/news/press-releases/2026/08/26/fy27-q2-earnings/  
PUBLISHED: 2026-08-26  
QUOTE: “Revenue of $11.3 billion, up 11% Y/Y… GAAP diluted net income per share of $4.29… non-GAAP… $5.90.”  
SUMMARY: Confirms the overnight software catalyst.

---

OUTCOME_BEGIN
SECTOR: Financial
ETF: XLF
ETF_PCT: -0.652
SPY_PCT: 0.655
REL_PCT: -1.308
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Tech/AI snapback after Nvidia and Salesforce earnings reversed the banks-rotation bid; XLF faded vs prior close and sat flat open-to-close while SPY rallied.
KEY_INTERACTION: Overnight NVDA/CRM inverted the morning “AI unravels into financials” factor; Wednesday’s slightly hot headline PCE was already in the tape and Thursday yields barely moved.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Direction miss — S4-flat plus a live NVDA/CRM overnight binary should not have been an absolute up call; PCE was a day late and the same 3d/1w rotation tape was triple-counted in S1/S2/S3. Mild magnitude cap was the part that aged well.
OUTCOME_END

---

## RESEARCH APPENDIX

**Queries run**
- web_search: `XLF financials sector August 27 2026 why down PCE banks`
- web_search: `July 2026 core PCE release August 27 2026 result Fed`
- web_search: `SPY stock market August 27 2026 close financials lag banks`
- web_search: `Nvidia earnings August 26 2026 stock market August 27 financials underperform`
- web_search: `Treasury yields August 27 2026 10-year PCE banks financials`
- web_search: `site:bea.gov Personal Income and Outlays July 2026 PCE`
- web_search: `XLF JPM BAC GS V MA BRK August 27 2026 stock performance`
- web_search: `S&P 500 sector performance August 27 2026 financials technology`
- web_search: `"Personal Income and Outlays" July 2026 released August`
- web_search: `Bank of America BAC stock August 27 2026 decline`
- web_search: `Salesforce earnings August 26 2026 stock surge August 27`
- web_search: `XLF close August 27 2026 57.88 financials underperform Nvidia`
- x_search: `What happened to XLF financials banks stocks on August 27 2026 after PCE?` (from 2026-08-27 to 2026-08-28)
- memory_search: Financial/XLF 2026-08-27 and S4/PCE lessons — **unavailable** (index metadata missing)

**Key sources (title + URL + fetched/published)**
- BEA, *Personal Income and Outlays, July 2026* — https://www.bea.gov/news/2026/personal-income-and-outlays-july-2026 — published 2026-08-26 — PCE +0.2%/3.7%, core +0.2%/3.3%, income +0.4%, PCE spending +0.2%
- CNBC, *Fed’s preferred inflation gauge shows core prices rose 3.3% annually in July* — https://www.cnbc.com/2026/08/26/feds-preferred-inflation-gauge-shows-core-prices-rose-3point3percent-annually-in-july.html — 2026-08-26 — headline 0.1 ppt hot vs DJ; core in line; futures down, yields up *that day*
- CNBC, *Treasury yields are little changed ahead of Jackson Hole* — https://www.cnbc.com/2026/08/27/us-bonds-us10y-jackson-hold.html — 2026-08-27 — 10y 4.672%, 30y 5.19%, 2y 4.232%
- CNBC, *Dollar flat after data as focus shifts to Jackson Hole* — https://www.cnbc.com/2026/08/27/dollar-near-eight-day-high-as-us-data-lifts-fed-hike-bets.html — 2026-08-27 — Sep hike odds 40%→34%; claims 203k
- NVIDIA IR, *Q2 FY27 results* — https://nvidianews.nvidia.com/news/nvidia-announces-financial-results-for-second-quarter-fiscal-2027 — 2026-08-26 — $96.2B rev, $108B Q3 guide
- Forbes, *Nvidia Soars 7% In Premarket…* — https://www.forbes.com/sites/siladityaray/2026/08/27/nvidia-soars-7-in-premarket-as-it-leads-upswing-in-chip-stocks-after-strong-earnings/ — 2026-08-27 — NVDA +7.2% premarket
- Salesforce IR, *FY27 Q2 earnings* — https://www.salesforce.com/news/press-releases/2026/08/26/fy27-q2-earnings/ — 2026-08-26 — $11.3B rev, non-GAAP EPS $5.90
- CNBC, *Salesforce rockets 22%…* — https://www.cnbc.com/2026/08/27/salesforce-stock-soars-on-track-for-second-best-day-ever.html — 2026-08-27 — CRM +22%, IGV ~+5%
- Deterministic actuals (injected): XLF −0.652%, SPY +0.655%, rel −1.308%, open 57.91 / close 57.88

**Not used as primary evidence:** Benzinga/Zacks/WSJ/TipRanks fetches (403/401/bot wall); X posts used only as color on failed highs / XLK vs XLF, not as price facts.