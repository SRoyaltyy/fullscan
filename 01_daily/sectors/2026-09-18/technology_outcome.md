# Sector Outcome — Technology — 2026-09-18

Actuals: {'etf': 'XLK', 'pct': 0.818892143419303, 'spy_pct': -0.11932509489422927, 'rel': 0.9382172383135323, 'open': 189.22999572753906, 'close': 189.60000610351562, 'source': 'yf_download'}

Memory search is paused this run (embedding index metadata missing). Review uses injected Technology/XLK morning logs plus live session sources only.

## 0. Facts

XLK **+0.82%** (open 189.23 → close 189.60; range ~187.53–189.81 — dip then close near highs). SPY **−0.12%**. Relative **+0.94%**. Nasdaq Composite **+0.40%** to 26,522.55; NDX **+0.67%**; SOX **+2.78%** to 11,921.69. Majority of stocks fell; RTY **−0.5%**. Path: premarket XLK **+0.60%** did not melt up; cash session was a mild, chip-led grind with an intraday drawdown, not a notable trend day.

**CLAIM:** XLK closed ~$189.60, about +0.82% on 2026-09-18.  
**URL:** https://www.stockmonitor.com/quote/xlk/  
**PUBLISHED:** 2026-09-18  
**QUOTE:** “18/09/26 open 188.79 high 189.805 low 187.53 close 189.64”  
**SUMMARY:** Confirms a mild up day and an intraday low well below the open.

**CLAIM:** S&P 500 +0.2% to 7,650.50; Nasdaq +0.4% to 26,522.55; Dow −0.2%; RTY −0.5%; 10Y to 5.00%.  
**URL:** https://wtop.com/national/2026/09/how-major-us-stock-indexes-fared-friday-9-18-2026/  
**PUBLISHED:** 2026-09-18  
**QUOTE:** “The S&P 500 rose 0.2% Friday. The Dow Jones Industrial Average slipped 0.2%, and the Nasdaq composite added 0.4%… the yield on the 10-year Treasury climbed to 5.00%.”  
**SUMMARY:** Broad tape mixed/weak-breadth; Nasdaq outperformed. SPY’s −0.12% vs S&P +0.17% is a share-class/quote difference; relative XLK vs SPY still clearly positive.

## 1. What drove the sector

Primary driver was **semiconductor / AI-hardware follow-through**, not Apple launch day and not a broad tech beta melt-up.

**CLAIM:** SOX +2.78%; NVDA +1.34%; MU +3.92%; SOXL +7.79%; AAPL −0.26%; MSFT −0.80%; NDX +0.67%.  
**URL:** https://en.fnnews.com/news/202609190523172107  
**PUBLISHED:** 2026-09-19 05:26 KST  
**QUOTE:** “The Philadelphia Semiconductor Index (SOX) jumped 322.19 points, or 2.78%, to 11,921.69… Apple fell $0.87, or 0.26%… Microsoft declined $3.97, or 0.80%.”  
**SUMMARY:** XLK’s +0.82% is a chip-weighted average: memory/semis carried the ETF while two of the three largest holdings were red.

Taxonomy mapping:
- **Semiconductor demand / HBM-AI tightness (S1 spine, carried):** HIT in the tape. MU +3.92%, SanDisk +11%, SOX +2.78% after 09-17’s SOX +3.14%. This is continuation of the one AI-infra cluster, not a new print.
- **Large-cap / high-beta leadership inside sector (S2):** PARTIAL/HIT for *hardware*, not the whole book. Leadership was chips, not Apple.
- **Sector rotation into technology (S4/S1):** HIT on a relative basis (XLK vs SPY +0.94%) while SPY was slightly down.
- **Risk-on tape (S0):** PARTIAL. VIX 14.83 (−4%); oil still easing; Nasdaq up. But index breadth was poor and 10Y back to 5.00% — not a clean beta expansion.
- **Real yields / duration tax (S0):** LIVE during the session, not just a residual level. 10Y climbed to 5.00% and software/Apple paid it.
- **Apple product-availability catalyst (S1 named):** MISS as a same-session XLK support. AAPL −0.26% on retail-availability day.
- **Software net-retention / multiple (S1 secondary):** Still a drag. CRM ~−2.0%, NOW ~−2.2%, INTU ~−3.2%.
- **Crowded-long unwind (S3):** ABSENT. Relative leadership held; no oil-spike / backwardation / −0.9 corr overlay.
- **IP / LEI (calendar, not a spine):** Soft prints, muted XLK impact.

**CLAIM:** August industrial production 0.0% MoM vs +0.3% expected; manufacturing −0.3%.  
**URL:** https://www.reuters.com/business/us-manufacturing-output-falls-august-after-rising-seven-straight-months-2026-09-18/  
**PUBLISHED:** 2026-09-18  
**QUOTE:** (search extract) “US industrial production was unchanged in August… Manufacturing output declined 0.3%… first drop of the year.”  
**SUMMARY:** Two-sided data printed soft; not the XLK driver. Morning was right not to pre-score it.

**CLAIM:** iPhone 18 Pro demand survey stronger-than-expected (intentions, not sell-through).  
**URL:** https://www.macrumors.com/2026/09/18/iphone-18-pro-demand-stronger-than-expected/  
**PUBLISHED:** 2026-09-18  
**QUOTE:** “The iPhone 18 Pro and Pro Max together attracted 53% of prospective buyers, with 32% choosing the Pro Max, compared with 29% a year earlier.”  
**SUMMARY:** Narrative support for Apple existed; the stock still lagged. Do not retrofit Apple as the XLK driver.

## 2. Audit of morning S0–S4 (use morning numbers, not a rewrite)

Morning published call: **flat / flat**, total 11.236, regime risk_on, confidence 0.55. Narrative scores: S0 +1.0, S1 +1.0, S2 +0.5, S3 0, S4 +1.0. Tape anchor was already up (NQ +1.50%, ES +1.14%, PM:XLK +0.60%). Pipeline then applied `sector_rs_veto` (stale d1/w1 −2.05/−2.07) and `calendar_size_gate` and emitted **flat**.

| Sleeve | Morning | Reality | Verdict |
|---|---|---|---|
| **S0 +1.0** | Risk-on for duration tech; oil offered; hawkish *level* caps below +2; do not flatten vs confirming NQ | Oil eased; VIX down; Nasdaq up; **10Y to 5.00%** taxed software/Apple; SPY slightly red | **Sign right, cap right.** +1 not +2 was the correct ceiling. Residual hawkish path was not fully “paid” — yields re-tightened *in session*. |
| **S1 +1.0** | One AI-infra cluster intact; Apple availability named, modest support, **not notable** | Semis delivered the +; Apple **did not**. Software still red | **Spine right, Apple sleeve wrong.** 08-12 notable-up FAIL was correct. S1 was a bit hot if Apple was doing real work in the +1. |
| **S2 +0.5** | Constructive large-cap/AI-hardware, not a % names-up melt-up; live tape over leftover HEAT | SOX +2.78% vs AAPL/MSFT/software red; majority of stocks fell | **Hit.** Nested leftover HEAT (semis down / software up) was stale again. |
| **S3 0** | 09-11 crowding-zero: overlay inverted | Rel +0.94%; no unwind | **Hit.** 4-horizon RS was leadership, not fuel. |
| **S4 +1.0** | Rel tape confirms, not an absolute-up certificate | Rel +0.94% with SPY slightly down | **Hit.** Absolute XLK was only mild; relative was the cleaner tell. |

**Direction:** published **flat** vs actual **up**. That is a miss versus the *output*, and a hit versus the *narrative* (scores + confirming NQ). Lessons **08-21 / 09-16** (“do not emit flat against confirming NQ”) were in the morning memo and then overridden by the size/RS gates.

**Magnitude:** published **flat** vs actual **mild**. 08-12 / 09-14 were right that this was **not notable** (no mega-cap beat; PM only +0.60%). The gate that crushed *direction* to flat over-shrunk a mild-up tape.

Last graded 09-17 was already dir MISS / mag MISS (predicted flat/flat vs XLK +2.25% notable). 09-18 repeats the **flat-vs-confirming-NQ** error at smaller size.

## 3. Interactions / double-count / knowable-at-open

- **One S0 object:** oil-easing + VIX contango + NQ green = one risk-on impulse. Do not restack oil as a second XLK hit. It helped chips; it did not create a broad melt-up.
- **Duration tax stays in S0:** 10Y to 5.00% is the same real-yield sleeve scored at +1 in the morning. It showed up as **software/Apple down, chips up** — a cross-sleeve interaction, not a new S1.
- **One AI-infra cluster:** TSMC/HBM/Intel memory/ASML EUV were correctly carried. Today’s SOX extension is that cluster’s *tape*, not three new spines.
- **Apple ≠ XLK:** Naming availability was required (09-09). Treating it as modest same-session support **double-counted a scheduled event that did not move the top weight**.
- **FOMC is paid:** day-3. Do not re-score Warsh/dots. Yields *re-tightening* is a live duration observation, not a new Chair shock.
- **IP/LEI:** correctly not pre-scored; they were not the sector driver.
- **Stale RS veto vs live tape:** pipeline `sector_rs_tape d1/w1 −2.05/−2.07` was the leftover the morning memo already told you to ignore. Applying it to flatten **direction** failed the 09-17 stale-RS rule.

**Knowable at open: partially.** Direction **up** was knowable (NQ +1.50%, XLK PM +0.60%, Kospi +2.66%, 4-horizon RS). Mild-not-notable was knowable (no beat, PM <1%, 08-12). Not knowable: Apple lagging launch day, 10Y back to 5.00%, how far SOX would extend after +3.14% yesterday, IP/LEI.

## 4. Outliers inside the sector

- **Upside:** SanDisk +11%, MU +3.92%, AVGO ~+3.0%, AMD ~+2.7%, SOX +2.78%, SOXL +7.8%, NVDA +1.34%. Memory/AI hardware.
- **Downside:** INTU ~−3.2%, CRM ~−2.0%, NOW ~−2.2%, MSFT −0.80%, AAPL −0.26% *on iPhone 18 Pro availability day*. Software multiple-compression is still live, not a 09-17 one-off.
- **Tell:** XLK is a chip fund with Apple/Microsoft strapped on. When those two lag, you still get a mild green ETF if SOX is +2.8%. Do not let AAPL define the sector on a hardware-availability calendar print.

OUTCOME_BEGIN
SECTOR: Technology
ETF: XLK
ETF_PCT: 0.818892143419303
SPY_PCT: -0.11932509489422927
REL_PCT: 0.9382172383135323
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: SOX/AI-hardware follow-through (SOX +2.78%, MU/AVGO/AMD bid) lifted XLK while SPY slipped.
KEY_INTERACTION: 10Y back to 5.00% taxed Apple/software; oil-easing risk-on stayed in chips — one S0 tape, split internally, not two independent positives.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: S-scores and 08-12 were right (constructive, not notable); published flat/flat fought confirming NQ and missed direction — should have been up/mild.
OUTCOME_END

## RESEARCH APPENDIX

**Queries run**
- memory_search: `Technology XLK sector prediction lessons 2026-09-18 outcome crowding Apple` (index unavailable)
- web_search: `XLK ETF September 18 2026 stock market technology` (freshness=day)
- web_search: `stock market September 18 2026 Nasdaq Apple iPhone oil Fed` (freshness=week)
- web_search: `S&P 500 Nasdaq close September 18 2026 technology sector semiconductors` (freshness=week)
- web_search: `Apple iPhone 18 Pro demand September 18 2026 AAPL stock` (freshness=week)
- web_search: `US industrial production LEI September 18 2026 market reaction` (freshness=week)
- web_search: `NVDA AMD INTC MU AVGO MSFT AAPL close September 18 2026` (freshness=week)
- web_search: `Philadelphia Semiconductor Index SOX September 18 2026 close` (freshness=week)
- web_search: `oil prices September 18 2026 WTI Brent close` (freshness=day)
- web_search: `10-year Treasury yield September 18 2026 5.00` (freshness=day)
- web_search: `CRM NOW INTU software stocks September 18 2026` (freshness=week)
- web_search: `US industrial production August 2026 unchanged manufacturing output fell 0.3 percent` (freshness=week)
- x_search: `XLK Nasdaq technology stocks September 18 2026 Apple semiconductors what moved the market` (2026-09-18 to 2026-09-19)
- web_fetch: Reuters Nasdaq-futures / manufacturing (401 JS wall); MarketScreener (403); WTOP indexes (ok); MacRumors iPhone demand (ok); FNNews SOX recap (ok); StockMonitor XLK (ok)

**Key sources and facts taken**
- Channel 1 actuals (injected, unaltered): XLK +0.8189%, SPY −0.1193%, rel +0.9382%; open 189.23 / close 189.60.
- WTOP — How major US stock indexes fared Friday 9/18/2026 (2026-09-18): S&P +0.2% to 7,650.50; Nasdaq +0.4% to 26,522.55; Dow −0.2%; RTY −0.5%; 10Y to 5.00%; Brent briefly < $102 then back > $103. https://wtop.com/national/2026/09/how-major-us-stock-indexes-fared-friday-9-18-2026/
- Financial News / FNNews — NYSE mixed, SOXL +8%, SOX +2.78% (updated 2026-09-19 05:26): SOX 11,921.69 (+2.78%); NVDA +1.34% to $222.27; MU +3.92% to $1,015.80; SanDisk +10.99%; AAPL −0.26% to $336.13; MSFT −0.80% to $493.78; VIX 14.83 (−3.96%); NDX +0.67%. https://en.fnnews.com/news/202609190523172107
- StockMonitor XLK (2026-09-18): close ~189.64, range 187.53–189.805, volume ~6.4M. https://www.stockmonitor.com/quote/xlk/
- MacRumors — iPhone 18 Pro demand stronger than expected (2026-09-18): Evercore ~4,000-person survey; Pro+Pro Max 53% of prospective buyers. https://www.macrumors.com/2026/09/18/iphone-18-pro-demand-stronger-than-expected/
- Reuters — US manufacturing output falls in August (2026-09-18): IP 0.0% MoM vs +0.3% cons; manufacturing −0.3%. https://www.reuters.com/business/us-manufacturing-output-falls-august-after-rising-seven-straight-months-2026-09-18/
- Conference Board LEI (via Morningstar/PR Newswire search, 2026-09-18): LEI −0.1% in August. https://www.morningstar.com/news/pr-newswire/20260918ny51550/the-conference-board-leading-economic-index-lei-for-the-us-edged-down-in-august
- FinanceCharts / StockMonitor software closes (2026-09-18): CRM ~−2.0%, NOW ~−2.2%, INTU ~−3.2%.
- Morning XLK prediction (injected): predicted flat/flat; S0 +1 / S1 +1 / S2 +0.5 / S3 0 / S4 +1; NQ +1.50%; PM:XLK +0.60%; Apple availability named; 08-12 notable-up FAIL.

**Not used as XLK drivers:** IP/LEI (soft, muted); Apple availability as a positive same-session impulse (stock red); leftover MAP HEAT semis-down/software-up; stale RS d1/w1 −2%; FOMC/Warsh as a live binary.