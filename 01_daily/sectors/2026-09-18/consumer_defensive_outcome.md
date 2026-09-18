# Sector Outcome — Consumer Defensive — 2026-09-18

Actuals: {'etf': 'XLP', 'pct': -0.8264400882337819, 'spy_pct': -0.11932509489422927, 'rel': -0.7071149933395526, 'open': 83.13999938964844, 'close': 82.80000305175781, 'source': 'yf_download'}

## 0. Facts

XLP **−0.83%** (open **83.14** → close **82.80**). SPY **−0.12%**. Relative **−0.71%**. Path: gap/open already below Thursday’s ~83.49 close, then a grind lower — not a late-day accident. Absolute **down**; **notable** for a low-beta staples book (not a market crash: SPY only −12 bp). Premarket ES **+1.14% / NQ +1.50%** did **not** survive into cash.

---

## 1. What drove the sector

Not FTS. Not oil-relief. Not a second post-FOMC beta-up day.

Cash tape was **mixed/choppy after Thursday’s rebound**: Nasdaq finished green, Dow red, SPY barely red. The 10-year **backed up ~5 bp to ~5.00%** (3 p.m. Tradeweb **4.995%**, largest one-day yield gain since Sep 10). That is a **bond-proxy duration hit**. Defensives lagged growth/tech (Benzinga session snapshot: XLC/XLK green; XLP among losers with XLF/XLU/XLB). Oil’s premarket **offer reversed** (WTI ~+1% toward $103); the S1 packaging/freight tailwind did not print. Inside the book, **PEP ~−3% to a 52-week low** was the nested outlier; PG roughly tracked the ETF; COST was closer to flat.

Primary object: **duration + rotation out of defensives** after the risk-on futures bid failed. Triple-witching volume was a path amplifier, not the thesis.

---

## 2. Audit morning S0–S4 (morning numbers, not rewritten)

| Sleeve | Morning | Reality | Verdict |
|---|---|---|---|
| **S0 −1** | One anti-FTS / risk-on rotation object. NQ lead 36 bp, PM XLP −1 bp, “no fresh duration break.” Absolute flat-band, relative funding source. | Rotation **HIT**. ES continuation **MISS** (SPY −12 bp). Duration **did** print a same-session shock (10Y +~5 bp to 5%). | Sign right, **too light**. Folding the 1w real-yield grind into “same S0 risk-on object” failed when ES faded **and** yields rose. Those are not always one shock. |
| **S1 −0.5** | Capped oil relief vs carried private-label/volume drag. Rotation not restacked. | Oil relief **did not pay**. Structural brand/private-label is not an 83 bp same-day driver. | Sign right; oil **+0.2 was a miss**. |
| **S2 0** | Mixed HEAT; WMT/COST/KO carry, not expansion. | Mega-retail did **not** lift the ETF. PEP smashed; PG down. | Too generous. Breadth was a mild negative, not zero. |
| **S3 0** | Lagged outflows / BofA underweight; not a same-morning forced sell. | No evidence a flow print created the sign. | Hold. |
| **S4 0** | PM −0.01% = non-participation, not a down forecast (08-28). | Cash opened ~40 bp below Thursday close and kept going. | PM flat **understated** the open. Absolute object was already weak vs yesterday’s close. |

**Engine vs card:** LLM overlay **−2.2**, leading sum **−1.5**, **divergence_flagged True**, relative lean **down**. Pipeline still emitted **up/mild** because `tape_anchor 0.86` (ES +1.14%) + `index_carry 1.215` overrode factors. That is the **09-17 DO-INSTEAD** repeating: ES green + flat PM is **not** an XLP-up certificate. Factor card won the relative call; engine lost the printed direction.

LLM absolute call was **flat-band, not notable down** — magnitude still a miss.

---

## 3. Interactions / double-count / knowable-at-open

- **Do not double-count** Thursday’s paid rel **−0.94%** as today’s driver. Today was a **new** down day, not a restack.
- **Do split** what morning fused: risk-on beta and duration. Today they **decoupled** (beta bid died, yields rose). Staples got neither a haven bid nor a beta bid.
- Oil in S0 as FTS: correctly **off**. Oil in S1 as relief: **over-credited**.
- PEP is nested, not the ETF thesis — but at ~4% weight a −3% print is a real ~12 bp drag, not noise.

**Knowable at open:** rotation (PM lag, XLK lead, 1d rel already red), duration **stress zone**, non-haven PM, witching **calendar**. **Not knowable:** cash fade of ES +1.14%, 10Y’s largest 1-day jump since Sep 10, oil reversal, PEP 52-week-low magnitude.

---

## 4. Outliers inside the sector

- **PEP ~−3% / 52-week low** — idiosyncratic (NA demand, costs, technical breakdown, plant wind-down). Morning HEAT already had PEP **flat vs KO-only bid**. That was the tell; the size of the break was not.
- **COST ~flat** — relative winner vs the ETF; morning “discount-store up” did not generalize.
- **PG ~−0.8%** — in-line, not a catalyst.
- No evidence WMT/COST/KO carry ran the tape. Single-ticker rule held: don’t make the ETF call from mega-retail.

---

### Evidence

CLAIM: XLP closed ~82.80, down ~0.83% from prior close; open 83.14.  
URL: Channel 1 actuals (deterministic); corroboration https://chartexchange.com/symbol/nyse-xlp/historical/  
PUBLISHED: 2026-09-18 session  
QUOTE: open 83.14 / close 82.80  
SUMMARY: Absolute −83 bp, gap/open already below Thursday ~83.49.

CLAIM: SPY −0.12%; XLP relative −0.71%.  
URL: Channel 1 actuals  
PUBLISHED: 2026-09-18  
QUOTE: ETF_PCT −0.826%; SPY_PCT −0.119%; REL_PCT −0.707%  
SUMMARY: Staples lagged a nearly flat SPY — funding-source day, not beta.

CLAIM: Cash indexes mixed; Nasdaq +0.4% to 26,522.55, S&P +0.2% to 7,650.50, Dow −0.2% to 51,682.64 after Thursday’s rally.  
URL: https://www.rttnews.com/3692364/u-s-stocks-close-mixed-following-choppy-trading-day.aspx  
PUBLISHED: 2026-09-18  
QUOTE: “The Nasdaq climbed … 0.4 percent … S&P 500 rose … 0.2 percent … Dow dipped … 0.2 percent.”  
SUMMARY: Premarket ES +1.14% did not persist as a broad risk-on cash session. (SPY vs SPX sign gap is ETF vs index; Channel 1 SPY is the scoreboard.)

CLAIM: 10-year yield +4.9 bp to 4.995% (3 p.m.), largest 1-day gain since Sep 10, second-highest of the year.  
URL: https://www.morningstar.com/news/dow-jones/202609186066/10-year-treasury-yield-rises-to-4995-this-week-data-talk  
PUBLISHED: 2026-09-18 15:47 ET  
QUOTE: “Today it is up 0.049 percentage point … Largest one-day yield gain since Thursday, Sept. 10, 2026 … second highest this year.”  
SUMMARY: Fresh duration shock for a bond-proxy; morning “no fresh 10Y>5% break” was too complacent.

CLAIM: Midday, 10Y ~5.00% and most sectors flat/falling.  
URL: https://www.fool.com/coverage/stock-market-today/2026/09/18/stock-market-midday-sept-18-stocks-slip-crypto-gains/  
PUBLISHED: 2026-09-18 (~11:44 a.m. ET snapshot)  
QUOTE: “the 10-Year Treasury yield is up 5 basis points to 5.00%. Most sectors were trading flat or falling”  
SUMMARY: Path was yield-led drift, not a staples haven bid.

CLAIM: Yields back above 5%; oil mixed (WTI ~+1% to ~$103) after a week near $110 Brent.  
URL: https://www.fool.com/investing/2026/09/18/the-dow-is-down-for-a-third-straight-week/  
PUBLISHED: 2026-09-18  
QUOTE: “the 10-year Treasury yield rose more than 5 basis points to 5.004% … West Texas Intermediate rose about 1% to roughly $103 a barrel”  
SUMMARY: Premarket CL −6.31% relief did not hold; oil was not an S1 tailwind in cash.

CLAIM: Intraday/session sector snapshot — XLC +0.25%, XLK +0.07%; XLP −0.38% among laggards with XLF/XLU/XLB.  
URL: https://www.benzinga.com/etfs/sector-etfs/26/09/61867052/leading-and-lagging-sectors-september-18-2026  
PUBLISHED: 2026-09-18 (point-in-time; XLP close was weaker than this snapshot)  
QUOTE: Consumer Staples (XLP) −0.38% among losers  
SUMMARY: Relative leadership was growth/comms, not defensives. Snapshot understates the ETF’s full-session −83 bp.

CLAIM: PEP ~−3% to a 52-week low; KO barely budged; no same-day PEP earnings catalyst.  
URL: https://247wallst.com/investing/2026/09/18/pepsico-falls-3-while-consumer-staples-hold-firm-keurig-dr-pepper-eases-coca-cola-barely-budges/  
PUBLISHED: 2026-09-18  
QUOTE: PepsiCo falls ~3% while staples “hold firm”; KO barely budges  
SUMMARY: Nested beverage outlier. Headline “staples hold firm” is **wrong vs Channel 1 XLP −83 bp** — use it only for PEP vs KO dispersion.

CLAIM: Fed hike already printed Sep 16; market pricing another hike; Thursday SPX +1.1% was relief, not a staples bid.  
URL: https://www.fool.com/investing/2026/09/18/the-fed-just-hiked-interest-rates-are-there-more-h/  
PUBLISHED: 2026-09-18  
QUOTE: “this latest rate hike was met with some joy … Thursday … S&P 500 index rose 1.1%.”  
SUMMARY: Confirms 09-17 rebound was paid; Friday was digestion + yields, not session-2 of the same object.

CLAIM: X posts tagged PEP 52-week lows / snack-demand pressure the same session.  
URL: https://x.com/StockMKTNewz/status/2101037703341203606  
PUBLISHED: 2026-09-18  
QUOTE: staples names including PEP hitting new 52-week lows  
SUMMARY: Confirms inside-sector outlier tape; not an XLP flow print.

---

OUTCOME_BEGIN
SECTOR: Consumer Defensive
ETF: XLP
ETF_PCT: -0.826
SPY_PCT: -0.119
REL_PCT: -0.707
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Duration backup (10Y ~+5 bp to 5%) plus rotation out of defensives after the ES +1.14% bid failed in cash
KEY_INTERACTION: Morning fused risk-on beta and duration into one S0 object; they decoupled — yields rose while the beta bid died, so staples got a bond-proxy smash without a haven bid
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Factor card (S0/S1 negative, relative down, divergence flag) was right; v2 tape_anchor/index_carry still printed up/mild and missed direction and magnitude
OUTCOME_END

---

## RESEARCH APPENDIX

**Queries run**
- web_search: “XLP consumer staples ETF September 18 2026”
- web_search: “US stocks September 18 2026 SPY XLP consumer staples rotation”
- web_search: “leading lagging sectors September 18 2026 consumer staples XLP”
- web_search: “why stocks fell September 18 2026 Fed oil yields consumer staples”
- web_search: “WMT COST PG KO PEP KR XLP September 18 2026 stock movers”
- web_search: “September 18 2026 triple witching stocks yields oil consumer staples”
- web_search: site:benzinga.com leading and lagging sectors September 18 2026
- web_search: “Consumer Staples” OR XLP “September 18” 2026 sector ETF
- web_search: “PepsiCo Costco Walmart Procter Gamble stock price September 18 2026”
- web_search: “10-year Treasury yield September 18 2026 5 percent stocks mixed”
- web_search: “PepsiCo PEP stock September 18 2026 down why”
- web_search: “US stocks mixed September 18 2026 industrial production utilities technology yields”
- web_search: “XLP open close September 18 2026 83.14 82.80”
- x_search: “XLP consumer staples ETF performance rotation September 18 2026 vs SPY” (2026-09-18 to 2026-09-19)
- x_search: “XLP OR staples OR PepsiCo OR PG OR Walmart stock today September 18 2026 lagging yields” (2026-09-18 to 2026-09-19)
- web_fetch: Benzinga leading/lagging (403), AP (403), Reuters (401), Yahoo (fail), Fool midday, Toledo Blade (thin), Fool Fed-hike, Morningstar/DJ 10Y data talk, 24/7 Wall St PEP (403), RTTNews close, Fool Dow week, stocknear XLP history (403)

**Key sources (title + URL + timestamp / as-of)**
1. Channel 1 actuals — 2026-09-18 close. Facts: XLP −0.826%, SPY −0.119%, rel −0.707%, open 83.14 / close 82.80.
2. Morning sector card (injected) — 2026-09-18 pre-open. Facts: predicted up/mild, S0 −1 / S1 −0.5 / S2–S4 0, overlay −2.2, tape_anchor ES +1.14% / PM XLP −0.01%, divergence flagged.
3. RTTNews — “U.S. Stocks Close Mixed Following Choppy Trading Day” — https://www.rttnews.com/3692364/u-s-stocks-close-mixed-following-choppy-trading-day.aspx — 2026-09-18. Facts: Nasdaq +0.4% 26,522.55; S&P +0.2% 7,650.50; Dow −0.2% 51,682.64.
4. Dow Jones / Morningstar Data Talk — “10-Year Treasury Yield Rises to 4.995% This Week” — https://www.morningstar.com/news/dow-jones/202609186066/10-year-treasury-yield-rises-to-4995-this-week-data-talk — 2026-09-18 15:47 ET. Facts: +4.9 bp today; largest 1-day yield gain since Sep 10; second-highest yield this year.
5. Motley Fool midday — https://www.fool.com/coverage/stock-market-today/2026/09/18/stock-market-midday-sept-18-stocks-slip-crypto-gains/ — 2026-09-18 ~11:44 a.m. ET. Facts: 10Y +5 bp to 5.00%; most sectors flat/falling.
6. Motley Fool — “The Dow Is Down for a Third Straight Week…” — https://www.fool.com/investing/2026/09/18/the-dow-is-down-for-a-third-straight-week/ — 2026-09-18. Facts: 10Y 5.004%; WTI ~+1% to ~$103; Broadcom lift; mixed oil/Hormuz tape.
7. Motley Fool — Fed hike follow-up — https://www.fool.com/investing/2026/09/18/the-fed-just-hiked-interest-rates-are-there-more-h/ — 2026-09-18. Facts: Sep 16 25 bp hike paid; another hike priced; Thursday SPX +1.1% relief.
8. Benzinga — “Leading And Lagging Sectors For September 18, 2026” — https://www.benzinga.com/etfs/sector-etfs/26/09/61867052/leading-and-lagging-sectors-september-18-2026 — 2026-09-18 (intraday snapshot). Facts: XLC +0.25%, XLK +0.07%; XLP −0.38% among laggards. **Not used as the close** (Channel 1 −0.83% wins).
9. 24/7 Wall St / search corroboration — PEP ~−3% 52-week low vs KO flat — https://247wallst.com/investing/2026/09/18/pepsico-falls-3-while-consumer-staples-hold-firm-keurig-dr-pepper-eases-coca-cola-barely-budges/ — 2026-09-18. Facts: PEP idiosyncratic smash. Ignore “staples hold firm” vs Channel 1.
10. ChartExchange / Stocknear (search) — XLP history — https://chartexchange.com/symbol/nyse-xlp/historical/ — 2026-09-18. Facts: open ~83.14, close ~82.80–82.83, range ~82.82–83.19.
11. X — StockMKTNewz — https://x.com/StockMKTNewz/status/2101037703341203606 — 2026-09-18. Fact: PEP among 52-week lows.
12. X — baalhadid sector board — https://x.com/baalhadid/status/2100942750933815493 — 2026-09-18. Fact: XLP tagged weak vs SPY (approx −0.6% vs −0.1% in that post; Channel 1 is the official tape).

**Not used as Channel 1 substitutes:** weeklytrader “PEP −8% / COST −7%” (inconsistent with XLP −83 bp and other PEP ~−3% prints); AP/Reuters full text (blocked); Finviz-style YTD RS claims; Benzinga −0.38% as the official close.