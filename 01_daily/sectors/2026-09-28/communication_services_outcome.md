# Sector Outcome — Communication Services — 2026-09-28

Actuals: {'etf': 'XLC', 'pct': -1.5757779689479001, 'spy_pct': -0.7441486246259066, 'rel': -0.8316293443219935, 'open': 112.7699966430664, 'close': 111.18000030517578, 'source': 'yf_download'}

OUTCOME_BEGIN
SECTOR: Communication Services
ETF: XLC
ETF_PCT: -1.5758
SPY_PCT: -0.7441
REL_PCT: -0.8316
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Duration shock (10Y to 5.23%) hit the META-concentrated growth book; META cash continuation after Friday’s NM verdict did most of the ETF damage.
KEY_INTERACTION: Oil/Hormuz fed the same yield impulse (not a second S0); META legal overhang amplified duration on ~23% of XLC while GOOGL did not confirm.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Factor lean down was right; published flat/flat was a PM-fight/size_gate flatten that cash invalidated.
OUTCOME_END

## 0. Facts

XLC cash: open **112.77** → close **111.18** (**−1.576%**). High **$112.80** / low **$111.00** — opened on the high, closed on the low. SPY **−0.744%**. Relative **−0.832%**. Path is a cash-session selloff, not a premarket gap that mean-reverted.

S&P 500 **−0.8%** to 7,683.69; Nasdaq **−0.9%** to 26,820.38; Dow **−0.7%**. 10Y **5.23%**, highest since 2007. That is a down day for a duration/growth book, and XLC lost to the market.

Morning published call: **flat / flat** (engine total **−4.633**, size_gate on, RS veto on). LLM factor lean was **down, mild** (S0=−1, S1=−1, S2–S4=0) against PM:XLC **+0.06%**. Actual: **down / notable**. Direction miss on the published card; factor lean was the right sign and too small.

## 1. What drove the sector

Taxonomy, in order:

**Real yields / duration (HIT, load-bearing).** The 10Y jumped to **5.23%** (highest since 2007) as oil swung on the Hormuz standoff. Morning already had DGS10 **5.18**, DFII10 **2.85 / +53 bp 1m**, 10Y–SPX corr **−0.877**, Oct hike odds **~64–69%**. That object printed in cash. XLC is a two-name duration book (META ~23%, Alphabet A+C ~20%). This is S0.

**Regulatory crackdown continuation on the largest holding (HIT, S1).** No new Monday legal print. Penalty still unprinted. META still paid: Friday **−3.33%**, Monday **−4.79%** to **$715.62** (high $750.6 / low $713.2, volume 26.1M). Morning PM was **~$732 / ~−2.5%**; cash went further. ~23% × −4.79% ≈ **−1.1 pp** of XLC’s −1.58%. That is most of the ETF.

**Risk-off tape / equity beta compression (HIT, shared).** Nasdaq **−0.9%**, XLC **−1.58%**. NVDA buyback was the offset for the index, not for this book. Asia already **−0.88%** overnight.

**Oil / Hormuz (interaction, not a second S0).** Trump’s weekend rejection of Iran’s 7-day Hormuz reopen was live at open. Brent through **$107** in Asia. WTOP ties the 10Y spike to oil swings. Morning correctly refused to stack oil as a second minus on top of yields. That call still holds: oil transmitted *through* duration.

**Not drivers.** No same-morning ad-spend print. Connect/Muse/eMarketer still leftover. Google ad-tech ruling still stale. Dallas Fed TMOS printed **9.8 vs 11.6** (production **29.5**, raw-materials prices **52.2**) — same rates/data cluster, not an XLC catalyst. AMX upgrade still nested. 09-23 $123.8M XLC redemption still plumbing.

## 2. Audit of morning S0–S4 (use morning numbers)

| Cell | Morning | Reality | Verdict |
|---|---|---|---|
| **S0 −1** | One duration object; oil not stacked; Finviz not a crash tape | 10Y 5.23%; SPX −0.8%; NDX −0.9%; oil fed yields | **Correct object, understated transmission.** −1 was the right cap vs −2; the *book* felt it as notable because of META weight. |
| **S1 −1** | META NM verdict, PM ~−2.5%; leftover ad/AI = 0; not a full-book −3 | META −4.79%; GOOGL only −0.34%; no new ad print | **Right signed cell, too small vs cash.** Cap for single-name was principled; cash continuation exceeded the PM. |
| **S2 0** | Remainder mixed; don’t reuse META; NFLX PM ~+0.5% | GOOGL −0.34%, DIS −0.53%, TMUS **+0.62%**, NFLX **−2.69%** | **Mostly holds.** Remainder was mixed, not a washout. NFLX PM green → cash −2.69% was the miss inside the cell, not a reason to have copied META into S2. |
| **S3 0** | 09-23 outflow is T+3 plumbing; engine weight 0 | No same-day flow story | **Holds.** |
| **S4 0** | Friday 1d rel −1.45% leftover; live PM **+0.06%** does not confirm down | Open 112.77 → close 111.18; high-to-low down day | **Honest at 4am, false as confirmation.** A 6 bp PM print is noise, not a tape fight. |

**Hit-grid vs cash**
- Risk-on expansion: morning MISS — confirmed.
- Risk-off / flight: morning HIT — confirmed (indexes red; XLC worse than SPY).
- Real yields rising: morning HIT — confirmed (10Y 5.23%).
- Large-cap leadership: morning MISS — confirmed. META led *down*. GOOGL did not lead.
- Digital ad recovery / AI monetization proof: morning MISS — confirmed, leftover.
- Regulatory crackdown: morning HIT (dated 09-25) — **continued** into Monday cash, no new filing.
- Sector rotation out of comms: morning WATCH on leftover Friday rel — **fresh print today** (rel −0.83%).

**Published vs lean.** LLM wrote: factor lean down/mild; PM fight is why conviction is cut, not why S0/S1 are zeroed. Engine then emitted **flat/flat** (`calendar_size_gate_applied: true`, `sector_rs_veto_applied: true`). That flatten is the miss. Do-instead “if conviction fights live tape, prefer flat/mild” treated **PM:XLC +0.06%** as a fight. It wasn’t. The live sleeve was ES **−0.53%** / NQ **−0.97%** and META PM **−2.5%**. 09-25’s lesson was “don’t flatten a negative card off a *green* ES/NQ sleeve while XLC is absent.” Today the sleeve was red and XLC was on the board. Flattening anyway repeated the 09-25 error in a different costume.

## 3. Interactions / double-count / knowable-at-open

**Same-shock (counted once, correctly):** 10Y, hike odds, hawkish path, Dallas Fed/Barkin cluster = one rates object. Oil not stacked on S0. Muse+Connect+eMarketer = one leftover, left at 0. META legal in S1 only, not S2. NQ/XLK not mapped onto XLC as an up-creator.

**Interaction that *did* fire:** Hormuz/oil → yields → duration book, with META already impaired. That is one pipe with a wounded 23% weight, not two independent minuses. GOOGL **−0.34%** vs META **−4.79%** is the proof they are not the same object.

**Knowable at open: partially.**
- Yes: 10Y multi-decade highs, hike-odds cluster, red NQ sleeve, Hormuz rejection, META PM ~−2.5%, Friday legal overhang, penalty unprinted.
- No / only in cash: META finishing **−4.79%** vs PM **−2.5%**; NFLX reversing from PM green to **−2.69%**; XLC selling from 112.77 to 111.18 after a +6 bp PM.
- Dallas Fed was a same-day print and not the driver.

The open **112.77** vs close **111.18** is the knowable-at-open test in one line: the down day was mostly *session*, so a flat PM was not a forecast of a flat cash close.

## 4. Outliers inside the sector

- **META −4.79%** ($715.62, range $750.6–$713.2): the ETF. Legal leftover + duration. Not a new Monday filing.
- **GOOGL −0.34%** ($342.75): two-name book did **not** dual-lead lower. Morning “not a full-book −3” (09-09) still right.
- **NFLX −2.69%** ($69.23): entertainment failed after a green PM. Nested MAP HEAT must not lift the parent — and today it didn’t; it dragged. Secondary, not the thesis.
- **TMUS +0.62%** ($166.45): telecom/low-beta outlier. Did not save XLC. AMX-class names remain non-drivers.
- **DIS −0.53%**: in line with SPY, not an idiosyncratic print.

Concentration math: META alone explains most of −1.58%. Remainder-of-book was mixed-to-soft, not a crash. That is why S2=0 was defensible even though S1 cash was worse than scored.

## Evidence

CLAIM: XLC closed $111.18, −1.58% on 2026-09-28 (open ~$112.77, high $112.80, low $111.00).
URL: https://stockscan.io/stocks/XLC/price-history
PUBLISHED: 2026-09-28 (session close table)
QUOTE: “Sep 28, 2026 $112.8 $111.0 … −1.58%” / “latest closing stock price as of September 28, 2026, is $111.18”
SUMMARY: Matches injected actuals (open 112.77, close 111.18, −1.576%). Down-day path, open-near-high.

CLAIM: S&P 500 −0.8% to 7,683.69; Nasdaq −0.9% to 26,820.38; 10Y to 5.23%, highest since 2007, after oil swings on Hormuz uncertainty.
URL: https://wtop.com/national/2026/09/how-major-us-stock-indexes-fared-monday-9-28-2026/
PUBLISHED: 2026-09-28
QUOTE: “The yield on the 10-year Treasury jumped to 5.23% and touched its highest level since 2007 following the latest swings for oil prices.”
SUMMARY: Shared macro tape was duration + oil, indexes mildly-to-notably red. XLC (−1.58%) lost to SPY (−0.74%).

CLAIM: META closed $715.62, −4.79% on 2026-09-28 after Friday −3.33%.
URL: https://stockscan.io/stocks/META/price-history
PUBLISHED: 2026-09-28
QUOTE: “Sep 28, 2026 $750.6 $713.2 … −4.79%” / Friday “−3.33%”
SUMMARY: Largest XLC weight extended the Friday legal dump; cash worse than morning PM ~−2.5%.

CLAIM: GOOGL −0.34% to $342.75 on 2026-09-28.
URL: https://stockscan.io/stocks/GOOGL/price-history
PUBLISHED: 2026-09-28
QUOTE: “Sep 28, 2026 … −0.34%”
SUMMARY: Second captain did not confirm META. Two-name book split.

CLAIM: NFLX −2.69%; DIS −0.53%; TMUS +0.62% on 2026-09-28.
URL: https://stockscan.io/stocks/NFLX/price-history ; https://stockscan.io/stocks/DIS/price-history ; https://stockscan.io/stocks/TMUS/price-history
PUBLISHED: 2026-09-28
QUOTE: NFLX “−2.69%”; DIS “−0.53%”; TMUS “+0.62%”
SUMMARY: Remainder mixed. Entertainment failed (NFLX). Telecom did not drive the parent.

CLAIM: Friday 2026-09-25 NM jury found Meta liable on 26/29 statements, 43.9M UPA counts; penalty unprinted, statutory cap $5k/count.
URL: https://ppc.land/meta-loses-new-mexico-facebook-trial-as-jury-finds-43-9-million-violations/
PUBLISHED: 2026-09-25
QUOTE: “A Santa Fe jury on Friday, September 25, 2026, decided that Facebook willfully broke New Mexico's Unfair Practices Act through 26 public statements… Jurors recorded 43,899,720 violations, leaving a district judge to set a penalty that state law caps at $5,000 for each one.”
SUMMARY: Monday had no new legal number. S1 was continuation, not a fresh print.

CLAIM: Oil jumped after Trump rejected Iran’s 7-day Hormuz reopen; Brent >$107 in Asia Monday.
URL: https://www.aljazeera.com/economy/2026/9/28/oil-prices-surge-after-trump-rejects-irans-plan-to-reopen-strait-of-hormuz
PUBLISHED: 2026-09-28
QUOTE: “Brent crude, the international benchmark, rose more than 3 percent on Monday, nearing $108 a barrel during trading in Asia.”
SUMMARY: Weekend geo object was knowable at open; transmitted via yields, not as a separate XLC factor.

CLAIM: Dallas Fed September TMOS general business activity 9.8 vs 11.6; production 29.5; raw materials prices 52.2.
URL: https://www.dallasfed.org/research/surveys/tmos/2026/2609
PUBLISHED: 2026-09-28
QUOTE: “The production index… jumped 13 points to 29.5” / “general business activity index edged down to 9.8 from 11.6” / “raw materials prices index advanced eight points to 52.2”
SUMMARY: Same-day data, hot prices / still-positive activity. Not an XLC spine. Morning was right not to pre-score it as a second hit.

---

## RESEARCH APPENDIX

**Queries run**
- memory_search: Communication Services XLC sector outcome lessons Meta Alphabet 2026-09-28 (index unavailable)
- memory_search: XLC post-session review magnitude bands S0 S1 Meta legal duration (index unavailable)
- web_search: XLC Communication Services ETF September 28 2026 performance Meta Google
- web_search: Meta stock META New Mexico verdict September 28 2026
- web_search: stock market September 28 2026 Treasury yields Fed hike SPY Nasdaq
- web_search: GOOGL NFLX DIS VZ TMUS CMCSA stock September 28 2026
- web_search: META stock close September 28 2026 Meta Platforms
- web_search: Dallas Fed manufacturing September 28 2026 Barkin
- web_search: oil prices Hormuz Trump Iran September 28 2026 stocks
- web_search: sector performance September 28 2026 XLC XLK XLE communication services
- web_search: Alphabet GOOGL close September 28 2026
- web_search: NFLX DIS VZ TMUS CMCSA T stock price history September 28 2026 close
- web_search: 10 year Treasury yield close September 28 2026 5.23
- web_search: site:reuters.com stocks September 28 2026 yields oil Meta
- web_search: XLC open close September 28 2026 111.18
- x_search: XLC/META/GOOGL 2026-09-28 movers (from 2026-09-28 to 2026-09-29)
- web_fetch: WTOP 9/28/2026 index recap
- web_fetch: Yahoo Finance live blog (fetch failed)
- web_fetch: StockScan XLC, META, GOOGL, NFLX, DIS, TMUS history
- web_fetch: Dallas Fed TMOS 2026-09
- web_fetch: Al Jazeera Hormuz/oil 2026-09-28
- web_fetch: ppc.land Meta NM verdict
- web_fetch: Reuters global markets 2026-09-28 (401 / JS wall)

**Key sources and facts taken**

1. Injected Channel 1 actuals (pipeline) — XLC −1.5758%, SPY −0.7441%, rel −0.8316%, open 112.77, close 111.18. **Used as given.**
2. Morning prediction card (injected, 2026-09-28) — published flat/flat; S0=−1, S1=−1, S2–S4=0; PM:XLC +0.06%; META PM ~−2.5%; size_gate + RS veto; factor lean down/mild. **Audited, not rewritten.**
3. StockScan XLC — close $111.18, −1.58%, high $112.8 / low $111.0. https://stockscan.io/stocks/XLC/price-history
4. StockScan META — −4.79% to $715.62; Friday −3.33%. https://stockscan.io/stocks/META/price-history
5. StockScan GOOGL — −0.34% to $342.75. https://stockscan.io/stocks/GOOGL/price-history
6. StockScan NFLX / DIS / TMUS — −2.69% / −0.53% / +0.62%.
7. WTOP, 2026-09-28 — SPX −0.8% to 7,683.69; Nasdaq −0.9%; 10Y 5.23% highest since 2007; oil/Hormuz; NVDA buyback offset. https://wtop.com/national/2026/09/how-major-us-stock-indexes-fared-monday-9-28-2026/
8. Al Jazeera, 2026-09-28 — Trump rejected Iran 7-day Hormuz plan; Brent >$107, +>3% Asia. https://www.aljazeera.com/economy/2026/9/28/oil-prices-surge-after-trump-rejects-irans-plan-to-reopen-strait-of-hormuz
9. ppc.land, 2026-09-25 — NM jury 26/29 statements, 43,899,720 violations; penalty TBD. https://ppc.land/meta-loses-new-mexico-facebook-trial-as-jury-finds-43-9-million-violations/
10. Dallas Fed TMOS, 2026-09-28 — activity 9.8 vs 11.6; production 29.5; raw materials prices 52.2. https://www.dallasfed.org/research/surveys/tmos/2026/2609
11. X search 2026-09-28..29 — tape chatter on yields/oil/Fed; no new META legal print. Not used as a price source.
12. Reuters global-markets URL — fetch blocked (401/JS). Not used as a primary quote.
13. Memory index — unavailable this run (embedding metadata missing). Morning scoreboard/lessons used as injected.

**Lesson to keep:** A **+6 bp PM:XLC** against a **red NQ sleeve** and a **−2.5% META PM** is not a tape fight. Size_gate/do-instead flattened a correctly signed S0+S1 card to flat/flat; cash was down/notable. Next time this setup prints, keep **down/mild** (or down/notable if META PM is already ≥1% and duration is live). Do not let calendar_size_gate convert a 6 bp noise print into an up-or-flat veto.