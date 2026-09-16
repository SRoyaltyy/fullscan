# Sector Outcome — Industrials — 2026-09-16

Actuals: {'etf': 'XLI', 'pct': -0.08291346436943847, 'spy_pct': -0.4409916675517711, 'rel': 0.35807820318233263, 'open': 169.61000061035156, 'close': 168.7100067138672, 'source': 'yf_download'}

Memory search is paused this run (embedding metadata mismatch). Used the injected morning Industrials pack + live sources only.

## 0. Facts

XLI cash session **2026-09-16**: open **169.61**, close **168.71**, **−0.083%**. SPY **−0.441%**. Relative **+0.358%**. Path: gapped up from 09-15 close **$168.85** (morning PM ~**$169.85 / +0.59%**), traded a wide event range (search tape ~**170.67 / 167.50**), faded the gap into the close. Close-to-close is **flat**; the path was **not**.

**CLAIM:** XLI −0.083% vs SPY −0.441%, rel +0.358%; open 169.61 / close 168.71.  
**URL:** Channel 1 actuals (injected)  
**PUBLISHED:** 2026-09-16 session  
**QUOTE:** `ETF_PCT: -0.0829; SPY_PCT: -0.4410; REL_PCT: +0.3581; OPEN: 169.61 CLOSE: 168.71`  
**SUMMARY:** Tiny down close, beat the index by ~36 bp after a gap-and-fade.

---

## 1. What drove Industrials today

Primary driver was **S0 event risk printing hawkish in the afternoon**, not a same-morning ISM/durables/factory-orders HIT.

The 25 bp hike to **3.75–4.00%** (12–0) was the paid move. Equities were **green into 14:00 ET** and barely flinched at the statement. The tape broke during **Warsh’s presser**: inflation “too high … for too long,” summer readings not improved, **16/18 dots** wanting **at least one more 2026 hike**. 2Y **4.60% → 4.73%**, 10Y **4.95% → 5.01%**. SPY finished **−0.44%**; Dow **−1.21%**.

**CLAIM:** FOMC hiked 25 bp to 3.75–4.00%, unanimous; statement stressed elevated inflation and a “timelier” return to 2%.  
**URL:** https://www.federalreserve.gov/newsevents/pressreleases/monetary20260916a.htm  
**PUBLISHED:** 2026-09-16, 14:00 EDT  
**QUOTE:** “The Committee decided to raise the target range for the federal funds rate by 1/4 percentage point to 3-3/4 to 4 percent… Inflation remains elevated. Today's policy action will support a timelier return to the Committee's 2 percent goal.”  
**SUMMARY:** Paid hike; hawkish framing, not a surprise cut/hold.

**CLAIM:** Warsh presser + dots, not the hike print, did the equity damage.  
**URL:** https://www.cnbc.com/2026/09/16/fed-meeting-today-live-updates.html  
**PUBLISHED:** 2026-09-16  
**QUOTE:** “The plain fact is that inflation is too high and has been for too long.” / “S&P 500 trims earlier gains as Warsh highlights stubborn inflation.” Pre-presser: “S&P 500 … last up 0.4% … 10-year … 4.947%.”  
**SUMMARY:** Two-sided morning → hawkish afternoon. Knowable as a binary, not as the sign.

**CLAIM:** Industrials were a relative outperformer at **−0.1%** while SPX **−0.4%**, energy **−3.0%**, financials **−1.6%**.  
**URL:** https://hosting.briefing.com/cschwab/InDepth/DailySectorWrap.htm  
**PUBLISHED:** 2026-09-16  
**QUOTE:** “the industrials sector (−0.1%) finished as another relative outperformer.”  
**SUMMARY:** Matches Channel 1: XLI flat-down, **+36 bp vs SPY**. Motley Fool’s “industrials led gains” is the same relative story, sloppier on sign.

Why XLI held vs SPY (taxonomy):

- **Oil/cost LEVEL vs same-session crude change (S0, once):** WTI settled **$102.41, −$3.41 / −3.2%** (Briefing). Morning already had crude **down**. That is **cost relief on the crude tape**, not a Hormuz increment. 08-11/08-12 correctly stayed off.
- **Fed growth language (S0/S1 overlap, count as S0 context):** statement said activity “solid,” “capital investment is robust.” That is a **cyclical cushion** for machinery/capex beta versus a pure risk-off smash.
- **Freight diesel is a different factor (S1):** crude down did **not** equal trucking relief. AAA diesel **~$6.31/gal** records; **JBHT −13.3%** after CFO Brad Delco (Morgan Stanley Laguna, late 09-15) flagged Q3 EPS **−5% to −10% q/q** on fuel-surcharge lag + driver costs. DJT **~−3%**. UPS ~**−3.5%**. Morning’s “oil-down = trucking cost relief” failed this split.
- **Grid/electrical (S1, single-ticker, do not drive ETF):** GEV **~+4.8%** on Laguna backlog comments ($176B, path to **>$200B** early 2027). AXON **+5.96%** bounce. 08-18 held: GEV is **not** an XLI cushion. ETF still faded the gap to flat.

**CLAIM:** WTI −3.2% to $102.41; energy sector −3.0%.  
**URL:** https://hosting.briefing.com/cschwab/InDepth/DailySectorWrap.htm  
**PUBLISHED:** 2026-09-16  
**QUOTE:** “WTI crude settled $3.41 lower (−3.2%) at $102.41 per barrel after CNBC reported that the Saudi pipeline … is expected to restart operations within days.”  
**SUMMARY:** Live oil change stayed **down**; absolute level still elevated.

**CLAIM:** JBHT warned on Q3 fuel/driver costs; stock −13%+.  
**URL:** https://www.cnbc.com/2026/09/16/transport-economy-diesel-prices-iran.html  
**PUBLISHED:** 2026-09-16  
**QUOTE:** “Delco said to expect a drop in earnings between 5% and 10% from the second to third quarter… Shares of J.B. Hunt dropped more than 13%… Dow Jones Transportation Average, a broader gauge of the sector, close to 3% on Wednesday.”  
**SUMMARY:** Same-session freight HIT. Diesel, not WTI, was the transport shock.

---

## 2. Audit morning S0–S4 (use morning numbers, not post-close rewrites)

Morning scores: **S0=0, S1=0, S2=0, S3=0, S4=0**. Predicted **flat / flat** (engine). LLM self-audit wanted **mild** for the 14:00 binary (09-03). `calendar_size_gate_applied: True` but engine still emitted **flat**. Divergence: LLM True / engine False.

| Sleeve | Morning | Reality | Verdict |
|---|---|---|---|
| **S0** | 0. Mixed pause. Don’t pre-score hike. Oil down ≠ squeeze and ≠ cyclical green light. Futures green but not 09-11 unanimous. | Paid 25 bp; **hawkish presser/dots** after 14:30. Crude kept falling. Yields backed up **after** Warsh, not at the open. | **S0=0 was the right pre-print score.** Encoding −1 at the open would have been hindsight. The print was two-sided until the presser. |
| **S1** | 0. No same-morning ISM/durables. Cap S1 (08-18). Cass/construction/GEV **carried**. VRT must not drive ETF. | No ISM. **JBHT/diesel** was a fresh freight HIT. **GEV conference** was a fresh grid print. They **netted**. XLI didn’t follow GEV. | **Net S1≈0 for the ETF was right; the decomposition was incomplete.** Missed diesel≠crude and the Laguna freight warning. 08-18 GEV-not-cushion **HIT**. |
| **S2** | 0. Lag scored once under S4, not again here. | Internals split: freight smash vs GEV/AXON. ETF not “up / names flat.” | **Process HIT.** Don’t restack 1m −7% RS. |
| **S3** | 0. Weekly create vs 1m outflow. | No flow spike drove the close. | **HIT.** |
| **S4** | 0. Confirmation only. Do not sign the +0.59% PM gap. 09-15 fade rule **off** (no oil/duration shock). 08-27 **forbid up**. | Gap-and-**fade** to flat, not gap-and-hold. Did **not** go up. Rel **flipped positive**. | **Direction HIT.** Path qualifier “event-wait” was right; “hold” was wrong. Forbidding up was right. |

**09-03 mild-band:** right about **variance** (intraday range ~1.9%), wrong about **close-to-close**. XLI digested a hawkish FOMC with a **flat** print. SPY took the mild down. Industrials ≠ SPX.

**08-27 forbid-up:** HIT. 1w/1m laggard did not bounce into an up day.

**DO-INSTEAD flatten (leading 0 vs leftover RS):** HIT. Emitting **down** from 1m −7% would have missed the **+36 bp** relative win.

**09-15 better-than-index gap fade:** precondition (live oil/duration shock) was **off**. They still faded the **gap**, but only back to unchanged, not to a down day. Rule correctly blocked treating PM as a long; it should not have been inverted into a down call either. **HIT.**

**09-14 S4=−1 lag rule:** correctly **off** (oil down, long end quiet at open, PM gap *better* than Finviz ES).

---

## 3. Interactions / double-count / knowable-at-open

- **One FOMC, not two shocks.** Hike + Warsh + dots + 2Y backup are **one afternoon event**. Do not also score “real yields rising” as a second S0. Morning’s “don’t encode the hike as paid” was the right anti-double-count.
- **Crude ≠ diesel.** Counting oil-down once in S0 was correct. Treating that as trucking relief **double-used the wrong barrel**. Distillate records + surcharge lag are **S1 freight**, knowable only if Laguna/JBHT was in the morning set (it wasn’t).
- **GEV conference ≠ carried $176B RPO.** Incremental same-day color, but 08-18 already forbids letting it raise the ETF. Confirmed: GEV +5, XLI flat.
- **Retail sales +1.2% (Aug)** printed on the calendar and supported the Fed’s “solid spending” line. That is **index/S0**, not a second industrials spine HIT.
- **Knowable at open:** event risk, oil-down, laggard RS, no ISM. **Not knowable:** Warsh/dots hawkish surprise, JBHT −13%, GEV +5, XLI beating SPY by 36 bp.

**KNOWABLE_AT_OPEN: partially.**

---

## 4. Outliers inside the sector

- **JBHT −13.3%** — diesel/surcharge/driver Q3 warning. Biggest transport wreck; **not** an XLI-setting weight, but it pulled freight/DJT.
- **UPS ~−3.5%** — XLI-relevant transport drag, no separate company print found; beta to the diesel tape.
- **GEV ~+4.8%** — Laguna backlog/pricing. Electrical/grid captain; **must not** set the ETF call (08-18). It didn’t.
- **AXON +5.96%** — bounce after 09-15 smash; A&D/high-beta, not breadth.
- **Dow −1.21% vs XLI −0.08%** — Dow ≠ XLI (IBM/MSFT etc. in the average). Don’t read DJIA as industrials beta.

---

OUTCOME_BEGIN
SECTOR: Industrials
ETF: XLI
ETF_PCT: -0.083
SPY_PCT: -0.441
REL_PCT: 0.358
ACTUAL_DIRECTION: flat
ACTUAL_MAGNITUDE: flat
PRIMARY_DRIVER: Hawkish Warsh/FOMC afternoon fade after a paid 25bp hike; XLI only faded the gap as oil-down and Fed "solid activity/capex" offset a diesel/freight smash.
KEY_INTERACTION: Crude-down (S0 cost relief) ≠ record diesel/JBHT surcharge lag (S1 freight); GEV +5 did not lift XLI (08-18).
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Flat/flat HIT on close; S0=0 correctly refused to pre-score FOMC; missed diesel≠crude and over-hedged magnitude (LLM mild) versus a flat XLI print that beat SPY.
OUTCOME_END

---

## RESEARCH APPENDIX

**Queries run**
- memory_search: `Industrials XLI sector lessons FOMC Warsh September 2026` (index disabled)
- web_search: `FOMC September 16 2026 Fed decision Warsh rate hike industrials stocks`
- web_search: `XLI industrials ETF September 16 2026 performance FOMC`
- web_search: `SPY stock market September 16 2026 Fed meeting close`
- web_search: `leading lagging sectors September 16 2026 industrials XLI CAT GE HON UNP UPS`
- web_search: `oil WTI price September 16 2026 close FOMC`
- web_search: `Kevin Warsh press conference September 16 2026 hawkish industrials relative performance`
- web_search: `UPS stock September 16 2026 drop industrials transports`
- web_search: `site:cnbc.com stock market today September 16 2026 sectors industrials`
- web_search: `GE Vernova stock September 16 2026 FOMC`
- web_search: `J.B. Hunt JBHT fuel costs third quarter warning September 16 2026`
- web_search: `XLI open high low close September 16 2026`
- x_search: `XLI industrials FOMC Fed Warsh September 16 2026 market reaction` (2026-09-16 to 2026-09-17)
- web_fetch: Fed statement; CNBC Fed decision; CNBC live blog; CNBC oil; CNBC diesel/transport; Motley Fool market wrap; Briefing.com Daily Sector Wrap; CNBC GEV/Starbucks show page (thin)

**Key sources and facts taken**
- Federal Reserve FOMC statement (fetched 2026-09-16T21:34:57Z): +25 bp to 3.75–4.00%, 12–0; solid activity, robust capex, inflation elevated. https://www.federalreserve.gov/newsevents/pressreleases/monetary20260916a.htm
- CNBC Fed decision: hike widely anticipated (>90%); 16/18 dots at least one more 2026 hike; Warsh “too high … for too long.” https://www.cnbc.com/2026/09/16/fed-rate-decision-september-2026.html
- CNBC live blog: pre-presser SPX +0.4% / 10Y 4.947%; then fade as Warsh spoke; diesel records; White House called the hike “unfortunate.” https://www.cnbc.com/2026/09/16/fed-meeting-today-live-updates.html
- Briefing.com sector wrap: SPX −0.4%, DJIA −1.2%, Nasdaq ~0; industrials −0.1%; energy −3.0% / WTI $102.41 −3.2%; financials −1.6%; JBHT −13.30%; AXON +5.96%; 2Y 4.73% (+13 bp from pre-announcement 4.60%), 10Y 5.01%; Aug retail sales +1.2%. https://hosting.briefing.com/cschwab/InDepth/DailySectorWrap.htm
- Motley Fool: SPX −0.45% to 7,551; Dow −1.21%; “Industrials led session gains while energy and financial services lagged.” (relative, not XLI sign). https://www.fool.com/coverage/stock-market-today/2026/09/16/stock-market-today-sept-16-stocks-slip-as-fed-raises-rates/
- CNBC transport/diesel: AAA diesel ~$6.31; JBHT Q3 −5% to −10% q/q; JBHT −13%+; DJT ~−3%. https://www.cnbc.com/2026/09/16/transport-economy-diesel-prices-iran.html
- CNBC oil (early print): inventory build / Yanbu; later session oil weaker than this GMT snapshot. https://www.cnbc.com/2026/09/16/oil-falls-as-us-crude-inventories-rise-despite-saudi-supply-concerns.html
- GEV: ~+4.8% on Morgan Stanley conference backlog comments (search/Tipranks citations; Tipranks fetch 403).
- Channel 1 actuals: XLI −0.083%, SPY −0.441%, rel +0.358%, O/C 169.61 / 168.71.
- X/Twitter: little XLI-specific post; general hawkish-presser risk-off. Not used as a price source.

**Not used / conflicts**
- Fool’s funds-rate range “3.50%–3.75%” contradicts the Fed statement; ignored.
- CNBC `.SPLRCI −2.03%` quote is inconsistent with XLI −0.08% and Briefing industrials −0.1%; treated as a stale/wrong quote, not the sector tape.
- Benzinga / Tipranks / Investing.com / Axios fetches 403.