# Sector Outcome — Healthcare — 2026-09-18

Actuals: {'etf': 'XLV', 'pct': -0.24879934542948456, 'spy_pct': -0.11932509489422927, 'rel': -0.1294742505352553, 'open': 168.27000427246094, 'close': 168.38999938964844, 'source': 'yf_download'}

Memory search is paused (embedding index metadata missing). Review uses injected Channel 1 actuals and live sources, not MEMORY.md.

## 0. FACTS

**XLV 2026-09-18:** −0.249% (open 168.27 → close 168.39). Prior close ~168.81. Path: **gap-down ~0.32%**, then a small bounce that never recovered the open. Not a smash; not a bid.

**SPY:** −0.119%. **Relative:** −0.129%.

**Direction:** down. **Magnitude:** mild (abs ~0.25%, rel ~−0.13%).

**Cash vs morning futures:** Nasdaq Composite **+0.39%** to 26,522.55; Dow **−0.18%**; S&P 500 index **+0.17%** to 7,650.50 — while **SPY cash was −0.12%**. Triple-witching Friday. XLK ~**+0.8%**; XBI **−0.97%** to $156.72. 10-year **5.00%** (+5 bp midday). Russell 2000 **−0.5%**.

Morning engine printed **up / mild** (total 2.404) off tape_anchor ES **+1.14%** + index_carry, against an LLM card of **S0 −0.5 / S1–S4 0**, PM:XLV **−0.01%**, and an explicit ban on **up/notable**.

---

## 1. What drove Healthcare today

Taxonomy, not a stock-picker:

- **S0 / risk-on tape, HC as funding source (HIT, confirmed).** NQ/tech led; defensives and small-caps did not. XLK green vs XLV red is the same split as yesterday’s 1d rel **−0.51%** and the live PM (XLV flat vs XLK +0.60%). Oil-offered + NQ≥ES was a **cyclical/high-beta impulse**, not a duration tailwind into XLV.
- **Real yields / 10y sticky (second-order).** 10-year back to **5.00%**. That is a mild duration/XBI-sleeve drag, not a new FOMC shock (09-16 already paid).
- **S1 spines did not re-rate the basket.** No live MA-rate HIT. IRA/MFN residual. FOMC paid. LLY **Inluriyo+Verzenio** combo approval printed **same day** and **did not move LLY** (flat) — incremental oncology, not an XLV bid. XENE psychiatry enrollment pause was **pre-open / 09-17 AH**, single-name, and **must not dominate**; it did help **XBI −0.97%**.
- **S2 breadth:** mega-caps mixed/quiet (UNH modest green, LLY/JNJ/ABBV ~flat-to-soft). Biotech sleeve weaker than large-cap HC. Nested RUT bid from the morning heat map **did not hold** (RTY −0.5%).
- **S3 flows:** no evidence of a same-session inflow spike or crowded-long unwind. Triple witching = volume/vol, not an HC directional spine (morning was right).
- **S4 tape:** PM flat → cash **opened weaker** and stayed mildly red. Confirmation sleeve was not a breakout.

**Not the driver:** CMS 2027 MA +2.48% (April, stale); Warsh/JH/gold −3% (stale vs Channel 1); oil-falling restacked as “rotation into healthcare”; AVGO/XLK mapped into S0=+1.

---

## 2. Audit morning S0–S4 (morning numbers, not rewritten)

| Sleeve | Morning | Reality | Verdict |
|---|---|---|---|
| **S0 −0.5** | Tech-led modest risk-on; HC = funding source; PM lag; do not treat oil-off as HC bid | XLK ~+0.8%, Nasdaq +0.39%, XLV −0.25%, rel −0.13% | **Correct sign.** Cash SPY −0.12% was *less* risk-on than Finviz ES +0.20% and **far less** than yfinance ES +1.14%. |
| **S1 0** | No live MA/IRA/MFN/FOMC; XBI not leadership; single-names banned | LLY FDA combo = same-day mega-cap Rx **headline**, stock **flat**. XENE cratered, XBI −0.97%, XLV only −0.25% | **Basket score holds.** Missed that a mega-cap FDA print could appear *during* the session; impact was correctly ~0. |
| **S2 0** | Split heat; don’t copy 1w RS; facilities/RUT nested ≠ XLV | Mega-caps mixed; biotech weaker; RTY −0.5% | **Holds.** |
| **S3 0** | −$151M 1d (09-17); weekly ~$43M not crowding; witching ≠ bid | Mild down, no flow event visible in the ETF print | **Holds.** |
| **S4 0** | PM −0.01% not breakout/smash; 1d rel −0.51% is *yesterday* | Gap-down, close −0.25% | **Slightly too neutral on cash open**, still right that it wasn’t a smash or a catch-up. |

**Engine vs card:** LLM leading sum **−0.5**, PM **flat/lag**, **no factor-vs-PM divergence**. Pipeline still emitted **predicted_direction = up** because tape_anchor weighted ES **+1.14%** (w=0.6) over PM:XLV **−0.01%** (w=0.7) and index_carry **1.215**. Morning text **already flagged this** (“trust factors over that ES object”; 09-17 mild-PM distinguisher **absent**). **08-13 reversal-tell** banned **up/notable**; engine printed **up/mild** anyway.

Direction call **missed**. Magnitude band **mild** was the right *size* of day, wrong *sign*.

---

## 3. Interactions / double-count / knowable-at-open

- **No oil double-count:** morning did not restack CL=F −6.3% as rotation *into* XLV. Good. Oil-off + NQ green stayed in **S0 funding-source** only.
- **No leftover 1w RS (+1.27%) in S2/S4.** Good. That repair faded, as HORIZON_3D/1W fade sketched.
- **FOMC/SEP not restacked.** Good. 10y at 5% is continuation, not a new binary.
- **XENE vs XBI vs XLV:** correctly kept out of S1 as single-name; still showed up as **XBI −0.97%** vs XLV −0.25% — sleeve drag, not an XLV smash. Do not promote it to a sector spine after the close.
- **LLY FDA vs 08-14:** not knowable as a *morning* HIT if it dropped after the card; **knowable as “don’t let one Rx headline dominate”** — and it didn’t (LLY flat).
- **SPX +0.17% vs SPY −0.12%:** expiry-Friday ETF vs index. Do not audit XLV against SPX and call it “market up, HC down” without the Channel 1 SPY print.
- **KNOWABLE AT OPEN:** **partially.** Relative lag / “not a bid” / “not up/notable” was **knowable** (PM −0.01% vs XLK +0.60%, 1d rel −0.51%, NQ leading, 08-13). Absolute **down vs SPY slightly down** was only partly knowable — PM was flat, cash gapped. The **engine up** print was **not** justified by knowable-at-open factors.

---

## 4. Outliers inside the sector

- **XENE:** ~−25% to −30% on azetukalner psychiatry enrollment pause (neuropsychiatric AEs) plus focal-seizure NDA. **Single-name.** Helped XBI, not XLV weights.
- **XBI −0.97% vs XLV −0.25%:** high-beta biotech **underperformed** large-cap HC. Morning “not XBI leadership” confirmed; 09-17 XBI +2.64% was one-day catch-up, faded.
- **LLY:** FDA Inluriyo+Verzenio combo; **stock ~flat**. Incremental, not a mega-cap bid.
- **UNH ~+0.45%:** modest offset, not an insurer smash and not a sector bid. Utilization remains structural (09-09), not today’s binary.
- **JNJ / ABBV:** ~flat to slightly red. Quiet large-cap pharma — matches morning “LLY/JNJ none.”
- **RTY −0.5%:** kills any leftover nested small-cap/facilities overlay as an XLV tell.

---

## Evidence

CLAIM: XLV closed −0.249% on 2026-09-18 (open 168.27, close 168.39); SPY −0.119%; rel −0.129%.  
URL: (Channel 1 deterministic actuals, this run)  
PUBLISHED: 2026-09-18 session  
QUOTE: ETF_PCT −0.2488; SPY_PCT −0.1193; REL_PCT −0.1295  
SUMMARY: Mild down, slight lag vs SPY; gap-down then small bounce.

CLAIM: S&P 500 +0.17% to 7650.50 Friday; Nasdaq +0.39% to 26522.55; Dow −0.18% to 51682.64; RTY −0.5%.  
URL: https://www.morningstar.com/news/dow-jones/202609186726/sp-500-falls-008-this-week-to-765050-data-talk  
PUBLISHED: 2026-09-18 16:31 ET  
QUOTE: “Today it is up 12.74 points or 0.17%”  
SUMMARY: Index tape was NQ-led mixed; SPY cash (−0.12%) lagged SPX on witching Friday.

CLAIM: Nasdaq +0.39% Friday; 10-year at 5.00%; majority of stocks fell.  
URL: https://wtop.com/national/2026/09/how-major-us-stock-indexes-fared-friday-9-18-2026/  
PUBLISHED: 2026-09-18  
QUOTE: “The majority of stocks on Wall Street fell, and pressure picked up on them as the yield on the 10-year Treasury climbed to 5.00%.”  
SUMMARY: Breadth weak under a Nasdaq-up print; yields sticky — hostile to low-beta HC catch-up.

CLAIM: Midday, most sectors flat/falling; 10y +5 bp to 5.00%.  
URL: https://www.fool.com/coverage/stock-market-today/2026/09/18/stock-market-midday-sept-18-stocks-slip-crypto-gains/  
PUBLISHED: 2026-09-18 ~11:44 ET  
QUOTE: “Most sectors were trading flat or falling, with only utilities showing growth.”  
SUMMARY: Not a broad risk-on melt-up; HC had no defensive bid either.

CLAIM: XBI closed $156.72, −0.97%; XLV ~−0.25%; XLK ~+0.5% to +0.84%.  
URL: https://www.financecharts.com/etfs/XBI/summary/price  
PUBLISHED: 2026-09-18 close  
QUOTE: XBI −$1.53 / −0.97% from $158.25  
SUMMARY: Biotech sleeve lagged XLV; tech led. Funding-source split confirmed.

CLAIM: Xenon paused Phase 3 depression/bipolar enrollment on neuropsychiatric AEs; shares fell as much as ~30% Friday.  
URL: https://www.biopharmadive.com/news/xenon-azetukalner-depression-trial-halt-adverse-events/830756/  
PUBLISHED: 2026-09-18  
QUOTE: “Shares of high-flying Xenon Pharmaceuticals fell by as much as 30% Friday morning following an announcement that it has paused enrollment in trials evaluating its lead drug in people with depression.”  
SUMMARY: Pre-open single-name; XBI-relevant, not an XLV spine.

CLAIM: FDA approved Inluriyo (imlunestrant) + Verzenio (abemaciclib) for ER+/HER2−/ESR1-mutated advanced breast cancer; LLY ~flat.  
URL: https://www.prnewswire.com/news-releases/us-fda-approves-inluriyo-imlunestrant-in-combination-with-verzenio-abemaciclib-for-adults-with-er-her2-esr1-mutated-advanced-or-metastatic-breast-cancer-302883515.html  
PUBLISHED: 2026-09-18  
QUOTE: combo PFS 11.1 vs 5.5 months in ESR1-mutated EMBER-3 subgroup  
SUMMARY: Same-day mega-cap Rx headline, **no basket bid**.

CLAIM: Triple witching ~$7T options expiry Friday 2026-09-18.  
URL: https://www.bloomberg.com/news/articles/2026-09-18/options-triple-witching-sees-7-trillion-expiring-on-friday  
PUBLISHED: 2026-09-18  
QUOTE: (search wrap) ~$7 trillion expiring, second-largest on record  
SUMMARY: Volume/vol event; not an HC directional catalyst. Helps explain SPY vs SPX gap.

---

OUTCOME_BEGIN
SECTOR: Healthcare
ETF: XLV
ETF_PCT: -0.2488
SPY_PCT: -0.1193
REL_PCT: -0.1295
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: NQ/tech-led session with XLV as low-beta funding source; no HC spine, XBI sleeve softer.
KEY_INTERACTION: Engine tape_anchor (ES +1.14%) minted up/mild against S0=-0.5 and PM:XLV -0.01%; cash SPY also failed to match that ES object.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Factor card (lag, not-up, 08-13 ban) was right; v2 up/mild override was the miss.
OUTCOME_END

---

## RESEARCH APPENDIX

**Queries run**
- memory_search: `Healthcare XLV 2026-09-18 sector prediction lessons` (disabled — index metadata missing)
- web_search: `XLV healthcare ETF September 18 2026 performance` (week)
- web_search: `SPY XLV sector performance Friday September 18 2026 triple witching` (week)
- web_search: `healthcare stocks news September 18 2026 UNH LLY JNJ XBI FDA` (week)
- x_search: `XLV healthcare ETF September 18 2026 why down vs SPY drivers UNH LLY XBI` (2026-09-18 to 2026-09-19)
- web_search: `stock market recap September 18 2026 S&P 500 sectors healthcare technology` (week)
- web_search: `XBI XLV XLK September 18 2026 close percentage` (week)
- web_search: `Eli Lilly Inluriyo FDA approval September 18 2026` (week)
- web_search: `Xenon XENE Phase 3 pause September 18 2026 healthcare` (week)
- web_search: `UNH LLY JNJ ABBV MRK TMO BSX stock performance September 18 2026` (week)
- web_search: `sector ETF performance Friday September 18 2026 XLK XLV XLE XLF` (week)
- web_search: `site:lilly.com FDA approves Inluriyo combination Verzenio September 18 2026` (week)
- web_search: `Nasdaq Composite Dow Jones close September 18 2026 healthcare lag technology` (day)
- web_search: `Eli Lilly stock reaction Inluriyo Verzenio FDA combo approval September 18 2026` (day)
- web_fetch: FinanceCharts XLV (403 captcha); Morningstar SPX Data Talk (ok); Reuters recap/Lilly/Xenon (401 JS wall); MarketScreener (403); FDA Inluriyo page (404); Lilly PR index (403); Motley Fool midday (ok); Bloomberg witching (403 robot); WTOP AP recap (ok); Morningstar Nasdaq Data Talk (ok); BioPharma Dive Xenon (ok)

**Key sources (title + URL + timestamp) and facts taken**

- Channel 1 actuals (this run, 2026-09-18 close): XLV −0.2488%, SPY −0.1193%, rel −0.1295%, open 168.27 / close 168.39.
- Morningstar / Dow Jones Market Data — “S&P 500 Falls 0.08% This Week to 7650.50 — Data Talk” — https://www.morningstar.com/news/dow-jones/202609186726/sp-500-falls-008-this-week-to-765050-data-talk — 2026-09-18 16:31 ET — SPX +0.17% Friday to 7650.50; week −0.08%.
- Morningstar / Dow Jones — “NASDAQ Composite Rises 0.72% This Week to 26522.55” — https://www.morningstar.com/news/dow-jones/202609186730/nasdaq-composite-rises-072-this-week-to-2652255-data-talk — 2026-09-18 16:31 ET — Nasdaq +0.39% Friday / +0.72% week.
- WTOP / AP — “How major US stock indexes fared Friday 9/18/2026” — https://wtop.com/national/2026/09/how-major-us-stock-indexes-fared-friday-9-18-2026/ — 2026-09-18 — SPX +0.2%, Dow −0.2%, Nasdaq +0.4%, RTY −0.5%; 10y to 5.00%; majority of stocks fell.
- Motley Fool — “Stock Market Midday, Sept. 18: Stocks Slip, Crypto Gains” — https://www.fool.com/coverage/stock-market-today/2026/09/18/stock-market-midday-sept-18-stocks-slip-crypto-gains/ — ~11:44 ET 2026-09-18 — 10y +5 bp to 5.00%; most sectors flat/falling.
- FinanceCharts / search wrap — XBI close $156.72 −0.97%; XLV ~$168.39 −0.25%; XLK ~+0.5–0.84% — https://www.financecharts.com/etfs/XBI/summary/price — 2026-09-18 close.
- BioPharma Dive — “Xenon shares fall following depression trial pause” — https://www.biopharmadive.com/news/xenon-azetukalner-depression-trial-halt-adverse-events/830756/ — 2026-09-18 — XENE as much as −30%; psychiatry enrollment pause; epilepsy NDA continues.
- PR Newswire / Reuters / Lilly wrap — Inluriyo+Verzenio FDA combo approval — https://www.prnewswire.com/news-releases/us-fda-approves-inluriyo-imlunestrant-in-combination-with-verzenio-abemaciclib-for-adults-with-er-her2-esr1-mutated-advanced-or-metastatic-breast-cancer-302883515.html — 2026-09-18 — LLY ~flat.
- Bloomberg search wrap — triple witching ~$7T — https://www.bloomberg.com/news/articles/2026-09-18/options-triple-witching-sees-7-trillion-expiring-on-friday — 2026-09-18 — volume/vol, not HC spine.
- StreetStats / search wrap — XLK ~+0.82% day, XLV ~−0.25% day, XLV week still ~+1.83% — https://streetstats.finance/markets/sectors-industries — 2026-09-18.
- Yahoo/search wrap — UNH ~$376.90 +0.45%; LLY ~$1,152 flat; JNJ ~$270 −0.09% — 2026-09-18 close.
- X search (2026-09-18): XENE −24–27% pre-market on Phase 3 pause; no clean XLV-wide headline beyond single-name/biotech color.

**Not used as XLV spines:** Warsh/JH/gold −3% (contradicts morning Channel 1 gold +0.90%, FOMC paid); CMS 2027 MA +2.48% (April); MFN nine-name (08-31 residual); CI Jefferies (Aug); BSX Citi Neutral (09-17 single-name).