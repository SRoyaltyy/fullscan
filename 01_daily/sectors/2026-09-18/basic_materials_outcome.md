# Sector Outcome — Basic Materials — 2026-09-18

Actuals: {'etf': 'XLB', 'pct': -1.4198332064776609, 'spy_pct': -0.11932509489422927, 'rel': -1.3005081115834316, 'open': 50.42499923706055, 'close': 49.9900016784668, 'source': 'yf_download'}

Memory index is unavailable this run (embedding metadata missing), so this autopsy uses the injected morning card, Channel 1 actuals, and live sources only.

## 0. Facts

XLB **−1.420%** (open **50.425** → close **49.990**). SPY **−0.119%**. Relative **−1.301%**.

Path (Yahoo daily): prior close **50.71** → gap-down open **50.42** → high **50.59** (never reclaimed Thursday’s close) → low **49.88** → close **49.99**. Volume ~9.62M, not a spike.

That is **down / notable**, and a **lagging** print vs SPY — not the morning’s flat/flat, and not the LLM’s “absolute up / mild, lagging” expression.

CLAIM: XLB cash session was a gap-and-grind lower, not a continuation of the +0.51% PM bid.  
URL: https://finance.yahoo.com/quote/XLB/history/  
PUBLISHED: 2026-09-18 session (table as of close)  
QUOTE: “Sep 18, 2026: Open 50.42 | High 50.59 | Low 49.88 | Close 49.99 | Volume 9,620,818”  
SUMMARY: Cash opened ~0.56% below Thursday’s 50.71 close; the morning PM:XLB +0.51% did not survive the open.

CLAIM: S&P materials led sector declines at about −1.4% while the tape was mixed-to-soft.  
URL: https://sundayguardianlive.com/business/sp-500-today-live-index-down-022-at-762074-as-treasury-yields-near-5-oil-prices-stay-volatile-check-top-gainers-losers-what-investors-should-watch-287691/  
PUBLISHED: 2026-09-18 (intraday wrap citing Reuters, ~10:17 a.m. ET)  
QUOTE: “Ten of the 11 major S&P 500 sector indexes were lower during Friday’s session, with the materials index leading the declines with a 1.4% fall, Reuters reported.”  
SUMMARY: Materials was the worst major sector on the day, matching the XLB −1.42% close.

---

## 1. What drove the sector

Primary driver was **hawkish-path duration**, not copper and not a gold smash.

The 10-year backed up to **5.00%** (~+5 bp on the session) as the market priced a second hike in October. Motley Fool had industrials and basic materials as the **bottom performers** by late morning. That is S0 cyclical beta + chemicals-book rate sensitivity, not an industrial-metals spine.

CLAIM: Yields rose to 5% and materials/industrials were the laggards while gold was still green.  
URL: https://www.fool.com/coverage/stock-market-today/2026/09/18/stock-market-midday-sept-18-stocks-slip-crypto-gains/  
PUBLISHED: 2026-09-18, ~11:44 a.m. ET  
QUOTE: “Gold is trading at $4,354.92, up 0.33%, while the 10-Year Treasury yield is up 5 basis points to 5.00%. … Industrials and basic material stocks were the bottom performers this morning.”  
SUMMARY: Cash XLB sold with yields, not with bullion.

Same-morning **industrial production** confirmed the demand overlay the morning card treated as already paid:

CLAIM: August IP 0.0% vs +0.3% expected; manufacturing −0.3%, first decline of 2026.  
URL: https://www.morningstar.com/news/dow-jones/202609183834/us-industrial-production-unchanged-in-august  
PUBLISHED: 2026-09-18 09:50 ET  
QUOTE: “Analysts polled by The Wall Street Journal expected a 0.3% increase. Manufacturing output fell 0.3% in August following seven consecutive months of gains.”  
SUMMARY: A 9:15-style factory miss hit the chemicals/steel/construction sleeve. This was not in the morning “no pending CPI/NFP/FOMC binary” gate.

Metals **did not** explain the ETF. COMEX copper settled **+0.43%** to $6.6150; gold **+0.59%** to $4,385.90. FCX was reported green; NEM mixed-to-soft. The book that should have transmitted Cu/Au (miners) was not the wrecking ball. The wrecking ball was **cyclical industrials inside XLB** plus a steel outlier.

Oil stayed offered (WTI/Brent still easing on the week). That was supposed to be chemicals feedstock relief. It did **not** print in cash XLB — the 09-16 transmission haircut was the correct instinct, and today it was still not haircut enough.

---

## 2. Audit of morning S0–S4 (use morning numbers, do not rewrite them)

Morning card (locked): S0 **+0.5**, S1 **0**, S2 **0**, S3 **0**, S4 **0**, mult **0.85**, predicted **flat / flat**. LLM prose wanted “absolute up / mild, lagging.” Engine flattened via `sector_rs_veto` + `calendar_size_gate`.

**S0 +0.5 — too high.** Knowable tape was mixed, not a materials lean. ES/NQ green vs prior close and XLB PM +0.51% were real, and 09-17 correctly forbade an all-zero flatten. But four-index was already OFF, NQ>ES, Europe red, real yields elevated, hawkish path T+2. Those were reasons for **0**, not +0.5. Cash invalidated the lean: SPY −0.12%, 10Y to 5%, materials worst sector. The 09-17 rule (“don’t call residual flat when ES ≥ +0.5% and PM green”) over-fit Thursday’s miss and under-fit a Friday where the parent PM **did not hold into cash**.

**S1 0 — composition right, sign too kind.** Spine surge/collapse OFF was correct (Cu +0.43%, not a squeeze). China rebound OFF was correct. 8/14 gold sleeve ON was correct as a *sleeve* and wrong as support: gold up, XLB down (09-16 NEM lesson, repeated). Oil-offered chemicals relief did not transmit. Missing from S1: (a) Nucor Q3 guide **after Thursday’s close**, (b) the IP calendar. Net S1 should have been **≤ 0**, more honestly **−0.5 / −1** once steel guidance and factory demand were counted once.

**S2 0 — understated a knowable breadth fail.** Nested HEAT was already Copper/Gold/Al/Ag-inputs/Building Materials/Other industrials **down**, Chemicals **flat**. Morning correctly refused 09-09 S2=−1 (no 8/18 co-move). Cash still printed a sector-wide down day. Breadth failure was the live tell; S2=0 was a conviction cut that should have been a **minus**.

**S3 0 — fine.** No inflow spike, no washout. Volume ordinary.

**S4 0 — cap was right; PM-as-S0 was wrong.** 8/27 S4-cap ON (1d rel −0.44% < 0.5%) correctly blocked confirmed-up. Scoring PM:XLB +0.51% in S0 rather than S4 followed 09-15 “exactly one of S1 or S4,” but then treated a **non-held PM bid** as participation. Cash open 50.42 vs 50.71 was the tape. S4=0 did not cause the miss; using PM as S0 fuel did.

**Engine vs LLM:** Deterministic **flat/flat** was less wrong than the LLM’s “up / mild lagging.” RS veto + size_gate saved a confirmed-up error and still **missed direction**. Last three BM losses already said: when score sign fights tape/breadth, cut conviction, prefer flat/mild. Nested HEAT vs parent PM **was** that fight. The card cut to flat instead of **down / mild**.

---

## 3. Interactions / double-count / knowable-at-open

**Double-count to avoid:** Do not add oil-relief (S1 chemicals) + gold sleeve (S1) + green PM (S0) + green Cu (S1) as four supports. Today those four were one non-transmitting metals/feedstock complex. Yields + IP + steel guidance were the cash factors.

**Same-shock:** FOMC/Warsh was correctly not re-scored as a binary. The **level** (hawkish path, 10Y one print from 5%) still hit cyclicals. “T+2, already in Wednesday’s close” is process, not a ban on scoring **live duration**.

**Knowable at open — partially.**
- Knowable: nested HEAT-down vs parent; 1d/1w/1m relative lag; 8/25 cannot emit confirmed-up; Nucor guide after 09-17 close; hawkish path as overlay; PM bid <1% so 09-10 gap-up OFF.
- Not knowable at the pre-open lock: IP 0.0% vs +0.3% (Seeking Alpha stamp 9:17 a.m. ET); 10Y +5 bp to 5.00% (intraday); cash gap vs the +0.51% PM print.

Nucor was the cleanest miss of something that **was** on the tape before 9:30.

CLAIM: Nucor guided Q3 EPS $5.55–$5.65, below consensus, after Thursday’s close.  
URL: https://www.prnewswire.com/news-releases/nucor-announces-guidance-for-the-third-quarter-of-2026-earnings-302882443.html  
PUBLISHED: 2026-09-17 after the close (cash reaction 2026-09-18)  
QUOTE: “Nucor expects third quarter earnings to be in the range of $5.55 to $5.65 per diluted share.”  
SUMMARY: Steel-sleeve guidance miss was knowable at Friday’s open and showed up as an XLB outlier (reports ~−5% to −6% on NUE). It is not the whole −1.42%, but it is the inside-sector captain the morning card did not name.

---

## 4. Outliers inside the sector

- **NUE / steel:** Q3 guide miss; largest named wrecking ball. Peer STLD also offered in secondary reports. Morning HRC −0.16% / iron ore −0.14% already said “not a steel bid.”
- **FCX / copper:** Green with COMEX +0.43%. Nested copper HEAT-down in the morning was **stale vs Friday cash**. Do not let FCX rewrite the ETF call either way — it was an offset, not the parent.
- **NEM / gold:** Bullion +0.59%; miner mixed-to-soft. Gold ≠ cash XLB, third session in a row.
- **LIN / SHW / chemicals majority:** LIN relatively resilient / slightly green in some tapes; SHW ~flat; APD/ECL/DOW offered. Oil-down did not lift the majority sleeve. That is the 09-08 composition math **failing to transmit**, not a LIN blow-up.
- **Path outlier vs morning PM:** +0.51% PM → −0.56% cash gap. Treat unfilled PM as **non-information** when nested HEAT is already down.

---

OUTCOME_BEGIN
SECTOR: Basic Materials
ETF: XLB
ETF_PCT: -1.4198
SPY_PCT: -0.1193
REL_PCT: -1.3005
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Hawkish-path 10Y backup to 5% plus a same-morning IP miss, expressed in chemicals/steel cyclicals — not copper or gold.
KEY_INTERACTION: Green Cu/Au and offered oil were counted as support that never printed in cash XLB; duration + factory demand + NUE guide were the overlapping hit.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Flat/flat was less wrong than the LLM’s mild-up, but S0=+0.5 on a non-held PM bid missed a down/notable lag; nested HEAT and NUE guide already argued down/mild.
OUTCOME_END

---

## RESEARCH APPENDIX

**Queries run**
- memory_search: Basic Materials XLB sector outcome lessons copper China gold chemicals (index unavailable)
- web_search: XLB ETF September 18 2026 materials sector performance copper gold
- web_search: why materials stocks down September 18 2026 XLB copper gold chemicals
- web_search: XLB LIN NEM FCX SHW APD ECL stock performance September 18 2026
- web_search: SPY S&P 500 close September 18 2026 materials lag
- web_search: copper gold oil DXY 10-year yield September 18 2026 close
- web_search: Linde Sherwin-Williams Freeport Newmont September 18 2026 news
- web_search: site:finance.yahoo.com XLB historical September 2026
- web_search: stock market today September 18 2026 materials sector worst performer yields oil
- web_search: XLB holdings performance LIN NEM FCX SHW DD DOW NUE VMC MLM September 18 2026 percent change
- web_search: COMEX copper close September 18 2026 gold silver WTI Brent
- web_search: Nucor NUE stock September 18 2026 drop earnings guidance
- web_search: S&P 500 materials sector -1.4% September 18 2026 Reuters triple witching
- web_search: 10-year Treasury yield 5 percent September 18 2026 materials stocks
- web_search: industrial production September 2026 manufacturing capacity utilization materials
- web_search: Nucor guidance September 17 2026 after hours Q3 $5.55
- x_search: XLB copper gold LME stocks China property materials sector September 18 2026 (2026-09-18 to 2026-09-19)
- x_search: XLB down materials worst sector yields 5% September 18 2026 (2026-09-18 to 2026-09-19)
- web_fetch: https://www.fool.com/coverage/stock-market-today/2026/09/18/stock-market-midday-sept-18-stocks-slip-crypto-gains/
- web_fetch: https://sundayguardianlive.com/business/sp-500-today-live-index-down-022-at-762074-as-treasury-yields-near-5-oil-prices-stay-volatile-check-top-gainers-losers-what-investors-should-watch-287691/
- web_fetch: https://www.morningstar.com/news/dow-jones/202609185396/comex-copper-ends-the-week-225-higher-at-66150-data-talk
- web_fetch: https://www.morningstar.com/news/dow-jones/202609185394/comex-gold-ends-the-week-045-higher-at-438590-data-talk
- web_fetch: https://www.prnewswire.com/news-releases/nucor-announces-guidance-for-the-third-quarter-of-2026-earnings-302882443.html
- web_fetch: https://seekingalpha.com/news/4644334-industrial-production-growth-stalls-in-august-falling-short-of-expectations
- web_fetch: https://www.morningstar.com/news/dow-jones/202609183834/us-industrial-production-unchanged-in-august
- web_fetch failed/blocked: MarketWatch XLB, Reuters wrap, Yahoo history page, Marketscreener, Rio Times copper

**Key sources and facts taken**

1. **Injected Channel 1 actuals** — session input — 2026-09-18 close — XLB **−1.4198%**, SPY **−0.1193%**, rel **−1.3005%**, open **50.425**, close **49.990**. Used as the official tape; not re-derived.
2. **Yahoo Finance XLB history (via search extract)** — https://finance.yahoo.com/quote/XLB/history/ — 2026-09-18 — O/H/L/C **50.42 / 50.59 / 49.88 / 49.99**, prior close **50.71**, volume **9,620,818**. Used for path (gap-down, failed bounce).
3. **Sunday Guardian / Reuters** — https://sundayguardianlive.com/business/sp-500-today-live-index-down-022-at-762074-as-treasury-yields-near-5-oil-prices-stay-volatile-check-top-gainers-losers-what-investors-should-watch-287691/ — 2026-09-18 ~10:17 a.m. ET — materials **−1.4%**, worst sector; 10Y **4.996%** (+4.9 bp); 10 of 11 sectors lower. Used for sector-rank and yield.
4. **Motley Fool midday** — https://www.fool.com/coverage/stock-market-today/2026/09/18/stock-market-midday-sept-18-stocks-slip-crypto-gains/ — 2026-09-18 ~11:44 a.m. ET — 10Y **5.00%** (+5 bp); gold **+$0.33%**; industrials and basic materials **bottom performers**; second-hike-in-October odds. Used for S0 duration vs gold split.
5. **Dow Jones / Morningstar — COMEX copper** — https://www.morningstar.com/news/dow-jones/202609185396/comex-copper-ends-the-week-225-higher-at-66150-data-talk — 2026-09-18 13:53 ET — copper **+2.85¢ / +0.43%** to **$6.6150**; week **+2.25%**. Used to keep industrial-metal collapse **OFF**.
6. **Dow Jones / Morningstar — COMEX gold** — https://www.morningstar.com/news/dow-jones/202609185394/comex-gold-ends-the-week-045-higher-at-438590-data-talk — 2026-09-18 13:53 ET — gold **+$25.70 / +0.59%** to **$4,385.90**. Used to keep gold-smash **OFF** and gold≠XLB **ON**.
7. **Dow Jones / Morningstar — IP** — https://www.morningstar.com/news/dow-jones/202609183834/us-industrial-production-unchanged-in-august — 2026-09-18 09:50 ET — IP **0.0%** vs **+0.3%** expected; manufacturing **−0.3%**; capacity utilization **76.3%**. Used as same-morning demand shock.
8. **Seeking Alpha — IP timestamp** — https://seekingalpha.com/news/4644334-industrial-production-growth-stalls-in-august-falling-short-of-expectations — 2026-09-18 **9:17 a.m. ET** — same print. Used for knowable-at-open = partial (after typical pre-open lock).
9. **Nucor PR** — https://www.prnewswire.com/news-releases/nucor-announces-guidance-for-the-third-quarter-of-2026-earnings-302882443.html — 2026-09-17 after close — Q3 EPS **$5.55–$5.65**; mills/products up, raw materials down; Q2 refunds/Helion not repeating. Used as knowable steel-sleeve outlier.
10. **Morning card / pipeline JSON** — injected — predicted **flat/flat**, S0 **0.5**, S1–S4 **0**, `sector_rs_veto_applied: True`, `calendar_size_gate_applied: True`, PM:XLB **+0.51%**. Used for the audit, not rewritten after the close.

Channel 1 ETF/SPY percentages were **not** replaced by web estimates. Memory search was paused (index metadata missing).