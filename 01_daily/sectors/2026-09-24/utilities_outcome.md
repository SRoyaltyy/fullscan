# Sector Outcome — Utilities — 2026-09-24

Actuals: {'etf': 'XLU', 'pct': -0.9811305399960668, 'spy_pct': -0.0820521854125067, 'rel': -0.8990783545835601, 'open': 39.880001068115234, 'close': 39.36000061035156, 'source': 'yf_download'}

Memory index is paused (`openclaw memory status --index` / `openclaw memory index --force`); this review uses the injected morning card, Channel 1 actuals, and live sources only.

## 0. Facts

XLU **−0.981%** (open **39.88** → close **39.36**). SPY **−0.082%**. Relative **−0.899%**. Path: opened near the session high (**39.88–39.89**), sold through the cash session, and closed on the **day/week/month low**. Direction **down**, magnitude **mild** (sub-1% absolute; not a second notable after 09-23’s −2.24%).

CLAIM: XLU closed 39.36, −0.98% vs SPY −0.08%, rel −0.90%; open 39.88, range down to 39.36.  
URL: injected Channel 1 actuals (OPEN 39.880 / CLOSE 39.360)  
PUBLISHED: 2026-09-24 session  
QUOTE: `ETF_PCT: -0.9811305399960668 | SPY_PCT: -0.0820521854125067 | REL_PCT: -0.8990783545835601`  
SUMMARY: Bond-proxy sold from a slightly green open to the low while the index was essentially unchanged.

CLAIM: S&P 500 −1.90 pts (<0.1%) to 7,704.13; Dow −0.3%; Nasdaq essentially flat.  
URL: https://apnews.com/article/wall-street-stocks-dow-nasdaq-2487345eeaa8145f051da6b1d7e9f37d  
PUBLISHED: 2026-09-24  
QUOTE: “S&P 500: Down 1.90 points (less than 0.1%) to 7,704.13 … Nasdaq Composite: Up 3.34 points (less than 0.1%)”  
SUMMARY: Cash equity tape was mixed/flat, not the ES −0.64% / NQ −1.09% risk-off the morning premarket implied.

## 1. What drove the sector

Primary map = **rates / duration** (taxonomy: rates rising → bond-proxy selloff). The long end did not stay at the AM “tiny backup.” In US hours the 10Y pushed **>+10 bp toward ~5.22%** and the 30Y printed a **2004-era high**. That is the classical XLU channel, and it printed as **absolute down + relative lag** with SPY flat.

CLAIM: 10Y +>10 bp to 5.223%; 30Y +~10 bp, high 5.501% (since Jun 2004); 2Y +>4 bp to 4.941%.  
URL: https://www.cnbc.com/2026/09-24/us-treasury-yields-bonds-fed-inflation.html  
PUBLISHED: 2026-09-24  
QUOTE: “The 30-year Treasury bond yield was last about 10 basis points higher, hitting a high of 5.501% … The benchmark 10-year … surged more than 10 basis points to 5.223%.”  
SUMMARY: US-hours bond rout was the live macro impulse. Morning FRED 09-22 4.96% / ZN −0.03% was the *pre-cash* state, not the close.

CLAIM: NY Fed Williams: another hike by year-end is “reasonable”; Barr (Wed) said further hikes likely needed; FedWatch October hike odds >75% vs ~49% a week earlier.  
URL: https://www.cnbc.com/2026/09/24/us-treasury-yields-bonds-fed-inflation.html  
PUBLISHED: 2026-09-24  
QUOTE: “Speaking in London on Thursday, New York Federal Reserve President John Williams said it would be ‘reasonable’ to expect another Fed interest rate hike by the end of the year.”  
SUMMARY: Hawkish follow-through after Warsh/09-16 hike, not a new FOMC. Extra-confirm of the rates channel, not a separate sector catalyst.

Secondary, same-sign: **oil still elevated** (inflation/duration feed, 09-08 map). Energy led sector tape; utilities did **not** get an FTS bid (VIX still sub-20 contango in the morning frame; cash SPY/Nasdaq not in crash mode).

CLAIM: Energy among leaders (~+1%); 8 of 11 sectors lower; XLU among laggards (~−0.98%).  
URL: https://www.benzinga.com/etfs/sector-etfs/26/09/61981197/8-of-11-sectors-fall-in-thursday-trading-as-leaders-split  
PUBLISHED: 2026-09-24  
QUOTE: search recap: “8 of 11 sectors were lower … Energy +0.97% to +1.01% … XLU … −0.98%”  
SUMMARY: Rotation was rates/commodity, not classic FTS into defensives.

AI-power / IPP bid was a **dampener inside the sector**, not a band engine (08-12 / 08-28). No same-day XLU-wide rate-case or earnings print.

## 2. Audit of morning S0–S4 (use morning numbers, do not rewrite them)

| Sleeve | Morning | Reality | Verdict |
|---|---|---|---|
| **S0 −1.0** rates live, named, corr −0.826, oil feeding; PM +0.10% is relative not absolute | 10Y/30Y ripped in cash hours; Williams/Barr hawkish; XLU still sold | **HIT on sign.** AM ZN −0.03% understated the *US-hours* impulse; the *channel* was correctly live. |
| **S1 −1.0** rotation-away PARTIAL; carried rotation HIT; rates **not** re-HIT here | SPY flat / Nasdaq green, energy up, XLU lag — rates de-rating, not a risk-on rip | **Sign OK, weight too heavy.** This is mostly the S0 shock showing up as relative lag. |
| **S2 −0.5** breadth collapsed | Close at week/month/year low; RSI14 ~15, WPR14 −100 | **HIT.** Sector-wide, not ETF-only. |
| **S3 −0.5** carried outflow | Volume ~17–25M, not a dry-up; sold to the low | **Directionally HIT**, not a fresh flow event. |
| **S4 −1.0** 1d/3d/1w/1m all lag; \|1d rel\| 1.50% | Another lag: −0.98% vs SPY −0.08% (rel −0.90%) | **HIT.** Path = fade from green open, not a gap-down smash. |

**Band audit:** Morning capped **mild** because (a) AM long end was only a tiny backup, (b) XLU PM **+0.10%**, (c) 09-16 forbids letting trailing lag pay a *second* notable. That cap was **correct**. Yields later smashed more than ZN showed, but SPY went **flat** and 09-23 had already paid −2.24%, so absolute XLU stayed **mild** (~1%), not notable.

**PM green:** It was real at the open (39.88 vs prior ~39.75) and **fully faded**. It was a magnitude/path tell, not a sign flip — exactly as written.

**09-23 lesson application:** Not zeroing aligned negatives kept the **sign**. It did **not** force a notable band. That split was the right one.

## 3. Interactions / double-count / knowable-at-open

**Same shock:** Warsh/hike-odds + long-end backup = S0. The same impulse is the relative lag (S1 rotation), the collapsed tape (S2), and S4 confirmation. Counting S0 at −1.0 **and** S1 at −1.0 **and** S4 at −1.0 restacks one rates move. Breadth (S2 −0.5) is the cleanest non-tape expression of that de-rating.

**Not double-counted well:** Oil→yields belongs in S0 only (morning did that). BX/PNM-TXNM stayed out of S1 (08-28). Trump–Xi stayed two-sided. No CPI/NFP/FOMC template (09-11). Good.

**Knowable at open:**
- **Yes:** rates channel live and named (global bond-rout headlines, Warsh regime, 5d corr −0.826, 1m DGS10/DFII10 backup, every-horizon lag). Sign should be down, relative lag.
- **No:** the cash-session **+10 bp 10Y / 30Y 5.50%** print. 08:04 ET DJ copy still had 10Y **+0.6 bp at 5.119%** — that *is* the morning “tiny backup.”
- **No:** ES/NQ red PM resolving to **SPY −0.08% / Nasdaq flat**. That fade is why absolute XLU did not go notable even after the yield smash.

Net: **partially**.

CLAIM: Pre-US-open Treasuries were only marginally higher (10Y +0.6 bp at 5.119%).  
URL: https://www.morningstar.com/news/dow-jones/202609241547/us-treasury-yields-hover-close-to-multiyear-highs-update  
PUBLISHED: 2026-09-24 04:04 ET (08:04 GMT)  
QUOTE: “The 10-year Treasury yield … last traded up 0.6 basis points at 5.119% … The 30-year yield … last traded 1.3 basis points higher at 5.414%.”  
SUMMARY: Morning ZN −0.03% matched the *open*. The smash was an in-session event.

## 4. Outliers inside the sector

ETF close is **not** a mega-cap illusion: XLU at the range low with RSI 15.

CLAIM: XLU technicals at the low (MA20 6.5% above, RSI14 15.08, WPR14 −100, week/month/year low 39.36).  
URL: https://eoddata.com/stockquote/AMEX/XLU.htm  
PUBLISHED: 2026-09-24  
QUOTE: “RSI14: 15.08 … WPR14: −100.00 … Week Low: 39.36 … Year Low: 39.36”  
SUMMARY: Breadth/tape failure is sector-wide oversold, consistent with S2.

Holdings (treat as approximate; feeds disagreed earlier in the day):

CLAIM: NEE closed ~$75.62, about −1.8% vs prior $77.02 — worse than XLU.  
URL: https://beta.finance.yahoo.com/quote/NEE/  
PUBLISHED: 2026-09-24  
QUOTE: search recap: “Closed at approximately $75.61–$75.62 … down about 1.8% from the prior close of $77.02.”  
SUMMARY: Largest weight (duration/growth utility) led the ETF lower — rate-proxy, not an IPP rescue.

Regulated names (DUK/SO) clustered nearer to or slightly better than XLU; **VST ~flat** is the internal outlier (IPP/AI-power bid), **not** enough to lift the ETF. That is the 08-28/08-12 rule working: do not promote CEG/VST into S1 as a 1d override.

---

OUTCOME_BEGIN
SECTOR: Utilities
ETF: XLU
ETF_PCT: -0.981
SPY_PCT: -0.082
REL_PCT: -0.899
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: US-hours long-end backup (10Y >+10 bp toward ~5.22%, 30Y ~5.50%) extended the bond-proxy de-rating.
KEY_INTERACTION: One rates shock restacked across S0/S1/S2/S4; SPY-flat tape plus 09-23’s already-paid −2.24% capped the print at mild.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Down/mild HIT on sign and band; S0 rates call confirmed, AM ZN understated the cash yield smash, S1 −1.0 restacked the same shock.
OUTCOME_END

---

## RESEARCH APPENDIX

**Queries run**
- web_search: `XLU utilities ETF September 24 2026`
- web_search: `stock market September 24 2026 Treasury yields Fed Warsh utilities`
- web_search: `US Treasury yields September 24 2026 10-year close`
- web_search: `XLU NEE DUK SO CEG VST September 24 2026`
- web_search: `"utilities" sector stocks September 24 2026 yields`
- web_search: `how major US stock indexes fared Thursday September 24 2026`
- web_search: `site:finance.yahoo.com XLU September 24 2026`
- web_search: `S&P 500 utilities sector performance September 24 2026`
- web_search: `Kevin Warsh Fed rate hike September 24 2026`
- web_search: `oil prices WTI Brent September 24 2026`
- web_search: `NextEra Duke Southern Constellation Vistra stock September 24 2026`
- web_search: `"Treasury yields" "September 24" 2026 10-year 5.1`
- web_search: `Michael Barr further rate increases September 24 2026`
- web_search: `XLU volume September 24 2026 close 39.36`
- web_search: `sector performance utilities lagging September 24 2026 XLE XLK`
- web_search: `John Williams Fed another rate hike September 24 2026`
- web_search: `NEE stock close September 24 2026 -1.8`
- x_search: Warsh/XLU/yields 2026-09-24; XLU lag vs SPX 2026-09-24
- web_fetch: Reuters 09-24 futures (JS wall); Seattle Times (empty); Morningstar DJ yields 08:04 GMT; CNBC 10Y/30Y; Barr DJ 09-23; AP (403); Yahoo XLU/NEE (fail/404); EODData XLU; FRED DGS10 (timeout); Benzinga/Gate (403)

**Key sources (title + URL + timestamp)**
1. Injected Channel 1 actuals — XLU −0.981% / SPY −0.082% / rel −0.899%, open 39.88 close 39.36 — 2026-09-24 session.
2. CNBC — “30-year Treasury yield hits highest level since 2004 as bond market rout continues” — https://www.cnbc.com/2026/09/24/us-treasury-yields-bonds-fed-inflation.html — fetched 2026-09-24T21:30Z. Facts: 10Y +>10 bp to 5.223%; 30Y high 5.501%; 2Y 4.941%; Williams “reasonable” another hike; October hike odds >75%.
3. Morningstar / Dow Jones — “U.S. Treasury Yields Hover Close to Multiyear Highs — Update” — https://www.morningstar.com/news/dow-jones/202609241547/us-treasury-yields-hover-close-to-multiyear-highs-update — 2026-09-24 04:04 ET. Facts: pre-cash 10Y +0.6 bp at 5.119%, 30Y +1.3 bp at 5.414%.
4. Morningstar / Dow Jones — “Fed's Barr Says More Rate Hikes Likely Needed…” — https://www.morningstar.com/news/dow-jones/202609234882/feds-barr-says-more-rate-hikes-likely-needed-to-return-inflation-to-target — 2026-09-23 10:19 ET. Facts: Barr “further policy adjustments are likely to be needed.”
5. AP via search — “How major US stock indexes fared Thursday 9-24-2026” — https://apnews.com/article/wall-street-stocks-dow-nasdaq-2487345eeaa8145f051da6b1d7e9f37d — 2026-09-24. Facts: SPX −1.90 to 7,704.13; Dow −0.3%; Nasdaq +3.34.
6. Yahoo Finance (search recap) — https://beta.finance.yahoo.com/quote/XLU/ — 2026-09-24. Facts: prev 39.75, open 39.88, range 39.36–39.89, volume elevated.
7. EODData — https://eoddata.com/stockquote/AMEX/XLU.htm — fetched 2026-09-24T21:29Z. Facts: RSI14 15.08, WPR −100, week/month/year low 39.36.
8. Yahoo NEE (search recap) — https://beta.finance.yahoo.com/quote/NEE/ — 2026-09-24. Facts: NEE ~$75.62, ~−1.8%.
9. Benzinga sector recap (search; page 403 on fetch) — https://www.benzinga.com/etfs/sector-etfs/26/09/61981197/8-of-11-sectors-fall-in-thursday-trading-as-leaders-split — 2026-09-24. Facts used cautiously: energy lead, 8/11 down, XLU lag.
10. Reuters Williams (search; fetch 401) — https://www.reuters.com/business/feds-williams-says-it-is-reasonable-see-another-us-rate-hike-this-year-2026-09-24/ — 2026-09-24. Corroborates CNBC Williams quote.

**Discarded / conflicting**
- First x_search XLU close $79.03 / 10Y 4.19% — inconsistent with injected 39.36 tape and CNBC 5.22% 10Y; ignored.
- Early Yahoo basket showing NEE/DUK/CEG green while XLU −1% — mechanically implausible vs later NEE −1.8% / XLU −0.98%; later quotes used.
- Oil $94 WTI vs morning $104 — feeds disagree on the *level*; direction (still elevated/bid) is what the utilities map needs.