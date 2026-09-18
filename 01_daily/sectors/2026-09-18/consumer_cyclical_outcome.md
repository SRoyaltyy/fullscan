# Sector Outcome — Consumer Cyclical — 2026-09-18

Actuals: {'etf': 'XLY', 'pct': -0.3231893458336965, 'spy_pct': -0.11932509489422927, 'rel': -0.20386425093946725, 'open': 111.45999908447266, 'close': 111.02999877929688, 'source': 'yf_download'}

Memory index is paused (embedding metadata missing), so this autopsy uses the injected 2026-09-18 morning card plus live sources only.

## 0. Facts

XLY **−0.32%** (open **111.46** → close **111.03**). SPY **−0.12%**. Relative **−0.20%**. Path: opened a few cents green vs ~111.39 prior close, then faded. That is a **mild down** day, not a shock — |XLY| just clears a ~30 bp mild gate; relative lag is sub-1d-gate but signed.

Morning call was **flat / flat**, all-zero S0–S4, mixed regime, divergence off. Engine still carried a stale tape_anchor (ES +1.14% / NQ +1.50% vs prior close) that the morning card correctly discarded in favor of live Finviz (ES +0.20% / NQ +0.41%, PM XLY +0.14%).

## 1. What drove XLY today

Taxonomy, not a two-name story.

**S0 shared macro (duration, not oil, not Bowman).** Midday 10Y was **+5 bp to 5.00%**; most sectors were flat-to-down with only utilities green. That is the live increment on the same real-yield grind the morning already had (DFII10 +6 bp 1d / +22 bp 1w). Oil stayed **offered** (WTI settlement ~**$99.82, −2.05%** vs 9/17) — 08-11 does **not** fire. Bowman was exactly the stress-testing speech the morning said it was; it is an XLF/supervision object, not an XLY path-binary. FOMC remains **paid**.

**S1 sector factors (calendar overlay on a still-split consumer book).** 9:15 ET G.17: IP **unchanged 0.0%** vs **+0.3%** expected; manufacturing **−0.3%** (first decline of 2026 after seven up months); **motor vehicles and parts −1.2%** (vehicles −1.8%); construction supplies **−0.7%**. 10:00 ET LEI **−0.1%** to 99.5, first monthly drop since March, with **consumer expectations** still a drag and building permits down. Those are same-session cyclical/auto/home-improvement prints. They do **not** rewrite the T-2 retail-sales beat or 9/17 claims. Pump *level* remains a tax; the *increment* was still oil-down.

**S2 breadth.** Split book, not expansion. AMZN (the ~23% sleeve) was reported **green**; NKE ~**−2.3%**, BKNG ~**−1.5%**; lodging nested (MAR) reportedly bid. Mega-cap internet retail did **not** lift the parent. That is the 08-27 / size_gate point in cash: do not map Nasdaq/AMZN into XLY.

**S3/S4.** Thursday’s **+$615M** XLY inflow already printed with the +1.10% beta catch-up. Today’s 1d rel **−0.20%** is leftover rotation-out, not a new flow shock.

**NFLX Wells Fargo cut (~−4.5–5%, UW / $57 PT)** is **Communication Services**, not an XLY holding. Do not import it.

## 2. Audit of morning S0–S4 (use morning numbers, not a rewrite)

| Sleeve | Morning | Cash | Verdict |
|---|---|---|---|
| **S0 = 0** | Paid FOMC, oil offered, Finviz futures inside ±0.5%, real-yield *grind* not kinetic; 08-27 bans mapping XLK/NQ into +1 | Yields +5 bp to 5%, oil still down, tape mixed/slightly red, Bowman not path | **Hold.** A −1 would re-vote 9/16 and fight a 32 bp tape. |
| **S1 = 0** | Retail beat / claims / SAAR / oil-relief vs carried confidence-credit and structural rotation-out; 09-11: stale cluster cannot flip live sign | IP auto/mfg miss and LEI consumer-expectations drag are *same-morning* but small; retail beat still T-2 | **Hold as a pre-open card.** IP was the one live S1 increment; it was on the calendar and explicitly **not pre-scored**. |
| **S2 = 0** | 08-28: no PM breakdown, no expansion; do not restack 3d/1w/1m lag | Split book in cash; AMZN up, apparel/travel down | **Hold.** |
| **S3 = 0** | Thursday inflow is leftover | No evidence of a fresh 1-day bid | **Hold.** |
| **S4 = 0** | 1d rel −0.04% = flat confirmation | 1d rel −0.20% | **Hold** (still sub-notable). |

Engine vs LLM: pipeline `total_score` 4.013 was still inflated by the discarded ES/NQ prior-close anchor + index_carry. LLM overlay 0.0 + `sector_rs_veto` kept the **call** at flat. That split is the same 09-16/09-17 lesson, and today the *live* tape really was the mixed case those lessons reserved.

Horizon: morning **1W −1 / 1M −1** already said relative lag while DFII10 stays bid. Today’s −20 bp rel is that leak, not a regime break.

## 3. Interactions / double-count / knowable-at-open

- **Do not double-count** paid FOMC + Bowman + 10Y 5% as three hawkish hits. One duration object.
- **Do not double-count** oil: not in S0, not a gasoline spike in S1; live sign still down.
- **Do not double-count** UMich 47.8 / LEI consumer-expectations. LEI is a 10:00 print that *echoes* the stale confidence cluster; 09-11 still forbids letting that cluster own the sign.
- **Do not map** XLK/NQ/NFLX/ASML into XLY (08-27).
- **Knowable at open: partially.** Mixed tape, real-yield grind, structural 1w/1m lag, “don’t convert all-zero into down,” and Bowman-is-not-path were knowable. IP/LEI *existence* was knowable; the **miss** was not. +5 bp back to 5.00% was a same-session increment.

## 4. Outliers inside the sector

- **AMZN green** vs parent red — concentration did not save XLY (size_gate earned).
- **NKE ~−2.3%, BKNG ~−1.5%** — apparel/travel sleeve, not the mega-cap.
- **Auto IP −1.2% / vehicles −1.8%** — TSLA/auto color, not a confirmed TSLA breakdown by itself (prints on TSLA were mixed across vendors).
- **Construction supplies −0.7%** — HD/LOW nested, consistent with MAP HEAT home-improvement down.
- **NFLX** — outlier for *entertainment*, not this ETF.

---

**CLAIM:** XLY closed −0.32% vs SPY −0.12% (rel −0.20%), open 111.46 / close 111.03.  
**URL:** deterministic Channel 1 actuals (injected)  
**PUBLISHED:** 2026-09-18 session  
**QUOTE:** ETF_PCT −0.3231893458336965; SPY_PCT −0.11932509489422927; REL_PCT −0.20386425093946725  
**SUMMARY:** Mild down, slight fade from a green-ish open; relative lag continues but is not a shock.

**CLAIM:** August industrial production unchanged; manufacturing −0.3%; motor vehicles and parts −1.2%.  
**URL:** https://www.federalreserve.gov/releases/g17/current/  
**PUBLISHED:** 2026-09-18  
**QUOTE:** “Industrial production (IP) was unchanged in August after increasing 0.2 percent in July. Manufacturing output decreased 0.3 percent in August.”  
**SUMMARY:** Miss vs +0.3% consensus; auto and construction supplies are the XLY-relevant slices.

**CLAIM:** LEI −0.1% in August; consumer expectations a drag.  
**URL:** https://www.prnewswire.com/news-releases/the-conference-board-leading-economic-index-lei-for-the-us-edged-down-in-august-302883352.html  
**PUBLISHED:** 2026-09-18  
**QUOTE:** “The US LEI receded slightly in August, the first monthly decline since March of this year… consumer expectations remaining a significant strain on the Index.”  
**SUMMARY:** Soft leading index, not a recession signal; echoes carried confidence weakness.

**CLAIM:** 10Y +5 bp to 5.00% midday; most sectors flat/down.  
**URL:** https://www.fool.com/coverage/stock-market-today/2026/09/18/stock-market-midday-sept-18-stocks-slip-crypto-gains/  
**PUBLISHED:** 2026-09-18 (~11:44 ET)  
**QUOTE:** “the 10-Year Treasury yield is up 5 basis points to 5.00%. Most sectors were trading flat or falling, with only utilities showing growth.”  
**SUMMARY:** Duration re-tightening is the honest S0 increment.

**CLAIM:** Bowman speech was stress-testing / SCB transparency, not a funds-path lean.  
**URL:** https://www.federalreserve.gov/newsevents/speech/bowman20260918a.htm  
**PUBLISHED:** 2026-09-18  
**QUOTE:** “Today, my remarks will highlight our work to enhance and improve the bank regulatory stress test framework.”  
**SUMMARY:** Morning “do not convert S0=0 into a two-sided Fed −1/+1” was correct.

**CLAIM:** WTI still down on the cash session.  
**URL:** https://hk.investing.com/commodities/crude-oil-historical-data  
**PUBLISHED:** 2026-09-18  
**QUOTE:** Close 99.82 vs prior 101.91 (−2.05%).  
**SUMMARY:** Oil increment remains relief; 08-11 stays off.

**CLAIM:** NFLX Wells Fargo downgrade is not an XLY factor.  
**URL:** https://www.marketscreener.com/news/wells-fargo-downgrades-netflix-to-underweight-from-equalweight-adjusts-price-target-to-57-from-80-ce785adadb88f12d  
**PUBLISHED:** 2026-09-18  
**QUOTE:** Underweight from Equal Weight; PT $57 from $80.  
**SUMMARY:** Communication Services single-name; do not map into consumer cyclical.

OUTCOME_BEGIN
SECTOR: Consumer Cyclical
ETF: XLY
ETF_PCT: -0.3231893458336965
SPY_PCT: -0.11932509489422927
REL_PCT: -0.20386425093946725
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: 10Y back to 5.00% (+5 bp) duration grind plus a still-split discretionary book; AMZN bid did not lift XLY while IP auto/mfg missed.
KEY_INTERACTION: Knowable real-yield hangover stacked with structural rotation-out; oil-down relief did not net against duration; IP/LEI were calendar overlays, not a rewrite of the all-zero pre-open card.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: All-zero/flat was the right pre-open stance vs the discarded ES/NQ +1% anchor; cash leaked a mild down/lag that the 1W horizon already implied, not a regime miss.
OUTCOME_END

## RESEARCH APPENDIX

**Queries run**
- memory_search: Consumer Cyclical XLY 2026-09-18 outcome lessons S0 S1 oil FOMC *(index paused)*
- web_search: XLY consumer discretionary September 18 2026 stock market industrial production LEI
- web_search: US industrial production capacity utilization LEI September 18 2026 consumer spending
- web_search: Amazon Tesla Home Depot stock September 18 2026 XLY decline
- web_search: Fed Bowman stress testing speech September 18 2026 market reaction
- web_search: stock market news September 18 2026 S&P 500 oil yields consumer discretionary
- web_search: AMZN TSLA HD NKE MCD LOW September 18 2026 closing percent change
- web_search: Federal Reserve industrial production August 2026 G.17 manufacturing output vehicles
- web_search: XLY holdings performance September 18 2026 Amazon Tesla Nike Booking Marriott
- web_search: Netflix Wells Fargo downgrade September 18 2026 consumer discretionary NFLX
- web_search: 10-year Treasury yield September 18 2026 5.00 consumer stocks
- web_search: XLY vs SPY September 18 2026 sector performance consumer discretionary lag
- web_search: WTI crude close September 18 2026 oil price
- web_search: Nike Booking Holdings stock September 18 2026 down consumer discretionary
- x_search: XLY consumer discretionary AMZN TSLA HD September 18 2026 market why down oil Fed (2026-09-18 to 2026-09-19)
- web_fetch: Fed G.17 current + Table 2; Conference Board / PR Newswire LEI; Fed Bowman speech; Motley Fool midday wrap; Morningstar DJ IP item

**Key sources (title + URL + timestamp) and facts taken**
- Injected actuals (pipeline) — XLY −0.32%, SPY −0.12%, rel −0.20%, O/C 111.46/111.03. Session 2026-09-18.
- Federal Reserve G.17 (https://www.federalreserve.gov/releases/g17/current/, 2026-09-18) — IP 0.0%; manufacturing −0.3%; cap-u 76.3%; construction supplies −0.7%.
- Fed G.17 Table 2 (https://www.federalreserve.gov/releases/g17/current/table2.htm, 2026-09-18) — motor vehicles and parts −1.2%; motor vehicles −1.8%.
- Morningstar/DJ (https://www.morningstar.com/news/dow-jones/202609183834/us-industrial-production-unchanged-in-august, 09:50 ET 2026-09-18) — IP miss vs WSJ +0.3% consensus.
- Conference Board via PR Newswire (https://www.prnewswire.com/news-releases/the-conference-board-leading-economic-index-lei-for-the-us-edged-down-in-august-302883352.html, 2026-09-18) — LEI −0.1% to 99.5; consumer expectations + permits as drags; GDP 2026 1.9% / 2027 cut to 1.8%.
- Motley Fool midday (https://www.fool.com/coverage/stock-market-today/2026/09/18/stock-market-midday-sept-18-stocks-slip-crypto-gains/, ~11:44 ET) — 10Y +5 bp to 5.00%; most sectors flat/down; NFLX ~−5% on Wells Fargo (not XLY).
- Bowman speech (https://www.federalreserve.gov/newsevents/speech/bowman20260918a.htm, 2026-09-18) — stress-test transparency / SCB averaging; not SEP/path.
- Investing.com WTI hist (https://hk.investing.com/commodities/crude-oil-historical-data) — WTI close 99.82 vs 101.91 (−2.05%).
- Holdings color (stockanalysis / financecharts search digest, 2026-09-18) — NKE ~−2.3%, BKNG ~−1.5%; AMZN reported green. Vendor prints on TSLA/HD conflicted; not used as a parent driver.
- X search — oil-spike / hawkish-Fed chatter; **discarded** where it contradicts cash oil-down and paid FOMC. Social color only.

**Not used / conflicts**
- Yahoo multi-ticker digest that had AMZN +2.16% / HD +0.48% on a down XLY day — inconsistent with other vendors; treated as unreliable for single-name attribution.
- X narrative that oil *spiked* on 9/18 — false vs WTI −2%.
- Reuters wraps (fetch 401). Memory index paused; no MEMORY.md recall this turn.