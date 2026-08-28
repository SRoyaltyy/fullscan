# Sector Outcome — Industrials — 2026-08-28

Actuals: {'etf': 'XLI', 'pct': -0.9284136654230668, 'spy_pct': -0.2269485222937795, 'rel': -0.7014651431292873, 'open': 179.44000244140625, 'close': 177.13999938964844}

Memory search is paused this run (embedding-index mismatch). Review uses injected morning prediction + live sources only.

## 0. Facts

XLI sold from the open and lagged a mildly red tape.

- **XLI:** open **179.44** → close **177.14** → **−0.93%**
- **SPY:** **−0.23%**
- **Relative:** **−0.70%**
- **Path:** session high ~179.5 (near the open); close near the low. Not a gap-and-recover. Grind lower through 9:45 Chicago PMI and 10:00 ET Warsh.
- **Direction:** down. **Magnitude:** mild (sub-1% ETF, not a crash tape).

Morning call was **down**. Magnitude was internally split: narrative **down/mild**, pipeline JSON **down/flat** (total −2.25). Actual landed on **down/mild**.

---

## 1. What drove Industrials today

Taxonomy, in order:

1. **S0 rates / policy (Warsh JH)** — hawkish resolution of a two-sided 10:00 ET event. Inflation “still too high,” “otherwise we have work to do,” financial conditions “few signs of policy restraint.” 2Y yields jumped; September hike odds ~35–40% → ~55–60%. Cyclical duration + capex discounting, not a VIX panic.
2. **S1 activity (Chicago PMI)** — 47.1 vs consensus ~59 / prior 57.6. First sub-50 since April, weakest of the year. New orders −15.4 pts. Same-morning industrials print the morning correctly refused to score until it hit.
3. **S2/S4 continuation** — XLI was already a 1d/1w/1m laggard. Another −0.70% rel day is confirmation, not a new thesis.
4. **Not the driver:** oil squeeze (morning CL −0.8% was demand/risk), NVDA/XLK beta, F-15/SPEEA, durables/ISM (stale), UMich 51.7 (secondary vs Warsh/PMI).

Primary: **hawkish Warsh + Chicago contraction, on a pre-existing industrials lag.**

---

## 2. Morning S0–S4 vs reality (no post-close rewrite)

| Sleeve | Morning | After the close | Verdict |
|---|---|---|---|
| **S0 = 0** | ES 0.0% / NQ −0.19%, VIX calm, Warsh two-sided, do not one-way hawkish | Warsh resolved hawkish; SPY −0.23%; 2Y +~8–12 bp; hike odds up | **Fair at 09:30.** Overlay was mixed until 10:00. Scoring S0 −1/−2 *before* the speech would have been hindsight. After print, S0 was the live macro hit. |
| **S1 = 0** | No same-morning industrials print; Chicago PMI 9:45 *not scored until it prints*; consensus *above* prior | PMI 47.1 vs 59 — large miss | **Correct process, wrong (unknowable) outcome.** S1 was the post-open HIT. Cap at 0/+1 vs stale ISM/durables/grid still right. |
| **S2 = −1** | Laggard all horizons; 08-26 bounce fully reversed 08-27 | Rel −0.70% again; GEV/ETN led *down* | **HIT.** Mega-name AI-power did not save the ETF — it dragged it. |
| **S3 = 0** | Modest 5d outflows, not crowded | No evidence of a flow spike as the story | **Hold.** Do not triple-count the lag. |
| **S4 = −1** | 1d rel −1.51%, 1m −4.50% | Another red relative day | **HIT as confirmation only.** |

**Call audit:** direction **HIT**. Magnitude: narrative **mild = HIT**; pipeline **flat = slight miss** (actual −0.93% is mild, not flat). Confidence 0.50 was appropriate given 20% rolling dir accuracy. `divergence_flagged = False` stayed valid — leading sleeves and tape were aligned down.

08-27 governing rule (laggard after NVDA AHR → prefer flat or down:mild; don’t treat 08-21 green futures as an up license) **did its job**. 08-21 bounce gate stayed off. 08-25 “need a *fresh* single-name negative to force down:mild” was **overridden by a fresh *macro* negative (PMI + Warsh)**, which is the right exception — not a single-name.

---

## 3. Interactions / double-count / knowable-at-open

**Interaction (the day’s real structure):** Chicago PMI is a *growth* shock (normally dovish). Warsh was an *inflation/policy* shock (hawkish). They did **not** cancel. Cyclicals got the ugly mix: weaker regional activity **and** higher near-term rate odds. Soft PMI did not buy XLI a bid because the Fed chair refused the dovish read.

**Double-count test:**
- Warsh = **S0 only** (rates/policy). Do not re-score as S1 “industrials news.”
- Chicago PMI = **S1 only**. Do not also call it S0 risk-off (VIX was not a flight-to-safety tape).
- XLI lag = **S2 once, S4 confirmation**. Do not add S3.
- GEV/ETN dump is **skew/outlier**, not a second S1 spine and not a reason to have scored S1 −2 at the open.

**Knowable at open:** **partially.**
- Knowable: 1w/1m lag, 08-27 rotation into tech, mixed (not risk-on) futures, Warsh *could* hurt cyclicals, magnitude should stay mild/flat.
- Not knowable: PMI 47.1 vs 59; Warsh’s hawkish resolution vs two-sided prior; GEV −4%/ETN −3% as the sleeve that *pulled* XLI.

Morning was right to call **down** without needing those prints. It was also right **not** to call **notable**.

---

## 4. Outliers inside the sector

Approximate 08-28 closes (aggregator tape; treat as ± a few bp):

- **GEV ~−4.4%**, **ETN ~−3.1–3.2%** — AI-power/grid, the sleeve morning said must not be used as a *downside cushion*. Today it was a **downside amplifier** (valuation/profit-taking, not a new backlog miss).
- **CAT ~−2.1%** — machinery/cyclical beta, worse than the ETF.
- **HON ~−1.3–1.5%** — in line / slightly worse.
- **BA ~flat**, **RTX ~−0.2–0.4%** — aerospace/defense **held**. SPEEA/F-15 were correctly treated as stale.
- **XLI −0.93%** sits between defense (stable) and electrical/machinery (soft). Breadth inside the sector was **not** uniform; the ETF was a blend, not a GEV proxy.

That skew matters for the next morning: do not let GEV/ETN bounce rewrite an XLI call, and do not treat BA flat as industrials strength.

---

OUTCOME_BEGIN
SECTOR: Industrials
ETF: XLI
ETF_PCT: -0.9284
SPY_PCT: -0.2269
REL_PCT: -0.7015
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Hawkish Warsh Jackson Hole (hike odds/yields up) plus Chicago PMI 47.1 collapse, on an already-lagging XLI tape
KEY_INTERACTION: Growth miss (PMI) did not offset policy shock (Warsh); cyclicals ate both weaker activity and higher-for-longer rates
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Direction HIT (down); narrative magnitude HIT (mild) vs pipeline FLAT miss; S0/S1 correctly unscored until 9:45–10:00 prints, S2/S4 lag confirmed
OUTCOME_END

---

## RESEARCH APPENDIX

**Queries run**
- XLI industrials ETF August 28 2026 Warsh Jackson Hole Chicago PMI
- Chicago PMI August 2026 actual vs forecast August 28
- Kevin Warsh Jackson Hole speech August 28 2026 industrials stocks
- X search: XLI industrials August 28 2026 Warsh Chicago PMI GE Vernova Boeing CAT market close (2026-08-28 to 2026-08-29)
- XLI CAT GE Vernova Boeing RTX Eaton Honeywell August 28 2026 stock performance
- stock market today August 28 2026 industrials sector recap Warsh PMI
- site:wsj.com stock market today Jackson Hole 08-28-2026 industrials
- Chicago Business Barometer August 2026 47.1 new orders MNI
- GE Vernova Eaton decline August 28 2026 why
- University of Michigan consumer sentiment final August 2026 August 28
- XLI historical data August 28 2026 close 177.14
- Caterpillar stock August 28 2026 down industrials machinery
- Fetches: CNBC Warsh; Forbes Warsh; CNBC Chicago PMI video (title only); Motley Fool midday; RTTNews PMI; TradingKey ETN; Nikkei/Reuters Warsh. Failed/blocked: Reuters, WSJ, CME Econoday, MarketScreener, TradingEconomics, Benzinga, Detroit News.

**Key sources (title + URL + timestamp/facts used)**

CLAIM: XLI open 179.44 / close 177.14 / −0.928%; SPY −0.227%; rel −0.701%  
URL: injected Channel 1 actuals (deterministic)  
PUBLISHED: 2026-08-28 session  
QUOTE: OPEN: 179.44000244140625 CLOSE: 177.13999938964844  
SUMMARY: Official tape for this review; direction down, magnitude mild, lag vs SPY.

CLAIM: Chicago PMI/Business Barometer August 2026 = 47.1 vs prior 57.6, consensus ~59.0  
URL: https://www.rttnews.com/3686474/chicago-business-barometer-unexpectedly-plunges-to-eight-month-low-in-august.aspx  
PUBLISHED: 2026-08-28  
QUOTE: “the Chicago business barometer plunged to 47.1 in August from 57.6 in July… Economists had expected the index to rise to 59.0.”  
SUMMARY: Same-morning S1 miss; first contraction since April; not knowable at the open. CNBC video title (Fri Aug 28, 9:52 AM ET) confirms 47.1 as “weakest number for the year”: https://www.cnbc.com/video/2026/08/28/august-chicago-pmi-comes-in-at-47-point-1-the-weakest-number-for-the-year.html

CLAIM: New orders −15.4 pts; production/backlogs/inventories also down; employment up; prices paid up  
URL: web search synthesis citing MNI/TradingEconomics/MarketScreener (primary pages 403)  
PUBLISHED: 2026-08-28  
QUOTE: n/a (blocked fetches)  
SUMMARY: Used only as color on *why* 47.1 was an activity shock; headline 47.1 vs 59 is sourced to RTTNews.

CLAIM: Warsh: inflation still too high; summer readings “do not tell me that underlying trends have meaningfully improved”; “Otherwise, we have work to do.”  
URL: https://www.forbes.com/sites/tylerroush/2026/08/28/fed-chair-kevin-warsh-says-inflation-still-too-high-in-first-jackson-hole-speech/  
PUBLISHED: 2026-08-28  
QUOTE: “We must be confident that underlying inflation is moving to our objective, clearly and at sufficient speed… Otherwise, we have work to do.”  
SUMMARY: Hawkish resolution of the morning’s two-sided S0 event.

CLAIM: Speech at 10:00 ET; 2Y yields up; September hike odds to ~55.7%  
URL: https://www.cnbc.com/2026/08/28/kevin-warsh-jackson-hole-federal-reserve-inflation.html  
PUBLISHED: 2026-08-28  
QUOTE: “Traders also raised the probability for a rate hike at the September policy meeting to 55.7%, or about 20 percentage points higher than a day ago”  
SUMMARY: Policy-sensitive yields, not a crash VIX, were the S0 transmission into XLI.

CLAIM: Financial conditions not restrictive; prices should be the Fed’s predominant focus; hike odds ~60% after speech  
URL: https://asia.nikkei.com/economy/warsh-says-fed-has-work-to-do-if-above-target-inflation-persists  
PUBLISHED: 2026-08-28 23:47 JST / updated 2026-08-29 05:29 JST (Reuters)  
QUOTE: “credit and loan markets are showing few signs of policy restraint”  
SUMMARY: Why cyclicals did not get a dovish PMI bid.

CLAIM: Midday: industrials and healthcare lower; 10Y ~4.70%; S&P later −0.25%  
URL: https://www.fool.com/coverage/stock-market-today/2026/08/28/stock-market-midday-aug-28-stocks-edge-higher-on-fed-s-clear-inflation-message/  
PUBLISHED: 2026-08-28 (~11:48 AM ET snapshot; page also shows S&P −0.25% to 7,711.76)  
QUOTE: “Communication services stocks are the leading sector gains, while industrials and healthcare stocks are trading lower.”  
SUMMARY: Sector ranking matches XLI underperformance vs SPY.

CLAIM: ETN −3.06% on 2026-08-28; valuation/profit-taking in AI-power electricals, not a fundamental break  
URL: https://www.tradingkey.com/news/market-movers/262139301-market-movers-etn-20260828  
PUBLISHED: 2026-08-28  
QUOTE: “The pull-back reflects broader sector rebalancing across industrial and electrical equipment names, particularly those heavily tied to the artificial intelligence data center infrastructure trade.”  
SUMMARY: Outlier sleeve (with GEV) that dragged XLI; validates morning “do not use GEV/ETN as a downside cushion.”

CLAIM: Approximate single-name closes 08-28: GEV ~−4.4%, CAT ~−2.05% to $800.25, BA ~flat, RTX ~−0.2–0.4%, HON ~−1.3–1.5%  
URL: https://stockscan.io/stocks/CAT/price-history (and aggregator search citing MarketWatch/Stockmonitor)  
PUBLISHED: 2026-08-28  
QUOTE: CAT closed at $800.25, down 2.05%  
SUMMARY: Internal skew: machinery/electrical worse than defense; ETF is a blend.

CLAIM: UMich final August sentiment 51.7 (vs July 55.2); year-ahead inflation exp 4.0%  
URL: https://www.sca.isr.umich.edu/  
PUBLISHED: 2026-08-28  
QUOTE: Index of Consumer Sentiment 51.7  
SUMMARY: Calendar item printed; secondary to Warsh/PMI for XLI.

CLAIM: X posts: Chicago PMI 47.1; Warsh hawkish; industrials ~−0.97%; indexes mixed-to-lower  
URL: https://x.com/ThreeDrives/status/2093383421468750257 ; https://x.com/CHItrader/status/2093432601121808870 ; https://x.com/CTKCapitalIntel/status/2093432136766496989  
PUBLISHED: 2026-08-28  
QUOTE: n/a (tool summary)  
SUMMARY: Real-time confirmation of PMI print + sector underperformance; not used as price official.

**Facts taken / rejected**
- Took: PMI 47.1 vs 59; Warsh hawkish quotes and hike-odds move; industrials lag vs SPY; ETN/GEV/CAT/BA skew; XLI OHLC from injected actuals.
- Rejected as primary: search-blender XLI close “~177.10” (use injected 177.14); Fool midday “stocks edge higher” vs later S&P −0.25% (path, not close); any claim that PMI was *the* sole driver without Warsh.
- Memory index unavailable; no MEMORY.md citations.