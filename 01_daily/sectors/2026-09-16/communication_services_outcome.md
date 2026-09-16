# Sector Outcome — Communication Services — 2026-09-16

Actuals: {'etf': 'XLC', 'pct': -0.903270008176027, 'spy_pct': -0.4409916675517711, 'rel': -0.4622783406242559, 'open': 114.22000122070312, 'close': 113.0, 'source': 'yf_download'}

Memory index is unavailable this run (embedding metadata missing); used the injected morning packet and live sources only.

## 0. Facts

XLC **−0.90%** (open **114.22** → close **113.00**; prior close **114.03**). Path: slight gap-up (~+0.17% vs prior close), then a post-14:00 ET fade to a lower close. SPY **−0.44%**. Relative **−0.46%**. Direction **down**, magnitude **mild**. Nasdaq Composite ~flat (−0.01%); S&P 500 −0.45% — XLC lagged both the index and NDX.

That is the same print shape as 09-15 (XLC −0.90% / SPY −0.46% / rel −0.45%): another mild underperformance day, not a crash.

---

## 1. What drove the sector

**Primary: hawkish FOMC increment after a priced hike.** The 25 bp move to 3.75–4.00% was the object morning treated as ~85–94% priced. The *surprise* was the package around it: unanimous vote, SEP median **4.1%** (one more 2026 hike), **16/18** dots wanting at least one more, inflation forecasts nudged up, and a short hawkish Warsh presser (“too high for too long”). Equities were green into 14:00 ET and sold after the statement/presser (Dow −631 / ~−1.2%; 2Y +7 bp in the CNBC takeaway). That is **rates / duration / policy-path**, not a same-day ad or AI print.

**Secondary, taxonomy-aligned:**
- **S0 shared macro:** risk-on overnight (oil offered, ES/NQ green) reversed into risk-off after the dots/presser. Hot August retail sales (**+1.2%** vs ~0.7–0.8% expected) at 08:30 ET was the same “resilient demand → Fed can hike” shock, not a separate XLC factor.
- **S1 spine:** no fresh Meta/Google ad or AI monetization proof. Carried Q2 ad/AI thesis did not pay.
- **S2 breadth:** the two-name book **did not move as a unit**. META **+0.46%**; GOOGL **−0.73%**; GOOG **+0.03%**. Drag sat in **telecom/media**: VZ ~**−3.3%**, T ~**−3.2% to −3.6%**, CMCSA ~**−2.8% to −3.0%**, NFLX ~**−1.9%** (continuation after 09-15’s ~−3%). DIS ~**+0.5%**. Rough weight math: T+VZ+CMCSA (~15%) × ~−3% ≈ **−0.45 ppt** of XLC — about half the ETF’s day — while META roughly offset GOOGL.
- **S3 flows:** no evidence a same-day inflow/outflow spike was the driver; morning’s ~$184.5M weekly outflow was leftover.
- **S4 tape:** leftover 3d/1w/1m RS **+2.3% to +3.4%** did not show up as a bid. Live session confirmed the 09-15 de-risk, not the prior-window winner.

Oil-down / Asia-Europe green was the *open* overlay. It was not the *close* driver.

---

## 2. Audit of morning S0–S4 (morning numbers, not rewritten)

Morning **factor card was all zeros**: S0=0, S1=0, S2=0, S3=0, S4=0. LLM call: **flat / flat**, FOMC suppressor forbids up, priced hike + oil-offered + mixed anchors forbids down. Engine then emitted **up / mild** (total **9.244**) off **tape_anchor 7.92** (NQ +1.50%, ES +1.14%) + index_carry **1.324**.

| Sleeve | Morning | Reality | Verdict |
|---|---|---|---|
| **S0** | 0. Priced hike is stale; 08-21 bans leftover-hawkish minus; 08-13 FOMC is a high-impact suppressor, not an up license; do not map NQ/ES onto XLC. | Priced 25 bp delivered; **unpriced hawkish increment** (dots + Warsh) sold duration/growth and the tape. Retail sales hot, dominated by 14:00. | **S0=0 as a pre-binary score was right.** Treating FOMC as two-sided was right. Emitting **up** from green ES/NQ was the error. A post-close S0 would be **−** on the hawkish increment, which was **not** knowable in sign at 07:20. |
| **S1** | 0. Carried ad+AI, no fresh proof, legal stale, FCF overhang carried. | No Meta/Google revenue or regulatory HIT. META’s +0.46% was tape, not a spine print. | **Hold.** |
| **S2** | 0. META slightly red PM, GOOGL flat; do not average nested HEAT; do not reuse 1d rel −0.45% as leadership. | META flipped green; GOOGL red; telecom/media failed. Breadth was **worse than the two anchors**. | **0 at open was fair** (no same-morning sweep). The miss was not “META red ⇒ sector down”; it was **non-anchor lag** plus GOOGL. |
| **S3** | 0. Prior-window outflow; 09-15 already de-risked crowded 1w/1m RS. | No same-day flow story required to explain −0.90%. | **Hold.** |
| **S4** | 0. Live PM flat / XLC absent from PM board; leftover 3d/1w/1m RS banned (08-28 / 09-15). | Open only +0.17% vs prior close, then sold to −0.90%. Overnight NQ/ES **did not** certify XLC. | **LLM S4=0 was right. Engine tape_anchor was a 08-27 / 09-10 violation.** |

**Engine vs LLM split is the autopsy.** Factors agreed (leading sum 0, no divergence flag). The **up/mild** print was almost entirely **NQ/ES mapped onto a two-name book that was not participating together**. That is the same class of miss as 08-27 (green NQ/XLK ≠ XLC) and 09-10 (never map NQ/ES onto XLC). Calendar size gate (no notable) was the one engine piece that matched reality.

Direction: engine **up** vs actual **down** = **MISS**. Magnitude band **mild** = **HIT**. LLM **flat** was the better pre-binary call (wrong side vs down, but it refused the engine’s up).

---

## 3. Interactions / double-count / knowable-at-open

- **One rates object.** Hike odds, Warsh JH leftover, and 14:00 FOMC are the same policy path. Morning correctly refused to stack them as three S0 hits. The *increment* that moved the tape was **SEP/dots + presser hawkishness**, which is still that one object, resolved after 14:00.
- **Do not double-count** hot retail sales and hawkish Fed as two XLC theses. Both are “activity resilient → more restriction.” Retail sales was knowable as a binary at open; morning correctly did not pre-score it; it did not independently drive XLC vs the 14:00 package.
- **Oil-falling + green futures** were the same pre-FOMC easing overlay. Morning refused to pay S0+ for both. Correct. They also did **not** protect XLC after 14:00.
- **NQ/ES tape_anchor + index_carry** is the double-count that *did* fire: LLM already set S0=0 / S4=0 with an explicit ban on mapping NQ onto XLC, then the engine paid ~8 points of overnight futures into an **up** call. That was knowable-at-open as a **process error**, not new information.
- **Leftover RS + crowded-long:** 1w/1m +3.4% was history. 09-15 already printed the de-risk. Using it to lift (engine) or to force down (veto leftover) would both have been wrong at 07:20. Live PM was flat; close was another mild down day. Leftover RS still does not get a vote.
- **Knowable at open: partially.** Knowable: FOMC is the day’s high-impact suppressor; do not emit up; do not map NQ/ES; two anchors mixed; leftover RS banned. Not knowable: sign of dots/presser, META flipping green, telecoms −3%.

---

## 4. Outliers inside the sector

- **META +0.46%** vs XLC **−0.90%**: largest weight was the **positive** outlier. A “mega-cap growth duration crush” story does not fit the two-name book as a unit.
- **GOOGL −0.73% / GOOG +0.03%**: share-class split; the heavier XLC line (GOOGL) lagged.
- **Telecom/cable (VZ, T, CMCSA) ~−3%**: rate-sensitive lag that **did** move the ETF despite “telecom weight is limited.” On a hike-day with a hawkish presser, mid-single-digit names can still account for ~half of a −0.90% print.
- **NFLX ~−1.9%**: residual from 09-15’s ~−3%, not a new catalyst.
- **DIS ~+0.5%**: did not save the book.
- **XLC vs NDX:** Nasdaq ~flat, XLC −0.90%. Another 08-27 reminder: **NQ strength (overnight or on the close) is not XLC participation.**

---

## Evidence

CLAIM: FOMC raised the funds rate 25 bp to 3.75–4.00%, 12–0, 2:00 p.m. EDT.  
URL: https://www.federalreserve.gov/newsevents/pressreleases/monetary20260916a.htm  
PUBLISHED: 2026-09-16  
QUOTE: “The Committee decided to raise the target range for the federal funds rate by 1/4 percentage point to 3-3/4 to 4 percent… Inflation remains elevated.”  
SUMMARY: Official statement; the priced hike leg.

CLAIM: Dots and Warsh presser were the hawkish increment; stocks sold after a green open into the decision.  
URL: https://www.cnbc.com/2026/09/16/here-are-five-key-takeaways-from-wednesdays-fed-rate-hike.html  
PUBLISHED: 2026-09-16  
QUOTE: “Stocks were in the green heading into the rate decision… sold off sharply after the decision. The Dow… tumbled 631 points and the 2-year Treasury yield… rocketed more than 7 basis points higher.” / “Sixteen of the 18 participants expected at least one more rate hike this year.”  
SUMMARY: Path (green → post-14:00 selloff) and SEP median path to another 2026 hike.

CLAIM: Warsh: inflation “too high for too long”; 16/18 see another hike; PCE/core nudged to 3.7%/3.4%.  
URL: https://www.cnbc.com/2026/09/16/fed-rate-decision-september-2026.html  
PUBLISHED: 2026-09-16  
QUOTE: “We must be confident that underlying inflation is moving to our objective clearly and at sufficient speed… this standard has not been satisfied.”  
SUMMARY: Presser + SEP as the unpriced (or under-priced) hawkish package.

CLAIM: S&P 500 −0.45% to 7,551.81; Nasdaq Composite −0.01% to 25,978.42; Dow ~−1.2%.  
URL: https://www.morningstar.com/news/dow-jones/202609167425/sp-500-falls-045-to-755181-data-talk  
PUBLISHED: 2026-09-16  
QUOTE: S&P 500 closed at 7,551.81, down ~0.45%.  
SUMMARY: Index tape; XLC lagged SPY and lagged NDX.

CLAIM: META closed $673.31, +0.46%; range $671.91–$685.31.  
URL: https://www.techi.com/quote/META/historical/  
PUBLISHED: 2026-09-16 (as of 4:00 p.m. EDT)  
QUOTE: “Sep 16, 2026 … Close $673.31 … +0.46%”  
SUMMARY: Largest XLC weight was a positive outlier.

CLAIM: GOOGL closed $342.47, −0.73%.  
URL: https://www.techi.com/quote/GOOGL/historical/  
PUBLISHED: 2026-09-16 (as of 4:00 p.m. EDT)  
QUOTE: “Sep 16, 2026 … Close $342.47 … −0.73%”  
SUMMARY: Heavier Alphabet line lagged; two-name book split.

CLAIM: GOOG closed $341.53, +0.03%.  
URL: https://www.techi.com/quote/GOOG/historical/  
PUBLISHED: 2026-09-16 (as of 4:00 p.m. EDT)  
QUOTE: “Sep 16, 2026 … Close $341.53 … +0.03%”  
SUMMARY: Class C flat; does not offset GOOGL’s drag on a weight basis.

CLAIM: NFLX closed ~$76.41, about −1.91% vs $77.90 prior close.  
URL: https://stockanalysis.com/stocks/nflx/history/  
PUBLISHED: 2026-09-16  
QUOTE: Close ~$76.41, down ~1.91% from $77.90.  
SUMMARY: Media residual, not a new print.

CLAIM: August retail sales +1.2% to $773.9B, above ~0.7–0.8% consensus.  
URL: https://www.census.gov/retail/sales.html  
PUBLISHED: 2026-09-16 (08:30 ET advance)  
QUOTE: Sales +1.2% (±0.4%) from July; +6.0% YoY.  
SUMMARY: Hot activity print; same-shock as hawkish Fed, not an independent XLC HIT. Census page itself 403’d on fetch; figure taken from search citation of the Census release.

CLAIM: XLC close $113.00, −0.90%; open 114.22 (deterministic actuals).  
URL: (injected Channel 1 actuals)  
PUBLISHED: 2026-09-16 session  
QUOTE: ETF_PCT −0.90327; SPY_PCT −0.44099; REL_PCT −0.46228; OPEN 114.22; CLOSE 113.00.  
SUMMARY: Official grading tape for this review.

---

OUTCOME_BEGIN
SECTOR: Communication Services
ETF: XLC
ETF_PCT: -0.903270008176027
SPY_PCT: -0.4409916675517711
REL_PCT: -0.4622783406242559
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Hawkish FOMC increment (SEP/dots + Warsh presser) after a priced 25 bp hike; XLC sold with duration/growth and lagged SPY.
KEY_INTERACTION: Engine paid NQ/ES overnight rebound into an up call after LLM S0–S4=0 forbade mapping futures onto this two-name book; META held (+0.46%) while GOOGL/NFLX/telecom dragged.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Factor card (all zeros + FOMC suppressor) was the right pre-binary stance; engine up/mild from tape_anchor was a 08-27/09-10 miss — direction MISS, mild band HIT.
OUTCOME_END

---

## RESEARCH APPENDIX

**Queries run**
- memory_search: `Communication Services XLC sector prediction outcome 2026-09-16 FOMC` (disabled — index metadata missing)
- memory_search: `XLC leftover RS veto sector_rs_veto 09-15 lessons communication services` (disabled)
- web_search: `XLC Communication Services ETF September 16 2026 FOMC Meta Google`
- web_search: `FOMC September 16 2026 rate decision Warsh statement dots`
- web_search: `Meta Alphabet Netflix Disney stock September 16 2026 FOMC`
- web_search: `US retail sales August 2026 September 16 result`
- web_search: `XLC holdings performance September 16 2026 META GOOGL NFLX DIS T VZ CMCSA`
- web_search: `S&P 500 Nasdaq close September 16 2026 Fed hike stocks selloff`
- web_search: `META stock close September 16 2026`
- web_search: `NFLX Netflix close September 16 2026`
- web_search: `Disney DIS stock close September 16 2026`
- web_search: `Verizon VZ AT&T T Comcast CMCSA close September 16 2026`
- web_search: `site:apnews.com wall street stocks September 16 2026 Fed`
- x_search: `XLC Meta GOOGL NFLX FOMC September 16 2026 Communication Services` (2026-09-16 to 2026-09-17)
- web_fetch: `https://www.federalreserve.gov/newsevents/pressreleases/monetary20260916a.htm`
- web_fetch: `https://www.cnbc.com/2026/09/16/here-are-five-key-takeaways-from-wednesdays-fed-rate-hike.html`
- web_fetch: `https://www.reuters.com/business/view-markets-steady-after-fed-raises-rates-points-another-hike-this-year-2026-09-16/` (401 / JS wall — not used)
- web_fetch: `https://www.seattletimes.com/business/how-major-us-stock-indexes-fared-wednesday-9-16-2026/` (empty extract — not used)
- web_fetch: `https://www.census.gov/retail/sales.html` (403 Cloudflare — figure from search citation only)
- web_fetch: `https://www.techi.com/quote/META/historical/`
- web_fetch: `https://www.techi.com/quote/GOOGL/historical/`
- web_fetch: `https://www.techi.com/quote/GOOG/historical/`
- web_fetch: `https://www.cnbc.com/2026/09/16/fed-rate-decision-september-2026.html`

**Key sources and facts taken**
- Injected actuals (2026-09-16): XLC −0.903%, SPY −0.441%, rel −0.462%, open 114.22, close 113.00.
- Federal Reserve FOMC statement (2026-09-16, 2:00 p.m. EDT): +25 bp to 3.75–4.00%, 12–0; inflation elevated.
- CNBC takeaways (2026-09-16): green into decision, post-decision selloff, Dow −631, 2Y +7 bp, 16/18 dots want another 2026 hike, ~22-minute hawkish Warsh presser, unanimous vote.
- CNBC decision story (2026-09-16): Warsh “too high for too long”; PCE 3.7% / core 3.4%; hike was >90% priced; another hike in the SEP.
- Morningstar Data Talk (2026-09-16): SPX 7,551.81 −0.45%; Nasdaq 25,978.42 −0.01%.
- TECHi META/GOOGL/GOOG history (as of Sep 16, 4:00 p.m. EDT): META $673.31 +0.46%; GOOGL $342.47 −0.73%; GOOG $341.53 +0.03%.
- StockAnalysis NFLX history: ~$76.41, ~−1.91% vs $77.90.
- Search citations (StockAnalysis / MarketWatch): VZ ~$49.76 −3.28%; T ~−3.2% to −3.6%; CMCSA ~−2.8% to −3.0%; DIS ~$106.97–$106.99 vs $106.42 prior (~+0.5%).
- Census advance retail sales via search citation of census.gov/retail/sales.html: August $773.9B, +1.2% m/m, +6.0% y/y; consensus ~0.7–0.8%. Direct fetch 403’d.
- X/Twitter 2026-09-16: pre-FOMC “XLC strongest sector last 20d / META leadership” chatter — treated as leftover-RS color, not a close driver.
- Reuters FOMC markets URL: 401, unused as a fact source.