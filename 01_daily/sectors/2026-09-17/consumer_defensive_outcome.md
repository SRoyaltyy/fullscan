# Sector Outcome — Consumer Defensive — 2026-09-17

Actuals: {'etf': 'XLP', 'pct': 0.19200291515559798, 'spy_pct': 1.1338754633487547, 'rel': -0.9418725481931567, 'open': 83.51000213623047, 'close': 83.48999786376953, 'source': 'yf_download'}

Memory search is paused (embedding metadata mismatch); this review uses the injected morning card, Channel 1 actuals, and live sources — not MEMORY.md.

## 0. Facts

XLP **+0.192%** vs SPY **+1.134%** → relative **−0.942%**. Open **83.51** / close **83.49**: cash session faded a few cents after a slightly green open. Path = premarket beta (+0.18% PM) that never expanded; SPY ripped, staples sat.

| Print | Value |
|---|---|
| ETF | XLP +0.192% |
| SPY | +1.134% |
| Rel | −0.942% |
| Path | Open 83.51 → close 83.49 (session slightly red vs open) |
| Actual dir / mag | **up / flat** |
| Morning call | **up / mild** (score 0.765, mult 0.8, divergence flagged) |

Direction is a mechanical HIT on a 19 bp print. Magnitude is a MISS. The sector object was relative lag, not an FTS up-day.

CLAIM: S&P 500 ~+1.14% on 2026-09-17; Nasdaq led (~+1.7%).
URL: https://www.barrons.com/market-data/stocks/us/indexes
PUBLISHED: 2026-09-17
QUOTE: S&P 500 rose about 1.14% to 7,637.76; Nasdaq Composite ~+1.69%.
SUMMARY: Broad risk-on cash session after 09-16 post-FOMC selloff.

CLAIM: Intraday sector book had XLK ~+2.36% vs XLP ~+0.13% (XLY ~+1.23%).
URL: https://streetstats.finance/markets/sectors-industries
PUBLISHED: 2026-09-17 (~15:35 ET snapshot)
QUOTE: Consumer Staples +0.13%; Technology +2.36%; Discretionary +1.23%.
SUMMARY: Live leadership was tech/cyclical, not staples. Matches Channel 1 close (XLP +0.19% / SPY +1.13%).

## 1. What drove the sector

Primary object: **post-FOMC risk-on rebound / rotation away from defensives.** Tech and chips led; oil kept offering; 10Y pulled back from the 5% stress print. XLP only tagged along with low-beta noise.

CLAIM: Rebound driven by falling oil, lower yields, tech dip-buy after the 09-16 hike.
URL: https://www.investopedia.com/stock-market-today-dow-jones-s-and-p-500-09172026-12125532
PUBLISHED: 2026-09-17
QUOTE: 10-year yield fell back below 5%; oil eased; tech led the rebound.
SUMMARY: Same four-leg shock the morning named as one anti-FTS object (ES/NQ rip + XLK/XLY lead + oil offered + VIX contango).

CLAIM: WTI continued lower (~$101.2, ~−1.1%).
URL: https://www.marketwatch.com/investing/future/cl.1/download-data
PUBLISHED: 2026-09-17
QUOTE: WTI close ~$101.23–$101.31 vs prior ~$102.43.
SUMMARY: Input-cost relief printed. It did **not** produce staples RS — oil was a risk-on/inflation-relief leg, not an FTS bid.

8:30 slate (not knowable as signed at the open; morning correctly refused to flatten S0):

CLAIM: Initial claims 196k vs ~208k consensus; housing starts 1.275M (−2.6%); Philly Fed 37.8 (beat, still expansion).
URL: https://www.reuters.com/business/us-weekly-jobless-claims-unexpectedly-fall-2026-09-17/
PUBLISHED: 2026-09-17
QUOTE: Claims fell unexpectedly to 196,000; four-week average 203,250.
SUMMARY: Labor surprise was mildly risk-on, not a staples haven print. Housing miss was local. Did not rewrite the sector object.

Taxonomy vs live:

| Factor | Morning | Live |
|---|---|---|
| Risk-on / beta expansion | HIT (S0 −1) | HIT — SPY +1.13%, XLK lead |
| FTS RS vs cyclicals | MISS | MISS — rel −0.94% |
| Rotation out of defensives | HIT (not double-counted in S1) | HIT |
| Input-cost relief (oil) | PARTIAL, capped | Printed; did not cancel rotation |
| Breadth expansion | MISS / split HEAT | Split: WMT down, COST flat, PG modestly up |
| Fresh staples earnings | MISS (COST 09-24) | MISS |
| Same-morning flow spike | MISS | No evidence of a 09-17 create/redeem event |

## 2. Audit of morning S0–S4 (use morning numbers, not rewrites)

**S0 −1 (risk-on overlay only):** Right object, right size. Absolute +0.19% is the “small green beta” the card already named. Relative −0.94% is the scored headwind. Not −2 (NQ lead was 39 bp, XLP still green absolutely). Not 0.

**S1 −0.5 (rotation residual + capped oil + mixed ag, no food-crash):** Held. Oil relief did not offset rotation. No new CPB/GIS/KHC print. Nested discounter HEAT was not averaged into the ETF — correct, because WMT was an **outlier drag**, not an ETF lift.

**S2 0:** Held. No sector-wide bid. Large-cap quality did not carry XLP.

**S3 0:** Held. Trailing outflows were drip into 09-16, not a same-session spike. InvestingLive’s XLP “Cooling Off” note is **through the 09-16 close**, not a 09-17 flow print — do not restack it as a new S3.

**S4 0:** Held as “not a haven / leftover 3d/1w RS is paid.” Cash 1d +0.19% ≈ PM +0.18%. Leftover 3d/1w RS (+1.28% / +1.43%) did **not** continue. Divergence flag (leading − vs leftover RS +) — **trust factors** was the right instruction.

**Pipeline vs overlay:** LLM overlay **−3.2** and leading factors **−1.5** said relative-negative / not FTS. Engine still printed **up/mild** because tape_anchor (ES +1.71, PM:XLP +0.18) + index_carry **1.846** overrode the overlay. Cash matched the **PM beta**, not the “up/mild” label. This is the same class of error as 09-11 (benign tape = relative negative for a low-beta defensive) except today the engine won the printed direction.

CLAIM: WMT ~−0.7% on 09-17; COST ~flat; XLP ~+0.19%.
URL: https://stocknear.com/stocks/WMT/history
PUBLISHED: 2026-09-17
QUOTE: WMT close ~$106.47–$106.79 vs prior $107.50.
SUMMARY: Largest XLP weight lagged the ETF. Do not treat discounters as the sector.

## 3. Interactions / double-count / knowable-at-open

**Same-shock:** ES/NQ ≥ +0.5% + XLK/XLY lead + oil offered + VIX contango = **one** S0 object. Morning did not restack FOMC, did not restack 10Y>5% from 09-15, did not convert leftover RS into an S2/S4 veto, did not fire 08-27 notable (NQ lead 39 bp). Audit clean.

**Double-count trap avoided:** “Sector rotation out of defensives” was tagged HIT on the grid but **not** added as a second S1 full HIT. Correct. Live relative −94 bp is that same object, not a new one.

**Oil interaction:** Offering was knowable at the open (CL=F −0.98% PM). Using it as input-cost **+** in S1 and **not** as an FTS bid in S0 was right. Live: oil down, XLP still lagged — oil was risk-on confirmation, not a staples tailwind.

**8:30:** Knowable as *event risk*, not as a signed factor. Claims beat was incremental risk-on after the open. It did not create the lag; it may have slightly widened it. Do not retrofit S0 more negative because claims printed strong.

**KNOWABLE_AT_OPEN: yes** for the sector object (relative lag / not FTS). The 19 bp absolute green was also knowable as PM beta. The **up/mild** printed label was an engine/anchor artifact, not a new afternoon fact.

## 4. Outliers inside the sector

- **WMT (~−0.7%)** — largest weight, opposite sign to XLP. Not a fresh 08-20 earnings rewrite; just didn’t participate in the risk-on tape. Nested MAP HEAT “discounters up” from the morning did **not** survive into cash.
- **COST (~flat)** — quiet ahead of 09-24 Q4. Correctly excluded from the ETF thesis.
- **PG modestly green** — household sleeve did not fail hard; it also didn’t lead.
- No packaged-food crash, no dividend/guidance cut, no COST print.

X chatter that “XLP was the only red sector” was an **intraday** snapshot, not the close (XLP finished +0.19%). Relative lag is the robust fact; “only red” is not.

---

OUTCOME_BEGIN
SECTOR: Consumer Defensive
ETF: XLP
ETF_PCT: 0.192
SPY_PCT: 1.134
REL_PCT: -0.942
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: flat
PRIMARY_DRIVER: Post-FOMC risk-on rebound led by tech; XLP only printed leftover beta.
KEY_INTERACTION: One anti-FTS shock (ES/NQ rip + XLK/XLY lead + oil offered) → tiny absolute green and −94 bp relative lag.
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: LLM S0/S1 relative-negative card was right; engine up/mild was a tape-anchor HIT on 19 bp of non-thesis beta.
OUTCOME_END

---

## RESEARCH APPENDIX

**Queries run**
- memory_search: Consumer Defensive XLP sector prediction lessons risk-on rotation FOMC → **disabled** (index metadata missing)
- web_search: XLP consumer staples ETF September 17 2026 performance vs SPY
- web_search: stock market September 17 2026 S&P Nasdaq consumer staples rotation after Fed
- web_search: jobless claims housing starts Philly Fed September 17 2026 market reaction
- web_search: XLP WMT COST PG KO stock performance September 17 2026
- web_search: oil prices WTI September 17 2026 close consumer staples
- web_search: S&P 500 sectors performance September 17 2026 consumer staples technology
- web_search: site:reuters.com jobless claims September 17 2026
- web_search: Walmart stock down September 17 2026 XLP
- web_search: "consumer staples" XLP lag OR underperform September 17 2026
- web_search: why is the stock market up today September 17 2026 oil tech rebound
- web_search: Philly Fed September 2026 37.8 housing starts August 1.275 million
- web_fetch: https://investinglive.com/stocks/stock-sector-rotation-with-the-fed-decision-healthcare-attracts-fresh-interest-as-consumer-staples-lose-support/
- web_fetch: https://streetstats.finance/markets/sectors-industries (thin extract)
- web_fetch attempts (blocked/403/401): TipRanks 9-17 recap, AP markets, Yahoo recap, Reuters claims, Bloomberg claims, Benzinga sector leaders
- x_search: XLP consumer staples vs SPY rotation September 17 2026 risk-on (2026-09-17..18)

**Key sources (title + URL + timestamp)**
- Channel 1 actuals (pipeline, 2026-09-17 close) — XLP +0.192%, SPY +1.134%, rel −0.942%, open 83.51 / close 83.49.
- Barron’s US indexes, 2026-09-17, https://www.barrons.com/market-data/stocks/us/indexes — SPX ~+1.14%, Nasdaq ~+1.69%, Dow ~+0.61%.
- StreetStats sector snapshot, 2026-09-17 ~15:35 ET, https://streetstats.finance/markets/sectors-industries — XLK ~+2.36%, XLY ~+1.23%, XLP ~+0.13%.
- Investopedia market recap, 2026-09-17, https://www.investopedia.com/stock-market-today-dow-jones-s-and-p-500-09172026-12125532 — oil down, 10Y back below 5%, tech-led rebound.
- Reuters claims, 2026-09-17, https://www.reuters.com/business/us-weekly-jobless-claims-unexpectedly-fall-2026-09-17/ — claims 196k vs ~208k; housing starts 1.275M.
- Philly Fed / Seeking Alpha, 2026-09-17, https://seekingalpha.com/news/4643753-philly-fed-manufacturing-index-falls-less-than-expected-in-september — September index 37.8 vs ~30.5–34.
- Official Philly Fed, 2026-09-17, https://www.philadelphiafed.org/surveys-and-data/regional-economic-analysis/mbos-2026-09 — confirms 37.8.
- WTI historical (Investing/MarketWatch), 2026-09-17 — WTI ~$101.2, ~−1.1%.
- Stocknear / Stockmonitor WMT, 2026-09-17 — WMT ~−0.7% vs prior $107.50.
- InvestingLive sector-rotation note, 2026-09-17 (data through 09-16 close), https://investinglive.com/stocks/stock-sector-rotation-with-the-fed-decision-healthcare-attracts-fresh-interest-as-consumer-staples-lose-support/ — XLP weekly flows flipped to outflows into 09-16; **not** a 09-17 same-session print.
- X: CiovaccoCapital 2026-09-17, https://x.com/CiovaccoCapital/status/2100647430865686942 — intraday “XLP only red sector” (intraday, not close).

**Facts taken**
- Absolute XLP was +19 bp vs prior close and slightly red open-to-close; SPY +1.13%; relative −94 bp.
- Leadership was XLK/Nasdaq, not XLP; oil offered; yields eased; claims beat was risk-on, not haven.
- WMT lagged the ETF; COST quiet; no fresh packaged-food catalyst.
- Morning leftover 3d/1w RS did not persist.
- Memory index unavailable this run.