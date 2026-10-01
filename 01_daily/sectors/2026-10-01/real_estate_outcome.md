# Sector Outcome — Real Estate — 2026-10-01

Actuals: {'etf': 'XLRE', 'pct': -0.5622086118166503, 'spy_pct': 0.17832832997064507, 'rel': -0.7405369417872953, 'open': 40.779998779296875, 'close': 40.68000030517578, 'source': 'yf_download'}

I have the close, the rate path, and the sector split. Compiling the autopsy against the morning S0–S4 scores rather than rewriting them after the fact.

**0. FACTS**

XLRE **−0.562%** (open **40.78** → close **40.68**). SPY **+0.178%**. Relative **−0.741%**. Path: opened near the high, traded a ~$40.41–$40.85 range, closed slightly red with elevated volume (~9.4M). Absolute move is **down / mild**. Relative lag vs SPY is the cleaner signal — REITs did not participate in the index bounce.

Memory search is paused this run (embedding metadata mismatch). Standing lessons used are only those injected in the morning packet (09-22 flat-cap, 09-23 skew, 09-25 level-vs-impulse, 08-27 leftover-tech ban).

---

**1. What drove the sector**

Primary object: **the long end, as a LEVEL that stayed in the multi-decade stress zone, plus a same-day impulse that spiked then faded.**

- 10Y hit a fresh post-2002 high in the morning (eOption: ~**5.34%**, +3 bp at the spike) on ISM Prices Paid **77.9** (+6.8 pts) and still-tight claims (**197k**), then pulled back mid-afternoon. Close ~**5.248%** (eOption −5 bp) / Hoya **5.24% (−5 bp)**; 30Y **5.61% (−2 bp)**; 2Y **4.78% (−10 bp)**. Front end eased more than the long end. For a pure-duration sector that is **not** relief — it is a two-sided rates day that left the binding constraint (30Y still ~5.6, 10Y still ~24-year-high zone) intact.
- SPX path matches: flat open → yield-driven morning pressure → noon low → bounce as yields faded, **paced by XLK / SOX / IGV, XLI, XLE**. XLRE did not join that bounce. Heatmap: Energy ~+1.9%, Tech green, Real Estate ~−0.55%.
- Subsector: **net-lease** (the most bond-like sleeve) worse than the ETF — Realty Income **O −1.42%** to $53.53. eOption: net-lease down ~14% since late August vs XLRE ~9% as 10Y climbed >50 bp. Homebuilders (DHI/KBH/PHM/TOL) weaker on mortgage-rate pass-through. That is duration + housing-affordability, not a single-name event.
- Construction print was a **split, not a REIT bid**: private “office” +4.6% m/m was **data centers +7.5% / +73% y/y**; other private office **unchanged / −9.4% y/y**. Matches the morning MAP heat (Hotel/Residential nested up; Office/Mortgage/Specialty down; Industrial/Healthcare/Retail flat).

Taxonomy: **S0 rates/duration (level + same-day inflation impulse)** did the work. **S2/S4 relative lag** confirmed. Leftover tech/AI beta did **not** transmit into REITs (08-27 held).

---

**2. Audit of morning S0–S4 (use morning numbers, do not rewrite)**

Morning scores: S0 **−1.0**, S1 **−0.5**, S2 **−1.0**, S3 **−0.5**, S4 **−1.0**, mult **0.85**, leading sum **−5.5**, total **−5.858**, **down/mild**, divergence **true**, confidence **0.55**.

| Sleeve | Morning read | Reality | Verdict |
|---|---|---|---|
| **S0 −1** | Stress-zone 30Y 5.59 scored **once** as level; dovish PCE as half-notch offset; 09-23 negative skew, not S0=0 | 10Y spiked to ~5.34 then closed ~5.25; 30Y still ~5.61. Level never left the stress zone. PCE/front-end ease showed up as the **afternoon fade**, which is why the print was mild not notable. S0=−1 (not 0, not −2) was the right notch. | **HIT** |
| **S1 −0.5** | Rate object banned from S1 (09-25). Residual = light office/refi structural. Live board at open was Finviz flat-to-+1 bp, **not** a fresh 1d impulse. | Correct **at the open**. The fresh impulse (ISM prices → 10Y spike) arrived **after** the open. Scoring it into S1 pre-open would have been a 09-25 violation. Structural office did not need to do extra work; net-lease underperformance is the same duration object. S1=−0.5 was slightly busy but not a sign error. | **Mostly HIT** (method right; residual drag over-specified vs the live rate path) |
| **S2 −1** | Breadth failure / uniform relative lag; XLRE absent PM board scored 0 | ETF −0.56%, equity REITs −0.3% (Hoya), O −1.42%, heatmap RE ~−0.55%. Broad, not WELL/EQIX/PLD. | **HIT** |
| **S3 −0.5** | Stale rotation-out, damped, not restacked with S2 | Rel −0.74% continues 1d −0.83 / 1w −1.55 / 1m −6.14. Tech/energy absorbed the SPX bounce. | **HIT** |
| **S4 −1** | Confirmation only | Another red relative day. Tape confirmed; it did not invent the thesis. | **HIT** |

**09-22 flat-cap:** ES/NQ were inside ±0.5% at the open. Cap said **mild at most**, pull toward flat. They kept **down/mild** on stress-zone skew (09-23/09-25). Reality: **down/mild**. The cap earned its keep — this was not a smash. Promoting to notable on the −5.858 score would have been a miss on magnitude.

**09-25 level vs impulse:** Morning scored the **level** in S0 and refused to restack Monday’s +2–3 bp into S1. Correct. Today’s **fresh** impulse was ISM Prices Paid, which was **not** on the open board.

**Divergence flag:** Green ES/NQ vs negative leading sum. SPY **+0.18%**, XLRE **−0.56%**, rel **−0.74%**. Flag was real, not cosmetic.

**08-27 leftover NQ/AI:** Tech led the bounce; XLRE still red. Not duration relief.

---

**3. Interactions / double-count / knowable-at-open**

- **Same-shock:** Long-end LEVEL (S0) and the afternoon **fade** are one object with two intra-day signs. Morning did **not** double-count it. The fade is why absolute XLRE was only −0.56% while relative lag stayed ~−0.74% (SPY bounced on tech/energy, not on REIT duration relief). Do not score the close-of-day −5 bp 10Y as “rates falling / REIT bid” — the **level** stayed in the 24-year-high zone and REITs still lagged.
- **S2 vs S3 vs S4:** All three are the same relative-lag object at different horizons. Morning damped S3 and called S4 confirmation. That discipline was right; stacking them harder would have over-fit a mild print.
- **Knowable at open:** Stress-zone long end, uniform multi-horizon lag, green-but-inside-±0.5% ES/NQ, XLRE off the PM board — **yes**. ISM Prices Paid 77.9, the 10Y spike to 5.34%, the noon-to-close fade, and O’s extra −1.4% — **no**. PCE-as-offset was only **partially** knowable (it was in the News Judge, not in a live long-end down-move).

**KNOWABLE_AT_OPEN: partially**

---

**4. Outliers inside the sector**

- **Net-lease / Realty Income (O) −1.42%** vs XLRE −0.56% — more bond-like cash flows, extra duration beta. Confirms the call should stay ETF-level, not WELL-defined (09-24).
- **Homebuilders** weaker than listed equity REITs; Hoya **Housing +0.3% vs equity REITs −0.3%** — residential nested vs public REITs diverged, matching morning MAP split.
- **Data-center construction** (office-including-DC +4.6% m/m; DC +73% y/y) vs **other office −9.4% y/y** — EQIX/DLR structural bid is real and **still not** an XLRE-day vote (08-27 / size_gate).
- **Energy +~1.9% / Tech green / XLRE red** — the index bounce was a different factor set. Using it as REIT relief would have been the 08-27 error.

No evidence a single name (WELL/EQIX/PLD/BXP) set the ETF print.

---

OUTCOME_BEGIN
SECTOR: Real Estate
ETF: XLRE
ETF_PCT: -0.562
SPY_PCT: 0.178
REL_PCT: -0.741
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Stress-zone long-end rates (10Y spiked to ~24-year highs on ISM prices, then faded) kept REITs as a duration laggard while SPX bounced on tech/energy.
KEY_INTERACTION: Same rate object: AM yield spike vs PM fade; fade capped XLRE at mild, SPY's tech bounce widened relative lag — not a second factor.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Down/mild HIT on both dir and mag; S0=-1 and the 09-22 flat-cap were the load-bearing calls; do not reclassify the close-of-day -5bp 10Y as duration relief.
OUTCOME_END

---

**Evidence**

CLAIM: XLRE closed down ~0.51–0.56% on 2026-10-01 (injected tape −0.562%; ChartExchange −0.513% to $40.70, vol 9.38M).
URL: https://chartexchange.com/symbol/nyse-xlre/historical/
PUBLISHED: 2026-10-01 15:59 ET
QUOTE: “At Close Oct 1, 2026 3:59:59 PM EDT 40.70USD −0.513% (−0.21) 9,377,866”
SUMMARY: Independent close confirms Channel 1 direction and mild magnitude.

CLAIM: S&P 500 +0.20% to 7,666; 10Y spiked to ~5.34% (highest since 2002) then ended ~5.248%; XLRE under pressure as 10Y stayed >5.3% zone; net-lease the weakest sleeve.
URL: https://www.eoption.com/market-review-october-01-2026/
PUBLISHED: 2026-10-01
QUOTE: “Treasury yields extended their recent run higher as the 10-yr and 30-yr yield both hit more than 20 years high again… The 10-year yield hit its highest level since 2002 today, rising over 3bps to around 5.34% (but ended around 5.24%)… REIT (XLRE) sector has come under pressure the last few weeks amid the quick ascent of the 10-year Treasury rate above 5.3%… net lease sector (down roughly 14% since late August).”
SUMMARY: Path = AM rate smash, PM fade, SPX bounce in tech/industrials/energy; REITs remained the duration casualty.

CLAIM: Equity REITs −0.3% vs S&P +0.3%; 2Y 4.78% (−10 bp), 10Y 5.24% (−5 bp), 30Y 5.61% (−2 bp).
URL: https://x.com/HoyaCapital/status/2105752698981077311
PUBLISHED: 2026-10-01
QUOTE: Hoya Capital daily: Equity REITs −0.3%; 10-Year 5.24% (−5 bps); 30-Year 5.61% (−2 bps).
SUMMARY: Listed REITs lagged a green index with the long end still ~5.6%; front end eased more than the long end.

CLAIM: ISM Manufacturing PMI 54.5 in September; Prices Index 77.9, +6.8 pts vs August 71.1.
URL: https://www.prnewswire.com/news-releases/manufacturing-pmi-at-54-5-september-2026-ism-manufacturing-pmi-report-302894520.html
PUBLISHED: 2026-10-01
QUOTE: “The Prices Index remained in expansion… registering 77.9 percent, a notable increase of 6.8 percentage points compared to August’s reading of 71.1 percent.”
SUMMARY: Same-day inflation impulse that was **not** on the morning board; the knowable-at-open miss on the intra-day yield spike.

CLAIM: August construction +0.9%; private “office” +4.6% was data centers (+7.5% m/m, +73% y/y); other private office unchanged / −9.4% y/y.
URL: https://www.agc.org/news/2026/10/01/construction-spending-climbs-august-july-trails-year-ago-level-contractors-face-multiple-threats
PUBLISHED: 2026-10-01
QUOTE: “this apparent increase was due entirely to data-center construction, which soared 7.5% for the month and 73% over the past 12 months, while other private office construction was unchanged from July and down 9.4% from a year earlier.”
SUMMARY: Confirms morning MAP split; DC strength is not an XLRE-day offset.

CLAIM: Realty Income (O) closed $53.53, −1.42% on 2026-10-01.
URL: https://stockanalysis.com/stocks/o/history/
PUBLISHED: 2026-10-01 16:00 ET
QUOTE: “53.53 −0.77 (−1.42%) At close: Oct 1, 2026, 4:00 PM EDT”
SUMMARY: Net-lease outlier vs XLRE −0.56%; more bond-like sleeve, not a single-name fluke.

---

## RESEARCH APPENDIX

**Queries run**
- web_search: `XLRE ETF October 1 2026 performance real estate REITs`
- web_search: `US 10 year yield October 1 2026 30 year Treasury REIT stocks`
- web_search: `SPY XLRE October 1 2026 stock market sector performance`
- web_search: `S&P 500 sectors October 1 2026 real estate energy technology heatmap`
- web_search: `October 1 2026 Treasury yields 10-year 30-year close payrolls France bonds`
- web_search: `XLRE constituents October 1 2026 WELL PLD EQIX AMT CCI SPG O performance`
- web_search: `ISM manufacturing October 1 2026 prices paid 77.9 jobless claims construction spending`
- web_search: `site:x.com HoyaCapital equity REITs October 1 2026 10-year 5.24`
- web_search: `net lease REITs October 1 2026 Realty Income O selloff 10-year`
- web_search: `"10-year" yield 5.24 OR 5.248 October 1 2026 stocks bounce real estate`
- x_search: XLRE/REITs/Treasury yields on 2026-10-01 (from_date 2026-10-01, to_date 2026-10-02)
- x_search: Hoya Capital daily REIT update, allowed_x_handles HoyaCapital
- memory_search: XLRE / sector_real_estate lessons (unavailable)

**Key sources (title + URL + timestamp + facts taken)**
1. **ChartExchange XLRE historical** — https://chartexchange.com/symbol/nyse-xlre/historical/ — fetched 2026-10-01 ~21:11Z — close $40.70, −0.513%, volume 9,377,866.
2. **eOption Market Review: October 01, 2026** — https://www.eoption.com/market-review-october-01-2026/ — fetched 2026-10-01 ~21:01Z — SPX +0.20% to 7,666; 10Y spike ~5.34% then ~5.24%/5.248%; XLK/XLI/XLE led bounce; XLRE/net-lease duration pressure; claims 197k; ISM 54.5 / prices 77.9; construction +0.9%.
3. **Hoya Capital (@HoyaCapital)** — https://x.com/HoyaCapital/status/2105752698981077311 — 2026-10-01 — Equity REITs −0.3%, S&P +0.3%, 2Y 4.78% (−10 bp), 10Y 5.24% (−5 bp), 30Y 5.61% (−2 bp).
4. **ISM Manufacturing PMI September 2026 (PR Newswire)** — https://www.prnewswire.com/news-releases/manufacturing-pmi-at-54-5-september-2026-ism-manufacturing-pmi-report-302894520.html — 2026-10-01 — PMI 54.5, Prices 77.9 (+6.8).
5. **AGC construction spending** — https://www.agc.org/news/2026/10/01/construction-spending-climbs-august-july-trails-year-ago-level-contractors-face-multiple-threats — 2026-10-01 — +0.9% to $2.20T; DC vs other-office split.
6. **StockAnalysis Realty Income history** — https://stockanalysis.com/stocks/o/history/ — 2026-10-01 16:00 ET — O $53.53, −1.42%.
7. **StockTitan heatmap / sector rotation (via search)** — https://www.stocktitan.net/stock-market-heatmap — Real Estate ~−0.55%, Energy ~+1.95%, Tech green.

**Blocked / unused:** Reuters, MarketScreener, GuruFocus, AP, 247wallst, ETFAction, tradersagency, stocknear — 401/403/Cloudflare. Not used as evidence.

**Memory:** index unavailable (embedding metadata mismatch). Did not invent prior-session grades beyond the injected morning packet.