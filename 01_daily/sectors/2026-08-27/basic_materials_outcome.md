# Sector Outcome — Basic Materials — 2026-08-27

Actuals: {'etf': 'XLB', 'pct': -0.8198223248004122, 'spy_pct': 0.6552786111251541, 'rel': -1.4751009359255662, 'open': 53.220001220703125, 'close': 53.22999954223633}

Memory index is unavailable, so this autopsy uses the injected morning packet plus live tape. Official call was pipeline **up/flat** (the writeup wanted **flat/mild**).

## 0. Facts

XLB **−0.82%**, SPY **+0.66%**, relative **−1.48%**. Open **53.22** / close **53.23** vs prior close **53.67**. Path: **gap-and-chop**. Almost the entire loss printed overnight into the open (gap ≈ **−0.84%**); cash session then sat in **53.10–53.49** and finished a cent above the open. Not a late-day washout.

**Actual:** down / mild on the ETF print; relative lag is worse than the absolute move (notable underperformance vs SPY).

S&P **+0.72%** to **7,731**; Nasdaq **+1.57%**. Tech led; materials were among the laggards.

---

## 1. What drove the sector (taxonomy)

Primary cluster: **tech-led risk-on that did not transmit**, plus **Jackson Hole / sticky-PCE rate trepidation** hitting the chemicals-heavy XLB book. Industrial metals did not surge. Gold did not save the ETF.

| Factor | Morning | Session reality |
|---|---|---|
| Risk-on / beta expansion | PARTIAL (ES +0.31%, NQ +0.55%) | Confirmed **tech-only**. NVDA **+8.74%**; IT ~**+3.4%**; XLB **−0.82%**. Classic 8/25 composition miss. |
| Monetary metals | HIT (GC=F **+1.45%** premarket) | Faded. COMEX gold settle **+0.25%** to **$4,609.70**. NEM only **+0.52%**. Sleeve held; book did not. |
| Industrial metals | PARTIAL (Cu ~$6.60–6.64, off record) | Soft. COMEX Cu **−0.11%** to **$6.5870**; two-day **−1.83%** from **$6.7095** (Aug 25 record). FCX **−0.73%**. |
| China / property | HIT (July PMI 49.2 / construction 47.0) | Still the industrial offset; no same-day China print. |
| Inventory draw | MISS (LME Cu stocks rebuilt ~240kt) | Still off. No squeeze day. |
| Supply disruption | STALE (DRC) | Stale. |
| Policy / 232 | HIT, carried | Carried; not today’s catalyst. |
| Sector rotation into materials | MISS (1m rel **−0.86%**) | Confirmed miss, and then some: 1d rel **−1.48%**. |
| Shared macro / Jackson Hole | S0 = 0; “don’t ding S0 just because JH exists” | **This was the materials-specific overlay.** DJ/MarketScreener: materials down on Warsh-debut trepidation; sticky July PCE (headline ~**3.7%**) still in the tape. |

Chemicals/gases did the damage, not miners: **LIN −1.02%** ($490.33 → $485.35), **SHW −1.00%**, **ECL ~−1.6%**. That is the XLB weights. Gold-miner bid was a sub-sleeve, exactly as the morning single-ticker warning said — and the inverse also held: NEM green did not offset LIN/SHW.

Oil/USD were not the spine. DXY ~**99.13**, roughly flat. Copper’s two-day fade from the record is consistent with “tightness easing,” not a new squeeze.

---

## 2. Audit of morning S0–S4 (use morning numbers, not hindsight rewrites)

**Official emitted call:** pipeline `predicted_direction: up`, `predicted_magnitude_band: flat`, `total_score: 2.6`.  
**Writeup call:** direction **flat**, band **mild**, hand-sum **+1.2**. Those two disagreed before the open.

- **S0 = 0 — too kind for XLB.** Premarket *was* mixed/tech-led, and they correctly refused to treat NQ strength as a materials green light. They then underweighted a knowable overlay: NQ >> ES + carried hot PCE + Warsh debut = cyclicals fade. “Don’t score S0 negative merely because Jackson Hole exists” over-learned a prior false-negative. JH was two-sided for SPX; for XLB it was a rate/cyclical headwind.

- **S1 = +1 — too high.** Gold HIT was real in the *premarket* print and was correctly kept as an 8/14 offset, not a dampener. It was not permission for net +1 once copper was already off the record, LME stocks had rebuilt, and China/property was still HIT. Session gold **+0.25%** / copper **−0.11%** is S1 ≈ 0, not +1. Gold must not wash out the industrial breakdown — they said that, then still netted +1.

- **S2 = 0 — HIT.** Premarket mixed (LIN/FCX flat, NEM −0.5%, SHW +0.5%). Session mixed-to-weak. Friday’s miner thrust was correctly not re-scored.

- **S3 = 0 — HIT.** No flow shock. Slight 1m outflow was crowding risk, not forced selling.

- **S4 = +0.5 — wrong sign / wrong job.** 1d rel **+0.15%** is below the 0.5% bar in the **8/25 composition/transmission lesson they themselves marked active**. Tape was weak confirmation of a *stalled* run, not of today’s direction. 1m rel **−0.86%** was already a mild negative. S4 should have been 0 (or a small minus), not a +0.5 that the pipeline could promote into **up**.

**Pipeline vs prose:** components sum to **1.5**, ×0.8 = **1.2** (flat/mild). Pipeline `leading_sum: 3.0` / `total_score: 2.6` / **up** does not match those components. The 8/25-compliant writeup was overwritten by a deterministic **up/flat**. That is the process miss.

---

## 3. Interactions / double-count / knowable-at-open

- **Same-shock:** PCE counted once (carried). Gold counted once in S1. Iran/oil correctly *not* scored as an 8/18 complex-liquidation. Good.
- **Double-count avoided:** Friday FCX/NEM/AU thrust stayed in the 1w tape, not re-used as today’s S2. Good.
- **Missed interaction:** Nvidia AHs (8/26) × NQ >> ES × chemicals-heavy XLB × Warsh-day event risk is **one** transmission story, not three independent positives. Morning split “mild risk-on” (S0=0) from “gold HIT” (S1=+1) from “weak tape” (S4=+0.5) and let the last two still point up. 8/25 was written to collapse that stack into **flat**.
- **Gold ≠ XLB:** stated, then insufficiently enforced. NEM **+0.52%** vs LIN **−1.02%** is the book.
- **Knowable at open: partially.** Knowable: NQ >> ES, XLB 1d rel <0.5%, mixed breadth, 1m rel negative, copper off the record, China still contracting, 8/25 rule active, NVDA already reported, Warsh speech next day. Not fully knowable: gap size (−0.84% into the open), LIN/SHW/ECL leading the decline, gold fade from +1.45% to +0.25%, how hard the “trepidation” headline would hit cyclicals vs SPX.

Direction **up** was the part that was mostly knowable as too optimistic. **−0.82% / rel −1.48%** was only partly knowable.

---

## 4. Outliers inside the sector

- **LIN −1.02%** — top weight, industrial gases; this *is* XLB, not noise.
- **SHW −1.00%**, **ECL ~−1.6%** — coatings/chemicals; housing/industrial sleeve, not metals.
- **FCX −0.73%** — tracked copper; not an outlier.
- **NEM +0.52%** — gold sleeve worked at the name, failed at the ETF. Outlier vs XLB, not vs gold.
- Do not let GDX/COPX (noisy cross-source) drive the XLB autopsy. The ETF is LIN/chemicals first.

---

## Evidence

CLAIM: XLB 2026-08-27 close ~$53.23, prior close $53.67, range ~$53.10–$53.49, open $53.22.  
URL: https://www.marketwatch.com/investing/fund/xlb/download-data  
PUBLISHED: 2026-08-27  
QUOTE: Open $53.22 / high $53.49 / low $53.10 / close $53.23 (cross-checked vs injected actuals).  
SUMMARY: Gap-down day; cash session flat around the open. Injected ETF_PCT **−0.820%**.

CLAIM: SPY/S&P up on an Nvidia-led tape while materials lagged.  
URL: https://www.fool.com/coverage/stock-market-today/2026/08/27/stock-market-today-aug-27-nvidia-surges-on-blowout-results-and-surprising-guidance/  
PUBLISHED: 2026-08-27  
QUOTE: “Nvidia … closed at $227.98, up 8.74%.” “S&P 500 … +0.72% … Nasdaq … +1.57%.”  
SUMMARY: Risk-on was tech/AI, not cyclical beta. Matches injected SPY **+0.655%**.

CLAIM: Materials sold off on Jackson Hole trepidation.  
URL: https://www.marketscreener.com/news/materials-down-on-jackson-hole-trepidation-materials-roundup-ce7858dfd98afe2d  
PUBLISHED: 2026-08-27  
QUOTE: Headline: “Materials Down on Jackson Hole Trepidation — Materials Roundup.”  
SUMMARY: Same-day sector narrative was Warsh-debut / policy anxiety, not a metals squeeze. (Full text 403’d; headline + secondary summaries used.)

CLAIM: COMEX copper settled $6.5870, −0.11%; two-day −1.83% from Aug 25 record $6.7095.  
URL: https://www.morningstar.com/news/dow-jones/202608278498/comex-copper-settles-011-lower-at-65870-data-talk  
PUBLISHED: 2026-08-27 14:00 ET  
QUOTE: “Front Month Comex Copper … lost 0.75 cent per pound, or 0.11% to $6.5870 today … Down 12.25 cents or 1.83% over the last two sessions … Off 1.83% from its record high of $6.7095 hit Tuesday, Aug. 25, 2026.”  
SUMMARY: Spine “surge” was already fading into the open and faded further. Validates morning PARTIAL, not a +S1 upgrade.

CLAIM: COMEX gold settled $4,609.70, +0.25% — not the premarket +1.45% print.  
URL: https://www.morningstar.com/news/dow-jones/202608278496/comex-gold-settles-025-higher-at-460970-data-talk  
PUBLISHED: 2026-08-27 14:00 ET  
QUOTE: “Front Month Comex Gold … gained $11.50 … or 0.25% to $4609.70 today.”  
SUMMARY: Monetary-metals HIT faded in cash hours. 8/14 still applies as a sleeve; it did not carry XLB.

CLAIM: LIN closed $485.35, −1.02% from $490.33.  
URL: https://www.onvista.de/aktien/historische-kurse/Linde-Aktie-IE000S9YS762  
PUBLISHED: 2026-08-27  
QUOTE: Close $485.35, prior $490.33, −$4.98 / −1.02%.  
SUMMARY: Largest-weight chemical/gas name led the ETF lower.

CLAIM: FCX −0.73% to $78.42; NEM ~+0.52% to ~$132.29; SHW −1.00% to $345.18.  
URL: https://stockanalysis.com/stocks/fcx/history/  
PUBLISHED: 2026-08-27  
QUOTE: FCX close $78.42 vs $79.00 prior (−0.73%).  
SUMMARY: Breadth mixed-to-weak; gold miner up, copper miner and coatings down. Matches S2=0, not expanding leadership.

CLAIM: Warsh Jackson Hole keynote was the next-day (Aug 28, 10:00 ET) event; sticky PCE still the inflation backdrop.  
URL: https://www.reuters.com/business/feds-warsh-faces-challenge-whether-inflation-is-problem-or-not-2026-08-27/  
PUBLISHED: 2026-08-27  
QUOTE: (search extract) July PCE headline ~3.7% y/y, core ~3.3%; three July dissenters for a hike; Warsh debut at JH.  
SUMMARY: Event risk was live and materials-relevant. Morning treated it as two-sided/non-scoring; session treated it as a cyclical headwind.

---

OUTCOME_BEGIN
SECTOR: Basic Materials
ETF: XLB
ETF_PCT: -0.8198223248004122
SPY_PCT: 0.6552786111251541
REL_PCT: -1.4751009359255662
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Nvidia/tech-led risk-on failed to transmit; Jackson Hole/Warsh trepidation plus fading copper hit the chemicals-heavy XLB book (LIN/SHW), not a metals squeeze.
KEY_INTERACTION: 8/25 composition (NQ >> ES, 1d rel <0.5%, mixed breadth) × gold-sleeve ≠ LIN-weighted ETF × carried hot PCE/JH event risk — gold and weak tape were not independent ups.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Pipeline up/flat was a direction miss; the writeup’s 8/25 flat/mild was the better call and got overwritten — S1/S4 too positive, S0 too agnostic on JH for XLB.
OUTCOME_END

---

## RESEARCH APPENDIX

**Queries run**
- web_search: `XLB basic materials August 27 2026 stock market Jackson Hole`
- web_search: `XLB ETF August 27 2026 performance copper gold miners`
- web_search: `why did materials stocks fall August 27 2026 XLB copper gold Nvidia Jackson Hole`
- web_search: `XLB LIN FCX NEM SHW APD ECL stock price August 27 2026`
- web_search: `copper gold oil DXY August 27 2026 COMEX LME copper price`
- web_search: `S&P 500 sector performance August 27 2026 materials lag Nvidia Jackson Hole`
- web_search: `"Materials Down on Jackson Hole Trepidation" August 27 2026`
- web_search: `LIN Linde stock August 27 2026 close percent change`
- web_search: `NVDA Nvidia earnings August 26 2026 stock August 27 rally percent`
- web_search: `FCX Freeport Newmont NEM SHW Sherwin-Williams August 27 2026 percent change`
- web_search: `WTI crude oil August 27 2026 settle percent change DXY dollar`
- web_search: `Kevin Warsh Jackson Hole speech August 28 2026 materials stocks trepidation PCE`
- web_search: `XLB historical prices August 26 27 2026 open high low close`
- web_search: `Ecolab ECL Air Products APD August 27 2026 close`
- web_search: `COMEX copper August 26 2026 settlement price`
- web_search: `S&P 500 materials sector August 27 2026 -0.8% chemicals Linde`
- x_search: `XLB materials sector August 27 2026 Jackson Hole copper gold miners selloff` (2026-08-27 to 2026-08-28)
- x_search: `materials sector lag Nvidia rally Jackson Hole Warsh August 27 2026 XLB LIN FCX` (2026-08-27 to 2026-08-28)
- web_fetch: MarketScreener JH roundup (403), Reuters wrap (401), Yahoo XLB history (404), TipRanks (403), Axios Warsh (403), Onvista LIN (429)
- web_fetch success: Morningstar/DJ copper Data Talk; Morningstar/DJ gold Data Talk; Motley Fool 2026-08-27 market recap
- memory_search: unavailable (index metadata missing); used injected morning packet / scoreboard / 8/14, 8/21, 8/25 lessons only

**Key sources (title + URL + timestamp where available) and facts taken**

1. **Injected Channel 1 actuals** (deterministic) — XLB **−0.8198%**, SPY **+0.6553%**, rel **−1.4751%**, open **53.220**, close **53.230**. Ground truth for OUTCOME block.
2. **MarketWatch / Yahoo XLB history** — https://www.marketwatch.com/investing/fund/xlb/download-data — 2026-08-27 OHLC: prior close **$53.67**; 8/27 open **$53.22**, high **$53.49**, low **$53.10**, close **$53.23**. Path = gap-and-chop.
3. **Motley Fool, “Stock Market Today, Aug. 27: Nvidia Surges…”** — https://www.fool.com/coverage/stock-market-today/2026/08/27/stock-market-today-aug-27-nvidia-surges-on-blowout-results-and-surprising-guidance/ — fetched 2026-08-27T23:47Z — NVDA **$227.98, +8.74%**; S&P **7,731, +0.72%**; Nasdaq **26,541, +1.57%**.
4. **Dow Jones / Morningstar Data Talk, copper** — https://www.morningstar.com/news/dow-jones/202608278498/comex-copper-settles-011-lower-at-65870-data-talk — 2026-08-27 14:00 ET — Cu **$6.5870, −0.11%**; two-day **−1.83%**; record **$6.7095** on 2026-08-25.
5. **Dow Jones / Morningstar Data Talk, gold** — https://www.morningstar.com/news/dow-jones/202608278496/comex-gold-settles-025-higher-at-460970-data-talk — 2026-08-27 14:00 ET — GC **$4,609.70, +0.25%** (fade vs morning **+1.45%**).
6. **MarketScreener / DJ, “Materials Down on Jackson Hole Trepidation — Materials Roundup”** — https://www.marketscreener.com/news/materials-down-on-jackson-hole-trepidation-materials-roundup-ce7858dfd98afe2d — 2026-08-27 — headline used as same-day sector driver (page 403; corroborated by secondary search summaries).
7. **Onvista LIN history** — https://www.onvista.de/aktien/historische-kurse/Linde-Aktie-IE000S9YS762 — 2026-08-27 — LIN close **$485.35** vs **$490.33**, **−1.02%**.
8. **StockAnalysis FCX history** — https://stockanalysis.com/stocks/fcx/history/ — 2026-08-27 — FCX **$78.42, −0.73%**.
9. **NEM / SHW aggregators** (stockanalysis / stocknear) — NEM ~**+0.52%** to ~**$132.29**; SHW **−1.00%** to **$345.18**.
10. **Reuters / Axios search extracts on Warsh JH** — https://www.reuters.com/business/feds-warsh-faces-challenge-whether-inflation-is-problem-or-not-2026-08-27/ — 2026-08-27 — Warsh keynote **2026-08-28 10:00 ET**; sticky PCE backdrop (headline ~**3.7%**). Full Reuters pages 401/JS-blocked.
11. **X search (2026-08-27–28)** — caution/frozen-ahead-of-Warsh, NVDA-led NQ, limited XLB-specific chatter; **not** used as primary driver evidence.

**Not used as hard facts:** COPX/GDX day-move snippets (inconsistent with FCX); WTI **+1.59%** bounce (conflicts with morning CL **−1.11%** and not needed for the XLB call); any 403/401 page bodies.