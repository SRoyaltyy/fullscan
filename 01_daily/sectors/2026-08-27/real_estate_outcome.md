# Sector Outcome — Real Estate — 2026-08-27

Actuals: {'etf': 'XLRE', 'pct': -0.9536489326250397, 'spy_pct': 0.6552786111251541, 'rel': -1.6089275437501938, 'open': 44.790000915527344, 'close': 44.65999984741211}

Memory search is paused (index metadata missing), so this review uses the injected morning packet, deterministic actuals, and live sources only.

## 0. Facts

XLRE **−0.95%** (open **44.79** → close **44.66**). SPY **+0.66%**. Relative **−1.61%**. Path: gap/weak open versus the ~45.09 prior close, then a further drift lower — not a late-day crash. Direction **down**, magnitude **mild** on the ETF print, **notable relative** versus SPY.

That is a clean duration-proxy miss versus a tech-led tape: equity REITs ~**−1.0%**, housing index **−1.1%**, S&P **+0.7%**.

CLAIM: XLRE closed ~$44.64–$44.66 on 2026-08-27, down ~0.95–1.0%.
URL: https://stocknear.com/etf/XLRE/history
PUBLISHED: 2026-08-27 session / historical table
QUOTE: “Close: ~$44.64–$44.66… down ~$0.45 or –0.95% to –1.0% from the prior close of $45.09”
SUMMARY: Confirms the deterministic −0.9536% print and weak open/close path.

CLAIM: Session yields backed up; equity REITs underperformed a green S&P.
URL: https://x.com/HoyaCapital/status/2093070764157092081
PUBLISHED: 2026-08-27 20:18 ET
QUOTE: “S&P 500: +0.7% / Equity REITs: −1.0% / Housing Index: −1.1% / Crude Oil: $83.67 (+1.8%) / 2-Year: 4.23% (+2 bps) / 10-Year: 4.67% (+3 bps) / 30-Year: 5.19% (+2 bps)”
SUMMARY: Live close tape reversed the morning easing-yield / oil-slide snapshot.

CLAIM: Official July PCE was 3.7% headline / 3.3% core YoY, 0.2% MoM both.
URL: https://www.bea.gov/news/2026/personal-income-and-outlays-july-2026
PUBLISHED: 2026-08-26 (BEA Personal Income and Outlays, July 2026)
QUOTE: “From the same month one year ago, the PCE price index for July increased 3.7 percent. Excluding food and energy, the PCE price index increased 3.3 percent”
SUMMARY: PCE was already in the market before 8/27 cash open; it was not a same-day surprise.

CLAIM: Warsh’s first Jackson Hole keynote was still ahead (Fri 8/28 ~10am ET), with sticky inflation and long-end anxiety as the setup.
URL: https://www.axios.com/2026/08/27/warsh-guidance-jackson-hole
PUBLISHED: 2026-08-27
QUOTE: Warsh scheduled to deliver the Jackson Hole keynote 2026-08-28 ~10 a.m. ET; markets pressing for inflation/reaction-function clarity rather than tactical guidance.
SUMMARY: 8/27 was a positioning day into a two-sided policy event, not a duration-relief day.

---

## 1. What drove the sector

Primary mechanism: **rates / duration**, not SPX beta.

Morning treated the **prior-close** curve (10Y 4.64%, −6 bp; 30Y 5.17%, −6 bp; DFII10 2.32%, −6 bp) plus an **oil slide** (CL −1.11%, BZ −2.11%) as an 08-25-style easing-inflation spine. Cash session did the opposite: **10Y 4.67% (+3 bp), 30Y 5.19% (+2 bp), WTI ~+1.8% to $83.67**. Bond proxies sold; growth led.

That is the taxonomy spine: **rates rising / REIT selloff**, not duration relief. The 30Y stayed in the **~5.2% stress zone** (08-21 level-vs-change). Sticky PCE (3.7 / 3.3) plus **Warsh Friday** kept the long end from being treated as a buy-the-dip.

Secondary, not the ETF driver:
- **Risk-on was XLK, not XLRE.** SPY +0.66% with tech leadership; utilities also lagged. This is duration vs equity-beta split, not “risk-on helps REITs.”
- **Housing tape confirmed:** housing index −1.1% with equity REITs −1.0%.
- Structural sleeves (data-center demand, industrial occupancy, office vacancy, 2026 refi wall) did not flip in one session. They explain *why* XLRE is a picky beta, not *why* it dropped 95 bp today.

CLAIM: Sector leadership was tech; XLRE/XLU lagged.
URL: https://www.benzinga.com/etfs/sector-etfs/26/08/61461100/leading-and-lagging-sectors-august-27-2026
PUBLISHED: 2026-08-27
QUOTE: XLK among leaders; XLRE listed among laggards (intraday snapshot showed XLRE already red).
SUMMARY: Confirms rotation into growth, not into rate-sensitive defensives.

---

## 2. Audit of morning S0–S4 (use morning numbers, do not rewrite them)

Morning scores as written: **S0 +1, S1 +1, S2 0, S3 0, S4 0**, mult **0.9**, total **4.5**, call **up / mild**. Narrative even said “up/flat” and “divergence flagged,” but the pipeline emitted **up/mild** with `divergence_flagged: False`.

| Sleeve | Morning | vs 8/27 reality | Verdict |
|---|---|---|---|
| **S0 +1** | Easing real yields, oil down, green futures, in-line core PCE | Yields **up** on the day; oil **reversed up**; SPY up on tech; PCE was **yesterday’s** print | **Too high.** Should have been **0**. Hawkish Collins + Warsh Friday + still-hot headline PCE were already in the morning packet. |
| **S1 +1** | Real-yield HIT + duration PARTIAL + DC/industrial HITS vs office/refi | Duration partial **failed live**. DC/industrial are **not 1-day**. Office/refi are slow negatives | **Too high.** Same-day book was **0 / −1**. Structural HITS padded a duration call. |
| **S2 0** | 1d rel −0.62%, 3d flat, 1w +0.61%, 1m −5.40%; large-cap leadership not breadth | Session breadth was **worse**: sector −1%, housing −1.1% | **Correct sleeve, under-trusted.** |
| **S3 0** | No same-day flow spike | No evidence a flow shock hit 8/27 | **Fine.** |
| **S4 0** | “1d negative, defensive bid fading” | Tape was the best 1-day signal and it **kept going** | **Correct score, wrong use.** Factors were allowed to override a red 1d tape. |

Direction **MISS**. Magnitude band **mild** was the right *size* if they had been short/flat; they had the sign wrong. Confidence 0.5 was honest; the **up** lean was not.

Lessons they cited and then mis-applied:
- **08-25 live-curve / oil-slide:** correctly says don’t force *down* when yields are easing on an oil slide. They used it to force **up** off a **stale prior-close** curve. Session oil and yields reversed.
- **08-17 live-rate reversal:** they said “yields are NOT rising, trigger NOT firing.” That was true at **prior close**, false **into the close**. This is the miss.
- **08-21 level-vs-change:** they named 30Y 5.17% as still-elevated, then still called **up**. A −6 bp dip at a 19-year-high zone is not duration relief.
- **08-18 relative bid = magnitude cap:** 1w rel +0.61% was already fading (1d rel −0.62%). That caps **up**, it does not authorize **up**.
- **08-12 two-sided CPI/duration:** core in-line vs headline hot. They netted this to a mild positive. Sticky 3.7/3.3 into Warsh was two-sided **at best**, not a REIT bid.

---

## 3. Interactions / double-count / knowable-at-open

**Double-count:** “real yields falling” (S1) and “easing rate tape / oil-slide easing-inflation” (S0) are **one shock**. Scoring both +1 manufactured a +2 leading sum from a single prior-close snapshot. When oil and yields reversed together, both sleeves failed together — that is the tell.

**Not independent:** green ES/NQ was counted as supportive macro. For XLRE it was **equity beta**, and the beta that actually printed was **XLK**, not duration. Risk-on ≠ REIT bid.

**Structural padding:** data-center vacancy and PLD occupancy cannot justify a same-day **up**. They are always-on positives until they aren’t.

**Knowable at open:** **partially.**
- Knowable: PCE already out; 3.7/3.3 sticky; Collins hike-risk; Warsh Friday; 30Y still ~5.17%; XLRE 1d already −0.62% rel and 1m −5.40% rel; no breadth.
- Not knowable: exact +3 bp 10Y / +2 bp 30Y backup, oil’s −1.1% → +1.8% reversal, XLK-led SPY +0.66%.

A flat/down-mild call was available from **open-known** facts. The **up** call required the easing snapshot to **persist**. It didn’t.

---

## 4. Outliers inside the sector

This was **broad rate-proxy weakness**, not one ticker.

- **Data centers (held / green):** EQIX ~**+0.57%**, DLR ~**+0.58%** on the 8/27 tape — the only large XLRE sleeve that behaved like growth/AI rather than duration.
- **Healthcare leader did not save the ETF:** WELL closed **$239.60, −0.81%**. Morning’s “WELL carrying XLRE” failed as a same-day shield.
- **Industrial:** PLD close **$141.85, −0.52%** — not a crash, not a bid.
- **Towers / duration:** AMT ~**−1.59%** midday/session weakness; CCI ~**−1.0%**.
- **Office (worse than the ETF):** BXP **−2.54%**, HPP **−4.16%** on the 8/27 REITNotes tape — stressed sleeve, not the whole −95 bp.
- **Housing complex:** housing index **−1.1%** with AMH/INVH/ESS also red.

Single-name rule holds: EQIX/DLR cannot define XLRE, and WELL didn’t.

CLAIM: WELL closed $239.60, −0.81% on 2026-08-27.
URL: https://stockscan.io/stocks/WELL/price-history
PUBLISHED: 2026-08-27
QUOTE: Close $239.60, down $1.96 (−0.81%) from $241.56; range $237.22–$240.63
SUMMARY: Largest XLRE weight did not offset the duration dump.

CLAIM: PLD closed $141.85, about −0.52%.
URL: https://stockinvest.us/stock-price/PLD
PUBLISHED: 2026-08-27
QUOTE: Open $141.46 / high $142.87 / low $141.02 / close $141.85
SUMMARY: Industrial leadership was not a same-day positive.

---

OUTCOME_BEGIN
SECTOR: Real Estate
ETF: XLRE
ETF_PCT: -0.9536
SPY_PCT: 0.6553
REL_PCT: -1.6089
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Intraday long-end backup (10Y +3 bp to 4.67%, 30Y +2 bp to 5.19%) plus oil reversal, into Warsh Friday — duration dump, not SPX beta.
KEY_INTERACTION: Morning double-counted one stale easing-yield/oil-slide shock in S0 and S1; both reversed together while SPY rallied on XLK.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Up/mild overrode a red 1d tape and a still-stressed 30Y; 08-17 live-curve check was applied to prior close, not the session.
OUTCOME_END

---

## RESEARCH APPENDIX

**Queries run**
- web_search: `XLRE real estate ETF August 27 2026`
- web_search: `XLRE vs SPY August 27 2026 REIT rates yields`
- web_search: `Warsh Jackson Hole speech August 28 2026 Fed`
- web_search: `US 10 year Treasury yield August 27 2026`
- web_search: `July 2026 PCE inflation core 3.3 headline 3.7 Collins Fed`
- web_search: `REIT sector performance August 27 2026 WELL PLD EQIX AMT office`
- web_search: `housing starts July 2026 1.427 million industrial production`
- web_search: `crude oil WTI August 27 2026 price change`
- web_search: `XLK XLU XLRE sector performance August 27 2026`
- web_search: `site:fred.stlouisfed.org DGS10 4.67 August 27 2026`
- web_search: `Benzinga leading lagging sectors August 27 2026 XLRE`
- web_search: `Welltower WELL stock August 27 2026`
- web_search: `Prologis PLD August 27 2026 close`
- web_search: `Kevin Warsh Jackson Hole August 27 2026 bond yields REIT`
- x_search: `XLRE REIT real estate stocks August 27 2026 rates yields Fed` (2026-08-27 to 2026-08-28)
- x_search: `Treasury yields 10-year August 27 2026 close REIT underperform` (2026-08-27 to 2026-08-28)
- web_fetch: Hoya Capital status 2093070764157092081; BEA July 2026 PCE; REITNotes movers; FRED DGS10 (timeout); Reuters/Axios (blocked)

**Key sources and facts used**
1. **Hoya Capital Daily REITBeat (late edition)** — https://x.com/HoyaCapital/status/2093070764157092081 — 2026-08-27 20:18 ET — S&P +0.7%; equity REITs −1.0%; housing −1.1%; WTI $83.67 (+1.8%); 2Y 4.23% (+2 bp); 10Y 4.67% (+3 bp); 30Y 5.19% (+2 bp).
2. **BEA Personal Income and Outlays, July 2026** — https://www.bea.gov/news/2026/personal-income-and-outlays-july-2026 — released 2026-08-26 — PCE +3.7% YoY, core +3.3% YoY; both +0.2% MoM.
3. **Deterministic actuals (injected)** — XLRE −0.9536% (44.79 → 44.66); SPY +0.6553%; rel −1.6089%.
4. **Stocknear XLRE history** — https://stocknear.com/etf/XLRE/history — close ~$44.64–$44.66, −0.95% to −1.0% vs $45.09 prior close.
5. **Axios Warsh preview** — https://www.axios.com/2026/08/27/warsh-guidance-jackson-hole — 2026-08-27 — Jackson Hole keynote 2026-08-28 ~10am ET; markets wanting inflation/reaction-function clarity.
6. **GuruFocus / FRED DGS10** — https://www.gurufocus.com/economic_indicators/37/10-year-treasury-yield ; https://fred.stlouisfed.org/series/DGS10 — market 10Y ~4.67% on 8/27; FRED official 8/27 not posted yet (last obs 2026-08-26: 4.66).
7. **Benzinga sector leaders/laggards** — https://www.benzinga.com/etfs/sector-etfs/26/08/61461100/leading-and-lagging-sectors-august-27-2026 — 2026-08-27 — XLK lead, XLRE among laggards.
8. **WELL** — https://stockscan.io/stocks/WELL/price-history — close $239.60, −0.81%.
9. **PLD** — https://stockinvest.us/stock-price/PLD — close $141.85, ~−0.52%.
10. **REITNotes movers** — https://www.reitnotes.com/reit-movers/ — 2026-08-27 ~12:56 ET delayed tape — AMT −1.59%, EQIX +0.57%, DLR +0.58%, BXP −2.54%, HPP −4.16% (midday, used only as sleeve dispersion, not as official closes).
11. **Reuters Collins (search snippet)** — https://www.reuters.com/business/feds-collins-says-latest-us-inflation-readings-are-mixed-2026-08-27/ — 2026-08-27 — Collins called July inflation “mixed”; hike risk still live. Page fetch blocked; used as secondary confirmation of the morning’s hawkish-Collins item, not as a new print.

**Not used as session facts:** Census housing-starts 1.427M (that is June, not an 8/27 catalyst); Benzinga’s shallower XLRE −0.41% print (intraday, inconsistent with close).