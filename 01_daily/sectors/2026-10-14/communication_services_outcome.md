# Sector Outcome — Communication Services — 2026-10-14

Actuals: {'etf': 'XLC', 'pct': -1.3003198257443005, 'spy_pct': -1.0809673492788385, 'rel': -0.21935247646546197, 'open': 112.36000061035156, 'close': 110.81999969482422, 'source': 'yf_download'}

I have the CPI print, cash path, and mega-cap split. Writing the autopsy against the morning card as it was scored, not as rewritten after the close.

## 0. Facts

XLC **−1.30%** (open **112.36** → close **110.82**). Prior close ~**112.28**, so the cash open was essentially unchanged (**+0.07%**), then the ETF sold to a ~**110.41–112.58** range. SPY **−1.08%**. Relative **−0.22%**. Path: pre-market/open matched the morning PM tape (**XLC PM +0.01%**); the notable loss was an **in-session grind**, not a gap-down.

GICS communication services **−1.78%**; S&P 500 **−1.11%** (7,718.94); Nasdaq **−1.88%** (26,692.87); information technology **−2.6%**. Eight of 11 sectors down.

**CLAIM:** September CPI, released 8:30 a.m. ET on 2026-10-14, was hot on core and in-line-to-hot on headline.  
**URL:** https://www.cnbc.com/2026/10/14/cpi-inflation-report-september-2026.html  
**PUBLISHED:** 2026-10-14  
**QUOTE:** “rose a seasonally adjusted 0.6% for the month, putting the 12-month rate at 3.7%… Economists surveyed by Dow Jones expected respective gains of 0.6% and 3.6%. … core CPI rose 0.3% for the month and was up 2.6% … estimates for 0.2% and 2.5%.”  
**SUMMARY:** Headline 0.6%/3.7% vs 0.6%/3.6%; core 0.3%/2.6% vs 0.2%/2.5%. Energy +5.2%, gasoline +9.1% (~half the headline gain). Shelter +0.4%. FedWatch: Oct 27–28 hike ~54%; year-end hike >96%; ~50/50 for 50 bp by December.

**CLAIM:** Equities sold on the print; communication services was the second-weakest sector.  
**URL:** https://www.straitstimes.com/business/companies-markets/nasdaq-ends-sharply-lower-as-hot-inflation-data-stirs-rate-hike-worries  
**PUBLISHED:** 2026-10-14 (Reuters)  
**QUOTE:** “The S&P 500 declined 1.11% … Nasdaq declined 1.88% … Eight of the 11 S&P 500 sector indexes declined, led lower by information technology, down 2.6%, followed by a 1.78% loss in communication services.”  
**SUMMARY:** Duration/AI-capex complex led. Oct hike odds 22% → 37% (CME, this write-up). Heavy volume.

Direction **down**. Magnitude **notable** (|1.30%|), not severe. Relative to SPY, almost a beta tape (only **−22 bp**).

---

## 1. What drove the sector

Taxonomy: **real yields / hawkish CPI (S0 duration)**, transmitted through the **two-name book**, not a communication-services idiosyncratic factor.

No fresh ad-spend print, no AI-monetization proof, no antitrust/app-store/content shock. Gemini 4 Argon / Muse Wonder stayed carried narrative, as the morning S1 judgment said. The hit was **higher-for-longer**: core beat, hike odds up, growth/AI spenders de-rated. META and Alphabet are ~46% of XLC (META ~24.3%, GOOGL ~12.5%, GOOG ~10.0% as of 2026-10-13 on SSGA). META **−3.27%** ($737.54), GOOGL **−1.74%** ($347.64). That is the ETF.

XLC **outperformed Nasdaq and XLK**, **underperformed SPY only modestly**. This was a **growth-duration day**, not a comms-specific accident.

---

## 2. Audit of morning S0–S4 (morning numbers, not rewritten)

Morning call: **flat / flat**, total **−0.686**, conf **~0.43**, regime mixed, **divergence_flagged True**, calendar size gate on, sector RS veto on.

| Sleeve | Morning | Reality | Verdict |
|---|---|---|---|
| **S0 −0.5** | Pre-binary duration tax; rates consolidating (10Y 5.24%, −4 bp week); dollar/oil easing at the margin; CPI “after the session” | CPI **in this cash session** (8:30 ET). Core hot. Yields jumped (~5.20s → ~5.30%). Hike odds repriced. Growth book sold. | **Sign right, size and timing wrong.** The −0.5 assumed a consolidating pre-print tape and a binary that did not span the call. The binary **did** span the call. |
| **S1 0** | No ad-spend, no monetization proof; Gemini/Muse carried; AMX/JPM single-name | Confirmed. Nothing sector-idiosyncratic printed. META/GOOGL down is S0 through weights, not S1. | **Correct.** |
| **S2 0** | NQ −0.06% vs ES +0.18% “inside noise”; XLK −0.26% vs XLC +0.01% | Post-print: Nasdaq −1.88%, IT −2.6%, comms −1.78%. Pre-open lag was the calm **before** CPI, not a breadth read for the session. | **Pre-binary snapshot, not a session read.** Not false at 6 a.m.; useless after 8:30. |
| **S3 +0.5** | Weekly inflows all 11 sectors; XLC not crowded (1m rel −4.61%) | Inflows did not buffer a CPI shock. Positioning was not the driver. | **Not disproven as positioning; swamped.** Should not have offset S0 on a CPI morning. |
| **S4 +0.5** | PM +0.01%; 1d/1w rel +0.76/+0.70; confirmation-only | Open **did** match PM. Then cash sold. Confirmation sleeve was asked to **offset** a known binary. | **Role-correct, weight-wrong.** +0.5 against S0 on a CPI day is how a −0.5 macro read becomes a flat call. |

**Lessons as applied this morning:**

- **10-01/10-02 (rates once, no direction off index beta):** Followed. Rates not restacked into S1. `index_carry −0.411` / `general_total −1.645` did **not** mint down. That discipline is why a negative engine score still printed **flat**. On a **same-session CPI**, S0 **is** the sector driver for this duration book — not “index beta from a near-zero card.”
- **09-25 pre-binary asymmetric-downside, capped:** The cap was the miss. Oil offered and pre-print yields/dollar calm do not cancel an **8:30 CPI** for XLC. Asymmetric downside was live; it was muted because the **pre-print** tape looked fine.
- **10-13 down card stale:** Fair. Prior-day +0.74% / +0.76% rel was real. It did not forecast CPI day.
- **09-10/09-09 two-name book:** S1 correctly refused to monetize Gemini/Muse. The same two-name structure **did** transmit the CPI shock via META.

**Calendar error (process, not hindsight on the print):** The card said “Tonight’s CPI” and “the CPI binary resolves **after** the session.” September CPI was **08:30 ET on the prediction date**. From Asia morning that is “tonight” on the clock, but it is **before the US cash open**, not after the close. `calendar_size_gate` capped **magnitude** to flat; it should have also stopped treating the session as a consolidation day. The print (hot vs in-line) was unknown. The **clock** was known.

Engine already leaned negative (`overlay −0.425`, `total −0.686`, divergence true). Human/LLM overlay plus “when score sign fights tape, prefer flat/mild” plus RS veto chose the **pre-CPI tape** over the **known binary**. Tape won; tape was stale by 9:30.

---

## 3. Interactions / double-count / knowable-at-open

**Not double-counted:** CPI is one object (S0). META/GOOGL are the **mechanism**, not a second factor. Do not add “AI narrative failed” on top — there was no same-morning monetization test.

**False offsets (the actual error):** S3 inflows + S4 stabilizing rel tape were used to neutralize S0 on a day whose whole point was the CPI binary. That is stacking **pre-print consolations** against a **scheduled macro event**.

**Knowable at open:**
- Knowable: CPI prints 8:30 ET **this** session; XLC is a duration/two-name growth book; pre-print S0 was already −0.5; engine score already negative.
- Not knowable: core 0.3 vs 0.2, hike-odds jump, META −3.3%.
- **Partially.** A flat/flat **because the binary is after the close** fails the knowable-at-open test. A flat/flat **because the print is unknown, size gated, no S1** is defensible — that is not the rationale the card wrote.

---

## 4. Outliers inside the sector

**Downside:** **META −3.27%** — largest weight, duration/AI-capex proxy, the XLC story. GOOGL **−1.74%** in-line to slightly worse than the ETF.

**Upside / relative winners:** **NFLX +0.16%** (close 70.57); **DIS +0.38%** in secondary prints. Entertainment held; interactive media did not. That split is why XLC (−1.30%) was **less bad** than the GICS comms index (−1.78%) and much less bad than XLK (−2.6%): the book is not a pure Nasdaq clone, but META is still large enough to force a notable red day.

No regulatory or ad-spend outlier. No single-name telecom upgrade (AMX/JPM) visible in the ETF.

---

OUTCOME_BEGIN
SECTOR: Communication Services
ETF: XLC
ETF_PCT: -1.3003
SPY_PCT: -1.0810
REL_PCT: -0.2194
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Hot September CPI (core 0.3%/2.6% vs 0.2%/2.5%) repriced hike odds and yields; META/GOOGL duration dump, not ads/AI/regulation.
KEY_INTERACTION: Same-session CPI (S0) × two-name book (~46% META+Alphabet) — one rates object, not a second S1; S3/S4 pre-print offsets should not have neutralized it.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Flat/flat missed a same-session CPI duration hit; S0 sign was right, but CPI was timed as post-close and 09-25 downside was capped by a calm pre-print tape.
OUTCOME_END

---

## RESEARCH APPENDIX

Memory search was paused (index built with a different embedding provider). No MEMORY.md hits used. Workspace has no XLC/comms daily files.

**Queries run**
- Web: `XLC Communication Services ETF October 14 2026 performance CPI`
- Web: `CPI inflation September 2026 released October 14 stock market reaction`
- Web: `GOOGL META NFLX DIS XLC stocks October 14 2026`
- Web: `XLC holdings META Alphabet weight Communication Services Select Sector`
- Web: `September 2026 CPI BLS core 0.3 gasoline 9.1 site:bls.gov`
- Web: `US 10-year Treasury yield October 14 2026 CPI reaction`
- Web: `Nasdaq S&P 500 close October 14 2026 CPI`
- Web: `TMUS T VZ CMCSA EA TTWO stock performance October 14 2026` *(discarded: mixed Oct 13 index levels into Oct 14 names)*
- Web: `META stock close October 14 2026 -3.27`
- Web: `GOOGL closed October 14 2026 347.64`
- Web: `site:ssga.com XLC holdings META Alphabet`
- X: `What happened to XLC, GOOGL, META, communication services stocks on October 14 2026 after CPI?` (2026-10-14 to 2026-10-15)
- X: `September CPI 3.7% core 2.6% October 14 2026 market reaction yields META GOOGL` (2026-10-14 to 2026-10-15)

**Fetches**
- https://www.cnbc.com/2026/10/14/cpi-inflation-report-september-2026.html — **used** (CPI details, FedWatch, energy/gasoline)
- https://www.bls.gov/news.release/cpi.nr0.htm — **403 bot block**; CPI figures taken from CNBC citing BLS
- https://www.reuters.com/business/view-hot-september-cpi-sends-rate-hike-bets-soaring-2026-10-14/ — **401**
- https://www.cnn.com/2026/10/14/economy/us-cpi-inflation-september — title only
- https://chartexchange.com/symbol/nyse-xlc/ — prior close $112.28; session down ~1.3–1.4%; volume ~6.35M
- https://www.straitstimes.com/business/companies-markets/nasdaq-ends-sharply-lower-as-hot-inflation-data-stirs-rate-hike-worries — **used** (SPX/Nasdaq closes, IT −2.6%, comms −1.78%)
- https://stockanalysis.com/stocks/nflx/history/ — NFLX 70.57, +0.16% at close Oct 14
- https://upstox.com/...article-201861/ — boilerplate only, no usable tape

**Facts taken**
- **CNBC:** CPI 0.6%/3.7% vs 0.6%/3.6%; core 0.3%/2.6% vs 0.2%/2.5%; energy +5.2%; gasoline +9.1%; Oct hike ~54%; year-end hike >96%.
- **Strait Times/Reuters:** SPX 7,718.94 −1.11%; Nasdaq 26,692.87 −1.88%; Dow −0.09%; IT −2.6%; communication services −1.78%; Oct hike odds 22%→37% in that article’s CME read.
- **Deterministic channel:** XLC −1.3003%, SPY −1.0810%, rel −0.2194%, open 112.36, close 110.82.
- **ChartExchange:** prior close $112.28 (open was a tiny green vs prior close).
- **SSGA (search, as of 2026-10-13):** META 24.28%, GOOGL 12.47%, GOOG 9.98%.
- **TECHi/MarketWatch (search):** META 737.54 −3.27%; GOOGL 347.64 −1.74%.
- **stockanalysis:** NFLX +0.16%.

**Not used as evidence:** TMUS/T/VZ/CMCSA prints from the mixed-date search; Reuters/Bloomberg yield pages that 401’d; X posts except as color (META −3.3% consistent with TECHi).