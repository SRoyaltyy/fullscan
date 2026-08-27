# Sector Outcome — Utilities — 2026-08-27

Actuals: {'etf': 'XLU', 'pct': -0.7584418043843133, 'spy_pct': 0.6552786111251541, 'rel': -1.4137204155094674, 'open': 43.220001220703125, 'close': 43.18000030517578}

Memory search is paused (index metadata missing; `openclaw memory status --index` or `openclaw memory index --force` would rebuild it). This review uses the injected morning note plus live sources.

## 0. Facts

Trusted Channel 1 actuals for **2026-08-27**:

| | |
|---|---|
| XLU | **−0.758%** (open 43.22 → close 43.18) |
| SPY | **+0.655%** |
| Relative | **−1.414%** |

**Path:** Almost all of the absolute loss was overnight/gap. ChartExchange shows premarket **43.25 (−0.60%)**, cash high ~43.33 / low ~42.92, close **43.16 (−0.80% / −0.35)** vs prior **43.51**. Deterministic open→close was only **−0.09%**. This was a gap-down, stay-heavy defensive session, not a late-day crash.

**Actual vs morning:** predicted **up / notable** (score 7.5). Realized **down / mild** absolutely, **notable** relative lag vs SPY. Direction miss; magnitude miss.

---

## 1. What drove the sector

**Primary (taxonomy: risk-on rotation away / growth-led tape):** After the **26 Aug close**, Nvidia printed a blowout Q2 and guided Q3 to **$108B ±2%**. On **27 Aug** NVDA ripped ~8–9% and pulled a **narrow, tech-led risk-on** tape (Nasdaq ~+1.6%, SPY +0.66%). Defensives were the funding side. That is the classic **S1 “risk-on rotation away”** factor the morning said was *not firing*.

**Secondary (taxonomy: rates / bond-proxy):** The duration bid was **dead on the day**, not live. 10Y ~**4.67%**, 30Y ~**5.19%** — little changed, slightly *up* from the morning’s 8/25 FRED 4.64 / 5.17. Jackson Hole day-one chatter (sticky inflation, Warsh speech still Friday) kept a hawkish overlay without a large yield spike. Utilities did not get a bond-proxy bid; they got a **relative dump into growth**.

**Not the driver:** July PCE. BEA released it **26 Aug**, not 27 Aug. Headline **+0.2% m/m / +3.7% y/y** (a touch hot); core **+0.2% / +3.3%** (in line). That print was already in the 26 Aug tape (XLU +0.46% that day). Treating PCE as *today’s* load-bearing catalyst was a calendar error.

**Breadth:** Sector-wide, not a single-name smash. CSIMarket-style reads had ~**84%** of utilities down; Fidelity sector ~**−0.75%**. Large XLU names: NEE ~−0.9%, DUK ~−0.8%, SO ~−1.15%, AEP ~−0.4%, SRE ~−0.5%.

---

## 2. Morning S0–S4 vs reality (morning numbers, not rewritten)

Morning scores: **S0 +1, S1 +1, S2 +1, S3 0, S4 +1** → pipeline **up / notable**.

| Sleeve | Morning read | 27 Aug reality | Verdict |
|---|---|---|---|
| **S0 shared macro** | Confirmed easing + cool-PCE positioning; PCE “due today,” two-sided but scored +1 | PCE was **yesterday** and slightly hot on headline; yields **not easing** on 27 Aug; NVDA was the actual shared-macro shock and it was **growth-positive / utilities-negative** | **Wrong sign** |
| **S1 sector factors** | Bond-proxy HIT (10Y/30Y/real −6 bp); “risk-on rotation away: not firing”; AI-power stale | Bond-proxy did **not** HIT on the session. Risk-on rotation **did** fire. No fresh rate-case/load catalyst | **Wrong sign** |
| **S2 breadth** | +1 on 1d rel +0.44% / 3d rel +1.68% “inflection” | That was **prior tape**. 27 Aug breadth was **negative and broad** | **Stale confirmation** |
| **S3 flows** | 0 (carried 5d/1m outflows, no same-day spike) | Still no evidence of a same-day inflow rescue | **OK** |
| **S4 ETF tape** | +1 on 1d/3d; 1w/1m still ugly (rel −0.77% / −7.82%) | Using yesterday’s green print as today’s tape. Premarket already **−0.6%**. 1m underperformance was the live regime | **Stale / too bullish** |

**Lesson audit (as written in the morning, not hindsight-rescoring):**

- **08-11** (don’t keep calling down when yields ease and 1d/3d inflect): applied, and it **overfit two green days**. The driver had already stopped easing after 26 Aug PCE.
- **08-12** (cap to mild on risk-on tech-led + sector headwind): morning said it **did not fire** because archived premarket was only ES +0.31% / NQ +0.55%. **This was the load-bearing miss.** NVDA was out before the cash open; the tape *was* strong tech-led. Even if direction stayed up, magnitude should have been capped — and direction should have flipped or gone flat/relative-only.
- **08-14** (scan 8:30 ET high-impact): they scanned the **wrong day**. PCE was 26 Aug.
- **08-17** (carried defensive bid with no fresh catalyst = relative, not absolute): ignored. There was **no fresh utilities catalyst** on 27 Aug.
- **08-18** (risk-off + rising yields → rel beat / flat-to-neg absolute): correctly noted as not the setup. The **mirror** was the live one: risk-on + non-falling yields → **absolute and relative lag**.
- **08-21** (don’t score easing off stale FRED): they claimed live confirmation off 8/25 prints + “this morning ~4.64/5.17.” By 27 Aug cash, 10Y was ~4.67% and 30Y ~5.19%. The easing regime was **stalling**.
- **08-25** (don’t manufacture down from carried negatives if S0/S1 are neutral): they used this to **upgrade** S0/S1 to +1. Circular. S0/S1 were only “not neutral” because of the same disputed easing narrative.

Pipeline vs analyst: the write-up wanted **mild**; the deterministic pipeline printed **notable**. Both still had the **wrong direction**.

---

## 3. Interactions / double-count / knowable-at-open

**Double-count:** S0 +1 and S1 +1 were the **same duration-easing shock** counted twice (macro sleeve + bond-proxy sleeve). Morning self-audit dismissed this; the session showed why it matters — when that one factor is stale, the whole +7.5 collapses.

**Not independent:** 1d/3d tape (S2/S4) was the *result* of that same 8/25–8/26 easing, not a new confirming impulse.

**Knowable at the 27 Aug open (yes):**
- NVDA results/guide were **already public** (26 Aug after-close).
- July PCE was **already public** (26 Aug 8:30 ET) — not a same-day binary.
- XLU premarket already **~−0.6%**.
- 10Y had **stopped falling**.
- 08-12 setup (tech-led risk-on vs bond-proxy) was visible if premarket was refreshed after NVDA.

**Not knowable:** Warsh Friday speech; whether the gap would fill; exact Nasdaq close magnitude.

The up/notable call fails the knowable-at-open test. A competent open read was **down or flat absolutely, lag vs SPY**, magnitude **mild**, with NVDA as the S0 shock and rotation-away as S1.

---

## 4. Outliers inside the sector

- **Broad, not idiosyncratic.** No AEP-style single name drove XLU. AEP (~−0.4%) was actually **better** than the ETF — the carried PT-cut did not explain the day (morning was right to ignore it).
- **SO** (~−1.1% to −1.2%) was the soft large-cap; **NEE** tracked the ETF.
- Market-level outlier was **NVDA (~+8–9%)**, which lifted cap-weighted SPY while equal-weight/defensives lagged. That is SPX composition, not a utilities-fundamental event.
- AI-power / data-center narrative stayed **stale** on a 1d horizon, as the morning correctly said — and then failed to keep it from being crowded out by the rate/rotation lens.

---

## Evidence

CLAIM: July PCE was released 26 Aug 2026, not 27 Aug.  
URL: https://www.bea.gov/news/2026/personal-income-and-outlays-july-2026  
PUBLISHED: 2026-08-26  
QUOTE: “From the preceding month, the PCE price index for July increased 0.2 percent. Excluding food and energy, the PCE price index also increased 0.2 percent.” / “From the same month one year ago, the PCE price index for July increased 3.7 percent. Excluding food and energy, the PCE price index increased 3.3 percent.”  
SUMMARY: Official BEA print; current-releases page lists the same date.

CLAIM: Headline PCE was slightly hot vs consensus; core in line; yields/futures reacted on Wednesday.  
URL: https://www.cnbc.com/2026/08/26/feds-preferred-inflation-gauge-shows-core-prices-rose-3point3percent-annually-in-july.html  
PUBLISHED: 2026-08-26  
QUOTE: “increased a seasonally adjusted 0.2% for the month, putting the annual inflation rate at 3.7% … Both were 0.1 percentage point above the Dow Jones consensus.” / “core PCE posted respective gains of 0.2% and 3.3%, in line with forecasts.” / “Stock market futures pulled back a bit after the report while Treasury yields were higher.”  
SUMMARY: Sticky/hot-enough headline, not the cool print the morning positioned for.

CLAIM: 27 Aug yields were little changed, slightly above the morning’s 8/25 4.64/5.17.  
URL: https://www.cnbc.com/2026/08/27/us-bonds-us10y-jackson-hold.html  
PUBLISHED: 2026-08-27  
QUOTE: “The benchmark 10-year Treasury note was up less than 1 basis point at 4.672%. The 30-year Treasury bond yield was less than 1 basis point higher as well at 5.19%.”  
SUMMARY: No same-day easing impulse; hawkish Jackson Hole overlay, Warsh still Friday.

CLAIM: Nvidia Q2 FY27 was the 26 Aug after-close shock that re-rated growth on 27 Aug.  
URL: https://nvidianews.nvidia.com/news/nvidia-announces-financial-results-for-second-quarter-fiscal-2027  
PUBLISHED: 2026-08-26 (call 2 p.m. PT / 5 p.m. ET)  
QUOTE: “revenue for the second quarter ended July 26, 2026, of $96.2 billion, up 18% from the previous quarter and up 106% from a year ago.” / “Revenue is expected to be $108.0 billion, plus or minus 2%.”  
SUMMARY: Knowable before 27 Aug cash open; this is the S0 factor the morning omitted.

CLAIM: XLU cash session was a gap-down stay-weak day.  
URL: https://chartexchange.com/symbol/nyse-xlu/historical/  
PUBLISHED: 2026-08-27 session  
QUOTE: “At Close Aug 27, 2026 … 43.16 USD −0.804% (−0.35)” / “Pre-market … 43.25 USD −0.598% (−0.26)”  
SUMMARY: Matches Channel 1 direction; vendor close 43.16 vs deterministic 43.18.

CLAIM: Dollar/Fed-hike odds still reflected Wednesday’s slightly hot inflation into Thursday.  
URL: https://www.cnbc.com/2026/08/27/dollar-near-eight-day-high-as-us-data-lifts-fed-hike-bets.html  
PUBLISHED: 2026-08-27  
QUOTE: “Data on Wednesday showed inflation rose more than expected in July, boosting expectations that interest rates could stay restrictive … briefly lifting expectations for a September rate hike … to over 40%. But expectations … slipped back to 34.1% on Thursday.”  
SUMMARY: Residual hawkish PCE overlay, not a new 8:30 shock on 27 Aug.

---

OUTCOME_BEGIN
SECTOR: Utilities
ETF: XLU
ETF_PCT: -0.758
SPY_PCT: 0.655
REL_PCT: -1.414
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Nvidia-led tech risk-on rotation out of defensives; duration bid absent
KEY_INTERACTION: Wed PCE already slightly hot and yields stalled, so the bond-proxy +1/+1 stacked on a dead factor just as NVDA (knowable at open) pulled SPY up
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Up/notable failed: PCE was yesterday, 08-12 tech-led rotation did fire, and S0/S1 double-counted stale easing
OUTCOME_END

---

## RESEARCH APPENDIX

**Queries run**
- Web: `XLU utilities ETF August 27 2026 performance PCE`
- Web: `July 2026 core PCE release August 27 2026 result utilities stocks`
- Web: `US 10-year Treasury yield August 27 2026 close`
- Web: `SPY XLU August 27 2026 close percentage change`
- Web: `site:reuters.com PCE July 2026 August 26 inflation sticky`
- Web: `utilities sector August 27 2026 XLU underperform Nasdaq risk-on`
- Web: `Personal Income and Outlays July 2026 BEA release date August`
- Web: `XLU holdings NEE DUK SO AEP SRE August 27 2026 stock performance`
- Web: `10 year 30 year Treasury yield August 27 2026 DGS10 DGS30`
- Web: `Nvidia earnings August 2026 stock market rally August 27`
- Web: `Jackson Hole 2026 August 27 Fed Powell utilities bonds`
- Web: `"utilities" "August 27" 2026 sector lag OR underperform OR defensive`
- Web: `XLU August 26 2026 close price 43.51`
- Web: `S&P 500 utilities sector August 27 2026 percentage change breadth`
- X: `XLU utilities ETF August 27 2026 PCE reaction yields` (2026-08-27 to 2026-08-28)
- X: `Nvidia earnings rally August 27 2026 Nasdaq utilities lag XLU` (2026-08-27 to 2026-08-28)
- Fetches: BEA July 2026 PIO; BEA current releases; ChartExchange XLU; CNBC 10Y/Jackson Hole; CNBC PCE; CNBC dollar; Nvidia IR Q2 FY27
- Failed/blocked fetches: Reuters (JS/401), FRED DGS10 (timeout), Yahoo indexes article, CSIMarket (429), stockanalysis NEE/SO (403)
- Memory: `memory_search` unavailable (index metadata missing)

**Key sources and facts taken**

| Source | URL | Timestamp | Facts used |
|---|---|---|---|
| BEA Personal Income and Outlays, July 2026 | https://www.bea.gov/news/2026/personal-income-and-outlays-july-2026 | Release 2026-08-26; fetched 2026-08-27T21:55:48Z | PCE +0.2% m/m, core +0.2%; y/y 3.7% / 3.3%; income +0.4%, PCE spending +0.2% |
| BEA current releases | https://www.bea.gov/news/current-releases | Fetched 2026-08-27T21:56:51Z | Confirms PIO dated **August 26, 2026** (not the 27th) |
| CNBC PCE recap | https://www.cnbc.com/2026/08/26/feds-preferred-inflation-gauge-shows-core-prices-rose-3point3percent-annually-in-july.html | 2026-08-26; fetched 2026-08-27T21:49:35Z | Headline 0.1 ppt above DJ consensus; core in line; yields up / futures down on print |
| CNBC Treasuries 27 Aug | https://www.cnbc.com/2026/08/27/us-bonds-us10y-jackson-hold.html | 2026-08-27; fetched 2026-08-27T21:56:51Z | 10Y 4.672%, 30Y 5.19%, 2Y 4.232%; little changed; Jackson Hole / Warsh Friday |
| CNBC dollar / FedWatch | https://www.cnbc.com/2026/08/27/dollar-near-eight-day-high-as-us-data-lifts-fed-hike-bets.html | 2026-08-27; fetched 2026-08-27T21:59:17Z | Wed inflation lifted Sep hike odds >40% then back to 34.1% Thu; claims 203k vs 208k |
| Nvidia IR | https://nvidianews.nvidia.com/news/nvidia-announces-financial-results-for-second-quarter-fiscal-2027 | 2026-08-26 2 p.m. PT; fetched 2026-08-27T21:58:04Z | Rev $96.2B; DC $89.0B; Q3 outlook $108B ±2% |
| ChartExchange XLU | https://chartexchange.com/symbol/nyse-xlu/historical/ | 2026-08-27 15:59 ET; fetched 2026-08-27T21:56:52Z | Close 43.16 −0.80%; premarket 43.25 −0.60%; vol ~16.0M |
| Channel 1 actuals (injected) | pipeline | 2026-08-27 | XLU −0.758%, SPY +0.655%, rel −1.414%, O/C 43.22/43.18 |
| Search/X secondary (NVDA tape, names, breadth) | Reuters/WSJ/Yahoo/CSIMarket/X posts cited in search | 2026-08-27 | NVDA ~+8–9%; Nasdaq ~+1.6%; NEE/DUK/SO/AEP/SRE all red; ~84% of utilities down — used as corroboration, not as Channel 1 |

**Not used as primary:** Reuters HTML (blocked), FRED live series (timeout), CSIMarket body (429). Search snippets on those URLs were treated as secondary only.