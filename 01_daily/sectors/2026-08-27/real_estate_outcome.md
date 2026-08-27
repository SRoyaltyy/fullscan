# Sector Outcome — Real Estate — 2026-08-27

Actuals: {'etf': 'XLRE', 'pct': -0.9536489326250397, 'spy_pct': 0.6552786111251541, 'rel': -1.6089275437501938, 'open': 44.790000915527344, 'close': 44.65999984741211}

Memory search is paused (index metadata missing), so this review uses the injected morning note plus live sources only.

## 0. Facts
XLRE **−0.95%** (open 44.79 → close 44.66). SPY **+0.66%**. Relative **−1.61%**. Path: gap/open already soft vs prior close (~45.09 on 8/26), then a session-long duration/defensive flush while SPX was a one-sector Nvidia tape. Absolute ~−1% and relative ~−1.6% is **down / notable**, not the morning’s up/mild.

---

## 1. What drove the sector
Primary driver was a **live long-end backup into Warsh Friday**, not the prior-close easing the morning treated as the spine.

- Hoya’s close recap: equity REITs **−1.0%** vs S&P **+0.7%**; 10Y **4.67% (+3 bps)**, 30Y **5.19% (+2 bps)**, 2Y **4.23% (+2 bps)**. That is the REIT duration factor firing **against** the morning S0/S1 read.
- The “risk-on” tape was **not** duration-friendly beta. Nvidia’s 8/26 after-close print (rev $96.22B, DC $89B, bullish guide) produced a **tech-only** SPX: IT ~+3%, **10 of 11 sectors down**. XLRE was in the discarded sleeve, not the AI bid.
- Event risk stayed two-sided: Warsh still spoke Friday; Collins called July PCE “mixed,” policy “mildly restrictive,” and left a hike door open. That is not duration relief.
- Supply: $44B 7-year notes stopped at **4.512%** (highest since Dec 2024), modest tail — consistent with a still-stressed coupon market, not a refinancing window.
- Morning oil-slide/easing-inflation mechanism **did not persist**. Hoya had crude **$83.67, +1.8%**. The 08-25 “oil down = REIT-positive spine” mapping was stale by the close.

Taxonomy: **rates rising / REIT selloff** HIT; **real yields falling** MISS on the live day; **risk-on / equity beta expansion** MISS for this sector (narrow mega-cap, not REIT beta).

---

## 2. Audit of morning S0–S4 (use morning numbers, not a rewrite)

| Sleeve | Morning | Reality 8/27 | Verdict |
|---|---|---|---|
| **S0 +1** | Prior-close DFII10/DGS10/DGS30 all −6 bps; oil down; ES/NQ green; core PCE 3.3% in-line | Live 10Y/30Y **up**; oil **up**; SPX green only via NVDA; headline PCE 3.7% still sticky | **Too bullish.** Counted a *prior-close* easing snapshot as the live macro. 08-17 live-rate reversal was the actual tape. |
| **S1 +1** | Real-yield HIT + duration PARTIAL; DC/industrial structural HITs vs office/refi | Structural DC/industrial did not price today. Duration was the live factor and it **reversed**. Office is ~1% of XLRE — not the ETF. | **Over-scored.** Same-day REIT factor is the curve, not mid-year occupancy notes. |
| **S2 0** | 1d rel −0.62%, 1w rel +0.61%, large-cap/low-beta leadership | Breadth stayed narrow; housing −1.1%; VICI new 52w low | **Fair.** Did not pretend breadth was expanding. |
| **S3 0** | No same-day flow spike; prior 5d outflows | No flow rescue into the flush | **Fair.** |
| **S4 0** | Mixed confirmation; 1d already negative | Tape was the tell: 1d rel −0.62% into PCE, then −0.95% / −1.61% rel | **Under-weighted.** Negative 1d was confirmation the defensive bid was fading, not a 0. |

Morning also internally contradicted itself: narrative said Σ +1.8 → **up/flat** and flagged divergence, but the pipeline emitted **up/mild** with `divergence_flagged: False`. The published call was the optimistic one.

**Lessons that should have capped or flipped the call (and were only half-applied):**
- **08-17 live-rate reversal:** do not lock S0/S1 off *yesterday’s* −6 bp print. Today the curve went the other way.
- **08-21 level-vs-change:** 30Y still ~5.17–5.19% (19-year-high zone). A 6 bp dip is not duration relief. This was named, then overridden.
- **08-25 oil-slide/easing-inflation:** only valid if oil *stays* down and yields *stay* easier. Neither held.
- **08-18 relative bid is a mag cap:** 1w rel +0.61% was already fading (1d rel −0.62%). Using it as residual up-bias was the error.

---

## 3. Interactions / double-count / knowable-at-open

**Interaction (one shock, two sleeves):** Nvidia-only SPX strength **plus** a 2–3 bp long-end backup is the same rotation: capital into AI duration-insensitive names, out of bond proxies. Do not score “risk-on S0 +1” and “REIT duration S1 +1” as independent positives when the risk-on *is* the thing selling REITs.

**Double-count in the morning:** real-yield HIT and “rates falling / duration PARTIAL” were the same 6 bp move, counted twice into S0 and S1. That manufactured the +2 leading sum.

**Knowable at open: partially.**
- Known: sticky 3.7%/3.3% PCE, Collins hike optionality, Warsh Friday, 30Y still elevated, XLRE 1d already −0.62% rel, NVDA print already out.
- Not known: exact +3/+2 bp live backup, 7-year tail, oil reversing +1.8%, VICI 52w low.
- The miss was not “Warsh spoke hawkish” (he hadn’t). It was treating a **stale easing snapshot** as the live spine when the open already had a two-sided event and a fading 1d tape.

---

## 4. Outliers inside the sector
- **VICI** printed a fresh 52-week low (~$25.75) — long-lease/experiential duration, not office.
- **Housing index −1.1%** vs equity REITs −1.0%: rate beta, not a single-name XLRE story.
- **Office** remains structurally ugly but is ~1% of XLRE; it did not drive the ETF.
- **Data-center/AI REITs** did not offset the duration hit. NVDA strength stayed in semiconductors/software (CRM, CRWD), not EQIX/DLR enough to lift XLRE.
- Nareit North America **−0.92%**, FTSE EPRA Americas **−0.89%**: the flush was index-wide, not an XLRE idiosyncratic.

---

### Evidence

CLAIM: July PCE headline +3.7% YoY / core +3.3% YoY; m/m both +0.2%.  
URL: https://www.bea.gov/news/2026/personal-income-and-outlays-july-2026  
PUBLISHED: 2026-08-26  
QUOTE: “From the same month one year ago, the PCE price index for July increased 3.7 percent. Excluding food and energy, the PCE price index increased 3.3 percent.”  
SUMMARY: Official print matching the morning’s in-line core / slightly hot headline.

CLAIM: Core PCE in-line, headline 0.1 ppt hot; yields were already elevated near 2007 highs into Jackson Hole.  
URL: https://www.cnbc.com/2026/08/26/feds-preferred-inflation-gauge-shows-core-prices-rose-3point3percent-annually-in-july.html  
PUBLISHED: 2026-08-26  
QUOTE: “Both were 0.1 percentage point above the Dow Jones consensus… core PCE posted respective gains of 0.2% and 3.3%, in line with forecasts.”  
SUMMARY: Confirms two-sided PCE and that 10Y/30Y had recently tagged post-2007 highs — the 08-21 level problem.

CLAIM: Collins: PCE mixed, policy still mildly restrictive, hike still on the table if disinflation stalls.  
URL: https://www.reuters.com/business/feds-collins-says-latest-us-inflation-readings-are-mixed-2026-08-27/  
PUBLISHED: 2026-08-27  
QUOTE: (via contemporaneous roundup) “I continue to see rates as mildly restrictive” / open to an increase if disinflation evidence fails.  
SUMMARY: Not a dovish duration green light. Morning already had her as hawkish; the live comments did not reverse that.

CLAIM: Equity REITs −1.0% vs SPX +0.7%; 10Y 4.67% (+3 bps), 30Y 5.19% (+2 bps), 2Y 4.23% (+2 bps); oil $83.67 (+1.8%).  
URL: https://x.com/HoyaCapital/status/2093070764157092081  
PUBLISHED: 2026-08-27  
QUOTE: Equity REITs down 1.0% while S&P 500 +0.7%; 10-year 4.67% (+3 bps); 30-year 5.19% (+2 bps).  
SUMMARY: Same-day REIT/rates/oil tape. Directly falsifies the morning easing + oil-slide spine.

CLAIM: $44B 7-year auction high yield 4.512%.  
URL: https://www.treasurydirect.gov/instit/annceresult/press/preanre/2026/R_20260827_3.pdf  
PUBLISHED: 2026-08-27  
QUOTE: Auction results PDF dated 2026-08-27 (CUSIP 91282CRJ2).  
SUMMARY: Coupon supply cleared at a ~2-year high yield — no refinancing-window signal.

CLAIM: Nvidia Q2: $96.22B rev, $89B data center; shares +~8–9% on 8/27; SPX +~0.7% with tech the only green sector.  
URL: https://www.nytimes.com/2026/08/26/technology/nvidia-profit-ai-doubles-earnings.html  
PUBLISHED: 2026-08-26/27  
QUOTE: Revenue $96.22B, up 106% YoY; data-center $89B, up 117% YoY.  
SUMMARY: Explains SPY up / XLRE down: mega-cap AI, not broad risk-on.

CLAIM: VICI tagged a new 52-week low ~$25.75 on 8/27.  
URL: https://markets-data-api-proxy.ft.com/data/equities/tearsheet/summary?s=VICI:NYQ  
PUBLISHED: 2026-08-27  
QUOTE: 52-week low $25.75 (session).  
SUMMARY: Rate-sensitive REIT outlier; confirms duration flush beyond office.

CLAIM: Nareit/FTSE EPRA North America −0.92%, Americas −0.89% on the session snapshot.  
URL: https://www.reit.com/data-research/reit-indexes/real-time-index-returns  
PUBLISHED: 2026-08-27  
QUOTE: North America −0.92%; Americas −0.89%.  
SUMMARY: Index-wide real-estate down day, not XLRE idiosyncrasy.

---

OUTCOME_BEGIN
SECTOR: Real Estate
ETF: XLRE
ETF_PCT: -0.9536
SPY_PCT: 0.6553
REL_PCT: -1.6089
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Live 10Y/30Y backup (+3/+2 bps) into Warsh Friday, against a stale prior-close easing read.
KEY_INTERACTION: Nvidia-only SPX rally and the long-end backup were one rotation (AI duration-insensitive bid / bond-proxy dump), not independent S0 risk-on + S1 duration positives.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Direction miss — S0/S1 scored yesterday’s −6 bp easing and oil slide as a live REIT spine; the 1d tape, elevated 30Y level, and NVDA-narrow tape already argued fade/down, not up/mild.
OUTCOME_END

---

## RESEARCH APPENDIX

**Queries run**
- web_search: `XLRE real estate ETF August 27 2026 performance REITs rates`
- web_search: `US 10 year yield 30 year Treasury August 27 2026 Jackson Hole Warsh`
- web_search: `SPY stock market August 27 2026 PCE inflation Fed Collins`
- web_search: `XLRE EQIX PLD AMT WELL SPG office REITs August 27 2026`
- web_search: `site:fred.stlouisfed.org DGS10 DGS30 DFII10 August 27 2026`
- web_search: `stock market news August 27 2026 Nvidia Jackson Hole Warsh yields REITs lag`
- web_search: `Hoya Capital REIT daily recap August 27 2026 equity REITs Treasury yields`
- web_search: `Fed Collins PCE mixed restrictive August 27 2026 Jackson Hole`
- web_search: `7-year Treasury auction August 27 2026 yield`
- web_search: `S&P 500 sectors August 27 2026 only technology positive Nvidia REITs down`
- web_search: `VICI 52 week low August 27 2026 REIT`
- web_search: `Nvidia earnings August 26 2026 stock market August 27 only technology sector up`
- x_search: REIT/XLRE/yields 2026-08-27 (general)
- x_search: Hoya Capital 2026-08-27 recap (handle-filtered)
- web_fetch: CNBC PCE, BEA July 2026 PIO, Treasury 7-year results PDF, Nareit real-time indexes, Reuters/Axios/Zacks/TheStreet (several blocked)

**Key sources and facts taken**
- BEA PIO July 2026 — https://www.bea.gov/news/2026/personal-income-and-outlays-july-2026 — fetched 2026-08-27T21:33:51Z — headline PCE +3.7% YoY, core +3.3%, both +0.2% m/m.
- CNBC PCE wrap — https://www.cnbc.com/2026/08/26/feds-preferred-inflation-gauge-shows-core-prices-rose-3point3percent-annually-in-july.html — fetched 2026-08-27T21:33:51Z — core in-line, headline 0.1 ppt hot; 10Y/30Y recently at 2007-high zone; Warsh Friday.
- Hoya Capital — https://x.com/HoyaCapital/status/2093070764157092081 — 2026-08-27 — equity REITs −1.0%, SPX +0.7%, 10Y 4.67% +3 bps, 30Y 5.19% +2 bps, 2Y 4.23% +2 bps, oil $83.67 +1.8%.
- TreasuryDirect 7-year results — https://www.treasurydirect.gov/instit/annceresult/press/preanre/2026/R_20260827_3.pdf — 2026-08-27 13:02 ET — $44B 7s, high yield 4.512%.
- NYT / AP Nvidia — https://www.nytimes.com/2026/08/26/technology/nvidia-profit-ai-doubles-earnings.html — 2026-08-26 — $96.22B rev, $89B DC; 8/27 tech-only SPX.
- Nareit real-time — https://www.reit.com/data-research/reit-indexes/real-time-index-returns — fetched 2026-08-27T21:45:58Z — NA −0.92%, Americas −0.89%.
- FT VICI — https://markets-data-api-proxy.ft.com/data/equities/tearsheet/summary?s=VICI:NYQ — 2026-08-27 — 52w low $25.75.
- Reuters/Bloomberg/WSJ Collins roundup (search; some fetches 401/403) — mixed PCE, mildly restrictive, hike optionality.
- FRED search (DGS10/DGS30/DFII10) — official 8/27 series not yet posted; 8/26 ~4.66 / 5.18 / 2.34 vs morning’s 8/25 4.64 / 5.17 / 2.32. Live 8/27 levels taken from Hoya, not FRED.

**Not used as facts:** blocked Reuters/Axios/Zacks HTML challenges; FRED pages timed out; memory index unavailable.