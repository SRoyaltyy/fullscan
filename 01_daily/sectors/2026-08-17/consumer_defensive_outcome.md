# Sector Outcome — Consumer Defensive — 2026-08-17

Actuals: {'etf': 'XLP', 'pct': -1.6378163464902618, 'spy_pct': -0.47273666413644166, 'rel': -1.16507968235382, 'open': 85.36000061035156, 'close': 84.68000030517578}

I now have enough evidence to compile the post-session review. Let me gather the key facts.

Key findings:
- XLP fell -1.64% vs SPY -0.47%, relative -1.17% — a clear underperformance
- Constellation Brands (STZ) was the worst performer in the S&P 500, down ~5% (4.8% per TheStreet), following its recent earnings report and a string of weak results
- Walmart fell from $115.27 to $114.43 (-0.73%) ahead of its Aug 20 earnings
- The market was in "retail earnings week" mode — Walmart, Target, Home Depot all reporting this week
- Weak retail sales data (July -0.6%) and consumer sentiment at 51.0 were already known
- The morning prediction was up/notable (total 9.0) — this was a significant miss

Let me now compile the review.

OUTCOME_BEGIN
SECTOR: Consumer Defensive
ETF: XLP
ETF_PCT: -1.64
SPY_PCT: -0.47
REL_PCT: -1.17
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Pre-earnings profit-taking in the sector's mega-cap retail names (Walmart -0.7%, Constellation Brands -4.8% as worst S&P 500 performer) ahead of the retail earnings gauntlet (WMT/TGT/HD this week), combined with weak July retail sales (-0.6%) and consumer sentiment (51.0) that had already been priced as a defensive tailwind but instead triggered de-risking into the earnings prints.
KEY_INTERACTION: The morning read treated weak consumer data (retail sales -0.6%, sentiment 51.0) as a defensive-bid catalyst, but the market interpreted the same data as a warning sign ahead of Walmart/Target/Home Depot earnings — converting a would-be flight-to-safety tailwind into a de-risking headwind.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Direction and magnitude both MISS — the model scored all five components positive (9.0 total) and called up/notable, but the sector fell -1.64% with -1.17% relative underperformance; the weak consumer data was double-counted as a tailwind when it was actually a pre-earnings de-risking trigger.
OUTCOME_END

---

## Sector Post-Session Review — Consumer Defensive (XLP) — 2026-08-17

### 0. FACTS
- **ETF_PCT:** -1.64% (open 85.36 → close 84.68)
- **SPY_PCT:** -0.47%
- **REL_PCT:** -1.17% (XLP underperformed SPY by over a full point)
- **Path:** Opened at 85.36 and closed at 84.68 — a steady decline through the session, consistent with broad de-risking in the sector.

### 1. What drove the sector today

**Primary driver: Pre-earnings de-risking in mega-cap retail/staples names ahead of the retail earnings gauntlet.**

- **Constellation Brands (STZ) was the worst performer in the entire S&P 500**, down ~4.8-5.0%. Per TheStreet: "beer, wine and spirits company Constellation Brands (STZ) fell 4.8% following its recent earnings report and a string of..." — STZ is a top-10 XLP holding, so a 5% single-name drop mechanically drags the ETF.
  - CLAIM: STZ worst S&P 500 performer, -4.8% to -5%
  - URL: https://www.thestreet.com/stock-market-today/stock-market-today-dow-jones-sp-500-nasdaq-updates-aug-17-2026 and https://www.investopedia.com/stock-market-today-dow-jones-s-and-p-500-08172026-12061093
  - PUBLISHED: 2026-08-17
  - QUOTE: "Consumer Staples component Constellation Brands (STZ) is the worst-performing stock in the S&P 500 with a 5% decline" (Investopedia); "Constellation Brands (STZ) fell 4.8% following its recent earnings report" (TheStreet)

- **Walmart (WMT) fell ~0.7%** ($115.27 → $114.43) ahead of its Aug 20 earnings. WMT is XLP's largest holding (~10.4% weight), so even a modest decline in WMT moves the ETF.
  - CLAIM: WMT traded at $114.43 on Aug 17, down from $115.27 prior close
  - URL: https://www.ad-hoc-news.de/boerse/news/corporate-news/walmart-stock-holds-near-115-27-before-august-20-earnings/69960424
  - PUBLISHED: 2026-08-17
  - QUOTE: "Walmart Inc. stock traded at $114.43 on August 17, 2026, after a $115.27 previous close"

- **Retail earnings week context:** Walmart (Aug 20), Target, Home Depot, Lowe's all report this week. TheStreet headline: "Dow, S&P 500 fall as retail earnings reports loom." The market was de-risking into these prints given the weak July retail sales (-0.6%) and consumer sentiment (51.0) backdrop.
  - CLAIM: Retail earnings week (WMT, TGT, HD) drove de-risking
  - URL: https://www.thestreet.com/stock-market-today/stock-market-today-dow-jones-sp-500-nasdaq-updates-aug-17-2026
  - PUBLISHED: 2026-08-17

- **Broad market context:** SPY fell -0.47% — a down day overall. The sector's -1.64% was a notable underperformance, not just a beta move. Communication Services (XLC) also sank 1.6% per Benzinga, suggesting a broad de-risking day, but staples were hit hardest among defensives.

### 2. Audit of morning S0–S4 reads

| Component | Morning Score | Reality | Verdict |
|---|---|---|---|
| **S0 Shared Macro** | +1 (easing real yields, weak consumer data = defensive bid) | Weak consumer data (retail sales -0.6%, sentiment 51.0) was NOT a defensive bid catalyst — it was a de-risking trigger ahead of retail earnings. Real yields easing was real but insufficient. | **MISS** — the macro read was directionally wrong on the consumer data interpretation |
| **S1 Sector Factors** | +1 (flight-to-safety emerging, input cost relief) | Flight-to-safety did NOT materialize; the sector was sold, not bid. Input cost relief was real but irrelevant to the day's move. | **MISS** — flight-to-safety factor failed to confirm |
| **S2 Breadth** | +1 (3d rel +0.90%, 1w rel +0.74% improving) | The improving relative tape from prior days did NOT persist — XLP gave back the entire 3d relative gain in one session (-1.17% rel). | **MISS** — tape inflection reversed sharply |
| **S3 Flows/Positioning** | +1 (first inflows since Feb, BofA defensive rotation) | Inflows/rotation thesis did not hold on the day; the sector was net sold. | **MISS** — flow thesis failed to confirm |
| **S4 ETF Tape** | +1 (confirmation of positive inflection) | Tape was the OPPOSITE — XLP opened and closed lower, underperforming SPY by -1.17%. | **MISS** — tape contradicted the call |

**Overall:** All five components scored +1 (total 9.0, up/notable). Actual was down/notable. This was a complete directional miss.

### 3. Interactions / double-count / knowable-at-open test

**Double-count issue (the core error):** The morning read counted the weak consumer data (retail sales -0.6%, sentiment 51.0) as a **positive** for defensives in BOTH S0 (defensive bid from weak data) and S1 (flight-to-safety). But the same data was simultaneously the **reason to de-risk** ahead of Walmart/Target/Home Depot earnings. The model treated "weak consumer → defensive bid" as a one-way street, when in a retail-earnings week the weak data is a two-sided risk: it could mean staples benefit from flight-to-safety, OR it could mean the sector's biggest holdings (WMT, TGT, HD) are about to disappoint.

**Knowable at open?** Partially. The following were knowable at the open:
- Retail earnings week (WMT Aug 20, TGT, HD) was known — the morning read even flagged it as a catalyst.
- STZ's weak earnings trajectory was known (it had already reported and was down ~39% YTD per CNBC).
- The risk-on tape with tech leading was known.

What was NOT knowable at open: the magnitude of the STZ selloff (-5%) and the broad de-risking into retail earnings. But the *direction* of the risk (de-risking into a retail earnings week with weak consumer data) was knowable — the model should have at least flagged the two-sided risk rather than scoring all components positive.

**Interaction:** The STZ single-name drag (-5%, worst in S&P 500) interacted with WMT's -0.7% to mechanically push XLP down ~1.6%. This is a concentration risk the morning read acknowledged (Walmart 10.4% weight) but did not adequately weight.

### 4. Outliers inside the sector

- **Constellation Brands (STZ):** -4.8% to -5.0%, worst performer in the entire S&P 500. Following its recent earnings report and a string of weak results. STZ is a top-10 XLP holding, so this single name drove a meaningful chunk of the ETF's decline.
- **Walmart (WMT):** -0.7% ($115.27 → $114.43), below its 200-day moving average, ahead of Aug 20 earnings. As XLP's largest holding (~10.4%), this dragged the ETF.
- **Broad sector:** The entire consumer staples complex was sold — VDC (Vanguard Consumer Staples) also fell 1.1% per Fidelity, confirming this was sector-wide, not just single-name.

---

### OUTCOME_BEGIN
SECTOR: Consumer Defensive
ETF: XLP
ETF_PCT: -1.64
SPY_PCT: -0.47
REL_PCT: -1.17
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Pre-earnings de-risking in mega-cap retail/staples (STZ -4.8% worst in S&P 500, WMT -0.7%) ahead of the retail earnings gauntlet (WMT/TGT/HD this week), with weak July retail sales (-0.6%) and consumer sentiment (51.0) converting from a would-be defensive tailwind into a de-risking trigger.
KEY_INTERACTION: The morning read double-counted weak consumer data as a defensive bid catalyst (S0 + S1), but in a retail-earnings week the same data was a de-risking trigger — the two-sided risk was not flagged.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Complete directional miss — all five components scored +1 (9.0 total, up/notable) but XLP fell -1.64% with -1.17% relative underperformance; the model failed to weight the two-sided risk of weak consumer data ahead of retail earnings week.
OUTCOME_END