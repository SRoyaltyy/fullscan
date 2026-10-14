# Sector Outcome — Consumer Cyclical — 2026-10-13

Actuals: {'etf': 'XLY', 'pct': 0.08016742671801413, 'spy_pct': -0.011562244330465266, 'rel': 0.0917296710484794, 'open': 112.30000305175781, 'close': 112.36000061035156, 'source': 'yf_download'}

OUTCOME_BEGIN
SECTOR: Consumer Cyclical
ETF: XLY
ETF_PCT: 0.0802
SPY_PCT: -0.0116
REL_PCT: 0.0917
ACTUAL_DIRECTION: flat
ACTUAL_MAGNITUDE: flat
PRIMARY_DRIVER: Hormuz oil-shock thesis did not reprice on the cash session (WTI only +0.4%, Brent −0.9%); AMZN/TSLA dragged as NQ beta implied, but HD and the rest of the book offset into a tight, earnings-mixed tape.
KEY_INTERACTION: Overlay double-weighted a knowable headline against a non-confirming live tape (XLY PM +0.09%, ES −0.09%, premarket WTI already −1.59%) — S0 shock and S1 gasoline transmission were the same unconfirmed object.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Direction miss (down vs flat); mild cap was the right instinct, but S0 −2 on Hormuz overrode the tape_anchor that already said this was not a break.
OUTCOME_END

## 0. Facts

XLY opened 112.30 and closed 112.36 (**+0.080%**). High 112.92 / low 112.16 — a ~0.7-pt range, no trend day. SPY **−0.012%**. Relative **+0.092%**. Path: slightly green open, contained, closed a few cents above the open. This is **flat**, not mild-down.

Index tape: S&P 500 **−0.02%** to 7,805.45; Nasdaq **−0.15%** to 27,204.38; Dow **−0.35%**; Russell 2000 **−0.40%**. Near-record, little-changed session. 10-year yield eased to **~5.21%** (from 5.24% Friday). CPI was **not** printed (Wed 10-14).

## 1. What drove the sector

Taxonomy, in order of actual cash-session weight:

- **S0 shared macro — oil/geopolitics failed as a crush.** Morning treated overnight Hormuz/Iran/Houthi kinetic news as a live supply shock (S0 −2). Cash oil did **not** follow that script: WTI settled **$94.14, +$0.40 / +0.4%**; Brent **fell 0.9% to $105.69** on hopes a US–Russia diesel deal would free supply, offsetting Saudi/Yemen attacks. Equities treated geopolitics as background, not a risk-off event. Energy **did** lead (XLE ~**+1.76% to +1.80%**; S&P Energy **+1.74%**), which is the oil-beta sleeve, not discretionary pain.
- **S4/S0 growth beta into the two megas — this part worked at the name level.** AMZN **−0.37%** ($258.58), TSLA **−0.74%** ($375.23). That is the NQ −0.73% premarket channel into ~41% of XLY. It was **not** enough to take the ETF down.
- **S1/S2 internal book — HD and the rest offset the megas.** HD **+1.14%** ($293.92). NKE **−0.06%**. S&P Consumer Discretionary index **−0.08%** vs XLY **+0.08%** — composition noise, not a sector crash. Nested “8/10 groups down vs parent” did not show up as ETF damage.
- **S2 rotation call inverted vs the PM snapshot.** Morning: XLY PM +0.09% vs XLP +0.98% as flight-to-staples. Close: **XLP −0.26%**, XLY +0.08%, XLK −0.04%. Defensives did **not** lead the cash session. Energy did.
- **S3 flows — correctly unused.** No VIX stress (still ~15s), no forced-flow tell. Bank earnings (GS fade after a 4% open pop; mixed JPM/WFC) and chip/China-memory headlines were the **index** story, not a consumer-credit event. HY OAS was not the day’s object.

Primary object for XLY was therefore **non-transmission**: a geopolitical headline that oil futures and the ETF tape refused to validate, plus mega-cap softness that the rest of the book neutralized.

## 2. Audit of morning S0–S4 (use morning numbers, not rewrites)

| Sleeve | Morning | Reality 10-13 | Verdict |
|---|---|---|---|
| **S0 −2** | Hormuz kinetic shock dominant; NQ −0.73%; hawkish minutes as widener; real-yield grind confirmatory | Oil mixed (WTI +0.4% / Brent −0.9%); SPY flat; yields eased; CPI unprinted as instructed | **Too hot.** Headline was real; dominance was not. Premarket WTI **−1.59%** already contradicted “futures haven’t repriced.” |
| **S1 −1** | Gasoline transmission + nested HEAT down; no retail/claims print | No gasoline crush in the tape; HD +1.14% vs AMZN/TSLA down | **Overstated transmission.** Incremental S1 was mostly the same shock counted as pass-through. |
| **S2 −1** | Rotation out of XLY into defensives | XLP reversed; energy led; XLY slightly beat SPY | **Wrong sign on the cash print.** PM snapshot was not the close. |
| **S3 0** | No forced flow | Correct | **Hold.** |
| **S4 −1** | 1d rel −0.45%; NQ vs ES growth-red; mild cap | Rel flipped to **+0.09%**; XLY PM +0.09% was the better tell | **Direction of S4 was the weakest sleeve;** the **mild cap** was the useful part. |

**Predicted:** down / mild, score −7.678, overlay −6.0, tape_anchor **+0.014**, divergence flagged, size_gate on.  
**Actual:** flat / flat, tiny green, slight relative win.

The 09-08 oil-shock pattern **should not have been allowed to set S0 = −2** when (a) XLY PM was flat, (b) ES was −0.09%, (c) oil futures were **down** on the morning snapshot. The pattern’s own mild-cap clause (“unless futures confirm ≥0.5% down”) was the tell: futures did **not** confirm a down equity shock, and WTI never delivered a crush. Overlay −6.0 vs tape_anchor +0.014 is the quantitative fingerprint of that override.

08-27 XLK-map ban: AMZN/TSLA duration-to-NQ **was** a legitimate direct channel (they did fall). Using it to sink the **ETF** still failed because weight-adjacent drag ≠ book-wide close.

09-25 flat-absolute companion correctly did **not** fire (needed green ES/NQ).

CPI/Powell: correctly left as unprinted binaries. That was clean.

## 3. Interactions / double-count / knowable-at-open

**Double-count:** Morning claimed Hormuz once in S0 and transmission once in S1. Functionally it was **one unconfirmed object scored twice** (shock + gasoline). Not a two-factor stack.

**Interaction that mattered:** NQ-beta (AMZN/TSLA) **did** leak into the book, but it was **offset** by HD and non-mega cyclical names. Scoring “growth-led red” as ETF direction ignored that XLY is not NQ. Rel +0.09% vs SPY is the residual of that offset.

**Knowable at open: partially.**

- Knowable: XLY PM +0.09%, ES −0.09%, oil futures **down** 1.59% on Finviz, mega-caps not breaking, VIX 15.4 — this is a **flat/mild** setup, not a down-day with S0 = −2.
- Not knowable: whether HD would +1.1% and fully offset the megas; whether Brent would sell off on a diesel-deal headline; whether bank-earnings noise would pin SPY at unchanged.
- The divergence flag and size_gate were the parts of the morning that were **right**. The overlay that ignored them was the miss.

## 4. Outliers inside the sector

- **HD +1.14%** vs **TSLA −0.74%** / **AMZN −0.37%** — the ETF is not “two names,” and today the third name and the tail mattered.
- **XLY +0.08%** vs **S&P Consumer Discretionary −0.08%** — small composition gap; do not treat them as identical.
- **XLE +1.8%** vs **XLY +0.08%** — oil-beta winner was energy, not a discretionary loser.
- **XLP −0.26%** after PM **+0.98%** — the defensive-lead snapshot died in cash hours.
- **NKE −0.06%** — apparel did not validate the nested HEAT “crush.”

---

**Evidence**

CLAIM: XLY closed 112.36 on 2026-10-13, +0.08%; open 112.30, high 112.92, low 112.16.  
URL: https://www.marketwatch.com/investing/fund/xly/download-data  
PUBLISHED: 2026-10-13 session (historical quotes as of 2026-10-14 retrieval)  
QUOTE: “10/13/2026: Open $112.30 / High $112.92 / Low $112.16 / Close $112.36”  
SUMMARY: Matches deterministic OPEN/CLOSE; tight range, flat close.

CLAIM: SPY/S&P 500 essentially unchanged Tuesday; Nasdaq modestly lower.  
URL: https://www.euronews.com/2026/10/14/oil-prices-rise-after-reported-attacks-on-ships-in-strait-of-hormuz  
PUBLISHED: 2026-10-14 08:28 GMT+2  
QUOTE: “Wall Street indices finished little changed near their records on Tuesday. … The S&P 500 edged down by less than 0.1% … The Dow Jones Industrial Average fell 0.3%, while the Nasdaq Composite dipped 0.2%.”  
SUMMARY: Index tape was a non-event, not a risk-off liquidation.

CLAIM: S&P 500 −0.02% to 7,805.45; Nasdaq −0.15% to 27,204.38; Dow −0.35%.  
URL: https://www.morningstar.com/news/dow-jones/202610138290/sp-500-falls-002-to-780545-data-talk  
PUBLISHED: 2026-10-13  
QUOTE: “S&P 500 falls 0.02% to 7805.45”  
SUMMARY: Confirms SPY_PCT ≈ 0.

CLAIM: WTI settled $94.14, +$0.40 / +0.4%; Brent −0.9% to $105.69 on diesel-supply offset vs Gulf attacks.  
URL: https://okenergytoday.com/2026/10/mixed-oil-prices-on-tuesday/  
PUBLISHED: 2026-10-13/14  
QUOTE: “West Texas Intermediate … finished up 40 cents or 0.4% to settle at $94.14 … Brent futures … fell 96 cents or 0.9% to close at $105.69 … hopes a US-Russel diesel fuel deal could free up more supplies from Moscow offset attacks in Saudi Arabia and Yemen.”  
SUMMARY: The live oil object was mixed, not a Hormuz spike. Morning S0 assumed repricing that Tuesday did not deliver.

CLAIM: Wednesday (10-14) oil rose on *further* Hormuz attacks — i.e. Tuesday was not the crash session.  
URL: https://www.euronews.com/2026/10/14/oil-prices-rise-after-reported-attacks-on-ships-in-strait-of-hormuz  
PUBLISHED: 2026-10-14  
QUOTE: “Oil prices climbed in early trading on Wednesday after Iran reportedly attacked two vessels in the Strait of Hormuz … Brent crude futures rose around 1% spiking above $107 … WTI … gained 0.5% to $94.64.”  
SUMMARY: Incremental Hormuz repricing was a **next-day** object. Do not rewrite Tuesday’s S0 with Wednesday’s tape.

CLAIM: AMZN −0.37% to $258.58; TSLA −0.74% to $375.23; HD +1.14% to $293.92.  
URL: https://www.marketwatch.com/investing/stock/tsla/download-data  
PUBLISHED: 2026-10-13 historical  
QUOTE: TSLA close $375.23 vs prior $378.03; AMZN $258.58 vs $259.53; HD $293.92 vs $290.62.  
SUMMARY: Mega-cap NQ channel worked; HD offset; ETF flat is the residual.

CLAIM: S&P sector: Energy +1.74%, Consumer Discretionary −0.08%, Staples −0.33%; S&P −0.02%.  
URL: https://digital.fidelity.com/prgw/digital/research/sector  
PUBLISHED: 2026-10-13 16:33 ET  
QUOTE: Energy +1.74%; Consumer Discretionary −0.08%; Consumer Staples −0.33%.  
SUMMARY: Energy led; discretionary was noise-flat; staples did not confirm the morning defensive-rotation read.

CLAIM: XLE ~+1.76–1.80%; XLP −0.26% to $84.19; XLK −0.04% to $196.28.  
URL: https://www.marketwatch.com/investing/fund/xlp/download-data  
PUBLISHED: 2026-10-13  
QUOTE: XLP $84.19, −0.26%; XLE ~$67.20, +1.76–1.80%; XLK $196.28, −0.04%.  
SUMMARY: PM XLP lead reversed; tech was not the XLY driver; energy was the oil winner.

CLAIM: NKE −0.06% to $35.33.  
URL: https://investors.nike.com/stock-info/historical-data  
PUBLISHED: 2026-10-13  
QUOTE: Close $35.33 vs prior $35.35 (−0.06%).  
SUMMARY: Apparel nested-HEAT “down” did not show as a material ETF factor.

CLAIM: 10-year yield eased to 5.21% Tuesday after oil fell; bond market had been closed Monday.  
URL: https://www.euronews.com/2026/10/14/oil-prices-rise-after-reported-attacks-on-ships-in-strait-of-hormuz  
PUBLISHED: 2026-10-14  
QUOTE: “US Treasury yields eased on Tuesday after oil prices fell. The yield on the 10-year Treasury slipped to 5.21% from 5.24% late Friday.”  
SUMMARY: Duration sleeve was **not** a fresh 1-day tightening shock. Real-yield grind was level, not the session driver.

CLAIM: CPI printed Wednesday 10-14, not Tuesday.  
URL: https://www.newsquawk.com/headlines/primer-us-cpi-due-wednesday-14th-october-2026-at-1330-bst--0830-edt  
PUBLISHED: ahead of 2026-10-14  
QUOTE: “US CPI due Wednesday 14th October 2026 at 1330 BST / 0830 EDT.”  
SUMMARY: Morning correctly refused to pre-score CPI/Powell.

---

## RESEARCH APPENDIX

**Queries run**

1. `XLY ETF October 13 2026 performance consumer discretionary` (web_search)
2. `stock market October 13 2026 Hormuz Iran oil attack SPY` (web_search)
3. `WTI oil price October 13 2026 Hormuz tanker Iran` (web_search)
4. X search: XLY / AMZN / TSLA / oil / Hormuz on 2026-10-13–14
5. `Amazon Tesla Home Depot stock October 13 2026` (web_search)
6. `S&P 500 sector performance October 13 2026 consumer discretionary energy staples` (web_search)
7. `"October 13" 2026 CPI Powell Fed stocks oil Hormuz` (web_search)
8. `XLP XLE XLK performance October 13 2026` (web_search)
9. `Nasdaq October 13 2026 close AMZN TSLA NKE MCD SBUX` (web_search)
10. `WTI crude oil October 13 2026 close price change` (web_search)
11. `site:marketwatch.com XLY historical October 2026` (web_search)
12. `how major US stock indexes fared Tuesday October 13 2026 consumer discretionary` (web_search)
13. `NKE Nike stock October 13 2026 percent change` (web_search)
14. `XLY AMZN TSLA HD October 13 2026 session energy rotation` (web_search)
15. X search: XLY / AMZN / TSLA / HD / WTI recap 2026-10-13–14
16. Fetches: FinanceCharts XLY (403), Seattle PI (challenge), Euronews Hormuz (ok), OK Energy Today (ok), Seattle Times (empty)

**Key sources (title + URL + timestamp)**

| Source | URL | Time |
|---|---|---|
| MarketWatch XLY historical | https://www.marketwatch.com/investing/fund/xly/download-data | 2026-10-13 quotes, retrieved 2026-10-14 |
| Euronews: Oil prices rise after Hormuz attacks (Wed wrap of Tue close) | https://www.euronews.com/2026/10/14/oil-prices-rise-after-reported-attacks-on-ships-in-strait-of-hormuz | 2026-10-14 08:28 GMT+2 |
| Oklahoma Energy Today: Mixed oil prices on Tuesday | https://okenergytoday.com/2026/10/mixed-oil-prices-on-tuesday/ | 2026-10-13 session recap |
| Morningstar/Dow Jones: S&P 500 falls 0.02% to 7805.45 | https://www.morningstar.com/news/dow-jones/202610138290/sp-500-falls-002-to-780545-data-talk | 2026-10-13 |
| Fidelity sector performance | https://digital.fidelity.com/prgw/digital/research/sector | 2026-10-13 16:33 ET |
| MarketWatch TSLA / HD / XLP download-data | marketwatch.com investing pages | 2026-10-13 |
| Nike IR historical | https://investors.nike.com/stock-info/historical-data | 2026-10-13 |
| Newswquawk CPI primer | https://www.newsquawk.com/headlines/primer-us-cpi-due-wednesday-14th-october-2026-at-1330-bst--0830-edt | pre-10-14 |
| YCharts / Stocknear SPY | https://ycharts.com/companies/SPY/price | 2026-10-13 close ~778.01 |

**Facts taken from each**

- **MarketWatch XLY:** O/H/L/C 112.30 / 112.92 / 112.16 / 112.36; volume ~5.68M. Used as independent confirm of deterministic actuals.
- **Euronews 10-14:** Tuesday indexes little changed; 10y 5.21%; Wednesday oil up on *new* Hormuz attacks; bank-earnings mixed (GS fade). Separates Tuesday non-event from Wednesday oil jump.
- **OK Energy Today:** WTI +0.4% to $94.14; Brent −0.9% to $105.69; diesel-deal offset vs Saudi/Yemen. Core S0 falsifier.
- **Morningstar / Fool / portfolio-terminal:** SPX −0.02%, NDX −0.15%, DJI −0.35%, RUT −0.40%. Index magnitude = flat.
- **Fidelity sectors:** Energy +1.74%, Discretionary −0.08%, Staples −0.33%. Rotation was into energy, not staples.
- **AMZN/TSLA/HD/NKE prints:** Internal book split; HD is the offset outlier.
- **XLP/XLE/XLK ETF closes:** Staples PM lead reversed; tech not the XLY driver.
- **CPI calendar:** Binary was 10-14, not 10-13 — morning non-score stands.
- **X posts 10-13:** Energy leadership, mixed banks, chips weak, oil elevated but not a panic close. Low weight vs exchange prints; used only as color.

Memory index is paused (embedding metadata missing); review used injected morning card + live sources only.