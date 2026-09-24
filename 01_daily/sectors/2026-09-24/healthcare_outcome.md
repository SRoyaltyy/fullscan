# Sector Outcome — Healthcare — 2026-09-24

Actuals: {'etf': 'XLV', 'pct': 0.6338815438893031, 'spy_pct': -0.0820521854125067, 'rel': 0.7159337293018098, 'open': 168.85000610351562, 'close': 169.8699951171875, 'source': 'yf_download'}

Memory search is paused (embedding index metadata missing). Review uses the injected morning card, deterministic actuals, and live sources.

OUTCOME_BEGIN
SECTOR: Healthcare
ETF: XLV
ETF_PCT: 0.634
SPY_PCT: -0.082
REL_PCT: 0.716
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Mega-cap pharma leadership (LLY FDA once-weekly insulin) on a nearly-flat, yield-pressured SPY tape
KEY_INTERACTION: Hawkish-yield shock scored as XLV duration headwind instead hit bond-proxy defensives (staples/utilities ~−1%) while a same-session LLY FDA print overrode the empty S1 spine
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Engine down/mild was a carry-minted direction miss; the near-zero card’s flat/flat was closer but still missed mild-up from an afternoon mega-cap FDA plus the relative cushion the card described then zeroed in S4
OUTCOME_END

## 0. Facts

Deterministic cash session (open→close): **XLV +0.634%**, **SPY −0.082%**, **rel +0.716%**. Open **168.85** → close **169.87**.

Path (cross-checked, not in the actuals block): cash opened near Wednesday’s close, traded a wide range (reports ~168.73–171.54), and finished mild green. Premarket **PM:XLV −0.40% did not persist**. Absolute band is **mild** (not flat, not notable). Relative was a **clear defensive outperformance** vs a flat-to-red SPY.

CLAIM: XLV closed ~169.87, +0.63%, open 168.85 on 2026-09-24.
URL: https://finance.yahoo.com/quote/XLV/history
PUBLISHED: 2026-09-24 (EOD history)
QUOTE: “Sep 24, 2026 … Open: 168.85 … Close: 169.87”
SUMMARY: Matches the injected actuals (open 168.85 / close 169.87 / +0.63%).

CLAIM: S&P 500 finished nearly flat (−<0.1%); 10-year yield settled +7 bp to 5.18%.
URL: https://www.bnnbloomberg.ca/markets/2026/09/24/us-stocks-swing-as-the-bond-market-oil-prices-keep-up-the-pressure/
PUBLISHED: 2026-09-24
QUOTE: “The S&P 500 finished nearly flat and edged down by less than 0.1 per cent… The 10-year note yield settled up seven basis points to 5.18%.”
SUMMARY: Macro tape was a yield-up, index-flat session — not a crash, not a risk-on melt-up.

CLAIM: Health care +0.7% was a standout; LLY +2.85%, MRNA +6.98%; staples and utilities −1.0%.
URL: https://hosting.briefing.com/cschwab/InDepth/DailySectorWrap.htm
PUBLISHED: 2026-09-24 16:18 ET
QUOTE: “The health care sector (+0.7%) was another standout, supported by continued strength in Moderna (MRNA 194.82, +12.71, +6.98%) and Eli Lilly (LLY 1183.79, +32.80, +2.85%)… utilities (−1.0%), and consumer staples (−1.0%) recorded the widest losses.”
SUMMARY: XLV’s mild up was sector-real, not a quote glitch; leadership was mega-cap pharma/biotech, not a broad defensive bid.

## 1. What drove the sector (taxonomy)

**Primary: large-cap leadership / favorable FDA (S1), expressed through XLV’s top weight.**  
Eli Lilly (~15% of XLV) jumped ~3% after FDA approved **Onswik (insulin efsitora alfa-gobe)**, a once-weekly basal insulin for type 2 diabetes. That is a same-session mega-cap Rx/FDA HIT. With LLY that size, a ~3% print is enough to mechanically lift XLV by ~0.4–0.5 pp before any other name moves — i.e., most of the ETF’s +0.63%.

CLAIM: LLY +3.1% in the afternoon after FDA approved Onswik.
URL: https://markets.financialcontent.com/wss/article/stockstory-2026-9-24-eli-lilly-lly-stock-is-up-what-you-need-to-know
PUBLISHED: 2026-09-24
QUOTE: “Shares of … Eli Lilly jumped 3.1% in the afternoon session after The U.S. Food and Drug Administration approved Eli Lilly’s once-weekly basal insulin, Onswik, for adults with type 2 diabetes.”
SUMMARY: Idiosyncratic mega-cap FDA, timed to the **afternoon**, not the morning card.

CLAIM: Official Lilly release: FDA approved Onswik on 2026-09-24.
URL: https://investor.lilly.com/news-releases/news-release-details/us-food-and-drug-administration-fda-approves-lillys-onswiktm
PUBLISHED: 2026-09-24
QUOTE: “U.S. Food and Drug Administration (FDA) Approves Lilly’s Onswik™”
SUMMARY: Primary-source confirmation of the catalyst (page itself 403’d here; title/date corroborated by search + secondary recap).

**Secondary: relative defensive bid vs duration (S0/S4), not flight-to-safety.**  
10Y +7 bp and FOMC speakers (Williams: another hike this year; Paulson: modest further tightening) kept growth/bond-proxies under pressure. Healthcare **did not** trade as a duration victim. It traded as the defensive that still has earnings/idiosyncratic bid while **XLP/XLU were sold**. That is low-beta leadership **inside the defensive complex**, not 08-13 leftover RS and not a classic risk-off pile-in.

**Not the driver:**  
- AMGN dazodalibep (09-22) — leftover; AMGN ~flat (~+0.2%). Morning cap was correct.  
- IRA cycle-3 / MFN / MA 2027 — not today.  
- Oil — energy’s story (WTI reversed off highs, still settled up); do not map into XLV.  
- Engine ES/PM tape_anchor — cash XLV reversed the −0.40% PM print.

**Outliers inside the sector:** LLY +~2.9% (FDA); MRNA +~7% (oncology/ESMO leftover momentum — Briefing cited it as sector support; it is not an XLV mega-weight). UNH ~+1.0%, JNJ ~+0.6%, ABBV/MRK/AMGN ~flat. Breadth was **large-cap pharma-led**, not a small-cap biotech melt (XBI only ~+0.5%).

## 2. Audit of morning S0–S4 vs reality

Use **morning numbers**, not a rewrite.

| Sleeve | Morning | Reality | Verdict |
|---|---|---|---|
| **S0 −0.3** | Hawkish Warsh/hike overhang, mixed futures, PM:XLV −0.40% offered with tech; 09-14 relative cushion only “partial” | Yields **did** rise (+7 bp). Absolute smash **did not**. XLV **up** vs SPY **flat**. Staples/utilities, not healthcare, took the duration hit | Sign wrong for *absolute*. The prose already had the better relative read and then under-weighted it |
| **S1 0.0** | “No same-morning mega-cap Rx headline”; AMGN capped; IRA Sep 30 | **LLY Onswik FDA printed in-session.** AMGN leftover correctly did not dominate | Process-right at the open; **session-false**. This is the main S1 miss, and it was **not knowable at open** if the approval hit in the afternoon |
| **S2 0.0** | No premarket mega-cap breakdown; don’t copy 3d/1w/1m rel | Session breadth was LLY/MRNA-led, other mega-caps modestly green — not a failure | Morning 0 is fair as a *pre-open* score |
| **S3 0.0** | 1m rel −3.48% = crowded long already unwound | Mild up does not require a flow spike; under-owned was a **cushion**, not an accelerant | Hold. Do not promote 1m lag into a 1-day inflow story |
| **S4 0.0** | 1d rel +0.61% called a “defensive cushion” then scored 0 so it wouldn’t license absolute up | Cash rel **+0.72%** — the cushion **printed** | **Biggest knowable miss.** Prose saw it; score suppressed it |

**Engine vs card:** LLM leading sum ≈ −0.3 (pipeline `leading_sum: -0.6` after skill multipliers) with **divergence flagged True** and an explicit **flat/flat** official call. Engine still emitted **down/mild** off `tape_anchor −2.371` (ES −0.64%, PM:XLV −0.40%) + `index_carry −1.915`. That is exactly the **09-23 empty-spine** failure mode the morning write-up warned about: **do not mint a signed direction off a near-zero healthcare card**.

Predicted **down/mild** vs actual **up/mild** = **direction MISS, magnitude HIT** (band only). The LLM flat/flat would have been a **direction miss / magnitude miss** too, but a smaller signed error.

## 3. Interactions / double-count / knowable-at-open

**Double-count:** Clean. Hawkish-rate drag was scored once in S0 and not restacked into S1 (XBI sleeve) or S2. 3d/1w/1m rel were not copied into S2/S3 (08-28). Oil was not scored as healthcare rotation. AMGN/BSX/CI/CAH were capped; none drove the close.

**False interaction that mattered:** Morning treated **hawkish duration + PM red** as a small absolute lean down, then used 09-18/09-22 emit-caps to block *up* and *down*. The live interaction was different: **higher yields sold bond proxies (XLP/XLU) and left XLV as the defensive that could still rally on a mega-cap FDA.** Scoring S0 negative and S4 at 0 double-suppressed the one knowable positive (relative cushion / under-owned).

**Knowable at open: partially.**
- **Yes:** 1m rel −3.48% under-owned; Wed 1d rel +0.61% cushion; FOMC paid; no CPI/NFP; empty MA/IRA spine; AMGN must not dominate; PM red forbids a *fat* up.
- **No:** Afternoon LLY Onswik FDA (StockStory: afternoon session). That single print is most of the absolute +0.63%.
- **Mis-mapped:** “Hawkish rates → XLV down.” Rates went up; XLV went up. The 09-14 destination/relative-cushion mapping was the one that survived.

09-18 up-cap (PM ≤ 0) correctly forbade a **fat** absolute-up *at the open*. It did **not** imply the cash session couldn’t reverse a −0.40% PM on a mega-cap FDA. Treating PM red as a lid on *mild* up was too tight once S1 got a real HIT.

## 4. Outliers

- **LLY:** ~+2.9% on Onswik FDA — *the* XLV mover; ~15% weight.  
- **MRNA:** ~+7% oncology continuation — Briefing sector support; not an XLV mega-weight.  
- **AMGN:** leftover Phase-3 **did not** follow through (~flat).  
- **XLP/XLU:** ~−1.0% vs XLV +0.6–0.7% — healthcare ≠ generic defensive.  
- **XLK:** recovered from >1% down to ~−0.3%; the “offered with tech” PM analog died in cash.

---

**Lesson (binding, alongside 09-23):** When S0–S4 are near zero, **do not let ES/PM carry mint down/mild**. When the card itself describes a 1d relative cushion + deep 1m under-own, **S4 cannot be 0 if you are going to claim that cushion is real** — score a small relative-positive or keep flat/flat, but don’t then let tape_anchor flip the sign. Same-morning mega-cap FDA (LLY-sized) **can** dominate XLV; 08-14 “no mega-cap Rx headline” is a **live** check, not a morning fossil — if it prints after the open, the autopsy should mark S1 as unknowable-at-open, not as a factor-card error.

---

## RESEARCH APPENDIX

**Memory:** `memory_search` disabled — index metadata missing (`openclaw memory status --index` / `openclaw memory index --force`). No MEMORY.md hits used.

**Queries run**
- web_search: `XLV healthcare ETF September 24 2026 market news`
- web_search: `S&P 500 healthcare sector September 24 2026 why stocks rose`
- web_search: `XLV stock price September 24 2026 close`
- web_search: `Kevin Warsh Fed September 24 2026 stocks healthcare defensive`
- web_search: `Amgen dazodalibep Sjögren September 2026 stock`
- web_search: `Eli Lilly LLY stock September 24 2026`
- web_search: `site:finance.yahoo.com XLV historical data September 24 2026`
- web_search: `US stocks September 24 2026 healthcare outperforms yields Warsh`
- web_search: `XBI IBB UNH JNJ ABBV MRK AMGN September 24 2026 performance`
- web_search: `sector performance September 24 2026 XLV XLK XLE XLP XLU`
- web_search: `Eli Lilly LLY why up September 24 2026`
- web_search: `healthcare sector wrap September 24 2026 Lilly Moderna UnitedHealth`
- web_search: `Eli Lilly FDA approves Onswik insulin efsitora September 24 2026 press release`
- web_search: `Moderna stock why up September 24 2026 ESMO cancer vaccine`
- web_search: `"health care" OR healthcare +0.7 OR XLV September 24 2026 Briefing Lilly Moderna`
- web_search: `UnitedHealth UNH stock September 24 2026 close percent`
- x_search: movers XLV/AMGN/UNH/LLY/JNJ/Warsh/rotation (2026-09-24 to 2026-09-25)
- x_search: XLV close / LLY UNH AMGN XBI / defensive yields (same window)
- web_fetch: Benzinga sector leaders (403), CNBC video page (title only), BNN Bloomberg recap, Yahoo XLV history (fetch failed), Yahoo healthcare sector (empty), Seattle Times (empty), Briefing.com Daily Sector Wrap, Reuters (401), Stocknear LLY (403), StockStory LLY recap, TipRanks (403), Lilly IR (403), Reuters LLY FDA (401)

**Key sources and facts taken**

1. **Injected actuals** — XLV +0.6339%, SPY −0.0821%, rel +0.7159%, O/C 168.85/169.87. Used as the official tape.  
2. **Yahoo XLV history** (https://finance.yahoo.com/quote/XLV/history) — Open 168.85, High 171.54, Low 168.73, Close 169.87, Vol ~8.78M. Confirms path.  
3. **Briefing.com Daily Sector Wrap**, 24-Sep-26 16:18 ET (https://hosting.briefing.com/cschwab/InDepth/DailySectorWrap.htm) — HC +0.7%; LLY 1183.79 +32.80 (+2.85%); MRNA +6.98%; XLP/XLU −1.0%; 10Y +7 bp to 5.18%; Williams/Paulson hike talk; oil reversed from highs. **Primary session taxonomy source.**  
4. **BNN Bloomberg / AP Stan Choe** (https://www.bnnbloomberg.ca/markets/2026/09/24/us-stocks-swing-as-the-bond-market-oil-prices-keep-up-the-pressure/) — SPX nearly flat, 10Y to 5.18%, claims down, oil/yields whipsaw. Macro path.  
5. **StockStory / financialcontent**, 2026-09-24 (https://markets.financialcontent.com/wss/article/stockstory-2026-9-24-eli-lilly-lly-stock-is-up-what-you-need-to-know) — LLY +3.1% **afternoon** on FDA Onswik; Reuters-attributed trial/label details. Timing = not knowable at open.  
6. **Lilly IR title via search** (https://investor.lilly.com/news-releases/news-release-details/us-food-and-drug-administration-fda-approves-lillys-onswiktm) — FDA approval date 2026-09-24. Fetch 403; used as citation target only.  
7. **Reuters AMGN 2026-09-22** (https://www.reuters.com/business/healthcare-pharmaceuticals/amgens-autoimmune-drug-succeeds-late-stage-trial-2026-09-22/) — dazodalibep is **T−2 leftover**, not a 09-24 print.  
8. **CNBC video title** (https://www.cnbc.com/video/2026/09/24/stocks-recover-slightly-from-lows-as-healthcare-outperforms.html) — “Stocks recover slightly from lows as healthcare outperforms.” Directional corroboration only (page body empty).  
9. **X posts 2026-09-24** — mixed quality; used only as color that yields/rotation were the chatter. Not used for official % (one post said XLV flat; deterministic actuals override).  
10. **Secondary quotes (UNH ~+1%, JNJ ~+0.6%, AMGN ~+0.2%, XBI ~+0.5%)** — aggregator history pages; treated as approximate outliers, not official ETF actuals.

**Not used as facts:** Benzinga bodies (403), Fear & Greed 58.2 (morning already marked stale), engine ES −0.64% as cash reality, X posts that contradicted deterministic XLV %.