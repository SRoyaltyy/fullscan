# Sector Outcome — Healthcare — 2026-09-16

Actuals: {'etf': 'XLV', 'pct': 0.06560933314379014, 'spy_pct': -0.4409916675517711, 'rel': 0.5066010006955612, 'open': 167.75, 'close': 167.77000427246094, 'source': 'yf_download'}

Memory search is paused this run (embedding index metadata missing). Review uses injected morning prediction, Channel 1 actuals, and live sources.

## 0. Facts

XLV **+0.066%** (open **167.75** → close **167.77**). SPY **−0.441%**. Relative **+0.507%**. Absolute path is **flat**; relative is a **mild defensive hold**.

Session was two-regime: pre-14:00 ET modest tech-led green (oil offered, Nasdaq as much as **+0.9%**), then a hawkish FOMC/presser fade. S&P **−0.45%**, Nasdaq **unchanged**, Dow **−1.21%**. Healthcare **flat** with utilities; energy **~−3%**, financials **~−1.6%**, tech **~+0.1%**.

CLAIM: XLV closed essentially unchanged at $167.77.  
URL: Channel 1 actuals (open 167.75 / close 167.770004)  
PUBLISHED: 2026-09-16 session  
QUOTE: ETF_PCT 0.0656; SPY_PCT −0.4410; REL_PCT +0.5066  
SUMMARY: Absolute print is flat, not an up day; relative +51 bp vs SPY.

CLAIM: S&P 500 fell 0.45% to 7551.81.  
URL: https://www.morningstar.com/news/dow-jones/202609167425/sp-500-falls-045-to-755181-data-talk  
PUBLISHED: 2026-09-16 16:28 ET  
QUOTE: “The S&P 500 Index is down 33.92 points or 0.45% today to 7551.81”  
SUMMARY: Confirms Channel 1 SPY −0.44% as the market tape.

CLAIM: Healthcare finished flat while the tape unraveled after the Warsh presser.  
URL: https://hosting.briefing.com/cschwab/InDepth/DailySectorWrap.htm  
PUBLISHED: 2026-09-16  
QUOTE: “The health care sector (flat) and utilities sector (flat) also avoided losses”  
SUMMARY: XLV’s close is a defensive hold, not a sector melt-up.

## 1. What drove the sector

**Primary driver = FOMC hawkish branch (macro / rates), not an HC spine.**

Fed hiked **25 bp to 3.75–4.00%** (12–0). Statement: inflation still elevated. Dots: **16/18** see at least one more 2026 hike; 2026 median **4.1%**. Warsh: inflation “too high, and has been for too long.” 2y **4.60% → 4.73%**; 10y **~4.95% → 5.01%**. Little reaction to the hike itself; selling started in the presser.

That is **S0 shared-macro**, not S1. No same-day CMS/MA, IRA, mega-cap Rx, or FDA-breadth event. Oil kept falling (WTI **~−3%** to ~$102); energy was the loser, not a healthcare rotation bid.

Inside XLV: large-cap pharma/low-beta held (LLY/JNJ/AMGN modest green). UNH slightly red. XBI **~+0.1%** after 09-15 **−2.27%** — **no biotech duration smash** despite hawkish dots. Morning single-names (AZN SERENA-4, AMGN IMDELLTRA) did **not** dominate.

CLAIM: FOMC hiked 25 bp to 3.75–4.00%, unanimous.  
URL: https://www.federalreserve.gov/newsevents/pressreleases/monetary20260916a.htm  
PUBLISHED: 2026-09-16 14:00 EDT  
QUOTE: “raise the target range for the federal funds rate by 1/4 percentage point to 3-3/4 to 4 percent”  
SUMMARY: The priced hike arrived; the surprise was the hawkish follow-through, not the 25 bp itself.

CLAIM: Dots and Warsh presser, not the hike, sold risk assets.  
URL: https://www.cnbc.com/2026/09/16/here-are-five-key-takeaways-from-wednesdays-fed-rate-hike.html  
PUBLISHED: 2026-09-16  
QUOTE: “Sixteen of the 18 participants expected at least one more rate hike this year.”  
SUMMARY: Hawkish path, not the decision print, flipped the afternoon.

CLAIM: Path was green into 14:00, then faded.  
URL: https://hosting.briefing.com/cschwab/InDepth/DailySectorWrap.htm  
PUBLISHED: 2026-09-16  
QUOTE: “Stocks had been mostly higher through early afternoon… Selling picked up during Mr. Warsh's subsequent press conference”  
SUMMARY: Confirms FOMC as the session binary the morning left unscored.

## 2. Audit morning S0–S4 (use morning numbers, not rewrites)

| Sleeve | Morning | Reality | Verdict |
|---|---|---|---|
| **S0 −0.5** | NQ-led risk-on + oil offered = HC is **funding source**; both FOMC branches **neg-to-neutral for XLV relative** | Pre-14:00: yes, XLV lagged tech. Post-presser: HC **held**, SPY **fell**, rel **+51 bp**. Hawkish branch was a **relative haven**, not an XBI duration hit | **Half-right on tape, wrong on relative skew** |
| **S1 0** | No fresh CMS/IRA/Rx/FDA-breadth; AZN/AMGN single-name | Confirmed. XBI flat. No insurer smash | **Correct** |
| **S2 0** | Split MAP HEAT; large-cap/low-beta, not high-beta expansion | Large-cap pharma held; XBI not leadership | **Correct** |
| **S3 0** | Trailing create, not a same-session spike | No evidence flows ran the close | **Correct** |
| **S4 0** | PM XLV +0.20% vs XLK +0.65% = lag, leftover RS not a bid | Close +0.07% vs PM +0.20% — faded to flat, not a breakout | **Correct** |

**08-13 reversal-tell (ban up/notable):** Fired correctly. Absolute was **flat**. Pipeline still printed **up/mild** via ES tape_anchor **+1.14%** + index_carry — that was the miss.

**09-11 funding-source:** True only **before** 14:00. After hawkish dots, XLV was not the funding source.

**09-14 destination:** Correctly **off** at open (NQ leading *up*). After the presser, Nasdaq faded +0.9% → flat and XLV became a *soft* destination. That inversion was **not** knowable at open.

**09-15 S0~0 risk-off complement:** Correctly off at open. Afternoon *became* risk-off; morning regime call was for the *pre-binary* tape.

**HC mag experiment / prefer flat/mild:** LLM overlay (−0.4, conf 0.42, “must not be up/notable”) was closer than engine **up/mild**. Absolute outcome = **flat**.

**Pipeline vs LLM:** Engine `predicted_direction: up` / `mild` / total **2.995** with `divergence_flagged: True`. LLM scores were S0 −0.5, rest 0, leading sum −0.5, divergence **false**. The up call was **carry + ES anchor**, not the sector stack.

## 3. Interactions / double-count / knowable-at-open

- **Oil-offered + risk-on counted once in S0:** Correct. Do not restack oil-down as “rotation into XLV.” Energy ate the oil move; XLV did not rally on it.
- **Real yields / duration:** Morning put sticky real yields in S0 and refused to restack into S1/XBI. Hawkish dots *did* lift 2y, but XBI did **not** break. Not a double-count error; the duration-hit *hypothesis* just didn’t print in XLV weights.
- **Leftover 3d/1w/1m RS:** 08-28/08-13 correctly blocked copying paid RS into S2/S4. Close did not validate an RS-breakout.
- **FOMC unscored as binary:** Right process. The *relative* haven after a hawkish presser was the unknowable piece. Morning’s “both branches neg-to-neutral for XLV relative” failed: hawkish + SPY down = **XLV relative bid**.
- **Knowable at open:** FOMC is the binary; pre-tape is tech-led; leftover RS is not an up license; no HC spine. **Not knowable:** Warsh/dots turning the afternoon into defensive-destination vs SPY.

**KNOWABLE_AT_OPEN: partially**

## 4. Outliers inside the sector

- **XBI ~+0.10%** vs morning 09-15 **−2.27%** and hawkish-dots duration fear — biotech did **not** lead *or* collapse.
- **UNH slightly red** vs **LLY/JNJ/AMGN modest green** — insurer not a smash; pharma/low-beta carried the flat ETF.
- **AZN/AMGN** morning items stayed single-name; neither ran the basket.
- **Energy −3% / financials −1.6%** are *outside* XLV but explain why relative +51 bp looks large versus a **+7 bp** absolute print.

---

OUTCOME_BEGIN
SECTOR: Healthcare
ETF: XLV
ETF_PCT: 0.066
SPY_PCT: -0.441
REL_PCT: 0.507
ACTUAL_DIRECTION: flat
ACTUAL_MAGNITUDE: flat
PRIMARY_DRIVER: Hawkish FOMC (25bp hike + Warsh/dots) flipped a tech-led morning into afternoon risk-off; XLV held flat as low-beta defense.
KEY_INTERACTION: 09-11 funding-source was true only until 14:00 ET; hawkish binary inverted it into a soft 09-14 destination vs SPY without an HC spine.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: LLM stack (S0 -0.5, rest 0, cap up/notable) beat the engine’s up/mild print on absolute; overlay wrongly expected relative drag on the hawkish branch.
OUTCOME_END

## RESEARCH APPENDIX

**Queries run**
- XLV healthcare ETF September 16 2026 performance FOMC
- stock market today September 16 2026 Fed rate decision healthcare sector
- FOMC September 16 2026 rate hike decision statement dots
- X search: XLV healthcare ETF FOMC September 16 2026 sector performance UNH LLY JNJ (2026-09-16..2026-09-17)
- XBI UNH LLY JNJ AMGN AZN stock performance September 16 2026
- healthcare sector wrap September 16 2026 XLV defensive Fed hike
- SPY close September 16 2026 percentage change
- XLV historical data September 16 2026 close 167.77
- XLK XLE XLF XLV sector ETF performance September 16 2026
- WTI crude oil close September 16 2026 102.41
- web_fetch: Fool 09-16 wrap; Fed statement; Briefing Daily Sector Wrap; CNBC takeaways; CNBC Fed decision; Morningstar S&P data talk; Benzinga sector leaders (403)

**Key sources and facts taken**
- Channel 1 actuals (injected) — XLV +0.0656%, SPY −0.4410%, rel +0.5066%, open 167.75 / close 167.77.
- Federal Reserve FOMC statement — https://www.federalreserve.gov/newsevents/pressreleases/monetary20260916a.htm — 2026-09-16 14:00 EDT — 25 bp hike to 3.75–4.00%, 12–0, inflation remains elevated.
- CNBC Fed decision — https://www.cnbc.com/2026/09/16/fed-rate-decision-september-2026.html — 16/18 dots at least one more 2026 hike; PCE 3.7% / core 3.4%; Warsh: inflation too high for too long.
- CNBC five takeaways — https://www.cnbc.com/2026/09/16/here-are-five-key-takeaways-from-wednesdays-fed-rate-hike.html — stocks green into decision, sold off in presser; 2y +7 bp; unanimous vote.
- Briefing.com Daily Sector Wrap — https://hosting.briefing.com/cschwab/InDepth/DailySectorWrap.htm — path green on oil drop + tech into early afternoon; healthcare/utilities flat; energy −3.0%; financials −1.6%; tech +0.1%; Nasdaq +0.9% then unchanged; 2y 4.60→4.73, 10y 4.95→5.01; WTI −$3.41 (−3.2%) to $102.41.
- Motley Fool wrap — https://www.fool.com/coverage/stock-market-today/2026/09/16/stock-market-today-sept-16-stocks-slip-as-fed-raises-rates/ — Nasdaq −0.01% to 25,978; S&P −0.45% to 7,551; Dow −1.21% to 51,462. (Ignore Fool’s 3.50–3.75% funds-rate line; Fed statement is 3.75–4.00%.)
- Morningstar / DJ Market Data — https://www.morningstar.com/news/dow-jones/202609167425/sp-500-falls-045-to-755181-data-talk — 2026-09-16 16:28 ET — S&P −0.45% to 7551.81.
- Investing/MarketWatch/Stockscan XLV history — close ~$167.77–167.78, range ~$167.41–$168.80, ~+0.07%.
- Name tape (Stocknear/Stockmonitor/Stockanalysis) — XBI ~+0.10%; UNH ~−0.15%; LLY modest green; JNJ ~+0.06–0.15%; AMGN ~+0.2–0.85%; AZN ~+0.5–0.87%. Treat as corroboration, not Channel 1.
- X posts 09-16 — LLY ~+0.2% cited; UNH discussed as insurer beneficiary; not used to override Channel 1.
- Benzinga sector leaders page — 403, unused.

Memory index unavailable this run (embedding metadata missing).