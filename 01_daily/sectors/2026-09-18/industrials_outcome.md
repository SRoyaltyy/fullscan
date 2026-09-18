# Sector Outcome — Industrials — 2026-09-18

Actuals: {'etf': 'XLI', 'pct': 0.4378471789928007, 'spy_pct': -0.11932509489422927, 'rel': 0.55717227388703, 'open': 169.0, 'close': 169.75, 'source': 'yf_download'}

Memory search is paused this run (embedding index metadata mismatch). Used the injected morning card, Channel 1 actuals, and live sources only.

## 0. Facts

XLI **+0.438%** (open **169.00** → close **169.75**), SPY **−0.119%**, relative **+0.557%**. Path: cash opened flat vs 09-17 close (~169.01), dipped (session low **168.48**), then grinded to **169.75** (high **169.92**). That is **up / mild**, not the morning **flat / flat**.

Overnight tape (ES **+1.14%**, PM:XLI **+0.27%**) did **not** show up as a cash gap. Midday industrials were still heavy; the green close was a late large-cap/electrical squeeze, not a four-index cyclical bid.

---

## 1. What drove Industrials today

Taxonomy: **S1 sleeve (electrical / AI-power) + S0 oil-down cost-level**, with **S1 IP print as a faded headwind**.

The 9:15 G.17 was a **miss**, not a beat: IP **unchanged (0.0%)** vs ~**+0.3%** consensus; manufacturing **−0.3%** after seven up months; construction supplies **−0.7%**; business equipment **−0.5%**; cap-util **76.3%** (unchanged, vs 76.4% consensus). LEI **−0.1%** at 10:00 was a small second drag. That package should have been XLI-negative.

It was not. The ETF still closed green because:

- **Electrical/grid reversed the morning SPLIT:** ETN ~**+3.7%**, VRT ~**+3.3%**, GEV ~**+1.7%**. Morning said this sleeve is breaking and must not drive the ETF. Today it **did** lift the close.
- **CAT ~+1.3%**, BA ~**+0.6%**, GE ~**+0.3%** — large-cap machinery/aero bid.
- **Freight/construction did not participate:** UNP ~**−1.1%**, DE ~**−0.2%**. Oil-down was **not** trucking/rail relief (09-16 rule held).
- **Oil extended the pullback** (WTI ~**$99.8**, Brent ~**$103**) — S0 cost-level easing, not a supply-shock unwind that rewrites ISM.
- **SPY slightly red** while XLI was bid → the +56 bp relative is mostly **index softness + electrical/CAT**, not industrials-led breadth.
- **Schmid** (inflation hot ex-energy, backed the 09-16 hike) and **10Y at 5.00%** were a hawkish overlay; **Bowman** was stress-testing, not a funds-path shock. Neither smashed XLI.

Midday X tape (~11:49 ET): industrials **−0.68%**, only **17/63** names green. Close-to-close XLI **+0.44%** is therefore a **narrow late rebound**, not a sector-wide rotation.

---

## 2. Audit morning S0–S4 vs reality (morning numbers, not rewritten)

| Sleeve | Morning | Reality | Verdict |
|---|---|---|---|
| **S0 = 0** | FOMC paid; oil down; futures green but **not** four-index ≥+0.5%; tech-led PM; IP/Fed speakers unscored | Oil kept falling; cash SPY **−0.12%** (overnight ES sleeve faded, as 09-17 required); IP miss; Schmid hawkish; RTY **−0.5%**, Dow **−0.2%**, Nasdaq **+0.4%** | **S0=0 held.** Not −1 (no live squeeze, no kinetic increment). Not +1 (no cyclical four-index bid). IP was correctly **unscored at the open**. |
| **S1 = 0** | No same-morning spine; ISM 54.6 carried/slowing; grid **HIT but splitting**; construction drag carried; oil-down **not** S1 relief | IP **was** the same-morning spine and it **missed**. Electrical **rallied** (opposite of the VRT-split). Rails lagged. | **Process right, internals wrong.** Capping S1 at 0 pre-print was correct. Treating grid as a wash **underweighted** the sleeve that actually moved XLI. |
| **S2 = 0** | 1m rel **−7.30%** is a **condition**; do not sign down; ~7% of S&P industrials >20d | Midday breadth **failed**; close was large-cap/electrical, not expansion | **Held.** Relative **+0.56%** is not breadth expansion. |
| **S3 = 0** | Sep 15 **−$212M** is the 1m-lag twin, not a same-morning spike | Volume ~**8.55M**; no independent forced-flow print found | **Held.** |
| **S4 = 0** | Confirmation only; do **not** promote 09-17 rel **−0.96%** into down; PM:XLI **+0.27%** vs ES **+0.20%** | Open **169.00** = no gap. Close **+0.44% / rel +0.56%** is path, not a morning tape fact | **Held as a morning rule.** The engine’s tape_anchor (**ES +1.14%**) was the trap 09-17 already forbade. |

**Predicted flat/flat vs actual up/mild:** direction **MISS**, magnitude **MISS**. Gap is small (44 bp), but it is not a flat close. The **unsigned card + size_gate + RS veto** correctly refused **up** at the open; they also refused the thing that happened (a mild relative bounce). That is a **small outcome miss with a sound flatten**, not a factor inversion.

09-17 governing rule (“keep close-to-close flat; do not promote ES gap into up; do not promote yesterday’s −0.96% rel into down”) **did its job on the two failure modes it was written for**. The miss is a **third** path: IP miss faded + electrical mean-reversion while SPY slipped.

---

## 3. Interactions / double-count / knowable-at-open

- **Oil once in S0:** correct. Do not restack as S1 freight relief — UNP **down** confirms.
- **1m lag once as condition:** correct. Promoting it to S2+S4 down would have been the **worse** miss today.
- **Paid 09-16 FOMC not restacked:** correct. Schmid is same-day commentary, not a second hike. 10Y **5.00%** is level, not a 09-15 washout.
- **IP two-sided / unscored:** correct at 09:14. After 09:15 it was a **miss** that the tape **did not honor**. Knowable only after the print.
- **Grid/VRT:** morning forbade single-ticker drive. Fair as a **call rule**; unfair as a **description of what closed the ETF**. Electrical was the live S1 interaction with a soft SPY.
- **Overnight ES +1.14% vs Finviz ES +0.20%:** morning used Finviz and still had engine tape_anchor 2.923. Cash XLI opened **flat**. Knowable: **yes, that sleeve was a fake gap** (09-17 already said so).

**Knowable at open:** oil-down continuation and “don’t buy the ES gap” — **yes**. IP miss, electrical squeeze, SPY −0.12%, XLI relative +56 bp — **no**.

---

## 4. Outliers inside the sector

- **Leaders:** ETN, VRT, GEV (electrical / AI power), CAT. This is the sleeve morning called **SPLIT / not a cushion**.
- **Laggards:** UNP (rail), DE (ag/construction machinery). Matches IP construction-supplies **−0.7%** and the carried housing-starts drag.
- **A&D:** BA modest green, RTX ~flat — already-traded ceilings (KC-46 / MQ-25) did not reprice the group.
- **Do not let VRT set the grade:** it helped the ETF, but XLI +0.44% is CAT+electrical+index-relative, not a Vertiv event.

---

## Evidence

CLAIM: August IP unchanged; manufacturing −0.3%; utilities +1.8%; mining +0.1%; cap-util 76.3%.  
URL: https://www.federalreserve.gov/releases/g17/current/  
PUBLISHED: 2026-09-18 (Last Update: September 18, 2026)  
QUOTE: “Industrial production (IP) was unchanged in August after increasing 0.2 percent in July. Manufacturing output decreased 0.3 percent in August. The index for mining ticked up 0.1 percent, and the index for utilities increased 1.8 percent. … Capacity utilization was unchanged at 76.3 percent.”  
SUMMARY: Official G.17 miss vs ~+0.3% IP / 76.4% cap-util consensus; factory output broke a seven-month streak.

CLAIM: IP 0.0% m/m missed +0.3% consensus; manufacturing −0.3%.  
URL: https://seekingalpha.com/news/4644334-industrial-production-growth-stalls-in-august-falling-short-of-expectations  
PUBLISHED: 2026-09-18, 9:17 AM ET  
QUOTE: “U.S. industrial production growth came in at 0.0% M/M in August, missing the +0.3% consensus and slowing from +0.2% in July (unrevised).”  
SUMMARY: Confirms the print as a miss immediately after 9:15.

CLAIM: LEI −0.1% in August to 99.5; first monthly decline since March.  
URL: https://www.prnewswire.com/news-releases/the-conference-board-leading-economic-index-lei-for-the-us-edged-down-in-august-302883352.html  
PUBLISHED: 2026-09-18  
QUOTE: “The Conference Board Leading Economic Index® (LEI) for the US decreased by 0.1% in August 2026 to 99.5 (2016=100), after an increase of 0.2% in July.”  
SUMMARY: Secondary 10:00 print, slight growth-scare, not a smash.

CLAIM: Mixed indexes; 10Y at 5.00%; Brent briefly < $102 then back > $103.  
URL: https://wtop.com/national/2026/09/how-major-us-stock-indexes-fared-friday-9-18-2026/  
PUBLISHED: 2026-09-18  
QUOTE: “The S&P 500 rose 0.2% Friday. The Dow Jones Industrial Average slipped 0.2%, and the Nasdaq composite added 0.4%. … the yield on the 10-year Treasury climbed to 5.00%.”  
SUMMARY: Tech-led, small-cap/Dow red; use pipeline SPY −0.119% for the grade, not SPX +0.2%.

CLAIM: Schmid backed the hike; inflation hot even ex-energy.  
URL: https://www.morningstar.com/news/dow-jones/202609185048/inflation-running-too-hot-even-without-oil-feds-schmid-says  
PUBLISHED: 2026-09-18 12:32 ET  
QUOTE: “A broad range of goods and services are showing price growth inconsistent with our price stability target.”  
SUMMARY: Hawkish overlay, not a new FOMC decision; Bowman speech was stress-testing.

CLAIM: Bowman previewed stress-test finalization, not rates.  
URL: https://www.federalreserve.gov/newsevents/speech/bowman20260918a.htm  
PUBLISHED: 2026-09-18  
QUOTE: “In the coming weeks, the Federal Reserve Board will consider final revisions to the Board's stress testing framework.”  
SUMMARY: Calendar item resolved as non-XLI-spine.

CLAIM: XLI cash path 169.00 → 169.75, volume ~8.55M.  
URL: https://finance.yahoo.com/quote/XLI/history?p=XLI  
PUBLISHED: 2026-09-18 session  
QUOTE: open 169.00, high 169.92, low 168.48, close 169.75, volume 8,547,135  
SUMMARY: Matches Channel 1 open/close; dip-then-grind, not a gap-and-go.

---

OUTCOME_BEGIN
SECTOR: Industrials
ETF: XLI
ETF_PCT: 0.438
SPY_PCT: -0.119
REL_PCT: 0.557
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Electrical/AI-power rebound (ETN/VRT/GEV) plus CAT while SPY slipped; the 9:15 IP miss was faded, not honored.
KEY_INTERACTION: Oil-down cost-level (S0) and a narrow electrical/machinery bid offset an IP manufacturing/construction miss (S1) without a freight or breadth confirmation.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Flat/flat was the right unsigned open (don’t buy ES, don’t short the 1m lag); actual up/mild is a small dir+mag miss from a late electrical squeeze, not a factor-card inversion.
OUTCOME_END

---

## RESEARCH APPENDIX

**Queries run**
- US industrial production August 2026 September 18 capacity utilization
- XLI industrials ETF September 18 2026 stock market industrial production
- stock market September 18 2026 industrials oil Fed Bowman Schmid
- X search: XLI industrials industrial production stocks September 18 2026 (2026-09-18 to 2026-09-19)
- Conference Board LEI August 2026 September 18
- XLI top holdings CAT GE RTX UNP HON BA September 18 2026
- S&P 500 sector performance September 18 2026 industrials technology energy
- Caterpillar GE Aerospace RTX Boeing Deere Union Pacific stock September 18 2026
- oil prices WTI Brent close September 18 2026
- Federal Reserve industrial production August 2026 G.17 unchanged manufacturing -0.3
- Fed Schmid backed rate hike inflation beyond energy September 18 2026
- GE Vernova Vertiv Eaton stock September 18 2026
- site:federalreserve.gov industrial production August 2026 G.17
- industrial production August 2026 0.0 percent manufacturing output -0.3 capacity utilization 76.3
- XLI ETF September 18 2026 close 169.75 volume
- memory_search: Industrials XLI sector prediction outcome 2026-09-18 industrial production (index unavailable)

**Key sources (title + URL + timestamp/facts taken)**

1. Fed G.17 current release — https://www.federalreserve.gov/releases/g17/current/ — 2026-09-18. IP unchanged; mfg −0.3%; mining +0.1%; utilities +1.8%; IP 103.1, +1.4% YoY; cap-util 76.3%; construction supplies −0.7%; business equipment −0.5%; mfg cap-util 75.7%.
2. Seeking Alpha IP recap — https://seekingalpha.com/news/4644334-industrial-production-growth-stalls-in-august-falling-short-of-expectations — 2026-09-18 9:17 AM ET. 0.0% vs +0.3% consensus; July +0.2% unrevised.
3. Conference Board LEI PR — https://www.prnewswire.com/news-releases/the-conference-board-leading-economic-index-lei-for-the-us-edged-down-in-august-302883352.html — 2026-09-18. LEI −0.1% to 99.5; CEI +0.1%; first LEI decline since March.
4. WTOP index wrap — https://wtop.com/national/2026/09/how-major-us-stock-indexes-fared-friday-9-18-2026/ — 2026-09-18. SPX +0.2% to 7650.50; Dow −0.2%; Nasdaq +0.4%; RTY −0.5%; 10Y 5.00%.
5. Morningstar/DJ Schmid — https://www.morningstar.com/news/dow-jones/202609185048/inflation-running-too-hot-even-without-oil-feds-schmid-says — 2026-09-18 12:32 ET. Backed 09-16 hike; inflation hot ex-energy.
6. Fed Bowman speech — https://www.federalreserve.gov/newsevents/speech/bowman20260918a.htm — 2026-09-18. Stress-test transparency/SCB averaging; not a rate call.
7. Yahoo XLI history — https://finance.yahoo.com/quote/XLI/history?p=XLI — 2026-09-18. O/H/L/C 169.00 / 169.92 / 168.48 / 169.75; volume 8,547,135.
8. Channel 1 actuals (pipeline, do not alter) — XLI +0.4378%, SPY −0.1193%, rel +0.5572%; open 169.0 close 169.75.
9. Reuters IP (search citation; page JS-blocked on fetch) — https://www.reuters.com/business/us-manufacturing-output-falls-august-after-rising-seven-straight-months-2026-09-18/ — used only as corroboration of mfg −0.3% / seven-month streak; primary numbers taken from Fed G.17.
10. X posts 2026-09-18 — midday industrials −0.68% (17/63 green); later XLI described as a modest leader vs SPY −0.1% / QQQ +0.8%. Path color, not a price source.

**Facts used vs discarded**
- Used: pipeline XLI/SPY/rel; Fed G.17 print; LEI −0.1%; oil still offered into the close; Schmid/Bowman split; electrical vs rail divergence; open-flat then grind.
- Discarded / not re-derived: SPX +0.2% as the SPY actual (pipeline SPY −0.119% governs); overnight ES +1.14% as the cash session; oil-down as S1 trucking relief; VRT as the ETF call; paid 09-16 FOMC as a second S0 shock.