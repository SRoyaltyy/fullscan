# Sector Outcome — Industrials — 2026-09-24

Actuals: {'etf': 'XLI', 'pct': -0.7466221204531109, 'spy_pct': -0.0820521854125067, 'rel': -0.6645699350406042, 'open': 168.75, 'close': 168.8300018310547, 'source': 'yf_download'}

OUTCOME_BEGIN
SECTOR: Industrials
ETF: XLI
ETF_PCT: -0.7466221204531109
SPY_PCT: -0.0820521854125067
REL_PCT: -0.6645699350406042
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Hawkish yields/Fed-tightening tape plus oil-up cost shock sold cyclicals; SPY was rescued by energy/comms, not by XLI.
KEY_INTERACTION: Score-vs-tape flatten used a stale PM:XLI 0% veto against a live ES/yields/oil stack; the “tech-sold, XLI-shield” relative lean inverted when energy/XLC held the index.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Pipeline down/mild HIT; LLM flat/flat + relative-outperformance lean MISS — S0 was right, the tape veto and S2 shield were not.
OUTCOME_END

## 0. Facts

XLI **−0.75%**, SPY **−0.08%**, relative **−0.66%**. Absolute **down / mild**. Path: **gap-down open that stuck**. Open **168.75** vs close **168.83** is only **+0.05%** cash-session drift; almost the entire loss was the overnight gap vs ~**170.10** prior close. This is the inverse of 09-22 (PM gap died at the cash open). Today the **flat PM quote died at the cash open**.

Memory index was unavailable this run (embedding metadata mismatch). Review uses the injected morning card, deterministic actuals, and live sources below.

## 1. What drove the sector

Taxonomy-aligned, in order:

**S0 — shared macro (dominant, confirmed).** Hawkish-repricing / real-yield backup transmitted into cyclicals. 10Y stayed on the **~5.11–5.15%** shelf (19-year high neighborhood); 30Y near **5.41%** (highest since 2004). Same-session **Philly Fed Paulson** (voting) said modest further tightening may be warranted. Oil **up** on US–Iran / Middle East supply risk — a **cost LEVEL plus a same-session increment** for freight, aviation, and machinery. SPY only barely red because **energy and communication services** offset; industrials did not.

CLAIM: 10Y yield hovered near multiyear highs after Wednesday’s 5.14% print; 30Y near 5.41%.
URL: https://www.morningstar.com/news/dow-jones/202609241547/us-treasury-yields-hover-close-to-multiyear-highs-update
PUBLISHED: 2026-09-24 04:04 ET
QUOTE: “The 10-year Treasury yield, which hit a 19-year high of 5.14% on Wednesday, last traded up 0.6 basis points at 5.119%… The 30-year yield, which rose to 5.415% on Wednesday, last traded 1.3 basis points higher at 5.414%.”
SUMMARY: Pre-US-cash confirmation that the yield backup was still the live macro.

CLAIM: Voting Fed speaker Paulson endorsed further modest hikes on 09-24.
URL: https://www.reuters.com/business/feds-paulson-says-more-rate-hikes-may-be-needed-quash-inflation-2026-09-24/
PUBLISHED: 2026-09-24
QUOTE: “if conditions evolve as I expect, some modest further tightening may be warranted”
SUMMARY: Same-session hawkish add-on to the Warsh regime already on the morning card.

CLAIM: Wall Street mixed-to-down on oil + yields; energy bid, cyclicals not.
URL: https://www.reuters.com/business/wall-st-futures-fall-middle-east-uncertainties-ahead-trump-xi-talks-2026-09-24/
PUBLISHED: 2026-09-24
QUOTE: (search extract) S&P nearly unchanged (~−0.03%); Dow ~−0.3%; oil/yields the named drivers; energy gained.
SUMMARY: Index resilience was **not** an industrials bid. Matches SPY −0.08% vs XLI −0.75%.

CLAIM: Mid-morning tape: indexes red, energy/healthcare/comms bid, tech/materials lag; oil up; 10Y ~5.09%.
URL: https://www.eoption.com/mid-morning-look-september-24-2026/
PUBLISHED: 2026-09-24
QUOTE: “U.S. stock markets opened at the lows following another day of higher oil prices, rising Treasury yields… Healthcare, Energy and Communications sectors while technology and Materials lag.”
SUMMARY: Breadth inside the market was **not** “tech dumped, cyclicals shielded.” Energy/comms held the index; cyclicals lagged.

**S1 — spine (not the driver).** No same-morning industrials hard print (durable goods is **09-25**). Superheated **Sep flash PMIs** (composite 58.4, mfg 57.0) were **T−1** and fed the **yield** channel, not an XLI order-book bid. New-home-sales beat (+6.4% to 684k) was a same-session construction positive that **did not** lift XLI. GEV/grid stayed a single-name cushion (GEV ~**+0.34%**) and did **not** raise the ETF — 08-18 held.

**S2 — breadth (failed the morning shield).** XLI was a **laggard**, not mid-pack. Reports had industrials ~**−0.64% to −0.79%**, energy **+~1%**, 8/11 sectors down. Inside the book: CAT ~−1%, BA ~−1.5%, UNP ~−0.5%, GEV green. Not a mega-name washout; **broad cyclical fade**.

**S3 — positioning.** 1m rel **−5.90%** was a condition, correctly scored once. The “stabilizing” 1d/09-23 relative bid **reversed**. Rotation-out **reasserted**.

**S4 — tape.** Convert-the-gap needed a worse-than-index PM gap. Morning measured ~**20 bp**. Cash open was ~**−0.79%** vs prior close. The PM:XLI **−0.00%** leg (weight 0.7 in the tape anchor) was the wrong open.

## 2. Audit of morning S0–S4 (morning numbers, not rewritten)

| Sleeve | Morning | Reality | Verdict |
|---|---|---|---|
| **S0 = −1** | Warsh/hikes live, 10Y 19y high, 5d yield-equity corr −0.826, ES −0.64% / NQ −1.09%, oil counted once, not −2 because VIX 16 / HY tight / operator tape green | Yields stayed extreme; Paulson added; oil up; XLI sold; SPY barely red | **HIT, and the −1 was the only sleeve that mattered.** Not −2 was fair for SPY; too kind for XLI. |
| **S1 = 0** | No same-morning print; GEV barred; freight zero; tariffs secondary | Correct empty spine. Oil-up cost was already in S0. Home-sales beat did not save the ETF. | **HIT as a zero.** |
| **S2 = 0** | PM:XLI −0.00% mid-pack; XLK −1.51% worst ⇒ “relative-shield” | XLI −0.75% vs SPY −0.08%; industrials among laggards | **MISS.** Shield was a PM-board artifact. |
| **S3 = 0** | 1m lag scored once; 1d rel +0.81% / 09-23 +0.79% = stabilizing | Two-day relative bid died | **Condition scoring HIT; “stabilizing” read MISS.** |
| **S4 = 0** | 09-14 stack off (gap only ~20 bp vs ES) | Cash open **was** the gap; PM quote was not | **MISS on confirmation.** Same failure mode as 09-22, opposite sign. |

LLM self-audit said leading_sum = **−1** ⇒ shrink to **flat/flat**, divergence **FLAGGED**, relative-outperformance lean. Pipeline kept **down/mild** (total **−4.289**, tape_anchor **−0.774**, index_carry **−1.915**, overlay **−1.6**).

**Official recorded call = pipeline down/mild → dir HIT, mag HIT.**
**LLM overlay = flat/flat + beat-SPY lean → dir MISS, mag MISS, relative MISS.**

## 3. Interactions / double-count / knowable-at-open

**Double-count:** Clean. Oil once (S0). Yields/Warsh once (S0). 1m lag once (S3). GEV not allowed to lift. No CapEx-cut reprint of the hike.

**The actual error was a veto, not a double-count.** DO-INSTEAD (“score sign fights tape ⇒ flatten”) treated **PM:XLI −0.00% + two-day relative bid** as the tape. The live tape was **ES −0.64%** and a **gap-down cash open**. 09-22 forbade minting down from a PM gap that dies at the open; today they forbade minting down because a **flat PM** died at the open. That is applying 09-22 **backwards**.

**09-23 relative lean** (1m rel ≤ −5% + carried rotation-out + non-sector leadership + flat PM) fired only **partially** (leadership was **energy**, not a growth complex). The lean still went out. Reality: **rel −0.66%**. Overfit.

**Knowable at open: partially.**
- Absolute down from S0 + ES sleeve: **yes**.
- Mild band: **yes**.
- “XLI outperforms a falling SPY”: **no**. SPY did not fall; XLI did. Energy/comms rescue of the index was not on the PM board (XLK was the overnight worst, which manufactured the shield).
- Paulson: **not** knowable at the open (same-session). Fed-speaker lesson would have kept *his* increment at 0; it does not erase the already-live Warsh/yields regime.

## 4. Outliers inside the sector

- **GEV ~+0.34%** — AI-power/grid sleeve held. 08-18 correctly forbade this from raising XLI. ETF still −0.75%.
- **BA ~−1.5%** — worse than the ETF; aerospace did not offset.
- **CAT ~−0.8% to −1.2%** — machinery followed the cyclical fade.
- **UNP ~−0.5%** — freight milder than the ETF, not a volume-recovery HIT.
- **KNF +4%** (Starboard) — single-name activist, ignore for the ETF.

No evidence a single mega-name *caused* the XLI print. It was **broad cyclical underperformance vs a near-flat SPY**.

## Lesson (for the next industrials card)

When ES/NQ sleeve is red, yields are at regime highs, and oil is up, **do not let a flat PM:XLI quote or a two-day relative bounce veto S0**. Score-vs-tape flatten needs a **cash-open** tape, not a PM quote that has already been wrong twice in three sessions (09-22 PM gap died long; 09-24 PM flat died short). 1m rel ≤ −5% is still a **condition**, not a relative-outperformance coupon. Relative lean requires the index actually being sold in the **same complex XLI is supposed to shield against** — energy leadership is not that complex.

---

## RESEARCH APPENDIX

**Queries run**
- web_search: `XLI industrials ETF September 24 2026 market news`
- web_search: `stock market September 24 2026 Treasury yields Warsh industrials`
- web_search: `XLI vs SPY September 24 2026 why industrials fell`
- web_search: `September 24 2026 S&P 500 close sector performance energy technology industrials`
- web_search: `US Treasury 10-year yield September 24 2026 close`
- web_search: `8 of 11 sectors fall Thursday September 24 2026 XLI XLE XLK`
- web_search: `Caterpillar GE Vernova Boeing Union Pacific stock September 24 2026`
- web_search: `S&P Global PMI September 24 2026 flash manufacturing services`
- web_search: `Kevin Warsh Fed hike September 24 2026 stocks industrials yields`
- web_search: `XLI close September 24 2026 Industrial Select Sector SPDR`
- web_search: `Philadelphia Fed Paulson additional rate hikes September 24 2026`
- web_search: `site:reuters.com Wall Street September 24 2026 oil yields`
- x_search: market/XLI/yields/Warsh movers 2026-09-24 to 2026-09-25
- x_search: XLI vs SPY / CAT BA GEV 2026-09-24 to 2026-09-25
- web_fetch: Morningstar/DJ yields update; eOption mid-morning look; Motley Fool Warsh piece; S&P Global PMI PDF (binary, unused); Reuters/Benzinga/Bloomberg/Seattle Times/FMT blocked or empty
- memory_search: unavailable (index metadata mismatch)

**Key sources and facts taken**

1. **Deterministic actuals (injected)** — XLI −0.7466%, SPY −0.0821%, rel −0.6646%, open 168.75, close 168.83. Path = gap-down, cash flat.
2. **Morningstar / Dow Jones Newswires, 2026-09-24 04:04 ET** — https://www.morningstar.com/news/dow-jones/202609241547/us-treasury-yields-hover-close-to-multiyear-highs-update — 10Y 5.119% after 5.14% 19-year high; 30Y 5.414% after 5.415%; flash PMIs cited as the Wednesday accelerator; Barr “further rate increases likely needed.”
3. **eOption Mid-Morning Look, 2026-09-24** — https://www.eoption.com/mid-morning-look-september-24-2026/ — opened at lows on oil + yields; energy/healthcare/comms bid, tech/materials lag; Paulson modest-further-tightening quote; jobless claims 197k; new home sales +6.4% to 684k.
4. **Reuters (search + URL)** — https://www.reuters.com/business/wall-st-futures-fall-middle-east-uncertainties-ahead-trump-xi-talks-2026-09-24/ — S&P ~flat, Dow ~−0.3%, oil/yields/Middle East; energy gained. Paulson piece: https://www.reuters.com/business/feds-paulson-says-more-rate-hikes-may-be-needed-quash-inflation-2026-09-24/
5. **Benzinga sector wrap (search extract; page 403)** — https://www.benzinga.com/etfs/sector-etfs/26/09/61981197/8-of-11-sectors-fall-in-thursday-trading-as-leaders-split and https://www.benzinga.com/etfs/sector-etfs/26/09/61970129/leading-and-lagging-sectors-september-24-2026 — 8/11 sectors down; XLE ~+1%; XLI ~−0.64% to −0.79%; XLK ~−0.85% to −1.27%.
6. **Seattle Times index recap (search extract)** — https://www.seattletimes.com/business/how-major-us-stock-indexes-fared-thursday-9-24-2026/ — S&P ~7704 (−0.02% area); Dow ~−0.3%.
7. **Name-level closes (search extracts)** — CAT ~−0.8% to −1.2%; GEV ~+0.34% to ~$955; BA ~−1.5%; UNP ~−0.5%.
8. **S&P Global flash PMI (search extract; official PDF fetched as binary)** — https://www.pmi.spglobal.com/Public/Home/PressRelease/ed177f50167b4203ac490a961ea706be — Sep flash composite 58.4 / mfg 57.0 / services 58.7 (released 09-23, yield-relevant T−1).
9. **Motley Fool, 2026-09-24** — https://www.fool.com/investing/2026/09/24/if-kevin-warsh-keeps-rates-elevated-these-2-stocks/ — Warsh/FOMC already hiked 25 bp to 3.75–4.00%; market pricing at least one more hike.
10. **X search (09-24/09-25)** — yield-driven mixed tape; XLI ~−0.7% in one snapshot; energy relative bright spot; no single industrial headline dominating CAT/BA/GEV.

**Not used as facts:** eOption WTI **$94.09** (conflicts with morning Finviz ~$104 and Reuters Brent ~$107) — direction (oil **up**) kept, level discarded. Bloomberg industrials-technical piece 403’d; treated only as a contemporaneous headline that the 1m slide was live, not as a same-session print.