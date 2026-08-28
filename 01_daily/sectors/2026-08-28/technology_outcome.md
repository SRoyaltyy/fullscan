# Sector Outcome — Technology — 2026-08-28

Actuals: {'etf': 'XLK', 'pct': -1.5481672018960002, 'spy_pct': -0.2269485222937795, 'rel': -1.3212186796022207, 'open': 188.07000732421875, 'close': 185.69000244140625}

Workspace bootstrap cannot finish in this automated XLK review (no human identity conversation). Memory index is unavailable this run; used injected Technology logs/lessons only.

## 0. Facts

XLK cash session 2026-08-28: **open 188.07 → close 185.69**, **−1.548%**. SPY **−0.227%**. Relative **−1.321%**. Implied prior close ~188.61, so the open was already a small gap-down (~−0.29%), then the ETF sold through the cash session (Warsh ~10:00 ET window). Path = slight soft open → further downside, not a gap-and-go crash.

**ACTUAL_DIRECTION = down.** **ACTUAL_MAGNITUDE = notable** (absolute 1.55% sits on the mild/notable line; SOX/SOXX and NVDA make the *sector environment* clearly more than a flat fade). Nasdaq Composite **−0.52%**; S&P 500 **−0.25%** — tech lagged the tape, it did not merely ride beta.

## 1. What drove the sector

Taxonomy: **S0 policy/duration shock**, amplified by **S1 hardware leadership unwind** after a +2.50% rel day, not a broken AI-demand spine.

Warsh’s first Jackson Hole keynote as Chair (~10:00 ET) was the load-bearing print. He kept 2% PCE as a “firm, fixed target,” said summer CPI/PCE were “better than expected” but **do not show meaningful improvement in underlying trends**, and: “We must be confident that underlying inflation is moving to our objective, clearly and at sufficient speed. Otherwise, we have work to do.” Markets heard hike optionality, not Powell-style easing. 2Y **+~8 to +12 bp** (CNBC ~4.31%; later wrap 4.356%), 10Y **+5.4 bp to 4.726%**, CME Sept hike odds **~35–40% → ~55–57%**. That is a duration tax on long-duration AI hardware, not a new foundry/HBM/capex kill. Warsh even described AI infra capex as a growth impulse — the *fundamental* S1 spine was not retracted.

Hardware vs software split confirms the taxonomy: NVDA **−4.57%** to $217.55 (gave back more than half of Thursday’s ~+8.7%); SOX **−3.47%**; SOXX **−3.20%**; AMD **−2.33%**; INTC **−2.85%**. MSFT **+1.68%**, AAPL **+1.63%**, ADBE **+0.82%**, GOOGL **+1.74%**, AMZN **+3.97%**. XLK fell because it is NVDA/semi-heavy, not because “tech” as a whole was bidless.

Secondary, same-window: Chicago PMI **47.1** vs ~58 cons (9:45 ET, weakest of the year) — a growth miss in the same hour, **not** NFP/CPI-class and not the thing that repriced the 2Y. MRVL **−10.28%** after Thursday AH (beat/raise but GM guide 57.5–58.5% nGAAP and Google custom upside pushed to FY29+) — intra-semi outlier, not an XLK-weight analog of NVDA.

## 2. Audit of morning S0–S4 (use morning numbers, do not rewrite them)

Morning scores (unaltered): **S0=0, S1=+1, S2=0, S3=0, S4=0**, mult **0.9**, pipeline **up / flat**, narrative “absolute flat, not up/notable and not down.” NQ **−0.19%** (Finviz **−0.29%**). Divergence flagged in prose, not in the JSON.

| Sleeve | Morning | vs reality | Verdict |
|---|---|---|---|
| **S0** | 0. Sticky PCE already Thursday; Warsh two-sided; NQ not ≤−0.5%; real-yield impulse +2 bp | Binary resolved **hawkish in cash hours**. 2Y jump is the session’s macro. S0=0 was fair *before* 10:00; **mega-cap-over-macro-drag forbidding down** treated an unresolved policy event as a direction floor. | **Miss on mapping.** Neutral-until-print was ok; the down-veto was not. |
| **S1** | +1 carried AI-infra (NVDA $96.2B / Q3 $108B). One cluster, not three. | Spine still intact (Warsh even cited AI capex). Price failed because of duration + day-2 NVDA, not peak-spend. +1 was too sticky for a second day after **rel +2.50%**. | **Factor right, score too high for *today*.** |
| **S2** | 0. Kospi −1.79%, NQ slightly red, don’t restack Thursday leadership | Confirmed: semis led down, software/AAPL/MSFT up. Not “ETF up / names flat”; it was **hardware leadership reversal**. | **Hit.** |
| **S3** | 0. Crowding #1 at 53%; no inflow spike; Warsh = supply risk | Crowded long + hawkish speech = the supply. Flows were not the impulse. | **Hit as dampener; did not need to be −1 at the open.** |
| **S4** | 0. Do not restack Thursday +2.50% rel; NQ inside ±0.5% | Soft open then cash selloff. Tape vetoed magnitude *and*, once Warsh printed, direction. | **Hit on “don’t boost”; miss if S4 was used as a down-floor.** |

Lessons: **08-14 scheduled-data** (cap mild, include flat/fade) was the right instinct. **08-13 / 08-12 notable-off** correctly blocked notable-*up*. **08-27 timestamp gate** correctly refused to re-trade NVDA/PCE. **After >+1% rel day** correctly refused a fresh positive. **mega-cap-over-macro-drag** **failed as a close rule** — it is an *open* constraint only.

## 3. Interactions / double-count / knowable-at-open

**One shock, not three:** Warsh hawkish + 2Y spike + hike-odds jump = **one S0**. Do not also score “real yields rising” and “USD/duration” as extra sector hits.

**One hardware unwind, not three spines failing:** NVDA −4.6% + SOX −3.5% + XLK −1.55% = **NVDA weight + semi beta**, not capex + HBM + foundry independently dying. Morning was right not to triple-count the AI cluster; the error was leaving S1 at +1 into a known two-sided 10:00.

**MRVL is not XLK.** −10% is an outlier (custom-silicon/GM narrative), knowable from **Thursday AH**, under-weighted in the morning packet.

**Chicago PMI ≠ Warsh.** Same clock window; PMI is a noisy regional print. Yields and FedWatch moved on the speech.

**Knowable at open:** Warsh pending (yes); NQ slightly red (yes); Thursday extension already in the tape (yes); crowded long (yes); MRVL AH slide (yes, and missed); hawkish resolution / +8–12 bp 2Y / 55%+ hike odds (no); PMI 47.1 (no). **→ partially.**

## 4. Outliers inside the sector

- **NVDA −4.57%**: index-relevant mean-reversion after +8.7%, not a new fundamental miss.
- **MRVL −10.28%**: earnings-quality/GM/timing disappointment; SOX-relevant, XLK-secondary.
- **MSFT +1.68% / AAPL +1.63% / AMZN +3.97%**: mega-cap ex-NVDA **diverged up** — forbids a “tech risk-off” blanket.
- **MU −0.27%**: memory held vs logic/AI hardware.
- **IonQ −7.7%** and other quantum: high-duration junk beta, not XLK.

---

**CLAIM:** XLK −1.548% (open 188.07 / close 185.69) vs SPY −0.227%, rel −1.321%.  
**URL:** injected Channel 1 actuals (this run)  
**PUBLISHED:** 2026-08-28 session  
**QUOTE:** OPEN 188.07000732421875 CLOSE 185.69000244140625  
**SUMMARY:** Soft open, cash-session selloff; tech lagged SPY.

**CLAIM:** Warsh: inflation trends not meaningfully improved; “otherwise, we have work to do.”  
**URL:** https://www.federalreserve.gov/newsevents/speech/warsh20260828a.htm  
**PUBLISHED:** 2026-08-28 ~10:00 ET  
**QUOTE:** “We must be confident that underlying inflation is moving to our objective, clearly and at sufficient speed. Otherwise, we have work to do.”  
**SUMMARY:** Hawkish-optionality speech, no tactical easing, quieter Fed / no forward guidance.

**CLAIM:** Same quote + 2Y ~+8 bp to 4.31%; Sept hike odds to 55.7%.  
**URL:** https://www.cnbc.com/2026/08/28/kevin-warsh-jackson-hole-federal-reserve-inflation.html  
**PUBLISHED:** 2026-08-28  
**QUOTE:** “Stock market indexes climbed after digesting the speech… while Treasury yields moved substantially higher. The policy-sensitive 2-year note soared nearly 8 basis points… to 4.31%.”  
**SUMMARY:** Bond market heard hike risk; equities mixed, yields the clean tell.

**CLAIM:** NVDA −4.57% to $217.55; Nasdaq −0.52%; MSFT/AAPL green; MRVL −10.28%; SOXX −3.20%.  
**URL:** https://en.fnnews.com/news/202608290528098266  
**PUBLISHED:** 2026-08-29 05:31 KST wrap of 08-28 close  
**QUOTE:** “NVIDIA Corporation slid to $217.55, down $10.43, or 4.57%… Marvell Technology plunged $24.83, or 10.28%, to $216.62.”  
**SUMMARY:** Hardware led XLK down; big-tech ex-NVDA mostly gained.

**CLAIM:** MRVL Q2 record $2.739B, nGAAP GM 58.9%; Q3 nGAAP GM guided 57.5–58.5%; FY27/FY28 raises.  
**URL:** https://investor.marvell.com/news-events/press-releases/detail/1031/marvell-technology-inc-reports-second-quarter-of-fiscal-year-2027-financial-results  
**PUBLISHED:** 2026-08-27 (call 1:45 p.m. PT)  
**QUOTE:** “Q2 Gross Margin: 53.1% GAAP… 58.9% non-GAAP… Non-GAAP gross margin is expected to be 57.5% to 58.5%.”  
**SUMMARY:** Beat-and-raise with sequential GM pressure; stock still sold Friday.

**CLAIM:** MRVL ~−7% AH Thursday as Google custom upside pushed to FY29+.  
**URL:** https://www.morningstar.com/news/marketwatch/20260827426/marvell-boosts-its-forecasts-but-the-stock-slides-as-wall-street-wonders-if-theres-more-to-the-story  
**PUBLISHED:** 2026-08-27 18:13 ET  
**QUOTE:** “Shares, though, were trading about 7% lower in Thursday's extended session.”  
**SUMMARY:** Knowable-before-open semi outlier, not in morning S1.

**CLAIM:** Chicago PMI 47.1, weakest of the year.  
**URL:** https://www.cnbc.com/video/2026/08/28/august-chicago-pmi-comes-in-at-47-point-1-the-weakest-number-for-the-year.html  
**PUBLISHED:** 2026-08-28 9:52 AM EDT  
**QUOTE:** “August Chicago PMI comes in at 47.1, the weakest number for the year.”  
**SUMMARY:** Same-window miss; secondary to Warsh/yields.

OUTCOME_BEGIN
SECTOR: Technology
ETF: XLK
ETF_PCT: -1.548
SPY_PCT: -0.227
REL_PCT: -1.321
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Hawkish Warsh Jackson Hole (inflation not improving; hike door open) taxed duration-sensitive AI hardware after Thursday’s NVDA squeeze.
KEY_INTERACTION: One S0 policy-binary resolution × day-2 crowded NVDA/SOX mean-reversion — not a broken AI-demand spine; MSFT/AAPL diverged higher.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Pipeline up/flat missed direction; narrative “flat not down” was closer, but S0=0 plus mega-cap-over-macro-drag wrongly forbade the fade 08-14 had already flagged.
OUTCOME_END

## RESEARCH APPENDIX

**Queries run**
- memory_search: Technology XLK sector prediction lessons NVDA Warsh Jackson Hole 2026-08-28 — **unavailable** (index metadata missing)
- memory_search: XLK notable gate mega-cap-earnings-over-macro-drag 08-12 08-13 08-14 — **unavailable**
- web_search: XLK technology sector stocks August 28 2026 Nvidia Warsh Jackson Hole
- web_search: Kevin Warsh Jackson Hole speech August 28 2026 Fed Chair market reaction
- web_search: Nasdaq XLK close August 28 2026 Nvidia semiconductor stocks
- x_search: XLK Nvidia Nasdaq Warsh Jackson Hole August 28 2026 market reaction (2026-08-28 to 2026-08-29)
- web_search: Chicago PMI University of Michigan August 28 2026
- web_search: NVDA AMD AVGO MSFT AAPL CRM software semiconductor August 28 2026
- web_search: SOX semiconductor index August 28 2026 Nvidia Broadcom AMD Marvell
- web_search: 2-year Treasury yield August 28 2026 Warsh Jackson Hole hike odds
- web_search: site:reuters.com Warsh Jackson Hole stocks tech August 28 2026
- web_search: Marvell earnings August 28 2026 MRVL stock drop margin
- web_search: Microsoft Apple Salesforce Adobe XLK August 28 2026 close
- web_search: Chicago PMI 47.1 August 28 2026 weakest
- web_fetch: Forbes Warsh; Fed speech; CNBC Warsh; InvestmentNews; FN News wrap; Marvell IR; Morningstar/MarketWatch MRVL; CNBC Chicago PMI video
- web_fetch failed/blocked: WSJ live, NYT, Reuters live, NDTV Profit, Zacks (bot wall)

**Key sources (title + URL + timestamp)**
- Fed Board, *Keynote remarks by Chairman Warsh*, 2026-08-28 ~10:00 ET — https://www.federalreserve.gov/newsevents/speech/warsh20260828a.htm
- Forbes/AP, *Fed’s Kevin Warsh Says Inflation Still Too High At Jackson Hole*, 2026-08-28 — https://www.forbes.com/sites/tylerroush/2026/08/28/fed-chair-kevin-warsh-says-inflation-still-too-high-in-first-jackson-hole-speech/
- CNBC, *Fed Chairman Warsh expresses concern about inflation…*, 2026-08-28 — https://www.cnbc.com/2026/08/28/kevin-warsh-jackson-hole-federal-reserve-inflation.html
- InvestmentNews, *No Fed relief at Jackson Hole: Warsh keeps rate-hike door open*, 2026-08-28 — https://www.investmentnews.com/equities/no-fed-relief-at-jackson-hole-warsh-keeps-rate-hike-door-open/267991
- Financial News (AP wrap), *Stocks fall… Big tech mostly gains*, updated 2026-08-29 05:31 — https://en.fnnews.com/news/202608290528098266
- Marvell IR, Q2 FY27 results, 2026-08-27 — https://investor.marvell.com/news-events/press-releases/detail/1031/marvell-technology-inc-reports-second-quarter-of-fiscal-year-2027-financial-results
- MarketWatch via Morningstar, *Marvell boosts its forecasts, but the stock slides…*, 2026-08-27 18:13 ET — https://www.morningstar.com/news/marketwatch/20260827426/marvell-boosts-its-forecasts-but-the-stock-slides-as-wall-street-wonders-if-theres-more-to-the-story
- CNBC video, *August Chicago PMI comes in at 47.1…*, 2026-08-28 9:52 AM EDT — https://www.cnbc.com/video/2026/08/28/august-chicago-pmi-comes-in-at-47-point-1-the-weakest-number-for-the-year.html
- Injected Channel 1 actuals: XLK −1.548% / SPY −0.227% / rel −1.321%, open 188.07 close 185.69

**Facts taken**
- Warsh delivered (was pending at the morning snapshot); hawkish on inflation, no easing guidance; 2Y +8–12 bp; Sept hike odds ~55–57%.
- XLK −1.55% vs SPY −0.23%; Nasdaq −0.52% / SPX −0.25% / Dow ~flat.
- NVDA −4.57% after Thursday ~+8.7%; SOX ~−3.47%; SOXX −3.20%; MRVL −10.28%; AMD −2.33%; MSFT +1.68%; AAPL +1.63%.
- MRVL beat/raised but guided nGAAP GM down a touch and pushed Google custom impact to FY29+; AH weakness Thu was knowable at Friday open.
- Chicago PMI 47.1 at 9:45 ET; UMich final ~51.7 (secondary).
- Morning S0=0 / S1=+1 / up-flat over-weighted a carried AI spine and under-weighted the live 10:00 binary.

**Memory:** search paused (embedding index metadata missing). Injected 08-12/13/14/27 lessons used as provided; no additional MEMORY.md hits.