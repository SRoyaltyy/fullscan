# Sector Outcome — Basic Materials — 2026-09-16

Actuals: {'etf': 'XLB', 'pct': -0.7293493696500342, 'spy_pct': -0.4409916675517711, 'rel': -0.28835770209826306, 'open': 50.93000030517578, 'close': 50.36000061035156, 'source': 'yf_download'}

Memory index is unavailable this run (embedding metadata missing), so this autopsy uses the injected morning scoreboard plus live sources only.

## 0. Facts

XLB **−0.73%** (open **50.93** → close **50.36**), SPY **−0.44%**, relative **−0.29%**. Path: cash **gapped up** from the 09-15 close ($50.73) / flat premarket, was still green into midday, then **sold off after 14:00 ET**. Intraday range ~$49.91–$51.11. Actual = **down / mild**, not a washout.

Morning call was **flat / flat** (S0 +0.5, S1 +1, S2/S3/S4 0; FOMC treated as an unscored 14:00 binary; oil-offered chemicals + 8/14 gold as the live bid).

---

## 1. What drove the sector

**Primary driver was the FOMC increment, not the pre-open commodity tape.**

The 25 bp hike to 3.75–4.00% was the *priced* event. The *live* shock was the hawkish package around it: unanimous 12–0, SEP with **16/18** dots for at least one more 2026 hike, and Warsh’s “inflation is too high and has been for too long” presser. Equities were green into 14:00 and reversed; Dow **−1.2%**, S&P **−0.45%**, 2Y **+7 bp**, 10Y back through **5.00%**. XLB is a cyclical/rate book; that beta dump is the close.

Commodity transmission **failed to save cash XLB**:

- Oil kept falling (Brent **−2.69%** to $105.83; WTI ~$102.4 vs ~$105.8 prior) — morning’s chemicals-feedstock relief was *real* and *larger* by the close, but LIN/SHW/APD still finished red.
- Gold **HIT** in metal (Comex **+1.27%** to $4,346.30) and copper **HIT** in metal (Comex **+0.99%** to $6.4315), but the equity sleeves did not follow: NEM ~**−2%**, CRH printed a **52-week low ($86.32)**.
- Taxonomy: this is **shared-macro / real-yield / Fed-path** dominating **industrial-metal** and **gold-sleeve** factors, with **rate-sensitive building materials** as the internal weak link. China property / LME glut were still *levels*, not the 1d impulse.

---

## 2. Audit of morning S0–S4 (use morning numbers, not a rewrite)

| Sleeve | Morning | Reality vs that read |
|---|---|---|
| **S0 +0.5** | Modest lean with green ES/NQ; **do not pre-score 14:00**; hike is a *level* | Process-correct on the binary. The +0.5 lean described the **open-to-2pm** tape (XLB green midday ~+0.27%) and was **overwritten** by Warsh/SEP. Using the hawkish *level* to force down at the open would have been the 09-15 error; using it as the *close* driver is correct. |
| **S1 +1** | Majority oil relief + 8/14 gold, net of LME glut / China / copper-HEAT down | Commodities did what S1 said (oil down, gold/copper up). **Cash did not.** Chemicals did not rally on feedstock; NEM decoupled from bullion. S1 overpaid **commodity-to-XLB transmission** on an FOMC day. |
| **S2 0** | Split HEAT, no thrust | Confirmed: no breadth expansion. Construction (CRH) was the down outlier; chemicals modestly red; FCX mixed/firm vs NEM weak. 0 was the right *morning* print; close was a **soft down tape**, not a washout. |
| **S3 0** | No flow spike | No evidence of a positioning event. Fine. |
| **S4 0** | Yesterday’s +0.93% rel already in S1; 09-03/09-15 single-print | Correct zero. Today’s tape was **down/lag**, so paying S4 would have been worse. |

**Direction:** morning **flat** vs actual **down** → **MISS**.  
**Magnitude:** morning **flat** (calendar size gate + FOMC mild cap) vs actual **mild (−0.73%)** → **near-miss / slight undershoot**. The gate was directionally right to refuse notable; it was too tight vs a 70 bp FOMC fade.

Rolling context: 09-15 was dir MISS / mag HIT on a chemicals bounce. Today is the complement: **process-right to stay off down at the open, wrong to assume the binary would close flat**.

---

## 3. Interactions / double-count / knowable-at-open

- **Same-shock:** Oil was counted once in S1 (chemicals), not restacked in S0 — no double-count. The miss is **transmission**, not stacking.
- **Gold 8/14:** Metal HIT, **equity sleeve MISS**. NEM ~8% of XLB did not monetize GC +1.27% once discount rates / equity beta hit. Gold did not cancel China (morning was right); it also did not **bid the book**.
- **09-15 nested-bid:** Live bid scored in S1 only, S4 zeroed — process HIT. The “live bid” was oil+gold, and it **did not survive 14:00**.
- **09-11 pre-binary:** Correctly refused to zero the knowable tape *and* refused a materials +1. The residual error is treating “priced hike” as “close ≈ open.” The **unknowable** object was SEP + Warsh tone, not the 25 bp itself.
- **09-10 gap-at-open:** Morning said OFF (PM 0.00%). Cash still **opened 50.93 vs 50.73** (~+0.4% gap) then faded — a small gap that 09-10 would not have forced notable, but it did set a **fade-from-open** path (~−1.1% open-to-close).
- **Relative:** `sector_rs_veto` (d1 −2.08 / w1 −4.78) stayed valid: XLB lagged SPY by **29 bp**. 1w/1m lag thesis was the better horizon call than the 1d flat.

**Knowable at open:** hike odds, oil offered, gold/copper green, XLB PM flat, 1w/1m lag, FOMC as binary.  
**Not knowable:** 16/18 additional-hike dots, Warsh presser, 2Y +7 bp, NEM/gold decoupling, CRH 52-week low, post-14:00 beta dump.

---

## 4. Outliers inside the sector

- **CRH** — new 52-week low $86.32; rate-sensitive building materials were the cleanest **down** expression of the hike/SEP.
- **NEM** — ~−2% with gold **+1.27%**; miner/equity beta ≠ bullion. 8/14 sleeve failed as a cash hedge.
- **LIN / SHW / APD** — majority chemicals **did not rally** despite a larger oil drop. Oil-relief as XLB-weight was a **false friend** on FOMC day.
- **FCX / copper** — metal **+0.99%**; FCX mixed-to-firm vs the book. Minority copper did **not** dominate (09-08 composition still right), but it also didn’t *hurt* the way morning copper-HEAT-down implied.
- **ECL** — roughly flat; not a leadership story.

No single-name explosion moved XLB; the book lost on **shared beta + construction**, with gold miners as a second drag.

---

### Evidence

CLAIM: FOMC hiked 25 bp to 3.75–4.00%, unanimous 12–0, 14:00 ET.  
URL: https://www.federalreserve.gov/newsevents/pressreleases/monetary20260916a.htm  
PUBLISHED: 2026-09-16 14:00 EDT  
QUOTE: “The Committee decided to raise the target range for the federal funds rate by 1/4 percentage point to 3-3/4 to 4 percent… Inflation remains elevated.”  
SUMMARY: Priced hike delivered; statement itself was terse.

CLAIM: SEP/dots and Warsh tone were the hawkish increment; stocks reversed after the presser.  
URL: https://www.cnbc.com/2026/09/16/fed-rate-decision-september-2026.html  
PUBLISHED: 2026-09-16  
QUOTE: “16 of the 18 participants… expected another rate increase”; Warsh: inflation “too high … for too long.”  
SUMMARY: Another-hike path, not the 25 bp, is the live macro shock.

CLAIM: Post-decision equity dump; Dow −631 pts / −1.2%; S&P −0.45% to 7551.81 after giving up a modest gain.  
URL: https://www.cnbc.com/2026/09/16/here-are-five-key-takeaways-from-wednesdays-fed-rate-hike.html  
PUBLISHED: 2026-09-16  
QUOTE: “Stocks were in the green heading into the rate decision… sold off sharply after the decision. The Dow… tumbled 631 points and the 2-year Treasury yield… rocketed more than 7 basis points higher.”  
SUMMARY: Path matches XLB: green into 14:00, fade into the close.

CLAIM: S&P 500 −0.45% to 7551.81 (aligns with injected SPY −0.44%).  
URL: https://www.morningstar.com/news/dow-jones/202609167425/sp-500-falls-045-to-755181-data-talk  
PUBLISHED: 2026-09-16 16:28 ET  
QUOTE: “The S&P 500 Index is down 33.92 points or 0.45% today to 7551.81.”  
SUMMARY: Broad tape down mild; XLB worse than SPX.

CLAIM: 10Y yield 5.003% (3 p.m. close); 2Y +7 bp to ~4.74%.  
URL: https://www.morningstar.com/news/dow-jones/202609166777/10-year-treasury-yield-rises-to-5003-data-talk  
PUBLISHED: 2026-09-16 15:45 ET  
QUOTE: “The 10-year yield rose 0.008 percentage point to 5.003% today… A new 52-week high.”  
SUMMARY: Real-yield / discount-rate impulse into cyclicals and miners.

CLAIM: Comex copper +0.99% to $6.4315.  
URL: https://www.morningstar.com/news/dow-jones/202609165642/comex-copper-settles-099-higher-at-64315-data-talk  
PUBLISHED: 2026-09-16 13:51 ET  
QUOTE: “Front Month Comex Copper… gained 6.30 cents per pound, or 0.99% to $6.4315.”  
SUMMARY: Spine did **not** collapse; industrial metal bid continued through FOMC.

CLAIM: Comex gold +1.27% to $4,346.30.  
URL: https://www.morningstar.com/news/dow-jones/202609165639/comex-gold-settles-127-higher-at-434630-data-talk  
PUBLISHED: 2026-09-16 13:50 ET  
QUOTE: “Front Month Comex Gold… gained $54.70… or 1.27% to $4346.30.”  
SUMMARY: 8/14 metal HIT; cash miners did not follow.

CLAIM: Brent −2.69% to $105.83.  
URL: https://www.morningstar.com/news/dow-jones/202609166555/front-month-ice-brent-crude-fell-269-to-settle-at-10583-data-talk  
PUBLISHED: 2026-09-16 15:08 ET  
QUOTE: “Front Month ICE Brent Crude… lost $2.92 per barrel, or 2.69% to $105.83.”  
SUMMARY: Oil-offered chemicals overlay was larger by the close, not faded.

CLAIM: CRH hit a new 52-week low $86.32 (prior close $87.69).  
URL: https://www.marketbeat.com/instant-alerts/price-crh-nyse-crh-reaches-new-1-year-low-whats-next-2026-09-16/  
PUBLISHED: 2026-09-16  
QUOTE: “The stock traded as low as $86.32 and last traded at $86.2710… previously closed at $87.69.”  
SUMMARY: Building-materials outlier; rate path > aggregates tape.

CLAIM: Midday sector snapshot still had XLB green before the full fade.  
URL: https://www.benzinga.com/etfs/sector-etfs/26/09/61814178/leading-and-lagging-sectors-september-16-2026  
PUBLISHED: 2026-09-16  
QUOTE: (search extract) XLK +0.52%, XLB +0.27%, XLE −1.18% midday.  
SUMMARY: Confirms green-into-FOMC then reversal; energy weaker on oil, materials not a leader into the close.

---

OUTCOME_BEGIN
SECTOR: Basic Materials
ETF: XLB
ETF_PCT: -0.729
SPY_PCT: -0.441
REL_PCT: -0.288
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Hawkish FOMC package (25bp + another-hike dots + Warsh inflation presser) reversed a green-into-2pm tape.
KEY_INTERACTION: Oil-down / gold-up / copper-up did not transmit to cash XLB; CRH 52w low and NEM vs gold decoupling dominated the book.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Flat/flat was a process-right FOMC cap but a dir miss; S1 overpaid commodity transmission that the 14:00 beta dump overwrote.
OUTCOME_END

---

## RESEARCH APPENDIX

**Queries run**
- memory_search: Basic Materials XLB sector lessons 2026-09-16 FOMC outcome *(disabled: index metadata missing)*
- web_search: XLB Materials Select Sector September 16 2026 FOMC performance
- web_search: Fed FOMC September 16 2026 rate decision hike Warsh materials stocks
- web_search: copper gold oil prices September 16 2026 FOMC XLB LIN FCX NEM
- web_search: leading and lagging sectors September 16 2026 XLB XLE XLK SPY
- web_search: LIN SHW NEM FCX ECL APD stock performance September 16 2026
- web_search: stock market close September 16 2026 S&P 500 after Fed hike yields dollar gold
- web_search: XLB holdings performance LIN NEM CRH SHW September 16 2026 close
- web_search: WTI crude oil settle September 16 2026 dollar DXY gold after FOMC
- web_search: Newmont NEM CRH Linde Sherwin Williams close September 16 2026 FOMC
- web_search: WTI crude oil settlement September 16 2026 CL=F close
- web_search: Newmont stock drops gold rises September 16 2026 FOMC
- web_search: site:reuters.com gold Fed rate decision September 16 2026
- x_search: XLB materials sector FOMC copper gold chemicals September 16 2026 (2026-09-16 to 2026-09-17)
- web_fetch: Fed statement; CNBC decision + takeaways + yields; Morningstar copper/gold/Brent/SPX/10Y; WTOP indexes; MarketBeat CRH; Seeking Alpha close; Benzinga sectors (403)

**Key sources (title + URL + timestamp) and facts taken**

| Source | URL | Timestamp | Facts taken |
|---|---|---|---|
| Fed FOMC statement | https://www.federalreserve.gov/newsevents/pressreleases/monetary20260916a.htm | 2026-09-16 14:00 EDT | +25 bp to 3.75–4.00%; 12–0; inflation elevated |
| CNBC Fed decision | https://www.cnbc.com/2026/09/16/fed-rate-decision-september-2026.html | 2026-09-16 | 16/18 dots another hike; Warsh “too high for too long”; hike was >90% priced |
| CNBC five takeaways | https://www.cnbc.com/2026/09/16/here-are-five-key-takeaways-from-wednesdays-fed-rate-hike.html | 2026-09-16 | Green into decision, dump after; Dow −631; 2Y +7 bp |
| CNBC yields | https://www.cnbc.com/2026/09/16/treasury-yield-bond-market-fed-decision.html | 2026-09-16 | 10Y ~5.016%; 2Y 4.738% |
| DJMD S&P | https://www.morningstar.com/news/dow-jones/202609167425/sp-500-falls-045-to-755181-data-talk | 2026-09-16 16:28 ET | SPX −0.45% to 7551.81 |
| WTOP index recap | https://wtop.com/national/2026/09/how-major-us-stock-indexes-fared-wednesday-9-16-2026/ | 2026-09-16 | SPX −0.4%, Dow −1.2%, Nasdaq ~0%, RTY −0.4%; gave up earlier gain |
| DJMD 10Y | https://www.morningstar.com/news/dow-jones/202609166777/10-year-treasury-yield-rises-to-5003-data-talk | 2026-09-16 15:45 ET | 10Y 5.003%, 52-week high |
| DJMD copper | https://www.morningstar.com/news/dow-jones/202609165642/comex-copper-settles-099-higher-at-64315-data-talk | 2026-09-16 13:51 ET | HG +0.99% to $6.4315 |
| DJMD gold | https://www.morningstar.com/news/dow-jones/202609165639/comex-gold-settles-127-higher-at-434630-data-talk | 2026-09-16 13:50 ET | GC +1.27% to $4,346.30 |
| DJMD Brent | https://www.morningstar.com/news/dow-jones/202609166555/front-month-ice-brent-crude-fell-269-to-settle-at-10583-data-talk | 2026-09-16 15:08 ET | Brent −2.69% to $105.83 |
| MarketBeat CRH | https://www.marketbeat.com/instant-alerts/price-crh-nyse-crh-reaches-new-1-year-low-whats-next-2026-09-16/ | 2026-09-16 | CRH 52w low $86.32 vs prior $87.69 |
| Benzinga sectors (search; page 403) | https://www.benzinga.com/etfs/sector-etfs/26/09/61814178/leading-and-lagging-sectors-september-16-2026 | 2026-09-16 | Midday XLK +0.52% / XLB +0.27% / XLE −1.18% |
| Reuters gold (pre) | https://www.reuters.com/world/india/gold-muted-investors-brace-fed-rate-decision-2026-09-16/ | 2026-09-16 | Gold firmer into decision; hike priced |
| Injected actuals | Channel 1 deterministic | 2026-09-16 close | XLB −0.729%, SPY −0.441%, rel −0.288%; O 50.93 / C 50.36 |

**Not used / weak:** X posts were mostly *pre*-FOMC and one copper-inventory claim conflicted with the morning LME-glut tape — discarded. Yahoo/search quotes for LIN/NEM/FCX were approximate and sometimes internally inconsistent; used only as sleeve *direction* (chemicals red, NEM ~−2%, CRH weak), not as official closes. Memory search paused (run `openclaw memory status --index` or `openclaw memory index --force` to restore).