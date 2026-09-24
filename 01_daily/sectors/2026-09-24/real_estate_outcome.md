# Sector Outcome — Real Estate — 2026-09-24

Actuals: {'etf': 'XLRE', 'pct': -0.4541076147611678, 'spy_pct': -0.0820521854125067, 'rel': -0.3720554293486611, 'open': 42.029998779296875, 'close': 41.650001525878906, 'source': 'yf_download'}

Memory search is paused (index metadata missing; `openclaw memory status --index` or `openclaw memory index --force` would rebuild it). Review uses the injected morning card, deterministic actuals, and live sources only.

## 0. Facts

XLRE **−0.454%** (open **42.03** → close **41.65**). SPY **−0.082%**. Relative **−0.372%**. Path: slight gap-up vs ~9/23 close, then a fade into the close. Sign **down**, unsigned move **mild**. SPY was essentially flat; XLRE lagged again, but the lag shrank vs 9/23’s rel **−1.03%**.

CLAIM: XLRE closed −0.454% vs SPY −0.082% (rel −0.372%), open 42.03 / close 41.65.  
URL: session actuals (deterministic Channel 1)  
PUBLISHED: 2026-09-24 close  
QUOTE: `ETF_PCT: -0.454… | SPY_PCT: -0.082… | REL_PCT: -0.372… | OPEN: 42.03 CLOSE: 41.65`  
SUMMARY: Down/mild print; still a relative laggard, not a smash.

CLAIM: Cash indices finished near unchanged after a red overnight tape.  
URL: https://www.eoption.com/market-review-september-24-2026/  
PUBLISHED: 2026-09-24 close recap  
QUOTE: “S&P 500 −1.80 0.02% 7,704 … Nasdaq 3.34 0.01% 26,939”  
SUMMARY: SPX/Nasdaq closed flat; the overnight ES −0.64% / NQ −1.09% risk-off did not hold into the cash close.

## 1. What drove the sector

Primary object was the **rate channel**, not REIT-specific news.

The long end was already in the stress zone at the open and **backed up further**. Pre-open, 10Y was **+3.3 bp to 5.15%** and 30Y **+4.1 bp to 5.44%**. The close recap has 10Y at **5.16%** and the 30Y having printed **5.446%**, highest since 2004. That is a live duration shock for a pure-REIT ETF.

CLAIM: Long-end yields were already ripping into the open, not “flat-to-+1 bp.”  
URL: https://www.eoption.com/morning-preview-september-24-2026/  
PUBLISHED: 2026-09-24 early look  
QUOTE: “The 10-year Treasury yield rose 3.3 basis points to 5.15%, while the 30-year yield gained 4.1 basis points to 5.44%.”  
SUMMARY: Live curve at the open was a multi-decade-high 30Y, not the Finviz note-price “slightly down = yields ~flat” read on the morning card.

CLAIM: 10Y finished ~5.16%; 30Y had tagged 5.446%.  
URL: https://www.eoption.com/market-review-september-24-2026/  
PUBLISHED: 2026-09-24 close  
QUOTE: “the 30-year Treasury yield climbed to 5.446%, hitting its highest point since 2004… 10-Year Note 0.05 5.16%”  
SUMMARY: Session confirmed a long-end backup, not relief.

Same-day hawkish increment was **Paulson, not a fresh Warsh quote**. Philly Fed President Anna Paulson (2026 voter) said modest further tightening may be warranted if conditions evolve as she expects — a continuation of the Warsh/hike regime the morning card had already mapped.

CLAIM: Paulson kept the hike path live on 9/24.  
URL: https://www.eoption.com/mid-morning-look-september-24-2026/  
PUBLISHED: 2026-09-24 mid-morning  
QUOTE: “additional interest rate hikes may be needed… ‘if conditions evolve as I expect, some modest further tightening may be warranted’”  
SUMMARY: Same-day Fed-speak reinforced S0’s hawkish map; it was not a new Warsh interview.

Growth data leaned **hawkish for duration, mixed for property**: jobless claims 197k vs 201k est; August new-home sales **684k** vs ~620k, +6.4% m/m. That is a residential-demand positive and a “economy too firm for relief” negative for cap rates.

CLAIM: New-home sales beat, 684k SAAR.  
URL: https://www.census.gov/construction/nrs/pdf/newressales.pdf  
PUBLISHED: 2026-09-24  
QUOTE: August 2026 sales 684,000 SAAR, up 6.4% from revised July 643,000.  
SUMMARY: Housing print was a 10:00 ET two-sided object — good for residential REITs, bad for the rate path.

Oil resumed higher (eOption: WTI **+$2.45 / +2.66%** to $94.61; Brent **+3.41%** to $106.60), feeding the term-premium / inflation-stickiness story. Absolute oil levels disagree with the morning Finviz $104 WTI board; **direction** (re-acceleration) matches the 09-23 oil-skew lesson.

Inside the sector, the rate hit was **not uniform**. JPM’s REIT reshuffle upgraded **WELL, EGP, MAC, REG** and cut ARE / SAFE / others. WELL, XLRE’s largest weight, closed **+1.63%**.

CLAIM: WELL was the session outlier on a JPM upgrade.  
URL: https://stockscan.io/stocks/WELL/price-history  
PUBLISHED: 2026-09-24  
QUOTE: “Sep 24, 2026 … % Change +1.63%” close $233.61  
SUMMARY: Healthcare REIT bid offset some of the duration drag; XLRE still finished red, so the ETF was not WELL.

Taxonomy: **rates rising / REIT selloff HIT**; **refi-wall / cap-rate expansion HIT**; **rotation-out HIT but milder**; **duration relief MISS**; **DC demand MISS** (ORCL force-majeure / AI-complex wobble); **residential occupancy/sales mixed-positive** (home-sales beat + WELL).

## 2. Audit of morning S0–S4 (morning numbers, not rewritten)

Morning scores: S0 **−1**, S1 **−2.5**, S2 **−1**, S3 **−0.5**, S4 **−1**, mult **0.85**, call **down / mild**, conf **0.6**, no divergence.

**S0 (−1) — HIT on sign, process miss on the live curve.**  
The hawkish-regime map was right: stress-zone long end + hike path + two-sided calendar = negative skew, not S0=0 (09-23 lesson applied correctly). What was wrong was the **Channel 1 Finviz “10Y note −0.03% / 30Y bond −0.06% = yields ~flat-to-+1 bp”** sentence. That is the 08-25 failure mode again. The early-look tape already had 30Y at **5.44%**. S0 should have been “negative skew with a live long-end rip,” not “negative skew because the curve isn’t relief.” Sign still matched.

**S1 (−2.5) — HIT on rates spine, slightly hot on stacking.**  
Rates-rising was scored **partial (−1)** because the morning thought the live curve was flat. Reality: the curve *did* rise. Real-yield 1m (+0.23 on DFII10 through 9/22) remained the structural duration headwind. Cap-rate / refi-wall (−0.5) was the same rate object in property language — flagged in the morning self-audit, still double-adjacent. Rotation-out (−0.5) was directionally right (rel −0.37%) but **over-copied yesterday’s −1.03% 1d rel** into today; the lag continued, it did not re-crash. Missed same-day offsets: WELL upgrade, home-sales beat.

**S2 (−1) — HIT.**  
No pre-open XLRE print, so S2 was a stale-lag participation fail. Close validates it: WELL **+1.63%** while XLRE **−0.45%** is exactly “names not confirming a sector bid” / ETF down with leadership split. MAP HEAT (healthcare/residential nested up; office nested down) showed up in the JPM cuts to ARE and the WELL bid. Do not let WELL define XLRE — morning got that rule right.

**S3 (−0.5) — weak HIT / untested.**  
No flow print. Persistent non-participation is still the prior, not a new 9/24 fact. Scoring it small was fine; it did not drive the day.

**S4 (−1) — direction HIT, intensity MISS.**  
Prior tape (1d −1.76 / rel −1.03, 1m rel −6.93) confirmed the *sign*. Today’s tape was a **much smaller** down day. S4 treated a multi-day lag as if it would reprint. Confirmation-only is the right slot; the magnitude of S4 was yesterday’s move, not today’s.

**Band self-audit vs engine.**  
LLM self-audit: |leading| ~5 × 0.85 → down/mild; notable only if a verified long-end smash. Engine total **−9.97** with overlay **−6.0** and index_carry **−1.915** was far hotter than the −0.45% outcome. The **band cap was the correct object**; the overlay restack was not. 09-22 flat-cap correctly OFF (ES/NQ outside ±0.5%). 09-11 no-force-down correctly OFF (live tape was red). 09-08 cushion correctly OFF (1d rel was red).

## 3. Interactions / double-count / knowable-at-open

**Same-shock stack:** Warsh/hike path in S0, rates-rising in S1, overlay −6, index_carry from ES −0.64%. That is one hawkish/risk-off object counted ~four ways. Morning claimed S0 = regime and S1 = live curve. Fair *if* the live curve is independently verified. It wasn’t, on Finviz — and then the overlay copied it anyway. Net: direction survived, conviction/score did not earn the −9.97.

**Offsets that cut the unsigned move:** (1) SPX recovered from overnight red to **~flat**, so index beta did not add a second leg lower; (2) WELL +1.63% JPM upgrade; (3) new-home sales beat bid into residential/healthcare nested longs. Duration still won the **sign**. The offsets won the **band**.

**Knowable at open:**  
- Knowable: multi-horizon XLRE lag, 30Y in stress zone, hawkish regime after Warsh, overnight ES/NQ red, oil bouncing, two-sided calendar (claims, new-home sales, 7Y auction).  
- Knowable but **unused**: eOption early-look 30Y **5.44% / +4.1 bp** — that was the live curve.  
- Not knowable: Paulson copy, JPM WELL upgrade, 684k home-sales beat, SPX’s full retracement of the overnight gap.

KNOWABLE_AT_OPEN = **partially**.

## 4. Outliers inside the sector

- **WELL +1.63%** on JPM Overweight — largest XLRE weight, opposite the ETF.  
- **JPM cuts** to ARE (office) / SAFE (rate-sensitive ground lease) fit office/mortgage nested down.  
- **EQIX/PLD** were not the smash sleeve; DC was a headwind in the morning (NQ/XLK/ORCL) and did not rescue XLRE.  
- **New-home sales beat** vs **mortgage-rate / 30Y high** — residential nested “up” in the morning MAP, but not enough to flip XLRE.

Do not rewrite S0 from WELL. The ETF still closed down and lagged SPY.

---

OUTCOME_BEGIN
SECTOR: Real Estate
ETF: XLRE
ETF_PCT: -0.454
SPY_PCT: -0.082
REL_PCT: -0.372
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Long-end backup (10Y ~5.16%, 30Y ~5.45% multi-decade high) plus same-day hawkish Fed-speak kept duration REITs as a relative laggard vs a flat SPY.
KEY_INTERACTION: Rate smash vs SPX recovery-to-flat and WELL +1.63% JPM upgrade — duration won the sign; the idiosyncratic bid and index beta capped the band at mild.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Down/mild HIT; 09-23 negative-skew S0 was right, but Finviz “flat curve” missed a live 30Y rip and the overlay restacked the same hawkish object into a −9.97 score the tape never delivered.
OUTCOME_END

---

## RESEARCH APPENDIX

**Queries run**
- web_search: `XLRE real estate ETF September 24 2026 performance rates REITs`
- web_search: `S&P 500 September 24 2026 Warsh Fed rate hike yields REITs`
- web_search: `10 year treasury yield September 24 2026 close DGS10 DGS30`
- web_search: `XLRE WELL PLD EQIX AMT DLR SPG September 24 2026 stock performance`
- web_search: `"September 24" 2026 stock market recap yields Warsh REIT real estate`
- web_search: `site:finance.yahoo.com XLRE historical 2026-09-24`
- web_search: `US 10-year Treasury yield September 24 2026 close 5.11 5.16`
- web_search: `sector performance September 24 2026 XLRE XLE XLK real estate lagging`
- web_search: `Kevin Warsh rate hike comments September 24 2026 Paulson additional tightening`
- web_search: `WELL Welltower EQIX Equinix Prologis PLD September 24 2026 close`
- web_search: `new home sales August 2026 684000 Census Bureau`
- web_search: `Philadelphia Fed Paulson additional tightening September 24 2026`
- x_search: XLRE/REITs/yields/Warsh 2026-09-23→09-25
- x_search: XLRE/WELL/10Y 5.16/30Y 5.44 close 2026-09-24→09-25
- memory_search: Real Estate XLRE 2026-09-24 (failed — index unavailable)

**Key sources (title + URL + timestamp) and facts taken**

1. **Market Review: September 24, 2026 | eOption** — https://www.eoption.com/market-review-september-24-2026/ — fetched 2026-09-24T21:18Z  
   Facts: SPX −0.02% to 7704; Nasdaq +0.01%; 30Y tagged 5.446% (highest since 2004); 10Y 5.16%; WTI +2.66% to $94.61; Brent +3.41% to $106.60; JPM REIT upgrades WELL/EGP/MAC/REG; claims 197k; new-home sales +6.4% to 684k.

2. **Morning Preview: September 24, 2026 | eOption** — https://www.eoption.com/morning-preview-september-24-2026/ — fetched 2026-09-24T21:19Z  
   Facts: Pre-open 10Y +3.3 bp to 5.15%, 30Y +4.1 bp to 5.44%; ES futures −0.54% in that board; 7Y auction on calendar.

3. **Mid-Morning Look: September 24, 2026 | eOption** — https://www.eoption.com/mid-morning-look-september-24-2026/ — fetched 2026-09-24T21:12Z  
   Facts: Paulson “modest further tightening may be warranted”; JPM WELL upgrade rationale (same-store NOI); early index red then pare.

4. **U.S. Treasury Yields Hover Close to Multiyear Highs — Update (Dow Jones / Morningstar)** — https://www.morningstar.com/news/dow-jones/202609241547/us-treasury-yields-hover-close-to-multiyear-highs-update — 2026-09-24 08:04 GMT  
   Facts: 10Y had hit 5.14% Wed; early Thu Tradeweb 10Y 5.119% / 30Y 5.414%; weak 5Y auction Wed; Barr had said further hikes likely (prior).

5. **Census/HUD New Residential Sales** — https://www.census.gov/construction/nrs/pdf/newressales.pdf — 2026-09-24  
   Facts: August SAAR 684,000, +6.4% m/m from 643,000.

6. **Welltower price history | stockscan** — https://stockscan.io/stocks/WELL/price-history — fetched 2026-09-24T21:19Z  
   Facts: 2026-09-24 close $233.61, **+1.63%**.

7. **Session actuals (injected)** — 2026-09-24  
   Facts: XLRE −0.454%, SPY −0.082%, rel −0.372%, open 42.03, close 41.65.

**Not used as close evidence:** Benzinga sector pages (403), Reuters (401/JS wall), Yahoo history (fetch failed), FRED DGS10 (timeout; 9/24 official H.15 not yet posted). Intraday XLRE snapshots (~$41.73) were superseded by deterministic close.