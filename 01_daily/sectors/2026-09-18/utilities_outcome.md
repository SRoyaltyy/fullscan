# Sector Outcome — Utilities — 2026-09-18

Actuals: {'etf': 'XLU', 'pct': -1.4152078964327464, 'spy_pct': -0.11932509489422927, 'rel': -1.295882801538517, 'open': 41.375, 'close': 41.099998474121094, 'source': 'yf_download'}

Memory index is paused this run (`openclaw memory status --index` / `openclaw memory index --force`); review uses injected morning logs + Channel 1 actuals + live sources.

## 0. Facts

XLU **−1.42%** (open **41.375** → close **41.10**). Prior close was **$41.69**, so the session **gapped down ~0.76%** and then drifted another ~0.66% from the open — weak from the open, not a late orphan smash. SPY **−0.12%**. Relative **−1.30%**. Direction **down**; magnitude **notable** (absolute and relative).

Path vs morning tape: AM snapshot was XLU PM **−0.07%**, live 10Y **~4.96% vs 4.94%** (+2 bp), ZN **−0.03%**. The close was a much larger duration backup than that AM print.

---

## 1. What drove the sector

**Primary: rates rising / bond-proxy selloff (S0).** Yesterday’s duration-relief print was given back. 10Y closed **5.00%** vs **4.94%** on 09-17 (**+6 bp**, CountryEconomy); Tradeweb 3 p.m. **4.995%**, **+4.9 bp** on the day, largest one-day yield gain since 09-10. CNBC: 10Y **+>5 bp to 5.006%**, 2Y **+7 bp to 4.76%**, 30Y **+>3 bp to 5.331%**. Tradeweb 30Y **+3.1 bp to 5.327%**. That is a same-session long-end backup, not “+2 bp noise inside a stress zone.”

**Secondary, same complex: rotation away from utilities (S1, relative-only in the morning).** SPY nearly flat; utilities led decliners. Captains were red with the ETF: **NEE −1.00%** ($80.47), **SO −1.43%** ($85.52), **DUK ~−0.85%**. IPP ran hotter: **CEG ~−3.1%**, **VST ~−2.0%** — outliers, not the XLU engine (morning MAP HEAT already had IPP SPLIT down and forbade driving the ETF).

**Not the driver:**
- **IP/LEI did not deliver the XLU-positive duration bid.** IP **0.0%** vs **+0.3%** expected; manufacturing **−0.3%**; LEI **−0.1%**. Morning map said weak IP → possible duration bid. Bonds sold anyway. Do not score the IP *utilities-output +1.8%* line as an XLU equity positive.
- **FOMC/Warsh/dots:** paid 09-16. Today is the **09-17 easing reversal**, not a restack of the hike.
- **AI-power / House Ratepayer bill / FERC:** stale 09-15/16 politics; no 09-18 XLU-wide print.
- **Triple witching / Monday rebalance:** mechanical; no utilities add/delete.
- **FTS:** not a risk-off smash (SPY ~flat). Classic rate-sensitive lag.

**CLAIM:** 10Y backed up to ~5.00% on 09-18, reversing 09-17’s −6 bp dip.  
**URL:** https://countryeconomy.com/bonds/usa  
**PUBLISHED:** 2026-09-18 (page fetched 2026-09-18T21:48:41Z)  
**QUOTE:** “09/18/2026 5.00% 0.06 … 09/17/2026 4.94% −0.06”  
**SUMMARY:** Same-session +6 bp 10Y backup; the AM +2 bp seed became a full reversal of yesterday’s duration relief.

**CLAIM:** Tradeweb 3 p.m. 10Y +4.9 bp to 4.995%, largest 1-day yield gain since 09-10.  
**URL:** https://www.morningstar.com/news/dow-jones/202609186066/10-year-treasury-yield-rises-to-4995-this-week-data-talk  
**PUBLISHED:** 2026-09-18 15:47 ET  
**QUOTE:** “Today it is up 0.049 percentage point … Largest one-day yield gain since Thursday, Sept. 10, 2026.”  
**SUMMARY:** Confirms the backup was a real 1-day rates impulse, not weekly carry.

**CLAIM:** CNBC: 10Y back above 5%, +>5 bp to 5.006%; 30Y +>3 bp to 5.331%.  
**URL:** https://www.cnbc.com/2026/09/18/treasury-yields-fed-rates-volatile-week.html  
**PUBLISHED:** 2026-09-18  
**QUOTE:** “The yield on the benchmark 10-year Treasury note climbed back above the 5% level, rising more than 5 basis points to 5.006%.”  
**SUMMARY:** Session narrative is post-FOMC curve re-steepening/backup, not a new CPI-class print.

**CLAIM:** IP stalled 0.0% m/m, missed +0.3%; manufacturing −0.3%.  
**URL:** https://seekingalpha.com/news/4644334-industrial-production-growth-stalls-in-august-falling-short-of-expectations  
**PUBLISHED:** 2026-09-18, 9:17 AM ET  
**QUOTE:** “U.S. industrial production growth came in at 0.0% M/M in August, missing the +0.3% consensus and slowing from +0.2% in July.”  
**SUMMARY:** The calendar leftover printed *weak*, the branch that morning said could bid duration — and duration still sold.

**CLAIM:** LEI −0.1% in August, first monthly decline since March.  
**URL:** https://www.prnewswire.com/news-releases/the-conference-board-leading-economic-index-lei-for-the-us-edged-down-in-august-302883352.html  
**PUBLISHED:** 2026-09-18  
**QUOTE:** “The US LEI receded slightly in August, the first monthly decline since March of this year.”  
**SUMMARY:** Soft LEI did not flip XLU into a flight-to-safety bid.

**CLAIM:** NEE closed $80.47, −1.00%.  
**URL:** https://stockanalysis.com/stocks/nee/history/  
**PUBLISHED:** 2026-09-18, 4:00 PM EDT  
**QUOTE:** “80.47 −0.81 (−1.00%) At close: Sep 18, 2026”  
**SUMMARY:** Largest XLU weight participated in the down tape; not an IPP-only move.

**CLAIM:** SO closed $85.52, −1.43%.  
**URL:** https://stockanalysis.com/stocks/so/history/  
**PUBLISHED:** 2026-09-18, 4:00 PM EDT  
**QUOTE:** “85.52 −1.24 (−1.43%) At close: Sep 18, 2026”  
**SUMMARY:** Regulated captain down in line with XLU; confirms breadth, not a single-name smash.

---

## 2. Audit of morning S0–S4 (use morning numbers, not post-close rewrites)

| Bucket | Morning | Reality | Verdict |
|---|---|---|---|
| **S0 = 0** | Mixed; extra confirm for smash **absent**; extra confirm for relief **absent**; 09-17 duration slot “filled”; live 10Y +2 bp = noise | 10Y **+5 to +6 bp** back to **5.00%**; 30Y **+3 bp**; largest 1d 10Y gain since 09-10 | **MISS.** The open experiment correctly withheld *full* smash weight at 10:33Z, then treated that as S0=0 / flat. After a paid −6 bp relief day, leftover variance was a **two-sided reversal slot**, not all-clear. |
| **S1 = 0** | Rotation PARTIAL, relative-only; rates-rising MISS; AI-power stale; IPP must not drive | Rotation printed **absolute down** because yields actually backed up; captains ~−1%; IPP worse but not the ETF | **Partial miss.** Standalone “rotation is not an absolute ceiling” (09-17) still holds; it does **not** veto down when S0 rates-rising **hits**. Scoring S1=0 was consistent with AM evidence; the miss lives in S0. |
| **S2 = 0** | 1d rel −0.24% not breadth; don’t pay 1w/1m | Broad down (NEE/SO/DUK and IPP) | **OK as AM read.** Post-close breadth confirmed the down move; it was not a leadership divergence. |
| **S3 = 0** | No flow spike; witching mechanical; no utilities rebalance | No same-day flow/rebalance evidence surfaced | **OK.** |
| **S4 = 0** | Confirmation only; 09-14 floor not binding | Tape confirmed −1.42% / rel −1.30% | **OK as AM.** Do not retrofit S4 to manufacture the down call. |

**Applied lessons that bound correctly:** 09-16 do not restack the paid hike; 09-11 IP/LEI is not a CPI-class S0=−1; 08-12 AI-power is a dampener not a band engine; 08-28 don’t promote CEG/VST into S1; 08-25 don’t manufacture down from carried S2/S3 when S0=S1=0; 09-14 S4 floor did not bind on the *morning* 1d rel.

**Lesson that was mis-applied:** 09-17 “slot filled yesterday.” Filling the slot on 09-17 does **not** mean 09-18 cannot give the −6 bp back. The 09-17 do-instead was “leave a two-sided duration slot after a paid FOMC / prefer flat/mild over flat/flat.” This morning collapsed that into **flat/flat** because AM confirm was missing.

**Open experiment:** “no extra confirm for a fresh rates smash” was true at the AM snapshot (ZN −0.03%, 10Y +2 bp). The confirm **arrived in session**. Absence of AM confirm ≠ S0=0 when the curve is still in the 19-year-high zone one day after a relief dip.

---

## 3. Interactions / double-count / knowable-at-open

- **Same shock:** 10Y backup (S0) and rotation-away (S1) are one complex. Full-weight both would double-count. Correct taxonomy: **S0 rates-rising is the parent**; S1 rotation is the mapping, not a second independent HIT.
- **Do not restack FOMC** into today’s −1.4%. The impulse is **reversal of 09-17’s −6 bp**, which was knowable as *risk* (curve still ~5%) but not as *direction* until the backup printed.
- **IP/LEI vs yields:** morning’s XLU-positive branch (weak data → duration bid) **failed the market test**. Weak data + higher yields = do not score the calendar as a utilities bid.
- **IPP vs regulated:** CEG/VST worse than NEE/SO/DUK. If you let IPP drive XLU you over-explain. Regulated captains ~−1% already get you most of the ETF.
- **Knowable at open:** **partially.** Visible: live +2 bp, tiny-red bond futures, XLU PM −0.07%, NQ-lead, gap-down open **41.375**. Not visible: the full **+5–6 bp** 10Y impulse and the −1.42% close. Witching/IP/LEI were leftover variance, not a signed down license.

---

## 4. Outliers inside the sector

- **CEG ~−3.1%**, **VST ~−2.0%** vs **NEE −1.0% / SO −1.43% / DUK ~−0.85%**. IPP high-beta lag, consistent with morning MAP HEAT SPLIT down. **Must not drive XLU**; the ETF down-day is still real because captains participated.
- Nuclear satellites (e.g. SMR names handing back a prior rally) are outside XLU’s regulated spine.
- No utilities S&P add/delete for Monday’s rebalance — not an outlier flow.

---

OUTCOME_BEGIN
SECTOR: Utilities
ETF: XLU
ETF_PCT: -1.4152078964327464
SPY_PCT: -0.11932509489422927
REL_PCT: -1.295882801538517
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: 10Y reversed 09-17’s −6 bp relief and backed up ~5–6 bp to ~5.00%, a same-session bond-proxy selloff.
KEY_INTERACTION: Rates backup and defensive rotation are one shock (score in S0, not twice); weak IP/LEI did not produce the duration bid the morning left as leftover.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: S0=0 / flat-flat missed a two-sided duration-reversal slot after paid 09-17 easing; “no AM smash confirm” was not all-clear.
OUTCOME_END

---

## RESEARCH APPENDIX

**Queries run**
- memory_search: Utilities XLU 2026-09-18 sector outcome duration yields IP LEI *(index paused)*
- web_search: XLU utilities ETF September 18 2026 why down yields
- web_search: US 10 year treasury yield September 18 2026 industrial production LEI utilities
- web_search: utilities stocks NextEra Southern Duke Constellation September 18 2026
- x_search: What happened to XLU utilities ETF and Treasury yields on September 18 2026? Why did utilities fall? (2026-09-18 to 2026-09-19)
- web_fetch: https://countryeconomy.com/bonds/usa
- web_fetch: https://www.cnbc.com/2026/09/18/treasury-yields-fed-rates-volatile-week.html
- web_fetch: https://www.morningstar.com/news/dow-jones/202609186066/10-year-treasury-yield-rises-to-4995-this-week-data-talk
- web_fetch: https://www.reuters.com/business/us-manufacturing-output-falls-august-after-rising-seven-straight-months-2026-09-18/ *(401/JS wall)*
- web_fetch: https://www.prnewswire.com/news-releases/the-conference-board-leading-economic-index-lei-for-the-us-edged-down-in-august-302883352.html
- web_search: utilities roundup September 18 2026 Morningstar XLU yields
- web_fetch: https://seekingalpha.com/news/4644334-industrial-production-growth-stalls-in-august-falling-short-of-expectations
- web_search: utilities down as treasury yields rise utilities roundup September 18 2026
- web_search: US industrial production August 2026 unchanged manufacturing -0.3% utilities +1.8%
- web_search: SPY close September 18 2026 sector performance utilities lag technology
- web_fetch: https://247wallst.com/investing/2026/09/18/nuscale-power-drops-7-as-nuclear-stocks-hand-back-the-vote-rally-oklo-falls-5-centrus-energy-slides-3/ *(403)*
- web_search: NEE SO DUK CEG VST stock performance September 18 2026
- web_fetch: https://www.marketscreener.com/news/wall-st-mixed-as-benchmark-treasury-yields-reach-5-oil-takes-a-pause-ce785adadc8ef62c *(403)*
- web_fetch: https://stockanalysis.com/stocks/nee/history/
- web_fetch: https://stockanalysis.com/stocks/so/history/
- web_search: US 30 year treasury yield September 18 2026 close
- web_search: site:morningstar.com utilities roundup September 18 2026 yields
- web_fetch: https://stockanalysis.com/stocks/duk/
- web_fetch: https://stockanalysis.com/stocks/ceg/
- web_fetch: https://www.morningstar.com/news/dow-jones/202609186069/30-year-treasury-yield-falls-to-5327-this-week-data-talk
- web_fetch: https://streetstats.finance/markets/sectors-industries *(empty extract)*
- web_search: Vistra VST close September 18 2026

**Key sources and facts taken**
- CountryEconomy US 10Y (fetched 2026-09-18T21:48:41Z): 09/18 **5.00% (+0.06)**; 09/17 **4.94% (−0.06)**; 09/16 **5.00%**. https://countryeconomy.com/bonds/usa
- CNBC 09-18: 10Y **+>5 bp to 5.006%**; 2Y **+7 bp to 4.76%**; 30Y **+>3 bp to 5.331%**; post-FOMC credibility narrative, yields had pulled back after Wednesday’s hike then backed up Friday. https://www.cnbc.com/2026/09/18/treasury-yields-fed-rates-volatile-week.html
- Morningstar/Dow Jones Data Talk 09-18 15:47 ET: 10Y **+0.049 to 4.995%** today; largest 1-day yield gain since 09-10; week +0.021 to 4.995%. https://www.morningstar.com/news/dow-jones/202609186066/10-year-treasury-yield-rises-to-4995-this-week-data-talk
- Morningstar/Dow Jones Data Talk 09-18 15:47 ET: 30Y **+0.031 to 5.327%** today; week −0.027. https://www.morningstar.com/news/dow-jones/202609186069/30-year-treasury-yield-falls-to-5327-this-week-data-talk
- Seeking Alpha 09-18 9:17 AM ET: IP **0.0% m/m** vs **+0.3%** consensus; manufacturing **−0.3%**. https://seekingalpha.com/news/4644334-industrial-production-growth-stalls-in-august-falling-short-of-expectations
- Conference Board PR 09-18: LEI **−0.1% to 99.5** in August; first monthly decline since March; 6-month growth **−0.1%**. https://www.prnewswire.com/news-releases/the-conference-board-leading-economic-index-lei-for-the-us-edged-down-in-august-302883352.html
- Reuters/Trading Economics (search corroboration; Reuters page 401): manufacturing first decline of the year; utilities IP **+1.8%**; capacity utilization **76.3%**. Not used as an XLU equity driver.
- StockAnalysis: NEE **$80.47 −1.00%**; SO **$85.52 −1.43%** at 09-18 close. https://stockanalysis.com/stocks/nee/history/ https://stockanalysis.com/stocks/so/history/
- Search corroboration: DUK **~$117.53 ~−0.85%**; CEG **~$254.71 ~−3.08%**; VST **~$140.67 ~−2.01%** (prior ~$143.56).
- MarketScreener/search: S&P 500 Utilities index **~−1.40%**; Wall St mixed as 10Y reached 5%; tech relative outperformer. Primary page 403 — treated as secondary.
- X search 09-18: posts tying XLU weakness to 10Y testing **5.00%** and rate-sensitive rotation (not used as primary facts).
- Channel 1 actuals (injected, trusted): XLU **−1.4152%**, SPY **−0.1193%**, rel **−1.2959%**, open **41.375**, close **41.10**.