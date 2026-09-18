# Sector Outcome — Communication Services — 2026-09-18

Actuals: {'etf': 'XLC', 'pct': -1.3707173444083898, 'spy_pct': -0.11932509489422927, 'rel': -1.2513922495141605, 'open': 112.37000274658203, 'close': 110.80999755859375, 'source': 'yf_download'}

Memory search is paused (index metadata missing). Review uses the injected morning card, deterministic actuals, and live sources only.

## 0. Facts

XLC **−1.371%** (open **112.37** → close **110.81**). SPY **−0.119%**. Relative **−1.251%**. Path: cash open ≈ prior close / PM **−0.01%**, then a full-session selloff — not a gap-down. Actual **down / notable**. Official engine call was **up / mild** (score 3.549) against an all-zero LLM card that wanted **flat / flat**.

---

## 1. What drove the sector

Taxonomy, in order:

**Duration / real-yield re-tightening (S0).** The 10Y did not stay “stale FOMC.” It rose again into the cash session (~**+5 bp**, ~**4.995–5.00%**), the largest one-day yield gain since 09-10. XLC is a two-name duration/growth book (META ~19.5%, Alphabet A+C ~18.5%). Oil offered overnight did **not** buy this sleeve a bid.

**Engagement deceleration, entertainment sleeve (S1).** Wells Fargo cut NFLX to **Underweight** from Equal Weight, PT **$57** from **$80**, citing hours-watched / originals engagement. NFLX closed **−4.31%**. That is a same-morning HIT on the engagement-deceleration row the morning grid marked MISS.

**Internal leadership failure (S2), not an index crash.** META **−1.60%** (largest weight) faded from a green PM print. GOOGL **+0.64%** (Tigress PT to $485) was the only two-name that held. DIS **−2.54%** caught NFLX contagion. Telecom did **not** repeat 09-17’s drag: T **+0.49%**, TMUS **+0.47%**, VZ **−0.69%**. The ETF lost because the growth/entertainment book broke, not because VZ/TMUS/CMCSA reran.

**Not the driver:** leftover Google remedies (T−2), Meta One (T−3), AMX/APP/RUM/ASML, XLC flow prints, oil-down as an XLC up-license, NQ/ES vs-cash rebound.

---

## 2. Audit of morning S0–S4 (use morning numbers, do not rewrite them)

| Sleeve | Morning | Reality | Verdict |
|---|---|---|---|
| **S0 = 0** | Oil-offered + Finviz not red + 08-21 forbids leftover hawkish **−1**; yields “carried,” not a crush | Yields **re-accelerated same day**; duration hit META/XLC. Correct not to score **+1**. Wrong to treat Warsh/SEP as fully digested. | **Directionally closer than engine-up; too willing to call rates stale** |
| **S1 = 0** | Carried ad/AI, no fresh spine HIT/MISS; APP PT cut excluded; NFLX PM **~−2%** noted as color | NFLX WF note **08:36 ET** was a fresh engagement HIT. Not ads/cloud proof, but it is on the sector grid. | **Missed a knowable S1 MISS** |
| **S2 = 0** | ETF PM flat; not 09-17 isolated failure; refused to map XLK **+0.60%** or META/GOOGL PM onto XLC | Correct participation test at the ETF. Cash session then showed **true** breadth failure (META/NFLX/DIS down, GOOGL alone up). | **S2=0 right at the open; did not license up** |
| **S3 = 0** | Trailing redemptions, not a same-morning spike | Fine. Flows were not the tape. | **Hold** |
| **S4 = 0** | Live PM:XLC **−0.01%**; leftover 1d/3d rel banned | PM was not a down confirmation. Cash session **was**. Engine still built tape_anchor **2.334** off NQ **+1.50%** / ES **+1.14%** with PM:XLC **−0.01%**. | **S4=0 was the right ETF print; engine overrode it** |

LLM self-audit said: *all-zero card → flat/flat; 09-16 binds; do not emit up/mild.* Engine emitted **up/mild** anyway. That is the miss, not the factor card.

---

## 3. Interactions / double-count / knowable-at-open

- **Double-count that fired:** NQ/ES vs-cash rebound counted as XLC beta while Channel 1 PM:XLC was **flat**. Same object as 09-16/09-17. LLM named it; engine repeated it (`tape_anchor` 2.334 + `index_carry` 1.215 on an all-zero card).
- **Not double-count:** yields (S0) and NFLX engagement (S1) are two objects. Alphabet PT/antitrust leftover is one Alphabet object and did **not** lift the ETF.
- **Oil vs duration:** oil-offered was used to keep S0 off **−1**. Fair as a cap. It is **not** an XLC up-certificate (08-13 / 08-27). Engine treated the index overlay as if it were.
- **KNOWABLE_AT_OPEN: partially.** Knowable: PM:XLC **−0.01%**, NFLX PM **~−2%**, WF note **08:36 ET**, elevated/rising real yields, 09-16 rule against NQ/ES overlay. Not knowable: META’s fade from PM green to **−1.60%**, DIS **−2.54%**, same-day **+5 bp** 10Y extension.

---

## 4. Outliers inside the sector

- **NFLX −4.31%** — WF Underweight / $57 PT; volume ~51.4M vs ~28M prior. Largest same-name shock. Weight ~4.5% cannot explain **−1.37%** XLC alone.
- **DIS −2.54%** — entertainment contagion, not a telecom leftover.
- **META −1.60%** — load-bearing. High **$690.15**, close **$671.39**. This is the ETF.
- **GOOGL +0.64%** (high **$359.44**, close **$349.54**) — two-name book **split**. Morning “both anchors bid” falsifier never printed at the ETF.
- **Telecom (T/TMUS green, VZ −0.69%)** — **not** yesterday’s drag. Do not retcon 09-17 telecom into today.

Implied: NFLX+DIS explain a slice; META’s de-rate plus failed GOOGL offset explain the rest. Relative **−1.25%** vs a nearly flat SPY is sector-idiosyncratic, not SPX beta.

---

### Evidence

CLAIM: XLC closed −1.37% (112.37 → 110.81) vs SPY −0.12%, rel −1.25%.  
URL: pipeline Channel 1 actuals (deterministic)  
PUBLISHED: 2026-09-18 session  
QUOTE: `ETF_PCT: -1.3707; SPY_PCT: -0.1193; REL_PCT: -1.2514; OPEN: 112.37 CLOSE: 110.81`  
SUMMARY: Gapless open, cash-session drawdown; notable down, lagging SPY.

CLAIM: 10-year yield rose ~5 bp on 09-18, near 5.00%.  
URL: https://www.morningstar.com/news/dow-jones/202609186066/10-year-treasury-yield-rises-to-4995-this-week-data-talk  
PUBLISHED: 2026-09-18 15:47 ET  
QUOTE: “Today it is up 0.049 percentage point… Largest one-day yield gain since Thursday, Sept. 10, 2026… 4.995%”  
SUMMARY: Duration re-tightened in cash hours; leftover FOMC was not inert.

CLAIM: Midday tape showed 10Y +5 bp to 5.00% while indexes slipped.  
URL: https://www.fool.com/coverage/stock-market-today/2026/09/18/stock-market-midday-sept-18-stocks-slip-crypto-gains/  
PUBLISHED: 2026-09-18 ~11:44 ET  
QUOTE: “the 10-Year Treasury yield is up 5 basis points to 5.00%… Netflix slipped almost 5% after Wells Fargo downgraded the stock and highlighted declining viewership.”  
SUMMARY: Same-morning pairing of yields + NFLX engagement hit.

CLAIM: Wells Fargo downgraded NFLX to Underweight, PT $57 from $80, premarket.  
URL: https://seekingalpha.com/news/4644248-wells-fargo-downgrades-netflix-to-underweight-on-engagement-risks  
PUBLISHED: 2026-09-18 08:36 ET  
QUOTE: “Shares of Netflix slipped about 3.5% in the premarket trading on Friday after Wells Fargo downgraded… to Underweight from Equal Weight and cut its price target to $57 from $80, citing concerns over engagement trends”  
SUMMARY: Knowable-at-open S1 engagement HIT; morning grid scored MISS.

CLAIM: NFLX closed −4.31% at $72.07.  
URL: https://www.techi.com/quote/NFLX/historical/  
PUBLISHED: 2026-09-18 16:00 ET  
QUOTE: “Sep 18, 2026 $72.38 $70.11 $72.07 −4.31% 51,350,445”  
SUMMARY: Entertainment outlier; volume nearly doubled.

CLAIM: META −1.60% / GOOGL +0.64% — two-name book split.  
URL: https://www.techi.com/quote/META/historical/ ; https://www.techi.com/quote/GOOGL/historical/  
PUBLISHED: 2026-09-18 16:00 ET  
QUOTE: “META … $671.39 −1.60%”; “GOOGL … $349.54 +0.64%”  
SUMMARY: Largest weight sold; Alphabet’s leftover bid did not lift XLC.

CLAIM: DIS −2.54%; telecom mixed-to-green.  
URL: https://www.techi.com/quote/DIS/historical/ ; https://www.techi.com/quote/T/historical/ ; https://www.techi.com/quote/TMUS/historical/ ; https://www.techi.com/quote/VZ/historical/  
PUBLISHED: 2026-09-18 16:00 ET  
QUOTE: “DIS … $102.67 −2.54%”; “T … +0.49%”; “TMUS … +0.47%”; “VZ … −0.69%”  
SUMMARY: Media/entertainment, not telecom, was the non-META drag.

CLAIM: Alphabet’s morning pop was a Tigress PT raise, later faded to +0.64%.  
URL: https://www.fool.com/investing/2026/09/18/why-alphabet-stock-popped-this-morning/  
PUBLISHED: 2026-09-18 (intraday)  
QUOTE: “jumped as much as 3% in early trading Friday before retracing… Tigress Financial Partners… $485 a share (up from $415)”  
SUMMARY: Single-name overlay, not sector breadth; 09-16 participation test still failed at the ETF.

CLAIM: Engine overrode an all-zero card via NQ/ES tape_anchor.  
URL: injected pipeline JSON  
PUBLISHED: 2026-09-18 pre-open  
QUOTE: `S0–S4 all 0.0; predicted_direction: up; tape_anchor score 2.334; legs NQ 1.5 / ES 1.14 / PM:XLC -0.01`  
SUMMARY: Fourth consecutive XLC dir miss created by index overlay, not by the LLM spine.

---

OUTCOME_BEGIN
SECTOR: Communication Services
ETF: XLC
ETF_PCT: -1.3707
SPY_PCT: -0.1193
REL_PCT: -1.2514
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Duration re-tightening (10Y ~+5 bp) plus META de-rate, with NFLX engagement downgrade as the same-morning entertainment shock
KEY_INTERACTION: Engine mapped NQ/ES vs-cash rebound onto an all-zero card / flat PM:XLC — same 09-16 overlay the LLM correctly refused
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: LLM flat/flat was the right card; official up/mild was an engine overlay miss — S1 also missed a premarket NFLX engagement HIT
OUTCOME_END

---

## RESEARCH APPENDIX

**Queries run**
1. `XLC Communication Services ETF September 18 2026 Meta Alphabet Netflix` (web_search, freshness=week)
2. `why did communication services stocks fall September 18 2026 META GOOGL NFLX XLC` (web_search, freshness=week)
3. `SPY stock market September 18 2026 close Fed yields oil` (web_search, freshness=week)
4. X search: `XLC META GOOGL NFLX communication services sector September 18 2026 why down` (2026-09-18 to 2026-09-19)
5. `META GOOGL NFLX DIS T VZ TMUS CMCSA stock close September 18 2026` (web_search, freshness=week)
6. `Wells Fargo Netflix downgrade September 18 2026 engagement hours watched` (web_search, freshness=week)
7. `XLC vs XLK sector performance September 18 2026 leading lagging` (web_search, freshness=week)
8. `site:finance.yahoo.com XLC historical September 18 2026` (web_search)
9. `"META" "GOOGL" September 18 2026 closed down OR fell OR dropped communication` (web_search, freshness=week)
10. `10-year Treasury yield September 18 2026 close 5.00` (web_search, freshness=week)
11. `Alphabet rises Meta Microsoft slip September 18 2026` (web_search, freshness=week)
12. `TMUS VZ T CMCSA DIS stock September 18 2026 percent change` (web_search, freshness=week)
13. `XLC Communication Services Select Sector close September 18 2026 -1.37` (web_search, freshness=day)
14. `XLK Technology Select Sector close September 18 2026 percent` (web_search, freshness=day)
15. memory_search — unavailable (embedding index metadata missing)

**Fetches**
- Motley Fool midday 09-18; StockStory NFLX; Irish Times close tape; TECHi META/GOOGL/NFLX/DIS/SPY/T/TMUS/VZ; Seeking Alpha WF note; Fool Alphabet Tigress; Morningstar/DJ 10Y Data Talk
- Failed/blocked: Benzinga 403, QZ 403, 24/7 Wall St 403, Yahoo XLC history fetch failed, Yahoo WF article fetch failed, TECHi CMCSA 404

**Key sources and facts taken**
- Pipeline actuals (2026-09-18): XLC −1.3707%, SPY −0.1193%, rel −1.2514%, open 112.37 / close 110.81. **Used as given.**
- Morning card (injected): S0–S4 all 0; LLM flat/flat; engine up/mild; PM:XLC −0.01%; NFLX PM ~−2%; META/GOOGL PM green.
- TECHi (as-of 09-18 16:00 ET): META −1.60% to $671.39; GOOGL +0.64% to $349.54; NFLX −4.31% to $72.07; DIS −2.54% to $102.67; T +0.49%; TMUS +0.47%; VZ −0.69%. https://www.techi.com/quote/META/historical/
- Seeking Alpha (08:36 ET 09-18): WF NFLX Underweight, PT $57 from $80, engagement. https://seekingalpha.com/news/4644248-wells-fargo-downgrades-netflix-to-underweight-on-engagement-risks
- Motley Fool midday (09-18): 10Y +5 bp to 5.00%; NFLX ~−5% on WF viewership note. https://www.fool.com/coverage/stock-market-today/2026/09/18/stock-market-midday-sept-18-stocks-slip-crypto-gains/
- Morningstar/Dow Jones (15:47 ET 09-18): 10Y +0.049 pp to 4.995%, largest 1-day gain since 09-10. https://www.morningstar.com/news/dow-jones/202609186066/10-year-treasury-yield-rises-to-4995-this-week-data-talk
- Motley Fool Alphabet (09-18): Tigress PT $485 from $415; early +3% faded. https://www.fool.com/investing/2026/09/18/why-alphabet-stock-popped-this-morning/
- StockStory (09-18): WF Cahall; top-100 originals viewing hours −21% 2H26 base case; NFLX ~−4.6% after bounce. https://markets.financialcontent.com/stocks/article/stockstory-2026-9-18-netflix-nflx-stock-trades-down-here-is-why
- Irish Times / Reuters (09-18): Wall St digested oil, higher yields, Fed hike; 2Y to 4.74%. Used for macro path, not XLC prints. https://www.irishtimes.com/business/2026/09/18/markets-end-the-week-on-a-down-note/

Memory index is down this run; if you want recall restored: `openclaw memory status --index` or `openclaw memory index --force`.