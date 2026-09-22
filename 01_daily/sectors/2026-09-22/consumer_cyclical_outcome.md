# Sector Outcome — Consumer Cyclical — 2026-09-22

Actuals: {'etf': 'XLY', 'pct': 0.08910137318900446, 'spy_pct': -0.015513266604716414, 'rel': 0.10461463979372088, 'open': 112.9800033569336, 'close': 112.33000183105469, 'source': 'yf_download'}

Memory index is paused (embedding metadata missing); this autopsy uses the injected morning card, deterministic actuals, and live sources only.

## 0. Facts

XLY **+0.089%**, SPY **−0.016%**, relative **+0.105%**. Close-to-close that is **flat / flat**, not a down day.

Path (not in the % print): open **112.98**, close **112.33** — a **~0.58% fade from a green open** (range ~111.98–113.29). Prior close ~112.23. Cash tape was T+1 digestion after Monday’s XLY +1.30% / SPY +1.55%, not a fresh sector impulse.

SPX cash was essentially unchanged (AP: S&P 500 **−0.06 pts to 7,764.64**); Nasdaq **+0.5%** to a record; Dow **−0.4%**. Banks (JPM **−3.4%**) and energy (oil offered) pinned SPY; that is why XLY’s tiny green beat SPY without a discretionary expansion.

---

## 1. What drove the sector

Taxonomy-aligned, in order:

**S0 shared macro — mixed, not a signed shock.** Monday’s AI/Nasdaq melt-up was leftover. Cash SPX flat, Nasdaq bid, financials/energy offered. Oil **kept falling** (Brent settle **$99.25, −1.1%** after a sub-$98 print in the morning). 10Y **4.95% vs 4.96%** — a 1 bp ease, not a duration event. Williams/Jefferson were Treasury-market-structure, not a path binary.

**S1 sector factors — split, net ~0.** No CPI/NFP/retail-sales print. Gasoline **level** still a tax; **increment** was relief. Nested AZO Q4: EPS beat / sales miss, stock **+3.3%** — Auto Parts, not the AMZN/TSLA/HD spine. Thor/On Holding were even more nested.

**S2 breadth — split leadership, not expansion.** Spine diverged: **AMZN −1.12%**, **TSLA +0.82%**, **HD ~+2.7–2.9%**. Home-improvement/auto-parts bid vs Amazon drag cancelled at the ETF. Staples still won the consumer pair (**XLP ~+0.96–0.99%** vs XLY +0.09%).

**S3/S4 — no flow or tape confirmation.** Close-to-close 1d and 1d rel are both **sub-gate**. The only “tape” was open-to-close fade, which is path, not a scored down close.

Primary driver is **T+1 mixed digestion with an AMZN vs HD/TSLA/AZO offset**, not a consumer-health print and not a risk-off dump.

---

## 2. Audit of morning S0–S4 (use morning numbers, do not rewrite them)

| Sleeve | Morning | Reality | Verdict |
|---|---|---|---|
| **S0 = 0** | Leftover AI banned; oil offered not 08-11; futures mixed; speakers = structure; real-yield hangover not kinetic | SPX flat, oil still down, 10Y −1 bp, Williams on clearing/ample reserves — **no path comment** | **Correct** |
| **S1 = 0** | Stale retail/claims/SAAR/confidence/credit; AZO nested; gas *level* ≠ live spike | No spend print; AZO +3.3% nested; oil increment relief | **Correct** |
| **S2 = 0** | Split MAP HEAT; 08-28 no restack of 1w/1m lag | AMZN down, HD/TSLA/AZO up — split, not breakdown | **Correct** |
| **S3 = 0** | Trailing 5d inflow not a 1-day lid | No same-day creation/redemption story | **Correct** |
| **S4 = 0** | 1d rel −0.25% sub-gate | Close +0.09% / rel +0.10% still sub-gate | **Correct** |

LLM honest band was **flat**. Engine still printed **down / mild** from `tape_anchor −0.324` (ES −0.07%, NQ −0.07%, ER2 +0.08%) + `index_carry −0.124` against an **all-zero leading card**. That is exactly the **09-16 discarded-anchor** pathology the morning self-audit already named. `size_gate=True` was right for magnitude; it did not save direction because the engine had already signed the card from leftover futures.

Predicted **down/mild** vs actual **flat/flat** = **direction miss, magnitude miss** at the engine layer; **hit** at the factor-card layer.

---

## 3. Interactions / double-count / knowable-at-open

- **Do not double-count** leftover Monday AI into S0 **and** S4. Morning banned it; the engine re-imported it via ES/NQ vs prior close.
- **Do not double-count** oil: live *sign* was relief (S0/S1 gasoline-spike = miss). Level remains a 1W/1M hangover, not today’s close.
- **Do not double-count** duration: DFII10 2.68 / DGS10 ~5% was a *level*; today’s 10Y move was 1 bp.
- **AZO must not drive the parent.** It didn’t at the ETF (+0.09% with AMZN −1.1%). Nested hit, parent flat — morning call on that was right.
- **XLY vs SPY relative is contaminated by XLF:** JPM −3.4% made SPY slightly red. Rel +0.10% is **not** a rotation-into-discretionary certificate (XLP still beat XLY).

**Knowable at open:** mixed/flat futures, oil offered, unsigned S0–S4, no data binary, speakers = structure, size_gate. **Not knowable:** AMZN −1.1% vs HD ~+3% split, AZO +3.3% reaction size, open 112.98 → close 112.33 fade, XLP beating XLY while banks dragged SPY.

---

## 4. Outliers inside the sector

- **AMZN −1.12%** (close $255.57) — largest weight, the drag.
- **TSLA +0.82%** (close $378.39) — offset, not a new catalyst day.
- **HD ~+2.7–2.9%** — duration/home-improvement sleeve, not Amazon beta.
- **AZO +3.3%** — nested Auto Parts EPS beat / sales miss.
- **ONON +7.6%**, **THO +5.5%** — nested/idiosyncratic; do not map to XLY.
- **XLP ~+1%** vs **XLY +0.09%** — staples-over-discretionary still the 1d consumer pair.

---

## Evidence

CLAIM: XLY close-to-close +0.089%; SPY −0.016%; rel +0.105%; open 112.98 / close 112.33  
URL: deterministic Channel 1 actuals (this run)  
PUBLISHED: 2026-09-22 session  
QUOTE: ETF_PCT 0.08910137318900446; SPY_PCT −0.015513266604716414; REL_PCT 0.10461463979372088  
SUMMARY: Flat vs prior close after a green open fade.

CLAIM: S&P 500 virtually unchanged; Nasdaq +0.5% record; Dow −0.4%; Brent $99.25 −1.1%; 10Y 4.95% from 4.96%; JPM −3.4%; AZO +3.3%  
URL: https://www.durangoherald.com/articles/associated-press/wall-street-inches-higher-as-oil-prices-bond-yields-retreat-for-fifth-consecutive-day/  
PUBLISHED: 2026-09-22  
QUOTE: “The S&P 500 was virtually unchanged, edging down by less than 0.1%… Nasdaq composite added 0.5%… Brent settled at $99.25 per barrel, down 1.1%… AutoZone rose 3.3%… JPMorgan Chase fell 3.4%… 10-year Treasury yield edged down to 4.95% from 4.96%.”  
SUMMARY: Mixed cash tape, oil/yields slightly offered, banks dragged SPX, AZO nested beat.

CLAIM: AMZN 09-22 close $255.57, return −1.12%  
URL: https://www.techi.com/quote/AMZN/historical/  
PUBLISHED: 2026-09-22 16:00 EDT  
QUOTE: “Sep 22, 2026 $255.38 $259.00 $253.79 $255.57 −1.12%”  
SUMMARY: Largest XLY weight was the drag.

CLAIM: TSLA 09-22 close $378.39, return +0.82%  
URL: https://www.techi.com/quote/TSLA/historical/  
PUBLISHED: 2026-09-22 16:00 EDT  
QUOTE: “Sep 22, 2026 $379.07 $379.46 $372.88 $378.39 +0.82%”  
SUMMARY: Second-largest weight offset Amazon.

CLAIM: Williams remarks were market structure / ample reserves / clearing, not the rate path  
URL: https://www.newyorkfed.org/newsevents/speeches/2026/wil260922  
PUBLISHED: 2026-09-22  
QUOTE: “this operational framework has proven to be highly effective at delivering interest rate control… as markets evolve, policy tools must evolve”  
SUMMARY: Confirms morning “distribution widener, not mean shift.”

CLAIM: XLP closed ~$82.71–$82.73, ~+0.96–0.99%  
URL: https://stocknear.com/etf/XLP/history  
PUBLISHED: 2026-09-22  
QUOTE: close ~$82.71–$82.73 vs prior $81.92  
SUMMARY: Staples beat discretionary on the day; XLY>SPY was bank/energy contamination, not a rotation into XLY.

---

OUTCOME_BEGIN
SECTOR: Consumer Cyclical
ETF: XLY
ETF_PCT: 0.089
SPY_PCT: -0.016
REL_PCT: 0.105
ACTUAL_DIRECTION: flat
ACTUAL_MAGNITUDE: flat
PRIMARY_DRIVER: T+1 mixed digestion — AMZN drag offset by TSLA/HD and nested AZO, oil/yields slightly offered, SPY pinned by banks
KEY_INTERACTION: Engine signed down from leftover ES/NQ tape_anchor against an all-zero S0–S4 card; oil-relief + XLF drag inflated XLY vs SPY without a discretionary expansion
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: LLM S0–S4=0 / honest flat was right; engine mild-down from leftover futures was the 09-16 discarded-anchor miss
OUTCOME_END

---

## RESEARCH APPENDIX

**Queries run**
- XLY consumer discretionary ETF September 22 2026
- stock market recap September 22 2026 S&P 500 consumer discretionary Amazon Tesla
- AutoZone earnings September 22 2026 AZO stock
- X search: XLY consumer discretionary Amazon Tesla Home Depot market September 22 2026 what moved the sector
- Amazon Tesla Home Depot stock close September 22 2026
- Wall Street recap September 22 2026 oil yields Fed Williams Jefferson
- XLY vs XLP sector performance September 22 2026 consumer discretionary staples
- AMZN stock September 22 2026 percent change close
- TSLA HD MCD LOW BKNG stock performance September 22 2026
- John Williams Fed Treasury Market Conference September 22 2026 remarks
- WTI crude oil price September 22 2026 close
- XLP ETF close September 22 2026 percent change
- Home Depot HD stock September 22 2026 close percent
- Tesla TSLA September 22 2026 close percent change
- S&P 500 sector performance September 22 2026 consumer discretionary financials energy
- memory_search: Consumer Cyclical XLY sector outcome lessons (index paused)

**Key sources (title + URL + timestamp) and facts taken**
- Deterministic actuals (this run, 2026-09-22) — XLY +0.089%, SPY −0.016%, rel +0.105%, open 112.98, close 112.33
- Investing/Finviz/Yahoo search cluster — XLY close ~$112.33, prior ~$112.23, range $111.98–$113.29
- AP via Durango Herald / Boston Herald, 2026-09-22 — SPX ~unchanged 7,764.64; Nasdaq +0.5% to 27,244.28; Dow −185 / −0.4%; Brent $99.25 −1.1%; 10Y 4.95% from 4.96%; AZO +3.3%; JPM −3.4%; THO +5.5%; ONON +7.6% — https://www.durangoherald.com/articles/associated-press/wall-street-inches-higher-as-oil-prices-bond-yields-retreat-for-fifth-consecutive-day/
- TECHi AMZN historical, as of Sep 22 16:00 EDT — close $255.57, −1.12% — https://www.techi.com/quote/AMZN/historical/
- TECHi TSLA historical, as of Sep 22 16:00 EDT — close $378.39, +0.82% — https://www.techi.com/quote/TSLA/historical/
- NY Fed Williams speech “Do You Remember?”, 2026-09-22 — clearing ahead of schedule; ample-reserves framework; no rate-path call — https://www.newyorkfed.org/newsevents/speeches/2026/wil260922
- Stocknear / ChartExchange search — XLP close ~$82.71–$82.73, ~+0.97% vs $81.92 — https://stocknear.com/etf/XLP/history
- HD search cluster (Stocknear/EODData) — HD ~+2.7–2.9% on 2026-09-22
- AutoZone IR / Benzinga / Zacks search — AZO Q4 sales $6.6B (+5.6%, slight miss), EPS $56.05 beat; stock +3–6% depending on snapshot; AP close-side print +3.3%
- Reuters/TipRanks search (futures, 2026-09-22) — T+1 pause after 09-21 AI rally; used as context, not close prints
- X posts 2026-09-22 — noisy midday XLY greens / homebuilder chatter; **not** used to override close actuals (XLY close is +0.09%, not +0.8%)

**Not used / failed fetches:** Reuters, Yahoo Finance, Benzinga, Investopedia, Zacks article pages (401/403/bot walls). Memory index unavailable.