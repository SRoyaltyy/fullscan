# Sector Outcome — Utilities — 2026-09-15

Actuals: {'etf': 'XLU', 'pct': None, 'spy_pct': None, 'rel': None, 'open': None, 'close': None}

Memory index is unavailable this run (`openclaw memory status --index` / `openclaw memory index --force`), so this autopsy uses the injected 09-15 morning book plus live tape/news, not prior MEMORY.md.

## 0. Facts

XLU closed **−1.20%** (41.82 → **41.32**). Path: opened ~**41.92–41.93** (green vs prior close, matching the morning **PM:XLU +0.20%**), high ~**41.95**, then a full-session grind to low **41.29** / close **41.32**. Open-to-close ≈ **−1.45%**. Volume ~24.1–24.2M.

SPY closed **−0.46%** (760.88 → **757.39**; open 760.12). Relative: **−0.74%**.

That is **down / mild** in absolute terms, and a **relative lag** vs SPY — the exact 08-18 / 09-10 configuration the morning called (fall **and** underperform). Not a crash, not a haven bid.

S&P 500 **−0.45%** to 7,585.73; Dow **−0.63%**; Nasdaq **−0.78%**. Energy was the exception: **XLE +2.17%**.

## 1. What drove the sector

Taxonomy, in order:

1. **Rates rising / bond-proxy selloff (S1 spine).** 10Y **+3.5 bp to 4.996%**, session high **5.016%** — highest since July 2007. That is the duration channel. Utilities are the transmission, not the bid.
2. **Oil as inflation impulse, not FTS (S0 / 09-08).** WTI pushed from the morning ~$103.79 print into the **~$105** area; Brent was already ~$107–108 into the US session. Elevated $90+ oil stayed an inflation/duration negative.
3. **Risk-off with a rising long end (08-18 qualifier).** Equities down, VIX still sub-20/contango in the morning book — no FTS gate. XLU lagged SPY instead of beating it.
4. **Rotation out of the dividend/bond-proxy book into energy.** XLE **+2.17%** vs XLU **−1.20%** is the same oil+yield tape, expressed as sector rotation.

No same-day XLU-wide rate order, load print, or regulatory smash. AI-power remained the stale 1d dampener the morning treated it as.

**CLAIM:** XLU closed 41.32, −0.50 (−1.20%) at 4:00 PM EDT 2026-09-15.  
**URL:** https://stockanalysis.com/etf/xlu/history/  
**PUBLISHED:** as of close 2026-09-15 16:00 ET  
**QUOTE:** “41.32 −0.50 (−1.20%) At close: Sep 15, 2026, 4:00 PM EDT”  
**SUMMARY:** Confirms close-to-close −1.20% after a green open faded.

**CLAIM:** SPY closed 757.39, −3.49 (−0.46%) on 2026-09-15; OHLC 760.12 / 760.35 / 756.15 / 757.39.  
**URL:** https://stockanalysis.com/etf/spy/history/  
**PUBLISHED:** as of close 2026-09-15 16:00 ET  
**QUOTE:** “Sep 15, 2026 760.12 760.35 756.15 757.39 … −0.46%”  
**SUMMARY:** Benchmark down 46 bp; XLU lag ≈ 74 bp.

**CLAIM:** 10Y yield +3.5 bp to 4.996%, high 5.016%, highest since July 2007.  
**URL:** https://www.rttnews.com/3691246/treasuries-move-back-to-the-downside-ahead-of-fed-announcement.aspx  
**PUBLISHED:** 2026-09-15 (Tuesday session wrap)  
**QUOTE:** “the yield on the benchmark ten-year note … advanced 3.5 basis points to 4.996 percent. The ten-year yield climbed as high as 5.016 percent during the day, marking its highest level since July 2007.”  
**SUMMARY:** Live long-end backup through 5% into FOMC.

**CLAIM:** Energy prices and the 5% 10Y were already pressing bonds before the US open; Fed decision Wednesday.  
**URL:** https://www.euronews.com/business/2026/09/15/us-10-year-treasury-yield-breaches-5-as-global-bond-sell-off-deepens  
**PUBLISHED:** 15/09/2026, 07:51 GMT+2  
**QUOTE:** “Brent crude rose to around $107 a barrel on Tuesday morning, while US West Texas Intermediate traded close to $103… The US Federal Reserve announces its decision on Wednesday… money markets placed the probability of an increase at around 93%.”  
**SUMMARY:** Oil + 5% 10Y + hike pricing were knowable into the cash open.

**CLAIM:** XLE closed +2.17% the same day XLU was −1.20%.  
**URL:** https://stockanalysis.com/etf/xle/  
**PUBLISHED:** as of close 2026-09-15 16:00 ET  
**QUOTE:** “65.93 +1.40 (2.17%) At close: Sep 15, 2026, 4:00 PM EDT”  
**SUMMARY:** Oil bid went to energy, not to the bond-proxy defensive.

## 2. Audit of morning S0–S4 (morning numbers, not rewritten)

Morning LLM: S0 −1 / S1 −2 / S2 −1 / S3 −0.5 / S4 −1.0 → **down / mild**. Engine: total **−6.556**, `divergence_flagged True` from tape_anchor **+0.403** (ES +0.34%, ZN −0.46%, PM:XLU +0.20%), overlay shrunk −9.875 → −6.0, calendar size gate on.

| Bucket | Morning | Reality | Verdict |
|---|---|---|---|
| **S0** −1 | Rates + oil + sub-20 VIX dominate; PM green is relative-only | Absolute red; PM green fully faded; oil stayed bid; 10Y held ~5% | **HIT** |
| **S1** −2 | Rates-rising HIT; rotation-out HIT; FTS only partial/relative | Same. No haven bid. Energy absorbed the oil shock | **HIT** |
| **S2** −1 | Regulated core no bid; IPP nested, not confirm | NEE −0.69%, SO −1.21%, DUK −1.05%, D −0.92%, AWK −0.65%; SRE −2.16%; VST +0.57% vs CEG −1.77% | **HIT** |
| **S3** −0.5 | De-risking, no inflow reversal | No washout-reversal evidence; still a down tape | **HIT (soft)** |
| **S4** −1.0 | Unidirectional red multi-horizon tape; 09-14 lesson applied | Close −1.20% / rel −0.74%, second straight ~1%+ lag day | **HIT** |

**09-14 lesson applied correctly:** S4 at −1.0, not −0.5. Overlay shrink was 39% in the engine (−9.875 → −6.0) — still larger than the “≤15% when unidirectional” rule, but the **call** stayed down/mild and was right. Calendar gate keeping magnitude at **mild** was the right cap: this was not a −2% liquidation.

**Premarket +0.20%:** correctly treated as the 09-14 analog (relative-only, do not cap the down call). Open ~41.93 then a one-way fade is the autopsy of that tell.

**Engine vs LLM divergence flag:** LLM `DIVERGENCE_FLAGGED: False` was the better read. Engine `True` was an artifact of a **green tape_anchor** (PM:XLU +0.20% and an ES +0.34% leg that **contradicted Channel 1 ES −0.54%**). Cash SPY −0.46% sided with Channel 1’s red ES, not the engine’s +0.34% ES. ZN −0.46% was the honest duration leg.

**FOMC (09-11):** Morning FedWatch **~56%** was **stale vs live**. Euronews (07:51 GMT+2) already had money-market hike odds **~93%**; Fox Business later printed CME FedWatch **92.5% / 7.5% hold**. The binary was not a 50/50 at the US open. It did **not** break the call, because 09-11 already treated both branches as negative-to-neutral for a bond proxy. It **does** mean “surprise not knowable” was overstated.

**09-10 gate:** VIX 17.49 / contango — no relative-beat authorization. Outcome: rel **−0.74%**. Gate held.

**08-18:** Long end as *cause* of risk-off → XLU transmission channel. Confirmed: down and lagging, while XLE ripped.

**08-12 / 08-28:** AI-power not used as a 1d override; Duke/single-name stayed out of S1. Correct.

## 3. Interactions / double-count / knowable-at-open

**One shock, two expressions:** Hormuz/Saudi-infrastructure oil spike → (a) energy equity bid and (b) inflation/term-premium backup in the long end. Counting “oil” in S0 and “rates rising” in S1 is the intended spine if it is **one** inflation-duration complex. It is **not** a second independent down. The morning mostly did this right (S0 −1, S1 −2 as rates + carried rotation, not oil twice).

**Do not double-count:** risk-off tape + FTS. Morning marked FTS **PARTIAL / relative only** and denied the relative-beat. Close agrees.

**Knowable at open: yes** for direction, relative lag, and “fade the PM green.” 10Y through 5%, oil at war-premium, red global tape, regulated MAP HEAT, and the 08-18 qualifier were all in the morning book (Euronews was out before the US cash open).

**Not fully knowable:** the last 1–2 bp of the 10Y high (5.016%), WTI’s extra push into ~$105, and SRE’s −2.16% reversal of the “ceremony” bid. Those sized the day; they did not flip it.

**Stale-at-open error:** hike odds ~56% vs live ~90%+. Process miss, not a direction miss.

## 4. Outliers inside the sector

Regulated book: **SO −1.21%**, **DUK −1.05%**, **D −0.92%**, **NEE −0.69%** (least-bad large regulated), **AWK −0.65%**. Diversified: **SRE −2.16%** — the morning’s SRE +5.55 “ceremony / tape drift” fully mean-reverted (09-09 static-shock lesson). IPP split: **VST +0.57%** vs **CEG −1.77%** — nested IPP was **not** an XLU bid, exactly as 08-28 / morning S2 said. No single name explains the ETF; the rate-base core did the damage.

---

OUTCOME_BEGIN
SECTOR: Utilities
ETF: XLU
ETF_PCT: -1.20
SPY_PCT: -0.46
REL_PCT: -0.74
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: 10Y through ~5% (high 5.016%) duration unwind, oil-fed — XLU as transmission, not haven
KEY_INTERACTION: Same oil shock bid XLE (+2.17%) and sold the long end; do not count oil and rates as two independent downs
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: down/mild HIT on direction, relative lag, and fade-the-PM-green; S0–S4 signs matched; FedWatch 56% was stale vs ~93% hike odds already live
OUTCOME_END

---

## RESEARCH APPENDIX

**Queries run**
- `XLU utilities ETF September 15 2026 close percentage change`
- `SPY S&P 500 ETF September 15 2026 close percentage change`
- `US stock market September 15 2026 Treasury yield 5 percent utilities oil FOMC`
- `XLU NEE SO DUK VST CEG stock performance September 15 2026`
- X search: `XLU utilities ETF September 15 2026 market close yields oil FOMC` (2026-09-15 to 2026-09-16)
- `site:finance.yahoo.com XLU historical data September 2026`
- `SPY historical prices September 14 15 2026 open close`
- `XLU September 15 2026 open 41.93 close 41.32 volume`
- `FedWatch September 15 2026 rate hike probability FOMC September 16`
- `WTI crude oil close September 15 2026`
- `10-year Treasury yield close September 15 2026`
- `S&P 500 sector performance September 15 2026 utilities energy`
- `XLU September 15 2026 open high low close 41.32`

**Key sources (title + URL + timestamp / as-of)**

1. StockAnalysis — XLU history — https://stockanalysis.com/etf/xlu/history/ — fetched 2026-09-16T00:22Z — close 41.32, −1.20% at 16:00 ET 2026-09-15; AH 41.35.
2. StockAnalysis — SPY history — https://stockanalysis.com/etf/spy/history/ — fetched 2026-09-16T00:22Z — 09-15 OHLC 760.12/760.35/756.15/757.39, −0.46%; 09-14 close 760.88.
3. StockAnalysis — XLE — https://stockanalysis.com/etf/xle/ — fetched 2026-09-16T00:22Z — +2.17% to 65.93.
4. StockAnalysis names, close 2026-09-15 16:00 ET: NEE −0.69% (81.07); SO −1.21% (85.95); DUK −1.05% (117.77); VST +0.57% (141.53); CEG −1.77% (259.89); AWK −0.65% (138.02); SRE −2.16% (81.20); D −0.92% (63.87).
5. KTTC MarketMinute XLU — https://kttc.marketminute.com/quote/NY:XLU/historical — last 41.32, −0.50 (−1.20%), last trade Sep 15th 8:22 PM EDT.
6. Euronews — “US 10-year Treasury yield breaches 5%…” — https://www.euronews.com/business/2026/09/15/us-10-year-treasury-yield-breaches-5-as-global-bond-sell-off-deepens — published 15/09/2026 07:51 GMT+2 — Monday 10Y 5.011% then back below 5%; Tuesday AM Brent ~$107 / WTI ~$103; money-market hike odds ~93%; Fed Wednesday.
7. RTT News — “Treasuries Move Back To The Downside Ahead Of Fed Announcement” — https://www.rttnews.com/3691246/treasuries-move-back-to-the-downside-ahead-of-fed-announcement.aspx — Tuesday session — 10Y +3.5 bp to 4.996%, high 5.016%, highest since July 2007.
8. Fox Business — “Stubborn inflation sets stage for Federal Reserve to hike interest rates” — https://www.foxbusiness.com/economy/stubborn-inflation-sets-stage-federal-reserve-hike-interest-rates — CME FedWatch 92.5% hike / 7.5% hold; 10Y hovering ~5%, highest since 2007; Warsh FOMC Wednesday.
9. Web-search synthesis (Reuters/AP/Benzinga, several 403/401 on fetch): 2026-09-15 cash close Dow −0.63% / S&P −0.45% / Nasdaq −0.78%; energy up on oil; utilities under pressure from 5% 10Y. Reuters URL attempted: https://www.reuters.com/business/wall-st-futures-slip-rising-oil-treasury-yields-compound-ai-anxiety-2026-09-15/
10. X (2026-09-15): TradeApologist — XLU ~−1.1% mid / ~−1.3% close vs XLE +2.3%; zillennialdad — 10Y 5.04%, Brent ~$108.

**Facts taken**
- Tape: XLU −1.20%, SPY −0.46%, rel −0.74%; open green then sold; XLE +2.17%.
- Rates: 10Y 4.996% / high 5.016%.
- Oil: continued war-premium bid (WTI from ~$103.79 morning toward ~$105; Brent ~$107–108).
- Policy: FOMC 2026-09-16; live hike odds ~90%+ into/through 09-15, vs morning book ~56%.
- Breadth: regulated core red; SRE worst large name; IPP split (VST up, CEG down).

**Blocked / unused as primary:** Yahoo history 404; FinanceCharts captcha; Reuters JS wall; AP/TipRanks/TheStreet/Benzinga/GuruFocus 403. Memory search disabled (index metadata missing).