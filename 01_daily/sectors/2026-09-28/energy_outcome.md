# Sector Outcome — Energy — 2026-09-28

Actuals: {'etf': 'XLE', 'pct': 0.0967078621991746, 'spy_pct': -0.7441486246259066, 'rel': 0.8408564868250812, 'open': 62.77000045776367, 'close': 62.099998474121094, 'source': 'yf_download'}

Memory index is unavailable this run (same as the morning note); this autopsy uses the injected prediction, deterministic actuals, and live sources only.

## 0. Facts

XLE **+0.097%** (open **62.77** → close **62.10**). SPY **−0.744%**. Relative **+0.841%**. Path: gap to the high (**62.79**), fade to **61.83**, settle barely green vs Friday’s **~$62.04**. That is a **gap-and-fade**, not a hold of the **+1.21%** premarket.

Oil did the same thing. Morning live WTI was **~$94.44 (+2.20%)**; NYMEX WTI settled **$93.29** (Fri close **$92.41**, session range **$91.25–$96.54**) — still green, most of the open bid gone.

**CLAIM:** XLE closed ~$62.10 after opening $62.77, high $62.79, low $61.83.  
**URL:** https://chartexchange.com/symbol/nyse-xle/historical/  
**PUBLISHED:** 2026-09-28 session  
**QUOTE:** Open 62.77 / High 62.79 / Low 61.83 / Close ~62.10  
**SUMMARY:** Deterministic actuals match: ETF_PCT +0.097, open 62.77, close 62.10.

**CLAIM:** S&P 500 fell 0.77% to 7683.69, largest drop since 2026-08-20.  
**URL:** https://www.morningstar.com/news/dow-jones/202609287080/sp-500-falls-077-to-768369-data-talk  
**PUBLISHED:** 2026-09-28 16:30 ET  
**QUOTE:** “The S&P 500 Index is down 59.72 points or 0.77% today to 7683.69”  
**SUMMARY:** Confirms SPY −0.74% as a real risk-off session, not a quiet tape.

**CLAIM:** WTI CL.1 settled $93.29 on 2026-09-28 after $93.58 / $96.54 / $91.25.  
**URL:** https://markets-data-api-proxy.ft.com/data/commodities/tearsheet/historical?c=WTI+Crude+Oil  
**PUBLISHED:** as of 2026-09-28 21:59 BST  
**QUOTE:** “Monday, September 28, 2026 … 93.58 96.54 91.25 93.29”  
**SUMMARY:** Oil closed ~+0.9% vs Friday $92.41, far below the morning ~+2% print.

## 1. What drove Energy today

Taxonomy: **geopolitical supply-risk premium (HIT, then faded)** + **sector rotation into energy on a risk-off tape (HIT, relative only)** + **real yields / 10Y as a multiple lid (HIT)**. Not a physical-outage day, not an inventory day, not OPEC.

The open was the morning book: Trump’s weekend rejection of Iran’s 7-day Hormuz reopen re-expanded the war premium; oil jumped; XLE was the premarket leader while ES/NQ were soft. Midday tape still described energy as a **defensive gainer** while indexes slid on yields and faded Hormuz-reopen hopes.

The close was digestion, not a new shock. No confirmed fresh kinetic wave, Kpler/Reuters flow rebound to **12.8 mbpd** stayed in the tape, and WTI was also leaned on by diesel-export-ban talk. Crude spiked to **$96.54** and dumped to **$91.25**. XLE never extended the gap (high only **2 cents** above the open) and gave back ~**1.1%** from the open. Captains split: XOM/CVX held green, COP faded.

**CLAIM:** Midday, energy and defensives led while indexes fell on the rejected U.S.–Iran truce and rising yields.  
**URL:** https://www.fool.com/coverage/stock-market-today/2026/09/28/stock-market-midday-sept-28-stocks-slide-as-yields-rise-mongodb-tumbles/  
**PUBLISHED:** 2026-09-28, ~11:56 AM ET  
**QUOTE:** “Energy and consumer defensive stocks are the biggest gainers… The rejection of a U.S.-Iran truce proposal is weighing on sentiment today… Investors fled to defensive sectors as oil prices surged and hopes of an imminent reopening of the Strait of Hormuz faded again. … 10-Year Treasury yield is up 8 basis points to 5.26%.”  
**SUMMARY:** Session narrative = risk-off rotation into energy, not a broad beta bid; yields were the lid.

**CLAIM:** Weekend Trump reject of Iran’s Hormuz roadmap; Qeshm blasts reported, not confirmed by Iranian authorities.  
**URL:** https://www.aljazeera.com/economy/2026/9/27/strait-of-hormuz-tensions-linger-as-iran-and-us-move-further-from-a-deal  
**PUBLISHED:** 2026-09-27  
**QUOTE:** “Hours after US President Donald Trump rejected a diplomatic solution… explosions were heard… Iranian authorities did not confirm any attacks… Wright… ‘running average’ of crude oil in transit was nearly 13 million barrels per day.”  
**SUMMARY:** Premium re-open was diplomatic/standoff, not a documented ≥2% supply outage — morning was right not to fire 09-15 notable.

**CLAIM:** Oil jumped overnight on the reject; flow rebound “failed to offset” war fears at 00:37 CDT.  
**URL:** https://oilprice.com/Latest-Energy-News/World-News/Oil-Moves-Higher-on-Rekindled-War-Fears.html  
**PUBLISHED:** 2026-09-28 12:37 AM CDT  
**QUOTE:** “Brent crude was trading at $107.24… WTI at $94.10… a fresh report about improving oil flows out of the Persian Gulf failed to offset fears of continued war.”  
**SUMMARY:** That was the **open** impulse. By the NYMEX close, WTI was $93.29 — the offset did show up in the fade.

## 2. Audit of morning S0–S4 (use morning numbers, not a rewrite)

| Sleeve | Morning | Reality | Verdict |
|---|---|---|---|
| **S0 +0.5** | Soft index, energy as rotation destination; USD/real yields as lid | SPY −0.74%; XLE rel **+0.84%**; 10Y ~**5.26%** | **Sign right.** Absolute bid did not stick; relative rotation did. +0.5 was the right size. |
| **S1 +2** | One cluster: ~+2% crude + Hormuz premium re-expand; not +3 (no outage); not +1 (fresh weekend increment) | Oil closed ~**+0.9%** after a $5+ range; no physical HIT | **Sign right, persistence too high.** +2 treated a gap driver as a close-to-close factor. Mixed News Judge + flow rebound already argued for a smaller S1. |
| **S2 0** | PM captains with ETF, but gap ≠ independent breadth | XOM **~$162.52 (+1.20%)**, CVX **~$206.37 (+0.94%)**, COP **~$126.04 (~−1%)**; OFS still soft | **Correct.** Majors ≠ whole book; COP/OFS broke the “captains with the ETF” PM read. |
| **S3 0** | No flow print; crowded-long off | Volume ~29–33M, not a dry-up or spike | **Correct.** |
| **S4 0** | Friday −1.44% rel leftover; gap-up clause → S4=0 | Live path **fought** the PM tape (open 62.77 → close 62.10) | **Score hygiene right, interpretation wrong.** They said this was **not** a gap-fade setup. It was. |

Predicted **up / mild** vs actual **flat / flat**. Direction is only a HIT if you count +0.10% as “up”; as a session it is noise around unchanged. Relative **+0.84%** is the real HIT and matches the 09-24 rotation book.

Size_gate / last-30 mag 0.393 / “do not license notable” **saved** the band. Arithmetic 7.946 would have over-claimed if notable had been allowed.

## 3. Interactions / double-count / knowable-at-open

Morning double-count hygiene was good: Trump-reject + Qeshm + oil-up = **one** S1 cluster. They did not restack PM +1.21% into S0, S2, and S4. They did not reuse Friday’s −1.44% rel. They did not treat Finviz $104 / BZ=F −5.2% as live.

The interaction they named but underweighted: **oil-up and 10Y-up are the same inflation cluster.** The geo bid that gapped XLE is also what sent 10Y to **5.26%** and capped the equity multiple. That is why energy can be the **leader** and still close **flat**. Morning said duration “caps extension, does not flip the oil sign” — correct, and that is exactly the close.

**Knowable at open:** weekend reject, live oil ~+2%, PM XLE +1.21%, soft ES/NQ, leftover EIA, Oct 4 OPEC, unprinted NFP/PCE, flow rebound as a **size** offset. **Not knowable at open:** whether the premium would hold through the cash session, the $96.54→$91.25 oil reversal, or that XLE’s high would be the open.

They explicitly declined the gap-fade lesson because 1m rel was not ≥+8% and the weekend increment was “fresh vs Friday,” not day-3 of the same shock. Fair on **shock identity**. Still a **≥1% gap with no extension** — the fade printed anyway.

## 4. Outliers inside the sector

- **XOM +1.2% / CVX +0.9%** held more of the geo bid than XLE. Large-cap integrated ≠ ETF close.
- **COP ~−1%** faded with afternoon crude — high-beta E&P, not the captains-with-ETF PM picture.
- **OFS (HAL ~−1%, SLB ~flat/down)** stayed broken, as the nested override said; they did not veto XLE, they just didn’t help.
- **AESI +13.5%** (idiosyncratic daily gainer) is not an XLE driver.
- Crude’s range was **much** wider than XLE’s. Equities faded the gap; oil actually **traded both a squeeze and a dump**.

---

OUTCOME_BEGIN
SECTOR: Energy
ETF: XLE
ETF_PCT: 0.097
SPY_PCT: -0.744
REL_PCT: 0.841
ACTUAL_DIRECTION: flat
ACTUAL_MAGNITUDE: flat
PRIMARY_DRIVER: Weekend Hormuz-premium gap faded with oil; energy still the relative winner on a yields-up risk-off tape.
KEY_INTERACTION: Oil-up and 10Y-up are one inflation cluster — the geo bid that opened XLE is also the multiple lid that faded it.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Relative rotation HIT; S1 too sticky on a ~2% unconfirmed-premium gap; size_gate correctly blocked notable; absolute close was flat, not mild-up.
OUTCOME_END

---

# RESEARCH APPENDIX

**Queries run**
- memory_search: `Energy XLE sector prediction lessons 2026-09-28 gap fade oil Hormuz` (index unavailable)
- memory_search: `sector energy XLE post-session review magnitude direction hits` (index unavailable)
- web_search: `XLE energy sector stock market September 28 2026`
- web_search: `WTI Brent crude oil price September 28 2026 Hormuz Iran`
- web_search: `stock market today September 28 2026 energy XLE SPY oil`
- web_search: `XOM CVX COP MPC VLO stock performance September 28 2026`
- web_search: `WTI crude oil closed September 28 2026 oil prices jump war fears`
- web_search: `site:investopedia.com stock market today September 28 2026 energy oil yields`
- web_search: `XLE historical data September 28 2026 open high low close`
- web_search: `"energy" "XLE" OR "Exxon" OR "Chevron" "September 28" 2026 stocks close`
- web_search: `SLB HAL BKR oilfield services stocks September 28 2026`
- web_search: `WTI crude oil settlement September 28 2026 $93`
- web_search: `10-year Treasury yield 5.26 September 28 2026 energy stocks`
- web_search: `COP ConocoPhillips close September 28 2026`
- web_search: `Brent crude oil close September 28 2026`
- web_search: `energy sector biggest gainer stocks slide yields September 28 2026`
- x_search: `XLE energy oil WTI close September 28 2026 Hormuz Iran rotation` (2026-09-28 to 2026-09-29)
- web_fetch: Motley Fool midday 2026-09-28
- web_fetch: Post-Gazette 2026-09-28 (404)
- web_fetch: AP stocks (403)
- web_fetch: Oilprice “Oil Moves Higher on Rekindled War Fears”
- web_fetch: Reuters oil-rebounds (401)
- web_fetch: Yahoo live markets (failed)
- web_fetch: Investopedia live markets (403)
- web_fetch: Morningstar/DJ S&P 500 data talk
- web_fetch: oilprice.com homepage
- web_fetch: FT WTI historical
- web_fetch: Stocknear XLE history (403)
- web_fetch: Al Jazeera Hormuz 2026-09-27

**Key sources and facts taken**

1. **Deterministic actuals (injected)** — XLE +0.0967%, SPY −0.744%, rel +0.841%, open 62.77, close 62.10.
2. **FT / LSEG WTI historical** — https://markets-data-api-proxy.ft.com/data/commodities/tearsheet/historical?c=WTI+Crude+Oil — fetched 2026-09-28 ~21:10 UTC. 2026-09-28 O/H/L/C **93.58 / 96.54 / 91.25 / 93.29**; 2026-09-25 close **92.41**.
3. **Motley Fool, Emma Newbery, midday 2026-09-28** — https://www.fool.com/coverage/stock-market-today/2026/09/28/stock-market-midday-sept-28-stocks-slide-as-yields-rise-mongodb-tumbles/ — S&P −0.77% to 7684; energy/defensives lead; 10Y **5.26% (+8 bp)**; truce reject / Hormuz hopes fade.
4. **Dow Jones Market Data via Morningstar, 16:30 ET 2026-09-28** — https://www.morningstar.com/news/dow-jones/202609287080/sp-500-falls-077-to-768369-data-talk — S&P **7683.69, −0.77%**, largest drop since 2026-08-20.
5. **Irina Slav, Oilprice, 2026-09-28 12:37 AM CDT** — https://oilprice.com/Latest-Energy-News/World-News/Oil-Moves-Higher-on-Rekindled-War-Fears.html — Brent **$107.24**, WTI **$94.10**; Trump reject; WSJ midterms-strikes color; flow rebound failed to offset **at the open**.
6. **Al Jazeera, 2026-09-27** — https://www.aljazeera.com/economy/2026/9/27/strait-of-hormuz-tensions-linger-as-iran-and-us-move-further-from-a-deal — Trump rejected 7-day Hormuz reopen; Qeshm explosions **unconfirmed**; Wright ~**13 mbpd** running average.
7. **XLE OHLC aggregators** — ChartExchange / FT ETF historical / Stocknear (search) — Open **62.77**, High **62.79**, Low **61.83**, Close **~$62.10**, volume ~29–33M.
8. **Captain closes (Yahoo/Stockscan via search)** — XOM **$162.52 (+1.20%)**; CVX **$206.37 (+0.94%)**; COP **~$126.04 (~−1%)**.
9. **OFS snapshots (search)** — HAL ~−1%; SLB ~flat/slightly down — nested lag, not the parent.
10. **X search 2026-09-28/29** — sentiment on Trump reject / Hormuz flows; **not** used for official XLE or WTI settlement (those come from FT/actuals).

**Rejected as live close oil:** morning Oilprice **$94.44 (+2.20%)** (open impulse, not settlement); Channel 1 Finviz **$104.16 / BZ=F −5.2%** (already rejected pre-open); search blurbs putting Brent at $98–$100 (inconsistent with Oilprice/ICE color; unused for the driver).