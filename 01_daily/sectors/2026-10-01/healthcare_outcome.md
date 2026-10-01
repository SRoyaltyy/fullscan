# Sector Outcome — Healthcare — 2026-10-01

Actuals: {'etf': 'XLV', 'pct': -1.318133977460445, 'spy_pct': 0.17832832997064507, 'rel': -1.49646230743109, 'open': 168.14500427246094, 'close': 166.1999969482422, 'source': 'yf_download'}

Memory search is paused this run (embedding metadata missing). Review uses the injected morning card, deterministic actuals, and live sources only.

## 0. Facts

XLV cash session **2026-10-01**: open **168.15** → high **168.60** → low **165.83** → close **166.20**. Sold from the open; the midday SPX bounce did not reclaim the open.

| Object | Print |
|---|---|
| XLV | **−1.318%** |
| SPY | **+0.178%** |
| Relative | **−1.496%** |
| Path | gap/open near 168.15, never recovered; close 166.20 |

That is **down / notable** in absolute terms and a **hard relative underperformance** on a mildly green SPY tape — a second consecutive relative smash after Sep 30’s 1d rel **−1.15%**.

---

## 1. What drove Healthcare today

**Primary (taxonomy: sector rotation out of healthcare + rate/duration drag, not flight-to-safety).**  
The session was a modest, tech-led risk-on close (SPX **+0.20%**, Nasdaq ~flat, XLK/semis/software led the bounce) after an early yield scare. The 10Y tagged a **24-year high (~5.34% intraday)** then faded to ~**5.25%**. Healthcare did **not** catch a haven bid. It stayed offered with other rate-sensitive sleeves while tech/energy absorbed the midday recovery. That is the same **rotation-out** shape the morning PM board already showed (XLV worst at **−0.53%**, XLK/XLC green).

**Secondary, nested — do not restack as a fresh sector binary:**
- **Mega-cap weight follow-through:** LLY (top XLV weight) **−0.92%** on Oct 1 after **−2.33%** on Sep 30. That is leftover large-cap drag, not a new GLP-1/FDA shock.
- **MA / payer overhang (standing, not a same-morning rate cut):** 2027 MA exits and footprint shrink were in the Sep 29–30 trade press; an UNH investor suit survived dismissal Wed; Oct 1 Medicaid coverage ended for refugees/some legal immigrants (provider-payment squeeze). These are **utilization/reimbursement pressure**, not a CMS 2027 MA-rate HIT.
- **Drug-pricing residual:** Trump GLOBE MFN-style Medicare Part B pilot finalized **Sep 30**, watered down (four companies). IRA cycle-3 offers were **paid Sep 30**. Not a mega-cap Rx headline that should have dominated Net.
- **Single-name positives failed to carry the ETF:** ABBV JUVMO (tavapadon) FDA approval was **Sep 28**; AMGN dazodalibep Ph3 was **Sep 22**. Taxonomy correctly forbade them from dominating. They did not.

**Not the driver:** India hospital pharmacy mark-up noise is not an XLV factor. No 8:30 CPI/NFP. FOMC is paid (09-16). Q3 HC mega-cap earnings not open.

---

## 2. Audit of morning S0–S4 (use morning numbers, not post-close rewrites)

Morning card: **down / mild**, total **−3.878**, S0 **−0.5** / S1 **−0.5** / S2 **0** / S3 **0** / S4 **−0.5**, LLM divergence **True**, engine divergence **False**, **09-18 up-cap fired** (PM:XLV **−0.53% ≤ 0**).

| Sleeve | Morning | Reality | Verdict |
|---|---|---|---|
| **S0 shared macro −0.5** | Modest NQ≥ES risk-on, 10Y/real yields hostile, PM rotation-out not FTS; forbade fat ES-carry up | SPY **+0.18%**, XLV **−1.32%**, 10Y spiked then faded; XLK led the bounce | **Sign HIT, size light.** The relative smash was larger than a −0.5 lean. 09-18 up-cap was the correct veto of an ES-up call. |
| **S1 sector −0.5** | MA 2027 rule stale; IRA paid; no same-morning mega-cap Rx; rotation-out + XBI duration drag; ABBV/AMGN nested | Rotation-out confirmed. MA *exits/plan cuts* were live in the overnight clips but not a fresh CMS-rate binary. Nested FDA/trial did not lift XLV. | **Sign HIT.** Underweighted standing MA/payer pressure; correctly refused to let ABBV/AMGN dominate. |
| **S2 breadth 0** | Two large-cap readouts vs red parent tape = mixed-to-soft | LLY follow-through, ETF sold open-to-close; MCK was a distributor outlier, not breadth | **Too benign.** Breadth was soft, not offset. Nested positives did not show up in the ETF. |
| **S3 flows 0** | No flow print; crowded-long gone | Unfalsifiable. No forced-liquidation tape, but selling continued anyway | **Neutral OK** (no data). |
| **S4 tape −0.5** | 1d rel **−1.15%** confirms rotation-out and the up-cap; 1w still +0.45% so not a standalone down thesis | Open 168.15 → close 166.20 continued the PM offer. 1w leftover did **not** mean-revert | **Confirmation HIT.** The 1d rel was the live object; 1w repair was a trap. |

**Engine vs LLM:** Engine **down/mild** (tape_anchor −2.042 off PM −0.53% / ES +0.17%) was closer to the outcome than the LLM’s “flat-to-mild-down, confidence 0.42.” The LLM divergence flag was internally consistent with green futures vs red PM, but **09-18 + S4 non-zero** already forbade buying the ES-carry. Do not punish the engine for calling down; punish the **mild** cap and the **S2 = 0** gift.

---

## 3. Interactions / double-count / knowable-at-open

- **Same-shock:** 10Y/real-yield was scored once in S0. Intraday 5.34% was an *intensification* of the morning DGS10 **5.26** fact, not a second factor. Oil-offered was correctly **not** scored as FTS or rotation. Good.
- **Mild double-count:** PM worst-in-class + Sep 30 1d rel showed up in both S0 (funding/rotation) and S1 (sector rotation) and again as S4 confirmation. Sign was right; stacking still did not produce a notable band because size_gate + |sleeve| = 0.5 kept the call at mild.
- **Nested HEAT:** ABBV/AMGN/BDX/BSX/CI correctly nested. They did not dominate — and they did not save the tape. Lesson holds.
- **Knowable at open: yes.** PM:XLV **−0.53%** worst on the board, 1d rel **−1.15%**, DGS10 at/near 24-year highs, NQ≥ES but **not** a ≥1% rip, FOMC paid, 09-18 up-cap on. The cash path (never reclaim the open) was the PM offer continuing. Magnitude extra came from the **intraday yield spike** and **LLY leftover**, both visible as risk at the open even if the 5.34% print was not.

---

## 4. Outliers inside the sector

- **MCK +5.27%** midday (Motley Fool) — distributor/earnings-style outlier vs parent XLV. Did not lift the ETF.
- **LLY −0.92%** (after −2.33% Sep 30) — mega-cap weight, continuation, not a new catalyst. This is the name that mattered for XLV beta.
- **REGN upgrade / SNY–REGN immunology deal** — single-name, not sector breadth.
- **FHTX / LQDA** smashes — small/mid biotech, not XLV weights.
- **India hospital pharmacy mark-up** — not an XLV driver.

---

### Evidence

CLAIM: XLV 2026-10-01 open 168.15 / high 168.60 / low 165.83 / close 166.20 (~−1.32%).  
URL: https://finance.yahoo.com/quote/XLV/history  
PUBLISHED: 2026-10-01 (session close)  
QUOTE: “Open: 168.15; High: 168.60; Low: 165.83; Close: 166.20”  
SUMMARY: Matches injected actuals (open 168.145, close 166.20). Sold from the open.

CLAIM: S&P 500 +0.20% to 7,666; 10Y tagged >20-year/24-year highs then pulled back; bounce paced by XLK/semis/software, XLI, XLE.  
URL: https://www.eoption.com/market-review-october-01-2026/  
PUBLISHED: 2026-10-01  
QUOTE: “U.S. stocks started the day flat, came under pressure early to late morning as Treasury yields extended their recent run higher as the 10-yr and 30-yr yield both hit more than 20 years high again … The S&P 500 bounced more than 60 points off morning lows to end the day higher, once again paced by strength in technology (XLK)”  
SUMMARY: Green index close was a midday yield-fade bounce led by tech, not a healthcare bid.

CLAIM: Midday, 10Y ~5.29% and indexes were still red; high yields competing with dividend/defensive stocks.  
URL: https://www.fool.com/coverage/stock-market-today/2026/10/01/stock-market-midday-oct-1-stocks-edge-lower-as-treasury-yields-surge-to-24-year-high/  
PUBLISHED: 2026-10-01 ~11:31 a.m. ET  
QUOTE: “Stocks Edge Lower as Treasury Yields Surge to 24-Year High” / “High Treasury yields also compete with dividend-paying stocks”  
SUMMARY: Confirms the morning rates-hostile read intensified into the cash session before the afternoon fade.

CLAIM: LLY Oct 1 close $1,146.43, −0.92%, after Sep 30 −2.33%.  
URL: https://www.techi.com/quote/LLY/historical/  
PUBLISHED: 2026-10-01  
QUOTE: “Oct 1, 2026 … Close $1,146.43 −0.92%” / “Sep 30, 2026 … Close $1,157.08 −2.33%”  
SUMMARY: Top XLV weight continued lower; leftover mega-cap drag, not a fresh label.

CLAIM: 2027 MA insurer exits piling up; UNH suit proceeds; Medicaid coverage for refugees/some legal immigrants ends Oct 1.  
URL: https://kffhealthnews.org/morning-briefing/thursday-october-1-2026/  
PUBLISHED: 2026-10-01  
QUOTE: “The Medicare Advantage company exits are piling up. At least 11 health insurance companies … are abandoning the program for 2027” / “Refugees, asylum seekers and some other immigrants with legal status will lose Medicaid coverage Oct. 1”  
SUMMARY: Standing MA/payer and coverage-access pressure in the overnight clips — not a same-morning CMS rate surprise, but a live S1-negative that morning scored only as “stale MA / standing utilization.”

CLAIM: GLOBE Medicare drug-pricing final rule published Sep 30, applies to only four companies, saves far less than proposed.  
URL: https://www.statnews.com/2026/09/30/trump-final-globe-rule-will-save-medicare-much-less-money-on-drugs/  
PUBLISHED: 2026-09-30  
QUOTE: “The Trump administration published a final rule aimed at lowering Medicare prices for drugs administered in doctor offices. It applies to only four companies.”  
SUMMARY: Residual MFN/drug-pricing print, paid before the cash open; correctly not a mega-cap Rx 08-14 HIT.

CLAIM: ABBV JUVMO FDA approval Sep 28; AMGN dazodalibep Ph3 Sep 22 — both T+n vs Oct 1 cash.  
URL: https://news.abbvie.com/2026-09-28-U-S-FDA-Approves-AbbVies-JUVMO-TM-tavapadon-for-Parkinsons-Disease  
PUBLISHED: 2026-09-28  
QUOTE: “U.S. FDA Approves AbbVie’s JUVMO (tavapadon) for Parkinson’s Disease”  
SUMMARY: Morning was right to nest these. They did not dominate, and they did not offset the ETF.

---

OUTCOME_BEGIN
SECTOR: Healthcare
ETF: XLV
ETF_PCT: -1.318
SPY_PCT: 0.178
REL_PCT: -1.496
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Rotation out of healthcare on a tech-led, 10Y-spike tape; XLV sold from the open and missed the midday bounce.
KEY_INTERACTION: 24-year-high yields + leftover Sep-30 relative smash (−1.15% 1d rel) compounded; nested ABBV/AMGN positives did not offset LLY follow-through.
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Direction HIT (down); magnitude undershot (mild vs notable) — S0/S1 sign right but too small, S2 too generous on nested large-cap readouts, 09-18 up-cap correctly forbade ES-carry up.
OUTCOME_END

---

## RESEARCH APPENDIX

**Queries run**
- memory_search: Healthcare XLV 2026-10-01 sector prediction outcome *(unavailable — index metadata missing)*
- memory_search: XLV healthcare sector drivers CMS IRA biotech *(unavailable)*
- web_search: XLV healthcare ETF October 1 2026 why down
- web_search: SPY stock market October 1 2026 healthcare sector XLV
- web_search: KFF Health News first edition Thursday Oct 1 2026
- web_search: Treasury yields October 1 2026 10-year healthcare stocks rotation
- web_search: AbbVie JUVMO tavapadon Amgen dazodalibep October 1 2026
- web_search: sector performance October 1 2026 XLV XLK XLE XLF
- web_search: XLV top holdings October 1 2026 LLY UNH JNJ ABBV performance
- web_search: site:finance.yahoo.com XLV historical October 1 2026
- web_search: stock market news October 1 2026 S&P 500 healthcare underperform yields
- web_search: Eli Lilly UnitedHealth AbbVie Johnson Johnson stock October 1 2026
- web_search: UnitedHealth UNH stock down October 1 2026 Medicare Advantage
- web_search: "XLV" "October 1" 2026 sector worst OR weakest OR underperform
- x_search: What happened to XLV healthcare stocks on October 1 2026?
- x_search: XLV OR healthcare sector OR UNH OR LLY stock market October 1 2026 underperform yields
- web_fetch: kffhealthnews.org morning-breakout / morning-briefing Oct 1 2026
- web_fetch: eoption.com market-review-october-01-2026
- web_fetch: fool.com midday Oct 1 yields
- web_fetch: statnews.com GLOBE rule
- web_fetch: techi.com LLY historical
- web_fetch attempts that failed/blocked: etfaction.com (403), reuters.com (401), apnews (403), benzinga (403), modernhealthcare.com (403), yahoo live article (fetch failed)

**Key sources (title + URL + timestamp + facts taken)**
1. **Injected deterministic actuals** (pipeline, 2026-10-01 close) — XLV −1.318%, SPY +0.178%, rel −1.496%, open 168.145 / close 166.20.
2. **Yahoo Finance XLV history** — https://finance.yahoo.com/quote/XLV/history — 2026-10-01 — OHLC 168.15 / 168.60 / 165.83 / 166.20, volume ~9.97M.
3. **eOption Market Review: October 01, 2026** — https://www.eoption.com/market-review-october-01-2026/ — 2026-10-01 — SPX +0.20% to 7,666; yield spike then fade; bounce led by XLK/SOX/IGV, XLI, XLE; 10Y ~5.34% high then ~5.25%; jobless claims 197k; ISM mfg 54.5.
4. **Motley Fool midday Oct 1** — https://www.fool.com/coverage/stock-market-today/2026/10/01/stock-market-midday-oct-1-stocks-edge-lower-as-treasury-yields-surge-to-24-year-high/ — ~11:31 a.m. ET 2026-10-01 — indexes still red midday; 10Y 5.29%; MCK +5.27%; yields competing with dividend stocks.
5. **TECHi LLY historical** — https://www.techi.com/quote/LLY/historical/ — prices as of Oct 1, 4:04 p.m. EDT — LLY Oct 1 −0.92% to $1,146.43; Sep 30 −2.33%.
6. **KFF Health News Thursday, October 1, 2026 briefing** — https://kffhealthnews.org/morning-briefing/thursday-october-1-2026/ — 2026-10-01 — MA exits for 2027; Medicaid immigrant coverage ends Oct 1; UNH investor suit proceeds; GLOBE rule clip.
7. **STAT, Sep 30, 2026** — https://www.statnews.com/2026/09/30/trump-final-globe-rule-will-save-medicare-much-less-money-on-drugs/ — GLOBE final rule, four companies, 96% less savings than proposed.
8. **AbbVie PR, Sep 28, 2026** — https://news.abbvie.com/2026-09-28-U-S-FDA-Approves-AbbVies-JUVMO-TM-tavapadon-for-Parkinsons-Disease — JUVMO FDA approval (T+3 vs this session).
9. **Morning sector card (injected)** — PM:XLV −0.53%, Channel 1 through 2026-09-30, S0–S4 prints, 09-18 up-cap, engine JSON.

**Not used as XLV drivers:** X posts on Indian hospital pharmacy mark-ups; blocked etfaction/benzinga sector tables (secondary, inconsistent prints).