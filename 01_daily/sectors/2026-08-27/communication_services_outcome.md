# Sector Outcome — Communication Services — 2026-08-27

Actuals: {'etf': 'XLC', 'pct': -1.0656220066940336, 'spy_pct': 0.6552786111251541, 'rel': -1.7209006178191877, 'open': 112.02999877929688, 'close': 111.41000366210938}

Memory search is paused (index metadata missing; `openclaw memory status --index` / `openclaw memory index --force` would rebuild it). Review uses the injected morning note, deterministic actuals, and live sources only.

## 0. Facts

XLC **−1.07%** (open **112.03** → close **111.41**). Implied prior close ≈ **112.61**, so the session **gapped down ~0.5%** then sold another **~0.55%** into the close — not a green-open fade.

SPY **+0.66%**. Relative **−1.72%**. Direction **down**. Absolute move is **notable** (~1.1%); relative vs SPY is clearly notable.

Path: gap-down, grind lower, while the tape that “risk-on” was supposed to ride was **NVDA/Nasdaq concentrated**. AP: S&P **+0.7%** to 7,730.99, Nasdaq **+1.6%**, “Nvidia was the strongest force… and more than offset drops for the majority of the stocks within the S&P 500.” Treasury yields **ticked higher**.

Holdings (approx., 08-27): META **~−0.9% to −1.0%** (~$571); GOOGL **~−0.4%** (~$340.7); NFLX **~−2.0%**; DIS **~−2.1%**; T **~−1.7%**; VZ **~−1%**. Entertainment lagged the two-name spine; telecom did **not** offset.

---

## 1. What drove the sector

Taxonomy, in order:

**S0 mapping failure (primary).** Shared macro was risk-on **only in the NVDA/XLK sleeve**. CNBC: NVDA **~+9%** Thursday, **~$440B** of cap, after Wed AMC beat/guide ($96.2B rev, ~$108B Q3 guide, 70% FY28 growth talk). That bid **is** NQ. It is **not** META/GOOGL duration. Morning Channel 2 said this explicitly — “Risk-on helps XLC **only if mega-cap growth participates**” — then treated **NQ +0.55%** as that participation. NQ was the chip print.

**S1 legal + capex/FCF (co-driver, not a second independent shock).** Wed 08-26 META multi-state kids-addiction settlement: **$16.7B** headline / Meta **~$18B** over 10 years, **~$10B Q3 legal accrual**, teen daily caps + night blocks; YouTube/TikTok **contingent** $5.3B. Needham kept **Hold** on cash + AI-capex collision. Business Insider Thursday: NVDA soars while META/GOOGL are punished for **AI spend vs FCF** (Alphabet capex track **~$205B**; Meta **~$145B**; Meta Q2 FCF **−91%** to **$784M**). Ad/AI “proof” did not reprice XLC; the **buyer** of GPUs was the funding side.

**S2 breadth.** Not a META-only event. NFLX/DIS **~−2%** and T/VZ red = **inside-sector risk-off**, not large-cap leadership.

**S3.** Persistent outflows (morning: 1m ~−$594M, 3m ~−$1.97B) were the demand tell. Price had led units; Thursday mean-reverted that.

**S4.** Prior 1d XLC **−0.50% / rel −0.53%** already said “not a follow-through up day.” The 08-27 continuation was that tape, not the 3d/1w relative lead.

Jackson Hole **started** Thursday; Warsh keynote is **Fri 08-28**. Yields ticking up into that is a duration headwind, secondary to the NVDA/XLC split.

---

## 2. Audit of morning S0–S4 (use morning numbers, do not rewrite them)

| Sleeve | Morning | Reality 08-27 | Verdict |
|---|---|---|---|
| **S0 = +1** | Green ES/NQ, real yields easing, oil off, PCE in hand, 08-21 “don’t keep S0 negative” | SPY/Nasdaq **did** risk-on; XLC **did not**. Yields **up** on the day | S0 sign for *equities* was fair; **XLC mapping was wrong** |
| **S1 = 0** | Ad+AI one thesis; Oakland “week two, not fresh”; no META/GOOGL print; NVDA = XLK only | Settlement **was** the fresh legal event (Wed); $10B accrual + teen MAU rules; GOOGL in the contingent bucket | **Underweighted.** Stale-trial rule misfired on a same-week consent judgment |
| **S2 = 0** | Premarket META ~+0.3% / GOOGL ~−0.2% / NFLX flat; “large-cap leadership, not % names expanding” | Leaders **and** secondaries down; NFLX/DIS worse than META | Too kind. Breadth was **soft**, not mixed-flat |
| **S3 = 0** | Price-up / units-out; not same-day support | Rel **−1.72%** fits weak demand | Directionally right, **not negative enough** |
| **S4 = 0** | 1d rel **−0.53%**; 08-14 no follow-through-up | Followed through **down** | The caution was correct; the **up** call ignored it |

Pipeline called **up / flat** (score 1.8, conf 0.5). Direction **MISS**. Magnitude band **flat** was the only conservative piece and still too tight vs **−1.07% / −1.72% rel**.

Internal contradiction: Channel 2.1 required mega-cap **participation**; Channel 2.4 already had premarket **mixed-to-flat** META/GOOGL/NFLX; S4 was **not** a follow-through-up tape. S0 still won the call.

Note: pipeline JSON `leading_sum: 2.0` vs components summing to **1.0** is a compute inconsistency, not an 08-27 market fact.

---

## 3. Interactions / double-count / knowable-at-open

**Interaction (the day):** NVDA beat **confirmed AI demand** → XLK/SPY up **and** raised the **who pays** question for META/GOOGL capex. Settlement cash + teen limits **stacked on** the FCF hole. Do **not** count “AI boom” and “hyperscaler spend” as two XLC positives; on this tape they were **one trade with opposite ETF signs** (XLK vs XLC).

**Double-count check:** Morning correctly **deduped ad+AI** (08-11) and did **not** re-score the 3d reversal as S2 (08-14). Error was **undercount**, not double-count: NVDA was excluded from the comms spine **and** still used via NQ to justify S0→up.

**Knowable at Thursday open:**
- NVDA AMC 08-26 print/guide: **yes**. Classification as “XLK only, therefore XLC still up on NQ” was the miss.
- META settlement + $10B Q3 accrual + product restrictions: **yes** (Wed filing/approval). Morning still wrote “Oakland week two / not fresh.”
- Warsh Friday: **yes**, flagged, not used as a same-day down bias.
- Intraday NVDA **+9%** oxygen-suck + yield uptick: **partial**.

**KNOWABLE_AT_OPEN: partially**

---

## 4. Outliers inside the sector

- **NFLX ~−2%, DIS ~−2%:** entertainment weaker than the META/GOOGL spine — streaming/media risk-off, not just duration mega-caps.
- **GOOGL ~−0.4% vs META ~−1%:** Alphabet less hit than Meta (settlement is META-primary; YouTube is contingent). Still **not** an XLC bid.
- **T/VZ red:** 08-26 DJ “defensive carriers bid” did **not** repeat Thursday. No telecom ballast.
- Do not treat APP/AMX as drivers (morning was right).

---

### Evidence

CLAIM: XLC 08-27 open 112.03 / close 111.41, −1.066% vs SPY +0.655%, rel −1.721%.  
URL: deterministic Channel 1 actuals (injected).  
PUBLISHED: 2026-08-27 session.  
QUOTE: `ETF_PCT: -1.0656; SPY_PCT: 0.6553; REL_PCT: -1.7209; OPEN: 112.03; CLOSE: 111.41`.  
SUMMARY: Gap-down then further sell; notable absolute and relative underperformance.

CLAIM: NVDA-led tape lifted indexes while most S&P names fell; yields up.  
URL: https://apnews.com/article/wall-street-stocks-dow-nasdaq-b4216a1f191d0304b4ed59e6912e23a4  
PUBLISHED: 2026-08-27.  
QUOTE: “Nvidia was the strongest force lifting the market and more than offset drops for the majority of the stocks within the S&P 500… Treasury yields ticked higher.”  
SUMMARY: Index risk-on was concentration, not comms beta.

CLAIM: NVDA +~9% / ~$440B Thursday after blowout guide.  
URL: https://www.cnbc.com/2026/08/27/nvidia-nvda-q2-earnings.html  
PUBLISHED: 2026-08-27.  
QUOTE: “Nvidia shares rose nearly 9% Thursday… The surge added about $440 billion to the chip giant's market cap.”  
SUMMARY: The NQ mapping was NVDA, not XLC leaders.

CLAIM: META ~$16.7–18B kids-addiction settlement; ~$10B Q3 legal expense; teen limits; YouTube/TikTok contingent.  
URL: https://www.cnbc.com/2026/08/26/meta-social-media-trial-settlement.html  
PUBLISHED: 2026-08-26.  
QUOTE: “Meta agreed to pay $16.7 billion… expects to ‘accrue a legal expense of approximately $10 billion in the third quarter of 2026’… remaining $5.3 billion… tied to YouTube’s payment and… TikTok’s.”  
SUMMARY: Fresh legal/cash/engagement event by Thursday open; not a stale week-two trial.

CLAIM: Settlement does not clear the legal overhang; TD Cowen: some overhang resolved, significant civil liability remains.  
URL: https://www.cnbc.com/2026/08/27/after-meta-landmark-settlement-with-state-ags-legal-headaches-remain.html  
PUBLISHED: 2026-08-27.  
QUOTE: “It is not preemptive in any way… It's a floor conceptually, not a ceiling.”  
SUMMARY: Thursday legal read was still HIT, not relief.

CLAIM: Thursday narrative was NVDA up, META/GOOGL down on AI capex and FCF.  
URL: https://www.businessinsider.com/meta-alphabet-stock-price-decline-record-highs-mag7-nvda-earnings-2026-8  
PUBLISHED: 2026-08-27.  
QUOTE: “Nvidia… Shares were up 10% around midday… Meanwhile, there's Alphabet and Meta… waning free cash flow and worries that both tech firms are spending excessively on AI.”  
SUMMARY: Hyperscaler-as-GPU-buyer was the XLC factor, not “AI proof.”

CLAIM: 08-26 comms roundup was pre-NVDA-session rotation (streaming down, some carriers up) — **not** the 08-27 close.  
URL: https://www.morningstar.com/news/dow-jones/202608269496/communications-services-down-on-retreat-from-risk-communications-services-roundup  
PUBLISHED: 2026-08-26 18:11 ET.  
QUOTE: “Communications-services companies fell as traders rotated out of more speculative sectors ahead of Nvidia's earnings.”  
SUMMARY: Use only as prior-day context; Thursday was the post-print split.

---

OUTCOME_BEGIN
SECTOR: Communication Services
ETF: XLC
ETF_PCT: -1.0656220066940336
SPY_PCT: 0.6552786111251541
REL_PCT: -1.7209006178191877
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: NVDA/XLK AI-infrastructure melt-up lifted SPY while XLC (META/GOOGL buyers + media/telecom) sold.
KEY_INTERACTION: Green NQ was NVDA, not comms duration; META settlement cash/teen limits stacked on capex-vs-FCF.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Direction miss — S0 mapped NQ onto XLC; S1 treated a Wed $16.7–18B META settlement as stale Oakland week-two.
OUTCOME_END

---

## RESEARCH APPENDIX

**Queries run**
- Web: `XLC Communication Services ETF August 27 2026 performance META GOOGL NFLX`
- Web: `why did communication services stocks fall August 27 2026 Meta Alphabet Netflix`
- Web: `stock market today August 27 2026 Nvidia earnings S&P 500 communication services`
- Web: `XLC holdings META GOOGL NFLX DIS T VZ August 27 2026 performance`
- Web: `META stock August 27 2026 close change`
- Web: `GOOGL NFLX DIS T VZ stock performance August 27 2026`
- Web: `"August 27, 2026" communication services OR XLC OR Meta OR Alphabet Nvidia`
- Web: `Meta $16.7 billion settlement California August 26 2026`
- Web: `Jackson Hole Warsh August 28 2026 yields stocks August 27`
- Web: `XLC vs XLK August 27 2026 Nvidia hyperscaler rotation`
- Web: `Needham Meta settlement cash financing AI race August 2026`
- Web: `XLC historical data August 27 2026 open close`
- X: `XLC Communication Services META GOOGL NFLX stock move August 27 2026 why down` (2026-08-27 to 2026-08-28)
- X: `What happened to META Alphabet Netflix XLC on August 27 2026 after Nvidia earnings and Meta settlement` (same window)
- memory_search: XLC 2026-08-27 / lessons — **unavailable** (index metadata missing)

**Key sources (title + URL + timestamp / facts taken)**

1. **Injected actuals** — 2026-08-27 close. XLC −1.066%, SPY +0.655%, rel −1.721%, O/C 112.03/111.41.
2. **AP — How major US stock indexes fared Thursday 8/27/2026** — https://apnews.com/article/wall-street-stocks-dow-nasdaq-b4216a1f191d0304b4ed59e6912e23a4 — fetched ~2026-08-27 21:11Z. S&P +0.7% to 7730.99, Nasdaq +1.6%, NVDA offset majority of S&P decliners, yields up.
3. **CNBC — Nvidia adds more than $400 billion…** — https://www.cnbc.com/2026/08/27/nvidia-nvda-q2-earnings.html — 2026-08-27. NVDA ~+9%, ~$440B; demand/guide calmed AI-capex nerves **for NVDA**.
4. **CNBC — Meta settles… $16.7 billion** — https://www.cnbc.com/2026/08/26/meta-social-media-trial-settlement.html — 2026-08-26. Settlement terms, $10B Q3 accrual, teen limits, YouTube/TikTok contingent $5.3B; META +~1% *Wednesday* afternoon (not Thursday XLC).
5. **CNBC — After Meta's landmark settlement… headaches remain** — https://www.cnbc.com/2026/08/27/after-meta-landmark-settlement-with-state-ags-legal-headaches-remain.html — 2026-08-27. Bonta “floor not ceiling”; remaining MDL/school cases; TD Cowen partial overhang only.
6. **Business Insider — Magnificent no more… Alphabet and Meta** — https://www.businessinsider.com/meta-alphabet-stock-price-decline-record-highs-mag7-nvda-earnings-2026-8 — 2026-08-27 (Thursday). NVDA ~+10% midday; GOOGL ~$336 / META ~$571; capex $205B / $145B; Meta FCF −91% to $784M; Needham cash-timing vs AI race.
7. **Dow Jones via Morningstar — Communications Services Roundup** — https://www.morningstar.com/news/dow-jones/202608269496/communications-services-down-on-retreat-from-risk-communications-services-roundup — **2026-08-26 18:11 ET**. Prior-day pre-NVDA rotation only.
8. **TradeSmith / Investing.com / StockAnalysis (via search)** — META ~$570.6–571 −0.9–1.0%; GOOGL ~$340.7 −0.37%; NFLX ~$79.9 −2%; DIS ~$107.2 −2.1%; T ~$25.4 −1.7%; VZ ~$49.6 −1%. Used as holdings path, not as ETF official close.
9. **X search (2026-08-27/28)** — noisy; some posts mixed Wed/Thu prints. Not used for ETF_PCT. Qualitative only: capex/ROI and settlement cash vs AI spend.

**Not used / failed fetches:** Reuters 401, NYT 403, Yahoo live 403/fail, Benzinga 403, Investing.com XLC history 403. Yahoo-search claim that GICS comms **+0.72%** is **rejected** as conflicting with deterministic XLC −1.07%.