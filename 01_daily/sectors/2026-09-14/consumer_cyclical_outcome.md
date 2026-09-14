# Sector Outcome — Consumer Cyclical — 2026-09-14

Actuals: {'etf': 'XLY', 'pct': -0.09738014451408095, 'spy_pct': -0.446162221482016, 'rel': 0.34878207696793506, 'open': 112.30999755859375, 'close': 112.8499984741211, 'source': 'yf_download'}

# Sector Post-Session Review — Consumer Cyclical (XLY) — 2026-09-14

## 0. FACTS

**CLAIM:** XLY closed at $112.85, −0.097% on the day, after opening at $112.31.
**URL:** deterministic actuals (yfinance, injected)
**PUBLISHED:** 2026-09-14
**QUOTE:** `OPEN: 112.30999755859375 CLOSE: 112.8499984741211`; `ETF_PCT: -0.09738014451408095`
**SUMMARY:** XLY was essentially flat — a hair below unchanged. The intraday path was **up**: opened $112.31 (below Friday's $112.96 close), traded down to a $112.20 low (MarketWatch real-time quote at 10:29 showed $112.20 −0.67%), then recovered to close $112.85, i.e. **+0.48% off the open**. So the day was a gap-down-and-recover, not a trend-down.

**CLAIM:** SPY closed −0.446%; XLY's relative return was **+0.349%**.
**URL:** deterministic actuals (injected)
**PUBLISHED:** 2026-09-14
**QUOTE:** `SPY_PCT: -0.446162221482016`; `REL_PCT: 0.34878207696793506`
**SUMMARY:** XLY **outperformed** SPY by ~35bp on a red tape. The sector was a relative winner on a risk-off day.

**CLAIM:** The broad tape fell on rising oil and an AI/semis unwind; chipmakers were pummeled.
**URL:** https://news.google.com/rss/articles/CBMinwFBVV95cUxQYVB5TUc4VTc5MGdNMjVWcVYxUUszYjdXU3hrVnZXbVRCRm40Z3JBX3BhTElKaXVFQmU4SW82WWRwLVdxcGFTVVhPclllM25kSzR0MWdqUmU5YjQydl9MZDhLZ1MzQmF0b01UY3MxaWx0VkxOMjJUQzhmbHJPQjQzdXJMWkJ2OGs5R0ZQc2FjeVpEdjZkM2VqMC1aeGhXSm8?oc=5 (Reuters); https://news.google.com/rss/articles/CBMigAFBVV95cUxNM29KV0w0ZUhKUjkzcng4ZlZzeXJMaDc2SUtfbEJubFh5cEVFX0hVeDZDUWtWcjNfWG5jMWJmNy1pcGR0Mzg1Sk5wNHh2MDhkUUstODgyejVmVWJQYjJtNGZOczRrWVhIN05PS2E5OGI2Znp5bm1zalhocXQzR2FncQ?oc=5 (Yahoo Finance)
**PUBLISHED:** 2026-09-14 20:45 GMT / 13:38 GMT
**QUOTE:** "Wall Street ends down, calls for AI slowdown pummel chipmakers"; "S&P 500 falls on rising oil prices, sagging AI stocks"
**SUMMARY:** The day's dominant narrative was **oil up + AI/semis down**. That is an XLK/XLE story, not an XLY story. XLY's mega-cap book (AMZN ~24%, TSLA ~17%, HD ~5.4%) has no semis exposure.

**CLAIM:** XLY's 10-day MA crossed bearishly below its 50-day MA on 2026-09-08.
**URL:** https://tickeron.com/ticker/XLY/
**PUBLISHED:** 2026-09-14 (page state)
**QUOTE:** "The 10-day moving average for XLY crossed bearishly below the 50-day moving average on September 08, 2026."
**SUMMARY:** Confirms the morning read that XLY was in a multi-horizon downtrend and oversold into the session.

**Facts summary:** XLY −0.10% / SPY −0.45% / **rel +0.35%**. Direction **flat-to-marginally-down**; magnitude **flat**. The sector *beat* the index on a day the index fell.

---

## 1. What actually drove the sector

The morning thesis was "live oil shock + hawkish rates → discretionary demand destruction → XLY down notable." The tape delivered the opposite sign on the relative line. Three things explain it:

**(a) The oil shock was real but XLY is not the clean short.** WTI >$102 / Brent >$107 did hit the tape — but the *transmission* to XLY's actual holdings is weak and slow. XLY is ~46% AMZN+TSLA+HD. Gasoline at the pump is a real-income tax on the marginal consumer, but it does not reprice AMZN's AWS/retail mix or TSLA's order book within one session. The sector that *should* have been punished by an oil shock is the one with the highest energy-cost beta and lowest-income consumer exposure — and on this day the pain was concentrated in **semis/AI (XLK)**, which is where the "AI slowdown" headline landed. XLY was the *beneficiary* of rotation out of the AI complex, not the victim of the oil complex.

**(b) The AI/semis unwind was the day's actual risk-off object, and it is exogenous to XLY.** Reuters and Yahoo both lead with chipmakers. The morning note itself flagged this ("NQ −1.59% is the weak leg on a non-XLY AI/semis impulse," Kospi −3.26%, XLK −1.95%, APH −6.5%). The morning correctly *identified* the object — and then failed to draw the correct conclusion from it: if the risk-off is an AI/semis unwind, XLY is a **relative safe haven within cyclicals**, not a co-victim. The morning treated NQ weakness as ambient risk-off pressure on XLY; the tape treated it as a rotation *into* XLY.

**(c) The gap-down-and-recover path is the tell.** XLY opened −0.65% (premarket), printed a −0.67% low at 10:29, then closed −0.10%. That is a **+0.48% intraday recovery** while SPY closed −0.45%. Buyers stepped in at the open and never gave it back. This is the signature of a sector being *bought* on a macro scare, not sold.

**Taxonomy alignment:** the dominant factor was **sector rotation / relative-value flow** (out of AI/semis into non-AI cyclicals), with the oil shock as a **background macro headwind that did not transmit**. The morning's "gasoline spike crushing discretionary" spine hit was directionally wrong for the session — it was a *level* fact (pump prices are high) misread as a *flow* fact (consumers are cutting spend today).

---

## 2. Audit of morning S0–S4 reads against reality

The morning **prose** concluded **down/mild, total −2.7, confidence 0.55, divergence_flagged True**. The **pipeline** overrode this to **down/notable, total −13.101, confidence 0.85, divergence_flagged False**. Both were wrong on direction; the prose was wrong by less.

**S0_SHARED_MACRO = −2 (prose) / −2.0 × 1.25 skill = −2.5 effective (pipeline).**
*Verdict: directionally defensible, magnitude overstated.* The oil shock and rising real yields were real. But S0 was scored as if XLY were a direct oil-shock victim. The correct S0 for a **mega-cap-concentrated, AI-adjacent-free** discretionary basket on an oil-shock day is closer to **−0.5 to −1**: a headwind, not a dominant negative. The morning's own 08-11 lesson ("make S0 more negative for Consumer Cyclical") was applied mechanically without re-checking whether *this* Consumer Cyclical basket has the gasoline-transmission exposure the lesson assumes. It largely doesn't.

**S1_SECTOR_FACTORS = −1 (prose) / −1.0 × 1.0 = −1.0 (pipeline).**
*Verdict: this is the double-count the morning claimed to have avoided but didn't.* The morning explicitly said S1 would carry "the transmission channel of the S0 oil object, counted once." But the gasoline spike **is** the S0 oil object — scoring it in S1 as a separate −1 is exactly the 09-10 double-count lesson firing, just relabeled as "transmission." The stale soft-consumer cluster (retail miss, confidence, credit) was correctly held out of the stack, but the live gasoline hit was counted twice (S0 macro + S1 spine). **S1 should have been 0**, with the gasoline transmission folded into S0.

**S2_BREADTH = 0.**
*Verdict: correct, and under-credited.* The morning set S2=0 on "no fresh same-day breadth data." But the premarket rotation tell was right there: **XLP +0.61% vs XLY −0.65%** was read as "rotation out of discretionary." The actual session showed XLY *outperforming* SPY — i.e., the rotation was out of **AI/semis**, not out of discretionary. S2=0 was the right number for the wrong reason; a +0.5 would have been defensible.

**S3_FLOWS_POSITIONING = 0 (prose) / × 0.5 skill (pipeline).**
*Verdict: correct.* No fresh flow print; trailing outflows correctly not treated as a 1-day lid.

**S4_ETF_TAPE = 0 (prose) / × 1.25 skill = 0 (pipeline).**
*Verdict: correct, and the most important honest read in the whole note.* The morning correctly refused to restack the oil object into S4 and correctly noted the 1d rel print was **flat (+0.04%)**. The flat tape was the signal. The morning then overrode its own tape read with the factor stack.

**The pipeline's fatal move:** it took a prose note that self-audited to **−2.7 / mild / divergence True** and produced **−13.101 / notable / divergence False**. The `leading_sum: -7.0` and `index_carry: -2.751` and `overlay_score: -6.0` stacked the same macro object three more times on top of S0/S1. The `divergence_flagged: False` is the tell — the pipeline *suppressed* the divergence the prose had correctly identified (factors down, tape flat). That suppression is what turned a mild miss into a notable miss.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count inventory:**
1. Oil object in S0 (macro) **and** S1 (gasoline transmission) — the morning *claimed* this was a permitted separation; it was a double-count. **−1 spurious.**
2. Pipeline `index_carry −2.751` re-expresses the same risk-off macro already in S0. **Spurious.**
3. Pipeline `overlay_score −6.0` (raw −7.2) re-expresses the same again. **Spurious.**
4. Pipeline `anchor −4.35` (ES −0.66%, NQ −1.59%, PM:XLY −0.65%) — the NQ leg is the AI/semis object, which the morning *itself* said must not be mapped into XLY. The anchor mapped it anyway. **Spurious for XLY.**

Net: the pipeline counted the same macro/AI object roughly **four to five times**, and the prose counted the oil object **twice**. The honest single-count stack was roughly **S0 −1, S1 0, S2 0, S3 0, S4 0 → −1 × 0.9 = −0.9 → down/flat**, which would have been a **direction MISS but magnitude HIT** (flat band).

**Knowable-at-open test:** **YES, partially — and the morning had the pieces.**
- The premarket rotation tell (XLP +0.61% vs XLY −0.65%) was visible and was *misread* as anti-discretionary when it was anti-AI.
- The morning explicitly wrote "NQ weakness is exogenous to XLY's book, so it should not be triple-counted as a consumer-specific negative either" — then let the pipeline triple-count it.
- The 09-10 lesson ("when futures are flat/mixed the cap at mild must BIND"; "oversold ETF → mean-reversion setup, not momentum") was **cited and then not applied** — the pipeline produced notable anyway.
- The 1d rel print was flat (+0.04%). A flat tape + oversold + exogenous risk-off = **do not predict notable down**. This was knowable at open.

The failure was not missing information. It was **the pipeline overriding the prose's own correct self-audit.**

---

## 4. Outliers inside the sector

- **XLY vs XLK spread:** XLY −0.10% vs XLK −1.95% premarket (and semis pummeled all day per Reuters). A ~185bp+ intra-cyclical spread in XLY's favor. This is the single cleanest outlier of the session and it is *the* story: the AI unwind was a **rotation into** non-AI cyclicals, and XLY was the liquid vehicle.
- **XLY vs XLI:** XLI was −1.13% premarket; XLY −0.65%. XLY outperformed the *industrial* cyclical too — consistent with "AI/semis-adjacent capex fear" rather than "broad growth fear."
- **XLY vs XLP:** the morning read XLP +0.61% / XLY −0.65% as staples-over-discretionary rotation. The close (XLY rel +0.35% vs SPY) suggests the premarket spread **compressed or reversed** intraday — the defensive bid faded as the AI unwind was absorbed. Worth flagging as a premarket-vs-close divergence the morning did not anticipate.
- **No single-name outlier identified** in the injected data (no fresh AMZN/TSLA/HD catalyst; AAPL PT cut is not an XLY holding; ADBE is XLK). The move was a **basket/flow move**, not a constituent move — which is itself evidence the driver was rotation, not fundamentals.

---

## 5. Verdict

The morning got the **macro environment** right (oil shock real, rates hawkish, risk-off tape) and the **sector conclusion** wrong. The error was a **transmission error**: it assumed an oil shock transmits to a mega-cap-concentrated discretionary basket the way it transmits to a low-income-consumer basket, and it let a pipeline stack the same macro/AI object four-plus times into a "notable" call that its own prose had capped at "mild." The tape's flat 1d rel print, the oversold condition, and the explicitly-flagged exogenous NQ weakness were all knowable at open and all pointed to **flat, not notable down**. XLY's +0.35% relative outperformance on a −0.45% SPY day is the market telling us the sector was a **rotation destination**, not a victim.

OUTCOME_BEGIN
SECTOR: Consumer Cyclical
ETF: XLY
ETF_PCT: -0.10
SPY_PCT: -0.45
REL_PCT: +0.35
ACTUAL_DIRECTION: flat
ACTUAL_MAGNITUDE: flat
PRIMARY_DRIVER: Rotation out of AI/semis (XLK −1.95%, chipmakers pummeled) into non-AI cyclicals; oil shock was a background headwind that did not transmit to XLY's mega-cap book.
KEY_INTERACTION: Pipeline stacked the same macro/AI object 4–5× (S0+S1 oil double-count, index_carry, overlay, NQ anchor leg) and suppressed the prose's own divergence flag, converting a mild miss into a notable miss.
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Direction MISS (predicted down, actual flat) and magnitude MISS (predicted notable, actual flat); prose self-audit (−2.7/mild/divergence True) was closer than the pipeline (−13.101/notable/divergence False), and the pipeline's override was the decisive error.
OUTCOME_END