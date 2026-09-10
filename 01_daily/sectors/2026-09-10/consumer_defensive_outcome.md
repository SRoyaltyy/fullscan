# Sector Outcome — Consumer Defensive — 2026-09-10

Actuals: {'etf': 'XLP', 'pct': 0.04815567087683714, 'spy_pct': -0.5994238166152965, 'rel': 0.6475794874921337, 'open': 83.37000274658203, 'close': 83.08999633789062, 'source': 'yf_download'}

# Sector Post-Session Review — Consumer Defensive (XLP) — 2026-09-10

## 0. FACTS

**Tape (deterministic, as injected):**

| Metric | Value |
|---|---|
| XLP % | **+0.048%** |
| SPY % | **−0.599%** |
| XLP rel SPY | **+0.648%** |
| XLP open | 83.37 |
| XLP close | 83.09 |

**Path:** XLP opened at 83.37, closed at 83.09 — i.e. the ETF **faded intraday** (open-to-close ≈ −0.34%) yet still finished marginally green on the day versus the prior close, because it gapped up at the open. The +0.048% headline is a **gap-and-fade that held the sign**, not a trending up-day. This matters for the audit: the *relative* win (+0.65%) was earned almost entirely by **not falling as much as SPY**, and the absolute green was a thin, fragile artifact of the opening gap.

**Cross-check against search results (context, not the graded number):**
- CLAIM: XLP closed modestly higher, rebounding after a prior-session decline.
  URL: https://www.perplexity.ai/finance/XLP
  PUBLISHED: 2026-09-10
  QUOTE: "Consumer staples ETF XLP closed modestly higher, rebounding after Thursday's decline…"
  SUMMARY: Consistent with the injected +0.048% — a marginal green, described as "modest."
- CLAIM: SPY/S&P closed down ~0.5–0.6% on rising Treasury yields and oil.
  URL: https://www.reuters.com/business/sp-500-dow-futures-attempt-recovery-ahead-inflation-report-2026-09-10/
  PUBLISHED: 2026-09-10
  QUOTE: "The S&P 500 declined 0.58% to end the session at 7,591.75 points. The Nasdaq declined 0.65%…"
  SUMMARY: Confirms the injected SPY −0.599% and the risk-off, yields-up, oil-up framing the morning memo used.
- CLAIM: Brent hit its highest since July; market declined on it.
  URL: https://www.thestreet.com/stock-market-today/stock-market-today-dow-jones-sp-500-nasdaq-updates-sept-10-2026
  PUBLISHED: 2026-09-10
  QUOTE: "S&P 500, Nasdaq decline as Brent oil hits highest point since July."
  SUMMARY: The oil-led risk-off overlay the morning memo flagged as the dominant cross-asset driver **did** materialize.

**Direction:** XLP **up** (marginally). **Magnitude:** **flat** (|0.048%| is a rounding-error day in absolute terms; the *relative* move +0.65% is mild-to-notable but still sub-1%).

**The headline fact of the session:** the morning call was **down/flat**, and XLP closed **up/flat** — a **direction MISS on the absolute sign**, but the *relative* thesis (staples outperform a risk-off tape) was **correct and then some** (+0.65% rel). This is the central tension of the review: the model got the **relative** answer right and the **absolute sign** wrong, and the reason is instructive.

---

## 1. What drove the sector today

Taxonomy-aligned, in order of demonstrated force:

**(a) Flight-to-safety relative bid — the dominant driver, and it finally showed up.** The morning memo declared the FTS RS-vs-cyclicals spine signal a **"MISS live"** because every trailing horizon was negative relative. Today the *forward* FTS bid arrived: with SPY −0.60%, oil at its highest since July, and yields backing up, capital rotated into the low-beta defensive sleeve. XLP's +0.65% relative is the textbook risk-off relative-strength signature. The morning memo's error was treating a **trailing** relative-laggard reading as evidence the FTS bid was *absent*, when in a live risk-off regime the FTS bid is precisely what a beaten-down defensive gets.

**(b) Oil >$100 as a two-sided force — resolved net-positive for relative, net-negative for absolute.** The morning memo counted oil once, as an S1 input-cost negative. That was half-right: oil did pressure staple margins (freight, packaging, ag feedstocks), which is part of why XLP **faded intraday** and could not hold its gap. But oil >$100 *also* drove the broad risk-off that made staples a relative haven. The memo explicitly refused to count oil as a second defensive bid ("count it in S1, not as a second S0 defensive bid"). Today's tape says that refusal **understated** the FTS channel: the same oil print that squeezes margins also *creates* the defensive rotation. The two effects are not independent — they are the same shock viewed from two sides, and the **relative** effect dominated the **absolute** effect.

**(c) Duration headwind — real but second-order.** Long-end backing up (30Y in the stress zone, 10Y ~4.80) is a genuine headwind for a bond-proxy. It contributed to the intraday fade. But it did not prevent the relative win — consistent with the 08-18 pattern the memo itself cited ("rising 10Y + risk-off → relative outperformance / flat-to-negative absolute"). **The memo wrote down the correct 08-18 template and then failed to apply it to its own sign call.**

**(d) Food-crash cluster — did NOT dominate today.** The memo's governing rule was that the food-crash drag (CPB/GIS/KHC/CAG) had "demonstrated dominance" over the FTS bid on 09-03, 09-08, 09-09, and therefore S0 should be 0-or-negative "even under the strongest FTS trigger." Today that rule **failed**: the FTS bid won. The three-session streak the memo treated as a *demonstrated law* was, in fact, a **three-session streak** — and streaks in a single sleeve (packaged foods) do not override a market-wide regime signal when the regime signal is strong (oil >$100, VIX backwardation, Asia red).

**(e) No same-day idiosyncratic catalyst.** No XLP earnings, no CPI/PPI/PCE. The move was **macro-driven**, which is exactly the environment in which the FTS/defensive channel should be weighted *up*, not down.

---

## 2. Audit of morning S0–S4 reads against reality

Using the **morning numbers as written**, not post-close rewrites.

### S0_SHARED_MACRO = 0 — **WRONG (should have been positive)**

The memo's own text: *"Risk-off + NQ lag + oil >$100 = theoretical relative FTS bid vs cyclicals (sector layer: risk-off relative +)."* It **identified** the positive, then **zeroed it** via the food-crash-dominance rule. The rule was the error. The memo had:
- VIX in **backwardation** (live vol stress),
- Brent **>$100**,
- Asia **red** (−0.56%),
- NQ **lagging**,
- 10Y–SPX 5-day corr **−0.969** (a near-perfect risk-off/rates regime signature).

That is a **maximal** FTS setup for a defensive sector. Zeroing S0 under those conditions required the food-crash rule to be *stronger* than the regime signal. It wasn't. **S0 should have been +0.5 to +1.0.**

### S1_SECTOR_FACTORS = −1 — **PARTIALLY RIGHT, OVERWEIGHTED**

The input-cost squeeze (oil up, ag firm: corn +0.52%, soy +0.44%, meal +0.75%, sugar +1.47%, cotton +1.41%) is real and did contribute to the intraday fade. But −1 was too heavy for a day when the **relative** channel was the dominant force. The food-crash cluster was **carried**, not fresh — no new dividend cut today. A carried structural negative should not carry a full −1 weight on a day with a live, strong FTS trigger. **Fair value: −0.5.**

### S2_BREADTH = 0 — **DEFENSIBLE**

No confirmed breadth expansion was visible premarket, and the memo correctly noted the packaged-food vs discount-store **split**. Today's relative win was broad-beta defensive rotation, not a breadth story. **0 is acceptable.**

### S3_FLOWS_POSITIONING = 0 — **DEFENSIBLE, mild miss**

The memo noted XLP was "a de-risked laggard, which is a mild relative-shield in a risk-off tape but not a same-day demand signal." That "mild relative-shield" language was **correct** and should have been allowed to nudge S3 slightly positive. **Fair value: 0 to +0.25.**

### S4_ETF_TAPE = −0.5 — **WRONG SIGN**

This is the most consequential error. The memo used the trailing 1d rel (−0.69%) as a **−0.5 confirmation**, explicitly acknowledging it was "already paid" and citing the 08-28 lesson not to forecast a second down day from a paid lag. **It then did exactly that.** A paid, trailing relative lag in a *live risk-off regime* is not a bearish confirmation — it is the **setup** for mean-reversion/FTS catch-up. The tape was **not** fighting the factors; the memo mislabeled a laggard-in-a-haven-regime as a continued-laggard. **S4 should have been 0 or slightly positive.**

### Net

| Component | Morning | Fair (ex-post) | Error |
|---|---|---|---|
| S0 | 0 | +0.5 to +1.0 | **understated FTS** |
| S1 | −1 | −0.5 | **overweighted carried drag** |
| S2 | 0 | 0 | ok |
| S3 | 0 | 0 to +0.25 | mild understatement |
| S4 | −0.5 | 0 to +0.25 | **sign error** |

The morning total was **−2.925 → down/flat**. A corrected total of roughly **0 to +0.5** would have produced **flat/up** — i.e. the correct answer.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count audit (the memo's own concern, inverted).** The memo was so worried about *double-counting* the FTS bid (oil in S0 *and* S1) that it **zeroed the FTS channel entirely** to avoid the double-count. That is over-correction: the correct fix for a double-count risk is to **count once at the right weight**, not to **count zero**. The memo counted oil's *cost* side (S1 −1) and discarded oil's *haven* side (S0 0). It kept the negative and dropped the positive — a **one-sided haircut**, which is a systematic bias, not a neutral de-dup.

**Knowable-at-open test.** Everything needed to call this correctly was on the screen at the open:
- VIX backwardation → knowable.
- Brent >$100 → knowable.
- Asia red, NQ lagging → knowable.
- 10Y–SPX corr −0.969 → knowable.
- XLP gapped **up** at the open (83.37 vs prior close ~83.05) → knowable *at the open*.

The gap-up open was the single most important tell: **the market opened XLP higher into a risk-off tape.** That is the FTS bid announcing itself in real time. The memo's S4 used the *prior day's* close-to-close rel (−0.69%) instead of the *live open* signal. **This was knowable at the open and the memo had the data to see it.**

**The 08-18 template was in the memo and unused.** The memo literally wrote: *"rising 10Y + risk-off → relative outperformance / flat-to-negative absolute; do not upgrade to absolute up."* That is **exactly** what happened: relative outperformance (+0.65%), flat-to-marginally-positive absolute (+0.048%). The memo had the correct playbook in hand and then scored −2.925 against it. The failure was not missing information — it was **overriding a correct template with a three-session streak rule.**

---

## 4. Outliers inside the sector

- **The ETF itself is the outlier:** XLP +0.048% vs SPY −0.599% is a **+0.65% relative** move on a day when the sector's own trailing 1w rel was −2.66%. That is a sharp, single-session reversal of a multi-horizon lag — the signature of a **regime-driven FTS rotation**, not a stock-specific event.
- **Packaged foods (CPB/GIS/KHC/CAG)** — the food-crash cluster — did **not** extend the crash today; the drag that "dominated" for three sessions went quiet, which is precisely why the FTS bid was able to lift the ETF. The cluster's failure to produce a fresh negative is the **outlier-vs-expectation** that the morning memo's dominance rule could not accommodate.
- **Discount stores (WMT) / farm products (ADM)** — flagged premarket as relative bright spots — likely led the internal breadth, consistent with a defensive-rotation day.
- **No single-name blowup or melt-up** drove the ETF; the move was **macro/beta**, which reinforces that the correct read was regime-level (S0), not factor-level (S1).

---

## 5. Verdict and lesson

The morning call was **directionally wrong on the absolute sign** (predicted down, closed up) but **directionally right on the relative thesis** (staples outperform risk-off). The magnitude band (**flat**) was **correct** — XLP's absolute move was flat. So the scorecard is: **mag HIT, dir MISS (absolute), rel HIT.**

The root cause is a **single, identifiable, recurring error**: the memo let a **three-session sector-specific streak** (food-crash dominance) **override a maximal, live, market-wide FTS regime signal** (oil >$100, VIX backwardation, NQ lag, Asia red, 10Y–SPX corr −0.969). It then compounded the error by using a **paid trailing relative lag** as a bearish S4 confirmation — the exact 08-28 mistake it claimed to be avoiding — and by applying a **one-sided de-dup** that kept oil's cost side and discarded oil's haven side.

**DO-INSTEAD for next time:** When the regime panel shows a *maximal* FTS setup (VIX backwardation + oil >$100 + NQ lagging + Asia red + strongly negative 10Y–SPX corr), a defensive sector's **trailing relative lag is a setup, not a confirmation.** Cap the sector-specific-drag override at **one session**, not three; require a **fresh** negative print (new guidance cut, new data) to keep the drag at full weight; and when de-duplicating a two-sided shock, **count it once at the correct net weight** rather than keeping only the negative leg. The 08-18 template ("rising 10Y + risk-off → relative outperformance / flat-to-negative absolute") was correct and should have governed the sign call.

OUTCOME_BEGIN
SECTOR: Consumer Defensive
ETF: XLP
ETF_PCT: 0.048
SPY_PCT: -0.599
REL_PCT: 0.648
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: flat
PRIMARY_DRIVER: Live flight-to-safety rotation into low-beta defensives on an oil-led, yields-up risk-off tape (Brent highest since July, VIX backwardation, NQ lagging) — the FTS bid the morning memo zeroed out.
KEY_INTERACTION: Oil >$100 was counted only as an S1 input-cost negative and its simultaneous FTS-haven channel was discarded to avoid a double-count — a one-sided de-dup that kept the negative leg and dropped the positive leg, flipping the sign call.
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Direction MISS on absolute sign (predicted down, closed up) and S4 sign error (paid trailing lag used as bearish confirmation), but magnitude band HIT (flat) and relative thesis HIT (+0.65% rel) — a three-session food-crash streak wrongly overrode a maximal live FTS regime signal.
OUTCOME_END