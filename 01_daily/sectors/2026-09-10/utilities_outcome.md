# Sector Outcome — Utilities — 2026-09-10

Actuals: {'etf': 'XLU', 'pct': -0.9781047563519718, 'spy_pct': -0.5994238166152965, 'rel': -0.37868093973667527, 'open': 42.9900016784668, 'close': 42.52000045776367, 'source': 'yf_download'}

# Sector Post-Session Review — Utilities (XLU) — 2026-09-10

## 0. FACTS

**Channel 1 (actuals, deterministic):**

| Metric | Value |
|---|---|
| XLU % | **−0.978%** |
| SPY % | **−0.599%** |
| Relative % | **−0.379%** |
| Open | 42.99 |
| Close | 42.52 |
| Path | Open 42.99 → Close 42.52; XLU opened near flat-to-slightly-up vs prior close and sold off through the session |

**Direction:** down. **Magnitude:** mild (sub-1%, inside a normal daily band for XLU). **Relative:** lagged SPY by ~38 bp — XLU did **not** get the defensive relative bid the morning note allowed for.

**Morning prediction:** down / mild, total_score −4.725, confidence 0.55, regime risk_off, divergence_flagged False.

**Verdict on the headline call:** direction **HIT**, magnitude **HIT** (mild), relative **MISS** (predicted possible relative resilience; actual was relative lag).

---

## 1. What actually drove the sector

**Primary driver: the long end kept grinding higher, and the 30Y auction tailed into a 19-year-high zone — a pure duration/bond-proxy headwind that hit XLU harder than SPY.**

Evidence:

> CLAIM: The 30-year bond auction on Sep 10, 2026 priced at a high yield of 5.308% with bid-to-cover 2.61 and indirect bidders 79.5%.
> URL: https://www.sofrrate.com/treasury-rates
> PUBLISHED: 2026-09-10
> QUOTE: "Latest Treasury auction, 30-year bond, Sep 10, 2026: high yield 5.308%, bid-to-cover 2.61, indirect bidders 79.5%."
> SUMMARY: The live supply event the morning note flagged (10Y auction) resolved in the long end at a high absolute yield — consistent with the "sticky-high, not easing" read and with continued duration pressure on bond proxies.

> CLAIM: The 10-year was 4.83% and the 30-year 5.28% as of September 9, 2026; the 10Y has risen from 4.19% to 4.80% over nine months while the Fed held at 3.50–3.75%.
> URL: https://www.sofrrate.com/treasury-rates / https://averin.com/en/journal/ruslan-averin-us-treasury-yield-curve-september-2026
> PUBLISHED: 2026-09-09 / 2026-09-10
> QUOTE: "The 10-year Treasury yield is 4.83%, the 2-year is 4.43%, and the 30-year is 5.28%, as of September 9, 2026."
> SUMMARY: Confirms the morning panel's live-curve read (10Y ~4.80, 30Y ~5.25) was directionally correct and that the long end was at/near multi-decade stress zones into the session.

**Taxonomy alignment:** the dominant factor is **"Rates rising (bond-proxy selloff)"** — the morning's S1 HIT — and it *did* fire. The morning note's own framing ("one shock, counted once") was right about the mechanism; it was wrong about the *relative* consequence.

**Secondary/contextual:** the oil shock (Brent >$102) remained the inflation-expectations input feeding the long end, but it was **not escalating** on the day — so it acted as a background duration tax, not a fresh kinetic impulse. That is exactly the "static shock = mean-reversion fuel, not a flat override" logic the morning note imported from 09-09, and it cut the right way for direction.

**What did NOT show up:** no fresh XLU-wide regulatory or load-growth catalyst. The Duke Energy Florida rate-lower filing stayed single-name and non-ETF-moving, as the morning note judged (08-28 rule correctly applied).

---

## 2. Audit of morning S0–S4 reads against reality

| Bucket | Morning | Actual outcome | Verdict |
|---|---|---|---|
| **S0 Shared macro** | −1 | Long end sticky-high, 30Y auction at 5.308%, duration headwind dominant | **CORRECT** |
| **S1 Sector factors** | −1 | Rates-rising HIT; rates-falling MISS; risk-on rotation MISS | **CORRECT** |
| **S2 Breadth** | 0 | No breadth expansion; XLU lagged SPY on the day | **CORRECT (neutral was right)** |
| **S3 Flows** | 0 | No confirmed same-day flow signal; no evidence of a flow-driven move | **CORRECT (neutral was right)** |
| **S4 ETF tape** | −0.5 | 1d rel −0.71% carried into a −0.38% rel day; medium-term rel positive but did not protect | **CORRECT direction, under-weighted** |

**The one genuine miss is not in the scores — it is in the qualitative overlay.** The morning note wrote: *"XLU can outperform SPY relatively on a defensive bid while falling in absolute terms."* That is the 08-18 frame, and it **did not hold today**. XLU fell *more* than SPY (−0.98% vs −0.60%). The defensive bid was absent.

Why the 08-18 frame failed here: 08-18's relative beat required a *risk-off* tape with a *rising* long end where the defensive bid was strong enough to offset duration. Today the tape was only **mildly** risk-off (VIX 16.51, backwardated but not >20), so the defensive bid was too weak to offset a long end pressing into a 19-year-high zone via a live 30Y auction. **When the long end is the binding constraint and the risk-off bid is shallow, utilities are a pure duration short, not a defensive long.** The morning note flagged this risk in the S1 "PARTIAL (relative)" line but did not let it move the relative call.

**Score-level audit:** the pipeline total of −4.725 (leading_sum −5.0 × 0.9) produced down/mild — correct. The multiplier of 0.9 was appropriate: it kept the call at mild rather than notable, and the actual −0.98% is squarely mild. No band error.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count check:** the morning note explicitly counted the oil shock **once** (in S1, as the inflation→duration channel) and kept S0 as the pure rates/duration channel. That separation held up — there was no double-count inflating the score. If anything the score was *conservative*: the 30Y auction was a **known same-day supply event** that the note classified as "not a scored binary," which was the right call for scoring but understated its capacity to push the long end.

**Interaction that mattered:** *live long-end supply event × shallow risk-off bid*. Individually each was mild; together they removed the relative cushion. The morning note treated the auction as a background item and the defensive bid as a partial offset. In reality the auction **was** the marginal price-setter for a bond proxy, and the shallow VIX meant there was no offsetting bid. This is the single most important interaction of the session and it was **knowable at open** — the auction was on the calendar, and VIX 16.51 was already printed.

**Knowable-at-open test:** **YES, substantially.** Direction (down) was knowable — rates-rising was a live HIT with a live supply event. Magnitude (mild) was knowable — no escalating shock, no >20 VIX. The *relative lag* was the only piece that required judgment, and the ingredients (shallow VIX + live long-end auction + 10Y at 4.80%) were all available pre-open. The morning note had the right inputs and drew the wrong relative conclusion.

**Single-ticker check:** Duke Energy Florida's rate-lower filing did not drive the ETF — correct to exclude. No IPP (CEG/VST) contamination of the call.

---

## 4. Outliers inside the sector

No single-name outlier is visible in the ETF-level data provided, and the −0.98% move is broad-based rather than idiosyncratic — consistent with a **factor-driven** (duration) selloff rather than a stock-specific event. The absence of an outlier is itself informative: it confirms the driver was the shared macro channel (long end), not a regulatory or earnings surprise inside the sector. If a constituent had blown up, XLU would have shown a fatter tail than −0.98% against SPY −0.60%.

---

## 5. Lessons for the log

1. **The 08-18 "relative beat on risk-off + rising long end" frame has a precondition the note did not state: a *deep* risk-off bid (VIX >20 or a genuine flight-to-quality impulse).** With VIX at 16.5 and backwardated, the defensive bid is too shallow to offset duration. Add a gate: *relative beat requires VIX ≥ ~20 or an explicit FTS impulse; otherwise rising long end = relative lag for XLU.*
2. **A live long-end auction is not a background item for a bond proxy.** When the 10Y/30Y are in multi-decade stress zones and an auction is on the calendar, treat the auction as a **scored S1 input**, not a "supply event, not a binary." Today it was the marginal price-setter.
3. **The 09-09 static-shock rule worked.** Non-escalating oil → no flat override → down was correct. Keep it.
4. **Direction and magnitude scoring were sound; the qualitative relative overlay was the failure point.** The scores said down/mild and delivered; the prose added a relative-resilience claim the scores did not support (S4 was −0.5, i.e., already negative on the tape). **Do not let a medium-term positive relative tape (3d/1w/1m) soften a negative 1d tape into a relative-beat claim.** The 08-13 rule ("S2/S4 confirmation only") was cited but then partially violated in the prose.

---

OUTCOME_BEGIN
SECTOR: Utilities
ETF: XLU
ETF_PCT: -0.978
SPY_PCT: -0.599
REL_PCT: -0.379
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Long end sticky-high into a live 30Y auction (high yield 5.308%) — pure duration/bond-proxy headwind, with no escalating oil impulse to change the sign
KEY_INTERACTION: Live long-end supply event × shallow risk-off bid (VIX 16.51, backwardated) — the auction set the marginal price and the shallow VIX removed the defensive relative cushion, so XLU lagged SPY instead of beating it
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Direction and magnitude correct (down/mild, −4.725); relative-resilience overlay wrong — XLU lagged SPY by 38 bp, invalidating the 08-18 "relative beat" frame under a shallow risk-off tape
OUTCOME_END