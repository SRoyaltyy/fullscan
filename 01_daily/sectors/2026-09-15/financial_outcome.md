# Sector Outcome — Financial — 2026-09-15

Actuals: {'etf': 'XLF', 'pct': -0.3156238979986181, 'spy_pct': -0.45867813741702346, 'rel': 0.14305423941840534, 'open': 56.970001220703125, 'close': 56.849998474121094, 'source': 'yf_download'}

# Sector Post-Session Review — Financial (XLF) — 2026-09-15

## 0. FACTS

**Channel 1 / deterministic actuals (trusted, not re-derived):**

- **XLF: −0.3156%** (open 56.97 → close 56.85)
- **SPY: −0.4587%**
- **Relative: +0.1431%** (XLF outperformed SPY by ~14 bps)
- **Actual direction: down. Actual magnitude: mild** (a third of a percent, inside the mild band; not flat, not notable)

**Path:** open 56.97, close 56.85 — XLF opened near the premarket reference (−0.08% premarket) and drifted modestly lower through the session, closing near the low end of a narrow range. No gap-and-fade, no intraday reversal of consequence. A quiet, low-amplitude down day.

**Cross-check against the morning tape:** the morning Channel 1 print showed XLF 1d −0.41% / rel +0.10%. The realized session is XLF −0.32% / rel +0.14%. The relative outperformance *widened slightly* versus the pre-session print — the sector's own tape was, if anything, marginally more resilient than the morning read implied, not less.

**Constituent context (from search, published 2026-09-14/15):**

- CLAIM: BAC CEO Brian Moynihan guided Q3 investment banking fees to $1.6–1.8bn, down YoY, and Q3 trading revenue "relatively flat" vs $5.4bn in Q3 2025.
  URL: https://www.gurufocus.com/news/9080428/bank-of-america-bac-forecasts-flat-q3-trading-revenue-shares-tumble
  PUBLISHED: 2026-09-14
  QUOTE: "projected that the bank's trading revenue for the third quarter will remain 'relatively flat' compared to last year… investment banking fees to be between $1.6 billion and $1.8 billion, falling…"
  SUMMARY: The BAC negative was a **capital-markets revenue** guidance cut delivered at the Barclays conference on **09-14**, i.e. the prior session — not a fresh 09-15 event.

- CLAIM: The BAC warning was read as potentially sector-wide, with Jefferies data showing IB activity already weakening.
  URL: https://www.tipranks.com/news/bank-of-america-stock-bac-drops-on-weak-q3-outlook-is-a-broader-slowdown-coming
  PUBLISHED: 2026-09-15
  QUOTE: "BofA's warning may not be an isolated case. Jefferies… data indicates that investment banking activity at major banks was already weakening before BAC lowered its Q3 outlook."
  SUMMARY: The BAC item had a plausible read-through to the capital-markets sub-group (MS, GS), which the morning MAP HEAT already flagged as dir=down.

---

## 1. What actually drove the sector today

The honest answer is: **very little that was sector-specific, and mostly the shared macro tape.** XLF fell 0.32% on a day SPY fell 0.46% — the sector moved *with* the market, slightly less than it. The realized relative (+0.14%) is a mild outperformance, consistent with a defensive-ish, lower-beta, value-tilted sector in a broad risk-off tape rather than with any financial-specific catalyst firing.

Decomposing the drivers by taxonomy:

**(a) Shared macro / risk-off — the dominant driver.** The morning read had ES −0.54%, NQ −0.62%, RTY −0.73%, DJIA −0.71%, oil re-spiking (WTI +2.37%, Brent +2.31%), long-end selling off hard (30Y futures −0.93%), real yields rising (DFII10 +0.05 1d). SPY realized −0.46%, essentially confirming the red-futures setup. XLF's −0.32% is a *partial* beta capture of that — the sector did not decouple upward, but it also did not lead downward. This is the classic "risk-off tape, financials participate but don't amplify" shape.

**(b) Curve & credit — headwind, not tailwind.** The long-end selloff (30Y 5.35 / 10Y 4.96, bear-long-end steepener) is, per the standing 08-17 lesson, a **headwind, not NIM+**. HY OAS at 2.71, +0.06 1d — tight but creeping wider — is a mild negative that did not blow out. Neither channel produced a same-session shock; both were background drags consistent with a mild down day.

**(c) Constituent / capital-markets news — a *carried* negative, not a fresh one.** The BAC guidance cut was delivered **09-14** (per the search results), and the morning note itself describes it as "a fresh, sector-specific negative" — but the search evidence dates the Barclays appearance to Monday 09-14. So by the 09-15 open, the BAC item was a **one-session-stale** negative that had already been partly priced (BAC −5% on 09-14). This matters for the audit below: the morning note treated a *prior-session* constituent event as if it were a same-morning confirmation of S1. That is a knowability/timing issue.

**(d) Breadth — soft-to-mixed, as flagged.** MAP HEAT showed money-center flat (JPM:neg, BAC:none), capital markets down (MS:neg, GS:neg), credit services down (V:none, MA:mixed), asset management down (BX:neg), with only exchanges (SPGI:pos, CME:pos) and thin regionals (breadth 0.074) positive. That mixed-soft breadth is consistent with a mild down day where the ETF's own relative tape stays flat-to-slightly-positive because the drag is concentrated in a few sub-groups while the index-weight giants (JPM, and the payments complex) hold roughly flat.

**Primary driver, one line:** Broad risk-off tape (SPY −0.46%) with a long-end selloff and oil re-spike; XLF participated at slightly less than market beta, with the carried BAC capital-markets guidance cut as a mild sub-group drag rather than a fresh sector shock.

---

## 2. Audit of morning S0–S4 reads against reality

I use the **morning numbers as written**, not post-close rewrites.

**S0_SHARED_MACRO = −1.5 → VERDICT: directionally correct, magnitude slightly rich.**
The macro setup (red futures, oil re-spike, long-end selloff, real yields up) was real and SPY did close −0.46%. A negative S0 was earned. But −1.5 is a heavy weight for a day that produced a −0.32% sector move and a −0.46% index move — a *mild* tape, not a stress tape (VIX 17.49, contango 0.897, HY 2.71 tight). The score was defensible in sign, somewhat aggressive in size. **Partial hit.**

**S1_SECTOR_FACTORS = −0.5 → VERDICT: sign correct, but the *basis* was partly stale.**
The morning note explicitly earned this negative on "the BAC/BNY constituent news + MAP HEAT soft breadth," citing the 09-10 lesson that S1 needs sector-specific confirmation, not macro narrative. That was the right *discipline*. The problem: the BAC item was a **09-14 event** (search-confirmed), so it was not a same-morning confirmation — it was a carried negative. The MAP HEAT soft breadth (cap-mkts/credit/asset-mgmt down) was genuine and same-day. So S1 = −0.5 is *roughly* right in size, but the note over-credited the freshness of the BAC catalyst. **Partial hit — right answer, slightly wrong reasoning.**

**S2_BREADTH = −0.5 → VERDICT: correct.**
Mixed-soft subsector breadth (money-center flat, three sub-groups down, only exchanges/thin-regionals up) is exactly the configuration that produces a mild down day with flat relative. The note correctly refused to copy a completed lag (08-28 compliance) and scored breadth on its own merits. **Hit.**

**S3_FLOWS_POSITIONING = 0 → VERDICT: correct.**
Trailing outflows are not a 1-day lid (08-28). No fresh inflow/outflow spike. Zero was right. **Hit.**

**S4_ETF_TAPE = 0 → VERDICT: correct, and this is the key call.**
The morning tape showed 1d rel +0.10% (flat/sub-gate), 3d rel −0.02%, 1w rel +0.29%, 1m rel +0.15%. The note held S4 at 0 and flagged divergence. Realized rel was **+0.14%** — still flat/sub-gate, still mildly positive on the longer horizons. S4 = 0 was the correct read: the ETF's own tape never confirmed a sector-specific breakdown. **Hit — and the most important one.**

**Divergence flag → VERDICT: correctly flagged, correctly resolved.**
Leading sum (S0+S1+S2 = −2.5) vs S4 = 0 → divergence flagged. The 09-14 standing rule says resolve toward the live macro overlay but **cap magnitude at flat/mild**. The note did exactly that: down/mild. Realized: down/mild. **The resolution rule worked.**

**Multiplier 0.9 / confidence 0.55 → VERDICT: reasonable.** A 0.9 multiplier on a divergence-flagged, mixed-signal day is appropriately humble.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count check.** The note claims oil/yields were counted **once** in S0, and that S1 was earned by constituent news + breadth, not by re-scoring the macro channel. On inspection this is *mostly* clean, but there is a soft double-count: the "long-end selloff / real yields rising" appears in S0 (−1.5) **and** is invoked again in the S1 justification ("MAP HEAT soft breadth" is partly a rates-channel story for cap-mkts/asset-mgmt). The overlap is small but nonzero. Net effect: S0+S1 together may be ~0.25–0.5 too negative. This is consistent with the observed outcome (mild down, not the deeper move a −2.5 leading sum might imply).

**Knowable-at-open test.** What was knowable at the 09-15 open?
- Red futures, oil re-spike, long-end selloff, real yields up — **yes, knowable.**
- XLF premarket −0.08%, 1d rel +0.10% — **yes, knowable.**
- MAP HEAT mixed-soft breadth — **yes, knowable.**
- BAC guidance cut — **knowable, but as a 09-14 event, already partly priced.** The note treated it as fresher than it was.

So the *entire* negative case was knowable at open. The question was never information — it was **weighting**. And the weighting error was: treating a stale constituent negative as a fresh confirmation, which nudged S1 to −0.5 when a smaller value (or a note explicitly labeling BAC as carried) would have been more honest.

**The decisive interaction:** flat ETF relative tape (+0.10%) vs negative macro overlay. The 09-14 rule says the flat tape is a **downside cap**, not an up-license, and to resolve toward the macro overlay but cap at flat/mild. That is precisely what happened: XLF went down (macro overlay won) but only mildly and it *outperformed* SPY (the flat tape acted as the cap). **The rule was validated a second consecutive session.**

---

## 4. Outliers inside the sector

- **BAC (−5% on 09-14)** — the single largest idiosyncratic move, but it was a *prior-session* event. On 09-15 itself, BAC was flagged "none" in MAP HEAT, i.e. no fresh directional signal. The outlier was already in the tape before the open.
- **Capital markets (MS, GS)** — flagged dir=down; the BAC read-through (IB fees falling, trading flat) plausibly pressured this sub-group. This is the most likely source of the sector's mild underperformance *within* the day, though it was not large enough to drag the ETF below market beta.
- **Exchanges (SPGI, CME)** — the cleanest positive inside Financial, and a likely reason XLF's relative tape stayed positive: index-weight exchange names with rate/volatility leverage can offset money-center softness.
- **Regionals** — dir=up but breadth 0.074 (thin). Not a real offset; correctly discounted.
- **No blowout anywhere.** No credit-spread blowout (HY 2.71), no deposit-flight headline, no charge-off spike, no CRE shock. The absence of a tail event is itself the story of why the day was mild.

---

## 5. Scorecard

| Read | Morning | Realized | Verdict |
|---|---|---|---|
| Direction | down | down (−0.32%) | **HIT** |
| Magnitude | mild | mild | **HIT** |
| Relative | (flat tape, divergence flagged) | +0.14% (flat/mildly positive) | **HIT** |
| S0 | −1.5 | negative tape confirmed, size rich | partial |
| S1 | −0.5 | sign right, basis partly stale (BAC = 09-14) | partial |
| S2 | −0.5 | mixed-soft breadth confirmed | HIT |
| S3 | 0 | no flow signal | HIT |
| S4 | 0 | rel stayed flat/sub-gate | HIT |
| Divergence resolution | down/mild | down/mild | **HIT** |

**Binding lesson for the log:** the 09-14 standing rule (flat sub-gate relative tape = downside cap; resolve leading-vs-tape divergence toward the macro overlay but cap at flat/mild) **worked again**. The one process defect to fix: **do not label a prior-session constituent event (BAC, 09-14) as a "fresh, same-session" S1 confirmation.** The 09-10 lesson requires *same-morning* sector-specific confirmation; a carried negative should be scored smaller or explicitly tagged as carried. That defect likely contributed ~0.25–0.5 of excess negativity to S0+S1, which is why the leading sum (−2.5) over-predicted relative to the realized mild move.

---

OUTCOME_BEGIN
SECTOR: Financial
ETF: XLF
ETF_PCT: -0.3156
SPY_PCT: -0.4587
REL_PCT: +0.1431
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Broad risk-off tape (SPY −0.46%) with long-end selloff and oil re-spike; XLF participated at slightly sub-market beta, with the carried BAC capital-markets guidance cut (09-14) as a mild sub-group drag rather than a fresh sector shock.
KEY_INTERACTION: Flat/sub-gate ETF relative tape (+0.10% premarket → +0.14% realized) acted as a downside cap against a negative macro overlay — the 09-14 standing rule resolved correctly toward the overlay but capped magnitude at mild.
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Direction and magnitude HIT; S2/S3/S4 and divergence resolution correct; S0 slightly rich and S1 basis partly stale (BAC was a 09-14 event mislabeled as fresh), producing a mild over-prediction in the leading sum.
OUTCOME_END