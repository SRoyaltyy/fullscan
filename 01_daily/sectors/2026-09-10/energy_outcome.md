# Sector Outcome — Energy — 2026-09-10

Actuals: {'etf': 'XLE', 'pct': -0.5818362695191537, 'spy_pct': -0.5994238166152965, 'rel': 0.017587547096142853, 'open': 66.13999938964844, 'close': 64.93000030517578, 'source': 'yf_download'}

# Sector Post-Session Review — Energy (XLE) — 2026-09-10

## 0. FACTS

**Tape (deterministic actuals):**
- XLE: **−0.58%** (open 66.14 → close 64.93)
- SPY: **−0.60%**
- Relative: **+0.02%** (essentially flat vs SPY)
- Actual direction: **down**; actual magnitude: **mild** (sub-1%, no gap-and-trend)

**Path:** XLE opened **up** at 66.14 (vs prior close 65.31, ~+1.3% gap) and sold off all day to close 64.93 — a **full round-trip fade**, closing near the low. This is the single most important fact of the session: the morning's "oil is bid, tape is confirming" thesis was **right at the open and wrong at the close**.

**Cross-check on the crude spine:**
- CLAIM: WTI settled ~$97.26 on 2026-09-09 (prior session reference).
  URL: https://fred.stlouisfed.org/series/DCOILWTICO
  PUBLISHED: 2026-09-10
  QUOTE: "2026-09-09: 97.26 | Dollars per Barrel"
  SUMMARY: The morning's WTI $97.44 reference was the prior-session level; the barrel was NOT materially higher intraday.

- CLAIM: Intraday crude prints on 09-10 were mixed/conflicting across sources (one showing Brent ~$105.20 at 8am ET, another showing crude "up 6.90% to $102.68," another showing a Brent pullback to $77.69).
  URL: https://fortune.com/article/price-of-oil-09-10-2026/ ; https://tradingeconomics.com/commodity/crude-oil
  PUBLISHED: 2026-09-10
  QUOTE: "By 8 a.m. Eastern Time today, oil had reached $105.20 per barrel, measured using the Brent benchmark."
  SUMMARY: The crude tape was **not a clean continuation higher** — the morning's "still bid" read did not extend into a decisive up-day for the barrel, and XLE's fade is consistent with crude giving back the overnight premium.

**Key structural fact:** XLE gapped up ~+1.3% on the open, then bled ~1.9% from open to close. The **gap was the entire move** — and it was given back. This is a classic "buy the rumor / sell the continuation" day-3 pattern.

---

## 1. What drove the sector today

**Primary driver: the day-3 geopolitical/oil premium was sold, not extended.** The morning thesis was "day-3 continuation of the US-Iran kinetic escalation, crude still bid." Reality: the market treated day-3 as **exhaustion, not continuation**. XLE opened at the high (66.14) and closed at the low (64.93) — the exact signature of a premium being distributed into strength rather than accumulated.

Taxonomy-aligned factors:
- **Crude oil price surge (WTI/Brent):** the morning HIT grid scored this 0.85. The barrel did **not** deliver a decisive up-day; the overnight premium faded. This is the load-bearing miss.
- **Geopolitical supply risk premium:** live, but **day-3 = priced**. The morning itself flagged "no fresh step-change headline beyond the ongoing escalation." That caveat was the tell — and it was under-weighted.
- **Risk-off tape / flight to safety:** SPY −0.60%, and XLE could not decouple. The morning's "sector_shock, SPY red / XLE green on its own object" regime **failed** — XLE tracked SPY down almost tick-for-tick (rel +0.02%).
- **Real yields / duration:** 10Y 4.80 sticky; not an XLE spine, correctly de-weighted.

**The honest one-liner:** the sector's own shock (oil/geo) stopped producing incremental upside on day 3, and with SPY red, XLE had nothing to hold it up — so it round-tripped its gap.

---

## 2. Audit of morning S0–S4 reads against reality

**S0_SHARED_MACRO = 0 (muted).** *Verdict: correct in level, wrong in implication.* The morning said risk-off equities were "a cyclical overlay, not a veto when oil is the sector's own shock." Reality: with the oil shock no longer producing incremental upside, the risk-off overlay **became** the dominant force. S0=0 was defensible as a score, but the *reasoning* ("not a veto") was the error — on day-3 of a priced shock, shared macro is not an overlay, it's the spine. **Read: MISS on implication.**

**S1_SECTOR_FACTORS = +2.** *Verdict: MISS.* The morning netted the oil/Hormuz cluster at +2, explicitly refusing +3 because "day-3 continuation, no fresh step-change." That instinct was right — but +2 was still too high. A day-3 continuation with **no fresh increment** and a **sub-1.5% crude print** should have been **+1 or 0**, not +2. The morning even wrote "this is a modest continuation, not 09-08's +3.18% surge" — and then scored it +2 anyway. **The score contradicted its own prose.** This is the core error.

**S2_BREADTH = +1.** *Verdict: MISS.* The morning read 1d rel +1.30% as "the oil bid is transmitting" and large-caps participating. Reality: the 1d rel was **stale** (prior session's close), and on the day XLE's rel was +0.02% — zero transmission. The morning used **trailing** breadth to justify a **forward** breadth score. **Read: MISS — trailing tape misused as live confirmation.**

**S3_FLOWS_POSITIONING = 0.** *Verdict: correct, and under-credited.* The morning noted 1m rel +9.90% "leftover leadership" but declined to fire the crowded-long unwind trigger because 1w rel was only +0.75%. Reality: the +9.90% 1m run **was** the vulnerability — day-3 of a shock after a +9.9% month is exactly when late longs distribute. The morning had the right data and the wrong conclusion. **Read: score correct, reasoning incomplete.**

**S4_ETF_TAPE = +1.** *Verdict: MISS.* The morning scored Channel 1 confirmation (+1.30% 1d rel) as a live up-signal. But Channel 1 was **through 2026-09-09** — it was the *prior* session's tape, not a forward signal. Scoring stale tape as +1 confirmation is a **knowability error**: at 9:30 ET on 09-10, that +1.30% was already in the price (it was the gap). **Read: MISS — stale tape double-counted as forward confirmation.**

**Net audit:** S1 and S4 were the two errors that mattered. Both involved treating **already-printed** information (prior-session crude, prior-session rel tape) as **forward** signal. The morning's own caveats ("day-3," "no fresh step-change," "modest continuation") were correct and were then overridden by the scoring.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count check:** The morning explicitly claimed it counted oil + Hormuz **once** in S1. That's true *within* S1. But the real double-count was **across channels**: the +1.30% 1d rel (S4) and the "oil bid transmitting" (S2) are the **same fact** — the prior session's oil-driven XLE outperformance. The morning scored it in S2 (+1) *and* S4 (+1), i.e., **+2 for one stale observation**. That inflated the leading sum to 8.0 and pushed the pipeline to **notable** (8.5) even though the narrative text said "cap at mild."

**Knowable-at-open test:** At 9:30 ET on 09-10, what was knowable?
- Prior-session crude ($97.26 WTI) — **known, already priced**.
- Prior-session XLE rel (+1.30%) — **known, already priced** (it was the gap).
- Day-3 status of the shock — **known**.
- No fresh kinetic headline — **known**.
- SPY risk-off, VIX backwardation — **known**.

Everything the morning cited as bullish was **knowable and already in the open**. There was **no forward catalyst** in the bullish column. The only genuinely forward item was the **10:30 ET EIA WPSR** — which the morning correctly flagged as "two-sided, not a scored HIT." So the honest knowable-at-open read was: **a gapped-up sector with no fresh catalyst, in a risk-off tape, after a +9.9% month.** That is a **fade setup**, not a continuation setup. **KNOWABLE_AT_OPEN: yes — the fade was inferable.**

**The pipeline vs. narrative divergence:** The narrative text said **mild**; the pipeline JSON emitted **notable** (total_score 8.5). The morning's own magnitude-discipline lesson (09-08/09-09, "cap at mild") was **overridden by the pipeline's arithmetic**. This is a process failure: the discipline rule was written, then not enforced at the scoring layer.

---

## 4. Outliers inside the sector

- **XLE itself is the outlier:** a +1.3% gap fully round-tripped to −0.58% while SPY was −0.60%. The **rel +0.02%** is the tell — XLE did not lead, did not lag, it simply **tracked SPY down** after giving back its oil premium. The "sector_shock decoupling" regime did not hold.
- **Refiner sleeve:** the morning flagged HO −0.75% / RBOB +1.14% as "products mixed, not a clean refiner squeeze" and correctly dampened VLO/MPC. That call held — no refiner outlier drove the ETF.
- **Nat gas $2.793 (−0.99%):** correctly N/A; no outlier.
- **Metals split (gold −0.56%, silver −2.43%, copper −2.89%):** the morning correctly refused to import a commodity-bid cushion. That refusal was right — and it should have been a **warning**, not a neutral: a broad commodity complex that is *not* bid is inconsistent with an "oil still bid" continuation thesis.

---

## 5. Verdict and lessons

**The morning got the direction wrong (predicted up/notable, actual down/mild) and the magnitude band wrong (notable vs mild).** The narrative text was *closer to right* than the pipeline output — it said mild, it flagged day-3 exhaustion, it flagged no fresh catalyst. The scoring layer then overrode all of that.

**Three concrete lessons:**
1. **Stale tape is not confirmation.** Channel 1 through the prior close is *already in the open*. Scoring it as S4 +1 (and again in S2) double-counts one stale fact and inflates the leading sum. On a gap-up open, prior-session rel should score **0**, not +1.
2. **Day-3 of a shock with no fresh increment is a fade candidate, not a continuation.** The morning wrote the correct caveat and then scored +2 on S1 anyway. When the prose says "modest continuation, no fresh step-change," S1 should be **+1 max**.
3. **Enforce the magnitude-discipline rule at the pipeline layer.** The narrative said mild; the pipeline emitted notable. A written discipline rule that the arithmetic can silently override is not a discipline rule.

**What would have caught it:** the knowable-at-open test. A gapped-up sector, no forward catalyst, risk-off tape, +9.9% 1m run, and a commodity complex that is *not* bid → the base case was a **gap fade**, i.e., **down/mild**. That was inferable before the bell.

OUTCOME_BEGIN
SECTOR: Energy
ETF: XLE
ETF_PCT: -0.58
SPY_PCT: -0.60
REL_PCT: +0.02
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Day-3 oil/geopolitical premium distributed into a gap-up open; with no fresh catalyst and SPY risk-off, XLE round-tripped its +1.3% gap to close near the low.
KEY_INTERACTION: Prior-session rel tape (+1.30%) was double-counted as live confirmation in both S2 and S4, inflating the leading sum and pushing the pipeline to "notable" against the narrative's own "mild" cap.
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Direction and magnitude both MISS — narrative text correctly flagged day-3 exhaustion and capped at mild, but the scoring layer (S1 +2, S2 +1, S4 +1 on stale tape) overrode it and emitted up/notable.
OUTCOME_END