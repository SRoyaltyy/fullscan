# Sector Outcome — Utilities — 2026-09-11

Actuals: {'etf': 'XLU', 'pct': -0.30574098474991374, 'spy_pct': 0.8524287494320992, 'rel': -1.158169734182013, 'open': 42.88999938964844, 'close': 42.38999938964844, 'source': 'yf_download'}

# Sector Post-Session Review — Utilities (XLU) — 2026-09-11

## 0. FACTS

**Tape (deterministic actuals):**
- XLU: **−0.31%** (open 42.89 → close 42.39)
- SPY: **+0.85%**
- Relative: **−1.16%** (XLU lagged SPY by 116 bp)
- Path: opened at 42.89, closed at 42.39 — a **monotone-ish fade from the open**, no intraday recovery. The high was essentially the open; the close was near the low.

**Context (from search, corroborating the morning panel):**
- CPI (Aug, released 8:30 ET 09-11): headline **+0.4% MoM / +3.4% YoY**, core **+0.3% MoM / +2.4% YoY** — matched expectations. Gasoline +3.9% was over a third of the monthly all-items increase.
- Equities **surged** on the in-line print + falling oil. SPY +0.85% is consistent with a relief rally.
- NYT: "Investors believe the Federal Reserve is very likely to **raise** rates at its meeting next week." Elevated inflation keeps pressure on the Fed to hike.

**Direction:** down (absolute). **Magnitude:** flat (|−0.31%| < 0.5%). **Relative:** severe lag (−1.16%).

---

## 1. What drove the sector today

The taxonomy-aligned driver is clean and singular: **a risk-on relief rally on an in-line CPI, with oil offering hard, rotated capital OUT of the defensive bond-proxy complex and INTO cyclicals/growth.** Utilities were the funding source for the SPY rip.

Three reinforcing channels:

1. **CPI in-line → relief rally → risk-on rotation away from defensives.** The binary resolved benignly for equities (not for duration). SPY +0.85% on a "good enough" print. In a risk-on tape, XLU is a source of funds, not a destination. This is the **"Risk-on rotation away from utilities"** factor — scored PARTIAL in the morning, but it fired at closer to full weight than the morning allowed.

2. **Oil offering hard (WTI −2.78%, Brent −3.37%) → disinflation-at-the-margin → but ALSO a rotation signal.** The morning read treated oil-offering as an *offset* that "forbids forcing down." In reality, oil-offering on a CPI day fed the **risk-on** impulse (lower headline risk → buy cyclicals, sell defensives). The morning double-counted oil as a utilities *cushion* when it was actually a utilities *headwind* via the rotation channel. This is the key interaction error.

3. **Rates: sticky-high long end, no easing impulse.** 10Y ~4.83%, 30Y ~5.28%, bond futures flat-to-marginally-green. The morning correctly noted "not falling." But the operative point is that **a bond proxy with a sticky-high long end and a Fed that is "very likely to raise rates next week" has no duration bid on a risk-on day.** The morning's S1 "Rates rising (carried)" HIT was directionally right but under-weighted — it was scored as carried/stale when the CPI-day risk-on rotation made it live.

**Primary driver:** Risk-on relief rally on in-line CPI + oil-offering → capital rotated out of the defensive bond-proxy complex; XLU was the funding source for the SPY rip.

---

## 2. Audit of morning S0–S4 reads against reality

### S0_SHARED_MACRO = 0 — **MISS (should have been negative)**

The morning wrote: *"S0 = 0 (do not score +1 from green futures/oil-slide; do not score −2 from rates — the CPI binary is unresolved and the oil slide is a genuine offset)."*

**Audit:** The "do not score −2 from rates" caution was correct in spirit (don't over-penalize), but the **net-zero call was wrong in sign**. The morning treated green futures + oil-slide as *offsets* to the sticky-high long end. In reality, on a CPI day, **green futures + oil-offering + in-line CPI = a risk-on impulse that is unambiguously negative for a defensive bond proxy.** The morning failed to recognize that the *same* inputs it called "offsets" were actually *reinforcing* the rotation-away pressure. S0 should have been **−1**, not 0.

The morning's own text contains the tell: *"green futures + oil-offering are offsets, but sticky-high long end + CPI binary + backwardated VIX cap the upside."* It framed everything as "capping upside" (i.e., capping a *positive*), when the correct frame was "**removing the floor**" (i.e., enabling a *negative*). This is a **frame-sign error**, not a data error.

### S1_SECTOR_FACTORS = −1 — **HIT (direction), under-weighted (magnitude)**

The morning scored "Rates rising (carried) + mild risk-on rotation away" as the dominant fresh factors, net −1. Directionally correct. But:
- "Risk-on rotation away" was scored **PARTIAL / mild** with the caveat *"NQ is only marginally leading ES (+0.65 vs +0.63) — not an 08-27-style NQ≥+0.5% anti-FTS rip."* The morning used the **premarket** NQ-ES spread as the rotation gauge. By the close, SPY was +0.85% — a full risk-on session. The premarket spread understated the rotation that actually materialized.
- "Rates rising (carried)" was scored as **carried/stale** ("do not HIT and do not double-count with S0"). But on a CPI day with a Fed hike "very likely next week," the rate pressure was **live**, not carried. The morning's own HIT_GRID scored "Rates rising (bond-proxy selloff)" as **HIT 0.60** — inconsistent with the S1 treatment of the same factor as carried/stale.

**Net:** S1 direction right, but the two dominant factors were both under-weighted. S1 should have been **−1.5 to −2**.

### S2_BREADTH = 0 — **MISS (should have been negative)**

The morning wrote: *"1d rel −0.38% (yesterday's fade), 3d rel +0.30%, 1w rel +0.61%, 1m rel −0.89%. Mixed... No durable breadth expansion today; no live premarket breakdown. S2 = 0."*

**Audit:** The morning leaned on the **3d/1w relative positives** to justify a neutral breadth score. But those positives were **stale** — they reflected the prior week's defensive bid, not today's setup. The **1d rel was already negative (−0.38%)** and the **1m rel was negative (−0.89%)**. The correct read was that the medium-term relative cushion was **decaying**, and on a risk-on CPI day, breadth would **fail** (ETF down, names flat-to-down). S2 should have been **−0.5**.

The morning's own HIT_GRID scored "Sector breadth failure (ETF up, names flat)" as **MISS** — but that's the wrong test. The relevant breadth test on a risk-on day is "**ETF down, names down**" (broad-based defensive liquidation), which is what happened. The morning tested the wrong breadth hypothesis.

### S3_FLOWS_POSITIONING = 0 — **NEUTRAL (acceptable)**

No confirmed same-day flow data at the open. The morning correctly declined to score. **No audit fault** — this is a genuinely unknowable-at-open input. (Post-hoc: the −1.16% relative lag is consistent with **outflows** from XLU into the risk-on trade, but that's not scoreable at the open.)

### S4_ETF_TAPE = −0.5 — **HIT (direction), under-weighted (magnitude)**

The morning scored −0.5 on "1d rel −0.38%, 1m rel −0.89%." Directionally correct. But the tape read **understated the momentum**: the 1d rel was already negative *into* a risk-on setup, and the 1m rel was negative. The correct S4 read was **−1** (the tape was already telling you XLU was the laggard, and the setup reinforced it).

### Divergence check — **FALSE NEGATIVE**

The morning wrote: *"Factors and tape agree in sign (mild negative) — no divergence. But the CPI binary is unresolved and futures are green, so conviction is capped: multiplier 0.9, confidence 0.55."*

**Audit:** The morning **correctly identified the sign agreement** but then **capped conviction** because of the CPI binary and green futures. This was backwards. The CPI binary was **two-sided**, but the *setup* (green futures + oil-offering + sticky-high long end + backwardated VIX) was **one-sided negative** for a defensive bond proxy. The morning treated "unresolved binary" as a reason to **reduce** conviction, when the correct treatment was: *the binary is unresolved, but BOTH branches are negative-to-neutral for XLU* (in-line CPI → risk-on rotation away; hot CPI → rates up, bond proxy down). **The binary was not symmetric for utilities.** This is the single most important audit finding.

---

## 3. Interactions / double-count / knowable-at-open test

### Double-count audit
- **S0 (0) and S1 (−1):** The morning explicitly avoided double-counting rates between S0 and S1 ("do not double-count with S0"). **Correct discipline.** But the cost was that **neither bucket fully captured the live rate pressure** — the factor fell into the gap between "not S0" and "carried in S1." Net: the rate factor was **under-counted**, not double-counted.
- **S1 "risk-on rotation" and S4 "tape":** The morning scored rotation as PARTIAL (S1) and tape as −0.5 (S4). These are **distinct** (forward factor vs. realized tape) — no double-count. But both were under-weighted, so the **sum** understated the pressure.

### Knowable-at-open test
**Was the −1.16% relative lag knowable at the open?** **YES — substantially.**

The morning had, at the open:
1. **Green futures (ES +0.63%, NQ +0.65%)** — a risk-on tilt into the print. ✅ knowable
2. **Oil offering hard (WTI −2.78%, Brent −3.37%)** — disinflation-at-the-margin, risk-on fuel. ✅ knowable
3. **Sticky-high long end (10Y 4.83%, 30Y 5.28%), bond futures flat** — no duration bid. ✅ knowable
4. **VIX 17.24, backwardated (ratio 1.111)** — no flight-to-quality bid. ✅ knowable
5. **1d rel already negative (−0.38%), 1m rel negative (−0.89%)** — tape already lagging. ✅ knowable
6. **CPI binary two-sided** — but **both branches negative-to-neutral for XLU**. ✅ knowable (this is the key inference the morning missed)

**Every input needed to call XLU down/mild-to-notable relative lag was on the desk at the open.** The morning had the right direction (down) but:
- Called magnitude **flat** when the setup supported **mild** (the CPI binary + risk-on rotation + no duration bid = at least mild, arguably notable relative).
- Capped confidence at 0.55 when the setup was **one-sided**.
- Applied a 0.9 multiplier when the divergence check should have **raised** conviction, not lowered it.

**The miss was not informational — it was inferential.** The morning saw all the pieces and assembled them with the wrong sign on the "offsets."

---

## 4. Outliers inside the sector

Without name-level tape, the structural read is:
- **Rate-sensitive regulated utilities** (the bulk of XLU) — the drag. Sticky-high long end + no duration bid + risk-on rotation = broad-based underperformance. This is the −1.16% relative lag.
- **Merchant/IPP names with data-center/power-demand exposure** — the morning flagged "AI-power is a 1d dampener, not a band engine" (08-12 lesson). On a risk-on day, these names likely **outperformed** the regulated cohort (growth-adjacent, less pure bond-proxy), but not enough to lift the ETF. This is the **internal dispersion** that explains why XLU fell only −0.31% absolute while lagging −1.16% relative: the AI-power cohort cushioned the absolute drawdown but couldn't offset the regulated-cohort drag.
- **No single-name regulatory item** drove the ETF (08-28 rule held — correctly not promoted).

**Outlier verdict:** The dispersion was **intra-sector** (AI-power vs. regulated), not a single-name event. The morning's structural framing (AI-power = dampener, not engine) was **correct** and is confirmed by the modest absolute decline vs. the severe relative lag.

---

## 5. Morning read verdict

**Direction: HIT.** The morning called **down**; XLU closed **−0.31%**. ✅

**Magnitude: HIT (by the letter), MISS (by the spirit).** The morning called **flat**; |−0.31%| < 0.5% = flat. ✅ by the band definition. But the morning's *reasoning* for flat was "the CPI binary forbids flat per 09-03/09-04; the green futures + oil-slide forbid notable." In reality, the setup supported **mild** absolute and **severe relative** — the morning's own logic pointed to mild, and it rounded down to flat. The band call was **defensible but under-confident**.

**Relative: SEVERE MISS.** The morning did **not** flag divergence and did **not** write a relative-lag clause with conviction. The −1.16% relative lag is the **headline miss**. The morning's 09-10 gate ("VIX <20 backwardated → rising long end is a relative-lag signal") was **correctly applied** — but the morning then **failed to act on its own gate**, scoring S1 rotation as only PARTIAL and capping confidence at 0.55. **The gate fired; the position sizing didn't follow.**

**The core error:** The morning treated the CPI binary as **symmetric** ("do not pre-score either branch") when it was **asymmetric negative** for a defensive bond proxy. Both branches (in-line → risk-on rotation away; hot → rates up) hurt XLU. Recognizing this asymmetry would have:
- Raised S0 from 0 to −1
- Raised S1 from −1 to −1.5/−2
- Raised S2 from 0 to −0.5
- Raised S4 from −0.5 to −1
- Raised confidence from 0.55 to ~0.70
- Raised the multiplier from 0.9 to ~1.0
- Produced a **down/mild** call with a **relative-lag** clause

**Secondary error:** The morning used **premarket NQ-ES spread** as the rotation gauge and found it "marginal" (+0.65 vs +0.63). By the close, SPY was +0.85% — a full risk-on session. Premarket spread **understated** the rotation. Lesson: on a CPI day, the **premarket** risk-on tilt is a **floor**, not a ceiling — the print can amplify it.

**Tertiary error:** The morning leaned on **stale 3d/1w relative positives** (+0.30%, +0.61%) to justify S2 = 0, while the **live 1d and 1m** were negative. Stale relative cushions should not neutralize live relative deterioration.

---

## 6. Lessons for the log

1. **CPI-day asymmetry for bond proxies:** When CPI is the dominant binary, do **not** treat it as symmetric for rate-sensitive defensives. Both branches (in-line → risk-on rotation away; hot → rates up) are **negative-to-neutral** for XLU. Score the **asymmetry**, not the uncertainty. This is a refinement of 09-03/09-04 ("do not call flat into a high-impact binary") — the refinement is: **for a bond proxy, the binary is one-sided; call down/mild, not flat.**

2. **"Offsets" can be "reinforcements":** Green futures + oil-offering were scored as *offsets* to the sticky-high long end. They were actually *reinforcements* of the risk-on rotation-away pressure. **Test the sign of every "offset" against the sector's factor exposure** — for a defensive, risk-on inputs are headwinds, not cushions.

3. **Premarket rotation gauge is a floor, not a ceiling:** On a CPI day, the premarket NQ-ES spread understates the realized rotation. Do not use "NQ only marginally leading" to cap the rotation factor at PARTIAL.

4. **Stale relative cushions decay:** 3d/1w relative positives should not neutralize live 1d/1m relative negatives in S2. Weight the **live** tape.

5. **The 09-10 gate works — act on it:** The gate ("VIX <20 backwardated → rising long end is a relative-lag signal") correctly identified the setup. The failure was **not following through** with conviction/multiplier. When the gate fires, **raise** confidence, don't cap it.

6. **Frame-sign discipline:** The morning framed the setup as "capping upside" when the correct frame was "**removing the floor**." For a defensive on a risk-on day, the question is not "how much can it rise?" but "**what holds it up?**" — and the answer was: nothing.

---

OUTCOME_BEGIN
SECTOR: Utilities
ETF: XLU
ETF_PCT: -0.31
SPY_PCT: +0.85
REL_PCT: -1.16
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: flat
PRIMARY_DRIVER: Risk-on relief rally on in-line CPI (headline +0.4% MoM / +3.4% YoY, core +0.3% / +2.4%) plus oil offering hard rotated capital out of the defensive bond-proxy complex; XLU was the funding source for the SPY rip, with a sticky-high long end (10Y 4.83%, 30Y 5.28%) and a Fed hike "very likely next week" removing any duration bid.
KEY_INTERACTION: The morning scored green futures + oil-offering as OFFSETS to the sticky-high long end; they were actually REINFORCEMENTS of the risk-on rotation-away pressure. The CPI binary was treated as symmetric when it was asymmetric-negative for a defensive bond proxy (both branches hurt XLU). The 09-10 gate correctly fired (VIX <20 backwardated → rising long end = relative-lag signal) but the morning failed to act on it, capping confidence at 0.55 and scoring rotation as only PARTIAL.
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Direction HIT (down called, −0.31% realized) and magnitude HIT by the band letter (flat), but the headline miss is the unflagged −1.16% relative lag — every input needed to call down/mild with a relative-lag clause was on the desk at the open; the miss was inferential (wrong sign on the "offsets," symmetric treatment of an asymmetric binary), not informational.
OUTCOME_END