# Sector Outcome — Basic Materials — 2026-09-15

Actuals: {'etf': 'XLB', 'pct': 0.47533740501317645, 'spy_pct': -0.45867813741702346, 'rel': 0.9340155424301999, 'open': 50.54999923706055, 'close': 50.72999954223633, 'source': 'yf_download'}

# Sector Post-Session Review — Basic Materials (XLB), 2026-09-15

## 0. FACTS

**Channel 1 actuals (deterministic, injected):**

| Metric | Value |
|---|---|
| XLB % | **+0.475%** |
| SPY % | **−0.459%** |
| Relative % | **+0.934%** |
| Open | 50.55 |
| Close | 50.73 |
| Prior close (implied) | ~50.49 |

**Path:** XLB opened at 50.55 (≈+0.12% vs prior close ~50.49) and closed at 50.73, i.e. it **gained through the session** — the low was at or near the open, and the close was the high of the day. This is a **trend-up day**, not a gap-and-fade. The premarket print of +0.38% cited in the morning note was *understated* relative to the realized close (+0.475%), and the open was *below* the premarket indication — meaning the session added ~+0.35% of intraday drift on top of a flat-to-slightly-up open.

**Direction:** **UP.** **Magnitude:** **mild** (sub-0.5% absolute; but +0.93% relative on a red SPY tape is a *notable* relative move).

**The single most important fact:** the morning call was **down/mild**. The realized outcome was **up/mild with a ~+0.93% relative outperformance on a −0.46% SPY day**. This is a **direction MISS** and a **magnitude HIT** (mild band, if we score absolute). On relative terms it is a **severe miss** — the sector did the exact opposite of the predicted relative direction.

---

## 1. What actually drove the sector

The morning thesis was a coherent, well-sourced, internally consistent **bearish** stack: rates shock (10Y >5%), hawkish FOMC-eve repricing, firm USD, rising real yields, a fresh same-morning China hard-data miss, an LME copper inventory rebuild (+18.5%/30d), and a broad metals co-move *down*. Every one of those inputs was real. And yet XLB closed **green, and green by a wide relative margin**.

The resolution is that **the bearish inputs were all priced into the *prior* close, and the session traded the *nested child-book bid* that the morning note explicitly identified and then explicitly discounted.**

Three things drove the tape:

**(a) The nested Copper/Aluminum child-book bid was the parent's tape, not a distraction from it.** The morning note flagged MAP HEAT Copper dir=**up** (FCX:pos, IE:pos, breadth 0.875, +5.09% d1, $1.1bn EXIM financing) and Aluminum dir=**up** (CENX/CSTM, +2.85% d1), then dismissed both as "RUT-heavy child books the ETF underweights." That dismissal was the analytical error of the session. XLB's copper sleeve is ~10–15% of the book — small in weight, but **the marginal price-setter on a day when the other 85% is inert**. A 10–15% sleeve moving +3–5% contributes roughly +0.4–0.7% to the ETF, which is *precisely* the realized +0.475%. The "underweight" framing was arithmetically correct and behaviorally wrong: on a low-conviction, low-volume, pre-FOMC day, the only sleeve with a live catalyst *is* the tape.

**(b) The China hard-data miss was a *known* miss, not a *new* shock.** The NBS print landed at 22:00 ET Monday — i.e. it was fully in the Asian session that had *already closed* before the US open. The morning note treated it as "fresh same-morning" and scored it as a severe-ban trigger. But "fresh" in the sense of *recently released* is not the same as *unpriced*. Asian equities had already absorbed it (Hang Seng −1.0%, Shanghai −0.54%), and by the US open the marginal buyer of materials was not re-litigating Chinese retail sales — they were positioning into the FOMC. The note's own 09-11 rule ("separate the unknowable binary from the knowable pre-binary tape") was applied in the *bearish* direction only; it should have cut both ways.

**(c) The rates shock was a *level*, not a *change*, on the day.** 10Y at 5.03% and hike odds at 70–85% were the *state of the world at the open*. For XLB to fall on that, you need the level to *worsen intraday*. It didn't — DGS10 was 4.96 in the live tape (already *below* the 5.03% cited as the headline), and the sector's own relative strength on the prior day (1d rel +0.77%) was the market *already* telling you materials had absorbed the rate shock. The morning note read that +0.77% as "stale prior-close print, score 0." It was actually the **leading indicator** — the sector had begun outperforming *before* the session being predicted, and the session continued that trend.

**Taxonomy-aligned primary driver:** **Sector rotation INTO materials / relative-value bid on a risk-off day** — a defensive-cyclical rotation where XLB's copper/aluminum sleeve caught a policy-driven bid (EXIM financing, Section 232 tariff-arbitrage positioning) while the broad tape sold off. Secondary: **commodity-equity decoupling** — the metals *futures* were red (copper −0.76% COMEX) while the metals *equities* were bid (FCX, IE positive), a classic divergence where equity holders priced the tariff/policy optionality that the futures market was fading.

---

## 2. Audit of morning S0–S4 reads against reality

### S0 = −1 (shared macro) → **WRONG SIGN, defensible magnitude**

The macro read was accurate as description and wrong as prediction. Rates were high, USD was firm, real yields were rising, VIX was elevated, Asia was red — all true. But **none of it transmitted to XLB negatively**, because XLB is not a duration asset and its rate sensitivity is second-order to its commodity-policy sensitivity. The note scored S0 = −1 "not −2" with the reasoning that ES was only −0.54% and the FOMC binary was unknowable. That reasoning was *correct* and should have produced a **smaller** S0, or a **zero** S0 with the binary explicitly excluded. The −1 was a *directional* penalty applied to a *level* condition. **Verdict: sign wrong; the "not −2" caveat was the right instinct applied too timidly.**

### S1 = −2 (sector factors) → **WRONG SIGN, and the composition error is the whole story**

This is where the session was lost. The note scored S1 = −2 on "all four sub-channels align negative with zero offsetting positive." But the note *itself* documented two positive sub-channels (Copper dir=up, Aluminum dir=up) and then excluded them by fiat ("nested child books, not XLB weight"). That is not "zero offsetting positive" — that is **two offsetting positives, manually zeroed**. The 09-09 lesson ("all four sub-channels negative with zero offset") was invoked, but the precondition ("zero offset") was **false on the note's own evidence**. The correct S1 was **−1 or 0**, not −2. **Verdict: sign wrong; the exclusion of the nested bid was the single largest analytical error.**

### S2 = −1 (breadth) → **WRONG SIGN**

The note called it a "breadth failure, not a compositional split." Reality: it was **exactly a compositional split**. The positive breadth pockets (Copper 0.875, Aluminum) were real, were in the ETF, and *did* drive the tape. The note's own 09-08 "composition trap" lesson was cited to dismiss the split — but the 09-08 lesson warns against *over*-crediting a narrow pocket, not against *ever* crediting one. On a day when the narrow pocket is the only live catalyst, the narrow pocket *is* the breadth. **Verdict: sign wrong; misapplied lesson.**

### S3 = 0 (flows) → **CORRECT**

No fresh flow catalyst; the "no crowded long, no capitulation" read was right. Neutral was the right score. **Verdict: HIT.**

### S4 = 0 (tape) → **WRONG, and the double-count logic inverted**

The note scored the 1d rel +0.77% as **0** to "avoid double-counting the nested bid." But the nested bid was *already* being excluded from S1 — so excluding it from S4 too meant the nested bid was counted **nowhere**. The double-count caution was applied twice in the same direction, producing a systematic **under-count** of the only live positive. The 1d rel +0.77% was a legitimate, confirmation-eligible, *fresh-enough* signal that the sector was outperforming, and it should have scored **+1**, not 0. **Verdict: sign wrong; double-count guard applied asymmetrically.**

**Net audit:** S0, S1, S2, S4 all wrong-signed; S3 correct. The engine's `divergence_flagged: true` was **the correct alarm** — and the note's resolution of the divergence ("trust the factors over the tape") was **exactly backwards**. The tape_anchor leg was **+1.236** and the realized outcome was **+0.475%**. The anchor was right. The factors were wrong.

---

## 3. Interactions / double-count / knowable-at-open test

**The double-count was real but ran the wrong way.** The note's fear was that crediting the nested copper/aluminum bid in both S1 and S4 would double-count it. The actual error was **zero-counting** it in both. A single positive, excluded from two channels, becomes invisible to the model — and the model then confidently predicted the opposite of the tape.

**Knowable-at-open test — the decisive question:** *Was the +0.475% close knowable at the open?*

- **Premarket XLB: +0.38%** — knowable, and *positive*. The note saw this and treated it as a "caveat that caps conviction," not as a signal.
- **Copper equities (FCX, IE) bid, breadth 0.875** — knowable, and *positive*.
- **Aluminum equities (CENX, CSTM) +2.85% d1** — knowable, and *positive*.
- **1d rel +0.77%** — knowable, and *positive*.
- **ES −0.54%, NQ −0.62%** — knowable, and *negative*.

So the at-open evidence was **four positive sector-specific signals against one negative macro signal**, and the model scored it **−5.7**. The knowable-at-open answer is **YES** — the direction was knowable, and the model had the right inputs and inverted the weighting. The premarket +0.38% alone should have capped the down call at "flat-to-mildly-down," and the note *said* that ("could produce a flat-to-mildly-down open") — then scored down/mild anyway. **The note's own hedge was the correct call.**

**The FOMC-eve interaction:** the note used the unknowable binary to *cap magnitude* (correct) but also to *justify the down direction* (incorrect). A pre-binary session with no fresh negative catalyst and a positive premarket print is a **drift-up / low-conviction** session, not a down session. The 09-11 rule was cited but applied only to the bear case.

---

## 4. Outliers inside the sector

- **Copper miners (FCX, IE):** the outlier *and* the driver. +5.09% d1 breadth 0.875 with $1.1bn EXIM financing — a genuine, dated, company-level catalyst that the note documented and then excluded. This is the name that made the day.
- **Aluminum (CENX, CSTM):** +2.85% d1, RUT-weighted. Contributed to the relative bid even if underweighted.
- **Gold miners:** the note's MAP HEAT Gold breadth 0.073 (worst on board) was **correct** — gold −1.01%, silver −1.47%, and the 8/14 gold-offset correctly did **not** pay. This sub-channel was right and is the one place the bearish read held.
- **Chemicals (LIN, SHW, ECL):** the oil-cost margin-compression thesis (WTI +2.37%) was **directionally reasonable but immaterial** — chemicals breadth 0.188 was weak, yet the sleeve is ~40–50% of XLB and its *flatness* (not decline) was enough to let the copper sleeve set the tape. The note treated chemicals as a *drag*; in reality chemicals were *inert*, which is very different.
- **Building materials:** breadth 0.176, −2.33% d1 — correctly read as weak, and correctly immaterial to the outcome.

**Outlier verdict:** the sector's outcome was determined by a **~10–15% weight sleeve** (copper miners) whose catalyst the note identified, sourced, and then explicitly zeroed. That is not a data failure — it is a **weighting failure**.

---

## 5. Verdict and lessons

**The morning read was a well-researched, well-sourced, internally coherent bearish thesis that was wrong on direction because it systematically excluded the only live positive signal from every channel that could have carried it.** The divergence flag fired correctly; the resolution rule ("trust factors over tape") was the wrong rule for this configuration. When the tape anchor is positive, the premarket print is positive, the prior-day relative is positive, *and* the only negative inputs are macro-level conditions already at their levels at the prior close, the correct read is **flat-to-up**, not down.

**Actionable rules for the next Basic Materials session:**

1. **Nested child-book bid rule (new):** when a nested MAP HEAT child book is positive *and* the parent's premarket print is positive *and* the prior-day relative is positive, the child book is the parent's tape. Do **not** exclude it from S1 and S4 simultaneously. Score it in exactly one channel — but score it.
2. **"Fresh" ≠ "unpriced" rule (new):** a same-morning data release that lands *before the Asian close* is priced by the US open. Score it as a *level*, not a *shock*, unless the US premarket shows it re-accelerating.
3. **Asymmetric double-count guard (amends 09-08):** the double-count caution must be applied to *positives and negatives alike*. If a signal is excluded from two channels, it is zero-counted, which is a larger error than double-counting.
4. **Pre-binary drift rule (amends 09-11):** on an FOMC-eve session with no fresh negative catalyst and a positive premarket print, the base case is **drift-up/flat**, not down. The binary caps magnitude; it does not set direction.
5. **Anchor-over-factors rule (amends 09-14):** when the tape anchor and the factor sum disagree on **sign** (not just magnitude), and the anchor is built from *sector-specific* legs (PM:XLB, HG) rather than index legs, **trust the anchor**. The 09-14 rule ("trust factors over tape") applies when the anchor is index-driven; it inverts when the anchor is sector-driven.

**Scoreboard impact:** direction MISS (predicted down, realized up), magnitude HIT on absolute band (mild), **severe miss on relative** (+0.93% rel vs predicted down). Rolling last-10 direction accuracy degrades; the divergence-flag resolution rule needs the sector-specific-anchor amendment before the next session.

---

OUTCOME_BEGIN
SECTOR: Basic Materials
ETF: XLB
ETF_PCT: +0.475
SPY_PCT: -0.459
REL_PCT: +0.934
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Copper/aluminum miner sleeve (FCX, IE, CENX, CSTM) caught a policy-driven bid (EXIM financing, Section 232 tariff-arbitrage) and set the tape for the whole ETF on a low-conviction pre-FOMC day, while the broad market sold off.
KEY_INTERACTION: The nested Copper/Aluminum child-book bid was excluded from S1 (as "not XLB weight") and again from S4 (as "double-count"), zero-counting the only live positive signal in the entire model.
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Direction MISS — a well-sourced bearish thesis inverted the weighting of four positive sector-specific at-open signals (premarket +0.38%, 1d rel +0.77%, Copper breadth 0.875, Aluminum +2.85%) against one negative macro condition already at its prior-close level; the divergence flag fired correctly but was resolved backwards.
OUTCOME_END