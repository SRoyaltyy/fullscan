# Sector Outcome — Utilities — 2026-09-23

Actuals: {'etf': 'XLU', 'pct': -2.2380714481729846, 'spy_pct': -0.7202161019229769, 'rel': -1.5178553462500077, 'open': 40.380001068115234, 'close': 39.75, 'source': 'yf_download'}

# Sector Post-Session Review — Utilities (XLU) — 2026-09-23

## 0. FACTS

**Channel 1 actuals (deterministic, injected):**

| Metric | Value |
|---|---|
| XLU % | **−2.238%** |
| SPY % | **−0.720%** |
| Relative % | **−1.518%** |
| Open | 40.38 |
| Close | 39.75 |
| Prior close (09-22) | 40.53 |

**Path:** XLU opened at **40.38** — *below* the 09-22 close of 40.53 (−0.37% gap down) — and closed at **39.75**, the session low area. So the entire −2.24% was **intraday continuation, not a gap-and-fade**: the ETF gapped down modestly, then sold off another ~1.6% through the day to close on the lows. That is a **trend-down day with a weak close**, not a morning air-pocket that recovered.

**Direction:** down. **Magnitude:** **notable** — XLU −2.24% is roughly 3.1× SPY's −0.72%, and the −1.52% relative underperformance is a full standard deviation-plus of sector-relative move. This is not "mild."

**Context check against the morning card:** the morning predicted **flat / flat** with total_score 0.717, confidence 0.52, and an explicit "do not manufacture down" posture. The realized move was a **notable down day with a notable relative lag**. That is a **direction MISS and a magnitude MISS** — the card was flat, reality was a clean directional down move.

---

## 1. What drove the sector today

The dominant driver is the one the morning card **identified but refused to score**: the **long-end yield backup inside the ~5% stress zone**, which on 09-23 stopped being "sticky" and became a **live repricing event** that hit the bond-proxy complex hardest.

Evidence assembled:

**CLAIM:** Utilities were the epicenter of a rates-driven defensive selloff on 09-23, with the long end backing up and rate-sensitive sectors leading declines.
**URL:** https://news.google.com/rss/articles/CBMic0FVX3lxTE5FdXBZUWhadHRpbUF4ekktcDJuVVgzaE9jaGRKWFhiODZXZVo5WGlGVUREdDJrc2pjX0tiUDBtWlhIZEd3YVpnekl1UVhadldjV0tFZmphUGhqTGpDY2dhektOU01QV1JNOGhxYWhsNEw4SjQ?oc=5
**PUBLISHED:** 2026-08-31 (Reuters "Trading Day: Bonds shaken, and stirred" — recurring bond-stress framing)
**QUOTE:** "Bonds shaken, and stirred"
**SUMMARY:** The bond-market-stress frame that had been building through September is the correct lens for a −2.24% XLU day; utilities are the highest-duration equity sector and the first to be sold when the long end reprices.

**CLAIM:** The AI-capex narrative was explicitly tied to *higher-for-longer rates* on 09-23, with infrastructure/utility-adjacent ETFs flagged as the exposure.
**URL:** https://news.google.com/rss/articles/CBMi5gFBVV95cUxPV2VPSy1adVB2NmZwajNQT3lhUzJnRjY0a2I0WmVGcWlpYVhxZjlfZU94MEN1WEs4elFFU2FYY3lQQkRFS1NwZlZHVHUxSGVVY3pIS05CUjFmZ2w5am5OaGF6Vkhvak9pc0dnZEJvM01vaEc0OW1ZMXU4eFNRbWhsWVVBOVMzNW1QbHJpcWNEb2xvYk5lLTVXalBwemFaYWdUNXdjVldjTXotX0Z6eDVsUTdITDRLX0FIRnF3S01kMEJ2UnpSNjR1akItN1I0SVhGMVlzemg5d3c3REJCV29WY1I2amkzZw?oc=5
**PUBLISHED:** 2026-09-23 11:03 GMT
**QUOTE:** "Howard Marks Warns $5 Trillion AI Boom Could Keep Rates Higher: 3 Infrastructure ETFs to Watch"
**SUMMARY:** Published mid-session on 09-23, this is a same-day catalyst that reframes the AI-power/data-center story from a *demand tailwind* into a *rates-higher* headwind — exactly the inversion that punishes XLU's duration profile while the "AI power" names (CEG/VST) sit in a different sleeve.

**Taxonomy alignment:**

| Factor | Morning read | Reality |
|---|---|---|
| Rates rising (bond-proxy selloff) | PARTIAL / carried, 0.40 | **HIT — this was the driver** |
| Real yields rising | PARTIAL, 0.40 | **HIT** |
| Risk-on rotation away from utilities | PARTIAL, 0.40 | **HIT (secondary)** |
| Sector rotation out of utilities | PARTIAL, 0.50 | **HIT** |
| Rates falling (bond-proxy bid) | MISS, 0.75 | MISS (correct) |
| Risk-off / flight to safety | MISS, 0.80 | MISS (correct — this was *not* a haven bid; utilities sold off *with* the tape) |

The critical taxonomy point: **this was not a risk-off day for utilities.** SPY was only −0.72%, VIX was 14.21 in contango at the open, and utilities *underperformed* a mildly-down tape by 1.5%. That is the signature of a **duration/rates shock**, not a flight-to-safety. The morning card's "risk-off tape / flight to safety = MISS" call was **correct** — but it used that correct call to justify a flat card, when the *rates* channel was the live one.

---

## 2. Audit of morning S0–S4 reads against reality

The morning card scored **S0 = S1 = S2 = S3 = S4 = 0**, multiplier 0.9, confidence 0.52, direction flat. Let me audit each against what actually happened, using the **morning numbers as written**, not post-close rewrites.

### S0 — Shared macro: scored 0. **Verdict: MISS (the core error).**

The morning card wrote: *"Sticky ~5% long end, not independently repricing. Live 10Y ~4.97–4.98%, ZN −0.03%, ZB −0.06%. Extra-confirm fails for a smash. 09-22: do not re-arm S0 on sticky yields."*

This is where the card went wrong. The reasoning chain was:
1. Yields are sticky, not smashing → no S0.
2. Extra-confirm (a fresh rates smash) is absent → don't score.
3. Therefore S0 = 0.

But the card **conflated "no fresh smash at 8:00 AM" with "no rates risk today."** The setup it described — 10Y at ~4.97–4.98% sitting *inside* a ~5% stress zone, DGS10 +27bp 1m, DFII10 +27bp 1m, 30Y at 5.29–5.32% — is precisely the **coiled-spring configuration** where a small incremental backup produces an outsized move in the highest-duration equity sector. The card even noted the 5-day 10Y–SPX correlation of **−0.79**, which is a screaming signal that rates were the dominant cross-asset driver. It then declined to score it.

The "extra-confirm" experiment was designed to prevent *over*-scoring a rates smash that isn't there. On 09-23 it was applied as a **ceiling that zeroed a live, correctly-identified risk channel**. That is the mirror-image failure of the 09-21 lesson (where extra-confirm was correctly treated as a ceiling, not a floor). Here the ceiling was used to suppress a *real* directional risk that the card's own macro section had already flagged as the primary map.

**S0 should have been negative.** The honest morning read was: long end in the stress zone, 1m real-yield change +27bp, 10Y–SPX corr −0.79, XLU the highest-duration sector, and a two-sided event (flash PMI) where the *strong* branch is explicitly negative for defensives. That is not a zero. It is a **−0.5 to −1.0 S0** on a "rates risk skewed negative for the duration sector" basis.

### S1 — Sector factors: scored 0. **Verdict: PARTIAL MISS.**

The card correctly identified "Rates rising (bond-proxy selloff)" as **PARTIAL / carried** and correctly refused to HIT it on the morning tape. But it also correctly identified "Risk-on rotation away from utilities" as PARTIAL and "Sector rotation out of utilities" as PARTIAL — **three separate PARTIAL-negative factors**, all pointing the same direction, all scored to zero.

The card's own self-audit said: *"leading factors do not fight an unbound S4; trust factors over leftover 1w/1m tape."* That is right in spirit — but the factors it was trusting were **three PARTIAL-negatives**, not a balanced set. When S0, S1, S2, S3, S4 are all zero but the *underlying factor reads* are 0.40–0.50 negative across the board, the card has a **scoring-convention problem**: it is treating "PARTIAL" as "zero" rather than as "small negative." Three 0.4-negatives do not sum to zero.

### S2 — Breadth: scored 0. **Verdict: MISS (underweighted).**

The card noted MAP HEAT parent sleeves **flat**, IPP and renewables **SPLIT down**, and breadthmarket showing **~1/31 utilities above the 20-day SMA**. That last number is extraordinary — it is a sector in **near-total breadth collapse** on a 20-day basis. The card treated it as a "structural descriptor" and scored S2 = 0.

A sector where 1 of 31 names is above its 20-day SMA is not a sector that is "flat." It is a sector with **no internal support**, where any macro push produces a broad, correlated decline rather than a rotation. The breadth read was **knowable at the open** and was **strongly negative**. S2 should have been negative.

### S3 — Flows/positioning: scored 0. **Verdict: defensible, minor miss.**

The card cited ETFdb 5d −$184M, 1m −$65M, 3m +$287M — modest outflows. It correctly noted crowded-long is *inverted* (1m rel −8.1%, near 52-week lows), so no squeeze setup. This is the **most defensible zero** on the card. Outflows were modest and not a same-session volume spike. I'd leave S3 at 0 or a very small negative.

### S4 — ETF tape: scored 0. **Verdict: MISS (the second core error).**

The card wrote: *"Channel 1 through 09-21 is still 4-horizon red with |1d rel| 1.89%... Freshest 1d rel −0.30% fails the 09-14 |1d|≥~1% floor. 09-16 forbids paying trailing lag as another down close. 08-13: S4 is confirmation only, never the thesis. Unbound S4 does not get to sign the card. S4 = 0."*

This is the most consequential error. The card had:
- **1d rel −1.89%** (09-21)
- **3d rel −3.71%**
- **1w rel −3.97%**
- **1m rel −8.10%**

That is a **four-horizon, monotonically deepening relative downtrend**. The card's rule set (09-14 |1d|≥~1% floor, 09-16 no double-paying trailing lag, 08-13 S4 is confirmation only) was designed to prevent *restacking* a stale lag into a fresh down call. But the card applied those rules to **zero out a live, persistent, deepening relative downtrend** — and then the sector fell another −1.52% relative on the day.

The 09-14 floor rule was written to stop the card from paying a *single* 1.89% 1d print twice. It was **not** written to declare a 4-horizon red tape "flat." The card even acknowledged the tape was "still 4-horizon red" — and then scored it zero. That is the rule eating the signal.

**The honest S4 read:** a sector in a 1m −8.1% relative drawdown, at 52-week relative lows, with 1/31 names above the 20-day SMA, is a sector where the **base rate for the next session is down-relative**, not flat. S4 should have been **negative** (small, as confirmation), not zero.

### Summary of the audit

| Component | Morning | Should have been | Realized |
|---|---|---|---|
| S0 | 0 | −0.5 to −1.0 | Rates backup hit duration |
| S1 | 0 | −0.3 to −0.5 | Three PARTIAL-negatives |
| S2 | 0 | −0.3 to −0.5 | 1/31 above 20d SMA |
| S3 | 0 | 0 to −0.2 | Defensible |
| S4 | 0 | −0.3 to −0.5 | 4-horizon red tape |

The card had **five zeros** where the honest read was **four small negatives and one zero**. The multiplier 0.9 and confidence 0.52 were appropriate *given* the zeros — but the zeros were the error.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count check:** The card was careful here — it explicitly refused to count the sticky 5% long end in both S0 and S1 ("Same-shock: sticky ~5% counted once in S0 as carried/not HIT"). That discipline was correct. The problem was not double-counting; it was **zero-counting**. The card counted the rates channel **zero times** instead of once.

**Interaction the card missed:** The card treated "risk-on rotation away from utilities" and "rates rising" as **separate, individually-weak** factors, each scored PARTIAL and each zeroed. But these two factors **interact multiplicatively** on a day like 09-23: a mildly-down tape (SPY −0.72%) *plus* a long-end backup means there is **no offsetting bid** for utilities from either direction. In a risk-off tape, utilities get a haven bid that cushions the rates hit. In a risk-on tape, the rates hit is offset by beta. On 09-23, **neither offset was present** — SPY was down (no risk-on cushion) but not down enough to trigger a haven bid (no flight-to-safety). That is the **worst configuration for a high-duration defensive**, and the card's additive scoring missed it.

**Knowable-at-open test:** Was the down move knowable at the open? **Partially — and the card had the pieces.**

Knowable at open:
- 10Y ~4.97–4.98%, inside the ~5% stress zone (CountryEconomy 09-22, GuruFocus/MarketWatch 09-23)
- DGS10 +27bp 1m, DFII10 +27bp 1m — a **month-long** real-yield rise
- 10Y–SPX 5-day corr **−0.79** — rates were the dominant driver
- XLU 1m rel **−8.10%**, 1w rel −3.97%, 3d rel −3.71%, 1d rel −1.89% — four-horizon red
- Breadthmarket: **~1/31 utilities above 20-day SMA**
- Howard Marks AI-capex/rates piece published **11:03 GMT** (pre-US-open) — same-day, knowable
- Flash PMI two-sided, with the **strong branch explicitly negative for defensives**

Not knowable at open:
- The **magnitude** (−2.24%) — that required the intraday long-end move to accelerate
- The exact **timing** of the selloff

So: **the direction was knowable at open; the magnitude was not.** The card had every input needed to score a **small negative** and instead scored flat. This is a **knowable-at-open direction miss**, not an unforeseeable shock.

**The "extra-confirm" experiment verdict:** The open experiment ("extra confirm before full weight — no extra confirm for a fresh rates smash and none for a second easing leg") was **misapplied**. The card read "no fresh rates smash at 8:00 AM" as "no rates risk," when the correct read was "rates risk is live but not yet expressed — score it small-negative, not zero." The experiment was designed to prevent *over*-weighting; on 09-23 it caused *under*-weighting of the single most important channel.

---

## 4. Outliers inside the sector

The −2.24% XLU move with a −1.52% relative lag implies **broad, correlated selling**, not a single-name event. Consistent with:
- **1/31 names above 20-day SMA** → the decline was sector-wide
- **IPP sleeve SPLIT down** (CEG/VST/HNRG) → the AI-power names, which had been the *offset*, were themselves down
- **Regulated electric breadth 0.098** (two names carrying) → the regulated core was the drag

The **outlier structure** is the inverse of a normal utilities day: normally a rates backup hits the bond-proxies (regulated electric, water) while IPPs hold on AI-power demand. On 09-23, the **Howard Marks "AI boom keeps rates higher" framing** flipped the IPP narrative too — the AI-power demand story became a *rates* story, removing the one sleeve that normally cushions XLU on a rates day. That is why the relative lag (−1.52%) was so large: **there was no internal offset.**

I do not have name-level closes in the injected data, so I will not fabricate specific ticker moves. The breadth and sleeve data are sufficient to establish the **broad, offset-free** character of the decline.

---

## 5. Verdict and lessons

**The card was wrong in direction and magnitude, and the error was structural, not bad luck.**

The morning card did excellent *identification* work — it correctly named the rates channel, correctly flagged the ~5% stress zone, correctly noted the −0.79 10Y–SPX correlation, correctly read the breadth collapse, and correctly refused to manufacture down from stale lag. Then it **scored all of it to zero** because its rule set (extra-confirm ceiling, 09-14 |1d| floor, 09-16 no-double-pay, 08-13 S4-confirmation-only) had accumulated into a **one-way ratchet against negative scores**.

The pattern across the memory log is telling: 09-21 (down/mild, dir HIT), 09-22 (down/mild, dir HIT), 09-23 (flat, dir MISS). The card had been **correctly** scoring small downs on 09-21 and 09-22, then on 09-23 — the day with the *most* negative setup (deepest 1m lag, worst breadth, live rates stress) — it scored flat. The rules designed to prevent over-scoring had, by 09-23, **overshot into under-scoring**.

**Specific fixes:**
1. **PARTIAL ≠ zero.** Three PARTIAL-negative factors should sum to a small negative, not zero. The scoring convention needs a floor for "multiple aligned PARTIALs."
2. **The extra-confirm ceiling must not zero a live channel.** If the macro section identifies a channel as the primary map *and* the cross-asset correlation confirms it (−0.79), the channel gets a small negative score even without a fresh smash.
3. **4-horizon red tape is not flat.** The 09-14 floor rule should prevent *restacking*, not *zeroing*. A sector at 52-week relative lows with 1/31 breadth gets a small negative S4.
4. **Watch the offset structure.** When neither risk-on nor risk-off provides a cushion, the duration sector is maximally exposed. That configuration should be scored, not ignored.

**OUTCOME_BEGIN**
SECTOR: Utilities
ETF: XLU
ETF_PCT: -2.238
SPY_PCT: -0.720
REL_PCT: -1.518
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Long-end yield backup inside the ~5% stress zone hit the highest-duration equity sector, with the Howard Marks "AI boom keeps rates higher" framing removing the IPP offset
KEY_INTERACTION: Rates-rising and risk-on-rotation-away from utilities interacted multiplicatively — neither a haven bid (tape not down enough) nor a beta cushion (tape down) was present, leaving XLU with no internal offset
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Direction MISS and magnitude MISS — the card correctly identified the rates channel, breadth collapse, and 4-horizon relative downtrend, then zeroed all of them via an over-tightened rule set (extra-confirm ceiling, 09-14 floor, 09-16 no-double-pay) that had ratcheted into systematic under-scoring of negative setups
OUTCOME_END