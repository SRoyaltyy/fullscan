# Sector Outcome — Communication Services — 2026-09-23

Actuals: {'etf': 'XLC', 'pct': -1.9084988596132946, 'spy_pct': -0.7202161019229769, 'rel': -1.1882827576903177, 'open': 113.52999877929688, 'close': 112.55999755859375, 'source': 'yf_download'}

# Sector Post-Session Review — Communication Services (XLC) — 2026-09-23

## 0. FACTS

**CLAIM:** XLC closed 2026-09-23 at $112.56, −1.908% on the session, from an open of $113.53.
**URL:** Injected deterministic actuals (yfinance).
**PUBLISHED:** 2026-09-23.
**QUOTE:** `OPEN: 113.52999877929688 CLOSE: 112.55999755859375` / `ETF_PCT: -1.9084988596132946`.
**SUMMARY:** XLC opened essentially flat-to-slightly-down vs the 09-22 close of $113.53 (open $113.53 = ~0.00%), then sold off through the day to close near the low at $112.56. The entire −1.91% was intraday distribution, not a gap. This matters: the morning Channel 2 read of "XLC ~$113.16 / −0.33%" was a *premarket* quote that never even held — the ETF opened flat and then broke.

**CLAIM:** SPY closed −0.720% on the same session.
**URL:** Injected deterministic actuals.
**PUBLISHED:** 2026-09-23.
**QUOTE:** `SPY_PCT: -0.7202161019229769`.
**SUMMARY:** The broad tape was down but modestly. XLC's relative return was **−1.188%** — the sector underperformed a down market by more than a full point. This is a *sector-specific* loss, not beta.

**Path:** Open ~flat → close at/near session low. No recovery. That is a one-way tape, the signature of a sector being sold as a funding source or on a sector-specific catalyst, not a market-wide risk-off (SPY only −0.72%, VIX was 14.21 contango in the morning).

---

## 1. What drove the sector

The dominant fact of the day is structural and was **knowable at the open**: XLC is a two-name book (META ~19–20%, Alphabet A+C ~18–19%), and META had run **~+9–13% in the two sessions into Meta Connect**, with the keynote scheduled for **16:00 PT / 19:00 ET — after the US cash close**.

**CLAIM:** Meta had run ~13% in two sessions into the Connect keynote, and the event was after the cash close.
**URL:** https://financefeeds.com/meta-connect-2026-keynote-what-to-deliver/ ; https://5thscape.com/blog/meta-connect-keynote-live/
**PUBLISHED:** 2026-09-23 (both ~20h before close).
**QUOTE:** "Meta has run 13% in two sessions into tonight's Meta Connect keynote." / "Zuckerberg taking the stage at 4:00 PM PT (7:00 PM ET) for the main keynote."
**SUMMARY:** The setup was a textbook **sell-the-news / de-risk-into-event** configuration: a crowded long into a binary catalyst that resolves *after* cash. The correct cash-session behavior for a two-name book in that configuration is distribution, not accumulation.

**CLAIM:** META fell ~9% on soft guidance in a prior episode (context for how violently this name moves the ETF).
**URL:** https://news.google.com/rss/articles/CBMivAFBVV95cUxNU0p3bDhfcmt1RVBEMkFZWU5tVWZyd09CMXplRjBZT01IRDhOQnJ5Wkp2UVh6VDJ5aF9wa00yaTdVM3g0QmhBdkd3ZXBNVjVMb0NqN2RWTWZrVUNqemNYd0QyNkhXVXlGUzEyNDV5Rk5WSnBwaGt1WTNuVWJOZlVua25iY2JyVHhwb092Q2hGOHR3RU1zeU9TUHpLSUZmZUZXVTBWSldWTi1SMDBwVVpZdUZBRUpzbVJLa3BOUw?oc=5
**PUBLISHED:** 2026-07-30.
**SUMMARY:** META is a high-beta single-name driver of XLC; a ~9% META move is roughly a ~1.8–2.0% XLC move on weight alone. The −1.91% XLC print is consistent with META de-risking plus Alphabet softness, not with a broad market event.

**Taxonomy alignment:** The primary driver maps to **"Crowded long (extreme relative performance + valuation)"** and **"AI product monetization proof"** — but with the sign flipped from the morning's framing. The morning card scored both as MISS/neutral. In reality, the crowded-long-into-event condition *was* the driver, and it resolved as an unwind. The secondary driver is **"Sector rotation out of communication services"** — XLC was the funding source for a down tape.

**What did NOT drive it:** Oil (CL=F −4.97% was a tailwind-if-anything), rates (stale, two-sided), USD (already in the morning read), antitrust (stale 09-02/09-16). None of the macro objects the morning card debated moved XLC −1.9% relative to SPY.

---

## 2. Audit of morning S0–S4 reads against reality

The morning card was **all zeros** (S0=S1=S2=S3=S4=0), multiplier 0.80, leading_sum 0.0, divergence False — yet the engine emitted **predicted_direction: up, magnitude: mild**. That is the central failure: the deterministic pipeline converted a flat card into an up call via `index_carry 0.573` / `general_total 2.293`, which the morning narrative *explicitly* warned against ("If the engine later writes up from Finviz NDX +0.41% / leftover Nasdaq record, that is the 09-16/09-18 overlay error — overlay stays 0").

**S0_SHARED_MACRO = 0 — VERDICT: CORRECT, but under-weighted.**
The morning reasoning (one stale rates object, USD up, mixed Europe, real yields last-print easing) was sound and S0=0 was defensible. Reality: SPY −0.72% confirms a mildly negative shared macro, not zero. S0=0 was *directionally* too generous by a hair, but not the cause of the miss. The 08-21 rule (forbids S0=−1 on leftover Warsh/hike while oil offered and futures not red) held up — futures were not red at the open, and the tape only turned after the open. **S0=0 was the right call on the information available.**

**S1_SECTOR_FACTORS = 0 — VERDICT: CORRECT SIGN, WRONG MAGNITUDE.**
This is the most important audit point. The morning card *correctly* refused to score the leftover ad/AI/Muse/Connect thesis as +1, citing 09-22's lesson (leftover same-thesis spine + after-close event + non-confirming tape → S1=0). That refusal was right. **But the card stopped at zero and never considered the mirror-image: a crowded long into an after-cash binary is a *negative* same-session setup, not a neutral one.** The 09-22 lesson taught "don't score the leftover spine as +1"; it did not teach "therefore score it 0." The correct read was **S1 = −1**: de-risking into an after-close event in a two-name book that had just run 13%. The morning card identified every ingredient of the short setup (crowded long, after-cash binary, implied 4–5% move, "buy-the-rumor already ran") and then declined to sign it. That is the knowable-at-open error.

**S2_BREADTH = 0 — VERDICT: CORRECT.**
No Channel 1 XLC PM print; two anchors modestly green, ETF slightly red. S2=0 was right. Reality confirmed no breadth expansion — the ETF broke down, which is breadth *failure*, but that is downstream of S1, not an independent S2 signal. The 09-11 same-print cap (don't feed 09-21 1d rel into both S2 and S4) was correctly applied.

**S3_FLOWS_POSITIONING = 0 — VERDICT: WRONG.**
The morning card explicitly reasoned: "the binding crowded-unwind lesson says a just-derisked book into easing oil is **not** a fresh S3 minus." That reasoning inverted the actual setup. The book was **not** just-derisked — META had run 13% in two sessions into the event. The "crowded long" condition was *live and extreme*, which is precisely the S3 minus trigger. The card cited the crowded-unwind lesson to *suppress* the signal that the lesson was designed to catch. **S3 should have been −1.**

**S4_ETF_TAPE = 0 — VERDICT: CORRECT at the open, but the tape was the tell.**
XLC was absent from the Channel 1 PM board and Channel 2 showed ~−0.33%. S4=0 (no same-morning ETF tape signal) was defensible. But the *absence* of XLC from a green-ish PM board while its two anchors were green was itself a divergence — the ETF not participating in its own names' premarket strength. The morning card noted this ("two anchors modestly green, ETF slightly red — not breadth expansion") but treated it as neutral rather than as a warning that the ETF was being offered.

**Net audit:** S0 ✓, S1 ✗ (should be −1), S2 ✓, S3 ✗ (should be −1), S4 ✓. The card's *narrative* was more correct than its *scores* — it described a de-risking setup in prose and then scored it flat.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count check:** The morning card was disciplined here. It counted Warsh + hike-odds + 10Y corr as one stale rates object; Muse + IAB + Connect as one unpaid leftover thesis; refused to map ASML/APH/Nasdaq-record onto XLC; refused AMX/APP/TTWO/NFLX as drivers. **No double-count error.** The card's problem was not over-counting — it was *under-signing*.

**The real interaction the card missed:** META-crowded-long × after-cash-binary × two-name-book concentration. Each element alone is neutral-to-mild; combined, they produce a *directional* same-session sell. The card treated them as three separate zeros that sum to zero. They don't — they multiply into a negative. This is the same class of error as 09-22 (leftover spine as sole up-creator), just with the sign reversed: the card learned "don't create up from leftovers" and over-applied it to "therefore create nothing."

**Knowable-at-open test:** **YES.** Every input needed to call XLC down/mild-to-notable was in the morning packet:
- META +9–13% into the event (in the appendix: "buy-the-rumor already ran").
- Keynote after cash close (in the card: "16:00 PT / 19:00 ET — after the cash close").
- Implied ~4–5% META move around the event (in the card).
- XLC absent from PM board, ETF slightly red while anchors green (in the card).
- Two-name concentration ~38–39% (in the card).

The card had the full short thesis written out and declined to sign it. This is a **knowable-at-open miss**, not bad luck.

**The engine overlay error:** The deterministic pipeline emitted `predicted_direction: up` from `index_carry 0.573` / `general_total 2.293` despite `leading_sum 0.0`. The morning narrative *explicitly* flagged this as the 09-16/09-18 overlay error and said "overlay stays 0." The pipeline overrode the narrative. **This is a pipeline bug, not an analyst error** — but it is the proximate cause of the "up" call. The analyst's flat card, if honored, would have produced a flat call (still a direction miss vs −1.91%, but a magnitude miss rather than a sign miss).

---

## 4. Outliers inside the sector

- **META:** The outlier by construction. Ran ~13% into the event, then de-risked. The after-cash keynote means the *cash-session* move is pure positioning unwind; the event reaction itself prints 09-24 (or after-hours 09-23), outside this session's window. **Do not grade the Connect reaction into this session.**
- **Alphabet (GOOGL/GOOG):** Morning PM ~+0.4–0.5%, but as the second ~19% weight, its failure to hold green contributed to the ETF break. No fresh Alphabet catalyst — this is beta to the META de-risk plus the down tape.
- **NFLX/DIS:** Morning ~+0.3% / flat; mid-single-digit weights, not drivers. NFLX's 09-22 −1.64% is prior-close history.
- **Telecom (VZ/TMUS/AMX):** AMX JPM upgrade correctly excluded as a driver. Telecom MAP HEAT flat/low. No outlier.
- **Paramount Skydance / WBD:** A 12-state settlement headline (24/7 Wall St., 2026-09-22) is a single-name media item, not an XLC-weight driver. Correctly not in the thesis.

No single-name outlier *outside* the two anchors moved the ETF. The loss is META+GOOGL concentration plus a down tape.

---

## 5. Verdict and lessons

**The morning call was wrong on direction (predicted up/mild, actual down/notable).** The analyst's flat card was closer to right than the engine's up call, but the flat card was itself too generous: the correct read was **down/mild-to-notable** on a de-risking-into-after-cash-event setup that was fully knowable at the open.

**Lesson (do-instead for 09-24):** When a two-name-concentrated sector ETF has a top holding that has run >10% into an **after-cash** binary event, and the ETF is *absent or red* on the PM board while its anchors are green, score **S1 = −1 and S3 = −1** — do not stop at zero. "Don't create up from leftovers" is not the same as "score flat." The 09-22 lesson must be extended: a leftover thesis that has already printed and is now crowded into an after-close event is a **negative** same-session factor, not a neutral one.

**Pipeline flag:** The v2 engine's `index_carry`/`general_total` overlay produced an up call from an all-zero leading card, directly contradicting the narrative's explicit overlay-ban. This needs a guard: **if leading_sum == 0 and the narrative flags overlay-ban, the engine must not emit a signed direction from index_carry alone.**

OUTCOME_BEGIN
SECTOR: Communication Services
ETF: XLC
ETF_PCT: -1.91
SPY_PCT: -0.72
REL_PCT: -1.19
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: De-risking of a crowded META long (~+13% into the after-cash Meta Connect keynote) in a two-name-concentrated ETF, amplified by a mildly down tape
KEY_INTERACTION: META crowded-long × after-cash binary × ~38–39% two-name concentration multiplied into a directional sell; card treated them as three separate zeros summing to zero
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: S0/S2/S4 correct; S1 and S3 should have been −1 (crowded-long-into-after-cash-event is a negative same-session factor, not neutral); engine overlay wrongly emitted "up" from an all-zero card
OUTCOME_END