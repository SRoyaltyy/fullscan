# Sector Outcome — Consumer Cyclical — 2026-09-17

Actuals: {'etf': 'XLY', 'pct': 1.0982021066629155, 'spy_pct': 1.1338754633487547, 'rel': -0.03567335668583915, 'open': 112.04000091552734, 'close': 111.38999938964844, 'source': 'yf_download'}

# Sector Post-Session Review — Consumer Cyclical (XLY) — 2026-09-17

## 0. FACTS

**Channel 1 actuals (deterministic, injected):**

| Metric | Value |
|---|---|
| XLY % change | **+1.098%** |
| SPY % change | **+1.134%** |
| Relative (XLY − SPY) | **−0.036%** |
| XLY open | 112.04 |
| XLY close | 111.39 |

**Path:** XLY opened at **112.04** and closed at **111.39** — i.e. the ETF **gapped up and then faded ~0.58% off the open** into the close. The +1.098% headline is a *gap-capture* number, not a trend-day number. This is the single most important structural fact of the session and it is the mirror image of 09-16 (open-then-fade on the hawkish FOMC), except today the fade happened *after* a large positive gap rather than after a flat open.

**Direction:** up. **Magnitude:** notable (≈+1.1% absolute) but **flat on a relative basis** — XLY tracked SPY almost exactly (−0.036% rel, deep sub-gate).

**Cross-check:** third-party quote services show XLY ~$111.67, +1.36% on 09-17 (stockanalysis.com / financecharts.com, retrieved 2026-09-18). The injected deterministic close of $111.39 / +1.098% is the number of record here; the small discrepancy is a vendor/close-timestamp difference and does not change the read. SPY +1.13% and the broad tape (S&P 500 +0.59% per TradingEconomics CFD, Bloomberg "S&P 500 up about 1%", chipmaker gauge +3%, 10Y yields down from the highest since 2007) confirm a **broad risk-on rebound session**, not a discretionary-specific move.

**The headline fact:** XLY was **up in absolute terms and dead-flat in relative terms**. The morning call was **flat/flat**. On direction the call was *wrong in sign* (flat vs +1.1%); on relative performance the call was *exactly right* (−0.036% rel is the definition of flat-vs-SPY).

---

## 1. WHAT DROVE THE SECTOR

**Primary driver: broad equity beta / risk-on rebound, not a consumer-cyclical factor.**

The session's character is unambiguous from the cross-asset tape:

- S&P 500 +~0.6–1.0%; Nasdaq +1.60% (26,395.18, +416.75); Russell 2000 +0.84%; VIX **−12.14% to 15.56** (Yahoo Finance historical, 2026-09-17).
- **Chipmakers +3%** and **10Y Treasury yields declining from the highest level since 2007, snapping an eight-day rising streak** (Bloomberg, "Stock Market Today: Dow, S&P Live Updates for September 17", 2026-09-16/17).
- Gold +0.10%, crude −0.60% to ~$101.82.

This is a **rate-relief + AI/semis-led beta rebound**. The mechanism is the one the morning card explicitly flagged as *not* an XLY object: the 10Y snapping an eight-day rise is a **duration/valuation** event that lifts high-multiple growth (XLK, NQ) hardest, and lifts XLY only insofar as XLY is a mega-cap-beta vehicle (AMZN ~24%, TSLA ~17–20%).

**Taxonomy mapping:**

| Factor | Fired? | Evidence |
|---|---|---|
| Risk-on tape / equity beta expansion | **HIT** (morning grid said *miss*) | SPY +1.13%, NQ +1.60%, VIX −12% |
| Real yields falling / duration relief | **HIT** (morning grid said *miss* on falling, *HIT_CARRIED* on rising) | 10Y down from highest since 2007, 8-day streak snapped |
| Large-cap leadership inside sector | **HIT** (as scored) | TSLA PM +1.70% carried; mega-cap carry |
| Sector breadth expansion | **PARTIAL** | XLY +1.1% but rel −0.04% — participation was index-wide, not sector-wide |
| Sector rotation into discretionary | **miss → effectively miss** | rel −0.036% = zero rotation |
| Gasoline spike crushing discretionary | **HIT_LEVEL, non-binding** | pump level still a tax but crude −0.60% same-session |

**What did NOT drive it:** no fresh consumer print. Claims (8:30 ET, consensus ~208k) was a second-order input at best; the tape's leadership was semis/AI, and XLY's own relative line was flat. The August retail +1.2% beat (09-16) did **not** produce a same-session discretionary bid on 09-16, and it did not produce a *relative* bid on 09-17 either — it was simply carried along by beta.

---

## 2. AUDIT OF MORNING S0–S4 READS

The morning card scored **S0=0, S1=0, S2=0, S3=0, S4=0**, mult 0.9, confidence 0.45, regime mixed, direction flat/flat. The pipeline's deterministic engine, however, computed **total_score 7.655** with a **tape_anchor of 5.809** (ES +1.71%, ER2 +0.08%, NQ +2.10%, PM:XLY +0.61%) and **index_carry 1.846** — and still emitted **flat/flat** because the LLM overlay was 0.0 and the sector-RS veto fired (`sector_rs_veto_applied: True`, tape d1 −0.39 / w1 −1.96).

That split is the whole story of this review.

### S0 — Shared macro: scored 0. **Verdict: WRONG SIGN, and the card knew the risk.**

The card's own reasoning was: *"Discard yfinance ES/NQ +1.71% / +2.10% vs prior close (overnight already in)"* and *"Finviz futures modestly green, not a 09-11 ≥+0.5% mean-shift."*

Reality: the **discarded anchor was the correct one.** ES +1.71% / NQ +2.10% vs prior close was not "overnight already in" — it was the *pre-cash* expression of a genuine risk-on session that carried through to a +1.13% SPY close. The card applied the **09-16 discarded-anchor pathology** lesson (where the stale ES/NQ anchor had indeed misled) to a session where the anchor was *live and correct*. That is a **lesson mis-application**, not a lesson failure: the 09-16 lesson was "don't let a stale anchor override live tape," and the card generalized it to "discard the anchor," which is a different and wrong rule.

The card also correctly noted **XLK +1.28% PM leads** and invoked **08-27** to ban mapping XLK/NQ into S0=+1. That ban is about *not scoring XLY positive from a non-holding leader*. But the correct inference from "XLK leads, NQ +2.10%, 10Y snapping an 8-day rise" was not S0=0 — it was **S0=+1 for beta/duration relief**, because XLY is ~46–49% mega-cap growth-beta (AMZN+TSLA) and *does* participate in a duration-relief rally even when the leader is semis. The 08-27 ban protects against *attributing XLK's move to XLY's fundamentals*; it does not license *zeroing out the beta channel entirely*.

**S0 should have been +1.** The card's own HIT_GRID scored "Risk-on tape / equity beta expansion" as **miss** with 0.55 confidence — that was the single largest scoring error of the morning.

### S1 — Sector factors: scored 0. **Verdict: defensible, mildly wrong.**

The netting logic (fresh spend/auto/RevPAR cancel pump-LEVEL + UMich + carried credit) is sound *as a fundamental read*. But the card's own HIT_GRID shows a **lopsided HIT column**: retail sales HIT (0.85), auto SAAR HIT (0.75), RevPAR HIT (0.70), employment HIT (0.60), consumer confidence collapse HIT (0.80), credit tightening HIT_CARRIED (0.60), gasoline HIT_LEVEL (0.75), sector rotation out HIT_STALE (0.70). The misses are confidence-jump, retail-miss, rotation-in.

The card netted this to 0 by treating the HITs as "already in the price" and the pump as a *level* not a *sign*. That is internally consistent, but it produced a **flat fundamental read on a day when the sector's own spend spine (retail +1.2%, control +1.4%, 12/13 categories up) was the freshest positive data in the book.** A +0.5 or +1 tilt on S1 was available and would have been closer to right. Not a large error — the relative line was flat, so S1's contribution to *relative* performance was genuinely ~0 — but the card over-weighted the stale negatives (UMich 47.8 from 09-11, TransUnion from Q2) against the fresh positive.

### S2 — Breadth: scored 0. **Verdict: CORRECT, and the best call on the card.**

The card said: *"XLY PM +0.61% with nested HEAT mostly down vs parent is mega-cap carry, not % names expansion... 08-28: S0=0 → S2 = 0. Do not treat TSLA as the sector."*

Reality: XLY +1.098% vs SPY +1.134% → **rel −0.036%**. That is *precisely* mega-cap carry with no breadth expansion. The card's HIT_GRID scored "Sector breadth failure (ETF up, names flat)" as **HIT** — and that is exactly what happened. The ~6.4% of discretionary names above their 20-day SMA (breadthmarket, through 09-16) was the correct structural read, and it correctly predicted that XLY would rise *with* the index and *not* outperform it.

**S2=0 was right, and the reasoning that produced it was right.**

### S3 — Flows: scored 0. **Verdict: correct.**

ETFDB 5d −$528M / 1m −$973M were trailing redemptions, and the card correctly refused to treat them as a same-morning forced-flow lid. XLY rose +1.1%; the outflows did not bind. S3=0 correct.

### S4 — ETF tape: scored 0. **Verdict: correct in construction, but the card mislabeled the risk.**

The card used the 1d rel −0.19% (sub-gate) as "not confirming" and set S4=0. Correct. But the card's self-audit contains the key sentence:

> *"If the v2 engine builds a large positive `tape_anchor` from yfinance ES/NQ vs prior close, that is the 09-16 discarded-anchor pathology, not a real leading-vs-tape fight. Do not convert an all-zero card into official up."*

The engine **did** build a large positive tape_anchor (5.809) — and the card's instruction to suppress it is what produced the flat call. The card **identified the exact mechanism that would make it wrong and then chose to be wrong anyway.** That is the central finding of this review.

---

## 3. INTERACTIONS / DOUBLE-COUNT / KNOWABLE-AT-OPEN

**Double-count audit:** The card was disciplined here. FOMC was not restacked into S0 and S1; oil was kept out of S0; gasoline LEVEL was confined to S1 and netted to zero; the 1m rel −3.22% was used once (S4 "not confirming") and explicitly barred from S2/S4 restacking per 08-28. **No material double-count.** The card's *hygiene* was good.

**The real interaction error was the opposite of double-counting — it was over-suppression.** By (a) discarding the ES/NQ anchor, (b) banning XLK→S0 mapping, (c) zeroing S2 on 08-28, (d) zeroing S3 on 08-28, and (e) zeroing S4 on sub-gate, the card **removed every channel through which a broad risk-on session could register.** Five independent "don't restack" rules, each individually correct, composed into a card that could not see a +1.13% SPY day. This is a **composition failure of defensive rules** — a new failure mode worth naming.

**Knowable-at-open test:** **YES — the direction was knowable at the open, and the card had the evidence in hand.**

At the open, the card possessed:
1. ES +1.71% / NQ +2.10% vs prior close (its own Channel 1).
2. XLK +1.28% PM leading, XLY +0.61% PM.
3. VIX 16.04 and falling, VIX/VIX3M 0.813 in contango (no hedging demand).
4. 10Y at the highest since 2007 — a *mean-reversion-prone* extreme, with the 5-day 10Y–SPX corr at −0.109 (yields not the driver).
5. Gold +0.90%, DXY −0.13% — mild risk-on.

The card read (1) as stale, (2) as forbidden, (3) as neutral, (4) as neutral, (5) as neutral. **Every risk-on signal was individually rationalized away.** The correct synthesis — "broad futures are up ~2% on NQ, semis lead, VIX is collapsing, yields are snapping a streak, and XLY is 46–49% mega-cap growth beta" — pointed to **up/mild, with flat relative**. The card got the *relative* half right and the *absolute* half wrong.

**The honest verdict:** the card's flat/flat was a **defensible relative call and an indefensible absolute call.** The engine's tape_anchor (5.809) was the correct signal; the overlay's 0.0 and the sector-RS veto suppressed it. The card's own self-audit *predicted this exact failure* and instructed against the correct answer.

---

## 4. OUTLIERS INSIDE THE SECTOR

- **TSLA** — PM +1.70%, the mega-cap carry engine. The card correctly refused to let TSLA drive the ETF call (08-12/08-28 single-name rule). TSLA's Cybercab HK showcase + NHTSA inquiry was two-sided and did not flip the ETF. **Correct handling.**
- **AMZN** — PM +0.14%, essentially flat. The card's "AMZN ~flat" read was accurate. AMZN contributed beta but not leadership.
- **HD** — $2.33 dividend payable today, mechanical. Correctly excluded as a catalyst.
- **Nested HEAT** — only Auto Manufacturers nested UP vs XLY (low conviction, TSLA-mixed); apparel, dealers, parts, footwear, furnishings, HD/LOW all residual-down. The card refused to average these into the parent. **Correct** — and consistent with the flat relative outcome.
- **XLK / semis** — the actual session leader (+3% chipmaker gauge). Not an XLY object, correctly excluded from the *sector* call, but **incorrectly excluded from the beta channel** (see S0 audit).

No single-name outlier broke the ETF. The sector's +1.1% was index beta, full stop.

---

## 5. VERDICT AND LESSONS

**The call was flat/flat. Reality was up-notable / flat-relative.** Direction: **MISS** (sign wrong). Magnitude: **MISS** (flat vs notable). Relative: **HIT** (−0.036% is flat).

This is a **partial-credit session with a clear, nameable root cause**: the card correctly identified that XLY would not outperform, and then incorrectly concluded that XLY would not *move*. Those are different claims, and the card collapsed them.

**New lesson for the scope (candidate DO-INSTEAD):**

> **When the leading sum is all-zero but the engine's tape_anchor is large and positive (>~4) on a broad risk-on futures tape (NQ ≥ +1.5%, VIX falling, yields snapping a streak), do NOT suppress the anchor to flat. The correct expression is `up / flat-relative` — i.e. take the absolute direction from the anchor and the relative direction from the sector-RS veto. Flat/flat is only correct when the anchor is *also* small.**

The 09-16 discarded-anchor lesson must be **narrowed**: it applies when the anchor is *stale relative to a live opposing tape* (09-16: anchor up, cash fading). It does **not** apply when the anchor is *confirmed by live PM sector tape and a falling VIX* (09-17: anchor up, PM XLY +0.61%, VIX −12%). The card applied the 09-16 rule to a 09-17 setup that failed the rule's own precondition.

**Secondary lesson:** the composition of five independent "don't restack" rules produced a card structurally incapable of registering a +1.13% SPY day. Defensive rules need a **sum-check**: if every channel is zero but the index is up >1% pre-open, at least one channel must be allowed to carry the beta.

**What the card got right and should keep:** S2 breadth-failure reasoning (mega-cap carry ≠ expansion), S3 refusal to treat trailing outflows as a same-morning lid, S1's refusal to double-count oil into S0, and the single-name discipline on TSLA. The *relative* call was excellent. The failure was confined to the absolute-direction channel.

---

OUTCOME_BEGIN
SECTOR: Consumer Cyclical
ETF: XLY
ETF_PCT: 1.098
SPY_PCT: 1.134
REL_PCT: -0.036
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Broad risk-on beta rebound (NQ +1.60%, semis +3%, VIX −12%, 10Y snapping an 8-day rise) lifting XLY as ~46–49% mega-cap growth beta (AMZN/TSLA) — no discretionary-specific catalyst
KEY_INTERACTION: Five independent "don't restack" rules (discard ES/NQ anchor, 08-27 XLK ban, 08-28 S2/S3 zeroing, sub-gate S4) composed into a card structurally unable to register a +1.13% SPY day; engine tape_anchor 5.809 was correct and was suppressed by overlay 0.0 + sector-RS veto
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Relative call correct (flat vs SPY, −0.036% rel) but absolute direction MISS — card identified the exact suppression mechanism in its own self-audit and chose flat anyway; correct expression was up / flat-relative
OUTCOME_END