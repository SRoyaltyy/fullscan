# Sector Outcome — Communication Services — 2026-09-21

Actuals: {'etf': 'XLC', 'pct': 3.564664438827414, 'spy_pct': 1.5518133737258744, 'rel': 2.0128510651015397, 'open': 111.62000274658203, 'close': 114.76000213623047, 'source': 'yf_download'}

# Sector Post-Session Review — Communication Services (XLC) — 2026-09-21

## 0. FACTS

**CLAIM:** XLC closed 2026-09-21 at 114.76, +3.56% on the day, from an open of 111.62.
**URL:** injected deterministic actuals (yfinance)
**PUBLISHED:** 2026-09-21 post-close
**QUOTE:** `ETF_PCT: 3.564664438827414` / `OPEN: 111.62000274658203 CLOSE: 114.76000213623047`
**SUMMARY:** XLC's move was a full-session trend day, not a gap-and-fade. Open 111.62 → close 114.76 means the ETF added roughly **+2.8% of the +3.56% after the open** — i.e. ~79% of the day's gain was earned in the cash session, not handed over by the premarket. That matters for the audit: the morning card's PM:XLC +0.59% was a *floor*, not the day.

**CLAIM:** SPY closed +1.55% on the same session.
**URL:** injected deterministic actuals
**PUBLISHED:** 2026-09-21 post-close
**QUOTE:** `SPY_PCT: 1.5518133737258744`
**SUMMARY:** Relative return **+2.01%** — XLC beat the market by two full points. This is the single most important number in the review, because the morning card was built on a *four-session relative-lag* premise (1d rel −1.50%, 3d rel −3.64%, 1w rel −1.50%) and explicitly banned that leftover from voting. The ban was correct, and the reversal was larger than the ban implied.

**CLAIM:** The broad tape's character was "Meta, Tech Stocks Lift S&P 500 to Best Day Since August," with the Dow *down* 0.2% to 51,682.64.
**URL:** https://www.bloomberg.com/news/articles/2026-09-21/us-stock-futures-climb-ahead-of-trump-xi-meeting-as-oil-slips ; https://www.zacks.com/stock/news/2992813/stock-market-news-for-sep-21-2026
**PUBLISHED:** 2026-09-21
**QUOTE:** "Meta, Tech Stocks Lift S&P 500 to Best Day Since August" / "the Dow Jones Industrial Average (DJI) slid 0.2%, or 95.40 points, to close at 51,682.64 points."
**SUMMARY:** This is a **narrow, mega-cap-growth-led up day**, not a broad risk-on day. SPY +1.55% with the Dow red is the signature of index concentration doing the work. XLC is a two-name concentration book (META ~19.9%, GOOGL+GOOG ~18.9%), so a narrow mega-cap-growth day is *structurally* an XLC-outperformance day. The morning card had this mechanism right in Channel 2 and then declined to pay for it.

**CLAIM:** The session's macro overlay was oil-down / yields-down / AI-trade-regaining-footing, with a Trump–Xi meeting ahead.
**URL:** https://www.schwab.com/learn/story/stock-market-update-open ; https://www.thestreet.com/stock-market-today/stock-market-today-dow-jones-sp-500-nasdaq-updates-sept-21-2026
**PUBLISHED:** 2026-09-21
**QUOTE:** "Stocks rose in early trading as oil prices and Treasury yields fell and the AI trade regained its footing, with chipmakers gaining ground." / "Markets are coming off a volatile week as President Trump prepares to meet with Chinese President Xi Jinping."
**SUMMARY:** The morning card's S0 object (risk-on / oil-off / 1d real-yield relief) was **confirmed, not falsified**. The card scored it +1. The tape paid it as part of a much larger move.

**Path:** open 111.62 (already above the +0.59% premarket reference), monotone-ish advance to 114.76. No reversal signature. Magnitude: **severe** for a sector ETF (3.56% is a >2σ day for XLC), and **notable-to-severe** on a relative basis (+2.01% vs SPY).

---

## 1. What drove the sector

Taxonomy-aligned, in order of load-bearing weight:

**(a) Mega-cap growth / AI-trade re-rate — the dominant driver.** The Bloomberg headline names Meta explicitly as a lift for the S&P's best day since August. XLC's top two positions are ~39% of the fund. When META and GOOGL both re-rate hard on an AI-monetization bid, XLC's arithmetic *forces* a multi-percent move. This is the "large-cap leadership inside sector" grid cell, and it HIT.

**(b) Shared macro relief — real, but second-order.** Oil extending falls (CL=F −5.94% premarket, Morningstar confirming "oil prices extend falls" intraday), yields lower, VIX 14.98 in contango. This is the S0 object. It is a *permission* condition for a duration-heavy growth book, not the cause of a +3.56% print. Scored +1 in the morning; that was directionally right and magnitude-light.

**(c) Antitrust-relief carry + Connect positioning.** The Google ad-tech no-breakup remedy (ruling 09-02, unsealed ~09-16/17) is stale-positive but it removes a tail. Meta Connect on 09-23 (T+2) is a live, knowable-at-open catalyst family sitting on the AI-monetization spine. Both were correctly identified in the morning card and both were correctly *not* paid as fresh S1+. They were, however, part of why the tape was willing to bid the two names aggressively into the event.

**(d) What did NOT drive it.** Telecom (VZ/TMUS/T) is mid-single-digit weight and flat; NFLX was ~flat-red premarket; AMX/APP/RUM/TTWO are not the book. The morning card's insistence that these "must not drive the ETF" was correct and the outcome vindicates it — this was a two-name day, not a breadth day.

---

## 2. Audit of morning S0–S4 against reality

**S0_SHARED_MACRO = +1. VERDICT: CORRECT DIRECTION, UNDERWEIGHTED.**
The card built one object (risk-on / oil-off / 1d real-yield relief), refused to double-count the stale FOMC/Warsh/Dow-worst-week News Judge items, correctly killed the geo-oil cap (CL=F −5.94%), and correctly refused S0 = −1 under the 08-21 rule. It capped at +1 because of carried DFII10 +20bp 1m and corr −0.592. The cap was defensible *as a cap on the macro object* — but the macro object was never going to be the thing that produced a 3.56% print. The error is not the score; it is that the card treated S0 as the primary engine when the primary engine was S1/S4 concentration. **No change to the score, but a change to the weighting logic.**

**S1_SECTOR_FACTORS = +1. VERDICT: CORRECT DIRECTION, SEVERELY UNDERWEIGHTED — this is the main miss.**
The card wrote an explicit +1 (correctly refusing the 09-10 default-0 trap), listed the right spine (IAB +12.3%, Meta Q2 ads +27%, Cloud +82%, Muse/Advantage+, Connect 09-23), correctly refused to re-pay stale antitrust relief, and correctly refused to let NFLX's stale Wells Fargo note set S1 = −1. Every individual judgment was right. The magnitude was wrong. The card's own reasoning — "dual anchors bid together on the one ad/AI object, Connect is a live same-week catalyst family, no spine negative" — describes a **+2**, and it was talked down to +1 by the "no same-morning revenue print" clause. That clause is a *magnitude* discipline, not a *direction* discipline, and it was applied to a book whose entire thesis is two names that re-rate on narrative, not on prints. **A two-name book with a live T+2 catalyst and both anchors bid premarket is a +2 setup, not a +1.**

**S2_BREADTH = 0. VERDICT: CORRECT, and the reasoning held.**
The card refused to restack the dual-anchor bid as a second +1 (09-11 same-print cap), refused to map NQ/ES onto XLC, and correctly noted that nested MAP HEAT names (RUM/TTWO/APP/SPHR) do not average into the parent. Outcome: the day was *not* broad — Dow red, defensives red, energy red. Breadth was genuinely 0 as a *separate* factor. The card was right that this was not a breadth day. It was wrong to conclude from "not a breadth day" that the day would be small — because concentration, not breadth, was the engine.

**S3_FLOWS_POSITIONING = 0. VERDICT: CORRECT.**
ETFdb leftover outflows (5d −$254M, 1m −$132M, 3m −$1.4B, 1y −$2.9B), no live inflow spike, crowded-long absent (1m rel +0.25%). The card correctly refused to score prior-window outflows as a same-day drag and correctly refused to restack washout-bounce fuel already expressed in S0/S4. Outcome consistent: this was not a flow-driven day, and there is no evidence of a same-day flow event. **0 was right.**

**S4_ETF_TAPE = +1. VERDICT: CORRECT DIRECTION, UNDERWEIGHTED — and the most instructive cell.**
The card used live PM:XLC +0.59% with both anchors bid as a "participation certificate," explicitly invoking the 09-16 falsifier and the 08-28 leftover ban. That was exactly right and it is the single best call on the card: it refused to let 1d/3d/1w rel −1.50/−3.64/−1.50 set S4 = −1. But S4's range is capped at ±1, so the card could not express "the tape is confirming *and* the confirmation is strong." The +0.59% premarket print was the *floor*; the cash session delivered +2.8% more. **The structural lesson: when the leftover-relative ban fires and live PM is green with both anchors bid, S4 = +1 is a floor, and the magnitude must be carried by S1, not by S4.**

**Multiplier 0.85 / confidence 0.56. VERDICT: THE DAMP WAS THE ERROR.**
The multiplier was set by topic hit 20% (3/15) and size_gate, with the four official overlay misses 09-15..09-18 cited as arguing "damp, not a license to zero a green dual-anchor card." The card explicitly recognized the four misses were *overlay-created* (engine wrote up from ES/NQ while ignoring a non-green XLC). Today the condition that created those misses — non-green XLC, non-participating anchors — was **absent**. Applying the same damp to a card whose stated falsifier had flipped is a category error: the damp was calibrated on a failure mode that was not present. Confidence 0.56 on a card whose own self-audit says "dual-leader + green PM is cleaner than 09-16/17/18" is internally inconsistent.

---

## 3. Interactions / double-count / knowable-at-open

**Double-count audit — the card was clean.** One rates object (FOMC/Warsh/yields/Dow-worst-week = same object, stale). One ad/AI object (IAB + Meta Q2 + Cloud + Muse + Connect = one thesis, per 08-11). S2 not restacked onto S1/S4. S3 not restacked onto S0/S4. No NQ/ES mapping. This is genuinely good hygiene and it is why the card got the *direction* right on every cell.

**The interaction the card missed:** S1 and S4 were treated as two modest positives on different objects (spine vs tape). In reality they were **the same object measured twice** — the dual-anchor bid *was* the spine expressing itself in the tape. The card's own 09-11 same-print cap forbade stacking them, which is correct for *direction*, but the card then read the non-stackability as evidence of *modest* magnitude. The correct read is the opposite: when the spine and the tape are the same object and both point up, the magnitude is *larger*, not smaller, because there is no offsetting factor anywhere on the card. Every cell was ≥ 0. A card with no negative cell and a live T+2 catalyst on a two-name book is not a mild card.

**Knowable-at-open test — YES, and this is the uncomfortable finding.** Everything needed was on the card at 09:00:
- PM:XLC +0.59%, second-best sector print on the board
- META ~+2.5%, GOOGL ~+1% premarket — both anchors bid
- Oil down 1–6%, yields lower, Asia +1.04%, Europe +0.95%
- Connect T+2, knowable
- Leftover relative lag explicitly banned from voting
- The 09-16 falsifier explicitly named and explicitly satisfied

The card *wrote down the correct answer* in Channel 2 ("That is the 09-16 falsifier, not the 09-16 mapping error") and then scored it +1/+1/0/0/+1 with a 0.85 damp. The information was sufficient for a larger call. This is not a knowability failure; it is a **calibration failure** — the card's magnitude discipline (built to prevent over-writing on non-participating tapes) was applied to a participating tape.

---

## 4. Outliers inside the sector

- **META** — the named driver in the Bloomberg headline. ~19.9% of the fund. A ~+2.5% premarket bid that held and extended is, by itself, worth roughly +0.5% to XLC; the actual move implies META and GOOGL both ran materially harder than the premarket print.
- **GOOGL/GOOG** — ~18.9% combined, bid ~+1% premarket, and carrying the stale-positive antitrust-relief tail. Second engine.
- **NFLX** — ~flat-red premarket, stale Wells Fargo Underweight. Correctly excluded from the call. Its non-participation is the reason XLC did not go even higher; it is also the reason the card's "do not let NFLX drive S1 to −1" instruction was right.
- **Telecom (VZ/T/TMUS)** — flat, mid-single-digit weight, no contribution. Confirms the "telecom is not the thesis" framing.
- **The Dow being red while SPY was +1.55%** is the sector-level outlier worth flagging: this was a day where *not* owning mega-cap growth was the losing position. XLC's concentration was an asset today, having been a liability for the prior four sessions.

---

## 5. Verdict and carry-forward

The morning card got **direction right on all five cells** and **magnitude wrong by roughly a full band** (mild vs the actual severe/notable). The four-session overlay-miss streak (09-15..09-18) was created by the engine writing up on non-participating tapes; today the tape participated and the card *still* damped, which means the damp has become a standing bias rather than a conditional correction.

**Carry-forward rules for this scope:**
1. **When the 09-16 falsifier is explicitly satisfied** (PM:XLC green, both anchors bid), the 09-15..09-18 overlay damp does **not** apply. Damp is conditional on non-participation, not on recency of misses.
2. **S1 on a two-name book with a live T+2 catalyst and both anchors bid is a +2, not a +1.** "No same-morning revenue print" is a magnitude caveat for broad books; for a 39%-concentrated book it is not a reason to halve the spine.
3. **A card with no negative cell and a live catalyst is not a mild card.** Check for the absence of offsets explicitly before settling on the band.
4. **S4 = +1 with a green PM and both anchors bid is a floor.** Magnitude must be carried by S1; do not let S4's ±1 cap implicitly cap the day.
5. **Do not re-apply a damp calibrated on a failure mode that is absent.** State the failure mode, test whether it is present, and only then damp.

OUTCOME_BEGIN
SECTOR: Communication Services
ETF: XLC
ETF_PCT: 3.56
SPY_PCT: 1.55
REL_PCT: 2.01
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: severe
PRIMARY_DRIVER: Mega-cap growth/AI-trade re-rate in the two-name book (META ~19.9%, GOOGL+GOOG ~18.9%) on a narrow, tech-led up day (SPY best day since August, Dow red), with oil-off/yields-lower macro relief as permission and Meta Connect T+2 as a live catalyst.
KEY_INTERACTION: S1 (spine) and S4 (tape) were the same object measured twice — the dual-anchor bid was the spine expressing itself — and the card read non-stackability as evidence of modest magnitude when it actually implied larger magnitude (no negative cell anywhere on the card).
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Direction correct on all five cells (S0 +1, S1 +1, S2 0, S3 0, S4 +1) with clean double-count hygiene, but magnitude wrong by a full band — the 0.85 damp and the +1 S1 cap were calibrated on the 09-15..09-18 non-participation failure mode, which was explicitly absent today (PM:XLC +0.59% green, META and GOOGL both bid, 09-16 falsifier satisfied).
OUTCOME_END