# Sector Outcome — Consumer Cyclical — 2026-09-11

Actuals: {'etf': 'XLY', 'pct': 0.8931761416374417, 'spy_pct': 0.8524287494320992, 'rel': 0.040747392205342514, 'open': 112.87000274658203, 'close': 112.95999908447266, 'source': 'yf_download'}

# Sector Post-Session Review — Consumer Cyclical (XLY) — 2026-09-11

## 0. FACTS

**Tape (deterministic actuals):**
- XLY: **+0.89%** (open 112.87 → close 112.96)
- SPY: **+0.85%**
- Relative: **+0.04%** (essentially dead flat)
- Actual direction: **up**; actual magnitude: **mild** (sub-1%, no gap-and-go)

**Path:** XLY opened at 112.87, closed at 112.96 — a **~0.08% intraday drift** on top of an opening gap. Nearly the entire +0.89% was the **gap**, not the session. This is a "CPI-day relief gap, then flatline" tape, not a trend day.

**Macro context (verified):**
- CLAIM: August CPI rose 0.4% m/m and 3.4% y/y, matching expectations.
  URL: https://www.stephens.com/perspectives/consumer-price-index-update-september-11-2026
  PUBLISHED: 2026-09-11
  QUOTE: "The report showed prices increased 0.4% from July to August and increased 3.4% year over year."
  SUMMARY: The CPI binary resolved **in line** — not hot, not cool.

- CLAIM: Stocks surged Friday after CPI matched expectations and oil fell; S&P 500 broke a four-day losing streak, +0.9%.
  URL: https://apnews.com/article/wall-street-stocks-dow-nasdaq-67a463295d9ea178d7802ca4338a6eb5
  PUBLISHED: 2026-09-11
  QUOTE: "The S&P 500 climbed 0.9% and broke a four-day losing streak."
  SUMMARY: Broad risk-on relief day; the driver was **CPI-in-line + oil retreat**, exactly the two objects the morning note flagged.

- CLAIM: Core inflation ran hotter, keeping pressure on the Fed to hike.
  URL: https://www.bloomberg.com/news/live-blog/2026-09-11/us-cpi-report-for-august-live-updates
  PUBLISHED: 2026-09-11
  QUOTE: "Hotter Core Inflation Adds Pressure for Fed Rate Hike"
  SUMMARY: The headline was in-line but the **core was firm** — the hawkish regime was *not* disarmed, it was merely not worsened.

---

## 1. What actually drove the sector

The taxonomy-aligned driver set for XLY today:

1. **Shared macro (dominant): CPI-in-line → relief beta.** The single scheduled binary resolved benignly. The market was positioned for a benign print (green futures), got one, and the four-day slide reversed. XLY, as a high-beta growth/duration book (AMZN+TSLA ~41%), participated in the broad relief rally — but only *at market*, not *ahead of it*.

2. **Oil relief leg (secondary, sector-relevant).** WTI/Brent were already down 2.5–3.4% pre-open and the AP/Investopedia wrap confirms "oil prices fell" as a named driver. For a discretionary sector carrying a gasoline-tax narrative, this is a genuine (if modest) tailwind — but it was **already in the tape at the open**, so it cannot explain the *session*.

3. **No sector-owned catalyst.** No fresh AMZN/TSLA/HD print. XLY's move is **inherited beta**, not idiosyncratic. The +0.04% relative print is the tell: XLY did not outrun or lag SPY — it *was* SPY.

**Primary driver:** CPI-in-line relief rally + oil retreat → broad risk-on; XLY rode beta with zero sector alpha.

---

## 2. Audit of morning S0–S4 reads

### S0 (shared macro) — morning −0.5 → **WRONG SIGN, right reasoning**
The morning note built a **downside-skewed** S0 on the 09-04 pending-macro asymmetry (hawkish regime + growth-heavy book → asymmetric downside into CPI). The asymmetry logic was sound *as a conditional*: hot print → amplified downside. But the print came **in line**, which is the branch the note explicitly said "only relieves pressure." The note then *still* scored −0.5, effectively pricing the hawkish branch as more likely than the relief branch despite green futures ≥ +0.5% and falling oil.

**This is the core error:** the note identified the correct two-sided structure, then leaned to the wrong side of its own structure. When futures are green ≥ +0.5%, oil is falling 2.8–3.4%, and the 08-21 reversal checklist is *positive*, the honest S0 is **0 to +0.5**, not −0.5. The note even wrote "the 08-21 reversal checklist is the cleanest in the recent run" — and then overrode it.

### S1 (sector factors) — morning −0.5 → **WRONG SIGN**
The note correctly identified that the **gasoline spike had flipped to relief** ("score the live sign — do not carry the spike as a fresh negative"). Having done that, it *still* netted S1 to −0.5 by leaning on the stale soft-consumer cluster (retail miss, confidence, credit) — all 2–4 weeks old, all explicitly labeled HIT_STALE/HIT_CARRIED. Per the note's own 08-17 rule, stale prints don't get restacked. The live sign was **relief**; S1 should have been **0 to +0.5**.

### S2 (breadth) — morning 0 → **CORRECT**
No expansion, no contraction. XLY moved with SPY. 0 was right.

### S3 (flows) — morning 0 → **CORRECT**
No fresh flow data; trailing outflows correctly not treated as a 1-day lid. 0 was right.

### S4 (ETF tape) — morning 0 → **CORRECT, and the note's best call**
The note invoked the 09-10 lesson: multi-horizon lag on a ≥3rd-session continuation with an oversold ETF is a **mean-reversion setup, not momentum**, and refused to re-vote the completed lag. That was exactly right — XLY mean-reverted up. S4 = 0 was the correct discipline.

**Scorecard:** S2/S3/S4 correct (all zeros, all defensible). S0 and S1 both wrong-signed. The two components that carried the −2.25 total were the two that were wrong.

---

## 3. Interactions / double-count / knowable-at-open

**Double-count check:** The morning note claimed oil was counted once (S1) and the hawkish regime once (S0). That's clean on its face — but there's a subtler **sign-flip double-count**: the note counted the *hawkish regime* as a negative in S0 while simultaneously counting *oil relief* as a negative in S1 (by netting S1 to −0.5 despite acknowledging relief). In effect, the same "risk-off, hawkish, oil-elevated" object was expressed negatively in **both** S0 and S1 — the exact 09-10 triple-count failure mode the note's own lesson warned about, just compressed into two components instead of three.

**Knowable-at-open test:** The direction was **knowable at the open — partially, and the note had the evidence but discarded it.**
- Green futures ≥ +0.5% (ES +0.63%, NQ +0.65%) → positive
- Oil falling 2.8–3.4% → positive for discretionary
- 08-21 reversal checklist explicitly "positive" and "cleanest in the recent run" → positive
- Oversold ETF after −4.45% 1m rel → mean-reversion setup → positive

Four independent positive tells were in hand. The note's own self-audit even wrote: *"That argues for flat rather than down."* The pipeline then emitted **down**. The gap between the note's prose conclusion (flat) and the pipeline's deterministic output (down) is the failure: the component scores (−0.5, −0.5) did not match the note's own stated reasoning.

**The magnitude band was right.** "Flat" magnitude was the correct band — XLY moved +0.89%, mild, no trend. The band discipline held; only the direction was wrong.

---

## 4. Outliers inside the sector

- **XLY ≈ SPY (+0.04% rel).** No outlier. The mega-cap anchors (AMZN+TSLA) neither led nor lagged. This is the cleanest evidence that today was **pure beta**, and that any sector-specific thesis (bullish or bearish) was noise.
- **No single-name divergence** worth flagging — consistent with the note's "no knowable same-morning top-holding catalyst" read, which was correct.
- **The real outlier is the prediction itself:** a −2.25 total score producing a "down" call on a day when every live, non-stale input pointed up. The model's own prose knew this; the component arithmetic didn't.

---

## 5. Lessons for the ledger

1. **When the note's prose conclusion and the component sum disagree, the sum is the bug.** The note wrote "flat is the honest band" and "argues for flat rather than down," then the pipeline emitted down. Add a **consistency gate**: if the self-audit text says flat, the component scores must reconcile to flat or the divergence must be explicitly justified.

2. **"Asymmetry" is not a direction.** The 09-04 pending-macro asymmetry lesson describes a *conditional payoff shape*, not a directional forecast. Scoring S0 = −0.5 "because asymmetry" prices the hawkish branch as the base case. When futures are green ≥ +0.5% and oil is falling, the base case is the **relief** branch. Asymmetry should widen the *distribution*, not shift the *mean*.

3. **The 09-10 triple-count lesson needs a two-component version.** S0 and S1 both expressed the same risk-off/hawkish/oil object negatively. If two components share an underlying object, one of them must be zeroed.

4. **Stale clusters cannot net a live relief sign to negative.** S1 had a live positive (oil relief) and a stale negative (2–4 week old consumer data). The live sign must dominate; S1 should have been ≥ 0.

5. **The 08-21 reversal checklist worked.** When ES/NQ ≥ +0.3%, real yields flat, and oil not spiking, the reversal checklist is a *positive* signal that should not be overridden by stale negatives. Today it fired correctly and was ignored. Promote it.

---

OUTCOME_BEGIN
SECTOR: Consumer Cyclical
ETF: XLY
ETF_PCT: 0.89
SPY_PCT: 0.85
REL_PCT: 0.04
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: CPI-in-line relief rally + oil retreat → broad risk-on; XLY rode SPY beta with zero sector alpha (entire move was the opening gap)
KEY_INTERACTION: S0 and S1 both expressed the same risk-off/hawkish/oil object negatively — a two-component version of the 09-10 triple-count failure; the note's prose said "flat" but the component sum emitted "down"
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Direction wrong (down vs up +0.89%); magnitude band correct (flat/mild); S2/S3/S4 correct zeros, S0/S1 wrong-signed — the note identified the right two-sided structure and leaned to the wrong side of it
OUTCOME_END