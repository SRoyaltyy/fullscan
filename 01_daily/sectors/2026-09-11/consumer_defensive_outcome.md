# Sector Outcome — Consumer Defensive — 2026-09-11

Actuals: {'etf': 'XLP', 'pct': 0.34902025310969975, 'spy_pct': 0.8524287494320992, 'rel': -0.5034084963223995, 'open': 83.51000213623047, 'close': 83.37999725341797, 'source': 'yf_download'}

# Sector Post-Session Review — Consumer Defensive (XLP) — 2026-09-11

## 0. FACTS

**Channel 1 actuals (deterministic, injected):**

```
ETF XLP:  +0.349%   (open 83.510 → close 83.380)
SPY:      +0.852%
REL:      -0.503%   (XLP underperformed SPY by ~50bp)
```

**Path:** XLP opened at 83.510 and closed at 83.380 — i.e. the ETF **faded from the open and finished below its opening print**, even though it closed green on the day. The +0.349% close is measured against the prior close; intraday, the tape was a **down day from the open**. SPY, by contrast, climbed +0.852%. So the relative damage was done *during* the session, not carried in from the prior close.

**Direction:** up (absolute). **Magnitude:** flat (sub-0.5% absolute). **Relative:** a clear **underperformance** — the exact opposite of the prior session's +0.65% rel FTS bid.

**Context — what actually happened in the tape (search-confirmed):**

- CLAIM: CPI matched expectations; stocks surged Friday; S&P 500 climbed ~0.9% and broke a four-day losing streak.
  - URL: https://wtop.com/national/2026/09/how-major-us-stock-indexes-fared-friday-9-11-2026/
  - PUBLISHED: 2026-09-11
  - QUOTE: "An update on inflation across the United States that came in close to economists' expectations also helped calm the market Friday. The S&P 500 climbed 0.9% and broke a four-day losing streak."
  - SUMMARY: The dominant 8:30 binary resolved **benignly** — a risk-on outcome, not a risk-off one.

- CLAIM: August CPI +0.4% m/m, 3.4% y/y; core +0.3% m/m, 2.4% y/y.
  - URL: https://www.cnbc.com/2026/09/10/stock-market-today-live-updates.html
  - PUBLISHED: 2026-09-11
  - QUOTE: "The consumer price index rose 0.4% in August, putting the annual rate at 3.4%. Core CPI... increased 0.3% on a monthly basis, while the annual rate came in at 2.4%."
  - SUMMARY: In-line print. Not a cool print that relieves duration stress, not a hot print that compounds it — a **match**, which is the "relief" branch for a two-sided binary.

- CLAIM: CPI "more or less assures a Fed rate hike in September"; investors solidified rising hike bets.
  - URL: https://www.thestreet.com/stock-market-today/stock-market-today-dow-jones-sp-500-nasdaq-updates-sept-11-2026
  - PUBLISHED: 2026-09-11
  - QUOTE: "August's Consumer Inflation report more or less assures a Fed rate hike in September as economy remains strong enough to support hike."
  - SUMMARY: The resolution was **risk-on with a hawkish rate path intact** — the worst combination for a bond-proxy defensive.

- CLAIM: Stocks surged Friday after CPI matched expectations **and oil prices fell**.
  - URL: https://www.investopedia.com/stock-market-today-dow-jones-s-and-p-500-09112026-12115543
  - PUBLISHED: 2026-09-11
  - QUOTE: "Stocks surged Friday after an important Consumer Price Index reading matched expectations and oil prices fell."
  - SUMMARY: The morning's oil-offering read **persisted** — the input-cost relief channel was real, not a head-fake.

**Net fact pattern:** a benign CPI + falling oil produced a **broad risk-on melt** (SPY +0.85%). XLP participated only marginally (+0.35%) and **faded from its open**. The morning's own framing — "green futures + oil offering + ag relief is a risk-on-leaning setup... a **relative headwind** for staples even as it is an **absolute tailwind** via input costs" — is precisely what printed. The absolute tailwind showed up (+0.35%); the relative headwind dominated (−0.50%).

---

## 1. What drove the sector today

**Primary driver: risk-on rotation away from defensives, triggered by a benign CPI resolution.**

The taxonomy-aligned factor that fired is the spine's **"Risk-on rotation away from defensives"** — which the morning scored as PARTIAL HIT (0.55) and which is the single best-mapped factor to the actual outcome. When CPI matches and the four-day slide breaks, capital rotates *out* of low-beta bond-proxies and *into* cyclicals/high-beta. XLP is the textbook funding source for that rotation.

**Secondary driver: the input-cost relief channel was real but insufficient to overcome the rotation.**

Oil fell (Investopedia confirms), ag was broadly lower in the morning panel, and the packaged-food/household-products margin tailwind the morning flagged as its strongest live spine item (S1 +0.5) **did** show up in the absolute print — XLP closed green. But margin relief is a slow, fundamental channel; it does not compete with a same-day risk-on impulse in a single session. The morning correctly identified the channel and **incorrectly weighted it as the marginal driver** when the marginal driver was the rotation.

**Tertiary: the FTS bid was absent, as the morning said — and its absence was the whole story.**

The morning's S0 = 0 and S4 = 0 were directionally right (no FTS bid), but the morning treated "no FTS bid" as *neutral*. In a risk-on tape, "no FTS bid" is not neutral for a defensive — it is a **relative negative**, because the sector's beta to the risk-on impulse is negative. That is the core analytical error.

---

## 2. Audit of morning S0–S4 reads against reality

Using the **morning numbers as written**, not post-close rewrites:

### S0_SHARED_MACRO = 0 — **MISS (underweighted)**

The morning's reasoning: "CPI is the dominant binary... do not force S0 negative merely because CPI exists... the live curve is flat-to-slightly-easing this morning, which is *not* the 09-04 setup... So the asymmetry is **milder** than 09-04."

This was **half right and half wrong**. Right: the asymmetry *was* milder — CPI matched, so the hawkish tail did not fire. Wrong: the morning framed the CPI resolution as a **two-sided binary whose benign branch is neutral-to-mildly-positive for staples**. In reality, the benign branch is **negative for staples relatively**, because a benign CPI removes the vol-hedge demand that bids defensives and simultaneously unlocks risk-on rotation. The morning's own text even said this — "VIX backwardation... is also consistent with a CPI-day hedge, which unwinds on a benign print" — but then scored S0 = 0 instead of recognizing that the *expected* unwind of the hedge is a **negative** for the sector on the benign branch.

**The knowable-at-open test:** the morning had green ES/NQ/Europe, oil offering, and a two-sided CPI. The *conditional* structure was fully knowable: benign CPI → risk-on → staples lag. The morning wrote that branch down and then failed to weight it. S0 should have been **negative** (≈ −0.3 to −0.5) as a risk-on-rotation overlay, not zero.

### S1_SECTOR_FACTORS = +0.5 — **PARTIAL HIT (right channel, wrong magnitude)**

The input-cost relief channel was **real**: oil fell, ag was lower, and XLP closed green. The morning's identification of "Input cost relief (ag, packaging, freight)" as the strongest live spine item was **correct as a fact**. But +0.5 was too large a weight for a channel that operates on **gross margin over quarters**, not on a single session's relative return. The morning itself flagged the offset ("risk-on-leaning tape... a relative headwind") but netted it to a positive rather than recognizing the two channels operate on **different time horizons** — the relief is multi-day, the rotation is same-day. Same-day wins.

### S2_BREADTH = 0 — **UNVERIFIABLE, but the outcome is consistent with broad, low-beta participation**

No MAP HEAT block was injected; the morning scored conservatively. The +0.35% absolute with −0.50% rel is consistent with **broad but shallow** participation — every staple up a little, none up enough to keep pace with a +0.85% SPY. No evidence of single-name carry. S2 = 0 is defensible.

### S3_FLOWS_POSITIONING = 0 — **HIT (no signal, no signal)**

No fresh flow print; the morning correctly declined to manufacture one. Nothing in the outcome contradicts this.

### S4_ETF_TAPE = 0 — **HIT on the rule, MISS on the implication**

The morning's 09-10-lesson correction — "yesterday's +0.65% rel is **paid**; it does not forecast a second up day, and it does not license a down day either" — was **exactly right**. XLP did not repeat the FTS bid. S4 = 0 was the correct score. But the morning stopped at "does not license a down day" and failed to ask the symmetric question: *does the paid FTS bid license a relative down day if the regime flips risk-on?* It does — and it did.

**Scorecard:** S0 MISS (should have been negative), S1 partial (right channel, overweighted), S2/S3/S4 correct-as-scored. The error is concentrated in **S0 and the S1 weighting**, and both errors point the same direction: the morning **under-weighted the risk-on-rotation channel** and **over-weighted the slow fundamental relief channel**.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count audit (the morning's own self-audit claimed cleanliness — let me test it):**

- The morning claimed oil was counted **once** (as S1 input-cost relief, not re-scored as an FTS bid in S0). **This is true and correct.** No double-count there.
- The morning claimed CPI was counted **once** (as S0 event-risk overlay, not re-scored in S1). **This is true but incomplete.** CPI was counted once — but it was counted as a *volatility/event-risk* item and **not** as a *rotation* item. The rotation effect of a benign CPI is a **separate, non-overlapping channel** from the event-risk effect, and the morning omitted it entirely. That is not a double-count; it is a **missing count**.
- The food-crash cluster was carried at half weight per the 09-10 one-session cap. **Correct application of the rule.** No fresh print appeared, and the cluster did not drive the session.

**The real interaction the morning missed:** *benign CPI × oil-offering × green futures* are not three independent mild positives for the broad tape — they are **one reinforcing risk-on impulse**. The morning scored them as separate small items (S0 = 0, S1 = +0.5) and netted to +0.45. But their **interaction** is super-additive for the *relative* staples trade: benign CPI removes the hedge bid, oil-offering confirms the benign read, green futures confirm the rotation — and all three push the *same* direction (out of defensives). The morning's additive scoring **structurally understated a correlated shock**.

**Knowable-at-open test:** **YES.** Every element needed to call the relative underperformance was in the morning panel:
- Green ES/NQ/Europe (risk-on tape) ✓
- Oil offering (confirms benign macro read) ✓
- VIX backwardation explicitly flagged as "consistent with a CPI-day hedge, which unwinds on a benign print" ✓
- The morning's own sentence: "Green futures + oil offering + ag relief is a risk-on-leaning setup for the broad tape, which is a **relative headwind** for staples" ✓

The morning **wrote the correct answer in its own text** and then scored it to zero. This is a **knowable-at-open miss**, not bad luck.

---

## 4. Outliers inside the sector

No constituent-level breadth or MAP HEAT data was injected, and Channel 1 gives no dispersion. What can be inferred:

- The **+0.35% absolute / −0.50% rel** pattern is the signature of a **uniform, low-dispersion up day** — the whole basket drifted up with the tape but lagged. This is *not* the signature of a single-name blowup or a single-name carry.
- The **open-to-close fade** (83.510 → 83.380) is the notable intraday feature: XLP **sold off through the session** while SPY rallied. That is consistent with **steady rotation out of the sector all day**, not a morning gap that held. If there were an outlier, it would be a large staple that gapped up on the benign CPI and then bled — but with no constituent data, I flag this as **UNCONFIRMED** rather than assert it.
- The absence of any fresh staples-specific catalyst (no WMT/PG/COST/KO print, Barclays conference T+2 and already traded) means the session was **pure macro beta** for XLP. No idiosyncratic outlier is required to explain the print.

---

## 5. Verdict and lesson

**The morning got the direction right (up) and the magnitude band right (flat) — and still lost the trade**, because the object of the exercise is the sector's *environment*, and the environment was a **relative underperformance**. A flat/up absolute call in a +0.85% SPY tape is a **relative miss**, and the morning's own divergence check declared "no divergence to flag" when the honest read was: *the tape and the factors agree on flat-to-mildly-up absolute **with a negative relative edge**.*

**The single transferable lesson:** when a two-sided macro binary (CPI) has a **benign branch that is risk-on**, a defensive sector's benign-branch outcome is **relatively negative**, not neutral. The morning correctly identified the hedge-unwind mechanism and then scored it as zero. The fix is to score the **conditional benign branch** of a scheduled binary as a **negative S0 overlay for defensives** when the pre-binary tape is already risk-on-leaning (green futures, oil offering). "No FTS bid" is not neutral for a defensive in a risk-on tape — it is a relative negative.

**Secondary lesson:** input-cost relief is a **multi-quarter gross-margin channel**; it should be capped at a small weight (≈ +0.2, not +0.5) when scoring a **single-session relative** outcome, because it cannot compete with a same-day rotation impulse.

---

OUTCOME_BEGIN
SECTOR: Consumer Defensive
ETF: XLP
ETF_PCT: +0.349
SPY_PCT: +0.852
REL_PCT: -0.503
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: flat
PRIMARY_DRIVER: Benign in-line CPI + falling oil triggered a broad risk-on melt (SPY +0.85%); capital rotated out of low-beta defensives, leaving XLP green in absolute but ~50bp behind on a session where it faded from its open.
KEY_INTERACTION: Benign-CPI × oil-offering × green-futures were scored as three separate mild items (S0=0, S1=+0.5) when they are one reinforcing risk-on impulse whose interaction is super-additive for the *relative* staples trade — the morning's additive scoring structurally understated a correlated shock.
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Direction (up) and magnitude band (flat) were correct, but the call missed the object — a relative underperformance — because S0 scored the benign-CPI branch as neutral when it is relatively negative for a defensive in an already risk-on tape; the morning wrote the correct answer ("relative headwind for staples") and then scored it to zero.
OUTCOME_END