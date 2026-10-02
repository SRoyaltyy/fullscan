# Sector Outcome — Consumer Cyclical — 2026-10-02

Actuals: {'etf': 'XLY', 'pct': 1.130413918326978, 'spy_pct': 0.742154083513169, 'rel': 0.3882598348138089, 'open': 109.88999938964844, 'close': 110.04000091552734, 'source': 'yf_download'}

# Sector Post-Session Review — Consumer Cyclical (XLY) — 2026-10-02

## 0. FACTS

**CLAIM:** XLY closed **+1.13%** on 2026-10-02, SPY **+0.74%**, relative **+0.39%**.
**URL:** https://www.marketwatch.com/investing/fund/xly (also Business Insider ETF page)
**PUBLISHED:** 2026-10-02 16:49 NYA
**QUOTE:** "XLY 110.54 +1.73 +1.59% 10/02/2026 04:49 PM NYA"
**SUMMARY:** The tape print (110.54 / +1.59%) is the *last* quote of the session and is slightly richer than the deterministic close used here (OPEN 109.89 → CLOSE 110.04, +1.13%). Both agree on sign and rough size: XLY was **up ~1.1–1.6%**, a **notable** absolute move, and it **outperformed SPY by ~+0.39%**. Note the path: XLY opened at 109.89 — *above* the prior close — and closed at 110.04, i.e. the entire gain was essentially **gapped in at the open** and then held. There was no intraday trend leg; the session was an open-and-hold.

**CLAIM:** September 2026 nonfarm payrolls printed **+29,000**, far below the +90k consensus; August revised down to +133k.
**URL:** https://www.bls.gov/news.release/archives/empsit_10022026.htm
**PUBLISHED:** 2026-10-02 (8:30 ET release)
**QUOTE:** "Total nonfarm payroll employment changed little in September (+29,000), following an average monthly gain of 45,000..."
**SUMMARY:** The single most important input to this card — the one the morning explicitly refused to pre-score — landed as a **large downside miss**. This is the session's dominant fact.

**CLAIM:** Trading Economics confirms the miss vs forecast.
**URL:** https://tradingeconomics.com/united-states/non-farm-payrolls
**QUOTE:** "The US economy added 29K jobs in September 2026, following a downwardly revised 133K in August and well below forecasts of 90K."
**SUMMARY:** Confirms direction and magnitude of the surprise.

**CLAIM:** HubbardOBrien characterizes the report as "Unexpectedly Weak."
**URL:** https://hubbardobrieneconomics.com/2026/10/02/unexpectedly-weak-september-jobs-report/
**QUOTE:** "According to the establishment survey, there was a net increase of only 29,000 nonfarm jobs during September."
**SUMMARY:** Independent confirmation of the weak-labor read.

**Facts as given (deterministic):**
- ETF_PCT **+1.130%**; SPY_PCT **+0.742%**; REL_PCT **+0.388%**
- OPEN 109.89 / CLOSE 110.04 → gain was **open-gap, then flat**
- ACTUAL_DIRECTION: **up**; ACTUAL_MAGNITUDE: **notable** (≈1.1%, ~1.5x SPY)

---

## 1. What drove the sector today

The taxonomy-aligned driver is **duration relief via a weak-labor → rates-path repricing**, transmitted through XLY's growth-heavy book (AMZN ~23% + TSLA ~17.5% ≈ 40%+ of the ETF).

The causal chain, in order:

1. **NFP +29k vs +90k consensus** (BLS, 10-02). A ~61k downside surprise with a downward August revision.
2. **Rates path repricing.** A labor miss of this size pushes the Fed's next move from "two-sided" toward "on hold / cut-leaning," which mechanically **lowers the front-end and real-yield path** — the exact object the morning card identified as the *tax* on the AMZN/TSLA sleeve ("DFII10 2.93, +49 bp 1m is still a tax on the AMZN/TSLA growth sleeve").
3. **Long-duration equity bid.** XLY's top two names are the market's longest-duration mega-caps. When the real-yield tax is lifted even partially, they lead. That is precisely the "duration relief for AMZN/TSLA" branch the morning card wrote into HORIZON_3D: *"a clean miss is duration relief for AMZN/TSLA."*
4. **Broad risk-on confirmation.** SPY +0.74% on the same print — the market read the miss as **good news** (soft-landing / Fed-pivot), not as recession-onset. That is the regime tell: a weak jobs number that lifts equities is a *rates* trade, not an *earnings* trade.

Secondary/confirmatory: oil offered (WTI −1.59%, CL=F −3.97%) is a mild discretionary tailwind at the pump, and Europe +0.79% / VIX contango were consistent with a risk-on open. But these were **already in the morning tape** and cannot explain a +1.1% session that gapped at 9:30 on the NFP print.

**PRIMARY_DRIVER:** Weak NFP (+29k vs +90k) → rates-path/duration relief → AMZN/TSLA-led XLY bid, gapped at the open and held.

---

## 2. Audit of the morning S0–S4 reads against reality

The morning card was **explicitly and deliberately unsigned**: S0=S1=S2=S3=S4=0, leading_sum 0.0, predicted **flat/flat**, with a prose "relative lean slightly down." The engine, by contrast, emitted total_score 3.723 with a tape_anchor of 2.632 and index_carry 1.091 — i.e. the *engine* was leaning up off the green ES/NQ/PM tape, while the *analyst card* overrode it to flat. Let me grade each leg.

### S0 — Shared macro: **morning 0 → reality +1. MISS (in the sense of under-scoring a knowable-direction event).**

The morning card's S0 reasoning was structurally sound but **directionally agnostic by construction**:

- **(a) NFP as "the session binary, not a signed mean."** This was the correct *process* call — you cannot pre-score an unprinted number. **But** the card also wrote the *conditional map* explicitly: *"a clean miss is duration relief for AMZN/TSLA."* So the card **knew the sign of the miss branch** and chose not to weight it. That is defensible risk management, but it means the card was **not** actually neutral on the *distribution* — it was neutral on the *mean* while carrying a known positive tail. The realized outcome landed squarely in the branch the card had already labeled bullish for XLY.
- **(b) Real yields "persistent level, not escalating shock."** Correct as of the open. The NFP miss then *changed the increment* — this is the one input the card could not have known, and it is the input that flipped the day.
- **(c) Oil offered.** Correct; mild positive, correctly not put in S0.
- **(d) 08-27 XLK-map ban / 09-21 certificate.** The card refused S0=+1 because Asia was red and real yields weren't dipping. **Asia red was a real signal that did not matter** — the US rates channel dominated. The 09-21 certificate requirement (real yields dipping) was *satisfied intraday* by the NFP reaction, but not at the open.

**Verdict on S0:** The *level* (0) was wrong for the day, but the *reasoning* was honest. The card correctly identified NFP as the binary and correctly wrote the bullish branch — it simply declined to take it. This is a **process-pass / outcome-miss**: the card was right that it couldn't know, and right about what would happen if the miss came.

### S1 — Sector factors: **morning 0 → reality +1 (mild). MISS (under-scored).**

The card's S1 netting was: leftover spend/claims/RevPAR **+** cancel paid confidence / nested footwear / credit-level **−**. Two problems:

- **Nike was correctly nested** (~1.2% weight, ~12 bp of XLY). The card's insistence that "Nike must not drive the ETF call" was **vindicated** — Nike's ~−10% premarket did not prevent XLY from +1.1%. **Process win.**
- **The card treated the labor object as "unprinted → don't double-count."** Correct. But it also had **claims 197k** as a *support* input and then refused to let it lean positive. In a session where the labor *spine* was the driver, the card had a live positive labor-support datapoint (claims) and a known bullish branch on the bigger labor print, and still netted to zero. That is **over-neutralization** — the 09-23 "unsigned-justification" gate was applied so aggressively that it flattened a genuinely one-sided setup *conditional on the print*.

**Verdict on S1:** Nike nesting = **win**. Net-zero = **miss** (too flat given the conditional map).

### S2 — Breadth: **morning 0 → reality +1. MISS.**

The card said: *"S2=0 unless a live mega-cap breakdown is confirmed"* and *"nested-down + mega-cap-bid is relative quality, not an absolute down vote."* Both correct in *direction* — but the card had **AMZN ~+0.5–0.6%, TSLA ~+0.7% green at the open** and MAP HEAT showing broad retail sub-industries down. The realized session was **large-cap leadership inside the sector** (the HIT_GRID's own "Large-cap leadership inside sector" = HIT). The card saw this and still scored 0. The correct read was **+1 (large-cap leadership, breadth narrow but positive)**.

**Verdict on S2:** Directionally identified, numerically under-scored. **Miss.**

### S3 — Flows: **morning 0 → reality ~0. PASS.**

The card correctly read the Oct-1 −$213M as uniform creation/redemption trims, not an active dump, and correctly refused to treat trailing outflows as a 1-day lid. XLY rose +1.1% — flows were not a constraint. **Process win.**

### S4 — ETF tape: **morning 0 → reality +1. MISS (under-scored).**

The card said 1d rel −0.21% is sub-gate, 3d flat, 1w/1m leftover, and *"Live PM +0.39% is not a lagging crash print."* All true. But the card then refused to let the **live PM +0.39%** lean positive, citing 08-28. In reality, the PM tape was **the correct leading indicator** — XLY gapped up and held. The card had the right signal (green PM, green ES/NQ) and the 08-28 rule told it to ignore it. **08-28 was the wrong rule to apply today** because 08-28's premise (S0=0 with no live catalyst) was violated by the NFP binary.

**Verdict on S4:** Signal seen, correctly identified as non-crash, but **under-weighted**. **Miss.**

### Engine vs card

The **engine** (total_score 3.723, tape_anchor 2.632, index_carry 1.091) was **directionally correct** (up) but the card overrode it to flat. The card's own self-audit said: *"Trust the unsigned factor card over tape_anchor / index_carry. Do not flip official off flat."* **That instruction was wrong today.** The engine's tape_anchor was picking up the same green ES/NQ/PM that correctly preceded the gap-up. This is the **mirror image of the 10-01 failure mode** the card itself flagged: on 10-01 the engine's down/mild was the miss and the flat card was right; on 10-02 the engine's up-lean was right and the flat card was the miss.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count audit (morning card's own list):**
- Rates in S0 as a *level*, not again in S1 — **held**. No double-count.
- Oil offered in S0, not a gasoline-spike HIT in S1 — **held**. Correct; oil was relief, not a spike.
- Claims not stacked with unprinted NFP — **held**. Correct process.

So the card **did not double-count**. Its error was the opposite: **under-counting a single dominant factor** (the NFP binary) by refusing to weight either branch.

**Knowable-at-open test:**
- **NFP +29k: NOT knowable at open.** The card was right not to pre-score it. This is the honest defense.
- **The *conditional* bullish branch: WAS knowable at open.** The card wrote it: *"a clean miss is duration relief for AMZN/TSLA."* The card knew that *if* NFP missed, XLY's growth sleeve would bid. It chose not to express this as even a mild-up skew.
- **Green ES/NQ/PM, AMZN/TSLA green, oil offered, VIX contango: ALL knowable at open.** These were the engine's tape_anchor inputs and they pointed up.
- **Asia red: knowable, and it was a false negative** — it did not transmit to US cyclicals on a US-rates-driven day.

**The key interaction:** The card treated "unprinted NFP" as a reason to be **flat in both directions**. But an unprinted binary with a *known asymmetric branch map* (miss → duration relief → AMZN/TSLA up) is not symmetric — it is a **distribution with a fat positive tail for this specific ETF**, because XLY's book is the most rate-sensitive in the S&P. The card's own HORIZON_3D line said exactly this. The correct expression was **flat-to-mild-up with a stated conditional**, not flat-to-slightly-down.

**The 09-25 companion:** The card said it "does not fire (leading sum is 0, not net-negative)." Correct — and that was the tell that the card should have been *at least* flat-to-up, not flat-with-down-lean. The card's prose skew ("relative lean slightly down") was the actual error: it let the 1m rel −5.37% and nested heat pull the *tone* down while the *scores* stayed at zero. **The tone and the scores disagreed, and the tone was wrong.**

---

## 4. Outliers inside the sector

- **AMZN / TSLA (the ~40% book):** The outliers that *were* the sector. Both green at the open, both duration-sensitive, both bid on the NFP miss. XLY's +1.1% is essentially a two-name story — this is the "Large-cap leadership inside sector" HIT the grid already flagged.
- **Nike (~1.2%):** The **negative outlier that didn't matter**. ~−10% premarket on a revenue miss and FY27 high-single-digit decline guidance, yet XLY +1.1%. This is the cleanest vindication of the card's nesting discipline — a ~12 bp drag against a ~110 bp sector gain. **The card was right; the market proved it.**
- **HD (~5.1%):** Housing-sensitive, rate-sensitive — a second beneficiary of the duration-relief channel, consistent with the sector bid.
- **MAP HEAT sub-industries (Apparel, Footwear, Dept Stores, Home Improvement, Auto Parts):** Mostly down per the morning grid. The session was therefore **narrow**: mega-cap-led, sub-industry breadth weak. That is a *quality* of rally worth flagging for the 3D horizon — a two-name-led +1.1% is more fragile than a broad +1.1%.
- **XLE (−0.99% PM):** The mirror outlier — energy offered on the same oil tape, confirming the risk-on rotation *out* of defensibles/energy *into* duration.

---

## 5. Verdict and lessons

**MORNING_READ_VERDICT:** The card's **process was sound** (NFP unprinted → don't pre-score; Nike nested; no double-count; flows read correctly) but its **output was wrong** — it flattened a genuinely one-sided *conditional* setup and even carried a mild down-skew in prose. The engine's up-lean (tape_anchor 2.632) was the better call, and the card's instruction to "trust the unsigned card over tape_anchor" was the specific error.

**The core lesson:** When the session's dominant binary is **unprinted but has a known asymmetric branch map for this specific ETF** (XLY = most rate-sensitive book; miss → duration relief → AMZN/TSLA up), "flat" is not neutral — it is a **bet against the fat tail**. The correct expression was **flat-to-mild-up with the conditional stated**, not flat-with-down-lean. The 09-23 unsigned-justification gate is a tool against *manufactured* conviction, not a license to ignore a *pre-written* conditional map.

**Secondary lesson:** The 08-28 inherited-lag rule (ignore live PM when S0=0) **backfired** today because S0=0 was itself the error. Rules that suppress live tape signals are only safe when the factor card is genuinely balanced — and here it wasn't, because the NFP binary was asymmetric.

**Tertiary lesson:** Asia-red was a **false negative** for a US-rates-driven session. Cross-asset certificates (09-21) that require Asia green will systematically under-call US duration rallies.

---

OUTCOME_BEGIN
SECTOR: Consumer Cyclical
ETF: XLY
ETF_PCT: 1.130413918326978
SPY_PCT: 0.742154083513169
REL_PCT: 0.3882598348138089
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Weak NFP (+29k vs +90k) → rates-path/duration relief → AMZN/TSLA-led XLY bid, gapped at open and held
KEY_INTERACTION: Unprinted-but-asymmetric NFP binary (miss → duration relief for the most rate-sensitive mega-cap book) was treated as symmetric-flat instead of flat-to-mild-up
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Sound process (NFP not pre-scored, Nike correctly nested, no double-count) but wrong output — flattened a one-sided conditional setup and carried a mild down-skew; engine's up-lean was the better call
OUTCOME_END