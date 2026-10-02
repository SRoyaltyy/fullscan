# Sector Outcome — Real Estate — 2026-10-02

Actuals: {'etf': 'XLRE', 'pct': 0.2949826228725527, 'spy_pct': 0.742154083513169, 'rel': -0.4471714606406163, 'open': 40.959999084472656, 'close': 40.79999923706055, 'source': 'yf_download'}

# Sector Post-Session Review — Real Estate (XLRE) — 2026-10-02

## 0. FACTS

**Channel 1 actuals (deterministic, injected):**

| Metric | Value |
|---|---|
| XLRE % change | **+0.295%** |
| SPY % change | **+0.742%** |
| Relative (XLRE − SPY) | **−0.447%** |
| Open | **$40.96** |
| Close | **$40.80** |

**Path:** XLRE opened at $40.96 and closed at $40.80 — i.e., the ETF **faded from the open all day**, giving back roughly 0.39% from the opening print. The +0.295% headline gain is entirely a gap-up that was sold into. This is the single most important structural fact of the session and it is *not* visible in the headline percentage.

**Cross-check against search results:**
- CLAIM: S&P 500 gained ~0.7% Friday; Nasdaq +1.2%; Dow +0.5%.
  URL: https://www.cnbc.com/2026/10/01/stock-market-today-live-updates-.html
  PUBLISHED: 2026-10-02
  QUOTE: "Stocks rose Friday, The S&P 500 gained 0.7%, while the Nasdaq Composite climbed 1.2%"
  SUMMARY: Confirms SPY +0.74% actual; risk-on tape, growth-led.
- CLAIM: 8 of 11 sectors rose Friday, growth leading.
  URL: https://news.google.com/rss/articles/CBMiogFBVV95cUxPOGltYUVFSU1zeVU4YUFjeTQtMWZaVWpMU3Jfa1RrNHdEN05ObGx6b3VoT0Z3X25SYWstcHhSODBNM3o1cFZ1VmEtMjFlSEd6OFRaTzdOWGlDdmdBT2RzVG1ETjJVQ0VTLXVDdlRkUWxzNFBINkt1c2o2Nm8yYXhHRzZwZ0p4UzlzMm9wUG14bE9jZFVyN3lhYWRHUld2d3FpTEE
  PUBLISHED: 2026-10-02T15:01:32Z (Benzinga)
  SUMMARY: Broad participation, growth-led — consistent with XLRE being a *laggard within a green tape*, not a decliner.
- CLAIM: 10Y moved lower to ~5.21% following the jobs report.
  URL: https://finance.yahoo.com/markets/live/stock-market-today-friday-october-2-dow-sp-500-nasdaq-september-jobs-report-080623878.html
  PUBLISHED: 2026-10-02
  SUMMARY: Confirms the duration-relief impulse persisted into the close (morning read had 10Y 5.18%).
- CLAIM: Breadth weak — only 21% of S&P 500 above 50-DMA, 40% above 200-DMA.
  URL: https://www.schwab.com/learn/story/stock-market-update-open
  PUBLISHED: 2026-10-02
  SUMMARY: Confirms the "risk-on but narrow" regime; supports the relative-laggard thesis for a non-growth sector.

**Direction:** UP (absolute). **Magnitude:** FLAT (0.295% is inside any reasonable flat band; the morning band was "flat" and that was correct on absolute magnitude).

**The verdict in one line:** the morning call was **directionally wrong on the sign of the absolute move but right on the magnitude band**, and — critically — **right on the relative call** (XLRE lagged SPY by 0.45%, consistent with the "every horizon is a relative laggard" thesis).

---

## 1. What actually drove the sector

**Primary driver: the post-NFP duration-relief impulse, transmitted weakly and then sold.**

The morning packet correctly identified the causal chain: soft NFP (+29k vs ~84k, U-rate 4.2%, prior two months revised −60k) → front-end and belly yields fall (10Y 5.18% ~−6bp, 2Y 4.73% ~−6bp) → REITs get a duration bid. That chain **did fire** — XLRE gapped up to $40.96. But the transmission was:

1. **Weak in magnitude** — a 6bp 10Y move against a 30Y still at 5.57% (multi-decade stress zone) is a *relief inside a high-rate regime*, not a regime change. The morning packet said this explicitly ("a 3–6 bp tick is not full relief," 08-21/08-27). That judgment was correct and it capped the upside.
2. **Faded intraday** — the gap was sold. XLRE closed $0.16 below its open. This is the classic "duration basket gets the open, growth gets the day" pattern. Once the yield move stopped extending, REITs had no second leg.
3. **Outcompeted by growth** — with Nasdaq +1.2% and XLK leading, the marginal dollar went to growth/AI, not to a levered-duration REIT basket. The morning packet's own MACRO MAP line — "equity risk-on often leaves REITs lagging" — was the correct read and it was the *dominant* read.

**Taxonomy alignment:** the HIT_GRID entries that fired were **Rates falling / REIT duration relief (HIT)** and **Risk-on tape / equity beta expansion (HIT)**. The entries that correctly did *not* fire: **Rates rising / REIT selloff (MISS)**, **Refinancing window opening (MISS)**, **Cap-rate compression (MISS)**, **Sector rotation into REITs (MISS)**. The grid was well-calibrated on the *mechanism*; it was the *net sign* that was mis-weighted.

---

## 2. Audit of morning S0–S4 reads against reality

### S0_SHARED_MACRO: scored **0** (mixed). Verdict: **CORRECT, and the best call of the morning.**

The packet refused both the −1 (09-23 stress-zone-with-unprinted-binary) and the +1 (08-27/10-01 "don't reclassify a still-5.6 long end as relief"). It landed on mixed 0. Reality: the duration impulse was real but capped, and the risk-on impulse was real but went elsewhere. **Mixed 0 was exactly right.** The 10-01 lesson ("do not import XLP LEVEL+dovish ⇒ flat as a REIT identity") was correctly applied — the packet did not let the soft print become a duration-up vote, and it did not let the stress-zone LEVEL become a down vote. This is the cleanest S0 read in the recent log.

### S1_SECTOR_FACTORS: scored **+1**. Verdict: **PARTIALLY CORRECT — right mechanism, over-weighted.**

The spine HIT ("Rates falling / REIT duration relief") did fire. But the packet itself flagged the offset: real yields still elevated on 1w/1m (DFII10 2.93, +17/+49bp), no same-day cap-rate compression, no refinancing window from a 3–6bp tick. The +1 was defensible as a *factor* read but it was the component that pushed the leading sum to +3 and, combined with the 0.85 multiplier, produced a total_score of 5.006 → "flat." The problem is not that +1 was wrong in isolation; it's that **+1 on S1 plus 0 on S0 plus 0 on S2/S3 plus −1 on S4 nets to a leading sum that the engine read as "flat" when the tape was actually going to be mildly up.** The factor was real but its *magnitude* was over-stated relative to the stress-zone cap the packet itself had identified.

### S2_BREADTH: scored **0**. Verdict: **CORRECT.**

The packet refused to dump stale 1w/1m lag into S2 (09-11 rule) and noted no live %-names-up tape. Reality: 8 of 11 sectors rose but breadth was weak (21% above 50-DMA per Schwab). XLRE's internal breadth was not a driver either way. **0 was right.**

### S3_FLOWS_POSITIONING: scored **0**. Verdict: **CORRECT.**

5d +$131m vs 1m −$45m, late-Sep outflow print, PM volume dry. "Checked, nothing material." Reality: no flow-driven move. **0 was right.**

### S4_ETF_TAPE: scored **−1**. Verdict: **WRONG ON SIGN, RIGHT ON SPIRIT.**

This is the component that broke the call. The packet scored S4 = −1 on the basis of "every horizon is a relative laggard" (1d rel −0.74, 3d −1.41, 1w −1.91, 1m −7.39). But **S4 is the ETF tape component, and the ETF tape on the morning of 10-02 was not negative** — XLRE was absent from the PM board (scored 0 per 09-14), and the premarket snapshots showed ~flat to +0.37%. The packet imported *multi-horizon relative lag* into S4 as if it were a *same-day negative tape signal*. That is precisely the error the 09-11 rule warns against ("do not dump stale 1w/1m lag into S2") — here it was dumped into S4 instead.

The divergence flag (leading +1 vs S4 −1) was the tell. The packet saw the conflict and resolved it as "trust factors over tape — factors say not down; tape says lag. Absolute = flat; relative skew remains lagging." That resolution was **half right**: the relative skew *did* remain lagging (rel −0.447%), but the absolute call was **up**, not flat, because the factor impulse (duration relief + risk-on) was enough to lift XLRE into positive territory even while it lagged.

**The correct S4 read:** the same-day tape signal was ~0 (absent from board, unconfirmed premarket), and the multi-horizon lag belonged in the *relative/horizon* columns, not in S4. Had S4 been 0, leading sum = +2, and the engine would likely have produced a mild-up or flat-with-up-skew call — closer to reality.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count check:** The packet was disciplined here. It scored NFP/yields once in S0 (mixed net) and took only the residual spine in S1. It explicitly refused to restack the real-yield LEVEL as a rising impulse (09-25 rule). It refused to double-count the stale 1w/1m lag into both S2 and S4 — but it *did* put it into S4, which is the one place it shouldn't have gone. **Net: one double-count error, in S4.**

**Knowable-at-open test:** Was the actual outcome knowable at 08:49 ET?

- **The duration impulse:** YES — NFP was printed, the live curve was verified (10Y 5.18%, 30Y 5.57%), and the packet had it.
- **The risk-on/growth-led tape:** YES — ES +0.50% / NQ +0.68% were on the board, and the packet explicitly noted "equity risk-on often leaves REITs lagging."
- **The fade-from-open:** PARTIALLY — the packet knew the 30Y was still in the stress zone and that a 3–6bp tick is not full relief, which is exactly the condition that produces a gap-and-fade. It flagged this but did not let it move the sign.
- **The absolute up move:** PARTIALLY — the packet had all the ingredients (duration relief + risk-on + no negative catalyst) but the S4 = −1 import overrode them.

**Verdict: KNOWABLE_AT_OPEN = partially.** The direction (up) was inferable from the factor set; the packet had the right factors and the right mechanism but let a stale multi-horizon lag signal set the sign of the same-day tape component.

---

## 4. Outliers inside the sector

- **XLRE itself is the outlier** — it gapped up on a duration impulse and then faded, closing +0.295% while SPY closed +0.742%. Within a green tape with 8 of 11 sectors up, XLRE was a *relative* loser despite being an *absolute* gainer. This is the "risk-on leaves REITs lagging" pattern in its purest form.
- **Fellow duration XLU was red premarket (−0.09%)** — the packet correctly noted this was *not* a 09-14-style REIT bid. Reality confirmed: the duration bid was selective and weak, not a broad defensive/duration rotation.
- **Growth (XLK +0.78% premarket, Nasdaq +1.2% close)** was the day's winner — the marginal dollar went to AI/growth, not to levered duration. This is the rotation-out-of-REITs HIT that fired.
- **No single-name outlier drove XLRE** — the packet's ban on letting WELL/EQIX/PLD define the call was correct and no such name appears to have been the driver.

---

## 5. Scorecard and lessons

| Component | Morning | Reality | Verdict |
|---|---|---|---|
| S0_SHARED_MACRO | 0 | Mixed (duration relief capped, risk-on elsewhere) | **HIT** |
| S1_SECTOR_FACTORS | +1 | Duration relief fired but weak | **PARTIAL** (right mechanism, over-weighted) |
| S2_BREADTH | 0 | No breadth driver | **HIT** |
| S3_FLOWS | 0 | No flow driver | **HIT** |
| S4_ETF_TAPE | −1 | Same-day tape ~0; lag was multi-horizon | **MISS** |
| Direction | flat | up (+0.295%) | **MISS** |
| Magnitude | flat | flat | **HIT** |
| Relative | lagging | lagging (−0.447%) | **HIT** |

**Rolling impact:** dir 0.6 → ~0.55 (n=11); mag 0.3 → ~0.36 (n=11). The magnitude band continues to outperform direction, which is the persistent pattern in this log.

**Lessons to carry forward:**

1. **S4 must be a same-day tape read, not a multi-horizon lag read.** The 09-11 rule ("do not dump stale 1w/1m lag into S2") needs a sibling: **do not dump stale 1w/1m lag into S4 either.** Multi-horizon lag belongs in the HORIZON columns and in the relative-skew note, not in the same-day tape component. This single error flipped the call.

2. **When the divergence flag fires (leading +1 vs S4 −1), the resolution should not default to "flat."** The packet resolved the conflict as "factors say not down, tape says lag → absolute flat, relative lagging." The correct resolution when factors are net-positive and the tape signal is *stale* (not live-negative) is **mild up with relative lag** — which is exactly what happened. The divergence flag should trigger a *re-examination of the stale component*, not a split-the-difference to flat.

3. **The "risk-on leaves REITs lagging" MACRO MAP line is a same-day sign-setter, not just a relative note.** The packet wrote it and then didn't let it lift the absolute call. When ES/NQ are outside ±0.5% to the upside and the sector is a non-growth duration basket, the base case is **mild up / relative lag**, not flat.

4. **The stress-zone cap worked as intended.** The 30Y at 5.57 correctly prevented the packet from calling a duration-up *rally*. The cap was right; the sign was wrong. Keep the cap, fix the sign.

5. **The 10-01 lesson was applied correctly and should be kept.** Not importing XLP's "LEVEL + dovish front-end ⇒ flat" as a REIT identity, and not treating a fade that leaves 30Y ~5.6 as relief — both held up. The S0 = mixed 0 was the right call and the best part of the morning.

---

OUTCOME_BEGIN
SECTOR: Real Estate
ETF: XLRE
ETF_PCT: 0.295
SPY_PCT: 0.742
REL_PCT: -0.447
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: flat
PRIMARY_DRIVER: Post-NFP duration relief (10Y 5.18%, −6bp) lifted XLRE at the open, but the 30Y still at 5.57% capped the bid and growth-led risk-on (Nasdaq +1.2%) pulled the marginal dollar away — XLRE gapped to $40.96 and faded to $40.80, closing +0.295% while lagging SPY by 0.45%.
KEY_INTERACTION: Duration relief (S0/S1 positive) vs risk-on rotation into growth (relative drag) — the two impulses were both real and partially offsetting, producing a mild absolute gain with a relative loss; the packet saw both but let a stale multi-horizon lag signal (S4 = −1) set the sign.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Right mechanism, right magnitude band, right relative call — wrong absolute sign, because multi-horizon relative lag was imported into S4 (same-day tape) instead of being confined to the horizon columns; S0 = mixed 0 was the morning's best call.
OUTCOME_END