# Sector Outcome — Basic Materials — 2026-09-23

Actuals: {'etf': 'XLB', 'pct': 1.1466499805313113, 'spy_pct': -0.7202161019229769, 'rel': 1.8668660824542882, 'open': 50.25, 'close': 50.279998779296875, 'source': 'yf_download'}

# Sector Post-Session Review — Basic Materials / XLB — 2026-09-23

## 0. FACTS

**Channel 1 / deterministic actuals (injected):**

| Item | Value |
|---|---|
| XLB % change | **+1.1466%** |
| SPY % change | **−0.7202%** |
| XLB relative vs SPY | **+1.8669%** |
| XLB open | 50.25 |
| XLB close | 50.28 |

**Path:** Open 50.25 → close 50.28. The entire session's gain was **gapped in at the open** — XLB opened at 50.25 against a 2026-09-22 close of ~50.53 (TradeSmith print in the morning appendix), i.e. the ETF actually opened *below* Tuesday's close and then closed essentially flat to its open (+0.06% open-to-close). This is a critical fact for the audit: **the +1.15% is a gap-and-hold, not an intraday trend day.** The morning's PM:XLB **+0.02%** read was therefore not "non-information" — it was the *entire* signal, and it was directionally correct while being magnitude-blind.

**Cross-check on the tape:** SPY **−0.72%** on the day. So the "dead-flat index" premise in the morning card (ES +0.03% / NQ −0.10%) was **wrong in realization** — the broad tape sold off ~0.7% and materials rose ~1.15%. That is a **+1.87% relative day**, the largest positive relative print for XLB in this memory window, against a 1m rel hole of −6.41% as of 09-21.

**Direction:** up. **Magnitude:** notable (1.15% absolute is ~1.5–2× the last-10 mag average of 0.2; 1.87% relative is severe by any sector standard).

---

## 1. What drove the sector

### 1a. The copper spine — the thing the morning card correctly identified and then under-paid

The morning card's S1 = +1 was built on: copper $6.489 (+0.66%), aluminum +1.10%, LME 3M ~$14,745–14,766 within ~1% of the $14,875 Sep-10 record, sixth straight up day, cash-3M backwardation ~$62/t, cancelled warrants 122,150 t = 48% of on-warrant, SHFE stocks −70% since early June, Shanghai cathode 43,900 t (lowest since 2023), Yangshan premium near 4-year highs.

**What actually happened intraday (London):**

> CLAIM: LME 3M copper touched **$14,833/t** — highest since the $14,875 record of Sep-10 — before closing down ~1% at **$14,606.50/t** on profit-taking and a firmer dollar.
> URL: https://www.kitco.com/news/off-the-wire/2026-09-23/copper-falls-near-record-highs-profit-taking-and-firmer-dollar
> PUBLISHED: 2026-09-23 (1600 GMT)
> QUOTE: "Benchmark three-month copper CMCU3 on the London Metal Exchange (LME) was down 1% at $14,606.50 a ton by 1600 GMT after touching $14,833 for its highest since the record high of $14,875 on September 10. Comex copper HGc1 hit a record peak…"
> SUMMARY: The London metal **made a new near-record high intraday and then faded**. The US equity session, however, closed *after* the London fade — and XLB still finished +1.15%. This is the single most important interaction of the day (see §3).

> CLAIM: As of 2026-09-23, COMEX copper ~$6.70/lb; LME aluminum ~$3,264/t; zinc ~$3,938/t.
> URL: https://metalcharts.org/lme
> PUBLISHED: 2026-09-23
> SUMMARY: The complex was elevated across the board, not copper-only. Aluminum strength (the morning card's +1.10%) persisted into the session.

> CLAIM: Materials and industrials led a TSX rally, with copper and gold stocks dominating the leaderboard.
> URL: https://in.investing.com/news/stock-market-news/materials-lead-tsx-rally-as-metals-infrastructure-stocks-surge-93CH-5601761
> PUBLISHED: 2026-09-22/23
> SUMMARY: The materials bid was **global and cross-listed**, not a US-only ETF quirk. This matters for the "was it knowable at open" test — the Canadian materials complex was already leading into the US session.

### 1b. The gold/silver sleeve — the morning card's "book bid OFF" call was the second miss

The morning card wrote: *"Monetary metals — 8/14 sleeve ON, book bid OFF. Finviz gold +0.90%, silver +1.96%. GC=F −0.57% 1d and NEM premarket ~−1.8% mean the cash miner is not confirming."*

That was a **premarket read of NEM at −1.8%** used to discount a live +0.90%/+1.96% cash metals print. The card chose the *fading* leg (GC=F 1d, NEM PM) over the *live* leg (spot gold/silver up hard). Given the outcome — XLB +1.15% with NEM ~8% of the book — the live leg won. The morning card's own rule **"do not let gold cancel China"** was applied in the correct direction (gold did not cancel the copper spine), but the card then *also* refused to let gold **add** to the spine. It double-discounted: once by not paying it, once by treating NEM PM as confirmation of absence.

### 1c. What did NOT drive it

- **China demand:** NBS mfg 49.8, construction 46.9, property FAI ~−19.9% YoY — all still contraction. Hang Seng −0.83% / Shanghai −0.34% in the morning. China was **not** the driver; the pre-holiday physical restock (Mid-Autumn / National Day) was the *tightness* mechanism, exactly as the card said.
- **USD:** DXY +0.38% 1d / +2.04% 1m — firm. A firm dollar is normally a headwind for metals, and indeed London copper faded *on* the firmer dollar. The US equity session ignored it.
- **Oil:** WTI −1.59%, CL=F −4.97% 1d — feedstock relief for the chemicals majority sleeve (LIN ~12–13%, SHW/ECL in the 40–50% chemicals/process book). This is a **supporting** factor, not the driver, but it is the reason the chemicals-heavy book did not fight the metals sleeve.
- **Tariffs:** Section 232 refined-copper still stalled — uncertainty, not support. Correctly scored OFF.

### 1d. Driver taxonomy

**PRIMARY_DRIVER:** Industrial-metals tightness/continuation (copper near-record with cancelled-warrant and SHFE draw tightness) transmitted into a chemicals-heavy XLB book on a day when the broad tape sold off — i.e. a **relative-safe-haven / real-asset rotation** into materials while SPY fell 0.72%.

The **secondary** driver is the one the morning card explicitly refused to pay: **gold/silver sleeve strength** (spot +0.90%/+1.96%) contributing through NEM and the precious-metals complex.

---

## 2. Audit of morning S0–S4 against reality

I am auditing the **morning numbers as written**, not rewriting them post-close.

### S0_SHARED_MACRO = 0 → **should have been +1**

The card's reasoning for 0: *"Not +1: four-index off, ES/NQ flat, Europe red, China equities red, leftover AI is a funding rotation away from this cyclical. Not −1: oil offered, USD not spiking, no kinetic increment, no same-morning China miss, futures not red, VIX calm."*

**Verdict: MISS in the +1 direction, but a defensible one.** The card was right that there was no *macro thrust* — and indeed SPY fell 0.72%, so "risk-on tape" was correctly OFF. But the card treated "no macro thrust" as "S0 = 0," when the actual setup was **materials as a relative destination on a down-tape day**. The card's own HIT_GRID had "Risk-off tape / flight to safety" OFF at 0.70 confidence — that was the wrong call. SPY −0.72% with VIX 14.21 is not a flight to safety, but it *is* a tape where real-asset/commodity exposure outperforms. The card had the ingredients (firm dollar, oil offered, metals tight) but assembled them as "neutral" rather than "materials-favorable relative."

**This is the structural error of the session:** the card optimized for *absolute* direction on a dead index and never priced the *relative* setup that the 1m rel hole (−6.41%) plus a live tight copper spine actually created.

### S1_SECTOR_FACTORS = +1 → **directionally correct, magnitude badly under-paid**

The card wrote: *"S1 = +1. Net of spine tightness/continuation versus carried China/property + tariff stall + incomplete iron/steel + 8/25 composition. Not +2/+3: chemicals are the book, 8/17 caps severe on copper into flat futures, nested FCX/NEM PM is soft."*

**Verdict: right sign, wrong size.** The card identified the exact mechanism that paid — copper tightness/continuation — and then capped it at +1 because (a) chemicals are the book and (b) nested FCX/NEM PM was soft. Both caps were wrong:
- Chemicals did **not** fight the metals bid; oil-offered feedstock relief plus a broad materials bid meant the chemicals sleeve was neutral-to-supportive, not a drag.
- FCX/NEM PM softness was a **premarket artifact** that reversed. The card used premarket single-name prints to cap a live physical-tightness signal. That is the same error class as 09-22 (using stale HEAT to sign a live spine), just in the opposite direction.

The card's own DO-INSTEAD rule — *"when factor sign fights leftover tape/HEAT, cut conviction; prefer flat/mild — do not flip back to down"* — was followed correctly (it did not flip to down), but the rule's "cut conviction" instruction was applied to a **live green spine**, which is precisely the case where the rule should *not* bind. The rule was written for "factor sign fights leftover tape," and here the factor sign **agreed** with the live spine and only fought the *leftover* tape. The card conflated the two.

### S2_BREADTH = 0 → **should have been +1**

The card wrote: *"S2 = 0. Not −1 (stale HEAT after a transmitted bounce). Not +1 (no same-morning expansion; nested miners fading)."*

**Verdict: MISS.** The card explicitly refused to pay Tuesday's FCX +3.03% / LIN +1.62% / XLB +1.65% as T-1, which is correct discipline. But it then had **no live breadth read at all** — PM:XLB +0.02% was the only same-morning number, and the card dismissed it as "non-information (09-18: PM ∈ (0,1%) with nested names not holding is not a bid)." The outcome shows PM +0.02% **was** the bid, and the nested names **did** hold. The card's 09-18 rule ("PM ∈ (0,1%) with nested names not holding is not a bid") was applied with the *second clause unverified* — the card asserted "nested names not holding" from premarket FCX ~−1% / NEM ~−1.8%, which did not hold into the session.

### S3_FLOWS_POSITIONING = 0 → **correct**

5d ~−$53M, 1m ~−$238M, week of Sep-11 ~$167M outflow. 1m rel −6.41% is washout, not crowding. No same-morning volume spike. **Verdict: HIT.** Flows were not the driver and the card did not manufacture a flow signal. The skill multiplier on S3 (1.25) was correctly applied to a genuine zero.

### S4_ETF_TAPE = 0 → **correct in construction, but the card's own divergence flag was the tell**

The card wrote: *"S4 = 0. Channel 1 through 09-21 only: 1d rel −1.65%. 8/27 / 09-04 / 8/28: S4 confirms this session, not Monday's hole and not Tuesday's already-printed +1.65%. PM +0.02% is non-information."*

**Verdict: HIT on the discipline, MISS on the read.** The card correctly refused to copy Monday's −1.65% rel hole into S4. But it also refused to let PM +0.02% count as *any* confirmation, which left S4 structurally unable to confirm anything except a large PM move. On a gap-and-hold day, PM is the only pre-open tape you have, and +0.02% on a −6.41% 1m rel hole is a **stabilization signal**, not noise.

### Divergence flag

The card flagged divergence (leading S0–S3 = +1 vs S4 = 0 and leftover rel deeply negative) and wrote: *"Trust the live spine over leftover tape (09-22)."* **This was the correct instinct and the card should have acted on it more aggressively.** The divergence flag was the session's most valuable output — it said "the live spine disagrees with the leftover tape" — and the card then resolved the disagreement by *splitting the difference* (flat) rather than by *trusting the live spine* (mild up). The card's own stated resolution rule pointed to up/mild; the card's output was flat.

---

## 3. Interactions / double-count / knowable-at-open test

### 3a. The critical interaction: London fade vs US close

This is the session's defining feature and the card could not have known it, but it explains the outcome:

- **London copper made a near-record high ($14,833) intraday and then faded to −1% ($14,606.50) by 1600 GMT** on profit-taking and a firmer dollar.
- **The US equity session closed after that fade** — and XLB still finished +1.15% while SPY fell 0.72%.

Two readings:
1. **The US session was trading the *tightness narrative*, not the *spot price*.** Cancelled warrants at 48%, SHFE −70%, backwardation — these are *stock* facts, not *flow* facts, and they do not fade with a one-day profit-take. The card's S1 was built on exactly these stock facts. The card was right about the mechanism and wrong about the day's expression.
2. **The relative bid was the real trade.** SPY −0.72% with XLB +1.15% means money rotated *into* materials on a down-tape day. That is a **relative-safe-haven / real-asset** flow, and it is not visible in any single-commodity print. The card's S0 = 0 ("no macro thrust") missed that "no macro thrust + down tape" is itself a materials-favorable setup.

### 3b. Double-count audit

The card's self-audit claimed: *"oil once (S0 offered / S1 haircut relief, not two pluses). China once (S1 offset). Tightness in S1 only, not S4."* **This was correctly executed.** No double-count is visible in the scoring. The problem was not double-counting; it was **under-counting** — the card paid the copper spine once (S1 = +1) and paid nothing for the gold/silver sleeve, nothing for the relative setup, and nothing for the PM stabilization.

### 3c. Knowable-at-open test

**Was the outcome knowable at the open?** **Partially — and the card had the pieces.**

Knowable at open:
- Copper tightness/continuation (cancelled warrants 48%, SHFE −70%, backwardation $62/t) — **fully knowable**, and the card knew it.
- Gold +0.90% / silver +1.96% spot — **fully knowable**, and the card *chose* to discount it via GC=F 1d and NEM PM.
- PM:XLB +0.02% — **fully knowable**, and the card *chose* to call it non-information.
- TSX materials leading with copper/gold stocks dominating — **knowable** (the Investing.com piece is dated 09-22/23), and the card did not cite it.
- DXY firm, oil offered — **knowable**, correctly scored.

Not knowable at open:
- That SPY would fall 0.72% (the card's ES +0.03% / NQ −0.10% read was a *flat* index, not a down index).
- That London copper would fade from $14,833 to $14,606.50 while US materials equities held.
- The exact magnitude (+1.15%).

**Verdict: PARTIALLY knowable.** The *direction* was knowable — the card's own divergence flag said so. The *magnitude* was not. A mild-up call was available at the open; a notable-up call was not.

### 3d. The binding-rule audit

The card listed several active rules. Checking each against the outcome:

- **09-22 Cu-tightness vs flat-index is BINDING** — *"|ES|,|NQ| < 0.5% and copper continuation/cancelled-warrant tightness must not be signed down; pay the spine in S1; cap conviction not direction."* **The card followed this correctly** (S1 = +1, not down). But note the rule says "cap conviction not direction" — the card capped conviction (mult 0.85, conf 0.42) *and* effectively capped direction (flat). The rule's intent was to prevent a *down* call; it did not authorize a *flat* call when the spine was green. **The card over-applied the rule.**
- **09-18 HEAT-down→down/mild OFF** — correctly not used as a down trigger. **HIT.**
- **09-21 RS-veto triad OFF** — correctly not used to create a down overlay. **HIT.** (Note: the pipeline JSON shows `sector_rs_veto_applied: True` with `sector_rs_tape: {d1: -2.08, w1: -4.78}` — the veto was applied by the engine, which is consistent with the card's refusal to sign down.)
- **09-17 residual-mild-up OFF** — the card did not use it. **But this is the rule that should have been reconsidered.** It requires ES/NQ ≥ +0.5% and a held green PM. ES/NQ were flat (not ≥ +0.5%), so the rule was correctly OFF *as written*. However, the rule's *condition* was designed for a risk-on tape; on a down tape with a green PM and a green spine, the analogous "residual mild up" setup existed in a different form. **The rule set had no branch for "down tape + green sector spine."**
- **8/14 gold sleeve ON** — the card acknowledged it but refused to pay it. **This was the second miss.** The rule said the sleeve was ON; the card treated it as ON-but-not-paying.
- **8/25 / 8/27 confirmed-up ban** — the card honored it (no confirmed-up call). **HIT on discipline, but this ban is now suspect** given the outcome. A "confirmed-up ban" that fires on a day the sector rose 1.15% relative is a rule that needs re-examination.
- **8/17 commodity-vs-flat-futures: cap severe, not direction** — correctly applied as a cap. **HIT.**
- **09-04 / 8/28: do not copy Monday's −1.65% rel into S4** — correctly applied. **HIT.**
- **DO-INSTEAD (last three BM losses): cut conviction; prefer flat/mild; do not flip back to down** — the card followed this *literally* (flat) but the rule's own logic ("when factor sign fights leftover tape/HEAT") did not apply, because the factor sign **agreed** with the live spine. **The rule was mis-triggered.**

---

## 4. Outliers inside the sector

The morning card gave the composition: LIN ~12–13%, NEM ~8%, FCX ~10–15% copper miners, SHW/ECL in the 40–50% chemicals/process book.

**Outliers implied by the outcome:**

1. **Copper miners (FCX and peers) — likely the largest positive contributors.** FCX closed 09-22 at $74.35 (+3.03%). With copper making a near-record intraday high on 09-23 before fading, FCX likely gave back some of the London fade but held a strong session. The morning card's "FCX premarket ~−1%" was a premarket artifact that reversed — **this is the single-name outlier that most directly contradicts the card's S2 = 0.**

2. **NEM (gold) — likely a positive contributor, not the −1.8% premarket drag the card assumed.** Spot gold +0.90% / silver +1.96% with the 8/14 sleeve ON. NEM at ~8% of XLB means a 2–3% NEM move is worth ~0.2% on the ETF — meaningful but not the driver. The card's decision to treat NEM PM −1.8% as confirmation of absence was the outlier error.

3. **LIN (chemicals, ~12–13%) — likely neutral-to-slightly-positive.** LIN printed +1.62% on 09-22. Oil-offered feedstock relief supports the chemicals book. The card's "LIN ~unchanged" premarket read was probably close to right, but "unchanged" on a +1.15% ETF day means LIN was a *relative drag* — which is consistent with the card's "chemicals are the book" caution, just not enough to cap the ETF.

4. **SHW / ECL (coatings/chemicals) — likely neutral.** No specific catalyst. These are the names that would have *prevented* a >2% XLB day, which is consistent with the +1.15% outcome being a metals-led, chemicals-diluted move.

**Outlier verdict:** The dispersion was **metals-led, chemicals-diluted** — exactly the composition the card described, but the card used that composition to *cap the upside* rather than to *size the expected move*. The correct inference from "copper miners ~10–15%, chemicals ~40–50%" on a day with a near-record copper print and a green PM was **mild-to-notable up**, not flat.

---

## 5. Morning read verdict

The morning card was **directionally wrong (flat vs actual up) but structurally sound in its reasoning.** It identified the correct driver (copper tightness/continuation), correctly refused to sign down, correctly flagged the divergence between the live spine and the leftover tape, and correctly avoided double-counting. It then **failed to convert its own correct divergence flag into a directional call**, capping at flat because:
- It over-applied the "cap conviction not direction" rule from 09-22.
- It discounted the gold/silver sleeve via premarket single-name prints.
- It dismissed PM:XLB +0.02% as non-information.
- It had no scoring branch for "down tape + green sector spine."

The **magnitude miss is the larger error**: a +1.87% relative day against a −6.41% 1m rel hole is a regime-relevant print, and the card's flat/flat call with confidence 0.42 gave no indication that a notable relative move was on the table.

**The single most actionable lesson:** when the card's own divergence flag says "leading S0–S3 disagrees with S4 and leftover tape," and the card's stated resolution rule is "trust the live spine over leftover tape," the output must **resolve in the direction of the live spine** — not split the difference. A divergence flag that resolves to flat is a flag that has been neutralized rather than used.

---

OUTCOME_BEGIN
SECTOR: Basic Materials
ETF: XLB
ETF_PCT: 1.1466
SPY_PCT: -0.7202
REL_PCT: 1.8669
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Industrial-metals tightness/continuation (copper near-record $14,833 intraday, cancelled warrants 48%, SHFE −70%, backwardation $62/t) transmitted into a chemicals-heavy XLB book on a down-tape day — a relative real-asset rotation while SPY fell 0.72%.
KEY_INTERACTION: London copper made a near-record high then faded to −1% by 1600 GMT on profit-taking/firmer dollar, yet US materials equities closed +1.15% — the US session traded the tightness *stock* narrative, not the spot *flow* price, and the relative bid (XLB +1.15% vs SPY −0.72%) was the real trade.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Right driver, right refusal to sign down, right divergence flag — but the card neutralized its own flag into flat instead of resolving it toward the live green spine; direction MISS (flat vs up) and magnitude MISS (flat vs notable), with the gold/silver sleeve and PM:XLB +0.02% both wrongly discounted.
OUTCOME_END