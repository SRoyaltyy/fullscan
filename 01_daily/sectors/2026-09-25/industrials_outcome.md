# Sector Outcome — Industrials — 2026-09-25

Actuals: {'etf': 'XLI', 'pct': 0.9476934356297928, 'spy_pct': 0.5435468794763754, 'rel': 0.40414655615341744, 'open': 169.52999877929688, 'close': 170.42999267578125, 'source': 'yf_download'}

# Sector Post-Session Review — Industrials (XLI) — 2026-09-25

## 0. FACTS

**CLAIM:** XLI closed up on 2026-09-25, outperforming SPY.
**URL:** https://stockanalysis.com/etf/xli/ (and stockscan.io price history)
**PUBLISHED:** 2026-09-25 (post-close)
**QUOTE:** "the latest closing stock price as of September 25, 2026, is $170.28" (stockscan); stockanalysis shows XLI ~$170.32.
**SUMMARY:** Confirms the deterministic actuals — XLI closed ~$170.43 (injected), up ~+0.95% on the day, vs SPY +0.54%, for a relative outperformance of ~+0.40%.

| Metric | Value |
|---|---|
| XLI % | **+0.9477%** |
| SPY % | **+0.5435%** |
| Relative % | **+0.4041%** |
| Open | 169.53 |
| Close | 170.43 |
| Path | Opened ~169.53 (already above the −0.38% PM quote implied level), trended up through the session to close near the high |

**ACTUAL_DIRECTION: up. ACTUAL_MAGNITUDE: mild (just under +1%).**

The morning call was **down / mild**. That is a **direction MISS** and a **magnitude MISS** (mild was the right band, wrong sign). The relative call was also wrong: the morning leaned negative on relative (PM:XLI worst cyclical, 1m rel −6.16%), and XLI instead **beat** SPY by ~40 bp.

---

## 1. What actually drove the sector

**PRIMARY DRIVER: the durable-goods spine print beat on the internals, and the AI-power/grid de-rate that the morning treated as a live negative was the exact thing that reversed.**

**CLAIM:** August durable goods came in flat on the headline but strong underneath, with capex notably firm.
**URL:** https://kpmg.com/us/en/articles/2026/august-2026-durable-goods.html
**PUBLISHED:** 2026-09-25
**QUOTE:** "August durable goods orders held steady after adding a revised 0.9% in July. Away from the headline figure, which was pulled down by lower aircraft orders, the underlying data showed strength. Excluding transportation, durable goods orders rose 0.3% while a measure of capex jumped 1.6%."
**SUMMARY:** The 8:30 ET spine print — the one item the morning explicitly left **unscored** — landed with a **+1.6% core capex** jump. That is the single most XLI-relevant line in the release: core capex is the sector's demand proxy, and it accelerated.

**CLAIM:** The headline was flat, dragged by transportation/aircraft.
**URL:** https://tedmag.com/august-durable-goods-orders-hold-steady-beating-forecasts/
**PUBLISHED:** 2026-09-25
**QUOTE:** "New orders for U.S. manufactured durable goods were virtually unchanged in August, edging down by just $0.1 billion to $338.6 billion... The flat reading followed two consecutive monthly increases."
**SUMMARY:** Headline "unchanged" but the release **beat forecasts** and the composition was capex-positive. The market read the internals, not the headline.

**CLAIM:** Industrials have decoupled from the AI trade — and the decoupling is now working *in XLI's favor*.
**URL:** https://www.cnbc.com/2026/09/25/industrials-have-decoupled-from-rest-of-ai-trade-heres-why.html
**PUBLISHED:** 2026-09-25
**QUOTE:** "Industrials began 2026 trading in tandem with chipmakers — the posterchildren of the artificial intelligence trade. That's now far from the case. The S&P 500 industrials sector ETF (XLI) is up..."
**SUMMARY:** This is the taxonomy-level story of the day. The morning thesis was that the AI-power/grid sleeve (VRT −15.4% w1, breadth 0.109) was the sector's structural bull case *unwinding* and therefore an ETF-level negative. The session's actual behavior says the opposite: XLI **decoupled upward** from the AI complex. The de-rate in VRT/PWR/FIX was a **sleeve-specific unwind**, not an ETF-level drag — and with the AI complex no longer leading, XLI's non-AI cyclical core (defense, machinery, transport-adjacent, HVAC) carried the tape.

**Secondary drivers:**
- **Rates did not bite.** The morning's S0 was built on a "live duration shock" (10Y >5.2%, DFII10 +0.13 1d, 10Y–SPX corr −0.958). XLI is a capex-heavy cyclical that *should* be rate-sensitive — but it rose anyway. Either the rate move stabilized intraday, or the durable-goods capex beat simply dominated the discount-rate channel. Either way, the rate-shock transmission the morning treated as *the* same-session driver did not show up in XLI's price.
- **Green index tape was a tailwind, not a veto.** ES was +0.20–0.28% pre-open; SPY closed +0.54%. The morning explicitly refused to let a green index lift XLI ("an index rebound is not an XLI participation certificate"). In fact XLI **participated and then some** — it beat the index.

---

## 2. Audit of morning S0–S4 reads against reality

Using the **morning numbers as written**, not post-close rewrites:

| Score | Morning value | Morning rationale | Reality | Verdict |
|---|---|---|---|---|
| **S0** Shared macro | **−1** | Live duration shock (10Y >5.2%, real yields +0.13 1d, corr −0.958); XLI worst cyclical on PM board | XLI rose ~+0.95% *despite* the rate backdrop; the rate channel did not transmit to XLI's price | **WRONG SIGN** |
| **S1** Sector factors | **−1** | ISM decelerating (new orders −3.0); MAP HEAT down 8/10 sub-industries; AI-power sleeve de-rating; durable goods **unscored** | Durable goods **beat on internals** (core capex +1.6%); the de-rating sleeve did not drag the ETF | **WRONG SIGN** |
| **S2** Breadth | **−1** | 8/10 sub-industries down; only green sleeve is low-beta Consulting; sector absent from green index tape | Sector **participated in and beat** the green tape; breadth evidently repaired intraday | **WRONG SIGN** |
| **S3** Flows/positioning | **−1** | Multi-horizon laggard (1m rel −6.16%); AI-power crowded-long unwind; no inflow evidence | The crowded-long unwind in VRT/PWR/FIX was **sleeve-specific**; XLI itself was accumulated relative to SPY | **WRONG SIGN** |
| **S4** ETF tape | **−1** | Negative 1d/3d rel; worse-than-index PM gap (−0.38% vs green ES) | PM gap **died at the cash open** — XLI opened at 169.53 and never looked back | **WRONG SIGN** |

**Every one of the five scores was negative, and every one was wrong in sign.** This is not a "one leg misfired" session — it is a **unanimous five-for-five directional miss**.

**The critical audit finding:** the morning's own **self-audit** flagged the exact risk and then dismissed it. It wrote: *"Not flat: the full 09-14 stack (live duration shock + worse-than-index PM gap) is present, and the tape confirms."* But the 09-14 stack requires **both** legs, and the morning **itself** noted the durable-goods print was **unscored and two-sided**. It then scored S1 = −1 anyway, treating an *unprinted* spine print as if it were already a miss. That is the mechanical error: **an unscored binary was scored as a negative.**

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count check (morning's own claims):**
- Oil counted once in S0 ✓ (correctly, as a cost level)
- 1m lag scored once in S3, not re-scored in S4 ✓ (correctly)
- Rate shock counted once in S0, not re-scored in S1 ✓ (correctly)

The morning's double-count hygiene was **clean**. The problem was not double-counting — it was **single-counting the wrong things and zero-counting the right one.**

**The knowable-at-open test — this is where the session is decided:**

What was **knowable at the open** that the morning under-weighted or mis-signed?

1. **The durable-goods print was scheduled for 8:30 ET — before the cash open.** The morning treated it as "live and unscored" and then let S1 = −1 stand on the *pre-print* MAP HEAT. But by the cash open, the print **had occurred**. The morning's own framework says the spine print "could confirm or falsify the deceleration." It **falsified** it (core capex +1.6%). A disciplined open-time re-read would have caught this: **the spine print was knowable at the open and it was positive.**

2. **The PM:XLI −0.38% gap was the *only* fresh negative, and the morning knew this.** It even wrote the distinction explicitly: *"This is the key distinction from 09-22 (where the PM gap was the only negative and died at the open)."* The morning then **failed to apply its own distinction** — because it had manufactured four *additional* negatives (S0/S1/S2/S3) to sit alongside the PM gap, it no longer looked like "the PM gap is the only negative." But three of those four (S1, S2, S3) were **derived from the same pre-print MAP HEAT and the same multi-horizon lag** — i.e., they were **not independent fresh negatives**, they were restatements of the stale condition. Strip the restatements and the fresh same-session negative set was: **PM gap (−0.38%) + negative 1d rel (−0.66%)**. That is a **09-22-shaped card**, not a 09-14-shaped card.

3. **The 09-24 governing lesson was misapplied.** The morning invoked 09-24 (A) — "when |ES|/|NQ| are outside the ±0.5% band, derive absolute direction from the ES/NQ sign and do not let PM:XLI ≈ 0 veto S0." But on 09-25, **ES was +0.20% Finviz / +0.28% sleeve — INSIDE the ±0.5% band.** The morning even admitted this: *"the magnitude is inside the 09-22 mixed ±0.5% band on ES."* So the 09-24 (A) clause **did not fire** — and the morning knew it, yet still emitted a directional down call. The correct governing lesson was **09-22 (A)**: *"mixed T+1 with |ES|/|NQ| inside ±0.5% is NOT a 09-21 tape; do not mint down/mild from MAP HEAT + 1m lag + a PM quote."* That is **exactly** what the morning did.

**KEY INTERACTION:** The morning built a five-leg negative card where **four legs (S1/S2/S3/S4) were restatements of the same stale multi-horizon lag + pre-print MAP HEAT**, and the one genuinely fresh, two-sided, sector-spine binary (durable goods) was left unscored — then the print landed positive and the whole card inverted.

**KNOWABLE_AT_OPEN: partially.** The durable-goods print was released at 8:30 ET, **before** the 9:30 cash open. A disciplined open-time re-read would have seen core capex +1.6% and **flattened or flipped** the call. The morning's failure was not lack of information — it was **refusing to re-score S1 after the spine print landed**, having pre-committed to "do not pre-score a beat or a miss" and then effectively pre-scoring a miss.

---

## 4. Outliers inside the sector

- **The AI-power/grid sleeve (VRT −15.4% w1, PWR −4.4%, FIX post-earnings fade) was the morning's headline negative — and it was the session's biggest *non*-event for the ETF.** This is the 08-18 clause ("GEV/FIX/VRT must not raise or sink the ETF") firing in the direction the morning ignored: the sleeve **did not sink the ETF**. The morning used the sleeve as an ETF-level driver; the session proved it was a sleeve-level event.
- **CAT −4.2% and HON (GE downgrade read-through)** were MAP HEAT captains the morning correctly labeled as captains — but it then let them inform S2 breadth as if they were ETF-level. They were not.
- **The CNBC "decoupling" piece** is the outlier that explains the whole session: XLI's non-AI cyclical core (defense, machinery, HVAC, transport-adjacent) carried the tape while the AI-adjacent sleeve bled. The morning's breadth read (8/10 down) was a **pre-open snapshot of a sleeve that was about to stop mattering to the ETF.**
- **Consulting Services (the only green sleeve, breadth 0.727)** — the morning dismissed it as "low-beta, non-cyclical, not the ETF's cyclical core." In the event, the *non-AI* character of the green sleeves was the tell, not a disqualifier.

---

## 5. Verdict and lesson

**MORNING_READ_VERDICT:** A unanimous five-for-five negative card built on a stale multi-horizon lag and a pre-print MAP HEAT snapshot, with the one fresh sector-spine binary (durable goods) left unscored and then landing positive — a textbook 09-22-shaped card that the morning itself had the lesson to avoid and did not apply.

**The single most important correction:** When a sector's own spine print is scheduled **before the cash open**, the morning score must be treated as **provisional** and re-read at the open. Scoring S1 = −1 on pre-print MAP HEAT while the spine print is pending is **not** "leaving it unscored" — it is **scoring the miss by default**. The 09-25 session is the clean proof: the print beat, and every downstream score (S2 breadth, S3 flows, S4 tape) that was derived from the same pre-print snapshot inverted with it.

**Secondary correction:** The 09-24 (A) clause requires |ES|/|NQ| **outside** ±0.5%. On 09-25 ES was **inside** the band. The morning cited 09-24 (A) as governing while admitting the band condition was unmet. The correct governing lesson was 09-22 (A) — and it said, in advance, exactly what went wrong.

**What would have been right:** flat/flat, or a low-confidence up/mild lean on the capex internals — with the relative call **dropped** (the 1m lag was a condition, not a same-session relative signal, and the PM gap was the only fresh negative).

---

OUTCOME_BEGIN
SECTOR: Industrials
ETF: XLI
ETF_PCT: 0.9477
SPY_PCT: 0.5435
REL_PCT: 0.4041
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: August durable goods beat on internals (core capex +1.6%) at 8:30 ET, and XLI decoupled upward from the de-rating AI-power/grid sleeve that the morning treated as an ETF-level negative
KEY_INTERACTION: Four of five negative legs (S1/S2/S3/S4) were restatements of the same stale multi-horizon lag + pre-print MAP HEAT, while the one fresh sector-spine binary (durable goods) was left unscored and landed positive — inverting the whole card
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Unanimous five-for-five negative card built on a stale lag and a pre-print snapshot, with the spine print knowable at the open and positive — a 09-22-shaped card the morning had the lesson to avoid and did not apply
OUTCOME_END