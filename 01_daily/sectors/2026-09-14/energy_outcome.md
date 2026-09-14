# Sector Outcome — Energy — 2026-09-14

Actuals: {'etf': 'XLE', 'pct': -0.9364455266613003, 'spy_pct': -0.446162221482016, 'rel': -0.4902833051792843, 'open': 65.95500183105469, 'close': 64.52999877929688, 'source': 'yf_download'}

# Sector Post-Session Review — Energy (XLE) — 2026-09-14

## 0. FACTS

**CLAIM:** XLE closed at $64.53, down 0.94% on the session, after opening at $65.955.
**URL:** https://robinhood.com/us/en/stocks/XLE/
**PUBLISHED:** 2026-09-14
**QUOTE:** "On 2026-09-14, State Street Energy Select Sector SPDR ETF(XLE) stock traded between a low of $64.18 and a high of $66.25."
**SUMMARY:** The injected deterministic actuals (OPEN 65.955, CLOSE 64.53, ETF_PCT −0.936%) are consistent with the Robinhood intraday range: XLE gapped up to ~$65.96, printed a high of $66.25, then sold off through the day to a low of $64.18 before closing at $64.53. This is a **full reversal of the premarket gap** — the +1.50% premarket print was entirely given back and then some.

**CLAIM:** SPY fell 0.45% on the day; XLE underperformed by ~0.49%.
**URL:** (injected deterministic actuals)
**PUBLISHED:** 2026-09-14
**QUOTE:** SPY_PCT: −0.4462; REL_PCT: −0.4903
**SUMMARY:** Energy was a **relative loser** on a red tape — the opposite of the morning's "only green sector" thesis. The sector did not act as a defensive/commodity hedge; it traded as a high-beta cyclical.

**CLAIM:** Crude oil rose on the day but well short of the premarket surge implied by the morning snapshot.
**URL:** https://tradingeconomics.com/commodity/crude-oil
**PUBLISHED:** 2026-09-14
**QUOTE:** "Crude Oil rose to 101.35 USD/Bbl on September 14, 2026, up 1.29% from the previous day."
**SUMMARY:** WTI settled/printed ~$101.35, **+1.29%** — versus the morning's live-verified CL=F +3.05% / Finviz WTI $102.29 (+2.44%). The barrel **faded roughly half its gain intraday**. Brent context: "Brent crude rose toward $108 a barrel, a 4-month high, after rallying over 9% last week, as Saudi Arabia shut a major crude pipeline" (tradingeconomics, 2026-09-14) — note the driver named is a **Saudi pipeline shutdown**, not Hormuz.

**Path:** Gap up (+1.50% premarket) → high $66.25 → steady fade → low $64.18 → close $64.53. A classic **gap-and-fade / distribution day** for the sector.

---

## 1. What actually drove the sector

The morning thesis was "sector_shock oil-surge session inside a broad risk-off tape, XLE the only green sector." Reality delivered a **partial version of the oil story and a full version of the risk-off story**, and the risk-off tape won.

Taxonomy-aligned drivers, in order of realized weight:

1. **Crude surge — HIT but decaying.** Oil was genuinely up (+1.29% WTI), and the Saudi pipeline-shutdown headline (tradingeconomics) is a real supply-risk catalyst. But the morning's +2.44–3.05% print **halved** by the close. Energy equities price the *forward* barrel; a fading spot bid into a red tape is a sell signal for the equity sleeve, not a buy signal. This is the single most important fact of the session: **the sector's own object was green and the sector still closed red.**

2. **Risk-off tape / flight to safety — HIT, and it dominated.** ES −0.66%, NQ −1.59% premarket; SPY −0.45% close. VIX 17.67 with VIX/VIX3M **1.135 backwardation** was flagged in the morning as "a live vol bid." Backwardation in the vol term structure is a *de-risking* regime signal, and in that regime **high-beta cyclicals get sold regardless of their commodity**. XLE is a high-beta cyclical first and a commodity proxy second.

3. **USD strengthening — HIT.** DXY +0.45%, USD 99.245 +0.41%. A firm dollar is a direct headwind to the dollar-denominated barrel and to the sector's relative appeal.

4. **Real yields rising — HIT.** DFII10 2.55 (+0.09 1d), DGS10 4.95 (+0.12). A duration/multiple headwind that the morning correctly labeled "secondary vs oil" — but when oil fades, secondary becomes primary.

5. **Crowded long / positioning — the morning scored this MISS; it should have been a HIT.** 1m rel **+7.80%** is exactly the "leftover leadership" the morning itself described. The 09-10 lesson (record-close sequence + 1m rel ≥ +8% = live crowded-long) was *nearly* triggered at +7.80%, and the morning explicitly chose not to fire it because 1w rel was only +1.95%. That judgment call is the crux of the miss — see §2.

6. **Crack spreads / refining margins — HIT but refiner-sleeve only.** Products were bid with crude premarket (HO +2.67%, RBOB +2.73%). The morning correctly ring-fenced this to the refiner sleeve and refused to let VLO/MPC drive XLE. That discipline was right and did not save the call, because the *integrated* majors (XOM/CVX, the bulk of XLE) traded on the fading barrel and the risk-off tape.

7. **Inventory (EIA WPSR 10:30 ET) — UNRESOLVED, and likely a fade accelerant.** The morning flagged this as "the only same-session energy print — two-sided." A 10:30 ET print landing mid-morning on a gap-up day is a natural distribution trigger if it disappoints; the intraday path (high $66.25 early, then fade) is consistent with a post-10:30 roll-over. We cannot confirm the print from available evidence, so this stays UNRESOLVED — but the *timing* of the fade is suggestive.

**Primary driver:** Risk-off de-risking (VIX backwardation + red index tape) overwhelmed a **fading** oil bid; XLE traded as a high-beta cyclical, not as a commodity hedge.

---

## 2. Audit of morning S0–S4 reads against reality

The morning scored **S0=0, S1=+2, S2=+1, S3=0, S4=0, mult 0.9 → total 2.7 (prose) / 11.129 (pipeline)** with direction **up/mild**. Actual: **down, mild-to-notable (−0.94%, rel −0.49%).** Direction MISS.

**S0_SHARED_MACRO = 0 → should have been NEGATIVE.**
The morning's own prose said: *"risk-off equities + firming USD/real yields are a cyclical overlay, not a veto when oil is the sector's own shock."* That is the error in one sentence. The morning **identified all three headwinds correctly** (ES −0.66%, NQ −1.59%, DXY +0.45%, DFII10 +0.09, VIX backwardation) and then **zeroed them** on the theory that the oil shock would dominate. It did not. The 08-10 "keep S0 muted under sector_shock" rule was applied mechanically to a session where the *shared macro was itself the dominant driver*. **S0 should have been −1.** The morning even wrote the tell: *"historically the risk-off tape caps energy extension near +1–1.5%"* — it used that fact to cap *magnitude* but not to flip *sign*. That is internally inconsistent: if the tape caps extension, and the sector is already +1.50% premarket, the tape is a **ceiling**, and a ceiling on a gap-up is a fade setup.

**S1_SECTOR_FACTORS = +2 → directionally right, magnitude overstated.**
Oil *was* up, the supply-risk catalyst *was* live (Saudi pipeline shutdown). +2 was defensible at the open. But the morning's own live-verify showed CL=F +3.05% while the settled print was +1.29% — the factor **decayed by more than half**. S1 is a *level* score applied to a *decaying* input. The correct treatment was to score S1 positive but **explicitly discount it for the fade risk** (a >2.4% premarket oil spike that is already the "first >2.4% print since 09-08" is a *late* move, not an early one). **S1 = +1** would have been the honest score.

**S2_BREADTH = +1 → this was the single worst read.**
The morning scored +1 because "XLE +1.50% is the only green sector" premarket. But **premarket breadth in a single ETF is not breadth** — it is the ETF's own gap, relabeled. The morning *itself* warned (09-10 lesson): *"do not score S2 and S4 both positive off the same prior-close Channel 1 series."* It then scored S2 positive off the **premarket XLE print** — which is the *same object* as S4, just at a different timestamp. This is the double-count the lesson forbade, executed in a new disguise. **S2 should have been 0.** The "only green sector" observation is a *relative-strength* fact, and relative strength that exists only in the premarket gap is not breadth expansion — it is a gap.

**S3_FLOWS_POSITIONING = 0 → should have been NEGATIVE.**
The morning noted 1m rel **+7.80%** and called it "leftover leadership," then declined to fire the crowded-long trigger because 1w rel was only +1.95%. But the 09-10 lesson's *spirit* is about **crowding**, and +7.80% 1m rel with a multi-week outflow hangover and a fresh record-close sequence is crowded by any reasonable definition. The morning also correctly noted the 09-11 fade "interrupted" the record-close sequence — that interruption is itself a **distribution signal**, not a neutral one. **S3 should have been −1.** The pipeline's skill multiplier of 0.5 on S3 (vs 1.0 elsewhere) suggests the system already distrusts this component; here the distrust was misplaced.

**S4_ETF_TAPE = 0 → correct, and the morning deserves credit.**
The morning refused to score the 1d rel −0.53% positive, explicitly noting "the 1d tape does not confirm." That was right. The prior-session lag was a genuine warning that the sector was *already* losing relative momentum, and the morning saw it. It just didn't let that warning propagate to S0/S2/S3.

**Divergence flag:** The morning's prose says `divergence_flagged: True` ("leading factors up, prior-close tape flat-to-negative"), but the pipeline JSON says `divergence_flagged: False`. **This is a pipeline/prose contradiction.** The prose was right to flag it — and per the 09-11 lesson, *"if the divergence flag fires, FLATTEN the absolute call."* Had the flag been honored, the call would have been flattened to flat/mild, which would have been a **direction HIT** (flat) instead of a MISS. The pipeline's `False` overrode the prose's `True` and cost the call.

**Magnitude:** Predicted mild; actual −0.94% is mild-to-notable. **Magnitude HIT** (band-wise), consistent with the morning's own "risk-off tape caps extension near +1–1.5%" logic — it just applied the cap to the wrong sign.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count identified:** S2 (breadth) and S4 (ETF tape) were scored off the **same object** — XLE's own price — at two timestamps. The morning's own 09-10 lesson forbade exactly this. Net effect: the leading sum was inflated by +1 that should have been 0, and the pipeline's `leading_sum: 8.0` with `anchor score 7.88` shows the anchor was already carrying the XLE premarket print at weight 0.7. So XLE's gap was counted **three times**: once in the anchor (PM:XLE +1.50%, w=0.7), once in S2 (+1), and implicitly in S4's "confirmation" framing. **Triple-count of a single premarket print.**

**Knowable-at-open test:** The *sign* was not knowable at open — oil was genuinely up and the sector was genuinely the only green one. But the **fade risk was knowable at open**, and the morning wrote it down:
- VIX/VIX3M 1.135 backwardation = de-risking regime (knowable, written, ignored).
- "risk-off tape caps energy extension near +1–1.5%" (knowable, written, applied to magnitude only).
- 1d rel −0.53% = sector already losing relative momentum (knowable, written, not propagated).
- 1m rel +7.80% = crowded (knowable, written, dismissed).
- Oil +2.44–3.05% premarket = a *late* move, first >2.4% since 09-08 (knowable, written, treated as fresh fuel).

**Verdict: PARTIALLY knowable at open.** The direction flip was not certain, but the *asymmetry* was: a gap-up sector, in a de-risking vol regime, on a fading-late oil spike, with crowded 1m positioning, has a **negative expected value on the gap**. The correct call was **flat/mild with a downside skew** — which the 09-11 lesson's "FLATTEN the absolute call" rule would have produced had the divergence flag been honored.

**The 09-11 lesson was mis-applied.** The morning read it as "don't assign S0 a negative sign for a beta sector on green futures." But today futures were **RED** — the lesson's precondition (green futures) was absent. The morning correctly noted this ("today futures are RED, so there is no green-futures tailwind to mis-sign") but then concluded the risk-off tape was only "a mild absolute headwind." With green futures absent, the 09-11 lesson's *protective* clause doesn't apply, and the risk-off tape should have been scored at **full negative weight**, not muted.

---

## 4. Outliers inside the sector

- **BKR [Energy] −6.5%** on Chart acquisition margin drags (flagged in the morning as a single-name, correctly excluded from S1). This was a genuine idiosyncratic outlier and the morning's discipline here was correct — it did not set the sector tone.
- **Refiners (VLO/MPC):** The morning ring-fenced crack-spread strength to the refiner sleeve. With products bid premarket (HO +2.67%, RBOB +2.73%) but crude fading, refiners likely **outperformed** the integrated majors intraday (crack expansion + falling feedstock = margin tailwind). This is the one place the morning's factor read may have been *under*-weighted — but correctly so for an XLE-level call, since XLE is dominated by XOM/CVX.
- **Integrated majors (XOM/CVX):** The likely drag. These trade on the forward barrel and on the broad tape; a fading spot bid plus red SPY is a sell. They are the reason XLE closed red despite green oil.
- **Nat gas $2.903 (+2.54%)** — mild bid, N/A for oil-weighted XLE, correctly excluded.

---

## 5. Lessons for the next Energy session

1. **A gap-up in the "only green sector" on a red tape is a fade candidate, not a momentum candidate.** Relative strength that exists *only* in the premarket gap is not breadth — it is a gap. Score S2 = 0 unless breadth is confirmed by *intra-sector* participation (multiple names green on volume), not by the ETF's own print.
2. **When VIX/VIX3M is in backwardation, do not mute S0 for a high-beta sector.** Backwardation is a de-risking regime; high-beta cyclicals get sold regardless of their commodity. S0 should carry full negative weight in backwardation.
3. **A late oil spike (>2.4%, first such print in ~a week) is fuel that is already spent.** Discount S1 for fade risk when the move is late and the sector has already gapped.
4. **Honor the divergence flag.** The prose said True, the pipeline said False; the prose was right, and the 09-11 rule ("FLATTEN the absolute call") would have converted a direction MISS into a HIT. Fix the pipeline/prose contradiction.
5. **1m rel ≥ +7.5% with a multi-week outflow hangover is crowded.** Do not require the full +8% / 1w rel >+5% trigger; the spirit of the 09-10 lesson is crowding, and +7.80% 1m rel qualifies.
6. **Do not triple-count the ETF's own premarket print** across anchor + S2 + S4.

---

OUTCOME_BEGIN
SECTOR: Energy
ETF: XLE
ETF_PCT: -0.94
SPY_PCT: -0.45
REL_PCT: -0.49
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Risk-off de-risking (VIX backwardation + red index tape) overwhelmed a fading oil bid; XLE traded as a high-beta cyclical, not a commodity hedge.
KEY_INTERACTION: S2 (breadth) and S4 (ETF tape) were scored off the same object — XLE's own premarket print — triple-counting the gap alongside the anchor's PM:XLE leg; the 09-10 lesson forbade exactly this.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Direction MISS — the morning correctly identified every headwind (VIX backwardation, red tape, firm USD, crowded 1m rel, fading-late oil) but zeroed S0 and scored S2 positive off the ETF's own gap, producing an up/mild call where the asymmetry favored flat-to-down.
OUTCOME_END