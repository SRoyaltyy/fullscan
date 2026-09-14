# Sector Outcome — Financial — 2026-09-14

Actuals: {'etf': 'XLF', 'pct': -0.38428160821506463, 'spy_pct': -0.446162221482016, 'rel': 0.06188061326695138, 'open': 57.34000015258789, 'close': 57.029998779296875, 'source': 'yf_download'}

# Sector Post-Session Review — Financial (XLF) — 2026-09-14

## 0. FACTS

**Channel 1 (actuals, deterministic):**
- **XLF: −0.384%** (open 57.34 → close 57.03)
- **SPY: −0.446%**
- **Relative: +0.062%** (XLF outperformed SPY by ~6 bps)
- **Path:** opened at 57.34 (above the +0.33% premarket tell), faded through the session to close near the low end at 57.03.

**Morning prediction:** direction **down**, magnitude **mild**, total_score −3.345, mult 0.9, divergence_flagged True.

**Verdict on the call:** **Direction HIT** (down). **Magnitude HIT** (mild — a −0.38% move is the definition of mild). The relative outcome (+0.06%) is essentially flat, which is exactly what the morning's "flat/mild" band and the sub-gate rel read implied.

**Context from search (corroborating the tape):**
- CLAIM: S&P 500 fell ~0.5%, Nasdaq 100 dropped ~0.8%, Dow shed ~152 points on Sept 14, 2026.
  URL: https://tradingeconomics.com/united-states/stock-market
  PUBLISHED: 2026-09-14
  QUOTE: "The S&P 500 fell 0.5% and the Nasdaq 100 dropped 0.8%, while the Dow shed 152 points."
  SUMMARY: Confirms the red-tape, tech-led risk-off regime the morning read flagged (ES −0.66%, NQ −1.59%).

- CLAIM: Stocks finished lower under a selloff in AI-linked semis, elevated crude, and a sharp rise in Treasury yields ahead of the Fed.
  URL: https://vistapglobal.com/sp-500-nasdaq-close-lower-as-ai-euphoria-encounters-a-5-reality-check-sept-14-2026-...
  PUBLISHED: 2026-09-14
  QUOTE: "U.S. stocks finished lower Monday... as investor appetite for risk weakened under the combined weight of a selloff in AI-linked semiconductor shares, elevated crude oil prices, and a sharp rise in Treasury yields ahead of this..."
  SUMMARY: The three live drivers the morning S0 named — oil re-spike, long-end backup, tech unwind — all showed up in the close.

- CLAIM: 10Y Treasury yield hit its highest since 2023; crude spiked to four-month highs.
  URL: https://www.thestreet.com/stock-market-today/stock-market-today-dow-jones-sp-500-nasdaq-updates-sept-14-2026
  PUBLISHED: 2026-09-14
  QUOTE: "The move in stocks today coincided with Crude oil spiking to four-month highs, the 10Y Treasury reaching its highest since 2023..."
  SUMMARY: Confirms the long-end stress (DGS10 4.95, DGS30 5.37) and oil re-spike (WTI $102.29) were the session's dominant macro facts — exactly the S0 inputs.

---

## 1. What drove the sector today

The taxonomy-aligned drivers, in order of weight:

1. **Shared macro / rates (dominant, but sector-neutral-to-slightly-negative).** The 10Y hit its highest since 2023 and the 30Y sat at 5.37% — a bear/long-end steepener. Per the standing 08-17 lesson, this is a **headwind, not NIM+**. It pressured the whole tape (SPY −0.45%) and XLF with it, but did not single out financials.

2. **Oil re-spike (WTI $102.29 +2.44%, Brent $107.33 +2.80%).** A mild inflation/funding-channel negative, carried once in S0. It did not become a bank-specific catalyst.

3. **Tech-led risk-off / value rotation.** NQ led down (−1.59% futures, ~−0.8% cash per search). The rotation *out of* high-multiple growth *into* value/financials is the reason XLF's relative print was **positive (+0.06%)** despite an absolute down day. This is the 08-18 shape — but it never fired the ≥ +0.4% live-rel gate, so it stayed a *relative* tell, not an absolute bid.

4. **Credit (HY OAS 2.70, +0.05 1w).** Tight, creeping wider — no blowout, no bank-specific stress. Contributed nothing directional.

**Net:** XLF fell because the whole market fell (shared macro), and it fell *slightly less* than SPY because the value-rotation bid partially offset the rates headwind. That is precisely a "mild down, flat relative" day.

---

## 2. Audit of morning S0–S4 reads against reality

| Score | Morning read | Reality | Verdict |
|---|---|---|---|
| **S0_SHARED_MACRO** | −1 (red futures + oil re-spike + long-end stress, partly offset by value-rotation bid) | SPY −0.45%, oil spiked, 10Y highest since 2023 | **Correct.** The −1 was the right size: negative but not the −2 of the 09-08 shock. XLF's −0.38% is a mild-down day, consistent with a −1 S0. |
| **S1_SECTOR_FACTORS** | 0 (no fresh money-center catalyst; BBVA/BCS/BNS/AON not XLF drivers; 09-10 lesson: don't score S1 on macro narrative alone) | No fresh bank-specific headline moved XLF | **Correct.** Holding S1 at 0 was right — there was no sector-specific transmission channel confirmed by XLF's own tape. |
| **S2_BREADTH** | 0 (1d rel −0.18% sub-gate = noise; 08-28 lesson: don't triple-count the 1w lag) | Rel came in +0.06% — flat | **Correct.** The sub-gate rel was indeed noise; treating it as signal would have over-scored the downside. |
| **S3_FLOWS_POSITIONING** | 0 (trailing outflows, not crowded) | No fresh flow signal | **Correct / inert.** |
| **S4_ETF_TAPE** | 0 (premarket +0.33% is a relative tell, not an absolute-up license; 09-11 lesson: don't score S4 on sub-gate stale rel) | XLF opened 57.34, faded to 57.03 | **Correct.** The premarket green did NOT translate into an absolute up day — exactly why S4 was held at 0 rather than scored positive. |

**The critical audit point:** the morning flagged `divergence_flagged: True` — leading sum (S0–S3 = −1) vs S4 = 0. The morning resolved this by *trusting the live macro overlay over the premarket relative tell, but not promoting to notable*. Reality validated that resolution: XLF went **down** (macro overlay won) but only **mildly** and **outperformed** (the relative tell was real, just not an absolute-up license). Both halves of the divergence resolution were correct.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count check:** Oil/yields were counted **once** in S0. S1 was capped at 0 per the 09-10 lesson. The 1w rel lag was **not** restacked into S2/S3/S4 per 08-28. The premarket +0.33% was used as a *relative* tell (capping the downside) but **not** scored as an S4 positive. No double-counting detected.

**Interaction that mattered:** the value-rotation bid (XLF green premarket while XLI/XLK/XLY red) *interacted with* the shared macro headwind to produce a **flat relative / mild absolute down** outcome. The morning correctly identified this as the 08-18 shape but correctly refused to fire the trigger because the live 1d rel (−0.18%) was below the ≥ +0.4% gate. Had it fired the trigger, it would have predicted flat-to-up — which would have been a **direction MISS**. The gate saved the call.

**Knowable-at-open test:** Everything that drove the outcome was knowable at the open — red futures, oil re-spike, long-end stress, XLF premarket green, sub-gate rel. There was no 8:30 print, no fresh money-center earnings, no same-morning binary. **KNOWABLE_AT_OPEN: yes.** The morning read extracted the right signal from available information.

---

## 4. Outliers inside the sector

- **XLF's positive relative print (+0.06%) on a red day** is the notable intra-sector feature: financials were one of the few cyclical groups to hold up better than the index, consistent with the value-rotation thesis. This is the "low-beta/value bid" that the HIT_GRID scored as HIT.
- **No single-name outlier** is identifiable from the data provided (no constituent-level tape injected). The morning correctly refused to map BBVA/BCS/BNS/AON (foreign/Canadian banks, insurance M&A) into XLF — none of those are XLF money-center drivers, and none showed up as an XLF mover.
- **The 10Y "highest since 2023"** is a macro outlier worth flagging: it's the kind of long-end stress that, per 08-17, is a headwind rather than a NIM tailwind — and the tape confirmed financials did not get a NIM bid from it.

---

## 5. Scorecard and lessons

**Outcome:** Direction HIT, magnitude HIT, relative essentially flat (+0.06%). This is a **clean, well-calibrated call** — the first clean dir+mag hit in the recent Financial sequence after the 09-11 miss.

**What worked:**
- The **09-10 lesson** (don't score S1 on macro narrative alone when 1d rel is sub-gate) kept S1 at 0 and prevented an over-bearish stack.
- The **09-11 lesson** (don't score S4 on a sub-gate stale rel and resolve toward the benign branch) kept S4 at 0 and prevented a manufactured flat/up call.
- The **08-18 gate** (≥ +0.4% live rel to fire rotation-into-banks) correctly stayed off, preventing a direction miss.
- The **08-21 mag temper** (one band, rolling mag 0.5) correctly produced flat/mild rather than notable.

**What to carry forward:**
- The divergence resolution rule — *trust the live macro overlay over a sub-gate premarket relative tell, but cap at flat/mild* — is now validated by a second data point (09-11 was the miss that taught it; 09-14 is the hit that confirms it). This should be promoted from "lesson" to "standing rule" for Financial.
- The value-rotation-into-financials shape (XLF green while cyclicals red) is a **relative** signal only until the ≥ +0.4% live-rel gate fires. Today it produced +0.06% rel — real but tiny. Do not upgrade its weight.

**Residual risk:** the call was correct but the magnitude band was wide (flat/mild), and the relative outcome (+0.06%) sat right at the flat/mild boundary. A slightly larger value-rotation bid or a slightly smaller rates headwind could have flipped the absolute sign to flat. The confidence (0.5) was appropriately modest.

---

OUTCOME_BEGIN
SECTOR: Financial
ETF: XLF
ETF_PCT: -0.384
SPY_PCT: -0.446
REL_PCT: +0.062
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Shared macro — 10Y highest since 2023 + oil re-spike + tech-led risk-off dragged the whole tape down; value-rotation bid kept XLF's relative print slightly positive.
KEY_INTERACTION: Value-rotation-into-financials (XLF green premarket vs red cyclicals) partially offset the shared macro headwind, producing mild-down absolute / flat relative — but the 08-18 ≥+0.4% live-rel gate never fired, so it stayed a relative tell, not an absolute bid.
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Direction HIT, magnitude HIT — the divergence resolution (trust macro overlay over sub-gate premarket rel, cap at flat/mild) was validated; S1/S2/S4 correctly held at 0 per 09-10/09-11/08-28 lessons.
OUTCOME_END