# Sector Outcome — Industrials — 2026-08-25

Actuals: {'etf': 'XLI', 'pct': -0.33519894051152743, 'spy_pct': 0.3195937672779925, 'rel': -0.6547927077895199, 'open': 180.75999450683594, 'close': 178.39999389648438}

I have enough context. Let me analyze the actuals against the morning prediction.

**FACTS:**
- ETF_PCT: -0.335% (XLI down)
- SPY_PCT: +0.32% (SPY up)
- REL_PCT: -0.655% (XLI underperformed SPY by ~0.65%)
- Open 180.76 → Close 178.40 (down ~1.3% intraday from open to close)
- Direction: down, mild magnitude

The morning predicted **flat/flat** with a mild negative bias on the tape. Actual was **down mild** (-0.34% absolute, -0.65% relative). The direction call was flat vs actual down — a miss on direction but the magnitude band (mild) was correct. The morning explicitly noted "flat with a mild negative bias on the tape" and S4=-1 (tape negative), so the negative bias was acknowledged but the call was capped at flat.

Let me verify the knowable-at-open test. The morning had:
- S0=0 (shared macro)
- S1=+1 (sector factors)
- S2=-1 (breadth)
- S3=0 (flows)
- S4=-1 (ETF tape)
- Total score 0.45 → flat

The tape was decisively negative (1d/3d/1w/1m all negative relative). The morning itself flagged S4=-1 and noted "XLI's tape is decisively negative." The Boeing labor negative was a fresh knowable-at-open single-name drag. The futures bounce (ES +0.44%) lifted SPY but XLI lagged — exactly the pattern that played out (SPY +0.32%, XLI -0.34%).

The morning's own self-audit said "flat with a mild negative bias on the tape" — the actual was a mild down. The direction was a miss (flat vs down), but the magnitude (mild) was correct. The negative bias was acknowledged but the call was capped at flat due to the 08-21 reversal checklist (positive futures bounce). This was the key tension: the futures bounce argued against a down call, but the tape argued for it. The tape won.

Let me write the review.

OUTCOME_BEGIN
SECTOR: Industrials
ETF: XLI
ETF_PCT: -0.335
SPY_PCT: +0.320
REL_PCT: -0.655
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Sector laggard tape (1d/3d/1w/1m all negative relative) plus fresh Boeing SPEEA strike-authorization single-name drag, while SPY rose on the broad futures bounce — XLI failed to participate.
KEY_INTERACTION: The 08-21 reversal checklist (positive futures bounce: ES +0.44%, NQ +0.92%) lifted SPY +0.32%, but XLI's relative weakness and lack of a fresh positive catalyst meant the bounce did not translate into the sector — the tape (S4=-1) beat the macro bounce (S0=0).
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Direction miss (flat vs down) but magnitude correct (mild); the negative bias was explicitly flagged (S4=-1, "flat with a mild negative bias") yet the call was capped at flat by the 08-21 reversal checklist, which overrode the tape.
OUTCOME_END

---

## Sector Post-Session Review — Industrials (XLI)

### 0. FACTS
- **XLI:** −0.335% (open 180.76 → close 178.40, ~−1.3% intraday from open)
- **SPY:** +0.320%
- **Relative:** −0.655% (XLI underperformed SPY by ~0.65%)
- **Path:** Opened near flat, drifted lower through the session while SPY held gains — a classic laggard tape, not a sector-specific crash.

### 1. What drove the sector today
- **Broad risk-on tape lifted SPY, not XLI.** The morning correctly identified a positive futures bounce (ES +0.44%, NQ +0.92%, Asia/Europe +0.41%). SPY closed +0.32%. But XLI did not participate — it closed −0.34%, a ~0.65% relative miss. This is the exact "laggard, not leader" pattern the morning flagged in S2/S4.
- **Boeing SPEEA strike authorization (fresh, knowable-at-open).** On 8/21, Boeing's largest white-collar union (SPEEA) overwhelmingly rejected the proposed four-year contract and authorized a strike (earliest strike date Oct 7). Boeing is a top XLI weight. This was a fresh single-name negative the morning explicitly noted in S1 and the self-audit.
- **No fresh positive sector catalyst.** ISM expansion and grid/AI backlog (GEV) were carried/structural and already in the tape — nothing new to bid the sector up.
- **Elevated long-end yields** (10Y 4.74 / 30Y 5.27) remained a headwind for this cyclical, though real yield was flat — a background drag, not the spine.

### 2. Audit of morning S0–S4 reads vs reality
- **S0 = 0 (shared macro):** Correct. The futures bounce was real (SPY +0.32%), but it did not translate to XLI. Scoring S0=0 (not +1) was right — the bounce was broad equity beta, not a cyclical tailwind.
- **S1 = +1 (sector factors):** Reasonable. ISM expansion and grid backlog were real but carried. The Boeing labor negative was correctly identified as a fresh drag. Capping at +1 (not +2) was correct.
- **S2 = −1 (breadth):** Correct and underweighted. XLI was a laggard across all timeframes; the actual confirmed this decisively.
- **S3 = 0 (flows):** Neutral, fine.
- **S4 = −1 (ETF tape):** **This was the most predictive read and it was correct.** The morning's own tape showed 1d/3d/1w/1m all negative relative. The actual (−0.65% rel) confirmed S4=−1 was the right call.

### 3. Interactions / double-count / knowable-at-open
- **No double-count:** Oil-down was counted once in S0 (as demand-sign, not cyclical tailwind) and not re-counted in S1. Rates used only in S0. Boeing counted once in S1. Clean.
- **Knowable-at-open: YES.** The Boeing SPEEA strike authorization was published 8/21 (Reuters, CNBC, Leeham) — fully knowable before the open. The laggard tape (all negative relative timeframes) was in Channel 1. The only genuinely two-sided input was the PCE/durables event risk (8/26), which did not fire today.

### 4. Outliers inside the sector
- **Boeing (BA)** was the standout single-name drag — SPEEA strike authorization is a top-weight negative.
- **GEV / CAT / ETN** (AI-power/machinery mega-names) were the leadership that still worked, consistent with the morning's "mega-name carry of a structural theme" read — but they were not enough to lift the broad ETF.

### Verdict
The morning's **direction call (flat) was a miss** — actual was down mild. But the **magnitude band (mild) was correct**, and the negative bias was explicitly acknowledged ("flat with a mild negative bias on the tape," S4=−1). The core error was letting the 08-21 reversal checklist (positive futures bounce) cap the call at flat when the tape (S4=−1) and the fresh Boeing negative both argued for a mild down. The tape beat the macro bounce. The lesson: when S4 is decisively negative and there is a fresh single-name negative, a positive-futures day does not automatically rescue a laggard sector — the relative call should have been down:mild, not flat.

OUTCOME_BEGIN
SECTOR: Industrials
ETF: XLI
ETF_PCT: -0.335
SPY_PCT: +0.320
REL_PCT: -0.655
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Sector laggard tape (all negative relative timeframes) plus fresh Boeing SPEEA strike-authorization single-name drag; broad futures bounce lifted SPY but not XLI.
KEY_INTERACTION: The 08-21 reversal checklist (positive futures bounce) lifted SPY +0.32% while XLI's relative weakness and lack of a fresh positive catalyst left it −0.34% — the tape (S4=−1) beat the macro bounce (S0=0).
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Direction miss (flat vs down) but magnitude correct (mild); negative bias was flagged (S4=−1) yet capped at flat by the 08-21 reversal checklist, which overrode the tape.
OUTCOME_END