---
trigger_pattern: "A high-beta consumer/cyclical ETF (XLY) is scored S0 positive because ES/NQ are green and the 08-21 reversal checklist is ticked, while the morning itself attributes the NQ impulse to a non-holdings mega-cap earnings print (NVDA/XLK) and the sector’s own 1d/3d/1w relative tape is already negative."
current_behavior: "Maps green ES/NQ onto XLY beta (S0=+1), uses 08-21 as a license to call up, holds S4 at 0 because “XLY may bounce on green futures” (same shock counted twice), credits 1m/3m inflows as a 1-day bid, and treats a prior-session PCE core-in-line print as same-day relief of a downside tilt."
corrected_behavior: "Do not score S0=+1 or invoke 08-21-up when the futures impulse is explicitly a non-XLY holdings earnings print and XLY 1d/3d/1w rel is already negative. Map S0 from consumer-beta participation (AMZN/TSLA/HD), not index futures. Let S4 go negative on that lag; do not keep S4=0 on the same green-futures story. Verify the sector-owned print calendar; a T-1 sticky PCE/confidence print stays in the consumer tape and does not relieve S1. Default to down/mild or down/flat, not up. Do not force notable from unknowable intraday oil/breadth."
evidence_cited: "2026-08-27 pipeline flat/flat vs XLY -1.09% / SPY +0.66% / rel -1.75%. Only S&P tech advanced; XLY worst sector; NVDA +8.7%; AMZN ~-1.5%, HD ~-1.9%, MCD ~-2.4%, TSLA +2.6%. July PCE released Aug 26 (headline +3.7% YoY, real goods -$49.9B), not Aug 27 8:30. Narrative up/mild would have been a worse miss than pipeline flat."
error_category: "B"
falsifier: "If ES/NQ are green solely on a non-holdings mega-cap earnings print, XLY 1d/3d/1w relative is already negative, and XLY still closes up and outperforms SPY by more than ~0.5% with no offsetting consumer-specific positive, this lesson is wrong and must be revised."
sector: "Consumer Cyclical"
date: "2026-08-27"
status: "promoted"
---

# Sector Reflection — Consumer Cyclical — 2026-08-27

**TRIAGE:** Reasoning failure (Category **B**), not a tool outage. Inputs were on the desk: NVDA labeled “not today’s XLY driver,” XLY already lagging 1d/3d/1w vs SPY, Conference Board 89.4 / Expectations 68.2, prior-session PCE. Those were **mis-mapped** — green ES/NQ became S0 = +1 and an 08-21 “tape = up” license, S4 was held at 0 on the same futures shock, and July PCE was treated as a **same-day in-line** that relieved the 08-25 tilt. PCE calendar (BEA **Aug 26**, not the 27th 8:30) is a secondary data error; it does not reclassify the miss as D. Knowable-at-open was **partial** (intraday oil rebound and 1-of-11 tech-only breadth were not point-estimateable) — that discounts forcing **notable**, not the direction miss. Pipeline **flat/flat** was less wrong than narrative **up/mild**; both missed **down**.

**CHECK 1 — LESSON MATCH:** Same-day sibling of `2026-08-27_sector_communication_services_lesson` (S0 = +1 on NVDA-driven NQ while the sector’s own relative tape is already red; 08-21 used as a license to call up). Could not have been retrieved at XLY predict time. Partial match to `2026-08-25_sector_industrials_lesson` (green futures do not rescue a laggard) — available, not applied; XLY was 1d/3d/1w lag, not all-timeframe + fresh single-name, so not a clean retrieval failure. **08-21 was applied and is the miss.** 08-25 XLY PCE candidate was applied to the **wrong calendar day**, then inverted into relief.

**CHECK 2 — BACKWARD TEST:** Narrowed rule (don’t map a **non-holdings** mega-cap earnings impulse onto XLY when 1d/3d/1w rel is already negative) would have **helped 08-25** (XLY lagged a rising SPY on the CB print) and **08-27**. It would **not** have flipped **08-21** (post-risk-off recovery, XLY +1.15%, impulse was not labeled NVDA/XLK-only). **08-26** down/flat HIT stays intact. Inverse of **08-18** (don’t assume XLY follows a tech-led **selloff**) — this is don’t assume XLY follows a tech-led **rally**. Not a one-day fit if 08-21 stays the broad-recovery case.

**CHECK 3 — CONFLICT:** Conflicts with **08-21** unless narrowed. **08-21** still governs: stale negatives vs a **broad** high-beta recovery tape, no fresh sector catalyst. It does **not** fire when the morning itself attributes ES/NQ to a non-XLY holdings print **and** XLY’s own near-window relative tape is already negative. No conflict with 08-18 (severe cap; AMZN was not flat), 08-11 (oil shock not live at open), or 08-12 (idiosyncratic CEO shock).

**CHECK 4 — APPLIED-LESSON REVIEW:** **08-21 reversal: applied, hurt** (S0 +1 / narrative up; XLY −1.09% trips its own −0.5% falsifier, and the “no fresh catalyst” clause was mis-read). **08-18:** applied as a severe cap; orthogonal. **08-11:** correctly not fired (oil down at open; rebound was intraday). **08-25 PCE candidate: misapplied / hurt** (wrong day + core YoY used to clear a tilt that headline 3.7% / real goods already put in the tape on the 26th). **08-12:** n/a.

**CHECK 5 — FALSIFIER:** If ES/NQ are green solely on a non-holdings mega-cap earnings print, XLY 1d/3d/1w relative is already negative, and XLY still **closes up and beats SPY by >0.5%** with no offsetting consumer-specific positive, this lesson is wrong.

**Verdict:** Leading spine was right; futures-to-XLY mapping was wrong. `DIVERGENCE_VERDICT: leading_right`. Corrected call is **down/mild or down/flat**, not notable (unknowable oil/breadth discount).

LESSON_BEGIN
ERROR_CATEGORY: B
TRIGGER_PATTERN: A high-beta consumer/cyclical ETF (XLY) is scored S0 positive because ES/NQ are green and the 08-21 reversal checklist is ticked, while the morning itself attributes the NQ impulse to a non-holdings mega-cap earnings print (NVDA/XLK) and the sector’s own 1d/3d/1w relative tape is already negative.
CURRENT_BEHAVIOR: Maps green ES/NQ onto XLY beta (S0=+1), uses 08-21 as a license to call up, holds S4 at 0 because “XLY may bounce on green futures” (same shock counted twice), credits 1m/3m inflows as a 1-day bid, and treats a prior-session PCE core-in-line print as same-day relief of a downside tilt.
CORRECTED_BEHAVIOR: Do not score S0=+1 or invoke 08-21-up when the futures impulse is explicitly a non-XLY holdings earnings print and XLY 1d/3d/1w rel is already negative. Map S0 from consumer-beta participation (AMZN/TSLA/HD), not index futures. Let S4 go negative on that lag; do not keep S4=0 on the same green-futures story. Verify the sector-owned print calendar; a T-1 sticky PCE/confidence print stays in the consumer tape and does not relieve S1. Default to down/mild or down/flat, not up. Do not force notable from unknowable intraday oil/breadth.
EVIDENCE: 2026-08-27 pipeline flat/flat vs XLY -1.09% / SPY +0.66% / rel -1.75%. Only S&P tech advanced; XLY worst sector; NVDA +8.7%; AMZN ~-1.5%, HD ~-1.9%, MCD ~-2.4%, TSLA +2.6%. July PCE released Aug 26 (headline +3.7% YoY, real goods -$49.9B), not Aug 27 8:30. Narrative up/mild would have been a worse miss than pipeline flat.
LESSON_MATCH_CHECK: Matches 2026-08-27_sector_communication_services_lesson (same-day sibling; not retrievable at predict time). Partial match to 2026-08-25_sector_industrials_lesson (green futures do not rescue a laggard) — available, not applied; trigger not exact (XLY 1m rel still positive). 08-21 was applied and caused the miss, not a retrieval gap.
BACKWARD_CHECK: Helped 08-25 (XLY lagged a rising SPY) and 08-27; would not flip 08-21 if narrowed to non-holdings earnings impulse vs broad post-risk-off recovery; 08-26 down/flat HIT unchanged. Mixed only if 08-21 is left un-narrowed.
CONFLICT_CHECK: Conflicts with 08-21 unless narrowed — 08-21 still governs stale negatives vs a broad high-beta recovery with no fresh sector catalyst; it does not apply when ES/NQ are attributed to a non-XLY holdings print and XLY’s own near-window relative tape is already negative. No conflict with 08-18, 08-11, or 08-12.
FALSIFIER: If ES/NQ are green solely on a non-holdings mega-cap earnings print, XLY 1d/3d/1w relative is already negative, and XLY still closes up and outperforms SPY by more than ~0.5% with no offsetting consumer-specific positive, this lesson is wrong and must be revised.
DIVERGENCE_VERDICT: leading_right
ACTIVE_LESSON_REVIEW: 08-21 reversal applied and hurt; 08-25 PCE candidate misapplied to the wrong day and hurt; 08-18 severe-cap applied, orthogonal; 08-11 oil-shock correctly not fired at open; 08-12 n/a.
SECTOR: Consumer Cyclical
LESSON_END
