# Sector Reflect — Utilities — 2026-08-27

**Triage:** Reasoning failure (Category **B**), not a tool/data outage. Channel 1 actuals and the overnight NVDA/CRM tape were available; July PCE was already on the BEA calendar for **8/26**. The miss is misweighting: one carried easing fact was paid twice (S0=+1 and S1=+1), a 1d/3d bounce was treated as a regime, PCE was scored as still due, and 08-12/08-17 were dismissed. Rotation *intensity* (XLK ~+3%, CRM ~+23%) was only partially knowable; rotation *direction*, the PCE date, and “no fresh XLU catalyst” were knowable. No A/B discount that would turn this into a shock.

Predicted **up/notable** vs actual XLU **−0.76%** / SPY **+0.66%** / rel **−1.41%** (down/mild). Direction miss, magnitude miss.

### CHECK 1 — Lesson match
Matches standing Utilities lessons that were on the books and not used as vetoes:

- **08-17** (carried defensive bid, no fresh catalyst → relative, not absolute) — should have fired; no same-day rate-order/load/PCE impulse.
- **08-12** (risk-on tech-led tape → cap mild) — explicitly dismissed as “mildly risk-on, not strong” after NVDA/CRM were already public.
- **08-21** (don’t score easing off a stale curve) — recency was claimed via “live ~4.64%”; that was still not an 8/27 impulse (session 10Y ~4.67%).
- **08-13** (S2/S4 confirmation only) — 1d/3d bounce was scored as durable breadth.
- **08-14** — calendar was scanned, then **PCE was put on the wrong day**.
- **08-25** — applied *backwards* as a license to flip S0/S1 to +1. That lesson only blocks manufacturing *down* when S0=S1=0.
- **08-11** — applied, and **this day is its own falsifier** (yield tick + 1d/3d rel strength, no risk-off, equities rally, XLU lags).
- Cross-sector 08-27 candidates (REITs, staples, industrials, XLC, XLY) describe the same post-AHR mega-cap / carried-yield pattern. Not a retrieval blank; an application failure. New lesson is a narrowing of 08-11 plus a calendar-verify rule, not a duplicate of 08-25.

### CHECK 2 — Backward test
Scoped to *prior-session inflation already out + mega-cap growth earnings already public + NQ leading ES + only carried easing*:

- Helps **08-27**, and is consistent with **08-17 / 08-18 / 08-21** up-call misses.
- Does not overturn **08-12 / 08-13 / 08-14** hits (those had a *live* same-session CPI/PPI/retail impulse, not a print already in the rear-view).
- Preserves **08-11** via its written exception (Hormuz/risk-off bid). Unscoped “never call XLU up after a 1d bounce” would have hurt 08-11/08-12/08-13; do not write that.

### CHECK 3 — Conflict
Apparent clash with **08-11** (don’t keep calling down when yields ease and tape inflects). Resolve by 08-11’s own falsifier: require *same-session* yield relief **or** an actual defensive/risk-off bid. Carried 8/25 FRED easing + a 2-day bounce on an NQ-led morning is not 08-11. No clash with 08-12/08-17/08-18/08-21. **08-25** stays: it does not authorize minting S0/S1 = +1.

### CHECK 4 — Applied-lesson review
| Lesson | Applied? | Effect |
|---|---|---|
| 08-11 | Yes | **Hurt** — falsifier printed |
| 08-12 | Cited, dismissed | **Hurt** (magnitude; also the rotation warning) |
| 08-13 | No | **Hurt** — S2/S4 = +1 on a bounce |
| 08-14 | Yes, wrong date | **Hurt** |
| 08-17 | No | **Hurt** — should have been the directional default |
| 08-18 | Correctly skipped | Tape was risk-on, not risk-off+rising yields |
| 08-21 | Claimed satisfied | **Hurt** — prior-session easing treated as today’s spine |
| 08-25 | Over-applied | **Hurt** — anti-down rule used to justify up |

### CHECK 5 — Falsifier
If this setup recurs — inflation print already released prior session, mega-cap AI/software earnings already public, NQ leading ES, no fresh utility rate-order/load catalyst, S0/S1 only from carried easing — and XLU still closes **> +0.5% absolute** (or clearly beats SPY), the flat-to-down default is too rigid. Separately, if a *true* same-day 8:30 cool print lifts XLU despite the tech tape, “always lag after mega-cap AHR” is too broad.

**Verdict:** Correct open call was **flat-to-down / mild**, not **up / notable**. Pipeline `divergence_flagged: False` buried a narrative “mild divergence”; the 1w/1m lag and NQ-led futures were the right side.

LESSON_BEGIN
ERROR_CATEGORY: B
TRIGGER_PATTERN: A bond-proxy/defensive sector (Utilities/XLU) is scored absolute up from carried prior-session yield easing, with that same easing shock paid in both S0 and S1, on a morning when a high-impact inflation print has already been released, mega-cap AI/software earnings are already public from the prior after-hours, NQ is leading ES, and the only “fresh” sector tape is a 1–3 day relative bounce against still-ugly 1w/1m underperformance.
CURRENT_BEHAVIOR: Treats stale/carried yield easing as a confirmed live regime, double-counts it in S0 and S1, misdates the inflation print as still due today, reads a 2-day bounce as durable breadth (S2/S4=+1), dismisses the tech-led rotation cap as “not strong,” uses the 08-25 anti-down rule to justify flipping S0/S1 to +1, and lets the pipeline emit up/notable even when the write-up caps magnitude at mild.
CORRECTED_BEHAVIOR: Verify the economic calendar against a primary source before scoring any same-day 8:30 event. Do not score S0/S1 from carried yield easing; one easing fact cannot pay twice. If mega-cap growth earnings are already public and NQ leads ES, default Utilities to relative lag / flat-to-down absolute unless there is a fresh same-session yield impulse or a utility-specific catalyst. Treat a 1d/3d bounce after a smash as 08-13 confirmation only. Do not use 08-25 as a license to manufacture up. If the narrative caps mild, do not let the pipeline promote to notable. Narrow 08-11 to same-session relief or an actual risk-off/defensive bid.
EVIDENCE: 2026-08-27 predicted up/notable (S0=+1, S1=+1, S2=+1, S3=0, S4=+1, total 7.5) vs XLU −0.76%, SPY +0.66%, rel −1.41% (down/mild). July PCE printed 8/26 (headline +0.2%/3.7% 0.1pp hot; core in line), not 8/27. NVDA/CRM reported 8/26 AHR; 8/27 NVDA ~+8.7%, CRM ~+22.6%, XLK ~+3.3%. 10Y ~4.67% close vs morning ~4.64% (no fresh −6bp impulse). NEE/DUK/SO/PEG down with XLU; CEG +1.03% did not save the ETF. Morningstar/DJ: utilities down on rotation into tech on Nvidia’s outlook.
LESSON_MATCH_CHECK: Matches 08-17 (not applied), 08-12 (dismissed), 08-21 (claimed satisfied, not in spirit), 08-13 (not applied to S2/S4), 08-14 (applied with the wrong date), and 08-11 (applied; this print is 08-11’s falsifier). 08-25 was over-applied. Cross-sector match to 08-27 REIT/staples/industrials/XLC/XLY candidates. Retrieval/application failure more than a blank lesson; the new text narrows 08-11 and adds calendar-verify plus no-double-count.
BACKWARD_CHECK: Helps 08-27 and is consistent with 08-17/08-18/08-21 up-call misses. Does not overturn 08-12/08-13/08-14 hits if scoped to “print already out + mega-cap AHR already public.” Preserves 08-11 via the risk-off exception. Unscoped “never up after a bounce” would hurt 08-11/08-12/08-13 — reject that broadening.
CONFLICT_CHECK: Conflicts with a naive reading of 08-11 — resolve by 08-11’s own falsifier (need same-session yield relief or a real defensive/risk-off bid). No conflict with 08-12/08-17/08-18/08-21. 08-25 remains a ban on manufacturing down from carried S2/S3 when S0=S1=0; it does not flip S0/S1 to +1.
FALSIFIER: If this setup recurs (prior-session inflation already out, mega-cap AI/software earnings already public, NQ leading ES, no fresh utility catalyst, S0/S1 only from carried easing) and XLU still closes >+0.5% absolute or clearly beats SPY, the flat-to-down default is too rigid. If a verified same-day 8:30 cool print lifts XLU despite the tech tape, do not treat post-AHR tech follow-through as an automatic absolute veto.
DIVERGENCE_VERDICT: futures_right
ACTIVE_LESSON_REVIEW: 08-11 applied and hurt (falsifier). 08-12 applicable, dismissed, hurt. 08-13 not applied, hurt. 08-14 applied with wrong PCE date, hurt. 08-17 applicable, not applied, hurt. 08-18 correctly not applied. 08-21 spirit not applied, hurt. 08-25 over-applied as an up-license, hurt.
SECTOR: Utilities
LESSON_END
