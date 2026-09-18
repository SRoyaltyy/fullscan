---
trigger_pattern: "Technology/XLK on a post-printed FOMC (day-2+), leading S0–S4 same-sign as an independently confirming NQ (≥ +0.5% vs prior cash), with only a modest green PM (PM:XLK < +1%) — a leftover negative sector_rs_tape (d1/w1) and/or calendar_size_gate then flatten the official call to flat/flat against that tape."
current_behavior: "Memo cites 08-21 / 09-16 / 09-17 (“do not emit flat against confirming NQ”; stale RS must not flip direction; FOMC size-gate idle on day-3) then still publishes flat/flat because pipeline `sector_rs_veto` (stale d1/w1 −2.05/−2.07) and `calendar_size_gate` crush both direction and band. 09-17’s candidate still required PM ≥ +1%, so a +0.60% PM was treated as license to flatten."
corrected_behavior: "If NQ independently ≥ +0.5% vs prior cash and leading scores agree up, official DIRECTION = up. Stale leftover RS (not live lag) cannot veto. Calendar_size_gate after a paid FOMC may cap MAGNITUDE at mild (08-12 notable-up still fails without a mega-cap beat; 09-14: PM < 1% is not a notable extrapolant) but must not rewrite direction to flat. Do not require PM ≥ +1% as a second key. Name scheduled Apple availability (09-09); do not add S1 support unless AAPL is actually bid."
evidence_cited: "2026-09-18 predicted flat/flat vs XLK +0.82% (up/mild), SPY −0.12%, rel +0.94%; NQ +1.50%, ES +1.14%, PM:XLK +0.60%; SOX +2.78% carried the ETF; AAPL −0.26% / MSFT −0.80% / software red; 10Y to 5.00%. Morning S0=+1, S1=+1, S2=+0.5, S3=0, S4=+1, divergence_flagged=false. Repeat of 09-17 flat-vs-confirming-NQ (then +2.25%) at smaller size. Rolling dir=0.3 mag=0.2 (n=10)."
error_category: "A"
falsifier: "NQ ≥ +0.5% vs prior cash but live XLK PM red, or live (not leftover) 1d/1w RS still negative with nested hardware not participating — and XLK finishes down/flat. Also falsified if this rule is applied on the FOMC print session itself rather than day-2+."
sector: "Technology"
date: "2026-09-18"
status: "candidate"
---

# Sector Reflection — Technology — 2026-09-18

Memory index is unavailable this run, so this uses only the injected Technology/XLK predict, outcome, scoreboard, and standing/candidate lessons.

Triage: **TOOL/DATA (deterministic override)**, not a bad S-card. Analyst scores and 08-21/09-16 already pointed **up**; `sector_rs_veto` + `calendar_size_gate` rewrote the official print to **flat/flat**.

LESSON_BEGIN
ERROR_CATEGORY: A
TRIGGER_PATTERN: Technology/XLK on a post-printed FOMC (day-2+), leading S0–S4 same-sign as an independently confirming NQ (≥ +0.5% vs prior cash), with only a modest green PM (PM:XLK < +1%) — a leftover negative sector_rs_tape (d1/w1) and/or calendar_size_gate then flatten the official call to flat/flat against that tape.
CURRENT_BEHAVIOR: Memo cites 08-21 / 09-16 / 09-17 (“do not emit flat against confirming NQ”; stale RS must not flip direction; FOMC size-gate idle on day-3) then still publishes flat/flat because pipeline `sector_rs_veto` (stale d1/w1 −2.05/−2.07) and `calendar_size_gate` crush both direction and band. 09-17’s candidate still required PM ≥ +1%, so a +0.60% PM was treated as license to flatten.
CORRECTED_BEHAVIOR: If NQ independently ≥ +0.5% vs prior cash and leading scores agree up, official DIRECTION = up. Stale leftover RS (not live lag) cannot veto. Calendar_size_gate after a paid FOMC may cap MAGNITUDE at mild (08-12 notable-up still fails without a mega-cap beat; 09-14: PM < 1% is not a notable extrapolant) but must not rewrite direction to flat. Do not require PM ≥ +1% as a second key. Name scheduled Apple availability (09-09); do not add S1 support unless AAPL is actually bid.
EVIDENCE: 2026-09-18 predicted flat/flat vs XLK +0.82% (up/mild), SPY −0.12%, rel +0.94%; NQ +1.50%, ES +1.14%, PM:XLK +0.60%; SOX +2.78% carried the ETF; AAPL −0.26% / MSFT −0.80% / software red; 10Y to 5.00%. Morning S0=+1, S1=+1, S2=+0.5, S3=0, S4=+1, divergence_flagged=false. Repeat of 09-17 flat-vs-confirming-NQ (then +2.25%) at smaller size. Rolling dir=0.3 mag=0.2 (n=10).
LESSON_MATCH_CHECK: 08-21 and 09-16 already forbid emitting flat/down against confirming NQ — this is non-application, not a missing rule. 09-17 candidate is the same family but too narrow (PM:XLK ≥ +1% AND NQ ≥ +0.5%); 09-18 is the gap (PM +0.60% + day-3 size-gate still flattened). Do not mint a parallel “Apple launch = XLK up” lesson; availability-day AAPL was red.
BACKWARD_CHECK: Would have flipped 09-17 and 09-18 to up (actuals +2.25% notable / +0.82% mild) without touching 08-12’s notable block. Does not bind on FOMC print day (09-16, actual +0.10% — size-gate may still cap that session). Does not fire if NQ is not independently ≥ +0.5% or if 1d/1w lag is live rather than leftover. 09-10 crowding-unwind path unchanged (overlay inverted here).
CONFLICT_CHECK: No fight with 08-12 / 09-14 (mild, not notable). Resolves the false fight with calendar_size_gate by splitting duties: gate caps band, NQ binds direction. 09-11 crowding-zero stays (oil offered, corr −0.437, contango). Nested leftover HEAT (semis down / software up) remains outranked by live Channel 1 + Kospi, as on 09-17.
FALSIFIER: NQ ≥ +0.5% vs prior cash but live XLK PM red, or live (not leftover) 1d/1w RS still negative with nested hardware not participating — and XLK finishes down/flat. Also falsified if this rule is applied on the FOMC print session itself rather than day-2+.
DIVERGENCE_VERDICT: none_flagged
ACTIVE_LESSON_REVIEW: 08-21, 09-16, and 09-17 stale-RS were cited and not executed in the official print — applied-lesson miss. 08-12 notable-up FAIL and 09-14 PM-not-magnitude were correct (not notable). 09-11 crowding-zero was correct (rel +0.94%, no unwind). 09-09 naming was required; treating iPhone 18 Pro availability as same-session S1 support was extra and wrong (AAPL −0.26%) but did not cause the XLK miss — SOX follow-through did. 09-04 hawkish overlay correctly zeroed; yields re-tightening to 5.00% was the same S0 duration tax (cap +1, not a sign flip).
SECTOR: Technology
LESSON_END
