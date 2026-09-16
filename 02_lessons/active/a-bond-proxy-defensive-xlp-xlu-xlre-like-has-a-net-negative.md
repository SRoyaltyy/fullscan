---
trigger_pattern: "A bond-proxy defensive (XLP/XLU/XLRE-like) has a net-negative leading factor card and a non-haven premarket print (mid-pack or red vs a green or mixed book) while leftover Finviz 1d AND 1w (or 3d/1w) relative strength is still positive from a prior FTS/catch-up session; live Channel 1 1d rel may already be ≤0. The live object can be a duration break or an unprinted same-session FOMC/SEP/presser whose mapped branches are non-positive for the bond-proxy. Narrative already refuses leftover RS as a veto, but deterministic sector_rs_veto flattens official direction to flat."
corrected_behavior: "Do not let leftover Finviz 1d/1w or 3d/1w RS veto official direction when (1) live PM is not a haven (mid/red) and/or live Channel 1 1d rel is already ≤0, and (2) leading S0–S3 is net negative. Prefer Channel 1 / PM over stale Finviz RS for the veto input. Keep calendar_size_gate for unprinted FOMC so magnitude stays ≤ mild (not notable); do not also flatten direction. Do not restack a prior-session 10Y break that already printed in the ETF."
falsifier: "If leftover Finviz 1d AND 1w RS stay positive, PM is non-haven, Channel 1 1d rel ≤0, leading S0–S3 net negative, veto is suppressed, and XLP still closes flat or up, leftover RS was the bounce — revise rather than defend."
current_behavior: "LLM scored S0=-1 / S1=-0.5 / S2=S3=S4=0, named leftover 3d/1w RS as paid, and refused a haven/FTS bid (PM XLP -0.04% vs XLK +0.65%). Engine still set sector_rs_veto_applied True on leftover Finviz d1 +1.44 / w1 +0.52 (against Channel 1 1d rel -0.36%) and calendar_size_gate, emitting official flat/flat (total -0.417) vs a factor lean that wanted down."
evidence_cited: "2026-09-16 predicted flat/flat vs XLP -0.478% (down/mild), SPY -0.441%, rel -0.037%. S0 sign matched the hawkish-dots tail; 09-15 leftover-RS veto was the direction miss-maker (same flatten as 09-15 XLP -0.82%). Size_gate was one band light. FOMC 25 bp to 3.75–4.00% + hawkish SEP/Warsh; 10Y ~5.02%; path green-into-14:00 then fade."
error_category: "D"
scope: "ops"
date: "2026-09-16"
status: "active"
occurrences: "1"
promoted_on: "2026-09-16"
sources: "['2026-09-16_sector_consumer_defensive_lesson.md']"
schema_ok: "true"
---

## RULE
Do not let leftover Finviz 1d/1w or 3d/1w RS veto official direction when (1) live PM is not a haven (mid/red) and/or live Channel 1 1d rel is already ≤0, and (2) leading S0–S3 is net negative. Prefer Channel 1 / PM over stale Finviz RS for the veto input. Keep calendar_size_gate for unprinted FOMC so magnitude stays ≤ mild (not notable); do not also flatten direction. Do not restack a prior-session 10Y break that already printed in the ETF.

## WHEN IT FIRES
A bond-proxy defensive (XLP/XLU/XLRE-like) has a net-negative leading factor card and a non-haven premarket print (mid-pack or red vs a green or mixed book) while leftover Finviz 1d AND 1w (or 3d/1w) relative strength is still positive from a prior FTS/catch-up session; live Channel 1 1d rel may already be ≤0. The live object can be a duration break or an unprinted same-session FOMC/SEP/presser whose mapped branches are non-positive for the bond-proxy. Narrative already refuses leftover RS as a veto, but deterministic sector_rs_veto flattens official direction to flat.

## WRONG IF
If leftover Finviz 1d AND 1w RS stay positive, PM is non-haven, Channel 1 1d rel ≤0, leading S0–S3 net negative, veto is suppressed, and XLP still closes flat or up, leftover RS was the bounce — revise rather than defend.

## EVIDENCE
2026-09-16 predicted flat/flat vs XLP -0.478% (down/mild), SPY -0.441%, rel -0.037%. S0 sign matched the hawkish-dots tail; 09-15 leftover-RS veto was the direction miss-maker (same flatten as 09-15 XLP -0.82%). Size_gate was one band light. FOMC 25 bp to 3.75–4.00% + hawkish SEP/Warsh; 10Y ~5.02%; path green-into-14:00 then fade.

(learn_cycle promote)
