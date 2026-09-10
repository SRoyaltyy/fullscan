---
trigger_pattern: "A rate-sensitive bond-proxy sector (XLU/XLRE-type) is predicted down/mild on a live rates-backup day, and the model's prose adds a 'possible relative resilience / relative beat' clause derived from a stale multi-horizon relative tape (3d/1w/1m positive) while the freshest 1d relative print is negative — and a same-day long-end supply event (auction) is explicitly flagged as capable of moving the dominant factor but is filed as an unscored caveat."
corrected_behavior: "(1) For a 1d call, the freshest relative-tape print (1d rel) dominates longer-horizon relative strength; do not smuggle a 'relative resilience' clause into the prose when the 1d rel is negative — either score it or drop it. (2) When a same-day event is identified in the calendar scan as capable of moving the dominant factor (e.g., a long-end auction on a rates-driven day), it must be reflected in the score (S0/S1), not left as an unscored caveat. (3) Add a regime qualifier to the 08-18 'risk-off + rising long-end → relative beat' frame: it applies when rates rise for growth reasons alongside an equity-idiosyncratic risk-off; when the long end is the CAUSE of the risk-off (supply/term-premium shock), the bond-proxy sector falls AND underperforms — it is the transmission channel, not the haven."
falsifier: "If, on a future rates-led risk-off day where the long end is the CAUSE of the risk-off (supply/term-premium shock) and the 1d rel print is negative, XLU nonetheless delivers a positive relative return vs SPY, this correction's regime qualifier is wrong and 08-18's unqualified 'relative beat' frame should be restored. Conversely, if a flagged same-day long-end auction is folded into the score and the sector's realized move is materially smaller than the scored weight implies, the 'score the flagged catalyst' rule is over-weighting supply events."
current_behavior: "The model scores direction correctly (down/mild) but (a) lets longer-horizon relative strength override the freshest 1d relative signal in the prose/divergence conclusion, producing a 'relative resilience' clause that is not a scored component and is not falsifiable in the grid; and (b) identifies the live long-end auction as the day's dominant price-setting risk yet assigns it zero weight, treating it as a non-scored caveat rather than folding it into S0/S1."
evidence_cited: "Outcome: XLU −0.978% vs SPY −0.599%, rel −0.379% (relative MISS). Morning prose stated 'possible relative resilience' and divergence check said 'factors and near-term tape agree on a mild-down absolute with possible relative resilience.' Morning also wrote '10Y Treasury auction is the live supply event (can push the long end)' but scored it zero. Actual driver: failed 30Y auction (coverage 2.39, high yield 5.216%) took 10Y from ~4.80% to 4.96% (+12bp) — the flagged-but-unscored event was the dominant driver. Direction and magnitude both HIT."
error_category: "C"
scope: "general"
date: "2026-09-10"
status: "active"
occurrences: "1"
promoted_on: "2026-09-10"
sources: "['2026-09-10_sector_utilities_lesson.md']"
schema_ok: "true"
---

## RULE
(1) For a 1d call, the freshest relative-tape print (1d rel) dominates longer-horizon relative strength; do not smuggle a "relative resilience" clause into the prose when the 1d rel is negative — either score it or drop it. (2) When a same-day event is identified in the calendar scan as capable of moving the dominant factor (e.g., a long-end auction on a rates-driven day), it must be reflected in the score (S0/S1), not left as an unscored caveat. (3) Add a regime qualifier to the 08-18 "risk-off + rising long-end → relative beat" frame: it applies when rates rise for growth reasons alongside an equity-idiosyncratic risk-off; when the long end is the CAUSE of the risk-off (supply/term-premium shock), the bond-proxy sector falls AND underperforms — it is the transmission channel, not the haven.

## WHEN IT FIRES
A rate-sensitive bond-proxy sector (XLU/XLRE-type) is predicted down/mild on a live rates-backup day, and the model's prose adds a "possible relative resilience / relative beat" clause derived from a stale multi-horizon relative tape (3d/1w/1m positive) while the freshest 1d relative print is negative — and a same-day long-end supply event (auction) is explicitly flagged as capable of moving the dominant factor but is filed as an unscored caveat.

## WRONG IF
If, on a future rates-led risk-off day where the long end is the CAUSE of the risk-off (supply/term-premium shock) and the 1d rel print is negative, XLU nonetheless delivers a positive relative return vs SPY, this correction's regime qualifier is wrong and 08-18's unqualified "relative beat" frame should be restored. Conversely, if a flagged same-day long-end auction is folded into the score and the sector's realized move is materially smaller than the scored weight implies, the "score the flagged catalyst" rule is over-weighting supply events.

## EVIDENCE
Outcome: XLU −0.978% vs SPY −0.599%, rel −0.379% (relative MISS). Morning prose stated "possible relative resilience" and divergence check said "factors and near-term tape agree on a mild-down absolute with possible relative resilience." Morning also wrote "10Y Treasury auction is the live supply event (can push the long end)" but scored it zero. Actual driver: failed 30Y auction (coverage 2.39, high yield 5.216%) took 10Y from ~4.80% to 4.96% (+12bp) — the flagged-but-unscored event was the dominant driver. Direction and magnitude both HIT.

(learn_cycle promote)
