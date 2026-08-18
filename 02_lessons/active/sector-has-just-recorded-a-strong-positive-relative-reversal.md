---
trigger_pattern: "Sector has just recorded a strong positive relative reversal (1d/3d/1w rel > 0) and the model invokes a follow-through up/mild default, but a fresh legal/regulatory catalyst hits a top holding at the open. Instead of treating that catalyst as the explicit exception to the follow-through lesson, the model treats it only as a magnitude cap, and it fails to scan for same-theme legal catalysts (Section 230, parallel state suits, antitrust) that can turn a single-stock event into a sector-wide liability repricing."
corrected_behavior: "When a fresh comparable top-holding catalyst is present, do not default up/mild. Treat it as the follow-through lesson's exception, score S1 negative, and search for compounding industry-wide legal/regulatory catalysts affecting the same theme before finalizing. Re-check S0 against actual yield/futures action, especially long-end yields, rather than carrying prior risk-on. If a sector-wide legal shock is present, predict down — down/notable when damages/liability headlines are severe — unless offsetting positives are at least as strong."
falsifier: "If in two future cases with (a) prior sector rel > +1%, (b) a fresh top-holding legal catalyst confirmed at open, and (c) no stronger offsetting catalyst, XLC closes with positive relative return both times, then the 'down override' is too strong and should be relaxed to a flat cap or follow-through default. Also, if a same-themed Section 230 / parallel-litigation event fails to move sector prices after this rule predicts down, the same-day repricing assumption should be reconsidered."
current_behavior: "After a >+1% relative reversal, the model defaults to up/mild. Even when it explicitly identifies a fresh negative top-holding catalyst (Meta trial), it keeps direction up and only caps magnitude at mild. It scores S1 as 0 because positive ad/AI factors offset the legal negative, and scores S0 as +1 on prior risk-on tape without stress-testing for rising long-end yields / risk-off."
evidence_cited: "2026-08-17 XLC -1.89%, SPY -0.47%, rel -1.41%; predicted up/mild. Meta -4% on trial start / $1.4T potential damages; Snap -3%, Pinterest -4% on Ninth Circuit Section 230 ruling; Alphabet under antitrust overhang; 30Y yield hit a fresh high. The Meta catalyst was named in the morning, but S1 stayed 0 and direction stayed up."
error_category: "A"
scope: "general"
date: "2026-08-17"
status: "active"
occurrences: "1"
promoted_on: "2026-08-18"
sources: "['2026-08-17_sector_communication_services_lesson.md']"
schema_ok: "true"
---

## RULE
When a fresh comparable top-holding catalyst is present, do not default up/mild. Treat it as the follow-through lesson's exception, score S1 negative, and search for compounding industry-wide legal/regulatory catalysts affecting the same theme before finalizing. Re-check S0 against actual yield/futures action, especially long-end yields, rather than carrying prior risk-on. If a sector-wide legal shock is present, predict down — down/notable when damages/liability headlines are severe — unless offsetting positives are at least as strong.

## WHEN IT FIRES
Sector has just recorded a strong positive relative reversal (1d/3d/1w rel > 0) and the model invokes a follow-through up/mild default, but a fresh legal/regulatory catalyst hits a top holding at the open. Instead of treating that catalyst as the explicit exception to the follow-through lesson, the model treats it only as a magnitude cap, and it fails to scan for same-theme legal catalysts (Section 230, parallel state suits, antitrust) that can turn a single-stock event into a sector-wide liability repricing.

## WRONG IF
If in two future cases with (a) prior sector rel > +1%, (b) a fresh top-holding legal catalyst confirmed at open, and (c) no stronger offsetting catalyst, XLC closes with positive relative return both times, then the "down override" is too strong and should be relaxed to a flat cap or follow-through default. Also, if a same-themed Section 230 / parallel-litigation event fails to move sector prices after this rule predicts down, the same-day repricing assumption should be reconsidered.

## EVIDENCE
2026-08-17 XLC -1.89%, SPY -0.47%, rel -1.41%; predicted up/mild. Meta -4% on trial start / $1.4T potential damages; Snap -3%, Pinterest -4% on Ninth Circuit Section 230 ruling; Alphabet under antitrust overhang; 30Y yield hit a fresh high. The Meta catalyst was named in the morning, but S1 stayed 0 and direction stayed up.

(learn_cycle promote)
