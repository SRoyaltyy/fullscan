---
trigger_pattern: "On 1w books, micro/small names with maxed peer RS and positive AB still die on a hard join eligibility floor, so they never enter the top-10 auction."
corrected_behavior: "Replace the hard join-floor kill with a compete-if-supported exception — if s_peer is maxed (~1.0) and s_ab>0, keep the ticker in the 1w ranker and let the existing score stack decide (no family-weight change)."
falsifier: "Passing peer-maxed, AB-positive names through the join-floor does not raise 1w excess vs the gated book, or it fattens the left tail more than it catches CYPH-type hits."
current_behavior: "Join-floor gated_out drops the name before the rest of the stack can score; s_peer=1.0 and solid s_ab cannot restore eligibility."
evidence_cited: "CYPH 2026-08-21 gated_out (join 0.102, AB 0.704, peer 1.0, news 0.0, +20.42%); RR 2026-08-21 gated_out (join -0.15, AB 0.358, peer 0.371, +7.65%)."
error_category: "A"
scope: "book"
date: "2026-09-02"
status: "candidate"
schema_ok: "true"
validation_errors: ""
---

# Book reflection — 2026-09-02

Gap scan: `data/stock_book/2026-08-21_book_gaps.json`
