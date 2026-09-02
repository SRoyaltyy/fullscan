---
trigger_pattern: "When earnings land inside the 1w horizon, deeply negative s_join and wrong-signed s_news outweigh high s_ab on the print name and fade the whole software follow-through."
corrected_behavior: "Events-addon anti-fade gate — if days-to-earnings is in (0, 7] and s_ab is top-tercile, clip s_join and s_news contributions at 0 (no negative veto) so the name can still clear the 1w book."
falsifier: "Clipping join/news to ≥0 on high-AB names with earnings inside 7d fails to lift 1w excess, or those names still move against the book after the overlay."
current_behavior: "The six-family ranker treats join/news as unrestricted fades; the events addon is healthy but unused in ranking, so CRM/NOW look like ordinary weak-join large caps."
evidence_cited: "CRM 2026-08-21 outweighed into Aug 26 print (join -0.642, news -0.465, AB 0.925, +22.39%); NOW (join -0.656, news -0.465, AB 0.809, +12.63%); CRWD/TEAM/PATH same-scan software follow-through (+13.78/+10.83/+10.74)."
error_category: "B"
scope: "book"
date: "2026-09-02"
status: "candidate"
schema_ok: "true"
validation_errors: ""
---

# Book reflection — 2026-09-02

Gap scan: `data/stock_book/2026-08-21_book_gaps.json`
