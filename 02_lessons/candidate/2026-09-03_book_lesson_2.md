---
trigger_pattern: "Already-public 1–7 day headlines still print s_news=0.0 while news_actions is degraded, so catalyst names cannot displace crowded high-join buys."
corrected_behavior: "Restore news_actions ticker tilts on already-public 8-K/13G/clinical PRs in finviz_digest so s_news is non-zero at pick; do not treat news-dark dual-high join+AB as 1w confirmation."
falsifier: "Restored news_judge still prints s_news≈0 on SMMT/CYPH/GPRO-like 1–7 day headlines, or non-zero news still fails to raise 1w buy-book excess vs the news-dark book."
current_behavior: "news_actions n=23 (family degraded); every 2026-08-27 missed mover has s_news=0.0; the only worst-buy with live news (ACMR 0.345) was the worst 1w loser."
evidence_cited: "SMMT Aug 25 HARMONi-GI1 OS win still news 0.0 / join -0.95 then +19.54% outweighed; CYPH Aug 18 mining PR news 0.0 gated_out +20.11%; GPRO 13G (~Aug 20) news 0.0 gated_out +129.75%; ACMR news 0.345 then -13.75%."
error_category: "C"
scope: "book"
date: "2026-09-03"
status: "candidate_incomplete"
schema_ok: "false"
validation_errors: "do_instead must name a B-score, direction rule, futures, weight, or gate"
---

# Book reflection — 2026-09-03

Gap scan: `data/stock_book/2026-08-27_book_gaps.json`
