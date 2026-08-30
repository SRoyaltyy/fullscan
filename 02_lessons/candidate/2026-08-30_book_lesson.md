---
trigger_pattern: "This shows up on 1w books when the news family is absent and s_news is zero-filled, so a name with a 1–5 day idiosyncratic catalyst and already-strong AB/join still dies as outweighed."
corrected_behavior: "When news_actions/news_judge are missing, drop the news family and renormalize remaining scores (same handling as general_predict when s_general=0) instead of scoring s_news as a fake 0; when the family is present, require news_judge tilts from dated catalysts in the digest so a high-AB name with a fresh product/earnings event can clear the buy cutoff."
falsifier: "Wrong if, once news_judge is live and non-zero, 1w top-10 excess does not improve on names that had a 1–5 day catalyst and s_ab>0.9 at pick time, or if those news-tilted adds underperform the universe median."
current_behavior: "On 2026-08-21 every missed mover has s_news=0.0 (news_actions/news_judge missing; finviz_digest is not turned into ticker tilts). WLY still posted s_ab=0.925 and s_join=0.442 and was left out of the top-10."
evidence_cited: "WLY 2026-08-21 (fwd +4.13 vs universe median -2.4; s_ab=0.925, s_join=0.442, s_news=0.0, class=outweighed) after the 2026-08-19 Spectral Analysis API launch; s_news=0.0 also on ADEA/HUBB/SHEL/SITC/UA; absent_inputs=['s_news']; n_blind=0; worst_buys=[]"
error_category: "C"
scope: "book"
date: "2026-08-30"
status: "candidate"
schema_ok: "true"
validation_errors: ""
---

# Book reflection — 2026-08-30

Gap scan: `data/stock_book/2026-08-21_book_gaps.json`
