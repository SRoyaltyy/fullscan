---
trigger_pattern: "Missed 1w movers show s_news=0 despite a catalog headline already public before pick time, or a negative news tilt on large-cap software that then rips through a scheduled earnings window inside the horizon."
corrected_behavior: "Require news_judge to attach finviz_digest plus FDA/8-K/partnership headlines onto every name with |s_join|>0.5 or |s_ab|>0.5 (so CAPR cannot stay at 0.0 a week after the BLA-amendment news); and add a reliability gate that treats negative s_news as missing—not a sell veto—when s_ab>0.8 and an events-addon earnings date falls inside the 1w horizon."
falsifier: "Wrong if those attachments still leave s_news≈0 on CAPR-like pre-signal catalysts, or if neutralizing pre-earnings news tilts on high-AB mega-caps does not raise 1w buy-book excess vs the news-dark/inverted book."
current_behavior: "news_judge/news_actions leave s_news silent on 90% of the 2026-08-21 missed movers and assigned s_news=-0.465 to CRM and NOW, which then gained +22.39% and +12.63% while high s_ab was outweighed by negative join/news."
evidence_cited: "input_silence s_news=0.9 on 2026-08-21 movers; CAPR s_news=0.0 after the Aug 14 FDA headline; CRM and NOW s_news=-0.465 with s_ab 0.925/0.809; RZLV/ASST/GAP/CRWD s_news=0.0."
error_category: "C"
scope: "book"
date: "2026-09-01"
status: "active"
occurrences: "1"
promoted_on: "2026-09-01"
sources: "['2026-09-01_book_lesson_2.md']"
schema_ok: "true"
---

## RULE
Require news_judge to attach finviz_digest plus FDA/8-K/partnership headlines onto every name with |s_join|>0.5 or |s_ab|>0.5 (so CAPR cannot stay at 0.0 a week after the BLA-amendment news); and add a reliability gate that treats negative s_news as missing—not a sell veto—when s_ab>0.8 and an events-addon earnings date falls inside the 1w horizon.

## WHEN IT FIRES
Missed 1w movers show s_news=0 despite a catalog headline already public before pick time, or a negative news tilt on large-cap software that then rips through a scheduled earnings window inside the horizon.

## WRONG IF
Wrong if those attachments still leave s_news≈0 on CAPR-like pre-signal catalysts, or if neutralizing pre-earnings news tilts on high-AB mega-caps does not raise 1w buy-book excess vs the news-dark/inverted book.

## EVIDENCE
input_silence s_news=0.9 on 2026-08-21 movers; CAPR s_news=0.0 after the Aug 14 FDA headline; CRM and NOW s_news=-0.465 with s_ab 0.925/0.809; RZLV/ASST/GAP/CRWD s_news=0.0.

(learn_cycle promote)
