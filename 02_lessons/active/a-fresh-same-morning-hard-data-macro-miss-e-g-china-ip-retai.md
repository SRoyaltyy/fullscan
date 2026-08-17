---
trigger_pattern: "A fresh same-morning hard-data macro miss (e.g., China IP/retail) is released while a stagflation/oil narrative has already dragged US stocks lower the prior session; US futures are only flat-to-mildly positive (ES within ±0.5%); the strongest positive component is a carried, LOW-confidence Fed-easing repricing from the prior week; and there is no fresh index-relevant mega-cap earnings catalyst. Asia rallies but is led by non-China-demand sectors (e.g., Korean chip rebound) and is misread as confirmation that the market embraces the bad-news-good channel."
corrected_behavior: "When the fresh same-morning catalyst is a hard-data growth miss feeding an existing stagflation/down tape and the main positive is a carried repricing (not a fresh catalyst), score B1 at -2 for the miss cluster; cap B3 at +0.5 when the repricing is >1 session old or the source is LOW confidence and no fresh US macro print/earnings catalyst confirms it; score B0 for raw session strength only and do not import regime confirmation from an Asia rally led by non-China sectors. Unless ES or NQ independently confirm up (net ≥ +0.5%), emit direction DOWN with magnitude capped at MILD. If a fresh index-relevant mega-cap earnings catalyst is present, mega-cap-earnings-over-macro-drag takes precedence and forbids the down call."
falsifier: "If this trigger recurs — fresh hard-data growth miss + prior-session stagflation pull + carried Fed-easing hopes + no fresh earnings catalyst — and SPX closes ≥ +0.5% (or flat-to-up) on 2 of the next 3 such days, the down-bias correction is wrong and must be narrowed or discarded."
current_behavior: "Scored B1=-1 for the China-miss/stagflation cluster while B3=+1 (×2 = +2.0, the largest positive, at LOW confidence) and B0=+0.5 (Asia read as bad-news-good confirmation) carried the net to UP/FLAT (total 2.25). Actual SPX -0.52% (down/mild), closed near day low — the market followed the fresh negative cluster, not the carried repricing."
evidence_cited: "2026-08-17 predicted up/flat (total 2.25; B1=-1, B3=+1 low-conf, B0=+0.5); actual SPX -0.52% down/mild — direction miss and magnitude miss. China July IP/retail miss, Friday's stagflation pull, and firm Brent were all cited premarket; the misweight was elevating the carried Fed-easing repricing and Asia's chip-led rally above the fresh negative cluster."
error_category: "B"
scope: "general"
date: "2026-08-17"
status: "active"
occurrences: "1"
promoted_on: "2026-08-17"
sources: "['2026-08-17_lesson.md']"
schema_ok: "true"
---

## RULE
When the fresh same-morning catalyst is a hard-data growth miss feeding an existing stagflation/down tape and the main positive is a carried repricing (not a fresh catalyst), score B1 at -2 for the miss cluster; cap B3 at +0.5 when the repricing is >1 session old or the source is LOW confidence and no fresh US macro print/earnings catalyst confirms it; score B0 for raw session strength only and do not import regime confirmation from an Asia rally led by non-China sectors. Unless ES or NQ independently confirm up (net ≥ +0.5%), emit direction DOWN with magnitude capped at MILD. If a fresh index-relevant mega-cap earnings catalyst is present, mega-cap-earnings-over-macro-drag takes precedence and forbids the down call.

## WHEN IT FIRES
A fresh same-morning hard-data macro miss (e.g., China IP/retail) is released while a stagflation/oil narrative has already dragged US stocks lower the prior session; US futures are only flat-to-mildly positive (ES within ±0.5%); the strongest positive component is a carried, LOW-confidence Fed-easing repricing from the prior week; and there is no fresh index-relevant mega-cap earnings catalyst. Asia rallies but is led by non-China-demand sectors (e.g., Korean chip rebound) and is misread as confirmation that the market embraces the bad-news-good channel.

## WRONG IF
If this trigger recurs — fresh hard-data growth miss + prior-session stagflation pull + carried Fed-easing hopes + no fresh earnings catalyst — and SPX closes ≥ +0.5% (or flat-to-up) on 2 of the next 3 such days, the down-bias correction is wrong and must be narrowed or discarded.

## EVIDENCE
2026-08-17 predicted up/flat (total 2.25; B1=-1, B3=+1 low-conf, B0=+0.5); actual SPX -0.52% down/mild — direction miss and magnitude miss. China July IP/retail miss, Friday's stagflation pull, and firm Brent were all cited premarket; the misweight was elevating the carried Fed-easing repricing and Asia's chip-led rally above the fresh negative cluster.

(learn_cycle promote)
