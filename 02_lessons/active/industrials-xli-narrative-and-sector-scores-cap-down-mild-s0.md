---
trigger_pattern: "Industrials/XLI narrative and SECTOR_SCORES cap down/mild (Σ(S0..S4)×mult ≈ −1.8; two-sided scheduled policy event; no same-morning print; 1w/1m laggard after a non-holdings mega-cap AHR) but the deterministic pipeline emits down/flat from a more negative total_score and the scoreboard grades the pipeline band."
corrected_behavior: "Reconcile before emit: if narrative arithmetic and the 08-27 laggard-after-AHR rule say down/mild, emit down/mild — do not let pipeline −2.25 rewrite the band to flat. Do not write a new Industrials factor lesson. Do not import 08-28 leftover-S2/S4 down-bans from XLF/XLY/XLC; 08-27’s down:mild branch remains valid when the bounce gate is off. Keep S0/S1 at 0 until Chicago PMI / policy remarks print."
falsifier: "Narrative down/mild vs pipeline down/flat recurs, we emit mild, and XLI prints |pct| < 0.3% (pipeline flat would have hit) — then forcing narrative mild is wrong. Also false if 08-27 down:mild is used without the 1w/1m-laggard-after-AHR clause and XLI closes up/non-lagging the way XLF did on 08-28."
current_behavior: "Writeup called down/mild and forbade a pipeline rewrite to up/notable; official JSON still emitted down/flat (−2.25). Scoreboard recorded magnitude_hit False against actual −0.93% (mild). S0=0/S1=0 until 9:45–10:00 were left honest."
evidence_cited: "2026-08-28 predicted pipeline down/flat vs XLI −0.928% / SPY −0.227% / rel −0.701% (dir HIT, mag MISS). Narrative down/mild would have HIT. Path: open 179.44 → close 177.14, grind lower through Chicago PMI 47.1 vs ~59 and hawkish Warsh (hike odds ~55–60%). GEV ~−4.4% / ETN ~−3.1% amplified; BA/RTX held. PMI+Warsh not knowable at 09:30."
error_category: "D"
scope: "ops"
date: "2026-08-28"
status: "active"
occurrences: "1"
promoted_on: "2026-08-28"
sources: "['2026-08-28_sector_industrials_lesson.md']"
schema_ok: "true"
---

## RULE
Reconcile before emit: if narrative arithmetic and the 08-27 laggard-after-AHR rule say down/mild, emit down/mild — do not let pipeline −2.25 rewrite the band to flat. Do not write a new Industrials factor lesson. Do not import 08-28 leftover-S2/S4 down-bans from XLF/XLY/XLC; 08-27’s down:mild branch remains valid when the bounce gate is off. Keep S0/S1 at 0 until Chicago PMI / policy remarks print.

## WHEN IT FIRES
Industrials/XLI narrative and SECTOR_SCORES cap down/mild (Σ(S0..S4)×mult ≈ −1.8; two-sided scheduled policy event; no same-morning print; 1w/1m laggard after a non-holdings mega-cap AHR) but the deterministic pipeline emits down/flat from a more negative total_score and the scoreboard grades the pipeline band.

## WRONG IF
Narrative down/mild vs pipeline down/flat recurs, we emit mild, and XLI prints |pct| < 0.3% (pipeline flat would have hit) — then forcing narrative mild is wrong. Also false if 08-27 down:mild is used without the 1w/1m-laggard-after-AHR clause and XLI closes up/non-lagging the way XLF did on 08-28.

## EVIDENCE
2026-08-28 predicted pipeline down/flat vs XLI −0.928% / SPY −0.227% / rel −0.701% (dir HIT, mag MISS). Narrative down/mild would have HIT. Path: open 179.44 → close 177.14, grind lower through Chicago PMI 47.1 vs ~59 and hawkish Warsh (hike odds ~55–60%). GEV ~−4.4% / ETN ~−3.1% amplified; BA/RTX held. PMI+Warsh not knowable at 09:30.

(learn_cycle promote)
