---
trigger_pattern: "A sector prediction is narratively scored as down-biased because of a live risk-off macro overlay (geopolitical/oil supply shock, sharply negative futures) and a fresh same-morning hard-data miss that hits the sector’s own fundamental spine, but the final official output is taken from the deterministic pipeline without reconciliation, allowing positive structural factors (S1) or flows (S3) to override the macro overlay and emit up/flat. The model also keeps soft survey data (ISM) positive while ignoring the harder coincident manufacturing print (G.17 industrial production) released the same morning."
corrected_behavior: "Before finalizing, reconcile the deterministic pipeline with the narrative score and live macro inputs. If the pipeline output conflicts with a narrative explicitly driven by risk-off futures, a live geopolitical/oil shock, and a fresh hard-data miss, override or flag the pipeline. On any morning with a scheduled hard-data release for the sector (e.g., G.17 industrial production), do not keep S1 at +2 on soft survey/backlog evidence if the hard data misses; cap S1 at 0 or +1 and let S0/S4/negative futures set direction. If AI-power/defense leaders are themselves rolling over, do not count them as a downside cushion. Emit down/notable, or at minimum down/mild, when the macro overlay is decisively negative and the sector tape is not confirming."
falsifier: "This lesson would be falsified if a same-morning hard-data miss (e.g., G.17 IP at 0%) plus negative futures/risk-off tape were followed by XLI closing positive or near flat because AI-power/defense names rallied strongly and inflows overwhelmed the macro drag. A second falsifier would be if the deterministic pipeline’s up/flat output outperformed the down-biased narrative over a meaningful sample of similar events."
current_behavior: "Predicted Industrials up/flat despite the narrative SECTOR_SCORES summing to 0 with an explicit “direction should be down” conclusion and HORIZON_3D down:mild. The pipeline emitted total_score 2.7 and direction up/flat from the same components. S1 was held at +2 on ISM expansion, AI-power backlog, and freight recovery even though the scheduled G.17 Industrial Production report that morning printed 0% growth (miss) with manufacturing stagnant a second straight month. The AI-power/defense cushion was treated as protective even as GE Vernova was falling ~5-6% and Eaton’s Q3 estimates were cut. Actual: XLI -1.48% vs SPY -0.68%, relative -0.80%; direction and magnitude both missed."
evidence_cited: "Actual XLI -1.48%, SPY -0.68%, rel -0.80%; direction_hit False, magnitude_hit False. G.17 Industrial Production for July: 0% vs +0.1% to +0.3% expected, manufacturing stagnant for a second month; released Aug 18 and was scheduled/knowable before the open. GE Vernova fell ~5-6%; Eaton Q3 estimates cut; 30Y yield near two-year highs. The morning narrative itself concluded “direction should be down,” but the official pipeline emitted up/flat."
error_category: "B"
scope: "general"
date: "2026-08-18"
status: "active"
occurrences: "1"
promoted_on: "2026-08-19"
sources: "['2026-08-18_sector_industrials_lesson.md']"
schema_ok: "true"
---

## RULE
Before finalizing, reconcile the deterministic pipeline with the narrative score and live macro inputs. If the pipeline output conflicts with a narrative explicitly driven by risk-off futures, a live geopolitical/oil shock, and a fresh hard-data miss, override or flag the pipeline. On any morning with a scheduled hard-data release for the sector (e.g., G.17 industrial production), do not keep S1 at +2 on soft survey/backlog evidence if the hard data misses; cap S1 at 0 or +1 and let S0/S4/negative futures set direction. If AI-power/defense leaders are themselves rolling over, do not count them as a downside cushion. Emit down/notable, or at minimum down/mild, when the macro overlay is decisively negative and the sector tape is not confirming.

## WHEN IT FIRES
A sector prediction is narratively scored as down-biased because of a live risk-off macro overlay (geopolitical/oil supply shock, sharply negative futures) and a fresh same-morning hard-data miss that hits the sector’s own fundamental spine, but the final official output is taken from the deterministic pipeline without reconciliation, allowing positive structural factors (S1) or flows (S3) to override the macro overlay and emit up/flat. The model also keeps soft survey data (ISM) positive while ignoring the harder coincident manufacturing print (G.17 industrial production) released the same morning.

## WRONG IF
This lesson would be falsified if a same-morning hard-data miss (e.g., G.17 IP at 0%) plus negative futures/risk-off tape were followed by XLI closing positive or near flat because AI-power/defense names rallied strongly and inflows overwhelmed the macro drag. A second falsifier would be if the deterministic pipeline’s up/flat output outperformed the down-biased narrative over a meaningful sample of similar events.

## EVIDENCE
Actual XLI -1.48%, SPY -0.68%, rel -0.80%; direction_hit False, magnitude_hit False. G.17 Industrial Production for July: 0% vs +0.1% to +0.3% expected, manufacturing stagnant for a second month; released Aug 18 and was scheduled/knowable before the open. GE Vernova fell ~5-6%; Eaton Q3 estimates cut; 30Y yield near two-year highs. The morning narrative itself concluded “direction should be down,” but the official pipeline emitted up/flat.

(learn_cycle promote)
