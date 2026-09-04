---
trigger_pattern: "When a sector ETF is a deep medium-term laggard (1m rel ≤ −5%) AND a hawkish macro shock (hot jobs print, Fed repricing) is the dominant same-day driver, the model treats laggard status as purely negative (S2/S4 = −1) without considering the relative-shield interaction: in a risk-off tape, the selloff targets crowded recent winners (tech/AI) first, so already-de-risked laggards often outperform. The model must not double-count the same laggard fact in both S2 and S4, and must consider that laggard status can be a relative positive in a hawkish tape."
corrected_behavior: "When a sector is a deep laggard AND the dominant driver is a hawkish macro shock (hot NFP, Fed repricing), score the laggard ONCE (not in both S2 and S4), and consider the relative-shield interaction: laggards that already de-risked have less to give up when crowded longs (tech/AI) get sold. When the 1d relative tape is flat/stabilizing (rel ≥ −0.1%) and the score is negative but the tape is not confirming, cut conviction further — prefer flat/flat or flat/mild over down/mild. Do not let a double-counted laggard fact push the score more negative than the tape supports."
falsifier: "A future session where a sector is a deep laggard (1m rel ≤ −5%), a hawkish macro shock hits (hot NFP/CPI), the 1d tape is flat/stabilizing, and the sector still underperforms SPY by >0.5% — this would falsify the shield hypothesis and support the pure-negative laggard read. Also falsified if the model applies this lesson on a non-shock day and misses a down move that the laggard correctly predicted."
current_behavior: "Scores S2 (breadth) = −1 and S4 (ETF tape) = −1 for the same underlying fact (XLI 1m rel −6.77% laggard), double-counting the negative. Treats laggard status as purely bearish without considering that in a hawkish macro shock, prior laggards with less crowded positioning are relatively shielded. Emits down/mild despite acknowledging a 'mild divergence' between score and flat tape, when DO-INSTEAD guidance says to cut conviction/prefer flat when score fights tape."
evidence_cited: "2026-09-04 XLI: predicted down/flat (total −1.35), actual +0.41% absolute / +0.79% relative (up/flat). Hot jobs report (Sep hike odds → 63%) hit SPY −0.39% but XLI rose. Outcome review: 'laggard status was a shield — XLI had already de-risked, so the hot-jobs selloff had less to hit.' S2 and S4 both scored the same laggard fact as separate negatives (double-count acknowledged in morning text but still summed). CAT strength + CNH +21% 5-day drove XLI up. Morning itself noted 'mild divergence — tape is not confirming a strong down move' but still emitted down/mild."
error_category: "B"
scope: "general"
date: "2026-09-04"
status: "active"
occurrences: "1"
promoted_on: "2026-09-04"
sources: "['2026-09-04_sector_industrials_lesson.md']"
schema_ok: "true"
---

## RULE
When a sector is a deep laggard AND the dominant driver is a hawkish macro shock (hot NFP, Fed repricing), score the laggard ONCE (not in both S2 and S4), and consider the relative-shield interaction: laggards that already de-risked have less to give up when crowded longs (tech/AI) get sold. When the 1d relative tape is flat/stabilizing (rel ≥ −0.1%) and the score is negative but the tape is not confirming, cut conviction further — prefer flat/flat or flat/mild over down/mild. Do not let a double-counted laggard fact push the score more negative than the tape supports.

## WHEN IT FIRES
When a sector ETF is a deep medium-term laggard (1m rel ≤ −5%) AND a hawkish macro shock (hot jobs print, Fed repricing) is the dominant same-day driver, the model treats laggard status as purely negative (S2/S4 = −1) without considering the relative-shield interaction: in a risk-off tape, the selloff targets crowded recent winners (tech/AI) first, so already-de-risked laggards often outperform. The model must not double-count the same laggard fact in both S2 and S4, and must consider that laggard status can be a relative positive in a hawkish tape.

## WRONG IF
A future session where a sector is a deep laggard (1m rel ≤ −5%), a hawkish macro shock hits (hot NFP/CPI), the 1d tape is flat/stabilizing, and the sector still underperforms SPY by >0.5% — this would falsify the shield hypothesis and support the pure-negative laggard read. Also falsified if the model applies this lesson on a non-shock day and misses a down move that the laggard correctly predicted.

## EVIDENCE
2026-09-04 XLI: predicted down/flat (total −1.35), actual +0.41% absolute / +0.79% relative (up/flat). Hot jobs report (Sep hike odds → 63%) hit SPY −0.39% but XLI rose. Outcome review: "laggard status was a shield — XLI had already de-risked, so the hot-jobs selloff had less to hit." S2 and S4 both scored the same laggard fact as separate negatives (double-count acknowledged in morning text but still summed). CAT strength + CNH +21% 5-day drove XLI up. Morning itself noted "mild divergence — tape is not confirming a strong down move" but still emitted down/mild.

(learn_cycle promote)
