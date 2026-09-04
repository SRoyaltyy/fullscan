---
trigger_pattern: "A scheduled high-impact macro release (NFP/CPI/FOMC) is the dominant two-sided driver, the regime lens (BN=GN) is applied correctly (no pre-scored hawkish), but the model leans directionally UP into the binary based on constructive signals (green Asia, NQ-leading futures from an earlier snapshot, sub-15 VIX) while the most recent premarket tape is weakening (ES turning negative in the second snapshot) and the 1-week real-yield backup + EPU spike point the other way."
corrected_behavior: "When a scheduled high-impact macro release is the dominant driver and the most recent premarket tape is weakening (ES/NQ turning negative in the latest snapshot, even if an earlier snapshot was positive), do not lean directionally into the binary. Default to FLAT/no-sign with magnitude capped at MILD. Use the most recent futures snapshot for B6, not the earliest. If the tape is weakening into a binary, treat that as a signal to reduce directional conviction, not as noise. Only emit a signed direction (up or down) when the tape independently confirms (net ≥ +0.5% in that direction) or a fresh same-morning catalyst resolves the binary."
falsifier: "If this trigger recurs (scheduled binary + weakening tape) and the market resolves UP on 2 of the next 3 such days (binary resolves dovish/positive despite weak tape), the flat/no-sign default is wrong and the model should lean with the expected-print conditional under the regime lens."
current_behavior: "Scores B3=0 and B2=0 (correctly two-sided into NFP per 09-03 lesson), but lets B0_ASIA +0.5, B4_VIX +0.5, B5_SENTIMENT +0.5, and B6_FUTURES +0.5 (from the first, more positive snapshot) carry the total to UP/FLAT. The second snapshot showing ES −0.44% is not used to reduce directional conviction. The model leans constructive into a binary that the weakening tape does not confirm."
evidence_cited: "2026-09-04 predicted UP/FLAT (total 2.25, B3=0, B2=0, B6=+0.5 from first snapshot ES +0.08%/NQ +0.44%); actual SPX −0.38% (down/mild). NFP printed 162k vs 53k expected (~3x), hike odds jumped 49.4%→58-63%, 2Y hit 4.416% (highest since Jan 2025). Second Channel 1 snapshot showed ES −0.44%/NQ +0.10% — futures weakening into the print. The 1w real-yield backup (+13bp 10Y, +11bp DFII10) and EPU +95 1d spike pointed hawkish but were not converted into a reduced lean."
error_category: "B"
scope: "general"
date: "2026-09-04"
status: "active"
occurrences: "1"
promoted_on: "2026-09-04"
sources: "['2026-09-04_lesson.md']"
schema_ok: "true"
---

## RULE
When a scheduled high-impact macro release is the dominant driver and the most recent premarket tape is weakening (ES/NQ turning negative in the latest snapshot, even if an earlier snapshot was positive), do not lean directionally into the binary. Default to FLAT/no-sign with magnitude capped at MILD. Use the most recent futures snapshot for B6, not the earliest. If the tape is weakening into a binary, treat that as a signal to reduce directional conviction, not as noise. Only emit a signed direction (up or down) when the tape independently confirms (net ≥ +0.5% in that direction) or a fresh same-morning catalyst resolves the binary.

## WHEN IT FIRES
A scheduled high-impact macro release (NFP/CPI/FOMC) is the dominant two-sided driver, the regime lens (BN=GN) is applied correctly (no pre-scored hawkish), but the model leans directionally UP into the binary based on constructive signals (green Asia, NQ-leading futures from an earlier snapshot, sub-15 VIX) while the most recent premarket tape is weakening (ES turning negative in the second snapshot) and the 1-week real-yield backup + EPU spike point the other way.

## WRONG IF
If this trigger recurs (scheduled binary + weakening tape) and the market resolves UP on 2 of the next 3 such days (binary resolves dovish/positive despite weak tape), the flat/no-sign default is wrong and the model should lean with the expected-print conditional under the regime lens.

## EVIDENCE
2026-09-04 predicted UP/FLAT (total 2.25, B3=0, B2=0, B6=+0.5 from first snapshot ES +0.08%/NQ +0.44%); actual SPX −0.38% (down/mild). NFP printed 162k vs 53k expected (~3x), hike odds jumped 49.4%→58-63%, 2Y hit 4.416% (highest since Jan 2025). Second Channel 1 snapshot showed ES −0.44%/NQ +0.10% — futures weakening into the print. The 1w real-yield backup (+13bp 10Y, +11bp DFII10) and EPU +95 1d spike pointed hawkish but were not converted into a reduced lean.

(learn_cycle promote)
