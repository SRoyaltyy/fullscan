---
trigger_pattern: "For a bond-proxy/defensive sector, when S0 and S1 are both neutral because the macro forces are offsetting (risk-on equity tape vs easing/intermediate yields but elevated long-end), and the only negative components are carried relative-breadth/outflow scores (S2/S3) with no fresh decisive sector-level negative, the model over-commits to `down` instead of `flat`. Separately, if the PREDICT block contains explicit `predicted_direction` and `predicted_magnitude_band`, the scoreboard must not later record `predicted None/None`."
corrected_behavior: "When S0=0 and S1=0, and the only negatives are carried S2/S3 relative/flow scores, prefer `flat` (or `flat/up`) with a mild band on a risk-on tape; do not manufacture a directional down call from carried negatives alone. Reserve `down` for cases with a fresh sector-level negative catalyst and/or decisive negative sector tape. Also preserve the explicit predicted direction/magnitude in the scoreboard output; a PREDICT block with `predicted_direction` and `predicted_magnitude_band` must not be graded as `None/None`."
falsifier: "A future Utilities case with S0=0, S1=0, S2=-1, S3=-1, S4=0, risk-on futures, and XLU closing down by more than ~0.5% would falsify the flat-default rule. That would show carried breadth/outflows can dominate even with neutral macro and a positive tape. Likewise, if a future scoreboard still records `predicted None/None` when the PREDICT block has explicit labels, the tooling/pipeline correction is incomplete."
current_behavior: "The model takes S2=-1 and S3=-1 mechanically to a negative total, applies a magnitude-capping multiplier, and outputs `down` even when the shared macro read is neutral and the premarket tape is risk-on. It treats positive futures as a reason to cap the negative magnitude but not as a reason to avoid the down call. The scoreboard also shows `predicted None/None` despite the PREDICT block clearly saying `down/mild`."
evidence_cited: "XLU actual +0.21% vs SPY +0.32% (rel -0.11%), actual direction `up`/magnitude `flat`. Morning components: S0=0, S1=0, S2=-1, S3=-1, S4=0, multiplier 0.9, total ~ -1.8 → predicted `down/mild`. The risk-on Nasdaq-led tape (NQ +0.92%, ES +0.44%) carried XLU up via beta; carried breadth/outflows did not force a down day. The scoreboard entry reads `predicted None/None`, a tooling error, but even with the correct `down/mild` extraction the directional call would still have missed."
error_category: "C"
scope: "general"
date: "2026-08-25"
status: "active"
occurrences: "1"
promoted_on: "2026-08-27"
sources: "['2026-08-25_sector_utilities_lesson.md']"
schema_ok: "true"
---

## RULE
When S0=0 and S1=0, and the only negatives are carried S2/S3 relative/flow scores, prefer `flat` (or `flat/up`) with a mild band on a risk-on tape; do not manufacture a directional down call from carried negatives alone. Reserve `down` for cases with a fresh sector-level negative catalyst and/or decisive negative sector tape. Also preserve the explicit predicted direction/magnitude in the scoreboard output; a PREDICT block with `predicted_direction` and `predicted_magnitude_band` must not be graded as `None/None`.

## WHEN IT FIRES
For a bond-proxy/defensive sector, when S0 and S1 are both neutral because the macro forces are offsetting (risk-on equity tape vs easing/intermediate yields but elevated long-end), and the only negative components are carried relative-breadth/outflow scores (S2/S3) with no fresh decisive sector-level negative, the model over-commits to `down` instead of `flat`. Separately, if the PREDICT block contains explicit `predicted_direction` and `predicted_magnitude_band`, the scoreboard must not later record `predicted None/None`.

## WRONG IF
A future Utilities case with S0=0, S1=0, S2=-1, S3=-1, S4=0, risk-on futures, and XLU closing down by more than ~0.5% would falsify the flat-default rule. That would show carried breadth/outflows can dominate even with neutral macro and a positive tape. Likewise, if a future scoreboard still records `predicted None/None` when the PREDICT block has explicit labels, the tooling/pipeline correction is incomplete.

## EVIDENCE
XLU actual +0.21% vs SPY +0.32% (rel -0.11%), actual direction `up`/magnitude `flat`. Morning components: S0=0, S1=0, S2=-1, S3=-1, S4=0, multiplier 0.9, total ~ -1.8 → predicted `down/mild`. The risk-on Nasdaq-led tape (NQ +0.92%, ES +0.44%) carried XLU up via beta; carried breadth/outflows did not force a down day. The scoreboard entry reads `predicted None/None`, a tooling error, but even with the correct `down/mild` extraction the directional call would still have missed.

(learn_cycle promote)
