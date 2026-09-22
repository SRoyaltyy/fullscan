# Sector Reflect — Consumer Cyclical — 2026-09-22

Memory index is paused; this uses the injected 2026-09-22 card, outcome, scoreboard, and THIS-scope lessons only.

**TRIAGE:** TOOL/DATA (engine), not LLM reasoning. Factor card S0–S4 = 0, overlay 0.0, honest band **flat**. Published **down/mild** came from leftover `tape_anchor −0.324` (ES/NQ −0.07% vs prior close) + `index_carry −0.124` against `leading_sum 0`. Close-to-close XLY **+0.089%** / SPY **−0.016%** = **flat/flat**. Direction miss, magnitude miss at the engine layer; factor card hit. Not A (Channel 2 covered). Not B at the LLM (unsigned card was right). Knowable-at-open discount: mixed/flat was knowable; AMZN −1.12% vs HD ~+2.8% was not — that split cancelled to the unsigned close, it did not create the miss.

**CHECK 1 — LESSON MATCH:** Matches standing **09-16 discarded-anchor**: do not let leftover ES/NQ vs a prior paid close mint a signed official call against an all-zero leading card. LLM applied it (self-audit named it; overlay 0; no fake divergence). Engine did not — same pathology, leftover-red instead of leftover-green. That is pipeline enforcement failure, not a missing prompt lesson. **09-21 all-zero-on-clear-risk-on** does not match (ES/NQ vs prior close −0.07%, Asia +0.41%, Europe +0.07%, XLP-led PM — T+1 digestion, not a shared-macro melt-up).

**CHECK 2 — BACKWARD TEST:** Last similar unsigned mixed days: **09-16** predicted flat, actual −0.63% (staying flat hurt direction); **09-18** flat vs −0.32% (mixed/borderline); **09-21** is the opposite trigger (needed S0+ on confirmed risk-on). Suppressing `|ES|,|NQ|<0.5%` leftover anchors on an unsigned card **helped today**, **hurt 09-16**, **n/a on 09-21**. Do not widen. Keep the existing 09-16 gate; do not mint a same-day-only “always fade T+1” rule.

**CHECK 3 — CONFLICT SCAN:** No conflict if 09-16 stays narrow: `leading_sum=0` **and** both ES and NQ inside ±0.5% vs prior close **and** no 09-21 cross-asset certificate. Distinguisher vs **08-21 reversal** (needs ES ≥ +0.3% and NQ ≥ +0.5% — Finviz +0.20%/+0.41%, did not fire). Distinguisher vs **08-11 oil-shock** (live oil sign was down). Distinguisher vs **08-27 leftover-AI** (ban on S0=+1, not a mandate to sign S0=−1). **08-28 inherited-lag** is complementary (don’t restack 1w/1m into S2–S4 when S0=0).

**CHECK 4 — APPLIED-LESSON REVIEW:** **08-27** leftover-AI ban — applied, helped (S0 stayed 0). **08-28** inherited-lag — applied, helped (no 1w/1m restack). **08-11 / 08-21 / 09-17 residual-up / 09-21 risk-on** — correctly not fired. Two-sided Fed — applied, helped (Williams/Jefferson = structure, not path). **09-16 discarded-anchor** — applied in prose/scores, **not** in v2 tape_anchor; that gap **hurt** the published call. **08-18 severe-cap** n/a.

**CHECK 5 — FALSIFIER:** If this setup recurs (unsigned S0–S4, |ES| and |NQ| vs prior close inside ±0.5%, T+1 after a paid risk-on session, no sector data binary) and XLY still closes ≤ −0.3% (down/mild or worse) with no fresh same-session consumer shock, leftover futures were informative and 09-16 must be narrowed, not defended.

**Verdict:** LLM flat was right. Engine leftover-anchor is the miss. Restate 09-16 for **both signs** at the pipeline; no new XLY prompt lesson.

LESSON_BEGIN
ERROR_CATEGORY: D
TRIGGER_PATTERN: All-zero leading S0–S4 on a T+1 digestion session where ES and NQ are both inside ±0.5% vs prior close (no cross-asset melt-up certificate), and v2 still mints a signed official direction from leftover tape_anchor plus index_carry.
CURRENT_BEHAVIOR: LLM kept S0–S4=0, overlay 0.0, honest flat, and named 09-16 discarded-anchor; engine still published down/mild from tape_anchor −0.324 (ES −0.07%, NQ −0.07%) + index_carry −0.124 against leading_sum 0.
CORRECTED_BEHAVIOR: When leading_sum=0 and both ES and NQ are inside ±0.5% vs prior close with no 09-21-style cross-asset confirmation, suppress tape_anchor and index_carry from flipping the official call off the unsigned factor-card band (flat). Apply symmetrically to leftover-green and leftover-red anchors. Do not write a new XLY prompt rule — enforce 09-16 in the engine.
EVIDENCE: 2026-09-22 predicted down/mild (engine total −0.448) vs XLY +0.089% / SPY −0.016% / rel +0.105% (flat/flat). Morning S0–S4 all 0 audited correct; AMZN −1.12% vs HD ~+2.8%/TSLA +0.82% cancelled; oil/yields slightly offered; Williams was clearing/reserves not a path binary.
LESSON_MATCH_CHECK: matches 09-16 discarded-anchor — applied at LLM (overlay 0, no fake divergence), unapplied at engine; retrieval/enforcement failure, not a new prompt lesson. 09-21 all-zero-on-clear-risk-on does not match.
BACKWARD_CHECK: mixed — helped today; 09-16 itself was flat vs actual −0.63% (hurt); 09-18 flat vs −0.32% mixed; 09-21 is the opposite trigger (do not widen)
CONFLICT_CHECK: none if 09-16 stays gated on unsigned leading_sum + |ES|,|NQ|<0.5% vs prior close; 09-21 risk-on, 08-21 reversal, and 08-11 oil-shock remain distinguished
FALSIFIER: Same unsigned T+1 mixed setup with |ES|,|NQ|<0.5% vs prior close where XLY still closes ≤ −0.3% with no fresh consumer shock — then leftover futures were informative and 09-16 must be revised
DIVERGENCE_VERDICT: none_flagged
ACTIVE_LESSON_REVIEW: 09-16 discarded-anchor applied by LLM, not by engine — hurt published call; 08-27 and 08-28 applied and helped; 08-11/08-21/09-21 correctly not fired
SECTOR: Consumer Cyclical
LESSON_END
