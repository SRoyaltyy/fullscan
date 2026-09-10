---
trigger_pattern: "A live macro shock (oil/geopolitical risk-off) is present at the open, and the model stacks the SAME macro fact into two separate negative score components — S0 (shared macro overlay) AND S1 (sector transmission channel, e.g. 'oil→inflation→long-end yield→rate-sensitive financials') — without independently confirming that the S1 transmission channel is actually firing in the live tape. The S1 negative is a phantom double-count of S0."
corrected_behavior: "When S0 already carries a macro shock, do NOT add an S1 negative for the same fact unless the sector's own live tape independently confirms the transmission channel is firing (e.g. XLF 1d rel ≤ −0.4%, or a fresh sector-specific rate-sensitivity headline). If the sector's 1d and 1m relative are flat (here +0.05% and +0.08%), treat the S1 transmission as UNCONFIRMED and score S1=0, not −0.5. The macro shock is counted ONCE, in S0. Additionally, when 1d/1m rel are flat and only a stale 3d rel lag is red, do not convert that lag into an S2 rotation-out vote (this is the 08-28 anti-triple-count lesson applied to S2)."
falsifier: "If on a future oil-shock day with flat 1d/1m XLF relative and a live long-end steepener, XLF underperforms SPY by ≥0.4% (rel ≤ −0.4%), then the S1 transmission channel WAS firing and this correction is wrong — S1 should remain −0.5 in that configuration."
current_behavior: "On 2026-09-10 the morning note scored S0=−2 (oil >$100 day-3, risk-off, long-end stress) AND S1=−0.5 (bear/long-end steepener as 'actively negative for rate-sensitive financials' per the 09-08 lesson), explicitly acknowledging 'oil/yields counted once in S0, once in S1.' The S1 channel did not transmit: XLF closed −0.33% vs SPY −0.60%, i.e. +0.27% RELATIVE OUTPERFORMANCE. The rate-sensitivity negative never bit. The stacked S1 −0.5 inflated the leading sum to −6.5 and pushed the magnitude band to 'mild' when the actual was flat."
evidence_cited: "Outcome: XLF −0.33%, SPY −0.60%, rel +0.27%. Morning S1=−0.5 rationale ('bear/long-end steepener = actively negative for rate-sensitive financials') produced no underperformance — XLF outperformed. Morning S2=−0.5 rationale ('3d rel −1.17%, no participation bid') was contradicted by the +0.27% rel print. The morning's own self-audit flagged the double-count ('oil/yields counted once in S0, once in S1') but treated it as legitimate rather than requiring independent confirmation. The 08-28 lesson ('do not triple-count a completed lag into S2/S3/S4') was on the books and violated in spirit via S2."
error_category: "B"
scope: "general"
date: "2026-09-10"
status: "active"
occurrences: "1"
promoted_on: "2026-09-10"
sources: "['2026-09-10_sector_financial_lesson.md']"
schema_ok: "true"
---

## RULE
When S0 already carries a macro shock, do NOT add an S1 negative for the same fact unless the sector's own live tape independently confirms the transmission channel is firing (e.g. XLF 1d rel ≤ −0.4%, or a fresh sector-specific rate-sensitivity headline). If the sector's 1d and 1m relative are flat (here +0.05% and +0.08%), treat the S1 transmission as UNCONFIRMED and score S1=0, not −0.5. The macro shock is counted ONCE, in S0. Additionally, when 1d/1m rel are flat and only a stale 3d rel lag is red, do not convert that lag into an S2 rotation-out vote (this is the 08-28 anti-triple-count lesson applied to S2).

## WHEN IT FIRES
A live macro shock (oil/geopolitical risk-off) is present at the open, and the model stacks the SAME macro fact into two separate negative score components — S0 (shared macro overlay) AND S1 (sector transmission channel, e.g. "oil→inflation→long-end yield→rate-sensitive financials") — without independently confirming that the S1 transmission channel is actually firing in the live tape. The S1 negative is a phantom double-count of S0.

## WRONG IF
If on a future oil-shock day with flat 1d/1m XLF relative and a live long-end steepener, XLF underperforms SPY by ≥0.4% (rel ≤ −0.4%), then the S1 transmission channel WAS firing and this correction is wrong — S1 should remain −0.5 in that configuration.

## EVIDENCE
Outcome: XLF −0.33%, SPY −0.60%, rel +0.27%. Morning S1=−0.5 rationale ("bear/long-end steepener = actively negative for rate-sensitive financials") produced no underperformance — XLF outperformed. Morning S2=−0.5 rationale ("3d rel −1.17%, no participation bid") was contradicted by the +0.27% rel print. The morning's own self-audit flagged the double-count ("oil/yields counted once in S0, once in S1") but treated it as legitimate rather than requiring independent confirmation. The 08-28 lesson ("do not triple-count a completed lag into S2/S3/S4") was on the books and violated in spirit via S2.

(learn_cycle promote)
