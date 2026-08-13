---
trigger_pattern: "Active geopolitical oil-supply risk-off (e.g., Iran/Hormuz) and/or an imminent high-impact CPI print is knowable at open, while the target sector is long-duration/rate-sensitive (REITs). Premarket equity futures are flat and global equity indices are mildly positive, so the model scores S0_SHARED_MACRO as 0 and treats 1d/1w real-yield easing as a sufficient offset, even though the 1m real-yield trend is still elevated and the sector ETF has been chronically lagging."
corrected_behavior: "When an active geopolitical/oil risk-off story and/or imminent CPI is present for long-duration REITs, score S0_SHARED_MACRO negative rather than neutral, even if premarket equity futures are flat and global equity indices are mildly positive. Treat the 1m real-yield trend as the operative duration horizon for a daily REIT call; 1d/1w easing is not a sufficient positive offset when 1m real yields remain elevated. The appropriate call on 2026-08-11 was down/mild, not down/flat."
falsifier: "A session with the same setup — active oil/Hormuz risk-off and CPI imminent — but where the 1m real-yield trend has already turned down and/or 10Y TIPS real yields fall on the day, while XLRE closes positive or outperforms SPY, would falsify the claim that S0 must be negative and that 1m real-yield elevation should override 1d/1w easing."
current_behavior: "On 2026-08-11 Real Estate, the model saw DFII10 easing on 1d/1w, futures flat, and global indices mildly positive, so it scored S0_SHARED_MACRO = 0. It used the short-term real-yield easing as a positive anchor and emitted down/flat despite active Hormuz/oil risk-off, next-day CPI, and XLRE lagging SPY on every timeframe."
evidence_cited: "Actual XLRE closed -0.73%; SPY -0.32%; relative -0.41%. The 10Y backed up to ~4.70% from 4.65% on Aug 7, pressuring rate-sensitive REITs. Scoreboard: direction_hit=True, magnitude_hit=False (predicted down/flat vs actual -0.72%). The magnitude miss came directly from underweighting the known macro risk-off at S0."
error_category: "D"
scope: "ops"
date: "2026-08-11"
status: "active"
occurrences: "1"
promoted_on: "2026-08-13"
sources: "['2026-08-11_sector_real_estate_lesson.md']"
schema_ok: "true"
---

## RULE
When an active geopolitical/oil risk-off story and/or imminent CPI is present for long-duration REITs, score S0_SHARED_MACRO negative rather than neutral, even if premarket equity futures are flat and global equity indices are mildly positive. Treat the 1m real-yield trend as the operative duration horizon for a daily REIT call; 1d/1w easing is not a sufficient positive offset when 1m real yields remain elevated. The appropriate call on 2026-08-11 was down/mild, not down/flat.

## WHEN IT FIRES
Active geopolitical oil-supply risk-off (e.g., Iran/Hormuz) and/or an imminent high-impact CPI print is knowable at open, while the target sector is long-duration/rate-sensitive (REITs). Premarket equity futures are flat and global equity indices are mildly positive, so the model scores S0_SHARED_MACRO as 0 and treats 1d/1w real-yield easing as a sufficient offset, even though the 1m real-yield trend is still elevated and the sector ETF has been chronically lagging.

## WRONG IF
A session with the same setup — active oil/Hormuz risk-off and CPI imminent — but where the 1m real-yield trend has already turned down and/or 10Y TIPS real yields fall on the day, while XLRE closes positive or outperforms SPY, would falsify the claim that S0 must be negative and that 1m real-yield elevation should override 1d/1w easing.

## EVIDENCE
Actual XLRE closed -0.73%; SPY -0.32%; relative -0.41%. The 10Y backed up to ~4.70% from 4.65% on Aug 7, pressuring rate-sensitive REITs. Scoreboard: direction_hit=True, magnitude_hit=False (predicted down/flat vs actual -0.72%). The magnitude miss came directly from underweighting the known macro risk-off at S0.

(learn_cycle promote)
