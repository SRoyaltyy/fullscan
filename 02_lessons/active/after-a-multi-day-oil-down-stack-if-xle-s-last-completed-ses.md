---
trigger_pattern: "After a multi-day oil-down stack, if XLE’s last completed session was green / S4=0 (oil not transmitting) and the inventory/macro prints treated as live catalysts are already released, do not keep S1 at −2 or emit notable. Keep direction on relative fade; cap magnitude at mild/flat; shrink the multiplier when divergence is flagged."
corrected_behavior: "Re-verify oil as live for this session. If prior-day XLE was green despite CL/BZ down, the oil-down is in the price: S1 at most −1/0 unless a fresh same-session crude or inventory shock exists. Do not date already-released EIA/PCE as live event risk. When S4=0 and divergence is flagged, cap mag at mild/flat even if leading_sum is ≤−8. Keep direction if S2/S3 relative fade is intact. After a non-transmitting green XLE day, treat oil/XLE decoupling as live again, not inverted. Do not let S2 and S3 both fully score the same rotation/fade into a notable band."
falsifier: "After a green XLE day with oil down (S4=0), the next session still prints notable absolute XLE down driven by a fresh same-session crude collapse (CL/BZ ~−1% or worse at the live open) or a live bearish inventory shock. In that case S1=−2/notable remains valid and this cap is wrong."
current_behavior: "Reuse the prior session’s crude print as a live S1=−2 spine, carry EIA/OPEC/IEA on top, treat yesterday’s PCE/EIA as today’s two-sided event risk, and let leading_sum≤−8 print notable even with S4=0 and divergence_flagged=True. Energy experiment only shrinks confidence, not the graded mag band. Morning also declared “no divergence / trust factors,” so the 08-21 oil-up/XLE-down template was marked inverted instead of back on the table."
evidence_cited: "Predicted down/notable (−8.55, S1=−2, S4=0, divergence_flagged=True, conf 0.55) vs XLE −0.224% / SPY +0.655% / rel −0.880% (dir HIT, mag MISS, actual mag flat). 08-26 XLE was already green (+0.60%) on CL −1.11% / BZ −2.11%. 08-27 oil did not repeat that collapse; EIA (08-26) was only +95 kb crude with gasoline −2.54 Mb and distillate −2.23 Mb; July core PCE was already out in-line. Path notable mid-day (~−1.4%) then recovered; close-to-close was flat. HORIZON_3D down:mild was the better 1-day call. Rolling Energy mag 0.2 (n=10) / 0.273 (n=11); 08-26 was already a down/notable miss into a green bounce."
error_category: "A"
scope: "general"
date: "2026-08-27"
status: "active"
occurrences: "1"
promoted_on: "2026-08-28"
sources: "['2026-08-27_sector_energy_lesson.md']"
schema_ok: "true"
---

## RULE
Re-verify oil as live for this session. If prior-day XLE was green despite CL/BZ down, the oil-down is in the price: S1 at most −1/0 unless a fresh same-session crude or inventory shock exists. Do not date already-released EIA/PCE as live event risk. When S4=0 and divergence is flagged, cap mag at mild/flat even if leading_sum is ≤−8. Keep direction if S2/S3 relative fade is intact. After a non-transmitting green XLE day, treat oil/XLE decoupling as live again, not inverted. Do not let S2 and S3 both fully score the same rotation/fade into a notable band.

## WHEN IT FIRES
After a multi-day oil-down stack, if XLE’s last completed session was green / S4=0 (oil not transmitting) and the inventory/macro prints treated as live catalysts are already released, do not keep S1 at −2 or emit notable. Keep direction on relative fade; cap magnitude at mild/flat; shrink the multiplier when divergence is flagged.

## WRONG IF
After a green XLE day with oil down (S4=0), the next session still prints notable absolute XLE down driven by a fresh same-session crude collapse (CL/BZ ~−1% or worse at the live open) or a live bearish inventory shock. In that case S1=−2/notable remains valid and this cap is wrong.

## EVIDENCE
Predicted down/notable (−8.55, S1=−2, S4=0, divergence_flagged=True, conf 0.55) vs XLE −0.224% / SPY +0.655% / rel −0.880% (dir HIT, mag MISS, actual mag flat). 08-26 XLE was already green (+0.60%) on CL −1.11% / BZ −2.11%. 08-27 oil did not repeat that collapse; EIA (08-26) was only +95 kb crude with gasoline −2.54 Mb and distillate −2.23 Mb; July core PCE was already out in-line. Path notable mid-day (~−1.4%) then recovered; close-to-close was flat. HORIZON_3D down:mild was the better 1-day call. Rolling Energy mag 0.2 (n=10) / 0.273 (n=11); 08-26 was already a down/notable miss into a green bounce.

(learn_cycle promote)
