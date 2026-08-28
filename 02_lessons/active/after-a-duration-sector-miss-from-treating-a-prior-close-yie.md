---
trigger_pattern: "After a duration-sector miss from treating a prior-close yield table as the live curve, the next REIT call over-corrects: it treats the next prior-close easing / oil-slide snapshot as a live positive rate spine, scores that same shock in both S0 and S1, and emits up even though the long end is still in a multi-decade stress zone, the sector’s own 1d relative tape is already red, a sticky inflation print is already in the market, and a two-sided policy event is next session."
corrected_behavior: "08-25 is a ban on forcing down, not an up authorization — default flat unless the open/premarket 10Y/30Y/TIPS curve is independently verified live and still falling. If 30Y remains in the stress zone, cap S0/S1 at 0 (or negative) and do not double-count one easing/oil-slide shock across S0 and S1. Do not use structural DC/industrial occupancy as same-day up votes. A fading 1d relative tape plus an already-printed sticky PCE into a two-sided policy event is a magnitude-and-direction cap: emit flat or down/mild, not up. Green equity futures are XLK beta, not REIT duration relief."
falsifier: "If 30Y remains ≥5.15% near a multi-decade high, the open/premarket curve is only a ~6 bp prior-close dip (not a verified second-day live decline), XLRE 1d relative is already red, sticky PCE is already printed, and a two-sided policy event is next session — and XLRE still closes ≥+0.5% absolute or ≥+0.5% vs SPY even after a same-day long-end backup — then “don’t call up” is too strict. Separately, a verified live falling 10Y/30Y/TIPS curve at the open on an oil slide, with no next-session policy event, that produces a REIT rally after a flat call would show 08-25’s positive spine should sometimes win."
current_behavior: "Cites 08-25 (“don’t force down when yields ease on an oil slide”) as a license to call up; runs the 08-17 live-rate check on stale prior-close deltas; names 08-21 level-vs-change (30Y still ~5.17%) then still scores S0=+1 and S1=+1; pads S1 with always-on data-center/industrial HITS; treats green ES/NQ as REIT-supportive; and lets leading factors override a red 1d tape. Narrative even said up/flat / divergence flagged; pipeline still emitted up/mild."
evidence_cited: "2026-08-27 predicted XLRE up/mild (S0=+1, S1=+1, S2=0, S3=0, S4=0, mult 0.9, total 4.5). Actual XLRE −0.9536%, SPY +0.6553%, rel −1.6089% (dir MISS, mag HIT). Morning used prior-close 10Y 4.64% (−6 bp) / 30Y 5.17% (−6 bp) / DFII10 2.32% (−6 bp) plus CL −1.11%. Session reversed: 10Y 4.67% (+3 bp), 30Y 5.19% (+2 bp), WTI +1.8% to $83.67; equity REITs −1.0%, housing −1.1%, S&P +0.7%. PCE 3.7/3.3 was already released 8/26; Warsh JH was still 8/28. Open-known facts already supported flat/down-mild."
error_category: "B"
scope: "general"
date: "2026-08-27"
status: "active"
occurrences: "1"
promoted_on: "2026-08-28"
sources: "['2026-08-27_sector_real_estate_lesson.md']"
schema_ok: "true"
---

## RULE
08-25 is a ban on forcing down, not an up authorization — default flat unless the open/premarket 10Y/30Y/TIPS curve is independently verified live and still falling. If 30Y remains in the stress zone, cap S0/S1 at 0 (or negative) and do not double-count one easing/oil-slide shock across S0 and S1. Do not use structural DC/industrial occupancy as same-day up votes. A fading 1d relative tape plus an already-printed sticky PCE into a two-sided policy event is a magnitude-and-direction cap: emit flat or down/mild, not up. Green equity futures are XLK beta, not REIT duration relief.

## WHEN IT FIRES
After a duration-sector miss from treating a prior-close yield table as the live curve, the next REIT call over-corrects: it treats the next prior-close easing / oil-slide snapshot as a live positive rate spine, scores that same shock in both S0 and S1, and emits up even though the long end is still in a multi-decade stress zone, the sector’s own 1d relative tape is already red, a sticky inflation print is already in the market, and a two-sided policy event is next session.

## WRONG IF
If 30Y remains ≥5.15% near a multi-decade high, the open/premarket curve is only a ~6 bp prior-close dip (not a verified second-day live decline), XLRE 1d relative is already red, sticky PCE is already printed, and a two-sided policy event is next session — and XLRE still closes ≥+0.5% absolute or ≥+0.5% vs SPY even after a same-day long-end backup — then “don’t call up” is too strict. Separately, a verified live falling 10Y/30Y/TIPS curve at the open on an oil slide, with no next-session policy event, that produces a REIT rally after a flat call would show 08-25’s positive spine should sometimes win.

## EVIDENCE
2026-08-27 predicted XLRE up/mild (S0=+1, S1=+1, S2=0, S3=0, S4=0, mult 0.9, total 4.5). Actual XLRE −0.9536%, SPY +0.6553%, rel −1.6089% (dir MISS, mag HIT). Morning used prior-close 10Y 4.64% (−6 bp) / 30Y 5.17% (−6 bp) / DFII10 2.32% (−6 bp) plus CL −1.11%. Session reversed: 10Y 4.67% (+3 bp), 30Y 5.19% (+2 bp), WTI +1.8% to $83.67; equity REITs −1.0%, housing −1.1%, S&P +0.7%. PCE 3.7/3.3 was already released 8/26; Warsh JH was still 8/28. Open-known facts already supported flat/down-mild.

(learn_cycle promote)
