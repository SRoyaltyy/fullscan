# Book learn — weight tuner ledger (v2)

Updated: **2026-08-22T23:29:41.323528-04:00** · evaluation as of **2026-08-23**

Objective: mean forward return of the top-10 buy book **in excess of the
liquid-universe median**, walk-forward on fully-realized dates only.
Guardrails: ≥5 dates, ≥0.05pp improvement, wins on ≥60% of dates, half-step adoption, ±0.12 drift cap vs code defaults.

| Horizon | dates | incumbent excess | best excess | decision |
|---------|-------|------------------|-------------|----------|
| 1d | 6 | -0.2129 | -0.1122 | hold — wins only 50% of dates (< 60%) |
| 3d | 4 | — | — | observe — only 4 realized dates (< 5) |
| 1w | 2 | — | — | observe — only 2 realized dates (< 5) |
| 2w | 0 | — | — | observe — only 0 realized dates (< 5) |
| 1m | 0 | — | — | observe — only 0 realized dates (< 5) |

## Adopted weights (join / sector / general / news / AB / peer)

| Horizon | adopted | code default |
|---------|---------|--------------|
| 1d | [0.12, 0.1, 0.08, 0.25, 0.25, 0.2] | [0.12, 0.1, 0.08, 0.25, 0.25, 0.2] |
| 3d | [0.16, 0.14, 0.08, 0.16, 0.26, 0.2] | [0.16, 0.14, 0.08, 0.16, 0.26, 0.2] |
| 1w | [0.18, 0.16, 0.08, 0.1, 0.28, 0.2] | [0.18, 0.16, 0.08, 0.1, 0.28, 0.2] |
| 2w | [0.2, 0.18, 0.08, 0.06, 0.28, 0.2] | [0.2, 0.18, 0.08, 0.06, 0.28, 0.2] |
| 1m | [0.22, 0.2, 0.08, 0.0, 0.3, 0.2] | [0.22, 0.2, 0.08, 0.0, 0.3, 0.2] |

## Sell-book construction

- hold True — only 2 dates (n=2)

## Risk-off entry scaling (LLM weather call → sizing action)

- scale: **0.5** (effective 2026-08-25) — hold scale 0.5 — only 3 realized risk-off dates (< 4)

## History

- v2 @ 2026-08-23: 1d: hold — wins only 50% of dates (< 60%); 3d: observe — only 4 realized dates (< 5); 1w: observe — only 2 realized dates (< 5); 2w: observe — only 0 realized dates (< 5); 1m: observe — only 0 realized dates (< 5)
- v1 @ 2026-08-22: 1d: hold — wins only 50% of dates (< 60%); 3d: observe — only 4 realized dates (< 5); 1w: observe — only 2 realized dates (< 5); 2w: observe — only 0 realized dates (< 5); 1m: observe — only 0 realized dates (< 5)