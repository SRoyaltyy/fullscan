# Book learn — weight tuner ledger (v15)

Updated: **2026-09-17T07:14:42.975236-04:00** · evaluation as of **2026-09-17**

Objective: mean forward return of the top-10 buy book **in excess of the
liquid-universe median**, walk-forward on fully-realized dates only.
Guardrails: ≥5 dates, ≥0.05pp improvement, wins on ≥60% of dates, half-step adoption, ±0.12 drift cap vs code defaults.

| Horizon | dates | incumbent excess | best excess | decision |
|---------|-------|------------------|-------------|----------|
| 1d | 24 | -0.1704 | -0.0822 | hold — wins only 29% of dates (< 60%) |
| 3d | 20 | -0.5003 | -0.459 | hold — improvement 0.041pp < 0.05pp |
| 1w | 18 | -0.4702 | -0.1884 | hold — wins only 28% of dates (< 60%) |
| 2w | 11 | 0.8107 | 0.9749 | hold — wins only 27% of dates (< 60%) |
| 1m | 3 | — | — | observe — only 3 realized dates (< 5) |

## Adopted weights (join / sector / general / news / AB / peer)

| Horizon | adopted | code default |
|---------|---------|--------------|
| 1d | [0.12, 0.1, 0.08, 0.25, 0.25, 0.2] | [0.12, 0.1, 0.08, 0.25, 0.25, 0.2] |
| 3d | [0.16, 0.14, 0.08, 0.16, 0.26, 0.2] | [0.16, 0.14, 0.08, 0.16, 0.26, 0.2] |
| 1w | [0.18, 0.16, 0.08, 0.1, 0.28, 0.2] | [0.18, 0.16, 0.08, 0.1, 0.28, 0.2] |
| 2w | [0.2, 0.18, 0.08, 0.06, 0.28, 0.2] | [0.2, 0.18, 0.08, 0.06, 0.28, 0.2] |
| 1m | [0.22, 0.2, 0.08, 0.0, 0.3, 0.2] | [0.22, 0.2, 0.08, 0.0, 0.3, 0.2] |

## Sell-book construction

- core=0.236pp full=0.777pp → sell_excludes_addons=False (n=18)

## Risk-off entry scaling (LLM weather call → sizing action)

- scale: **0.5** (effective 2026-08-25) — book loses -0.33% on risk-off days → keep entry scale 0.5

## Map/captain heat scale (realized 1d excess return)

- scale: **0.25** — hold 0.25 — best 0.00, improvement 0.048pp, wins 25%

## History

- v15 @ 2026-09-17: 1d: hold — wins only 29% of dates (< 60%); 3d: hold — improvement 0.041pp < 0.05pp; 1w: hold — wins only 28% of dates (< 60%); 2w: hold — wins only 27% of dates (< 60%); 1m: observe — only 3 realized dates (< 5)
- v14 @ 2026-09-16: 1d: hold — wins only 25% of dates (< 60%); 3d: hold — improvement 0.047pp < 0.05pp; 1w: hold — wins only 22% of dates (< 60%); 2w: hold — wins only 27% of dates (< 60%); 1m: observe — only 3 realized dates (< 5)
- v13 @ 2026-09-11: 1d: hold — wins only 33% of dates (< 60%); 3d: hold — wins only 21% of dates (< 60%); 1w: hold — wins only 33% of dates (< 60%); 2w: hold — wins only 43% of dates (< 60%); 1m: observe — only 0 realized dates (< 5)
- v12 @ 2026-09-10: 1d: hold — wins only 29% of dates (< 60%); 3d: hold — wins only 23% of dates (< 60%); 1w: hold — wins only 36% of dates (< 60%); 2w: hold — wins only 43% of dates (< 60%); 1m: observe — only 0 realized dates (< 5)
- v11 @ 2026-09-03: 1d: hold — wins only 33% of dates (< 60%); 3d: hold — wins only 10% of dates (< 60%); 1w: hold — wins only 38% of dates (< 60%); 2w: hold — wins only 50% of dates (< 60%); 1m: observe — only 0 realized dates (< 5)
- v10 @ 2026-09-02: 1d: hold — wins only 27% of dates (< 60%); 3d: hold — wins only 12% of dates (< 60%); 1w: hold — wins only 29% of dates (< 60%); 2w: hold — wins only 40% of dates (< 60%); 1m: observe — only 0 realized dates (< 5)
- v9 @ 2026-09-02: 1d: hold — wins only 27% of dates (< 60%); 3d: hold — wins only 12% of dates (< 60%); 1w: hold — wins only 29% of dates (< 60%); 2w: hold — wins only 40% of dates (< 60%); 1m: observe — only 0 realized dates (< 5)
- v8 @ 2026-09-01: 1d: hold — wins only 30% of dates (< 60%); 3d: hold — wins only 12% of dates (< 60%); 1w: hold — wins only 29% of dates (< 60%); 2w: observe — only 4 realized dates (< 5); 1m: observe — only 0 realized dates (< 5)
- v7 @ 2026-09-01: 1d: hold — wins only 30% of dates (< 60%); 3d: hold — improvement 0.042pp < 0.05pp; 1w: hold — wins only 29% of dates (< 60%); 2w: observe — only 4 realized dates (< 5); 1m: observe — only 0 realized dates (< 5)
- v6 @ 2026-08-31: 1d: hold — wins only 43% of dates (< 60%); 3d: hold — wins only 14% of dates (< 60%); 1w: hold — wins only 33% of dates (< 60%); 2w: observe — only 2 realized dates (< 5); 1m: observe — only 0 realized dates (< 5)