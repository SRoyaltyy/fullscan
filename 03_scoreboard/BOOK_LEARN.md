# Book learn — weight tuner ledger (v20)

Updated: **2026-11-12T08:31:08.801247-05:00** · evaluation as of **2026-11-12**

Objective: mean forward return of the top-10 buy book **in excess of the
liquid-universe median**, walk-forward on fully-realized dates only.
Guardrails: ≥5 dates, ≥0.05pp improvement, wins on ≥60% of dates, half-step adoption, ±0.12 drift cap vs code defaults.

| Horizon | dates | incumbent excess | best excess | decision |
|---------|-------|------------------|-------------|----------|
| 1d | 38 | 0.4254 | 0.5231 | hold — wins only 24% of dates (< 60%) |
| 3d | 36 | 1.5937 | 1.8303 | hold — wins only 42% of dates (< 60%) |
| 1w | 34 | 3.3362 | 3.3871 | hold — wins only 24% of dates (< 60%) |
| 2w | 29 | 6.1664 | 6.4736 | hold — wins only 38% of dates (< 60%) |
| 1m | 17 | 10.9941 | 11.5596 | hold — wins only 47% of dates (< 60%) |

## Adopted weights (join / sector / general / news / AB / peer)

| Horizon | adopted | code default |
|---------|---------|--------------|
| 1d | [0.12, 0.1, 0.08, 0.25, 0.25, 0.2] | [0.12, 0.1, 0.08, 0.25, 0.25, 0.2] |
| 3d | [0.16, 0.14, 0.08, 0.16, 0.26, 0.2] | [0.16, 0.14, 0.08, 0.16, 0.26, 0.2] |
| 1w | [0.18, 0.16, 0.08, 0.1, 0.28, 0.2] | [0.18, 0.16, 0.08, 0.1, 0.28, 0.2] |
| 2w | [0.2, 0.18, 0.08, 0.06, 0.28, 0.2] | [0.2, 0.18, 0.08, 0.06, 0.28, 0.2] |
| 1m | [0.22, 0.2, 0.08, 0.0, 0.3, 0.2] | [0.22, 0.2, 0.08, 0.0, 0.3, 0.2] |

## Sell-book construction

- core=2.180pp full=2.229pp → sell_excludes_addons=False (n=34)

## Risk-off entry scaling (LLM weather call → sizing action)

- scale: **1.0** (effective 2026-08-25) — book still makes +0.76% on risk-off days → scale back to 1.0 (cash drag not justified)

## Map/captain heat scale (realized 1d excess return)

- scale: **0.25** — hold 0.25 — best 1.00, improvement 0.151pp, wins 36%

## History

- v20 @ 2026-11-12: 1d: hold — wins only 24% of dates (< 60%); 3d: hold — wins only 42% of dates (< 60%); 1w: hold — wins only 24% of dates (< 60%); 2w: hold — wins only 38% of dates (< 60%); 1m: hold — wins only 47% of dates (< 60%)
- v19 @ 2026-11-03: 1d: hold — wins only 37% of dates (< 60%); 3d: hold — wins only 36% of dates (< 60%); 1w: hold — wins only 26% of dates (< 60%); 2w: hold — wins only 34% of dates (< 60%); 1m: hold — wins only 47% of dates (< 60%)
- v18 @ 2026-10-29: 1d: hold — wins only 37% of dates (< 60%); 3d: hold — wins only 33% of dates (< 60%); 1w: hold — wins only 26% of dates (< 60%); 2w: hold — wins only 29% of dates (< 60%); 1m: hold — wins only 53% of dates (< 60%)
- v17 @ 2026-10-26: 1d: hold — wins only 37% of dates (< 60%); 3d: hold — wins only 36% of dates (< 60%); 1w: hold — wins only 30% of dates (< 60%); 2w: hold — wins only 32% of dates (< 60%); 1m: hold — wins only 25% of dates (< 60%)
- v16 @ 2026-10-23: 1d: hold — wins only 39% of dates (< 60%); 3d: hold — wins only 36% of dates (< 60%); 1w: hold — wins only 30% of dates (< 60%); 2w: hold — wins only 25% of dates (< 60%); 1m: hold — wins only 25% of dates (< 60%)
- v15 @ 2026-09-17: 1d: hold — wins only 29% of dates (< 60%); 3d: hold — improvement 0.041pp < 0.05pp; 1w: hold — wins only 28% of dates (< 60%); 2w: hold — wins only 27% of dates (< 60%); 1m: observe — only 3 realized dates (< 5)
- v14 @ 2026-09-16: 1d: hold — wins only 25% of dates (< 60%); 3d: hold — improvement 0.047pp < 0.05pp; 1w: hold — wins only 22% of dates (< 60%); 2w: hold — wins only 27% of dates (< 60%); 1m: observe — only 3 realized dates (< 5)
- v13 @ 2026-09-11: 1d: hold — wins only 33% of dates (< 60%); 3d: hold — wins only 21% of dates (< 60%); 1w: hold — wins only 33% of dates (< 60%); 2w: hold — wins only 43% of dates (< 60%); 1m: observe — only 0 realized dates (< 5)
- v12 @ 2026-09-10: 1d: hold — wins only 29% of dates (< 60%); 3d: hold — wins only 23% of dates (< 60%); 1w: hold — wins only 36% of dates (< 60%); 2w: hold — wins only 43% of dates (< 60%); 1m: observe — only 0 realized dates (< 5)
- v11 @ 2026-09-03: 1d: hold — wins only 33% of dates (< 60%); 3d: hold — wins only 10% of dates (< 60%); 1w: hold — wins only 38% of dates (< 60%); 2w: hold — wins only 50% of dates (< 60%); 1m: observe — only 0 realized dates (< 5)