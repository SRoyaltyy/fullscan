# Book learn — weight tuner ledger (v6)

Updated: **2026-08-31T19:16:35.860422-04:00** · evaluation as of **2026-08-31**

Objective: mean forward return of the top-10 buy book **in excess of the
liquid-universe median**, walk-forward on fully-realized dates only.
Guardrails: ≥5 dates, ≥0.05pp improvement, wins on ≥60% of dates, half-step adoption, ±0.12 drift cap vs code defaults.

| Horizon | dates | incumbent excess | best excess | decision |
|---------|-------|------------------|-------------|----------|
| 1d | 7 | -0.4569 | -0.1884 | hold — wins only 43% of dates (< 60%) |
| 3d | 7 | -0.5239 | -0.401 | hold — wins only 14% of dates (< 60%) |
| 1w | 6 | -0.0641 | 0.3232 | hold — wins only 33% of dates (< 60%) |
| 2w | 2 | — | — | observe — only 2 realized dates (< 5) |
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

- core=-2.773pp full=-0.992pp → sell_excludes_addons=False (n=6)

## Risk-off entry scaling (LLM weather call → sizing action)

- scale: **1.0** (effective 2026-08-25) — book still makes +0.25% on risk-off days → scale back to 1.0 (cash drag not justified)

## Map/captain heat scale (realized 1d excess return)

- scale: **0.25** — incubate 0.25 — only 0 realized heat dates (< 5)

## History

- v6 @ 2026-08-31: 1d: hold — wins only 43% of dates (< 60%); 3d: hold — wins only 14% of dates (< 60%); 1w: hold — wins only 33% of dates (< 60%); 2w: observe — only 2 realized dates (< 5); 1m: observe — only 0 realized dates (< 5)
- v5 @ 2026-08-31: 1d: hold — wins only 43% of dates (< 60%); 3d: hold — wins only 29% of dates (< 60%); 1w: hold — wins only 17% of dates (< 60%); 2w: observe — only 1 realized dates (< 5); 1m: observe — only 0 realized dates (< 5)
- v4 @ 2026-08-30: 1d: hold — wins only 43% of dates (< 60%); 3d: hold — wins only 29% of dates (< 60%); 1w: hold — wins only 17% of dates (< 60%); 2w: observe — only 1 realized dates (< 5); 1m: observe — only 0 realized dates (< 5)
- v3 @ 2026-08-27: 1d: hold — wins only 43% of dates (< 60%); 3d: hold — wins only 29% of dates (< 60%); 1w: hold — wins only 20% of dates (< 60%); 2w: observe — only 0 realized dates (< 5); 1m: observe — only 0 realized dates (< 5)
- v2 @ 2026-08-23: 1d: hold — wins only 50% of dates (< 60%); 3d: observe — only 4 realized dates (< 5); 1w: observe — only 2 realized dates (< 5); 2w: observe — only 0 realized dates (< 5); 1m: observe — only 0 realized dates (< 5)
- v1 @ 2026-08-22: 1d: hold — wins only 50% of dates (< 60%); 3d: observe — only 4 realized dates (< 5); 1w: observe — only 2 realized dates (< 5); 2w: observe — only 0 realized dates (< 5); 1m: observe — only 0 realized dates (< 5)