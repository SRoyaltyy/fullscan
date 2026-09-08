# Research cards — shade hex open entry (same-day H)

_Generated 2026-09-08 · expand tip `35cbab9d` / `f13adab7` · **research cards only** · live `flatten_robust` frozen · **not** under `strategies/` · **all cards DEMOTE** · shade cut **exhausted** (KEEP 0 after leak fix).

## Plain English

Expanded open-only mid-M `#95CA82` on **5223** names / **2018→2026** / **8.54M** name-days is **DEMOTE** (holdout **−0.19%** after fees, n≈320k; loses to book; ghost **FAIL**).

The prior ~**+10.6%** / **4.4k** KEEP was an **H-fill index leak** (letter M accidentally reading H’s fill = same-day `H≥5%` as both feature and label). That path is aborted.

Real M fill (IZ from prior-row H) does not pay. Shade cut on this tape is **exhausted** (KEEP 0 after leak fix).

O onset stays demoted. M hex-onset stays demoted with the family.

| panel | value |
|---|---|
| tickers / grids | **5223** |
| date range | **2018-09-10 → 2026-09-04** |
| name-days | **8,537,720** |
| M `#95CA82` fires (holdout / discovery) | **320,499 / 478,008** |
| buy-everyone book (holdout H) | −0.10% (n=3,421,306) |

## Why demoted

| card | why | cite |
|---|---|---|
| M mid `#95CA82` / ge15 | Real M **fill** at open: holdout −0.19% (n=320499), loses to book (−0.09 pp), ghost FAIL. Not the 4.4k H-fill leak. | expand `35cbab9d` |
| M hex-onset `#95CA82` | Same family. Holdout −0.23% (n=250101). Demoted with the parent. | expand `35cbab9d` |
| O red→green onset | Prior 1000-grid ghost FAIL. Not re-painted on the M-only expand (O fills absent). Stays demoted. | expand `35cbab9d` |
| M fill × open 44 / lags | KEEP 0 / KILL 24 / family null when `SHADE_GATE.md` is present (later clock-gate tip). Not a KEEP on this tape. | `SHADE_GATE.md` if present |

## Card 1 — M mid-green `#95CA82`

| field | value |
|---|---|
| name | `research_M_hex_95CA82_1d_H` |
| recipe | morning M **fill** hex `#95CA82` (mid green / score ≥ 1.5). Not M’s number. |
| entry | open |
| label | H (intraday %, same day) — outcome, not a feature |
| horizon | 1d |
| holdout | **−0.19%** (n=320499) |
| discovery | −0.19% (n=478008) |
| vs book | **−0.09 pp** (loses) |
| vs any-green M | **+0.00 pp** (same trades — real M has one green hex) |
| time holdout | early −0.18% · late −0.41% (cut 2026-05-01); both red |
| Q1 | −0.20% (n=299468) red |
| SPY↑ / ↓ | +0.27% (does not beat that tape’s book) / **−0.74%** red |
| drop-5 leftover | −0.20% (n=319875) |
| codes | `M_hex_95CA82`, `M_ge15` (identical on this dump) |
| verdict | **DEMOTE** · **GHOST FAIL** · family **null** |
| status | retired research · **DEMOTE** · **demoted** · not live |

## Card 2 — M mid-green onset `#95CA82`

| field | value |
|---|---|
| name | `research_M_onset_hex_95CA82_1d_H` |
| recipe | M fill flips onto `#95CA82` today (yesterday’s fill is the lag) |
| entry | open |
| label | H |
| holdout | **−0.23%** (n=250101) |
| vs book | −0.13 pp |
| vs any-green M | −0.04 pp |
| verdict | **DEMOTE** · **GHOST FAIL** · demoted with the family |
| status | retired research · **DEMOTE** · **demoted** · not live |

## Card 3 — O red→green onset

| field | value |
|---|---|
| name | `research_O_onset_red2green_1d_H` |
| recipe | morning O flips from red yesterday to green today |
| entry | open |
| label | H |
| this expand | O fills absent (M-only paint) — **THIN** here |
| prior 1000-grid | ghost **FAIL** |
| verdict | **DEMOTE** · stays demoted |
| status | retired research · **DEMOTE** · **demoted** · not live |

## Open-only gate (leak aborted)

Every feature is knowable at that day’s open:

- M **fill** `#95CA82` only. CF uses prior-row H + IY (lag).
- M **value** `-(E-C)/C` is close-only — excluded.
- No same-row H/I value, fill, text, or transform as a feature. H is the label.
- The 4.4k / +10.6% KEEP indexed `OPEN_FILL` so M read **H’s fill**. Aborted.
- Soft-regime heat = prior-5 I (`[ei-5, ei)`). SPY tape is a ship-bar slice, not a buy feature.

## Explicitly not a live wire

Live `flatten_robust` stays frozen. No card under `strategies/`. Soft-regime majority is N/A (no H KEEP candidates). Pale `#DCEDD5` on the old “M” dump was H’s fill from the index leak. Shade KEEP on this tape is exhausted.

## Source

`excel_bot/research/SHADE_OPEN.md` · `shade_panel_stats.json` · PR #150 · expand tip `35cbab9d`. `SHADE_GATE.md` if present. Research only. Live frozen.

## 2d / 3d stacked I recut (follow-on)

_Generated 2026-09-08 · tip `2899245a` · **research cards only** · live `flatten_robust` frozen · family **null**._

Primary labels are HI_HORIZON `I_sum` 2d and 3d (compound daily I). Same-day H is baseline only. Same expand panel as #150/#151 (5193 names, 2018-09-04 → 2026-09-04).

**Cyrus (visual):** Deep green/red look predictive because the loudest paints are same-row outcome color: Excel CF paints H>5% `#95CA82` and H>0 pale `#DCEDD5` (I>3% mint) after the close — that is H/I itself, not an open forecast; the one open-knowable mid-green (M `#95CA82` from yesterday's H × IY) is not that paint.

| field | value |
|---|---|
| family | **null** |
| panel | 5193 names / 8,543,587 name-days |
| live | frozen (`flatten_robust`) |

| recipe | label | holdout | vs book | vs parent | verdict |
|---|---|---|---|---|---|
| `M_ge15` | I_sum@2d | -0.08% (n=318151) | -0.29 pp | +0.00 pp | **KILL** |
| `M_green` | I_sum@2d | -0.08% (n=318151) | -0.29 pp | — | **KILL** |
| `M_hex_95CA82` | I_sum@2d | -0.08% (n=318151) | -0.29 pp | +0.00 pp | **KILL** |
| `M_onset_hex_95CA82` | I_sum@2d | -0.13% (n=249061) | -0.34 pp | -0.05 pp | **KILL** |
| `M_ge15` | I_sum@3d | -0.06% (n=318145) | -0.41 pp | +0.00 pp | **KILL** |
| `M_green` | I_sum@3d | -0.06% (n=318145) | -0.41 pp | — | **KILL** |
| `M_hex_95CA82` | I_sum@3d | -0.06% (n=318145) | -0.41 pp | +0.00 pp | **KILL** |
| `M_onset_hex_95CA82` | I_sum@3d | -0.15% (n=249056) | -0.51 pp | -0.09 pp | **KILL** |

Status: retired research · not live. Must beat any-green parent on the same letter, not just the book.
