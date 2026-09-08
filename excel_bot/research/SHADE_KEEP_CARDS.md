# Research cards — shade hex open entry (same-day H)

_Generated 2026-09-08 · tip `35cbab9d` · **research cards only** · live `flatten_robust` frozen · **not** under `strategies/` · expanded-universe open-only re-score of #150 **landed**: family **null** · **DEMOTE**.

## Plain English

**DEMOTE.** Standing M mid-green fill `#95CA82` / ge15 does **not** hold once the feature is actually column M’s **fill** at the open, on all stocks × all Yahoo/excel_bot history.

The 1000-grid / **4.4k** KEEP (`+10.57%`) was a close-knowable leak: `enumerate(OPEN_FILL)` made letter M read `fills[7]` = **H**. That is same-day `H≥5%` as both feature and label. Aborted.

This cut paints real M from locked CF (`IZ=1` → `#95CA82`: *prior-row* H > 0 and IY = 1 — a lag, known at 9:30). M’s **number** (same-day low wick) is never a feature. Excel vs fast M match **12048/12048**.

| panel | value |
|---|---|
| tickers with rows / grids | **5223** / 5223 |
| date range | **2018-09-10 → 2026-09-04** |
| median / max sessions | 2008 / 2008 |
| panel name-days | **8,537,720** |
| M `#95CA82` fires (holdout / discovery) | **320,499 / 478,008** (≫ 4.4k) |
| buy-everyone book (holdout H) | −0.10% (n=3,421,306) |

## Card 1 — M mid-green `#95CA82` — DEMOTE

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
| status | research card · **demoted** · not live |

## Card 2 — M mid-green onset `#95CA82` — DEMOTE

| field | value |
|---|---|
| name | `research_M_onset_hex_95CA82_1d_H` |
| recipe | M fill flips onto `#95CA82` today (yesterday’s fill is the lag) |
| entry | open |
| label | H |
| holdout | **−0.23%** (n=250101) |
| vs book | −0.13 pp |
| vs any-green M | −0.04 pp |
| verdict | **DEMOTE** · **GHOST FAIL** |
| status | research card · **demoted** · not live |

## Card 3 — O red→green onset

Not re-painted on this M-only expand (O fills absent). Prior ghost FAIL on the 1000-grid cut stands. **THIN** here.

## Excel clock gate (source of truth)

`OPEN_SAME_ROW_LABELS.md` + `CLOCK_MAP.md`. Enforce exactly:

- Shades/fills at open: only **A B C G J K L M O IR IS IT**
- Numbers/text at open: only the **44 `value_mine_open` cols** (never same-row H/I)
- Lags: any letter from rows above is fair
- OUT: M’s number, B/G/K/M/O numbers, D/E/F/H/I same-row, `core_score`

Mid-M `#95CA82` expand stays **open-fill only** under this gate. Pairs with the locked 44 / lags: `SHADE_GATE.md`.

## Open-only gate

Every feature is knowable at that day’s open:

- M **fill** `#95CA82` only. CF uses prior-row H + IY (lag).
- M **value** `-(E-C)/C` is close-only — excluded.
- No same-row H/I value, fill, text, or transform as a feature. H is the label.
- Soft-regime heat = prior-5 I (`[ei-5, ei)`). SPY tape is a ship-bar slice, not a buy feature.

## Explicitly not a live wire

Live `flatten_robust` stays frozen. No card under `strategies/`. Soft-regime majority is N/A (no H KEEP candidates). Pale `#DCEDD5` on the old “M” dump was H’s fill from the index leak.

## Source

`excel_bot/research/SHADE_OPEN.md` · `shade_panel_stats.json` · PR #150. Gate: `OPEN_SAME_ROW_LABELS.md` + `CLOCK_MAP.md`. Pairs: `SHADE_GATE.md`. Research only. Live frozen.
