# Region / H–I correlation mine

Research only. Live `flatten_robust` is not imported or written.

## What this answers

From the spreadsheet screenshot: lower rows are later days. Green paint clusters.
The claim to test is **not** “I know the exact day the pop happens.” It is:

> If rows *above* T are already a deep-green I region, then rows *below* T
> keep printing green I more often, and the green prints are larger in H
> (intraday range) and |I| (daily move), until the region breaks.

Same scan in red regions (more red, smaller opposing greens).

Time horizon is free. The miner scores:

| bucket | meaning |
|---|---|
| `h1` `h3` `h5` `h10` | next 1 / 3 / 5 / 10 completed sessions after the 09:30 snapshot |
| `hold_green_region` | buy T’s open, hold until the first forward close with `I ≤ 0` |
| `hold_red_region` | the red-side mirror |

## Clock (hard)

For session T:

**Legal features**
- every letter / fill / text on rows `< T`
- same-row open fills: `A B C G J K L M O IR IS IT`
- same-row open-44 values (AH, FR, FQ, J, …)
- today’s **open** (column C / J)

**Labels only (never features on T)**
- `H[T]`, `I[T]`, D/E/F/N, `core_score`
- DF / DG / DH / BB / BQ on row T — close same-row. The miner uses
  `excel_open_features` which computes those on **prior** bars only.

`excel_clock_gate.assert_feature_legal` + `assert_excel_clock_gate` run
before a live scan.

## Column meanings used here

- **I** = `close[T] / close[T−1] − 1` (Daily)
- **H** = `(high[T] − low[T]) / open[T]` (Intraday range strength)
- green I day = `I > 0`
- red I day = `I < 0`

## Deep sheet mine + GitHub Action

The I-streak pass is the narrow test. The full-sheet miner is
`engine/excel_deep_corr_mine.py`. It walks every emulator column the
clock allows:

- same-row open fills A B C G J K L M O green/red
- last 5 / 10 / 20 rows of every A–O letter: green-count, red-count, green>red
- current A-region (already in a green or red paint run)
- lag-1 DF / DG / DH candles
- open-44 ratios (`C/B`, `J/C`, `FR/AH`, …) when the grid stored values
- pairwise AND of the holdout-surviving singles

Universe: Finviz market cap > $50M and average volume > 100k shares.
Tape: `grids_deep/` (multi-year if those grids are). A rule is kept only
when discovery and holdout both lift green density the same way.

```
python engine/excel_deep_corr_mine.py --grids grids_deep --min-disc 200
```

Action: `.github/workflows/excel_deep_corr_mine.yml` (`workflow_dispatch`
+ Saturday cron). Overlays `excel-state` grids when the checkout does
not have them. Writes `03_scoreboard/EXCEL_DEEP_CORR_MINE.md`.

## What this is not

Not XGBoost. Not a 1-day SPY call. Not a live book wire.
It is an exhaustive-but-whitelisted frequency table over the emulator
grids, with the same clock the rest of `excel_bot` already locked.
