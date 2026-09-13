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

These match the noodle headers in the screenshot, rebuilt from OHLCV
when a grid has no cached H/I:

- **I** = `close[T] / close[T−1] − 1` (Daily)
- **H** = `(high[T] − low[T]) / open[T]` (Intraday range strength)
- green I day = `I > 0`
- red I day = `I < 0`
- **streak** at T = consecutive signed I days on rows *above* T
- **deep green** = streak ≥ 5 *or* streak ≥ 3 and run-sum I ≥ 8%

Text columns **DF / DG / DH** (candle names) enter as lag-1 flags:
Hammer, Engulfing, Morning/Evening Star, Doji, bullish/bearish net.

## How to run (from `excel_bot/`)

```
python engine/region_corr_mine.py --grids grids --min-n 80
python engine/test_region_corr_mine.py
```

Deep history if present:

```
python engine/region_corr_mine.py --grids grids_deep --min-n 200 \
  --out-md research/REGION_CORR_MINE.md \
  --out-csv research/region_corr_mine.csv
```

## How to read a row

- `lift_green` > 1: the IF fires, then forward I is green more often than the
  unconditional tape.
- `lift_abs_I` / `lift_H` > 1: the moves are *bigger*, not just more often green.
- `hold` / `lift_hold`: open-of-T → last close of that bucket.
- `g_streak>=8` on `hold_green_region` is the screenshot claim in one line.
- `r_streak>=5` on `h5` should raise `red_density` and shrink opposing greens
  if the paint-regime idea is real.

A rule that needs tiny `n` to look like 80% is a story. Keep `min-n` high.

## What this is not

Not XGBoost. Not a 1-day SPY call. Not a live book wire.
It is an exhaustive-but-whitelisted frequency table over the emulator
grids, with the same clock the rest of `excel_bot` already locked.
