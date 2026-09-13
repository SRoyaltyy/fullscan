# Pixel descriptors — read off Simple View--Calculation.xlsx

The live file is Sheet1 **A1:JO364**: 275 letters, 35,769 formulas, CF
fills. This factory is the high-definition paint of that sheet. `NOTES.md`
+ `CLOCK_MAP.md` + the noodle screenshot (row 1 headers A–O, DF7 `IFERROR(`
candle block, green/red H/I regions) are the read.

Clock is unchanged. Same-row H/I never leave as features. Same-row
DF/DG/DH/BB/BQ/BU never leave. FDR + holdout + Apriori still sit on top.

## What a column actually is (from the replica + the screenshot)

| letters | on the sheet | mined as |
|---|---|---|
| A | date serial + regime fill | fill lag 0; number skipped (date) |
| B | close + open fill | fill lag 0; **number is close** → lag 1+ |
| C | open | fill + number lag 0 |
| D E F | high / low / volume | close same-row → lag 1+ |
| G | Vol ratio (Q warmup uses G>2.5) | fill lag 0; number lag 1+; cuts 1/1.5/2/2.5/3/5 |
| H | Intraday `(high-low)/open` | **label on row T**; feature T−1+; cuts 2/3/5/8/12% |
| I | Daily `close/prev−1` | **label on row T**; feature T−1+; the green-region claim |
| J | open-to-open (header also Range) | number + fill lag 0; cuts 1/2/3/5% |
| K L M | Body / Candl / Boomer | fill lag 0; numbers lag 1+ |
| N | Yesterday | lag 1+ |
| O | Volume sls | fill lag 0; number lag 1+ |
| P–Y | small-int tallies, flags, `#VALUE!` | exist/blank/err + discrete ==n |
| Q Z AC AH BT BV … JL | locked 44 open values | number lag 0; AH/FR/FQ/ER/EP/EN/JB/JC cuts |
| DF DG DH | 1-bar / 2-bar / 3-bar candle **text** | exact token + bull/bear/doji/hammer/engulf/star family, lag 1+ |
| BB BQ BU | doji / money flow / typical sign | lag 1+ |
| IR IS IT | STOCKHISTORY date/close/open spill | fill lag 0; IT/IR number lag 0; IS close |

## Pixels per letter (legal lag L in {0,1,2,3,5})

- `X@L_present` / `_blank` / `_exists` / `_err` / `_missing`
- `X@L_g` / `_r` / `_deepg` / `_deepr` / `_none`
- `X@L_pos` / `_neg` / `_zero` / `_nonzero`
- `X@L>p90` `>p75` `>p50` `<p25` `<p10` (discovery quantiles, frozen)
- sheet-native cuts: `G@L>2.5`, `H@L>=0.05`, `I@L>0.03`, `AH@L==2`, …
- `X@L=<text>` plus candle tags `_bull _bear _doji _hammer _engulf _star`
- paint/sign streaks `>=2,3,4,5,8,12` and `==1` (just started)
- windows 3/5/10/20/40: green/red counts, deep counts, max vs |min|, last-is-extrema, both signs
- `ratio_X/C`, `X/J`, `X/H`, `X/G`, `X/I` at `>1 >1.5 >2 <0.5 <0`
- neighbor pairs on A–O (`neigh_HI@L1_both_g` style)

## Horizontal (in the past X columns)

- bands A–O, P–Z, AA–AZ, BA–BZ, CA–CZ, DA–DH, EA–EZ, FA–FZ, GA–GZ, HA–HZ, IA–JO
- sliding 3/5/8/15-column windows across the A–O camera strip
- whole-camera density: `camera@L_g>=10`, majority, all-green, max vs |min|

## Region claim (the screenshot H/I cluster)

- `HI@L_both_g` / `_both_r` / `_split` / `_deepg`
- `Iregion_g>=5`, `Iregion_deepg` (streak >=5 or >=3 and run-sum I >= 8%)
- `I_broke_green`, `I_flipped_to_g` — the run just ended or flipped
- open gap `gap_C/B>0.01` (today's open vs yesterday's close)

A session typically lights **~8k–10k true pixels**. The miner still
keeps only FDR q=0.10 survivors that also lift on holdout.

```
python excel_bot/engine/excel_stat_mine_pixel_hook.py --grids grids_deep
```
