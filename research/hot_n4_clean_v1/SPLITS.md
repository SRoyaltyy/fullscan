# Splits in the hot_n4_clean_v1 candidate universe

Window: NYSE sessions 2026-08-13 through 2026-09-25.

The candidate universe is every ticker that passes the liquid gate on at least one frozen Theme Radar row whose `trade_date` is one of those sessions. The gate is `Ticker` present, `Industry` not `Exchange Traded Fund`, `Market Cap` at least 100 (millions of dollars), `Average Volume` at least 500 (thousands of shares), and `Volume` greater than 0. That set has 2,809 names. `2026-08-28` has no frozen row and adds nobody.

Split factors come from `data/factor_mine/retro_prices/actions.parquet` sha256 `9007c3aec0db3fa42735bf10ad8611e19ba78a2d532cec0df8f99205fbd6b633` (git blob `b583117809d0ccac78b766084d5cb04ca6d6dec3`). The stored `split` column is Yahoo's share factor. The price factor is one divided by that share factor. `data/prices/actions.parquet` sha256 `0471b2d76c30960eb494134a7bd34eea499bb5c696190f5fb3628c4becd94e4a` has no nonzero split in this window, so it is not the explanation table.

`boundary` is the adjustment date. Bars strictly before `boundary` are the old price level. OHLC on those bars is multiplied by the price factor, and volume is multiplied by the share factor, before any return, hot score, rvol, candle, or breakout. Bars on or after `boundary` are left as stored. Dividends are not applied. A 3x jump uses the jumping bar as `boundary` when the recorded ex-date is different. A split that does not trip the 3x check uses the recorded ex-date.

| ex-date | ticker | share factor | price factor | boundary | what it is |
| --- | --- | --- | --- | --- | --- |
| 2026-08-14 | BYND | 1/30 (`0.03333333333333333`) | 30 | 2026-08-17 | reverse split. No stored bar on the ex-date. The next stored bar, 2026-08-17, carries the jump. |
| 2026-08-17 | AVB | 2.793 | 1/2.793 | 2026-08-17 | not an integer split. Does not trip the 3x check. |
| 2026-08-19 | NRDY | 1/15 (`0.06666666666666667`) | 15 | 2026-08-17 | reverse split. The 3x leg is the 2026-08-17 open over the 2026-08-14 close. |
| 2026-08-21 | SFBS | 2 | 1/2 | 2026-08-21 | 2-for-1. Not liquid on 2026-08-13 (average volume under 500). Liquid later in the window. |
| 2026-08-24 | BRCC | 1/10 (`0.1`) | 10 | 2026-08-20 | reverse split. The 3x leg is the 2026-08-20 open over the 2026-08-19 close. |
| 2026-08-25 | REAX | 1/10 (`0.1`) | 10 | 2026-08-20 | reverse split. The 3x leg is the 2026-08-20 open over the 2026-08-19 close. |
| 2026-09-01 | RUSHA | 3/2 (`1.5`) | 2/3 | 2026-09-01 | does not trip the 3x check. |
| 2026-09-03 | APH | 2 | 1/2 | 2026-09-03 | 2-for-1. Does not trip the 3x check. |
| 2026-09-08 | IGR | 1/3 (`0.3333333333333333`) | 3 | 2026-09-08 | reverse split. Industry is `Closed-End Fund - Foreign`, so the ETF exclusion does not drop it. The check flags a ratio strictly above 3 or strictly below 1/3, so a ratio of exactly 3 is not a halt. No 3x leg was found. |

## Not in the table

YMT has share factor `0.0625` on 2026-08-27 in `research/breadth_rank_v1c/bars/splits.json`. It is not a row of the retro actions file. Market cap on the frozen rows stays between 11.2 and 23.86, under the 100 million floor, so YMT is not in the candidate universe.

AAC-U, HYAC-U, and PNAQ-U pass the liquid gate at least once and are not in `data/prices/ohlc.parquet`. A Yahoo chart request for each returned HTTP 404. They have no bar and no split row. They cannot enter a price-ranked list. No split is invented for them.

## Unexplained 3x legs

These liquid names have a leg inside the window that the pinned split table does not explain. The ratio is not recorded here. The writer halts instead of dropping the name.

| ticker | bar date | leg | previous stored bar |
| --- | --- | --- | --- |
| XHG | 2026-08-13 | open over previous close | 2026-08-12 |
| EYPT | 2026-08-17 | open over previous close | 2026-08-14 |
| SMJF | 2026-08-27 | close over open | 2026-08-27 |
| ADBT | 2026-09-03 | close over open | 2026-09-03 |

XHG is not liquid on 2026-08-13. EYPT is. The first session that reads one of these legs while building that morning's candidate list is 2026-08-17. Sessions 2026-08-13 and 2026-08-14 do not read them. IWM has no 3x leg in the window. An IWM jump is not a baseline, and there is not one to use.
