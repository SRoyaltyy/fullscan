# Splits in the hot_n4_clean_v4 candidate universe

Window: NYSE sessions 2026-08-13 through 2026-09-25.

The candidate universe is every ticker that passes the liquid gate on at least one frozen Theme Radar row whose `trade_date` is one of those sessions. The gate is `Ticker` present, `Industry` not `Exchange Traded Fund`, `Market Cap` at least 100 (millions of dollars), `Average Volume` at least 500 (thousands of shares), and `Volume` greater than 0. That set has 2,809 names. `2026-08-28` has no frozen row and adds nobody.

Nine rows come from `data/factor_mine/retro_prices/actions.parquet` sha256 `9007c3aec0db3fa42735bf10ad8611e19ba78a2d532cec0df8f99205fbd6b633` (git blob `b583117809d0ccac78b766084d5cb04ca6d6dec3`). The stored `split` column is Yahoo's share factor. The price factor is one divided by that share factor. `data/prices/actions.parquet` sha256 `0471b2d76c30960eb494134a7bd34eea499bb5c696190f5fb3628c4becd94e4a` has no nonzero split in this window, so it is not the explanation table.

Two more rows are primary-source corporate actions. The retro file has no MVIS split. AEHL has retro share factor `0.0625` on 2026-08-10, before this window, so v1 did not list it. The price factor on those two rows is the filing's split ratio. The retro cell is not the citation.

`boundary` is the adjustment date. Bars strictly before `boundary` are the old price level. OHLC on those bars is multiplied by the price factor, and volume is multiplied by the share factor, before any return, hot score, rvol, candle, or breakout. Bars on or after `boundary` are left as stored. Dividends are not applied. A 3x jump uses the jumping bar as `boundary` when the recorded ex-date is different. A split that does not trip the 3x check uses the recorded ex-date.

| ex-date | ticker | share factor | price factor | boundary | what it is |
| --- | --- | --- | --- | --- | --- |
| 2026-08-03 | MVIS | 1/15 (`0.06666666666666667`) | 15 | 2026-08-03 | 1-for-15 reverse split. Form 8-K filed 2026-07-22, accession `0001493152-26-034194`, https://www.sec.gov/Archives/edgar/data/65770/000149315226034194/form8-k.htm. Effective 5:00 p.m. ET on 2026-08-01. Split-adjusted Nasdaq trading begins at the open on 2026-08-03. The jumping bar is that open over the 2026-07-30 close. |
| 2026-08-10 | AEHL | 1/16 (`0.0625`) | 16 | 2026-08-10 | 1-for-16 reverse split. Form 6-K filed 2026-08-05, accession `0001493152-26-036122`, https://www.sec.gov/Archives/edgar/data/1470683/000149315226036122/form6-k.htm. Effective 4:01 p.m. ET on 2026-08-07. Split-adjusted Nasdaq trading begins at the open on 2026-08-10. |
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

## Real price moves

`JUMP_SCAN.csv` is every open-over-previous-close, close-over-previous-close, and close-over-open leg above 3 or below 1/3 in the bars a session reads, that the v1 nine-row table does not explain. The scan covers every liquid name on each session from 2026-08-13 through 2026-09-25, the last 60 stored bars before that session, and that session's own bar. MVIS and AEHL are the verified rows above. A leg that is not a verified split is a real price move when `REAL_MOVES.csv` has that row. The bars stay as stored. The writer does not adjust them and does not halt. The filing type, filing date, accession, URL, and one-line note are in that file.

A rescan of the same liquid names and the same 60-bar window found eight further legs: BYND and NRDY on 2026-08-17, and BRCC and REAX on 2026-08-20, each as open over the previous close and close over the previous close. The v1 nine-row table verifies all eight. No unexplained leg was missing from v2.

## Unexplained 3x legs

A leg with no primary filing in the five-session window, or a filing that shows a share change this study did not verify as the split, stays unexplained. A candidate that loads that leg is removed before ranking. The writer does not halt that session for the removal. A name the book already holds still halts.

The first candidate session is the first session on which the name enters that day's candidate list and the jumping bar is one of the bars that session reads. v3 halted on that session. This study removes the name instead, on that session and on every later session where it is a candidate while the bar is loaded. The list is `EXCLUSIONS_PLAN.csv`. This prereg holds no book, so nothing is already held. A blank cell means the name does not enter the candidate list on any session that loads the bar. A later run that is holding the name still halts. A real price move does not.

| ticker | bar date | leg | previous stored bar | first candidate session |
| --- | --- | --- | --- | --- |
| XHG | 2026-08-13 | open over previous close | 2026-08-12 | 2026-08-19 |
| XHG | 2026-08-13 | close over previous close | 2026-08-12 | 2026-08-19 |
| JLHL | 2026-07-09 | close over open | 2026-07-09 | 2026-09-03 |
| JLHL | 2026-07-09 | close over previous close | 2026-07-08 | 2026-09-03 |
| SLBT | 2026-06-16 | open over previous close | 2026-06-15 | 2026-09-03 |
| MFP | 2026-06-29 | open over previous close | 2026-06-26 | none in this window |
| MFP | 2026-06-29 | close over previous close | 2026-06-26 | none in this window |
| MB | 2026-08-07 | open over previous close | 2026-08-06 | none in this window |

The first of those candidate sessions is 2026-08-19, on XHG. This study removes XHG that day and writes the day file. It does not refuse the session. A name the book already holds still refuses the day file. IWM has no 3x leg in the stored bars. An IWM jump is not a baseline, and there is not one to use.
