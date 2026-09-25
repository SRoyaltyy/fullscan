# Factor Mine retroactive point-in-time rebuild

Window `2026-08-13` → `2026-09-24`. Each D-dated packet is the last git commit at or before D 09:30 ET. A named input that exists only in a later commit is withheld.

- pit_rebuilt: 11
- incomplete_pit: 5
- held: 14

## Held days

A day is held when more than 10% of its candidates are unrankable, or when a Yahoo print disagrees with the external open or close beyond PRICE_CHECK. An unrankable name under that line is left off the ranking and is not scored. A raw.csv bar Yahoo did not have is single_source. The book is empty and the ledger recipes are empty. The return is not 0.

- `2026-08-14` price cross-check missing: (none); cross-check gaps: 10; unrankable 2.3%
- `2026-08-17` price cross-check missing: (none); cross-check gaps: 10; unrankable 2.2%
- `2026-08-18` price cross-check missing: EQR; cross-check gaps: 10; unrankable 2.0%
- `2026-08-19` price cross-check missing: (none); cross-check gaps: 8; unrankable 2.0%
- `2026-08-20` price cross-check missing: (none); cross-check gaps: 8; unrankable 2.0%
- `2026-08-21` price cross-check missing: (none); cross-check gaps: 8; unrankable 1.8%
- `2026-08-24` price cross-check missing: (none); cross-check gaps: 6; unrankable 1.9%
- `2026-08-25` price cross-check missing: (none); cross-check gaps: 4; unrankable 1.9%
- `2026-08-26` price cross-check missing: (none); cross-check gaps: 2; unrankable 1.5%
- `2026-08-28` price cross-check missing: (none); cross-check gaps: 4; unrankable 1.8%
- `2026-08-31` price cross-check missing: (none); cross-check gaps: 4; unrankable 1.6%
- `2026-09-01` price cross-check missing: (none); cross-check gaps: 2; unrankable 1.0%
- `2026-09-02` price cross-check missing: (none); cross-check gaps: 2; unrankable 1.2%
- `2026-09-23` price cross-check missing: (none); cross-check gaps: 2; unrankable 0.8%

## raw.csv versus Yahoo raw

Compared on names that have both a Yahoo raw print and a commit-guarded raw.csv print. The Yahoo download is not stored.
- open: n=90944 median abs=4.5776366874861196e-07 max abs=86.07000030517577 median pct=2.0956405864409692e-08 max pct=1.0000000854485105
- close: n=90944 median abs=4.5776367230132564e-07 max abs=85.43500244140625 median pct=2.1201175251523378e-08 max pct=1.000000089768588

## Incomplete days

These days are carried (no new ranking). The later file is not used.

- `2026-08-13` withheld: judge
- `2026-08-27` withheld: catalyst, events
- `2026-09-03` withheld: join, weather
- `2026-09-08` withheld: predict, actions, events, research
- `2026-09-09` withheld: predict

## pit_rebuilt

`2026-09-04`, `2026-09-10`, `2026-09-11`, `2026-09-14`, `2026-09-15`, `2026-09-16`, `2026-09-17`, `2026-09-18`, `2026-09-21`, `2026-09-22`, `2026-09-24`

## HOT4 and holdup

Total return on the frozen history. `union_hot_n4_holdup` days before `2026-09-21` are in-sample. Without GLND drops that ticker from the candidate rows. Flat 15bp is 7.5 bp per side (15 bp round trip), not the Futubull schedule.

| recipe | fee | universe | window | return % | trades |
| --- | --- | --- | --- | ---: | ---: |
| `union_hot_n4_h1` | futubull | with GLND | 2026-08-13 | 6.235 | 48 |
| `union_hot_n4_h1` | futubull | without GLND | 2026-08-13 | 6.235 | 48 |
| `union_hot_n4_h1` | flat_15bp | with GLND | 2026-08-13 | 9.895 | 48 |
| `union_hot_n4_h1` | flat_15bp | without GLND | 2026-08-13 | 9.895 | 48 |
| `union_hot_n4_holdup` | futubull | with GLND | 2026-08-13 | 24.708 | 42 |
| `union_hot_n4_holdup` | futubull | with GLND | 2026-09-21 | -1.013 | 8 |
| `union_hot_n4_holdup` | futubull | without GLND | 2026-08-13 | 24.708 | 42 |
| `union_hot_n4_holdup` | futubull | without GLND | 2026-09-21 | -1.013 | 8 |
| `union_hot_n4_holdup` | flat_15bp | with GLND | 2026-08-13 | 27.487 | 42 |
| `union_hot_n4_holdup` | flat_15bp | with GLND | 2026-09-21 | -0.342 | 8 |
| `union_hot_n4_holdup` | flat_15bp | without GLND | 2026-08-13 | 27.487 | 42 |
| `union_hot_n4_holdup` | flat_15bp | without GLND | 2026-09-21 | -0.342 | 8 |

Prices: `data/factor_mine/retro_prices/ohlc.parquet` (Yahoo `auto_adjust=False`, raw prints, locked, first bar wins). Split and dividend factors: `data/factor_mine/retro_prices/actions.parquet` (dated, keep-first). Indicators apply only events with ex-date before D. The live `data/prices` tape was not rewritten.

## Baselines

Scored on the same frozen sessions as HOT4 and holdup. Same $10k leftover book and the same fees (Futubull, and flat 15bp as 7.5 bp per side). RANDOM4 draws 4 names from that day's frozen snapshot list, the morning candidate rows the HOT4 book saw. Seed `20260813`, 1000 draws, `random.Random(seed + draw)`. An empty snapshot buys nothing. With GLND keeps that name in the pool. Without GLND drops it before the draw. Mean and 5/50/95 are total return percent, linear percentile. Trades are the median count. The morning S is the same one the HOT4 book reads, so a hard-red morning still sits.

IWM is buy-and-hold over those sessions. The name stays on the list every day, so the lot is not sold, and the book is marked at the last close. Hard-red does not skip the IWM entry. IWM is one series, not a with-GLND and without-GLND pair.

IWM tape: Yahoo auto_adjust=False, fetched in memory. Not written to data/factor_mine/retro_prices or data/prices.

| baseline | fee | universe | window | mean % | p5 | p50 | p95 | trades |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `random4` | futubull | with GLND | 2026-08-13 | -2.377 | -16.637 | -2.207 | 9.537 | 56 |
| `random4` | futubull | with GLND | 2026-09-21 | -2.43 | -6.799 | -2.666 | 3.066 | 16 |
| `random4` | futubull | without GLND | 2026-08-13 | -2.125 | -16.151 | -1.639 | 10.445 | 56 |
| `random4` | futubull | without GLND | 2026-09-21 | -2.215 | -6.548 | -2.486 | 3.114 | 16 |
| `random4` | flat_15bp | with GLND | 2026-08-13 | -0.238 | -14.145 | -0.115 | 11.518 | 56 |
| `random4` | flat_15bp | with GLND | 2026-09-21 | -1.795 | -5.936 | -2.087 | 3.63 | 16 |
| `random4` | flat_15bp | without GLND | 2026-08-13 | 0.015 | -13.667 | 0.281 | 12.473 | 56 |
| `random4` | flat_15bp | without GLND | 2026-09-21 | -1.585 | -5.828 | -1.902 | 3.731 | 16 |
| `iwm` | futubull | buy-and-hold | 2026-08-13 | -7.183 |  |  |  | 1 |
| `iwm` | futubull | buy-and-hold | 2026-09-21 | -1.588 |  |  |  | 1 |
| `iwm` | flat_15bp | buy-and-hold | 2026-08-13 | -7.235 |  |  |  | 1 |
| `iwm` | flat_15bp | buy-and-hold | 2026-09-21 | -1.64 |  |  |  | 1 |

Open cross-check uses theme-radar `{D}.raw.csv` Finviz Open (`finviz_raw`) when the latest git commit of that file is before the next session's 09:30 ET. The time is the GitHub commits API committer timestamp (`commits?path=data/snapshots/{D}.raw.csv`). There is no lower bound; the snapshot workflow starts after the close. From 2026-09-24 a present scrape_ts must also be before that next open. The stamp is the slim `{D}.csv` scrape_ts column, or `scrape_ts_utc` in theme-radar `manifest.json`. It is not read from raw.csv. A missing raw export, a commit at or after the next open, a late scrape_ts, or a hash mismatch uses Stooq. From 2026-09-25 the slim `{D}.csv` Open column is the next file (`finviz_snapshot`) under the same upper bound. `current.csv` is not a source. Webull paper fills stay a third check. On this window every session except 2026-08-27 uses `finviz_raw`. 2026-08-27 has no raw export, so it uses Stooq. The log is `data/factor_mine/open_source_log.csv` (source, commit sha, commit time). Per-recipe session returns for the shuffle test are `data/factor_mine/daily_returns.csv` (recipe, recipe_created_date, start_date, D, net_ret_futubull, net_ret_15bp, day_status, source_shas). One row per recipe, start date, and day. A held or missing day is `held` with empty returns, never 0.
