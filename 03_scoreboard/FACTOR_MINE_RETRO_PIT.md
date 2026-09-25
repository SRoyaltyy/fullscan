# Factor Mine retroactive point-in-time rebuild

Window `2026-08-13` → `2026-09-24`. Each D-dated packet is the last git commit at or before D 09:30 ET. A named input that exists only in a later commit is withheld.

- pit_rebuilt: 15
- incomplete_pit: 15

## Incomplete days

These days are carried (no new ranking). The later file is not used.

- `2026-08-13` withheld: judge
- `2026-08-14` withheld: judge
- `2026-08-17` withheld: judge
- `2026-08-18` withheld: export, join, weather, judge
- `2026-08-19` withheld: export, join, weather, judge
- `2026-08-20` withheld: export, join, weather, judge
- `2026-08-21` withheld: export, join, weather, events
- `2026-08-25` withheld: digest, predict, actions, judge, events
- `2026-08-26` withheld: join, catalyst, weather
- `2026-08-27` withheld: catalyst, events
- `2026-09-01` withheld: baseline
- `2026-09-02` withheld: join, weather
- `2026-09-03` withheld: join, weather
- `2026-09-08` withheld: predict, actions, events, research
- `2026-09-09` withheld: predict

## pit_rebuilt

`2026-08-24`, `2026-08-28`, `2026-08-31`, `2026-09-04`, `2026-09-10`, `2026-09-11`, `2026-09-14`, `2026-09-15`, `2026-09-16`, `2026-09-17`, `2026-09-18`, `2026-09-21`, `2026-09-22`, `2026-09-23`, `2026-09-24`

## HOT4 and holdup

Total return on the frozen history. `union_hot_n4_holdup` days before `2026-09-21` are in-sample. Without GLND drops that ticker from the candidate rows. Flat 15bp is 7.5 bp per side (15 bp round trip), not the Futubull schedule.

| recipe | fee | universe | window | return % | trades |
| --- | --- | --- | --- | ---: | ---: |
| `union_hot_n4_h1` | futubull | with GLND | 2026-08-13 | 23.467 | 61 |
| `union_hot_n4_h1` | futubull | without GLND | 2026-08-13 | -4.133 | 62 |
| `union_hot_n4_h1` | flat_15bp | with GLND | 2026-08-13 | 27.53 | 61 |
| `union_hot_n4_h1` | flat_15bp | without GLND | 2026-08-13 | -1.031 | 62 |
| `union_hot_n4_holdup` | futubull | with GLND | 2026-08-13 | 51.788 | 53 |
| `union_hot_n4_holdup` | futubull | with GLND | 2026-09-21 | 12.898 | 11 |
| `union_hot_n4_holdup` | futubull | without GLND | 2026-08-13 | 16.667 | 53 |
| `union_hot_n4_holdup` | futubull | without GLND | 2026-09-21 | -10.905 | 11 |
| `union_hot_n4_holdup` | flat_15bp | with GLND | 2026-08-13 | 55.157 | 53 |
| `union_hot_n4_holdup` | flat_15bp | with GLND | 2026-09-21 | 13.882 | 11 |
| `union_hot_n4_holdup` | flat_15bp | without GLND | 2026-08-13 | 19.137 | 53 |
| `union_hot_n4_holdup` | flat_15bp | without GLND | 2026-09-21 | -10.197 | 11 |

Prices: `data/factor_mine/retro_prices/ohlc.parquet` (Yahoo `auto_adjust=True`, locked sha `d54a5f5322ad48d2`, 274,058 rows, 2,792 tickers, 2026-05-01 through 2026-09-24, first bar wins). Indicators use bars dated before D. The live `data/prices/ohlc.parquet` print tape was not rewritten. Marks on this book are split-adjusted opens, not the unadjusted print tape.

Names with no adjusted bar or no session open were dropped from that day's rows. The day still froze. Dropped counts: 2026-08-28 (1), 2026-09-04 (3), 2026-09-10 (1), 2026-09-14 (1), 2026-09-16 (1), 2026-09-22 (47 of 74 candidates). 2026-09-22 is the thin session.

Ledgers are gzip of the canonical decision JSON (`{D}.json.gz`). The manifest sha256 is those gzip bytes.

## Baselines

Scored on the same frozen sessions as HOT4 and holdup. Same $10k leftover book and the same fees (Futubull, and flat 15bp as 7.5 bp per side). RANDOM4 draws 4 names from that day's frozen snapshot list, the morning candidate rows the HOT4 book saw. Seed `20260813`, 1000 draws, `random.Random(seed + draw)`. An empty snapshot buys nothing. With GLND keeps that name in the pool. Without GLND drops it before the draw. Mean and 5/50/95 are total return percent, linear percentile. Trades are the median count. The morning S is the same one the HOT4 book reads, so a hard-red morning still sits.

IWM is buy-and-hold over those sessions. The name stays on the list every day, so the lot is not sold, and the book is marked at the last close. Hard-red does not skip the IWM entry. IWM is one series, not a with-GLND and without-GLND pair.

IWM tape: Yahoo auto_adjust=True, fetched in memory. Not written to data/factor_mine/retro_prices or data/prices.

| baseline | fee | universe | window | mean % | p5 | p50 | p95 | trades |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `random4` | futubull | with GLND | 2026-08-13 | -7.501 | -21.497 | -7.31 | 6.313 | 70 |
| `random4` | futubull | with GLND | 2026-09-21 | -6.439 | -12.771 | -6.803 | 0.471 | 22 |
| `random4` | futubull | without GLND | 2026-08-13 | -7.694 | -22.32 | -7.735 | 6.626 | 70 |
| `random4` | futubull | without GLND | 2026-09-21 | -6.707 | -13.031 | -6.759 | -0.363 | 22 |
| `random4` | flat_15bp | with GLND | 2026-08-13 | -4.993 | -18.462 | -4.88 | 9.012 | 70 |
| `random4` | flat_15bp | with GLND | 2026-09-21 | -5.622 | -11.631 | -5.908 | 1.163 | 22 |
| `random4` | flat_15bp | without GLND | 2026-08-13 | -5.202 | -19.63 | -5.359 | 9.019 | 70 |
| `random4` | flat_15bp | without GLND | 2026-09-21 | -5.898 | -11.974 | -5.957 | 0.365 | 22 |
| `iwm` | futubull | buy-and-hold | 2026-08-13 | -6.929 |  |  |  | 1 |
| `iwm` | futubull | buy-and-hold | 2026-09-21 | -1.588 |  |  |  | 1 |
| `iwm` | flat_15bp | buy-and-hold | 2026-08-13 | -6.981 |  |  |  | 1 |
| `iwm` | flat_15bp | buy-and-hold | 2026-09-21 | -1.64 |  |  |  | 1 |

Open cross-check uses theme-radar `{D}.raw.csv` Finviz Open (`finviz_raw`) when the latest git commit of that file is before the next session's 09:30 ET. The time is the GitHub commits API committer timestamp (`commits?path=data/snapshots/{D}.raw.csv`). There is no lower bound; the snapshot workflow starts after the close. From 2026-09-24 a present scrape_ts must also be before that next open. The stamp is the slim `{D}.csv` scrape_ts column, or `scrape_ts_utc` in theme-radar `manifest.json`. It is not read from raw.csv. A missing raw export, a commit at or after the next open, a late scrape_ts, or a hash mismatch uses Stooq. From 2026-09-25 the slim `{D}.csv` Open column is the next file (`finviz_snapshot`) under the same upper bound. `current.csv` is not a source. Webull paper fills stay a third check. On this window every session except 2026-08-27 uses `finviz_raw`. 2026-08-27 has no raw export, so it uses Stooq. The log is `data/factor_mine/open_source_log.csv` (source, commit sha, commit time). Per-recipe session returns for the shuffle test are `data/factor_mine/daily_returns.csv` (recipe, recipe_created_date, start_date, D, net_ret_futubull, net_ret_15bp, day_status, source_shas). One row per recipe, start date, and day. A held or missing day is `held` with empty returns, never 0.
