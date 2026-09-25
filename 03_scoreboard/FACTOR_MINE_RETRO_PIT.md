# Factor Mine retroactive point-in-time rebuild

Window `2026-08-13` → `2026-09-24`. Each D-dated packet is the last git commit at or before D 09:30 ET. A named input that exists only in a later commit is withheld.

Yahoo `auto_adjust=False` daily bars are the only price source and are used as stored (split-adjusted, dividends not applied). A Finviz or Stooq disagreement is a warning and does not hold the day. raw.csv fills a name only when Yahoo has no print at all, and that name is tagged single_source. A name with no print or too few earlier bars is dropped. The day locks unless nobody is rankable.

- pit_rebuilt: 15
- incomplete_pit: 15
- held: 0
- skipped: 0

## Held and skipped days

A day is not held because a print is missing or because Yahoo disagrees with Finviz or Stooq. The only skip is zero rankable names. A held row here is a ledger failure, not a price gap.

- (none)

## Dropped names

- `2026-08-13` dropped 0
- `2026-08-14` dropped 61 / 0.022838
- `2026-08-17` dropped 58 / 0.02178
- `2026-08-18` dropped 52 / 0.019556
- `2026-08-19` dropped 54 / 0.020293
- `2026-08-20` dropped 54 / 0.020293
- `2026-08-21` dropped 49 / 0.018407
- `2026-08-24` dropped 50 / 0.018762
- `2026-08-25` dropped 50 / 0.018797
- `2026-08-26` dropped 32 / 0.015296
- `2026-08-27` dropped 32 / 0.015296
- `2026-08-28` dropped 47 / 0.017689
- `2026-08-31` dropped 43 / 0.016214
- `2026-09-01` dropped 15 / 0.009585
- `2026-09-02` dropped 21 / 0.011986
- `2026-09-03` dropped 38 / 0.014378
- `2026-09-04` dropped 35 / 0.013268
- `2026-09-08` dropped 32 / 0.012158
- `2026-09-09` dropped 12 / 0.006973
- `2026-09-10` dropped 26 / 0.009912
- `2026-09-11` dropped 26 / 0.009927
- `2026-09-14` dropped 24 / 0.009167
- `2026-09-15` dropped 23 / 0.008785
- `2026-09-16` dropped 22 / 0.00841
- `2026-09-17` dropped 22 / 0.0084
- `2026-09-18` dropped 21 / 0.008021
- `2026-09-21` dropped 20 / 0.007637
- `2026-09-22` dropped 22 / 0.008362
- `2026-09-23` dropped 22 / 0.008349
- `2026-09-24` dropped 21 / 0.007988

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
| `union_hot_n4_h1` | futubull | with GLND | 2026-08-13 | 24.991 | 62 |
| `union_hot_n4_h1` | futubull | without GLND | 2026-08-13 | -4.125 | 63 |
| `union_hot_n4_h1` | flat_15bp | with GLND | 2026-08-13 | 29.536 | 62 |
| `union_hot_n4_h1` | flat_15bp | without GLND | 2026-08-13 | -0.553 | 63 |
| `union_hot_n4_holdup` | futubull | with GLND | 2026-08-13 | 52.196 | 53 |
| `union_hot_n4_holdup` | futubull | with GLND | 2026-09-21 | 16.444 | 11 |
| `union_hot_n4_holdup` | futubull | without GLND | 2026-08-13 | 15.832 | 53 |
| `union_hot_n4_holdup` | futubull | without GLND | 2026-09-21 | -8.983 | 11 |
| `union_hot_n4_holdup` | flat_15bp | with GLND | 2026-08-13 | 55.918 | 53 |
| `union_hot_n4_holdup` | flat_15bp | with GLND | 2026-09-21 | 17.404 | 11 |
| `union_hot_n4_holdup` | flat_15bp | without GLND | 2026-08-13 | 18.574 | 53 |
| `union_hot_n4_holdup` | flat_15bp | without GLND | 2026-09-21 | -8.274 | 11 |

Prices: `data/factor_mine/retro_prices/ohlc.parquet` (Yahoo `auto_adjust=False` split-adjusted bars, used as stored, locked, first bar wins). Split and dividend factors: `data/factor_mine/retro_prices/actions.parquet` (dated, keep-first). Indicators apply only events with ex-date before D. The live `data/prices` tape was not rewritten.

## Baselines

Scored on the same frozen sessions as HOT4 and holdup. Same $10k leftover book and the same fees (Futubull, and flat 15bp as 7.5 bp per side). RANDOM4 draws 4 names from that day's frozen snapshot list, the morning candidate rows the HOT4 book saw. Seed `20260813`, 1000 draws, `random.Random(seed + draw)`. An empty snapshot buys nothing. With GLND keeps that name in the pool. Without GLND drops it before the draw. Mean and 5/50/95 are total return percent, linear percentile. Trades are the median count. The morning S is the same one the HOT4 book reads, so a hard-red morning still sits.

IWM is buy-and-hold over those sessions. The name stays on the list every day, so the lot is not sold, and the book is marked at the last close. Hard-red does not skip the IWM entry. IWM is one series, not a with-GLND and without-GLND pair.

IWM tape: Yahoo auto_adjust=False, fetched in memory. Not written to data/factor_mine/retro_prices or data/prices.

| baseline | fee | universe | window | mean % | p5 | p50 | p95 | trades |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `random4` | futubull | with GLND | 2026-08-13 | -9.487 | -22.907 | -9.17 | 2.757 | 72 |
| `random4` | futubull | with GLND | 2026-09-21 | -6.432 | -12.959 | -6.534 | 0.564 | 24 |
| `random4` | futubull | without GLND | 2026-08-13 | -9.776 | -23.492 | -9.399 | 2.445 | 72 |
| `random4` | futubull | without GLND | 2026-09-21 | -6.672 | -12.967 | -6.602 | -0.506 | 24 |
| `random4` | flat_15bp | with GLND | 2026-08-13 | -6.817 | -20.038 | -6.546 | 5.464 | 72 |
| `random4` | flat_15bp | with GLND | 2026-09-21 | -5.49 | -11.887 | -5.635 | 1.773 | 24 |
| `random4` | flat_15bp | without GLND | 2026-08-13 | -7.14 | -20.644 | -6.857 | 4.858 | 72 |
| `random4` | flat_15bp | without GLND | 2026-09-21 | -5.737 | -11.952 | -5.713 | 0.316 | 24 |
| `iwm` | futubull | buy-and-hold | 2026-08-13 | -7.183 |  |  |  | 1 |
| `iwm` | futubull | buy-and-hold | 2026-09-21 | -1.588 |  |  |  | 1 |
| `iwm` | flat_15bp | buy-and-hold | 2026-08-13 | -7.235 |  |  |  | 1 |
| `iwm` | flat_15bp | buy-and-hold | 2026-09-21 | -1.64 |  |  |  | 1 |

## Dropped names that moved more than 10%

Move is that session's close versus the prior stored Yahoo close. A dropped name with no session print is not a mover. `2026-08-18` EQR and `2026-09-03` HLX had no print.

| date | dropped | moved more than 10% |
| --- | ---: | --- |
| 2026-08-13 | 0 | |
| 2026-08-14 | 61 | ADIG -17.7%, APMD +10.3%, STDN +10.1% |
| 2026-08-17 | 58 | ATTO +14.4% |
| 2026-08-18 | 52 | EROC -12.5% |
| 2026-08-19 | 54 | QMLS -12.3% |
| 2026-08-20 | 54 | APMD -10.9%, AADX -10.2% |
| 2026-08-21 | 49 | |
| 2026-08-24 | 50 | USDE -15.4%, ITG -10.1% |
| 2026-08-25 | 50 | STDN +16.8% |
| 2026-08-26 | 32 | |
| 2026-08-27 | 32 | USDE +34.9% |
| 2026-08-28 | 47 | USDE -16.2% |
| 2026-08-31 | 43 | USDE +31.2%, STDN +14.3%, APMD -11.6% |
| 2026-09-01 | 15 | |
| 2026-09-02 | 21 | ADBT -53.3%, STDN -11.5%, USDE -10.3% |
| 2026-09-03 | 38 | ADBT -80.2%, USDE +16.9%, BRR +13.9% |
| 2026-09-04 | 35 | |
| 2026-09-08 | 32 | QMLS +12.9%, SECZ +11.2%, STDN -10.1% |
| 2026-09-09 | 12 | TRBG -16.1% |
| 2026-09-10 | 26 | |
| 2026-09-11 | 26 | ADBT -25.8% |
| 2026-09-14 | 24 | ADBT -56.5% |
| 2026-09-15 | 23 | SWRD -30.2%, USDE -17.7% |
| 2026-09-16 | 22 | |
| 2026-09-17 | 22 | USDE +24.4%, SWRD +17.7%, SECZ +14.9% |
| 2026-09-18 | 21 | USDE +32.2%, SECZ +21.6% |
| 2026-09-21 | 20 | SECZ +24.3% |
| 2026-09-22 | 22 | BRR +14.5%, XTND -10.8% |
| 2026-09-23 | 22 | APMD -11.8%, SWRD +11.0%, SECZ +10.5% |
| 2026-09-24 | 21 | SWRD +16.1%, SECZ +15.1% |

## pit_rebuilt days only

The all-days book keeps the 15 incomplete sessions on the calendar (no new ranking). The pit-only book uses only the 15 `pit_rebuilt` sessions, so the first day is `2026-08-24`. `2026-09-21` through `2026-09-24` are all `pit_rebuilt`, so that window matches the all-days window.

| recipe | fee | days | window | return % | trades |
| --- | --- | --- | --- | ---: | ---: |
| `union_hot_n4_h1` | futubull | all | 2026-08-13 | 24.991 | 62 |
| `union_hot_n4_h1` | futubull | pit_rebuilt | 2026-08-24 | 17.277 | 62 |
| `union_hot_n4_h1` | flat_15bp | all | 2026-08-13 | 29.536 | 62 |
| `union_hot_n4_h1` | flat_15bp | pit_rebuilt | 2026-08-24 | 21.486 | 62 |
| `union_hot_n4_holdup` | futubull | all | 2026-08-13 | 52.196 | 53 |
| `union_hot_n4_holdup` | futubull | pit_rebuilt | 2026-08-24 | 41.690 | 51 |
| `union_hot_n4_holdup` | futubull | all | 2026-09-21 | 16.444 | 11 |
| `union_hot_n4_holdup` | futubull | pit_rebuilt | 2026-09-21 | 16.444 | 11 |
| `union_hot_n4_holdup` | flat_15bp | all | 2026-08-13 | 55.918 | 53 |
| `union_hot_n4_holdup` | flat_15bp | pit_rebuilt | 2026-08-24 | 44.606 | 51 |
| `union_hot_n4_holdup` | flat_15bp | all | 2026-09-21 | 17.404 | 11 |
| `union_hot_n4_holdup` | flat_15bp | pit_rebuilt | 2026-09-21 | 17.404 | 11 |

## HOT4 without its best stock

The largest closed winner on the all-days book is INDP (realized pnl $617.41 on `union_hot_n4_h1`, $1,132.67 on `union_hot_n4_holdup`), not GLND. Dropping INDP from the candidate rows and rescoring:

| recipe | fee | universe | window | return % | trades |
| --- | --- | --- | --- | ---: | ---: |
| `union_hot_n4_h1` | futubull | without INDP | 2026-08-13 | 24.669 | 64 |
| `union_hot_n4_h1` | flat_15bp | without INDP | 2026-08-13 | 28.777 | 64 |
| `union_hot_n4_holdup` | futubull | without INDP | 2026-08-13 | 45.124 | 59 |
| `union_hot_n4_holdup` | futubull | without INDP | 2026-09-21 | 16.444 | 11 |
| `union_hot_n4_holdup` | flat_15bp | without INDP | 2026-08-13 | 48.569 | 57 |
| `union_hot_n4_holdup` | flat_15bp | without INDP | 2026-09-21 | 17.404 | 11 |

INDP was not held in the `2026-09-21` window, so that window is unchanged. The without-GLND rows above are a different cut.

Open cross-check uses theme-radar `{D}.raw.csv` Finviz Open (`finviz_raw`) when the latest git commit of that file is before the next session's 09:30 ET. The time is the GitHub commits API committer timestamp (`commits?path=data/snapshots/{D}.raw.csv`). There is no lower bound; the snapshot workflow starts after the close. From 2026-09-24 a present scrape_ts must also be before that next open. The stamp is the slim `{D}.csv` scrape_ts column, or `scrape_ts_utc` in theme-radar `manifest.json`. It is not read from raw.csv. A missing raw export, a commit at or after the next open, a late scrape_ts, or a hash mismatch uses Stooq. From 2026-09-25 the slim `{D}.csv` Open column is the next file (`finviz_snapshot`) under the same upper bound. `current.csv` is not a source. Webull paper fills stay a third check. On this window every session except 2026-08-27 uses `finviz_raw`. 2026-08-27 has no raw export, so it uses Stooq. The log is `data/factor_mine/open_source_log.csv` (source, commit sha, commit time). Per-recipe session returns for the shuffle test are `data/factor_mine/daily_returns.csv` (recipe, recipe_created_date, start_date, D, net_ret_futubull, net_ret_15bp, day_status, source_shas). One row per recipe, start date, and day. A held or missing day is `held` with empty returns, never 0.
