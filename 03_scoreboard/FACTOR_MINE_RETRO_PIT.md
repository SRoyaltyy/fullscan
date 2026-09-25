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

Open cross-check uses theme-radar `{D}.raw.csv` Finviz Open (`finviz_raw`) when the latest git commit of that file is before the next session's 09:30 ET. The time is the GitHub commits API committer timestamp (`commits?path=data/snapshots/{D}.raw.csv`). There is no lower bound; the snapshot workflow starts after the close. From 2026-09-24 a present scrape_ts must also be before that next open. The stamp is the slim `{D}.csv` scrape_ts column, or `scrape_ts_utc` in theme-radar `manifest.json`. It is not read from raw.csv. A missing raw export, a commit at or after the next open, a late scrape_ts, or a hash mismatch uses Stooq. From 2026-09-25 the slim `{D}.csv` Open column is the next file (`finviz_snapshot`) under the same upper bound. `current.csv` is not a source. Webull paper fills stay a third check. On this window every session except 2026-08-27 uses `finviz_raw`. 2026-08-27 has no raw export, so it uses Stooq. The log is `data/factor_mine/open_source_log.csv` (source, commit sha, commit time). Per-recipe session returns for the shuffle test are `data/factor_mine/daily_returns.csv` (recipe, recipe_created_date, start_date, D, net_ret_futubull, net_ret_15bp, day_status, source_shas, timing_clean, news_clean, reads_news). One row per recipe, start date, and day. A held or missing day is `held` with empty returns, never 0. `timing_clean` is true only on `pit_rebuilt` days. `news_clean` is false on the #331 quarantine dates.

## Clean windows

`timing_clean` is true only on `pit_rebuilt` days. `news_clean` is false on the 18 sessions in `data/quarantine_sessions.json` from #331 (stale dated news and undated Finviz headlines). `reads_news` is true when the recipe, or a combo member, gates on news, a headline, the digest, a catalyst, the judge, or map-heat. The both-clean window is filled only for those recipes. Each book is the earliest start date. A blank return is omitted. Returns are the compounded session percents already in the CSV. Days that fail the flag are left out of the chain. The calendar is not rebuilt.

| recipe | reads_news | all n | all futubull % | all 15bp % | timing n | timing futubull % | timing 15bp % | both n | both futubull % | both 15bp % |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `coil_h3_exit_alarm` | false | 30 | -20.662 | -18.730 | 15 | -15.253 | -13.327 |  |  |  |
| `combo_e1er_5050_shared` | false | 30 | -8.398 | -7.690 | 15 | -7.767 | -7.169 |  |  |  |
| `combo_e1s_7030_shared` | true | 30 | -15.150 | -13.975 | 15 | -16.524 | -15.497 | 2 | -1.649 | -1.492 |
| `combo_ee1_3070_shared` | false | 30 | -8.396 | -7.772 | 15 | -2.152 | -1.610 |  |  |  |
| `combo_ee1_5050_shared` | false | 30 | -8.396 | -7.772 | 15 | -2.152 | -1.610 |  |  |  |
| `combo_ee1_7030_shared` | false | 30 | -8.396 | -7.772 | 15 | -2.152 | -1.610 |  |  |  |
| `combo_eer_5050_shared` | false | 30 | -8.396 | -7.772 | 15 | -2.152 | -1.610 |  |  |  |
| `combo_ef_3070_shared` | false | 30 | -12.406 | -10.451 | 15 | -6.396 | -4.456 |  |  |  |
| `combo_ef_5050_shared` | false | 30 | -11.253 | -9.310 | 15 | -5.114 | -3.193 |  |  |  |
| `combo_ef_7030_shared` | false | 30 | -9.591 | -7.715 | 15 | -3.374 | -1.523 |  |  |  |
| `combo_eh_3070_shared` | false | 30 | 10.710 | 15.489 | 15 | 15.330 | 19.880 |  |  |  |
| `combo_eh_5050_shared` | false | 30 | 3.473 | 6.656 | 15 | 8.573 | 11.584 |  |  |  |
| `combo_eh_7030_shared` | false | 30 | -3.721 | -0.582 | 15 | 1.744 | 4.771 |  |  |  |
| `combo_ehs_601525_shared` | true | 30 | -2.493 | 0.227 | 15 | -0.100 | 2.399 | 2 | -1.306 | -1.105 |
| `combo_ehs_702010_shared` | true | 30 | -4.138 | -1.120 | 15 | 0.412 | 3.278 | 2 | -2.569 | -2.344 |
| `combo_ej_5050_shared` | true | 30 | -5.048 | -1.910 | 15 | -1.259 | 1.695 | 2 | -2.447 | -2.287 |
| `combo_en_3070_shared` | true | 30 | -10.682 | -6.483 | 15 | -9.323 | -5.328 | 2 | -3.240 | -2.979 |
| `combo_en_5050_shared` | true | 30 | -8.958 | -5.676 | 15 | -6.270 | -3.117 | 2 | -3.468 | -3.205 |
| `combo_en_7030_shared` | true | 30 | -8.498 | -5.550 | 15 | -4.530 | -1.725 | 2 | -3.639 | -3.373 |
| `combo_ers_7030_shared` | true | 30 | -15.083 | -13.929 | 15 | -12.926 | -11.885 | 2 | -1.649 | -1.492 |
| `combo_es_8020_shared` | true | 30 | -8.666 | -7.476 | 15 | -5.253 | -4.199 | 2 | -2.389 | -2.241 |
| `combo_es_9010_shared` | true | 30 | -10.813 | -9.194 | 15 | -6.039 | -4.502 | 2 | -3.116 | -2.964 |
| `combo_fe1_5050_shared` | false | 30 | -9.836 | -7.378 | 15 | -6.356 | -3.934 |  |  |  |
| `combo_fe_5050_shared` | false | 30 | -11.253 | -9.310 | 15 | -5.114 | -3.193 |  |  |  |
| `combo_fer_5050_shared` | false | 30 | -14.295 | -12.624 | 15 | -8.366 | -6.731 |  |  |  |
| `combo_fes_403030_shared` | true | 30 | -6.196 | -3.381 | 15 | -3.367 | -0.646 | 2 | -1.229 | -0.984 |
| `combo_fh_7030_shared` | false | 30 | -6.613 | -3.964 | 15 | -1.508 | 1.153 |  |  |  |
| `combo_fse_333_shared` | true | 30 | -5.411 | -2.578 | 15 | -2.915 | -0.183 | 2 | -1.158 | -0.923 |
| `combo_he1_5050_shared` | false | 30 | 5.493 | 9.191 | 15 | 7.612 | 11.065 |  |  |  |
| `combo_her_5050_shared` | false | 30 | -1.894 | 1.140 | 15 | 2.942 | 5.813 |  |  |  |
| `combo_hf_5050_shared` | false | 30 | -1.402 | 0.842 | 15 | 3.223 | 5.388 |  |  |  |
| `combo_hj_5050_shared` | true | 30 | 8.338 | 12.879 | 15 | 10.445 | 14.786 | 2 | -2.005 | -1.928 |
| `combo_hn_3070_shared` | true | 30 | -5.244 | -0.480 | 15 | -4.807 | -0.268 | 2 | -2.383 | -2.241 |
| `combo_hn_5050_shared` | true | 30 | 2.313 | 7.561 | 15 | 3.483 | 8.488 | 2 | -1.999 | -1.842 |
| `combo_hn_7030_shared` | true | 30 | 10.554 | 16.247 | 15 | 12.561 | 17.996 | 2 | -1.630 | -1.474 |
| `combo_je1_5050_shared` | true | 30 | -6.270 | -3.137 | 15 | -5.285 | -2.395 | 2 | -2.447 | -2.287 |
| `combo_jer_5050_shared` | true | 30 | -9.344 | -6.300 | 15 | -5.726 | -2.857 | 2 | -2.447 | -2.287 |
| `combo_jf_5050_shared` | true | 30 | -11.447 | -8.090 | 15 | -7.836 | -4.484 | 2 | -2.270 | -2.134 |
| `combo_jse_333_shared` | true | 30 | -10.275 | -6.494 | 15 | -10.194 | -6.682 | 2 | -0.490 | -0.294 |
| `combo_ne1_5050_shared` | true | 30 | -14.748 | -11.244 | 15 | -14.683 | -11.412 | 2 | -3.468 | -3.205 |
| `combo_ner_5050_shared` | true | 30 | -13.937 | -10.419 | 15 | -11.396 | -7.989 | 2 | -3.468 | -3.205 |
| `combo_nf_5050_shared` | true | 30 | -12.577 | -9.487 | 15 | -9.665 | -6.579 | 2 | -3.030 | -2.811 |
| `combo_nj_5050_shared` | true | 30 | -12.170 | -7.655 | 15 | -11.916 | -7.630 | 2 | -2.416 | -2.238 |
| `combo_nse_333_shared` | true | 30 | -16.035 | -12.074 | 15 | -16.276 | -12.595 | 2 | -1.312 | -1.009 |
| `combo_oh_5050_shared` | false | 30 | 24.991 | 29.536 | 15 | 28.623 | 32.974 |  |  |  |
| `combo_p2s_5050_shared` | true | 30 | -16.728 | -16.167 | 15 | -20.067 | -19.533 | 2 | 0.852 | 0.913 |
| `combo_ps_5050_shared` | true | 30 | -17.356 | -16.561 | 15 | -19.969 | -19.220 | 2 | 0.855 | 0.935 |
| `combo_ps_7030_shared` | true | 30 | -17.720 | -17.334 | 15 | -19.834 | -19.471 | 2 | 0.751 | 0.822 |
| `combo_se1_5050_shared` | true | 30 | -13.551 | -12.126 | 15 | -15.988 | -14.716 | 2 | -0.360 | -0.219 |
| `combo_se_3070_shared` | true | 30 | -8.502 | -7.167 | 15 | -6.177 | -4.963 | 2 | -1.649 | -1.492 |
| `combo_se_5050_shared` | true | 30 | -7.835 | -6.169 | 15 | -7.875 | -6.409 | 2 | -0.360 | -0.219 |
| `combo_se_5050_skip` | true | 30 | -3.254 | -1.644 | 15 | -3.295 | -1.896 | 2 | -0.360 | -0.219 |
| `combo_se_5050_split` | true | 30 | -53.389 | -7.280 | 15 | -47.666 | -5.996 | 2 | -1.237 | -1.079 |
| `combo_se_5050_weather` | true | 30 | -7.835 | -6.169 | 15 | -7.875 | -6.409 | 2 | -0.360 | -0.219 |
| `combo_se_7030_shared` | true | 30 | -7.020 | -5.311 | 15 | -8.289 | -6.749 | 2 | 0.447 | 0.599 |
| `combo_seh_333_shared` | true | 30 | 7.216 | 11.678 | 15 | 8.104 | 12.261 | 2 | -0.308 | -0.090 |
| `combo_seh_333_skip` | true | 30 | 10.553 | 14.870 | 15 | 11.468 | 15.470 | 2 | -0.308 | -0.090 |
| `combo_seh_333_split` | true | 30 | -30.508 | 5.362 | 15 | -25.013 | 7.196 | 2 | -1.089 | -0.856 |
| `combo_seh_333_weather` | true | 30 | 7.216 | 11.678 | 15 | 8.104 | 12.261 | 2 | -0.308 | -0.090 |
| `combo_seh_403525_shared` | true | 30 | 2.932 | 7.117 | 15 | 3.156 | 6.872 | 2 | -0.174 | 0.106 |
| `combo_seh_404020_shared` | true | 30 | -0.302 | 3.615 | 15 | 0.219 | 3.810 | 2 | -0.391 | -0.173 |
| `combo_seh_451540_shared` | true | 30 | 12.864 | 18.408 | 15 | 11.714 | 16.908 | 2 | 0.665 | 0.831 |
| `combo_seh_502525_shared` | true | 30 | 3.824 | 8.129 | 15 | 2.912 | 6.888 | 2 | 0.339 | 0.556 |
| `combo_seh_502525_split` | true | 30 | -24.025 | 2.302 | 15 | -22.769 | 2.766 | 2 | -0.538 | -0.302 |
| `combo_seh_601525_shared` | true | 30 | 4.679 | 9.144 | 15 | 3.046 | 7.206 | 2 | 0.833 | 1.011 |
| `combo_ser_5050_shared` | true | 30 | -12.638 | -11.299 | 15 | -12.676 | -11.525 | 2 | -0.360 | -0.219 |
| `combo_sf_3070_shared` | true | 30 | -8.387 | -6.619 | 15 | -5.730 | -3.951 | 2 | -0.750 | -0.632 |
| `combo_sf_5050_shared` | true | 30 | -3.764 | -2.786 | 15 | -3.147 | -2.183 | 2 | 0.367 | 0.462 |
| `combo_sf_7030_shared` | true | 30 | -2.881 | -1.882 | 15 | -3.480 | -2.503 | 2 | 0.859 | 0.943 |
| `combo_sh_3070_shared` | true | 30 | 33.073 | 39.782 | 15 | 32.946 | 39.353 | 2 | 0.650 | 0.704 |
| `combo_sh_5050_shared` | true | 30 | 20.903 | 26.891 | 15 | 18.910 | 24.602 | 2 | 1.261 | 1.319 |
| `combo_sh_7030_shared` | true | 30 | 9.021 | 13.055 | 15 | 6.602 | 10.407 | 2 | 1.425 | 1.495 |
| `combo_sh_macd_5050_shared` | true | 30 | 18.733 | 23.297 | 15 | 16.776 | 21.073 | 2 | 1.261 | 1.319 |
| `combo_sj_3070_shared` | true | 30 | -12.253 | -9.452 | 15 | -13.459 | -10.819 | 2 | -0.272 | -0.217 |
| `combo_sj_5050_shared` | true | 30 | -11.556 | -8.611 | 15 | -13.827 | -11.080 | 2 | 0.691 | 0.747 |
| `combo_sj_7030_shared` | true | 30 | -10.836 | -7.861 | 15 | -13.289 | -10.510 | 2 | 1.046 | 1.115 |
| `combo_sn_3070_shared` | true | 30 | -25.193 | -21.178 | 15 | -27.124 | -23.329 | 2 | -0.953 | -0.815 |
| `combo_sn_5050_shared` | true | 30 | -21.790 | -17.701 | 15 | -24.491 | -20.637 | 2 | 0.227 | 0.366 |
| `combo_sn_7030_shared` | true | 30 | -16.361 | -13.018 | 15 | -19.193 | -16.058 | 2 | 0.781 | 0.931 |
| `combo_snj_333_shared` | true | 30 | -21.730 | -16.436 | 15 | -23.340 | -18.370 | 2 | -0.597 | -0.380 |
| `flatten_h1` | false | 30 | -13.369 | -11.532 | 15 | -12.653 | -11.101 |  |  |  |
| `flatten_h3` | false | 30 | -21.682 | -20.412 | 15 | -17.024 | -15.813 |  |  |  |
| `flatten_h3_cut` | false | 30 | -21.682 | -20.412 | 15 | -17.024 | -15.813 |  |  |  |
| `flatten_h3_half` | false | 30 | -11.402 | -9.789 | 15 | -8.857 | -7.288 |  |  |  |
| `flatten_h3_rankw` | false | 30 | -18.371 | -17.089 | 15 | -12.989 | -11.701 |  |  |  |
| `flatten_h3_sboost` | false | 30 | -21.682 | -20.412 | 15 | -17.024 | -15.813 |  |  |  |
| `flatten_h3_sizeup` | false | 30 | -21.682 | -20.412 | 15 | -17.024 | -15.813 |  |  |  |
| `flatten_h3_time` | false | 30 | -21.682 | -20.412 | 15 | -17.024 | -15.813 |  |  |  |
| `flatten_h3_topheavy` | false | 30 | -20.719 | -19.500 | 15 | -13.344 | -12.127 |  |  |  |
| `flatten_h3_trail` | false | 30 | -21.682 | -20.412 | 15 | -17.024 | -15.813 |  |  |  |
| `flatten_h5` | false | 30 | -13.983 | -13.029 | 15 | -8.273 | -7.275 |  |  |  |
| `flatten_h5_cut` | false | 30 | -13.983 | -13.029 | 15 | -8.273 | -7.275 |  |  |  |
| `flatten_h5_half` | false | 30 | -7.962 | -6.566 | 15 | -5.023 | -3.586 |  |  |  |
| `flatten_h5_rankw` | false | 30 | -16.303 | -15.442 | 15 | -10.167 | -9.266 |  |  |  |
| `flatten_h5_s8` | false | 30 | -13.008 | -11.651 | 15 | -7.939 | -6.547 |  |  |  |
| `flatten_h5_sboost` | false | 30 | -13.983 | -13.029 | 15 | -8.273 | -7.275 |  |  |  |
| `flatten_h5_sizeup` | false | 30 | -13.983 | -13.029 | 15 | -8.273 | -7.275 |  |  |  |
| `flatten_h5_time` | false | 30 | -13.983 | -13.029 | 15 | -8.273 | -7.275 |  |  |  |
| `flatten_h5_topheavy` | false | 30 | -17.322 | -16.256 | 15 | -9.269 | -8.149 |  |  |  |
| `flatten_h5_trail` | false | 30 | -13.983 | -13.029 | 15 | -8.273 | -7.275 |  |  |  |
| `flatten_live_h1` | false | 30 | -1.724 | -1.045 | 15 | -0.902 | -0.583 |  |  |  |
| `flatten_live_h1_cut` | false | 30 | -1.724 | -1.045 | 15 | -0.902 | -0.583 |  |  |  |
| `flatten_live_h1_half` | false | 30 | -0.927 | -0.521 | 15 | -0.491 | -0.290 |  |  |  |
| `flatten_live_h1_rankw` | false | 30 | -2.647 | -1.872 | 15 | -1.034 | -0.667 |  |  |  |
| `flatten_live_h1_sboost` | false | 30 | -1.724 | -1.045 | 15 | -0.902 | -0.583 |  |  |  |
| `flatten_live_h1_sizeup` | false | 30 | -1.724 | -1.045 | 15 | -0.902 | -0.583 |  |  |  |
| `flatten_live_h1_time` | false | 30 | -1.724 | -1.045 | 15 | -0.902 | -0.583 |  |  |  |
| `flatten_live_h1_topheavy` | false | 30 | -1.749 | -1.045 | 15 | -0.661 | -0.329 |  |  |  |
| `flatten_live_h1_trail` | false | 30 | -1.724 | -1.045 | 15 | -0.902 | -0.583 |  |  |  |
| `flatten_live_h3` | false | 30 | -9.531 | -8.867 | 15 | -2.849 | -2.174 |  |  |  |
| `flatten_live_h5` | false | 30 | -9.666 | -8.976 | 15 | -2.993 | -2.291 |  |  |  |
| `flatten_vol_g_h3` | false | 30 | -28.402 | -27.747 | 15 | -21.972 | -21.273 |  |  |  |
| `flatten_white_yday_h5` | false | 30 | -4.969 | -4.953 | 15 | 0.737 | 0.757 |  |  |  |
| `ohlc_hot_coil_h1` | false | 30 | -1.846 | -0.348 | 15 | -0.892 | 0.499 |  |  |  |
| `ohlc_hot_h1` | false | 30 | -1.398 | 1.494 | 15 | -0.597 | 2.146 |  |  |  |
| `ohlc_hot_h3` | false | 30 | -11.712 | -10.179 | 15 | -3.658 | -2.071 |  |  |  |
| `ohlc_hot_h5` | false | 30 | -3.405 | -2.386 | 15 | 0.363 | 1.417 |  |  |  |
| `oppset_h1` | false | 30 | -4.525 | -3.360 | 15 | -3.330 | -2.258 |  |  |  |
| `overnight_h1` | false | 30 | -21.604 | -20.462 | 15 | -21.672 | -20.509 |  |  |  |
| `overnight_h3` | false | 30 | -17.041 | -16.023 | 15 | -12.150 | -11.614 |  |  |  |
| `overnight_h5` | false | 30 | -21.040 | -20.006 | 15 | -14.797 | -13.734 |  |  |  |
| `overnight_mega_green_h1` | false | 30 | 0.000 | 0.000 | 15 | 0.000 | 0.000 |  |  |  |
| `overnight_mega_h1` | false | 30 | 0.000 | 0.000 | 15 | 0.000 | 0.000 |  |  |  |
| `overnight_mega_h2` | false | 30 | 0.000 | 0.000 | 15 | 0.000 | 0.000 |  |  |  |
| `probable_h1` | false | 30 | -6.514 | -1.940 | 15 | -6.026 | -1.626 |  |  |  |
| `probable_h3` | false | 30 | -12.800 | -10.598 | 15 | -10.839 | -8.934 |  |  |  |
| `probable_h5` | false | 30 | -7.249 | -5.600 | 15 | -6.724 | -5.080 |  |  |  |
| `probable_probable_ok_h1` | true | 30 | -3.476 | -0.354 | 15 | -2.907 | 0.285 | 2 | -1.193 | -1.119 |
| `probable_probable_ok_h3` | true | 30 | -20.801 | -19.552 | 15 | -12.202 | -10.908 | 2 | -1.193 | -1.119 |
| `short_alarm_h1` | false | 30 | -0.034 | 1.365 | 15 | -0.034 | 1.365 |  |  |  |
| `short_alarm_h3` | false | 30 | 0.458 | 1.614 | 15 | -0.315 | 0.543 |  |  |  |
| `short_clk_ext_veto_h3` | false | 30 | -8.007 | -6.831 | 15 | -9.480 | -8.428 |  |  |  |
| `short_clk_ext_veto_opp_h3` | false | 30 | 3.164 | 3.319 | 15 | -0.021 | 0.098 |  |  |  |
| `short_clk_neg_weak_fail_h3` | true | 30 | -0.818 | 2.554 | 15 | 2.292 | 5.451 | 2 | 0.481 | 0.778 |
| `short_clk_neg_weak_fail_opp_h3` | true | 30 | -5.798 | -4.832 | 15 | -3.865 | -3.003 | 2 | 0.738 | 0.872 |
| `short_extended_h1` | false | 30 | -5.791 | -2.854 | 15 | -6.325 | -3.597 |  |  |  |
| `short_extended_h3` | false | 30 | -16.235 | -13.982 | 15 | -18.501 | -16.495 |  |  |  |
| `short_last_red_h1` | false | 30 | 0.984 | 4.611 | 15 | 1.028 | 4.445 |  |  |  |
| `short_last_red_h3` | false | 30 | 1.248 | 4.310 | 15 | -2.045 | 0.697 |  |  |  |
| `short_macd_dn_h1` | false | 30 | -2.251 | 1.787 | 15 | -2.015 | 1.869 |  |  |  |
| `short_macd_dn_h3` | false | 30 | 1.754 | 5.404 | 15 | 2.663 | 6.096 |  |  |  |
| `short_news_head_h3` | true | 30 | -8.576 | -7.837 | 15 | -11.948 | -11.223 | 2 | 1.199 | 1.184 |
| `short_news_or_h3` | true | 30 | -7.517 | -6.742 | 15 | -10.427 | -9.682 | 2 | 1.649 | 1.657 |
| `short_news_pack_h3` | true | 30 | 3.055 | 2.998 | 15 | 1.935 | 1.887 | 2 | 2.217 | 2.205 |
| `short_news_r_h1` | true | 30 | -2.547 | -1.702 | 15 | -2.256 | -1.571 | 2 | 1.649 | 1.657 |
| `short_news_r_h3` | true | 30 | -7.517 | -6.742 | 15 | -10.427 | -9.682 | 2 | 1.649 | 1.657 |
| `short_news_r_macd_h3` | true | 30 | -1.772 | -1.155 | 15 | -4.861 | -4.270 | 2 | 1.649 | 1.657 |
| `short_r_down_h1` | false | 30 | 0.000 | 0.000 | 15 | 0.000 | 0.000 |  |  |  |
| `short_r_down_h3` | false | 30 | 0.000 | 0.000 | 15 | 0.000 | 0.000 |  |  |  |
| `short_rsi_ob_h1` | false | 30 | -1.400 | 1.042 | 15 | -1.611 | 0.607 |  |  |  |
| `short_rsi_ob_h3` | false | 30 | -14.458 | -12.662 | 15 | -17.636 | -16.028 |  |  |  |
| `union_ab_g_h1` | false | 30 | -13.105 | -9.918 | 15 | -12.388 | -9.481 |  |  |  |
| `union_ab_g_h3` | false | 30 | -24.524 | -22.926 | 15 | -17.858 | -16.254 |  |  |  |
| `union_blue_coil_h1` | true | 30 | -15.610 | -11.703 | 15 | -15.069 | -11.310 | 2 | -2.913 | -2.807 |
| `union_blue_coil_h3` | true | 30 | -22.153 | -20.256 | 15 | -15.504 | -13.555 | 2 | -2.913 | -2.807 |
| `union_blue_h1` | false | 30 | -14.751 | -11.123 | 15 | -14.033 | -10.704 |  |  |  |
| `union_blue_h3` | false | 30 | -22.089 | -20.456 | 15 | -14.994 | -13.328 |  |  |  |
| `union_blue_vol_h1` | true | 30 | -26.025 | -21.282 | 15 | -25.280 | -20.712 | 2 | -2.023 | -1.991 |
| `union_blue_vol_h3` | true | 30 | -16.832 | -15.268 | 15 | -11.193 | -9.567 | 2 | -2.023 | -1.991 |
| `union_break10_h1` | false | 30 | -8.044 | -4.851 | 15 | -7.073 | -3.998 |  |  |  |
| `union_break10_h3` | false | 30 | 6.137 | 8.490 | 15 | 10.180 | 12.436 |  |  |  |
| `union_candle_h1` | false | 30 | -12.125 | -8.773 | 15 | -11.339 | -8.167 |  |  |  |
| `union_candle_h3` | false | 30 | -16.337 | -14.500 | 15 | -15.677 | -14.041 |  |  |  |
| `union_candle_score_h1` | false | 30 | -4.896 | -2.256 | 15 | -4.439 | -1.893 |  |  |  |
| `union_candle_score_h3` | false | 30 | -8.376 | -7.123 | 15 | -2.411 | -1.196 |  |  |  |
| `union_catal_present_h1` | true | 30 | -5.502 | -5.142 | 15 | -5.502 | -5.142 | 2 | 0.000 | 0.000 |
| `union_catal_present_h3` | true | 30 | -11.493 | -11.156 | 15 | -11.493 | -11.156 | 2 | 0.000 | 0.000 |
| `union_clk_earn_guide_react_h1` | true | 30 | -4.370 | -4.492 | 15 | -4.370 | -4.492 | 2 | -0.211 | -0.264 |
| `union_clk_flow_coil_h1` | false | 30 | -4.548 | -4.665 | 15 | 0.041 | -0.154 |  |  |  |
| `union_clk_fresh_cat_coil_h1` | true | 30 | -1.894 | 0.429 | 15 | -1.372 | 0.786 | 2 | -3.097 | -3.016 |
| `union_clk_fresh_cat_coil_opp_h1` | true | 30 | 4.759 | 5.885 | 15 | 5.449 | 6.463 | 2 | -2.040 | -1.946 |
| `union_clk_hold_vs_sector_h1` | false | 30 | -5.491 | -1.740 | 15 | -6.058 | -2.483 |  |  |  |
| `union_clk_hold_vs_sector_opp_h1` | false | 30 | 0.831 | 1.538 | 15 | -0.656 | 0.020 |  |  |  |
| `union_clk_insider_cash_stab_h3` | false | 30 | -8.378 | -8.575 | 15 | -3.997 | -4.151 |  |  |  |
| `union_clk_mom_break_peer_h1` | false | 30 | -4.979 | -2.757 | 15 | -3.519 | -1.387 |  |  |  |
| `union_clk_mom_break_peer_opp_h1` | false | 30 | -0.351 | 0.792 | 15 | 0.459 | 1.509 |  |  |  |
| `union_clk_nr7_mom_h1` | false | 30 | 7.070 | 7.622 | 15 | 12.218 | 12.714 |  |  |  |
| `union_clk_nr7_mom_opp_h1` | false | 30 | 0.312 | 0.310 | 15 | 0.312 | 0.310 |  |  |  |
| `union_clk_r_up_coil_h1` | false | 30 | 0.000 | 0.000 | 15 | 0.000 | 0.000 |  |  |  |
| `union_coil_green_h1` | true | 30 | -6.326 | -3.126 | 15 | -4.471 | -1.332 | 2 | -2.094 | -1.948 |
| `union_coil_green_h3` | true | 30 | -18.440 | -17.000 | 15 | -12.572 | -11.184 | 2 | -2.094 | -1.948 |
| `union_coil_off_h1` | false | 30 | -11.854 | -8.429 | 15 | -11.268 | -8.026 |  |  |  |
| `union_coil_off_h3` | false | 30 | -22.996 | -20.867 | 15 | -17.904 | -15.815 |  |  |  |
| `union_coil_off_h5` | false | 30 | -10.736 | -9.558 | 15 | -5.873 | -4.635 |  |  |  |
| `union_cond_h1` | false | 30 | -13.611 | -11.123 | 15 | -13.011 | -10.720 |  |  |  |
| `union_cond_h3` | false | 30 | -17.573 | -16.095 | 15 | -12.084 | -10.598 |  |  |  |
| `union_cond_n4_h3` | false | 30 | -16.485 | -15.848 | 15 | -8.150 | -7.479 |  |  |  |
| `union_e_fresh_h1` | false | 30 | -8.398 | -7.690 | 15 | -7.767 | -7.169 |  |  |  |
| `union_e_fresh_h3` | false | 30 | -8.396 | -7.772 | 15 | -2.152 | -1.610 |  |  |  |
| `union_e_green_h1` | true | 30 | -4.569 | -4.602 | 15 | -3.914 | -3.967 | 2 | -1.130 | -1.031 |
| `union_e_green_h3` | true | 30 | -5.047 | -4.901 | 15 | 1.743 | 1.777 | 2 | -1.130 | -1.031 |
| `union_earn_react_h1` | false | 30 | -13.195 | -12.577 | 15 | -12.597 | -12.084 |  |  |  |
| `union_earn_react_h3` | false | 30 | -17.447 | -16.962 | 15 | -11.820 | -11.413 |  |  |  |
| `union_flow_in_h1` | false | 30 | 0.841 | 1.339 | 15 | 1.272 | 1.568 |  |  |  |
| `union_flow_in_h3` | false | 30 | -11.745 | -11.292 | 15 | -8.174 | -7.709 |  |  |  |
| `union_flow_in_h5` | false | 30 | -6.342 | -5.956 | 15 | -2.675 | -2.282 |  |  |  |
| `union_flow_in_white_h1` | true | 30 | -0.730 | 0.076 | 15 | -2.310 | -1.814 | 2 | 0.000 | 0.000 |
| `union_flow_in_white_h3` | true | 30 | -7.455 | -6.819 | 15 | -3.652 | -2.986 | 2 | 0.000 | 0.000 |
| `union_h1` | false | 30 | -12.200 | -9.326 | 15 | -11.456 | -8.894 |  |  |  |
| `union_h1_cut` | false | 30 | -12.275 | -9.401 | 15 | -11.532 | -8.969 |  |  |  |
| `union_h1_half` | false | 30 | -7.348 | -4.539 | 15 | -6.918 | -4.295 |  |  |  |
| `union_h1_rankw` | false | 30 | -10.199 | -7.431 | 15 | -8.715 | -6.298 |  |  |  |
| `union_h1_sboost` | false | 30 | -13.863 | -10.162 | 15 | -13.133 | -9.734 |  |  |  |
| `union_h1_sizeup` | false | 30 | -12.200 | -9.326 | 15 | -11.456 | -8.894 |  |  |  |
| `union_h1_time` | false | 30 | -12.275 | -9.401 | 15 | -11.532 | -8.969 |  |  |  |
| `union_h1_topheavy` | false | 30 | -8.278 | -5.691 | 15 | -7.269 | -5.016 |  |  |  |
| `union_h1_trail` | false | 30 | -12.200 | -9.326 | 15 | -11.456 | -8.894 |  |  |  |
| `union_h3` | false | 30 | -19.684 | -17.847 | 15 | -13.962 | -12.165 |  |  |  |
| `union_h3_cut` | false | 30 | -19.684 | -17.847 | 15 | -13.962 | -12.165 |  |  |  |
| `union_h3_exit_alarm` | false | 30 | -21.013 | -18.867 | 15 | -14.330 | -12.166 |  |  |  |
| `union_h3_exit_news_r` | true | 30 | -22.066 | -20.462 | 15 | -14.803 | -13.197 | 2 | -2.790 | -2.651 |
| `union_h3_exit_red` | false | 30 | -14.159 | -12.547 | 15 | -10.762 | -9.274 |  |  |  |
| `union_h3_half` | false | 30 | -10.929 | -8.859 | 15 | -7.830 | -5.828 |  |  |  |
| `union_h3_rankw` | false | 30 | -19.031 | -17.363 | 15 | -11.841 | -10.196 |  |  |  |
| `union_h3_sboost` | false | 30 | -19.470 | -17.382 | 15 | -13.732 | -11.667 |  |  |  |
| `union_h3_sizeup` | false | 30 | -19.684 | -17.847 | 15 | -13.962 | -12.165 |  |  |  |
| `union_h3_time` | false | 30 | -19.684 | -17.847 | 15 | -13.962 | -12.165 |  |  |  |
| `union_h3_topheavy` | false | 30 | -18.947 | -17.373 | 15 | -10.749 | -9.185 |  |  |  |
| `union_h3_trail` | false | 30 | -19.684 | -17.847 | 15 | -13.962 | -12.165 |  |  |  |
| `union_h5` | false | 30 | -7.374 | -6.212 | 15 | -1.628 | -0.420 |  |  |  |
| `union_h5_cut` | false | 30 | -7.374 | -6.212 | 15 | -1.628 | -0.420 |  |  |  |
| `union_h5_exit_alarm` | false | 30 | -8.671 | -7.254 | 15 | -1.370 | 0.143 |  |  |  |
| `union_h5_half` | false | 30 | -6.689 | -4.838 | 15 | -3.905 | -2.002 |  |  |  |
| `union_h5_rankw` | false | 30 | -12.698 | -11.286 | 15 | -5.092 | -3.610 |  |  |  |
| `union_h5_sboost` | false | 30 | -8.792 | -7.303 | 15 | -3.135 | -1.578 |  |  |  |
| `union_h5_sizeup` | false | 30 | -7.374 | -6.212 | 15 | -1.628 | -0.420 |  |  |  |
| `union_h5_time` | false | 30 | -7.374 | -6.212 | 15 | -1.628 | -0.420 |  |  |  |
| `union_h5_topheavy` | false | 30 | -12.716 | -11.370 | 15 | -4.692 | -3.271 |  |  |  |
| `union_h5_trail` | false | 30 | -7.374 | -6.212 | 15 | -1.628 | -0.420 |  |  |  |
| `union_hot_n12_h1` | false | 30 | 3.835 | 8.420 | 15 | 5.443 | 9.800 |  |  |  |
| `union_hot_n4_h1` | false | 30 | 24.991 | 29.536 | 15 | 28.623 | 32.974 |  |  |  |
| `union_hot_n4_holdup` | false | 30 | 52.196 | 55.918 | 15 | 55.026 | 58.392 |  |  |  |
| `union_hot_score_h1` | false | 30 | 11.788 | 15.765 | 15 | 13.746 | 17.508 |  |  |  |
| `union_hot_score_h3` | false | 30 | 13.807 | 15.881 | 15 | 19.999 | 22.025 |  |  |  |
| `union_join_g_h1` | false | 30 | -10.258 | -7.443 | 15 | -9.502 | -7.006 |  |  |  |
| `union_join_g_h3` | false | 30 | -21.940 | -20.399 | 15 | -15.034 | -13.480 |  |  |  |
| `union_join_present_h1` | false | 30 | -10.748 | -7.766 | 15 | -9.991 | -7.324 |  |  |  |
| `union_join_present_h3` | false | 30 | -20.635 | -18.866 | 15 | -13.920 | -12.165 |  |  |  |
| `union_join_vol_green_h1` | true | 30 | -4.005 | -1.931 | 15 | -2.897 | -0.908 | 2 | -1.834 | -1.819 |
| `union_join_vol_green_h3` | true | 30 | 2.089 | 3.575 | 15 | 8.626 | 10.174 | 2 | -1.834 | -1.819 |
| `union_last_green_h1` | false | 30 | -3.308 | -0.422 | 15 | -2.172 | 0.605 |  |  |  |
| `union_last_green_h3` | false | 30 | -14.326 | -12.747 | 15 | -10.936 | -9.482 |  |  |  |
| `union_last_green_h5` | false | 30 | -4.272 | -3.358 | 15 | -0.358 | 0.588 |  |  |  |
| `union_last_red_h1` | false | 30 | -12.781 | -8.058 | 15 | -12.046 | -7.624 |  |  |  |
| `union_last_red_h3` | false | 30 | -26.577 | -24.367 | 15 | -20.162 | -17.955 |  |  |  |
| `union_macd_hist_h1` | false | 30 | 0.689 | 2.297 | 15 | 1.211 | 2.703 |  |  |  |
| `union_macd_hist_h3` | false | 30 | -9.616 | -8.306 | 15 | -6.366 | -5.116 |  |  |  |
| `union_macd_up_h1` | false | 30 | -14.174 | -10.734 | 15 | -13.261 | -10.086 |  |  |  |
| `union_macd_up_h3` | false | 30 | -6.789 | -4.879 | 15 | 0.933 | 2.827 |  |  |  |
| `union_macd_xup_h1` | false | 30 | -1.941 | 1.153 | 15 | -2.253 | 0.709 |  |  |  |
| `union_macd_xup_h3` | false | 30 | -5.781 | -3.925 | 15 | -1.719 | -0.024 |  |  |  |
| `union_news_both_h1` | true | 30 | -3.119 | -3.179 | 15 | -3.119 | -3.179 | 2 | 0.000 | 0.000 |
| `union_news_both_h3` | true | 30 | -5.169 | -5.229 | 15 | -5.169 | -5.229 | 2 | 0.000 | 0.000 |
| `union_news_g_cam61_h1` | true | 30 | -20.384 | -20.324 | 15 | -20.671 | -20.607 | 2 | -3.825 | -3.772 |
| `union_news_g_cam61_h3` | true | 30 | -26.174 | -26.170 | 15 | -20.879 | -20.924 | 2 | -3.825 | -3.772 |
| `union_news_g_cam71_conv_h1` | true | 30 | -13.611 | -13.875 | 15 | -12.711 | -12.972 | 2 | -2.174 | -2.178 |
| `union_news_g_cam71_h1` | true | 30 | -12.598 | -12.764 | 15 | -12.915 | -13.076 | 2 | -3.486 | -3.451 |
| `union_news_g_cam71_h3` | true | 30 | -26.201 | -26.248 | 15 | -20.484 | -20.572 | 2 | -3.486 | -3.451 |
| `union_news_g_cam71_n2_h1` | true | 30 | -12.360 | -12.511 | 15 | -12.972 | -13.115 | 2 | -5.022 | -5.054 |
| `union_news_g_cam91_n1_h1` | true | 30 | -22.111 | -22.376 | 15 | -20.446 | -20.676 | 2 | 0.000 | 0.000 |
| `union_news_g_cond_h1` | true | 30 | -14.225 | -11.260 | 15 | -14.864 | -12.045 | 2 | -2.605 | -2.511 |
| `union_news_g_cond_h3` | true | 30 | -20.618 | -18.918 | 15 | -17.242 | -15.565 | 2 | -2.605 | -2.511 |
| `union_news_g_conv_h1` | true | 30 | -14.707 | -13.487 | 15 | -14.188 | -12.971 | 2 | -2.174 | -2.178 |
| `union_news_g_conv_h3` | true | 30 | -12.887 | -12.030 | 15 | -7.936 | -7.029 | 2 | -2.174 | -2.178 |
| `union_news_g_h1` | true | 30 | -14.342 | -11.342 | 15 | -14.981 | -12.127 | 2 | -2.710 | -2.609 |
| `union_news_g_h3` | true | 30 | -19.733 | -18.085 | 15 | -16.902 | -15.297 | 2 | -2.710 | -2.609 |
| `union_news_g_h5` | true | 30 | -15.929 | -14.951 | 15 | -13.470 | -12.471 | 2 | -2.710 | -2.609 |
| `union_news_head_h1` | true | 30 | -12.430 | -9.226 | 15 | -13.838 | -10.870 | 2 | -2.737 | -2.623 |
| `union_news_head_h3` | true | 30 | -20.146 | -18.749 | 15 | -16.905 | -15.568 | 2 | -2.737 | -2.623 |
| `union_news_missing_h1` | true | 30 | 0.000 | 0.000 | 15 | 0.000 | 0.000 | 2 | 0.000 | 0.000 |
| `union_news_missing_h3` | true | 30 | 0.000 | 0.000 | 15 | 0.000 | 0.000 | 2 | 0.000 | 0.000 |
| `union_news_or_h1` | true | 30 | -14.225 | -11.260 | 15 | -14.864 | -12.045 | 2 | -2.605 | -2.511 |
| `union_news_or_h3` | true | 30 | -20.618 | -18.918 | 15 | -17.242 | -15.565 | 2 | -2.605 | -2.511 |
| `union_news_or_net2_h1` | true | 30 | -13.512 | -11.872 | 15 | -15.021 | -13.528 | 2 | -2.605 | -2.511 |
| `union_news_or_net2_h3` | true | 30 | -17.992 | -16.987 | 15 | -15.578 | -14.637 | 2 | -2.605 | -2.511 |
| `union_news_or_net3_h1` | true | 30 | -21.826 | -20.700 | 15 | -23.190 | -22.191 | 2 | -2.605 | -2.511 |
| `union_news_or_net3_h3` | true | 30 | -23.545 | -22.898 | 15 | -21.295 | -20.715 | 2 | -2.605 | -2.511 |
| `union_news_or_net4_conv_h1` | true | 30 | -19.679 | -19.269 | 15 | -19.191 | -18.787 | 2 | -2.174 | -2.178 |
| `union_news_or_net4_h1` | true | 30 | -20.664 | -19.547 | 15 | -22.048 | -21.060 | 2 | -2.605 | -2.511 |
| `union_news_or_net4_h3` | true | 30 | -23.545 | -22.892 | 15 | -21.295 | -20.709 | 2 | -2.605 | -2.511 |
| `union_news_or_net4_rw_h1` | true | 30 | -20.788 | -20.361 | 15 | -21.306 | -20.895 | 2 | -3.697 | -3.699 |
| `union_news_or_net5_h1` | true | 30 | -18.799 | -18.103 | 15 | -20.190 | -19.517 | 2 | -3.363 | -3.290 |
| `union_news_or_net5_h3` | true | 30 | -23.052 | -22.360 | 15 | -19.586 | -18.934 | 2 | -3.363 | -3.290 |
| `union_news_pack_h1` | true | 30 | -12.867 | -13.080 | 15 | -12.885 | -13.089 | 2 | -2.054 | -2.001 |
| `union_news_pack_h3` | true | 30 | -17.680 | -17.677 | 15 | -13.070 | -13.123 | 2 | -2.054 | -2.001 |
| `union_news_pack_net2_h1` | true | 30 | -11.767 | -11.912 | 15 | -13.449 | -13.563 | 2 | -2.038 | -2.002 |
| `union_news_pack_net2_h3` | true | 30 | -16.105 | -16.174 | 15 | -12.986 | -13.096 | 2 | -2.038 | -2.002 |
| `union_news_pack_net3_h1` | true | 30 | -21.657 | -21.711 | 15 | -23.151 | -23.178 | 2 | -2.038 | -2.002 |
| `union_news_pack_net3_h3` | true | 30 | -24.603 | -24.695 | 15 | -21.799 | -21.930 | 2 | -2.038 | -2.002 |
| `union_news_present_h1` | true | 30 | -10.748 | -7.766 | 15 | -9.991 | -7.324 | 2 | -2.912 | -2.775 |
| `union_news_present_h3` | true | 30 | -20.635 | -18.866 | 15 | -13.920 | -12.165 | 2 | -2.912 | -2.775 |
| `union_news_vol_h1` | true | 30 | -20.015 | -17.301 | 15 | -21.719 | -19.315 | 2 | -3.254 | -3.175 |
| `union_news_vol_h3` | true | 30 | -16.363 | -15.421 | 15 | -15.601 | -14.715 | 2 | -3.254 | -3.175 |
| `union_oppset_h1` | false | 30 | -5.360 | -4.168 | 15 | -4.176 | -3.075 |  |  |  |
| `union_overnight_h1` | false | 30 | -21.793 | -20.680 | 15 | -21.860 | -20.726 |  |  |  |
| `union_overnight_h3` | false | 30 | -17.041 | -16.023 | 15 | -12.150 | -11.614 |  |  |  |
| `union_r_up_h1` | false | 30 | 0.000 | 0.000 | 15 | 0.000 | 0.000 |  |  |  |
| `union_r_up_h3` | false | 30 | 0.000 | 0.000 | 15 | 0.000 | 0.000 |  |  |  |
| `union_ret_5_h1` | false | 30 | 10.160 | 15.035 | 15 | 11.845 | 16.326 |  |  |  |
| `union_ret_5_h3` | false | 30 | 7.997 | 11.005 | 15 | 12.969 | 15.734 |  |  |  |
| `union_rsi_h1` | false | 30 | -25.120 | -20.963 | 15 | -24.122 | -20.183 |  |  |  |
| `union_rsi_h3` | false | 30 | -16.109 | -13.432 | 15 | -13.462 | -10.935 |  |  |  |
| `union_rsi_os_h1` | false | 30 | -26.224 | -22.131 | 15 | -22.979 | -19.244 |  |  |  |
| `union_rsi_os_h3` | false | 30 | -10.167 | -7.416 | 15 | -6.341 | -3.895 |  |  |  |
| `union_rsi_os_h5` | false | 30 | -18.494 | -15.936 | 15 | -15.858 | -13.242 |  |  |  |
| `union_rsi_os_macd_h1` | true | 30 | 3.677 | 5.001 | 15 | 4.597 | 5.816 | 2 | 0.000 | 0.000 |
| `union_rsi_os_macd_h3` | true | 30 | 9.980 | 11.333 | 15 | 5.570 | 6.870 | 2 | 0.000 | 0.000 |
| `union_vol_ab_h1` | true | 30 | -7.092 | -3.543 | 15 | -5.653 | -2.319 | 2 | -1.746 | -1.593 |
| `union_vol_ab_h3` | true | 30 | -5.101 | -2.730 | 15 | 0.669 | 3.005 | 2 | -1.746 | -1.593 |
| `union_vol_g_h1` | false | 30 | -18.066 | -13.952 | 15 | -17.155 | -13.313 |  |  |  |
| `union_vol_g_h3` | false | 30 | -7.762 | -5.431 | 15 | -1.377 | 0.946 |  |  |  |
| `union_vol_g_h5` | false | 30 | -11.982 | -10.550 | 15 | -5.246 | -3.766 |  |  |  |
| `union_vol_green_h1` | true | 30 | -17.392 | -13.454 | 15 | -16.348 | -12.464 | 2 | -2.473 | -2.394 |
| `union_vol_green_h3` | true | 30 | -6.108 | -4.155 | 15 | 0.803 | 2.795 | 2 | -2.473 | -2.394 |
| `union_vol_missing_h1` | false | 30 | 0.000 | 0.000 | 15 | 0.000 | 0.000 |  |  |  |
| `union_vol_missing_h3` | false | 30 | 0.000 | 0.000 | 15 | 0.000 | 0.000 |  |  |  |
| `union_w_hot_candle_h1` | false | 30 | 7.690 | 11.261 | 15 | 9.360 | 12.746 |  |  |  |
| `union_w_hot_candle_h3` | false | 30 | 3.807 | 5.751 | 15 | 12.439 | 14.412 |  |  |  |
| `union_w_hot_cond_h1` | false | 30 | 12.012 | 15.180 | 15 | 14.687 | 17.659 |  |  |  |
| `union_w_hot_cond_h3` | false | 30 | 14.520 | 16.192 | 15 | 21.591 | 23.200 |  |  |  |
| `union_white_any_h1` | true | 30 | -13.109 | -10.789 | 15 | -12.376 | -10.365 | 2 | -3.581 | -3.487 |
| `union_white_any_h2` | true | 30 | -9.755 | -7.797 | 15 | -5.266 | -3.660 | 2 | -3.581 | -3.487 |
| `union_white_any_h3` | true | 30 | -19.604 | -18.100 | 15 | -11.799 | -10.259 | 2 | -3.581 | -3.487 |
| `union_white_any_h5` | true | 30 | -9.980 | -8.720 | 15 | -2.133 | -0.808 | 2 | -3.581 | -3.487 |
| `union_white_both_n4_h1` | true | 30 | -5.136 | -4.808 | 15 | -3.596 | -3.361 | 2 | -3.797 | -3.787 |
| `union_white_both_n4_h2` | true | 30 | -2.903 | -2.482 | 15 | -1.319 | -0.995 | 2 | -3.797 | -3.787 |
| `union_white_both_n4_h3` | true | 30 | -4.688 | -4.355 | 15 | -3.619 | -3.296 | 2 | -3.797 | -3.787 |
| `union_white_both_n4_h5` | true | 30 | 2.205 | 2.591 | 15 | 1.803 | 2.188 | 2 | -3.797 | -3.787 |
| `union_white_both_n4_h5_s12` | true | 30 | 3.765 | 4.271 | 15 | 3.357 | 3.862 | 2 | -3.797 | -3.787 |
| `union_white_coil_h1` | true | 30 | -17.995 | -16.346 | 15 | -17.470 | -15.974 | 2 | -3.159 | -3.067 |
| `union_white_coil_h3` | true | 30 | -21.343 | -20.415 | 15 | -14.914 | -14.018 | 2 | -3.159 | -3.067 |
| `union_white_h1` | false | 30 | -13.109 | -10.789 | 15 | -12.376 | -10.365 |  |  |  |
| `union_white_h3` | false | 30 | -19.604 | -18.100 | 15 | -11.799 | -10.259 |  |  |  |
| `union_white_h5` | false | 30 | -9.980 | -8.720 | 15 | -2.133 | -0.808 |  |  |  |
| `union_white_yday_h1` | false | 30 | -13.153 | -10.934 | 15 | -12.458 | -10.331 |  |  |  |
| `union_white_yday_h3` | false | 30 | -11.753 | -10.717 | 15 | -9.382 | -8.406 |  |  |  |
| `yday_gainer_h1` | false | 30 | -11.209 | -6.522 | 15 | -10.441 | -5.971 |  |  |  |
| `yday_gainer_h3` | false | 30 | -13.355 | -10.900 | 15 | -10.468 | -8.285 |  |  |  |
| `yday_gainer_h5` | false | 30 | -6.979 | -5.373 | 15 | -5.463 | -3.848 |  |  |  |
