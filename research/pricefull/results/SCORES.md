# Price-only fullscan scores

Window 2019-01-01 through 2026-08-12. Two variants. Holm N and luck-test N are 2.
Every section 4 id failed `REBUILD_MATCH=exact` on `research/audit/INPUT_PROVENANCE_336.md`, so both variants buy nobody.
This is a hindsight study. It is not a live strategy.

## Result

| variant | trades | trades per session | Futubull P&L | return | 15bp P&L | study |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `pricefull_w1d` | 0 | 0.0000 | 0.00 | 0.00% | 0.00 | fail |
| `pricefull_hot4` | 0 | 0.0000 | 0.00 | 0.00% | 0.00 | fail |

| variant | 9.1 mean after Holm | 9.2 years 2019–2024 | 9.3 best removed | 9.4 fires per year | 9.5 P&L from 2025-01-01 | keep bar |
| --- | --- | --- | --- | --- | --- | --- |
| `pricefull_w1d` | fail (p=1.0, Holm=1.0, no positive mean) | fail (0 of 6) | fail (rerun P&L 0.00) | fail (rate 0.00) | fail (P&L 0.00) | fail |
| `pricefull_hot4` | fail (p=1.0, Holm=1.0, no positive mean) | fail (0 of 6) | fail (rerun P&L 0.00) | fail (rate 0.00) | fail (P&L 0.00) | fail |

## Per variant

### `pricefull_w1d`

Trades (closed) 0. Buy fills 0. Trades per session 0.0000. Buy fills per session 0.0000.

| year | wins | losses | flat | closed P&L | buy fills |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2019 | 0 | 0 | 0 | 0.00 | 0 |
| 2020 | 0 | 0 | 0 | 0.00 | 0 |
| 2021 | 0 | 0 | 0 | 0.00 | 0 |
| 2022 | 0 | 0 | 0 | 0.00 | 0 |
| 2023 | 0 | 0 | 0 | 0.00 | 0 |
| 2024 | 0 | 0 | 0 | 0.00 | 0 |
| 2025 | 0 | 0 | 0 | 0.00 | 0 |
| 2026 | 0 | 0 | 0 | 0.00 | 0 |

Return without the best stock: 0.00% (P&L 0.00; removed nobody).
Return without the top 3: 0.00% (P&L 0.00; removed nobody).
Best stock's share of closed P&L: not defined (closed P&L is not strictly positive, and there is no best stock).

### `pricefull_hot4`

Trades (closed) 0. Buy fills 0. Trades per session 0.0000. Buy fills per session 0.0000.

| year | wins | losses | flat | closed P&L | buy fills |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2019 | 0 | 0 | 0 | 0.00 | 0 |
| 2020 | 0 | 0 | 0 | 0.00 | 0 |
| 2021 | 0 | 0 | 0 | 0.00 | 0 |
| 2022 | 0 | 0 | 0 | 0.00 | 0 |
| 2023 | 0 | 0 | 0 | 0.00 | 0 |
| 2024 | 0 | 0 | 0 | 0.00 | 0 |
| 2025 | 0 | 0 | 0 | 0.00 | 0 |
| 2026 | 0 | 0 | 0 | 0.00 | 0 |

Return without the best stock: 0.00% (P&L 0.00; removed nobody).
Return without the top 3: 0.00% (P&L 0.00; removed nobody).
Best stock's share of closed P&L: not defined (closed P&L is not strictly positive, and there is no best stock).

## Baselines

RANDOM4, seed 20260813, 1000 draws, 4 names, hold 1, Futubull, $10,000. Mean ending-equity return -99.96%.

| variant | draws whose ending equity this book beats |
| --- | ---: |
| `pricefull_w1d` | 100.00% |
| `pricefull_hot4` | 100.00% |

IWM buy-and-hold, one round trip, 2019-01-02 open to 2026-08-12 close. Futubull P&L 12,785.88 (127.86%). Flat 15bp P&L 12,775.63 (127.76%).

## Luck test

Seed 20260927. Reshuffles 10000. Permutation calls 19130000.

`pricefull_w1d` pooled 1-share picks 0. Real 1-share mean not defined. p_rule not defined. no pooled pick, so null_mean >= real_1share_mean is not defined.
`pricefull_hot4` pooled 1-share picks 0. Real 1-share mean not defined. p_rule not defined. no pooled pick, so null_mean >= real_1share_mean is not defined.

p_best not defined. no variant has at least one pooled pick.

## What was dropped

The audit file is on this commit. No section 4 id has `REBUILD_MATCH` equal to `exact`. A different spelling in that table (hot score, prior-day gainers, VIX, rates) is not the id. `A06_volume_red_green_2day` and `A14_profitable_oversold_setup` are on a REBUILD_MATCH row and the cell is not `exact`. A14 would stay out under section 3 even if that cell were exact.

With ab and peer both absent, `effective_weights` at the freeze returns the original 1d vector because no family survives. Section 5.2 then gives the variant no score. `pricefull_w1d` does not fall through to `hot_score`. `pricefull_hot4` does not use the full proxy as a backup list.

The Excel set is empty: zero cards at the locked commit have `cohort_filter` `ALL`. That set is not a variant and does not add to N. The second gate is not reached.

## Prices and survivorship

Fills would use the long-history Yahoo chart v8 `indicators.quote` cache (split-adjusted, not dividend-adjusted). `adj_close` is on the file and is not read. Both books have zero fills, so no trade interval contains a split ex-date. The long-history jump check classifies Yahoo split events against the cached open and the prior close. Those event payloads are not in this repository, and this run does not download a new tape or rewrite a stored bar.

Delisted candidates 8018. Yahoo does not chart 618: HTTP 404 281, HTTP 400 200, HTTP 200 with no bar on or before 2026-08-12 137. The study is not CRSP. The sign of the net bias is not known.

Sessions 1913, from 2019-01-02 through 2026-08-12.
