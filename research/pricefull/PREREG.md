# Price-only fullscan — preregistration

- status: locked before any score. This commit has no returns, no p-values, and no luck-test output.
- written: 2026-09-26
- fingerprint_sha256: 0dc51aeced8323f5466145e2311acd66a89319548ae5705db06c235b81200f3e
- fingerprint_scope: SHA-256 of the UTF-8 bytes after the line `<!-- BEGIN COVERED -->`, including the final newline. Line endings are LF.
- window: 2019-01-01 through 2026-08-12, NYSE (XNYS) sessions, scored one session at a time.
- live paths: not used. No edits to flatten_robust, Webull submit, the live factor-mine core, the stock book writer, or Supabase. Output of a later run lives only under `research/pricefull/`.
- audit file at the time of this commit: `research/audit/INPUT_PROVENANCE_336.md` is not on `main`. The inclusion rule below does not wait for it and does not invent marks.

<!-- BEGIN COVERED -->

## 0. What this file is

This is the protocol for one research study. It is a price-only copy of the fullscan selection and sizing. It is not a live strategy and it does not open a live record.

Every session in the window is before this file's date, so every session is `designed_after` (hindsight) under IRONCLAD rule 5. Nothing here counts toward a real record. No real money follows from it (IRONCLAD rule 23).

Changing a gate, weight, hold, exit, size, fee, fill, candidate id, or the variant count after this fingerprint is a new study, not a rerun.

This commit does not score a day and does not run a backtest.

Two variants are declared in section 7. The count is 2. That count is the Holm N and the luck-test N.

## 1. What is reused

Universe, bars, the delisted manifest, and fees are the long-history study's, as already written. This file does not re-tune them.

| piece | source | locked value |
| --- | --- | --- |
| Price source and bar hash | `research/longhist/PREREG.md` section 1 | Yahoo chart v8 daily `indicators.quote` open, high, low, close, volume. `adjclose` is not read. A missing quote drops that name-day. |
| Listed common stocks | same file, section 2.1 | 4,193 symbols. `listed_common.txt` sha256 `62609d9165bcbd5e7327516ea4da03f05297698d67aab660f207ddf8426195d5`. |
| Proxy panel | same file, section 3 | The `proxy60` rule: 20 prior bars, prior close in [$1, $30], 20-day mean dollar volume in [$1,000,000, $40,000,000], opening gap abs at least 0.04, prior-day rvol at least 1.5, cap 60 by abs gap then rvol then ticker. Today's high, low, close, and volume are not membership inputs. |
| Pre-open check | `research/longhist/ADDENDUM_1.md` section 2 | Price, dollar volume, and prior-day rvol use bars with `date < D` only. Session D contributes its official open to the gap and to fills. |
| Delisted manifest | `research/longhist/ADDENDUM_1.md` section 3 | `research/longhist/tickers.json` sha256 `40e5b09c5460d71d07947c62c1123d9cd43ec03cab6abcbbb3527acbe074e653`. |
| Fees | `research/longhist/PREREG.md` section 4 | Futubull via `paper_trade.order_fees`. Fee-model sha256 `3fc9b5829758722bd968e84baf0923f5efd8006d3b780a18e4d417552a9e80a7`. Flat 15bp is printed beside it and is not a pass rule. |
| Day loop | same file, section 5 | Sells first at today's open, then buys at today's open from leftover cash. `sell=time`. No take, no stop, `s_boost=none`, no hard-red sit. |

Survivorship, disclosed here so a later table cannot hide it. From the addendum's manifest, of the 8,018 delisted candidates, **618** do not chart on Yahoo (`n_yahoo_does_not_chart`). 281 of those are HTTP 404, 200 are HTTP 400, and 137 are HTTP 200 with no bar on or before 2026-08-12. Those 618 symbols are missing. The study is not CRSP. The sign of the net bias is not known. Thresholds are not changed by this count.

A later run may read the bar cache and `tickers.json` described in those two files. It may not read a long-history return, ledger, p-value, or luck-test output, and it may not change a stored bar.

## 2. Mechanical inclusion

An input is used only when `research/audit/INPUT_PROVENANCE_336.md` marks that input `REBUILD_MATCH=exact`.

The mark is read on the commit the run is built from. A candidate id in section 4 is exact only when that file exists and contains the id, and the same table row or the same bullet sets `REBUILD_MATCH` to the string `exact`. The match is case-sensitive. These are not exact: a missing file, a missing id, a different spelling, a different value, `REBUILD_MATCH` on a nearby row, or a mark in any other file.

Any id that is not exact is dropped. There is no substitute, no proxy feature, no second vendor, and no hand-filled value. Dropped means the variant does not read it. The variant does not invent a replacement column.

A forever exclusion in section 3 wins over an exact mark. A Finviz snapshot field, a news field, an LLM score, or a Theme Radar score that the audit marks exact is still dropped.

If the audit file is still absent when a run starts, every candidate id is not exact, every candidate is dropped, and both variants buy nobody. The run reports that fact. It does not pause to guess.

## 3. Excluded forever

These never enter a gate, a rank, a size, or a fill, even if a file marks them exact:

- Every LLM or AI score. That includes general predict, sector essays, weather stance built from those scores, Grok review, news judge, Finviz digest tilts, catalyst dossiers, map-heat captain essays, and the morning S score.
- News. Headlines, parses, actions, and the last-24h news table inside a channel-1 file.
- Finviz snapshot fields. Change%, gap, relative volume, RSI, SMAs, market cap, average volume, beta, earnings date, sector, industry, and any other column of a `finviz_*.csv`. A number recomputed from Yahoo bars is not a Finviz column. The column itself is out.
- Theme Radar scores and flags, including `oppset`, `opp_*`, and `flag_*`.

Hard-red sit is the morning S gate. S is an LLM score, so hard-red is off for every session. `book_policy.json` learned weights are not read. Code defaults only.

## 4. Candidate inputs

Each row is one id. The code named is the freeze in section 5. A later run uses that code only for an id the audit has marked exact, and only inside the variant that section 7 says reads it.

### 4.1 Price features

Computed from that symbol's Yahoo bars with `date < D`. Session D's open is not an input to these, except `overnight_gap`, which is the proxy gap and uses D's official open.

| id | what it is |
| --- | --- |
| `hot_score` | `ohlc_ripper.hot_score` on `from_bars` of the last 60 prior bars. The formula at the freeze is `0.08 * max(ret_5, 0) + 0.04 * max(ret_10, 0) + 0.4 * min(rvol, 3) + 1.2 if break_10 else 0 + 0.3 if last_green else 0`, and 0 when `ok` is false. |
| `ret_1` | Prior 1-session percent from `from_bars`. |
| `ret_5` | Prior 5-session percent from `from_bars`. |
| `ret_10` | Prior 10-session percent from `from_bars`. |
| `rvol` | `volume[-1] / mean(volume[-20:])` from `from_bars`, the same prior-day ratio the proxy uses. |
| `break_10` | Prior close above the max high of the 10 bars before that close (`from_bars`). |
| `last_green` | Prior bar `close > open`. |
| `candle_score` | `candle_factor` score on the last 8 prior bars. |
| `rsi` | Wilder RSI(14) from `from_bars`. `rsi_os` and `rsi_ob` are this series at 30 and 70. They are not separate ids. |
| `macd` | MACD(12, 26, 9) line, signal, histogram, and the up/down cross flags from `from_bars`. One id. |
| `overnight_gap` | `G = open_D / prior_close - 1`, the proxy gap. It is already a membership test. It is not a second threshold. |

`hot_score` is exact only when its own id is exact and `ret_5`, `ret_10`, `rvol`, `break_10`, and `last_green` are exact. A partial formula is not hot_score. There is no substitute formula.

### 4.2 Price-only candidate sources

These are lists. A name can be on a list only if it is already in that morning's proxy panel. A list never adds a name the proxy rejected. The live functions that read a Finviz column are not the implementation. The Finviz column is excluded by section 3.

| id | price-only list, if and only if this id is exact |
| --- | --- |
| `yday_gainer` | Top 25 proxy names by prior-session close-to-close return, then ticker A→Z. 25 is the `top_n` in `factor_mine._candidates` at the freeze, not the 50 default on `yesterday_gainers`. |
| `yday_mover` | Top 20 proxy names by absolute prior-session close-to-close return, then ticker A→Z. 20 is the `top_n` in `_candidates`. |
| `ohlc_hot` | Top 30 proxy names by `hot_score`, then ticker A→Z. 30 is the `liquid_hot` `top_n` in `_candidates`. Requires `hot_score` exact. The Finviz liquid-tape filter inside `liquid_hot` is not used. |
| `overnight_move` | The proxy names themselves, ordered by absolute `overnight_gap`, then ticker A→Z. No extra cutoff. Requires `overnight_gap` exact. |

The earnings calendars `overnight_scheduled` and `overnight_mega` read Finviz earnings date and market cap. They are section 3 exclusions. They are not `overnight_move` and they are not a fallback if `overnight_move` is dropped.

### 4.3 AB checklist, the fifteen A-rules

The price rules are the A-rules in `src/ab_checklist.py` (base sha `01d6380c8ed6dfff34afc78feed675386b26bf68`, plus the A15 patch in that file). B-rules are earnings and Finviz fundamentals. Every B-rule is a section 3 exclusion.

| id | rule |
| --- | --- |
| `A01_rsi_value` | RSI level |
| `A02_rsi_cross_30` | RSI cross 30 |
| `A03_rsi_cross_50` | RSI cross 50 |
| `A04_rsi_cross_70` | RSI cross 70 |
| `A05_body_red_green_2day` | 2-day body, red vs green |
| `A06_volume_red_green_2day` | 2-day volume, red vs green |
| `A07_rvol` | checklist rvol |
| `A08_bollinger_position` | Bollinger position |
| `A09_above_sma50` | close vs SMA50 |
| `A10_sma20_50_80_stack` | SMA 20/50/80 stack |
| `A11_three_section_lows` | three-section lows |
| `A12_green_body_vs_wick_2day` | green body vs wick |
| `A13_red_body_vs_wick_2day` | red body vs wick |
| `A14_profitable_oversold_setup` | oversold setup gated by `profitable` |
| `A15_tape_recovery_setup` | 2-day body R:G > 1.4, red wick > 1.15× green wick, max green body > max red body, last 5 prior sessions |

`A14_profitable_oversold_setup` reads `profitable` from part B. That flag is a Finviz snapshot field. Section 3 drops A14. It is listed so the drop is visible. There is no price-only rewrite of A14.

Each other A-rule is its own id. The flag is the checklist's −1, 0, or 1 on bars with `date < D`. A rule that is not exact is omitted from the sum. It is not replaced and it does not change the divisor in section 5.

Peer enrichment P01–P04 and the sector board inside the enriched AB file are LLM or peer-file overlays. They are not these ids. P04 reads the sector LLM board and is section 3.

### 4.4 Peer RS, VIX, rates

| id | what it is |
| --- | --- |
| `peer_rs` | Prior 5-session close-to-close return minus the median of that return on the name's peer set. The book's map is `s_peer = tanh(rs_week / 8)`. The peer set has to be part of the exact rebuild. A Finviz Performance column is not `peer_rs`. |
| `vix` | Prior session close of Yahoo `^VIX`. Not VIX3M, not VIX futures, not the news table in the channel-1 JSON. |
| `dgs10` | Latest FRED `DGS10` print dated before session D. |
| `dgs30` | Latest FRED `DGS30` print dated before session D. |
| `dfii10` | Latest FRED `DFII10` print dated before session D. |
| `sofr` | Latest FRED `SOFR` print dated before session D. |

`BAMLH0A0HYM2`, `IORB`, `RRPONTSYD`, and `USEPUINDXD` are in `fetch_channel1.FRED_SERIES` and are not rate ids here. They are not candidates. Fear & Greed is not a candidate.

## 5. Selection and sizing, frozen

Freeze commit, `main`: `3fe544103b94d9795f2b182211e9a8d90d99b95e` (`Add long-history addendum 1 before any score.`).

The run imports these callables from that tree. It does not edit them.

| role | callable |
| --- | --- |
| Weight table | `src/stock_book.py` `WEIGHTS`, `SIGNAL_FAMILIES` |
| Slot fill | `src/stock_book.py` `effective_weights` |
| Rank | `src/factor_mine.py` `rank_key` |
| Pick | `src/factor_mine.py` `pick_day` |
| Size | `src/factor_mine_book.py` `split_budgets` with mode `leftover` |

`WEIGHTS["1d"]` at that commit, order `(join, sector, general, news, ab, peer)`:

`(0.12, 0.10, 0.08, 0.25, 0.25, 0.20)`

### 5.1 Non-price inputs removed

Removed from the six-slot vector. These four are out on every session:

| slot | 1d weight | why it is non-price |
| --- | ---: | --- |
| `join` | 0.12 | Membership labels times the weather multiplier. The labels are Finviz. The multiplier follows the general LLM score. |
| `sector` | 0.10 | Sector LLM essays. |
| `general` | 0.08 | General-predict LLM score, times beta. |
| `news` | 0.25 | News actions, news judge, Finviz digest. |

`ab` (0.25) stays only when at least one of A01–A13 and A15 is exact. Otherwise `ab` is removed too. `peer` (0.20) stays only when `peer_rs` is exact. Otherwise `peer` is removed too.

Also removed. They are not slots in `WEIGHTS`, so they have no weight to move:

- `s_heat` (map heat and captain essays)
- `s_opp` and the size and range opportunity adds (Finviz cap and range bins)
- the rebound boost and the persist penalty
- the green pile, the decision lattice, and lookback alarm / fade / blue / white
- morning S, hard-red, and every `s_boost`
- `book_policy.json`
- beta load from a Finviz beta
- earnings-entry tickers and the calendar entry scale
- `forbid.alarm` (a camera flag). It is not replaced by a price veto
- universe buckets `flatten`, `probable`, `earn_react`, `overnight_scheduled`, `overnight_mega`, `mover_buy`, and `oppset`

### 5.2 One fill rule

**Dropped, weight redistributed pro rata.**

That is `effective_weights` and nothing else. Absent families are set to 0. Each surviving weight is multiplied by `sum(original 1d weights) / sum(surviving weights)`, then rounded to 4 decimal places the way that function rounds. No family is replaced by another input. No new slot is appended for VIX, rates, MACD, candle score, or a source list.

The arithmetic on the 1d vector, sum 1:

| surviving | ab | peer |
| --- | ---: | ---: |
| ab and peer | 0.5556 | 0.4444 |
| ab only | 1.0 | 0 |
| peer only | 0 | 1.0 |
| neither | no score | no score |

If neither survives, variant 1 buys nobody. It does not fall through to `hot_score`.

The ab score, when the family survives, is `tanh(raw / 8)`. `raw` is the sum of the flags of the A-rules that are exact, among A01–A13 and A15. The divisor stays 8, including when some of those rules were dropped. A14 is never in the sum. The peer score, when the family survives, is `tanh(rs_week / 8)` from the exact `peer_rs` rebuild. Both divisors are the ones in `stock_book.py` at the freeze.

`vix`, `dgs10`, `dgs30`, `dfii10`, `sofr`, `macd`, and `candle_score` have no slot in this vector and no term in `hot_score`. They are not inserted. An exact mark does not open a third variant.

### 5.3 Sizing

One mode: `split_budgets(names, cash, "leftover")`. Equal split of leftover cash across the new names. Whole shares. Budget that cannot buy 1 share is not handed to another name. Capital is $10,000 per variant, independent books, `day_cap` 1. The sleeve menu (`long_pct`, `2w_size`, rank-weighted, top-heavy, half, conviction) is not used.

Shared order body, both variants: `side=long`, `top_n=4`, `hold=1`, `size=leftover`, `sell=time`, `s_boost=none`, no stop, no take. `top_n=4` and `hold=1` are the body of `union_hot_n4_h1` in `build_recipes` at the freeze. The stock-book `--top 25` display cap is not used.

The weight numbers and the HOT4 body were already in the tree, and they were chosen on 2026 mornings that sit inside this window. That is a caveat. It is not added to N. This protocol tries 2 variants, so N is 2.

## 6. Excel color engine

Engine file: `excel_bot/engine/daily_run.py` at commit `76aef1278a53cda755071b7bd970a3f886fcf2cc`.

Cards: `excel_bot/strategies/*/card.json` at commit `abe8d79facd84c94b61f3158b245f2c50262f1bd`. Seven cards:

| card | `cohort_filter` at that commit |
| --- | --- |
| `L1_long_green_tp8_lowvol` | `volM:low(<3%)` |
| `L2_long_green_tp3_lowvol` | `volM:low(<3%)` |
| `L3_long_green_hold2_midcap` | `mid(1-10B):all` |
| `L4_long_green_hold8_bbailike` | `mid:BBAI-like(hi-beta,unprof)` |
| `L5_long_green_hold2_midhibeta` | `mid:beta>1.5` |
| `S1_short_red_1day_optionable` | `opt:Yes` |
| `S2_short_red_1day_hivol` | `volM:high(>8%)` |

A card is included only when `spec.cohort_filter` is the string `ALL`. That is the universal cohort `find_signals` builds with `{"ALL"} | cohorts_of(rec)`. A Finviz cohort whose label contains the letters "all", including `mid(1-10B):all`, is not `ALL`.

At `abe8d79f`, zero cards have `cohort_filter` `ALL`. The Excel set is empty. No card is rewritten to ALL. IRONCLAD rule 22: a recipe that never trades is `untestable` and stays out of the ranking. The empty Excel set is not a variant. It does not add to the Holm N or the luck-test N. The count in section 7 stays 2.

The second gate is stated so it cannot be skipped if a card at that commit had been ALL. It is not reached for these seven. Include the card only when `daily_run.py` at `76aef127` reproduces that card's proven signals from 2026-09-14 on. Proven signals are the rows of `excel_bot/suggestions/suggestions.csv` with that `strategy` and `signal_date` on or after 2026-09-14. Reproduce means the same set of `(signal_date, ticker, side)`. One mismatch drops the card. There is no substitute card. This commit does not run that check.

Training period, read from the cards. None of the seven stores a start date. Every card's `caveats` says `6.5-month single-regime window`. The dossier heading at that commit is 2026-07-28. `holdout_split.json` (`created` 2026-07-27) is a ticker split, so it does not move the dates. The training window locked for the year cut is 2026-01-13 through 2026-07-28. 2026-07-28 minus six calendar months is 2026-01-28, and half a month is 15 days, so the start is 2026-01-13. It is the same window for every card.

Out-of-sample years for a card are the calendar years strictly before that start. For these cards that is 2019, 2020, 2021, 2022, 2023, 2024, and 2025. The year 2026 is not out-of-sample for them. That label does not replace section 9, and it does not score an empty set.

## 7. Variants

**Count: 2.**

Both use section 5 sizing, section 8 fills, and the proxy panel. Neither adds a name from outside the proxy.

| name | rank | who can be bought |
| --- | --- | --- |
| `pricefull_w1d` | section 5.2 score, higher first, ticker A→Z | the proxy panel, then `pick_day` keeps 4 |
| `pricefull_hot4` | `rank_key` with `rank=hot_score` | proxy names that sit on at least one exact source in section 4.2, then `pick_day` keeps 4 |

`pricefull_w1d` reads only the surviving ab and peer slots. It does not read the section 4.2 lists.

`pricefull_hot4` reads `hot_score` and the exact source lists. If `hot_score` is not exact, this variant buys nobody. If none of the four source ids is exact, this variant buys nobody. The full proxy is not a backup list for this variant. A dropped source is not replaced by another list.

No third variant is declared. A rank by RSI, MACD, candle score, VIX, or rates is not declared. Excel is not a variant.

## 8. How a day is scored

Same loop as `research/longhist/PREREG.md` section 5, on this window, for these two names.

Walk XNYS sessions from 2019-01-01 through 2026-08-12. The holiday set is `exchange_calendars` calendar `XNYS`. The session list is written into the run manifest before the first score. A restart from a saved day must match (IRONCLAD rule 8).

Day D sees bars with `date < D`, the official open of D, and cash and lots from the close of D−1. It does not see D's high, low, close, or volume when it decides.

1. Sells first, at today's open. A lot bought on session index `i` is sold at the open of session `i + 1`.
2. Then buys, at today's open, from leftover cash, equal split.

A missing open does not invent a fill. The lot is carried to the next open. A missing open on a new pick skips that pick and does not pull in the next ranked name.

A closed trade's after-fee P&L is `shares * (exit - entry) - buy_fee - sell_fee`. Its return is that P&L divided by `shares * entry`. Open lots at 2026-08-12 are marked at that session's close for the equity line and are not closed trades. They do not enter the mean, the t-test, the year sums, or the best-stock sum.

Flat 15bp is printed next to Futubull (IRONCLAD rule 14). Pass rules use Futubull.

## 9. Pass rules

Identical to `research/longhist/PREREG.md` section 6, plus one required period. All five are required, after Futubull fees. Failing one fails the variant. Flat 15bp, RANDOM4, and IWM are reported and are not a sixth pass rule. IRONCLAD rule 21's keep bar (at least 30 fires in total and a win rate above 55% after fees) is also reported. A variant that clears the five and fails the keep bar is `study_pass` and `ironclad_keep_bar_fail`. It is still not a live strategy.

A fire is a buy fill.

### 9.1 Mean trade above 0, after Holm

The test unit is the entry day. For each entry date with at least one closed trade, `r_d` is the equal-weight mean of that day's closed-trade returns. One-sided Student t on the `{r_d}` series, H1: mean > 0, via `scipy.stats.ttest_1samp(series, 0.0, alternative="greater")`. If scipy is missing, the run fails. It does not swap in another test.

If there are fewer than 2 entry days, the p-value is 1. If the sample standard deviation is 0 and the mean is positive and there are at least 2 days, the p-value is 0. If the mean is not positive, rule 9.1 fails even if the p-value is small.

Holm across **both variants**, family-wise α = 0.05. N = 2. Sort p-values ascending. For rank i starting at 1, `raw_adj = min(1, (N - i + 1) * p_i)`, then enforce `adj_i = max(raw_adj, adj_{i-1})`. Pass when the mean of `r_d` is > 0 and `adj < 0.05`.

### 9.2 Positive in at least 4 of 2019–2024

A year is positive when the sum of Futubull closed-trade P&L with **exit date** in that calendar year is > 0. A year with no closed trade is not positive.

The six years are 2019, 2020, 2021, 2022, 2023, 2024. The variant passes when at least 4 of these 6 are positive. This rule does not look at 2025 or 2026. Those years cannot flip a 9.2 decision.

### 9.3 Positive with the single best stock removed

The best stock is the ticker with the largest sum of Futubull closed-trade P&L. A tie goes to the ticker that sorts first A→Z. Rerun the same book on the same sessions with that ticker ineligible. Pass when the rerun's sum of Futubull closed-trade P&L is > 0. The rerun does not alter the original ledger.

### 9.4 At least 30 fires per year

`rate = n_buy_fills / (n_sessions_in_the_slice / 252)`. The 252 is fixed. Pass when `rate ≥ 30`. Also print the raw buy count in each calendar year. The pass is the rate, not a requirement that every year has 30.

### 9.5 Positive from 2025-01-01 through 2026-08-12

The sum of Futubull closed-trade P&L with an exit date from 2025-01-01 through 2026-08-12 is > 0. One period, not two. A variant that passes 9.2 and fails this period fails the study.

## 10. Luck test and baselines

The luck test is the long-history section 9 procedure, once, on these two variants. It is a report. It does not replace section 9.

N = 2. The statistic is the mean 1-share Futubull return. Generator: `numpy.random.Generator(numpy.random.PCG64(20260927))`. For k = 1..10000, in calendar order, permute each session's return vector with `Generator.permutation`. Nothing else draws from the generator.

- `p_rule = (1 + count(null_mean >= real_1share_mean)) / 10001`
- Best-of-N: `p_best = (1 + count(best_null >= real_best_1share_mean)) / 10001` across the two variants that have at least one pooled pick.

Baselines, same sessions, Futubull, $10,000, reported only (IRONCLAD rule 19):

- RANDOM4. Seed 20260813, 1000 draws, 4 names, hold 1, from that morning's proxy panel.
- IWM buy-and-hold, one round trip, first open in the window to the close of 2026-08-12, plus the flat 15bp print of that same trip.

## 11. What a later commit may contain

Allowed after this fingerprint, and not as a result before the inputs it uses are marked:

- reading the audit file and recording which section 4 ids were exact
- the two ledgers and the one luck test
- the RANDOM4 and IWM prints

Not allowed: a third variant, a new slot, a substitute for a dropped id, a new threshold, a new year definition, a new Holm N, a rewrite of a stored bar, scoring Excel by changing a cohort to ALL, or a write into a live path.
