# Long-history price-only study — preregistration

- status: locked before any score. This commit has no returns, no p-values, and no luck-test output.
- written: 2026-09-26
- fingerprint_sha256: 0e519c4f8e3081571b79f23ef15f9946716ebeb46c354ea7e8c53470ec78394a
- fingerprint_scope: SHA-256 of the UTF-8 bytes after the line `<!-- BEGIN COVERED -->`, including the final newline. Line endings are LF.
- window: 2019-01-01 through 2026-08-12, NYSE (XNYS) sessions, scored one session at a time.
- live paths: not used. No edits to flatten_robust, Webull submit, the live factor-mine core, or Supabase. Output lives only under `research/longhist/`.

<!-- BEGIN COVERED -->

## 0. What this file is

This is the protocol for a research study. It is not a live strategy and it does not open a live record.

Every session in the window is before this file's date, so every session is `designed_after` (hindsight) under IRONCLAD rule 5. Nothing here counts toward a real record. No real money follows from it (IRONCLAD rule 23).

Changing a gate, weight, hold, exit, size, fee, or fill after this fingerprint is a new study, not a rerun.

Two parts:

- Part 1 scores four price-only copies of the OOS-0914 frozen rules on the whole window.
- Part 2 searches a declared grid only on 2019-01-01..2023-12-31, freezes the winners with their own fingerprint, then scores them once on 2024-01-01..2026-08-12.

Bars, once written, are append-only. This commit does not write bars.

## 1. Price source

Yahoo daily bars only (IRONCLAD rule 11). No premarket, no Finviz, no Stooq, no news, no cameras.

Download: Yahoo chart v8, `interval=1d`. Signals and fills use `indicators.quote` open, high, low, close, and volume. Those fields are the split-adjusted series. `adjclose` is stored in the cache and is not read by any gate, rank, fill, or fee. If `quote` is missing for a name-day, that name-day is dropped. It is not replaced with `adjclose`.

A symbol is kept only when the chart meta has `quoteType` EQUITY and `currency` USD.

Cache (later commits, not this one):

- `research/longhist/bars/ohlcv.parquet`
- `research/longhist/bars/manifest.jsonl`, one JSON object per line: `ticker`, `date`, `sha256`

The bar hash is SHA-256 of the canonical JSON (`sort_keys`, separators `,` and `:`) of `{date, ticker, open, high, low, close, volume}` with prices formatted to 6 decimal places, half away from zero, and volume as an integer. A second fetch that disagrees with a stored hash fails the run. Rows are never rewritten. A new ticker-day is appended.

Train code for Part 2 may open only rows with `date <= 2023-12-31`. A train read of a bar dated 2024-01-01 or later fails the run. The test slice is a separate open, after the winner file is fingerprinted.

## 2. Ticker list

Two legs. The panel on a session can use a symbol only if that symbol is on the frozen list and Yahoo has a bar that session.

### 2.1 Current listings (counted)

Source files, both stamped `File Creation Time: 0925202621:31` by Nasdaq Trader:

| file | sha256 |
| --- | --- |
| `nasdaqlisted.txt` | `82a4fdb8cf5b0e2c001e827fa90eba03be826a9005b0ce5b8d2c78d023cb940e` |
| `otherlisted.txt` | `2fffd43ed31ef453ec0f39a2b3c06e3cd83f734a200a81d95df9f741a33f89aa` |

Keep a row when `Test Issue` is `N`, `ETF` is `N`, Nasdaq `NextShares` is `N` when that column exists, and the security name contains `common stock` or `common shares` (case-insensitive). Drop the row when the name contains any of: `warrant`, `right`, `unit`, `preferred`, `debenture`, `note`, `fund`, `etf`, `etn`, `acquisition`, `depositary`, `depository`, ` ads`, `ads,`, `ordinary`.

That filter produced **4,193** unique symbols. They are `research/longhist/listed_common.txt` (one symbol per line, sorted, LF, trailing newline).

`listed_common.txt` sha256: `62609d9165bcbd5e7327516ea4da03f05297698d67aab660f207ddf8426195d5`

A dot in a symbol is sent to Yahoo as a hyphen (`BRK.B` → `BRK-B`). The frozen spelling stays the Nasdaq spelling.

### 2.2 Delisted tickers Yahoo still serves (count not claimed)

There is no official roster of every delisted ticker Yahoo still charts. This commit does not invent that count.

The delisted leg is measured before any score and written to `research/longhist/tickers.json`, which is then fingerprinted. Scoring is refused until that file exists and records `n_listed` (must be 4193), `n_delisted_candidates`, `n_delisted_served`, and `n_delisted_absent`.

Candidate symbols are the union of:

1. The 4,193 listed symbols (the listed leg, not the delisted leg).
2. Every other symbol that appears as a `Ticker` in a committed `data/exports/finviz_*.csv` on this branch's base at the time the manifest is built.

A candidate is `served` when the chart request in section 1 returns at least one daily bar with a session date on or before 2026-08-12, `quoteType` EQUITY, and `currency` USD. Otherwise it is `absent`. Only `served` symbols enter the panel. The manifest stores the symbol, the leg (`listed` or `delisted`), and `served` or `absent`. It does not store a return.

This is not CRSP and it is not "every US stock that ever listed." The Finviz exports in this repo cover late August and September 2026 only, so the delisted leg can add only names that were still on that tape and then disappeared from the Nasdaq file, plus any listed name Yahoo still charts. Names Yahoo has already dropped, and names that died before this repo's exports, are missing. See the bias list below. The thresholds in section 3 do not depend on the delisted count and are not retuned after the count is known.

### 2.3 Known survivorship and membership bias

- Yahoo deletes many dead symbols. Bankruptcies that were purged are missing (that pushes a long-only study up). Cash buyouts that were purged are also missing (that pushes it down). The sign of the net bias is not known. Its existence is.
- The current Nasdaq file plus a short Finviz window is not the point-in-time exchange membership. A name is eligible on a date only when Yahoo has a bar that date.
- Split-adjusted prices are used. A later split changes the historical price level the $1–$30 band sees.
- The same letters can be a different company after a symbol is reused. Yahoo's series is taken as printed. No hand merge and no hand split.
- ADRs, ordinary shares, units, rights, warrants, preferreds, notes, funds, and SPACs (`acquisition` in the name) are out of the listed filter. A delisted Yahoo series that is EQUITY and USD can still enter on the delisted leg. That leg is small and late, as section 2.2 says.
- There is no market-cap history on the daily bar. Section 3 is a price and dollar-volume proxy, not the Russell 2000.

## 3. Proxy universe (the morning panel)

The live factor-mine morning panel is the union of several 09:30 lists and runs about 60–100 names a day (`docs/FACTOR_MINE_AUX_PANEL.md`). This study cannot rebuild that panel: the long window has no cameras, no news box, and no premarket print. The proxy below is price-only and uses only what is known at 09:30 ET.

On session D (an XNYS session, including half days), a listed-or-served symbol is in the raw pool when all of the following hold. Bars are that symbol's own sessions with `date < D`. Missing sessions are absent. They are not forward-filled.

1. At least 20 bars with `date < D`.
2. Prior close `C` (close of the last of those bars) is finite and **$1.00 ≤ C ≤ $30.00**.
3. Mean dollar volume of the last 20 of those bars, `mean(close * volume)`, is finite and **$1,000,000 ≤ mean ≤ $40,000,000**.
4. Today's official open `O` is finite and `O > 0`. The opening gap is `G = O / C - 1`. Require **|G| ≥ 0.04**. Compare the raw ratio. Do not round before the compare. `O` is the daily open. Yahoo daily has no premarket, so a move that reverted before 09:30 is not a gap here.
5. Prior-day relative volume uses `ohlc_ripper.from_bars` on those prior bars, unchanged: `rvol = volume[-1] / mean(volume[-20:])` when there are at least 8 bars. Require **`rvol ≥ 1.5`**. The function's `ok` flag must be true.

Today's high, low, close, and volume are not inputs to membership, gates, or rank.

If more than 60 names pass, keep **60**. Order is larger `|G|`, then larger `rvol`, then ticker A→Z. If 60 or fewer pass, keep all of them. Do not lower a threshold to fill the list. The realized daily count is a result. It is reported later and it does not change this file.

The cap is the device that keeps the panel near the live morning list. The floors are the pre-registered meaning of "small-price / small dollar-volume," "big opening gap," and "unusual prior-day volume." They were not fit to a histogram of 2019–2026.

## 4. Fees

Primary figure: Futubull, file `00_grounding/futubull_fees.json`, function `paper_trade.order_fees`. Comment keys (names starting with `_`) are not part of the model.

Fee-model sha256 (canonical JSON of the priced object, `sort_keys`, separators `,` and `:`): `3fc9b5829758722bd968e84baf0923f5efd8006d3b780a18e4d417552a9e80a7`

One order, `shares > 0` and `price > 0`, amount = shares * price:

- commission = min(max(0.0049 * shares, 0.99), 0.005 * amount)
- platform = min(max(0.005 * shares, 1.00), 0.005 * amount)
- settlement = 0.003 * shares
- sells also add regulatory = max(0.000008 * amount, 0.01)
- sells also add TAF = min(max(0.000166 * shares, 0.01), 8.30)

The order fee is the sum, rounded to 4 decimal places. A non-positive share count or price costs 0.

Alongside, not instead: a flat **15bp round trip** (0.0015 * entry notional, once per closed trade, no per-share schedule). Pass rules use Futubull. The 15bp column is printed next to it (IRONCLAD rule 14).

Capital is **$10,000** per rule, from `paper_account.starting_capital_per_sleeve`. Whole shares only. Independent books. No shared cash across rules.

## 5. How a day is scored

Walk XNYS sessions from 2019-01-01 through 2026-08-12 in order. The holiday set is `exchange_calendars` calendar `XNYS`. The session list is written into the run manifest before the first score. A restart from a saved day must match (IRONCLAD rule 8).

Day D sees:

- bars with `date < D` for features, prior close, rvol, and dollar volume
- the official open of D for the gap test and for fills
- cash and lots from the close of D−1

It does not see D's high, low, close, or volume when it decides.

Order of operations, matching `factor_mine_book.simulate_book` with these switches off: no morning-S boost (`s_boost` is `none`), no hard-red sit, no holdup.

1. Sells first, at today's open.
2. Then buys, at today's open, from leftover cash.

`sell = time`. A lot bought on session index `i` is sold at the open of session `i + hold` (`held >= min_hold`). No take-profit and no stop, so the same-bar "stop first" rule (IRONCLAD rule 13) does not fire. It remains the rule if a later study adds both levels.

A missing open does not invent a fill. The lot is carried. The next session that has an open sells it then. A missing open on a new pick skips that pick and does **not** pull in the next ranked name.

Size is `leftover`: equal split of `cash * day_cap` (`day_cap` is 1) across the new names (not names already held). Shares = `floor(budget / open)`. If that is below 1, the name is skipped and its budget is not handed to another name. If the buy fee makes the cost exceed cash, shares are reduced with the same fee function; if that falls below 1 share, skip. A name sold earlier the same morning is not held, so it may be bought again if it is on today's list.

Rank follows `factor_mine.rank_key`. Higher score first. Tie: ticker A→Z. `top_n` is the length of the look list, not a quota of fills.

A closed trade's after-fee P&L is `shares * (exit - entry) - buy_fee - sell_fee`. Its return is that P&L divided by `shares * entry` (fees are not in the denominator). Open lots at 2026-08-12 are marked at that session's close for the equity line and are **not** closed trades. They do not enter the mean, the t-test, the year sums, or the best-stock sum.

## 6. Pass rules

All four are required, after Futubull fees. Failing one fails the rule. 15bp, RANDOM4, and IWM are reported and are not a fifth pass rule. IRONCLAD rule 21's keep bar (at least 30 fires in total and a win rate above 55% after fees) is also reported. A rule that clears the four and fails the keep bar is `study_pass` and `ironclad_keep_bar_fail`. It is still not a live strategy.

A fire is a buy fill.

### 6.1 Mean trade above 0, after a multiple-testing correction

The test unit is the entry day. For each entry date with at least one closed trade, `r_d` is the equal-weight mean of that day's closed-trade returns. One-sided Student t on the `{r_d}` series, H1: mean > 0, via `scipy.stats.ttest_1samp(series, 0.0, alternative="greater")`. If scipy is missing, the run fails. It does not swap in another test.

If there are fewer than 2 entry days, the p-value is 1. If the sample standard deviation is 0 and the mean is positive and there are at least 2 days, the p-value is 0. If the mean is not positive, rule 6.1 fails even if the p-value is small.

Correction: Holm across **every rule tried in that part**, family-wise α = 0.05. Sort p-values ascending. For rank i starting at 1, `raw_adj = min(1, (N - i + 1) * p_i)`, then enforce `adj_i = max(raw_adj, adj_{i-1})`. Pass when the mean of `r_d` is > 0 and `adj < 0.05`.

- Part 1: N = 4. These four were chosen in the OOS-0914 mine, whose luck-test N was 37, using sessions that start 2026-08-13. That is after this window ends (2026-08-12). This protocol's Holm N is 4 because this protocol tries 4 rules. The earlier selection is a caveat. It is not folded into N.
- Part 2: N = 40, the full grid in section 8, on the slice being judged. Train Holm uses train p-values. Test Holm uses test p-values of all 40 rules, including rules that were not frozen as winners. Every one of the 40 is scored once on the test window so the correction can be checked. The test is not a second search and it does not change a rule.

### 6.2 Positive in at least 4 of 6 years

A year is positive when the sum of Futubull closed-trade P&L with **exit date** in that calendar year is > 0. A year with no closed trade is not positive.

Part 1's six years are **2019, 2020, 2021, 2022, 2023, 2024**. The rule passes when at least 4 of these 6 are positive. 2025 and 2026-01-01..2026-08-12 are printed in the same table and cannot change the pass. They are outside the six because the sentence says six years, and 2019–2024 are the first six calendar years of the window. That choice is locked now so it cannot be swapped for a friendlier six after the results.

Part 2's test window is 2024-01-01..2026-08-12 (three year buckets). Four of six cannot be met on that slice. Part 2 therefore does not pass rule 6.2 on the test slice. The train filter uses the analogue in section 8: at least 4 of the 5 train years 2019–2023. That analogue is not called an out-of-sample pass.

### 6.3 Positive with the single best stock removed

The best stock is the ticker with the largest sum of Futubull closed-trade P&L. A tie goes to the ticker that sorts first A→Z. Rerun the same book on the same sessions with that ticker ineligible (it cannot be bought; lots already open cannot occur because it was never bought). Pass when the rerun's sum of Futubull closed-trade P&L is > 0. The rerun does not alter the original ledger.

### 6.4 At least 30 fires per year

`rate = n_buy_fills / (n_sessions_in_the_slice / 252)`. The 252 is fixed. Pass when `rate ≥ 30`. Also print the raw buy count in each calendar year. The pass is the rate, not a requirement that every year has 30.

## 7. Part 1 — frozen rules, whole window

Source: `data/factor_mine/oos0914/frozen_rules.json`. The four source fingerprints are copied and are not recomputed as the longhist names:

| source name | sha256 |
| --- | --- |
| `oos0914_break10_h2_sx` | `d4a1d4d853a7f296b1b30778bd55c571bd28dc97eb513d4a908721dd909b3237` |
| `oos0914_rvol_lg_h1_sx` | `cf8a5668b664d6737acabe292d0c2a2572533acda6b9d63bff916380046c4b67` |
| `oos0914_break10_h1_sx` | `a2a8b4eb1f90a3761c6a86f734cb3d66c8e1a2a7f2fde6bbf25a4c889379a859` |
| `oos0914_zero_candle_h2_sx` | `dc551f2b899846179d8040877c84663dc0c2735798760f7ad051985eeb1d95c5` |

The universe is no longer `union`, and two inputs are not on this tape. IRONCLAD rule 4 therefore gives these copies new names. The source file is not edited.

Shared body: `side=long`, `top_n=4`, `size=leftover`, `sell=time`, `s_boost=none`, `day_cap=1`, `take_pct` null, `stop_pct` null, `exit_when` empty, universe `proxy60`, capital 10000, created_on 2026-09-26.

`forbid.alarm` is on the source cards. Alarm is a camera flag. This tape has no cameras. Alarm is **false for every row**, so the forbid removes nobody. That is recorded on every trade. It is not a new price gate.

`zero_red` on the source card is `n_red == 0 and n_good >= 1` on morning cameras. This tape has no cameras. The price stand-in, used only by the zero-candle copy, is `zero_red_px`: on the last 8 bars with `date < D` (candle lookback), count a bar green when `close > open` and red when `close < open`; flats do not count; true when greens ≥ 1 and reds = 0.

Other features, from bars with `date < D`, functions unchanged:

- `break_10`, `rvol`, `last_green`, `ret_5`, `hot_score`: `ohlc_ripper.from_bars` on the last 60 prior bars (the indicator lookback). `last_green` is `close > open` on the last of those bars. `hot_score` is `0.08 * max(ret_5, 0) + 0.04 * max(ret_10, 0) + 0.4 * min(rvol, 3) + 1.2 if break_10 else 0 + 0.3 if last_green else 0`, and 0 when `ok` is false.
- `candle_score`: `candle_factor` score on the last 8 prior bars.

The universe already requires `rvol ≥ 1.5`, so the rvol copy's extra cut inside the panel is `last_green`. The 1.5 floor stays on the rule card anyway.

| longhist name | hold | require | rank |
| --- | --- | --- | --- |
| `longhist_break10_h2` | 2 | `break_10` | `hot_score` |
| `longhist_rvol_lg_h1` | 1 | `last_green` and `rvol ≥ 1.5` | `hot_score` |
| `longhist_break10_h1` | 1 | `break_10` | `hot_score` |
| `longhist_zero_candle_h2` | 2 | `zero_red_px` and `last_green` | `candle_score` |

Scored sequentially on 2019-01-01..2026-08-12. No other rule is added to Part 1 after this file.

## 8. Part 2 — fresh grid, train then one test

Searched only on XNYS sessions from 2019-01-01 through 2023-12-31. The test window 2024-01-01..2026-08-12 is not loaded in the search.

Every combination of the levers below is tried. Nothing else is a lever. Fixed, same as Part 1: long, leftover, time exit, no stop, no take, `day_cap` 1, `s_boost` none, alarm inert, universe `proxy60`, capital 10000, whole shares, Futubull.

| lever | values | count |
| --- | --- | --- |
| gate | `break10`, `rvol_lg`, `zero_px`, `ret5_up`, `pullback` | 5 |
| hold | 1, 2 | 2 |
| rank | `hot_score`, `candle_score` | 2 |
| top_n | 2, 4 | 2 |

**Total combinations: 5 × 2 × 2 × 2 = 40.** That is the Holm N and the luck-test N. It is under the 50-candidate cap (IRONCLAD rule 18).

Gates (all on bars with `date < D`, except that the universe gap already used today's open):

- `break10`: `break_10` is true
- `rvol_lg`: `last_green` and `rvol ≥ 1.5`
- `zero_px`: `zero_red_px` and `last_green`
- `ret5_up`: `ret_5 ≥ 0` (`ret_5` is the percent from `ohlc_ripper`)
- `pullback`: `last_green` and `ret_5 ≤ -3`

Name: `longhist2_{gate}_h{hold}_n{top_n}_{rank}`. Example: `longhist2_pullback_h2_n2_candle_score`.

A train candidate is a **winner** only when, on the train slice, all of these hold:

1. Section 6.1, Holm N = 40, using train p-values.
2. At least 4 of the train years 2019, 2020, 2021, 2022, 2023 are positive (section 6.2's definition).
3. Section 6.3 on the train rerun.
4. Section 6.4 on the train sessions.

If more than 5 candidates win, freeze the 5 with the largest train sum of Futubull closed-trade P&L. Tie: larger train t-statistic on the `{r_d}` series, then name A→Z. If none win, freeze none and say so. Do not freeze the best failure.

Winners are written to `research/longhist/part2_frozen.json` with a SHA-256 of the canonical JSON of the frozen bodies (same canonical form as the fee hash). That file is committed **before** any test session is scored. The test run checks the hash and fails on a mismatch.

The test run then scores all 40 rules once, so section 6.1 can Holm-adjust the test p-values across every rule tried. Frozen winners are the only names that can be called winners. The other 34 are published so a loser cannot be hidden. No rule is refit on the test window. No lever is added after this file.

## 9. Excel luck test (10,000 reshuffles, each part)

Excel runs this once per part, after that part's real books exist. It is a report. It is not a substitute for section 6.1.

The statistic is the mean **1-share** Futubull return, not the leftover-book return. One share is bought at the entry open and sold at the time-exit open. Fees use `order_fees` on each leg. Return = `(shares * (exit - entry) - buy_fee - sell_fee) / (shares * entry)` with shares = 1. A missing exit drops that name from the pool.

For each session and each hold in {1, 2}, build the vector of finite 1-share returns on that day's proxy panel (section 3), in ticker A→Z order.

Picks are computed once from features and then held fixed. A reshuffle does not change who was picked. It reassigns returns.

Generator: `numpy.random.Generator(numpy.random.PCG64(20260926))`. Part 1 and Part 2 each start from that seed. They do not share a stream. For k = 1..10000, in calendar order, permute each session's return vector with `Generator.permutation`. One part's draws are exactly 10000 times its session count, and nothing else draws from the generator.

For each rule, the null mean on reshuffle k is the mean of the permuted 1-share returns of its picked (ticker, entry date) pairs that sit in the pool. The real comparison mean is the same 1-share mean on the unpermuted vectors.

- `p_rule = (1 + count(null_mean >= real_1share_mean)) / 10001`
- Best-of-N: each reshuffle records the maximum null mean across rules that have at least one pooled pick. `p_best = (1 + count(best_null >= real_best_1share_mean)) / 10001`, where `real_best_1share_mean` is the best real 1-share mean among those rules.

Part 1 uses N = 4. Part 2 reports the test twice: once on the train slice with N = 40, and once on the test slice with N = 40. The book-level pass rules stay on the leftover-book P&L. The luck test does not replace them. Both numbers are printed and they are not interchangeable.

## 10. Baselines (reported on every slice)

IRONCLAD rule 19. Same sessions as the slice, Futubull fees, $10,000.

- **RANDOM4.** Seed 20260813, 1000 draws, 4 names, hold 1, drawn without replacement from that morning's proxy panel. If the panel has fewer than 4 names, draw all of them. Same leftover book as section 5. Report the mean ending-equity return across the 1000 draws, and the fraction of draws the rule's ending equity beats.
- **IWM.** Buy-and-hold. Buy whole shares at the first session open in the slice where IWM has an open, sell at the close of the last session in the slice, one round trip, Futubull on that buy and that sell. IWM does not have to pass section 3. Also print the flat 15bp version of the same one round trip.

## 11. What a later commit may contain

Allowed after this fingerprint, and not before any of it is used as a result:

- the ticker manifest in section 2.2, then the append-only bar cache
- Part 1 ledgers and the Part 1 luck test
- `part2_frozen.json` after the train search and before the test score
- the one test score and the Part 2 luck test

Not allowed: a new gate, a new threshold, a new year definition, a new Holm N, a rewrite of a stored bar, or a write into a live path.
