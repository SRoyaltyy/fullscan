# Fullscan lever search — preregistration

- status: body written. Section 5 is the one open line. The fingerprint is filled when that line closes, at or after 17:30 HKT on 2026-09-26 and before 17:45 HKT. This commit has no returns, no p-values, and no luck-test output.
- written: 2026-09-26
- fingerprint_sha256: PENDING
- fingerprint_scope: SHA-256 of the UTF-8 bytes after the line `<!-- BEGIN COVERED -->`, including the final newline. Line endings are LF.
- scoring: do not start until this preregistration is merged and Cyrus replies that it is merged. Results of the scored run are due by 22:00 HKT on Sunday 2026-09-27.
- live paths: not used. No edits to flatten_robust, Webull submit, Supabase, or Theme Radar. Output of the later run lives only under `research/lever_search/`.

<!-- BEGIN COVERED -->

## 0. What this file is

This is the protocol for one research search. It is not a live strategy and it does not open a live record.

The rules are [IRONCLAD_RULES.md](../../IRONCLAD_RULES.md), sha256 `9dade507949485d4c0e65a2595a25a07c18018f509bafcce421300408d4e6f95`. Inputs are only those that [research/INPUT_HISTORY.md](../INPUT_HISTORY.md) marks as knowable at 09:30 ET, sha256 `cc1e13d59e613527c1682da66b78d055444ba753b044f384c4d539307d6315ec`, both taken from fullscan `ad3e064f66eb4e0c62e768a3f7f675d06db8a8f7`.

Changing a gate, weight, hold, exit, size, fee, fill, universe, or the combination count after this fingerprint is a new study, not a rerun.

No score is computed in this commit.

## 1. Sessions the search may read

Source: `data/factor_mine/panel.json`, sha256 `f9a4efc13a8aa13b92ac5847f0ef4f3dc4bddf0b16652684b183a9fe3170a506`.

The search and the walk-forward read these 21 sessions and no others:

`2026-08-13`, `2026-08-14`, `2026-08-17`, `2026-08-18`, `2026-08-19`, `2026-08-20`, `2026-08-21`, `2026-08-24`, `2026-08-25`, `2026-08-26`, `2026-08-27`, `2026-08-28`, `2026-08-31`, `2026-09-01`, `2026-09-02`, `2026-09-03`, `2026-09-04`, `2026-09-08`, `2026-09-09`, `2026-09-10`, `2026-09-11`.

`panel.json` also holds 2026-09-14 through 2026-09-25. The loader keeps a row only when `date` is in the list above. A kept row dated 2026-09-14 or later fails the run. The same cutoff applies to every other file: a bar, snapshot row, Excel signal, or Theme Radar row dated 2026-09-14 or later is not an input to the search or the walk-forward.

The one designed-after look in section 8 is a separate open. It may read 2026-09-14 through 2026-09-25 only after the walk-forward choices are written and fingerprinted. It does not read 2026-09-28. That session is the first day of any later clean record.

## 2. Clock and prices

Day N uses inputs knowable at 09:30 ET on day N, plus day N−1 cash, holdings, and fees. The book is built one session at a time. A restart from a saved day must match byte for byte.

Fills follow IRONCLAD rules 12 and 13. Buy at the 09:30 open. A stop fills at the stop, or at the open when the open gaps through it. When one bar touches both a stop and a target, the stop fills first. A finished hold sells at the next session's open. The close is the mark.

Prices are Yahoo daily `indicators.quote` open, high, low, close, and volume (split-adjusted). `adjclose` is not read. Where `data/prices` already has that name-day, the search uses the stored print and does not download a second adjustment over it. A name with no bar that session is dropped from that session's book. The dropped list for the session is frozen. The day still locks.

Fees are `00_grounding/futubull_fees.json`, sha256 `019ebdba0fc0b20e02c91f116dc5591b81e96630f60c110dfbfd3b5d8e16c0d3`. Capital is $10,000. Shares are whole shares. Leftover cash is what the entry rule splits. The flat 15 bp series is the same fills priced at 7.5 bp per side, reported beside the Futubull series. Shorts pay the book's 1% annual borrow on notional, `sessions / 252`.

An LLM score is used only from the file already committed for that morning. A missing file does not get a new model call. The atom matches nobody that morning, the combination still counts as one try, and the day sits when every name fails the gate.

## 3. Theme Radar export

Frozen export on `SRoyaltyy/theme-radar` commit `3973e13cd953e5705d08d8d9f78a5b1b9dd1a1d0` (2026-09-26T03:35:00Z), message `research: frozen 09:30-knowable Finviz lever panel for fullscan lever search`.

| file | sha256 |
| --- | --- |
| `research/lever_panel/finviz_panel_asof0930_2026-08.csv.gz` | `c8977b8eea8e74115899e9d4cc04d5b4ea67490376d972905781eb8e1aeb6459` |
| `research/lever_panel/finviz_panel_asof0930_2026-09.csv.gz` | `cbf35da9e1587703059abd9ff77525a1047c67a91edc3276ca93db4cd8669c16` |

`trade_date` is the morning the row may be used. `snapshot_date` is the previous trading day. Every value is from a snapshot taken after that prior close and committed before 09:30 ET on `trade_date`. The builder's clock check passed on that commit. This search still drops a cell when that date's provenance says the family was not committed before 09:30 ET. Empty cells stay empty.

The September file contains trade dates through 2026-09-28. During the search and the walk-forward the loader keeps `trade_date` in the 21 sessions of section 1. 2026-08-28 has no Theme Radar row (no prior-close snapshot). A `theme_radar` universe sits that day.

Columns that are not levers, and are not read as gates, ranks, or deltas:

- clock and identity: `trade_date`, `snapshot_date`, `scrape_ts_utc`, `scrape_ts_source`, `snapshot_commit_ts_utc`, `tr_scores_commit_ts_utc`, `tr_features_commit_ts_utc`, `tr_composite_commit_ts_utc`, `tr_segments_commit_ts_utc`, `Ticker`, `Company`, `Industry`, `Sector`, `Country`, `Exchange`, `Index`, `News Time`
- text buckets: every `tr*_mcap_bucket`, `tr*_beta_bucket`, `tr*_status_*`, `tr*_kill_flags`, `tr*_top_pos`, `tr*_top_neg`, `trf_pair_date`
- outcome-style returns: `tr1d_ret_H`, `tr1w_ret_H`, `tr1m_ret_H`, `trf_true_ret`, `trf_true_ret_dir`
- `Open` (empty on the search window; not a fill)
- `trf_dir_*` (the sign of the matching `trf_d_*`; the delta lever uses `trf_d_*` once)

## 4. One combination

A combination picks exactly one signal and exactly one value from each other lever. A signal is one factor, or one unordered pair, or one day-over-day delta. It is not all three at once. Pairing every factor with every pair with every delta is a different study.

The combination id, used for tie-breaks and the parquet key, is:

`{signal}|{side}|{entry}|{exit}|h{hold}m{min_hold}|stop{stop}|n{top_n}|u{universe}|s{s_gate}|hr{hard_red}`

`signal` is `f:{atom}`, `p:{a}+{b}` with `a` < `b` in byte order, or `d:{delta}`.

Rank is not a free lever. It is fixed by the signal:

- any panel price atom, or `hot_top`, or a delta of a price field: rank by `ohlc_hot_score` descending
- an Excel card alone: the card's own row order
- a Theme Radar score atom: that score descending
- a pair: the first of those rules that matches either member
- tie-break: ticker ascending

Top-N is applied after the gate. Names that fail the gate are not ranked.

### 4.1 Factors (91 atoms)

Panel atoms use `data/factor_mine/panel.json` for that session. A camera whose source file is missing that morning matches nobody (INPUT_HISTORY). Theme Radar atoms use the frozen export joined on `(trade_date, Ticker)`. Excel atoms use `excel_bot/suggestions/suggestions.csv`. A card with `signal_date` D is knowable after D's close, so it is an input on the next panel session, not on D. `current_price`, `ret_vs_close`, and `ret_vs_open` are ignored. The seven cards are the folders under `excel_bot/strategies/`. The committed CSV has rows for L1, L2, L3, and L5 only. L4, S1, and S2 have no row through 2026-09-25, so those atoms match nobody on this window and still count.

| id | rule |
| --- | --- |
| `ab_good` | `boxes.ab` is `good` |
| `last_green` | `last_green` is true |
| `break_10` | `ohlc_break_10` is true |
| `rsi_os` | `rsi_os` is true |
| `rsi_ob` | `rsi_ob` is true |
| `macd_cross_up` | `macd_cross_up` is true |
| `macd_up` | `macd_up` is true |
| `flow_in` | `flow_in` is true |
| `ret5_pos` | `ohlc_ret_5` > 0 |
| `rvol_ge_1_5` | `ohlc_rvol` ≥ 1.5 |
| `nr7` | `ohlc_nr7` is true |
| `hot_top` | no boolean gate; rank by `ohlc_hot_score` |
| `xl_L1` | Excel `L1_long_green_tp8_lowvol` |
| `xl_L2` | Excel `L2_long_green_tp3_lowvol` |
| `xl_L3` | Excel `L3_long_green_hold2_midcap` |
| `xl_L4` | Excel `L4_long_green_hold8_bbailike` |
| `xl_L5` | Excel `L5_long_green_hold2_midhibeta` |
| `xl_S1` | Excel `S1_short_red_1day_optionable` |
| `xl_S2` | Excel `S2_short_red_1day_hivol` |
| `fv_perf_w_pos` | Finviz `Performance (Week)` > 0 |
| `fv_perf_m_pos` | Finviz `Performance (Month)` > 0 |
| `fv_perf_q_pos` | Finviz `Performance (Quarter)` > 0 |
| `fv_rvol_ge_1_5` | Finviz `Relative Volume` ≥ 1.5 |
| `fv_rsi_lt_30` | Finviz `Relative Strength Index (14)` < 30 |
| `fv_rsi_gt_70` | Finviz `Relative Strength Index (14)` > 70 |
| `fv_eps_surp_pos` | Finviz `EPS Surprise` > 0 |
| `fv_rev_surp_pos` | Finviz `Revenue Surprise` > 0 |
| `fv_sma20_pos` | Finviz `20-Day Simple Moving Average` > 0 |
| `fv_sma50_pos` | Finviz `50-Day Simple Moving Average` > 0 |
| `fv_short_float_ge_10` | Finviz `Short Float` ≥ 10 |
| `fv_insider_trans_pos` | Finviz `Insider Transactions` > 0 |
| `fv_inst_trans_pos` | Finviz `Institutional Transactions` > 0 |
| `fv_recom_le_2` | Finviz `Analyst Recom` ≤ 2 |
| `fv_gross_margin_pos` | Finviz `Gross Margin` > 0 |
| `fv_profit_margin_pos` | Finviz `Profit Margin` > 0 |

Theme Radar rubric atoms. For each horizon `h` in `1d`, `1w`, `1m`, the id prefix is `tr{h}_` and the column prefix is `tr{h}_`:

| suffix | rule |
| --- | --- |
| `price_pos` | `price_score` > 0 |
| `flow_pos` | `flow_score` > 0 |
| `technical_pos` | `technical_score` > 0 |
| `positioning_pos` | `positioning_score` > 0 |
| `valuation_pos` | `valuation_score` > 0 |
| `fundamental_pos` | `fundamental_score` > 0 |
| `catalyst_pos` | `catalyst_score` > 0 |
| `total_pos` | `total_score` > 0 |
| `score100_ge_60` | `score_100` ≥ 60 |
| `upside_pos` | `upside_pct` > 0 |
| `npos_gt_nneg` | `n_pos` > `n_neg` |

That is 11 × 3 = 33 atoms (`tr1d_price_pos` through `tr1m_npos_gt_nneg`).

Composite atoms, column in parentheses, rule is `> 0`:

`trc_resid_pos` (`trc_resid`), `trc_pressure_pos` (`trc_pressure`), `trc_ret_pos` (`trc_ret`), `trc_spec_duration_pos` (`trc_SPEC_DURATION`), `trc_quality_def_pos` (`trc_QUALITY_DEFENSIVE`), `trc_crowding_pos` (`trc_CROWDING`), `trc_size_tilt_pos` (`trc_SIZE_TILT`), `trc_mom_pos` (`trc_mom`), `trc_profitable_pos` (`trc_profitable`), `trc_leverage_pos` (`trc_leverage`), `trc_short_pos` (`trc_short`), `trc_beta_pos` (`trc_beta`), `trc_size_pos` (`trc_size`), `trc_index_pos` (`trc_index`).

Catalyst flags, rule is the column equal to 1:

`trf_cat_nuclear_smr`, `trf_cat_optics_transceiver`, `trf_cat_data_center_power`, `trf_cat_hbm_memory`, `trf_cat_copper_metals`, `trf_cat_ai_capex`, `trf_cat_defense`, `trf_cat_semiconductor_equip`.

`trf_upside_pos`: `trf_upside_pct_lvl` > 0.

Count: 12 panel + 7 Excel + 16 Finviz + 33 rubric + 14 composite + 8 catalyst + 1 upside = 91.

### 4.2 Pairs

Every unordered pair of two distinct atoms. 91 × 90 / 2 = 4,095. Both atoms must be true. A name that is absent from one side of the join fails the pair.

### 4.3 Day-over-day deltas (38)

A delta uses the value on the prior session and the value on this morning. Both must already be knowable at 09:30. The first session, 2026-08-13, has no prior search session, so every delta matches nobody and the book sits.

| id | rule |
| --- | --- |
| `d_hot` | `ohlc_hot_score` above the prior session |
| `d_ret5` | `ohlc_ret_5` above the prior session |
| `d_rvol` | `ohlc_rvol` above the prior session |
| `d_rsi` | `rsi` above the prior session |
| `d_ab_tone` | camera `ab` moved toward `good` (`bad` to `neutral` or `good`, or `neutral` to `good`) |
| `d_s` | morning S above the prior session |
| `d_tr1d_total` | `tr1d_total_score` above the prior trade_date |
| `d_tr1w_total` | `tr1w_total_score` above the prior trade_date |
| `d_tr1m_total` | `tr1m_total_score` above the prior trade_date |
| `d_trc_resid` | `trc_resid` above the prior trade_date |

Plus `trf_d_*` > 0 for these 28 columns, id `d_` plus the column slug (`d_price`, `d_market_cap`, `d_average_volume`, `d_relative_volume`, `d_perf_week`, `d_perf_month`, `d_perf_quarter`, `d_perf_ytd`, `d_rsi14`, `d_short_float`, `d_short_ratio`, `d_inst_trans`, `d_inst_own`, `d_insider_trans`, `d_analyst_recom`, `d_target_price`, `d_forward_pe`, `d_sales_yoy`, `d_sales_qoq`, `d_eps_surprise`, `d_profit_margin`, `d_gross_margin`, `d_sma20`, `d_sma50`, `d_sma200`, `d_beta`, `d_vol_month`, `d_debt_equity`):

`trf_d_Price`, `trf_d_Market Cap`, `trf_d_Average Volume`, `trf_d_Relative Volume`, `trf_d_Performance (Week)`, `trf_d_Performance (Month)`, `trf_d_Performance (Quarter)`, `trf_d_Performance (YTD)`, `trf_d_Relative Strength Index (14)`, `trf_d_Short Float`, `trf_d_Short Ratio`, `trf_d_Institutional Transactions`, `trf_d_Institutional Ownership`, `trf_d_Insider Transactions`, `trf_d_Analyst Recom`, `trf_d_Target Price`, `trf_d_Forward P/E`, `trf_d_Sales Year Over Year TTM`, `trf_d_Sales Growth Quarter Over Quarter`, `trf_d_EPS Surprise`, `trf_d_Profit Margin`, `trf_d_Gross Margin`, `trf_d_20-Day Simple Moving Average`, `trf_d_50-Day Simple Moving Average`, `trf_d_200-Day Simple Moving Average`, `trf_d_Beta`, `trf_d_Volatility (Month)`, `trf_d_Total Debt/Equity`.

### 4.4 Other levers

| lever | values |
| --- | --- |
| side | `long`, `short` |
| entry | `open_equal` (leftover cash split equally), `open_rank` (leftover cash weighted by rank). Both buy at the 09:30 open. |
| exit | `time` (sell the remainder at the open after `hold` sessions), `list` (sell at the open when the name leaves the list, after `min_hold`), `cut_loser` (sell a 3% loser at the next open; book `CUT_LOS`), `trail` (5% off the favorable extreme; book `TRAIL_OFF`) |
| hold and min_hold | hold is 1, 2, 3, 4, or 5 sessions. min_hold is an integer from 1 through hold. The 15 legal pairs are (1,1), (2,1), (2,2), (3,1), (3,2), (3,3), (4,1), (4,2), (4,3), (4,4), (5,1), (5,2), (5,3), (5,4), (5,5). |
| stop | `none`, `0.03`, `0.05`, `0.08` (fraction under the fill for a long, over the fill for a short) |
| top-N | `1`, `4`, `8`, `12` |
| universe | `panel` (that morning's `panel.json` rows), `theme_radar` (that `trade_date` on the frozen export; 11,568 to 11,659 tickers on the search sessions that have a row) |
| S gate | `off`, `gt_0` (new buys only when morning S > 0), `gt_5` (S > 5), `le_0` (S ≤ 0). S is the committed general-predict total score, else weather `general_score`. A missing S fails every gate except `off`. |
| hard-red | `off`, `on`. `on` sits new buys when S ≤ −3. A missing S with `on` sits. |

Signal count = 91 + 4,095 + 38 = 4,224.

The other levers multiply to 2 × 2 × 4 × 15 × 4 × 4 × 2 × 4 × 2 = 61,440.

**N = 4,224 × 61,440 = 259,522,560.**

Every one of those combinations is one try, including a combination that sits because a file is missing or a gate matches nobody.

## 5. Excel ML-lever spec

Checked `origin/main` at `ad3e064f66eb4e0c62e768a3f7f675d06db8a8f7` (2026-09-26T03:39:09Z). No ML-lever spec was on main.

`excel_ml_count: UNRESOLVED`

This is the one line still open. At or after 17:30 HKT on 2026-09-26 (09:30 UTC), look again at fullscan `main` for a single spec committed for this search. If it is there, set `excel_ml_count` to 1, record its path and sha256, and count that spec as one try. The spec is not unpacked into the grid. If it is not there, set `excel_ml_count` to 0 and the status to `pending, excluded`. Then recompute the fingerprint in the header. Do not wait past that check.

## 6. Walk-forward

Check days, in order: `2026-08-27`, `2026-08-28`, `2026-08-31`, `2026-09-01`, `2026-09-02`, `2026-09-03`, `2026-09-04`, `2026-09-08`, `2026-09-09`, `2026-09-10`, `2026-09-11`. Eleven days.

For each check day:

1. The choice set is every combination's after-fee compound return on the sessions in section 1 that are strictly before that check day. The first choice set starts at 2026-08-13.
2. Drop combinations with zero fires on that choice set (untestable).
3. Freeze the one winner: highest after-fee return, then highest without-best-stock return on that same choice set, then combination id ascending.
4. Write the frozen id and the choice-set fingerprint before scoring the check day.
5. Score that check day with the frozen combination. The check day's return is not an input to its own choice.

The walk-forward path is those eleven day returns, compounded in order.

## 7. Tally for the luck test

Three layers are already closed. The fourth is section 5.

| layer | tries |
| --- | ---: |
| this grid, N | 259,522,560 |
| OOS-0914 candidates, `data/factor_mine/oos0914_preregister.json` sha256 `989e05291a04a059062bed0ba15514ae674060679ded87de3c30447307fc659e` | 37 |
| Theme Radar prior tries (`theme_radar_search.tries_floor` in that same file) | 8,264 |
| Excel ML spec | `excel_ml_count` |

The luck-test denominator is N + 37 + 8,264 + `excel_ml_count`. The 37 and the 8,264 are prior searches on these same days. They are counted. They are not rerun. Each layer stays separate in the report.

Baselines, on the same sessions, after Futubull fees: RANDOM4 (4 names, 1,000 draws, seed `20260813`, hold 1, drawn from the combination's universe that morning) and IWM buy-and-hold. IRONCLAD rule 19.

## 8. What the scored run writes later

This commit writes none of these files.

Daily returns for every combination, sessions of section 1, one row per combination per session:

`research/lever_search/returns/` parquet, sharded, columns `combo_id`, `date`, `ret_futubull`, `ret_flat_15bp`. This is the input to the best-of-N luck test.

List A: combinations whose after-fee compound return on all 21 sessions is strictly above 20%.

List B: combinations on list A whose own after-fee compound return on the eleven check sessions is strictly positive, and whose after-fee compound return on the one designed-after window 2026-09-14 through 2026-09-25 is greater than or equal to zero. Beside each list-B row, report the without-best-stock after-fee return on the 21 sessions (drop the ticker with the highest attributed P&L; ties break to the earlier ticker). Also report the single walk-forward path from section 6.

The designed-after window is hindsight. It is not the clean record.

A named finalist is a list-B combination. At most 50 are named (IRONCLAD rule 18). Rank: walk-forward day-return compound on the days that combination was the frozen choice, descending; then the 21-session after-fee return, descending; then combination id ascending. A combination that was never the frozen choice has an empty walk-forward compound and ranks after those that were chosen. Each name is `ls_` plus the combination id. The strategy is built one day at a time from frozen 09:30 inputs. Days before 2026-09-28 are `designed_after`. The clean record starts 2026-09-28.

Keep bar (IRONCLAD rule 21) is reported and does not add or remove a list-A or list-B row: at least 30 fires and a win rate above 55% after fees. A recipe that never traded is `untestable` and stays out of the ranking (rule 22). No real money follows from this search (rule 23).

## 9. Refusal

The scored run refuses to start when the header fingerprint disagrees with the covered bytes, when `excel_ml_count` is still `UNRESOLVED`, or when a row dated 2026-09-14 or later is passed into the search or the walk-forward.
