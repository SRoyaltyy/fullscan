# Fullscan lever search — preregistration

- status: two layers written. The Excel ML line in section 5 is the one open line. The fingerprint is filled when that line closes, at or after 17:30 HKT on 2026-09-26 and before 17:45 HKT. This commit has no returns, no p-values, and no luck-test output.
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

The search is two layers. Each layer has its own combination count, its own rolling check, and its own line in the luck-test tally. Both daily-return series go into that test. A try in one layer is not a try in the other, even when the factor id matches.

Layer B is the full fullscan grid on the real morning panel, 2026-08-13 through 2026-09-11. Its core is the recipe space of the Factor Mine engine in section 4. That core is extended with the levers the engine did not have: Excel cards, Theme Radar fields, the full pairwise set, day-over-day deltas, stops, min-hold below the hold, entry sizing, the S gate, hard-red, and the `theme_radar` universe. It includes the inputs that cannot be rebuilt: enriched AB (`boxes.ab`), S, hard-red, sector, news, heat, catalyst, and the Theme Radar fields.

Layer A is rebuildable inputs only. Its universe is the price-only stand-in in section 6. Its span is 2024-04-02 through 2026-09-11.

## 1. Layer B sessions

Source: `data/factor_mine/panel.json`, sha256 `f9a4efc13a8aa13b92ac5847f0ef4f3dc4bddf0b16652684b183a9fe3170a506`.

Layer B's search and Layer B's walk-forward read these 21 sessions and no others:

`2026-08-13`, `2026-08-14`, `2026-08-17`, `2026-08-18`, `2026-08-19`, `2026-08-20`, `2026-08-21`, `2026-08-24`, `2026-08-25`, `2026-08-26`, `2026-08-27`, `2026-08-28`, `2026-08-31`, `2026-09-01`, `2026-09-02`, `2026-09-03`, `2026-09-04`, `2026-09-08`, `2026-09-09`, `2026-09-10`, `2026-09-11`.

`panel.json` also holds 2026-09-14 through 2026-09-25. The loader keeps a row only when `date` is in the list above. A kept row dated 2026-09-14 or later fails the run. The same cutoff applies to every other file: a bar, snapshot row, Excel signal, or Theme Radar row dated 2026-09-14 or later is not an input to the search or the walk-forward.

The designed-after look in section 9 is a separate open for each layer. It may read 2026-09-14 through 2026-09-25 only after that layer's walk-forward choices are written and fingerprinted. It does not read 2026-09-28. That session is the first day of any later clean record.

## 2. Clock and prices

Day N uses inputs knowable at 09:30 ET on day N, plus day N−1 cash, holdings, and fees. The book is built one session at a time. A restart from a saved day must match byte for byte.

Both layers walk every candidate that way. Sells and buys on day N come from the locked day N−1 lots and from inputs knowable at 09:30 on day N. A name already held is not bought again. A fill that was locked is not replaced. The old engine's `score_recipe` / `hold_return` does not do this: each session is graded as its own trade from that morning's open to the horizon close, and the next session picks again as if those shares were not still held. Compounding those overlapping equal-weight returns is the signal-only path. On the current scoreboard that path is the Signal% column. `union_vol_ab_h3` prints +81.51 there, against a cash-book Book% of +4.57. This search does not use that path.

Fills follow IRONCLAD rules 12 and 13. Buy at the 09:30 open. A stop fills at the stop, or at the open when the open gaps through it. When one bar touches both a stop and a target, the stop fills first. A finished hold sells at the next session's open. The close is the mark. The engine's horizon-close grade is not a fill.

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

## 4. Layer B — one combination

The core is the Factor Mine mining engine introduced in [PR #126](https://github.com/SRoyaltyy/fullscan/pull/126), commit `8e8c36a7117e60040d04cfa598e503b82fd7c6ea` (`Leak-free 09:30 factor strategy miner`, merged 2026-09-05). `build_recipes()` in that commit returns **110** recipes. The count is 15 universe baselines (5 lists × holds 1, 3, 5) + 38 single gates (19 gates × holds 1 and 3) + 5 of those gates at hold 5 + 20 named combos (10 × holds 1 and 3) + 2 one-offs + 12 rank recipes (6 ranks × holds 1 and 3) + 3 top-N recipes + 5 exit recipes + 10 shorts (5 × holds 1 and 3).

Those 110 are the engine's enumerated list. They are not an extra line in the luck-test tally. A grid point that lands on one of them is still one Layer B try.

The axes that list varies, and the values this grid extends them with:

| axis | values in the 110 | extended value list in this grid |
| --- | --- | --- |
| universe | `union`, `flatten`, `probable`, `yday_gainer`, `ohlc_hot` | those five, plus `theme_radar` |
| hold | 1, 3, 5 | 1, 2, 3, 4, 5, with min-hold 1 through hold |
| top-N | 4, 8, 12 (`TOP_N_DEFAULT` is 8) | 1, 4, 8, 12 |
| sell | time, exit on alarm, exit on last red, exit on news red | those four, plus `list`, `cut_loser`, `trail` |
| gate | empty, 19 single gates, 10 named combos | that gate list, plus the Excel, Theme Radar, Finviz, and remaining panel atoms below, their unordered pairs, and the deltas |

Side is already `long` or `short` in the 110. Stops, entry sizing, the S gate, and hard-red are not in `build_recipes()`. They are extension axes.

A Layer B combination picks exactly one signal and exactly one value from each other lever. A signal is one factor, or one unordered pair, or one named compound, or one day-over-day delta. It is not more than one of those at once.

The combination id, used for tie-breaks and the parquet key, is:

`{signal}|{side}|{entry}|{exit}|h{hold}m{min_hold}|stop{stop}|n{top_n}|u{universe}|s{s_gate}|hr{hard_red}`

`signal` is `f:{atom}`, `p:{a}+{b}` with `a` < `b` in byte order, `c:{compound}`, or `d:{delta}`.

`union` is that morning's `panel.json` rows. `flatten`, `probable`, `yday_gainer`, and `ohlc_hot` keep a row only when `sources` contains that name. `theme_radar` is the frozen export for that `trade_date`. A camera gate on a name that is not on the panel that morning matches nobody.

The 19 single gates and the named combos forbid `alarm`, as those recipes do in `build_recipes()`. Any other signal does not.

Rank is not a free lever. The engine's `rank_key` names are `src_rank`, `hot_score`, `candle_score`, `ret_5`, `cond`, `w_hot_cond`, and `w_hot_candle`. This grid fixes the key from the signal:

- a core gate with no score of its own, or a camera atom: `src_rank` ascending, then ticker
- any panel price atom, `hot_top`, or a delta of a price field: `hot_score` descending
- an Excel card alone: the card's own row order
- a Theme Radar score atom: that score descending
- a pair: the first of those rules that matches either member
- tie-break: ticker ascending

Top-N is applied after the gate. Names that fail the gate are not ranked.

### 4.1 Factors (110 atoms)

Panel atoms use `data/factor_mine/panel.json` for that session. A camera whose source file is missing that morning matches nobody (INPUT_HISTORY). Theme Radar atoms use the frozen export joined on `(trade_date, Ticker)`. Excel atoms use `excel_bot/suggestions/suggestions.csv`. A card with `signal_date` D is knowable after D's close, so it is an input on the next panel session, not on D. `current_price`, `ret_vs_close`, and `ret_vs_open` are ignored. The seven cards are the folders under `excel_bot/strategies/`. The committed CSV has rows for L1, L2, L3, and L5 only. L4, S1, and S2 have no row through 2026-09-25, so those atoms match nobody on this window and still count.

| id | rule |
| --- | --- |
| `ab_good` | `boxes.ab` is `good` (enriched checklist; not the price-only A rules) |
| `sector_good` | `boxes.sector` is `good` |
| `news_good` | `boxes.news` is `good` |
| `heat_good` | `boxes.heat` is `good` |
| `catal_good` | `boxes.catal` is `good` |
| `vol_g` | `boxes.vol` is `good` |
| `vol_missing` | `boxes.vol` is `missing` |
| `join_g` | `boxes.join` is `good` |
| `join_present` | `boxes.join` is not `missing` |
| `news_present` | `boxes.news` is not `missing` |
| `news_missing` | `boxes.news` is `missing` |
| `catal_present` | `boxes.catal` is not `missing` |
| `blue` | `blue` is true |
| `white` | `zero_red` is true |
| `last_red` | `last_red` is true |
| `candle` | `candle_capture` is true |
| `coil_off` | `ohlc_ret_5` is between 0 and 10 inclusive, and `ohlc_rvol` is between 0.7 and 2.2 inclusive |
| `earn_react` | `erd_earn_react` is true |
| `e_fresh` | `erd_days_since_E` ≤ 1 and `erd_flag_E` ≥ 0 |
| `r_up` | `erd_days_since_R` ≤ 5 and `erd_flag_R` is 1 |
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

Count: 16 panel + 15 engine gates + 7 Excel + 16 Finviz + 33 rubric + 14 composite + 8 catalyst + 1 upside = 110.

The 16 panel atoms are `ab_good`, `sector_good`, `news_good`, `heat_good`, `catal_good`, and the 11 price atoms from `last_green` through `hot_top`. The 15 engine gates are `vol_g` through `r_up`. Together with `ab_good`, `news_good`, `last_green`, and `break_10`, those are the 19 single gates in `build_recipes()`.

### 4.2 Pairs and named compounds

Every unordered pair of two distinct atoms. 110 × 109 / 2 = 5,995. Both atoms must be true. A name that is absent from one side of the join fails the pair.

Four engine combos are not a pair of two atoms. Each is one signal, id `c:` plus the name. They forbid `alarm`.

| id | rule |
| --- | --- |
| `c:probable_ok` | `last_green` and `ohlc_ret_5` ≤ 10 |
| `c:blue_coil` | `blue` and `ohlc_ret_5` ≤ 10 |
| `c:white_coil` | `white` and `ohlc_ret_5` ≤ 10 and `ohlc_rvol` ≤ 2.2 |
| `c:join_vol_green` | `join_g` and `vol_g` and `last_green` |

`coil_green` is the pair `coil_off` + `last_green`. `vol_ab`, `blue_vol`, `news_vol`, `e_green`, and `vol_green` are pairs of the atoms above. They are not a second signal.

### 4.3 Day-over-day deltas (44)

A delta uses the value on the prior session and the value on this morning. Both must already be knowable at 09:30. The first session, 2026-08-13, has no prior search session, so every delta matches nobody and the book sits.

| id | rule |
| --- | --- |
| `d_hot` | `ohlc_hot_score` above the prior session |
| `d_ret5` | `ohlc_ret_5` above the prior session |
| `d_rvol` | `ohlc_rvol` above the prior session |
| `d_rsi` | `rsi` above the prior session |
| `d_ab_tone` | `boxes.ab` moved toward `good` (`bad` to `neutral` or `good`, or `neutral` to `good`) |
| `d_sector_tone` | `boxes.sector` moved toward `good`, same steps |
| `d_news_tone` | `boxes.news` moved toward `good`, same steps |
| `d_heat_tone` | `boxes.heat` moved toward `good`, same steps |
| `d_catal_tone` | `boxes.catal` moved toward `good`, same steps |
| `d_vol_tone` | `boxes.vol` moved toward `good`, same steps |
| `d_join_tone` | `boxes.join` moved toward `good`, same steps |
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
| exit | `time` (sell the remainder at the next open after `hold` sessions), `list` (sell at the open when the name leaves the list, after `min_hold`), `cut_loser` (sell a 3% loser at the next open; book `CUT_LOS`), `trail` (5% off the favorable extreme; book `TRAIL_OFF`), `exit_alarm` (sell at the next open when `alarm` is true, after `min_hold`), `exit_last_red` (sell at the next open when `last_red` is true, after `min_hold`), `exit_news_bad` (sell at the next open when `boxes.news` is `bad`, after `min_hold`) |
| hold and min_hold | hold is 1, 2, 3, 4, or 5 sessions. min_hold is an integer from 1 through hold. The 15 legal pairs are (1,1), (2,1), (2,2), (3,1), (3,2), (3,3), (4,1), (4,2), (4,3), (4,4), (5,1), (5,2), (5,3), (5,4), (5,5). |
| stop | `none`, `0.03`, `0.05`, `0.08` (fraction under the fill for a long, over the fill for a short) |
| top-N | `1`, `4`, `8`, `12` |
| universe | `union` (that morning's `panel.json` rows), `flatten`, `probable`, `yday_gainer`, `ohlc_hot` (row `sources` must contain the name), `theme_radar` (that `trade_date` on the frozen export; 11,568 to 11,659 tickers on the search sessions that have a row) |
| S gate | `off`, `gt_0` (new buys only when morning S > 0), `gt_5` (S > 5), `le_0` (S ≤ 0). S is the committed general-predict total score, else weather `general_score`. A missing S fails every gate except `off`. |
| hard-red | `off`, `on`. `on` sits new buys when S ≤ −3. A missing S with `on` sits. |

Signal count = 110 + 5,995 + 4 + 44 = 6,153.

The other levers multiply to 2 × 2 × 7 × 15 × 4 × 4 × 6 × 4 × 2 = 322,560.

**N_B = 6,153 × 322,560 = 1,984,711,680.**

Every one of those combinations is one Layer B try, including a combination that sits because a file is missing or a gate matches nobody.

## 5. Excel ML-lever spec

Checked `origin/main` at `ad3e064f66eb4e0c62e768a3f7f675d06db8a8f7` (2026-09-26T03:39:09Z). No ML-lever spec was on main.

`excel_ml_count: UNRESOLVED`

This is the one line still open. At or after 17:30 HKT on 2026-09-26 (09:30 UTC), look again at fullscan `main` for a single spec committed for this search. If it is there, set `excel_ml_count` to 1, record its path and sha256, and count that spec as one try. The spec is not unpacked into either layer. If it is not there, set `excel_ml_count` to 0 and the status to `pending, excluded`. Then recompute the fingerprint in the header. Do not wait past that check.

## 6. Layer B walk-forward

Check days, in order: `2026-08-27`, `2026-08-28`, `2026-08-31`, `2026-09-01`, `2026-09-02`, `2026-09-03`, `2026-09-04`, `2026-09-08`, `2026-09-09`, `2026-09-10`, `2026-09-11`. Eleven days.

For each check day:

1. The choice set is every Layer B combination's after-fee compound return on the section 1 sessions that are strictly before that check day. The first choice set starts at 2026-08-13.
2. Drop combinations with zero fires on that choice set (untestable).
3. Freeze the one winner: highest after-fee return, then highest without-best-stock return on that same choice set, then combination id ascending.
4. Write the frozen id and the choice-set fingerprint before scoring the check day.
5. Score that check day with the frozen combination. The check day's return is not an input to its own choice.

The Layer B walk-forward path is those eleven day returns, compounded in order. Layer A does not enter this choice.

## 7. Layer A — rebuildable only

Layer A does not read `panel.json`, Theme Radar, or any LLM file. Opening one of those inside the Layer A search fails the run.

### 7.1 Universe

The membership rule is section 3 of [research/longhist/PREREG.md](../longhist/PREREG.md). That file's covered-body fingerprint is `0e519c4f8e3081571b79f23ef15f9946716ebeb46c354ea7e8c53470ec78394a` (SHA-256 of the UTF-8 bytes after `<!-- BEGIN COVERED -->`, including the final newline).

The symbol roster is the frozen listed leg only: `research/longhist/listed_common.txt`, sha256 `62609d9165bcbd5e7327516ea4da03f05297698d67aab660f207ddf8426195d5`, 4,193 symbols. The delisted leg in that prereg is not fingerprinted (`tickers.json` does not exist yet), so it is out of this tally.

On session D, apply section 3 unchanged: 20 prior bars, prior close in [$1, $30], 20-day mean dollar volume in [$1,000,000, $40,000,000], official open gap |G| ≥ 0.04, prior-day rvol ≥ 1.5 with `ok` true. If more than 60 names pass, keep 60 by larger |G|, then larger rvol, then ticker A→Z. Do not lower a threshold to fill the list. A dot in a symbol is sent to Yahoo as a hyphen. Bars are `data/prices` quote OHLC where the name-day is already stored.

### 7.2 Span

Store sessions are the distinct `date` values in `data/prices/ohlc.parquet` (644 dates, 2024-03-04 through 2026-09-25).

Layer A reads the 614 store sessions from `2024-04-02` through `2026-09-11` inclusive. `2024-04-02` is the first store session with 20 earlier store sessions, which is the hot-score and section-3 warmup. That is the ~2024-04 start. The search and the rolling check do not read a session after 2026-09-11.

A feature whose own lookback is longer stays false until that name has enough bars with `date` < session. The combination still counts. The lookbacks are: OHLC `from_bars` 5 bars, candle patterns 2 bars, RSI 15 closes, MACD 35 bars (`MACD_SLOW + MACD_SIGNAL`), A09 50 bars, A11 63 bars, A10 80 bars. Excel cards need 600 calendar days of that name's bars before `signal_date` (`DEEP_WARMUP_DAYS`). The first store session on which a name present since 2024-03-04 can clear that Excel warmup is 2025-10-27.

Calendar-year session counts inside the span: 2024 has 190 (2024-04-02 through 2024-12-31), 2025 has 250, 2026 has 174 (through 2026-09-11).

### 7.3 One combination

Same signal rule as Layer B: exactly one factor, or one unordered pair, or one day-over-day delta, plus one value of each other lever.

Id: `{signal}|{side}|{entry}|{exit}|h{hold}m{min_hold}|stop{stop}|n{top_n}|rg{regime}`

Rank: a price, candle, or AB atom, and `hot_top`, rank by `ohlc_hot_score` descending. An Excel card uses the engine's own row order. A pair uses the first of those rules that matches either member. Tie-break: ticker ascending.

#### Factors (41 atoms)

Price atoms use `ohlc_ripper.from_bars` on bars with `date` < session. The predicate is the same one Layer B reads off the panel row.

| id | rule |
| --- | --- |
| `last_green` | `last_green` is true |
| `break_10` | `break_10` is true |
| `rsi_os` | `rsi` ≤ 30 |
| `rsi_ob` | `rsi` ≥ 70 |
| `macd_cross_up` | `macd_cross_up` is true |
| `macd_up` | MACD histogram > 0 |
| `flow_in` | `flow_in` is true |
| `ret5_pos` | `ret_5` > 0 |
| `rvol_ge_1_5` | `rvol` ≥ 1.5 |
| `nr7` | `nr7` is true |
| `hot_top` | no boolean gate; rank by `hot_score` |

Candle atoms use `candle_factor.features` (8 completed bars, `date` < session). `low_s_*` rules are not here; they read S.

| id | rule |
| --- | --- |
| `cd_engulf_bull` | `engulf_bull` |
| `cd_engulf_bear` | `engulf_bear` |
| `cd_hammer` | `hammer` |
| `cd_shooting_star` | `shooting_star` |
| `cd_morning_star` | `morning_star` |
| `cd_three_green` | `three_green` |
| `cd_three_red` | `three_red` |
| `cd_body_rg_gt_1` | `body_rg` > 1 |
| `cd_vol_rg_gt_1` | `vol_rg` > 1 |

Price-only AB atoms. The flag is the committed `_pass_a` value, and the atom is true when that flag equals 1. Source is `src/ab_checklist.py` at git `01d6380c8ed6dfff34afc78feed675386b26bf68` plus the A15 patch in the current `src/ab_checklist.py`. Part B is not read.

| id | true when |
| --- | --- |
| `a01` | `A01_rsi_value` = 1 (RSI ≤ 30) |
| `a02` | `A02_rsi_cross_30` = 1 (cross up through 30) |
| `a03` | `A03_rsi_cross_50` = 1 (cross up through 50) |
| `a04` | `A04_rsi_cross_70` = 1 |
| `a05` | `A05_body_red_green_2day` = 1 |
| `a06` | `A06_volume_red_green_2day` = 1 |
| `a07` | `A07_rvol` = 1 (rvol ≥ 1.5) |
| `a08` | `A08_bollinger_position` = 1 (position ≤ −0.8) |
| `a09` | `A09_above_sma50` = 1 |
| `a10` | `A10_sma20_50_80_stack` = 1 (bull stack) |
| `a11` | `A11_three_section_lows` = 1 |
| `a12` | `A12_green_body_vs_wick_2day` = 1 |
| `a13` | `A13_red_body_vs_wick_2day` = 1 |
| `a15` | `A15_tape_recovery_setup` = 1 |

`A14_profitable_oversold_setup` is not an atom. It requires the part-B `profitable` flag, which is not a price. `A04` equals 1 only if a later code path sets it; the committed `_pass_a` sets `A04` to −1 on both cross directions, so `a04` matches nobody and still counts.

Excel atoms are the seven cards. Each `card.json` sha256:

| id | card | sha256 |
| --- | --- | --- |
| `xl_L1` | `L1_long_green_tp8_lowvol` | `01171dfa11a9ab6fe81a040d84e0c683c39a8e1aac38b96fae8258b983cc0805` |
| `xl_L2` | `L2_long_green_tp3_lowvol` | `04ee7e2994ccd616b95f249abda5f0e25b56653e8825669f5c176eee1c2b8978` |
| `xl_L3` | `L3_long_green_hold2_midcap` | `49ecdba5338d58bcaee9157a52220daa8b96801b9780ffc4ff9cf4269d29c556` |
| `xl_L4` | `L4_long_green_hold8_bbailike` | `3d48351ffa3f0b8e46b5df3460d494749690b6701af46de19b8ebb874f194f23` |
| `xl_L5` | `L5_long_green_hold2_midhibeta` | `dc289f7a0856f47af841738cd764952aeaa837853ff0b60a3d638bcee7201127` |
| `xl_S1` | `S1_short_red_1day_optionable` | `d4f29b1db30ab3097bd72ad7f472ccb4840b95a79429684e1b4fec504720f047` |
| `xl_S2` | `S2_short_red_1day_hivol` | `b90b3207da854d41f1d51a01b4a7a6c8f01061ca9894f6c7d20e31ed7776938d` |

A card with `signal_date` D is an input on the next store session. Color and hold math come from quote OHLC. A filter the price store cannot answer (market cap, beta, optionable) fails closed for that name unless a committed Finviz export with `date` < session already contains it. `current_price`, `ret_vs_close`, and `ret_vs_open` are ignored. Where `suggestions.csv` has that `signal_date`, the rebuilt membership is not required to match it: that CSV is a later tape and is not an input to Layer A.

Count: 11 price + 9 candle + 14 AB + 7 Excel = 41.

#### Pairs

Every unordered pair of two distinct atoms. 41 × 40 / 2 = 820. Both must be true.

#### Deltas (6)

The first span session, 2024-04-02, has no prior Layer A session, so every delta matches nobody.

| id | rule |
| --- | --- |
| `d_hot` | `hot_score` above the prior session |
| `d_ret5` | `ret_5` above the prior session |
| `d_rvol` | `rvol` above the prior session |
| `d_rsi` | `rsi` above the prior session |
| `d_cd_score` | candle `score` above the prior session |
| `d_ab_n` | count of {A01–A13, A15} equal to 1 above the prior session |

#### Other levers

Side is `long` or `short`. Entry is `open_equal` or `open_rank`. Exit is `time`, `list`, `cut_loser`, or `trail`. Hold and min-hold are the 15 pairs in section 4.4. Stop is `none`, `0.03`, `0.05`, or `0.08`. Top-N is `1`, `4`, `8`, or `12`. There is no universe lever, no S gate, and no hard-red lever. Layer A's exits do not include `exit_alarm`, `exit_last_red`, or `exit_news_bad`.

Regime is one lever. It gates new buys. It does not rank names. Labels use `00_grounding/weather_rules.json`, sha256 `9e2715fae8586906a082623da799dbc9814ee2f7e315c2a037358bcbe56969d8`, and the branches in `src/weather.py` for VIX and for FRED DGS10. They do not read the general-predict score, Fear & Greed, or a committed channel-1 JSON.

VIX label, from Yahoo daily `^VIX` and `^VIX3M` (else `^VXV`) closes with `date` < session: `spiking` when the ratio ≥ 1.10, `falling` when the ratio ≤ 0.90, else `spiking` when the 1-day change ≥ 1.5, `falling` when that change ≤ −1.5, else `spiking` when the spot ≥ 25, else `calm`. The ratio is VIX/VIX3M when both dates exist, else VIX over its 20-day mean when at least 10 closes exist. Missing VIX is `unknown`.

Yields label, from FRED `DGS10`, latest observation with `date` < session: `delta_1d` if present, else `delta_1w`. `rising` when the delta > 0.02, `falling` when the delta < −0.02, else `flat`. Missing DGS10 is `unknown`.

| regime | new buys |
| --- | --- |
| `off` | no regime gate |
| `vix_calm` | only when the VIX label is `calm` |
| `vix_not_spiking` | only when the VIX label is `calm` or `falling` |
| `yields_falling` | only when the yields label is `falling` |
| `yields_not_rising` | only when the yields label is `falling` or `flat` |

`unknown` fails every regime except `off`, and the book sits.

Signal count = 41 + 820 + 6 = 867.

Other levers: 2 × 2 × 4 × 15 × 4 × 4 × 5 = 19,200.

**N_A = 867 × 19,200 = 16,646,400.**

Every one of those combinations is one Layer A try, including a combination that sits.

### 7.4 Rolling check and yearly breakdown

Check days, the last 12 store sessions before 2026-09-14, in order: `2026-08-26`, `2026-08-27`, `2026-08-28`, `2026-08-31`, `2026-09-01`, `2026-09-02`, `2026-09-03`, `2026-09-04`, `2026-09-08`, `2026-09-09`, `2026-09-10`, `2026-09-11`.

The choice procedure is section 6, applied to Layer A combinations only. The choice set is Layer A sessions strictly before that check day, starting at 2024-04-02. Layer B does not enter this choice.

The yearly breakdown is a report, not an extra pass gate. For every Layer A combination, and for each of 2024, 2025, and 2026, write the after-fee compound on the span sessions in that calendar year, and the without-best-stock after-fee compound on those same sessions. 2024 starts 2024-04-02. 2026 ends 2026-09-11.

## 8. Tally for the luck test

| layer | tries |
| --- | ---: |
| Layer A, N_A | 16,646,400 |
| Layer B, N_B | 1,984,711,680 |
| OOS-0914 candidates, `data/factor_mine/oos0914_preregister.json` sha256 `989e05291a04a059062bed0ba15514ae674060679ded87de3c30447307fc659e` | 37 |
| Theme Radar prior tries (`theme_radar_search.tries_floor` in that same file) | 8,264 |
| Excel ML spec | `excel_ml_count` |

The luck-test denominator is N_A + N_B + 37 + 8,264 + `excel_ml_count`. With `excel_ml_count` at 0 that is 2,001,366,381. The 37 and the 8,264 are prior searches. They are counted. They are not rerun. Each line stays separate in the report.

Baselines, on that layer's own sessions, after Futubull fees: RANDOM4 (4 names, 1,000 draws, seed `20260813`, hold 1, drawn from the combination's universe that morning) and IWM buy-and-hold. IRONCLAD rule 19.

## 9. What the scored run writes later

This commit writes none of these files.

Daily returns for every combination, one row per combination per session of that layer:

- `research/lever_search/returns/layer_a/` parquet, sharded, the 614 sessions
- `research/lever_search/returns/layer_b/` parquet, sharded, the 21 sessions

Columns `combo_id`, `date`, `ret_futubull`, `ret_flat_15bp`. Both series are inputs to the best-of-N luck test.

`research/lever_search/returns/layer_a_years.parquet`: `combo_id`, `year`, `ret_futubull`, `ret_without_best`.

Layer B list A: after-fee compound on all 21 sessions strictly above 20%.

Layer B list B: on list A, own after-fee compound on the eleven check sessions strictly positive, and after-fee compound on the designed-after window 2026-09-14 through 2026-09-25 greater than or equal to zero. Beside each row, the without-best-stock after-fee return on the 21 sessions (drop the ticker with the highest attributed P&L; ties break to the earlier ticker). Also the single Layer B walk-forward path.

Layer A has no 20% screen. That bar was set for 21 sessions. A Layer A naming candidate has a strictly positive after-fee compound on its twelve check sessions, and an after-fee compound on the same designed-after window greater than or equal to zero. Beside each candidate, the yearly breakdown and the without-best-stock after-fee return on the 614 sessions.

The designed-after window is hindsight. It is not the clean record.

Named finalists are Layer B list B plus Layer A naming candidates, at most 50 together (IRONCLAD rule 18). Rank: that layer's walk-forward compound on the days the combination was the frozen choice, descending; then that layer's full-span after-fee return, descending; then combination id ascending. A combination that was never the frozen choice ranks after those that were chosen. Layer A names start with `lsa_`. Layer B names start with `lsb_`. The strategy is built one day at a time from frozen 09:30 inputs. Days before 2026-09-28 are `designed_after`. The clean record starts 2026-09-28.

Keep bar (IRONCLAD rule 21) is reported and does not add or remove a row: at least 30 fires and a win rate above 55% after fees. A recipe that never traded is `untestable` and stays out of the ranking (rule 22). No real money follows from this search (rule 23).

## 10. Refusal

The scored run refuses to start when the header fingerprint disagrees with the covered bytes, when `excel_ml_count` is still `UNRESOLVED`, when a row dated 2026-09-14 or later is passed into either search or either rolling check, or when the Layer A search opens `panel.json`, a Theme Radar file, or an LLM morning file.
