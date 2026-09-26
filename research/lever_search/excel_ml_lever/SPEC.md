# Excel ML lever (`excel_ml_lever_h1`)

One lever. Hold 1 only. Hold 2 is not a variant and is not implemented.

This file is the frozen rule. The code that follows it is `excel_ml_lever.py` in this folder. Created 2026-09-26, before any session on or after 2026-09-14 is scored.

Every session in the published series (2026-08-13 through 2026-09-11) is before this creation date. Each of those days is `designed_after` (hindsight) under IRONCLAD rule 5. The series is for Excel's luck test. It is not a real record and it is not a live order.

Changing a feature, the target, the ridge penalty, the training minimum, the hold, the top-N, the fees, or the fill makes a new lever. This one stays as written.

## What it trades

The universe each morning is that session's rows on the fullscan morning panel, `data/factor_mine/panel.json`. That file is the 09:30 ET candidate list (about 60 to 100 names on a normal day; 9 names on 2026-08-13). Rows dated 2026-09-14 or later are dropped before any feature, fit, or score.

Prices are the factor-mine Yahoo tape `data/prices/ohlc.parquet`. Those bars are `auto_adjust=false`, which this repo treats as split-adjusted and not dividend-adjusted. A name with no positive open that session is dropped from the buy list. The loader refuses any bar dated 2026-09-14 or later. The luck-test load stops at 2026-09-11.

## Features (93)

Each feature is known at 09:30 ET on day N, or it is not used. Within each day, finite values are turned into average ranks and mapped to about -0.5 to +0.5. A missing value sits at 0 (the middle). Ties share one rank.

Morning panel scores and gates, as already stamped on that day's row (not recomputed by calling a model again):

- `src_rank`, `cond_good`, `cond_bad`
- `ohlc_ret_1`, `ohlc_ret_5`, `ohlc_ret_10`, `ohlc_rvol`, `ohlc_hot_score`
- `candle_score`, `candle_body_rg`
- `erd_days_since_E`, `erd_days_since_R`, `erd_days_since_D`
- `rsi`, `fv_rsi`, `macd`, `macd_sig`, `macd_hist`
- `close_loc`, `fv_rvol`, `fv_sma20`, `fv_sma50`, `fv_inst`, `rs_week`
- `opp_rvol`, `opp_gap_pct`, `opp_change_pct`, `n_neg`
- Box polarity, good = 1, neutral = 0, bad = -1, missing = empty: `box_join`, `box_sector`, `box_gen`, `box_news`, `box_digest`, `box_judge`, `box_ab`, `box_peer`, `box_heat`, `box_vol`, `box_catal`, `box_buy`
- True/false gates: `blue`, `alarm`, `zero_red`, `ohlc_nr7`, `ohlc_break_10`, `last_green`, `last_red`, `candle_capture`, `erd_earn_react`, `overnight_sched`, `erd_flag_E`, `erd_flag_R`, `macd_cross_up`, `macd_cross_down`, `rsi_os`, `rsi_ob`, `macd_up`, `macd_down`, `flow_in`, `ins_buy`, `form4_buy`, `oppset`, `opp_any`, `burst`
- Clock-B stamps already on the row: `clk_mom_break_peer`, `clk_fresh_cat_coil`, `clk_earn_guide_react`, `clk_neg_weak_fail`, `clk_ext_veto`, `clk_hold_vs_sector`, `clk_insider_cash_stab`, `clk_flow_coil`, `clk_r_up_coil`, `clk_nr7_mom`
- Morning polarity: `cat_e_pol`, `cat_r_pol`, `cat_news_prior`, `cat_news_box`, `cat_headline_tone`

`box_ab` is the AB gate. The panel does not carry a numeric `s_ab`. The enriched AB file is not re-read.

Oppset numbers are the Clock-B T-1 stamp (`opp_finviz_asof` is before the row's date on every row checked). If that as-of date is not before day N, the opp fields are blanked.

Price features, from bars with date strictly before N (today's open is not one of them):

- `px_ret1`: prior close / close before that, minus 1
- `px_ret5`: prior close / close five sessions earlier, minus 1
- `px_ret20`: prior close / close twenty sessions earlier, minus 1
- `px_gap_prior`: prior session's open / the close before that, minus 1
- `px_rvol_prior`: prior volume / mean of the last 20 prior volumes, including that prior bar, when at least 8 prior bars exist (same rvol as `ohlc_ripper.from_bars`)

Excel open letters from `excel_bot/research/excel_clear_letter_panel.csv`, joined on the panel date and ticker. The panel note says FQ, ER, EP, AH, FR and lagged DF are knowable at 09:30. Same-row DF, BB, and BQ are not in that file and are not used.

- `excel_FQ`, `excel_ER`, `excel_EP`, `excel_AH`, `excel_FR`
- `excel_prior_hammer`: 1 when `DF_lag1` contains "Hammer" and does not contain "Inverted", else 0 when a pattern is present, else missing

Excel suggestions from `excel_bot/suggestions/suggestions.csv`. `signal_date` is the confirm day. The job runs after the US close, and the fill is the next open. On morning N the prior session's confirm is known. `run_date` is not a clock (the file carries stale bake dates).

- `excel_sugg_long`: 1 if that ticker has a LONG row with `signal_date` equal to the prior panel session
- `excel_sugg_short`: same for SHORT
- `excel_sugg_n`: how many such rows

A suggestion with `signal_date` on or after 2026-09-14 is dropped at load.

## Excluded, and why

- `open`, `open_0930`: the buy fill, not a feature
- `close`: the same-day close is not known at 09:30
- `headline`, `e_label`, `r_label`: free text. The polarities above are the gates
- `sources`: an open list of list names. `src_rank` is the number
- `news_export_date`, `prior_date`, `opp_finviz_asof`: dates. The export date is only a leak guard (if it is not before day N, the news tones are blanked)
- `heat_vintage`, `_clock_b`: stamps, not name scores
- Excel `current_price`, `ret_vs_close`, `ret_vs_open`, `days_held`: tracking marks refreshed later
- Excel `ref_close`, `first_open`: prices, not the signal identity
- Excel `run_date`: not a clock
- Excel `signal_colors`: an open color vocabulary
- Excel `signal_date` equal to day N: that confirm uses day N's close
- Numeric `s_ab`: not on the panel. `box_ab` is the morning AB gate
- Morning S and the hard-red sit: a day-level rule, not a per-name panel column. This lever's pick rule is top 4, not that sit
- Theme Radar frozen export: not in the repo. The hook is off (`THEME_RADAR_ENABLED = False`). Oppset columns already stamped on the panel are the Clock-B aisle and they are included. Turning the hook on would be a new lever

## Target

Hold 1. Buy at the open of session D. Sell at the open of the next fullscan session E (Friday buys exit Monday).

The training target is E's open / D's open, minus 1, minus 0.0015 (15 basis points). That 15bp is a per-name haircut so the ranker sees an after-fee return. It is not the Futubull minimum schedule, because those minimums depend on share count.

A row is usable only when both opens are finite and positive.

## Model (fixed, no test-day tuning)

Rank the features inside the day, as above. On the training rows only, subtract the column mean and divide by the column standard deviation. A flat column becomes 0. Then ridge regression with an unpenalized intercept.

- Penalty `alpha = 10`
- Seed `7` (the fit itself has no draw)
- No cross-validation and no search over alpha

There is no hold-2 model.

## Training window

On morning N, with the fullscan session calendar:

- The exit of an entry on D is the open of the next session E. That print happens at 09:30 on E.
- It is known before 09:30 on N only when E is strictly before N.
- So the training entries are the sessions at index `<= N-2`. The latest exit is the open of N-1, which already printed the previous morning.
- The entry on N-1 is excluded. Its exit is today's open, and that open is not known before 09:30 on N.
- Day N's own rows are scored and never trained on.

The first pick waits until at least 8 such resolved sessions exist and at least 30 training rows have both opens. Before that the book sits: no buy, return 0. On this calendar the first pick is 2026-08-26 (entries 2026-08-13 through 2026-08-24, eight sessions, exiting by the 2026-08-25 open).

The fit is redone from those rows every morning. It does not reuse yesterday's coefficients. Days are still walked one at a time: day N's trades and equity are finished, and written, before day N+1 is fit.

## Picks, fills, book

Score day N. Buy the top 4 at the open, equal weight, long only. A higher score wins. A tie goes to the ticker that sorts first, A to Z. A name with no positive open is skipped and the next name fills the slot. A name that cannot be sold because it has no open is kept and is not bought again.

Cash starts at $10,000. Each buy's slice is leftover cash divided by the number of names bought that morning. Whole shares only. A slice that cannot buy one share is left as cash.

Sell a lot at the next session's open (time exit, hold 1). The day's percent is the change in equity from the previous session's equity to this session's equity after that morning's sells and buys. Holdings are marked at the fill just traded, not at the same-day close. That is the same session-equity percent the OOS-0914 mine stores as `ret_pct`.

The last scored morning is 2026-09-11. Its buys are in the series. Their open-to-next-open move is the 2026-09-14 open, and that session is not scored, so that move is not in the total. The 2026-09-11 percent is the exit of the 2026-09-10 buys, plus the 2026-09-11 buy fees.

A locked state file for a day is not rewritten. A fresh walk that disagrees with it fails.

## Fees

Primary figure: Futubull, `00_grounding/futubull_fees.json`, the same formula as `paper_trade.order_fees` (commission, platform, settlement, and on sells the regulatory fee and TAF, with the stated minimums and caps).

Alongside: the same shares and the same prices, charged 7.5bp per side (15bp round trip) instead of Futubull. Picks do not change.

Borrow, if a short were opened: 0.3% of short notional. This lever is long only, so the book pays no borrow.

## Output

`outputs/daily_returns.json` stores, for each session, `date`, `ret_pct` (Futubull, percent), `ret_pct_flat_15bp`, and `picks`. That is the OOS-0914 daily-return pair, plus the picks. `outputs/daily_returns.csv` is the same series in a flat table. `outputs/fills.json` and `state/<date>.json` are the orders and the locked day.

Nothing in those files is a session on or after 2026-09-14.

Published luck-test result, designed_after, 21 sessions, 12 mornings with a buy:

- Total return after Futubull: 0.8074% (equity $10,080.74)
- Total return at 15bp: 8.0679% (equity $10,806.79)

The gap is the Futubull per-order minimum on a $10,000 book split four ways. It is not a second model.

## Theme Radar hook

`THEME_RADAR_ENABLED` is false. The expected file would be `data/theme_radar/frozen_export.json` from SRoyaltyy/theme-radar. It was not available when this spec was frozen. The code does not wait for it and does not read it. Oppset fields already on the morning panel stay in the feature list above.

## Fingerprint

SHA-256 of the UTF-8 bytes of `excel_ml_lever.py`:

`be64b1f14a36fab1e2f4ec598276b77c9b5bc2eed74b6fd023025f20eeabe68a`

The SHA-256 of this SPEC.md file is in `frozen_spec.json`.
