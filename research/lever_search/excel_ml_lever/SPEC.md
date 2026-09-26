# Excel ML lever (`excel_ml_lever_h1`)

One lever. Hold 1 only. Hold 2 is not a variant and is not implemented.

This file is the frozen rule. The code that follows it is `excel_ml_lever.py` in this folder. Created 2026-09-26, before any session on or after 2026-09-14 is scored.

Every session in the published series (2026-08-13 through 2026-09-11) is before this creation date. Each of those days is `designed_after` (hindsight) under IRONCLAD rule 5. The series is for Excel's luck test. It is not a real record and it is not a live order.

Changing a feature, the target, the ridge penalty, the training minimum, the hold, the top-N, the fees, or the fill makes a new lever. This one stays as written.

`research/INPUT_HISTORY.md` is the source of truth for which columns and dates are legal. A column that file does not treat as knowable at 09:30 ET is not a feature.

## What it trades

The universe each morning is that session's rows on the fullscan morning panel, `data/factor_mine/panel.json`. That file is the 09:30 ET candidate list (about 60 to 100 names on a normal day; 9 names on 2026-08-13). Rows dated 2026-09-14 or later are dropped before any feature, fit, or score.

Prices are the factor-mine Yahoo tape `data/prices/ohlc.parquet`. Those bars are `auto_adjust=false`, which this repo treats as split-adjusted and not dividend-adjusted. A name with no positive open that session is dropped from the buy list. The loader refuses any bar dated 2026-09-14 or later. The luck-test load stops at 2026-09-11. Price features may use bars back to 2024-03-04. This lever's load starts at 2026-05-01, which is inside that window and is enough for a 20-session return before 2026-08-13.

## Features (87)

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

`box_ab` is the AB gate already stamped on the 31 morning sessions. The panel does not carry a numeric `s_ab`. The enriched AB file is not re-read, and it is not rebuilt for any date before 2026-08-13.

On a row dated before 2026-08-13, the LLM packet is blanked (missing, so it ranks at the center): `box_join`, `box_sector`, `box_gen`, `box_news`, `box_digest`, `box_judge`, `box_ab`, `box_heat`, `box_catal`, the news tones `cat_news_prior`, `cat_news_box`, `cat_headline_tone`, and every `clk_*` gate. Those columns exist only inside the 31 sessions (list size 9 to 118 names a day) and must not be regenerated earlier. Price fields on the row stay. The published panel already starts on 2026-08-13, so this guard does not change the luck-test rows. It stops a later backfill from inventing them.

Oppset numbers are the Clock-B T-1 stamp (`opp_finviz_asof` is before the row's date on every row checked). If that as-of date is not before day N, the opp fields are blanked. If `news_export_date` is not before day N, the news tones are blanked.

Price features, from bars with date strictly before N (today's open is not one of them). Same-day open, close, and volume are not features. Bars on or after 2026-09-14 are not read.

- `px_ret1`: prior close / close before that, minus 1
- `px_ret5`: prior close / close five sessions earlier, minus 1
- `px_ret20`: prior close / close twenty sessions earlier, minus 1
- `px_gap_prior`: prior session's open / the close before that, minus 1
- `px_rvol_prior`: prior volume / mean of the last 20 prior volumes, including that prior bar, when at least 8 prior bars exist (same rvol as `ohlc_ripper.from_bars`)

Excel features come only from `excel_bot/daily/<date>_excel_bot.md`. Draft notes are not signal files. The live `excel_bot/suggestions/suggestions.csv` is not read: it is rewritten through later dates and its `current_price`, `ret_vs_close`, and `ret_vs_open` columns are tracking marks. The clear-letter panel `excel_bot/research/excel_clear_letter_panel.csv` is not a morning input in INPUT_HISTORY (it was generated 2026-09-12) and is not read.

The job that writes a daily note runs after the US close. Some dates were committed twice, once midday and again after the close. For a note dated D, the legal blob is the last git commit strictly before the next fullscan panel session's 09:30 ET. A commit at 09:30 is too late. That is the same rule Factor Mine's freeze uses (`last_commit_before`: strictly before the open). That blob is a feature on that next session only. It is not a feature on D, and it is not picked up on a later morning. If every commit is at or after that open, the note is unused. If the next panel session is on or after 2026-09-14, the note is not loaded.

Two notes can share one morning. The 2026-09-04 note and the 2026-09-05 note both become visible at the 2026-09-08 open, and both are used that morning. Mornings before 2026-08-31 have no daily note, so the Excel flags are 0.

Only the New suggestions table is read. The parser stops at the next heading, so the scoreboard and the best and worst sections (live returns) are ignored. Columns kept from that table are ticker, side, and strategy. Strategy is an open name list and is not a model column. Signal colors are not a feature.

- `excel_sugg_long`: 1 if this ticker has a LONG row on a note pinned to morning N, else 0
- `excel_sugg_short`: same for SHORT
- `excel_sugg_n`: how many such rows (a name can match more than one strategy)

Pins used for this window (short sha, full sha is the git object):

- 2026-08-30 note, visible 2026-08-31: `2cc2571f494e` (the 15:45Z commit is earlier and is not used)
- 2026-09-01 note, visible 2026-09-02: `5fc8152510be`
- 2026-09-02 note, visible 2026-09-03: `a9ed6a403daa` (the 14:21Z commit is earlier and is not used)
- 2026-09-03 note, visible 2026-09-04: `1c8354e489fa`
- 2026-09-04 note, visible 2026-09-08: `fb22c0b5de2b`
- 2026-09-05 note, visible 2026-09-08: `284a7ad025f9`
- 2026-09-09 note, visible 2026-09-10: `ee2dc5369963`
- 2026-09-10 note, visible 2026-09-11: `4dc97fcbd5d4`
- 2026-09-11 note: unused. Its next session is 2026-09-14, which is not scored. The commit itself is also after the 2026-09-11 open.

## Excluded, and why

- `open`, `open_0930`: the buy fill, not a feature
- `close`: the same-day close is not known at 09:30
- `headline`, `e_label`, `r_label`: free text. The polarities above are the gates
- `sources`: an open list of list names. `src_rank` is the number
- `news_export_date`, `prior_date`, `opp_finviz_asof`: dates. The export date is only a leak guard
- `heat_vintage`, `_clock_b`: stamps, not name scores
- Excel `current_price`, `ret_vs_close`, `ret_vs_open`, `days_held`: tracking marks refreshed later
- Excel `ref_close`, `first_open`: prices, not the signal identity
- Excel `run_date`: not a clock
- Excel `signal_colors`: an open color vocabulary
- Excel strategy name: an open vocabulary. Side and row count are the features
- Excel `signal_date` equal to day N: that confirm uses day N's close, and the note is written after the close
- Excel clear-letter panel (FQ, ER, EP, AH, FR, lagged DF): not a legal morning input
- The live suggestions CSV: not the pinned note
- Scoreboard, best, and worst sections of the daily note: live returns
- A daily-note commit at or after the next session's 09:30: too late, and not reused later
- Numeric `s_ab`: not on the panel. `box_ab` is the morning AB gate, and only inside the 31 sessions
- Morning S and the hard-red sit: a day-level rule, not a per-name panel column. This lever's pick rule is top 4, not that sit. Hard-red is not rebuilt for earlier dates
- Sector essays, news judgments, and Grok review before 2026-08-13: not rebuildable
- Theme Radar frozen 09:30 export: optional and off. See the hook section. Oppset columns already stamped on the panel stay in the feature list above

## Target

Hold 1. Buy at the open of session D. Sell at the open of the next fullscan session E (Friday buys exit Monday).

The training target is E's open / D's open, minus 1, minus 0.0015 (15 basis points). That 15bp is a per-name haircut so the ranker sees an after-fee return. It is not the Futubull minimum schedule, because those minimums depend on share count.

A row is usable only when both opens are finite and positive.

## Model (fixed, no test-day tuning)

Rank the features inside the day, as above. On the training rows only, subtract the column mean and divide by the column standard deviation. A flat column becomes 0. Then ridge regression with an unpenalized intercept.

- Penalty `alpha = 10`
- Seed `7` (the fit itself has no draw)
- No cross-validation and no search over alpha

There is no hold-2 model and no second feature set.

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

- Total return after Futubull: 0.4731% (equity $10,047.31)
- Total return at 15bp: 8.7633% (equity $10,876.33)

The gap is the Futubull per-order minimum on a $10,000 book split four ways. It is not a second model.

## Theme Radar hook

`THEME_RADAR_ENABLED` is false. The declared feature list does not include Theme Radar columns, and the published series does not read the files.

The frozen export is on SRoyaltyy/theme-radar at commit `3973e13cd953e5705d08d8d9f78a5b1b9dd1a1d0`:

- `research/lever_panel/finviz_panel_asof0930_2026-08.csv.gz`, sha256 `c8977b8eea8e74115899e9d4cc04d5b4ea67490376d972905781eb8e1aeb6459`
- `research/lever_panel/finviz_panel_asof0930_2026-09.csv.gz`, sha256 `cbf35da9e1587703059abd9ff77525a1047c67a91edc3276ca93db4cd8669c16`

Each row is one stock on one morning, captured before that morning's 09:30 ET. The join key is `trade_date` plus `Ticker`. That key is unique in the export. It does not cover this lever's calendar: trade date 2026-08-28 has no prior-close snapshot, so that fullscan morning would be entirely missing. The files are also not in this repo. The declared lever therefore does not depend on them.

If the flag is turned on, the reader loads only an explicit whitelist (snapshot fundamentals and Theme Radar scores such as `Price`, `Relative Volume`, `tr1d_total_score`, `trc_pressure`, `seg_n_themes`). It does not read label, outcome, future-return, hit, or `trf_true_ret` columns. A test fails if a cell outside that allow-list is subscripted. Rows after 2026-09-11 are dropped, and a scrape stamp that is not strictly before 09:30 ET is dropped. The pinned sha256 is checked before a blob is used. Oppset fields already on the morning panel stay in the feature list above either way.

## Fingerprint

SHA-256 of the UTF-8 bytes of `excel_ml_lever.py`:

`55800c34419de632af9598f4c10ba112450c6a1bd03d147556de979ea4064694`

The SHA-256 of this SPEC.md file is in `frozen_spec.json`.
