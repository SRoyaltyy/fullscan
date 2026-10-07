# hot_n4_clean_v4 — preregistration

- status: locked before any score. This commit has no return, no win rate, and no trade outcome.
- written: 2026-09-27
- fingerprint_sha256: d1f36a229fd8d6eedb8b9f840a53b6ad6a9019bdaefb8e52e8bf4e57ac78fbcf
- engine restatement: 2026-09-28 ET, Cyrus approved. `src/factor_mine.py` sha256 moved from `ce4f1954b0c5e97009dedf6c2d7604d8c225a5633c96090d6e2b20f9e04a9272` to `b51eed634fb5ce213e3a2e1f85c83749e97ee42519b33d1d60ad7ad0c0047860`. See RESTATEMENTS.md.
- engine restatement: 2026-10-07. `src/paper_trade.py` sha256 moved from `54e70b314dc0b959b45573343a234f46bb396588ed7f65dff678d79c88d23f9d` to `ebda9df4a672481d28b0464e7f5b50abf430034be63eb65bc582a27462417b4b` after #502. The change is the paper CSV past-day lock. `order_fees` is unchanged. See RESTATEMENTS.md.
- fingerprint_scope: SHA-256 of the UTF-8 bytes after the line `<!-- BEGIN COVERED -->`, including the final newline. Line endings are LF. The header above the marker is not covered.
- study: `research/hot_n4_clean_v4/`. New files only. No edit to an engine, to `src/`, to `forward_shadow`, to `hot_n4_clean_v1`, to `hot_n4_clean_v2`, to `hot_n4_clean_v3`, or to another study.
- sessions: 2026-08-13 through 2026-09-25. The before window ends 2026-09-11. The after window starts 2026-09-14 and can only reject.

<!-- BEGIN COVERED -->

## 0. What this file is

This is the protocol for one sequential rebuild of two already-named recipes. It does not choose a new recipe. It does not score a day. A later scoring commit may write under `returns/`. It must not rewrite `days/`.

Changing a gate, a rank, a hold, a sell, a fee, a fill, a candidate source, a pin, the variant count, or the luck N after this fingerprint is a new study.

The two ids are `union_hot_n4_h1__w0` and `union_hot_n4_holdup__w0`, the weather-off forms of the v4 parts `union_hot_n4_h1` and `union_hot_n4_holdup`.

This study copies `research/hot_n4_clean_v3` except the candidate-removal rule in section 5. It does not edit v1, v2, or v3. The real-move table is unchanged.

## 1. Engine semantics

The later run calls the pinned copies of `pick_day`, `matches`, `should_exit`, and `lot_should_sell`. It does not reimplement their branches. If a working-tree hash in section 11 differs from the pin, the run stops and does not use the changed file.

Both variants:

| field | value |
| --- | --- |
| universe | `union` |
| rank | `hot_score` |
| top_n | 4 |
| hold | 1 |
| sell | `list` |
| side | `long` |
| weather | off |
| require | empty |
| forbid | empty |
| exit_when | empty |
| earn_news | false |
| skip_first | false |

`union_hot_n4_h1__w0` has `s_boost=none`. `union_hot_n4_holdup__w0` has `s_boost=holdup`. The v4 default forbid `{"alarm": true}` is not copied. Section 9 says why.

`pick_day` keeps rows `matches` accepts, sorts by `rank_key`, and takes `top_n`. For `hot_score` the key is `(-hot_score, ticker)`: higher score first, ticker A to Z on a tie. `matches` with empty require and empty forbid accepts every candidate row. `should_exit` with empty `exit_when` does not fire an early exit.

`lot_should_sell` for `sell=list`, with no take and no stop: a name still on the list is kept; a name that has dropped is sold only after `min_hold` sessions. The sell print is that session's official open from the pinned bars, never the prior close. Rule 9 covers a missing open.

Holdup, and only the holdup variant: when the morning score S is not null, S > 0, and the book is not sitting out, a new lot's `min_hold` is `max(1, 2) = 2`. `HOLDUP_S` is 0 and `HOLDUP_SESS` is 2, the same constants as v4. Weather is off, so the hard-red sit is false on every session. When section 8 says the morning file is absent, holdup does not apply and `min_hold` stays 1. In this window the file is present every day. This commit does not record the sign of S.

Fill is `keep_held`. A name that is still selected and already held is kept. That keep has no new trade and no new fee. The flat 15bp figure is a second print of the same fills, not a second variant and not a renew book. Renew is not scored.

Capital is $10,000. Sells run before buys. Leftover cash is split equally across the new names (`split_budgets` mode `leftover`). Shares are whole. A name that cannot buy one share is skipped, and that leftover is not offered to a later name. A buy with no open does not fill. Rule 9 covers the held lot.

Futubull fees are `paper_trade.order_fees` with `00_grounding/futubull_fees.json` sha256 `019ebdba0fc0b20e02c91f116dc5591b81e96630f60c110dfbfd3b5d8e16c0d3`, charged on the actual open. Rule 9 adds 0.5% per side on top of that fee. That sum is the primary figure. The flat 15bp print charges 7.5bp per side (`0.000075` times shares times the actual open, rounded to 4 decimals) on those same shares, with no extra slip. That is the v4 `fee_15` definition. It is a fee-schedule line, not a slip rate, and not a third variant.

`hot_score` is the pinned `ohlc_ripper.hot_score` on `from_bars`. With `ok` true:

`0.08 * max(ret_5, 0) + 0.04 * max(ret_10, 0) + 0.4 * min(rvol, 3) + 1.2 if break_10 else 0 + 0.3 if last_green else 0`

With `ok` false the score is 0. `ok` needs at least 5 bars. `ret_5`, `ret_10`, and `ret_1` are percent points from adjusted closes (`100 * (last / older - 1)`), using `c[-6]` and `c[-11]` when those bars exist. `rvol` is `volume[-1] / mean(volume[-20:])` when there are at least 8 bars, otherwise the mean of the bars in hand. `break_10` is the last close above the max high of the preceding bars (`h[-11:-1]` when 11 bars exist, otherwise every high but the last). `last_green` is last close above last open. `too_extended` is `ret_5 > 18` or `rvol > 2.8`, and those names are dropped from `ohlc_hot` and from `probable` before the top-N cut. The panel path does not drop yesterday's losers. `watchlist` does; this rebuild does not call that drop.

## 2. Luck N

The variant count is 2. Luck N is 22,017.

`research/concentration_cap_v3/protocol.py` locks `LUCK_N = 22009`, which is `84 + 84 + 45 + 21796`. `21796` is v4's `21536` plus the concentration screen's 260. v4's `21536` is `150 + 9500 + 9132 + 102 + 1326 + 1326`. `research/hot_n4_clean_v1/protocol.py` locks `LUCK_N = 22011`, which is `22009 + 2`. `research/hot_n4_clean_v2/protocol.py` locks `LUCK_N = 22013`, which is `22011 + 2`. `research/hot_n4_clean_v3/protocol.py` locks `LUCK_N = 22015`, which is `22013 + 2`. This study adds the same two variants and nothing else. Keep-held, the flat 15bp print, and the 0% and 1% slip lines are one try. `22015 + 2 = 22017`.

## 3. Sequential generation

The build walks `SESSIONS` in order. There are 31 sessions. `2026-09-07` is Labor Day and is not a session. The before window is the 21 sessions `2026-08-13` through `2026-09-11`. The after window is the 10 sessions `2026-09-14` through `2026-09-25`.

Day D is written once, to `research/hot_n4_clean_v4/days/D.json`, and only by `commit_day`. The bytes are canonical JSON: UTF-8, keys sorted, separators `(",", ":")`, one trailing newline. The sha256 of those bytes is appended to `days/LEDGER.jsonl` as one JSON object, also canonical, with keys `bytes`, `date`, and `sha256`.

If `days/D.json` already exists and the new bytes differ, `commit_day` raises and does not write the file and does not append a line. If the bytes are the same, it does not append a second line. A later session file must not exist when D is first written. Every earlier session file must already exist. The ledger dates are a prefix of `SESSIONS`, with no duplicate and no gap.

`verify_ledger` checks every line on every run: the hash matches the file, the byte count matches, no day file lacks a line, no line lacks a file, and no day file contains an outcome key. An empty ledger with no day file is the starting state and it passes. This commit ships that empty ledger. The CI job `.github/workflows/hot_n4_clean_v4.yml` runs the check.

The day file is an input card. It may carry the session, the bar cutoff (the previous session), the source map from section 7, the S source from section 8, the candidate rows that `pick_day` will see (ticker, source names, adjusted `hot_score`, `ret_5`, `ret_10`, `rvol`, `break_10`, `last_green`, and the prior-session adjusted return used to rank gainers and movers), the price and panel hashes, and `excluded_unexplained_legs`. It must not carry a P&L, a win, a win rate, a compound, an equity, a day return, an up-day share, or any other key in `OUTCOME_KEYS`. A later score lives under `returns/` and does not rewrite `days/`.

The candidate removal in section 5 runs after the candidate list is built and before ranking. A removed name is logged on the day card. The day file is still written. If the book already holds that name, the halt in section 5 still refuses the day file and does not append the ledger.

## 4. What day D may read

Day D reads only files labelled on or before D.

An input that is not a deterministic function of the pinned price file is used only when a copy was on GitHub before 13:30 UTC on D. 13:30 UTC is 09:30 EDT, the cash open. The proof is `research/audit/FULLSCAN_FILE_PROOF.csv` and `research/audit/INPUT_PROVENANCE_336.md` at fullscan commit `4d964db7a8c975ff5151a859577a9d34d993e6ca` (the #354 proof). `proven=yes` and `before_0930=yes` are required. `PROVEN_BUT_CHANGED` counts. The blob is the pre-open blob at `server_time_utc`, not a later edit of that path.

A file labelled after D is not read. A missing proof is not filled in. There is no substitute column, no second vendor, and no hand-typed list.

## 5. Prices, adjustment, and the jump check

Bars are `data/prices/ohlc.parquet` sha256 `559c8cf099808930bef2b4de4280b4e902883c9a1de85c8a417074f11aaefa55`, git blob `3456f7f489a6fa7033e8ae5cc942d8279f0113e3`. The file is not copied into this folder. `src/price_store.py` stores the session print with `AUTO_ADJUST` false, and a stored bar is never replaced. A split is a level jump in that file. It is not a back-adjusted series.

Features use bars with `date < D` only, after the adjustment below. D's official open is the fill and the list-drop sell. It is not a feature. D's close is an accounting mark for a later equity line when the open exists. It is not read when the candidate list or the rank is built. A missing open is rule 9: the buy does not fill, and a held lot that cannot be sold is marked at the last close.

Only returns and ratios come from these bars: hot score, returns, rvol, candles, and breakouts. Any price-level test or dollar-volume test uses the Finviz `Price` and `Volume` on the frozen row for that `trade_date`. This protocol's liquid gate does not apply a $1 cutoff, a $5 cutoff, or a price-times-volume floor. Adjusted closes are never used as a price level.

Split factors are the nonzero `split` rows in `data/factor_mine/retro_prices/actions.parquet` sha256 `9007c3aec0db3fa42735bf10ad8611e19ba78a2d532cec0df8f99205fbd6b633`, git blob `b583117809d0ccac78b766084d5cb04ca6d6dec3`. The stored value is Yahoo's share factor. The price factor is `1 / share_factor`. `data/prices/actions.parquet` sha256 `0471b2d76c30960eb494134a7bd34eea499bb5c696190f5fb3628c4becd94e4a` has no nonzero split in this window and is not the explanation table.

For each row in `SPLITS.md`, bars strictly before `boundary` have OHLC multiplied by the price factor and volume multiplied by the share factor. Bars on or after `boundary` stay as stored. Dividends are not applied. The full table, the excluded names, and the boundaries are in `SPLITS.md`, sha256 `4c9d3b2547afbb74b7f95a7e7fdb0aab19e68c0d504bddcd93a94750eeac47a6`. Eleven in-universe names are in that table. Nine are the v1 retro rows: BYND, AVB, NRDY, SFBS, BRCC, REAX, RUSHA, APH, and IGR. Two are the primary-source rows v2 added.

MVIS is a 1-for-15 reverse split. Share factor `1/15`, price factor 15, ex-date and boundary 2026-08-03. Form 8-K filed 2026-07-22, accession `0001493152-26-034194`, https://www.sec.gov/Archives/edgar/data/65770/000149315226034194/form8-k.htm. The amendment is effective at 5:00 p.m. ET on 2026-08-01. Nasdaq trading on the split-adjusted basis begins at the open on 2026-08-03. The stored open that morning is 4.06 against the 2026-07-30 close of 0.24.

AEHL is a 1-for-16 reverse split. Share factor `1/16` (`0.0625`), price factor 16, ex-date and boundary 2026-08-10. Form 6-K filed 2026-08-05, accession `0001493152-26-036122`, https://www.sec.gov/Archives/edgar/data/1470683/000149315226036122/form6-k.htm. It is effective at 4:01 p.m. ET on 2026-08-07. Nasdaq trading on the split-adjusted basis begins at the open on 2026-08-10. The retro file has the same share factor on that date. The citation is the 6-K, not the retro cell. v1 left it out because 2026-08-10 is before the session window.

The IRONCLAD jump check uses `RATIO_HI = 3`, `RATIO_LO = 1/3`, and `SPLIT_TOL = 0.25` from `src/breadth_rank_v1c_bars.py`. The legs are open over the previous stored close, and close over open. A leg is a verified split when `SPLITS.md` has that ticker, the ratio is within 25% of the price factor or of the share factor, and the recorded ex-date is after the previous stored bar and no later than 5 NYSE sessions after the jumping bar. The 5-session slack is what places REAX (jump on 2026-08-20, recorded ex-date 2026-08-25) and the other early jumps in `SPLITS.md` on the verified side. A ratio equal to 3 is not a halt, because the code test is strict. The nine retro rows remain in `SPLITS.md`. MVIS and AEHL are verified by the primary-source rows in that file, not by a retro row. A verified split is adjusted as that table says. It does not halt.

v3's rule for a leg that is not a verified split stays unchanged. That leg is a real price move when a primary source for that issuer is dated inside the five-session window and shows no share change. The bars are used as stored. There is no adjustment and no halt. A primary source is an SEC EDGAR filing by the issuer, such as a Form 8-K, 6-K, 10-Q, 20-F, or 424B, or an exchange notice. A secondary aggregator is not a primary source. The window is the NYSE session five sessions before the jumping bar through the NYSE session five sessions after it, including a weekend or holiday that falls between those two sessions. The holiday set is the XNYS set already used for `SESSIONS`, including Memorial Day 2026-05-25, Juneteenth 2026-06-19, Independence Day observed 2026-07-03, and Labor Day 2026-09-07.

A filing shows a share change when it states a split, a reverse split, a share consolidation, or a share distribution, and the effective date, the record date, or the ex-date is inside the window or is not stated. An exchange ratio that cancels shares and issues a different number of shares is a share change. A spin-off distribution is a share change. A filing that only recites a split whose effective date is outside the window does not show a share change for this leg. A resale prospectus that does not state one of those actions does not either. A filing that shows a share change and is not the verified split for this leg is not a real price move. That leg stays unexplained. A leg with no primary source in the window also stays unexplained. v1 and v2 halt on it. v3 halts on it. This study removes a candidate that loads it, and still halts when the book already holds the name.

The check covers every bar the day reads for IWM and for every name that enters that day's candidate list or is already held, including the fill open and the mark close. The bars are the last 60 stored bars with `date < D` and session D's own bar. It runs before `commit_day`. A real price move does not halt and is not removed. IWM has no 3x leg in the window. An IWM jump is not a baseline for another name.

The one rule this study adds is what happens to a still-unexplained leg. On session D, after the candidate list in section 7 is built and before `pick_day` ranks it, a candidate is removed when any loaded bar contains a still-unexplained >3x leg. A still-unexplained leg is one that is not a verified split in `SPLITS.md` and not a filing-verified real move in the table above, which is the same table as v3. The name is not replaced by a hand-picked substitute. Ranking then takes `top_n` from the names that remain, so the only name that fills the slot is the next name that rank order already produces.

The removal is logged on that day's card, `days/D.json`, under `excluded_unexplained_legs`. Each object has `ticker`, `bar_date`, `leg`, and `reason`. The day file is still written. If the book already holds that ticker when day D reads the bar, this removal does not apply. The v1 halt is unchanged: do not write `days/D.json` and do not append the ledger.

A held-name halt is possible in principle only when the book already holds the ticker on a session whose loaded bars include that ticker's still-unexplained leg. That holding has to come from an earlier session that bought the name while the leg was not among the bars that session read, with the lot still open when a later session reads the leg. Whether that buy happens depends on hot-score ranking, leftover cash, and holdup. This commit does not score, so it does not decide that any name is held and it does not set a halt date. The book opens flat on 2026-08-13. The exclusion plan below is an input, not a scored path: XHG, JLHL, and SLBT are removed on every session where they are candidates and the leg is loaded, and none of the five names is a candidate on an earlier session that does not load its leg. MFP and MB are not candidates on any session that loads the leg. A buy of these five therefore cannot be open when the leg is read, unless a path this protocol does not have is already holding them. The halt rule for a name that is already held stays in force anyway.

`EXCLUSIONS_PLAN.csv`, sha256 `86de3c366759a0c06d20ea8af1d1e967978d7e6e8f927951f71990cba3c61ea7`, lists those removals for every session from 2026-08-13 through 2026-09-25. It is inputs only. It has no return, no win rate, and no P&L. A session with no removal is one row with `n_excluded` 0 and an empty ticker. A session with removals has one row per leg. `n_excluded` is the count of distinct tickers that day. The reasons are: XHG and JLHL and MB, no filing; SLBT, 20-F de-SPAC exchange ratio, unverified share change; MFP, spin-off distribution, unverified.

| session | n_excluded | names |
| --- | --- | --- |
| 2026-08-13 | 0 | |
| 2026-08-14 | 0 | |
| 2026-08-17 | 0 | |
| 2026-08-18 | 0 | |
| 2026-08-19 | 1 | XHG |
| 2026-08-20 | 0 | |
| 2026-08-21 | 1 | XHG |
| 2026-08-24 | 1 | XHG |
| 2026-08-25 | 1 | XHG |
| 2026-08-26 | 1 | XHG |
| 2026-08-27 | 1 | XHG |
| 2026-08-28 | 0 | |
| 2026-08-31 | 0 | |
| 2026-09-01 | 0 | |
| 2026-09-02 | 0 | |
| 2026-09-03 | 2 | JLHL, SLBT |
| 2026-09-04 | 0 | |
| 2026-09-08 | 0 | |
| 2026-09-09 | 0 | |
| 2026-09-10 | 1 | XHG |
| 2026-09-11 | 1 | SLBT |
| 2026-09-14 | 0 | |
| 2026-09-15 | 0 | |
| 2026-09-16 | 0 | |
| 2026-09-17 | 0 | |
| 2026-09-18 | 0 | |
| 2026-09-21 | 0 | |
| 2026-09-22 | 0 | |
| 2026-09-23 | 0 | |
| 2026-09-24 | 0 | |
| 2026-09-25 | 0 | |

Nine sessions remove a name. Twenty-two sessions, including the gap day 2026-08-28, remove none. The name-day count is 10. XHG is removed on 7 sessions, SLBT on 2, JLHL on 1. MFP and MB are not removed, because they are not candidates while the bar is loaded. Each logged leg is the leg in v3's unexplained table. XHG logs both the open over the previous close and the close over the previous close. JLHL logs the close over open and the close over the previous close. SLBT logs the open over the previous close.

`JUMP_SCAN.csv`, sha256 `52ec0d95f5295d0b5005de786bcb58519ec66ef7305639dd842279e15ea4155e`, is every leg the scan found that the v1 nine-row table does not explain. The scan walks every liquid name on every session from 2026-08-13 through 2026-09-25. For each of those names it reads the last 60 stored bars before the session and the session's own bar, which is the window `ohlc.features` passes to `from_bars` plus the fill open and the mark close. A leg is an open or a close more than 3 times the previous stored close, or a close more than 3 times the same bar's open. The same scan run again for this study found 33 legs. Twenty-five are the rows in v2's `JUMP_SCAN.csv`. The other eight are BYND on 2026-08-17, NRDY on 2026-08-17, BRCC on 2026-08-20, and REAX on 2026-08-20, each as open over the previous close and close over the previous close. Those eight are verified by the v1 nine-row table. No unexplained leg was missing from v2. MVIS and AEHL stay verified. Thirteen legs are real price moves. Eight stay unexplained.

`REAL_MOVES.csv`, sha256 `03e5c6704a6d3af65810080cf90e4017e784626e55ee4e397e0ceda87156c149`, is those thirteen legs. Each row has the ticker, the bar date, the leg, the filing type, the filing date, the accession, the URL, and a one-line note of what the filing says. The same fields are columns of `JUMP_SCAN.csv`. A real price move leaves `first_halt_session` blank.

| ticker | bar date | leg | form | filed | accession | what the filing says |
| --- | --- | --- | --- | --- | --- | --- |
| SION | 2026-08-10 | open over previous close | 8-K | 2026-08-10 | `0001193125-26-341230` | Topline data from the PreciSION CF Phase 2a trial of SION-719 and a Phase 1 trial of SION-451. It does not state a share change. The same-window 10-Q and earnings 8-K do not either. |
| SION | 2026-08-10 | close over previous close | 8-K | 2026-08-10 | `0001193125-26-341230` | Same filing. |
| CAPR | 2026-07-27 | open over previous close | 8-K | 2026-07-29 | `0001104659-26-087891` | Phase 3 HOPE-3 results published in The Lancet, with an LVEF model update. It does not state a share change. The July 30 8-K on the FDA advisory vote does not either. |
| EYPT | 2026-08-17 | open over previous close | 8-K | 2026-08-17 | `0001193125-26-353308` | Topline data for the LUGANO Phase 3 trial of DURAVYU in wet AMD. It does not state a share change. |
| EYPT | 2026-08-17 | close over previous close | 8-K | 2026-08-17 | `0001193125-26-353308` | Same filing. |
| XHLD | 2026-08-06 | close over open | 8-K | 2026-08-10 | `0001493152-26-036864` | Quarterly results for the quarter ended June 30, 2026. It does not state a new share change. The same-window 10-Q and 424B3 recite reverse splits effective 2024-10-09 and 2025-12-01 only. |
| XHLD | 2026-08-06 | close over previous close | 8-K | 2026-08-10 | `0001493152-26-036864` | Same filing. |
| ADBT | 2026-09-03 | close over open | 8-K | 2026-09-02 | `0001493152-26-041157` | CFO Katharyn Field resigned for personal reasons. It does not state a share change. Same-window 424B3 filings are resale supplements and do not state one either. |
| ADBT | 2026-09-03 | close over previous close | 8-K | 2026-09-02 | `0001493152-26-041157` | Same filing. |
| VRRM | 2026-05-27 | close over previous close | 8-K | 2026-05-26 | `0001193125-26-239351` | Avis Budget terminated its contract. It does not state a share change. The May 20 annual-meeting 8-K and the June 1 CEO-change 8-K do not either. |
| MPLT | 2026-07-27 | close over previous close | 8-K | 2026-07-27 | `0001193125-26-316820` | ZEPHYR trial results. It does not state a share change. |
| SMJF | 2026-08-27 | close over open | 6-K | 2026-08-28 | `0001213900-26-094830` | No-news statement on unusual trading under NYSE American Company Guide Section 401(d). It does not state a share change. |
| SMJF | 2026-08-27 | close over previous close | 6-K | 2026-08-28 | `0001213900-26-094830` | Same filing. |

v3 halted on an unexplained open over the previous stored close, or close over open, for IWM and for every name that entered that day's candidate list or was already held. This study keeps that halt only for a name the book already holds, and for IWM. A candidate is removed instead, as the rule above says. The scan also records a close over the previous stored close. That third ratio does not add a halt by itself. Its first candidate session is the first session the name enters the candidate list while that bar is loaded, which is the session the locked leg on the same bar would have halted under v3. This commit holds no book, so nothing is already held. A leg whose name never enters on a session that loads the bar is not removed in this window. It stays in the table. A later run that is holding the name still halts. A real price move does not halt on any session and is not removed.

| ticker | bar date | leg | previous stored bar | first candidate session | why it stays unexplained |
| --- | --- | --- | --- | --- | --- |
| XHG | 2026-08-13 | open over previous close | 2026-08-12 | 2026-08-19 | No issuer filing on CIK 0001769256 from 2026-08-06 through 2026-08-20. The latest filing is the 6-K of 2026-07-16, accession `0001213900-26-078731`. Nasdaq's XHG press-release page listed no notice. |
| XHG | 2026-08-13 | close over previous close | 2026-08-12 | 2026-08-19 | Same gap. |
| JLHL | 2026-07-09 | close over open | 2026-07-09 | 2026-09-03 | No issuer filing on CIK 0002007846 from 2026-07-01 through 2026-07-16. The nearest are a 6-K filed 2026-06-16, accession `0001493152-26-028812`, and an F-3 filed 2026-07-24, accession `0001493152-26-034489`. Nasdaq's JLHL press-release page listed no notice. |
| JLHL | 2026-07-09 | close over previous close | 2026-07-08 | 2026-09-03 | Same gap. |
| SLBT | 2026-06-16 | open over previous close | 2026-06-15 | 2026-09-03 | Form 20-F filed 2026-06-18, accession `0001213900-26-070158`, states each exchanging share was cancelled for new ordinary shares at an exchange ratio of $5.568 billion divided by $10.00. That share change is not a verified split. |
| MFP | 2026-06-29 | open over previous close | 2026-06-26 | none in this window | Form 8-K filed 2026-06-22, accession `0001193125-26-276742`, states a spin-off distribution to Middleby holders of record at 4:00 p.m. Central Time on June 26, 2026. The July 6 8-K, accession `0001193125-26-295649`, states the spin-off was consummated. That distribution is not a verified split. |
| MFP | 2026-06-29 | close over previous close | 2026-06-26 | none in this window | Same distribution. |
| MB | 2026-08-07 | open over previous close | 2026-08-06 | none in this window | No issuer filing on CIK 0002027265 from 2026-07-31 through 2026-08-14. The latest filing is a 6-K filed 2026-05-19, accession `0001493152-26-024320`. Nasdaq's MB press-release page listed no notice. |

XHG enters the candidate list on 2026-08-19, so 2026-08-19 is the first session that removes XHG. It is not a halt. SION is on the 2026-08-13 list and is not removed, because the 8-K above is a real price move. MVIS is on that same list and is not removed, because the 8-K above verifies the split. This commit does not write a day file. The later run removes the candidate by the rule above. It does not drop a name by hand.

YMT's 2026-08-27 share factor `0.0625` is in the v1c splits file only. Market cap on the frozen rows stays under 100, so YMT is not in the candidate universe. AAC-U, HYAC-U, and PNAQ-U pass the liquid gate and have no bar in the price store (Yahoo chart HTTP 404) and no split row. They cannot enter a price-ranked list.

## 6. Finviz frozen record

Finviz fields come only from Theme Radar's frozen daily record.

Repo: https://github.com/SRoyaltyy/theme-radar

Head pinned for the panel files: `b5a324f8531af33c1f3879f64992f505d71fa18b`

Panel content commit: `3973e13cd953e5705d08d8d9f78a5b1b9dd1a1d0`

| file | sha256 | bytes | git blob |
| --- | --- | --- | --- |
| `research/lever_panel/finviz_panel_asof0930_2026-08.csv.gz` | `c8977b8eea8e74115899e9d4cc04d5b4ea67490376d972905781eb8e1aeb6459` | 55836833 | `762462c4a6d11ec7dea49fca3f9bfcc11e712393` |
| `research/lever_panel/finviz_panel_asof0930_2026-09.csv.gz` | `cbf35da9e1587703059abd9ff77525a1047c67a91edc3276ca93db4cd8669c16` | 71452629 | `1f2da45043200e0100c33e97989a9244e760bbf0` |

Rows are keyed by `trade_date`. On every row that is used, `snapshot_date` must be the previous NYSE session. Weekends are skipped. `2026-09-07` is skipped. Checked examples: trade `2026-08-13` uses snapshot `2026-08-12`; trade `2026-08-31` uses snapshot `2026-08-28`; trade `2026-09-08` uses snapshot `2026-09-04`. A row that fails the assert is not used, and it is not repaired.

Gaps are skipped, never filled.

`trade_date` `2026-08-28` has no row. The snapshot that morning would have been `2026-08-27`, and there is no `data/snapshots/2026-08-27.csv`. An archive run is not a dated snapshot. Nothing from `2026-08-26` or `2026-08-27` is copied forward. Every source that needs the frozen row is empty that day, including the Finviz earnings-reaction source.

Field families that this rebuild does not read, stated so a later edit cannot treat a gap as a fill:

- Trend fields `tr1d_*`, `tr1w_*`, `tr1m_*`, and `trf_*` are omitted on trade_date `2026-08-07` and are present from `2026-08-10`.
- Sector and segment fields `seg_*` are omitted on `2026-08-07`, `2026-08-10`, and `2026-08-11`, and are present from `2026-08-12`.
- Theme-change fields `trc_*` are absent before trade_date `2026-08-13` and start on `2026-08-13`.

`Earnings Date` is not a column of either pinned gzip. It is not borrowed from a fullscan export.

The liquid gate, from the frozen row for trade_date D, uses only `Ticker`, `Industry`, `Market Cap`, `Average Volume`, and `Volume`. `Industry` must not be `Exchange Traded Fund`. `Market Cap` is in millions of dollars and the floor is 100, the same floor as `MIN_MCAP_M`. `Average Volume` is in thousands of shares and the floor is 500, the same floor as `MIN_AVG_VOL_K`. `Volume` must be greater than 0. `Price` is the field a price floor would use. This gate does not apply one. Finviz `Change`, `Relative Volume`, performance, RSI, and SMA columns are not membership inputs and are not rank inputs.

## 7. Candidate list

The candidate list is the union of the sources below. A name keeps every source that produced it. Rank is by hot score across the union, not by source order. `oppset` is opt-in in the engine and is not part of this union.

| source | present when | fields |
| --- | --- | --- |
| `ohlc_hot` | frozen row exists | liquid gate, then `hot_score` from adjusted bars with `date < D`. Drop `too_extended`. Top 30 by hot score, ticker A to Z on ties. The panel call uses 30, not `HOT_TOP_N` 80. |
| `yday_gainer` | frozen row exists | liquid gate, then prior-session close-to-close return from adjusted bars (the session is `snapshot_date`). Top 25, ticker A to Z on ties. Not Finviz `Change`. |
| `yday_mover` | frozen row exists | same pool, ranked by the absolute value of that prior return. Top 20, ticker A to Z on ties. |
| `probable` | frozen row exists | liquid names ordered by that prior return, up to 60 considered. Keep `ok`, not `too_extended`, and `ret_5 <= 10`. Top 8. |
| `overnight` | never, on this pin | engine `overnight_scheduled`: liquid gate plus `Earnings Date` (AMC today with a time at or after 16:00, or BMO the next session). The column is absent, so the source is empty. No gap-rank substitute. |
| `overnight_mega` | never, on this pin | the same function with `Market Cap >= 50000`. The column is absent, so the source is empty. `OVERNIGHT_MEGA_MCAP_M` is 50000. |
| `earn_react` | never, on this pin | engine `earnings_reaction`: liquid gate plus `Earnings Date` (prior session AMC at or after 16:00 or date-only, or session BMO at or before 09:30 or date-only). The column is absent, so the source is empty on every session that has a row. It is also empty on `2026-08-28` because that trade_date has no row. |
| `flatten` | never, in this window | the book list. Used on D only if `INPUT_PROVENANCE_336.md` or `FULLSCAN_FILE_PROOF.csv` shows a copy on GitHub before that open. Both files mark it missing on all 31 sessions. The CSV has no `flatten` input. `stock_book` and `stock_suggestions` are not a substitute. |
| `mover_buy` | never, in this window | the same proof rule. Both files mark it missing on all 31 sessions. The CSV has no `mover_buy` input. The first 15 names of a mover file are not invented. |

On `2026-08-28` the present set is empty. New buys that morning: nobody. A lot already held still follows the list-drop rule at the open after `min_hold`.

On every other session the present sources are `ohlc_hot`, `probable`, `yday_gainer`, and `yday_mover`. The other five are absent.

| session | present |
| --- | --- |
| 2026-08-13 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-08-14 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-08-17 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-08-18 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-08-19 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-08-20 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-08-21 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-08-24 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-08-25 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-08-26 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-08-27 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-08-28 | none |
| 2026-08-31 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-09-01 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-09-02 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-09-03 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-09-04 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-09-08 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-09-09 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-09-10 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-09-11 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-09-14 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-09-15 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-09-16 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-09-17 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-09-18 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-09-21 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-09-22 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-09-23 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-09-24 | ohlc_hot, probable, yday_gainer, yday_mover |
| 2026-09-25 | ohlc_hot, probable, yday_gainer, yday_mover |

## 8. Holdup morning score

Holdup reads S on D only from a predict file or a weather file that the #354 proof marks proven and before the open. Predict wins when both are proven. The predict path is `01_daily/general/D_predict.md` and the score is that file's `Prediction` total. The weather path is `01_daily/weather/D_weather.json` and the score is `general_score`. The numeric score is not written in this commit.

Every one of the 31 sessions has such a file, so holdup is allowed to apply on every session. There is no session in this window where holdup is off for lack of a file. v4's note that 2026-08-27 had no score used earliest committer time. This study follows the #354 proof instead. The weather file is the source on four days, because predict is not proven before the open:

| session | source | status | server time UTC |
| --- | --- | --- | --- |
| 2026-08-13 | predict | PROVEN | 2026-08-13T12:10:30Z |
| 2026-08-14 | predict | PROVEN | 2026-08-14T12:09:44Z |
| 2026-08-17 | predict | PROVEN | 2026-08-17T11:51:24Z |
| 2026-08-18 | predict | PROVEN | 2026-08-18T11:53:13Z |
| 2026-08-19 | predict | PROVEN | 2026-08-19T11:52:33Z |
| 2026-08-20 | predict | PROVEN | 2026-08-20T11:55:52Z |
| 2026-08-21 | predict | PROVEN | 2026-08-21T11:54:18Z |
| 2026-08-24 | predict | PROVEN_BUT_CHANGED | 2026-08-24T11:27:45Z |
| 2026-08-25 | weather | PROVEN | 2026-08-25T11:16:03Z |
| 2026-08-26 | predict | PROVEN | 2026-08-26T11:25:52Z |
| 2026-08-27 | weather | PROVEN_BUT_CHANGED | 2026-08-27T06:29:13Z |
| 2026-08-28 | predict | PROVEN | 2026-08-28T08:16:51Z |
| 2026-08-31 | predict | PROVEN_BUT_CHANGED | 2026-08-31T11:09:45Z |
| 2026-09-01 | predict | PROVEN | 2026-09-01T13:13:03Z |
| 2026-09-02 | predict | PROVEN | 2026-09-02T13:29:24Z |
| 2026-09-03 | predict | PROVEN | 2026-09-03T11:25:21Z |
| 2026-09-04 | predict | PROVEN | 2026-09-04T12:21:59Z |
| 2026-09-08 | weather | PROVEN_BUT_CHANGED | 2026-09-08T12:34:01Z |
| 2026-09-09 | weather | PROVEN_BUT_CHANGED | 2026-09-09T13:04:53Z |
| 2026-09-10 | predict | PROVEN | 2026-09-10T10:32:14Z |
| 2026-09-11 | predict | PROVEN | 2026-09-11T09:12:05Z |
| 2026-09-14 | predict | PROVEN | 2026-09-14T08:27:06Z |
| 2026-09-15 | predict | PROVEN_BUT_CHANGED | 2026-09-15T08:32:39Z |
| 2026-09-16 | predict | PROVEN | 2026-09-16T08:32:10Z |
| 2026-09-17 | predict | PROVEN | 2026-09-17T08:38:40Z |
| 2026-09-18 | predict | PROVEN | 2026-09-18T08:37:27Z |
| 2026-09-21 | predict | PROVEN | 2026-09-21T08:40:31Z |
| 2026-09-22 | predict | PROVEN | 2026-09-22T08:39:43Z |
| 2026-09-23 | predict | PROVEN | 2026-09-23T08:39:57Z |
| 2026-09-24 | predict | PROVEN | 2026-09-24T10:08:33Z |
| 2026-09-25 | predict | PROVEN | 2026-09-25T08:44:07Z |

Each of those times is before 13:30 UTC on that session date.

## 9. Alarm

`src/factor_mine.py` `_attach_row` sets `alarm` from `bool(card.get("signal_alarm"))`. `src/ticker_lookback.py` `annotate_signal_improved` sets `signal_alarm` to `purely_worse(prev, boxes)`. `purely_worse` compares the twelve tones in `BOX_COLS`: `join`, `sector`, `gen`, `news`, `digest`, `judge`, `ab`, `peer`, `heat`, `vol`, `catal`, `buy`. Those tones are taken from the lookback packet (predict, news, digest, the stock book, and the other morning files). They are not a formula of the pinned Yahoo bars and the frozen Finviz numeric columns.

Alarm is not deterministic from the pinned prices and the frozen Finviz record. These two recipes do not read it. `forbid` is empty and `exit_when` is empty.

The nonews recipe `union_hot_n4_h1_nonews` is excluded. It forbids `alarm` and `news=bad`, and that news box is an AI field.

## 10. Report plan

This section is the contract for a later scoring commit. That commit has not been run. No figure from it belongs in this file. Where v4 published a number, the later table has a comparison column that copies the published value for the same recipe id from `research/factor_mine_recipe_search_v4/returns/RESULTS.json` and `research/factor_mine_recipe_search_v4/returns/REPORT.md`. Where v4 did not publish the statistic, the comparison cell is the text `not published`. The cell is not filled by recomputing v4.

The later table, for each variant, for the before window and again for the after window:

- Primary return: Futubull keep-held fees plus 0.5% per side slippage. Beside it, the same shares at 0% slip and at 1% slip, and the flat 15bp fee-schedule print. The slip lines are locked in rule 9. They are not tuned and they are not extra variants.
- The per-day count of names removed before ranking because a loaded bar contains a still-unexplained >3x leg. The count is the number of distinct tickers in that day's `excluded_unexplained_legs`, which is `n_excluded` in `EXCLUSIONS_PLAN.csv`. The cell is that count. It is not a return, a win rate, or a P&L.
- The same two returns with the best stock removed. The best stock is the ticker with the largest Futubull closed-trade P&L in that slice. A tie breaks A to Z. That ticker is ineligible on the rerun. The rerun does not alter the original ledger.
- The same two returns with the top 3 removed: the three largest Futubull closed-trade P&L tickers, same tie break, same rule about the original ledger.
- The best stock's share: that ticker's Futubull closed-trade P&L divided by the book's Futubull closed-trade P&L. If the book P&L is 0, the share is undefined and the cell says so.
- Win rate: winning closed trades divided by closed trades, on the primary path (Futubull fees plus 0.5% per side). A win is primary P&L greater than 0 on the sell date. Rule 11 puts a 95% Wilson interval on that rate. Skipped and unfilled orders are not in the numerator or the denominator. They are listed and counted beside the rate.
- Up days: the count of sessions that have a buy or a sell and whose Futubull day return is greater than 0. Also the winning-day share, because the reject rule uses it.
- Trades per session: closed trades divided by sessions in the slice, and the raw buy count beside it. A missing-input day stays in that session count. Rule 10.
- Positive-from-X-of-Y start days. Y is 3. The Monday starts inside `2026-08-17` through `2026-09-08` are `2026-08-17`, `2026-08-24`, and `2026-08-31`. `2026-09-07` is not a session. A start is positive when the Futubull keep-held sum of closed-trade P&L from that Monday through `2026-09-11` is greater than 0. The cell is X of 3. The after window is not a start.
- IWM buy-and-hold on the same sessions, after the entry fee, plus the flat 15bp print of that same path.
- RANDOM4. Seed `20260813`, 1000 draws, 4 names from that morning's candidate list. The draw uses the recipe's hold, list-drop sell, and holdup rule, with weather off. It does not use the recipe's filter or its hot-score sort.

The after window can only reject. It rejects a variant only when that slice has at least 30 closed trades and the joint is under 0.5. The joint is the minimum of the win rate and the winning-day share. Fewer than 30 closed trades cannot prove the variant and cannot reject it. A variant that is not rejected is not thereby proven. This study does not re-rank a grid and does not crown a winner from the before window.

## 11. Pins checked by the test

`SPLITS.md` sha256 `4c9d3b2547afbb74b7f95a7e7fdb0aab19e68c0d504bddcd93a94750eeac47a6`.

`JUMP_SCAN.csv` sha256 `52ec0d95f5295d0b5005de786bcb58519ec66ef7305639dd842279e15ea4155e`.

`REAL_MOVES.csv` sha256 `03e5c6704a6d3af65810080cf90e4017e784626e55ee4e397e0ceda87156c149`.

`EXCLUSIONS_PLAN.csv` sha256 `86de3c366759a0c06d20ea8af1d1e967978d7e6e8f927951f71990cba3c61ea7`.

Audit files at `4d964db7a8c975ff5151a859577a9d34d993e6ca`:

| file | sha256 |
| --- | --- |
| `research/audit/INPUT_PROVENANCE_336.md` | `cafff6a314723ed14b9d9cdf9b52658129981421dcd3a22e3a8945960fe68569` |
| `research/audit/FULLSCAN_FILE_PROOF.csv` | `7b60805ca3260f783313737f0e35c3d9010f50092a1c88fc500242c2d39a812a` |
| `research/audit/FULLSCAN_FILE_PROOF.md` | `8c81c4c507a7b09982d9ea08fb50cabaae6aa5039c25f21dd7c11d85727a3c1a` |
| `research/audit/REBUILD_MATCH_336.md` | `21f69d69095f3b8b5c5db92fd63327b8e25903ce9e7fb96867047f4affffc72a` |

Engine files, content sha256. A later run whose bytes differ stops.

| file | sha256 |
| --- | --- |
| `src/factor_mine.py` | `b51eed634fb5ce213e3a2e1f85c83749e97ee42519b33d1d60ad7ad0c0047860` |
| `src/factor_mine_book.py` | `1fc16961b2680ea7fa2b4f6939402176b27f20c38d0ceda85826d674f4ecb76a` |
| `src/ohlc_ripper.py` | `7b3d674b2f4b5f5e7c52331543b7e0abf24219c3a3d0f2a178476842783bf641` |
| `src/gainer_capture.py` | `ee03ea4d5cfad51dddcf3dc24b434b63fd19169ad0aacf657d505f5c5da82af5` |
| `src/gainer_asof.py` | `43b36e5a17b7ffb07ddbee6ecb7b047a03281513fd9670d1828abf427e27f51e` |
| `src/paper_trade.py` | `ebda9df4a672481d28b0464e7f5b50abf430034be63eb65bc582a27462417b4b` |
| `src/ticker_lookback.py` | `1e08f2f42c732407f834847f2e347b0218d8a18b2612a2ee565f24bcc6a2731e` |

## 12. Rule 9 — Fills, slippage, and the liquidity cap

Buys and sells fill at day D's actual 09:30 open from the pinned bars. The prior close is never a fill. It is the mark used only when rule 9 says a held lot cannot be sold.

Slippage is a fixed fraction of shares times that open, charged on top of the Futubull fee. The primary figure uses 0.5% per side (`0.005`). The same shares are also reported at 0% per side and at 1% per side (`0.01`). Those three rates are locked in this file. A later run does not pick a different rate, and the two sensitivity lines do not add to luck N.

Share count is chosen once, on the primary path. A buy's cash cost is `shares * open * (1 + 0.005)` plus `order_fees(shares, open, buy)`. A sell's cash proceeds are `shares * open * (1 - 0.005)` minus `order_fees(shares, open, sell)`. The fee function sees the actual open, not the slipped price. If the primary cost is above cash, shares are reduced until one share no longer fits, and that order is then skipped. The 0% line and the 1% line reprice those same shares. They do not resize, and they do not change who is held. The flat 15bp line also uses those shares, with 7.5bp per side on the actual open and with no slip.

The liquidity cap sizes a new buy. Excess budget stays cash. The cap does not trim a name that is already held. The dollar level is rule 14: 1% of Finviz `Average Volume` times Finviz `Price` on the frozen row for trade_date D, with `Average Volume` converted from thousands of shares. Whole shares are `min(shares from the cash budget, floor(cap / open))`. If that floor is under 1, or `Price` or `Average Volume` is missing or not positive, the buy is unfilled and counted under rule 11.

A stock with no open on D does not fill. A buy that day is an unfilled order. A held lot that cannot be sold stays held. It is marked at the last close, which is the most recent pinned close strictly before D. It is sold at the next later session that has an open, still subject to `lot_should_sell`. The missing open is not replaced by that close as a fill.

## 13. Rule 10 — Missing days and delisted names

A day whose required inputs are missing is a day in cash. It stays in the return series and in the day count. It is never dropped. Required inputs for a new buy are the pinned bars through D-1 and, for every source in section 7, the frozen row for `trade_date` D. `2026-08-28` has no frozen row, so that session takes no new buy. Cash that is not already in a held lot stays cash. A lot already held still follows rule 9 at the open. The session remains one of the 31. A missing morning-score file, if one occurred, would turn holdup off for that day and would not delete the day. This window has a score file every day.

The held-name halt in section 5 is not a missing-input day. A candidate with an unexplained 3x leg is removed and the day file is still written. A name the book already holds still refuses the day file. A real price move does not. Rule 10 does not waive that halt.

A delisted name that has pinned bars is included for every session where those bars exist. After its last bar, a held lot that cannot be sold stays marked at that last close through the end of the window when no later open exists. The sell waits for an open that this store does not have.

`research/longhist/tickers.json` has cutoff `2026-08-12`, the session before this window. It does not date a delisting inside the window. The list below is the candidate-universe evidence: a name that passes the liquid gate at least once, then has no frozen row on `2026-09-25`, and does not reappear on any earlier session after its last panel date. `bars` is whether `data/prices/ohlc.parquet` has a row for that ticker with a date in August or September 2026. `last bar` is that row's last date.

| ticker | last panel date | last bar | bars exist |
| --- | --- | --- | --- |
| BBBY | 2026-08-17 | 2026-08-14 | yes |
| EWAVU | 2026-08-17 | 2026-08-14 | yes |
| THEOU | 2026-08-17 | 2026-08-14 | yes |
| EQR | 2026-08-18 | 2026-08-14 | yes |
| CGCFU | 2026-08-24 | 2026-08-21 | yes |
| OSPRU | 2026-08-24 | 2026-08-21 | yes |
| AVB | 2026-08-31 | 2026-08-21 | yes |
| JAB | 2026-08-31 | 2026-08-21 | yes |
| TALK | 2026-08-31 | 2026-08-18 | yes |
| HLX | 2026-09-02 | 2026-08-21 | yes |
| AAC-U | 2026-09-08 | | no |
| FBRX | 2026-09-08 | 2026-08-21 | yes |
| JONEU | 2026-09-08 | 2026-08-21 | yes |
| LBRDK | 2026-09-08 | 2026-08-19 | yes |
| LEG | 2026-09-08 | 2026-08-21 | yes |
| NSAIU | 2026-09-08 | 2026-08-21 | yes |
| TWO | 2026-09-08 | 2026-08-21 | yes |
| WBS | 2026-09-08 | 2026-08-20 | yes |
| XTERU | 2026-09-08 | 2026-08-21 | yes |
| BCAR | 2026-09-14 | 2026-09-11 | yes |
| BRTMU | 2026-09-14 | 2026-09-08 | yes |
| CRNX | 2026-09-14 | 2026-09-02 | yes |
| OCLTU | 2026-09-14 | 2026-09-08 | yes |
| APGE | 2026-09-21 | 2026-09-02 | yes |
| CATLU | 2026-09-21 | 2026-09-08 | yes |
| DUKU | 2026-09-21 | 2026-09-08 | yes |
| MTAKU | 2026-09-21 | 2026-09-08 | yes |
| TLACU | 2026-09-21 | 2026-09-08 | yes |
| XIIIU | 2026-09-21 | 2026-09-08 | yes |
| BRR | 2026-09-22 | 2026-09-11 | yes |
| DOMO | 2026-09-24 | 2026-09-11 | yes |

HYAC-U and PNAQ-U pass the liquid gate, have no pinned bar, and are still on the frozen record on `2026-09-25`. They are not in the table. An order in either is unfilled. AAC-U is in the table and also has no bar.

## 14. Rule 11 — Wilson interval and unfilled orders

The win rate is reported with a 95% Wilson interval. With `n` closed trades, `k` wins, `p = k / n`, and `z = 1.96`:

`denom = 1 + z² / n`

`center = (p + z² / (2n)) / denom`

`half = z * sqrt(p * (1 - p) / n + z² / (4n²)) / denom`

The interval is `center - half` through `center + half`. `z = 1.96` is locked. If `n` is 0, the rate and the interval are undefined and the cell says so. This commit does not compute a rate.

Skipped and unfilled orders are listed separately and counted in the report. Each line has the session, the ticker, the side, and the reason. The reasons are: no open, cannot buy one share, missing Finviz `Price` or `Average Volume`, cap below one share, no bar left to fill, and a frozen-record ticker with no pinned bar on or before D (rule 13). The count is printed. Those orders are not closed trades, so they are not wins and they are not losses. The session stays in the day count.

## 15. Rule 12 — What v4 and the cap studies did

This table says which of rules 1–11 and 13–15 the earlier preregistrations followed. It copies no return. "Partial" means the study did some of the rule and not the rest. The weight caps in `concentration_cap_v1` through `concentration_cap_v3` are a share of book equity. They are not the 1% dollar-volume cap in rules 9 and 14. Rule 16 is a measurement in section 19, not a rule those studies wrote down.

| rule | v4 | cap v1 | cap v2 | cap v3 |
| --- | --- | --- | --- | --- |
| 1. Sequential day file and hash ledger | No. Score files append and earlier score files are not rewritten. There is no input-day sha256 ledger that refuses a changed day. | No. Same, and the forward ledger is not edited. | No. Same. | No. Same. |
| 2. Only copies proven before 13:30 UTC | No. The board is the earliest commit whose `panel.json` contains D. The #354 proof shows those copies were not proven before the open. | No. Names and S are the v4 stored rows. | No. Same v4 `INPUTS.json`. | No. Same. |
| 3. Pinned Yahoo print, Finviz price levels, jump check | Partial. Open is the fill. The tape is the cleaned v1c parquet, not `data/prices/ohlc.parquet`. The jump check is the same-date 3x test. Dollar tests do not use Finviz `Price`. | Partial. Same v1c tape and same-date jump check. | Partial. Same. | Partial. Same. |
| 4. Theme Radar frozen record, `snapshot_date`, gaps skipped | No. The input is `data/factor_mine/panel.json`. | No. The v4 stored board. | No. Same. | No. Same. |
| 5. Rebuild price sources; drop `flatten` and `mover_buy` unless proven | No. The stored board is used as saved. | No. Same board. | No. Same. | No. Same. |
| 6. Holdup S only from a file proven before the open | No. S is the earliest predict file, else the earliest weather file. v4 records 2026-08-27 as having no score. | No. The v4 stored S. | No. Same. | No. Same. |
| 7. Alarm identified and unused | No. The default forbid includes `alarm`. The nonews rows are in the grid. | No. The frozen bases keep the v4 gates. | No. Same. | No. Same. |
| 8. This report plan | Partial. Before and after, ex-best, win rate, up days, IWM, RANDOM4, and flat 15bp are in the v4 plan. Wilson, the slip lines, the dollar-volume cap, the skipped-order count, and positive-from-X-of-Y are not. | Partial. Adds ex top 1, top 3, and top 5, and a profit share. Not this plan. | Partial. Adds a 20% profit-share gate on the tune book. Not this plan. | Partial. Adds dependence, `1 - R_-1/R`. Not this plan. |
| 9. Open fill, 0.5% slip, 1% dollar-volume cap, missing open | Partial. Open fill yes. Futubull fees yes. No slippage. No dollar-volume cap. A missing open keeps the slot in cash and is not replaced by the close. A held lot is not marked at the last close and sold at the next open. | Partial. Open fill yes. Futubull yes. No slippage. No dollar-volume cap. A missing open keeps the slot. A weight-cap trim sells at the open. | Partial. Open fill yes. Futubull yes. No slippage. No dollar-volume cap. The prereg says open is the fill. It does not restate the next-open carry. | Partial. Same as v2. |
| 10. Missing day stays in cash and in the count; delists listed | No. | No. | No. | No. |
| 11. Wilson interval; unfilled orders counted | No. Win rate is a point. A one-share skip is described and is not a counted report line. | No. | No. | No. |
| 13. Universe is the frozen record for D; missing Yahoo bars listed | No. The board is the earliest factor-mine panel. Missing Yahoo bars are not listed per day. | No. The v4 stored board. | No. Same. | No. Same. |
| 14. Liquidity cap from Finviz `Average Volume` times `Price` | No. | No. The weight cap is a share of book equity. | No. Same. | No. Same. |
| 15. Gap through a stop fills at the open; stop before target | No. These recipes have no stop. The prereg does not lock the gap fill or stop-before-target. | No. The prereg says the recipes have no stop and no target. | No. The prereg does not restate a stop rule. | No. Same as v2. |

## 16. Rule 13 — Universe is the frozen record

Day D's candidate sources (`ohlc_hot`, `yday_gainer`, `yday_mover`, `probable`, `overnight`, `overnight_mega`) are computed only over tickers that have a row in the Theme Radar frozen record for `trade_date` D. Today's Yahoo ticker list is not the universe. Bars are read under the ticker string as it stood on D. A later rename is not substituted.

A day-D ticker is missing when the pinned `data/prices/ohlc.parquet` has no row for that exact ticker with `date` on or before D. The name is listed. It is not dropped quietly. `2026-08-28` has no frozen row, so that session has no ticker list and is not a line in the count. The list for the other 30 sessions is `research/hot_n4_clean_v4/MISSING_BARS.csv`, sha256 `75e2778152e04cb0c0e059aca0c7c7c2aa4cdaf1b512260c4c7f6c8afeeb82b0`. It has 1,214 ticker-days and 141 distinct tickers. `no_store` means the ticker is absent from the price file. `first_bar_after` means the first stored bar is after D.

| session | missing tickers | of which pass the liquid gate |
| --- | --- | --- |
| 2026-08-13 | 21 | 3 |
| 2026-08-14 | 20 | 3 |
| 2026-08-17 | 19 | 3 |
| 2026-08-18 | 19 | 3 |
| 2026-08-19 | 19 | 3 |
| 2026-08-20 | 19 | 3 |
| 2026-08-21 | 19 | 3 |
| 2026-08-24 | 20 | 2 |
| 2026-08-25 | 19 | 2 |
| 2026-08-26 | 18 | 2 |
| 2026-08-27 | 23 | 1 |
| 2026-08-31 | 21 | 1 |
| 2026-09-01 | 21 | 1 |
| 2026-09-02 | 20 | 1 |
| 2026-09-03 | 19 | 1 |
| 2026-09-04 | 18 | 0 |
| 2026-09-08 | 18 | 0 |
| 2026-09-09 | 14 | 0 |
| 2026-09-10 | 21 | 0 |
| 2026-09-11 | 38 | 0 |
| 2026-09-14 | 42 | 0 |
| 2026-09-15 | 48 | 0 |
| 2026-09-16 | 65 | 0 |
| 2026-09-17 | 72 | 0 |
| 2026-09-18 | 83 | 0 |
| 2026-09-21 | 86 | 1 |
| 2026-09-22 | 88 | 1 |
| 2026-09-23 | 94 | 1 |
| 2026-09-24 | 111 | 1 |
| 2026-09-25 | 119 | 1 |

A missing ticker that also passes the liquid gate is flagged `no_fill`. It was inside the set the price sources are computed over, and it cannot be ranked or filled. It does not take a top-4 slot, and it is not replaced by the next name. The flag is written on the day card. There are 37 such ticker-days, four tickers: AAC-U on the 10 sessions 2026-08-13 through 2026-08-26 (`no_store`), PNAQ-U on the 7 sessions 2026-08-13 through 2026-08-21 (`no_store`), NAT on the 15 sessions 2026-08-13 through 2026-09-03 (`first_bar_after`, first stored bar 2026-09-04), and HYAC-U on the 5 sessions 2026-09-21 through 2026-09-25 (`no_store`).

## 17. Rule 14 — Liquidity cap field

The cap base is the frozen row for trade_date D, as that row stood for that morning. Dollar average volume is Finviz `Price` times Finviz `Average Volume` times 1,000. `Average Volume` is thousands of shares: on the 2026-09-21 row, AAPL's `Average Volume` is 53,434.76 and its `Volume` is 86,561,467 shares. The 1,000 is that unit. It is not a tuned factor. The position notional at the open may not exceed 1% of that dollar average volume.

The record's `Open` column is not used. On every frozen row from 2026-09-01 through 2026-09-25, `Open` is empty. On trade_date 2026-09-28, which is after this window, `Open` is filled and is the snapshot day's open, not session D's open. Session D's open remains the pinned bar.

If `Price` or `Average Volume` is missing or not positive, the buy is unfilled under rule 11.

## 18. Rule 15 — Stops

`union_hot_n4_h1__w0` and `union_hot_n4_holdup__w0` have no stop and no target. `take_pct` and `stop_pct` stay null, so the stop branch does not fire for these two.

The general rule, for any later recipe that does set them, is the pinned `lot_should_sell` path. For a long, the stop level is the entry times one minus the stop fraction. If the open is at or below that level, the sell fills at that open, not at the stop. If the same daily bar touches both the stop and the target, the stop is taken first: the code checks the stop before the take, and a bar that touches both is `stop_first_same_bar`. A short uses the mirrored test. These two recipes do not enter that branch.

## 19. Rule 16 — Measured slippage beside the locked 0.5%

This is a measurement of paper fills. It is not a recipe score, and it does not replace the locked 0.5% per side. The 0.5%, the 0% line, and the 1% line stay as rule 9 locked them.

The committed paper and Webull fill prices for 2026-09-14 through 2026-09-25 live in `data/paper_open/`. The only file with an observed fill price is `data/paper_open/2026-09-21_status.json`, sha256 `0a88a810ec35a11dd94dc641edb8ef328c67b1e226c3697b7be50f89ec70050e`. `2026-09-21_submit.json` repeats the same sent rows and is not a second sample. There is no `data/paper_open` file for 2026-09-14, 2026-09-15, 2026-09-16, 2026-09-17, or 2026-09-18. The files for 2026-09-22 through 2026-09-25 are plans: ticket status `plan`, no `avg_fill_px`. VSTS on 2026-09-21 is `SUBMITTED` with `filled_qty` 0 and a null fill price, so it is not in the sample.

A counted fill has `filled_qty` above 0 and a non-null `avg_fill_px`. The gap is `avg_fill_px / official_open - 1`. The official open is the pinned bar's open that session. The price split is that open: under $3, or $3 and above. The median is the middle signed gap. The worst 10% is the mean adverse gap among the `ceil(0.1 * n)` most adverse fills in the cell. Adverse for a buy is the signed gap. Adverse for a sell is the signed gap multiplied by minus one. A cell with n = 0 is empty.

The host on that file is `api.sandbox.webull.com`, and the three fills are `FILLED_VIA_POSITION`. Paper fills are a lower bound on real slippage.

| cell | n | median gap | worst 10% |
| --- | --- | --- | --- |
| buys, open under $3 | 0 | | |
| buys, open $3 and above | 3 | +0.2407% | +0.5268% |
| sells, open under $3 | 0 | | |
| sells, open $3 and above | 0 | | |

The three buys, all with an official open at or above $3:

| ticker | side | fill | official open | gap |
| --- | --- | --- | --- | --- |
| DELL | BUY | 587.89 | 586.77001953125 | +0.1909% |
| UMC | BUY | 24.99 | 24.93000030517578 | +0.2407% |
| GME | BUY | 22.9 | 22.780000686645508 | +0.5268% |

With n = 3, `ceil(0.1 * 3)` is 1, so the worst 10% is GME's gap. The median is UMC's gap, the middle of the three.

