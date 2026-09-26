# Morning-panel input inventory

Inventory only. Nothing here is a backfill, a new score, or a rewritten day.

Counted on the tree at `c98b26613` (main, 2026-09-26). The morning the picks read is `data/factor_mine/panel.json` (31 sessions, 2026-08-13 through 2026-09-25, 2,559 candidate rows). HOT4 (`union_hot_n4_h1`) calls `pick_day` on that session's rows. Research sleeves call `pick_morning` on those rows plus the Theme Radar Clock-B aisle. Excel sleeves read `excel_bot/suggestions/suggestions.csv`. Hard-red is the morning S gate (`S <= -3`), not a separate file.

A score that an LLM would have to write again today is **NOT rebuildable**. A number that is only prior prices, or a filing that already existed that morning, is rebuildable. "Trading days" below is the count of distinct `YYYY-MM-DD` files in the repo, not a filled calendar.

## Universe per day

Two sizes. **Panel** is the candidate list a recipe can pick from (`panel.json` rows that morning). **Join** is the ranked stock-book universe (`data/join/<date>_ranked.csv`). **Book** is `data/stock_book/<date>_stock_book.csv`, the card the cameras are painted from. Finviz exports that exist sit near 11,600 names; they are the screen, not the pick list.

The frozen snapshot `n_rows` is a different object. On many mornings the lock file has `n_rows=0` and the names live under `dropped`. Picks do not read that empty row list. They read the panel.

| date | panel | join | book |
| --- | ---: | ---: | ---: |
| 2026-08-13 | 9 | 11579 | 11579 |
| 2026-08-14 | 111 | 11586 | 11586 |
| 2026-08-17 | 86 | 11573 | 2697 |
| 2026-08-18 | 117 | 11584 | 2698 |
| 2026-08-19 | 118 | 11600 | 2702 |
| 2026-08-20 | 92 | 5908 | 2707 |
| 2026-08-21 | 81 | 5909 | 2707 |
| 2026-08-24 | 78 | 5904 | — |
| 2026-08-25 | 112 | 5907 | — |
| 2026-08-26 | 108 | 5911 | — |
| 2026-08-27 | 113 | 5916 | 2698 |
| 2026-08-28 | 98 | 5916 | — |
| 2026-08-31 | 72 | 5910 | 2685 |
| 2026-09-01 | 91 | 5913 | 2683 |
| 2026-09-02 | 103 | 5915 | 2683 |
| 2026-09-03 | 103 | 5915 | 2095 |
| 2026-09-04 | 77 | 5918 | 2091 |
| 2026-09-08 | 92 | 5902 | 2065 |
| 2026-09-09 | 90 | 5902 | 2065 |
| 2026-09-10 | 82 | 5902 | 2065 |
| 2026-09-11 | 74 | 5904 | 2059 |
| 2026-09-14 | 62 | 5895 | 2057 |
| 2026-09-15 | 64 | 5897 | 2089 |
| 2026-09-16 | 65 | 5900 | 2062 |
| 2026-09-17 | 60 | 5902 | 2096 |
| 2026-09-18 | 55 | 5902 | 2087 |
| 2026-09-21 | 62 | 5894 | 2069 |
| 2026-09-22 | 72 | 5895 | 2066 |
| 2026-09-23 | 76 | 5897 | 2066 |
| 2026-09-24 | 74 | 5898 | 2059 |
| 2026-09-25 | 62 | 5898 | 2073 |

31 trading days. Panel size runs from 9 (the first session, almost empty) to 118. Join is ~11,600 names through 2026-08-19 and ~5,900 from 2026-08-20. The book card is the full join on 08-13 and 08-14, about 2,700 names from 08-17, and about 2,100 from 09-03. Four panel mornings have no stock-book CSV: 08-24, 08-25, 08-26, 08-28.

Candidate sources on a panel row (`sources`): `flatten`, `probable`, `yday_gainer`, `yday_mover`, `ohlc_hot`, `earn_react`, `overnight`, `overnight_mega`, `mover_buy`. Oppset is stamped on the research aisle; it is not a HOT4 source.

## How to read rebuildable

- **NOT rebuildable** — the number is a model judgment (Grok, sector essay, news judge, general predict, or any mix that includes one). Calling the model again today for a blank morning is a hindsight leak. The file already in the repo stays the record for the date it carries.
- **Rebuildable** — prior OHLCV, or a filing/print that was public that morning. Stated with how far the tape goes.
- The factor-mine price store (`data/prices/meta.json`) is raw Yahoo, `auto_adjust` false: first bar 2024-03-04, last bar 2026-09-25, 11,712 names, 3,065,066 rows. Indicator features need a warmup after that first bar (about 20 sessions for the hot score, `slow+signal` bars for MACD).

## Scores and gates

| Input | What the morning uses | Source | Earliest file | Trading days | Before 2026-08-13 | Lookahead |
| --- | --- | --- | --- | --- | --- | --- |
| AB score (`s_ab`, camera `ab`) | Polarity of the enriched checklist on the stock-book card. Gates `ab_g`. | `data/ab_checklist/<date>_ab_checklist_enriched.csv` via `src/stock_book.py`. Raw checklist is `src/ab_checklist.py` (OHLC rules A1–A15 plus `B01_eps_surprise`). Enrichment is `src/ab_enrich.py` (peer week, industry, sector board). | Raw 2026-08-18. Enriched 2026-08-19. | Raw 31. Enriched 32. | No file before 08-13. | Enriched score is **NOT rebuildable**. P04 reads the sector LLM board. Re-scoring that board today is a leak. The price-only A rules are rebuildable from the price store back to 2024-03-04 plus a few sessions of warmup. `B01` needs the earnings print that existed that morning; the Finviz export that carries it is not a continuous history (see below). Inside the 31 sessions, raw AB is missing 08-13, 08-14, 08-17, 08-26, 09-18, 09-25. Enriched is missing 08-13 through 08-18 and 08-26. The 08-18 raw file has 1 data row. |
| S score | `morning_s`: regime `predict_score`, else the general-predict "total score", else weather `general_score`. | `01_daily/general/<date>_predict.md`, then `01_daily/weather/<date>_weather.json`. | Predict 2026-07-31. Weather 2026-08-12. | Predict 40. Weather 38. | Predict has 9 files before 08-13, first 2026-07-31. Weather has 2026-08-12 only. | **NOT rebuildable.** The total score is the LLM factor card. Weather copies that score into `signals.general_score`. Re-writing the essay today does not recreate the morning. Predict is missing on panel day 2026-08-27; weather exists all 31 panel days. |
| Hard-red | New buys sit when morning S is less than or equal to −3. Same threshold on the book and the Webull wire. | Derived from S. Constant `HARD_RED = -3` in `src/factor_mine_book.py`. | Same dates as S. | Same as S. | Same as S. | No independent series. It is as rebuildable as S, which is **NOT rebuildable**. |
| Sector | Camera `sector` is the sign of `s_sector`. | `01_daily/sectors/<date>/*_predict.md` and `_board.json`, read by the stock book. | 2026-08-08 (37 day folders). | 37. | Folders exist from 2026-08-08, so a few sessions before 08-13 are already files. | **NOT rebuildable.** These are sector LLM essays. A missing essay is a zero in the book, not an invitation to write one later. Panel mornings are not all 11-for-11: 08-19 and 08-20 have a board and zero predict files; several other days have 8–10 of 11. |
| Regime / weather stance | Join multiplies labels by the day's stance. Also the fallback S. | `src/weather.py` from the general predict, channel 1, and the event file. Rules in `00_grounding/weather_rules.json`. | 2026-08-12. | 38. | One file, 2026-08-12. | The stance that includes the general LLM score is **NOT rebuildable**. Channel 1's FRED and VIX prints are a separate row below. |
| General predict | `s_general` on the card, times beta. Feeds S and weather. | `01_daily/general/<date>_predict.md`. | 2026-07-31. | 40. | 9 files, 2026-07-31 through the sessions before 08-13. | **NOT rebuildable.** |
| Grok review | Pre-open auditor. It does not enter `rank_key` or the HOT4 list. A failed review marks the packet, it does not add a ticker score. | `01_daily/<date>_grok_review.json` from `src/grok_review.py` (one Grok read of that morning's files). | 2026-08-25. | 22. | None. | **NOT rebuildable.** A new Grok read of the same files today is a new judgment. Missing on the first nine panel sessions through 08-24, and on 08-27. |
| News actions | Camera `news` / `s_news`. | `01_daily/news/<date>_actions.json`. The action pass can abstain when Grok news is stale (`src/news_actions.py`). | 2026-08-09. | 37. | 4 files, from 2026-08-09. | **NOT rebuildable** as a score. Present on all 31 panel days. |
| News judge | Ticker tilt on the news camera (`judge` box). | `01_daily/news/<date>_judge.json` (md from 2026-08-19). | 2026-07-31. | 46 json. | 9 files from 2026-07-31. | **NOT rebuildable.** Present on all 31 panel days. |
| Finviz digest | Company-headline piece of `s_news`. | `01_daily/news/<date>_finviz_digest.json`. | 2026-08-20. | 31. | None. | The digest is an LLM read of headlines. **NOT rebuildable.** Missing 08-13 through 08-19. A headline that is already sitting in a committed Finviz export can be re-read as text; the tilt written on top of it cannot be regenerated. |
| News parse | Upstream of actions. | `01_daily/news/<date>_parsed.json`. | 2026-08-08. | 35. | Files from 2026-08-08. | **NOT rebuildable** where the parse is a model pass. |
| Map heat / captain | Camera `heat`. Research essay wins; otherwise the Finviz industry residual on `*_map_heat.json`. | `01_daily/map_heat/<date>_research.json` and `<date>_map_heat.json`. | Both 2026-08-26. Heat json also has a file dated 2026-09-28, after the last panel session. | Heat json 27. Research 19. | None. | Captain essays are Grok. **NOT rebuildable.** The Finviz residual table is a vendor tape of that morning: rebuildable only for dates an export exists, not by asking a model what the map "would have" said. Heat json is missing on the panel through 08-25. Research is also missing 08-27, 08-31, 09-02. |
| Catalyst dossiers | Camera `catal`. The stock-book ranker marks catalyst as a separate chart workflow; the lookback card still paints the box when the file exists. | `01_daily/catalyst/<date>_dossiers.json`. | 2026-08-26. | 22. | None. | Dossier text is a model write-up. **NOT rebuildable.** Missing most of August on the panel, and 09-08, 09-09. |
| Events | Sector-tilt overlay and a weather input when `scan_date` matches. | `01_daily/events/<date>_events.json`. | 2026-08-10. | 33. | 3 files, 08-10 through 08-12. | The scan write-up is a model pass. **NOT rebuildable.** Missing 08-13 through 08-19 on the panel. |

## Price, filings, and vendor tapes

| Input | What the morning uses | Source | Earliest file | Trading days | Before 2026-08-13 | Lookahead |
| --- | --- | --- | --- | --- | --- | --- |
| OHLC hot score, ret_1 / ret_5 / ret_10, rvol, NR7, break-10, last green/red | Rank key for HOT4 (`hot_score`) and the `ohlc_hot` / `probable` lists. Features are as of the morning, on bars through the prior close. | `src/ohlc_ripper.py` on `data/prices`. Hot score is `0.08*ret_5 + 0.04*ret_10 + 0.4*min(rvol,3) + 1.2*break_10 + 0.3*last_green`. | Price store 2024-03-04. | The store is a bar panel, not one file per session. Panel uses it on all 31 mornings. | Rebuildable. | Rebuildable from the committed raw tape back to 2024-03-04. A 20-session feature is trustworthy a month after that first bar. Same-day change, gap, and relvol are not inputs. |
| Candle score, RSI, MACD, close location, flow | Row fields `candle_*`, `rsi`, `macd*`, `close_loc`, `flow_in`. | Prior OHLC in the same price store. | 2024-03-04. | Same store. | Rebuildable. | Rebuildable. MACD needs the slow-plus-signal warmup after the first bar. |
| Excel signals | `excel_strats` keeps suggestions whose `signal_date` equals the session date. Columns used: ticker, side, strategy, signal_date. | `excel_bot/suggestions/suggestions.csv`. Daily note `excel_bot/daily/<date>_excel_bot.md`. Engine is `excel_bot/` — replica of the STOCKHISTORY workbook, no LLM. | Signal dates 2026-07-23 through 2026-09-25 (33 dates, 2,698 rows). Daily notes from 2026-08-30 (21 files). | 33 signal dates. | Yes. Signals already exist from 2026-07-23, before 08-13. | Rebuildable from Yahoo daily OHLCV. This engine follows the STOCKHISTORY convention (split-adjusted, not dividend-adjusted), which is a different tape from the factor-mine `auto_adjust=False` store. Weekly warmup in `excel_bot/engine/backfill.py` is 600 days, so a full color grid needs about 600 calendar days of history before the day you score. `current_price`, `ret_vs_close`, and `ret_vs_open` on the CSV are later tracking marks. They are not the morning input. The job that writes a signal runs after the US close; the workbook's own fill is the next open (`first_open`). |
| Finviz export | Prior-session export only. Labels, liquidity, `fv_rsi`, `fv_sma20`, `fv_sma50`, `fv_rvol`, `fv_inst`, and the vol camera. | `data/exports/finviz_<date>.csv`. | 2026-04-26. | 39. | One file, 2026-04-26. Not a continuous pre-08-13 tape. | The committed CSV is the morning's vendor tape. RSI and moving averages can be recomputed from the price store back to 2024-03-04. Institutional ownership and the rest of the Finviz fundamental columns cannot. Panel day 2026-08-26 has no export. |
| Membership | Join labels (sector, cap, earnings/range bins). | `data/universe/<date>_membership.csv`, from that day's Finviz via `src/segments.py`. | 2026-04-26. | 39. | Same single 2026-04-26 file as Finviz. | Rebuildable only where an export of that morning exists. Price-only bins (cap, average volume) can be approximated from the price store plus a share count; the Finviz fundamental bins cannot. |
| Join score (`s_join`, camera `join`) | Ranked universe the book starts from. | `data/join/<date>_ranked.csv` from membership × weather (`src/join.py`). | 2026-08-12. | 38. | One file, 2026-08-12. | The label piece follows membership. The weather multiplier follows the LLM general score, so the joined rank is **NOT rebuildable** for a day whose predict you would have to write now. All 31 panel days have a join file. |
| Peer RS | Camera `peer`, and AB enrichment P01–P03. `rs_week` is also copied onto the panel row. | `data/peers/<date>_peer_rs.csv`. | 2026-08-18. | 30. | None. | Week/month relative strength is price math. Rebuildable from the price store back to 2024-03-04 plus a week of bars, for names in the store. The committed peer file itself starts 2026-08-18. Missing on the panel: 08-13, 08-14, 08-17, 08-24, 08-25, 08-26, 08-28. |
| Channel 1 | VIX regime, Fear & Greed, FRED yields. Weather reads it. Not a ticker score. | `01_daily/_channel1/<date>_predict.json` via `src/fetch_channel1.py` (FRED, yfinance, and a last-24h news table). | 2026-07-31. | 46. | 9 files from 2026-07-31. | FRED series and VIX are public and rebuildable for years before 08-13 (FRED daily history; Yahoo `^VIX`). The last-24h news slice inside the same JSON is a scrape of that morning. **NOT rebuildable.** Present on all 31 panel days. |
| Earnings reaction / ERD flags | `earn_react` names, `erd_days_since_E/R/D`, `erd_flag_E/R`. | Prior Finviz export events (`src` factor-mine `asof_snapshot`). | Follows Finviz exports, 2026-04-26. | 39 exports. | The single 2026-04-26 export. | A filing date and an EPS print that were already public are rebuildable from the filing. The flag as stored is whatever that export contained. There is no separate earnings archive in this repo before the exports. |
| Insider / form-4 keyword | `ins_buy` from the prior headline; `form4_buy` on the row. | Keyword on the prior news title. The stock book itself records no daily insider file. | Follows news parse, 2026-08-08. | 35 parse files. | Parse files from 2026-08-08. | The keyword on a headline that is already in the repo can be re-applied. A new model read of "was this insider buying" is **NOT rebuildable**. |
| Ticker checklist | Rebound tag on the book. Not a pick rank by itself. | `data/checklist/<date>_checklist.csv` (history parquet beside it). | 2026-08-14. | 16. | None. | Checklist score is a tape rule. Rebuildable from prices where the rule is the committed one, back to 2024-03-04. Only 16 dated files exist, so most panel mornings have no dated checklist. |
| Quote colors | Lookback index input. Not the Excel color engine. | `data/quote_colors/<date>_quote_colors_detail.json`. | 2026-08-18. | 28. | None. | These are stored snapshots of a quote paint. Rebuilding the paint from prices is possible only for the rule that produced them; the file history starts 2026-08-18. |

## Theme Radar fields

Research aisle only (`src/morning_scan.py`, `src/oppset_clock_b.py`). HOT4 `pick_day` does not union this list.

Vendored file: `data/theme_radar/oppset_clock_b/oppset_flagged.csv` (7,370 rows). Upstream theme-radar commit `a782cc2b`, clock `join_morning` = the morning T, `finviz_asof` = T−1. Same-day gap, relvol, and change on snapshot T are not features.

Stamped onto a panel row: `oppset`, `opp_any`, `opp_rvol`, `opp_gap_pct`, `opp_change_pct`, `opp_finviz_asof`.

Columns on the CSV, all T−1: `join_morning`, `finviz_asof`, `ticker`, `sector`, `price`, `avg_vol`, `mcap`, `rvol`, `change_pct`, `chg_open_pct`, `gap_pct`, `ah_change_pct`, `pweek`, and the `flag_*` bits (`rvol`, absolute change, gap, after-hours change, prior week) plus `any_opp`.

| | |
| --- | --- |
| Earliest `join_morning` | 2026-08-14 |
| Latest | 2026-09-18 |
| Trading days | 24 |
| Before 08-13 | None |
| Missing inside the panel | 2026-08-13, 2026-08-27, and every session after 2026-09-18 (09-21 through 09-25) |

Counts on the days that exist: 392, 247, 244, 390, 453, 300, 270, 265, 318, 332, 282, 199, 315, 237, 244, 220, 359, 284, 332, 215, 518, 242, 261, 451 (08-14 through 09-18, skipping 08-27).

Those T−1 Finviz fields are rebuildable for mornings where the prior export is in `data/exports`. They are not rebuildable for a blank day by re-deriving them from a model. Theme Radar's own snapshot open (used by the factor-mine lock as a price cross-check, `src/factor_mine_freeze.py`) is a print, not a score. Those snapshot bodies are not vendored here as a daily history.

Clock-B gates on the research pick (`clk_mom_break_peer`, `clk_fresh_cat_coil`, `clk_hold_vs_sector`, `clk_nr7_mom`, `clk_neg_weak_fail`, `clk_ext_veto`, and the other `clk_*` stamps) are booleans computed from the row. They go back only as far as the cameras and OHLC fields they read. Any gate that needs `sector`, `ab`, `news`, or `heat` is **NOT rebuildable** on a day those LLM files are absent.

## What a pre-08-13 morning can actually be made of

From files already in the repo, before 2026-08-13:

- Price features (hot score, returns, rvol, NR7, break-10, candle, RSI, MACD): back to 2024-03-04, plus indicator warmup.
- Excel color signals: already present from 2026-07-23, and recomputable from Yahoo with a 600-day weekly warmup.
- Channel 1 FRED/VIX: files from 2026-07-31; the public series go back much further.
- One Finviz export and one membership file on 2026-04-26. Not a calendar.
- LLM packet that already exists and must not be regenerated: general predict and news judge from 2026-07-31 (9 sessions), news actions from 2026-08-09, events from 2026-08-10, sector folders from 2026-08-08, weather and join on 2026-08-12.

Absent before 08-13, and **NOT rebuildable** by running a model now: AB enriched score, peer file, Finviz digest, map-heat captain, catalyst dossiers, Grok review, and a continuous Finviz/join history.
