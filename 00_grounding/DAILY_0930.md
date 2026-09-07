# Four workflows — Daily, Generate, Backtest, Deploy

Operator map. You should not need any other Action for 09:30 BUY/SELL.

Future PRs that add a strategy or a morning/night step **must** update:

1. `src/run_generate.py` `STRATEGIES` (and a collector) if it prints tickets
2. `src/run_daily.py` if it needs a new precursor
3. This file, in one sentence

Do not add a fifth scheduled workflow.

Clocks are **America/New_York**. Skip-if-good:
`python3 -m src.skip_if_good --job daily|generate|stock_book_all|postclose_all --date DATE`

---

## The four

| Workflow | yml | What it is |
|---|---|---|
| **Daily** | `.github/workflows/daily.yml` → `src.run_daily` | Post-close, Elite scrape, morning ranker inputs, diagnostics |
| **Generate** | `.github/workflows/generate.yml` → `src.run_generate` | BUY/SELL for every registered strategy. Heals Daily if inputs are missing |
| **Backtest** | `.github/workflows/backtest.yml` → `src.run_backtest` | Refresh historical dashboards from books already on disk. No remine |
| **Deploy** | `.github/workflows/deploy-dashboard.yml` | Pages backup. Daily/Generate already call `scripts/publish_dashboard.sh` |

Live production tickets are **`flatten_robust`**. The stock book is the
suggestions list. Factor-mine keepers are research (no remine on this clock).

---

## Clock (Daily.yml)

| ET | Cron (UTC) | Job |
|---|---|---|
| 05:40 | `40 9 * * 1-5` | Elite scrape (ubuntu — ECS 403s Finviz) |
| 06:10 | `10 10 * * 1-5` | Generate (heal ranker + write tickets + Pages) |
| 09:15 | `15 13 * * 1-5` | Last-chance Generate |
| 09:20 | `20 13 * * 1-5` | Generate heal |
| 16:10 | `10 20 * * 1-5` | Post-close (ubuntu / DeepSeek) |
| 17:15 | `15 21 * * 1-5` | Post-close heal |
| 23:30 | `30 3 * * 2-6` | Post-close backup |

ECS systemd is unchanged: **05:55** `scripts/ecs_preopen.sh` (Grok essays,
then dispatches Generate) and **22:00** `scripts/ecs_map_postclose.sh`
(Grok night pack). NYSE holidays are skipped (`src/skip_if_good.is_nyse_holiday`).

---

## Generate — every strategy

Registered in `src/run_generate.py` `STRATEGIES`:

| id | Writer | Dashboard |
|---|---|---|
| `stock_book` | `src.stock_book` (healed by Daily) | `dashboard/` paper page |
| `flatten_robust` | `sleeve_merge --card --write-card` | `dashboard/sleeve-merge/` |
| `paper_io` | `src.paper_trade` | `dashboard/index.html` |
| `sleeve_combine` | `src.sleeve_combine_bt` | `dashboard/sleeve-combine/` |
| `factor_mine` | collect from `03_scoreboard/factor_mine.json` (no remine) | `dashboard/factor-mine/` |
| `excel` | collect today's rows from `excel_bot/suggestions/suggestions.csv` | confirm-only |
| `strategy_board` | `src.strategy_board --write` | `dashboard/strategy-board/` |

One board for the morning:
`https://sroyaltyy.github.io/fullscan/dashboard/generate/`
plus `01_daily/{D}_generate.md` and `data/generate/{D}_tickets.json`.

If scrape / weather / join / AB / book are missing, Generate runs
`run_daily.scrape` then `run_daily.morning --skip-llm` and then writes.

---

## Inputs → outputs

**Night (`run_daily --phase night`)**  
In: closed session D. Out: outcomes, reflects, learnings, next-session
`map_heat` captains. Does not rewrite the book.

**Scrape (`--phase scrape`)**  
Out: `{D}_finviz_digest.*`, `{D}_map_heat.json` overlay, `data/exports/finviz_{D}.csv`.

**Morning heal (`--phase morning --skip-llm`)**  
Out: weather, join, AB, membership, stock book + green.

**Generate**  
Out: flatten card, paper dashboard, combine dashboard, generate board, diag.

**Backtest**  
Replays paper / book-backtest / combine / sleeve-merge `--write` (sweep
dashboard only — `LIVE_POLICY` stays `flatten_robust`) / strategy board.

---

## Retired clocks (do not click for 09:30)

Schedules commented out on: `postclose_all.yml`, `postclose_last_closed.yml`,
`finviz_preopen_scrape.yml`, `stock_book_all.yml`, `sleeve_merge_live.yml`,
`daily_orchestrator.yml`, `stock_book_diag.yml`.

`preopen_all.yml` stays dispatch-only + ECS 05:55 essays.
`excel_bot.yml` and `finviz_all.yml` keep their own (non-09:30) crons.

---

## If everything is on fire

1. Actions → **Daily** → phase `scrape` (if Elite CSV missing).
2. Actions → **Generate** (heals the ranker, writes every ticket, publishes Pages).
3. Confirm `dashboard/generate/` and `dashboard/sleeve-merge/` on Pages.

Do not `--force` a pack skip-if-good already accepts.
Do not remine factor-mine from this clock.
Do not change `LIVE_POLICY`.
