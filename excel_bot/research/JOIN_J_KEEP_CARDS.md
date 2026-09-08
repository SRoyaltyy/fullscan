# Research cards — Excel J × fullscan join (post-8-13)

_Generated 2026-09-08 · tip `f41d73a5` / `JOIN_POST_813.md` · **research cards only** · live frozen._

## Plain English

J clock leak **PASS**. Prove (weekday 2026-08-26→2026-09-07) does **not** re-clear the ship bar. `avoid_J_ge0` n=64 H +0.22% (+0.04 pp vs same-window top-8, ghost PASS/FAIL/FAIL). `elev_cap2_J_le-1` n=64 H +0.33% (+0.15 pp, ghost PASS/FAIL/FAIL). Discovery half still prints (`avoid_J_ge0` +0.72 pp n=64) — that is the peek, not prove. Pooled weekday leftover is `avoid_J_ge0` +0.38 pp n=128 / `elev_cap2_J_le-1` +0.26 pp n=128 (includes discovery; not a holdout). Wider book (top-80) prove `avoid_J_ge0` +0.12 pp n=640. Join dumps do not add sessions before 8-13 with a fresh J (08-13 prior Open is 04-26). Family is **CONDITIONAL**: not KEEP holds, not a full KILL of the discovery print. Live flatten_robust stays frozen. Do not wire. Expanded prove: the J overlay does **not** hold as a general rule. Yahoo liquid names (prior-session volume ≥ 1M, 2024–2026) are about flat (+2 to +5 bp, ghost month fail). The all-name Yahoo tape’s large mean is a microcap lottery (ghost FAIL). Join-full / membership on the Finviz window is +5 to +13 bp (under 20 bp). membership_liq is flat to negative. Join top-8 stays **CONDITIONAL** (discovery only). Dashboard sleeves (open J only): 0 KEEP / 68 KILL / 83 CONDITIONAL / 88 null of 239 scored rows. Question is after-fee H vs that sleeve alone. Live flatten_robust stays frozen. flatten_robust: **KILL** — pooled avoid -0.59 pp n=11; elev +0.19 pp n=23. live tickets; io often 16:00 / 3d. Blanket J-avoid fights mover gap-up. flatten_h5: **KILL** — pooled avoid -1.26 pp n=10; elev -0.63 pp n=19. factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for context, overlay is on fills (cash book) flatten_live_h5: **null** — pooled avoid -2.09 pp n=2; elev -1.61 pp n=9. factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for context, overlay is on fills (cash book) green_pile_prior: **KILL** — prove avoid +0.20 pp n=50; elev +0.14 pp n=61. prior-day green live_buy with in_pile=true (PIT). green_book_prior: **KILL** — prove avoid -0.39 pp n=61; elev -0.37 pp n=64. prior-day green.json live_buy (PIT). Afternoon stamp is not a same-day open feature. weighted_book_1d_prior: **CONDITIONAL** — prove avoid +0.49 pp n=37; elev +0.06 pp n=64. prior-day stock_book 1d buy (weighted). Same clock as book_1d_prior universe. unweighted_book_prior: **KILL** — prove avoid -0.39 pp n=61; elev -0.37 pp n=64. prior-day unweighted.json live_buy (PIT).

Clock: **J value is open** (today Open vs prior weekday session Open). Never same-row H/I or H-fill paint. Live `flatten_robust` is not changed.

## J clock / leak

**Verdict: PASS.** The J number used by both recipes is open-knowable at that day’s 9:30 open.

| check | result |
|---|---|
| Excel map | `CLOCK_MAP.md`: J value-open = C[t] vs C[t−1]. C = Open / IT. In `value_mine_open` (44). |
| Dump formula | `J = (Finviz Open[t] − Finviz Open[prior weekday]) / prior Open` |
| Same-row H/I | labels only — never features. J ≠ H and J ≠ I on checked name-days. |
| M number / H paint / `core_score` | not used |
| High / Low / Close / Price | loaded as labels / reconstruction; **not** in J |
| File clock | Finviz CSVs are EOD; Open column is still the 09:30 print |
| Weekend / Sunday | Sat/Sun Finviz Opens skipped; Sunday join dumps held out |
| Stale | 08-13 vs 04-26 Open unused. 08-26 missing Finviz → 08-27 J uses 08-25 Open (hole, not future). |

pick_book reads only J flags + join rank. See `JOIN_POST_813.md` leak section.

## Universes (same open J, beat same-universe baseline)

| universe | n (holdout) | avoid vs | ghost | family |
|---|---:|---:|---|---|
| `join_top8` | 64 | +0.04 pp | PASS/FAIL/FAIL | **CONDITIONAL** |
| `join_top15` | 53 | -0.01 pp | PASS/FAIL/FAIL | **CONDITIONAL** |
| `join_top80` | 640 | +0.12 pp | PASS/PASS/PASS | **DEMOTE** |
| `join_full` | 24822 | +0.13 pp | PASS/PASS/PASS | **DEMOTE** |
| `membership` | 24822 | +0.13 pp | PASS/PASS/PASS | **DEMOTE** |
| `membership_liq` | 4591 | +0.00 pp | PASS/FAIL/PASS | **DEMOTE** |
| `book_1d_prior` | 43 | +0.54 pp | FAIL/FAIL/FAIL | **DEMOTE** |
| `book_3d_prior` | 49 | +0.14 pp | PASS/FAIL/FAIL | **DEMOTE** |
| `ohlc_all` | 771148 | +0.64 pp | FAIL/FAIL/FAIL | **DEMOTE** |
| `ohlc_liq` | 174214 | +0.02 pp | PASS/FAIL/PASS | **DEMOTE** |
| `mem_20260426` | 750330 | +0.66 pp | FAIL/FAIL/FAIL | **DEMOTE** |

Yahoo OHLC cannot invent join ranks before 2026-08-12. Prices parquet ends 2026-08-21. No earlier join+J holdout exists. Universe family stays as scored — this sleeve add-on does not drop that work.

## Dashboard sleeves (J vs sleeve-alone)

Dashboard sleeves (open J only): 0 KEEP / 68 KILL / 83 CONDITIONAL / 88 null of 239 scored rows. Question is after-fee H vs that sleeve alone. Live flatten_robust stays frozen. flatten_robust: **KILL** — pooled avoid -0.59 pp n=11; elev +0.19 pp n=23. live tickets; io often 16:00 / 3d. Blanket J-avoid fights mover gap-up. flatten_h5: **KILL** — pooled avoid -1.26 pp n=10; elev -0.63 pp n=19. factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for context, overlay is on fills (cash book) flatten_live_h5: **null** — pooled avoid -2.09 pp n=2; elev -1.61 pp n=9. factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for context, overlay is on fills (cash book) green_pile_prior: **KILL** — prove avoid +0.20 pp n=50; elev +0.14 pp n=61. prior-day green live_buy with in_pile=true (PIT). green_book_prior: **KILL** — prove avoid -0.39 pp n=61; elev -0.37 pp n=64. prior-day green.json live_buy (PIT). Afternoon stamp is not a same-day open feature. weighted_book_1d_prior: **CONDITIONAL** — prove avoid +0.49 pp n=37; elev +0.06 pp n=64. prior-day stock_book 1d buy (weighted). Same clock as book_1d_prior universe. unweighted_book_prior: **KILL** — prove avoid -0.39 pp n=61; elev -0.37 pp n=64. prior-day unweighted.json live_buy (PIT).

KEEP = prove window ≥20 bp after fees vs that sleeve’s own morning picks, ghost pass, H>0. CONDITIONAL = edge only in pooled/discovery, ghost fail, or clock/sleeve-P&L disagreement. KILL = no 20 bp edge. null = n<15, shorts, excel/`strategies/` frozen, or no fills.

### Featured

| sleeve | family | verdict | n | avoid vs sleeve | elev vs sleeve | ghost | note |
|---|---|---|---:|---:|---:|---|---|
| `flatten_robust` | sleeve merge | **KILL** | 11 | -0.59 pp | +0.19 pp | FAIL/PASS/FAIL | live tickets; io often 16:00 / 3d. Blanket J-avoid fights mover gap-up. |
| `flatten_h5` | factor mine | **KILL** | 10 | -1.26 pp | -0.63 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_live_h5` | factor mine | **null** | 2 | -2.09 pp | -1.61 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `green_book_prior` | stock book | **KILL** | 61 | -0.39 pp | -0.37 pp | PASS/PASS/PASS | prior-day green.json live_buy (PIT). Afternoon stamp is not a same-day open feat |
| `green_pile_prior` | stock book | **KILL** | 50 | +0.20 pp | +0.14 pp | PASS/FAIL/FAIL | prior-day green live_buy with in_pile=true (PIT). |
| `weighted_book_1d_prior` | stock book | **CONDITIONAL** | 37 | +0.49 pp | +0.06 pp | FAIL/FAIL/FAIL | prior-day stock_book 1d buy (weighted). Same clock as book_1d_prior universe. |
| `unweighted_book_prior` | stock book | **KILL** | 61 | -0.39 pp | -0.37 pp | PASS/PASS/PASS | prior-day unweighted.json live_buy (PIT). |

### Plain English per featured sleeve

- flatten_robust: **KILL** — pooled avoid -0.59 pp n=11; elev +0.19 pp n=23. live tickets; io often 16:00 / 3d. Blanket J-avoid fights mover gap-up.
- flatten_h5: **KILL** — pooled avoid -1.26 pp n=10; elev -0.63 pp n=19. factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for context, overlay is on fills (cash book)
- flatten_live_h5: **null** — pooled avoid -2.09 pp n=2; elev -1.61 pp n=9. factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for context, overlay is on fills (cash book)
- green_pile_prior: **KILL** — prove avoid +0.20 pp n=50; elev +0.14 pp n=61. prior-day green live_buy with in_pile=true (PIT).
- green_book_prior: **KILL** — prove avoid -0.39 pp n=61; elev -0.37 pp n=64. prior-day green.json live_buy (PIT). Afternoon stamp is not a same-day open feature.
- weighted_book_1d_prior: **CONDITIONAL** — prove avoid +0.49 pp n=37; elev +0.06 pp n=64. prior-day stock_book 1d buy (weighted). Same clock as book_1d_prior universe.
- unweighted_book_prior: **KILL** — prove avoid -0.39 pp n=61; elev -0.37 pp n=64. prior-day unweighted.json live_buy (PIT).

### All STRATEGY_BOARD / dashboard sleeves

| sleeve | family | verdict | n | avoid vs sleeve | elev vs sleeve | ghost | note |
|---|---|---|---:|---:|---:|---|---|
| `flatten_robust` | sleeve merge | **KILL** | 11 | -0.59 pp | +0.19 pp | FAIL/PASS/FAIL | live tickets; io often 16:00 / 3d. Blanket J-avoid fights mover gap-up. |
| `coil_h3_exit_alarm` | factor mine | **CONDITIONAL** | 4 | +5.43 pp | +0.77 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `flatten_h1` | factor mine | **KILL** | 15 | -1.13 pp | -0.06 pp | PASS/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h3` | factor mine | **KILL** | 13 | -1.51 pp | -0.90 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h3_cut` | factor mine | **KILL** | 13 | -1.51 pp | -0.90 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h3_half` | factor mine | **KILL** | 14 | -1.19 pp | -0.10 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h3_rankw` | factor mine | **KILL** | 12 | -1.44 pp | -0.84 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h3_sboost` | factor mine | **KILL** | 13 | -1.53 pp | -0.92 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h3_sizeup` | factor mine | **KILL** | 13 | -1.51 pp | -0.90 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h3_time` | factor mine | **KILL** | 14 | -1.45 pp | -0.83 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h3_topheavy` | factor mine | **KILL** | 13 | -0.92 pp | -0.06 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h3_trail` | factor mine | **KILL** | 13 | -1.51 pp | -0.90 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h5` | factor mine | **KILL** | 10 | -1.26 pp | -0.63 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h5_cut` | factor mine | **KILL** | 10 | -1.26 pp | -0.63 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h5_half` | factor mine | **KILL** | 13 | -1.19 pp | -0.09 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h5_rankw` | factor mine | **KILL** | 9 | -0.85 pp | -0.02 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h5_sboost` | factor mine | **KILL** | 10 | -1.28 pp | -0.64 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h5_sizeup` | factor mine | **KILL** | 10 | -1.26 pp | -0.63 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h5_time` | factor mine | **KILL** | 10 | -1.26 pp | -0.63 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h5_topheavy` | factor mine | **KILL** | 11 | -1.57 pp | -0.82 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_h5_trail` | factor mine | **KILL** | 10 | -1.26 pp | -0.63 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_live_h1` | factor mine | **null** | 2 | -2.01 pp | +0.31 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_live_h1_cut` | factor mine | **null** | 2 | -2.01 pp | +0.31 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_live_h1_half` | factor mine | **null** | 2 | -2.01 pp | +0.31 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_live_h1_rankw` | factor mine | **null** | 2 | -2.01 pp | +0.31 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_live_h1_sboost` | factor mine | **null** | 2 | -2.01 pp | +0.31 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_live_h1_sizeup` | factor mine | **null** | 2 | -2.01 pp | +0.31 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_live_h1_time` | factor mine | **null** | 2 | -2.01 pp | +0.31 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_live_h1_topheavy` | factor mine | **null** | 2 | -2.01 pp | +0.31 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_live_h1_trail` | factor mine | **null** | 2 | -2.01 pp | +0.31 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_live_h3` | factor mine | **null** | 2 | -2.09 pp | -1.61 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_live_h5` | factor mine | **null** | 2 | -2.09 pp | -1.61 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `flatten_vol_g_h3` | factor mine | **null** | 4 | -1.62 pp | -0.86 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for contex |
| `ohlc_hot_coil_h1` | factor mine | **null** | 2 | +0.08 pp | -0.47 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `ohlc_hot_h1` | factor mine | **KILL** | 8 | +0.94 pp | +0.16 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `ohlc_hot_h3` | factor mine | **KILL** | 5 | +0.31 pp | -0.08 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `ohlc_hot_h5` | factor mine | **KILL** | 4 | +0.58 pp | -0.43 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `probable_h1` | factor mine | **CONDITIONAL** | 2 | +17.66 pp | +1.25 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `probable_h3` | factor mine | **CONDITIONAL** | 6 | +6.14 pp | +0.94 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `probable_h5` | factor mine | **CONDITIONAL** | 2 | +17.87 pp | +1.33 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `probable_probable_ok_h1` | factor mine | **CONDITIONAL** | 2 | +17.66 pp | +1.05 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `probable_probable_ok_h3` | factor mine | **CONDITIONAL** | 2 | +17.75 pp | +1.62 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `short_alarm_h1` | factor mine | **null** | 0 | — | — | — | short sleeve — J avoid/elevate is a long overlay; not scored |
| `short_alarm_h3` | factor mine | **null** | 0 | — | — | — | short sleeve — J avoid/elevate is a long overlay; not scored |
| `short_extended_h1` | factor mine | **null** | 0 | — | — | — | short sleeve — J avoid/elevate is a long overlay; not scored |
| `short_extended_h3` | factor mine | **null** | 0 | — | — | — | short sleeve — J avoid/elevate is a long overlay; not scored |
| `short_last_red_h1` | factor mine | **null** | 0 | — | — | — | short sleeve — J avoid/elevate is a long overlay; not scored |
| `short_last_red_h3` | factor mine | **null** | 0 | — | — | — | short sleeve — J avoid/elevate is a long overlay; not scored |
| `short_news_r_h1` | factor mine | **null** | 0 | — | — | — | short sleeve — J avoid/elevate is a long overlay; not scored |
| `short_news_r_h3` | factor mine | **null** | 0 | — | — | — | short sleeve — J avoid/elevate is a long overlay; not scored |
| `short_r_down_h1` | factor mine | **null** | 0 | — | — | — | short sleeve — J avoid/elevate is a long overlay; not scored |
| `short_r_down_h3` | factor mine | **null** | 0 | — | — | — | short sleeve — J avoid/elevate is a long overlay; not scored |
| `union_ab_g_h1` | factor mine | **CONDITIONAL** | 4 | +0.60 pp | +0.20 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_ab_g_h3` | factor mine | **KILL** | 3 | +0.57 pp | +0.02 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_blue_coil_h1` | factor mine | **CONDITIONAL** | 3 | +3.24 pp | +1.22 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_blue_coil_h3` | factor mine | **CONDITIONAL** | 2 | +2.44 pp | +1.47 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_blue_h1` | factor mine | **CONDITIONAL** | 3 | +2.64 pp | +1.33 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_blue_h3` | factor mine | **CONDITIONAL** | 2 | +1.74 pp | +1.42 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_blue_vol_h1` | factor mine | **KILL** | 2 | +3.38 pp | +0.81 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_blue_vol_h3` | factor mine | **KILL** | 2 | +3.77 pp | +0.75 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_break10_h1` | factor mine | **CONDITIONAL** | 6 | +1.08 pp | +0.57 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_break10_h3` | factor mine | **CONDITIONAL** | 10 | -0.03 pp | +0.61 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_candle_h1` | factor mine | **CONDITIONAL** | 6 | +2.70 pp | +0.91 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_candle_h3` | factor mine | **CONDITIONAL** | 5 | +2.10 pp | +1.31 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_candle_score_h1` | factor mine | **CONDITIONAL** | 10 | +0.80 pp | +0.55 pp | PASS/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_candle_score_h3` | factor mine | **CONDITIONAL** | 9 | +0.81 pp | +0.65 pp | PASS/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_catal_present_h1` | factor mine | **null** | 2 | -0.57 pp | -0.57 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_catal_present_h3` | factor mine | **null** | 2 | -1.11 pp | -1.11 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_coil_green_h1` | factor mine | **CONDITIONAL** | 12 | +1.82 pp | +0.83 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_coil_green_h3` | factor mine | **CONDITIONAL** | 11 | +0.77 pp | +0.31 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_coil_off_h1` | factor mine | **KILL** | 9 | +2.21 pp | +0.16 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_coil_off_h3` | factor mine | **CONDITIONAL** | 8 | +2.95 pp | +0.37 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_coil_off_h5` | factor mine | **CONDITIONAL** | 8 | +2.66 pp | +0.26 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_cond_h1` | factor mine | **CONDITIONAL** | 2 | +5.41 pp | +0.64 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_cond_h3` | factor mine | **CONDITIONAL** | 1 | +4.63 pp | +0.88 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_cond_n4_h3` | factor mine | **null** | 6 | -1.30 pp | +0.46 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_e_fresh_h1` | factor mine | **CONDITIONAL** | 10 | +0.57 pp | +0.02 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_e_fresh_h3` | factor mine | **KILL** | 7 | +0.65 pp | -0.17 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_e_green_h1` | factor mine | **KILL** | 11 | -0.31 pp | -0.06 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_e_green_h3` | factor mine | **CONDITIONAL** | 9 | -0.27 pp | -0.08 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_earn_react_h1` | factor mine | **KILL** | 10 | +0.57 pp | +0.02 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_earn_react_h3` | factor mine | **KILL** | 9 | +0.49 pp | -0.03 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h1` | factor mine | **CONDITIONAL** | 5 | +6.20 pp | +1.01 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h1_cut` | factor mine | **CONDITIONAL** | 5 | +6.20 pp | +1.01 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h1_half` | factor mine | **CONDITIONAL** | 5 | +6.13 pp | +1.12 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h1_rankw` | factor mine | **CONDITIONAL** | 5 | +6.13 pp | +1.12 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h1_sboost` | factor mine | **CONDITIONAL** | 5 | +6.20 pp | +1.01 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h1_sizeup` | factor mine | **CONDITIONAL** | 5 | +6.20 pp | +1.01 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h1_time` | factor mine | **CONDITIONAL** | 6 | +6.11 pp | +1.01 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h1_topheavy` | factor mine | **CONDITIONAL** | 5 | +6.13 pp | +1.12 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h1_trail` | factor mine | **CONDITIONAL** | 5 | +6.20 pp | +1.01 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h3` | factor mine | **KILL** | 14 | +0.70 pp | -0.12 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h3_cut` | factor mine | **KILL** | 14 | +0.70 pp | -0.12 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h3_exit_alarm` | factor mine | **KILL** | 14 | +1.08 pp | -0.12 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h3_exit_news_r` | factor mine | **KILL** | 13 | +1.04 pp | +0.04 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h3_exit_red` | factor mine | **CONDITIONAL** | 7 | +4.73 pp | +1.35 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h3_half` | factor mine | **CONDITIONAL** | 5 | +6.13 pp | +1.12 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h3_rankw` | factor mine | **KILL** | 13 | +0.72 pp | -0.19 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h3_sboost` | factor mine | **KILL** | 14 | +0.78 pp | -0.08 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h3_sizeup` | factor mine | **KILL** | 14 | +0.70 pp | -0.12 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h3_time` | factor mine | **CONDITIONAL** | 5 | +5.36 pp | +1.24 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h3_topheavy` | factor mine | **CONDITIONAL** | 14 | +0.92 pp | +0.53 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h3_trail` | factor mine | **KILL** | 14 | +0.70 pp | -0.12 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h5` | factor mine | **CONDITIONAL** | 4 | +8.85 pp | +0.98 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h5_cut` | factor mine | **CONDITIONAL** | 4 | +8.85 pp | +0.98 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h5_exit_alarm` | factor mine | **KILL** | 4 | +8.48 pp | +1.33 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h5_half` | factor mine | **CONDITIONAL** | 4 | +8.51 pp | +1.31 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h5_rankw` | factor mine | **CONDITIONAL** | 4 | +8.78 pp | +1.08 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h5_sboost` | factor mine | **CONDITIONAL** | 4 | +8.85 pp | +0.98 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h5_sizeup` | factor mine | **CONDITIONAL** | 4 | +8.85 pp | +0.98 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h5_time` | factor mine | **CONDITIONAL** | 5 | +6.38 pp | +0.92 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h5_topheavy` | factor mine | **KILL** | 4 | +8.48 pp | +1.33 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_h5_trail` | factor mine | **CONDITIONAL** | 4 | +8.85 pp | +0.98 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_hot_n12_h1` | factor mine | **CONDITIONAL** | 17 | +1.30 pp | +0.58 pp | PASS/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_hot_n4_h1` | factor mine | **KILL** | 11 | +1.46 pp | +0.83 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_hot_score_h1` | factor mine | **CONDITIONAL** | 11 | +1.75 pp | +0.85 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_hot_score_h3` | factor mine | **CONDITIONAL** | 10 | +1.91 pp | +0.87 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_join_g_h1` | factor mine | **KILL** | 4 | +0.90 pp | +0.17 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_join_g_h3` | factor mine | **KILL** | 3 | +1.14 pp | +0.38 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_join_present_h1` | factor mine | **CONDITIONAL** | 5 | +6.20 pp | +1.01 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_join_present_h3` | factor mine | **KILL** | 13 | +1.04 pp | +0.04 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_join_vol_green_h1` | factor mine | **KILL** | 4 | +1.02 pp | +0.60 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_join_vol_green_h3` | factor mine | **KILL** | 5 | +1.07 pp | -0.10 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_last_green_h1` | factor mine | **CONDITIONAL** | 7 | +4.86 pp | +1.36 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_last_green_h3` | factor mine | **CONDITIONAL** | 7 | +4.73 pp | +1.35 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_last_green_h5` | factor mine | **CONDITIONAL** | 6 | +6.41 pp | +1.40 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_last_red_h1` | factor mine | **CONDITIONAL** | 3 | -0.40 pp | +0.27 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_last_red_h3` | factor mine | **KILL** | 1 | +0.85 pp | +0.50 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_news_g_h1` | factor mine | **CONDITIONAL** | 5 | +1.07 pp | +0.23 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_news_g_h3` | factor mine | **CONDITIONAL** | 10 | +1.76 pp | +0.58 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_news_g_h5` | factor mine | **CONDITIONAL** | 9 | +1.74 pp | +0.32 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_news_missing_h1` | factor mine | **null** | 2 | +3.76 pp | +0.55 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_news_missing_h3` | factor mine | **null** | 2 | +3.76 pp | +0.55 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_news_present_h1` | factor mine | **CONDITIONAL** | 7 | +3.29 pp | +0.85 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_news_present_h3` | factor mine | **CONDITIONAL** | 4 | +7.14 pp | +1.04 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_news_vol_h1` | factor mine | **KILL** | 8 | +2.33 pp | +0.03 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_news_vol_h3` | factor mine | **KILL** | 7 | +1.98 pp | +0.28 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_r_up_h1` | factor mine | **null** | 0 | — | — | — | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_r_up_h3` | factor mine | **null** | 0 | — | — | — | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_ret_5_h1` | factor mine | **CONDITIONAL** | 12 | +0.39 pp | +1.02 pp | PASS/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_ret_5_h3` | factor mine | **CONDITIONAL** | 12 | +0.36 pp | +1.14 pp | PASS/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_vol_ab_h1` | factor mine | **KILL** | 3 | -0.89 pp | -0.42 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_vol_ab_h3` | factor mine | **KILL** | 3 | -0.57 pp | -0.59 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_vol_g_h1` | factor mine | **CONDITIONAL** | 4 | +6.72 pp | +1.13 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_vol_g_h3` | factor mine | **KILL** | 4 | +7.24 pp | +1.15 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_vol_g_h5` | factor mine | **CONDITIONAL** | 3 | +8.31 pp | +1.19 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_vol_green_h1` | factor mine | **CONDITIONAL** | 4 | +8.17 pp | +0.67 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_vol_green_h3` | factor mine | **CONDITIONAL** | 4 | +8.47 pp | +0.64 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_vol_missing_h1` | factor mine | **null** | 0 | — | — | — | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_vol_missing_h3` | factor mine | **null** | 0 | — | — | — | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_w_hot_candle_h1` | factor mine | **CONDITIONAL** | 11 | +2.17 pp | +0.83 pp | PASS/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_w_hot_candle_h3` | factor mine | **CONDITIONAL** | 11 | +1.57 pp | +0.12 pp | PASS/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_w_hot_cond_h1` | factor mine | **CONDITIONAL** | 7 | +2.29 pp | +0.93 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_w_hot_cond_h3` | factor mine | **CONDITIONAL** | 5 | +3.01 pp | +1.10 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_white_coil_h1` | factor mine | **CONDITIONAL** | 2 | +2.67 pp | +0.73 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_white_coil_h3` | factor mine | **CONDITIONAL** | 2 | +2.61 pp | +0.81 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_white_h1` | factor mine | **CONDITIONAL** | 1 | +3.46 pp | +1.05 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_white_h3` | factor mine | **KILL** | 1 | +3.57 pp | +1.16 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `union_white_h5` | factor mine | **CONDITIONAL** | 0 | — | +0.81 pp | FAIL/FAIL/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `yday_gainer_h1` | factor mine | **CONDITIONAL** | 2 | +17.66 pp | +1.25 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `yday_gainer_h3` | factor mine | **CONDITIONAL** | 2 | +17.97 pp | +1.90 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `yday_gainer_h5` | factor mine | **CONDITIONAL** | 2 | +18.04 pp | +1.19 pp | FAIL/PASS/FAIL | factor-mine 09:30 BUY fills vs same-day H after fees |
| `green_book_prior` | stock book | **KILL** | 61 | -0.39 pp | -0.37 pp | PASS/PASS/PASS | prior-day green.json live_buy (PIT). Afternoon stamp is not a same-day open feat |
| `green_pile_prior` | stock book | **KILL** | 50 | +0.20 pp | +0.14 pp | PASS/FAIL/FAIL | prior-day green live_buy with in_pile=true (PIT). |
| `weighted_book_1d_prior` | stock book | **CONDITIONAL** | 37 | +0.49 pp | +0.06 pp | FAIL/FAIL/FAIL | prior-day stock_book 1d buy (weighted). Same clock as book_1d_prior universe. |
| `unweighted_book_prior` | stock book | **KILL** | 61 | -0.39 pp | -0.37 pp | PASS/PASS/PASS | prior-day unweighted.json live_buy (PIT). |
| `1d_size` | .io paper | **CONDITIONAL** | 8 | +1.95 pp | +0.72 pp | FAIL/PASS/FAIL | .io paper buy date; fill may be close (not 09:30). |
| `1d_top` | .io paper | **CONDITIONAL** | 12 | +0.81 pp | +0.17 pp | FAIL/PASS/FAIL | .io paper buy date; fill may be close (not 09:30). |
| `1m_size` | .io paper | **null** | 4 | -0.84 pp | -0.44 pp | FAIL/PASS/FAIL | .io paper buy date; fill may be close (not 09:30). |
| `1m_top` | .io paper | **null** | 9 | -0.17 pp | -0.46 pp | FAIL/PASS/FAIL | .io paper buy date; fill may be close (not 09:30). |
| `1w_size` | .io paper | **KILL** | 6 | +2.05 pp | -0.09 pp | FAIL/PASS/FAIL | .io paper buy date; fill may be close (not 09:30). |
| `1w_top` | .io paper | **KILL** | 5 | +0.70 pp | -0.14 pp | FAIL/PASS/FAIL | .io paper buy date; fill may be close (not 09:30). |
| `2w_size` | .io paper | **CONDITIONAL** | 10 | +1.45 pp | +0.23 pp | FAIL/PASS/FAIL | .io paper buy date; fill may be close (not 09:30). |
| `2w_top` | .io paper | **null** | 13 | -0.29 pp | -0.25 pp | FAIL/PASS/FAIL | .io paper buy date; fill may be close (not 09:30). |
| `3d_size` | .io paper | **CONDITIONAL** | 9 | +2.31 pp | -0.07 pp | PASS/PASS/FAIL | .io paper buy date; fill may be close (not 09:30). |
| `3d_top` | .io paper | **CONDITIONAL** | 9 | +1.32 pp | +0.05 pp | FAIL/PASS/FAIL | .io paper buy date; fill may be close (not 09:30). |
| `sleeve_combine_bt` | sleeve combine | **KILL** | 7 | +1.62 pp | -0.02 pp | FAIL/PASS/FAIL | on-disk bt_trades.csv (io often 16:00). Other catalog combines share these fills |
| `sleeve_combine_io` | sleeve combine | **KILL** | 7 | +1.62 pp | -0.02 pp | FAIL/PASS/FAIL | bt_trades source=io |
| `sleeve_combine_mover` | sleeve combine | **null** | 0 | — | — | — | bt_trades source=mover |
| `book_paper_1w` | book paper | **CONDITIONAL** | 5 | +1.12 pp | +0.24 pp | FAIL/PASS/FAIL | book paper: catalog says close entry / 1w hold |
| `mover_paper_live` | mover stitch | **KILL** | 11 | +0.25 pp | +0.06 pp | FAIL/PASS/FAIL | mover paper 09:30 fills |
| `flatten_rotate` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_switch_recycle` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_hard_red` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_switch_full` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_carry_book` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_cash_mover` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_blank_cash` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_skip_blank_io` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_switch_70` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_rich` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_overlap` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_overlap_55` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_switch_60` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_switch` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `Combine 3d io_boost` | sleeve combine | **KILL** | 7 | +1.62 pp | -0.02 pp | FAIL/PASS/FAIL | filtered from shared bt_trades.csv |
| `Flatten core50_switch` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `Flatten concentrated_switch` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `Combine 3d overlay_boost` | sleeve combine | **KILL** | 7 | +1.62 pp | -0.02 pp | FAIL/PASS/FAIL | filtered from shared bt_trades.csv |
| `Combine 3d io_only` | sleeve combine | **KILL** | 7 | +1.62 pp | -0.02 pp | FAIL/PASS/FAIL | filtered from shared bt_trades.csv |
| `Flatten core_switch` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `Flatten switch_70` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `Flatten switch_80` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `flatten_3d` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `Combine 3d overlay` | sleeve combine | **KILL** | 7 | +1.62 pp | -0.02 pp | FAIL/PASS/FAIL | filtered from shared bt_trades.csv |
| `Flatten io_3d_switch` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `Flatten switch_no_short` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `Combine 1w overlay_boost` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Combine 1w io_boost` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Flatten mover_heavy` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `Combine 1d overlay_boost` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Combine 1w io_only` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Combine 1w overlay` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Flatten hard_red_shorts` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `Flatten switch_80_overlap` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `Combine 1d overlay` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Combine 3d dual` | sleeve combine | **KILL** | 7 | +1.62 pp | -0.02 pp | FAIL/PASS/FAIL | filtered from shared bt_trades.csv |
| `Flatten switch_90_overlap` | sleeve merge | **null** | 0 | — | — | — | sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade |
| `Combine 1d io_boost` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Combine 2w io_only` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Combine 1d io_only` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Combine 1w dual` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Combine 1w mover_only` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Combine 1w combine` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Combine 3d mover_only` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Combine 1d dual` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Combine 3d combine` | sleeve combine | **KILL** | 7 | +1.62 pp | -0.02 pp | FAIL/PASS/FAIL | filtered from shared bt_trades.csv |
| `1d mover × soft-red 1d .io` | mover stitch | **null** | 0 | — | — | — | no separate per-trade dump (see mover_paper_live) |
| `Combine 1d mover_only` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Combine 1d combine` | sleeve combine | **null** | 0 | — | — | — | no matching rows in bt_trades.csv |
| `Empty BUY list + skip → live 2w_size` | mover stitch | **null** | 0 | — | — | — | no separate per-trade dump (see mover_paper_live) |
| `Skip-day → live 2w_size` | mover stitch | **null** | 0 | — | — | — | no separate per-trade dump (see mover_paper_live) |
| `Mover paper v2 (old sim)` | mover | **null** | 0 | — | — | — | no separate per-trade dump (see mover_paper_live) |
| `Excel L3_long_green_hold2_midcap` | excel | **null** | 0 | — | — | — | excel / strategies/ frozen — not scored |
| `Excel live ledger (all cards)` | excel | **null** | 0 | — | — | — | excel / strategies/ frozen — not scored |
| `Excel L1_long_green_tp8_lowvol` | excel | **null** | 0 | — | — | — | excel / strategies/ frozen — not scored |
| `Excel L2_long_green_tp3_lowvol` | excel | **null** | 0 | — | — | — | excel / strategies/ frozen — not scored |
| `.io SPY (benchmark)` | .io paper | **null** | 0 | — | — | — | benchmark, not a name-picking sleeve |
| `Excel L5_long_green_hold2_midhibeta` | excel | **null** | 0 | — | — | — | excel / strategies/ frozen — not scored |

## Case studies (prove window)

### Avoid — 2026-08-27 FIGR → EMBJ (`avoid_J_ge0`)

Join top-8 that morning: MNDY, RELY, **FIGR**, ECO, NVDA, SKHY, HPE, CRDO.

| | FIGR (dropped) | EMBJ (refilled) |
|---|---|---|
| join rank | 3 | 9 |
| J at open | **+3.82%** (Open 40.50 vs 2026-08-25 Open 39.01) | **−2.09%** (Open 75.78 vs 08-25 Open 77.40) |
| without J | stays in the eight | stays out (rank 9) |
| with J | dropped (J≥0) | refilled (J<0) |
| H after 15 bp | **−8.59%** (raw −8.44%) | **−0.82%** (raw −0.67%) |
| I after 15 bp | **−10.02%** (raw −9.87%) | **+0.71%** (raw +0.86%) |

08-26 has join but no Finviz, so this J is vs Tuesday Open, not Wednesday. Still prior, not a close peek.

### Elevate — 2026-09-04 HRMY → AVAH (`elev_cap2_J_le-1`)

Join top-8 that morning: **HRMY**, HALO, CDNA, WAY, PLMR, ONC, KKR, NU.

| | HRMY (dropped) | AVAH (elevated) |
|---|---|---|
| join rank | 1 | 13 |
| J at open | **+3.92%** (Open 42.93 vs 2026-09-03 Open 41.31) | **−2.29%** (Open 13.22 vs 09-03 Open 13.53) |
| without J | stays #1 in the eight | stays out (rank 13) |
| with J | swapped out (J≥0, one of ≤2) | swapped in (J≤−1% from ranks 9–80) |
| H after 15 bp | **−2.64%** (raw −2.49%) | **+3.03%** (raw +3.18%) |
| I after 15 bp | **−2.48%** (raw −2.33%) | **+3.18%** (raw +3.33%) |

These two name-days illustrate the rule. They are not a new holdout. Family stays **CONDITIONAL**.

## Card 1 — avoid J≥0

| field | value |
|---|---|
| rule | drop morning picks with J≥0; refill from J<0 when the book is ranked |
| entry | open |
| label | same-day H after fees |
| code | `avoid_J_ge0` |
| join top-8 prove | CONDITIONAL (under 20 bp + ghost fail) |
| broader universes | DEMOTE |
| dashboard sleeves | see table — no live wire |
| status | research card · not live · not KEEP holds |

## Card 2 — elevate J≤−1% cap 2

| field | value |
|---|---|
| rule | swap ≤2 J≥0 names for J≤−1% from ranks n+1–80 (list) or drop ≤2 J≥0 / day (tickets) |
| entry | open |
| label | same-day H after fees |
| code | `elev_cap2_J_le-1` |
| join top-8 prove | CONDITIONAL |
| broader universes | DEMOTE |
| dashboard sleeves | see table — no live wire |
| status | research card · not live · not KEEP holds |

## Explicitly not carded / caveats

| item | note |
|---|---|
| Sunday join dumps 08-30 / 09-06 | not a 1d session; held out of prove |
| 08-13 J | stale (prior Open 04-26); not used |
| flatten_robust overlay | blanket KEEP recipes hurt sleeve P&L; io-only fair test is not a live change |
| excel / `strategies/` | frozen — not scored |
| shorts | J overlay is long-only; null |
| sleeve_merge sweep variants | no per-trade rows; null |

## Source

`JOIN_POST_813.md` tip `f41d73a5` · PR #153. Gate: `OPEN_SAME_ROW_LABELS.md` / `CLOCK_MAP.md`. Research only. Live frozen.
