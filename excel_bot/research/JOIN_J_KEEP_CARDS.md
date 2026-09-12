# Research cards — Excel J × fullscan join (post-8-13)

_Generated 2026-09-08 · tip `35695bb2` / `JOIN_POST_813.md` · **research cards only** · live frozen._

## Plain English

J clock leak **PASS**. Cyrus fire bar (>55% of days the rule changes the book vs the same-day no-rule book, ≥30 fires): **CLEAR** — ohlc_liq vol_top8 long `elev_cap2_J_le-1` 55.9% (156/279 fires); ohlc_liq prior_green_top8 long `elev_cap2_J_le-1` 55.7% (141/253 fires); ohlc_liq vol_top8 pre813 `avoid_J_ge0` 60.9% (92/151 fires); ohlc_liq vol_top8 pre813 `elev_cap2_J_le-1` 56.9% (87/153 fires); ohlc_liq prior_green_top8 pre813 `avoid_J_ge0` 56.9% (87/153 fires); ohlc_liq prior_green_top8 pre813 `elev_cap2_J_le-1` 56.8% (83/146 fires); mem_20260426 unranked long `avoid_J_ge0` 71.0% (198/279 fires); mem_20260426 vol_top8 long `avoid_J_ge0` 55.1% (150/272 fires); mem_20260426 vol_top8 long `elev_cap2_J_le-1` 56.6% (158/279 fires); mem_20260426 prior_green_top8 long `elev_cap2_J_le-1` 55.3% (142/257 fires); mem_20260426 prior_green_unranked long `avoid_J_ge0` 80.2% (223/278 fires); mem_20260426 unranked y2025 `avoid_J_ge0` 74.8% (89/119 fires); mem_20260426 prior_green_unranked y2025 `avoid_J_ge0` 88.1% (104/118 fires); mem_20260426 unranked pre813 `avoid_J_ge0` 68.0% (104/153 fires); mem_20260426 vol_top8 pre813 `avoid_J_ge0` 60.3% (91/151 fires); mem_20260426 vol_top8 pre813 `elev_cap2_J_le-1` 58.8% (90/153 fires); mem_20260426 prior_green_top8 pre813 `elev_cap2_J_le-1` 55.1% (81/147 fires); mem_20260426 prior_green_unranked pre813 `avoid_J_ge0` 73.9% (113/153 fires); ohlc_all unranked long `avoid_J_ge0` 61.4% (333/542 fires); ohlc_all unranked y2025 `avoid_J_ge0` 65.1% (142/218 fires); ohlc_all unranked pre813 `avoid_J_ge0` 68.0% (104/153 fires). After-fee H is the equal-weight day-mean minus 15 bp Futubull, not dollar-weighted. Prior 6/8 weighted-book avoid and 5/8 green-pile elev are **PROVISIONAL** (n<30). y2025 does not confirm liquid vol_top8 / prior_green_top8 (those sit 47–54% in 2025). Unranked ohlc_all / mem avoid CLEARs are the already-demoted microcap lottery (H+ ~41%). Prove (weekday 2026-08-26→2026-09-07) does **not** re-clear the ship bar. `avoid_J_ge0` n=64 H +0.22% (+0.04 pp vs same-window top-8, ghost PASS/FAIL/FAIL). `elev_cap2_J_le-1` n=64 H +0.33% (+0.15 pp, ghost PASS/FAIL/FAIL). Discovery half still prints (`avoid_J_ge0` +0.72 pp n=64) — that is the peek, not prove. Pooled weekday leftover is `avoid_J_ge0` +0.38 pp n=128 / `elev_cap2_J_le-1` +0.26 pp n=128 (includes discovery; not a holdout). Wider book (top-80) prove `avoid_J_ge0` +0.12 pp n=640. Join dumps do not add sessions before 8-13 with a fresh J (08-13 prior Open is 04-26). Family is **CONDITIONAL**: not KEEP holds, not a full KILL of the discovery print. Live flatten_robust stays frozen. Do not wire. Expanded prove: the J overlay does **not** hold as a general rule. Yahoo liquid names (prior-session volume ≥ 1M, 2024–2026) are about flat (+2 to +5 bp, ghost month fail). The all-name Yahoo tape’s large mean is a microcap lottery (ghost FAIL). Join-full / membership on the Finviz window is +5 to +13 bp (under 20 bp). membership_liq is flat to negative. Join top-8 stays **CONDITIONAL** (discovery only). Dashboard sleeves (open J only): 0 KEEP / 68 KILL / 83 CONDITIONAL / 88 null of 239 scored rows. Question is after-fee H vs that sleeve alone. Live flatten_robust stays frozen. flatten_robust: **KILL** — pooled avoid -0.59 pp n=11 fire 50.0% (2/4 fires); elev +0.19 pp n=23 fire 50.0% (2/4 fires). live tickets; io often 16:00 / 3d. Blanket J-avoid fights mover gap-up. flatten_h5: **KILL** — pooled avoid -1.26 pp n=10 fire 50.0% (3/6 fires); elev -0.63 pp n=19 fire 33.3% (2/6 fires). factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for context, overlay is on fills (cash book) flatten_live_h5: **null** — pooled avoid -2.09 pp n=2 fire 50.0% (1/2 fires); elev -1.61 pp n=9 fire 0.0% (0/2 fires). factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for context, overlay is on fills (cash book) green_pile_prior: **KILL** — prove avoid +0.20 pp n=50 fire 50.0% (4/8 fires); elev +0.14 pp n=61 fire 62.5% (5/8 fires). prior-day green live_buy with in_pile=true (PIT). green_book_prior: **KILL** — prove avoid -0.39 pp n=61 fire 37.5% (3/8 fires); elev -0.37 pp n=64 fire 37.5% (3/8 fires). prior-day green.json live_buy (PIT). Afternoon stamp is not a same-day open feature. weighted_book_1d_prior: **CONDITIONAL** — prove avoid +0.49 pp n=37 fire 75.0% (6/8 fires); elev +0.06 pp n=64 fire 66.7% (2/3 fires). prior-day stock_book 1d buy (weighted). Same clock as book_1d_prior universe. unweighted_book_prior: **KILL** — prove avoid -0.39 pp n=61 fire 37.5% (3/8 fires); elev -0.37 pp n=64 fire 37.5% (3/8 fires). prior-day unweighted.json live_buy (PIT).

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

## Win-rate bar (Cyrus: >55% of fires, n≥30)

**Fire** = a morning the rule changes the book (ticker set ≠ no-rule set). **Win** = that day’s rule-book mean after-fee H beats the same-day no-rule book. Ties do not beat. **CLEAR** needs win-rate **>55% and ≥30 fires** on the prove/pooled window used for the call. n=8 is **not** proven. A print that is >55% but n_fires<30 is **PROVISIONAL** (demoted) — including the prior weighted-book avoid 6/8 and green-pile elev 5/8. After-fee H is the equal-weight day-mean minus 15 bp Futubull, not dollar-weighted. Separately: % of the rule’s name-days with after-fee H>0 and I>0. Native Finviz/join/stock_book dumps are ~16 weekdays and cannot reach 30 fires alone; longer Yahoo OHLC day-books (2024-03→2026-08-21) are the material-n tape. Live = wired into the cash/paper bot — docs are not live. Live stays frozen.

| circumstance | window | recipe | fire bar | fire win-rate | H+ after fees | I+ after fees | mean vs |
|---|---|---|---|---|---|---|---|
| join top-8 | prove | `avoid_J_ge0` | **FAIL** | 50.0% (4/8 fires) | 48.4% n=64 | 46.9% n=64 | +0.04 pp |
| join top-8 | prove | `elev_cap2_J_le-1` | **FAIL** | 50.0% (4/8 fires, 1 tie) | 51.6% n=64 | 56.2% n=64 | +0.15 pp |
| join top-8 | discovery | `avoid_J_ge0` | **PROVISIONAL** | 87.5% (7/8 fires) | 64.1% n=64 | 59.4% n=64 | +0.72 pp |
| join top-8 | discovery | `elev_cap2_J_le-1` | **PROVISIONAL** | 75.0% (6/8 fires) | 51.6% n=64 | 54.7% n=64 | +0.36 pp |
| join top-8 | pooled_sessions | `avoid_J_ge0` | **PROVISIONAL** | 68.8% (11/16 fires) | 56.2% n=128 | 53.1% n=128 | +0.38 pp |
| join top-8 | pooled_sessions | `elev_cap2_J_le-1` | **PROVISIONAL** | 62.5% (10/16 fires, 1 tie) | 51.6% n=128 | 55.5% n=128 | +0.26 pp |
| ohlc_liq unranked | long | `avoid_J_ge0` | **FAIL** | 48.7% (201/413 fires) | 45.5% n=298877 | — | — |
| ohlc_liq vol_top8 | long | `avoid_J_ge0` | **FAIL** | 53.4% (217/406 fires) | 45.3% n=2569 | — | — |
| ohlc_liq vol_top8 | long | `elev_cap2_J_le-1` | **CLEAR** | 55.9% (156/279 fires) | 44.8% n=2887 | — | — |
| ohlc_liq vol_top80 | long | `avoid_J_ge0` | **FAIL** | 50.6% (209/413 fires) | 44.9% n=22657 | — | — |
| ohlc_liq prior_green_unranked | long | `avoid_J_ge0` | **FAIL** | 49.7% (146/294 fires) | 47.2% n=37399 | — | — |
| ohlc_liq prior_green_top8 | long | `avoid_J_ge0` | **FAIL** | 53.7% (158/294 fires) | 44.9% n=2263 | — | — |
| ohlc_liq prior_green_top8 | long | `elev_cap2_J_le-1` | **CLEAR** | 55.7% (141/253 fires) | 45.6% n=2480 | — | — |
| ohlc_liq prior_green_top80 | long | `avoid_J_ge0` | **FAIL** | 52.4% (154/294 fires) | 45.5% n=19691 | — | — |
| ohlc_liq unranked | y2025 | `avoid_J_ge0` | **FAIL** | 51.2% (88/172 fires) | 44.9% n=116611 | — | — |
| ohlc_liq vol_top8 | y2025 | `avoid_J_ge0` | **FAIL** | 51.5% (86/167 fires) | 45.2% n=1084 | — | — |
| ohlc_liq vol_top8 | y2025 | `elev_cap2_J_le-1` | **FAIL** | 53.8% (64/119 fires) | 44.8% n=1213 | — | — |
| ohlc_liq vol_top80 | y2025 | `avoid_J_ge0` | **FAIL** | 51.2% (88/172 fires) | 43.4% n=9652 | — | — |
| ohlc_liq prior_green_unranked | y2025 | `avoid_J_ge0` | **FAIL** | 51.2% (65/127 fires) | 46.7% n=11925 | — | — |
| ohlc_liq prior_green_top8 | y2025 | `avoid_J_ge0` | **FAIL** | 47.2% (60/127 fires) | 42.7% n=963 | — | — |
| ohlc_liq prior_green_top8 | y2025 | `elev_cap2_J_le-1` | **FAIL** | 53.0% (53/100 fires) | 43.5% n=1050 | — | — |
| ohlc_liq prior_green_top80 | y2025 | `avoid_J_ge0` | **FAIL** | 52.8% (67/127 fires) | 44.3% n=7767 | — | — |
| ohlc_liq unranked | pre813 | `avoid_J_ge0` | **FAIL** | 49.7% (76/153 fires) | 46.3% n=174214 | — | — |
| ohlc_liq vol_top8 | pre813 | `avoid_J_ge0` | **CLEAR** | 60.9% (92/151 fires) | 46.7% n=1224 | — | — |
| ohlc_liq vol_top8 | pre813 | `elev_cap2_J_le-1` | **CLEAR** | 56.9% (87/153 fires) | 46.2% n=1224 | — | — |
| ohlc_liq vol_top80 | pre813 | `avoid_J_ge0` | **FAIL** | 52.3% (80/153 fires) | 46.1% n=12240 | — | — |
| ohlc_liq prior_green_unranked | pre813 | `avoid_J_ge0` | **FAIL** | 46.4% (71/153 fires) | 47.9% n=24495 | — | — |
| ohlc_liq prior_green_top8 | pre813 | `avoid_J_ge0` | **CLEAR** | 56.9% (87/153 fires) | 46.6% n=1224 | — | — |
| ohlc_liq prior_green_top8 | pre813 | `elev_cap2_J_le-1` | **CLEAR** | 56.8% (83/146 fires) | 47.8% n=1210 | — | — |
| ohlc_liq prior_green_top80 | pre813 | `avoid_J_ge0` | **FAIL** | 49.0% (75/153 fires) | 46.6% n=11379 | — | — |
| mem_20260426 unranked | long | `avoid_J_ge0` | **CLEAR** | 71.0% (198/279 fires) | 41.2% n=1332777 | — | — |
| mem_20260426 vol_top8 | long | `avoid_J_ge0` | **CLEAR** | 55.1% (150/272 fires) | 45.5% n=2232 | — | — |
| mem_20260426 vol_top8 | long | `elev_cap2_J_le-1` | **CLEAR** | 56.6% (158/279 fires) | 45.2% n=2232 | — | — |
| mem_20260426 vol_top80 | long | `avoid_J_ge0` | **FAIL** | 51.3% (143/279 fires) | 44.9% n=22320 | — | — |
| mem_20260426 prior_green_top8 | long | `avoid_J_ge0` | **FAIL** | 54.7% (152/278 fires) | 45.3% n=2224 | — | — |
| mem_20260426 prior_green_top8 | long | `elev_cap2_J_le-1` | **CLEAR** | 55.3% (142/257 fires) | 45.8% n=2189 | — | — |
| mem_20260426 prior_green_unranked | long | `avoid_J_ge0` | **CLEAR** | 80.2% (223/278 fires) | 44.7% n=214966 | — | — |
| mem_20260426 unranked | y2025 | `avoid_J_ge0` | **CLEAR** | 74.8% (89/119 fires) | 40.8% n=546130 | — | — |
| mem_20260426 vol_top8 | y2025 | `avoid_J_ge0` | **FAIL** | 49.1% (56/114 fires) | 44.7% n=952 | — | — |
| mem_20260426 vol_top8 | y2025 | `elev_cap2_J_le-1` | **FAIL** | 52.9% (63/119 fires) | 44.2% n=952 | — | — |
| mem_20260426 vol_top80 | y2025 | `avoid_J_ge0` | **FAIL** | 49.6% (59/119 fires) | 43.4% n=9520 | — | — |
| mem_20260426 prior_green_top8 | y2025 | `avoid_J_ge0` | **FAIL** | 53.4% (63/118 fires) | 43.4% n=944 | — | — |
| mem_20260426 prior_green_top8 | y2025 | `elev_cap2_J_le-1` | **FAIL** | 54.4% (56/103 fires) | 43.4% n=924 | — | — |
| mem_20260426 prior_green_unranked | y2025 | `avoid_J_ge0` | **CLEAR** | 88.1% (104/118 fires) | 44.0% n=78355 | — | — |
| mem_20260426 unranked | pre813 | `avoid_J_ge0` | **CLEAR** | 68.0% (104/153 fires) | 41.9% n=750330 | — | — |
| mem_20260426 vol_top8 | pre813 | `avoid_J_ge0` | **CLEAR** | 60.3% (91/151 fires) | 46.5% n=1224 | — | — |
| mem_20260426 vol_top8 | pre813 | `elev_cap2_J_le-1` | **CLEAR** | 58.8% (90/153 fires) | 46.2% n=1224 | — | — |
| mem_20260426 vol_top80 | pre813 | `avoid_J_ge0` | **FAIL** | 50.3% (77/153 fires) | 46.0% n=12240 | — | — |
| mem_20260426 prior_green_top8 | pre813 | `avoid_J_ge0` | **FAIL** | 54.9% (84/153 fires) | 46.5% n=1224 | — | — |
| mem_20260426 prior_green_top8 | pre813 | `elev_cap2_J_le-1` | **CLEAR** | 55.1% (81/147 fires) | 47.5% n=1210 | — | — |
| mem_20260426 prior_green_unranked | pre813 | `avoid_J_ge0` | **CLEAR** | 73.9% (113/153 fires) | 45.6% n=131361 | — | — |
| ohlc_all unranked | long | `avoid_J_ge0` | **CLEAR** | 61.4% (333/542 fires) | 41.1% n=1363125 | — | — |
| ohlc_all unranked | y2025 | `avoid_J_ge0` | **CLEAR** | 65.1% (142/218 fires) | 40.9% n=552198 | — | — |
| ohlc_all unranked | pre813 | `avoid_J_ge0` | **CLEAR** | 68.0% (104/153 fires) | 41.7% n=771148 | — | — |
| flatten_robust | pooled | `avoid_J_ge0` | **FAIL** | 50.0% (2/4 fires) | 45.5% n=11 | 54.5% n=11 | -0.59 pp |
| flatten_robust | pooled | `elev_cap2_J_le-1` | **FAIL** | 50.0% (2/4 fires) | 56.5% n=23 | 56.5% n=23 | +0.19 pp |
| flatten_h5 | pooled | `avoid_J_ge0` | **FAIL** | 50.0% (3/6 fires) | 50.0% n=10 | 60.0% n=10 | -1.26 pp |
| flatten_h5 | pooled | `elev_cap2_J_le-1` | **FAIL** | 33.3% (2/6 fires) | 57.9% n=19 | 57.9% n=19 | -0.63 pp |
| flatten_live_h5 | pooled | `avoid_J_ge0` | **FAIL** | 50.0% (1/2 fires) | 50.0% n=2 | 50.0% n=2 | -2.09 pp |
| flatten_live_h5 | pooled | `elev_cap2_J_le-1` | **FAIL** | 0.0% (0/2 fires) | 55.6% n=9 | 44.4% n=9 | -1.61 pp |
| union_coil_green_h1 | prove | `avoid_J_ge0` | **PROVISIONAL** | 75.0% (3/4 fires) | 58.3% n=12 | 58.3% n=12 | +1.82 pp |
| union_coil_green_h1 | prove | `elev_cap2_J_le-1` | **PROVISIONAL** | 75.0% (3/4 fires) | 57.9% n=19 | 63.2% n=19 | +0.83 pp |
| union_e_fresh_h1 | prove | `avoid_J_ge0` | **PROVISIONAL** | 100.0% (3/3 fires) | 50.0% n=10 | 70.0% n=10 | +0.57 pp |
| union_e_fresh_h1 | prove | `elev_cap2_J_le-1` | **PROVISIONAL** | 66.7% (2/3 fires) | 50.0% n=18 | 61.1% n=18 | +0.02 pp |
| union_e_green_h3 | prove | `avoid_J_ge0` | **PROVISIONAL** | 100.0% (3/3 fires) | 55.6% n=9 | 55.6% n=9 | -0.27 pp |
| union_e_green_h3 | prove | `elev_cap2_J_le-1` | **PROVISIONAL** | 66.7% (2/3 fires) | 64.3% n=14 | 64.3% n=14 | -0.08 pp |
| union_h1 | prove | `avoid_J_ge0` | **PROVISIONAL** | 100.0% (2/2 fires) | 80.0% n=5 | 80.0% n=5 | +6.20 pp |
| union_h1 | prove | `elev_cap2_J_le-1` | **PROVISIONAL** | 75.0% (3/4 fires) | 75.0% n=16 | 81.2% n=16 | +1.01 pp |
| union_h1_cut | prove | `avoid_J_ge0` | **PROVISIONAL** | 100.0% (2/2 fires) | 80.0% n=5 | 80.0% n=5 | +6.20 pp |
| union_h1_cut | prove | `elev_cap2_J_le-1` | **PROVISIONAL** | 75.0% (3/4 fires) | 75.0% n=16 | 81.2% n=16 | +1.01 pp |
| union_h1_half | prove | `avoid_J_ge0` | **PROVISIONAL** | 100.0% (2/2 fires) | 80.0% n=5 | 80.0% n=5 | +6.13 pp |
| union_h1_half | prove | `elev_cap2_J_le-1` | **PROVISIONAL** | 75.0% (3/4 fires) | 73.3% n=15 | 80.0% n=15 | +1.12 pp |
| union_h1_rankw | prove | `avoid_J_ge0` | **PROVISIONAL** | 100.0% (2/2 fires) | 80.0% n=5 | 80.0% n=5 | +6.13 pp |
| union_h1_rankw | prove | `elev_cap2_J_le-1` | **PROVISIONAL** | 75.0% (3/4 fires) | 73.3% n=15 | 80.0% n=15 | +1.12 pp |
| union_h1_sboost | prove | `avoid_J_ge0` | **PROVISIONAL** | 100.0% (2/2 fires) | 80.0% n=5 | 80.0% n=5 | +6.20 pp |
| union_h1_sboost | prove | `elev_cap2_J_le-1` | **PROVISIONAL** | 75.0% (3/4 fires) | 75.0% n=16 | 81.2% n=16 | +1.01 pp |
| union_h1_sizeup | prove | `avoid_J_ge0` | **PROVISIONAL** | 100.0% (2/2 fires) | 80.0% n=5 | 80.0% n=5 | +6.20 pp |
| union_h1_sizeup | prove | `elev_cap2_J_le-1` | **PROVISIONAL** | 75.0% (3/4 fires) | 75.0% n=16 | 81.2% n=16 | +1.01 pp |
| union_h1_time | prove | `avoid_J_ge0` | **PROVISIONAL** | 66.7% (2/3 fires) | 66.7% n=6 | 66.7% n=6 | +6.11 pp |
| union_h1_time | prove | `elev_cap2_J_le-1` | **PROVISIONAL** | 75.0% (3/4 fires) | 62.5% n=24 | 66.7% n=24 | +1.01 pp |
| union_h1_topheavy | prove | `avoid_J_ge0` | **PROVISIONAL** | 100.0% (2/2 fires) | 80.0% n=5 | 80.0% n=5 | +6.13 pp |
| union_h1_topheavy | prove | `elev_cap2_J_le-1` | **PROVISIONAL** | 75.0% (3/4 fires) | 73.3% n=15 | 80.0% n=15 | +1.12 pp |
| union_h1_trail | prove | `avoid_J_ge0` | **PROVISIONAL** | 100.0% (2/2 fires) | 80.0% n=5 | 80.0% n=5 | +6.20 pp |
| union_h1_trail | prove | `elev_cap2_J_le-1` | **PROVISIONAL** | 75.0% (3/4 fires) | 75.0% n=16 | 81.2% n=16 | +1.01 pp |
| union_h3_half | prove | `avoid_J_ge0` | **PROVISIONAL** | 100.0% (2/2 fires) | 80.0% n=5 | 80.0% n=5 | +6.13 pp |
| union_h3_half | prove | `elev_cap2_J_le-1` | **PROVISIONAL** | 75.0% (3/4 fires) | 73.3% n=15 | 80.0% n=15 | +1.12 pp |
| union_h3_time | prove | `avoid_J_ge0` | **PROVISIONAL** | 66.7% (2/3 fires) | 60.0% n=5 | 60.0% n=5 | +5.36 pp |
| union_h3_time | prove | `elev_cap2_J_le-1` | **PROVISIONAL** | 75.0% (3/4 fires) | 66.7% n=12 | 66.7% n=12 | +1.24 pp |
| union_hot_n12_h1 | prove | `avoid_J_ge0` | **PROVISIONAL** | 100.0% (3/3 fires) | 76.5% n=17 | 64.7% n=17 | +1.30 pp |
| union_hot_n12_h1 | prove | `elev_cap2_J_le-1` | **PROVISIONAL** | 75.0% (3/4 fires) | 61.3% n=31 | 58.1% n=31 | +0.58 pp |
| union_hot_score_h1 | prove | `avoid_J_ge0` | **PROVISIONAL** | 100.0% (3/3 fires) | 81.8% n=11 | 72.7% n=11 | +1.75 pp |
| union_hot_score_h1 | prove | `elev_cap2_J_le-1` | **PROVISIONAL** | 75.0% (3/4 fires) | 68.4% n=19 | 68.4% n=19 | +0.85 pp |
| union_join_present_h1 | prove | `avoid_J_ge0` | **PROVISIONAL** | 100.0% (2/2 fires) | 80.0% n=5 | 80.0% n=5 | +6.20 pp |
| union_join_present_h1 | prove | `elev_cap2_J_le-1` | **PROVISIONAL** | 75.0% (3/4 fires) | 75.0% n=16 | 81.2% n=16 | +1.01 pp |
| union_news_present_h1 | prove | `avoid_J_ge0` | **PROVISIONAL** | 66.7% (2/3 fires) | 42.9% n=7 | 42.9% n=7 | +3.29 pp |
| union_news_present_h1 | prove | `elev_cap2_J_le-1` | **PROVISIONAL** | 75.0% (3/4 fires) | 66.7% n=18 | 66.7% n=18 | +0.85 pp |
| union_ret_5_h1 | prove | `avoid_J_ge0` | **FAIL** | 33.3% (1/3 fires) | 75.0% n=12 | 66.7% n=12 | +0.39 pp |
| union_ret_5_h1 | prove | `elev_cap2_J_le-1` | **PROVISIONAL** | 75.0% (3/4 fires) | 65.0% n=20 | 65.0% n=20 | +1.02 pp |
| union_ret_5_h3 | prove | `avoid_J_ge0` | **FAIL** | 33.3% (1/3 fires) | 75.0% n=12 | 66.7% n=12 | +0.36 pp |
| union_ret_5_h3 | prove | `elev_cap2_J_le-1` | **PROVISIONAL** | 75.0% (3/4 fires) | 66.7% n=18 | 61.1% n=18 | +1.14 pp |
| union_w_hot_candle_h1 | prove | `avoid_J_ge0` | **PROVISIONAL** | 100.0% (3/3 fires) | 81.8% n=11 | 72.7% n=11 | +2.17 pp |
| union_w_hot_candle_h1 | prove | `elev_cap2_J_le-1` | **PROVISIONAL** | 75.0% (3/4 fires) | 63.2% n=19 | 63.2% n=19 | +0.83 pp |
| union_w_hot_candle_h3 | prove | `avoid_J_ge0` | **PROVISIONAL** | 100.0% (3/3 fires) | 81.8% n=11 | 72.7% n=11 | +1.57 pp |
| union_w_hot_candle_h3 | prove | `elev_cap2_J_le-1` | **PROVISIONAL** | 66.7% (2/3 fires) | 58.8% n=17 | 58.8% n=17 | +0.12 pp |
| union_w_hot_cond_h1 | prove | `avoid_J_ge0` | **PROVISIONAL** | 100.0% (3/3 fires) | 85.7% n=7 | 71.4% n=7 | +2.29 pp |
| union_w_hot_cond_h1 | prove | `elev_cap2_J_le-1` | **PROVISIONAL** | 75.0% (3/4 fires) | 61.9% n=21 | 66.7% n=21 | +0.93 pp |
| green_book_prior | prove | `avoid_J_ge0` | **FAIL** | 37.5% (3/8 fires) | 39.3% n=61 | 31.1% n=61 | -0.39 pp |
| green_book_prior | prove | `elev_cap2_J_le-1` | **FAIL** | 37.5% (3/8 fires) | 37.5% n=64 | 32.8% n=64 | -0.37 pp |
| green_pile_prior | prove | `avoid_J_ge0` | **FAIL** | 50.0% (4/8 fires) | 44.0% n=50 | 36.0% n=50 | +0.20 pp |
| green_pile_prior | prove | `elev_cap2_J_le-1` | **PROVISIONAL** | 62.5% (5/8 fires) | 42.6% n=61 | 41.0% n=61 | +0.14 pp |
| weighted_book_1d_prior | prove | `avoid_J_ge0` | **PROVISIONAL** | 75.0% (6/8 fires) | 51.4% n=37 | 45.9% n=37 | +0.49 pp |
| weighted_book_1d_prior | prove | `elev_cap2_J_le-1` | **PROVISIONAL** | 66.7% (2/3 fires) | 37.5% n=64 | 48.4% n=64 | +0.06 pp |
| unweighted_book_prior | prove | `avoid_J_ge0` | **FAIL** | 37.5% (3/8 fires) | 39.3% n=61 | 31.1% n=61 | -0.39 pp |
| unweighted_book_prior | prove | `elev_cap2_J_le-1` | **FAIL** | 37.5% (3/8 fires) | 37.5% n=64 | 32.8% n=64 | -0.37 pp |
| 1d_size | prove | `avoid_J_ge0` | **PROVISIONAL** | 100.0% (2/2 fires) | 75.0% n=8 | 87.5% n=8 | +1.95 pp |
| 1d_size | prove | `elev_cap2_J_le-1` | **PROVISIONAL** | 100.0% (6/6 fires) | 73.7% n=19 | 84.2% n=19 | +0.72 pp |
| 1d_top | prove | `avoid_J_ge0` | **PROVISIONAL** | 100.0% (3/3 fires) | 75.0% n=12 | 50.0% n=12 | +0.81 pp |
| 1d_top | prove | `elev_cap2_J_le-1` | **FAIL** | 50.0% (3/6 fires) | 68.4% n=38 | 71.1% n=38 | +0.17 pp |
| 3d_size | prove | `avoid_J_ge0` | **PROVISIONAL** | 100.0% (3/3 fires) | 100.0% n=9 | 100.0% n=9 | +2.31 pp |
| 3d_size | prove | `elev_cap2_J_le-1` | **FAIL** | 33.3% (2/6 fires) | 68.0% n=25 | 88.0% n=25 | -0.07 pp |
| 3d_top | prove | `avoid_J_ge0` | **PROVISIONAL** | 66.7% (2/3 fires) | 100.0% n=9 | 77.8% n=9 | +1.32 pp |
| 3d_top | prove | `elev_cap2_J_le-1` | **FAIL** | 40.0% (2/5 fires) | 75.0% n=28 | 89.3% n=28 | +0.05 pp |
| sleeve_combine_bt | pooled | `avoid_J_ge0` | **FAIL** | 50.0% (1/2 fires) | 100.0% n=7 | 100.0% n=7 | +1.62 pp |
| sleeve_combine_bt | pooled | `elev_cap2_J_le-1` | **FAIL** | 50.0% (2/4 fires) | 58.3% n=24 | 91.7% n=24 | -0.02 pp |
| book_paper_1w | pooled | `avoid_J_ge0` | **PROVISIONAL** | 100.0% (1/1 fires) | 100.0% n=5 | 60.0% n=5 | +1.12 pp |
| book_paper_1w | pooled | `elev_cap2_J_le-1` | **FAIL** | 50.0% (1/2 fires) | 81.2% n=16 | 56.2% n=16 | +0.24 pp |
| mover_paper_live | pooled | `avoid_J_ge0` | **PROVISIONAL** | 100.0% (3/3 fires) | 45.5% n=11 | 36.4% n=11 | +0.25 pp |
| mover_paper_live | pooled | `elev_cap2_J_le-1` | **PROVISIONAL** | 66.7% (2/3 fires) | 52.2% n=23 | 47.8% n=23 | +0.06 pp |

**Clears >55% with ≥30 fires:** ohlc_liq vol_top8 long `elev_cap2_J_le-1` 55.9% (156/279 fires); ohlc_liq prior_green_top8 long `elev_cap2_J_le-1` 55.7% (141/253 fires); ohlc_liq vol_top8 pre813 `avoid_J_ge0` 60.9% (92/151 fires); ohlc_liq vol_top8 pre813 `elev_cap2_J_le-1` 56.9% (87/153 fires); ohlc_liq prior_green_top8 pre813 `avoid_J_ge0` 56.9% (87/153 fires); ohlc_liq prior_green_top8 pre813 `elev_cap2_J_le-1` 56.8% (83/146 fires); mem_20260426 unranked long `avoid_J_ge0` 71.0% (198/279 fires); mem_20260426 vol_top8 long `avoid_J_ge0` 55.1% (150/272 fires); mem_20260426 vol_top8 long `elev_cap2_J_le-1` 56.6% (158/279 fires); mem_20260426 prior_green_top8 long `elev_cap2_J_le-1` 55.3% (142/257 fires); mem_20260426 prior_green_unranked long `avoid_J_ge0` 80.2% (223/278 fires); mem_20260426 unranked y2025 `avoid_J_ge0` 74.8% (89/119 fires); mem_20260426 prior_green_unranked y2025 `avoid_J_ge0` 88.1% (104/118 fires); mem_20260426 unranked pre813 `avoid_J_ge0` 68.0% (104/153 fires); mem_20260426 vol_top8 pre813 `avoid_J_ge0` 60.3% (91/151 fires); mem_20260426 vol_top8 pre813 `elev_cap2_J_le-1` 58.8% (90/153 fires); mem_20260426 prior_green_top8 pre813 `elev_cap2_J_le-1` 55.1% (81/147 fires); mem_20260426 prior_green_unranked pre813 `avoid_J_ge0` 73.9% (113/153 fires); ohlc_all unranked long `avoid_J_ge0` 61.4% (333/542 fires); ohlc_all unranked y2025 `avoid_J_ge0` 65.1% (142/218 fires); ohlc_all unranked pre813 `avoid_J_ge0` 68.0% (104/153 fires)

**Provisional >55% but n_fires<30 (demoted, not a call):** join top-8 discovery `avoid_J_ge0` 87.5% (7/8 fires); join top-8 discovery `elev_cap2_J_le-1` 75.0% (6/8 fires); join top-8 pooled_sessions `avoid_J_ge0` 68.8% (11/16 fires); join top-8 pooled_sessions `elev_cap2_J_le-1` 62.5% (10/16 fires, 1 tie); union_coil_green_h1 prove `avoid_J_ge0` 75.0% (3/4 fires); union_coil_green_h1 prove `elev_cap2_J_le-1` 75.0% (3/4 fires); union_e_fresh_h1 prove `avoid_J_ge0` 100.0% (3/3 fires); union_e_fresh_h1 prove `elev_cap2_J_le-1` 66.7% (2/3 fires); union_e_green_h3 prove `avoid_J_ge0` 100.0% (3/3 fires); union_e_green_h3 prove `elev_cap2_J_le-1` 66.7% (2/3 fires); union_h1 prove `avoid_J_ge0` 100.0% (2/2 fires); union_h1 prove `elev_cap2_J_le-1` 75.0% (3/4 fires); union_h1_cut prove `avoid_J_ge0` 100.0% (2/2 fires); union_h1_cut prove `elev_cap2_J_le-1` 75.0% (3/4 fires); union_h1_half prove `avoid_J_ge0` 100.0% (2/2 fires); union_h1_half prove `elev_cap2_J_le-1` 75.0% (3/4 fires); union_h1_rankw prove `avoid_J_ge0` 100.0% (2/2 fires); union_h1_rankw prove `elev_cap2_J_le-1` 75.0% (3/4 fires); union_h1_sboost prove `avoid_J_ge0` 100.0% (2/2 fires); union_h1_sboost prove `elev_cap2_J_le-1` 75.0% (3/4 fires); union_h1_sizeup prove `avoid_J_ge0` 100.0% (2/2 fires); union_h1_sizeup prove `elev_cap2_J_le-1` 75.0% (3/4 fires); union_h1_time prove `avoid_J_ge0` 66.7% (2/3 fires); union_h1_time prove `elev_cap2_J_le-1` 75.0% (3/4 fires); union_h1_topheavy prove `avoid_J_ge0` 100.0% (2/2 fires); union_h1_topheavy prove `elev_cap2_J_le-1` 75.0% (3/4 fires); union_h1_trail prove `avoid_J_ge0` 100.0% (2/2 fires); union_h1_trail prove `elev_cap2_J_le-1` 75.0% (3/4 fires); union_h3_half prove `avoid_J_ge0` 100.0% (2/2 fires); union_h3_half prove `elev_cap2_J_le-1` 75.0% (3/4 fires); union_h3_time prove `avoid_J_ge0` 66.7% (2/3 fires); union_h3_time prove `elev_cap2_J_le-1` 75.0% (3/4 fires); union_hot_n12_h1 prove `avoid_J_ge0` 100.0% (3/3 fires); union_hot_n12_h1 prove `elev_cap2_J_le-1` 75.0% (3/4 fires); union_hot_score_h1 prove `avoid_J_ge0` 100.0% (3/3 fires); union_hot_score_h1 prove `elev_cap2_J_le-1` 75.0% (3/4 fires); union_join_present_h1 prove `avoid_J_ge0` 100.0% (2/2 fires); union_join_present_h1 prove `elev_cap2_J_le-1` 75.0% (3/4 fires); union_news_present_h1 prove `avoid_J_ge0` 66.7% (2/3 fires); union_news_present_h1 prove `elev_cap2_J_le-1` 75.0% (3/4 fires); union_ret_5_h1 prove `elev_cap2_J_le-1` 75.0% (3/4 fires); union_ret_5_h3 prove `elev_cap2_J_le-1` 75.0% (3/4 fires); union_w_hot_candle_h1 prove `avoid_J_ge0` 100.0% (3/3 fires); union_w_hot_candle_h1 prove `elev_cap2_J_le-1` 75.0% (3/4 fires); union_w_hot_candle_h3 prove `avoid_J_ge0` 100.0% (3/3 fires); union_w_hot_candle_h3 prove `elev_cap2_J_le-1` 66.7% (2/3 fires); union_w_hot_cond_h1 prove `avoid_J_ge0` 100.0% (3/3 fires); union_w_hot_cond_h1 prove `elev_cap2_J_le-1` 75.0% (3/4 fires); green_pile_prior prove `elev_cap2_J_le-1` 62.5% (5/8 fires); weighted_book_1d_prior prove `avoid_J_ge0` 75.0% (6/8 fires); weighted_book_1d_prior prove `elev_cap2_J_le-1` 66.7% (2/3 fires); 1d_size prove `avoid_J_ge0` 100.0% (2/2 fires); 1d_size prove `elev_cap2_J_le-1` 100.0% (6/6 fires); 1d_top prove `avoid_J_ge0` 100.0% (3/3 fires); 3d_size prove `avoid_J_ge0` 100.0% (3/3 fires); 3d_top prove `avoid_J_ge0` 66.7% (2/3 fires); book_paper_1w pooled `avoid_J_ge0` 100.0% (1/1 fires); mover_paper_live pooled `avoid_J_ge0` 100.0% (3/3 fires); mover_paper_live pooled `elev_cap2_J_le-1` 66.7% (2/3 fires)

Native weighted-book avoid (6/8) and green-pile elev (5/8) stay **PROVISIONAL** — Finviz/join/stock_book dumps are ~16 weekdays. Long analogs: `ohlc_liq vol_top8` ≈ weighted book; `ohlc_liq prior_green_top8` ≈ green pile. y2025 does **not** confirm those liquid ranked recipes (47–54%). Unranked `ohlc_all` / `mem_20260426` avoid CLEARs are the already-demoted microcap lottery (H+ ~41%, ghost FAIL on mean edge). After-fee H is the equal-weight day-mean minus 15 bp Futubull, not dollar-weighted. Do not wire.


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

Dashboard sleeves (open J only): 0 KEEP / 68 KILL / 83 CONDITIONAL / 88 null of 239 scored rows. Question is after-fee H vs that sleeve alone. Live flatten_robust stays frozen. flatten_robust: **KILL** — pooled avoid -0.59 pp n=11 fire 50.0% (2/4 fires); elev +0.19 pp n=23 fire 50.0% (2/4 fires). live tickets; io often 16:00 / 3d. Blanket J-avoid fights mover gap-up. flatten_h5: **KILL** — pooled avoid -1.26 pp n=10 fire 50.0% (3/6 fires); elev -0.63 pp n=19 fire 33.3% (2/6 fires). factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for context, overlay is on fills (cash book) flatten_live_h5: **null** — pooled avoid -2.09 pp n=2 fire 50.0% (1/2 fires); elev -1.61 pp n=9 fire 0.0% (0/2 fires). factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for context, overlay is on fills (cash book) green_pile_prior: **KILL** — prove avoid +0.20 pp n=50 fire 50.0% (4/8 fires); elev +0.14 pp n=61 fire 62.5% (5/8 fires). prior-day green live_buy with in_pile=true (PIT). green_book_prior: **KILL** — prove avoid -0.39 pp n=61 fire 37.5% (3/8 fires); elev -0.37 pp n=64 fire 37.5% (3/8 fires). prior-day green.json live_buy (PIT). Afternoon stamp is not a same-day open feature. weighted_book_1d_prior: **CONDITIONAL** — prove avoid +0.49 pp n=37 fire 75.0% (6/8 fires); elev +0.06 pp n=64 fire 66.7% (2/3 fires). prior-day stock_book 1d buy (weighted). Same clock as book_1d_prior universe. unweighted_book_prior: **KILL** — prove avoid -0.39 pp n=61 fire 37.5% (3/8 fires); elev -0.37 pp n=64 fire 37.5% (3/8 fires). prior-day unweighted.json live_buy (PIT).

KEEP = prove window ≥20 bp after fees vs that sleeve’s own morning picks, ghost pass, H>0. CONDITIONAL = edge only in pooled/discovery, ghost fail, or clock/sleeve-P&L disagreement. KILL = no 20 bp edge. null = n<15, shorts, excel/`strategies/` frozen, or no fills.

### Featured

| sleeve | family | verdict | n | avoid vs | avoid fire>55 | elev vs | elev fire>55 | H+ | I+ |
|---|---|---|---:|---:|---|---:|---|---|---|
| `flatten_robust` | sleeve merge | **KILL** | 11 | -0.59 pp | 50.0% (2/4 fires) FAIL | +0.19 pp | 50.0% (2/4 fires) FAIL | 45.5% n=11 | 54.5% n=11 |
| `flatten_h5` | factor mine | **KILL** | 10 | -1.26 pp | 50.0% (3/6 fires) FAIL | -0.63 pp | 33.3% (2/6 fires) FAIL | 50.0% n=10 | 60.0% n=10 |
| `flatten_live_h5` | factor mine | **null** | 2 | -2.09 pp | 50.0% (1/2 fires) FAIL | -1.61 pp | 0.0% (0/2 fires) FAIL | 50.0% n=2 | 50.0% n=2 |
| `green_book_prior` | stock book | **KILL** | 61 | -0.39 pp | 37.5% (3/8 fires) FAIL | -0.37 pp | 37.5% (3/8 fires) FAIL | 39.3% n=61 | 31.1% n=61 |
| `green_pile_prior` | stock book | **KILL** | 50 | +0.20 pp | 50.0% (4/8 fires) FAIL | +0.14 pp | 62.5% (5/8 fires) PROVISIONAL | 44.0% n=50 | 36.0% n=50 |
| `weighted_book_1d_prior` | stock book | **CONDITIONAL** | 37 | +0.49 pp | 75.0% (6/8 fires) PROVISIONAL | +0.06 pp | 66.7% (2/3 fires) PROVISIONAL | 51.4% n=37 | 45.9% n=37 |
| `unweighted_book_prior` | stock book | **KILL** | 61 | -0.39 pp | 37.5% (3/8 fires) FAIL | -0.37 pp | 37.5% (3/8 fires) FAIL | 39.3% n=61 | 31.1% n=61 |

### Plain English per featured sleeve

- flatten_robust: **KILL** — pooled avoid -0.59 pp n=11 fire 50.0% (2/4 fires); elev +0.19 pp n=23 fire 50.0% (2/4 fires). live tickets; io often 16:00 / 3d. Blanket J-avoid fights mover gap-up.
- flatten_h5: **KILL** — pooled avoid -1.26 pp n=10 fire 50.0% (3/6 fires); elev -0.63 pp n=19 fire 33.3% (2/6 fires). factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for context, overlay is on fills (cash book)
- flatten_live_h5: **null** — pooled avoid -2.09 pp n=2 fire 50.0% (1/2 fires); elev -1.61 pp n=9 fire 0.0% (0/2 fires). factor-mine 09:30 BUY fills vs same-day H after fees; wishlist exists for context, overlay is on fills (cash book)
- green_pile_prior: **KILL** — prove avoid +0.20 pp n=50 fire 50.0% (4/8 fires); elev +0.14 pp n=61 fire 62.5% (5/8 fires). prior-day green live_buy with in_pile=true (PIT).
- green_book_prior: **KILL** — prove avoid -0.39 pp n=61 fire 37.5% (3/8 fires); elev -0.37 pp n=64 fire 37.5% (3/8 fires). prior-day green.json live_buy (PIT). Afternoon stamp is not a same-day open feature.
- weighted_book_1d_prior: **CONDITIONAL** — prove avoid +0.49 pp n=37 fire 75.0% (6/8 fires); elev +0.06 pp n=64 fire 66.7% (2/3 fires). prior-day stock_book 1d buy (weighted). Same clock as book_1d_prior universe.
- unweighted_book_prior: **KILL** — prove avoid -0.39 pp n=61 fire 37.5% (3/8 fires); elev -0.37 pp n=64 fire 37.5% (3/8 fires). prior-day unweighted.json live_buy (PIT).

### All STRATEGY_BOARD / dashboard sleeves

| sleeve | family | verdict | n | avoid vs | avoid fire>55 | elev vs | elev fire>55 | H+ | I+ |
|---|---|---|---:|---:|---|---:|---|---|---|
| `flatten_robust` | sleeve merge | **KILL** | 11 | -0.59 pp | 50.0% (2/4 fires) FAIL | +0.19 pp | 50.0% (2/4 fires) FAIL | 45.5% n=11 | 54.5% n=11 |
| `coil_h3_exit_alarm` | factor mine | **CONDITIONAL** | 4 | +5.43 pp | 50.0% (1/2 fires) FAIL | +0.77 pp | 66.7% (2/3 fires) PROVISIONAL | 50.0% n=4 | 50.0% n=4 |
| `flatten_h1` | factor mine | **KILL** | 15 | -1.13 pp | 42.9% (3/7 fires) FAIL | -0.06 pp | 37.5% (3/8 fires) FAIL | 46.7% n=15 | 53.3% n=15 |
| `flatten_h3` | factor mine | **KILL** | 13 | -1.51 pp | 50.0% (3/6 fires) FAIL | -0.90 pp | 33.3% (2/6 fires) FAIL | 38.5% n=13 | 46.2% n=13 |
| `flatten_h3_cut` | factor mine | **KILL** | 13 | -1.51 pp | 50.0% (3/6 fires) FAIL | -0.90 pp | 33.3% (2/6 fires) FAIL | 38.5% n=13 | 46.2% n=13 |
| `flatten_h3_half` | factor mine | **KILL** | 14 | -1.19 pp | 42.9% (3/7 fires) FAIL | -0.10 pp | 37.5% (3/8 fires) FAIL | 42.9% n=14 | 50.0% n=14 |
| `flatten_h3_rankw` | factor mine | **KILL** | 12 | -1.44 pp | 42.9% (3/7 fires) FAIL | -0.84 pp | 28.6% (2/7 fires) FAIL | 41.7% n=12 | 50.0% n=12 |
| `flatten_h3_sboost` | factor mine | **KILL** | 13 | -1.53 pp | 42.9% (3/7 fires) FAIL | -0.92 pp | 28.6% (2/7 fires) FAIL | 38.5% n=13 | 46.2% n=13 |
| `flatten_h3_sizeup` | factor mine | **KILL** | 13 | -1.51 pp | 50.0% (3/6 fires) FAIL | -0.90 pp | 33.3% (2/6 fires) FAIL | 38.5% n=13 | 46.2% n=13 |
| `flatten_h3_time` | factor mine | **KILL** | 14 | -1.45 pp | 50.0% (3/6 fires) FAIL | -0.83 pp | 33.3% (2/6 fires) FAIL | 35.7% n=14 | 42.9% n=14 |
| `flatten_h3_topheavy` | factor mine | **KILL** | 13 | -0.92 pp | 57.1% (4/7 fires) PROVISIONAL | -0.06 pp | 42.9% (3/7 fires) FAIL | 38.5% n=13 | 46.2% n=13 |
| `flatten_h3_trail` | factor mine | **KILL** | 13 | -1.51 pp | 50.0% (3/6 fires) FAIL | -0.90 pp | 33.3% (2/6 fires) FAIL | 38.5% n=13 | 46.2% n=13 |
| `flatten_h5` | factor mine | **KILL** | 10 | -1.26 pp | 50.0% (3/6 fires) FAIL | -0.63 pp | 33.3% (2/6 fires) FAIL | 50.0% n=10 | 60.0% n=10 |
| `flatten_h5_cut` | factor mine | **KILL** | 10 | -1.26 pp | 50.0% (3/6 fires) FAIL | -0.63 pp | 33.3% (2/6 fires) FAIL | 50.0% n=10 | 60.0% n=10 |
| `flatten_h5_half` | factor mine | **KILL** | 13 | -1.19 pp | 42.9% (3/7 fires) FAIL | -0.09 pp | 37.5% (3/8 fires) FAIL | 46.2% n=13 | 53.8% n=13 |
| `flatten_h5_rankw` | factor mine | **KILL** | 9 | -0.85 pp | 57.1% (4/7 fires) PROVISIONAL | -0.02 pp | 37.5% (3/8 fires) FAIL | 55.6% n=9 | 66.7% n=9 |
| `flatten_h5_sboost` | factor mine | **KILL** | 10 | -1.28 pp | 42.9% (3/7 fires) FAIL | -0.64 pp | 28.6% (2/7 fires) FAIL | 50.0% n=10 | 60.0% n=10 |
| `flatten_h5_sizeup` | factor mine | **KILL** | 10 | -1.26 pp | 50.0% (3/6 fires) FAIL | -0.63 pp | 33.3% (2/6 fires) FAIL | 50.0% n=10 | 60.0% n=10 |
| `flatten_h5_time` | factor mine | **KILL** | 10 | -1.26 pp | 50.0% (3/6 fires) FAIL | -0.63 pp | 33.3% (2/6 fires) FAIL | 50.0% n=10 | 60.0% n=10 |
| `flatten_h5_topheavy` | factor mine | **KILL** | 11 | -1.57 pp | 42.9% (3/7 fires) FAIL | -0.82 pp | 28.6% (2/7 fires) FAIL | 45.5% n=11 | 54.5% n=11 |
| `flatten_h5_trail` | factor mine | **KILL** | 10 | -1.26 pp | 50.0% (3/6 fires) FAIL | -0.63 pp | 33.3% (2/6 fires) FAIL | 50.0% n=10 | 60.0% n=10 |
| `flatten_live_h1` | factor mine | **null** | 2 | -2.01 pp | 50.0% (1/2 fires) FAIL | +0.31 pp | 50.0% (1/2 fires) FAIL | 50.0% n=2 | 50.0% n=2 |
| `flatten_live_h1_cut` | factor mine | **null** | 2 | -2.01 pp | 50.0% (1/2 fires) FAIL | +0.31 pp | 50.0% (1/2 fires) FAIL | 50.0% n=2 | 50.0% n=2 |
| `flatten_live_h1_half` | factor mine | **null** | 2 | -2.01 pp | 50.0% (1/2 fires) FAIL | +0.31 pp | 50.0% (1/2 fires) FAIL | 50.0% n=2 | 50.0% n=2 |
| `flatten_live_h1_rankw` | factor mine | **null** | 2 | -2.01 pp | 50.0% (1/2 fires) FAIL | +0.31 pp | 50.0% (1/2 fires) FAIL | 50.0% n=2 | 50.0% n=2 |
| `flatten_live_h1_sboost` | factor mine | **null** | 2 | -2.01 pp | 50.0% (1/2 fires) FAIL | +0.31 pp | 50.0% (1/2 fires) FAIL | 50.0% n=2 | 50.0% n=2 |
| `flatten_live_h1_sizeup` | factor mine | **null** | 2 | -2.01 pp | 50.0% (1/2 fires) FAIL | +0.31 pp | 50.0% (1/2 fires) FAIL | 50.0% n=2 | 50.0% n=2 |
| `flatten_live_h1_time` | factor mine | **null** | 2 | -2.01 pp | 50.0% (1/2 fires) FAIL | +0.31 pp | 50.0% (1/2 fires) FAIL | 50.0% n=2 | 50.0% n=2 |
| `flatten_live_h1_topheavy` | factor mine | **null** | 2 | -2.01 pp | 50.0% (1/2 fires) FAIL | +0.31 pp | 50.0% (1/2 fires) FAIL | 50.0% n=2 | 50.0% n=2 |
| `flatten_live_h1_trail` | factor mine | **null** | 2 | -2.01 pp | 50.0% (1/2 fires) FAIL | +0.31 pp | 50.0% (1/2 fires) FAIL | 50.0% n=2 | 50.0% n=2 |
| `flatten_live_h3` | factor mine | **null** | 2 | -2.09 pp | 50.0% (1/2 fires) FAIL | -1.61 pp | 0.0% (0/2 fires) FAIL | 50.0% n=2 | 50.0% n=2 |
| `flatten_live_h5` | factor mine | **null** | 2 | -2.09 pp | 50.0% (1/2 fires) FAIL | -1.61 pp | 0.0% (0/2 fires) FAIL | 50.0% n=2 | 50.0% n=2 |
| `flatten_vol_g_h3` | factor mine | **null** | 4 | -1.62 pp | 50.0% (1/2 fires) FAIL | -0.86 pp | 0.0% (0/2 fires) FAIL | 50.0% n=4 | 50.0% n=4 |
| `ohlc_hot_coil_h1` | factor mine | **null** | 2 | +0.08 pp | 100.0% (1/1 fires) PROVISIONAL | -0.47 pp | 25.0% (1/4 fires) FAIL | 50.0% n=2 | 50.0% n=2 |
| `ohlc_hot_h1` | factor mine | **KILL** | 8 | +0.94 pp | 66.7% (2/3 fires) PROVISIONAL | +0.16 pp | 42.9% (3/7 fires) FAIL | 75.0% n=8 | 50.0% n=8 |
| `ohlc_hot_h3` | factor mine | **KILL** | 5 | +0.31 pp | 50.0% (1/2 fires) FAIL | -0.08 pp | 33.3% (2/6 fires) FAIL | 60.0% n=5 | 20.0% n=5 |
| `ohlc_hot_h5` | factor mine | **KILL** | 4 | +0.58 pp | 66.7% (2/3 fires) PROVISIONAL | -0.43 pp | 33.3% (2/6 fires) FAIL | 50.0% n=4 | 25.0% n=4 |
| `probable_h1` | factor mine | **CONDITIONAL** | 2 | +17.66 pp | 100.0% (1/1 fires) PROVISIONAL | +1.25 pp | 100.0% (3/3 fires) PROVISIONAL | 50.0% n=2 | 50.0% n=2 |
| `probable_h3` | factor mine | **CONDITIONAL** | 6 | +6.14 pp | 50.0% (2/4 fires) FAIL | +0.94 pp | 100.0% (7/7 fires) PROVISIONAL | 50.0% n=6 | 50.0% n=6 |
| `probable_h5` | factor mine | **CONDITIONAL** | 2 | +17.87 pp | 100.0% (1/1 fires) PROVISIONAL | +1.33 pp | 100.0% (3/3 fires) PROVISIONAL | 50.0% n=2 | 50.0% n=2 |
| `probable_probable_ok_h1` | factor mine | **CONDITIONAL** | 2 | +17.66 pp | 100.0% (1/1 fires) PROVISIONAL | +1.05 pp | 75.0% (6/8 fires) PROVISIONAL | 50.0% n=2 | 50.0% n=2 |
| `probable_probable_ok_h3` | factor mine | **CONDITIONAL** | 2 | +17.75 pp | 100.0% (1/1 fires) PROVISIONAL | +1.62 pp | 85.7% (6/7 fires) PROVISIONAL | 50.0% n=2 | 50.0% n=2 |
| `short_alarm_h1` | factor mine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `short_alarm_h3` | factor mine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `short_extended_h1` | factor mine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `short_extended_h3` | factor mine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `short_last_red_h1` | factor mine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `short_last_red_h3` | factor mine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `short_news_r_h1` | factor mine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `short_news_r_h3` | factor mine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `short_r_down_h1` | factor mine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `short_r_down_h3` | factor mine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `union_ab_g_h1` | factor mine | **CONDITIONAL** | 4 | +0.60 pp | 50.0% (1/2 fires) FAIL | +0.20 pp | 75.0% (3/4 fires) PROVISIONAL | 75.0% n=4 | 75.0% n=4 |
| `union_ab_g_h3` | factor mine | **KILL** | 3 | +0.57 pp | 50.0% (1/2 fires) FAIL | +0.02 pp | 50.0% (2/4 fires) FAIL | 66.7% n=3 | 66.7% n=3 |
| `union_blue_coil_h1` | factor mine | **CONDITIONAL** | 3 | +3.24 pp | 50.0% (1/2 fires) FAIL | +1.22 pp | 100.0% (4/4 fires) PROVISIONAL | 66.7% n=3 | 66.7% n=3 |
| `union_blue_coil_h3` | factor mine | **CONDITIONAL** | 2 | +2.44 pp | 0.0% (0/1 fires) FAIL | +1.47 pp | 100.0% (3/3 fires) PROVISIONAL | 50.0% n=2 | 50.0% n=2 |
| `union_blue_h1` | factor mine | **CONDITIONAL** | 3 | +2.64 pp | 50.0% (1/2 fires) FAIL | +1.33 pp | 100.0% (4/4 fires) PROVISIONAL | 66.7% n=3 | 66.7% n=3 |
| `union_blue_h3` | factor mine | **CONDITIONAL** | 2 | +1.74 pp | 0.0% (0/1 fires) FAIL | +1.42 pp | 100.0% (3/3 fires) PROVISIONAL | 50.0% n=2 | 50.0% n=2 |
| `union_blue_vol_h1` | factor mine | **KILL** | 2 | +3.38 pp | 100.0% (1/1 fires) PROVISIONAL | +0.81 pp | 33.3% (1/3 fires) FAIL | 50.0% n=2 | 50.0% n=2 |
| `union_blue_vol_h3` | factor mine | **KILL** | 2 | +3.77 pp | 100.0% (1/1 fires) PROVISIONAL | +0.75 pp | 33.3% (1/3 fires) FAIL | 50.0% n=2 | 50.0% n=2 |
| `union_break10_h1` | factor mine | **CONDITIONAL** | 6 | +1.08 pp | 50.0% (1/2 fires) FAIL | +0.57 pp | 100.0% (3/3 fires) PROVISIONAL | 83.3% n=6 | 66.7% n=6 |
| `union_break10_h3` | factor mine | **CONDITIONAL** | 10 | -0.03 pp | 75.0% (3/4 fires) PROVISIONAL | +0.61 pp | 80.0% (4/5 fires) PROVISIONAL | 60.0% n=10 | 50.0% n=10 |
| `union_candle_h1` | factor mine | **CONDITIONAL** | 6 | +2.70 pp | 50.0% (1/2 fires) FAIL | +0.91 pp | 75.0% (3/4 fires) PROVISIONAL | 83.3% n=6 | 83.3% n=6 |
| `union_candle_h3` | factor mine | **CONDITIONAL** | 5 | +2.10 pp | 50.0% (1/2 fires) FAIL | +1.31 pp | 75.0% (3/4 fires) PROVISIONAL | 80.0% n=5 | 80.0% n=5 |
| `union_candle_score_h1` | factor mine | **CONDITIONAL** | 10 | +0.80 pp | 75.0% (3/4 fires) PROVISIONAL | +0.55 pp | 75.0% (3/4 fires) PROVISIONAL | 70.0% n=10 | 70.0% n=10 |
| `union_candle_score_h3` | factor mine | **CONDITIONAL** | 9 | +0.81 pp | 66.7% (2/3 fires) PROVISIONAL | +0.65 pp | 66.7% (2/3 fires) PROVISIONAL | 66.7% n=9 | 66.7% n=9 |
| `union_catal_present_h1` | factor mine | **null** | 2 | -0.57 pp | n_fires=0 null | -0.57 pp | n_fires=0 null | 50.0% n=2 | 50.0% n=2 |
| `union_catal_present_h3` | factor mine | **null** | 2 | -1.11 pp | n_fires=0 null | -1.11 pp | n_fires=0 null | 50.0% n=2 | 50.0% n=2 |
| `union_coil_green_h1` | factor mine | **CONDITIONAL** | 12 | +1.82 pp | 75.0% (3/4 fires) PROVISIONAL | +0.83 pp | 75.0% (3/4 fires) PROVISIONAL | 58.3% n=12 | 58.3% n=12 |
| `union_coil_green_h3` | factor mine | **CONDITIONAL** | 11 | +0.77 pp | 66.7% (2/3 fires) PROVISIONAL | +0.31 pp | 50.0% (2/4 fires) FAIL | 27.3% n=11 | 36.4% n=11 |
| `union_coil_off_h1` | factor mine | **KILL** | 9 | +2.21 pp | 50.0% (2/4 fires) FAIL | +0.16 pp | 50.0% (2/4 fires) FAIL | 55.6% n=9 | 55.6% n=9 |
| `union_coil_off_h3` | factor mine | **CONDITIONAL** | 8 | +2.95 pp | 50.0% (2/4 fires) FAIL | +0.37 pp | 66.7% (4/6 fires) PROVISIONAL | 37.5% n=8 | 50.0% n=8 |
| `union_coil_off_h5` | factor mine | **CONDITIONAL** | 8 | +2.66 pp | 50.0% (2/4 fires) FAIL | +0.26 pp | 50.0% (2/4 fires) FAIL | 62.5% n=8 | 62.5% n=8 |
| `union_cond_h1` | factor mine | **CONDITIONAL** | 2 | +5.41 pp | 100.0% (2/2 fires) PROVISIONAL | +0.64 pp | 100.0% (4/4 fires) PROVISIONAL | 100.0% n=2 | 100.0% n=2 |
| `union_cond_h3` | factor mine | **CONDITIONAL** | 1 | +4.63 pp | 100.0% (1/1 fires) PROVISIONAL | +0.88 pp | 100.0% (3/3 fires) PROVISIONAL | 100.0% n=1 | 100.0% n=1 |
| `union_cond_n4_h3` | factor mine | **null** | 6 | -1.30 pp | 50.0% (1/2 fires) FAIL | +0.46 pp | 50.0% (2/4 fires) FAIL | 50.0% n=6 | 33.3% n=6 |
| `union_e_fresh_h1` | factor mine | **CONDITIONAL** | 10 | +0.57 pp | 100.0% (3/3 fires) PROVISIONAL | +0.02 pp | 66.7% (2/3 fires) PROVISIONAL | 50.0% n=10 | 70.0% n=10 |
| `union_e_fresh_h3` | factor mine | **KILL** | 7 | +0.65 pp | 100.0% (3/3 fires) PROVISIONAL | -0.17 pp | 66.7% (2/3 fires) PROVISIONAL | 42.9% n=7 | 71.4% n=7 |
| `union_e_green_h1` | factor mine | **KILL** | 11 | -0.31 pp | 100.0% (3/3 fires) PROVISIONAL | -0.06 pp | 66.7% (2/3 fires) PROVISIONAL | 54.5% n=11 | 54.5% n=11 |
| `union_e_green_h3` | factor mine | **CONDITIONAL** | 9 | -0.27 pp | 100.0% (3/3 fires) PROVISIONAL | -0.08 pp | 66.7% (2/3 fires) PROVISIONAL | 55.6% n=9 | 55.6% n=9 |
| `union_earn_react_h1` | factor mine | **KILL** | 10 | +0.57 pp | 100.0% (3/3 fires) PROVISIONAL | +0.02 pp | 66.7% (2/3 fires) PROVISIONAL | 50.0% n=10 | 70.0% n=10 |
| `union_earn_react_h3` | factor mine | **KILL** | 9 | +0.49 pp | 100.0% (3/3 fires) PROVISIONAL | -0.03 pp | 66.7% (2/3 fires) PROVISIONAL | 44.4% n=9 | 66.7% n=9 |
| `union_h1` | factor mine | **CONDITIONAL** | 5 | +6.20 pp | 100.0% (2/2 fires) PROVISIONAL | +1.01 pp | 75.0% (3/4 fires) PROVISIONAL | 80.0% n=5 | 80.0% n=5 |
| `union_h1_cut` | factor mine | **CONDITIONAL** | 5 | +6.20 pp | 100.0% (2/2 fires) PROVISIONAL | +1.01 pp | 75.0% (3/4 fires) PROVISIONAL | 80.0% n=5 | 80.0% n=5 |
| `union_h1_half` | factor mine | **CONDITIONAL** | 5 | +6.13 pp | 100.0% (2/2 fires) PROVISIONAL | +1.12 pp | 75.0% (3/4 fires) PROVISIONAL | 80.0% n=5 | 80.0% n=5 |
| `union_h1_rankw` | factor mine | **CONDITIONAL** | 5 | +6.13 pp | 100.0% (2/2 fires) PROVISIONAL | +1.12 pp | 75.0% (3/4 fires) PROVISIONAL | 80.0% n=5 | 80.0% n=5 |
| `union_h1_sboost` | factor mine | **CONDITIONAL** | 5 | +6.20 pp | 100.0% (2/2 fires) PROVISIONAL | +1.01 pp | 75.0% (3/4 fires) PROVISIONAL | 80.0% n=5 | 80.0% n=5 |
| `union_h1_sizeup` | factor mine | **CONDITIONAL** | 5 | +6.20 pp | 100.0% (2/2 fires) PROVISIONAL | +1.01 pp | 75.0% (3/4 fires) PROVISIONAL | 80.0% n=5 | 80.0% n=5 |
| `union_h1_time` | factor mine | **CONDITIONAL** | 6 | +6.11 pp | 66.7% (2/3 fires) PROVISIONAL | +1.01 pp | 75.0% (3/4 fires) PROVISIONAL | 66.7% n=6 | 66.7% n=6 |
| `union_h1_topheavy` | factor mine | **CONDITIONAL** | 5 | +6.13 pp | 100.0% (2/2 fires) PROVISIONAL | +1.12 pp | 75.0% (3/4 fires) PROVISIONAL | 80.0% n=5 | 80.0% n=5 |
| `union_h1_trail` | factor mine | **CONDITIONAL** | 5 | +6.20 pp | 100.0% (2/2 fires) PROVISIONAL | +1.01 pp | 75.0% (3/4 fires) PROVISIONAL | 80.0% n=5 | 80.0% n=5 |
| `union_h3` | factor mine | **KILL** | 14 | +0.70 pp | 66.7% (4/6 fires) PROVISIONAL | -0.12 pp | 50.0% (4/8 fires) FAIL | 35.7% n=14 | 42.9% n=14 |
| `union_h3_cut` | factor mine | **KILL** | 14 | +0.70 pp | 66.7% (4/6 fires) PROVISIONAL | -0.12 pp | 50.0% (4/8 fires) FAIL | 35.7% n=14 | 42.9% n=14 |
| `union_h3_exit_alarm` | factor mine | **KILL** | 14 | +1.08 pp | 57.1% (4/7 fires) PROVISIONAL | -0.12 pp | 44.4% (4/9 fires) FAIL | 42.9% n=14 | 50.0% n=14 |
| `union_h3_exit_news_r` | factor mine | **KILL** | 13 | +1.04 pp | 71.4% (5/7 fires) PROVISIONAL | +0.04 pp | 55.6% (5/9 fires) PROVISIONAL | 38.5% n=13 | 46.2% n=13 |
| `union_h3_exit_red` | factor mine | **CONDITIONAL** | 7 | +4.73 pp | 66.7% (2/3 fires) PROVISIONAL | +1.35 pp | 66.7% (4/6 fires) PROVISIONAL | 42.9% n=7 | 42.9% n=7 |
| `union_h3_half` | factor mine | **CONDITIONAL** | 5 | +6.13 pp | 100.0% (2/2 fires) PROVISIONAL | +1.12 pp | 75.0% (3/4 fires) PROVISIONAL | 80.0% n=5 | 80.0% n=5 |
| `union_h3_rankw` | factor mine | **KILL** | 13 | +0.72 pp | 57.1% (4/7 fires) PROVISIONAL | -0.19 pp | 37.5% (3/8 fires) FAIL | 38.5% n=13 | 46.2% n=13 |
| `union_h3_sboost` | factor mine | **KILL** | 14 | +0.78 pp | 57.1% (4/7 fires) PROVISIONAL | -0.08 pp | 44.4% (4/9 fires) FAIL | 35.7% n=14 | 42.9% n=14 |
| `union_h3_sizeup` | factor mine | **KILL** | 14 | +0.70 pp | 66.7% (4/6 fires) PROVISIONAL | -0.12 pp | 50.0% (4/8 fires) FAIL | 35.7% n=14 | 42.9% n=14 |
| `union_h3_time` | factor mine | **CONDITIONAL** | 5 | +5.36 pp | 66.7% (2/3 fires) PROVISIONAL | +1.24 pp | 75.0% (3/4 fires) PROVISIONAL | 60.0% n=5 | 60.0% n=5 |
| `union_h3_topheavy` | factor mine | **CONDITIONAL** | 14 | +0.92 pp | 71.4% (5/7 fires) PROVISIONAL | +0.53 pp | 50.0% (4/8 fires) FAIL | 35.7% n=14 | 42.9% n=14 |
| `union_h3_trail` | factor mine | **KILL** | 14 | +0.70 pp | 66.7% (4/6 fires) PROVISIONAL | -0.12 pp | 50.0% (4/8 fires) FAIL | 35.7% n=14 | 42.9% n=14 |
| `union_h5` | factor mine | **CONDITIONAL** | 4 | +8.85 pp | 100.0% (2/2 fires) PROVISIONAL | +0.98 pp | 75.0% (3/4 fires) PROVISIONAL | 100.0% n=4 | 100.0% n=4 |
| `union_h5_cut` | factor mine | **CONDITIONAL** | 4 | +8.85 pp | 100.0% (2/2 fires) PROVISIONAL | +0.98 pp | 75.0% (3/4 fires) PROVISIONAL | 100.0% n=4 | 100.0% n=4 |
| `union_h5_exit_alarm` | factor mine | **KILL** | 4 | +8.48 pp | 100.0% (2/2 fires) PROVISIONAL | +1.33 pp | 75.0% (3/4 fires) PROVISIONAL | 100.0% n=4 | 100.0% n=4 |
| `union_h5_half` | factor mine | **CONDITIONAL** | 4 | +8.51 pp | 100.0% (2/2 fires) PROVISIONAL | +1.31 pp | 75.0% (3/4 fires) PROVISIONAL | 100.0% n=4 | 100.0% n=4 |
| `union_h5_rankw` | factor mine | **CONDITIONAL** | 4 | +8.78 pp | 100.0% (2/2 fires) PROVISIONAL | +1.08 pp | 75.0% (3/4 fires) PROVISIONAL | 100.0% n=4 | 100.0% n=4 |
| `union_h5_sboost` | factor mine | **CONDITIONAL** | 4 | +8.85 pp | 100.0% (2/2 fires) PROVISIONAL | +0.98 pp | 75.0% (3/4 fires) PROVISIONAL | 100.0% n=4 | 100.0% n=4 |
| `union_h5_sizeup` | factor mine | **CONDITIONAL** | 4 | +8.85 pp | 100.0% (2/2 fires) PROVISIONAL | +0.98 pp | 75.0% (3/4 fires) PROVISIONAL | 100.0% n=4 | 100.0% n=4 |
| `union_h5_time` | factor mine | **CONDITIONAL** | 5 | +6.38 pp | 100.0% (2/2 fires) PROVISIONAL | +0.92 pp | 75.0% (3/4 fires) PROVISIONAL | 80.0% n=5 | 80.0% n=5 |
| `union_h5_topheavy` | factor mine | **KILL** | 4 | +8.48 pp | 100.0% (2/2 fires) PROVISIONAL | +1.33 pp | 75.0% (3/4 fires) PROVISIONAL | 100.0% n=4 | 100.0% n=4 |
| `union_h5_trail` | factor mine | **CONDITIONAL** | 4 | +8.85 pp | 100.0% (2/2 fires) PROVISIONAL | +0.98 pp | 75.0% (3/4 fires) PROVISIONAL | 100.0% n=4 | 100.0% n=4 |
| `union_hot_n12_h1` | factor mine | **CONDITIONAL** | 17 | +1.30 pp | 100.0% (3/3 fires) PROVISIONAL | +0.58 pp | 75.0% (3/4 fires) PROVISIONAL | 76.5% n=17 | 64.7% n=17 |
| `union_hot_n4_h1` | factor mine | **KILL** | 11 | +1.46 pp | 100.0% (7/7 fires) PROVISIONAL | +0.83 pp | 77.8% (7/9 fires) PROVISIONAL | 72.7% n=11 | 54.5% n=11 |
| `union_hot_score_h1` | factor mine | **CONDITIONAL** | 11 | +1.75 pp | 100.0% (3/3 fires) PROVISIONAL | +0.85 pp | 75.0% (3/4 fires) PROVISIONAL | 81.8% n=11 | 72.7% n=11 |
| `union_hot_score_h3` | factor mine | **CONDITIONAL** | 10 | +1.91 pp | 100.0% (3/3 fires) PROVISIONAL | +0.87 pp | 75.0% (3/4 fires) PROVISIONAL | 80.0% n=10 | 70.0% n=10 |
| `union_join_g_h1` | factor mine | **KILL** | 4 | +0.90 pp | 50.0% (1/2 fires) FAIL | +0.17 pp | 75.0% (3/4 fires) PROVISIONAL | 75.0% n=4 | 75.0% n=4 |
| `union_join_g_h3` | factor mine | **KILL** | 3 | +1.14 pp | 50.0% (1/2 fires) FAIL | +0.38 pp | 75.0% (3/4 fires) PROVISIONAL | 66.7% n=3 | 66.7% n=3 |
| `union_join_present_h1` | factor mine | **CONDITIONAL** | 5 | +6.20 pp | 100.0% (2/2 fires) PROVISIONAL | +1.01 pp | 75.0% (3/4 fires) PROVISIONAL | 80.0% n=5 | 80.0% n=5 |
| `union_join_present_h3` | factor mine | **KILL** | 13 | +1.04 pp | 71.4% (5/7 fires) PROVISIONAL | +0.04 pp | 55.6% (5/9 fires) PROVISIONAL | 38.5% n=13 | 46.2% n=13 |
| `union_join_vol_green_h1` | factor mine | **KILL** | 4 | +1.02 pp | 50.0% (1/2 fires) FAIL | +0.60 pp | 33.3% (1/3 fires) FAIL | 50.0% n=4 | 50.0% n=4 |
| `union_join_vol_green_h3` | factor mine | **KILL** | 5 | +1.07 pp | 50.0% (1/2 fires) FAIL | -0.10 pp | 14.3% (1/7 fires) FAIL | 40.0% n=5 | 20.0% n=5 |
| `union_last_green_h1` | factor mine | **CONDITIONAL** | 7 | +4.86 pp | 100.0% (2/2 fires) PROVISIONAL | +1.36 pp | 75.0% (3/4 fires) PROVISIONAL | 85.7% n=7 | 85.7% n=7 |
| `union_last_green_h3` | factor mine | **CONDITIONAL** | 7 | +4.73 pp | 66.7% (2/3 fires) PROVISIONAL | +1.35 pp | 66.7% (4/6 fires) PROVISIONAL | 42.9% n=7 | 42.9% n=7 |
| `union_last_green_h5` | factor mine | **CONDITIONAL** | 6 | +6.41 pp | 100.0% (2/2 fires) PROVISIONAL | +1.40 pp | 75.0% (3/4 fires) PROVISIONAL | 100.0% n=6 | 100.0% n=6 |
| `union_last_red_h1` | factor mine | **CONDITIONAL** | 3 | -0.40 pp | 33.3% (1/3 fires) FAIL | +0.27 pp | 75.0% (3/4 fires) PROVISIONAL | 33.3% n=3 | 0.0% n=3 |
| `union_last_red_h3` | factor mine | **KILL** | 1 | +0.85 pp | 0.0% (0/1 fires) FAIL | +0.50 pp | 100.0% (3/3 fires) PROVISIONAL | 100.0% n=1 | 0.0% n=1 |
| `union_news_g_h1` | factor mine | **CONDITIONAL** | 5 | +1.07 pp | 100.0% (2/2 fires) PROVISIONAL | +0.23 pp | 33.3% (1/3 fires) FAIL | 80.0% n=5 | 80.0% n=5 |
| `union_news_g_h3` | factor mine | **CONDITIONAL** | 10 | +1.76 pp | 75.0% (3/4 fires) PROVISIONAL | +0.58 pp | 42.9% (3/7 fires) FAIL | 80.0% n=10 | 90.0% n=10 |
| `union_news_g_h5` | factor mine | **CONDITIONAL** | 9 | +1.74 pp | 50.0% (2/4 fires) FAIL | +0.32 pp | 28.6% (2/7 fires) FAIL | 77.8% n=9 | 88.9% n=9 |
| `union_news_missing_h1` | factor mine | **null** | 2 | +3.76 pp | 100.0% (1/1 fires) PROVISIONAL | +0.55 pp | 100.0% (1/1 fires) PROVISIONAL | 100.0% n=2 | 100.0% n=2 |
| `union_news_missing_h3` | factor mine | **null** | 2 | +3.76 pp | 100.0% (1/1 fires) PROVISIONAL | +0.55 pp | 100.0% (1/1 fires) PROVISIONAL | 100.0% n=2 | 100.0% n=2 |
| `union_news_present_h1` | factor mine | **CONDITIONAL** | 7 | +3.29 pp | 66.7% (2/3 fires) PROVISIONAL | +0.85 pp | 75.0% (3/4 fires) PROVISIONAL | 42.9% n=7 | 42.9% n=7 |
| `union_news_present_h3` | factor mine | **CONDITIONAL** | 4 | +7.14 pp | 100.0% (1/1 fires) PROVISIONAL | +1.04 pp | 66.7% (2/3 fires) PROVISIONAL | 75.0% n=4 | 75.0% n=4 |
| `union_news_vol_h1` | factor mine | **KILL** | 8 | +2.33 pp | 75.0% (3/4 fires) PROVISIONAL | +0.03 pp | 60.0% (3/5 fires) PROVISIONAL | 75.0% n=8 | 75.0% n=8 |
| `union_news_vol_h3` | factor mine | **KILL** | 7 | +1.98 pp | 66.7% (2/3 fires) PROVISIONAL | +0.28 pp | 60.0% (3/5 fires) PROVISIONAL | 71.4% n=7 | 71.4% n=7 |
| `union_r_up_h1` | factor mine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `union_r_up_h3` | factor mine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `union_ret_5_h1` | factor mine | **CONDITIONAL** | 12 | +0.39 pp | 33.3% (1/3 fires) FAIL | +1.02 pp | 75.0% (3/4 fires) PROVISIONAL | 75.0% n=12 | 66.7% n=12 |
| `union_ret_5_h3` | factor mine | **CONDITIONAL** | 12 | +0.36 pp | 33.3% (1/3 fires) FAIL | +1.14 pp | 75.0% (3/4 fires) PROVISIONAL | 75.0% n=12 | 66.7% n=12 |
| `union_vol_ab_h1` | factor mine | **KILL** | 3 | -0.89 pp | 50.0% (1/2 fires) FAIL | -0.42 pp | 33.3% (1/3 fires) FAIL | 33.3% n=3 | 33.3% n=3 |
| `union_vol_ab_h3` | factor mine | **KILL** | 3 | -0.57 pp | 50.0% (1/2 fires) FAIL | -0.59 pp | 33.3% (1/3 fires) FAIL | 33.3% n=3 | 33.3% n=3 |
| `union_vol_g_h1` | factor mine | **CONDITIONAL** | 4 | +6.72 pp | 50.0% (1/2 fires) FAIL | +1.13 pp | 66.7% (2/3 fires) PROVISIONAL | 50.0% n=4 | 50.0% n=4 |
| `union_vol_g_h3` | factor mine | **KILL** | 4 | +7.24 pp | 50.0% (1/2 fires) FAIL | +1.15 pp | 66.7% (2/3 fires) PROVISIONAL | 50.0% n=4 | 50.0% n=4 |
| `union_vol_g_h5` | factor mine | **CONDITIONAL** | 3 | +8.31 pp | 50.0% (1/2 fires) FAIL | +1.19 pp | 66.7% (2/3 fires) PROVISIONAL | 33.3% n=3 | 33.3% n=3 |
| `union_vol_green_h1` | factor mine | **CONDITIONAL** | 4 | +8.17 pp | 50.0% (1/2 fires) FAIL | +0.67 pp | 100.0% (3/3 fires) PROVISIONAL | 50.0% n=4 | 50.0% n=4 |
| `union_vol_green_h3` | factor mine | **CONDITIONAL** | 4 | +8.47 pp | 50.0% (1/2 fires) FAIL | +0.64 pp | 66.7% (2/3 fires) PROVISIONAL | 50.0% n=4 | 50.0% n=4 |
| `union_vol_missing_h1` | factor mine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `union_vol_missing_h3` | factor mine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `union_w_hot_candle_h1` | factor mine | **CONDITIONAL** | 11 | +2.17 pp | 100.0% (3/3 fires) PROVISIONAL | +0.83 pp | 75.0% (3/4 fires) PROVISIONAL | 81.8% n=11 | 72.7% n=11 |
| `union_w_hot_candle_h3` | factor mine | **CONDITIONAL** | 11 | +1.57 pp | 100.0% (3/3 fires) PROVISIONAL | +0.12 pp | 66.7% (2/3 fires) PROVISIONAL | 81.8% n=11 | 72.7% n=11 |
| `union_w_hot_cond_h1` | factor mine | **CONDITIONAL** | 7 | +2.29 pp | 100.0% (3/3 fires) PROVISIONAL | +0.93 pp | 75.0% (3/4 fires) PROVISIONAL | 85.7% n=7 | 71.4% n=7 |
| `union_w_hot_cond_h3` | factor mine | **CONDITIONAL** | 5 | +3.01 pp | 100.0% (3/3 fires) PROVISIONAL | +1.10 pp | 75.0% (3/4 fires) PROVISIONAL | 100.0% n=5 | 80.0% n=5 |
| `union_white_coil_h1` | factor mine | **CONDITIONAL** | 2 | +2.67 pp | 100.0% (1/1 fires) PROVISIONAL | +0.73 pp | 100.0% (3/3 fires) PROVISIONAL | 100.0% n=2 | 100.0% n=2 |
| `union_white_coil_h3` | factor mine | **CONDITIONAL** | 2 | +2.61 pp | 100.0% (1/1 fires) PROVISIONAL | +0.81 pp | 100.0% (2/2 fires) PROVISIONAL | 100.0% n=2 | 100.0% n=2 |
| `union_white_h1` | factor mine | **CONDITIONAL** | 1 | +3.46 pp | 0.0% (0/1 fires) FAIL | +1.05 pp | 100.0% (3/3 fires) PROVISIONAL | 100.0% n=1 | 100.0% n=1 |
| `union_white_h3` | factor mine | **KILL** | 1 | +3.57 pp | 0.0% (0/1 fires) FAIL | +1.16 pp | 100.0% (3/3 fires) PROVISIONAL | 100.0% n=1 | 100.0% n=1 |
| `union_white_h5` | factor mine | **CONDITIONAL** | 0 | — | n_fires=0 null | +0.81 pp | 100.0% (3/3 fires) PROVISIONAL | — | — |
| `yday_gainer_h1` | factor mine | **CONDITIONAL** | 2 | +17.66 pp | 100.0% (1/1 fires) PROVISIONAL | +1.25 pp | 100.0% (3/3 fires) PROVISIONAL | 50.0% n=2 | 50.0% n=2 |
| `yday_gainer_h3` | factor mine | **CONDITIONAL** | 2 | +17.97 pp | 100.0% (1/1 fires) PROVISIONAL | +1.90 pp | 100.0% (3/3 fires) PROVISIONAL | 50.0% n=2 | 50.0% n=2 |
| `yday_gainer_h5` | factor mine | **CONDITIONAL** | 2 | +18.04 pp | 100.0% (1/1 fires) PROVISIONAL | +1.19 pp | 100.0% (3/3 fires) PROVISIONAL | 50.0% n=2 | 50.0% n=2 |
| `green_book_prior` | stock book | **KILL** | 61 | -0.39 pp | 37.5% (3/8 fires) FAIL | -0.37 pp | 37.5% (3/8 fires) FAIL | 39.3% n=61 | 31.1% n=61 |
| `green_pile_prior` | stock book | **KILL** | 50 | +0.20 pp | 50.0% (4/8 fires) FAIL | +0.14 pp | 62.5% (5/8 fires) PROVISIONAL | 44.0% n=50 | 36.0% n=50 |
| `weighted_book_1d_prior` | stock book | **CONDITIONAL** | 37 | +0.49 pp | 75.0% (6/8 fires) PROVISIONAL | +0.06 pp | 66.7% (2/3 fires) PROVISIONAL | 51.4% n=37 | 45.9% n=37 |
| `unweighted_book_prior` | stock book | **KILL** | 61 | -0.39 pp | 37.5% (3/8 fires) FAIL | -0.37 pp | 37.5% (3/8 fires) FAIL | 39.3% n=61 | 31.1% n=61 |
| `1d_size` | .io paper | **CONDITIONAL** | 8 | +1.95 pp | 100.0% (2/2 fires) PROVISIONAL | +0.72 pp | 100.0% (6/6 fires) PROVISIONAL | 75.0% n=8 | 87.5% n=8 |
| `1d_top` | .io paper | **CONDITIONAL** | 12 | +0.81 pp | 100.0% (3/3 fires) PROVISIONAL | +0.17 pp | 50.0% (3/6 fires) FAIL | 75.0% n=12 | 50.0% n=12 |
| `1m_size` | .io paper | **null** | 4 | -0.84 pp | 100.0% (1/1 fires) PROVISIONAL | -0.44 pp | 0.0% (0/1 fires) FAIL | 25.0% n=4 | 25.0% n=4 |
| `1m_top` | .io paper | **null** | 9 | -0.17 pp | 0.0% (0/1 fires) FAIL | -0.46 pp | 0.0% (0/3 fires) FAIL | 33.3% n=9 | 44.4% n=9 |
| `1w_size` | .io paper | **KILL** | 6 | +2.05 pp | 100.0% (2/2 fires) PROVISIONAL | -0.09 pp | 40.0% (2/5 fires) FAIL | 100.0% n=6 | 100.0% n=6 |
| `1w_top` | .io paper | **KILL** | 5 | +0.70 pp | 100.0% (2/2 fires) PROVISIONAL | -0.14 pp | 50.0% (2/4 fires) FAIL | 100.0% n=5 | 60.0% n=5 |
| `2w_size` | .io paper | **CONDITIONAL** | 10 | +1.45 pp | 66.7% (2/3 fires) PROVISIONAL | +0.23 pp | 40.0% (2/5 fires) FAIL | 60.0% n=10 | 60.0% n=10 |
| `2w_top` | .io paper | **null** | 13 | -0.29 pp | 50.0% (1/2 fires) FAIL | -0.25 pp | 50.0% (1/2 fires) FAIL | 53.8% n=13 | 46.2% n=13 |
| `3d_size` | .io paper | **CONDITIONAL** | 9 | +2.31 pp | 100.0% (3/3 fires) PROVISIONAL | -0.07 pp | 33.3% (2/6 fires) FAIL | 100.0% n=9 | 100.0% n=9 |
| `3d_top` | .io paper | **CONDITIONAL** | 9 | +1.32 pp | 66.7% (2/3 fires) PROVISIONAL | +0.05 pp | 40.0% (2/5 fires) FAIL | 100.0% n=9 | 77.8% n=9 |
| `sleeve_combine_bt` | sleeve combine | **KILL** | 7 | +1.62 pp | 50.0% (1/2 fires) FAIL | -0.02 pp | 50.0% (2/4 fires) FAIL | 100.0% n=7 | 100.0% n=7 |
| `sleeve_combine_io` | sleeve combine | **KILL** | 7 | +1.62 pp | 50.0% (1/2 fires) FAIL | -0.02 pp | 50.0% (2/4 fires) FAIL | 100.0% n=7 | 100.0% n=7 |
| `sleeve_combine_mover` | sleeve combine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `book_paper_1w` | book paper | **CONDITIONAL** | 5 | +1.12 pp | 100.0% (1/1 fires) PROVISIONAL | +0.24 pp | 50.0% (1/2 fires) FAIL | 100.0% n=5 | 60.0% n=5 |
| `mover_paper_live` | mover stitch | **KILL** | 11 | +0.25 pp | 100.0% (3/3 fires) PROVISIONAL | +0.06 pp | 66.7% (2/3 fires) PROVISIONAL | 45.5% n=11 | 36.4% n=11 |
| `flatten_rotate` | sleeve merge | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `flatten_switch_recycle` | sleeve merge | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `flatten_hard_red` | sleeve merge | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `flatten_switch_full` | sleeve merge | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `flatten_carry_book` | sleeve merge | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `flatten_cash_mover` | sleeve merge | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `flatten_blank_cash` | sleeve merge | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `flatten_skip_blank_io` | sleeve merge | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `flatten_switch_70` | sleeve merge | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `flatten_rich` | sleeve merge | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `flatten_overlap` | sleeve merge | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `flatten_overlap_55` | sleeve merge | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `flatten_switch_60` | sleeve merge | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `flatten_switch` | sleeve merge | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Combine 3d io_boost` | sleeve combine | **KILL** | 7 | +1.62 pp | 50.0% (1/2 fires) FAIL | -0.02 pp | 50.0% (2/4 fires) FAIL | 100.0% n=7 | 100.0% n=7 |
| `Flatten core50_switch` | sleeve merge | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Flatten concentrated_switch` | sleeve merge | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Combine 3d overlay_boost` | sleeve combine | **KILL** | 7 | +1.62 pp | 50.0% (1/2 fires) FAIL | -0.02 pp | 50.0% (2/4 fires) FAIL | 100.0% n=7 | 100.0% n=7 |
| `Combine 3d io_only` | sleeve combine | **KILL** | 7 | +1.62 pp | 50.0% (1/2 fires) FAIL | -0.02 pp | 50.0% (2/4 fires) FAIL | 100.0% n=7 | 100.0% n=7 |
| `Flatten core_switch` | sleeve merge | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Flatten switch_70` | sleeve merge | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Flatten switch_80` | sleeve merge | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `flatten_3d` | sleeve merge | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Combine 3d overlay` | sleeve combine | **KILL** | 7 | +1.62 pp | 50.0% (1/2 fires) FAIL | -0.02 pp | 50.0% (2/4 fires) FAIL | 100.0% n=7 | 100.0% n=7 |
| `Flatten io_3d_switch` | sleeve merge | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Flatten switch_no_short` | sleeve merge | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Combine 1w overlay_boost` | sleeve combine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Combine 1w io_boost` | sleeve combine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Flatten mover_heavy` | sleeve merge | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Combine 1d overlay_boost` | sleeve combine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Combine 1w io_only` | sleeve combine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Combine 1w overlay` | sleeve combine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Flatten hard_red_shorts` | sleeve merge | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Flatten switch_80_overlap` | sleeve merge | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Combine 1d overlay` | sleeve combine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Combine 3d dual` | sleeve combine | **KILL** | 7 | +1.62 pp | 50.0% (1/2 fires) FAIL | -0.02 pp | 50.0% (2/4 fires) FAIL | 100.0% n=7 | 100.0% n=7 |
| `Flatten switch_90_overlap` | sleeve merge | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Combine 1d io_boost` | sleeve combine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Combine 2w io_only` | sleeve combine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Combine 1d io_only` | sleeve combine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Combine 1w dual` | sleeve combine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Combine 1w mover_only` | sleeve combine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Combine 1w combine` | sleeve combine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Combine 3d mover_only` | sleeve combine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Combine 1d dual` | sleeve combine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Combine 3d combine` | sleeve combine | **KILL** | 7 | +1.62 pp | 50.0% (1/2 fires) FAIL | -0.02 pp | 50.0% (2/4 fires) FAIL | 100.0% n=7 | 100.0% n=7 |
| `1d mover × soft-red 1d .io` | mover stitch | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Combine 1d mover_only` | sleeve combine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Combine 1d combine` | sleeve combine | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Empty BUY list + skip → live 2w_size` | mover stitch | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Skip-day → live 2w_size` | mover stitch | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Mover paper v2 (old sim)` | mover | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Excel L3_long_green_hold2_midcap` | excel | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Excel live ledger (all cards)` | excel | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Excel L1_long_green_tp8_lowvol` | excel | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Excel L2_long_green_tp3_lowvol` | excel | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `.io SPY (benchmark)` | .io paper | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |
| `Excel L5_long_green_hold2_midhibeta` | excel | **null** | 0 | — | n_fires=0  | — | n_fires=0  | — | — |

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

`JOIN_POST_813.md` tip `35695bb2` · PR #153. Gate: `OPEN_SAME_ROW_LABELS.md` / `CLOCK_MAP.md`. Research only. Live frozen.
