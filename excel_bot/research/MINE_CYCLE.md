# Excel emulator mine — A–O clock cycle + scaled all-cols

_Generated 2026-09-07 · live `flatten_robust` frozen. No merge without Cyrus._

## 1. Stored A–O surface (priority)

Grids rebuilt from excel-state rows: **3603** on disk · **3598** mined (`rebuild_grids.py --resume` was 0 remaining). Patterns **103**. SPY tape **191** days. ALL-cohort only (finviz slices skipped — they inflate PASS counts without a new edge).

**PASS 376 · FAIL 1160 · THIN 18.** Sleeve holds 1/2/3/5/8. Costs futubull + mcap_bps. 276 of the PASSes are hold5/8 (same defs as a hold2 sibling on a bull tape).

Ship bar: disc n≥300 t≥3; holdout n≥100 t≥2 same sign; ≥50 tickers; ≥20 dates; lottery; both tape halves; spy↑/↓; hold1 needs hold2; hold3/5/8 need hold2 edge. Leak-free clocks. This miner does **not** require beat-uncond (see `AO_FIRST_MINE.md`: uncond close-long hold2 is +0.66%).

### Primary keepers (ALL × hold1/2 × futubull)

**15** sleeve-shaped PASS cells. Research only — one Jan–Sep 2026 regime. No cards.

| verdict | def | clock | side | exit | disc n/effect | hold n/effect | tape early | tape late | tickers | why |
|---|---|---|---|---|---|---|---|---|---:|---|
| PASS | `hyst_open_core_e5_x2` | open | long | hold1 | 15881/+1.84%/t=30.1 | 10931/+1.74%/t=34.4 | 11306/+1.86%/t=37.0 | 15506/+1.74%/t=28.2 | 3577 | — |
| PASS | `hyst_open_score_e5_x2` | open | long | hold1 | 15778/+1.44%/t=31.8 | 10862/+1.33%/t=27.6 | 12227/+1.66%/t=34.9 | 14413/+1.17%/t=25.3 | 3552 | — |
| PASS | `hyst_open_core_e5_x0` | open | long | hold1 | 16027/+1.35%/t=31.9 | 11120/+1.28%/t=26.4 | 11603/+1.46%/t=30.7 | 15544/+1.23%/t=28.4 | 3585 | — |
| PASS | `hyst_open_core_e5_x2` | open | long | hold2 | 15881/+1.68%/t=18.4 | 10931/+1.62%/t=20.1 | 11306/+1.76%/t=24.3 | 15506/+1.59%/t=16.5 | 3577 | — |
| PASS | `combo_HI_red` | close | short | hold2 | 8563/+6.23%/t=23.1 | 5529/+6.28%/t=18.1 | 6866/+6.65%/t=52.8 | 7226/+5.87%/t=14.8 | 2563 | — |
| PASS | `hyst_open_core_e5_x0` | open | long | hold2 | 16027/+1.24%/t=13.5 | 11120/+1.21%/t=14.6 | 11603/+1.34%/t=18.4 | 15544/+1.14%/t=11.7 | 3585 | — |
| PASS | `h_up_3` | close | long | hold2 | 7684/+8.15%/t=10.0 | 5083/+7.17%/t=12.7 | 6340/+6.35%/t=34.2 | 6427/+9.15%/t=8.7 | 2571 | — |
| PASS | `combo_HI_green` | close | long | hold2 | 7217/+10.00%/t=10.8 | 4706/+8.99%/t=8.6 | 5802/+7.73%/t=39.6 | 6121/+11.38%/t=8.5 | 2667 | — |
| PASS | `h_dn_3` | close | short | hold2 | 9000/+3.98%/t=4.7 | 5801/+4.41%/t=8.2 | 7293/+5.68%/t=41.1 | 7508/+2.67%/t=2.4 | 2509 | — |
| PASS | `hyst_open_score_e5_x2` | open | long | hold2 | 15778/+1.40%/t=13.1 | 10862/+1.54%/t=4.6 | 12227/+1.45%/t=20.8 | 14413/+1.47%/t=5.4 | 3552 | — |
| PASS | `strict_core_score_ml2` | close | long | hold2 | 19277/+0.51%/t=3.8 | 13273/+0.34%/t=4.1 | 16850/+0.30%/t=4.4 | 15700/+0.59%/t=3.6 | 3595 | — |
| PASS | `hyst_core_score_e3_x2` | close | long | hold2 | 19775/+0.49%/t=3.9 | 13689/+0.28%/t=4.0 | 15955/+0.33%/t=5.0 | 17509/+0.48%/t=3.4 | 3585 | — |
| PASS | `tol3_core_score_ml2` | close | long | hold2 | 11927/+0.75%/t=3.5 | 8153/+0.37%/t=3.5 | 10757/+0.49%/t=6.1 | 9323/+0.71%/t=2.6 | 3597 | — |
| PASS | `tol3_row_score_ml2` | close | long | hold2 | 12147/+1.02%/t=3.2 | 8265/+0.33%/t=3.4 | 11112/+0.50%/t=5.3 | 9300/+1.03%/t=2.5 | 3595 | — |
| PASS | `strict_row_score_ml2` | close | long | hold2 | 18297/+0.72%/t=3.3 | 12566/+0.26%/t=3.0 | 15870/+0.26%/t=3.7 | 14993/+0.82%/t=3.1 | 3594 | — |

Hysteresis `open_core` / `open_score` hold1/2 match the A–O first-mine candidates and also beat uncond (open-long hold1 uncond −0.07%). `combo_HI_*` / `h_up_3` / `h_dn_3` are close-entry after same-day H/I; legal PIT, huge hold2 means — still one regime. `core_score` / `row_score` hold2 PASSes are **below** uncond +0.66% (+0.26% to +0.37% holdout) — keep only vs this ship bar, not vs “just hold”.

### Near-miss (FAIL, ALL, futubull, hold1/2, holdout t≥1.5, disc avg>0)

| verdict | def | clock | side | exit | disc n/effect | hold n/effect | tape early | tape late | tickers | why |
|---|---|---|---|---|---|---|---|---|---:|---|
| FAIL | `hyst_open_core_e3_x0` | open | short | hold1 | 17747/+1.37%/t=37.1 | 12116/+1.35%/t=35.9 | 13068/+1.43%/t=37.9 | 16795/+1.30%/t=35.1 | 3593 | hold1_without_hold2 |
| FAIL | `hyst_open_score_e3_x0` | open | short | hold1 | 17728/+1.17%/t=31.3 | 12077/+1.12%/t=29.0 | 13150/+1.28%/t=33.7 | 16655/+1.05%/t=27.3 | 3593 | hold1_without_hold2 |
| FAIL | `hyst_open_score_e5_x2` | open | short | hold1 | 16562/+1.27%/t=38.8 | 11337/+1.19%/t=28.9 | 12184/+1.33%/t=32.8 | 15715/+1.18%/t=35.4 | 3591 | hold1_without_hold2 |
| FAIL | `tol3_open_core_ml2` | open | short | hold1 | 10783/+1.23%/t=31.7 | 7347/+1.15%/t=25.7 | 8329/+1.22%/t=27.6 | 9801/+1.18%/t=30.0 | 3598 | hold1_without_hold2 |
| FAIL | `hyst_open_score_e5_x0` | open | short | hold1 | 15939/+0.99%/t=28.8 | 10871/+0.88%/t=21.2 | 11681/+1.04%/t=25.0 | 15129/+0.87%/t=25.5 | 3592 | hold1_without_hold2 |
| FAIL | `tol3_A_ml2` | open | short | hold1 | 8566/+1.31%/t=28.7 | 5846/+1.14%/t=20.1 | 6355/+1.26%/t=22.4 | 8057/+1.23%/t=26.8 | 3586 | hold1_without_hold2 |
| FAIL | `tol3_open_score_ml2` | open | short | hold1 | 12400/+0.81%/t=22.9 | 8488/+0.75%/t=19.3 | 9558/+0.79%/t=20.3 | 11330/+0.78%/t=22.0 | 3598 | hold1_without_hold2 |
| FAIL | `tol2_open_core_ml3` | open | long | hold1 | 15742/+0.27%/t=5.2 | 10769/+0.37%/t=7.7 | 13402/+0.23%/t=5.8 | 13109/+0.39%/t=6.4 | 3589 | hold1_without_hold2 |
| FAIL | `tol2_open_score_ml3` | open | long | hold1 | 14432/+0.09%/t=2.2 | 9882/+0.30%/t=5.9 | 12556/+0.20%/t=4.6 | 11758/+0.15%/t=3.2 | 3589 | disc_t,spy_regime |
| FAIL | `tol2_A_ml3` | open | long | hold1 | 12135/+0.21%/t=3.2 | 8224/+0.35%/t=5.8 | 10790/+0.13%/t=2.9 | 9569/+0.41%/t=5.0 | 3598 | hold1_without_hold2 |
| FAIL | `tol2_open_core_ml3` | open | long | hold2 | 15704/+1.05%/t=2.5 | 10735/+0.38%/t=5.3 | 13402/+0.48%/t=7.3 | 13037/+1.08%/t=2.1 | 3589 | disc_t |
| FAIL | `strict_core_score_ml3` | close | long | hold2 | 14102/+0.76%/t=2.0 | 9768/+0.42%/t=4.5 | 11887/+0.64%/t=7.7 | 11983/+0.61%/t=1.4 | 3589 | disc_t |
| FAIL | `tol2_A_ml3` | open | short | hold1 | 10637/+0.28%/t=5.8 | 7294/+0.23%/t=3.8 | 7845/+0.23%/t=3.8 | 10086/+0.29%/t=5.9 | 3587 | spy_regime |
| FAIL | `tol2_open_score_ml3` | open | long | hold2 | 14409/+0.83%/t=1.8 | 9852/+0.26%/t=3.6 | 12556/+0.45%/t=5.9 | 11705/+0.76%/t=1.4 | 3589 | disc_t |
| FAIL | `hyst_core_score_e5_x2` | close | long | hold1 | 17030/+1.01%/t=1.6 | 11846/+0.18%/t=3.5 | 13238/+0.38%/t=6.7 | 15638/+0.91%/t=1.3 | 3572 | disc_t,lottery |
| FAIL | `strict_row_score_ml3` | close | long | hold2 | 13671/+0.66%/t=1.7 | 9367/+0.33%/t=3.3 | 11286/+0.59%/t=6.8 | 11752/+0.47%/t=1.0 | 3584 | disc_t |

Most near-misses are hold1 without a hold2 sibling (one-day lottery).

## 2. Scaled all-cols near-misses

Lean A–JL capture this cycle: **N=499** (311 discovery / 188 holdout) · **1.32 s/ticker** · 0 errors. Raw dumps not committed. Prior first A–JL cut (N=55) was PASS 0 FAIL 275 THIN 25; this is the scaled re-mine.

**PASS 0 · FAIL 305 · THIN 0.** Clean null.

Re-mined defs from the N=25 sample (`IZ_eq1`, `deeper_g5`, `AD_ge1`, `O_ge1`, `L_ge1`, `JA_eq1`, `lag_ELge2_Agreen`, `lag_Lge1_Agreen`):

| verdict | def | clock | side | exit | disc n/effect | hold n/effect | tape early | tape late | tickers | why |
|---|---|---|---|---|---|---|---|---|---:|---|
| FAIL | `AD_ge1` | close | long | hold1 | 12954/+0.28%/t=1.1 | 7276/-0.18%/t=-2.3 | 7142/+0.34%/t=1.0 | 13088/-0.01%/t=-0.1 | 499 | disc_t,hold_t,hold_sign,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `AD_ge1` | close | long | hold2 | 12892/+0.25%/t=1.0 | 7235/+3.09%/t=0.9 | 7142/+0.51%/t=1.5 | 12985/+1.69%/t=0.9 | 499 | disc_t,hold_t,spy_regime,no_edge_vs_uncond |
| FAIL | `IZ_eq1` | close | long | hold1 | 3929/+0.01%/t=0.1 | 2359/-0.26%/t=-2.6 | 4278/-0.14%/t=-1.8 | 2010/+0.01%/t=0.1 | 499 | disc_t,hold_t,hold_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `IZ_eq1` | close | long | hold2 | 3929/+0.32%/t=2.8 | 2359/-0.01%/t=-0.1 | 4278/+0.50%/t=4.6 | 2010/-0.44%/t=-2.6 | 499 | disc_t,hold_t,hold_sign,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `JA_eq1` | close | long | hold1 | 5383/-0.20%/t=-2.3 | 3107/-0.36%/t=-3.7 | 2729/-0.01%/t=-0.1 | 5761/-0.37%/t=-4.7 | 497 | disc_t,hold_t,disc_sign,hold_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `JA_eq1` | close | long | hold2 | 5351/+0.05%/t=0.2 | 3092/-0.43%/t=-3.0 | 2729/+0.35%/t=2.2 | 5714/-0.35%/t=-1.3 | 497 | disc_t,hold_t,hold_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `L_ge1` | close | long | hold1 | 10263/+0.09%/t=0.5 | 5926/+3.97%/t=1.0 | 6198/+0.09%/t=1.2 | 9991/+2.39%/t=1.0 | 499 | disc_t,hold_t,lottery,no_edge_vs_uncond |
| FAIL | `L_ge1` | close | long | hold2 | 10218/-0.08%/t=-0.5 | 5909/+3.88%/t=0.9 | 6198/+0.28%/t=2.4 | 9929/+2.05%/t=0.8 | 499 | disc_t,hold_t,disc_sign,lottery,spy_regime,no_edge_vs_uncond |
| FAIL | `O_ge1` | close | long | hold1 | 6902/-0.05%/t=-0.2 | 3874/-0.20%/t=-1.7 | 3888/-0.07%/t=-0.6 | 6888/-0.12%/t=-0.6 | 482 | disc_t,hold_t,disc_sign,hold_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `O_ge1` | close | long | hold2 | 6870/-0.22%/t=-1.0 | 3862/+6.04%/t=1.0 | 3888/+0.18%/t=1.1 | 6844/+3.08%/t=0.9 | 482 | disc_t,hold_t,disc_sign,lottery,spy_regime,no_edge_vs_uncond |
| FAIL | `deeper_g5` | close | long | hold1 | 39469/+0.21%/t=1.5 | 23732/+2.26%/t=1.7 | 21833/+0.07%/t=0.6 | 41368/+1.46%/t=1.9 | 499 | disc_t,hold_t,no_edge_vs_uncond |
| FAIL | `deeper_g5` | close | long | hold2 | 39187/+0.39%/t=2.3 | 23557/+2.43%/t=1.8 | 21833/+0.24%/t=1.6 | 40911/+1.65%/t=2.0 | 499 | disc_t,hold_t,no_edge_vs_uncond |
| FAIL | `lag_ELge2_Agreen` | open | long | hold1 | 10335/-0.15%/t=-3.4 | 6235/-0.15%/t=-3.0 | 6024/-0.09%/t=-1.6 | 10546/-0.19%/t=-4.5 | 498 | disc_t,hold_t,disc_sign,hold_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `lag_ELge2_Agreen` | open | long | hold2 | 10335/-0.02%/t=-0.1 | 6235/-0.22%/t=-2.8 | 6024/-0.16%/t=-2.0 | 10546/-0.06%/t=-0.3 | 498 | disc_t,hold_t,disc_sign,hold_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `lag_Lge1_Agreen` | open | long | hold1 | 8717/-0.10%/t=-2.0 | 5101/-0.11%/t=-1.8 | 5295/+0.11%/t=1.7 | 8523/-0.23%/t=-4.9 | 499 | disc_t,hold_t,disc_sign,hold_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `lag_Lge1_Agreen` | open | long | hold2 | 8716/-0.10%/t=-1.2 | 5101/-0.25%/t=-2.8 | 5295/+0.29%/t=2.9 | 8522/-0.44%/t=-5.6 | 499 | disc_t,hold_t,disc_sign,hold_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |

`deeper_g5` hold8 is still FAIL `no_edge_vs_uncond` (disc 37499/+1.48%/t=5.3 vs close-long hold8 uncond on this sample). Full table: `excel_bot/research/ALL_COLS_MINE.md`.

## 3. Decision

- **Keep (research, not a card):** the 6 hysteresis `open_core` / `open_score` ALL × hold1/2 rows. They clear the ship bar and beat uncond. Live frozen.
- **Do not keep:** hold5/8 tape-rides; `core_score`/`row_score` hold2 that lose to uncond; all scaled all-cols near-misses (N=499, 0 PASS).
- Stored **A–O surface is not exhausted** — keepers exist, but they are one-regime and not wired. The **workbook** is not exhausted; all-cols is scaled to 499 and those 8 defs are FAIL, not the full 275-col formula space.
- Next smallest family if iterating: another regime / walk-forward on the hysteresis cluster, not more A–JL dumps of the same 8 defs.

Research only. No cards. No merge without Cyrus. Live `flatten_robust` untouched.

## Harden (6 open-hysteresis lights)

KEEP 6 · KILL 0 · futubull · walk-forward + both-tape + top-day lottery. See `AO_FIRST_MINE.md` / `03_scoreboard/EXCEL_BOT_MINE.md`.

## Color + join mine

KEEP 9 · THIN 0 · KILL 113. Open-knowable fills only. PIT joins (prior AB/weather/book). See `AO_FIRST_MINE.md`.

## A–F seed (STOCKHISTORY / Yahoo rows)

Harden used Yahoo/rows `source: rows_cache` grids, not Excel's cached STOCKHISTORY. Tile TODAY is the anchor, not each day. See `AF_SEED_AUDIT.md`.

## Color harden (light + open fill vs light alone)

KEEP 3 recipes · KILL 3. Green O adds +24–45 bp on 3 of 6 lights. Other open colors fail the +20 bp parent bar. No joins in this beat. See `COLOR_HARDEN.md`.

## Join verdict (light vs color vs leak-free joins)

Re-scored cells KEEP 9 · KILL 15. Snapshot Finviz volM is BLOCKED. AB/weather/book KILL (short tape). See `JOIN_VERDICT.md`.

## Remaining A–JL families (unmined sweep)

Unmined A–JL sweep N=3603 · KEEP 126 raw / 66 unique letter×hold (40 hold1/2) · KILL 2890 · THIN 15. Letters: AH, AM, BA, BN, CJ, CZ, EH, EJ, EY, FC, FK, FL, FR, GV, HO, HZ, IB, IK, IL, N, R, T, U. Light+O still the only standing A–O color keep. Finviz BLOCKED. See `UNMINED_SWEEP.md`.

## Leftover KEEP harden (unique letters)

Leftover KEEP harden: raw 126 → unique 59 → KEEP 36 (36 hold1/2). Twins killed 67. Letters: AH, AM, BA, BN, CJ, CZ, EH, EJ, EY, FC, FK, FL, FR, GV, HO, HZ, IB, IK, IL, N, R, T, U. Light+O unchanged. Finviz BLOCKED. See `UNMINED_HARDEN.md`.

## Open-stack verdict (light+O vs AH / FR)

Open-stack: light+O ∧ AH/FR KEEP 4 · KILL 2 · THIN 0. Standing light+O unchanged. Close letters out of scope. Finviz BLOCKED. See `OPEN_STACK.md`.

## Close-cluster harden (T / BA)

Close-cluster T/BA: KEEP 0 · KILL 6 · THIN 0. Open recipes untouched. Finviz BLOCKED. See `CLOSE_CLUSTER.md`.

## Close-cluster peers (CZ / EH / IB / HO / IL / GV)

Close-cluster peers CZ/EH/IB/HO/IL/GV: KEEP 0 · KILL 12 · THIN 0. T/BA untouched. Open recipes untouched. See `CLOSE_CLUSTER.md`.

## Next A–JL region (weekly + leftover lag)

Next A–JL region: weekly+lag KEEP 0 · KILL 230 · THIN 0. Letter space exhausted. Light+O untouched. T/BA untouched. See `NEXT_REGION.md`.

## Same-day multi-letter counts (A–JL)

Same-day multi-letter: KEEP 0 · KILL 32 · THIN 0. A–JL surface exhausted. Light+O untouched. T/BA untouched. See `SAME_DAY.md`.

## Pair+lag mine (open-entry pilot)

Pair+lag open (locked 44): KEEP 0 · KILL 4266 · THIN 0. Trees skipped_no_pair_keep. Light+O baseline. See `PAIR_LAG.md` / `OPEN_SAME_ROW_LABELS.md`.

## H/I multi-horizon (standing keep + close pair)

H/I multi-horizon: KEEP 85 · KILL 1227 · THIN 0. Standing light+O re-scored. Close pair AA-today, no same-row H/I. See `HI_HORIZON.md`.

## Shade 2d/3d cumulative (open-entry stacked I)

Open shade → 2d/3d stacked I: family **null**. KEEP 0 · KILL 8. See `SHADE_2D3D.md`.
