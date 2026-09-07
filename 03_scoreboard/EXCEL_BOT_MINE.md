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

## Harden: morning hysteresis light (prove or kill)

_Generated 2026-09-07. Futubull fees kept (0.15% long round-trip). Live `flatten_robust` is not changed. No cards. No merge._

**Proved on this window.** When the morning green light first turns on, buying that name at the open and selling the same close made about **+1.3% to +1.7%** after fees, versus **−0.07%** if you bought everyone. The next-day close is still ahead of the +0.41% everyone-else baseline. Both halves of 2026 and both SPY tapes stay green. The fattest single day is 2–8% of winning-day P&L, not a lottery. Research only — one 2026 regime, no card.

### What the cell means (English first)

Every morning the sheet paints a few cells that are already known at the 9:30 open. Green is plus, red is minus (deep green = +2, light green = +1). When that sum **first** hits +5, the light turns on. We buy that name at **that open** and sell after N sessions. We do **not** ride the whole stretch — one trade at the first morning the light turns on.

- **next 1 session (hold1)** = sell the same day's close.
- **next 2 sessions (hold2)** = sell the next day's close.
- **five-cell light** = A, B, C, G, J (known at the open).
- **nine-cell light** = A, B, C, G, J, K, L, M, O (known at the open).
- Fees are taken off every trade before we judge it.

Everyone-else baseline, same clock and fees, no light required: next-1-session **−0.07%**, next-2-sessions **+0.41%**.

Prove needs **all** of: discovery and holdout both make money; first half and second half of 2026 both make money; SPY-up days **and** SPY-down days both make money; the fattest single **day** is under 25% of winning-day P&L; beat the everyone-else baseline by at least 20 bps. First-half / second-half cut is 2026-05-01.

A–O grids rebuilt: **3603**. Names that lit at least once: **3603**. Candidates: **6**. **KEEP 6** · **KILL 0**.

### English scoreboard

| meaning | hold | holdout after fees | vs everyone | first half | second half | SPY-up | SPY-down | fattest day | verdict | code |
|---|---|---|---|---|---|---|---|---|---|---|
| the five morning cells that are already known at 9:30 (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. Then buy at that open and sell at the same day's close (next 1 session). | next 1 | +1.74% (n=10931) | +1.81 pp | +1.86% (n=11306) | +1.74% (n=15506) | +2.03% (n=14857) | +1.51% (n=11955) | 2.7% (2026-05-13) | **KEEP** | `hyst_open_core_e5_x2` `hold1` |
| all nine morning cells known at 9:30 (A, B, C, G, J, K, L, M, O) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. Then buy at that open and sell at the same day's close (next 1 session). | next 1 | +1.33% (n=10862) | +1.40 pp | +1.66% (n=12227) | +1.17% (n=14413) | +1.63% (n=14634) | +1.11% (n=12006) | 2.2% (2026-03-03) | **KEEP** | `hyst_open_score_e5_x2` `hold1` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to 0. Then buy at that open and sell at the same day's close (next 1 session). | next 1 | +1.28% (n=11120) | +1.36 pp | +1.46% (n=11603) | +1.23% (n=15544) | +1.54% (n=14980) | +1.06% (n=12167) | 2.2% (2026-08-04) | **KEEP** | `hyst_open_core_e5_x0` `hold1` |
| the five morning cells that are already known at 9:30 (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. Then buy at that open and sell at the next day's close (next 2 sessions). | next 2 | +1.62% (n=10931) | +1.21 pp | +1.76% (n=11306) | +1.59% (n=15506) | +1.88% (n=14857) | +1.38% (n=11955) | 3.7% (2026-07-30) | **KEEP** | `hyst_open_core_e5_x2` `hold2` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to 0. Then buy at that open and sell at the next day's close (next 2 sessions). | next 2 | +1.21% (n=11120) | +0.80 pp | +1.34% (n=11603) | +1.14% (n=15544) | +1.46% (n=14980) | +0.94% (n=12167) | 4.4% (2026-07-30) | **KEEP** | `hyst_open_core_e5_x0` `hold2` |
| all nine morning cells known at 9:30 (A, B, C, G, J, K, L, M, O) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. Then buy at that open and sell at the next day's close (next 2 sessions). | next 2 | +1.54% (n=10862) | +1.13 pp | +1.45% (n=12227) | +1.47% (n=14413) | +1.61% (n=14634) | +1.27% (n=12006) | 7.6% (2026-07-30) | **KEEP** | `hyst_open_score_e5_x2` `hold2` |

### Numbers behind the English (same rows)

| verdict | def | exit | disc | holdout | early | late | SPY-up | SPY-down | day-lottery | tickers | why |
|---|---|---|---|---|---|---|---|---|---|---:|---|
| KEEP | `hyst_open_core_e5_x2` | hold1 | 15881/+1.84%/t=30.1 | 10931/+1.74%/t=34.4 | 11306/+1.86%/t=37.0 | 15506/+1.74%/t=28.2 | 14857/+2.03%/t=31.5 | 11955/+1.51%/t=31.5 | 2.7% | 3577 | — |
| KEEP | `hyst_open_score_e5_x2` | hold1 | 15778/+1.44%/t=31.8 | 10862/+1.33%/t=27.6 | 12227/+1.66%/t=34.9 | 14413/+1.17%/t=25.3 | 14634/+1.63%/t=37.4 | 12006/+1.11%/t=21.7 | 2.2% | 3552 | — |
| KEEP | `hyst_open_core_e5_x0` | hold1 | 16027/+1.35%/t=31.9 | 11120/+1.28%/t=26.4 | 11603/+1.46%/t=30.7 | 15544/+1.23%/t=28.4 | 14980/+1.54%/t=36.5 | 12167/+1.06%/t=21.6 | 2.2% | 3585 | — |
| KEEP | `hyst_open_core_e5_x2` | hold2 | 15881/+1.68%/t=18.4 | 10931/+1.62%/t=20.1 | 11306/+1.76%/t=24.3 | 15506/+1.59%/t=16.5 | 14857/+1.88%/t=19.4 | 11955/+1.38%/t=18.5 | 3.7% | 3577 | — |
| KEEP | `hyst_open_core_e5_x0` | hold2 | 16027/+1.24%/t=13.5 | 11120/+1.21%/t=14.6 | 11603/+1.34%/t=18.4 | 15544/+1.14%/t=11.7 | 14980/+1.46%/t=15.1 | 12167/+0.94%/t=11.9 | 4.4% | 3585 | — |
| KEEP | `hyst_open_score_e5_x2` | hold2 | 15778/+1.40%/t=13.1 | 10862/+1.54%/t=4.6 | 12227/+1.45%/t=20.8 | 14413/+1.47%/t=5.4 | 14634/+1.61%/t=14.3 | 12006/+1.27%/t=4.2 | 7.6% | 3552 | — |

KEEP is still research-only: one 2026 window, overlapping cluster days, Futubull model, no card, no live wire.

## Color + join mine (open-knowable fills, PIT joins)

_Generated 2026-09-07. Colors are signals, not decoration. Futubull fees kept. Live `flatten_robust` frozen. No cards._

### What a color means

The sheet paints a cell green or red. That paint is the signal. At 9:30 we can already see columns **A, B, C, G, J, K, L, M, O**. Columns D, E, F, H, I, N wait until the close — they never start an open trade. Green = buy at the open. Red = short at the open. Sell after 1 session (same-day close) or 2 sessions (next close).

Everyone-else baseline, same clock and fees: next-1 **−0.07%**, next-2 **+0.41%**.

### Joins (yesterday's tape only)

AB / weather / overnight book use the last file **dated before** the entry date. Same-day files are not knowable at 9:30. AB coverage 14 days (2026-08-19–2026-09-06); book 15 days; weather 20 days. Finviz cohorts are the **current snapshot**, not 2026 history.

Grids **3598**. Cells scored **122**. **KEEP 9** · **THIN 0** · **KILL 113**.

**Color alone does not clear the bar.** One morning cell being green is not enough — the late half of 2026 goes red on same-day holds. The already-proved light **plus a green O** adds about +20–45 bp over the light itself. High-vol Finviz names add +24–64 bp, but that tag is today's snapshot, not 2026 history. AB / weather / overnight book only exist for late Aug–Sep, so they fail walk-forward (no first half).

### Color → next-N-days (open letters only)

| meaning | hold | side | holdout after fees | vs everyone | first half | second half | fattest day | verdict | code |
|---|---|---|---|---|---|---|---|---|---|
| Morning cell L is highlighted green (known at 9:30). Buy at that open and sell at the next day's close. | next 2 | long | +1.06% (n=67329) | — | +0.21% (n=79261) | +0.63% (n=87265) | 11.2% | **KILL** | `color_L_green` |
| Morning cell O is highlighted green (known at 9:30). Buy at that open and sell at the next day's close. | next 2 | long | +0.96% (n=35984) | — | +0.37% (n=40310) | +0.61% (n=49128) | 13.5% | **KILL** | `color_O_green` |
| Morning cell G is highlighted green (known at 9:30). Buy at that open and sell at the next day's close. | next 2 | long | +0.51% (n=61404) | — | +0.39% (n=59896) | +0.18% (n=92424) | 10.6% | **KILL** | `color_G_green` |
| Morning cell J is highlighted green (known at 9:30). Buy at that open and sell at the next day's close. | next 2 | long | +0.48% (n=70544) | — | +0.17% (n=96161) | +0.69% (n=77567) | 12.3% | **KILL** | `color_J_green` |
| Morning cell B is highlighted green (known at 9:30). Buy at that open and sell at the next day's close. | next 2 | long | +0.44% (n=91063) | — | +0.03% (n=103443) | +0.44% (n=120427) | 12.7% | **KILL** | `color_B_green` |
| Morning cell A is highlighted green (known at 9:30). Buy at that open and sell at the next day's close. | next 2 | long | +0.41% (n=152822) | — | +0.11% (n=174580) | +0.28% (n=200812) | 9.5% | **KILL** | `color_A_green` |
| Morning cell O is highlighted green (known at 9:30). Buy at that open and sell at the same day's close. | next 1 | long | +0.41% (n=36139) | — | +0.38% (n=40310) | -0.14% (n=49506) | 6.1% | **KILL** | `color_O_green` |
| Morning cell L is highlighted green (known at 9:30). Buy at that open and sell at the same day's close. | next 1 | long | +0.21% (n=67560) | — | +0.17% (n=79261) | -0.14% (n=87800) | 5.3% | **KILL** | `color_L_green` |
| Morning cell G is highlighted green (known at 9:30). Buy at that open and sell at the same day's close. | next 1 | long | +0.20% (n=61695) | — | +0.25% (n=59896) | -0.15% (n=93145) | 5.8% | **KILL** | `color_G_green` |
| Morning cell J is highlighted green (known at 9:30). Buy at that open and sell at the same day's close. | next 1 | long | +0.17% (n=70849) | — | +0.11% (n=96161) | -0.12% (n=78304) | 7.0% | **KILL** | `color_J_green` |
| Morning cell A is highlighted green (known at 9:30). Buy at that open and sell at the same day's close. | next 1 | long | +0.12% (n=152838) | — | +0.10% (n=174580) | -0.15% (n=200853) | 5.0% | **KILL** | `color_A_green` |
| Morning cell B is highlighted green (known at 9:30). Buy at that open and sell at the same day's close. | next 1 | long | +0.08% (n=91076) | — | +0.09% (n=103443) | -0.16% (n=120453) | 6.1% | **KILL** | `color_B_green` |
| Morning cell M is highlighted green (known at 9:30). Buy at that open and sell at the same day's close. | next 1 | long | +0.01% (n=28328) | — | +0.14% (n=55157) | -0.35% (n=14490) | 9.3% | **KILL** | `color_M_green` |
| Morning cell M is highlighted green (known at 9:30). Buy at that open and sell at the next day's close. | next 2 | long | -0.03% (n=28327) | — | -0.02% (n=55157) | -0.06% (n=14489) | 13.3% | **KILL** | `color_M_green` |
| Morning cell K is highlighted green (known at 9:30). Buy at that open and sell at the next day's close. | next 2 | long | -0.05% (n=45745) | — | -0.05% (n=45513) | -0.06% (n=68391) | 4.8% | **KILL** | `color_K_green` |
| Morning cell K is highlighted green (known at 9:30). Buy at that open and sell at the same day's close. | next 1 | long | -0.16% (n=46080) | — | -0.12% (n=45513) | -0.16% (n=69185) | 4.9% | **KILL** | `color_K_green` |
| Morning cell A is highlighted red (known at 9:30). Short at that open and sell at the same day's close. | next 1 | short | -0.21% (n=78522) | — | -0.14% (n=83046) | -0.16% (n=111267) | 6.3% | **KILL** | `color_A_red` |
| Morning cell B is highlighted red (known at 9:30). Short at that open and sell at the same day's close. | next 1 | short | -0.26% (n=81292) | — | -0.16% (n=86917) | -0.17% (n=114147) | 6.5% | **KILL** | `color_B_red` |
| Morning cell O is highlighted red (known at 9:30). Short at that open and sell at the same day's close. | next 1 | short | -0.31% (n=181969) | — | -0.28% (n=200116) | -0.18% (n=246904) | 5.2% | **KILL** | `color_O_red` |
| Morning cell J is highlighted red (known at 9:30). Short at that open and sell at the same day's close. | next 1 | short | -0.35% (n=137776) | — | -0.29% (n=139736) | -0.17% (n=199040) | 6.4% | **KILL** | `color_J_red` |
| Morning cell L is highlighted red (known at 9:30). Short at that open and sell at the same day's close. | next 1 | short | -0.59% (n=47817) | — | -0.56% (n=46451) | -0.16% (n=72320) | 5.8% | **KILL** | `color_L_red` |
| Morning cell O is highlighted red (known at 9:30). Short at that open and sell at the next day's close. | next 2 | short | -0.91% (n=180728) | — | -0.32% (n=200116) | -1.01% (n=243852) | 6.5% | **KILL** | `color_O_red` |
| Morning cell J is highlighted red (known at 9:30). Short at that open and sell at the next day's close. | next 2 | short | -1.16% (n=136727) | — | -0.30% (n=139736) | -1.20% (n=196437) | 7.5% | **KILL** | `color_J_red` |
| Morning cell B is highlighted red (known at 9:30). Short at that open and sell at the next day's close. | next 2 | short | -1.28% (n=81279) | — | -0.22% (n=86917) | -1.70% (n=114115) | 7.0% | **KILL** | `color_B_red` |

### Light + a green cell (fold into the 6 KEEP hysteresis names)

| meaning | hold | holdout | vs parent light | verdict | code |
|---|---|---|---|---|---|
| Same morning light as `hyst_open_score_e5_x2`, and morning cell O is also green. | next 2 | +1.99% (n=5264) | +0.45 pp | **KEEP** | `hyst_open_score_e5_x2__O_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell O is also green. | next 1 | +1.98% (n=4812) | +0.24 pp | **KEEP** | `hyst_open_core_e5_x2__O_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell O is also green. | next 2 | +1.88% (n=4812) | +0.27 pp | **KEEP** | `hyst_open_core_e5_x2__O_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell L is also green. | next 1 | +1.92% (n=6667) | +0.18 pp | **KILL** | `hyst_open_core_e5_x2__L_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell G is also green. | next 1 | +1.90% (n=9073) | +0.16 pp | **KILL** | `hyst_open_core_e5_x2__G_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell G is also green. | next 2 | +1.76% (n=9073) | +0.15 pp | **KILL** | `hyst_open_core_e5_x2__G_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell B is also green. | next 1 | +1.76% (n=10518) | +0.03 pp | **KILL** | `hyst_open_core_e5_x2__B_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell J is also green. | next 1 | +1.76% (n=8900) | +0.02 pp | **KILL** | `hyst_open_core_e5_x2__J_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell A is also green. | next 1 | +1.74% (n=10931) | +0.00 pp | **KILL** | `hyst_open_core_e5_x2__A_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell M is also green. | next 1 | +1.68% (n=2485) | -0.05 pp | **KILL** | `hyst_open_core_e5_x2__M_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell L is also green. | next 2 | +1.66% (n=6667) | +0.04 pp | **KILL** | `hyst_open_core_e5_x2__L_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell B is also green. | next 2 | +1.63% (n=10518) | +0.01 pp | **KILL** | `hyst_open_core_e5_x2__B_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell A is also green. | next 2 | +1.62% (n=10931) | +0.00 pp | **KILL** | `hyst_open_core_e5_x2__A_green` |
| Same morning light as `hyst_open_score_e5_x2`, and morning cell J is also green. | next 2 | +1.60% (n=8827) | +0.05 pp | **KILL** | `hyst_open_score_e5_x2__J_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell J is also green. | next 2 | +1.60% (n=8900) | -0.02 pp | **KILL** | `hyst_open_core_e5_x2__J_green` |
| Same morning light as `hyst_open_score_e5_x2`, and morning cell B is also green. | next 2 | +1.58% (n=9887) | +0.03 pp | **KILL** | `hyst_open_score_e5_x2__B_green` |
| Same morning light as `hyst_open_core_e5_x2`, and morning cell K is also green. | next 1 | +1.57% (n=6099) | -0.17 pp | **KILL** | `hyst_open_core_e5_x2__K_green` |
| Same morning light as `hyst_open_score_e5_x2`, and morning cell A is also green. | next 2 | +1.54% (n=10861) | -0.00 pp | **KILL** | `hyst_open_score_e5_x2__A_green` |

### Light + AB / weather / book / Finviz (useful combo = win)

| meaning | hold | holdout | vs parent | dates | verdict | why | code |
|---|---|---|---|---:|---|---|---|
| Same morning light as `hyst_open_core_e5_x2`, and Finviz snapshot cohort volM (not a historical as-of). | next 1 | +2.22% (n=5287) | +0.48 pp | 160 | **KEEP** | — | `hyst_open_core_e5_x2__fz_volM` |
| Same morning light as `hyst_open_score_e5_x2`, and Finviz snapshot cohort volM (not a historical as-of). | next 2 | +2.19% (n=5238) | +0.64 pp | 160 | **KEEP** | — | `hyst_open_score_e5_x2__fz_volM` |
| Same morning light as `hyst_open_core_e5_x2`, and Finviz snapshot cohort volM (not a historical as-of). | next 2 | +2.02% (n=5287) | +0.40 pp | 160 | **KEEP** | — | `hyst_open_core_e5_x2__fz_volM` |
| Same morning light as `hyst_open_score_e5_x2`, and Finviz snapshot cohort volM (not a historical as-of). | next 1 | +1.65% (n=5238) | +0.32 pp | 160 | **KEEP** | — | `hyst_open_score_e5_x2__fz_volM` |
| Same morning light as `hyst_open_core_e5_x0`, and Finviz snapshot cohort volM (not a historical as-of). | next 1 | +1.61% (n=5553) | +0.33 pp | 160 | **KEEP** | — | `hyst_open_core_e5_x0__fz_volM` |
| Same morning light as `hyst_open_core_e5_x0`, and Finviz snapshot cohort volM (not a historical as-of). | next 2 | +1.45% (n=5553) | +0.24 pp | 160 | **KEEP** | — | `hyst_open_core_e5_x0__fz_volM` |
| Same morning light as `hyst_open_core_e5_x2`, and prior weather was risk-off. | next 2 | +2.21% (n=496) | +0.59 pp | 9 | **KILL** | date_bar,lottery_day,tape_split | `hyst_open_core_e5_x2__wx_risk_off` |
| Same morning light as `hyst_open_core_e5_x2`, and prior weather was risk-off. | next 1 | +2.07% (n=496) | +0.34 pp | 9 | **KILL** | date_bar,tape_split | `hyst_open_core_e5_x2__wx_risk_off` |
| Same morning light as `hyst_open_core_e5_x0`, and prior weather was risk-off. | next 2 | +1.71% (n=505) | +0.50 pp | 9 | **KILL** | date_bar,lottery_day,tape_split | `hyst_open_core_e5_x0__wx_risk_off` |
| Same morning light as `hyst_open_core_e5_x2`, and the overnight 1-day book had it as a buy. | next 2 | +1.69% (n=7) | +0.07 pp | 10 | **KILL** | thin_disc,thin_hold,disc_t,hold_t,ticker_bar,date_bar,lottery_trade,lottery_day,tape_split,no_edge_vs_parent | `hyst_open_core_e5_x2__book_1d_buy` |
| Same morning light as `hyst_open_core_e5_x0`, and the overnight 1-day book had it as a buy. | next 1 | +1.68% (n=7) | +0.40 pp | 10 | **KILL** | thin_disc,thin_hold,disc_t,hold_t,ticker_bar,date_bar,lottery_trade,lottery_day,tape_split | `hyst_open_core_e5_x0__book_1d_buy` |
| Same morning light as `hyst_open_core_e5_x2`, and the overnight 1-day book had it as a buy. | next 1 | +1.65% (n=7) | -0.08 pp | 10 | **KILL** | thin_disc,thin_hold,hold_t,ticker_bar,date_bar,lottery_trade,lottery_day,tape_split,no_edge_vs_parent | `hyst_open_core_e5_x2__book_1d_buy` |
| Same morning light as `hyst_open_score_e5_x2`, and prior weather was risk-off. | next 2 | +1.65% (n=457) | +0.11 pp | 9 | **KILL** | disc_t,date_bar,lottery_day,tape_split,spy_regime,no_edge_vs_parent | `hyst_open_score_e5_x2__wx_risk_off` |
| Same morning light as `hyst_open_score_e5_x2`, and prior weather was risk-off. | next 1 | +1.54% (n=457) | +0.22 pp | 9 | **KILL** | date_bar,lottery_day,tape_split | `hyst_open_score_e5_x2__wx_risk_off` |
| Same morning light as `hyst_open_core_e5_x0`, and prior weather was risk-off. | next 1 | +1.51% (n=505) | +0.22 pp | 9 | **KILL** | date_bar,lottery_day,tape_split | `hyst_open_core_e5_x0__wx_risk_off` |
| Same morning light as `hyst_open_core_e5_x2`, and prior weather was risk-on. | next 1 | +1.46% (n=157) | -0.28 pp | 2 | **KILL** | thin_disc,date_bar,lottery_day,tape_split,spy_regime,no_edge_vs_parent | `hyst_open_core_e5_x2__wx_risk_on` |
| Same morning light as `hyst_open_core_e5_x0`, and the overnight 1-day book had it as a buy. | next 2 | +1.44% (n=7) | +0.23 pp | 10 | **KILL** | thin_disc,thin_hold,disc_t,hold_t,ticker_bar,date_bar,lottery_trade,lottery_day,tape_split | `hyst_open_core_e5_x0__book_1d_buy` |
| Same morning light as `hyst_open_core_e5_x2`, and yesterday's AB tape already liked the name. | next 2 | +1.27% (n=251) | -0.35 pp | 11 | **KILL** | date_bar,lottery_day,tape_split,no_edge_vs_parent | `hyst_open_core_e5_x2__ab_good` |
| Same morning light as `hyst_open_core_e5_x2`, and Finviz snapshot cohort opt (not a historical as-of). | next 1 | +1.23% (n=7990) | -0.51 pp | 160 | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__fz_opt` |
| Same morning light as `hyst_open_core_e5_x2`, and Finviz snapshot cohort opt (not a historical as-of). | next 2 | +1.19% (n=7990) | -0.42 pp | 160 | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__fz_opt` |
| Same morning light as `hyst_open_core_e5_x2`, and Finviz snapshot cohort mid(1-10B) (not a historical as-of). | next 2 | +1.05% (n=3664) | -0.57 pp | 160 | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__fz_mid(1-10B)` |
| Same morning light as `hyst_open_core_e5_x2`, and yesterday's AB tape already liked the name. | next 1 | +1.04% (n=251) | -0.70 pp | 11 | **KILL** | date_bar,tape_split,no_edge_vs_parent | `hyst_open_core_e5_x2__ab_good` |
| Same morning light as `hyst_open_core_e5_x2`, and Finviz snapshot cohort mid(1-10B) (not a historical as-of). | next 1 | +1.03% (n=3664) | -0.71 pp | 160 | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__fz_mid(1-10B)` |
| Same morning light as `hyst_open_core_e5_x0`, and Finviz snapshot cohort opt (not a historical as-of). | next 2 | +0.94% (n=7763) | -0.27 pp | 160 | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__fz_opt` |

KEEP still research-only. Join tapes only exist for late Aug–Sep 2026, so most join cells are THIN on dates. Color cells use the full Jan–Sep window. No live wire.

## A–F seed (STOCKHISTORY / Yahoo rows)

_Generated 2026-09-07. Live `flatten_robust` frozen. No cards._

### What A–F are

In the real sheet, columns **A–F are STOCKHISTORY**. They alias the daily spill IR:IW (date, close, open, high, low, volume). The emulator must fill that spill from **our** price history — the Yahoo / excel-state rows cache — for the day under test. It must not paste Excel's last-calc cache, and it must not put later bars into that day's A–F.

### Path harden actually used

`rebuild_grids.py` reads `data/rows/<T>.json`, calls `backtest.seed_anchor` / `build_ticker`, and writes `source: rows_cache`. Harden, color-join, and the A–O mines load those grids. They do **not** call `run.py --from-cache` or `validate.build_seeds`.

Grids on disk this run: **3603**. Yahoo/rows rebuilds: **3603**. Excel-cache or untagged: **0**.

**No Excel-cache grids on this disk.** Every mined file is a Yahoo/rows rebuild. The Excel-cache replay path still exists (`run.py --from-cache`) — it is for matching the xlsx, not for harden or walk-forward.

### Day under test vs the tile shortcut

`seed_anchor` only keeps rows with **date ≤ the engine's TODAY**. That TODAY is the **tile anchor** (about every 120 trading days), not each calendar day. So a day in the middle of a tile is painted by an engine that can already see later prices in later rows of IR:IW.

We checked that leak two ways on AAPL (Yahoo rows, not Excel cache):

- **Strip later prices, keep the later TODAY.** 2026-06-26 A–O fills were **identical** to the full later tile. Future daily bars did not change that day's paint.
- **Strict as-of (TODAY = that day) vs the later tile.** Most mid-window days matched. Two open-letter misses: **M** on 2026-07-27 (green vs blank) and **J** on 2026-03-31 (red vs light green). Those are TODAY / window-alignment, not Excel cache, and not “a later close leaked into A–F.”

Days that land on **rows 2–9** of a tile often have no A/B/J/L/M/O highlight — those paint rules start around row 10. That is a **missing color** (we under-count lights), not a peek at the future.

### Other flags (not open-entry)

- **D / E look-ahead:** helper columns CD / CE sum the next five rows. That only paints D and E, which are close-knowable and never start an open trade.
- **Weekly AP:AU** is seeded through the same TODAY. Weekly high/low/vol are treated close.

### Verdict for #144

Harden and walk-forward used the Yahoo/rows rebuild for every ticker on disk. They did **not** reuse Excel's cached A–F. The remaining gap is the **tile TODAY**, not a hidden xlsx dump. KEEP-6 / color-join stay research-only. No live wire.

## Color harden (light + open fill vs light alone)

_Generated 2026-09-07. Color-only beat — no Finviz / AB / weather / book. Futubull fees kept. Live `flatten_robust` frozen. No cards._

### Question

When the morning green light first turns on, does **also** requiring an open-knowable cell to be green (or red) add money after fees, versus the light alone?

Open-knowable paints: **A, B, C, G, J, K, L, M, O**. Close paints D, E, F, H, I, N never start this trade. Same ship bar as the KEEP-6 harden: discovery/holdout, both 2026 halves (cut 2026-05-01), SPY up and down, top-day lottery under 25%, beat the parent light by at least 20 bps. Grids are the Yahoo/rows rebuild (`source: rows_cache`), **3598** files. No Excel cached A–F.

**KEEP 3** recipes · **KILL 3** · **THIN 0**. Color folds that themselves KEEP: **3** (all of them are a green **O**).

**A green O on top of the light can add about +24–45 bp.** Other morning greens (A, B, G, J, K, L, M) do not clear +20 bp versus the light itself. A color by itself, without the light, already failed this bar (late 2026 goes red on same-day holds).

### Per recipe (light alone vs light + best open color)

| meaning | hold | light alone | light + best color | extra after fees | verdict | code |
|---|---|---|---|---|---|---|
| the five morning cells that are already known at 9:30 (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. Plus morning cell O is also highlighted green (known at 9:30). | next 1 | +1.74% (n=10931) | +1.98% (n=4812) | +0.24 pp | **KEEP** | `hyst_open_core_e5_x2__O_green` |
| all nine morning cells known at 9:30 (A, B, C, G, J, K, L, M, O) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. Plus morning cell M is also highlighted green (known at 9:30). | next 1 | +1.33% (n=10862) | +1.49% (n=3267) | +0.16 pp | **KILL** | `hyst_open_score_e5_x2__M_green` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to 0. Plus morning cell L is also highlighted green (known at 9:30). | next 1 | +1.28% (n=11120) | +1.46% (n=6403) | +0.18 pp | **KILL** | `hyst_open_core_e5_x0__L_green` |
| the five morning cells that are already known at 9:30 (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. Plus morning cell O is also highlighted green (known at 9:30). | next 2 | +1.62% (n=10931) | +1.88% (n=4812) | +0.27 pp | **KEEP** | `hyst_open_core_e5_x2__O_green` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to 0. Plus morning cell O is also highlighted green (known at 9:30). | next 2 | +1.21% (n=11120) | +1.38% (n=4903) | +0.17 pp | **KILL** | `hyst_open_core_e5_x0__O_green` |
| all nine morning cells known at 9:30 (A, B, C, G, J, K, L, M, O) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. Plus morning cell O is also highlighted green (known at 9:30). | next 2 | +1.54% (n=10862) | +1.99% (n=5264) | +0.45 pp | **KEEP** | `hyst_open_score_e5_x2__O_green` |

### Every open-letter fold (same six lights)

| light | hold | color | holdout | vs light | verdict | why | code |
|---|---|---|---|---|---|---|---|
| `hyst_open_score_e5_x2` | next 2 | O_green | +1.99% (n=5264) | +0.45 pp | **KEEP** | — | `hyst_open_score_e5_x2__O_green` |
| `hyst_open_core_e5_x2` | next 1 | O_green | +1.98% (n=4812) | +0.24 pp | **KEEP** | — | `hyst_open_core_e5_x2__O_green` |
| `hyst_open_core_e5_x2` | next 2 | O_green | +1.88% (n=4812) | +0.27 pp | **KEEP** | — | `hyst_open_core_e5_x2__O_green` |
| `hyst_open_core_e5_x2` | next 1 | L_green | +1.92% (n=6667) | +0.18 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__L_green` |
| `hyst_open_core_e5_x2` | next 1 | G_green | +1.90% (n=9073) | +0.16 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__G_green` |
| `hyst_open_core_e5_x2` | next 2 | G_green | +1.76% (n=9073) | +0.15 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__G_green` |
| `hyst_open_core_e5_x2` | next 1 | B_green | +1.76% (n=10518) | +0.03 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__B_green` |
| `hyst_open_core_e5_x2` | next 1 | J_green | +1.76% (n=8900) | +0.02 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__J_green` |
| `hyst_open_core_e5_x2` | next 1 | A_green | +1.74% (n=10931) | +0.00 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__A_green` |
| `hyst_open_core_e5_x2` | next 1 | M_green | +1.68% (n=2485) | -0.05 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__M_green` |
| `hyst_open_core_e5_x2` | next 2 | L_green | +1.66% (n=6667) | +0.04 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__L_green` |
| `hyst_open_core_e5_x2` | next 2 | B_green | +1.63% (n=10518) | +0.01 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__B_green` |
| `hyst_open_core_e5_x2` | next 2 | A_green | +1.62% (n=10931) | +0.00 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__A_green` |
| `hyst_open_score_e5_x2` | next 2 | J_green | +1.60% (n=8827) | +0.05 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__J_green` |
| `hyst_open_core_e5_x2` | next 2 | J_green | +1.60% (n=8900) | -0.02 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__J_green` |
| `hyst_open_score_e5_x2` | next 2 | B_green | +1.58% (n=9887) | +0.03 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__B_green` |
| `hyst_open_core_e5_x2` | next 1 | K_green | +1.57% (n=6099) | -0.17 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__K_green` |
| `hyst_open_score_e5_x2` | next 2 | A_green | +1.54% (n=10861) | -0.00 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__A_green` |
| `hyst_open_score_e5_x2` | next 1 | M_green | +1.49% (n=3267) | +0.16 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__M_green` |
| `hyst_open_core_e5_x0` | next 1 | L_green | +1.46% (n=6403) | +0.18 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__L_green` |
| `hyst_open_core_e5_x0` | next 1 | O_green | +1.45% (n=4903) | +0.17 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__O_green` |
| `hyst_open_core_e5_x2` | next 2 | K_green | +1.45% (n=6099) | -0.17 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__K_green` |
| `hyst_open_core_e5_x0` | next 1 | G_green | +1.43% (n=8940) | +0.14 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__G_green` |
| `hyst_open_score_e5_x2` | next 1 | L_green | +1.42% (n=7184) | +0.09 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__L_green` |
| `hyst_open_core_e5_x2` | next 2 | M_green | +1.39% (n=2485) | -0.23 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x2__M_green` |
| `hyst_open_core_e5_x0` | next 2 | O_green | +1.38% (n=4903) | +0.17 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__O_green` |
| `hyst_open_score_e5_x2` | next 1 | O_green | +1.37% (n=5264) | +0.04 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__O_green` |
| `hyst_open_score_e5_x2` | next 1 | B_green | +1.35% (n=9887) | +0.02 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__B_green` |
| `hyst_open_score_e5_x2` | next 1 | J_green | +1.34% (n=8827) | +0.01 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__J_green` |
| `hyst_open_core_e5_x0` | next 2 | G_green | +1.33% (n=8940) | +0.12 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__G_green` |
| `hyst_open_score_e5_x2` | next 1 | A_green | +1.33% (n=10861) | -0.00 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__A_green` |
| `hyst_open_core_e5_x0` | next 1 | M_green | +1.33% (n=2513) | +0.04 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__M_green` |
| `hyst_open_score_e5_x2` | next 1 | G_green | +1.32% (n=8239) | -0.01 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__G_green` |
| `hyst_open_core_e5_x0` | next 1 | J_green | +1.30% (n=9186) | +0.02 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__J_green` |
| `hyst_open_core_e5_x0` | next 1 | B_green | +1.30% (n=10834) | +0.01 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__B_green` |
| `hyst_open_core_e5_x0` | next 1 | A_green | +1.28% (n=11120) | +0.00 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__A_green` |
| `hyst_open_core_e5_x0` | next 2 | L_green | +1.26% (n=6403) | +0.05 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__L_green` |
| `hyst_open_score_e5_x2` | next 2 | G_green | +1.22% (n=8239) | -0.33 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__G_green` |
| `hyst_open_core_e5_x0` | next 2 | M_green | +1.22% (n=2513) | +0.01 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__M_green` |
| `hyst_open_core_e5_x0` | next 2 | A_green | +1.21% (n=11120) | +0.00 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__A_green` |
| `hyst_open_core_e5_x0` | next 2 | B_green | +1.21% (n=10834) | -0.00 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__B_green` |
| `hyst_open_core_e5_x0` | next 2 | J_green | +1.20% (n=9186) | -0.01 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__J_green` |
| `hyst_open_score_e5_x2` | next 2 | L_green | +1.18% (n=7184) | -0.37 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__L_green` |
| `hyst_open_core_e5_x0` | next 1 | K_green | +1.17% (n=5714) | -0.12 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__K_green` |
| `hyst_open_score_e5_x2` | next 2 | M_green | +1.13% (n=3267) | -0.41 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__M_green` |
| `hyst_open_core_e5_x0` | next 2 | K_green | +1.12% (n=5714) | -0.09 pp | **KILL** | no_edge_vs_parent | `hyst_open_core_e5_x0__K_green` |
| `hyst_open_score_e5_x2` | next 1 | K_green | +0.99% (n=6244) | -0.34 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__K_green` |
| `hyst_open_score_e5_x2` | next 2 | K_green | +0.95% (n=6244) | -0.60 pp | **KILL** | no_edge_vs_parent | `hyst_open_score_e5_x2__K_green` |

KEEP is still research-only: one 2026 window, Futubull model, no card, no live wire. Joins (Finviz / AB / weather / book) are out of scope for this beat.

## Join verdict (light vs color vs leak-free joins)

_Generated 2026-09-07. Joins on top of the six KEEP lights. Futubull fees. Live `flatten_robust` frozen. No cards._

### Incremental layers

Same ship bar as harden: both 2026 halves (cut 2026-05-01), SPY up and down, top-day lottery, beat the previous layer by ≥20 bp. AB / weather / overnight book / Finviz **as-of** use the last file **dated before** the entry. Same-day files are not knowable at 9:30.

Finviz Elite dated exports: **21** days (2026-04-26–2026-09-06). One spring file (2026-04-26), then a gap until mid-August. That is not a 2026 history. The old snapshot `fz_volM` KEEP is **not shippable**.

Second-regime cut for surviving color folds: **2026-07-01** (Q1–Q2 vs Q3), past the May 1 half already used to KEEP the lights.

Grids **3603**, Yahoo/rows only.

### Light alone vs light+color vs joins

| layer | meaning | hold | holdout after fees | vs light | verdict | why |
|---|---|---|---|---|---|---|
| light alone | the five morning cells that are already known at 9:30 (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. | next 1 | +1.74% (n=10931) | — | **KEEP** | already hardened |
| light + color | best open color on that light (O green) | next 1 | +1.98% (n=4812) | +0.24 pp | **KEEP** | green O ≥20 bp |
| light alone | all nine morning cells known at 9:30 (A, B, C, G, J, K, L, M, O) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. | next 1 | +1.33% (n=10862) | — | **KEEP** | already hardened |
| light + color | best open color on that light (M green) | next 1 | +1.49% (n=3267) | +0.16 pp | **KILL** | no_edge_vs_parent |
| light alone | the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to 0. | next 1 | +1.28% (n=11120) | — | **KEEP** | already hardened |
| light + color | best open color on that light (L green) | next 1 | +1.46% (n=6403) | +0.18 pp | **KILL** | no_edge_vs_parent |
| light alone | the five morning cells that are already known at 9:30 (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. | next 2 | +1.62% (n=10931) | — | **KEEP** | already hardened |
| light + color | best open color on that light (O green) | next 2 | +1.88% (n=4812) | +0.27 pp | **KEEP** | green O ≥20 bp |
| light alone | the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to 0. | next 2 | +1.21% (n=11120) | — | **KEEP** | already hardened |
| light + color | best open color on that light (O green) | next 2 | +1.38% (n=4903) | +0.17 pp | **KILL** | no_edge_vs_parent |
| light alone | all nine morning cells known at 9:30 (A, B, C, G, J, K, L, M, O) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. | next 2 | +1.54% (n=10862) | — | **KEEP** | already hardened |
| light + color | best open color on that light (O green) | next 2 | +1.99% (n=5264) | +0.45 pp | **KEEP** | green O ≥20 bp |
| Finviz snapshot (not as-of) | today's high-vol tag on the light — current CSV, not 2026 history | next 1 | +2.22% (n=5287) | +0.48 pp | **BLOCKED** | no historical as-of; cannot ship |
| Finviz snapshot (not as-of) | today's high-vol tag on the light — current CSV, not 2026 history | next 2 | +2.19% (n=5238) | +0.64 pp | **BLOCKED** | no historical as-of; cannot ship |
| Finviz snapshot (not as-of) | today's high-vol tag on the light — current CSV, not 2026 history | next 2 | +2.02% (n=5287) | +0.40 pp | **BLOCKED** | no historical as-of; cannot ship |
| Finviz snapshot (not as-of) | today's high-vol tag on the light — current CSV, not 2026 history | next 1 | +1.65% (n=5238) | +0.32 pp | **BLOCKED** | no historical as-of; cannot ship |
| Finviz snapshot (not as-of) | today's high-vol tag on the light — current CSV, not 2026 history | next 1 | +1.61% (n=5553) | +0.33 pp | **BLOCKED** | no historical as-of; cannot ship |
| Finviz snapshot (not as-of) | today's high-vol tag on the light — current CSV, not 2026 history | next 2 | +1.45% (n=5553) | +0.24 pp | **BLOCKED** | no historical as-of; cannot ship |
| Finviz as-of high-vol | prior Elite export had Volatility (Month) > 8% | next 1 | +2.59% (n=806) | +1.30 pp | **KILL** | finviz_asof_gappy |
| Finviz as-of high-vol | prior Elite export had Volatility (Month) > 8% | next 2 | +2.39% (n=806) | +1.18 pp | **KILL** | finviz_asof_gappy,disc_t |
| Finviz as-of high-vol | prior Elite export had Volatility (Month) > 8% | next 1 | +2.82% (n=1425) | +1.53 pp | **KILL** | finviz_asof_gappy |
| Finviz as-of high-vol | prior Elite export had Volatility (Month) > 8% | next 2 | +2.34% (n=1425) | +1.13 pp | **KILL** | finviz_asof_gappy |
| Finviz as-of high-vol | prior Elite export had Volatility (Month) > 8% | next 1 | +4.02% (n=738) | +2.28 pp | **KILL** | finviz_asof_gappy |
| Finviz as-of high-vol | prior Elite export had Volatility (Month) > 8% | next 2 | +3.77% (n=738) | +2.15 pp | **KILL** | finviz_asof_gappy |
| Finviz as-of high-vol | prior Elite export had Volatility (Month) > 8% | next 1 | +4.08% (n=1311) | +2.34 pp | **KILL** | finviz_asof_gappy |
| Finviz as-of high-vol | prior Elite export had Volatility (Month) > 8% | next 2 | +3.61% (n=1311) | +1.99 pp | **KILL** | finviz_asof_gappy |
| Finviz as-of high-vol | prior Elite export had Volatility (Month) > 8% | next 1 | +2.18% (n=778) | +0.86 pp | **KILL** | finviz_asof_gappy |
| Finviz as-of high-vol | prior Elite export had Volatility (Month) > 8% | next 2 | +7.22% (n=778) | +5.68 pp | **KILL** | finviz_asof_gappy,disc_t,hold_t,lottery_day |
| Finviz as-of high-vol | prior Elite export had Volatility (Month) > 8% | next 1 | +2.49% (n=1244) | +1.17 pp | **KILL** | finviz_asof_gappy |
| Finviz as-of high-vol | prior Elite export had Volatility (Month) > 8% | next 2 | +5.18% (n=1244) | +3.63 pp | **KILL** | finviz_asof_gappy,hold_t,lottery_day |
| light + O + as-of high-vol | green O and prior Elite high-vol together | next 1 | +2.59% (n=806) | +1.30 pp | **KILL** | finviz_asof_gappy |
| light + O + as-of high-vol | green O and prior Elite high-vol together | next 2 | +2.39% (n=806) | +1.18 pp | **KILL** | finviz_asof_gappy,disc_t |
| light + O + as-of high-vol | green O and prior Elite high-vol together | next 1 | +4.02% (n=738) | +2.28 pp | **KILL** | finviz_asof_gappy |
| light + O + as-of high-vol | green O and prior Elite high-vol together | next 2 | +3.77% (n=738) | +2.15 pp | **KILL** | finviz_asof_gappy |
| light + O + as-of high-vol | green O and prior Elite high-vol together | next 1 | +2.18% (n=778) | +0.86 pp | **KILL** | finviz_asof_gappy |
| light + O + as-of high-vol | green O and prior Elite high-vol together | next 2 | +7.22% (n=778) | +5.68 pp | **KILL** | finviz_asof_gappy,disc_t,hold_t,lottery_day |
| AB (yesterday) | Same morning light as `hyst_open_core_e5_x2`, and yesterday's AB tape already liked the name. | next 2 | +1.27% (n=251) | -0.35 pp | **KILL** | date_bar,lottery_day,tape_split,no_edge_vs_parent |
| weather (yesterday) | Same morning light as `hyst_open_core_e5_x2`, and prior weather was risk-off. | next 2 | +2.21% (n=496) | +0.59 pp | **KILL** | date_bar,lottery_day,tape_split |
| overnight book (yesterday) | Same morning light as `hyst_open_core_e5_x2`, and the overnight 1-day book had it as a buy. | next 2 | +1.69% (n=7) | +0.07 pp | **KILL** | thin_disc,thin_hold,disc_t,hold_t,ticker_bar,date_bar,lottery_trade,lottery_day,tape_split,no_edge_vs_parent |

### Second regime (Q1–Q2 vs Q3) on light + green O

Held-out regime starts **2026-07-01**. KEEP only if Q3 stays green, still beats the parent light by 20 bp on the ticker-holdout, and clears the usual tape / lottery bar.

| meaning | hold | Q1–Q2 | Q3 (held out) | ticker-holdout | vs light | fattest day | verdict | why |
|---|---|---|---|---|---|---|---|---|
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to 0. plus green O. | next 1 | +1.62% (n=8929) | +1.21% (n=3064) | +1.45% (n=4903) | +0.17 pp | 2.8% | **KILL** | no_edge_vs_parent |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to 0. plus green O. | next 2 | +1.61% (n=8929) | +1.00% (n=3064) | +1.38% (n=4903) | +0.17 pp | 7.9% | **KILL** | no_edge_vs_parent |
| the five morning cells that are already known at 9:30 (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. plus green O. | next 1 | +2.15% (n=8836) | +1.78% (n=3048) | +1.98% (n=4812) | +0.24 pp | 5.0% | **KEEP** | — |
| the five morning cells that are already known at 9:30 (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. plus green O. | next 2 | +2.04% (n=8836) | +1.72% (n=3048) | +1.88% (n=4812) | +0.27 pp | 6.9% | **KEEP** | — |
| all nine morning cells known at 9:30 (A, B, C, G, J, K, L, M, O) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. plus green O. | next 1 | +1.57% (n=9636) | +0.98% (n=3307) | +1.37% (n=5264) | +0.04 pp | 2.9% | **KILL** | no_edge_vs_parent |
| all nine morning cells known at 9:30 (A, B, C, G, J, K, L, M, O) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. plus green O. | next 2 | +1.50% (n=9636) | +2.37% (n=3307) | +1.99% (n=5264) | +0.45 pp | 13.0% | **KEEP** | — |

**Join finding.** The only incremental KEEP versus the light is a green **O**. AB / weather / book are yesterday-knowable but too short (late Aug–Sep) — **KILL**. Finviz high-vol as a **snapshot** is **BLOCKED** (not 2026 history). Dated Elite exports are too gappy to ship. Light+O still has to clear Q3; see the table. Research only. No live wire.

## Remaining A–JL families (unmined sweep)

# Remaining A–JL families — unmined sweep

_Generated 2026-09-07 · live `flatten_robust` frozen. Yahoo/rows A–F seed only. No merge._

## Plain English

Same four leftover families as the 400-name cut (open leftover numbers, close leftover numbers, leftover green/red highlights, 0/1 formula flags). This beat rebuilds A–JL from **every Yahoo/rows cache** (~3,603 names), not a new tiny hand list. Never Excel's STOCKHISTORY cache. Fees are Futubull. We only buy at the open when the sheet already knows the number or the color at 9:30; everything else waits for the close.

A keeper has to work on both ticker halves, both calendar halves (cut 2026-05-01), **Q3** (cut 2026-07-01), both SPY tapes, and must beat 'just buy everyone' by 20 bps. The fattest single day cannot be more than 25% of winning-day P&L. hold1 needs a hold2 sibling; hold5 needs hold2 edge.

Full rows-cache rebuild **3603** tickers (2137 discovery / 1461 holdout) · lean capture **1.2 s/ticker** · specs **2207** · scored cells **3031**.

**KEEP 126 · KILL 2890 · THIN 15** (raw PASS 126 / FAIL 2890 / THIN 15).

Live cards stay frozen. Light+green O remain the only standing A–O color keeps. Finviz volume stays BLOCKED. AB / weather / book stay dead. Leftover-family KEEPs below are research-only on this 2026 tape.

### Family scoreboard

| family | what it is | KEEP | KILL | THIN |
|---|---|---:|---:|---:|
| `val_open` | open-knowable numbers past A/C/J | 10 | 356 | 3 |
| `val_close` | close-knowable leftover numbers | 99 | 2104 | 12 |
| `fill_new` | leftover green/red highlights | 14 | 286 | 0 |
| `flag` | 0/1 formula flags | 3 | 144 | 0 |

### Unconditional baseline (this sample, Futubull)

| clock | side | hold | n | avg net | t | win |
|---|---|---:|---:|---:|---:|---:|
| open | long | 1 | 502208 | -0.04% | -0.9 | 45% |
| open | long | 2 | 498610 | +0.54% | 5.1 | 46% |
| open | long | 5 | 487816 | +1.45% | 10.9 | 47% |
| open | short | 1 | 502208 | -0.31% | -6.0 | 46% |
| open | short | 2 | 498610 | -0.89% | -8.4 | 47% |
| open | short | 5 | 487816 | -1.80% | -13.5 | 49% |
| close | long | 1 | 498610 | +0.48% | 5.2 | 45% |
| close | long | 2 | 495012 | +0.83% | 7.6 | 46% |
| close | long | 5 | 484218 | +1.71% | 13.6 | 47% |
| close | short | 1 | 498610 | -0.83% | -9.1 | 46% |
| close | short | 2 | 495012 | -1.18% | -10.9 | 47% |
| close | short | 5 | 484218 | -2.06% | -16.4 | 49% |

### KEEP (hardened, unique letters)

Raw KEEP **126** collapses to **66** letter×hold cells (**40** hold1/2) on letters `AH/AM/BA/BN/CJ/CZ/EH/EJ/EY/FC/FK/FL/FR/GV/HO/HZ/IB/IK/IL/N/R/T/U`. eq1/ge1/gt0 twins that fire the same days are counted once. These cleared the same bar as light+O (both halves, Q3, both tapes, top-day lottery, beat baseline, horizon sibling). Research only — one 2026 regime. No card.

| letter | what it is | clock | hold | disc | holdout | Q3 | vs everyone | day-lottery | def |
|---|---|---|---|---|---|---|---|---|---|
| **EH** | any of FP–FU is negative → 4, else 0 | close | hold2 | 180453/+1.31%/t=6.7 | 124724/+1.34%/t=5.4 | 116468/+2.74%/t=7.3 | +0.51 pp | 9.4% | `valclose_EH_ge1` |
| **BA** | a 0/1 stress flag (HN/CP) | close | hold2 | 111276/+2.06%/t=6.5 | 75291/+1.96%/t=5.0 | 69356/+4.42%/t=7.1 | +1.13 pp | 10.2% | `valclose_BA_eq1` |
| **CZ** | how many recent CP prints were negative | close | hold2 | 192876/+1.30%/t=7.0 | 131044/+1.51%/t=5.0 | 123062/+2.91%/t=7.0 | +0.68 pp | 8.7% | `valclose_CZ_ge2` |
| **FC** | carried-forward FA state | close | hold2 | 172664/+1.44%/t=6.9 | 117363/+1.65%/t=4.8 | 128649/+2.77%/t=7.0 | +0.82 pp | 9.0% | `valclose_FC_ge1` |
| **IK** | carried-forward HO | close | hold2 | 83091/+1.86%/t=5.7 | 54754/+2.16%/t=4.6 | 54787/+3.86%/t=6.2 | +1.33 pp | 8.4% | `valclose_IK_eq1` |
| **HZ** | carried-forward HT | close | hold2 | 221745/+1.09%/t=6.7 | 151661/+1.20%/t=4.6 | 159892/+2.20%/t=6.9 | +0.37 pp | 8.9% | `valclose_HZ_ge1` |
| **BN** | today's volume vs a ~50-day average | close | hold2 | 189800/+1.19%/t=6.3 | 129756/+1.39%/t=4.5 | 160019/+2.21%/t=6.9 | +0.56 pp | 10.0% | `valclose_BN_gt0` |
| **CJ** | a running price sum / 50 | close | hold2 | 208427/+1.10%/t=6.4 | 142665/+1.26%/t=4.5 | 161575/+2.18%/t=6.9 | +0.43 pp | 9.6% | `valclose_CJ_gt0` |
| **FK** | last open while FH is on | close | hold2 | 151020/+1.27%/t=6.3 | 100923/+1.70%/t=4.4 | 112401/+2.75%/t=6.5 | +0.87 pp | 8.2% | `valclose_FK_gt0` |
| **FL** | last open while X is on | close | hold2 | 152775/+1.43%/t=6.2 | 102181/+1.68%/t=4.4 | 112948/+3.00%/t=6.7 | +0.85 pp | 9.5% | `valclose_FL_gt0` |
| **IB** | a 0/1 count of HK/HM/HN/HS/HV/IA (IB≥3 was already mined; IB=1 is new) | close | hold2 | 46814/+2.14%/t=4.4 | 31778/+1.48%/t=4.1 | 32349/+3.17%/t=5.0 | +0.64 pp | 8.6% | `flag_IB_eq1` |
| **GV** | a 0/1 composite of several deeper flags | close | hold2 | 71885/+2.23%/t=5.1 | 49429/+2.56%/t=3.5 | 46011/+5.31%/t=5.3 | +1.73 pp | 8.7% | `valclose_GV_eq1` |
| **N** | EL times a 3% same-day move — stored A–O close fill, not past-O | close | hold2 | 57869/+1.30%/t=4.3 | 38944/+1.47%/t=3.4 | 64741/+1.80%/t=4.8 | +0.64 pp | 11.8% | `fill_N_green` |
| **BN** | today's volume vs a ~50-day average | close | hold2 | 61346/+1.14%/t=3.1 | 41897/+2.15%/t=3.3 | 41655/+3.47%/t=4.1 | +1.32 pp | 16.6% | `fill_BN_green` |
| **U** | alias of CV (CV fill was on the old hand list; U value/fill was not) | close | hold2 | 45241/+1.94%/t=4.3 | 30165/+2.39%/t=3.1 | 22399/+6.58%/t=4.9 | +1.56 pp | 10.1% | `fill_U_green` |
| **EJ** | a 0/1 stress flag (AA/AJ/L) | close | hold2 | 101838/+1.48%/t=5.0 | 69618/+1.38%/t=3.1 | 69333/+2.85%/t=4.8 | +0.55 pp | 8.5% | `valclose_EJ_eq1` |
| **FR** | recent volume over 1M and/or G ≥ 3 (open-knowable walk) | open | hold2 | 120379/+0.86%/t=4.3 | 85081/+0.94%/t=3.0 | 71327/+2.42%/t=4.9 | +0.39 pp | 8.9% | `valopen_FR_ge1` |
| **T** | alias of EN | close | hold2 | 40556/+2.19%/t=3.5 | 26772/+3.98%/t=3.0 | 24859/+7.73%/t=4.5 | +3.15 pp | 14.2% | `fill_T_green` |
| **AH** | count of recent same-day drops of 5% or more (open-knowable walk) | open | hold2 | 77814/+1.19%/t=4.0 | 51855/+2.22%/t=2.9 | 42703/+3.89%/t=4.2 | +1.67 pp | 8.7% | `valopen_AH_ge1` |
| **R** | column J when the Change-sheet flag Q is on | close | hold2 | 60746/+1.09%/t=3.0 | 41483/+1.56%/t=2.9 | 42037/+2.87%/t=3.9 | +0.73 pp | 17.0% | `fill_R_green` |
| **IL** | bins same-day return H (close-knowable) | close | hold2 | 39718/+2.86%/t=4.3 | 26155/+3.03%/t=2.9 | 21895/+7.47%/t=4.4 | +2.19 pp | 12.4% | `valclose_IL_eq1` |
| **N** | EL times a 3% same-day move — stored A–O close fill, not past-O | close | hold2 | 37344/+1.74%/t=3.7 | 24856/+1.53%/t=2.8 | 18459/+4.70%/t=4.1 | +0.70 pp | 13.3% | `valclose_N_ge1` |
| **EY** | a short-window max of EX | close | hold2 | 86868/+1.12%/t=5.3 | 58433/+1.28%/t=2.7 | 54568/+2.78%/t=4.6 | +0.45 pp | 12.8% | `valclose_EY_ge1` |
| **AM** | carried-forward deeper state | close | hold2 | 85182/+1.15%/t=4.5 | 56284/+1.36%/t=2.5 | 45810/+3.63%/t=4.5 | +0.53 pp | 8.0% | `valclose_AM_gt0` |
| **HO** | a signed HN/GU cross | close | hold2 | 14286/+2.78%/t=3.6 | 9528/+3.41%/t=2.0 | 8209/+7.36%/t=3.2 | +2.58 pp | 19.8% | `valclose_HO_eq1` |
| **HO** | a signed HN/GU cross | close | hold2 | 14286/+2.78%/t=3.6 | 9528/+3.41%/t=2.0 | 8209/+7.36%/t=3.2 | +2.58 pp | 19.8% | `fill_HO_green` |
| **CZ** | how many recent CP prints were negative | close | hold1 | 194476/+0.69%/t=5.0 | 132153/+0.97%/t=3.5 | 125771/+1.85%/t=5.3 | +0.50 pp | 9.8% | `valclose_CZ_ge2` |
| **FC** | carried-forward FA state | close | hold1 | 174401/+0.79%/t=5.1 | 118554/+1.08%/t=3.5 | 131577/+1.77%/t=5.3 | +0.60 pp | 9.8% | `valclose_FC_ge1` |
| **IK** | carried-forward HO | close | hold1 | 83749/+1.09%/t=4.1 | 55217/+1.33%/t=3.4 | 55908/+2.59%/t=4.9 | +0.86 pp | 10.5% | `valclose_IK_eq1` |
| **BA** | a 0/1 stress flag (HN/CP) | close | hold1 | 112469/+1.09%/t=4.7 | 76076/+1.57%/t=3.4 | 71334/+3.03%/t=5.1 | +1.09 pp | 9.5% | `valclose_BA_eq1` |
| **IB** | a 0/1 count of HK/HM/HN/HS/HV/IA (IB≥3 was already mined; IB=1 is new) | close | hold1 | 47386/+1.22%/t=3.4 | 32167/+1.01%/t=3.3 | 33310/+2.11%/t=4.1 | +0.54 pp | 12.4% | `flag_IB_eq1` |
| **FL** | last open while X is on | close | hold1 | 154328/+0.80%/t=4.6 | 103229/+1.14%/t=3.3 | 115549/+1.94%/t=5.1 | +0.67 pp | 10.5% | `valclose_FL_gt0` |
| **FK** | last open while FH is on | close | hold1 | 152566/+0.72%/t=4.4 | 101965/+1.15%/t=3.3 | 114989/+1.82%/t=4.9 | +0.68 pp | 11.3% | `valclose_FK_gt0` |
| **IL** | bins same-day return H (close-knowable) | close | hold1 | 39986/+1.50%/t=3.7 | 26313/+1.86%/t=3.0 | 22321/+3.99%/t=4.0 | +1.38 pp | 14.7% | `valclose_IL_eq1` |
| **N** | EL times a 3% same-day move — stored A–O close fill, not past-O | close | hold1 | 58866/+0.87%/t=3.1 | 39635/+0.72%/t=2.6 | 66429/+1.10%/t=3.8 | +0.25 pp | 22.6% | `fill_N_green` |
| **GV** | a 0/1 composite of several deeper flags | close | hold1 | 72432/+1.10%/t=3.7 | 49804/+1.78%/t=2.6 | 46933/+3.33%/t=4.0 | +1.30 pp | 11.7% | `valclose_GV_eq1` |
| **N** | EL times a 3% same-day move — stored A–O close fill, not past-O | close | hold1 | 37583/+1.33%/t=3.1 | 25018/+1.32%/t=2.6 | 18860/+3.80%/t=3.6 | +0.84 pp | 23.9% | `valclose_N_ge1` |
| **HZ** | carried-forward HT | close | hold1 | 88317/+0.86%/t=3.7 | 60186/+0.96%/t=2.4 | 65037/+2.00%/t=4.2 | +0.49 pp | 12.5% | `valclose_HZ_eq1` |
| **T** | alias of EN | close | hold1 | 40775/+1.44%/t=3.1 | 26909/+2.99%/t=2.4 | 25215/+5.50%/t=3.6 | +2.52 pp | 12.7% | `fill_T_green` |
| **EY** | a short-window max of EX | close | hold1 | 87368/+0.75%/t=3.6 | 58787/+1.12%/t=2.2 | 55422/+2.26%/t=3.6 | +0.65 pp | 15.8% | `valclose_EY_ge1` |

### Near-miss / top KILL (FAIL, hold1/2 first)

| keep | def | family | clock | side | exit | disc | hold | Q3 | tickers | why |
|---|---|---|---|---|---|---|---|---|---:|---|
| KILL | `fill_DE_red` | fill_new | close | short | hold2 | 40603/-0.04%/t=-0.4 | 27759/+0.11%/t=2.5 | — | 3598 | disc_t,disc_sign,lottery_day,spy_regime,q3_missing |
| KILL | `fill_DE_red` | fill_new | close | short | hold1 | 40603/-0.13%/t=-2.0 | 27759/-0.05%/t=-1.8 | — | 3598 | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_missing |
| KILL | `valclose_BE_ge2` | val_close | close | long | hold1 | 1807/+1.75%/t=10.9 | 1238/+1.71%/t=14.6 | — | 3045 | lottery_day,tape_thin,spy_thin,q3_missing |
| KILL | `valclose_BE_ge1` | val_close | close | long | hold1 | 2013/+1.73%/t=11.6 | 1359/+1.69%/t=14.3 | — | 3372 | lottery_day,tape_thin,spy_thin,q3_missing |
| KILL | `valclose_AO_le-1` | val_close | close | short | hold1 | 1747/+1.62%/t=16.7 | 1202/+1.58%/t=14.3 | — | 2949 | date_bar,lottery_day,tape_thin,spy_thin,q3_missing |

### Ghost that looked like a KEEP (then died)

Column **DE** paints red when the cell equals 0 (green when it equals 1). The formula that writes DE also reads same-day volume and same-day return H, so we only enter at the **close**.
 `fill_DE_red` short hold5: disc 40603/+0.59%/t=4.1, hold 27759/+0.91%/t=14.0, Q3 —, late 327/+2.23%/t=3.0. hold2: 27759/+0.11%/t=2.5. **KILL** (q3_missing).

### Exhaustion

126 leftover cells KEEP on the full Yahoo/rows set. The regions with KEEP 0 are null here; KEEP rows still need another regime before anyone would wire a card.

A–F seed: Yahoo/rows cache via `seed_anchor`. Capture path `lean_rows_cache` · excel STOCKHISTORY cache used: False.

Research only. No cards. Live `flatten_robust` untouched.

## Leftover KEEP harden (unique letters)

# Leftover KEEP harden — unique letters

_Generated 2026-09-07 · live `flatten_robust` frozen. Yahoo/rows A–F seed only. No merge._

## Plain English

The full 3,603-name dump printed **126** leftover KEEP rows. Most of those are the same letter written three ways (equals 1 / at least 1 / greater than 0), or a green highlight and a number that turn on the same days. We keep **one** survivor per letter, side, and hold, prefer the next-day and same-day holds, then re-check the ship bar: Futubull fees, both ticker halves, both calendar halves, **Q3 (2026-07-01)**, both SPY tapes, fattest day under 25% of winning-day P&L, and at least 20 bps better than buying everyone.

Five-session holds that only looked good because the tape was up are dropped. Twin defs are **KILL**. Light+green O on the A–O strip is unchanged. No card.

Raw KEEP **126** → unique letter×side×hold **59** → after ship bar **KEEP 36** · **KILL 23** · **THIN 0**. Sleeve hold1/2 KEEP **36** on letters `AH/AM/BA/BN/CJ/CZ/EH/EJ/EY/FC/FK/FL/FR/GV/HO/HZ/IB/IK/IL/N/R/T/U`. Twins killed **67**. N=3603.

### Shortboard (hold2 survivors)

Ranked by how much they beat 'buy everyone' after fees. Featured letters first (T, BA, AH, CZ, EH, IB), then peers.

| rank | letter | what it is | clock | holdout | vs everyone | Q3 | day-lottery | tickers | verdict |
|---:|---|---|---|---|---|---|---|---:|---|
| 1 | **T** | alias of EN | close | 26772/+3.98%/t=3.0 | +3.15 pp | 24859/+7.73%/t=4.5 | 14.2% | 2827 | **KEEP** |
| 2 | **BA** | a 0/1 stress flag (HN/CP) | close | 75291/+1.96%/t=5.0 | +1.13 pp | 69356/+4.42%/t=7.1 | 10.2% | 3594 | **KEEP** |
| 3 | **AH** | count of recent same-day drops of 5% or more (open-knowable walk) | open | 51855/+2.22%/t=2.9 | +1.67 pp | 42703/+3.89%/t=4.2 | 8.7% | 2802 | **KEEP** |
| 4 | **CZ** | how many recent CP prints were negative | close | 131044/+1.51%/t=5.0 | +0.68 pp | 123062/+2.91%/t=7.0 | 8.7% | 3593 | **KEEP** |
| 5 | **EH** | any of FP–FU is negative → 4, else 0 | close | 124724/+1.34%/t=5.4 | +0.51 pp | 116468/+2.74%/t=7.3 | 9.4% | 3598 | **KEEP** |
| 6 | **IB** | a 0/1 count of HK/HM/HN/HS/HV/IA (IB≥3 was already mined; IB=1 is new) | close | 31778/+1.48%/t=4.1 | +0.64 pp | 32349/+3.17%/t=5.0 | 8.6% | 3595 | **KEEP** |
| 7 | **HO** | a signed HN/GU cross | close | 9528/+3.41%/t=2.0 | +2.58 pp | 8209/+7.36%/t=3.2 | 19.8% | 2961 | **KEEP** |
| 8 | **IL** | bins same-day return H (close-knowable) | close | 26155/+3.03%/t=2.9 | +2.19 pp | 21895/+7.47%/t=4.4 | 12.4% | 3315 | **KEEP** |
| 9 | **GV** | a 0/1 composite of several deeper flags | close | 49429/+2.56%/t=3.5 | +1.73 pp | 46011/+5.31%/t=5.3 | 8.7% | 3598 | **KEEP** |
| 10 | **U** | alias of CV (CV fill was on the old hand list; U value/fill was not) | close | 30165/+2.39%/t=3.1 | +1.56 pp | 22399/+6.58%/t=4.9 | 10.1% | 3025 | **KEEP** |
| 11 | **IK** | carried-forward HO | close | 54754/+2.16%/t=4.6 | +1.33 pp | 54787/+3.86%/t=6.2 | 8.4% | 2961 | **KEEP** |
| 12 | **FK** | last open while FH is on | close | 100923/+1.70%/t=4.4 | +0.87 pp | 112401/+2.75%/t=6.5 | 8.2% | 2587 | **KEEP** |
| 13 | **FL** | last open while X is on | close | 102181/+1.68%/t=4.4 | +0.85 pp | 112948/+3.00%/t=6.7 | 9.5% | 3041 | **KEEP** |
| 14 | **FC** | carried-forward FA state | close | 117363/+1.65%/t=4.8 | +0.82 pp | 128649/+2.77%/t=7.0 | 9.0% | 2928 | **KEEP** |
| 15 | **R** | column J when the Change-sheet flag Q is on | close | 41483/+1.56%/t=2.9 | +0.73 pp | 42037/+2.87%/t=3.9 | 17.0% | 3588 | **KEEP** |
| 16 | **N** | EL times a 3% same-day move — stored A–O close fill, not past-O | close | 38944/+1.47%/t=3.4 | +0.64 pp | 64741/+1.80%/t=4.8 | 11.8% | 3594 | **KEEP** |
| 17 | **BN** | today's volume vs a ~50-day average | close | 129756/+1.39%/t=4.5 | +0.56 pp | 160019/+2.21%/t=6.9 | 10.0% | 3589 | **KEEP** |
| 18 | **EJ** | a 0/1 stress flag (AA/AJ/L) | close | 69618/+1.38%/t=3.1 | +0.55 pp | 69333/+2.85%/t=4.8 | 8.5% | 3579 | **KEEP** |
| 19 | **AM** | carried-forward deeper state | close | 56284/+1.36%/t=2.5 | +0.53 pp | 45810/+3.63%/t=4.5 | 8.0% | 3057 | **KEEP** |
| 20 | **EY** | a short-window max of EX | close | 58433/+1.28%/t=2.7 | +0.45 pp | 54568/+2.78%/t=4.6 | 12.8% | 2986 | **KEEP** |
| 21 | **CJ** | a running price sum / 50 | close | 142665/+1.26%/t=4.5 | +0.43 pp | 161575/+2.18%/t=6.9 | 9.6% | 3598 | **KEEP** |
| 22 | **FR** | recent volume over 1M and/or G ≥ 3 (open-knowable walk) | open | 85081/+0.94%/t=3.0 | +0.39 pp | 71327/+2.42%/t=4.9 | 8.9% | 3472 | **KEEP** |
| 23 | **HZ** | carried-forward HT | close | 151661/+1.20%/t=4.6 | +0.37 pp | 159892/+2.20%/t=6.9 | 8.9% | 3573 | **KEEP** |

### Unique-letter KEEP list (hold1/2)

| letter | hold | clock | side | what it is | disc | holdout | Q3 | vs everyone | day-lottery | survivor def |
|---|---|---|---|---|---|---|---|---|---|---|
| **T** | hold2 | close | long | alias of EN | 40556/+2.19%/t=3.5 | 26772/+3.98%/t=3.0 | 24859/+7.73%/t=4.5 | +3.15 pp | 14.2% | `fill_T_green` |
| **HO** | hold2 | close | long | a signed HN/GU cross | 14286/+2.78%/t=3.6 | 9528/+3.41%/t=2.0 | 8209/+7.36%/t=3.2 | +2.58 pp | 19.8% | `valclose_HO_eq1` |
| **IL** | hold2 | close | long | bins same-day return H (close-knowable) | 39718/+2.86%/t=4.3 | 26155/+3.03%/t=2.9 | 21895/+7.47%/t=4.4 | +2.19 pp | 12.4% | `valclose_IL_eq1` |
| **GV** | hold2 | close | long | a 0/1 composite of several deeper flags | 71885/+2.23%/t=5.1 | 49429/+2.56%/t=3.5 | 46011/+5.31%/t=5.3 | +1.73 pp | 8.7% | `valclose_GV_eq1` |
| **AH** | hold2 | open | long | count of recent same-day drops of 5% or more (open-knowable walk) | 77814/+1.19%/t=4.0 | 51855/+2.22%/t=2.9 | 42703/+3.89%/t=4.2 | +1.67 pp | 8.7% | `valopen_AH_ge1` |
| **U** | hold2 | close | long | alias of CV (CV fill was on the old hand list; U value/fill was not) | 45241/+1.94%/t=4.3 | 30165/+2.39%/t=3.1 | 22399/+6.58%/t=4.9 | +1.56 pp | 10.1% | `fill_U_green` |
| **IK** | hold2 | close | long | carried-forward HO | 83091/+1.86%/t=5.7 | 54754/+2.16%/t=4.6 | 54787/+3.86%/t=6.2 | +1.33 pp | 8.4% | `valclose_IK_eq1` |
| **BA** | hold2 | close | long | a 0/1 stress flag (HN/CP) | 111276/+2.06%/t=6.5 | 75291/+1.96%/t=5.0 | 69356/+4.42%/t=7.1 | +1.13 pp | 10.2% | `valclose_BA_eq1` |
| **FK** | hold2 | close | long | last open while FH is on | 151020/+1.27%/t=6.3 | 100923/+1.70%/t=4.4 | 112401/+2.75%/t=6.5 | +0.87 pp | 8.2% | `valclose_FK_gt0` |
| **FL** | hold2 | close | long | last open while X is on | 152775/+1.43%/t=6.2 | 102181/+1.68%/t=4.4 | 112948/+3.00%/t=6.7 | +0.85 pp | 9.5% | `valclose_FL_gt0` |
| **FC** | hold2 | close | long | carried-forward FA state | 172664/+1.44%/t=6.9 | 117363/+1.65%/t=4.8 | 128649/+2.77%/t=7.0 | +0.82 pp | 9.0% | `valclose_FC_ge1` |
| **R** | hold2 | close | long | column J when the Change-sheet flag Q is on | 60746/+1.09%/t=3.0 | 41483/+1.56%/t=2.9 | 42037/+2.87%/t=3.9 | +0.73 pp | 17.0% | `fill_R_green` |
| **CZ** | hold2 | close | long | how many recent CP prints were negative | 192876/+1.30%/t=7.0 | 131044/+1.51%/t=5.0 | 123062/+2.91%/t=7.0 | +0.68 pp | 8.7% | `valclose_CZ_ge2` |
| **IB** | hold2 | close | long | a 0/1 count of HK/HM/HN/HS/HV/IA (IB≥3 was already mined; IB=1 is new) | 46814/+2.14%/t=4.4 | 31778/+1.48%/t=4.1 | 32349/+3.17%/t=5.0 | +0.64 pp | 8.6% | `flag_IB_eq1` |
| **N** | hold2 | close | long | EL times a 3% same-day move — stored A–O close fill, not past-O | 57869/+1.30%/t=4.3 | 38944/+1.47%/t=3.4 | 64741/+1.80%/t=4.8 | +0.64 pp | 11.8% | `fill_N_green` |
| **BN** | hold2 | close | long | today's volume vs a ~50-day average | 189800/+1.19%/t=6.3 | 129756/+1.39%/t=4.5 | 160019/+2.21%/t=6.9 | +0.56 pp | 10.0% | `valclose_BN_gt0` |
| **EJ** | hold2 | close | long | a 0/1 stress flag (AA/AJ/L) | 101838/+1.48%/t=5.0 | 69618/+1.38%/t=3.1 | 69333/+2.85%/t=4.8 | +0.55 pp | 8.5% | `valclose_EJ_eq1` |
| **AM** | hold2 | close | long | carried-forward deeper state | 85182/+1.15%/t=4.5 | 56284/+1.36%/t=2.5 | 45810/+3.63%/t=4.5 | +0.53 pp | 8.0% | `valclose_AM_gt0` |
| **EH** | hold2 | close | long | any of FP–FU is negative → 4, else 0 | 180453/+1.31%/t=6.7 | 124724/+1.34%/t=5.4 | 116468/+2.74%/t=7.3 | +0.51 pp | 9.4% | `valclose_EH_ge1` |
| **EY** | hold2 | close | long | a short-window max of EX | 86868/+1.12%/t=5.3 | 58433/+1.28%/t=2.7 | 54568/+2.78%/t=4.6 | +0.45 pp | 12.8% | `valclose_EY_ge1` |
| **CJ** | hold2 | close | long | a running price sum / 50 | 208427/+1.10%/t=6.4 | 142665/+1.26%/t=4.5 | 161575/+2.18%/t=6.9 | +0.43 pp | 9.6% | `valclose_CJ_gt0` |
| **FR** | hold2 | open | long | recent volume over 1M and/or G ≥ 3 (open-knowable walk) | 120379/+0.86%/t=4.3 | 85081/+0.94%/t=3.0 | 71327/+2.42%/t=4.9 | +0.39 pp | 8.9% | `valopen_FR_ge1` |
| **HZ** | hold2 | close | long | carried-forward HT | 221745/+1.09%/t=6.7 | 151661/+1.20%/t=4.6 | 159892/+2.20%/t=6.9 | +0.37 pp | 8.9% | `valclose_HZ_ge1` |
| **T** | hold1 | close | long | alias of EN | 40775/+1.44%/t=3.1 | 26909/+2.99%/t=2.4 | 25215/+5.50%/t=3.6 | +2.52 pp | 12.7% | `fill_T_green` |
| **IL** | hold1 | close | long | bins same-day return H (close-knowable) | 39986/+1.50%/t=3.7 | 26313/+1.86%/t=3.0 | 22321/+3.99%/t=4.0 | +1.38 pp | 14.7% | `valclose_IL_eq1` |
| **GV** | hold1 | close | long | a 0/1 composite of several deeper flags | 72432/+1.10%/t=3.7 | 49804/+1.78%/t=2.6 | 46933/+3.33%/t=4.0 | +1.30 pp | 11.7% | `valclose_GV_eq1` |
| **BA** | hold1 | close | long | a 0/1 stress flag (HN/CP) | 112469/+1.09%/t=4.7 | 76076/+1.57%/t=3.4 | 71334/+3.03%/t=5.1 | +1.09 pp | 9.5% | `valclose_BA_eq1` |
| **IK** | hold1 | close | long | carried-forward HO | 83749/+1.09%/t=4.1 | 55217/+1.33%/t=3.4 | 55908/+2.59%/t=4.9 | +0.86 pp | 10.5% | `valclose_IK_eq1` |
| **FK** | hold1 | close | long | last open while FH is on | 152566/+0.72%/t=4.4 | 101965/+1.15%/t=3.3 | 114989/+1.82%/t=4.9 | +0.68 pp | 11.3% | `valclose_FK_gt0` |
| **FL** | hold1 | close | long | last open while X is on | 154328/+0.80%/t=4.6 | 103229/+1.14%/t=3.3 | 115549/+1.94%/t=5.1 | +0.67 pp | 10.5% | `valclose_FL_gt0` |
| **EY** | hold1 | close | long | a short-window max of EX | 87368/+0.75%/t=3.6 | 58787/+1.12%/t=2.2 | 55422/+2.26%/t=3.6 | +0.65 pp | 15.8% | `valclose_EY_ge1` |
| **FC** | hold1 | close | long | carried-forward FA state | 174401/+0.79%/t=5.1 | 118554/+1.08%/t=3.5 | 131577/+1.77%/t=5.3 | +0.60 pp | 9.8% | `valclose_FC_ge1` |
| **IB** | hold1 | close | long | a 0/1 count of HK/HM/HN/HS/HV/IA (IB≥3 was already mined; IB=1 is new) | 47386/+1.22%/t=3.4 | 32167/+1.01%/t=3.3 | 33310/+2.11%/t=4.1 | +0.54 pp | 12.4% | `flag_IB_eq1` |
| **CZ** | hold1 | close | long | how many recent CP prints were negative | 194476/+0.69%/t=5.0 | 132153/+0.97%/t=3.5 | 125771/+1.85%/t=5.3 | +0.50 pp | 9.8% | `valclose_CZ_ge2` |
| **HZ** | hold1 | close | long | carried-forward HT | 88317/+0.86%/t=3.7 | 60186/+0.96%/t=2.4 | 65037/+2.00%/t=4.2 | +0.49 pp | 12.5% | `valclose_HZ_eq1` |
| **N** | hold1 | close | long | EL times a 3% same-day move — stored A–O close fill, not past-O | 58866/+0.87%/t=3.1 | 39635/+0.72%/t=2.6 | 66429/+1.10%/t=3.8 | +0.25 pp | 22.6% | `fill_N_green` |

### Killed twins

Same letter, same days (or a weaker fill/value of the same letter). Not a second edge.

| killed def | letter | hold | why | survivor |
|---|---|---|---|---|
| `valopen_AH_ge2` | AH | hold2 | twin_weaker | `valopen_AH_ge1` |
| `valopen_AH_gt0` | AH | hold2 | twin_same_days | `valopen_AH_ge1` |
| `valopen_AH_ge2` | AH | hold5 | twin_weaker | `valopen_AH_ge1` |
| `valopen_AH_gt0` | AH | hold5 | twin_same_days | `valopen_AH_ge1` |
| `valclose_BA_ge1` | BA | hold1 | twin_same_days | `valclose_BA_eq1` |
| `valclose_BA_gt0` | BA | hold1 | twin_same_days | `valclose_BA_eq1` |
| `valclose_BA_ge1` | BA | hold2 | twin_same_days | `valclose_BA_eq1` |
| `valclose_BA_gt0` | BA | hold2 | twin_same_days | `valclose_BA_eq1` |
| `valclose_BA_ge1` | BA | hold5 | twin_same_days | `valclose_BA_eq1` |
| `valclose_BA_gt0` | BA | hold5 | twin_same_days | `valclose_BA_eq1` |
| `fill_BN_green` | BN | hold2 | twin_weaker | `valclose_BN_gt0` |
| `valclose_BN_ge1` | BN | hold2 | twin_weaker | `valclose_BN_gt0` |
| `fill_BN_green` | BN | hold5 | twin_weaker | `valclose_BN_gt0` |
| `valclose_BN_ge1` | BN | hold5 | twin_weaker | `valclose_BN_gt0` |
| `valclose_CZ_ge1` | CZ | hold2 | twin_weaker | `valclose_CZ_ge2` |
| `valclose_CZ_gt0` | CZ | hold2 | twin_weaker | `valclose_CZ_ge2` |
| `valclose_CZ_ge1` | CZ | hold5 | twin_weaker | `valclose_CZ_ge2` |
| `valclose_CZ_gt0` | CZ | hold5 | twin_weaker | `valclose_CZ_ge2` |
| `valclose_EH_ge2` | EH | hold2 | twin_same_days | `valclose_EH_ge1` |
| `valclose_EH_gt0` | EH | hold2 | twin_same_days | `valclose_EH_ge1` |
| `valclose_EH_ge2` | EH | hold5 | twin_same_days | `valclose_EH_ge1` |
| `valclose_EH_gt0` | EH | hold5 | twin_same_days | `valclose_EH_ge1` |
| `valclose_EJ_ge1` | EJ | hold2 | twin_same_days | `valclose_EJ_eq1` |
| `valclose_EJ_gt0` | EJ | hold2 | twin_same_days | `valclose_EJ_eq1` |
| `valclose_EJ_ge1` | EJ | hold5 | twin_same_days | `valclose_EJ_eq1` |
| `valclose_EJ_gt0` | EJ | hold5 | twin_same_days | `valclose_EJ_eq1` |
| `valclose_EY_gt0` | EY | hold1 | twin_same_days | `valclose_EY_ge1` |
| `valclose_EY_eq1` | EY | hold2 | twin_weaker | `valclose_EY_ge1` |
| `valclose_EY_gt0` | EY | hold2 | twin_same_days | `valclose_EY_ge1` |
| `valclose_EY_eq1` | EY | hold5 | twin_weaker | `valclose_EY_ge1` |
| `valclose_EY_gt0` | EY | hold5 | twin_same_days | `valclose_EY_ge1` |
| `valclose_FC_eq1` | FC | hold1 | twin_weaker | `valclose_FC_ge1` |
| `valclose_FC_gt0` | FC | hold1 | twin_same_days | `valclose_FC_ge1` |
| `valclose_FC_eq1` | FC | hold2 | twin_weaker | `valclose_FC_ge1` |
| `valclose_FC_gt0` | FC | hold2 | twin_same_days | `valclose_FC_ge1` |
| `valclose_FC_eq1` | FC | hold5 | twin_weaker | `valclose_FC_ge1` |
| `valclose_FC_gt0` | FC | hold5 | twin_same_days | `valclose_FC_ge1` |
| `valopen_FR_gt0` | FR | hold2 | twin_same_days | `valopen_FR_ge1` |
| `valopen_FR_gt0` | FR | hold5 | twin_same_days | `valopen_FR_ge1` |
| `valclose_GV_ge1` | GV | hold1 | twin_same_days | `valclose_GV_eq1` |
| `valclose_GV_gt0` | GV | hold1 | twin_same_days | `valclose_GV_eq1` |
| `valclose_GV_ge1` | GV | hold2 | twin_same_days | `valclose_GV_eq1` |
| `valclose_GV_gt0` | GV | hold2 | twin_same_days | `valclose_GV_eq1` |
| `valclose_GV_ge1` | GV | hold5 | twin_same_days | `valclose_GV_eq1` |
| `valclose_GV_gt0` | GV | hold5 | twin_same_days | `valclose_GV_eq1` |
| `fill_HO_green` | HO | hold2 | twin_same_days | `valclose_HO_eq1` |
| `valclose_HO_ge1` | HO | hold2 | twin_same_days | `valclose_HO_eq1` |
| `valclose_HO_gt0` | HO | hold2 | twin_same_days | `valclose_HO_eq1` |
| `fill_HO_green` | HO | hold5 | twin_same_days | `valclose_HO_eq1` |
| `valclose_HO_ge1` | HO | hold5 | twin_same_days | `valclose_HO_eq1` |
| `valclose_HO_gt0` | HO | hold5 | twin_same_days | `valclose_HO_eq1` |
| `valclose_HZ_eq1` | HZ | hold2 | twin_weaker | `valclose_HZ_ge1` |
| `valclose_HZ_gt0` | HZ | hold2 | twin_same_days | `valclose_HZ_ge1` |
| `valclose_HZ_eq1` | HZ | hold5 | twin_weaker | `valclose_HZ_ge1` |
| `valclose_HZ_gt0` | HZ | hold5 | twin_same_days | `valclose_HZ_ge1` |
| `valclose_IK_ge1` | IK | hold1 | twin_same_days | `valclose_IK_eq1` |
| `valclose_IK_gt0` | IK | hold1 | twin_same_days | `valclose_IK_eq1` |
| `valclose_IK_ge1` | IK | hold2 | twin_same_days | `valclose_IK_eq1` |
| `valclose_IK_gt0` | IK | hold2 | twin_same_days | `valclose_IK_eq1` |
| `valclose_IK_ge1` | IK | hold5 | twin_same_days | `valclose_IK_eq1` |
| `valclose_IK_gt0` | IK | hold5 | twin_same_days | `valclose_IK_eq1` |
| `valclose_N_ge1` | N | hold1 | twin_weaker | `fill_N_green` |
| `valclose_N_gt0` | N | hold1 | twin_weaker | `fill_N_green` |
| `valclose_N_ge1` | N | hold2 | twin_weaker | `fill_N_green` |
| `valclose_N_gt0` | N | hold2 | twin_weaker | `fill_N_green` |
| `valclose_N_ge1` | N | hold5 | twin_weaker | `fill_N_green` |
| `valclose_N_gt0` | N | hold5 | twin_weaker | `fill_N_green` |

### Survivors that died on re-judge

| letter | hold | def | verdict | why |
|---|---|---|---|---|
| HO | hold5 | `valclose_HO_eq1` | **KILL** | hold5_not_primary |
| T | hold5 | `fill_T_green` | **KILL** | hold5_not_primary |
| U | hold5 | `fill_U_green` | **KILL** | hold5_not_primary |
| IK | hold5 | `valclose_IK_eq1` | **KILL** | hold5_not_primary |
| GV | hold5 | `valclose_GV_eq1` | **KILL** | hold5_not_primary |
| BA | hold5 | `valclose_BA_eq1` | **KILL** | hold5_not_primary |
| AH | hold5 | `valopen_AH_ge1` | **KILL** | hold5_not_primary |
| IL | hold5 | `valclose_IL_eq1` | **KILL** | hold5_not_primary |
| N | hold5 | `fill_N_green` | **KILL** | hold5_not_primary |
| IB | hold5 | `flag_IB_eq1` | **KILL** | hold5_not_primary |
| FC | hold5 | `valclose_FC_ge1` | **KILL** | hold5_not_primary |
| FK | hold5 | `valclose_FK_gt0` | **KILL** | hold5_not_primary |
| EH | hold5 | `valclose_EH_ge1` | **KILL** | hold5_not_primary |
| CZ | hold5 | `valclose_CZ_ge2` | **KILL** | hold5_not_primary |
| EY | hold5 | `valclose_EY_ge1` | **KILL** | hold5_not_primary |
| FL | hold5 | `valclose_FL_gt0` | **KILL** | hold5_not_primary |
| FR | hold5 | `valopen_FR_ge1` | **KILL** | hold5_not_primary |
| R | hold5 | `fill_R_green` | **KILL** | hold5_not_primary |
| BN | hold5 | `valclose_BN_gt0` | **KILL** | hold5_not_primary |
| AM | hold5 | `valclose_AM_gt0` | **KILL** | hold5_not_primary |
| EJ | hold5 | `valclose_EJ_eq1` | **KILL** | hold5_not_primary |
| CJ | hold5 | `valclose_CJ_gt0` | **KILL** | hold5_not_primary |
| HZ | hold5 | `valclose_HZ_ge1` | **KILL** | hold5_not_primary |

### What this does not change

- Standing A–O research keeps stay the **three light + green O** recipes. This table does not touch them.
- Finviz volume stays **BLOCKED**. AB / weather / book stay dead.
- Live `flatten_robust` is not imported or changed. No cards.
- Calendar half cut **2026-05-01**. Q3 cut **2026-07-01**. Futubull 0.15% long / 0.20% short.

Research only. One 2026 regime.

## Open-stack verdict (light+O vs AH / FR)

_Generated 2026-09-07 · live `flatten_robust` frozen. Yahoo/rows A–F seed only. No merge. Close-letter shortboard (T, BA, …) is out of scope._

## Plain English

The leftover harden left two open-knowable unique KEEPs: **AH** (recent 5% down-day count) and **FR** (recent volume over 1M and/or G ≥ 3). The standing open keep is still **light + green O** (three recipes after Q3). This beat asks: if you already wait for that morning light and a green O, does also requiring AH≥1 or FR≥1 add money after Futubull fees?

**On the five-cell light + green O, AH is stronger** (about +116 to +133 bp after fees) and **FR is stronger** (about +35 bp). Both KEEP hold1 and hold2. **On the nine-cell light + green O both stacks print stronger but KILL** — AH is a one-day lottery (fattest day over 25%) with a weak holdout t, and FR misses the holdout t-bar. AH alone already KEEP as a leftover letter and is a bit stronger than light+O on a much wider book. FR alone already KEEP versus buy-everyone, but **weaker** than light+O — it is not a substitute for the light. Neither letter is a new standing open keep unless the stack itself clears.

Ship bar is the same as light+O: both ticker halves, both calendar halves (cut 2026-05-01), **Q3 (2026-07-01)**, both SPY tapes, fattest day under 25% of winning-day P&L, and at least 20 bp better than the parent (light+O on the same dump-covered dates). Grids **3603**. Names with both a grid and an AH/FR dump: **3603**.

Stacks: **KEEP 4** · **KILL 2** · **THIN 0**. Standing light+O recipes stay KEEP. No card.

### Standing light+O (baseline KEEP)

Published Q3 numbers, then the same recipes re-scored only on days that also have an AH/FR dump (the parent used for stacks).

| recipe | hold | published holdout | overlap holdout | Q3 (overlap) | verdict |
|---|---|---|---|---|---|
| the five morning cells that are already known at 9:30 (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. plus green O. | next 1 | +1.98% (n=4812) | +1.86% (n=4345) | +1.78% (n=3048) | **KEEP** |
| the five morning cells that are already known at 9:30 (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. plus green O. | next 2 | +1.88% (n=4812) | +1.81% (n=4345) | +1.72% (n=3048) | **KEEP** |
| all nine morning cells known at 9:30 (A, B, C, G, J, K, L, M, O) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. plus green O. | next 2 | +1.99% (n=5264) | +2.09% (n=4500) | +2.37% (n=3307) | **KEEP** |

### Stacks vs light+O

| recipe | hold | layer | holdout | vs light+O | Q3 | day-lottery | verdict | why | code |
|---|---|---|---|---|---|---|---|---|---|
| the five morning cells that are already known at 9:30 (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. plus green O. | next 1 | light+O ∧ AH≥1 | +3.19% (n=1317) | +1.33 pp (stronger) | +3.02% (n=927) | 10.5% | **KEEP** | — | `hyst_open_core_e5_x2__O_green__AH_ge1` |
| the five morning cells that are already known at 9:30 (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. plus green O. | next 1 | light+O ∧ FR≥1 | +2.21% (n=2282) | +0.35 pp (stronger) | +2.08% (n=1676) | 8.6% | **KEEP** | — | `hyst_open_core_e5_x2__O_green__FR_ge1` |
| the five morning cells that are already known at 9:30 (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. plus green O. | next 2 | light+O ∧ AH≥1 | +2.98% (n=1317) | +1.16 pp (stronger) | +3.50% (n=927) | 14.2% | **KEEP** | — | `hyst_open_core_e5_x2__O_green__AH_ge1` |
| the five morning cells that are already known at 9:30 (A, B, C, G, J) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. plus green O. | next 2 | light+O ∧ FR≥1 | +2.16% (n=2282) | +0.35 pp (stronger) | +2.21% (n=1676) | 11.7% | **KEEP** | — | `hyst_open_core_e5_x2__O_green__FR_ge1` |
| all nine morning cells known at 9:30 (A, B, C, G, J, K, L, M, O) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. plus green O. | next 2 | light+O ∧ AH≥1 | +4.61% (n=1373) | +2.52 pp (stronger) | +6.59% (n=987) | 25.2% | **KILL** | hold_t,lottery_day | `hyst_open_score_e5_x2__O_green__AH_ge1` |
| all nine morning cells known at 9:30 (A, B, C, G, J, K, L, M, O) add up to a strong green (+5 or more). The light stays on until that sum falls to +2. plus green O. | next 2 | light+O ∧ FR≥1 | +2.93% (n=2356) | +0.84 pp (stronger) | +3.76% (n=1814) | 22.2% | **KILL** | hold_t | `hyst_open_score_e5_x2__O_green__FR_ge1` |

### AH / FR alone vs light+O (already KEEP, not re-mined)

Leftover hold2 survivors. Incremental is versus the published light+O hold2 on the matching recipe, not a new mine.

| letter | what it is | leftover hold2 | vs five-cell+O hold2 | vs nine-cell+O hold2 | note |
|---|---|---|---|---|---|
| **AH** | count of recent same-day drops of 5% or more (open-knowable walk) | +2.22% (n=51855) | +0.33 pp | +0.22 pp | already KEEP as leftover; stack must still beat light+O |
| **FR** | recent volume over 1M and/or G ≥ 3 (open-knowable walk) | +0.94% (n=85081) | -0.95 pp | -1.05 pp | already KEEP as leftover; stack must still beat light+O |

### What this does not change

- Standing A–O research keeps stay the **three light + green O** recipes.
- Close-letter leftover KEEPs (T, BA, CZ, EH, IB and peers) are out of scope this beat.
- Finviz volume stays **BLOCKED**. AB / weather / book stay dead.
- Live `flatten_robust` is not imported or changed. No cards.
- Calendar half cut **2026-05-01**. Q3 cut **2026-07-01**. Futubull 0.15% long / 0.20% short.

Research only. One 2026 regime.

## Close-cluster harden (T / BA)

_Generated 2026-09-07 · live `flatten_robust` frozen. Yahoo/rows A–F seed only. Close entry only. No merge. Open recipes (light+O, AH/FR) untouched._

## Plain English

Leftover harden already kept two close letters: **T** (a green highlight, alias of EN) and **BA** (a 0/1 stress flag). This beat asks whether they still pay after a deeper walk-forward, after we drop the fattest day and the fattest five names, and whether buying only when **both** fire is better than either alone. Buy at that close; sell the next close (hold1) or the close two sessions later (hold2). Futubull fees. Beat buy-everyone by 20 bp. Q1 / Q2 / **Q3 (2026-07-01)**. Both SPY tapes.

**T alone is KILL. BA alone is KILL. T∧BA is KILL.** Hold1 and hold2 die the same way. The leftover KEEP was a May-cut illusion: Q1 loses money, June flips red, and **July is 66% of T's winning-month P&L** (BA 44%, stack 71%). Day-lottery is under the 25% bar (T 14.2%, same as leftover ~14%), and dropping the fattest day still leaves T holdout +3.70% (n=26560). The ghost is names, not days: five tickers are 59% of T P&L and 29% of BA (RCON / DFNS / FFAI and peers). After those five are dropped, T hold2 shrinks to +1.14% (n=26617).

The stack is stronger (+4.99 pp) than T and stronger (+7.01 pp) than BA — about 30% of T days are also BA — but it is an even fatter July / five-name ghost. Not a new close keep.

Dumps **3603**. Close-long everyone-else hold2 +0.83% (n=495012). Open recipes untouched. No card.

### Verdict table

| layer | hold | holdout | vs everyone | Q1 | Q2 | Q3 | day-lottery | drop-top-day holdout | top-5 names | verdict | why |
|---|---|---|---|---|---|---|---|---|---|---|---|
| column T is highlighted green (alias of EN) | next 1 | +2.99% (n=26909) | +2.52 pp | -0.52% (n=4779) | +0.08% (n=37690) | +5.50% (n=25215) | 12.7% | +2.83% (n=26697) | 62.1% | **KILL** | q1_sign,month_split,month_lottery,ticker_ghost |
| column T is highlighted green (alias of EN) | next 2 | +3.98% (n=26772) | +3.15 pp | -0.67% (n=4779) | +0.17% (n=37690) | +7.73% (n=24859) | 14.2% | +3.70% (n=26560) | 59.4% | **KILL** | q1_sign,month_split,month_lottery,ticker_ghost |
| the 0/1 stress flag BA equals 1 (HN/CP) | next 1 | +1.57% (n=76076) | +1.09 pp | -0.20% (n=29869) | +0.36% (n=87342) | +3.03% (n=71334) | 9.5% | +1.24% (n=75560) | 41.8% | **KILL** | q1_sign,month_split,month_lottery,ticker_ghost |
| the 0/1 stress flag BA equals 1 (HN/CP) | next 2 | +1.96% (n=75291) | +1.13 pp | -0.10% (n=29869) | +0.85% (n=87342) | +4.42% (n=69356) | 10.2% | +1.87% (n=74684) | 29.1% | **KILL** | q1_sign,month_split,month_lottery,ticker_ghost |
| T is green and BA equals 1 on the same close | next 1 | +9.47% (n=7876) | +8.99 pp | -0.48% (n=2349) | +0.43% (n=10374) | +16.57% (n=7639) | 16.2% | +9.59% (n=7785) | 66.2% | **KILL** | q1_sign,month_split,month_lottery,ticker_ghost |
| T is green and BA equals 1 on the same close | next 2 | +8.97% (n=7830) | +8.14 pp | -0.31% (n=2349) | +0.71% (n=10374) | +20.01% (n=7506) | 16.8% | +8.72% (n=7739) | 57.8% | **KILL** | q1_sign,month_split,month_lottery,ticker_ghost |

Code names (after the English): `fill_T_green`, `valclose_BA_eq1`, `close_T_green_and_BA_eq1`. Close entry only.

### T vs BA overlap (same close)

| hold | T days | BA days | both | T that are also BA | BA that are also T | T∧BA vs T | T∧BA vs BA |
|---|---:|---:|---:|---|---|---|---|
| next 1 | 67684 | 188545 | 20362 | 30.1% | 10.8% | +6.48 pp (stronger) | +7.90 pp (stronger) |
| next 2 | 67328 | 186567 | 20229 | 30.0% | 10.8% | +4.99 pp (stronger) | +7.01 pp (stronger) |

### Monthly walk-forward (discovery, hold2)

| layer | months | fattest month | share of winning-month P&L |
|---|---|---|---|
| T | 2026-03 -0.74% (n=2962), 2026-04 +0.90% (n=7665), 2026-05 +0.83% (n=6424), 2026-06 -0.83% (n=8478), 2026-07 +8.78% (n=7329), 2026-08 +2.90% (n=7225), 2026-09 +0.73% (n=472) | 2026-07 | 65.8% |
| BA | 2026-02 -0.86% (n=174), 2026-03 -0.14% (n=17703), 2026-04 +0.98% (n=13995), 2026-05 +2.09% (n=19007), 2026-06 -0.06% (n=19336), 2026-07 +4.91% (n=20798), 2026-08 +4.24% (n=17780), 2026-09 +0.85% (n=2482) | 2026-07 | 43.8% |
| T∧BA | 2026-03 -0.32% (n=1467), 2026-04 +2.10% (n=1690), 2026-05 +2.73% (n=1790), 2026-06 -0.48% (n=2928), 2026-07 +25.39% (n=2493), 2026-08 +9.01% (n=1847), 2026-09 +0.80% (n=184) | 2026-07 | 71.5% |

### What this does not change

- Standing A–O research keeps stay the **three light + green O** recipes. AH/FR open-stack is not reopened.
- No new A–JL surface. Close-cluster only: T, BA, T∧BA.
- Finviz volume stays **BLOCKED**. AB / weather / book stay dead.
- Live `flatten_robust` is not imported or changed. No cards.
- Calendar half cut **2026-05-01**. Q3 cut **2026-07-01**. Q1 cut **2026-04-01**. Futubull 0.15% long.

Research only. One 2026 regime.

## Close-cluster peers (CZ / EH / IB / HO / IL / GV)

_Generated 2026-09-07 · live `flatten_robust` frozen. Yahoo/rows A–F seed only. Close entry only. T/BA and open recipes untouched._

## Plain English

The leftover unique-36 shortboard still listed six close letters after T and BA: **CZ, EH, IB, HO, IL, GV**. Same deeper bar that killed T/BA: Q1 must not be red, five names must not own the P&L, July must not be the year, both SPY tapes, Futubull, hold1 and hold2, beat buy-everyone by 20 bp.

**CZ, EH, IB, HO, IL, and GV are all KILL** on hold1 and hold2. Same ghost as T/BA: leftover KEEP was a May-cut / Q3 print. EH and IB lose money in Q1. CZ / HO / IL / GV hold2 Q1 is flat to a few basis points of green, but five names still own 30–48% of P&L and the months flip (June red, July fat). Day-lottery stays under 25%. Not a new close keep.

Stacks were skipped — no two peers cleared Q1 and the name-ghost bar, so a pairwise book would only stack ghosts.

Singles: **KEEP 0** · **KILL 12** · **THIN 0**. Dumps **3603**. Close-long everyone-else hold2 +0.83% (n=495012). T/BA stay KILL. Open recipes untouched.

### Peers table

| letter | what it is | hold | holdout | vs everyone | Q1 | Q3 | July share | day-lottery | top-5 names | verdict | why |
|---|---|---|---|---|---|---|---|---|---|---|---|
| **CZ** | how many recent CP prints were negative is at least 2 | next 1 | +0.97% (n=132153) | +0.50 pp | -0.15% (n=29168) | +1.85% (n=125771) | 41% | 9.8% | 39% | **KILL** | q1_sign,month_split,month_lottery,ticker_ghost |
| **CZ** | how many recent CP prints were negative is at least 2 | next 2 | +1.51% (n=131044) | +0.68 pp | +0.01% (n=29168) | +2.91% (n=123062) | 38% | 8.7% | 30% | **KILL** | month_split,ticker_ghost |
| **EH** | any of FP–FU is negative (EH ≥ 1) | next 1 | +0.97% (n=125906) | +0.50 pp | -0.25% (n=33846) | +1.84% (n=119362) | 45% | 8.4% | 42% | **KILL** | q1_sign,month_split,month_lottery,ticker_ghost |
| **EH** | any of FP–FU is negative (EH ≥ 1) | next 2 | +1.34% (n=124724) | +0.51 pp | -0.13% (n=33846) | +2.74% (n=116468) | 42% | 9.4% | 28% | **KILL** | q1_sign,month_split,month_lottery,ticker_ghost |
| **IB** | the 0/1 count IB equals 1 (IB≥3 was already mined) | next 1 | +1.01% (n=32167) | +0.54 pp | -0.03% (n=9854) | +2.11% (n=33310) | 41% | 12.4% | 42% | **KILL** | q1_sign,month_split,month_lottery,ticker_ghost |
| **IB** | the 0/1 count IB equals 1 (IB≥3 was already mined) | next 2 | +1.48% (n=31778) | +0.64 pp | -0.08% (n=9854) | +3.17% (n=32349) | 37% | 8.6% | 44% | **KILL** | q1_sign,month_split,ticker_ghost |
| **HO** | a signed HN/GU cross equals 1 | next 1 | +3.18% (n=9584) | +2.70 pp | -0.41% (n=3603) | +7.38% (n=8352) | 62% | 22.8% | 58% | **KILL** | disc_t,hold_t,tape_split,q1_sign,month_split,month_lottery,ticker_ghost |
| **HO** | a signed HN/GU cross equals 1 | next 2 | +3.41% (n=9528) | +2.58 pp | +0.08% (n=3603) | +7.36% (n=8209) | 42% | 19.8% | 48% | **KILL** | month_split,month_lottery,ticker_ghost |
| **IL** | bins of same-day return H equal 1 | next 1 | +1.86% (n=26313) | +1.38 pp | +0.17% (n=12588) | +3.99% (n=22321) | 35% | 14.7% | 38% | **KILL** | month_split,month_lottery,ticker_ghost |
| **IL** | bins of same-day return H equal 1 | next 2 | +3.03% (n=26155) | +2.19 pp | +0.14% (n=12588) | +7.47% (n=21895) | 51% | 12.4% | 42% | **KILL** | month_split,month_lottery,ticker_ghost |
| **GV** | a 0/1 composite of deeper flags equals 1 | next 1 | +1.78% (n=49804) | +1.30 pp | -0.02% (n=17751) | +3.33% (n=46933) | 55% | 11.7% | 53% | **KILL** | q1_sign,month_split,month_lottery,ticker_ghost |
| **GV** | a 0/1 composite of deeper flags equals 1 | next 2 | +2.56% (n=49429) | +1.73 pp | +0.18% (n=17751) | +5.31% (n=46011) | 50% | 8.7% | 46% | **KILL** | month_split,month_lottery,ticker_ghost |

Code names (after the English): `valclose_CZ_ge2`, `valclose_EH_ge1`, `flag_IB_eq1`, `valclose_HO_eq1`, `valclose_IL_eq1`, `valclose_GV_eq1`.

### What this does not change

- T / BA / T∧BA stay **KILL** from the prior close-cluster beat.
- Standing A–O keeps stay the **three light + green O** recipes. AH/FR open-stack is not reopened.
- No new A–JL surface. Close peers only.
- Finviz volume stays **BLOCKED**. Live `flatten_robust` frozen. No cards.
- Q1 cut **2026-04-01**. Q3 cut **2026-07-01**. Futubull 0.15% long.

Research only. One 2026 regime.

## Next A–JL region (weekly + leftover lag)

_Generated 2026-09-07 · live `flatten_robust` frozen. Yahoo/rows A–F seed only. No merge. T/BA and light+O untouched._

## Plain English

The leftover four families already used every A–JL letter. This beat scores the **next region**: weekly spill AP–AU (close), and yesterday's leftover-open number plus this morning's green A (open). Same ship bar as light+O, plus the deeper Q1 / July-share / five-name bar that killed the close shortboard.

**Clean null.** KEEP 0 · KILL 230 · THIN 0. Weekly spill is sparse or a Q1 / July / name ghost. Leftover-open lags do not add a shippable open edge on top of A. Single-letter leftover space stays exhausted. Next region, if any, is same-day multi-letter counts (already ghosted on T/BA) or a new tape — not more A–JL threshold twins.

Dumps **3603**. Specs **165**. Close-long everyone-else hold2 +0.83% (n=495012). Standing five-cell light+O ± AH/FR untouched.

### Family scoreboard

| family | what it is | KEEP | KILL | THIN |
|---|---|---:|---:|---:|
| weekly | weekly spill AP–AU (close) | 0 | 74 | 0 |
| lag_open | yesterday leftover-open + today's A green | 0 | 156 | 0 |

### New-family table

| family | meaning | hold | clock | holdout | vs everyone | Q1 | Q3 | July | top-5 | verdict | why |
|---|---|---|---|---|---|---|---|---|---|---|---|
| lag_open | yesterday leftover-open number + today's green A | hold2 | open | +0.34% (n=14793) | -0.20 pp | +0.05% (n=6122) | +2.12% (n=5004) | 30% | 46% | **KILL** | disc_t,lottery_day,no_edge_vs_uncond,month_split,ticker_ghost |
| lag_open | yesterday leftover-open number + today's green A | hold2 | open | +0.34% (n=14793) | -0.20 pp | +0.05% (n=6122) | +2.12% (n=5004) | 30% | 46% | **KILL** | disc_t,lottery_day,no_edge_vs_uncond,month_split,ticker_ghost |
| lag_open | yesterday leftover-open number + today's green A | hold2 | open | +0.34% (n=14793) | -0.20 pp | +0.05% (n=6122) | +2.12% (n=5004) | 30% | 46% | **KILL** | disc_t,lottery_day,no_edge_vs_uncond,month_split,ticker_ghost |
| lag_open | yesterday leftover-open number + today's green A | hold2 | open | +2.59% (n=161) | +2.04 pp | +3.02% (n=364) | — | 0% | 16% | **KILL** | thin_disc,date_bar,lottery_day,tape_thin,spy_thin,q3_missing |
| lag_open | yesterday leftover-open number + today's green A | hold2 | open | +2.44% (n=234) | +1.90 pp | +3.06% (n=548) | — | 0% | 14% | **KILL** | date_bar,lottery_day,tape_thin,spy_thin,q3_missing |
| lag_open | yesterday leftover-open number + today's green A | hold2 | open | +2.44% (n=234) | +1.90 pp | +3.06% (n=548) | — | 0% | 14% | **KILL** | date_bar,lottery_day,tape_thin,spy_thin,q3_missing |
| weekly | weekly spill cell fires (close) | hold2 | close | +1.53% (n=104250) | +0.70 pp | -0.34% (n=104737) | +2.65% (n=138383) | 46% | 39% | **KILL** | tape_split,q3_split,q1_sign,month_lottery,month_split,ticker_ghost |
| weekly | weekly spill cell fires (close) | hold2 | close | +1.53% (n=104458) | +0.69 pp | -0.33% (n=104578) | +2.63% (n=138761) | 46% | 39% | **KILL** | tape_split,q3_split,q1_sign,month_lottery,month_split,ticker_ghost |
| weekly | weekly spill cell fires (close) | hold2 | close | +1.48% (n=107146) | +0.65 pp | -0.36% (n=110530) | +2.65% (n=138383) | 46% | 39% | **KILL** | tape_split,q3_split,q1_sign,month_lottery,month_split,ticker_ghost |
| weekly | weekly spill cell fires (close) | hold2 | close | +1.48% (n=107395) | +0.65 pp | -0.36% (n=110530) | +2.63% (n=138761) | 46% | 39% | **KILL** | tape_split,q3_split,q1_sign,month_lottery,month_split,ticker_ghost |
| weekly | weekly spill cell fires (close) | hold2 | close | +3.01% (n=36464) | +2.18 pp | — | +2.40% (n=90083) | 37% | 38% | **KILL** | tape_thin,q3_split,q1_thin,month_split,ticker_ghost |
| weekly | weekly spill cell fires (close) | hold2 | close | +1.32% (n=95578) | +0.49 pp | -0.36% (n=110530) | +2.80% (n=109064) | 48% | 44% | **KILL** | tape_split,q3_split,q1_sign,month_lottery,month_split,ticker_ghost |
| weekly | weekly spill cell fires (close) | hold2 | close | +1.32% (n=95578) | +0.49 pp | -0.36% (n=110530) | +2.80% (n=109064) | 48% | 44% | **KILL** | tape_split,q3_split,q1_sign,month_lottery,month_split,ticker_ghost |
| weekly | weekly spill cell fires (close) | hold2 | close | +1.31% (n=79829) | +0.48 pp | -0.30% (n=95902) | +2.59% (n=88338) | 41% | 38% | **KILL** | tape_split,q3_split,no_edge_vs_uncond,q1_sign,month_lottery,month_split,ticker_ghost |
| weekly | weekly spill cell fires (close) | hold2 | close | +2.05% (n=26135) | +1.21 pp | — | +1.80% (n=64629) | 37% | 51% | **KILL** | tape_thin,q1_thin,ticker_ghost |
| weekly | weekly spill cell fires (close) | hold2 | close | +1.01% (n=77464) | +0.18 pp | -0.31% (n=96071) | +2.90% (n=81343) | 49% | 45% | **KILL** | tape_split,q3_split,no_edge_vs_uncond,q1_sign,month_lottery,month_split,ticker_ghost |
| weekly | weekly spill cell fires (close) | hold2 | close | +1.29% (n=81200) | +0.46 pp | -0.36% (n=110530) | +3.36% (n=74193) | 49% | 51% | **KILL** | tape_split,q3_split,q1_sign,month_lottery,month_split,ticker_ghost |
| weekly | weekly spill cell fires (close) | hold2 | close | +2.63% (n=19683) | +1.80 pp | — | +2.16% (n=49148) | 30% | 59% | **KILL** | tape_thin,q3_split,q1_thin,month_split,ticker_ghost |
| weekly | weekly spill cell fires (close) | hold2 | close | +2.63% (n=19683) | +1.80 pp | — | +2.16% (n=49148) | 30% | 59% | **KILL** | tape_thin,q3_split,q1_thin,month_split,ticker_ghost |
| weekly | weekly spill cell fires (close) | hold2 | close | +2.33% (n=25407) | +1.50 pp | — | +2.13% (n=62562) | 36% | 39% | **KILL** | tape_thin,q1_thin,ticker_ghost |
| weekly | weekly spill cell fires (close) | hold2 | close | +2.56% (n=113) | — | — | +0.20% (n=338) | 100% | 400% | **KILL** | thin_disc,disc_t,disc_sign,lottery_day,tape_thin,spy_regime,q1_thin,month_lottery,ticker_ghost |
| weekly | weekly spill cell fires (close) | hold2 | close | +2.56% (n=113) | — | — | +0.20% (n=338) | 100% | 400% | **KILL** | thin_disc,disc_t,disc_sign,lottery_day,tape_thin,spy_regime,q1_thin,month_lottery,ticker_ghost |
| weekly | weekly spill cell fires (close) | hold2 | close | +3.61% (n=22956) | +2.78 pp | -0.31% (n=132) | +2.28% (n=57040) | 40% | 56% | **KILL** | tape_split,q3_split,q1_sign,month_lottery,month_split,ticker_ghost |
| weekly | weekly spill cell fires (close) | hold2 | close | +0.95% (n=76412) | +0.12 pp | -0.33% (n=105732) | +2.33% (n=68533) | 27% | 54% | **KILL** | tape_split,q3_split,no_edge_vs_uncond,q1_sign,month_split,ticker_ghost |

Code names (after the English): `lag_IZ_ge1_Agreen`, `lag_IZ_gt0_Agreen`, `lag_IZ_eq1_Agreen`, `lag_GD_eq1_Agreen`, `lag_GD_ge1_Agreen`, `lag_GD_gt0_Agreen`, `weekly_AR_ge1`, `weekly_AQ_ge1` ….

### What this does not change

- Standing open keeps stay **five-cell light + green O**, with or without AH/FR from the open-stack beat.
- T / BA / CZ / EH / IB / HO / IL / GV stay **KILL**. Not reopened.
- Leftover four families are not re-swept.
- Finviz BLOCKED. Live `flatten_robust` frozen. No cards.
- Q1 cut **2026-04-01**. Q3 cut **2026-07-01**. Futubull 0.15%/0.20%.

Research only. One 2026 regime.

## Same-day multi-letter counts (A–JL)

_Generated 2026-09-07 · live `flatten_robust` frozen. Yahoo/rows A–F seed only. No merge. T/BA, weekly/lag, and light+O untouched._

## Plain English

The leftover four families, weekly spill, and leftover-open+A lags already used every single-letter and lag kind. This beat scores the last unused kind: **same-day multi-letter counts and AND joins** — how many leftover letters fire together today. Same ship bar as light+O, plus the deeper Q1 / July / five-name bar that killed T/BA.

**A–JL surface exhausted.** KEEP 0 · KILL 32 · THIN 0. Counts of leftover-close KEEPs, leftover fill greens, leftover-open numbers, and named AND joins all die under Q1 / July / five-name prove — the same ghost as T/BA. No remaining A–JL threshold-family kind is unmined on this tape. Next work, if any, is a new tape or an external join, not more sheet-letter twins.

Dumps **3603**. Specs **16**. Close-long everyone-else hold2 +0.83% (n=495012). Standing five-cell light+O ± AH/FR untouched.

### Family scoreboard

| family | what it is | KEEP | KILL | THIN |
|---|---|---:|---:|---:|
| count_fresh | leftover-close KEEP count, T/BA/peers out | 0 | 6 | 0 |
| count_keepclose | leftover-close KEEP count, T/BA out | 0 | 6 | 0 |
| count_fill | leftover fills green the same close | 0 | 6 | 0 |
| count_open | leftover-open numbers ≥1 this morning (AH/FR out) | 0 | 6 | 0 |
| and_join | two leftover letters both fire (not T/BA) | 0 | 8 | 0 |

### Multi-letter table

| family | meaning | hold | clock | holdout | vs everyone | Q1 | Q3 | July | top-5 | verdict | why |
|---|---|---|---|---|---|---|---|---|---|---|---|
| count_keepclose | at least k leftover-close KEEP letters fire (T/BA out) | hold2 | close | +1.18% (n=162547) | +0.35 pp | -0.22% (n=32219) | +2.19% (n=161058) | 36% | 31% | **KILL** | no_edge_vs_uncond,q1_sign,month_split,ticker_ghost |
| count_keepclose | at least k leftover-close KEEP letters fire (T/BA out) | hold2 | close | +1.12% (n=170940) | +0.29 pp | -0.23% (n=42790) | +2.18% (n=161603) | 36% | 31% | **KILL** | no_edge_vs_uncond,q1_sign,month_split,ticker_ghost |
| count_keepclose | at least k leftover-close KEEP letters fire (T/BA out) | hold2 | close | +1.06% (n=178351) | +0.23 pp | -0.30% (n=56019) | +2.18% (n=161606) | 36% | 31% | **KILL** | no_edge_vs_uncond,q1_sign,month_split,ticker_ghost |
| count_fresh | at least k leftover-close KEEP letters fire (T/BA/peers out) | hold2 | close | +1.32% (n=143780) | +0.49 pp | -0.30% (n=17604) | +2.25% (n=156780) | 37% | 32% | **KILL** | q1_sign,month_split,ticker_ghost |
| count_fresh | at least k leftover-close KEEP letters fire (T/BA/peers out) | hold2 | close | +1.14% (n=166017) | +0.31 pp | -0.38% (n=36836) | +2.18% (n=161477) | 36% | 32% | **KILL** | no_edge_vs_uncond,q1_sign,month_split,ticker_ghost |
| count_fresh | at least k leftover-close KEEP letters fire (T/BA/peers out) | hold2 | close | +1.20% (n=157075) | +0.37 pp | -0.43% (n=26088) | +2.19% (n=160818) | 37% | 32% | **KILL** | q1_sign,month_split,ticker_ghost |
| and_join | two leftover letters both fire the same close (not T/BA) | hold2 | close | +2.69% (n=43577) | +1.86 pp | +0.20% (n=3153) | +4.35% (n=48726) | 34% | 30% | **KILL** | month_split,ticker_ghost |
| and_join | two leftover letters both fire the same close (not T/BA) | hold2 | close | +1.73% (n=99882) | +0.89 pp | -0.60% (n=16894) | +2.76% (n=112181) | 34% | 36% | **KILL** | q1_sign,month_split,ticker_ghost |
| count_fill | at least k leftover fills are green the same close | hold2 | close | +0.89% (n=184413) | +0.05 pp | -0.48% (n=90064) | +2.04% (n=151400) | 31% | 35% | **KILL** | tape_split,no_edge_vs_uncond,q1_sign,month_split,ticker_ghost |
| count_fill | at least k leftover fills are green the same close | hold2 | close | +1.00% (n=159098) | +0.16 pp | -0.56% (n=67671) | +2.00% (n=132883) | 29% | 41% | **KILL** | no_edge_vs_uncond,q1_sign,month_split,ticker_ghost |
| count_fill | at least k leftover fills are green the same close | hold2 | close | +0.97% (n=134175) | +0.14 pp | -0.58% (n=48642) | +1.73% (n=114542) | 26% | 45% | **KILL** | no_edge_vs_uncond,q1_sign,month_split,ticker_ghost |
| count_open | at least k leftover-open numbers are ≥1 this morning (AH/FR out) | hold2 | open | +0.82% (n=188090) | +0.28 pp | +0.13% (n=75556) | +1.19% (n=165203) | 28% | 47% | **KILL** | no_edge_vs_uncond,month_split,ticker_ghost |
| count_open | at least k leftover-open numbers are ≥1 this morning (AH/FR out) | hold2 | open | +0.76% (n=178442) | +0.21 pp | -0.12% (n=62873) | +1.24% (n=160156) | 31% | 41% | **KILL** | no_edge_vs_uncond,q1_sign,month_split,ticker_ghost |
| count_open | at least k leftover-open numbers are ≥1 this morning (AH/FR out) | hold2 | open | +0.86% (n=155112) | +0.32 pp | -0.08% (n=47570) | +1.40% (n=140053) | 32% | 42% | **KILL** | no_edge_vs_uncond,q1_sign,month_split,ticker_ghost |
| and_join | two leftover letters both fire the same close (not T/BA) | hold2 | close | +7.05% (n=6876) | +6.22 pp | — | +9.65% (n=6587) | 20% | 76% | **KILL** | disc_t,q1_thin,month_split,ticker_ghost |
| and_join | two leftover letters both fire the same close (not T/BA) | hold2 | close | +2.74% (n=21132) | +1.90 pp | +0.42% (n=5140) | +6.18% (n=19434) | 47% | 68% | **KILL** | hold_t,month_lottery,month_split,ticker_ghost |

Code names (after the English): `close_count_keepclose_ge4`, `close_count_keepclose_ge3`, `close_count_keepclose_ge2`, `close_count_fresh_ge4`, `close_count_fresh_ge2`, `close_count_fresh_ge3`, `close_and_IK_FC`, `close_and_FK_FL`, `close_count_fillnew_ge3`, `close_count_fillnew_ge5` ….

### What this does not change

- Standing open keeps stay **five-cell light + green O**, with or without AH/FR from the open-stack beat.
- T / BA / CZ / EH / IB / HO / IL / GV stay **KILL**. Not reopened.
- Leftover four families, weekly AP–AU, and leftover-open+A lags are not re-swept.
- Finviz BLOCKED. Live `flatten_robust` frozen. No cards.
- Q1 cut **2026-04-01**. Q3 cut **2026-07-01**. Futubull 0.15%/0.20%.

Research only. One 2026 regime.

## Pair+lag mine (open-entry pilot)

_Generated 2026-09-07 · live `flatten_robust` frozen. Yahoo/rows A–F seed only. No merge. Excel-locked 44. T/BA highlight ghosts not reopened._

## Plain English

Open-entry **pair + lag** on Excel's locked same-row gate: the 44 value-open letters (numbers/text) and the timing-tested open fills. Yesterday-and-older of any letter is fair. O green is open; O as a number is close. AA today is close — O two days ago under 1 **and** AA today equals 1 waits for close-entry. Open siblings (O two days ago under 1 and today's ES equals 1; or yesterday's AA equals 1) are in this pilot.

**Clean null.** KEEP 0 · KILL 4266 · THIN 0. The locked 44 plus open-fill legs and lags do not clear ship + Q1 / July / five-name on this tape. Trees skipped. Next: close-entry (AA-today pairs) — not a remine of T/BA.

Dumps **3603**. Atoms **838**. Alive **64**. Pairs **1521**. Open-long everyone-else hold2 +0.54% (n=498610). Both SPY tapes required. Light+O ± AH/FR baseline.

### Family scoreboard

| family | what it is | KEEP | KILL | THIN |
|---|---|---:|---:|---:|
| single | locked-44 number/text, open fill, or a lag | 0 | 1262 | 0 |
| pair | two open-knowable legs (lag any col ∨ same-row 44/fill) | 0 | 3004 | 0 |

### Pilot table (hold2, English first)

| meaning | clock | holdout | vs everyone | Q1 | spy↑ | spy↓ | July | top-5 | verdict | why |
|---|---|---|---|---|---|---|---|---|---|---|
| GD yesterday is under 1 AND JD today is under 1 | open | +1.63% (n=1092) | +1.09 pp | +1.55% (n=2681) | +0.94% (n=24) | +1.55% (n=2672) | 0% | 5% | **KILL** | lottery_day,tape_thin,spy_thin,q3_missing |
| Q 3 days ago is at least 1 AND FS today is under 1 | open | +1.66% (n=1584) | +1.12 pp | +1.72% (n=3847) | -0.06% (n=325) | +1.84% (n=3557) | 0% | 7% | **KILL** | lottery_day,tape_thin,spy_regime,q3_missing,month_split |
| Q 3 days ago is at least 1 AND FU today is under 1 | open | +1.66% (n=1584) | +1.12 pp | +1.72% (n=3847) | -0.06% (n=325) | +1.84% (n=3557) | 0% | 7% | **KILL** | lottery_day,tape_thin,spy_regime,q3_missing,month_split |
| GD yesterday is under 1 AND ER today is under 1 | open | +1.56% (n=1049) | +1.02 pp | +1.52% (n=2582) | +0.92% (n=29) | +1.51% (n=2571) | 0% | 5% | **KILL** | lottery_day,tape_thin,spy_thin,q3_missing |
| Q 3 days ago is at least 1 AND JB today is under 1 | open | +1.62% (n=1613) | +1.08 pp | +1.62% (n=3928) | -0.18% (n=309) | +1.74% (n=3648) | 0% | 8% | **KILL** | lottery_day,tape_thin,spy_regime,q3_missing |
| GD yesterday is under 1 AND J today is under 1 | open | +1.67% (n=1204) | +1.13 pp | +1.63% (n=2973) | +0.49% (n=31) | +1.63% (n=2961) | 0% | 6% | **KILL** | lottery_day,tape_thin,spy_thin,q3_missing |
| GD yesterday is under 1 AND EP today is under 1 | open | +1.67% (n=1204) | +1.13 pp | +1.63% (n=2973) | +0.49% (n=31) | +1.63% (n=2961) | 0% | 6% | **KILL** | lottery_day,tape_thin,spy_thin,q3_missing |
| GD yesterday is under 1 AND JL today is under 1 | open | +1.67% (n=1204) | +1.13 pp | +1.63% (n=2973) | +0.49% (n=31) | +1.63% (n=2961) | 0% | 6% | **KILL** | lottery_day,tape_thin,spy_thin,q3_missing |
| Q 3 days ago is at least 1 AND JL today is under 1 | open | +1.62% (n=1664) | +1.07 pp | +1.63% (n=4049) | -0.02% (n=327) | +1.75% (n=3757) | 0% | 8% | **KILL** | lottery_day,tape_thin,spy_regime,q3_missing,month_split |
| GD yesterday is under 1 AND JB today is under 1 | open | +1.64% (n=1181) | +1.09 pp | +1.60% (n=2915) | +0.27% (n=28) | +1.60% (n=2904) | 0% | 6% | **KILL** | lottery_day,tape_thin,spy_thin,q3_missing |
| Q 3 days ago is at least 1 AND HF today is above 0 | open | +1.60% (n=1650) | +1.06 pp | +1.63% (n=4020) | -0.02% (n=317) | +1.74% (n=3738) | 0% | 8% | **KILL** | lottery_day,tape_thin,spy_regime,q3_missing,month_split |
| Q 3 days ago is at least 1 AND JE today is under 1 | open | +1.59% (n=1665) | +1.04 pp | +1.62% (n=4050) | -0.02% (n=327) | +1.73% (n=3758) | 0% | 8% | **KILL** | lottery_day,tape_thin,spy_regime,q3_missing,month_split |
| Q 3 days ago is at least 1 AND JF today is under 1 | open | +1.59% (n=1665) | +1.04 pp | +1.62% (n=4050) | -0.02% (n=327) | +1.73% (n=3758) | 0% | 8% | **KILL** | lottery_day,tape_thin,spy_regime,q3_missing,month_split |
| Q 3 days ago is at least 1 AND HW today is under 1 | open | +1.59% (n=1665) | +1.04 pp | +1.62% (n=4050) | -0.02% (n=327) | +1.73% (n=3758) | 0% | 8% | **KILL** | lottery_day,tape_thin,spy_regime,q3_missing,month_split |
| Q 3 days ago is at least 1 AND JD today is under 1 | open | +1.52% (n=1503) | +0.97 pp | +1.52% (n=3650) | -0.06% (n=316) | +1.65% (n=3363) | 0% | 9% | **KILL** | lottery_day,tape_thin,spy_regime,q3_missing |
| GD yesterday is under 1 AND HF today is above 0 | open | +1.64% (n=1200) | +1.10 pp | +1.62% (n=2964) | +0.51% (n=30) | +1.62% (n=2953) | 0% | 6% | **KILL** | lottery_day,tape_thin,spy_thin,q3_missing |
| GD yesterday is under 1 AND IZ today is under 1 | open | +1.63% (n=1205) | +1.09 pp | +1.61% (n=2974) | +0.49% (n=31) | +1.61% (n=2962) | 0% | 6% | **KILL** | lottery_day,tape_thin,spy_thin,q3_missing |
| GD yesterday is under 1 AND EQ today is S | open | +1.63% (n=1205) | +1.09 pp | +1.61% (n=2974) | +0.49% (n=31) | +1.61% (n=2962) | 0% | 6% | **KILL** | lottery_day,tape_thin,spy_thin,q3_missing |
| GD yesterday is under 1 AND FS today is under 1 | open | +1.63% (n=1205) | +1.09 pp | +1.61% (n=2974) | +0.49% (n=31) | +1.61% (n=2962) | 0% | 6% | **KILL** | lottery_day,tape_thin,spy_thin,q3_missing |
| GD yesterday is under 1 AND JE today is under 1 | open | +1.63% (n=1205) | +1.09 pp | +1.61% (n=2974) | +0.49% (n=31) | +1.61% (n=2962) | 0% | 6% | **KILL** | lottery_day,tape_thin,spy_thin,q3_missing |
| GD yesterday is under 1 AND JF today is under 1 | open | +1.63% (n=1205) | +1.09 pp | +1.61% (n=2974) | +0.49% (n=31) | +1.61% (n=2962) | 0% | 6% | **KILL** | lottery_day,tape_thin,spy_thin,q3_missing |
| GD yesterday is under 1 AND FU today is under 1 | open | +1.63% (n=1205) | +1.09 pp | +1.61% (n=2974) | +0.49% (n=31) | +1.61% (n=2962) | 0% | 6% | **KILL** | lottery_day,tape_thin,spy_thin,q3_missing |
| GD yesterday is under 1 AND HW today is under 1 | open | +1.63% (n=1205) | +1.09 pp | +1.61% (n=2974) | +0.49% (n=31) | +1.61% (n=2962) | 0% | 6% | **KILL** | lottery_day,tape_thin,spy_thin,q3_missing |
| GD yesterday is under 1 AND IY today is under 1 | open | +1.63% (n=1205) | +1.09 pp | +1.61% (n=2974) | +0.49% (n=31) | +1.61% (n=2962) | 0% | 6% | **KILL** | lottery_day,tape_thin,spy_thin,q3_missing |
| GD yesterday is under 1 AND Q today is above 0 | open | +1.64% (n=1189) | +1.10 pp | +1.64% (n=2938) | +0.51% (n=30) | +1.64% (n=2926) | 0% | 6% | **KILL** | lottery_day,tape_thin,spy_thin,q3_missing |
| Q 3 days ago is at least 1 AND HG today is under 1 | open | +1.61% (n=1621) | +1.06 pp | +1.63% (n=3951) | -0.04% (n=306) | +1.74% (n=3678) | 0% | 8% | **KILL** | lottery_day,tape_thin,spy_regime,q3_missing |
| GD yesterday is under 1 AND HG today is under 1 | open | +1.64% (n=1181) | +1.09 pp | +1.61% (n=2918) | +0.55% (n=29) | +1.61% (n=2907) | 0% | 6% | **KILL** | lottery_day,tape_thin,spy_thin,q3_missing |
| GD yesterday is under 1 AND ET today is under 1 | open | +1.63% (n=1169) | +1.08 pp | +1.60% (n=2881) | +0.49% (n=31) | +1.60% (n=2869) | 0% | 7% | **KILL** | lottery_day,tape_thin,spy_thin,q3_missing |

Code names (after the English): `GD_l1_lt1__and__JD_l0_lt1`, `Q_l3_ge1__and__FS_l0_lt1`, `Q_l3_ge1__and__FU_l0_lt1`, `GD_l1_lt1__and__ER_l0_lt1`, `Q_l3_ge1__and__JB_l0_lt1`, `GD_l1_lt1__and__J_l0_lt1`, `GD_l1_lt1__and__EP_l0_lt1`, `GD_l1_lt1__and__JL_l0_lt1` ….

### What this does not change

- Standing open keeps stay **five-cell light + green O**, with or without AH/FR. Baseline, not a card.
- T / BA / CZ / EH / IB / HO / IL / GV stay **KILL** as highlight / leftover-close ghosts.
- Weekly+lag and same-day fill counts stay exhausted.
- Close-entry pair+lag (AA today) is **next**.
- Finviz BLOCKED. Live `flatten_robust` frozen. No cards.
- Q1 cut **2026-04-01**. Q3 cut **2026-07-01**. Futubull 0.15%/0.20%.

Research only. One 2026 regime.
