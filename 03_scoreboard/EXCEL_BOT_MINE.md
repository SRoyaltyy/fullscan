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

