# Excel A–O first mine (inventory plan)

_Generated 2026-09-07 · live `flatten_robust` is not changed. No merge without Cyrus._

## Inventory (folded)

- **VISIBLE_COLS A..O (15)** = what daily/backtest grids store (~139 days × OHLCV + 15 fills). Rebuilt this cycle: **3603** from excel-state `state/rows.tar.gz`. Daily `done_grids` is ~3445; excel-state itself has **no** grid JSON.
- **ALL_COLS A..JL (275)** via `run.py --all-cols` exists (#139) but is **not** used by the daily bot. `signal_colors` in suggestions = A→O strip only.
- PIT: SOD/open A,B,C,G,J,K,L,M,O · close D,E,F,H,I,N. `core_score` needs **CLOSE** entry. G/K/M *values* are close-knowable even when the fill clock is open.
- Cards L1–L5 / S1–S2 holdout PASS historically. Live 2026-09-05: L1/L2 −0.55%, L3 +0.35%, L5 −4%. **L4/L5 deferred.**
- Phase-2 A–JL: 35-ticker lean pilot **0 PASS / 0 FAIL / 90 THIN** (`research/ALL_COLS_MINE.md`). Full 3603 rebuild stays scheduled after this A–O surface is exhausted.

## This cycle (recommended order)

1. Open-knowable **a_score / open_score / open_core / combos / lags / gap-J** (never `core_score` at open).
2. Close-entry **L3-like** + non-TP low-vol holds; then **S1/S2**.
3. Beat unconditional same-clock/hold baseline by **≥20 bps** or FAIL `no_edge_vs_uncond`.
4. hold3/5/8 also need hold2 short-horizon edge or FAIL `long_hold_without_hold2`.

Grids **3603**. Patterns **67**. Cells **2283**. **PASS 240** · **FAIL 2043** · **THIN 0**. Cost = futubull 0.15%/0.20%.

### Unconditional baseline (futubull)

| clock | side | hold | n | avg net | t | win |
|---|---|---:|---:|---:|---:|---:|
| open | long | 1 | 602650 | -0.07% | -1.7 | 45% |
| open | long | 2 | 599052 | +0.41% | 4.6 | 46% |
| open | long | 3 | 595454 | +0.69% | 7.1 | 47% |
| open | long | 5 | 588258 | +1.12% | 10.2 | 47% |
| open | long | 8 | 577464 | +1.84% | 13.0 | 48% |
| open | short | 1 | 602650 | -0.28% | -6.5 | 46% |
| open | short | 2 | 599052 | -0.76% | -8.5 | 47% |
| open | short | 3 | 595454 | -1.04% | -10.7 | 48% |
| open | short | 5 | 588258 | -1.47% | -13.3 | 49% |
| open | short | 8 | 577464 | -2.19% | -15.5 | 49% |
| close | long | 1 | 599052 | +0.37% | 4.9 | 45% |
| close | long | 2 | 595454 | +0.66% | 7.2 | 46% |
| close | long | 3 | 591856 | +0.88% | 9.3 | 47% |
| close | long | 5 | 584660 | +1.33% | 12.7 | 47% |
| close | long | 8 | 573866 | +2.18% | 8.6 | 48% |
| close | short | 1 | 599052 | -0.72% | -9.5 | 46% |
| close | short | 2 | 595454 | -1.01% | -11.1 | 47% |
| close | short | 3 | 591856 | -1.23% | -12.9 | 48% |
| close | short | 5 | 584660 | -1.68% | -16.1 | 49% |
| close | short | 8 | 573866 | -2.53% | -10.0 | 49% |

### Focus refresh (L3 / low-vol holds / S1–S2)

| verdict | def | clock | side | exit | cohort | disc | hold | base | tickers | why |
|---|---|---|---|---|---|---|---|---|---:|---|
| FAIL | `tol2_core_score_ml3` | close | long | hold8 | volM:low(<3%) | 2107/+2.30%/t=23.4 | 1443/+2.21%/t=21.1 | 573866/+2.18%/t=8.6 | 957 | no_edge_vs_uncond |
| FAIL | `tol2_core_score_ml3` | close | long | hold5 | volM:low(<3%) | 2900/+1.25%/t=17.1 | 1951/+1.35%/t=18.0 | 584660/+1.33%/t=12.7 | 960 | no_edge_vs_uncond |
| FAIL | `tol2_core_score_ml3` | close | long | hold3 | volM:low(<3%) | 3651/+0.61%/t=11.7 | 2506/+0.48%/t=8.4 | 591856/+0.88%/t=9.3 | 960 | no_edge_vs_uncond |
| FAIL | `tol2_core_score_ml3` | close | long | hold2 | volM:low(<3%) | 4129/+0.32%/t=7.6 | 2846/+0.22%/t=4.5 | 595454/+0.66%/t=7.2 | 960 | no_edge_vs_uncond |
| FAIL | `tol2_core_score_ml3` | close | long | hold2 | mid(1-10B):all | 4718/+0.45%/t=5.9 | 3193/+0.37%/t=4.1 | 595454/+0.66%/t=7.2 | 1085 | no_edge_vs_uncond |
| FAIL | `tol2_core_score_ml3_sr` | close | short | hold1 | volM:high(>8%) | 4022/-3.44%/t=-1.7 | 2729/-2.02%/t=-1.1 | 599052/-0.72%/t=-9.5 | 863 | disc_t,hold_t,disc_sign,hold_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `tol2_core_score_ml3_sr` | close | short | hold1 | opt:Yes | 9454/-0.11%/t=-2.6 | 6478/-0.89%/t=-1.2 | 599052/-0.72%/t=-9.5 | 2388 | disc_t,hold_t,disc_sign,hold_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `strict_A_ml1_sr` | close | short | hold1 | volM:high(>8%) | 11525/-0.43%/t=-1.8 | 7646/-4.25%/t=-1.3 | 599052/-0.72%/t=-9.5 | 863 | disc_t,hold_t,disc_sign,hold_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `strict_A_ml1_sr` | close | short | hold1 | opt:Yes | 20990/-0.32%/t=-4.3 | 14244/-2.31%/t=-1.3 | 599052/-0.72%/t=-9.5 | 2388 | disc_t,hold_t,disc_sign,hold_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `tol2_core_score_ml2` | close | long | hold2 | volM:low(<3%) | 4902/+0.07%/t=2.2 | 3396/-0.08%/t=-1.7 | 595454/+0.66%/t=7.2 | 960 | disc_t,hold_t,hold_sign,tape_split,no_edge_vs_uncond |

### Primary (ALL × hold1/2 only — not cohort/hold8 slices)

**240** PASS cells include overlapping cohort × hold slices of the same defs. The honest sleeve-shaped set is **6** ALL-cohort hold1/2 rows. hold8 / opt / hi-vol slices are not independent edges. Research candidates only — one Jan–Sep 2026 regime, overlapping cluster days, no cards.

| def | clock | side | exit | hold | base | tickers |
|---|---|---|---|---|---|---:|
| `hyst_open_core_e5_x2` | open | long | hold1 | 10931/+1.74%/t=34.4 | 602650/-0.07%/t=-1.7 | 3577 |
| `hyst_open_score_e5_x2` | open | long | hold1 | 10862/+1.33%/t=27.6 | 602650/-0.07%/t=-1.7 | 3552 |
| `hyst_open_core_e5_x0` | open | long | hold1 | 11120/+1.28%/t=26.4 | 602650/-0.07%/t=-1.7 | 3585 |
| `hyst_open_core_e5_x2` | open | long | hold2 | 10931/+1.62%/t=20.1 | 599052/+0.41%/t=4.6 | 3577 |
| `hyst_open_core_e5_x0` | open | long | hold2 | 11120/+1.21%/t=14.6 | 599052/+0.41%/t=4.6 | 3585 |
| `hyst_open_score_e5_x2` | open | long | hold2 | 10862/+1.54%/t=4.6 | 599052/+0.41%/t=4.6 | 3552 |

Same-day open→close (hold1) of hysteresis `open_core`/`open_score` beat uncond (−0.07%) by ~130–180 bp on ~11k holdout trades / ~3500 tickers. Not a card. Needs another regime before anyone talks live.

### Keepers (ship bar + hold2 sibling + beat baseline)

| def | clock | side | exit | cohort | disc | hold | base | tickers |
|---|---|---|---|---|---|---|---|---:|
| `hyst_open_core_e3_x0` | open | long | hold8 | opt:Yes | 3931/+7.44%/t=40.1 | 2684/+7.20%/t=34.5 | 577464/+1.84%/t=13.0 | 2268 |
| `hyst_open_core_e5_x2` | open | long | hold1 | ALL | 15881/+1.84%/t=30.1 | 10931/+1.74%/t=34.4 | 602650/-0.07%/t=-1.7 | 3577 |
| `hyst_open_score_e3_x0` | open | long | hold8 | opt:Yes | 4377/+7.44%/t=40.3 | 2998/+7.25%/t=33.3 | 577464/+1.84%/t=13.0 | 2316 |
| `hyst_open_core_e5_x2` | open | long | hold1 | opt:Yes | 11620/+1.26%/t=41.2 | 7990/+1.23%/t=33.2 | 602650/-0.07%/t=-1.7 | 2387 |
| `hyst_open_core_e5_x2` | open | long | hold8 | opt:Yes | 2578/+8.35%/t=18.1 | 1743/+7.74%/t=32.2 | 577464/+1.84%/t=13.0 | 1971 |
| `hyst_open_score_e5_x2` | open | long | hold8 | opt:Yes | 3372/+8.42%/t=38.5 | 2302/+8.35%/t=31.7 | 577464/+1.84%/t=13.0 | 2187 |
| `hyst_open_core_e3_x0` | open | long | hold5 | opt:Yes | 7854/+3.76%/t=37.0 | 5377/+3.81%/t=31.1 | 588258/+1.12%/t=10.2 | 2382 |
| `hyst_open_core_e5_x2` | open | long | hold5 | opt:Yes | 5952/+4.23%/t=34.4 | 4026/+4.34%/t=30.7 | 588258/+1.12%/t=10.2 | 2340 |
| `hyst_open_score_e5_x0` | open | long | hold8 | opt:Yes | 4104/+7.15%/t=36.6 | 2773/+7.00%/t=30.4 | 577464/+1.84%/t=13.0 | 2300 |
| `hyst_open_core_e5_x0` | open | long | hold8 | opt:Yes | 3556/+7.23%/t=36.0 | 2459/+7.08%/t=30.1 | 577464/+1.84%/t=13.0 | 2245 |
| `hyst_open_core_e3_x0` | open | long | hold1 | opt:Yes | 14589/+0.93%/t=35.7 | 9977/+0.91%/t=29.6 | 602650/-0.07%/t=-1.7 | 2388 |
| `hyst_open_score_e5_x2` | open | long | hold5 | opt:Yes | 6629/+3.90%/t=32.9 | 4515/+4.15%/t=29.1 | 588258/+1.12%/t=10.2 | 2355 |
| `hyst_open_score_e3_x0` | open | long | hold5 | opt:Yes | 7912/+3.44%/t=34.1 | 5418/+3.52%/t=28.9 | 588258/+1.12%/t=10.2 | 2385 |
| `hyst_open_score_e5_x2` | open | long | hold1 | ALL | 15778/+1.44%/t=31.8 | 10862/+1.33%/t=27.6 | 602650/-0.07%/t=-1.7 | 3552 |
| `hyst_open_core_e5_x0` | open | long | hold5 | opt:Yes | 6781/+3.60%/t=32.3 | 4644/+3.73%/t=26.5 | 588258/+1.12%/t=10.2 | 2373 |
| `hyst_open_core_e5_x0` | open | long | hold1 | ALL | 16027/+1.35%/t=31.9 | 11120/+1.28%/t=26.4 | 602650/-0.07%/t=-1.7 | 3585 |
| `hyst_open_score_e5_x2` | open | long | hold1 | opt:Yes | 11686/+0.99%/t=32.4 | 8014/+0.93%/t=25.9 | 602650/-0.07%/t=-1.7 | 2388 |
| `hyst_open_score_e5_x0` | open | long | hold5 | opt:Yes | 7118/+3.20%/t=29.2 | 4868/+3.33%/t=25.2 | 588258/+1.12%/t=10.2 | 2379 |
| `hyst_open_core_e3_x0` | open | long | hold8 | mid(1-10B):all | 1932/+7.44%/t=23.8 | 1296/+6.69%/t=25.0 | 577464/+1.84%/t=13.0 | 1056 |
| `hyst_open_core_e5_x2` | open | long | hold1 | volM:high(>8%) | 3800/+4.35%/t=18.4 | 2604/+4.20%/t=24.4 | 602650/-0.07%/t=-1.7 | 860 |
| `hyst_open_score_e3_x0` | open | long | hold8 | mid(1-10B):all | 2109/+7.24%/t=23.7 | 1394/+6.48%/t=24.3 | 577464/+1.84%/t=13.0 | 1063 |
| `hyst_open_core_e5_x0` | open | long | hold1 | opt:Yes | 11298/+0.95%/t=29.8 | 7763/+0.91%/t=24.2 | 602650/-0.07%/t=-1.7 | 2388 |
| `hyst_open_score_e5_x2` | open | long | hold8 | mid(1-10B):all | 1634/+8.32%/t=22.6 | 1096/+7.59%/t=24.0 | 577464/+1.84%/t=13.0 | 1008 |
| `strict_A_ml2` | open | long | hold8 | ALL | 8120/+6.11%/t=13.9 | 5523/+5.04%/t=23.6 | 577464/+1.84%/t=13.0 | 3525 |
| `hyst_open_core_e5_x2` | open | long | hold8 | mid(1-10B):all | 1255/+8.21%/t=14.4 | 873/+7.70%/t=22.8 | 577464/+1.84%/t=13.0 | 945 |

### Near-miss (holdout t≥2, still FAIL, not baseline / tape-ride)

| def | clock | side | exit | cohort | hold t | why |
|---|---|---|---|---|---:|---|
| `hyst_open_core_e3_x0` | open | short | hold1 | ALL | 35.9 | hold1_without_hold2 |
| `hyst_open_core_e3_x0` | open | short | hold1 | volM:high(>8%) | 29.8 | hold1_without_hold2 |
| `hyst_open_score_e3_x0` | open | short | hold1 | ALL | 29.0 | hold1_without_hold2 |
| `hyst_open_score_e5_x2` | open | short | hold1 | ALL | 28.9 | hold1_without_hold2 |
| `tol3_open_core_ml2` | open | short | hold1 | ALL | 25.7 | hold1_without_hold2 |
| `hyst_open_core_e3_x0` | open | short | hold1 | opt:Yes | 24.7 | hold1_without_hold2 |
| `hyst_open_score_e3_x0` | open | short | hold1 | volM:high(>8%) | 23.9 | hold1_without_hold2 |
| `lag_Hred_Ared` | open | short | hold8 | ALL | 23.1 | disc_t |
| `j_dn_1` | open | short | hold8 | ALL | 22.9 | thin_disc |
| `hyst_open_score_e5_x2` | open | short | hold1 | volM:high(>8%) | 22.3 | hold1_without_hold2 |
| `j_dn_1` | open | short | hold8 | volM:high(>8%) | 21.9 | thin_disc |
| `hyst_open_score_e5_x0` | open | short | hold1 | ALL | 21.2 | hold1_without_hold2 |
| `j_up_1` | open | long | hold5 | volM:low(<3%) | 20.6 | thin_disc |
| `hyst_open_score_e5_x2` | open | short | hold1 | opt:Yes | 20.5 | hold1_without_hold2 |
| `hyst_open_score_e3_x0` | open | short | hold1 | opt:Yes | 20.0 | hold1_without_hold2 |

hold5/8 demoted as tape-rides (`long_hold_without_hold2`): **546** cells. Not keepers.

A–JL full rebuild stays phase 2. Live frozen. No strategy cards emitted.

