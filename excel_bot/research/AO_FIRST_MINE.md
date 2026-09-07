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

## H/I multi-horizon (standing keep + close pair)

_Generated 2026-09-07 · live `flatten_robust` frozen. Yahoo/rows A–F seed only. No merge. Label is H/I, not Futubull-only. T/BA highlight ghosts not the search._

## Plain English

The mine now predicts **H** (intraday %, close vs open) and **I** (daily %, close vs yesterday) on the ticker train/test split. Horizons: 1d, 2d, 3d, 1 week, 2 weeks. Same-row H and I are the labels — they are never inputs. Yesterday’s H/I may be a feature. Standing five-cell light + green O ± AH/FR is re-scored here. Close-entry pairs may use AA today. Futubull open→close is not the sole label.

**Standing KEEP 6** on stacked daily I (of 20 standing I-horizon rows). Research only — not a card.
**Close-entry KEEP 1 singles / 33 pairs** on 1d stacked I. 30 of those are today's F-green (volume fill) or a twin of it — same-close association with the I print, not a lagged forecast. AA-today pairs do not KEEP (SPY-down red / no edge). Research only — not a card. Trees only if a non-twin pair KEEPs; this F-green cluster is one print, not a new family.

Dumps **3603**. Close atoms **228**. Alive **37**. Pairs **430**. Everyone-else 1d stacked I +0.63% (n=498610). Everyone-else 1d H +0.11% (n=498610). Both SPY tapes required for a global KEEP. Light+O is the check, not a new search.

### Standing light+O ± AH/FR on stacked daily I (holdout moves)

| meaning | horizon | holdout I-stack | vs everyone | Q1 | spy↑ | spy↓ | heat hot | July | top-5 | verdict | why |
|---|---|---|---|---|---|---|---|---|---|---|---|
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light stays on until that sum falls to +2, and O is green | 1d | +2.04% (n=4012) | +1.41 pp | +3.00% (n=977) | +2.73% (n=5049) | +1.63% (n=3428) | +2.20% (n=9935) | 0% | 10% | **KEEP** | — |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light stays on until that sum falls to +2, and O is green | 2d | +2.08% (n=4012) | +1.10 pp | +3.03% (n=977) | +2.54% (n=5049) | +1.70% (n=3428) | +2.20% (n=9935) | 0% | 13% | **KEEP** | — |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light stays on until that sum falls to +2, and O is green | 3d | +3.48% (n=3992) | +2.21 pp | +2.66% (n=977) | +3.68% (n=4991) | +1.67% (n=3428) | +2.84% (n=9877) | 0% | 27% | **KILL** | ticker_ghost |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light stays on until that sum falls to +2, and O is green | 1w | +8.22% (n=3957) | +6.36 pp | +2.07% (n=977) | +3.97% (n=4947) | +1.52% (n=3391) | +4.76% (n=9796) | 0% | 53% | **KILL** | hold_t,ticker_ghost |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light stays on until that sum falls to +2, and O is green | 2w | +4.55% (n=3852) | +0.74 pp | +1.16% (n=977) | +5.19% (n=4860) | +3.16% (n=3306) | +4.83% (n=9548) | 0% | 45% | **KILL** | ticker_ghost |
| same five-cell light + green O, and the recent 5% down-day count (AH) is at least 1 | 1d | +3.39% (n=1277) | +2.76 pp | +4.78% (n=341) | +4.22% (n=1700) | +2.60% (n=1080) | +3.60% (n=3177) | 0% | 16% | **KEEP** | — |
| same five-cell light + green O, and the recent 5% down-day count (AH) is at least 1 | 2d | +3.42% (n=1277) | +2.44 pp | +4.29% (n=341) | +3.82% (n=1700) | +2.80% (n=1080) | +3.57% (n=3177) | 0% | 24% | **KEEP** | — |
| same five-cell light + green O, and the recent 5% down-day count (AH) is at least 1 | 3d | +7.20% (n=1277) | +5.93 pp | +3.58% (n=341) | +6.78% (n=1700) | +2.41% (n=1080) | +5.17% (n=3177) | 0% | 45% | **KILL** | hold_t,ticker_ghost |
| same five-cell light + green O, and the recent 5% down-day count (AH) is at least 1 | 1w | +6.82% (n=1266) | +4.96 pp | +2.04% (n=341) | +7.15% (n=1686) | +1.55% (n=1066) | +4.92% (n=3149) | 0% | 41% | **KILL** | ticker_ghost |
| same five-cell light + green O, and the recent 5% down-day count (AH) is at least 1 | 2w | +10.00% (n=1226) | +6.19 pp | -0.17% (n=341) | +9.50% (n=1658) | +1.61% (n=1037) | +6.78% (n=3062) | 0% | 58% | **KILL** | hold_t,q1_sign,ticker_ghost |
| same five-cell light + green O, and recent volume over 1M and/or G ≥ 3 (FR) is at least 1 | 1d | +2.34% (n=2222) | +1.72 pp | +3.31% (n=556) | +3.04% (n=2753) | +2.04% (n=1857) | +2.56% (n=5400) | 0% | 15% | **KEEP** | — |
| same five-cell light + green O, and recent volume over 1M and/or G ≥ 3 (FR) is at least 1 | 2d | +2.44% (n=2222) | +1.46 pp | +3.57% (n=556) | +2.89% (n=2753) | +2.15% (n=1857) | +2.58% (n=5400) | 0% | 20% | **KEEP** | — |
| same five-cell light + green O, and recent volume over 1M and/or G ≥ 3 (FR) is at least 1 | 3d | +4.70% (n=2210) | +3.43 pp | +3.03% (n=556) | +4.80% (n=2719) | +1.97% (n=1857) | +3.59% (n=5366) | 0% | 38% | **KILL** | ticker_ghost |
| same five-cell light + green O, and recent volume over 1M and/or G ≥ 3 (FR) is at least 1 | 1w | +12.77% (n=2186) | +10.90 pp | +2.29% (n=556) | +4.90% (n=2694) | +1.62% (n=1832) | +6.88% (n=5316) | 0% | 67% | **KILL** | hold_t,ticker_ghost |
| same five-cell light + green O, and recent volume over 1M and/or G ≥ 3 (FR) is at least 1 | 2w | +6.29% (n=2117) | +2.47 pp | +0.97% (n=556) | +7.20% (n=2638) | +4.29% (n=1778) | +6.81% (n=5158) | 0% | 59% | **KILL** | ticker_ghost |
| same five-cell light + green O, and both AH and FR are at least 1 | 1d | +3.81% (n=796) | +3.19 pp | +5.56% (n=203) | +4.70% (n=1011) | +3.26% (n=680) | +4.14% (n=1947) | 0% | 22% | **KILL** | hold1_without_hold2 |
| same five-cell light + green O, and both AH and FR are at least 1 | 2d | +3.93% (n=796) | +2.95 pp | +5.55% (n=203) | +4.39% (n=1011) | +3.65% (n=680) | +4.25% (n=1947) | 0% | 34% | **KILL** | ticker_ghost |
| same five-cell light + green O, and both AH and FR are at least 1 | 3d | +10.03% (n=796) | +8.76 pp | +4.52% (n=203) | +9.28% (n=1011) | +2.80% (n=680) | +6.76% (n=1947) | 0% | 56% | **KILL** | hold_t,ticker_ghost |
| same five-cell light + green O, and both AH and FR are at least 1 | 1w | +8.22% (n=787) | +6.36 pp | +2.47% (n=203) | +8.83% (n=1001) | +1.48% (n=670) | +5.84% (n=1927) | 0% | 54% | **KILL** | hold_t,ticker_ghost |
| same five-cell light + green O, and both AH and FR are at least 1 | 2w | +13.61% (n=758) | +9.79 pp | -1.40% (n=203) | +13.18% (n=988) | -0.13% (n=650) | +8.40% (n=1876) | 0% | 73% | **KILL** | disc_t,hold_t,spy_regime,q1_sign,ticker_ghost,regime_split |

Code names (after the English): `light_O` 1d, `light_O` 2d, `light_O` 3d, `light_O` 1w, `light_O` 2w, `light_O_AH` 1d, `light_O_AH` 2d, `light_O_AH` 3d.

### Same standing recipes on 1d H (intraday print)

| meaning | holdout H | spy↑ | spy↓ | verdict | why |
|---|---|---|---|---|---|
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light stays on until that sum falls to +2, and O is green | +2.11% (n=4012) | +2.43% (n=5049) | +1.98% (n=3428) | **KEEP** | — |
| same five-cell light + green O, and the recent 5% down-day count (AH) is at least 1 | +3.51% (n=1277) | +3.84% (n=1700) | +3.27% (n=1080) | **KEEP** | — |
| same five-cell light + green O, and recent volume over 1M and/or G ≥ 3 (FR) is at least 1 | +2.48% (n=2222) | +2.77% (n=2753) | +2.40% (n=1857) | **KEEP** | — |
| same five-cell light + green O, and both AH and FR are at least 1 | +3.93% (n=796) | +4.27% (n=1011) | +3.85% (n=680) | **KILL** | hold1_without_hold2 |

### Close-entry pair+lag on 1d stacked I (AA today legal)

| meaning | clock | holdout I-stack | vs everyone | Q1 | spy↑ | spy↓ | July | top-5 | verdict | why |
|---|---|---|---|---|---|---|---|---|---|---|
| F today is green AND G today is at least 1 | close | +2.38% (n=32383) | +1.75 pp | +2.89% (n=3713) | +2.94% (n=42410) | +1.64% (n=27050) | 0% | 4% | **KEEP** | — |
| F today is green | close | +2.91% (n=70223) | +2.28 pp | +1.36% (n=8056) | +2.87% (n=89698) | +2.63% (n=59668) | 0% | 23% | **KEEP** | — |
| AA today equals 1 | close | +0.86% (n=37862) | +0.24 pp | -0.41% (n=13315) | +1.28% (n=44204) | +0.06% (n=36380) | 0% | 79% | **KILL** | disc_t,hold_t,no_edge_vs_uncond,q1_sign,ticker_ghost |
| O 2 days ago is under 1 AND AA today equals 1 | close | +1.02% (n=27401) | +0.39 pp | -0.45% (n=8101) | +1.33% (n=32302) | +0.25% (n=25923) | 0% | 90% | **KILL** | disc_t,hold_t,lottery_day,no_edge_vs_uncond,q1_sign,ticker_ghost |
| F today is green AND K today is above 0 | close | +2.78% (n=64057) | +2.16 pp | +1.67% (n=7262) | +3.04% (n=82890) | +2.10% (n=54311) | 0% | 17% | **KEEP** | — |
| J yesterday is under 1 AND F today is green | close | +2.91% (n=70157) | +2.29 pp | +1.31% (n=8049) | +2.87% (n=89601) | +2.64% (n=59597) | 0% | 23% | **KEEP** | — |
| H 4 days ago is under 1 AND F today is green | close | +2.91% (n=70214) | +2.28 pp | +1.36% (n=8055) | +2.87% (n=89681) | +2.63% (n=59655) | 0% | 23% | **KEEP** | — |
| H yesterday is under 1 AND F today is green | close | +2.91% (n=70213) | +2.28 pp | +1.37% (n=8054) | +2.87% (n=89673) | +2.63% (n=59657) | 0% | 23% | **KEEP** | — |
| H 2 days ago is under 1 AND F today is green | close | +2.91% (n=70214) | +2.28 pp | +1.37% (n=8054) | +2.87% (n=89675) | +2.63% (n=59654) | 0% | 23% | **KEEP** | — |
| J 5 days ago is under 1 AND F today is green | close | +2.91% (n=70160) | +2.29 pp | +1.36% (n=8054) | +2.87% (n=89621) | +2.63% (n=59584) | 0% | 23% | **KEEP** | — |
| H 5 days ago is under 1 AND F today is green | close | +2.91% (n=70215) | +2.28 pp | +1.36% (n=8056) | +2.87% (n=89681) | +2.63% (n=59655) | 0% | 23% | **KEEP** | — |
| I 5 days ago is under 1 AND F today is green | close | +2.91% (n=70167) | +2.29 pp | +1.36% (n=8054) | +2.86% (n=89620) | +2.63% (n=59590) | 0% | 23% | **KEEP** | — |
| Q 5 days ago is under 1 AND F today is green | close | +2.91% (n=70223) | +2.28 pp | +1.36% (n=8056) | +2.87% (n=89698) | +2.63% (n=59668) | 0% | 23% | **KEEP** | — |
| Q yesterday is under 1 AND F today is green | close | +2.91% (n=70223) | +2.28 pp | +1.36% (n=8056) | +2.87% (n=89698) | +2.63% (n=59668) | 0% | 23% | **KEEP** | — |
| Q 3 days ago is under 1 AND F today is green | close | +2.91% (n=70223) | +2.28 pp | +1.36% (n=8056) | +2.87% (n=89698) | +2.63% (n=59668) | 0% | 23% | **KEEP** | — |
| Q 2 days ago is under 1 AND F today is green | close | +2.91% (n=70223) | +2.28 pp | +1.36% (n=8056) | +2.87% (n=89698) | +2.63% (n=59668) | 0% | 23% | **KEEP** | — |
| F today is green AND M today is under 1 | close | +2.91% (n=70223) | +2.28 pp | +1.36% (n=8056) | +2.87% (n=89698) | +2.63% (n=59668) | 0% | 23% | **KEEP** | — |
| I yesterday is under 1 AND F today is green | close | +2.91% (n=70163) | +2.28 pp | +1.35% (n=8049) | +2.86% (n=89596) | +2.64% (n=59603) | 0% | 23% | **KEEP** | — |
| H 3 days ago is under 1 AND F today is green | close | +2.91% (n=70213) | +2.28 pp | +1.36% (n=8055) | +2.87% (n=89674) | +2.63% (n=59655) | 0% | 23% | **KEEP** | — |
| DE yesterday is at least 1 AND F today is green | close | +2.91% (n=69921) | +2.29 pp | +1.33% (n=7326) | +2.88% (n=88961) | +2.63% (n=59663) | 0% | 23% | **KEEP** | — |

Code names (after the English): `F_l0_green__and__G_l0_ge1`, `F_l0_green`, `AA_l0_eq1`, `O_l2_lt1__and__AA_l0_eq1`, `F_l0_green__and__K_l0_gt0`, `J_l1_lt1__and__F_l0_green`, `H_l4_lt1__and__F_l0_green`, `H_l1_lt1__and__F_l0_green` ….

### Soft regimes

Sheet heat is the morning five-cell sum (hot ≥5 / mixed / cold ≤0). Tape is SPY up / down / flat. A global KEEP needs SPY-up and SPY-down to agree. Conditional notes stay in the why column (`regime_split`).

### What this does not change

- Standing open Futubull keep is still the research baseline, not a card. This file only re-scores it on H/I.
- Open-entry locked-44 pair+lag stays the accepted null (KEEP 0 / KILL 4266).
- T / BA / CZ / EH / IB / HO / IL / GV stay KILL as highlight ghosts — not this search.
- Same-row H and I are never features.
- Finviz BLOCKED. Live `flatten_robust` frozen. No cards.
- Q1 cut **2026-04-01**. Q3 cut **2026-07-01**.

Board totals (all labels × horizons): KEEP 85 · KILL 1227 · THIN 0.

Research only. One 2026 regime.
