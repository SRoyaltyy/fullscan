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

