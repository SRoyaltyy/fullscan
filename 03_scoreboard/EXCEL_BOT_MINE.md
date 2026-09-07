# Excel emulator mine — first A–JL cut

_Generated 2026-09-07 · live `flatten_robust` frozen. No merge._

## First cut (critical path)

Lean `--all-cols` sample rebuild: **N=55** (40 discovery / 15 holdout) · **1.15 s/ticker** · 1.1 min total · rows 2–145 · 275 cols. 0 capture errors.

**PASS 0 · FAIL 275 · THIN 25.** Clean null.

Ship bar: disc n≥300 t≥3; hold n≥100 t≥2; ≥50 tickers; ≥20 dates; lottery; both tape halves; spy↑/↓; ≥20 bp vs uncond; hold3/5/8 need hold2. Futubull 0.15%/0.20%.

| verdict | def | clock | side | exit | n / effect (disc) | hold | tape early | tape late | why |
|---|---|---|---|---|---|---|---|---|---|
| FAIL | `HF_fill_green` | close | long | hold2 | 5052/-0.16%/t=-1.6 | 1842/+0.25%/t=1.9 | 2325/+0.17%/t=1.3 | 4569/-0.16%/t=-1.6 | disc_t,hold_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `lag_IZeq1_Agreen` | open | long | hold2 | 390/+0.68%/t=2.6 | 154/+0.48%/t=1.7 | 372/+1.13%/t=4.7 | 172/-0.49%/t=-1.4 | disc_t,hold_t,tape_split,spy_regime |
| FAIL | `HD_fill_green` | close | long | hold2 | 2496/+0.13%/t=0.5 | 983/+0.27%/t=1.5 | 1070/+0.61%/t=3.0 | 2409/-0.02%/t=-0.1 | disc_t,hold_t,lottery,tape_split,no_edge_vs_uncond |
| FAIL | `deeper_g5` | close | long | hold2 | 5055/-0.06%/t=-0.4 | 1911/+0.18%/t=1.5 | 2415/+0.12%/t=0.9 | 4551/-0.05%/t=-0.3 | disc_t,hold_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `CP_ge1` | close | long | hold2 | 1914/-0.14%/t=-1.0 | 865/+0.24%/t=1.5 | 834/+0.66%/t=3.1 | 1945/-0.31%/t=-2.6 | disc_t,hold_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `GU_fill_green` | close | long | hold2 | 2394/-0.19%/t=-1.4 | 919/+0.20%/t=1.1 | 1110/+0.25%/t=1.4 | 2203/-0.25%/t=-1.9 | disc_t,hold_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `HB_fill_green` | close | long | hold2 | 2413/-0.20%/t=-1.5 | 923/+0.20%/t=1.1 | 1133/+0.22%/t=1.2 | 2203/-0.25%/t=-1.9 | disc_t,hold_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `IZ_eq1` | close | long | hold2 | 495/+0.50%/t=1.7 | 191/+0.33%/t=1.0 | 465/+1.12%/t=4.3 | 221/-0.95%/t=-2.2 | disc_t,hold_t,tape_split,no_edge_vs_uncond |
| FAIL | `A_green` | open | long | hold2 | 3540/+0.15%/t=0.6 | 1417/+0.12%/t=1.0 | 1788/+0.12%/t=0.9 | 3169/+0.15%/t=0.6 | disc_t,hold_t,lottery,spy_regime,no_edge_vs_uncond |
| FAIL | `EL_ge2` | close | long | hold2 | 1718/-0.04%/t=-0.2 | 605/+0.21%/t=0.9 | 863/+0.42%/t=1.8 | 1460/-0.21%/t=-1.2 | disc_t,hold_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `O_ge1` | close | long | hold1 | 857/-0.24%/t=-1.2 | 285/+0.26%/t=0.9 | 409/+0.46%/t=1.5 | 733/-0.44%/t=-2.2 | disc_t,hold_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `CP_ge1` | close | long | hold1 | 1924/-0.19%/t=-1.8 | 872/+0.10%/t=0.8 | 834/+0.31%/t=1.9 | 1962/-0.27%/t=-2.9 | disc_t,hold_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `HH_fill_green` | close | long | hold2 | 3356/+0.02%/t=0.1 | 1314/+0.12%/t=0.8 | 1687/+0.18%/t=1.1 | 2983/-0.03%/t=-0.2 | disc_t,hold_t,lottery,tape_split,no_edge_vs_uncond |
| FAIL | `O_ge1` | close | long | hold2 | 852/-0.60%/t=-2.2 | 285/+0.34%/t=0.8 | 409/+0.79%/t=1.9 | 728/-1.01%/t=-3.7 | disc_t,hold_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `L_ge1` | close | long | hold2 | 1280/-0.64%/t=-3.3 | 449/+0.24%/t=0.8 | 683/-0.10%/t=-0.4 | 1046/-0.62%/t=-2.8 | disc_t,hold_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `lag_ELge2_Agreen` | open | long | hold2 | 1375/-0.10%/t=-0.6 | 512/+0.17%/t=0.8 | 671/+0.50%/t=2.1 | 1216/-0.32%/t=-1.9 | disc_t,hold_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `lag_Lge1_Agreen` | open | long | hold2 | 1103/-0.56%/t=-3.0 | 407/+0.20%/t=0.8 | 603/-0.19%/t=-0.8 | 907/-0.47%/t=-2.3 | disc_t,hold_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `HD_fill_green` | close | long | hold1 | 2510/+0.27%/t=0.9 | 991/+0.09%/t=0.7 | 1070/+0.28%/t=1.9 | 2431/+0.20%/t=0.6 | disc_t,hold_t,no_edge_vs_uncond |
| FAIL | `EL_fill_green` | close | long | hold2 | 2520/-0.12%/t=-0.9 | 935/+0.12%/t=0.7 | 1302/+0.33%/t=1.8 | 2153/-0.29%/t=-2.1 | disc_t,hold_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `core_score_ge2` | close | long | hold2 | 2057/-0.22%/t=-1.6 | 804/+0.13%/t=0.7 | 1020/+0.38%/t=1.8 | 1841/-0.39%/t=-3.0 | disc_t,hold_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `HF_fill_green` | close | long | hold1 | 5091/-0.18%/t=-2.4 | 1855/+0.05%/t=0.6 | 2325/+0.02%/t=0.2 | 4621/-0.18%/t=-2.5 | disc_t,hold_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `P_fill_green` | close | long | hold2 | 829/-0.29%/t=-1.3 | 368/+0.11%/t=0.5 | 289/+0.72%/t=1.9 | 908/-0.44%/t=-2.5 | disc_t,hold_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `HF_fill_red` | close | short | hold2 | 428/-4.55%/t=-1.0 | 213/+0.09%/t=0.5 | 480/+0.29%/t=1.1 | 161/-12.85%/t=-1.0 | disc_t,hold_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `IB_ge3` | close | long | hold2 | 2399/-0.09%/t=-0.5 | 967/+0.07%/t=0.4 | 942/+0.29%/t=1.4 | 2424/-0.18%/t=-1.0 | disc_t,hold_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |

No keeper. No hold1/2 sleeve. Live `flatten_robust` untouched.

Full table (300 cells / 64 pats): `excel_bot/research/ALL_COLS_MINE.md`.
Research only. No cards. No merge without Cyrus.

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

