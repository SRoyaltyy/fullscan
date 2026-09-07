# Excel A–JL (whole emulator) sample mine

_Generated 2026-09-07 · live `flatten_robust` is not changed. No merge without Cyrus._

## This is the whole Excel, not A–O

`model.json` already covers A..JL (275 cols). Stored daily grids only persist A–O fills — that is a **storage gap**, not a missing emulator. `run.py --all-cols` dumps all 275. This sample rebuilds A–JL from excel-state rows (lean path) and mines under PIT.

### Cost

- Lean rows-cache capture (this sample): **1.15 s/ticker** · rows 2–145 · 275 cols. N=55 → ~1.1 min.
- `run.py --all-cols` (Yahoo + rows 1–364): minutes/ticker — not used for this sample. Full 3603 via lean path ≈ 70 min.

### PIT

- Open: A-keyed fills; yesterday deeper value + today A.
- Close: same-day deeper values/fills; **core_score** (A..J includes D,E,F,H,I — landmine, CLOSE only).
- Sleeve holds 1/2/3/5/8. Futubull 0.15%/0.20%. Ship bar + ≥20 bp vs uncond. hold3/5/8 need hold2 edge.

Sample **55** tickers (40 discovery / 15 holdout). Patterns **64**. Cells **300**. **PASS 0** · **FAIL 275** · **THIN 25**.

A–O first mine is a **parallel thin track** (`AO_FIRST_MINE.md`) — not a substitute for this surface.

### Unconditional baseline (sample, futubull)

| clock | side | hold | n | avg net | t | win |
|---|---|---:|---:|---:|---:|---:|
| open | long | 1 | 7700 | -0.12% | -1.9 | 44% |
| open | long | 2 | 7645 | +0.00% | 0.0 | 46% |
| open | long | 3 | 7590 | -0.07% | -0.6 | 47% |
| open | long | 5 | 7480 | -0.07% | -0.4 | 47% |
| open | long | 8 | 7315 | +0.09% | 0.5 | 48% |
| open | short | 1 | 7700 | -0.23% | -3.4 | 45% |
| open | short | 2 | 7645 | -0.35% | -2.6 | 47% |
| open | short | 3 | 7590 | -0.28% | -2.4 | 48% |
| open | short | 5 | 7480 | -0.28% | -1.9 | 49% |
| open | short | 8 | 7315 | -0.44% | -2.5 | 49% |
| close | long | 1 | 7645 | +0.21% | 0.8 | 45% |
| close | long | 2 | 7590 | +0.17% | 0.6 | 46% |
| close | long | 3 | 7535 | +0.20% | 0.7 | 46% |
| close | long | 5 | 7425 | +0.16% | 0.8 | 47% |
| close | long | 8 | 7260 | +0.21% | 1.1 | 48% |
| close | short | 1 | 7645 | -0.56% | -2.1 | 45% |
| close | short | 2 | 7590 | -0.52% | -1.9 | 47% |
| close | short | 3 | 7535 | -0.55% | -1.9 | 47% |
| close | short | 5 | 7425 | -0.51% | -2.4 | 48% |
| close | short | 8 | 7260 | -0.56% | -2.9 | 49% |

### Primary (hold1/2 PASS)

*(none — no sleeve-shaped keeper on this sample)*

### Top cells (PASS then FAIL then THIN)

| verdict | def | clock | side | exit | disc | hold | base | tickers | why |
|---|---|---|---|---|---|---|---|---:|---|
| FAIL | `HF_fill_green` | close | long | hold8 | 4820/+0.01%/t=0.0 | 1764/+1.60%/t=5.6 | 7260/+0.21%/t=1.1 | 55 | disc_t,lottery,tape_split,no_edge_vs_uncond |
| FAIL | `HD_fill_green` | close | long | hold8 | 2398/+0.70%/t=1.6 | 944/+2.03%/t=4.9 | 7260/+0.21%/t=1.1 | 55 | disc_t |
| FAIL | `deeper_g5` | close | long | hold8 | 4831/+0.04%/t=0.1 | 1829/+1.30%/t=4.8 | 7260/+0.21%/t=1.1 | 55 | disc_t,lottery,tape_split,no_edge_vs_uncond |
| FAIL | `HH_fill_green` | close | long | hold8 | 3213/+0.34%/t=1.3 | 1263/+1.59%/t=4.5 | 7260/+0.21%/t=1.1 | 55 | disc_t,no_edge_vs_uncond |
| FAIL | `HF_fill_red` | close | short | hold8 | 420/+1.34%/t=0.6 | 201/+1.75%/t=4.5 | 7260/-0.56%/t=-2.9 | 55 | disc_t,tape_split,spy_regime |
| FAIL | `HF_fill_green` | close | long | hold5 | 4936/-0.12%/t=-0.8 | 1805/+0.84%/t=4.0 | 7425/+0.16%/t=0.8 | 55 | disc_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `GU_fill_green` | close | long | hold8 | 2290/-0.35%/t=-1.2 | 877/+1.61%/t=3.8 | 7260/+0.21%/t=1.1 | 55 | disc_t,disc_sign,lottery,tape_split,no_edge_vs_uncond |
| FAIL | `HB_fill_green` | close | long | hold8 | 2309/-0.35%/t=-1.2 | 881/+1.59%/t=3.8 | 7260/+0.21%/t=1.1 | 55 | disc_t,disc_sign,lottery,tape_split,no_edge_vs_uncond |
| FAIL | `IB_ge3` | close | long | hold8 | 2298/-0.15%/t=-0.5 | 922/+1.40%/t=3.7 | 7260/+0.21%/t=1.1 | 55 | disc_t,disc_sign,lottery,tape_split,no_edge_vs_uncond |
| FAIL | `CP_ge1` | close | long | hold8 | 1837/-0.45%/t=-1.8 | 820/+1.27%/t=3.7 | 7260/+0.21%/t=1.1 | 55 | disc_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `A_green` | open | long | hold8 | 3405/-0.22%/t=-0.9 | 1355/+1.11%/t=3.7 | 7315/+0.09%/t=0.5 | 55 | disc_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `HD_fill_green` | close | long | hold5 | 2448/+0.51%/t=1.3 | 966/+1.09%/t=3.5 | 7425/+0.16%/t=0.8 | 55 | disc_t |
| FAIL | `deeper_g5` | close | long | hold5 | 4939/-0.03%/t=-0.1 | 1871/+0.68%/t=3.4 | 7425/+0.16%/t=0.8 | 55 | disc_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `AD_ge1` | close | long | hold8 | 1633/-0.26%/t=-0.5 | 553/+1.70%/t=3.2 | 7260/+0.21%/t=1.1 | 55 | disc_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `HH_fill_green` | close | long | hold5 | 3290/+0.33%/t=1.5 | 1294/+0.78%/t=3.1 | 7425/+0.16%/t=0.8 | 55 | disc_t,no_edge_vs_uncond |
| FAIL | `HN_ge3` | close | long | hold8 | 1823/-0.61%/t=-2.4 | 764/+1.25%/t=3.0 | 7260/+0.21%/t=1.1 | 55 | disc_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `IB_ge3` | close | long | hold5 | 2357/+0.02%/t=0.1 | 946/+0.82%/t=3.0 | 7425/+0.16%/t=0.8 | 55 | disc_t,lottery,tape_split,no_edge_vs_uncond |
| FAIL | `GR_fill_green` | close | long | hold8 | 2800/-0.24%/t=-0.9 | 1051/+1.02%/t=2.8 | 7260/+0.21%/t=1.1 | 55 | disc_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `core_score_ge2` | close | long | hold8 | 1985/-0.60%/t=-2.3 | 767/+1.22%/t=2.8 | 7260/+0.21%/t=1.1 | 55 | disc_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `HF_fill_green` | close | long | hold3 | 5013/-0.12%/t=-1.1 | 1830/+0.43%/t=2.7 | 7535/+0.20%/t=0.7 | 55 | disc_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `EL_fill_green` | close | long | hold8 | 2425/-0.36%/t=-1.1 | 901/+1.09%/t=2.7 | 7260/+0.21%/t=1.1 | 55 | disc_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `AD_ge1` | close | long | hold5 | 1663/-0.55%/t=-1.4 | 571/+1.08%/t=2.7 | 7425/+0.16%/t=0.8 | 55 | disc_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `GU_fill_green` | close | long | hold5 | 2333/-0.30%/t=-1.3 | 895/+0.81%/t=2.6 | 7425/+0.16%/t=0.8 | 55 | disc_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `HF_fill_red` | close | short | hold5 | 424/-1.74%/t=-0.5 | 205/+0.79%/t=2.6 | 7425/-0.51%/t=-2.4 | 55 | disc_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `HB_fill_green` | close | long | hold5 | 2352/-0.30%/t=-1.3 | 899/+0.80%/t=2.6 | 7425/+0.16%/t=0.8 | 55 | disc_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `CP_ge1` | close | long | hold5 | 1883/-0.32%/t=-1.5 | 844/+0.70%/t=2.6 | 7425/+0.16%/t=0.8 | 55 | disc_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `IZ_eq1` | close | long | hold8 | 474/+0.85%/t=1.3 | 184/+2.20%/t=2.6 | 7260/+0.21%/t=1.1 | 55 | disc_t,tape_split |
| FAIL | `A_green` | open | long | hold5 | 3476/-0.15%/t=-0.7 | 1387/+0.56%/t=2.5 | 7480/-0.07%/t=-0.4 | 55 | disc_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `deeper_g5` | close | long | hold3 | 5017/-0.07%/t=-0.4 | 1898/+0.35%/t=2.4 | 7535/+0.20%/t=0.7 | 55 | disc_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `HD_fill_green` | close | long | hold3 | 2479/+0.17%/t=0.5 | 975/+0.51%/t=2.3 | 7535/+0.20%/t=0.7 | 55 | disc_t,lottery,tape_split,no_edge_vs_uncond |
| FAIL | `HN_ge3` | close | long | hold5 | 1868/-0.42%/t=-2.1 | 786/+0.68%/t=2.3 | 7425/+0.16%/t=0.8 | 55 | disc_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `IZ_eq1` | close | long | hold5 | 474/+0.96%/t=1.9 | 184/+1.30%/t=2.1 | 7425/+0.16%/t=0.8 | 55 | disc_t,tape_split |
| FAIL | `HI_fill_green` | close | long | hold8 | 1189/-1.07%/t=-3.0 | 482/+0.99%/t=2.0 | 7260/+0.21%/t=1.1 | 55 | disc_t,hold_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `HF_fill_green` | close | long | hold2 | 5052/-0.16%/t=-1.6 | 1842/+0.25%/t=1.9 | 7590/+0.17%/t=0.6 | 55 | disc_t,hold_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `EL_ge2` | close | long | hold8 | 1664/-0.64%/t=-2.0 | 586/+1.05%/t=1.9 | 7260/+0.21%/t=1.1 | 55 | disc_t,hold_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `lag_IZeq1_Agreen` | open | long | hold8 | 377/+0.82%/t=1.3 | 149/+1.56%/t=1.9 | 7315/+0.09%/t=0.5 | 55 | disc_t,hold_t,tape_split |
| FAIL | `CP_ge1` | close | long | hold3 | 1903/-0.28%/t=-1.6 | 859/+0.37%/t=1.9 | 7535/+0.20%/t=0.7 | 55 | disc_t,hold_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `lag_JAge1_Agreen` | open | long | hold8 | 616/-0.72%/t=-1.4 | 266/+1.20%/t=1.8 | 7315/+0.09%/t=0.5 | 55 | disc_t,hold_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `GR_fill_green` | close | long | hold5 | 2864/-0.14%/t=-0.6 | 1076/+0.51%/t=1.8 | 7425/+0.16%/t=0.8 | 55 | disc_t,hold_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `HH_fill_green` | close | long | hold3 | 3334/+0.08%/t=0.5 | 1308/+0.33%/t=1.8 | 7535/+0.20%/t=0.7 | 55 | disc_t,hold_t,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `EL_fill_green` | close | long | hold5 | 2477/-0.15%/t=-0.6 | 922/+0.54%/t=1.8 | 7425/+0.16%/t=0.8 | 55 | disc_t,hold_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `lag_IZeq1_Agreen` | open | long | hold5 | 377/+0.78%/t=1.5 | 149/+1.15%/t=1.8 | 7480/-0.07%/t=-0.4 | 55 | disc_t,hold_t,tape_split |
| FAIL | `JA_eq1` | close | long | hold5 | 689/-0.38%/t=-0.9 | 284/+0.96%/t=1.8 | 7425/+0.16%/t=0.8 | 55 | disc_t,hold_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `lag_IZeq1_Agreen` | open | long | hold2 | 390/+0.68%/t=2.6 | 154/+0.48%/t=1.7 | 7645/+0.00%/t=0.0 | 55 | disc_t,hold_t,tape_split,spy_regime |
| FAIL | `HI_fill_green` | close | long | hold5 | 1217/-0.71%/t=-2.4 | 498/+0.69%/t=1.7 | 7425/+0.16%/t=0.8 | 55 | disc_t,hold_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |

Tickers: A, AAL, AENT, AMC, ARE, ARVN, AZN, BLND, BMHL, BYRN, CETX, CLNE, CMS, CTO, DHI, DJCO, E, EPR, FBRT, FEAM, FRNM, GGZ, GRAF, H, HUHU, INFQ, INSM, JRSH, LAKE, LEA, LSTA, MDIA, MOS, MTB, NAMS, NOW, OC, OGN, OXLC, PL, PRDO, QBTS, RIME, RY, SAGT, SHLS, SOPH, SUIG, SVCO, THG, TTEK, UTMD, VBNK, VSTD, WNEB.

Research only. Live frozen. No strategy cards.

