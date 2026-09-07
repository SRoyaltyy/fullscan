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

