# Excel emulator mine — whole workbook (A–JL)

_Generated 2026-09-07 · live `flatten_robust` is not changed. No merge without Cyrus._

## Priority (Cyrus override)

Mine the **whole emulator** (A..JL, 275 cols). Stored daily grids only persist A–O fills — that is the gap, not a missing model. `model.json` already has max_col 275. `run.py --all-cols` dumps it.

A–JL sample: **185** tickers · **PASS 0** · **FAIL 300** · **THIN 5** · 1.15 s/ticker.

A–O parallel thin track is in `AO_FIRST_MINE.md` (L3/S1 FAIL; 6 hysteresis hold1/2 research candidates). **A–O-only is not the whole Excel.**

Full A–JL table: `ALL_COLS_MINE.md`. Research only. No live wire.


# Excel A–JL (whole emulator) sample mine

_Generated 2026-09-07 · live `flatten_robust` is not changed. No merge without Cyrus._

## This is the whole Excel, not A–O

`model.json` already covers A..JL (275 cols). Stored daily grids only persist A–O fills — that is a **storage gap**, not a missing emulator. `run.py --all-cols` dumps all 275. This sample rebuilds A–JL from excel-state rows (lean path) and mines under PIT.

### Cost

- Lean rows-cache capture (this sample): **1.15 s/ticker** · rows 2–145 · 275 cols. N=185 → ~3.6 min.
- `run.py --all-cols` (Yahoo + rows 1–364): minutes/ticker — not used for this sample. Full 3603 via lean path ≈ 70 min.

### PIT

- Open: A-keyed fills; yesterday deeper value + today A.
- Close: same-day deeper values/fills; **core_score** (A..J includes D,E,F,H,I — landmine, CLOSE only).
- Sleeve holds 1/2/3/5/8. Futubull 0.15%/0.20%. Ship bar + ≥20 bp vs uncond. hold3/5/8 need hold2 edge.

Sample **185** tickers (114 discovery / 71 holdout). Patterns **64**. Cells **305**. **PASS 0** · **FAIL 300** · **THIN 5**.

A–O first mine is a **parallel thin track** (`AO_FIRST_MINE.md`) — not a substitute for this surface.

### Unconditional baseline (sample, futubull)

| clock | side | hold | n | avg net | t | win |
|---|---|---:|---:|---:|---:|---:|
| open | long | 1 | 25895 | -0.11% | -3.6 | 45% |
| open | long | 2 | 25710 | +2.03% | 1.6 | 46% |
| open | long | 3 | 25525 | +2.66% | 2.1 | 47% |
| open | long | 5 | 25155 | +3.14% | 2.4 | 48% |
| open | long | 8 | 24600 | +3.74% | 3.1 | 48% |
| open | short | 1 | 25895 | -0.24% | -7.7 | 46% |
| open | short | 2 | 25710 | -2.38% | -1.9 | 47% |
| open | short | 3 | 25525 | -3.01% | -2.4 | 47% |
| open | short | 5 | 25155 | -3.49% | -2.7 | 48% |
| open | short | 8 | 24600 | -4.09% | -3.4 | 48% |
| close | long | 1 | 25710 | +2.33% | 1.9 | 46% |
| close | long | 2 | 25525 | +2.72% | 2.1 | 47% |
| close | long | 3 | 25340 | +2.77% | 2.4 | 48% |
| close | long | 5 | 24970 | +3.49% | 3.0 | 48% |
| close | long | 8 | 24415 | +4.13% | 3.7 | 49% |
| close | short | 1 | 25710 | -2.68% | -2.2 | 45% |
| close | short | 2 | 25525 | -3.07% | -2.4 | 47% |
| close | short | 3 | 25340 | -3.12% | -2.7 | 47% |
| close | short | 5 | 24970 | -3.84% | -3.3 | 48% |
| close | short | 8 | 24415 | -4.48% | -4.1 | 48% |

### Primary (hold1/2 PASS)

*(none — no sleeve-shaped keeper on this sample)*

### Top cells (PASS then FAIL then THIN)

| verdict | def | clock | side | exit | disc | hold | base | tickers | why |
|---|---|---|---|---|---|---|---|---:|---|
| FAIL | `lag_IZeq1_Agreen` | open | long | hold8 | 1155/+1.54%/t=3.5 | 705/+1.67%/t=4.2 | 24600/+3.74%/t=3.1 | 185 | tape_split,no_edge_vs_uncond,no_edge_vs_uncond |
| FAIL | `lag_IZeq1_Agreen` | open | long | hold5 | 1155/+1.01%/t=3.1 | 705/+1.21%/t=3.9 | 25155/+3.14%/t=2.4 | 185 | tape_split,no_edge_vs_uncond,no_edge_vs_uncond |
| FAIL | `lag_IZeq1_Agreen` | open | long | hold3 | 1186/+0.61%/t=3.1 | 724/+0.75%/t=3.5 | 25525/+2.66%/t=2.1 | 185 | tape_split,spy_regime,no_edge_vs_uncond,no_edge_vs_uncond |
| FAIL | `lag_IZeq1_Agreen` | open | long | hold2 | 1186/+0.67%/t=4.0 | 724/+0.54%/t=3.4 | 25710/+2.03%/t=1.6 | 185 | tape_split,spy_regime,no_edge_vs_uncond,no_edge_vs_uncond |
| FAIL | `HI_fill_green` | close | long | hold5 | 3794/+0.15%/t=0.7 | 2240/+0.59%/t=2.9 | 24970/+3.49%/t=3.0 | 184 | disc_t,tape_split,no_edge_vs_uncond,no_edge_vs_uncond |
| FAIL | `CP_ge1` | close | long | hold8 | 5388/+0.17%/t=0.9 | 3336/+1.45%/t=2.8 | 24415/+4.13%/t=3.7 | 185 | disc_t,tape_split,no_edge_vs_uncond,no_edge_vs_uncond |
| FAIL | `IZ_eq1` | close | long | hold5 | 1429/+1.13%/t=3.6 | 856/+0.87%/t=2.8 | 24970/+3.49%/t=3.0 | 185 | tape_split,no_edge_vs_uncond,no_edge_vs_uncond |
| FAIL | `IB_ge3` | close | long | hold8 | 6739/+0.56%/t=1.8 | 3948/+1.51%/t=2.7 | 24415/+4.13%/t=3.7 | 184 | disc_t,no_edge_vs_uncond,no_edge_vs_uncond |
| FAIL | `HH_fill_green` | close | long | hold8 | 9416/+1.45%/t=2.9 | 5794/+1.04%/t=2.7 | 24415/+4.13%/t=3.7 | 185 | disc_t,no_edge_vs_uncond,no_edge_vs_uncond |
| FAIL | `core_score_ge2` | close | long | hold8 | 5870/+1.55%/t=2.2 | 3470/+1.31%/t=2.5 | 24415/+4.13%/t=3.7 | 185 | disc_t,tape_split,no_edge_vs_uncond,no_edge_vs_uncond |
| FAIL | `EL_fill_green` | close | long | hold8 | 6942/+2.90%/t=3.2 | 4206/+1.30%/t=2.4 | 24415/+4.13%/t=3.7 | 185 | no_edge_vs_uncond,no_edge_vs_uncond |
| FAIL | `HI_fill_green` | close | long | hold8 | 3709/+0.31%/t=1.2 | 2184/+2.34%/t=2.4 | 24415/+4.13%/t=3.7 | 184 | disc_t,no_edge_vs_uncond,no_edge_vs_uncond |
| FAIL | `HS_fill_green` | close | long | hold8 | 7096/+2.14%/t=2.7 | 4296/+1.26%/t=2.4 | 24415/+4.13%/t=3.7 | 185 | disc_t,no_edge_vs_uncond,no_edge_vs_uncond |
| FAIL | `HN_ge3` | close | long | hold5 | 5765/+0.59%/t=1.3 | 3449/+0.38%/t=2.4 | 24970/+3.49%/t=3.0 | 185 | disc_t,tape_split,no_edge_vs_uncond,no_edge_vs_uncond |
| FAIL | `HN_ge3` | close | long | hold8 | 5638/+0.76%/t=1.6 | 3353/+1.51%/t=2.3 | 24415/+4.13%/t=3.7 | 185 | disc_t,no_edge_vs_uncond,no_edge_vs_uncond |
| FAIL | `AA_fill_green` | close | long | hold8 | 2920/+2.47%/t=2.5 | 1631/+2.89%/t=2.2 | 24415/+4.13%/t=3.7 | 147 | disc_t,no_edge_vs_uncond,no_edge_vs_uncond |
| FAIL | `A_green` | open | long | hold8 | 9922/+1.14%/t=2.4 | 6035/+0.80%/t=2.2 | 24600/+3.74%/t=3.1 | 185 | disc_t,no_edge_vs_uncond,no_edge_vs_uncond |
| FAIL | `deeper_g5` | close | long | hold8 | 13847/+3.08%/t=4.5 | 8563/+6.35%/t=2.2 | 24415/+4.13%/t=3.7 | 185 | no_edge_vs_uncond |
| FAIL | `HF_fill_green` | close | long | hold8 | 13752/+1.49%/t=3.2 | 8514/+6.31%/t=2.1 | 24415/+4.13%/t=3.7 | 185 | no_edge_vs_uncond |
| FAIL | `deeper_g5` | close | long | hold5 | 14164/+1.76%/t=3.4 | 8750/+6.66%/t=2.1 | 24970/+3.49%/t=3.0 | 185 | no_edge_vs_uncond |
| FAIL | `EL_ge2` | close | long | hold8 | 4709/+1.51%/t=1.7 | 2763/+1.58%/t=2.0 | 24415/+4.13%/t=3.7 | 184 | disc_t,no_edge_vs_uncond,no_edge_vs_uncond |
| FAIL | `IZ_eq1` | close | long | hold8 | 1429/+1.67%/t=4.0 | 856/+3.14%/t=2.0 | 24415/+4.13%/t=3.7 | 185 | hold_t,no_edge_vs_uncond,no_edge_vs_uncond |
| FAIL | `CU_fill_green` | close | long | hold8 | 1132/+14.89%/t=2.5 | 631/+77.56%/t=2.0 | 24415/+4.13%/t=3.7 | 145 | disc_t,hold_t |
| FAIL | `GR_fill_green` | close | long | hold8 | 8150/+1.84%/t=2.8 | 4912/+0.86%/t=1.9 | 24415/+4.13%/t=3.7 | 185 | disc_t,hold_t,no_edge_vs_uncond,no_edge_vs_uncond |
| FAIL | `T_ge2` | close | long | hold8 | 1298/+0.56%/t=1.0 | 778/+60.91%/t=1.9 | 24415/+4.13%/t=3.7 | 120 | disc_t,hold_t,no_edge_vs_uncond |
| FAIL | `AD_ge1` | close | long | hold8 | 4562/+2.56%/t=2.8 | 2611/+1.58%/t=1.9 | 24415/+4.13%/t=3.7 | 185 | disc_t,hold_t,no_edge_vs_uncond,no_edge_vs_uncond |
| FAIL | `HF_fill_green` | close | long | hold5 | 14081/+1.02%/t=2.6 | 8715/+4.46%/t=1.9 | 24970/+3.49%/t=3.0 | 185 | disc_t,hold_t,no_edge_vs_uncond |
| FAIL | `CU_fill_green` | close | long | hold5 | 1143/+7.81%/t=1.9 | 637/+59.92%/t=1.9 | 24970/+3.49%/t=3.0 | 145 | disc_t,hold_t |
| FAIL | `P_fill_green` | close | long | hold8 | 2558/-0.06%/t=-0.2 | 1530/+1.94%/t=1.8 | 24415/+4.13%/t=3.7 | 180 | disc_t,hold_t,disc_sign,lottery,no_edge_vs_uncond,no_edge_vs_uncond |
| FAIL | `JA_eq1` | close | long | hold8 | 1927/+0.52%/t=1.3 | 1158/+3.27%/t=1.8 | 24415/+4.13%/t=3.7 | 184 | disc_t,hold_t,no_edge_vs_uncond,no_edge_vs_uncond |
| FAIL | `deeper_g5` | close | long | hold3 | 14366/+0.97%/t=2.4 | 8874/+5.84%/t=1.8 | 25340/+2.77%/t=2.4 | 185 | disc_t,hold_t,no_edge_vs_uncond |
| FAIL | `HF_fill_green` | close | long | hold3 | 14301/+0.64%/t=1.8 | 8846/+5.81%/t=1.8 | 25340/+2.77%/t=2.4 | 185 | disc_t,hold_t,no_edge_vs_uncond |
| FAIL | `L_ge1` | close | long | hold8 | 3804/+2.42%/t=2.3 | 2232/+1.83%/t=1.8 | 24415/+4.13%/t=3.7 | 185 | disc_t,hold_t,no_edge_vs_uncond,no_edge_vs_uncond |
| FAIL | `CU_fill_green` | close | long | hold2 | 1169/+6.53%/t=1.5 | 650/+86.01%/t=1.7 | 25525/+2.72%/t=2.1 | 145 | disc_t,hold_t,lottery |
| FAIL | `deeper_g5` | close | long | hold2 | 14468/+0.81%/t=1.9 | 8936/+6.23%/t=1.7 | 25525/+2.72%/t=2.1 | 185 | disc_t,hold_t,no_edge_vs_uncond |
| FAIL | `T_ge2` | close | long | hold3 | 1339/+0.32%/t=0.9 | 802/+62.15%/t=1.7 | 25340/+2.77%/t=2.4 | 120 | disc_t,hold_t,no_edge_vs_uncond |
| FAIL | `HF_fill_green` | close | long | hold2 | 14411/+0.46%/t=1.3 | 8913/+6.23%/t=1.7 | 25525/+2.72%/t=2.1 | 185 | disc_t,hold_t,no_edge_vs_uncond |
| FAIL | `T_ge2` | close | long | hold5 | 1320/+0.15%/t=0.3 | 793/+43.98%/t=1.7 | 24970/+3.49%/t=3.0 | 120 | disc_t,hold_t,no_edge_vs_uncond |
| FAIL | `CV_fill_green` | close | long | hold5 | 1915/+2.42%/t=1.1 | 1028/+38.47%/t=1.7 | 24970/+3.49%/t=3.0 | 135 | disc_t,hold_t,no_edge_vs_uncond |
| FAIL | `CV_fill_green` | close | long | hold8 | 1892/+5.93%/t=1.9 | 1022/+38.33%/t=1.7 | 24415/+4.13%/t=3.7 | 135 | disc_t,hold_t |
| FAIL | `T_ge2` | close | long | hold2 | 1344/+0.14%/t=0.5 | 805/+67.10%/t=1.7 | 25525/+2.72%/t=2.1 | 120 | disc_t,hold_t,no_edge_vs_uncond |
| FAIL | `DD_ge2` | close | long | hold5 | 9048/+1.77%/t=2.5 | 5547/+7.80%/t=1.7 | 24970/+3.49%/t=3.0 | 185 | disc_t,hold_t,no_edge_vs_uncond |
| FAIL | `CU_fill_green` | close | long | hold1 | 1172/+4.40%/t=1.2 | 651/+80.36%/t=1.7 | 25710/+2.33%/t=1.9 | 145 | disc_t,hold_t,lottery |
| FAIL | `HF_fill_green` | close | long | hold1 | 14521/+0.20%/t=0.7 | 8980/+5.74%/t=1.6 | 25710/+2.33%/t=1.9 | 185 | disc_t,hold_t,lottery,no_edge_vs_uncond |
| FAIL | `deeper_g5` | close | long | hold1 | 14576/+0.45%/t=1.3 | 8998/+5.72%/t=1.6 | 25710/+2.33%/t=1.9 | 185 | disc_t,hold_t,no_edge_vs_uncond |

Tickers: A, AAL, AAPL, ACCO, ADBE, ADUS, AEVA, AGIG, AIRS, ALEC, AMC, AMKR, ANNA, ARCT, ARE, ASPI, ATKR, AUR, AZN, BAC, BALL, BBAI, BEBE, BFLY, BKE, BMHL, BN, BSL, BTCS, BYRN, CAC, CASS, CAT, CDW, CDXS, CHEF, CLMT, CLNE, CMS, COLB, COOK, CROX, CSTM, CUZ, CVX, DAKT, DBGI, DHI, DIS, DJCO, DOO, DUKR, DUOL, EDSA, ELTX, ELVR, EPR, ETO, ETR, FAST, FBRT, FICO, FMFC, FNV, FRNM, FWDI, GBLI, GDOT, GLAS, GLDG, GOOGL, GPN, GRAF, H, HBNB, HD, HIX, HNGE, HSHP, HWH, IART, IIIV, INFQ, INSM, INTC, IREN, ISRG, JGH, JLL, JNJ, KD, KNX, KO, KOPN, LAKE, LEA, LGI, LNTH, LOAR, LWLG, MASK, MBOT, MCD, MDIA, MHF, MKTX, MNOV, MRKR, MSFT, MTB, MTG, NAMS, NBHC, NFGC, NFLX, NIXX, NMS, NSIT, NTR, NVDA, NVS, OC, OGN, ONB, ORCL, ORN, OSIS, PAVS, PCT, PEP, PESI, PFE, PL, PLBY, PPT, PRDO, PSTL, PXED, QCOM, QTTB, RCON, RDNT, RIME, RMCO, ROC, RVSN, RY, SBCF, SBUX, SEAT, SGHC, SHLS, SLF, SLGB, SNA, SPMC, SPWH, SSTI, SUIG, SVCO, TAOP, TDUP, TEI, TLS, TMDE, TRDA, TSLA, TTEK, TWG, TXN, TYRA, URGN, VBNK, VCEL, VKI, VRSN, VSTD, VZ, WBX, WIX, WLTH, WMT, WTTR, XFOR, YORW.

Research only. Live frozen. No strategy cards.

