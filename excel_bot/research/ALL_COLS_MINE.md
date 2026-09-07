# Excel A–JL (whole emulator) sample mine

_Generated 2026-09-07 · live `flatten_robust` is not changed. No merge without Cyrus._

## This is the whole Excel, not A–O

`model.json` already covers A..JL (275 cols). Stored daily grids only persist A–O fills — that is a **storage gap**, not a missing emulator. `run.py --all-cols` dumps all 275. This sample rebuilds A–JL from excel-state rows (lean path) and mines under PIT.

### Cost

- Lean rows-cache capture (this sample): **1.32 s/ticker** · rows 2–145 · 275 cols. N=499 → ~11.0 min.
- `run.py --all-cols` (Yahoo + rows 1–364): minutes/ticker — not used for this sample. Full 3603 via lean path ≈ 70 min.

### PIT

- Open: A-keyed fills; yesterday deeper value + today A.
- Close: same-day deeper values/fills; **core_score** (A..J includes D,E,F,H,I — landmine, CLOSE only).
- Sleeve holds 1/2/3/5/8. Futubull 0.15%/0.20%. Ship bar + ≥20 bp vs uncond. hold3/5/8 need hold2 edge.

Sample **499** tickers (311 discovery / 188 holdout). Patterns **64**. Cells **305**. **PASS 0** · **FAIL 305** · **THIN 0**.

A–O first mine is a **parallel thin track** (`AO_FIRST_MINE.md`) — not a substitute for this surface.

### Unconditional baseline (sample, futubull)

| clock | side | hold | n | avg net | t | win |
|---|---|---:|---:|---:|---:|---:|
| open | long | 1 | 69753 | -0.14% | -7.6 | 44% |
| open | long | 2 | 69254 | +0.80% | 1.7 | 46% |
| open | long | 3 | 68755 | +1.16% | 2.4 | 47% |
| open | long | 5 | 67757 | +1.51% | 2.9 | 47% |
| open | long | 8 | 66260 | +2.03% | 4.2 | 48% |
| open | short | 1 | 69753 | -0.21% | -11.1 | 46% |
| open | short | 2 | 69254 | -1.15% | -2.5 | 48% |
| open | short | 3 | 68755 | -1.51% | -3.1 | 48% |
| open | short | 5 | 67757 | -1.86% | -3.6 | 49% |
| open | short | 8 | 66260 | -2.38% | -4.9 | 49% |
| close | long | 1 | 69254 | +0.93% | 2.0 | 45% |
| close | long | 2 | 68755 | +1.20% | 2.4 | 47% |
| close | long | 3 | 68256 | +1.31% | 2.9 | 47% |
| close | long | 5 | 67258 | +1.79% | 3.8 | 47% |
| close | long | 8 | 65761 | +2.25% | 4.9 | 48% |
| close | short | 1 | 69254 | -1.28% | -2.7 | 46% |
| close | short | 2 | 68755 | -1.55% | -3.1 | 47% |
| close | short | 3 | 68256 | -1.66% | -3.6 | 48% |
| close | short | 5 | 67258 | -2.14% | -4.6 | 49% |
| close | short | 8 | 65761 | -2.60% | -5.7 | 49% |

### Primary (hold1/2 PASS)

*(none — no sleeve-shaped keeper on this sample)*

### Top cells (PASS then FAIL then THIN)

| verdict | def | clock | side | exit | disc | hold | base | tickers | why |
|---|---|---|---|---|---|---|---|---:|---|
| FAIL | `deeper_g5` | close | long | hold8 | 37499/+1.48%/t=5.3 | 22547/+3.53%/t=2.9 | 65761/+2.25%/t=4.9 | 499 | no_edge_vs_uncond |
| FAIL | `deeper_g5` | close | long | hold5 | 38356/+0.86%/t=4.0 | 23059/+3.22%/t=2.5 | 67258/+1.79%/t=3.8 | 499 | no_edge_vs_uncond |
| FAIL | `CV_fill_green` | close | long | hold8 | 5761/+1.58%/t=1.4 | 3139/+18.41%/t=2.3 | 65761/+2.25%/t=4.9 | 400 | disc_t,no_edge_vs_uncond |
| FAIL | `T_ge2` | close | long | hold8 | 2507/-0.06%/t=-0.1 | 1529/+37.48%/t=2.2 | 65761/+2.25%/t=4.9 | 294 | disc_t,disc_sign,lottery,no_edge_vs_uncond |
| FAIL | `CU_fill_green` | close | long | hold8 | 3427/+6.04%/t=2.9 | 1934/+29.23%/t=2.2 | 65761/+2.25%/t=4.9 | 410 | disc_t |
| FAIL | `T_ge2` | close | long | hold5 | 2562/-0.55%/t=-1.6 | 1555/+32.04%/t=2.2 | 67258/+1.79%/t=3.8 | 294 | disc_t,disc_sign,lottery,no_edge_vs_uncond |
| FAIL | `DD_ge2` | close | long | hold5 | 25058/+0.67%/t=2.4 | 14853/+4.04%/t=2.2 | 67258/+1.79%/t=3.8 | 499 | disc_t,no_edge_vs_uncond |
| FAIL | `CU_fill_red` | close | short | hold3 | 258/-4.10%/t=-0.5 | 128/+4.64%/t=2.1 | 68256/-1.66%/t=-3.6 | 140 | thin_disc,disc_t,disc_sign,lottery,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `DD_ge2` | close | long | hold8 | 24849/+1.24%/t=3.5 | 14742/+3.54%/t=2.1 | 65761/+2.25%/t=4.9 | 499 | no_edge_vs_uncond |
| FAIL | `CU_fill_green` | close | long | hold5 | 3475/+3.06%/t=2.1 | 1959/+22.53%/t=2.1 | 67258/+1.79%/t=3.8 | 410 | disc_t |
| FAIL | `GU_fill_green` | close | long | hold8 | 17759/+0.91%/t=2.6 | 10794/+4.68%/t=2.1 | 65761/+2.25%/t=4.9 | 499 | disc_t,no_edge_vs_uncond |
| FAIL | `HB_fill_green` | close | long | hold8 | 17909/+0.90%/t=2.6 | 10879/+4.62%/t=2.0 | 65761/+2.25%/t=4.9 | 499 | disc_t,no_edge_vs_uncond |
| FAIL | `HF_fill_green` | close | long | hold8 | 37220/+0.89%/t=4.4 | 22489/+2.27%/t=2.0 | 65761/+2.25%/t=4.9 | 498 | no_edge_vs_uncond |
| FAIL | `EL_fill_green` | close | long | hold8 | 18689/+1.45%/t=3.7 | 11248/+1.82%/t=2.0 | 65761/+2.25%/t=4.9 | 499 | no_edge_vs_uncond |
| FAIL | `T_ge2` | close | long | hold3 | 2602/-0.47%/t=-1.9 | 1568/+38.09%/t=2.0 | 68256/+1.31%/t=2.9 | 295 | disc_t,hold_t,disc_sign,lottery,no_edge_vs_uncond |
| FAIL | `lag_IZeq1_Agreen` | open | long | hold2 | 3125/+0.47%/t=4.4 | 1871/+0.26%/t=2.0 | 69254/+0.80%/t=1.7 | 499 | hold_t,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `deeper_g5` | close | long | hold3 | 38908/+0.48%/t=2.8 | 23392/+2.51%/t=2.0 | 68256/+1.31%/t=2.9 | 499 | disc_t,hold_t,no_edge_vs_uncond |
| FAIL | `CV_fill_green` | close | long | hold5 | 5837/+0.51%/t=0.7 | 3180/+14.37%/t=1.9 | 67258/+1.79%/t=3.8 | 400 | disc_t,hold_t,lottery,no_edge_vs_uncond |
| FAIL | `HD_fill_green` | close | long | hold8 | 19231/+0.68%/t=3.0 | 11203/+3.75%/t=1.9 | 65761/+2.25%/t=4.9 | 498 | hold_t,no_edge_vs_uncond |
| FAIL | `AD_ge1` | close | long | hold8 | 12458/+1.23%/t=3.0 | 6967/+2.62%/t=1.8 | 65761/+2.25%/t=4.9 | 499 | hold_t,no_edge_vs_uncond |
| FAIL | `T_ge2` | close | long | hold2 | 2616/-0.42%/t=-2.1 | 1575/+37.22%/t=1.8 | 68755/+1.20%/t=2.4 | 295 | disc_t,hold_t,disc_sign,lottery,no_edge_vs_uncond |
| FAIL | `HF_fill_green` | close | long | hold5 | 38109/+0.53%/t=3.1 | 23023/+1.60%/t=1.8 | 67258/+1.79%/t=3.8 | 498 | hold_t,no_edge_vs_uncond |
| FAIL | `CU_fill_green` | close | long | hold2 | 3557/+2.82%/t=1.8 | 2005/+28.34%/t=1.8 | 68755/+1.20%/t=2.4 | 410 | disc_t,hold_t |
| FAIL | `deeper_g5` | close | long | hold2 | 39187/+0.39%/t=2.3 | 23557/+2.43%/t=1.8 | 68755/+1.20%/t=2.4 | 499 | disc_t,hold_t,no_edge_vs_uncond |
| FAIL | `CU_fill_green` | close | long | hold3 | 3535/+3.66%/t=2.5 | 1995/+19.10%/t=1.7 | 68256/+1.31%/t=2.9 | 410 | disc_t,hold_t |
| FAIL | `HF_fill_green` | close | long | hold3 | 38703/+0.32%/t=2.2 | 23369/+2.12%/t=1.7 | 68256/+1.31%/t=2.9 | 498 | disc_t,hold_t,no_edge_vs_uncond |
| FAIL | `deeper_g5` | close | long | hold1 | 39469/+0.21%/t=1.5 | 23732/+2.26%/t=1.7 | 69254/+0.93%/t=2.0 | 499 | disc_t,hold_t,no_edge_vs_uncond |
| FAIL | `CU_fill_red` | close | short | hold8 | 251/+1.87%/t=0.5 | 126/+6.81%/t=1.7 | 65761/-2.60%/t=-5.7 | 139 | thin_disc,disc_t,hold_t,tape_split |
| FAIL | `EL_ge2` | close | long | hold8 | 12547/+0.96%/t=2.3 | 7553/+2.05%/t=1.7 | 65761/+2.25%/t=4.9 | 498 | disc_t,hold_t,no_edge_vs_uncond |
| FAIL | `HF_fill_green` | close | long | hold2 | 39001/+0.22%/t=1.5 | 23546/+2.29%/t=1.7 | 68755/+1.20%/t=2.4 | 499 | disc_t,hold_t,no_edge_vs_uncond |
| FAIL | `CU_fill_green` | close | long | hold1 | 3568/+1.56%/t=1.2 | 2008/+26.02%/t=1.7 | 69254/+0.93%/t=2.0 | 410 | disc_t,hold_t,lottery |
| FAIL | `lag_IZeq1_Agreen` | open | long | hold3 | 3125/+0.49%/t=3.5 | 1871/+0.29%/t=1.6 | 68755/+1.16%/t=2.4 | 499 | hold_t,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `DD_ge2` | close | long | hold3 | 25187/+0.26%/t=1.3 | 14920/+1.93%/t=1.6 | 68256/+1.31%/t=2.9 | 499 | disc_t,hold_t,no_edge_vs_uncond |
| FAIL | `L_ge1` | close | long | hold8 | 9893/+1.13%/t=2.4 | 5766/+2.30%/t=1.6 | 65761/+2.25%/t=4.9 | 499 | disc_t,hold_t,no_edge_vs_uncond |
| FAIL | `lag_IZeq1_Agreen` | open | long | hold5 | 3051/+0.78%/t=3.6 | 1827/+0.35%/t=1.6 | 67757/+1.51%/t=2.9 | 499 | hold_t,tape_split,spy_regime,no_edge_vs_uncond |
| FAIL | `L_ge1` | close | long | hold5 | 10061/+0.60%/t=1.5 | 5858/+2.63%/t=1.6 | 67258/+1.79%/t=3.8 | 499 | disc_t,hold_t,no_edge_vs_uncond |
| FAIL | `HF_fill_green` | close | long | hold1 | 39297/+0.07%/t=0.6 | 23723/+2.11%/t=1.6 | 69254/+0.93%/t=2.0 | 499 | disc_t,hold_t,lottery,tape_split,no_edge_vs_uncond |
| FAIL | `IB_ge3` | close | long | hold8 | 17546/+0.52%/t=2.5 | 10420/+1.17%/t=1.6 | 65761/+2.25%/t=4.9 | 498 | disc_t,hold_t,no_edge_vs_uncond |
| FAIL | `DD_ge2` | close | long | hold2 | 25250/+0.08%/t=0.6 | 14947/+3.06%/t=1.6 | 68755/+1.20%/t=2.4 | 499 | disc_t,hold_t,lottery,no_edge_vs_uncond |
| FAIL | `GU_fill_green` | close | long | hold5 | 18123/+0.38%/t=1.5 | 11015/+3.07%/t=1.5 | 67258/+1.79%/t=3.8 | 499 | disc_t,hold_t,no_edge_vs_uncond |
| FAIL | `HB_fill_green` | close | long | hold5 | 18273/+0.37%/t=1.5 | 11100/+3.03%/t=1.5 | 67258/+1.79%/t=3.8 | 499 | disc_t,hold_t,no_edge_vs_uncond |
| FAIL | `CV_fill_green` | close | long | hold1 | 5944/+0.54%/t=0.8 | 3248/+9.56%/t=1.5 | 69254/+0.93%/t=2.0 | 400 | disc_t,hold_t,lottery,no_edge_vs_uncond |
| FAIL | `O_ge1` | close | long | hold8 | 6660/+0.20%/t=0.6 | 3776/+3.65%/t=1.5 | 65761/+2.25%/t=4.9 | 482 | disc_t,hold_t,no_edge_vs_uncond |
| FAIL | `HH_fill_green` | close | long | hold8 | 25269/+0.77%/t=3.4 | 15126/+0.69%/t=1.4 | 65761/+2.25%/t=4.9 | 499 | hold_t,no_edge_vs_uncond |
| FAIL | `T_ge2` | close | long | hold1 | 2626/-0.34%/t=-2.1 | 1583/+17.29%/t=1.4 | 69254/+0.93%/t=2.0 | 295 | disc_t,hold_t,disc_sign,lottery,spy_regime,no_edge_vs_uncond |

Tickers: A, AAL, AAPL, ABOS, ACCO, ACI, ACOG, ACXP, ADBE, ADMA, ADUS, AEHR, AERT, AEVA, AGI, AGIG, AGRO, AIFA, AIOT, AIRS, ALEC, ALG, ALMS, ALMU, AMBO, AMC, AMKR, AMR, AMRX, AN, ANNA, APA, APAM, APT, ARCT, ARE, AROC, ASAN, ASB, ASPI, ASTI, ATEX, ATKR, ATPC, AUNA, AUR, AVPT, AWRE, AX, AZN, BAC, BALL, BBAI, BBIO, BBOT, BCO, BCV, BEBE, BFLY, BFRG, BGY, BHRB, BIPC, BKE, BLFS, BLSH, BMHL, BN, BOH, BON, BRAI, BRLS, BSL, BTCS, BTT, BUDA, BWA, BWLP, BYRN, CAC, CAE, CANG, CASS, CAT, CB, CBRL, CCL, CCU, CDW, CDXS, CEPS, CFBK, CGEN, CHEF, CHT, CIGI, CION, CLMT, CLNE, CLSK, CMDB, CMP, CMS, CNMD, CNX, COGT, COLB, COOK, CPA, CPRI, CRDF, CRMD, CROX, CSTM, CTGO, CTSH, CUZ, CVE, CVV, CVX, CYAB, CZNC, DAKT, DBGI, DCOY, DDD, DFH, DFLI, DHI, DIS, DJCO, DJT, DMRC, DOLE, DOO, DRI, DSWL, DSY, DUKR, DUOL, DXLG, EBMT, ECC, EDSA, EGAN, EGO, EIM, ELTX, ELVR, EMF, ENTG, EPR, EPRT, ERC, ES, ESI, ETO, ETR, EVH, EVR, EXC, EZPW, FAST, FBRT, FDBC, FENC, FF, FICO, FIZZ, FLNA, FLUX, FMFC, FNV, FOA, FRA, FRNM, FRST, FTDR, FTF, FUFU, FWDI, GANX, GBLI, GBX, GDOT, GELS, GENB, GIFT, GILT, GLAS, GLDG, GLOB, GLV, GNE, GNT, GOOGL, GPN, GRAF, GRND, GT, GTEC, H, HAFN, HBNB, HCA, HD, HDRN, HEQ, HIMS, HIX, HNGE, HODO, HQI, HRI, HSHP, HUBB, HUN, HWH, HXHX, IART, IBTA, IDA, IDT, IIIV, IMNN, IMUX, INFQ, INKT, INSM, INTC, INTJ, IOTR, IP, IRDM, IREN, ISRG, ITRG, IVDA, JANX, JBTM, JGH, JLL, JNJ, JWEL, KD, KEN, KGEI, KLIC, KMRK, KNX, KO, KOPN, KRMN, KTB, KTF, LABT, LAKE, LBTYB, LEA, LEGO, LFVN, LGI, LHX, LIQT, LMND, LNTH, LOAR, LPCN, LQDA, LUCD, LWLG, LXFR, MAAS, MAMO, MASK, MBLY, MBOT, MCD, MCRP, MDIA, MEGI, MF, MGNI, MHF, MINE, MIR, MKTX, MLKN, MLTX, MNOV, MOD, MORN, MPC, MRKR, MSCI, MSFT, MSLE, MTB, MTG, MUC, MXC, MYGN, NAMS, NAT, NBHC, NCL, NCNO, NEM, NEON, NFGC, NFLX, NHTC, NIXX, NKTX, NMS, NOA, NOMA, NRDY, NSC, NSIT, NTR, NTRB, NUWE, NVAX, NVDA, NVS, NXGL, NXPL, NZF, OC, OFLX, OGN, OIO, OMH, ONB, OPEN, OPY, ORA, ORCL, ORN, OSIS, OVBC, OVID, PACK, PASG, PAVS, PBYI, PCT, PDX, PEG, PEP, PESI, PFE, PGC, PHIN, PHVS, PL, PLBY, PLSM, PLUR, POAS, POCI, PPT, PRDO, PROV, PRVA, PSHG, PSTL, PTRN, PUSA, PXED, QCOM, QLYS, QTTB, RAIL, RAIN, RBB, RCON, RDNT, REFR, RETO, RFM, RIME, RIOT, RKT, RMCO, RMI, RMTI, ROC, RPGL, RSF, RSKD, RVSN, RY, SA, SAN, SBCF, SBRA, SBUX, SCYX, SEAT, SENS, SEZL, SFNC, SGHC, SHBI, SHLS, SII, SITC, SKYA, SLF, SLGB, SMA, SMP, SNA, SNAL, SOAR, SON, SPCB, SPMC, SPSC, SPWH, SRFM, SRXH, SSTI, STGW, STKE, STRR, SUIG, SVCO, SVM, SYM, TAOP, TAOX, TBCH, TDAY, TDUP, TDW, TEI, TFC, TGHL, THRY, TILE, TLS, TMDE, TNMG, TPB, TRDA, TRN, TRON, TSLA, TSSI, TTEK, TVE, TWG, TWLO, TXN, TYRA, UEC, UMH, UP, URGN, USIO, UVSP, VBNK, VCEL, VFS, VICI, VIVS, VKI, VOXR, VPG, VRSN, VRXA, VSTD, VTGN, VVR, VZ, WAL, WBX, WEA, WEYS, WGS, WIX, WLTH, WMS, WMT, WRBY, WSHP, WTTR, WYY, XEL, XFOR, XPEL, XTNT, YIBO, YORW, ZBRA, ZKIN.

Research only. Live frozen. No strategy cards.

## Remaining A–JL families (unmined sweep)

The 8-def / 16-fill list above is the **old** near-miss (N=499, PASS 0). Leftover P–JL / formula / fill families are scored in `UNMINED_SWEEP.md` (N=3603, KEEP 126 raw / 66 unique).
