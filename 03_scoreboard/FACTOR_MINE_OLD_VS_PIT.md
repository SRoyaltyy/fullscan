# Old nightly board vs the sequential rebuild

Old board is commit `0eab52983` (2026-09-24 21:44 UTC, the last `chore: factor strategy mine` before #335). Session percent is that commit's `daily` shard `mean`. Rebuild picks and session percent are `data/factor_mine/state/<recipe>/<date>.json`. A day is the same only when the buy list, the sell list, and the session percent match. Cumulative totals are ending equity versus $10,000. Frozen snapshots, ledgers, and state files were not rewritten.

## HOT4 `union_hot_n4_h1`

| date | old buys | old sells | old session % | rebuild buys | rebuild sells | rebuild session % | |
| --- | --- | --- | ---: | --- | --- | ---: | --- |
| 2026-08-13 | IREN, TNDM, TPG, INO |  | 3.4537 |  |  | 0.0000 | different |
| 2026-08-14 | QMCO, ARX, ZENA, AIRO | IREN, TNDM, TPG, INO | -2.6928 |  |  | 0.0000 | different |
| 2026-08-17 | XHG, CAPR, STDN, HTFL | QMCO, ARX, ZENA, AIRO | -2.1341 |  |  | 0.0000 | different |
| 2026-08-18 |  | XHG, STDN, HTFL | -1.5558 |  |  | 0.0000 | different |
| 2026-08-19 |  | CAPR | 0.3577 |  |  | 0.0000 | different |
| 2026-08-20 | MRNA, CYPH, ABCL, AZI |  | -1.7036 |  |  | 0.0000 | different |
| 2026-08-21 | XHG, CAPR | ABCL, AZI | 4.6218 |  |  | 0.0000 | different |
| 2026-08-24 |  | MRNA, CYPH, XHG, CAPR | 13.6238 |  |  | 0.0000 | different |
| 2026-08-25 | REAX, CYPH, XHG, ASST |  | 8.1490 |  |  | 0.0000 | different |
| 2026-08-26 | BYND, USDE, PURR | REAX, CYPH, ASST | -2.3313 |  |  | 0.0000 | different |
| 2026-08-27 | CAPR, MRNA, BZ | BYND, USDE, PURR | 3.3529 |  |  | 0.0000 | different |
| 2026-08-28 | BYND, ANF | XHG, BZ | -2.5734 | CAPR, MRNA, ANF, SNPS |  | -0.8708 | different |
| 2026-08-31 |  | CAPR, MRNA, ANF | -2.0434 |  | ANF, CAPR, SNPS | -0.1830 | different |
| 2026-09-01 |  | BYND | -0.4703 |  | MRNA | -0.0373 | different |
| 2026-09-02 |  |  | 0.0000 |  |  | 0.0000 | same |
| 2026-09-03 | GPRO, REAX, CNH, MMED |  | -5.5260 |  |  | 0.0000 | different |
| 2026-09-04 | ASST, USDE, DFDV | REAX, CNH, MMED | 6.7568 | GPRO, ASST, DFDV, HOOD |  | 6.0150 | different |
| 2026-09-08 |  | GPRO, ASST, USDE, DFDV | -3.6479 |  | ASST, DFDV, GPRO, HOOD | -2.7876 | different |
| 2026-09-09 |  |  | 0.0000 |  |  | 0.0000 | same |
| 2026-09-10 |  |  | 0.0000 |  |  | 0.0000 | same |
| 2026-09-11 | INDP, BNC, IRD, CMRC |  | 2.2204 | INDP, BNC, VISN, GPRO |  | -0.7636 | different |
| 2026-09-14 |  | IRD | 6.6000 |  | BNC, INDP, VISN | 0.3784 | different |
| 2026-09-15 |  | BNC, CMRC | 3.3449 |  | GPRO | -0.3189 | different |
| 2026-09-16 | HLP, SDGR, SSL |  | -0.1237 | INDP, HLP, SDGR, CAI |  | 1.0903 | different |
| 2026-09-17 | BBNX, FPS | SDGR, SSL | 4.2946 | BBNX, IQ | CAI, SDGR | 3.8633 | different |
| 2026-09-18 | SDGR, CYPH, TEM | HLP, BBNX, FPS | -0.2922 | SDGR, TEM, LVWR | BBNX, HLP, IQ | -2.6824 | different |
| 2026-09-21 | FEAM, TJGC, LVWR, SECZ | INDP, SDGR, CYPH, TEM | 6.3056 | FEAM, CYPH, TJGC | INDP, SDGR, TEM | -3.9250 | different |
| 2026-09-22 |  |  | -1.0246 | GRAL, NUAI, ARM, INDP | CYPH, FEAM, LVWR, TJGC | 7.0570 | different |
| 2026-09-23 | GLND, VKTX, SVIA | TJGC, LVWR, SECZ | -2.6639 | FEAM, GLND, VICR, VKTX | ARM, GRAL, INDP, NUAI | -0.5555 | different |
| 2026-09-24 |  | FEAM, VKTX, SVIA | 18.5852 |  | FEAM, VKTX | 18.1549 | different |

Days 2026-08-13 through 2026-09-24: 27 different, 3 same. Old cumulative 62.798% ($16,279.84). Rebuild cumulative 24.991% ($12,499.09).

## holdup `union_hot_n4_holdup`

| date | old buys | old sells | old session % | rebuild buys | rebuild sells | rebuild session % | |
| --- | --- | --- | ---: | --- | --- | ---: | --- |
| 2026-08-13 | IREN, TNDM, TPG, INO |  | 3.4537 |  |  | 0.0000 | different |
| 2026-08-14 |  |  | 4.1233 |  |  | 0.0000 | different |
| 2026-08-17 | XHG, CAPR, STDN, HTFL | IREN, TNDM, TPG, INO | -0.7143 |  |  | 0.0000 | different |
| 2026-08-18 |  |  | 0.8064 |  |  | 0.0000 | different |
| 2026-08-19 |  | XHG, CAPR, STDN, HTFL | 1.2029 |  |  | 0.0000 | different |
| 2026-08-20 | MRNA, CYPH, ABCL, AZI |  | -1.7158 |  |  | 0.0000 | different |
| 2026-08-21 |  |  | 6.7154 |  |  | 0.0000 | different |
| 2026-08-24 |  | MRNA, CYPH, ABCL, AZI | 7.0403 |  |  | 0.0000 | different |
| 2026-08-25 | REAX, CYPH, XHG, ASST |  | 8.1597 |  |  | 0.0000 | different |
| 2026-08-26 |  |  | -1.5537 |  |  | 0.0000 | different |
| 2026-08-27 | CAPR, MRNA, BZ | REAX, CYPH, ASST | 2.0162 |  |  | 0.0000 | different |
| 2026-08-28 | BYND, ANF | XHG, BZ | -2.5465 | CAPR, MRNA, ANF, SNPS |  | -0.8708 | different |
| 2026-08-31 |  | CAPR, MRNA | -2.8212 |  |  | 0.1850 | different |
| 2026-09-01 |  | BYND, ANF | -0.6756 |  | ANF, CAPR, MRNA, SNPS | 1.3467 | different |
| 2026-09-02 |  |  | 0.0000 |  |  | 0.0000 | same |
| 2026-09-03 | GPRO, REAX, CNH, MMED |  | -5.5230 |  |  | 0.0000 | different |
| 2026-09-04 | ASST, USDE, DFDV | REAX, CNH, MMED | 6.7564 | GPRO, ASST, DFDV, HOOD |  | 5.9969 | different |
| 2026-09-08 |  | GPRO | -2.5322 |  |  | -4.2915 | different |
| 2026-09-09 |  | ASST, USDE, DFDV | 2.1357 |  | ASST, DFDV, GPRO, HOOD | 1.2139 | different |
| 2026-09-10 |  |  | 0.0000 |  |  | 0.0000 | same |
| 2026-09-11 | INDP, BNC, IRD, CMRC |  | 2.2225 | INDP, BNC, VISN, GPRO |  | -0.7630 | different |
| 2026-09-14 |  |  | 6.4520 |  |  | 5.2707 | different |
| 2026-09-15 |  | BNC, IRD, CMRC | 3.1875 |  | BNC, GPRO, VISN | 3.3609 | different |
| 2026-09-16 | HLP, SDGR, SSL |  | -0.1310 | HLP, SDGR, CAI |  | 0.2169 | different |
| 2026-09-17 |  |  | 11.3688 | IQ |  | 13.8516 | different |
| 2026-09-18 | CYPH, TEM | HLP, SSL | -2.1318 | TEM, LVWR | CAI, HLP | -4.2802 | different |
| 2026-09-21 | FEAM, TJGC, LVWR, SECZ | INDP, SDGR | 0.3730 | FEAM, CYPH, TJGC | INDP, IQ, SDGR | -3.3334 | different |
| 2026-09-22 | CRML, NUAI | CYPH, TEM | -1.4218 | GRAL, NUAI, ARM, INDP | LVWR, TEM | 6.9827 | different |
| 2026-09-23 | GLND, VKTX, SVIA | TJGC, LVWR, SECZ, CRML, NUAI | -2.6077 | GLND, VICR, VKTX | ARM, CYPH, GRAL, INDP, NUAI, TJGC | 1.0524 | different |
| 2026-09-24 |  | FEAM | 22.2603 |  | FEAM | 19.4952 | different |

Days 2026-08-13 through 2026-09-24: 28 different, 2 same. Old cumulative 80.971% ($18,097.07). Rebuild cumulative 52.196% ($15,219.62).

