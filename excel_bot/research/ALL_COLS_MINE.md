# ALL_COLS_MINE — recovered THIN sample (research only)
_Recovered from cloud agent bc-42f796ec workspace after it finished without committing to #144._
_Live `flatten_robust` untouched. N≈25 tickers — **THIN** vs ≥50-ticker ship bar. Deeper cols treated as **close**-knowable until timing_test extends past A–O._

## Verdict
- PASS: **0**
- FAIL: **0**
- THIN: **~90** (honest ceiling this cycle)
- Sample: lean `--all-cols` ~25 tickers (~1.2 s/ticker per Excel pulse)

## Top THIN leads (close entry)

| verdict | def | clock | side | exit | n | avg net | t | win | tickers |
|---|---|---|---|---|---:|---:|---:|---:|---:|
| THIN | `IZ_eq1` | close | long | hold8 | 339 | +1.89% | 4.0 | 56% | 25 |
| THIN | `deeper_g5` | close | long | hold8 | 3078 | +0.50% | 3.6 | 50% | 25 |
| THIN | `IZ_eq1` | close | long | hold5 | 339 | +0.96% | 2.8 | 53% | 25 |
| THIN | `AD_ge1` | close | long | hold8 | 757 | +0.91% | 2.6 | 49% | 25 |
| THIN | `deeper_g5` | close | long | hold5 | 3151 | +0.24% | 2.3 | 49% | 25 |
| THIN | `IZ_eq1` | close | long | hold3 | 348 | +0.52% | 2.2 | 51% | 25 |
| THIN | `IZ_eq1` | close | long | hold2 | 348 | +0.35% | 1.8 | 55% | 25 |
| THIN | `AD_ge1` | close | long | hold5 | 766 | +0.41% | 1.6 | 49% | 25 |
| THIN | `O_ge1` | close | long | hold8 | 429 | +0.80% | 1.6 | 50% | 25 |
| THIN | `lag_ELge2_Agreen` | open | long | hold8 | 804 | +0.29% | 1.1 | 50% | 25 |
| THIN | `AD_ge1` | close | long | hold3 | 775 | +0.19% | 1.0 | 49% | 25 |
| THIN | `L_ge1` | close | long | hold8 | 679 | +0.39% | 1.0 | 45% | 25 |
| THIN | `JA_eq1` | close | long | hold8 | 352 | +0.51% | 1.0 | 49% | 25 |
| THIN | `O_ge1` | close | long | hold5 | 432 | +0.37% | 0.9 | 48% | 25 |
| THIN | `deeper_g5` | close | long | hold3 | 3196 | +0.07% | 0.9 | 49% | 25 |
| THIN | `JA_eq1` | close | long | hold2 | 363 | +0.18% | 0.8 | 50% | 25 |
| THIN | `EL_ge2` | close | long | hold8 | 920 | +0.20% | 0.8 | 48% | 25 |
| THIN | `O_ge1` | close | long | hold3 | 438 | +0.21% | 0.8 | 49% | 25 |
| THIN | `lag_Lge1_Agreen` | open | long | hold8 | 629 | +0.23% | 0.6 | 45% | 25 |
| THIN | `L_ge1` | close | long | hold5 | 689 | +0.14% | 0.5 | 47% | 25 |
| THIN | `O_ge1` | close | long | hold2 | 441 | +0.11% | 0.5 | 49% | 25 |
| THIN | `AD_ge1` | close | long | hold2 | 781 | +0.07% | 0.5 | 48% | 25 |
| THIN | `lag_ELge2_Agreen` | open | long | hold5 | 821 | +0.07% | 0.3 | 48% | 25 |
| THIN | `JA_eq1` | close | long | hold3 | 363 | +0.06% | 0.2 | 48% | 25 |
| THIN | `lag_Lge1_Agreen` | open | long | hold5 | 639 | +0.04% | 0.1 | 48% | 25 |
| THIN | `EL_ge2` | close | long | hold2 | 956 | +0.00% | 0.0 | 48% | 25 |
| THIN | `deeper_g5` | close | long | hold2 | 3220 | +0.00% | 0.0 | 48% | 25 |
| THIN | `JA_eq1` | close | long | hold5 | 356 | -0.01% | -0.0 | 47% | 25 |
| THIN | `lag_Lge1_Agreen` | open | long | hold3 | 642 | -0.01% | -0.0 | 49% | 25 |
| THIN | `EL_ge2` | close | long | hold5 | 937 | -0.01% | -0.1 | 47% | 25 |
| THIN | `L_ge1` | close | long | hold3 | 692 | -0.02% | -0.1 | 46% | 25 |
| THIN | `lag_ELge2_Agreen` | open | long | hold3 | 837 | -0.03% | -0.2 | 47% | 25 |
| THIN | `EL_ge2` | close | long | hold3 | 954 | -0.04% | -0.3 | 48% | 25 |
| THIN | `AD_ge1` | close | long | hold1 | 785 | -0.03% | -0.3 | 48% | 25 |
| THIN | `O_ge1` | close | long | hold1 | 443 | -0.05% | -0.3 | 49% | 25 |
| THIN | `lag_ELge2_Agreen` | open | long | hold2 | 839 | -0.04% | -0.4 | 46% | 25 |
| THIN | `IZ_eq1` | close | long | hold1 | 348 | -0.06% | -0.4 | 43% | 25 |
| THIN | `lag_Lge1_Agreen` | open | long | hold2 | 646 | -0.07% | -0.4 | 46% | 25 |
| THIN | `JA_eq1` | close | long | hold1 | 364 | -0.07% | -0.5 | 47% | 25 |
| THIN | `L_ge1` | close | long | hold2 | 696 | -0.09% | -0.5 | 45% | 25 |

## Notes
- Near-miss: `IZ_eq1` hold8 (+1.89% t≈4) late/holdout soft; `deeper_g5` hold8 (+0.50% t≈3.6) holdout soft; `AD_ge1`/`O_ge1` tape flips.
- Do **not** claim open entry on deeper L/O/EL/V/AD/JA/IZ without new timing_test.
- Next: full A–O `mine_clock` on rebuilt ~3603 for real PASS/FAIL; then scale surviving all-cols defs.
- Source PR: https://github.com/SRoyaltyy/fullscan/pull/144
