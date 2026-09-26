# REBUILD_MATCH

Each deterministic input is tested on its own. A session from 2026-09-09 on counts only when an Actions job log pushed the reference file at or before 13:30 UTC. `exact` means every tested day matches. `mismatch (k/n, first day)` is the number of tested days that differ and the earliest of them. AI text is not regenerated. A Finviz print is not a price-store formula.

Price features and the candidate lists have no pre-open file of their own. The number under Proven days is how many snapshot sessions were compared. A match there does not make the input rebuildable for days before 09-09. The snapshot commit is `5f13a4415ea0`, which is outside every harvested push range.

| Input | Proven days (09-09 on) | REBUILD_MATCH | Rebuildable for earlier days |
| --- | ---: | --- | --- |
| hot score | 0 pre-open; 12 snapshot | mismatch (11/12, first 2026-09-10) | no |
| returns (1d, 5d, 10d) | 0 pre-open; 12 snapshot | mismatch (11/12, first 2026-09-10) | no |
| rvol | 0 pre-open; 12 snapshot | mismatch (11/12, first 2026-09-10) | no |
| 10d breakout | 0 pre-open; 12 snapshot | mismatch (7/12, first 2026-09-11) | no |
| candles | 0 pre-open; 12 snapshot | mismatch (11/12, first 2026-09-10) | no |
| MACD | 0 pre-open; 12 snapshot | mismatch (11/12, first 2026-09-10) | no |
| RSI | 0 pre-open; 12 snapshot | not-rebuildable (Finviz snapshot) | no |
| prior-day gainers | 0 pre-open output; 12 with proven prior export | mismatch (8/12 name sets, first 2026-09-10; order-only 4, first 2026-09-11) | no |
| prior-day movers | 0 pre-open output; 12 with proven prior export | mismatch (7/12 name sets, first 2026-09-10; order-only 5, first 2026-09-11) | no |
| hot list | 0 pre-open output; 12 with proven prior export | mismatch (11/12 name sets, first 2026-09-10; order-only 2026-09-25) | no |
| overnight moves | 0 pre-open output; 12 with proven prior export | mismatch (1/12, first 2026-09-10) | no |
| probable continuation | 0 pre-open output; 12 with proven prior export | mismatch (7/12, first 2026-09-11) | no |
| earnings reaction | 0 pre-open output; 12 with proven prior export | not-rebuildable (Finviz snapshot) | no |
| A01_rsi_value | 10 | mismatch (9/10, first 2026-09-10) | no |
| A02_rsi_cross_30 | 10 | mismatch (9/10, first 2026-09-10) | no |
| A03_rsi_cross_50 | 10 | mismatch (9/10, first 2026-09-10) | no |
| A04_rsi_cross_70 | 10 | mismatch (9/10, first 2026-09-10) | no |
| A05_body_red_green_2day | 10 | mismatch (9/10, first 2026-09-10) | no |
| A06_volume_red_green_2day | 10 | mismatch (10/10, first 2026-09-09) | no |
| A07_rvol | 10 | mismatch (9/10, first 2026-09-10) | no |
| A08_bollinger_position | 10 | mismatch (9/10, first 2026-09-10) | no |
| A09_above_sma50 | 10 | mismatch (9/10, first 2026-09-10) | no |
| A10_sma20_50_80_stack | 10 | mismatch (9/10, first 2026-09-10) | no |
| A11_three_section_lows | 10 | mismatch (9/10, first 2026-09-10) | no |
| A12_green_body_vs_wick_2day | 10 | mismatch (9/10, first 2026-09-10) | no |
| A13_red_body_vs_wick_2day | 10 | mismatch (9/10, first 2026-09-10) | no |
| A15_tape_recovery_setup | 10 | mismatch (9/10, first 2026-09-10) | no |
| A14_profitable_oversold_setup | 0 | not-rebuildable (no proven pre-open copy) | no |
| peer RS | 13 | mismatch (2/13, first 2026-09-15) | no |
| VIX | 13 | mismatch (13/13, first 2026-09-09) | no |
| rates (DGS10) | 13 weather files | not-rebuildable (FRED print; live series timed out) | no |
| predict | 12 | not-rebuildable (AI) | no |
| actions | 1 | not-rebuildable (AI) | no |
| judge | 1 | not-rebuildable (AI) | no |
| digest | 1 | not-rebuildable (AI) | no |
| map_heat | 1 | not-rebuildable (AI) | no |
| catalyst | 1 | not-rebuildable (AI) | no |
| research | 1 | not-rebuildable (AI) | no |
| baseline | 1 | not-rebuildable (AI) | no |
| events | 1 | not-rebuildable (AI) | no |
| sector predict | 10 | not-rebuildable (AI) | no |
| Finviz export | 8 | not-rebuildable (Finviz snapshot) | no |

Panel RSI prefers the prior Finviz print. On the snapshot rows, stored `rsi` equals `fv_rsi` on 808 of 808 rows that have a Finviz RSI. Wilder RSI from the price store versus that stored field is `mismatch (12/12, first 2026-09-10)`.

The one snapshot day that matches today's hot score, returns, rvol, 10-day breakout, candles, and MACD on every row is 2026-09-25. The other 11 days differ. 2026-09-09 is the one checklist day that matches every A rule except A06.

Rates source: a live FRED DGS10 pull timed out, so the yield was not recomputed. The morning number sits on the log-proven `weather.json` (`dgs10_current`). VIX is the prior Yahoo close (bars dated before the session), compared with `vix_spot` and `vix_ratio` on that same weather file. Every one of the 13 days differs. On 2026-09-22 the file has spot 14.74 and the prior close is 14.87.

A14 is the backfill setup flag in `src/ab_backfill.py`. The proven checklist files have A01–A13 and A15, not A14.

Peer RS is `Performance (Week)` minus the median of the correlation peers, scored from the log-proven pre-open Finviz export against the log-proven pre-open `peer_rs.csv`. The checklist coverage rule does not reject this file here: the formula does not need a session date inside the body.

## Names the price-list functions select

### prior-day gainers

- 2026-09-10 mismatch. Rebuilt 25, snapshot 23. Only in rebuild: JMKE, SKHY. Only in snapshot: —.
- 2026-09-11 same names, different order. Rebuilt 25, snapshot 25.
- 2026-09-14 mismatch. Rebuilt 25, snapshot 24. Only in rebuild: USDE. Only in snapshot: —.
- 2026-09-15 same names, different order. Rebuilt 25, snapshot 25.
- 2026-09-16 mismatch. Rebuilt 25, snapshot 24. Only in rebuild: REF. Only in snapshot: —.
- 2026-09-17 same names, different order. Rebuilt 25, snapshot 25.
- 2026-09-18 mismatch. Rebuilt 25, snapshot 22. Only in rebuild: USDE, SWRD, SECZ. Only in snapshot: —.
- 2026-09-21 mismatch. Rebuilt 25, snapshot 24. Only in rebuild: SECZ. Only in snapshot: —.
- 2026-09-22 mismatch. Rebuilt 25, snapshot 24. Only in rebuild: SECZ. Only in snapshot: —.
- 2026-09-23 same names, different order. Rebuilt 25, snapshot 25.
- 2026-09-24 mismatch. Rebuilt 25, snapshot 23. Only in rebuild: SWRD, SECZ. Only in snapshot: —.
- 2026-09-25 mismatch. Rebuilt 25, snapshot 24. Only in rebuild: SWRD. Only in snapshot: —.

### prior-day movers

- 2026-09-10 mismatch. Rebuilt 20, snapshot 19. Only in rebuild: TRBG. Only in snapshot: —.
- 2026-09-11 same names, different order. Rebuilt 20, snapshot 20.
- 2026-09-14 mismatch. Rebuilt 20, snapshot 19. Only in rebuild: ADBT. Only in snapshot: —.
- 2026-09-15 same names, different order. Rebuilt 20, snapshot 20.
- 2026-09-16 mismatch. Rebuilt 20, snapshot 18. Only in rebuild: SWRD, USDE. Only in snapshot: —.
- 2026-09-17 same names, different order. Rebuilt 20, snapshot 20.
- 2026-09-18 mismatch. Rebuilt 20, snapshot 17. Only in rebuild: USDE, SWRD, SECZ. Only in snapshot: —.
- 2026-09-21 mismatch. Rebuilt 20, snapshot 19. Only in rebuild: SECZ. Only in snapshot: —.
- 2026-09-22 mismatch. Rebuilt 20, snapshot 18. Only in rebuild: SECZ, BRVE. Only in snapshot: —.
- 2026-09-23 same names, different order. Rebuilt 20, snapshot 20.
- 2026-09-24 same names, different order. Rebuilt 20, snapshot 20.
- 2026-09-25 mismatch. Rebuilt 20, snapshot 19. Only in rebuild: SWRD. Only in snapshot: —.

### hot list

- 2026-09-10 mismatch. Rebuilt 30, snapshot 30. Only in rebuild: HSLV, OPFI, QRVO, TXG, CRDL. Only in snapshot: VISN, INTC, AGRO, DHT, EQNR.
- 2026-09-11 mismatch. Rebuilt 30, snapshot 29. Only in rebuild: DBI, TJGC, SSL, INSW, TK. Only in snapshot: VISN, APH, HPK, NAT.
- 2026-09-14 mismatch. Rebuilt 30, snapshot 29. Only in rebuild: HPE, BE, TK, SFL, CVI, CRSR, VSH, SECZ. Only in snapshot: VERI, BW, APH, SIMO, NAT, KOS, TXG.
- 2026-09-15 mismatch. Rebuilt 30, snapshot 30. Only in rebuild: GPRO, TRX, CYPH, BLSH. Only in snapshot: RBRK, APH, NAT, BBY.
- 2026-09-16 mismatch. Rebuilt 30, snapshot 28. Only in rebuild: REF, GH, CRGY, YPF, KR, ATRC, MTCH. Only in snapshot: TENB, WAY, NAT, APH, QCOM.
- 2026-09-17 mismatch. Rebuilt 30, snapshot 29. Only in rebuild: FOSL, CON. Only in snapshot: APH.
- 2026-09-18 mismatch. Rebuilt 30, snapshot 29. Only in rebuild: CHPT, CVLT, BLLN. Only in snapshot: NAT, CERT.
- 2026-09-21 mismatch. Rebuilt 30, snapshot 30. Only in rebuild: ALVO, COHR, STLN. Only in snapshot: COIN, PRAA, ABSI.
- 2026-09-22 mismatch. Rebuilt 30, snapshot 30. Only in rebuild: NET. Only in snapshot: UPXI.
- 2026-09-23 mismatch. Rebuilt 30, snapshot 30. Only in rebuild: KLIC, ASPN, BLLN, TXG, HYLN, AOSL, VPG, TGB, TNGX, A. Only in snapshot: MAZE, SGRY, CTKB, OPTU, HUYA, NTSK, MANE, LUNR, SHLS, ENTG.
- 2026-09-24 mismatch. Rebuilt 30, snapshot 30. Only in rebuild: KEYS, IR, FFIV, AMAT, ZBRA, TRLV, FPS. Only in snapshot: CAI, SG, STX, AKAM, MDB, OLLI, NTSK.
- 2026-09-25 same names, different order. Rebuilt 30, snapshot 30.

### overnight moves

- 2026-09-10 mismatch. Rebuilt 8, snapshot 7. Only in rebuild: REF. Only in snapshot: —.
- 2026-09-11 exact. Rebuilt 0, snapshot 0. Only in rebuild: —. Only in snapshot: —.
- 2026-09-14 exact. Rebuilt 3, snapshot 3. Only in rebuild: —. Only in snapshot: —.
- 2026-09-15 exact. Rebuilt 1, snapshot 1. Only in rebuild: —. Only in snapshot: —.
- 2026-09-16 exact. Rebuilt 2, snapshot 2. Only in rebuild: —. Only in snapshot: —.
- 2026-09-17 exact. Rebuilt 0, snapshot 0. Only in rebuild: —. Only in snapshot: —.
- 2026-09-18 exact. Rebuilt 0, snapshot 0. Only in rebuild: —. Only in snapshot: —.
- 2026-09-21 exact. Rebuilt 3, snapshot 3. Only in rebuild: —. Only in snapshot: —.
- 2026-09-22 exact. Rebuilt 5, snapshot 5. Only in rebuild: —. Only in snapshot: —.
- 2026-09-23 exact. Rebuilt 6, snapshot 6. Only in rebuild: —. Only in snapshot: —.
- 2026-09-24 exact. Rebuilt 1, snapshot 1. Only in rebuild: —. Only in snapshot: —.
- 2026-09-25 exact. Rebuilt 0, snapshot 0. Only in rebuild: —. Only in snapshot: —.

### probable continuation

- 2026-09-10 exact. Rebuilt 8, snapshot 8. Only in rebuild: —. Only in snapshot: —.
- 2026-09-11 mismatch. Rebuilt 8, snapshot 8. Only in rebuild: AMTX, CLOV, TYRA. Only in snapshot: APPS, INTR, PAGS.
- 2026-09-14 mismatch. Rebuilt 8, snapshot 7. Only in rebuild: USDE, NTAP, ON. Only in snapshot: VICR, REAX.
- 2026-09-15 exact. Rebuilt 8, snapshot 8. Only in rebuild: —. Only in snapshot: —.
- 2026-09-16 mismatch. Rebuilt 8, snapshot 8. Only in rebuild: HQ. Only in snapshot: ADPT.
- 2026-09-17 mismatch. Rebuilt 8, snapshot 8. Only in rebuild: DVLT. Only in snapshot: AIB.
- 2026-09-18 mismatch. Rebuilt 8, snapshot 7. Only in rebuild: DCX, TLSA, SWRD. Only in snapshot: RARE, SHLS.
- 2026-09-21 mismatch. Rebuilt 8, snapshot 8. Only in rebuild: BKKT. Only in snapshot: ALMU.
- 2026-09-22 mismatch. Rebuilt 8, snapshot 8. Only in rebuild: ARQQ, AIBZ. Only in snapshot: NN, BTQ.
- 2026-09-23 exact. Rebuilt 8, snapshot 8. Only in rebuild: —. Only in snapshot: —.
- 2026-09-24 exact. Rebuilt 8, snapshot 8. Only in rebuild: —. Only in snapshot: —.
- 2026-09-25 exact. Rebuilt 8, snapshot 8. Only in rebuild: —. Only in snapshot: —.

### earnings reaction

- 2026-09-10 exact. Rebuilt 9, snapshot 9. Only in rebuild: —. Only in snapshot: —.
- 2026-09-11 mismatch. Rebuilt 8, snapshot 7. Only in rebuild: REF. Only in snapshot: —.
- 2026-09-14 exact. Rebuilt 2, snapshot 2. Only in rebuild: —. Only in snapshot: —.
- 2026-09-15 exact. Rebuilt 4, snapshot 4. Only in rebuild: —. Only in snapshot: —.
- 2026-09-16 exact. Rebuilt 1, snapshot 1. Only in rebuild: —. Only in snapshot: —.
- 2026-09-17 exact. Rebuilt 2, snapshot 2. Only in rebuild: —. Only in snapshot: —.
- 2026-09-18 exact. Rebuilt 0, snapshot 0. Only in rebuild: —. Only in snapshot: —.
- 2026-09-21 exact. Rebuilt 0, snapshot 0. Only in rebuild: —. Only in snapshot: —.
- 2026-09-22 exact. Rebuilt 4, snapshot 4. Only in rebuild: —. Only in snapshot: —.
- 2026-09-23 exact. Rebuilt 5, snapshot 5. Only in rebuild: —. Only in snapshot: —.
- 2026-09-24 exact. Rebuilt 6, snapshot 6. Only in rebuild: —. Only in snapshot: —.
- 2026-09-25 exact. Rebuilt 2, snapshot 2. Only in rebuild: —. Only in snapshot: —.

## AB rules, rows that differ

| Rule | Days tested | Days that differ | First differing day | Worst day (bad rows) |
| --- | ---: | ---: | --- | --- |
| A01_rsi_value | 10 | 9 | 2026-09-10 | 2026-09-24 422/2658 |
| A02_rsi_cross_30 | 10 | 9 | 2026-09-10 | 2026-09-22 91/2670 |
| A03_rsi_cross_50 | 10 | 9 | 2026-09-10 | 2026-09-10 651/2660 |
| A04_rsi_cross_70 | 10 | 9 | 2026-09-10 | 2026-09-11 62/2653 |
| A05_body_red_green_2day | 10 | 9 | 2026-09-10 | 2026-09-16 1349/2657 |
| A06_volume_red_green_2day | 10 | 10 | 2026-09-09 | 2026-09-16 1325/2657 |
| A07_rvol | 10 | 9 | 2026-09-10 | 2026-09-21 1753/2663 |
| A08_bollinger_position | 10 | 9 | 2026-09-10 | 2026-09-24 1181/2658 |
| A09_above_sma50 | 10 | 9 | 2026-09-10 | 2026-09-24 652/2658 |
| A10_sma20_50_80_stack | 10 | 9 | 2026-09-10 | 2026-09-24 897/2658 |
| A11_three_section_lows | 10 | 9 | 2026-09-10 | 2026-09-24 752/2658 |
| A12_green_body_vs_wick_2day | 10 | 9 | 2026-09-10 | 2026-09-17 1783/2653 |
| A13_red_body_vs_wick_2day | 10 | 9 | 2026-09-10 | 2026-09-16 1702/2657 |
| A15_tape_recovery_setup | 10 | 9 | 2026-09-10 | 2026-09-16 468/2657 |

## Panel names a price-only rebuild misses

No snapshot row has a source named news. A name is missed when none of its sources are prior-day gainers, prior-day movers, the hot list, overnight, overnight mega, or probable. The miss is the book list (`flatten`, `mover_buy`) or the Finviz earnings calendar (`earn_react`) with no price source beside it. Share is missed names / panel names. Early sessions with an empty snapshot have no names to miss.

| Day | Panel names | Missed | Share | Names (source) |
| --- | ---: | ---: | ---: | --- |
| 08-13 | 0 | 0 | — | — |
| 08-14 | 0 | 0 | — | — |
| 08-17 | 0 | 0 | — | — |
| 08-18 | 0 | 0 | — | — |
| 08-19 | 0 | 0 | — | — |
| 08-20 | 0 | 0 | — | — |
| 08-21 | 0 | 0 | — | — |
| 08-24 | 78 | 18 | 23% | RZLT (flatten), MOS (flatten), OCUL (flatten), INSP (flatten), CRMD (flatten), HCA (flatten), PDD (earn_react), XPEV (earn_react), ALM (mover_buy), CRSP (mover_buy), HMY (mover_buy), KGC (mover_buy), +6 more |
| 08-25 | 0 | 0 | — | — |
| 08-26 | 0 | 0 | — | — |
| 08-27 | 0 | 0 | — | — |
| 08-28 | 98 | 32 | 33% | RRC (flatten), CRK (flatten), MOS (flatten), ADSK (earn_react), AFRM (earn_react), BBAR (earn_react), CHA (earn_react), ESTC (earn_react), FINV (earn_react), FRO (earn_react), GAP (earn_react), HAFN (earn_react), +20 more |
| 08-31 | 72 | 15 | 21% | RES (flatten), PBF (flatten), NOV (flatten+mover_buy), WTTR (flatten), LX (earn_react), SAIC (earn_react), MPC (mover_buy), NCNO (mover_buy), DINO (mover_buy), HAL (mover_buy), ACIW (mover_buy), AVPT (mover_buy), +3 more |
| 09-01 | 0 | 0 | — | — |
| 09-02 | 0 | 0 | — | — |
| 09-03 | 0 | 0 | — | — |
| 09-04 | 77 | 22 | 29% | CABA (flatten+mover_buy), BHC (flatten+mover_buy), BMEA (flatten+mover_buy), VIR (flatten+mover_buy), ATRC (flatten+mover_buy), CRM (flatten+mover_buy), HRMY (flatten+mover_buy), AMBA (earn_react), ASAN (earn_react), DOCU (earn_react), GWRE (earn_react), IOT (earn_react), +10 more |
| 09-08 | 0 | 0 | — | — |
| 09-09 | 0 | 0 | — | — |
| 09-10 | 82 | 12 | 15% | UGP (flatten), CLB (flatten), OIS (flatten), AEO (earn_react), AVAV (earn_react), COO (earn_react), DBI (earn_react), M (earn_react), NAVN (earn_react), NB (earn_react), SHOE (earn_react), WLTH (earn_react) |
| 09-11 | 74 | 12 | 16% | AUPH (flatten), OVID (flatten), SANM (flatten), ORCL (flatten+earn_react), NVT (flatten), COHU (flatten), ADBE (earn_react), CPRT (earn_react), DSGX (earn_react), KR (earn_react), LPTH (earn_react), RH (earn_react) |
| 09-14 | 62 | 6 | 10% | CVE (flatten), DK (flatten), BG (flatten), NVT (flatten), ANAB (earn_react), BNC (earn_react) |
| 09-15 | 64 | 6 | 9% | ICLR (flatten), WAY (flatten), FPS (earn_react), HITI (earn_react), PLAY (earn_react), UROY (earn_react) |
| 09-16 | 65 | 5 | 8% | IQV (flatten), RDNT (flatten), AVAH (flatten), BLFS (flatten), TCOM (earn_react) |
| 09-17 | 60 | 3 | 5% | AMN (flatten), ALMU (earn_react), LEN (earn_react) |
| 09-18 | 55 | 3 | 5% | RBRK (flatten), ECO (flatten), FIVN (flatten) |
| 09-21 | 62 | 6 | 10% | A (flatten), HUM (flatten), DXCM (flatten), PGEN (flatten), IOVA (flatten), MGTX (flatten) |
| 09-22 | 72 | 10 | 14% | PACS (flatten), MKC (flatten), EL (flatten), USFD (flatten), DLO (flatten), TDC (flatten), ABVX (earn_react), ANAB (earn_react), MLKN (earn_react), THO (earn_react) |
| 09-23 | 76 | 12 | 16% | DXCM (flatten), A (flatten), HALO (flatten), ARQT (flatten), PGEN (flatten), ADMA (flatten), FTRE (flatten), CBRL (earn_react), CTAS (earn_react), GIS (earn_react), KBH (earn_react), PAYX (earn_react) |
| 09-24 | 74 | 12 | 16% | EOG (flatten), CVE (flatten), RRC (flatten), CHKP (flatten), S (flatten), BAH (flatten), BB (earn_react), DRI (earn_react), FUL (earn_react), NEOV (earn_react), SFIX (earn_react), SNX (earn_react) |
| 09-25 | 62 | 7 | 11% | REGN (flatten), HALO (flatten), OMER (flatten), BLFS (flatten), MRVI (flatten), COST (earn_react), RZLT (earn_react) |

