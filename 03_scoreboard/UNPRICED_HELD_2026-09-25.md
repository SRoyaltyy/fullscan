# Unpriced held on 2026-09-25

Informational only. This note does not restate 2026-09-25 or any earlier locked day. The factor-mine ledger, the 09-25 snapshot, the price pin, and every state file stay as they were landed in `77db279`.

## Cause

The 09-25 snapshot is right to list `dropped_missing_bars` as HYAC-U and TBCVU. Yahoo printed a regular-session bar for all 13 `unpriced_held` names. The flag came from the local store, which had not been asked for them.

`ensure_candidate_bars` asked Yahoo only for that morning's ranking universe. A name a book already held, and a name that showed up only on a ticket, was never in that request. The price store's last print for all 13 is 2026-09-08 (AIBZ, DCX, DGII, DYOR, GRC, IOSP, IRMD, KMTS, UPXI, WOR, WTS) or 2026-09-11 (ARQQ, FJET). `unpriced_held_tickers` then treated "history in the store, no bar for this session" as a missing Yahoo bar. The classifier never asked Yahoo.

Yahoo does have a 2026-09-25 regular-session bar for every one of the 13 (`auto_adjust=False`, the same tape the store locks). WTS opened at 355.20, WOR at 60.00, DGII at 77.46.

## Who carried each name

Positions below are the primary book at the 2026-09-24 close, still open on the locked 09-25 book. Thirty-six sleeves. No combo book held any of the 13.

- **AIBZ** (5): `union_break10_h3`, `union_candle_h3`, `union_macd_xup_h3`, `union_ret_5_h3`, `union_vol_green_h3`
- **ARQQ** (7): `short_extended_h3`, `union_break10_h3`, `union_cond_h3`, `union_macd_up_h3`, `union_macd_xup_h3`, `union_ret_5_h3`, `union_w_hot_cond_h3`
- **DCX** (1): `union_vol_g_h5`
- **FJET** (24): `probable_h3`, `probable_h5`, `short_alarm_h3`, `short_macd_dn_h3`, `union_h3`, `union_h3_cut`, `union_h3_half`, `union_h3_rankw`, `union_h3_sboost`, `union_h3_sizeup`, `union_h3_time`, `union_h3_topheavy`, `union_h3_trail`, `union_h5`, `union_h5_cut`, `union_h5_half`, `union_h5_rankw`, `union_h5_sboost`, `union_h5_sizeup`, `union_h5_time`, `union_h5_topheavy`, `union_h5_trail`, `yday_gainer_h3`, `yday_gainer_h5`
- **UPXI** (3): `union_cond_h3`, `union_cond_n4_h3`, `union_last_red_h3`

These eight were not a position in any factor-mine recipe, in HOT4, in holdup, or in an OOS-0914 book: DGII, DYOR, GRC, IOSP, IRMD, KMTS, WOR, WTS.

They were flagged because `morning_pick_tickers` read `data/factor_mine/strategy_tickets.json`, which is dated 2026-09-25 and was generated at 16:49 ET, after the close. That file's excel buys include:

| name | ticket strategies |
| --- | --- |
| DGII, GRC, IOSP, IRMD, KMTS, WOR | `excel_all`, `excel_L3_long_green_hold2_midcap` |
| DYOR, WTS | `excel_all` (twice), `excel_L1_long_green_tp8_lowvol`, `excel_L2_long_green_tp3_lowvol` |

The send-time file `data/day_board/2026-09-25_strategy_tickets.json` (generated 10:15 ET) does not contain any of the 13. Neither does the 09-24 ticket file. None of the 13 are rows in the 09-25 factor-mine snapshot, so no sleeve could newly buy them that day.

## Yahoo bars used for the counterfactual

Downloaded in memory for 2026-09-25 only. Nothing was written to `data/prices/ohlc.parquet`.

| ticker | open | high | low | close | in a book |
| --- | ---: | ---: | ---: | ---: | --- |
| AIBZ | 4.35 | 4.35 | 3.98 | 4.09 | yes |
| ARQQ | 23.1 | 24.99 | 22.4901 | 24.6 | yes |
| DCX | 0.0502 | 0.0524 | 0.0456 | 0.0494 | yes |
| DGII | 77.46 | 78.53 | 77.12 | 78.3 | no |
| DYOR | 10.13 | 10.13 | 10.13 | 10.13 | no |
| FJET | 1.79 | 1.85 | 1.75 | 1.82 | yes |
| GRC | 75.04 | 75.37 | 74.09 | 74.21 | no |
| IOSP | 97.1 | 98.77 | 96.91 | 98.72 | no |
| IRMD | 85.3 | 86.24 | 84.77 | 85.76 | no |
| KMTS | 26.02 | 27.68 | 25.62 | 27.14 | no |
| UPXI | 1.23 | 1.25 | 1.16 | 1.23 | yes |
| WOR | 60.0 | 62.43 | 59.3098 | 62.24 | no |
| WTS | 355.2 | 360.36 | 355.66 | 357.79 | no |

## P&L if those bars had been on the tape

Each row is one primary sleeve, resumed from its 2026-09-24 state and walked through 09-25 on the locked pin, with the Yahoo bar above added for the carried name. A replay on the locked pin alone matched the locked 09-25 equity on all 36 sleeves. Buys are unchanged except where noted. Session P&L is equity minus yesterday's equity.

The eight names with no position change no sleeve. Their P&L impact is zero.

Where the minimum hold was already met (the h3 books, and DCX on `union_vol_g_h5`), the name was not on the 09-25 list, so the counterfactual sells it at the Yahoo open. The locked book could not sell, and carried the prior close. Where the hold is 5 sessions (`FJET` on the h5 books and `probable_h5` / `yday_gainer_h5`), the lot stays and is marked at the Yahoo close instead of the stale close. `union_last_red_h3` also buys REGN with the cash the UPXI sale frees. No other buy list changes.

| recipe | carried | locked equity | priced equity | locked session | priced session | delta |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `probable_h3` | FJET | 8,671.01 | 8,666.25 | -48.94 | -53.70 | -4.76 |
| `probable_h5` | FJET | 9,275.04 | 9,275.06 | -0.02 | 0.00 | +0.02 |
| `short_alarm_h3` | FJET | 9,801.96 | 9,801.02 | -243.83 | -244.77 | -0.94 |
| `short_extended_h3` | ARQQ | 8,346.31 | 8,344.80 | -30.19 | -31.70 | -1.51 |
| `short_macd_dn_h3` | FJET | 10,407.50 | 10,406.62 | 232.11 | 231.23 | -0.88 |
| `union_break10_h3` | AIBZ, ARQQ | 10,681.20 | 10,676.71 | 67.56 | 63.07 | -4.49 |
| `union_candle_h3` | AIBZ | 8,293.38 | 8,292.68 | -72.92 | -73.62 | -0.70 |
| `union_cond_h3` | ARQQ, UPXI | 8,276.48 | 8,279.90 | 33.83 | 37.25 | +3.42 |
| `union_cond_n4_h3` | UPXI | 8,289.42 | 8,292.76 | -62.10 | -58.76 | +3.34 |
| `union_h3` | FJET | 8,057.62 | 8,057.04 | 26.05 | 25.47 | -0.58 |
| `union_h3_cut` | FJET | 8,057.62 | 8,057.04 | 26.05 | 25.47 | -0.58 |
| `union_h3_half` | FJET | 8,918.39 | 8,917.28 | 11.28 | 10.17 | -1.11 |
| `union_h3_rankw` | FJET | 8,017.43 | 8,017.23 | -79.43 | -79.63 | -0.20 |
| `union_h3_sboost` | FJET | 8,083.13 | 8,082.51 | 30.12 | 29.50 | -0.62 |
| `union_h3_sizeup` | FJET | 8,057.62 | 8,057.04 | 26.05 | 25.47 | -0.58 |
| `union_h3_time` | FJET | 8,057.62 | 8,057.04 | 26.05 | 25.47 | -0.58 |
| `union_h3_topheavy` | FJET | 8,061.62 | 8,061.17 | -43.63 | -44.08 | -0.45 |
| `union_h3_trail` | FJET | 8,057.62 | 8,057.04 | 26.05 | 25.47 | -0.58 |
| `union_h5` | FJET | 9,244.94 | 9,245.00 | -17.69 | -17.63 | +0.06 |
| `union_h5_cut` | FJET | 9,244.94 | 9,245.00 | -17.69 | -17.63 | +0.06 |
| `union_h5_half` | FJET | 9,340.12 | 9,341.10 | 9.04 | 10.02 | +0.98 |
| `union_h5_rankw` | FJET | 8,711.03 | 8,711.07 | -19.13 | -19.09 | +0.04 |
| `union_h5_sboost` | FJET | 9,101.12 | 9,101.20 | -19.66 | -19.58 | +0.08 |
| `union_h5_sizeup` | FJET | 9,244.94 | 9,245.00 | -17.69 | -17.63 | +0.06 |
| `union_h5_time` | FJET | 9,244.94 | 9,245.00 | -17.69 | -17.63 | +0.06 |
| `union_h5_topheavy` | FJET | 8,717.50 | 8,717.62 | -10.92 | -10.80 | +0.12 |
| `union_h5_trail` | FJET | 9,244.94 | 9,245.00 | -17.69 | -17.63 | +0.06 |
| `union_last_red_h3` | UPXI | 7,123.58 | 7,107.15 | -218.70 | -235.13 | -16.43 |
| `union_macd_up_h3` | ARQQ | 9,424.59 | 9,422.84 | 103.45 | 101.70 | -1.75 |
| `union_macd_xup_h3` | AIBZ, ARQQ | 9,416.55 | 9,415.13 | -5.31 | -6.73 | -1.42 |
| `union_ret_5_h3` | AIBZ, ARQQ | 10,800.82 | 10,804.97 | 1.12 | 5.27 | +4.15 |
| `union_vol_g_h5` | DCX | 8,761.52 | 8,761.30 | -40.31 | -40.53 | -0.22 |
| `union_vol_green_h3` | AIBZ | 9,293.94 | 9,291.22 | -95.31 | -98.03 | -2.72 |
| `union_w_hot_cond_h3` | ARQQ | 11,407.90 | 11,407.31 | -44.14 | -44.73 | -0.59 |
| `yday_gainer_h3` | FJET | 8,499.19 | 8,492.98 | -165.32 | -171.53 | -6.21 |
| `yday_gainer_h5` | FJET | 9,299.90 | 9,299.96 | -2.18 | -2.12 | +0.06 |

Sum of the 36 sleeve deltas: **-35.39**. These sleeves share names and are not one account, so that sum is not a fund P&L.

`union_last_red_h3` locked buys were OMER, NEOV, SFIX, LRMR, ACAD, SGMT, SMWB. With UPXI sold at 1.23, the same list plus REGN is bought. Equity moves from 7,123.58 to 7,107.15 (−16.43), the largest single sleeve gap.

## HOT4, holdup, and the four OOS-0914 rules

None of the 13 were held or traded by these books on 09-24 or 09-25.

| book | 09-25 holdings | any of the 13 |
| --- | --- | --- |
| HOT4 `union_hot_n4_h1` | GLND, VICR, TJGC, SECZ, USDE | no |
| holdup `union_hot_n4_holdup` | GLND, VICR, TJGC, SECZ, USDE, VKTX | no |
| `oos0914_break10_h1_sx` | flat on 09-24; no 09-25 state | no |
| `oos0914_break10_h2_sx` | FEAM, SVIA on 09-24; no 09-25 state | no |
| `oos0914_rvol_lg_h1_sx` | flat on 09-24; no 09-25 state | no |
| `oos0914_zero_candle_h2_sx` | A, DXCM, NICE, NTSK on 09-24; no 09-25 state | no |

OOS-0914 did not append 09-25. HOT4 and holdup have no 09-25 state file; their 09-25 result is the factor-mine ledger above. The false `unpriced_held` flag did not change their fills or their equity.

