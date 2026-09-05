# Factor mine action — `ohlc_hot_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `ohlc_hot` · top 8 · rank `list` · baseline list, no extra gate

Cash book **+1.06%** ($10,106) · signal-only (no cash/fees) was -2.36%. Starts YES **10/17**. Fills 64 · skips 88 · realized $+47.67.

## Each session

| Date | S | Route | Hard-red | Cash | Stock | Equity | Bought | Sold | Skipped | Why |
|---|---:|---|---|---:|---:|---:|---|---|---|---|
| 2026-08-13 | +8.53 | io | no | $10,000.00 | $0.00 | $10,000.00 | — | — | — | flat cash |
| 2026-08-14 | +5.50 | io | no | $250.70 | $9,630.86 | $9,881.56 | ADUR, ANRO, LIFE, VOYG, LUNR, BETA, FORM, ENTG | — | — | BUY ADUR x75 @ 16.50; BUY ANRO x39 @ 31.77; BUY LIFE x35 @ 35.04; BUY VOYG x28 @ 44.49; BUY LUNR x65 @ 19.17; BUY BETA x49 @ 25.21; BUY FORM x9 @ 129.48; BUY ENTG x7 @ 162.45 |
| 2026-08-17 | +2.25 | io | no | $69.50 | $10,107.03 | $10,176.53 | OCC, ALM, LPTH, CLYM, BORR, IOVA | — | ADUR, ANRO, LIFE, VOYG, BETA, FORM, ENTG, AAOI | BUY OCC x1 @ 18.24; BUY ALM x2 @ 16.20; BUY LPTH x2 @ 14.94; BUY CLYM x2 @ 16.25; BUY BORR x7 @ 4.59; BUY IOVA x5 @ 6.84 |
| 2026-08-18 | -6.20 | hold | yes | $69.50 | $9,790.51 | $9,860.01 | — | — | ADUR, ANRO, LIFE, VOYG, LUNR, BETA, FORM, ENTG, OCC, ALM, LPTH, CLYM, BORR, ANGX, SMTC, MRVL, AAOI, FN, ELMT, STDN | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | hold | yes | $9,717.04 | $178.63 | $9,895.67 | — | ADUR, ANRO, LIFE, VOYG, LUNR, BETA, FORM, ENTG | OCC, ALM, LPTH, CLYM, BORR, IOVA, OBE, SENS, TRGP, REAX, OABI, ABCL, XNCR, PAYS | SELL ADUR (dropped from list after 3 sess (min 3)); SELL ANRO (dropped from list after 3 sess (min 3)); SELL LIFE (dropped from list after 3 sess (min 3)); SELL VOYG (dropped from list after 3 sess (min 3)); SELL LUNR (dropped from list after 3 sess (min 3)); SELL BETA (dropped from list after 3 sess (min 3)); SELL FORM (dropped from list after 3 sess (min 3)); SELL ENTG (dropped from list after 3 sess (min 3)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | mover | no | $31.30 | $9,913.06 | $9,944.36 | AEM, TWST, ABTC, HL, SBET, PPC, ABCL, SENS | OCC, ALM, LPTH, CLYM, BORR, IOVA | — | SELL OCC (dropped from list after 3 sess (min 3)); SELL ALM (dropped from list after 3 sess (min 3)); SELL LPTH (dropped from list after 3 sess (min 3)); SELL CLYM (dropped from list after 3 sess (min 3)); SELL BORR (dropped from list after 3 sess (min 3)); SELL IOVA (dropped from list after 3 sess (min 3)); BUY AEM x6 @ 204.45; BUY TWST x9 @ 136.84; BUY ABTC x146 @ 8.46; BUY HL x61 @ 20.25; BUY SBET x163 @ 7.55; BUY PPC x40 @ 30.65; BUY ABCL x104 @ 11.81; BUY SENS x138 @ 8.91 |
| 2026-08-21 | +3.25 | mover | no | $17.58 | $10,138.67 | $10,156.25 | ORBS, TRON, XHG | — | TWST, HL, SBET, PPC, ABCL, SENS, GRAL, MSTR, AUGO | BUY ORBS x6 @ 0.86; BUY TRON x2 @ 1.94; BUY XHG x1 @ 4.49 |
| 2026-08-24 | -5.17 | hold | yes | $17.58 | $10,117.07 | $10,134.65 | — | — | AEM, TWST, ABTC, HL, SBET, PPC, ABCL, SENS, ORBS, TRON, BKKT, GUTS, DEFT, HOOD, UEC, NIQ, OMER | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | io | no | $60.24 | $9,998.72 | $10,058.96 | DEFT, AMTX, NIQ, OMER, ERO, TRLV, FUTU | AEM, TWST, ABTC, HL, SBET, PPC, ABCL, SENS | ORBS, TRON | SELL AEM (dropped from list after 3 sess (min 3)); SELL TWST (dropped from list after 3 sess (min 3)); SELL ABTC (dropped from list after 3 sess (min 3)); SELL HL (dropped from list after 3 sess (min 3)); SELL SBET (dropped from list after 3 sess (min 3)); SELL PPC (dropped from list after 3 sess (min 3)); SELL ABCL (dropped from list after 3 sess (min 3)); SELL SENS (dropped from list after 3 sess (min 3)); BUY DEFT x2252 @ 0.64; BUY AMTX x775 @ 1.86; BUY NIQ x73 @ 19.56; BUY OMER x76 @ 18.75; BUY ERO x37 @ 38.00; BUY TRLV x130 @ 11.02; BUY FUTU x12 @ 118.02 |
| 2026-08-26 | +2.02 | io | no | $60.24 | $10,004.05 | $10,064.29 | — | — | ORBS, TRON, AMTX, HOOD | hold ORBS,TRON,XHG,DEFT,AMTX,NIQ,OMER,ERO,TRLV,FUTU |
| 2026-08-27 | — | io | no | $72.79 | $10,004.28 | $10,077.07 | — | ORBS, TRON, XHG | DEFT, AMTX, NIQ, OMER, ERO, TRLV, FUTU | SELL ORBS (dropped from list after 4 sess (min 3)); SELL TRON (dropped from list after 4 sess (min 3)); SELL XHG (dropped from list after 4 sess (min 3)) |
| 2026-08-28 | +0.75 | io | no | $18.88 | $10,153.04 | $10,171.92 | ZYME, XHG | AMTX | — | SELL AMTX (dropped from list after 3 sess (min 3)); BUY ZYME x25 @ 29.33; BUY XHG x186 @ 4.06 |
| 2026-08-31 | -5.85 | hold | yes | $18.88 | $10,076.34 | $10,095.22 | — | — | ZYME, CVI | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | hold | yes | $1,408.03 | $8,501.62 | $9,909.65 | — | NIQ | ZYME, HOOD, TXG | SELL NIQ (dropped from list after 5 sess (min 3)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | hold | yes | $2,138.94 | $7,810.08 | $9,949.02 | — | ZYME | CVI, HOOD | SELL ZYME (dropped from list after 3 sess (min 3)); hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | io | no | $6.20 | $9,856.69 | $9,862.89 | NVAX, NIQ | — | — | BUY NVAX x104 @ 10.27; BUY NIQ x57 @ 18.60 |
| 2026-09-04 | — | io | no | $6.20 | $10,099.46 | $10,105.66 | — | — | — | hold DEFT,OMER,ERO,TRLV,FUTU,XHG,NVAX,NIQ |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $8,760.28 | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANRO` | 39 | $31.77 | $2.11 | — | $7,519.15 | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+13.5 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LIFE` | 35 | $35.04 | $2.10 | — | $6,290.65 | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+16.7 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VOYG` | 28 | $44.49 | $2.07 | — | $5,042.86 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.6 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 65 | $19.17 | $2.19 | — | $3,794.62 | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+17.6 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETA` | 49 | $25.21 | $2.14 | — | $2,557.20 | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+15.5 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `FORM` | 9 | $129.48 | $2.02 | — | $1,389.86 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+14.3 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ENTG` | 7 | $162.45 | $2.01 | — | $250.70 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+14.8 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 1 | $18.24 | $0.19 | — | $232.27 | baseline list, no extra gate; list probable,ohlc_hot; ⚪; ret5=+9.5 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 2 | $16.20 | $0.33 | — | $199.54 | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 2 | $14.94 | $0.30 | — | $169.36 | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `CLYM` | 2 | $16.25 | $0.33 | — | $136.53 | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+16.6 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 7 | $4.59 | $0.34 | — | $104.06 | baseline list, no extra gate; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `IOVA` | 5 | $6.84 | $0.36 | — | $69.50 | baseline list, no extra gate; list ohlc_hot; ret5=+10.1 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `ADUR` | 75 | $15.65 | $2.24 | $-68.20 | $1,241.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANRO` | 39 | $35.00 | $2.13 | $+121.74 | $2,603.88 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LIFE` | 35 | $34.37 | $2.12 | $-27.66 | $3,804.72 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `VOYG` | 28 | $41.93 | $2.09 | $-75.85 | $4,976.66 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LUNR` | 65 | $18.98 | $2.21 | $-16.74 | $6,208.16 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `BETA` | 49 | $26.80 | $2.16 | $+73.62 | $7,519.20 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `FORM` | 9 | $126.03 | $2.04 | $-35.10 | $8,651.43 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ENTG` | 7 | $152.52 | $2.03 | $-73.55 | $9,717.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `OCC` | 1 | $14.10 | $0.16 | $-4.49 | $9,730.98 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ALM` | 2 | $15.81 | $0.34 | $-1.45 | $9,762.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `LPTH` | 2 | $13.09 | $0.29 | $-4.29 | $9,788.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `CLYM` | 2 | $17.16 | $0.37 | $+1.12 | $9,822.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `BORR` | 7 | $4.46 | $0.35 | $-1.61 | $9,852.97 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `IOVA` | 5 | $8.07 | $0.44 | $+5.35 | $9,892.88 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AEM` | 6 | $204.45 | $2.01 | — | $8,664.17 | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TWST` | 9 | $136.84 | $2.02 | — | $7,430.59 | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+13.7 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABTC` | 146 | $8.46 | $2.43 | — | $6,193.00 | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+14.0 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HL` | 61 | $20.25 | $2.17 | — | $4,955.58 | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+13.5 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SBET` | 163 | $7.55 | $2.48 | — | $3,722.45 | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+14.6 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `PPC` | 40 | $30.65 | $2.11 | — | $2,494.34 | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+16.5 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 104 | $11.81 | $2.30 | — | $1,263.28 | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+16.0 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 138 | $8.91 | $2.40 | — | $31.30 | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+14.9 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 6 | $0.86 | $0.07 | — | $26.04 | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TRON` | 2 | $1.94 | $0.04 | — | $22.12 | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+15.4 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 1 | $4.49 | $0.05 | — | $17.58 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+12.7 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `AEM` | 6 | $200.48 | $2.03 | $-27.86 | $1,218.43 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `TWST` | 9 | $141.51 | $2.04 | $+37.98 | $2,489.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABTC` | 146 | $9.00 | $2.46 | $+73.95 | $3,801.52 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `HL` | 61 | $20.48 | $2.19 | $+9.66 | $5,048.61 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SBET` | 163 | $8.16 | $2.52 | $+94.43 | $6,376.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `PPC` | 40 | $31.76 | $2.13 | $+40.16 | $7,644.44 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABCL` | 104 | $10.77 | $2.33 | $-113.31 | $8,762.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SENS` | 138 | $9.66 | $2.44 | $+98.66 | $10,092.84 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2252 | $0.64 | $21.17 | — | $8,630.39 | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AMTX` | 775 | $1.86 | $10.00 | — | $7,178.89 | baseline list, no extra gate; list yday_mover,ohlc_hot; ⚪; ret5=+16.9 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NIQ` | 73 | $19.56 | $2.21 | — | $5,748.80 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+7.6 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `OMER` | 76 | $18.75 | $2.22 | — | $4,321.58 | baseline list, no extra gate; list ohlc_hot; ret5=+12.1 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ERO` | 37 | $38.00 | $2.10 | — | $2,913.48 | baseline list, no extra gate; list ohlc_hot,mover_buy; ⚪; ret5=+16.6 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `TRLV` | 130 | $11.02 | $2.38 | — | $1,478.50 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.0 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FUTU` | 12 | $118.02 | $2.03 | — | $60.24 | baseline list, no extra gate; list ohlc_hot; ⚪; ret5=+17.5 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `ORBS` | 6 | $0.80 | $0.09 | $-0.54 | $64.95 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRON` | 2 | $2.08 | $0.07 | $+0.17 | $69.04 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 1 | $3.81 | $0.06 | $-0.79 | $72.79 | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `AMTX` | 775 | $1.87 | $10.14 | $-12.39 | $1,511.90 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 25 | $29.33 | $2.06 | — | $776.59 | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+14.1 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `XHG` | 186 | $4.06 | $2.55 | — | $18.88 | baseline list, no extra gate; list ohlc_hot; ret5=+16.1 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `NIQ` | 73 | $19.06 | $2.23 | $-40.94 | $1,408.03 | dropped from list after 5 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ZYME` | 25 | $29.32 | $2.08 | $-4.40 | $2,138.94 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 104 | $10.27 | $2.30 | — | $1,068.56 | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NIQ` | 57 | $18.60 | $2.16 | — | $6.20 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+7.6 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟡 ab🟢 heat🔴 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `ADUR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `VOYG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BETA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `FORM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ENTG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AAOI` | cash | leftover split 35.81 < 1 share @ 152.64 |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VOYG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LUNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BETA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `FORM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ENTG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OCC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ALM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `LPTH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `CLYM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BORR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SMTC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MRVL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AAOI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ELMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `STDN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OCC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ALM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `LPTH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `CLYM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `BORR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRGP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `OABI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `XNCR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PAYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `TWST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `PPC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SENS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `GRAL` | cash | leftover split 5.22 < 1 share @ 78.88 |
| 2026-08-21 | `MSTR` | cash | leftover split 5.22 < 1 share @ 119.69 |
| 2026-08-21 | `AUGO` | cash | leftover split 5.22 < 1 share @ 89.10 |
| 2026-08-24 | `AEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TWST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `PPC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SENS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `TRON` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GUTS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `UEC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NIQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `TRON` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `ORBS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `TRON` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `HOOD` | no_price | no 09:30 open |
| 2026-08-27 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NIQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `OMER` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ERO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `TRLV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FUTU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ZYME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ZYME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `HOOD` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TXG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `CVI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HOOD` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `DEFT` | 2252 | 2026-08-25 @ $0.64 | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6 |
| `OMER` | 76 | 2026-08-25 @ $18.75 | baseline list, no extra gate; list ohlc_hot; ret5=+12.1 |
| `ERO` | 37 | 2026-08-25 @ $38.00 | baseline list, no extra gate; list ohlc_hot,mover_buy; ⚪; ret5=+16.6 |
| `TRLV` | 130 | 2026-08-25 @ $11.02 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.0 |
| `FUTU` | 12 | 2026-08-25 @ $118.02 | baseline list, no extra gate; list ohlc_hot; ⚪; ret5=+17.5 |
| `XHG` | 186 | 2026-08-28 @ $4.06 | baseline list, no extra gate; list ohlc_hot; ret5=+16.1 |
| `NVAX` | 104 | 2026-09-03 @ $10.27 | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1 |
| `NIQ` | 57 | 2026-09-03 @ $18.60 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+7.6 |
