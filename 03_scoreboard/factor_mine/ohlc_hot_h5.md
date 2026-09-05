# Factor mine action — `ohlc_hot_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `ohlc_hot` · top 8 · rank `list` · baseline list, no extra gate

Cash book **-8.27%** ($9,174) · signal-only (no cash/fees) was +86.73%. Starts YES **8/17**. Fills 58 · skips 137 · realized $-107.32.

## Each session

| Date | S | Route | Hard-red | Cash | Stock | Equity | Bought | Sold | Skipped | Why |
|---|---:|---|---|---:|---:|---:|---|---|---|---|
| 2026-08-13 | +8.53 | io | no | $10,000.00 | $0.00 | $10,000.00 | — | — | — | flat cash |
| 2026-08-14 | +5.50 | io | no | $250.70 | $9,630.86 | $9,881.56 | ADUR, ANRO, LIFE, VOYG, LUNR, BETA, FORM, ENTG | — | — | BUY ADUR x75 @ 16.50; BUY ANRO x39 @ 31.77; BUY LIFE x35 @ 35.04; BUY VOYG x28 @ 44.49; BUY LUNR x65 @ 19.17; BUY BETA x49 @ 25.21; BUY FORM x9 @ 129.48; BUY ENTG x7 @ 162.45 |
| 2026-08-17 | +2.25 | io | no | $69.50 | $10,107.03 | $10,176.53 | OCC, ALM, LPTH, CLYM, BORR, IOVA | — | ADUR, ANRO, LIFE, VOYG, BETA, FORM, ENTG, AAOI | BUY OCC x1 @ 18.24; BUY ALM x2 @ 16.20; BUY LPTH x2 @ 14.94; BUY CLYM x2 @ 16.25; BUY BORR x7 @ 4.59; BUY IOVA x5 @ 6.84 |
| 2026-08-18 | -6.20 | hold | yes | $69.50 | $9,790.51 | $9,860.01 | — | — | ADUR, ANRO, LIFE, VOYG, LUNR, BETA, FORM, ENTG, OCC, ALM, LPTH, CLYM, BORR, ANGX, SMTC, MRVL, AAOI, FN, ELMT, STDN | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | hold | yes | $69.50 | $9,473.05 | $9,542.55 | — | — | ADUR, ANRO, LIFE, VOYG, LUNR, BETA, FORM, ENTG, OCC, ALM, LPTH, CLYM, BORR, IOVA, OBE, SENS, TRGP, REAX, OABI, ABCL, XNCR, PAYS | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | mover | no | $53.32 | $9,428.83 | $9,482.15 | ABTC, SBET | — | ADUR, ANRO, LIFE, VOYG, LUNR, BETA, FORM, ENTG, OCC, ALM, LPTH, CLYM, BORR, IOVA, AEM, TWST, HL, PPC, ABCL, SENS | BUY ABTC x1 @ 8.46; BUY SBET x1 @ 7.55 |
| 2026-08-21 | +3.25 | mover | no | $44.25 | $9,549.28 | $9,593.53 | AEM, ORBS, GRAL, MSTR, TRON, XHG, AUGO | ADUR, ANRO, LIFE, VOYG, LUNR, BETA, FORM, ENTG | OCC, ALM, LPTH, CLYM, BORR, IOVA, SBET | SELL ADUR (dropped from list after 5 sess (min 5)); SELL ANRO (dropped from list after 5 sess (min 5)); SELL LIFE (dropped from list after 5 sess (min 5)); SELL VOYG (dropped from list after 5 sess (min 5)); SELL LUNR (dropped from list after 5 sess (min 5)); SELL BETA (dropped from list after 5 sess (min 5)); SELL FORM (dropped from list after 5 sess (min 5)); SELL ENTG (dropped from list after 5 sess (min 5)); BUY AEM x6 @ 216.30; BUY ORBS x1554 @ 0.86; BUY GRAL x17 @ 78.88; BUY MSTR x11 @ 119.69; BUY TRON x692 @ 1.94; BUY XHG x299 @ 4.49; BUY AUGO x15 @ 89.10 |
| 2026-08-24 | -5.17 | hold | yes | $227.20 | $9,260.96 | $9,488.16 | — | OCC, ALM, LPTH, CLYM, BORR, IOVA | ABTC, SBET, AEM, ORBS, GRAL, MSTR, TRON, AUGO, BKKT, GUTS, DEFT, HOOD, UEC, NIQ, OMER | SELL OCC (dropped from list after 5 sess (min 5)); SELL ALM (dropped from list after 5 sess (min 5)); SELL LPTH (dropped from list after 5 sess (min 5)); SELL CLYM (dropped from list after 5 sess (min 5)); SELL BORR (dropped from list after 5 sess (min 5)); SELL IOVA (dropped from list after 5 sess (min 5)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | io | no | $101.77 | $9,302.73 | $9,404.50 | DEFT, AMTX, NIQ, OMER, TRLV | — | ABTC, SBET, AEM, ORBS, GRAL, MSTR, TRON, AUGO, ERO, FUTU | BUY DEFT x50 @ 0.64; BUY AMTX x17 @ 1.86; BUY NIQ x1 @ 19.56; BUY OMER x1 @ 18.75; BUY TRLV x2 @ 11.02 |
| 2026-08-26 | +2.02 | io | no | $101.77 | $9,459.48 | $9,561.25 | — | — | ABTC, SBET, AEM, ORBS, GRAL, MSTR, TRON, AUGO, AMTX, ERO, FUTU, HOOD | hold ABTC,SBET,AEM,ORBS,GRAL,MSTR,TRON,XHG,AUGO,DEFT,AMTX,NIQ,OMER,TRLV |
| 2026-08-27 | — | io | no | $118.56 | $9,398.08 | $9,516.64 | — | ABTC, SBET | AEM, ORBS, GRAL, MSTR, TRON, XHG, AUGO, DEFT, AMTX, NIQ, OMER, TRLV | SELL ABTC (dropped from list after 5 sess (min 5)); SELL SBET (dropped from list after 5 sess (min 5)) |
| 2026-08-28 | +0.75 | io | no | $73.81 | $9,362.17 | $9,435.98 | ZYME, ERO, FUTU | AEM, ORBS, GRAL, MSTR, TRON, AUGO | AMTX | SELL AEM (dropped from list after 5 sess (min 5)); SELL ORBS (dropped from list after 5 sess (min 5)); SELL GRAL (dropped from list after 5 sess (min 5)); SELL MSTR (dropped from list after 5 sess (min 5)); SELL TRON (dropped from list after 5 sess (min 5)); SELL AUGO (dropped from list after 5 sess (min 5)); BUY ZYME x93 @ 29.33; BUY ERO x70 @ 39.20; BUY FUTU x21 @ 128.00 |
| 2026-08-31 | -5.85 | hold | yes | $73.81 | $9,082.63 | $9,156.44 | — | — | AMTX, ZYME, CVI | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | hold | yes | $124.06 | $8,885.51 | $9,009.57 | — | AMTX, NIQ | ZYME, HOOD, TXG | SELL AMTX (dropped from list after 5 sess (min 5)); SELL NIQ (dropped from list after 5 sess (min 5)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | hold | yes | $124.06 | $8,826.31 | $8,950.37 | — | — | ZYME, CVI, HOOD | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | io | no | $5.43 | $8,984.92 | $8,990.35 | NVAX, NIQ | — | ZYME | BUY NVAX x6 @ 10.27; BUY NIQ x3 @ 18.60 |
| 2026-09-04 | — | io | no | $2,917.75 | $6,255.77 | $9,173.52 | — | ZYME | — | SELL ZYME (dropped from list after 5 sess (min 5)) |

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
| 2026-08-20 09:30 ET | **BUY** | `ABTC` | 1 | $8.46 | $0.09 | — | $60.95 | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+14.0 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SBET` | 1 | $7.55 | $0.08 | — | $53.32 | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+14.6 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ADUR` | 75 | $16.00 | $2.24 | $-41.95 | $1,251.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ANRO` | 39 | $34.44 | $2.13 | $+99.90 | $2,592.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LIFE` | 35 | $33.90 | $2.12 | $-44.11 | $3,776.50 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `VOYG` | 28 | $38.84 | $2.09 | $-162.37 | $4,861.93 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LUNR` | 65 | $18.74 | $2.21 | $-32.34 | $6,077.82 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BETA` | 49 | $25.56 | $2.16 | $+12.86 | $7,328.11 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `FORM` | 9 | $117.69 | $2.04 | $-110.16 | $8,385.28 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ENTG` | 7 | $145.64 | $2.03 | $-121.71 | $9,402.73 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $8,102.92 | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1554 | $0.86 | $18.09 | — | $6,742.17 | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 17 | $78.88 | $2.04 | — | $5,399.17 | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MSTR` | 11 | $119.69 | $2.02 | — | $4,080.56 | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+15.7 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TRON` | 692 | $1.94 | $8.93 | — | $2,729.15 | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+15.4 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 299 | $4.49 | $3.86 | — | $1,382.79 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+12.7 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUGO` | 15 | $89.10 | $2.04 | — | $44.25 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.8 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `OCC` | 1 | $13.60 | $0.16 | $-4.98 | $57.69 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `ALM` | 2 | $18.69 | $0.40 | $+4.25 | $94.67 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `LPTH` | 2 | $13.92 | $0.30 | $-2.65 | $122.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `CLYM` | 2 | $17.27 | $0.37 | $+1.34 | $156.38 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `BORR` | 7 | $4.48 | $0.35 | $-1.47 | $187.38 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 5 | $8.05 | $0.44 | $+5.26 | $227.20 | dropped from list after 5 sess (min 5) | — |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 50 | $0.64 | $0.47 | — | $194.73 | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AMTX` | 17 | $1.86 | $0.37 | — | $162.74 | baseline list, no extra gate; list yday_mover,ohlc_hot; ⚪; ret5=+16.9 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NIQ` | 1 | $19.56 | $0.20 | — | $142.98 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+7.6 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `OMER` | 1 | $18.75 | $0.19 | — | $124.04 | baseline list, no extra gate; list ohlc_hot; ret5=+12.1 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `TRLV` | 2 | $11.02 | $0.23 | — | $101.77 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.0 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `ABTC` | 1 | $8.84 | $0.11 | $+0.18 | $110.50 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `SBET` | 1 | $8.16 | $0.10 | $+0.43 | $118.56 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AEM` | 6 | $214.11 | $2.03 | $-17.18 | $1,401.19 | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ORBS` | 1554 | $0.82 | $17.67 | $-104.14 | $2,657.80 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRAL` | 17 | $79.00 | $2.06 | $-2.06 | $3,998.73 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `MSTR` | 11 | $126.77 | $2.04 | $+73.81 | $5,391.16 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `TRON` | 692 | $2.21 | $9.05 | $+168.86 | $6,911.43 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUGO` | 15 | $88.71 | $2.06 | $-9.94 | $8,240.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 93 | $29.33 | $2.27 | — | $5,510.06 | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+14.1 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERO` | 70 | $39.20 | $2.20 | — | $2,763.86 | baseline list, no extra gate; list ohlc_hot; ret5=+16.6 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `FUTU` | 21 | $128.00 | $2.05 | — | $73.81 | baseline list, no extra gate; list ohlc_hot; ret5=+17.5 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `AMTX` | 17 | $1.87 | $0.39 | $-0.59 | $105.21 | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `NIQ` | 1 | $19.06 | $0.21 | $-0.91 | $124.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 6 | $10.27 | $0.63 | — | $61.80 | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NIQ` | 3 | $18.60 | $0.57 | — | $5.43 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+7.6 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `ZYME` | 93 | $31.34 | $2.31 | $+182.35 | $2,917.75 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `ADUR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ANRO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `LIFE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `VOYG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BETA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `FORM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ENTG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `AAOI` | cash | leftover split 35.81 < 1 share @ 152.64 |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ANRO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `LIFE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `VOYG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `LUNR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BETA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `FORM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ENTG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `OCC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `ALM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `LPTH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `CLYM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `BORR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SMTC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MRVL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AAOI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ELMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `STDN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `ADUR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ANRO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `LIFE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `VOYG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `LUNR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BETA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `FORM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ENTG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `OCC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `ALM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `LPTH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `CLYM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `BORR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `IOVA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRGP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `OABI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `XNCR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PAYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `ADUR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ANRO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `LIFE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `VOYG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `LUNR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BETA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `FORM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ENTG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `OCC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `ALM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `LPTH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `CLYM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `BORR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `IOVA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `AEM` | cash | leftover split 8.69 < 1 share @ 204.45 |
| 2026-08-20 | `TWST` | cash | leftover split 8.69 < 1 share @ 136.84 |
| 2026-08-20 | `HL` | cash | leftover split 8.69 < 1 share @ 20.25 |
| 2026-08-20 | `PPC` | cash | leftover split 8.69 < 1 share @ 30.65 |
| 2026-08-20 | `ABCL` | cash | leftover split 8.69 < 1 share @ 11.81 |
| 2026-08-20 | `SENS` | cash | leftover split 8.69 < 1 share @ 8.91 |
| 2026-08-21 | `OCC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `ALM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `LPTH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `CLYM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `BORR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `IOVA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `SBET` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ABTC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `SBET` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `AEM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `GRAL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `MSTR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `TRON` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AUGO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GUTS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `UEC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NIQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ABTC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `SBET` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `AEM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `GRAL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `MSTR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `TRON` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUGO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ERO` | cash | leftover split 32.46 < 1 share @ 38.00 |
| 2026-08-25 | `FUTU` | cash | leftover split 32.46 < 1 share @ 118.02 |
| 2026-08-26 | `ABTC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `SBET` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `AEM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ORBS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `GRAL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `MSTR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `TRON` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUGO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AMTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ERO` | no_price | no 09:30 open |
| 2026-08-26 | `FUTU` | no_price | no 09:30 open |
| 2026-08-26 | `HOOD` | no_price | no 09:30 open |
| 2026-08-27 | `AEM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ORBS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `GRAL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `MSTR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `TRON` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `XHG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUGO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `DEFT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `AMTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `NIQ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `OMER` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `TRLV` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `AMTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `AMTX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ZYME` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ZYME` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `HOOD` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TXG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `ZYME` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `CVI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HOOD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `ZYME` | min_hold | dropped but min-hold 4/5 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `XHG` | 299 | 2026-08-21 @ $4.49 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+12.7 |
| `DEFT` | 50 | 2026-08-25 @ $0.64 | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6 |
| `OMER` | 1 | 2026-08-25 @ $18.75 | baseline list, no extra gate; list ohlc_hot; ret5=+12.1 |
| `TRLV` | 2 | 2026-08-25 @ $11.02 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.0 |
| `ERO` | 70 | 2026-08-28 @ $39.20 | baseline list, no extra gate; list ohlc_hot; ret5=+16.6 |
| `FUTU` | 21 | 2026-08-28 @ $128.00 | baseline list, no extra gate; list ohlc_hot; ret5=+17.5 |
| `NVAX` | 6 | 2026-09-03 @ $10.27 | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1 |
| `NIQ` | 3 | 2026-09-03 @ $18.60 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+7.6 |
