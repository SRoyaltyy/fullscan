# Factor mine action — `ohlc_hot_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `ohlc_hot` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-5.07%** ($9,493) · signal-only (no cash/fees) was -3.30%. Starts YES **5/17**. Fills 84 · skips 28 · realized $-418.83.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `ohlc_hot` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $13.27.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-14 | +5.50 | $10,000.00 | — | ADUR, ANRO, LIFE, VOYG, LUNR, BETA, FORM, ENTG | — | $250.70 | $9,630.86 | $9,881.56 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7 | BUY ADUR x75 @ 16.50; BUY ANRO x39 @ 31.77; BUY LIFE x35 @ 35.04; BUY VOYG x28 @ 44.49; BUY LUNR x65 @ 19.17; BUY BETA x49 @ 25.21; BUY FORM x9 @ 129.48; BUY ENTG x7 @ 162.45 |
| 2026-08-17 | +2.25 | $250.70 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7 | OCC, ALM, LPTH, AAOI, CLYM, BORR, IOVA | ADUR, ANRO, LIFE, VOYG, BETA, FORM, ENTG | $17.77 | $9,931.86 | $9,949.63 | LUNR×65, OCC×67, ALM×75, LPTH×82, AAOI×8, CLYM×75, BORR×267, IOVA×179 | SELL ADUR (dropped from list after 1 sess (min 1)); SELL ANRO (dropped from list after 1 sess (min 1)); SELL LIFE (dropped from list after 1 sess (min 1)); SELL VOYG (dropped from list after 1 sess (min 1)); SELL BETA (dropped from list after 1 sess (min 1)); SELL FORM (dropped from list after 1 sess (min 1)); SELL ENTG (dropped from list after 1 sess (min 1)); BUY OCC x67 @ 18.24; BUY ALM x75 @ 16.20; BUY LPTH x82 @ 14.94; BUY AAOI x8 @ 152.64; BUY CLYM x75 @ 16.25; BUY BORR x267 @ 4.59; BUY IOVA x179 @ 6.84 |
| 2026-08-18 | -6.20 | $17.77 | LUNR×65, OCC×67, ALM×75, LPTH×82, AAOI×8, CLYM×75, BORR×267, IOVA×179 | — | LUNR, OCC, ALM, LPTH, CLYM, BORR | $7,161.01 | $2,309.65 | $9,470.66 | AAOI×8, IOVA×179 | SELL LUNR (dropped from list after 2 sess (min 1)); SELL OCC (dropped from list after 1 sess (min 1)); SELL ALM (dropped from list after 1 sess (min 1)); SELL LPTH (dropped from list after 1 sess (min 1)); SELL CLYM (dropped from list after 1 sess (min 1)); SELL BORR (dropped from list after 1 sess (min 1)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $7,161.01 | AAOI×8, IOVA×179 | — | AAOI, IOVA | $9,532.01 | $0.00 | $9,532.01 | — | SELL AAOI (dropped from list after 2 sess (min 1)); SELL IOVA (dropped from list after 2 sess (min 1)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $9,532.01 | — | AEM, TWST, ABTC, HL, SBET, PPC, ABCL, SENS | — | $321.72 | $9,253.01 | $9,574.73 | AEM×5, TWST×8, ABTC×140, HL×58, SBET×157, PPC×38, ABCL×100, SENS×133 | BUY AEM x5 @ 204.45; BUY TWST x8 @ 136.84; BUY ABTC x140 @ 8.46; BUY HL x58 @ 20.25; BUY SBET x157 @ 7.55; BUY PPC x38 @ 30.65; BUY ABCL x100 @ 11.81; BUY SENS x133 @ 8.91 |
| 2026-08-21 | +3.25 | $321.72 | AEM×5, TWST×8, ABTC×140, HL×58, SBET×157, PPC×38, ABCL×100, SENS×133 | ORBS, GRAL, MSTR, TRON, XHG, AUGO | TWST, HL, SBET, PPC, ABCL, SENS | $160.86 | $9,479.48 | $9,640.34 | AEM×5, ABTC×140, ORBS×1438, GRAL×15, MSTR×10, TRON×640, XHG×276, AUGO×13 | SELL TWST (dropped from list after 1 sess (min 1)); SELL HL (dropped from list after 1 sess (min 1)); SELL SBET (dropped from list after 1 sess (min 1)); SELL PPC (dropped from list after 1 sess (min 1)); SELL ABCL (dropped from list after 1 sess (min 1)); SELL SENS (dropped from list after 1 sess (min 1)); BUY ORBS x1438 @ 0.86; BUY GRAL x15 @ 78.88; BUY MSTR x10 @ 119.69; BUY TRON x640 @ 1.94; BUY XHG x276 @ 4.49; BUY AUGO x13 @ 89.10 |
| 2026-08-24 | -5.17 | $160.86 | AEM×5, ABTC×140, ORBS×1438, GRAL×15, MSTR×10, TRON×640, XHG×276, AUGO×13 | — | AEM, ABTC, ORBS, GRAL, MSTR, TRON, AUGO | $8,524.65 | $1,120.56 | $9,645.21 | XHG×276 | SELL AEM (dropped from list after 2 sess (min 1)); SELL ABTC (dropped from list after 2 sess (min 1)); SELL ORBS (dropped from list after 1 sess (min 1)); SELL GRAL (dropped from list after 1 sess (min 1)); SELL MSTR (dropped from list after 1 sess (min 1)); SELL TRON (dropped from list after 1 sess (min 1)); SELL AUGO (dropped from list after 1 sess (min 1)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $8,524.65 | XHG×276 | DEFT, AMTX, NIQ, OMER, ERO, TRLV, FUTU | — | $32.71 | $9,568.72 | $9,601.43 | XHG×276, DEFT×1902, AMTX×654, NIQ×62, OMER×64, ERO×32, TRLV×110, FUTU×10 | BUY DEFT x1902 @ 0.64; BUY AMTX x654 @ 1.86; BUY NIQ x62 @ 19.56; BUY OMER x64 @ 18.75; BUY ERO x32 @ 38.00; BUY TRLV x110 @ 11.02; BUY FUTU x10 @ 118.02 |
| 2026-08-26 | +2.02 | $32.71 | XHG×276, DEFT×1902, AMTX×654, NIQ×62, OMER×64, ERO×32, TRLV×110, FUTU×10 | — | — | $32.71 | $9,564.36 | $9,597.07 | XHG×276, DEFT×1902, AMTX×654, NIQ×62, OMER×64, ERO×32, TRLV×110, FUTU×10 | hold XHG,DEFT,AMTX,NIQ,OMER,ERO,TRLV,FUTU |
| 2026-08-27 | — | $32.71 | XHG×276, DEFT×1902, AMTX×654, NIQ×62, OMER×64, ERO×32, TRLV×110, FUTU×10 | — | XHG, DEFT, AMTX, NIQ, OMER, ERO, TRLV, FUTU | $9,615.16 | $0.00 | $9,615.16 | — | SELL XHG (dropped from list after 4 sess (min 1)); SELL DEFT (dropped from list after 2 sess (min 1)); SELL AMTX (dropped from list after 2 sess (min 1)); SELL NIQ (dropped from list after 2 sess (min 1)); SELL OMER (dropped from list after 2 sess (min 1)); SELL ERO (dropped from list after 2 sess (min 1)); SELL TRLV (dropped from list after 2 sess (min 1)); SELL FUTU (dropped from list after 2 sess (min 1)) |
| 2026-08-28 | +0.75 | $9,615.16 | — | ZYME, XHG, NIQ, DEFT, OMER, ERO, TRLV, FUTU | — | $111.41 | $9,513.69 | $9,625.10 | ZYME×40, XHG×296, NIQ×63, DEFT×2003, OMER×65, ERO×30, TRLV×105, FUTU×9 | BUY ZYME x40 @ 29.33; BUY XHG x296 @ 4.06; BUY NIQ x63 @ 18.79; BUY DEFT x2003 @ 0.60; BUY OMER x65 @ 18.24; BUY ERO x30 @ 39.20; BUY TRLV x105 @ 11.38; BUY FUTU x9 @ 128.00 |
| 2026-08-31 | -5.85 | $111.41 | ZYME×40, XHG×296, NIQ×63, DEFT×2003, OMER×65, ERO×30, TRLV×105, FUTU×9 | — | ZYME | $1,240.08 | $8,246.31 | $9,486.39 | XHG×296, NIQ×63, DEFT×2003, OMER×65, ERO×30, TRLV×105, FUTU×9 | SELL ZYME (dropped from list after 1 sess (min 1)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $1,240.08 | XHG×296, NIQ×63, DEFT×2003, OMER×65, ERO×30, TRLV×105, FUTU×9 | — | NIQ | $2,438.66 | $6,875.13 | $9,313.79 | XHG×296, DEFT×2003, OMER×65, ERO×30, TRLV×105, FUTU×9 | SELL NIQ (dropped from list after 2 sess (min 1)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $2,438.66 | XHG×296, DEFT×2003, OMER×65, ERO×30, TRLV×105, FUTU×9 | — | — | $2,438.66 | $6,930.51 | $9,369.17 | XHG×296, DEFT×2003, OMER×65, ERO×30, TRLV×105, FUTU×9 | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $2,438.66 | XHG×296, DEFT×2003, OMER×65, ERO×30, TRLV×105, FUTU×9 | NVAX, NIQ | — | $13.27 | $9,254.05 | $9,267.32 | XHG×296, DEFT×2003, OMER×65, ERO×30, TRLV×105, FUTU×9, NVAX×118, NIQ×65 | BUY NVAX x118 @ 10.27; BUY NIQ x65 @ 18.60 |
| 2026-09-04 | — | $13.27 | XHG×296, DEFT×2003, OMER×65, ERO×30, TRLV×105, FUTU×9, NVAX×118, NIQ×65 | — | — | $13.27 | $9,479.53 | $9,492.80 | XHG×296, DEFT×2003, OMER×65, ERO×30, TRLV×105, FUTU×9, NVAX×118, NIQ×65 | hold XHG,DEFT,OMER,ERO,TRLV,FUTU,NVAX,NIQ |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $8,760.28 | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANRO` | 39 | $31.77 | $2.11 | — | $7,519.15 | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+13.5; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LIFE` | 35 | $35.04 | $2.10 | — | $6,290.65 | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+16.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VOYG` | 28 | $44.49 | $2.07 | — | $5,042.86 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 65 | $19.17 | $2.19 | — | $3,794.62 | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETA` | 49 | $25.21 | $2.14 | — | $2,557.20 | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+15.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `FORM` | 9 | $129.48 | $2.02 | — | $1,389.86 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+14.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ENTG` | 7 | $162.45 | $2.01 | — | $250.70 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+14.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 75 | $15.73 | $2.24 | $-62.20 | $1,428.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANRO` | 39 | $32.15 | $2.13 | $+10.59 | $2,679.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LIFE` | 35 | $34.03 | $2.12 | $-39.56 | $3,868.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VOYG` | 28 | $42.12 | $2.09 | $-70.53 | $5,046.14 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `BETA` | 49 | $24.61 | $2.16 | $-33.69 | $6,249.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `FORM` | 9 | $134.05 | $2.04 | $+37.08 | $7,454.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ENTG` | 7 | $162.04 | $2.03 | $-6.91 | $8,586.53 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 67 | $18.24 | $2.19 | — | $7,362.26 | baseline list, no extra gate; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1226.65 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 75 | $16.20 | $2.21 | — | $6,145.04 | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1226.65 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 82 | $14.94 | $2.24 | — | $4,917.73 | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $1226.65 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `AAOI` | 8 | $152.64 | $2.01 | — | $3,694.59 | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+10.8; leftover $1226.65 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CLYM` | 75 | $16.25 | $2.21 | — | $2,473.63 | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+16.6; leftover $1226.65 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 267 | $4.59 | $3.44 | — | $1,244.66 | baseline list, no extra gate; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; leftover $1226.65 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `IOVA` | 179 | $6.84 | $2.53 | — | $17.77 | baseline list, no extra gate; list ohlc_hot; ret5=+10.1; leftover $1226.65 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `LUNR` | 65 | $19.31 | $2.21 | $+4.71 | $1,270.71 | dropped from list after 2 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 67 | $16.20 | $2.21 | $-141.08 | $2,353.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 75 | $15.78 | $2.24 | $-35.95 | $3,535.16 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 82 | $14.01 | $2.26 | $-80.76 | $4,681.72 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CLYM` | 75 | $16.90 | $2.24 | $+44.30 | $5,946.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `BORR` | 267 | $4.56 | $3.50 | $-14.95 | $7,161.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-19 09:30 ET | **SELL** | `AAOI` | 8 | $135.85 | $2.03 | $-138.37 | $8,245.77 | dropped from list after 2 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 peer🔴 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `IOVA` | 179 | $7.20 | $2.57 | $+59.35 | $9,532.01 | dropped from list after 2 sess (min 1) | join🔴 sector🟡 gen🔴 news🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AEM` | 5 | $204.45 | $2.00 | — | $8,507.75 | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $1191.50 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TWST` | 8 | $136.84 | $2.01 | — | $7,411.02 | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+13.7; leftover $1191.50 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABTC` | 140 | $8.46 | $2.41 | — | $6,224.21 | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+14.0; leftover $1191.50 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HL` | 58 | $20.25 | $2.16 | — | $5,047.54 | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+13.5; leftover $1191.50 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SBET` | 157 | $7.55 | $2.46 | — | $3,859.73 | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+14.6; leftover $1191.50 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `PPC` | 38 | $30.65 | $2.10 | — | $2,692.93 | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+16.5; leftover $1191.50 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 100 | $11.81 | $2.29 | — | $1,509.14 | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1191.50 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 133 | $8.91 | $2.39 | — | $321.72 | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1191.50 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `TWST` | 8 | $138.43 | $2.03 | $+8.67 | $1,427.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HL` | 58 | $21.33 | $2.18 | $+58.29 | $2,662.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `SBET` | 157 | $7.87 | $2.50 | $+45.28 | $3,895.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `PPC` | 38 | $31.13 | $2.12 | $+14.01 | $5,075.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 100 | $11.57 | $2.32 | $-29.11 | $6,230.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SENS` | 133 | $9.24 | $2.42 | $+39.08 | $7,457.17 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1438 | $0.86 | $16.74 | — | $6,198.00 | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1242.86 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 15 | $78.88 | $2.04 | — | $5,012.77 | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1242.86 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MSTR` | 10 | $119.69 | $2.02 | — | $3,813.85 | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+15.7; leftover $1242.86 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TRON` | 640 | $1.94 | $8.26 | — | $2,563.99 | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+15.4; leftover $1242.86 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 276 | $4.49 | $3.56 | — | $1,321.19 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+12.7; leftover $1242.86 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUGO` | 13 | $89.10 | $2.03 | — | $160.86 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.8; leftover $1242.86 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $+58.87 | $1,243.99 | dropped from list after 2 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 140 | $8.06 | $2.44 | $-60.85 | $2,369.94 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 1438 | $0.89 | $17.36 | $+3.29 | $3,632.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 15 | $81.87 | $2.06 | $+40.76 | $4,858.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MSTR` | 10 | $121.76 | $2.04 | $+16.64 | $6,073.96 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TRON` | 640 | $2.02 | $8.37 | $+34.57 | $7,358.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUGO` | 13 | $89.87 | $2.05 | $+5.93 | $8,524.65 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 1902 | $0.64 | $17.88 | — | $7,289.49 | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1217.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AMTX` | 654 | $1.86 | $8.44 | — | $6,064.61 | baseline list, no extra gate; list yday_mover,ohlc_hot; ⚪; ret5=+16.9; leftover $1217.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NIQ` | 62 | $19.56 | $2.18 | — | $4,849.71 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+7.6; leftover $1217.81 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `OMER` | 64 | $18.75 | $2.18 | — | $3,647.53 | baseline list, no extra gate; list ohlc_hot; ret5=+12.1; leftover $1217.81 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ERO` | 32 | $38.00 | $2.09 | — | $2,429.45 | baseline list, no extra gate; list ohlc_hot,mover_buy; ⚪; ret5=+16.6; leftover $1217.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `TRLV` | 110 | $11.02 | $2.32 | — | $1,214.93 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.0; leftover $1217.81 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FUTU` | 10 | $118.02 | $2.02 | — | $32.71 | baseline list, no extra gate; list ohlc_hot; ⚪; ret5=+17.5; leftover $1217.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 276 | $3.81 | $3.62 | $-194.86 | $1,080.65 | dropped from list after 4 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DEFT` | 1902 | $0.60 | $17.44 | $-111.40 | $2,204.41 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AMTX` | 654 | $1.91 | $8.56 | $+15.71 | $3,444.99 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NIQ` | 62 | $19.20 | $2.20 | $-26.69 | $4,633.19 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `OMER` | 64 | $18.96 | $2.20 | $+9.06 | $5,844.43 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ERO` | 32 | $40.51 | $2.11 | $+76.13 | $7,138.65 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 110 | $11.22 | $2.35 | $+17.33 | $8,370.50 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FUTU` | 10 | $124.67 | $2.04 | $+62.44 | $9,615.16 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 40 | $29.33 | $2.11 | — | $8,439.85 | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1201.89 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `XHG` | 296 | $4.06 | $3.82 | — | $7,234.27 | baseline list, no extra gate; list ohlc_hot; ret5=+16.1; leftover $1201.89 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NIQ` | 63 | $18.79 | $2.18 | — | $6,048.32 | baseline list, no extra gate; list ohlc_hot; ret5=+7.6; leftover $1201.89 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DEFT` | 2003 | $0.60 | $18.03 | — | $4,828.49 | baseline list, no extra gate; list ohlc_hot; ret5=+17.6; leftover $1201.89 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OMER` | 65 | $18.24 | $2.19 | — | $3,640.71 | baseline list, no extra gate; list ohlc_hot; ret5=+12.1; leftover $1201.89 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERO` | 30 | $39.20 | $2.08 | — | $2,462.63 | baseline list, no extra gate; list ohlc_hot; ret5=+16.6; leftover $1201.89 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `TRLV` | 105 | $11.38 | $2.31 | — | $1,265.42 | baseline list, no extra gate; list ohlc_hot; ret5=+15.0; leftover $1201.89 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FUTU` | 9 | $128.00 | $2.02 | — | $111.41 | baseline list, no extra gate; list ohlc_hot; ret5=+17.5; leftover $1201.89 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 40 | $28.27 | $2.13 | $-46.64 | $1,240.08 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `NIQ` | 63 | $19.06 | $2.20 | $+12.63 | $2,438.66 | dropped from list after 2 sess (min 1) | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 118 | $10.27 | $2.34 | — | $1,224.45 | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1219.33 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NIQ` | 65 | $18.60 | $2.19 | — | $13.27 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+7.6; leftover $1219.33 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟡 ab🟢 heat🔴 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SMTC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MRVL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ELMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `STDN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRGP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `OABI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `XNCR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PAYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GUTS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `UEC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NIQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `AMTX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `HOOD` | no_price | no 09:30 open |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `HOOD` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TXG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `CVI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HOOD` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `XHG` | 296 | 2026-08-28 @ $4.06 | baseline list, no extra gate; list ohlc_hot; ret5=+16.1; leftover $1201.89 |
| `DEFT` | 2003 | 2026-08-28 @ $0.60 | baseline list, no extra gate; list ohlc_hot; ret5=+17.6; leftover $1201.89 |
| `OMER` | 65 | 2026-08-28 @ $18.24 | baseline list, no extra gate; list ohlc_hot; ret5=+12.1; leftover $1201.89 |
| `ERO` | 30 | 2026-08-28 @ $39.20 | baseline list, no extra gate; list ohlc_hot; ret5=+16.6; leftover $1201.89 |
| `TRLV` | 105 | 2026-08-28 @ $11.38 | baseline list, no extra gate; list ohlc_hot; ret5=+15.0; leftover $1201.89 |
| `FUTU` | 9 | 2026-08-28 @ $128.00 | baseline list, no extra gate; list ohlc_hot; ret5=+17.5; leftover $1201.89 |
| `NVAX` | 118 | 2026-09-03 @ $10.27 | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1219.33 |
| `NIQ` | 65 | 2026-09-03 @ $18.60 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+7.6; leftover $1219.33 |
