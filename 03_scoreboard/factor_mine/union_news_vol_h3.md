# Factor mine action — `union_news_vol_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+0.89%** ($10,089) · signal-only (no cash/fees) was -1.72%. Starts YES **16/17**. Fills 60 · skips 76 · realized $-354.11.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news=good,vol=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $0.54.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-14 | +5.50 | $10,000.00 | — | ANGX, ARX, SNDK, MH, HLIT | — | $359.91 | $9,693.57 | $10,053.48 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 | BUY ANGX x464 @ 4.31; BUY ARX x102 @ 19.57; BUY SNDK x1 @ 1646.93; BUY MH x147 @ 13.55; BUY HLIT x151 @ 13.18 |
| 2026-08-17 | +2.25 | $359.91 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 | — | — | $359.91 | $9,870.49 | $10,230.40 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 | hold ANGX,ARX,SNDK,MH,HLIT |
| 2026-08-18 | -6.20 | $359.91 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 | — | — | $359.91 | $9,722.17 | $10,082.08 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $359.91 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 | — | ARX, SNDK, MH, HLIT | $7,890.55 | $2,134.40 | $10,024.95 | ANGX×464 | SELL ARX (dropped from list after 3 sess (min 3)); SELL SNDK (dropped from list after 3 sess (min 3)); SELL MH (dropped from list after 3 sess (min 3)); SELL HLIT (dropped from list after 3 sess (min 3)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $7,890.55 | ANGX×464 | BHP, MRNA, HUMA, BTGO, ZLAB, CRSP, APA, AUTL | ANGX | $144.73 | $9,621.91 | $9,766.64 | BHP×13, MRNA×8, HUMA×1768, BTGO×189, ZLAB×47, CRSP×21, APA×27, AUTL×506 | SELL ANGX (dropped from list after 4 sess (min 3)); BUY BHP x13 @ 91.01; BUY MRNA x8 @ 150.14; BUY HUMA x1768 @ 0.71; BUY BTGO x189 @ 6.61; BUY ZLAB x47 @ 26.57; BUY CRSP x21 @ 58.73; BUY APA x27 @ 44.76; BUY AUTL x506 @ 2.47 |
| 2026-08-21 | +3.25 | $144.73 | BHP×13, MRNA×8, HUMA×1768, BTGO×189, ZLAB×47, CRSP×21, APA×27, AUTL×506 | MARA, BTDR, HIVE | — | $75.74 | $9,779.68 | $9,855.42 | BHP×13, MRNA×8, HUMA×1768, BTGO×189, ZLAB×47, CRSP×21, APA×27, AUTL×506, MARA×2, BTDR×2, HIVE×7 | BUY MARA x2 @ 11.70; BUY BTDR x2 @ 11.10; BUY HIVE x7 @ 3.24 |
| 2026-08-24 | -5.17 | $75.74 | BHP×13, MRNA×8, HUMA×1768, BTGO×189, ZLAB×47, CRSP×21, APA×27, AUTL×506, MARA×2, BTDR×2, HIVE×7 | — | — | $75.74 | $9,673.75 | $9,749.49 | BHP×13, MRNA×8, HUMA×1768, BTGO×189, ZLAB×47, CRSP×21, APA×27, AUTL×506, MARA×2, BTDR×2, HIVE×7 | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $75.74 | BHP×13, MRNA×8, HUMA×1768, BTGO×189, ZLAB×47, CRSP×21, APA×27, AUTL×506, MARA×2, BTDR×2, HIVE×7 | RUM, EZPW, REAX, BKKT, FCX, NVAX, AU | BHP, MRNA, HUMA, BTGO, ZLAB, CRSP, APA, AUTL | $151.26 | $9,557.34 | $9,708.60 | MARA×2, BTDR×2, HIVE×7, RUM×147, EZPW×39, REAX×57, BKKT×166, FCX×17, NVAX×155, AU×11 | SELL BHP (dropped from list after 3 sess (min 3)); SELL MRNA (dropped from list after 3 sess (min 3)); SELL HUMA (dropped from list after 3 sess (min 3)); SELL BTGO (dropped from list after 3 sess (min 3)); SELL ZLAB (dropped from list after 3 sess (min 3)); SELL CRSP (dropped from list after 3 sess (min 3)); SELL APA (dropped from list after 3 sess (min 3)); SELL AUTL (dropped from list after 3 sess (min 3)); BUY RUM x147 @ 9.36; BUY EZPW x39 @ 34.48; BUY REAX x57 @ 24.00; BUY BKKT x166 @ 8.28; BUY FCX x17 @ 77.90; BUY NVAX x155 @ 8.88; BUY AU x11 @ 119.46 |
| 2026-08-26 | +2.02 | $151.26 | MARA×2, BTDR×2, HIVE×7, RUM×147, EZPW×39, REAX×57, BKKT×166, FCX×17, NVAX×155, AU×11 | — | — | $151.26 | $9,542.56 | $9,693.82 | MARA×2, BTDR×2, HIVE×7, RUM×147, EZPW×39, REAX×57, BKKT×166, FCX×17, NVAX×155, AU×11 | hold MARA,BTDR,HIVE,RUM,EZPW,REAX,BKKT,FCX,NVAX,AU |
| 2026-08-27 | — | $151.26 | MARA×2, BTDR×2, HIVE×7, RUM×147, EZPW×39, REAX×57, BKKT×166, FCX×17, NVAX×155, AU×11 | — | MARA, BTDR, HIVE | $216.38 | $9,652.53 | $9,868.91 | RUM×147, EZPW×39, REAX×57, BKKT×166, FCX×17, NVAX×155, AU×11 | SELL MARA (dropped from list after 4 sess (min 3)); SELL BTDR (dropped from list after 4 sess (min 3)); SELL HIVE (dropped from list after 4 sess (min 3)) |
| 2026-08-28 | +0.75 | $216.38 | RUM×147, EZPW×39, REAX×57, BKKT×166, FCX×17, NVAX×155, AU×11 | CAPR, SEDG, SMTC, ERAS, BBWI, ZYME | RUM, EZPW, REAX, BKKT, FCX, NVAX, AU | $213.94 | $9,678.72 | $9,892.66 | CAPR×178, SEDG×48, SMTC×10, ERAS×84, BBWI×87, ZYME×55 | SELL RUM (dropped from list after 3 sess (min 3)); SELL EZPW (dropped from list after 3 sess (min 3)); SELL REAX (dropped from list after 3 sess (min 3)); SELL BKKT (dropped from list after 3 sess (min 3)); SELL FCX (dropped from list after 3 sess (min 3)); SELL NVAX (dropped from list after 3 sess (min 3)); SELL AU (dropped from list after 3 sess (min 3)); BUY CAPR x178 @ 9.19; BUY SEDG x48 @ 33.78; BUY SMTC x10 @ 149.40; BUY ERAS x84 @ 19.30; BUY BBWI x87 @ 18.68; BUY ZYME x55 @ 29.33 |
| 2026-08-31 | -5.85 | $213.94 | CAPR×178, SEDG×48, SMTC×10, ERAS×84, BBWI×87, ZYME×55 | — | — | $213.94 | $9,223.03 | $9,436.97 | CAPR×178, SEDG×48, SMTC×10, ERAS×84, BBWI×87, ZYME×55 | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $213.94 | CAPR×178, SEDG×48, SMTC×10, ERAS×84, BBWI×87, ZYME×55 | — | — | $213.94 | $9,396.87 | $9,610.81 | CAPR×178, SEDG×48, SMTC×10, ERAS×84, BBWI×87, ZYME×55 | hard-red sit S=-6.30 |
| 2026-09-02 | -3.83 | $213.94 | CAPR×178, SEDG×48, SMTC×10, ERAS×84, BBWI×87, ZYME×55 | — | CAPR, SEDG, SMTC, ERAS, BBWI, ZYME | $9,645.88 | $0.00 | $9,645.88 | — | SELL CAPR (dropped from list after 3 sess (min 3)); SELL SEDG (dropped from list after 3 sess (min 3)); SELL SMTC (dropped from list after 3 sess (min 3)); SELL ERAS (dropped from list after 3 sess (min 3)); SELL BBWI (dropped from list after 3 sess (min 3)); SELL ZYME (dropped from list after 3 sess (min 3)) |
| 2026-09-03 | -0.90 | $9,645.88 | — | MMED | — | $4.48 | $10,050.48 | $10,054.96 | MMED×423 | BUY MMED x423 @ 22.78 |
| 2026-09-04 | — | $4.48 | MMED×423 | BAK | — | $0.54 | $10,088.20 | $10,088.74 | MMED×423, BAK×2 | BUY BAK x2 @ 1.95 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 464 | $4.31 | $5.99 | — | $7,994.17 | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 102 | $19.57 | $2.30 | — | $5,995.74 | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SNDK` | 1 | $1646.93 | $1.99 | — | $4,346.82 | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.4; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 147 | $13.55 | $2.43 | — | $2,352.53 | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 151 | $13.18 | $2.44 | — | $359.91 | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 102 | $19.58 | $2.33 | $-3.60 | $2,354.74 | dropped from list after 3 sess (min 3) | join🟡 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `SNDK` | 1 | $1682.40 | $2.02 | $+31.47 | $4,035.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MH` | 147 | $13.01 | $2.47 | $-84.28 | $5,945.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `HLIT` | 151 | $12.90 | $2.48 | $-47.21 | $7,890.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ANGX` | 464 | $4.57 | $6.08 | $+108.57 | $10,004.95 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,819.79 | combo gate; gate news=good,vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1250.62 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $7,616.65 | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1250.62 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1768 | $0.71 | $17.80 | — | $6,348.87 | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1250.62 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 189 | $6.61 | $2.56 | — | $5,097.97 | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1250.62 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 47 | $26.57 | $2.13 | — | $3,847.05 | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ret5=+4.8; leftover $1250.62 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $2,611.67 | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1250.62 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 27 | $44.76 | $2.07 | — | $1,401.08 | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ret5=+8.7; leftover $1250.62 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 506 | $2.47 | $6.53 | — | $144.73 | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1250.62 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 2 | $11.70 | $0.24 | — | $121.09 | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $24.12 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 2 | $11.10 | $0.23 | — | $98.67 | combo gate; gate news=good,vol=good; list yday_gainer; ret5=+19.1; leftover $24.12 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 7 | $3.24 | $0.25 | — | $75.74 | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ret5=+21.3; leftover $24.12 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.95 | $2.05 | $+60.14 | $1,321.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $141.19 | $2.03 | $-75.65 | $2,448.53 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HUMA` | 1768 | $0.67 | $17.45 | $-100.67 | $3,615.64 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BTGO` | 189 | $6.89 | $2.60 | $+48.71 | $4,915.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ZLAB` | 47 | $25.93 | $2.15 | $-34.36 | $6,131.81 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.00 | $2.07 | $-40.46 | $7,326.74 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 27 | $42.70 | $2.09 | $-59.78 | $8,477.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 506 | $2.32 | $6.62 | $-89.05 | $9,644.84 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 147 | $9.36 | $2.43 | — | $8,266.49 | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ret5=+21.3; leftover $1377.83 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 39 | $34.48 | $2.11 | — | $6,919.66 | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1377.83 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 57 | $24.00 | $2.16 | — | $5,549.50 | combo gate; gate news=good,vol=good; list yday_mover; ret5=+10.0; leftover $1377.83 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BKKT` | 166 | $8.28 | $2.49 | — | $4,172.54 | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ret5=+12.3; leftover $1377.83 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 17 | $77.90 | $2.04 | — | $2,846.19 | combo gate; gate news=good,vol=good; list ohlc_hot,mover_buy; ⚪; ret5=+15.3; leftover $1377.83 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NVAX` | 155 | $8.88 | $2.46 | — | $1,467.34 | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ret5=+11.1; leftover $1377.83 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 11 | $119.46 | $2.02 | — | $151.26 | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1377.83 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `MARA` | 2 | $11.56 | $0.26 | $-0.78 | $174.12 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `BTDR` | 2 | $11.05 | $0.25 | $-0.56 | $195.97 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `HIVE` | 7 | $2.95 | $0.25 | $-2.53 | $216.38 | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RUM` | 147 | $9.51 | $2.47 | $+17.15 | $1,611.88 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 39 | $33.50 | $2.13 | $-42.45 | $2,916.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `REAX` | 57 | $25.91 | $2.18 | $+104.53 | $4,390.94 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BKKT` | 166 | $8.50 | $2.53 | $+31.51 | $5,799.41 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 17 | $78.83 | $2.06 | $+11.71 | $7,137.46 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 catal🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `NVAX` | 155 | $9.12 | $2.49 | $+32.25 | $8,548.57 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 11 | $117.41 | $2.04 | $-26.62 | $9,838.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 178 | $9.19 | $2.52 | — | $8,199.69 | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1639.67 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 48 | $33.78 | $2.13 | — | $6,576.12 | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1639.67 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 10 | $149.40 | $2.02 | — | $5,080.10 | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1639.67 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 84 | $19.30 | $2.24 | — | $3,456.65 | combo gate; gate news=good,vol=good; list yday_gainer; ret5=-4.1; leftover $1639.67 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 87 | $18.68 | $2.25 | — | $1,829.24 | combo gate; gate news=good,vol=good; list yday_gainer; ret5=+0.2; leftover $1639.67 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 55 | $29.33 | $2.15 | — | $213.94 | combo gate; gate news=good,vol=good; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1639.67 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `CAPR` | 178 | $10.77 | $2.57 | $+276.15 | $2,128.43 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 48 | $31.87 | $2.16 | $-95.97 | $3,656.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 10 | $127.63 | $2.04 | $-221.76 | $4,930.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ERAS` | 84 | $17.58 | $2.27 | $-148.99 | $6,404.75 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBWI` | 87 | $18.77 | $2.28 | $+3.30 | $8,035.46 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ZYME` | 55 | $29.32 | $2.18 | $-4.88 | $9,645.88 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 423 | $22.78 | $5.46 | — | $4.48 | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $9645.88 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 2 | $1.95 | $0.04 | — | $0.54 | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $4.48 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `SNDK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HLIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `SNDK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HLIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AUTL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HUMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BTGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ZLAB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `APA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 24.12 < 1 share @ 119.43 |
| 2026-08-21 | `FUTU` | cash | leftover split 24.12 < 1 share @ 115.18 |
| 2026-08-21 | `DE` | cash | leftover split 24.12 < 1 share @ 623.26 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HUMA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ZLAB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `APA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `BTDR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `HIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `MARA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `HIVE` | no_price | no 09:30 open — carry |
| 2026-08-26 | `RUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `REAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BKKT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NVAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `REAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BKKT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NVAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ERAS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BBWI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ZYME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ERAS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BBWI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ZYME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `MMED` | 423 | 2026-09-03 @ $22.78 | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $9645.88 |
| `BAK` | 2 | 2026-09-04 @ $1.95 | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $4.48 |
