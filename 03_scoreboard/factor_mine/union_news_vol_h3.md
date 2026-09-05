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

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | ANGX, ARX, SNDK, MH, HLIT | — | $359.91 | $10,053.48 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $359.91 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 | $10,215.56 | +162.08 | — | — | $359.91 | $10,230.40 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 | 09:30 open · cash $359.91 (unchanged overnight, no fees) · equity $10,215.56 vs prior close $10,053.48 (+162.08) because holdings re-marked: ANGX×464 yday $4.37 → 09:30 $4.60 +106.72; ARX×102 yday $19.58 → 09:30 $19.57 -1.02; SNDK×1 yday $1641.11 → 09:30 $1700.74 +59.63; MH×147 yday $13.10 → 09:30 $13.16 +8.82; HLIT×151 yday $13.92 → 09:30 $13.84 -12.08 |
| 2026-08-18 | -6.20 | $359.91 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 | $10,119.58 | -110.82 | — | — | $359.91 | $10,082.08 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 | 09:30 open · cash $359.91 (unchanged overnight, no fees) · equity $10,119.58 vs prior close $10,230.40 (-110.82) because holdings re-marked: ANGX×464 yday $4.71 → 09:30 $4.79 +37.12; ARX×102 yday $19.54 → 09:30 $19.57 +3.06; SNDK×1 yday $1786.85 → 09:30 $1677.54 -109.31; MH×147 yday $12.77 → 09:30 $13.00 +33.81; HLIT×151 yday $13.43 → 09:30 $12.93 -75.50 |
| 2026-08-19 | -7.20 | $359.91 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 | $10,122.41 | +40.33 | — | ARX, SNDK, MH, HLIT | $7,890.55 | $10,024.95 | ANGX×464 | 09:30 open · cash $359.91 (unchanged overnight, no fees) · equity $10,122.41 vs prior close $10,082.08 (+40.33) because holdings re-marked: ANGX×464 yday $4.85 → 09:30 $4.79 -27.84; ARX×102 yday $19.56 → 09:30 $19.58 +2.04; SNDK×1 yday $1625.78 → 09:30 $1682.40 +56.62; MH×147 yday $13.12 → 09:30 $13.01 -16.17; HLIT×151 yday $12.73 → 09:30 $12.90 +25.67 |
| 2026-08-20 | +1.12 | $7,890.55 | ANGX×464 | $10,011.03 | -13.92 | BHP, MRNA, HUMA, BTGO, ZLAB, CRSP, APA, AUTL | ANGX | $144.73 | $9,766.64 | BHP×13, MRNA×8, HUMA×1768, BTGO×189, ZLAB×47, CRSP×21, APA×27, AUTL×506 | 09:30 open · cash $7,890.55 (unchanged overnight, no fees) · equity $10,011.03 vs prior close $10,024.95 (-13.92) because holdings re-marked: ANGX×464 yday $4.60 → 09:30 $4.57 -13.92 |
| 2026-08-21 | +3.25 | $144.73 | BHP×13, MRNA×8, HUMA×1768, BTGO×189, ZLAB×47, CRSP×21, APA×27, AUTL×506 | $9,898.88 | +132.24 | MARA, BTDR, HIVE | — | $75.74 | $9,855.42 | BHP×13, MRNA×8, HUMA×1768, BTGO×189, ZLAB×47, CRSP×21, APA×27, AUTL×506, MARA×2, BTDR×2, HIVE×7 | 09:30 open · cash $144.73 (unchanged overnight, no fees) · equity $9,898.88 vs prior close $9,766.64 (+132.24) because holdings re-marked: BHP×13 yday $93.63 → 09:30 $95.72 +27.17; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; HUMA×1768 yday $0.68 → 09:30 $0.67 -12.38; BTGO×189 yday $6.60 → 09:30 $6.95 +66.15; ZLAB×47 yday $26.02 → 09:30 $26.25 +10.81; CRSP×21 yday $58.12 → 09:30 $59.72 +33.60; APA×27 yday $44.39 → 09:30 $44.52 +3.51; AUTL×506 yday $2.46 → 09:30 $2.47 +5.06 |
| 2026-08-24 | -5.17 | $75.74 | BHP×13, MRNA×8, HUMA×1768, BTGO×189, ZLAB×47, CRSP×21, APA×27, AUTL×506, MARA×2, BTDR×2, HIVE×7 | $9,840.22 | -15.20 | — | — | $75.74 | $9,749.49 | BHP×13, MRNA×8, HUMA×1768, BTGO×189, ZLAB×47, CRSP×21, APA×27, AUTL×506, MARA×2, BTDR×2, HIVE×7 | 09:30 open · cash $75.74 (unchanged overnight, no fees) · equity $9,840.22 vs prior close $9,855.42 (-15.20) because holdings re-marked: BHP×13 yday $97.03 → 09:30 $97.34 +4.03; MRNA×8 yday $145.13 → 09:30 $142.70 -19.44; HUMA×1768 yday $0.64 → 09:30 $0.68 +67.18; BTGO×189 yday $6.84 → 09:30 $6.87 +5.67; ZLAB×47 yday $26.01 → 09:30 $25.59 -19.74; CRSP×21 yday $59.50 → 09:30 $58.79 -14.91; APA×27 yday $43.39 → 09:30 $42.93 -12.42; AUTL×506 yday $2.41 → 09:30 $2.36 -25.30; MARA×2 yday $11.26 → 09:30 $11.18 -0.16; BTDR×2 yday $11.37 → 09:30 $11.49 +0.24; HIVE×7 yday $3.03 → 09:30 $2.98 -0.35 |
| 2026-08-25 | +1.80 | $75.74 | BHP×13, MRNA×8, HUMA×1768, BTGO×189, ZLAB×47, CRSP×21, APA×27, AUTL×506, MARA×2, BTDR×2, HIVE×7 | $9,746.59 | -2.90 | RUM, EZPW, REAX, BKKT, FCX, NVAX, AU | BHP, MRNA, HUMA, BTGO, ZLAB, CRSP, APA, AUTL | $151.26 | $9,708.60 | MARA×2, BTDR×2, HIVE×7, RUM×147, EZPW×39, REAX×57, BKKT×166, FCX×17, NVAX×155, AU×11 | 09:30 open · cash $75.74 (unchanged overnight, no fees) · equity $9,746.59 vs prior close $9,749.49 (-2.90) because holdings re-marked: BHP×13 yday $96.66 → 09:30 $95.95 -9.23; MRNA×8 yday $139.27 → 09:30 $141.19 +15.36; HUMA×1768 yday $0.67 → 09:30 $0.67 +0.00; BTGO×189 yday $6.97 → 09:30 $6.89 -15.12; ZLAB×47 yday $25.51 → 09:30 $25.93 +19.74; CRSP×21 yday $56.91 → 09:30 $57.00 +1.89; APA×27 yday $42.10 → 09:30 $42.70 +16.20; AUTL×506 yday $2.38 → 09:30 $2.32 -30.36; MARA×2 yday $11.44 → 09:30 $11.28 -0.32; BTDR×2 yday $11.30 → 09:30 $11.19 -0.22; HIVE×7 yday $2.94 → 09:30 $2.82 -0.84 |
| 2026-08-26 | +2.02 | $151.26 | MARA×2, BTDR×2, HIVE×7, RUM×147, EZPW×39, REAX×57, BKKT×166, FCX×17, NVAX×155, AU×11 | $9,708.60 | -0.00 | — | — | $151.26 | $9,693.82 | MARA×2, BTDR×2, HIVE×7, RUM×147, EZPW×39, REAX×57, BKKT×166, FCX×17, NVAX×155, AU×11 | 09:30 open · cash $151.26 (unchanged overnight, no fees) · equity $9,708.60 vs prior close $9,708.60 (-0.00) because holdings re-marked: MARA×2 yday $11.29 → 09:30 $11.29 +0.00; BTDR×2 yday $11.28 → 09:30 $11.28 +0.00; HIVE×7 yday $2.89 → 09:30 $2.89 +0.00; RUM×147 yday $9.35 → 09:30 $9.35 +0.00; EZPW×39 yday $34.69 → 09:30 $34.69 +0.00; REAX×57 yday $24.00 → 09:30 $24.00 +0.00; BKKT×166 yday $8.38 → 09:30 $8.38 +0.00; FCX×17 yday $77.49 → 09:30 $77.49 +0.00; NVAX×155 yday $8.93 → 09:30 $8.93 +0.00; AU×11 yday $118.55 → 09:30 $118.55 +0.00 |
| 2026-08-27 | — | $151.26 | MARA×2, BTDR×2, HIVE×7, RUM×147, EZPW×39, REAX×57, BKKT×166, FCX×17, NVAX×155, AU×11 | $10,110.30 | +416.48 | — | MARA, BTDR, HIVE | $216.38 | $9,868.91 | RUM×147, EZPW×39, REAX×57, BKKT×166, FCX×17, NVAX×155, AU×11 | 09:30 open · cash $151.26 (unchanged overnight, no fees) · equity $10,110.30 vs prior close $9,693.82 (+416.48) because holdings re-marked: MARA×2 yday $11.29 → 09:30 $11.56 +0.54; BTDR×2 yday $11.28 → 09:30 $11.05 -0.46; HIVE×7 yday $2.89 → 09:30 $2.95 +0.42; RUM×147 yday $9.35 → 09:30 $10.07 +105.84; EZPW×39 yday $34.69 → 09:30 $35.70 +39.39; REAX×57 yday $24.00 → 09:30 $26.61 +148.77; BKKT×166 yday $8.38 → 09:30 $8.38 +0.00; FCX×17 yday $77.49 → 09:30 $79.34 +31.45; NVAX×155 yday $8.93 → 09:30 $9.33 +62.00; AU×11 yday $118.55 → 09:30 $119.80 +13.75 |
| 2026-08-28 | +0.75 | $216.38 | RUM×147, EZPW×39, REAX×57, BKKT×166, FCX×17, NVAX×155, AU×11 | $9,853.94 | -14.97 | CAPR, SEDG, SMTC, ERAS, BBWI, ZYME | RUM, EZPW, REAX, BKKT, FCX, NVAX, AU | $213.94 | $9,892.66 | CAPR×178, SEDG×48, SMTC×10, ERAS×84, BBWI×87, ZYME×55 | 09:30 open · cash $216.38 (unchanged overnight, no fees) · equity $9,853.94 vs prior close $9,868.91 (-14.97) because holdings re-marked: RUM×147 yday $9.38 → 09:30 $9.51 +19.11; EZPW×39 yday $33.90 → 09:30 $33.50 -15.60; REAX×57 yday $26.59 → 09:30 $25.91 -38.76; BKKT×166 yday $8.23 → 09:30 $8.50 +44.82; FCX×17 yday $79.00 → 09:30 $78.83 -2.89; NVAX×155 yday $9.21 → 09:30 $9.12 -13.95; AU×11 yday $118.11 → 09:30 $117.41 -7.70 |
| 2026-08-31 | -5.85 | $213.94 | CAPR×178, SEDG×48, SMTC×10, ERAS×84, BBWI×87, ZYME×55 | $9,474.21 | -418.45 | — | — | $213.94 | $9,436.97 | CAPR×178, SEDG×48, SMTC×10, ERAS×84, BBWI×87, ZYME×55 | 09:30 open · cash $213.94 (unchanged overnight, no fees) · equity $9,474.21 vs prior close $9,892.66 (-418.45) because holdings re-marked: CAPR×178 yday $10.06 → 09:30 $9.44 -110.36; SEDG×48 yday $33.51 → 09:30 $31.50 -96.48; SMTC×10 yday $142.43 → 09:30 $133.04 -93.90; ERAS×84 yday $19.49 → 09:30 $17.90 -133.56; BBWI×87 yday $18.65 → 09:30 $19.30 +56.55; ZYME×55 yday $29.01 → 09:30 $28.27 -40.70 |
| 2026-09-01 | -6.30 | $213.94 | CAPR×178, SEDG×48, SMTC×10, ERAS×84, BBWI×87, ZYME×55 | $9,719.84 | +282.87 | — | — | $213.94 | $9,610.81 | CAPR×178, SEDG×48, SMTC×10, ERAS×84, BBWI×87, ZYME×55 | 09:30 open · cash $213.94 (unchanged overnight, no fees) · equity $9,719.84 vs prior close $9,436.97 (+282.87) because holdings re-marked: CAPR×178 yday $9.36 → 09:30 $10.43 +190.46; SEDG×48 yday $31.27 → 09:30 $32.22 +45.60; SMTC×10 yday $132.54 → 09:30 $131.65 -8.90; ERAS×84 yday $17.90 → 09:30 $18.00 +8.40; BBWI×87 yday $19.22 → 09:30 $19.10 -10.44; ZYME×55 yday $28.27 → 09:30 $29.32 +57.75 |
| 2026-09-02 | -3.83 | $213.94 | CAPR×178, SEDG×48, SMTC×10, ERAS×84, BBWI×87, ZYME×55 | $9,659.37 | +48.56 | — | CAPR, SEDG, SMTC, ERAS, BBWI, ZYME | $9,645.88 | $9,645.88 | — | 09:30 open · cash $213.94 (unchanged overnight, no fees) · equity $9,659.37 vs prior close $9,610.81 (+48.56) because holdings re-marked: CAPR×178 yday $10.19 → 09:30 $10.77 +103.24; SEDG×48 yday $31.80 → 09:30 $31.87 +3.36; SMTC×10 yday $129.50 → 09:30 $127.63 -18.70; ERAS×84 yday $17.70 → 09:30 $17.58 -10.08; BBWI×87 yday $19.10 → 09:30 $18.77 -28.71; ZYME×55 yday $29.33 → 09:30 $29.32 -0.55 |
| 2026-09-03 | -0.90 | $9,645.88 | — | $9,645.88 | -0.00 | MMED | — | $4.48 | $10,054.96 | MMED×423 | 09:30 open · cash $9,645.88 · no holdings · equity $9,645.88 vs prior close $9,645.88 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $4.48 | MMED×423 | $10,105.72 | +50.76 | BAK | — | $0.54 | $10,088.74 | MMED×423, BAK×2 | 09:30 open · cash $4.48 (unchanged overnight, no fees) · equity $10,105.72 vs prior close $10,054.96 (+50.76) because holdings re-marked: MMED×423 yday $23.76 → 09:30 $23.88 +50.76 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 464 | $4.31 | $5.99 | — | $7,994.17 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 102 | $19.57 | $2.30 | — | $5,995.74 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SNDK` | 1 | $1646.93 | $1.99 | — | $4,346.82 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.4; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 147 | $13.55 | $2.43 | — | $2,352.53 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 151 | $13.18 | $2.44 | — | $359.91 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $359.91 | ▲ 09:30 equity $10,215.56 vs yday $10,053.48 (+162.08) | 09:30 open · cash $359.91 (unchanged overnight, no fees) · equity $10,215.56 vs prior close $10,053.48 (+162.08) because holdings re-marked: ANGX×464 yday $4.37 → 09:30 $4.60 +106.72; ARX×102 yday $19.58 → 09:30 $19.57 -1.02; SNDK×1 yday $1641.11 → 09:30 $1700.74 +59.63; MH×147 yday $13.10 → 09:30 $13.16 +8.82; HLIT×151 yday $13.92 → 09:30 $13.84 -12.08 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $359.91 | ▼ 09:30 equity $10,119.58 vs yday $10,230.40 (-110.82) | 09:30 open · cash $359.91 (unchanged overnight, no fees) · equity $10,119.58 vs prior close $10,230.40 (-110.82) because holdings re-marked: ANGX×464 yday $4.71 → 09:30 $4.79 +37.12; ARX×102 yday $19.54 → 09:30 $19.57 +3.06; SNDK×1 yday $1786.85 → 09:30 $1677.54 -109.31; MH×147 yday $12.77 → 09:30 $13.00 +33.81; HLIT×151 yday $13.43 → 09:30 $12.93 -75.50 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $359.91 | ▲ 09:30 equity $10,122.41 vs yday $10,082.08 (+40.33) | 09:30 open · cash $359.91 (unchanged overnight, no fees) · equity $10,122.41 vs prior close $10,082.08 (+40.33) because holdings re-marked: ANGX×464 yday $4.85 → 09:30 $4.79 -27.84; ARX×102 yday $19.56 → 09:30 $19.58 +2.04; SNDK×1 yday $1625.78 → 09:30 $1682.40 +56.62; MH×147 yday $13.12 → 09:30 $13.01 -16.17; HLIT×151 yday $12.73 → 09:30 $12.90 +25.67 | — |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 102 | $19.58 | $2.33 | $-3.60 | $2,354.74 | ▼ -3.60 after sell → book $10,120.08; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | join🟡 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `SNDK` | 1 | $1682.40 | $2.02 | $+31.47 | $4,035.13 | ▲ +31.47 after sell → book $10,118.06; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MH` | 147 | $13.01 | $2.47 | $-84.28 | $5,945.13 | ▼ -84.28 after sell → book $10,115.59; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `HLIT` | 151 | $12.90 | $2.48 | $-47.21 | $7,890.55 | ▼ -47.21 after sell → book $10,113.11; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,890.55 | ▼ 09:30 equity $10,011.03 vs yday $10,024.95 (-13.92) | 09:30 open · cash $7,890.55 (unchanged overnight, no fees) · equity $10,011.03 vs prior close $10,024.95 (-13.92) because holdings re-marked: ANGX×464 yday $4.60 → 09:30 $4.57 -13.92 | — |
| 2026-08-20 09:30 ET | **SELL** | `ANGX` | 464 | $4.57 | $6.08 | $+108.57 | $10,004.95 | ▲ +108.57 after sell → book $10,004.95; vs 09:30 mark -6.08 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,819.79 | — | combo gate; gate news=good,vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1250.62 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $7,616.65 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1250.62 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1768 | $0.71 | $17.80 | — | $6,348.87 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1250.62 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 189 | $6.61 | $2.56 | — | $5,097.97 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1250.62 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 47 | $26.57 | $2.13 | — | $3,847.05 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ret5=+4.8; leftover $1250.62 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $2,611.67 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1250.62 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 27 | $44.76 | $2.07 | — | $1,401.08 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ret5=+8.7; leftover $1250.62 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 506 | $2.47 | $6.53 | — | $144.73 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1250.62 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $144.73 | ▲ 09:30 equity $9,898.88 vs yday $9,766.64 (+132.24) | 09:30 open · cash $144.73 (unchanged overnight, no fees) · equity $9,898.88 vs prior close $9,766.64 (+132.24) because holdings re-marked: BHP×13 yday $93.63 → 09:30 $95.72 +27.17; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; HUMA×1768 yday $0.68 → 09:30 $0.67 -12.38; BTGO×189 yday $6.60 → 09:30 $6.95 +66.15; ZLAB×47 yday $26.02 → 09:30 $26.25 +10.81; CRSP×21 yday $58.12 → 09:30 $59.72 +33.60; APA×27 yday $44.39 → 09:30 $44.52 +3.51; AUTL×506 yday $2.46 → 09:30 $2.47 +5.06 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 2 | $11.70 | $0.24 | — | $121.09 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $24.12 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 2 | $11.10 | $0.23 | — | $98.67 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=+19.1; leftover $24.12 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 7 | $3.24 | $0.25 | — | $75.74 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ret5=+21.3; leftover $24.12 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.74 | ▼ 09:30 equity $9,840.22 vs yday $9,855.42 (-15.20) | 09:30 open · cash $75.74 (unchanged overnight, no fees) · equity $9,840.22 vs prior close $9,855.42 (-15.20) because holdings re-marked: BHP×13 yday $97.03 → 09:30 $97.34 +4.03; MRNA×8 yday $145.13 → 09:30 $142.70 -19.44; HUMA×1768 yday $0.64 → 09:30 $0.68 +67.18; BTGO×189 yday $6.84 → 09:30 $6.87 +5.67; ZLAB×47 yday $26.01 → 09:30 $25.59 -19.74; CRSP×21 yday $59.50 → 09:30 $58.79 -14.91; APA×27 yday $43.39 → 09:30 $42.93 -12.42; AUTL×506 yday $2.41 → 09:30 $2.36 -25.30; MARA×2 yday $11.26 → 09:30 $11.18 -0.16; BTDR×2 yday $11.37 → 09:30 $11.49 +0.24; HIVE×7 yday $3.03 → 09:30 $2.98 -0.35 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.74 | ▼ 09:30 equity $9,746.59 vs yday $9,749.49 (-2.90) | 09:30 open · cash $75.74 (unchanged overnight, no fees) · equity $9,746.59 vs prior close $9,749.49 (-2.90) because holdings re-marked: BHP×13 yday $96.66 → 09:30 $95.95 -9.23; MRNA×8 yday $139.27 → 09:30 $141.19 +15.36; HUMA×1768 yday $0.67 → 09:30 $0.67 +0.00; BTGO×189 yday $6.97 → 09:30 $6.89 -15.12; ZLAB×47 yday $25.51 → 09:30 $25.93 +19.74; CRSP×21 yday $56.91 → 09:30 $57.00 +1.89; APA×27 yday $42.10 → 09:30 $42.70 +16.20; AUTL×506 yday $2.38 → 09:30 $2.32 -30.36; MARA×2 yday $11.44 → 09:30 $11.28 -0.32; BTDR×2 yday $11.30 → 09:30 $11.19 -0.22; HIVE×7 yday $2.94 → 09:30 $2.82 -0.84 | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.95 | $2.05 | $+60.14 | $1,321.05 | ▲ +60.14 after sell → book $9,744.55; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $141.19 | $2.03 | $-75.65 | $2,448.53 | ▼ -75.65 after sell → book $9,742.51; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HUMA` | 1768 | $0.67 | $17.45 | $-100.67 | $3,615.64 | ▼ -100.67 after sell → book $9,725.06; vs 09:30 mark -17.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BTGO` | 189 | $6.89 | $2.60 | $+48.71 | $4,915.25 | ▲ +48.71 after sell → book $9,722.46; vs 09:30 mark -2.60 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ZLAB` | 47 | $25.93 | $2.15 | $-34.36 | $6,131.81 | ▼ -34.36 after sell → book $9,720.31; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.00 | $2.07 | $-40.46 | $7,326.74 | ▼ -40.46 after sell → book $9,718.24; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 27 | $42.70 | $2.09 | $-59.78 | $8,477.54 | ▼ -59.78 after sell → book $9,716.14; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 506 | $2.32 | $6.62 | $-89.05 | $9,644.84 | ▼ -89.05 after sell → book $9,709.52; vs 09:30 mark -6.62 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 147 | $9.36 | $2.43 | — | $8,266.49 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ret5=+21.3; leftover $1377.83 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 39 | $34.48 | $2.11 | — | $6,919.66 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1377.83 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 57 | $24.00 | $2.16 | — | $5,549.50 | — | combo gate; gate news=good,vol=good; list yday_mover; ret5=+10.0; leftover $1377.83 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BKKT` | 166 | $8.28 | $2.49 | — | $4,172.54 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ret5=+12.3; leftover $1377.83 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 17 | $77.90 | $2.04 | — | $2,846.19 | — | combo gate; gate news=good,vol=good; list ohlc_hot,mover_buy; ⚪; ret5=+15.3; leftover $1377.83 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NVAX` | 155 | $8.88 | $2.46 | — | $1,467.34 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ret5=+11.1; leftover $1377.83 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 11 | $119.46 | $2.02 | — | $151.26 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1377.83 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.26 | ▲ 09:30 equity $9,708.60 vs yday $9,708.60 (-0.00) | 09:30 open · cash $151.26 (unchanged overnight, no fees) · equity $9,708.60 vs prior close $9,708.60 (-0.00) because holdings re-marked: MARA×2 yday $11.29 → 09:30 $11.29 +0.00; BTDR×2 yday $11.28 → 09:30 $11.28 +0.00; HIVE×7 yday $2.89 → 09:30 $2.89 +0.00; RUM×147 yday $9.35 → 09:30 $9.35 +0.00; EZPW×39 yday $34.69 → 09:30 $34.69 +0.00; REAX×57 yday $24.00 → 09:30 $24.00 +0.00; BKKT×166 yday $8.38 → 09:30 $8.38 +0.00; FCX×17 yday $77.49 → 09:30 $77.49 +0.00; NVAX×155 yday $8.93 → 09:30 $8.93 +0.00; AU×11 yday $118.55 → 09:30 $118.55 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.26 | ▲ 09:30 equity $10,110.30 vs yday $9,693.82 (+416.48) | 09:30 open · cash $151.26 (unchanged overnight, no fees) · equity $10,110.30 vs prior close $9,693.82 (+416.48) because holdings re-marked: MARA×2 yday $11.29 → 09:30 $11.56 +0.54; BTDR×2 yday $11.28 → 09:30 $11.05 -0.46; HIVE×7 yday $2.89 → 09:30 $2.95 +0.42; RUM×147 yday $9.35 → 09:30 $10.07 +105.84; EZPW×39 yday $34.69 → 09:30 $35.70 +39.39; REAX×57 yday $24.00 → 09:30 $26.61 +148.77; BKKT×166 yday $8.38 → 09:30 $8.38 +0.00; FCX×17 yday $77.49 → 09:30 $79.34 +31.45; NVAX×155 yday $8.93 → 09:30 $9.33 +62.00; AU×11 yday $118.55 → 09:30 $119.80 +13.75 | — |
| 2026-08-27 09:30 ET | **SELL** | `MARA` | 2 | $11.56 | $0.26 | $-0.78 | $174.12 | ▼ -0.78 after sell → book $10,110.04; vs 09:30 mark -0.26 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `BTDR` | 2 | $11.05 | $0.25 | $-0.56 | $195.97 | ▼ -0.56 after sell → book $10,109.79; vs 09:30 mark -0.25 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `HIVE` | 7 | $2.95 | $0.25 | $-2.53 | $216.38 | ▼ -2.53 after sell → book $10,109.55; vs 09:30 mark -0.24 | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $216.38 | ▼ 09:30 equity $9,853.94 vs yday $9,868.91 (-14.97) | 09:30 open · cash $216.38 (unchanged overnight, no fees) · equity $9,853.94 vs prior close $9,868.91 (-14.97) because holdings re-marked: RUM×147 yday $9.38 → 09:30 $9.51 +19.11; EZPW×39 yday $33.90 → 09:30 $33.50 -15.60; REAX×57 yday $26.59 → 09:30 $25.91 -38.76; BKKT×166 yday $8.23 → 09:30 $8.50 +44.82; FCX×17 yday $79.00 → 09:30 $78.83 -2.89; NVAX×155 yday $9.21 → 09:30 $9.12 -13.95; AU×11 yday $118.11 → 09:30 $117.41 -7.70 | — |
| 2026-08-28 09:30 ET | **SELL** | `RUM` | 147 | $9.51 | $2.47 | $+17.15 | $1,611.88 | ▲ +17.15 after sell → book $9,851.47; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 39 | $33.50 | $2.13 | $-42.45 | $2,916.25 | ▼ -42.45 after sell → book $9,849.34; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `REAX` | 57 | $25.91 | $2.18 | $+104.53 | $4,390.94 | ▲ +104.53 after sell → book $9,847.16; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BKKT` | 166 | $8.50 | $2.53 | $+31.51 | $5,799.41 | ▲ +31.51 after sell → book $9,844.63; vs 09:30 mark -2.53 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 17 | $78.83 | $2.06 | $+11.71 | $7,137.46 | ▲ +11.71 after sell → book $9,842.57; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 catal🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `NVAX` | 155 | $9.12 | $2.49 | $+32.25 | $8,548.57 | ▲ +32.25 after sell → book $9,840.08; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 11 | $117.41 | $2.04 | $-26.62 | $9,838.03 | ▼ -26.62 after sell → book $9,838.03; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 178 | $9.19 | $2.52 | — | $8,199.69 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1639.67 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 48 | $33.78 | $2.13 | — | $6,576.12 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1639.67 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 10 | $149.40 | $2.02 | — | $5,080.10 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1639.67 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 84 | $19.30 | $2.24 | — | $3,456.65 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=-4.1; leftover $1639.67 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 87 | $18.68 | $2.25 | — | $1,829.24 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=+0.2; leftover $1639.67 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 55 | $29.33 | $2.15 | — | $213.94 | — | combo gate; gate news=good,vol=good; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1639.67 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $213.94 | ▼ 09:30 equity $9,474.21 vs yday $9,892.66 (-418.45) | 09:30 open · cash $213.94 (unchanged overnight, no fees) · equity $9,474.21 vs prior close $9,892.66 (-418.45) because holdings re-marked: CAPR×178 yday $10.06 → 09:30 $9.44 -110.36; SEDG×48 yday $33.51 → 09:30 $31.50 -96.48; SMTC×10 yday $142.43 → 09:30 $133.04 -93.90; ERAS×84 yday $19.49 → 09:30 $17.90 -133.56; BBWI×87 yday $18.65 → 09:30 $19.30 +56.55; ZYME×55 yday $29.01 → 09:30 $28.27 -40.70 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $213.94 | ▲ 09:30 equity $9,719.84 vs yday $9,436.97 (+282.87) | 09:30 open · cash $213.94 (unchanged overnight, no fees) · equity $9,719.84 vs prior close $9,436.97 (+282.87) because holdings re-marked: CAPR×178 yday $9.36 → 09:30 $10.43 +190.46; SEDG×48 yday $31.27 → 09:30 $32.22 +45.60; SMTC×10 yday $132.54 → 09:30 $131.65 -8.90; ERAS×84 yday $17.90 → 09:30 $18.00 +8.40; BBWI×87 yday $19.22 → 09:30 $19.10 -10.44; ZYME×55 yday $28.27 → 09:30 $29.32 +57.75 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $213.94 | ▲ 09:30 equity $9,659.37 vs yday $9,610.81 (+48.56) | 09:30 open · cash $213.94 (unchanged overnight, no fees) · equity $9,659.37 vs prior close $9,610.81 (+48.56) because holdings re-marked: CAPR×178 yday $10.19 → 09:30 $10.77 +103.24; SEDG×48 yday $31.80 → 09:30 $31.87 +3.36; SMTC×10 yday $129.50 → 09:30 $127.63 -18.70; ERAS×84 yday $17.70 → 09:30 $17.58 -10.08; BBWI×87 yday $19.10 → 09:30 $18.77 -28.71; ZYME×55 yday $29.33 → 09:30 $29.32 -0.55 | — |
| 2026-09-02 09:30 ET | **SELL** | `CAPR` | 178 | $10.77 | $2.57 | $+276.15 | $2,128.43 | ▲ +276.15 after sell → book $9,656.80; vs 09:30 mark -2.57 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 48 | $31.87 | $2.16 | $-95.97 | $3,656.03 | ▼ -95.97 after sell → book $9,654.64; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 10 | $127.63 | $2.04 | $-221.76 | $4,930.29 | ▼ -221.76 after sell → book $9,652.60; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ERAS` | 84 | $17.58 | $2.27 | $-148.99 | $6,404.75 | ▼ -148.99 after sell → book $9,650.34; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBWI` | 87 | $18.77 | $2.28 | $+3.30 | $8,035.46 | ▲ +3.30 after sell → book $9,648.06; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ZYME` | 55 | $29.32 | $2.18 | $-4.88 | $9,645.88 | ▼ -4.88 after sell → book $9,645.88; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,645.88 | ▲ 09:30 equity $9,645.88 vs yday $9,645.88 (-0.00) | 09:30 open · cash $9,645.88 · no holdings · equity $9,645.88 vs prior close $9,645.88 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 423 | $22.78 | $5.46 | — | $4.48 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $9645.88 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.48 | ▲ 09:30 equity $10,105.72 vs yday $10,054.96 (+50.76) | 09:30 open · cash $4.48 (unchanged overnight, no fees) · equity $10,105.72 vs prior close $10,054.96 (+50.76) because holdings re-marked: MMED×423 yday $23.76 → 09:30 $23.88 +50.76 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 2 | $1.95 | $0.04 | — | $0.54 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $4.48 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

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
