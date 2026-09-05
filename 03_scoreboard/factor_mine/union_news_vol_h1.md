# Factor mine action — `union_news_vol_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+2.72%** ($10,272) · signal-only (no cash/fees) was +3.52%. Starts YES **13/17**. Fills 67 · skips 24 · realized $+393.65.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news=good,vol=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $2.07.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | ANGX, ARX, SNDK, MH, HLIT | — | $359.91 | $10,053.48 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $359.91 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 | $10,215.56 | +162.08 | — | ANGX, ARX, SNDK, MH, HLIT | $10,200.18 | $10,200.18 | — | 09:30 open · cash $359.91 (unchanged overnight, no fees) · equity $10,215.56 vs prior close $10,053.48 (+162.08) because holdings re-marked: ANGX×464 yday $4.37 → 09:30 $4.60 +106.72; ARX×102 yday $19.58 → 09:30 $19.57 -1.02; SNDK×1 yday $1641.11 → 09:30 $1700.74 +59.63; MH×147 yday $13.10 → 09:30 $13.16 +8.82; HLIT×151 yday $13.92 → 09:30 $13.84 -12.08 |
| 2026-08-18 | -6.20 | $10,200.18 | — | $10,200.18 | -0.00 | — | — | $10,200.18 | $10,200.18 | — | 09:30 open · cash $10,200.18 · no holdings · equity $10,200.18 vs prior close $10,200.18 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-19 | -7.20 | $10,200.18 | — | $10,200.18 | -0.00 | — | — | $10,200.18 | $10,200.18 | — | 09:30 open · cash $10,200.18 · no holdings · equity $10,200.18 vs prior close $10,200.18 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-20 | +1.12 | $10,200.18 | — | $10,200.18 | -0.00 | BHP, MRNA, HUMA, BTGO, ZLAB, CRSP, APA, AUTL | — | $127.82 | $9,962.59 | BHP×14, MRNA×8, HUMA×1803, BTGO×193, ZLAB×47, CRSP×21, APA×28, AUTL×516 | 09:30 open · cash $10,200.18 · no holdings · equity $10,200.18 vs prior close $10,200.18 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $127.82 | BHP×14, MRNA×8, HUMA×1803, BTGO×193, ZLAB×47, CRSP×21, APA×28, AUTL×516 | $10,098.31 | +135.72 | AU, FUTU, DE, MARA, BTDR, HIVE | BHP, MRNA, HUMA, BTGO, ZLAB, APA | $173.05 | $10,071.89 | CRSP×21, AUTL×516, AU×10, FUTU×10, DE×2, MARA×107, BTDR×113, HIVE×387 | 09:30 open · cash $127.82 (unchanged overnight, no fees) · equity $10,098.31 vs prior close $9,962.59 (+135.72) because holdings re-marked: BHP×14 yday $93.63 → 09:30 $95.72 +29.26; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; HUMA×1803 yday $0.68 → 09:30 $0.67 -12.62; BTGO×193 yday $6.60 → 09:30 $6.95 +67.55; ZLAB×47 yday $26.02 → 09:30 $26.25 +10.81; CRSP×21 yday $58.12 → 09:30 $59.72 +33.60; APA×28 yday $44.39 → 09:30 $44.52 +3.64; AUTL×516 yday $2.46 → 09:30 $2.47 +5.16 |
| 2026-08-24 | -5.17 | $173.05 | CRSP×21, AUTL×516, AU×10, FUTU×10, DE×2, MARA×107, BTDR×113, HIVE×387 | $9,994.23 | -77.66 | — | AUTL, AU, FUTU, DE, MARA, BTDR, HIVE | $8,737.03 | $9,932.14 | CRSP×21 | 09:30 open · cash $173.05 (unchanged overnight, no fees) · equity $9,994.23 vs prior close $10,071.89 (-77.66) because holdings re-marked: CRSP×21 yday $59.50 → 09:30 $58.79 -14.91; AUTL×516 yday $2.41 → 09:30 $2.36 -25.80; AU×10 yday $121.22 → 09:30 $120.50 -7.20; FUTU×10 yday $123.64 → 09:30 $120.87 -27.70; DE×2 yday $647.47 → 09:30 $653.62 +12.30; MARA×107 yday $11.26 → 09:30 $11.18 -8.56; BTDR×113 yday $11.37 → 09:30 $11.49 +13.56; HIVE×387 yday $3.03 → 09:30 $2.98 -19.35 |
| 2026-08-25 | +1.80 | $8,737.03 | CRSP×21 | $9,934.03 | +1.89 | RUM, EZPW, REAX, BKKT, FCX, NVAX, AU | CRSP | $129.09 | $9,930.95 | RUM×151, EZPW×41, REAX×59, BKKT×171, FCX×18, NVAX×159, AU×11 | 09:30 open · cash $8,737.03 (unchanged overnight, no fees) · equity $9,934.03 vs prior close $9,932.14 (+1.89) because holdings re-marked: CRSP×21 yday $56.91 → 09:30 $57.00 +1.89 |
| 2026-08-26 | +2.02 | $129.09 | RUM×151, EZPW×41, REAX×59, BKKT×171, FCX×18, NVAX×159, AU×11 | $9,930.95 | +0.00 | — | — | $129.09 | $9,916.19 | RUM×151, EZPW×41, REAX×59, BKKT×171, FCX×18, NVAX×159, AU×11 | 09:30 open · cash $129.09 (unchanged overnight, no fees) · equity $9,930.95 vs prior close $9,930.95 (+0.00) because holdings re-marked: RUM×151 yday $9.35 → 09:30 $9.35 +0.00; EZPW×41 yday $34.69 → 09:30 $34.69 +0.00; REAX×59 yday $24.00 → 09:30 $24.00 +0.00; BKKT×171 yday $8.38 → 09:30 $8.38 +0.00; FCX×18 yday $77.49 → 09:30 $77.49 +0.00; NVAX×159 yday $8.93 → 09:30 $8.93 +0.00; AU×11 yday $118.55 → 09:30 $118.55 +0.00 |
| 2026-08-27 | — | $129.09 | RUM×151, EZPW×41, REAX×59, BKKT×171, FCX×18, NVAX×159, AU×11 | $10,345.72 | +429.53 | — | RUM, EZPW, REAX, BKKT, FCX, NVAX, AU | $10,329.76 | $10,329.76 | — | 09:30 open · cash $129.09 (unchanged overnight, no fees) · equity $10,345.72 vs prior close $9,916.19 (+429.53) because holdings re-marked: RUM×151 yday $9.35 → 09:30 $10.07 +108.72; EZPW×41 yday $34.69 → 09:30 $35.70 +41.41; REAX×59 yday $24.00 → 09:30 $26.61 +153.99; BKKT×171 yday $8.38 → 09:30 $8.38 +0.00; FCX×18 yday $77.49 → 09:30 $79.34 +33.30; NVAX×159 yday $8.93 → 09:30 $9.33 +63.60; AU×11 yday $118.55 → 09:30 $119.80 +13.75 |
| 2026-08-28 | +0.75 | $10,329.76 | — | $10,329.76 | +0.00 | CAPR, SEDG, SMTC, ERAS, BBWI, ZYME | — | $128.03 | $10,384.47 | CAPR×187, SEDG×50, SMTC×11, ERAS×89, BBWI×92, ZYME×58 | 09:30 open · cash $10,329.76 · no holdings · equity $10,329.76 vs prior close $10,329.76 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-31 | -5.85 | $128.03 | CAPR×187, SEDG×50, SMTC×11, ERAS×89, BBWI×92, ZYME×58 | $9,940.11 | -444.36 | — | CAPR, SEDG, SMTC, ERAS, BBWI, ZYME | $9,926.54 | $9,926.54 | — | 09:30 open · cash $128.03 (unchanged overnight, no fees) · equity $9,940.11 vs prior close $10,384.47 (-444.36) because holdings re-marked: CAPR×187 yday $10.06 → 09:30 $9.44 -115.94; SEDG×50 yday $33.51 → 09:30 $31.50 -100.50; SMTC×11 yday $142.43 → 09:30 $133.04 -103.29; ERAS×89 yday $19.49 → 09:30 $17.90 -141.51; BBWI×92 yday $18.65 → 09:30 $19.30 +59.80; ZYME×58 yday $29.01 → 09:30 $28.27 -42.92 |
| 2026-09-01 | -6.30 | $9,926.54 | — | $9,926.54 | +0.00 | — | — | $9,926.54 | $9,926.54 | — | 09:30 open · cash $9,926.54 · no holdings · equity $9,926.54 vs prior close $9,926.54 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-02 | -3.83 | $9,926.54 | — | $9,926.54 | +0.00 | — | — | $9,926.54 | $9,926.54 | — | 09:30 open · cash $9,926.54 · no holdings · equity $9,926.54 vs prior close $9,926.54 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-03 | -0.90 | $9,926.54 | — | $9,926.54 | +0.00 | MMED | — | $11.63 | $10,347.23 | MMED×435 | 09:30 open · cash $9,926.54 · no holdings · equity $9,926.54 vs prior close $9,926.54 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $11.63 | MMED×435 | $10,399.43 | +52.20 | BAK | MMED | $2.07 | $10,272.43 | BAK×5294 | 09:30 open · cash $11.63 (unchanged overnight, no fees) · equity $10,399.43 vs prior close $10,347.23 (+52.20) because holdings re-marked: MMED×435 yday $23.76 → 09:30 $23.88 +52.20 |

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
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 464 | $4.60 | $6.08 | $+122.49 | $2,488.23 | ▲ +122.49 after sell → book $10,209.48; vs 09:30 mark -6.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 102 | $19.57 | $2.33 | $-4.62 | $4,482.04 | ▼ -4.62 after sell → book $10,207.15; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SNDK` | 1 | $1700.74 | $2.02 | $+49.81 | $6,180.77 | ▲ +49.81 after sell → book $10,205.13; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 147 | $13.16 | $2.47 | $-62.23 | $8,112.82 | ▼ -62.23 after sell → book $10,202.66; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 151 | $13.84 | $2.48 | $+94.73 | $10,200.18 | ▲ +94.73 after sell → book $10,200.18; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,200.18 | ▲ 09:30 equity $10,200.18 vs yday $10,200.18 (-0.00) | 09:30 open · cash $10,200.18 · no holdings · equity $10,200.18 vs prior close $10,200.18 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,200.18 | ▲ 09:30 equity $10,200.18 vs yday $10,200.18 (-0.00) | 09:30 open · cash $10,200.18 · no holdings · equity $10,200.18 vs prior close $10,200.18 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,200.18 | ▲ 09:30 equity $10,200.18 vs yday $10,200.18 (-0.00) | 09:30 open · cash $10,200.18 · no holdings · equity $10,200.18 vs prior close $10,200.18 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $8,924.00 | — | combo gate; gate news=good,vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1275.02 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $7,720.87 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1275.02 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1803 | $0.71 | $18.16 | — | $6,427.99 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1275.02 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 193 | $6.61 | $2.57 | — | $5,150.66 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1275.02 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 47 | $26.57 | $2.13 | — | $3,899.74 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ret5=+4.8; leftover $1275.02 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $2,664.35 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1275.02 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 28 | $44.76 | $2.07 | — | $1,409.00 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ret5=+8.7; leftover $1275.02 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 516 | $2.47 | $6.66 | — | $127.82 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1275.02 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $127.82 | ▲ 09:30 equity $10,098.31 vs yday $9,962.59 (+135.72) | 09:30 open · cash $127.82 (unchanged overnight, no fees) · equity $10,098.31 vs prior close $9,962.59 (+135.72) because holdings re-marked: BHP×14 yday $93.63 → 09:30 $95.72 +29.26; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; HUMA×1803 yday $0.68 → 09:30 $0.67 -12.62; BTGO×193 yday $6.60 → 09:30 $6.95 +67.55; ZLAB×47 yday $26.02 → 09:30 $26.25 +10.81; CRSP×21 yday $58.12 → 09:30 $59.72 +33.60; APA×28 yday $44.39 → 09:30 $44.52 +3.64; AUTL×516 yday $2.46 → 09:30 $2.47 +5.16 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 14 | $95.72 | $2.05 | $+61.86 | $1,465.85 | ▲ +61.86 after sell → book $10,096.25; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $2,528.70 | ▼ -140.29 after sell → book $10,094.22; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 1803 | $0.67 | $17.87 | $-95.53 | $3,726.05 | ▼ -95.53 after sell → book $10,076.35; vs 09:30 mark -17.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 193 | $6.95 | $2.61 | $+61.40 | $5,064.79 | ▲ +61.40 after sell → book $10,073.74; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 47 | $26.25 | $2.15 | $-19.32 | $6,296.39 | ▼ -19.32 after sell → book $10,071.59; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 28 | $44.52 | $2.09 | $-10.89 | $7,540.85 | ▼ -10.89 after sell → book $10,069.49; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $6,344.53 | — | combo gate; gate news=good,vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1256.81 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 10 | $115.18 | $2.02 | — | $5,190.71 | — | combo gate; gate news=good,vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1256.81 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $3,942.20 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1256.81 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 107 | $11.70 | $2.31 | — | $2,687.98 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1256.81 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 113 | $11.10 | $2.33 | — | $1,431.92 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=+19.1; leftover $1256.81 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 387 | $3.24 | $4.99 | — | $173.05 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ret5=+21.3; leftover $1256.81 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $173.05 | ▼ 09:30 equity $9,994.23 vs yday $10,071.89 (-77.66) | 09:30 open · cash $173.05 (unchanged overnight, no fees) · equity $9,994.23 vs prior close $10,071.89 (-77.66) because holdings re-marked: CRSP×21 yday $59.50 → 09:30 $58.79 -14.91; AUTL×516 yday $2.41 → 09:30 $2.36 -25.80; AU×10 yday $121.22 → 09:30 $120.50 -7.20; FUTU×10 yday $123.64 → 09:30 $120.87 -27.70; DE×2 yday $647.47 → 09:30 $653.62 +12.30; MARA×107 yday $11.26 → 09:30 $11.18 -8.56; BTDR×113 yday $11.37 → 09:30 $11.49 +13.56; HIVE×387 yday $3.03 → 09:30 $2.98 -19.35 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 516 | $2.36 | $6.75 | $-70.17 | $1,384.06 | ▼ -70.17 after sell → book $9,987.48; vs 09:30 mark -6.75 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.50 | $2.04 | $+6.64 | $2,587.02 | ▲ +6.64 after sell → book $9,985.44; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 10 | $120.87 | $2.04 | $+52.84 | $3,793.68 | ▲ +52.84 after sell → book $9,983.40; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.62 | $2.02 | $+56.71 | $5,098.90 | ▲ +56.71 after sell → book $9,981.38; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 107 | $11.18 | $2.34 | $-60.29 | $6,292.82 | ▼ -60.29 after sell → book $9,979.04; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTDR` | 113 | $11.49 | $2.36 | $+39.95 | $7,588.83 | ▲ +39.95 after sell → book $9,976.68; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 387 | $2.98 | $5.07 | $-110.68 | $8,737.03 | ▼ -110.68 after sell → book $9,971.62; vs 09:30 mark -5.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,737.03 | ▲ 09:30 equity $9,934.03 vs yday $9,932.14 (+1.89) | 09:30 open · cash $8,737.03 (unchanged overnight, no fees) · equity $9,934.03 vs prior close $9,932.14 (+1.89) because holdings re-marked: CRSP×21 yday $56.91 → 09:30 $57.00 +1.89 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.00 | $2.07 | $-40.46 | $9,931.95 | ▼ -40.46 after sell → book $9,931.95; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 151 | $9.36 | $2.44 | — | $8,516.15 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ret5=+21.3; leftover $1418.85 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 41 | $34.48 | $2.11 | — | $7,100.36 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1418.85 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 59 | $24.00 | $2.17 | — | $5,682.19 | — | combo gate; gate news=good,vol=good; list yday_mover; ret5=+10.0; leftover $1418.85 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BKKT` | 171 | $8.28 | $2.50 | — | $4,263.81 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ret5=+12.3; leftover $1418.85 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 18 | $77.90 | $2.04 | — | $2,859.56 | — | combo gate; gate news=good,vol=good; list ohlc_hot,mover_buy; ⚪; ret5=+15.3; leftover $1418.85 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NVAX` | 159 | $8.88 | $2.47 | — | $1,445.18 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ret5=+11.1; leftover $1418.85 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 11 | $119.46 | $2.02 | — | $129.09 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1418.85 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.09 | ▲ 09:30 equity $9,930.95 vs yday $9,930.95 (+0.00) | 09:30 open · cash $129.09 (unchanged overnight, no fees) · equity $9,930.95 vs prior close $9,930.95 (+0.00) because holdings re-marked: RUM×151 yday $9.35 → 09:30 $9.35 +0.00; EZPW×41 yday $34.69 → 09:30 $34.69 +0.00; REAX×59 yday $24.00 → 09:30 $24.00 +0.00; BKKT×171 yday $8.38 → 09:30 $8.38 +0.00; FCX×18 yday $77.49 → 09:30 $77.49 +0.00; NVAX×159 yday $8.93 → 09:30 $8.93 +0.00; AU×11 yday $118.55 → 09:30 $118.55 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.09 | ▲ 09:30 equity $10,345.72 vs yday $9,916.19 (+429.53) | 09:30 open · cash $129.09 (unchanged overnight, no fees) · equity $10,345.72 vs prior close $9,916.19 (+429.53) because holdings re-marked: RUM×151 yday $9.35 → 09:30 $10.07 +108.72; EZPW×41 yday $34.69 → 09:30 $35.70 +41.41; REAX×59 yday $24.00 → 09:30 $26.61 +153.99; BKKT×171 yday $8.38 → 09:30 $8.38 +0.00; FCX×18 yday $77.49 → 09:30 $79.34 +33.30; NVAX×159 yday $8.93 → 09:30 $9.33 +63.60; AU×11 yday $118.55 → 09:30 $119.80 +13.75 | — |
| 2026-08-27 09:30 ET | **SELL** | `RUM` | 151 | $10.07 | $2.48 | $+102.29 | $1,647.18 | ▲ +102.29 after sell → book $10,343.24; vs 09:30 mark -2.48 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `EZPW` | 41 | $35.70 | $2.13 | $+45.77 | $3,108.75 | ▲ +45.77 after sell → book $10,341.11; vs 09:30 mark -2.13 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `REAX` | 59 | $26.61 | $2.19 | $+149.63 | $4,676.55 | ▲ +149.63 after sell → book $10,338.92; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BKKT` | 171 | $8.38 | $2.54 | $+12.05 | $6,106.99 | ▲ +12.05 after sell → book $10,336.38; vs 09:30 mark -2.54 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FCX` | 18 | $79.34 | $2.07 | $+21.81 | $7,533.04 | ▲ +21.81 after sell → book $10,334.31; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NVAX` | 159 | $9.33 | $2.51 | $+66.58 | $9,014.01 | ▲ +66.58 after sell → book $10,331.81; vs 09:30 mark -2.50 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AU` | 11 | $119.80 | $2.04 | $-0.33 | $10,329.76 | ▼ -0.33 after sell → book $10,329.76; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,329.76 | ▲ 09:30 equity $10,329.76 vs yday $10,329.76 (+0.00) | 09:30 open · cash $10,329.76 · no holdings · equity $10,329.76 vs prior close $10,329.76 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 187 | $9.19 | $2.55 | — | $8,608.68 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1721.63 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 50 | $33.78 | $2.14 | — | $6,917.54 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1721.63 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 11 | $149.40 | $2.02 | — | $5,272.12 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1721.63 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 89 | $19.30 | $2.26 | — | $3,552.16 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=-4.1; leftover $1721.63 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 92 | $18.68 | $2.27 | — | $1,831.34 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=+0.2; leftover $1721.63 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 58 | $29.33 | $2.16 | — | $128.03 | — | combo gate; gate news=good,vol=good; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1721.63 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $128.03 | ▼ 09:30 equity $9,940.11 vs yday $10,384.47 (-444.36) | 09:30 open · cash $128.03 (unchanged overnight, no fees) · equity $9,940.11 vs prior close $10,384.47 (-444.36) because holdings re-marked: CAPR×187 yday $10.06 → 09:30 $9.44 -115.94; SEDG×50 yday $33.51 → 09:30 $31.50 -100.50; SMTC×11 yday $142.43 → 09:30 $133.04 -103.29; ERAS×89 yday $19.49 → 09:30 $17.90 -141.51; BBWI×92 yday $18.65 → 09:30 $19.30 +59.80; ZYME×58 yday $29.01 → 09:30 $28.27 -42.92 | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 187 | $9.44 | $2.60 | $+41.60 | $1,890.71 | ▲ +41.60 after sell → book $9,937.51; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 50 | $31.50 | $2.16 | $-118.30 | $3,463.55 | ▼ -118.30 after sell → book $9,935.35; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 11 | $133.04 | $2.04 | $-184.03 | $4,924.95 | ▼ -184.03 after sell → book $9,933.31; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 89 | $17.90 | $2.28 | $-129.14 | $6,515.76 | ▼ -129.14 after sell → book $9,931.02; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 92 | $19.30 | $2.30 | $+52.48 | $8,289.07 | ▲ +52.48 after sell → book $9,928.73; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 58 | $28.27 | $2.19 | $-65.83 | $9,926.54 | ▼ -65.83 after sell → book $9,926.54; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,926.54 | ▲ 09:30 equity $9,926.54 vs yday $9,926.54 (+0.00) | 09:30 open · cash $9,926.54 · no holdings · equity $9,926.54 vs prior close $9,926.54 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,926.54 | ▲ 09:30 equity $9,926.54 vs yday $9,926.54 (+0.00) | 09:30 open · cash $9,926.54 · no holdings · equity $9,926.54 vs prior close $9,926.54 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,926.54 | ▲ 09:30 equity $9,926.54 vs yday $9,926.54 (+0.00) | 09:30 open · cash $9,926.54 · no holdings · equity $9,926.54 vs prior close $9,926.54 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 435 | $22.78 | $5.61 | — | $11.63 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $9926.54 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.63 | ▲ 09:30 equity $10,399.43 vs yday $10,347.23 (+52.20) | 09:30 open · cash $11.63 (unchanged overnight, no fees) · equity $10,399.43 vs prior close $10,347.23 (+52.20) because holdings re-marked: MMED×435 yday $23.76 → 09:30 $23.88 +52.20 | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 435 | $23.88 | $5.77 | $+467.12 | $10,393.66 | ▲ +467.12 after sell → book $10,393.66; vs 09:30 mark -5.77 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 5294 | $1.95 | $68.29 | — | $2.07 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $10393.66 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AUTL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `RUM` | no_price | no 09:30 open — carry |
| 2026-08-26 | `EZPW` | no_price | no 09:30 open — carry |
| 2026-08-26 | `REAX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BKKT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `FCX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `NVAX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AU` | no_price | no 09:30 open — carry |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `BAK` | 5294 | 2026-09-04 @ $1.95 | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $10393.66 |
