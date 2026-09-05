# Factor mine action — `union_news_g_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ news_g, no 🚨

Cash book **-0.31%** ($9,969) · signal-only (no cash/fees) was +0.23%. Starts YES **2/17**. Fills 107 · skips 51 · realized $+120.57.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $1.32.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | TLN, VST, NRG, ANGX, ARX, MH, HLIT | — | $1,560.49 | $10,110.67 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $1,560.49 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94 | $10,211.68 | +101.01 | DVN, EOG, FANG, CELC, OUST | TLN, VST, NRG, ANGX, ARX, MH, HLIT | $165.17 | $10,281.82 | DVN×44, EOG×14, FANG×10, CELC×21, OUST×41 | 09:30 open · cash $1,560.49 (unchanged overnight, no fees) · equity $10,211.68 vs prior close $10,110.67 (+101.01) because holdings re-marked: TLN×3 yday $362.74 → 09:30 $367.88 +15.42; VST×8 yday $148.13 → 09:30 $149.37 +9.92; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; MH×92 yday $13.10 → 09:30 $13.16 +5.52; HLIT×94 yday $13.92 → 09:30 $13.84 -7.52 |
| 2026-08-18 | -6.20 | $165.17 | DVN×44, EOG×14, FANG×10, CELC×21, OUST×41 | $10,227.70 | -54.12 | — | DVN, EOG, FANG, CELC, OUST | $10,217.23 | $10,217.23 | — | 09:30 open · cash $165.17 (unchanged overnight, no fees) · equity $10,227.70 vs prior close $10,281.82 (-54.12) because holdings re-marked: DVN×44 yday $47.57 → 09:30 $48.00 +18.92; EOG×14 yday $146.15 → 09:30 $148.04 +26.46; FANG×10 yday $206.29 → 09:30 $208.93 +26.40; CELC×21 yday $92.44 → 09:30 $92.38 -1.26; OUST×41 yday $48.13 → 09:30 $45.09 -124.64 |
| 2026-08-19 | -7.20 | $10,217.23 | — | $10,217.23 | -0.00 | — | — | $10,217.23 | $10,217.23 | — | 09:30 open · cash $10,217.23 · no holdings · equity $10,217.23 vs prior close $10,217.23 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-20 | +1.12 | $10,217.23 | — | $10,217.23 | -0.00 | BHP, MRNA, HUMA, BTGO, ZLAB, CRSP, APA, AUTL | — | $113.67 | $9,978.95 | BHP×14, MRNA×8, HUMA×1806, BTGO×193, ZLAB×48, CRSP×21, APA×28, AUTL×517 | 09:30 open · cash $10,217.23 · no holdings · equity $10,217.23 vs prior close $10,217.23 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $113.67 | BHP×14, MRNA×8, HUMA×1806, BTGO×193, ZLAB×48, CRSP×21, APA×28, AUTL×517 | $10,114.89 | +135.94 | AU, FUTU, DE, MARA, BTDR, HIVE | BHP, MRNA, HUMA, BTGO, ZLAB, APA | $183.88 | $10,088.16 | CRSP×21, AUTL×517, AU×10, FUTU×10, DE×2, MARA×107, BTDR×113, HIVE×388 | 09:30 open · cash $113.67 (unchanged overnight, no fees) · equity $10,114.89 vs prior close $9,978.95 (+135.94) because holdings re-marked: BHP×14 yday $93.63 → 09:30 $95.72 +29.26; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; HUMA×1806 yday $0.68 → 09:30 $0.67 -12.64; BTGO×193 yday $6.60 → 09:30 $6.95 +67.55; ZLAB×48 yday $26.02 → 09:30 $26.25 +11.04; CRSP×21 yday $58.12 → 09:30 $59.72 +33.60; APA×28 yday $44.39 → 09:30 $44.52 +3.64; AUTL×517 yday $2.46 → 09:30 $2.47 +5.17 |
| 2026-08-24 | -5.17 | $183.88 | CRSP×21, AUTL×517, AU×10, FUTU×10, DE×2, MARA×107, BTDR×113, HIVE×388 | $10,010.40 | -77.76 | — | AUTL, AU, FUTU, DE, MARA, BTDR, HIVE | $8,753.17 | $9,948.28 | CRSP×21 | 09:30 open · cash $183.88 (unchanged overnight, no fees) · equity $10,010.40 vs prior close $10,088.16 (-77.76) because holdings re-marked: CRSP×21 yday $59.50 → 09:30 $58.79 -14.91; AUTL×517 yday $2.41 → 09:30 $2.36 -25.85; AU×10 yday $121.22 → 09:30 $120.50 -7.20; FUTU×10 yday $123.64 → 09:30 $120.87 -27.70; DE×2 yday $647.47 → 09:30 $653.62 +12.30; MARA×107 yday $11.26 → 09:30 $11.18 -8.56; BTDR×113 yday $11.37 → 09:30 $11.49 +13.56; HIVE×388 yday $3.03 → 09:30 $2.98 -19.40 |
| 2026-08-25 | +1.80 | $8,753.17 | CRSP×21 | $9,950.17 | +1.89 | RUM, EZPW, REAX, TRLV, VIRT, HOOD, ZYME, BKKT | CRSP | $169.59 | $9,929.72 | RUM×132, EZPW×36, REAX×51, TRLV×112, VIRT×18, HOOD×11, ZYME×41, BKKT×150 | 09:30 open · cash $8,753.17 (unchanged overnight, no fees) · equity $9,950.17 vs prior close $9,948.28 (+1.89) because holdings re-marked: CRSP×21 yday $56.91 → 09:30 $57.00 +1.89 |
| 2026-08-26 | +2.02 | $169.59 | RUM×132, EZPW×36, REAX×51, TRLV×112, VIRT×18, HOOD×11, ZYME×41, BKKT×150 | $9,929.72 | +0.00 | — | — | $169.59 | $9,930.52 | RUM×132, EZPW×36, REAX×51, TRLV×112, VIRT×18, HOOD×11, ZYME×41, BKKT×150 | 09:30 open · cash $169.59 (unchanged overnight, no fees) · equity $9,929.72 vs prior close $9,929.72 (+0.00) because holdings re-marked: RUM×132 yday $9.35 → 09:30 $9.35 +0.00; EZPW×36 yday $34.69 → 09:30 $34.69 +0.00; REAX×51 yday $24.00 → 09:30 $24.00 +0.00; TRLV×112 yday $11.02 → 09:30 $11.02 +0.00; VIRT×18 yday $66.29 → 09:30 $66.29 +0.00; HOOD×11 yday $104.22 → 09:30 $104.22 +0.00; ZYME×41 yday $29.81 → 09:30 $29.81 +0.00; BKKT×150 yday $8.38 → 09:30 $8.38 +0.00 |
| 2026-08-27 | — | $169.59 | RUM×132, EZPW×36, REAX×51, TRLV×112, VIRT×18, HOOD×11, ZYME×41, BKKT×150 | $10,164.51 | +233.99 | RRC, ACMR, MU, LRCX, NVDA | RUM, EZPW, REAX, TRLV, VIRT, HOOD, ZYME, BKKT | $2,860.38 | $10,116.55 | RRC×41, ACMR×20, MU×1, LRCX×5, NVDA×7 | 09:30 open · cash $169.59 (unchanged overnight, no fees) · equity $10,164.51 vs prior close $9,930.52 (+233.99) because holdings re-marked: RUM×132 yday $9.35 → 09:30 $10.07 +95.04; EZPW×36 yday $34.69 → 09:30 $35.70 +36.36; REAX×51 yday $24.00 → 09:30 $26.61 +133.11; TRLV×112 yday $11.02 → 09:30 $11.22 +22.40; VIRT×18 yday $66.29 → 09:30 $64.92 -24.66; HOOD×11 yday $104.22 → 09:30 $110.11 +64.79; ZYME×41 yday $29.81 → 09:30 $27.56 -92.25; BKKT×150 yday $8.38 → 09:30 $8.38 +0.00 |
| 2026-08-28 | +0.75 | $2,860.38 | RRC×41, ACMR×20, MU×1, LRCX×5, NVDA×7 | $10,313.85 | +197.30 | CAPR, SEDG, SMTC, OPTX, ERAS, BBWI, ZYME | ACMR, MU, LRCX, NVDA | $99.58 | $10,368.52 | RRC×41, CAPR×133, SEDG×36, SMTC×8, OPTX×143, ERAS×63, BBWI×65, ZYME×41 | 09:30 open · cash $2,860.38 (unchanged overnight, no fees) · equity $10,313.85 vs prior close $10,116.55 (+197.30) because holdings re-marked: RRC×41 yday $41.55 → 09:30 $41.44 -4.51; ACMR×20 yday $79.11 → 09:30 $81.65 +50.80; MU×1 yday $938.40 → 09:30 $967.01 +28.61; LRCX×5 yday $312.88 → 09:30 $318.88 +30.00; NVDA×7 yday $209.66 → 09:30 $222.86 +92.40 |
| 2026-08-31 | -5.85 | $99.58 | RRC×41, CAPR×133, SEDG×36, SMTC×8, OPTX×143, ERAS×63, BBWI×65, ZYME×41 | $9,998.56 | -369.96 | — | RRC, CAPR, SEDG, SMTC, OPTX, ERAS, BBWI | $8,823.92 | $9,982.99 | ZYME×41 | 09:30 open · cash $99.58 (unchanged overnight, no fees) · equity $9,998.56 vs prior close $10,368.52 (-369.96) because holdings re-marked: RRC×41 yday $41.64 → 09:30 $41.11 -21.73; CAPR×133 yday $10.06 → 09:30 $9.44 -82.46; SEDG×36 yday $33.51 → 09:30 $31.50 -72.36; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; OPTX×143 yday $8.73 → 09:30 $8.52 -30.03; ERAS×63 yday $19.49 → 09:30 $17.90 -100.17; BBWI×65 yday $18.65 → 09:30 $19.30 +42.25; ZYME×41 yday $29.01 → 09:30 $28.27 -30.34 |
| 2026-09-01 | -6.30 | $8,823.92 | ZYME×41 | $10,026.04 | +43.05 | — | ZYME | $10,023.91 | $10,023.91 | — | 09:30 open · cash $8,823.92 (unchanged overnight, no fees) · equity $10,026.04 vs prior close $9,982.99 (+43.05) because holdings re-marked: ZYME×41 yday $28.27 → 09:30 $29.32 +43.05 |
| 2026-09-02 | -3.83 | $10,023.91 | — | $10,023.91 | -0.00 | — | — | $10,023.91 | $10,023.91 | — | 09:30 open · cash $10,023.91 · no holdings · equity $10,023.91 vs prior close $10,023.91 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-03 | -0.90 | $10,023.91 | — | $10,023.91 | -0.00 | MMED, CNXC, OPTX, TRLV, TXG, ZYME, FCX, AVGO | — | $232.33 | $10,200.44 | MMED×55, CNXC×39, OPTX×172, TRLV×106, TXG×20, ZYME×41, FCX×17, AVGO×3 | 09:30 open · cash $10,023.91 · no holdings · equity $10,023.91 vs prior close $10,023.91 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $232.33 | MMED×55, CNXC×39, OPTX×172, TRLV×106, TXG×20, ZYME×41, FCX×17, AVGO×3 | $10,261.81 | +61.37 | BAK, AMTX | MMED, CNXC, OPTX, TRLV, AVGO | $1.32 | $9,969.43 | TXG×20, ZYME×41, FCX×17, BAK×1650, AMTX×1663 | 09:30 open · cash $232.33 (unchanged overnight, no fees) · equity $10,261.81 vs prior close $10,200.44 (+61.37) because holdings re-marked: MMED×55 yday $23.76 → 09:30 $23.88 +6.60; CNXC×39 yday $32.37 → 09:30 $32.88 +19.89; OPTX×172 yday $7.53 → 09:30 $7.59 +10.32; TRLV×106 yday $11.69 → 09:30 $11.89 +21.20; TXG×20 yday $61.65 → 09:30 $62.35 +14.00; ZYME×41 yday $31.05 → 09:30 $31.34 +11.89; FCX×17 yday $73.93 → 09:30 $75.34 +23.97; AVGO×3 yday $367.24 → 09:30 $351.74 -46.50 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $8,918.51 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,741.30 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+3.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,539.28 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $5,285.64 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $4,050.55 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $2,801.68 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $1,560.49 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,560.49 | ▲ 09:30 equity $10,211.68 vs yday $10,110.67 (+101.01) | 09:30 open · cash $1,560.49 (unchanged overnight, no fees) · equity $10,211.68 vs prior close $10,110.67 (+101.01) because holdings re-marked: TLN×3 yday $362.74 → 09:30 $367.88 +15.42; VST×8 yday $148.13 → 09:30 $149.37 +9.92; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; MH×92 yday $13.10 → 09:30 $13.16 +5.52; HLIT×94 yday $13.92 → 09:30 $13.84 -7.52 | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $2,662.11 | ▲ +20.13 after sell → book $10,209.66; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $3,855.04 | ▲ +15.71 after sell → book $10,207.63; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $5,127.00 | ▲ +69.94 after sell → book $10,205.59; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $6,457.20 | ▲ +76.56 after sell → book $10,201.79; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $7,687.91 | ▼ -4.38 after sell → book $10,199.59; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 92 | $13.16 | $2.29 | $-40.44 | $8,896.34 | ▼ -40.44 after sell → book $10,197.30; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 94 | $13.84 | $2.30 | $+57.47 | $10,195.00 | ▲ +57.47 after sell → book $10,195.00; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 44 | $46.18 | $2.12 | — | $8,160.96 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+6.7; leftover $2039.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 14 | $142.77 | $2.03 | — | $6,160.14 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.8; leftover $2039.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 10 | $202.70 | $2.02 | — | $4,131.12 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+8.3; leftover $2039.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 21 | $92.99 | $2.05 | — | $2,176.28 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.8; leftover $2039.00 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 41 | $49.00 | $2.11 | — | $165.17 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ⚪; ret5=+12.2; leftover $2039.00 | join🟡 sector🟢 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $165.17 | ▼ 09:30 equity $10,227.70 vs yday $10,281.82 (-54.12) | 09:30 open · cash $165.17 (unchanged overnight, no fees) · equity $10,227.70 vs prior close $10,281.82 (-54.12) because holdings re-marked: DVN×44 yday $47.57 → 09:30 $48.00 +18.92; EOG×14 yday $146.15 → 09:30 $148.04 +26.46; FANG×10 yday $206.29 → 09:30 $208.93 +26.40; CELC×21 yday $92.44 → 09:30 $92.38 -1.26; OUST×41 yday $48.13 → 09:30 $45.09 -124.64 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 44 | $48.00 | $2.15 | $+75.81 | $2,275.02 | ▲ +75.81 after sell → book $10,225.55; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 14 | $148.04 | $2.06 | $+69.69 | $4,345.52 | ▲ +69.69 after sell → book $10,223.49; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 10 | $208.93 | $2.05 | $+58.23 | $6,432.77 | ▲ +58.23 after sell → book $10,221.44; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 21 | $92.38 | $2.08 | $-16.94 | $8,370.67 | ▼ -16.94 after sell → book $10,219.36; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 41 | $45.09 | $2.14 | $-164.56 | $10,217.23 | ▼ -164.56 after sell → book $10,217.23; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,217.23 | ▲ 09:30 equity $10,217.23 vs yday $10,217.23 (-0.00) | 09:30 open · cash $10,217.23 · no holdings · equity $10,217.23 vs prior close $10,217.23 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,217.23 | ▲ 09:30 equity $10,217.23 vs yday $10,217.23 (-0.00) | 09:30 open · cash $10,217.23 · no holdings · equity $10,217.23 vs prior close $10,217.23 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $8,941.05 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1277.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $7,737.92 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1277.15 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1806 | $0.71 | $18.19 | — | $6,442.89 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1277.15 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 193 | $6.61 | $2.57 | — | $5,165.56 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1277.15 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 48 | $26.57 | $2.13 | — | $3,888.06 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.8; leftover $1277.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $2,652.68 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1277.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 28 | $44.76 | $2.07 | — | $1,397.33 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+8.7; leftover $1277.15 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 517 | $2.47 | $6.67 | — | $113.67 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1277.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.67 | ▲ 09:30 equity $10,114.89 vs yday $9,978.95 (+135.94) | 09:30 open · cash $113.67 (unchanged overnight, no fees) · equity $10,114.89 vs prior close $9,978.95 (+135.94) because holdings re-marked: BHP×14 yday $93.63 → 09:30 $95.72 +29.26; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; HUMA×1806 yday $0.68 → 09:30 $0.67 -12.64; BTGO×193 yday $6.60 → 09:30 $6.95 +67.55; ZLAB×48 yday $26.02 → 09:30 $26.25 +11.04; CRSP×21 yday $58.12 → 09:30 $59.72 +33.60; APA×28 yday $44.39 → 09:30 $44.52 +3.64; AUTL×517 yday $2.46 → 09:30 $2.47 +5.17 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 14 | $95.72 | $2.05 | $+61.86 | $1,451.70 | ▲ +61.86 after sell → book $10,112.84; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $2,514.54 | ▼ -140.29 after sell → book $10,110.81; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 1806 | $0.67 | $17.90 | $-95.68 | $3,713.89 | ▼ -95.68 after sell → book $10,092.91; vs 09:30 mark -17.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 193 | $6.95 | $2.61 | $+61.40 | $5,052.62 | ▲ +61.40 after sell → book $10,090.29; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 48 | $26.25 | $2.15 | $-19.65 | $6,310.47 | ▼ -19.65 after sell → book $10,088.14; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 28 | $44.52 | $2.09 | $-10.89 | $7,554.94 | ▼ -10.89 after sell → book $10,086.05; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $6,358.62 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1259.16 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 10 | $115.18 | $2.02 | — | $5,204.80 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1259.16 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $3,956.28 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1259.16 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 107 | $11.70 | $2.31 | — | $2,702.07 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1259.16 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 113 | $11.10 | $2.33 | — | $1,446.00 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+19.1; leftover $1259.16 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 388 | $3.24 | $5.01 | — | $183.88 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; leftover $1259.16 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $183.88 | ▼ 09:30 equity $10,010.40 vs yday $10,088.16 (-77.76) | 09:30 open · cash $183.88 (unchanged overnight, no fees) · equity $10,010.40 vs prior close $10,088.16 (-77.76) because holdings re-marked: CRSP×21 yday $59.50 → 09:30 $58.79 -14.91; AUTL×517 yday $2.41 → 09:30 $2.36 -25.85; AU×10 yday $121.22 → 09:30 $120.50 -7.20; FUTU×10 yday $123.64 → 09:30 $120.87 -27.70; DE×2 yday $647.47 → 09:30 $653.62 +12.30; MARA×107 yday $11.26 → 09:30 $11.18 -8.56; BTDR×113 yday $11.37 → 09:30 $11.49 +13.56; HIVE×388 yday $3.03 → 09:30 $2.98 -19.40 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 517 | $2.36 | $6.77 | $-70.30 | $1,397.23 | ▼ -70.30 after sell → book $10,003.63; vs 09:30 mark -6.77 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.50 | $2.04 | $+6.64 | $2,600.19 | ▲ +6.64 after sell → book $10,001.59; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 10 | $120.87 | $2.04 | $+52.84 | $3,806.85 | ▲ +52.84 after sell → book $9,999.55; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.62 | $2.02 | $+56.71 | $5,112.08 | ▲ +56.71 after sell → book $9,997.54; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 107 | $11.18 | $2.34 | $-60.29 | $6,306.00 | ▼ -60.29 after sell → book $9,995.20; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTDR` | 113 | $11.49 | $2.36 | $+39.95 | $7,602.01 | ▲ +39.95 after sell → book $9,992.84; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 388 | $2.98 | $5.08 | $-110.96 | $8,753.17 | ▼ -110.96 after sell → book $9,987.76; vs 09:30 mark -5.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,753.17 | ▲ 09:30 equity $9,950.17 vs yday $9,948.28 (+1.89) | 09:30 open · cash $8,753.17 (unchanged overnight, no fees) · equity $9,950.17 vs prior close $9,948.28 (+1.89) because holdings re-marked: CRSP×21 yday $56.91 → 09:30 $57.00 +1.89 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.00 | $2.07 | $-40.46 | $9,948.10 | ▼ -40.46 after sell → book $9,948.10; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 132 | $9.36 | $2.39 | — | $8,710.19 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; leftover $1243.51 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 36 | $34.48 | $2.10 | — | $7,466.81 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1243.51 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 51 | $24.00 | $2.14 | — | $6,240.67 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=+10.0; leftover $1243.51 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `TRLV` | 112 | $11.02 | $2.33 | — | $5,004.10 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.0; leftover $1243.51 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VIRT` | 18 | $66.29 | $2.04 | — | $3,808.84 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+13.2; leftover $1243.51 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HOOD` | 11 | $106.00 | $2.02 | — | $2,640.82 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ⚪; ret5=+13.2; leftover $1243.51 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 41 | $29.87 | $2.11 | — | $1,414.03 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+14.1; leftover $1243.51 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BKKT` | 150 | $8.28 | $2.44 | — | $169.59 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+12.3; leftover $1243.51 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $169.59 | ▲ 09:30 equity $9,929.72 vs yday $9,929.72 (+0.00) | 09:30 open · cash $169.59 (unchanged overnight, no fees) · equity $9,929.72 vs prior close $9,929.72 (+0.00) because holdings re-marked: RUM×132 yday $9.35 → 09:30 $9.35 +0.00; EZPW×36 yday $34.69 → 09:30 $34.69 +0.00; REAX×51 yday $24.00 → 09:30 $24.00 +0.00; TRLV×112 yday $11.02 → 09:30 $11.02 +0.00; VIRT×18 yday $66.29 → 09:30 $66.29 +0.00; HOOD×11 yday $104.22 → 09:30 $104.22 +0.00; ZYME×41 yday $29.81 → 09:30 $29.81 +0.00; BKKT×150 yday $8.38 → 09:30 $8.38 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $169.59 | ▲ 09:30 equity $10,164.51 vs yday $9,930.52 (+233.99) | 09:30 open · cash $169.59 (unchanged overnight, no fees) · equity $10,164.51 vs prior close $9,930.52 (+233.99) because holdings re-marked: RUM×132 yday $9.35 → 09:30 $10.07 +95.04; EZPW×36 yday $34.69 → 09:30 $35.70 +36.36; REAX×51 yday $24.00 → 09:30 $26.61 +133.11; TRLV×112 yday $11.02 → 09:30 $11.22 +22.40; VIRT×18 yday $66.29 → 09:30 $64.92 -24.66; HOOD×11 yday $104.22 → 09:30 $110.11 +64.79; ZYME×41 yday $29.81 → 09:30 $27.56 -92.25; BKKT×150 yday $8.38 → 09:30 $8.38 +0.00 | — |
| 2026-08-27 09:30 ET | **SELL** | `RUM` | 132 | $10.07 | $2.42 | $+88.92 | $1,496.42 | ▲ +88.92 after sell → book $10,162.10; vs 09:30 mark -2.41 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `EZPW` | 36 | $35.70 | $2.12 | $+39.70 | $2,779.50 | ▲ +39.70 after sell → book $10,159.98; vs 09:30 mark -2.12 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `REAX` | 51 | $26.61 | $2.16 | $+128.80 | $4,134.44 | ▲ +128.80 after sell → book $10,157.81; vs 09:30 mark -2.17 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 112 | $11.22 | $2.35 | $+17.72 | $5,388.73 | ▲ +17.72 after sell → book $10,155.46; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `VIRT` | 18 | $64.92 | $2.06 | $-28.77 | $6,555.23 | ▼ -28.77 after sell → book $10,153.40; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HOOD` | 11 | $110.11 | $2.04 | $+41.14 | $7,764.39 | ▲ +41.14 after sell → book $10,151.35; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ZYME` | 41 | $27.56 | $2.13 | $-98.96 | $8,892.22 | ▼ -98.96 after sell → book $10,149.22; vs 09:30 mark -2.13 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BKKT` | 150 | $8.38 | $2.48 | $+10.09 | $10,146.74 | ▲ +10.09 after sell → book $10,146.74; vs 09:30 mark -2.48 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 41 | $40.72 | $2.11 | — | $8,475.11 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; ret5=+1.8; leftover $1691.12 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 20 | $80.97 | $2.05 | — | $6,853.66 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=-1.3; leftover $1691.12 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $925.74 | $1.99 | — | $5,925.93 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=-0.5; leftover $1691.12 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 5 | $314.61 | $2.00 | — | $4,350.87 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=-5.5; leftover $1691.12 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 7 | $212.64 | $2.01 | — | $2,860.38 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=-4.6; leftover $1691.12 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,860.38 | ▲ 09:30 equity $10,313.85 vs yday $10,116.55 (+197.30) | 09:30 open · cash $2,860.38 (unchanged overnight, no fees) · equity $10,313.85 vs prior close $10,116.55 (+197.30) because holdings re-marked: RRC×41 yday $41.55 → 09:30 $41.44 -4.51; ACMR×20 yday $79.11 → 09:30 $81.65 +50.80; MU×1 yday $938.40 → 09:30 $967.01 +28.61; LRCX×5 yday $312.88 → 09:30 $318.88 +30.00; NVDA×7 yday $209.66 → 09:30 $222.86 +92.40 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 20 | $81.65 | $2.07 | $+9.48 | $4,491.31 | ▲ +9.48 after sell → book $10,311.78; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $967.01 | $2.01 | $+37.26 | $5,456.31 | ▲ +37.26 after sell → book $10,309.77; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 5 | $318.88 | $2.03 | $+17.32 | $7,048.68 | ▲ +17.32 after sell → book $10,307.74; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 7 | $222.86 | $2.03 | $+67.50 | $8,606.67 | ▲ +67.50 after sell → book $10,305.71; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 133 | $9.19 | $2.39 | — | $7,382.01 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1229.52 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 36 | $33.78 | $2.10 | — | $6,163.83 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1229.52 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $4,966.61 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1229.52 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 143 | $8.57 | $2.42 | — | $3,738.69 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-3.4; leftover $1229.52 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 63 | $19.30 | $2.18 | — | $2,520.61 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-4.1; leftover $1229.52 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 65 | $18.68 | $2.19 | — | $1,304.22 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+0.2; leftover $1229.52 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 41 | $29.33 | $2.11 | — | $99.58 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1229.52 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.58 | ▼ 09:30 equity $9,998.56 vs yday $10,368.52 (-369.96) | 09:30 open · cash $99.58 (unchanged overnight, no fees) · equity $9,998.56 vs prior close $10,368.52 (-369.96) because holdings re-marked: RRC×41 yday $41.64 → 09:30 $41.11 -21.73; CAPR×133 yday $10.06 → 09:30 $9.44 -82.46; SEDG×36 yday $33.51 → 09:30 $31.50 -72.36; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; OPTX×143 yday $8.73 → 09:30 $8.52 -30.03; ERAS×63 yday $19.49 → 09:30 $17.90 -100.17; BBWI×65 yday $18.65 → 09:30 $19.30 +42.25; ZYME×41 yday $29.01 → 09:30 $28.27 -30.34 | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 41 | $41.11 | $2.14 | $+11.74 | $1,782.95 | ▲ +11.74 after sell → book $9,996.42; vs 09:30 mark -2.14 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 133 | $9.44 | $2.42 | $+28.44 | $3,036.05 | ▲ +28.44 after sell → book $9,994.00; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 36 | $31.50 | $2.12 | $-86.30 | $4,167.93 | ▼ -86.30 after sell → book $9,991.88; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 8 | $133.04 | $2.03 | $-134.93 | $5,230.22 | ▼ -134.93 after sell → book $9,989.85; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 143 | $8.52 | $2.45 | $-12.02 | $6,446.13 | ▼ -12.02 after sell → book $9,987.40; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 63 | $17.90 | $2.20 | $-92.58 | $7,571.63 | ▼ -92.58 after sell → book $9,985.20; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 65 | $19.30 | $2.21 | $+35.91 | $8,823.92 | ▲ +35.91 after sell → book $9,982.99; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,823.92 | ▲ 09:30 equity $10,026.04 vs yday $9,982.99 (+43.05) | 09:30 open · cash $8,823.92 (unchanged overnight, no fees) · equity $10,026.04 vs prior close $9,982.99 (+43.05) because holdings re-marked: ZYME×41 yday $28.27 → 09:30 $29.32 +43.05 | — |
| 2026-09-01 09:30 ET | **SELL** | `ZYME` | 41 | $29.32 | $2.13 | $-4.66 | $10,023.91 | ▼ -4.66 after sell → book $10,023.91; vs 09:30 mark -2.13 | dropped from list after 2 sess (min 1) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,023.91 | ▲ 09:30 equity $10,023.91 vs yday $10,023.91 (-0.00) | 09:30 open · cash $10,023.91 · no holdings · equity $10,023.91 vs prior close $10,023.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,023.91 | ▲ 09:30 equity $10,023.91 vs yday $10,023.91 (-0.00) | 09:30 open · cash $10,023.91 · no holdings · equity $10,023.91 vs prior close $10,023.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 55 | $22.78 | $2.15 | — | $8,768.85 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1252.99 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 39 | $31.80 | $2.11 | — | $7,526.55 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+3.7; leftover $1252.99 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 172 | $7.25 | $2.51 | — | $6,277.04 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-3.4; leftover $1252.99 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `TRLV` | 106 | $11.78 | $2.31 | — | $5,026.05 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.0; leftover $1252.99 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `TXG` | 20 | $60.24 | $2.05 | — | $3,819.20 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+16.1; leftover $1252.99 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ZYME` | 41 | $30.00 | $2.11 | — | $2,587.09 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ⚪; ret5=+14.1; leftover $1252.99 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟡 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FCX` | 17 | $73.04 | $2.04 | — | $1,343.37 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.3; leftover $1252.99 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $369.68 | $2.00 | — | $232.33 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-6.2; leftover $1252.99 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $232.33 | ▲ 09:30 equity $10,261.81 vs yday $10,200.44 (+61.37) | 09:30 open · cash $232.33 (unchanged overnight, no fees) · equity $10,261.81 vs prior close $10,200.44 (+61.37) because holdings re-marked: MMED×55 yday $23.76 → 09:30 $23.88 +6.60; CNXC×39 yday $32.37 → 09:30 $32.88 +19.89; OPTX×172 yday $7.53 → 09:30 $7.59 +10.32; TRLV×106 yday $11.69 → 09:30 $11.89 +21.20; TXG×20 yday $61.65 → 09:30 $62.35 +14.00; ZYME×41 yday $31.05 → 09:30 $31.34 +11.89; FCX×17 yday $73.93 → 09:30 $75.34 +23.97; AVGO×3 yday $367.24 → 09:30 $351.74 -46.50 | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 55 | $23.88 | $2.18 | $+56.17 | $1,543.55 | ▲ +56.17 after sell → book $10,259.63; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 39 | $32.88 | $2.13 | $+37.89 | $2,823.75 | ▲ +37.89 after sell → book $10,257.51; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 172 | $7.59 | $2.54 | $+53.43 | $4,126.68 | ▲ +53.43 after sell → book $10,254.96; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `TRLV` | 106 | $11.89 | $2.34 | $+7.02 | $5,384.69 | ▲ +7.02 after sell → book $10,252.63; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $351.74 | $2.02 | $-57.84 | $6,437.89 | ▼ -57.84 after sell → book $10,250.61; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 1650 | $1.95 | $21.29 | — | $3,199.10 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $3218.94 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `AMTX` | 1663 | $1.91 | $21.45 | — | $1.32 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+16.9; leftover $3218.94 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1250.00 < 1 share @ 1646.93 |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `GO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `RUM` | no_price | no 09:30 open — carry |
| 2026-08-26 | `EZPW` | no_price | no 09:30 open — carry |
| 2026-08-26 | `REAX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `VIRT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `FLNC` | no_price | no 09:30 open |
| 2026-08-26 | `CAPR` | no_price | no 09:30 open |
| 2026-08-26 | `FWRD` | no_price | no 09:30 open |
| 2026-08-26 | `FCX` | no_price | no 09:30 open |
| 2026-08-27 | `ASML` | cash | leftover split 1691.12 < 1 share @ 1746.33 |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HOOD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BKKT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLBT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TXG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEM` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `POWW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TRLV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TXG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZYME` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `TXG` | 20 | 2026-09-03 @ $60.24 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+16.1; leftover $1252.99 |
| `ZYME` | 41 | 2026-09-03 @ $30.00 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ⚪; ret5=+14.1; leftover $1252.99 |
| `FCX` | 17 | 2026-09-03 @ $73.04 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.3; leftover $1252.99 |
| `BAK` | 1650 | 2026-09-04 @ $1.95 | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $3218.94 |
| `AMTX` | 1663 | 2026-09-04 @ $1.91 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+16.9; leftover $3218.94 |
