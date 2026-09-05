# Factor mine action — `union_news_g_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ news_g, no 🚨

Cash book **-0.31%** ($9,969) · signal-only (no cash/fees) was -0.12%. Starts YES **2/17**. Fills 107 · skips 51 · realized $+120.57.

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

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-14 | +5.50 | $10,000.00 | — | TLN, VST, NRG, ANGX, ARX, MH, HLIT | — | $1,560.49 | $8,550.18 | $10,110.67 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94 | BUY TLN x3 @ 359.83; BUY VST x8 @ 146.90; BUY NRG x10 @ 120.00; BUY ANGX x290 @ 4.31; BUY ARX x63 @ 19.57; BUY MH x92 @ 13.55; BUY HLIT x94 @ 13.18 |
| 2026-08-17 | +2.25 | $1,560.49 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94 | DVN, EOG, FANG, CELC, OUST | TLN, VST, NRG, ANGX, ARX, MH, HLIT | $165.17 | $10,116.65 | $10,281.82 | DVN×44, EOG×14, FANG×10, CELC×21, OUST×41 | SELL TLN (dropped from list after 1 sess (min 1)); SELL VST (dropped from list after 1 sess (min 1)); SELL NRG (dropped from list after 1 sess (min 1)); SELL ANGX (dropped from list after 1 sess (min 1)); SELL ARX (dropped from list after 1 sess (min 1)); SELL MH (dropped from list after 1 sess (min 1)); SELL HLIT (dropped from list after 1 sess (min 1)); BUY DVN x44 @ 46.18; BUY EOG x14 @ 142.77; BUY FANG x10 @ 202.70; BUY CELC x21 @ 92.99; BUY OUST x41 @ 49.00 |
| 2026-08-18 | -6.20 | $165.17 | DVN×44, EOG×14, FANG×10, CELC×21, OUST×41 | — | DVN, EOG, FANG, CELC, OUST | $10,217.23 | $0.00 | $10,217.23 | — | SELL DVN (dropped from list after 1 sess (min 1)); SELL EOG (dropped from list after 1 sess (min 1)); SELL FANG (dropped from list after 1 sess (min 1)); SELL CELC (dropped from list after 1 sess (min 1)); SELL OUST (dropped from list after 1 sess (min 1)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $10,217.23 | — | — | — | $10,217.23 | $0.00 | $10,217.23 | — | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $10,217.23 | — | BHP, MRNA, HUMA, BTGO, ZLAB, CRSP, APA, AUTL | — | $113.67 | $9,865.29 | $9,978.95 | BHP×14, MRNA×8, HUMA×1806, BTGO×193, ZLAB×48, CRSP×21, APA×28, AUTL×517 | BUY BHP x14 @ 91.01; BUY MRNA x8 @ 150.14; BUY HUMA x1806 @ 0.71; BUY BTGO x193 @ 6.61; BUY ZLAB x48 @ 26.57; BUY CRSP x21 @ 58.73; BUY APA x28 @ 44.76; BUY AUTL x517 @ 2.47 |
| 2026-08-21 | +3.25 | $113.67 | BHP×14, MRNA×8, HUMA×1806, BTGO×193, ZLAB×48, CRSP×21, APA×28, AUTL×517 | AU, FUTU, DE, MARA, BTDR, HIVE | BHP, MRNA, HUMA, BTGO, ZLAB, APA | $183.88 | $9,904.28 | $10,088.16 | CRSP×21, AUTL×517, AU×10, FUTU×10, DE×2, MARA×107, BTDR×113, HIVE×388 | SELL BHP (dropped from list after 1 sess (min 1)); SELL MRNA (dropped from list after 1 sess (min 1)); SELL HUMA (dropped from list after 1 sess (min 1)); SELL BTGO (dropped from list after 1 sess (min 1)); SELL ZLAB (dropped from list after 1 sess (min 1)); SELL APA (dropped from list after 1 sess (min 1)); BUY AU x10 @ 119.43; BUY FUTU x10 @ 115.18; BUY DE x2 @ 623.26; BUY MARA x107 @ 11.70; BUY BTDR x113 @ 11.10; BUY HIVE x388 @ 3.24 |
| 2026-08-24 | -5.17 | $183.88 | CRSP×21, AUTL×517, AU×10, FUTU×10, DE×2, MARA×107, BTDR×113, HIVE×388 | — | AUTL, AU, FUTU, DE, MARA, BTDR, HIVE | $8,753.17 | $1,195.11 | $9,948.28 | CRSP×21 | SELL AUTL (dropped from list after 2 sess (min 1)); SELL AU (dropped from list after 1 sess (min 1)); SELL FUTU (dropped from list after 1 sess (min 1)); SELL DE (dropped from list after 1 sess (min 1)); SELL MARA (dropped from list after 1 sess (min 1)); SELL BTDR (dropped from list after 1 sess (min 1)); SELL HIVE (dropped from list after 1 sess (min 1)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $8,753.17 | CRSP×21 | RUM, EZPW, REAX, TRLV, VIRT, HOOD, ZYME, BKKT | CRSP | $169.59 | $9,760.13 | $9,929.72 | RUM×132, EZPW×36, REAX×51, TRLV×112, VIRT×18, HOOD×11, ZYME×41, BKKT×150 | SELL CRSP (dropped from list after 3 sess (min 1)); BUY RUM x132 @ 9.36; BUY EZPW x36 @ 34.48; BUY REAX x51 @ 24.00; BUY TRLV x112 @ 11.02; BUY VIRT x18 @ 66.29; BUY HOOD x11 @ 106.00; BUY ZYME x41 @ 29.87; BUY BKKT x150 @ 8.28 |
| 2026-08-26 | +2.02 | $169.59 | RUM×132, EZPW×36, REAX×51, TRLV×112, VIRT×18, HOOD×11, ZYME×41, BKKT×150 | — | — | $169.59 | $9,760.93 | $9,930.52 | RUM×132, EZPW×36, REAX×51, TRLV×112, VIRT×18, HOOD×11, ZYME×41, BKKT×150 | hold RUM,EZPW,REAX,TRLV,VIRT,HOOD,ZYME,BKKT |
| 2026-08-27 | — | $169.59 | RUM×132, EZPW×36, REAX×51, TRLV×112, VIRT×18, HOOD×11, ZYME×41, BKKT×150 | RRC, ACMR, MU, LRCX, NVDA | RUM, EZPW, REAX, TRLV, VIRT, HOOD, ZYME, BKKT | $2,860.38 | $7,256.17 | $10,116.55 | RRC×41, ACMR×20, MU×1, LRCX×5, NVDA×7 | SELL RUM (dropped from list after 2 sess (min 1)); SELL EZPW (dropped from list after 2 sess (min 1)); SELL REAX (dropped from list after 2 sess (min 1)); SELL TRLV (dropped from list after 2 sess (min 1)); SELL VIRT (dropped from list after 2 sess (min 1)); SELL HOOD (dropped from list after 2 sess (min 1)); SELL ZYME (dropped from list after 2 sess (min 1)); SELL BKKT (dropped from list after 2 sess (min 1)); BUY RRC x41 @ 40.72; BUY ACMR x20 @ 80.97; BUY MU x1 @ 925.74; BUY LRCX x5 @ 314.61; BUY NVDA x7 @ 212.64 |
| 2026-08-28 | +0.75 | $2,860.38 | RRC×41, ACMR×20, MU×1, LRCX×5, NVDA×7 | CAPR, SEDG, SMTC, OPTX, ERAS, BBWI, ZYME | ACMR, MU, LRCX, NVDA | $99.58 | $10,268.94 | $10,368.52 | RRC×41, CAPR×133, SEDG×36, SMTC×8, OPTX×143, ERAS×63, BBWI×65, ZYME×41 | SELL ACMR (dropped from list after 1 sess (min 1)); SELL MU (dropped from list after 1 sess (min 1)); SELL LRCX (dropped from list after 1 sess (min 1)); SELL NVDA (dropped from list after 1 sess (min 1)); BUY CAPR x133 @ 9.19; BUY SEDG x36 @ 33.78; BUY SMTC x8 @ 149.40; BUY OPTX x143 @ 8.57; BUY ERAS x63 @ 19.30; BUY BBWI x65 @ 18.68; BUY ZYME x41 @ 29.33 |
| 2026-08-31 | -5.85 | $99.58 | RRC×41, CAPR×133, SEDG×36, SMTC×8, OPTX×143, ERAS×63, BBWI×65, ZYME×41 | — | RRC, CAPR, SEDG, SMTC, OPTX, ERAS, BBWI | $8,823.92 | $1,159.07 | $9,982.99 | ZYME×41 | SELL RRC (dropped from list after 2 sess (min 1)); SELL CAPR (dropped from list after 1 sess (min 1)); SELL SEDG (dropped from list after 1 sess (min 1)); SELL SMTC (dropped from list after 1 sess (min 1)); SELL OPTX (dropped from list after 1 sess (min 1)); SELL ERAS (dropped from list after 1 sess (min 1)); SELL BBWI (dropped from list after 1 sess (min 1)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $8,823.92 | ZYME×41 | — | ZYME | $10,023.91 | $0.00 | $10,023.91 | — | SELL ZYME (dropped from list after 2 sess (min 1)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $10,023.91 | — | — | — | $10,023.91 | $0.00 | $10,023.91 | — | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $10,023.91 | — | MMED, CNXC, OPTX, TRLV, TXG, ZYME, FCX, AVGO | — | $232.33 | $9,968.11 | $10,200.44 | MMED×55, CNXC×39, OPTX×172, TRLV×106, TXG×20, ZYME×41, FCX×17, AVGO×3 | BUY MMED x55 @ 22.78; BUY CNXC x39 @ 31.80; BUY OPTX x172 @ 7.25; BUY TRLV x106 @ 11.78; BUY TXG x20 @ 60.24; BUY ZYME x41 @ 30.00; BUY FCX x17 @ 73.04; BUY AVGO x3 @ 369.68 |
| 2026-09-04 | — | $232.33 | MMED×55, CNXC×39, OPTX×172, TRLV×106, TXG×20, ZYME×41, FCX×17, AVGO×3 | BAK, AMTX | MMED, CNXC, OPTX, TRLV, AVGO | $1.32 | $9,968.11 | $9,969.43 | TXG×20, ZYME×41, FCX×17, BAK×1650, AMTX×1663 | SELL MMED (dropped from list after 1 sess (min 1)); SELL CNXC (dropped from list after 1 sess (min 1)); SELL OPTX (dropped from list after 1 sess (min 1)); SELL TRLV (dropped from list after 1 sess (min 1)); SELL AVGO (dropped from list after 1 sess (min 1)); BUY BAK x1650 @ 1.95; BUY AMTX x1663 @ 1.91 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $8,918.51 | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,741.30 | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+3.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,539.28 | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $5,285.64 | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $4,050.55 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $2,801.68 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $1,560.49 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $2,662.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $3,855.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $5,127.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $6,457.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $7,687.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 92 | $13.16 | $2.29 | $-40.44 | $8,896.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 94 | $13.84 | $2.30 | $+57.47 | $10,195.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 44 | $46.18 | $2.12 | — | $8,160.96 | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+6.7; leftover $2039.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 14 | $142.77 | $2.03 | — | $6,160.14 | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.8; leftover $2039.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 10 | $202.70 | $2.02 | — | $4,131.12 | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+8.3; leftover $2039.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 21 | $92.99 | $2.05 | — | $2,176.28 | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.8; leftover $2039.00 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 41 | $49.00 | $2.11 | — | $165.17 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ⚪; ret5=+12.2; leftover $2039.00 | join🟡 sector🟢 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 44 | $48.00 | $2.15 | $+75.81 | $2,275.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 14 | $148.04 | $2.06 | $+69.69 | $4,345.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 10 | $208.93 | $2.05 | $+58.23 | $6,432.77 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 21 | $92.38 | $2.08 | $-16.94 | $8,370.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 41 | $45.09 | $2.14 | $-164.56 | $10,217.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $8,941.05 | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1277.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $7,737.92 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1277.15 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1806 | $0.71 | $18.19 | — | $6,442.89 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1277.15 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 193 | $6.61 | $2.57 | — | $5,165.56 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1277.15 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 48 | $26.57 | $2.13 | — | $3,888.06 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.8; leftover $1277.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $2,652.68 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1277.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 28 | $44.76 | $2.07 | — | $1,397.33 | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+8.7; leftover $1277.15 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 517 | $2.47 | $6.67 | — | $113.67 | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1277.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 14 | $95.72 | $2.05 | $+61.86 | $1,451.70 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $2,514.54 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 1806 | $0.67 | $17.90 | $-95.68 | $3,713.89 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 193 | $6.95 | $2.61 | $+61.40 | $5,052.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 48 | $26.25 | $2.15 | $-19.65 | $6,310.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 28 | $44.52 | $2.09 | $-10.89 | $7,554.94 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $6,358.62 | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1259.16 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 10 | $115.18 | $2.02 | — | $5,204.80 | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1259.16 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $3,956.28 | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1259.16 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 107 | $11.70 | $2.31 | — | $2,702.07 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1259.16 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 113 | $11.10 | $2.33 | — | $1,446.00 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+19.1; leftover $1259.16 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 388 | $3.24 | $5.01 | — | $183.88 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; leftover $1259.16 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 517 | $2.36 | $6.77 | $-70.30 | $1,397.23 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.50 | $2.04 | $+6.64 | $2,600.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 10 | $120.87 | $2.04 | $+52.84 | $3,806.85 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.62 | $2.02 | $+56.71 | $5,112.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 107 | $11.18 | $2.34 | $-60.29 | $6,306.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTDR` | 113 | $11.49 | $2.36 | $+39.95 | $7,602.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 388 | $2.98 | $5.08 | $-110.96 | $8,753.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.00 | $2.07 | $-40.46 | $9,948.10 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 132 | $9.36 | $2.39 | — | $8,710.19 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; leftover $1243.51 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 36 | $34.48 | $2.10 | — | $7,466.81 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1243.51 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 51 | $24.00 | $2.14 | — | $6,240.67 | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=+10.0; leftover $1243.51 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `TRLV` | 112 | $11.02 | $2.33 | — | $5,004.10 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.0; leftover $1243.51 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VIRT` | 18 | $66.29 | $2.04 | — | $3,808.84 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+13.2; leftover $1243.51 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HOOD` | 11 | $106.00 | $2.02 | — | $2,640.82 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ⚪; ret5=+13.2; leftover $1243.51 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 41 | $29.87 | $2.11 | — | $1,414.03 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+14.1; leftover $1243.51 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BKKT` | 150 | $8.28 | $2.44 | — | $169.59 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+12.3; leftover $1243.51 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `RUM` | 132 | $10.07 | $2.42 | $+88.92 | $1,496.42 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `EZPW` | 36 | $35.70 | $2.12 | $+39.70 | $2,779.50 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `REAX` | 51 | $26.61 | $2.16 | $+128.80 | $4,134.44 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 112 | $11.22 | $2.35 | $+17.72 | $5,388.73 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `VIRT` | 18 | $64.92 | $2.06 | $-28.77 | $6,555.23 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HOOD` | 11 | $110.11 | $2.04 | $+41.14 | $7,764.39 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ZYME` | 41 | $27.56 | $2.13 | $-98.96 | $8,892.22 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BKKT` | 150 | $8.38 | $2.48 | $+10.09 | $10,146.74 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 41 | $40.72 | $2.11 | — | $8,475.11 | union ∩ news_g, no 🚨; gate news=good; list flatten; ret5=+1.8; leftover $1691.12 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 20 | $80.97 | $2.05 | — | $6,853.66 | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=-1.3; leftover $1691.12 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $925.74 | $1.99 | — | $5,925.93 | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=-0.5; leftover $1691.12 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 5 | $314.61 | $2.00 | — | $4,350.87 | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=-5.5; leftover $1691.12 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 7 | $212.64 | $2.01 | — | $2,860.38 | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=-4.6; leftover $1691.12 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 20 | $81.65 | $2.07 | $+9.48 | $4,491.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $967.01 | $2.01 | $+37.26 | $5,456.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 5 | $318.88 | $2.03 | $+17.32 | $7,048.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 7 | $222.86 | $2.03 | $+67.50 | $8,606.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 133 | $9.19 | $2.39 | — | $7,382.01 | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1229.52 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 36 | $33.78 | $2.10 | — | $6,163.83 | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1229.52 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $4,966.61 | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1229.52 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 143 | $8.57 | $2.42 | — | $3,738.69 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-3.4; leftover $1229.52 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 63 | $19.30 | $2.18 | — | $2,520.61 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-4.1; leftover $1229.52 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 65 | $18.68 | $2.19 | — | $1,304.22 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+0.2; leftover $1229.52 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 41 | $29.33 | $2.11 | — | $99.58 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1229.52 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 41 | $41.11 | $2.14 | $+11.74 | $1,782.95 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 133 | $9.44 | $2.42 | $+28.44 | $3,036.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 36 | $31.50 | $2.12 | $-86.30 | $4,167.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 8 | $133.04 | $2.03 | $-134.93 | $5,230.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 143 | $8.52 | $2.45 | $-12.02 | $6,446.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 63 | $17.90 | $2.20 | $-92.58 | $7,571.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 65 | $19.30 | $2.21 | $+35.91 | $8,823.92 | dropped from list after 1 sess (min 1) | — |
| 2026-09-01 09:30 ET | **SELL** | `ZYME` | 41 | $29.32 | $2.13 | $-4.66 | $10,023.91 | dropped from list after 2 sess (min 1) | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 55 | $22.78 | $2.15 | — | $8,768.85 | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1252.99 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 39 | $31.80 | $2.11 | — | $7,526.55 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+3.7; leftover $1252.99 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 172 | $7.25 | $2.51 | — | $6,277.04 | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-3.4; leftover $1252.99 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `TRLV` | 106 | $11.78 | $2.31 | — | $5,026.05 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.0; leftover $1252.99 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `TXG` | 20 | $60.24 | $2.05 | — | $3,819.20 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+16.1; leftover $1252.99 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ZYME` | 41 | $30.00 | $2.11 | — | $2,587.09 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ⚪; ret5=+14.1; leftover $1252.99 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟡 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FCX` | 17 | $73.04 | $2.04 | — | $1,343.37 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.3; leftover $1252.99 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $369.68 | $2.00 | — | $232.33 | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-6.2; leftover $1252.99 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 55 | $23.88 | $2.18 | $+56.17 | $1,543.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 39 | $32.88 | $2.13 | $+37.89 | $2,823.75 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 172 | $7.59 | $2.54 | $+53.43 | $4,126.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `TRLV` | 106 | $11.89 | $2.34 | $+7.02 | $5,384.69 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $351.74 | $2.02 | $-57.84 | $6,437.89 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 1650 | $1.95 | $21.29 | — | $3,199.10 | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $3218.94 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `AMTX` | 1663 | $1.91 | $21.45 | — | $1.32 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+16.9; leftover $3218.94 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |

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
| 2026-09-01 | `SSRM` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEM` | hard_red | hard-red S=-6.30 sit; no new buys |
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
