# Factor mine action — `union_news_g_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ news_g, no 🚨

Cash book **-4.81%** ($9,519) · signal-only (no cash/fees) was -3.60%. Starts YES **6/17**. Fills 86 · skips 129 · realized $-659.53.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $0.78.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-14 | +5.50 | $10,000.00 | — | TLN, VST, NRG, ANGX, ARX, MH, HLIT | — | $1,560.49 | $8,550.18 | $10,110.67 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94 | BUY TLN x3 @ 359.83; BUY VST x8 @ 146.90; BUY NRG x10 @ 120.00; BUY ANGX x290 @ 4.31; BUY ARX x63 @ 19.57; BUY MH x92 @ 13.55; BUY HLIT x94 @ 13.18 |
| 2026-08-17 | +2.25 | $1,560.49 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94 | DVN, EOG, FANG, CELC, OUST | — | $212.20 | $9,847.63 | $10,059.83 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6 | BUY DVN x6 @ 46.18; BUY EOG x2 @ 142.77; BUY FANG x1 @ 202.70; BUY CELC x3 @ 92.99; BUY OUST x6 @ 49.00 |
| 2026-08-18 | -6.20 | $212.20 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6 | — | — | $212.20 | $9,607.24 | $9,819.43 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6 | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $212.20 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6 | — | TLN, VST, NRG, ARX, MH, HLIT | $7,093.29 | $2,651.96 | $9,745.25 | ANGX×290, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6 | SELL TLN (dropped from list after 3 sess (min 3)); SELL VST (dropped from list after 3 sess (min 3)); SELL NRG (dropped from list after 3 sess (min 3)); SELL ARX (dropped from list after 3 sess (min 3)); SELL MH (dropped from list after 3 sess (min 3)); SELL HLIT (dropped from list after 3 sess (min 3)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $7,093.29 | ANGX×290, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6 | BHP, MRNA, HUMA, BTGO, ZLAB, CRSP, APA, AUTL | ANGX, DVN, EOG, FANG, CELC, OUST | $90.86 | $9,412.30 | $9,503.17 | BHP×13, MRNA×8, HUMA×1721, BTGO×184, ZLAB×45, CRSP×20, APA×27, AUTL×492 | SELL ANGX (dropped from list after 4 sess (min 3)); SELL DVN (dropped from list after 3 sess (min 3)); SELL EOG (dropped from list after 3 sess (min 3)); SELL FANG (dropped from list after 3 sess (min 3)); SELL CELC (dropped from list after 3 sess (min 3)); SELL OUST (dropped from list after 3 sess (min 3)); BUY BHP x13 @ 91.01; BUY MRNA x8 @ 150.14; BUY HUMA x1721 @ 0.71; BUY BTGO x184 @ 6.61; BUY ZLAB x45 @ 26.57; BUY CRSP x20 @ 58.73; BUY APA x27 @ 44.76; BUY AUTL x492 @ 2.47 |
| 2026-08-21 | +3.25 | $90.86 | BHP×13, MRNA×8, HUMA×1721, BTGO×184, ZLAB×45, CRSP×20, APA×27, AUTL×492 | MARA, BTDR, HIVE | — | $54.73 | $9,538.32 | $9,593.06 | BHP×13, MRNA×8, HUMA×1721, BTGO×184, ZLAB×45, CRSP×20, APA×27, AUTL×492, MARA×1, BTDR×1, HIVE×4 | BUY MARA x1 @ 11.70; BUY BTDR x1 @ 11.10; BUY HIVE x4 @ 3.24 |
| 2026-08-24 | -5.17 | $54.73 | BHP×13, MRNA×8, HUMA×1721, BTGO×184, ZLAB×45, CRSP×20, APA×27, AUTL×492, MARA×1, BTDR×1, HIVE×4 | — | — | $54.73 | $9,434.60 | $9,489.33 | BHP×13, MRNA×8, HUMA×1721, BTGO×184, ZLAB×45, CRSP×20, APA×27, AUTL×492, MARA×1, BTDR×1, HIVE×4 | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $54.73 | BHP×13, MRNA×8, HUMA×1721, BTGO×184, ZLAB×45, CRSP×20, APA×27, AUTL×492, MARA×1, BTDR×1, HIVE×4 | RUM, EZPW, REAX, TRLV, VIRT, HOOD, ZYME, BKKT | BHP, MRNA, HUMA, BTGO, ZLAB, CRSP, APA, AUTL | $79.68 | $9,352.36 | $9,432.04 | MARA×1, BTDR×1, HIVE×4, RUM×125, EZPW×34, REAX×49, TRLV×106, VIRT×17, HOOD×11, ZYME×39, BKKT×142 | SELL BHP (dropped from list after 3 sess (min 3)); SELL MRNA (dropped from list after 3 sess (min 3)); SELL HUMA (dropped from list after 3 sess (min 3)); SELL BTGO (dropped from list after 3 sess (min 3)); SELL ZLAB (dropped from list after 3 sess (min 3)); SELL CRSP (dropped from list after 3 sess (min 3)); SELL APA (dropped from list after 3 sess (min 3)); SELL AUTL (dropped from list after 3 sess (min 3)); BUY RUM x125 @ 9.36; BUY EZPW x34 @ 34.48; BUY REAX x49 @ 24.00; BUY TRLV x106 @ 11.02; BUY VIRT x17 @ 66.29; BUY HOOD x11 @ 106.00; BUY ZYME x39 @ 29.87; BUY BKKT x142 @ 8.28 |
| 2026-08-26 | +2.02 | $79.68 | MARA×1, BTDR×1, HIVE×4, RUM×125, EZPW×34, REAX×49, TRLV×106, VIRT×17, HOOD×11, ZYME×39, BKKT×142 | — | — | $79.68 | $9,353.81 | $9,433.49 | MARA×1, BTDR×1, HIVE×4, RUM×125, EZPW×34, REAX×49, TRLV×106, VIRT×17, HOOD×11, ZYME×39, BKKT×142 | hold MARA,BTDR,HIVE,RUM,EZPW,REAX,TRLV,VIRT,HOOD,ZYME,BKKT |
| 2026-08-27 | — | $79.68 | MARA×1, BTDR×1, HIVE×4, RUM×125, EZPW×34, REAX×49, TRLV×106, VIRT×17, HOOD×11, ZYME×39, BKKT×142 | — | MARA, BTDR, HIVE | $113.66 | $9,462.86 | $9,576.52 | RUM×125, EZPW×34, REAX×49, TRLV×106, VIRT×17, HOOD×11, ZYME×39, BKKT×142 | SELL MARA (dropped from list after 4 sess (min 3)); SELL BTDR (dropped from list after 4 sess (min 3)); SELL HIVE (dropped from list after 4 sess (min 3)) |
| 2026-08-28 | +0.75 | $113.66 | RUM×125, EZPW×34, REAX×49, TRLV×106, VIRT×17, HOOD×11, ZYME×39, BKKT×142 | RRC, CAPR, SEDG, SMTC, OPTX, ERAS, BBWI | RUM, EZPW, REAX, TRLV, VIRT, HOOD, BKKT | $48.16 | $9,593.28 | $9,641.44 | ZYME×39, RRC×29, CAPR×131, SEDG×35, SMTC×8, OPTX×140, ERAS×62, BBWI×64 | SELL RUM (dropped from list after 3 sess (min 3)); SELL EZPW (dropped from list after 3 sess (min 3)); SELL REAX (dropped from list after 3 sess (min 3)); SELL TRLV (dropped from list after 3 sess (min 3)); SELL VIRT (dropped from list after 3 sess (min 3)); SELL HOOD (dropped from list after 3 sess (min 3)); SELL BKKT (dropped from list after 3 sess (min 3)); BUY RRC x29 @ 41.44; BUY CAPR x131 @ 9.19; BUY SEDG x35 @ 33.78; BUY SMTC x8 @ 149.40; BUY OPTX x140 @ 8.57; BUY ERAS x62 @ 19.30; BUY BBWI x64 @ 18.68 |
| 2026-08-31 | -5.85 | $48.16 | ZYME×39, RRC×29, CAPR×131, SEDG×35, SMTC×8, OPTX×140, ERAS×62, BBWI×64 | — | — | $48.16 | $9,227.76 | $9,275.92 | ZYME×39, RRC×29, CAPR×131, SEDG×35, SMTC×8, OPTX×140, ERAS×62, BBWI×64 | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $48.16 | ZYME×39, RRC×29, CAPR×131, SEDG×35, SMTC×8, OPTX×140, ERAS×62, BBWI×64 | — | ZYME | $1,189.52 | $8,148.57 | $9,338.09 | RRC×29, CAPR×131, SEDG×35, SMTC×8, OPTX×140, ERAS×62, BBWI×64 | SELL ZYME (dropped from list after 5 sess (min 3)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $1,189.52 | RRC×29, CAPR×131, SEDG×35, SMTC×8, OPTX×140, ERAS×62, BBWI×64 | — | RRC, CAPR, SEDG, SMTC, OPTX, ERAS, BBWI | $9,340.47 | $0.00 | $9,340.47 | — | SELL RRC (dropped from list after 3 sess (min 3)); SELL CAPR (dropped from list after 3 sess (min 3)); SELL SEDG (dropped from list after 3 sess (min 3)); SELL SMTC (dropped from list after 3 sess (min 3)); SELL OPTX (dropped from list after 3 sess (min 3)); SELL ERAS (dropped from list after 3 sess (min 3)); SELL BBWI (dropped from list after 3 sess (min 3)); hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $9,340.47 | — | MMED, CNXC, OPTX, TRLV, TXG, ZYME, FCX, AVGO | — | $194.04 | $9,308.64 | $9,502.68 | MMED×51, CNXC×36, OPTX×161, TRLV×99, TXG×19, ZYME×38, FCX×15, AVGO×3 | BUY MMED x51 @ 22.78; BUY CNXC x36 @ 31.80; BUY OPTX x161 @ 7.25; BUY TRLV x99 @ 11.78; BUY TXG x19 @ 60.24; BUY ZYME x38 @ 30.00; BUY FCX x15 @ 73.04; BUY AVGO x3 @ 369.68 |
| 2026-09-04 | — | $194.04 | MMED×51, CNXC×36, OPTX×161, TRLV×99, TXG×19, ZYME×38, FCX×15, AVGO×3 | BAK, AMTX | — | $0.78 | $9,518.63 | $9,519.41 | MMED×51, CNXC×36, OPTX×161, TRLV×99, TXG×19, ZYME×38, FCX×15, AVGO×3, BAK×49, AMTX×50 | BUY BAK x49 @ 1.95; BUY AMTX x50 @ 1.91 |

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
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 6 | $46.18 | $2.01 | — | $1,281.40 | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+6.7; leftover $312.10 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 2 | $142.77 | $2.00 | — | $993.87 | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.8; leftover $312.10 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 1 | $202.70 | $1.99 | — | $789.17 | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+8.3; leftover $312.10 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 3 | $92.99 | $2.00 | — | $508.20 | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.8; leftover $312.10 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 6 | $49.00 | $2.01 | — | $212.20 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ⚪; ret5=+12.2; leftover $312.10 | join🟡 sector🟢 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `TLN` | 3 | $321.00 | $2.02 | $-120.51 | $1,173.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `VST` | 8 | $140.74 | $2.03 | $-53.33 | $2,297.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NRG` | 10 | $116.20 | $2.04 | $-42.06 | $3,457.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 63 | $19.58 | $2.20 | $-3.75 | $4,688.36 | dropped from list after 3 sess (min 3) | join🟡 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `MH` | 92 | $13.01 | $2.29 | $-54.24 | $5,882.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `HLIT` | 94 | $12.90 | $2.30 | $-30.89 | $7,093.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ANGX` | 290 | $4.57 | $3.80 | $+67.86 | $8,414.79 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DVN` | 6 | $49.02 | $2.03 | $+13.00 | $8,706.89 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `EOG` | 2 | $151.45 | $2.02 | $+13.35 | $9,007.77 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `FANG` | 1 | $213.51 | $2.01 | $+6.80 | $9,219.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `CELC` | 3 | $92.90 | $2.02 | $-4.29 | $9,495.95 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `OUST` | 6 | $40.63 | $2.03 | $-54.26 | $9,737.70 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,552.54 | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1217.21 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $7,349.41 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1217.21 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1721 | $0.71 | $17.33 | — | $6,115.33 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1217.21 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 184 | $6.61 | $2.54 | — | $4,897.47 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1217.21 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 45 | $26.57 | $2.12 | — | $3,699.69 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.8; leftover $1217.21 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 20 | $58.73 | $2.05 | — | $2,523.04 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1217.21 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 27 | $44.76 | $2.07 | — | $1,312.45 | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+8.7; leftover $1217.21 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 492 | $2.47 | $6.35 | — | $90.86 | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1217.21 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 1 | $11.70 | $0.12 | — | $79.04 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $15.14 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 1 | $11.10 | $0.11 | — | $67.84 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+19.1; leftover $15.14 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 4 | $3.24 | $0.14 | — | $54.73 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; leftover $15.14 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.95 | $2.05 | $+60.14 | $1,300.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $141.19 | $2.03 | $-75.65 | $2,427.52 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HUMA` | 1721 | $0.67 | $16.99 | $-98.00 | $3,563.60 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BTGO` | 184 | $6.89 | $2.58 | $+47.32 | $4,828.78 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ZLAB` | 45 | $25.93 | $2.15 | $-33.07 | $5,993.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 20 | $57.00 | $2.07 | $-38.72 | $7,131.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 27 | $42.70 | $2.09 | $-59.78 | $8,282.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 492 | $2.32 | $6.44 | $-86.59 | $9,417.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 125 | $9.36 | $2.37 | — | $8,244.86 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; leftover $1177.15 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 34 | $34.48 | $2.09 | — | $7,070.45 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1177.15 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 49 | $24.00 | $2.14 | — | $5,892.31 | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=+10.0; leftover $1177.15 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `TRLV` | 106 | $11.02 | $2.31 | — | $4,721.88 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.0; leftover $1177.15 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VIRT` | 17 | $66.29 | $2.04 | — | $3,592.91 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+13.2; leftover $1177.15 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HOOD` | 11 | $106.00 | $2.02 | — | $2,424.89 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ⚪; ret5=+13.2; leftover $1177.15 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 39 | $29.87 | $2.11 | — | $1,257.85 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+14.1; leftover $1177.15 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BKKT` | 142 | $8.28 | $2.42 | — | $79.68 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+12.3; leftover $1177.15 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `MARA` | 1 | $11.56 | $0.14 | $-0.40 | $91.10 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `BTDR` | 1 | $11.05 | $0.13 | $-0.29 | $102.01 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `HIVE` | 4 | $2.95 | $0.15 | $-1.45 | $113.66 | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RUM` | 125 | $9.51 | $2.40 | $+13.99 | $1,300.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 34 | $33.50 | $2.11 | $-37.52 | $2,436.91 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `REAX` | 49 | $25.91 | $2.16 | $+89.30 | $3,704.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `TRLV` | 106 | $11.38 | $2.34 | $+33.52 | $4,908.28 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `VIRT` | 17 | $65.42 | $2.06 | $-18.89 | $6,018.36 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `HOOD` | 11 | $110.70 | $2.04 | $+47.63 | $7,234.02 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `BKKT` | 142 | $8.50 | $2.45 | $+26.37 | $8,438.57 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 29 | $41.44 | $2.08 | — | $7,234.73 | union ∩ news_g, no 🚨; gate news=good; list flatten; ret5=+1.8; leftover $1205.51 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 131 | $9.19 | $2.38 | — | $6,028.46 | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1205.51 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 35 | $33.78 | $2.10 | — | $4,844.06 | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1205.51 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $3,646.85 | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1205.51 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 140 | $8.57 | $2.41 | — | $2,444.64 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-3.4; leftover $1205.51 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 62 | $19.30 | $2.18 | — | $1,245.86 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-4.1; leftover $1205.51 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 64 | $18.68 | $2.18 | — | $48.16 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+0.2; leftover $1205.51 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `ZYME` | 39 | $29.32 | $2.13 | $-25.68 | $1,189.52 | dropped from list after 5 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 29 | $41.94 | $2.10 | $+10.33 | $2,403.68 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CAPR` | 131 | $10.77 | $2.42 | $+202.18 | $3,812.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 35 | $31.87 | $2.12 | $-71.06 | $4,925.47 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $127.63 | $2.03 | $-178.21 | $5,944.47 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `OPTX` | 140 | $7.94 | $2.44 | $-93.05 | $7,053.63 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ERAS` | 62 | $17.58 | $2.20 | $-111.01 | $8,141.39 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBWI` | 64 | $18.77 | $2.20 | $+1.38 | $9,340.47 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 51 | $22.78 | $2.14 | — | $8,176.55 | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1167.56 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 36 | $31.80 | $2.10 | — | $7,029.65 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+3.7; leftover $1167.56 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 161 | $7.25 | $2.47 | — | $5,859.93 | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-3.4; leftover $1167.56 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `TRLV` | 99 | $11.78 | $2.29 | — | $4,691.42 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.0; leftover $1167.56 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `TXG` | 19 | $60.24 | $2.05 | — | $3,544.81 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+16.1; leftover $1167.56 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ZYME` | 38 | $30.00 | $2.10 | — | $2,402.71 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ⚪; ret5=+14.1; leftover $1167.56 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟡 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FCX` | 15 | $73.04 | $2.04 | — | $1,305.07 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.3; leftover $1167.56 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $369.68 | $2.00 | — | $194.04 | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-6.2; leftover $1167.56 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 49 | $1.95 | $1.10 | — | $97.38 | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $97.02 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `AMTX` | 50 | $1.91 | $1.10 | — | $0.78 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+16.9; leftover $97.02 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1250.00 < 1 share @ 1646.93 |
| 2026-08-17 | `TLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `VST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NRG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HLIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NRG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HLIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `EOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `FANG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `CELC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OUST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `GO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `EOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `FANG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `CELC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OUST` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-08-21 | `AU` | cash | leftover split 15.14 < 1 share @ 119.43 |
| 2026-08-21 | `FUTU` | cash | leftover split 15.14 < 1 share @ 115.18 |
| 2026-08-21 | `DE` | cash | leftover split 15.14 < 1 share @ 623.26 |
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
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
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
| 2026-08-26 | `VIRT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FLNC` | no_price | no 09:30 open |
| 2026-08-26 | `CAPR` | no_price | no 09:30 open |
| 2026-08-26 | `FWRD` | no_price | no 09:30 open |
| 2026-08-26 | `FCX` | no_price | no 09:30 open |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `REAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `TRLV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `VIRT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HOOD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZYME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BKKT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 18.94 < 1 share @ 40.72 |
| 2026-08-27 | `ACMR` | cash | leftover split 18.94 < 1 share @ 80.97 |
| 2026-08-27 | `MU` | cash | leftover split 18.94 < 1 share @ 925.74 |
| 2026-08-27 | `ASML` | cash | leftover split 18.94 < 1 share @ 1746.33 |
| 2026-08-27 | `LRCX` | cash | leftover split 18.94 < 1 share @ 314.61 |
| 2026-08-27 | `NVDA` | cash | leftover split 18.94 < 1 share @ 212.64 |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `OPTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ERAS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BBWI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HOOD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BKKT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `OPTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ERAS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BBWI` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CNXC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `OPTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `TRLV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `MMED` | 51 | 2026-09-03 @ $22.78 | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1167.56 |
| `CNXC` | 36 | 2026-09-03 @ $31.80 | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+3.7; leftover $1167.56 |
| `OPTX` | 161 | 2026-09-03 @ $7.25 | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-3.4; leftover $1167.56 |
| `TRLV` | 99 | 2026-09-03 @ $11.78 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.0; leftover $1167.56 |
| `TXG` | 19 | 2026-09-03 @ $60.24 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+16.1; leftover $1167.56 |
| `ZYME` | 38 | 2026-09-03 @ $30.00 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ⚪; ret5=+14.1; leftover $1167.56 |
| `FCX` | 15 | 2026-09-03 @ $73.04 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.3; leftover $1167.56 |
| `AVGO` | 3 | 2026-09-03 @ $369.68 | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-6.2; leftover $1167.56 |
| `BAK` | 49 | 2026-09-04 @ $1.95 | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $97.02 |
| `AMTX` | 50 | 2026-09-04 @ $1.91 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+16.9; leftover $97.02 |
