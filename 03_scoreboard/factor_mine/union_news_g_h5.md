# Factor mine action — `union_news_g_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ news_g hold 5, no 🚨

Cash book **-10.92%** ($8,908) · signal-only (no cash/fees) was +152.34%. Starts YES **4/17**. Fills 81 · skips 189 · realized $-876.73.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $1.94.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-14 | +5.50 | $10,000.00 | — | TLN, VST, NRG, ANGX, ARX, MH, HLIT | — | $1,560.49 | $8,550.18 | $10,110.67 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94 | BUY TLN x3 @ 359.83; BUY VST x8 @ 146.90; BUY NRG x10 @ 120.00; BUY ANGX x290 @ 4.31; BUY ARX x63 @ 19.57; BUY MH x92 @ 13.55; BUY HLIT x94 @ 13.18 |
| 2026-08-17 | +2.25 | $1,560.49 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94 | DVN, EOG, FANG, CELC, OUST | — | $212.20 | $9,847.63 | $10,059.83 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6 | BUY DVN x6 @ 46.18; BUY EOG x2 @ 142.77; BUY FANG x1 @ 202.70; BUY CELC x3 @ 92.99; BUY OUST x6 @ 49.00 |
| 2026-08-18 | -6.20 | $212.20 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6 | — | — | $212.20 | $9,607.24 | $9,819.43 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6 | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $212.20 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6 | — | — | $212.20 | $9,593.10 | $9,805.29 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6 | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $212.20 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6 | HUMA, BTGO, AUTL | — | $133.99 | $9,449.01 | $9,583.00 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6, HUMA×37, BTGO×4, AUTL×10 | BUY HUMA x37 @ 0.71; BUY BTGO x4 @ 6.61; BUY AUTL x10 @ 2.47 |
| 2026-08-21 | +3.25 | $133.99 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6, HUMA×37, BTGO×4, AUTL×10 | AU, CRSP, FUTU, DE, MARA, BTDR, HIVE | TLN, VST, NRG, ANGX, ARX, MH, HLIT | $709.20 | $8,934.20 | $9,643.41 | DVN×6, EOG×2, FANG×1, CELC×3, OUST×6, HUMA×37, BTGO×4, AUTL×10, AU×9, CRSP×19, FUTU×10, DE×1, MARA×100, BTDR×106, HIVE×363 | SELL TLN (dropped from list after 5 sess (min 5)); SELL VST (dropped from list after 5 sess (min 5)); SELL NRG (dropped from list after 5 sess (min 5)); SELL ANGX (dropped from list after 5 sess (min 5)); SELL ARX (dropped from list after 5 sess (min 5)); SELL MH (dropped from list after 5 sess (min 5)); SELL HLIT (dropped from list after 5 sess (min 5)); BUY AU x9 @ 119.43; BUY CRSP x19 @ 59.72; BUY FUTU x10 @ 115.18; BUY DE x1 @ 623.26; BUY MARA x100 @ 11.70; BUY BTDR x106 @ 11.10; BUY HIVE x363 @ 3.24 |
| 2026-08-24 | -5.17 | $709.20 | DVN×6, EOG×2, FANG×1, CELC×3, OUST×6, HUMA×37, BTGO×4, AUTL×10, AU×9, CRSP×19, FUTU×10, DE×1, MARA×100, BTDR×106, HIVE×363 | — | DVN, EOG, FANG, CELC, OUST | $2,007.92 | $7,454.00 | $9,461.92 | HUMA×37, BTGO×4, AUTL×10, AU×9, CRSP×19, FUTU×10, DE×1, MARA×100, BTDR×106, HIVE×363 | SELL DVN (dropped from list after 5 sess (min 5)); SELL EOG (dropped from list after 5 sess (min 5)); SELL FANG (dropped from list after 5 sess (min 5)); SELL CELC (dropped from list after 5 sess (min 5)); SELL OUST (dropped from list after 5 sess (min 5)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $2,007.92 | HUMA×37, BTGO×4, AUTL×10, AU×9, CRSP×19, FUTU×10, DE×1, MARA×100, BTDR×106, HIVE×363 | RUM, EZPW, REAX, TRLV, VIRT, HOOD, ZYME, BKKT | — | $126.29 | $9,300.10 | $9,426.39 | HUMA×37, BTGO×4, AUTL×10, AU×9, CRSP×19, FUTU×10, DE×1, MARA×100, BTDR×106, HIVE×363, RUM×26, EZPW×7, REAX×10, TRLV×22, VIRT×3, HOOD×2, ZYME×8, BKKT×30 | BUY RUM x26 @ 9.36; BUY EZPW x7 @ 34.48; BUY REAX x10 @ 24.00; BUY TRLV x22 @ 11.02; BUY VIRT x3 @ 66.29; BUY HOOD x2 @ 106.00; BUY ZYME x8 @ 29.87; BUY BKKT x30 @ 8.28 |
| 2026-08-26 | +2.02 | $126.29 | HUMA×37, BTGO×4, AUTL×10, AU×9, CRSP×19, FUTU×10, DE×1, MARA×100, BTDR×106, HIVE×363, RUM×26, EZPW×7, REAX×10, TRLV×22, VIRT×3, HOOD×2, ZYME×8, BKKT×30 | — | — | $126.29 | $9,265.72 | $9,392.01 | HUMA×37, BTGO×4, AUTL×10, AU×9, CRSP×19, FUTU×10, DE×1, MARA×100, BTDR×106, HIVE×363, RUM×26, EZPW×7, REAX×10, TRLV×22, VIRT×3, HOOD×2, ZYME×8, BKKT×30 | hold HUMA,BTGO,AUTL,AU,CRSP,FUTU,DE,MARA,BTDR,HIVE,RUM,EZPW,REAX,TRLV,VIRT,HOOD,ZYME,BKKT |
| 2026-08-27 | — | $126.29 | HUMA×37, BTGO×4, AUTL×10, AU×9, CRSP×19, FUTU×10, DE×1, MARA×100, BTDR×106, HIVE×363, RUM×26, EZPW×7, REAX×10, TRLV×22, VIRT×3, HOOD×2, ZYME×8, BKKT×30 | — | HUMA, BTGO, AUTL | $203.90 | $9,285.35 | $9,489.25 | AU×9, CRSP×19, FUTU×10, DE×1, MARA×100, BTDR×106, HIVE×363, RUM×26, EZPW×7, REAX×10, TRLV×22, VIRT×3, HOOD×2, ZYME×8, BKKT×30 | SELL HUMA (dropped from list after 5 sess (min 5)); SELL BTGO (dropped from list after 5 sess (min 5)); SELL AUTL (dropped from list after 5 sess (min 5)) |
| 2026-08-28 | +0.75 | $203.90 | AU×9, CRSP×19, FUTU×10, DE×1, MARA×100, BTDR×106, HIVE×363, RUM×26, EZPW×7, REAX×10, TRLV×22, VIRT×3, HOOD×2, ZYME×8, BKKT×30 | RRC, CAPR, SEDG, SMTC, OPTX, ERAS, BBWI | AU, CRSP, FUTU, DE, MARA, BTDR, HIVE | $115.59 | $9,510.02 | $9,625.61 | RUM×26, EZPW×7, REAX×10, TRLV×22, VIRT×3, HOOD×2, ZYME×8, BKKT×30, RRC×26, CAPR×119, SEDG×32, SMTC×7, OPTX×128, ERAS×56, BBWI×58 | SELL AU (dropped from list after 5 sess (min 5)); SELL CRSP (dropped from list after 5 sess (min 5)); SELL FUTU (dropped from list after 5 sess (min 5)); SELL DE (dropped from list after 5 sess (min 5)); SELL MARA (dropped from list after 5 sess (min 5)); SELL BTDR (dropped from list after 5 sess (min 5)); SELL HIVE (dropped from list after 5 sess (min 5)); BUY RRC x26 @ 41.44; BUY CAPR x119 @ 9.19; BUY SEDG x32 @ 33.78; BUY SMTC x7 @ 149.40; BUY OPTX x128 @ 8.57; BUY ERAS x56 @ 19.30; BUY BBWI x58 @ 18.68 |
| 2026-08-31 | -5.85 | $115.59 | RUM×26, EZPW×7, REAX×10, TRLV×22, VIRT×3, HOOD×2, ZYME×8, BKKT×30, RRC×26, CAPR×119, SEDG×32, SMTC×7, OPTX×128, ERAS×56, BBWI×58 | — | — | $115.59 | $9,150.47 | $9,266.06 | RUM×26, EZPW×7, REAX×10, TRLV×22, VIRT×3, HOOD×2, ZYME×8, BKKT×30, RRC×26, CAPR×119, SEDG×32, SMTC×7, OPTX×128, ERAS×56, BBWI×58 | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $115.59 | RUM×26, EZPW×7, REAX×10, TRLV×22, VIRT×3, HOOD×2, ZYME×8, BKKT×30, RRC×26, CAPR×119, SEDG×32, SMTC×7, OPTX×128, ERAS×56, BBWI×58 | — | RUM, EZPW, REAX, VIRT, HOOD, ZYME, BKKT | $1,629.41 | $7,619.93 | $9,249.34 | TRLV×22, RRC×26, CAPR×119, SEDG×32, SMTC×7, OPTX×128, ERAS×56, BBWI×58 | SELL RUM (dropped from list after 5 sess (min 5)); SELL EZPW (dropped from list after 5 sess (min 5)); SELL REAX (dropped from list after 5 sess (min 5)); SELL VIRT (dropped from list after 5 sess (min 5)); SELL HOOD (dropped from list after 5 sess (min 5)); SELL ZYME (dropped from list after 5 sess (min 5)); SELL BKKT (dropped from list after 5 sess (min 5)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $1,629.41 | TRLV×22, RRC×26, CAPR×119, SEDG×32, SMTC×7, OPTX×128, ERAS×56, BBWI×58 | — | — | $1,629.41 | $7,467.22 | $9,096.63 | TRLV×22, RRC×26, CAPR×119, SEDG×32, SMTC×7, OPTX×128, ERAS×56, BBWI×58 | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $1,629.41 | TRLV×22, RRC×26, CAPR×119, SEDG×32, SMTC×7, OPTX×128, ERAS×56, BBWI×58 | MMED, CNXC, TXG, ZYME, FCX | — | $384.30 | $8,786.34 | $9,170.64 | TRLV×22, RRC×26, CAPR×119, SEDG×32, SMTC×7, OPTX×128, ERAS×56, BBWI×58, MMED×11, CNXC×8, TXG×4, ZYME×9, FCX×3 | BUY MMED x11 @ 22.78; BUY CNXC x8 @ 31.80; BUY TXG x4 @ 60.24; BUY ZYME x9 @ 30.00; BUY FCX x3 @ 73.04 |
| 2026-09-04 | — | $384.30 | TRLV×22, RRC×26, CAPR×119, SEDG×32, SMTC×7, OPTX×128, ERAS×56, BBWI×58, MMED×11, CNXC×8, TXG×4, ZYME×9, FCX×3 | BAK, AMTX | TRLV, RRC, CAPR, SEDG, SMTC, OPTX, ERAS, BBWI | $1.94 | $8,905.72 | $8,907.66 | MMED×11, CNXC×8, TXG×4, ZYME×9, FCX×3, BAK×2020, AMTX×2034 | SELL TRLV (dropped from list after 8 sess (min 5)); SELL RRC (dropped from list after 5 sess (min 5)); SELL CAPR (dropped from list after 5 sess (min 5)); SELL SEDG (dropped from list after 5 sess (min 5)); SELL SMTC (dropped from list after 5 sess (min 5)); SELL OPTX (dropped from list after 5 sess (min 5)); SELL ERAS (dropped from list after 5 sess (min 5)); SELL BBWI (dropped from list after 5 sess (min 5)); BUY BAK x2020 @ 1.95; BUY AMTX x2034 @ 1.91 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $8,918.51 | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,741.30 | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten; 🔵; ret5=+3.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,539.28 | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $5,285.64 | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $4,050.55 | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $2,801.68 | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $1,560.49 | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 6 | $46.18 | $2.01 | — | $1,281.40 | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten; 🔵; ret5=+6.7; leftover $312.10 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 2 | $142.77 | $2.00 | — | $993.87 | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.8; leftover $312.10 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 1 | $202.70 | $1.99 | — | $789.17 | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten; 🔵; ret5=+8.3; leftover $312.10 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 3 | $92.99 | $2.00 | — | $508.20 | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.8; leftover $312.10 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 6 | $49.00 | $2.01 | — | $212.20 | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; ⚪; ret5=+12.2; leftover $312.10 | join🟡 sector🟢 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 37 | $0.71 | $0.37 | — | $185.66 | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $26.52 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 4 | $6.61 | $0.28 | — | $158.97 | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $26.52 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $133.99 | union ∩ news_g hold 5, no 🚨; gate news=good; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $26.52 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `TLN` | 3 | $318.52 | $2.02 | $-127.95 | $1,087.53 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `VST` | 8 | $139.99 | $2.03 | $-59.33 | $2,205.42 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `NRG` | 10 | $116.58 | $2.04 | $-38.26 | $3,369.18 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ANGX` | 290 | $4.43 | $3.80 | $+27.26 | $4,650.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $5,880.79 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `MH` | 92 | $12.87 | $2.29 | $-67.12 | $7,062.54 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `HLIT` | 94 | $12.48 | $2.30 | $-70.37 | $8,233.36 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 9 | $119.43 | $2.02 | — | $7,156.47 | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1176.19 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 19 | $59.72 | $2.05 | — | $6,019.75 | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1176.19 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 10 | $115.18 | $2.02 | — | $4,865.93 | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1176.19 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 1 | $623.26 | $1.99 | — | $4,240.67 | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1176.19 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 100 | $11.70 | $2.29 | — | $3,068.38 | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1176.19 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 106 | $11.10 | $2.31 | — | $1,890.00 | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; ret5=+19.1; leftover $1176.19 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 363 | $3.24 | $4.68 | — | $709.20 | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; leftover $1176.19 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DVN` | 6 | $48.84 | $2.03 | $+11.92 | $1,000.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `EOG` | 2 | $152.61 | $2.02 | $+15.67 | $1,303.42 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `FANG` | 1 | $209.47 | $2.01 | $+2.76 | $1,510.87 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `CELC` | 3 | $92.75 | $2.02 | $-4.74 | $1,787.11 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `OUST` | 6 | $37.14 | $2.03 | $-75.20 | $2,007.92 | dropped from list after 5 sess (min 5) | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 26 | $9.36 | $2.07 | — | $1,762.49 | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; leftover $250.99 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 7 | $34.48 | $2.01 | — | $1,519.12 | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $250.99 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 10 | $24.00 | $2.02 | — | $1,277.10 | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_mover; ret5=+10.0; leftover $250.99 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `TRLV` | 22 | $11.02 | $2.06 | — | $1,032.60 | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.0; leftover $250.99 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VIRT` | 3 | $66.29 | $1.99 | — | $831.74 | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; ret5=+13.2; leftover $250.99 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HOOD` | 2 | $106.00 | $2.00 | — | $617.74 | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; ⚪; ret5=+13.2; leftover $250.99 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 8 | $29.87 | $2.01 | — | $376.77 | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+14.1; leftover $250.99 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BKKT` | 30 | $8.28 | $2.08 | — | $126.29 | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+12.3; leftover $250.99 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `HUMA` | 37 | $0.71 | $0.39 | $-0.66 | $152.17 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BTGO` | 4 | $7.06 | $0.31 | $+1.23 | $180.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 10 | $2.41 | $0.29 | $-1.17 | $203.90 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 9 | $117.41 | $2.04 | $-22.23 | $1,258.55 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRSP` | 19 | $59.12 | $2.07 | $-15.51 | $2,379.77 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `FUTU` | 10 | $128.00 | $2.04 | $+124.14 | $3,657.73 | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `DE` | 1 | $628.82 | $2.01 | $+1.55 | $4,284.53 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `MARA` | 100 | $11.53 | $2.32 | $-21.61 | $5,435.22 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `BTDR` | 106 | $11.20 | $2.34 | $+6.49 | $6,620.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `HIVE` | 363 | $2.96 | $4.75 | $-111.08 | $7,689.81 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 26 | $41.44 | $2.07 | — | $6,610.30 | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten; ret5=+1.8; leftover $1098.54 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 119 | $9.19 | $2.35 | — | $5,514.34 | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1098.54 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 32 | $33.78 | $2.09 | — | $4,431.30 | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1098.54 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 7 | $149.40 | $2.01 | — | $3,383.49 | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1098.54 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 128 | $8.57 | $2.37 | — | $2,284.15 | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; ret5=-3.4; leftover $1098.54 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 56 | $19.30 | $2.16 | — | $1,201.19 | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; ret5=-4.1; leftover $1098.54 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 58 | $18.68 | $2.16 | — | $115.59 | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; ret5=+0.2; leftover $1098.54 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `RUM` | 26 | $8.90 | $2.09 | $-16.12 | $344.90 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `EZPW` | 7 | $32.05 | $2.03 | $-21.05 | $567.22 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `REAX` | 10 | $19.32 | $1.98 | $-50.80 | $758.44 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `VIRT` | 3 | $65.64 | $2.00 | $-5.94 | $953.36 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `HOOD` | 2 | $107.57 | $2.02 | $-0.87 | $1,166.48 | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `ZYME` | 8 | $29.32 | $2.03 | $-8.45 | $1,399.01 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `BKKT` | 30 | $7.75 | $2.10 | $-20.08 | $1,629.41 | dropped from list after 5 sess (min 5) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 11 | $22.78 | $2.02 | — | $1,376.81 | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $271.57 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 8 | $31.80 | $2.01 | — | $1,120.39 | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+3.7; leftover $271.57 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `TXG` | 4 | $60.24 | $2.00 | — | $877.43 | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; ret5=+16.1; leftover $271.57 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ZYME` | 9 | $30.00 | $2.02 | — | $605.41 | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; ⚪; ret5=+14.1; leftover $271.57 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟡 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FCX` | 3 | $73.04 | $2.00 | — | $384.30 | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.3; leftover $271.57 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `TRLV` | 22 | $11.89 | $2.08 | $+15.01 | $643.80 | dropped from list after 8 sess (min 5) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `RRC` | 26 | $42.43 | $2.09 | $+21.58 | $1,744.89 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `CAPR` | 119 | $9.83 | $2.38 | $+71.44 | $2,912.28 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SEDG` | 32 | $33.69 | $2.11 | $-7.07 | $3,988.26 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SMTC` | 7 | $133.10 | $2.03 | $-118.14 | $4,917.93 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 128 | $7.59 | $2.41 | $-130.22 | $5,887.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `ERAS` | 56 | $16.38 | $2.18 | $-167.86 | $6,802.14 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BBWI` | 58 | $18.59 | $2.18 | $-9.57 | $7,878.18 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 2020 | $1.95 | $26.06 | — | $3,913.12 | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $3939.09 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `AMTX` | 2034 | $1.91 | $26.24 | — | $1.94 | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; ret5=+16.9; leftover $3939.09 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1250.00 < 1 share @ 1646.93 |
| 2026-08-17 | `TLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `VST` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `NRG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `MH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `HLIT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `TLN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `VST` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `NRG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `MH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `HLIT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `EOG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `FANG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `CELC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `OUST` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `GO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `TLN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `VST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `NRG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ARX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `MH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `HLIT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `EOG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `FANG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `CELC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `OUST` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `TLN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `VST` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `NRG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ANGX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ARX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `MH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `HLIT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `DVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `EOG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `FANG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `CELC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `OUST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `BHP` | cash | leftover split 26.52 < 1 share @ 91.01 |
| 2026-08-20 | `MRNA` | cash | leftover split 26.52 < 1 share @ 150.14 |
| 2026-08-20 | `ZLAB` | cash | leftover split 26.52 < 1 share @ 26.57 |
| 2026-08-20 | `CRSP` | cash | leftover split 26.52 < 1 share @ 58.73 |
| 2026-08-20 | `APA` | cash | leftover split 26.52 < 1 share @ 44.76 |
| 2026-08-21 | `DVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `EOG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `FANG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `CELC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `OUST` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `HUMA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `BTGO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `HUMA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BTGO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `AU` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `FUTU` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `DE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `MARA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `BTDR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `HIVE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `HUMA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `BTGO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `AU` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRSP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `FUTU` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `DE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `MARA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `BTDR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `HIVE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-26 | `HUMA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BTGO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `AUTL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRSP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `FUTU` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `DE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `MARA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `BTDR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `HIVE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `RUM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `REAX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `VIRT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `FLNC` | no_price | no 09:30 open |
| 2026-08-26 | `CAPR` | no_price | no 09:30 open |
| 2026-08-26 | `FWRD` | no_price | no 09:30 open |
| 2026-08-26 | `FCX` | no_price | no 09:30 open |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRSP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `FUTU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `DE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `MARA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `BTDR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `HIVE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `REAX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `TRLV` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `VIRT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `HOOD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `ZYME` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `BKKT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 33.98 < 1 share @ 40.72 |
| 2026-08-27 | `ACMR` | cash | leftover split 33.98 < 1 share @ 80.97 |
| 2026-08-27 | `MU` | cash | leftover split 33.98 < 1 share @ 925.74 |
| 2026-08-27 | `ASML` | cash | leftover split 33.98 < 1 share @ 1746.33 |
| 2026-08-27 | `LRCX` | cash | leftover split 33.98 < 1 share @ 314.61 |
| 2026-08-27 | `NVDA` | cash | leftover split 33.98 < 1 share @ 212.64 |
| 2026-08-28 | `RUM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `EZPW` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `REAX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `TRLV` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `VIRT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `HOOD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `BKKT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `RUM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `EZPW` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `REAX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `TRLV` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `VIRT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `OPTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `ERAS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `BBWI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `CAPR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `OPTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `ERAS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `BBWI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLBT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TXG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SSRM` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEM` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `RRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `CAPR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `SEDG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `SMTC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `OPTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `ERAS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `BBWI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `POWW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TXG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZYME` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `RRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `CAPR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `SEDG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `SMTC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `ERAS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `BBWI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `AVGO` | cash | leftover split 271.57 < 1 share @ 369.68 |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CNXC` | min_hold | dropped but min-hold 1/5 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `MMED` | 11 | 2026-09-03 @ $22.78 | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $271.57 |
| `CNXC` | 8 | 2026-09-03 @ $31.80 | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+3.7; leftover $271.57 |
| `TXG` | 4 | 2026-09-03 @ $60.24 | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; ret5=+16.1; leftover $271.57 |
| `ZYME` | 9 | 2026-09-03 @ $30.00 | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; ⚪; ret5=+14.1; leftover $271.57 |
| `FCX` | 3 | 2026-09-03 @ $73.04 | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.3; leftover $271.57 |
| `BAK` | 2020 | 2026-09-04 @ $1.95 | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $3939.09 |
| `AMTX` | 2034 | 2026-09-04 @ $1.91 | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; ret5=+16.9; leftover $3939.09 |
