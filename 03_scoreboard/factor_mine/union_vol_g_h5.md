# Factor mine action — `union_vol_g_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ vol_g hold 5, no 🚨

Cash book **-10.12%** ($8,988) · signal-only (no cash/fees) was +7.06%. Starts YES **7/17**. Fills 73 · skips 177 · realized $-745.57.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `vol=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $326.26.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-14 | +5.50 | $10,000.00 | — | BTBT, BETR, ANGX, HYLN, ADUR, ARX, AIRO, NCMI | — | $10.28 | $9,787.54 | $9,797.82 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | BUY BTBT x833 @ 1.50; BUY BETR x84 @ 14.80; BUY ANGX x290 @ 4.31; BUY HYLN x299 @ 4.18; BUY ADUR x75 @ 16.50; BUY ARX x63 @ 19.57; BUY AIRO x112 @ 11.12; BUY NCMI x464 @ 2.69 |
| 2026-08-17 | +2.25 | $10.28 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | — | — | $10.28 | $9,799.38 | $9,809.66 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | hold BTBT,BETR,ANGX,HYLN,ADUR,ARX,AIRO,NCMI |
| 2026-08-18 | -6.20 | $10.28 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | — | — | $10.28 | $9,444.26 | $9,454.54 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $10.28 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | — | — | $10.28 | $9,264.99 | $9,275.27 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $10.28 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | — | — | $10.28 | $9,105.70 | $9,115.98 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | hold BTBT,BETR,ANGX,HYLN,ADUR,ARX,AIRO,NCMI |
| 2026-08-21 | +3.25 | $10.28 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | BTBT, BETR, ANGX, HYLN, ADUR, ARX, AIRO, NCMI | $151.04 | $9,252.06 | $9,403.10 | AU×9, AUPH×66, AEM×5, ARCT×103, AUTL×465, CRDL×596, CRSP×19, CYPH×871 | SELL BTBT (dropped from list after 5 sess (min 5)); SELL BETR (dropped from list after 5 sess (min 5)); SELL ANGX (dropped from list after 5 sess (min 5)); SELL HYLN (dropped from list after 5 sess (min 5)); SELL ADUR (dropped from list after 5 sess (min 5)); SELL ARX (dropped from list after 5 sess (min 5)); SELL AIRO (dropped from list after 5 sess (min 5)); SELL NCMI (dropped from list after 5 sess (min 5)); BUY AU x9 @ 119.43; BUY AUPH x66 @ 17.20; BUY AEM x5 @ 216.30; BUY ARCT x103 @ 11.13; BUY AUTL x465 @ 2.47; BUY CRDL x596 @ 1.93; BUY CRSP x19 @ 59.72; BUY CYPH x871 @ 1.32 |
| 2026-08-24 | -5.17 | $151.04 | AU×9, AUPH×66, AEM×5, ARCT×103, AUTL×465, CRDL×596, CRSP×19, CYPH×871 | — | — | $151.04 | $9,340.45 | $9,491.49 | AU×9, AUPH×66, AEM×5, ARCT×103, AUTL×465, CRDL×596, CRSP×19, CYPH×871 | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $151.04 | AU×9, AUPH×66, AEM×5, ARCT×103, AUTL×465, CRDL×596, CRSP×19, CYPH×871 | BMEA, NPWR, PUSA, ALVO, CAPR, ZURA, SUJA | — | $12.01 | $9,582.78 | $9,594.79 | AU×9, AUPH×66, AEM×5, ARCT×103, AUTL×465, CRDL×596, CRSP×19, CYPH×871, BMEA×13, NPWR×10, PUSA×5, ALVO×4, CAPR×3, ZURA×3, SUJA×2 | BUY BMEA x13 @ 1.62; BUY NPWR x10 @ 2.00; BUY PUSA x5 @ 3.70; BUY ALVO x4 @ 5.22; BUY CAPR x3 @ 6.79; BUY ZURA x3 @ 6.38; BUY SUJA x2 @ 8.79 |
| 2026-08-26 | +2.02 | $12.01 | AU×9, AUPH×66, AEM×5, ARCT×103, AUTL×465, CRDL×596, CRSP×19, CYPH×871, BMEA×13, NPWR×10, PUSA×5, ALVO×4, CAPR×3, ZURA×3, SUJA×2 | — | — | $12.01 | $9,569.85 | $9,581.86 | AU×9, AUPH×66, AEM×5, ARCT×103, AUTL×465, CRDL×596, CRSP×19, CYPH×871, BMEA×13, NPWR×10, PUSA×5, ALVO×4, CAPR×3, ZURA×3, SUJA×2 | hold AU,AUPH,AEM,ARCT,AUTL,CRDL,CRSP,CYPH,BMEA,NPWR,PUSA,ALVO,CAPR,ZURA,SUJA |
| 2026-08-27 | — | $12.01 | AU×9, AUPH×66, AEM×5, ARCT×103, AUTL×465, CRDL×596, CRSP×19, CYPH×871, BMEA×13, NPWR×10, PUSA×5, ALVO×4, CAPR×3, ZURA×3, SUJA×2 | — | — | $12.01 | $9,903.46 | $9,915.47 | AU×9, AUPH×66, AEM×5, ARCT×103, AUTL×465, CRDL×596, CRSP×19, CYPH×871, BMEA×13, NPWR×10, PUSA×5, ALVO×4, CAPR×3, ZURA×3, SUJA×2 | hold AU,AUPH,AEM,ARCT,AUTL,CRDL,CRSP,CYPH,BMEA,NPWR,PUSA,ALVO,CAPR,ZURA,SUJA |
| 2026-08-28 | +0.75 | $12.01 | AU×9, AUPH×66, AEM×5, ARCT×103, AUTL×465, CRDL×596, CRSP×19, CYPH×871, BMEA×13, NPWR×10, PUSA×5, ALVO×4, CAPR×3, ZURA×3, SUJA×2 | ANF, BHVN, BZ, SEDG, SMTC, URBN, ERAS | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $246.60 | $9,448.23 | $9,694.83 | BMEA×13, NPWR×10, PUSA×5, ALVO×4, CAPR×3, ZURA×3, SUJA×2, ANF×9, BHVN×82, BZ×75, SEDG×41, SMTC×9, URBN×16, ERAS×72 | SELL AU (dropped from list after 5 sess (min 5)); SELL AUPH (dropped from list after 5 sess (min 5)); SELL AEM (dropped from list after 5 sess (min 5)); SELL ARCT (dropped from list after 5 sess (min 5)); SELL AUTL (dropped from list after 5 sess (min 5)); SELL CRDL (dropped from list after 5 sess (min 5)); SELL CRSP (dropped from list after 5 sess (min 5)); SELL CYPH (dropped from list after 5 sess (min 5)); BUY ANF x9 @ 144.70; BUY BHVN x82 @ 16.95; BUY BZ x75 @ 18.50; BUY SEDG x41 @ 33.78; BUY SMTC x9 @ 149.40; BUY URBN x16 @ 82.70; BUY ERAS x72 @ 19.30 |
| 2026-08-31 | -5.85 | $246.60 | BMEA×13, NPWR×10, PUSA×5, ALVO×4, CAPR×3, ZURA×3, SUJA×2, ANF×9, BHVN×82, BZ×75, SEDG×41, SMTC×9, URBN×16, ERAS×72 | — | — | $246.60 | $9,154.44 | $9,401.04 | BMEA×13, NPWR×10, PUSA×5, ALVO×4, CAPR×3, ZURA×3, SUJA×2, ANF×9, BHVN×82, BZ×75, SEDG×41, SMTC×9, URBN×16, ERAS×72 | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $246.60 | BMEA×13, NPWR×10, PUSA×5, ALVO×4, CAPR×3, ZURA×3, SUJA×2, ANF×9, BHVN×82, BZ×75, SEDG×41, SMTC×9, URBN×16, ERAS×72 | — | BMEA, NPWR, PUSA, ALVO, CAPR, ZURA, SUJA | $391.44 | $8,876.39 | $9,267.83 | ANF×9, BHVN×82, BZ×75, SEDG×41, SMTC×9, URBN×16, ERAS×72 | SELL BMEA (dropped from list after 5 sess (min 5)); SELL NPWR (dropped from list after 5 sess (min 5)); SELL PUSA (dropped from list after 5 sess (min 5)); SELL ALVO (dropped from list after 5 sess (min 5)); SELL CAPR (dropped from list after 5 sess (min 5)); SELL ZURA (dropped from list after 5 sess (min 5)); SELL SUJA (dropped from list after 5 sess (min 5)) |
| 2026-09-02 | -3.83 | $391.44 | ANF×9, BHVN×82, BZ×75, SEDG×41, SMTC×9, URBN×16, ERAS×72 | — | — | $391.44 | $8,870.93 | $9,262.37 | ANF×9, BHVN×82, BZ×75, SEDG×41, SMTC×9, URBN×16, ERAS×72 | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $391.44 | ANF×9, BHVN×82, BZ×75, SEDG×41, SMTC×9, URBN×16, ERAS×72 | GPRO, FRVO, CRK, MMED, CTMX, CRDL | — | $114.32 | $9,143.75 | $9,258.07 | ANF×9, BHVN×82, BZ×75, SEDG×41, SMTC×9, URBN×16, ERAS×72, GPRO×40, FRVO×2, CRK×3, MMED×2, CTMX×13, CRDL×22 | BUY GPRO x40 @ 1.22; BUY FRVO x2 @ 18.40; BUY CRK x3 @ 15.70; BUY MMED x2 @ 22.78; BUY CTMX x13 @ 3.72; BUY CRDL x22 @ 2.16 |
| 2026-09-04 | — | $114.32 | ANF×9, BHVN×82, BZ×75, SEDG×41, SMTC×9, URBN×16, ERAS×72, GPRO×40, FRVO×2, CRK×3, MMED×2, CTMX×13, CRDL×22 | CABA, BAK, EOSE, DELL, MLYS, CCOI, SGLD | ANF, BHVN, BZ, SEDG, SMTC, URBN, ERAS | $326.26 | $8,662.03 | $8,988.29 | GPRO×40, FRVO×2, CRK×3, MMED×2, CTMX×13, CRDL×22, CABA×353, BAK×657, EOSE×359, DELL×2, MLYS×43, CCOI×125, SGLD×197 | SELL ANF (dropped from list after 5 sess (min 5)); SELL BHVN (dropped from list after 5 sess (min 5)); SELL BZ (dropped from list after 5 sess (min 5)); SELL SEDG (dropped from list after 5 sess (min 5)); SELL SMTC (dropped from list after 5 sess (min 5)); SELL URBN (dropped from list after 5 sess (min 5)); SELL ERAS (dropped from list after 5 sess (min 5)); BUY CABA x353 @ 3.63; BUY BAK x657 @ 1.95; BUY EOSE x359 @ 3.57; BUY DELL x2 @ 486.31; BUY MLYS x43 @ 29.15; BUY CCOI x125 @ 10.22; BUY SGLD x197 @ 6.48 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $8,739.75 | ▼ $9,989.25 (-10.75) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 84 | $14.80 | $2.24 | — | $7,494.31 | ▼ $9,987.01 (-12.99) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $6,240.67 | ▼ $9,983.27 (-16.73) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $4,986.99 | ▼ $9,979.41 (-20.59) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $3,747.28 | ▼ $9,977.20 (-22.80) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $2,512.19 | ▼ $9,975.02 (-24.98) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $1,264.42 | ▼ $9,972.69 (-27.31) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $10.28 | ▼ $9,966.71 (-33.29) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 833 | $1.66 | $10.89 | $+111.64 | $1,382.16 | ▼ $9,230.55 (-769.45) | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `BETR` | 84 | $11.73 | $2.27 | $-262.39 | $2,365.22 | ▼ $9,228.29 (-771.71) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ANGX` | 290 | $4.43 | $3.80 | $+27.26 | $3,646.12 | ▼ $9,224.49 (-775.51) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `HYLN` | 299 | $3.42 | $3.92 | $-235.01 | $4,664.78 | ▼ $9,220.57 (-779.43) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ADUR` | 75 | $16.00 | $2.24 | $-41.95 | $5,862.54 | ▼ $9,218.33 (-781.67) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $7,093.25 | ▼ $9,216.13 (-783.87) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `AIRO` | 112 | $8.39 | $2.35 | $-310.44 | $8,030.58 | ▼ $9,213.78 (-786.22) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `NCMI` | 464 | $2.55 | $6.07 | $-77.02 | $9,207.71 | ▼ $9,207.71 (-792.29) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 9 | $119.43 | $2.02 | — | $8,130.82 | ▼ $9,205.69 (-794.31) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1150.96 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 66 | $17.20 | $2.19 | — | $6,993.43 | ▼ $9,203.50 (-796.50) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1150.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $5,909.93 | ▼ $9,201.50 (-798.50) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1150.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 103 | $11.13 | $2.30 | — | $4,761.24 | ▼ $9,199.20 (-800.80) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1150.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 465 | $2.47 | $6.00 | — | $3,606.69 | ▼ $9,193.20 (-806.80) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1150.96 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 596 | $1.93 | $7.69 | — | $2,448.72 | ▼ $9,185.51 (-814.49) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1150.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 19 | $59.72 | $2.05 | — | $1,311.99 | ▼ $9,183.46 (-816.54) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1150.96 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 871 | $1.32 | $11.24 | — | $151.04 | ▼ $9,172.23 (-827.77) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1150.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 13 | $1.62 | $0.25 | — | $129.73 | ▼ $9,583.11 (-416.89) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $21.58 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 10 | $2.00 | $0.23 | — | $109.50 | ▼ $9,582.88 (-417.12) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $21.58 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 5 | $3.70 | $0.20 | — | $90.80 | ▼ $9,582.68 (-417.32) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $21.58 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 4 | $5.22 | $0.22 | — | $69.70 | ▼ $9,582.46 (-417.54) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $21.58 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 3 | $6.79 | $0.21 | — | $49.12 | ▼ $9,582.25 (-417.75) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $21.58 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 3 | $6.38 | $0.20 | — | $29.78 | ▼ $9,582.05 (-417.95) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $21.58 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 2 | $8.79 | $0.18 | — | $12.01 | ▼ $9,581.86 (-418.14) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.5; leftover $21.58 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 9 | $117.41 | $2.04 | $-22.23 | $1,066.67 | ▼ $9,961.62 (-38.38) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 66 | $16.47 | $2.21 | $-52.58 | $2,151.48 | ▼ $9,959.41 (-40.59) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AEM` | 5 | $214.11 | $2.02 | $-14.98 | $3,220.00 | ▼ $9,957.38 (-42.62) | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 103 | $15.74 | $2.33 | $+470.20 | $4,838.89 | ▼ $9,955.05 (-44.95) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 465 | $2.32 | $6.09 | $-81.83 | $5,911.61 | ▼ $9,948.97 (-51.03) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 596 | $2.09 | $7.80 | $+79.87 | $7,149.45 | ▼ $9,941.17 (-58.83) | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CRSP` | 19 | $59.12 | $2.07 | $-15.51 | $8,270.66 | ▼ $9,939.10 (-60.90) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 871 | $1.75 | $11.39 | $+351.90 | $9,783.52 | ▼ $9,927.71 (-72.29) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $144.70 | $2.02 | — | $8,479.20 | ▼ $9,925.69 (-74.31) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1397.65 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 82 | $16.95 | $2.24 | — | $7,087.07 | ▼ $9,923.46 (-76.54) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1397.65 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 75 | $18.50 | $2.21 | — | $5,697.35 | ▼ $9,921.24 (-78.76) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1397.65 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 41 | $33.78 | $2.11 | — | $4,310.26 | ▼ $9,919.13 (-80.87) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1397.65 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $149.40 | $2.02 | — | $2,963.64 | ▼ $9,917.11 (-82.89) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1397.65 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 16 | $82.70 | $2.04 | — | $1,638.40 | ▼ $9,915.07 (-84.93) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=-4.6; leftover $1397.65 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 72 | $19.30 | $2.21 | — | $246.60 | ▼ $9,912.87 (-87.13) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer; ret5=-4.1; leftover $1397.65 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `BMEA` | 13 | $1.65 | $0.27 | $-0.13 | $267.78 | ▼ $9,337.69 (-662.31) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `NPWR` | 10 | $1.78 | $0.23 | $-2.66 | $285.35 | ▼ $9,337.46 (-662.54) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `PUSA` | 5 | $3.93 | $0.23 | $+0.72 | $304.77 | ▼ $9,337.23 (-662.77) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `ALVO` | 4 | $5.24 | $0.24 | $-0.38 | $325.48 | ▼ $9,336.98 (-663.02) | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `CAPR` | 3 | $10.43 | $0.34 | $+10.37 | $356.43 | ▼ $9,336.64 (-663.36) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `ZURA` | 3 | $5.60 | $0.20 | $-2.74 | $373.04 | ▼ $9,336.45 (-663.55) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `SUJA` | 2 | $9.31 | $0.21 | $+0.65 | $391.44 | ▼ $9,336.23 (-663.77) | dropped from list after 5 sess (min 5) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 40 | $1.22 | $0.61 | — | $342.03 | ▼ $9,290.47 (-709.53) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $48.93 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 2 | $18.40 | $0.37 | — | $304.86 | ▼ $9,290.10 (-709.90) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $48.93 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 3 | $15.70 | $0.48 | — | $257.28 | ▼ $9,289.62 (-710.38) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $48.93 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 2 | $22.78 | $0.46 | — | $211.26 | ▼ $9,289.16 (-710.84) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $48.93 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 13 | $3.72 | $0.52 | — | $162.38 | ▼ $9,288.64 (-711.36) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $48.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 22 | $2.16 | $0.54 | — | $114.32 | ▼ $9,288.10 (-711.90) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $48.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `ANF` | 9 | $137.70 | $2.04 | $-67.05 | $1,351.58 | ▼ $9,288.54 (-711.46) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BHVN` | 82 | $15.89 | $2.26 | $-91.42 | $2,652.30 | ▼ $9,286.28 (-713.72) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BZ` | 75 | $17.31 | $2.24 | $-93.70 | $3,948.31 | ▼ $9,284.04 (-715.96) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SEDG` | 41 | $33.69 | $2.13 | $-7.94 | $5,327.47 | ▼ $9,281.91 (-718.09) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SMTC` | 9 | $133.10 | $2.04 | $-150.75 | $6,523.33 | ▼ $9,279.87 (-720.13) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `URBN` | 16 | $79.93 | $2.06 | $-48.42 | $7,800.15 | ▼ $9,277.81 (-722.19) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `ERAS` | 72 | $16.38 | $2.23 | $-214.67 | $8,977.28 | ▼ $9,275.58 (-724.42) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 353 | $3.63 | $4.55 | — | $7,691.34 | ▼ $9,271.03 (-728.97) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1282.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 657 | $1.95 | $8.48 | — | $6,401.71 | ▼ $9,262.55 (-737.45) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1282.47 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 359 | $3.57 | $4.63 | — | $5,115.45 | ▼ $9,257.92 (-742.08) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1282.47 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $4,140.84 | ▼ $9,255.93 (-744.07) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1282.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 43 | $29.15 | $2.12 | — | $2,885.27 | ▼ $9,253.81 (-746.19) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1282.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 125 | $10.22 | $2.37 | — | $1,605.40 | ▼ $9,251.44 (-748.56) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1282.47 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SGLD` | 197 | $6.48 | $2.58 | — | $326.26 | ▼ $9,248.86 (-751.14) | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+0.0; leftover $1282.47 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BETR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ADUR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `AIRO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `NCMI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `TMC` | cash | leftover split 1.28 < 1 share @ 4.05 |
| 2026-08-17 | `CDNL` | cash | leftover split 1.28 < 1 share @ 39.85 |
| 2026-08-17 | `ABX` | cash | leftover split 1.28 < 1 share @ 9.12 |
| 2026-08-17 | `VERA` | cash | leftover split 1.28 < 1 share @ 31.30 |
| 2026-08-17 | `CAPR` | cash | leftover split 1.28 < 1 share @ 6.87 |
| 2026-08-17 | `HTFL` | cash | leftover split 1.28 < 1 share @ 41.23 |
| 2026-08-17 | `UMAC` | cash | leftover split 1.28 < 1 share @ 32.55 |
| 2026-08-17 | `NPWR` | cash | leftover split 1.28 < 1 share @ 1.92 |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BETR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `NCMI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `WFF` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HIVE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BETR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ANGX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `HYLN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ADUR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ARX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `AIRO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `NCMI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BETR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ANGX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `HYLN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ADUR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ARX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `AIRO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `NCMI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `AG` | cash | leftover split 1.28 < 1 share @ 20.55 |
| 2026-08-20 | `BHP` | cash | leftover split 1.28 < 1 share @ 91.01 |
| 2026-08-20 | `CDE` | cash | leftover split 1.28 < 1 share @ 20.65 |
| 2026-08-20 | `HDSN` | cash | leftover split 1.28 < 1 share @ 5.77 |
| 2026-08-20 | `IAG` | cash | leftover split 1.28 < 1 share @ 19.63 |
| 2026-08-20 | `KGC` | cash | leftover split 1.28 < 1 share @ 29.63 |
| 2026-08-20 | `NFGC` | cash | leftover split 1.28 < 1 share @ 1.75 |
| 2026-08-20 | `WPM` | cash | leftover split 1.28 < 1 share @ 144.54 |
| 2026-08-24 | `AU` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AEM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRSP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GUTS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AU` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AEM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRSP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AEM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUTL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRSP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `PUSA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `SUJA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `SLQT` | no_price | no 09:30 open |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `DKS` | no_price | no 09:30 open |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AEM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUTL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRSP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `PUSA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `SUJA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `NPWR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `PUSA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ALVO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ZURA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `SUJA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `NPWR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `PUSA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ALVO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ZURA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `SUJA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `ERAS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `ERAS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-02 | `ANF` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `BHVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `BZ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `SEDG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `SMTC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `URBN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `ERAS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `YDDL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `ANF` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `BHVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `BZ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `SEDG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `SMTC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `URBN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `ERAS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `RVTY` | cash | leftover split 48.93 < 1 share @ 125.94 |
| 2026-09-03 | `EIX` | cash | leftover split 48.93 < 1 share @ 56.78 |
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CTMX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GPRO` | 40 | 2026-09-03 @ $1.22 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $48.93 |
| `FRVO` | 2 | 2026-09-03 @ $18.40 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $48.93 |
| `CRK` | 3 | 2026-09-03 @ $15.70 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $48.93 |
| `MMED` | 2 | 2026-09-03 @ $22.78 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $48.93 |
| `CTMX` | 13 | 2026-09-03 @ $3.72 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $48.93 |
| `CRDL` | 22 | 2026-09-03 @ $2.16 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $48.93 |
| `CABA` | 353 | 2026-09-04 @ $3.63 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1282.47 |
| `BAK` | 657 | 2026-09-04 @ $1.95 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1282.47 |
| `EOSE` | 359 | 2026-09-04 @ $3.57 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1282.47 |
| `DELL` | 2 | 2026-09-04 @ $486.31 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1282.47 |
| `MLYS` | 43 | 2026-09-04 @ $29.15 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1282.47 |
| `CCOI` | 125 | 2026-09-04 @ $10.22 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1282.47 |
| `SGLD` | 197 | 2026-09-04 @ $6.48 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+0.0; leftover $1282.47 |
