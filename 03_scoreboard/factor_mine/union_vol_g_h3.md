# Factor mine action — `union_vol_g_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ vol_g, no 🚨

Cash book **-2.49%** ($9,751) · signal-only (no cash/fees) was +1.81%. Starts YES **8/17**. Fills 83 · skips 130 · realized $-317.80.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `vol=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $31.84.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-14 | +5.50 | $10,000.00 | — | BTBT, BETR, ANGX, HYLN, ADUR, ARX, AIRO, NCMI | — | $10.28 | $9,787.54 | $9,797.82 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | BUY BTBT x833 @ 1.50; BUY BETR x84 @ 14.80; BUY ANGX x290 @ 4.31; BUY HYLN x299 @ 4.18; BUY ADUR x75 @ 16.50; BUY ARX x63 @ 19.57; BUY AIRO x112 @ 11.12; BUY NCMI x464 @ 2.69 |
| 2026-08-17 | +2.25 | $10.28 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | — | — | $10.28 | $9,799.38 | $9,809.66 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | hold BTBT,BETR,ANGX,HYLN,ADUR,ARX,AIRO,NCMI |
| 2026-08-18 | -6.20 | $10.28 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | — | — | $10.28 | $9,444.26 | $9,454.54 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $10.28 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | — | BTBT, BETR, ANGX, HYLN, ADUR, ARX, AIRO, NCMI | $9,414.48 | $0.00 | $9,414.48 | — | SELL BTBT (dropped from list after 3 sess (min 3)); SELL BETR (dropped from list after 3 sess (min 3)); SELL ANGX (dropped from list after 3 sess (min 3)); SELL HYLN (dropped from list after 3 sess (min 3)); SELL ADUR (dropped from list after 3 sess (min 3)); SELL ARX (dropped from list after 3 sess (min 3)); SELL AIRO (dropped from list after 3 sess (min 3)); SELL NCMI (dropped from list after 3 sess (min 3)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $9,414.48 | — | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $153.32 | $9,457.53 | $9,610.85 | AG×57, BHP×12, CDE×56, HDSN×203, IAG×59, KGC×39, NFGC×672, WPM×8 | BUY AG x57 @ 20.55; BUY BHP x12 @ 91.01; BUY CDE x56 @ 20.65; BUY HDSN x203 @ 5.77; BUY IAG x59 @ 19.63; BUY KGC x39 @ 29.63; BUY NFGC x672 @ 1.75; BUY WPM x8 @ 144.54 |
| 2026-08-21 | +3.25 | $153.32 | AG×57, BHP×12, CDE×56, HDSN×203, IAG×59, KGC×39, NFGC×672, WPM×8 | AUPH, ARCT, AUTL, CRDL, CYPH | — | $70.94 | $9,790.91 | $9,861.85 | AG×57, BHP×12, CDE×56, HDSN×203, IAG×59, KGC×39, NFGC×672, WPM×8, AUPH×1, ARCT×1, AUTL×7, CRDL×9, CYPH×14 | BUY AUPH x1 @ 17.20; BUY ARCT x1 @ 11.13; BUY AUTL x7 @ 2.47; BUY CRDL x9 @ 1.93; BUY CYPH x14 @ 1.32 |
| 2026-08-24 | -5.17 | $70.94 | AG×57, BHP×12, CDE×56, HDSN×203, IAG×59, KGC×39, NFGC×672, WPM×8, AUPH×1, ARCT×1, AUTL×7, CRDL×9, CYPH×14 | — | — | $70.94 | $9,764.11 | $9,835.05 | AG×57, BHP×12, CDE×56, HDSN×203, IAG×59, KGC×39, NFGC×672, WPM×8, AUPH×1, ARCT×1, AUTL×7, CRDL×9, CYPH×14 | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $70.94 | AG×57, BHP×12, CDE×56, HDSN×203, IAG×59, KGC×39, NFGC×672, WPM×8, AUPH×1, ARCT×1, AUTL×7, CRDL×9, CYPH×14 | BMEA, NPWR, PUSA, ALVO, CAPR, ZURA, SUJA | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $5.76 | $9,994.28 | $10,000.04 | AUPH×1, ARCT×1, AUTL×7, CRDL×9, CYPH×14, BMEA×863, NPWR×699, PUSA×377, ALVO×267, CAPR×205, ZURA×219, SUJA×156 | SELL AG (dropped from list after 3 sess (min 3)); SELL BHP (dropped from list after 3 sess (min 3)); SELL CDE (dropped from list after 3 sess (min 3)); SELL HDSN (dropped from list after 3 sess (min 3)); SELL IAG (dropped from list after 3 sess (min 3)); SELL KGC (dropped from list after 3 sess (min 3)); SELL NFGC (dropped from list after 3 sess (min 3)); SELL WPM (dropped from list after 3 sess (min 3)); BUY BMEA x863 @ 1.62; BUY NPWR x699 @ 2.00; BUY PUSA x377 @ 3.70; BUY ALVO x267 @ 5.22; BUY CAPR x205 @ 6.79; BUY ZURA x219 @ 6.38; BUY SUJA x156 @ 8.79 |
| 2026-08-26 | +2.02 | $5.76 | AUPH×1, ARCT×1, AUTL×7, CRDL×9, CYPH×14, BMEA×863, NPWR×699, PUSA×377, ALVO×267, CAPR×205, ZURA×219, SUJA×156 | — | — | $5.76 | $9,833.30 | $9,839.06 | AUPH×1, ARCT×1, AUTL×7, CRDL×9, CYPH×14, BMEA×863, NPWR×699, PUSA×377, ALVO×267, CAPR×205, ZURA×219, SUJA×156 | hold AUPH,ARCT,AUTL,CRDL,CYPH,BMEA,NPWR,PUSA,ALVO,CAPR,ZURA,SUJA |
| 2026-08-27 | — | $5.76 | AUPH×1, ARCT×1, AUTL×7, CRDL×9, CYPH×14, BMEA×863, NPWR×699, PUSA×377, ALVO×267, CAPR×205, ZURA×219, SUJA×156 | — | AUPH, ARCT, AUTL, CRDL, CYPH | $94.16 | $10,206.59 | $10,300.75 | BMEA×863, NPWR×699, PUSA×377, ALVO×267, CAPR×205, ZURA×219, SUJA×156 | SELL AUPH (dropped from list after 4 sess (min 3)); SELL ARCT (dropped from list after 4 sess (min 3)); SELL AUTL (dropped from list after 4 sess (min 3)); SELL CRDL (dropped from list after 4 sess (min 3)); SELL CYPH (dropped from list after 4 sess (min 3)) |
| 2026-08-28 | +0.75 | $94.16 | BMEA×863, NPWR×699, PUSA×377, ALVO×267, CAPR×205, ZURA×219, SUJA×156 | ANF, BHVN, BZ, SEDG, SMTC, URBN, ERAS | BMEA, NPWR, PUSA, ALVO, ZURA, SUJA | $110.53 | $10,132.43 | $10,242.96 | CAPR×205, ANF×8, BHVN×70, BZ×64, SEDG×35, SMTC×8, URBN×14, ERAS×62 | SELL BMEA (dropped from list after 3 sess (min 3)); SELL NPWR (dropped from list after 3 sess (min 3)); SELL PUSA (dropped from list after 3 sess (min 3)); SELL ALVO (dropped from list after 3 sess (min 3)); SELL ZURA (dropped from list after 3 sess (min 3)); SELL SUJA (dropped from list after 3 sess (min 3)); BUY ANF x8 @ 144.70; BUY BHVN x70 @ 16.95; BUY BZ x64 @ 18.50; BUY SEDG x35 @ 33.78; BUY SMTC x8 @ 149.40; BUY URBN x14 @ 82.70; BUY ERAS x62 @ 19.30 |
| 2026-08-31 | -5.85 | $110.53 | CAPR×205, ANF×8, BHVN×70, BZ×64, SEDG×35, SMTC×8, URBN×14, ERAS×62 | — | CAPR | $2,043.03 | $7,817.67 | $9,860.70 | ANF×8, BHVN×70, BZ×64, SEDG×35, SMTC×8, URBN×14, ERAS×62 | SELL CAPR (dropped from list after 4 sess (min 3)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $2,043.03 | ANF×8, BHVN×70, BZ×64, SEDG×35, SMTC×8, URBN×14, ERAS×62 | — | — | $2,043.03 | $7,700.44 | $9,743.47 | ANF×8, BHVN×70, BZ×64, SEDG×35, SMTC×8, URBN×14, ERAS×62 | hard-red sit S=-6.30 |
| 2026-09-02 | -3.83 | $2,043.03 | ANF×8, BHVN×70, BZ×64, SEDG×35, SMTC×8, URBN×14, ERAS×62 | — | ANF, BHVN, BZ, SEDG, SMTC, URBN, ERAS | $9,682.17 | $0.00 | $9,682.17 | — | SELL ANF (dropped from list after 3 sess (min 3)); SELL BHVN (dropped from list after 3 sess (min 3)); SELL BZ (dropped from list after 3 sess (min 3)); SELL SEDG (dropped from list after 3 sess (min 3)); SELL SMTC (dropped from list after 3 sess (min 3)); SELL URBN (dropped from list after 3 sess (min 3)); SELL ERAS (dropped from list after 3 sess (min 3)); hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $9,682.17 | — | RVTY, GPRO, FRVO, CRK, MMED, CTMX, EIX, CRDL | — | $80.41 | $10,062.69 | $10,143.10 | RVTY×9, GPRO×992, FRVO×65, CRK×77, MMED×53, CTMX×325, EIX×21, CRDL×560 | BUY RVTY x9 @ 125.94; BUY GPRO x992 @ 1.22; BUY FRVO x65 @ 18.40; BUY CRK x77 @ 15.70; BUY MMED x53 @ 22.78; BUY CTMX x325 @ 3.72; BUY EIX x21 @ 56.78; BUY CRDL x560 @ 2.16 |
| 2026-09-04 | — | $80.41 | RVTY×9, GPRO×992, FRVO×65, CRK×77, MMED×53, CTMX×325, EIX×21, CRDL×560 | CABA, BAK, EOSE, CCOI, SGLD | — | $31.84 | $9,718.87 | $9,750.71 | RVTY×9, GPRO×992, FRVO×65, CRK×77, MMED×53, CTMX×325, EIX×21, CRDL×560, CABA×3, BAK×5, EOSE×3, CCOI×1, SGLD×1 | BUY CABA x3 @ 3.63; BUY BAK x5 @ 1.95; BUY EOSE x3 @ 3.57; BUY CCOI x1 @ 10.22; BUY SGLD x1 @ 6.48 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $8,739.75 | ▼ $9,989.25 (-10.75) | union ∩ vol_g, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 84 | $14.80 | $2.24 | — | $7,494.31 | ▼ $9,987.01 (-12.99) | union ∩ vol_g, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $6,240.67 | ▼ $9,983.27 (-16.73) | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $4,986.99 | ▼ $9,979.41 (-20.59) | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $3,747.28 | ▼ $9,977.20 (-22.80) | union ∩ vol_g, no 🚨; gate vol=good; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $2,512.19 | ▼ $9,975.02 (-24.98) | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $1,264.42 | ▼ $9,972.69 (-27.31) | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $10.28 | ▼ $9,966.71 (-33.29) | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 833 | $1.42 | $10.89 | $-88.28 | $1,182.24 | ▼ $9,437.32 (-562.68) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BETR` | 84 | $13.03 | $2.27 | $-153.19 | $2,274.50 | ▼ $9,435.06 (-564.94) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 290 | $4.79 | $3.80 | $+131.66 | $3,659.80 | ▼ $9,431.26 (-568.74) | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 299 | $3.87 | $3.92 | $-100.46 | $4,813.01 | ▼ $9,427.34 (-572.66) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ADUR` | 75 | $15.65 | $2.24 | $-68.20 | $5,984.52 | ▼ $9,425.10 (-574.90) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 63 | $19.58 | $2.20 | $-3.75 | $7,215.86 | ▼ $9,422.90 (-577.10) | dropped from list after 3 sess (min 3) | join🟡 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `AIRO` | 112 | $9.10 | $2.35 | $-230.92 | $8,232.71 | ▼ $9,420.55 (-579.45) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NCMI` | 464 | $2.56 | $6.07 | $-72.38 | $9,414.48 | ▼ $9,414.48 (-585.52) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 57 | $20.55 | $2.16 | — | $8,240.97 | ▼ $9,412.32 (-587.68) | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1176.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 12 | $91.01 | $2.03 | — | $7,146.82 | ▼ $9,410.29 (-589.71) | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1176.81 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 56 | $20.65 | $2.16 | — | $5,988.26 | ▼ $9,408.13 (-591.87) | union ∩ vol_g, no 🚨; gate vol=good; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1176.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 203 | $5.77 | $2.62 | — | $4,814.33 | ▼ $9,405.51 (-594.49) | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1176.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 59 | $19.63 | $2.17 | — | $3,654.00 | ▼ $9,403.35 (-596.65) | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1176.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $2,496.32 | ▼ $9,401.24 (-598.76) | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1176.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 672 | $1.75 | $8.67 | — | $1,311.65 | ▼ $9,392.57 (-607.43) | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1176.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $153.32 | ▼ $9,390.56 (-609.44) | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1176.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $135.94 | ▼ $9,863.23 (-136.77) | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $19.16 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $124.70 | ▼ $9,863.12 (-136.88) | union ∩ vol_g, no 🚨; gate vol=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $19.16 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 7 | $2.47 | $0.19 | — | $107.21 | ▼ $9,862.92 (-137.08) | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $19.16 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 9 | $1.93 | $0.20 | — | $89.64 | ▼ $9,862.72 (-137.28) | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $19.16 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 14 | $1.32 | $0.23 | — | $70.94 | ▼ $9,862.50 (-137.50) | union ∩ vol_g, no 🚨; gate vol=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $19.16 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 57 | $20.73 | $2.18 | $+5.92 | $1,250.37 | ▼ $9,897.48 (-102.52) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 12 | $95.95 | $2.05 | $+55.21 | $2,399.72 | ▼ $9,895.43 (-104.57) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 56 | $20.85 | $2.18 | $+6.86 | $3,565.14 | ▼ $9,893.25 (-106.75) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 203 | $5.53 | $2.66 | $-54.00 | $4,685.07 | ▼ $9,890.59 (-109.41) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 59 | $21.63 | $2.19 | $+113.65 | $5,959.05 | ▼ $9,888.40 (-111.60) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 39 | $32.76 | $2.13 | $+117.84 | $7,234.56 | ▼ $9,886.27 (-113.73) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 672 | $1.91 | $8.79 | $+90.06 | $8,509.29 | ▼ $9,877.48 (-122.52) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $160.00 | $2.03 | $+119.63 | $9,787.26 | ▼ $9,875.45 (-124.55) | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 863 | $1.62 | $11.13 | — | $8,378.07 | ▼ $9,864.32 (-135.68) | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1398.18 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 699 | $2.00 | $9.02 | — | $6,971.05 | ▼ $9,855.30 (-144.70) | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1398.18 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 377 | $3.70 | $4.86 | — | $5,571.29 | ▼ $9,850.44 (-149.56) | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $1398.18 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 267 | $5.22 | $3.44 | — | $4,174.10 | ▼ $9,846.99 (-153.01) | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1398.18 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 205 | $6.79 | $2.64 | — | $2,779.51 | ▼ $9,844.35 (-155.65) | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $1398.18 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 219 | $6.38 | $2.83 | — | $1,379.46 | ▼ $9,841.52 (-158.48) | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1398.18 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 156 | $8.79 | $2.46 | — | $5.76 | ▼ $9,839.06 (-160.94) | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.5; leftover $1398.18 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $22.18 | ▲ $10,238.49 (+238.49) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 1 | $15.35 | $0.18 | $+3.93 | $37.35 | ▲ $10,238.31 (+238.31) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 7 | $2.41 | $0.21 | $-0.82 | $54.01 | ▲ $10,238.10 (+238.10) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 9 | $2.03 | $0.23 | $+0.47 | $72.05 | ▲ $10,237.87 (+237.87) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 14 | $1.60 | $0.29 | $+3.41 | $94.16 | ▲ $10,237.58 (+237.58) | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 863 | $1.74 | $11.29 | $+81.14 | $1,584.50 | ▲ $10,292.14 (+292.14) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `NPWR` | 699 | $1.83 | $9.14 | $-136.99 | $2,854.52 | ▲ $10,282.99 (+282.99) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `PUSA` | 377 | $3.86 | $4.94 | $+50.52 | $4,304.81 | ▲ $10,278.06 (+278.06) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 267 | $4.88 | $3.50 | $-97.72 | $5,604.27 | ▲ $10,274.56 (+274.56) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 219 | $6.02 | $2.87 | $-84.54 | $6,919.77 | ▲ $10,271.68 (+271.68) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SUJA` | 156 | $9.41 | $2.50 | $+91.77 | $8,385.24 | ▲ $10,269.19 (+269.19) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $144.70 | $2.01 | — | $7,225.62 | ▲ $10,267.17 (+267.17) | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1197.89 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 70 | $16.95 | $2.20 | — | $6,036.92 | ▲ $10,264.97 (+264.97) | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1197.89 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 64 | $18.50 | $2.18 | — | $4,850.74 | ▲ $10,262.79 (+262.79) | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1197.89 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 35 | $33.78 | $2.10 | — | $3,666.35 | ▲ $10,260.70 (+260.70) | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1197.89 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $2,469.13 | ▲ $10,258.68 (+258.68) | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1197.89 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 14 | $82.70 | $2.03 | — | $1,309.30 | ▲ $10,256.65 (+256.65) | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=-4.6; leftover $1197.89 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 62 | $19.30 | $2.18 | — | $110.53 | ▲ $10,254.48 (+254.48) | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer; ret5=-4.1; leftover $1197.89 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 205 | $9.44 | $2.69 | $+537.91 | $2,043.03 | ▼ $9,870.03 (-129.97) | dropped from list after 4 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 8 | $142.00 | $2.03 | $-25.65 | $3,177.00 | ▼ $9,694.99 (-305.01) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BHVN` | 70 | $15.39 | $2.22 | $-113.62 | $4,252.08 | ▼ $9,692.77 (-307.23) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 64 | $17.29 | $2.20 | $-81.82 | $5,356.43 | ▼ $9,690.56 (-309.44) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 35 | $31.87 | $2.12 | $-71.06 | $6,469.77 | ▼ $9,688.45 (-311.55) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $127.63 | $2.03 | $-178.21 | $7,488.77 | ▼ $9,686.41 (-313.59) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 14 | $79.12 | $2.05 | $-54.20 | $8,594.40 | ▼ $9,684.36 (-315.64) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ERAS` | 62 | $17.58 | $2.20 | $-111.01 | $9,682.17 | ▼ $9,682.17 (-317.83) | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $8,546.69 | ▼ $9,680.15 (-319.85) | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1210.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 992 | $1.22 | $12.80 | — | $7,323.65 | ▼ $9,667.35 (-332.65) | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1210.27 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 65 | $18.40 | $2.19 | — | $6,125.47 | ▼ $9,665.17 (-334.83) | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1210.27 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 77 | $15.70 | $2.22 | — | $4,914.35 | ▼ $9,662.95 (-337.05) | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1210.27 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 53 | $22.78 | $2.15 | — | $3,704.86 | ▼ $9,660.80 (-339.20) | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1210.27 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 325 | $3.72 | $4.19 | — | $2,491.66 | ▼ $9,656.60 (-343.40) | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1210.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 21 | $56.78 | $2.05 | — | $1,297.23 | ▼ $9,654.55 (-345.45) | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; ret5=+0.3; leftover $1210.27 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 560 | $2.16 | $7.22 | — | $80.41 | ▼ $9,647.33 (-352.67) | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1210.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 3 | $3.63 | $0.12 | — | $69.40 | ▲ $10,277.81 (+277.81) | union ∩ vol_g, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+13.8; leftover $11.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 5 | $1.95 | $0.11 | — | $59.54 | ▲ $10,277.70 (+277.70) | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $11.49 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 3 | $3.57 | $0.12 | — | $48.71 | ▲ $10,277.58 (+277.58) | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $11.49 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 1 | $10.22 | $0.11 | — | $38.39 | ▲ $10,277.48 (+277.48) | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $11.49 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SGLD` | 1 | $6.48 | $0.07 | — | $31.84 | ▲ $10,277.41 (+277.41) | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+0.0; leftover $11.49 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BETR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ADUR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NCMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `TMC` | cash | leftover split 1.28 < 1 share @ 4.05 |
| 2026-08-17 | `CDNL` | cash | leftover split 1.28 < 1 share @ 39.85 |
| 2026-08-17 | `ABX` | cash | leftover split 1.28 < 1 share @ 9.12 |
| 2026-08-17 | `VERA` | cash | leftover split 1.28 < 1 share @ 31.30 |
| 2026-08-17 | `CAPR` | cash | leftover split 1.28 < 1 share @ 6.87 |
| 2026-08-17 | `HTFL` | cash | leftover split 1.28 < 1 share @ 41.23 |
| 2026-08-17 | `UMAC` | cash | leftover split 1.28 < 1 share @ 32.55 |
| 2026-08-17 | `NPWR` | cash | leftover split 1.28 < 1 share @ 1.92 |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BETR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NCMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `WFF` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HIVE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 19.16 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 19.16 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 19.16 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GUTS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AUPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ARCT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AUTL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRDL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `PUSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SUJA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SLQT` | no_price | no 09:30 open |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `DKS` | no_price | no 09:30 open |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `PUSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SUJA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ERAS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ERAS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `YDDL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CTMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `EIX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | cash | leftover split 11.49 < 1 share @ 486.31 |
| 2026-09-04 | `MLYS` | cash | leftover split 11.49 < 1 share @ 29.15 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RVTY` | 9 | 2026-09-03 @ $125.94 | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1210.27 |
| `GPRO` | 992 | 2026-09-03 @ $1.22 | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1210.27 |
| `FRVO` | 65 | 2026-09-03 @ $18.40 | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1210.27 |
| `CRK` | 77 | 2026-09-03 @ $15.70 | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1210.27 |
| `MMED` | 53 | 2026-09-03 @ $22.78 | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1210.27 |
| `CTMX` | 325 | 2026-09-03 @ $3.72 | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1210.27 |
| `EIX` | 21 | 2026-09-03 @ $56.78 | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; ret5=+0.3; leftover $1210.27 |
| `CRDL` | 560 | 2026-09-03 @ $2.16 | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1210.27 |
| `CABA` | 3 | 2026-09-04 @ $3.63 | union ∩ vol_g, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+13.8; leftover $11.49 |
| `BAK` | 5 | 2026-09-04 @ $1.95 | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $11.49 |
| `EOSE` | 3 | 2026-09-04 @ $3.57 | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $11.49 |
| `CCOI` | 1 | 2026-09-04 @ $10.22 | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $11.49 |
| `SGLD` | 1 | 2026-09-04 @ $6.48 | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+0.0; leftover $11.49 |
