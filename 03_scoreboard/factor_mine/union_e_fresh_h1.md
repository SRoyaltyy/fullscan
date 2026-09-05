# Factor mine action — `union_e_fresh_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ e_fresh, no 🚨

Cash book **+8.62%** ($10,862) · signal-only (no cash/fees) was -4.91%. Starts YES **4/17**. Fills 108 · skips 38 · realized $+870.61.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `days_since_E_max=1,flag_E_min=0` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $248.60.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | INO, VOR | — | $21.06 | $10,748.47 | $10,769.53 | INO×6172, VOR×223 | BUY INO x6172 @ 0.81; BUY VOR x223 @ 22.01 |
| 2026-08-14 | +5.50 | $21.06 | INO×6172, VOR×223 | BTBT, ARX, AIRO, MH, CLBT, EU, LUNR, NMAX | INO, VOR | $11.72 | $10,857.29 | $10,869.01 | BTBT×906, ARX×69, AIRO×122, MH×100, CLBT×125, EU×1152, LUNR×70, NMAX×137 | SELL INO (dropped from list after 1 sess (min 1)); SELL VOR (dropped from list after 1 sess (min 1)); BUY BTBT x906 @ 1.50; BUY ARX x69 @ 19.57; BUY AIRO x122 @ 11.12; BUY MH x100 @ 13.55; BUY CLBT x125 @ 10.83; BUY EU x1152 @ 1.18; BUY LUNR x70 @ 19.17; BUY NMAX x137 @ 9.89 |
| 2026-08-17 | +2.25 | $11.72 | BTBT×906, ARX×69, AIRO×122, MH×100, CLBT×125, EU×1152, LUNR×70, NMAX×137 | — | BTBT, ARX, AIRO, MH, CLBT, EU, LUNR, NMAX | $10,894.88 | $0.00 | $10,894.88 | — | SELL BTBT (dropped from list after 1 sess (min 1)); SELL ARX (dropped from list after 1 sess (min 1)); SELL AIRO (dropped from list after 1 sess (min 1)); SELL MH (dropped from list after 1 sess (min 1)); SELL CLBT (dropped from list after 1 sess (min 1)); SELL EU (dropped from list after 1 sess (min 1)); SELL LUNR (dropped from list after 1 sess (min 1)); SELL NMAX (dropped from list after 1 sess (min 1)) |
| 2026-08-18 | -6.20 | $10,894.88 | — | — | — | $10,894.88 | $0.00 | $10,894.88 | — | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $10,894.88 | — | — | — | $10,894.88 | $0.00 | $10,894.88 | — | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $10,894.88 | — | EL, TOYO, DVLT, AAP, AEG, ALVO, ATAT, ATHM | — | $105.03 | $10,835.81 | $10,940.84 | EL×13, TOYO×307, DVLT×4539, AAP×29, AEG×151, ALVO×350, ATAT×39, ATHM×60 | BUY EL x13 @ 97.43; BUY TOYO x307 @ 4.43; BUY DVLT x4539 @ 0.30; BUY AAP x29 @ 46.85; BUY AEG x151 @ 9.01; BUY ALVO x350 @ 3.89; BUY ATAT x39 @ 34.05; BUY ATHM x60 @ 22.44 |
| 2026-08-21 | +3.25 | $105.03 | EL×13, TOYO×307, DVLT×4539, AAP×29, AEG×151, ALVO×350, ATAT×39, ATHM×60 | FUTU, DE, WMT, BEKE, BJ, BKE, PSEC | EL, TOYO, DVLT, AEG, ALVO, ATAT, ATHM | $251.18 | $10,882.75 | $11,133.93 | AAP×29, FUTU×12, DE×2, WMT×13, BEKE×77, BJ×14, BKE×32, PSEC×602 | SELL EL (dropped from list after 1 sess (min 1)); SELL TOYO (dropped from list after 1 sess (min 1)); SELL DVLT (dropped from list after 1 sess (min 1)); SELL AEG (dropped from list after 1 sess (min 1)); SELL ALVO (dropped from list after 1 sess (min 1)); SELL ATAT (dropped from list after 1 sess (min 1)); SELL ATHM (dropped from list after 1 sess (min 1)); BUY FUTU x12 @ 115.18; BUY DE x2 @ 623.26; BUY WMT x13 @ 103.69; BUY BEKE x77 @ 17.93; BUY BJ x14 @ 93.98; BUY BKE x32 @ 43.08; BUY PSEC x602 @ 2.30 |
| 2026-08-24 | -5.17 | $251.18 | AAP×29, FUTU×12, DE×2, WMT×13, BEKE×77, BJ×14, BKE×32, PSEC×602 | — | AAP, FUTU, DE, WMT, BEKE, BJ, BKE, PSEC | $11,173.20 | $0.00 | $11,173.20 | — | SELL AAP (dropped from list after 2 sess (min 1)); SELL FUTU (dropped from list after 1 sess (min 1)); SELL DE (dropped from list after 1 sess (min 1)); SELL WMT (dropped from list after 1 sess (min 1)); SELL BEKE (dropped from list after 1 sess (min 1)); SELL BJ (dropped from list after 1 sess (min 1)); SELL BKE (dropped from list after 1 sess (min 1)); SELL PSEC (dropped from list after 1 sess (min 1)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $11,173.20 | — | BMO, BNS, BZ, DKS, EH, GFI, GRRR, SHMD | — | $180.67 | $10,917.30 | $11,097.97 | BMO×8, BNS×16, BZ×91, DKS×7, EH×250, GFI×29, GRRR×97, SHMD×296 | BUY BMO x8 @ 172.40; BUY BNS x16 @ 86.86; BUY BZ x91 @ 15.34; BUY DKS x7 @ 179.33; BUY EH x250 @ 5.57; BUY GFI x29 @ 47.68; BUY GRRR x97 @ 14.26; BUY SHMD x296 @ 4.71 |
| 2026-08-26 | +2.02 | $180.67 | BMO×8, BNS×16, BZ×91, DKS×7, EH×250, GFI×29, GRRR×97, SHMD×296 | — | — | $180.67 | $10,972.81 | $11,153.48 | BMO×8, BNS×16, BZ×91, DKS×7, EH×250, GFI×29, GRRR×97, SHMD×296 | hold BMO,BNS,BZ,DKS,EH,GFI,GRRR,SHMD |
| 2026-08-27 | — | $180.67 | BMO×8, BNS×16, BZ×91, DKS×7, EH×250, GFI×29, GRRR×97, SHMD×296 | NVDA | BMO, BNS, BZ, DKS, EH, GFI, GRRR, SHMD | $151.85 | $10,063.68 | $10,215.53 | NVDA×48 | SELL BMO (dropped from list after 2 sess (min 1)); SELL BNS (dropped from list after 2 sess (min 1)); SELL BZ (dropped from list after 2 sess (min 1)); SELL DKS (dropped from list after 2 sess (min 1)); SELL EH (dropped from list after 2 sess (min 1)); SELL GFI (dropped from list after 2 sess (min 1)); SELL GRRR (dropped from list after 2 sess (min 1)); SELL SHMD (dropped from list after 2 sess (min 1)); BUY NVDA x48 @ 212.64 |
| 2026-08-28 | +0.75 | $151.85 | NVDA×48 | ADSK, BBAR, ESTC, FINV, FRO, GAP, HAFN, IREN | NVDA | $136.34 | $10,747.78 | $10,884.12 | ADSK×5, BBAR×90, ESTC×16, FINV×318, FRO×31, GAP×65, HAFN×171, IREN×33 | SELL NVDA (dropped from list after 1 sess (min 1)); BUY ADSK x5 @ 261.47; BUY BBAR x90 @ 14.96; BUY ESTC x16 @ 82.64; BUY FINV x318 @ 4.26; BUY FRO x31 @ 42.51; BUY GAP x65 @ 20.75; BUY HAFN x171 @ 7.91; BUY IREN x33 @ 40.65 |
| 2026-08-31 | -5.85 | $136.34 | ADSK×5, BBAR×90, ESTC×16, FINV×318, FRO×31, GAP×65, HAFN×171, IREN×33 | — | ADSK, BBAR, ESTC, FINV, FRO, GAP, HAFN, IREN | $10,872.01 | $0.00 | $10,872.01 | — | SELL ADSK (dropped from list after 1 sess (min 1)); SELL BBAR (dropped from list after 1 sess (min 1)); SELL ESTC (dropped from list after 1 sess (min 1)); SELL FINV (dropped from list after 1 sess (min 1)); SELL FRO (dropped from list after 1 sess (min 1)); SELL GAP (dropped from list after 1 sess (min 1)); SELL HAFN (dropped from list after 1 sess (min 1)); SELL IREN (dropped from list after 1 sess (min 1)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $10,872.01 | — | — | — | $10,872.01 | $0.00 | $10,872.01 | — | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $10,872.01 | — | — | — | $10,872.01 | $0.00 | $10,872.01 | — | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $10,872.01 | — | AI, AVGO, CHPT, CIEN, CPB, FIVE, HPE, MEI | — | $685.47 | $10,128.80 | $10,814.27 | AI×131, AVGO×3, CHPT×256, CIEN×3, CPB×57, FIVE×5, HPE×26, MEI×74 | BUY AI x131 @ 10.30; BUY AVGO x3 @ 369.68; BUY CHPT x256 @ 5.30; BUY CIEN x3 @ 357.25; BUY CPB x57 @ 23.80; BUY FIVE x5 @ 244.98; BUY HPE x26 @ 51.99; BUY MEI x74 @ 18.22 |
| 2026-09-04 | — | $685.47 | AI×131, AVGO×3, CHPT×256, CIEN×3, CPB×57, FIVE×5, HPE×26, MEI×74 | AMBA, ASAN, DOCU, DOMO, GWRE, IOT, LULU, MAMA | AI, AVGO, CHPT, CIEN, CPB, FIVE, HPE, MEI | $248.60 | $10,613.77 | $10,862.37 | AMBA×20, ASAN×133, DOCU×20, DOMO×359, GWRE×6, IOT×36, LULU×11, MAMA×86 | SELL AI (dropped from list after 1 sess (min 1)); SELL AVGO (dropped from list after 1 sess (min 1)); SELL CHPT (dropped from list after 1 sess (min 1)); SELL CIEN (dropped from list after 1 sess (min 1)); SELL CPB (dropped from list after 1 sess (min 1)); SELL FIVE (dropped from list after 1 sess (min 1)); SELL HPE (dropped from list after 1 sess (min 1)); SELL MEI (dropped from list after 1 sess (min 1)); BUY AMBA x20 @ 66.61; BUY ASAN x133 @ 10.16; BUY DOCU x20 @ 67.06; BUY DOMO x359 @ 3.78; BUY GWRE x6 @ 198.00; BUY IOT x36 @ 37.69; BUY LULU x11 @ 121.15; BUY MAMA x86 @ 15.62 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `INO` | 6172 | $0.81 | $68.51 | — | $4,932.17 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+13.2; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 223 | $22.01 | $2.88 | — | $21.06 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+0.3; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 6172 | $0.93 | $76.99 | $+595.14 | $5,684.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 223 | $23.33 | $2.96 | $+288.53 | $10,883.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 906 | $1.50 | $11.69 | — | $9,512.99 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 69 | $19.57 | $2.20 | — | $8,160.46 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 122 | $11.12 | $2.36 | — | $6,801.46 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 100 | $13.55 | $2.29 | — | $5,444.17 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 125 | $10.83 | $2.37 | — | $4,088.06 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-30.1; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 1152 | $1.18 | $14.86 | — | $2,713.84 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 70 | $19.17 | $2.20 | — | $1,369.74 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 137 | $9.89 | $2.40 | — | $11.72 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 906 | $1.52 | $11.85 | $-5.42 | $1,376.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 69 | $19.57 | $2.22 | $-4.42 | $2,725.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 122 | $9.57 | $2.39 | $-193.84 | $3,890.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 100 | $13.16 | $2.32 | $-43.61 | $5,203.94 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 125 | $11.19 | $2.40 | $+40.24 | $6,600.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `EU` | 1152 | $1.21 | $15.06 | $+4.64 | $7,979.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 70 | $20.25 | $2.22 | $+71.18 | $9,394.43 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `NMAX` | 137 | $10.97 | $2.44 | $+142.44 | $10,894.88 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 13 | $97.43 | $2.03 | — | $9,626.26 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; leftover $1361.86 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 307 | $4.43 | $3.96 | — | $8,262.29 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; leftover $1361.86 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 4539 | $0.30 | $27.23 | — | $6,873.36 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; leftover $1361.86 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 29 | $46.85 | $2.08 | — | $5,512.63 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; leftover $1361.86 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 151 | $9.01 | $2.44 | — | $4,149.68 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; leftover $1361.86 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 350 | $3.89 | $4.51 | — | $2,783.66 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; leftover $1361.86 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 39 | $34.05 | $2.11 | — | $1,453.60 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; leftover $1361.86 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 60 | $22.44 | $2.17 | — | $105.03 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; leftover $1361.86 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `EL` | 13 | $96.75 | $2.05 | $-12.92 | $1,360.74 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TOYO` | 307 | $4.68 | $4.02 | $+68.77 | $2,793.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DVLT` | 4539 | $0.31 | $28.45 | $-10.30 | $4,172.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AEG` | 151 | $9.04 | $2.48 | $-0.39 | $5,534.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ALVO` | 350 | $4.32 | $4.59 | $+141.40 | $7,042.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 39 | $34.31 | $2.13 | $+5.91 | $8,378.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATHM` | 60 | $22.20 | $2.19 | $-18.76 | $9,707.86 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 12 | $115.18 | $2.03 | — | $8,323.67 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1386.84 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $7,075.16 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1386.84 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `WMT` | 13 | $103.69 | $2.03 | — | $5,725.16 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; ret5=-10.5; leftover $1386.84 | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 77 | $17.93 | $2.22 | — | $4,341.94 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=+0.2; leftover $1386.84 | join🟢 sector🟢 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 14 | $93.98 | $2.03 | — | $3,024.19 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.4; leftover $1386.84 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 32 | $43.08 | $2.09 | — | $1,643.54 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; leftover $1386.84 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 602 | $2.30 | $7.77 | — | $251.18 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; leftover $1386.84 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AAP` | 29 | $43.10 | $2.10 | $-112.92 | $1,498.98 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 12 | $120.87 | $2.05 | $+64.21 | $2,947.37 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.62 | $2.02 | $+56.71 | $4,252.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `WMT` | 13 | $104.16 | $2.05 | $+2.03 | $5,604.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 77 | $18.06 | $2.24 | $+5.16 | $6,993.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 14 | $97.02 | $2.05 | $+38.48 | $8,349.23 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 32 | $44.54 | $2.11 | $+42.53 | $9,772.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PSEC` | 602 | $2.34 | $7.88 | $+8.44 | $11,173.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 8 | $172.40 | $2.01 | — | $9,791.99 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-6.1; leftover $1396.65 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 16 | $86.86 | $2.04 | — | $8,400.19 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.3; leftover $1396.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 91 | $15.34 | $2.26 | — | $7,001.99 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=+2.8; leftover $1396.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 7 | $179.33 | $2.01 | — | $5,744.67 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-9.3; leftover $1396.65 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 250 | $5.57 | $3.23 | — | $4,348.94 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-7.1; leftover $1396.65 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 29 | $47.68 | $2.08 | — | $2,964.15 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+18.8; leftover $1396.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 97 | $14.26 | $2.28 | — | $1,578.64 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-1.9; leftover $1396.65 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 296 | $4.71 | $3.82 | — | $180.67 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-9.9; leftover $1396.65 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `BMO` | 8 | $173.22 | $2.04 | $+2.51 | $1,564.39 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BNS` | 16 | $92.64 | $2.06 | $+88.38 | $3,044.57 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 91 | $16.77 | $2.29 | $+125.58 | $4,568.35 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 7 | $121.87 | $2.03 | $-406.26 | $5,419.41 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `EH` | 250 | $4.77 | $3.28 | $-206.50 | $6,608.63 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `GFI` | 29 | $48.24 | $2.10 | $+12.06 | $8,005.50 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `GRRR` | 97 | $14.03 | $2.31 | $-26.90 | $9,364.10 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SHMD` | 296 | $3.38 | $3.88 | $-401.38 | $10,360.70 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 48 | $212.64 | $2.13 | — | $151.85 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list mover_buy; 🔵; ret5=-4.6; leftover $10360.70 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 48 | $222.86 | $2.23 | $+486.20 | $10,846.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 5 | $261.47 | $2.00 | — | $9,537.54 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; leftover $1355.86 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 90 | $14.96 | $2.26 | — | $8,188.88 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.4; leftover $1355.86 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 16 | $82.64 | $2.04 | — | $6,864.60 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.9; leftover $1355.86 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 318 | $4.26 | $4.10 | — | $5,505.82 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.7; leftover $1355.86 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 31 | $42.51 | $2.08 | — | $4,185.93 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+6.0; leftover $1355.86 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 65 | $20.75 | $2.19 | — | $2,834.99 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.9; leftover $1355.86 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 171 | $7.91 | $2.50 | — | $1,479.88 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.4; leftover $1355.86 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 33 | $40.65 | $2.09 | — | $136.34 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; leftover $1355.86 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 5 | $258.50 | $2.03 | $-18.88 | $1,426.82 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBAR` | 90 | $14.50 | $2.29 | $-45.95 | $2,729.53 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ESTC` | 16 | $99.99 | $2.06 | $+273.50 | $4,327.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FINV` | 318 | $3.46 | $4.17 | $-262.67 | $5,423.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FRO` | 31 | $43.54 | $2.10 | $+27.74 | $6,771.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 65 | $22.89 | $2.21 | $+134.71 | $8,256.70 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 171 | $8.43 | $2.54 | $+83.87 | $9,695.69 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `IREN` | 33 | $35.71 | $2.11 | $-167.22 | $10,872.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 131 | $10.30 | $2.38 | — | $9,520.33 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.7; leftover $1359.00 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $369.68 | $2.00 | — | $8,409.29 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; leftover $1359.00 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 256 | $5.30 | $3.30 | — | $7,049.19 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.1; leftover $1359.00 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 3 | $357.25 | $2.00 | — | $5,975.44 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-7.7; leftover $1359.00 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 57 | $23.80 | $2.16 | — | $4,616.68 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.5; leftover $1359.00 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 5 | $244.98 | $2.00 | — | $3,389.77 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+2.3; leftover $1359.00 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 26 | $51.99 | $2.07 | — | $2,035.96 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-9.0; leftover $1359.00 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 74 | $18.22 | $2.21 | — | $685.47 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-16.7; leftover $1359.00 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `AI` | 131 | $10.74 | $2.42 | $+52.84 | $2,090.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $351.74 | $2.02 | $-57.84 | $3,143.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CHPT` | 256 | $6.90 | $3.36 | $+402.94 | $4,906.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 3 | $354.49 | $2.02 | $-12.30 | $5,967.69 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CPB` | 57 | $22.32 | $2.18 | $-88.70 | $7,237.75 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 5 | $256.99 | $2.03 | $+56.02 | $8,520.67 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 26 | $47.60 | $2.09 | $-118.30 | $9,756.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MEI` | 74 | $15.09 | $2.23 | $-236.07 | $10,870.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 20 | $66.61 | $2.05 | — | $9,536.36 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.1; leftover $1358.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 133 | $10.16 | $2.39 | — | $8,182.69 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+4.8; leftover $1358.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 20 | $67.06 | $2.05 | — | $6,839.44 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.1; leftover $1358.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 359 | $3.78 | $4.63 | — | $5,477.79 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-2.8; leftover $1358.83 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 6 | $198.00 | $2.01 | — | $4,287.78 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.7; leftover $1358.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 36 | $37.69 | $2.10 | — | $2,928.84 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; leftover $1358.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 11 | $121.15 | $2.02 | — | $1,594.17 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.3; leftover $1358.83 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 86 | $15.62 | $2.25 | — | $248.60 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.7; leftover $1358.83 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `BMO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BNS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `EH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `GFI` | no_price | no 09:30 open — carry |
| 2026-08-26 | `GRRR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `SHMD` | no_price | no 09:30 open — carry |
| 2026-08-26 | `SLQT` | no_price | no 09:30 open |
| 2026-08-26 | `TIGR` | no_price | no 09:30 open |
| 2026-08-26 | `ANF` | no_price | no 09:30 open |
| 2026-08-26 | `BBWI` | no_price | no 09:30 open |
| 2026-08-26 | `BOX` | no_price | no 09:30 open |
| 2026-08-26 | `DY` | no_price | no 09:30 open |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `SBSW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `AMBA` | 20 | 2026-09-04 @ $66.61 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.1; leftover $1358.83 |
| `ASAN` | 133 | 2026-09-04 @ $10.16 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+4.8; leftover $1358.83 |
| `DOCU` | 20 | 2026-09-04 @ $67.06 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.1; leftover $1358.83 |
| `DOMO` | 359 | 2026-09-04 @ $3.78 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-2.8; leftover $1358.83 |
| `GWRE` | 6 | 2026-09-04 @ $198.00 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.7; leftover $1358.83 |
| `IOT` | 36 | 2026-09-04 @ $37.69 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; leftover $1358.83 |
| `LULU` | 11 | 2026-09-04 @ $121.15 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.3; leftover $1358.83 |
| `MAMA` | 86 | 2026-09-04 @ $15.62 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.7; leftover $1358.83 |
