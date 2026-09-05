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

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | INO, VOR | — | $21.06 | $10,769.53 | INO×6172, VOR×223 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $21.06 | INO×6172, VOR×223 | $10,963.61 | +194.08 | BTBT, ARX, AIRO, MH, CLBT, EU, LUNR, NMAX | INO, VOR | $11.72 | $10,869.01 | BTBT×906, ARX×69, AIRO×122, MH×100, CLBT×125, EU×1152, LUNR×70, NMAX×137 | 09:30 open · cash $21.06 (unchanged overnight, no fees) · equity $10,963.61 vs prior close $10,769.53 (+194.08) because holdings re-marked: INO×6172 yday $0.90 → 09:30 $0.93 +185.16; VOR×223 yday $23.29 → 09:30 $23.33 +8.92 |
| 2026-08-17 | +2.25 | $11.72 | BTBT×906, ARX×69, AIRO×122, MH×100, CLBT×125, EU×1152, LUNR×70, NMAX×137 | $10,935.77 | +66.76 | — | BTBT, ARX, AIRO, MH, CLBT, EU, LUNR, NMAX | $10,894.88 | $10,894.88 | — | 09:30 open · cash $11.72 (unchanged overnight, no fees) · equity $10,935.77 vs prior close $10,869.01 (+66.76) because holdings re-marked: BTBT×906 yday $1.57 → 09:30 $1.52 -45.30; ARX×69 yday $19.58 → 09:30 $19.57 -0.69; AIRO×122 yday $9.57 → 09:30 $9.57 +0.00; MH×100 yday $13.10 → 09:30 $13.16 +6.00; CLBT×125 yday $11.14 → 09:30 $11.19 +6.25; EU×1152 yday $1.21 → 09:30 $1.21 +0.00; LUNR×70 yday $19.01 → 09:30 $20.25 +86.80; NMAX×137 yday $10.87 → 09:30 $10.97 +13.70 |
| 2026-08-18 | -6.20 | $10,894.88 | — | $10,894.88 | +0.00 | — | — | $10,894.88 | $10,894.88 | — | 09:30 open · cash $10,894.88 · no holdings · equity $10,894.88 vs prior close $10,894.88 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-19 | -7.20 | $10,894.88 | — | $10,894.88 | +0.00 | — | — | $10,894.88 | $10,894.88 | — | 09:30 open · cash $10,894.88 · no holdings · equity $10,894.88 vs prior close $10,894.88 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-20 | +1.12 | $10,894.88 | — | $10,894.88 | +0.00 | EL, TOYO, DVLT, AAP, AEG, ALVO, ATAT, ATHM | — | $105.03 | $10,940.84 | EL×13, TOYO×307, DVLT×4539, AAP×29, AEG×151, ALVO×350, ATAT×39, ATHM×60 | 09:30 open · cash $10,894.88 · no holdings · equity $10,894.88 vs prior close $10,894.88 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $105.03 | EL×13, TOYO×307, DVLT×4539, AAP×29, AEG×151, ALVO×350, ATAT×39, ATHM×60 | $10,983.65 | +42.81 | FUTU, DE, WMT, BEKE, BJ, BKE, PSEC | EL, TOYO, DVLT, AEG, ALVO, ATAT, ATHM | $251.18 | $11,133.93 | AAP×29, FUTU×12, DE×2, WMT×13, BEKE×77, BJ×14, BKE×32, PSEC×602 | 09:30 open · cash $105.03 (unchanged overnight, no fees) · equity $10,983.65 vs prior close $10,940.84 (+42.81) because holdings re-marked: EL×13 yday $96.15 → 09:30 $96.75 +7.80; TOYO×307 yday $4.51 → 09:30 $4.68 +50.66; DVLT×4539 yday $0.32 → 09:30 $0.31 -45.39; AAP×29 yday $42.39 → 09:30 $42.41 +0.58; AEG×151 yday $9.01 → 09:30 $9.04 +4.53; ALVO×350 yday $4.27 → 09:30 $4.32 +17.50; ATAT×39 yday $34.25 → 09:30 $34.31 +2.34; ATHM×60 yday $22.12 → 09:30 $22.20 +4.80 |
| 2026-08-24 | -5.17 | $251.18 | AAP×29, FUTU×12, DE×2, WMT×13, BEKE×77, BJ×14, BKE×32, PSEC×602 | $11,195.70 | +61.77 | — | AAP, FUTU, DE, WMT, BEKE, BJ, BKE, PSEC | $11,173.20 | $11,173.20 | — | 09:30 open · cash $251.18 (unchanged overnight, no fees) · equity $11,195.70 vs prior close $11,133.93 (+61.77) because holdings re-marked: AAP×29 yday $42.58 → 09:30 $43.10 +15.08; FUTU×12 yday $123.64 → 09:30 $120.87 -33.24; DE×2 yday $647.47 → 09:30 $653.62 +12.30; WMT×13 yday $103.70 → 09:30 $104.16 +5.98; BEKE×77 yday $17.75 → 09:30 $18.06 +23.87; BJ×14 yday $96.42 → 09:30 $97.02 +8.40; BKE×32 yday $43.81 → 09:30 $44.54 +23.36; PSEC×602 yday $2.33 → 09:30 $2.34 +6.02 |
| 2026-08-25 | +1.80 | $11,173.20 | — | $11,173.20 | +0.00 | BMO, BNS, BZ, DKS, EH, GFI, GRRR, SHMD | — | $180.67 | $11,097.97 | BMO×8, BNS×16, BZ×91, DKS×7, EH×250, GFI×29, GRRR×97, SHMD×296 | 09:30 open · cash $11,173.20 · no holdings · equity $11,173.20 vs prior close $11,173.20 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-26 | +2.02 | $180.67 | BMO×8, BNS×16, BZ×91, DKS×7, EH×250, GFI×29, GRRR×97, SHMD×296 | $11,097.97 | -0.00 | — | — | $180.67 | $11,153.48 | BMO×8, BNS×16, BZ×91, DKS×7, EH×250, GFI×29, GRRR×97, SHMD×296 | 09:30 open · cash $180.67 (unchanged overnight, no fees) · equity $11,097.97 vs prior close $11,097.97 (-0.00) because holdings re-marked: BMO×8 yday $175.00 → 09:30 $175.00 +0.00; BNS×16 yday $90.08 → 09:30 $90.08 +0.00; BZ×91 yday $16.32 → 09:30 $16.32 +0.00; DKS×7 yday $156.70 → 09:30 $156.70 +0.00; EH×250 yday $5.28 → 09:30 $5.28 +0.00; GFI×29 yday $48.36 → 09:30 $48.36 +0.00; GRRR×97 yday $14.20 → 09:30 $14.20 +0.00; SHMD×296 yday $4.71 → 09:30 $4.71 +0.00 |
| 2026-08-27 | — | $180.67 | BMO×8, BNS×16, BZ×91, DKS×7, EH×250, GFI×29, GRRR×97, SHMD×296 | $10,380.68 | -772.80 | NVDA | BMO, BNS, BZ, DKS, EH, GFI, GRRR, SHMD | $151.85 | $10,215.53 | NVDA×48 | 09:30 open · cash $180.67 (unchanged overnight, no fees) · equity $10,380.68 vs prior close $11,153.48 (-772.80) because holdings re-marked: BMO×8 yday $175.00 → 09:30 $173.22 -14.24; BNS×16 yday $90.08 → 09:30 $92.64 +40.96; BZ×91 yday $16.32 → 09:30 $16.77 +40.95; DKS×7 yday $156.70 → 09:30 $121.87 -243.81; EH×250 yday $5.28 → 09:30 $4.77 -127.50; GFI×29 yday $48.36 → 09:30 $48.24 -3.48; GRRR×97 yday $14.20 → 09:30 $14.03 -16.49; SHMD×296 yday $4.71 → 09:30 $3.38 -393.68 |
| 2026-08-28 | +0.75 | $151.85 | NVDA×48 | $10,849.13 | +633.60 | ADSK, BBAR, ESTC, FINV, FRO, GAP, HAFN, IREN | NVDA | $136.34 | $10,884.12 | ADSK×5, BBAR×90, ESTC×16, FINV×318, FRO×31, GAP×65, HAFN×171, IREN×33 | 09:30 open · cash $151.85 (unchanged overnight, no fees) · equity $10,849.13 vs prior close $10,215.53 (+633.60) because holdings re-marked: NVDA×48 yday $209.66 → 09:30 $222.86 +633.60 |
| 2026-08-31 | -5.85 | $136.34 | ADSK×5, BBAR×90, ESTC×16, FINV×318, FRO×31, GAP×65, HAFN×171, IREN×33 | $10,891.51 | +7.39 | — | ADSK, BBAR, ESTC, FINV, FRO, GAP, HAFN, IREN | $10,872.01 | $10,872.01 | — | 09:30 open · cash $136.34 (unchanged overnight, no fees) · equity $10,891.51 vs prior close $10,884.12 (+7.39) because holdings re-marked: ADSK×5 yday $270.58 → 09:30 $258.50 -60.40; BBAR×90 yday $14.60 → 09:30 $14.50 -9.00; ESTC×16 yday $83.74 → 09:30 $99.99 +260.00; FINV×318 yday $4.02 → 09:30 $3.46 -178.08; FRO×31 yday $43.75 → 09:30 $43.54 -6.51; GAP×65 yday $20.79 → 09:30 $22.89 +136.50; HAFN×171 yday $8.29 → 09:30 $8.43 +23.94; IREN×33 yday $40.53 → 09:30 $35.71 -159.06 |
| 2026-09-01 | -6.30 | $10,872.01 | — | $10,872.01 | +0.00 | — | — | $10,872.01 | $10,872.01 | — | 09:30 open · cash $10,872.01 · no holdings · equity $10,872.01 vs prior close $10,872.01 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-02 | -3.83 | $10,872.01 | — | $10,872.01 | +0.00 | — | — | $10,872.01 | $10,872.01 | — | 09:30 open · cash $10,872.01 · no holdings · equity $10,872.01 vs prior close $10,872.01 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-03 | -0.90 | $10,872.01 | — | $10,872.01 | +0.00 | AI, AVGO, CHPT, CIEN, CPB, FIVE, HPE, MEI | — | $685.47 | $10,814.27 | AI×131, AVGO×3, CHPT×256, CIEN×3, CPB×57, FIVE×5, HPE×26, MEI×74 | 09:30 open · cash $10,872.01 · no holdings · equity $10,872.01 vs prior close $10,872.01 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $685.47 | AI×131, AVGO×3, CHPT×256, CIEN×3, CPB×57, FIVE×5, HPE×26, MEI×74 | $10,888.95 | +74.68 | AMBA, ASAN, DOCU, DOMO, GWRE, IOT, LULU, MAMA | AI, AVGO, CHPT, CIEN, CPB, FIVE, HPE, MEI | $248.60 | $10,862.37 | AMBA×20, ASAN×133, DOCU×20, DOMO×359, GWRE×6, IOT×36, LULU×11, MAMA×86 | 09:30 open · cash $685.47 (unchanged overnight, no fees) · equity $10,888.95 vs prior close $10,814.27 (+74.68) because holdings re-marked: AI×131 yday $10.52 → 09:30 $10.74 +28.82; AVGO×3 yday $367.24 → 09:30 $351.74 -46.50; CHPT×256 yday $5.19 → 09:30 $6.90 +437.76; CIEN×3 yday $354.16 → 09:30 $354.49 +0.99; CPB×57 yday $23.78 → 09:30 $22.32 -83.22; FIVE×5 yday $243.08 → 09:30 $256.99 +69.55; HPE×26 yday $51.83 → 09:30 $47.60 -109.98; MEI×74 yday $18.10 → 09:30 $15.09 -222.74 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 6172 | $0.81 | $68.51 | — | $4,932.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+13.2; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 223 | $22.01 | $2.88 | — | $21.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+0.3; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.06 | ▲ 09:30 equity $10,963.61 vs yday $10,769.53 (+194.08) | 09:30 open · cash $21.06 (unchanged overnight, no fees) · equity $10,963.61 vs prior close $10,769.53 (+194.08) because holdings re-marked: INO×6172 yday $0.90 → 09:30 $0.93 +185.16; VOR×223 yday $23.29 → 09:30 $23.33 +8.92 | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 6172 | $0.93 | $76.99 | $+595.14 | $5,684.04 | ▲ +595.14 after sell → book $10,886.63; vs 09:30 mark -76.98 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 223 | $23.33 | $2.96 | $+288.53 | $10,883.67 | ▲ +288.53 after sell → book $10,883.67; vs 09:30 mark -2.96 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 906 | $1.50 | $11.69 | — | $9,512.99 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 69 | $19.57 | $2.20 | — | $8,160.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 122 | $11.12 | $2.36 | — | $6,801.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 100 | $13.55 | $2.29 | — | $5,444.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 125 | $10.83 | $2.37 | — | $4,088.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-30.1; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 1152 | $1.18 | $14.86 | — | $2,713.84 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 70 | $19.17 | $2.20 | — | $1,369.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 137 | $9.89 | $2.40 | — | $11.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.72 | ▲ 09:30 equity $10,935.77 vs yday $10,869.01 (+66.76) | 09:30 open · cash $11.72 (unchanged overnight, no fees) · equity $10,935.77 vs prior close $10,869.01 (+66.76) because holdings re-marked: BTBT×906 yday $1.57 → 09:30 $1.52 -45.30; ARX×69 yday $19.58 → 09:30 $19.57 -0.69; AIRO×122 yday $9.57 → 09:30 $9.57 +0.00; MH×100 yday $13.10 → 09:30 $13.16 +6.00; CLBT×125 yday $11.14 → 09:30 $11.19 +6.25; EU×1152 yday $1.21 → 09:30 $1.21 +0.00; LUNR×70 yday $19.01 → 09:30 $20.25 +86.80; NMAX×137 yday $10.87 → 09:30 $10.97 +13.70 | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 906 | $1.52 | $11.85 | $-5.42 | $1,376.99 | ▼ -5.42 after sell → book $10,923.92; vs 09:30 mark -11.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 69 | $19.57 | $2.22 | $-4.42 | $2,725.10 | ▼ -4.42 after sell → book $10,921.70; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 122 | $9.57 | $2.39 | $-193.84 | $3,890.26 | ▼ -193.84 after sell → book $10,919.32; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 100 | $13.16 | $2.32 | $-43.61 | $5,203.94 | ▼ -43.61 after sell → book $10,917.00; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 125 | $11.19 | $2.40 | $+40.24 | $6,600.29 | ▲ +40.24 after sell → book $10,914.60; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `EU` | 1152 | $1.21 | $15.06 | $+4.64 | $7,979.15 | ▲ +4.64 after sell → book $10,899.54; vs 09:30 mark -15.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 70 | $20.25 | $2.22 | $+71.18 | $9,394.43 | ▲ +71.18 after sell → book $10,897.32; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `NMAX` | 137 | $10.97 | $2.44 | $+142.44 | $10,894.88 | ▲ +142.44 after sell → book $10,894.88; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,894.88 | ▲ 09:30 equity $10,894.88 vs yday $10,894.88 (+0.00) | 09:30 open · cash $10,894.88 · no holdings · equity $10,894.88 vs prior close $10,894.88 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,894.88 | ▲ 09:30 equity $10,894.88 vs yday $10,894.88 (+0.00) | 09:30 open · cash $10,894.88 · no holdings · equity $10,894.88 vs prior close $10,894.88 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,894.88 | ▲ 09:30 equity $10,894.88 vs yday $10,894.88 (+0.00) | 09:30 open · cash $10,894.88 · no holdings · equity $10,894.88 vs prior close $10,894.88 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 13 | $97.43 | $2.03 | — | $9,626.26 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; leftover $1361.86 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 307 | $4.43 | $3.96 | — | $8,262.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; leftover $1361.86 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 4539 | $0.30 | $27.23 | — | $6,873.36 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; leftover $1361.86 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 29 | $46.85 | $2.08 | — | $5,512.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; leftover $1361.86 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 151 | $9.01 | $2.44 | — | $4,149.68 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; leftover $1361.86 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 350 | $3.89 | $4.51 | — | $2,783.66 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; leftover $1361.86 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 39 | $34.05 | $2.11 | — | $1,453.60 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; leftover $1361.86 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 60 | $22.44 | $2.17 | — | $105.03 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; leftover $1361.86 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $105.03 | ▲ 09:30 equity $10,983.65 vs yday $10,940.84 (+42.81) | 09:30 open · cash $105.03 (unchanged overnight, no fees) · equity $10,983.65 vs prior close $10,940.84 (+42.81) because holdings re-marked: EL×13 yday $96.15 → 09:30 $96.75 +7.80; TOYO×307 yday $4.51 → 09:30 $4.68 +50.66; DVLT×4539 yday $0.32 → 09:30 $0.31 -45.39; AAP×29 yday $42.39 → 09:30 $42.41 +0.58; AEG×151 yday $9.01 → 09:30 $9.04 +4.53; ALVO×350 yday $4.27 → 09:30 $4.32 +17.50; ATAT×39 yday $34.25 → 09:30 $34.31 +2.34; ATHM×60 yday $22.12 → 09:30 $22.20 +4.80 | — |
| 2026-08-21 09:30 ET | **SELL** | `EL` | 13 | $96.75 | $2.05 | $-12.92 | $1,360.74 | ▼ -12.92 after sell → book $10,981.61; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TOYO` | 307 | $4.68 | $4.02 | $+68.77 | $2,793.47 | ▲ +68.77 after sell → book $10,977.58; vs 09:30 mark -4.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DVLT` | 4539 | $0.31 | $28.45 | $-10.30 | $4,172.11 | ▼ -10.30 after sell → book $10,949.13; vs 09:30 mark -28.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AEG` | 151 | $9.04 | $2.48 | $-0.39 | $5,534.67 | ▼ -0.39 after sell → book $10,946.65; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ALVO` | 350 | $4.32 | $4.59 | $+141.40 | $7,042.09 | ▲ +141.40 after sell → book $10,942.07; vs 09:30 mark -4.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 39 | $34.31 | $2.13 | $+5.91 | $8,378.05 | ▲ +5.91 after sell → book $10,939.94; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATHM` | 60 | $22.20 | $2.19 | $-18.76 | $9,707.86 | ▼ -18.76 after sell → book $10,937.75; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 12 | $115.18 | $2.03 | — | $8,323.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1386.84 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $7,075.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1386.84 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `WMT` | 13 | $103.69 | $2.03 | — | $5,725.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; ret5=-10.5; leftover $1386.84 | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 77 | $17.93 | $2.22 | — | $4,341.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=+0.2; leftover $1386.84 | join🟢 sector🟢 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 14 | $93.98 | $2.03 | — | $3,024.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.4; leftover $1386.84 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 32 | $43.08 | $2.09 | — | $1,643.54 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; leftover $1386.84 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 602 | $2.30 | $7.77 | — | $251.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; leftover $1386.84 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $251.18 | ▲ 09:30 equity $11,195.70 vs yday $11,133.93 (+61.77) | 09:30 open · cash $251.18 (unchanged overnight, no fees) · equity $11,195.70 vs prior close $11,133.93 (+61.77) because holdings re-marked: AAP×29 yday $42.58 → 09:30 $43.10 +15.08; FUTU×12 yday $123.64 → 09:30 $120.87 -33.24; DE×2 yday $647.47 → 09:30 $653.62 +12.30; WMT×13 yday $103.70 → 09:30 $104.16 +5.98; BEKE×77 yday $17.75 → 09:30 $18.06 +23.87; BJ×14 yday $96.42 → 09:30 $97.02 +8.40; BKE×32 yday $43.81 → 09:30 $44.54 +23.36; PSEC×602 yday $2.33 → 09:30 $2.34 +6.02 | — |
| 2026-08-24 09:30 ET | **SELL** | `AAP` | 29 | $43.10 | $2.10 | $-112.92 | $1,498.98 | ▼ -112.92 after sell → book $11,193.60; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 12 | $120.87 | $2.05 | $+64.21 | $2,947.37 | ▲ +64.21 after sell → book $11,191.55; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.62 | $2.02 | $+56.71 | $4,252.60 | ▲ +56.71 after sell → book $11,189.54; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `WMT` | 13 | $104.16 | $2.05 | $+2.03 | $5,604.63 | ▲ +2.03 after sell → book $11,187.49; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 77 | $18.06 | $2.24 | $+5.16 | $6,993.00 | ▲ +5.16 after sell → book $11,185.24; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 14 | $97.02 | $2.05 | $+38.48 | $8,349.23 | ▲ +38.48 after sell → book $11,183.19; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 32 | $44.54 | $2.11 | $+42.53 | $9,772.40 | ▲ +42.53 after sell → book $11,181.08; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PSEC` | 602 | $2.34 | $7.88 | $+8.44 | $11,173.20 | ▲ +8.44 after sell → book $11,173.20; vs 09:30 mark -7.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,173.20 | ▲ 09:30 equity $11,173.20 vs yday $11,173.20 (+0.00) | 09:30 open · cash $11,173.20 · no holdings · equity $11,173.20 vs prior close $11,173.20 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 8 | $172.40 | $2.01 | — | $9,791.99 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-6.1; leftover $1396.65 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 16 | $86.86 | $2.04 | — | $8,400.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.3; leftover $1396.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 91 | $15.34 | $2.26 | — | $7,001.99 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=+2.8; leftover $1396.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 7 | $179.33 | $2.01 | — | $5,744.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-9.3; leftover $1396.65 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 250 | $5.57 | $3.23 | — | $4,348.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-7.1; leftover $1396.65 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 29 | $47.68 | $2.08 | — | $2,964.15 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+18.8; leftover $1396.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 97 | $14.26 | $2.28 | — | $1,578.64 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-1.9; leftover $1396.65 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 296 | $4.71 | $3.82 | — | $180.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-9.9; leftover $1396.65 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $180.67 | ▲ 09:30 equity $11,097.97 vs yday $11,097.97 (-0.00) | 09:30 open · cash $180.67 (unchanged overnight, no fees) · equity $11,097.97 vs prior close $11,097.97 (-0.00) because holdings re-marked: BMO×8 yday $175.00 → 09:30 $175.00 +0.00; BNS×16 yday $90.08 → 09:30 $90.08 +0.00; BZ×91 yday $16.32 → 09:30 $16.32 +0.00; DKS×7 yday $156.70 → 09:30 $156.70 +0.00; EH×250 yday $5.28 → 09:30 $5.28 +0.00; GFI×29 yday $48.36 → 09:30 $48.36 +0.00; GRRR×97 yday $14.20 → 09:30 $14.20 +0.00; SHMD×296 yday $4.71 → 09:30 $4.71 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $180.67 | ▼ 09:30 equity $10,380.68 vs yday $11,153.48 (-772.80) | 09:30 open · cash $180.67 (unchanged overnight, no fees) · equity $10,380.68 vs prior close $11,153.48 (-772.80) because holdings re-marked: BMO×8 yday $175.00 → 09:30 $173.22 -14.24; BNS×16 yday $90.08 → 09:30 $92.64 +40.96; BZ×91 yday $16.32 → 09:30 $16.77 +40.95; DKS×7 yday $156.70 → 09:30 $121.87 -243.81; EH×250 yday $5.28 → 09:30 $4.77 -127.50; GFI×29 yday $48.36 → 09:30 $48.24 -3.48; GRRR×97 yday $14.20 → 09:30 $14.03 -16.49; SHMD×296 yday $4.71 → 09:30 $3.38 -393.68 | — |
| 2026-08-27 09:30 ET | **SELL** | `BMO` | 8 | $173.22 | $2.04 | $+2.51 | $1,564.39 | ▲ +2.51 after sell → book $10,378.64; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BNS` | 16 | $92.64 | $2.06 | $+88.38 | $3,044.57 | ▲ +88.38 after sell → book $10,376.58; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 91 | $16.77 | $2.29 | $+125.58 | $4,568.35 | ▲ +125.58 after sell → book $10,374.29; vs 09:30 mark -2.29 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 7 | $121.87 | $2.03 | $-406.26 | $5,419.41 | ▼ -406.26 after sell → book $10,372.26; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `EH` | 250 | $4.77 | $3.28 | $-206.50 | $6,608.63 | ▼ -206.50 after sell → book $10,368.98; vs 09:30 mark -3.28 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `GFI` | 29 | $48.24 | $2.10 | $+12.06 | $8,005.50 | ▲ +12.06 after sell → book $10,366.89; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `GRRR` | 97 | $14.03 | $2.31 | $-26.90 | $9,364.10 | ▼ -26.90 after sell → book $10,364.58; vs 09:30 mark -2.31 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SHMD` | 296 | $3.38 | $3.88 | $-401.38 | $10,360.70 | ▼ -401.38 after sell → book $10,360.70; vs 09:30 mark -3.88 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 48 | $212.64 | $2.13 | — | $151.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list mover_buy; 🔵; ret5=-4.6; leftover $10360.70 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.85 | ▲ 09:30 equity $10,849.13 vs yday $10,215.53 (+633.60) | 09:30 open · cash $151.85 (unchanged overnight, no fees) · equity $10,849.13 vs prior close $10,215.53 (+633.60) because holdings re-marked: NVDA×48 yday $209.66 → 09:30 $222.86 +633.60 | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 48 | $222.86 | $2.23 | $+486.20 | $10,846.90 | ▲ +486.20 after sell → book $10,846.90; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 5 | $261.47 | $2.00 | — | $9,537.54 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; leftover $1355.86 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 90 | $14.96 | $2.26 | — | $8,188.88 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.4; leftover $1355.86 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 16 | $82.64 | $2.04 | — | $6,864.60 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.9; leftover $1355.86 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 318 | $4.26 | $4.10 | — | $5,505.82 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.7; leftover $1355.86 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 31 | $42.51 | $2.08 | — | $4,185.93 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+6.0; leftover $1355.86 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 65 | $20.75 | $2.19 | — | $2,834.99 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.9; leftover $1355.86 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 171 | $7.91 | $2.50 | — | $1,479.88 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.4; leftover $1355.86 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 33 | $40.65 | $2.09 | — | $136.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; leftover $1355.86 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $136.34 | ▲ 09:30 equity $10,891.51 vs yday $10,884.12 (+7.39) | 09:30 open · cash $136.34 (unchanged overnight, no fees) · equity $10,891.51 vs prior close $10,884.12 (+7.39) because holdings re-marked: ADSK×5 yday $270.58 → 09:30 $258.50 -60.40; BBAR×90 yday $14.60 → 09:30 $14.50 -9.00; ESTC×16 yday $83.74 → 09:30 $99.99 +260.00; FINV×318 yday $4.02 → 09:30 $3.46 -178.08; FRO×31 yday $43.75 → 09:30 $43.54 -6.51; GAP×65 yday $20.79 → 09:30 $22.89 +136.50; HAFN×171 yday $8.29 → 09:30 $8.43 +23.94; IREN×33 yday $40.53 → 09:30 $35.71 -159.06 | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 5 | $258.50 | $2.03 | $-18.88 | $1,426.82 | ▼ -18.88 after sell → book $10,889.49; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBAR` | 90 | $14.50 | $2.29 | $-45.95 | $2,729.53 | ▼ -45.95 after sell → book $10,887.20; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ESTC` | 16 | $99.99 | $2.06 | $+273.50 | $4,327.31 | ▲ +273.50 after sell → book $10,885.14; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FINV` | 318 | $3.46 | $4.17 | $-262.67 | $5,423.42 | ▼ -262.67 after sell → book $10,880.97; vs 09:30 mark -4.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FRO` | 31 | $43.54 | $2.10 | $+27.74 | $6,771.06 | ▲ +27.74 after sell → book $10,878.87; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 65 | $22.89 | $2.21 | $+134.71 | $8,256.70 | ▲ +134.71 after sell → book $10,876.66; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 171 | $8.43 | $2.54 | $+83.87 | $9,695.69 | ▲ +83.87 after sell → book $10,874.12; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `IREN` | 33 | $35.71 | $2.11 | $-167.22 | $10,872.01 | ▼ -167.22 after sell → book $10,872.01; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,872.01 | ▲ 09:30 equity $10,872.01 vs yday $10,872.01 (+0.00) | 09:30 open · cash $10,872.01 · no holdings · equity $10,872.01 vs prior close $10,872.01 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,872.01 | ▲ 09:30 equity $10,872.01 vs yday $10,872.01 (+0.00) | 09:30 open · cash $10,872.01 · no holdings · equity $10,872.01 vs prior close $10,872.01 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,872.01 | ▲ 09:30 equity $10,872.01 vs yday $10,872.01 (+0.00) | 09:30 open · cash $10,872.01 · no holdings · equity $10,872.01 vs prior close $10,872.01 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 131 | $10.30 | $2.38 | — | $9,520.33 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.7; leftover $1359.00 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $369.68 | $2.00 | — | $8,409.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; leftover $1359.00 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 256 | $5.30 | $3.30 | — | $7,049.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.1; leftover $1359.00 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 3 | $357.25 | $2.00 | — | $5,975.44 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-7.7; leftover $1359.00 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 57 | $23.80 | $2.16 | — | $4,616.68 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.5; leftover $1359.00 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 5 | $244.98 | $2.00 | — | $3,389.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+2.3; leftover $1359.00 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 26 | $51.99 | $2.07 | — | $2,035.96 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-9.0; leftover $1359.00 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 74 | $18.22 | $2.21 | — | $685.47 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-16.7; leftover $1359.00 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $685.47 | ▲ 09:30 equity $10,888.95 vs yday $10,814.27 (+74.68) | 09:30 open · cash $685.47 (unchanged overnight, no fees) · equity $10,888.95 vs prior close $10,814.27 (+74.68) because holdings re-marked: AI×131 yday $10.52 → 09:30 $10.74 +28.82; AVGO×3 yday $367.24 → 09:30 $351.74 -46.50; CHPT×256 yday $5.19 → 09:30 $6.90 +437.76; CIEN×3 yday $354.16 → 09:30 $354.49 +0.99; CPB×57 yday $23.78 → 09:30 $22.32 -83.22; FIVE×5 yday $243.08 → 09:30 $256.99 +69.55; HPE×26 yday $51.83 → 09:30 $47.60 -109.98; MEI×74 yday $18.10 → 09:30 $15.09 -222.74 | — |
| 2026-09-04 09:30 ET | **SELL** | `AI` | 131 | $10.74 | $2.42 | $+52.84 | $2,090.00 | ▲ +52.84 after sell → book $10,886.54; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $351.74 | $2.02 | $-57.84 | $3,143.20 | ▼ -57.84 after sell → book $10,884.52; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CHPT` | 256 | $6.90 | $3.36 | $+402.94 | $4,906.24 | ▲ +402.94 after sell → book $10,881.16; vs 09:30 mark -3.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 3 | $354.49 | $2.02 | $-12.30 | $5,967.69 | ▼ -12.30 after sell → book $10,879.14; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CPB` | 57 | $22.32 | $2.18 | $-88.70 | $7,237.75 | ▼ -88.70 after sell → book $10,876.96; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 5 | $256.99 | $2.03 | $+56.02 | $8,520.67 | ▲ +56.02 after sell → book $10,874.93; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 26 | $47.60 | $2.09 | $-118.30 | $9,756.18 | ▼ -118.30 after sell → book $10,872.84; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MEI` | 74 | $15.09 | $2.23 | $-236.07 | $10,870.61 | ▼ -236.07 after sell → book $10,870.61; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 20 | $66.61 | $2.05 | — | $9,536.36 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.1; leftover $1358.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 133 | $10.16 | $2.39 | — | $8,182.69 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+4.8; leftover $1358.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 20 | $67.06 | $2.05 | — | $6,839.44 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.1; leftover $1358.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 359 | $3.78 | $4.63 | — | $5,477.79 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-2.8; leftover $1358.83 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 6 | $198.00 | $2.01 | — | $4,287.78 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.7; leftover $1358.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 36 | $37.69 | $2.10 | — | $2,928.84 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; leftover $1358.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 11 | $121.15 | $2.02 | — | $1,594.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.3; leftover $1358.83 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 86 | $15.62 | $2.25 | — | $248.60 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.7; leftover $1358.83 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |

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
