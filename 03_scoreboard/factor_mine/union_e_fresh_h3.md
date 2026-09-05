# Factor mine action — `union_e_fresh_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ e_fresh, no 🚨

Cash book **+27.57%** ($12,757) · signal-only (no cash/fees) was -12.67%. Starts YES **16/17**. Fills 69 · skips 114 · realized $+2125.57.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `days_since_E_max=1,flag_E_min=0` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $126.14.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | INO, VOR | — | $21.06 | $10,769.53 | INO×6172, VOR×223 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $21.06 | INO×6172, VOR×223 | $10,963.61 | +194.08 | BTBT, EU | — | $17.16 | $11,884.32 | INO×6172, VOR×223, BTBT×1, EU×2 | 09:30 open · cash $21.06 (unchanged overnight, no fees) · equity $10,963.61 vs prior close $10,769.53 (+194.08) because holdings re-marked: INO×6172 yday $0.90 → 09:30 $0.93 +185.16; VOR×223 yday $23.29 → 09:30 $23.33 +8.92 |
| 2026-08-17 | +2.25 | $17.16 | INO×6172, VOR×223, BTBT×1, EU×2 | $11,734.07 | -150.25 | — | — | $17.16 | $12,250.05 | INO×6172, VOR×223, BTBT×1, EU×2 | 09:30 open · cash $17.16 (unchanged overnight, no fees) · equity $11,734.07 vs prior close $11,884.32 (-150.25) because holdings re-marked: INO×6172 yday $1.09 → 09:30 $1.07 -123.44; VOR×223 yday $23.03 → 09:30 $22.91 -26.76; BTBT×1 yday $1.57 → 09:30 $1.52 -0.05; EU×2 yday $1.21 → 09:30 $1.21 +0.00 |
| 2026-08-18 | -6.20 | $17.16 | INO×6172, VOR×223, BTBT×1, EU×2 | $12,145.90 | -104.15 | — | INO, VOR | $12,058.44 | $12,062.03 | BTBT×1, EU×2 | 09:30 open · cash $17.16 (unchanged overnight, no fees) · equity $12,145.90 vs prior close $12,250.05 (-104.15) because holdings re-marked: INO×6172 yday $1.15 → 09:30 $1.14 -61.72; VOR×223 yday $23.01 → 09:30 $22.82 -42.37; BTBT×1 yday $1.60 → 09:30 $1.54 -0.06; EU×2 yday $1.13 → 09:30 $1.13 +0.00 |
| 2026-08-19 | -7.20 | $12,058.44 | BTBT×1, EU×2 | $12,062.00 | -0.03 | — | BTBT, EU | $12,061.92 | $12,061.92 | — | 09:30 open · cash $12,058.44 (unchanged overnight, no fees) · equity $12,062.00 vs prior close $12,062.03 (-0.03) because holdings re-marked: BTBT×1 yday $1.45 → 09:30 $1.42 -0.03; EU×2 yday $1.07 → 09:30 $1.07 +0.00 |
| 2026-08-20 | +1.12 | $12,061.92 | — | $12,061.92 | -0.00 | EL, TOYO, DVLT, AAP, AEG, ALVO, ATAT, ATHM | — | $25.33 | $12,113.36 | EL×15, TOYO×340, DVLT×5025, AAP×32, AEG×167, ALVO×387, ATAT×44, ATHM×67 | 09:30 open · cash $12,061.92 · no holdings · equity $12,061.92 vs prior close $12,061.92 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $25.33 | EL×15, TOYO×340, DVLT×5025, AAP×32, AEG×167, ALVO×387, ATAT×44, ATHM×67 | $12,161.21 | +47.85 | PSEC | — | $23.01 | $12,397.28 | EL×15, TOYO×340, DVLT×5025, AAP×32, AEG×167, ALVO×387, ATAT×44, ATHM×67, PSEC×1 | 09:30 open · cash $25.33 (unchanged overnight, no fees) · equity $12,161.21 vs prior close $12,113.36 (+47.85) because holdings re-marked: EL×15 yday $96.15 → 09:30 $96.75 +9.00; TOYO×340 yday $4.51 → 09:30 $4.68 +56.10; DVLT×5025 yday $0.32 → 09:30 $0.31 -50.25; AAP×32 yday $42.39 → 09:30 $42.41 +0.64; AEG×167 yday $9.01 → 09:30 $9.04 +5.01; ALVO×387 yday $4.27 → 09:30 $4.32 +19.35; ATAT×44 yday $34.25 → 09:30 $34.31 +2.64; ATHM×67 yday $22.12 → 09:30 $22.20 +5.36 |
| 2026-08-24 | -5.17 | $23.01 | EL×15, TOYO×340, DVLT×5025, AAP×32, AEG×167, ALVO×387, ATAT×44, ATHM×67, PSEC×1 | $12,414.21 | +16.93 | — | — | $23.01 | $12,620.03 | EL×15, TOYO×340, DVLT×5025, AAP×32, AEG×167, ALVO×387, ATAT×44, ATHM×67, PSEC×1 | 09:30 open · cash $23.01 (unchanged overnight, no fees) · equity $12,414.21 vs prior close $12,397.28 (+16.93) because holdings re-marked: EL×15 yday $101.94 → 09:30 $101.68 -3.90; TOYO×340 yday $4.82 → 09:30 $4.58 -81.60; DVLT×5025 yday $0.32 → 09:30 $0.31 -50.25; AAP×32 yday $42.58 → 09:30 $43.10 +16.64; AEG×167 yday $8.99 → 09:30 $9.16 +28.39; ALVO×387 yday $4.43 → 09:30 $4.79 +139.32; ATAT×44 yday $34.75 → 09:30 $34.70 -2.20; ATHM×67 yday $22.22 → 09:30 $21.78 -29.48; PSEC×1 yday $2.33 → 09:30 $2.34 +0.01 |
| 2026-08-25 | +1.80 | $23.01 | EL×15, TOYO×340, DVLT×5025, AAP×32, AEG×167, ALVO×387, ATAT×44, ATHM×67, PSEC×1 | $12,677.62 | +57.59 | BMO, BNS, BZ, DKS, EH, GFI, GRRR, SHMD | EL, TOYO, DVLT, AAP, AEG, ALVO, ATAT, ATHM | $196.09 | $12,538.36 | PSEC×1, BMO×9, BNS×18, BZ×102, DKS×8, EH×283, GFI×33, GRRR×110, SHMD×334 | 09:30 open · cash $23.01 (unchanged overnight, no fees) · equity $12,677.62 vs prior close $12,620.03 (+57.59) because holdings re-marked: EL×15 yday $102.85 → 09:30 $104.07 +18.30; TOYO×340 yday $4.61 → 09:30 $4.48 -44.20; DVLT×5025 yday $0.31 → 09:30 $0.32 +50.25; AAP×32 yday $43.83 → 09:30 $43.61 -7.04; AEG×167 yday $9.19 → 09:30 $9.29 +16.70; ALVO×387 yday $5.15 → 09:30 $5.22 +27.09; ATAT×44 yday $34.83 → 09:30 $34.75 -3.52; ATHM×67 yday $21.85 → 09:30 $21.85 +0.00; PSEC×1 yday $2.31 → 09:30 $2.32 +0.01 |
| 2026-08-26 | +2.02 | $196.09 | PSEC×1, BMO×9, BNS×18, BZ×102, DKS×8, EH×283, GFI×33, GRRR×110, SHMD×334 | $12,538.36 | -0.00 | — | — | $196.09 | $12,604.30 | PSEC×1, BMO×9, BNS×18, BZ×102, DKS×8, EH×283, GFI×33, GRRR×110, SHMD×334 | 09:30 open · cash $196.09 (unchanged overnight, no fees) · equity $12,538.36 vs prior close $12,538.36 (-0.00) because holdings re-marked: PSEC×1 yday $2.33 → 09:30 $2.33 +0.00; BMO×9 yday $175.00 → 09:30 $175.00 +0.00; BNS×18 yday $90.08 → 09:30 $90.08 +0.00; BZ×102 yday $16.32 → 09:30 $16.32 +0.00; DKS×8 yday $156.70 → 09:30 $156.70 +0.00; EH×283 yday $5.28 → 09:30 $5.28 +0.00; GFI×33 yday $48.36 → 09:30 $48.36 +0.00; GRRR×110 yday $14.20 → 09:30 $14.20 +0.00; SHMD×334 yday $4.71 → 09:30 $4.71 +0.00 |
| 2026-08-27 | — | $196.09 | PSEC×1, BMO×9, BNS×18, BZ×102, DKS×8, EH×283, GFI×33, GRRR×110, SHMD×334 | $11,724.49 | -879.81 | — | PSEC | $198.39 | $12,109.79 | BMO×9, BNS×18, BZ×102, DKS×8, EH×283, GFI×33, GRRR×110, SHMD×334 | 09:30 open · cash $196.09 (unchanged overnight, no fees) · equity $11,724.49 vs prior close $12,604.30 (-879.81) because holdings re-marked: PSEC×1 yday $2.33 → 09:30 $2.35 +0.02; BMO×9 yday $175.00 → 09:30 $173.22 -16.02; BNS×18 yday $90.08 → 09:30 $92.64 +46.08; BZ×102 yday $16.32 → 09:30 $16.77 +45.90; DKS×8 yday $156.70 → 09:30 $121.87 -278.64; EH×283 yday $5.28 → 09:30 $4.77 -144.33; GFI×33 yday $48.36 → 09:30 $48.24 -3.96; GRRR×110 yday $14.20 → 09:30 $14.03 -18.70; SHMD×334 yday $4.71 → 09:30 $3.38 -444.22 |
| 2026-08-28 | +0.75 | $198.39 | BMO×9, BNS×18, BZ×102, DKS×8, EH×283, GFI×33, GRRR×110, SHMD×334 | $12,131.47 | +21.68 | ADSK, BBAR, ESTC, FINV, FRO, GAP, HAFN, IREN | BMO, BNS, BZ, DKS, EH, GFI, GRRR, SHMD | $275.73 | $12,148.78 | ADSK×5, BBAR×101, ESTC×18, FINV×355, FRO×35, GAP×72, HAFN×191, IREN×37 | 09:30 open · cash $198.39 (unchanged overnight, no fees) · equity $12,131.47 vs prior close $12,109.79 (+21.68) because holdings re-marked: BMO×9 yday $172.90 → 09:30 $172.85 -0.45; BNS×18 yday $93.59 → 09:30 $93.52 -1.26; BZ×102 yday $18.84 → 09:30 $18.50 -34.68; DKS×8 yday $129.66 → 09:30 $128.73 -7.44; EH×283 yday $4.86 → 09:30 $4.90 +11.32; GFI×33 yday $47.82 → 09:30 $47.93 +3.63; GRRR×110 yday $15.45 → 09:30 $15.94 +53.90; SHMD×334 yday $3.17 → 09:30 $3.16 -3.34 |
| 2026-08-31 | -5.85 | $275.73 | ADSK×5, BBAR×101, ESTC×18, FINV×355, FRO×35, GAP×72, HAFN×191, IREN×37 | $12,164.23 | +15.45 | — | — | $275.73 | $12,203.68 | ADSK×5, BBAR×101, ESTC×18, FINV×355, FRO×35, GAP×72, HAFN×191, IREN×37 | 09:30 open · cash $275.73 (unchanged overnight, no fees) · equity $12,164.23 vs prior close $12,148.78 (+15.45) because holdings re-marked: ADSK×5 yday $270.58 → 09:30 $258.50 -60.40; BBAR×101 yday $14.60 → 09:30 $14.50 -10.10; ESTC×18 yday $83.74 → 09:30 $99.99 +292.50; FINV×355 yday $4.02 → 09:30 $3.46 -198.80; FRO×35 yday $43.75 → 09:30 $43.54 -7.35; GAP×72 yday $20.79 → 09:30 $22.89 +151.20; HAFN×191 yday $8.29 → 09:30 $8.43 +26.74; IREN×37 yday $40.53 → 09:30 $35.71 -178.34 |
| 2026-09-01 | -6.30 | $275.73 | ADSK×5, BBAR×101, ESTC×18, FINV×355, FRO×35, GAP×72, HAFN×191, IREN×37 | $12,241.88 | +38.20 | — | — | $275.73 | $12,210.86 | ADSK×5, BBAR×101, ESTC×18, FINV×355, FRO×35, GAP×72, HAFN×191, IREN×37 | 09:30 open · cash $275.73 (unchanged overnight, no fees) · equity $12,241.88 vs prior close $12,203.68 (+38.20) because holdings re-marked: ADSK×5 yday $259.14 → 09:30 $258.17 -4.85; BBAR×101 yday $14.50 → 09:30 $15.14 +64.64; ESTC×18 yday $99.00 → 09:30 $96.54 -44.28; FINV×355 yday $3.46 → 09:30 $3.67 +74.55; FRO×35 yday $44.09 → 09:30 $43.60 -17.15; GAP×72 yday $23.30 → 09:30 $22.28 -73.44; HAFN×191 yday $8.45 → 09:30 $8.43 -3.82; IREN×37 yday $35.75 → 09:30 $36.90 +42.55 |
| 2026-09-02 | -3.83 | $275.73 | ADSK×5, BBAR×101, ESTC×18, FINV×355, FRO×35, GAP×72, HAFN×191, IREN×37 | $12,145.70 | -65.16 | — | ADSK, BBAR, ESTC, FINV, FRO, GAP, HAFN, IREN | $12,125.56 | $12,125.56 | — | 09:30 open · cash $275.73 (unchanged overnight, no fees) · equity $12,145.70 vs prior close $12,210.86 (-65.16) because holdings re-marked: ADSK×5 yday $259.89 → 09:30 $253.48 -32.05; BBAR×101 yday $15.14 → 09:30 $14.82 -32.32; ESTC×18 yday $96.07 → 09:30 $95.76 -5.58; FINV×355 yday $3.67 → 09:30 $3.58 -31.95; FRO×35 yday $43.66 → 09:30 $44.39 +25.55; GAP×72 yday $22.20 → 09:30 $22.05 -10.80; HAFN×191 yday $8.41 → 09:30 $8.56 +28.65; IREN×37 yday $36.26 → 09:30 $36.08 -6.66 |
| 2026-09-03 | -0.90 | $12,125.56 | — | $12,125.56 | -0.00 | AI, AVGO, CHPT, CIEN, CPB, FIVE, HPE, MEI | — | $185.37 | $12,058.55 | AI×147, AVGO×4, CHPT×285, CIEN×4, CPB×63, FIVE×6, HPE×29, MEI×83 | 09:30 open · cash $12,125.56 · no holdings · equity $12,125.56 vs prior close $12,125.56 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $185.37 | AI×147, AVGO×4, CHPT×285, CIEN×4, CPB×63, FIVE×6, HPE×29, MEI×83 | $12,136.54 | +77.99 | ASAN, DOMO, MAMA | — | $126.14 | $12,757.24 | AI×147, AVGO×4, CHPT×285, CIEN×4, CPB×63, FIVE×6, HPE×29, MEI×83, ASAN×2, DOMO×6, MAMA×1 | 09:30 open · cash $185.37 (unchanged overnight, no fees) · equity $12,136.54 vs prior close $12,058.55 (+77.99) because holdings re-marked: AI×147 yday $10.52 → 09:30 $10.74 +32.34; AVGO×4 yday $367.24 → 09:30 $351.74 -62.00; CHPT×285 yday $5.19 → 09:30 $6.90 +487.35; CIEN×4 yday $354.16 → 09:30 $354.49 +1.32; CPB×63 yday $23.78 → 09:30 $22.32 -91.98; FIVE×6 yday $243.08 → 09:30 $256.99 +83.46; HPE×29 yday $51.83 → 09:30 $47.60 -122.67; MEI×83 yday $18.10 → 09:30 $15.09 -249.83 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 6172 | $0.81 | $68.51 | — | $4,932.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+13.2; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 223 | $22.01 | $2.88 | — | $21.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+0.3; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.06 | ▲ 09:30 equity $10,963.61 vs yday $10,769.53 (+194.08) | 09:30 open · cash $21.06 (unchanged overnight, no fees) · equity $10,963.61 vs prior close $10,769.53 (+194.08) because holdings re-marked: INO×6172 yday $0.90 → 09:30 $0.93 +185.16; VOR×223 yday $23.29 → 09:30 $23.33 +8.92 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 1 | $1.50 | $0.02 | — | $19.55 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; 🔵; ⚪; ret5=+9.2; leftover $2.63 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 2 | $1.18 | $0.03 | — | $17.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $2.63 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.16 | ▼ 09:30 equity $11,734.07 vs yday $11,884.32 (-150.25) | 09:30 open · cash $17.16 (unchanged overnight, no fees) · equity $11,734.07 vs prior close $11,884.32 (-150.25) because holdings re-marked: INO×6172 yday $1.09 → 09:30 $1.07 -123.44; VOR×223 yday $23.03 → 09:30 $22.91 -26.76; BTBT×1 yday $1.57 → 09:30 $1.52 -0.05; EU×2 yday $1.21 → 09:30 $1.21 +0.00 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.16 | ▼ 09:30 equity $12,145.90 vs yday $12,250.05 (-104.15) | 09:30 open · cash $17.16 (unchanged overnight, no fees) · equity $12,145.90 vs prior close $12,250.05 (-104.15) because holdings re-marked: INO×6172 yday $1.15 → 09:30 $1.14 -61.72; VOR×223 yday $23.01 → 09:30 $22.82 -42.37; BTBT×1 yday $1.60 → 09:30 $1.54 -0.06; EU×2 yday $1.13 → 09:30 $1.13 +0.00 | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 6172 | $1.14 | $80.70 | $+1887.55 | $6,972.54 | ▲ +1,887.55 after sell → book $12,065.20; vs 09:30 mark -80.70 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 223 | $22.82 | $2.95 | $+174.80 | $12,058.44 | ▲ +174.80 after sell → book $12,062.24; vs 09:30 mark -2.96 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,058.44 | ▼ 09:30 equity $12,062.00 vs yday $12,062.03 (-0.03) | 09:30 open · cash $12,058.44 (unchanged overnight, no fees) · equity $12,062.00 vs prior close $12,062.03 (-0.03) because holdings re-marked: BTBT×1 yday $1.45 → 09:30 $1.42 -0.03; EU×2 yday $1.07 → 09:30 $1.07 +0.00 | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 1 | $1.42 | $0.04 | $-0.14 | $12,059.83 | ▼ -0.14 after sell → book $12,061.97; vs 09:30 mark -0.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `EU` | 2 | $1.07 | $0.05 | $-0.30 | $12,061.92 | ▼ -0.30 after sell → book $12,061.92; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,061.92 | ▲ 09:30 equity $12,061.92 vs yday $12,061.92 (-0.00) | 09:30 open · cash $12,061.92 · no holdings · equity $12,061.92 vs prior close $12,061.92 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 15 | $97.43 | $2.04 | — | $10,598.43 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; leftover $1507.74 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 340 | $4.43 | $4.39 | — | $9,087.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; leftover $1507.74 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 5025 | $0.30 | $30.15 | — | $7,550.20 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; leftover $1507.74 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 32 | $46.85 | $2.09 | — | $6,048.91 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; leftover $1507.74 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 167 | $9.01 | $2.49 | — | $4,541.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; leftover $1507.74 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 387 | $3.89 | $4.99 | — | $3,031.33 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; leftover $1507.74 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 44 | $34.05 | $2.12 | — | $1,531.01 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; leftover $1507.74 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 67 | $22.44 | $2.19 | — | $25.33 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; leftover $1507.74 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.33 | ▲ 09:30 equity $12,161.21 vs yday $12,113.36 (+47.85) | 09:30 open · cash $25.33 (unchanged overnight, no fees) · equity $12,161.21 vs prior close $12,113.36 (+47.85) because holdings re-marked: EL×15 yday $96.15 → 09:30 $96.75 +9.00; TOYO×340 yday $4.51 → 09:30 $4.68 +56.10; DVLT×5025 yday $0.32 → 09:30 $0.31 -50.25; AAP×32 yday $42.39 → 09:30 $42.41 +0.64; AEG×167 yday $9.01 → 09:30 $9.04 +5.01; ALVO×387 yday $4.27 → 09:30 $4.32 +19.35; ATAT×44 yday $34.25 → 09:30 $34.31 +2.64; ATHM×67 yday $22.12 → 09:30 $22.20 +5.36 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 1 | $2.30 | $0.03 | — | $23.01 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; leftover $3.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.01 | ▲ 09:30 equity $12,414.21 vs yday $12,397.28 (+16.93) | 09:30 open · cash $23.01 (unchanged overnight, no fees) · equity $12,414.21 vs prior close $12,397.28 (+16.93) because holdings re-marked: EL×15 yday $101.94 → 09:30 $101.68 -3.90; TOYO×340 yday $4.82 → 09:30 $4.58 -81.60; DVLT×5025 yday $0.32 → 09:30 $0.31 -50.25; AAP×32 yday $42.58 → 09:30 $43.10 +16.64; AEG×167 yday $8.99 → 09:30 $9.16 +28.39; ALVO×387 yday $4.43 → 09:30 $4.79 +139.32; ATAT×44 yday $34.75 → 09:30 $34.70 -2.20; ATHM×67 yday $22.22 → 09:30 $21.78 -29.48; PSEC×1 yday $2.33 → 09:30 $2.34 +0.01 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.01 | ▲ 09:30 equity $12,677.62 vs yday $12,620.03 (+57.59) | 09:30 open · cash $23.01 (unchanged overnight, no fees) · equity $12,677.62 vs prior close $12,620.03 (+57.59) because holdings re-marked: EL×15 yday $102.85 → 09:30 $104.07 +18.30; TOYO×340 yday $4.61 → 09:30 $4.48 -44.20; DVLT×5025 yday $0.31 → 09:30 $0.32 +50.25; AAP×32 yday $43.83 → 09:30 $43.61 -7.04; AEG×167 yday $9.19 → 09:30 $9.29 +16.70; ALVO×387 yday $5.15 → 09:30 $5.22 +27.09; ATAT×44 yday $34.83 → 09:30 $34.75 -3.52; ATHM×67 yday $21.85 → 09:30 $21.85 +0.00; PSEC×1 yday $2.31 → 09:30 $2.32 +0.01 | — |
| 2026-08-25 09:30 ET | **SELL** | `EL` | 15 | $104.07 | $2.06 | $+95.51 | $1,582.00 | ▲ +95.51 after sell → book $12,675.56; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `TOYO` | 340 | $4.48 | $4.45 | $+8.16 | $3,100.75 | ▲ +8.16 after sell → book $12,671.11; vs 09:30 mark -4.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DVLT` | 5025 | $0.32 | $32.00 | $+38.35 | $4,676.74 | ▲ +38.35 after sell → book $12,639.10; vs 09:30 mark -32.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AAP` | 32 | $43.61 | $2.11 | $-107.87 | $6,070.16 | ▼ -107.87 after sell → book $12,637.00; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AEG` | 167 | $9.29 | $2.53 | $+41.74 | $7,619.06 | ▲ +41.74 after sell → book $12,634.47; vs 09:30 mark -2.53 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALVO` | 387 | $5.22 | $5.07 | $+504.64 | $9,634.12 | ▲ +504.64 after sell → book $12,629.39; vs 09:30 mark -5.08 | dropped from list after 3 sess (min 3) | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `ATAT` | 44 | $34.75 | $2.14 | $+26.53 | $11,160.98 | ▲ +26.53 after sell → book $12,627.25; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATHM` | 67 | $21.85 | $2.21 | $-43.93 | $12,622.72 | ▼ -43.93 after sell → book $12,625.04; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 9 | $172.40 | $2.02 | — | $11,069.10 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-6.1; leftover $1577.84 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 18 | $86.86 | $2.04 | — | $9,503.57 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.3; leftover $1577.84 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 102 | $15.34 | $2.30 | — | $7,936.60 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=+2.8; leftover $1577.84 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 8 | $179.33 | $2.01 | — | $6,499.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-9.3; leftover $1577.84 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 283 | $5.57 | $3.65 | — | $4,919.98 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-7.1; leftover $1577.84 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 33 | $47.68 | $2.09 | — | $3,344.45 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+18.8; leftover $1577.84 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 110 | $14.26 | $2.32 | — | $1,773.53 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-1.9; leftover $1577.84 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 334 | $4.71 | $4.31 | — | $196.09 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-9.9; leftover $1577.84 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $196.09 | ▲ 09:30 equity $12,538.36 vs yday $12,538.36 (-0.00) | 09:30 open · cash $196.09 (unchanged overnight, no fees) · equity $12,538.36 vs prior close $12,538.36 (-0.00) because holdings re-marked: PSEC×1 yday $2.33 → 09:30 $2.33 +0.00; BMO×9 yday $175.00 → 09:30 $175.00 +0.00; BNS×18 yday $90.08 → 09:30 $90.08 +0.00; BZ×102 yday $16.32 → 09:30 $16.32 +0.00; DKS×8 yday $156.70 → 09:30 $156.70 +0.00; EH×283 yday $5.28 → 09:30 $5.28 +0.00; GFI×33 yday $48.36 → 09:30 $48.36 +0.00; GRRR×110 yday $14.20 → 09:30 $14.20 +0.00; SHMD×334 yday $4.71 → 09:30 $4.71 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $196.09 | ▼ 09:30 equity $11,724.49 vs yday $12,604.30 (-879.81) | 09:30 open · cash $196.09 (unchanged overnight, no fees) · equity $11,724.49 vs prior close $12,604.30 (-879.81) because holdings re-marked: PSEC×1 yday $2.33 → 09:30 $2.35 +0.02; BMO×9 yday $175.00 → 09:30 $173.22 -16.02; BNS×18 yday $90.08 → 09:30 $92.64 +46.08; BZ×102 yday $16.32 → 09:30 $16.77 +45.90; DKS×8 yday $156.70 → 09:30 $121.87 -278.64; EH×283 yday $5.28 → 09:30 $4.77 -144.33; GFI×33 yday $48.36 → 09:30 $48.24 -3.96; GRRR×110 yday $14.20 → 09:30 $14.03 -18.70; SHMD×334 yday $4.71 → 09:30 $3.38 -444.22 | — |
| 2026-08-27 09:30 ET | **SELL** | `PSEC` | 1 | $2.35 | $0.05 | $-0.02 | $198.39 | ▼ -0.02 after sell → book $11,724.44; vs 09:30 mark -0.05 | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $198.39 | ▲ 09:30 equity $12,131.47 vs yday $12,109.79 (+21.68) | 09:30 open · cash $198.39 (unchanged overnight, no fees) · equity $12,131.47 vs prior close $12,109.79 (+21.68) because holdings re-marked: BMO×9 yday $172.90 → 09:30 $172.85 -0.45; BNS×18 yday $93.59 → 09:30 $93.52 -1.26; BZ×102 yday $18.84 → 09:30 $18.50 -34.68; DKS×8 yday $129.66 → 09:30 $128.73 -7.44; EH×283 yday $4.86 → 09:30 $4.90 +11.32; GFI×33 yday $47.82 → 09:30 $47.93 +3.63; GRRR×110 yday $15.45 → 09:30 $15.94 +53.90; SHMD×334 yday $3.17 → 09:30 $3.16 -3.34 | — |
| 2026-08-28 09:30 ET | **SELL** | `BMO` | 9 | $172.85 | $2.04 | $-0.01 | $1,752.00 | ▼ -0.01 after sell → book $12,129.43; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BNS` | 18 | $93.52 | $2.07 | $+115.77 | $3,433.29 | ▲ +115.77 after sell → book $12,127.36; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 102 | $18.50 | $2.33 | $+317.70 | $5,317.96 | ▲ +317.70 after sell → book $12,125.03; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 8 | $128.73 | $2.03 | $-408.85 | $6,345.77 | ▼ -408.85 after sell → book $12,123.00; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EH` | 283 | $4.90 | $3.71 | $-196.97 | $7,728.76 | ▼ -196.97 after sell → book $12,119.29; vs 09:30 mark -3.71 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GFI` | 33 | $47.93 | $2.11 | $+4.05 | $9,308.34 | ▲ +4.05 after sell → book $12,117.18; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRRR` | 110 | $15.94 | $2.35 | $+180.13 | $11,059.39 | ▲ +180.13 after sell → book $12,114.83; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `SHMD` | 334 | $3.16 | $4.37 | $-526.38 | $12,110.45 | ▼ -526.38 after sell → book $12,110.45; vs 09:30 mark -4.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 5 | $261.47 | $2.00 | — | $10,801.10 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; leftover $1513.81 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 101 | $14.96 | $2.29 | — | $9,287.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.4; leftover $1513.81 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 18 | $82.64 | $2.04 | — | $7,798.28 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.9; leftover $1513.81 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 355 | $4.26 | $4.58 | — | $6,281.40 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.7; leftover $1513.81 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 35 | $42.51 | $2.10 | — | $4,791.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+6.0; leftover $1513.81 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 72 | $20.75 | $2.21 | — | $3,295.25 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.9; leftover $1513.81 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 191 | $7.91 | $2.56 | — | $1,781.88 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.4; leftover $1513.81 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 37 | $40.65 | $2.10 | — | $275.73 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; leftover $1513.81 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $275.73 | ▲ 09:30 equity $12,164.23 vs yday $12,148.78 (+15.45) | 09:30 open · cash $275.73 (unchanged overnight, no fees) · equity $12,164.23 vs prior close $12,148.78 (+15.45) because holdings re-marked: ADSK×5 yday $270.58 → 09:30 $258.50 -60.40; BBAR×101 yday $14.60 → 09:30 $14.50 -10.10; ESTC×18 yday $83.74 → 09:30 $99.99 +292.50; FINV×355 yday $4.02 → 09:30 $3.46 -198.80; FRO×35 yday $43.75 → 09:30 $43.54 -7.35; GAP×72 yday $20.79 → 09:30 $22.89 +151.20; HAFN×191 yday $8.29 → 09:30 $8.43 +26.74; IREN×37 yday $40.53 → 09:30 $35.71 -178.34 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $275.73 | ▲ 09:30 equity $12,241.88 vs yday $12,203.68 (+38.20) | 09:30 open · cash $275.73 (unchanged overnight, no fees) · equity $12,241.88 vs prior close $12,203.68 (+38.20) because holdings re-marked: ADSK×5 yday $259.14 → 09:30 $258.17 -4.85; BBAR×101 yday $14.50 → 09:30 $15.14 +64.64; ESTC×18 yday $99.00 → 09:30 $96.54 -44.28; FINV×355 yday $3.46 → 09:30 $3.67 +74.55; FRO×35 yday $44.09 → 09:30 $43.60 -17.15; GAP×72 yday $23.30 → 09:30 $22.28 -73.44; HAFN×191 yday $8.45 → 09:30 $8.43 -3.82; IREN×37 yday $35.75 → 09:30 $36.90 +42.55 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $275.73 | ▼ 09:30 equity $12,145.70 vs yday $12,210.86 (-65.16) | 09:30 open · cash $275.73 (unchanged overnight, no fees) · equity $12,145.70 vs prior close $12,210.86 (-65.16) because holdings re-marked: ADSK×5 yday $259.89 → 09:30 $253.48 -32.05; BBAR×101 yday $15.14 → 09:30 $14.82 -32.32; ESTC×18 yday $96.07 → 09:30 $95.76 -5.58; FINV×355 yday $3.67 → 09:30 $3.58 -31.95; FRO×35 yday $43.66 → 09:30 $44.39 +25.55; GAP×72 yday $22.20 → 09:30 $22.05 -10.80; HAFN×191 yday $8.41 → 09:30 $8.56 +28.65; IREN×37 yday $36.26 → 09:30 $36.08 -6.66 | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 5 | $253.48 | $2.03 | $-43.98 | $1,541.10 | ▼ -43.98 after sell → book $12,143.67; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBAR` | 101 | $14.82 | $2.32 | $-18.75 | $3,035.60 | ▼ -18.75 after sell → book $12,141.35; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 18 | $95.76 | $2.07 | $+232.05 | $4,757.21 | ▲ +232.05 after sell → book $12,139.28; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FINV` | 355 | $3.58 | $4.65 | $-250.63 | $6,023.46 | ▼ -250.63 after sell → book $12,134.63; vs 09:30 mark -4.65 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FRO` | 35 | $44.39 | $2.12 | $+61.59 | $7,575.00 | ▲ +61.59 after sell → book $12,132.52; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 72 | $22.05 | $2.23 | $+89.16 | $9,160.37 | ▲ +89.16 after sell → book $12,130.29; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 191 | $8.56 | $2.61 | $+118.98 | $10,792.72 | ▲ +118.98 after sell → book $12,127.68; vs 09:30 mark -2.61 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `IREN` | 37 | $36.08 | $2.12 | $-173.31 | $12,125.56 | ▼ -173.31 after sell → book $12,125.56; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,125.56 | ▲ 09:30 equity $12,125.56 vs yday $12,125.56 (-0.00) | 09:30 open · cash $12,125.56 · no holdings · equity $12,125.56 vs prior close $12,125.56 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 147 | $10.30 | $2.43 | — | $10,609.03 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.7; leftover $1515.69 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 4 | $369.68 | $2.00 | — | $9,128.30 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; leftover $1515.69 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 285 | $5.30 | $3.68 | — | $7,614.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.1; leftover $1515.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 4 | $357.25 | $2.00 | — | $6,183.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-7.7; leftover $1515.69 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 63 | $23.80 | $2.18 | — | $4,681.55 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.5; leftover $1515.69 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 6 | $244.98 | $2.01 | — | $3,209.66 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+2.3; leftover $1515.69 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 29 | $51.99 | $2.08 | — | $1,699.87 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-9.0; leftover $1515.69 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 83 | $18.22 | $2.24 | — | $185.37 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-16.7; leftover $1515.69 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $185.37 | ▲ 09:30 equity $12,136.54 vs yday $12,058.55 (+77.99) | 09:30 open · cash $185.37 (unchanged overnight, no fees) · equity $12,136.54 vs prior close $12,058.55 (+77.99) because holdings re-marked: AI×147 yday $10.52 → 09:30 $10.74 +32.34; AVGO×4 yday $367.24 → 09:30 $351.74 -62.00; CHPT×285 yday $5.19 → 09:30 $6.90 +487.35; CIEN×4 yday $354.16 → 09:30 $354.49 +1.32; CPB×63 yday $23.78 → 09:30 $22.32 -91.98; FIVE×6 yday $243.08 → 09:30 $256.99 +83.46; HPE×29 yday $51.83 → 09:30 $47.60 -122.67; MEI×83 yday $18.10 → 09:30 $15.09 -249.83 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 2 | $10.16 | $0.21 | — | $164.84 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+4.8; leftover $23.17 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 6 | $3.78 | $0.24 | — | $141.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-2.8; leftover $23.17 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 1 | $15.62 | $0.16 | — | $126.14 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.7; leftover $23.17 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `ARX` | cash | leftover split 2.63 < 1 share @ 19.57 |
| 2026-08-14 | `AIRO` | cash | leftover split 2.63 < 1 share @ 11.12 |
| 2026-08-14 | `MH` | cash | leftover split 2.63 < 1 share @ 13.55 |
| 2026-08-14 | `CLBT` | cash | leftover split 2.63 < 1 share @ 10.83 |
| 2026-08-14 | `LUNR` | cash | leftover split 2.63 < 1 share @ 19.17 |
| 2026-08-14 | `NMAX` | cash | leftover split 2.63 < 1 share @ 9.89 |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `EU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `EU` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-08-21 | `EL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TOYO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AEG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ATAT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ATHM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `FUTU` | cash | leftover split 3.62 < 1 share @ 115.18 |
| 2026-08-21 | `DE` | cash | leftover split 3.62 < 1 share @ 623.26 |
| 2026-08-21 | `WMT` | cash | leftover split 3.62 < 1 share @ 103.69 |
| 2026-08-21 | `BEKE` | cash | leftover split 3.62 < 1 share @ 17.93 |
| 2026-08-21 | `BJ` | cash | leftover split 3.62 < 1 share @ 93.98 |
| 2026-08-21 | `BKE` | cash | leftover split 3.62 < 1 share @ 43.08 |
| 2026-08-24 | `EL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TOYO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AEG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ATAT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ATHM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `PSEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `PSEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `PSEC` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BNS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `GFI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SHMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SLQT` | no_price | no 09:30 open |
| 2026-08-26 | `TIGR` | no_price | no 09:30 open |
| 2026-08-26 | `ANF` | no_price | no 09:30 open |
| 2026-08-26 | `BBWI` | no_price | no 09:30 open |
| 2026-08-26 | `BOX` | no_price | no 09:30 open |
| 2026-08-26 | `DY` | no_price | no 09:30 open |
| 2026-08-27 | `BMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BNS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `GFI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SHMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NVDA` | cash | leftover split 198.39 < 1 share @ 212.64 |
| 2026-08-31 | `ADSK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BBAR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ESTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FINV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `HAFN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ADSK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BBAR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ESTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FINV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `HAFN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `SBSW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `AI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CHPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CPB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HPE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MEI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `AMBA` | cash | leftover split 23.17 < 1 share @ 66.61 |
| 2026-09-04 | `DOCU` | cash | leftover split 23.17 < 1 share @ 67.06 |
| 2026-09-04 | `GWRE` | cash | leftover split 23.17 < 1 share @ 198.00 |
| 2026-09-04 | `IOT` | cash | leftover split 23.17 < 1 share @ 37.69 |
| 2026-09-04 | `LULU` | cash | leftover split 23.17 < 1 share @ 121.15 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `AI` | 147 | 2026-09-03 @ $10.30 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.7; leftover $1515.69 |
| `AVGO` | 4 | 2026-09-03 @ $369.68 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; leftover $1515.69 |
| `CHPT` | 285 | 2026-09-03 @ $5.30 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.1; leftover $1515.69 |
| `CIEN` | 4 | 2026-09-03 @ $357.25 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-7.7; leftover $1515.69 |
| `CPB` | 63 | 2026-09-03 @ $23.80 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.5; leftover $1515.69 |
| `FIVE` | 6 | 2026-09-03 @ $244.98 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+2.3; leftover $1515.69 |
| `HPE` | 29 | 2026-09-03 @ $51.99 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-9.0; leftover $1515.69 |
| `MEI` | 83 | 2026-09-03 @ $18.22 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-16.7; leftover $1515.69 |
| `ASAN` | 2 | 2026-09-04 @ $10.16 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+4.8; leftover $23.17 |
| `DOMO` | 6 | 2026-09-04 @ $3.78 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-2.8; leftover $23.17 |
| `MAMA` | 1 | 2026-09-04 @ $15.62 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.7; leftover $23.17 |
