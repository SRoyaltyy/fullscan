# Factor mine action — `short_last_red_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · last bar red

Cash book **-7.96%** ($9,204) · signal-only (no cash/fees) was -22.66%. Starts YES **4/17**. Fills 126 · skips 174 · realized $-786.62.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_red=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $17,916.05.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | TGTX, SLS, HIMS, VOR | — | $14,955.47 | $9,934.23 | TGTX×25, SLS×106, HIMS×42, VOR×56 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $14,955.47 | TGTX×25, SLS×106, HIMS×42, VOR×56 | $9,928.54 | -5.69 | TLN, NRG, MARA, FOSL, ARX, CRMD, BIRK, HLIT | — | $19,573.33 | $9,863.88 | TGTX×25, SLS×106, HIMS×42, VOR×56, TLN×1, NRG×5, MARA×68, FOSL×110, ARX×31, CRMD×77, BIRK×15, HLIT×47 | 09:30 open · cash $14,955.47 (unchanged overnight, no fees) · equity $9,928.54 vs prior close $9,934.23 (-5.69) because holdings re-marked: TGTX×25 yday $47.94 → 09:30 $47.27 +16.75; SLS×106 yday $12.36 → 09:30 $12.40 -4.24; HIMS×42 yday $28.77 → 09:30 $29.15 -15.96; VOR×56 yday $23.29 → 09:30 $23.33 -2.24 |
| 2026-08-17 | +2.25 | $19,573.33 | TGTX×25, SLS×106, HIMS×42, VOR×56, TLN×1, NRG×5, MARA×68, FOSL×110, ARX×31, CRMD×77, BIRK×15, HLIT×47 | $9,867.77 | +3.89 | TMC, TGB, ELF, DNN, HNST, CAPR, BYND, NU | — | $24,397.40 | $9,855.62 | TGTX×25, SLS×106, HIMS×42, VOR×56, TLN×1, NRG×5, MARA×68, FOSL×110, ARX×31, CRMD×77, BIRK×15, HLIT×47, TMC×152, TGB×72, ELF×6, DNN×190, HNST×128, CAPR×89, BYND×48, NU×40 | 09:30 open · cash $19,573.33 (unchanged overnight, no fees) · equity $9,867.77 vs prior close $9,863.88 (+3.89) because holdings re-marked: TGTX×25 yday $48.74 → 09:30 $48.74 +0.00; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; HIMS×42 yday $28.15 → 09:30 $28.14 +0.42; VOR×56 yday $23.03 → 09:30 $22.91 +6.72; TLN×1 yday $362.74 → 09:30 $367.88 -5.14; NRG×5 yday $126.24 → 09:30 $127.40 -5.80; MARA×68 yday $9.20 → 09:30 $9.22 -1.36; FOSL×110 yday $5.57 → 09:30 $5.50 +7.70; ARX×31 yday $19.58 → 09:30 $19.57 +0.31; CRMD×77 yday $7.54 → 09:30 $7.55 -0.77; BIRK×15 yday $39.35 → 09:30 $39.48 -1.95; HLIT×47 yday $13.92 → 09:30 $13.84 +3.76 |
| 2026-08-18 | -6.20 | $24,397.40 | TGTX×25, SLS×106, HIMS×42, VOR×56, TLN×1, NRG×5, MARA×68, FOSL×110, ARX×31, CRMD×77, BIRK×15, HLIT×47, TMC×152, TGB×72, ELF×6, DNN×190, HNST×128, CAPR×89, BYND×48, NU×40 | $10,051.15 | +195.53 | — | TGTX, SLS, HIMS, VOR | $19,367.17 | $10,090.26 | TLN×1, NRG×5, MARA×68, FOSL×110, ARX×31, CRMD×77, BIRK×15, HLIT×47, TMC×152, TGB×72, ELF×6, DNN×190, HNST×128, CAPR×89, BYND×48, NU×40 | 09:30 open · cash $24,397.40 (unchanged overnight, no fees) · equity $10,051.15 vs prior close $9,855.62 (+195.53) because holdings re-marked: TGTX×25 yday $49.28 → 09:30 $49.28 +0.00; SLS×106 yday $13.00 → 09:30 $12.66 +36.04; HIMS×42 yday $28.61 → 09:30 $27.85 +31.92; VOR×56 yday $23.01 → 09:30 $22.82 +10.64; TLN×1 yday $356.92 → 09:30 $350.89 +6.03; NRG×5 yday $122.37 → 09:30 $121.92 +2.25; MARA×68 yday $9.72 → 09:30 $9.36 +24.48; FOSL×110 yday $5.74 → 09:30 $5.78 -4.40; ARX×31 yday $19.54 → 09:30 $19.57 -0.93; CRMD×77 yday $7.67 → 09:30 $7.71 -3.08; BIRK×15 yday $37.86 → 09:30 $38.07 -3.15; HLIT×47 yday $13.43 → 09:30 $12.93 +23.50; TMC×152 yday $3.77 → 09:30 $3.72 +7.60; TGB×72 yday $8.77 → 09:30 $8.55 +15.84; ELF×6 yday $93.66 → 09:30 $93.44 +1.32; DNN×190 yday $3.19 → 09:30 $3.11 +15.20; HNST×128 yday $4.70 → 09:30 $4.67 +3.84; CAPR×89 yday $7.45 → 09:30 $7.50 -4.45; BYND×48 yday $11.63 → 09:30 $11.12 +24.48; NU×40 yday $14.74 → 09:30 $14.53 +8.40 |
| 2026-08-19 | -7.20 | $19,367.17 | TLN×1, NRG×5, MARA×68, FOSL×110, ARX×31, CRMD×77, BIRK×15, HLIT×47, TMC×152, TGB×72, ELF×6, DNN×190, HNST×128, CAPR×89, BYND×48, NU×40 | $9,991.40 | -98.86 | — | TLN, NRG, MARA, FOSL, ARX, CRMD, BIRK, HLIT | $14,818.03 | $9,782.03 | TMC×152, TGB×72, ELF×6, DNN×190, HNST×128, CAPR×89, BYND×48, NU×40 | 09:30 open · cash $19,367.17 (unchanged overnight, no fees) · equity $9,991.40 vs prior close $10,090.26 (-98.86) because holdings re-marked: TLN×1 yday $317.66 → 09:30 $321.00 -3.34; NRG×5 yday $115.56 → 09:30 $116.20 -3.20; MARA×68 yday $8.96 → 09:30 $8.91 +3.40; FOSL×110 yday $5.50 → 09:30 $5.54 -4.40; ARX×31 yday $19.56 → 09:30 $19.58 -0.62; CRMD×77 yday $8.17 → 09:30 $8.30 -10.01; BIRK×15 yday $37.23 → 09:30 $37.50 -4.05; HLIT×47 yday $12.73 → 09:30 $12.90 -7.99; TMC×152 yday $3.92 → 09:30 $3.93 -1.52; TGB×72 yday $8.36 → 09:30 $8.70 -24.48; ELF×6 yday $92.51 → 09:30 $96.00 -20.94; DNN×190 yday $3.15 → 09:30 $3.19 -7.60; HNST×128 yday $4.75 → 09:30 $4.80 -6.40; CAPR×89 yday $7.08 → 09:30 $7.19 -9.79; BYND×48 yday $12.74 → 09:30 $12.63 +5.28; NU×40 yday $14.35 → 09:30 $14.43 -3.20 |
| 2026-08-20 | +1.12 | $14,818.03 | TMC×152, TGB×72, ELF×6, DNN×190, HNST×128, CAPR×89, BYND×48, NU×40 | $9,862.31 | +80.28 | BHP, MRVI, WYFI, TOYO, DVLT, SAFX, AAP, AEG | TMC, TGB, ELF, DNN, HNST, CAPR, BYND, NU | $14,627.80 | $9,749.01 | BHP×6, MRVI×83, WYFI×28, TOYO×138, DVLT×2050, SAFX×1738, AAP×13, AEG×68 | 09:30 open · cash $14,818.03 (unchanged overnight, no fees) · equity $9,862.31 vs prior close $9,782.03 (+80.28) because holdings re-marked: TMC×152 yday $3.97 → 09:30 $3.92 +7.60; TGB×72 yday $8.47 → 09:30 $8.35 +8.64; ELF×6 yday $99.65 → 09:30 $98.15 +9.00; DNN×190 yday $3.22 → 09:30 $3.20 +3.80; HNST×128 yday $5.02 → 09:30 $4.98 +5.12; CAPR×89 yday $7.98 → 09:30 $7.66 +28.48; BYND×48 yday $14.08 → 09:30 $13.60 +23.04; NU×40 yday $14.61 → 09:30 $14.74 -5.40 |
| 2026-08-21 | +3.25 | $14,627.80 | BHP×6, MRVI×83, WYFI×28, TOYO×138, DVLT×2050, SAFX×1738, AAP×13, AEG×68 | $9,714.07 | -34.94 | AUTL, CRDL, CRSP, FUTU, GMAB, ENHA, CAN | — | $19,379.50 | $9,501.91 | BHP×6, MRVI×83, WYFI×28, TOYO×138, DVLT×2050, SAFX×1738, AAP×13, AEG×68, AUTL×280, CRDL×359, CRSP×11, FUTU×6, GMAB×20, ENHA×405, CAN×2360 | 09:30 open · cash $14,627.80 (unchanged overnight, no fees) · equity $9,714.07 vs prior close $9,749.01 (-34.94) because holdings re-marked: BHP×6 yday $93.63 → 09:30 $95.72 -12.54; MRVI×83 yday $8.26 → 09:30 $8.20 +4.98; WYFI×28 yday $21.16 → 09:30 $21.54 -10.64; TOYO×138 yday $4.51 → 09:30 $4.68 -22.77; DVLT×2050 yday $0.32 → 09:30 $0.31 +20.50; SAFX×1738 yday $0.34 → 09:30 $0.35 -12.17; AAP×13 yday $42.39 → 09:30 $42.41 -0.26; AEG×68 yday $9.01 → 09:30 $9.04 -2.04 |
| 2026-08-24 | -5.17 | $19,379.50 | BHP×6, MRVI×83, WYFI×28, TOYO×138, DVLT×2050, SAFX×1738, AAP×13, AEG×68, AUTL×280, CRDL×359, CRSP×11, FUTU×6, GMAB×20, ENHA×405, CAN×2360 | $9,500.97 | -0.94 | — | — | $19,379.50 | $9,600.75 | BHP×6, MRVI×83, WYFI×28, TOYO×138, DVLT×2050, SAFX×1738, AAP×13, AEG×68, AUTL×280, CRDL×359, CRSP×11, FUTU×6, GMAB×20, ENHA×405, CAN×2360 | 09:30 open · cash $19,379.50 (unchanged overnight, no fees) · equity $9,500.97 vs prior close $9,501.91 (-0.94) because holdings re-marked: BHP×6 yday $97.03 → 09:30 $97.34 -1.86; MRVI×83 yday $8.70 → 09:30 $8.59 +9.13; WYFI×28 yday $20.72 → 09:30 $20.02 +19.60; TOYO×138 yday $4.82 → 09:30 $4.58 +33.12; DVLT×2050 yday $0.32 → 09:30 $0.31 +20.50; SAFX×1738 yday $0.33 → 09:30 $0.35 -43.45; AAP×13 yday $42.58 → 09:30 $43.10 -6.76; AEG×68 yday $8.99 → 09:30 $9.16 -11.56; AUTL×280 yday $2.41 → 09:30 $2.36 +14.00; CRDL×359 yday $1.86 → 09:30 $1.87 -3.59; CRSP×11 yday $59.50 → 09:30 $58.79 +7.81; FUTU×6 yday $123.64 → 09:30 $120.87 +16.62; GMAB×20 yday $33.45 → 09:30 $32.82 +12.60; ENHA×405 yday $1.72 → 09:30 $1.74 -8.10; CAN×2360 yday $0.35 → 09:30 $0.38 -59.00 |
| 2026-08-25 | +1.80 | $19,379.50 | BHP×6, MRVI×83, WYFI×28, TOYO×138, DVLT×2050, SAFX×1738, AAP×13, AEG×68, AUTL×280, CRDL×359, CRSP×11, FUTU×6, GMAB×20, ENHA×405, CAN×2360 | $9,509.01 | -91.74 | OCUL, CRMD, PUSA, CAPR, SUJA, FWDI, JANX | BHP, MRVI, WYFI, TOYO, DVLT, AAP, AEG | $19,729.33 | $9,436.68 | SAFX×1738, AUTL×280, CRDL×359, CRSP×11, FUTU×6, GMAB×20, ENHA×405, CAN×2360, OCUL×62, CRMD×81, PUSA×183, CAPR×99, SUJA×77, FWDI×113, JANX×36 | 09:30 open · cash $19,379.50 (unchanged overnight, no fees) · equity $9,509.01 vs prior close $9,600.75 (-91.74) because holdings re-marked: BHP×6 yday $96.66 → 09:30 $95.95 +4.26; MRVI×83 yday $8.26 → 09:30 $8.31 -4.15; WYFI×28 yday $20.79 → 09:30 $20.98 -5.32; TOYO×138 yday $4.61 → 09:30 $4.48 +17.94; DVLT×2050 yday $0.31 → 09:30 $0.32 -20.50; SAFX×1738 yday $0.35 → 09:30 $0.37 -34.76; AAP×13 yday $43.83 → 09:30 $43.61 +2.86; AEG×68 yday $9.19 → 09:30 $9.29 -6.80; AUTL×280 yday $2.38 → 09:30 $2.32 +16.80; CRDL×359 yday $1.80 → 09:30 $1.90 -35.90; CRSP×11 yday $56.91 → 09:30 $57.00 -0.99; FUTU×6 yday $116.49 → 09:30 $118.02 -9.18; GMAB×20 yday $33.06 → 09:30 $33.49 -8.60; ENHA×405 yday $1.69 → 09:30 $1.65 +16.20; CAN×2360 yday $0.37 → 09:30 $0.38 -23.60 |
| 2026-08-26 | +2.02 | $19,729.33 | SAFX×1738, AUTL×280, CRDL×359, CRSP×11, FUTU×6, GMAB×20, ENHA×405, CAN×2360, OCUL×62, CRMD×81, PUSA×183, CAPR×99, SUJA×77, FWDI×113, JANX×36 | $9,436.68 | +0.00 | — | — | $19,729.33 | $9,467.15 | SAFX×1738, AUTL×280, CRDL×359, CRSP×11, FUTU×6, GMAB×20, ENHA×405, CAN×2360, OCUL×62, CRMD×81, PUSA×183, CAPR×99, SUJA×77, FWDI×113, JANX×36 | 09:30 open · cash $19,729.33 (unchanged overnight, no fees) · equity $9,436.68 vs prior close $9,436.68 (+0.00) because holdings re-marked: SAFX×1738 yday $0.37 → 09:30 $0.37 +0.00; AUTL×280 yday $2.34 → 09:30 $2.34 +0.00; CRDL×359 yday $1.90 → 09:30 $1.90 +0.00; CRSP×11 yday $57.03 → 09:30 $57.03 +0.00; FUTU×6 yday $118.50 → 09:30 $118.50 +0.00; GMAB×20 yday $33.68 → 09:30 $33.68 +0.00; ENHA×405 yday $1.66 → 09:30 $1.66 +0.00; CAN×2360 yday $0.36 → 09:30 $0.36 +0.00; OCUL×62 yday $10.92 → 09:30 $10.92 +0.00; CRMD×81 yday $8.28 → 09:30 $8.28 +0.00; PUSA×183 yday $3.91 → 09:30 $3.91 +0.00; CAPR×99 yday $7.19 → 09:30 $7.19 +0.00; SUJA×77 yday $8.54 → 09:30 $8.54 +0.00; FWDI×113 yday $5.86 → 09:30 $5.86 +0.00; JANX×36 yday $18.99 → 09:30 $18.99 +0.00 |
| 2026-08-27 | — | $19,729.33 | SAFX×1738, AUTL×280, CRDL×359, CRSP×11, FUTU×6, GMAB×20, ENHA×405, CAN×2360, OCUL×62, CRMD×81, PUSA×183, CAPR×99, SUJA×77, FWDI×113, JANX×36 | $9,071.82 | -395.33 | ACMR, GGB, MT, TX, LRCX, MRVL, NUE | SAFX, AUTL, CRDL, CRSP, FUTU, GMAB, ENHA, CAN | $17,383.14 | $8,904.82 | OCUL×62, CRMD×81, PUSA×183, CAPR×99, SUJA×77, FWDI×113, JANX×36, ACMR×6, GGB×127, MT×7, TX×10, LRCX×1, MRVL×2, NUE×2 | 09:30 open · cash $19,729.33 (unchanged overnight, no fees) · equity $9,071.82 vs prior close $9,467.15 (-395.33) because holdings re-marked: SAFX×1738 yday $0.37 → 09:30 $0.35 +34.76; AUTL×280 yday $2.34 → 09:30 $2.41 -19.60; CRDL×359 yday $1.90 → 09:30 $2.03 -46.67; CRSP×11 yday $57.03 → 09:30 $60.18 -34.65; FUTU×6 yday $118.50 → 09:30 $124.67 -37.02; GMAB×20 yday $33.68 → 09:30 $33.78 -2.00; ENHA×405 yday $1.66 → 09:30 $1.63 +12.15; CAN×2360 yday $0.36 → 09:30 $0.40 -94.40; OCUL×62 yday $10.92 → 09:30 $10.79 +8.06; CRMD×81 yday $8.28 → 09:30 $8.60 -25.92; PUSA×183 yday $3.91 → 09:30 $3.84 +12.81; CAPR×99 yday $7.19 → 09:30 $8.29 -108.90; SUJA×77 yday $8.54 → 09:30 $9.39 -65.45; FWDI×113 yday $5.86 → 09:30 $5.97 -12.43; JANX×36 yday $18.99 → 09:30 $18.59 +14.40 |
| 2026-08-28 | +0.75 | $17,383.14 | OCUL×62, CRMD×81, PUSA×183, CAPR×99, SUJA×77, FWDI×113, JANX×36, ACMR×6, GGB×127, MT×7, TX×10, LRCX×1, MRVL×2, NUE×2 | $8,815.23 | -89.59 | SEDG, SMTC, PYXS, SAFX, OPTX, TTMI, APMD | OCUL, CRMD, PUSA, SUJA, FWDI, JANX | $17,374.55 | $8,775.69 | CAPR×99, ACMR×6, GGB×127, MT×7, TX×10, LRCX×1, MRVL×2, NUE×2, SEDG×18, SMTC×4, PYXS×189, SAFX×1612, OPTX×73, TTMI×4, APMD×21 | 09:30 open · cash $17,383.14 (unchanged overnight, no fees) · equity $8,815.23 vs prior close $8,904.82 (-89.59) because holdings re-marked: OCUL×62 yday $10.77 → 09:30 $10.63 +8.68; CRMD×81 yday $8.39 → 09:30 $8.49 -8.10; PUSA×183 yday $3.85 → 09:30 $3.86 -1.83; CAPR×99 yday $9.36 → 09:30 $9.19 +16.83; SUJA×77 yday $9.44 → 09:30 $9.41 +2.31; FWDI×113 yday $5.93 → 09:30 $6.39 -51.98; JANX×36 yday $18.89 → 09:30 $19.00 -3.96; ACMR×6 yday $79.11 → 09:30 $81.65 -15.24; GGB×127 yday $4.46 → 09:30 $4.57 -13.97; MT×7 yday $74.53 → 09:30 $74.54 -0.07; TX×10 yday $55.13 → 09:30 $55.25 -1.20; LRCX×1 yday $312.88 → 09:30 $318.88 -6.00; MRVL×2 yday $245.11 → 09:30 $253.44 -16.66; NUE×2 yday $252.80 → 09:30 $252.00 +1.60 |
| 2026-08-31 | -5.85 | $17,374.55 | CAPR×99, ACMR×6, GGB×127, MT×7, TX×10, LRCX×1, MRVL×2, NUE×2, SEDG×18, SMTC×4, PYXS×189, SAFX×1612, OPTX×73, TTMI×4, APMD×21 | $9,059.40 | +283.71 | — | CAPR | $16,437.70 | $9,069.71 | ACMR×6, GGB×127, MT×7, TX×10, LRCX×1, MRVL×2, NUE×2, SEDG×18, SMTC×4, PYXS×189, SAFX×1612, OPTX×73, TTMI×4, APMD×21 | 09:30 open · cash $17,374.55 (unchanged overnight, no fees) · equity $9,059.40 vs prior close $8,775.69 (+283.71) because holdings re-marked: CAPR×99 yday $10.06 → 09:30 $9.44 +61.38; ACMR×6 yday $80.49 → 09:30 $75.10 +32.34; GGB×127 yday $4.70 → 09:30 $4.55 +19.05; MT×7 yday $74.63 → 09:30 $75.07 -3.08; TX×10 yday $55.83 → 09:30 $54.84 +9.90; LRCX×1 yday $318.58 → 09:30 $308.14 +10.44; MRVL×2 yday $241.45 → 09:30 $216.69 +49.52; NUE×2 yday $252.37 → 09:30 $248.99 +6.76; SEDG×18 yday $33.51 → 09:30 $31.50 +36.18; SMTC×4 yday $142.43 → 09:30 $133.04 +37.56; PYXS×189 yday $3.32 → 09:30 $3.23 +17.01; SAFX×1612 yday $0.37 → 09:30 $0.38 -16.12; OPTX×73 yday $8.73 → 09:30 $8.52 +15.33; TTMI×4 yday $124.73 → 09:30 $117.20 +30.12; APMD×21 yday $28.72 → 09:30 $29.80 -22.68 |
| 2026-09-01 | -6.30 | $16,437.70 | ACMR×6, GGB×127, MT×7, TX×10, LRCX×1, MRVL×2, NUE×2, SEDG×18, SMTC×4, PYXS×189, SAFX×1612, OPTX×73, TTMI×4, APMD×21 | $9,221.72 | +152.01 | — | ACMR, GGB, MT, TX, LRCX, MRVL, NUE | $13,124.72 | $9,232.79 | SEDG×18, SMTC×4, PYXS×189, SAFX×1612, OPTX×73, TTMI×4, APMD×21 | 09:30 open · cash $16,437.70 (unchanged overnight, no fees) · equity $9,221.72 vs prior close $9,069.71 (+152.01) because holdings re-marked: ACMR×6 yday $75.02 → 09:30 $71.24 +22.68; GGB×127 yday $4.55 → 09:30 $4.61 -7.62; MT×7 yday $75.06 → 09:30 $74.31 +5.25; TX×10 yday $54.84 → 09:30 $54.82 +0.20; LRCX×1 yday $305.05 → 09:30 $300.97 +4.08; MRVL×2 yday $216.35 → 09:30 $210.57 +11.56; NUE×2 yday $250.00 → 09:30 $247.60 +4.80; SEDG×18 yday $31.27 → 09:30 $32.22 -17.10; SMTC×4 yday $132.54 → 09:30 $131.65 +3.56; PYXS×189 yday $3.23 → 09:30 $3.14 +17.01; SAFX×1612 yday $0.37 → 09:30 $0.37 +0.00; OPTX×73 yday $8.52 → 09:30 $8.19 +24.09; TTMI×4 yday $120.19 → 09:30 $119.79 +1.60; APMD×21 yday $29.80 → 09:30 $25.90 +81.90 |
| 2026-09-02 | -3.83 | $13,124.72 | SEDG×18, SMTC×4, PYXS×189, SAFX×1612, OPTX×73, TTMI×4, APMD×21 | $9,237.09 | +4.30 | — | SEDG, SMTC, PYXS, SAFX, OPTX, TTMI, APMD | $9,213.42 | $9,213.42 | — | 09:30 open · cash $13,124.72 (unchanged overnight, no fees) · equity $9,237.09 vs prior close $9,232.79 (+4.30) because holdings re-marked: SEDG×18 yday $31.80 → 09:30 $31.87 -1.26; SMTC×4 yday $129.50 → 09:30 $127.63 +7.48; PYXS×189 yday $3.14 → 09:30 $3.24 -18.90; SAFX×1612 yday $0.37 → 09:30 $0.37 +0.00; OPTX×73 yday $8.19 → 09:30 $7.94 +18.25; TTMI×4 yday $116.94 → 09:30 $116.68 +1.04; APMD×21 yday $26.00 → 09:30 $26.11 -2.31 |
| 2026-09-03 | -0.90 | $9,213.42 | — | $9,213.42 | -0.00 | CABA, FRVO, CTMX, EIX, CRDL, SION, DUOL, SAFX | — | $13,661.59 | $9,110.80 | CABA×176, FRVO×31, CTMX×154, EIX×10, CRDL×266, SION×86, DUOL×3, SAFX×1476 | 09:30 open · cash $9,213.42 · no holdings · equity $9,213.42 vs prior close $9,213.42 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $13,661.59 | CABA×176, FRVO×31, CTMX×154, EIX×10, CRDL×266, SION×86, DUOL×3, SAFX×1476 | $9,073.68 | -37.12 | ASND, SLBT, MLYS, CCOI, IRD, JLHL | — | $17,916.05 | $9,204.08 | CABA×176, FRVO×31, CTMX×154, EIX×10, CRDL×266, SION×86, DUOL×3, SAFX×1476, ASND×2, SLBT×246, MLYS×25, CCOI×73, IRD×162, JLHL×121 | 09:30 open · cash $13,661.59 (unchanged overnight, no fees) · equity $9,073.68 vs prior close $9,110.80 (-37.12) because holdings re-marked: CABA×176 yday $3.57 → 09:30 $3.63 -10.56; FRVO×31 yday $17.98 → 09:30 $18.27 -8.99; CTMX×154 yday $3.72 → 09:30 $3.73 -1.54; EIX×10 yday $55.19 → 09:30 $55.42 -2.30; CRDL×266 yday $2.17 → 09:30 $2.18 -2.66; SION×86 yday $7.31 → 09:30 $7.31 +0.00; DUOL×3 yday $157.85 → 09:30 $161.54 -11.07; SAFX×1476 yday $0.38 → 09:30 $0.38 +0.00 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **SHORT** | `TGTX` | 25 | $49.70 | $2.12 | — | $11,240.38 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **SHORT** | `SLS` | 106 | $11.70 | $2.37 | — | $12,478.21 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **SHORT** | `HIMS` | 42 | $29.74 | $2.17 | — | $13,725.12 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **SHORT** | `VOR` | 56 | $22.01 | $2.21 | — | $14,955.47 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,955.47 | ▼ 09:30 equity $9,928.54 vs yday $9,934.23 (-5.69) | 09:30 open · cash $14,955.47 (unchanged overnight, no fees) · equity $9,928.54 vs prior close $9,934.23 (-5.69) because holdings re-marked: TGTX×25 yday $47.94 → 09:30 $47.27 +16.75; SLS×106 yday $12.36 → 09:30 $12.40 -4.24; HIMS×42 yday $28.77 → 09:30 $29.15 -15.96; VOR×56 yday $23.29 → 09:30 $23.33 -2.24 | — |
| 2026-08-14 09:30 ET | **SHORT** | `TLN` | 1 | $359.83 | $2.02 | — | $15,313.28 | — | last bar red; gate last_red=True; list flatten; 🔵; ret5=+5.9; leftover $620.53 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `NRG` | 5 | $120.00 | $2.04 | — | $15,911.24 | — | last bar red; gate last_red=True; list flatten; 🔵; ret5=+0.6; leftover $620.53 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `MARA` | 68 | $9.01 | $2.23 | — | $16,521.68 | — | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $620.53 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `FOSL` | 110 | $5.64 | $2.37 | — | $17,139.72 | — | last bar red; gate last_red=True; list probable; 🔵; ret5=-4.1; leftover $620.53 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `ARX` | 31 | $19.57 | $2.12 | — | $17,744.27 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $620.53 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `CRMD` | 77 | $8.05 | $2.26 | — | $18,361.86 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $620.53 | join🟢 sector🔴 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `BIRK` | 15 | $39.75 | $2.07 | — | $18,956.04 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=+10.2; leftover $620.53 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `HLIT` | 47 | $13.18 | $2.17 | — | $19,573.33 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $620.53 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,573.33 | ▲ 09:30 equity $9,867.77 vs yday $9,863.88 (+3.89) | 09:30 open · cash $19,573.33 (unchanged overnight, no fees) · equity $9,867.77 vs prior close $9,863.88 (+3.89) because holdings re-marked: TGTX×25 yday $48.74 → 09:30 $48.74 +0.00; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; HIMS×42 yday $28.15 → 09:30 $28.14 +0.42; VOR×56 yday $23.03 → 09:30 $22.91 +6.72; TLN×1 yday $362.74 → 09:30 $367.88 -5.14; NRG×5 yday $126.24 → 09:30 $127.40 -5.80; MARA×68 yday $9.20 → 09:30 $9.22 -1.36; FOSL×110 yday $5.57 → 09:30 $5.50 +7.70; ARX×31 yday $19.58 → 09:30 $19.57 +0.31; CRMD×77 yday $7.54 → 09:30 $7.55 -0.77; BIRK×15 yday $39.35 → 09:30 $39.48 -1.95; HLIT×47 yday $13.92 → 09:30 $13.84 +3.76 | — |
| 2026-08-17 09:30 ET | **SHORT** | `TMC` | 152 | $4.05 | $2.50 | — | $20,186.43 | — | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $616.74 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `TGB` | 72 | $8.46 | $2.24 | — | $20,793.31 | — | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $616.74 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `ELF` | 6 | $90.54 | $2.04 | — | $21,334.50 | — | last bar red; gate last_red=True; list flatten; ret5=-7.2; leftover $616.74 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `DNN` | 190 | $3.24 | $2.62 | — | $21,947.48 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=+0.3; leftover $616.74 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `HNST` | 128 | $4.81 | $2.42 | — | $22,560.74 | — | last bar red; gate last_red=True; list flatten; ⚪; ret5=-11.4; leftover $616.74 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `CAPR` | 89 | $6.87 | $2.30 | — | $23,169.87 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=+62.6; leftover $616.74 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `BYND` | 48 | $12.83 | $2.17 | — | $23,783.54 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; ⚪; ret5=-34.1; leftover $616.74 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `NU` | 40 | $15.40 | $2.15 | — | $24,397.40 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+10.0; leftover $616.74 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24,397.40 | ▲ 09:30 equity $10,051.15 vs yday $9,855.62 (+195.53) | 09:30 open · cash $24,397.40 (unchanged overnight, no fees) · equity $10,051.15 vs prior close $9,855.62 (+195.53) because holdings re-marked: TGTX×25 yday $49.28 → 09:30 $49.28 +0.00; SLS×106 yday $13.00 → 09:30 $12.66 +36.04; HIMS×42 yday $28.61 → 09:30 $27.85 +31.92; VOR×56 yday $23.01 → 09:30 $22.82 +10.64; TLN×1 yday $356.92 → 09:30 $350.89 +6.03; NRG×5 yday $122.37 → 09:30 $121.92 +2.25; MARA×68 yday $9.72 → 09:30 $9.36 +24.48; FOSL×110 yday $5.74 → 09:30 $5.78 -4.40; ARX×31 yday $19.54 → 09:30 $19.57 -0.93; CRMD×77 yday $7.67 → 09:30 $7.71 -3.08; BIRK×15 yday $37.86 → 09:30 $38.07 -3.15; HLIT×47 yday $13.43 → 09:30 $12.93 +23.50; TMC×152 yday $3.77 → 09:30 $3.72 +7.60; TGB×72 yday $8.77 → 09:30 $8.55 +15.84; ELF×6 yday $93.66 → 09:30 $93.44 +1.32; DNN×190 yday $3.19 → 09:30 $3.11 +15.20; HNST×128 yday $4.70 → 09:30 $4.67 +3.84; CAPR×89 yday $7.45 → 09:30 $7.50 -4.45; BYND×48 yday $11.63 → 09:30 $11.12 +24.48; NU×40 yday $14.74 → 09:30 $14.53 +8.40 | — |
| 2026-08-18 09:30 ET | **COVER** | `TGTX` | 25 | $49.28 | $2.06 | $+6.32 | $23,163.33 | ▲ +6.32 after sell → book $10,049.08; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **COVER** | `SLS` | 106 | $12.66 | $2.31 | $-106.44 | $21,819.06 | ▼ -106.44 after sell → book $10,046.77; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **COVER** | `HIMS` | 42 | $27.85 | $2.12 | $+75.09 | $20,647.25 | ▲ +75.09 after sell → book $10,044.66; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **COVER** | `VOR` | 56 | $22.82 | $2.16 | $-49.73 | $19,367.17 | ▼ -49.73 after sell → book $10,042.50; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,367.17 | ▼ 09:30 equity $9,991.40 vs yday $10,090.26 (-98.86) | 09:30 open · cash $19,367.17 (unchanged overnight, no fees) · equity $9,991.40 vs prior close $10,090.26 (-98.86) because holdings re-marked: TLN×1 yday $317.66 → 09:30 $321.00 -3.34; NRG×5 yday $115.56 → 09:30 $116.20 -3.20; MARA×68 yday $8.96 → 09:30 $8.91 +3.40; FOSL×110 yday $5.50 → 09:30 $5.54 -4.40; ARX×31 yday $19.56 → 09:30 $19.58 -0.62; CRMD×77 yday $8.17 → 09:30 $8.30 -10.01; BIRK×15 yday $37.23 → 09:30 $37.50 -4.05; HLIT×47 yday $12.73 → 09:30 $12.90 -7.99; TMC×152 yday $3.92 → 09:30 $3.93 -1.52; TGB×72 yday $8.36 → 09:30 $8.70 -24.48; ELF×6 yday $92.51 → 09:30 $96.00 -20.94; DNN×190 yday $3.15 → 09:30 $3.19 -7.60; HNST×128 yday $4.75 → 09:30 $4.80 -6.40; CAPR×89 yday $7.08 → 09:30 $7.19 -9.79; BYND×48 yday $12.74 → 09:30 $12.63 +5.28; NU×40 yday $14.35 → 09:30 $14.43 -3.20 | — |
| 2026-08-19 09:30 ET | **COVER** | `TLN` | 1 | $321.00 | $1.99 | $+34.81 | $19,044.18 | ▲ +34.81 after sell → book $9,989.41; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `NRG` | 5 | $116.20 | $2.00 | $+14.95 | $18,461.17 | ▲ +14.95 after sell → book $9,987.40; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `MARA` | 68 | $8.91 | $2.19 | $+2.37 | $17,853.10 | ▲ +2.37 after sell → book $9,985.21; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `FOSL` | 110 | $5.54 | $2.32 | $+6.31 | $17,241.38 | ▲ +6.31 after sell → book $9,982.89; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `ARX` | 31 | $19.58 | $2.08 | $-4.51 | $16,632.31 | ▼ -4.51 after sell → book $9,980.80; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | join🟡 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **COVER** | `CRMD` | 77 | $8.30 | $2.22 | $-23.73 | $15,990.99 | ▼ -23.73 after sell → book $9,978.58; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `BIRK` | 15 | $37.50 | $2.04 | $+29.64 | $15,426.46 | ▲ +29.64 after sell → book $9,976.55; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `HLIT` | 47 | $12.90 | $2.13 | $+8.86 | $14,818.03 | ▲ +8.86 after sell → book $9,974.42; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,818.03 | ▲ 09:30 equity $9,862.31 vs yday $9,782.03 (+80.28) | 09:30 open · cash $14,818.03 (unchanged overnight, no fees) · equity $9,862.31 vs prior close $9,782.03 (+80.28) because holdings re-marked: TMC×152 yday $3.97 → 09:30 $3.92 +7.60; TGB×72 yday $8.47 → 09:30 $8.35 +8.64; ELF×6 yday $99.65 → 09:30 $98.15 +9.00; DNN×190 yday $3.22 → 09:30 $3.20 +3.80; HNST×128 yday $5.02 → 09:30 $4.98 +5.12; CAPR×89 yday $7.98 → 09:30 $7.66 +28.48; BYND×48 yday $14.08 → 09:30 $13.60 +23.04; NU×40 yday $14.61 → 09:30 $14.74 -5.40 | — |
| 2026-08-20 09:30 ET | **COVER** | `TMC` | 152 | $3.92 | $2.45 | $+14.82 | $14,219.74 | ▲ +14.82 after sell → book $9,859.86; vs 09:30 mark -2.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `TGB` | 72 | $8.35 | $2.21 | $+3.47 | $13,616.33 | ▲ +3.47 after sell → book $9,857.65; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ELF` | 6 | $98.15 | $2.01 | $-49.71 | $13,025.43 | ▼ -49.71 after sell → book $9,855.65; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `DNN` | 190 | $3.20 | $2.56 | $+2.42 | $12,414.87 | ▲ +2.42 after sell → book $9,853.09; vs 09:30 mark -2.56 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HNST` | 128 | $4.98 | $2.37 | $-26.56 | $11,775.05 | ▼ -26.56 after sell → book $9,850.71; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `CAPR` | 89 | $7.66 | $2.26 | $-74.87 | $11,091.06 | ▼ -74.87 after sell → book $9,848.46; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `BYND` | 48 | $13.60 | $2.13 | $-41.26 | $10,436.12 | ▼ -41.26 after sell → book $9,846.32; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `NU` | 40 | $14.74 | $2.11 | $+21.94 | $9,844.21 | ▲ +21.94 after sell → book $9,844.21; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SHORT** | `BHP` | 6 | $91.01 | $2.04 | — | $10,388.23 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $615.26 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `MRVI` | 83 | $7.38 | $2.28 | — | $10,998.49 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $615.26 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 28 | $21.40 | $2.11 | — | $11,595.58 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-25.2; leftover $615.26 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 138 | $4.43 | $2.45 | — | $12,204.47 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-23.1; leftover $615.26 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `DVLT` | 2050 | $0.30 | $12.67 | — | $12,806.80 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-3.2; leftover $615.26 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `SAFX` | 1738 | $0.35 | $11.68 | — | $13,410.37 | — | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-29.4; leftover $615.26 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 13 | $46.85 | $2.07 | — | $14,017.35 | — | last bar red; gate last_red=True; list earn_react; 🔵; ret5=+5.0; leftover $615.26 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AEG` | 68 | $9.01 | $2.23 | — | $14,627.80 | — | last bar red; gate last_red=True; list earn_react; 🔵; ⚪; ret5=-1.3; leftover $615.26 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,627.80 | ▼ 09:30 equity $9,714.07 vs yday $9,749.01 (-34.94) | 09:30 open · cash $14,627.80 (unchanged overnight, no fees) · equity $9,714.07 vs prior close $9,749.01 (-34.94) because holdings re-marked: BHP×6 yday $93.63 → 09:30 $95.72 -12.54; MRVI×83 yday $8.26 → 09:30 $8.20 +4.98; WYFI×28 yday $21.16 → 09:30 $21.54 -10.64; TOYO×138 yday $4.51 → 09:30 $4.68 -22.77; DVLT×2050 yday $0.32 → 09:30 $0.31 +20.50; SAFX×1738 yday $0.34 → 09:30 $0.35 -12.17; AAP×13 yday $42.39 → 09:30 $42.41 -0.26; AEG×68 yday $9.01 → 09:30 $9.04 -2.04 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUTL` | 280 | $2.47 | $3.69 | — | $15,315.71 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $693.86 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CRDL` | 359 | $1.93 | $4.72 | — | $16,003.86 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $693.86 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CRSP` | 11 | $59.72 | $2.06 | — | $16,658.72 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $693.86 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `FUTU` | 6 | $115.18 | $2.05 | — | $17,347.76 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $693.86 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `GMAB` | 20 | $33.36 | $2.09 | — | $18,012.87 | — | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $693.86 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ENHA` | 405 | $1.71 | $5.32 | — | $18,700.10 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $693.86 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CAN` | 2360 | $0.29 | $14.44 | — | $19,379.50 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $693.86 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,379.50 | ▼ 09:30 equity $9,500.97 vs yday $9,501.91 (-0.94) | 09:30 open · cash $19,379.50 (unchanged overnight, no fees) · equity $9,500.97 vs prior close $9,501.91 (-0.94) because holdings re-marked: BHP×6 yday $97.03 → 09:30 $97.34 -1.86; MRVI×83 yday $8.70 → 09:30 $8.59 +9.13; WYFI×28 yday $20.72 → 09:30 $20.02 +19.60; TOYO×138 yday $4.82 → 09:30 $4.58 +33.12; DVLT×2050 yday $0.32 → 09:30 $0.31 +20.50; SAFX×1738 yday $0.33 → 09:30 $0.35 -43.45; AAP×13 yday $42.58 → 09:30 $43.10 -6.76; AEG×68 yday $8.99 → 09:30 $9.16 -11.56; AUTL×280 yday $2.41 → 09:30 $2.36 +14.00; CRDL×359 yday $1.86 → 09:30 $1.87 -3.59; CRSP×11 yday $59.50 → 09:30 $58.79 +7.81; FUTU×6 yday $123.64 → 09:30 $120.87 +16.62; GMAB×20 yday $33.45 → 09:30 $32.82 +12.60; ENHA×405 yday $1.72 → 09:30 $1.74 -8.10; CAN×2360 yday $0.35 → 09:30 $0.38 -59.00 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,379.50 | ▼ 09:30 equity $9,509.01 vs yday $9,600.75 (-91.74) | 09:30 open · cash $19,379.50 (unchanged overnight, no fees) · equity $9,509.01 vs prior close $9,600.75 (-91.74) because holdings re-marked: BHP×6 yday $96.66 → 09:30 $95.95 +4.26; MRVI×83 yday $8.26 → 09:30 $8.31 -4.15; WYFI×28 yday $20.79 → 09:30 $20.98 -5.32; TOYO×138 yday $4.61 → 09:30 $4.48 +17.94; DVLT×2050 yday $0.31 → 09:30 $0.32 -20.50; SAFX×1738 yday $0.35 → 09:30 $0.37 -34.76; AAP×13 yday $43.83 → 09:30 $43.61 +2.86; AEG×68 yday $9.19 → 09:30 $9.29 -6.80; AUTL×280 yday $2.38 → 09:30 $2.32 +16.80; CRDL×359 yday $1.80 → 09:30 $1.90 -35.90; CRSP×11 yday $56.91 → 09:30 $57.00 -0.99; FUTU×6 yday $116.49 → 09:30 $118.02 -9.18; GMAB×20 yday $33.06 → 09:30 $33.49 -8.60; ENHA×405 yday $1.69 → 09:30 $1.65 +16.20; CAN×2360 yday $0.37 → 09:30 $0.38 -23.60 | — |
| 2026-08-25 09:30 ET | **COVER** | `BHP` | 6 | $95.95 | $2.01 | $-33.69 | $18,801.79 | ▼ -33.69 after sell → book $9,507.00; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `MRVI` | 83 | $8.31 | $2.24 | $-81.71 | $18,109.82 | ▼ -81.71 after sell → book $9,504.76; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 28 | $20.98 | $2.07 | $+7.58 | $17,520.31 | ▲ +7.58 after sell → book $9,502.69; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 138 | $4.48 | $2.40 | $-11.76 | $16,899.66 | ▼ -11.76 after sell → book $9,500.28; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `DVLT` | 2050 | $0.32 | $12.71 | $-66.38 | $16,230.95 | ▼ -66.38 after sell → book $9,487.57; vs 09:30 mark -12.71 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 13 | $43.61 | $2.03 | $+38.03 | $15,661.99 | ▲ +38.03 after sell → book $9,485.54; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AEG` | 68 | $9.29 | $2.19 | $-23.47 | $15,028.08 | ▼ -23.47 after sell → book $9,483.35; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SHORT** | `OCUL` | 62 | $10.92 | $2.21 | — | $15,702.90 | — | last bar red; gate last_red=True; list flatten; 🔵; ret5=+10.4; leftover $677.38 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **SHORT** | `CRMD` | 81 | $8.28 | $2.27 | — | $16,371.31 | — | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=+8.8; leftover $677.38 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `PUSA` | 183 | $3.70 | $2.60 | — | $17,045.81 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $677.38 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `CAPR` | 99 | $6.79 | $2.33 | — | $17,715.69 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $677.38 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `SUJA` | 77 | $8.79 | $2.26 | — | $18,390.26 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.5; leftover $677.38 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `FWDI` | 113 | $5.99 | $2.38 | — | $19,064.75 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+20.7; leftover $677.38 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `JANX` | 36 | $18.52 | $2.14 | — | $19,729.33 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=+7.9; leftover $677.38 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,729.33 | ▲ 09:30 equity $9,436.68 vs yday $9,436.68 (+0.00) | 09:30 open · cash $19,729.33 (unchanged overnight, no fees) · equity $9,436.68 vs prior close $9,436.68 (+0.00) because holdings re-marked: SAFX×1738 yday $0.37 → 09:30 $0.37 +0.00; AUTL×280 yday $2.34 → 09:30 $2.34 +0.00; CRDL×359 yday $1.90 → 09:30 $1.90 +0.00; CRSP×11 yday $57.03 → 09:30 $57.03 +0.00; FUTU×6 yday $118.50 → 09:30 $118.50 +0.00; GMAB×20 yday $33.68 → 09:30 $33.68 +0.00; ENHA×405 yday $1.66 → 09:30 $1.66 +0.00; CAN×2360 yday $0.36 → 09:30 $0.36 +0.00; OCUL×62 yday $10.92 → 09:30 $10.92 +0.00; CRMD×81 yday $8.28 → 09:30 $8.28 +0.00; PUSA×183 yday $3.91 → 09:30 $3.91 +0.00; CAPR×99 yday $7.19 → 09:30 $7.19 +0.00; SUJA×77 yday $8.54 → 09:30 $8.54 +0.00; FWDI×113 yday $5.86 → 09:30 $5.86 +0.00; JANX×36 yday $18.99 → 09:30 $18.99 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,729.33 | ▼ 09:30 equity $9,071.82 vs yday $9,467.15 (-395.33) | 09:30 open · cash $19,729.33 (unchanged overnight, no fees) · equity $9,071.82 vs prior close $9,467.15 (-395.33) because holdings re-marked: SAFX×1738 yday $0.37 → 09:30 $0.35 +34.76; AUTL×280 yday $2.34 → 09:30 $2.41 -19.60; CRDL×359 yday $1.90 → 09:30 $2.03 -46.67; CRSP×11 yday $57.03 → 09:30 $60.18 -34.65; FUTU×6 yday $118.50 → 09:30 $124.67 -37.02; GMAB×20 yday $33.68 → 09:30 $33.78 -2.00; ENHA×405 yday $1.66 → 09:30 $1.63 +12.15; CAN×2360 yday $0.36 → 09:30 $0.40 -94.40; OCUL×62 yday $10.92 → 09:30 $10.79 +8.06; CRMD×81 yday $8.28 → 09:30 $8.60 -25.92; PUSA×183 yday $3.91 → 09:30 $3.84 +12.81; CAPR×99 yday $7.19 → 09:30 $8.29 -108.90; SUJA×77 yday $8.54 → 09:30 $9.39 -65.45; FWDI×113 yday $5.86 → 09:30 $5.97 -12.43; JANX×36 yday $18.99 → 09:30 $18.59 +14.40 | — |
| 2026-08-27 09:30 ET | **COVER** | `SAFX` | 1738 | $0.35 | $11.30 | $-16.03 | $19,109.74 | ▼ -16.03 after sell → book $9,060.53; vs 09:30 mark -11.29 | dropped from list after 5 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `AUTL` | 280 | $2.41 | $3.61 | $+9.50 | $18,431.33 | ▲ +9.50 after sell → book $9,056.92; vs 09:30 mark -3.61 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `CRDL` | 359 | $2.03 | $4.63 | $-45.25 | $17,697.92 | ▼ -45.25 after sell → book $9,052.28; vs 09:30 mark -4.64 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `CRSP` | 11 | $60.18 | $2.02 | $-9.14 | $17,033.92 | ▼ -9.14 after sell → book $9,050.26; vs 09:30 mark -2.02 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `FUTU` | 6 | $124.67 | $2.01 | $-60.99 | $16,283.89 | ▼ -60.99 after sell → book $9,048.25; vs 09:30 mark -2.01 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `GMAB` | 20 | $33.78 | $2.05 | $-12.54 | $15,606.24 | ▼ -12.54 after sell → book $9,046.20; vs 09:30 mark -2.05 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `ENHA` | 405 | $1.63 | $5.22 | $+21.85 | $14,940.87 | ▲ +21.85 after sell → book $9,040.98; vs 09:30 mark -5.22 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `CAN` | 2360 | $0.40 | $16.52 | $-281.12 | $13,980.35 | ▼ -281.12 after sell → book $9,024.46; vs 09:30 mark -16.52 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SHORT** | `ACMR` | 6 | $80.97 | $2.04 | — | $14,464.13 | — | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=-1.3; leftover $564.03 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `GGB` | 127 | $4.42 | $2.42 | — | $15,023.05 | — | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=-8.6; leftover $564.03 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 7 | $75.12 | $2.05 | — | $15,546.84 | — | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=-2.2; leftover $564.03 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 10 | $55.20 | $2.06 | — | $16,096.79 | — | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=+3.0; leftover $564.03 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `LRCX` | 1 | $314.61 | $2.02 | — | $16,409.38 | — | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=-5.5; leftover $564.03 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `MRVL` | 2 | $240.00 | $2.03 | — | $16,887.35 | — | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=+6.8; leftover $564.03 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `NUE` | 2 | $248.91 | $2.03 | — | $17,383.14 | — | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=-9.4; leftover $564.03 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,383.14 | ▼ 09:30 equity $8,815.23 vs yday $8,904.82 (-89.59) | 09:30 open · cash $17,383.14 (unchanged overnight, no fees) · equity $8,815.23 vs prior close $8,904.82 (-89.59) because holdings re-marked: OCUL×62 yday $10.77 → 09:30 $10.63 +8.68; CRMD×81 yday $8.39 → 09:30 $8.49 -8.10; PUSA×183 yday $3.85 → 09:30 $3.86 -1.83; CAPR×99 yday $9.36 → 09:30 $9.19 +16.83; SUJA×77 yday $9.44 → 09:30 $9.41 +2.31; FWDI×113 yday $5.93 → 09:30 $6.39 -51.98; JANX×36 yday $18.89 → 09:30 $19.00 -3.96; ACMR×6 yday $79.11 → 09:30 $81.65 -15.24; GGB×127 yday $4.46 → 09:30 $4.57 -13.97; MT×7 yday $74.53 → 09:30 $74.54 -0.07; TX×10 yday $55.13 → 09:30 $55.25 -1.20; LRCX×1 yday $312.88 → 09:30 $318.88 -6.00; MRVL×2 yday $245.11 → 09:30 $253.44 -16.66; NUE×2 yday $252.80 → 09:30 $252.00 +1.60 | — |
| 2026-08-28 09:30 ET | **COVER** | `OCUL` | 62 | $10.63 | $2.18 | $+13.59 | $16,721.90 | ▲ +13.59 after sell → book $8,813.05; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `CRMD` | 81 | $8.49 | $2.23 | $-21.52 | $16,031.98 | ▼ -21.52 after sell → book $8,810.82; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `PUSA` | 183 | $3.86 | $2.54 | $-34.42 | $15,323.06 | ▼ -34.42 after sell → book $8,808.28; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `SUJA` | 77 | $9.41 | $2.22 | $-52.22 | $14,596.27 | ▼ -52.22 after sell → book $8,806.06; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `FWDI` | 113 | $6.39 | $2.33 | $-49.91 | $13,871.87 | ▼ -49.91 after sell → book $8,803.73; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `JANX` | 36 | $19.00 | $2.10 | $-21.51 | $13,185.77 | ▼ -21.51 after sell → book $8,801.63; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SHORT** | `SEDG` | 18 | $33.78 | $2.08 | — | $13,791.73 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $628.69 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `SMTC` | 4 | $149.40 | $2.04 | — | $14,387.29 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $628.69 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `PYXS` | 189 | $3.31 | $2.62 | — | $15,010.27 | — | last bar red; gate last_red=True; list yday_gainer; ret5=+2.3; leftover $628.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `SAFX` | 1612 | $0.39 | $11.42 | — | $15,627.53 | — | last bar red; gate last_red=True; list yday_gainer; ret5=-26.5; leftover $628.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `OPTX` | 73 | $8.57 | $2.25 | — | $16,250.89 | — | last bar red; gate last_red=True; list yday_gainer; ret5=-3.4; leftover $628.69 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `TTMI` | 4 | $127.07 | $2.04 | — | $16,757.14 | — | last bar red; gate last_red=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=-21.0; leftover $628.69 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `APMD` | 21 | $29.50 | $2.09 | — | $17,374.55 | — | last bar red; gate last_red=True; list yday_gainer; ret5=-11.7; leftover $628.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,374.55 | ▲ 09:30 equity $9,059.40 vs yday $8,775.69 (+283.71) | 09:30 open · cash $17,374.55 (unchanged overnight, no fees) · equity $9,059.40 vs prior close $8,775.69 (+283.71) because holdings re-marked: CAPR×99 yday $10.06 → 09:30 $9.44 +61.38; ACMR×6 yday $80.49 → 09:30 $75.10 +32.34; GGB×127 yday $4.70 → 09:30 $4.55 +19.05; MT×7 yday $74.63 → 09:30 $75.07 -3.08; TX×10 yday $55.83 → 09:30 $54.84 +9.90; LRCX×1 yday $318.58 → 09:30 $308.14 +10.44; MRVL×2 yday $241.45 → 09:30 $216.69 +49.52; NUE×2 yday $252.37 → 09:30 $248.99 +6.76; SEDG×18 yday $33.51 → 09:30 $31.50 +36.18; SMTC×4 yday $142.43 → 09:30 $133.04 +37.56; PYXS×189 yday $3.32 → 09:30 $3.23 +17.01; SAFX×1612 yday $0.37 → 09:30 $0.38 -16.12; OPTX×73 yday $8.73 → 09:30 $8.52 +15.33; TTMI×4 yday $124.73 → 09:30 $117.20 +30.12; APMD×21 yday $28.72 → 09:30 $29.80 -22.68 | — |
| 2026-08-31 09:30 ET | **COVER** | `CAPR` | 99 | $9.44 | $2.29 | $-266.97 | $16,437.70 | ▼ -266.97 after sell → book $9,057.11; vs 09:30 mark -2.29 | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,437.70 | ▲ 09:30 equity $9,221.72 vs yday $9,069.71 (+152.01) | 09:30 open · cash $16,437.70 (unchanged overnight, no fees) · equity $9,221.72 vs prior close $9,069.71 (+152.01) because holdings re-marked: ACMR×6 yday $75.02 → 09:30 $71.24 +22.68; GGB×127 yday $4.55 → 09:30 $4.61 -7.62; MT×7 yday $75.06 → 09:30 $74.31 +5.25; TX×10 yday $54.84 → 09:30 $54.82 +0.20; LRCX×1 yday $305.05 → 09:30 $300.97 +4.08; MRVL×2 yday $216.35 → 09:30 $210.57 +11.56; NUE×2 yday $250.00 → 09:30 $247.60 +4.80; SEDG×18 yday $31.27 → 09:30 $32.22 -17.10; SMTC×4 yday $132.54 → 09:30 $131.65 +3.56; PYXS×189 yday $3.23 → 09:30 $3.14 +17.01; SAFX×1612 yday $0.37 → 09:30 $0.37 +0.00; OPTX×73 yday $8.52 → 09:30 $8.19 +24.09; TTMI×4 yday $120.19 → 09:30 $119.79 +1.60; APMD×21 yday $29.80 → 09:30 $25.90 +81.90 | — |
| 2026-09-01 09:30 ET | **COVER** | `ACMR` | 6 | $71.24 | $2.01 | $+54.33 | $16,008.25 | ▲ +54.33 after sell → book $9,219.71; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `GGB` | 127 | $4.61 | $2.37 | $-28.92 | $15,420.41 | ▼ -28.92 after sell → book $9,217.34; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 7 | $74.31 | $2.01 | $+1.61 | $14,898.23 | ▲ +1.61 after sell → book $9,215.33; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 10 | $54.82 | $2.02 | $-0.28 | $14,348.01 | ▼ -0.28 after sell → book $9,213.31; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `LRCX` | 1 | $300.97 | $1.99 | $+9.63 | $14,045.05 | ▲ +9.63 after sell → book $9,211.32; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `MRVL` | 2 | $210.57 | $2.00 | $+54.83 | $13,621.91 | ▲ +54.83 after sell → book $9,209.32; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `NUE` | 2 | $247.60 | $2.00 | $-1.41 | $13,124.72 | ▼ -1.41 after sell → book $9,207.33; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,124.72 | ▲ 09:30 equity $9,237.09 vs yday $9,232.79 (+4.30) | 09:30 open · cash $13,124.72 (unchanged overnight, no fees) · equity $9,237.09 vs prior close $9,232.79 (+4.30) because holdings re-marked: SEDG×18 yday $31.80 → 09:30 $31.87 -1.26; SMTC×4 yday $129.50 → 09:30 $127.63 +7.48; PYXS×189 yday $3.14 → 09:30 $3.24 -18.90; SAFX×1612 yday $0.37 → 09:30 $0.37 +0.00; OPTX×73 yday $8.19 → 09:30 $7.94 +18.25; TTMI×4 yday $116.94 → 09:30 $116.68 +1.04; APMD×21 yday $26.00 → 09:30 $26.11 -2.31 | — |
| 2026-09-02 09:30 ET | **COVER** | `SEDG` | 18 | $31.87 | $2.04 | $+30.26 | $12,549.01 | ▲ +30.26 after sell → book $9,235.04; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SMTC` | 4 | $127.63 | $2.00 | $+83.04 | $12,036.49 | ▲ +83.04 after sell → book $9,233.04; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `PYXS` | 189 | $3.24 | $2.56 | $+8.06 | $11,421.57 | ▲ +8.06 after sell → book $9,230.48; vs 09:30 mark -2.56 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SAFX` | 1612 | $0.37 | $10.80 | $+10.02 | $10,814.33 | ▲ +10.02 after sell → book $9,219.68; vs 09:30 mark -10.80 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `OPTX` | 73 | $7.94 | $2.21 | $+41.53 | $10,232.50 | ▲ +41.53 after sell → book $9,217.47; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `TTMI` | 4 | $116.68 | $2.00 | $+37.52 | $9,763.78 | ▲ +37.52 after sell → book $9,215.47; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `APMD` | 21 | $26.11 | $2.05 | $+67.05 | $9,213.42 | ▲ +67.05 after sell → book $9,213.42; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,213.42 | ▲ 09:30 equity $9,213.42 vs yday $9,213.42 (-0.00) | 09:30 open · cash $9,213.42 · no holdings · equity $9,213.42 vs prior close $9,213.42 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **SHORT** | `CABA` | 176 | $3.27 | $2.57 | — | $9,786.36 | — | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $575.84 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `FRVO` | 31 | $18.40 | $2.12 | — | $10,354.65 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $575.84 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `CTMX` | 154 | $3.72 | $2.50 | — | $10,925.02 | — | last bar red; gate last_red=True; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $575.84 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `EIX` | 10 | $56.78 | $2.06 | — | $11,490.77 | — | last bar red; gate last_red=True; list probable,yday_gainer; ret5=+0.3; leftover $575.84 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `CRDL` | 266 | $2.16 | $3.50 | — | $12,061.83 | — | last bar red; gate last_red=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $575.84 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `SION` | 86 | $6.63 | $2.29 | — | $12,629.72 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=-18.1; leftover $575.84 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `DUOL` | 3 | $156.24 | $2.03 | — | $13,096.41 | — | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=+10.0; leftover $575.84 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `SAFX` | 1476 | $0.39 | $10.46 | — | $13,661.59 | — | last bar red; gate last_red=True; list yday_gainer; ret5=-26.5; leftover $575.84 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,661.59 | ▼ 09:30 equity $9,073.68 vs yday $9,110.80 (-37.12) | 09:30 open · cash $13,661.59 (unchanged overnight, no fees) · equity $9,073.68 vs prior close $9,110.80 (-37.12) because holdings re-marked: CABA×176 yday $3.57 → 09:30 $3.63 -10.56; FRVO×31 yday $17.98 → 09:30 $18.27 -8.99; CTMX×154 yday $3.72 → 09:30 $3.73 -1.54; EIX×10 yday $55.19 → 09:30 $55.42 -2.30; CRDL×266 yday $2.17 → 09:30 $2.18 -2.66; SION×86 yday $7.31 → 09:30 $7.31 +0.00; DUOL×3 yday $157.85 → 09:30 $161.54 -11.07; SAFX×1476 yday $0.38 → 09:30 $0.38 +0.00 | — |
| 2026-09-04 09:30 ET | **SHORT** | `ASND` | 2 | $266.94 | $2.03 | — | $14,193.44 | — | last bar red; gate last_red=True; list flatten; ret5=+1.9; leftover $756.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `SLBT` | 246 | $3.07 | $3.24 | — | $14,945.42 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $756.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `MLYS` | 25 | $29.15 | $2.10 | — | $15,672.06 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $756.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `CCOI` | 73 | $10.22 | $2.25 | — | $16,415.87 | — | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $756.14 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `IRD` | 162 | $4.66 | $2.53 | — | $17,168.26 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $756.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `JLHL` | 121 | $6.20 | $2.40 | — | $17,916.05 | — | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=-8.2; leftover $756.14 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NRG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `FOSL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CRMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BIRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HLIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NRG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `FOSL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BIRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HLIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ELF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `HNST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BYND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TBPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ENHA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ENVX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INV` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ELF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `HNST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `BYND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `NU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `INMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WYFI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TOYO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AEG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WYFI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TOYO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AEG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRSP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `FUTU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `GMAB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ENHA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `XHG` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AEM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `FUTU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `GMAB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ENHA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `SAFX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AUTL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRDL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRSP` | no_price | no 09:30 open — carry |
| 2026-08-26 | `FUTU` | no_price | no 09:30 open — carry |
| 2026-08-26 | `GMAB` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ENHA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CAN` | no_price | no 09:30 open — carry |
| 2026-08-26 | `PUSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SUJA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FWDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `JANX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-26 | `AVEX` | no_price | no 09:30 open |
| 2026-08-26 | `BE` | no_price | no 09:30 open |
| 2026-08-26 | `INDP` | no_price | no 09:30 open |
| 2026-08-26 | `AXTI` | no_price | no 09:30 open |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `PUSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SUJA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FWDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `JANX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `MU` | cash | leftover split 564.03 < 1 share @ 925.74 |
| 2026-08-28 | `ACMR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `MT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `TX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `LRCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `MRVL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `NUE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ACMR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `GGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `TX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `LRCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MRVL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `NUE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `PYXS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `OPTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TTMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `APMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `PYXS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `OPTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TTMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `APMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OKE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `IRD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BEP` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `STIM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VLRS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CTMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `EIX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DUOL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CABA` | 176 | 2026-09-03 @ $3.27 | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $575.84 |
| `FRVO` | 31 | 2026-09-03 @ $18.40 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $575.84 |
| `CTMX` | 154 | 2026-09-03 @ $3.72 | last bar red; gate last_red=True; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $575.84 |
| `EIX` | 10 | 2026-09-03 @ $56.78 | last bar red; gate last_red=True; list probable,yday_gainer; ret5=+0.3; leftover $575.84 |
| `CRDL` | 266 | 2026-09-03 @ $2.16 | last bar red; gate last_red=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $575.84 |
| `SION` | 86 | 2026-09-03 @ $6.63 | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=-18.1; leftover $575.84 |
| `DUOL` | 3 | 2026-09-03 @ $156.24 | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=+10.0; leftover $575.84 |
| `SAFX` | 1476 | 2026-09-03 @ $0.39 | last bar red; gate last_red=True; list yday_gainer; ret5=-26.5; leftover $575.84 |
| `ASND` | 2 | 2026-09-04 @ $266.94 | last bar red; gate last_red=True; list flatten; ret5=+1.9; leftover $756.14 |
| `SLBT` | 246 | 2026-09-04 @ $3.07 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $756.14 |
| `MLYS` | 25 | 2026-09-04 @ $29.15 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $756.14 |
| `CCOI` | 73 | 2026-09-04 @ $10.22 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $756.14 |
| `IRD` | 162 | 2026-09-04 @ $4.66 | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $756.14 |
| `JLHL` | 121 | 2026-09-04 @ $6.20 | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=-8.2; leftover $756.14 |
