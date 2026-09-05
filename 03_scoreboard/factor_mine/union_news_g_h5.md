# Factor mine action — `union_news_g_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ news_g hold 5, no 🚨

Cash book **-10.92%** ($8,908) · signal-only (no cash/fees) was +152.54%. Starts YES **4/17**. Fills 81 · skips 189 · realized $-876.73.

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

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | TLN, VST, NRG, ANGX, ARX, MH, HLIT | — | $1,560.49 | $10,110.67 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $1,560.49 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94 | $10,211.68 | +101.01 | DVN, EOG, FANG, CELC, OUST | — | $212.20 | $10,059.83 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6 | 09:30 open · cash $1,560.49 (unchanged overnight, no fees) · equity $10,211.68 vs prior close $10,110.67 (+101.01) because holdings re-marked: TLN×3 yday $362.74 → 09:30 $367.88 +15.42; VST×8 yday $148.13 → 09:30 $149.37 +9.92; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; MH×92 yday $13.10 → 09:30 $13.16 +5.52; HLIT×94 yday $13.92 → 09:30 $13.84 -7.52 |
| 2026-08-18 | -6.20 | $212.20 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6 | $10,014.18 | -45.65 | — | — | $212.20 | $9,819.43 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6 | 09:30 open · cash $212.20 (unchanged overnight, no fees) · equity $10,014.18 vs prior close $10,059.83 (-45.65) because holdings re-marked: TLN×3 yday $356.92 → 09:30 $350.89 -18.09; VST×8 yday $146.11 → 09:30 $144.50 -12.88; NRG×10 yday $122.37 → 09:30 $121.92 -4.50; ANGX×290 yday $4.71 → 09:30 $4.79 +23.20; ARX×63 yday $19.54 → 09:30 $19.57 +1.89; MH×92 yday $12.77 → 09:30 $13.00 +21.16; HLIT×94 yday $13.43 → 09:30 $12.93 -47.00; DVN×6 yday $47.57 → 09:30 $48.00 +2.58; EOG×2 yday $146.15 → 09:30 $148.04 +3.78; FANG×1 yday $206.29 → 09:30 $208.93 +2.64; CELC×3 yday $92.44 → 09:30 $92.38 -0.18; OUST×6 yday $48.13 → 09:30 $45.09 -18.24 |
| 2026-08-19 | -7.20 | $212.20 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6 | $9,839.65 | +20.22 | — | — | $212.20 | $9,805.29 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6 | 09:30 open · cash $212.20 (unchanged overnight, no fees) · equity $9,839.65 vs prior close $9,819.43 (+20.22) because holdings re-marked: TLN×3 yday $317.66 → 09:30 $321.00 +10.02; VST×8 yday $140.52 → 09:30 $140.74 +1.76; NRG×10 yday $115.56 → 09:30 $116.20 +6.40; ANGX×290 yday $4.85 → 09:30 $4.79 -17.40; ARX×63 yday $19.56 → 09:30 $19.58 +1.26; MH×92 yday $13.12 → 09:30 $13.01 -10.12; HLIT×94 yday $12.73 → 09:30 $12.90 +15.98; DVN×6 yday $47.83 → 09:30 $48.22 +2.34; EOG×2 yday $148.70 → 09:30 $149.86 +2.32; FANG×1 yday $210.02 → 09:30 $210.84 +0.82; CELC×3 yday $93.24 → 09:30 $95.50 +6.78; OUST×6 yday $42.99 → 09:30 $43.00 +0.06 |
| 2026-08-20 | +1.12 | $212.20 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6 | $9,773.30 | -31.99 | HUMA, BTGO, AUTL | — | $133.99 | $9,583.00 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6, HUMA×37, BTGO×4, AUTL×10 | 09:30 open · cash $212.20 (unchanged overnight, no fees) · equity $9,773.30 vs prior close $9,805.29 (-31.99) because holdings re-marked: TLN×3 yday $322.35 → 09:30 $320.28 -6.21; VST×8 yday $142.70 → 09:30 $142.70 +0.00; NRG×10 yday $120.58 → 09:30 $119.96 -6.20; ANGX×290 yday $4.60 → 09:30 $4.57 -8.70; ARX×63 yday $19.55 → 09:30 $19.55 +0.00; MH×92 yday $13.20 → 09:30 $13.01 -17.48; HLIT×94 yday $12.56 → 09:30 $12.47 -8.46; DVN×6 yday $48.19 → 09:30 $49.02 +4.98; EOG×2 yday $149.48 → 09:30 $151.45 +3.94; FANG×1 yday $208.55 → 09:30 $213.51 +4.96; CELC×3 yday $93.65 → 09:30 $92.90 -2.25; OUST×6 yday $40.06 → 09:30 $40.63 +3.42 |
| 2026-08-21 | +3.25 | $133.99 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6, HUMA×37, BTGO×4, AUTL×10 | $9,651.83 | +68.83 | AU, CRSP, FUTU, DE, MARA, BTDR, HIVE | TLN, VST, NRG, ANGX, ARX, MH, HLIT | $709.20 | $9,643.41 | DVN×6, EOG×2, FANG×1, CELC×3, OUST×6, HUMA×37, BTGO×4, AUTL×10, AU×9, CRSP×19, FUTU×10, DE×1, MARA×100, BTDR×106, HIVE×363 | 09:30 open · cash $133.99 (unchanged overnight, no fees) · equity $9,651.83 vs prior close $9,583.00 (+68.83) because holdings re-marked: TLN×3 yday $316.99 → 09:30 $318.52 +4.59; VST×8 yday $138.94 → 09:30 $139.99 +8.40; NRG×10 yday $115.36 → 09:30 $116.58 +12.20; ANGX×290 yday $4.37 → 09:30 $4.43 +17.40; ARX×63 yday $19.57 → 09:30 $19.57 +0.00; MH×92 yday $12.86 → 09:30 $12.87 +0.92; HLIT×94 yday $12.37 → 09:30 $12.48 +10.34; DVN×6 yday $49.30 → 09:30 $49.45 +0.90; EOG×2 yday $152.19 → 09:30 $152.29 +0.20; FANG×1 yday $211.02 → 09:30 $211.84 +0.82; CELC×3 yday $90.57 → 09:30 $91.39 +2.46; OUST×6 yday $37.95 → 09:30 $39.51 +9.36; HUMA×37 yday $0.68 → 09:30 $0.67 -0.26; BTGO×4 yday $6.60 → 09:30 $6.95 +1.40; AUTL×10 yday $2.46 → 09:30 $2.47 +0.10 |
| 2026-08-24 | -5.17 | $709.20 | DVN×6, EOG×2, FANG×1, CELC×3, OUST×6, HUMA×37, BTGO×4, AUTL×10, AU×9, CRSP×19, FUTU×10, DE×1, MARA×100, BTDR×106, HIVE×363 | $9,575.77 | -67.64 | — | DVN, EOG, FANG, CELC, OUST | $2,007.92 | $9,461.92 | HUMA×37, BTGO×4, AUTL×10, AU×9, CRSP×19, FUTU×10, DE×1, MARA×100, BTDR×106, HIVE×363 | 09:30 open · cash $709.20 (unchanged overnight, no fees) · equity $9,575.77 vs prior close $9,643.41 (-67.64) because holdings re-marked: DVN×6 yday $49.10 → 09:30 $48.84 -1.56; EOG×2 yday $153.05 → 09:30 $152.61 -0.88; FANG×1 yday $210.72 → 09:30 $209.47 -1.25; CELC×3 yday $93.53 → 09:30 $92.75 -2.34; OUST×6 yday $38.42 → 09:30 $37.14 -7.68; HUMA×37 yday $0.64 → 09:30 $0.68 +1.41; BTGO×4 yday $6.84 → 09:30 $6.87 +0.12; AUTL×10 yday $2.41 → 09:30 $2.36 -0.50; AU×9 yday $121.22 → 09:30 $120.50 -6.48; CRSP×19 yday $59.50 → 09:30 $58.79 -13.49; FUTU×10 yday $123.64 → 09:30 $120.87 -27.70; DE×1 yday $647.47 → 09:30 $653.62 +6.15; MARA×100 yday $11.26 → 09:30 $11.18 -8.00; BTDR×106 yday $11.37 → 09:30 $11.49 +12.72; HIVE×363 yday $3.03 → 09:30 $2.98 -18.15 |
| 2026-08-25 | +1.80 | $2,007.92 | HUMA×37, BTGO×4, AUTL×10, AU×9, CRSP×19, FUTU×10, DE×1, MARA×100, BTDR×106, HIVE×363 | $9,408.25 | -53.67 | RUM, EZPW, REAX, TRLV, VIRT, HOOD, ZYME, BKKT | — | $126.29 | $9,426.39 | HUMA×37, BTGO×4, AUTL×10, AU×9, CRSP×19, FUTU×10, DE×1, MARA×100, BTDR×106, HIVE×363, RUM×26, EZPW×7, REAX×10, TRLV×22, VIRT×3, HOOD×2, ZYME×8, BKKT×30 | 09:30 open · cash $2,007.92 (unchanged overnight, no fees) · equity $9,408.25 vs prior close $9,461.92 (-53.67) because holdings re-marked: HUMA×37 yday $0.67 → 09:30 $0.67 +0.00; BTGO×4 yday $6.97 → 09:30 $6.89 -0.32; AUTL×10 yday $2.38 → 09:30 $2.32 -0.60; AU×9 yday $118.66 → 09:30 $119.46 +7.20; CRSP×19 yday $56.91 → 09:30 $57.00 +1.71; FUTU×10 yday $116.49 → 09:30 $118.02 +15.30; DE×1 yday $654.38 → 09:30 $648.64 -5.74; MARA×100 yday $11.44 → 09:30 $11.28 -16.00; BTDR×106 yday $11.30 → 09:30 $11.19 -11.66; HIVE×363 yday $2.94 → 09:30 $2.82 -43.56 |
| 2026-08-26 | +2.02 | $126.29 | HUMA×37, BTGO×4, AUTL×10, AU×9, CRSP×19, FUTU×10, DE×1, MARA×100, BTDR×106, HIVE×363, RUM×26, EZPW×7, REAX×10, TRLV×22, VIRT×3, HOOD×2, ZYME×8, BKKT×30 | $9,426.39 | -0.00 | — | — | $126.29 | $9,392.01 | HUMA×37, BTGO×4, AUTL×10, AU×9, CRSP×19, FUTU×10, DE×1, MARA×100, BTDR×106, HIVE×363, RUM×26, EZPW×7, REAX×10, TRLV×22, VIRT×3, HOOD×2, ZYME×8, BKKT×30 | 09:30 open · cash $126.29 (unchanged overnight, no fees) · equity $9,426.39 vs prior close $9,426.39 (-0.00) because holdings re-marked: HUMA×37 yday $0.68 → 09:30 $0.68 +0.00; BTGO×4 yday $6.90 → 09:30 $6.90 +0.00; AUTL×10 yday $2.34 → 09:30 $2.34 +0.00; AU×9 yday $118.55 → 09:30 $118.55 +0.00; CRSP×19 yday $57.03 → 09:30 $57.03 +0.00; FUTU×10 yday $118.50 → 09:30 $118.50 +0.00; DE×1 yday $649.11 → 09:30 $649.11 +0.00; MARA×100 yday $11.29 → 09:30 $11.29 +0.00; BTDR×106 yday $11.28 → 09:30 $11.28 +0.00; HIVE×363 yday $2.89 → 09:30 $2.89 +0.00; RUM×26 yday $9.35 → 09:30 $9.35 +0.00; EZPW×7 yday $34.69 → 09:30 $34.69 +0.00; REAX×10 yday $24.00 → 09:30 $24.00 +0.00; TRLV×22 yday $11.02 → 09:30 $11.02 +0.00; VIRT×3 yday $66.29 → 09:30 $66.29 +0.00; HOOD×2 yday $104.22 → 09:30 $104.22 +0.00; ZYME×8 yday $29.81 → 09:30 $29.81 +0.00; BKKT×30 yday $8.38 → 09:30 $8.38 +0.00 |
| 2026-08-27 | — | $126.29 | HUMA×37, BTGO×4, AUTL×10, AU×9, CRSP×19, FUTU×10, DE×1, MARA×100, BTDR×106, HIVE×363, RUM×26, EZPW×7, REAX×10, TRLV×22, VIRT×3, HOOD×2, ZYME×8, BKKT×30 | $9,615.04 | +223.03 | — | HUMA, BTGO, AUTL | $203.90 | $9,489.25 | AU×9, CRSP×19, FUTU×10, DE×1, MARA×100, BTDR×106, HIVE×363, RUM×26, EZPW×7, REAX×10, TRLV×22, VIRT×3, HOOD×2, ZYME×8, BKKT×30 | 09:30 open · cash $126.29 (unchanged overnight, no fees) · equity $9,615.04 vs prior close $9,392.01 (+223.03) because holdings re-marked: HUMA×37 yday $0.68 → 09:30 $0.71 +1.11; BTGO×4 yday $6.90 → 09:30 $7.06 +0.64; AUTL×10 yday $2.34 → 09:30 $2.41 +0.70; AU×9 yday $118.55 → 09:30 $119.80 +11.25; CRSP×19 yday $57.03 → 09:30 $60.18 +59.85; FUTU×10 yday $118.50 → 09:30 $124.67 +61.70; DE×1 yday $649.11 → 09:30 $632.15 -16.96; MARA×100 yday $11.29 → 09:30 $11.56 +27.00; BTDR×106 yday $11.28 → 09:30 $11.05 -24.38; HIVE×363 yday $2.89 → 09:30 $2.95 +21.78; RUM×26 yday $9.35 → 09:30 $10.07 +18.72; EZPW×7 yday $34.69 → 09:30 $35.70 +7.07; REAX×10 yday $24.00 → 09:30 $26.61 +26.10; TRLV×22 yday $11.02 → 09:30 $11.22 +4.40; VIRT×3 yday $66.29 → 09:30 $64.92 -4.11; HOOD×2 yday $104.22 → 09:30 $110.11 +11.78; ZYME×8 yday $29.81 → 09:30 $27.56 -18.00; BKKT×30 yday $8.38 → 09:30 $8.38 +0.00 |
| 2026-08-28 | +0.75 | $203.90 | AU×9, CRSP×19, FUTU×10, DE×1, MARA×100, BTDR×106, HIVE×363, RUM×26, EZPW×7, REAX×10, TRLV×22, VIRT×3, HOOD×2, ZYME×8, BKKT×30 | $9,605.89 | +116.64 | RRC, CAPR, SEDG, SMTC, OPTX, ERAS, BBWI | AU, CRSP, FUTU, DE, MARA, BTDR, HIVE | $115.59 | $9,625.61 | RUM×26, EZPW×7, REAX×10, TRLV×22, VIRT×3, HOOD×2, ZYME×8, BKKT×30, RRC×26, CAPR×119, SEDG×32, SMTC×7, OPTX×128, ERAS×56, BBWI×58 | 09:30 open · cash $203.90 (unchanged overnight, no fees) · equity $9,605.89 vs prior close $9,489.25 (+116.64) because holdings re-marked: AU×9 yday $118.11 → 09:30 $117.41 -6.30; CRSP×19 yday $59.23 → 09:30 $59.12 -2.09; FUTU×10 yday $127.34 → 09:30 $128.00 +6.60; DE×1 yday $634.54 → 09:30 $628.82 -5.72; MARA×100 yday $11.22 → 09:30 $11.53 +31.00; BTDR×106 yday $10.67 → 09:30 $11.20 +56.18; HIVE×363 yday $2.87 → 09:30 $2.96 +32.67; RUM×26 yday $9.38 → 09:30 $9.51 +3.38; EZPW×7 yday $33.90 → 09:30 $33.50 -2.80; REAX×10 yday $26.59 → 09:30 $25.91 -6.80; TRLV×22 yday $11.43 → 09:30 $11.38 -1.10; VIRT×3 yday $65.74 → 09:30 $65.42 -0.96; HOOD×2 yday $108.54 → 09:30 $110.70 +4.32; ZYME×8 yday $29.31 → 09:30 $29.33 +0.16; BKKT×30 yday $8.23 → 09:30 $8.50 +8.10 |
| 2026-08-31 | -5.85 | $115.59 | RUM×26, EZPW×7, REAX×10, TRLV×22, VIRT×3, HOOD×2, ZYME×8, BKKT×30, RRC×26, CAPR×119, SEDG×32, SMTC×7, OPTX×128, ERAS×56, BBWI×58 | $9,269.48 | -356.13 | — | — | $115.59 | $9,266.06 | RUM×26, EZPW×7, REAX×10, TRLV×22, VIRT×3, HOOD×2, ZYME×8, BKKT×30, RRC×26, CAPR×119, SEDG×32, SMTC×7, OPTX×128, ERAS×56, BBWI×58 | 09:30 open · cash $115.59 (unchanged overnight, no fees) · equity $9,269.48 vs prior close $9,625.61 (-356.13) because holdings re-marked: RUM×26 yday $9.43 → 09:30 $8.90 -13.78; EZPW×7 yday $34.41 → 09:30 $33.00 -9.87; REAX×10 yday $23.63 → 09:30 $21.15 -24.80; TRLV×22 yday $11.03 → 09:30 $12.41 +30.36; VIRT×3 yday $67.04 → 09:30 $66.39 -1.95; HOOD×2 yday $109.76 → 09:30 $105.19 -9.14; ZYME×8 yday $29.01 → 09:30 $28.27 -5.92; BKKT×30 yday $8.42 → 09:30 $7.58 -25.20; RRC×26 yday $41.64 → 09:30 $41.11 -13.78; CAPR×119 yday $10.06 → 09:30 $9.44 -73.78; SEDG×32 yday $33.51 → 09:30 $31.50 -64.32; SMTC×7 yday $142.43 → 09:30 $133.04 -65.73; OPTX×128 yday $8.73 → 09:30 $8.52 -26.88; ERAS×56 yday $19.49 → 09:30 $17.90 -89.04; BBWI×58 yday $18.65 → 09:30 $19.30 +37.70 |
| 2026-09-01 | -6.30 | $115.59 | RUM×26, EZPW×7, REAX×10, TRLV×22, VIRT×3, HOOD×2, ZYME×8, BKKT×30, RRC×26, CAPR×119, SEDG×32, SMTC×7, OPTX×128, ERAS×56, BBWI×58 | $9,337.44 | +71.38 | — | RUM, EZPW, REAX, VIRT, HOOD, ZYME, BKKT | $1,629.41 | $9,249.34 | TRLV×22, RRC×26, CAPR×119, SEDG×32, SMTC×7, OPTX×128, ERAS×56, BBWI×58 | 09:30 open · cash $115.59 (unchanged overnight, no fees) · equity $9,337.44 vs prior close $9,266.06 (+71.38) because holdings re-marked: RUM×26 yday $8.86 → 09:30 $8.90 +1.04; EZPW×7 yday $33.00 → 09:30 $32.05 -6.65; REAX×10 yday $21.15 → 09:30 $19.32 -18.30; TRLV×22 yday $12.41 → 09:30 $11.89 -11.44; VIRT×3 yday $66.39 → 09:30 $65.64 -2.25; HOOD×2 yday $104.80 → 09:30 $107.57 +5.54; ZYME×8 yday $28.27 → 09:30 $29.32 +8.40; BKKT×30 yday $7.78 → 09:30 $7.75 -0.90; RRC×26 yday $41.78 → 09:30 $41.32 -11.96; CAPR×119 yday $9.36 → 09:30 $10.43 +127.33; SEDG×32 yday $31.27 → 09:30 $32.22 +30.40; SMTC×7 yday $132.54 → 09:30 $131.65 -6.23; OPTX×128 yday $8.52 → 09:30 $8.19 -42.24; ERAS×56 yday $17.90 → 09:30 $18.00 +5.60; BBWI×58 yday $19.22 → 09:30 $19.10 -6.96 |
| 2026-09-02 | -3.83 | $1,629.41 | TRLV×22, RRC×26, CAPR×119, SEDG×32, SMTC×7, OPTX×128, ERAS×56, BBWI×58 | $9,258.07 | +8.73 | — | — | $1,629.41 | $9,096.63 | TRLV×22, RRC×26, CAPR×119, SEDG×32, SMTC×7, OPTX×128, ERAS×56, BBWI×58 | 09:30 open · cash $1,629.41 (unchanged overnight, no fees) · equity $9,258.07 vs prior close $9,249.34 (+8.73) because holdings re-marked: TRLV×22 yday $11.89 → 09:30 $11.54 -7.70; RRC×26 yday $41.32 → 09:30 $41.94 +16.12; CAPR×119 yday $10.19 → 09:30 $10.77 +69.02; SEDG×32 yday $31.80 → 09:30 $31.87 +2.24; SMTC×7 yday $129.50 → 09:30 $127.63 -13.09; OPTX×128 yday $8.19 → 09:30 $7.94 -32.00; ERAS×56 yday $17.70 → 09:30 $17.58 -6.72; BBWI×58 yday $19.10 → 09:30 $18.77 -19.14 |
| 2026-09-03 | -0.90 | $1,629.41 | TRLV×22, RRC×26, CAPR×119, SEDG×32, SMTC×7, OPTX×128, ERAS×56, BBWI×58 | $9,096.04 | -0.59 | MMED, CNXC, TXG, ZYME, FCX | — | $384.30 | $9,170.64 | TRLV×22, RRC×26, CAPR×119, SEDG×32, SMTC×7, OPTX×128, ERAS×56, BBWI×58, MMED×11, CNXC×8, TXG×4, ZYME×9, FCX×3 | 09:30 open · cash $1,629.41 (unchanged overnight, no fees) · equity $9,096.04 vs prior close $9,096.63 (-0.59) because holdings re-marked: TRLV×22 yday $11.74 → 09:30 $11.78 +0.88; RRC×26 yday $42.40 → 09:30 $42.10 -7.80; CAPR×119 yday $10.01 → 09:30 $10.07 +7.14; SEDG×32 yday $32.49 → 09:30 $32.42 -2.24; SMTC×7 yday $132.27 → 09:30 $133.00 +5.11; OPTX×128 yday $7.28 → 09:30 $7.25 -3.84; ERAS×56 yday $16.76 → 09:30 $16.97 +11.76; BBWI×58 yday $18.61 → 09:30 $18.41 -11.60 |
| 2026-09-04 | — | $384.30 | TRLV×22, RRC×26, CAPR×119, SEDG×32, SMTC×7, OPTX×128, ERAS×56, BBWI×58, MMED×11, CNXC×8, TXG×4, ZYME×9, FCX×3 | $9,178.83 | +8.19 | BAK, AMTX | TRLV, RRC, CAPR, SEDG, SMTC, OPTX, ERAS, BBWI | $1.94 | $8,907.66 | MMED×11, CNXC×8, TXG×4, ZYME×9, FCX×3, BAK×2020, AMTX×2034 | 09:30 open · cash $384.30 (unchanged overnight, no fees) · equity $9,178.83 vs prior close $9,170.64 (+8.19) because holdings re-marked: TRLV×22 yday $11.69 → 09:30 $11.89 +4.40; RRC×26 yday $42.48 → 09:30 $42.43 -1.30; CAPR×119 yday $9.89 → 09:30 $9.83 -7.14; SEDG×32 yday $33.98 → 09:30 $33.69 -9.28; SMTC×7 yday $133.85 → 09:30 $133.10 -5.25; OPTX×128 yday $7.53 → 09:30 $7.59 +7.68; ERAS×56 yday $16.37 → 09:30 $16.38 +0.56; BBWI×58 yday $18.53 → 09:30 $18.59 +3.48; MMED×11 yday $23.76 → 09:30 $23.88 +1.32; CNXC×8 yday $32.37 → 09:30 $32.88 +4.08; TXG×4 yday $61.65 → 09:30 $62.35 +2.80; ZYME×9 yday $31.05 → 09:30 $31.34 +2.61; FCX×3 yday $73.93 → 09:30 $75.34 +4.23 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $8,918.51 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,741.30 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten; 🔵; ret5=+3.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,539.28 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $5,285.64 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $4,050.55 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $2,801.68 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $1,560.49 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,560.49 | ▲ 09:30 equity $10,211.68 vs yday $10,110.67 (+101.01) | 09:30 open · cash $1,560.49 (unchanged overnight, no fees) · equity $10,211.68 vs prior close $10,110.67 (+101.01) because holdings re-marked: TLN×3 yday $362.74 → 09:30 $367.88 +15.42; VST×8 yday $148.13 → 09:30 $149.37 +9.92; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; MH×92 yday $13.10 → 09:30 $13.16 +5.52; HLIT×94 yday $13.92 → 09:30 $13.84 -7.52 | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 6 | $46.18 | $2.01 | — | $1,281.40 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten; 🔵; ret5=+6.7; leftover $312.10 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 2 | $142.77 | $2.00 | — | $993.87 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.8; leftover $312.10 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 1 | $202.70 | $1.99 | — | $789.17 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten; 🔵; ret5=+8.3; leftover $312.10 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 3 | $92.99 | $2.00 | — | $508.20 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.8; leftover $312.10 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 6 | $49.00 | $2.01 | — | $212.20 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; ⚪; ret5=+12.2; leftover $312.10 | join🟡 sector🟢 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $212.20 | ▼ 09:30 equity $10,014.18 vs yday $10,059.83 (-45.65) | 09:30 open · cash $212.20 (unchanged overnight, no fees) · equity $10,014.18 vs prior close $10,059.83 (-45.65) because holdings re-marked: TLN×3 yday $356.92 → 09:30 $350.89 -18.09; VST×8 yday $146.11 → 09:30 $144.50 -12.88; NRG×10 yday $122.37 → 09:30 $121.92 -4.50; ANGX×290 yday $4.71 → 09:30 $4.79 +23.20; ARX×63 yday $19.54 → 09:30 $19.57 +1.89; MH×92 yday $12.77 → 09:30 $13.00 +21.16; HLIT×94 yday $13.43 → 09:30 $12.93 -47.00; DVN×6 yday $47.57 → 09:30 $48.00 +2.58; EOG×2 yday $146.15 → 09:30 $148.04 +3.78; FANG×1 yday $206.29 → 09:30 $208.93 +2.64; CELC×3 yday $92.44 → 09:30 $92.38 -0.18; OUST×6 yday $48.13 → 09:30 $45.09 -18.24 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $212.20 | ▲ 09:30 equity $9,839.65 vs yday $9,819.43 (+20.22) | 09:30 open · cash $212.20 (unchanged overnight, no fees) · equity $9,839.65 vs prior close $9,819.43 (+20.22) because holdings re-marked: TLN×3 yday $317.66 → 09:30 $321.00 +10.02; VST×8 yday $140.52 → 09:30 $140.74 +1.76; NRG×10 yday $115.56 → 09:30 $116.20 +6.40; ANGX×290 yday $4.85 → 09:30 $4.79 -17.40; ARX×63 yday $19.56 → 09:30 $19.58 +1.26; MH×92 yday $13.12 → 09:30 $13.01 -10.12; HLIT×94 yday $12.73 → 09:30 $12.90 +15.98; DVN×6 yday $47.83 → 09:30 $48.22 +2.34; EOG×2 yday $148.70 → 09:30 $149.86 +2.32; FANG×1 yday $210.02 → 09:30 $210.84 +0.82; CELC×3 yday $93.24 → 09:30 $95.50 +6.78; OUST×6 yday $42.99 → 09:30 $43.00 +0.06 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $212.20 | ▼ 09:30 equity $9,773.30 vs yday $9,805.29 (-31.99) | 09:30 open · cash $212.20 (unchanged overnight, no fees) · equity $9,773.30 vs prior close $9,805.29 (-31.99) because holdings re-marked: TLN×3 yday $322.35 → 09:30 $320.28 -6.21; VST×8 yday $142.70 → 09:30 $142.70 +0.00; NRG×10 yday $120.58 → 09:30 $119.96 -6.20; ANGX×290 yday $4.60 → 09:30 $4.57 -8.70; ARX×63 yday $19.55 → 09:30 $19.55 +0.00; MH×92 yday $13.20 → 09:30 $13.01 -17.48; HLIT×94 yday $12.56 → 09:30 $12.47 -8.46; DVN×6 yday $48.19 → 09:30 $49.02 +4.98; EOG×2 yday $149.48 → 09:30 $151.45 +3.94; FANG×1 yday $208.55 → 09:30 $213.51 +4.96; CELC×3 yday $93.65 → 09:30 $92.90 -2.25; OUST×6 yday $40.06 → 09:30 $40.63 +3.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 37 | $0.71 | $0.37 | — | $185.66 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $26.52 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 4 | $6.61 | $0.28 | — | $158.97 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $26.52 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $133.99 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $26.52 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $133.99 | ▲ 09:30 equity $9,651.83 vs yday $9,583.00 (+68.83) | 09:30 open · cash $133.99 (unchanged overnight, no fees) · equity $9,651.83 vs prior close $9,583.00 (+68.83) because holdings re-marked: TLN×3 yday $316.99 → 09:30 $318.52 +4.59; VST×8 yday $138.94 → 09:30 $139.99 +8.40; NRG×10 yday $115.36 → 09:30 $116.58 +12.20; ANGX×290 yday $4.37 → 09:30 $4.43 +17.40; ARX×63 yday $19.57 → 09:30 $19.57 +0.00; MH×92 yday $12.86 → 09:30 $12.87 +0.92; HLIT×94 yday $12.37 → 09:30 $12.48 +10.34; DVN×6 yday $49.30 → 09:30 $49.45 +0.90; EOG×2 yday $152.19 → 09:30 $152.29 +0.20; FANG×1 yday $211.02 → 09:30 $211.84 +0.82; CELC×3 yday $90.57 → 09:30 $91.39 +2.46; OUST×6 yday $37.95 → 09:30 $39.51 +9.36; HUMA×37 yday $0.68 → 09:30 $0.67 -0.26; BTGO×4 yday $6.60 → 09:30 $6.95 +1.40; AUTL×10 yday $2.46 → 09:30 $2.47 +0.10 | — |
| 2026-08-21 09:30 ET | **SELL** | `TLN` | 3 | $318.52 | $2.02 | $-127.95 | $1,087.53 | ▼ -127.95 after sell → book $9,649.81; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `VST` | 8 | $139.99 | $2.03 | $-59.33 | $2,205.42 | ▼ -59.33 after sell → book $9,647.78; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `NRG` | 10 | $116.58 | $2.04 | $-38.26 | $3,369.18 | ▼ -38.26 after sell → book $9,645.74; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ANGX` | 290 | $4.43 | $3.80 | $+27.26 | $4,650.08 | ▲ +27.26 after sell → book $9,641.94; vs 09:30 mark -3.80 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $5,880.79 | ▼ -4.38 after sell → book $9,639.74; vs 09:30 mark -2.20 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `MH` | 92 | $12.87 | $2.29 | $-67.12 | $7,062.54 | ▼ -67.12 after sell → book $9,637.44; vs 09:30 mark -2.30 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `HLIT` | 94 | $12.48 | $2.30 | $-70.37 | $8,233.36 | ▼ -70.37 after sell → book $9,635.15; vs 09:30 mark -2.30 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 9 | $119.43 | $2.02 | — | $7,156.47 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1176.19 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 19 | $59.72 | $2.05 | — | $6,019.75 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1176.19 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 10 | $115.18 | $2.02 | — | $4,865.93 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1176.19 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 1 | $623.26 | $1.99 | — | $4,240.67 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1176.19 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 100 | $11.70 | $2.29 | — | $3,068.38 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1176.19 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 106 | $11.10 | $2.31 | — | $1,890.00 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; ret5=+19.1; leftover $1176.19 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 363 | $3.24 | $4.68 | — | $709.20 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; leftover $1176.19 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $709.20 | ▼ 09:30 equity $9,575.77 vs yday $9,643.41 (-67.64) | 09:30 open · cash $709.20 (unchanged overnight, no fees) · equity $9,575.77 vs prior close $9,643.41 (-67.64) because holdings re-marked: DVN×6 yday $49.10 → 09:30 $48.84 -1.56; EOG×2 yday $153.05 → 09:30 $152.61 -0.88; FANG×1 yday $210.72 → 09:30 $209.47 -1.25; CELC×3 yday $93.53 → 09:30 $92.75 -2.34; OUST×6 yday $38.42 → 09:30 $37.14 -7.68; HUMA×37 yday $0.64 → 09:30 $0.68 +1.41; BTGO×4 yday $6.84 → 09:30 $6.87 +0.12; AUTL×10 yday $2.41 → 09:30 $2.36 -0.50; AU×9 yday $121.22 → 09:30 $120.50 -6.48; CRSP×19 yday $59.50 → 09:30 $58.79 -13.49; FUTU×10 yday $123.64 → 09:30 $120.87 -27.70; DE×1 yday $647.47 → 09:30 $653.62 +6.15; MARA×100 yday $11.26 → 09:30 $11.18 -8.00; BTDR×106 yday $11.37 → 09:30 $11.49 +12.72; HIVE×363 yday $3.03 → 09:30 $2.98 -18.15 | — |
| 2026-08-24 09:30 ET | **SELL** | `DVN` | 6 | $48.84 | $2.03 | $+11.92 | $1,000.21 | ▲ +11.92 after sell → book $9,573.74; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `EOG` | 2 | $152.61 | $2.02 | $+15.67 | $1,303.42 | ▲ +15.67 after sell → book $9,571.73; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `FANG` | 1 | $209.47 | $2.01 | $+2.76 | $1,510.87 | ▲ +2.76 after sell → book $9,569.71; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `CELC` | 3 | $92.75 | $2.02 | $-4.74 | $1,787.11 | ▼ -4.74 after sell → book $9,567.70; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `OUST` | 6 | $37.14 | $2.03 | $-75.20 | $2,007.92 | ▼ -75.20 after sell → book $9,565.67; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,007.92 | ▼ 09:30 equity $9,408.25 vs yday $9,461.92 (-53.67) | 09:30 open · cash $2,007.92 (unchanged overnight, no fees) · equity $9,408.25 vs prior close $9,461.92 (-53.67) because holdings re-marked: HUMA×37 yday $0.67 → 09:30 $0.67 +0.00; BTGO×4 yday $6.97 → 09:30 $6.89 -0.32; AUTL×10 yday $2.38 → 09:30 $2.32 -0.60; AU×9 yday $118.66 → 09:30 $119.46 +7.20; CRSP×19 yday $56.91 → 09:30 $57.00 +1.71; FUTU×10 yday $116.49 → 09:30 $118.02 +15.30; DE×1 yday $654.38 → 09:30 $648.64 -5.74; MARA×100 yday $11.44 → 09:30 $11.28 -16.00; BTDR×106 yday $11.30 → 09:30 $11.19 -11.66; HIVE×363 yday $2.94 → 09:30 $2.82 -43.56 | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 26 | $9.36 | $2.07 | — | $1,762.49 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; leftover $250.99 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 7 | $34.48 | $2.01 | — | $1,519.12 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $250.99 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 10 | $24.00 | $2.02 | — | $1,277.10 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_mover; ret5=+10.0; leftover $250.99 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `TRLV` | 22 | $11.02 | $2.06 | — | $1,032.60 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.0; leftover $250.99 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VIRT` | 3 | $66.29 | $1.99 | — | $831.74 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; ret5=+13.2; leftover $250.99 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HOOD` | 2 | $106.00 | $2.00 | — | $617.74 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; ⚪; ret5=+13.2; leftover $250.99 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 8 | $29.87 | $2.01 | — | $376.77 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+14.1; leftover $250.99 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BKKT` | 30 | $8.28 | $2.08 | — | $126.29 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+12.3; leftover $250.99 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $126.29 | ▲ 09:30 equity $9,426.39 vs yday $9,426.39 (-0.00) | 09:30 open · cash $126.29 (unchanged overnight, no fees) · equity $9,426.39 vs prior close $9,426.39 (-0.00) because holdings re-marked: HUMA×37 yday $0.68 → 09:30 $0.68 +0.00; BTGO×4 yday $6.90 → 09:30 $6.90 +0.00; AUTL×10 yday $2.34 → 09:30 $2.34 +0.00; AU×9 yday $118.55 → 09:30 $118.55 +0.00; CRSP×19 yday $57.03 → 09:30 $57.03 +0.00; FUTU×10 yday $118.50 → 09:30 $118.50 +0.00; DE×1 yday $649.11 → 09:30 $649.11 +0.00; MARA×100 yday $11.29 → 09:30 $11.29 +0.00; BTDR×106 yday $11.28 → 09:30 $11.28 +0.00; HIVE×363 yday $2.89 → 09:30 $2.89 +0.00; RUM×26 yday $9.35 → 09:30 $9.35 +0.00; EZPW×7 yday $34.69 → 09:30 $34.69 +0.00; REAX×10 yday $24.00 → 09:30 $24.00 +0.00; TRLV×22 yday $11.02 → 09:30 $11.02 +0.00; VIRT×3 yday $66.29 → 09:30 $66.29 +0.00; HOOD×2 yday $104.22 → 09:30 $104.22 +0.00; ZYME×8 yday $29.81 → 09:30 $29.81 +0.00; BKKT×30 yday $8.38 → 09:30 $8.38 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $126.29 | ▲ 09:30 equity $9,615.04 vs yday $9,392.01 (+223.03) | 09:30 open · cash $126.29 (unchanged overnight, no fees) · equity $9,615.04 vs prior close $9,392.01 (+223.03) because holdings re-marked: HUMA×37 yday $0.68 → 09:30 $0.71 +1.11; BTGO×4 yday $6.90 → 09:30 $7.06 +0.64; AUTL×10 yday $2.34 → 09:30 $2.41 +0.70; AU×9 yday $118.55 → 09:30 $119.80 +11.25; CRSP×19 yday $57.03 → 09:30 $60.18 +59.85; FUTU×10 yday $118.50 → 09:30 $124.67 +61.70; DE×1 yday $649.11 → 09:30 $632.15 -16.96; MARA×100 yday $11.29 → 09:30 $11.56 +27.00; BTDR×106 yday $11.28 → 09:30 $11.05 -24.38; HIVE×363 yday $2.89 → 09:30 $2.95 +21.78; RUM×26 yday $9.35 → 09:30 $10.07 +18.72; EZPW×7 yday $34.69 → 09:30 $35.70 +7.07; REAX×10 yday $24.00 → 09:30 $26.61 +26.10; TRLV×22 yday $11.02 → 09:30 $11.22 +4.40; VIRT×3 yday $66.29 → 09:30 $64.92 -4.11; HOOD×2 yday $104.22 → 09:30 $110.11 +11.78; ZYME×8 yday $29.81 → 09:30 $27.56 -18.00; BKKT×30 yday $8.38 → 09:30 $8.38 +0.00 | — |
| 2026-08-27 09:30 ET | **SELL** | `HUMA` | 37 | $0.71 | $0.39 | $-0.66 | $152.17 | ▼ -0.66 after sell → book $9,614.65; vs 09:30 mark -0.39 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BTGO` | 4 | $7.06 | $0.31 | $+1.23 | $180.09 | ▲ +1.23 after sell → book $9,614.33; vs 09:30 mark -0.32 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 10 | $2.41 | $0.29 | $-1.17 | $203.90 | ▼ -1.17 after sell → book $9,614.04; vs 09:30 mark -0.29 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $203.90 | ▲ 09:30 equity $9,605.89 vs yday $9,489.25 (+116.64) | 09:30 open · cash $203.90 (unchanged overnight, no fees) · equity $9,605.89 vs prior close $9,489.25 (+116.64) because holdings re-marked: AU×9 yday $118.11 → 09:30 $117.41 -6.30; CRSP×19 yday $59.23 → 09:30 $59.12 -2.09; FUTU×10 yday $127.34 → 09:30 $128.00 +6.60; DE×1 yday $634.54 → 09:30 $628.82 -5.72; MARA×100 yday $11.22 → 09:30 $11.53 +31.00; BTDR×106 yday $10.67 → 09:30 $11.20 +56.18; HIVE×363 yday $2.87 → 09:30 $2.96 +32.67; RUM×26 yday $9.38 → 09:30 $9.51 +3.38; EZPW×7 yday $33.90 → 09:30 $33.50 -2.80; REAX×10 yday $26.59 → 09:30 $25.91 -6.80; TRLV×22 yday $11.43 → 09:30 $11.38 -1.10; VIRT×3 yday $65.74 → 09:30 $65.42 -0.96; HOOD×2 yday $108.54 → 09:30 $110.70 +4.32; ZYME×8 yday $29.31 → 09:30 $29.33 +0.16; BKKT×30 yday $8.23 → 09:30 $8.50 +8.10 | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 9 | $117.41 | $2.04 | $-22.23 | $1,258.55 | ▼ -22.23 after sell → book $9,603.85; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRSP` | 19 | $59.12 | $2.07 | $-15.51 | $2,379.77 | ▼ -15.51 after sell → book $9,601.79; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `FUTU` | 10 | $128.00 | $2.04 | $+124.14 | $3,657.73 | ▲ +124.14 after sell → book $9,599.75; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `DE` | 1 | $628.82 | $2.01 | $+1.55 | $4,284.53 | ▲ +1.55 after sell → book $9,597.73; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `MARA` | 100 | $11.53 | $2.32 | $-21.61 | $5,435.22 | ▼ -21.61 after sell → book $9,595.42; vs 09:30 mark -2.31 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `BTDR` | 106 | $11.20 | $2.34 | $+6.49 | $6,620.08 | ▲ +6.49 after sell → book $9,593.08; vs 09:30 mark -2.34 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `HIVE` | 363 | $2.96 | $4.75 | $-111.08 | $7,689.81 | ▼ -111.08 after sell → book $9,588.33; vs 09:30 mark -4.75 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 26 | $41.44 | $2.07 | — | $6,610.30 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list flatten; ret5=+1.8; leftover $1098.54 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 119 | $9.19 | $2.35 | — | $5,514.34 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1098.54 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 32 | $33.78 | $2.09 | — | $4,431.30 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1098.54 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 7 | $149.40 | $2.01 | — | $3,383.49 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1098.54 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 128 | $8.57 | $2.37 | — | $2,284.15 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; ret5=-3.4; leftover $1098.54 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 56 | $19.30 | $2.16 | — | $1,201.19 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; ret5=-4.1; leftover $1098.54 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 58 | $18.68 | $2.16 | — | $115.59 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; ret5=+0.2; leftover $1098.54 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $115.59 | ▼ 09:30 equity $9,269.48 vs yday $9,625.61 (-356.13) | 09:30 open · cash $115.59 (unchanged overnight, no fees) · equity $9,269.48 vs prior close $9,625.61 (-356.13) because holdings re-marked: RUM×26 yday $9.43 → 09:30 $8.90 -13.78; EZPW×7 yday $34.41 → 09:30 $33.00 -9.87; REAX×10 yday $23.63 → 09:30 $21.15 -24.80; TRLV×22 yday $11.03 → 09:30 $12.41 +30.36; VIRT×3 yday $67.04 → 09:30 $66.39 -1.95; HOOD×2 yday $109.76 → 09:30 $105.19 -9.14; ZYME×8 yday $29.01 → 09:30 $28.27 -5.92; BKKT×30 yday $8.42 → 09:30 $7.58 -25.20; RRC×26 yday $41.64 → 09:30 $41.11 -13.78; CAPR×119 yday $10.06 → 09:30 $9.44 -73.78; SEDG×32 yday $33.51 → 09:30 $31.50 -64.32; SMTC×7 yday $142.43 → 09:30 $133.04 -65.73; OPTX×128 yday $8.73 → 09:30 $8.52 -26.88; ERAS×56 yday $19.49 → 09:30 $17.90 -89.04; BBWI×58 yday $18.65 → 09:30 $19.30 +37.70 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $115.59 | ▲ 09:30 equity $9,337.44 vs yday $9,266.06 (+71.38) | 09:30 open · cash $115.59 (unchanged overnight, no fees) · equity $9,337.44 vs prior close $9,266.06 (+71.38) because holdings re-marked: RUM×26 yday $8.86 → 09:30 $8.90 +1.04; EZPW×7 yday $33.00 → 09:30 $32.05 -6.65; REAX×10 yday $21.15 → 09:30 $19.32 -18.30; TRLV×22 yday $12.41 → 09:30 $11.89 -11.44; VIRT×3 yday $66.39 → 09:30 $65.64 -2.25; HOOD×2 yday $104.80 → 09:30 $107.57 +5.54; ZYME×8 yday $28.27 → 09:30 $29.32 +8.40; BKKT×30 yday $7.78 → 09:30 $7.75 -0.90; RRC×26 yday $41.78 → 09:30 $41.32 -11.96; CAPR×119 yday $9.36 → 09:30 $10.43 +127.33; SEDG×32 yday $31.27 → 09:30 $32.22 +30.40; SMTC×7 yday $132.54 → 09:30 $131.65 -6.23; OPTX×128 yday $8.52 → 09:30 $8.19 -42.24; ERAS×56 yday $17.90 → 09:30 $18.00 +5.60; BBWI×58 yday $19.22 → 09:30 $19.10 -6.96 | — |
| 2026-09-01 09:30 ET | **SELL** | `RUM` | 26 | $8.90 | $2.09 | $-16.12 | $344.90 | ▼ -16.12 after sell → book $9,335.35; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `EZPW` | 7 | $32.05 | $2.03 | $-21.05 | $567.22 | ▼ -21.05 after sell → book $9,333.32; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `REAX` | 10 | $19.32 | $1.98 | $-50.80 | $758.44 | ▼ -50.80 after sell → book $9,331.34; vs 09:30 mark -1.98 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `VIRT` | 3 | $65.64 | $2.00 | $-5.94 | $953.36 | ▼ -5.94 after sell → book $9,329.34; vs 09:30 mark -2.00 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `HOOD` | 2 | $107.57 | $2.02 | $-0.87 | $1,166.48 | ▼ -0.87 after sell → book $9,327.32; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `ZYME` | 8 | $29.32 | $2.03 | $-8.45 | $1,399.01 | ▼ -8.45 after sell → book $9,325.29; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `BKKT` | 30 | $7.75 | $2.10 | $-20.08 | $1,629.41 | ▼ -20.08 after sell → book $9,323.19; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,629.41 | ▲ 09:30 equity $9,258.07 vs yday $9,249.34 (+8.73) | 09:30 open · cash $1,629.41 (unchanged overnight, no fees) · equity $9,258.07 vs prior close $9,249.34 (+8.73) because holdings re-marked: TRLV×22 yday $11.89 → 09:30 $11.54 -7.70; RRC×26 yday $41.32 → 09:30 $41.94 +16.12; CAPR×119 yday $10.19 → 09:30 $10.77 +69.02; SEDG×32 yday $31.80 → 09:30 $31.87 +2.24; SMTC×7 yday $129.50 → 09:30 $127.63 -13.09; OPTX×128 yday $8.19 → 09:30 $7.94 -32.00; ERAS×56 yday $17.70 → 09:30 $17.58 -6.72; BBWI×58 yday $19.10 → 09:30 $18.77 -19.14 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,629.41 | ▼ 09:30 equity $9,096.04 vs yday $9,096.63 (-0.59) | 09:30 open · cash $1,629.41 (unchanged overnight, no fees) · equity $9,096.04 vs prior close $9,096.63 (-0.59) because holdings re-marked: TRLV×22 yday $11.74 → 09:30 $11.78 +0.88; RRC×26 yday $42.40 → 09:30 $42.10 -7.80; CAPR×119 yday $10.01 → 09:30 $10.07 +7.14; SEDG×32 yday $32.49 → 09:30 $32.42 -2.24; SMTC×7 yday $132.27 → 09:30 $133.00 +5.11; OPTX×128 yday $7.28 → 09:30 $7.25 -3.84; ERAS×56 yday $16.76 → 09:30 $16.97 +11.76; BBWI×58 yday $18.61 → 09:30 $18.41 -11.60 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 11 | $22.78 | $2.02 | — | $1,376.81 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $271.57 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 8 | $31.80 | $2.01 | — | $1,120.39 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+3.7; leftover $271.57 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `TXG` | 4 | $60.24 | $2.00 | — | $877.43 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; ret5=+16.1; leftover $271.57 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ZYME` | 9 | $30.00 | $2.02 | — | $605.41 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; ⚪; ret5=+14.1; leftover $271.57 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟡 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FCX` | 3 | $73.04 | $2.00 | — | $384.30 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.3; leftover $271.57 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $384.30 | ▲ 09:30 equity $9,178.83 vs yday $9,170.64 (+8.19) | 09:30 open · cash $384.30 (unchanged overnight, no fees) · equity $9,178.83 vs prior close $9,170.64 (+8.19) because holdings re-marked: TRLV×22 yday $11.69 → 09:30 $11.89 +4.40; RRC×26 yday $42.48 → 09:30 $42.43 -1.30; CAPR×119 yday $9.89 → 09:30 $9.83 -7.14; SEDG×32 yday $33.98 → 09:30 $33.69 -9.28; SMTC×7 yday $133.85 → 09:30 $133.10 -5.25; OPTX×128 yday $7.53 → 09:30 $7.59 +7.68; ERAS×56 yday $16.37 → 09:30 $16.38 +0.56; BBWI×58 yday $18.53 → 09:30 $18.59 +3.48; MMED×11 yday $23.76 → 09:30 $23.88 +1.32; CNXC×8 yday $32.37 → 09:30 $32.88 +4.08; TXG×4 yday $61.65 → 09:30 $62.35 +2.80; ZYME×9 yday $31.05 → 09:30 $31.34 +2.61; FCX×3 yday $73.93 → 09:30 $75.34 +4.23 | — |
| 2026-09-04 09:30 ET | **SELL** | `TRLV` | 22 | $11.89 | $2.08 | $+15.01 | $643.80 | ▲ +15.01 after sell → book $9,176.75; vs 09:30 mark -2.08 | dropped from list after 8 sess (min 5) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `RRC` | 26 | $42.43 | $2.09 | $+21.58 | $1,744.89 | ▲ +21.58 after sell → book $9,174.66; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `CAPR` | 119 | $9.83 | $2.38 | $+71.44 | $2,912.28 | ▲ +71.44 after sell → book $9,172.28; vs 09:30 mark -2.38 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SEDG` | 32 | $33.69 | $2.11 | $-7.07 | $3,988.26 | ▼ -7.07 after sell → book $9,170.18; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SMTC` | 7 | $133.10 | $2.03 | $-118.14 | $4,917.93 | ▼ -118.14 after sell → book $9,168.15; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 128 | $7.59 | $2.41 | $-130.22 | $5,887.04 | ▼ -130.22 after sell → book $9,165.74; vs 09:30 mark -2.41 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `ERAS` | 56 | $16.38 | $2.18 | $-167.86 | $6,802.14 | ▼ -167.86 after sell → book $9,163.56; vs 09:30 mark -2.18 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BBWI` | 58 | $18.59 | $2.18 | $-9.57 | $7,878.18 | ▼ -9.57 after sell → book $9,161.38; vs 09:30 mark -2.18 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 2020 | $1.95 | $26.06 | — | $3,913.12 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $3939.09 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `AMTX` | 2034 | $1.91 | $26.24 | — | $1.94 | — | union ∩ news_g hold 5, no 🚨; gate news=good; list ohlc_hot; ret5=+16.9; leftover $3939.09 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |

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
| 2026-09-01 | `NEM` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
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
