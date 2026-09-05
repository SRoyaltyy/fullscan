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

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | TLN, VST, NRG, ANGX, ARX, MH, HLIT | — | $1,560.49 | $10,110.67 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $1,560.49 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94 | $10,211.68 | +101.01 | DVN, EOG, FANG, CELC, OUST | — | $212.20 | $10,059.83 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6 | 09:30 open · cash $1,560.49 (unchanged overnight, no fees) · equity $10,211.68 vs prior close $10,110.67 (+101.01) because holdings re-marked: TLN×3 yday $362.74 → 09:30 $367.88 +15.42; VST×8 yday $148.13 → 09:30 $149.37 +9.92; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; MH×92 yday $13.10 → 09:30 $13.16 +5.52; HLIT×94 yday $13.92 → 09:30 $13.84 -7.52 |
| 2026-08-18 | -6.20 | $212.20 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6 | $10,014.18 | -45.65 | — | — | $212.20 | $9,819.43 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6 | 09:30 open · cash $212.20 (unchanged overnight, no fees) · equity $10,014.18 vs prior close $10,059.83 (-45.65) because holdings re-marked: TLN×3 yday $356.92 → 09:30 $350.89 -18.09; VST×8 yday $146.11 → 09:30 $144.50 -12.88; NRG×10 yday $122.37 → 09:30 $121.92 -4.50; ANGX×290 yday $4.71 → 09:30 $4.79 +23.20; ARX×63 yday $19.54 → 09:30 $19.57 +1.89; MH×92 yday $12.77 → 09:30 $13.00 +21.16; HLIT×94 yday $13.43 → 09:30 $12.93 -47.00; DVN×6 yday $47.57 → 09:30 $48.00 +2.58; EOG×2 yday $146.15 → 09:30 $148.04 +3.78; FANG×1 yday $206.29 → 09:30 $208.93 +2.64; CELC×3 yday $92.44 → 09:30 $92.38 -0.18; OUST×6 yday $48.13 → 09:30 $45.09 -18.24 |
| 2026-08-19 | -7.20 | $212.20 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6 | $9,839.65 | +20.22 | — | TLN, VST, NRG, ARX, MH, HLIT | $7,093.29 | $9,745.25 | ANGX×290, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6 | 09:30 open · cash $212.20 (unchanged overnight, no fees) · equity $9,839.65 vs prior close $9,819.43 (+20.22) because holdings re-marked: TLN×3 yday $317.66 → 09:30 $321.00 +10.02; VST×8 yday $140.52 → 09:30 $140.74 +1.76; NRG×10 yday $115.56 → 09:30 $116.20 +6.40; ANGX×290 yday $4.85 → 09:30 $4.79 -17.40; ARX×63 yday $19.56 → 09:30 $19.58 +1.26; MH×92 yday $13.12 → 09:30 $13.01 -10.12; HLIT×94 yday $12.73 → 09:30 $12.90 +15.98; DVN×6 yday $47.83 → 09:30 $48.22 +2.34; EOG×2 yday $148.70 → 09:30 $149.86 +2.32; FANG×1 yday $210.02 → 09:30 $210.84 +0.82; CELC×3 yday $93.24 → 09:30 $95.50 +6.78; OUST×6 yday $42.99 → 09:30 $43.00 +0.06 |
| 2026-08-20 | +1.12 | $7,093.29 | ANGX×290, DVN×6, EOG×2, FANG×1, CELC×3, OUST×6 | $9,751.60 | +6.35 | BHP, MRNA, HUMA, BTGO, ZLAB, CRSP, APA, AUTL | ANGX, DVN, EOG, FANG, CELC, OUST | $90.86 | $9,503.17 | BHP×13, MRNA×8, HUMA×1721, BTGO×184, ZLAB×45, CRSP×20, APA×27, AUTL×492 | 09:30 open · cash $7,093.29 (unchanged overnight, no fees) · equity $9,751.60 vs prior close $9,745.25 (+6.35) because holdings re-marked: ANGX×290 yday $4.60 → 09:30 $4.57 -8.70; DVN×6 yday $48.19 → 09:30 $49.02 +4.98; EOG×2 yday $149.48 → 09:30 $151.45 +3.94; FANG×1 yday $208.55 → 09:30 $213.51 +4.96; CELC×3 yday $93.65 → 09:30 $92.90 -2.25; OUST×6 yday $40.06 → 09:30 $40.63 +3.42 |
| 2026-08-21 | +3.25 | $90.86 | BHP×13, MRNA×8, HUMA×1721, BTGO×184, ZLAB×45, CRSP×20, APA×27, AUTL×492 | $9,631.79 | +128.62 | MARA, BTDR, HIVE | — | $54.73 | $9,593.06 | BHP×13, MRNA×8, HUMA×1721, BTGO×184, ZLAB×45, CRSP×20, APA×27, AUTL×492, MARA×1, BTDR×1, HIVE×4 | 09:30 open · cash $90.86 (unchanged overnight, no fees) · equity $9,631.79 vs prior close $9,503.17 (+128.62) because holdings re-marked: BHP×13 yday $93.63 → 09:30 $95.72 +27.17; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; HUMA×1721 yday $0.68 → 09:30 $0.67 -12.05; BTGO×184 yday $6.60 → 09:30 $6.95 +64.40; ZLAB×45 yday $26.02 → 09:30 $26.25 +10.35; CRSP×20 yday $58.12 → 09:30 $59.72 +32.00; APA×27 yday $44.39 → 09:30 $44.52 +3.51; AUTL×492 yday $2.46 → 09:30 $2.47 +4.92 |
| 2026-08-24 | -5.17 | $54.73 | BHP×13, MRNA×8, HUMA×1721, BTGO×184, ZLAB×45, CRSP×20, APA×27, AUTL×492, MARA×1, BTDR×1, HIVE×4 | $9,578.28 | -14.78 | — | — | $54.73 | $9,489.33 | BHP×13, MRNA×8, HUMA×1721, BTGO×184, ZLAB×45, CRSP×20, APA×27, AUTL×492, MARA×1, BTDR×1, HIVE×4 | 09:30 open · cash $54.73 (unchanged overnight, no fees) · equity $9,578.28 vs prior close $9,593.06 (-14.78) because holdings re-marked: BHP×13 yday $97.03 → 09:30 $97.34 +4.03; MRNA×8 yday $145.13 → 09:30 $142.70 -19.44; HUMA×1721 yday $0.64 → 09:30 $0.68 +65.40; BTGO×184 yday $6.84 → 09:30 $6.87 +5.52; ZLAB×45 yday $26.01 → 09:30 $25.59 -18.90; CRSP×20 yday $59.50 → 09:30 $58.79 -14.20; APA×27 yday $43.39 → 09:30 $42.93 -12.42; AUTL×492 yday $2.41 → 09:30 $2.36 -24.60; MARA×1 yday $11.26 → 09:30 $11.18 -0.08; BTDR×1 yday $11.37 → 09:30 $11.49 +0.12; HIVE×4 yday $3.03 → 09:30 $2.98 -0.20 |
| 2026-08-25 | +1.80 | $54.73 | BHP×13, MRNA×8, HUMA×1721, BTGO×184, ZLAB×45, CRSP×20, APA×27, AUTL×492, MARA×1, BTDR×1, HIVE×4 | $9,487.37 | -1.96 | RUM, EZPW, REAX, TRLV, VIRT, HOOD, ZYME, BKKT | BHP, MRNA, HUMA, BTGO, ZLAB, CRSP, APA, AUTL | $79.68 | $9,432.04 | MARA×1, BTDR×1, HIVE×4, RUM×125, EZPW×34, REAX×49, TRLV×106, VIRT×17, HOOD×11, ZYME×39, BKKT×142 | 09:30 open · cash $54.73 (unchanged overnight, no fees) · equity $9,487.37 vs prior close $9,489.33 (-1.96) because holdings re-marked: BHP×13 yday $96.66 → 09:30 $95.95 -9.23; MRNA×8 yday $139.27 → 09:30 $141.19 +15.36; HUMA×1721 yday $0.67 → 09:30 $0.67 +0.00; BTGO×184 yday $6.97 → 09:30 $6.89 -14.72; ZLAB×45 yday $25.51 → 09:30 $25.93 +18.90; CRSP×20 yday $56.91 → 09:30 $57.00 +1.80; APA×27 yday $42.10 → 09:30 $42.70 +16.20; AUTL×492 yday $2.38 → 09:30 $2.32 -29.52; MARA×1 yday $11.44 → 09:30 $11.28 -0.16; BTDR×1 yday $11.30 → 09:30 $11.19 -0.11; HIVE×4 yday $2.94 → 09:30 $2.82 -0.48 |
| 2026-08-26 | +2.02 | $79.68 | MARA×1, BTDR×1, HIVE×4, RUM×125, EZPW×34, REAX×49, TRLV×106, VIRT×17, HOOD×11, ZYME×39, BKKT×142 | $9,432.04 | -0.00 | — | — | $79.68 | $9,433.49 | MARA×1, BTDR×1, HIVE×4, RUM×125, EZPW×34, REAX×49, TRLV×106, VIRT×17, HOOD×11, ZYME×39, BKKT×142 | 09:30 open · cash $79.68 (unchanged overnight, no fees) · equity $9,432.04 vs prior close $9,432.04 (-0.00) because holdings re-marked: MARA×1 yday $11.29 → 09:30 $11.29 +0.00; BTDR×1 yday $11.28 → 09:30 $11.28 +0.00; HIVE×4 yday $2.89 → 09:30 $2.89 +0.00; RUM×125 yday $9.35 → 09:30 $9.35 +0.00; EZPW×34 yday $34.69 → 09:30 $34.69 +0.00; REAX×49 yday $24.00 → 09:30 $24.00 +0.00; TRLV×106 yday $11.02 → 09:30 $11.02 +0.00; VIRT×17 yday $66.29 → 09:30 $66.29 +0.00; HOOD×11 yday $104.22 → 09:30 $104.22 +0.00; ZYME×39 yday $29.81 → 09:30 $29.81 +0.00; BKKT×142 yday $8.38 → 09:30 $8.38 +0.00 |
| 2026-08-27 | — | $79.68 | MARA×1, BTDR×1, HIVE×4, RUM×125, EZPW×34, REAX×49, TRLV×106, VIRT×17, HOOD×11, ZYME×39, BKKT×142 | $9,659.50 | +226.01 | — | MARA, BTDR, HIVE | $113.66 | $9,576.52 | RUM×125, EZPW×34, REAX×49, TRLV×106, VIRT×17, HOOD×11, ZYME×39, BKKT×142 | 09:30 open · cash $79.68 (unchanged overnight, no fees) · equity $9,659.50 vs prior close $9,433.49 (+226.01) because holdings re-marked: MARA×1 yday $11.29 → 09:30 $11.56 +0.27; BTDR×1 yday $11.28 → 09:30 $11.05 -0.23; HIVE×4 yday $2.89 → 09:30 $2.95 +0.24; RUM×125 yday $9.35 → 09:30 $10.07 +90.00; EZPW×34 yday $34.69 → 09:30 $35.70 +34.34; REAX×49 yday $24.00 → 09:30 $26.61 +127.89; TRLV×106 yday $11.02 → 09:30 $11.22 +21.20; VIRT×17 yday $66.29 → 09:30 $64.92 -23.29; HOOD×11 yday $104.22 → 09:30 $110.11 +64.79; ZYME×39 yday $29.81 → 09:30 $27.56 -87.75; BKKT×142 yday $8.38 → 09:30 $8.38 +0.00 |
| 2026-08-28 | +0.75 | $113.66 | RUM×125, EZPW×34, REAX×49, TRLV×106, VIRT×17, HOOD×11, ZYME×39, BKKT×142 | $9,597.99 | +21.47 | RRC, CAPR, SEDG, SMTC, OPTX, ERAS, BBWI | RUM, EZPW, REAX, TRLV, VIRT, HOOD, BKKT | $48.16 | $9,641.44 | ZYME×39, RRC×29, CAPR×131, SEDG×35, SMTC×8, OPTX×140, ERAS×62, BBWI×64 | 09:30 open · cash $113.66 (unchanged overnight, no fees) · equity $9,597.99 vs prior close $9,576.52 (+21.47) because holdings re-marked: RUM×125 yday $9.38 → 09:30 $9.51 +16.25; EZPW×34 yday $33.90 → 09:30 $33.50 -13.60; REAX×49 yday $26.59 → 09:30 $25.91 -33.32; TRLV×106 yday $11.43 → 09:30 $11.38 -5.30; VIRT×17 yday $65.74 → 09:30 $65.42 -5.44; HOOD×11 yday $108.54 → 09:30 $110.70 +23.76; ZYME×39 yday $29.31 → 09:30 $29.33 +0.78; BKKT×142 yday $8.23 → 09:30 $8.50 +38.34 |
| 2026-08-31 | -5.85 | $48.16 | ZYME×39, RRC×29, CAPR×131, SEDG×35, SMTC×8, OPTX×140, ERAS×62, BBWI×64 | $9,284.14 | -357.30 | — | — | $48.16 | $9,275.92 | ZYME×39, RRC×29, CAPR×131, SEDG×35, SMTC×8, OPTX×140, ERAS×62, BBWI×64 | 09:30 open · cash $48.16 (unchanged overnight, no fees) · equity $9,284.14 vs prior close $9,641.44 (-357.30) because holdings re-marked: ZYME×39 yday $29.01 → 09:30 $28.27 -28.86; RRC×29 yday $41.64 → 09:30 $41.11 -15.37; CAPR×131 yday $10.06 → 09:30 $9.44 -81.22; SEDG×35 yday $33.51 → 09:30 $31.50 -70.35; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; OPTX×140 yday $8.73 → 09:30 $8.52 -29.40; ERAS×62 yday $19.49 → 09:30 $17.90 -98.58; BBWI×64 yday $18.65 → 09:30 $19.30 +41.60 |
| 2026-09-01 | -6.30 | $48.16 | ZYME×39, RRC×29, CAPR×131, SEDG×35, SMTC×8, OPTX×140, ERAS×62, BBWI×64 | $9,422.15 | +146.23 | — | ZYME | $1,189.52 | $9,338.09 | RRC×29, CAPR×131, SEDG×35, SMTC×8, OPTX×140, ERAS×62, BBWI×64 | 09:30 open · cash $48.16 (unchanged overnight, no fees) · equity $9,422.15 vs prior close $9,275.92 (+146.23) because holdings re-marked: ZYME×39 yday $28.27 → 09:30 $29.32 +40.95; RRC×29 yday $41.78 → 09:30 $41.32 -13.34; CAPR×131 yday $9.36 → 09:30 $10.43 +140.17; SEDG×35 yday $31.27 → 09:30 $32.22 +33.25; SMTC×8 yday $132.54 → 09:30 $131.65 -7.12; OPTX×140 yday $8.52 → 09:30 $8.19 -46.20; ERAS×62 yday $17.90 → 09:30 $18.00 +6.20; BBWI×64 yday $19.22 → 09:30 $19.10 -7.68 |
| 2026-09-02 | -3.83 | $1,189.52 | RRC×29, CAPR×131, SEDG×35, SMTC×8, OPTX×140, ERAS×62, BBWI×64 | $9,355.98 | +17.89 | — | RRC, CAPR, SEDG, SMTC, OPTX, ERAS, BBWI | $9,340.47 | $9,340.47 | — | 09:30 open · cash $1,189.52 (unchanged overnight, no fees) · equity $9,355.98 vs prior close $9,338.09 (+17.89) because holdings re-marked: RRC×29 yday $41.32 → 09:30 $41.94 +17.98; CAPR×131 yday $10.19 → 09:30 $10.77 +75.98; SEDG×35 yday $31.80 → 09:30 $31.87 +2.45; SMTC×8 yday $129.50 → 09:30 $127.63 -14.96; OPTX×140 yday $8.19 → 09:30 $7.94 -35.00; ERAS×62 yday $17.70 → 09:30 $17.58 -7.44; BBWI×64 yday $19.10 → 09:30 $18.77 -21.12 |
| 2026-09-03 | -0.90 | $9,340.47 | — | $9,340.47 | +0.00 | MMED, CNXC, OPTX, TRLV, TXG, ZYME, FCX, AVGO | — | $194.04 | $9,502.68 | MMED×51, CNXC×36, OPTX×161, TRLV×99, TXG×19, ZYME×38, FCX×15, AVGO×3 | 09:30 open · cash $9,340.47 · no holdings · equity $9,340.47 vs prior close $9,340.47 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $194.04 | MMED×51, CNXC×36, OPTX×161, TRLV×99, TXG×19, ZYME×38, FCX×15, AVGO×3 | $9,555.59 | +52.91 | BAK, AMTX | — | $0.78 | $9,519.41 | MMED×51, CNXC×36, OPTX×161, TRLV×99, TXG×19, ZYME×38, FCX×15, AVGO×3, BAK×49, AMTX×50 | 09:30 open · cash $194.04 (unchanged overnight, no fees) · equity $9,555.59 vs prior close $9,502.68 (+52.91) because holdings re-marked: MMED×51 yday $23.76 → 09:30 $23.88 +6.12; CNXC×36 yday $32.37 → 09:30 $32.88 +18.36; OPTX×161 yday $7.53 → 09:30 $7.59 +9.66; TRLV×99 yday $11.69 → 09:30 $11.89 +19.80; TXG×19 yday $61.65 → 09:30 $62.35 +13.30; ZYME×38 yday $31.05 → 09:30 $31.34 +11.02; FCX×15 yday $73.93 → 09:30 $75.34 +21.15; AVGO×3 yday $367.24 → 09:30 $351.74 -46.50 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $8,918.51 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,741.30 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+3.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,539.28 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $5,285.64 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $4,050.55 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $2,801.68 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $1,560.49 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,560.49 | ▲ 09:30 equity $10,211.68 vs yday $10,110.67 (+101.01) | 09:30 open · cash $1,560.49 (unchanged overnight, no fees) · equity $10,211.68 vs prior close $10,110.67 (+101.01) because holdings re-marked: TLN×3 yday $362.74 → 09:30 $367.88 +15.42; VST×8 yday $148.13 → 09:30 $149.37 +9.92; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; MH×92 yday $13.10 → 09:30 $13.16 +5.52; HLIT×94 yday $13.92 → 09:30 $13.84 -7.52 | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 6 | $46.18 | $2.01 | — | $1,281.40 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+6.7; leftover $312.10 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 2 | $142.77 | $2.00 | — | $993.87 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.8; leftover $312.10 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 1 | $202.70 | $1.99 | — | $789.17 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+8.3; leftover $312.10 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 3 | $92.99 | $2.00 | — | $508.20 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.8; leftover $312.10 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 6 | $49.00 | $2.01 | — | $212.20 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ⚪; ret5=+12.2; leftover $312.10 | join🟡 sector🟢 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $212.20 | ▼ 09:30 equity $10,014.18 vs yday $10,059.83 (-45.65) | 09:30 open · cash $212.20 (unchanged overnight, no fees) · equity $10,014.18 vs prior close $10,059.83 (-45.65) because holdings re-marked: TLN×3 yday $356.92 → 09:30 $350.89 -18.09; VST×8 yday $146.11 → 09:30 $144.50 -12.88; NRG×10 yday $122.37 → 09:30 $121.92 -4.50; ANGX×290 yday $4.71 → 09:30 $4.79 +23.20; ARX×63 yday $19.54 → 09:30 $19.57 +1.89; MH×92 yday $12.77 → 09:30 $13.00 +21.16; HLIT×94 yday $13.43 → 09:30 $12.93 -47.00; DVN×6 yday $47.57 → 09:30 $48.00 +2.58; EOG×2 yday $146.15 → 09:30 $148.04 +3.78; FANG×1 yday $206.29 → 09:30 $208.93 +2.64; CELC×3 yday $92.44 → 09:30 $92.38 -0.18; OUST×6 yday $48.13 → 09:30 $45.09 -18.24 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $212.20 | ▲ 09:30 equity $9,839.65 vs yday $9,819.43 (+20.22) | 09:30 open · cash $212.20 (unchanged overnight, no fees) · equity $9,839.65 vs prior close $9,819.43 (+20.22) because holdings re-marked: TLN×3 yday $317.66 → 09:30 $321.00 +10.02; VST×8 yday $140.52 → 09:30 $140.74 +1.76; NRG×10 yday $115.56 → 09:30 $116.20 +6.40; ANGX×290 yday $4.85 → 09:30 $4.79 -17.40; ARX×63 yday $19.56 → 09:30 $19.58 +1.26; MH×92 yday $13.12 → 09:30 $13.01 -10.12; HLIT×94 yday $12.73 → 09:30 $12.90 +15.98; DVN×6 yday $47.83 → 09:30 $48.22 +2.34; EOG×2 yday $148.70 → 09:30 $149.86 +2.32; FANG×1 yday $210.02 → 09:30 $210.84 +0.82; CELC×3 yday $93.24 → 09:30 $95.50 +6.78; OUST×6 yday $42.99 → 09:30 $43.00 +0.06 | — |
| 2026-08-19 09:30 ET | **SELL** | `TLN` | 3 | $321.00 | $2.02 | $-120.51 | $1,173.18 | ▼ -120.51 after sell → book $9,837.64; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `VST` | 8 | $140.74 | $2.03 | $-53.33 | $2,297.06 | ▼ -53.33 after sell → book $9,835.60; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NRG` | 10 | $116.20 | $2.04 | $-42.06 | $3,457.02 | ▼ -42.06 after sell → book $9,833.56; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 63 | $19.58 | $2.20 | $-3.75 | $4,688.36 | ▼ -3.75 after sell → book $9,831.36; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | join🟡 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `MH` | 92 | $13.01 | $2.29 | $-54.24 | $5,882.99 | ▼ -54.24 after sell → book $9,829.07; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `HLIT` | 94 | $12.90 | $2.30 | $-30.89 | $7,093.29 | ▼ -30.89 after sell → book $9,826.77; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,093.29 | ▲ 09:30 equity $9,751.60 vs yday $9,745.25 (+6.35) | 09:30 open · cash $7,093.29 (unchanged overnight, no fees) · equity $9,751.60 vs prior close $9,745.25 (+6.35) because holdings re-marked: ANGX×290 yday $4.60 → 09:30 $4.57 -8.70; DVN×6 yday $48.19 → 09:30 $49.02 +4.98; EOG×2 yday $149.48 → 09:30 $151.45 +3.94; FANG×1 yday $208.55 → 09:30 $213.51 +4.96; CELC×3 yday $93.65 → 09:30 $92.90 -2.25; OUST×6 yday $40.06 → 09:30 $40.63 +3.42 | — |
| 2026-08-20 09:30 ET | **SELL** | `ANGX` | 290 | $4.57 | $3.80 | $+67.86 | $8,414.79 | ▲ +67.86 after sell → book $9,747.80; vs 09:30 mark -3.80 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DVN` | 6 | $49.02 | $2.03 | $+13.00 | $8,706.89 | ▲ +13.00 after sell → book $9,745.78; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `EOG` | 2 | $151.45 | $2.02 | $+13.35 | $9,007.77 | ▲ +13.35 after sell → book $9,743.76; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `FANG` | 1 | $213.51 | $2.01 | $+6.80 | $9,219.27 | ▲ +6.80 after sell → book $9,741.75; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `CELC` | 3 | $92.90 | $2.02 | $-4.29 | $9,495.95 | ▼ -4.29 after sell → book $9,739.73; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `OUST` | 6 | $40.63 | $2.03 | $-54.26 | $9,737.70 | ▼ -54.26 after sell → book $9,737.70; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,552.54 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1217.21 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $7,349.41 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1217.21 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1721 | $0.71 | $17.33 | — | $6,115.33 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1217.21 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 184 | $6.61 | $2.54 | — | $4,897.47 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1217.21 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 45 | $26.57 | $2.12 | — | $3,699.69 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.8; leftover $1217.21 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 20 | $58.73 | $2.05 | — | $2,523.04 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1217.21 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 27 | $44.76 | $2.07 | — | $1,312.45 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+8.7; leftover $1217.21 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 492 | $2.47 | $6.35 | — | $90.86 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1217.21 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.86 | ▲ 09:30 equity $9,631.79 vs yday $9,503.17 (+128.62) | 09:30 open · cash $90.86 (unchanged overnight, no fees) · equity $9,631.79 vs prior close $9,503.17 (+128.62) because holdings re-marked: BHP×13 yday $93.63 → 09:30 $95.72 +27.17; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; HUMA×1721 yday $0.68 → 09:30 $0.67 -12.05; BTGO×184 yday $6.60 → 09:30 $6.95 +64.40; ZLAB×45 yday $26.02 → 09:30 $26.25 +10.35; CRSP×20 yday $58.12 → 09:30 $59.72 +32.00; APA×27 yday $44.39 → 09:30 $44.52 +3.51; AUTL×492 yday $2.46 → 09:30 $2.47 +4.92 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 1 | $11.70 | $0.12 | — | $79.04 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $15.14 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 1 | $11.10 | $0.11 | — | $67.84 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+19.1; leftover $15.14 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 4 | $3.24 | $0.14 | — | $54.73 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; leftover $15.14 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.73 | ▼ 09:30 equity $9,578.28 vs yday $9,593.06 (-14.78) | 09:30 open · cash $54.73 (unchanged overnight, no fees) · equity $9,578.28 vs prior close $9,593.06 (-14.78) because holdings re-marked: BHP×13 yday $97.03 → 09:30 $97.34 +4.03; MRNA×8 yday $145.13 → 09:30 $142.70 -19.44; HUMA×1721 yday $0.64 → 09:30 $0.68 +65.40; BTGO×184 yday $6.84 → 09:30 $6.87 +5.52; ZLAB×45 yday $26.01 → 09:30 $25.59 -18.90; CRSP×20 yday $59.50 → 09:30 $58.79 -14.20; APA×27 yday $43.39 → 09:30 $42.93 -12.42; AUTL×492 yday $2.41 → 09:30 $2.36 -24.60; MARA×1 yday $11.26 → 09:30 $11.18 -0.08; BTDR×1 yday $11.37 → 09:30 $11.49 +0.12; HIVE×4 yday $3.03 → 09:30 $2.98 -0.20 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.73 | ▼ 09:30 equity $9,487.37 vs yday $9,489.33 (-1.96) | 09:30 open · cash $54.73 (unchanged overnight, no fees) · equity $9,487.37 vs prior close $9,489.33 (-1.96) because holdings re-marked: BHP×13 yday $96.66 → 09:30 $95.95 -9.23; MRNA×8 yday $139.27 → 09:30 $141.19 +15.36; HUMA×1721 yday $0.67 → 09:30 $0.67 +0.00; BTGO×184 yday $6.97 → 09:30 $6.89 -14.72; ZLAB×45 yday $25.51 → 09:30 $25.93 +18.90; CRSP×20 yday $56.91 → 09:30 $57.00 +1.80; APA×27 yday $42.10 → 09:30 $42.70 +16.20; AUTL×492 yday $2.38 → 09:30 $2.32 -29.52; MARA×1 yday $11.44 → 09:30 $11.28 -0.16; BTDR×1 yday $11.30 → 09:30 $11.19 -0.11; HIVE×4 yday $2.94 → 09:30 $2.82 -0.48 | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.95 | $2.05 | $+60.14 | $1,300.04 | ▲ +60.14 after sell → book $9,485.33; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $141.19 | $2.03 | $-75.65 | $2,427.52 | ▼ -75.65 after sell → book $9,483.29; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HUMA` | 1721 | $0.67 | $16.99 | $-98.00 | $3,563.60 | ▼ -98.00 after sell → book $9,466.30; vs 09:30 mark -16.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BTGO` | 184 | $6.89 | $2.58 | $+47.32 | $4,828.78 | ▲ +47.32 after sell → book $9,463.72; vs 09:30 mark -2.58 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ZLAB` | 45 | $25.93 | $2.15 | $-33.07 | $5,993.48 | ▼ -33.07 after sell → book $9,461.57; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 20 | $57.00 | $2.07 | $-38.72 | $7,131.41 | ▼ -38.72 after sell → book $9,459.50; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 27 | $42.70 | $2.09 | $-59.78 | $8,282.22 | ▼ -59.78 after sell → book $9,457.41; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 492 | $2.32 | $6.44 | $-86.59 | $9,417.22 | ▼ -86.59 after sell → book $9,450.97; vs 09:30 mark -6.44 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 125 | $9.36 | $2.37 | — | $8,244.86 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; leftover $1177.15 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 34 | $34.48 | $2.09 | — | $7,070.45 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1177.15 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 49 | $24.00 | $2.14 | — | $5,892.31 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=+10.0; leftover $1177.15 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `TRLV` | 106 | $11.02 | $2.31 | — | $4,721.88 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.0; leftover $1177.15 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VIRT` | 17 | $66.29 | $2.04 | — | $3,592.91 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+13.2; leftover $1177.15 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HOOD` | 11 | $106.00 | $2.02 | — | $2,424.89 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ⚪; ret5=+13.2; leftover $1177.15 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 39 | $29.87 | $2.11 | — | $1,257.85 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+14.1; leftover $1177.15 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BKKT` | 142 | $8.28 | $2.42 | — | $79.68 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+12.3; leftover $1177.15 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.68 | ▲ 09:30 equity $9,432.04 vs yday $9,432.04 (-0.00) | 09:30 open · cash $79.68 (unchanged overnight, no fees) · equity $9,432.04 vs prior close $9,432.04 (-0.00) because holdings re-marked: MARA×1 yday $11.29 → 09:30 $11.29 +0.00; BTDR×1 yday $11.28 → 09:30 $11.28 +0.00; HIVE×4 yday $2.89 → 09:30 $2.89 +0.00; RUM×125 yday $9.35 → 09:30 $9.35 +0.00; EZPW×34 yday $34.69 → 09:30 $34.69 +0.00; REAX×49 yday $24.00 → 09:30 $24.00 +0.00; TRLV×106 yday $11.02 → 09:30 $11.02 +0.00; VIRT×17 yday $66.29 → 09:30 $66.29 +0.00; HOOD×11 yday $104.22 → 09:30 $104.22 +0.00; ZYME×39 yday $29.81 → 09:30 $29.81 +0.00; BKKT×142 yday $8.38 → 09:30 $8.38 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.68 | ▲ 09:30 equity $9,659.50 vs yday $9,433.49 (+226.01) | 09:30 open · cash $79.68 (unchanged overnight, no fees) · equity $9,659.50 vs prior close $9,433.49 (+226.01) because holdings re-marked: MARA×1 yday $11.29 → 09:30 $11.56 +0.27; BTDR×1 yday $11.28 → 09:30 $11.05 -0.23; HIVE×4 yday $2.89 → 09:30 $2.95 +0.24; RUM×125 yday $9.35 → 09:30 $10.07 +90.00; EZPW×34 yday $34.69 → 09:30 $35.70 +34.34; REAX×49 yday $24.00 → 09:30 $26.61 +127.89; TRLV×106 yday $11.02 → 09:30 $11.22 +21.20; VIRT×17 yday $66.29 → 09:30 $64.92 -23.29; HOOD×11 yday $104.22 → 09:30 $110.11 +64.79; ZYME×39 yday $29.81 → 09:30 $27.56 -87.75; BKKT×142 yday $8.38 → 09:30 $8.38 +0.00 | — |
| 2026-08-27 09:30 ET | **SELL** | `MARA` | 1 | $11.56 | $0.14 | $-0.40 | $91.10 | ▼ -0.40 after sell → book $9,659.36; vs 09:30 mark -0.14 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `BTDR` | 1 | $11.05 | $0.13 | $-0.29 | $102.01 | ▼ -0.29 after sell → book $9,659.22; vs 09:30 mark -0.14 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `HIVE` | 4 | $2.95 | $0.15 | $-1.45 | $113.66 | ▼ -1.45 after sell → book $9,659.07; vs 09:30 mark -0.15 | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.66 | ▲ 09:30 equity $9,597.99 vs yday $9,576.52 (+21.47) | 09:30 open · cash $113.66 (unchanged overnight, no fees) · equity $9,597.99 vs prior close $9,576.52 (+21.47) because holdings re-marked: RUM×125 yday $9.38 → 09:30 $9.51 +16.25; EZPW×34 yday $33.90 → 09:30 $33.50 -13.60; REAX×49 yday $26.59 → 09:30 $25.91 -33.32; TRLV×106 yday $11.43 → 09:30 $11.38 -5.30; VIRT×17 yday $65.74 → 09:30 $65.42 -5.44; HOOD×11 yday $108.54 → 09:30 $110.70 +23.76; ZYME×39 yday $29.31 → 09:30 $29.33 +0.78; BKKT×142 yday $8.23 → 09:30 $8.50 +38.34 | — |
| 2026-08-28 09:30 ET | **SELL** | `RUM` | 125 | $9.51 | $2.40 | $+13.99 | $1,300.02 | ▲ +13.99 after sell → book $9,595.60; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 34 | $33.50 | $2.11 | $-37.52 | $2,436.91 | ▼ -37.52 after sell → book $9,593.49; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `REAX` | 49 | $25.91 | $2.16 | $+89.30 | $3,704.34 | ▲ +89.30 after sell → book $9,591.33; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `TRLV` | 106 | $11.38 | $2.34 | $+33.52 | $4,908.28 | ▲ +33.52 after sell → book $9,588.99; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `VIRT` | 17 | $65.42 | $2.06 | $-18.89 | $6,018.36 | ▼ -18.89 after sell → book $9,586.93; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `HOOD` | 11 | $110.70 | $2.04 | $+47.63 | $7,234.02 | ▲ +47.63 after sell → book $9,584.89; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `BKKT` | 142 | $8.50 | $2.45 | $+26.37 | $8,438.57 | ▲ +26.37 after sell → book $9,582.44; vs 09:30 mark -2.45 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 29 | $41.44 | $2.08 | — | $7,234.73 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; ret5=+1.8; leftover $1205.51 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 131 | $9.19 | $2.38 | — | $6,028.46 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1205.51 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 35 | $33.78 | $2.10 | — | $4,844.06 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1205.51 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $3,646.85 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1205.51 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 140 | $8.57 | $2.41 | — | $2,444.64 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-3.4; leftover $1205.51 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 62 | $19.30 | $2.18 | — | $1,245.86 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-4.1; leftover $1205.51 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 64 | $18.68 | $2.18 | — | $48.16 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+0.2; leftover $1205.51 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.16 | ▼ 09:30 equity $9,284.14 vs yday $9,641.44 (-357.30) | 09:30 open · cash $48.16 (unchanged overnight, no fees) · equity $9,284.14 vs prior close $9,641.44 (-357.30) because holdings re-marked: ZYME×39 yday $29.01 → 09:30 $28.27 -28.86; RRC×29 yday $41.64 → 09:30 $41.11 -15.37; CAPR×131 yday $10.06 → 09:30 $9.44 -81.22; SEDG×35 yday $33.51 → 09:30 $31.50 -70.35; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; OPTX×140 yday $8.73 → 09:30 $8.52 -29.40; ERAS×62 yday $19.49 → 09:30 $17.90 -98.58; BBWI×64 yday $18.65 → 09:30 $19.30 +41.60 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.16 | ▲ 09:30 equity $9,422.15 vs yday $9,275.92 (+146.23) | 09:30 open · cash $48.16 (unchanged overnight, no fees) · equity $9,422.15 vs prior close $9,275.92 (+146.23) because holdings re-marked: ZYME×39 yday $28.27 → 09:30 $29.32 +40.95; RRC×29 yday $41.78 → 09:30 $41.32 -13.34; CAPR×131 yday $9.36 → 09:30 $10.43 +140.17; SEDG×35 yday $31.27 → 09:30 $32.22 +33.25; SMTC×8 yday $132.54 → 09:30 $131.65 -7.12; OPTX×140 yday $8.52 → 09:30 $8.19 -46.20; ERAS×62 yday $17.90 → 09:30 $18.00 +6.20; BBWI×64 yday $19.22 → 09:30 $19.10 -7.68 | — |
| 2026-09-01 09:30 ET | **SELL** | `ZYME` | 39 | $29.32 | $2.13 | $-25.68 | $1,189.52 | ▼ -25.68 after sell → book $9,420.03; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,189.52 | ▲ 09:30 equity $9,355.98 vs yday $9,338.09 (+17.89) | 09:30 open · cash $1,189.52 (unchanged overnight, no fees) · equity $9,355.98 vs prior close $9,338.09 (+17.89) because holdings re-marked: RRC×29 yday $41.32 → 09:30 $41.94 +17.98; CAPR×131 yday $10.19 → 09:30 $10.77 +75.98; SEDG×35 yday $31.80 → 09:30 $31.87 +2.45; SMTC×8 yday $129.50 → 09:30 $127.63 -14.96; OPTX×140 yday $8.19 → 09:30 $7.94 -35.00; ERAS×62 yday $17.70 → 09:30 $17.58 -7.44; BBWI×64 yday $19.10 → 09:30 $18.77 -21.12 | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 29 | $41.94 | $2.10 | $+10.33 | $2,403.68 | ▲ +10.33 after sell → book $9,353.88; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CAPR` | 131 | $10.77 | $2.42 | $+202.18 | $3,812.13 | ▲ +202.18 after sell → book $9,351.46; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 35 | $31.87 | $2.12 | $-71.06 | $4,925.47 | ▼ -71.06 after sell → book $9,349.35; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $127.63 | $2.03 | $-178.21 | $5,944.47 | ▼ -178.21 after sell → book $9,347.31; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `OPTX` | 140 | $7.94 | $2.44 | $-93.05 | $7,053.63 | ▼ -93.05 after sell → book $9,344.87; vs 09:30 mark -2.44 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ERAS` | 62 | $17.58 | $2.20 | $-111.01 | $8,141.39 | ▼ -111.01 after sell → book $9,342.67; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBWI` | 64 | $18.77 | $2.20 | $+1.38 | $9,340.47 | ▲ +1.38 after sell → book $9,340.47; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,340.47 | ▲ 09:30 equity $9,340.47 vs yday $9,340.47 (+0.00) | 09:30 open · cash $9,340.47 · no holdings · equity $9,340.47 vs prior close $9,340.47 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 51 | $22.78 | $2.14 | — | $8,176.55 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1167.56 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 36 | $31.80 | $2.10 | — | $7,029.65 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+3.7; leftover $1167.56 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 161 | $7.25 | $2.47 | — | $5,859.93 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-3.4; leftover $1167.56 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `TRLV` | 99 | $11.78 | $2.29 | — | $4,691.42 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.0; leftover $1167.56 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `TXG` | 19 | $60.24 | $2.05 | — | $3,544.81 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+16.1; leftover $1167.56 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ZYME` | 38 | $30.00 | $2.10 | — | $2,402.71 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ⚪; ret5=+14.1; leftover $1167.56 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟡 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FCX` | 15 | $73.04 | $2.04 | — | $1,305.07 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.3; leftover $1167.56 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $369.68 | $2.00 | — | $194.04 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-6.2; leftover $1167.56 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $194.04 | ▲ 09:30 equity $9,555.59 vs yday $9,502.68 (+52.91) | 09:30 open · cash $194.04 (unchanged overnight, no fees) · equity $9,555.59 vs prior close $9,502.68 (+52.91) because holdings re-marked: MMED×51 yday $23.76 → 09:30 $23.88 +6.12; CNXC×36 yday $32.37 → 09:30 $32.88 +18.36; OPTX×161 yday $7.53 → 09:30 $7.59 +9.66; TRLV×99 yday $11.69 → 09:30 $11.89 +19.80; TXG×19 yday $61.65 → 09:30 $62.35 +13.30; ZYME×38 yday $31.05 → 09:30 $31.34 +11.02; FCX×15 yday $73.93 → 09:30 $75.34 +21.15; AVGO×3 yday $367.24 → 09:30 $351.74 -46.50 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 49 | $1.95 | $1.10 | — | $97.38 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $97.02 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `AMTX` | 50 | $1.91 | $1.10 | — | $0.78 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+16.9; leftover $97.02 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |

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
