# Factor mine action — `ohlc_hot_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `ohlc_hot` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **+1.06%** ($10,106) · signal-only (no cash/fees) was -2.36%. Starts YES **10/17**. Fills 64 · skips 88 · realized $+47.67.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `ohlc_hot` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $6.20.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | ADUR, ANRO, LIFE, VOYG, LUNR, BETA, FORM, ENTG | — | $250.70 | $9,881.56 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $250.70 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7 | $9,917.58 | +36.02 | OCC, ALM, LPTH, CLYM, BORR, IOVA | — | $69.50 | $10,176.53 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7, OCC×1, ALM×2, LPTH×2, CLYM×2, BORR×7, IOVA×5 | 09:30 open · cash $250.70 (unchanged overnight, no fees) · equity $9,917.58 vs prior close $9,881.56 (+36.02) because holdings re-marked: ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; ANRO×39 yday $32.14 → 09:30 $32.15 +0.39; LIFE×35 yday $34.02 → 09:30 $34.03 +0.35; VOYG×28 yday $42.98 → 09:30 $42.12 -24.08; LUNR×65 yday $19.01 → 09:30 $20.25 +80.60; BETA×49 yday $24.86 → 09:30 $24.61 -12.25; FORM×9 yday $131.60 → 09:30 $134.05 +22.05; ENTG×7 yday $161.76 → 09:30 $162.04 +1.96 |
| 2026-08-18 | -6.20 | $69.50 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7, OCC×1, ALM×2, LPTH×2, CLYM×2, BORR×7, IOVA×5 | $9,776.58 | -399.95 | — | — | $69.50 | $9,860.01 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7, OCC×1, ALM×2, LPTH×2, CLYM×2, BORR×7, IOVA×5 | 09:30 open · cash $69.50 (unchanged overnight, no fees) · equity $9,776.58 vs prior close $10,176.53 (-399.95) because holdings re-marked: ADUR×75 yday $15.85 → 09:30 $15.41 -33.00; ANRO×39 yday $33.60 → 09:30 $33.18 -16.38; LIFE×35 yday $35.17 → 09:30 $34.06 -38.85; VOYG×28 yday $43.98 → 09:30 $41.83 -60.20; LUNR×65 yday $20.38 → 09:30 $19.31 -69.55; BETA×49 yday $25.60 → 09:30 $24.99 -29.89; FORM×9 yday $138.16 → 09:30 $129.28 -79.92; ENTG×7 yday $163.09 → 09:30 $153.47 -67.34; OCC×1 yday $17.12 → 09:30 $16.20 -0.92; ALM×2 yday $16.36 → 09:30 $15.78 -1.16; LPTH×2 yday $14.80 → 09:30 $14.01 -1.58; CLYM×2 yday $17.44 → 09:30 $16.90 -1.08; BORR×7 yday $4.50 → 09:30 $4.56 +0.42; IOVA×5 yday $7.10 → 09:30 $7.00 -0.50 |
| 2026-08-19 | -7.20 | $69.50 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7, OCC×1, ALM×2, LPTH×2, CLYM×2, BORR×7, IOVA×5 | $9,914.71 | +54.70 | — | ADUR, ANRO, LIFE, VOYG, LUNR, BETA, FORM, ENTG | $9,717.04 | $9,895.67 | OCC×1, ALM×2, LPTH×2, CLYM×2, BORR×7, IOVA×5 | 09:30 open · cash $69.50 (unchanged overnight, no fees) · equity $9,914.71 vs prior close $9,860.01 (+54.70) because holdings re-marked: ADUR×75 yday $15.63 → 09:30 $15.65 +1.50; ANRO×39 yday $34.13 → 09:30 $35.00 +33.93; LIFE×35 yday $34.01 → 09:30 $34.37 +12.60; VOYG×28 yday $42.24 → 09:30 $41.93 -8.68; LUNR×65 yday $19.31 → 09:30 $18.98 -21.45; BETA×49 yday $26.76 → 09:30 $26.80 +1.96; FORM×9 yday $124.34 → 09:30 $126.03 +15.21; ENTG×7 yday $150.27 → 09:30 $152.52 +15.75; OCC×1 yday $16.20 → 09:30 $16.21 +0.01; ALM×2 yday $15.60 → 09:30 $16.05 +0.90; LPTH×2 yday $14.22 → 09:30 $14.30 +0.16; CLYM×2 yday $17.39 → 09:30 $18.09 +1.40; BORR×7 yday $4.43 → 09:30 $4.51 +0.56; IOVA×5 yday $7.03 → 09:30 $7.20 +0.85 |
| 2026-08-20 | +1.12 | $9,717.04 | OCC×1, ALM×2, LPTH×2, CLYM×2, BORR×7, IOVA×5 | $9,894.83 | -0.84 | AEM, TWST, ABTC, HL, SBET, PPC, ABCL, SENS | OCC, ALM, LPTH, CLYM, BORR, IOVA | $31.30 | $9,944.36 | AEM×6, TWST×9, ABTC×146, HL×61, SBET×163, PPC×40, ABCL×104, SENS×138 | 09:30 open · cash $9,717.04 (unchanged overnight, no fees) · equity $9,894.83 vs prior close $9,895.67 (-0.84) because holdings re-marked: OCC×1 yday $14.36 → 09:30 $14.10 -0.26; ALM×2 yday $16.18 → 09:30 $15.81 -0.74; LPTH×2 yday $13.24 → 09:30 $13.09 -0.30; CLYM×2 yday $17.34 → 09:30 $17.16 -0.36; BORR×7 yday $4.40 → 09:30 $4.46 +0.42; IOVA×5 yday $7.99 → 09:30 $8.07 +0.40 |
| 2026-08-21 | +3.25 | $31.30 | AEM×6, TWST×9, ABTC×146, HL×61, SBET×163, PPC×40, ABCL×104, SENS×138 | $10,146.87 | +202.51 | ORBS, TRON, XHG | — | $17.58 | $10,156.25 | AEM×6, TWST×9, ABTC×146, HL×61, SBET×163, PPC×40, ABCL×104, SENS×138, ORBS×6, TRON×2, XHG×1 | 09:30 open · cash $31.30 (unchanged overnight, no fees) · equity $10,146.87 vs prior close $9,944.36 (+202.51) because holdings re-marked: AEM×6 yday $212.04 → 09:30 $216.30 +25.56; TWST×9 yday $136.33 → 09:30 $138.43 +18.90; ABTC×146 yday $8.47 → 09:30 $8.66 +27.74; HL×61 yday $20.82 → 09:30 $21.33 +31.11; SBET×163 yday $7.59 → 09:30 $7.87 +45.64; PPC×40 yday $31.24 → 09:30 $31.13 -4.40; ABCL×104 yday $11.57 → 09:30 $11.57 +0.00; SENS×138 yday $8.82 → 09:30 $9.24 +57.96 |
| 2026-08-24 | -5.17 | $17.58 | AEM×6, TWST×9, ABTC×146, HL×61, SBET×163, PPC×40, ABCL×104, SENS×138, ORBS×6, TRON×2, XHG×1 | $10,172.18 | +15.93 | — | — | $17.58 | $10,134.65 | AEM×6, TWST×9, ABTC×146, HL×61, SBET×163, PPC×40, ABCL×104, SENS×138, ORBS×6, TRON×2, XHG×1 | 09:30 open · cash $17.58 (unchanged overnight, no fees) · equity $10,172.18 vs prior close $10,156.25 (+15.93) because holdings re-marked: AEM×6 yday $216.06 → 09:30 $217.03 +5.82; TWST×9 yday $145.59 → 09:30 $144.99 -5.40; ABTC×146 yday $7.93 → 09:30 $8.06 +18.98; HL×61 yday $20.72 → 09:30 $21.04 +19.52; SBET×163 yday $7.91 → 09:30 $8.05 +22.82; PPC×40 yday $32.25 → 09:30 $32.50 +10.00; ABCL×104 yday $11.32 → 09:30 $10.97 -36.40; SENS×138 yday $9.71 → 09:30 $9.57 -19.32; ORBS×6 yday $0.88 → 09:30 $0.89 +0.06; TRON×2 yday $2.01 → 09:30 $2.02 +0.02; XHG×1 yday $4.41 → 09:30 $4.24 -0.17 |
| 2026-08-25 | +1.80 | $17.58 | AEM×6, TWST×9, ABTC×146, HL×61, SBET×163, PPC×40, ABCL×104, SENS×138, ORBS×6, TRON×2, XHG×1 | $10,124.19 | -10.46 | DEFT, AMTX, NIQ, OMER, ERO, TRLV, FUTU | AEM, TWST, ABTC, HL, SBET, PPC, ABCL, SENS | $60.24 | $10,058.96 | ORBS×6, TRON×2, XHG×1, DEFT×2252, AMTX×775, NIQ×73, OMER×76, ERO×37, TRLV×130, FUTU×12 | 09:30 open · cash $17.58 (unchanged overnight, no fees) · equity $10,124.19 vs prior close $10,134.65 (-10.46) because holdings re-marked: AEM×6 yday $214.08 → 09:30 $200.48 -81.60; TWST×9 yday $140.83 → 09:30 $141.51 +6.12; ABTC×146 yday $8.45 → 09:30 $9.00 +80.30; HL×61 yday $20.16 → 09:30 $20.48 +19.52; SBET×163 yday $8.36 → 09:30 $8.16 -32.60; PPC×40 yday $32.22 → 09:30 $31.76 -18.40; ABCL×104 yday $10.52 → 09:30 $10.77 +26.00; SENS×138 yday $9.73 → 09:30 $9.66 -9.66; ORBS×6 yday $0.85 → 09:30 $0.85 +0.00; TRON×2 yday $2.10 → 09:30 $2.05 -0.10; XHG×1 yday $4.06 → 09:30 $4.02 -0.04 |
| 2026-08-26 | +2.02 | $60.24 | ORBS×6, TRON×2, XHG×1, DEFT×2252, AMTX×775, NIQ×73, OMER×76, ERO×37, TRLV×130, FUTU×12 | $10,058.96 | -0.00 | — | — | $60.24 | $10,063.96 | ORBS×6, TRON×2, XHG×1, DEFT×2252, AMTX×775, NIQ×73, OMER×76, ERO×37, TRLV×130, FUTU×12 | 09:30 open · cash $60.24 (unchanged overnight, no fees) · equity $10,058.96 vs prior close $10,058.96 (-0.00) because holdings re-marked: ORBS×6 yday $0.84 → 09:30 $0.84 +0.00; TRON×2 yday $2.04 → 09:30 $2.04 +0.00; XHG×1 yday $4.05 → 09:30 $4.05 +0.00; DEFT×2252 yday $0.62 → 09:30 $0.62 +0.00; AMTX×775 yday $1.86 → 09:30 $1.86 +0.00; NIQ×73 yday $19.46 → 09:30 $19.46 +0.00; OMER×76 yday $19.03 → 09:30 $19.03 +0.00; ERO×37 yday $38.55 → 09:30 $38.55 +0.00; TRLV×130 yday $11.02 → 09:30 $11.02 +0.00; FUTU×12 yday $118.50 → 09:30 $118.50 +0.00 |
| 2026-08-27 | — | $60.24 | ORBS×6, TRON×2, XHG×1, DEFT×2252, AMTX×775, NIQ×73, OMER×76, ERO×37, TRLV×130, FUTU×12 | $10,200.53 | +136.57 | — | ORBS, TRON, XHG | $72.79 | $10,077.07 | DEFT×2252, AMTX×775, NIQ×73, OMER×76, ERO×37, TRLV×130, FUTU×12 | 09:30 open · cash $60.24 (unchanged overnight, no fees) · equity $10,200.53 vs prior close $10,063.96 (+136.57) because holdings re-marked: ORBS×6 yday $0.84 → 09:30 $0.80 -0.24; TRON×2 yday $2.04 → 09:30 $2.08 +0.08; XHG×1 yday $4.05 → 09:30 $3.81 -0.24; DEFT×2252 yday $0.62 → 09:30 $0.60 -45.04; AMTX×775 yday $1.86 → 09:30 $1.91 +38.75; NIQ×73 yday $19.46 → 09:30 $19.20 -18.98; OMER×76 yday $19.03 → 09:30 $18.96 -5.32; ERO×37 yday $38.55 → 09:30 $40.51 +72.52; TRLV×130 yday $11.02 → 09:30 $11.22 +26.00; FUTU×12 yday $118.50 → 09:30 $124.67 +74.04 |
| 2026-08-28 | +0.75 | $72.79 | DEFT×2252, AMTX×775, NIQ×73, OMER×76, ERO×37, TRLV×130, FUTU×12 | $10,096.95 | +19.88 | ZYME, XHG | AMTX | $18.88 | $10,171.92 | DEFT×2252, NIQ×73, OMER×76, ERO×37, TRLV×130, FUTU×12, ZYME×25, XHG×186 | 09:30 open · cash $72.79 (unchanged overnight, no fees) · equity $10,096.95 vs prior close $10,077.07 (+19.88) because holdings re-marked: DEFT×2252 yday $0.59 → 09:30 $0.60 +22.52; AMTX×775 yday $1.88 → 09:30 $1.87 -7.75; NIQ×73 yday $18.74 → 09:30 $18.79 +3.65; OMER×76 yday $18.22 → 09:30 $18.24 +1.52; ERO×37 yday $39.24 → 09:30 $39.20 -1.48; TRLV×130 yday $11.43 → 09:30 $11.38 -6.50; FUTU×12 yday $127.34 → 09:30 $128.00 +7.92 |
| 2026-08-31 | -5.85 | $18.88 | DEFT×2252, NIQ×73, OMER×76, ERO×37, TRLV×130, FUTU×12, ZYME×25, XHG×186 | $10,093.01 | -78.91 | — | — | $18.88 | $10,095.22 | DEFT×2252, NIQ×73, OMER×76, ERO×37, TRLV×130, FUTU×12, ZYME×25, XHG×186 | 09:30 open · cash $18.88 (unchanged overnight, no fees) · equity $10,093.01 vs prior close $10,171.92 (-78.91) because holdings re-marked: DEFT×2252 yday $0.65 → 09:30 $0.62 -67.56; NIQ×73 yday $19.07 → 09:30 $19.20 +9.49; OMER×76 yday $19.25 → 09:30 $18.61 -48.64; ERO×37 yday $39.82 → 09:30 $38.60 -45.14; TRLV×130 yday $11.03 → 09:30 $12.41 +179.40; FUTU×12 yday $124.57 → 09:30 $122.82 -21.00; ZYME×25 yday $29.01 → 09:30 $28.27 -18.50; XHG×186 yday $3.80 → 09:30 $3.44 -66.96 |
| 2026-09-01 | -6.30 | $18.88 | DEFT×2252, NIQ×73, OMER×76, ERO×37, TRLV×130, FUTU×12, ZYME×25, XHG×186 | $9,947.14 | -148.08 | — | NIQ | $1,408.03 | $9,909.65 | DEFT×2252, OMER×76, ERO×37, TRLV×130, FUTU×12, ZYME×25, XHG×186 | 09:30 open · cash $18.88 (unchanged overnight, no fees) · equity $9,947.14 vs prior close $10,095.22 (-148.08) because holdings re-marked: DEFT×2252 yday $0.62 → 09:30 $0.59 -67.56; NIQ×73 yday $19.20 → 09:30 $19.06 -10.22; OMER×76 yday $18.50 → 09:30 $18.79 +22.04; ERO×37 yday $38.49 → 09:30 $37.30 -44.03; TRLV×130 yday $12.41 → 09:30 $11.89 -67.60; FUTU×12 yday $124.04 → 09:30 $122.22 -21.84; ZYME×25 yday $28.27 → 09:30 $29.32 +26.25; XHG×186 yday $3.44 → 09:30 $3.52 +14.88 |
| 2026-09-02 | -3.83 | $1,408.03 | DEFT×2252, OMER×76, ERO×37, TRLV×130, FUTU×12, ZYME×25, XHG×186 | $9,893.42 | -16.23 | — | ZYME | $2,138.94 | $9,949.02 | DEFT×2252, OMER×76, ERO×37, TRLV×130, FUTU×12, XHG×186 | 09:30 open · cash $1,408.03 (unchanged overnight, no fees) · equity $9,893.42 vs prior close $9,909.65 (-16.23) because holdings re-marked: DEFT×2252 yday $0.61 → 09:30 $0.63 +45.04; OMER×76 yday $18.79 → 09:30 $18.66 -9.88; ERO×37 yday $36.01 → 09:30 $35.95 -2.22; TRLV×130 yday $11.89 → 09:30 $11.54 -45.50; FUTU×12 yday $120.88 → 09:30 $119.82 -12.72; ZYME×25 yday $29.33 → 09:30 $29.32 -0.25; XHG×186 yday $3.43 → 09:30 $3.48 +9.30 |
| 2026-09-03 | -0.90 | $2,138.94 | DEFT×2252, OMER×76, ERO×37, TRLV×130, FUTU×12, XHG×186 | $10,036.38 | +87.36 | NVAX, NIQ | — | $6.20 | $9,862.89 | DEFT×2252, OMER×76, ERO×37, TRLV×130, FUTU×12, XHG×186, NVAX×104, NIQ×57 | 09:30 open · cash $2,138.94 (unchanged overnight, no fees) · equity $10,036.38 vs prior close $9,949.02 (+87.36) because holdings re-marked: DEFT×2252 yday $0.66 → 09:30 $0.67 +22.52; OMER×76 yday $18.75 → 09:30 $18.97 +16.72; ERO×37 yday $34.82 → 09:30 $35.62 +29.60; TRLV×130 yday $11.74 → 09:30 $11.78 +5.20; FUTU×12 yday $119.28 → 09:30 $119.46 +2.16; XHG×186 yday $3.51 → 09:30 $3.57 +11.16 |
| 2026-09-04 | — | $6.20 | DEFT×2252, OMER×76, ERO×37, TRLV×130, FUTU×12, XHG×186, NVAX×104, NIQ×57 | $9,977.50 | +114.61 | — | — | $6.20 | $10,105.66 | DEFT×2252, OMER×76, ERO×37, TRLV×130, FUTU×12, XHG×186, NVAX×104, NIQ×57 | 09:30 open · cash $6.20 (unchanged overnight, no fees) · equity $9,977.50 vs prior close $9,862.89 (+114.61) because holdings re-marked: DEFT×2252 yday $0.65 → 09:30 $0.65 +0.00; OMER×76 yday $18.86 → 09:30 $18.99 +9.88; ERO×37 yday $34.76 → 09:30 $35.82 +39.22; TRLV×130 yday $11.69 → 09:30 $11.89 +26.00; FUTU×12 yday $118.08 → 09:30 $118.19 +1.32; XHG×186 yday $3.32 → 09:30 $3.38 +11.16; NVAX×104 yday $10.32 → 09:30 $10.41 +9.36; NIQ×57 yday $18.35 → 09:30 $18.66 +17.67 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $8,760.28 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANRO` | 39 | $31.77 | $2.11 | — | $7,519.15 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+13.5; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LIFE` | 35 | $35.04 | $2.10 | — | $6,290.65 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+16.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VOYG` | 28 | $44.49 | $2.07 | — | $5,042.86 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 65 | $19.17 | $2.19 | — | $3,794.62 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETA` | 49 | $25.21 | $2.14 | — | $2,557.20 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+15.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `FORM` | 9 | $129.48 | $2.02 | — | $1,389.86 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+14.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ENTG` | 7 | $162.45 | $2.01 | — | $250.70 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+14.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $250.70 | ▲ 09:30 equity $9,917.58 vs yday $9,881.56 (+36.02) | 09:30 open · cash $250.70 (unchanged overnight, no fees) · equity $9,917.58 vs prior close $9,881.56 (+36.02) because holdings re-marked: ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; ANRO×39 yday $32.14 → 09:30 $32.15 +0.39; LIFE×35 yday $34.02 → 09:30 $34.03 +0.35; VOYG×28 yday $42.98 → 09:30 $42.12 -24.08; LUNR×65 yday $19.01 → 09:30 $20.25 +80.60; BETA×49 yday $24.86 → 09:30 $24.61 -12.25; FORM×9 yday $131.60 → 09:30 $134.05 +22.05; ENTG×7 yday $161.76 → 09:30 $162.04 +1.96 | — |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 1 | $18.24 | $0.19 | — | $232.27 | — | baseline list, no extra gate; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $35.81 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 2 | $16.20 | $0.33 | — | $199.54 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $35.81 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 2 | $14.94 | $0.30 | — | $169.36 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $35.81 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `CLYM` | 2 | $16.25 | $0.33 | — | $136.53 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+16.6; leftover $35.81 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 7 | $4.59 | $0.34 | — | $104.06 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; leftover $35.81 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `IOVA` | 5 | $6.84 | $0.36 | — | $69.50 | — | baseline list, no extra gate; list ohlc_hot; ret5=+10.1; leftover $35.81 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $69.50 | ▼ 09:30 equity $9,776.58 vs yday $10,176.53 (-399.95) | 09:30 open · cash $69.50 (unchanged overnight, no fees) · equity $9,776.58 vs prior close $10,176.53 (-399.95) because holdings re-marked: ADUR×75 yday $15.85 → 09:30 $15.41 -33.00; ANRO×39 yday $33.60 → 09:30 $33.18 -16.38; LIFE×35 yday $35.17 → 09:30 $34.06 -38.85; VOYG×28 yday $43.98 → 09:30 $41.83 -60.20; LUNR×65 yday $20.38 → 09:30 $19.31 -69.55; BETA×49 yday $25.60 → 09:30 $24.99 -29.89; FORM×9 yday $138.16 → 09:30 $129.28 -79.92; ENTG×7 yday $163.09 → 09:30 $153.47 -67.34; OCC×1 yday $17.12 → 09:30 $16.20 -0.92; ALM×2 yday $16.36 → 09:30 $15.78 -1.16; LPTH×2 yday $14.80 → 09:30 $14.01 -1.58; CLYM×2 yday $17.44 → 09:30 $16.90 -1.08; BORR×7 yday $4.50 → 09:30 $4.56 +0.42; IOVA×5 yday $7.10 → 09:30 $7.00 -0.50 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $69.50 | ▲ 09:30 equity $9,914.71 vs yday $9,860.01 (+54.70) | 09:30 open · cash $69.50 (unchanged overnight, no fees) · equity $9,914.71 vs prior close $9,860.01 (+54.70) because holdings re-marked: ADUR×75 yday $15.63 → 09:30 $15.65 +1.50; ANRO×39 yday $34.13 → 09:30 $35.00 +33.93; LIFE×35 yday $34.01 → 09:30 $34.37 +12.60; VOYG×28 yday $42.24 → 09:30 $41.93 -8.68; LUNR×65 yday $19.31 → 09:30 $18.98 -21.45; BETA×49 yday $26.76 → 09:30 $26.80 +1.96; FORM×9 yday $124.34 → 09:30 $126.03 +15.21; ENTG×7 yday $150.27 → 09:30 $152.52 +15.75; OCC×1 yday $16.20 → 09:30 $16.21 +0.01; ALM×2 yday $15.60 → 09:30 $16.05 +0.90; LPTH×2 yday $14.22 → 09:30 $14.30 +0.16; CLYM×2 yday $17.39 → 09:30 $18.09 +1.40; BORR×7 yday $4.43 → 09:30 $4.51 +0.56; IOVA×5 yday $7.03 → 09:30 $7.20 +0.85 | — |
| 2026-08-19 09:30 ET | **SELL** | `ADUR` | 75 | $15.65 | $2.24 | $-68.20 | $1,241.01 | ▼ -68.20 after sell → book $9,912.47; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANRO` | 39 | $35.00 | $2.13 | $+121.74 | $2,603.88 | ▲ +121.74 after sell → book $9,910.34; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LIFE` | 35 | $34.37 | $2.12 | $-27.66 | $3,804.72 | ▼ -27.66 after sell → book $9,908.23; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `VOYG` | 28 | $41.93 | $2.09 | $-75.85 | $4,976.66 | ▼ -75.85 after sell → book $9,906.13; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LUNR` | 65 | $18.98 | $2.21 | $-16.74 | $6,208.16 | ▼ -16.74 after sell → book $9,903.93; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `BETA` | 49 | $26.80 | $2.16 | $+73.62 | $7,519.20 | ▲ +73.62 after sell → book $9,901.77; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `FORM` | 9 | $126.03 | $2.04 | $-35.10 | $8,651.43 | ▼ -35.10 after sell → book $9,899.73; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ENTG` | 7 | $152.52 | $2.03 | $-73.55 | $9,717.04 | ▼ -73.55 after sell → book $9,897.70; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,717.04 | ▼ 09:30 equity $9,894.83 vs yday $9,895.67 (-0.84) | 09:30 open · cash $9,717.04 (unchanged overnight, no fees) · equity $9,894.83 vs prior close $9,895.67 (-0.84) because holdings re-marked: OCC×1 yday $14.36 → 09:30 $14.10 -0.26; ALM×2 yday $16.18 → 09:30 $15.81 -0.74; LPTH×2 yday $13.24 → 09:30 $13.09 -0.30; CLYM×2 yday $17.34 → 09:30 $17.16 -0.36; BORR×7 yday $4.40 → 09:30 $4.46 +0.42; IOVA×5 yday $7.99 → 09:30 $8.07 +0.40 | — |
| 2026-08-20 09:30 ET | **SELL** | `OCC` | 1 | $14.10 | $0.16 | $-4.49 | $9,730.98 | ▼ -4.49 after sell → book $9,894.67; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ALM` | 2 | $15.81 | $0.34 | $-1.45 | $9,762.26 | ▼ -1.45 after sell → book $9,894.33; vs 09:30 mark -0.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `LPTH` | 2 | $13.09 | $0.29 | $-4.29 | $9,788.15 | ▼ -4.29 after sell → book $9,894.04; vs 09:30 mark -0.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `CLYM` | 2 | $17.16 | $0.37 | $+1.12 | $9,822.10 | ▲ +1.12 after sell → book $9,893.67; vs 09:30 mark -0.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `BORR` | 7 | $4.46 | $0.35 | $-1.61 | $9,852.97 | ▼ -1.61 after sell → book $9,893.32; vs 09:30 mark -0.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `IOVA` | 5 | $8.07 | $0.44 | $+5.35 | $9,892.88 | ▲ +5.35 after sell → book $9,892.88; vs 09:30 mark -0.44 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AEM` | 6 | $204.45 | $2.01 | — | $8,664.17 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $1236.61 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TWST` | 9 | $136.84 | $2.02 | — | $7,430.59 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+13.7; leftover $1236.61 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABTC` | 146 | $8.46 | $2.43 | — | $6,193.00 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+14.0; leftover $1236.61 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HL` | 61 | $20.25 | $2.17 | — | $4,955.58 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+13.5; leftover $1236.61 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SBET` | 163 | $7.55 | $2.48 | — | $3,722.45 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+14.6; leftover $1236.61 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `PPC` | 40 | $30.65 | $2.11 | — | $2,494.34 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+16.5; leftover $1236.61 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 104 | $11.81 | $2.30 | — | $1,263.28 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1236.61 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 138 | $8.91 | $2.40 | — | $31.30 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1236.61 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $31.30 | ▲ 09:30 equity $10,146.87 vs yday $9,944.36 (+202.51) | 09:30 open · cash $31.30 (unchanged overnight, no fees) · equity $10,146.87 vs prior close $9,944.36 (+202.51) because holdings re-marked: AEM×6 yday $212.04 → 09:30 $216.30 +25.56; TWST×9 yday $136.33 → 09:30 $138.43 +18.90; ABTC×146 yday $8.47 → 09:30 $8.66 +27.74; HL×61 yday $20.82 → 09:30 $21.33 +31.11; SBET×163 yday $7.59 → 09:30 $7.87 +45.64; PPC×40 yday $31.24 → 09:30 $31.13 -4.40; ABCL×104 yday $11.57 → 09:30 $11.57 +0.00; SENS×138 yday $8.82 → 09:30 $9.24 +57.96 | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 6 | $0.86 | $0.07 | — | $26.04 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $5.22 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TRON` | 2 | $1.94 | $0.04 | — | $22.12 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+15.4; leftover $5.22 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 1 | $4.49 | $0.05 | — | $17.58 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+12.7; leftover $5.22 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.58 | ▲ 09:30 equity $10,172.18 vs yday $10,156.25 (+15.93) | 09:30 open · cash $17.58 (unchanged overnight, no fees) · equity $10,172.18 vs prior close $10,156.25 (+15.93) because holdings re-marked: AEM×6 yday $216.06 → 09:30 $217.03 +5.82; TWST×9 yday $145.59 → 09:30 $144.99 -5.40; ABTC×146 yday $7.93 → 09:30 $8.06 +18.98; HL×61 yday $20.72 → 09:30 $21.04 +19.52; SBET×163 yday $7.91 → 09:30 $8.05 +22.82; PPC×40 yday $32.25 → 09:30 $32.50 +10.00; ABCL×104 yday $11.32 → 09:30 $10.97 -36.40; SENS×138 yday $9.71 → 09:30 $9.57 -19.32; ORBS×6 yday $0.88 → 09:30 $0.89 +0.06; TRON×2 yday $2.01 → 09:30 $2.02 +0.02; XHG×1 yday $4.41 → 09:30 $4.24 -0.17 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.58 | ▼ 09:30 equity $10,124.19 vs yday $10,134.65 (-10.46) | 09:30 open · cash $17.58 (unchanged overnight, no fees) · equity $10,124.19 vs prior close $10,134.65 (-10.46) because holdings re-marked: AEM×6 yday $214.08 → 09:30 $200.48 -81.60; TWST×9 yday $140.83 → 09:30 $141.51 +6.12; ABTC×146 yday $8.45 → 09:30 $9.00 +80.30; HL×61 yday $20.16 → 09:30 $20.48 +19.52; SBET×163 yday $8.36 → 09:30 $8.16 -32.60; PPC×40 yday $32.22 → 09:30 $31.76 -18.40; ABCL×104 yday $10.52 → 09:30 $10.77 +26.00; SENS×138 yday $9.73 → 09:30 $9.66 -9.66; ORBS×6 yday $0.85 → 09:30 $0.85 +0.00; TRON×2 yday $2.10 → 09:30 $2.05 -0.10; XHG×1 yday $4.06 → 09:30 $4.02 -0.04 | — |
| 2026-08-25 09:30 ET | **SELL** | `AEM` | 6 | $200.48 | $2.03 | $-27.86 | $1,218.43 | ▼ -27.86 after sell → book $10,122.16; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `TWST` | 9 | $141.51 | $2.04 | $+37.98 | $2,489.99 | ▲ +37.98 after sell → book $10,120.13; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABTC` | 146 | $9.00 | $2.46 | $+73.95 | $3,801.52 | ▲ +73.95 after sell → book $10,117.66; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `HL` | 61 | $20.48 | $2.19 | $+9.66 | $5,048.61 | ▲ +9.66 after sell → book $10,115.47; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SBET` | 163 | $8.16 | $2.52 | $+94.43 | $6,376.17 | ▲ +94.43 after sell → book $10,112.95; vs 09:30 mark -2.52 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `PPC` | 40 | $31.76 | $2.13 | $+40.16 | $7,644.44 | ▲ +40.16 after sell → book $10,110.82; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABCL` | 104 | $10.77 | $2.33 | $-113.31 | $8,762.19 | ▼ -113.31 after sell → book $10,108.49; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SENS` | 138 | $9.66 | $2.44 | $+98.66 | $10,092.84 | ▲ +98.66 after sell → book $10,106.06; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2252 | $0.64 | $21.17 | — | $8,630.39 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1441.83 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AMTX` | 775 | $1.86 | $10.00 | — | $7,178.89 | — | baseline list, no extra gate; list yday_mover,ohlc_hot; ⚪; ret5=+16.9; leftover $1441.83 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NIQ` | 73 | $19.56 | $2.21 | — | $5,748.80 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+7.6; leftover $1441.83 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `OMER` | 76 | $18.75 | $2.22 | — | $4,321.58 | — | baseline list, no extra gate; list ohlc_hot; ret5=+12.1; leftover $1441.83 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ERO` | 37 | $38.00 | $2.10 | — | $2,913.48 | — | baseline list, no extra gate; list ohlc_hot,mover_buy; ⚪; ret5=+16.6; leftover $1441.83 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `TRLV` | 130 | $11.02 | $2.38 | — | $1,478.50 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.0; leftover $1441.83 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FUTU` | 12 | $118.02 | $2.03 | — | $60.24 | — | baseline list, no extra gate; list ohlc_hot; ⚪; ret5=+17.5; leftover $1441.83 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.24 | ▲ 09:30 equity $10,058.96 vs yday $10,058.96 (-0.00) | 09:30 open · cash $60.24 (unchanged overnight, no fees) · equity $10,058.96 vs prior close $10,058.96 (-0.00) because holdings re-marked: ORBS×6 yday $0.84 → 09:30 $0.84 +0.00; TRON×2 yday $2.04 → 09:30 $2.04 +0.00; XHG×1 yday $4.05 → 09:30 $4.05 +0.00; DEFT×2252 yday $0.62 → 09:30 $0.62 +0.00; AMTX×775 yday $1.86 → 09:30 $1.86 +0.00; NIQ×73 yday $19.46 → 09:30 $19.46 +0.00; OMER×76 yday $19.03 → 09:30 $19.03 +0.00; ERO×37 yday $38.55 → 09:30 $38.55 +0.00; TRLV×130 yday $11.02 → 09:30 $11.02 +0.00; FUTU×12 yday $118.50 → 09:30 $118.50 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.24 | ▲ 09:30 equity $10,200.53 vs yday $10,063.96 (+136.57) | 09:30 open · cash $60.24 (unchanged overnight, no fees) · equity $10,200.53 vs prior close $10,063.96 (+136.57) because holdings re-marked: ORBS×6 yday $0.84 → 09:30 $0.80 -0.24; TRON×2 yday $2.04 → 09:30 $2.08 +0.08; XHG×1 yday $4.05 → 09:30 $3.81 -0.24; DEFT×2252 yday $0.62 → 09:30 $0.60 -45.04; AMTX×775 yday $1.86 → 09:30 $1.91 +38.75; NIQ×73 yday $19.46 → 09:30 $19.20 -18.98; OMER×76 yday $19.03 → 09:30 $18.96 -5.32; ERO×37 yday $38.55 → 09:30 $40.51 +72.52; TRLV×130 yday $11.02 → 09:30 $11.22 +26.00; FUTU×12 yday $118.50 → 09:30 $124.67 +74.04 | — |
| 2026-08-27 09:30 ET | **SELL** | `ORBS` | 6 | $0.80 | $0.09 | $-0.54 | $64.95 | ▼ -0.54 after sell → book $10,200.44; vs 09:30 mark -0.09 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRON` | 2 | $2.08 | $0.07 | $+0.17 | $69.04 | ▲ +0.17 after sell → book $10,200.37; vs 09:30 mark -0.07 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 1 | $3.81 | $0.06 | $-0.79 | $72.79 | ▼ -0.79 after sell → book $10,200.31; vs 09:30 mark -0.06 | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.79 | ▲ 09:30 equity $10,096.95 vs yday $10,077.07 (+19.88) | 09:30 open · cash $72.79 (unchanged overnight, no fees) · equity $10,096.95 vs prior close $10,077.07 (+19.88) because holdings re-marked: DEFT×2252 yday $0.59 → 09:30 $0.60 +22.52; AMTX×775 yday $1.88 → 09:30 $1.87 -7.75; NIQ×73 yday $18.74 → 09:30 $18.79 +3.65; OMER×76 yday $18.22 → 09:30 $18.24 +1.52; ERO×37 yday $39.24 → 09:30 $39.20 -1.48; TRLV×130 yday $11.43 → 09:30 $11.38 -6.50; FUTU×12 yday $127.34 → 09:30 $128.00 +7.92 | — |
| 2026-08-28 09:30 ET | **SELL** | `AMTX` | 775 | $1.87 | $10.14 | $-12.39 | $1,511.90 | ▼ -12.39 after sell → book $10,086.81; vs 09:30 mark -10.14 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 25 | $29.33 | $2.06 | — | $776.59 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $755.95 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `XHG` | 186 | $4.06 | $2.55 | — | $18.88 | — | baseline list, no extra gate; list ohlc_hot; ret5=+16.1; leftover $755.95 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.88 | ▼ 09:30 equity $10,093.01 vs yday $10,171.92 (-78.91) | 09:30 open · cash $18.88 (unchanged overnight, no fees) · equity $10,093.01 vs prior close $10,171.92 (-78.91) because holdings re-marked: DEFT×2252 yday $0.65 → 09:30 $0.62 -67.56; NIQ×73 yday $19.07 → 09:30 $19.20 +9.49; OMER×76 yday $19.25 → 09:30 $18.61 -48.64; ERO×37 yday $39.82 → 09:30 $38.60 -45.14; TRLV×130 yday $11.03 → 09:30 $12.41 +179.40; FUTU×12 yday $124.57 → 09:30 $122.82 -21.00; ZYME×25 yday $29.01 → 09:30 $28.27 -18.50; XHG×186 yday $3.80 → 09:30 $3.44 -66.96 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.88 | ▼ 09:30 equity $9,947.14 vs yday $10,095.22 (-148.08) | 09:30 open · cash $18.88 (unchanged overnight, no fees) · equity $9,947.14 vs prior close $10,095.22 (-148.08) because holdings re-marked: DEFT×2252 yday $0.62 → 09:30 $0.59 -67.56; NIQ×73 yday $19.20 → 09:30 $19.06 -10.22; OMER×76 yday $18.50 → 09:30 $18.79 +22.04; ERO×37 yday $38.49 → 09:30 $37.30 -44.03; TRLV×130 yday $12.41 → 09:30 $11.89 -67.60; FUTU×12 yday $124.04 → 09:30 $122.22 -21.84; ZYME×25 yday $28.27 → 09:30 $29.32 +26.25; XHG×186 yday $3.44 → 09:30 $3.52 +14.88 | — |
| 2026-09-01 09:30 ET | **SELL** | `NIQ` | 73 | $19.06 | $2.23 | $-40.94 | $1,408.03 | ▼ -40.94 after sell → book $9,944.91; vs 09:30 mark -2.23 | dropped from list after 5 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,408.03 | ▼ 09:30 equity $9,893.42 vs yday $9,909.65 (-16.23) | 09:30 open · cash $1,408.03 (unchanged overnight, no fees) · equity $9,893.42 vs prior close $9,909.65 (-16.23) because holdings re-marked: DEFT×2252 yday $0.61 → 09:30 $0.63 +45.04; OMER×76 yday $18.79 → 09:30 $18.66 -9.88; ERO×37 yday $36.01 → 09:30 $35.95 -2.22; TRLV×130 yday $11.89 → 09:30 $11.54 -45.50; FUTU×12 yday $120.88 → 09:30 $119.82 -12.72; ZYME×25 yday $29.33 → 09:30 $29.32 -0.25; XHG×186 yday $3.43 → 09:30 $3.48 +9.30 | — |
| 2026-09-02 09:30 ET | **SELL** | `ZYME` | 25 | $29.32 | $2.08 | $-4.40 | $2,138.94 | ▼ -4.40 after sell → book $9,891.33; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,138.94 | ▲ 09:30 equity $10,036.38 vs yday $9,949.02 (+87.36) | 09:30 open · cash $2,138.94 (unchanged overnight, no fees) · equity $10,036.38 vs prior close $9,949.02 (+87.36) because holdings re-marked: DEFT×2252 yday $0.66 → 09:30 $0.67 +22.52; OMER×76 yday $18.75 → 09:30 $18.97 +16.72; ERO×37 yday $34.82 → 09:30 $35.62 +29.60; TRLV×130 yday $11.74 → 09:30 $11.78 +5.20; FUTU×12 yday $119.28 → 09:30 $119.46 +2.16; XHG×186 yday $3.51 → 09:30 $3.57 +11.16 | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 104 | $10.27 | $2.30 | — | $1,068.56 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1069.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NIQ` | 57 | $18.60 | $2.16 | — | $6.20 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+7.6; leftover $1069.47 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.20 | ▲ 09:30 equity $9,977.50 vs yday $9,862.89 (+114.61) | 09:30 open · cash $6.20 (unchanged overnight, no fees) · equity $9,977.50 vs prior close $9,862.89 (+114.61) because holdings re-marked: DEFT×2252 yday $0.65 → 09:30 $0.65 +0.00; OMER×76 yday $18.86 → 09:30 $18.99 +9.88; ERO×37 yday $34.76 → 09:30 $35.82 +39.22; TRLV×130 yday $11.69 → 09:30 $11.89 +26.00; FUTU×12 yday $118.08 → 09:30 $118.19 +1.32; XHG×186 yday $3.32 → 09:30 $3.38 +11.16; NVAX×104 yday $10.32 → 09:30 $10.41 +9.36; NIQ×57 yday $18.35 → 09:30 $18.66 +17.67 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `ADUR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `VOYG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BETA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `FORM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ENTG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AAOI` | cash | leftover split 35.81 < 1 share @ 152.64 |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VOYG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LUNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BETA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `FORM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ENTG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OCC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ALM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `LPTH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `CLYM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BORR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SMTC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MRVL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AAOI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ELMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `STDN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OCC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ALM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `LPTH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `CLYM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `BORR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRGP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `OABI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `XNCR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PAYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `TWST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `PPC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SENS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `GRAL` | cash | leftover split 5.22 < 1 share @ 78.88 |
| 2026-08-21 | `MSTR` | cash | leftover split 5.22 < 1 share @ 119.69 |
| 2026-08-21 | `AUGO` | cash | leftover split 5.22 < 1 share @ 89.10 |
| 2026-08-24 | `AEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TWST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `PPC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SENS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `TRON` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GUTS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `UEC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NIQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `TRON` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `ORBS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `TRON` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `HOOD` | no_price | no 09:30 open |
| 2026-08-27 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NIQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `OMER` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ERO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `TRLV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FUTU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ZYME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ZYME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `HOOD` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TXG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `CVI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HOOD` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `DEFT` | 2252 | 2026-08-25 @ $0.64 | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1441.83 |
| `OMER` | 76 | 2026-08-25 @ $18.75 | baseline list, no extra gate; list ohlc_hot; ret5=+12.1; leftover $1441.83 |
| `ERO` | 37 | 2026-08-25 @ $38.00 | baseline list, no extra gate; list ohlc_hot,mover_buy; ⚪; ret5=+16.6; leftover $1441.83 |
| `TRLV` | 130 | 2026-08-25 @ $11.02 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.0; leftover $1441.83 |
| `FUTU` | 12 | 2026-08-25 @ $118.02 | baseline list, no extra gate; list ohlc_hot; ⚪; ret5=+17.5; leftover $1441.83 |
| `XHG` | 186 | 2026-08-28 @ $4.06 | baseline list, no extra gate; list ohlc_hot; ret5=+16.1; leftover $755.95 |
| `NVAX` | 104 | 2026-09-03 @ $10.27 | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1069.47 |
| `NIQ` | 57 | 2026-09-03 @ $18.60 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+7.6; leftover $1069.47 |
