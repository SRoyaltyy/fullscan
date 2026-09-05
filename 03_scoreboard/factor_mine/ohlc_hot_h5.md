# Factor mine action — `ohlc_hot_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `ohlc_hot` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-8.27%** ($9,174) · signal-only (no cash/fees) was +86.73%. Starts YES **8/17**. Fills 58 · skips 137 · realized $-107.32.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `ohlc_hot` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $2,917.75.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | ADUR, ANRO, LIFE, VOYG, LUNR, BETA, FORM, ENTG | — | $250.70 | $9,881.56 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $250.70 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7 | $9,917.58 | +36.02 | OCC, ALM, LPTH, CLYM, BORR, IOVA | — | $69.50 | $10,176.53 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7, OCC×1, ALM×2, LPTH×2, CLYM×2, BORR×7, IOVA×5 | 09:30 open · cash $250.70 (unchanged overnight, no fees) · equity $9,917.58 vs prior close $9,881.56 (+36.02) because holdings re-marked: ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; ANRO×39 yday $32.14 → 09:30 $32.15 +0.39; LIFE×35 yday $34.02 → 09:30 $34.03 +0.35; VOYG×28 yday $42.98 → 09:30 $42.12 -24.08; LUNR×65 yday $19.01 → 09:30 $20.25 +80.60; BETA×49 yday $24.86 → 09:30 $24.61 -12.25; FORM×9 yday $131.60 → 09:30 $134.05 +22.05; ENTG×7 yday $161.76 → 09:30 $162.04 +1.96 |
| 2026-08-18 | -6.20 | $69.50 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7, OCC×1, ALM×2, LPTH×2, CLYM×2, BORR×7, IOVA×5 | $9,776.58 | -399.95 | — | — | $69.50 | $9,860.01 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7, OCC×1, ALM×2, LPTH×2, CLYM×2, BORR×7, IOVA×5 | 09:30 open · cash $69.50 (unchanged overnight, no fees) · equity $9,776.58 vs prior close $10,176.53 (-399.95) because holdings re-marked: ADUR×75 yday $15.85 → 09:30 $15.41 -33.00; ANRO×39 yday $33.60 → 09:30 $33.18 -16.38; LIFE×35 yday $35.17 → 09:30 $34.06 -38.85; VOYG×28 yday $43.98 → 09:30 $41.83 -60.20; LUNR×65 yday $20.38 → 09:30 $19.31 -69.55; BETA×49 yday $25.60 → 09:30 $24.99 -29.89; FORM×9 yday $138.16 → 09:30 $129.28 -79.92; ENTG×7 yday $163.09 → 09:30 $153.47 -67.34; OCC×1 yday $17.12 → 09:30 $16.20 -0.92; ALM×2 yday $16.36 → 09:30 $15.78 -1.16; LPTH×2 yday $14.80 → 09:30 $14.01 -1.58; CLYM×2 yday $17.44 → 09:30 $16.90 -1.08; BORR×7 yday $4.50 → 09:30 $4.56 +0.42; IOVA×5 yday $7.10 → 09:30 $7.00 -0.50 |
| 2026-08-19 | -7.20 | $69.50 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7, OCC×1, ALM×2, LPTH×2, CLYM×2, BORR×7, IOVA×5 | $9,914.71 | +54.70 | — | — | $69.50 | $9,542.55 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7, OCC×1, ALM×2, LPTH×2, CLYM×2, BORR×7, IOVA×5 | 09:30 open · cash $69.50 (unchanged overnight, no fees) · equity $9,914.71 vs prior close $9,860.01 (+54.70) because holdings re-marked: ADUR×75 yday $15.63 → 09:30 $15.65 +1.50; ANRO×39 yday $34.13 → 09:30 $35.00 +33.93; LIFE×35 yday $34.01 → 09:30 $34.37 +12.60; VOYG×28 yday $42.24 → 09:30 $41.93 -8.68; LUNR×65 yday $19.31 → 09:30 $18.98 -21.45; BETA×49 yday $26.76 → 09:30 $26.80 +1.96; FORM×9 yday $124.34 → 09:30 $126.03 +15.21; ENTG×7 yday $150.27 → 09:30 $152.52 +15.75; OCC×1 yday $16.20 → 09:30 $16.21 +0.01; ALM×2 yday $15.60 → 09:30 $16.05 +0.90; LPTH×2 yday $14.22 → 09:30 $14.30 +0.16; CLYM×2 yday $17.39 → 09:30 $18.09 +1.40; BORR×7 yday $4.43 → 09:30 $4.51 +0.56; IOVA×5 yday $7.03 → 09:30 $7.20 +0.85 |
| 2026-08-20 | +1.12 | $69.50 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7, OCC×1, ALM×2, LPTH×2, CLYM×2, BORR×7, IOVA×5 | $9,474.24 | -68.31 | ABTC, SBET | — | $53.32 | $9,482.15 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7, OCC×1, ALM×2, LPTH×2, CLYM×2, BORR×7, IOVA×5, ABTC×1, SBET×1 | 09:30 open · cash $69.50 (unchanged overnight, no fees) · equity $9,474.24 vs prior close $9,542.55 (-68.31) because holdings re-marked: ADUR×75 yday $15.39 → 09:30 $15.55 +12.00; ANRO×39 yday $34.89 → 09:30 $34.36 -20.67; LIFE×35 yday $32.85 → 09:30 $32.86 +0.35; VOYG×28 yday $38.85 → 09:30 $38.25 -16.80; LUNR×65 yday $18.52 → 09:30 $18.13 -25.35; BETA×49 yday $26.40 → 09:30 $26.16 -11.76; FORM×9 yday $114.95 → 09:30 $114.50 -4.05; ENTG×7 yday $144.28 → 09:30 $144.11 -1.19; OCC×1 yday $14.36 → 09:30 $14.10 -0.26; ALM×2 yday $16.18 → 09:30 $15.81 -0.74; LPTH×2 yday $13.24 → 09:30 $13.09 -0.30; CLYM×2 yday $17.34 → 09:30 $17.16 -0.36; BORR×7 yday $4.40 → 09:30 $4.46 +0.42; IOVA×5 yday $7.99 → 09:30 $8.07 +0.40 |
| 2026-08-21 | +3.25 | $53.32 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7, OCC×1, ALM×2, LPTH×2, CLYM×2, BORR×7, IOVA×5, ABTC×1, SBET×1 | $9,624.15 | +142.00 | AEM, ORBS, GRAL, MSTR, TRON, XHG, AUGO | ADUR, ANRO, LIFE, VOYG, LUNR, BETA, FORM, ENTG | $44.25 | $9,593.53 | OCC×1, ALM×2, LPTH×2, CLYM×2, BORR×7, IOVA×5, ABTC×1, SBET×1, AEM×6, ORBS×1554, GRAL×17, MSTR×11, TRON×692, XHG×299, AUGO×15 | 09:30 open · cash $53.32 (unchanged overnight, no fees) · equity $9,624.15 vs prior close $9,482.15 (+142.00) because holdings re-marked: ADUR×75 yday $15.85 → 09:30 $16.00 +11.25; ANRO×39 yday $34.25 → 09:30 $34.44 +7.41; LIFE×35 yday $33.52 → 09:30 $33.90 +13.30; VOYG×28 yday $37.92 → 09:30 $38.84 +25.76; LUNR×65 yday $17.93 → 09:30 $18.74 +52.65; BETA×49 yday $25.48 → 09:30 $25.56 +3.92; FORM×9 yday $115.71 → 09:30 $117.69 +17.82; ENTG×7 yday $144.63 → 09:30 $145.64 +7.07; OCC×1 yday $14.12 → 09:30 $14.20 +0.08; ALM×2 yday $17.69 → 09:30 $18.00 +0.62; LPTH×2 yday $13.02 → 09:30 $13.10 +0.16; CLYM×2 yday $17.02 → 09:30 $17.26 +0.48; BORR×7 yday $4.43 → 09:30 $4.51 +0.56; IOVA×5 yday $8.99 → 09:30 $9.08 +0.45; ABTC×1 yday $8.47 → 09:30 $8.66 +0.19; SBET×1 yday $7.59 → 09:30 $7.87 +0.28 |
| 2026-08-24 | -5.17 | $44.25 | OCC×1, ALM×2, LPTH×2, CLYM×2, BORR×7, IOVA×5, ABTC×1, SBET×1, AEM×6, ORBS×1554, GRAL×17, MSTR×11, TRON×692, XHG×299, AUGO×15 | $9,675.37 | +81.84 | — | OCC, ALM, LPTH, CLYM, BORR, IOVA | $227.20 | $9,488.16 | ABTC×1, SBET×1, AEM×6, ORBS×1554, GRAL×17, MSTR×11, TRON×692, XHG×299, AUGO×15 | 09:30 open · cash $44.25 (unchanged overnight, no fees) · equity $9,675.37 vs prior close $9,593.53 (+81.84) because holdings re-marked: OCC×1 yday $13.85 → 09:30 $13.60 -0.25; ALM×2 yday $18.51 → 09:30 $18.69 +0.36; LPTH×2 yday $14.42 → 09:30 $13.92 -1.00; CLYM×2 yday $17.28 → 09:30 $17.27 -0.02; BORR×7 yday $4.50 → 09:30 $4.48 -0.14; IOVA×5 yday $8.29 → 09:30 $8.05 -1.20; ABTC×1 yday $7.93 → 09:30 $8.06 +0.13; SBET×1 yday $7.91 → 09:30 $8.05 +0.14; AEM×6 yday $216.06 → 09:30 $217.03 +5.82; ORBS×1554 yday $0.88 → 09:30 $0.89 +15.54; GRAL×17 yday $79.54 → 09:30 $81.87 +39.61; MSTR×11 yday $119.25 → 09:30 $121.76 +27.61; TRON×692 yday $2.01 → 09:30 $2.02 +6.92; XHG×299 yday $4.41 → 09:30 $4.24 -50.83; AUGO×15 yday $87.26 → 09:30 $89.87 +39.15 |
| 2026-08-25 | +1.80 | $227.20 | ABTC×1, SBET×1, AEM×6, ORBS×1554, GRAL×17, MSTR×11, TRON×692, XHG×299, AUGO×15 | $9,406.74 | -81.42 | DEFT, AMTX, NIQ, OMER, TRLV | — | $101.77 | $9,404.50 | ABTC×1, SBET×1, AEM×6, ORBS×1554, GRAL×17, MSTR×11, TRON×692, XHG×299, AUGO×15, DEFT×50, AMTX×17, NIQ×1, OMER×1, TRLV×2 | 09:30 open · cash $227.20 (unchanged overnight, no fees) · equity $9,406.74 vs prior close $9,488.16 (-81.42) because holdings re-marked: ABTC×1 yday $8.45 → 09:30 $9.00 +0.55; SBET×1 yday $8.36 → 09:30 $8.16 -0.20; AEM×6 yday $214.08 → 09:30 $200.48 -81.60; ORBS×1554 yday $0.85 → 09:30 $0.85 +0.00; GRAL×17 yday $77.17 → 09:30 $76.58 -10.03; MSTR×11 yday $124.59 → 09:30 $125.56 +10.67; TRON×692 yday $2.10 → 09:30 $2.05 -34.60; XHG×299 yday $4.06 → 09:30 $4.02 -11.96; AUGO×15 yday $85.95 → 09:30 $89.00 +45.75 |
| 2026-08-26 | +2.02 | $101.77 | ABTC×1, SBET×1, AEM×6, ORBS×1554, GRAL×17, MSTR×11, TRON×692, XHG×299, AUGO×15, DEFT×50, AMTX×17, NIQ×1, OMER×1, TRLV×2 | $9,404.50 | +0.00 | — | — | $101.77 | $9,405.28 | ABTC×1, SBET×1, AEM×6, ORBS×1554, GRAL×17, MSTR×11, TRON×692, XHG×299, AUGO×15, DEFT×50, AMTX×17, NIQ×1, OMER×1, TRLV×2 | 09:30 open · cash $101.77 (unchanged overnight, no fees) · equity $9,404.50 vs prior close $9,404.50 (+0.00) because holdings re-marked: ABTC×1 yday $8.70 → 09:30 $8.70 +0.00; SBET×1 yday $8.28 → 09:30 $8.28 +0.00; AEM×6 yday $215.40 → 09:30 $215.40 +0.00; ORBS×1554 yday $0.84 → 09:30 $0.84 +0.00; GRAL×17 yday $76.58 → 09:30 $76.58 +0.00; MSTR×11 yday $121.60 → 09:30 $121.60 +0.00; TRON×692 yday $2.04 → 09:30 $2.04 +0.00; XHG×299 yday $4.05 → 09:30 $4.05 +0.00; AUGO×15 yday $86.85 → 09:30 $86.85 +0.00; DEFT×50 yday $0.62 → 09:30 $0.62 +0.00; AMTX×17 yday $1.86 → 09:30 $1.86 +0.00; NIQ×1 yday $19.46 → 09:30 $19.46 +0.00; OMER×1 yday $19.03 → 09:30 $19.03 +0.00; TRLV×2 yday $11.02 → 09:30 $11.02 +0.00 |
| 2026-08-27 | — | $101.77 | ABTC×1, SBET×1, AEM×6, ORBS×1554, GRAL×17, MSTR×11, TRON×692, XHG×299, AUGO×15, DEFT×50, AMTX×17, NIQ×1, OMER×1, TRLV×2 | $9,435.01 | +29.73 | — | ABTC, SBET | $118.56 | $9,516.64 | AEM×6, ORBS×1554, GRAL×17, MSTR×11, TRON×692, XHG×299, AUGO×15, DEFT×50, AMTX×17, NIQ×1, OMER×1, TRLV×2 | 09:30 open · cash $101.77 (unchanged overnight, no fees) · equity $9,435.01 vs prior close $9,405.28 (+29.73) because holdings re-marked: ABTC×1 yday $8.70 → 09:30 $8.84 +0.14; SBET×1 yday $8.28 → 09:30 $8.16 -0.12; AEM×6 yday $215.40 → 09:30 $219.50 +24.60; ORBS×1554 yday $0.84 → 09:30 $0.80 -62.16; GRAL×17 yday $76.58 → 09:30 $80.88 +73.10; MSTR×11 yday $121.60 → 09:30 $123.26 +18.26; TRON×692 yday $2.04 → 09:30 $2.08 +27.68; XHG×299 yday $4.05 → 09:30 $3.81 -71.76; AUGO×15 yday $86.85 → 09:30 $88.24 +20.85; DEFT×50 yday $0.62 → 09:30 $0.60 -1.00; AMTX×17 yday $1.86 → 09:30 $1.91 +0.85; NIQ×1 yday $19.46 → 09:30 $19.20 -0.26; OMER×1 yday $19.03 → 09:30 $18.96 -0.07; TRLV×2 yday $11.02 → 09:30 $11.22 +0.40 |
| 2026-08-28 | +0.75 | $118.56 | AEM×6, ORBS×1554, GRAL×17, MSTR×11, TRON×692, XHG×299, AUGO×15, DEFT×50, AMTX×17, NIQ×1, OMER×1, TRLV×2 | $9,610.46 | +93.82 | ZYME, ERO, FUTU | AEM, ORBS, GRAL, MSTR, TRON, AUGO | $73.81 | $9,435.98 | XHG×299, DEFT×50, AMTX×17, NIQ×1, OMER×1, TRLV×2, ZYME×93, ERO×70, FUTU×21 | 09:30 open · cash $118.56 (unchanged overnight, no fees) · equity $9,610.46 vs prior close $9,516.64 (+93.82) because holdings re-marked: AEM×6 yday $214.04 → 09:30 $214.11 +0.42; ORBS×1554 yday $0.80 → 09:30 $0.82 +31.08; GRAL×17 yday $79.59 → 09:30 $79.00 -10.03; MSTR×11 yday $123.19 → 09:30 $126.77 +39.38; TRON×692 yday $2.15 → 09:30 $2.21 +41.52; XHG×299 yday $4.06 → 09:30 $4.06 +0.00; AUGO×15 yday $89.30 → 09:30 $88.71 -8.85; DEFT×50 yday $0.59 → 09:30 $0.60 +0.50; AMTX×17 yday $1.88 → 09:30 $1.87 -0.17; NIQ×1 yday $18.74 → 09:30 $18.79 +0.05; OMER×1 yday $18.22 → 09:30 $18.24 +0.02; TRLV×2 yday $11.43 → 09:30 $11.38 -0.10 |
| 2026-08-31 | -5.85 | $73.81 | XHG×299, DEFT×50, AMTX×17, NIQ×1, OMER×1, TRLV×2, ZYME×93, ERO×70, FUTU×21 | $9,138.63 | -297.35 | — | — | $73.81 | $9,156.44 | XHG×299, DEFT×50, AMTX×17, NIQ×1, OMER×1, TRLV×2, ZYME×93, ERO×70, FUTU×21 | 09:30 open · cash $73.81 (unchanged overnight, no fees) · equity $9,138.63 vs prior close $9,435.98 (-297.35) because holdings re-marked: XHG×299 yday $3.80 → 09:30 $3.44 -107.64; DEFT×50 yday $0.65 → 09:30 $0.62 -1.50; AMTX×17 yday $1.87 → 09:30 $1.90 +0.51; NIQ×1 yday $19.07 → 09:30 $19.20 +0.13; OMER×1 yday $19.25 → 09:30 $18.61 -0.64; TRLV×2 yday $11.03 → 09:30 $12.41 +2.76; ZYME×93 yday $29.01 → 09:30 $28.27 -68.82; ERO×70 yday $39.82 → 09:30 $38.60 -85.40; FUTU×21 yday $124.57 → 09:30 $122.82 -36.75 |
| 2026-09-01 | -6.30 | $73.81 | XHG×299, DEFT×50, AMTX×17, NIQ×1, OMER×1, TRLV×2, ZYME×93, ERO×70, FUTU×21 | $9,153.59 | -2.85 | — | AMTX, NIQ | $124.06 | $9,009.57 | XHG×299, DEFT×50, OMER×1, TRLV×2, ZYME×93, ERO×70, FUTU×21 | 09:30 open · cash $73.81 (unchanged overnight, no fees) · equity $9,153.59 vs prior close $9,156.44 (-2.85) because holdings re-marked: XHG×299 yday $3.44 → 09:30 $3.52 +23.92; DEFT×50 yday $0.62 → 09:30 $0.59 -1.50; AMTX×17 yday $1.90 → 09:30 $1.87 -0.51; NIQ×1 yday $19.20 → 09:30 $19.06 -0.14; OMER×1 yday $18.50 → 09:30 $18.79 +0.29; TRLV×2 yday $12.41 → 09:30 $11.89 -1.04; ZYME×93 yday $28.27 → 09:30 $29.32 +97.65; ERO×70 yday $38.49 → 09:30 $37.30 -83.30; FUTU×21 yday $124.04 → 09:30 $122.22 -38.22 |
| 2026-09-02 | -3.83 | $124.06 | XHG×299, DEFT×50, OMER×1, TRLV×2, ZYME×93, ERO×70, FUTU×21 | $8,997.30 | -12.27 | — | — | $124.06 | $8,950.37 | XHG×299, DEFT×50, OMER×1, TRLV×2, ZYME×93, ERO×70, FUTU×21 | 09:30 open · cash $124.06 (unchanged overnight, no fees) · equity $8,997.30 vs prior close $9,009.57 (-12.27) because holdings re-marked: XHG×299 yday $3.43 → 09:30 $3.48 +14.95; DEFT×50 yday $0.61 → 09:30 $0.63 +1.00; OMER×1 yday $18.79 → 09:30 $18.66 -0.13; TRLV×2 yday $11.89 → 09:30 $11.54 -0.70; ZYME×93 yday $29.33 → 09:30 $29.32 -0.93; ERO×70 yday $36.01 → 09:30 $35.95 -4.20; FUTU×21 yday $120.88 → 09:30 $119.82 -22.26 |
| 2026-09-03 | -0.90 | $124.06 | XHG×299, DEFT×50, OMER×1, TRLV×2, ZYME×93, ERO×70, FUTU×21 | $9,059.58 | +109.21 | NVAX, NIQ | — | $5.43 | $8,990.35 | XHG×299, DEFT×50, OMER×1, TRLV×2, ZYME×93, ERO×70, FUTU×21, NVAX×6, NIQ×3 | 09:30 open · cash $124.06 (unchanged overnight, no fees) · equity $9,059.58 vs prior close $8,950.37 (+109.21) because holdings re-marked: XHG×299 yday $3.51 → 09:30 $3.57 +17.94; DEFT×50 yday $0.66 → 09:30 $0.67 +0.50; OMER×1 yday $18.75 → 09:30 $18.97 +0.22; TRLV×2 yday $11.74 → 09:30 $11.78 +0.08; ZYME×93 yday $29.67 → 09:30 $30.00 +30.69; ERO×70 yday $34.82 → 09:30 $35.62 +56.00; FUTU×21 yday $119.28 → 09:30 $119.46 +3.78 |
| 2026-09-04 | — | $5.43 | XHG×299, DEFT×50, OMER×1, TRLV×2, ZYME×93, ERO×70, FUTU×21, NVAX×6, NIQ×3 | $9,113.77 | +123.42 | — | ZYME | $2,917.75 | $9,173.52 | XHG×299, DEFT×50, OMER×1, TRLV×2, ERO×70, FUTU×21, NVAX×6, NIQ×3 | 09:30 open · cash $5.43 (unchanged overnight, no fees) · equity $9,113.77 vs prior close $8,990.35 (+123.42) because holdings re-marked: XHG×299 yday $3.32 → 09:30 $3.38 +17.94; DEFT×50 yday $0.65 → 09:30 $0.65 +0.00; OMER×1 yday $18.86 → 09:30 $18.99 +0.13; TRLV×2 yday $11.69 → 09:30 $11.89 +0.40; ZYME×93 yday $31.05 → 09:30 $31.34 +26.97; ERO×70 yday $34.76 → 09:30 $35.82 +74.20; FUTU×21 yday $118.08 → 09:30 $118.19 +2.31; NVAX×6 yday $10.32 → 09:30 $10.41 +0.54; NIQ×3 yday $18.35 → 09:30 $18.66 +0.93 |

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
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $69.50 | ▼ 09:30 equity $9,474.24 vs yday $9,542.55 (-68.31) | 09:30 open · cash $69.50 (unchanged overnight, no fees) · equity $9,474.24 vs prior close $9,542.55 (-68.31) because holdings re-marked: ADUR×75 yday $15.39 → 09:30 $15.55 +12.00; ANRO×39 yday $34.89 → 09:30 $34.36 -20.67; LIFE×35 yday $32.85 → 09:30 $32.86 +0.35; VOYG×28 yday $38.85 → 09:30 $38.25 -16.80; LUNR×65 yday $18.52 → 09:30 $18.13 -25.35; BETA×49 yday $26.40 → 09:30 $26.16 -11.76; FORM×9 yday $114.95 → 09:30 $114.50 -4.05; ENTG×7 yday $144.28 → 09:30 $144.11 -1.19; OCC×1 yday $14.36 → 09:30 $14.10 -0.26; ALM×2 yday $16.18 → 09:30 $15.81 -0.74; LPTH×2 yday $13.24 → 09:30 $13.09 -0.30; CLYM×2 yday $17.34 → 09:30 $17.16 -0.36; BORR×7 yday $4.40 → 09:30 $4.46 +0.42; IOVA×5 yday $7.99 → 09:30 $8.07 +0.40 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABTC` | 1 | $8.46 | $0.09 | — | $60.95 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+14.0; leftover $8.69 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SBET` | 1 | $7.55 | $0.08 | — | $53.32 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+14.6; leftover $8.69 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.32 | ▲ 09:30 equity $9,624.15 vs yday $9,482.15 (+142.00) | 09:30 open · cash $53.32 (unchanged overnight, no fees) · equity $9,624.15 vs prior close $9,482.15 (+142.00) because holdings re-marked: ADUR×75 yday $15.85 → 09:30 $16.00 +11.25; ANRO×39 yday $34.25 → 09:30 $34.44 +7.41; LIFE×35 yday $33.52 → 09:30 $33.90 +13.30; VOYG×28 yday $37.92 → 09:30 $38.84 +25.76; LUNR×65 yday $17.93 → 09:30 $18.74 +52.65; BETA×49 yday $25.48 → 09:30 $25.56 +3.92; FORM×9 yday $115.71 → 09:30 $117.69 +17.82; ENTG×7 yday $144.63 → 09:30 $145.64 +7.07; OCC×1 yday $14.12 → 09:30 $14.20 +0.08; ALM×2 yday $17.69 → 09:30 $18.00 +0.62; LPTH×2 yday $13.02 → 09:30 $13.10 +0.16; CLYM×2 yday $17.02 → 09:30 $17.26 +0.48; BORR×7 yday $4.43 → 09:30 $4.51 +0.56; IOVA×5 yday $8.99 → 09:30 $9.08 +0.45; ABTC×1 yday $8.47 → 09:30 $8.66 +0.19; SBET×1 yday $7.59 → 09:30 $7.87 +0.28 | — |
| 2026-08-21 09:30 ET | **SELL** | `ADUR` | 75 | $16.00 | $2.24 | $-41.95 | $1,251.09 | ▼ -41.95 after sell → book $9,621.92; vs 09:30 mark -2.23 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ANRO` | 39 | $34.44 | $2.13 | $+99.90 | $2,592.12 | ▲ +99.90 after sell → book $9,619.79; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LIFE` | 35 | $33.90 | $2.12 | $-44.11 | $3,776.50 | ▼ -44.11 after sell → book $9,617.67; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `VOYG` | 28 | $38.84 | $2.09 | $-162.37 | $4,861.93 | ▼ -162.37 after sell → book $9,615.58; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LUNR` | 65 | $18.74 | $2.21 | $-32.34 | $6,077.82 | ▼ -32.34 after sell → book $9,613.37; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BETA` | 49 | $25.56 | $2.16 | $+12.86 | $7,328.11 | ▲ +12.86 after sell → book $9,611.22; vs 09:30 mark -2.15 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `FORM` | 9 | $117.69 | $2.04 | $-110.16 | $8,385.28 | ▼ -110.16 after sell → book $9,609.18; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ENTG` | 7 | $145.64 | $2.03 | $-121.71 | $9,402.73 | ▼ -121.71 after sell → book $9,607.15; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $8,102.92 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1343.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1554 | $0.86 | $18.09 | — | $6,742.17 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1343.25 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 17 | $78.88 | $2.04 | — | $5,399.17 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1343.25 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MSTR` | 11 | $119.69 | $2.02 | — | $4,080.56 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+15.7; leftover $1343.25 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TRON` | 692 | $1.94 | $8.93 | — | $2,729.15 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+15.4; leftover $1343.25 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 299 | $4.49 | $3.86 | — | $1,382.79 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+12.7; leftover $1343.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUGO` | 15 | $89.10 | $2.04 | — | $44.25 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.8; leftover $1343.25 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $44.25 | ▲ 09:30 equity $9,675.37 vs yday $9,593.53 (+81.84) | 09:30 open · cash $44.25 (unchanged overnight, no fees) · equity $9,675.37 vs prior close $9,593.53 (+81.84) because holdings re-marked: OCC×1 yday $13.85 → 09:30 $13.60 -0.25; ALM×2 yday $18.51 → 09:30 $18.69 +0.36; LPTH×2 yday $14.42 → 09:30 $13.92 -1.00; CLYM×2 yday $17.28 → 09:30 $17.27 -0.02; BORR×7 yday $4.50 → 09:30 $4.48 -0.14; IOVA×5 yday $8.29 → 09:30 $8.05 -1.20; ABTC×1 yday $7.93 → 09:30 $8.06 +0.13; SBET×1 yday $7.91 → 09:30 $8.05 +0.14; AEM×6 yday $216.06 → 09:30 $217.03 +5.82; ORBS×1554 yday $0.88 → 09:30 $0.89 +15.54; GRAL×17 yday $79.54 → 09:30 $81.87 +39.61; MSTR×11 yday $119.25 → 09:30 $121.76 +27.61; TRON×692 yday $2.01 → 09:30 $2.02 +6.92; XHG×299 yday $4.41 → 09:30 $4.24 -50.83; AUGO×15 yday $87.26 → 09:30 $89.87 +39.15 | — |
| 2026-08-24 09:30 ET | **SELL** | `OCC` | 1 | $13.60 | $0.16 | $-4.98 | $57.69 | ▼ -4.98 after sell → book $9,675.21; vs 09:30 mark -0.16 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `ALM` | 2 | $18.69 | $0.40 | $+4.25 | $94.67 | ▲ +4.25 after sell → book $9,674.81; vs 09:30 mark -0.40 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `LPTH` | 2 | $13.92 | $0.30 | $-2.65 | $122.21 | ▼ -2.65 after sell → book $9,674.51; vs 09:30 mark -0.30 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `CLYM` | 2 | $17.27 | $0.37 | $+1.34 | $156.38 | ▲ +1.34 after sell → book $9,674.14; vs 09:30 mark -0.37 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `BORR` | 7 | $4.48 | $0.35 | $-1.47 | $187.38 | ▼ -1.47 after sell → book $9,673.78; vs 09:30 mark -0.36 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 5 | $8.05 | $0.44 | $+5.26 | $227.20 | ▲ +5.26 after sell → book $9,673.35; vs 09:30 mark -0.43 | dropped from list after 5 sess (min 5) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $227.20 | ▼ 09:30 equity $9,406.74 vs yday $9,488.16 (-81.42) | 09:30 open · cash $227.20 (unchanged overnight, no fees) · equity $9,406.74 vs prior close $9,488.16 (-81.42) because holdings re-marked: ABTC×1 yday $8.45 → 09:30 $9.00 +0.55; SBET×1 yday $8.36 → 09:30 $8.16 -0.20; AEM×6 yday $214.08 → 09:30 $200.48 -81.60; ORBS×1554 yday $0.85 → 09:30 $0.85 +0.00; GRAL×17 yday $77.17 → 09:30 $76.58 -10.03; MSTR×11 yday $124.59 → 09:30 $125.56 +10.67; TRON×692 yday $2.10 → 09:30 $2.05 -34.60; XHG×299 yday $4.06 → 09:30 $4.02 -11.96; AUGO×15 yday $85.95 → 09:30 $89.00 +45.75 | — |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 50 | $0.64 | $0.47 | — | $194.73 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $32.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AMTX` | 17 | $1.86 | $0.37 | — | $162.74 | — | baseline list, no extra gate; list yday_mover,ohlc_hot; ⚪; ret5=+16.9; leftover $32.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NIQ` | 1 | $19.56 | $0.20 | — | $142.98 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+7.6; leftover $32.46 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `OMER` | 1 | $18.75 | $0.19 | — | $124.04 | — | baseline list, no extra gate; list ohlc_hot; ret5=+12.1; leftover $32.46 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `TRLV` | 2 | $11.02 | $0.23 | — | $101.77 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.0; leftover $32.46 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $101.77 | ▲ 09:30 equity $9,404.50 vs yday $9,404.50 (+0.00) | 09:30 open · cash $101.77 (unchanged overnight, no fees) · equity $9,404.50 vs prior close $9,404.50 (+0.00) because holdings re-marked: ABTC×1 yday $8.70 → 09:30 $8.70 +0.00; SBET×1 yday $8.28 → 09:30 $8.28 +0.00; AEM×6 yday $215.40 → 09:30 $215.40 +0.00; ORBS×1554 yday $0.84 → 09:30 $0.84 +0.00; GRAL×17 yday $76.58 → 09:30 $76.58 +0.00; MSTR×11 yday $121.60 → 09:30 $121.60 +0.00; TRON×692 yday $2.04 → 09:30 $2.04 +0.00; XHG×299 yday $4.05 → 09:30 $4.05 +0.00; AUGO×15 yday $86.85 → 09:30 $86.85 +0.00; DEFT×50 yday $0.62 → 09:30 $0.62 +0.00; AMTX×17 yday $1.86 → 09:30 $1.86 +0.00; NIQ×1 yday $19.46 → 09:30 $19.46 +0.00; OMER×1 yday $19.03 → 09:30 $19.03 +0.00; TRLV×2 yday $11.02 → 09:30 $11.02 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $101.77 | ▲ 09:30 equity $9,435.01 vs yday $9,405.28 (+29.73) | 09:30 open · cash $101.77 (unchanged overnight, no fees) · equity $9,435.01 vs prior close $9,405.28 (+29.73) because holdings re-marked: ABTC×1 yday $8.70 → 09:30 $8.84 +0.14; SBET×1 yday $8.28 → 09:30 $8.16 -0.12; AEM×6 yday $215.40 → 09:30 $219.50 +24.60; ORBS×1554 yday $0.84 → 09:30 $0.80 -62.16; GRAL×17 yday $76.58 → 09:30 $80.88 +73.10; MSTR×11 yday $121.60 → 09:30 $123.26 +18.26; TRON×692 yday $2.04 → 09:30 $2.08 +27.68; XHG×299 yday $4.05 → 09:30 $3.81 -71.76; AUGO×15 yday $86.85 → 09:30 $88.24 +20.85; DEFT×50 yday $0.62 → 09:30 $0.60 -1.00; AMTX×17 yday $1.86 → 09:30 $1.91 +0.85; NIQ×1 yday $19.46 → 09:30 $19.20 -0.26; OMER×1 yday $19.03 → 09:30 $18.96 -0.07; TRLV×2 yday $11.02 → 09:30 $11.22 +0.40 | — |
| 2026-08-27 09:30 ET | **SELL** | `ABTC` | 1 | $8.84 | $0.11 | $+0.18 | $110.50 | ▲ +0.18 after sell → book $9,434.90; vs 09:30 mark -0.11 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `SBET` | 1 | $8.16 | $0.10 | $+0.43 | $118.56 | ▲ +0.43 after sell → book $9,434.80; vs 09:30 mark -0.10 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $118.56 | ▲ 09:30 equity $9,610.46 vs yday $9,516.64 (+93.82) | 09:30 open · cash $118.56 (unchanged overnight, no fees) · equity $9,610.46 vs prior close $9,516.64 (+93.82) because holdings re-marked: AEM×6 yday $214.04 → 09:30 $214.11 +0.42; ORBS×1554 yday $0.80 → 09:30 $0.82 +31.08; GRAL×17 yday $79.59 → 09:30 $79.00 -10.03; MSTR×11 yday $123.19 → 09:30 $126.77 +39.38; TRON×692 yday $2.15 → 09:30 $2.21 +41.52; XHG×299 yday $4.06 → 09:30 $4.06 +0.00; AUGO×15 yday $89.30 → 09:30 $88.71 -8.85; DEFT×50 yday $0.59 → 09:30 $0.60 +0.50; AMTX×17 yday $1.88 → 09:30 $1.87 -0.17; NIQ×1 yday $18.74 → 09:30 $18.79 +0.05; OMER×1 yday $18.22 → 09:30 $18.24 +0.02; TRLV×2 yday $11.43 → 09:30 $11.38 -0.10 | — |
| 2026-08-28 09:30 ET | **SELL** | `AEM` | 6 | $214.11 | $2.03 | $-17.18 | $1,401.19 | ▼ -17.18 after sell → book $9,608.43; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ORBS` | 1554 | $0.82 | $17.67 | $-104.14 | $2,657.80 | ▼ -104.14 after sell → book $9,590.76; vs 09:30 mark -17.67 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRAL` | 17 | $79.00 | $2.06 | $-2.06 | $3,998.73 | ▼ -2.06 after sell → book $9,588.69; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `MSTR` | 11 | $126.77 | $2.04 | $+73.81 | $5,391.16 | ▲ +73.81 after sell → book $9,586.65; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `TRON` | 692 | $2.21 | $9.05 | $+168.86 | $6,911.43 | ▲ +168.86 after sell → book $9,577.60; vs 09:30 mark -9.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUGO` | 15 | $88.71 | $2.06 | $-9.94 | $8,240.02 | ▼ -9.94 after sell → book $9,575.54; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 93 | $29.33 | $2.27 | — | $5,510.06 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $2746.67 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERO` | 70 | $39.20 | $2.20 | — | $2,763.86 | — | baseline list, no extra gate; list ohlc_hot; ret5=+16.6; leftover $2746.67 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `FUTU` | 21 | $128.00 | $2.05 | — | $73.81 | — | baseline list, no extra gate; list ohlc_hot; ret5=+17.5; leftover $2746.67 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $73.81 | ▼ 09:30 equity $9,138.63 vs yday $9,435.98 (-297.35) | 09:30 open · cash $73.81 (unchanged overnight, no fees) · equity $9,138.63 vs prior close $9,435.98 (-297.35) because holdings re-marked: XHG×299 yday $3.80 → 09:30 $3.44 -107.64; DEFT×50 yday $0.65 → 09:30 $0.62 -1.50; AMTX×17 yday $1.87 → 09:30 $1.90 +0.51; NIQ×1 yday $19.07 → 09:30 $19.20 +0.13; OMER×1 yday $19.25 → 09:30 $18.61 -0.64; TRLV×2 yday $11.03 → 09:30 $12.41 +2.76; ZYME×93 yday $29.01 → 09:30 $28.27 -68.82; ERO×70 yday $39.82 → 09:30 $38.60 -85.40; FUTU×21 yday $124.57 → 09:30 $122.82 -36.75 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $73.81 | ▼ 09:30 equity $9,153.59 vs yday $9,156.44 (-2.85) | 09:30 open · cash $73.81 (unchanged overnight, no fees) · equity $9,153.59 vs prior close $9,156.44 (-2.85) because holdings re-marked: XHG×299 yday $3.44 → 09:30 $3.52 +23.92; DEFT×50 yday $0.62 → 09:30 $0.59 -1.50; AMTX×17 yday $1.90 → 09:30 $1.87 -0.51; NIQ×1 yday $19.20 → 09:30 $19.06 -0.14; OMER×1 yday $18.50 → 09:30 $18.79 +0.29; TRLV×2 yday $12.41 → 09:30 $11.89 -1.04; ZYME×93 yday $28.27 → 09:30 $29.32 +97.65; ERO×70 yday $38.49 → 09:30 $37.30 -83.30; FUTU×21 yday $124.04 → 09:30 $122.22 -38.22 | — |
| 2026-09-01 09:30 ET | **SELL** | `AMTX` | 17 | $1.87 | $0.39 | $-0.59 | $105.21 | ▼ -0.59 after sell → book $9,153.20; vs 09:30 mark -0.39 | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `NIQ` | 1 | $19.06 | $0.21 | $-0.91 | $124.06 | ▼ -0.91 after sell → book $9,152.99; vs 09:30 mark -0.21 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.06 | ▼ 09:30 equity $8,997.30 vs yday $9,009.57 (-12.27) | 09:30 open · cash $124.06 (unchanged overnight, no fees) · equity $8,997.30 vs prior close $9,009.57 (-12.27) because holdings re-marked: XHG×299 yday $3.43 → 09:30 $3.48 +14.95; DEFT×50 yday $0.61 → 09:30 $0.63 +1.00; OMER×1 yday $18.79 → 09:30 $18.66 -0.13; TRLV×2 yday $11.89 → 09:30 $11.54 -0.70; ZYME×93 yday $29.33 → 09:30 $29.32 -0.93; ERO×70 yday $36.01 → 09:30 $35.95 -4.20; FUTU×21 yday $120.88 → 09:30 $119.82 -22.26 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.06 | ▲ 09:30 equity $9,059.58 vs yday $8,950.37 (+109.21) | 09:30 open · cash $124.06 (unchanged overnight, no fees) · equity $9,059.58 vs prior close $8,950.37 (+109.21) because holdings re-marked: XHG×299 yday $3.51 → 09:30 $3.57 +17.94; DEFT×50 yday $0.66 → 09:30 $0.67 +0.50; OMER×1 yday $18.75 → 09:30 $18.97 +0.22; TRLV×2 yday $11.74 → 09:30 $11.78 +0.08; ZYME×93 yday $29.67 → 09:30 $30.00 +30.69; ERO×70 yday $34.82 → 09:30 $35.62 +56.00; FUTU×21 yday $119.28 → 09:30 $119.46 +3.78 | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 6 | $10.27 | $0.63 | — | $61.80 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $62.03 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NIQ` | 3 | $18.60 | $0.57 | — | $5.43 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+7.6; leftover $62.03 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.43 | ▲ 09:30 equity $9,113.77 vs yday $8,990.35 (+123.42) | 09:30 open · cash $5.43 (unchanged overnight, no fees) · equity $9,113.77 vs prior close $8,990.35 (+123.42) because holdings re-marked: XHG×299 yday $3.32 → 09:30 $3.38 +17.94; DEFT×50 yday $0.65 → 09:30 $0.65 +0.00; OMER×1 yday $18.86 → 09:30 $18.99 +0.13; TRLV×2 yday $11.69 → 09:30 $11.89 +0.40; ZYME×93 yday $31.05 → 09:30 $31.34 +26.97; ERO×70 yday $34.76 → 09:30 $35.82 +74.20; FUTU×21 yday $118.08 → 09:30 $118.19 +2.31; NVAX×6 yday $10.32 → 09:30 $10.41 +0.54; NIQ×3 yday $18.35 → 09:30 $18.66 +0.93 | — |
| 2026-09-04 09:30 ET | **SELL** | `ZYME` | 93 | $31.34 | $2.31 | $+182.35 | $2,917.75 | ▲ +182.35 after sell → book $9,111.47; vs 09:30 mark -2.30 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `ADUR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ANRO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `LIFE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `VOYG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BETA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `FORM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ENTG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `AAOI` | cash | leftover split 35.81 < 1 share @ 152.64 |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ANRO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `LIFE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `VOYG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `LUNR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BETA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `FORM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ENTG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `OCC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `ALM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `LPTH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `CLYM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `BORR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SMTC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MRVL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AAOI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ELMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `STDN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `ADUR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ANRO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `LIFE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `VOYG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `LUNR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BETA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `FORM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ENTG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `OCC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `ALM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `LPTH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `CLYM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `BORR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `IOVA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRGP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `OABI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `XNCR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PAYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `ADUR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ANRO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `LIFE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `VOYG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `LUNR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BETA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `FORM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ENTG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `OCC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `ALM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `LPTH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `CLYM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `BORR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `IOVA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `AEM` | cash | leftover split 8.69 < 1 share @ 204.45 |
| 2026-08-20 | `TWST` | cash | leftover split 8.69 < 1 share @ 136.84 |
| 2026-08-20 | `HL` | cash | leftover split 8.69 < 1 share @ 20.25 |
| 2026-08-20 | `PPC` | cash | leftover split 8.69 < 1 share @ 30.65 |
| 2026-08-20 | `ABCL` | cash | leftover split 8.69 < 1 share @ 11.81 |
| 2026-08-20 | `SENS` | cash | leftover split 8.69 < 1 share @ 8.91 |
| 2026-08-21 | `OCC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `ALM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `LPTH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `CLYM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `BORR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `IOVA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `SBET` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ABTC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `SBET` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `AEM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `GRAL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `MSTR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `TRON` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AUGO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GUTS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `UEC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NIQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ABTC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `SBET` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `AEM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `GRAL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `MSTR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `TRON` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUGO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ERO` | cash | leftover split 32.46 < 1 share @ 38.00 |
| 2026-08-25 | `FUTU` | cash | leftover split 32.46 < 1 share @ 118.02 |
| 2026-08-26 | `ABTC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `SBET` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `AEM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ORBS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `GRAL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `MSTR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `TRON` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUGO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AMTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ERO` | no_price | no 09:30 open |
| 2026-08-26 | `FUTU` | no_price | no 09:30 open |
| 2026-08-26 | `HOOD` | no_price | no 09:30 open |
| 2026-08-27 | `AEM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ORBS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `GRAL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `MSTR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `TRON` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `XHG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUGO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `DEFT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `AMTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `NIQ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `OMER` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `TRLV` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `AMTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `AMTX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ZYME` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ZYME` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `HOOD` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TXG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `ZYME` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `CVI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HOOD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `ZYME` | min_hold | dropped but min-hold 4/5 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `XHG` | 299 | 2026-08-21 @ $4.49 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+12.7; leftover $1343.25 |
| `DEFT` | 50 | 2026-08-25 @ $0.64 | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $32.46 |
| `OMER` | 1 | 2026-08-25 @ $18.75 | baseline list, no extra gate; list ohlc_hot; ret5=+12.1; leftover $32.46 |
| `TRLV` | 2 | 2026-08-25 @ $11.02 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.0; leftover $32.46 |
| `ERO` | 70 | 2026-08-28 @ $39.20 | baseline list, no extra gate; list ohlc_hot; ret5=+16.6; leftover $2746.67 |
| `FUTU` | 21 | 2026-08-28 @ $128.00 | baseline list, no extra gate; list ohlc_hot; ret5=+17.5; leftover $2746.67 |
| `NVAX` | 6 | 2026-09-03 @ $10.27 | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $62.03 |
| `NIQ` | 3 | 2026-09-03 @ $18.60 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+7.6; leftover $62.03 |
