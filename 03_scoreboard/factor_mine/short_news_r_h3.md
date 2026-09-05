# Factor mine action — `short_news_r_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · news🔴

Cash book **+6.97%** ($10,697) · signal-only (no cash/fees) was +10.62%. Starts YES **17/17**. Fills 54 · skips 70 · realized $+673.06.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news=bad` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $23,731.41.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | EU, LUNR, OWL | — | $14,954.53 | $10,010.33 | EU×1412, LUNR×86, OWL×131 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $14,954.53 | EU×1412, LUNR×86, OWL×131 | $9,916.79 | -93.54 | VERI, ZNTL, APMD, HIVE | — | $19,879.09 | $10,014.29 | EU×1412, LUNR×86, OWL×131, VERI×1077, ZNTL×348, APMD×39, HIVE×411 | 09:30 open · cash $14,954.53 (unchanged overnight, no fees) · equity $9,916.79 vs prior close $10,010.33 (-93.54) because holdings re-marked: EU×1412 yday $1.21 → 09:30 $1.21 +0.00; LUNR×86 yday $19.01 → 09:30 $20.25 -106.64; OWL×131 yday $12.22 → 09:30 $12.12 +13.10 |
| 2026-08-18 | -6.20 | $19,879.09 | EU×1412, LUNR×86, OWL×131, VERI×1077, ZNTL×348, APMD×39, HIVE×411 | $10,177.57 | +163.28 | — | — | $19,879.09 | $10,453.88 | EU×1412, LUNR×86, OWL×131, VERI×1077, ZNTL×348, APMD×39, HIVE×411 | 09:30 open · cash $19,879.09 (unchanged overnight, no fees) · equity $10,177.57 vs prior close $10,014.29 (+163.28) because holdings re-marked: EU×1412 yday $1.13 → 09:30 $1.13 +0.00; LUNR×86 yday $20.38 → 09:30 $19.31 +92.02; OWL×131 yday $11.66 → 09:30 $11.54 +15.72; VERI×1077 yday $1.08 → 09:30 $1.05 +37.69; ZNTL×348 yday $3.71 → 09:30 $3.75 -15.66; APMD×39 yday $32.55 → 09:30 $32.85 -11.70; HIVE×411 yday $3.07 → 09:30 $2.96 +45.21 |
| 2026-08-19 | -7.20 | $19,879.09 | EU×1412, LUNR×86, OWL×131, VERI×1077, ZNTL×348, APMD×39, HIVE×411 | $10,415.59 | -38.29 | — | EU, OWL | $16,808.40 | $10,437.75 | LUNR×86, VERI×1077, ZNTL×348, APMD×39, HIVE×411 | 09:30 open · cash $19,879.09 (unchanged overnight, no fees) · equity $10,415.59 vs prior close $10,453.88 (-38.29) because holdings re-marked: EU×1412 yday $1.07 → 09:30 $1.07 +0.00; LUNR×86 yday $19.31 → 09:30 $18.98 +28.38; OWL×131 yday $11.59 → 09:30 $11.75 -20.96; VERI×1077 yday $0.99 → 09:30 $1.00 -5.39; ZNTL×348 yday $3.68 → 09:30 $3.76 -27.84; APMD×39 yday $31.81 → 09:30 $32.13 -12.48; HIVE×411 yday $2.78 → 09:30 $2.78 +0.00 |
| 2026-08-20 | +1.12 | $16,808.40 | LUNR×86, VERI×1077, ZNTL×348, APMD×39, HIVE×411 | $10,359.47 | -78.28 | AEM, WYFI, TOYO, ABCL, TEAM, AAP, WMT, AQST | LUNR, VERI, ZNTL, APMD, HIVE | $15,262.91 | $10,385.89 | AEM×3, WYFI×30, TOYO×145, ABCL×54, TEAM×3, AAP×13, WMT×6, AQST×140 | 09:30 open · cash $16,808.40 (unchanged overnight, no fees) · equity $10,359.47 vs prior close $10,437.75 (-78.28) because holdings re-marked: LUNR×86 yday $18.52 → 09:30 $18.13 +33.54; VERI×1077 yday $0.97 → 09:30 $0.96 +3.23; ZNTL×348 yday $3.82 → 09:30 $4.01 -67.86; APMD×39 yday $32.03 → 09:30 $31.87 +6.24; HIVE×411 yday $2.82 → 09:30 $2.95 -53.43 |
| 2026-08-21 | +3.25 | $15,262.91 | AEM×3, WYFI×30, TOYO×145, ABCL×54, TEAM×3, AAP×13, WMT×6, AQST×140 | $10,332.70 | -53.19 | QTRX, MRNA, AUGO, SSRM, ARIS, NOG | — | $20,246.40 | $10,307.77 | AEM×3, WYFI×30, TOYO×145, ABCL×54, TEAM×3, AAP×13, WMT×6, AQST×140, QTRX×276, MRNA×6, AUGO×9, SSRM×22, ARIS×41, NOG×31 | 09:30 open · cash $15,262.91 (unchanged overnight, no fees) · equity $10,332.70 vs prior close $10,385.89 (-53.19) because holdings re-marked: AEM×3 yday $212.04 → 09:30 $216.30 -12.78; WYFI×30 yday $21.16 → 09:30 $21.54 -11.40; TOYO×145 yday $4.51 → 09:30 $4.68 -23.93; ABCL×54 yday $11.57 → 09:30 $11.57 +0.00; TEAM×3 yday $174.91 → 09:30 $174.22 +2.07; AAP×13 yday $42.39 → 09:30 $42.41 -0.26; WMT×6 yday $103.59 → 09:30 $103.69 -0.59; AQST×140 yday $4.50 → 09:30 $4.54 -6.30 |
| 2026-08-24 | -5.17 | $20,246.40 | AEM×3, WYFI×30, TOYO×145, ABCL×54, TEAM×3, AAP×13, WMT×6, AQST×140, QTRX×276, MRNA×6, AUGO×9, SSRM×22, ARIS×41, NOG×31 | $10,357.41 | +49.64 | — | — | $20,246.40 | $10,511.92 | AEM×3, WYFI×30, TOYO×145, ABCL×54, TEAM×3, AAP×13, WMT×6, AQST×140, QTRX×276, MRNA×6, AUGO×9, SSRM×22, ARIS×41, NOG×31 | 09:30 open · cash $20,246.40 (unchanged overnight, no fees) · equity $10,357.41 vs prior close $10,307.77 (+49.64) because holdings re-marked: AEM×3 yday $216.06 → 09:30 $217.03 -2.91; WYFI×30 yday $20.72 → 09:30 $20.02 +21.00; TOYO×145 yday $4.82 → 09:30 $4.58 +34.80; ABCL×54 yday $11.32 → 09:30 $10.97 +18.90; TEAM×3 yday $171.81 → 09:30 $169.24 +7.71; AAP×13 yday $42.58 → 09:30 $43.10 -6.76; WMT×6 yday $103.70 → 09:30 $104.16 -2.76; AQST×140 yday $4.66 → 09:30 $4.67 -1.40; QTRX×276 yday $2.99 → 09:30 $2.98 +2.76; MRNA×6 yday $145.13 → 09:30 $142.70 +14.58; AUGO×9 yday $87.26 → 09:30 $89.87 -23.49; SSRM×22 yday $37.77 → 09:30 $38.48 -15.62; ARIS×41 yday $20.86 → 09:30 $20.98 -4.92; NOG×31 yday $27.34 → 09:30 $27.09 +7.75 |
| 2026-08-25 | +1.80 | $20,246.40 | AEM×3, WYFI×30, TOYO×145, ABCL×54, TEAM×3, AAP×13, WMT×6, AQST×140, QTRX×276, MRNA×6, AUGO×9, SSRM×22, ARIS×41, NOG×31 | $10,483.21 | -28.71 | BMO, AVAH | AEM, WYFI, TOYO, ABCL, TEAM, AAP, WMT, AQST | $20,580.94 | $10,436.11 | QTRX×276, MRNA×6, AUGO×9, SSRM×22, ARIS×41, NOG×31, BMO×15, AVAH×190 | 09:30 open · cash $20,246.40 (unchanged overnight, no fees) · equity $10,483.21 vs prior close $10,511.92 (-28.71) because holdings re-marked: AEM×3 yday $214.08 → 09:30 $200.48 +40.80; WYFI×30 yday $20.79 → 09:30 $20.98 -5.70; TOYO×145 yday $4.61 → 09:30 $4.48 +18.85; ABCL×54 yday $10.52 → 09:30 $10.77 -13.50; TEAM×3 yday $171.35 → 09:30 $170.65 +2.10; AAP×13 yday $43.83 → 09:30 $43.61 +2.86; WMT×6 yday $104.92 → 09:30 $106.54 -9.72; AQST×140 yday $4.66 → 09:30 $4.66 +0.00; QTRX×276 yday $2.76 → 09:30 $2.80 -11.04; MRNA×6 yday $139.27 → 09:30 $141.19 -11.52; AUGO×9 yday $85.95 → 09:30 $89.00 -27.45; SSRM×22 yday $37.65 → 09:30 $38.63 -21.56; ARIS×41 yday $20.63 → 09:30 $20.75 -4.92; NOG×31 yday $26.49 → 09:30 $26.10 +12.09 |
| 2026-08-26 | +2.02 | $20,580.94 | QTRX×276, MRNA×6, AUGO×9, SSRM×22, ARIS×41, NOG×31, BMO×15, AVAH×190 | $10,436.11 | +0.00 | — | — | $20,580.94 | $10,461.29 | QTRX×276, MRNA×6, AUGO×9, SSRM×22, ARIS×41, NOG×31, BMO×15, AVAH×190 | 09:30 open · cash $20,580.94 (unchanged overnight, no fees) · equity $10,436.11 vs prior close $10,436.11 (+0.00) because holdings re-marked: QTRX×276 yday $2.80 → 09:30 $2.80 +0.00; MRNA×6 yday $141.51 → 09:30 $141.51 +0.00; AUGO×9 yday $86.85 → 09:30 $86.85 +0.00; SSRM×22 yday $38.10 → 09:30 $38.10 +0.00; ARIS×41 yday $20.82 → 09:30 $20.82 +0.00; NOG×31 yday $26.50 → 09:30 $26.50 +0.00; BMO×15 yday $175.00 → 09:30 $175.00 +0.00; AVAH×190 yday $13.70 → 09:30 $13.70 +0.00 |
| 2026-08-27 | — | $20,580.94 | QTRX×276, MRNA×6, AUGO×9, SSRM×22, ARIS×41, NOG×31, BMO×15, AVAH×190 | $10,397.18 | -64.11 | — | QTRX, MRNA, AUGO, SSRM, ARIS, NOG | $15,575.14 | $10,393.84 | BMO×15, AVAH×190 | 09:30 open · cash $20,580.94 (unchanged overnight, no fees) · equity $10,397.18 vs prior close $10,461.29 (-64.11) because holdings re-marked: QTRX×276 yday $2.80 → 09:30 $2.83 -8.28; MRNA×6 yday $141.51 → 09:30 $154.20 -76.14; AUGO×9 yday $86.85 → 09:30 $88.24 -12.51; SSRM×22 yday $38.10 → 09:30 $38.41 -6.82; ARIS×41 yday $20.82 → 09:30 $20.50 +13.12; NOG×31 yday $26.50 → 09:30 $26.00 +15.50; BMO×15 yday $175.00 → 09:30 $173.22 +26.70; AVAH×190 yday $13.70 → 09:30 $13.65 +9.50 |
| 2026-08-28 | +0.75 | $15,575.14 | BMO×15, AVAH×190 | $10,394.59 | +0.75 | SIMO, NOG | BMO, AVAH | $15,406.49 | $10,502.77 | SIMO×9, NOG×100 | 09:30 open · cash $15,575.14 (unchanged overnight, no fees) · equity $10,394.59 vs prior close $10,393.84 (+0.75) because holdings re-marked: BMO×15 yday $172.90 → 09:30 $172.85 +0.75; AVAH×190 yday $13.62 → 09:30 $13.62 +0.00 |
| 2026-08-31 | -5.85 | $15,406.49 | SIMO×9, NOG×100 | $10,612.38 | +109.61 | — | — | $15,406.49 | $10,612.38 | SIMO×9, NOG×100 | 09:30 open · cash $15,406.49 (unchanged overnight, no fees) · equity $10,612.38 vs prior close $10,502.77 (+109.61) because holdings re-marked: SIMO×9 yday $255.08 → 09:30 $246.79 +74.61; NOG×100 yday $26.08 → 09:30 $25.73 +35.00 |
| 2026-09-01 | -6.30 | $15,406.49 | SIMO×9, NOG×100 | $10,542.72 | -69.66 | — | — | $15,406.49 | $10,599.69 | SIMO×9, NOG×100 | 09:30 open · cash $15,406.49 (unchanged overnight, no fees) · equity $10,542.72 vs prior close $10,612.38 (-69.66) because holdings re-marked: SIMO×9 yday $246.79 → 09:30 $247.53 -6.66; NOG×100 yday $25.73 → 09:30 $26.36 -63.00 |
| 2026-09-02 | -3.83 | $15,406.49 | SIMO×9, NOG×100 | $10,593.68 | -6.01 | — | SIMO | $13,243.66 | $10,607.66 | NOG×100 | 09:30 open · cash $15,406.49 (unchanged overnight, no fees) · equity $10,593.68 vs prior close $10,599.69 (-6.01) because holdings re-marked: SIMO×9 yday $241.20 → 09:30 $240.09 +9.99; NOG×100 yday $26.36 → 09:30 $26.52 -16.00 |
| 2026-09-03 | -0.90 | $13,243.66 | NOG×100 | $10,633.66 | +26.00 | SLN, NIQ, TX | — | $18,508.99 | $10,567.97 | NOG×100, SLN×120, NIQ×95, TX×31 | 09:30 open · cash $13,243.66 (unchanged overnight, no fees) · equity $10,633.66 vs prior close $10,607.66 (+26.00) because holdings re-marked: NOG×100 yday $26.36 → 09:30 $26.10 +26.00 |
| 2026-09-04 | — | $18,508.99 | NOG×100, SLN×120, NIQ×95, TX×31 | $10,505.66 | -62.31 | GSM, OPK | — | $23,731.41 | $10,697.17 | NOG×100, SLN×120, NIQ×95, TX×31, GSM×577, OPK×1535 | 09:30 open · cash $18,508.99 (unchanged overnight, no fees) · equity $10,505.66 vs prior close $10,567.97 (-62.31) because holdings re-marked: NOG×100 yday $26.60 → 09:30 $26.59 +1.00; SLN×120 yday $14.79 → 09:30 $14.85 -7.20; NIQ×95 yday $18.35 → 09:30 $18.66 -29.45; TX×31 yday $56.87 → 09:30 $57.73 -26.66 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 1412 | $1.18 | $18.51 | — | $11,647.65 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 86 | $19.17 | $2.32 | — | $13,293.95 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 131 | $12.70 | $2.46 | — | $14,954.53 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 judge🟢 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,954.53 | ▼ 09:30 equity $9,916.79 vs yday $10,010.33 (-93.54) | 09:30 open · cash $14,954.53 (unchanged overnight, no fees) · equity $9,916.79 vs prior close $10,010.33 (-93.54) because holdings re-marked: EU×1412 yday $1.21 → 09:30 $1.21 +0.00; LUNR×86 yday $19.01 → 09:30 $20.25 -106.64; OWL×131 yday $12.22 → 09:30 $12.12 +13.10 | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 1077 | $1.15 | $14.12 | — | $16,178.97 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; leftover $1239.60 | join🟡 sector🟢 gen🟢 news🔴 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 348 | $3.56 | $4.59 | — | $17,413.26 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; leftover $1239.60 | join🟡 sector🔴 gen🟢 news🔴 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 39 | $31.70 | $2.16 | — | $18,647.39 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; leftover $1239.60 | join🟡 sector🔴 gen🟢 news🔴 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 411 | $3.01 | $5.41 | — | $19,879.09 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; leftover $1239.60 | join🟢 sector🟢 gen🟢 news🔴 judge🟢 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,879.09 | ▲ 09:30 equity $10,177.57 vs yday $10,014.29 (+163.28) | 09:30 open · cash $19,879.09 (unchanged overnight, no fees) · equity $10,177.57 vs prior close $10,014.29 (+163.28) because holdings re-marked: EU×1412 yday $1.13 → 09:30 $1.13 +0.00; LUNR×86 yday $20.38 → 09:30 $19.31 +92.02; OWL×131 yday $11.66 → 09:30 $11.54 +15.72; VERI×1077 yday $1.08 → 09:30 $1.05 +37.69; ZNTL×348 yday $3.71 → 09:30 $3.75 -15.66; APMD×39 yday $32.55 → 09:30 $32.85 -11.70; HIVE×411 yday $3.07 → 09:30 $2.96 +45.21 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,879.09 | ▼ 09:30 equity $10,415.59 vs yday $10,453.88 (-38.29) | 09:30 open · cash $19,879.09 (unchanged overnight, no fees) · equity $10,415.59 vs prior close $10,453.88 (-38.29) because holdings re-marked: EU×1412 yday $1.07 → 09:30 $1.07 +0.00; LUNR×86 yday $19.31 → 09:30 $18.98 +28.38; OWL×131 yday $11.59 → 09:30 $11.75 -20.96; VERI×1077 yday $0.99 → 09:30 $1.00 -5.39; ZNTL×348 yday $3.68 → 09:30 $3.76 -27.84; APMD×39 yday $31.81 → 09:30 $32.13 -12.48; HIVE×411 yday $2.78 → 09:30 $2.78 +0.00 | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 1412 | $1.07 | $18.21 | $+118.60 | $18,350.04 | ▲ +118.60 after sell → book $10,397.38; vs 09:30 mark -18.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 131 | $11.75 | $2.38 | $+118.95 | $16,808.40 | ▲ +118.95 after sell → book $10,394.99; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,808.40 | ▼ 09:30 equity $10,359.47 vs yday $10,437.75 (-78.28) | 09:30 open · cash $16,808.40 (unchanged overnight, no fees) · equity $10,359.47 vs prior close $10,437.75 (-78.28) because holdings re-marked: LUNR×86 yday $18.52 → 09:30 $18.13 +33.54; VERI×1077 yday $0.97 → 09:30 $0.96 +3.23; ZNTL×348 yday $3.82 → 09:30 $4.01 -67.86; APMD×39 yday $32.03 → 09:30 $31.87 +6.24; HIVE×411 yday $2.82 → 09:30 $2.95 -53.43 | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 86 | $18.13 | $2.25 | $+84.87 | $15,246.97 | ▲ +84.87 after sell → book $10,357.22; vs 09:30 mark -2.25 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 1077 | $0.96 | $13.60 | $+173.68 | $14,196.22 | ▲ +173.68 after sell → book $10,343.62; vs 09:30 mark -13.60 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 348 | $4.01 | $4.49 | $-167.42 | $12,794.51 | ▼ -167.42 after sell → book $10,339.13; vs 09:30 mark -4.49 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 39 | $31.87 | $2.11 | $-10.90 | $11,549.48 | ▼ -10.90 after sell → book $10,337.03; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 411 | $2.95 | $5.30 | $+13.94 | $10,331.72 | ▲ +13.94 after sell → book $10,331.72; vs 09:30 mark -5.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $10,943.04 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $645.73 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 30 | $21.40 | $2.12 | — | $11,582.92 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; leftover $645.73 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 145 | $4.43 | $2.48 | — | $12,222.79 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; leftover $645.73 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 54 | $11.81 | $2.19 | — | $12,858.61 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $645.73 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 3 | $173.90 | $2.03 | — | $13,378.28 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; leftover $645.73 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 13 | $46.85 | $2.07 | — | $13,985.26 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=+5.0; leftover $645.73 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 6 | $106.13 | $2.05 | — | $14,619.97 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; leftover $645.73 | join🟢 sector🟡 gen🟢 news🔴 digest🟡 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 140 | $4.61 | $2.46 | — | $15,262.91 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; leftover $645.73 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,262.91 | ▼ 09:30 equity $10,332.70 vs yday $10,385.89 (-53.19) | 09:30 open · cash $15,262.91 (unchanged overnight, no fees) · equity $10,332.70 vs prior close $10,385.89 (-53.19) because holdings re-marked: AEM×3 yday $212.04 → 09:30 $216.30 -12.78; WYFI×30 yday $21.16 → 09:30 $21.54 -11.40; TOYO×145 yday $4.51 → 09:30 $4.68 -23.93; ABCL×54 yday $11.57 → 09:30 $11.57 +0.00; TEAM×3 yday $174.91 → 09:30 $174.22 +2.07; AAP×13 yday $42.39 → 09:30 $42.41 -0.26; WMT×6 yday $103.59 → 09:30 $103.69 -0.59; AQST×140 yday $4.50 → 09:30 $4.54 -6.30 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 276 | $3.11 | $3.64 | — | $16,117.63 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $861.06 | join🟡 sector🟢 gen🟢 news🔴 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 6 | $133.11 | $2.05 | — | $16,914.24 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; leftover $861.06 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 9 | $89.10 | $2.06 | — | $17,714.09 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $861.06 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 22 | $38.40 | $2.10 | — | $18,556.79 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $861.06 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 41 | $20.90 | $2.16 | — | $19,411.53 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $861.06 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 31 | $27.00 | $2.13 | — | $20,246.40 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; leftover $861.06 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,246.40 | ▲ 09:30 equity $10,357.41 vs yday $10,307.77 (+49.64) | 09:30 open · cash $20,246.40 (unchanged overnight, no fees) · equity $10,357.41 vs prior close $10,307.77 (+49.64) because holdings re-marked: AEM×3 yday $216.06 → 09:30 $217.03 -2.91; WYFI×30 yday $20.72 → 09:30 $20.02 +21.00; TOYO×145 yday $4.82 → 09:30 $4.58 +34.80; ABCL×54 yday $11.32 → 09:30 $10.97 +18.90; TEAM×3 yday $171.81 → 09:30 $169.24 +7.71; AAP×13 yday $42.58 → 09:30 $43.10 -6.76; WMT×6 yday $103.70 → 09:30 $104.16 -2.76; AQST×140 yday $4.66 → 09:30 $4.67 -1.40; QTRX×276 yday $2.99 → 09:30 $2.98 +2.76; MRNA×6 yday $145.13 → 09:30 $142.70 +14.58; AUGO×9 yday $87.26 → 09:30 $89.87 -23.49; SSRM×22 yday $37.77 → 09:30 $38.48 -15.62; ARIS×41 yday $20.86 → 09:30 $20.98 -4.92; NOG×31 yday $27.34 → 09:30 $27.09 +7.75 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,246.40 | ▼ 09:30 equity $10,483.21 vs yday $10,511.92 (-28.71) | 09:30 open · cash $20,246.40 (unchanged overnight, no fees) · equity $10,483.21 vs prior close $10,511.92 (-28.71) because holdings re-marked: AEM×3 yday $214.08 → 09:30 $200.48 +40.80; WYFI×30 yday $20.79 → 09:30 $20.98 -5.70; TOYO×145 yday $4.61 → 09:30 $4.48 +18.85; ABCL×54 yday $10.52 → 09:30 $10.77 -13.50; TEAM×3 yday $171.35 → 09:30 $170.65 +2.10; AAP×13 yday $43.83 → 09:30 $43.61 +2.86; WMT×6 yday $104.92 → 09:30 $106.54 -9.72; AQST×140 yday $4.66 → 09:30 $4.66 +0.00; QTRX×276 yday $2.76 → 09:30 $2.80 -11.04; MRNA×6 yday $139.27 → 09:30 $141.19 -11.52; AUGO×9 yday $85.95 → 09:30 $89.00 -27.45; SSRM×22 yday $37.65 → 09:30 $38.63 -21.56; ARIS×41 yday $20.63 → 09:30 $20.75 -4.92; NOG×31 yday $26.49 → 09:30 $26.10 +12.09 | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 3 | $200.48 | $2.00 | $+7.88 | $19,642.96 | ▲ +7.88 after sell → book $10,481.21; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 30 | $20.98 | $2.08 | $+8.40 | $19,011.48 | ▲ +8.40 after sell → book $10,479.13; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 145 | $4.48 | $2.42 | $-12.15 | $18,359.46 | ▼ -12.15 after sell → book $10,476.71; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 54 | $10.77 | $2.15 | $+52.09 | $17,775.73 | ▲ +52.09 after sell → book $10,474.56; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 3 | $170.65 | $2.00 | $+5.72 | $17,261.78 | ▲ +5.72 after sell → book $10,472.56; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 13 | $43.61 | $2.03 | $+38.03 | $16,692.82 | ▲ +38.03 after sell → book $10,470.53; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 6 | $106.54 | $2.01 | $-6.54 | $16,051.57 | ▼ -6.54 after sell → book $10,468.52; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 140 | $4.66 | $2.41 | $-11.87 | $15,396.76 | ▼ -11.87 after sell → book $10,466.11; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 15 | $172.40 | $2.14 | — | $17,980.63 | — | news🔴; gate news=bad; list earn_react; ret5=-6.1; leftover $2616.53 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 190 | $13.70 | $2.68 | — | $20,580.94 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+6.8; leftover $2616.53 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,580.94 | ▲ 09:30 equity $10,436.11 vs yday $10,436.11 (+0.00) | 09:30 open · cash $20,580.94 (unchanged overnight, no fees) · equity $10,436.11 vs prior close $10,436.11 (+0.00) because holdings re-marked: QTRX×276 yday $2.80 → 09:30 $2.80 +0.00; MRNA×6 yday $141.51 → 09:30 $141.51 +0.00; AUGO×9 yday $86.85 → 09:30 $86.85 +0.00; SSRM×22 yday $38.10 → 09:30 $38.10 +0.00; ARIS×41 yday $20.82 → 09:30 $20.82 +0.00; NOG×31 yday $26.50 → 09:30 $26.50 +0.00; BMO×15 yday $175.00 → 09:30 $175.00 +0.00; AVAH×190 yday $13.70 → 09:30 $13.70 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,580.94 | ▼ 09:30 equity $10,397.18 vs yday $10,461.29 (-64.11) | 09:30 open · cash $20,580.94 (unchanged overnight, no fees) · equity $10,397.18 vs prior close $10,461.29 (-64.11) because holdings re-marked: QTRX×276 yday $2.80 → 09:30 $2.83 -8.28; MRNA×6 yday $141.51 → 09:30 $154.20 -76.14; AUGO×9 yday $86.85 → 09:30 $88.24 -12.51; SSRM×22 yday $38.10 → 09:30 $38.41 -6.82; ARIS×41 yday $20.82 → 09:30 $20.50 +13.12; NOG×31 yday $26.50 → 09:30 $26.00 +15.50; BMO×15 yday $175.00 → 09:30 $173.22 +26.70; AVAH×190 yday $13.70 → 09:30 $13.65 +9.50 | — |
| 2026-08-27 09:30 ET | **COVER** | `QTRX` | 276 | $2.83 | $3.56 | $+70.08 | $19,796.30 | ▲ +70.08 after sell → book $10,393.62; vs 09:30 mark -3.56 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `MRNA` | 6 | $154.20 | $2.01 | $-130.60 | $18,869.09 | ▼ -130.60 after sell → book $10,391.61; vs 09:30 mark -2.01 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `AUGO` | 9 | $88.24 | $2.02 | $+3.66 | $18,072.92 | ▲ +3.66 after sell → book $10,389.60; vs 09:30 mark -2.01 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `SSRM` | 22 | $38.41 | $2.06 | $-4.38 | $17,225.84 | ▼ -4.38 after sell → book $10,387.54; vs 09:30 mark -2.06 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `ARIS` | 41 | $20.50 | $2.11 | $+12.13 | $16,383.23 | ▲ +12.13 after sell → book $10,385.43; vs 09:30 mark -2.11 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `NOG` | 31 | $26.00 | $2.08 | $+26.79 | $15,575.14 | ▲ +26.79 after sell → book $10,383.34; vs 09:30 mark -2.09 | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,575.14 | ▲ 09:30 equity $10,394.59 vs yday $10,393.84 (+0.75) | 09:30 open · cash $15,575.14 (unchanged overnight, no fees) · equity $10,394.59 vs prior close $10,393.84 (+0.75) because holdings re-marked: BMO×15 yday $172.90 → 09:30 $172.85 +0.75; AVAH×190 yday $13.62 → 09:30 $13.62 +0.00 | — |
| 2026-08-28 09:30 ET | **COVER** | `BMO` | 15 | $172.85 | $2.04 | $-10.92 | $12,980.36 | ▼ -10.92 after sell → book $10,392.56; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 190 | $13.62 | $2.56 | $+9.96 | $10,390.00 | ▲ +9.96 after sell → book $10,390.00; vs 09:30 mark -2.56 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 9 | $272.00 | $2.11 | — | $12,835.89 | — | news🔴; gate news=bad; list yday_gainer; ⚪; ret5=-3.9; leftover $2597.50 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `NOG` | 100 | $25.73 | $2.40 | — | $15,406.49 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.6; leftover $2597.50 | join🔴 sector🔴 gen🟡 news🔴 digest🟢 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,406.49 | ▲ 09:30 equity $10,612.38 vs yday $10,502.77 (+109.61) | 09:30 open · cash $15,406.49 (unchanged overnight, no fees) · equity $10,612.38 vs prior close $10,502.77 (+109.61) because holdings re-marked: SIMO×9 yday $255.08 → 09:30 $246.79 +74.61; NOG×100 yday $26.08 → 09:30 $25.73 +35.00 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,406.49 | ▼ 09:30 equity $10,542.72 vs yday $10,612.38 (-69.66) | 09:30 open · cash $15,406.49 (unchanged overnight, no fees) · equity $10,542.72 vs prior close $10,612.38 (-69.66) because holdings re-marked: SIMO×9 yday $246.79 → 09:30 $247.53 -6.66; NOG×100 yday $25.73 → 09:30 $26.36 -63.00 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,406.49 | ▼ 09:30 equity $10,593.68 vs yday $10,599.69 (-6.01) | 09:30 open · cash $15,406.49 (unchanged overnight, no fees) · equity $10,593.68 vs prior close $10,599.69 (-6.01) because holdings re-marked: SIMO×9 yday $241.20 → 09:30 $240.09 +9.99; NOG×100 yday $26.36 → 09:30 $26.52 -16.00 | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 9 | $240.09 | $2.02 | $+283.06 | $13,243.66 | ▲ +283.06 after sell → book $10,591.66; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,243.66 | ▲ 09:30 equity $10,633.66 vs yday $10,607.66 (+26.00) | 09:30 open · cash $13,243.66 (unchanged overnight, no fees) · equity $10,633.66 vs prior close $10,607.66 (+26.00) because holdings re-marked: NOG×100 yday $26.36 → 09:30 $26.10 +26.00 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 120 | $14.70 | $2.43 | — | $15,005.23 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1772.28 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `NIQ` | 95 | $18.60 | $2.35 | — | $16,769.88 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+7.6; leftover $1772.28 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `TX` | 31 | $56.17 | $2.15 | — | $18,508.99 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+3.0; leftover $1772.28 | join🔴 sector🟢 gen🟡 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,508.99 | ▼ 09:30 equity $10,505.66 vs yday $10,567.97 (-62.31) | 09:30 open · cash $18,508.99 (unchanged overnight, no fees) · equity $10,505.66 vs prior close $10,567.97 (-62.31) because holdings re-marked: NOG×100 yday $26.60 → 09:30 $26.59 +1.00; SLN×120 yday $14.79 → 09:30 $14.85 -7.20; NIQ×95 yday $18.35 → 09:30 $18.66 -29.45; TX×31 yday $56.87 → 09:30 $57.73 -26.66 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 577 | $4.55 | $7.63 | — | $21,126.71 | — | news🔴; gate news=bad; list yday_gainer; ret5=-7.1; leftover $2626.42 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `OPK` | 1535 | $1.71 | $20.15 | — | $23,731.41 | — | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+7.2; leftover $2626.42 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `EU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LUNR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `OWL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `EU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LUNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OWL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VERI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ZNTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `APMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `HIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `RNW` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `VERI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ZNTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `APMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `HIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WYFI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TOYO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TEAM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WMT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AQST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WYFI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TOYO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TEAM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WMT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AQST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `QTRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `SSRM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARIS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EU` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `QTRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `SSRM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARIS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `QTRX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `MRNA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AUGO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ARIS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BE` | no_price | no 09:30 open |
| 2026-08-27 | `BMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NIQ` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ARIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CELH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ARIS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `NIQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `TX` | min_hold | dropped but min-hold 1/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `NOG` | 100 | 2026-08-28 @ $25.73 | news🔴; gate news=bad; list ohlc_hot; ret5=+11.6; leftover $2597.50 |
| `SLN` | 120 | 2026-09-03 @ $14.70 | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1772.28 |
| `NIQ` | 95 | 2026-09-03 @ $18.60 | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+7.6; leftover $1772.28 |
| `TX` | 31 | 2026-09-03 @ $56.17 | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+3.0; leftover $1772.28 |
| `GSM` | 577 | 2026-09-04 @ $4.55 | news🔴; gate news=bad; list yday_gainer; ret5=-7.1; leftover $2626.42 |
| `OPK` | 1535 | 2026-09-04 @ $1.71 | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+7.2; leftover $2626.42 |
