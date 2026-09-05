# Factor mine action — `short_news_r_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · news🔴

Cash book **+0.28%** ($10,028) · signal-only (no cash/fees) was +7.11%. Starts YES **17/17**. Fills 59 · skips 21 · realized $-108.78.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news=bad` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $16,021.55.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | EU, LUNR, OWL | — | $14,954.53 | $10,010.33 | EU×1412, LUNR×86, OWL×131 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $14,954.53 | EU×1412, LUNR×86, OWL×131 | $9,916.79 | -93.54 | VERI, ZNTL, APMD, HIVE | EU, LUNR, OWL | $14,809.69 | $9,829.53 | VERI×1075, ZNTL×347, APMD×39, HIVE×410 | 09:30 open · cash $14,954.53 (unchanged overnight, no fees) · equity $9,916.79 vs prior close $10,010.33 (-93.54) because holdings re-marked: EU×1412 yday $1.21 → 09:30 $1.21 +0.00; LUNR×86 yday $19.01 → 09:30 $20.25 -106.64; OWL×131 yday $12.22 → 09:30 $12.12 +13.10 |
| 2026-08-18 | -6.20 | $14,809.69 | VERI×1075, ZNTL×347, APMD×39, HIVE×410 | $9,884.94 | +55.41 | — | VERI, ZNTL, APMD, HIVE | $9,859.20 | $9,859.20 | — | 09:30 open · cash $14,809.69 (unchanged overnight, no fees) · equity $9,884.94 vs prior close $9,829.53 (+55.41) because holdings re-marked: VERI×1075 yday $1.08 → 09:30 $1.05 +37.62; ZNTL×347 yday $3.71 → 09:30 $3.75 -15.61; APMD×39 yday $32.55 → 09:30 $32.85 -11.70; HIVE×410 yday $3.07 → 09:30 $2.96 +45.10 |
| 2026-08-19 | -7.20 | $9,859.20 | — | $9,859.20 | -0.00 | — | — | $9,859.20 | $9,859.20 | — | 09:30 open · cash $9,859.20 · no holdings · equity $9,859.20 vs prior close $9,859.20 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-20 | +1.12 | $9,859.20 | — | $9,859.20 | -0.00 | AEM, WYFI, TOYO, ABCL, TEAM, AAP, WMT, AQST | — | $14,559.04 | $9,909.62 | AEM×3, WYFI×28, TOYO×139, ABCL×52, TEAM×3, AAP×13, WMT×5, AQST×133 | 09:30 open · cash $9,859.20 · no holdings · equity $9,859.20 vs prior close $9,859.20 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $14,559.04 | AEM×3, WYFI×28, TOYO×139, ABCL×52, TEAM×3, AAP×13, WMT×5, AQST×133 | $9,858.60 | -51.02 | QTRX, MRNA, AUGO, SSRM, ARIS, NOG | AEM, WYFI, TOYO, ABCL, TEAM, AAP, WMT, AQST | $14,677.60 | $9,808.20 | QTRX×263, MRNA×6, AUGO×9, SSRM×21, ARIS×39, NOG×30 | 09:30 open · cash $14,559.04 (unchanged overnight, no fees) · equity $9,858.60 vs prior close $9,909.62 (-51.02) because holdings re-marked: AEM×3 yday $212.04 → 09:30 $216.30 -12.78; WYFI×28 yday $21.16 → 09:30 $21.54 -10.64; TOYO×139 yday $4.51 → 09:30 $4.68 -22.94; ABCL×52 yday $11.57 → 09:30 $11.57 +0.00; TEAM×3 yday $174.91 → 09:30 $174.22 +2.07; AAP×13 yday $42.39 → 09:30 $42.41 -0.26; WMT×5 yday $103.59 → 09:30 $103.69 -0.49; AQST×133 yday $4.50 → 09:30 $4.54 -5.98 |
| 2026-08-24 | -5.17 | $14,677.60 | QTRX×263, MRNA×6, AUGO×9, SSRM×21, ARIS×39, NOG×30 | $9,789.83 | -18.37 | — | QTRX, MRNA, AUGO, SSRM, ARIS | $10,590.95 | $9,796.25 | NOG×30 | 09:30 open · cash $14,677.60 (unchanged overnight, no fees) · equity $9,789.83 vs prior close $9,808.20 (-18.37) because holdings re-marked: QTRX×263 yday $2.99 → 09:30 $2.98 +2.63; MRNA×6 yday $145.13 → 09:30 $142.70 +14.58; AUGO×9 yday $87.26 → 09:30 $89.87 -23.49; SSRM×21 yday $37.77 → 09:30 $38.48 -14.91; ARIS×39 yday $20.86 → 09:30 $20.98 -4.68; NOG×30 yday $27.34 → 09:30 $27.09 +7.50 |
| 2026-08-25 | +1.80 | $10,590.95 | NOG×30 | $9,807.95 | +11.70 | BMO, AVAH | — | $15,438.39 | $9,754.79 | NOG×30, BMO×14, AVAH×178 | 09:30 open · cash $10,590.95 (unchanged overnight, no fees) · equity $9,807.95 vs prior close $9,796.25 (+11.70) because holdings re-marked: NOG×30 yday $26.49 → 09:30 $26.10 +11.70 |
| 2026-08-26 | +2.02 | $15,438.39 | NOG×30, BMO×14, AVAH×178 | $9,754.79 | -0.00 | — | — | $15,438.39 | $9,803.19 | NOG×30, BMO×14, AVAH×178 | 09:30 open · cash $15,438.39 (unchanged overnight, no fees) · equity $9,754.79 vs prior close $9,754.79 (-0.00) because holdings re-marked: NOG×30 yday $26.50 → 09:30 $26.50 +0.00; BMO×14 yday $175.00 → 09:30 $175.00 +0.00; AVAH×178 yday $13.70 → 09:30 $13.70 +0.00 |
| 2026-08-27 | — | $15,438.39 | NOG×30, BMO×14, AVAH×178 | $9,803.61 | +0.42 | — | NOG, BMO, AVAH | $9,796.97 | $9,796.97 | — | 09:30 open · cash $15,438.39 (unchanged overnight, no fees) · equity $9,803.61 vs prior close $9,803.19 (+0.42) because holdings re-marked: NOG×30 yday $26.50 → 09:30 $26.00 +15.00; BMO×14 yday $175.00 → 09:30 $173.22 +24.92; AVAH×178 yday $13.70 → 09:30 $13.65 +8.90 |
| 2026-08-28 | +0.75 | $9,796.97 | — | $9,796.97 | +0.00 | SIMO, NOG | — | $14,684.83 | $9,911.51 | SIMO×9, NOG×95 | 09:30 open · cash $9,796.97 · no holdings · equity $9,796.97 vs prior close $9,796.97 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-31 | -5.85 | $14,684.83 | SIMO×9, NOG×95 | $10,019.37 | +107.86 | — | SIMO | $12,461.70 | $10,017.35 | NOG×95 | 09:30 open · cash $14,684.83 (unchanged overnight, no fees) · equity $10,019.37 vs prior close $9,911.51 (+107.86) because holdings re-marked: SIMO×9 yday $255.08 → 09:30 $246.79 +74.61; NOG×95 yday $26.08 → 09:30 $25.73 +33.25 |
| 2026-09-01 | -6.30 | $12,461.70 | NOG×95 | $9,957.50 | -59.85 | — | NOG | $9,955.23 | $9,955.23 | — | 09:30 open · cash $12,461.70 (unchanged overnight, no fees) · equity $9,957.50 vs prior close $10,017.35 (-59.85) because holdings re-marked: NOG×95 yday $25.73 → 09:30 $26.36 -59.85 |
| 2026-09-02 | -3.83 | $9,955.23 | — | $9,955.23 | -0.00 | — | — | $9,955.23 | $9,955.23 | — | 09:30 open · cash $9,955.23 · no holdings · equity $9,955.23 vs prior close $9,955.23 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-03 | -0.90 | $9,955.23 | — | $9,955.23 | -0.00 | SLN, NIQ, NOG, TX | — | $14,871.23 | $9,916.43 | SLN×84, NIQ×66, NOG×47, TX×22 | 09:30 open · cash $9,955.23 · no holdings · equity $9,955.23 vs prior close $9,955.23 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $14,871.23 | SLN×84, NIQ×66, NOG×47, TX×22 | $9,872.48 | -43.95 | GSM, OPK | SLN, NIQ, TX | $16,021.55 | $10,027.84 | NOG×47, GSM×542, OPK×1442 | 09:30 open · cash $14,871.23 (unchanged overnight, no fees) · equity $9,872.48 vs prior close $9,916.43 (-43.95) because holdings re-marked: SLN×84 yday $14.79 → 09:30 $14.85 -5.04; NIQ×66 yday $18.35 → 09:30 $18.66 -20.46; NOG×47 yday $26.60 → 09:30 $26.59 +0.47; TX×22 yday $56.87 → 09:30 $57.73 -18.92 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 1412 | $1.18 | $18.51 | — | $11,647.65 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 86 | $19.17 | $2.32 | — | $13,293.95 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 131 | $12.70 | $2.46 | — | $14,954.53 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 judge🟢 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,954.53 | ▼ 09:30 equity $9,916.79 vs yday $10,010.33 (-93.54) | 09:30 open · cash $14,954.53 (unchanged overnight, no fees) · equity $9,916.79 vs prior close $10,010.33 (-93.54) because holdings re-marked: EU×1412 yday $1.21 → 09:30 $1.21 +0.00; LUNR×86 yday $19.01 → 09:30 $20.25 -106.64; OWL×131 yday $12.22 → 09:30 $12.12 +13.10 | — |
| 2026-08-17 09:30 ET | **COVER** | `EU` | 1412 | $1.21 | $18.21 | $-79.08 | $13,227.80 | ▼ -79.08 after sell → book $9,898.58; vs 09:30 mark -18.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `LUNR` | 86 | $20.25 | $2.25 | $-97.45 | $11,484.05 | ▼ -97.45 after sell → book $9,896.33; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **COVER** | `OWL` | 131 | $12.12 | $2.38 | $+70.48 | $9,893.95 | ▲ +70.48 after sell → book $9,893.95; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 1075 | $1.15 | $14.09 | — | $11,116.11 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; leftover $1236.74 | join🟡 sector🟢 gen🟢 news🔴 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 347 | $3.56 | $4.58 | — | $12,346.85 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; leftover $1236.74 | join🟡 sector🔴 gen🟢 news🔴 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 39 | $31.70 | $2.16 | — | $13,580.99 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; leftover $1236.74 | join🟡 sector🔴 gen🟢 news🔴 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 410 | $3.01 | $5.40 | — | $14,809.69 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; leftover $1236.74 | join🟢 sector🟢 gen🟢 news🔴 judge🟢 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,809.69 | ▲ 09:30 equity $9,884.94 vs yday $9,829.53 (+55.41) | 09:30 open · cash $14,809.69 (unchanged overnight, no fees) · equity $9,884.94 vs prior close $9,829.53 (+55.41) because holdings re-marked: VERI×1075 yday $1.08 → 09:30 $1.05 +37.62; ZNTL×347 yday $3.71 → 09:30 $3.75 -15.61; APMD×39 yday $32.55 → 09:30 $32.85 -11.70; HIVE×410 yday $3.07 → 09:30 $2.96 +45.10 | — |
| 2026-08-18 09:30 ET | **COVER** | `VERI` | 1075 | $1.05 | $13.87 | $+79.54 | $13,667.07 | ▲ +79.54 after sell → book $9,871.07; vs 09:30 mark -13.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `ZNTL` | 347 | $3.75 | $4.48 | $-74.98 | $12,361.34 | ▼ -74.98 after sell → book $9,866.59; vs 09:30 mark -4.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `APMD` | 39 | $32.85 | $2.11 | $-49.12 | $11,078.09 | ▼ -49.12 after sell → book $9,864.49; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `HIVE` | 410 | $2.96 | $5.29 | $+9.81 | $9,859.20 | ▲ +9.81 after sell → book $9,859.20; vs 09:30 mark -5.29 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,859.20 | ▲ 09:30 equity $9,859.20 vs yday $9,859.20 (-0.00) | 09:30 open · cash $9,859.20 · no holdings · equity $9,859.20 vs prior close $9,859.20 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,859.20 | ▲ 09:30 equity $9,859.20 vs yday $9,859.20 (-0.00) | 09:30 open · cash $9,859.20 · no holdings · equity $9,859.20 vs prior close $9,859.20 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $10,470.51 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $616.20 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 28 | $21.40 | $2.11 | — | $11,067.60 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; leftover $616.20 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 139 | $4.43 | $2.46 | — | $11,680.91 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; leftover $616.20 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 52 | $11.81 | $2.18 | — | $12,293.11 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $616.20 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 3 | $173.90 | $2.03 | — | $12,812.78 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; leftover $616.20 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 13 | $46.85 | $2.07 | — | $13,419.76 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=+5.0; leftover $616.20 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 5 | $106.13 | $2.04 | — | $13,948.35 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; leftover $616.20 | join🟢 sector🟡 gen🟢 news🔴 digest🟡 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 133 | $4.61 | $2.44 | — | $14,559.04 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; leftover $616.20 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,559.04 | ▼ 09:30 equity $9,858.60 vs yday $9,909.62 (-51.02) | 09:30 open · cash $14,559.04 (unchanged overnight, no fees) · equity $9,858.60 vs prior close $9,909.62 (-51.02) because holdings re-marked: AEM×3 yday $212.04 → 09:30 $216.30 -12.78; WYFI×28 yday $21.16 → 09:30 $21.54 -10.64; TOYO×139 yday $4.51 → 09:30 $4.68 -22.94; ABCL×52 yday $11.57 → 09:30 $11.57 +0.00; TEAM×3 yday $174.91 → 09:30 $174.22 +2.07; AAP×13 yday $42.39 → 09:30 $42.41 -0.26; WMT×5 yday $103.59 → 09:30 $103.69 -0.49; AQST×133 yday $4.50 → 09:30 $4.54 -5.98 | — |
| 2026-08-21 09:30 ET | **COVER** | `AEM` | 3 | $216.30 | $2.00 | $-39.58 | $13,908.15 | ▼ -39.58 after sell → book $9,856.61; vs 09:30 mark -1.99 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `WYFI` | 28 | $21.54 | $2.07 | $-8.10 | $13,302.95 | ▼ -8.10 after sell → book $9,854.53; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `TOYO` | 139 | $4.68 | $2.41 | $-39.61 | $12,650.02 | ▼ -39.61 after sell → book $9,852.12; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `ABCL` | 52 | $11.57 | $2.15 | $+8.41 | $12,046.24 | ▲ +8.41 after sell → book $9,849.98; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `TEAM` | 3 | $174.22 | $2.00 | $-4.99 | $11,521.58 | ▼ -4.99 after sell → book $9,847.98; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `AAP` | 13 | $42.41 | $2.03 | $+53.63 | $10,968.22 | ▲ +53.63 after sell → book $9,845.95; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `WMT` | 5 | $103.69 | $2.00 | $+8.13 | $10,447.77 | ▲ +8.13 after sell → book $9,843.95; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `AQST` | 133 | $4.54 | $2.39 | $+4.48 | $9,841.56 | ▲ +4.48 after sell → book $9,841.56; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 263 | $3.11 | $3.47 | — | $10,656.02 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $820.13 | join🟡 sector🟢 gen🟢 news🔴 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 6 | $133.11 | $2.05 | — | $11,452.63 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; leftover $820.13 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 9 | $89.10 | $2.06 | — | $12,252.47 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $820.13 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 21 | $38.40 | $2.10 | — | $13,056.77 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $820.13 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 39 | $20.90 | $2.15 | — | $13,869.72 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $820.13 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 30 | $27.00 | $2.12 | — | $14,677.60 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; leftover $820.13 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,677.60 | ▼ 09:30 equity $9,789.83 vs yday $9,808.20 (-18.37) | 09:30 open · cash $14,677.60 (unchanged overnight, no fees) · equity $9,789.83 vs prior close $9,808.20 (-18.37) because holdings re-marked: QTRX×263 yday $2.99 → 09:30 $2.98 +2.63; MRNA×6 yday $145.13 → 09:30 $142.70 +14.58; AUGO×9 yday $87.26 → 09:30 $89.87 -23.49; SSRM×21 yday $37.77 → 09:30 $38.48 -14.91; ARIS×39 yday $20.86 → 09:30 $20.98 -4.68; NOG×30 yday $27.34 → 09:30 $27.09 +7.50 | — |
| 2026-08-24 09:30 ET | **COVER** | `QTRX` | 263 | $2.98 | $3.39 | $+27.33 | $13,890.47 | ▲ +27.33 after sell → book $9,786.44; vs 09:30 mark -3.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `MRNA` | 6 | $142.70 | $2.01 | $-61.60 | $13,032.26 | ▼ -61.60 after sell → book $9,784.43; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `AUGO` | 9 | $89.87 | $2.02 | $-11.01 | $12,221.41 | ▼ -11.01 after sell → book $9,782.41; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `SSRM` | 21 | $38.48 | $2.05 | $-5.83 | $11,411.28 | ▼ -5.83 after sell → book $9,780.36; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `ARIS` | 39 | $20.98 | $2.11 | $-7.38 | $10,590.95 | ▼ -7.38 after sell → book $9,778.25; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,590.95 | ▲ 09:30 equity $9,807.95 vs yday $9,796.25 (+11.70) | 09:30 open · cash $10,590.95 (unchanged overnight, no fees) · equity $9,807.95 vs prior close $9,796.25 (+11.70) because holdings re-marked: NOG×30 yday $26.49 → 09:30 $26.10 +11.70 | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 14 | $172.40 | $2.13 | — | $13,002.43 | — | news🔴; gate news=bad; list earn_react; ret5=-6.1; leftover $2451.99 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 178 | $13.70 | $2.64 | — | $15,438.39 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+6.8; leftover $2451.99 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,438.39 | ▲ 09:30 equity $9,754.79 vs yday $9,754.79 (-0.00) | 09:30 open · cash $15,438.39 (unchanged overnight, no fees) · equity $9,754.79 vs prior close $9,754.79 (-0.00) because holdings re-marked: NOG×30 yday $26.50 → 09:30 $26.50 +0.00; BMO×14 yday $175.00 → 09:30 $175.00 +0.00; AVAH×178 yday $13.70 → 09:30 $13.70 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,438.39 | ▲ 09:30 equity $9,803.61 vs yday $9,803.19 (+0.42) | 09:30 open · cash $15,438.39 (unchanged overnight, no fees) · equity $9,803.61 vs prior close $9,803.19 (+0.42) because holdings re-marked: NOG×30 yday $26.50 → 09:30 $26.00 +15.00; BMO×14 yday $175.00 → 09:30 $173.22 +24.92; AVAH×178 yday $13.70 → 09:30 $13.65 +8.90 | — |
| 2026-08-27 09:30 ET | **COVER** | `NOG` | 30 | $26.00 | $2.08 | $+25.80 | $14,656.31 | ▲ +25.80 after sell → book $9,801.53; vs 09:30 mark -2.08 | dropped from list after 4 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `BMO` | 14 | $173.22 | $2.03 | $-15.64 | $12,229.20 | ▼ -15.64 after sell → book $9,799.50; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `AVAH` | 178 | $13.65 | $2.52 | $+3.74 | $9,796.97 | ▲ +3.74 after sell → book $9,796.97; vs 09:30 mark -2.53 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,796.97 | ▲ 09:30 equity $9,796.97 vs yday $9,796.97 (+0.00) | 09:30 open · cash $9,796.97 · no holdings · equity $9,796.97 vs prior close $9,796.97 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 9 | $272.00 | $2.11 | — | $12,242.86 | — | news🔴; gate news=bad; list yday_gainer; ⚪; ret5=-3.9; leftover $2449.24 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `NOG` | 95 | $25.73 | $2.38 | — | $14,684.83 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.6; leftover $2449.24 | join🔴 sector🔴 gen🟡 news🔴 digest🟢 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,684.83 | ▲ 09:30 equity $10,019.37 vs yday $9,911.51 (+107.86) | 09:30 open · cash $14,684.83 (unchanged overnight, no fees) · equity $10,019.37 vs prior close $9,911.51 (+107.86) because holdings re-marked: SIMO×9 yday $255.08 → 09:30 $246.79 +74.61; NOG×95 yday $26.08 → 09:30 $25.73 +33.25 | — |
| 2026-08-31 09:30 ET | **COVER** | `SIMO` | 9 | $246.79 | $2.02 | $+222.76 | $12,461.70 | ▲ +222.76 after sell → book $10,017.35; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,461.70 | ▼ 09:30 equity $9,957.50 vs yday $10,017.35 (-59.85) | 09:30 open · cash $12,461.70 (unchanged overnight, no fees) · equity $9,957.50 vs prior close $10,017.35 (-59.85) because holdings re-marked: NOG×95 yday $25.73 → 09:30 $26.36 -59.85 | — |
| 2026-09-01 09:30 ET | **COVER** | `NOG` | 95 | $26.36 | $2.27 | $-64.50 | $9,955.23 | ▼ -64.50 after sell → book $9,955.23; vs 09:30 mark -2.27 | dropped from list after 2 sess (min 1) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,955.23 | ▲ 09:30 equity $9,955.23 vs yday $9,955.23 (-0.00) | 09:30 open · cash $9,955.23 · no holdings · equity $9,955.23 vs prior close $9,955.23 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,955.23 | ▲ 09:30 equity $9,955.23 vs yday $9,955.23 (-0.00) | 09:30 open · cash $9,955.23 · no holdings · equity $9,955.23 vs prior close $9,955.23 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 84 | $14.70 | $2.30 | — | $11,187.73 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1244.40 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `NIQ` | 66 | $18.60 | $2.24 | — | $12,413.09 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+7.6; leftover $1244.40 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `NOG` | 47 | $26.10 | $2.18 | — | $13,637.60 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.6; leftover $1244.40 | join🔴 sector🔴 gen🟡 news🔴 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `TX` | 22 | $56.17 | $2.11 | — | $14,871.23 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+3.0; leftover $1244.40 | join🔴 sector🟢 gen🟡 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,871.23 | ▼ 09:30 equity $9,872.48 vs yday $9,916.43 (-43.95) | 09:30 open · cash $14,871.23 (unchanged overnight, no fees) · equity $9,872.48 vs prior close $9,916.43 (-43.95) because holdings re-marked: SLN×84 yday $14.79 → 09:30 $14.85 -5.04; NIQ×66 yday $18.35 → 09:30 $18.66 -20.46; NOG×47 yday $26.60 → 09:30 $26.59 +0.47; TX×22 yday $56.87 → 09:30 $57.73 -18.92 | — |
| 2026-09-04 09:30 ET | **COVER** | `SLN` | 84 | $14.85 | $2.24 | $-17.14 | $13,621.59 | ▼ -17.14 after sell → book $9,870.24; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `NIQ` | 66 | $18.66 | $2.19 | $-8.39 | $12,387.84 | ▼ -8.39 after sell → book $9,868.05; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **COVER** | `TX` | 22 | $57.73 | $2.06 | $-38.49 | $11,115.73 | ▼ -38.49 after sell → book $9,866.00; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 542 | $4.55 | $7.17 | — | $13,574.66 | — | news🔴; gate news=bad; list yday_gainer; ret5=-7.1; leftover $2466.50 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `OPK` | 1442 | $1.71 | $18.93 | — | $16,021.55 | — | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+7.2; leftover $2466.50 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `RNW` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `LUNR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EU` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `BMO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AVAH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BE` | no_price | no 09:30 open |
| 2026-08-26 | `SSRM` | no_price | no 09:30 open |
| 2026-08-31 | `NIQ` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ARIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CELH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NOG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ARIS` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `NOG` | 47 | 2026-09-03 @ $26.10 | news🔴; gate news=bad; list ohlc_hot; ret5=+11.6; leftover $1244.40 |
| `GSM` | 542 | 2026-09-04 @ $4.55 | news🔴; gate news=bad; list yday_gainer; ret5=-7.1; leftover $2466.50 |
| `OPK` | 1442 | 2026-09-04 @ $1.71 | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+7.2; leftover $2466.50 |
