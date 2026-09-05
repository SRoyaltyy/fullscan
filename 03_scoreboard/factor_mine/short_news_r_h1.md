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

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-14 | +5.50 | $10,000.00 | — | EU, LUNR, OWL | — | $14,954.53 | $-4,944.20 | $10,010.33 | EU×1412, LUNR×86, OWL×131 | SHORT EU x1412 @ 1.18; SHORT LUNR x86 @ 19.17; SHORT OWL x131 @ 12.70 |
| 2026-08-17 | +2.25 | $14,954.53 | EU×1412, LUNR×86, OWL×131 | VERI, ZNTL, APMD, HIVE | EU, LUNR, OWL | $14,809.69 | $-4,980.16 | $9,829.53 | VERI×1075, ZNTL×347, APMD×39, HIVE×410 | SELL EU (dropped from list after 1 sess (min 1)); SELL LUNR (dropped from list after 1 sess (min 1)); SELL OWL (dropped from list after 1 sess (min 1)); SHORT VERI x1075 @ 1.15; SHORT ZNTL x347 @ 3.56; SHORT APMD x39 @ 31.70; SHORT HIVE x410 @ 3.01 |
| 2026-08-18 | -6.20 | $14,809.69 | VERI×1075, ZNTL×347, APMD×39, HIVE×410 | — | VERI, ZNTL, APMD, HIVE | $9,859.20 | $0.00 | $9,859.20 | — | SELL VERI (dropped from list after 1 sess (min 1)); SELL ZNTL (dropped from list after 1 sess (min 1)); SELL APMD (dropped from list after 1 sess (min 1)); SELL HIVE (dropped from list after 1 sess (min 1)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $9,859.20 | — | — | — | $9,859.20 | $0.00 | $9,859.20 | — | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $9,859.20 | — | AEM, WYFI, TOYO, ABCL, TEAM, AAP, WMT, AQST | — | $14,559.04 | $-4,649.42 | $9,909.62 | AEM×3, WYFI×28, TOYO×139, ABCL×52, TEAM×3, AAP×13, WMT×5, AQST×133 | SHORT AEM x3 @ 204.45; SHORT WYFI x28 @ 21.40; SHORT TOYO x139 @ 4.43; SHORT ABCL x52 @ 11.81; SHORT TEAM x3 @ 173.90; SHORT AAP x13 @ 46.85; SHORT WMT x5 @ 106.13; SHORT AQST x133 @ 4.61 |
| 2026-08-21 | +3.25 | $14,559.04 | AEM×3, WYFI×28, TOYO×139, ABCL×52, TEAM×3, AAP×13, WMT×5, AQST×133 | QTRX, MRNA, AUGO, SSRM, ARIS, NOG | AEM, WYFI, TOYO, ABCL, TEAM, AAP, WMT, AQST | $14,677.60 | $-4,869.40 | $9,808.20 | QTRX×263, MRNA×6, AUGO×9, SSRM×21, ARIS×39, NOG×30 | SELL AEM (dropped from list after 1 sess (min 1)); SELL WYFI (dropped from list after 1 sess (min 1)); SELL TOYO (dropped from list after 1 sess (min 1)); SELL ABCL (dropped from list after 1 sess (min 1)); SELL TEAM (dropped from list after 1 sess (min 1)); SELL AAP (dropped from list after 1 sess (min 1)); SELL WMT (dropped from list after 1 sess (min 1)); SELL AQST (dropped from list after 1 sess (min 1)); SHORT QTRX x263 @ 3.11; SHORT MRNA x6 @ 133.11; SHORT AUGO x9 @ 89.10; SHORT SSRM x21 @ 38.40; SHORT ARIS x39 @ 20.90; SHORT NOG x30 @ 27.00 |
| 2026-08-24 | -5.17 | $14,677.60 | QTRX×263, MRNA×6, AUGO×9, SSRM×21, ARIS×39, NOG×30 | — | QTRX, MRNA, AUGO, SSRM, ARIS | $10,590.95 | $-794.70 | $9,796.25 | NOG×30 | SELL QTRX (dropped from list after 1 sess (min 1)); SELL MRNA (dropped from list after 1 sess (min 1)); SELL AUGO (dropped from list after 1 sess (min 1)); SELL SSRM (dropped from list after 1 sess (min 1)); SELL ARIS (dropped from list after 1 sess (min 1)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $10,590.95 | NOG×30 | BMO, AVAH | — | $15,438.39 | $-5,683.60 | $9,754.79 | NOG×30, BMO×14, AVAH×178 | SHORT BMO x14 @ 172.40; SHORT AVAH x178 @ 13.70 |
| 2026-08-26 | +2.02 | $15,438.39 | NOG×30, BMO×14, AVAH×178 | — | — | $15,438.39 | $-5,635.20 | $9,803.19 | NOG×30, BMO×14, AVAH×178 | hold NOG,BMO,AVAH |
| 2026-08-27 | — | $15,438.39 | NOG×30, BMO×14, AVAH×178 | — | NOG, BMO, AVAH | $9,796.97 | $0.00 | $9,796.97 | — | SELL NOG (dropped from list after 4 sess (min 1)); SELL BMO (dropped from list after 2 sess (min 1)); SELL AVAH (dropped from list after 2 sess (min 1)) |
| 2026-08-28 | +0.75 | $9,796.97 | — | SIMO, NOG | — | $14,684.83 | $-4,773.32 | $9,911.51 | SIMO×9, NOG×95 | SHORT SIMO x9 @ 272.00; SHORT NOG x95 @ 25.73 |
| 2026-08-31 | -5.85 | $14,684.83 | SIMO×9, NOG×95 | — | SIMO | $12,461.70 | $-2,444.35 | $10,017.35 | NOG×95 | SELL SIMO (dropped from list after 1 sess (min 1)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $12,461.70 | NOG×95 | — | NOG | $9,955.23 | $0.00 | $9,955.23 | — | SELL NOG (dropped from list after 2 sess (min 1)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $9,955.23 | — | — | — | $9,955.23 | $0.00 | $9,955.23 | — | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $9,955.23 | — | SLN, NIQ, NOG, TX | — | $14,871.23 | $-4,954.80 | $9,916.43 | SLN×84, NIQ×66, NOG×47, TX×22 | SHORT SLN x84 @ 14.70; SHORT NIQ x66 @ 18.60; SHORT NOG x47 @ 26.10; SHORT TX x22 @ 56.17 |
| 2026-09-04 | — | $14,871.23 | SLN×84, NIQ×66, NOG×47, TX×22 | GSM, OPK | SLN, NIQ, TX | $16,021.55 | $-5,993.71 | $10,027.84 | NOG×47, GSM×542, OPK×1442 | SELL SLN (dropped from list after 1 sess (min 1)); SELL NIQ (dropped from list after 1 sess (min 1)); SELL TX (dropped from list after 1 sess (min 1)); SHORT GSM x542 @ 4.55; SHORT OPK x1442 @ 1.71 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 1412 | $1.18 | $18.51 | — | $11,647.65 | ▼ $9,981.49 (-18.51) | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 86 | $19.17 | $2.32 | — | $13,293.95 | ▼ $9,979.17 (-20.83) | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 131 | $12.70 | $2.46 | — | $14,954.53 | ▼ $9,976.71 (-23.29) | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 judge🟢 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **COVER** | `EU` | 1412 | $1.21 | $18.21 | $-79.08 | $13,227.80 | ▼ $9,898.58 (-101.42) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `LUNR` | 86 | $20.25 | $2.25 | $-97.45 | $11,484.05 | ▼ $9,896.33 (-103.67) | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **COVER** | `OWL` | 131 | $12.12 | $2.38 | $+70.48 | $9,893.95 | ▼ $9,893.95 (-106.05) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 1075 | $1.15 | $14.09 | — | $11,116.11 | ▼ $9,879.86 (-120.14) | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; leftover $1236.74 | join🟡 sector🟢 gen🟢 news🔴 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 347 | $3.56 | $4.58 | — | $12,346.85 | ▼ $9,875.28 (-124.72) | news🔴; gate news=bad; list yday_mover; ret5=-15.6; leftover $1236.74 | join🟡 sector🔴 gen🟢 news🔴 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 39 | $31.70 | $2.16 | — | $13,580.99 | ▼ $9,873.12 (-126.88) | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; leftover $1236.74 | join🟡 sector🔴 gen🟢 news🔴 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 410 | $3.01 | $5.40 | — | $14,809.69 | ▼ $9,867.72 (-132.28) | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; leftover $1236.74 | join🟢 sector🟢 gen🟢 news🔴 judge🟢 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **COVER** | `VERI` | 1075 | $1.05 | $13.87 | $+79.54 | $13,667.07 | ▼ $9,871.07 (-128.93) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `ZNTL` | 347 | $3.75 | $4.48 | $-74.98 | $12,361.34 | ▼ $9,866.59 (-133.41) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `APMD` | 39 | $32.85 | $2.11 | $-49.12 | $11,078.09 | ▼ $9,864.49 (-135.51) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `HIVE` | 410 | $2.96 | $5.29 | $+9.81 | $9,859.20 | ▼ $9,859.20 (-140.80) | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $10,470.51 | ▼ $9,857.16 (-142.84) | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $616.20 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 28 | $21.40 | $2.11 | — | $11,067.60 | ▼ $9,855.05 (-144.95) | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; leftover $616.20 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 139 | $4.43 | $2.46 | — | $11,680.91 | ▼ $9,852.59 (-147.41) | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; leftover $616.20 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 52 | $11.81 | $2.18 | — | $12,293.11 | ▼ $9,850.41 (-149.59) | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $616.20 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 3 | $173.90 | $2.03 | — | $12,812.78 | ▼ $9,848.38 (-151.62) | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; leftover $616.20 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 13 | $46.85 | $2.07 | — | $13,419.76 | ▼ $9,846.31 (-153.69) | news🔴; gate news=bad; list earn_react; 🔵; ret5=+5.0; leftover $616.20 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 5 | $106.13 | $2.04 | — | $13,948.35 | ▼ $9,844.27 (-155.73) | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; leftover $616.20 | join🟢 sector🟡 gen🟢 news🔴 digest🟡 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 133 | $4.61 | $2.44 | — | $14,559.04 | ▼ $9,841.84 (-158.16) | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; leftover $616.20 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `AEM` | 3 | $216.30 | $2.00 | $-39.58 | $13,908.15 | ▼ $9,856.61 (-143.39) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `WYFI` | 28 | $21.54 | $2.07 | $-8.10 | $13,302.95 | ▼ $9,854.53 (-145.47) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `TOYO` | 139 | $4.68 | $2.41 | $-39.61 | $12,650.02 | ▼ $9,852.12 (-147.88) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `ABCL` | 52 | $11.57 | $2.15 | $+8.41 | $12,046.24 | ▼ $9,849.98 (-150.02) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `TEAM` | 3 | $174.22 | $2.00 | $-4.99 | $11,521.58 | ▼ $9,847.98 (-152.02) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `AAP` | 13 | $42.41 | $2.03 | $+53.63 | $10,968.22 | ▼ $9,845.95 (-154.05) | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `WMT` | 5 | $103.69 | $2.00 | $+8.13 | $10,447.77 | ▼ $9,843.95 (-156.05) | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `AQST` | 133 | $4.54 | $2.39 | $+4.48 | $9,841.56 | ▼ $9,841.56 (-158.44) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 263 | $3.11 | $3.47 | — | $10,656.02 | ▼ $9,838.09 (-161.91) | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $820.13 | join🟡 sector🟢 gen🟢 news🔴 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 6 | $133.11 | $2.05 | — | $11,452.63 | ▼ $9,836.04 (-163.96) | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; leftover $820.13 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 9 | $89.10 | $2.06 | — | $12,252.47 | ▼ $9,833.98 (-166.02) | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $820.13 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 21 | $38.40 | $2.10 | — | $13,056.77 | ▼ $9,831.88 (-168.12) | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $820.13 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 39 | $20.90 | $2.15 | — | $13,869.72 | ▼ $9,829.73 (-170.27) | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $820.13 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 30 | $27.00 | $2.12 | — | $14,677.60 | ▼ $9,827.61 (-172.39) | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; leftover $820.13 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **COVER** | `QTRX` | 263 | $2.98 | $3.39 | $+27.33 | $13,890.47 | ▼ $9,786.44 (-213.56) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `MRNA` | 6 | $142.70 | $2.01 | $-61.60 | $13,032.26 | ▼ $9,784.43 (-215.57) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `AUGO` | 9 | $89.87 | $2.02 | $-11.01 | $12,221.41 | ▼ $9,782.41 (-217.59) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `SSRM` | 21 | $38.48 | $2.05 | $-5.83 | $11,411.28 | ▼ $9,780.36 (-219.64) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `ARIS` | 39 | $20.98 | $2.11 | $-7.38 | $10,590.95 | ▼ $9,778.25 (-221.75) | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 14 | $172.40 | $2.13 | — | $13,002.43 | ▼ $9,805.83 (-194.17) | news🔴; gate news=bad; list earn_react; ret5=-6.1; leftover $2451.99 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 178 | $13.70 | $2.64 | — | $15,438.39 | ▼ $9,803.19 (-196.81) | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+6.8; leftover $2451.99 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **COVER** | `NOG` | 30 | $26.00 | $2.08 | $+25.80 | $14,656.31 | ▼ $9,801.53 (-198.47) | dropped from list after 4 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `BMO` | 14 | $173.22 | $2.03 | $-15.64 | $12,229.20 | ▼ $9,799.50 (-200.50) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `AVAH` | 178 | $13.65 | $2.52 | $+3.74 | $9,796.97 | ▼ $9,796.97 (-203.03) | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 9 | $272.00 | $2.11 | — | $12,242.86 | ▼ $9,794.86 (-205.14) | news🔴; gate news=bad; list yday_gainer; ⚪; ret5=-3.9; leftover $2449.24 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `NOG` | 95 | $25.73 | $2.38 | — | $14,684.83 | ▼ $9,792.48 (-207.52) | news🔴; gate news=bad; list ohlc_hot; ret5=+11.6; leftover $2449.24 | join🔴 sector🔴 gen🟡 news🔴 digest🟢 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **COVER** | `SIMO` | 9 | $246.79 | $2.02 | $+222.76 | $12,461.70 | ▲ $10,017.35 (+17.35) | dropped from list after 1 sess (min 1) | — |
| 2026-09-01 09:30 ET | **COVER** | `NOG` | 95 | $26.36 | $2.27 | $-64.50 | $9,955.23 | ▼ $9,955.23 (-44.77) | dropped from list after 2 sess (min 1) | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 84 | $14.70 | $2.30 | — | $11,187.73 | ▼ $9,952.93 (-47.07) | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1244.40 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `NIQ` | 66 | $18.60 | $2.24 | — | $12,413.09 | ▼ $9,950.69 (-49.31) | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+7.6; leftover $1244.40 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `NOG` | 47 | $26.10 | $2.18 | — | $13,637.60 | ▼ $9,948.50 (-51.50) | news🔴; gate news=bad; list ohlc_hot; ret5=+11.6; leftover $1244.40 | join🔴 sector🔴 gen🟡 news🔴 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `TX` | 22 | $56.17 | $2.11 | — | $14,871.23 | ▼ $9,946.39 (-53.61) | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+3.0; leftover $1244.40 | join🔴 sector🟢 gen🟡 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **COVER** | `SLN` | 84 | $14.85 | $2.24 | $-17.14 | $13,621.59 | ▼ $9,870.24 (-129.76) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `NIQ` | 66 | $18.66 | $2.19 | $-8.39 | $12,387.84 | ▼ $9,868.05 (-131.95) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **COVER** | `TX` | 22 | $57.73 | $2.06 | $-38.49 | $11,115.73 | ▼ $9,866.00 (-134.00) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 542 | $4.55 | $7.17 | — | $13,574.66 | ▼ $9,858.83 (-141.17) | news🔴; gate news=bad; list yday_gainer; ret5=-7.1; leftover $2466.50 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `OPK` | 1442 | $1.71 | $18.93 | — | $16,021.55 | ▼ $9,839.90 (-160.10) | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+7.2; leftover $2466.50 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

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
