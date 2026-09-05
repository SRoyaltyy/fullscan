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

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-14 | +5.50 | $10,000.00 | — | EU, LUNR, OWL | — | $14,954.53 | $-4,944.20 | $10,010.33 | EU×1412, LUNR×86, OWL×131 | SHORT EU x1412 @ 1.18; SHORT LUNR x86 @ 19.17; SHORT OWL x131 @ 12.70 |
| 2026-08-17 | +2.25 | $14,954.53 | EU×1412, LUNR×86, OWL×131 | VERI, ZNTL, APMD, HIVE | — | $19,879.09 | $-9,864.81 | $10,014.29 | EU×1412, LUNR×86, OWL×131, VERI×1077, ZNTL×348, APMD×39, HIVE×411 | SHORT VERI x1077 @ 1.15; SHORT ZNTL x348 @ 3.56; SHORT APMD x39 @ 31.70; SHORT HIVE x411 @ 3.01 |
| 2026-08-18 | -6.20 | $19,879.09 | EU×1412, LUNR×86, OWL×131, VERI×1077, ZNTL×348, APMD×39, HIVE×411 | — | — | $19,879.09 | $-9,425.22 | $10,453.88 | EU×1412, LUNR×86, OWL×131, VERI×1077, ZNTL×348, APMD×39, HIVE×411 | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $19,879.09 | EU×1412, LUNR×86, OWL×131, VERI×1077, ZNTL×348, APMD×39, HIVE×411 | — | EU, OWL | $16,808.40 | $-6,370.65 | $10,437.75 | LUNR×86, VERI×1077, ZNTL×348, APMD×39, HIVE×411 | SELL EU (dropped from list after 3 sess (min 3)); SELL OWL (dropped from list after 3 sess (min 3)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $16,808.40 | LUNR×86, VERI×1077, ZNTL×348, APMD×39, HIVE×411 | AEM, WYFI, TOYO, ABCL, TEAM, AAP, WMT, AQST | LUNR, VERI, ZNTL, APMD, HIVE | $15,262.91 | $-4,877.03 | $10,385.89 | AEM×3, WYFI×30, TOYO×145, ABCL×54, TEAM×3, AAP×13, WMT×6, AQST×140 | SELL LUNR (dropped from list after 4 sess (min 3)); SELL VERI (dropped from list after 3 sess (min 3)); SELL ZNTL (dropped from list after 3 sess (min 3)); SELL APMD (dropped from list after 3 sess (min 3)); SELL HIVE (dropped from list after 3 sess (min 3)); SHORT AEM x3 @ 204.45; SHORT WYFI x30 @ 21.40; SHORT TOYO x145 @ 4.43; SHORT ABCL x54 @ 11.81; SHORT TEAM x3 @ 173.90; SHORT AAP x13 @ 46.85; SHORT WMT x6 @ 106.13; SHORT AQST x140 @ 4.61 |
| 2026-08-21 | +3.25 | $15,262.91 | AEM×3, WYFI×30, TOYO×145, ABCL×54, TEAM×3, AAP×13, WMT×6, AQST×140 | QTRX, MRNA, AUGO, SSRM, ARIS, NOG | — | $20,246.40 | $-9,938.63 | $10,307.77 | AEM×3, WYFI×30, TOYO×145, ABCL×54, TEAM×3, AAP×13, WMT×6, AQST×140, QTRX×276, MRNA×6, AUGO×9, SSRM×22, ARIS×41, NOG×31 | SHORT QTRX x276 @ 3.11; SHORT MRNA x6 @ 133.11; SHORT AUGO x9 @ 89.10; SHORT SSRM x22 @ 38.40; SHORT ARIS x41 @ 20.90; SHORT NOG x31 @ 27.00 |
| 2026-08-24 | -5.17 | $20,246.40 | AEM×3, WYFI×30, TOYO×145, ABCL×54, TEAM×3, AAP×13, WMT×6, AQST×140, QTRX×276, MRNA×6, AUGO×9, SSRM×22, ARIS×41, NOG×31 | — | — | $20,246.40 | $-9,734.48 | $10,511.92 | AEM×3, WYFI×30, TOYO×145, ABCL×54, TEAM×3, AAP×13, WMT×6, AQST×140, QTRX×276, MRNA×6, AUGO×9, SSRM×22, ARIS×41, NOG×31 | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $20,246.40 | AEM×3, WYFI×30, TOYO×145, ABCL×54, TEAM×3, AAP×13, WMT×6, AQST×140, QTRX×276, MRNA×6, AUGO×9, SSRM×22, ARIS×41, NOG×31 | BMO, AVAH | AEM, WYFI, TOYO, ABCL, TEAM, AAP, WMT, AQST | $20,580.94 | $-10,144.83 | $10,436.11 | QTRX×276, MRNA×6, AUGO×9, SSRM×22, ARIS×41, NOG×31, BMO×15, AVAH×190 | SELL AEM (dropped from list after 3 sess (min 3)); SELL WYFI (dropped from list after 3 sess (min 3)); SELL TOYO (dropped from list after 3 sess (min 3)); SELL ABCL (dropped from list after 3 sess (min 3)); SELL TEAM (dropped from list after 3 sess (min 3)); SELL AAP (dropped from list after 3 sess (min 3)); SELL WMT (dropped from list after 3 sess (min 3)); SELL AQST (dropped from list after 3 sess (min 3)); SHORT BMO x15 @ 172.40; SHORT AVAH x190 @ 13.70 |
| 2026-08-26 | +2.02 | $20,580.94 | QTRX×276, MRNA×6, AUGO×9, SSRM×22, ARIS×41, NOG×31, BMO×15, AVAH×190 | — | — | $20,580.94 | $-10,119.65 | $10,461.29 | QTRX×276, MRNA×6, AUGO×9, SSRM×22, ARIS×41, NOG×31, BMO×15, AVAH×190 | hold QTRX,MRNA,AUGO,SSRM,ARIS,NOG,BMO,AVAH |
| 2026-08-27 | — | $20,580.94 | QTRX×276, MRNA×6, AUGO×9, SSRM×22, ARIS×41, NOG×31, BMO×15, AVAH×190 | — | QTRX, MRNA, AUGO, SSRM, ARIS, NOG | $15,575.14 | $-5,181.30 | $10,393.84 | BMO×15, AVAH×190 | SELL QTRX (dropped from list after 4 sess (min 3)); SELL MRNA (dropped from list after 4 sess (min 3)); SELL AUGO (dropped from list after 4 sess (min 3)); SELL SSRM (dropped from list after 4 sess (min 3)); SELL ARIS (dropped from list after 4 sess (min 3)); SELL NOG (dropped from list after 4 sess (min 3)) |
| 2026-08-28 | +0.75 | $15,575.14 | BMO×15, AVAH×190 | SIMO, NOG | BMO, AVAH | $15,406.49 | $-4,903.72 | $10,502.77 | SIMO×9, NOG×100 | SELL BMO (dropped from list after 3 sess (min 3)); SELL AVAH (dropped from list after 3 sess (min 3)); SHORT SIMO x9 @ 272.00; SHORT NOG x100 @ 25.73 |
| 2026-08-31 | -5.85 | $15,406.49 | SIMO×9, NOG×100 | — | — | $15,406.49 | $-4,794.11 | $10,612.38 | SIMO×9, NOG×100 | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $15,406.49 | SIMO×9, NOG×100 | — | — | $15,406.49 | $-4,806.80 | $10,599.69 | SIMO×9, NOG×100 | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $15,406.49 | SIMO×9, NOG×100 | — | SIMO | $13,243.66 | $-2,636.00 | $10,607.66 | NOG×100 | SELL SIMO (dropped from list after 3 sess (min 3)); hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $13,243.66 | NOG×100 | SLN, NIQ, TX | — | $18,508.99 | $-7,941.02 | $10,567.97 | NOG×100, SLN×120, NIQ×95, TX×31 | SHORT SLN x120 @ 14.70; SHORT NIQ x95 @ 18.60; SHORT TX x31 @ 56.17 |
| 2026-09-04 | — | $18,508.99 | NOG×100, SLN×120, NIQ×95, TX×31 | GSM, OPK | — | $23,731.41 | $-13,034.24 | $10,697.17 | NOG×100, SLN×120, NIQ×95, TX×31, GSM×577, OPK×1535 | SHORT GSM x577 @ 4.55; SHORT OPK x1535 @ 1.71 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 1412 | $1.18 | $18.51 | — | $11,647.65 | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 86 | $19.17 | $2.32 | — | $13,293.95 | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 131 | $12.70 | $2.46 | — | $14,954.53 | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 judge🟢 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 1077 | $1.15 | $14.12 | — | $16,178.97 | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; leftover $1239.60 | join🟡 sector🟢 gen🟢 news🔴 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 348 | $3.56 | $4.59 | — | $17,413.26 | news🔴; gate news=bad; list yday_mover; ret5=-15.6; leftover $1239.60 | join🟡 sector🔴 gen🟢 news🔴 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 39 | $31.70 | $2.16 | — | $18,647.39 | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; leftover $1239.60 | join🟡 sector🔴 gen🟢 news🔴 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 411 | $3.01 | $5.41 | — | $19,879.09 | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; leftover $1239.60 | join🟢 sector🟢 gen🟢 news🔴 judge🟢 vol🟡 buy🟡 |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 1412 | $1.07 | $18.21 | $+118.60 | $18,350.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 131 | $11.75 | $2.38 | $+118.95 | $16,808.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 86 | $18.13 | $2.25 | $+84.87 | $15,246.97 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 1077 | $0.96 | $13.60 | $+173.68 | $14,196.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 348 | $4.01 | $4.49 | $-167.42 | $12,794.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 39 | $31.87 | $2.11 | $-10.90 | $11,549.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 411 | $2.95 | $5.30 | $+13.94 | $10,331.72 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $10,943.04 | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $645.73 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 30 | $21.40 | $2.12 | — | $11,582.92 | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; leftover $645.73 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 145 | $4.43 | $2.48 | — | $12,222.79 | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; leftover $645.73 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 54 | $11.81 | $2.19 | — | $12,858.61 | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $645.73 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 3 | $173.90 | $2.03 | — | $13,378.28 | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; leftover $645.73 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 13 | $46.85 | $2.07 | — | $13,985.26 | news🔴; gate news=bad; list earn_react; 🔵; ret5=+5.0; leftover $645.73 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 6 | $106.13 | $2.05 | — | $14,619.97 | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; leftover $645.73 | join🟢 sector🟡 gen🟢 news🔴 digest🟡 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 140 | $4.61 | $2.46 | — | $15,262.91 | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; leftover $645.73 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 276 | $3.11 | $3.64 | — | $16,117.63 | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $861.06 | join🟡 sector🟢 gen🟢 news🔴 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 6 | $133.11 | $2.05 | — | $16,914.24 | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; leftover $861.06 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 9 | $89.10 | $2.06 | — | $17,714.09 | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $861.06 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 22 | $38.40 | $2.10 | — | $18,556.79 | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $861.06 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 41 | $20.90 | $2.16 | — | $19,411.53 | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $861.06 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 31 | $27.00 | $2.13 | — | $20,246.40 | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; leftover $861.06 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 3 | $200.48 | $2.00 | $+7.88 | $19,642.96 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 30 | $20.98 | $2.08 | $+8.40 | $19,011.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 145 | $4.48 | $2.42 | $-12.15 | $18,359.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 54 | $10.77 | $2.15 | $+52.09 | $17,775.73 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 3 | $170.65 | $2.00 | $+5.72 | $17,261.78 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 13 | $43.61 | $2.03 | $+38.03 | $16,692.82 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 6 | $106.54 | $2.01 | $-6.54 | $16,051.57 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 140 | $4.66 | $2.41 | $-11.87 | $15,396.76 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 15 | $172.40 | $2.14 | — | $17,980.63 | news🔴; gate news=bad; list earn_react; ret5=-6.1; leftover $2616.53 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 190 | $13.70 | $2.68 | — | $20,580.94 | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+6.8; leftover $2616.53 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **COVER** | `QTRX` | 276 | $2.83 | $3.56 | $+70.08 | $19,796.30 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `MRNA` | 6 | $154.20 | $2.01 | $-130.60 | $18,869.09 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `AUGO` | 9 | $88.24 | $2.02 | $+3.66 | $18,072.92 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `SSRM` | 22 | $38.41 | $2.06 | $-4.38 | $17,225.84 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `ARIS` | 41 | $20.50 | $2.11 | $+12.13 | $16,383.23 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `NOG` | 31 | $26.00 | $2.08 | $+26.79 | $15,575.14 | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMO` | 15 | $172.85 | $2.04 | $-10.92 | $12,980.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 190 | $13.62 | $2.56 | $+9.96 | $10,390.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 9 | $272.00 | $2.11 | — | $12,835.89 | news🔴; gate news=bad; list yday_gainer; ⚪; ret5=-3.9; leftover $2597.50 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `NOG` | 100 | $25.73 | $2.40 | — | $15,406.49 | news🔴; gate news=bad; list ohlc_hot; ret5=+11.6; leftover $2597.50 | join🔴 sector🔴 gen🟡 news🔴 digest🟢 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 9 | $240.09 | $2.02 | $+283.06 | $13,243.66 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 120 | $14.70 | $2.43 | — | $15,005.23 | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1772.28 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `NIQ` | 95 | $18.60 | $2.35 | — | $16,769.88 | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+7.6; leftover $1772.28 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `TX` | 31 | $56.17 | $2.15 | — | $18,508.99 | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+3.0; leftover $1772.28 | join🔴 sector🟢 gen🟡 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 577 | $4.55 | $7.63 | — | $21,126.71 | news🔴; gate news=bad; list yday_gainer; ret5=-7.1; leftover $2626.42 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `OPK` | 1535 | $1.71 | $20.15 | — | $23,731.41 | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+7.2; leftover $2626.42 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

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
