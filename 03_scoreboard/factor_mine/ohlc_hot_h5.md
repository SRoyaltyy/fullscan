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

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-14 | +5.50 | $10,000.00 | — | ADUR, ANRO, LIFE, VOYG, LUNR, BETA, FORM, ENTG | — | $250.70 | $9,630.86 | $9,881.56 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7 | BUY ADUR x75 @ 16.50; BUY ANRO x39 @ 31.77; BUY LIFE x35 @ 35.04; BUY VOYG x28 @ 44.49; BUY LUNR x65 @ 19.17; BUY BETA x49 @ 25.21; BUY FORM x9 @ 129.48; BUY ENTG x7 @ 162.45 |
| 2026-08-17 | +2.25 | $250.70 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7 | OCC, ALM, LPTH, CLYM, BORR, IOVA | — | $69.50 | $10,107.03 | $10,176.53 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7, OCC×1, ALM×2, LPTH×2, CLYM×2, BORR×7, IOVA×5 | BUY OCC x1 @ 18.24; BUY ALM x2 @ 16.20; BUY LPTH x2 @ 14.94; BUY CLYM x2 @ 16.25; BUY BORR x7 @ 4.59; BUY IOVA x5 @ 6.84 |
| 2026-08-18 | -6.20 | $69.50 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7, OCC×1, ALM×2, LPTH×2, CLYM×2, BORR×7, IOVA×5 | — | — | $69.50 | $9,790.51 | $9,860.01 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7, OCC×1, ALM×2, LPTH×2, CLYM×2, BORR×7, IOVA×5 | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $69.50 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7, OCC×1, ALM×2, LPTH×2, CLYM×2, BORR×7, IOVA×5 | — | — | $69.50 | $9,473.05 | $9,542.55 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7, OCC×1, ALM×2, LPTH×2, CLYM×2, BORR×7, IOVA×5 | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $69.50 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7, OCC×1, ALM×2, LPTH×2, CLYM×2, BORR×7, IOVA×5 | ABTC, SBET | — | $53.32 | $9,428.83 | $9,482.15 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7, OCC×1, ALM×2, LPTH×2, CLYM×2, BORR×7, IOVA×5, ABTC×1, SBET×1 | BUY ABTC x1 @ 8.46; BUY SBET x1 @ 7.55 |
| 2026-08-21 | +3.25 | $53.32 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7, OCC×1, ALM×2, LPTH×2, CLYM×2, BORR×7, IOVA×5, ABTC×1, SBET×1 | AEM, ORBS, GRAL, MSTR, TRON, XHG, AUGO | ADUR, ANRO, LIFE, VOYG, LUNR, BETA, FORM, ENTG | $44.25 | $9,549.28 | $9,593.53 | OCC×1, ALM×2, LPTH×2, CLYM×2, BORR×7, IOVA×5, ABTC×1, SBET×1, AEM×6, ORBS×1554, GRAL×17, MSTR×11, TRON×692, XHG×299, AUGO×15 | SELL ADUR (dropped from list after 5 sess (min 5)); SELL ANRO (dropped from list after 5 sess (min 5)); SELL LIFE (dropped from list after 5 sess (min 5)); SELL VOYG (dropped from list after 5 sess (min 5)); SELL LUNR (dropped from list after 5 sess (min 5)); SELL BETA (dropped from list after 5 sess (min 5)); SELL FORM (dropped from list after 5 sess (min 5)); SELL ENTG (dropped from list after 5 sess (min 5)); BUY AEM x6 @ 216.30; BUY ORBS x1554 @ 0.86; BUY GRAL x17 @ 78.88; BUY MSTR x11 @ 119.69; BUY TRON x692 @ 1.94; BUY XHG x299 @ 4.49; BUY AUGO x15 @ 89.10 |
| 2026-08-24 | -5.17 | $44.25 | OCC×1, ALM×2, LPTH×2, CLYM×2, BORR×7, IOVA×5, ABTC×1, SBET×1, AEM×6, ORBS×1554, GRAL×17, MSTR×11, TRON×692, XHG×299, AUGO×15 | — | OCC, ALM, LPTH, CLYM, BORR, IOVA | $227.20 | $9,260.96 | $9,488.16 | ABTC×1, SBET×1, AEM×6, ORBS×1554, GRAL×17, MSTR×11, TRON×692, XHG×299, AUGO×15 | SELL OCC (dropped from list after 5 sess (min 5)); SELL ALM (dropped from list after 5 sess (min 5)); SELL LPTH (dropped from list after 5 sess (min 5)); SELL CLYM (dropped from list after 5 sess (min 5)); SELL BORR (dropped from list after 5 sess (min 5)); SELL IOVA (dropped from list after 5 sess (min 5)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $227.20 | ABTC×1, SBET×1, AEM×6, ORBS×1554, GRAL×17, MSTR×11, TRON×692, XHG×299, AUGO×15 | DEFT, AMTX, NIQ, OMER, TRLV | — | $101.77 | $9,302.73 | $9,404.50 | ABTC×1, SBET×1, AEM×6, ORBS×1554, GRAL×17, MSTR×11, TRON×692, XHG×299, AUGO×15, DEFT×50, AMTX×17, NIQ×1, OMER×1, TRLV×2 | BUY DEFT x50 @ 0.64; BUY AMTX x17 @ 1.86; BUY NIQ x1 @ 19.56; BUY OMER x1 @ 18.75; BUY TRLV x2 @ 11.02 |
| 2026-08-26 | +2.02 | $101.77 | ABTC×1, SBET×1, AEM×6, ORBS×1554, GRAL×17, MSTR×11, TRON×692, XHG×299, AUGO×15, DEFT×50, AMTX×17, NIQ×1, OMER×1, TRLV×2 | — | — | $101.77 | $9,303.51 | $9,405.28 | ABTC×1, SBET×1, AEM×6, ORBS×1554, GRAL×17, MSTR×11, TRON×692, XHG×299, AUGO×15, DEFT×50, AMTX×17, NIQ×1, OMER×1, TRLV×2 | hold ABTC,SBET,AEM,ORBS,GRAL,MSTR,TRON,XHG,AUGO,DEFT,AMTX,NIQ,OMER,TRLV |
| 2026-08-27 | — | $101.77 | ABTC×1, SBET×1, AEM×6, ORBS×1554, GRAL×17, MSTR×11, TRON×692, XHG×299, AUGO×15, DEFT×50, AMTX×17, NIQ×1, OMER×1, TRLV×2 | — | ABTC, SBET | $118.56 | $9,398.08 | $9,516.64 | AEM×6, ORBS×1554, GRAL×17, MSTR×11, TRON×692, XHG×299, AUGO×15, DEFT×50, AMTX×17, NIQ×1, OMER×1, TRLV×2 | SELL ABTC (dropped from list after 5 sess (min 5)); SELL SBET (dropped from list after 5 sess (min 5)) |
| 2026-08-28 | +0.75 | $118.56 | AEM×6, ORBS×1554, GRAL×17, MSTR×11, TRON×692, XHG×299, AUGO×15, DEFT×50, AMTX×17, NIQ×1, OMER×1, TRLV×2 | ZYME, ERO, FUTU | AEM, ORBS, GRAL, MSTR, TRON, AUGO | $73.81 | $9,362.17 | $9,435.98 | XHG×299, DEFT×50, AMTX×17, NIQ×1, OMER×1, TRLV×2, ZYME×93, ERO×70, FUTU×21 | SELL AEM (dropped from list after 5 sess (min 5)); SELL ORBS (dropped from list after 5 sess (min 5)); SELL GRAL (dropped from list after 5 sess (min 5)); SELL MSTR (dropped from list after 5 sess (min 5)); SELL TRON (dropped from list after 5 sess (min 5)); SELL AUGO (dropped from list after 5 sess (min 5)); BUY ZYME x93 @ 29.33; BUY ERO x70 @ 39.20; BUY FUTU x21 @ 128.00 |
| 2026-08-31 | -5.85 | $73.81 | XHG×299, DEFT×50, AMTX×17, NIQ×1, OMER×1, TRLV×2, ZYME×93, ERO×70, FUTU×21 | — | — | $73.81 | $9,082.63 | $9,156.44 | XHG×299, DEFT×50, AMTX×17, NIQ×1, OMER×1, TRLV×2, ZYME×93, ERO×70, FUTU×21 | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $73.81 | XHG×299, DEFT×50, AMTX×17, NIQ×1, OMER×1, TRLV×2, ZYME×93, ERO×70, FUTU×21 | — | AMTX, NIQ | $124.06 | $8,885.51 | $9,009.57 | XHG×299, DEFT×50, OMER×1, TRLV×2, ZYME×93, ERO×70, FUTU×21 | SELL AMTX (dropped from list after 5 sess (min 5)); SELL NIQ (dropped from list after 5 sess (min 5)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $124.06 | XHG×299, DEFT×50, OMER×1, TRLV×2, ZYME×93, ERO×70, FUTU×21 | — | — | $124.06 | $8,826.31 | $8,950.37 | XHG×299, DEFT×50, OMER×1, TRLV×2, ZYME×93, ERO×70, FUTU×21 | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $124.06 | XHG×299, DEFT×50, OMER×1, TRLV×2, ZYME×93, ERO×70, FUTU×21 | NVAX, NIQ | — | $5.43 | $8,984.92 | $8,990.35 | XHG×299, DEFT×50, OMER×1, TRLV×2, ZYME×93, ERO×70, FUTU×21, NVAX×6, NIQ×3 | BUY NVAX x6 @ 10.27; BUY NIQ x3 @ 18.60 |
| 2026-09-04 | — | $5.43 | XHG×299, DEFT×50, OMER×1, TRLV×2, ZYME×93, ERO×70, FUTU×21, NVAX×6, NIQ×3 | — | ZYME | $2,917.75 | $6,255.77 | $9,173.52 | XHG×299, DEFT×50, OMER×1, TRLV×2, ERO×70, FUTU×21, NVAX×6, NIQ×3 | SELL ZYME (dropped from list after 5 sess (min 5)) |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $8,760.28 | ▼ $9,997.78 (-2.22) | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANRO` | 39 | $31.77 | $2.11 | — | $7,519.15 | ▼ $9,995.68 (-4.32) | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+13.5; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LIFE` | 35 | $35.04 | $2.10 | — | $6,290.65 | ▼ $9,993.58 (-6.42) | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+16.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VOYG` | 28 | $44.49 | $2.07 | — | $5,042.86 | ▼ $9,991.51 (-8.49) | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 65 | $19.17 | $2.19 | — | $3,794.62 | ▼ $9,989.32 (-10.68) | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETA` | 49 | $25.21 | $2.14 | — | $2,557.20 | ▼ $9,987.19 (-12.81) | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+15.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `FORM` | 9 | $129.48 | $2.02 | — | $1,389.86 | ▼ $9,985.17 (-14.83) | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+14.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ENTG` | 7 | $162.45 | $2.01 | — | $250.70 | ▼ $9,983.16 (-16.84) | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+14.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 1 | $18.24 | $0.19 | — | $232.27 | ▼ $9,917.39 (-82.61) | baseline list, no extra gate; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $35.81 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 2 | $16.20 | $0.33 | — | $199.54 | ▼ $9,917.06 (-82.94) | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $35.81 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 2 | $14.94 | $0.30 | — | $169.36 | ▼ $9,916.76 (-83.24) | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $35.81 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `CLYM` | 2 | $16.25 | $0.33 | — | $136.53 | ▼ $9,916.43 (-83.57) | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+16.6; leftover $35.81 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 7 | $4.59 | $0.34 | — | $104.06 | ▼ $9,916.09 (-83.91) | baseline list, no extra gate; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; leftover $35.81 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `IOVA` | 5 | $6.84 | $0.36 | — | $69.50 | ▼ $9,915.73 (-84.27) | baseline list, no extra gate; list ohlc_hot; ret5=+10.1; leftover $35.81 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABTC` | 1 | $8.46 | $0.09 | — | $60.95 | ▼ $9,474.15 (-525.85) | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+14.0; leftover $8.69 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SBET` | 1 | $7.55 | $0.08 | — | $53.32 | ▼ $9,474.07 (-525.93) | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+14.6; leftover $8.69 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ADUR` | 75 | $16.00 | $2.24 | $-41.95 | $1,251.09 | ▼ $9,621.92 (-378.08) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ANRO` | 39 | $34.44 | $2.13 | $+99.90 | $2,592.12 | ▼ $9,619.79 (-380.21) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LIFE` | 35 | $33.90 | $2.12 | $-44.11 | $3,776.50 | ▼ $9,617.67 (-382.33) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `VOYG` | 28 | $38.84 | $2.09 | $-162.37 | $4,861.93 | ▼ $9,615.58 (-384.42) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LUNR` | 65 | $18.74 | $2.21 | $-32.34 | $6,077.82 | ▼ $9,613.37 (-386.63) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BETA` | 49 | $25.56 | $2.16 | $+12.86 | $7,328.11 | ▼ $9,611.22 (-388.78) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `FORM` | 9 | $117.69 | $2.04 | $-110.16 | $8,385.28 | ▼ $9,609.18 (-390.82) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ENTG` | 7 | $145.64 | $2.03 | $-121.71 | $9,402.73 | ▼ $9,607.15 (-392.85) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $8,102.92 | ▼ $9,605.14 (-394.86) | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1343.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1554 | $0.86 | $18.09 | — | $6,742.17 | ▼ $9,587.05 (-412.95) | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1343.25 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 17 | $78.88 | $2.04 | — | $5,399.17 | ▼ $9,585.01 (-414.99) | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1343.25 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MSTR` | 11 | $119.69 | $2.02 | — | $4,080.56 | ▼ $9,582.99 (-417.01) | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+15.7; leftover $1343.25 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TRON` | 692 | $1.94 | $8.93 | — | $2,729.15 | ▼ $9,574.06 (-425.94) | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+15.4; leftover $1343.25 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 299 | $4.49 | $3.86 | — | $1,382.79 | ▼ $9,570.20 (-429.80) | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+12.7; leftover $1343.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUGO` | 15 | $89.10 | $2.04 | — | $44.25 | ▼ $9,568.17 (-431.83) | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.8; leftover $1343.25 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `OCC` | 1 | $13.60 | $0.16 | $-4.98 | $57.69 | ▼ $9,675.21 (-324.79) | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `ALM` | 2 | $18.69 | $0.40 | $+4.25 | $94.67 | ▼ $9,674.81 (-325.19) | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `LPTH` | 2 | $13.92 | $0.30 | $-2.65 | $122.21 | ▼ $9,674.51 (-325.49) | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `CLYM` | 2 | $17.27 | $0.37 | $+1.34 | $156.38 | ▼ $9,674.14 (-325.86) | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `BORR` | 7 | $4.48 | $0.35 | $-1.47 | $187.38 | ▼ $9,673.78 (-326.22) | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 5 | $8.05 | $0.44 | $+5.26 | $227.20 | ▼ $9,673.35 (-326.65) | dropped from list after 5 sess (min 5) | — |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 50 | $0.64 | $0.47 | — | $194.73 | ▼ $9,406.27 (-593.73) | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $32.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AMTX` | 17 | $1.86 | $0.37 | — | $162.74 | ▼ $9,405.90 (-594.10) | baseline list, no extra gate; list yday_mover,ohlc_hot; ⚪; ret5=+16.9; leftover $32.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NIQ` | 1 | $19.56 | $0.20 | — | $142.98 | ▼ $9,405.70 (-594.30) | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+7.6; leftover $32.46 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `OMER` | 1 | $18.75 | $0.19 | — | $124.04 | ▼ $9,405.51 (-594.49) | baseline list, no extra gate; list ohlc_hot; ret5=+12.1; leftover $32.46 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `TRLV` | 2 | $11.02 | $0.23 | — | $101.77 | ▼ $9,405.28 (-594.72) | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.0; leftover $32.46 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `ABTC` | 1 | $8.84 | $0.11 | $+0.18 | $110.50 | ▼ $9,434.90 (-565.10) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `SBET` | 1 | $8.16 | $0.10 | $+0.43 | $118.56 | ▼ $9,434.80 (-565.20) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AEM` | 6 | $214.11 | $2.03 | $-17.18 | $1,401.19 | ▼ $9,608.43 (-391.57) | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ORBS` | 1554 | $0.82 | $17.67 | $-104.14 | $2,657.80 | ▼ $9,590.76 (-409.24) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRAL` | 17 | $79.00 | $2.06 | $-2.06 | $3,998.73 | ▼ $9,588.69 (-411.31) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `MSTR` | 11 | $126.77 | $2.04 | $+73.81 | $5,391.16 | ▼ $9,586.65 (-413.35) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `TRON` | 692 | $2.21 | $9.05 | $+168.86 | $6,911.43 | ▼ $9,577.60 (-422.40) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUGO` | 15 | $88.71 | $2.06 | $-9.94 | $8,240.02 | ▼ $9,575.54 (-424.46) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 93 | $29.33 | $2.27 | — | $5,510.06 | ▼ $9,573.27 (-426.73) | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $2746.67 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERO` | 70 | $39.20 | $2.20 | — | $2,763.86 | ▼ $9,571.07 (-428.93) | baseline list, no extra gate; list ohlc_hot; ret5=+16.6; leftover $2746.67 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `FUTU` | 21 | $128.00 | $2.05 | — | $73.81 | ▼ $9,569.02 (-430.98) | baseline list, no extra gate; list ohlc_hot; ret5=+17.5; leftover $2746.67 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `AMTX` | 17 | $1.87 | $0.39 | $-0.59 | $105.21 | ▼ $9,153.20 (-846.80) | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `NIQ` | 1 | $19.06 | $0.21 | $-0.91 | $124.06 | ▼ $9,152.99 (-847.01) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 6 | $10.27 | $0.63 | — | $61.80 | ▼ $9,058.94 (-941.06) | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $62.03 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NIQ` | 3 | $18.60 | $0.57 | — | $5.43 | ▼ $9,058.37 (-941.63) | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+7.6; leftover $62.03 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `ZYME` | 93 | $31.34 | $2.31 | $+182.35 | $2,917.75 | ▼ $9,111.47 (-888.53) | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |

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
