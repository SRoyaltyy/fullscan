# Factor mine action — `union_vol_ab_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-6.81%** ($9,319) · signal-only (no cash/fees) was -3.28%. Starts YES **0/17**. Fills 70 · skips 92 · realized $-608.28.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `vol=good,ab=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $60.25.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-14 | +5.50 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-17 | +2.25 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-18 | -6.20 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | hard-red sit S=-6.20 |
| 2026-08-19 | -7.20 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | hard-red sit S=-7.20 |
| 2026-08-20 | +1.12 | $10,000.00 | — | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $186.91 | $10,021.37 | $10,208.28 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8 | BUY AG x60 @ 20.55; BUY BHP x13 @ 91.01; BUY CDE x60 @ 20.65; BUY HDSN x216 @ 5.77; BUY IAG x63 @ 19.63; BUY KGC x42 @ 29.63; BUY NFGC x714 @ 1.75; BUY WPM x8 @ 144.54 |
| 2026-08-21 | +3.25 | $186.91 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8 | AUPH, ARCT, AUTL, CRDL, CYPH | — | $78.42 | $10,396.51 | $10,474.93 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 | BUY AUPH x1 @ 17.20; BUY ARCT x2 @ 11.13; BUY AUTL x9 @ 2.47; BUY CRDL x12 @ 1.93; BUY CYPH x17 @ 1.32 |
| 2026-08-24 | -5.17 | $78.42 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 | — | — | $78.42 | $10,368.34 | $10,446.76 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $78.42 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 | BMEA, ALVO, ZURA, DEFT, RUM, KURA, EZPW | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $2.29 | $10,459.12 | $10,461.41 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17, BMEA×914, ALVO×283, ZURA×232, DEFT×2315, RUM×158, KURA×111, EZPW×42 | SELL AG (dropped from list after 3 sess (min 3)); SELL BHP (dropped from list after 3 sess (min 3)); SELL CDE (dropped from list after 3 sess (min 3)); SELL HDSN (dropped from list after 3 sess (min 3)); SELL IAG (dropped from list after 3 sess (min 3)); SELL KGC (dropped from list after 3 sess (min 3)); SELL NFGC (dropped from list after 3 sess (min 3)); SELL WPM (dropped from list after 3 sess (min 3)); BUY BMEA x914 @ 1.62; BUY ALVO x283 @ 5.22; BUY ZURA x232 @ 6.38; BUY DEFT x2315 @ 0.64; BUY RUM x158 @ 9.36; BUY KURA x111 @ 13.30; BUY EZPW x42 @ 34.48 |
| 2026-08-26 | +2.02 | $2.29 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17, BMEA×914, ALVO×283, ZURA×232, DEFT×2315, RUM×158, KURA×111, EZPW×42 | — | — | $2.29 | $10,441.01 | $10,443.30 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17, BMEA×914, ALVO×283, ZURA×232, DEFT×2315, RUM×158, KURA×111, EZPW×42 | hold AUPH,ARCT,AUTL,CRDL,CYPH,BMEA,ALVO,ZURA,DEFT,RUM,KURA,EZPW |
| 2026-08-27 | — | $2.29 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17, BMEA×914, ALVO×283, ZURA×232, DEFT×2315, RUM×158, KURA×111, EZPW×42 | — | AUPH, ARCT, AUTL, CRDL, CYPH | $121.41 | $10,063.50 | $10,184.91 | BMEA×914, ALVO×283, ZURA×232, DEFT×2315, RUM×158, KURA×111, EZPW×42 | SELL AUPH (dropped from list after 4 sess (min 3)); SELL ARCT (dropped from list after 4 sess (min 3)); SELL AUTL (dropped from list after 4 sess (min 3)); SELL CRDL (dropped from list after 4 sess (min 3)); SELL CYPH (dropped from list after 4 sess (min 3)) |
| 2026-08-28 | +0.75 | $121.41 | BMEA×914, ALVO×283, ZURA×232, DEFT×2315, RUM×158, KURA×111, EZPW×42 | ANF, BZ, SMTC, URBN, BBWI, CRDL, TIGR, FINV | BMEA, ALVO, ZURA, DEFT, RUM, KURA, EZPW | $226.83 | $9,598.79 | $9,825.62 | ANF×8, BZ×68, SMTC×8, URBN×15, BBWI×68, CRDL×608, TIGR×231, FINV×298 | SELL BMEA (dropped from list after 3 sess (min 3)); SELL ALVO (dropped from list after 3 sess (min 3)); SELL ZURA (dropped from list after 3 sess (min 3)); SELL DEFT (dropped from list after 3 sess (min 3)); SELL RUM (dropped from list after 3 sess (min 3)); SELL KURA (dropped from list after 3 sess (min 3)); SELL EZPW (dropped from list after 3 sess (min 3)); BUY ANF x8 @ 144.70; BUY BZ x68 @ 18.50; BUY SMTC x8 @ 149.40; BUY URBN x15 @ 82.70; BUY BBWI x68 @ 18.68; BUY CRDL x608 @ 2.09; BUY TIGR x231 @ 5.49; BUY FINV x298 @ 4.26 |
| 2026-08-31 | -5.85 | $226.83 | ANF×8, BZ×68, SMTC×8, URBN×15, BBWI×68, CRDL×608, TIGR×231, FINV×298 | — | — | $226.83 | $9,375.14 | $9,601.97 | ANF×8, BZ×68, SMTC×8, URBN×15, BBWI×68, CRDL×608, TIGR×231, FINV×298 | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $226.83 | ANF×8, BZ×68, SMTC×8, URBN×15, BBWI×68, CRDL×608, TIGR×231, FINV×298 | — | — | $226.83 | $9,309.21 | $9,536.04 | ANF×8, BZ×68, SMTC×8, URBN×15, BBWI×68, CRDL×608, TIGR×231, FINV×298 | hard-red sit S=-6.30 |
| 2026-09-02 | -3.83 | $226.83 | ANF×8, BZ×68, SMTC×8, URBN×15, BBWI×68, CRDL×608, TIGR×231, FINV×298 | — | ANF, BZ, SMTC, URBN, BBWI, CRDL, TIGR, FINV | $9,391.74 | $0.00 | $9,391.74 | — | SELL ANF (dropped from list after 3 sess (min 3)); SELL BZ (dropped from list after 3 sess (min 3)); SELL SMTC (dropped from list after 3 sess (min 3)); SELL URBN (dropped from list after 3 sess (min 3)); SELL BBWI (dropped from list after 3 sess (min 3)); SELL CRDL (dropped from list after 3 sess (min 3)); SELL TIGR (dropped from list after 3 sess (min 3)); SELL FINV (dropped from list after 3 sess (min 3)); hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $9,391.74 | — | RVTY, CRK, MMED, EIX, CRDL, MRNA, ARCT, NVAX | — | $205.01 | $9,242.98 | $9,447.99 | RVTY×9, CRK×74, MMED×51, EIX×20, CRDL×543, MRNA×7, ARCT×71, NVAX×114 | BUY RVTY x9 @ 125.94; BUY CRK x74 @ 15.70; BUY MMED x51 @ 22.78; BUY EIX x20 @ 56.78; BUY CRDL x543 @ 2.16; BUY MRNA x7 @ 151.40; BUY ARCT x71 @ 16.46; BUY NVAX x114 @ 10.27 |
| 2026-09-04 | — | $205.01 | RVTY×9, CRK×74, MMED×51, EIX×20, CRDL×543, MRNA×7, ARCT×71, NVAX×114 | CABA, BAK, SGLD, IRD, OABI, ALEC | — | $60.25 | $9,258.41 | $9,318.66 | RVTY×9, CRK×74, MMED×51, EIX×20, CRDL×543, MRNA×7, ARCT×71, NVAX×114, CABA×7, BAK×13, SGLD×3, IRD×5, OABI×5, ALEC×9 | BUY CABA x7 @ 3.63; BUY BAK x13 @ 1.95; BUY SGLD x3 @ 6.48; BUY IRD x5 @ 4.66; BUY OABI x5 @ 5.08; BUY ALEC x9 @ 2.70 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-20 09:30 ET | **BUY** | `AG` | 60 | $20.55 | $2.17 | — | $8,764.83 | ▼ $9,997.83 (-2.17) | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,579.67 | ▼ $9,995.80 (-4.20) | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 60 | $20.65 | $2.17 | — | $6,338.50 | ▼ $9,993.63 (-6.37) | combo gate; gate vol=good,ab=good; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 216 | $5.77 | $2.79 | — | $5,089.39 | ▼ $9,990.84 (-9.16) | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $3,850.53 | ▼ $9,988.67 (-11.33) | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,603.95 | ▼ $9,986.55 (-13.45) | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 714 | $1.75 | $9.21 | — | $1,345.24 | ▼ $9,977.34 (-22.66) | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $186.91 | ▼ $9,975.32 (-24.68) | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $169.53 | ▲ $10,475.32 (+475.32) | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $147.04 | ▲ $10,475.09 (+475.09) | combo gate; gate vol=good,ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 9 | $2.47 | $0.25 | — | $124.56 | ▲ $10,474.84 (+474.84) | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 12 | $1.93 | $0.27 | — | $101.13 | ▲ $10,474.57 (+474.57) | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 17 | $1.32 | $0.28 | — | $78.42 | ▲ $10,474.30 (+474.30) | combo gate; gate vol=good,ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 60 | $20.73 | $2.19 | $+6.44 | $1,320.03 | ▲ $10,513.18 (+513.18) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.95 | $2.05 | $+60.14 | $2,565.33 | ▲ $10,511.13 (+511.13) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 60 | $20.85 | $2.19 | $+7.64 | $3,814.14 | ▲ $10,508.94 (+508.94) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 216 | $5.53 | $2.83 | $-57.46 | $5,005.79 | ▲ $10,506.11 (+506.11) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 63 | $21.63 | $2.20 | $+121.62 | $6,366.28 | ▲ $10,503.91 (+503.91) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 42 | $32.76 | $2.14 | $+127.21 | $7,740.06 | ▲ $10,501.77 (+501.77) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 714 | $1.91 | $9.34 | $+95.69 | $9,094.46 | ▲ $10,492.43 (+492.43) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $160.00 | $2.03 | $+119.63 | $10,372.43 | ▲ $10,490.40 (+490.40) | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 914 | $1.62 | $11.79 | — | $8,879.96 | ▲ $10,478.61 (+478.61) | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1481.78 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 283 | $5.22 | $3.65 | — | $7,399.04 | ▲ $10,474.95 (+474.95) | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1481.78 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 232 | $6.38 | $2.99 | — | $5,915.89 | ▲ $10,471.96 (+471.96) | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1481.78 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2315 | $0.64 | $21.76 | — | $4,412.53 | ▲ $10,450.20 (+450.20) | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1481.78 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 158 | $9.36 | $2.46 | — | $2,931.19 | ▲ $10,447.74 (+447.74) | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ret5=+21.3; leftover $1481.78 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 111 | $13.30 | $2.32 | — | $1,452.56 | ▲ $10,445.41 (+445.41) | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ret5=+9.5; leftover $1481.78 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 42 | $34.48 | $2.12 | — | $2.29 | ▲ $10,443.30 (+443.30) | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1481.78 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $18.70 | ▲ $10,546.04 (+546.04) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $49.07 | ▲ $10,545.71 (+545.71) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 9 | $2.41 | $0.26 | $-1.05 | $70.49 | ▲ $10,545.44 (+545.44) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 12 | $2.03 | $0.30 | $+0.63 | $94.55 | ▲ $10,545.14 (+545.14) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 17 | $1.60 | $0.34 | $+4.14 | $121.41 | ▲ $10,544.80 (+544.80) | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 914 | $1.74 | $11.96 | $+85.93 | $1,699.81 | ▲ $10,216.85 (+216.85) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 283 | $4.88 | $3.71 | $-103.58 | $3,077.15 | ▲ $10,213.15 (+213.15) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 232 | $6.02 | $3.04 | $-89.56 | $4,470.74 | ▲ $10,210.10 (+210.10) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DEFT` | 2315 | $0.60 | $21.23 | $-135.59 | $5,838.51 | ▲ $10,188.87 (+188.87) | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `RUM` | 158 | $9.51 | $2.50 | $+18.73 | $7,338.59 | ▲ $10,186.37 (+186.37) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 111 | $12.98 | $2.35 | $-40.20 | $8,777.02 | ▲ $10,184.02 (+184.02) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 42 | $33.50 | $2.14 | $-45.41 | $10,181.88 | ▲ $10,181.88 (+181.88) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $144.70 | $2.01 | — | $9,022.27 | ▲ $10,179.87 (+179.87) | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1272.74 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 68 | $18.50 | $2.19 | — | $7,762.07 | ▲ $10,177.67 (+177.67) | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1272.74 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $6,564.86 | ▲ $10,175.66 (+175.66) | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1272.74 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 15 | $82.70 | $2.04 | — | $5,322.32 | ▲ $10,173.62 (+173.62) | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=-4.6; leftover $1272.74 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 68 | $18.68 | $2.19 | — | $4,049.89 | ▲ $10,171.43 (+171.43) | combo gate; gate vol=good,ab=good; list yday_gainer; ret5=+0.2; leftover $1272.74 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CRDL` | 608 | $2.09 | $7.84 | — | $2,771.33 | ▲ $10,163.59 (+163.59) | combo gate; gate vol=good,ab=good; list yday_gainer; ret5=+3.3; leftover $1272.74 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TIGR` | 231 | $5.49 | $2.98 | — | $1,500.16 | ▲ $10,160.61 (+160.61) | combo gate; gate vol=good,ab=good; list ohlc_hot; ret5=+15.9; leftover $1272.74 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 298 | $4.26 | $3.84 | — | $226.83 | ▲ $10,156.76 (+156.76) | combo gate; gate vol=good,ab=good; list earn_react; ret5=-0.7; leftover $1272.74 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 8 | $142.00 | $2.03 | $-25.65 | $1,360.80 | ▼ $9,415.15 (-584.85) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 68 | $17.29 | $2.22 | $-86.69 | $2,534.30 | ▼ $9,412.93 (-587.07) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $127.63 | $2.03 | $-178.21 | $3,553.31 | ▼ $9,410.90 (-589.10) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 15 | $79.12 | $2.06 | $-57.79 | $4,738.05 | ▼ $9,408.84 (-591.16) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBWI` | 68 | $18.77 | $2.22 | $+1.71 | $6,012.20 | ▼ $9,406.63 (-593.37) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRDL` | 608 | $1.94 | $7.95 | $-107.00 | $7,183.76 | ▼ $9,398.67 (-601.33) | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `TIGR` | 231 | $4.97 | $3.03 | $-126.13 | $8,328.81 | ▼ $9,395.65 (-604.35) | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `FINV` | 298 | $3.58 | $3.90 | $-210.39 | $9,391.74 | ▼ $9,391.74 (-608.26) | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $8,256.27 | ▼ $9,389.73 (-610.27) | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1173.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 74 | $15.70 | $2.21 | — | $7,092.25 | ▼ $9,387.51 (-612.49) | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1173.97 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 51 | $22.78 | $2.14 | — | $5,928.33 | ▼ $9,385.37 (-614.63) | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1173.97 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 20 | $56.78 | $2.05 | — | $4,790.68 | ▼ $9,383.32 (-616.68) | combo gate; gate vol=good,ab=good; list probable,yday_gainer; ret5=+0.3; leftover $1173.97 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 543 | $2.16 | $7.00 | — | $3,610.80 | ▼ $9,376.32 (-623.68) | combo gate; gate vol=good,ab=good; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1173.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 7 | $151.40 | $2.01 | — | $2,548.98 | ▼ $9,374.30 (-625.70) | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1173.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 71 | $16.46 | $2.20 | — | $1,378.12 | ▼ $9,372.10 (-627.90) | combo gate; gate vol=good,ab=good; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1173.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 114 | $10.27 | $2.33 | — | $205.01 | ▼ $9,369.77 (-630.23) | combo gate; gate vol=good,ab=good; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1173.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 7 | $3.63 | $0.28 | — | $179.32 | ▼ $9,449.16 (-550.84) | combo gate; gate vol=good,ab=good; list flatten; 🔵; ⚪; ret5=+13.8; leftover $25.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 13 | $1.95 | $0.29 | — | $153.68 | ▼ $9,448.87 (-551.13) | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $25.63 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SGLD` | 3 | $6.48 | $0.20 | — | $134.04 | ▼ $9,448.67 (-551.33) | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; ret5=+0.0; leftover $25.63 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 5 | $4.66 | $0.25 | — | $110.49 | ▼ $9,448.42 (-551.58) | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $25.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 5 | $5.08 | $0.27 | — | $84.82 | ▼ $9,448.15 (-551.85) | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $25.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 9 | $2.70 | $0.27 | — | $60.25 | ▼ $9,447.88 (-552.12) | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $25.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 23.36 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 23.36 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 23.36 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `QSI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AUPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ARCT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AUTL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRDL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SLQT` | no_price | no 09:30 open |
| 2026-08-26 | `DKS` | no_price | no 09:30 open |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BBWI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FINV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BBWI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FINV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `EIX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `NVAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | cash | leftover split 25.63 < 1 share @ 486.31 |
| 2026-09-04 | `MLYS` | cash | leftover split 25.63 < 1 share @ 29.15 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RVTY` | 9 | 2026-09-03 @ $125.94 | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1173.97 |
| `CRK` | 74 | 2026-09-03 @ $15.70 | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1173.97 |
| `MMED` | 51 | 2026-09-03 @ $22.78 | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1173.97 |
| `EIX` | 20 | 2026-09-03 @ $56.78 | combo gate; gate vol=good,ab=good; list probable,yday_gainer; ret5=+0.3; leftover $1173.97 |
| `CRDL` | 543 | 2026-09-03 @ $2.16 | combo gate; gate vol=good,ab=good; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1173.97 |
| `MRNA` | 7 | 2026-09-03 @ $151.40 | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1173.97 |
| `ARCT` | 71 | 2026-09-03 @ $16.46 | combo gate; gate vol=good,ab=good; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1173.97 |
| `NVAX` | 114 | 2026-09-03 @ $10.27 | combo gate; gate vol=good,ab=good; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1173.97 |
| `CABA` | 7 | 2026-09-04 @ $3.63 | combo gate; gate vol=good,ab=good; list flatten; 🔵; ⚪; ret5=+13.8; leftover $25.63 |
| `BAK` | 13 | 2026-09-04 @ $1.95 | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $25.63 |
| `SGLD` | 3 | 2026-09-04 @ $6.48 | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; ret5=+0.0; leftover $25.63 |
| `IRD` | 5 | 2026-09-04 @ $4.66 | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $25.63 |
| `OABI` | 5 | 2026-09-04 @ $5.08 | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $25.63 |
| `ALEC` | 9 | 2026-09-04 @ $2.70 | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $25.63 |
