# Factor mine action — `union_ab_g_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ ab_g, no 🚨

Cash book **+0.79%** ($10,079) · signal-only (no cash/fees) was +12.84%. Starts YES **13/17**. Fills 70 · skips 103 · realized $-152.82.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `ab=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $81.52.

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
| 2026-08-25 | +1.80 | $78.42 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, ALVO | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $7.51 | $10,452.61 | $10,460.12 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17, MOS×54, OCUL×118, INSP×21, CRMD×156, RZLT×247, HCA×3, BMEA×800, ALVO×248 | SELL AG (dropped from list after 3 sess (min 3)); SELL BHP (dropped from list after 3 sess (min 3)); SELL CDE (dropped from list after 3 sess (min 3)); SELL HDSN (dropped from list after 3 sess (min 3)); SELL IAG (dropped from list after 3 sess (min 3)); SELL KGC (dropped from list after 3 sess (min 3)); SELL NFGC (dropped from list after 3 sess (min 3)); SELL WPM (dropped from list after 3 sess (min 3)); BUY MOS x54 @ 24.00; BUY OCUL x118 @ 10.92; BUY INSP x21 @ 61.47; BUY CRMD x156 @ 8.28; BUY RZLT x247 @ 5.23; BUY HCA x3 @ 429.24; BUY BMEA x800 @ 1.62; BUY ALVO x248 @ 5.22 |
| 2026-08-26 | +2.02 | $7.51 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17, MOS×54, OCUL×118, INSP×21, CRMD×156, RZLT×247, HCA×3, BMEA×800, ALVO×248 | — | — | $7.51 | $10,455.17 | $10,462.68 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17, MOS×54, OCUL×118, INSP×21, CRMD×156, RZLT×247, HCA×3, BMEA×800, ALVO×248 | hold AUPH,ARCT,AUTL,CRDL,CYPH,MOS,OCUL,INSP,CRMD,RZLT,HCA,BMEA,ALVO |
| 2026-08-27 | — | $7.51 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17, MOS×54, OCUL×118, INSP×21, CRMD×156, RZLT×247, HCA×3, BMEA×800, ALVO×248 | CRK, SLI, GGB | AUPH, ARCT, AUTL, CRDL, CYPH | $78.82 | $10,342.18 | $10,421.00 | MOS×54, OCUL×118, INSP×21, CRMD×156, RZLT×247, HCA×3, BMEA×800, ALVO×248, CRK×1, SLI×6, GGB×4 | SELL AUPH (dropped from list after 4 sess (min 3)); SELL ARCT (dropped from list after 4 sess (min 3)); SELL AUTL (dropped from list after 4 sess (min 3)); SELL CRDL (dropped from list after 4 sess (min 3)); SELL CYPH (dropped from list after 4 sess (min 3)); BUY CRK x1 @ 14.09; BUY SLI x6 @ 2.59; BUY GGB x4 @ 4.42 |
| 2026-08-28 | +0.75 | $78.82 | MOS×54, OCUL×118, INSP×21, CRMD×156, RZLT×247, HCA×3, BMEA×800, ALVO×248, CRK×1, SLI×6, GGB×4 | RRC, ANF, BZ, SMTC, GRRR | OCUL, INSP, CRMD, RZLT, HCA, BMEA, ALVO | $146.53 | $10,096.56 | $10,243.09 | MOS×54, CRK×1, SLI×6, GGB×4, RRC×43, ANF×12, BZ×97, SMTC×12, GRRR×113 | SELL OCUL (dropped from list after 3 sess (min 3)); SELL INSP (dropped from list after 3 sess (min 3)); SELL CRMD (dropped from list after 3 sess (min 3)); SELL RZLT (dropped from list after 3 sess (min 3)); SELL HCA (dropped from list after 3 sess (min 3)); SELL BMEA (dropped from list after 3 sess (min 3)); SELL ALVO (dropped from list after 3 sess (min 3)); BUY RRC x43 @ 41.44; BUY ANF x12 @ 144.70; BUY BZ x97 @ 18.50; BUY SMTC x12 @ 149.40; BUY GRRR x113 @ 15.94 |
| 2026-08-31 | -5.85 | $146.53 | MOS×54, CRK×1, SLI×6, GGB×4, RRC×43, ANF×12, BZ×97, SMTC×12, GRRR×113 | — | MOS | $1,426.85 | $8,567.05 | $9,993.90 | CRK×1, SLI×6, GGB×4, RRC×43, ANF×12, BZ×97, SMTC×12, GRRR×113 | SELL MOS (dropped from list after 4 sess (min 3)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $1,426.85 | CRK×1, SLI×6, GGB×4, RRC×43, ANF×12, BZ×97, SMTC×12, GRRR×113 | — | CRK, SLI, GGB | $1,475.22 | $8,384.65 | $9,859.87 | RRC×43, ANF×12, BZ×97, SMTC×12, GRRR×113 | SELL CRK (dropped from list after 3 sess (min 3)); SELL SLI (dropped from list after 3 sess (min 3)); SELL GGB (dropped from list after 3 sess (min 3)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $1,475.22 | RRC×43, ANF×12, BZ×97, SMTC×12, GRRR×113 | — | RRC, ANF, BZ, SMTC, GRRR | $9,847.17 | $0.00 | $9,847.17 | — | SELL RRC (dropped from list after 3 sess (min 3)); SELL ANF (dropped from list after 3 sess (min 3)); SELL BZ (dropped from list after 3 sess (min 3)); SELL SMTC (dropped from list after 3 sess (min 3)); SELL GRRR (dropped from list after 3 sess (min 3)); hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $9,847.17 | — | ATRC, HRMY, CABA, VSTM, RVTY, CRK, MMED, SLN | — | $172.75 | $10,023.79 | $10,196.54 | ATRC×24, HRMY×29, CABA×376, VSTM×159, RVTY×9, CRK×78, MMED×54, SLN×83 | BUY ATRC x24 @ 49.76; BUY HRMY x29 @ 41.31; BUY CABA x376 @ 3.27; BUY VSTM x159 @ 7.70; BUY RVTY x9 @ 125.94; BUY CRK x78 @ 15.70; BUY MMED x54 @ 22.78; BUY SLN x83 @ 14.70 |
| 2026-09-04 | — | $172.75 | ATRC×24, HRMY×29, CABA×376, VSTM×159, RVTY×9, CRK×78, MMED×54, SLN×83 | NVAX, BVS, BAK, SLBT | — | $81.52 | $9,997.53 | $10,079.05 | ATRC×24, HRMY×29, CABA×376, VSTM×159, RVTY×9, CRK×78, MMED×54, SLN×83, NVAX×2, BVS×1, BAK×14, SLBT×9 | BUY NVAX x2 @ 10.41; BUY BVS x1 @ 14.50; BUY BAK x14 @ 1.95; BUY SLBT x9 @ 3.07 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-20 09:30 ET | **BUY** | `AG` | 60 | $20.55 | $2.17 | — | $8,764.83 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,579.67 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 60 | $20.65 | $2.17 | — | $6,338.50 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 216 | $5.77 | $2.79 | — | $5,089.39 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $3,850.53 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,603.95 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 714 | $1.75 | $9.21 | — | $1,345.24 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $186.91 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $169.53 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $147.04 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 9 | $2.47 | $0.25 | — | $124.56 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 12 | $1.93 | $0.27 | — | $101.13 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 17 | $1.32 | $0.28 | — | $78.42 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 60 | $20.73 | $2.19 | $+6.44 | $1,320.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.95 | $2.05 | $+60.14 | $2,565.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 60 | $20.85 | $2.19 | $+7.64 | $3,814.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 216 | $5.53 | $2.83 | $-57.46 | $5,005.79 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 63 | $21.63 | $2.20 | $+121.62 | $6,366.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 42 | $32.76 | $2.14 | $+127.21 | $7,740.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 714 | $1.91 | $9.34 | $+95.69 | $9,094.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $160.00 | $2.03 | $+119.63 | $10,372.43 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 54 | $24.00 | $2.15 | — | $9,074.27 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ⚪; ret5=+13.0; leftover $1296.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 118 | $10.92 | $2.34 | — | $7,783.37 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+10.4; leftover $1296.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 21 | $61.47 | $2.05 | — | $6,490.45 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+9.2; leftover $1296.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 156 | $8.28 | $2.46 | — | $5,196.31 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1296.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 247 | $5.23 | $3.19 | — | $3,901.31 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+10.7; leftover $1296.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $2,611.59 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+6.1; leftover $1296.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 800 | $1.62 | $10.32 | — | $1,305.27 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1296.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 248 | $5.22 | $3.20 | — | $7.51 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1296.55 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $23.93 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $54.29 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 9 | $2.41 | $0.26 | $-1.05 | $75.72 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 12 | $2.03 | $0.30 | $+0.63 | $99.78 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 17 | $1.60 | $0.34 | $+4.14 | $126.64 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 1 | $14.09 | $0.14 | — | $112.40 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+1.1; leftover $18.09 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 6 | $2.59 | $0.17 | — | $96.69 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+4.2; leftover $18.09 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 4 | $4.42 | $0.19 | — | $78.82 | union ∩ ab_g, no 🚨; gate ab=good; list mover_buy; 🔵; ret5=-8.6; leftover $18.09 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 118 | $10.63 | $2.37 | $-38.94 | $1,330.79 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 21 | $62.10 | $2.07 | $+9.10 | $2,632.81 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 156 | $8.49 | $2.49 | $+27.81 | $3,954.76 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 247 | $5.07 | $3.24 | $-45.94 | $5,203.81 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-17.91 | $6,475.62 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 800 | $1.74 | $10.46 | $+75.22 | $7,857.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 248 | $4.88 | $3.25 | $-90.77 | $9,064.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 43 | $41.44 | $2.12 | — | $7,280.11 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+1.8; leftover $1812.83 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 12 | $144.70 | $2.03 | — | $5,541.68 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1812.83 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 97 | $18.50 | $2.28 | — | $3,744.90 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1812.83 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 12 | $149.40 | $2.03 | — | $1,950.08 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1812.83 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 113 | $15.94 | $2.33 | — | $146.53 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1812.83 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 54 | $23.75 | $2.17 | $-17.82 | $1,426.85 | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 1 | $14.31 | $0.17 | $-0.09 | $1,441.00 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 6 | $2.70 | $0.20 | $+0.29 | $1,457.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `GGB` | 4 | $4.61 | $0.22 | $+0.35 | $1,475.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 43 | $41.94 | $2.14 | $+17.24 | $3,276.50 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 12 | $142.00 | $2.05 | $-36.48 | $4,978.45 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 97 | $17.29 | $2.31 | $-121.96 | $6,653.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 12 | $127.63 | $2.05 | $-265.31 | $8,182.78 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 113 | $14.75 | $2.36 | $-139.16 | $9,847.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 24 | $49.76 | $2.06 | — | $8,650.87 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1230.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $41.31 | $2.08 | — | $7,450.80 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1230.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 376 | $3.27 | $4.85 | — | $6,216.43 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1230.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 159 | $7.70 | $2.47 | — | $4,989.66 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1230.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $3,854.19 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1230.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 78 | $15.70 | $2.22 | — | $2,627.36 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1230.90 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 54 | $22.78 | $2.15 | — | $1,395.09 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1230.90 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 83 | $14.70 | $2.24 | — | $172.75 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1230.90 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 2 | $10.41 | $0.21 | — | $151.72 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $28.79 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 1 | $14.50 | $0.15 | — | $137.07 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+0.8; leftover $28.79 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 14 | $1.95 | $0.32 | — | $109.45 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $28.79 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 9 | $3.07 | $0.30 | — | $81.52 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $28.79 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |

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
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `QSI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AUPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ARCT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AUTL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRDL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 18.09 < 1 share @ 40.72 |
| 2026-08-27 | `ACMR` | cash | leftover split 18.09 < 1 share @ 80.97 |
| 2026-08-27 | `MT` | cash | leftover split 18.09 < 1 share @ 75.12 |
| 2026-08-27 | `MU` | cash | leftover split 18.09 < 1 share @ 925.74 |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `GGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OKE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ASND` | cash | leftover split 28.79 < 1 share @ 266.94 |
| 2026-09-04 | `OSCR` | cash | leftover split 28.79 < 1 share @ 30.65 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 24 | 2026-09-03 @ $49.76 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1230.90 |
| `HRMY` | 29 | 2026-09-03 @ $41.31 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1230.90 |
| `CABA` | 376 | 2026-09-03 @ $3.27 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1230.90 |
| `VSTM` | 159 | 2026-09-03 @ $7.70 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1230.90 |
| `RVTY` | 9 | 2026-09-03 @ $125.94 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1230.90 |
| `CRK` | 78 | 2026-09-03 @ $15.70 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1230.90 |
| `MMED` | 54 | 2026-09-03 @ $22.78 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1230.90 |
| `SLN` | 83 | 2026-09-03 @ $14.70 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1230.90 |
| `NVAX` | 2 | 2026-09-04 @ $10.41 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $28.79 |
| `BVS` | 1 | 2026-09-04 @ $14.50 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+0.8; leftover $28.79 |
| `BAK` | 14 | 2026-09-04 @ $1.95 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $28.79 |
| `SLBT` | 9 | 2026-09-04 @ $3.07 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $28.79 |
