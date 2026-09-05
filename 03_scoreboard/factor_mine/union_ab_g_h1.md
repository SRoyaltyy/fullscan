# Factor mine action — `union_ab_g_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ ab_g, no 🚨

Cash book **+10.69%** ($11,069) · signal-only (no cash/fees) was +5.13%. Starts YES **16/17**. Fills 90 · skips 36 · realized $+847.04.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `ab=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $46.63.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-14 | +5.50 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-17 | +2.25 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-18 | -6.20 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | hard-red sit S=-6.20 |
| 2026-08-19 | -7.20 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | hard-red sit S=-7.20 |
| 2026-08-20 | +1.12 | $10,000.00 | — | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $186.91 | $10,021.37 | $10,208.28 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8 | BUY AG x60 @ 20.55; BUY BHP x13 @ 91.01; BUY CDE x60 @ 20.65; BUY HDSN x216 @ 5.77; BUY IAG x63 @ 19.63; BUY KGC x42 @ 29.63; BUY NFGC x714 @ 1.75; BUY WPM x8 @ 144.54 |
| 2026-08-21 | +3.25 | $186.91 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $158.85 | $10,514.68 | $10,673.53 | AU×10, AUPH×75, AEM×6, ARCT×117, AUTL×528, CRDL×676, CRSP×21, CYPH×989 | SELL AG (dropped from list after 1 sess (min 1)); SELL BHP (dropped from list after 1 sess (min 1)); SELL CDE (dropped from list after 1 sess (min 1)); SELL HDSN (dropped from list after 1 sess (min 1)); SELL IAG (dropped from list after 1 sess (min 1)); SELL KGC (dropped from list after 1 sess (min 1)); SELL NFGC (dropped from list after 1 sess (min 1)); SELL WPM (dropped from list after 1 sess (min 1)); BUY AU x10 @ 119.43; BUY AUPH x75 @ 17.20; BUY AEM x6 @ 216.30; BUY ARCT x117 @ 11.13; BUY AUTL x528 @ 2.47; BUY CRDL x676 @ 1.93; BUY CRSP x21 @ 59.72; BUY CYPH x989 @ 1.32 |
| 2026-08-24 | -5.17 | $158.85 | AU×10, AUPH×75, AEM×6, ARCT×117, AUTL×528, CRDL×676, CRSP×21, CYPH×989 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,977.67 | $0.00 | $10,977.67 | — | SELL AU (dropped from list after 1 sess (min 1)); SELL AUPH (dropped from list after 1 sess (min 1)); SELL AEM (dropped from list after 1 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL AUTL (dropped from list after 1 sess (min 1)); SELL CRDL (dropped from list after 1 sess (min 1)); SELL CRSP (dropped from list after 1 sess (min 1)); SELL CYPH (dropped from list after 1 sess (min 1)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $10,977.67 | — | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, ALVO | — | $99.61 | $10,847.94 | $10,947.55 | MOS×57, OCUL×125, INSP×22, CRMD×165, RZLT×262, HCA×3, BMEA×847, ALVO×262 | BUY MOS x57 @ 24.00; BUY OCUL x125 @ 10.92; BUY INSP x22 @ 61.47; BUY CRMD x165 @ 8.28; BUY RZLT x262 @ 5.23; BUY HCA x3 @ 429.24; BUY BMEA x847 @ 1.62; BUY ALVO x262 @ 5.22 |
| 2026-08-26 | +2.02 | $99.61 | MOS×57, OCUL×125, INSP×22, CRMD×165, RZLT×262, HCA×3, BMEA×847, ALVO×262 | — | — | $99.61 | $10,849.30 | $10,948.91 | MOS×57, OCUL×125, INSP×22, CRMD×165, RZLT×262, HCA×3, BMEA×847, ALVO×262 | hold MOS,OCUL,INSP,CRMD,RZLT,HCA,BMEA,ALVO |
| 2026-08-27 | — | $99.61 | MOS×57, OCUL×125, INSP×22, CRMD×165, RZLT×262, HCA×3, BMEA×847, ALVO×262 | RRC, CRK, SLI, ACMR, GGB, MT, MU | OCUL, INSP, CRMD, RZLT, HCA, BMEA, ALVO | $529.35 | $10,432.51 | $10,961.86 | MOS×57, RRC×33, CRK×96, SLI×526, ACMR×16, GGB×308, MT×18, MU×1 | SELL OCUL (dropped from list after 2 sess (min 1)); SELL INSP (dropped from list after 2 sess (min 1)); SELL CRMD (dropped from list after 2 sess (min 1)); SELL RZLT (dropped from list after 2 sess (min 1)); SELL HCA (dropped from list after 2 sess (min 1)); SELL BMEA (dropped from list after 2 sess (min 1)); SELL ALVO (dropped from list after 2 sess (min 1)); BUY RRC x33 @ 40.72; BUY CRK x96 @ 14.09; BUY SLI x526 @ 2.59; BUY ACMR x16 @ 80.97; BUY GGB x308 @ 4.42; BUY MT x18 @ 75.12; BUY MU x1 @ 925.74 |
| 2026-08-28 | +0.75 | $529.35 | MOS×57, RRC×33, CRK×96, SLI×526, ACMR×16, GGB×308, MT×18, MU×1 | ANF, BZ, SMTC, GRRR | ACMR, GGB, MT, MU | $146.64 | $10,792.98 | $10,939.62 | MOS×57, RRC×33, CRK×96, SLI×526, ANF×9, BZ×74, SMTC×9, GRRR×86 | SELL ACMR (dropped from list after 1 sess (min 1)); SELL GGB (dropped from list after 1 sess (min 1)); SELL MT (dropped from list after 1 sess (min 1)); SELL MU (dropped from list after 1 sess (min 1)); BUY ANF x9 @ 144.70; BUY BZ x74 @ 18.50; BUY SMTC x9 @ 149.40; BUY GRRR x86 @ 15.94 |
| 2026-08-31 | -5.85 | $146.64 | MOS×57, RRC×33, CRK×96, SLI×526, ANF×9, BZ×74, SMTC×9, GRRR×86 | — | MOS, RRC, CRK, SLI, ANF, BZ, SMTC, GRRR | $10,643.74 | $0.00 | $10,643.74 | — | SELL MOS (dropped from list after 4 sess (min 1)); SELL RRC (dropped from list after 2 sess (min 1)); SELL CRK (dropped from list after 2 sess (min 1)); SELL SLI (dropped from list after 2 sess (min 1)); SELL ANF (dropped from list after 1 sess (min 1)); SELL BZ (dropped from list after 1 sess (min 1)); SELL SMTC (dropped from list after 1 sess (min 1)); SELL GRRR (dropped from list after 1 sess (min 1)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $10,643.74 | — | — | — | $10,643.74 | $0.00 | $10,643.74 | — | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $10,643.74 | — | — | — | $10,643.74 | $0.00 | $10,643.74 | — | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $10,643.74 | — | ATRC, HRMY, CABA, VSTM, RVTY, CRK, MMED, SLN | — | $133.02 | $10,891.66 | $11,024.68 | ATRC×26, HRMY×32, CABA×406, VSTM×172, RVTY×10, CRK×84, MMED×58, SLN×90 | BUY ATRC x26 @ 49.76; BUY HRMY x32 @ 41.31; BUY CABA x406 @ 3.27; BUY VSTM x172 @ 7.70; BUY RVTY x10 @ 125.94; BUY CRK x84 @ 15.70; BUY MMED x58 @ 22.78; BUY SLN x90 @ 14.70 |
| 2026-09-04 | — | $133.02 | ATRC×26, HRMY×32, CABA×406, VSTM×172, RVTY×10, CRK×84, MMED×58, SLN×90 | ASND, OSCR, NVAX, BVS, BAK, SLBT | HRMY, VSTM, RVTY, CRK, MMED, SLN | $46.63 | $11,022.16 | $11,068.79 | ATRC×26, CABA×406, ASND×5, OSCR×44, NVAX×131, BVS×94, BAK×702, SLBT×446 | SELL HRMY (dropped from list after 1 sess (min 1)); SELL VSTM (dropped from list after 1 sess (min 1)); SELL RVTY (dropped from list after 1 sess (min 1)); SELL CRK (dropped from list after 1 sess (min 1)); SELL MMED (dropped from list after 1 sess (min 1)); SELL SLN (dropped from list after 1 sess (min 1)); BUY ASND x5 @ 266.94; BUY OSCR x44 @ 30.65; BUY NVAX x131 @ 10.41; BUY BVS x94 @ 14.50; BUY BAK x702 @ 1.95; BUY SLBT x446 @ 3.07 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-20 09:30 ET | **BUY** | `AG` | 60 | $20.55 | $2.17 | — | $8,764.83 | ▼ $9,997.83 (-2.17) | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,579.67 | ▼ $9,995.80 (-4.20) | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 60 | $20.65 | $2.17 | — | $6,338.50 | ▼ $9,993.63 (-6.37) | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 216 | $5.77 | $2.79 | — | $5,089.39 | ▼ $9,990.84 (-9.16) | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $3,850.53 | ▼ $9,988.67 (-11.33) | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,603.95 | ▼ $9,986.55 (-13.45) | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 714 | $1.75 | $9.21 | — | $1,345.24 | ▼ $9,977.34 (-22.66) | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $186.91 | ▼ $9,975.32 (-24.68) | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 60 | $21.90 | $2.19 | $+76.64 | $1,498.71 | ▲ $10,473.30 (+473.30) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,741.03 | ▲ $10,471.26 (+471.26) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 60 | $21.75 | $2.19 | $+61.64 | $4,043.84 | ▲ $10,469.07 (+469.07) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 216 | $5.67 | $2.83 | $-27.22 | $5,265.72 | ▲ $10,466.23 (+466.23) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 63 | $21.17 | $2.20 | $+92.64 | $6,597.23 | ▲ $10,464.03 (+464.03) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $7,946.24 | ▲ $10,461.90 (+461.90) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 714 | $1.79 | $9.34 | $+10.01 | $9,214.96 | ▲ $10,452.56 (+452.56) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,450.52 | ▲ $10,450.52 (+450.52) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $9,254.20 | ▲ $10,448.50 (+448.50) | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 75 | $17.20 | $2.21 | — | $7,961.99 | ▲ $10,446.29 (+446.29) | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,662.18 | ▲ $10,444.28 (+444.28) | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 117 | $11.13 | $2.34 | — | $5,357.63 | ▲ $10,441.94 (+441.94) | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 528 | $2.47 | $6.81 | — | $4,046.66 | ▲ $10,435.13 (+435.13) | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 676 | $1.93 | $8.72 | — | $2,733.26 | ▲ $10,426.41 (+426.41) | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,477.08 | ▲ $10,424.35 (+424.35) | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 989 | $1.32 | $12.76 | — | $158.85 | ▲ $10,411.60 (+411.60) | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.50 | $2.04 | $+6.64 | $1,361.81 | ▲ $11,015.07 (+1,015.07) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 75 | $16.60 | $2.24 | $-49.45 | $2,604.57 | ▲ $11,012.83 (+1,012.83) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 6 | $217.03 | $2.03 | $+0.34 | $3,904.72 | ▲ $11,010.80 (+1,010.80) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 117 | $13.26 | $2.37 | $+244.50 | $5,453.77 | ▲ $11,008.43 (+1,008.43) | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 528 | $2.36 | $6.91 | $-71.80 | $6,692.94 | ▲ $11,001.52 (+1,001.52) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 676 | $1.87 | $8.84 | $-58.12 | $7,948.22 | ▲ $10,992.68 (+992.68) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.79 | $2.07 | $-23.66 | $9,180.73 | ▲ $10,990.60 (+990.60) | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 989 | $1.83 | $12.94 | $+478.70 | $10,977.67 | ▲ $10,977.67 (+977.67) | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 57 | $24.00 | $2.16 | — | $9,607.50 | ▲ $10,975.50 (+975.50) | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ⚪; ret5=+13.0; leftover $1372.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 125 | $10.92 | $2.37 | — | $8,240.14 | ▲ $10,973.14 (+973.14) | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+10.4; leftover $1372.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 22 | $61.47 | $2.06 | — | $6,885.74 | ▲ $10,971.08 (+971.08) | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+9.2; leftover $1372.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 165 | $8.28 | $2.48 | — | $5,517.06 | ▲ $10,968.60 (+968.60) | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1372.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 262 | $5.23 | $3.38 | — | $4,143.42 | ▲ $10,965.22 (+965.22) | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+10.7; leftover $1372.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $2,853.70 | ▲ $10,963.22 (+963.22) | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+6.1; leftover $1372.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 847 | $1.62 | $10.93 | — | $1,470.63 | ▲ $10,952.29 (+952.29) | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1372.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 262 | $5.22 | $3.38 | — | $99.61 | ▲ $10,948.91 (+948.91) | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1372.21 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 125 | $10.79 | $2.40 | $-21.01 | $1,445.97 | ▲ $10,984.52 (+984.52) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 22 | $60.07 | $2.08 | $-34.93 | $2,765.43 | ▲ $10,982.44 (+982.44) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 165 | $8.60 | $2.52 | $+47.79 | $4,181.91 | ▲ $10,979.92 (+979.92) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 262 | $5.01 | $3.43 | $-64.45 | $5,491.09 | ▲ $10,976.48 (+976.48) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $427.50 | $2.02 | $-9.24 | $6,771.57 | ▲ $10,974.46 (+974.46) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 847 | $1.75 | $11.08 | $+88.10 | $8,242.75 | ▲ $10,963.39 (+963.39) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALVO` | 262 | $4.98 | $3.43 | $-69.69 | $9,544.07 | ▲ $10,959.95 (+959.95) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 33 | $40.72 | $2.09 | — | $8,198.22 | ▲ $10,957.86 (+957.86) | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+1.8; leftover $1363.44 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 96 | $14.09 | $2.28 | — | $6,843.30 | ▲ $10,955.58 (+955.58) | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+1.1; leftover $1363.44 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 526 | $2.59 | $6.79 | — | $5,474.18 | ▲ $10,948.80 (+948.80) | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+4.2; leftover $1363.44 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 16 | $80.97 | $2.04 | — | $4,176.62 | ▲ $10,946.76 (+946.76) | union ∩ ab_g, no 🚨; gate ab=good; list mover_buy; 🔵; ret5=-1.3; leftover $1363.44 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 308 | $4.42 | $3.97 | — | $2,811.29 | ▲ $10,942.79 (+942.79) | union ∩ ab_g, no 🚨; gate ab=good; list mover_buy; 🔵; ret5=-8.6; leftover $1363.44 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 18 | $75.12 | $2.04 | — | $1,457.08 | ▲ $10,940.74 (+940.74) | union ∩ ab_g, no 🚨; gate ab=good; list mover_buy; 🔵; ret5=-2.2; leftover $1363.44 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $925.74 | $1.99 | — | $529.35 | ▲ $10,938.75 (+938.75) | union ∩ ab_g, no 🚨; gate ab=good; list mover_buy; 🔵; ret5=-0.5; leftover $1363.44 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 16 | $81.65 | $2.06 | $+6.78 | $1,833.69 | ▲ $11,037.42 (+1,037.42) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 308 | $4.57 | $4.04 | $+38.19 | $3,237.22 | ▲ $11,033.39 (+1,033.39) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 18 | $74.54 | $2.06 | $-14.55 | $4,576.87 | ▲ $11,031.32 (+1,031.32) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $967.01 | $2.01 | $+37.26 | $5,541.87 | ▲ $11,029.31 (+1,029.31) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $144.70 | $2.02 | — | $4,237.55 | ▲ $11,027.29 (+1,027.29) | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1385.47 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 74 | $18.50 | $2.21 | — | $2,866.34 | ▲ $11,025.08 (+1,025.08) | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1385.47 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $149.40 | $2.02 | — | $1,519.72 | ▲ $11,023.06 (+1,023.06) | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1385.47 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 86 | $15.94 | $2.25 | — | $146.64 | ▲ $11,020.82 (+1,020.82) | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1385.47 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 57 | $23.75 | $2.18 | $-18.59 | $1,498.20 | ▲ $10,663.62 (+663.62) | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 33 | $41.11 | $2.11 | $+8.67 | $2,852.72 | ▲ $10,661.51 (+661.51) | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 96 | $14.56 | $2.31 | $+40.54 | $4,248.18 | ▲ $10,659.21 (+659.21) | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 526 | $2.51 | $6.88 | $-55.75 | $5,561.56 | ▲ $10,652.33 (+652.33) | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.67 | $2.04 | $+31.68 | $6,897.55 | ▲ $10,650.29 (+650.29) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 74 | $17.89 | $2.23 | $-49.59 | $8,219.17 | ▲ $10,648.05 (+648.05) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $133.04 | $2.04 | $-151.29 | $9,414.50 | ▲ $10,646.02 (+646.02) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 86 | $14.32 | $2.27 | $-143.84 | $10,643.74 | ▲ $10,643.74 (+643.74) | dropped from list after 1 sess (min 1) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 26 | $49.76 | $2.07 | — | $9,347.92 | ▲ $10,641.68 (+641.68) | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1330.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 32 | $41.31 | $2.09 | — | $8,023.91 | ▲ $10,639.59 (+639.59) | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1330.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 406 | $3.27 | $5.24 | — | $6,691.05 | ▲ $10,634.35 (+634.35) | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1330.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 172 | $7.70 | $2.51 | — | $5,364.15 | ▲ $10,631.85 (+631.85) | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1330.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $4,102.73 | ▲ $10,629.83 (+629.83) | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1330.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 84 | $15.70 | $2.24 | — | $2,781.68 | ▲ $10,627.58 (+627.58) | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1330.47 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 58 | $22.78 | $2.16 | — | $1,458.28 | ▲ $10,625.42 (+625.42) | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1330.47 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 90 | $14.70 | $2.26 | — | $133.02 | ▲ $10,623.16 (+623.16) | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1330.47 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 32 | $42.93 | $2.11 | $+47.65 | $1,504.67 | ▲ $11,078.33 (+1,078.33) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 172 | $8.03 | $2.55 | $+51.71 | $2,883.29 | ▲ $11,075.79 (+1,075.79) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $132.45 | $2.04 | $+61.04 | $4,205.75 | ▲ $11,073.75 (+1,073.75) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 84 | $15.45 | $2.27 | $-25.51 | $5,501.28 | ▲ $11,071.48 (+1,071.48) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 58 | $23.88 | $2.19 | $+59.45 | $6,884.14 | ▲ $11,069.30 (+1,069.30) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SLN` | 90 | $14.85 | $2.29 | $+8.95 | $8,218.35 | ▲ $11,067.01 (+1,067.01) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 5 | $266.94 | $2.00 | — | $6,881.64 | ▲ $11,065.00 (+1,065.00) | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+1.9; leftover $1369.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 44 | $30.65 | $2.12 | — | $5,530.92 | ▲ $11,062.88 (+1,062.88) | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=-2.2; leftover $1369.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 131 | $10.41 | $2.38 | — | $4,164.83 | ▲ $11,060.50 (+1,060.50) | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1369.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 94 | $14.50 | $2.27 | — | $2,799.56 | ▲ $11,058.23 (+1,058.23) | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1369.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 702 | $1.95 | $9.06 | — | $1,421.60 | ▲ $11,049.17 (+1,049.17) | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1369.72 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 446 | $3.07 | $5.75 | — | $46.63 | ▲ $11,043.42 (+1,043.42) | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1369.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `QSI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `BMEA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALVO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
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

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 26 | 2026-09-03 @ $49.76 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1330.47 |
| `CABA` | 406 | 2026-09-03 @ $3.27 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1330.47 |
| `ASND` | 5 | 2026-09-04 @ $266.94 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+1.9; leftover $1369.72 |
| `OSCR` | 44 | 2026-09-04 @ $30.65 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=-2.2; leftover $1369.72 |
| `NVAX` | 131 | 2026-09-04 @ $10.41 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1369.72 |
| `BVS` | 94 | 2026-09-04 @ $14.50 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1369.72 |
| `BAK` | 702 | 2026-09-04 @ $1.95 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1369.72 |
| `SLBT` | 446 | 2026-09-04 @ $3.07 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1369.72 |
