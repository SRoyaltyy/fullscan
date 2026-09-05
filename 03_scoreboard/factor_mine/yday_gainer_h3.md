# Factor mine action — `yday_gainer_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `yday_gainer` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-5.11%** ($9,490) · signal-only (no cash/fees) was +13.89%. Starts YES **5/17**. Fills 86 · skips 152 · realized $-543.07.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `yday_gainer` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $25.53.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-14 | +5.50 | $10,000.00 | — | ANGX, WWW, HYLN, ARX, OMER, AIRO, NCMI, MXCT | — | $4.90 | $9,799.82 | $9,804.72 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 | BUY ANGX x290 @ 4.31; BUY WWW x60 @ 20.60; BUY HYLN x299 @ 4.18; BUY ARX x63 @ 19.57; BUY OMER x72 @ 17.35; BUY AIRO x112 @ 11.12; BUY NCMI x464 @ 2.69; BUY MXCT x899 @ 1.39 |
| 2026-08-17 | +2.25 | $4.90 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 | — | — | $4.90 | $9,766.87 | $9,771.77 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 | hold ANGX,WWW,HYLN,ARX,OMER,AIRO,NCMI,MXCT |
| 2026-08-18 | -6.20 | $4.90 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 | — | — | $4.90 | $9,546.77 | $9,551.67 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $4.90 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 | — | ANGX, WWW, HYLN, ARX, OMER, AIRO, NCMI, MXCT | $9,555.06 | $0.00 | $9,555.06 | — | SELL ANGX (dropped from list after 3 sess (min 3)); SELL WWW (dropped from list after 3 sess (min 3)); SELL HYLN (dropped from list after 3 sess (min 3)); SELL ARX (dropped from list after 3 sess (min 3)); SELL OMER (dropped from list after 3 sess (min 3)); SELL AIRO (dropped from list after 3 sess (min 3)); SELL NCMI (dropped from list after 3 sess (min 3)); SELL MXCT (dropped from list after 3 sess (min 3)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $9,555.06 | — | CDE, MRVI, DNA, MSTR, EXK, SCZM, NG, BLSH | — | $112.57 | $9,574.41 | $9,686.98 | CDE×57, MRVI×161, DNA×160, MSTR×10, EXK×110, SCZM×126, NG×142, BLSH×40 | BUY CDE x57 @ 20.65; BUY MRVI x161 @ 7.38; BUY DNA x160 @ 7.45; BUY MSTR x10 @ 113.23; BUY EXK x110 @ 10.77; BUY SCZM x126 @ 9.46; BUY NG x142 @ 8.38; BUY BLSH x40 @ 29.20 |
| 2026-08-21 | +3.25 | $112.57 | CDE×57, MRVI×161, DNA×160, MSTR×10, EXK×110, SCZM×126, NG×142, BLSH×40 | ARCT, CYPH, BTBT, ENHA, QDEL, ORBS | — | $23.73 | $9,904.70 | $9,928.43 | CDE×57, MRVI×161, DNA×160, MSTR×10, EXK×110, SCZM×126, NG×142, BLSH×40, ARCT×1, CYPH×12, BTBT×9, ENHA×9, QDEL×1, ORBS×18 | BUY ARCT x1 @ 11.13; BUY CYPH x12 @ 1.32; BUY BTBT x9 @ 1.66; BUY ENHA x9 @ 1.71; BUY QDEL x1 @ 14.96; BUY ORBS x18 @ 0.86 |
| 2026-08-24 | -5.17 | $23.73 | CDE×57, MRVI×161, DNA×160, MSTR×10, EXK×110, SCZM×126, NG×142, BLSH×40, ARCT×1, CYPH×12, BTBT×9, ENHA×9, QDEL×1, ORBS×18 | — | — | $23.73 | $9,882.30 | $9,906.03 | CDE×57, MRVI×161, DNA×160, MSTR×10, EXK×110, SCZM×126, NG×142, BLSH×40, ARCT×1, CYPH×12, BTBT×9, ENHA×9, QDEL×1, ORBS×18 | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $23.73 | CDE×57, MRVI×161, DNA×160, MSTR×10, EXK×110, SCZM×126, NG×142, BLSH×40, ARCT×1, CYPH×12, BTBT×9, ENHA×9, QDEL×1, ORBS×18 | BMEA, NPWR, PUSA, ALVO, CAPR, ALIT, ZURA, SAFX | CDE, MRVI, DNA, MSTR, EXK, SCZM, NG, BLSH | $0.79 | $10,044.34 | $10,045.13 | ARCT×1, CYPH×12, BTBT×9, ENHA×9, QDEL×1, ORBS×18, BMEA×758, NPWR×614, PUSA×332, ALVO×235, CAPR×180, ALIT×82, ZURA×192, SAFX×3238 | SELL CDE (dropped from list after 3 sess (min 3)); SELL MRVI (dropped from list after 3 sess (min 3)); SELL DNA (dropped from list after 3 sess (min 3)); SELL MSTR (dropped from list after 3 sess (min 3)); SELL EXK (dropped from list after 3 sess (min 3)); SELL SCZM (dropped from list after 3 sess (min 3)); SELL NG (dropped from list after 3 sess (min 3)); SELL BLSH (dropped from list after 3 sess (min 3)); BUY BMEA x758 @ 1.62; BUY NPWR x614 @ 2.00; BUY PUSA x332 @ 3.70; BUY ALVO x235 @ 5.22; BUY CAPR x180 @ 6.79; BUY ALIT x82 @ 14.86; BUY ZURA x192 @ 6.38; BUY SAFX x3238 @ 0.37 |
| 2026-08-26 | +2.02 | $0.79 | ARCT×1, CYPH×12, BTBT×9, ENHA×9, QDEL×1, ORBS×18, BMEA×758, NPWR×614, PUSA×332, ALVO×235, CAPR×180, ALIT×82, ZURA×192, SAFX×3238 | — | — | $0.79 | $9,868.13 | $9,868.92 | ARCT×1, CYPH×12, BTBT×9, ENHA×9, QDEL×1, ORBS×18, BMEA×758, NPWR×614, PUSA×332, ALVO×235, CAPR×180, ALIT×82, ZURA×192, SAFX×3238 | hold ARCT,CYPH,BTBT,ENHA,QDEL,ORBS,BMEA,NPWR,PUSA,ALVO,CAPR,ALIT,ZURA,SAFX |
| 2026-08-27 | — | $0.79 | ARCT×1, CYPH×12, BTBT×9, ENHA×9, QDEL×1, ORBS×18, BMEA×758, NPWR×614, PUSA×332, ALVO×235, CAPR×180, ALIT×82, ZURA×192, SAFX×3238 | — | ARCT, CYPH, BTBT, ENHA, QDEL, ORBS | $92.07 | $10,112.33 | $10,204.40 | BMEA×758, NPWR×614, PUSA×332, ALVO×235, CAPR×180, ALIT×82, ZURA×192, SAFX×3238 | SELL ARCT (dropped from list after 4 sess (min 3)); SELL CYPH (dropped from list after 4 sess (min 3)); SELL BTBT (dropped from list after 4 sess (min 3)); SELL ENHA (dropped from list after 4 sess (min 3)); SELL QDEL (dropped from list after 4 sess (min 3)); SELL ORBS (dropped from list after 4 sess (min 3)) |
| 2026-08-28 | +0.75 | $92.07 | BMEA×758, NPWR×614, PUSA×332, ALVO×235, CAPR×180, ALIT×82, ZURA×192, SAFX×3238 | ANF, BHVN, BZ, LVWR, SEDG, SMTC, GRRR | BMEA, NPWR, PUSA, ALVO, ALIT, ZURA, SAFX | $93.20 | $10,026.80 | $10,120.00 | CAPR×180, ANF×8, BHVN×71, BZ×65, LVWR×882, SEDG×36, SMTC×8, GRRR×76 | SELL BMEA (dropped from list after 3 sess (min 3)); SELL NPWR (dropped from list after 3 sess (min 3)); SELL PUSA (dropped from list after 3 sess (min 3)); SELL ALVO (dropped from list after 3 sess (min 3)); SELL ALIT (dropped from list after 3 sess (min 3)); SELL ZURA (dropped from list after 3 sess (min 3)); SELL SAFX (dropped from list after 3 sess (min 3)); BUY ANF x8 @ 144.70; BUY BHVN x71 @ 16.95; BUY BZ x65 @ 18.50; BUY LVWR x882 @ 1.38; BUY SEDG x36 @ 33.78; BUY SMTC x8 @ 149.40; BUY GRRR x76 @ 15.94 |
| 2026-08-31 | -5.85 | $93.20 | CAPR×180, ANF×8, BHVN×71, BZ×65, LVWR×882, SEDG×36, SMTC×8, GRRR×76 | — | CAPR | $1,789.83 | $7,898.26 | $9,688.09 | ANF×8, BHVN×71, BZ×65, LVWR×882, SEDG×36, SMTC×8, GRRR×76 | SELL CAPR (dropped from list after 4 sess (min 3)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $1,789.83 | ANF×8, BHVN×71, BZ×65, LVWR×882, SEDG×36, SMTC×8, GRRR×76 | — | — | $1,789.83 | $7,703.36 | $9,493.19 | ANF×8, BHVN×71, BZ×65, LVWR×882, SEDG×36, SMTC×8, GRRR×76 | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $1,789.83 | ANF×8, BHVN×71, BZ×65, LVWR×882, SEDG×36, SMTC×8, GRRR×76 | — | ANF, BHVN, BZ, LVWR, SEDG, SMTC, GRRR | $9,456.91 | $0.00 | $9,456.91 | — | SELL ANF (dropped from list after 3 sess (min 3)); SELL BHVN (dropped from list after 3 sess (min 3)); SELL BZ (dropped from list after 3 sess (min 3)); SELL LVWR (dropped from list after 3 sess (min 3)); SELL SEDG (dropped from list after 3 sess (min 3)); SELL SMTC (dropped from list after 3 sess (min 3)); SELL GRRR (dropped from list after 3 sess (min 3)); hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $9,456.91 | — | GPRO, FRVO, CRK, MMED, CTMX, SLN, EIX, CRDL | — | $52.26 | $9,817.13 | $9,869.39 | GPRO×968, FRVO×64, CRK×75, MMED×51, CTMX×317, SLN×80, EIX×20, CRDL×547 | BUY GPRO x968 @ 1.22; BUY FRVO x64 @ 18.40; BUY CRK x75 @ 15.70; BUY MMED x51 @ 22.78; BUY CTMX x317 @ 3.72; BUY SLN x80 @ 14.70; BUY EIX x20 @ 56.78; BUY CRDL x547 @ 2.16 |
| 2026-09-04 | — | $52.26 | GPRO×968, FRVO×64, CRK×75, MMED×51, CTMX×317, SLN×80, EIX×20, CRDL×547 | BAK, EOSE, SLBT, SION | — | $25.53 | $9,464.00 | $9,489.53 | GPRO×968, FRVO×64, CRK×75, MMED×51, CTMX×317, SLN×80, EIX×20, CRDL×547, BAK×3, EOSE×2, SLBT×2, SION×1 | BUY BAK x3 @ 1.95; BUY EOSE x2 @ 3.57; BUY SLBT x2 @ 3.07; BUY SION x1 @ 7.31 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $8,746.36 | ▼ $9,996.26 (-3.74) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WWW` | 60 | $20.60 | $2.17 | — | $7,508.19 | ▼ $9,994.09 (-5.91) | baseline list, no extra gate; list probable,yday_gainer; ret5=+4.4; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $6,254.51 | ▼ $9,990.23 (-9.77) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $5,019.42 | ▼ $9,988.05 (-11.95) | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `OMER` | 72 | $17.35 | $2.21 | — | $3,768.02 | ▼ $9,985.85 (-14.15) | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ret5=+31.9; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $2,520.25 | ▼ $9,983.52 (-16.48) | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $1,266.11 | ▼ $9,977.54 (-22.46) | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MXCT` | 899 | $1.39 | $11.60 | — | $4.90 | ▼ $9,965.94 (-34.06) | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ret5=+25.2; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 290 | $4.79 | $3.80 | $+131.66 | $1,390.20 | ▼ $9,585.78 (-414.22) | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `WWW` | 60 | $20.08 | $2.19 | $-35.56 | $2,592.81 | ▼ $9,583.59 (-416.41) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 299 | $3.87 | $3.92 | $-100.46 | $3,746.02 | ▼ $9,579.67 (-420.33) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 63 | $19.58 | $2.20 | $-3.75 | $4,977.36 | ▼ $9,577.47 (-422.53) | dropped from list after 3 sess (min 3) | join🟡 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `OMER` | 72 | $17.13 | $2.23 | $-20.27 | $6,208.49 | ▼ $9,575.24 (-424.76) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AIRO` | 112 | $9.10 | $2.35 | $-230.92 | $7,225.34 | ▼ $9,572.89 (-427.11) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NCMI` | 464 | $2.56 | $6.07 | $-72.38 | $8,407.11 | ▼ $9,566.82 (-433.18) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MXCT` | 899 | $1.29 | $11.76 | $-113.25 | $9,555.06 | ▼ $9,555.06 (-444.94) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 57 | $20.65 | $2.16 | — | $8,375.85 | ▼ $9,552.90 (-447.10) | baseline list, no extra gate; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1194.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 161 | $7.38 | $2.47 | — | $7,185.20 | ▼ $9,550.43 (-449.57) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1194.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 160 | $7.45 | $2.47 | — | $5,990.73 | ▼ $9,547.96 (-452.04) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1194.38 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MSTR` | 10 | $113.23 | $2.02 | — | $4,856.41 | ▼ $9,545.94 (-454.06) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1194.38 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 110 | $10.77 | $2.32 | — | $3,669.39 | ▼ $9,543.62 (-456.38) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1194.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 126 | $9.46 | $2.37 | — | $2,475.06 | ▼ $9,541.25 (-458.75) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1194.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NG` | 142 | $8.38 | $2.42 | — | $1,282.68 | ▼ $9,538.83 (-461.17) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+7.5; leftover $1194.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 40 | $29.20 | $2.11 | — | $112.57 | ▼ $9,536.72 (-463.28) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1194.38 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $101.33 | ▲ $10,014.71 (+14.71) | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $16.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 12 | $1.32 | $0.19 | — | $85.29 | ▲ $10,014.51 (+14.51) | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $16.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 9 | $1.66 | $0.18 | — | $70.18 | ▲ $10,014.34 (+14.34) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $16.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 9 | $1.71 | $0.18 | — | $54.61 | ▲ $10,014.16 (+14.16) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $16.08 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 1 | $14.96 | $0.15 | — | $39.49 | ▲ $10,014.00 (+14.00) | baseline list, no extra gate; list probable,yday_gainer; ret5=-1.6; leftover $16.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 18 | $0.86 | $0.21 | — | $23.73 | ▲ $10,013.79 (+13.79) | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $16.08 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 57 | $20.85 | $2.18 | $+7.06 | $1,210.00 | ▼ $9,939.34 (-60.66) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRVI` | 161 | $8.31 | $2.51 | $+144.75 | $2,545.40 | ▼ $9,936.83 (-63.17) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DNA` | 160 | $6.82 | $2.51 | $-105.78 | $3,634.09 | ▼ $9,934.32 (-65.68) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MSTR` | 10 | $125.56 | $2.04 | $+119.24 | $4,887.65 | ▼ $9,932.28 (-67.72) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EXK` | 110 | $10.72 | $2.35 | $-10.17 | $6,064.51 | ▼ $9,929.94 (-70.06) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SCZM` | 126 | $9.57 | $2.40 | $+9.09 | $7,267.93 | ▼ $9,927.54 (-72.46) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NG` | 142 | $9.34 | $2.45 | $+131.45 | $8,591.76 | ▼ $9,925.09 (-74.91) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BLSH` | 40 | $31.00 | $2.13 | $+67.76 | $9,829.63 | ▼ $9,922.96 (-77.04) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 758 | $1.62 | $9.78 | — | $8,591.89 | ▼ $9,913.18 (-86.82) | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1228.70 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 614 | $2.00 | $7.92 | — | $7,355.97 | ▼ $9,905.26 (-94.74) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1228.70 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 332 | $3.70 | $4.28 | — | $6,123.29 | ▼ $9,900.98 (-99.02) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $1228.70 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 235 | $5.22 | $3.03 | — | $4,893.55 | ▼ $9,897.94 (-102.06) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1228.70 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 180 | $6.79 | $2.53 | — | $3,668.82 | ▼ $9,895.41 (-104.59) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $1228.70 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 82 | $14.86 | $2.24 | — | $2,448.07 | ▼ $9,893.18 (-106.82) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $1228.70 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 192 | $6.38 | $2.57 | — | $1,220.54 | ▼ $9,890.61 (-109.39) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1228.70 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3238 | $0.37 | $21.69 | — | $0.79 | ▼ $9,868.92 (-131.08) | baseline list, no extra gate; list probable,yday_gainer; ret5=-26.5; leftover $1228.70 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 1 | $15.35 | $0.18 | $+3.93 | $15.96 | ▲ $10,069.95 (+69.95) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 12 | $1.60 | $0.25 | $+2.92 | $34.91 | ▲ $10,069.70 (+69.70) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `BTBT` | 9 | $1.53 | $0.18 | $-1.53 | $48.50 | ▲ $10,069.52 (+69.52) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ENHA` | 9 | $1.63 | $0.19 | $-1.09 | $62.97 | ▲ $10,069.32 (+69.32) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `QDEL` | 1 | $15.09 | $0.17 | $-0.20 | $77.89 | ▲ $10,069.15 (+69.15) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ORBS` | 18 | $0.80 | $0.22 | $-1.58 | $92.07 | ▲ $10,068.93 (+68.93) | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 758 | $1.74 | $9.91 | $+71.27 | $1,401.08 | ▲ $10,218.16 (+218.16) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `NPWR` | 614 | $1.83 | $8.03 | $-120.33 | $2,516.67 | ▲ $10,210.13 (+210.13) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `PUSA` | 332 | $3.86 | $4.35 | $+44.49 | $3,793.84 | ▲ $10,205.78 (+205.78) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 235 | $4.88 | $3.08 | $-86.01 | $4,937.56 | ▲ $10,202.70 (+202.70) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALIT` | 82 | $14.54 | $2.26 | $-30.74 | $6,127.58 | ▲ $10,200.44 (+200.44) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 192 | $6.02 | $2.61 | $-74.29 | $7,280.81 | ▲ $10,197.83 (+197.83) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SAFX` | 3238 | $0.39 | $22.89 | $+20.18 | $8,520.74 | ▲ $10,174.94 (+174.94) | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $144.70 | $2.01 | — | $7,361.13 | ▲ $10,172.93 (+172.93) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1217.25 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 71 | $16.95 | $2.20 | — | $6,155.47 | ▲ $10,170.72 (+170.72) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1217.25 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 65 | $18.50 | $2.19 | — | $4,950.79 | ▲ $10,168.54 (+168.54) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1217.25 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 882 | $1.38 | $11.38 | — | $3,722.25 | ▲ $10,157.16 (+157.16) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $1217.25 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 36 | $33.78 | $2.10 | — | $2,504.07 | ▲ $10,155.06 (+155.06) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1217.25 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $1,306.86 | ▲ $10,153.05 (+153.05) | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1217.25 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 76 | $15.94 | $2.22 | — | $93.20 | ▲ $10,150.83 (+150.83) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1217.25 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 180 | $9.44 | $2.57 | $+471.90 | $1,789.83 | ▼ $9,733.26 (-266.74) | dropped from list after 4 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 8 | $142.00 | $2.03 | $-25.65 | $2,923.79 | ▼ $9,479.27 (-520.73) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BHVN` | 71 | $15.39 | $2.22 | $-115.19 | $4,014.26 | ▼ $9,477.05 (-522.95) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 65 | $17.29 | $2.21 | $-83.04 | $5,135.90 | ▼ $9,474.84 (-525.16) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `LVWR` | 882 | $1.19 | $11.53 | $-190.49 | $6,173.95 | ▼ $9,463.31 (-536.69) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 36 | $31.87 | $2.12 | $-72.98 | $7,319.15 | ▼ $9,461.19 (-538.81) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $127.63 | $2.03 | $-178.21 | $8,338.16 | ▼ $9,459.16 (-540.84) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 76 | $14.75 | $2.24 | $-94.90 | $9,456.91 | ▼ $9,456.91 (-543.09) | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 968 | $1.22 | $12.49 | — | $8,263.47 | ▼ $9,444.43 (-555.57) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1182.11 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 64 | $18.40 | $2.18 | — | $7,083.69 | ▼ $9,442.25 (-557.75) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1182.11 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 75 | $15.70 | $2.21 | — | $5,903.97 | ▼ $9,440.03 (-559.97) | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1182.11 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 51 | $22.78 | $2.14 | — | $4,740.05 | ▼ $9,437.89 (-562.11) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1182.11 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 317 | $3.72 | $4.09 | — | $3,556.72 | ▼ $9,433.80 (-566.20) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1182.11 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 80 | $14.70 | $2.23 | — | $2,378.49 | ▼ $9,431.57 (-568.43) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1182.11 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 20 | $56.78 | $2.05 | — | $1,240.84 | ▼ $9,429.52 (-570.48) | baseline list, no extra gate; list probable,yday_gainer; ret5=+0.3; leftover $1182.11 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 547 | $2.16 | $7.06 | — | $52.26 | ▼ $9,422.46 (-577.54) | baseline list, no extra gate; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1182.11 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 3 | $1.95 | $0.07 | — | $46.34 | ▼ $9,992.41 (-7.59) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $7.47 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 2 | $3.57 | $0.08 | — | $39.13 | ▼ $9,992.34 (-7.66) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $7.47 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 2 | $3.07 | $0.07 | — | $32.92 | ▼ $9,992.27 (-7.73) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $7.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SION` | 1 | $7.31 | $0.08 | — | $25.53 | ▼ $9,992.19 (-7.81) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $7.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `WWW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NCMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MXCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CDNL` | cash | leftover split 0.61 < 1 share @ 39.85 |
| 2026-08-17 | `ABX` | cash | leftover split 0.61 < 1 share @ 9.12 |
| 2026-08-17 | `FCEL` | cash | leftover split 0.61 < 1 share @ 22.37 |
| 2026-08-17 | `VERA` | cash | leftover split 0.61 < 1 share @ 31.30 |
| 2026-08-17 | `CELC` | cash | leftover split 0.61 < 1 share @ 92.99 |
| 2026-08-17 | `CAPR` | cash | leftover split 0.61 < 1 share @ 6.87 |
| 2026-08-17 | `HTFL` | cash | leftover split 0.61 < 1 share @ 41.23 |
| 2026-08-17 | `UMAC` | cash | leftover split 0.61 < 1 share @ 32.55 |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `WWW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OMER` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NCMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MXCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CBRS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COHR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TDTH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PGEN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MSTR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `EXK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SCZM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BLSH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DE` | cash | leftover split 16.08 < 1 share @ 623.26 |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MSTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `EXK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SCZM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BLSH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ENHA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `QDEL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ENHA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `QDEL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `ARCT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BTBT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ENHA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `QDEL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ORBS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `PUSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RZLT` | no_price | no 09:30 open |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-26 | `FLNC` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `AVEX` | no_price | no 09:30 open |
| 2026-08-26 | `ITG` | no_price | no 09:30 open |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `PUSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LVWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `LVWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AME` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NAGE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BMO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CTMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `EIX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | cash | leftover split 7.47 < 1 share @ 486.31 |
| 2026-09-04 | `MLYS` | cash | leftover split 7.47 < 1 share @ 29.15 |
| 2026-09-04 | `CCOI` | cash | leftover split 7.47 < 1 share @ 10.22 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GPRO` | 968 | 2026-09-03 @ $1.22 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1182.11 |
| `FRVO` | 64 | 2026-09-03 @ $18.40 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1182.11 |
| `CRK` | 75 | 2026-09-03 @ $15.70 | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1182.11 |
| `MMED` | 51 | 2026-09-03 @ $22.78 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1182.11 |
| `CTMX` | 317 | 2026-09-03 @ $3.72 | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1182.11 |
| `SLN` | 80 | 2026-09-03 @ $14.70 | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1182.11 |
| `EIX` | 20 | 2026-09-03 @ $56.78 | baseline list, no extra gate; list probable,yday_gainer; ret5=+0.3; leftover $1182.11 |
| `CRDL` | 547 | 2026-09-03 @ $2.16 | baseline list, no extra gate; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1182.11 |
| `BAK` | 3 | 2026-09-04 @ $1.95 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $7.47 |
| `EOSE` | 2 | 2026-09-04 @ $3.57 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $7.47 |
| `SLBT` | 2 | 2026-09-04 @ $3.07 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $7.47 |
| `SION` | 1 | 2026-09-04 @ $7.31 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $7.47 |
