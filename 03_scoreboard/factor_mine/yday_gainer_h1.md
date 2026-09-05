# Factor mine action — `yday_gainer_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `yday_gainer` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **+4.56%** ($10,456) · signal-only (no cash/fees) was +5.63%. Starts YES **11/17**. Fills 116 · skips 62 · realized $+422.04.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `yday_gainer` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $332.47.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-14 | +5.50 | $10,000.00 | — | ANGX, WWW, HYLN, ARX, OMER, AIRO, NCMI, MXCT | — | $4.90 | $9,799.82 | $9,804.72 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 | BUY ANGX x290 @ 4.31; BUY WWW x60 @ 20.60; BUY HYLN x299 @ 4.18; BUY ARX x63 @ 19.57; BUY OMER x72 @ 17.35; BUY AIRO x112 @ 11.12; BUY NCMI x464 @ 2.69; BUY MXCT x899 @ 1.39 |
| 2026-08-17 | +2.25 | $4.90 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 | CDNL, ABX, FCEL, VERA, CELC, CAPR, HTFL, UMAC | ANGX, WWW, HYLN, ARX, OMER, AIRO, NCMI, MXCT | $120.48 | $9,699.62 | $9,820.10 | CDNL×30, ABX×134, FCEL×54, VERA×39, CELC×13, CAPR×178, HTFL×29, UMAC×37 | SELL ANGX (dropped from list after 1 sess (min 1)); SELL WWW (dropped from list after 1 sess (min 1)); SELL HYLN (dropped from list after 1 sess (min 1)); SELL ARX (dropped from list after 1 sess (min 1)); SELL OMER (dropped from list after 1 sess (min 1)); SELL AIRO (dropped from list after 1 sess (min 1)); SELL NCMI (dropped from list after 1 sess (min 1)); SELL MXCT (dropped from list after 1 sess (min 1)); BUY CDNL x30 @ 39.85; BUY ABX x134 @ 9.12; BUY FCEL x54 @ 22.37; BUY VERA x39 @ 31.30; BUY CELC x13 @ 92.99; BUY CAPR x178 @ 6.87; BUY HTFL x29 @ 41.23; BUY UMAC x37 @ 32.55 |
| 2026-08-18 | -6.20 | $120.48 | CDNL×30, ABX×134, FCEL×54, VERA×39, CELC×13, CAPR×178, HTFL×29, UMAC×37 | — | CDNL, ABX, FCEL, VERA, CELC, CAPR, HTFL, UMAC | $9,722.02 | $0.00 | $9,722.02 | — | SELL CDNL (dropped from list after 1 sess (min 1)); SELL ABX (dropped from list after 1 sess (min 1)); SELL FCEL (dropped from list after 1 sess (min 1)); SELL VERA (dropped from list after 1 sess (min 1)); SELL CELC (dropped from list after 1 sess (min 1)); SELL CAPR (dropped from list after 1 sess (min 1)); SELL HTFL (dropped from list after 1 sess (min 1)); SELL UMAC (dropped from list after 1 sess (min 1)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $9,722.02 | — | — | — | $9,722.02 | $0.00 | $9,722.02 | — | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $9,722.02 | — | CDE, MRVI, DNA, MSTR, EXK, SCZM, NG, BLSH | — | $119.55 | $9,737.06 | $9,856.61 | CDE×58, MRVI×164, DNA×163, MSTR×10, EXK×112, SCZM×128, NG×145, BLSH×41 | BUY CDE x58 @ 20.65; BUY MRVI x164 @ 7.38; BUY DNA x163 @ 7.45; BUY MSTR x10 @ 113.23; BUY EXK x112 @ 10.77; BUY SCZM x128 @ 9.46; BUY NG x145 @ 8.38; BUY BLSH x41 @ 29.20 |
| 2026-08-21 | +3.25 | $119.55 | CDE×58, MRVI×164, DNA×163, MSTR×10, EXK×112, SCZM×128, NG×145, BLSH×41 | ARCT, CYPH, BTBT, ENHA, DE, QDEL, ORBS | CDE, DNA, MSTR, EXK, SCZM, NG, BLSH | $0.91 | $10,518.76 | $10,519.67 | MRVI×164, ARCT×113, CYPH×955, BTBT×759, ENHA×737, DE×2, QDEL×84, ORBS×1425 | SELL CDE (dropped from list after 1 sess (min 1)); SELL DNA (dropped from list after 1 sess (min 1)); SELL MSTR (dropped from list after 1 sess (min 1)); SELL EXK (dropped from list after 1 sess (min 1)); SELL SCZM (dropped from list after 1 sess (min 1)); SELL NG (dropped from list after 1 sess (min 1)); SELL BLSH (dropped from list after 1 sess (min 1)); BUY ARCT x113 @ 11.13; BUY CYPH x955 @ 1.32; BUY BTBT x759 @ 1.66; BUY ENHA x737 @ 1.71; BUY DE x2 @ 623.26; BUY QDEL x84 @ 14.96; BUY ORBS x1425 @ 0.86 |
| 2026-08-24 | -5.17 | $0.91 | MRVI×164, ARCT×113, CYPH×955, BTBT×759, ENHA×737, DE×2, QDEL×84, ORBS×1425 | — | MRVI, ARCT, CYPH, BTBT, ENHA, DE, QDEL, ORBS | $10,867.23 | $0.00 | $10,867.23 | — | SELL MRVI (dropped from list after 2 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL CYPH (dropped from list after 1 sess (min 1)); SELL BTBT (dropped from list after 1 sess (min 1)); SELL ENHA (dropped from list after 1 sess (min 1)); SELL DE (dropped from list after 1 sess (min 1)); SELL QDEL (dropped from list after 1 sess (min 1)); SELL ORBS (dropped from list after 1 sess (min 1)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $10,867.23 | — | BMEA, NPWR, PUSA, ALVO, CAPR, ALIT, ZURA, SAFX | — | $0.84 | $11,003.77 | $11,004.61 | BMEA×838, NPWR×679, PUSA×367, ALVO×260, CAPR×200, ALIT×91, ZURA×212, SAFX×3551 | BUY BMEA x838 @ 1.62; BUY NPWR x679 @ 2.00; BUY PUSA x367 @ 3.70; BUY ALVO x260 @ 5.22; BUY CAPR x200 @ 6.79; BUY ALIT x91 @ 14.86; BUY ZURA x212 @ 6.38; BUY SAFX x3551 @ 0.37 |
| 2026-08-26 | +2.02 | $0.84 | BMEA×838, NPWR×679, PUSA×367, ALVO×260, CAPR×200, ALIT×91, ZURA×212, SAFX×3551 | — | — | $0.84 | $10,807.35 | $10,808.19 | BMEA×838, NPWR×679, PUSA×367, ALVO×260, CAPR×200, ALIT×91, ZURA×212, SAFX×3551 | hold BMEA,NPWR,PUSA,ALVO,CAPR,ALIT,ZURA,SAFX |
| 2026-08-27 | — | $0.84 | BMEA×838, NPWR×679, PUSA×367, ALVO×260, CAPR×200, ALIT×91, ZURA×212, SAFX×3551 | — | BMEA, NPWR, PUSA, ALVO, CAPR, ALIT, ZURA, SAFX | $10,974.21 | $0.00 | $10,974.21 | — | SELL BMEA (dropped from list after 2 sess (min 1)); SELL NPWR (dropped from list after 2 sess (min 1)); SELL PUSA (dropped from list after 2 sess (min 1)); SELL ALVO (dropped from list after 2 sess (min 1)); SELL CAPR (dropped from list after 2 sess (min 1)); SELL ALIT (dropped from list after 2 sess (min 1)); SELL ZURA (dropped from list after 2 sess (min 1)); SELL SAFX (dropped from list after 2 sess (min 1)) |
| 2026-08-28 | +0.75 | $10,974.21 | — | ANF, BHVN, BZ, CAPR, LVWR, SEDG, SMTC, GRRR | — | $111.15 | $10,753.16 | $10,864.31 | ANF×9, BHVN×80, BZ×74, CAPR×149, LVWR×994, SEDG×40, SMTC×9, GRRR×86 | BUY ANF x9 @ 144.70; BUY BHVN x80 @ 16.95; BUY BZ x74 @ 18.50; BUY CAPR x149 @ 9.19; BUY LVWR x994 @ 1.38; BUY SEDG x40 @ 33.78; BUY SMTC x9 @ 149.40; BUY GRRR x86 @ 15.94 |
| 2026-08-31 | -5.85 | $111.15 | ANF×9, BHVN×80, BZ×74, CAPR×149, LVWR×994, SEDG×40, SMTC×9, GRRR×86 | — | ANF, BHVN, BZ, CAPR, LVWR, SEDG, SMTC, GRRR | $10,437.02 | $0.00 | $10,437.02 | — | SELL ANF (dropped from list after 1 sess (min 1)); SELL BHVN (dropped from list after 1 sess (min 1)); SELL BZ (dropped from list after 1 sess (min 1)); SELL CAPR (dropped from list after 1 sess (min 1)); SELL LVWR (dropped from list after 1 sess (min 1)); SELL SEDG (dropped from list after 1 sess (min 1)); SELL SMTC (dropped from list after 1 sess (min 1)); SELL GRRR (dropped from list after 1 sess (min 1)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $10,437.02 | — | — | — | $10,437.02 | $0.00 | $10,437.02 | — | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $10,437.02 | — | — | — | $10,437.02 | $0.00 | $10,437.02 | — | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $10,437.02 | — | GPRO, FRVO, CRK, MMED, CTMX, SLN, EIX, CRDL | — | $59.04 | $10,835.56 | $10,894.60 | GPRO×1069, FRVO×70, CRK×83, MMED×57, CTMX×350, SLN×88, EIX×22, CRDL×603 | BUY GPRO x1069 @ 1.22; BUY FRVO x70 @ 18.40; BUY CRK x83 @ 15.70; BUY MMED x57 @ 22.78; BUY CTMX x350 @ 3.72; BUY SLN x88 @ 14.70; BUY EIX x22 @ 56.78; BUY CRDL x603 @ 2.16 |
| 2026-09-04 | — | $59.04 | GPRO×1069, FRVO×70, CRK×83, MMED×57, CTMX×350, SLN×88, EIX×22, CRDL×603 | BAK, EOSE, SLBT, DELL, MLYS, CCOI, SION | FRVO, CRK, MMED, CTMX, SLN, EIX, CRDL | $332.47 | $10,123.27 | $10,455.74 | GPRO×1069, BAK×666, EOSE×364, SLBT×423, DELL×2, MLYS×44, CCOI×127, SION×177 | SELL FRVO (dropped from list after 1 sess (min 1)); SELL CRK (dropped from list after 1 sess (min 1)); SELL MMED (dropped from list after 1 sess (min 1)); SELL CTMX (dropped from list after 1 sess (min 1)); SELL SLN (dropped from list after 1 sess (min 1)); SELL EIX (dropped from list after 1 sess (min 1)); SELL CRDL (dropped from list after 1 sess (min 1)); BUY BAK x666 @ 1.95; BUY EOSE x364 @ 3.57; BUY SLBT x423 @ 3.07; BUY DELL x2 @ 486.31; BUY MLYS x44 @ 29.15; BUY CCOI x127 @ 10.22; BUY SION x177 @ 7.31 |

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
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $1,335.10 | ▼ $9,846.67 (-153.33) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WWW` | 60 | $20.98 | $2.19 | $+18.44 | $2,591.71 | ▼ $9,844.48 (-155.52) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 299 | $4.10 | $3.92 | $-31.69 | $3,813.69 | ▼ $9,840.56 (-159.44) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $5,044.40 | ▼ $9,838.36 (-161.64) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `OMER` | 72 | $17.17 | $2.23 | $-17.39 | $6,278.41 | ▼ $9,836.13 (-163.87) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 112 | $9.57 | $2.35 | $-178.28 | $7,347.90 | ▼ $9,833.78 (-166.22) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 464 | $2.80 | $6.07 | $+38.98 | $8,641.03 | ▼ $9,827.71 (-172.29) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MXCT` | 899 | $1.32 | $11.76 | $-86.28 | $9,815.95 | ▼ $9,815.95 (-184.05) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 30 | $39.85 | $2.08 | — | $8,618.37 | ▼ $9,813.87 (-186.13) | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1226.99 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 134 | $9.12 | $2.39 | — | $7,393.90 | ▼ $9,811.48 (-188.52) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1226.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `FCEL` | 54 | $22.37 | $2.15 | — | $6,183.77 | ▼ $9,809.33 (-190.67) | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=+9.5; leftover $1226.99 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 39 | $31.30 | $2.11 | — | $4,960.96 | ▼ $9,807.22 (-192.78) | baseline list, no extra gate; list probable,yday_gainer; ret5=-3.8; leftover $1226.99 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 13 | $92.99 | $2.03 | — | $3,750.06 | ▼ $9,805.19 (-194.81) | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.8; leftover $1226.99 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 178 | $6.87 | $2.52 | — | $2,524.68 | ▼ $9,802.67 (-197.33) | baseline list, no extra gate; list yday_gainer,yday_mover; ret5=+62.6; leftover $1226.99 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $1,326.93 | ▼ $9,800.59 (-199.41) | baseline list, no extra gate; list yday_gainer,yday_mover; ret5=+46.0; leftover $1226.99 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 37 | $32.55 | $2.10 | — | $120.48 | ▼ $9,798.49 (-201.51) | baseline list, no extra gate; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1226.99 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 30 | $41.57 | $2.10 | $+47.42 | $1,365.48 | ▼ $9,737.58 (-262.42) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 134 | $9.03 | $2.42 | $-16.88 | $2,573.07 | ▼ $9,735.15 (-264.85) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FCEL` | 54 | $21.18 | $2.17 | $-68.58 | $3,714.62 | ▼ $9,732.98 (-267.02) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 39 | $31.31 | $2.13 | $-3.84 | $4,933.59 | ▼ $9,730.86 (-269.14) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 13 | $92.38 | $2.05 | $-12.01 | $6,132.48 | ▼ $9,728.81 (-271.19) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CAPR` | 178 | $7.50 | $2.56 | $+107.05 | $7,464.91 | ▼ $9,726.24 (-273.76) | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $8,666.31 | ▼ $9,724.14 (-275.86) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 37 | $28.59 | $2.12 | $-150.74 | $9,722.02 | ▼ $9,722.02 (-277.98) | dropped from list after 1 sess (min 1) | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 58 | $20.65 | $2.16 | — | $8,522.16 | ▼ $9,719.86 (-280.14) | baseline list, no extra gate; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1215.25 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 164 | $7.38 | $2.48 | — | $7,309.36 | ▼ $9,717.38 (-282.62) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1215.25 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 163 | $7.45 | $2.48 | — | $6,092.53 | ▼ $9,714.90 (-285.10) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1215.25 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MSTR` | 10 | $113.23 | $2.02 | — | $4,958.21 | ▼ $9,712.88 (-287.12) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1215.25 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 112 | $10.77 | $2.33 | — | $3,749.64 | ▼ $9,710.55 (-289.45) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1215.25 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 128 | $9.46 | $2.37 | — | $2,536.39 | ▼ $9,708.18 (-291.82) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1215.25 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NG` | 145 | $8.38 | $2.42 | — | $1,318.86 | ▼ $9,705.75 (-294.25) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+7.5; leftover $1215.25 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 41 | $29.20 | $2.11 | — | $119.55 | ▼ $9,703.64 (-296.36) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1215.25 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 58 | $21.75 | $2.18 | $+59.45 | $1,378.87 | ▲ $10,187.25 (+187.25) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DNA` | 163 | $7.09 | $2.52 | $-63.68 | $2,532.02 | ▲ $10,184.73 (+184.73) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MSTR` | 10 | $119.69 | $2.04 | $+60.54 | $3,726.88 | ▲ $10,182.69 (+182.69) | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `EXK` | 112 | $11.34 | $2.35 | $+59.16 | $4,994.61 | ▲ $10,180.34 (+180.34) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SCZM` | 128 | $10.26 | $2.41 | $+97.62 | $6,305.48 | ▲ $10,177.93 (+177.93) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NG` | 145 | $9.02 | $2.46 | $+87.92 | $7,610.92 | ▲ $10,175.47 (+175.47) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `BLSH` | 41 | $29.75 | $2.13 | $+18.30 | $8,828.54 | ▲ $10,173.34 (+173.34) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 113 | $11.13 | $2.33 | — | $7,568.52 | ▲ $10,171.01 (+171.01) | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1261.22 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 955 | $1.32 | $12.32 | — | $6,295.60 | ▲ $10,158.69 (+158.69) | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1261.22 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 759 | $1.66 | $9.79 | — | $5,025.87 | ▲ $10,148.90 (+148.90) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1261.22 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 737 | $1.71 | $9.51 | — | $3,756.09 | ▲ $10,139.39 (+139.39) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $1261.22 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $2,507.57 | ▲ $10,137.39 (+137.39) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1261.22 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 84 | $14.96 | $2.24 | — | $1,248.69 | ▲ $10,135.15 (+135.15) | baseline list, no extra gate; list probable,yday_gainer; ret5=-1.6; leftover $1261.22 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1425 | $0.86 | $16.59 | — | $0.91 | ▲ $10,118.57 (+118.57) | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1261.22 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 164 | $8.59 | $2.52 | $+193.44 | $1,407.15 | ▲ $10,923.14 (+923.14) | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 113 | $13.26 | $2.36 | $+236.00 | $2,903.17 | ▲ $10,920.78 (+920.78) | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 955 | $1.83 | $12.49 | $+462.24 | $4,638.32 | ▲ $10,908.28 (+908.28) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 759 | $1.55 | $9.93 | $-103.21 | $5,804.85 | ▲ $10,898.36 (+898.36) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ENHA` | 737 | $1.74 | $9.64 | $+2.96 | $7,077.59 | ▲ $10,888.72 (+888.72) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.62 | $2.02 | $+56.71 | $8,382.81 | ▲ $10,886.70 (+886.70) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QDEL` | 84 | $14.71 | $2.27 | $-25.51 | $9,616.18 | ▲ $10,884.43 (+884.43) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 1425 | $0.89 | $17.20 | $+3.26 | $10,867.23 | ▲ $10,867.23 (+867.23) | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 838 | $1.62 | $10.81 | — | $9,498.86 | ▲ $10,856.42 (+856.42) | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1358.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 679 | $2.00 | $8.76 | — | $8,132.10 | ▲ $10,847.66 (+847.66) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1358.40 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 367 | $3.70 | $4.73 | — | $6,769.47 | ▲ $10,842.93 (+842.93) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $1358.40 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 260 | $5.22 | $3.35 | — | $5,408.91 | ▲ $10,839.57 (+839.57) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1358.40 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 200 | $6.79 | $2.59 | — | $4,048.32 | ▲ $10,836.98 (+836.98) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $1358.40 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 91 | $14.86 | $2.26 | — | $2,693.80 | ▲ $10,834.72 (+834.72) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $1358.40 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 212 | $6.38 | $2.73 | — | $1,338.50 | ▲ $10,831.98 (+831.98) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1358.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3551 | $0.37 | $23.79 | — | $0.84 | ▲ $10,808.19 (+808.19) | baseline list, no extra gate; list probable,yday_gainer; ret5=-26.5; leftover $1358.40 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 838 | $1.75 | $10.96 | $+87.17 | $1,456.38 | ▲ $11,022.69 (+1,022.69) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 679 | $1.93 | $8.88 | $-65.17 | $2,757.97 | ▲ $11,013.81 (+1,013.81) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PUSA` | 367 | $3.84 | $4.81 | $+41.84 | $4,162.44 | ▲ $11,009.00 (+1,009.00) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALVO` | 260 | $4.98 | $3.41 | $-69.16 | $5,453.84 | ▲ $11,005.60 (+1,005.60) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 200 | $8.29 | $2.64 | $+294.77 | $7,109.20 | ▲ $11,002.96 (+1,002.96) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALIT` | 91 | $14.85 | $2.29 | $-5.46 | $8,458.26 | ▲ $11,000.67 (+1,000.67) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ZURA` | 212 | $6.13 | $2.78 | $-58.52 | $9,755.04 | ▲ $10,997.89 (+997.89) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SAFX` | 3551 | $0.35 | $23.68 | $-118.49 | $10,974.21 | ▲ $10,974.21 (+974.21) | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $144.70 | $2.02 | — | $9,669.89 | ▲ $10,972.19 (+972.19) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1371.78 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 80 | $16.95 | $2.23 | — | $8,311.66 | ▲ $10,969.96 (+969.96) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1371.78 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 74 | $18.50 | $2.21 | — | $6,940.45 | ▲ $10,967.75 (+967.75) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1371.78 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 149 | $9.19 | $2.44 | — | $5,568.70 | ▲ $10,965.31 (+965.31) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1371.78 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 994 | $1.38 | $12.82 | — | $4,184.16 | ▲ $10,952.49 (+952.49) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $1371.78 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 40 | $33.78 | $2.11 | — | $2,830.85 | ▲ $10,950.38 (+950.38) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1371.78 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $149.40 | $2.02 | — | $1,484.23 | ▲ $10,948.36 (+948.36) | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1371.78 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 86 | $15.94 | $2.25 | — | $111.15 | ▲ $10,946.12 (+946.12) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1371.78 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.67 | $2.04 | $+31.68 | $1,447.14 | ▲ $10,463.42 (+463.42) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 80 | $15.44 | $2.25 | $-125.28 | $2,680.08 | ▲ $10,461.16 (+461.16) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 74 | $17.89 | $2.23 | $-49.59 | $4,001.71 | ▲ $10,458.93 (+458.93) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 149 | $9.44 | $2.47 | $+32.34 | $5,405.80 | ▲ $10,456.46 (+456.46) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LVWR` | 994 | $1.37 | $13.00 | $-35.76 | $6,754.58 | ▲ $10,443.46 (+443.46) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 40 | $31.50 | $2.13 | $-95.44 | $8,012.45 | ▲ $10,441.33 (+441.33) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $133.04 | $2.04 | $-151.29 | $9,207.77 | ▲ $10,439.29 (+439.29) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 86 | $14.32 | $2.27 | $-143.84 | $10,437.02 | ▲ $10,437.02 (+437.02) | dropped from list after 1 sess (min 1) | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1069 | $1.22 | $13.79 | — | $9,119.05 | ▲ $10,423.23 (+423.23) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1304.63 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 70 | $18.40 | $2.20 | — | $7,828.85 | ▲ $10,421.03 (+421.03) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1304.63 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 83 | $15.70 | $2.24 | — | $6,523.51 | ▲ $10,418.79 (+418.79) | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1304.63 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 57 | $22.78 | $2.16 | — | $5,222.89 | ▲ $10,416.63 (+416.63) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1304.63 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 350 | $3.72 | $4.51 | — | $3,916.37 | ▲ $10,412.11 (+412.11) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1304.63 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 88 | $14.70 | $2.25 | — | $2,620.52 | ▲ $10,409.86 (+409.86) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1304.63 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 22 | $56.78 | $2.06 | — | $1,369.30 | ▲ $10,407.80 (+407.80) | baseline list, no extra gate; list probable,yday_gainer; ret5=+0.3; leftover $1304.63 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 603 | $2.16 | $7.78 | — | $59.04 | ▲ $10,400.02 (+400.02) | baseline list, no extra gate; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1304.63 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 70 | $18.27 | $2.22 | $-13.52 | $1,335.72 | ▲ $11,028.13 (+1,028.13) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 83 | $15.45 | $2.26 | $-25.25 | $2,615.81 | ▲ $11,025.87 (+1,025.87) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 57 | $23.88 | $2.18 | $+58.36 | $3,974.79 | ▲ $11,023.69 (+1,023.69) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CTMX` | 350 | $3.73 | $4.58 | $-5.60 | $5,275.70 | ▲ $11,019.10 (+1,019.10) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SLN` | 88 | $14.85 | $2.28 | $+8.67 | $6,580.23 | ▲ $11,016.83 (+1,016.83) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 22 | $55.42 | $2.08 | $-34.05 | $7,797.39 | ▲ $11,014.75 (+1,014.75) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 603 | $2.18 | $7.89 | $-3.61 | $9,104.04 | ▲ $11,006.86 (+1,006.86) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 666 | $1.95 | $8.59 | — | $7,796.75 | ▲ $10,998.27 (+998.27) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1300.58 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 364 | $3.57 | $4.70 | — | $6,492.57 | ▲ $10,993.57 (+993.57) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1300.58 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 423 | $3.07 | $5.46 | — | $5,188.51 | ▲ $10,988.12 (+988.12) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1300.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $4,213.89 | ▲ $10,986.12 (+986.12) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1300.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 44 | $29.15 | $2.12 | — | $2,929.17 | ▲ $10,984.00 (+984.00) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1300.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 127 | $10.22 | $2.37 | — | $1,628.86 | ▲ $10,981.63 (+981.63) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1300.58 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SION` | 177 | $7.31 | $2.52 | — | $332.47 | ▲ $10,979.11 (+979.11) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $1300.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
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
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `BMEA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `NPWR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `PUSA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALVO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALIT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ZURA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `SAFX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `RZLT` | no_price | no 09:30 open |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-26 | `FLNC` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `AVEX` | no_price | no 09:30 open |
| 2026-08-26 | `ITG` | no_price | no 09:30 open |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
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

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GPRO` | 1069 | 2026-09-03 @ $1.22 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1304.63 |
| `BAK` | 666 | 2026-09-04 @ $1.95 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1300.58 |
| `EOSE` | 364 | 2026-09-04 @ $3.57 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1300.58 |
| `SLBT` | 423 | 2026-09-04 @ $3.07 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1300.58 |
| `DELL` | 2 | 2026-09-04 @ $486.31 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1300.58 |
| `MLYS` | 44 | 2026-09-04 @ $29.15 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1300.58 |
| `CCOI` | 127 | 2026-09-04 @ $10.22 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1300.58 |
| `SION` | 177 | 2026-09-04 @ $7.31 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $1300.58 |
