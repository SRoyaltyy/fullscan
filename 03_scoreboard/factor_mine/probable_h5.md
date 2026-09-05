# Factor mine action — `probable_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `probable` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-19.05%** ($8,095) · signal-only (no cash/fees) was -5.25%. Starts YES **6/17**. Fills 96 · skips 228 · realized $-1793.65.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `probable` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $159.52.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-14 | +5.50 | $10,000.00 | — | ANGX, WWW, HYLN, WDC, FOSL, ADUR, AIRS, ALGM | — | $269.08 | $9,716.38 | $9,985.46 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28 | BUY ANGX x290 @ 4.31; BUY WWW x60 @ 20.60; BUY HYLN x299 @ 4.18; BUY WDC x2 @ 503.50; BUY FOSL x221 @ 5.64; BUY ADUR x75 @ 16.50; BUY AIRS x370 @ 3.37; BUY ALGM x28 @ 44.06 |
| 2026-08-17 | +2.25 | $269.08 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28 | ABX, FCEL, VERA, BW, OCC, ALM | — | $104.70 | $9,849.32 | $9,954.02 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28, ABX×3, FCEL×1, VERA×1, BW×3, OCC×1, ALM×2 | BUY ABX x3 @ 9.12; BUY FCEL x1 @ 22.37; BUY VERA x1 @ 31.30; BUY BW x3 @ 10.35; BUY OCC x1 @ 18.24; BUY ALM x2 @ 16.20 |
| 2026-08-18 | -6.20 | $104.70 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28, ABX×3, FCEL×1, VERA×1, BW×3, OCC×1, ALM×2 | — | — | $104.70 | $9,396.01 | $9,500.71 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28, ABX×3, FCEL×1, VERA×1, BW×3, OCC×1, ALM×2 | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $104.70 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28, ABX×3, FCEL×1, VERA×1, BW×3, OCC×1, ALM×2 | — | — | $104.70 | $9,285.83 | $9,390.53 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28, ABX×3, FCEL×1, VERA×1, BW×3, OCC×1, ALM×2 | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $104.70 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28, ABX×3, FCEL×1, VERA×1, BW×3, OCC×1, ALM×2 | MRVI, DNA, EXK, SCZM, NG | — | $60.81 | $9,087.68 | $9,148.49 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28, ABX×3, FCEL×1, VERA×1, BW×3, OCC×1, ALM×2, MRVI×1, DNA×1, EXK×1, SCZM×1, NG×1 | BUY MRVI x1 @ 7.38; BUY DNA x1 @ 7.45; BUY EXK x1 @ 10.77; BUY SCZM x1 @ 9.46; BUY NG x1 @ 8.38 |
| 2026-08-21 | +3.25 | $60.81 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28, ABX×3, FCEL×1, VERA×1, BW×3, OCC×1, ALM×2, MRVI×1, DNA×1, EXK×1, SCZM×1, NG×1 | BTBT, ENHA, DE, QDEL, ORBS, GORO, QTRX | ANGX, WWW, HYLN, WDC, FOSL, ADUR, AIRS, ALGM | $1.63 | $9,114.25 | $9,115.88 | ABX×3, FCEL×1, VERA×1, BW×3, OCC×1, ALM×2, MRVI×1, DNA×1, EXK×1, SCZM×1, NG×1, BTBT×776, ENHA×753, DE×2, QDEL×86, ORBS×1491, GORO×414, QTRX×413 | SELL ANGX (dropped from list after 5 sess (min 5)); SELL WWW (dropped from list after 5 sess (min 5)); SELL HYLN (dropped from list after 5 sess (min 5)); SELL WDC (dropped from list after 5 sess (min 5)); SELL FOSL (dropped from list after 5 sess (min 5)); SELL ADUR (dropped from list after 5 sess (min 5)); SELL AIRS (dropped from list after 5 sess (min 5)); SELL ALGM (dropped from list after 5 sess (min 5)); BUY BTBT x776 @ 1.66; BUY ENHA x753 @ 1.71; BUY DE x2 @ 623.26; BUY QDEL x86 @ 14.96; BUY ORBS x1491 @ 0.86; BUY GORO x414 @ 3.11; BUY QTRX x413 @ 3.11 |
| 2026-08-24 | -5.17 | $1.63 | ABX×3, FCEL×1, VERA×1, BW×3, OCC×1, ALM×2, MRVI×1, DNA×1, EXK×1, SCZM×1, NG×1, BTBT×776, ENHA×753, DE×2, QDEL×86, ORBS×1491, GORO×414, QTRX×413 | — | ABX, FCEL, VERA, BW, OCC, ALM | $155.78 | $8,956.81 | $9,112.59 | MRVI×1, DNA×1, EXK×1, SCZM×1, NG×1, BTBT×776, ENHA×753, DE×2, QDEL×86, ORBS×1491, GORO×414, QTRX×413 | SELL ABX (dropped from list after 5 sess (min 5)); SELL FCEL (dropped from list after 5 sess (min 5)); SELL VERA (dropped from list after 5 sess (min 5)); SELL BW (dropped from list after 5 sess (min 5)); SELL OCC (dropped from list after 5 sess (min 5)); SELL ALM (dropped from list after 5 sess (min 5)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $155.78 | MRVI×1, DNA×1, EXK×1, SCZM×1, NG×1, BTBT×776, ENHA×753, DE×2, QDEL×86, ORBS×1491, GORO×414, QTRX×413 | BMEA, NPWR, PUSA, ALVO, CAPR, ALIT, ZURA, SAFX | — | $15.72 | $9,050.06 | $9,065.78 | MRVI×1, DNA×1, EXK×1, SCZM×1, NG×1, BTBT×776, ENHA×753, DE×2, QDEL×86, ORBS×1491, GORO×414, QTRX×413, BMEA×12, NPWR×9, PUSA×5, ALVO×3, CAPR×2, ALIT×1, ZURA×3, SAFX×52 | BUY BMEA x12 @ 1.62; BUY NPWR x9 @ 2.00; BUY PUSA x5 @ 3.70; BUY ALVO x3 @ 5.22; BUY CAPR x2 @ 6.79; BUY ALIT x1 @ 14.86; BUY ZURA x3 @ 6.38; BUY SAFX x52 @ 0.37 |
| 2026-08-26 | +2.02 | $15.72 | MRVI×1, DNA×1, EXK×1, SCZM×1, NG×1, BTBT×776, ENHA×753, DE×2, QDEL×86, ORBS×1491, GORO×414, QTRX×413, BMEA×12, NPWR×9, PUSA×5, ALVO×3, CAPR×2, ALIT×1, ZURA×3, SAFX×52 | — | — | $15.72 | $9,057.02 | $9,072.74 | MRVI×1, DNA×1, EXK×1, SCZM×1, NG×1, BTBT×776, ENHA×753, DE×2, QDEL×86, ORBS×1491, GORO×414, QTRX×413, BMEA×12, NPWR×9, PUSA×5, ALVO×3, CAPR×2, ALIT×1, ZURA×3, SAFX×52 | hold MRVI,DNA,EXK,SCZM,NG,BTBT,ENHA,DE,QDEL,ORBS,GORO,QTRX,BMEA,NPWR,PUSA,ALVO,CAPR,ALIT,ZURA,SAFX |
| 2026-08-27 | — | $15.72 | MRVI×1, DNA×1, EXK×1, SCZM×1, NG×1, BTBT×776, ENHA×753, DE×2, QDEL×86, ORBS×1491, GORO×414, QTRX×413, BMEA×12, NPWR×9, PUSA×5, ALVO×3, CAPR×2, ALIT×1, ZURA×3, SAFX×52 | — | MRVI, DNA, EXK, SCZM, NG | $61.30 | $8,889.80 | $8,951.10 | BTBT×776, ENHA×753, DE×2, QDEL×86, ORBS×1491, GORO×414, QTRX×413, BMEA×12, NPWR×9, PUSA×5, ALVO×3, CAPR×2, ALIT×1, ZURA×3, SAFX×52 | SELL MRVI (dropped from list after 5 sess (min 5)); SELL DNA (dropped from list after 5 sess (min 5)); SELL EXK (dropped from list after 5 sess (min 5)); SELL SCZM (dropped from list after 5 sess (min 5)); SELL NG (dropped from list after 5 sess (min 5)) |
| 2026-08-28 | +0.75 | $61.30 | BTBT×776, ENHA×753, DE×2, QDEL×86, ORBS×1491, GORO×414, QTRX×413, BMEA×12, NPWR×9, PUSA×5, ALVO×3, CAPR×2, ALIT×1, ZURA×3, SAFX×52 | ANF, BHVN, BZ, LVWR, SEDG, SMTC, GRRR | BTBT, ENHA, DE, QDEL, ORBS, GORO, QTRX | $167.51 | $8,582.89 | $8,750.40 | BMEA×12, NPWR×9, PUSA×5, ALVO×3, CAPR×2, ALIT×1, ZURA×3, SAFX×52, ANF×8, BHVN×74, BZ×68, LVWR×913, SEDG×37, SMTC×8, GRRR×79 | SELL BTBT (dropped from list after 5 sess (min 5)); SELL ENHA (dropped from list after 5 sess (min 5)); SELL DE (dropped from list after 5 sess (min 5)); SELL QDEL (dropped from list after 5 sess (min 5)); SELL ORBS (dropped from list after 5 sess (min 5)); SELL GORO (dropped from list after 5 sess (min 5)); SELL QTRX (dropped from list after 5 sess (min 5)); BUY ANF x8 @ 144.70; BUY BHVN x74 @ 16.95; BUY BZ x68 @ 18.50; BUY LVWR x913 @ 1.38; BUY SEDG x37 @ 33.78; BUY SMTC x8 @ 149.40; BUY GRRR x79 @ 15.94 |
| 2026-08-31 | -5.85 | $167.51 | BMEA×12, NPWR×9, PUSA×5, ALVO×3, CAPR×2, ALIT×1, ZURA×3, SAFX×52, ANF×8, BHVN×74, BZ×68, LVWR×913, SEDG×37, SMTC×8, GRRR×79 | — | — | $167.51 | $8,253.25 | $8,420.76 | BMEA×12, NPWR×9, PUSA×5, ALVO×3, CAPR×2, ALIT×1, ZURA×3, SAFX×52, ANF×8, BHVN×74, BZ×68, LVWR×913, SEDG×37, SMTC×8, GRRR×79 | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $167.51 | BMEA×12, NPWR×9, PUSA×5, ALVO×3, CAPR×2, ALIT×1, ZURA×3, SAFX×52, ANF×8, BHVN×74, BZ×68, LVWR×913, SEDG×37, SMTC×8, GRRR×79 | — | BMEA, NPWR, PUSA, ALVO, CAPR, ALIT, ZURA, SAFX | $308.47 | $7,914.00 | $8,222.47 | ANF×8, BHVN×74, BZ×68, LVWR×913, SEDG×37, SMTC×8, GRRR×79 | SELL BMEA (dropped from list after 5 sess (min 5)); SELL NPWR (dropped from list after 5 sess (min 5)); SELL PUSA (dropped from list after 5 sess (min 5)); SELL ALVO (dropped from list after 5 sess (min 5)); SELL CAPR (dropped from list after 5 sess (min 5)); SELL ALIT (dropped from list after 5 sess (min 5)); SELL ZURA (dropped from list after 5 sess (min 5)); SELL SAFX (dropped from list after 5 sess (min 5)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $308.47 | ANF×8, BHVN×74, BZ×68, LVWR×913, SEDG×37, SMTC×8, GRRR×79 | — | — | $308.47 | $7,897.82 | $8,206.29 | ANF×8, BHVN×74, BZ×68, LVWR×913, SEDG×37, SMTC×8, GRRR×79 | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $308.47 | ANF×8, BHVN×74, BZ×68, LVWR×913, SEDG×37, SMTC×8, GRRR×79 | GPRO, FRVO, CRK, MMED, CTMX, SLN, CRDL | — | $73.83 | $8,184.66 | $8,258.49 | ANF×8, BHVN×74, BZ×68, LVWR×913, SEDG×37, SMTC×8, GRRR×79, GPRO×31, FRVO×2, CRK×2, MMED×1, CTMX×10, SLN×2, CRDL×17 | BUY GPRO x31 @ 1.22; BUY FRVO x2 @ 18.40; BUY CRK x2 @ 15.70; BUY MMED x1 @ 22.78; BUY CTMX x10 @ 3.72; BUY SLN x2 @ 14.70; BUY CRDL x17 @ 2.16 |
| 2026-09-04 | — | $73.83 | ANF×8, BHVN×74, BZ×68, LVWR×913, SEDG×37, SMTC×8, GRRR×79, GPRO×31, FRVO×2, CRK×2, MMED×1, CTMX×10, SLN×2, CRDL×17 | BAK, EOSE, SLBT, DELL, MLYS, CCOI, SION | ANF, BHVN, BZ, LVWR, SEDG, SMTC, GRRR | $159.52 | $7,935.05 | $8,094.57 | GPRO×31, FRVO×2, CRK×2, MMED×1, CTMX×10, SLN×2, CRDL×17, BAK×584, EOSE×318, SLBT×370, DELL×2, MLYS×39, CCOI×111, SION×155 | SELL ANF (dropped from list after 5 sess (min 5)); SELL BHVN (dropped from list after 5 sess (min 5)); SELL BZ (dropped from list after 5 sess (min 5)); SELL LVWR (dropped from list after 5 sess (min 5)); SELL SEDG (dropped from list after 5 sess (min 5)); SELL SMTC (dropped from list after 5 sess (min 5)); SELL GRRR (dropped from list after 5 sess (min 5)); BUY BAK x584 @ 1.95; BUY EOSE x318 @ 3.57; BUY SLBT x370 @ 3.07; BUY DELL x2 @ 486.31; BUY MLYS x39 @ 29.15; BUY CCOI x111 @ 10.22; BUY SION x155 @ 7.31 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $8,746.36 | ▼ $9,996.26 (-3.74) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WWW` | 60 | $20.60 | $2.17 | — | $7,508.19 | ▼ $9,994.09 (-5.91) | baseline list, no extra gate; list probable,yday_gainer; ret5=+4.4; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $6,254.51 | ▼ $9,990.23 (-9.77) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $5,245.52 | ▼ $9,988.24 (-11.76) | baseline list, no extra gate; list probable; 🔵; ⚪; ret5=+7.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `FOSL` | 221 | $5.64 | $2.85 | — | $3,996.23 | ▼ $9,985.38 (-14.62) | baseline list, no extra gate; list probable; 🔵; ret5=-4.1; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $2,756.51 | ▼ $9,983.17 (-16.83) | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRS` | 370 | $3.37 | $4.77 | — | $1,504.84 | ▼ $9,978.40 (-21.60) | baseline list, no extra gate; list probable; ret5=-29.1; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ALGM` | 28 | $44.06 | $2.07 | — | $269.08 | ▼ $9,976.32 (-23.68) | baseline list, no extra gate; list probable; 🔵; ret5=+3.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 3 | $9.12 | $0.28 | — | $241.44 | ▲ $10,058.92 (+58.92) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $33.64 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `FCEL` | 1 | $22.37 | $0.23 | — | $218.84 | ▲ $10,058.69 (+58.69) | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=+9.5; leftover $33.64 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 1 | $31.30 | $0.32 | — | $187.23 | ▲ $10,058.38 (+58.38) | baseline list, no extra gate; list probable,yday_gainer; ret5=-3.8; leftover $33.64 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `BW` | 3 | $10.35 | $0.32 | — | $155.86 | ▲ $10,058.06 (+58.06) | baseline list, no extra gate; list probable; ⚪; ret5=+9.8; leftover $33.64 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 1 | $18.24 | $0.19 | — | $137.43 | ▲ $10,057.87 (+57.87) | baseline list, no extra gate; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $33.64 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 2 | $16.20 | $0.33 | — | $104.70 | ▲ $10,057.54 (+57.54) | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $33.64 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 1 | $7.38 | $0.08 | — | $97.25 | ▼ $9,303.89 (-696.11) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $13.09 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 1 | $7.45 | $0.08 | — | $89.72 | ▼ $9,303.81 (-696.19) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $13.09 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 1 | $10.77 | $0.11 | — | $78.84 | ▼ $9,303.70 (-696.30) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $13.09 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 1 | $9.46 | $0.10 | — | $69.28 | ▼ $9,303.61 (-696.39) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $13.09 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NG` | 1 | $8.38 | $0.09 | — | $60.81 | ▼ $9,303.52 (-696.48) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+7.5; leftover $13.09 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ANGX` | 290 | $4.43 | $3.80 | $+27.26 | $1,341.71 | ▼ $9,242.49 (-757.51) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `WWW` | 60 | $20.32 | $2.19 | $-21.16 | $2,558.72 | ▼ $9,240.30 (-759.70) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `HYLN` | 299 | $3.42 | $3.92 | $-235.01 | $3,577.39 | ▼ $9,236.39 (-763.61) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `WDC` | 2 | $477.27 | $2.02 | $-56.47 | $4,529.91 | ▼ $9,234.37 (-765.63) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `FOSL` | 221 | $5.65 | $2.90 | $-3.54 | $5,775.66 | ▼ $9,231.47 (-768.53) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ADUR` | 75 | $16.00 | $2.24 | $-41.95 | $6,973.43 | ▼ $9,229.24 (-770.76) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `AIRS` | 370 | $2.71 | $4.84 | $-253.82 | $7,971.28 | ▼ $9,224.39 (-775.61) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ALGM` | 28 | $37.62 | $2.09 | $-184.35 | $9,022.69 | ▼ $9,222.30 (-777.70) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 776 | $1.66 | $10.01 | — | $7,724.52 | ▼ $9,212.29 (-787.71) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1288.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 753 | $1.71 | $9.71 | — | $6,427.17 | ▼ $9,202.57 (-797.43) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $1288.96 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $5,178.66 | ▼ $9,200.58 (-799.42) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1288.96 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 86 | $14.96 | $2.25 | — | $3,889.85 | ▼ $9,198.33 (-801.67) | baseline list, no extra gate; list probable,yday_gainer; ret5=-1.6; leftover $1288.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1491 | $0.86 | $17.36 | — | $2,584.27 | ▼ $9,180.97 (-819.03) | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1288.96 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 414 | $3.11 | $5.34 | — | $1,291.39 | ▼ $9,175.63 (-824.37) | baseline list, no extra gate; list probable,yday_gainer; ret5=+7.1; leftover $1288.96 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QTRX` | 413 | $3.11 | $5.33 | — | $1.63 | ▼ $9,170.31 (-829.69) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $1288.96 | join🟡 sector🟢 gen🟢 news🔴 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ABX` | 3 | $9.90 | $0.33 | $+1.73 | $31.01 | ▼ $9,170.59 (-829.41) | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `FCEL` | 1 | $18.51 | $0.21 | $-4.29 | $49.31 | ▼ $9,170.38 (-829.62) | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `VERA` | 1 | $32.25 | $0.35 | $+0.29 | $81.21 | ▼ $9,170.03 (-829.97) | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `BW` | 3 | $8.14 | $0.27 | $-7.22 | $105.36 | ▼ $9,169.76 (-830.24) | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `OCC` | 1 | $13.60 | $0.16 | $-4.98 | $118.80 | ▼ $9,169.60 (-830.40) | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `ALM` | 2 | $18.69 | $0.40 | $+4.25 | $155.78 | ▼ $9,169.20 (-830.80) | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 12 | $1.62 | $0.23 | — | $136.11 | ▼ $9,074.15 (-925.85) | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $19.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 9 | $2.00 | $0.21 | — | $117.90 | ▼ $9,073.94 (-926.06) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $19.47 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 5 | $3.70 | $0.20 | — | $99.20 | ▼ $9,073.74 (-926.26) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $19.47 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 3 | $5.22 | $0.17 | — | $83.38 | ▼ $9,073.58 (-926.42) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $19.47 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 2 | $6.79 | $0.14 | — | $69.66 | ▼ $9,073.44 (-926.56) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $19.47 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 1 | $14.86 | $0.15 | — | $54.64 | ▼ $9,073.28 (-926.72) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $19.47 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 3 | $6.38 | $0.20 | — | $35.30 | ▼ $9,073.08 (-926.92) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $19.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 52 | $0.37 | $0.35 | — | $15.72 | ▼ $9,072.74 (-927.26) | baseline list, no extra gate; list probable,yday_gainer; ret5=-26.5; leftover $19.47 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `MRVI` | 1 | $8.85 | $0.11 | $+1.28 | $24.45 | ▼ $9,101.37 (-898.63) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `DNA` | 1 | $7.33 | $0.10 | $-0.29 | $31.69 | ▼ $9,101.28 (-898.72) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `EXK` | 1 | $10.82 | $0.13 | $-0.19 | $42.38 | ▼ $9,101.15 (-898.85) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `SCZM` | 1 | $9.61 | $0.12 | $-0.07 | $51.87 | ▼ $9,101.03 (-898.97) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NG` | 1 | $9.55 | $0.12 | $+0.96 | $61.30 | ▼ $9,100.91 (-899.09) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `BTBT` | 776 | $1.59 | $10.15 | $-74.48 | $1,284.99 | ▼ $9,010.68 (-989.32) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ENHA` | 753 | $1.64 | $9.85 | $-72.27 | $2,510.06 | ▼ $9,000.83 (-999.17) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `DE` | 2 | $628.82 | $2.02 | $+7.11 | $3,765.68 | ▼ $8,998.81 (-1,001.19) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `QDEL` | 86 | $14.92 | $2.27 | $-7.96 | $5,046.53 | ▼ $8,996.54 (-1,003.46) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ORBS` | 1491 | $0.82 | $16.96 | $-99.92 | $6,252.20 | ▼ $8,979.59 (-1,020.41) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `GORO` | 414 | $3.59 | $5.42 | $+187.96 | $7,733.03 | ▼ $8,974.16 (-1,025.84) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `QTRX` | 413 | $2.66 | $5.41 | $-196.58 | $8,826.21 | ▼ $8,968.76 (-1,031.24) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $144.70 | $2.01 | — | $7,666.59 | ▼ $8,966.74 (-1,033.26) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1260.89 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 74 | $16.95 | $2.21 | — | $6,410.08 | ▼ $8,964.53 (-1,035.47) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1260.89 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 68 | $18.50 | $2.19 | — | $5,149.89 | ▼ $8,962.34 (-1,037.66) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1260.89 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 913 | $1.38 | $11.78 | — | $3,878.17 | ▼ $8,950.56 (-1,049.44) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $1260.89 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 37 | $33.78 | $2.10 | — | $2,626.21 | ▼ $8,948.46 (-1,051.54) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1260.89 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $1,429.00 | ▼ $8,946.45 (-1,053.55) | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1260.89 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 79 | $15.94 | $2.23 | — | $167.51 | ▼ $8,944.22 (-1,055.78) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1260.89 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `BMEA` | 12 | $1.65 | $0.25 | $-0.12 | $187.05 | ▼ $8,322.43 (-1,677.57) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `NPWR` | 9 | $1.78 | $0.21 | $-2.39 | $202.87 | ▼ $8,322.23 (-1,677.77) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `PUSA` | 5 | $3.93 | $0.23 | $+0.72 | $222.29 | ▼ $8,322.00 (-1,678.00) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `ALVO` | 3 | $5.24 | $0.19 | $-0.29 | $237.82 | ▼ $8,321.81 (-1,678.19) | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `CAPR` | 2 | $10.43 | $0.23 | $+6.90 | $258.44 | ▼ $8,321.57 (-1,678.43) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `ALIT` | 1 | $14.72 | $0.17 | $-0.46 | $272.99 | ▼ $8,321.40 (-1,678.60) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `ZURA` | 3 | $5.60 | $0.20 | $-2.74 | $289.60 | ▼ $8,321.21 (-1,678.79) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `SAFX` | 52 | $0.37 | $0.37 | $-0.72 | $308.47 | ▼ $8,320.84 (-1,679.16) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 31 | $1.22 | $0.47 | — | $270.18 | ▼ $8,238.61 (-1,761.39) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $38.56 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 2 | $18.40 | $0.37 | — | $233.00 | ▼ $8,238.23 (-1,761.77) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $38.56 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 2 | $15.70 | $0.32 | — | $201.28 | ▼ $8,237.91 (-1,762.09) | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $38.56 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 1 | $22.78 | $0.23 | — | $178.27 | ▼ $8,237.68 (-1,762.32) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $38.56 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 10 | $3.72 | $0.40 | — | $140.67 | ▼ $8,237.28 (-1,762.72) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $38.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 2 | $14.70 | $0.30 | — | $110.97 | ▼ $8,236.98 (-1,763.02) | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $38.56 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 17 | $2.16 | $0.42 | — | $73.83 | ▼ $8,236.56 (-1,763.44) | baseline list, no extra gate; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $38.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `ANF` | 8 | $137.70 | $2.03 | $-60.05 | $1,173.40 | ▼ $8,245.06 (-1,754.94) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BHVN` | 74 | $15.89 | $2.23 | $-82.89 | $2,347.02 | ▼ $8,242.82 (-1,757.18) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BZ` | 68 | $17.31 | $2.22 | $-85.33 | $3,521.89 | ▼ $8,240.61 (-1,759.39) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `LVWR` | 913 | $1.17 | $11.94 | $-215.45 | $4,578.16 | ▼ $8,228.67 (-1,771.33) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SEDG` | 37 | $33.69 | $2.12 | $-7.55 | $5,822.57 | ▼ $8,226.55 (-1,773.45) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SMTC` | 8 | $133.10 | $2.03 | $-134.45 | $6,885.34 | ▼ $8,224.52 (-1,775.48) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `GRRR` | 79 | $13.78 | $2.25 | $-175.12 | $7,971.71 | ▼ $8,222.27 (-1,777.73) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 584 | $1.95 | $7.53 | — | $6,825.37 | ▼ $8,214.73 (-1,785.27) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1138.82 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 318 | $3.57 | $4.10 | — | $5,686.01 | ▼ $8,210.63 (-1,789.37) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1138.82 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 370 | $3.07 | $4.77 | — | $4,545.34 | ▼ $8,205.86 (-1,794.14) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1138.82 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $3,570.72 | ▼ $8,203.86 (-1,796.14) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1138.82 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 39 | $29.15 | $2.11 | — | $2,431.76 | ▼ $8,201.75 (-1,798.25) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1138.82 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 111 | $10.22 | $2.32 | — | $1,295.02 | ▼ $8,199.43 (-1,800.57) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1138.82 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SION` | 155 | $7.31 | $2.46 | — | $159.52 | ▼ $8,196.98 (-1,803.02) | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $1138.82 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `WWW` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `WDC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `FOSL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ADUR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `AIRS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ALGM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `CDNL` | cash | leftover split 33.64 < 1 share @ 39.85 |
| 2026-08-17 | `CELC` | cash | leftover split 33.64 < 1 share @ 92.99 |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `WWW` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `WDC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `FOSL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `AIRS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ALGM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ABX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `FCEL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `VERA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `BW` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `OCC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `ALM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CBRS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COHR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TDTH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PGEN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `ANGX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `WWW` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `HYLN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `WDC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `FOSL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ADUR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `AIRS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ALGM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ABX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `FCEL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `VERA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `BW` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `OCC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `ALM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `ANGX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `WWW` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `HYLN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `WDC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `FOSL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ADUR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `AIRS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ALGM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ABX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `FCEL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `VERA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `BW` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `OCC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `ALM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `MSTR` | cash | leftover split 13.09 < 1 share @ 113.23 |
| 2026-08-20 | `BLSH` | cash | leftover split 13.09 < 1 share @ 29.20 |
| 2026-08-20 | `HYMC` | cash | leftover split 13.09 < 1 share @ 27.25 |
| 2026-08-21 | `ABX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `FCEL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `VERA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `BW` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `OCC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `ALM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `DNA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `EXK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `SCZM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `MRVI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `DNA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `EXK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `SCZM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ENHA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `DE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `QDEL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `GORO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `QTRX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `MRVI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `DNA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `EXK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `SCZM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `NG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ENHA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `DE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `QDEL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `GORO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `QTRX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-26 | `MRVI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `DNA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `EXK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `SCZM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ENHA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `DE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `QDEL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ORBS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `GORO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `QTRX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `PUSA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ALIT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `SAFX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-26 | `FLNC` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `AVEX` | no_price | no 09:30 open |
| 2026-08-26 | `ITG` | no_price | no 09:30 open |
| 2026-08-26 | `BE` | no_price | no 09:30 open |
| 2026-08-27 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ENHA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `DE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `QDEL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ORBS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `GORO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `QTRX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `PUSA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `ALIT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `SAFX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `NPWR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `PUSA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ALVO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ALIT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ZURA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `SAFX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `NPWR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `PUSA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ALVO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ALIT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ZURA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `SAFX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `LVWR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `LVWR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AME` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NAGE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `ANF` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `BHVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `BZ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `LVWR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `SEDG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `SMTC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `GRRR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BMO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `ANF` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `BHVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `BZ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `LVWR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `SEDG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `SMTC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `GRRR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `EIX` | cash | leftover split 38.56 < 1 share @ 56.78 |
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CTMX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GPRO` | 31 | 2026-09-03 @ $1.22 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $38.56 |
| `FRVO` | 2 | 2026-09-03 @ $18.40 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $38.56 |
| `CRK` | 2 | 2026-09-03 @ $15.70 | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $38.56 |
| `MMED` | 1 | 2026-09-03 @ $22.78 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $38.56 |
| `CTMX` | 10 | 2026-09-03 @ $3.72 | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $38.56 |
| `SLN` | 2 | 2026-09-03 @ $14.70 | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $38.56 |
| `CRDL` | 17 | 2026-09-03 @ $2.16 | baseline list, no extra gate; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $38.56 |
| `BAK` | 584 | 2026-09-04 @ $1.95 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1138.82 |
| `EOSE` | 318 | 2026-09-04 @ $3.57 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1138.82 |
| `SLBT` | 370 | 2026-09-04 @ $3.07 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1138.82 |
| `DELL` | 2 | 2026-09-04 @ $486.31 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1138.82 |
| `MLYS` | 39 | 2026-09-04 @ $29.15 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1138.82 |
| `CCOI` | 111 | 2026-09-04 @ $10.22 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1138.82 |
| `SION` | 155 | 2026-09-04 @ $7.31 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $1138.82 |
