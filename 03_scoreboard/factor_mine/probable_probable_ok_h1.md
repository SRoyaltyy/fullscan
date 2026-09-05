# Factor mine action — `probable_probable_ok_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `probable` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-1.69%** ($9,831) · signal-only (no cash/fees) was +1.91%. Starts YES **7/17**. Fills 72 · skips 32 · realized $-613.49.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `probable` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_green=True,ret_5_max=10.0` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $116.96.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | ANGX, HYLN, WDC, ADUR, ALGM | — | $493.79 | $9,942.67 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $493.79 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45 | $10,107.31 | +164.64 | CDNL, ABX, VERA, CELC, OCC, ALM | ANGX, HYLN, WDC, ADUR, ALGM | $43.81 | $9,969.98 | CDNL×42, ABX×184, VERA×53, CELC×18, OCC×92, ALM×103 | 09:30 open · cash $493.79 (unchanged overnight, no fees) · equity $10,107.31 vs prior close $9,942.67 (+164.64) because holdings re-marked: ANGX×464 yday $4.37 → 09:30 $4.60 +106.72; HYLN×478 yday $4.06 → 09:30 $4.10 +19.12; WDC×3 yday $508.80 → 09:30 $525.53 +50.19; ADUR×121 yday $16.17 → 09:30 $15.73 -53.24; ALGM×45 yday $44.39 → 09:30 $45.32 +41.85 |
| 2026-08-18 | -6.20 | $43.81 | CDNL×42, ABX×184, VERA×53, CELC×18, OCC×92, ALM×103 | $9,889.28 | -80.70 | — | CDNL, ABX, VERA, CELC, OCC, ALM | $9,875.70 | $9,875.70 | — | 09:30 open · cash $43.81 (unchanged overnight, no fees) · equity $9,889.28 vs prior close $9,969.98 (-80.70) because holdings re-marked: CDNL×42 yday $39.23 → 09:30 $41.57 +98.28; ABX×184 yday $9.12 → 09:30 $9.03 -16.56; VERA×53 yday $31.63 → 09:30 $31.31 -16.96; CELC×18 yday $92.44 → 09:30 $92.38 -1.08; OCC×92 yday $17.12 → 09:30 $16.20 -84.64; ALM×103 yday $16.36 → 09:30 $15.78 -59.74 |
| 2026-08-19 | -7.20 | $9,875.70 | — | $9,875.70 | -0.00 | — | — | $9,875.70 | $9,875.70 | — | 09:30 open · cash $9,875.70 · no holdings · equity $9,875.70 vs prior close $9,875.70 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-20 | +1.12 | $9,875.70 | — | $9,875.70 | -0.00 | DNA, MSTR, EXK, SCZM, NG, BLSH, HYMC | — | $83.88 | $9,781.48 | DNA×189, MSTR×12, EXK×130, SCZM×149, NG×168, BLSH×48, HYMC×51 | 09:30 open · cash $9,875.70 · no holdings · equity $9,875.70 vs prior close $9,875.70 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $83.88 | DNA×189, MSTR×12, EXK×130, SCZM×149, NG×168, BLSH×48, HYMC×51 | $10,203.87 | +422.39 | BTBT, DE, QDEL, ORBS, GORO | DNA, MSTR, EXK, SCZM, NG, BLSH, HYMC | $115.84 | $10,104.69 | BTBT×1227, DE×3, QDEL×136, ORBS×2358, GORO×655 | 09:30 open · cash $83.88 (unchanged overnight, no fees) · equity $10,203.87 vs prior close $9,781.48 (+422.39) because holdings re-marked: DNA×189 yday $6.96 → 09:30 $7.09 +24.57; MSTR×12 yday $112.39 → 09:30 $119.69 +87.60; EXK×130 yday $10.97 → 09:30 $11.34 +48.10; SCZM×149 yday $9.76 → 09:30 $10.26 +74.50; NG×168 yday $8.66 → 09:30 $9.02 +60.48; BLSH×48 yday $28.44 → 09:30 $29.75 +62.88; HYMC×51 yday $26.14 → 09:30 $27.40 +64.26 |
| 2026-08-24 | -5.17 | $115.84 | BTBT×1227, DE×3, QDEL×136, ORBS×2358, GORO×655 | $10,173.73 | +69.04 | — | BTBT, DE, QDEL, ORBS, GORO | $10,116.18 | $10,116.18 | — | 09:30 open · cash $115.84 (unchanged overnight, no fees) · equity $10,173.73 vs prior close $10,104.69 (+69.04) because holdings re-marked: BTBT×1227 yday $1.53 → 09:30 $1.55 +24.54; DE×3 yday $647.47 → 09:30 $653.62 +18.45; QDEL×136 yday $14.74 → 09:30 $14.71 -4.08; ORBS×2358 yday $0.88 → 09:30 $0.89 +23.58; GORO×655 yday $3.19 → 09:30 $3.20 +6.55 |
| 2026-08-25 | +1.80 | $10,116.18 | — | $10,116.18 | +0.00 | NPWR, ALVO, ALIT, ZURA | — | $4.44 | $10,174.62 | NPWR×1264, ALVO×484, ALIT×170, ZURA×392 | 09:30 open · cash $10,116.18 · no holdings · equity $10,116.18 vs prior close $10,116.18 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-26 | +2.02 | $4.44 | NPWR×1264, ALVO×484, ALIT×170, ZURA×392 | $10,174.62 | -0.00 | — | — | $4.44 | $10,086.08 | NPWR×1264, ALVO×484, ALIT×170, ZURA×392 | 09:30 open · cash $4.44 (unchanged overnight, no fees) · equity $10,174.62 vs prior close $10,174.62 (-0.00) because holdings re-marked: NPWR×1264 yday $2.02 → 09:30 $2.02 +0.00; ALVO×484 yday $5.25 → 09:30 $5.25 +0.00; ALIT×170 yday $14.87 → 09:30 $14.87 +0.00; ZURA×392 yday $6.50 → 09:30 $6.50 +0.00 |
| 2026-08-27 | — | $4.44 | NPWR×1264, ALVO×484, ALIT×170, ZURA×392 | $9,781.74 | -304.34 | — | NPWR, ALVO, ALIT, ZURA | $9,751.17 | $9,751.17 | — | 09:30 open · cash $4.44 (unchanged overnight, no fees) · equity $9,781.74 vs prior close $10,086.08 (-304.34) because holdings re-marked: NPWR×1264 yday $2.02 → 09:30 $1.93 -113.76; ALVO×484 yday $5.25 → 09:30 $4.98 -130.68; ALIT×170 yday $14.87 → 09:30 $14.85 -3.40; ZURA×392 yday $6.50 → 09:30 $6.13 -145.04 |
| 2026-08-28 | +0.75 | $9,751.17 | — | $9,751.17 | -0.00 | ANF, BHVN, BZ, LVWR, GRRR | — | $56.44 | $9,527.19 | ANF×13, BHVN×115, BZ×105, LVWR×1413, GRRR×122 | 09:30 open · cash $9,751.17 · no holdings · equity $9,751.17 vs prior close $9,751.17 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-31 | -5.85 | $56.44 | ANF×13, BHVN×115, BZ×105, LVWR×1413, GRRR×122 | $9,326.05 | -201.14 | — | ANF, BHVN, BZ, LVWR, GRRR | $9,298.43 | $9,298.43 | — | 09:30 open · cash $56.44 (unchanged overnight, no fees) · equity $9,326.05 vs prior close $9,527.19 (-201.14) because holdings re-marked: ANF×13 yday $145.75 → 09:30 $148.67 +37.96; BHVN×115 yday $16.12 → 09:30 $15.44 -78.20; BZ×105 yday $18.00 → 09:30 $17.89 -11.55; LVWR×1413 yday $1.36 → 09:30 $1.37 +14.13; GRRR×122 yday $15.66 → 09:30 $14.32 -163.48 |
| 2026-09-01 | -6.30 | $9,298.43 | — | $9,298.43 | -0.00 | — | — | $9,298.43 | $9,298.43 | — | 09:30 open · cash $9,298.43 · no holdings · equity $9,298.43 vs prior close $9,298.43 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-02 | -3.83 | $9,298.43 | — | $9,298.43 | -0.00 | — | — | $9,298.43 | $9,298.43 | — | 09:30 open · cash $9,298.43 · no holdings · equity $9,298.43 vs prior close $9,298.43 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-03 | -0.90 | $9,298.43 | — | $9,298.43 | -0.00 | GPRO, CRK, MMED | — | $16.47 | $10,554.29 | GPRO×2540, CRK×197, MMED×134 | 09:30 open · cash $9,298.43 · no holdings · equity $9,298.43 vs prior close $9,298.43 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $16.47 | GPRO×2540, CRK×197, MMED×134 | $10,781.24 | +226.95 | BAK, EOSE, DELL | CRK, MMED | $116.96 | $9,830.98 | GPRO×2540, BAK×1069, EOSE×584, DELL×4 | 09:30 open · cash $16.47 (unchanged overnight, no fees) · equity $10,781.24 vs prior close $10,554.29 (+226.95) because holdings re-marked: GPRO×2540 yday $1.69 → 09:30 $1.78 +228.60; CRK×197 yday $15.54 → 09:30 $15.45 -17.73; MMED×134 yday $23.76 → 09:30 $23.88 +16.08 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 464 | $4.31 | $5.99 | — | $7,994.17 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 478 | $4.18 | $6.17 | — | $5,989.97 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 3 | $503.50 | $2.00 | — | $4,477.47 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ⚪; ret5=+7.9; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 121 | $16.50 | $2.35 | — | $2,478.62 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ALGM` | 45 | $44.06 | $2.12 | — | $493.79 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ret5=+3.9; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $493.79 | ▲ 09:30 equity $10,107.31 vs yday $9,942.67 (+164.64) | 09:30 open · cash $493.79 (unchanged overnight, no fees) · equity $10,107.31 vs prior close $9,942.67 (+164.64) because holdings re-marked: ANGX×464 yday $4.37 → 09:30 $4.60 +106.72; HYLN×478 yday $4.06 → 09:30 $4.10 +19.12; WDC×3 yday $508.80 → 09:30 $525.53 +50.19; ADUR×121 yday $16.17 → 09:30 $15.73 -53.24; ALGM×45 yday $44.39 → 09:30 $45.32 +41.85 | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 464 | $4.60 | $6.08 | $+122.49 | $2,622.11 | ▲ +122.49 after sell → book $10,101.23; vs 09:30 mark -6.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 478 | $4.10 | $6.26 | $-50.67 | $4,575.65 | ▼ -50.67 after sell → book $10,094.97; vs 09:30 mark -6.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 3 | $525.53 | $2.02 | $+62.07 | $6,150.22 | ▲ +62.07 after sell → book $10,092.95; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 121 | $15.73 | $2.39 | $-97.91 | $8,051.16 | ▼ -97.91 after sell → book $10,090.56; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ALGM` | 45 | $45.32 | $2.15 | $+52.42 | $10,088.41 | ▲ +52.42 after sell → book $10,088.41; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 42 | $39.85 | $2.12 | — | $8,412.59 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1681.40 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 184 | $9.12 | $2.54 | — | $6,731.97 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1681.40 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 53 | $31.30 | $2.15 | — | $5,070.92 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-3.8; leftover $1681.40 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 18 | $92.99 | $2.04 | — | $3,395.06 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-0.8; leftover $1681.40 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 92 | $18.24 | $2.27 | — | $1,714.71 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1681.40 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 103 | $16.20 | $2.30 | — | $43.81 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1681.40 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.81 | ▼ 09:30 equity $9,889.28 vs yday $9,969.98 (-80.70) | 09:30 open · cash $43.81 (unchanged overnight, no fees) · equity $9,889.28 vs prior close $9,969.98 (-80.70) because holdings re-marked: CDNL×42 yday $39.23 → 09:30 $41.57 +98.28; ABX×184 yday $9.12 → 09:30 $9.03 -16.56; VERA×53 yday $31.63 → 09:30 $31.31 -16.96; CELC×18 yday $92.44 → 09:30 $92.38 -1.08; OCC×92 yday $17.12 → 09:30 $16.20 -84.64; ALM×103 yday $16.36 → 09:30 $15.78 -59.74 | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 42 | $41.57 | $2.14 | $+67.98 | $1,787.61 | ▲ +67.98 after sell → book $9,887.14; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 184 | $9.03 | $2.59 | $-21.69 | $3,446.55 | ▼ -21.69 after sell → book $9,884.56; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 53 | $31.31 | $2.17 | $-3.79 | $5,103.81 | ▼ -3.79 after sell → book $9,882.39; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 18 | $92.38 | $2.07 | $-15.09 | $6,764.58 | ▼ -15.09 after sell → book $9,880.32; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 92 | $16.20 | $2.29 | $-192.24 | $8,252.68 | ▼ -192.24 after sell → book $9,878.02; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 103 | $15.78 | $2.33 | $-47.89 | $9,875.70 | ▼ -47.89 after sell → book $9,875.70; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,875.70 | ▲ 09:30 equity $9,875.70 vs yday $9,875.70 (-0.00) | 09:30 open · cash $9,875.70 · no holdings · equity $9,875.70 vs prior close $9,875.70 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,875.70 | ▲ 09:30 equity $9,875.70 vs yday $9,875.70 (-0.00) | 09:30 open · cash $9,875.70 · no holdings · equity $9,875.70 vs prior close $9,875.70 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 189 | $7.45 | $2.56 | — | $8,465.09 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1410.81 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MSTR` | 12 | $113.23 | $2.03 | — | $7,104.30 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1410.81 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 130 | $10.77 | $2.38 | — | $5,701.82 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1410.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 149 | $9.46 | $2.44 | — | $4,289.85 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1410.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NG` | 168 | $8.38 | $2.49 | — | $2,879.51 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+7.5; leftover $1410.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 48 | $29.20 | $2.13 | — | $1,475.78 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1410.81 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HYMC` | 51 | $27.25 | $2.14 | — | $83.88 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ret5=+1.6; leftover $1410.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.88 | ▲ 09:30 equity $10,203.87 vs yday $9,781.48 (+422.39) | 09:30 open · cash $83.88 (unchanged overnight, no fees) · equity $10,203.87 vs prior close $9,781.48 (+422.39) because holdings re-marked: DNA×189 yday $6.96 → 09:30 $7.09 +24.57; MSTR×12 yday $112.39 → 09:30 $119.69 +87.60; EXK×130 yday $10.97 → 09:30 $11.34 +48.10; SCZM×149 yday $9.76 → 09:30 $10.26 +74.50; NG×168 yday $8.66 → 09:30 $9.02 +60.48; BLSH×48 yday $28.44 → 09:30 $29.75 +62.88; HYMC×51 yday $26.14 → 09:30 $27.40 +64.26 | — |
| 2026-08-21 09:30 ET | **SELL** | `DNA` | 189 | $7.09 | $2.60 | $-73.20 | $1,421.30 | ▼ -73.20 after sell → book $10,201.28; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MSTR` | 12 | $119.69 | $2.05 | $+73.45 | $2,855.53 | ▲ +73.45 after sell → book $10,199.23; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `EXK` | 130 | $11.34 | $2.41 | $+69.31 | $4,327.31 | ▲ +69.31 after sell → book $10,196.81; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SCZM` | 149 | $10.26 | $2.47 | $+114.29 | $5,853.58 | ▲ +114.29 after sell → book $10,194.34; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NG` | 168 | $9.02 | $2.53 | $+102.49 | $7,366.41 | ▲ +102.49 after sell → book $10,191.81; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `BLSH` | 48 | $29.75 | $2.16 | $+22.11 | $8,792.25 | ▲ +22.11 after sell → book $10,189.65; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HYMC` | 51 | $27.40 | $2.16 | $+3.34 | $10,187.49 | ▲ +3.34 after sell → book $10,187.49; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 1227 | $1.66 | $15.83 | — | $8,134.84 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $2037.50 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 3 | $623.26 | $2.00 | — | $6,263.06 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $2037.50 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 136 | $14.96 | $2.40 | — | $4,226.10 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-1.6; leftover $2037.50 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 2358 | $0.86 | $27.45 | — | $2,161.34 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $2037.50 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 655 | $3.11 | $8.45 | — | $115.84 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=+7.1; leftover $2037.50 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $115.84 | ▲ 09:30 equity $10,173.73 vs yday $10,104.69 (+69.04) | 09:30 open · cash $115.84 (unchanged overnight, no fees) · equity $10,173.73 vs prior close $10,104.69 (+69.04) because holdings re-marked: BTBT×1227 yday $1.53 → 09:30 $1.55 +24.54; DE×3 yday $647.47 → 09:30 $653.62 +18.45; QDEL×136 yday $14.74 → 09:30 $14.71 -4.08; ORBS×2358 yday $0.88 → 09:30 $0.89 +23.58; GORO×655 yday $3.19 → 09:30 $3.20 +6.55 | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 1227 | $1.55 | $16.05 | $-166.85 | $2,001.65 | ▼ -166.85 after sell → book $10,157.69; vs 09:30 mark -16.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 3 | $653.62 | $2.02 | $+87.06 | $3,960.48 | ▲ +87.06 after sell → book $10,155.66; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QDEL` | 136 | $14.71 | $2.44 | $-38.83 | $5,958.60 | ▼ -38.83 after sell → book $10,153.22; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 2358 | $0.89 | $28.47 | $+5.39 | $8,028.76 | ▲ +5.39 after sell → book $10,124.76; vs 09:30 mark -28.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GORO` | 655 | $3.20 | $8.57 | $+41.93 | $10,116.18 | ▲ +41.93 after sell → book $10,116.18; vs 09:30 mark -8.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,116.18 | ▲ 09:30 equity $10,116.18 vs yday $10,116.18 (+0.00) | 09:30 open · cash $10,116.18 · no holdings · equity $10,116.18 vs prior close $10,116.18 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 1264 | $2.00 | $16.31 | — | $7,571.88 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $2529.05 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 484 | $5.22 | $6.24 | — | $5,039.15 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $2529.05 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 170 | $14.86 | $2.50 | — | $2,510.45 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $2529.05 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 392 | $6.38 | $5.06 | — | $4.44 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $2529.05 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.44 | ▲ 09:30 equity $10,174.62 vs yday $10,174.62 (-0.00) | 09:30 open · cash $4.44 (unchanged overnight, no fees) · equity $10,174.62 vs prior close $10,174.62 (-0.00) because holdings re-marked: NPWR×1264 yday $2.02 → 09:30 $2.02 +0.00; ALVO×484 yday $5.25 → 09:30 $5.25 +0.00; ALIT×170 yday $14.87 → 09:30 $14.87 +0.00; ZURA×392 yday $6.50 → 09:30 $6.50 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.44 | ▼ 09:30 equity $9,781.74 vs yday $10,086.08 (-304.34) | 09:30 open · cash $4.44 (unchanged overnight, no fees) · equity $9,781.74 vs prior close $10,086.08 (-304.34) because holdings re-marked: NPWR×1264 yday $2.02 → 09:30 $1.93 -113.76; ALVO×484 yday $5.25 → 09:30 $4.98 -130.68; ALIT×170 yday $14.87 → 09:30 $14.85 -3.40; ZURA×392 yday $6.50 → 09:30 $6.13 -145.04 | — |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 1264 | $1.93 | $16.53 | $-121.32 | $2,427.42 | ▼ -121.32 after sell → book $9,765.20; vs 09:30 mark -16.53 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALVO` | 484 | $4.98 | $6.34 | $-128.75 | $4,831.40 | ▼ -128.75 after sell → book $9,758.86; vs 09:30 mark -6.34 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALIT` | 170 | $14.85 | $2.55 | $-6.75 | $7,353.35 | ▼ -6.75 after sell → book $9,756.31; vs 09:30 mark -2.55 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ZURA` | 392 | $6.13 | $5.14 | $-108.20 | $9,751.17 | ▼ -108.20 after sell → book $9,751.17; vs 09:30 mark -5.14 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,751.17 | ▲ 09:30 equity $9,751.17 vs yday $9,751.17 (-0.00) | 09:30 open · cash $9,751.17 · no holdings · equity $9,751.17 vs prior close $9,751.17 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 13 | $144.70 | $2.03 | — | $7,868.04 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1950.23 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 115 | $16.95 | $2.33 | — | $5,916.45 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1950.23 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 105 | $18.50 | $2.31 | — | $3,971.65 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1950.23 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 1413 | $1.38 | $18.23 | — | $2,003.48 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $1950.23 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 122 | $15.94 | $2.36 | — | $56.44 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1950.23 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.44 | ▼ 09:30 equity $9,326.05 vs yday $9,527.19 (-201.14) | 09:30 open · cash $56.44 (unchanged overnight, no fees) · equity $9,326.05 vs prior close $9,527.19 (-201.14) because holdings re-marked: ANF×13 yday $145.75 → 09:30 $148.67 +37.96; BHVN×115 yday $16.12 → 09:30 $15.44 -78.20; BZ×105 yday $18.00 → 09:30 $17.89 -11.55; LVWR×1413 yday $1.36 → 09:30 $1.37 +14.13; GRRR×122 yday $15.66 → 09:30 $14.32 -163.48 | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 13 | $148.67 | $2.05 | $+47.53 | $1,987.10 | ▲ +47.53 after sell → book $9,324.00; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 115 | $15.44 | $2.37 | $-178.35 | $3,760.33 | ▼ -178.35 after sell → book $9,321.63; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 105 | $17.89 | $2.34 | $-68.69 | $5,636.44 | ▼ -68.69 after sell → book $9,319.29; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LVWR` | 1413 | $1.37 | $18.48 | $-50.84 | $7,553.78 | ▼ -50.84 after sell → book $9,300.82; vs 09:30 mark -18.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 122 | $14.32 | $2.39 | $-202.39 | $9,298.43 | ▼ -202.39 after sell → book $9,298.43; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,298.43 | ▲ 09:30 equity $9,298.43 vs yday $9,298.43 (-0.00) | 09:30 open · cash $9,298.43 · no holdings · equity $9,298.43 vs prior close $9,298.43 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,298.43 | ▲ 09:30 equity $9,298.43 vs yday $9,298.43 (-0.00) | 09:30 open · cash $9,298.43 · no holdings · equity $9,298.43 vs prior close $9,298.43 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,298.43 | ▲ 09:30 equity $9,298.43 vs yday $9,298.43 (-0.00) | 09:30 open · cash $9,298.43 · no holdings · equity $9,298.43 vs prior close $9,298.43 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 2540 | $1.22 | $32.77 | — | $6,166.86 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $3099.48 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 197 | $15.70 | $2.58 | — | $3,071.38 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $3099.48 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 134 | $22.78 | $2.39 | — | $16.47 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $3099.48 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.47 | ▲ 09:30 equity $10,781.24 vs yday $10,554.29 (+226.95) | 09:30 open · cash $16.47 (unchanged overnight, no fees) · equity $10,781.24 vs prior close $10,554.29 (+226.95) because holdings re-marked: GPRO×2540 yday $1.69 → 09:30 $1.78 +228.60; CRK×197 yday $15.54 → 09:30 $15.45 -17.73; MMED×134 yday $23.76 → 09:30 $23.88 +16.08 | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 197 | $15.45 | $2.64 | $-54.47 | $3,057.48 | ▼ -54.47 after sell → book $10,778.60; vs 09:30 mark -2.64 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 134 | $23.88 | $2.44 | $+142.57 | $6,254.96 | ▲ +142.57 after sell → book $10,776.16; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 1069 | $1.95 | $13.79 | — | $4,156.62 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $2084.99 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 584 | $3.57 | $7.53 | — | $2,064.21 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $2084.99 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 4 | $486.31 | $2.00 | — | $116.96 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $2084.99 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `PGEN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `NPWR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALVO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALIT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ZURA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `ITG` | no_price | no 09:30 open |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NAGE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GPRO` | 2540 | 2026-09-03 @ $1.22 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $3099.48 |
| `BAK` | 1069 | 2026-09-04 @ $1.95 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $2084.99 |
| `EOSE` | 584 | 2026-09-04 @ $3.57 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $2084.99 |
| `DELL` | 4 | 2026-09-04 @ $486.31 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $2084.99 |
