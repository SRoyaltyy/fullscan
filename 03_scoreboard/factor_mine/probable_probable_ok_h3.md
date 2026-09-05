# Factor mine action — `probable_probable_ok_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `probable` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-11.54%** ($8,846) · signal-only (no cash/fees) was -8.12%. Starts YES **4/17**. Fills 63 · skips 95 · realized $-1507.28.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `probable` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_green=True,ret_5_max=10.0` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9.23.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | ANGX, HYLN, WDC, ADUR, ALGM | — | $493.79 | $9,942.67 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $493.79 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45 | $10,107.31 | +164.64 | CDNL, ABX, VERA, OCC, ALM | — | $111.60 | $10,143.27 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45, CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 | 09:30 open · cash $493.79 (unchanged overnight, no fees) · equity $10,107.31 vs prior close $9,942.67 (+164.64) because holdings re-marked: ANGX×464 yday $4.37 → 09:30 $4.60 +106.72; HYLN×478 yday $4.06 → 09:30 $4.10 +19.12; WDC×3 yday $508.80 → 09:30 $525.53 +50.19; ADUR×121 yday $16.17 → 09:30 $15.73 -53.24; ALGM×45 yday $44.39 → 09:30 $45.32 +41.85 |
| 2026-08-18 | -6.20 | $111.60 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45, CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 | $9,860.11 | -283.16 | — | — | $111.60 | $9,738.07 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45, CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 | 09:30 open · cash $111.60 (unchanged overnight, no fees) · equity $9,860.11 vs prior close $10,143.27 (-283.16) because holdings re-marked: ANGX×464 yday $4.71 → 09:30 $4.79 +37.12; HYLN×478 yday $4.09 → 09:30 $3.95 -66.92; WDC×3 yday $536.01 → 09:30 $496.07 -119.82; ADUR×121 yday $15.85 → 09:30 $15.41 -53.24; ALGM×45 yday $44.25 → 09:30 $42.54 -76.95; CDNL×2 yday $39.23 → 09:30 $41.57 +4.68; ABX×9 yday $9.12 → 09:30 $9.03 -0.81; VERA×2 yday $31.63 → 09:30 $31.31 -0.64; OCC×4 yday $17.12 → 09:30 $16.20 -3.68; ALM×5 yday $16.36 → 09:30 $15.78 -2.90 |
| 2026-08-19 | -7.20 | $111.60 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45, CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 | $9,742.74 | +4.67 | — | ANGX, HYLN, WDC, ADUR, ALGM | $9,341.61 | $9,713.51 | CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 | 09:30 open · cash $111.60 (unchanged overnight, no fees) · equity $9,742.74 vs prior close $9,738.07 (+4.67) because holdings re-marked: ANGX×464 yday $4.85 → 09:30 $4.79 -27.84; HYLN×478 yday $3.86 → 09:30 $3.87 +4.78; WDC×3 yday $496.16 → 09:30 $494.28 -5.64; ADUR×121 yday $15.63 → 09:30 $15.65 +2.42; ALGM×45 yday $39.39 → 09:30 $40.00 +27.45; CDNL×2 yday $45.14 → 09:30 $44.83 -0.62; ABX×9 yday $9.01 → 09:30 $9.08 +0.63; VERA×2 yday $32.28 → 09:30 $32.88 +1.20; OCC×4 yday $16.20 → 09:30 $16.21 +0.04; ALM×5 yday $15.60 → 09:30 $16.05 +2.25 |
| 2026-08-20 | +1.12 | $9,341.61 | CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 | $9,710.08 | -3.43 | DNA, MSTR, EXK, SCZM, NG, BLSH, HYMC | CDNL, ABX, VERA, OCC, ALM | $68.32 | $9,613.26 | DNA×186, MSTR×12, EXK×128, SCZM×146, NG×165, BLSH×47, HYMC×50 | 09:30 open · cash $9,341.61 (unchanged overnight, no fees) · equity $9,710.08 vs prior close $9,713.51 (-3.43) because holdings re-marked: CDNL×2 yday $43.33 → 09:30 $43.13 -0.40; ABX×9 yday $9.15 → 09:30 $9.13 -0.18; VERA×2 yday $32.27 → 09:30 $32.30 +0.04; OCC×4 yday $14.36 → 09:30 $14.10 -1.04; ALM×5 yday $16.18 → 09:30 $15.81 -1.85 |
| 2026-08-21 | +3.25 | $68.32 | DNA×186, MSTR×12, EXK×128, SCZM×146, NG×165, BLSH×47, HYMC×50 | $10,029.37 | +416.11 | BTBT, ORBS, GORO | — | $29.17 | $9,868.25 | DNA×186, MSTR×12, EXK×128, SCZM×146, NG×165, BLSH×47, HYMC×50, BTBT×8, ORBS×15, GORO×4 | 09:30 open · cash $68.32 (unchanged overnight, no fees) · equity $10,029.37 vs prior close $9,613.26 (+416.11) because holdings re-marked: DNA×186 yday $6.96 → 09:30 $7.09 +24.18; MSTR×12 yday $112.39 → 09:30 $119.69 +87.60; EXK×128 yday $10.97 → 09:30 $11.34 +47.36; SCZM×146 yday $9.76 → 09:30 $10.26 +73.00; NG×165 yday $8.66 → 09:30 $9.02 +59.40; BLSH×47 yday $28.44 → 09:30 $29.75 +61.57; HYMC×50 yday $26.14 → 09:30 $27.40 +63.00 |
| 2026-08-24 | -5.17 | $29.17 | DNA×186, MSTR×12, EXK×128, SCZM×146, NG×165, BLSH×47, HYMC×50, BTBT×8, ORBS×15, GORO×4 | $9,969.51 | +101.26 | — | — | $29.17 | $9,896.10 | DNA×186, MSTR×12, EXK×128, SCZM×146, NG×165, BLSH×47, HYMC×50, BTBT×8, ORBS×15, GORO×4 | 09:30 open · cash $29.17 (unchanged overnight, no fees) · equity $9,969.51 vs prior close $9,868.25 (+101.26) because holdings re-marked: DNA×186 yday $7.40 → 09:30 $7.26 -26.04; MSTR×12 yday $119.25 → 09:30 $121.76 +30.12; EXK×128 yday $10.62 → 09:30 $11.01 +49.92; SCZM×146 yday $9.68 → 09:30 $9.82 +21.17; NG×165 yday $8.72 → 09:30 $8.89 +28.05; BLSH×47 yday $30.41 → 09:30 $30.18 -10.81; HYMC×50 yday $27.07 → 09:30 $27.24 +8.50; BTBT×8 yday $1.53 → 09:30 $1.55 +0.16; ORBS×15 yday $0.88 → 09:30 $0.89 +0.15; GORO×4 yday $3.19 → 09:30 $3.20 +0.04 |
| 2026-08-25 | +1.80 | $29.17 | DNA×186, MSTR×12, EXK×128, SCZM×146, NG×165, BLSH×47, HYMC×50, BTBT×8, ORBS×15, GORO×4 | $9,897.66 | +1.56 | NPWR, ALVO, ALIT, ZURA | DNA, MSTR, EXK, SCZM, NG, BLSH, HYMC | $5.01 | $9,937.99 | BTBT×8, ORBS×15, GORO×4, NPWR×1230, ALVO×471, ALIT×165, ZURA×382 | 09:30 open · cash $29.17 (unchanged overnight, no fees) · equity $9,897.66 vs prior close $9,896.10 (+1.56) because holdings re-marked: DNA×186 yday $6.98 → 09:30 $6.82 -29.76; MSTR×12 yday $124.59 → 09:30 $125.56 +11.64; EXK×128 yday $10.74 → 09:30 $10.72 -2.56; SCZM×146 yday $9.53 → 09:30 $9.57 +5.84; NG×165 yday $9.24 → 09:30 $9.34 +16.50; BLSH×47 yday $30.88 → 09:30 $31.00 +5.64; HYMC×50 yday $25.84 → 09:30 $25.73 -5.50; BTBT×8 yday $1.56 → 09:30 $1.55 -0.08; ORBS×15 yday $0.85 → 09:30 $0.85 +0.00; GORO×4 yday $3.57 → 09:30 $3.53 -0.16 |
| 2026-08-26 | +2.02 | $5.01 | BTBT×8, ORBS×15, GORO×4, NPWR×1230, ALVO×471, ALIT×165, ZURA×382 | $9,937.99 | +0.00 | — | — | $5.01 | $9,851.96 | BTBT×8, ORBS×15, GORO×4, NPWR×1230, ALVO×471, ALIT×165, ZURA×382 | 09:30 open · cash $5.01 (unchanged overnight, no fees) · equity $9,937.99 vs prior close $9,937.99 (+0.00) because holdings re-marked: BTBT×8 yday $1.53 → 09:30 $1.53 +0.00; ORBS×15 yday $0.84 → 09:30 $0.84 +0.00; GORO×4 yday $3.56 → 09:30 $3.56 +0.00; NPWR×1230 yday $2.02 → 09:30 $2.02 +0.00; ALVO×471 yday $5.25 → 09:30 $5.25 +0.00; ALIT×165 yday $14.87 → 09:30 $14.87 +0.00; ZURA×382 yday $6.50 → 09:30 $6.50 +0.00 |
| 2026-08-27 | — | $5.01 | BTBT×8, ORBS×15, GORO×4, NPWR×1230, ALVO×471, ALIT×165, ZURA×382 | $9,555.72 | -296.24 | — | BTBT, ORBS, GORO | $43.80 | $9,235.34 | NPWR×1230, ALVO×471, ALIT×165, ZURA×382 | 09:30 open · cash $5.01 (unchanged overnight, no fees) · equity $9,555.72 vs prior close $9,851.96 (-296.24) because holdings re-marked: BTBT×8 yday $1.53 → 09:30 $1.53 +0.00; ORBS×15 yday $0.84 → 09:30 $0.80 -0.60; GORO×4 yday $3.56 → 09:30 $3.77 +0.84; NPWR×1230 yday $2.02 → 09:30 $1.93 -110.70; ALVO×471 yday $5.25 → 09:30 $4.98 -127.17; ALIT×165 yday $14.87 → 09:30 $14.85 -3.30; ZURA×382 yday $6.50 → 09:30 $6.13 -141.34 |
| 2026-08-28 | +0.75 | $43.80 | NPWR×1230, ALVO×471, ALIT×165, ZURA×382 | $9,291.92 | +56.58 | ANF, BHVN, BZ, LVWR, GRRR | NPWR, ALVO, ALIT, ZURA | $100.88 | $9,048.64 | ANF×12, BHVN×109, BZ×100, LVWR×1342, GRRR×116 | 09:30 open · cash $43.80 (unchanged overnight, no fees) · equity $9,291.92 vs prior close $9,235.34 (+56.58) because holdings re-marked: NPWR×1230 yday $1.81 → 09:30 $1.83 +24.60; ALVO×471 yday $4.91 → 09:30 $4.88 -14.13; ALIT×165 yday $14.33 → 09:30 $14.54 +34.65; ZURA×382 yday $5.99 → 09:30 $6.02 +11.46 |
| 2026-08-31 | -5.85 | $100.88 | ANF×12, BHVN×109, BZ×100, LVWR×1342, GRRR×116 | $8,856.54 | -192.10 | — | — | $100.88 | $8,806.32 | ANF×12, BHVN×109, BZ×100, LVWR×1342, GRRR×116 | 09:30 open · cash $100.88 (unchanged overnight, no fees) · equity $8,856.54 vs prior close $9,048.64 (-192.10) because holdings re-marked: ANF×12 yday $145.75 → 09:30 $148.67 +35.04; BHVN×109 yday $16.12 → 09:30 $15.44 -74.12; BZ×100 yday $18.00 → 09:30 $17.89 -11.00; LVWR×1342 yday $1.36 → 09:30 $1.37 +13.42; GRRR×116 yday $15.66 → 09:30 $14.32 -155.44 |
| 2026-09-01 | -6.30 | $100.88 | ANF×12, BHVN×109, BZ×100, LVWR×1342, GRRR×116 | $8,614.61 | -191.71 | — | — | $100.88 | $8,518.29 | ANF×12, BHVN×109, BZ×100, LVWR×1342, GRRR×116 | 09:30 open · cash $100.88 (unchanged overnight, no fees) · equity $8,614.61 vs prior close $8,806.32 (-191.71) because holdings re-marked: ANF×12 yday $149.28 → 09:30 $142.47 -81.72; BHVN×109 yday $15.40 → 09:30 $15.45 +5.45; BZ×100 yday $17.90 → 09:30 $17.37 -53.00; LVWR×1342 yday $1.34 → 09:30 $1.22 -161.04; GRRR×116 yday $14.20 → 09:30 $15.05 +98.60 |
| 2026-09-02 | -3.83 | $100.88 | ANF×12, BHVN×109, BZ×100, LVWR×1342, GRRR×116 | $8,519.37 | +1.08 | — | ANF, BHVN, BZ, LVWR, GRRR | $8,492.74 | $8,492.74 | — | 09:30 open · cash $100.88 (unchanged overnight, no fees) · equity $8,519.37 vs prior close $8,518.29 (+1.08) because holdings re-marked: ANF×12 yday $143.00 → 09:30 $142.00 -12.00; BHVN×109 yday $15.45 → 09:30 $15.39 -6.54; BZ×100 yday $17.17 → 09:30 $17.29 +12.00; LVWR×1342 yday $1.18 → 09:30 $1.19 +13.42; GRRR×116 yday $14.80 → 09:30 $14.75 -5.80 |
| 2026-09-03 | -0.90 | $8,492.74 | — | $8,492.74 | -0.00 | GPRO, CRK, MMED | — | $22.36 | $9,639.08 | GPRO×2320, CRK×180, MMED×122 | 09:30 open · cash $8,492.74 · no holdings · equity $8,492.74 vs prior close $8,492.74 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $22.36 | GPRO×2320, CRK×180, MMED×122 | $9,846.32 | +207.24 | BAK, EOSE | — | $9.23 | $8,846.33 | GPRO×2320, CRK×180, MMED×122, BAK×3, EOSE×2 | 09:30 open · cash $22.36 (unchanged overnight, no fees) · equity $9,846.32 vs prior close $9,639.08 (+207.24) because holdings re-marked: GPRO×2320 yday $1.69 → 09:30 $1.78 +208.80; CRK×180 yday $15.54 → 09:30 $15.45 -16.20; MMED×122 yday $23.76 → 09:30 $23.88 +14.64 |

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
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 2 | $39.85 | $0.80 | — | $413.29 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $82.30 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 9 | $9.12 | $0.85 | — | $330.36 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $82.30 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 2 | $31.30 | $0.63 | — | $267.13 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-3.8; leftover $82.30 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 4 | $18.24 | $0.74 | — | $193.43 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $82.30 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 5 | $16.20 | $0.82 | — | $111.60 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $82.30 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $111.60 | ▼ 09:30 equity $9,860.11 vs yday $10,143.27 (-283.16) | 09:30 open · cash $111.60 (unchanged overnight, no fees) · equity $9,860.11 vs prior close $10,143.27 (-283.16) because holdings re-marked: ANGX×464 yday $4.71 → 09:30 $4.79 +37.12; HYLN×478 yday $4.09 → 09:30 $3.95 -66.92; WDC×3 yday $536.01 → 09:30 $496.07 -119.82; ADUR×121 yday $15.85 → 09:30 $15.41 -53.24; ALGM×45 yday $44.25 → 09:30 $42.54 -76.95; CDNL×2 yday $39.23 → 09:30 $41.57 +4.68; ABX×9 yday $9.12 → 09:30 $9.03 -0.81; VERA×2 yday $31.63 → 09:30 $31.31 -0.64; OCC×4 yday $17.12 → 09:30 $16.20 -3.68; ALM×5 yday $16.36 → 09:30 $15.78 -2.90 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $111.60 | ▲ 09:30 equity $9,742.74 vs yday $9,738.07 (+4.67) | 09:30 open · cash $111.60 (unchanged overnight, no fees) · equity $9,742.74 vs prior close $9,738.07 (+4.67) because holdings re-marked: ANGX×464 yday $4.85 → 09:30 $4.79 -27.84; HYLN×478 yday $3.86 → 09:30 $3.87 +4.78; WDC×3 yday $496.16 → 09:30 $494.28 -5.64; ADUR×121 yday $15.63 → 09:30 $15.65 +2.42; ALGM×45 yday $39.39 → 09:30 $40.00 +27.45; CDNL×2 yday $45.14 → 09:30 $44.83 -0.62; ABX×9 yday $9.01 → 09:30 $9.08 +0.63; VERA×2 yday $32.28 → 09:30 $32.88 +1.20; OCC×4 yday $16.20 → 09:30 $16.21 +0.04; ALM×5 yday $15.60 → 09:30 $16.05 +2.25 | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 464 | $4.79 | $6.08 | $+210.65 | $2,328.08 | ▲ +210.65 after sell → book $9,736.66; vs 09:30 mark -6.08 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 478 | $3.87 | $6.26 | $-160.61 | $4,171.68 | ▼ -160.61 after sell → book $9,730.40; vs 09:30 mark -6.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `WDC` | 3 | $494.28 | $2.02 | $-31.68 | $5,652.50 | ▼ -31.68 after sell → book $9,728.38; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ADUR` | 121 | $15.65 | $2.39 | $-107.59 | $7,543.76 | ▼ -107.59 after sell → book $9,725.99; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ALGM` | 45 | $40.00 | $2.15 | $-186.97 | $9,341.61 | ▼ -186.97 after sell → book $9,723.84; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,341.61 | ▼ 09:30 equity $9,710.08 vs yday $9,713.51 (-3.43) | 09:30 open · cash $9,341.61 (unchanged overnight, no fees) · equity $9,710.08 vs prior close $9,713.51 (-3.43) because holdings re-marked: CDNL×2 yday $43.33 → 09:30 $43.13 -0.40; ABX×9 yday $9.15 → 09:30 $9.13 -0.18; VERA×2 yday $32.27 → 09:30 $32.30 +0.04; OCC×4 yday $14.36 → 09:30 $14.10 -1.04; ALM×5 yday $16.18 → 09:30 $15.81 -1.85 | — |
| 2026-08-20 09:30 ET | **SELL** | `CDNL` | 2 | $43.13 | $0.89 | $+4.87 | $9,426.98 | ▲ +4.87 after sell → book $9,709.19; vs 09:30 mark -0.89 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ABX` | 9 | $9.13 | $0.87 | $-1.63 | $9,508.29 | ▼ -1.63 after sell → book $9,708.33; vs 09:30 mark -0.86 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `VERA` | 2 | $32.30 | $0.67 | $+0.69 | $9,572.20 | ▲ +0.69 after sell → book $9,707.65; vs 09:30 mark -0.68 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `OCC` | 4 | $14.10 | $0.60 | $-17.90 | $9,628.01 | ▼ -17.90 after sell → book $9,707.06; vs 09:30 mark -0.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ALM` | 5 | $15.81 | $0.83 | $-3.60 | $9,706.23 | ▼ -3.60 after sell → book $9,706.23; vs 09:30 mark -0.83 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 186 | $7.45 | $2.55 | — | $8,317.98 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1386.60 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MSTR` | 12 | $113.23 | $2.03 | — | $6,957.20 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1386.60 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 128 | $10.77 | $2.37 | — | $5,576.26 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1386.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 146 | $9.46 | $2.43 | — | $4,192.68 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1386.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NG` | 165 | $8.38 | $2.48 | — | $2,807.49 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+7.5; leftover $1386.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 47 | $29.20 | $2.13 | — | $1,432.96 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1386.60 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HYMC` | 50 | $27.25 | $2.14 | — | $68.32 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ret5=+1.6; leftover $1386.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $68.32 | ▲ 09:30 equity $10,029.37 vs yday $9,613.26 (+416.11) | 09:30 open · cash $68.32 (unchanged overnight, no fees) · equity $10,029.37 vs prior close $9,613.26 (+416.11) because holdings re-marked: DNA×186 yday $6.96 → 09:30 $7.09 +24.18; MSTR×12 yday $112.39 → 09:30 $119.69 +87.60; EXK×128 yday $10.97 → 09:30 $11.34 +47.36; SCZM×146 yday $9.76 → 09:30 $10.26 +73.00; NG×165 yday $8.66 → 09:30 $9.02 +59.40; BLSH×47 yday $28.44 → 09:30 $29.75 +61.57; HYMC×50 yday $26.14 → 09:30 $27.40 +63.00 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 8 | $1.66 | $0.16 | — | $54.88 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $13.66 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 15 | $0.86 | $0.17 | — | $41.75 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $13.66 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 4 | $3.11 | $0.14 | — | $29.17 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=+7.1; leftover $13.66 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.17 | ▲ 09:30 equity $9,969.51 vs yday $9,868.25 (+101.26) | 09:30 open · cash $29.17 (unchanged overnight, no fees) · equity $9,969.51 vs prior close $9,868.25 (+101.26) because holdings re-marked: DNA×186 yday $7.40 → 09:30 $7.26 -26.04; MSTR×12 yday $119.25 → 09:30 $121.76 +30.12; EXK×128 yday $10.62 → 09:30 $11.01 +49.92; SCZM×146 yday $9.68 → 09:30 $9.82 +21.17; NG×165 yday $8.72 → 09:30 $8.89 +28.05; BLSH×47 yday $30.41 → 09:30 $30.18 -10.81; HYMC×50 yday $27.07 → 09:30 $27.24 +8.50; BTBT×8 yday $1.53 → 09:30 $1.55 +0.16; ORBS×15 yday $0.88 → 09:30 $0.89 +0.15; GORO×4 yday $3.19 → 09:30 $3.20 +0.04 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.17 | ▲ 09:30 equity $9,897.66 vs yday $9,896.10 (+1.56) | 09:30 open · cash $29.17 (unchanged overnight, no fees) · equity $9,897.66 vs prior close $9,896.10 (+1.56) because holdings re-marked: DNA×186 yday $6.98 → 09:30 $6.82 -29.76; MSTR×12 yday $124.59 → 09:30 $125.56 +11.64; EXK×128 yday $10.74 → 09:30 $10.72 -2.56; SCZM×146 yday $9.53 → 09:30 $9.57 +5.84; NG×165 yday $9.24 → 09:30 $9.34 +16.50; BLSH×47 yday $30.88 → 09:30 $31.00 +5.64; HYMC×50 yday $25.84 → 09:30 $25.73 -5.50; BTBT×8 yday $1.56 → 09:30 $1.55 -0.08; ORBS×15 yday $0.85 → 09:30 $0.85 +0.00; GORO×4 yday $3.57 → 09:30 $3.53 -0.16 | — |
| 2026-08-25 09:30 ET | **SELL** | `DNA` | 186 | $6.82 | $2.59 | $-122.32 | $1,295.10 | ▼ -122.32 after sell → book $9,895.07; vs 09:30 mark -2.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MSTR` | 12 | $125.56 | $2.05 | $+143.89 | $2,799.78 | ▲ +143.89 after sell → book $9,893.02; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EXK` | 128 | $10.72 | $2.41 | $-11.18 | $4,169.53 | ▼ -11.18 after sell → book $9,890.62; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SCZM` | 146 | $9.57 | $2.46 | $+11.17 | $5,564.29 | ▲ +11.17 after sell → book $9,888.16; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NG` | 165 | $9.34 | $2.52 | $+153.39 | $7,102.86 | ▲ +153.39 after sell → book $9,885.63; vs 09:30 mark -2.53 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BLSH` | 47 | $31.00 | $2.15 | $+80.32 | $8,557.71 | ▲ +80.32 after sell → book $9,883.48; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HYMC` | 50 | $25.73 | $2.16 | $-80.30 | $9,842.05 | ▼ -80.30 after sell → book $9,881.32; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 1230 | $2.00 | $15.87 | — | $7,366.18 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $2460.51 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 471 | $5.22 | $6.08 | — | $4,901.48 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $2460.51 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 165 | $14.86 | $2.48 | — | $2,447.10 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $2460.51 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 382 | $6.38 | $4.93 | — | $5.01 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $2460.51 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.01 | ▲ 09:30 equity $9,937.99 vs yday $9,937.99 (+0.00) | 09:30 open · cash $5.01 (unchanged overnight, no fees) · equity $9,937.99 vs prior close $9,937.99 (+0.00) because holdings re-marked: BTBT×8 yday $1.53 → 09:30 $1.53 +0.00; ORBS×15 yday $0.84 → 09:30 $0.84 +0.00; GORO×4 yday $3.56 → 09:30 $3.56 +0.00; NPWR×1230 yday $2.02 → 09:30 $2.02 +0.00; ALVO×471 yday $5.25 → 09:30 $5.25 +0.00; ALIT×165 yday $14.87 → 09:30 $14.87 +0.00; ZURA×382 yday $6.50 → 09:30 $6.50 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.01 | ▼ 09:30 equity $9,555.72 vs yday $9,851.96 (-296.24) | 09:30 open · cash $5.01 (unchanged overnight, no fees) · equity $9,555.72 vs prior close $9,851.96 (-296.24) because holdings re-marked: BTBT×8 yday $1.53 → 09:30 $1.53 +0.00; ORBS×15 yday $0.84 → 09:30 $0.80 -0.60; GORO×4 yday $3.56 → 09:30 $3.77 +0.84; NPWR×1230 yday $2.02 → 09:30 $1.93 -110.70; ALVO×471 yday $5.25 → 09:30 $4.98 -127.17; ALIT×165 yday $14.87 → 09:30 $14.85 -3.30; ZURA×382 yday $6.50 → 09:30 $6.13 -141.34 | — |
| 2026-08-27 09:30 ET | **SELL** | `BTBT` | 8 | $1.53 | $0.17 | $-1.36 | $17.09 | ▼ -1.36 after sell → book $9,555.56; vs 09:30 mark -0.16 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ORBS` | 15 | $0.80 | $0.18 | $-1.32 | $28.90 | ▼ -1.32 after sell → book $9,555.37; vs 09:30 mark -0.19 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `GORO` | 4 | $3.77 | $0.18 | $+2.32 | $43.80 | ▲ +2.32 after sell → book $9,555.19; vs 09:30 mark -0.18 | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.80 | ▲ 09:30 equity $9,291.92 vs yday $9,235.34 (+56.58) | 09:30 open · cash $43.80 (unchanged overnight, no fees) · equity $9,291.92 vs prior close $9,235.34 (+56.58) because holdings re-marked: NPWR×1230 yday $1.81 → 09:30 $1.83 +24.60; ALVO×471 yday $4.91 → 09:30 $4.88 -14.13; ALIT×165 yday $14.33 → 09:30 $14.54 +34.65; ZURA×382 yday $5.99 → 09:30 $6.02 +11.46 | — |
| 2026-08-28 09:30 ET | **SELL** | `NPWR` | 1230 | $1.83 | $16.09 | $-241.06 | $2,278.61 | ▼ -241.06 after sell → book $9,275.83; vs 09:30 mark -16.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 471 | $4.88 | $6.17 | $-172.39 | $4,570.92 | ▼ -172.39 after sell → book $9,269.66; vs 09:30 mark -6.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALIT` | 165 | $14.54 | $2.53 | $-57.82 | $6,967.48 | ▼ -57.82 after sell → book $9,267.12; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 382 | $6.02 | $5.01 | $-147.46 | $9,262.11 | ▼ -147.46 after sell → book $9,262.11; vs 09:30 mark -5.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 12 | $144.70 | $2.03 | — | $7,523.69 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1852.42 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 109 | $16.95 | $2.32 | — | $5,673.82 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1852.42 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 100 | $18.50 | $2.29 | — | $3,821.53 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1852.42 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 1342 | $1.38 | $17.31 | — | $1,952.26 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $1852.42 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 116 | $15.94 | $2.34 | — | $100.88 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1852.42 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $100.88 | ▼ 09:30 equity $8,856.54 vs yday $9,048.64 (-192.10) | 09:30 open · cash $100.88 (unchanged overnight, no fees) · equity $8,856.54 vs prior close $9,048.64 (-192.10) because holdings re-marked: ANF×12 yday $145.75 → 09:30 $148.67 +35.04; BHVN×109 yday $16.12 → 09:30 $15.44 -74.12; BZ×100 yday $18.00 → 09:30 $17.89 -11.00; LVWR×1342 yday $1.36 → 09:30 $1.37 +13.42; GRRR×116 yday $15.66 → 09:30 $14.32 -155.44 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $100.88 | ▼ 09:30 equity $8,614.61 vs yday $8,806.32 (-191.71) | 09:30 open · cash $100.88 (unchanged overnight, no fees) · equity $8,614.61 vs prior close $8,806.32 (-191.71) because holdings re-marked: ANF×12 yday $149.28 → 09:30 $142.47 -81.72; BHVN×109 yday $15.40 → 09:30 $15.45 +5.45; BZ×100 yday $17.90 → 09:30 $17.37 -53.00; LVWR×1342 yday $1.34 → 09:30 $1.22 -161.04; GRRR×116 yday $14.20 → 09:30 $15.05 +98.60 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $100.88 | ▲ 09:30 equity $8,519.37 vs yday $8,518.29 (+1.08) | 09:30 open · cash $100.88 (unchanged overnight, no fees) · equity $8,519.37 vs prior close $8,518.29 (+1.08) because holdings re-marked: ANF×12 yday $143.00 → 09:30 $142.00 -12.00; BHVN×109 yday $15.45 → 09:30 $15.39 -6.54; BZ×100 yday $17.17 → 09:30 $17.29 +12.00; LVWR×1342 yday $1.18 → 09:30 $1.19 +13.42; GRRR×116 yday $14.80 → 09:30 $14.75 -5.80 | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 12 | $142.00 | $2.05 | $-36.48 | $1,802.83 | ▼ -36.48 after sell → book $8,517.32; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BHVN` | 109 | $15.39 | $2.35 | $-174.71 | $3,477.99 | ▼ -174.71 after sell → book $8,514.97; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 100 | $17.29 | $2.32 | $-125.61 | $5,204.67 | ▼ -125.61 after sell → book $8,512.65; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `LVWR` | 1342 | $1.19 | $17.55 | $-289.84 | $6,784.11 | ▼ -289.84 after sell → book $8,495.11; vs 09:30 mark -17.54 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 116 | $14.75 | $2.37 | $-142.75 | $8,492.74 | ▼ -142.75 after sell → book $8,492.74; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,492.74 | ▲ 09:30 equity $8,492.74 vs yday $8,492.74 (-0.00) | 09:30 open · cash $8,492.74 · no holdings · equity $8,492.74 vs prior close $8,492.74 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 2320 | $1.22 | $29.93 | — | $5,632.41 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $2830.91 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 180 | $15.70 | $2.53 | — | $2,803.88 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $2830.91 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 122 | $22.78 | $2.36 | — | $22.36 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $2830.91 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.36 | ▲ 09:30 equity $9,846.32 vs yday $9,639.08 (+207.24) | 09:30 open · cash $22.36 (unchanged overnight, no fees) · equity $9,846.32 vs prior close $9,639.08 (+207.24) because holdings re-marked: GPRO×2320 yday $1.69 → 09:30 $1.78 +208.80; CRK×180 yday $15.54 → 09:30 $15.45 -16.20; MMED×122 yday $23.76 → 09:30 $23.88 +14.64 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 3 | $1.95 | $0.07 | — | $16.44 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $7.45 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 2 | $3.57 | $0.08 | — | $9.23 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $7.45 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `WDC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ADUR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ALGM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CELC` | cash | leftover split 82.30 < 1 share @ 92.99 |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `WDC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ALGM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `CDNL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ABX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `VERA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OCC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ALM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `PGEN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `CDNL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ABX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `VERA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OCC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ALM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `DNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MSTR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `EXK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SCZM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BLSH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HYMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DE` | cash | leftover split 13.66 < 1 share @ 623.26 |
| 2026-08-21 | `QDEL` | cash | leftover split 13.66 < 1 share @ 14.96 |
| 2026-08-24 | `DNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MSTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `EXK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SCZM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BLSH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HYMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `GORO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `GORO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `BTBT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ORBS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `GORO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `ITG` | no_price | no 09:30 open |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LVWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `LVWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NAGE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | cash | leftover split 7.45 < 1 share @ 486.31 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GPRO` | 2320 | 2026-09-03 @ $1.22 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $2830.91 |
| `CRK` | 180 | 2026-09-03 @ $15.70 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $2830.91 |
| `MMED` | 122 | 2026-09-03 @ $22.78 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $2830.91 |
| `BAK` | 3 | 2026-09-04 @ $1.95 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $7.45 |
| `EOSE` | 2 | 2026-09-04 @ $3.57 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $7.45 |
