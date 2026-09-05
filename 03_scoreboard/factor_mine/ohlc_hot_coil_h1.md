# Factor mine action — `ohlc_hot_coil_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `ohlc_hot` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · hot list ∩ not exploded

Cash book **-10.54%** ($8,946) · signal-only (no cash/fees) was +1.52%. Starts YES **12/17**. Fills 32 · skips 9 · realized $-1292.17.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `ohlc_hot` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8.16.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | ADUR | — | $9.70 | $9,792.55 | ADUR×605 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $9.70 | ADUR×605 | $9,526.35 | -266.20 | OCC, ALM, NEWP | ADUR | $21.29 | $9,216.87 | OCC×173, ALM×195, NEWP×457 | 09:30 open · cash $9.70 (unchanged overnight, no fees) · equity $9,526.35 vs prior close $9,792.55 (-266.20) because holdings re-marked: ADUR×605 yday $16.17 → 09:30 $15.73 -266.20 |
| 2026-08-18 | -6.20 | $21.29 | OCC×173, ALM×195, NEWP×457 | $8,876.06 | -340.81 | — | OCC, ALM, NEWP | $8,864.87 | $8,864.87 | — | 09:30 open · cash $21.29 (unchanged overnight, no fees) · equity $8,876.06 vs prior close $9,216.87 (-340.81) because holdings re-marked: OCC×173 yday $17.12 → 09:30 $16.20 -159.16; ALM×195 yday $16.36 → 09:30 $15.78 -113.10; NEWP×457 yday $6.66 → 09:30 $6.51 -68.55 |
| 2026-08-19 | -7.20 | $8,864.87 | — | $8,864.87 | -0.00 | — | — | $8,864.87 | $8,864.87 | — | 09:30 open · cash $8,864.87 · no holdings · equity $8,864.87 vs prior close $8,864.87 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-20 | +1.12 | $8,864.87 | — | $8,864.87 | -0.00 | NIQ, AUGO, ZLAB, PAYS | — | $46.47 | $8,881.24 | NIQ×121, AUGO×26, ZLAB×83, PAYS×161 | 09:30 open · cash $8,864.87 · no holdings · equity $8,864.87 vs prior close $8,864.87 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $46.47 | NIQ×121, AUGO×26, ZLAB×83, PAYS×161 | $8,995.63 | +114.39 | ORBS, EMBC, TXG, DXYZ | NIQ, AUGO, ZLAB, PAYS | $35.28 | $8,900.87 | ORBS×2600, EMBC×413, TXG×34, DXYZ×64 | 09:30 open · cash $46.47 (unchanged overnight, no fees) · equity $8,995.63 vs prior close $8,881.24 (+114.39) because holdings re-marked: NIQ×121 yday $18.15 → 09:30 $18.30 +18.15; AUGO×26 yday $86.69 → 09:30 $89.10 +62.66; ZLAB×83 yday $26.02 → 09:30 $26.25 +19.09; PAYS×161 yday $13.82 → 09:30 $13.91 +14.49 |
| 2026-08-24 | -5.17 | $35.28 | ORBS×2600, EMBC×413, TXG×34, DXYZ×64 | $8,765.07 | -135.80 | — | ORBS, EMBC, TXG, DXYZ | $8,723.94 | $8,723.94 | — | 09:30 open · cash $35.28 (unchanged overnight, no fees) · equity $8,765.07 vs prior close $8,900.87 (-135.80) because holdings re-marked: ORBS×2600 yday $0.88 → 09:30 $0.89 +26.00; EMBC×413 yday $5.23 → 09:30 $5.21 -8.26; TXG×34 yday $65.12 → 09:30 $63.07 -69.70; DXYZ×64 yday $34.43 → 09:30 $33.12 -83.84 |
| 2026-08-25 | +1.80 | $8,723.94 | — | $8,723.94 | +0.00 | NIQ, INO | — | $0.92 | $8,654.25 | NIQ×223, INO×3451 | 09:30 open · cash $8,723.94 · no holdings · equity $8,723.94 vs prior close $8,723.94 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-26 | +2.02 | $0.92 | NIQ×223, INO×3451 | $8,654.25 | -0.00 | — | — | $0.92 | $8,676.55 | NIQ×223, INO×3451 | 09:30 open · cash $0.92 (unchanged overnight, no fees) · equity $8,654.25 vs prior close $8,654.25 (-0.00) because holdings re-marked: NIQ×223 yday $19.46 → 09:30 $19.46 +0.00; INO×3451 yday $1.25 → 09:30 $1.25 +0.00 |
| 2026-08-27 | — | $0.92 | NIQ×223, INO×3451 | $8,699.80 | +23.25 | — | NIQ, INO | $8,651.72 | $8,651.72 | — | 09:30 open · cash $0.92 (unchanged overnight, no fees) · equity $8,699.80 vs prior close $8,676.55 (+23.25) because holdings re-marked: NIQ×223 yday $19.46 → 09:30 $19.20 -57.98; INO×3451 yday $1.25 → 09:30 $1.28 +103.53 |
| 2026-08-28 | +0.75 | $8,651.72 | — | $8,651.72 | +0.00 | NIQ, INO | — | $1.43 | $8,570.73 | NIQ×230, INO×3320 | 09:30 open · cash $8,651.72 · no holdings · equity $8,651.72 vs prior close $8,651.72 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-31 | -5.85 | $1.43 | NIQ×230, INO×3320 | $8,766.63 | +195.90 | — | — | $1.43 | $8,733.43 | NIQ×230, INO×3320 | 09:30 open · cash $1.43 (unchanged overnight, no fees) · equity $8,766.63 vs prior close $8,570.73 (+195.90) because holdings re-marked: NIQ×230 yday $19.07 → 09:30 $19.20 +29.90; INO×3320 yday $1.26 → 09:30 $1.31 +166.00 |
| 2026-09-01 | -6.30 | $1.43 | NIQ×230, INO×3320 | $8,402.43 | -331.00 | — | NIQ | $4,382.19 | $8,598.59 | INO×3320 | 09:30 open · cash $1.43 (unchanged overnight, no fees) · equity $8,402.43 vs prior close $8,733.43 (-331.00) because holdings re-marked: NIQ×230 yday $19.20 → 09:30 $19.06 -32.20; INO×3320 yday $1.30 → 09:30 $1.21 -298.80 |
| 2026-09-02 | -3.83 | $4,382.19 | INO×3320 | $8,565.39 | -33.20 | — | — | $4,382.19 | $8,797.79 | INO×3320 | 09:30 open · cash $4,382.19 (unchanged overnight, no fees) · equity $8,565.39 vs prior close $8,598.59 (-33.20) because holdings re-marked: INO×3320 yday $1.27 → 09:30 $1.26 -33.20 |
| 2026-09-03 | -0.90 | $4,382.19 | INO×3320 | $8,830.99 | +33.20 | NIQ | — | $8.16 | $8,835.61 | INO×3320, NIQ×235 | 09:30 open · cash $4,382.19 (unchanged overnight, no fees) · equity $8,830.99 vs prior close $8,797.79 (+33.20) because holdings re-marked: INO×3320 yday $1.33 → 09:30 $1.34 +33.20 |
| 2026-09-04 | — | $8.16 | INO×3320, NIQ×235 | $8,941.66 | +106.05 | — | — | $8.16 | $8,946.06 | INO×3320, NIQ×235 | 09:30 open · cash $8.16 (unchanged overnight, no fees) · equity $8,941.66 vs prior close $8,835.61 (+106.05) because holdings re-marked: INO×3320 yday $1.36 → 09:30 $1.37 +33.20; NIQ×235 yday $18.35 → 09:30 $18.66 +72.85 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 605 | $16.50 | $7.80 | — | $9.70 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $10000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.70 | ▼ 09:30 equity $9,526.35 vs yday $9,792.55 (-266.20) | 09:30 open · cash $9.70 (unchanged overnight, no fees) · equity $9,526.35 vs prior close $9,792.55 (-266.20) because holdings re-marked: ADUR×605 yday $16.17 → 09:30 $15.73 -266.20 | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 605 | $15.73 | $7.98 | $-481.64 | $9,518.36 | ▼ -481.64 after sell → book $9,518.36; vs 09:30 mark -7.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 173 | $18.24 | $2.51 | — | $6,360.34 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $3172.79 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 195 | $16.20 | $2.58 | — | $3,198.76 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $3172.79 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NEWP` | 457 | $6.94 | $5.90 | — | $21.29 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.1; leftover $3172.79 | join🟡 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.29 | ▼ 09:30 equity $8,876.06 vs yday $9,216.87 (-340.81) | 09:30 open · cash $21.29 (unchanged overnight, no fees) · equity $8,876.06 vs prior close $9,216.87 (-340.81) because holdings re-marked: OCC×173 yday $17.12 → 09:30 $16.20 -159.16; ALM×195 yday $16.36 → 09:30 $15.78 -113.10; NEWP×457 yday $6.66 → 09:30 $6.51 -68.55 | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 173 | $16.20 | $2.56 | $-357.99 | $2,821.32 | ▼ -357.99 after sell → book $8,873.49; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 195 | $15.78 | $2.63 | $-87.11 | $5,895.79 | ▼ -87.11 after sell → book $8,870.86; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `NEWP` | 457 | $6.51 | $6.00 | $-208.40 | $8,864.87 | ▼ -208.40 after sell → book $8,864.87; vs 09:30 mark -5.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,864.87 | ▲ 09:30 equity $8,864.87 vs yday $8,864.87 (-0.00) | 09:30 open · cash $8,864.87 · no holdings · equity $8,864.87 vs prior close $8,864.87 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,864.87 | ▲ 09:30 equity $8,864.87 vs yday $8,864.87 (-0.00) | 09:30 open · cash $8,864.87 · no holdings · equity $8,864.87 vs prior close $8,864.87 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `NIQ` | 121 | $18.31 | $2.35 | — | $6,647.00 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.9; leftover $2216.22 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUGO` | 26 | $83.58 | $2.07 | — | $4,471.86 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+9.6; leftover $2216.22 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 83 | $26.57 | $2.24 | — | $2,264.31 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+4.8; leftover $2216.22 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `PAYS` | 161 | $13.76 | $2.47 | — | $46.47 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.8; leftover $2216.22 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.47 | ▲ 09:30 equity $8,995.63 vs yday $8,881.24 (+114.39) | 09:30 open · cash $46.47 (unchanged overnight, no fees) · equity $8,995.63 vs prior close $8,881.24 (+114.39) because holdings re-marked: NIQ×121 yday $18.15 → 09:30 $18.30 +18.15; AUGO×26 yday $86.69 → 09:30 $89.10 +62.66; ZLAB×83 yday $26.02 → 09:30 $26.25 +19.09; PAYS×161 yday $13.82 → 09:30 $13.91 +14.49 | — |
| 2026-08-21 09:30 ET | **SELL** | `NIQ` | 121 | $18.30 | $2.39 | $-5.95 | $2,258.38 | ▼ -5.95 after sell → book $8,993.24; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AUGO` | 26 | $89.10 | $2.10 | $+139.36 | $4,572.89 | ▲ +139.36 after sell → book $8,991.15; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 83 | $26.25 | $2.27 | $-31.07 | $6,749.37 | ▼ -31.07 after sell → book $8,988.88; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `PAYS` | 161 | $13.91 | $2.52 | $+19.16 | $8,986.36 | ▲ +19.16 after sell → book $8,986.36; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 2600 | $0.86 | $30.26 | — | $6,709.70 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $2246.59 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `EMBC` | 413 | $5.43 | $5.33 | — | $4,461.78 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $2246.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TXG` | 34 | $64.39 | $2.09 | — | $2,270.43 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $2246.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DXYZ` | 64 | $34.89 | $2.18 | — | $35.28 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.6; leftover $2246.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.28 | ▼ 09:30 equity $8,765.07 vs yday $8,900.87 (-135.80) | 09:30 open · cash $35.28 (unchanged overnight, no fees) · equity $8,765.07 vs prior close $8,900.87 (-135.80) because holdings re-marked: ORBS×2600 yday $0.88 → 09:30 $0.89 +26.00; EMBC×413 yday $5.23 → 09:30 $5.21 -8.26; TXG×34 yday $65.12 → 09:30 $63.07 -69.70; DXYZ×64 yday $34.43 → 09:30 $33.12 -83.84 | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 2600 | $0.89 | $31.39 | $+5.95 | $2,317.89 | ▲ +5.95 after sell → book $8,733.68; vs 09:30 mark -31.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `EMBC` | 413 | $5.21 | $5.41 | $-101.60 | $4,464.21 | ▼ -101.60 after sell → book $8,728.27; vs 09:30 mark -5.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TXG` | 34 | $63.07 | $2.12 | $-49.09 | $6,606.47 | ▼ -49.09 after sell → book $8,726.15; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DXYZ` | 64 | $33.12 | $2.21 | $-117.67 | $8,723.94 | ▼ -117.67 after sell → book $8,723.94; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,723.94 | ▲ 09:30 equity $8,723.94 vs yday $8,723.94 (+0.00) | 09:30 open · cash $8,723.94 · no holdings · equity $8,723.94 vs prior close $8,723.94 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `NIQ` | 223 | $19.56 | $2.88 | — | $4,359.19 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.6; leftover $4361.97 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `INO` | 3451 | $1.25 | $44.52 | — | $0.92 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+8.3; leftover $4361.97 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟡 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.92 | ▲ 09:30 equity $8,654.25 vs yday $8,654.25 (-0.00) | 09:30 open · cash $0.92 (unchanged overnight, no fees) · equity $8,654.25 vs prior close $8,654.25 (-0.00) because holdings re-marked: NIQ×223 yday $19.46 → 09:30 $19.46 +0.00; INO×3451 yday $1.25 → 09:30 $1.25 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.92 | ▲ 09:30 equity $8,699.80 vs yday $8,676.55 (+23.25) | 09:30 open · cash $0.92 (unchanged overnight, no fees) · equity $8,699.80 vs prior close $8,676.55 (+23.25) because holdings re-marked: NIQ×223 yday $19.46 → 09:30 $19.20 -57.98; INO×3451 yday $1.25 → 09:30 $1.28 +103.53 | — |
| 2026-08-27 09:30 ET | **SELL** | `NIQ` | 223 | $19.20 | $2.95 | $-86.10 | $4,279.57 | ▼ -86.10 after sell → book $8,696.85; vs 09:30 mark -2.95 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INO` | 3451 | $1.28 | $45.13 | $+13.89 | $8,651.72 | ▲ +13.89 after sell → book $8,651.72; vs 09:30 mark -45.13 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,651.72 | ▲ 09:30 equity $8,651.72 vs yday $8,651.72 (+0.00) | 09:30 open · cash $8,651.72 · no holdings · equity $8,651.72 vs prior close $8,651.72 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `NIQ` | 230 | $18.79 | $2.97 | — | $4,327.06 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+7.6; leftover $4325.86 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `INO` | 3320 | $1.29 | $42.83 | — | $1.43 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+8.3; leftover $4325.86 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.43 | ▲ 09:30 equity $8,766.63 vs yday $8,570.73 (+195.90) | 09:30 open · cash $1.43 (unchanged overnight, no fees) · equity $8,766.63 vs prior close $8,570.73 (+195.90) because holdings re-marked: NIQ×230 yday $19.07 → 09:30 $19.20 +29.90; INO×3320 yday $1.26 → 09:30 $1.31 +166.00 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.43 | ▼ 09:30 equity $8,402.43 vs yday $8,733.43 (-331.00) | 09:30 open · cash $1.43 (unchanged overnight, no fees) · equity $8,402.43 vs prior close $8,733.43 (-331.00) because holdings re-marked: NIQ×230 yday $19.20 → 09:30 $19.06 -32.20; INO×3320 yday $1.30 → 09:30 $1.21 -298.80 | — |
| 2026-09-01 09:30 ET | **SELL** | `NIQ` | 230 | $19.06 | $3.04 | $+56.09 | $4,382.19 | ▲ +56.09 after sell → book $8,399.39; vs 09:30 mark -3.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,382.19 | ▼ 09:30 equity $8,565.39 vs yday $8,598.59 (-33.20) | 09:30 open · cash $4,382.19 (unchanged overnight, no fees) · equity $8,565.39 vs prior close $8,598.59 (-33.20) because holdings re-marked: INO×3320 yday $1.27 → 09:30 $1.26 -33.20 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,382.19 | ▲ 09:30 equity $8,830.99 vs yday $8,797.79 (+33.20) | 09:30 open · cash $4,382.19 (unchanged overnight, no fees) · equity $8,830.99 vs prior close $8,797.79 (+33.20) because holdings re-marked: INO×3320 yday $1.33 → 09:30 $1.34 +33.20 | — |
| 2026-09-03 09:30 ET | **BUY** | `NIQ` | 235 | $18.60 | $3.03 | — | $8.16 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.6; leftover $4382.19 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.16 | ▲ 09:30 equity $8,941.66 vs yday $8,835.61 (+106.05) | 09:30 open · cash $8.16 (unchanged overnight, no fees) · equity $8,941.66 vs prior close $8,835.61 (+106.05) because holdings re-marked: INO×3320 yday $1.36 → 09:30 $1.37 +33.20; NIQ×235 yday $18.35 → 09:30 $18.66 +72.85 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PAYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBNX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BETA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `U` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `VSTM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMTX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PSX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-09-01 | `VFF` | hard_red | hard-red S=-6.30 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `INO` | 3320 | 2026-08-28 @ $1.29 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+8.3; leftover $4325.86 |
| `NIQ` | 235 | 2026-09-03 @ $18.60 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.6; leftover $4382.19 |
