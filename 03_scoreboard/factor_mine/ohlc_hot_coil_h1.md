# Factor mine action — `ohlc_hot_coil_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `ohlc_hot` · top 8 · rank `list` · hot list ∩ not exploded

Cash book **-10.54%** ($8,946) · signal-only (no cash/fees) was +1.52%. Starts YES **12/17**. Fills 32 · skips 9 · realized $-1292.17.

## Each session

| Date | S | Route | Hard-red | Cash | Stock | Equity | Bought | Sold | Skipped | Why |
|---|---:|---|---|---:|---:|---:|---|---|---|---|
| 2026-08-13 | +8.53 | io | no | $10,000.00 | $0.00 | $10,000.00 | — | — | — | flat cash |
| 2026-08-14 | +5.50 | io | no | $9.70 | $9,782.85 | $9,792.55 | ADUR | — | — | BUY ADUR x605 @ 16.50 |
| 2026-08-17 | +2.25 | io | no | $21.29 | $9,195.58 | $9,216.87 | OCC, ALM, NEWP | ADUR | — | SELL ADUR (dropped from list after 1 sess (min 1)); BUY OCC x173 @ 18.24; BUY ALM x195 @ 16.20; BUY NEWP x457 @ 6.94 |
| 2026-08-18 | -6.20 | hold | yes | $8,864.87 | $0.00 | $8,864.87 | — | OCC, ALM, NEWP | — | SELL OCC (dropped from list after 1 sess (min 1)); SELL ALM (dropped from list after 1 sess (min 1)); SELL NEWP (dropped from list after 1 sess (min 1)) |
| 2026-08-19 | -7.20 | hold | yes | $8,864.87 | $0.00 | $8,864.87 | — | — | OBE, PAYS, BBNX, BETA, U, VSTM, AMTX, PSX | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | mover | no | $46.47 | $8,834.77 | $8,881.24 | NIQ, AUGO, ZLAB, PAYS | — | — | BUY NIQ x121 @ 18.31; BUY AUGO x26 @ 83.58; BUY ZLAB x83 @ 26.57; BUY PAYS x161 @ 13.76 |
| 2026-08-21 | +3.25 | mover | no | $35.28 | $8,865.59 | $8,900.87 | ORBS, EMBC, TXG, DXYZ | NIQ, AUGO, ZLAB, PAYS | — | SELL NIQ (dropped from list after 1 sess (min 1)); SELL AUGO (dropped from list after 1 sess (min 1)); SELL ZLAB (dropped from list after 1 sess (min 1)); SELL PAYS (dropped from list after 1 sess (min 1)); BUY ORBS x2600 @ 0.86; BUY EMBC x413 @ 5.43; BUY TXG x34 @ 64.39; BUY DXYZ x64 @ 34.89 |
| 2026-08-24 | -5.17 | hold | yes | $8,723.94 | $0.00 | $8,723.94 | — | ORBS, EMBC, TXG, DXYZ | — | SELL ORBS (dropped from list after 1 sess (min 1)); SELL EMBC (dropped from list after 1 sess (min 1)); SELL TXG (dropped from list after 1 sess (min 1)); SELL DXYZ (dropped from list after 1 sess (min 1)) |
| 2026-08-25 | +1.80 | io | no | $0.92 | $8,653.33 | $8,654.25 | NIQ, INO | — | — | BUY NIQ x223 @ 19.56; BUY INO x3451 @ 1.25 |
| 2026-08-26 | +2.02 | io | no | $0.92 | $8,675.63 | $8,676.55 | — | — | — | hold NIQ,INO |
| 2026-08-27 | — | io | no | $8,651.72 | $0.00 | $8,651.72 | — | NIQ, INO | — | SELL NIQ (dropped from list after 2 sess (min 1)); SELL INO (dropped from list after 2 sess (min 1)) |
| 2026-08-28 | +0.75 | io | no | $1.43 | $8,569.30 | $8,570.73 | NIQ, INO | — | — | BUY NIQ x230 @ 18.79; BUY INO x3320 @ 1.29 |
| 2026-08-31 | -5.85 | hold | yes | $1.43 | $8,732.00 | $8,733.43 | — | — | — | hard-red sit S=-5.85 |
| 2026-09-01 | -6.30 | hold | yes | $4,382.19 | $4,216.40 | $8,598.59 | — | NIQ | VFF | SELL NIQ (dropped from list after 2 sess (min 1)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | hold | yes | $4,382.19 | $4,415.60 | $8,797.79 | — | — | — | hard-red sit S=-3.83 |
| 2026-09-03 | -0.90 | io | no | $8.16 | $8,827.45 | $8,835.61 | NIQ | — | — | BUY NIQ x235 @ 18.60 |
| 2026-09-04 | — | io | no | $8.16 | $8,937.90 | $8,946.06 | — | — | — | hold INO,NIQ |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 605 | $16.50 | $7.80 | — | $9.70 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 605 | $15.73 | $7.98 | $-481.64 | $9,518.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 173 | $18.24 | $2.51 | — | $6,360.34 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; ⚪; ret5=+9.5 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 195 | $16.20 | $2.58 | — | $3,198.76 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NEWP` | 457 | $6.94 | $5.90 | — | $21.29 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.1 | join🟡 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 173 | $16.20 | $2.56 | $-357.99 | $2,821.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 195 | $15.78 | $2.63 | $-87.11 | $5,895.79 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `NEWP` | 457 | $6.51 | $6.00 | $-208.40 | $8,864.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-20 09:30 ET | **BUY** | `NIQ` | 121 | $18.31 | $2.35 | — | $6,647.00 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.9 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUGO` | 26 | $83.58 | $2.07 | — | $4,471.86 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+9.6 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 83 | $26.57 | $2.24 | — | $2,264.31 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+4.8 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `PAYS` | 161 | $13.76 | $2.47 | — | $46.47 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.8 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NIQ` | 121 | $18.30 | $2.39 | $-5.95 | $2,258.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AUGO` | 26 | $89.10 | $2.10 | $+139.36 | $4,572.89 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 83 | $26.25 | $2.27 | $-31.07 | $6,749.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `PAYS` | 161 | $13.91 | $2.52 | $+19.16 | $8,986.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 2600 | $0.86 | $30.26 | — | $6,709.70 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; ret5=+9.7 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `EMBC` | 413 | $5.43 | $5.33 | — | $4,461.78 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.0 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TXG` | 34 | $64.39 | $2.09 | — | $2,270.43 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+9.8 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DXYZ` | 64 | $34.89 | $2.18 | — | $35.28 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.6 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 2600 | $0.89 | $31.39 | $+5.95 | $2,317.89 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `EMBC` | 413 | $5.21 | $5.41 | $-101.60 | $4,464.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TXG` | 34 | $63.07 | $2.12 | $-49.09 | $6,606.47 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DXYZ` | 64 | $33.12 | $2.21 | $-117.67 | $8,723.94 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `NIQ` | 223 | $19.56 | $2.88 | — | $4,359.19 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.6 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `INO` | 3451 | $1.25 | $44.52 | — | $0.92 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+8.3 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟡 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `NIQ` | 223 | $19.20 | $2.95 | $-86.10 | $4,279.57 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INO` | 3451 | $1.28 | $45.13 | $+13.89 | $8,651.72 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `NIQ` | 230 | $18.79 | $2.97 | — | $4,327.06 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+7.6 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `INO` | 3320 | $1.29 | $42.83 | — | $1.43 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+8.3 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `NIQ` | 230 | $19.06 | $3.04 | $+56.09 | $4,382.19 | dropped from list after 2 sess (min 1) | — |
| 2026-09-03 09:30 ET | **BUY** | `NIQ` | 235 | $18.60 | $3.03 | — | $8.16 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.6 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟡 ab🟢 heat🔴 vol🟡 buy🟡 |

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
| `INO` | 3320 | 2026-08-28 @ $1.29 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+8.3 |
| `NIQ` | 235 | 2026-09-03 @ $18.60 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.6 |
