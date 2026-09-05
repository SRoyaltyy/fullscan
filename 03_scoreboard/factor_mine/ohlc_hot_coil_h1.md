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

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `ADUR` | 605 | — | $16.50 | +0.00 | $16.17 | -199.65 | -199.65 | +0.00 | -199.65 |
| 2026-08-17 | `ADUR` | 605 | $16.17 | $15.73 | -266.20 | — | +0.00 | -266.20 | -465.85 | — |
| 2026-08-17 | `OCC` | 173 | — | $18.24 | +0.00 | $17.12 | -193.76 | -193.76 | +0.00 | -193.76 |
| 2026-08-17 | `ALM` | 195 | — | $16.20 | +0.00 | $16.36 | +31.20 | +31.20 | +0.00 | +31.20 |
| 2026-08-17 | `NEWP` | 457 | — | $6.94 | +0.00 | $6.66 | -127.96 | -127.96 | +0.00 | -127.96 |
| 2026-08-18 | `OCC` | 173 | $17.12 | $16.20 | -159.16 | — | +0.00 | -159.16 | -352.92 | — |
| 2026-08-18 | `ALM` | 195 | $16.36 | $15.78 | -113.10 | — | +0.00 | -113.10 | -81.90 | — |
| 2026-08-18 | `NEWP` | 457 | $6.66 | $6.51 | -68.55 | — | +0.00 | -68.55 | -196.51 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `NIQ` | 121 | — | $18.31 | +0.00 | $18.15 | -19.36 | -19.36 | +0.00 | -19.36 |
| 2026-08-20 | `AUGO` | 26 | — | $83.58 | +0.00 | $86.69 | +80.86 | +80.86 | +0.00 | +80.86 |
| 2026-08-20 | `ZLAB` | 83 | — | $26.57 | +0.00 | $26.02 | -45.65 | -45.65 | +0.00 | -45.65 |
| 2026-08-20 | `PAYS` | 161 | — | $13.76 | +0.00 | $13.82 | +9.66 | +9.66 | +0.00 | +9.66 |
| 2026-08-21 | `NIQ` | 121 | $18.15 | $18.30 | +18.15 | — | +0.00 | +18.15 | -1.21 | — |
| 2026-08-21 | `AUGO` | 26 | $86.69 | $89.10 | +62.66 | — | +0.00 | +62.66 | +143.52 | — |
| 2026-08-21 | `ZLAB` | 83 | $26.02 | $26.25 | +19.09 | — | +0.00 | +19.09 | -26.56 | — |
| 2026-08-21 | `PAYS` | 161 | $13.82 | $13.91 | +14.49 | — | +0.00 | +14.49 | +24.15 | — |
| 2026-08-21 | `ORBS` | 2600 | — | $0.86 | +0.00 | $0.88 | +41.60 | +41.60 | +0.00 | +41.60 |
| 2026-08-21 | `EMBC` | 413 | — | $5.43 | +0.00 | $5.23 | -82.60 | -82.60 | +0.00 | -82.60 |
| 2026-08-21 | `TXG` | 34 | — | $64.39 | +0.00 | $65.12 | +24.82 | +24.82 | +0.00 | +24.82 |
| 2026-08-21 | `DXYZ` | 64 | — | $34.89 | +0.00 | $34.43 | -29.44 | -29.44 | +0.00 | -29.44 |
| 2026-08-24 | `ORBS` | 2600 | $0.88 | $0.89 | +26.00 | — | +0.00 | +26.00 | +67.60 | — |
| 2026-08-24 | `EMBC` | 413 | $5.23 | $5.21 | -8.26 | — | +0.00 | -8.26 | -90.86 | — |
| 2026-08-24 | `TXG` | 34 | $65.12 | $63.07 | -69.70 | — | +0.00 | -69.70 | -44.88 | — |
| 2026-08-24 | `DXYZ` | 64 | $34.43 | $33.12 | -83.84 | — | +0.00 | -83.84 | -113.28 | — |
| 2026-08-25 | `NIQ` | 223 | — | $19.56 | +0.00 | $19.46 | -22.30 | -22.30 | +0.00 | -22.30 |
| 2026-08-25 | `INO` | 3451 | — | $1.25 | +0.00 | $1.25 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `NIQ` | 223 | $19.46 | $19.46 | +0.00 | $19.46 | +0.00 | +0.00 | -22.30 | -22.30 |
| 2026-08-26 | `INO` | 3451 | $1.25 | $1.25 | +0.00 | $1.25 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-27 | `NIQ` | 223 | $19.46 | $19.20 | -57.98 | — | +0.00 | -57.98 | -80.28 | — |
| 2026-08-27 | `INO` | 3451 | $1.25 | $1.28 | +103.53 | — | +0.00 | +103.53 | +103.53 | — |
| 2026-08-28 | `NIQ` | 230 | — | $18.79 | +0.00 | $19.07 | +64.40 | +64.40 | +0.00 | +64.40 |
| 2026-08-28 | `INO` | 3320 | — | $1.29 | +0.00 | $1.26 | -99.60 | -99.60 | +0.00 | -99.60 |
| 2026-08-31 | `NIQ` | 230 | $19.07 | $19.20 | +29.90 | $19.20 | +0.00 | +29.90 | +94.30 | +94.30 |
| 2026-08-31 | `INO` | 3320 | $1.26 | $1.31 | +166.00 | $1.30 | -33.20 | +132.80 | +66.40 | +33.20 |
| 2026-09-01 | `NIQ` | 230 | $19.20 | $19.06 | -32.20 | — | +0.00 | -32.20 | +62.10 | — |
| 2026-09-01 | `INO` | 3320 | $1.30 | $1.21 | -298.80 | $1.27 | +199.20 | -99.60 | -265.60 | -66.40 |
| 2026-09-02 | `INO` | 3320 | $1.27 | $1.26 | -33.20 | $1.33 | +232.40 | +199.20 | -99.60 | +132.80 |
| 2026-09-03 | `INO` | 3320 | $1.33 | $1.34 | +33.20 | $1.36 | +66.40 | +99.60 | +166.00 | +232.40 |
| 2026-09-03 | `NIQ` | 235 | — | $18.60 | +0.00 | $18.35 | -58.75 | -58.75 | +0.00 | -58.75 |
| 2026-09-04 | `INO` | 3320 | $1.36 | $1.37 | +33.20 | $1.36 | -33.20 | +0.00 | +265.60 | +232.40 |
| 2026-09-04 | `NIQ` | 235 | $18.35 | $18.66 | +72.85 | $18.82 | +37.60 | +110.45 | +14.10 | +51.70 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -199.65 | ADUR | — | $9.70 | $9,792.55 | ADUR×605 |
| 2026-08-17 | +2.25 | $9.70 | ADUR×605 | $9,526.35 | -266.20 | -290.52 | OCC, ALM, NEWP | ADUR | $21.29 | $9,216.87 | OCC×173, ALM×195, NEWP×457 |
| 2026-08-18 | -6.20 | $21.29 | OCC×173, ALM×195, NEWP×457 | $8,876.06 | -340.81 | +0.00 | — | OCC, ALM, NEWP | $8,864.87 | $8,864.87 | — |
| 2026-08-19 | -7.20 | $8,864.87 | — | $8,864.87 | -0.00 | +0.00 | — | — | $8,864.87 | $8,864.87 | — |
| 2026-08-20 | +1.12 | $8,864.87 | — | $8,864.87 | -0.00 | +25.51 | NIQ, AUGO, ZLAB, PAYS | — | $46.47 | $8,881.24 | NIQ×121, AUGO×26, ZLAB×83, PAYS×161 |
| 2026-08-21 | +3.25 | $46.47 | NIQ×121, AUGO×26, ZLAB×83, PAYS×161 | $8,995.63 | +114.39 | -45.62 | ORBS, EMBC, TXG, DXYZ | NIQ, AUGO, ZLAB, PAYS | $35.28 | $8,900.87 | ORBS×2600, EMBC×413, TXG×34, DXYZ×64 |
| 2026-08-24 | -5.17 | $35.28 | ORBS×2600, EMBC×413, TXG×34, DXYZ×64 | $8,765.07 | -135.80 | +0.00 | — | ORBS, EMBC, TXG, DXYZ | $8,723.94 | $8,723.94 | — |
| 2026-08-25 | +1.80 | $8,723.94 | — | $8,723.94 | +0.00 | -22.30 | NIQ, INO | — | $0.92 | $8,654.25 | NIQ×223, INO×3451 |
| 2026-08-26 | +2.02 | $0.92 | NIQ×223, INO×3451 | $8,654.25 | -0.00 | +0.00 | — | — | $0.92 | $8,654.25 | NIQ×223, INO×3451 |
| 2026-08-27 | — | $0.92 | NIQ×223, INO×3451 | $8,699.80 | +45.55 | +0.00 | — | NIQ, INO | $8,651.72 | $8,651.72 | — |
| 2026-08-28 | +0.75 | $8,651.72 | — | $8,651.72 | +0.00 | -35.20 | NIQ, INO | — | $1.43 | $8,570.73 | NIQ×230, INO×3320 |
| 2026-08-31 | -5.85 | $1.43 | NIQ×230, INO×3320 | $8,766.63 | +195.90 | -33.20 | — | — | $1.43 | $8,733.43 | NIQ×230, INO×3320 |
| 2026-09-01 | -6.30 | $1.43 | NIQ×230, INO×3320 | $8,402.43 | -331.00 | +199.20 | — | NIQ | $4,382.19 | $8,598.59 | INO×3320 |
| 2026-09-02 | -3.83 | $4,382.19 | INO×3320 | $8,565.39 | -33.20 | +232.40 | — | — | $4,382.19 | $8,797.79 | INO×3320 |
| 2026-09-03 | -0.90 | $4,382.19 | INO×3320 | $8,830.99 | +33.20 | +7.65 | NIQ | — | $8.16 | $8,835.61 | INO×3320, NIQ×235 |
| 2026-09-04 | — | $8.16 | INO×3320, NIQ×235 | $8,941.66 | +106.05 | +4.40 | — | — | $8.16 | $8,946.06 | INO×3320, NIQ×235 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 605 | $16.50 | $7.80 | — | $9.70 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $10000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.70 | ▼ close $9,792.55 vs 09:30 $10,000.00 (session -199.65) | 16:00 close · cash $9.70 · equity $9,792.55 vs 09:30 $10,000.00 (-207.45; session marks -199.65) · 1 name(s) marked open→close (per-name table). ADUR×605 09:30 $16.50 → close $16.17 -199.65 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.70 | ▼ 09:30 equity $9,526.35 vs yday $9,792.55 (-266.20) | 09:30 open · cash $9.70 (unchanged overnight, no fees) · equity $9,526.35 vs prior close $9,792.55 (-266.20) · 1 name(s) re-marked at the open (per-name table). ADUR×605 yday $16.17 → 09:30 $15.73 -266.20 | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 605 | $15.73 | $7.98 | $-481.64 | $9,518.36 | ▼ -481.64 after sell → book $9,518.36; vs 09:30 mark -7.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 173 | $18.24 | $2.51 | — | $6,360.34 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $3172.79 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 195 | $16.20 | $2.58 | — | $3,198.76 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $3172.79 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NEWP` | 457 | $6.94 | $5.90 | — | $21.29 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.1; leftover $3172.79 | join🟡 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.29 | ▼ close $9,216.87 vs 09:30 $9,526.35 (session -290.52) | 16:00 close · cash $21.29 · equity $9,216.87 vs 09:30 $9,526.35 (-309.48; session marks -290.52) · 3 name(s) marked open→close (per-name table). OCC×173 09:30 $18.24 → close $17.12 -193.76; ALM×195 09:30 $16.20 → close $16.36 +31.20; NEWP×457 09:30 $6.94 → close $6.66 -127.96 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.29 | ▼ 09:30 equity $8,876.06 vs yday $9,216.87 (-340.81) | 09:30 open · cash $21.29 (unchanged overnight, no fees) · equity $8,876.06 vs prior close $9,216.87 (-340.81) · 3 name(s) re-marked at the open (per-name table). OCC×173 yday $17.12 → 09:30 $16.20 -159.16; ALM×195 yday $16.36 → 09:30 $15.78 -113.10; NEWP×457 yday $6.66 → 09:30 $6.51 -68.55 | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 173 | $16.20 | $2.56 | $-357.99 | $2,821.32 | ▼ -357.99 after sell → book $8,873.49; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 195 | $15.78 | $2.63 | $-87.11 | $5,895.79 | ▼ -87.11 after sell → book $8,870.86; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `NEWP` | 457 | $6.51 | $6.00 | $-208.40 | $8,864.87 | ▼ -208.40 after sell → book $8,864.87; vs 09:30 mark -5.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,864.87 | ▲ close $8,864.87 vs 09:30 $8,876.06 (session +0.00) | 16:00 close · cash $8,864.87 · no lots left · equity $8,864.87. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,864.87 | ▲ 09:30 equity $8,864.87 vs yday $8,864.87 (-0.00) | 09:30 open · cash $8,864.87 · no holdings · equity $8,864.87 vs prior close $8,864.87 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,864.87 | ▲ close $8,864.87 vs 09:30 $8,864.87 (session +0.00) | 16:00 close · cash $8,864.87 · no lots left · equity $8,864.87. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,864.87 | ▲ 09:30 equity $8,864.87 vs yday $8,864.87 (-0.00) | 09:30 open · cash $8,864.87 · no holdings · equity $8,864.87 vs prior close $8,864.87 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `NIQ` | 121 | $18.31 | $2.35 | — | $6,647.00 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.9; leftover $2216.22 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUGO` | 26 | $83.58 | $2.07 | — | $4,471.86 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+9.6; leftover $2216.22 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 83 | $26.57 | $2.24 | — | $2,264.31 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+4.8; leftover $2216.22 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `PAYS` | 161 | $13.76 | $2.47 | — | $46.47 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.8; leftover $2216.22 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.47 | ▲ close $8,881.24 vs 09:30 $8,864.87 (session +25.51) | 16:00 close · cash $46.47 · equity $8,881.24 vs 09:30 $8,864.87 (+16.37; session marks +25.51) · 4 name(s) marked open→close (per-name table). NIQ×121 09:30 $18.31 → close $18.15 -19.36; AUGO×26 09:30 $83.58 → close $86.69 +80.86; ZLAB×83 09:30 $26.57 → close $26.02 -45.65; PAYS×161 09:30 $13.76 → close $13.82 +9.66 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.47 | ▲ 09:30 equity $8,995.63 vs yday $8,881.24 (+114.39) | 09:30 open · cash $46.47 (unchanged overnight, no fees) · equity $8,995.63 vs prior close $8,881.24 (+114.39) · 4 name(s) re-marked at the open (per-name table). NIQ×121 yday $18.15 → 09:30 $18.30 +18.15; AUGO×26 yday $86.69 → 09:30 $89.10 +62.66; ZLAB×83 yday $26.02 → 09:30 $26.25 +19.09; PAYS×161 yday $13.82 → 09:30 $13.91 +14.49 | — |
| 2026-08-21 09:30 ET | **SELL** | `NIQ` | 121 | $18.30 | $2.39 | $-5.95 | $2,258.38 | ▼ -5.95 after sell → book $8,993.24; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AUGO` | 26 | $89.10 | $2.10 | $+139.36 | $4,572.89 | ▲ +139.36 after sell → book $8,991.15; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 83 | $26.25 | $2.27 | $-31.07 | $6,749.37 | ▼ -31.07 after sell → book $8,988.88; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `PAYS` | 161 | $13.91 | $2.52 | $+19.16 | $8,986.36 | ▲ +19.16 after sell → book $8,986.36; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 2600 | $0.86 | $30.26 | — | $6,709.70 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $2246.59 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `EMBC` | 413 | $5.43 | $5.33 | — | $4,461.78 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $2246.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TXG` | 34 | $64.39 | $2.09 | — | $2,270.43 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $2246.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DXYZ` | 64 | $34.89 | $2.18 | — | $35.28 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.6; leftover $2246.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🔴 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.28 | ▼ close $8,900.87 vs 09:30 $8,995.63 (session -45.62) | 16:00 close · cash $35.28 · equity $8,900.87 vs 09:30 $8,995.63 (-94.76; session marks -45.62) · 4 name(s) marked open→close (per-name table). ORBS×2600 09:30 $0.86 → close $0.88 +41.60; EMBC×413 09:30 $5.43 → close $5.23 -82.60; TXG×34 09:30 $64.39 → close $65.12 +24.82; DXYZ×64 09:30 $34.89 → close $34.43 -29.44 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.28 | ▼ 09:30 equity $8,765.07 vs yday $8,900.87 (-135.80) | 09:30 open · cash $35.28 (unchanged overnight, no fees) · equity $8,765.07 vs prior close $8,900.87 (-135.80) · 4 name(s) re-marked at the open (per-name table). ORBS×2600 yday $0.88 → 09:30 $0.89 +26.00; EMBC×413 yday $5.23 → 09:30 $5.21 -8.26; TXG×34 yday $65.12 → 09:30 $63.07 -69.70; DXYZ×64 yday $34.43 → 09:30 $33.12 -83.84 | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 2600 | $0.89 | $31.39 | $+5.95 | $2,317.89 | ▲ +5.95 after sell → book $8,733.68; vs 09:30 mark -31.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `EMBC` | 413 | $5.21 | $5.41 | $-101.60 | $4,464.21 | ▼ -101.60 after sell → book $8,728.27; vs 09:30 mark -5.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TXG` | 34 | $63.07 | $2.12 | $-49.09 | $6,606.47 | ▼ -49.09 after sell → book $8,726.15; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DXYZ` | 64 | $33.12 | $2.21 | $-117.67 | $8,723.94 | ▼ -117.67 after sell → book $8,723.94; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,723.94 | ▲ close $8,723.94 vs 09:30 $8,765.07 (session +0.00) | 16:00 close · cash $8,723.94 · no lots left · equity $8,723.94. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,723.94 | ▲ 09:30 equity $8,723.94 vs yday $8,723.94 (+0.00) | 09:30 open · cash $8,723.94 · no holdings · equity $8,723.94 vs prior close $8,723.94 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `NIQ` | 223 | $19.56 | $2.88 | — | $4,359.19 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.6; leftover $4361.97 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `INO` | 3451 | $1.25 | $44.52 | — | $0.92 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+8.3; leftover $4361.97 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.92 | ▼ close $8,654.25 vs 09:30 $8,723.94 (session -22.30) | 16:00 close · cash $0.92 · equity $8,654.25 vs 09:30 $8,723.94 (-69.69; session marks -22.30) · 2 name(s) marked open→close (per-name table). NIQ×223 09:30 $19.56 → close $19.46 -22.30; INO×3451 09:30 $1.25 → close $1.25 +0.00 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.92 | ▲ 09:30 equity $8,654.25 vs yday $8,654.25 (-0.00) | 09:30 open · cash $0.92 (unchanged overnight, no fees) · equity $8,654.25 vs prior close $8,654.25 (-0.00) · 2 name(s) re-marked at the open (per-name table). NIQ×223 yday $19.46 → 09:30 $19.46 +0.00; INO×3451 yday $1.25 → 09:30 $1.25 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.92 | ▲ close $8,654.25 vs 09:30 $8,654.25 (session +0.00) | 16:00 close · cash $0.92 · equity $8,654.25 vs 09:30 $8,654.25 (-0.00; session marks +0.00) · 2 name(s) marked open→close (per-name table). NIQ×223 09:30 $19.46 → close $19.46 +0.00; INO×3451 09:30 $1.25 → close $1.25 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.92 | ▲ 09:30 equity $8,699.80 vs yday $8,654.25 (+45.55) | 09:30 open · cash $0.92 (unchanged overnight, no fees) · equity $8,699.80 vs prior close $8,654.25 (+45.55) · 2 name(s) re-marked at the open (per-name table). NIQ×223 yday $19.46 → 09:30 $19.20 -57.98; INO×3451 yday $1.25 → 09:30 $1.28 +103.53 | — |
| 2026-08-27 09:30 ET | **SELL** | `NIQ` | 223 | $19.20 | $2.95 | $-86.10 | $4,279.57 | ▼ -86.10 after sell → book $8,696.85; vs 09:30 mark -2.95 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INO` | 3451 | $1.28 | $45.13 | $+13.89 | $8,651.72 | ▲ +13.89 after sell → book $8,651.72; vs 09:30 mark -45.13 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,651.72 | ▲ close $8,651.72 vs 09:30 $8,699.80 (session +0.00) | 16:00 close · cash $8,651.72 · no lots left · equity $8,651.72. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,651.72 | ▲ 09:30 equity $8,651.72 vs yday $8,651.72 (+0.00) | 09:30 open · cash $8,651.72 · no holdings · equity $8,651.72 vs prior close $8,651.72 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `NIQ` | 230 | $18.79 | $2.97 | — | $4,327.06 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+7.6; leftover $4325.86 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `INO` | 3320 | $1.29 | $42.83 | — | $1.43 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+8.3; leftover $4325.86 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.43 | ▼ close $8,570.73 vs 09:30 $8,651.72 (session -35.20) | 16:00 close · cash $1.43 · equity $8,570.73 vs 09:30 $8,651.72 (-80.99; session marks -35.20) · 2 name(s) marked open→close (per-name table). NIQ×230 09:30 $18.79 → close $19.07 +64.40; INO×3320 09:30 $1.29 → close $1.26 -99.60 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.43 | ▲ 09:30 equity $8,766.63 vs yday $8,570.73 (+195.90) | 09:30 open · cash $1.43 (unchanged overnight, no fees) · equity $8,766.63 vs prior close $8,570.73 (+195.90) · 2 name(s) re-marked at the open (per-name table). NIQ×230 yday $19.07 → 09:30 $19.20 +29.90; INO×3320 yday $1.26 → 09:30 $1.31 +166.00 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.43 | ▼ close $8,733.43 vs 09:30 $8,766.63 (session -33.20) | 16:00 close · cash $1.43 · equity $8,733.43 vs 09:30 $8,766.63 (-33.20; session marks -33.20) · 2 name(s) marked open→close (per-name table). NIQ×230 09:30 $19.20 → close $19.20 +0.00; INO×3320 09:30 $1.31 → close $1.30 -33.20 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.43 | ▼ 09:30 equity $8,402.43 vs yday $8,733.43 (-331.00) | 09:30 open · cash $1.43 (unchanged overnight, no fees) · equity $8,402.43 vs prior close $8,733.43 (-331.00) · 2 name(s) re-marked at the open (per-name table). NIQ×230 yday $19.20 → 09:30 $19.06 -32.20; INO×3320 yday $1.30 → 09:30 $1.21 -298.80 | — |
| 2026-09-01 09:30 ET | **SELL** | `NIQ` | 230 | $19.06 | $3.04 | $+56.09 | $4,382.19 | ▲ +56.09 after sell → book $8,399.39; vs 09:30 mark -3.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,382.19 | ▲ close $8,598.59 vs 09:30 $8,402.43 (session +199.20) | 16:00 close · cash $4,382.19 · equity $8,598.59 vs 09:30 $8,402.43 (+196.16; session marks +199.20) · 1 name(s) marked open→close (per-name table). INO×3320 09:30 $1.21 → close $1.27 +199.20 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,382.19 | ▼ 09:30 equity $8,565.39 vs yday $8,598.59 (-33.20) | 09:30 open · cash $4,382.19 (unchanged overnight, no fees) · equity $8,565.39 vs prior close $8,598.59 (-33.20) · 1 name(s) re-marked at the open (per-name table). INO×3320 yday $1.27 → 09:30 $1.26 -33.20 | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,382.19 | ▲ close $8,797.79 vs 09:30 $8,565.39 (session +232.40) | 16:00 close · cash $4,382.19 · equity $8,797.79 vs 09:30 $8,565.39 (+232.40; session marks +232.40) · 1 name(s) marked open→close (per-name table). INO×3320 09:30 $1.26 → close $1.33 +232.40 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,382.19 | ▲ 09:30 equity $8,830.99 vs yday $8,797.79 (+33.20) | 09:30 open · cash $4,382.19 (unchanged overnight, no fees) · equity $8,830.99 vs prior close $8,797.79 (+33.20) · 1 name(s) re-marked at the open (per-name table). INO×3320 yday $1.33 → 09:30 $1.34 +33.20 | — |
| 2026-09-03 09:30 ET | **BUY** | `NIQ` | 235 | $18.60 | $3.03 | — | $8.16 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.6; leftover $4382.19 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.16 | ▲ close $8,835.61 vs 09:30 $8,830.99 (session +7.65) | 16:00 close · cash $8.16 · equity $8,835.61 vs 09:30 $8,830.99 (+4.62; session marks +7.65) · 2 name(s) marked open→close (per-name table). INO×3320 09:30 $1.34 → close $1.36 +66.40; NIQ×235 09:30 $18.60 → close $18.35 -58.75 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.16 | ▲ 09:30 equity $8,941.66 vs yday $8,835.61 (+106.05) | 09:30 open · cash $8.16 (unchanged overnight, no fees) · equity $8,941.66 vs prior close $8,835.61 (+106.05) · 2 name(s) re-marked at the open (per-name table). INO×3320 yday $1.36 → 09:30 $1.37 +33.20; NIQ×235 yday $18.35 → 09:30 $18.66 +72.85 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.16 | ▲ close $8,946.06 vs 09:30 $8,941.66 (session +4.40) | 16:00 close · cash $8.16 · equity $8,946.06 vs 09:30 $8,941.66 (+4.40; session marks +4.40) · 2 name(s) marked open→close (per-name table). INO×3320 09:30 $1.37 → close $1.36 -33.20; NIQ×235 09:30 $18.66 → close $18.82 +37.60 | — |

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
