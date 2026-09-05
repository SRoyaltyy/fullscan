# Factor mine action — `union_news_missing_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ news_missing, no 🚨

Cash book **+1.92%** ($10,192) · signal-only (no cash/fees) was +2.56%. Starts YES **11/17**. Fills 32 · skips 0 · realized $+192.43.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news=missing` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,192.44.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $97.53 | $10,055.59 | $10,153.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | BUY BTSG x20 @ 59.80; BUY IREN x27 @ 45.98; BUY TPG x24 @ 50.62; BUY TGTX x25 @ 49.70; BUY SLS x106 @ 11.70; BUY HIMS x42 @ 29.74; BUY INO x1543 @ 0.81; BUY TNDM x53 @ 23.33 |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $10,143.91 | $0.00 | $10,143.91 | — | SELL BTSG (dropped from list after 1 sess (min 1)); SELL IREN (dropped from list after 1 sess (min 1)); SELL TPG (dropped from list after 1 sess (min 1)); SELL TGTX (dropped from list after 1 sess (min 1)); SELL SLS (dropped from list after 1 sess (min 1)); SELL HIMS (dropped from list after 1 sess (min 1)); SELL INO (dropped from list after 1 sess (min 1)); SELL TNDM (dropped from list after 1 sess (min 1)) |
| 2026-08-17 | +2.25 | $10,143.91 | — | — | — | $10,143.91 | $0.00 | $10,143.91 | — | flat cash |
| 2026-08-18 | -6.20 | $10,143.91 | — | — | — | $10,143.91 | $0.00 | $10,143.91 | — | hard-red sit S=-6.20 |
| 2026-08-19 | -7.20 | $10,143.91 | — | — | — | $10,143.91 | $0.00 | $10,143.91 | — | hard-red sit S=-7.20 |
| 2026-08-20 | +1.12 | $10,143.91 | — | — | — | $10,143.91 | $0.00 | $10,143.91 | — | flat cash |
| 2026-08-21 | +3.25 | $10,143.91 | — | — | — | $10,143.91 | $0.00 | $10,143.91 | — | flat cash |
| 2026-08-24 | -5.17 | $10,143.91 | — | — | — | $10,143.91 | $0.00 | $10,143.91 | — | hard-red sit S=-5.17 |
| 2026-08-25 | +1.80 | $10,143.91 | — | — | — | $10,143.91 | $0.00 | $10,143.91 | — | flat cash |
| 2026-08-26 | +2.02 | $10,143.91 | — | — | — | $10,143.91 | $0.00 | $10,143.91 | — | flat cash |
| 2026-08-27 | — | $10,143.91 | — | CRK, MOS, SLI, GGB, MT, TX, ANET, DLO | — | $244.38 | $9,937.51 | $10,181.89 | CRK×89, MOS×51, SLI×489, GGB×286, MT×16, TX×22, ANET×6, DLO×81 | BUY CRK x89 @ 14.09; BUY MOS x51 @ 24.84; BUY SLI x489 @ 2.59; BUY GGB x286 @ 4.42; BUY MT x16 @ 75.12; BUY TX x22 @ 55.20; BUY ANET x6 @ 190.90; BUY DLO x81 @ 15.60 |
| 2026-08-28 | +0.75 | $244.38 | CRK×89, MOS×51, SLI×489, GGB×286, MT×16, TX×22, ANET×6, DLO×81 | — | CRK, MOS, SLI, GGB, MT, TX, ANET, DLO | $10,192.44 | $0.00 | $10,192.44 | — | SELL CRK (dropped from list after 1 sess (min 1)); SELL MOS (dropped from list after 1 sess (min 1)); SELL SLI (dropped from list after 1 sess (min 1)); SELL GGB (dropped from list after 1 sess (min 1)); SELL MT (dropped from list after 1 sess (min 1)); SELL TX (dropped from list after 1 sess (min 1)); SELL ANET (dropped from list after 1 sess (min 1)); SELL DLO (dropped from list after 1 sess (min 1)) |
| 2026-08-31 | -5.85 | $10,192.44 | — | — | — | $10,192.44 | $0.00 | $10,192.44 | — | hard-red sit S=-5.85 |
| 2026-09-01 | -6.30 | $10,192.44 | — | — | — | $10,192.44 | $0.00 | $10,192.44 | — | hard-red sit S=-6.30 |
| 2026-09-02 | -3.83 | $10,192.44 | — | — | — | $10,192.44 | $0.00 | $10,192.44 | — | hard-red sit S=-3.83 |
| 2026-09-03 | -0.90 | $10,192.44 | — | — | — | $10,192.44 | $0.00 | $10,192.44 | — | flat cash |
| 2026-09-04 | — | $10,192.44 | — | — | — | $10,192.44 | $0.00 | $10,192.44 | — | flat cash |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $1,288.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $2,476.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,801.68 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 25 | $47.27 | $2.08 | $-64.90 | $4,981.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $6,293.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $7,515.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $8,931.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $10,143.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 89 | $14.09 | $2.26 | — | $8,887.64 | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ret5=+1.1; leftover $1267.99 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 51 | $24.84 | $2.14 | — | $7,618.66 | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ret5=+13.0; leftover $1267.99 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 489 | $2.59 | $6.31 | — | $6,345.84 | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ret5=+4.2; leftover $1267.99 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 286 | $4.42 | $3.69 | — | $5,078.03 | union ∩ news_missing, no 🚨; gate news=missing; list mover_buy; 🔵; ret5=-8.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 16 | $75.12 | $2.04 | — | $3,874.07 | union ∩ news_missing, no 🚨; gate news=missing; list mover_buy; 🔵; ret5=-2.2; leftover $1267.99 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 22 | $55.20 | $2.06 | — | $2,657.62 | union ∩ news_missing, no 🚨; gate news=missing; list mover_buy; 🔵; ret5=+3.0; leftover $1267.99 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 6 | $190.90 | $2.01 | — | $1,510.21 | union ∩ news_missing, no 🚨; gate news=missing; list mover_buy; 🔵; ret5=-5.1; leftover $1267.99 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 81 | $15.60 | $2.23 | — | $244.38 | union ∩ news_missing, no 🚨; gate news=missing; list mover_buy; 🔵; ret5=+7.1; leftover $1267.99 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CRK` | 89 | $14.42 | $2.28 | $+24.83 | $1,525.47 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 51 | $24.00 | $2.16 | $-47.15 | $2,747.31 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 489 | $2.60 | $6.40 | $-7.82 | $4,012.31 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 286 | $4.57 | $3.75 | $+35.46 | $5,315.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 16 | $74.54 | $2.06 | $-13.38 | $6,506.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TX` | 22 | $55.25 | $2.08 | $-3.03 | $7,719.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 6 | $205.90 | $2.03 | $+85.96 | $8,952.96 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 81 | $15.33 | $2.26 | $-26.36 | $10,192.44 | dropped from list after 1 sess (min 1) | — |
