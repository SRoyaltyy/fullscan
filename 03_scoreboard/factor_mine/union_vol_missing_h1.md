# Factor mine action — `union_vol_missing_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ vol_missing, no 🚨

Cash book **+1.44%** ($10,144) · signal-only (no cash/fees) was +1.88%. Starts YES **1/17**. Fills 16 · skips 0 · realized $+143.92.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `vol=missing` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,143.91.

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
| 2026-08-27 | — | $10,143.91 | — | — | — | $10,143.91 | $0.00 | $10,143.91 | — | flat cash |
| 2026-08-28 | +0.75 | $10,143.91 | — | — | — | $10,143.91 | $0.00 | $10,143.91 | — | flat cash |
| 2026-08-31 | -5.85 | $10,143.91 | — | — | — | $10,143.91 | $0.00 | $10,143.91 | — | hard-red sit S=-5.85 |
| 2026-09-01 | -6.30 | $10,143.91 | — | — | — | $10,143.91 | $0.00 | $10,143.91 | — | hard-red sit S=-6.30 |
| 2026-09-02 | -3.83 | $10,143.91 | — | — | — | $10,143.91 | $0.00 | $10,143.91 | — | hard-red sit S=-3.83 |
| 2026-09-03 | -0.90 | $10,143.91 | — | — | — | $10,143.91 | $0.00 | $10,143.91 | — | flat cash |
| 2026-09-04 | — | $10,143.91 | — | — | — | $10,143.91 | $0.00 | $10,143.91 | — | flat cash |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | ▼ $9,997.95 (-2.05) | union ∩ vol_missing, no 🚨; gate vol=missing; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | ▼ $9,995.88 (-4.12) | union ∩ vol_missing, no 🚨; gate vol=missing; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | ▼ $9,993.82 (-6.18) | union ∩ vol_missing, no 🚨; gate vol=missing; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | ▼ $9,991.75 (-8.25) | union ∩ vol_missing, no 🚨; gate vol=missing; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | ▼ $9,989.44 (-10.56) | union ∩ vol_missing, no 🚨; gate vol=missing; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | ▼ $9,987.33 (-12.67) | union ∩ vol_missing, no 🚨; gate vol=missing; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | ▼ $9,970.20 (-29.80) | union ∩ vol_missing, no 🚨; gate vol=missing; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | ▼ $9,968.05 (-31.95) | union ∩ vol_missing, no 🚨; gate vol=missing; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $1,288.46 | ▲ $10,176.05 (+176.05) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $2,476.80 | ▲ $10,173.96 (+173.96) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,801.68 | ▲ $10,171.88 (+171.88) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 25 | $47.27 | $2.08 | $-64.90 | $4,981.35 | ▲ $10,169.80 (+169.80) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $6,293.41 | ▲ $10,167.46 (+167.46) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $7,515.57 | ▲ $10,165.32 (+165.32) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $8,931.32 | ▲ $10,146.08 (+146.08) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $10,143.91 | ▲ $10,143.91 (+143.91) | dropped from list after 1 sess (min 1) | — |
