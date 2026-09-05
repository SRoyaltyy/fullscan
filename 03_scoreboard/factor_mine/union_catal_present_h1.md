# Factor mine action — `union_catal_present_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ catal_present, no 🚨

Cash book **-3.25%** ($9,675) · signal-only (no cash/fees) was +2.85%. Starts YES **4/17**. Fills 8 · skips 3 · realized $-324.97.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `catal_present=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,675.03.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-14 | +5.50 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-17 | +2.25 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-18 | -6.20 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | hard-red sit S=-6.20 |
| 2026-08-19 | -7.20 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | hard-red sit S=-7.20 |
| 2026-08-20 | +1.12 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-21 | +3.25 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-24 | -5.17 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | hard-red sit S=-5.17 |
| 2026-08-25 | +1.80 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-26 | +2.02 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-27 | — | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-28 | +0.75 | $10,000.00 | — | UEC, FCX | — | $39.19 | $10,047.96 | $10,087.15 | UEC×375, FCX×63 | BUY UEC x375 @ 13.30; BUY FCX x63 @ 78.83 |
| 2026-08-31 | -5.85 | $39.19 | UEC×375, FCX×63 | — | UEC, FCX | $9,465.08 | $0.00 | $9,465.08 | — | SELL UEC (dropped from list after 1 sess (min 1)); SELL FCX (dropped from list after 1 sess (min 1)) |
| 2026-09-01 | -6.30 | $9,465.08 | — | — | — | $9,465.08 | $0.00 | $9,465.08 | — | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $9,465.08 | — | — | — | $9,465.08 | $0.00 | $9,465.08 | — | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $9,465.08 | — | UEC, CF | — | $61.02 | $9,592.17 | $9,653.19 | UEC×406, CF×35 | BUY UEC x406 @ 11.63; BUY CF x35 @ 133.57 |
| 2026-09-04 | — | $61.02 | UEC×406, CF×35 | — | UEC, CF | $9,675.03 | $0.00 | $9,675.03 | — | SELL UEC (dropped from list after 1 sess (min 1)); SELL CF (dropped from list after 1 sess (min 1)) |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-28 09:30 ET | **BUY** | `UEC` | 375 | $13.30 | $4.84 | — | $5,007.66 | ▼ $9,995.16 (-4.84) | union ∩ catal_present, no 🚨; gate catal_present=True; list ohlc_hot; ret5=+13.8; leftover $5000.00 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 catal🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FCX` | 63 | $78.83 | $2.18 | — | $39.19 | ▼ $9,992.98 (-7.02) | union ∩ catal_present, no 🚨; gate catal_present=True; list ohlc_hot; ret5=+15.3; leftover $5000.00 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 catal🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `UEC` | 375 | $12.37 | $4.94 | $-358.52 | $4,673.01 | ▼ $9,467.31 (-532.69) | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `FCX` | 63 | $76.10 | $2.23 | $-176.40 | $9,465.08 | ▼ $9,465.08 (-534.92) | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `UEC` | 406 | $11.63 | $5.24 | — | $4,738.06 | ▼ $9,459.84 (-540.16) | union ∩ catal_present, no 🚨; gate catal_present=True; list ohlc_hot; ret5=+13.8; leftover $4732.54 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 catal🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CF` | 35 | $133.57 | $2.10 | — | $61.02 | ▼ $9,457.75 (-542.25) | union ∩ catal_present, no 🚨; gate catal_present=True; list mover_buy; 🔵; ret5=+9.6; leftover $4732.54 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟢 catal🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `UEC` | 406 | $11.75 | $5.34 | $+38.14 | $4,826.17 | ▼ $9,677.17 (-322.83) | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CF` | 35 | $138.60 | $2.14 | $+171.81 | $9,675.03 | ▼ $9,675.03 (-324.97) | dropped from list after 1 sess (min 1) | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-26 | `UEC` | no_price | no 09:30 open |
| 2026-09-01 | `TXG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `UEC` | hard_red | hard-red S=-3.83 sit; no new buys |
