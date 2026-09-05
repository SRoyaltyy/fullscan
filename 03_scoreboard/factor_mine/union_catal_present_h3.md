# Factor mine action — `union_catal_present_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ catal_present, no 🚨

Cash book **-7.86%** ($9,214) · signal-only (no cash/fees) was -10.11%. Starts YES **4/17**. Fills 5 · skips 7 · realized $-928.07.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `catal_present=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $4,528.46.

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
| 2026-08-31 | -5.85 | $39.19 | UEC×375, FCX×63 | — | — | $39.19 | $9,481.92 | $9,521.11 | UEC×375, FCX×63 | hard-red sit S=-5.85 |
| 2026-09-01 | -6.30 | $39.19 | UEC×375, FCX×63 | — | — | $39.19 | $9,115.17 | $9,154.36 | UEC×375, FCX×63 | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $39.19 | UEC×375, FCX×63 | — | FCX | $4,670.62 | $4,346.25 | $9,016.87 | UEC×375 | SELL FCX (dropped from list after 3 sess (min 3)) |
| 2026-09-03 | -0.90 | $4,670.62 | UEC×375 | CF | — | $127.15 | $9,092.68 | $9,219.83 | UEC×375, CF×34 | BUY CF x34 @ 133.57 |
| 2026-09-04 | — | $127.15 | UEC×375, CF×34 | — | UEC | $4,528.46 | $4,685.54 | $9,214.00 | CF×34 | SELL UEC (dropped from list after 5 sess (min 3)) |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-28 09:30 ET | **BUY** | `UEC` | 375 | $13.30 | $4.84 | — | $5,007.66 | union ∩ catal_present, no 🚨; gate catal_present=True; list ohlc_hot; ret5=+13.8; leftover $5000.00 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 catal🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FCX` | 63 | $78.83 | $2.18 | — | $39.19 | union ∩ catal_present, no 🚨; gate catal_present=True; list ohlc_hot; ret5=+15.3; leftover $5000.00 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 catal🟡 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `FCX` | 63 | $73.55 | $2.23 | $-337.05 | $4,670.62 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CF` | 34 | $133.57 | $2.09 | — | $127.15 | union ∩ catal_present, no 🚨; gate catal_present=True; list mover_buy; 🔵; ret5=+9.6; leftover $4670.62 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟢 catal🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `UEC` | 375 | $11.75 | $4.93 | $-591.02 | $4,528.46 | dropped from list after 5 sess (min 3) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-26 | `UEC` | no_price | no 09:30 open |
| 2026-08-31 | `UEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `UEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TXG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-04 | `CF` | min_hold | dropped but min-hold 1/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CF` | 34 | 2026-09-03 @ $133.57 | union ∩ catal_present, no 🚨; gate catal_present=True; list mover_buy; 🔵; ret5=+9.6; leftover $4670.62 |
