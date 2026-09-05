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

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-24 | -5.17 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-25 | +1.80 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-26 | +2.02 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-27 | — | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-28 | +0.75 | $10,000.00 | — | $10,000.00 | +0.00 | UEC, FCX | — | $39.19 | $10,087.15 | UEC×375, FCX×63 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-31 | -5.85 | $39.19 | UEC×375, FCX×63 | $9,472.24 | -614.91 | — | — | $39.19 | $9,521.11 | UEC×375, FCX×63 | 09:30 open · cash $39.19 (unchanged overnight, no fees) · equity $9,472.24 vs prior close $10,087.15 (-614.91) because holdings re-marked: UEC×375 yday $13.62 → 09:30 $12.37 -468.75; FCX×63 yday $78.42 → 09:30 $76.10 -146.16 |
| 2026-09-01 | -6.30 | $39.19 | UEC×375, FCX×63 | $9,407.35 | -113.76 | — | — | $39.19 | $9,154.36 | UEC×375, FCX×63 | 09:30 open · cash $39.19 (unchanged overnight, no fees) · equity $9,407.35 vs prior close $9,521.11 (-113.76) because holdings re-marked: UEC×375 yday $12.46 → 09:30 $12.16 -112.50; FCX×63 yday $76.34 → 09:30 $76.32 -1.26 |
| 2026-09-02 | -3.83 | $39.19 | UEC×375, FCX×63 | $9,022.84 | -131.52 | — | FCX | $4,670.62 | $9,016.87 | UEC×375 | 09:30 open · cash $39.19 (unchanged overnight, no fees) · equity $9,022.84 vs prior close $9,154.36 (-131.52) because holdings re-marked: UEC×375 yday $11.86 → 09:30 $11.60 -97.50; FCX×63 yday $74.09 → 09:30 $73.55 -34.02 |
| 2026-09-03 | -0.90 | $4,670.62 | UEC×375 | $9,031.87 | +15.00 | CF | — | $127.15 | $9,219.83 | UEC×375, CF×34 | 09:30 open · cash $4,670.62 (unchanged overnight, no fees) · equity $9,031.87 vs prior close $9,016.87 (+15.00) because holdings re-marked: UEC×375 yday $11.59 → 09:30 $11.63 +15.00 |
| 2026-09-04 | — | $127.15 | UEC×375, CF×34 | $9,245.80 | +25.97 | — | UEC | $4,528.46 | $9,214.00 | CF×34 | 09:30 open · cash $127.15 (unchanged overnight, no fees) · equity $9,245.80 vs prior close $9,219.83 (+25.97) because holdings re-marked: UEC×375 yday $11.62 → 09:30 $11.75 +48.75; CF×34 yday $139.27 → 09:30 $138.60 -22.78 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `UEC` | 375 | $13.30 | $4.84 | — | $5,007.66 | — | union ∩ catal_present, no 🚨; gate catal_present=True; list ohlc_hot; ret5=+13.8; leftover $5000.00 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 catal🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FCX` | 63 | $78.83 | $2.18 | — | $39.19 | — | union ∩ catal_present, no 🚨; gate catal_present=True; list ohlc_hot; ret5=+15.3; leftover $5000.00 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 catal🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.19 | ▼ 09:30 equity $9,472.24 vs yday $10,087.15 (-614.91) | 09:30 open · cash $39.19 (unchanged overnight, no fees) · equity $9,472.24 vs prior close $10,087.15 (-614.91) because holdings re-marked: UEC×375 yday $13.62 → 09:30 $12.37 -468.75; FCX×63 yday $78.42 → 09:30 $76.10 -146.16 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.19 | ▼ 09:30 equity $9,407.35 vs yday $9,521.11 (-113.76) | 09:30 open · cash $39.19 (unchanged overnight, no fees) · equity $9,407.35 vs prior close $9,521.11 (-113.76) because holdings re-marked: UEC×375 yday $12.46 → 09:30 $12.16 -112.50; FCX×63 yday $76.34 → 09:30 $76.32 -1.26 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.19 | ▼ 09:30 equity $9,022.84 vs yday $9,154.36 (-131.52) | 09:30 open · cash $39.19 (unchanged overnight, no fees) · equity $9,022.84 vs prior close $9,154.36 (-131.52) because holdings re-marked: UEC×375 yday $11.86 → 09:30 $11.60 -97.50; FCX×63 yday $74.09 → 09:30 $73.55 -34.02 | — |
| 2026-09-02 09:30 ET | **SELL** | `FCX` | 63 | $73.55 | $2.23 | $-337.05 | $4,670.62 | ▼ -337.05 after sell → book $9,020.62; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,670.62 | ▲ 09:30 equity $9,031.87 vs yday $9,016.87 (+15.00) | 09:30 open · cash $4,670.62 (unchanged overnight, no fees) · equity $9,031.87 vs prior close $9,016.87 (+15.00) because holdings re-marked: UEC×375 yday $11.59 → 09:30 $11.63 +15.00 | — |
| 2026-09-03 09:30 ET | **BUY** | `CF` | 34 | $133.57 | $2.09 | — | $127.15 | — | union ∩ catal_present, no 🚨; gate catal_present=True; list mover_buy; 🔵; ret5=+9.6; leftover $4670.62 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟢 catal🟡 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $127.15 | ▲ 09:30 equity $9,245.80 vs yday $9,219.83 (+25.97) | 09:30 open · cash $127.15 (unchanged overnight, no fees) · equity $9,245.80 vs prior close $9,219.83 (+25.97) because holdings re-marked: UEC×375 yday $11.62 → 09:30 $11.75 +48.75; CF×34 yday $139.27 → 09:30 $138.60 -22.78 | — |
| 2026-09-04 09:30 ET | **SELL** | `UEC` | 375 | $11.75 | $4.93 | $-591.02 | $4,528.46 | ▼ -591.02 after sell → book $9,240.86; vs 09:30 mark -4.93 | dropped from list after 5 sess (min 3) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |

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
