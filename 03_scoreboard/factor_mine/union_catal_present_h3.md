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

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-17 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-21 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-24 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-25 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-26 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-27 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-28 | `UEC` | 375 | — | $13.30 | +0.00 | $13.62 | +120.00 | +120.00 | +0.00 | +120.00 |
| 2026-08-28 | `FCX` | 63 | — | $78.83 | +0.00 | $78.42 | -25.83 | -25.83 | +0.00 | -25.83 |
| 2026-08-31 | `UEC` | 375 | $13.62 | $12.37 | -468.75 | $12.46 | +33.75 | -435.00 | -348.75 | -315.00 |
| 2026-08-31 | `FCX` | 63 | $78.42 | $76.10 | -146.16 | $76.34 | +15.12 | -131.04 | -171.99 | -156.87 |
| 2026-09-01 | `UEC` | 375 | $12.46 | $12.16 | -112.50 | $11.86 | -112.50 | -225.00 | -427.50 | -540.00 |
| 2026-09-01 | `FCX` | 63 | $76.34 | $76.32 | -1.26 | $74.09 | -140.49 | -141.75 | -158.13 | -298.62 |
| 2026-09-02 | `UEC` | 375 | $11.86 | $11.60 | -97.50 | $11.59 | -3.75 | -101.25 | -637.50 | -641.25 |
| 2026-09-02 | `FCX` | 63 | $74.09 | $73.55 | -34.02 | — | +0.00 | -34.02 | -332.64 | — |
| 2026-09-03 | `UEC` | 375 | $11.59 | $11.63 | +15.00 | $11.62 | -3.75 | +11.25 | -626.25 | -630.00 |
| 2026-09-03 | `CF` | 34 | — | $133.57 | +0.00 | $139.27 | +193.80 | +193.80 | +0.00 | +193.80 |
| 2026-09-04 | `UEC` | 375 | $11.62 | $11.75 | +48.75 | — | +0.00 | +48.75 | -581.25 | — |
| 2026-09-04 | `CF` | 34 | $139.27 | $138.60 | -22.78 | $137.81 | -26.86 | -49.64 | +171.02 | +144.16 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-21 | +3.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-24 | -5.17 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-25 | +1.80 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-26 | +2.02 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-27 | — | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-28 | +0.75 | $10,000.00 | — | $10,000.00 | +0.00 | +94.17 | UEC, FCX | — | $39.19 | $10,087.15 | UEC×375, FCX×63 |
| 2026-08-31 | -5.85 | $39.19 | UEC×375, FCX×63 | $9,472.24 | -614.91 | +48.87 | — | — | $39.19 | $9,521.11 | UEC×375, FCX×63 |
| 2026-09-01 | -6.30 | $39.19 | UEC×375, FCX×63 | $9,407.35 | -113.76 | -252.99 | — | — | $39.19 | $9,154.36 | UEC×375, FCX×63 |
| 2026-09-02 | -3.83 | $39.19 | UEC×375, FCX×63 | $9,022.84 | -131.52 | -3.75 | — | FCX | $4,670.62 | $9,016.87 | UEC×375 |
| 2026-09-03 | -0.90 | $4,670.62 | UEC×375 | $9,031.87 | +15.00 | +190.05 | CF | — | $127.15 | $9,219.83 | UEC×375, CF×34 |
| 2026-09-04 | — | $127.15 | UEC×375, CF×34 | $9,245.80 | +25.97 | -26.86 | — | UEC | $4,528.46 | $9,214.00 | CF×34 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `UEC` | 375 | $13.30 | $4.84 | — | $5,007.66 | — | union ∩ catal_present, no 🚨; gate catal_present=True; list ohlc_hot; ret5=+13.8; leftover $5000.00 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 catal🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FCX` | 63 | $78.83 | $2.18 | — | $39.19 | — | union ∩ catal_present, no 🚨; gate catal_present=True; list ohlc_hot; ret5=+15.3; leftover $5000.00 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 catal🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.19 | ▲ close $10,087.15 vs 09:30 $10,000.00 (session +94.17) | 16:00 close · cash $39.19 · equity $10,087.15 vs 09:30 $10,000.00 (+87.15; session marks +94.17) · 2 name(s) marked open→close (per-name table). UEC×375 09:30 $13.30 → close $13.62 +120.00; FCX×63 09:30 $78.83 → close $78.42 -25.83 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.19 | ▼ 09:30 equity $9,472.24 vs yday $10,087.15 (-614.91) | 09:30 open · cash $39.19 (unchanged overnight, no fees) · equity $9,472.24 vs prior close $10,087.15 (-614.91) · 2 name(s) re-marked at the open (per-name table). UEC×375 yday $13.62 → 09:30 $12.37 -468.75; FCX×63 yday $78.42 → 09:30 $76.10 -146.16 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.19 | ▲ close $9,521.11 vs 09:30 $9,472.24 (session +48.87) | 16:00 close · cash $39.19 · equity $9,521.11 vs 09:30 $9,472.24 (+48.87; session marks +48.87) · 2 name(s) marked open→close (per-name table). UEC×375 09:30 $12.37 → close $12.46 +33.75; FCX×63 09:30 $76.10 → close $76.34 +15.12 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.19 | ▼ 09:30 equity $9,407.35 vs yday $9,521.11 (-113.76) | 09:30 open · cash $39.19 (unchanged overnight, no fees) · equity $9,407.35 vs prior close $9,521.11 (-113.76) · 2 name(s) re-marked at the open (per-name table). UEC×375 yday $12.46 → 09:30 $12.16 -112.50; FCX×63 yday $76.34 → 09:30 $76.32 -1.26 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.19 | ▼ close $9,154.36 vs 09:30 $9,407.35 (session -252.99) | 16:00 close · cash $39.19 · equity $9,154.36 vs 09:30 $9,407.35 (-252.99; session marks -252.99) · 2 name(s) marked open→close (per-name table). UEC×375 09:30 $12.16 → close $11.86 -112.50; FCX×63 09:30 $76.32 → close $74.09 -140.49 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.19 | ▼ 09:30 equity $9,022.84 vs yday $9,154.36 (-131.52) | 09:30 open · cash $39.19 (unchanged overnight, no fees) · equity $9,022.84 vs prior close $9,154.36 (-131.52) · 2 name(s) re-marked at the open (per-name table). UEC×375 yday $11.86 → 09:30 $11.60 -97.50; FCX×63 yday $74.09 → 09:30 $73.55 -34.02 | — |
| 2026-09-02 09:30 ET | **SELL** | `FCX` | 63 | $73.55 | $2.23 | $-337.05 | $4,670.62 | ▼ -337.05 after sell → book $9,020.62; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,670.62 | ▼ close $9,016.87 vs 09:30 $9,022.84 (session -3.75) | 16:00 close · cash $4,670.62 · equity $9,016.87 vs 09:30 $9,022.84 (-5.97; session marks -3.75) · 1 name(s) marked open→close (per-name table). UEC×375 09:30 $11.60 → close $11.59 -3.75 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,670.62 | ▲ 09:30 equity $9,031.87 vs yday $9,016.87 (+15.00) | 09:30 open · cash $4,670.62 (unchanged overnight, no fees) · equity $9,031.87 vs prior close $9,016.87 (+15.00) · 1 name(s) re-marked at the open (per-name table). UEC×375 yday $11.59 → 09:30 $11.63 +15.00 | — |
| 2026-09-03 09:30 ET | **BUY** | `CF` | 34 | $133.57 | $2.09 | — | $127.15 | — | union ∩ catal_present, no 🚨; gate catal_present=True; list mover_buy; 🔵; ret5=+9.6; leftover $4670.62 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟢 catal🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $127.15 | ▲ close $9,219.83 vs 09:30 $9,031.87 (session +190.05) | 16:00 close · cash $127.15 · equity $9,219.83 vs 09:30 $9,031.87 (+187.95; session marks +190.05) · 2 name(s) marked open→close (per-name table). UEC×375 09:30 $11.63 → close $11.62 -3.75; CF×34 09:30 $133.57 → close $139.27 +193.80 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $127.15 | ▲ 09:30 equity $9,245.80 vs yday $9,219.83 (+25.97) | 09:30 open · cash $127.15 (unchanged overnight, no fees) · equity $9,245.80 vs prior close $9,219.83 (+25.97) · 2 name(s) re-marked at the open (per-name table). UEC×375 yday $11.62 → 09:30 $11.75 +48.75; CF×34 yday $139.27 → 09:30 $138.60 -22.78 | — |
| 2026-09-04 09:30 ET | **SELL** | `UEC` | 375 | $11.75 | $4.93 | $-591.02 | $4,528.46 | ▼ -591.02 after sell → book $9,240.86; vs 09:30 mark -4.93 | dropped from list after 5 sess (min 3) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,528.46 | ▼ close $9,214.00 vs 09:30 $9,245.80 (session -26.86) | 16:00 close · cash $4,528.46 · equity $9,214.00 vs 09:30 $9,245.80 (-31.80; session marks -26.86) · 1 name(s) marked open→close (per-name table). CF×34 09:30 $138.60 → close $137.81 -26.86 | — |

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
