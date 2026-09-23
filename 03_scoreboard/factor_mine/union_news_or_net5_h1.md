# Factor mine action — `union_news_or_net5_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 OR headline🟢 and camera net ≥ 5

Cash book **-10.29%** ($8,971) · signal-only (no cash/fees) was -5.36%. Starts YES **1/28**. Fills 98 · skips 13 · realized $-1028.59.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-have: the morning news packet OR the prior-export headline is green.
- Must-have: camera net (+G −R) is at least 5.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by how many morning cameras are green vs red and keep the top 8.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a LONG sleeve: it buys shares and wants the price to go up.

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news_or_headline=True,cam_net_min=5` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,971.40.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `HLIT` | 379 | — | $13.18 | +0.00 | $13.92 | +280.46 | +280.46 | +0.00 | +280.46 |
| 2026-08-14 | `SNDK` | 3 | — | $1646.93 | +0.00 | $1641.11 | -17.46 | -17.46 | +0.00 | -17.46 |
| 2026-08-17 | `HLIT` | 379 | $13.92 | $13.84 | -30.32 | — | +0.00 | -30.32 | +250.14 | — |
| 2026-08-17 | `SNDK` | 3 | $1641.11 | $1700.74 | +178.90 | — | +0.00 | +178.90 | +161.44 | — |
| 2026-08-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 28 | — | $91.01 | +0.00 | $93.63 | +73.36 | +73.36 | +0.00 | +73.36 |
| 2026-08-20 | `APA` | 58 | — | $44.76 | +0.00 | $44.39 | -21.46 | -21.46 | +0.00 | -21.46 |
| 2026-08-20 | `AUTL` | 1052 | — | $2.47 | +0.00 | $2.46 | -10.52 | -10.52 | +0.00 | -10.52 |
| 2026-08-20 | `CRSP` | 44 | — | $58.73 | +0.00 | $58.12 | -26.84 | -26.84 | +0.00 | -26.84 |
| 2026-08-21 | `BHP` | 28 | $93.63 | $95.72 | +58.52 | — | +0.00 | +58.52 | +131.88 | — |
| 2026-08-21 | `APA` | 58 | $44.39 | $44.52 | +7.54 | — | +0.00 | +7.54 | -13.92 | — |
| 2026-08-21 | `AUTL` | 1052 | $2.46 | $2.47 | +10.52 | $2.41 | -63.12 | -52.60 | +0.00 | -63.12 |
| 2026-08-21 | `CRSP` | 44 | $58.12 | $59.72 | +70.40 | $59.50 | -9.68 | +60.72 | +43.56 | +33.88 |
| 2026-08-21 | `AU` | 7 | — | $119.43 | +0.00 | $121.22 | +12.53 | +12.53 | +0.00 | +12.53 |
| 2026-08-21 | `FUTU` | 7 | — | $115.18 | +0.00 | $123.64 | +59.22 | +59.22 | +0.00 | +59.22 |
| 2026-08-21 | `GRAL` | 11 | — | $78.88 | +0.00 | $79.54 | +7.26 | +7.26 | +0.00 | +7.26 |
| 2026-08-21 | `ABTC` | 102 | — | $8.66 | +0.00 | $7.93 | -74.46 | -74.46 | +0.00 | -74.46 |
| 2026-08-21 | `HIVE` | 273 | — | $3.24 | +0.00 | $3.03 | -57.33 | -57.33 | +0.00 | -57.33 |
| 2026-08-21 | `MARA` | 75 | — | $11.70 | +0.00 | $11.26 | -33.00 | -33.00 | +0.00 | -33.00 |
| 2026-08-24 | `AUTL` | 1052 | $2.41 | $2.40 | -10.52 | — | +0.00 | -10.52 | -73.64 | — |
| 2026-08-24 | `CRSP` | 44 | $59.50 | $58.75 | -33.00 | $57.08 | -73.70 | -106.70 | +0.88 | -72.82 |
| 2026-08-24 | `AU` | 7 | $121.22 | $120.51 | -4.97 | — | +0.00 | -4.97 | +7.56 | — |
| 2026-08-24 | `FUTU` | 7 | $123.64 | $121.00 | -18.48 | — | +0.00 | -18.48 | +40.74 | — |
| 2026-08-24 | `GRAL` | 11 | $79.54 | $81.87 | +25.63 | — | +0.00 | +25.63 | +32.89 | — |
| 2026-08-24 | `ABTC` | 102 | $7.93 | $8.00 | +7.14 | — | +0.00 | +7.14 | -67.32 | — |
| 2026-08-24 | `HIVE` | 273 | $3.03 | $2.99 | -10.92 | — | +0.00 | -10.92 | -68.25 | — |
| 2026-08-24 | `MARA` | 75 | $11.26 | $11.17 | -6.75 | — | +0.00 | -6.75 | -39.75 | — |
| 2026-08-25 | `CRSP` | 44 | $57.08 | $57.93 | +37.62 | — | +0.00 | +37.62 | -35.20 | — |
| 2026-08-25 | `AU` | 28 | — | $118.52 | +0.00 | $123.39 | +136.36 | +136.36 | +0.00 | +136.36 |
| 2026-08-25 | `FCX` | 44 | — | $77.13 | +0.00 | $79.91 | +122.32 | +122.32 | +0.00 | +122.32 |
| 2026-08-25 | `EZPW` | 97 | — | $35.05 | +0.00 | $35.23 | +17.46 | +17.46 | +0.00 | +17.46 |
| 2026-08-26 | `AU` | 28 | $123.39 | $119.80 | -100.52 | — | +0.00 | -100.52 | +35.84 | — |
| 2026-08-26 | `FCX` | 44 | $79.91 | $79.34 | -25.08 | — | +0.00 | -25.08 | +97.24 | — |
| 2026-08-26 | `EZPW` | 97 | $35.23 | $35.70 | +45.59 | — | +0.00 | +45.59 | +63.05 | — |
| 2026-08-26 | `FNV` | 19 | — | $267.02 | +0.00 | $267.37 | +6.65 | +6.65 | +0.00 | +6.65 |
| 2026-08-26 | `CM` | 43 | — | $118.50 | +0.00 | $118.20 | -12.90 | -12.90 | +0.00 | -12.90 |
| 2026-08-27 | `FNV` | 19 | $267.37 | $267.23 | -2.66 | — | +0.00 | -2.66 | +3.99 | — |
| 2026-08-27 | `CM` | 43 | $118.20 | $118.77 | +24.51 | $114.84 | -168.99 | -144.48 | +11.61 | -157.38 |
| 2026-08-27 | `ACMR` | 9 | — | $81.65 | +0.00 | $80.49 | -10.44 | -10.44 | +0.00 | -10.44 |
| 2026-08-27 | `GEN` | 25 | — | $29.83 | +0.00 | $30.50 | +16.75 | +16.75 | +0.00 | +16.75 |
| 2026-08-27 | `LRCX` | 2 | — | $318.88 | +0.00 | $318.58 | -0.60 | -0.60 | +0.00 | -0.60 |
| 2026-08-27 | `NVDA` | 3 | — | $222.86 | +0.00 | $227.98 | +15.36 | +15.36 | +0.00 | +15.36 |
| 2026-08-27 | `ADSK` | 2 | — | $261.47 | +0.00 | $270.58 | +18.22 | +18.22 | +0.00 | +18.22 |
| 2026-08-28 | `CM` | 43 | $114.84 | $115.66 | +35.26 | — | +0.00 | +35.26 | -122.12 | — |
| 2026-08-28 | `ACMR` | 9 | $80.49 | $79.27 | -10.98 | — | +0.00 | -10.98 | -21.42 | — |
| 2026-08-28 | `GEN` | 25 | $30.50 | $30.50 | +0.00 | — | +0.00 | +0.00 | +16.75 | — |
| 2026-08-28 | `LRCX` | 2 | $318.58 | $318.03 | -1.10 | — | +0.00 | -1.10 | -1.70 | — |
| 2026-08-28 | `NVDA` | 3 | $227.98 | $227.36 | -1.86 | — | +0.00 | -1.86 | +13.50 | — |
| 2026-08-28 | `ADSK` | 2 | $270.58 | $261.16 | -18.84 | $260.66 | -1.00 | -19.84 | -0.62 | -1.62 |
| 2026-08-28 | `KEYS` | 5 | — | $324.41 | +0.00 | $319.97 | -22.20 | -22.20 | +0.00 | -22.20 |
| 2026-08-28 | `SMTC` | 11 | — | $141.76 | +0.00 | $131.17 | -116.49 | -116.49 | +0.00 | -116.49 |
| 2026-08-28 | `CIEN` | 4 | — | $400.42 | +0.00 | $378.44 | -87.92 | -87.92 | +0.00 | -87.92 |
| 2026-08-28 | `MPWR` | 1 | — | $1306.03 | +0.00 | $1256.26 | -49.77 | -49.77 | +0.00 | -49.77 |
| 2026-08-28 | `DDOG` | 6 | — | $240.22 | +0.00 | $236.98 | -19.44 | -19.44 | +0.00 | -19.44 |
| 2026-08-28 | `SEDG` | 49 | — | $32.90 | +0.00 | $31.41 | -73.01 | -73.01 | +0.00 | -73.01 |
| 2026-08-31 | `ADSK` | 2 | $260.66 | $257.71 | -5.90 | — | +0.00 | -5.90 | -7.52 | — |
| 2026-08-31 | `KEYS` | 5 | $319.97 | $322.49 | +12.60 | — | +0.00 | +12.60 | -9.60 | — |
| 2026-08-31 | `SMTC` | 11 | $131.17 | $132.30 | +12.43 | — | +0.00 | +12.43 | -104.06 | — |
| 2026-08-31 | `CIEN` | 4 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -87.92 | — |
| 2026-08-31 | `MPWR` | 1 | $1256.26 | $1261.90 | +5.64 | — | +0.00 | +5.64 | -44.13 | — |
| 2026-08-31 | `DDOG` | 6 | $236.98 | $233.97 | -18.09 | — | +0.00 | -18.09 | -37.53 | — |
| 2026-08-31 | `SEDG` | 49 | $31.41 | $31.15 | -12.74 | — | +0.00 | -12.74 | -85.75 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 4 | — | $351.74 | +0.00 | $357.16 | +21.68 | +21.68 | +0.00 | +21.68 |
| 2026-09-03 | `DELL` | 3 | — | $486.31 | +0.00 | $516.39 | +90.24 | +90.24 | +0.00 | +90.24 |
| 2026-09-03 | `CXW` | 50 | — | $32.31 | +0.00 | $33.66 | +67.50 | +67.50 | +0.00 | +67.50 |
| 2026-09-03 | `FRNM` | 103 | — | $15.87 | +0.00 | $16.90 | +106.09 | +106.09 | +0.00 | +106.09 |
| 2026-09-03 | `MMED` | 69 | — | $23.88 | +0.00 | $23.84 | -2.76 | -2.76 | +0.00 | -2.76 |
| 2026-09-03 | `DE` | 2 | — | $703.25 | +0.00 | $694.41 | -17.68 | -17.68 | +0.00 | -17.68 |
| 2026-09-04 | `AVGO` | 4 | $357.16 | $359.70 | +10.16 | — | +0.00 | +10.16 | +31.84 | — |
| 2026-09-04 | `DELL` | 3 | $516.39 | $513.78 | -7.83 | — | +0.00 | -7.83 | +82.41 | — |
| 2026-09-04 | `CXW` | 50 | $33.66 | $33.46 | -10.00 | — | +0.00 | -10.00 | +57.50 | — |
| 2026-09-04 | `FRNM` | 103 | $16.90 | $16.40 | -51.50 | $16.31 | -9.27 | -60.77 | +54.59 | +45.32 |
| 2026-09-04 | `MMED` | 69 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.76 | — |
| 2026-09-04 | `DE` | 2 | $694.41 | $692.03 | -4.76 | — | +0.00 | -4.76 | -22.44 | — |
| 2026-09-04 | `CRM` | 10 | — | $263.36 | +0.00 | $259.23 | -41.30 | -41.30 | +0.00 | -41.30 |
| 2026-09-04 | `MRX` | 36 | — | $75.65 | +0.00 | $78.27 | +94.32 | +94.32 | +0.00 | +94.32 |
| 2026-09-04 | `BE` | 11 | — | $236.82 | +0.00 | $252.87 | +176.55 | +176.55 | +0.00 | +176.55 |
| 2026-09-08 | `FRNM` | 103 | $16.31 | $16.74 | +44.29 | — | +0.00 | +44.29 | +89.61 | — |
| 2026-09-08 | `CRM` | 10 | $259.23 | $253.72 | -55.10 | — | +0.00 | -55.10 | -96.40 | — |
| 2026-09-08 | `MRX` | 36 | $78.27 | $78.84 | +20.52 | — | +0.00 | +20.52 | +114.84 | — |
| 2026-09-08 | `BE` | 11 | $252.87 | $267.76 | +163.79 | — | +0.00 | +163.79 | +340.34 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 63 | — | $164.43 | +0.00 | $150.28 | -891.45 | -891.45 | +0.00 | -891.45 |
| 2026-09-14 | `ORCL` | 63 | $150.28 | $141.42 | -558.18 | — | +0.00 | -558.18 | -1449.63 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `WAY` | 114 | — | $26.27 | +0.00 | $26.59 | +36.48 | +36.48 | +0.00 | +36.48 |
| 2026-09-16 | `QCOM` | 15 | — | $189.17 | +0.00 | $184.84 | -64.95 | -64.95 | +0.00 | -64.95 |
| 2026-09-16 | `SM` | 74 | — | $39.99 | +0.00 | $38.16 | -135.42 | -135.42 | +0.00 | -135.42 |
| 2026-09-17 | `WAY` | 114 | $26.59 | $26.51 | -9.12 | — | +0.00 | -9.12 | +27.36 | — |
| 2026-09-17 | `QCOM` | 15 | $184.84 | $190.35 | +82.65 | — | +0.00 | +82.65 | +17.70 | — |
| 2026-09-17 | `SM` | 74 | $38.16 | $37.57 | -43.66 | — | +0.00 | -43.66 | -179.08 | — |
| 2026-09-17 | `SMTC` | 17 | — | $170.85 | +0.00 | $178.19 | +124.78 | +124.78 | +0.00 | +124.78 |
| 2026-09-17 | `AVTR` | 186 | — | $15.81 | +0.00 | $15.86 | +9.30 | +9.30 | +0.00 | +9.30 |
| 2026-09-17 | `GME` | 133 | — | $22.12 | +0.00 | $22.77 | +86.45 | +86.45 | +0.00 | +86.45 |
| 2026-09-18 | `SMTC` | 17 | $178.19 | $182.33 | +70.38 | — | +0.00 | +70.38 | +195.16 | — |
| 2026-09-18 | `AVTR` | 186 | $15.86 | $15.87 | +1.86 | — | +0.00 | +1.86 | +11.16 | — |
| 2026-09-18 | `GME` | 133 | $22.77 | $22.90 | +17.29 | $22.64 | -34.58 | -17.29 | +103.74 | +69.16 |
| 2026-09-18 | `TH` | 97 | — | $20.91 | +0.00 | $21.19 | +27.16 | +27.16 | +0.00 | +27.16 |
| 2026-09-18 | `RARE` | 137 | — | $14.79 | +0.00 | $14.51 | -38.36 | -38.36 | +0.00 | -38.36 |
| 2026-09-18 | `BHVN` | 144 | — | $14.07 | +0.00 | $13.62 | -64.80 | -64.80 | +0.00 | -64.80 |
| 2026-09-21 | `GME` | 133 | $22.64 | $22.78 | +18.62 | — | +0.00 | +18.62 | +87.78 | — |
| 2026-09-21 | `TH` | 97 | $21.19 | $21.65 | +44.62 | — | +0.00 | +44.62 | +71.78 | — |
| 2026-09-21 | `RARE` | 137 | $14.51 | $14.58 | +9.59 | — | +0.00 | +9.59 | -28.77 | — |
| 2026-09-21 | `BHVN` | 144 | $13.62 | $13.90 | +40.32 | — | +0.00 | +40.32 | -24.48 | — |
| 2026-09-21 | `VICR` | 19 | — | $230.25 | +0.00 | $223.90 | -120.65 | -120.65 | +0.00 | -120.65 |
| 2026-09-21 | `SMTC` | 23 | — | $190.30 | +0.00 | $177.37 | -297.39 | -297.39 | +0.00 | -297.39 |
| 2026-09-22 | `VICR` | 19 | $223.90 | $241.04 | +325.66 | — | +0.00 | +325.66 | +205.01 | — |
| 2026-09-22 | `SMTC` | 23 | $177.37 | $175.00 | -54.51 | — | +0.00 | -54.51 | -351.90 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +263.00 | HLIT, SNDK | — | $57.10 | $10,256.11 | HLIT×379, SNDK×3 |
| 2026-08-17 | +2.25 | $57.10 | HLIT×379, SNDK×3 | $10,404.70 | +148.59 | +0.00 | — | HLIT, SNDK | $10,397.65 | $10,397.65 | — |
| 2026-08-18 | -6.20 | $10,397.65 | — | $10,397.65 | +0.00 | +0.00 | — | — | $10,397.65 | $10,397.65 | — |
| 2026-08-19 | -7.20 | $10,397.65 | — | $10,397.65 | +0.00 | +0.00 | — | — | $10,397.65 | $10,397.65 | — |
| 2026-08-20 | +1.12 | $10,397.65 | — | $10,397.65 | +0.00 | +14.54 | BHP, APA, AUTL, CRSP | — | $50.80 | $10,392.26 | BHP×28, APA×58, AUTL×1052, CRSP×44 |
| 2026-08-21 | +3.25 | $50.80 | BHP×28, APA×58, AUTL×1052, CRSP×44 | $10,539.24 | +146.98 | -158.58 | AU, FUTU, GRAL, ABTC, HIVE, MARA | BHP, APA | $139.45 | $10,362.28 | AUTL×1052, CRSP×44, AU×7, FUTU×7, GRAL×11, ABTC×102, HIVE×273, MARA×75 |
| 2026-08-24 | -5.17 | $139.45 | AUTL×1052, CRSP×44, AU×7, FUTU×7, GRAL×11, ABTC×102, HIVE×273, MARA×75 | $10,310.41 | -51.87 | -73.70 | — | AUTL, AU, FUTU, GRAL, ABTC, HIVE, MARA | $7,697.41 | $10,208.71 | CRSP×44 |
| 2026-08-25 | +1.80 | $7,697.41 | CRSP×44 | $10,246.33 | +37.62 | +276.14 | AU, FCX, EZPW | CRSP | $125.57 | $10,513.84 | AU×28, FCX×44, EZPW×97 |
| 2026-08-26 | +2.02 | $125.57 | AU×28, FCX×44, EZPW×97 | $10,433.83 | -80.01 | -6.25 | FNV, CM | AU, FCX, EZPW | $254.19 | $10,416.82 | FNV×19, CM×43 |
| 2026-08-27 | — | $254.19 | FNV×19, CM×43 | $10,438.67 | +21.85 | -129.70 | ACMR, GEN, LRCX, NVDA, ADSK | FNV | $2,009.50 | $10,296.79 | CM×43, ACMR×9, GEN×25, LRCX×2, NVDA×3, ADSK×2 |
| 2026-08-28 | +0.75 | $2,009.50 | CM×43, ACMR×9, GEN×25, LRCX×2, NVDA×3, ADSK×2 | $10,299.27 | +2.48 | -369.83 | KEYS, SMTC, CIEN, MPWR, DDOG, SEDG | CM, ACMR, GEN, LRCX, NVDA | $611.92 | $9,906.95 | ADSK×2, KEYS×5, SMTC×11, CIEN×4, MPWR×1, DDOG×6, SEDG×49 |
| 2026-08-31 | -5.85 | $611.92 | ADSK×2, KEYS×5, SMTC×11, CIEN×4, MPWR×1, DDOG×6, SEDG×49 | $9,900.89 | -6.06 | +0.00 | — | ADSK, KEYS, SMTC, CIEN, MPWR, DDOG, SEDG | $9,886.58 | $9,886.58 | — |
| 2026-09-01 | -6.30 | $9,886.58 | — | $9,886.58 | -0.00 | +0.00 | — | — | $9,886.58 | $9,886.58 | — |
| 2026-09-02 | -3.83 | $9,886.58 | — | $9,886.58 | -0.00 | +0.00 | — | — | $9,886.58 | $9,886.58 | — |
| 2026-09-03 | -0.90 | $9,886.58 | — | $9,886.58 | -0.00 | +265.07 | AVGO, DELL, CXW, FRNM, MMED, DE | — | $703.72 | $10,139.01 | AVGO×4, DELL×3, CXW×50, FRNM×103, MMED×69, DE×2 |
| 2026-09-04 | +2.25 | $703.72 | AVGO×4, DELL×3, CXW×50, FRNM×103, MMED×69, DE×2 | $10,075.08 | -63.93 | +220.30 | CRM, MRX, BE | AVGO, DELL, CXW, MMED, DE | $407.28 | $10,278.80 | FRNM×103, CRM×10, MRX×36, BE×11 |
| 2026-09-08 | -11.47 | $407.28 | FRNM×103, CRM×10, MRX×36, BE×11 | $10,452.30 | +173.50 | +0.00 | — | FRNM, CRM, MRX, BE | $10,443.73 | $10,443.73 | — |
| 2026-09-09 | -13.95 | $10,443.73 | — | $10,443.73 | -0.00 | +0.00 | — | — | $10,443.73 | $10,443.73 | — |
| 2026-09-10 | -13.28 | $10,443.73 | — | $10,443.73 | -0.00 | +0.00 | — | — | $10,443.73 | $10,443.73 | — |
| 2026-09-11 | +0.50 | $10,443.73 | — | $10,443.73 | -0.00 | -891.45 | ORCL | — | $82.46 | $9,550.10 | ORCL×63 |
| 2026-09-14 | -11.00 | $82.46 | ORCL×63 | $8,991.92 | -558.18 | +0.00 | — | ORCL | $8,989.66 | $8,989.66 | — |
| 2026-09-15 | -3.84 | $8,989.66 | — | $8,989.66 | -0.00 | +0.00 | — | — | $8,989.66 | $8,989.66 | — |
| 2026-09-16 | +5.30 | $8,989.66 | — | $8,989.66 | -0.00 | -163.89 | WAY, QCOM, SM | — | $191.49 | $8,819.19 | WAY×114, QCOM×15, SM×74 |
| 2026-09-17 | +7.38 | $191.49 | WAY×114, QCOM×15, SM×74 | $8,849.06 | +29.87 | +220.53 | SMTC, AVTR, GME | WAY, QCOM, SM | $48.32 | $9,055.92 | SMTC×17, AVTR×186, GME×133 |
| 2026-09-18 | +4.86 | $48.32 | SMTC×17, AVTR×186, GME×133 | $9,145.45 | +89.53 | -110.58 | TH, RARE, BHVN | SMTC, AVTR | $7.39 | $9,023.09 | GME×133, TH×97, RARE×137, BHVN×144 |
| 2026-09-21 | +12.87 | $7.39 | GME×133, TH×97, RARE×137, BHVN×144 | $9,136.24 | +113.15 | -418.04 | VICR, SMTC | GME, TH, RARE, BHVN | $370.83 | $8,704.44 | VICR×19, SMTC×23 |
| 2026-09-22 | -0.50 | $370.83 | VICR×19, SMTC×23 | $8,975.59 | +271.15 | +0.00 | — | VICR, SMTC | $8,971.40 | $8,971.40 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 379 | $13.18 | $4.89 | — | $4,999.89 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $5000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SNDK` | 3 | $1646.93 | $2.00 | — | $57.10 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.4; leftover $5000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.10 | ▲ close $10,256.11 vs 09:30 $10,000.00 (session +263.00) | 16:00 close · cash $57.10 · equity $10,256.11 vs 09:30 $10,000.00 (+256.11; session marks +263.00) · 2 name(s) marked open→close (per-name table). HLIT×379 09:30 $13.18 → close $13.92 +280.46; SNDK×3 09:30 $1646.93 → close $1641.11 -17.46 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.10 | ▲ 09:30 equity $10,404.70 vs yday $10,256.11 (+148.59) | 09:30 open · cash $57.10 (unchanged overnight, no fees) · equity $10,404.70 vs prior close $10,256.11 (+148.59) · 2 name(s) re-marked at the open (per-name table). HLIT×379 yday $13.92 → 09:30 $13.84 -30.32; SNDK×3 yday $1641.11 → 09:30 $1700.74 +178.90 | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 379 | $13.84 | $4.99 | $+240.26 | $5,297.47 | ▲ +240.26 after sell → book $10,399.70; vs 09:30 mark -5.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SNDK` | 3 | $1700.74 | $2.05 | $+157.40 | $10,397.65 | ▲ +157.40 after sell → book $10,397.65; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,397.65 | ▲ close $10,397.65 vs 09:30 $10,404.70 (session +0.00) | 16:00 close · cash $10,397.65 · no lots left · equity $10,397.65. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,397.65 | ▲ 09:30 equity $10,397.65 vs yday $10,397.65 (+0.00) | 09:30 open · cash $10,397.65 · no holdings · equity $10,397.65 vs prior close $10,397.65 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,397.65 | ▲ close $10,397.65 vs 09:30 $10,397.65 (session +0.00) | 16:00 close · cash $10,397.65 · no lots left · equity $10,397.65. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,397.65 | ▲ 09:30 equity $10,397.65 vs yday $10,397.65 (+0.00) | 09:30 open · cash $10,397.65 · no holdings · equity $10,397.65 vs prior close $10,397.65 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,397.65 | ▲ close $10,397.65 vs 09:30 $10,397.65 (session +0.00) | 16:00 close · cash $10,397.65 · no lots left · equity $10,397.65. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,397.65 | ▲ 09:30 equity $10,397.65 vs yday $10,397.65 (+0.00) | 09:30 open · cash $10,397.65 · no holdings · equity $10,397.65 vs prior close $10,397.65 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 28 | $91.01 | $2.07 | — | $7,847.30 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $2599.41 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 58 | $44.76 | $2.16 | — | $5,249.05 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $2599.41 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 1052 | $2.47 | $13.57 | — | $2,637.04 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $2599.41 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 44 | $58.73 | $2.12 | — | $50.80 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $2599.41 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.80 | ▲ close $10,392.26 vs 09:30 $10,397.65 (session +14.54) | 16:00 close · cash $50.80 · equity $10,392.26 vs 09:30 $10,397.65 (-5.39; session marks +14.54) · 4 name(s) marked open→close (per-name table). BHP×28 09:30 $91.01 → close $93.63 +73.36; APA×58 09:30 $44.76 → close $44.39 -21.46; AUTL×1052 09:30 $2.47 → close $2.46 -10.52; CRSP×44 09:30 $58.73 → close $58.12 -26.84 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.80 | ▲ 09:30 equity $10,539.24 vs yday $10,392.26 (+146.98) | 09:30 open · cash $50.80 (unchanged overnight, no fees) · equity $10,539.24 vs prior close $10,392.26 (+146.98) · 4 name(s) re-marked at the open (per-name table). BHP×28 yday $93.63 → 09:30 $95.72 +58.52; APA×58 yday $44.39 → 09:30 $44.52 +7.54; AUTL×1052 yday $2.46 → 09:30 $2.47 +10.52; CRSP×44 yday $58.12 → 09:30 $59.72 +70.40 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 28 | $95.72 | $2.11 | $+127.70 | $2,728.86 | ▲ +127.70 after sell → book $10,537.14; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 58 | $44.52 | $2.19 | $-18.28 | $5,308.82 | ▼ -18.28 after sell → book $10,534.94; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 7 | $119.43 | $2.01 | — | $4,470.80 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $884.80 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 7 | $115.18 | $2.01 | — | $3,662.53 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $884.80 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 11 | $78.88 | $2.02 | — | $2,792.83 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $884.80 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 102 | $8.66 | $2.30 | — | $1,907.21 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $884.80 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 273 | $3.24 | $3.52 | — | $1,019.17 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $884.80 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 75 | $11.70 | $2.21 | — | $139.45 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $884.80 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $139.45 | ▼ close $10,362.28 vs 09:30 $10,539.24 (session -158.58) | 16:00 close · cash $139.45 · equity $10,362.28 vs 09:30 $10,539.24 (-176.96; session marks -158.58) · 8 name(s) marked open→close (per-name table). AUTL×1052 09:30 $2.47 → close $2.41 -63.12; CRSP×44 09:30 $59.72 → close $59.50 -9.68; AU×7 09:30 $119.43 → close $121.22 +12.53; FUTU×7 09:30 $115.18 → close $123.64 +59.22; GRAL×11 09:30 $78.88 → close $79.54 +7.26; ABTC×102 09:30 $8.66 → close $7.93 -74.46; HIVE×273 09:30 $3.24 → close $3.03 -57.33; MARA×75 09:30 $11.70 → close $11.26 -33.00 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $139.45 | ▼ 09:30 equity $10,310.41 vs yday $10,362.28 (-51.87) | 09:30 open · cash $139.45 (unchanged overnight, no fees) · equity $10,310.41 vs prior close $10,362.28 (-51.87) · 8 name(s) re-marked at the open (per-name table). AUTL×1052 yday $2.41 → 09:30 $2.40 -10.52; CRSP×44 yday $59.50 → 09:30 $58.75 -33.00; AU×7 yday $121.22 → 09:30 $120.51 -4.97; FUTU×7 yday $123.64 → 09:30 $121.00 -18.48; GRAL×11 yday $79.54 → 09:30 $81.87 +25.63; ABTC×102 yday $7.93 → 09:30 $8.00 +7.14; HIVE×273 yday $3.03 → 09:30 $2.99 -10.92; MARA×75 yday $11.26 → 09:30 $11.17 -6.75 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 1052 | $2.40 | $13.77 | $-100.98 | $2,650.49 | ▼ -100.98 after sell → book $10,296.65; vs 09:30 mark -13.76 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 7 | $120.51 | $2.03 | $+3.52 | $3,492.03 | ▲ +3.52 after sell → book $10,294.62; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 7 | $121.00 | $2.03 | $+36.70 | $4,337.00 | ▲ +36.70 after sell → book $10,292.59; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 11 | $81.87 | $2.04 | $+28.82 | $5,235.52 | ▲ +28.82 after sell → book $10,290.54; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 102 | $8.00 | $2.32 | $-71.94 | $6,049.20 | ▼ -71.94 after sell → book $10,288.22; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 273 | $2.99 | $3.58 | $-75.35 | $6,861.89 | ▼ -75.35 after sell → book $10,284.64; vs 09:30 mark -3.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 75 | $11.17 | $2.24 | $-44.20 | $7,697.41 | ▼ -44.20 after sell → book $10,282.41; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,697.41 | ▼ close $10,208.71 vs 09:30 $10,310.41 (session -73.70) | 16:00 close · cash $7,697.41 · equity $10,208.71 vs 09:30 $10,310.41 (-101.70; session marks -73.70) · 1 name(s) marked open→close (per-name table). CRSP×44 09:30 $58.75 → close $57.08 -73.70 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,697.41 | ▲ 09:30 equity $10,246.33 vs yday $10,208.71 (+37.62) | 09:30 open · cash $7,697.41 (unchanged overnight, no fees) · equity $10,246.33 vs prior close $10,208.71 (+37.62) · 1 name(s) re-marked at the open (per-name table). CRSP×44 yday $57.08 → 09:30 $57.93 +37.62 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 44 | $57.93 | $2.15 | $-39.47 | $10,244.17 | ▼ -39.47 after sell → book $10,244.17; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 28 | $118.52 | $2.07 | — | $6,923.54 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $3414.72 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 44 | $77.13 | $2.12 | — | $3,527.70 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $3414.72 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 97 | $35.05 | $2.28 | — | $125.57 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $3414.72 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $125.57 | ▲ close $10,513.84 vs 09:30 $10,246.33 (session +276.14) | 16:00 close · cash $125.57 · equity $10,513.84 vs 09:30 $10,246.33 (+267.51; session marks +276.14) · 3 name(s) marked open→close (per-name table). AU×28 09:30 $118.52 → close $123.39 +136.36; FCX×44 09:30 $77.13 → close $79.91 +122.32; EZPW×97 09:30 $35.05 → close $35.23 +17.46 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $125.57 | ▼ 09:30 equity $10,433.83 vs yday $10,513.84 (-80.01) | 09:30 open · cash $125.57 (unchanged overnight, no fees) · equity $10,433.83 vs prior close $10,513.84 (-80.01) · 3 name(s) re-marked at the open (per-name table). AU×28 yday $123.39 → 09:30 $119.80 -100.52; FCX×44 yday $79.91 → 09:30 $79.34 -25.08; EZPW×97 yday $35.23 → 09:30 $35.70 +45.59 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 28 | $119.80 | $2.11 | $+31.66 | $3,477.86 | ▲ +31.66 after sell → book $10,431.72; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 44 | $79.34 | $2.16 | $+92.96 | $6,966.66 | ▲ +92.96 after sell → book $10,429.56; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 97 | $35.70 | $2.32 | $+58.44 | $10,427.23 | ▲ +58.44 after sell → book $10,427.23; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 19 | $267.02 | $2.05 | — | $5,351.80 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $5213.62 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 43 | $118.50 | $2.12 | — | $254.19 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $5213.62 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $254.19 | ▼ close $10,416.82 vs 09:30 $10,433.83 (session -6.25) | 16:00 close · cash $254.19 · equity $10,416.82 vs 09:30 $10,433.83 (-17.01; session marks -6.25) · 2 name(s) marked open→close (per-name table). FNV×19 09:30 $267.02 → close $267.37 +6.65; CM×43 09:30 $118.50 → close $118.20 -12.90 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $254.19 | ▲ 09:30 equity $10,438.67 vs yday $10,416.82 (+21.85) | 09:30 open · cash $254.19 (unchanged overnight, no fees) · equity $10,438.67 vs prior close $10,416.82 (+21.85) · 2 name(s) re-marked at the open (per-name table). FNV×19 yday $267.37 → 09:30 $267.23 -2.66; CM×43 yday $118.20 → 09:30 $118.77 +24.51 | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 19 | $267.23 | $2.10 | $-0.15 | $5,329.46 | ▼ -0.15 after sell → book $10,436.57; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 9 | $81.65 | $2.02 | — | $4,592.59 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $761.35 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 25 | $29.83 | $2.06 | — | $3,844.78 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+7.6; leftover $761.35 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 2 | $318.88 | $2.00 | — | $3,205.02 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $761.35 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 3 | $222.86 | $2.00 | — | $2,534.44 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $761.35 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ADSK` | 2 | $261.47 | $2.00 | — | $2,009.50 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list overnight,overnight_mega; 🔵; ret5=+1.4; leftover $761.35 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,009.50 | ▼ close $10,296.79 vs 09:30 $10,438.67 (session -129.70) | 16:00 close · cash $2,009.50 · equity $10,296.79 vs 09:30 $10,438.67 (-141.88; session marks -129.70) · 6 name(s) marked open→close (per-name table). CM×43 09:30 $118.77 → close $114.84 -168.99; ACMR×9 09:30 $81.65 → close $80.49 -10.44; GEN×25 09:30 $29.83 → close $30.50 +16.75; LRCX×2 09:30 $318.88 → close $318.58 -0.60; NVDA×3 09:30 $222.86 → close $227.98 +15.36; ADSK×2 09:30 $261.47 → close $270.58 +18.22 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,009.50 | ▲ 09:30 equity $10,299.27 vs yday $10,296.79 (+2.48) | 09:30 open · cash $2,009.50 (unchanged overnight, no fees) · equity $10,299.27 vs prior close $10,296.79 (+2.48) · 6 name(s) re-marked at the open (per-name table). CM×43 yday $114.84 → 09:30 $115.66 +35.26; ACMR×9 yday $80.49 → 09:30 $79.27 -10.98; GEN×25 yday $30.50 → 09:30 $30.50 +0.00; LRCX×2 yday $318.58 → 09:30 $318.03 -1.10; NVDA×3 yday $227.98 → 09:30 $227.36 -1.86; ADSK×2 yday $270.58 → 09:30 $261.16 -18.84 | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 43 | $115.66 | $2.17 | $-126.41 | $6,980.72 | ▼ -126.41 after sell → book $10,297.11; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 9 | $79.27 | $2.04 | $-25.47 | $7,692.11 | ▼ -25.47 after sell → book $10,295.07; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 25 | $30.50 | $2.08 | $+12.60 | $8,452.52 | ▲ +12.60 after sell → book $10,292.98; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 2 | $318.03 | $2.02 | $-5.71 | $9,086.57 | ▼ -5.71 after sell → book $10,290.97; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 3 | $227.36 | $2.02 | $+9.48 | $9,766.63 | ▲ +9.48 after sell → book $10,288.95; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 5 | $324.41 | $2.00 | — | $8,142.57 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1627.77 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 11 | $141.76 | $2.02 | — | $6,581.19 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1627.77 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 4 | $400.42 | $2.00 | — | $4,977.51 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1627.77 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $3,669.49 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1627.77 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 6 | $240.22 | $2.01 | — | $2,226.16 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1627.77 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 49 | $32.90 | $2.14 | — | $611.92 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1627.77 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $611.92 | ▼ close $9,906.95 vs 09:30 $10,299.27 (session -369.83) | 16:00 close · cash $611.92 · equity $9,906.95 vs 09:30 $10,299.27 (-392.32; session marks -369.83) · 7 name(s) marked open→close (per-name table). ADSK×2 09:30 $261.16 → close $260.66 -1.00; KEYS×5 09:30 $324.41 → close $319.97 -22.20; SMTC×11 09:30 $141.76 → close $131.17 -116.49; CIEN×4 09:30 $400.42 → close $378.44 -87.92; MPWR×1 09:30 $1306.03 → close $1256.26 -49.77; DDOG×6 09:30 $240.22 → close $236.98 -19.44; SEDG×49 09:30 $32.90 → close $31.41 -73.01 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $611.92 | ▼ 09:30 equity $9,900.89 vs yday $9,906.95 (-6.06) | 09:30 open · cash $611.92 (unchanged overnight, no fees) · equity $9,900.89 vs prior close $9,906.95 (-6.06) · 7 name(s) re-marked at the open (per-name table). ADSK×2 yday $260.66 → 09:30 $257.71 -5.90; KEYS×5 yday $319.97 → 09:30 $322.49 +12.60; SMTC×11 yday $131.17 → 09:30 $132.30 +12.43; CIEN×4 yday $378.44 → 09:30 $378.44 +0.00; MPWR×1 yday $1256.26 → 09:30 $1261.90 +5.64; DDOG×6 yday $236.98 → 09:30 $233.97 -18.09; SEDG×49 yday $31.41 → 09:30 $31.15 -12.74 | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 2 | $257.71 | $2.02 | $-11.53 | $1,125.33 | ▼ -11.53 after sell → book $9,898.88; vs 09:30 mark -2.01 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 5 | $322.49 | $2.03 | $-13.63 | $2,735.75 | ▼ -13.63 after sell → book $9,896.85; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 11 | $132.30 | $2.04 | $-108.13 | $4,189.00 | ▼ -108.13 after sell → book $9,894.80; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 4 | $378.44 | $2.02 | $-91.95 | $5,700.74 | ▼ -91.95 after sell → book $9,892.78; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $6,960.63 | ▼ -48.14 after sell → book $9,890.77; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 6 | $233.97 | $2.03 | $-41.57 | $8,362.39 | ▼ -41.57 after sell → book $9,888.74; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 49 | $31.15 | $2.16 | $-90.05 | $9,886.58 | ▼ -90.05 after sell → book $9,886.58; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,886.58 | ▲ close $9,886.58 vs 09:30 $9,900.89 (session +0.00) | 16:00 close · cash $9,886.58 · no lots left · equity $9,886.58. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,886.58 | ▲ 09:30 equity $9,886.58 vs yday $9,886.58 (-0.00) | 09:30 open · cash $9,886.58 · no holdings · equity $9,886.58 vs prior close $9,886.58 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,886.58 | ▲ close $9,886.58 vs 09:30 $9,886.58 (session +0.00) | 16:00 close · cash $9,886.58 · no lots left · equity $9,886.58. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,886.58 | ▲ 09:30 equity $9,886.58 vs yday $9,886.58 (-0.00) | 09:30 open · cash $9,886.58 · no holdings · equity $9,886.58 vs prior close $9,886.58 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,886.58 | ▲ close $9,886.58 vs 09:30 $9,886.58 (session +0.00) | 16:00 close · cash $9,886.58 · no lots left · equity $9,886.58. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,886.58 | ▲ 09:30 equity $9,886.58 vs yday $9,886.58 (-0.00) | 09:30 open · cash $9,886.58 · no holdings · equity $9,886.58 vs prior close $9,886.58 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 4 | $351.74 | $2.00 | — | $8,477.61 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1647.76 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 3 | $486.31 | $2.00 | — | $7,016.69 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1647.76 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 50 | $32.31 | $2.14 | — | $5,399.05 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1647.76 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 103 | $15.87 | $2.30 | — | $3,762.14 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1647.76 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 69 | $23.88 | $2.20 | — | $2,112.22 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1647.76 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 2 | $703.25 | $2.00 | — | $703.72 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1647.76 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $703.72 | ▲ close $10,139.01 vs 09:30 $9,886.58 (session +265.07) | 16:00 close · cash $703.72 · equity $10,139.01 vs 09:30 $9,886.58 (+252.43; session marks +265.07) · 6 name(s) marked open→close (per-name table). AVGO×4 09:30 $351.74 → close $357.16 +21.68; DELL×3 09:30 $486.31 → close $516.39 +90.24; CXW×50 09:30 $32.31 → close $33.66 +67.50; FRNM×103 09:30 $15.87 → close $16.90 +106.09; MMED×69 09:30 $23.88 → close $23.84 -2.76; DE×2 09:30 $703.25 → close $694.41 -17.68 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $703.72 | ▼ 09:30 equity $10,075.08 vs yday $10,139.01 (-63.93) | 09:30 open · cash $703.72 (unchanged overnight, no fees) · equity $10,075.08 vs prior close $10,139.01 (-63.93) · 6 name(s) re-marked at the open (per-name table). AVGO×4 yday $357.16 → 09:30 $359.70 +10.16; DELL×3 yday $516.39 → 09:30 $513.78 -7.83; CXW×50 yday $33.66 → 09:30 $33.46 -10.00; FRNM×103 yday $16.90 → 09:30 $16.40 -51.50; MMED×69 yday $23.84 → 09:30 $23.84 +0.00; DE×2 yday $694.41 → 09:30 $692.03 -4.76 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 4 | $359.70 | $2.02 | $+27.81 | $2,140.50 | ▲ +27.81 after sell → book $10,073.06; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 3 | $513.78 | $2.02 | $+78.39 | $3,679.82 | ▲ +78.39 after sell → book $10,071.04; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 50 | $33.46 | $2.16 | $+53.20 | $5,350.66 | ▲ +53.20 after sell → book $10,068.88; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 69 | $23.84 | $2.22 | $-7.18 | $6,993.39 | ▼ -7.18 after sell → book $10,066.65; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 2 | $692.03 | $2.02 | $-26.45 | $8,375.44 | ▼ -26.45 after sell → book $10,064.64; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 10 | $263.36 | $2.02 | — | $5,739.82 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $2791.81 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 36 | $75.65 | $2.10 | — | $3,014.32 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $2791.81 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 11 | $236.82 | $2.02 | — | $407.28 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; ret5=+8.1; leftover $2791.81 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $407.28 | ▲ close $10,278.80 vs 09:30 $10,075.08 (session +220.30) | 16:00 close · cash $407.28 · equity $10,278.80 vs 09:30 $10,075.08 (+203.72; session marks +220.30) · 4 name(s) marked open→close (per-name table). FRNM×103 09:30 $16.40 → close $16.31 -9.27; CRM×10 09:30 $263.36 → close $259.23 -41.30; MRX×36 09:30 $75.65 → close $78.27 +94.32; BE×11 09:30 $236.82 → close $252.87 +176.55 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $407.28 | ▲ 09:30 equity $10,452.30 vs yday $10,278.80 (+173.50) | 09:30 open · cash $407.28 (unchanged overnight, no fees) · equity $10,452.30 vs prior close $10,278.80 (+173.50) · 4 name(s) re-marked at the open (per-name table). FRNM×103 yday $16.31 → 09:30 $16.74 +44.29; CRM×10 yday $259.23 → 09:30 $253.72 -55.10; MRX×36 yday $78.27 → 09:30 $78.84 +20.52; BE×11 yday $252.87 → 09:30 $267.76 +163.79 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 103 | $16.74 | $2.33 | $+84.98 | $2,129.17 | ▲ +84.98 after sell → book $10,449.97; vs 09:30 mark -2.33 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 10 | $253.72 | $2.05 | $-100.47 | $4,664.32 | ▼ -100.47 after sell → book $10,447.92; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 36 | $78.84 | $2.13 | $+110.61 | $7,500.43 | ▲ +110.61 after sell → book $10,445.79; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 11 | $267.76 | $2.06 | $+336.26 | $10,443.73 | ▲ +336.26 after sell → book $10,443.73; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,443.73 | ▲ close $10,443.73 vs 09:30 $10,452.30 (session +0.00) | 16:00 close · cash $10,443.73 · no lots left · equity $10,443.73. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,443.73 | ▲ 09:30 equity $10,443.73 vs yday $10,443.73 (-0.00) | 09:30 open · cash $10,443.73 · no holdings · equity $10,443.73 vs prior close $10,443.73 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,443.73 | ▲ close $10,443.73 vs 09:30 $10,443.73 (session +0.00) | 16:00 close · cash $10,443.73 · no lots left · equity $10,443.73. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,443.73 | ▲ 09:30 equity $10,443.73 vs yday $10,443.73 (-0.00) | 09:30 open · cash $10,443.73 · no holdings · equity $10,443.73 vs prior close $10,443.73 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,443.73 | ▲ close $10,443.73 vs 09:30 $10,443.73 (session +0.00) | 16:00 close · cash $10,443.73 · no lots left · equity $10,443.73. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,443.73 | ▲ 09:30 equity $10,443.73 vs yday $10,443.73 (-0.00) | 09:30 open · cash $10,443.73 · no holdings · equity $10,443.73 vs prior close $10,443.73 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 63 | $164.43 | $2.18 | — | $82.46 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $10443.73 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $82.46 | ▼ close $9,550.10 vs 09:30 $10,443.73 (session -891.45) | 16:00 close · cash $82.46 · equity $9,550.10 vs 09:30 $10,443.73 (-893.63; session marks -891.45) · 1 name(s) marked open→close (per-name table). ORCL×63 09:30 $164.43 → close $150.28 -891.45 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $82.46 | ▼ 09:30 equity $8,991.92 vs yday $9,550.10 (-558.18) | 09:30 open · cash $82.46 (unchanged overnight, no fees) · equity $8,991.92 vs prior close $9,550.10 (-558.18) · 1 name(s) re-marked at the open (per-name table). ORCL×63 yday $150.28 → 09:30 $141.42 -558.18 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 63 | $141.42 | $2.26 | $-1454.07 | $8,989.66 | ▼ -1,454.07 after sell → book $8,989.66; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,989.66 | ▲ close $8,989.66 vs 09:30 $8,991.92 (session +0.00) | 16:00 close · cash $8,989.66 · no lots left · equity $8,989.66. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,989.66 | ▲ 09:30 equity $8,989.66 vs yday $8,989.66 (-0.00) | 09:30 open · cash $8,989.66 · no holdings · equity $8,989.66 vs prior close $8,989.66 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,989.66 | ▲ close $8,989.66 vs 09:30 $8,989.66 (session +0.00) | 16:00 close · cash $8,989.66 · no lots left · equity $8,989.66. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,989.66 | ▲ 09:30 equity $8,989.66 vs yday $8,989.66 (-0.00) | 09:30 open · cash $8,989.66 · no holdings · equity $8,989.66 vs prior close $8,989.66 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 114 | $26.27 | $2.33 | — | $5,992.55 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2996.55 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 15 | $189.17 | $2.04 | — | $3,152.96 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $2996.55 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 74 | $39.99 | $2.21 | — | $191.49 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2996.55 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $191.49 | ▼ close $8,819.19 vs 09:30 $8,989.66 (session -163.89) | 16:00 close · cash $191.49 · equity $8,819.19 vs 09:30 $8,989.66 (-170.47; session marks -163.89) · 3 name(s) marked open→close (per-name table). WAY×114 09:30 $26.27 → close $26.59 +36.48; QCOM×15 09:30 $189.17 → close $184.84 -64.95; SM×74 09:30 $39.99 → close $38.16 -135.42 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $191.49 | ▲ 09:30 equity $8,849.06 vs yday $8,819.19 (+29.87) | 09:30 open · cash $191.49 (unchanged overnight, no fees) · equity $8,849.06 vs prior close $8,819.19 (+29.87) · 3 name(s) re-marked at the open (per-name table). WAY×114 yday $26.59 → 09:30 $26.51 -9.12; QCOM×15 yday $184.84 → 09:30 $190.35 +82.65; SM×74 yday $38.16 → 09:30 $37.57 -43.66 | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 114 | $26.51 | $2.38 | $+22.65 | $3,211.25 | ▲ +22.65 after sell → book $8,846.68; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 15 | $190.35 | $2.07 | $+13.60 | $6,064.44 | ▲ +13.60 after sell → book $8,844.62; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 74 | $37.57 | $2.25 | $-183.54 | $8,842.37 | ▼ -183.54 after sell → book $8,842.37; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 17 | $170.85 | $2.04 | — | $5,935.88 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $2947.46 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `AVTR` | 186 | $15.81 | $2.55 | — | $2,992.67 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $2947.46 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 133 | $22.12 | $2.39 | — | $48.32 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $2947.46 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.32 | ▲ close $9,055.92 vs 09:30 $8,849.06 (session +220.53) | 16:00 close · cash $48.32 · equity $9,055.92 vs 09:30 $8,849.06 (+206.86; session marks +220.53) · 3 name(s) marked open→close (per-name table). SMTC×17 09:30 $170.85 → close $178.19 +124.78; AVTR×186 09:30 $15.81 → close $15.86 +9.30; GME×133 09:30 $22.12 → close $22.77 +86.45 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.32 | ▲ 09:30 equity $9,145.45 vs yday $9,055.92 (+89.53) | 09:30 open · cash $48.32 (unchanged overnight, no fees) · equity $9,145.45 vs prior close $9,055.92 (+89.53) · 3 name(s) re-marked at the open (per-name table). SMTC×17 yday $178.19 → 09:30 $182.33 +70.38; AVTR×186 yday $15.86 → 09:30 $15.87 +1.86; GME×133 yday $22.77 → 09:30 $22.90 +17.29 | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 17 | $182.33 | $2.08 | $+191.04 | $3,145.86 | ▲ +191.04 after sell → book $9,143.38; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 186 | $15.87 | $2.60 | $+6.01 | $6,095.07 | ▲ +6.01 after sell → book $9,140.77; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 97 | $20.91 | $2.28 | — | $4,064.52 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $2031.69 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 137 | $14.79 | $2.40 | — | $2,035.89 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $2031.69 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 144 | $14.07 | $2.42 | — | $7.39 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $2031.69 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.39 | ▼ close $9,023.09 vs 09:30 $9,145.45 (session -110.58) | 16:00 close · cash $7.39 · equity $9,023.09 vs 09:30 $9,145.45 (-122.36; session marks -110.58) · 4 name(s) marked open→close (per-name table). GME×133 09:30 $22.90 → close $22.64 -34.58; TH×97 09:30 $20.91 → close $21.19 +27.16; RARE×137 09:30 $14.79 → close $14.51 -38.36; BHVN×144 09:30 $14.07 → close $13.62 -64.80 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.39 | ▲ 09:30 equity $9,136.24 vs yday $9,023.09 (+113.15) | 09:30 open · cash $7.39 (unchanged overnight, no fees) · equity $9,136.24 vs prior close $9,023.09 (+113.15) · 4 name(s) re-marked at the open (per-name table). GME×133 yday $22.64 → 09:30 $22.78 +18.62; TH×97 yday $21.19 → 09:30 $21.65 +44.62; RARE×137 yday $14.51 → 09:30 $14.58 +9.59; BHVN×144 yday $13.62 → 09:30 $13.90 +40.32 | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 133 | $22.78 | $2.44 | $+82.96 | $3,034.69 | ▲ +82.96 after sell → book $9,133.80; vs 09:30 mark -2.44 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 97 | $21.65 | $2.31 | $+67.19 | $5,132.43 | ▲ +67.19 after sell → book $9,131.49; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 137 | $14.58 | $2.44 | $-33.61 | $7,127.45 | ▼ -33.61 after sell → book $9,129.05; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 144 | $13.90 | $2.46 | $-29.36 | $9,126.59 | ▼ -29.36 after sell → book $9,126.59; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 19 | $230.25 | $2.05 | — | $4,749.79 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; ret5=+12.5; leftover $4563.29 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 23 | $190.30 | $2.06 | — | $370.83 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; ret5=+10.6; leftover $4563.29 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $370.83 | ▼ close $8,704.44 vs 09:30 $9,136.24 (session -418.04) | 16:00 close · cash $370.83 · equity $8,704.44 vs 09:30 $9,136.24 (-431.80; session marks -418.04) · 2 name(s) marked open→close (per-name table). VICR×19 09:30 $230.25 → close $223.90 -120.65; SMTC×23 09:30 $190.30 → close $177.37 -297.39 | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $370.83 | ▲ 09:30 equity $8,975.59 vs yday $8,704.44 (+271.15) | 09:30 open · cash $370.83 (unchanged overnight, no fees) · equity $8,975.59 vs prior close $8,704.44 (+271.15) · 2 name(s) re-marked at the open (per-name table). VICR×19 yday $223.90 → 09:30 $241.04 +325.66; SMTC×23 yday $177.37 → 09:30 $175.00 -54.51 | — |
| 2026-09-22 09:30 ET | **SELL** | `VICR` | 19 | $241.04 | $2.09 | $+200.87 | $4,948.50 | ▲ +200.87 after sell → book $8,973.50; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SMTC` | 23 | $175.00 | $2.10 | $-356.06 | $8,971.40 | ▼ -356.06 after sell → book $8,971.40; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,971.40 | ▲ close $8,971.40 vs 09:30 $8,975.59 (session +0.00) | 16:00 close · cash $8,971.40 · no lots left · equity $8,971.40. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `MU` | cash | leftover split 761.35 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 761.35 < 1 share @ 1746.53 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
