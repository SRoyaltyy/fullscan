# Factor mine action — `union_news_or_net5_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 OR headline🟢 and camera net ≥ 5

Cash book **-15.43%** ($8,457) · signal-only (no cash/fees) was -7.45%. Starts YES **1/26**. Fills 69 · skips 92 · realized $-1116.64.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news_or_headline=True,cam_net_min=5` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $13.54.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `HLIT` | 379 | — | $13.18 | +0.00 | $13.92 | +280.46 | +280.46 | +0.00 | +280.46 |
| 2026-08-14 | `SNDK` | 3 | — | $1646.93 | +0.00 | $1641.11 | -17.46 | -17.46 | +0.00 | -17.46 |
| 2026-08-17 | `HLIT` | 379 | $13.92 | $13.84 | -30.32 | $13.43 | -155.39 | -185.71 | +250.14 | +94.75 |
| 2026-08-17 | `SNDK` | 3 | $1641.11 | $1700.74 | +178.90 | $1786.85 | +258.32 | +437.22 | +161.44 | +419.76 |
| 2026-08-18 | `HLIT` | 379 | $13.43 | $12.93 | -189.50 | $12.73 | -75.80 | -265.30 | -94.75 | -170.55 |
| 2026-08-18 | `SNDK` | 3 | $1786.85 | $1677.54 | -327.93 | $1625.78 | -155.28 | -483.21 | +91.83 | -63.45 |
| 2026-08-19 | `HLIT` | 379 | $12.73 | $12.90 | +64.43 | — | +0.00 | +64.43 | -106.12 | — |
| 2026-08-19 | `SNDK` | 3 | $1625.78 | $1682.40 | +169.88 | — | +0.00 | +169.88 | +106.42 | — |
| 2026-08-20 | `BHP` | 21 | — | $91.01 | +0.00 | $93.63 | +55.02 | +55.02 | +0.00 | +55.02 |
| 2026-08-20 | `APA` | 44 | — | $44.76 | +0.00 | $44.39 | -16.28 | -16.28 | +0.00 | -16.28 |
| 2026-08-20 | `AUTL` | 808 | — | $2.47 | +0.00 | $2.46 | -8.08 | -8.08 | +0.00 | -8.08 |
| 2026-08-20 | `CRSP` | 34 | — | $58.73 | +0.00 | $58.12 | -20.74 | -20.74 | +0.00 | -20.74 |
| 2026-08-20 | `MRK` | 13 | — | $150.78 | +0.00 | $148.99 | -23.27 | -23.27 | +0.00 | -23.27 |
| 2026-08-21 | `BHP` | 21 | $93.63 | $95.72 | +43.89 | $97.03 | +27.51 | +71.40 | +98.91 | +126.42 |
| 2026-08-21 | `APA` | 44 | $44.39 | $44.52 | +5.72 | $43.39 | -49.72 | -44.00 | -10.56 | -60.28 |
| 2026-08-21 | `AUTL` | 808 | $2.46 | $2.47 | +8.08 | $2.41 | -48.48 | -40.40 | +0.00 | -48.48 |
| 2026-08-21 | `CRSP` | 34 | $58.12 | $59.72 | +54.40 | $59.50 | -7.48 | +46.92 | +33.66 | +26.18 |
| 2026-08-21 | `MRK` | 13 | $148.99 | $149.12 | +1.69 | $152.55 | +44.59 | +46.28 | -21.58 | +23.01 |
| 2026-08-21 | `ABTC` | 2 | — | $8.66 | +0.00 | $7.93 | -1.46 | -1.46 | +0.00 | -1.46 |
| 2026-08-24 | `BHP` | 21 | $97.03 | $97.31 | +5.88 | $97.13 | -3.78 | +2.10 | +132.30 | +128.52 |
| 2026-08-24 | `APA` | 44 | $43.39 | $42.93 | -20.24 | $42.96 | +1.32 | -18.92 | -80.52 | -79.20 |
| 2026-08-24 | `AUTL` | 808 | $2.41 | $2.40 | -8.08 | $2.34 | -48.48 | -56.56 | -56.56 | -105.04 |
| 2026-08-24 | `CRSP` | 34 | $59.50 | $58.75 | -25.50 | $57.08 | -56.95 | -82.45 | +0.68 | -56.27 |
| 2026-08-24 | `MRK` | 13 | $152.55 | $150.72 | -23.79 | $150.66 | -0.78 | -24.57 | -0.78 | -1.56 |
| 2026-08-24 | `ABTC` | 2 | $7.93 | $8.00 | +0.14 | $8.64 | +1.28 | +1.42 | -1.32 | -0.04 |
| 2026-08-25 | `BHP` | 21 | $97.13 | $95.86 | -26.67 | — | +0.00 | -26.67 | +101.85 | — |
| 2026-08-25 | `APA` | 44 | $42.96 | $41.38 | -69.52 | — | +0.00 | -69.52 | -148.72 | — |
| 2026-08-25 | `AUTL` | 808 | $2.34 | $2.38 | +32.32 | — | +0.00 | +32.32 | -72.72 | — |
| 2026-08-25 | `CRSP` | 34 | $57.08 | $57.93 | +29.07 | — | +0.00 | +29.07 | -27.20 | — |
| 2026-08-25 | `MRK` | 13 | $150.66 | $151.00 | +4.42 | — | +0.00 | +4.42 | +2.86 | — |
| 2026-08-25 | `ABTC` | 2 | $8.64 | $8.62 | -0.04 | $9.24 | +1.24 | +1.20 | -0.08 | +1.16 |
| 2026-08-25 | `AU` | 27 | — | $118.52 | +0.00 | $123.39 | +131.49 | +131.49 | +0.00 | +131.49 |
| 2026-08-25 | `FCX` | 42 | — | $77.13 | +0.00 | $79.91 | +116.76 | +116.76 | +0.00 | +116.76 |
| 2026-08-25 | `EZPW` | 93 | — | $35.05 | +0.00 | $35.23 | +16.74 | +16.74 | +0.00 | +16.74 |
| 2026-08-26 | `ABTC` | 2 | $9.24 | $8.84 | -0.80 | — | +0.00 | -0.80 | +0.36 | — |
| 2026-08-26 | `AU` | 27 | $123.39 | $119.80 | -96.93 | $118.11 | -45.63 | -142.56 | +34.56 | -11.07 |
| 2026-08-26 | `FCX` | 42 | $79.91 | $79.34 | -23.94 | $79.00 | -14.28 | -38.22 | +92.82 | +78.54 |
| 2026-08-26 | `EZPW` | 93 | $35.23 | $35.70 | +43.71 | $33.90 | -167.40 | -123.69 | +60.45 | -106.95 |
| 2026-08-26 | `ASST` | 1 | — | $20.72 | +0.00 | $21.50 | +0.78 | +0.78 | +0.00 | +0.78 |
| 2026-08-26 | `AMX` | 1 | — | $23.75 | +0.00 | $23.62 | -0.13 | -0.13 | +0.00 | -0.13 |
| 2026-08-27 | `AU` | 27 | $118.11 | $117.41 | -18.90 | $118.40 | +26.73 | +7.83 | -29.97 | -3.24 |
| 2026-08-27 | `FCX` | 42 | $79.00 | $78.83 | -7.14 | $78.42 | -17.22 | -24.36 | +71.40 | +54.18 |
| 2026-08-27 | `EZPW` | 93 | $33.90 | $33.50 | -37.20 | $34.41 | +84.63 | +47.43 | -144.15 | -59.52 |
| 2026-08-27 | `ASST` | 1 | $21.50 | $22.45 | +0.95 | $23.12 | +0.67 | +1.62 | +1.73 | +2.40 |
| 2026-08-27 | `AMX` | 1 | $23.62 | $23.77 | +0.15 | $23.50 | -0.27 | -0.12 | +0.02 | -0.25 |
| 2026-08-28 | `AU` | 27 | $118.40 | $119.19 | +21.33 | — | +0.00 | +21.33 | +18.09 | — |
| 2026-08-28 | `FCX` | 42 | $78.42 | $78.57 | +6.30 | — | +0.00 | +6.30 | +60.48 | — |
| 2026-08-28 | `EZPW` | 93 | $34.41 | $34.50 | +8.37 | — | +0.00 | +8.37 | -51.15 | — |
| 2026-08-28 | `ASST` | 1 | $23.12 | $22.50 | -0.62 | $21.74 | -0.76 | -1.38 | +1.78 | +1.02 |
| 2026-08-28 | `AMX` | 1 | $23.50 | $23.64 | +0.14 | $23.18 | -0.46 | -0.32 | -0.11 | -0.57 |
| 2026-08-28 | `KEYS` | 3 | — | $324.41 | +0.00 | $319.97 | -13.32 | -13.32 | +0.00 | -13.32 |
| 2026-08-28 | `SMTC` | 8 | — | $141.76 | +0.00 | $131.17 | -84.72 | -84.72 | +0.00 | -84.72 |
| 2026-08-28 | `CIEN` | 3 | — | $400.42 | +0.00 | $378.44 | -65.94 | -65.94 | +0.00 | -65.94 |
| 2026-08-28 | `DDOG` | 5 | — | $240.22 | +0.00 | $236.98 | -16.20 | -16.20 | +0.00 | -16.20 |
| 2026-08-28 | `PLAB` | 40 | — | $30.01 | +0.00 | $27.73 | -91.20 | -91.20 | +0.00 | -91.20 |
| 2026-08-28 | `ADSK` | 4 | — | $261.16 | +0.00 | $260.66 | -2.00 | -2.00 | +0.00 | -2.00 |
| 2026-08-28 | `SEDG` | 37 | — | $32.90 | +0.00 | $31.41 | -55.13 | -55.13 | +0.00 | -55.13 |
| 2026-08-31 | `ASST` | 1 | $21.74 | $22.54 | +0.80 | — | +0.00 | +0.80 | +1.82 | — |
| 2026-08-31 | `AMX` | 1 | $23.18 | $23.37 | +0.19 | — | +0.00 | +0.19 | -0.38 | — |
| 2026-08-31 | `KEYS` | 3 | $319.97 | $322.49 | +7.56 | $322.70 | +0.63 | +8.19 | -5.76 | -5.13 |
| 2026-08-31 | `SMTC` | 8 | $131.17 | $132.30 | +9.04 | $132.96 | +5.28 | +14.32 | -75.68 | -70.40 |
| 2026-08-31 | `CIEN` | 3 | $378.44 | $378.44 | +0.00 | $382.80 | +13.08 | +13.08 | -65.94 | -52.86 |
| 2026-08-31 | `DDOG` | 5 | $236.98 | $233.97 | -15.07 | $237.04 | +15.37 | +0.30 | -31.27 | -15.90 |
| 2026-08-31 | `PLAB` | 40 | $27.73 | $28.04 | +12.40 | $28.14 | +4.00 | +16.40 | -78.80 | -74.80 |
| 2026-08-31 | `ADSK` | 4 | $260.66 | $257.71 | -11.80 | $258.53 | +3.28 | -8.52 | -13.80 | -10.52 |
| 2026-08-31 | `SEDG` | 37 | $31.41 | $31.15 | -9.62 | $32.20 | +38.85 | +29.23 | -64.75 | -25.90 |
| 2026-09-01 | `KEYS` | 3 | $322.70 | $321.47 | -3.69 | $319.27 | -6.60 | -10.29 | -8.82 | -15.42 |
| 2026-09-01 | `SMTC` | 8 | $132.96 | $127.63 | -42.64 | $132.27 | +37.12 | -5.52 | -113.04 | -75.92 |
| 2026-09-01 | `CIEN` | 3 | $382.80 | $376.89 | -17.73 | $360.33 | -49.68 | -67.41 | -70.59 | -120.27 |
| 2026-09-01 | `DDOG` | 5 | $237.04 | $232.88 | -20.80 | $223.84 | -45.20 | -66.00 | -36.70 | -81.90 |
| 2026-09-01 | `PLAB` | 40 | $28.14 | $27.69 | -18.00 | $27.33 | -14.40 | -32.40 | -92.80 | -107.20 |
| 2026-09-01 | `ADSK` | 4 | $258.53 | $253.48 | -20.20 | $247.69 | -23.16 | -43.36 | -30.72 | -53.88 |
| 2026-09-01 | `SEDG` | 37 | $32.20 | $31.87 | -12.21 | $32.49 | +22.94 | +10.73 | -38.11 | -15.17 |
| 2026-09-02 | `KEYS` | 3 | $319.27 | $318.04 | -3.69 | — | +0.00 | -3.69 | -19.11 | — |
| 2026-09-02 | `SMTC` | 8 | $132.27 | $133.00 | +5.84 | — | +0.00 | +5.84 | -70.08 | — |
| 2026-09-02 | `CIEN` | 3 | $360.33 | $357.25 | -9.24 | — | +0.00 | -9.24 | -129.51 | — |
| 2026-09-02 | `DDOG` | 5 | $223.84 | $219.46 | -21.90 | — | +0.00 | -21.90 | -103.80 | — |
| 2026-09-02 | `PLAB` | 40 | $27.33 | $27.41 | +3.20 | — | +0.00 | +3.20 | -104.00 | — |
| 2026-09-02 | `ADSK` | 4 | $247.69 | $246.70 | -3.96 | — | +0.00 | -3.96 | -57.84 | — |
| 2026-09-02 | `SEDG` | 37 | $32.49 | $32.42 | -2.59 | — | +0.00 | -2.59 | -17.76 | — |
| 2026-09-03 | `AVGO` | 4 | — | $351.74 | +0.00 | $357.16 | +21.68 | +21.68 | +0.00 | +21.68 |
| 2026-09-03 | `DELL` | 3 | — | $486.31 | +0.00 | $516.39 | +90.24 | +90.24 | +0.00 | +90.24 |
| 2026-09-03 | `CXW` | 47 | — | $32.31 | +0.00 | $33.66 | +63.45 | +63.45 | +0.00 | +63.45 |
| 2026-09-03 | `FRNM` | 97 | — | $15.87 | +0.00 | $16.90 | +99.91 | +99.91 | +0.00 | +99.91 |
| 2026-09-03 | `MMED` | 64 | — | $23.88 | +0.00 | $23.84 | -2.56 | -2.56 | +0.00 | -2.56 |
| 2026-09-03 | `DE` | 2 | — | $703.25 | +0.00 | $694.41 | -17.68 | -17.68 | +0.00 | -17.68 |
| 2026-09-04 | `AVGO` | 4 | $357.16 | $359.70 | +10.16 | $357.90 | -7.20 | +2.96 | +31.84 | +24.64 |
| 2026-09-04 | `DELL` | 3 | $516.39 | $513.78 | -7.83 | $524.14 | +31.08 | +23.25 | +82.41 | +113.49 |
| 2026-09-04 | `CXW` | 47 | $33.66 | $33.46 | -9.40 | $34.71 | +58.75 | +49.35 | +54.05 | +112.80 |
| 2026-09-04 | `FRNM` | 97 | $16.90 | $16.40 | -48.50 | $16.31 | -8.73 | -57.23 | +51.41 | +42.68 |
| 2026-09-04 | `MMED` | 64 | $23.84 | $23.84 | +0.00 | $23.29 | -35.20 | -35.20 | -2.56 | -37.76 |
| 2026-09-04 | `DE` | 2 | $694.41 | $692.03 | -4.76 | $693.53 | +3.00 | -1.76 | -22.44 | -19.44 |
| 2026-09-04 | `HPE` | 1 | — | $53.85 | +0.00 | $52.00 | -1.85 | -1.85 | +0.00 | -1.85 |
| 2026-09-04 | `MRX` | 1 | — | $75.65 | +0.00 | $78.27 | +2.62 | +2.62 | +0.00 | +2.62 |
| 2026-09-08 | `AVGO` | 4 | $357.90 | $363.68 | +23.12 | $368.56 | +19.52 | +42.64 | +47.76 | +67.28 |
| 2026-09-08 | `DELL` | 3 | $524.14 | $521.15 | -8.97 | $533.88 | +38.19 | +29.22 | +104.52 | +142.71 |
| 2026-09-08 | `CXW` | 47 | $34.71 | $34.49 | -10.34 | $35.05 | +26.32 | +15.98 | +102.46 | +128.78 |
| 2026-09-08 | `FRNM` | 97 | $16.31 | $16.74 | +41.71 | $15.99 | -72.75 | -31.04 | +84.39 | +11.64 |
| 2026-09-08 | `MMED` | 64 | $23.29 | $23.16 | -8.32 | $23.32 | +10.24 | +1.92 | -46.08 | -35.84 |
| 2026-09-08 | `DE` | 2 | $693.53 | $687.21 | -12.64 | $680.73 | -12.96 | -25.60 | -32.08 | -45.04 |
| 2026-09-08 | `HPE` | 1 | $52.00 | $52.29 | +0.29 | $56.03 | +3.74 | +4.03 | -1.56 | +2.18 |
| 2026-09-08 | `MRX` | 1 | $78.27 | $78.84 | +0.57 | $76.71 | -2.13 | -1.56 | +3.19 | +1.06 |
| 2026-09-09 | `AVGO` | 4 | $368.56 | $366.23 | -9.32 | — | +0.00 | -9.32 | +57.96 | — |
| 2026-09-09 | `DELL` | 3 | $533.88 | $538.47 | +13.77 | — | +0.00 | +13.77 | +156.48 | — |
| 2026-09-09 | `CXW` | 47 | $35.05 | $35.09 | +1.88 | — | +0.00 | +1.88 | +130.66 | — |
| 2026-09-09 | `FRNM` | 97 | $15.99 | $15.96 | -2.91 | — | +0.00 | -2.91 | +8.73 | — |
| 2026-09-09 | `MMED` | 64 | $23.32 | $23.22 | -6.40 | — | +0.00 | -6.40 | -42.24 | — |
| 2026-09-09 | `DE` | 2 | $680.73 | $681.32 | +1.18 | — | +0.00 | +1.18 | -43.86 | — |
| 2026-09-09 | `HPE` | 1 | $56.03 | $56.94 | +0.91 | $58.90 | +1.96 | +2.87 | +3.09 | +5.05 |
| 2026-09-09 | `MRX` | 1 | $76.71 | $76.60 | -0.11 | $75.72 | -0.88 | -0.99 | +0.95 | +0.07 |
| 2026-09-10 | `HPE` | 1 | $58.90 | $57.80 | -1.10 | — | +0.00 | -1.10 | +3.95 | — |
| 2026-09-10 | `MRX` | 1 | $75.72 | $75.00 | -0.72 | — | +0.00 | -0.72 | -0.65 | — |
| 2026-09-11 | `ORCL` | 28 | — | $164.43 | +0.00 | $150.28 | -396.20 | -396.20 | +0.00 | -396.20 |
| 2026-09-11 | `BTI` | 85 | — | $56.03 | +0.00 | $55.24 | -67.15 | -67.15 | +0.00 | -67.15 |
| 2026-09-14 | `ORCL` | 28 | $150.28 | $141.42 | -248.08 | $144.79 | +94.36 | -153.72 | -644.28 | -549.92 |
| 2026-09-14 | `BTI` | 85 | $55.24 | $57.12 | +159.80 | $57.29 | +14.45 | +174.25 | +92.65 | +107.10 |
| 2026-09-15 | `ORCL` | 28 | $144.79 | $143.46 | -37.24 | $140.35 | -87.08 | -124.32 | -587.16 | -674.24 |
| 2026-09-15 | `BTI` | 85 | $57.29 | $56.46 | -70.55 | $56.52 | +5.10 | -65.45 | +36.55 | +41.65 |
| 2026-09-16 | `ORCL` | 28 | $140.35 | $140.03 | -8.96 | — | +0.00 | -8.96 | -683.20 | — |
| 2026-09-16 | `BTI` | 85 | $56.52 | $56.54 | +1.70 | — | +0.00 | +1.70 | +43.35 | — |
| 2026-09-16 | `WAY` | 84 | — | $26.27 | +0.00 | $26.59 | +26.88 | +26.88 | +0.00 | +26.88 |
| 2026-09-16 | `QCOM` | 11 | — | $189.17 | +0.00 | $184.84 | -47.63 | -47.63 | +0.00 | -47.63 |
| 2026-09-16 | `SM` | 55 | — | $39.99 | +0.00 | $38.16 | -100.65 | -100.65 | +0.00 | -100.65 |
| 2026-09-16 | `AMX` | 95 | — | $23.18 | +0.00 | $22.98 | -19.00 | -19.00 | +0.00 | -19.00 |
| 2026-09-17 | `WAY` | 84 | $26.59 | $26.51 | -6.72 | $26.51 | +0.00 | -6.72 | +20.16 | +20.16 |
| 2026-09-17 | `QCOM` | 11 | $184.84 | $190.35 | +60.61 | $188.71 | -18.04 | +42.57 | +12.98 | -5.06 |
| 2026-09-17 | `SM` | 55 | $38.16 | $37.57 | -32.45 | $36.97 | -33.00 | -65.45 | -133.10 | -166.10 |
| 2026-09-17 | `AMX` | 95 | $22.98 | $23.09 | +10.45 | $23.03 | -5.70 | +4.75 | -8.55 | -14.25 |
| 2026-09-17 | `AVTR` | 3 | — | $15.81 | +0.00 | $15.86 | +0.15 | +0.15 | +0.00 | +0.15 |
| 2026-09-17 | `GME` | 2 | — | $22.12 | +0.00 | $22.77 | +1.30 | +1.30 | +0.00 | +1.30 |
| 2026-09-18 | `WAY` | 84 | $26.51 | $26.95 | +36.96 | $25.66 | -108.36 | -71.40 | +57.12 | -51.24 |
| 2026-09-18 | `QCOM` | 11 | $188.71 | $191.34 | +28.93 | $177.72 | -149.82 | -120.89 | +23.87 | -125.95 |
| 2026-09-18 | `SM` | 55 | $36.97 | $36.87 | -5.50 | $36.97 | +5.50 | +0.00 | -171.60 | -166.10 |
| 2026-09-18 | `AMX` | 95 | $23.03 | $22.90 | -12.35 | $22.43 | -44.65 | -57.00 | -26.60 | -71.25 |
| 2026-09-18 | `AVTR` | 3 | $15.86 | $15.87 | +0.03 | $15.52 | -1.05 | -1.02 | +0.18 | -0.87 |
| 2026-09-18 | `GME` | 2 | $22.77 | $22.90 | +0.26 | $22.64 | -0.52 | -0.26 | +1.56 | +1.04 |
| 2026-09-18 | `TH` | 1 | — | $20.91 | +0.00 | $21.19 | +0.28 | +0.28 | +0.00 | +0.28 |
| 2026-09-18 | `RARE` | 2 | — | $14.79 | +0.00 | $14.51 | -0.56 | -0.56 | +0.00 | -0.56 |
| 2026-09-18 | `BHVN` | 2 | — | $14.07 | +0.00 | $13.62 | -0.90 | -0.90 | +0.00 | -0.90 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +263.00 | HLIT, SNDK | — | $57.10 | $10,256.11 | HLIT×379, SNDK×3 |
| 2026-08-17 | +2.25 | $57.10 | HLIT×379, SNDK×3 | $10,404.70 | +148.59 | +102.93 | — | — | $57.10 | $10,507.62 | HLIT×379, SNDK×3 |
| 2026-08-18 | -6.20 | $57.10 | HLIT×379, SNDK×3 | $9,990.19 | -517.43 | -231.08 | — | — | $57.10 | $9,759.11 | HLIT×379, SNDK×3 |
| 2026-08-19 | -7.20 | $57.10 | HLIT×379, SNDK×3 | $9,993.42 | +234.31 | +0.00 | — | HLIT, SNDK | $9,986.38 | $9,986.38 | — |
| 2026-08-20 | +1.12 | $9,986.38 | — | $9,986.38 | -0.00 | -13.35 | BHP, APA, AUTL, CRSP, MRK | — | $134.29 | $9,954.31 | BHP×21, APA×44, AUTL×808, CRSP×34, MRK×13 |
| 2026-08-21 | +3.25 | $134.29 | BHP×21, APA×44, AUTL×808, CRSP×34, MRK×13 | $10,068.09 | +113.78 | -35.04 | ABTC | — | $116.79 | $10,032.87 | BHP×21, APA×44, AUTL×808, CRSP×34, MRK×13, ABTC×2 |
| 2026-08-24 | -5.17 | $116.79 | BHP×21, APA×44, AUTL×808, CRSP×34, MRK×13, ABTC×2 | $9,961.28 | -71.59 | -107.39 | — | — | $116.79 | $9,853.89 | BHP×21, APA×44, AUTL×808, CRSP×34, MRK×13, ABTC×2 |
| 2026-08-25 | +1.80 | $116.79 | BHP×21, APA×44, AUTL×808, CRSP×34, MRK×13, ABTC×2 | $9,823.47 | -30.42 | +266.23 | AU, FCX, EZPW | BHP, APA, AUTL, CRSP, MRK | $81.65 | $10,064.27 | ABTC×2, AU×27, FCX×42, EZPW×93 |
| 2026-08-26 | +2.02 | $81.65 | ABTC×2, AU×27, FCX×42, EZPW×93 | $9,986.31 | -77.96 | -226.66 | ASST, AMX | ABTC | $54.21 | $9,759.00 | AU×27, FCX×42, EZPW×93, ASST×1, AMX×1 |
| 2026-08-27 | — | $54.21 | AU×27, FCX×42, EZPW×93, ASST×1, AMX×1 | $9,696.86 | -62.14 | +94.54 | — | — | $54.21 | $9,791.40 | AU×27, FCX×42, EZPW×93, ASST×1, AMX×1 |
| 2026-08-28 | +0.75 | $54.21 | AU×27, FCX×42, EZPW×93, ASST×1, AMX×1 | $9,826.92 | +35.52 | -329.73 | KEYS, SMTC, CIEN, DDOG, PLAB, ADSK, SEDG | AU, FCX, EZPW | $1,787.97 | $9,476.39 | ASST×1, AMX×1, KEYS×3, SMTC×8, CIEN×3, DDOG×5, PLAB×40, ADSK×4, SEDG×37 |
| 2026-08-31 | -5.85 | $1,787.97 | ASST×1, AMX×1, KEYS×3, SMTC×8, CIEN×3, DDOG×5, PLAB×40, ADSK×4, SEDG×37 | $9,469.88 | -6.51 | +80.49 | — | ASST, AMX | $1,833.37 | $9,549.87 | KEYS×3, SMTC×8, CIEN×3, DDOG×5, PLAB×40, ADSK×4, SEDG×37 |
| 2026-09-01 | -6.30 | $1,833.37 | KEYS×3, SMTC×8, CIEN×3, DDOG×5, PLAB×40, ADSK×4, SEDG×37 | $9,414.60 | -135.27 | -78.98 | — | — | $1,833.37 | $9,335.62 | KEYS×3, SMTC×8, CIEN×3, DDOG×5, PLAB×40, ADSK×4, SEDG×37 |
| 2026-09-02 | -3.83 | $1,833.37 | KEYS×3, SMTC×8, CIEN×3, DDOG×5, PLAB×40, ADSK×4, SEDG×37 | $9,303.28 | -32.34 | +0.00 | — | KEYS, SMTC, CIEN, DDOG, PLAB, ADSK, SEDG | $9,288.91 | $9,288.91 | — |
| 2026-09-03 | -0.90 | $9,288.91 | — | $9,288.91 | +0.00 | +255.04 | AVGO, DELL, CXW, FRNM, MMED, DE | — | $417.65 | $9,531.36 | AVGO×4, DELL×3, CXW×47, FRNM×97, MMED×64, DE×2 |
| 2026-09-04 | +2.25 | $417.65 | AVGO×4, DELL×3, CXW×47, FRNM×97, MMED×64, DE×2 | $9,471.03 | -60.33 | +42.47 | HPE, MRX | — | $286.85 | $9,512.20 | AVGO×4, DELL×3, CXW×47, FRNM×97, MMED×64, DE×2, HPE×1, MRX×1 |
| 2026-09-08 | -11.47 | $286.85 | AVGO×4, DELL×3, CXW×47, FRNM×97, MMED×64, DE×2, HPE×1, MRX×1 | $9,537.62 | +25.42 | +10.17 | — | — | $286.85 | $9,547.79 | AVGO×4, DELL×3, CXW×47, FRNM×97, MMED×64, DE×2, HPE×1, MRX×1 |
| 2026-09-09 | -13.95 | $286.85 | AVGO×4, DELL×3, CXW×47, FRNM×97, MMED×64, DE×2, HPE×1, MRX×1 | $9,546.79 | -1.00 | +1.08 | — | AVGO, DELL, CXW, FRNM, MMED, DE | $9,400.52 | $9,535.14 | HPE×1, MRX×1 |
| 2026-09-10 | -13.28 | $9,400.52 | HPE×1, MRX×1 | $9,533.32 | -1.82 | +0.00 | — | HPE, MRX | $9,531.95 | $9,531.95 | — |
| 2026-09-11 | +0.50 | $9,531.95 | — | $9,531.95 | -0.00 | -463.35 | ORCL, BTI | — | $161.04 | $9,064.28 | ORCL×28, BTI×85 |
| 2026-09-14 | -11.00 | $161.04 | ORCL×28, BTI×85 | $8,976.00 | -88.28 | +108.81 | — | — | $161.04 | $9,084.81 | ORCL×28, BTI×85 |
| 2026-09-15 | -3.84 | $161.04 | ORCL×28, BTI×85 | $8,977.02 | -107.79 | -81.98 | — | — | $161.04 | $8,895.04 | ORCL×28, BTI×85 |
| 2026-09-16 | +5.30 | $161.04 | ORCL×28, BTI×85 | $8,887.78 | -7.26 | -140.40 | WAY, QCOM, SM, AMX | ORCL, BTI | $185.57 | $8,734.27 | WAY×84, QCOM×11, SM×55, AMX×95 |
| 2026-09-17 | +7.38 | $185.57 | WAY×84, QCOM×11, SM×55, AMX×95 | $8,766.16 | +31.89 | -55.29 | AVTR, GME | — | $92.97 | $8,709.94 | WAY×84, QCOM×11, SM×55, AMX×95, AVTR×3, GME×2 |
| 2026-09-18 | +4.86 | $92.97 | WAY×84, QCOM×11, SM×55, AMX×95, AVTR×3, GME×2 | $8,758.27 | +48.33 | -300.08 | TH, RARE, BHVN | — | $13.54 | $8,457.39 | WAY×84, QCOM×11, SM×55, AMX×95, AVTR×3, GME×2, TH×1, RARE×2, BHVN×2 |

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
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.10 | ▲ close $10,507.62 vs 09:30 $10,404.70 (session +102.93) | 16:00 close · cash $57.10 · equity $10,507.62 vs 09:30 $10,404.70 (+102.92; session marks +102.93) · 2 name(s) marked open→close (per-name table). HLIT×379 09:30 $13.84 → close $13.43 -155.39; SNDK×3 09:30 $1700.74 → close $1786.85 +258.32 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.10 | ▼ 09:30 equity $9,990.19 vs yday $10,507.62 (-517.43) | 09:30 open · cash $57.10 (unchanged overnight, no fees) · equity $9,990.19 vs prior close $10,507.62 (-517.43) · 2 name(s) re-marked at the open (per-name table). HLIT×379 yday $13.43 → 09:30 $12.93 -189.50; SNDK×3 yday $1786.85 → 09:30 $1677.54 -327.93 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.10 | ▼ close $9,759.11 vs 09:30 $9,990.19 (session -231.08) | 16:00 close · cash $57.10 · equity $9,759.11 vs 09:30 $9,990.19 (-231.08; session marks -231.08) · 2 name(s) marked open→close (per-name table). HLIT×379 09:30 $12.93 → close $12.73 -75.80; SNDK×3 09:30 $1677.54 → close $1625.78 -155.28 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.10 | ▲ 09:30 equity $9,993.42 vs yday $9,759.11 (+234.31) | 09:30 open · cash $57.10 (unchanged overnight, no fees) · equity $9,993.42 vs prior close $9,759.11 (+234.31) · 2 name(s) re-marked at the open (per-name table). HLIT×379 yday $12.73 → 09:30 $12.90 +64.43; SNDK×3 yday $1625.78 → 09:30 $1682.40 +169.88 | — |
| 2026-08-19 09:30 ET | **SELL** | `HLIT` | 379 | $12.90 | $4.99 | $-116.00 | $4,941.21 | ▼ -116.00 after sell → book $9,988.43; vs 09:30 mark -4.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `SNDK` | 3 | $1682.40 | $2.05 | $+102.38 | $9,986.38 | ▲ +102.38 after sell → book $9,986.38; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,986.38 | ▲ close $9,986.38 vs 09:30 $9,993.42 (session +0.00) | 16:00 close · cash $9,986.38 · no lots left · equity $9,986.38. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,986.38 | ▲ 09:30 equity $9,986.38 vs yday $9,986.38 (-0.00) | 09:30 open · cash $9,986.38 · no holdings · equity $9,986.38 vs prior close $9,986.38 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 21 | $91.01 | $2.05 | — | $8,073.11 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1997.28 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 44 | $44.76 | $2.12 | — | $6,101.55 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1997.28 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 808 | $2.47 | $10.42 | — | $4,095.37 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1997.28 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 34 | $58.73 | $2.09 | — | $2,096.46 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1997.28 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRK` | 13 | $150.78 | $2.03 | — | $134.29 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list oppset; 🔵; ⚪; ret5=+14.5; leftover $1997.28 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $134.29 | ▼ close $9,954.31 vs 09:30 $9,986.38 (session -13.35) | 16:00 close · cash $134.29 · equity $9,954.31 vs 09:30 $9,986.38 (-32.07; session marks -13.35) · 5 name(s) marked open→close (per-name table). BHP×21 09:30 $91.01 → close $93.63 +55.02; APA×44 09:30 $44.76 → close $44.39 -16.28; AUTL×808 09:30 $2.47 → close $2.46 -8.08; CRSP×34 09:30 $58.73 → close $58.12 -20.74; MRK×13 09:30 $150.78 → close $148.99 -23.27 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $134.29 | ▲ 09:30 equity $10,068.09 vs yday $9,954.31 (+113.78) | 09:30 open · cash $134.29 (unchanged overnight, no fees) · equity $10,068.09 vs prior close $9,954.31 (+113.78) · 5 name(s) re-marked at the open (per-name table). BHP×21 yday $93.63 → 09:30 $95.72 +43.89; APA×44 yday $44.39 → 09:30 $44.52 +5.72; AUTL×808 yday $2.46 → 09:30 $2.47 +8.08; CRSP×34 yday $58.12 → 09:30 $59.72 +54.40; MRK×13 yday $148.99 → 09:30 $149.12 +1.69 | — |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 2 | $8.66 | $0.18 | — | $116.79 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $22.38 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $116.79 | ▼ close $10,032.87 vs 09:30 $10,068.09 (session -35.04) | 16:00 close · cash $116.79 · equity $10,032.87 vs 09:30 $10,068.09 (-35.22; session marks -35.04) · 6 name(s) marked open→close (per-name table). BHP×21 09:30 $95.72 → close $97.03 +27.51; APA×44 09:30 $44.52 → close $43.39 -49.72; AUTL×808 09:30 $2.47 → close $2.41 -48.48; CRSP×34 09:30 $59.72 → close $59.50 -7.48; MRK×13 09:30 $149.12 → close $152.55 +44.59; ABTC×2 09:30 $8.66 → close $7.93 -1.46 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $116.79 | ▼ 09:30 equity $9,961.28 vs yday $10,032.87 (-71.59) | 09:30 open · cash $116.79 (unchanged overnight, no fees) · equity $9,961.28 vs prior close $10,032.87 (-71.59) · 6 name(s) re-marked at the open (per-name table). BHP×21 yday $97.03 → 09:30 $97.31 +5.88; APA×44 yday $43.39 → 09:30 $42.93 -20.24; AUTL×808 yday $2.41 → 09:30 $2.40 -8.08; CRSP×34 yday $59.50 → 09:30 $58.75 -25.50; MRK×13 yday $152.55 → 09:30 $150.72 -23.79; ABTC×2 yday $7.93 → 09:30 $8.00 +0.14 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $116.79 | ▼ close $9,853.89 vs 09:30 $9,961.28 (session -107.39) | 16:00 close · cash $116.79 · equity $9,853.89 vs 09:30 $9,961.28 (-107.39; session marks -107.39) · 6 name(s) marked open→close (per-name table). BHP×21 09:30 $97.31 → close $97.13 -3.78; APA×44 09:30 $42.93 → close $42.96 +1.32; AUTL×808 09:30 $2.40 → close $2.34 -48.48; CRSP×34 09:30 $58.75 → close $57.08 -56.95; MRK×13 09:30 $150.72 → close $150.66 -0.78; ABTC×2 09:30 $8.00 → close $8.64 +1.28 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $116.79 | ▼ 09:30 equity $9,823.47 vs yday $9,853.89 (-30.42) | 09:30 open · cash $116.79 (unchanged overnight, no fees) · equity $9,823.47 vs prior close $9,853.89 (-30.42) · 6 name(s) re-marked at the open (per-name table). BHP×21 yday $97.13 → 09:30 $95.86 -26.67; APA×44 yday $42.96 → 09:30 $41.38 -69.52; AUTL×808 yday $2.34 → 09:30 $2.38 +32.32; CRSP×34 yday $57.08 → 09:30 $57.93 +29.07; MRK×13 yday $150.66 → 09:30 $151.00 +4.42; ABTC×2 yday $8.64 → 09:30 $8.62 -0.04 | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 21 | $95.86 | $2.08 | $+97.72 | $2,127.77 | ▲ +97.72 after sell → book $9,821.39; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 44 | $41.38 | $2.15 | $-152.99 | $3,946.34 | ▼ -152.99 after sell → book $9,819.24; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 808 | $2.38 | $10.57 | $-93.72 | $5,858.81 | ▼ -93.72 after sell → book $9,808.67; vs 09:30 mark -10.57 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 34 | $57.93 | $2.12 | $-31.41 | $7,826.31 | ▼ -31.41 after sell → book $9,806.55; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRK` | 13 | $151.00 | $2.05 | $-1.22 | $9,787.26 | ▼ -1.22 after sell → book $9,804.50; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 27 | $118.52 | $2.07 | — | $6,585.15 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $3262.42 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 42 | $77.13 | $2.12 | — | $3,343.57 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $3262.42 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 93 | $35.05 | $2.27 | — | $81.65 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,oppset; 🔵; ⚪; ret5=+19.7; leftover $3262.42 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $81.65 | ▲ close $10,064.27 vs 09:30 $9,823.47 (session +266.23) | 16:00 close · cash $81.65 · equity $10,064.27 vs 09:30 $9,823.47 (+240.80; session marks +266.23) · 4 name(s) marked open→close (per-name table). ABTC×2 09:30 $8.62 → close $9.24 +1.24; AU×27 09:30 $118.52 → close $123.39 +131.49; FCX×42 09:30 $77.13 → close $79.91 +116.76; EZPW×93 09:30 $35.05 → close $35.23 +16.74 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.65 | ▼ 09:30 equity $9,986.31 vs yday $10,064.27 (-77.96) | 09:30 open · cash $81.65 (unchanged overnight, no fees) · equity $9,986.31 vs prior close $10,064.27 (-77.96) · 4 name(s) re-marked at the open (per-name table). ABTC×2 yday $9.24 → 09:30 $8.84 -0.80; AU×27 yday $123.39 → 09:30 $119.80 -96.93; FCX×42 yday $79.91 → 09:30 $79.34 -23.94; EZPW×93 yday $35.23 → 09:30 $35.70 +43.71 | — |
| 2026-08-26 09:30 ET | **SELL** | `ABTC` | 2 | $8.84 | $0.20 | $-0.02 | $99.13 | ▼ -0.02 after sell → book $9,986.11; vs 09:30 mark -0.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `ASST` | 1 | $20.72 | $0.21 | — | $78.20 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list oppset; 🔵; ret5=+67.1; leftover $33.04 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `AMX` | 1 | $23.75 | $0.24 | — | $54.21 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list oppset; 🔵; ret5=+0.5; leftover $33.04 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $54.21 | ▼ close $9,759.00 vs 09:30 $9,986.31 (session -226.66) | 16:00 close · cash $54.21 · equity $9,759.00 vs 09:30 $9,986.31 (-227.31; session marks -226.66) · 5 name(s) marked open→close (per-name table). AU×27 09:30 $119.80 → close $118.11 -45.63; FCX×42 09:30 $79.34 → close $79.00 -14.28; EZPW×93 09:30 $35.70 → close $33.90 -167.40; ASST×1 09:30 $20.72 → close $21.50 +0.78; AMX×1 09:30 $23.75 → close $23.62 -0.13 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.21 | ▼ 09:30 equity $9,696.86 vs yday $9,759.00 (-62.14) | 09:30 open · cash $54.21 (unchanged overnight, no fees) · equity $9,696.86 vs prior close $9,759.00 (-62.14) · 5 name(s) re-marked at the open (per-name table). AU×27 yday $118.11 → 09:30 $117.41 -18.90; FCX×42 yday $79.00 → 09:30 $78.83 -7.14; EZPW×93 yday $33.90 → 09:30 $33.50 -37.20; ASST×1 yday $21.50 → 09:30 $22.45 +0.95; AMX×1 yday $23.62 → 09:30 $23.77 +0.15 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $54.21 | ▲ close $9,791.40 vs 09:30 $9,696.86 (session +94.54) | 16:00 close · cash $54.21 · equity $9,791.40 vs 09:30 $9,696.86 (+94.54; session marks +94.54) · 5 name(s) marked open→close (per-name table). AU×27 09:30 $117.41 → close $118.40 +26.73; FCX×42 09:30 $78.83 → close $78.42 -17.22; EZPW×93 09:30 $33.50 → close $34.41 +84.63; ASST×1 09:30 $22.45 → close $23.12 +0.67; AMX×1 09:30 $23.77 → close $23.50 -0.27 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.21 | ▲ 09:30 equity $9,826.92 vs yday $9,791.40 (+35.52) | 09:30 open · cash $54.21 (unchanged overnight, no fees) · equity $9,826.92 vs prior close $9,791.40 (+35.52) · 5 name(s) re-marked at the open (per-name table). AU×27 yday $118.40 → 09:30 $119.19 +21.33; FCX×42 yday $78.42 → 09:30 $78.57 +6.30; EZPW×93 yday $34.41 → 09:30 $34.50 +8.37; ASST×1 yday $23.12 → 09:30 $22.50 -0.62; AMX×1 yday $23.50 → 09:30 $23.64 +0.14 | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 27 | $119.19 | $2.11 | $+13.91 | $3,270.23 | ▲ +13.91 after sell → book $9,824.81; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 42 | $78.57 | $2.15 | $+56.21 | $6,568.02 | ▲ +56.21 after sell → book $9,822.66; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 93 | $34.50 | $2.31 | $-55.73 | $9,774.21 | ▼ -55.73 after sell → book $9,820.35; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 3 | $324.41 | $2.00 | — | $8,798.98 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1221.78 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $141.76 | $2.01 | — | $7,662.89 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,yday_mover,mover_buy,oppset; 🔵; ⚪; ret5=+14.1; leftover $1221.78 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,459.63 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1221.78 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $5,256.52 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1221.78 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PLAB` | 40 | $30.01 | $2.11 | — | $4,054.01 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list oppset; 🔵; ret5=-0.9; leftover $1221.78 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $3,007.37 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list earn_react; ret5=+7.8; leftover $1221.78 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 37 | $32.90 | $2.10 | — | $1,787.97 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1221.78 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,787.97 | ▼ close $9,476.39 vs 09:30 $9,826.92 (session -329.73) | 16:00 close · cash $1,787.97 · equity $9,476.39 vs 09:30 $9,826.92 (-350.53; session marks -329.73) · 9 name(s) marked open→close (per-name table). ASST×1 09:30 $22.50 → close $21.74 -0.76; AMX×1 09:30 $23.64 → close $23.18 -0.46; KEYS×3 09:30 $324.41 → close $319.97 -13.32; SMTC×8 09:30 $141.76 → close $131.17 -84.72; CIEN×3 09:30 $400.42 → close $378.44 -65.94; DDOG×5 09:30 $240.22 → close $236.98 -16.20; PLAB×40 09:30 $30.01 → close $27.73 -91.20; ADSK×4 09:30 $261.16 → close $260.66 -2.00; SEDG×37 09:30 $32.90 → close $31.41 -55.13 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,787.97 | ▼ 09:30 equity $9,469.88 vs yday $9,476.39 (-6.51) | 09:30 open · cash $1,787.97 (unchanged overnight, no fees) · equity $9,469.88 vs prior close $9,476.39 (-6.51) · 9 name(s) re-marked at the open (per-name table). ASST×1 yday $21.74 → 09:30 $22.54 +0.80; AMX×1 yday $23.18 → 09:30 $23.37 +0.19; KEYS×3 yday $319.97 → 09:30 $322.49 +7.56; SMTC×8 yday $131.17 → 09:30 $132.30 +9.04; CIEN×3 yday $378.44 → 09:30 $378.44 +0.00; DDOG×5 yday $236.98 → 09:30 $233.97 -15.07; PLAB×40 yday $27.73 → 09:30 $28.04 +12.40; ADSK×4 yday $260.66 → 09:30 $257.71 -11.80; SEDG×37 yday $31.41 → 09:30 $31.15 -9.62 | — |
| 2026-08-31 09:30 ET | **SELL** | `ASST` | 1 | $22.54 | $0.25 | $+1.36 | $1,810.26 | ▲ +1.36 after sell → book $9,469.63; vs 09:30 mark -0.25 | dropped from list after 3 sess (min 3) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `AMX` | 1 | $23.37 | $0.26 | $-0.88 | $1,833.37 | ▼ -0.88 after sell → book $9,469.38; vs 09:30 mark -0.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,833.37 | ▲ close $9,549.87 vs 09:30 $9,469.88 (session +80.49) | 16:00 close · cash $1,833.37 · equity $9,549.87 vs 09:30 $9,469.88 (+79.99; session marks +80.49) · 7 name(s) marked open→close (per-name table). KEYS×3 09:30 $322.49 → close $322.70 +0.63; SMTC×8 09:30 $132.30 → close $132.96 +5.28; CIEN×3 09:30 $378.44 → close $382.80 +13.08; DDOG×5 09:30 $233.97 → close $237.04 +15.37; PLAB×40 09:30 $28.04 → close $28.14 +4.00; ADSK×4 09:30 $257.71 → close $258.53 +3.28; SEDG×37 09:30 $31.15 → close $32.20 +38.85 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,833.37 | ▼ 09:30 equity $9,414.60 vs yday $9,549.87 (-135.27) | 09:30 open · cash $1,833.37 (unchanged overnight, no fees) · equity $9,414.60 vs prior close $9,549.87 (-135.27) · 7 name(s) re-marked at the open (per-name table). KEYS×3 yday $322.70 → 09:30 $321.47 -3.69; SMTC×8 yday $132.96 → 09:30 $127.63 -42.64; CIEN×3 yday $382.80 → 09:30 $376.89 -17.73; DDOG×5 yday $237.04 → 09:30 $232.88 -20.80; PLAB×40 yday $28.14 → 09:30 $27.69 -18.00; ADSK×4 yday $258.53 → 09:30 $253.48 -20.20; SEDG×37 yday $32.20 → 09:30 $31.87 -12.21 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,833.37 | ▼ close $9,335.62 vs 09:30 $9,414.60 (session -78.98) | 16:00 close · cash $1,833.37 · equity $9,335.62 vs 09:30 $9,414.60 (-78.98; session marks -78.98) · 7 name(s) marked open→close (per-name table). KEYS×3 09:30 $321.47 → close $319.27 -6.60; SMTC×8 09:30 $127.63 → close $132.27 +37.12; CIEN×3 09:30 $376.89 → close $360.33 -49.68; DDOG×5 09:30 $232.88 → close $223.84 -45.20; PLAB×40 09:30 $27.69 → close $27.33 -14.40; ADSK×4 09:30 $253.48 → close $247.69 -23.16; SEDG×37 09:30 $31.87 → close $32.49 +22.94 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,833.37 | ▼ 09:30 equity $9,303.28 vs yday $9,335.62 (-32.34) | 09:30 open · cash $1,833.37 (unchanged overnight, no fees) · equity $9,303.28 vs prior close $9,335.62 (-32.34) · 7 name(s) re-marked at the open (per-name table). KEYS×3 yday $319.27 → 09:30 $318.04 -3.69; SMTC×8 yday $132.27 → 09:30 $133.00 +5.84; CIEN×3 yday $360.33 → 09:30 $357.25 -9.24; DDOG×5 yday $223.84 → 09:30 $219.46 -21.90; PLAB×40 yday $27.33 → 09:30 $27.41 +3.20; ADSK×4 yday $247.69 → 09:30 $246.70 -3.96; SEDG×37 yday $32.49 → 09:30 $32.42 -2.59 | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 3 | $318.04 | $2.02 | $-23.13 | $2,785.47 | ▼ -23.13 after sell → book $9,301.26; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $133.00 | $2.03 | $-74.13 | $3,847.44 | ▼ -74.13 after sell → book $9,299.23; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 3 | $357.25 | $2.02 | $-133.53 | $4,917.17 | ▼ -133.53 after sell → book $9,297.21; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `DDOG` | 5 | $219.46 | $2.02 | $-107.83 | $6,012.45 | ▼ -107.83 after sell → book $9,295.19; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `PLAB` | 40 | $27.41 | $2.13 | $-108.24 | $7,106.72 | ▼ -108.24 after sell → book $9,293.06; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 4 | $246.70 | $2.02 | $-61.86 | $8,091.49 | ▼ -61.86 after sell → book $9,291.03; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 37 | $32.42 | $2.12 | $-21.98 | $9,288.91 | ▼ -21.98 after sell → book $9,288.91; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,288.91 | ▲ close $9,288.91 vs 09:30 $9,303.28 (session +0.00) | 16:00 close · cash $9,288.91 · no lots left · equity $9,288.91. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,288.91 | ▲ 09:30 equity $9,288.91 vs yday $9,288.91 (+0.00) | 09:30 open · cash $9,288.91 · no holdings · equity $9,288.91 vs prior close $9,288.91 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 4 | $351.74 | $2.00 | — | $7,879.95 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1548.15 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 3 | $486.31 | $2.00 | — | $6,419.02 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy,oppset; 🔵; ret5=+6.1; leftover $1548.15 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 47 | $32.31 | $2.13 | — | $4,898.32 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1548.15 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 97 | $15.87 | $2.28 | — | $3,356.65 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1548.15 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 64 | $23.88 | $2.18 | — | $1,826.15 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+21.9; leftover $1548.15 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 2 | $703.25 | $2.00 | — | $417.65 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1548.15 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $417.65 | ▲ close $9,531.36 vs 09:30 $9,288.91 (session +255.04) | 16:00 close · cash $417.65 · equity $9,531.36 vs 09:30 $9,288.91 (+242.45; session marks +255.04) · 6 name(s) marked open→close (per-name table). AVGO×4 09:30 $351.74 → close $357.16 +21.68; DELL×3 09:30 $486.31 → close $516.39 +90.24; CXW×47 09:30 $32.31 → close $33.66 +63.45; FRNM×97 09:30 $15.87 → close $16.90 +99.91; MMED×64 09:30 $23.88 → close $23.84 -2.56; DE×2 09:30 $703.25 → close $694.41 -17.68 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $417.65 | ▼ 09:30 equity $9,471.03 vs yday $9,531.36 (-60.33) | 09:30 open · cash $417.65 (unchanged overnight, no fees) · equity $9,471.03 vs prior close $9,531.36 (-60.33) · 6 name(s) re-marked at the open (per-name table). AVGO×4 yday $357.16 → 09:30 $359.70 +10.16; DELL×3 yday $516.39 → 09:30 $513.78 -7.83; CXW×47 yday $33.66 → 09:30 $33.46 -9.40; FRNM×97 yday $16.90 → 09:30 $16.40 -48.50; MMED×64 yday $23.84 → 09:30 $23.84 +0.00; DE×2 yday $694.41 → 09:30 $692.03 -4.76 | — |
| 2026-09-04 09:30 ET | **BUY** | `HPE` | 1 | $53.85 | $0.54 | — | $363.26 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list oppset; 🔵; ret5=+0.1; leftover $104.41 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟡 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 1 | $75.65 | $0.76 | — | $286.85 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $104.41 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $286.85 | ▲ close $9,512.20 vs 09:30 $9,471.03 (session +42.47) | 16:00 close · cash $286.85 · equity $9,512.20 vs 09:30 $9,471.03 (+41.17; session marks +42.47) · 8 name(s) marked open→close (per-name table). AVGO×4 09:30 $359.70 → close $357.90 -7.20; DELL×3 09:30 $513.78 → close $524.14 +31.08; CXW×47 09:30 $33.46 → close $34.71 +58.75; FRNM×97 09:30 $16.40 → close $16.31 -8.73; MMED×64 09:30 $23.84 → close $23.29 -35.20; DE×2 09:30 $692.03 → close $693.53 +3.00; HPE×1 09:30 $53.85 → close $52.00 -1.85; MRX×1 09:30 $75.65 → close $78.27 +2.62 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $286.85 | ▲ 09:30 equity $9,537.62 vs yday $9,512.20 (+25.42) | 09:30 open · cash $286.85 (unchanged overnight, no fees) · equity $9,537.62 vs prior close $9,512.20 (+25.42) · 8 name(s) re-marked at the open (per-name table). AVGO×4 yday $357.90 → 09:30 $363.68 +23.12; DELL×3 yday $524.14 → 09:30 $521.15 -8.97; CXW×47 yday $34.71 → 09:30 $34.49 -10.34; FRNM×97 yday $16.31 → 09:30 $16.74 +41.71; MMED×64 yday $23.29 → 09:30 $23.16 -8.32; DE×2 yday $693.53 → 09:30 $687.21 -12.64; HPE×1 yday $52.00 → 09:30 $52.29 +0.29; MRX×1 yday $78.27 → 09:30 $78.84 +0.57 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $286.85 | ▲ close $9,547.79 vs 09:30 $9,537.62 (session +10.17) | 16:00 close · cash $286.85 · equity $9,547.79 vs 09:30 $9,537.62 (+10.17; session marks +10.17) · 8 name(s) marked open→close (per-name table). AVGO×4 09:30 $363.68 → close $368.56 +19.52; DELL×3 09:30 $521.15 → close $533.88 +38.19; CXW×47 09:30 $34.49 → close $35.05 +26.32; FRNM×97 09:30 $16.74 → close $15.99 -72.75; MMED×64 09:30 $23.16 → close $23.32 +10.24; DE×2 09:30 $687.21 → close $680.73 -12.96; HPE×1 09:30 $52.29 → close $56.03 +3.74; MRX×1 09:30 $78.84 → close $76.71 -2.13 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $286.85 | ▼ 09:30 equity $9,546.79 vs yday $9,547.79 (-1.00) | 09:30 open · cash $286.85 (unchanged overnight, no fees) · equity $9,546.79 vs prior close $9,547.79 (-1.00) · 8 name(s) re-marked at the open (per-name table). AVGO×4 yday $368.56 → 09:30 $366.23 -9.32; DELL×3 yday $533.88 → 09:30 $538.47 +13.77; CXW×47 yday $35.05 → 09:30 $35.09 +1.88; FRNM×97 yday $15.99 → 09:30 $15.96 -2.91; MMED×64 yday $23.32 → 09:30 $23.22 -6.40; DE×2 yday $680.73 → 09:30 $681.32 +1.18; HPE×1 yday $56.03 → 09:30 $56.94 +0.91; MRX×1 yday $76.71 → 09:30 $76.60 -0.11 | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 4 | $366.23 | $2.02 | $+53.93 | $1,749.75 | ▲ +53.93 after sell → book $9,544.77; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DELL` | 3 | $538.47 | $2.02 | $+152.46 | $3,363.14 | ▲ +152.46 after sell → book $9,542.75; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CXW` | 47 | $35.09 | $2.15 | $+126.37 | $5,010.21 | ▲ +126.37 after sell → book $9,540.59; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FRNM` | 97 | $15.96 | $2.31 | $+4.14 | $6,556.02 | ▲ +4.14 after sell → book $9,538.28; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 64 | $23.22 | $2.20 | $-46.63 | $8,039.90 | ▼ -46.63 after sell → book $9,536.08; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DE` | 2 | $681.32 | $2.02 | $-47.87 | $9,400.52 | ▼ -47.87 after sell → book $9,534.06; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,400.52 | ▲ close $9,535.14 vs 09:30 $9,546.79 (session +1.08) | 16:00 close · cash $9,400.52 · equity $9,535.14 vs 09:30 $9,546.79 (-11.65; session marks +1.08) · 2 name(s) marked open→close (per-name table). HPE×1 09:30 $56.94 → close $58.90 +1.96; MRX×1 09:30 $76.60 → close $75.72 -0.88 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,400.52 | ▼ 09:30 equity $9,533.32 vs yday $9,535.14 (-1.82) | 09:30 open · cash $9,400.52 (unchanged overnight, no fees) · equity $9,533.32 vs prior close $9,535.14 (-1.82) · 2 name(s) re-marked at the open (per-name table). HPE×1 yday $58.90 → 09:30 $57.80 -1.10; MRX×1 yday $75.72 → 09:30 $75.00 -0.72 | — |
| 2026-09-10 09:30 ET | **SELL** | `HPE` | 1 | $57.80 | $0.60 | $+2.81 | $9,457.72 | ▲ +2.81 after sell → book $9,532.72; vs 09:30 mark -0.60 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-10 09:30 ET | **SELL** | `MRX` | 1 | $75.00 | $0.77 | $-2.18 | $9,531.95 | ▼ -2.18 after sell → book $9,531.95; vs 09:30 mark -0.77 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,531.95 | ▲ close $9,531.95 vs 09:30 $9,533.32 (session +0.00) | 16:00 close · cash $9,531.95 · no lots left · equity $9,531.95. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,531.95 | ▲ 09:30 equity $9,531.95 vs yday $9,531.95 (-0.00) | 09:30 open · cash $9,531.95 · no holdings · equity $9,531.95 vs prior close $9,531.95 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 28 | $164.43 | $2.07 | — | $4,925.83 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $4765.97 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BTI` | 85 | $56.03 | $2.25 | — | $161.04 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list oppset; 🔵; ret5=-0.8; leftover $4765.97 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $161.04 | ▼ close $9,064.28 vs 09:30 $9,531.95 (session -463.35) | 16:00 close · cash $161.04 · equity $9,064.28 vs 09:30 $9,531.95 (-467.67; session marks -463.35) · 2 name(s) marked open→close (per-name table). ORCL×28 09:30 $164.43 → close $150.28 -396.20; BTI×85 09:30 $56.03 → close $55.24 -67.15 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $161.04 | ▼ 09:30 equity $8,976.00 vs yday $9,064.28 (-88.28) | 09:30 open · cash $161.04 (unchanged overnight, no fees) · equity $8,976.00 vs prior close $9,064.28 (-88.28) · 2 name(s) re-marked at the open (per-name table). ORCL×28 yday $150.28 → 09:30 $141.42 -248.08; BTI×85 yday $55.24 → 09:30 $57.12 +159.80 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $161.04 | ▲ close $9,084.81 vs 09:30 $8,976.00 (session +108.81) | 16:00 close · cash $161.04 · equity $9,084.81 vs 09:30 $8,976.00 (+108.81; session marks +108.81) · 2 name(s) marked open→close (per-name table). ORCL×28 09:30 $141.42 → close $144.79 +94.36; BTI×85 09:30 $57.12 → close $57.29 +14.45 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $161.04 | ▼ 09:30 equity $8,977.02 vs yday $9,084.81 (-107.79) | 09:30 open · cash $161.04 (unchanged overnight, no fees) · equity $8,977.02 vs prior close $9,084.81 (-107.79) · 2 name(s) re-marked at the open (per-name table). ORCL×28 yday $144.79 → 09:30 $143.46 -37.24; BTI×85 yday $57.29 → 09:30 $56.46 -70.55 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $161.04 | ▼ close $8,895.04 vs 09:30 $8,977.02 (session -81.98) | 16:00 close · cash $161.04 · equity $8,895.04 vs 09:30 $8,977.02 (-81.98; session marks -81.98) · 2 name(s) marked open→close (per-name table). ORCL×28 09:30 $143.46 → close $140.35 -87.08; BTI×85 09:30 $56.46 → close $56.52 +5.10 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $161.04 | ▼ 09:30 equity $8,887.78 vs yday $8,895.04 (-7.26) | 09:30 open · cash $161.04 (unchanged overnight, no fees) · equity $8,887.78 vs prior close $8,895.04 (-7.26) · 2 name(s) re-marked at the open (per-name table). ORCL×28 yday $140.35 → 09:30 $140.03 -8.96; BTI×85 yday $56.52 → 09:30 $56.54 +1.70 | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 28 | $140.03 | $2.12 | $-687.39 | $4,079.76 | ▼ -687.39 after sell → book $8,885.66; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BTI` | 85 | $56.54 | $2.30 | $+38.81 | $8,883.36 | ▲ +38.81 after sell → book $8,883.36; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 84 | $26.27 | $2.24 | — | $6,674.44 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2220.84 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 11 | $189.17 | $2.02 | — | $4,591.55 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+2.1; leftover $2220.84 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 55 | $39.99 | $2.15 | — | $2,389.94 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2220.84 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `AMX` | 95 | $23.18 | $2.27 | — | $185.57 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list oppset; 🔵; ret5=-0.2; leftover $2220.84 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $185.57 | ▼ close $8,734.27 vs 09:30 $8,887.78 (session -140.40) | 16:00 close · cash $185.57 · equity $8,734.27 vs 09:30 $8,887.78 (-153.51; session marks -140.40) · 4 name(s) marked open→close (per-name table). WAY×84 09:30 $26.27 → close $26.59 +26.88; QCOM×11 09:30 $189.17 → close $184.84 -47.63; SM×55 09:30 $39.99 → close $38.16 -100.65; AMX×95 09:30 $23.18 → close $22.98 -19.00 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $185.57 | ▲ 09:30 equity $8,766.16 vs yday $8,734.27 (+31.89) | 09:30 open · cash $185.57 (unchanged overnight, no fees) · equity $8,766.16 vs prior close $8,734.27 (+31.89) · 4 name(s) re-marked at the open (per-name table). WAY×84 yday $26.59 → 09:30 $26.51 -6.72; QCOM×11 yday $184.84 → 09:30 $190.35 +60.61; SM×55 yday $38.16 → 09:30 $37.57 -32.45; AMX×95 yday $22.98 → 09:30 $23.09 +10.45 | — |
| 2026-09-17 09:30 ET | **BUY** | `AVTR` | 3 | $15.81 | $0.48 | — | $137.66 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $61.86 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 2 | $22.12 | $0.45 | — | $92.97 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $61.86 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $92.97 | ▼ close $8,709.94 vs 09:30 $8,766.16 (session -55.29) | 16:00 close · cash $92.97 · equity $8,709.94 vs 09:30 $8,766.16 (-56.22; session marks -55.29) · 6 name(s) marked open→close (per-name table). WAY×84 09:30 $26.51 → close $26.51 +0.00; QCOM×11 09:30 $190.35 → close $188.71 -18.04; SM×55 09:30 $37.57 → close $36.97 -33.00; AMX×95 09:30 $23.09 → close $23.03 -5.70; AVTR×3 09:30 $15.81 → close $15.86 +0.15; GME×2 09:30 $22.12 → close $22.77 +1.30 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $92.97 | ▲ 09:30 equity $8,758.27 vs yday $8,709.94 (+48.33) | 09:30 open · cash $92.97 (unchanged overnight, no fees) · equity $8,758.27 vs prior close $8,709.94 (+48.33) · 6 name(s) re-marked at the open (per-name table). WAY×84 yday $26.51 → 09:30 $26.95 +36.96; QCOM×11 yday $188.71 → 09:30 $191.34 +28.93; SM×55 yday $36.97 → 09:30 $36.87 -5.50; AMX×95 yday $23.03 → 09:30 $22.90 -12.35; AVTR×3 yday $15.86 → 09:30 $15.87 +0.03; GME×2 yday $22.77 → 09:30 $22.90 +0.26 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 1 | $20.91 | $0.21 | — | $71.85 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $30.99 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 2 | $14.79 | $0.30 | — | $41.96 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $30.99 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 2 | $14.07 | $0.29 | — | $13.54 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $30.99 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.54 | ▼ close $8,457.39 vs 09:30 $8,758.27 (session -300.08) | 16:00 close · cash $13.54 · equity $8,457.39 vs 09:30 $8,758.27 (-300.88; session marks -300.08) · 9 name(s) marked open→close (per-name table). WAY×84 09:30 $26.95 → close $25.66 -108.36; QCOM×11 09:30 $191.34 → close $177.72 -149.82; SM×55 09:30 $36.87 → close $36.97 +5.50; AMX×95 09:30 $22.90 → close $22.43 -44.65; AVTR×3 09:30 $15.87 → close $15.52 -1.05; GME×2 09:30 $22.90 → close $22.64 -0.52; TH×1 09:30 $20.91 → close $21.19 +0.28; RARE×2 09:30 $14.79 → close $14.51 -0.56; BHVN×2 09:30 $14.07 → close $13.62 -0.90 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `HLIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `SNDK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `HLIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `SNDK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `APA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 22.38 < 1 share @ 119.43 |
| 2026-08-21 | `FUTU` | cash | leftover split 22.38 < 1 share @ 115.18 |
| 2026-08-21 | `GRAL` | cash | leftover split 22.38 < 1 share @ 78.88 |
| 2026-08-21 | `VIRT` | cash | leftover split 22.38 < 1 share @ 60.66 |
| 2026-08-21 | `MFC` | cash | leftover split 22.38 < 1 share @ 42.48 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `APA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ABTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FNV` | cash | leftover split 33.04 < 1 share @ 267.02 |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `AMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 7.74 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 7.74 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 7.74 < 1 share @ 1746.53 |
| 2026-08-27 | `CM` | cash | leftover split 7.74 < 1 share @ 118.77 |
| 2026-08-27 | `GEN` | cash | leftover split 7.74 < 1 share @ 29.83 |
| 2026-08-27 | `LRCX` | cash | leftover split 7.74 < 1 share @ 318.88 |
| 2026-08-27 | `NVDA` | cash | leftover split 7.74 < 1 share @ 222.86 |
| 2026-08-28 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `AMX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `MPWR` | cash | leftover split 1221.78 < 1 share @ 1306.03 |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `DDOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `PLAB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ADSK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DDOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `PLAB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ADSK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CXW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 104.41 < 1 share @ 263.36 |
| 2026-09-04 | `BE` | cash | leftover split 104.41 < 1 share @ 236.82 |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CXW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRNM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HPE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `MRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-09 | `HPE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `MRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BTI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BTI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `WAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `AMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SMTC` | cash | leftover split 61.86 < 1 share @ 170.85 |
| 2026-09-18 | `WAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `AMX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `AVTR` | min_hold | dropped but min-hold 1/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `WAY` | 84 | 2026-09-16 @ $26.27 | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2220.84 |
| `QCOM` | 11 | 2026-09-16 @ $189.17 | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+2.1; leftover $2220.84 |
| `SM` | 55 | 2026-09-16 @ $39.99 | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2220.84 |
| `AMX` | 95 | 2026-09-16 @ $23.18 | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list oppset; 🔵; ret5=-0.2; leftover $2220.84 |
| `AVTR` | 3 | 2026-09-17 @ $15.81 | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $61.86 |
| `GME` | 2 | 2026-09-17 @ $22.12 | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $61.86 |
| `TH` | 1 | 2026-09-18 @ $20.91 | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $30.99 |
| `RARE` | 2 | 2026-09-18 @ $14.79 | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $30.99 |
| `BHVN` | 2 | 2026-09-18 @ $14.07 | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $30.99 |
