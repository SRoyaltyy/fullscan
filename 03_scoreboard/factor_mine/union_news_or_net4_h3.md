# Factor mine action — `union_news_or_net4_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 OR headline🟢 and camera net ≥ 4

Cash book **-15.94%** ($8,406) · signal-only (no cash/fees) was -11.90%. Starts YES **3/28**. Fills 85 · skips 129 · realized $-1908.60.

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
- Must-have: camera net (+G −R) is at least 4.
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
- **Gate** `news_or_headline=True,cam_net_min=4` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $215.26.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `HLIT` | 126 | — | $13.18 | +0.00 | $13.92 | +93.24 | +93.24 | +0.00 | +93.24 |
| 2026-08-14 | `SNDK` | 1 | — | $1646.93 | +0.00 | $1641.11 | -5.82 | -5.82 | +0.00 | -5.82 |
| 2026-08-14 | `ANGX` | 386 | — | $4.31 | +0.00 | $4.37 | +23.16 | +23.16 | +0.00 | +23.16 |
| 2026-08-14 | `ARX` | 85 | — | $19.57 | +0.00 | $19.58 | +0.85 | +0.85 | +0.00 | +0.85 |
| 2026-08-14 | `MH` | 123 | — | $13.55 | +0.00 | $13.10 | -55.35 | -55.35 | +0.00 | -55.35 |
| 2026-08-14 | `VELO` | 108 | — | $15.38 | +0.00 | $16.16 | +84.24 | +84.24 | +0.00 | +84.24 |
| 2026-08-17 | `HLIT` | 126 | $13.92 | $13.84 | -10.08 | $13.43 | -51.66 | -61.74 | +83.16 | +31.50 |
| 2026-08-17 | `SNDK` | 1 | $1641.11 | $1700.74 | +59.63 | $1786.85 | +86.11 | +145.74 | +53.81 | +139.92 |
| 2026-08-17 | `ANGX` | 386 | $4.37 | $4.60 | +88.78 | $4.71 | +42.46 | +131.24 | +111.94 | +154.40 |
| 2026-08-17 | `ARX` | 85 | $19.58 | $19.57 | -0.85 | $19.54 | -2.55 | -3.40 | +0.00 | -2.55 |
| 2026-08-17 | `MH` | 123 | $13.10 | $13.16 | +7.38 | $12.77 | -47.97 | -40.59 | -47.97 | -95.94 |
| 2026-08-17 | `VELO` | 108 | $16.16 | $16.05 | -11.88 | $15.60 | -49.14 | -61.02 | +72.36 | +23.22 |
| 2026-08-18 | `HLIT` | 126 | $13.43 | $12.93 | -63.00 | $12.73 | -25.20 | -88.20 | -31.50 | -56.70 |
| 2026-08-18 | `SNDK` | 1 | $1786.85 | $1677.54 | -109.31 | $1625.78 | -51.76 | -161.07 | +30.61 | -21.15 |
| 2026-08-18 | `ANGX` | 386 | $4.71 | $4.79 | +30.88 | $4.85 | +23.16 | +54.04 | +185.28 | +208.44 |
| 2026-08-18 | `ARX` | 85 | $19.54 | $19.57 | +2.55 | $19.56 | -0.85 | +1.70 | +0.00 | -0.85 |
| 2026-08-18 | `MH` | 123 | $12.77 | $13.00 | +28.29 | $13.12 | +14.76 | +43.05 | -67.65 | -52.89 |
| 2026-08-18 | `VELO` | 108 | $15.60 | $14.73 | -93.42 | $14.63 | -10.80 | -104.22 | -70.20 | -81.00 |
| 2026-08-19 | `HLIT` | 126 | $12.73 | $12.90 | +21.42 | — | +0.00 | +21.42 | -35.28 | — |
| 2026-08-19 | `SNDK` | 1 | $1625.78 | $1682.40 | +56.62 | — | +0.00 | +56.62 | +35.47 | — |
| 2026-08-19 | `ANGX` | 386 | $4.85 | $4.79 | -23.16 | — | +0.00 | -23.16 | +185.28 | — |
| 2026-08-19 | `ARX` | 85 | $19.56 | $19.58 | +1.70 | — | +0.00 | +1.70 | +0.85 | — |
| 2026-08-19 | `MH` | 123 | $13.12 | $13.01 | -13.53 | — | +0.00 | -13.53 | -66.42 | — |
| 2026-08-19 | `VELO` | 108 | $14.63 | $14.51 | -12.96 | — | +0.00 | -12.96 | -93.96 | — |
| 2026-08-20 | `BHP` | 15 | — | $91.01 | +0.00 | $93.63 | +39.30 | +39.30 | +0.00 | +39.30 |
| 2026-08-20 | `APA` | 31 | — | $44.76 | +0.00 | $44.39 | -11.47 | -11.47 | +0.00 | -11.47 |
| 2026-08-20 | `AUTL` | 577 | — | $2.47 | +0.00 | $2.46 | -5.77 | -5.77 | +0.00 | -5.77 |
| 2026-08-20 | `CRSP` | 24 | — | $58.73 | +0.00 | $58.12 | -14.64 | -14.64 | +0.00 | -14.64 |
| 2026-08-20 | `ASST` | 89 | — | $16.00 | +0.00 | $16.13 | +11.57 | +11.57 | +0.00 | +11.57 |
| 2026-08-20 | `MRNA` | 9 | — | $150.14 | +0.00 | $133.32 | -151.38 | -151.38 | +0.00 | -151.38 |
| 2026-08-20 | `ZLAB` | 53 | — | $26.57 | +0.00 | $26.02 | -29.15 | -29.15 | +0.00 | -29.15 |
| 2026-08-21 | `BHP` | 15 | $93.63 | $95.72 | +31.35 | $97.03 | +19.65 | +51.00 | +70.65 | +90.30 |
| 2026-08-21 | `APA` | 31 | $44.39 | $44.52 | +4.03 | $43.39 | -35.03 | -31.00 | -7.44 | -42.47 |
| 2026-08-21 | `AUTL` | 577 | $2.46 | $2.47 | +5.77 | $2.41 | -34.62 | -28.85 | +0.00 | -34.62 |
| 2026-08-21 | `CRSP` | 24 | $58.12 | $59.72 | +38.40 | $59.50 | -5.28 | +33.12 | +23.76 | +18.48 |
| 2026-08-21 | `ASST` | 89 | $16.13 | $17.66 | +136.17 | $18.22 | +49.84 | +186.01 | +147.74 | +197.58 |
| 2026-08-21 | `MRNA` | 9 | $133.32 | $133.11 | -1.89 | $145.13 | +108.18 | +106.29 | -153.27 | -45.09 |
| 2026-08-21 | `ZLAB` | 53 | $26.02 | $26.25 | +12.19 | $26.01 | -12.72 | -0.53 | -16.96 | -29.68 |
| 2026-08-21 | `ABTC` | 3 | — | $8.66 | +0.00 | $7.93 | -2.19 | -2.19 | +0.00 | -2.19 |
| 2026-08-21 | `HIVE` | 10 | — | $3.24 | +0.00 | $3.03 | -2.10 | -2.10 | +0.00 | -2.10 |
| 2026-08-21 | `MARA` | 2 | — | $11.70 | +0.00 | $11.26 | -0.88 | -0.88 | +0.00 | -0.88 |
| 2026-08-24 | `BHP` | 15 | $97.03 | $97.31 | +4.20 | $97.13 | -2.70 | +1.50 | +94.50 | +91.80 |
| 2026-08-24 | `APA` | 31 | $43.39 | $42.93 | -14.26 | $42.96 | +0.93 | -13.33 | -56.73 | -55.80 |
| 2026-08-24 | `AUTL` | 577 | $2.41 | $2.40 | -5.77 | $2.34 | -34.62 | -40.39 | -40.39 | -75.01 |
| 2026-08-24 | `CRSP` | 24 | $59.50 | $58.75 | -18.00 | $57.08 | -40.20 | -58.20 | +0.48 | -39.72 |
| 2026-08-24 | `ASST` | 89 | $18.22 | $18.76 | +48.06 | $19.73 | +86.33 | +134.39 | +245.64 | +331.97 |
| 2026-08-24 | `MRNA` | 9 | $145.13 | $142.70 | -21.87 | $138.89 | -34.29 | -56.16 | -66.96 | -101.25 |
| 2026-08-24 | `ZLAB` | 53 | $26.01 | $25.43 | -30.74 | $25.64 | +11.13 | -19.61 | -60.42 | -49.29 |
| 2026-08-24 | `ABTC` | 3 | $7.93 | $8.00 | +0.21 | $8.64 | +1.92 | +2.13 | -1.98 | -0.06 |
| 2026-08-24 | `HIVE` | 10 | $3.03 | $2.99 | -0.40 | $2.86 | -1.30 | -1.70 | -2.50 | -3.80 |
| 2026-08-24 | `MARA` | 2 | $11.26 | $11.17 | -0.18 | $11.18 | +0.02 | -0.16 | -1.06 | -1.04 |
| 2026-08-25 | `BHP` | 15 | $97.13 | $95.86 | -19.05 | — | +0.00 | -19.05 | +72.75 | — |
| 2026-08-25 | `APA` | 31 | $42.96 | $41.38 | -48.98 | — | +0.00 | -48.98 | -104.78 | — |
| 2026-08-25 | `AUTL` | 577 | $2.34 | $2.38 | +23.08 | — | +0.00 | +23.08 | -51.93 | — |
| 2026-08-25 | `CRSP` | 24 | $57.08 | $57.93 | +20.52 | — | +0.00 | +20.52 | -19.20 | — |
| 2026-08-25 | `ASST` | 89 | $19.73 | $19.04 | -61.41 | — | +0.00 | -61.41 | +270.56 | — |
| 2026-08-25 | `MRNA` | 9 | $138.89 | $143.50 | +41.49 | — | +0.00 | +41.49 | -59.76 | — |
| 2026-08-25 | `ZLAB` | 53 | $25.64 | $26.04 | +21.20 | — | +0.00 | +21.20 | -28.09 | — |
| 2026-08-25 | `ABTC` | 3 | $8.64 | $8.62 | -0.06 | $9.24 | +1.86 | +1.80 | -0.12 | +1.74 |
| 2026-08-25 | `HIVE` | 10 | $2.86 | $2.87 | +0.10 | $3.02 | +1.50 | +1.60 | -3.70 | -2.20 |
| 2026-08-25 | `MARA` | 2 | $11.18 | $11.07 | -0.22 | $11.83 | +1.52 | +1.30 | -1.26 | +0.26 |
| 2026-08-25 | `AU` | 27 | — | $118.52 | +0.00 | $123.39 | +131.49 | +131.49 | +0.00 | +131.49 |
| 2026-08-25 | `FCX` | 43 | — | $77.13 | +0.00 | $79.91 | +119.54 | +119.54 | +0.00 | +119.54 |
| 2026-08-25 | `EZPW` | 94 | — | $35.05 | +0.00 | $35.23 | +16.92 | +16.92 | +0.00 | +16.92 |
| 2026-08-26 | `ABTC` | 3 | $9.24 | $8.84 | -1.20 | — | +0.00 | -1.20 | +0.54 | — |
| 2026-08-26 | `HIVE` | 10 | $3.02 | $2.95 | -0.70 | — | +0.00 | -0.70 | -2.90 | — |
| 2026-08-26 | `MARA` | 2 | $11.83 | $11.56 | -0.54 | — | +0.00 | -0.54 | -0.28 | — |
| 2026-08-26 | `AU` | 27 | $123.39 | $119.80 | -96.93 | $118.11 | -45.63 | -142.56 | +34.56 | -11.07 |
| 2026-08-26 | `FCX` | 43 | $79.91 | $79.34 | -24.51 | $79.00 | -14.62 | -39.13 | +95.03 | +80.41 |
| 2026-08-26 | `EZPW` | 94 | $35.23 | $35.70 | +44.18 | $33.90 | -169.20 | -125.02 | +61.10 | -108.10 |
| 2026-08-27 | `AU` | 27 | $118.11 | $117.41 | -18.90 | $118.40 | +26.73 | +7.83 | -29.97 | -3.24 |
| 2026-08-27 | `FCX` | 43 | $79.00 | $78.83 | -7.31 | $78.42 | -17.63 | -24.94 | +73.10 | +55.47 |
| 2026-08-27 | `EZPW` | 94 | $33.90 | $33.50 | -37.60 | $34.41 | +85.54 | +47.94 | -145.70 | -60.16 |
| 2026-08-28 | `AU` | 27 | $118.40 | $119.19 | +21.33 | — | +0.00 | +21.33 | +18.09 | — |
| 2026-08-28 | `FCX` | 43 | $78.42 | $78.57 | +6.45 | — | +0.00 | +6.45 | +61.92 | — |
| 2026-08-28 | `EZPW` | 94 | $34.41 | $34.50 | +8.46 | — | +0.00 | +8.46 | -51.70 | — |
| 2026-08-28 | `KEYS` | 3 | — | $324.41 | +0.00 | $319.97 | -13.32 | -13.32 | +0.00 | -13.32 |
| 2026-08-28 | `SMTC` | 8 | — | $141.76 | +0.00 | $131.17 | -84.72 | -84.72 | +0.00 | -84.72 |
| 2026-08-28 | `CIEN` | 3 | — | $400.42 | +0.00 | $378.44 | -65.94 | -65.94 | +0.00 | -65.94 |
| 2026-08-28 | `DDOG` | 5 | — | $240.22 | +0.00 | $236.98 | -16.20 | -16.20 | +0.00 | -16.20 |
| 2026-08-28 | `ADSK` | 4 | — | $261.16 | +0.00 | $260.66 | -2.00 | -2.00 | +0.00 | -2.00 |
| 2026-08-28 | `SEDG` | 38 | — | $32.90 | +0.00 | $31.41 | -56.62 | -56.62 | +0.00 | -56.62 |
| 2026-08-28 | `TLS` | 260 | — | $4.82 | +0.00 | $4.79 | -7.80 | -7.80 | +0.00 | -7.80 |
| 2026-08-31 | `KEYS` | 3 | $319.97 | $322.49 | +7.56 | $322.70 | +0.63 | +8.19 | -5.76 | -5.13 |
| 2026-08-31 | `SMTC` | 8 | $131.17 | $132.30 | +9.04 | $132.96 | +5.28 | +14.32 | -75.68 | -70.40 |
| 2026-08-31 | `CIEN` | 3 | $378.44 | $378.44 | +0.00 | $382.80 | +13.08 | +13.08 | -65.94 | -52.86 |
| 2026-08-31 | `DDOG` | 5 | $236.98 | $233.97 | -15.07 | $237.04 | +15.37 | +0.30 | -31.27 | -15.90 |
| 2026-08-31 | `ADSK` | 4 | $260.66 | $257.71 | -11.80 | $258.53 | +3.28 | -8.52 | -13.80 | -10.52 |
| 2026-08-31 | `SEDG` | 38 | $31.41 | $31.15 | -9.88 | $32.20 | +39.90 | +30.02 | -66.50 | -26.60 |
| 2026-08-31 | `TLS` | 260 | $4.79 | $4.81 | +5.20 | $4.82 | +2.60 | +7.80 | -2.60 | +0.00 |
| 2026-09-01 | `KEYS` | 3 | $322.70 | $321.47 | -3.69 | $319.27 | -6.60 | -10.29 | -8.82 | -15.42 |
| 2026-09-01 | `SMTC` | 8 | $132.96 | $127.63 | -42.64 | $132.27 | +37.12 | -5.52 | -113.04 | -75.92 |
| 2026-09-01 | `CIEN` | 3 | $382.80 | $376.89 | -17.73 | $360.33 | -49.68 | -67.41 | -70.59 | -120.27 |
| 2026-09-01 | `DDOG` | 5 | $237.04 | $232.88 | -20.80 | $223.84 | -45.20 | -66.00 | -36.70 | -81.90 |
| 2026-09-01 | `ADSK` | 4 | $258.53 | $253.48 | -20.20 | $247.69 | -23.16 | -43.36 | -30.72 | -53.88 |
| 2026-09-01 | `SEDG` | 38 | $32.20 | $31.87 | -12.54 | $32.49 | +23.56 | +11.02 | -39.14 | -15.58 |
| 2026-09-01 | `TLS` | 260 | $4.82 | $4.77 | -13.00 | $4.72 | -13.00 | -26.00 | -13.00 | -26.00 |
| 2026-09-02 | `KEYS` | 3 | $319.27 | $318.04 | -3.69 | — | +0.00 | -3.69 | -19.11 | — |
| 2026-09-02 | `SMTC` | 8 | $132.27 | $133.00 | +5.84 | — | +0.00 | +5.84 | -70.08 | — |
| 2026-09-02 | `CIEN` | 3 | $360.33 | $357.25 | -9.24 | — | +0.00 | -9.24 | -129.51 | — |
| 2026-09-02 | `DDOG` | 5 | $223.84 | $219.46 | -21.90 | — | +0.00 | -21.90 | -103.80 | — |
| 2026-09-02 | `ADSK` | 4 | $247.69 | $246.70 | -3.96 | — | +0.00 | -3.96 | -57.84 | — |
| 2026-09-02 | `SEDG` | 38 | $32.49 | $32.42 | -2.66 | — | +0.00 | -2.66 | -18.24 | — |
| 2026-09-02 | `TLS` | 260 | $4.72 | $4.73 | +2.60 | — | +0.00 | +2.60 | -23.40 | — |
| 2026-09-03 | `AVGO` | 3 | — | $351.74 | +0.00 | $357.16 | +16.26 | +16.26 | +0.00 | +16.26 |
| 2026-09-03 | `DELL` | 2 | — | $486.31 | +0.00 | $516.39 | +60.16 | +60.16 | +0.00 | +60.16 |
| 2026-09-03 | `CXW` | 42 | — | $32.31 | +0.00 | $33.66 | +56.70 | +56.70 | +0.00 | +56.70 |
| 2026-09-03 | `FRNM` | 86 | — | $15.87 | +0.00 | $16.90 | +88.58 | +88.58 | +0.00 | +88.58 |
| 2026-09-03 | `MMED` | 57 | — | $23.88 | +0.00 | $23.84 | -2.28 | -2.28 | +0.00 | -2.28 |
| 2026-09-03 | `DE` | 1 | — | $703.25 | +0.00 | $694.41 | -8.84 | -8.84 | +0.00 | -8.84 |
| 2026-09-03 | `HPE` | 28 | — | $47.60 | +0.00 | $54.44 | +191.52 | +191.52 | +0.00 | +191.52 |
| 2026-09-04 | `AVGO` | 3 | $357.16 | $359.70 | +7.62 | $357.90 | -5.40 | +2.22 | +23.88 | +18.48 |
| 2026-09-04 | `DELL` | 2 | $516.39 | $513.78 | -5.22 | $524.14 | +20.72 | +15.50 | +54.94 | +75.66 |
| 2026-09-04 | `CXW` | 42 | $33.66 | $33.46 | -8.40 | $34.71 | +52.50 | +44.10 | +48.30 | +100.80 |
| 2026-09-04 | `FRNM` | 86 | $16.90 | $16.40 | -43.00 | $16.31 | -7.74 | -50.74 | +45.58 | +37.84 |
| 2026-09-04 | `MMED` | 57 | $23.84 | $23.84 | +0.00 | $23.29 | -31.35 | -31.35 | -2.28 | -33.63 |
| 2026-09-04 | `DE` | 1 | $694.41 | $692.03 | -2.38 | $693.53 | +1.50 | -0.88 | -11.22 | -9.72 |
| 2026-09-04 | `HPE` | 28 | $54.44 | $53.85 | -16.52 | $52.00 | -51.80 | -68.32 | +175.00 | +123.20 |
| 2026-09-04 | `CRM` | 1 | — | $263.36 | +0.00 | $259.23 | -4.13 | -4.13 | +0.00 | -4.13 |
| 2026-09-04 | `MRX` | 4 | — | $75.65 | +0.00 | $78.27 | +10.48 | +10.48 | +0.00 | +10.48 |
| 2026-09-04 | `BE` | 1 | — | $236.82 | +0.00 | $252.87 | +16.05 | +16.05 | +0.00 | +16.05 |
| 2026-09-04 | `BAK` | 184 | — | $1.94 | +0.00 | $1.89 | -9.20 | -9.20 | +0.00 | -9.20 |
| 2026-09-08 | `AVGO` | 3 | $357.90 | $363.68 | +17.34 | $368.56 | +14.64 | +31.98 | +35.82 | +50.46 |
| 2026-09-08 | `DELL` | 2 | $524.14 | $521.15 | -5.98 | $533.88 | +25.46 | +19.48 | +69.68 | +95.14 |
| 2026-09-08 | `CXW` | 42 | $34.71 | $34.49 | -9.24 | $35.05 | +23.52 | +14.28 | +91.56 | +115.08 |
| 2026-09-08 | `FRNM` | 86 | $16.31 | $16.74 | +36.98 | $15.99 | -64.50 | -27.52 | +74.82 | +10.32 |
| 2026-09-08 | `MMED` | 57 | $23.29 | $23.16 | -7.41 | $23.32 | +9.12 | +1.71 | -41.04 | -31.92 |
| 2026-09-08 | `DE` | 1 | $693.53 | $687.21 | -6.32 | $680.73 | -6.48 | -12.80 | -16.04 | -22.52 |
| 2026-09-08 | `HPE` | 28 | $52.00 | $52.29 | +8.12 | $56.03 | +104.72 | +112.84 | +131.32 | +236.04 |
| 2026-09-08 | `CRM` | 1 | $259.23 | $253.72 | -5.51 | $249.12 | -4.60 | -10.11 | -9.64 | -14.24 |
| 2026-09-08 | `MRX` | 4 | $78.27 | $78.84 | +2.28 | $76.71 | -8.52 | -6.24 | +12.76 | +4.24 |
| 2026-09-08 | `BE` | 1 | $252.87 | $267.76 | +14.89 | $277.22 | +9.46 | +24.35 | +30.94 | +40.40 |
| 2026-09-08 | `BAK` | 184 | $1.89 | $1.94 | +9.20 | $1.92 | -3.68 | +5.52 | +0.00 | -3.68 |
| 2026-09-09 | `AVGO` | 3 | $368.56 | $366.23 | -6.99 | — | +0.00 | -6.99 | +43.47 | — |
| 2026-09-09 | `DELL` | 2 | $533.88 | $538.47 | +9.18 | — | +0.00 | +9.18 | +104.32 | — |
| 2026-09-09 | `CXW` | 42 | $35.05 | $35.09 | +1.68 | — | +0.00 | +1.68 | +116.76 | — |
| 2026-09-09 | `FRNM` | 86 | $15.99 | $15.96 | -2.58 | — | +0.00 | -2.58 | +7.74 | — |
| 2026-09-09 | `MMED` | 57 | $23.32 | $23.22 | -5.70 | — | +0.00 | -5.70 | -37.62 | — |
| 2026-09-09 | `DE` | 1 | $680.73 | $681.32 | +0.59 | — | +0.00 | +0.59 | -21.93 | — |
| 2026-09-09 | `HPE` | 28 | $56.03 | $56.94 | +25.48 | — | +0.00 | +25.48 | +261.52 | — |
| 2026-09-09 | `CRM` | 1 | $249.12 | $249.78 | +0.66 | $244.16 | -5.62 | -4.96 | -13.58 | -19.20 |
| 2026-09-09 | `MRX` | 4 | $76.71 | $76.60 | -0.44 | $75.72 | -3.52 | -3.96 | +3.80 | +0.28 |
| 2026-09-09 | `BE` | 1 | $277.22 | $272.99 | -4.23 | $269.28 | -3.71 | -7.94 | +36.17 | +32.46 |
| 2026-09-09 | `BAK` | 184 | $1.92 | $2.02 | +17.48 | $1.97 | -8.28 | +9.20 | +13.80 | +5.52 |
| 2026-09-10 | `CRM` | 1 | $244.16 | $245.35 | +1.19 | — | +0.00 | +1.19 | -18.01 | — |
| 2026-09-10 | `MRX` | 4 | $75.72 | $75.00 | -2.88 | — | +0.00 | -2.88 | -2.60 | — |
| 2026-09-10 | `BE` | 1 | $269.28 | $260.71 | -8.57 | — | +0.00 | -8.57 | +23.89 | — |
| 2026-09-10 | `BAK` | 184 | $1.97 | $1.97 | +0.00 | — | +0.00 | +0.00 | +5.52 | — |
| 2026-09-11 | `ORCL` | 60 | — | $164.43 | +0.00 | $150.28 | -849.00 | -849.00 | +0.00 | -849.00 |
| 2026-09-14 | `ORCL` | 60 | $150.28 | $141.42 | -531.60 | $144.79 | +202.20 | -329.40 | -1380.60 | -1178.40 |
| 2026-09-15 | `ORCL` | 60 | $144.79 | $143.46 | -79.80 | $140.35 | -186.60 | -266.40 | -1258.20 | -1444.80 |
| 2026-09-16 | `ORCL` | 60 | $140.35 | $140.03 | -19.20 | — | +0.00 | -19.20 | -1464.00 | — |
| 2026-09-16 | `WAY` | 108 | — | $26.27 | +0.00 | $26.59 | +34.56 | +34.56 | +0.00 | +34.56 |
| 2026-09-16 | `QCOM` | 15 | — | $189.17 | +0.00 | $184.84 | -64.95 | -64.95 | +0.00 | -64.95 |
| 2026-09-16 | `SM` | 71 | — | $39.99 | +0.00 | $38.16 | -129.93 | -129.93 | +0.00 | -129.93 |
| 2026-09-17 | `WAY` | 108 | $26.59 | $26.51 | -8.64 | $26.51 | +0.00 | -8.64 | +25.92 | +25.92 |
| 2026-09-17 | `QCOM` | 15 | $184.84 | $190.35 | +82.65 | $188.71 | -24.60 | +58.05 | +17.70 | -6.90 |
| 2026-09-17 | `SM` | 71 | $38.16 | $37.57 | -41.89 | $36.97 | -42.60 | -84.49 | -171.82 | -214.42 |
| 2026-09-18 | `WAY` | 108 | $26.51 | $26.95 | +47.52 | $25.66 | -139.32 | -91.80 | +73.44 | -65.88 |
| 2026-09-18 | `QCOM` | 15 | $188.71 | $191.34 | +39.45 | $177.72 | -204.30 | -164.85 | +32.55 | -171.75 |
| 2026-09-18 | `SM` | 71 | $36.97 | $36.87 | -7.10 | $36.97 | +7.10 | +0.00 | -221.52 | -214.42 |
| 2026-09-21 | `WAY` | 108 | $25.66 | $25.94 | +30.24 | — | +0.00 | +30.24 | -35.64 | — |
| 2026-09-21 | `QCOM` | 15 | $177.72 | $180.61 | +43.35 | — | +0.00 | +43.35 | -128.40 | — |
| 2026-09-21 | `SM` | 71 | $36.97 | $35.91 | -75.26 | — | +0.00 | -75.26 | -289.68 | — |
| 2026-09-21 | `VICR` | 11 | — | $230.25 | +0.00 | $223.90 | -69.85 | -69.85 | +0.00 | -69.85 |
| 2026-09-21 | `SMTC` | 14 | — | $190.30 | +0.00 | $177.37 | -181.02 | -181.02 | +0.00 | -181.02 |
| 2026-09-21 | `GLXY` | 103 | — | $25.95 | +0.00 | $26.07 | +12.36 | +12.36 | +0.00 | +12.36 |
| 2026-09-22 | `VICR` | 11 | $223.90 | $241.04 | +188.54 | $268.34 | +300.30 | +488.84 | +118.69 | +418.99 |
| 2026-09-22 | `SMTC` | 14 | $177.37 | $175.00 | -33.18 | $174.33 | -9.38 | -42.56 | -214.20 | -223.58 |
| 2026-09-22 | `GLXY` | 103 | $26.07 | $25.95 | -12.36 | $27.17 | +125.66 | +113.30 | +0.00 | +125.66 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +140.32 | HLIT, SNDK, ANGX, ARX, MH, VELO | — | $21.33 | $10,124.06 | HLIT×126, SNDK×1, ANGX×386, ARX×85, MH×123, VELO×108 |
| 2026-08-17 | +2.25 | $21.33 | HLIT×126, SNDK×1, ANGX×386, ARX×85, MH×123, VELO×108 | $10,257.05 | +132.99 | -22.75 | — | — | $21.33 | $10,234.29 | HLIT×126, SNDK×1, ANGX×386, ARX×85, MH×123, VELO×108 |
| 2026-08-18 | -6.20 | $21.33 | HLIT×126, SNDK×1, ANGX×386, ARX×85, MH×123, VELO×108 | $10,030.28 | -204.01 | -50.69 | — | — | $21.33 | $9,979.59 | HLIT×126, SNDK×1, ANGX×386, ARX×85, MH×123, VELO×108 |
| 2026-08-19 | -7.20 | $21.33 | HLIT×126, SNDK×1, ANGX×386, ARX×85, MH×123, VELO×108 | $10,009.69 | +30.10 | +0.00 | — | HLIT, SNDK, ANGX, ARX, MH, VELO | $9,993.20 | $9,993.20 | — |
| 2026-08-20 | +1.12 | $9,993.20 | — | $9,993.20 | +0.00 | -161.54 | BHP, APA, AUTL, CRSP, ASST, MRNA, ZLAB | — | $202.26 | $9,811.61 | BHP×15, APA×31, AUTL×577, CRSP×24, ASST×89, MRNA×9, ZLAB×53 |
| 2026-08-21 | +3.25 | $202.26 | BHP×15, APA×31, AUTL×577, CRSP×24, ASST×89, MRNA×9, ZLAB×53 | $10,037.63 | +226.02 | +84.85 | ABTC, HIVE, MARA | — | $119.62 | $10,121.62 | BHP×15, APA×31, AUTL×577, CRSP×24, ASST×89, MRNA×9, ZLAB×53, ABTC×3, HIVE×10, MARA×2 |
| 2026-08-24 | -5.17 | $119.62 | BHP×15, APA×31, AUTL×577, CRSP×24, ASST×89, MRNA×9, ZLAB×53, ABTC×3, HIVE×10, MARA×2 | $10,082.87 | -38.75 | -12.78 | — | — | $119.62 | $10,070.09 | BHP×15, APA×31, AUTL×577, CRSP×24, ASST×89, MRNA×9, ZLAB×53, ABTC×3, HIVE×10, MARA×2 |
| 2026-08-25 | +1.80 | $119.62 | BHP×15, APA×31, AUTL×577, CRSP×24, ASST×89, MRNA×9, ZLAB×53, ABTC×3, HIVE×10, MARA×2 | $10,046.76 | -23.33 | +272.83 | AU, FCX, EZPW | BHP, APA, AUTL, CRSP, ASST, MRNA, ZLAB | $131.98 | $10,292.84 | ABTC×3, HIVE×10, MARA×2, AU×27, FCX×43, EZPW×94 |
| 2026-08-26 | +2.02 | $131.98 | ABTC×3, HIVE×10, MARA×2, AU×27, FCX×43, EZPW×94 | $10,213.14 | -79.70 | -229.45 | — | ABTC, HIVE, MARA | $210.23 | $9,982.80 | AU×27, FCX×43, EZPW×94 |
| 2026-08-27 | — | $210.23 | AU×27, FCX×43, EZPW×94 | $9,918.99 | -63.81 | +94.64 | — | — | $210.23 | $10,013.63 | AU×27, FCX×43, EZPW×94 |
| 2026-08-28 | +0.75 | $210.23 | AU×27, FCX×43, EZPW×94 | $10,049.87 | +36.24 | -246.60 | KEYS, SMTC, CIEN, DDOG, ADSK, SEDG, TLS | AU, FCX, EZPW | $1,970.10 | $9,781.21 | KEYS×3, SMTC×8, CIEN×3, DDOG×5, ADSK×4, SEDG×38, TLS×260 |
| 2026-08-31 | -5.85 | $1,970.10 | KEYS×3, SMTC×8, CIEN×3, DDOG×5, ADSK×4, SEDG×38, TLS×260 | $9,766.26 | -14.95 | +80.14 | — | — | $1,970.10 | $9,846.40 | KEYS×3, SMTC×8, CIEN×3, DDOG×5, ADSK×4, SEDG×38, TLS×260 |
| 2026-09-01 | -6.30 | $1,970.10 | KEYS×3, SMTC×8, CIEN×3, DDOG×5, ADSK×4, SEDG×38, TLS×260 | $9,715.80 | -130.60 | -76.96 | — | — | $1,970.10 | $9,638.84 | KEYS×3, SMTC×8, CIEN×3, DDOG×5, ADSK×4, SEDG×38, TLS×260 |
| 2026-09-02 | -3.83 | $1,970.10 | KEYS×3, SMTC×8, CIEN×3, DDOG×5, ADSK×4, SEDG×38, TLS×260 | $9,605.83 | -33.01 | +0.00 | — | KEYS, SMTC, CIEN, DDOG, ADSK, SEDG, TLS | $9,590.18 | $9,590.18 | — |
| 2026-09-03 | -0.90 | $9,590.18 | — | $9,590.18 | +0.00 | +402.10 | AVGO, DELL, CXW, FRNM, MMED, DE, HPE | — | $1,428.71 | $9,977.70 | AVGO×3, DELL×2, CXW×42, FRNM×86, MMED×57, DE×1, HPE×28 |
| 2026-09-04 | +2.25 | $1,428.71 | AVGO×3, DELL×2, CXW×42, FRNM×86, MMED×57, DE×1, HPE×28 | $9,909.80 | -67.90 | -8.37 | CRM, MRX, BE, BAK | — | $260.44 | $9,892.90 | AVGO×3, DELL×2, CXW×42, FRNM×86, MMED×57, DE×1, HPE×28, CRM×1, MRX×4, BE×1, BAK×184 |
| 2026-09-08 | -11.47 | $260.44 | AVGO×3, DELL×2, CXW×42, FRNM×86, MMED×57, DE×1, HPE×28, CRM×1, MRX×4, BE×1, BAK×184 | $9,947.25 | +54.35 | +99.14 | — | — | $260.44 | $10,046.39 | AVGO×3, DELL×2, CXW×42, FRNM×86, MMED×57, DE×1, HPE×28, CRM×1, MRX×4, BE×1, BAK×184 |
| 2026-09-09 | -13.95 | $260.44 | AVGO×3, DELL×2, CXW×42, FRNM×86, MMED×57, DE×1, HPE×28, CRM×1, MRX×4, BE×1, BAK×184 | $10,081.52 | +35.13 | -21.13 | — | AVGO, DELL, CXW, FRNM, MMED, DE, HPE | $8,866.85 | $10,045.65 | CRM×1, MRX×4, BE×1, BAK×184 |
| 2026-09-10 | -13.28 | $8,866.85 | CRM×1, MRX×4, BE×1, BAK×184 | $10,035.39 | -10.26 | +0.00 | — | CRM, MRX, BE, BAK | $10,026.76 | $10,026.76 | — |
| 2026-09-11 | +0.50 | $10,026.76 | — | $10,026.76 | -0.00 | -849.00 | ORCL | — | $158.79 | $9,175.59 | ORCL×60 |
| 2026-09-14 | -11.00 | $158.79 | ORCL×60 | $8,643.99 | -531.60 | +202.20 | — | — | $158.79 | $8,846.19 | ORCL×60 |
| 2026-09-15 | -3.84 | $158.79 | ORCL×60 | $8,766.39 | -79.80 | -186.60 | — | — | $158.79 | $8,579.79 | ORCL×60 |
| 2026-09-16 | +5.30 | $158.79 | ORCL×60 | $8,560.59 | -19.20 | -160.32 | WAY, QCOM, SM | ORCL | $37.79 | $8,391.47 | WAY×108, QCOM×15, SM×71 |
| 2026-09-17 | +7.38 | $37.79 | WAY×108, QCOM×15, SM×71 | $8,423.59 | +32.12 | -67.20 | — | — | $37.79 | $8,356.39 | WAY×108, QCOM×15, SM×71 |
| 2026-09-18 | +4.86 | $37.79 | WAY×108, QCOM×15, SM×71 | $8,436.26 | +79.87 | -336.52 | — | — | $37.79 | $8,099.74 | WAY×108, QCOM×15, SM×71 |
| 2026-09-21 | +12.87 | $37.79 | WAY×108, QCOM×15, SM×71 | $8,098.07 | -1.67 | -238.51 | VICR, SMTC, GLXY | WAY, QCOM, SM | $215.26 | $7,846.55 | VICR×11, SMTC×14, GLXY×103 |
| 2026-09-22 | -0.50 | $215.26 | VICR×11, SMTC×14, GLXY×103 | $7,989.55 | +143.00 | +416.58 | — | — | $215.26 | $8,406.13 | VICR×11, SMTC×14, GLXY×103 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 126 | $13.18 | $2.37 | — | $8,336.95 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SNDK` | 1 | $1646.93 | $1.99 | — | $6,688.03 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.4; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 386 | $4.31 | $4.98 | — | $5,019.39 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 85 | $19.57 | $2.25 | — | $3,353.69 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 123 | $13.55 | $2.36 | — | $1,684.69 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VELO` | 108 | $15.38 | $2.31 | — | $21.33 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.3; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.33 | ▲ close $10,124.06 vs 09:30 $10,000.00 (session +140.32) | 16:00 close · cash $21.33 · equity $10,124.06 vs 09:30 $10,000.00 (+124.06; session marks +140.32) · 6 name(s) marked open→close (per-name table). HLIT×126 09:30 $13.18 → close $13.92 +93.24; SNDK×1 09:30 $1646.93 → close $1641.11 -5.82; ANGX×386 09:30 $4.31 → close $4.37 +23.16; ARX×85 09:30 $19.57 → close $19.58 +0.85; MH×123 09:30 $13.55 → close $13.10 -55.35; VELO×108 09:30 $15.38 → close $16.16 +84.24 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.33 | ▲ 09:30 equity $10,257.05 vs yday $10,124.06 (+132.99) | 09:30 open · cash $21.33 (unchanged overnight, no fees) · equity $10,257.05 vs prior close $10,124.06 (+132.99) · 6 name(s) re-marked at the open (per-name table). HLIT×126 yday $13.92 → 09:30 $13.84 -10.08; SNDK×1 yday $1641.11 → 09:30 $1700.74 +59.63; ANGX×386 yday $4.37 → 09:30 $4.60 +88.78; ARX×85 yday $19.58 → 09:30 $19.57 -0.85; MH×123 yday $13.10 → 09:30 $13.16 +7.38; VELO×108 yday $16.16 → 09:30 $16.05 -11.88 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.33 | ▼ close $10,234.29 vs 09:30 $10,257.05 (session -22.75) | 16:00 close · cash $21.33 · equity $10,234.29 vs 09:30 $10,257.05 (-22.76; session marks -22.75) · 6 name(s) marked open→close (per-name table). HLIT×126 09:30 $13.84 → close $13.43 -51.66; SNDK×1 09:30 $1700.74 → close $1786.85 +86.11; ANGX×386 09:30 $4.60 → close $4.71 +42.46; ARX×85 09:30 $19.57 → close $19.54 -2.55; MH×123 09:30 $13.16 → close $12.77 -47.97; VELO×108 09:30 $16.05 → close $15.60 -49.14 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.33 | ▼ 09:30 equity $10,030.28 vs yday $10,234.29 (-204.01) | 09:30 open · cash $21.33 (unchanged overnight, no fees) · equity $10,030.28 vs prior close $10,234.29 (-204.01) · 6 name(s) re-marked at the open (per-name table). HLIT×126 yday $13.43 → 09:30 $12.93 -63.00; SNDK×1 yday $1786.85 → 09:30 $1677.54 -109.31; ANGX×386 yday $4.71 → 09:30 $4.79 +30.88; ARX×85 yday $19.54 → 09:30 $19.57 +2.55; MH×123 yday $12.77 → 09:30 $13.00 +28.29; VELO×108 yday $15.60 → 09:30 $14.73 -93.42 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.33 | ▼ close $9,979.59 vs 09:30 $10,030.28 (session -50.69) | 16:00 close · cash $21.33 · equity $9,979.59 vs 09:30 $10,030.28 (-50.69; session marks -50.69) · 6 name(s) marked open→close (per-name table). HLIT×126 09:30 $12.93 → close $12.73 -25.20; SNDK×1 09:30 $1677.54 → close $1625.78 -51.76; ANGX×386 09:30 $4.79 → close $4.85 +23.16; ARX×85 09:30 $19.57 → close $19.56 -0.85; MH×123 09:30 $13.00 → close $13.12 +14.76; VELO×108 09:30 $14.73 → close $14.63 -10.80 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.33 | ▲ 09:30 equity $10,009.69 vs yday $9,979.59 (+30.10) | 09:30 open · cash $21.33 (unchanged overnight, no fees) · equity $10,009.69 vs prior close $9,979.59 (+30.10) · 6 name(s) re-marked at the open (per-name table). HLIT×126 yday $12.73 → 09:30 $12.90 +21.42; SNDK×1 yday $1625.78 → 09:30 $1682.40 +56.62; ANGX×386 yday $4.85 → 09:30 $4.79 -23.16; ARX×85 yday $19.56 → 09:30 $19.58 +1.70; MH×123 yday $13.12 → 09:30 $13.01 -13.53; VELO×108 yday $14.63 → 09:30 $14.51 -12.96 | — |
| 2026-08-19 09:30 ET | **SELL** | `HLIT` | 126 | $12.90 | $2.40 | $-40.05 | $1,644.33 | ▼ -40.05 after sell → book $10,007.28; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `SNDK` | 1 | $1682.40 | $2.02 | $+31.47 | $3,324.72 | ▲ +31.47 after sell → book $10,005.27; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 386 | $4.79 | $5.06 | $+175.24 | $5,168.60 | ▲ +175.24 after sell → book $10,000.21; vs 09:30 mark -5.06 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 85 | $19.58 | $2.27 | $-3.67 | $6,830.63 | ▼ -3.67 after sell → book $9,997.94; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | join🟡 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `MH` | 123 | $13.01 | $2.39 | $-71.17 | $8,428.47 | ▼ -71.17 after sell → book $9,995.55; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `VELO` | 108 | $14.51 | $2.34 | $-98.62 | $9,993.20 | ▼ -98.62 after sell → book $9,993.20; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,993.20 | ▲ close $9,993.20 vs 09:30 $10,009.69 (session +0.00) | 16:00 close · cash $9,993.20 · no lots left · equity $9,993.20. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,993.20 | ▲ 09:30 equity $9,993.20 vs yday $9,993.20 (+0.00) | 09:30 open · cash $9,993.20 · no holdings · equity $9,993.20 vs prior close $9,993.20 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 15 | $91.01 | $2.04 | — | $8,626.02 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1427.60 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 31 | $44.76 | $2.08 | — | $7,236.37 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1427.60 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 577 | $2.47 | $7.44 | — | $5,803.74 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1427.60 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 24 | $58.73 | $2.06 | — | $4,392.16 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1427.60 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 89 | $16.00 | $2.26 | — | $2,965.90 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1427.60 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 9 | $150.14 | $2.02 | — | $1,612.62 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1427.60 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 53 | $26.57 | $2.15 | — | $202.26 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1427.60 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $202.26 | ▼ close $9,811.61 vs 09:30 $9,993.20 (session -161.54) | 16:00 close · cash $202.26 · equity $9,811.61 vs 09:30 $9,993.20 (-181.59; session marks -161.54) · 7 name(s) marked open→close (per-name table). BHP×15 09:30 $91.01 → close $93.63 +39.30; APA×31 09:30 $44.76 → close $44.39 -11.47; AUTL×577 09:30 $2.47 → close $2.46 -5.77; CRSP×24 09:30 $58.73 → close $58.12 -14.64; ASST×89 09:30 $16.00 → close $16.13 +11.57; MRNA×9 09:30 $150.14 → close $133.32 -151.38; ZLAB×53 09:30 $26.57 → close $26.02 -29.15 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $202.26 | ▲ 09:30 equity $10,037.63 vs yday $9,811.61 (+226.02) | 09:30 open · cash $202.26 (unchanged overnight, no fees) · equity $10,037.63 vs prior close $9,811.61 (+226.02) · 7 name(s) re-marked at the open (per-name table). BHP×15 yday $93.63 → 09:30 $95.72 +31.35; APA×31 yday $44.39 → 09:30 $44.52 +4.03; AUTL×577 yday $2.46 → 09:30 $2.47 +5.77; CRSP×24 yday $58.12 → 09:30 $59.72 +38.40; ASST×89 yday $16.13 → 09:30 $17.66 +136.17; MRNA×9 yday $133.32 → 09:30 $133.11 -1.89; ZLAB×53 yday $26.02 → 09:30 $26.25 +12.19 | — |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 3 | $8.66 | $0.27 | — | $176.02 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $33.71 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 10 | $3.24 | $0.35 | — | $143.26 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $33.71 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 2 | $11.70 | $0.24 | — | $119.62 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $33.71 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $119.62 | ▲ close $10,121.62 vs 09:30 $10,037.63 (session +84.85) | 16:00 close · cash $119.62 · equity $10,121.62 vs 09:30 $10,037.63 (+83.99; session marks +84.85) · 10 name(s) marked open→close (per-name table). BHP×15 09:30 $95.72 → close $97.03 +19.65; APA×31 09:30 $44.52 → close $43.39 -35.03; AUTL×577 09:30 $2.47 → close $2.41 -34.62; CRSP×24 09:30 $59.72 → close $59.50 -5.28; ASST×89 09:30 $17.66 → close $18.22 +49.84; MRNA×9 09:30 $133.11 → close $145.13 +108.18; ZLAB×53 09:30 $26.25 → close $26.01 -12.72; ABTC×3 09:30 $8.66 → close $7.93 -2.19; HIVE×10 09:30 $3.24 → close $3.03 -2.10; MARA×2 09:30 $11.70 → close $11.26 -0.88 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $119.62 | ▼ 09:30 equity $10,082.87 vs yday $10,121.62 (-38.75) | 09:30 open · cash $119.62 (unchanged overnight, no fees) · equity $10,082.87 vs prior close $10,121.62 (-38.75) · 10 name(s) re-marked at the open (per-name table). BHP×15 yday $97.03 → 09:30 $97.31 +4.20; APA×31 yday $43.39 → 09:30 $42.93 -14.26; AUTL×577 yday $2.41 → 09:30 $2.40 -5.77; CRSP×24 yday $59.50 → 09:30 $58.75 -18.00; ASST×89 yday $18.22 → 09:30 $18.76 +48.06; MRNA×9 yday $145.13 → 09:30 $142.70 -21.87; ZLAB×53 yday $26.01 → 09:30 $25.43 -30.74; ABTC×3 yday $7.93 → 09:30 $8.00 +0.21; HIVE×10 yday $3.03 → 09:30 $2.99 -0.40; MARA×2 yday $11.26 → 09:30 $11.17 -0.18 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $119.62 | ▼ close $10,070.09 vs 09:30 $10,082.87 (session -12.78) | 16:00 close · cash $119.62 · equity $10,070.09 vs 09:30 $10,082.87 (-12.78; session marks -12.78) · 10 name(s) marked open→close (per-name table). BHP×15 09:30 $97.31 → close $97.13 -2.70; APA×31 09:30 $42.93 → close $42.96 +0.93; AUTL×577 09:30 $2.40 → close $2.34 -34.62; CRSP×24 09:30 $58.75 → close $57.08 -40.20; ASST×89 09:30 $18.76 → close $19.73 +86.33; MRNA×9 09:30 $142.70 → close $138.89 -34.29; ZLAB×53 09:30 $25.43 → close $25.64 +11.13; ABTC×3 09:30 $8.00 → close $8.64 +1.92; HIVE×10 09:30 $2.99 → close $2.86 -1.30; MARA×2 09:30 $11.17 → close $11.18 +0.02 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $119.62 | ▼ 09:30 equity $10,046.76 vs yday $10,070.09 (-23.33) | 09:30 open · cash $119.62 (unchanged overnight, no fees) · equity $10,046.76 vs prior close $10,070.09 (-23.33) · 10 name(s) re-marked at the open (per-name table). BHP×15 yday $97.13 → 09:30 $95.86 -19.05; APA×31 yday $42.96 → 09:30 $41.38 -48.98; AUTL×577 yday $2.34 → 09:30 $2.38 +23.08; CRSP×24 yday $57.08 → 09:30 $57.93 +20.52; ASST×89 yday $19.73 → 09:30 $19.04 -61.41; MRNA×9 yday $138.89 → 09:30 $143.50 +41.49; ZLAB×53 yday $25.64 → 09:30 $26.04 +21.20; ABTC×3 yday $8.64 → 09:30 $8.62 -0.06; HIVE×10 yday $2.86 → 09:30 $2.87 +0.10; MARA×2 yday $11.18 → 09:30 $11.07 -0.22 | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 15 | $95.86 | $2.06 | $+68.66 | $1,555.47 | ▲ +68.66 after sell → book $10,044.71; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 31 | $41.38 | $2.10 | $-108.97 | $2,836.14 | ▼ -108.97 after sell → book $10,042.60; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 577 | $2.38 | $7.55 | $-66.92 | $4,201.85 | ▼ -66.92 after sell → book $10,035.05; vs 09:30 mark -7.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 24 | $57.93 | $2.08 | $-23.35 | $5,590.09 | ▼ -23.35 after sell → book $10,032.97; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ASST` | 89 | $19.04 | $2.29 | $+266.02 | $7,282.36 | ▲ +266.02 after sell → book $10,030.68; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 9 | $143.50 | $2.04 | $-63.81 | $8,571.83 | ▼ -63.81 after sell → book $10,028.65; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ZLAB` | 53 | $26.04 | $2.17 | $-32.41 | $9,949.78 | ▼ -32.41 after sell → book $10,026.48; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 27 | $118.52 | $2.07 | — | $6,747.67 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $3316.59 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 43 | $77.13 | $2.12 | — | $3,428.96 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $3316.59 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 94 | $35.05 | $2.27 | — | $131.98 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $3316.59 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $131.98 | ▲ close $10,292.84 vs 09:30 $10,046.76 (session +272.83) | 16:00 close · cash $131.98 · equity $10,292.84 vs 09:30 $10,046.76 (+246.08; session marks +272.83) · 6 name(s) marked open→close (per-name table). ABTC×3 09:30 $8.62 → close $9.24 +1.86; HIVE×10 09:30 $2.87 → close $3.02 +1.50; MARA×2 09:30 $11.07 → close $11.83 +1.52; AU×27 09:30 $118.52 → close $123.39 +131.49; FCX×43 09:30 $77.13 → close $79.91 +119.54; EZPW×94 09:30 $35.05 → close $35.23 +16.92 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $131.98 | ▼ 09:30 equity $10,213.14 vs yday $10,292.84 (-79.70) | 09:30 open · cash $131.98 (unchanged overnight, no fees) · equity $10,213.14 vs prior close $10,292.84 (-79.70) · 6 name(s) re-marked at the open (per-name table). ABTC×3 yday $9.24 → 09:30 $8.84 -1.20; HIVE×10 yday $3.02 → 09:30 $2.95 -0.70; MARA×2 yday $11.83 → 09:30 $11.56 -0.54; AU×27 yday $123.39 → 09:30 $119.80 -96.93; FCX×43 yday $79.91 → 09:30 $79.34 -24.51; EZPW×94 yday $35.23 → 09:30 $35.70 +44.18 | — |
| 2026-08-26 09:30 ET | **SELL** | `ABTC` | 3 | $8.84 | $0.29 | $-0.02 | $158.21 | ▼ -0.02 after sell → book $10,212.85; vs 09:30 mark -0.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `HIVE` | 10 | $2.95 | $0.34 | $-3.60 | $187.36 | ▼ -3.60 after sell → book $10,212.50; vs 09:30 mark -0.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `MARA` | 2 | $11.56 | $0.26 | $-0.78 | $210.23 | ▼ -0.78 after sell → book $10,212.25; vs 09:30 mark -0.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $210.23 | ▼ close $9,982.80 vs 09:30 $10,213.14 (session -229.45) | 16:00 close · cash $210.23 · equity $9,982.80 vs 09:30 $10,213.14 (-230.34; session marks -229.45) · 3 name(s) marked open→close (per-name table). AU×27 09:30 $119.80 → close $118.11 -45.63; FCX×43 09:30 $79.34 → close $79.00 -14.62; EZPW×94 09:30 $35.70 → close $33.90 -169.20 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $210.23 | ▼ 09:30 equity $9,918.99 vs yday $9,982.80 (-63.81) | 09:30 open · cash $210.23 (unchanged overnight, no fees) · equity $9,918.99 vs prior close $9,982.80 (-63.81) · 3 name(s) re-marked at the open (per-name table). AU×27 yday $118.11 → 09:30 $117.41 -18.90; FCX×43 yday $79.00 → 09:30 $78.83 -7.31; EZPW×94 yday $33.90 → 09:30 $33.50 -37.60 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $210.23 | ▲ close $10,013.63 vs 09:30 $9,918.99 (session +94.64) | 16:00 close · cash $210.23 · equity $10,013.63 vs 09:30 $9,918.99 (+94.64; session marks +94.64) · 3 name(s) marked open→close (per-name table). AU×27 09:30 $117.41 → close $118.40 +26.73; FCX×43 09:30 $78.83 → close $78.42 -17.63; EZPW×94 09:30 $33.50 → close $34.41 +85.54 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $210.23 | ▲ 09:30 equity $10,049.87 vs yday $10,013.63 (+36.24) | 09:30 open · cash $210.23 (unchanged overnight, no fees) · equity $10,049.87 vs prior close $10,013.63 (+36.24) · 3 name(s) re-marked at the open (per-name table). AU×27 yday $118.40 → 09:30 $119.19 +21.33; FCX×43 yday $78.42 → 09:30 $78.57 +6.45; EZPW×94 yday $34.41 → 09:30 $34.50 +8.46 | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 27 | $119.19 | $2.11 | $+13.91 | $3,426.25 | ▲ +13.91 after sell → book $10,047.76; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 43 | $78.57 | $2.16 | $+57.64 | $6,802.60 | ▲ +57.64 after sell → book $10,045.60; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 94 | $34.50 | $2.31 | $-56.29 | $10,043.29 | ▼ -56.29 after sell → book $10,043.29; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 3 | $324.41 | $2.00 | — | $9,068.06 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1255.41 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $141.76 | $2.01 | — | $7,931.97 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1255.41 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,728.71 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1255.41 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $5,525.60 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1255.41 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $4,478.96 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; ret5=+7.8; leftover $1255.41 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 38 | $32.90 | $2.10 | — | $3,226.66 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1255.41 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 260 | $4.82 | $3.35 | — | $1,970.10 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1255.41 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,970.10 | ▼ close $9,781.21 vs 09:30 $10,049.87 (session -246.60) | 16:00 close · cash $1,970.10 · equity $9,781.21 vs 09:30 $10,049.87 (-268.66; session marks -246.60) · 7 name(s) marked open→close (per-name table). KEYS×3 09:30 $324.41 → close $319.97 -13.32; SMTC×8 09:30 $141.76 → close $131.17 -84.72; CIEN×3 09:30 $400.42 → close $378.44 -65.94; DDOG×5 09:30 $240.22 → close $236.98 -16.20; ADSK×4 09:30 $261.16 → close $260.66 -2.00; SEDG×38 09:30 $32.90 → close $31.41 -56.62; TLS×260 09:30 $4.82 → close $4.79 -7.80 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,970.10 | ▼ 09:30 equity $9,766.26 vs yday $9,781.21 (-14.95) | 09:30 open · cash $1,970.10 (unchanged overnight, no fees) · equity $9,766.26 vs prior close $9,781.21 (-14.95) · 7 name(s) re-marked at the open (per-name table). KEYS×3 yday $319.97 → 09:30 $322.49 +7.56; SMTC×8 yday $131.17 → 09:30 $132.30 +9.04; CIEN×3 yday $378.44 → 09:30 $378.44 +0.00; DDOG×5 yday $236.98 → 09:30 $233.97 -15.07; ADSK×4 yday $260.66 → 09:30 $257.71 -11.80; SEDG×38 yday $31.41 → 09:30 $31.15 -9.88; TLS×260 yday $4.79 → 09:30 $4.81 +5.20 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,970.10 | ▲ close $9,846.40 vs 09:30 $9,766.26 (session +80.14) | 16:00 close · cash $1,970.10 · equity $9,846.40 vs 09:30 $9,766.26 (+80.14; session marks +80.14) · 7 name(s) marked open→close (per-name table). KEYS×3 09:30 $322.49 → close $322.70 +0.63; SMTC×8 09:30 $132.30 → close $132.96 +5.28; CIEN×3 09:30 $378.44 → close $382.80 +13.08; DDOG×5 09:30 $233.97 → close $237.04 +15.37; ADSK×4 09:30 $257.71 → close $258.53 +3.28; SEDG×38 09:30 $31.15 → close $32.20 +39.90; TLS×260 09:30 $4.81 → close $4.82 +2.60 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,970.10 | ▼ 09:30 equity $9,715.80 vs yday $9,846.40 (-130.60) | 09:30 open · cash $1,970.10 (unchanged overnight, no fees) · equity $9,715.80 vs prior close $9,846.40 (-130.60) · 7 name(s) re-marked at the open (per-name table). KEYS×3 yday $322.70 → 09:30 $321.47 -3.69; SMTC×8 yday $132.96 → 09:30 $127.63 -42.64; CIEN×3 yday $382.80 → 09:30 $376.89 -17.73; DDOG×5 yday $237.04 → 09:30 $232.88 -20.80; ADSK×4 yday $258.53 → 09:30 $253.48 -20.20; SEDG×38 yday $32.20 → 09:30 $31.87 -12.54; TLS×260 yday $4.82 → 09:30 $4.77 -13.00 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,970.10 | ▼ close $9,638.84 vs 09:30 $9,715.80 (session -76.96) | 16:00 close · cash $1,970.10 · equity $9,638.84 vs 09:30 $9,715.80 (-76.96; session marks -76.96) · 7 name(s) marked open→close (per-name table). KEYS×3 09:30 $321.47 → close $319.27 -6.60; SMTC×8 09:30 $127.63 → close $132.27 +37.12; CIEN×3 09:30 $376.89 → close $360.33 -49.68; DDOG×5 09:30 $232.88 → close $223.84 -45.20; ADSK×4 09:30 $253.48 → close $247.69 -23.16; SEDG×38 09:30 $31.87 → close $32.49 +23.56; TLS×260 09:30 $4.77 → close $4.72 -13.00 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,970.10 | ▼ 09:30 equity $9,605.83 vs yday $9,638.84 (-33.01) | 09:30 open · cash $1,970.10 (unchanged overnight, no fees) · equity $9,605.83 vs prior close $9,638.84 (-33.01) · 7 name(s) re-marked at the open (per-name table). KEYS×3 yday $319.27 → 09:30 $318.04 -3.69; SMTC×8 yday $132.27 → 09:30 $133.00 +5.84; CIEN×3 yday $360.33 → 09:30 $357.25 -9.24; DDOG×5 yday $223.84 → 09:30 $219.46 -21.90; ADSK×4 yday $247.69 → 09:30 $246.70 -3.96; SEDG×38 yday $32.49 → 09:30 $32.42 -2.66; TLS×260 yday $4.72 → 09:30 $4.73 +2.60 | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 3 | $318.04 | $2.02 | $-23.13 | $2,922.21 | ▼ -23.13 after sell → book $9,603.82; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $133.00 | $2.03 | $-74.13 | $3,984.17 | ▼ -74.13 after sell → book $9,601.78; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 3 | $357.25 | $2.02 | $-133.53 | $5,053.90 | ▼ -133.53 after sell → book $9,599.76; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | join🔴 sector🟢 gen🔴 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `DDOG` | 5 | $219.46 | $2.02 | $-107.83 | $6,149.18 | ▼ -107.83 after sell → book $9,597.74; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 4 | $246.70 | $2.02 | $-61.86 | $7,133.96 | ▼ -61.86 after sell → book $9,595.72; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 38 | $32.42 | $2.12 | $-22.47 | $8,363.79 | ▼ -22.47 after sell → book $9,593.59; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TLS` | 260 | $4.73 | $3.41 | $-30.16 | $9,590.18 | ▼ -30.16 after sell → book $9,590.18; vs 09:30 mark -3.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,590.18 | ▲ close $9,590.18 vs 09:30 $9,605.83 (session +0.00) | 16:00 close · cash $9,590.18 · no lots left · equity $9,590.18. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,590.18 | ▲ 09:30 equity $9,590.18 vs yday $9,590.18 (+0.00) | 09:30 open · cash $9,590.18 · no holdings · equity $9,590.18 vs prior close $9,590.18 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $8,532.97 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1370.03 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $7,558.35 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1370.03 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 42 | $32.31 | $2.12 | — | $6,199.21 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1370.03 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 86 | $15.87 | $2.25 | — | $4,832.15 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1370.03 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 57 | $23.88 | $2.16 | — | $3,468.82 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1370.03 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $2,763.58 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1370.03 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 28 | $47.60 | $2.07 | — | $1,428.71 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $1370.03 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,428.71 | ▲ close $9,977.70 vs 09:30 $9,590.18 (session +402.10) | 16:00 close · cash $1,428.71 · equity $9,977.70 vs 09:30 $9,590.18 (+387.52; session marks +402.10) · 7 name(s) marked open→close (per-name table). AVGO×3 09:30 $351.74 → close $357.16 +16.26; DELL×2 09:30 $486.31 → close $516.39 +60.16; CXW×42 09:30 $32.31 → close $33.66 +56.70; FRNM×86 09:30 $15.87 → close $16.90 +88.58; MMED×57 09:30 $23.88 → close $23.84 -2.28; DE×1 09:30 $703.25 → close $694.41 -8.84; HPE×28 09:30 $47.60 → close $54.44 +191.52 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,428.71 | ▼ 09:30 equity $9,909.80 vs yday $9,977.70 (-67.90) | 09:30 open · cash $1,428.71 (unchanged overnight, no fees) · equity $9,909.80 vs prior close $9,977.70 (-67.90) · 7 name(s) re-marked at the open (per-name table). AVGO×3 yday $357.16 → 09:30 $359.70 +7.62; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; CXW×42 yday $33.66 → 09:30 $33.46 -8.40; FRNM×86 yday $16.90 → 09:30 $16.40 -43.00; MMED×57 yday $23.84 → 09:30 $23.84 +0.00; DE×1 yday $694.41 → 09:30 $692.03 -2.38; HPE×28 yday $54.44 → 09:30 $53.85 -16.52 | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 1 | $263.36 | $1.99 | — | $1,163.35 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $357.18 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 4 | $75.65 | $2.00 | — | $858.75 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $357.18 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 1 | $236.82 | $1.99 | — | $619.94 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+8.1; leftover $357.18 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 184 | $1.94 | $2.54 | — | $260.44 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $357.18 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $260.44 | ▼ close $9,892.90 vs 09:30 $9,909.80 (session -8.37) | 16:00 close · cash $260.44 · equity $9,892.90 vs 09:30 $9,909.80 (-16.90; session marks -8.37) · 11 name(s) marked open→close (per-name table). AVGO×3 09:30 $359.70 → close $357.90 -5.40; DELL×2 09:30 $513.78 → close $524.14 +20.72; CXW×42 09:30 $33.46 → close $34.71 +52.50; FRNM×86 09:30 $16.40 → close $16.31 -7.74; MMED×57 09:30 $23.84 → close $23.29 -31.35; DE×1 09:30 $692.03 → close $693.53 +1.50; HPE×28 09:30 $53.85 → close $52.00 -51.80; CRM×1 09:30 $263.36 → close $259.23 -4.13; MRX×4 09:30 $75.65 → close $78.27 +10.48; BE×1 09:30 $236.82 → close $252.87 +16.05; BAK×184 09:30 $1.94 → close $1.89 -9.20 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $260.44 | ▲ 09:30 equity $9,947.25 vs yday $9,892.90 (+54.35) | 09:30 open · cash $260.44 (unchanged overnight, no fees) · equity $9,947.25 vs prior close $9,892.90 (+54.35) · 11 name(s) re-marked at the open (per-name table). AVGO×3 yday $357.90 → 09:30 $363.68 +17.34; DELL×2 yday $524.14 → 09:30 $521.15 -5.98; CXW×42 yday $34.71 → 09:30 $34.49 -9.24; FRNM×86 yday $16.31 → 09:30 $16.74 +36.98; MMED×57 yday $23.29 → 09:30 $23.16 -7.41; DE×1 yday $693.53 → 09:30 $687.21 -6.32; HPE×28 yday $52.00 → 09:30 $52.29 +8.12; CRM×1 yday $259.23 → 09:30 $253.72 -5.51; MRX×4 yday $78.27 → 09:30 $78.84 +2.28; BE×1 yday $252.87 → 09:30 $267.76 +14.89; BAK×184 yday $1.89 → 09:30 $1.94 +9.20 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $260.44 | ▲ close $10,046.39 vs 09:30 $9,947.25 (session +99.14) | 16:00 close · cash $260.44 · equity $10,046.39 vs 09:30 $9,947.25 (+99.14; session marks +99.14) · 11 name(s) marked open→close (per-name table). AVGO×3 09:30 $363.68 → close $368.56 +14.64; DELL×2 09:30 $521.15 → close $533.88 +25.46; CXW×42 09:30 $34.49 → close $35.05 +23.52; FRNM×86 09:30 $16.74 → close $15.99 -64.50; MMED×57 09:30 $23.16 → close $23.32 +9.12; DE×1 09:30 $687.21 → close $680.73 -6.48; HPE×28 09:30 $52.29 → close $56.03 +104.72; CRM×1 09:30 $253.72 → close $249.12 -4.60; MRX×4 09:30 $78.84 → close $76.71 -8.52; BE×1 09:30 $267.76 → close $277.22 +9.46; BAK×184 09:30 $1.94 → close $1.92 -3.68 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $260.44 | ▲ 09:30 equity $10,081.52 vs yday $10,046.39 (+35.13) | 09:30 open · cash $260.44 (unchanged overnight, no fees) · equity $10,081.52 vs prior close $10,046.39 (+35.13) · 11 name(s) re-marked at the open (per-name table). AVGO×3 yday $368.56 → 09:30 $366.23 -6.99; DELL×2 yday $533.88 → 09:30 $538.47 +9.18; CXW×42 yday $35.05 → 09:30 $35.09 +1.68; FRNM×86 yday $15.99 → 09:30 $15.96 -2.58; MMED×57 yday $23.32 → 09:30 $23.22 -5.70; DE×1 yday $680.73 → 09:30 $681.32 +0.59; HPE×28 yday $56.03 → 09:30 $56.94 +25.48; CRM×1 yday $249.12 → 09:30 $249.78 +0.66; MRX×4 yday $76.71 → 09:30 $76.60 -0.44; BE×1 yday $277.22 → 09:30 $272.99 -4.23; BAK×184 yday $1.92 → 09:30 $2.02 +17.48 | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 3 | $366.23 | $2.02 | $+39.45 | $1,357.11 | ▲ +39.45 after sell → book $10,079.50; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DELL` | 2 | $538.47 | $2.02 | $+100.31 | $2,432.03 | ▲ +100.31 after sell → book $10,077.48; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CXW` | 42 | $35.09 | $2.14 | $+112.51 | $3,903.67 | ▲ +112.51 after sell → book $10,075.34; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FRNM` | 86 | $15.96 | $2.27 | $+3.22 | $5,273.96 | ▲ +3.22 after sell → book $10,073.07; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 57 | $23.22 | $2.18 | $-41.96 | $6,595.32 | ▼ -41.96 after sell → book $10,070.89; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DE` | 1 | $681.32 | $2.01 | $-25.94 | $7,274.63 | ▼ -25.94 after sell → book $10,068.88; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 28 | $56.94 | $2.10 | $+257.35 | $8,866.85 | ▲ +257.35 after sell → book $10,066.78; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,866.85 | ▼ close $10,045.65 vs 09:30 $10,081.52 (session -21.13) | 16:00 close · cash $8,866.85 · equity $10,045.65 vs 09:30 $10,081.52 (-35.87; session marks -21.13) · 4 name(s) marked open→close (per-name table). CRM×1 09:30 $249.78 → close $244.16 -5.62; MRX×4 09:30 $76.60 → close $75.72 -3.52; BE×1 09:30 $272.99 → close $269.28 -3.71; BAK×184 09:30 $2.02 → close $1.97 -8.28 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,866.85 | ▼ 09:30 equity $10,035.39 vs yday $10,045.65 (-10.26) | 09:30 open · cash $8,866.85 (unchanged overnight, no fees) · equity $10,035.39 vs prior close $10,045.65 (-10.26) · 4 name(s) re-marked at the open (per-name table). CRM×1 yday $244.16 → 09:30 $245.35 +1.19; MRX×4 yday $75.72 → 09:30 $75.00 -2.88; BE×1 yday $269.28 → 09:30 $260.71 -8.57; BAK×184 yday $1.97 → 09:30 $1.97 +0.00 | — |
| 2026-09-10 09:30 ET | **SELL** | `CRM` | 1 | $245.35 | $2.01 | $-22.02 | $9,110.19 | ▼ -22.02 after sell → book $10,033.37; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MRX` | 4 | $75.00 | $2.02 | $-6.62 | $9,408.16 | ▼ -6.62 after sell → book $10,031.35; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BE` | 1 | $260.71 | $2.01 | $+19.88 | $9,666.86 | ▲ +19.88 after sell → book $10,029.34; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BAK` | 184 | $1.97 | $2.58 | $+0.40 | $10,026.76 | ▲ +0.40 after sell → book $10,026.76; vs 09:30 mark -2.58 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,026.76 | ▲ close $10,026.76 vs 09:30 $10,035.39 (session +0.00) | 16:00 close · cash $10,026.76 · no lots left · equity $10,026.76. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,026.76 | ▲ 09:30 equity $10,026.76 vs yday $10,026.76 (-0.00) | 09:30 open · cash $10,026.76 · no holdings · equity $10,026.76 vs prior close $10,026.76 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 60 | $164.43 | $2.17 | — | $158.79 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $10026.76 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.79 | ▼ close $9,175.59 vs 09:30 $10,026.76 (session -849.00) | 16:00 close · cash $158.79 · equity $9,175.59 vs 09:30 $10,026.76 (-851.17; session marks -849.00) · 1 name(s) marked open→close (per-name table). ORCL×60 09:30 $164.43 → close $150.28 -849.00 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.79 | ▼ 09:30 equity $8,643.99 vs yday $9,175.59 (-531.60) | 09:30 open · cash $158.79 (unchanged overnight, no fees) · equity $8,643.99 vs prior close $9,175.59 (-531.60) · 1 name(s) re-marked at the open (per-name table). ORCL×60 yday $150.28 → 09:30 $141.42 -531.60 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.79 | ▲ close $8,846.19 vs 09:30 $8,643.99 (session +202.20) | 16:00 close · cash $158.79 · equity $8,846.19 vs 09:30 $8,643.99 (+202.20; session marks +202.20) · 1 name(s) marked open→close (per-name table). ORCL×60 09:30 $141.42 → close $144.79 +202.20 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.79 | ▼ 09:30 equity $8,766.39 vs yday $8,846.19 (-79.80) | 09:30 open · cash $158.79 (unchanged overnight, no fees) · equity $8,766.39 vs prior close $8,846.19 (-79.80) · 1 name(s) re-marked at the open (per-name table). ORCL×60 yday $144.79 → 09:30 $143.46 -79.80 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.79 | ▼ close $8,579.79 vs 09:30 $8,766.39 (session -186.60) | 16:00 close · cash $158.79 · equity $8,579.79 vs 09:30 $8,766.39 (-186.60; session marks -186.60) · 1 name(s) marked open→close (per-name table). ORCL×60 09:30 $143.46 → close $140.35 -186.60 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.79 | ▼ 09:30 equity $8,560.59 vs yday $8,579.79 (-19.20) | 09:30 open · cash $158.79 (unchanged overnight, no fees) · equity $8,560.59 vs prior close $8,579.79 (-19.20) · 1 name(s) re-marked at the open (per-name table). ORCL×60 yday $140.35 → 09:30 $140.03 -19.20 | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 60 | $140.03 | $2.25 | $-1468.42 | $8,558.34 | ▼ -1,468.42 after sell → book $8,558.34; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 108 | $26.27 | $2.31 | — | $5,718.87 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2852.78 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 15 | $189.17 | $2.04 | — | $2,879.28 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $2852.78 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 71 | $39.99 | $2.20 | — | $37.79 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2852.78 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.79 | ▼ close $8,391.47 vs 09:30 $8,560.59 (session -160.32) | 16:00 close · cash $37.79 · equity $8,391.47 vs 09:30 $8,560.59 (-169.12; session marks -160.32) · 3 name(s) marked open→close (per-name table). WAY×108 09:30 $26.27 → close $26.59 +34.56; QCOM×15 09:30 $189.17 → close $184.84 -64.95; SM×71 09:30 $39.99 → close $38.16 -129.93 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.79 | ▲ 09:30 equity $8,423.59 vs yday $8,391.47 (+32.12) | 09:30 open · cash $37.79 (unchanged overnight, no fees) · equity $8,423.59 vs prior close $8,391.47 (+32.12) · 3 name(s) re-marked at the open (per-name table). WAY×108 yday $26.59 → 09:30 $26.51 -8.64; QCOM×15 yday $184.84 → 09:30 $190.35 +82.65; SM×71 yday $38.16 → 09:30 $37.57 -41.89 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.79 | ▼ close $8,356.39 vs 09:30 $8,423.59 (session -67.20) | 16:00 close · cash $37.79 · equity $8,356.39 vs 09:30 $8,423.59 (-67.20; session marks -67.20) · 3 name(s) marked open→close (per-name table). WAY×108 09:30 $26.51 → close $26.51 +0.00; QCOM×15 09:30 $190.35 → close $188.71 -24.60; SM×71 09:30 $37.57 → close $36.97 -42.60 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.79 | ▲ 09:30 equity $8,436.26 vs yday $8,356.39 (+79.87) | 09:30 open · cash $37.79 (unchanged overnight, no fees) · equity $8,436.26 vs prior close $8,356.39 (+79.87) · 3 name(s) re-marked at the open (per-name table). WAY×108 yday $26.51 → 09:30 $26.95 +47.52; QCOM×15 yday $188.71 → 09:30 $191.34 +39.45; SM×71 yday $36.97 → 09:30 $36.87 -7.10 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.79 | ▼ close $8,099.74 vs 09:30 $8,436.26 (session -336.52) | 16:00 close · cash $37.79 · equity $8,099.74 vs 09:30 $8,436.26 (-336.52; session marks -336.52) · 3 name(s) marked open→close (per-name table). WAY×108 09:30 $26.95 → close $25.66 -139.32; QCOM×15 09:30 $191.34 → close $177.72 -204.30; SM×71 09:30 $36.87 → close $36.97 +7.10 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.79 | ▼ 09:30 equity $8,098.07 vs yday $8,099.74 (-1.67) | 09:30 open · cash $37.79 (unchanged overnight, no fees) · equity $8,098.07 vs prior close $8,099.74 (-1.67) · 3 name(s) re-marked at the open (per-name table). WAY×108 yday $25.66 → 09:30 $25.94 +30.24; QCOM×15 yday $177.72 → 09:30 $180.61 +43.35; SM×71 yday $36.97 → 09:30 $35.91 -75.26 | — |
| 2026-09-21 09:30 ET | **SELL** | `WAY` | 108 | $25.94 | $2.35 | $-40.31 | $2,836.95 | ▼ -40.31 after sell → book $8,095.71; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QCOM` | 15 | $180.61 | $2.07 | $-132.50 | $5,544.04 | ▼ -132.50 after sell → book $8,093.65; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SM` | 71 | $35.91 | $2.24 | $-294.12 | $8,091.41 | ▼ -294.12 after sell → book $8,091.41; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 11 | $230.25 | $2.02 | — | $5,556.64 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+12.5; leftover $2697.14 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 14 | $190.30 | $2.03 | — | $2,890.41 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+10.6; leftover $2697.14 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 103 | $25.95 | $2.30 | — | $215.26 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $2697.14 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $215.26 | ▼ close $7,846.55 vs 09:30 $8,098.07 (session -238.51) | 16:00 close · cash $215.26 · equity $7,846.55 vs 09:30 $8,098.07 (-251.52; session marks -238.51) · 3 name(s) marked open→close (per-name table). VICR×11 09:30 $230.25 → close $223.90 -69.85; SMTC×14 09:30 $190.30 → close $177.37 -181.02; GLXY×103 09:30 $25.95 → close $26.07 +12.36 | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $215.26 | ▲ 09:30 equity $7,989.55 vs yday $7,846.55 (+143.00) | 09:30 open · cash $215.26 (unchanged overnight, no fees) · equity $7,989.55 vs prior close $7,846.55 (+143.00) · 3 name(s) re-marked at the open (per-name table). VICR×11 yday $223.90 → 09:30 $241.04 +188.54; SMTC×14 yday $177.37 → 09:30 $175.00 -33.18; GLXY×103 yday $26.07 → 09:30 $25.95 -12.36 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $215.26 | ▲ close $8,406.13 vs 09:30 $7,989.55 (session +416.58) | 16:00 close · cash $215.26 · equity $8,406.13 vs 09:30 $7,989.55 (+416.58; session marks +416.58) · 3 name(s) marked open→close (per-name table). VICR×11 09:30 $241.04 → close $268.34 +300.30; SMTC×14 09:30 $175.00 → close $174.33 -9.38; GLXY×103 09:30 $25.95 → close $27.17 +125.66 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `HLIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `SNDK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `VELO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 7.11 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 7.11 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 7.11 < 1 share @ 202.70 |
| 2026-08-18 | `HLIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `SNDK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VELO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `APA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ZLAB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 33.71 < 1 share @ 119.43 |
| 2026-08-21 | `FUTU` | cash | leftover split 33.71 < 1 share @ 115.18 |
| 2026-08-21 | `GRAL` | cash | leftover split 33.71 < 1 share @ 78.88 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `APA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ZLAB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ABTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `HIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FNV` | cash | leftover split 105.11 < 1 share @ 267.02 |
| 2026-08-26 | `CM` | cash | leftover split 105.11 < 1 share @ 118.50 |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 26.28 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 26.28 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 26.28 < 1 share @ 1746.53 |
| 2026-08-27 | `CM` | cash | leftover split 26.28 < 1 share @ 118.77 |
| 2026-08-27 | `GEN` | cash | leftover split 26.28 < 1 share @ 29.83 |
| 2026-08-27 | `LRCX` | cash | leftover split 26.28 < 1 share @ 318.88 |
| 2026-08-27 | `NVDA` | cash | leftover split 26.28 < 1 share @ 222.86 |
| 2026-08-27 | `ADSK` | cash | leftover split 26.28 < 1 share @ 261.47 |
| 2026-08-28 | `MPWR` | cash | leftover split 1255.41 < 1 share @ 1306.03 |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `DDOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ADSK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DDOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ADSK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CXW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HPE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CXW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRNM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HPE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `MRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `CRM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `MRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `WAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SMTC` | cash | leftover split 9.45 < 1 share @ 170.85 |
| 2026-09-17 | `AVTR` | cash | leftover split 9.45 < 1 share @ 15.81 |
| 2026-09-17 | `GME` | cash | leftover split 9.45 < 1 share @ 22.12 |
| 2026-09-17 | `JBHT` | cash | leftover split 9.45 < 1 share @ 238.60 |
| 2026-09-18 | `WAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TH` | cash | leftover split 9.45 < 1 share @ 20.91 |
| 2026-09-18 | `GME` | cash | leftover split 9.45 < 1 share @ 22.90 |
| 2026-09-18 | `RARE` | cash | leftover split 9.45 < 1 share @ 14.79 |
| 2026-09-18 | `BHVN` | cash | leftover split 9.45 < 1 share @ 14.07 |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GLXY` | min_hold | dropped but min-hold 1/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `VICR` | 11 | 2026-09-21 @ $230.25 | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+12.5; leftover $2697.14 |
| `SMTC` | 14 | 2026-09-21 @ $190.30 | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+10.6; leftover $2697.14 |
| `GLXY` | 103 | 2026-09-21 @ $25.95 | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $2697.14 |
