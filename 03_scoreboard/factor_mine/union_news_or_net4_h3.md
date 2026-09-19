# Factor mine action — `union_news_or_net4_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 OR headline🟢 and camera net ≥ 4

Cash book **-12.35%** ($8,765) · signal-only (no cash/fees) was -11.78%. Starts YES **1/26**. Fills 88 · skips 134 · realized $-787.96.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $44.39.

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
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `APA` | 27 | — | $44.76 | +0.00 | $44.39 | -9.99 | -9.99 | +0.00 | -9.99 |
| 2026-08-20 | `AUTL` | 505 | — | $2.47 | +0.00 | $2.46 | -5.05 | -5.05 | +0.00 | -5.05 |
| 2026-08-20 | `CRSP` | 21 | — | $58.73 | +0.00 | $58.12 | -12.81 | -12.81 | +0.00 | -12.81 |
| 2026-08-20 | `MRK` | 8 | — | $150.78 | +0.00 | $148.99 | -14.32 | -14.32 | +0.00 | -14.32 |
| 2026-08-20 | `ASST` | 78 | — | $16.00 | +0.00 | $16.13 | +10.14 | +10.14 | +0.00 | +10.14 |
| 2026-08-20 | `MRNA` | 8 | — | $150.14 | +0.00 | $133.32 | -134.56 | -134.56 | +0.00 | -134.56 |
| 2026-08-20 | `ZLAB` | 47 | — | $26.57 | +0.00 | $26.02 | -25.85 | -25.85 | +0.00 | -25.85 |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | $97.03 | +17.03 | +44.20 | +61.23 | +78.26 |
| 2026-08-21 | `APA` | 27 | $44.39 | $44.52 | +3.51 | $43.39 | -30.51 | -27.00 | -6.48 | -36.99 |
| 2026-08-21 | `AUTL` | 505 | $2.46 | $2.47 | +5.05 | $2.41 | -30.30 | -25.25 | +0.00 | -30.30 |
| 2026-08-21 | `CRSP` | 21 | $58.12 | $59.72 | +33.60 | $59.50 | -4.62 | +28.98 | +20.79 | +16.17 |
| 2026-08-21 | `MRK` | 8 | $148.99 | $149.12 | +1.04 | $152.55 | +27.44 | +28.48 | -13.28 | +14.16 |
| 2026-08-21 | `ASST` | 78 | $16.13 | $17.66 | +119.34 | $18.22 | +43.68 | +163.02 | +129.48 | +173.16 |
| 2026-08-21 | `MRNA` | 8 | $133.32 | $133.11 | -1.68 | $145.13 | +96.16 | +94.48 | -136.24 | -40.08 |
| 2026-08-21 | `ZLAB` | 47 | $26.02 | $26.25 | +10.81 | $26.01 | -11.28 | -0.47 | -15.04 | -26.32 |
| 2026-08-21 | `ABTC` | 3 | — | $8.66 | +0.00 | $7.93 | -2.19 | -2.19 | +0.00 | -2.19 |
| 2026-08-24 | `BHP` | 13 | $97.03 | $97.31 | +3.64 | $97.13 | -2.34 | +1.30 | +81.90 | +79.56 |
| 2026-08-24 | `APA` | 27 | $43.39 | $42.93 | -12.42 | $42.96 | +0.81 | -11.61 | -49.41 | -48.60 |
| 2026-08-24 | `AUTL` | 505 | $2.41 | $2.40 | -5.05 | $2.34 | -30.30 | -35.35 | -35.35 | -65.65 |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.75 | -15.75 | $57.08 | -35.17 | -50.92 | +0.42 | -34.75 |
| 2026-08-24 | `MRK` | 8 | $152.55 | $150.72 | -14.64 | $150.66 | -0.48 | -15.12 | -0.48 | -0.96 |
| 2026-08-24 | `ASST` | 78 | $18.22 | $18.76 | +42.12 | $19.73 | +75.66 | +117.78 | +215.28 | +290.94 |
| 2026-08-24 | `MRNA` | 8 | $145.13 | $142.70 | -19.44 | $138.89 | -30.48 | -49.92 | -59.52 | -90.00 |
| 2026-08-24 | `ZLAB` | 47 | $26.01 | $25.43 | -27.26 | $25.64 | +9.87 | -17.39 | -53.58 | -43.71 |
| 2026-08-24 | `ABTC` | 3 | $7.93 | $8.00 | +0.21 | $8.64 | +1.92 | +2.13 | -1.98 | -0.06 |
| 2026-08-25 | `BHP` | 13 | $97.13 | $95.86 | -16.51 | — | +0.00 | -16.51 | +63.05 | — |
| 2026-08-25 | `APA` | 27 | $42.96 | $41.38 | -42.66 | — | +0.00 | -42.66 | -91.26 | — |
| 2026-08-25 | `AUTL` | 505 | $2.34 | $2.38 | +20.20 | — | +0.00 | +20.20 | -45.45 | — |
| 2026-08-25 | `CRSP` | 21 | $57.08 | $57.93 | +17.95 | — | +0.00 | +17.95 | -16.80 | — |
| 2026-08-25 | `MRK` | 8 | $150.66 | $151.00 | +2.72 | — | +0.00 | +2.72 | +1.76 | — |
| 2026-08-25 | `ASST` | 78 | $19.73 | $19.04 | -53.82 | — | +0.00 | -53.82 | +237.12 | — |
| 2026-08-25 | `MRNA` | 8 | $138.89 | $143.50 | +36.88 | — | +0.00 | +36.88 | -53.12 | — |
| 2026-08-25 | `ZLAB` | 47 | $25.64 | $26.04 | +18.80 | — | +0.00 | +18.80 | -24.91 | — |
| 2026-08-25 | `ABTC` | 3 | $8.64 | $8.62 | -0.06 | $9.24 | +1.86 | +1.80 | -0.12 | +1.74 |
| 2026-08-25 | `AU` | 21 | — | $118.52 | +0.00 | $123.39 | +102.27 | +102.27 | +0.00 | +102.27 |
| 2026-08-25 | `FCX` | 32 | — | $77.13 | +0.00 | $79.91 | +88.96 | +88.96 | +0.00 | +88.96 |
| 2026-08-25 | `EZPW` | 71 | — | $35.05 | +0.00 | $35.23 | +12.78 | +12.78 | +0.00 | +12.78 |
| 2026-08-25 | `AMX` | 104 | — | $23.80 | +0.00 | $23.75 | -5.20 | -5.20 | +0.00 | -5.20 |
| 2026-08-26 | `ABTC` | 3 | $9.24 | $8.84 | -1.20 | — | +0.00 | -1.20 | +0.54 | — |
| 2026-08-26 | `AU` | 21 | $123.39 | $119.80 | -75.39 | $118.11 | -35.49 | -110.88 | +26.88 | -8.61 |
| 2026-08-26 | `FCX` | 32 | $79.91 | $79.34 | -18.24 | $79.00 | -10.88 | -29.12 | +70.72 | +59.84 |
| 2026-08-26 | `EZPW` | 71 | $35.23 | $35.70 | +33.37 | $33.90 | -127.80 | -94.43 | +46.15 | -81.65 |
| 2026-08-26 | `AMX` | 104 | $23.75 | $23.75 | +0.00 | $23.62 | -13.52 | -13.52 | -5.20 | -18.72 |
| 2026-08-26 | `ASST` | 2 | — | $20.72 | +0.00 | $21.50 | +1.56 | +1.56 | +0.00 | +1.56 |
| 2026-08-27 | `AU` | 21 | $118.11 | $117.41 | -14.70 | $118.40 | +20.79 | +6.09 | -23.31 | -2.52 |
| 2026-08-27 | `FCX` | 32 | $79.00 | $78.83 | -5.44 | $78.42 | -13.12 | -18.56 | +54.40 | +41.28 |
| 2026-08-27 | `EZPW` | 71 | $33.90 | $33.50 | -28.40 | $34.41 | +64.61 | +36.21 | -110.05 | -45.44 |
| 2026-08-27 | `AMX` | 104 | $23.62 | $23.77 | +15.60 | $23.50 | -28.08 | -12.48 | -3.12 | -31.20 |
| 2026-08-27 | `ASST` | 2 | $21.50 | $22.45 | +1.90 | $23.12 | +1.34 | +3.24 | +3.46 | +4.80 |
| 2026-08-28 | `AU` | 21 | $118.40 | $119.19 | +16.59 | — | +0.00 | +16.59 | +14.07 | — |
| 2026-08-28 | `FCX` | 32 | $78.42 | $78.57 | +4.80 | — | +0.00 | +4.80 | +46.08 | — |
| 2026-08-28 | `EZPW` | 71 | $34.41 | $34.50 | +6.39 | — | +0.00 | +6.39 | -39.05 | — |
| 2026-08-28 | `AMX` | 104 | $23.50 | $23.64 | +14.56 | — | +0.00 | +14.56 | -16.64 | — |
| 2026-08-28 | `ASST` | 2 | $23.12 | $22.50 | -1.24 | $21.74 | -1.52 | -2.76 | +3.56 | +2.04 |
| 2026-08-28 | `KEYS` | 3 | — | $324.41 | +0.00 | $319.97 | -13.32 | -13.32 | +0.00 | -13.32 |
| 2026-08-28 | `SMTC` | 8 | — | $141.76 | +0.00 | $131.17 | -84.72 | -84.72 | +0.00 | -84.72 |
| 2026-08-28 | `CIEN` | 3 | — | $400.42 | +0.00 | $378.44 | -65.94 | -65.94 | +0.00 | -65.94 |
| 2026-08-28 | `DDOG` | 5 | — | $240.22 | +0.00 | $236.98 | -16.20 | -16.20 | +0.00 | -16.20 |
| 2026-08-28 | `PLAB` | 41 | — | $30.01 | +0.00 | $27.73 | -93.48 | -93.48 | +0.00 | -93.48 |
| 2026-08-28 | `ADSK` | 4 | — | $261.16 | +0.00 | $260.66 | -2.00 | -2.00 | +0.00 | -2.00 |
| 2026-08-28 | `SEDG` | 37 | — | $32.90 | +0.00 | $31.41 | -55.13 | -55.13 | +0.00 | -55.13 |
| 2026-08-31 | `ASST` | 2 | $21.74 | $22.54 | +1.60 | — | +0.00 | +1.60 | +3.64 | — |
| 2026-08-31 | `KEYS` | 3 | $319.97 | $322.49 | +7.56 | $322.70 | +0.63 | +8.19 | -5.76 | -5.13 |
| 2026-08-31 | `SMTC` | 8 | $131.17 | $132.30 | +9.04 | $132.96 | +5.28 | +14.32 | -75.68 | -70.40 |
| 2026-08-31 | `CIEN` | 3 | $378.44 | $378.44 | +0.00 | $382.80 | +13.08 | +13.08 | -65.94 | -52.86 |
| 2026-08-31 | `DDOG` | 5 | $236.98 | $233.97 | -15.07 | $237.04 | +15.37 | +0.30 | -31.27 | -15.90 |
| 2026-08-31 | `PLAB` | 41 | $27.73 | $28.04 | +12.71 | $28.14 | +4.10 | +16.81 | -80.77 | -76.67 |
| 2026-08-31 | `ADSK` | 4 | $260.66 | $257.71 | -11.80 | $258.53 | +3.28 | -8.52 | -13.80 | -10.52 |
| 2026-08-31 | `SEDG` | 37 | $31.41 | $31.15 | -9.62 | $32.20 | +38.85 | +29.23 | -64.75 | -25.90 |
| 2026-09-01 | `KEYS` | 3 | $322.70 | $321.47 | -3.69 | $319.27 | -6.60 | -10.29 | -8.82 | -15.42 |
| 2026-09-01 | `SMTC` | 8 | $132.96 | $127.63 | -42.64 | $132.27 | +37.12 | -5.52 | -113.04 | -75.92 |
| 2026-09-01 | `CIEN` | 3 | $382.80 | $376.89 | -17.73 | $360.33 | -49.68 | -67.41 | -70.59 | -120.27 |
| 2026-09-01 | `DDOG` | 5 | $237.04 | $232.88 | -20.80 | $223.84 | -45.20 | -66.00 | -36.70 | -81.90 |
| 2026-09-01 | `PLAB` | 41 | $28.14 | $27.69 | -18.45 | $27.33 | -14.76 | -33.21 | -95.12 | -109.88 |
| 2026-09-01 | `ADSK` | 4 | $258.53 | $253.48 | -20.20 | $247.69 | -23.16 | -43.36 | -30.72 | -53.88 |
| 2026-09-01 | `SEDG` | 37 | $32.20 | $31.87 | -12.21 | $32.49 | +22.94 | +10.73 | -38.11 | -15.17 |
| 2026-09-02 | `KEYS` | 3 | $319.27 | $318.04 | -3.69 | — | +0.00 | -3.69 | -19.11 | — |
| 2026-09-02 | `SMTC` | 8 | $132.27 | $133.00 | +5.84 | — | +0.00 | +5.84 | -70.08 | — |
| 2026-09-02 | `CIEN` | 3 | $360.33 | $357.25 | -9.24 | — | +0.00 | -9.24 | -129.51 | — |
| 2026-09-02 | `DDOG` | 5 | $223.84 | $219.46 | -21.90 | — | +0.00 | -21.90 | -103.80 | — |
| 2026-09-02 | `PLAB` | 41 | $27.33 | $27.41 | +3.28 | — | +0.00 | +3.28 | -106.60 | — |
| 2026-09-02 | `ADSK` | 4 | $247.69 | $246.70 | -3.96 | — | +0.00 | -3.96 | -57.84 | — |
| 2026-09-02 | `SEDG` | 37 | $32.49 | $32.42 | -2.59 | — | +0.00 | -2.59 | -17.76 | — |
| 2026-09-03 | `AVGO` | 3 | — | $351.74 | +0.00 | $357.16 | +16.26 | +16.26 | +0.00 | +16.26 |
| 2026-09-03 | `DELL` | 2 | — | $486.31 | +0.00 | $516.39 | +60.16 | +60.16 | +0.00 | +60.16 |
| 2026-09-03 | `CXW` | 41 | — | $32.31 | +0.00 | $33.66 | +55.35 | +55.35 | +0.00 | +55.35 |
| 2026-09-03 | `FRNM` | 85 | — | $15.87 | +0.00 | $16.90 | +87.55 | +87.55 | +0.00 | +87.55 |
| 2026-09-03 | `MMED` | 56 | — | $23.88 | +0.00 | $23.84 | -2.24 | -2.24 | +0.00 | -2.24 |
| 2026-09-03 | `DE` | 1 | — | $703.25 | +0.00 | $694.41 | -8.84 | -8.84 | +0.00 | -8.84 |
| 2026-09-03 | `HPE` | 28 | — | $47.60 | +0.00 | $54.44 | +191.52 | +191.52 | +0.00 | +191.52 |
| 2026-09-04 | `AVGO` | 3 | $357.16 | $359.70 | +7.62 | $357.90 | -5.40 | +2.22 | +23.88 | +18.48 |
| 2026-09-04 | `DELL` | 2 | $516.39 | $513.78 | -5.22 | $524.14 | +20.72 | +15.50 | +54.94 | +75.66 |
| 2026-09-04 | `CXW` | 41 | $33.66 | $33.46 | -8.20 | $34.71 | +51.25 | +43.05 | +47.15 | +98.40 |
| 2026-09-04 | `FRNM` | 85 | $16.90 | $16.40 | -42.50 | $16.31 | -7.65 | -50.15 | +45.05 | +37.40 |
| 2026-09-04 | `MMED` | 56 | $23.84 | $23.84 | +0.00 | $23.29 | -30.80 | -30.80 | -2.24 | -33.04 |
| 2026-09-04 | `DE` | 1 | $694.41 | $692.03 | -2.38 | $693.53 | +1.50 | -0.88 | -11.22 | -9.72 |
| 2026-09-04 | `HPE` | 28 | $54.44 | $53.85 | -16.52 | $52.00 | -51.80 | -68.32 | +175.00 | +123.20 |
| 2026-09-04 | `CRM` | 1 | — | $263.36 | +0.00 | $259.23 | -4.13 | -4.13 | +0.00 | -4.13 |
| 2026-09-04 | `MRX` | 3 | — | $75.65 | +0.00 | $78.27 | +7.86 | +7.86 | +0.00 | +7.86 |
| 2026-09-04 | `BE` | 1 | — | $236.82 | +0.00 | $252.87 | +16.05 | +16.05 | +0.00 | +16.05 |
| 2026-09-04 | `AMX` | 12 | — | $23.03 | +0.00 | $23.00 | -0.36 | -0.36 | +0.00 | -0.36 |
| 2026-09-04 | `BAK` | 143 | — | $1.94 | +0.00 | $1.89 | -7.15 | -7.15 | +0.00 | -7.15 |
| 2026-09-08 | `AVGO` | 3 | $357.90 | $363.68 | +17.34 | $368.56 | +14.64 | +31.98 | +35.82 | +50.46 |
| 2026-09-08 | `DELL` | 2 | $524.14 | $521.15 | -5.98 | $533.88 | +25.46 | +19.48 | +69.68 | +95.14 |
| 2026-09-08 | `CXW` | 41 | $34.71 | $34.49 | -9.02 | $35.05 | +22.96 | +13.94 | +89.38 | +112.34 |
| 2026-09-08 | `FRNM` | 85 | $16.31 | $16.74 | +36.55 | $15.99 | -63.75 | -27.20 | +73.95 | +10.20 |
| 2026-09-08 | `MMED` | 56 | $23.29 | $23.16 | -7.28 | $23.32 | +8.96 | +1.68 | -40.32 | -31.36 |
| 2026-09-08 | `DE` | 1 | $693.53 | $687.21 | -6.32 | $680.73 | -6.48 | -12.80 | -16.04 | -22.52 |
| 2026-09-08 | `HPE` | 28 | $52.00 | $52.29 | +8.12 | $56.03 | +104.72 | +112.84 | +131.32 | +236.04 |
| 2026-09-08 | `CRM` | 1 | $259.23 | $253.72 | -5.51 | $249.12 | -4.60 | -10.11 | -9.64 | -14.24 |
| 2026-09-08 | `MRX` | 3 | $78.27 | $78.84 | +1.71 | $76.71 | -6.39 | -4.68 | +9.57 | +3.18 |
| 2026-09-08 | `BE` | 1 | $252.87 | $267.76 | +14.89 | $277.22 | +9.46 | +24.35 | +30.94 | +40.40 |
| 2026-09-08 | `AMX` | 12 | $23.00 | $23.15 | +1.80 | $23.00 | -1.80 | +0.00 | +1.44 | -0.36 |
| 2026-09-08 | `BAK` | 143 | $1.89 | $1.94 | +7.15 | $1.92 | -2.86 | +4.29 | +0.00 | -2.86 |
| 2026-09-09 | `AVGO` | 3 | $368.56 | $366.23 | -6.99 | — | +0.00 | -6.99 | +43.47 | — |
| 2026-09-09 | `DELL` | 2 | $533.88 | $538.47 | +9.18 | — | +0.00 | +9.18 | +104.32 | — |
| 2026-09-09 | `CXW` | 41 | $35.05 | $35.09 | +1.64 | — | +0.00 | +1.64 | +113.98 | — |
| 2026-09-09 | `FRNM` | 85 | $15.99 | $15.96 | -2.55 | — | +0.00 | -2.55 | +7.65 | — |
| 2026-09-09 | `MMED` | 56 | $23.32 | $23.22 | -5.60 | — | +0.00 | -5.60 | -36.96 | — |
| 2026-09-09 | `DE` | 1 | $680.73 | $681.32 | +0.59 | — | +0.00 | +0.59 | -21.93 | — |
| 2026-09-09 | `HPE` | 28 | $56.03 | $56.94 | +25.48 | — | +0.00 | +25.48 | +261.52 | — |
| 2026-09-09 | `CRM` | 1 | $249.12 | $249.78 | +0.66 | $244.16 | -5.62 | -4.96 | -13.58 | -19.20 |
| 2026-09-09 | `MRX` | 3 | $76.71 | $76.60 | -0.33 | $75.72 | -2.64 | -2.97 | +2.85 | +0.21 |
| 2026-09-09 | `BE` | 1 | $277.22 | $272.99 | -4.23 | $269.28 | -3.71 | -7.94 | +36.17 | +32.46 |
| 2026-09-09 | `AMX` | 12 | $23.00 | $22.88 | -1.44 | $23.10 | +2.64 | +1.20 | -1.80 | +0.84 |
| 2026-09-09 | `BAK` | 143 | $1.92 | $2.02 | +13.59 | $1.97 | -6.44 | +7.15 | +10.73 | +4.29 |
| 2026-09-10 | `CRM` | 1 | $244.16 | $245.35 | +1.19 | — | +0.00 | +1.19 | -18.01 | — |
| 2026-09-10 | `MRX` | 3 | $75.72 | $75.00 | -2.16 | — | +0.00 | -2.16 | -1.95 | — |
| 2026-09-10 | `BE` | 1 | $269.28 | $260.71 | -8.57 | — | +0.00 | -8.57 | +23.89 | — |
| 2026-09-10 | `AMX` | 12 | $23.10 | $23.05 | -0.60 | — | +0.00 | -0.60 | +0.24 | — |
| 2026-09-10 | `BAK` | 143 | $1.97 | $1.97 | +0.00 | — | +0.00 | +0.00 | +4.29 | — |
| 2026-09-11 | `ORCL` | 30 | — | $164.43 | +0.00 | $150.28 | -424.50 | -424.50 | +0.00 | -424.50 |
| 2026-09-11 | `BTI` | 88 | — | $56.03 | +0.00 | $55.24 | -69.52 | -69.52 | +0.00 | -69.52 |
| 2026-09-14 | `ORCL` | 30 | $150.28 | $141.42 | -265.80 | $144.79 | +101.10 | -164.70 | -690.30 | -589.20 |
| 2026-09-14 | `BTI` | 88 | $55.24 | $57.12 | +165.44 | $57.29 | +14.96 | +180.40 | +95.92 | +110.88 |
| 2026-09-15 | `ORCL` | 30 | $144.79 | $143.46 | -39.90 | $140.35 | -93.30 | -133.20 | -629.10 | -722.40 |
| 2026-09-15 | `BTI` | 88 | $57.29 | $56.46 | -73.04 | $56.52 | +5.28 | -67.76 | +37.84 | +43.12 |
| 2026-09-16 | `ORCL` | 30 | $140.35 | $140.03 | -9.60 | — | +0.00 | -9.60 | -732.00 | — |
| 2026-09-16 | `BTI` | 88 | $56.52 | $56.54 | +1.76 | — | +0.00 | +1.76 | +44.88 | — |
| 2026-09-16 | `WAY` | 87 | — | $26.27 | +0.00 | $26.59 | +27.84 | +27.84 | +0.00 | +27.84 |
| 2026-09-16 | `QCOM` | 12 | — | $189.17 | +0.00 | $184.84 | -51.96 | -51.96 | +0.00 | -51.96 |
| 2026-09-16 | `SM` | 57 | — | $39.99 | +0.00 | $38.16 | -104.31 | -104.31 | +0.00 | -104.31 |
| 2026-09-16 | `AMX` | 99 | — | $23.18 | +0.00 | $22.98 | -19.80 | -19.80 | +0.00 | -19.80 |
| 2026-09-17 | `WAY` | 87 | $26.59 | $26.51 | -6.96 | $26.51 | +0.00 | -6.96 | +20.88 | +20.88 |
| 2026-09-17 | `QCOM` | 12 | $184.84 | $190.35 | +66.12 | $188.71 | -19.68 | +46.44 | +14.16 | -5.52 |
| 2026-09-17 | `SM` | 57 | $38.16 | $37.57 | -33.63 | $36.97 | -34.20 | -67.83 | -137.94 | -172.14 |
| 2026-09-17 | `AMX` | 99 | $22.98 | $23.09 | +10.89 | $23.03 | -5.94 | +4.95 | -8.91 | -14.85 |
| 2026-09-18 | `WAY` | 87 | $26.51 | $26.95 | +38.28 | $25.66 | -112.23 | -73.95 | +59.16 | -53.07 |
| 2026-09-18 | `QCOM` | 12 | $188.71 | $191.34 | +31.56 | $177.72 | -163.44 | -131.88 | +26.04 | -137.40 |
| 2026-09-18 | `SM` | 57 | $36.97 | $36.87 | -5.70 | $36.97 | +5.70 | +0.00 | -177.84 | -172.14 |
| 2026-09-18 | `AMX` | 99 | $23.03 | $22.90 | -12.87 | $22.43 | -46.53 | -59.40 | -27.72 | -74.25 |
| 2026-09-18 | `RARE` | 1 | — | $14.79 | +0.00 | $14.51 | -0.28 | -0.28 | +0.00 | -0.28 |
| 2026-09-18 | `BHVN` | 1 | — | $14.07 | +0.00 | $13.62 | -0.45 | -0.45 | +0.00 | -0.45 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +140.32 | HLIT, SNDK, ANGX, ARX, MH, VELO | — | $21.33 | $10,124.06 | HLIT×126, SNDK×1, ANGX×386, ARX×85, MH×123, VELO×108 |
| 2026-08-17 | +2.25 | $21.33 | HLIT×126, SNDK×1, ANGX×386, ARX×85, MH×123, VELO×108 | $10,257.05 | +132.99 | -22.75 | — | — | $21.33 | $10,234.29 | HLIT×126, SNDK×1, ANGX×386, ARX×85, MH×123, VELO×108 |
| 2026-08-18 | -6.20 | $21.33 | HLIT×126, SNDK×1, ANGX×386, ARX×85, MH×123, VELO×108 | $10,030.28 | -204.01 | -50.69 | — | — | $21.33 | $9,979.59 | HLIT×126, SNDK×1, ANGX×386, ARX×85, MH×123, VELO×108 |
| 2026-08-19 | -7.20 | $21.33 | HLIT×126, SNDK×1, ANGX×386, ARX×85, MH×123, VELO×108 | $10,009.69 | +30.10 | +0.00 | — | HLIT, SNDK, ANGX, ARX, MH, VELO | $9,993.20 | $9,993.20 | — |
| 2026-08-20 | +1.12 | $9,993.20 | — | $9,993.20 | +0.00 | -158.38 | BHP, APA, AUTL, CRSP, MRK, ASST, MRNA, ZLAB | — | $195.67 | $9,813.77 | BHP×13, APA×27, AUTL×505, CRSP×21, MRK×8, ASST×78, MRNA×8, ZLAB×47 |
| 2026-08-21 | +3.25 | $195.67 | BHP×13, APA×27, AUTL×505, CRSP×21, MRK×8, ASST×78, MRNA×8, ZLAB×47 | $10,012.61 | +198.84 | +105.41 | ABTC | — | $169.42 | $10,117.75 | BHP×13, APA×27, AUTL×505, CRSP×21, MRK×8, ASST×78, MRNA×8, ZLAB×47, ABTC×3 |
| 2026-08-24 | -5.17 | $169.42 | BHP×13, APA×27, AUTL×505, CRSP×21, MRK×8, ASST×78, MRNA×8, ZLAB×47, ABTC×3 | $10,069.16 | -48.59 | -10.51 | — | — | $169.42 | $10,058.65 | BHP×13, APA×27, AUTL×505, CRSP×21, MRK×8, ASST×78, MRNA×8, ZLAB×47, ABTC×3 |
| 2026-08-25 | +1.80 | $169.42 | BHP×13, APA×27, AUTL×505, CRSP×21, MRK×8, ASST×78, MRNA×8, ZLAB×47, ABTC×3 | $10,042.15 | -16.50 | +200.67 | AU, FCX, EZPW, AMX | BHP, APA, AUTL, CRSP, MRK, ASST, MRNA, ZLAB | $65.53 | $10,212.89 | ABTC×3, AU×21, FCX×32, EZPW×71, AMX×104 |
| 2026-08-26 | +2.02 | $65.53 | ABTC×3, AU×21, FCX×32, EZPW×71, AMX×104 | $10,151.43 | -61.46 | -186.13 | ASST | ABTC | $49.89 | $9,964.58 | AU×21, FCX×32, EZPW×71, AMX×104, ASST×2 |
| 2026-08-27 | — | $49.89 | AU×21, FCX×32, EZPW×71, AMX×104, ASST×2 | $9,933.54 | -31.04 | +45.54 | — | — | $49.89 | $9,979.08 | AU×21, FCX×32, EZPW×71, AMX×104, ASST×2 |
| 2026-08-28 | +0.75 | $49.89 | AU×21, FCX×32, EZPW×71, AMX×104, ASST×2 | $10,020.18 | +41.10 | -332.31 | KEYS, SMTC, CIEN, DDOG, PLAB, ADSK, SEDG | AU, FCX, EZPW, AMX | $1,950.16 | $9,664.87 | ASST×2, KEYS×3, SMTC×8, CIEN×3, DDOG×5, PLAB×41, ADSK×4, SEDG×37 |
| 2026-08-31 | -5.85 | $1,950.16 | ASST×2, KEYS×3, SMTC×8, CIEN×3, DDOG×5, PLAB×41, ADSK×4, SEDG×37 | $9,659.28 | -5.59 | +80.59 | — | ASST | $1,994.76 | $9,739.40 | KEYS×3, SMTC×8, CIEN×3, DDOG×5, PLAB×41, ADSK×4, SEDG×37 |
| 2026-09-01 | -6.30 | $1,994.76 | KEYS×3, SMTC×8, CIEN×3, DDOG×5, PLAB×41, ADSK×4, SEDG×37 | $9,603.68 | -135.72 | -79.34 | — | — | $1,994.76 | $9,524.34 | KEYS×3, SMTC×8, CIEN×3, DDOG×5, PLAB×41, ADSK×4, SEDG×37 |
| 2026-09-02 | -3.83 | $1,994.76 | KEYS×3, SMTC×8, CIEN×3, DDOG×5, PLAB×41, ADSK×4, SEDG×37 | $9,492.08 | -32.26 | +0.00 | — | KEYS, SMTC, CIEN, DDOG, PLAB, ADSK, SEDG | $9,477.71 | $9,477.71 | — |
| 2026-09-03 | -0.90 | $9,477.71 | — | $9,477.71 | -0.00 | +399.76 | AVGO, DELL, CXW, FRNM, MMED, DE, HPE | — | $1,388.30 | $9,862.89 | AVGO×3, DELL×2, CXW×41, FRNM×85, MMED×56, DE×1, HPE×28 |
| 2026-09-04 | +2.25 | $1,388.30 | AVGO×3, DELL×2, CXW×41, FRNM×85, MMED×56, DE×1, HPE×28 | $9,795.69 | -67.20 | -9.91 | CRM, MRX, BE, AMX, BAK | — | $96.96 | $9,775.35 | AVGO×3, DELL×2, CXW×41, FRNM×85, MMED×56, DE×1, HPE×28, CRM×1, MRX×3, BE×1, AMX×12, BAK×143 |
| 2026-09-08 | -11.47 | $96.96 | AVGO×3, DELL×2, CXW×41, FRNM×85, MMED×56, DE×1, HPE×28, CRM×1, MRX×3, BE×1, AMX×12, BAK×143 | $9,828.80 | +53.45 | +100.32 | — | — | $96.96 | $9,929.12 | AVGO×3, DELL×2, CXW×41, FRNM×85, MMED×56, DE×1, HPE×28, CRM×1, MRX×3, BE×1, AMX×12, BAK×143 |
| 2026-09-09 | -13.95 | $96.96 | AVGO×3, DELL×2, CXW×41, FRNM×85, MMED×56, DE×1, HPE×28, CRM×1, MRX×3, BE×1, AMX×12, BAK×143 | $9,959.12 | +30.00 | -15.77 | — | AVGO, DELL, CXW, FRNM, MMED, DE, HPE | $8,629.11 | $9,928.62 | CRM×1, MRX×3, BE×1, AMX×12, BAK×143 |
| 2026-09-10 | -13.28 | $8,629.11 | CRM×1, MRX×3, BE×1, AMX×12, BAK×143 | $9,918.48 | -10.14 | +0.00 | — | CRM, MRX, BE, AMX, BAK | $9,907.94 | $9,907.94 | — |
| 2026-09-11 | +0.50 | $9,907.94 | — | $9,907.94 | -0.00 | -494.02 | ORCL, BTI | — | $40.06 | $9,409.58 | ORCL×30, BTI×88 |
| 2026-09-14 | -11.00 | $40.06 | ORCL×30, BTI×88 | $9,309.22 | -100.36 | +116.06 | — | — | $40.06 | $9,425.28 | ORCL×30, BTI×88 |
| 2026-09-15 | -3.84 | $40.06 | ORCL×30, BTI×88 | $9,312.34 | -112.94 | -88.02 | — | — | $40.06 | $9,224.32 | ORCL×30, BTI×88 |
| 2026-09-16 | +5.30 | $40.06 | ORCL×30, BTI×88 | $9,216.48 | -7.84 | -148.23 | WAY, QCOM, SM, AMX | ORCL, BTI | $73.55 | $9,055.10 | WAY×87, QCOM×12, SM×57, AMX×99 |
| 2026-09-17 | +7.38 | $73.55 | WAY×87, QCOM×12, SM×57, AMX×99 | $9,091.52 | +36.42 | -59.82 | — | — | $73.55 | $9,031.70 | WAY×87, QCOM×12, SM×57, AMX×99 |
| 2026-09-18 | +4.86 | $73.55 | WAY×87, QCOM×12, SM×57, AMX×99 | $9,082.97 | +51.27 | -317.23 | RARE, BHVN | — | $44.39 | $8,765.44 | WAY×87, QCOM×12, SM×57, AMX×99, RARE×1, BHVN×1 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 126 | $13.18 | $2.37 | — | $8,336.95 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SNDK` | 1 | $1646.93 | $1.99 | — | $6,688.03 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.4; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 386 | $4.31 | $4.98 | — | $5,019.39 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 85 | $19.57 | $2.25 | — | $3,353.69 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+58.7; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
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
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,808.04 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1249.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 27 | $44.76 | $2.07 | — | $7,597.45 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1249.15 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 505 | $2.47 | $6.51 | — | $6,343.59 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1249.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $5,108.20 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1249.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRK` | 8 | $150.78 | $2.01 | — | $3,899.95 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list oppset; 🔵; ⚪; ret5=+14.5; leftover $1249.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 78 | $16.00 | $2.22 | — | $2,649.73 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1249.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $1,446.59 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover,oppset; 🔵; ret5=+173.9; leftover $1249.15 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 47 | $26.57 | $2.13 | — | $195.67 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1249.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $195.67 | ▼ close $9,813.77 vs 09:30 $9,993.20 (session -158.38) | 16:00 close · cash $195.67 · equity $9,813.77 vs 09:30 $9,993.20 (-179.43; session marks -158.38) · 8 name(s) marked open→close (per-name table). BHP×13 09:30 $91.01 → close $93.63 +34.06; APA×27 09:30 $44.76 → close $44.39 -9.99; AUTL×505 09:30 $2.47 → close $2.46 -5.05; CRSP×21 09:30 $58.73 → close $58.12 -12.81; MRK×8 09:30 $150.78 → close $148.99 -14.32; ASST×78 09:30 $16.00 → close $16.13 +10.14; MRNA×8 09:30 $150.14 → close $133.32 -134.56; ZLAB×47 09:30 $26.57 → close $26.02 -25.85 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $195.67 | ▲ 09:30 equity $10,012.61 vs yday $9,813.77 (+198.84) | 09:30 open · cash $195.67 (unchanged overnight, no fees) · equity $10,012.61 vs prior close $9,813.77 (+198.84) · 8 name(s) re-marked at the open (per-name table). BHP×13 yday $93.63 → 09:30 $95.72 +27.17; APA×27 yday $44.39 → 09:30 $44.52 +3.51; AUTL×505 yday $2.46 → 09:30 $2.47 +5.05; CRSP×21 yday $58.12 → 09:30 $59.72 +33.60; MRK×8 yday $148.99 → 09:30 $149.12 +1.04; ASST×78 yday $16.13 → 09:30 $17.66 +119.34; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; ZLAB×47 yday $26.02 → 09:30 $26.25 +10.81 | — |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 3 | $8.66 | $0.27 | — | $169.42 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $32.61 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $169.42 | ▲ close $10,117.75 vs 09:30 $10,012.61 (session +105.41) | 16:00 close · cash $169.42 · equity $10,117.75 vs 09:30 $10,012.61 (+105.14; session marks +105.41) · 9 name(s) marked open→close (per-name table). BHP×13 09:30 $95.72 → close $97.03 +17.03; APA×27 09:30 $44.52 → close $43.39 -30.51; AUTL×505 09:30 $2.47 → close $2.41 -30.30; CRSP×21 09:30 $59.72 → close $59.50 -4.62; MRK×8 09:30 $149.12 → close $152.55 +27.44; ASST×78 09:30 $17.66 → close $18.22 +43.68; MRNA×8 09:30 $133.11 → close $145.13 +96.16; ZLAB×47 09:30 $26.25 → close $26.01 -11.28; ABTC×3 09:30 $8.66 → close $7.93 -2.19 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $169.42 | ▼ 09:30 equity $10,069.16 vs yday $10,117.75 (-48.59) | 09:30 open · cash $169.42 (unchanged overnight, no fees) · equity $10,069.16 vs prior close $10,117.75 (-48.59) · 9 name(s) re-marked at the open (per-name table). BHP×13 yday $97.03 → 09:30 $97.31 +3.64; APA×27 yday $43.39 → 09:30 $42.93 -12.42; AUTL×505 yday $2.41 → 09:30 $2.40 -5.05; CRSP×21 yday $59.50 → 09:30 $58.75 -15.75; MRK×8 yday $152.55 → 09:30 $150.72 -14.64; ASST×78 yday $18.22 → 09:30 $18.76 +42.12; MRNA×8 yday $145.13 → 09:30 $142.70 -19.44; ZLAB×47 yday $26.01 → 09:30 $25.43 -27.26; ABTC×3 yday $7.93 → 09:30 $8.00 +0.21 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $169.42 | ▼ close $10,058.65 vs 09:30 $10,069.16 (session -10.51) | 16:00 close · cash $169.42 · equity $10,058.65 vs 09:30 $10,069.16 (-10.51; session marks -10.51) · 9 name(s) marked open→close (per-name table). BHP×13 09:30 $97.31 → close $97.13 -2.34; APA×27 09:30 $42.93 → close $42.96 +0.81; AUTL×505 09:30 $2.40 → close $2.34 -30.30; CRSP×21 09:30 $58.75 → close $57.08 -35.17; MRK×8 09:30 $150.72 → close $150.66 -0.48; ASST×78 09:30 $18.76 → close $19.73 +75.66; MRNA×8 09:30 $142.70 → close $138.89 -30.48; ZLAB×47 09:30 $25.43 → close $25.64 +9.87; ABTC×3 09:30 $8.00 → close $8.64 +1.92 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $169.42 | ▼ 09:30 equity $10,042.15 vs yday $10,058.65 (-16.50) | 09:30 open · cash $169.42 (unchanged overnight, no fees) · equity $10,042.15 vs prior close $10,058.65 (-16.50) · 9 name(s) re-marked at the open (per-name table). BHP×13 yday $97.13 → 09:30 $95.86 -16.51; APA×27 yday $42.96 → 09:30 $41.38 -42.66; AUTL×505 yday $2.34 → 09:30 $2.38 +20.20; CRSP×21 yday $57.08 → 09:30 $57.93 +17.95; MRK×8 yday $150.66 → 09:30 $151.00 +2.72; ASST×78 yday $19.73 → 09:30 $19.04 -53.82; MRNA×8 yday $138.89 → 09:30 $143.50 +36.88; ZLAB×47 yday $25.64 → 09:30 $26.04 +18.80; ABTC×3 yday $8.64 → 09:30 $8.62 -0.06 | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.86 | $2.05 | $+58.97 | $1,413.55 | ▲ +58.97 after sell → book $10,040.10; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 27 | $41.38 | $2.09 | $-95.42 | $2,528.72 | ▼ -95.42 after sell → book $10,038.01; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 505 | $2.38 | $6.61 | $-58.57 | $3,724.01 | ▼ -58.57 after sell → book $10,031.40; vs 09:30 mark -6.61 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.93 | $2.07 | $-20.93 | $4,938.47 | ▼ -20.93 after sell → book $10,029.33; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRK` | 8 | $151.00 | $2.03 | $-2.29 | $6,144.44 | ▼ -2.29 after sell → book $10,027.30; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ASST` | 78 | $19.04 | $2.25 | $+232.65 | $7,627.31 | ▲ +232.65 after sell → book $10,025.05; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $143.50 | $2.03 | $-57.17 | $8,773.27 | ▼ -57.17 after sell → book $10,023.01; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `ZLAB` | 47 | $26.04 | $2.15 | $-29.19 | $9,995.00 | ▼ -29.19 after sell → book $10,020.86; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 21 | $118.52 | $2.05 | — | $7,504.03 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $2498.75 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 32 | $77.13 | $2.09 | — | $5,033.78 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $2498.75 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 71 | $35.05 | $2.20 | — | $2,543.03 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,oppset; 🔵; ⚪; ret5=+19.7; leftover $2498.75 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AMX` | 104 | $23.80 | $2.30 | — | $65.53 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list oppset; 🔵; ret5=+0.5; leftover $2498.75 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $65.53 | ▲ close $10,212.89 vs 09:30 $10,042.15 (session +200.67) | 16:00 close · cash $65.53 · equity $10,212.89 vs 09:30 $10,042.15 (+170.74; session marks +200.67) · 5 name(s) marked open→close (per-name table). ABTC×3 09:30 $8.62 → close $9.24 +1.86; AU×21 09:30 $118.52 → close $123.39 +102.27; FCX×32 09:30 $77.13 → close $79.91 +88.96; EZPW×71 09:30 $35.05 → close $35.23 +12.78; AMX×104 09:30 $23.80 → close $23.75 -5.20 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $65.53 | ▼ 09:30 equity $10,151.43 vs yday $10,212.89 (-61.46) | 09:30 open · cash $65.53 (unchanged overnight, no fees) · equity $10,151.43 vs prior close $10,212.89 (-61.46) · 5 name(s) re-marked at the open (per-name table). ABTC×3 yday $9.24 → 09:30 $8.84 -1.20; AU×21 yday $123.39 → 09:30 $119.80 -75.39; FCX×32 yday $79.91 → 09:30 $79.34 -18.24; EZPW×71 yday $35.23 → 09:30 $35.70 +33.37; AMX×104 yday $23.75 → 09:30 $23.75 +0.00 | — |
| 2026-08-26 09:30 ET | **SELL** | `ABTC` | 3 | $8.84 | $0.29 | $-0.02 | $91.75 | ▼ -0.02 after sell → book $10,151.13; vs 09:30 mark -0.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `ASST` | 2 | $20.72 | $0.42 | — | $49.89 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list oppset; 🔵; ret5=+67.1; leftover $45.88 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.89 | ▼ close $9,964.58 vs 09:30 $10,151.43 (session -186.13) | 16:00 close · cash $49.89 · equity $9,964.58 vs 09:30 $10,151.43 (-186.85; session marks -186.13) · 5 name(s) marked open→close (per-name table). AU×21 09:30 $119.80 → close $118.11 -35.49; FCX×32 09:30 $79.34 → close $79.00 -10.88; EZPW×71 09:30 $35.70 → close $33.90 -127.80; AMX×104 09:30 $23.75 → close $23.62 -13.52; ASST×2 09:30 $20.72 → close $21.50 +1.56 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.89 | ▼ 09:30 equity $9,933.54 vs yday $9,964.58 (-31.04) | 09:30 open · cash $49.89 (unchanged overnight, no fees) · equity $9,933.54 vs prior close $9,964.58 (-31.04) · 5 name(s) re-marked at the open (per-name table). AU×21 yday $118.11 → 09:30 $117.41 -14.70; FCX×32 yday $79.00 → 09:30 $78.83 -5.44; EZPW×71 yday $33.90 → 09:30 $33.50 -28.40; AMX×104 yday $23.62 → 09:30 $23.77 +15.60; ASST×2 yday $21.50 → 09:30 $22.45 +1.90 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.89 | ▲ close $9,979.08 vs 09:30 $9,933.54 (session +45.54) | 16:00 close · cash $49.89 · equity $9,979.08 vs 09:30 $9,933.54 (+45.54; session marks +45.54) · 5 name(s) marked open→close (per-name table). AU×21 09:30 $117.41 → close $118.40 +20.79; FCX×32 09:30 $78.83 → close $78.42 -13.12; EZPW×71 09:30 $33.50 → close $34.41 +64.61; AMX×104 09:30 $23.77 → close $23.50 -28.08; ASST×2 09:30 $22.45 → close $23.12 +1.34 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.89 | ▲ 09:30 equity $10,020.18 vs yday $9,979.08 (+41.10) | 09:30 open · cash $49.89 (unchanged overnight, no fees) · equity $10,020.18 vs prior close $9,979.08 (+41.10) · 5 name(s) re-marked at the open (per-name table). AU×21 yday $118.40 → 09:30 $119.19 +16.59; FCX×32 yday $78.42 → 09:30 $78.57 +4.80; EZPW×71 yday $34.41 → 09:30 $34.50 +6.39; AMX×104 yday $23.50 → 09:30 $23.64 +14.56; ASST×2 yday $23.12 → 09:30 $22.50 -1.24 | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 21 | $119.19 | $2.08 | $+9.93 | $2,550.80 | ▲ +9.93 after sell → book $10,018.10; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 32 | $78.57 | $2.12 | $+41.88 | $5,062.92 | ▲ +41.88 after sell → book $10,015.98; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 71 | $34.50 | $2.23 | $-43.49 | $7,510.19 | ▼ -43.49 after sell → book $10,013.75; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `AMX` | 104 | $23.64 | $2.34 | $-21.28 | $9,966.41 | ▼ -21.28 after sell → book $10,011.41; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 3 | $324.41 | $2.00 | — | $8,991.18 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1245.80 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $141.76 | $2.01 | — | $7,855.09 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover,mover_buy,oppset; 🔵; ⚪; ret5=+14.1; leftover $1245.80 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,651.83 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1245.80 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $5,448.72 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1245.80 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PLAB` | 41 | $30.01 | $2.11 | — | $4,216.20 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list oppset; 🔵; ret5=-0.9; leftover $1245.80 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $3,169.56 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; ret5=+7.8; leftover $1245.80 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 37 | $32.90 | $2.10 | — | $1,950.16 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1245.80 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,950.16 | ▼ close $9,664.87 vs 09:30 $10,020.18 (session -332.31) | 16:00 close · cash $1,950.16 · equity $9,664.87 vs 09:30 $10,020.18 (-355.31; session marks -332.31) · 8 name(s) marked open→close (per-name table). ASST×2 09:30 $22.50 → close $21.74 -1.52; KEYS×3 09:30 $324.41 → close $319.97 -13.32; SMTC×8 09:30 $141.76 → close $131.17 -84.72; CIEN×3 09:30 $400.42 → close $378.44 -65.94; DDOG×5 09:30 $240.22 → close $236.98 -16.20; PLAB×41 09:30 $30.01 → close $27.73 -93.48; ADSK×4 09:30 $261.16 → close $260.66 -2.00; SEDG×37 09:30 $32.90 → close $31.41 -55.13 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,950.16 | ▼ 09:30 equity $9,659.28 vs yday $9,664.87 (-5.59) | 09:30 open · cash $1,950.16 (unchanged overnight, no fees) · equity $9,659.28 vs prior close $9,664.87 (-5.59) · 8 name(s) re-marked at the open (per-name table). ASST×2 yday $21.74 → 09:30 $22.54 +1.60; KEYS×3 yday $319.97 → 09:30 $322.49 +7.56; SMTC×8 yday $131.17 → 09:30 $132.30 +9.04; CIEN×3 yday $378.44 → 09:30 $378.44 +0.00; DDOG×5 yday $236.98 → 09:30 $233.97 -15.07; PLAB×41 yday $27.73 → 09:30 $28.04 +12.71; ADSK×4 yday $260.66 → 09:30 $257.71 -11.80; SEDG×37 yday $31.41 → 09:30 $31.15 -9.62 | — |
| 2026-08-31 09:30 ET | **SELL** | `ASST` | 2 | $22.54 | $0.48 | $+2.74 | $1,994.76 | ▲ +2.74 after sell → book $9,658.81; vs 09:30 mark -0.47 | dropped from list after 3 sess (min 3) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,994.76 | ▲ close $9,739.40 vs 09:30 $9,659.28 (session +80.59) | 16:00 close · cash $1,994.76 · equity $9,739.40 vs 09:30 $9,659.28 (+80.12; session marks +80.59) · 7 name(s) marked open→close (per-name table). KEYS×3 09:30 $322.49 → close $322.70 +0.63; SMTC×8 09:30 $132.30 → close $132.96 +5.28; CIEN×3 09:30 $378.44 → close $382.80 +13.08; DDOG×5 09:30 $233.97 → close $237.04 +15.37; PLAB×41 09:30 $28.04 → close $28.14 +4.10; ADSK×4 09:30 $257.71 → close $258.53 +3.28; SEDG×37 09:30 $31.15 → close $32.20 +38.85 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,994.76 | ▼ 09:30 equity $9,603.68 vs yday $9,739.40 (-135.72) | 09:30 open · cash $1,994.76 (unchanged overnight, no fees) · equity $9,603.68 vs prior close $9,739.40 (-135.72) · 7 name(s) re-marked at the open (per-name table). KEYS×3 yday $322.70 → 09:30 $321.47 -3.69; SMTC×8 yday $132.96 → 09:30 $127.63 -42.64; CIEN×3 yday $382.80 → 09:30 $376.89 -17.73; DDOG×5 yday $237.04 → 09:30 $232.88 -20.80; PLAB×41 yday $28.14 → 09:30 $27.69 -18.45; ADSK×4 yday $258.53 → 09:30 $253.48 -20.20; SEDG×37 yday $32.20 → 09:30 $31.87 -12.21 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,994.76 | ▼ close $9,524.34 vs 09:30 $9,603.68 (session -79.34) | 16:00 close · cash $1,994.76 · equity $9,524.34 vs 09:30 $9,603.68 (-79.34; session marks -79.34) · 7 name(s) marked open→close (per-name table). KEYS×3 09:30 $321.47 → close $319.27 -6.60; SMTC×8 09:30 $127.63 → close $132.27 +37.12; CIEN×3 09:30 $376.89 → close $360.33 -49.68; DDOG×5 09:30 $232.88 → close $223.84 -45.20; PLAB×41 09:30 $27.69 → close $27.33 -14.76; ADSK×4 09:30 $253.48 → close $247.69 -23.16; SEDG×37 09:30 $31.87 → close $32.49 +22.94 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,994.76 | ▼ 09:30 equity $9,492.08 vs yday $9,524.34 (-32.26) | 09:30 open · cash $1,994.76 (unchanged overnight, no fees) · equity $9,492.08 vs prior close $9,524.34 (-32.26) · 7 name(s) re-marked at the open (per-name table). KEYS×3 yday $319.27 → 09:30 $318.04 -3.69; SMTC×8 yday $132.27 → 09:30 $133.00 +5.84; CIEN×3 yday $360.33 → 09:30 $357.25 -9.24; DDOG×5 yday $223.84 → 09:30 $219.46 -21.90; PLAB×41 yday $27.33 → 09:30 $27.41 +3.28; ADSK×4 yday $247.69 → 09:30 $246.70 -3.96; SEDG×37 yday $32.49 → 09:30 $32.42 -2.59 | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 3 | $318.04 | $2.02 | $-23.13 | $2,946.86 | ▼ -23.13 after sell → book $9,490.06; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $133.00 | $2.03 | $-74.13 | $4,008.83 | ▼ -74.13 after sell → book $9,488.03; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 3 | $357.25 | $2.02 | $-133.53 | $5,078.56 | ▼ -133.53 after sell → book $9,486.01; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `DDOG` | 5 | $219.46 | $2.02 | $-107.83 | $6,173.83 | ▼ -107.83 after sell → book $9,483.98; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `PLAB` | 41 | $27.41 | $2.13 | $-110.85 | $7,295.51 | ▼ -110.85 after sell → book $9,481.85; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 4 | $246.70 | $2.02 | $-61.86 | $8,280.29 | ▼ -61.86 after sell → book $9,479.83; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 37 | $32.42 | $2.12 | $-21.98 | $9,477.71 | ▼ -21.98 after sell → book $9,477.71; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,477.71 | ▲ close $9,477.71 vs 09:30 $9,492.08 (session +0.00) | 16:00 close · cash $9,477.71 · no lots left · equity $9,477.71. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,477.71 | ▲ 09:30 equity $9,477.71 vs yday $9,477.71 (-0.00) | 09:30 open · cash $9,477.71 · no holdings · equity $9,477.71 vs prior close $9,477.71 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $8,420.49 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1353.96 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $7,445.87 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy,oppset; 🔵; ret5=+6.1; leftover $1353.96 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 41 | $32.31 | $2.11 | — | $6,119.05 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1353.96 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 85 | $15.87 | $2.25 | — | $4,767.86 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1353.96 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 56 | $23.88 | $2.16 | — | $3,428.42 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+21.9; leftover $1353.96 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $2,723.17 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1353.96 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 28 | $47.60 | $2.07 | — | $1,388.30 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react,oppset; 🔵; ret5=-6.2; leftover $1353.96 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,388.30 | ▲ close $9,862.89 vs 09:30 $9,477.71 (session +399.76) | 16:00 close · cash $1,388.30 · equity $9,862.89 vs 09:30 $9,477.71 (+385.18; session marks +399.76) · 7 name(s) marked open→close (per-name table). AVGO×3 09:30 $351.74 → close $357.16 +16.26; DELL×2 09:30 $486.31 → close $516.39 +60.16; CXW×41 09:30 $32.31 → close $33.66 +55.35; FRNM×85 09:30 $15.87 → close $16.90 +87.55; MMED×56 09:30 $23.88 → close $23.84 -2.24; DE×1 09:30 $703.25 → close $694.41 -8.84; HPE×28 09:30 $47.60 → close $54.44 +191.52 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,388.30 | ▼ 09:30 equity $9,795.69 vs yday $9,862.89 (-67.20) | 09:30 open · cash $1,388.30 (unchanged overnight, no fees) · equity $9,795.69 vs prior close $9,862.89 (-67.20) · 7 name(s) re-marked at the open (per-name table). AVGO×3 yday $357.16 → 09:30 $359.70 +7.62; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; CXW×41 yday $33.66 → 09:30 $33.46 -8.20; FRNM×85 yday $16.90 → 09:30 $16.40 -42.50; MMED×56 yday $23.84 → 09:30 $23.84 +0.00; DE×1 yday $694.41 → 09:30 $692.03 -2.38; HPE×28 yday $54.44 → 09:30 $53.85 -16.52 | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 1 | $263.36 | $1.99 | — | $1,122.95 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $277.66 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 3 | $75.65 | $2.00 | — | $894.00 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $277.66 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 1 | $236.82 | $1.99 | — | $655.19 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+8.1; leftover $277.66 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `AMX` | 12 | $23.03 | $2.03 | — | $376.80 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list oppset; 🔵; ret5=-1.4; leftover $277.66 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 143 | $1.94 | $2.42 | — | $96.96 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $277.66 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $96.96 | ▼ close $9,775.35 vs 09:30 $9,795.69 (session -9.91) | 16:00 close · cash $96.96 · equity $9,775.35 vs 09:30 $9,795.69 (-20.34; session marks -9.91) · 12 name(s) marked open→close (per-name table). AVGO×3 09:30 $359.70 → close $357.90 -5.40; DELL×2 09:30 $513.78 → close $524.14 +20.72; CXW×41 09:30 $33.46 → close $34.71 +51.25; FRNM×85 09:30 $16.40 → close $16.31 -7.65; MMED×56 09:30 $23.84 → close $23.29 -30.80; DE×1 09:30 $692.03 → close $693.53 +1.50; HPE×28 09:30 $53.85 → close $52.00 -51.80; CRM×1 09:30 $263.36 → close $259.23 -4.13; MRX×3 09:30 $75.65 → close $78.27 +7.86; BE×1 09:30 $236.82 → close $252.87 +16.05; AMX×12 09:30 $23.03 → close $23.00 -0.36; BAK×143 09:30 $1.94 → close $1.89 -7.15 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $96.96 | ▲ 09:30 equity $9,828.80 vs yday $9,775.35 (+53.45) | 09:30 open · cash $96.96 (unchanged overnight, no fees) · equity $9,828.80 vs prior close $9,775.35 (+53.45) · 12 name(s) re-marked at the open (per-name table). AVGO×3 yday $357.90 → 09:30 $363.68 +17.34; DELL×2 yday $524.14 → 09:30 $521.15 -5.98; CXW×41 yday $34.71 → 09:30 $34.49 -9.02; FRNM×85 yday $16.31 → 09:30 $16.74 +36.55; MMED×56 yday $23.29 → 09:30 $23.16 -7.28; DE×1 yday $693.53 → 09:30 $687.21 -6.32; HPE×28 yday $52.00 → 09:30 $52.29 +8.12; CRM×1 yday $259.23 → 09:30 $253.72 -5.51; MRX×3 yday $78.27 → 09:30 $78.84 +1.71; BE×1 yday $252.87 → 09:30 $267.76 +14.89; AMX×12 yday $23.00 → 09:30 $23.15 +1.80; BAK×143 yday $1.89 → 09:30 $1.94 +7.15 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $96.96 | ▲ close $9,929.12 vs 09:30 $9,828.80 (session +100.32) | 16:00 close · cash $96.96 · equity $9,929.12 vs 09:30 $9,828.80 (+100.32; session marks +100.32) · 12 name(s) marked open→close (per-name table). AVGO×3 09:30 $363.68 → close $368.56 +14.64; DELL×2 09:30 $521.15 → close $533.88 +25.46; CXW×41 09:30 $34.49 → close $35.05 +22.96; FRNM×85 09:30 $16.74 → close $15.99 -63.75; MMED×56 09:30 $23.16 → close $23.32 +8.96; DE×1 09:30 $687.21 → close $680.73 -6.48; HPE×28 09:30 $52.29 → close $56.03 +104.72; CRM×1 09:30 $253.72 → close $249.12 -4.60; MRX×3 09:30 $78.84 → close $76.71 -6.39; BE×1 09:30 $267.76 → close $277.22 +9.46; AMX×12 09:30 $23.15 → close $23.00 -1.80; BAK×143 09:30 $1.94 → close $1.92 -2.86 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $96.96 | ▲ 09:30 equity $9,959.12 vs yday $9,929.12 (+30.00) | 09:30 open · cash $96.96 (unchanged overnight, no fees) · equity $9,959.12 vs prior close $9,929.12 (+30.00) · 12 name(s) re-marked at the open (per-name table). AVGO×3 yday $368.56 → 09:30 $366.23 -6.99; DELL×2 yday $533.88 → 09:30 $538.47 +9.18; CXW×41 yday $35.05 → 09:30 $35.09 +1.64; FRNM×85 yday $15.99 → 09:30 $15.96 -2.55; MMED×56 yday $23.32 → 09:30 $23.22 -5.60; DE×1 yday $680.73 → 09:30 $681.32 +0.59; HPE×28 yday $56.03 → 09:30 $56.94 +25.48; CRM×1 yday $249.12 → 09:30 $249.78 +0.66; MRX×3 yday $76.71 → 09:30 $76.60 -0.33; BE×1 yday $277.22 → 09:30 $272.99 -4.23; AMX×12 yday $23.00 → 09:30 $22.88 -1.44; BAK×143 yday $1.92 → 09:30 $2.02 +13.59 | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 3 | $366.23 | $2.02 | $+39.45 | $1,193.63 | ▲ +39.45 after sell → book $9,957.10; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DELL` | 2 | $538.47 | $2.02 | $+100.31 | $2,268.56 | ▲ +100.31 after sell → book $9,955.08; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CXW` | 41 | $35.09 | $2.13 | $+109.73 | $3,705.11 | ▲ +109.73 after sell → book $9,952.95; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FRNM` | 85 | $15.96 | $2.27 | $+3.14 | $5,059.44 | ▲ +3.14 after sell → book $9,950.68; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 56 | $23.22 | $2.18 | $-41.30 | $6,357.58 | ▼ -41.30 after sell → book $9,948.50; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DE` | 1 | $681.32 | $2.01 | $-25.94 | $7,036.89 | ▼ -25.94 after sell → book $9,946.48; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 28 | $56.94 | $2.10 | $+257.35 | $8,629.11 | ▲ +257.35 after sell → book $9,944.39; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,629.11 | ▼ close $9,928.62 vs 09:30 $9,959.12 (session -15.77) | 16:00 close · cash $8,629.11 · equity $9,928.62 vs 09:30 $9,959.12 (-30.50; session marks -15.77) · 5 name(s) marked open→close (per-name table). CRM×1 09:30 $249.78 → close $244.16 -5.62; MRX×3 09:30 $76.60 → close $75.72 -2.64; BE×1 09:30 $272.99 → close $269.28 -3.71; AMX×12 09:30 $22.88 → close $23.10 +2.64; BAK×143 09:30 $2.02 → close $1.97 -6.44 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,629.11 | ▼ 09:30 equity $9,918.48 vs yday $9,928.62 (-10.14) | 09:30 open · cash $8,629.11 (unchanged overnight, no fees) · equity $9,918.48 vs prior close $9,928.62 (-10.14) · 5 name(s) re-marked at the open (per-name table). CRM×1 yday $244.16 → 09:30 $245.35 +1.19; MRX×3 yday $75.72 → 09:30 $75.00 -2.16; BE×1 yday $269.28 → 09:30 $260.71 -8.57; AMX×12 yday $23.10 → 09:30 $23.05 -0.60; BAK×143 yday $1.97 → 09:30 $1.97 +0.00 | — |
| 2026-09-10 09:30 ET | **SELL** | `CRM` | 1 | $245.35 | $2.01 | $-22.02 | $8,872.45 | ▼ -22.02 after sell → book $9,916.47; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MRX` | 3 | $75.00 | $2.02 | $-5.97 | $9,095.43 | ▼ -5.97 after sell → book $9,914.45; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BE` | 1 | $260.71 | $2.01 | $+19.88 | $9,354.13 | ▲ +19.88 after sell → book $9,912.44; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `AMX` | 12 | $23.05 | $2.05 | $-3.83 | $9,628.68 | ▼ -3.83 after sell → book $9,910.39; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BAK` | 143 | $1.97 | $2.45 | $-0.58 | $9,907.94 | ▼ -0.58 after sell → book $9,907.94; vs 09:30 mark -2.45 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,907.94 | ▲ close $9,907.94 vs 09:30 $9,918.48 (session +0.00) | 16:00 close · cash $9,907.94 · no lots left · equity $9,907.94. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,907.94 | ▲ 09:30 equity $9,907.94 vs yday $9,907.94 (-0.00) | 09:30 open · cash $9,907.94 · no holdings · equity $9,907.94 vs prior close $9,907.94 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 30 | $164.43 | $2.08 | — | $4,972.96 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $4953.97 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BTI` | 88 | $56.03 | $2.25 | — | $40.06 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list oppset; 🔵; ret5=-0.8; leftover $4953.97 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.06 | ▼ close $9,409.58 vs 09:30 $9,907.94 (session -494.02) | 16:00 close · cash $40.06 · equity $9,409.58 vs 09:30 $9,907.94 (-498.36; session marks -494.02) · 2 name(s) marked open→close (per-name table). ORCL×30 09:30 $164.43 → close $150.28 -424.50; BTI×88 09:30 $56.03 → close $55.24 -69.52 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.06 | ▼ 09:30 equity $9,309.22 vs yday $9,409.58 (-100.36) | 09:30 open · cash $40.06 (unchanged overnight, no fees) · equity $9,309.22 vs prior close $9,409.58 (-100.36) · 2 name(s) re-marked at the open (per-name table). ORCL×30 yday $150.28 → 09:30 $141.42 -265.80; BTI×88 yday $55.24 → 09:30 $57.12 +165.44 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.06 | ▲ close $9,425.28 vs 09:30 $9,309.22 (session +116.06) | 16:00 close · cash $40.06 · equity $9,425.28 vs 09:30 $9,309.22 (+116.06; session marks +116.06) · 2 name(s) marked open→close (per-name table). ORCL×30 09:30 $141.42 → close $144.79 +101.10; BTI×88 09:30 $57.12 → close $57.29 +14.96 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.06 | ▼ 09:30 equity $9,312.34 vs yday $9,425.28 (-112.94) | 09:30 open · cash $40.06 (unchanged overnight, no fees) · equity $9,312.34 vs prior close $9,425.28 (-112.94) · 2 name(s) re-marked at the open (per-name table). ORCL×30 yday $144.79 → 09:30 $143.46 -39.90; BTI×88 yday $57.29 → 09:30 $56.46 -73.04 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.06 | ▼ close $9,224.32 vs 09:30 $9,312.34 (session -88.02) | 16:00 close · cash $40.06 · equity $9,224.32 vs 09:30 $9,312.34 (-88.02; session marks -88.02) · 2 name(s) marked open→close (per-name table). ORCL×30 09:30 $143.46 → close $140.35 -93.30; BTI×88 09:30 $56.46 → close $56.52 +5.28 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.06 | ▼ 09:30 equity $9,216.48 vs yday $9,224.32 (-7.84) | 09:30 open · cash $40.06 (unchanged overnight, no fees) · equity $9,216.48 vs prior close $9,224.32 (-7.84) · 2 name(s) re-marked at the open (per-name table). ORCL×30 yday $140.35 → 09:30 $140.03 -9.60; BTI×88 yday $56.52 → 09:30 $56.54 +1.76 | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 30 | $140.03 | $2.12 | $-736.20 | $4,238.84 | ▼ -736.20 after sell → book $9,214.36; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BTI` | 88 | $56.54 | $2.31 | $+40.32 | $9,212.05 | ▲ +40.32 after sell → book $9,212.05; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 87 | $26.27 | $2.25 | — | $6,924.31 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2303.01 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 12 | $189.17 | $2.03 | — | $4,652.24 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+2.1; leftover $2303.01 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 57 | $39.99 | $2.16 | — | $2,370.65 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2303.01 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `AMX` | 99 | $23.18 | $2.29 | — | $73.55 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list oppset; 🔵; ret5=-0.2; leftover $2303.01 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $73.55 | ▼ close $9,055.10 vs 09:30 $9,216.48 (session -148.23) | 16:00 close · cash $73.55 · equity $9,055.10 vs 09:30 $9,216.48 (-161.38; session marks -148.23) · 4 name(s) marked open→close (per-name table). WAY×87 09:30 $26.27 → close $26.59 +27.84; QCOM×12 09:30 $189.17 → close $184.84 -51.96; SM×57 09:30 $39.99 → close $38.16 -104.31; AMX×99 09:30 $23.18 → close $22.98 -19.80 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $73.55 | ▲ 09:30 equity $9,091.52 vs yday $9,055.10 (+36.42) | 09:30 open · cash $73.55 (unchanged overnight, no fees) · equity $9,091.52 vs prior close $9,055.10 (+36.42) · 4 name(s) re-marked at the open (per-name table). WAY×87 yday $26.59 → 09:30 $26.51 -6.96; QCOM×12 yday $184.84 → 09:30 $190.35 +66.12; SM×57 yday $38.16 → 09:30 $37.57 -33.63; AMX×99 yday $22.98 → 09:30 $23.09 +10.89 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $73.55 | ▼ close $9,031.70 vs 09:30 $9,091.52 (session -59.82) | 16:00 close · cash $73.55 · equity $9,031.70 vs 09:30 $9,091.52 (-59.82; session marks -59.82) · 4 name(s) marked open→close (per-name table). WAY×87 09:30 $26.51 → close $26.51 +0.00; QCOM×12 09:30 $190.35 → close $188.71 -19.68; SM×57 09:30 $37.57 → close $36.97 -34.20; AMX×99 09:30 $23.09 → close $23.03 -5.94 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $73.55 | ▲ 09:30 equity $9,082.97 vs yday $9,031.70 (+51.27) | 09:30 open · cash $73.55 (unchanged overnight, no fees) · equity $9,082.97 vs prior close $9,031.70 (+51.27) · 4 name(s) re-marked at the open (per-name table). WAY×87 yday $26.51 → 09:30 $26.95 +38.28; QCOM×12 yday $188.71 → 09:30 $191.34 +31.56; SM×57 yday $36.97 → 09:30 $36.87 -5.70; AMX×99 yday $23.03 → 09:30 $22.90 -12.87 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 1 | $14.79 | $0.15 | — | $58.61 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $18.39 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 1 | $14.07 | $0.14 | — | $44.39 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $18.39 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $44.39 | ▼ close $8,765.44 vs 09:30 $9,082.97 (session -317.23) | 16:00 close · cash $44.39 · equity $8,765.44 vs 09:30 $9,082.97 (-317.53; session marks -317.23) · 6 name(s) marked open→close (per-name table). WAY×87 09:30 $26.95 → close $25.66 -112.23; QCOM×12 09:30 $191.34 → close $177.72 -163.44; SM×57 09:30 $36.87 → close $36.97 +5.70; AMX×99 09:30 $22.90 → close $22.43 -46.53; RARE×1 09:30 $14.79 → close $14.51 -0.28; BHVN×1 09:30 $14.07 → close $13.62 -0.45 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `HLIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `SNDK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `VELO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 4.27 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 4.27 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 4.27 < 1 share @ 202.70 |
| 2026-08-17 | `GLOB` | cash | leftover split 4.27 < 1 share @ 37.18 |
| 2026-08-17 | `TPG` | cash | leftover split 4.27 < 1 share @ 52.67 |
| 2026-08-18 | `HLIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `SNDK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VELO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `APA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ZLAB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 32.61 < 1 share @ 119.43 |
| 2026-08-21 | `FUTU` | cash | leftover split 32.61 < 1 share @ 115.18 |
| 2026-08-21 | `GRAL` | cash | leftover split 32.61 < 1 share @ 78.88 |
| 2026-08-21 | `VIRT` | cash | leftover split 32.61 < 1 share @ 60.66 |
| 2026-08-21 | `MFC` | cash | leftover split 32.61 < 1 share @ 42.48 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `APA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ZLAB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ABTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FNV` | cash | leftover split 45.88 < 1 share @ 267.02 |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AMX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 6.24 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 6.24 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 6.24 < 1 share @ 1746.53 |
| 2026-08-27 | `CM` | cash | leftover split 6.24 < 1 share @ 118.77 |
| 2026-08-27 | `GEN` | cash | leftover split 6.24 < 1 share @ 29.83 |
| 2026-08-27 | `LRCX` | cash | leftover split 6.24 < 1 share @ 318.88 |
| 2026-08-27 | `NVDA` | cash | leftover split 6.24 < 1 share @ 222.86 |
| 2026-08-27 | `AXTI` | cash | leftover split 6.24 < 1 share @ 70.30 |
| 2026-08-28 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `MPWR` | cash | leftover split 1245.80 < 1 share @ 1306.03 |
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
| 2026-08-31 | `ESI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DDOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `PLAB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ADSK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CXW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DE` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| 2026-09-08 | `AMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `CRM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `MRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `AMX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BTI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BTI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CVE` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `WAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `AMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SMTC` | cash | leftover split 14.71 < 1 share @ 170.85 |
| 2026-09-17 | `AVTR` | cash | leftover split 14.71 < 1 share @ 15.81 |
| 2026-09-17 | `GME` | cash | leftover split 14.71 < 1 share @ 22.12 |
| 2026-09-17 | `JBHT` | cash | leftover split 14.71 < 1 share @ 238.60 |
| 2026-09-17 | `SRRK` | cash | leftover split 14.71 < 1 share @ 49.52 |
| 2026-09-18 | `WAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `AMX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TH` | cash | leftover split 18.39 < 1 share @ 20.91 |
| 2026-09-18 | `GME` | cash | leftover split 18.39 < 1 share @ 22.90 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `WAY` | 87 | 2026-09-16 @ $26.27 | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2303.01 |
| `QCOM` | 12 | 2026-09-16 @ $189.17 | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+2.1; leftover $2303.01 |
| `SM` | 57 | 2026-09-16 @ $39.99 | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2303.01 |
| `AMX` | 99 | 2026-09-16 @ $23.18 | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list oppset; 🔵; ret5=-0.2; leftover $2303.01 |
| `RARE` | 1 | 2026-09-18 @ $14.79 | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $18.39 |
| `BHVN` | 1 | 2026-09-18 @ $14.07 | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $18.39 |
