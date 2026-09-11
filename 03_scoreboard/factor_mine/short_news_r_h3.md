# Factor mine action — `short_news_r_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · news🔴

Cash book **+14.99%** ($11,499) · signal-only (no cash/fees) was +18.06%. Starts YES **17/20**. Fills 68 · skips 80 · realized $+1499.03.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only short names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will fall.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the news camera (does the morning packet like the headline?) is red.

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- Keep the first 8 names in list order.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a SHORT sleeve: it borrows the name and profits if the price falls. Equity treats the short as a liability (must keep enough to cover).

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news=bad` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $11,499.01.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `EU` | 1412 | — | $1.18 | +0.00 | $1.21 | -42.36 | -42.36 | -0.00 | -42.36 |
| 2026-08-14 | `LUNR` | 86 | — | $19.17 | +0.00 | $19.01 | +13.76 | +13.76 | -0.00 | +13.76 |
| 2026-08-14 | `OWL` | 131 | — | $12.70 | +0.00 | $12.22 | +62.22 | +62.22 | -0.00 | +62.22 |
| 2026-08-17 | `EU` | 1412 | $1.21 | $1.21 | +0.00 | $1.13 | +112.96 | +112.96 | -42.36 | +70.60 |
| 2026-08-17 | `LUNR` | 86 | $19.01 | $20.25 | -106.64 | $20.38 | -11.18 | -117.82 | -92.88 | -104.06 |
| 2026-08-17 | `OWL` | 131 | $12.22 | $12.12 | +13.10 | $11.66 | +60.26 | +73.36 | +75.33 | +135.59 |
| 2026-08-17 | `VERI` | 1077 | — | $1.15 | +0.00 | $1.08 | +70.00 | +70.00 | -0.00 | +70.00 |
| 2026-08-17 | `ZNTL` | 348 | — | $3.56 | +0.00 | $3.71 | -50.46 | -50.46 | -0.00 | -50.46 |
| 2026-08-17 | `APMD` | 39 | — | $31.70 | +0.00 | $32.55 | -33.15 | -33.15 | -0.00 | -33.15 |
| 2026-08-17 | `HIVE` | 411 | — | $3.01 | +0.00 | $3.07 | -24.66 | -24.66 | -0.00 | -24.66 |
| 2026-08-18 | `EU` | 1412 | $1.13 | $1.13 | +0.00 | $1.07 | +84.72 | +84.72 | +70.60 | +155.32 |
| 2026-08-18 | `LUNR` | 86 | $20.38 | $19.31 | +92.02 | $19.31 | +0.00 | +92.02 | -12.04 | -12.04 |
| 2026-08-18 | `OWL` | 131 | $11.66 | $11.54 | +15.72 | $11.59 | -6.55 | +9.17 | +151.31 | +144.76 |
| 2026-08-18 | `VERI` | 1077 | $1.08 | $1.05 | +37.69 | $0.99 | +59.24 | +96.93 | +107.70 | +166.93 |
| 2026-08-18 | `ZNTL` | 348 | $3.71 | $3.75 | -15.66 | $3.68 | +24.36 | +8.70 | -66.12 | -41.76 |
| 2026-08-18 | `APMD` | 39 | $32.55 | $32.85 | -11.70 | $31.81 | +40.56 | +28.86 | -44.85 | -4.29 |
| 2026-08-18 | `HIVE` | 411 | $3.07 | $2.96 | +45.21 | $2.78 | +73.98 | +119.19 | +20.55 | +94.53 |
| 2026-08-19 | `EU` | 1412 | $1.07 | $1.07 | +0.00 | — | +0.00 | +0.00 | +155.32 | — |
| 2026-08-19 | `LUNR` | 86 | $19.31 | $18.98 | +28.38 | $18.52 | +39.56 | +67.94 | +16.34 | +55.90 |
| 2026-08-19 | `OWL` | 131 | $11.59 | $11.75 | -20.96 | — | +0.00 | -20.96 | +123.80 | — |
| 2026-08-19 | `VERI` | 1077 | $0.99 | $1.00 | -5.39 | $0.97 | +36.62 | +31.23 | +161.55 | +198.17 |
| 2026-08-19 | `ZNTL` | 348 | $3.68 | $3.76 | -27.84 | $3.82 | -20.88 | -48.72 | -69.60 | -90.48 |
| 2026-08-19 | `APMD` | 39 | $31.81 | $32.13 | -12.48 | $32.03 | +3.90 | -8.58 | -16.77 | -12.87 |
| 2026-08-19 | `HIVE` | 411 | $2.78 | $2.78 | +0.00 | $2.82 | -16.44 | -16.44 | +94.53 | +78.09 |
| 2026-08-20 | `LUNR` | 86 | $18.52 | $18.13 | +33.54 | — | +0.00 | +33.54 | +89.44 | — |
| 2026-08-20 | `VERI` | 1077 | $0.97 | $0.96 | +3.23 | — | +0.00 | +3.23 | +201.40 | — |
| 2026-08-20 | `ZNTL` | 348 | $3.82 | $4.01 | -67.86 | — | +0.00 | -67.86 | -158.34 | — |
| 2026-08-20 | `APMD` | 39 | $32.03 | $31.87 | +6.24 | — | +0.00 | +6.24 | -6.63 | — |
| 2026-08-20 | `HIVE` | 411 | $2.82 | $2.95 | -53.43 | — | +0.00 | -53.43 | +24.66 | — |
| 2026-08-20 | `AEM` | 3 | — | $204.45 | +0.00 | $212.04 | -22.77 | -22.77 | -0.00 | -22.77 |
| 2026-08-20 | `WYFI` | 30 | — | $21.40 | +0.00 | $21.16 | +7.20 | +7.20 | -0.00 | +7.20 |
| 2026-08-20 | `TOYO` | 145 | — | $4.43 | +0.00 | $4.51 | -12.32 | -12.32 | -0.00 | -12.32 |
| 2026-08-20 | `ABCL` | 54 | — | $11.81 | +0.00 | $11.57 | +13.23 | +13.23 | -0.00 | +13.23 |
| 2026-08-20 | `TEAM` | 3 | — | $173.90 | +0.00 | $174.91 | -3.03 | -3.03 | -0.00 | -3.03 |
| 2026-08-20 | `AAP` | 13 | — | $46.85 | +0.00 | $42.39 | +57.98 | +57.98 | -0.00 | +57.98 |
| 2026-08-20 | `WMT` | 6 | — | $106.38 | +0.00 | $103.84 | +15.24 | +15.24 | -0.00 | +15.24 |
| 2026-08-20 | `AQST` | 140 | — | $4.61 | +0.00 | $4.50 | +16.10 | +16.10 | -0.00 | +16.10 |
| 2026-08-21 | `AEM` | 3 | $212.04 | $216.30 | -12.78 | $216.06 | +0.72 | -12.06 | -35.55 | -34.83 |
| 2026-08-21 | `WYFI` | 30 | $21.16 | $21.54 | -11.40 | $20.72 | +24.60 | +13.20 | -4.20 | +20.40 |
| 2026-08-21 | `TOYO` | 145 | $4.51 | $4.68 | -23.93 | $4.82 | -20.30 | -44.23 | -36.25 | -56.55 |
| 2026-08-21 | `ABCL` | 54 | $11.57 | $11.57 | +0.00 | $11.32 | +13.50 | +13.50 | +13.23 | +26.73 |
| 2026-08-21 | `TEAM` | 3 | $174.91 | $174.22 | +2.07 | $171.81 | +7.23 | +9.30 | -0.96 | +6.27 |
| 2026-08-21 | `AAP` | 13 | $42.39 | $42.41 | -0.26 | $42.58 | -2.21 | -2.47 | +57.72 | +55.51 |
| 2026-08-21 | `WMT` | 6 | $103.84 | $103.69 | +0.90 | $103.70 | -0.06 | +0.84 | +16.14 | +16.08 |
| 2026-08-21 | `AQST` | 140 | $4.50 | $4.54 | -6.30 | $4.66 | -16.80 | -23.10 | +9.80 | -7.00 |
| 2026-08-21 | `QTRX` | 276 | — | $3.11 | +0.00 | $2.99 | +33.12 | +33.12 | -0.00 | +33.12 |
| 2026-08-21 | `MRNA` | 6 | — | $133.11 | +0.00 | $145.13 | -72.12 | -72.12 | -0.00 | -72.12 |
| 2026-08-21 | `AUGO` | 9 | — | $89.10 | +0.00 | $87.26 | +16.56 | +16.56 | -0.00 | +16.56 |
| 2026-08-21 | `SSRM` | 22 | — | $38.40 | +0.00 | $37.77 | +13.86 | +13.86 | -0.00 | +13.86 |
| 2026-08-21 | `ARIS` | 41 | — | $20.90 | +0.00 | $20.86 | +1.64 | +1.64 | -0.00 | +1.64 |
| 2026-08-21 | `NOG` | 31 | — | $27.00 | +0.00 | $27.34 | -10.54 | -10.54 | -0.00 | -10.54 |
| 2026-08-24 | `AEM` | 3 | $216.06 | $217.03 | -2.91 | $217.89 | -2.58 | -5.49 | -37.74 | -40.32 |
| 2026-08-24 | `WYFI` | 30 | $20.72 | $20.01 | +21.30 | $20.78 | -23.10 | -1.80 | +41.70 | +18.60 |
| 2026-08-24 | `TOYO` | 145 | $4.82 | $4.58 | +34.80 | $4.38 | +29.00 | +63.80 | -21.75 | +7.25 |
| 2026-08-24 | `ABCL` | 54 | $11.32 | $10.97 | +18.90 | $10.61 | +19.44 | +38.34 | +45.63 | +65.07 |
| 2026-08-24 | `TEAM` | 3 | $171.81 | $169.30 | +7.53 | $171.33 | -6.09 | +1.44 | +13.80 | +7.71 |
| 2026-08-24 | `AAP` | 13 | $42.58 | $43.05 | -6.11 | $43.63 | -7.54 | -13.65 | +49.40 | +41.86 |
| 2026-08-24 | `WMT` | 6 | $103.70 | $104.14 | -2.64 | $106.49 | -14.10 | -16.74 | +13.44 | -0.66 |
| 2026-08-24 | `AQST` | 140 | $4.66 | $4.67 | -1.40 | $4.80 | -18.20 | -19.60 | -8.40 | -26.60 |
| 2026-08-24 | `QTRX` | 276 | $2.99 | $2.99 | +0.00 | $2.80 | +52.44 | +52.44 | +33.12 | +85.56 |
| 2026-08-24 | `MRNA` | 6 | $145.13 | $142.70 | +14.58 | $138.89 | +22.86 | +37.44 | -57.54 | -34.68 |
| 2026-08-24 | `AUGO` | 9 | $87.26 | $88.60 | -12.06 | $87.37 | +11.07 | -0.99 | +4.50 | +15.57 |
| 2026-08-24 | `SSRM` | 22 | $37.77 | $38.32 | -12.10 | $38.61 | -6.38 | -18.48 | +1.76 | -4.62 |
| 2026-08-24 | `ARIS` | 41 | $20.86 | $20.98 | -4.92 | $20.81 | +6.97 | +2.05 | -3.28 | +3.69 |
| 2026-08-24 | `NOG` | 31 | $27.34 | $27.12 | +6.82 | $26.84 | +8.68 | +15.50 | -3.72 | +4.96 |
| 2026-08-25 | `AEM` | 3 | $217.89 | $212.00 | +17.67 | — | +0.00 | +17.67 | -22.65 | — |
| 2026-08-25 | `WYFI` | 30 | $20.78 | $20.90 | -3.60 | — | +0.00 | -3.60 | +15.00 | — |
| 2026-08-25 | `TOYO` | 145 | $4.38 | $4.42 | -5.80 | — | +0.00 | -5.80 | +1.45 | — |
| 2026-08-25 | `ABCL` | 54 | $10.61 | $11.00 | -21.06 | — | +0.00 | -21.06 | +44.01 | — |
| 2026-08-25 | `TEAM` | 3 | $171.33 | $170.64 | +2.07 | — | +0.00 | +2.07 | +9.78 | — |
| 2026-08-25 | `AAP` | 13 | $43.63 | $43.63 | +0.00 | — | +0.00 | +0.00 | +41.86 | — |
| 2026-08-25 | `WMT` | 6 | $106.49 | $105.58 | +5.46 | — | +0.00 | +5.46 | +4.80 | — |
| 2026-08-25 | `AQST` | 140 | $4.80 | $4.77 | +4.20 | — | +0.00 | +4.20 | -22.40 | — |
| 2026-08-25 | `QTRX` | 276 | $2.80 | $2.80 | +0.00 | $2.79 | +2.76 | +2.76 | +85.56 | +88.32 |
| 2026-08-25 | `MRNA` | 6 | $138.89 | $143.50 | -27.66 | $158.83 | -91.98 | -119.64 | -62.34 | -154.32 |
| 2026-08-25 | `AUGO` | 9 | $87.37 | $85.78 | +14.31 | $90.47 | -42.21 | -27.90 | +29.88 | -12.33 |
| 2026-08-25 | `SSRM` | 22 | $38.61 | $37.75 | +18.92 | $39.21 | -32.12 | -13.20 | +14.30 | -17.82 |
| 2026-08-25 | `ARIS` | 41 | $20.81 | $20.45 | +14.76 | $21.18 | -29.93 | -15.17 | +18.45 | -11.48 |
| 2026-08-25 | `NOG` | 31 | $26.84 | $26.06 | +24.18 | $26.42 | -11.16 | +13.02 | +29.14 | +17.98 |
| 2026-08-25 | `AVAH` | 128 | — | $13.62 | +0.00 | $13.59 | +4.48 | +4.48 | -0.00 | +4.48 |
| 2026-08-25 | `ARE` | 32 | — | $54.51 | +0.00 | $52.90 | +51.52 | +51.52 | -0.00 | +51.52 |
| 2026-08-25 | `BMO` | 9 | — | $175.01 | +0.00 | $173.46 | +13.95 | +13.95 | -0.00 | +13.95 |
| 2026-08-26 | `QTRX` | 276 | $2.79 | $2.83 | -11.04 | — | +0.00 | -11.04 | +77.28 | — |
| 2026-08-26 | `MRNA` | 6 | $158.83 | $154.20 | +27.78 | — | +0.00 | +27.78 | -126.54 | — |
| 2026-08-26 | `AUGO` | 9 | $90.47 | $88.24 | +20.07 | — | +0.00 | +20.07 | +7.74 | — |
| 2026-08-26 | `SSRM` | 22 | $39.21 | $38.41 | +17.60 | — | +0.00 | +17.60 | -0.22 | — |
| 2026-08-26 | `ARIS` | 41 | $21.18 | $20.50 | +27.88 | — | +0.00 | +27.88 | +16.40 | — |
| 2026-08-26 | `NOG` | 31 | $26.42 | $26.00 | +13.02 | — | +0.00 | +13.02 | +31.00 | — |
| 2026-08-26 | `AVAH` | 128 | $13.59 | $13.65 | -7.68 | $13.62 | +3.84 | -3.84 | -3.20 | +0.64 |
| 2026-08-26 | `ARE` | 32 | $52.90 | $52.77 | +4.16 | $52.97 | -6.40 | -2.24 | +55.68 | +49.28 |
| 2026-08-26 | `BMO` | 9 | $173.46 | $173.22 | +2.16 | $172.90 | +2.88 | +5.04 | +16.11 | +18.99 |
| 2026-08-26 | `BE` | 6 | — | $213.94 | +0.00 | $218.21 | -25.62 | -25.62 | -0.00 | -25.62 |
| 2026-08-26 | `ABCL` | 106 | — | $12.22 | +0.00 | $12.24 | -2.12 | -2.12 | -0.00 | -2.12 |
| 2026-08-26 | `AQST` | 256 | — | $5.08 | +0.00 | $5.39 | -79.36 | -79.36 | -0.00 | -79.36 |
| 2026-08-26 | `NEM` | 9 | — | $132.64 | +0.00 | $131.60 | +9.36 | +9.36 | -0.00 | +9.36 |
| 2026-08-27 | `AVAH` | 128 | $13.62 | $13.62 | +0.00 | $13.82 | -25.60 | -25.60 | +0.64 | -24.96 |
| 2026-08-27 | `ARE` | 32 | $52.97 | $52.45 | +16.64 | $52.28 | +5.44 | +22.08 | +65.92 | +71.36 |
| 2026-08-27 | `BMO` | 9 | $172.90 | $172.85 | +0.45 | $172.13 | +6.48 | +6.93 | +19.44 | +25.92 |
| 2026-08-27 | `BE` | 6 | $218.21 | $227.10 | -53.34 | $217.83 | +55.62 | +2.28 | -78.96 | -23.34 |
| 2026-08-27 | `ABCL` | 106 | $12.24 | $12.25 | -1.06 | $12.40 | -15.90 | -16.96 | -3.18 | -19.08 |
| 2026-08-27 | `AQST` | 256 | $5.39 | $5.39 | +0.00 | $5.16 | +58.88 | +58.88 | -79.36 | -20.48 |
| 2026-08-27 | `NEM` | 9 | $131.60 | $131.02 | +5.22 | $132.29 | -11.43 | -6.21 | +14.58 | +3.15 |
| 2026-08-28 | `AVAH` | 128 | $13.82 | $13.90 | -10.24 | — | +0.00 | -10.24 | -35.20 | — |
| 2026-08-28 | `ARE` | 32 | $52.28 | $52.49 | -6.72 | — | +0.00 | -6.72 | +64.64 | — |
| 2026-08-28 | `BMO` | 9 | $172.13 | $172.76 | -5.67 | — | +0.00 | -5.67 | +20.25 | — |
| 2026-08-28 | `BE` | 6 | $217.83 | $215.71 | +12.75 | $210.77 | +29.61 | +42.36 | -10.59 | +19.02 |
| 2026-08-28 | `ABCL` | 106 | $12.40 | $12.30 | +10.07 | $11.35 | +101.23 | +111.30 | -9.01 | +92.22 |
| 2026-08-28 | `AQST` | 256 | $5.16 | $5.11 | +12.80 | $5.02 | +23.04 | +35.84 | -7.68 | +15.36 |
| 2026-08-28 | `NEM` | 9 | $132.29 | $132.35 | -0.54 | $127.98 | +39.33 | +38.79 | +2.61 | +41.94 |
| 2026-08-28 | `SIMO` | 10 | — | $252.24 | +0.00 | $245.81 | +64.30 | +64.30 | -0.00 | +64.30 |
| 2026-08-28 | `FIG` | 85 | — | $30.18 | +0.00 | $28.82 | +115.60 | +115.60 | -0.00 | +115.60 |
| 2026-08-31 | `BE` | 6 | $210.77 | $208.88 | +11.34 | — | +0.00 | +11.34 | +30.36 | — |
| 2026-08-31 | `ABCL` | 106 | $11.35 | $11.10 | +26.50 | — | +0.00 | +26.50 | +118.72 | — |
| 2026-08-31 | `AQST` | 256 | $5.02 | $4.97 | +11.52 | — | +0.00 | +11.52 | +26.88 | — |
| 2026-08-31 | `NEM` | 9 | $127.98 | $127.45 | +4.77 | — | +0.00 | +4.77 | +46.71 | — |
| 2026-08-31 | `SIMO` | 10 | $245.81 | $247.05 | -12.40 | $246.84 | +2.10 | -10.30 | +51.90 | +54.00 |
| 2026-08-31 | `FIG` | 85 | $28.82 | $27.60 | +103.70 | $27.49 | +9.35 | +113.05 | +219.30 | +228.65 |
| 2026-09-01 | `SIMO` | 10 | $246.84 | $240.09 | +67.50 | $237.35 | +27.40 | +94.90 | +121.50 | +148.90 |
| 2026-09-01 | `FIG` | 85 | $27.49 | $27.06 | +36.55 | $27.20 | -11.90 | +24.65 | +265.20 | +253.30 |
| 2026-09-02 | `SIMO` | 10 | $237.35 | $235.71 | +16.40 | — | +0.00 | +16.40 | +165.30 | — |
| 2026-09-02 | `FIG` | 85 | $27.20 | $26.78 | +35.70 | — | +0.00 | +35.70 | +289.00 | — |
| 2026-09-03 | `SLN` | 185 | — | $14.85 | +0.00 | $14.79 | +11.10 | +11.10 | -0.00 | +11.10 |
| 2026-09-03 | `OPK` | 1612 | — | $1.71 | +0.00 | $1.61 | +161.20 | +161.20 | -0.00 | +161.20 |
| 2026-09-04 | `SLN` | 185 | $14.79 | $14.63 | +29.60 | $14.59 | +7.40 | +37.00 | +40.70 | +48.10 |
| 2026-09-04 | `OPK` | 1612 | $1.61 | $1.59 | +32.24 | $1.64 | -80.60 | -48.36 | +193.44 | +112.84 |
| 2026-09-04 | `GSM` | 601 | — | $4.67 | +0.00 | $4.67 | +0.00 | +0.00 | -0.00 | -0.00 |
| 2026-09-04 | `PIPR` | 36 | — | $76.55 | +0.00 | $77.04 | -17.64 | -17.64 | -0.00 | -17.64 |
| 2026-09-08 | `SLN` | 185 | $14.59 | $14.24 | +64.75 | $13.69 | +101.75 | +166.50 | +112.85 | +214.60 |
| 2026-09-08 | `OPK` | 1612 | $1.64 | $1.63 | +16.12 | $1.59 | +64.48 | +80.60 | +128.96 | +193.44 |
| 2026-09-08 | `GSM` | 601 | $4.67 | $4.75 | -48.08 | $4.52 | +138.23 | +90.15 | -48.08 | +90.15 |
| 2026-09-08 | `PIPR` | 36 | $77.04 | $76.64 | +14.40 | $77.34 | -25.20 | -10.80 | -3.24 | -28.44 |
| 2026-09-09 | `SLN` | 185 | $13.69 | $13.60 | +16.65 | — | +0.00 | +16.65 | +231.25 | — |
| 2026-09-09 | `OPK` | 1612 | $1.59 | $1.58 | +16.12 | — | +0.00 | +16.12 | +209.56 | — |
| 2026-09-09 | `GSM` | 601 | $4.52 | $4.52 | +0.00 | $4.49 | +18.03 | +18.03 | +90.15 | +108.18 |
| 2026-09-09 | `PIPR` | 36 | $77.34 | $77.24 | +3.60 | $76.96 | +10.08 | +13.68 | -24.84 | -14.76 |
| 2026-09-10 | `GSM` | 601 | $4.49 | $4.49 | +0.00 | — | +0.00 | +0.00 | +108.18 | — |
| 2026-09-10 | `PIPR` | 36 | $76.96 | $76.96 | +0.00 | — | +0.00 | +0.00 | -14.76 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +33.62 | EU, LUNR, OWL | — | $14,954.53 | $10,010.33 | EU×1412, LUNR×86, OWL×131 |
| 2026-08-17 | +2.25 | $14,954.53 | EU×1412, LUNR×86, OWL×131 | $9,916.79 | -93.54 | +123.77 | VERI, ZNTL, APMD, HIVE | — | $19,879.09 | $10,014.29 | EU×1412, LUNR×86, OWL×131, VERI×1077, ZNTL×348, APMD×39, HIVE×411 |
| 2026-08-18 | -6.20 | $19,879.09 | EU×1412, LUNR×86, OWL×131, VERI×1077, ZNTL×348, APMD×39, HIVE×411 | $10,177.57 | +163.28 | +276.31 | — | — | $19,879.09 | $10,453.88 | EU×1412, LUNR×86, OWL×131, VERI×1077, ZNTL×348, APMD×39, HIVE×411 |
| 2026-08-19 | -7.20 | $19,879.09 | EU×1412, LUNR×86, OWL×131, VERI×1077, ZNTL×348, APMD×39, HIVE×411 | $10,415.59 | -38.29 | +42.76 | — | EU, OWL | $16,808.40 | $10,437.75 | LUNR×86, VERI×1077, ZNTL×348, APMD×39, HIVE×411 |
| 2026-08-20 | +1.12 | $16,808.40 | LUNR×86, VERI×1077, ZNTL×348, APMD×39, HIVE×411 | $10,359.47 | -78.28 | +71.63 | AEM, WYFI, TOYO, ABCL, TEAM, AAP, WMT, AQST | LUNR, VERI, ZNTL, APMD, HIVE | $15,264.44 | $10,385.92 | AEM×3, WYFI×30, TOYO×145, ABCL×54, TEAM×3, AAP×13, WMT×6, AQST×140 |
| 2026-08-21 | +3.25 | $15,264.44 | AEM×3, WYFI×30, TOYO×145, ABCL×54, TEAM×3, AAP×13, WMT×6, AQST×140 | $10,334.23 | -51.69 | -10.80 | QTRX, MRNA, AUGO, SSRM, ARIS, NOG | — | $20,247.93 | $10,309.30 | AEM×3, WYFI×30, TOYO×145, ABCL×54, TEAM×3, AAP×13, WMT×6, AQST×140, QTRX×276, MRNA×6, AUGO×9, SSRM×22, ARIS×41, NOG×31 |
| 2026-08-24 | -5.17 | $20,247.93 | AEM×3, WYFI×30, TOYO×145, ABCL×54, TEAM×3, AAP×13, WMT×6, AQST×140, QTRX×276, MRNA×6, AUGO×9, SSRM×22, ARIS×41, NOG×31 | $10,371.09 | +61.79 | +72.47 | — | — | $20,247.93 | $10,443.56 | AEM×3, WYFI×30, TOYO×145, ABCL×54, TEAM×3, AAP×13, WMT×6, AQST×140, QTRX×276, MRNA×6, AUGO×9, SSRM×22, ARIS×41, NOG×31 |
| 2026-08-25 | +1.80 | $20,247.93 | AEM×3, WYFI×30, TOYO×145, ABCL×54, TEAM×3, AAP×13, WMT×6, AQST×140, QTRX×276, MRNA×6, AUGO×9, SSRM×22, ARIS×41, NOG×31 | $10,487.01 | +43.45 | -134.69 | AVAH, ARE, BMO | AEM, WYFI, TOYO, ABCL, TEAM, AAP, WMT, AQST | $20,409.25 | $10,328.52 | QTRX×276, MRNA×6, AUGO×9, SSRM×22, ARIS×41, NOG×31, AVAH×128, ARE×32, BMO×9 |
| 2026-08-26 | +2.02 | $20,409.25 | QTRX×276, MRNA×6, AUGO×9, SSRM×22, ARIS×41, NOG×31, AVAH×128, ARE×32, BMO×9 | $10,422.47 | +93.95 | -97.42 | BE, ABCL, AQST, NEM | QTRX, MRNA, AUGO, SSRM, ARIS, NOG | $20,466.76 | $10,301.32 | AVAH×128, ARE×32, BMO×9, BE×6, ABCL×106, AQST×256, NEM×9 |
| 2026-08-27 | — | $20,466.76 | AVAH×128, ARE×32, BMO×9, BE×6, ABCL×106, AQST×256, NEM×9 | $10,269.23 | -32.09 | +73.49 | — | — | $20,466.76 | $10,342.72 | AVAH×128, ARE×32, BMO×9, BE×6, ABCL×106, AQST×256, NEM×9 |
| 2026-08-28 | +0.75 | $20,466.76 | AVAH×128, ARE×32, BMO×9, BE×6, ABCL×106, AQST×256, NEM×9 | $10,355.17 | +12.45 | +373.11 | SIMO, FIG | AVAH, ARE, BMO | $20,529.79 | $10,717.33 | BE×6, ABCL×106, AQST×256, NEM×9, SIMO×10, FIG×85 |
| 2026-08-31 | -5.85 | $20,529.79 | BE×6, ABCL×106, AQST×256, NEM×9, SIMO×10, FIG×85 | $10,862.76 | +145.43 | +11.45 | — | BE, ABCL, AQST, NEM | $15,669.62 | $10,864.57 | SIMO×10, FIG×85 |
| 2026-09-01 | -6.30 | $15,669.62 | SIMO×10, FIG×85 | $10,968.62 | +104.05 | +15.50 | — | — | $15,669.62 | $10,984.12 | SIMO×10, FIG×85 |
| 2026-09-02 | -3.83 | $15,669.62 | SIMO×10, FIG×85 | $11,036.22 | +52.10 | +0.00 | — | SIMO, FIG | $11,031.96 | $11,031.96 | — |
| 2026-09-03 | -0.90 | $11,031.96 | — | $11,031.96 | -0.00 | +172.30 | SLN, OPK | — | $16,511.90 | $11,180.43 | SLN×185, OPK×1612 |
| 2026-09-04 | +2.25 | $16,511.90 | SLN×185, OPK×1612 | $11,242.27 | +61.84 | -90.84 | GSM, PIPR | — | $22,064.21 | $11,141.27 | SLN×185, OPK×1612, GSM×601, PIPR×36 |
| 2026-09-08 | -11.47 | $22,064.21 | SLN×185, OPK×1612, GSM×601, PIPR×36 | $11,188.46 | +47.19 | +279.26 | — | — | $22,064.21 | $11,467.72 | SLN×185, OPK×1612, GSM×601, PIPR×36 |
| 2026-09-09 | -13.95 | $22,064.21 | SLN×185, OPK×1612, GSM×601, PIPR×36 | $11,504.09 | +36.37 | +28.11 | — | SLN, OPK | $16,977.91 | $11,508.86 | GSM×601, PIPR×36 |
| 2026-09-10 | -13.28 | $16,977.91 | GSM×601, PIPR×36 | $11,508.86 | -0.00 | +0.00 | — | GSM, PIPR | $11,499.01 | $11,499.01 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 1412 | $1.18 | $18.51 | — | $11,647.65 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 86 | $19.17 | $2.32 | — | $13,293.95 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 131 | $12.70 | $2.46 | — | $14,954.53 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,954.53 | ▲ close $10,010.33 vs 09:30 $10,000.00 (session +33.62) | 16:00 close · cash $14,954.53 · equity $10,010.33 vs 09:30 $10,000.00 (+10.33; session marks +33.62) · 3 name(s) marked open→close (per-name table). EU×1412 09:30 $1.18 → close $1.21 -42.36; LUNR×86 09:30 $19.17 → close $19.01 +13.76; OWL×131 09:30 $12.70 → close $12.22 +62.22 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,954.53 | ▼ 09:30 equity $9,916.79 vs yday $10,010.33 (-93.54) | 09:30 open · cash $14,954.53 (unchanged overnight, no fees) · equity $9,916.79 vs prior close $10,010.33 (-93.54) · 3 name(s) re-marked at the open (per-name table). EU×1412 yday $1.21 → 09:30 $1.21 -0.00; LUNR×86 yday $19.01 → 09:30 $20.25 -106.64; OWL×131 yday $12.22 → 09:30 $12.12 +13.10 | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 1077 | $1.15 | $14.12 | — | $16,178.97 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; leftover $1239.60 | join🟡 sector🟢 gen🟢 news🔴 judge🟢 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 348 | $3.56 | $4.59 | — | $17,413.26 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; leftover $1239.60 | join🟡 sector🔴 gen🟢 news🔴 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 39 | $31.70 | $2.16 | — | $18,647.39 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; leftover $1239.60 | join🟡 sector🔴 gen🟢 news🔴 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 411 | $3.01 | $5.41 | — | $19,879.09 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; leftover $1239.60 | join🟢 sector🟢 gen🟢 news🔴 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,879.09 | ▲ close $10,014.29 vs 09:30 $9,916.79 (session +123.77) | 16:00 close · cash $19,879.09 · equity $10,014.29 vs 09:30 $9,916.79 (+97.50; session marks +123.77) · 7 name(s) marked open→close (per-name table). EU×1412 09:30 $1.21 → close $1.13 +112.96; LUNR×86 09:30 $20.25 → close $20.38 -11.18; OWL×131 09:30 $12.12 → close $11.66 +60.26; VERI×1077 09:30 $1.15 → close $1.08 +70.00; ZNTL×348 09:30 $3.56 → close $3.71 -50.46; APMD×39 09:30 $31.70 → close $32.55 -33.15; HIVE×411 09:30 $3.01 → close $3.07 -24.66 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,879.09 | ▲ 09:30 equity $10,177.57 vs yday $10,014.29 (+163.28) | 09:30 open · cash $19,879.09 (unchanged overnight, no fees) · equity $10,177.57 vs prior close $10,014.29 (+163.28) · 7 name(s) re-marked at the open (per-name table). EU×1412 yday $1.13 → 09:30 $1.13 -0.00; LUNR×86 yday $20.38 → 09:30 $19.31 +92.02; OWL×131 yday $11.66 → 09:30 $11.54 +15.72; VERI×1077 yday $1.08 → 09:30 $1.05 +37.69; ZNTL×348 yday $3.71 → 09:30 $3.75 -15.66; APMD×39 yday $32.55 → 09:30 $32.85 -11.70; HIVE×411 yday $3.07 → 09:30 $2.96 +45.21 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,879.09 | ▲ close $10,453.88 vs 09:30 $10,177.57 (session +276.31) | 16:00 close · cash $19,879.09 · equity $10,453.88 vs 09:30 $10,177.57 (+276.31; session marks +276.31) · 7 name(s) marked open→close (per-name table). EU×1412 09:30 $1.13 → close $1.07 +84.72; LUNR×86 09:30 $19.31 → close $19.31 -0.00; OWL×131 09:30 $11.54 → close $11.59 -6.55; VERI×1077 09:30 $1.05 → close $0.99 +59.24; ZNTL×348 09:30 $3.75 → close $3.68 +24.36; APMD×39 09:30 $32.85 → close $31.81 +40.56; HIVE×411 09:30 $2.96 → close $2.78 +73.98 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,879.09 | ▼ 09:30 equity $10,415.59 vs yday $10,453.88 (-38.29) | 09:30 open · cash $19,879.09 (unchanged overnight, no fees) · equity $10,415.59 vs prior close $10,453.88 (-38.29) · 7 name(s) re-marked at the open (per-name table). EU×1412 yday $1.07 → 09:30 $1.07 -0.00; LUNR×86 yday $19.31 → 09:30 $18.98 +28.38; OWL×131 yday $11.59 → 09:30 $11.75 -20.96; VERI×1077 yday $0.99 → 09:30 $1.00 -5.39; ZNTL×348 yday $3.68 → 09:30 $3.76 -27.84; APMD×39 yday $31.81 → 09:30 $32.13 -12.48; HIVE×411 yday $2.78 → 09:30 $2.78 -0.00 | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 1412 | $1.07 | $18.21 | $+118.60 | $18,350.04 | ▲ +118.60 after sell → book $10,397.38; vs 09:30 mark -18.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 131 | $11.75 | $2.38 | $+118.95 | $16,808.40 | ▲ +118.95 after sell → book $10,394.99; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,808.40 | ▲ close $10,437.75 vs 09:30 $10,415.59 (session +42.76) | 16:00 close · cash $16,808.40 · equity $10,437.75 vs 09:30 $10,415.59 (+22.16; session marks +42.76) · 5 name(s) marked open→close (per-name table). LUNR×86 09:30 $18.98 → close $18.52 +39.56; VERI×1077 09:30 $1.00 → close $0.97 +36.62; ZNTL×348 09:30 $3.76 → close $3.82 -20.88; APMD×39 09:30 $32.13 → close $32.03 +3.90; HIVE×411 09:30 $2.78 → close $2.82 -16.44 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,808.40 | ▼ 09:30 equity $10,359.47 vs yday $10,437.75 (-78.28) | 09:30 open · cash $16,808.40 (unchanged overnight, no fees) · equity $10,359.47 vs prior close $10,437.75 (-78.28) · 5 name(s) re-marked at the open (per-name table). LUNR×86 yday $18.52 → 09:30 $18.13 +33.54; VERI×1077 yday $0.97 → 09:30 $0.96 +3.23; ZNTL×348 yday $3.82 → 09:30 $4.01 -67.86; APMD×39 yday $32.03 → 09:30 $31.87 +6.24; HIVE×411 yday $2.82 → 09:30 $2.95 -53.43 | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 86 | $18.13 | $2.25 | $+84.87 | $15,246.97 | ▲ +84.87 after sell → book $10,357.22; vs 09:30 mark -2.25 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 1077 | $0.96 | $13.60 | $+173.68 | $14,196.22 | ▲ +173.68 after sell → book $10,343.62; vs 09:30 mark -13.60 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 348 | $4.01 | $4.49 | $-167.42 | $12,794.51 | ▼ -167.42 after sell → book $10,339.13; vs 09:30 mark -4.49 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 39 | $31.87 | $2.11 | $-10.90 | $11,549.48 | ▼ -10.90 after sell → book $10,337.03; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 411 | $2.95 | $5.30 | $+13.94 | $10,331.72 | ▲ +13.94 after sell → book $10,331.72; vs 09:30 mark -5.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $10,943.04 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $645.73 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 30 | $21.40 | $2.12 | — | $11,582.92 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; leftover $645.73 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 145 | $4.43 | $2.48 | — | $12,222.79 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; leftover $645.73 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 54 | $11.81 | $2.19 | — | $12,858.61 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $645.73 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 3 | $173.90 | $2.03 | — | $13,378.28 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; leftover $645.73 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 13 | $46.85 | $2.07 | — | $13,985.26 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=+5.0; leftover $645.73 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 6 | $106.38 | $2.05 | — | $14,621.50 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; leftover $645.73 | join🟢 sector🟡 gen🟢 news🔴 digest🟡 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 140 | $4.61 | $2.46 | — | $15,264.44 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; leftover $645.73 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,264.44 | ▲ close $10,385.92 vs 09:30 $10,359.47 (session +71.63) | 16:00 close · cash $15,264.44 · equity $10,385.92 vs 09:30 $10,359.47 (+26.45; session marks +71.63) · 8 name(s) marked open→close (per-name table). AEM×3 09:30 $204.45 → close $212.04 -22.77; WYFI×30 09:30 $21.40 → close $21.16 +7.20; TOYO×145 09:30 $4.43 → close $4.51 -12.32; ABCL×54 09:30 $11.81 → close $11.57 +13.23; TEAM×3 09:30 $173.90 → close $174.91 -3.03; AAP×13 09:30 $46.85 → close $42.39 +57.98; WMT×6 09:30 $106.38 → close $103.84 +15.24; AQST×140 09:30 $4.61 → close $4.50 +16.10 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,264.44 | ▼ 09:30 equity $10,334.23 vs yday $10,385.92 (-51.69) | 09:30 open · cash $15,264.44 (unchanged overnight, no fees) · equity $10,334.23 vs prior close $10,385.92 (-51.69) · 8 name(s) re-marked at the open (per-name table). AEM×3 yday $212.04 → 09:30 $216.30 -12.78; WYFI×30 yday $21.16 → 09:30 $21.54 -11.40; TOYO×145 yday $4.51 → 09:30 $4.68 -23.93; ABCL×54 yday $11.57 → 09:30 $11.57 -0.00; TEAM×3 yday $174.91 → 09:30 $174.22 +2.07; AAP×13 yday $42.39 → 09:30 $42.41 -0.26; WMT×6 yday $103.84 → 09:30 $103.69 +0.90; AQST×140 yday $4.50 → 09:30 $4.54 -6.30 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 276 | $3.11 | $3.64 | — | $16,119.16 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $861.19 | join🟡 sector🟢 gen🟢 news🔴 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 6 | $133.11 | $2.05 | — | $16,915.77 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; leftover $861.19 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 9 | $89.10 | $2.06 | — | $17,715.61 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $861.19 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 22 | $38.40 | $2.10 | — | $18,558.31 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $861.19 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 41 | $20.90 | $2.16 | — | $19,413.05 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $861.19 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 31 | $27.00 | $2.13 | — | $20,247.93 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; leftover $861.19 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,247.93 | ▼ close $10,309.30 vs 09:30 $10,334.23 (session -10.80) | 16:00 close · cash $20,247.93 · equity $10,309.30 vs 09:30 $10,334.23 (-24.93; session marks -10.80) · 14 name(s) marked open→close (per-name table). AEM×3 09:30 $216.30 → close $216.06 +0.72; WYFI×30 09:30 $21.54 → close $20.72 +24.60; TOYO×145 09:30 $4.68 → close $4.82 -20.30; ABCL×54 09:30 $11.57 → close $11.32 +13.50; TEAM×3 09:30 $174.22 → close $171.81 +7.23; AAP×13 09:30 $42.41 → close $42.58 -2.21; WMT×6 09:30 $103.69 → close $103.70 -0.06; AQST×140 09:30 $4.54 → close $4.66 -16.80; QTRX×276 09:30 $3.11 → close $2.99 +33.12; MRNA×6 09:30 $133.11 → close $145.13 -72.12; AUGO×9 09:30 $89.10 → close $87.26 +16.56; SSRM×22 09:30 $38.40 → close $37.77 +13.86; ARIS×41 09:30 $20.90 → close $20.86 +1.64; NOG×31 09:30 $27.00 → close $27.34 -10.54 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,247.93 | ▲ 09:30 equity $10,371.09 vs yday $10,309.30 (+61.79) | 09:30 open · cash $20,247.93 (unchanged overnight, no fees) · equity $10,371.09 vs prior close $10,309.30 (+61.79) · 14 name(s) re-marked at the open (per-name table). AEM×3 yday $216.06 → 09:30 $217.03 -2.91; WYFI×30 yday $20.72 → 09:30 $20.01 +21.30; TOYO×145 yday $4.82 → 09:30 $4.58 +34.80; ABCL×54 yday $11.32 → 09:30 $10.97 +18.90; TEAM×3 yday $171.81 → 09:30 $169.30 +7.53; AAP×13 yday $42.58 → 09:30 $43.05 -6.11; WMT×6 yday $103.70 → 09:30 $104.14 -2.64; AQST×140 yday $4.66 → 09:30 $4.67 -1.40; QTRX×276 yday $2.99 → 09:30 $2.99 -0.00; MRNA×6 yday $145.13 → 09:30 $142.70 +14.58; AUGO×9 yday $87.26 → 09:30 $88.60 -12.06; SSRM×22 yday $37.77 → 09:30 $38.32 -12.10; ARIS×41 yday $20.86 → 09:30 $20.98 -4.92; NOG×31 yday $27.34 → 09:30 $27.12 +6.82 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,247.93 | ▲ close $10,443.56 vs 09:30 $10,371.09 (session +72.47) | 16:00 close · cash $20,247.93 · equity $10,443.56 vs 09:30 $10,371.09 (+72.47; session marks +72.47) · 14 name(s) marked open→close (per-name table). AEM×3 09:30 $217.03 → close $217.89 -2.58; WYFI×30 09:30 $20.01 → close $20.78 -23.10; TOYO×145 09:30 $4.58 → close $4.38 +29.00; ABCL×54 09:30 $10.97 → close $10.61 +19.44; TEAM×3 09:30 $169.30 → close $171.33 -6.09; AAP×13 09:30 $43.05 → close $43.63 -7.54; WMT×6 09:30 $104.14 → close $106.49 -14.10; AQST×140 09:30 $4.67 → close $4.80 -18.20; QTRX×276 09:30 $2.99 → close $2.80 +52.44; MRNA×6 09:30 $142.70 → close $138.89 +22.86; AUGO×9 09:30 $88.60 → close $87.37 +11.07; SSRM×22 09:30 $38.32 → close $38.61 -6.38; ARIS×41 09:30 $20.98 → close $20.81 +6.97; NOG×31 09:30 $27.12 → close $26.84 +8.68 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,247.93 | ▲ 09:30 equity $10,487.01 vs yday $10,443.56 (+43.45) | 09:30 open · cash $20,247.93 (unchanged overnight, no fees) · equity $10,487.01 vs prior close $10,443.56 (+43.45) · 14 name(s) re-marked at the open (per-name table). AEM×3 yday $217.89 → 09:30 $212.00 +17.67; WYFI×30 yday $20.78 → 09:30 $20.90 -3.60; TOYO×145 yday $4.38 → 09:30 $4.42 -5.80; ABCL×54 yday $10.61 → 09:30 $11.00 -21.06; TEAM×3 yday $171.33 → 09:30 $170.64 +2.07; AAP×13 yday $43.63 → 09:30 $43.63 -0.00; WMT×6 yday $106.49 → 09:30 $105.58 +5.46; AQST×140 yday $4.80 → 09:30 $4.77 +4.20; QTRX×276 yday $2.80 → 09:30 $2.80 -0.00; MRNA×6 yday $138.89 → 09:30 $143.50 -27.66; AUGO×9 yday $87.37 → 09:30 $85.78 +14.31; SSRM×22 yday $38.61 → 09:30 $37.75 +18.92; ARIS×41 yday $20.81 → 09:30 $20.45 +14.76; NOG×31 yday $26.84 → 09:30 $26.06 +24.18 | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 3 | $212.00 | $2.00 | $-26.68 | $19,609.93 | ▼ -26.68 after sell → book $10,485.01; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 30 | $20.90 | $2.08 | $+10.80 | $18,980.85 | ▲ +10.80 after sell → book $10,482.93; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 145 | $4.42 | $2.42 | $-3.45 | $18,337.52 | ▼ -3.45 after sell → book $10,480.50; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 54 | $11.00 | $2.15 | $+39.67 | $17,741.37 | ▲ +39.67 after sell → book $10,478.35; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 3 | $170.64 | $2.00 | $+5.75 | $17,227.45 | ▲ +5.75 after sell → book $10,476.35; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 13 | $43.63 | $2.03 | $+37.77 | $16,658.23 | ▲ +37.77 after sell → book $10,474.32; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 6 | $105.58 | $2.01 | $+0.75 | $16,022.75 | ▲ +0.75 after sell → book $10,472.32; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 140 | $4.77 | $2.41 | $-27.27 | $15,352.54 | ▼ -27.27 after sell → book $10,469.91; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 128 | $13.62 | $2.46 | — | $17,094.08 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $1744.98 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 32 | $54.51 | $2.16 | — | $18,836.24 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; leftover $1744.98 | join🔴 sector🟡 gen🟡 news🔴 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 9 | $175.01 | $2.08 | — | $20,409.25 | — | news🔴; gate news=bad; list earn_react; ret5=-7.0; leftover $1744.98 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,409.25 | ▼ close $10,328.52 vs 09:30 $10,487.01 (session -134.69) | 16:00 close · cash $20,409.25 · equity $10,328.52 vs 09:30 $10,487.01 (-158.49; session marks -134.69) · 9 name(s) marked open→close (per-name table). QTRX×276 09:30 $2.80 → close $2.79 +2.76; MRNA×6 09:30 $143.50 → close $158.83 -91.98; AUGO×9 09:30 $85.78 → close $90.47 -42.21; SSRM×22 09:30 $37.75 → close $39.21 -32.12; ARIS×41 09:30 $20.45 → close $21.18 -29.93; NOG×31 09:30 $26.06 → close $26.42 -11.16; AVAH×128 09:30 $13.62 → close $13.59 +4.48; ARE×32 09:30 $54.51 → close $52.90 +51.52; BMO×9 09:30 $175.01 → close $173.46 +13.95 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,409.25 | ▲ 09:30 equity $10,422.47 vs yday $10,328.52 (+93.95) | 09:30 open · cash $20,409.25 (unchanged overnight, no fees) · equity $10,422.47 vs prior close $10,328.52 (+93.95) · 9 name(s) re-marked at the open (per-name table). QTRX×276 yday $2.79 → 09:30 $2.83 -11.04; MRNA×6 yday $158.83 → 09:30 $154.20 +27.78; AUGO×9 yday $90.47 → 09:30 $88.24 +20.07; SSRM×22 yday $39.21 → 09:30 $38.41 +17.60; ARIS×41 yday $21.18 → 09:30 $20.50 +27.88; NOG×31 yday $26.42 → 09:30 $26.00 +13.02; AVAH×128 yday $13.59 → 09:30 $13.65 -7.68; ARE×32 yday $52.90 → 09:30 $52.77 +4.16; BMO×9 yday $173.46 → 09:30 $173.22 +2.16 | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 276 | $2.83 | $3.56 | $+70.08 | $19,624.61 | ▲ +70.08 after sell → book $10,418.91; vs 09:30 mark -3.56 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MRNA` | 6 | $154.20 | $2.01 | $-130.60 | $18,697.40 | ▼ -130.60 after sell → book $10,416.90; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 9 | $88.24 | $2.02 | $+3.66 | $17,901.22 | ▲ +3.66 after sell → book $10,414.88; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 22 | $38.41 | $2.06 | $-4.38 | $17,054.15 | ▼ -4.38 after sell → book $10,412.83; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 41 | $20.50 | $2.11 | $+12.13 | $16,211.53 | ▲ +12.13 after sell → book $10,410.71; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 31 | $26.00 | $2.08 | $+26.79 | $15,403.45 | ▲ +26.79 after sell → book $10,408.63; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 6 | $213.94 | $2.06 | — | $16,685.03 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $1301.08 | join🟢 sector🔴 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 106 | $12.22 | $2.37 | — | $17,977.98 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; leftover $1301.08 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 256 | $5.08 | $3.39 | — | $19,275.07 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; leftover $1301.08 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 9 | $132.64 | $2.07 | — | $20,466.76 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; leftover $1301.08 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,466.76 | ▼ close $10,301.32 vs 09:30 $10,422.47 (session -97.42) | 16:00 close · cash $20,466.76 · equity $10,301.32 vs 09:30 $10,422.47 (-121.15; session marks -97.42) · 7 name(s) marked open→close (per-name table). AVAH×128 09:30 $13.65 → close $13.62 +3.84; ARE×32 09:30 $52.77 → close $52.97 -6.40; BMO×9 09:30 $173.22 → close $172.90 +2.88; BE×6 09:30 $213.94 → close $218.21 -25.62; ABCL×106 09:30 $12.22 → close $12.24 -2.12; AQST×256 09:30 $5.08 → close $5.39 -79.36; NEM×9 09:30 $132.64 → close $131.60 +9.36 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,466.76 | ▼ 09:30 equity $10,269.23 vs yday $10,301.32 (-32.09) | 09:30 open · cash $20,466.76 (unchanged overnight, no fees) · equity $10,269.23 vs prior close $10,301.32 (-32.09) · 7 name(s) re-marked at the open (per-name table). AVAH×128 yday $13.62 → 09:30 $13.62 -0.00; ARE×32 yday $52.97 → 09:30 $52.45 +16.64; BMO×9 yday $172.90 → 09:30 $172.85 +0.45; BE×6 yday $218.21 → 09:30 $227.10 -53.34; ABCL×106 yday $12.24 → 09:30 $12.25 -1.06; AQST×256 yday $5.39 → 09:30 $5.39 -0.00; NEM×9 yday $131.60 → 09:30 $131.02 +5.22 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,466.76 | ▲ close $10,342.72 vs 09:30 $10,269.23 (session +73.49) | 16:00 close · cash $20,466.76 · equity $10,342.72 vs 09:30 $10,269.23 (+73.49; session marks +73.49) · 7 name(s) marked open→close (per-name table). AVAH×128 09:30 $13.62 → close $13.82 -25.60; ARE×32 09:30 $52.45 → close $52.28 +5.44; BMO×9 09:30 $172.85 → close $172.13 +6.48; BE×6 09:30 $227.10 → close $217.83 +55.62; ABCL×106 09:30 $12.25 → close $12.40 -15.90; AQST×256 09:30 $5.39 → close $5.16 +58.88; NEM×9 09:30 $131.02 → close $132.29 -11.43 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,466.76 | ▲ 09:30 equity $10,355.17 vs yday $10,342.72 (+12.45) | 09:30 open · cash $20,466.76 (unchanged overnight, no fees) · equity $10,355.17 vs prior close $10,342.72 (+12.45) · 7 name(s) re-marked at the open (per-name table). AVAH×128 yday $13.82 → 09:30 $13.90 -10.24; ARE×32 yday $52.28 → 09:30 $52.49 -6.72; BMO×9 yday $172.13 → 09:30 $172.76 -5.67; BE×6 yday $217.83 → 09:30 $215.71 +12.75; ABCL×106 yday $12.40 → 09:30 $12.30 +10.07; AQST×256 yday $5.16 → 09:30 $5.11 +12.80; NEM×9 yday $132.29 → 09:30 $132.35 -0.54 | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 128 | $13.90 | $2.37 | $-40.03 | $18,685.18 | ▼ -40.03 after sell → book $10,352.79; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 32 | $52.49 | $2.09 | $+60.40 | $17,003.42 | ▲ +60.40 after sell → book $10,350.71; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMO` | 9 | $172.76 | $2.02 | $+16.15 | $15,446.56 | ▲ +16.15 after sell → book $10,348.69; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 10 | $252.24 | $2.12 | — | $17,966.84 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $2587.17 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 85 | $30.18 | $2.35 | — | $20,529.79 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; leftover $2587.17 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,529.79 | ▲ close $10,717.33 vs 09:30 $10,355.17 (session +373.11) | 16:00 close · cash $20,529.79 · equity $10,717.33 vs 09:30 $10,355.17 (+362.16; session marks +373.11) · 6 name(s) marked open→close (per-name table). BE×6 09:30 $215.71 → close $210.77 +29.61; ABCL×106 09:30 $12.30 → close $11.35 +101.23; AQST×256 09:30 $5.11 → close $5.02 +23.04; NEM×9 09:30 $132.35 → close $127.98 +39.33; SIMO×10 09:30 $252.24 → close $245.81 +64.30; FIG×85 09:30 $30.18 → close $28.82 +115.60 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,529.79 | ▲ 09:30 equity $10,862.76 vs yday $10,717.33 (+145.43) | 09:30 open · cash $20,529.79 (unchanged overnight, no fees) · equity $10,862.76 vs prior close $10,717.33 (+145.43) · 6 name(s) re-marked at the open (per-name table). BE×6 yday $210.77 → 09:30 $208.88 +11.34; ABCL×106 yday $11.35 → 09:30 $11.10 +26.50; AQST×256 yday $5.02 → 09:30 $4.97 +11.52; NEM×9 yday $127.98 → 09:30 $127.45 +4.77; SIMO×10 yday $245.81 → 09:30 $247.05 -12.40; FIG×85 yday $28.82 → 09:30 $27.60 +103.70 | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 6 | $208.88 | $2.01 | $+26.29 | $19,274.50 | ▲ +26.29 after sell → book $10,860.75; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 106 | $11.10 | $2.31 | $+114.04 | $18,095.59 | ▲ +114.04 after sell → book $10,858.44; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 256 | $4.97 | $3.30 | $+20.19 | $16,818.69 | ▲ +20.19 after sell → book $10,855.14; vs 09:30 mark -3.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 9 | $127.45 | $2.02 | $+42.62 | $15,669.62 | ▲ +42.62 after sell → book $10,853.12; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,669.62 | ▲ close $10,864.57 vs 09:30 $10,862.76 (session +11.45) | 16:00 close · cash $15,669.62 · equity $10,864.57 vs 09:30 $10,862.76 (+1.81; session marks +11.45) · 2 name(s) marked open→close (per-name table). SIMO×10 09:30 $247.05 → close $246.84 +2.10; FIG×85 09:30 $27.60 → close $27.49 +9.35 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,669.62 | ▲ 09:30 equity $10,968.62 vs yday $10,864.57 (+104.05) | 09:30 open · cash $15,669.62 (unchanged overnight, no fees) · equity $10,968.62 vs prior close $10,864.57 (+104.05) · 2 name(s) re-marked at the open (per-name table). SIMO×10 yday $246.84 → 09:30 $240.09 +67.50; FIG×85 yday $27.49 → 09:30 $27.06 +36.55 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,669.62 | ▲ close $10,984.12 vs 09:30 $10,968.62 (session +15.50) | 16:00 close · cash $15,669.62 · equity $10,984.12 vs 09:30 $10,968.62 (+15.50; session marks +15.50) · 2 name(s) marked open→close (per-name table). SIMO×10 09:30 $240.09 → close $237.35 +27.40; FIG×85 09:30 $27.06 → close $27.20 -11.90 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,669.62 | ▲ 09:30 equity $11,036.22 vs yday $10,984.12 (+52.10) | 09:30 open · cash $15,669.62 (unchanged overnight, no fees) · equity $11,036.22 vs prior close $10,984.12 (+52.10) · 2 name(s) re-marked at the open (per-name table). SIMO×10 yday $237.35 → 09:30 $235.71 +16.40; FIG×85 yday $27.20 → 09:30 $26.78 +35.70 | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 10 | $235.71 | $2.02 | $+161.16 | $13,310.50 | ▲ +161.16 after sell → book $11,034.20; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 85 | $26.78 | $2.25 | $+284.41 | $11,031.96 | ▲ +284.41 after sell → book $11,031.96; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,031.96 | ▲ close $11,031.96 vs 09:30 $11,036.22 (session +0.00) | 16:00 close · cash $11,031.96 · no lots left · equity $11,031.96. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,031.96 | ▲ 09:30 equity $11,031.96 vs yday $11,031.96 (-0.00) | 09:30 open · cash $11,031.96 · no holdings · equity $11,031.96 vs prior close $11,031.96 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 185 | $14.85 | $2.67 | — | $13,776.54 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $2757.99 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1612 | $1.71 | $21.16 | — | $16,511.90 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; leftover $2757.99 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,511.90 | ▲ close $11,180.43 vs 09:30 $11,031.96 (session +172.30) | 16:00 close · cash $16,511.90 · equity $11,180.43 vs 09:30 $11,031.96 (+148.47; session marks +172.30) · 2 name(s) marked open→close (per-name table). SLN×185 09:30 $14.85 → close $14.79 +11.10; OPK×1612 09:30 $1.71 → close $1.61 +161.20 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,511.90 | ▲ 09:30 equity $11,242.27 vs yday $11,180.43 (+61.84) | 09:30 open · cash $16,511.90 (unchanged overnight, no fees) · equity $11,242.27 vs prior close $11,180.43 (+61.84) · 2 name(s) re-marked at the open (per-name table). SLN×185 yday $14.79 → 09:30 $14.63 +29.60; OPK×1612 yday $1.61 → 09:30 $1.59 +32.24 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 601 | $4.67 | $7.95 | — | $19,310.61 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; leftover $2810.57 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 36 | $76.55 | $2.21 | — | $22,064.21 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; leftover $2810.57 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,064.21 | ▼ close $11,141.27 vs 09:30 $11,242.27 (session -90.84) | 16:00 close · cash $22,064.21 · equity $11,141.27 vs 09:30 $11,242.27 (-101.00; session marks -90.84) · 4 name(s) marked open→close (per-name table). SLN×185 09:30 $14.63 → close $14.59 +7.40; OPK×1612 09:30 $1.59 → close $1.64 -80.60; GSM×601 09:30 $4.67 → close $4.67 -0.00; PIPR×36 09:30 $76.55 → close $77.04 -17.64 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,064.21 | ▲ 09:30 equity $11,188.46 vs yday $11,141.27 (+47.19) | 09:30 open · cash $22,064.21 (unchanged overnight, no fees) · equity $11,188.46 vs prior close $11,141.27 (+47.19) · 4 name(s) re-marked at the open (per-name table). SLN×185 yday $14.59 → 09:30 $14.24 +64.75; OPK×1612 yday $1.64 → 09:30 $1.63 +16.12; GSM×601 yday $4.67 → 09:30 $4.75 -48.08; PIPR×36 yday $77.04 → 09:30 $76.64 +14.40 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,064.21 | ▲ close $11,467.72 vs 09:30 $11,188.46 (session +279.26) | 16:00 close · cash $22,064.21 · equity $11,467.72 vs 09:30 $11,188.46 (+279.26; session marks +279.26) · 4 name(s) marked open→close (per-name table). SLN×185 09:30 $14.24 → close $13.69 +101.75; OPK×1612 09:30 $1.63 → close $1.59 +64.48; GSM×601 09:30 $4.75 → close $4.52 +138.23; PIPR×36 09:30 $76.64 → close $77.34 -25.20 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,064.21 | ▲ 09:30 equity $11,504.09 vs yday $11,467.72 (+36.37) | 09:30 open · cash $22,064.21 (unchanged overnight, no fees) · equity $11,504.09 vs prior close $11,467.72 (+36.37) · 4 name(s) re-marked at the open (per-name table). SLN×185 yday $13.69 → 09:30 $13.60 +16.65; OPK×1612 yday $1.59 → 09:30 $1.58 +16.12; GSM×601 yday $4.52 → 09:30 $4.52 -0.00; PIPR×36 yday $77.34 → 09:30 $77.24 +3.60 | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 185 | $13.60 | $2.54 | $+226.03 | $19,545.66 | ▲ +226.03 after sell → book $11,501.54; vs 09:30 mark -2.55 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1612 | $1.58 | $20.79 | $+167.61 | $16,977.91 | ▲ +167.61 after sell → book $11,480.75; vs 09:30 mark -20.79 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,977.91 | ▲ close $11,508.86 vs 09:30 $11,504.09 (session +28.11) | 16:00 close · cash $16,977.91 · equity $11,508.86 vs 09:30 $11,504.09 (+4.77; session marks +28.11) · 2 name(s) marked open→close (per-name table). GSM×601 09:30 $4.52 → close $4.49 +18.03; PIPR×36 09:30 $77.24 → close $76.96 +10.08 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,977.91 | ▲ 09:30 equity $11,508.86 vs yday $11,508.86 (-0.00) | 09:30 open · cash $16,977.91 (unchanged overnight, no fees) · equity $11,508.86 vs prior close $11,508.86 (-0.00) · 2 name(s) re-marked at the open (per-name table). GSM×601 yday $4.49 → 09:30 $4.49 -0.00; PIPR×36 yday $76.96 → 09:30 $76.96 -0.00 | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 601 | $4.49 | $7.75 | $+92.48 | $14,271.67 | ▲ +92.48 after sell → book $11,501.11; vs 09:30 mark -7.75 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 36 | $76.96 | $2.10 | $-19.06 | $11,499.01 | ▼ -19.06 after sell → book $11,499.01; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,499.01 | ▲ close $11,499.01 vs 09:30 $11,508.86 (session +0.00) | 16:00 close · cash $11,499.01 · no lots left · equity $11,499.01. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `EU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LUNR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `OWL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `EU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LUNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OWL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VERI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ZNTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `APMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `HIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `RNW` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `VERI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ZNTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `APMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `HIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WYFI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TOYO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TEAM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WMT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AQST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WYFI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TOYO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TEAM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WMT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AQST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `QTRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `SSRM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARIS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EU` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `QTRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARIS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `NOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ARE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ARE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `AQST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `NEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `BE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `AQST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `NEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `PIPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `GSM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `PIPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
