# Factor mine action — `short_news_r_macd_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · short news🔴 ∩ prior MACD histogram > 0

Cash book **+9.65%** ($10,965) · signal-only (no cash/fees) was +28.68%. Starts YES **20/29**. Fills 73 · skips 81 · realized $+1135.66.

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
- Must-have: prior MACD histogram is above zero (momentum still up).

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
- **Gate** `news=bad,macd_up=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $21,922.71.

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
| 2026-08-17 | `VERI` | 1437 | — | $1.15 | +0.00 | $1.08 | +93.40 | +93.40 | -0.00 | +93.40 |
| 2026-08-17 | `HIVE` | 549 | — | $3.01 | +0.00 | $3.07 | -32.94 | -32.94 | -0.00 | -32.94 |
| 2026-08-17 | `RNW` | 243 | — | $6.80 | +0.00 | $6.82 | -4.86 | -4.86 | -0.00 | -4.86 |
| 2026-08-18 | `EU` | 1412 | $1.13 | $1.13 | +0.00 | $1.07 | +84.72 | +84.72 | +70.60 | +155.32 |
| 2026-08-18 | `LUNR` | 86 | $20.38 | $19.31 | +92.02 | $19.31 | +0.00 | +92.02 | -12.04 | -12.04 |
| 2026-08-18 | `OWL` | 131 | $11.66 | $11.54 | +15.72 | $11.59 | -6.55 | +9.17 | +151.31 | +144.76 |
| 2026-08-18 | `VERI` | 1437 | $1.08 | $1.05 | +50.29 | $0.99 | +79.04 | +129.33 | +143.70 | +222.73 |
| 2026-08-18 | `HIVE` | 549 | $3.07 | $2.96 | +60.39 | $2.78 | +98.82 | +159.21 | +27.45 | +126.27 |
| 2026-08-18 | `RNW` | 243 | $6.82 | $6.83 | -2.43 | $6.82 | +2.43 | +0.00 | -7.29 | -4.86 |
| 2026-08-19 | `EU` | 1412 | $1.07 | $1.07 | +0.00 | — | +0.00 | +0.00 | +155.32 | — |
| 2026-08-19 | `LUNR` | 86 | $19.31 | $18.98 | +28.38 | $18.52 | +39.56 | +67.94 | +16.34 | +55.90 |
| 2026-08-19 | `OWL` | 131 | $11.59 | $11.75 | -20.96 | — | +0.00 | -20.96 | +123.80 | — |
| 2026-08-19 | `VERI` | 1437 | $0.99 | $1.00 | -7.19 | $0.97 | +48.86 | +41.67 | +215.55 | +264.41 |
| 2026-08-19 | `HIVE` | 549 | $2.78 | $2.78 | +0.00 | $2.82 | -21.96 | -21.96 | +126.27 | +104.31 |
| 2026-08-19 | `RNW` | 243 | $6.82 | $6.84 | -4.86 | $6.80 | +9.72 | +4.86 | -9.72 | -0.00 |
| 2026-08-20 | `LUNR` | 86 | $18.52 | $18.13 | +33.54 | — | +0.00 | +33.54 | +89.44 | — |
| 2026-08-20 | `VERI` | 1437 | $0.97 | $0.96 | +4.31 | — | +0.00 | +4.31 | +268.72 | — |
| 2026-08-20 | `HIVE` | 549 | $2.82 | $2.95 | -71.37 | — | +0.00 | -71.37 | +32.94 | — |
| 2026-08-20 | `RNW` | 243 | $6.80 | $6.81 | -2.43 | — | +0.00 | -2.43 | -2.43 | — |
| 2026-08-20 | `AEM` | 3 | — | $204.45 | +0.00 | $212.04 | -22.77 | -22.77 | -0.00 | -22.77 |
| 2026-08-20 | `WYFI` | 30 | — | $21.40 | +0.00 | $21.16 | +7.20 | +7.20 | -0.00 | +7.20 |
| 2026-08-20 | `TOYO` | 149 | — | $4.43 | +0.00 | $4.51 | -12.66 | -12.66 | -0.00 | -12.66 |
| 2026-08-20 | `ABCL` | 55 | — | $11.81 | +0.00 | $11.57 | +13.47 | +13.47 | -0.00 | +13.47 |
| 2026-08-20 | `TEAM` | 3 | — | $173.90 | +0.00 | $174.91 | -3.03 | -3.03 | -0.00 | -3.03 |
| 2026-08-20 | `AAP` | 14 | — | $46.85 | +0.00 | $42.39 | +62.44 | +62.44 | -0.00 | +62.44 |
| 2026-08-20 | `WMT` | 6 | — | $106.38 | +0.00 | $103.84 | +15.24 | +15.24 | -0.00 | +15.24 |
| 2026-08-20 | `AQST` | 143 | — | $4.61 | +0.00 | $4.50 | +16.45 | +16.45 | -0.00 | +16.45 |
| 2026-08-21 | `AEM` | 3 | $212.04 | $216.30 | -12.78 | $216.06 | +0.72 | -12.06 | -35.55 | -34.83 |
| 2026-08-21 | `WYFI` | 30 | $21.16 | $21.54 | -11.40 | $20.72 | +24.60 | +13.20 | -4.20 | +20.40 |
| 2026-08-21 | `TOYO` | 149 | $4.51 | $4.68 | -24.59 | $4.82 | -20.86 | -45.45 | -37.25 | -58.11 |
| 2026-08-21 | `ABCL` | 55 | $11.57 | $11.57 | +0.00 | $11.32 | +13.75 | +13.75 | +13.47 | +27.22 |
| 2026-08-21 | `TEAM` | 3 | $174.91 | $174.22 | +2.07 | $171.81 | +7.23 | +9.30 | -0.96 | +6.27 |
| 2026-08-21 | `AAP` | 14 | $42.39 | $42.41 | -0.28 | $42.58 | -2.38 | -2.66 | +62.16 | +59.78 |
| 2026-08-21 | `WMT` | 6 | $103.84 | $103.69 | +0.90 | $103.70 | -0.06 | +0.84 | +16.14 | +16.08 |
| 2026-08-21 | `AQST` | 143 | $4.50 | $4.54 | -6.43 | $4.66 | -17.16 | -23.59 | +10.01 | -7.15 |
| 2026-08-21 | `MRNA` | 7 | — | $133.11 | +0.00 | $145.13 | -84.14 | -84.14 | -0.00 | -84.14 |
| 2026-08-21 | `AUGO` | 11 | — | $89.10 | +0.00 | $87.26 | +20.24 | +20.24 | -0.00 | +20.24 |
| 2026-08-21 | `SSRM` | 27 | — | $38.40 | +0.00 | $37.77 | +17.01 | +17.01 | -0.00 | +17.01 |
| 2026-08-21 | `ARIS` | 50 | — | $20.90 | +0.00 | $20.86 | +2.00 | +2.00 | -0.00 | +2.00 |
| 2026-08-21 | `NOG` | 39 | — | $27.00 | +0.00 | $27.34 | -13.26 | -13.26 | -0.00 | -13.26 |
| 2026-08-24 | `AEM` | 3 | $216.06 | $217.03 | -2.91 | $217.89 | -2.58 | -5.49 | -37.74 | -40.32 |
| 2026-08-24 | `WYFI` | 30 | $20.72 | $20.01 | +21.30 | $20.78 | -23.10 | -1.80 | +41.70 | +18.60 |
| 2026-08-24 | `TOYO` | 149 | $4.82 | $4.58 | +35.76 | $4.38 | +29.80 | +65.56 | -22.35 | +7.45 |
| 2026-08-24 | `ABCL` | 55 | $11.32 | $10.97 | +19.25 | $10.61 | +19.80 | +39.05 | +46.47 | +66.28 |
| 2026-08-24 | `TEAM` | 3 | $171.81 | $169.30 | +7.53 | $171.33 | -6.09 | +1.44 | +13.80 | +7.71 |
| 2026-08-24 | `AAP` | 14 | $42.58 | $43.05 | -6.58 | $43.63 | -8.12 | -14.70 | +53.20 | +45.08 |
| 2026-08-24 | `WMT` | 6 | $103.70 | $104.14 | -2.64 | $106.49 | -14.10 | -16.74 | +13.44 | -0.66 |
| 2026-08-24 | `AQST` | 143 | $4.66 | $4.67 | -1.43 | $4.80 | -18.59 | -20.02 | -8.58 | -27.17 |
| 2026-08-24 | `MRNA` | 7 | $145.13 | $142.70 | +17.01 | $138.89 | +26.67 | +43.68 | -67.13 | -40.46 |
| 2026-08-24 | `AUGO` | 11 | $87.26 | $88.60 | -14.74 | $87.37 | +13.53 | -1.21 | +5.50 | +19.03 |
| 2026-08-24 | `SSRM` | 27 | $37.77 | $38.32 | -14.85 | $38.61 | -7.83 | -22.68 | +2.16 | -5.67 |
| 2026-08-24 | `ARIS` | 50 | $20.86 | $20.98 | -6.00 | $20.81 | +8.50 | +2.50 | -4.00 | +4.50 |
| 2026-08-24 | `NOG` | 39 | $27.34 | $27.12 | +8.58 | $26.84 | +10.92 | +19.50 | -4.68 | +6.24 |
| 2026-08-25 | `AEM` | 3 | $217.89 | $212.00 | +17.67 | — | +0.00 | +17.67 | -22.65 | — |
| 2026-08-25 | `WYFI` | 30 | $20.78 | $20.90 | -3.60 | — | +0.00 | -3.60 | +15.00 | — |
| 2026-08-25 | `TOYO` | 149 | $4.38 | $4.42 | -5.96 | — | +0.00 | -5.96 | +1.49 | — |
| 2026-08-25 | `ABCL` | 55 | $10.61 | $11.00 | -21.45 | — | +0.00 | -21.45 | +44.82 | — |
| 2026-08-25 | `TEAM` | 3 | $171.33 | $170.64 | +2.07 | — | +0.00 | +2.07 | +9.78 | — |
| 2026-08-25 | `AAP` | 14 | $43.63 | $43.63 | +0.00 | — | +0.00 | +0.00 | +45.08 | — |
| 2026-08-25 | `WMT` | 6 | $106.49 | $105.58 | +5.46 | — | +0.00 | +5.46 | +4.80 | — |
| 2026-08-25 | `AQST` | 143 | $4.80 | $4.77 | +4.29 | — | +0.00 | +4.29 | -22.88 | — |
| 2026-08-25 | `MRNA` | 7 | $138.89 | $143.50 | -32.27 | $158.83 | -107.31 | -139.58 | -72.73 | -180.04 |
| 2026-08-25 | `AUGO` | 11 | $87.37 | $85.78 | +17.49 | $90.47 | -51.59 | -34.10 | +36.52 | -15.07 |
| 2026-08-25 | `SSRM` | 27 | $38.61 | $37.75 | +23.22 | $39.21 | -39.42 | -16.20 | +17.55 | -21.87 |
| 2026-08-25 | `ARIS` | 50 | $20.81 | $20.45 | +18.00 | $21.18 | -36.50 | -18.50 | +22.50 | -14.00 |
| 2026-08-25 | `NOG` | 39 | $26.84 | $26.06 | +30.42 | $26.42 | -14.04 | +16.38 | +36.66 | +22.62 |
| 2026-08-25 | `AVAH` | 130 | — | $13.62 | +0.00 | $13.59 | +4.55 | +4.55 | -0.00 | +4.55 |
| 2026-08-25 | `ARE` | 32 | — | $54.51 | +0.00 | $52.90 | +51.52 | +51.52 | -0.00 | +51.52 |
| 2026-08-25 | `INTU` | 4 | — | $364.35 | +0.00 | $357.46 | +27.56 | +27.56 | -0.00 | +27.56 |
| 2026-08-26 | `MRNA` | 7 | $158.83 | $154.20 | +32.41 | — | +0.00 | +32.41 | -147.63 | — |
| 2026-08-26 | `AUGO` | 11 | $90.47 | $88.24 | +24.53 | — | +0.00 | +24.53 | +9.46 | — |
| 2026-08-26 | `SSRM` | 27 | $39.21 | $38.41 | +21.60 | — | +0.00 | +21.60 | -0.27 | — |
| 2026-08-26 | `ARIS` | 50 | $21.18 | $20.50 | +34.00 | — | +0.00 | +34.00 | +20.00 | — |
| 2026-08-26 | `NOG` | 39 | $26.42 | $26.00 | +16.38 | — | +0.00 | +16.38 | +39.00 | — |
| 2026-08-26 | `AVAH` | 130 | $13.59 | $13.65 | -7.80 | $13.62 | +3.90 | -3.90 | -3.25 | +0.65 |
| 2026-08-26 | `ARE` | 32 | $52.90 | $52.77 | +4.16 | $52.97 | -6.40 | -2.24 | +55.68 | +49.28 |
| 2026-08-26 | `INTU` | 4 | $357.46 | $323.47 | +135.96 | $345.88 | -89.64 | +46.32 | +163.52 | +73.88 |
| 2026-08-26 | `BE` | 5 | — | $213.94 | +0.00 | $218.21 | -21.35 | -21.35 | -0.00 | -21.35 |
| 2026-08-26 | `ABCL` | 87 | — | $12.22 | +0.00 | $12.24 | -1.74 | -1.74 | -0.00 | -1.74 |
| 2026-08-26 | `AQST` | 210 | — | $5.08 | +0.00 | $5.39 | -65.10 | -65.10 | -0.00 | -65.10 |
| 2026-08-26 | `NEM` | 8 | — | $132.64 | +0.00 | $131.60 | +8.32 | +8.32 | -0.00 | +8.32 |
| 2026-08-26 | `CRM` | 5 | — | $199.94 | +0.00 | $205.62 | -28.40 | -28.40 | -0.00 | -28.40 |
| 2026-08-27 | `AVAH` | 130 | $13.62 | $13.62 | +0.00 | $13.82 | -26.00 | -26.00 | +0.65 | -25.35 |
| 2026-08-27 | `ARE` | 32 | $52.97 | $52.45 | +16.64 | $52.28 | +5.44 | +22.08 | +65.92 | +71.36 |
| 2026-08-27 | `INTU` | 4 | $345.88 | $353.54 | -30.64 | $348.00 | +22.16 | -8.48 | +43.24 | +65.40 |
| 2026-08-27 | `BE` | 5 | $218.21 | $227.10 | -44.45 | $217.83 | +46.35 | +1.90 | -65.80 | -19.45 |
| 2026-08-27 | `ABCL` | 87 | $12.24 | $12.25 | -0.87 | $12.40 | -13.05 | -13.92 | -2.61 | -15.66 |
| 2026-08-27 | `AQST` | 210 | $5.39 | $5.39 | +0.00 | $5.16 | +48.30 | +48.30 | -65.10 | -16.80 |
| 2026-08-27 | `NEM` | 8 | $131.60 | $131.02 | +4.64 | $132.29 | -10.16 | -5.52 | +12.96 | +2.80 |
| 2026-08-27 | `CRM` | 5 | $205.62 | $230.05 | -122.15 | $252.05 | -110.00 | -232.15 | -150.55 | -260.55 |
| 2026-08-28 | `AVAH` | 130 | $13.82 | $13.90 | -10.40 | — | +0.00 | -10.40 | -35.75 | — |
| 2026-08-28 | `ARE` | 32 | $52.28 | $52.49 | -6.72 | — | +0.00 | -6.72 | +64.64 | — |
| 2026-08-28 | `INTU` | 4 | $348.00 | $347.82 | +0.72 | — | +0.00 | +0.72 | +66.12 | — |
| 2026-08-28 | `BE` | 5 | $217.83 | $215.71 | +10.62 | $210.77 | +24.68 | +35.30 | -8.83 | +15.85 |
| 2026-08-28 | `ABCL` | 87 | $12.40 | $12.30 | +8.27 | $11.35 | +83.09 | +91.36 | -7.39 | +75.69 |
| 2026-08-28 | `AQST` | 210 | $5.16 | $5.11 | +10.50 | $5.02 | +18.90 | +29.40 | -6.30 | +12.60 |
| 2026-08-28 | `NEM` | 8 | $132.29 | $132.35 | -0.48 | $127.98 | +34.96 | +34.48 | +2.32 | +37.28 |
| 2026-08-28 | `CRM` | 5 | $252.05 | $250.47 | +7.90 | $256.00 | -27.65 | -19.75 | -252.65 | -280.30 |
| 2026-08-28 | `SIMO` | 10 | — | $252.24 | +0.00 | $245.81 | +64.30 | +64.30 | -0.00 | +64.30 |
| 2026-08-28 | `FIG` | 85 | — | $30.18 | +0.00 | $28.82 | +115.60 | +115.60 | -0.00 | +115.60 |
| 2026-08-31 | `BE` | 5 | $210.77 | $208.88 | +9.45 | — | +0.00 | +9.45 | +25.30 | — |
| 2026-08-31 | `ABCL` | 87 | $11.35 | $11.10 | +21.75 | — | +0.00 | +21.75 | +97.44 | — |
| 2026-08-31 | `AQST` | 210 | $5.02 | $4.97 | +9.45 | — | +0.00 | +9.45 | +22.05 | — |
| 2026-08-31 | `NEM` | 8 | $127.98 | $127.45 | +4.24 | — | +0.00 | +4.24 | +41.52 | — |
| 2026-08-31 | `CRM` | 5 | $256.00 | $254.39 | +8.05 | — | +0.00 | +8.05 | -272.25 | — |
| 2026-08-31 | `SIMO` | 10 | $245.81 | $247.05 | -12.40 | $246.84 | +2.10 | -10.30 | +51.90 | +54.00 |
| 2026-08-31 | `FIG` | 85 | $28.82 | $27.60 | +103.70 | $27.49 | +9.35 | +113.05 | +219.30 | +228.65 |
| 2026-09-01 | `SIMO` | 10 | $246.84 | $240.09 | +67.50 | $237.35 | +27.40 | +94.90 | +121.50 | +148.90 |
| 2026-09-01 | `FIG` | 85 | $27.49 | $27.06 | +36.55 | $27.20 | -11.90 | +24.65 | +265.20 | +253.30 |
| 2026-09-02 | `SIMO` | 10 | $237.35 | $235.71 | +16.40 | — | +0.00 | +16.40 | +165.30 | — |
| 2026-09-02 | `FIG` | 85 | $27.20 | $26.78 | +35.70 | — | +0.00 | +35.70 | +289.00 | — |
| 2026-09-03 | `OPK` | 3194 | — | $1.71 | +0.00 | $1.61 | +319.40 | +319.40 | -0.00 | +319.40 |
| 2026-09-04 | `OPK` | 3194 | $1.61 | $1.59 | +63.88 | $1.64 | -159.70 | -95.82 | +383.28 | +223.58 |
| 2026-09-04 | `GSM` | 603 | — | $4.67 | +0.00 | $4.67 | +0.00 | +0.00 | -0.00 | -0.00 |
| 2026-09-04 | `PIPR` | 36 | — | $76.55 | +0.00 | $77.04 | -17.64 | -17.64 | -0.00 | -17.64 |
| 2026-09-08 | `OPK` | 3194 | $1.64 | $1.63 | +31.94 | $1.59 | +127.76 | +159.70 | +255.52 | +383.28 |
| 2026-09-08 | `GSM` | 603 | $4.67 | $4.75 | -48.24 | $4.52 | +138.69 | +90.45 | -48.24 | +90.45 |
| 2026-09-08 | `PIPR` | 36 | $77.04 | $76.64 | +14.40 | $77.34 | -25.20 | -10.80 | -3.24 | -28.44 |
| 2026-09-09 | `OPK` | 3194 | $1.59 | $1.58 | +31.94 | — | +0.00 | +31.94 | +415.22 | — |
| 2026-09-09 | `GSM` | 603 | $4.52 | $4.52 | +0.00 | $4.49 | +18.09 | +18.09 | +90.45 | +108.54 |
| 2026-09-09 | `PIPR` | 36 | $77.34 | $77.24 | +3.60 | $76.96 | +10.08 | +13.68 | -24.84 | -14.76 |
| 2026-09-10 | `GSM` | 603 | $4.49 | $4.36 | +78.39 | — | +0.00 | +78.39 | +186.93 | — |
| 2026-09-10 | `PIPR` | 36 | $76.96 | $76.79 | +6.12 | — | +0.00 | +6.12 | -8.64 | — |
| 2026-09-11 | `QRVO` | 25 | — | $112.83 | +0.00 | $116.65 | -95.38 | -95.38 | -0.00 | -95.38 |
| 2026-09-11 | `MYGN` | 846 | — | $3.37 | +0.00 | $3.42 | -42.30 | -42.30 | -0.00 | -42.30 |
| 2026-09-14 | `QRVO` | 25 | $116.65 | $114.11 | +63.50 | $107.98 | +153.25 | +216.75 | -31.88 | +121.37 |
| 2026-09-14 | `MYGN` | 846 | $3.42 | $3.43 | -8.46 | $3.79 | -304.56 | -313.02 | -50.76 | -355.32 |
| 2026-09-15 | `QRVO` | 25 | $107.98 | $108.40 | -10.50 | $118.06 | -241.50 | -252.00 | +110.87 | -130.63 |
| 2026-09-15 | `MYGN` | 846 | $3.79 | $3.80 | -8.46 | $3.88 | -67.68 | -76.14 | -363.78 | -431.46 |
| 2026-09-16 | `QRVO` | 25 | $118.06 | $118.18 | -3.00 | — | +0.00 | -3.00 | -133.63 | — |
| 2026-09-16 | `MYGN` | 846 | $3.88 | $3.75 | +109.98 | — | +0.00 | +109.98 | -321.48 | — |
| 2026-09-16 | `GFR` | 800 | — | $6.83 | +0.00 | $6.49 | +272.00 | +272.00 | -0.00 | +272.00 |
| 2026-09-17 | `GFR` | 800 | $6.49 | $6.48 | +8.00 | $6.66 | -144.00 | -136.00 | +280.00 | +136.00 |
| 2026-09-18 | `GFR` | 800 | $6.66 | $6.64 | +16.00 | $6.66 | -16.00 | +0.00 | +152.00 | +136.00 |
| 2026-09-21 | `GFR` | 800 | $6.66 | $6.55 | +88.00 | — | +0.00 | +88.00 | +224.00 | — |
| 2026-09-21 | `AEHL` | 337 | — | $8.26 | +0.00 | $6.92 | +451.58 | +451.58 | -0.00 | +451.58 |
| 2026-09-21 | `AMD` | 4 | — | $583.88 | +0.00 | $615.52 | -126.56 | -126.56 | -0.00 | -126.56 |
| 2026-09-22 | `AEHL` | 337 | $6.92 | $7.13 | -70.77 | $7.61 | -161.76 | -232.53 | +380.81 | +219.05 |
| 2026-09-22 | `AMD` | 4 | $615.52 | $606.57 | +35.80 | $623.77 | -68.80 | -33.00 | -90.76 | -159.56 |
| 2026-09-22 | `FIVN` | 152 | — | $37.35 | +0.00 | $37.20 | +22.80 | +22.80 | -0.00 | +22.80 |
| 2026-09-23 | `AEHL` | 337 | $7.61 | $7.62 | -3.37 | $8.22 | -203.88 | -207.25 | +215.68 | +11.80 |
| 2026-09-23 | `AMD` | 4 | $623.77 | $622.15 | +6.48 | $614.61 | +30.16 | +36.64 | -153.08 | -122.92 |
| 2026-09-23 | `FIVN` | 152 | $37.20 | $38.99 | -272.08 | $37.68 | +199.12 | -72.96 | -249.28 | -50.16 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +33.62 | EU, LUNR, OWL | — | $14,954.53 | $10,010.33 | EU×1412, LUNR×86, OWL×131 |
| 2026-08-17 | +2.25 | $14,954.53 | EU×1412, LUNR×86, OWL×131 | $9,916.79 | -93.54 | +217.64 | VERI, HIVE, RNW | — | $19,882.67 | $10,105.14 | EU×1412, LUNR×86, OWL×131, VERI×1437, HIVE×549, RNW×243 |
| 2026-08-18 | -6.20 | $19,882.67 | EU×1412, LUNR×86, OWL×131, VERI×1437, HIVE×549, RNW×243 | $10,321.13 | +215.99 | +258.46 | — | — | $19,882.67 | $10,579.59 | EU×1412, LUNR×86, OWL×131, VERI×1437, HIVE×549, RNW×243 |
| 2026-08-19 | -7.20 | $19,882.67 | EU×1412, LUNR×86, OWL×131, VERI×1437, HIVE×549, RNW×243 | $10,574.96 | -4.63 | +76.18 | — | EU, OWL | $16,811.99 | $10,630.54 | LUNR×86, VERI×1437, HIVE×549, RNW×243 |
| 2026-08-20 | +1.12 | $16,811.99 | LUNR×86, VERI×1437, HIVE×549, RNW×243 | $10,594.59 | -35.95 | +76.34 | AEM, WYFI, TOYO, ABCL, TEAM, AAP, WMT, AQST | LUNR, VERI, HIVE, RNW | $15,586.88 | $10,622.86 | AEM×3, WYFI×30, TOYO×149, ABCL×55, TEAM×3, AAP×14, WMT×6, AQST×143 |
| 2026-08-21 | +3.25 | $15,586.88 | AEM×3, WYFI×30, TOYO×149, ABCL×55, TEAM×3, AAP×14, WMT×6, AQST×143 | $10,570.35 | -52.51 | -52.31 | MRNA, AUGO, SSRM, ARIS, NOG | — | $20,622.96 | $10,507.45 | AEM×3, WYFI×30, TOYO×149, ABCL×55, TEAM×3, AAP×14, WMT×6, AQST×143, MRNA×7, AUGO×11, SSRM×27, ARIS×50, NOG×39 |
| 2026-08-24 | -5.17 | $20,622.96 | AEM×3, WYFI×30, TOYO×149, ABCL×55, TEAM×3, AAP×14, WMT×6, AQST×143, MRNA×7, AUGO×11, SSRM×27, ARIS×50, NOG×39 | $10,567.73 | +60.28 | +28.81 | — | — | $20,622.96 | $10,596.54 | AEM×3, WYFI×30, TOYO×149, ABCL×55, TEAM×3, AAP×14, WMT×6, AQST×143, MRNA×7, AUGO×11, SSRM×27, ARIS×50, NOG×39 |
| 2026-08-25 | +1.80 | $20,622.96 | AEM×3, WYFI×30, TOYO×149, ABCL×55, TEAM×3, AAP×14, WMT×6, AQST×143, MRNA×7, AUGO×11, SSRM×27, ARIS×50, NOG×39 | $10,651.88 | +55.34 | -165.23 | AVAH, ARE, INTU | AEM, WYFI, TOYO, ABCL, TEAM, AAP, WMT, AQST | $20,607.20 | $10,462.83 | MRNA×7, AUGO×11, SSRM×27, ARIS×50, NOG×39, AVAH×130, ARE×32, INTU×4 |
| 2026-08-26 | +2.02 | $20,607.20 | MRNA×7, AUGO×11, SSRM×27, ARIS×50, NOG×39, AVAH×130, ARE×32, INTU×4 | $10,724.07 | +261.24 | -200.41 | BE, ABCL, AQST, NEM, CRM | MRNA, AUGO, SSRM, ARIS, NOG | $20,719.94 | $10,502.05 | AVAH×130, ARE×32, INTU×4, BE×5, ABCL×87, AQST×210, NEM×8, CRM×5 |
| 2026-08-27 | — | $20,719.94 | AVAH×130, ARE×32, INTU×4, BE×5, ABCL×87, AQST×210, NEM×8, CRM×5 | $10,325.22 | -176.83 | -36.96 | — | — | $20,719.94 | $10,288.26 | AVAH×130, ARE×32, INTU×4, BE×5, ABCL×87, AQST×210, NEM×8, CRM×5 |
| 2026-08-28 | +0.75 | $20,719.94 | AVAH×130, ARE×32, INTU×4, BE×5, ABCL×87, AQST×210, NEM×8, CRM×5 | $10,308.67 | +20.41 | +313.88 | SIMO, FIG | AVAH, ARE, INTU | $20,918.75 | $10,611.61 | BE×5, ABCL×87, AQST×210, NEM×8, CRM×5, SIMO×10, FIG×85 |
| 2026-08-31 | -5.85 | $20,918.75 | BE×5, ABCL×87, AQST×210, NEM×8, CRM×5, SIMO×10, FIG×85 | $10,755.85 | +144.24 | +11.45 | — | BE, ABCL, AQST, NEM, CRM | $15,561.36 | $10,756.31 | SIMO×10, FIG×85 |
| 2026-09-01 | -6.30 | $15,561.36 | SIMO×10, FIG×85 | $10,860.36 | +104.05 | +15.50 | — | — | $15,561.36 | $10,875.86 | SIMO×10, FIG×85 |
| 2026-09-02 | -3.83 | $15,561.36 | SIMO×10, FIG×85 | $10,927.96 | +52.10 | +0.00 | — | SIMO, FIG | $10,923.70 | $10,923.70 | — |
| 2026-09-03 | -0.90 | $10,923.70 | — | $10,923.70 | -0.00 | +319.40 | OPK | — | $16,343.51 | $11,201.17 | OPK×3194 |
| 2026-09-04 | +2.25 | $16,343.51 | OPK×3194 | $11,265.05 | +63.88 | -177.34 | GSM, PIPR | — | $21,905.14 | $11,077.53 | OPK×3194, GSM×603, PIPR×36 |
| 2026-09-08 | -11.47 | $21,905.14 | OPK×3194, GSM×603, PIPR×36 | $11,075.63 | -1.90 | +241.25 | — | — | $21,905.14 | $11,316.88 | OPK×3194, GSM×603, PIPR×36 |
| 2026-09-09 | -13.95 | $21,905.14 | OPK×3194, GSM×603, PIPR×36 | $11,352.42 | +35.54 | +28.17 | — | OPK | $16,817.42 | $11,339.39 | GSM×603, PIPR×36 |
| 2026-09-10 | -13.28 | $16,817.42 | GSM×603, PIPR×36 | $11,423.90 | +84.51 | +0.00 | — | GSM, PIPR | $11,414.02 | $11,414.02 | — |
| 2026-09-11 | +0.50 | $11,414.02 | — | $11,414.02 | -0.00 | -137.68 | QRVO, MYGN | — | $17,072.58 | $11,263.01 | QRVO×25, MYGN×846 |
| 2026-09-14 | -11.00 | $17,072.58 | QRVO×25, MYGN×846 | $11,318.05 | +55.04 | -151.31 | — | — | $17,072.58 | $11,166.74 | QRVO×25, MYGN×846 |
| 2026-09-15 | -3.84 | $17,072.58 | QRVO×25, MYGN×846 | $11,147.78 | -18.96 | -309.18 | — | — | $17,072.58 | $10,838.60 | QRVO×25, MYGN×846 |
| 2026-09-16 | +5.30 | $17,072.58 | QRVO×25, MYGN×846 | $10,945.58 | +106.98 | +272.00 | GFR | QRVO, MYGN | $16,385.96 | $11,193.96 | GFR×800 |
| 2026-09-17 | +7.38 | $16,385.96 | GFR×800 | $11,201.96 | +8.00 | -144.00 | — | — | $16,385.96 | $11,057.96 | GFR×800 |
| 2026-09-18 | +4.86 | $16,385.96 | GFR×800 | $11,073.96 | +16.00 | -16.00 | — | — | $16,385.96 | $11,057.96 | GFR×800 |
| 2026-09-21 | +12.87 | $16,385.96 | GFR×800 | $11,145.96 | +88.00 | +325.02 | AEHL, AMD | GFR | $16,248.18 | $11,454.06 | AEHL×337, AMD×4 |
| 2026-09-22 | -0.50 | $16,248.18 | AEHL×337, AMD×4 | $11,419.09 | -34.97 | -207.76 | FIVN | — | $21,922.71 | $11,208.66 | AEHL×337, AMD×4, FIVN×152 |
| 2026-09-23 | +2.29 | $21,922.71 | AEHL×337, AMD×4, FIVN×152 | $10,939.69 | -268.97 | +25.40 | — | — | $21,922.71 | $10,965.09 | AEHL×337, AMD×4, FIVN×152 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 1412 | $1.18 | $18.51 | — | $11,647.65 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 86 | $19.17 | $2.32 | — | $13,293.95 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 131 | $12.70 | $2.46 | — | $14,954.53 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+12.6; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,954.53 | ▲ close $10,010.33 vs 09:30 $10,000.00 (session +33.62) | 16:00 close · cash $14,954.53 · equity $10,010.33 vs 09:30 $10,000.00 (+10.33; session marks +33.62) · 3 name(s) marked open→close (per-name table). EU×1412 09:30 $1.18 → close $1.21 -42.36; LUNR×86 09:30 $19.17 → close $19.01 +13.76; OWL×131 09:30 $12.70 → close $12.22 +62.22 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,954.53 | ▼ 09:30 equity $9,916.79 vs yday $10,010.33 (-93.54) | 09:30 open · cash $14,954.53 (unchanged overnight, no fees) · equity $9,916.79 vs prior close $10,010.33 (-93.54) · 3 name(s) re-marked at the open (per-name table). EU×1412 yday $1.21 → 09:30 $1.21 -0.00; LUNR×86 yday $19.01 → 09:30 $20.25 -106.64; OWL×131 yday $12.22 → 09:30 $12.12 +13.10 | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 1437 | $1.15 | $18.83 | — | $16,588.25 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_mover; ⚪; ret5=-12.2; leftover $1652.80 | join🟡 sector🟢 gen🟢 news🔴 judge🟢 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 549 | $3.01 | $7.23 | — | $18,233.51 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list earn_react; ⚪; ret5=-5.3; leftover $1652.80 | join🟢 sector🟢 gen🟢 news🔴 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 243 | $6.80 | $3.23 | — | $19,882.67 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list overnight; ⚪; ret5=+10.4; leftover $1652.80 | join🟡 sector🟢 gen🟢 news🔴 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,882.67 | ▲ close $10,105.14 vs 09:30 $9,916.79 (session +217.64) | 16:00 close · cash $19,882.67 · equity $10,105.14 vs 09:30 $9,916.79 (+188.35; session marks +217.64) · 6 name(s) marked open→close (per-name table). EU×1412 09:30 $1.21 → close $1.13 +112.96; LUNR×86 09:30 $20.25 → close $20.38 -11.18; OWL×131 09:30 $12.12 → close $11.66 +60.26; VERI×1437 09:30 $1.15 → close $1.08 +93.40; HIVE×549 09:30 $3.01 → close $3.07 -32.94; RNW×243 09:30 $6.80 → close $6.82 -4.86 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,882.67 | ▲ 09:30 equity $10,321.13 vs yday $10,105.14 (+215.99) | 09:30 open · cash $19,882.67 (unchanged overnight, no fees) · equity $10,321.13 vs prior close $10,105.14 (+215.99) · 6 name(s) re-marked at the open (per-name table). EU×1412 yday $1.13 → 09:30 $1.13 -0.00; LUNR×86 yday $20.38 → 09:30 $19.31 +92.02; OWL×131 yday $11.66 → 09:30 $11.54 +15.72; VERI×1437 yday $1.08 → 09:30 $1.05 +50.29; HIVE×549 yday $3.07 → 09:30 $2.96 +60.39; RNW×243 yday $6.82 → 09:30 $6.83 -2.43 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,882.67 | ▲ close $10,579.59 vs 09:30 $10,321.13 (session +258.46) | 16:00 close · cash $19,882.67 · equity $10,579.59 vs 09:30 $10,321.13 (+258.46; session marks +258.46) · 6 name(s) marked open→close (per-name table). EU×1412 09:30 $1.13 → close $1.07 +84.72; LUNR×86 09:30 $19.31 → close $19.31 -0.00; OWL×131 09:30 $11.54 → close $11.59 -6.55; VERI×1437 09:30 $1.05 → close $0.99 +79.04; HIVE×549 09:30 $2.96 → close $2.78 +98.82; RNW×243 09:30 $6.83 → close $6.82 +2.43 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,882.67 | ▼ 09:30 equity $10,574.96 vs yday $10,579.59 (-4.63) | 09:30 open · cash $19,882.67 (unchanged overnight, no fees) · equity $10,574.96 vs prior close $10,579.59 (-4.63) · 6 name(s) re-marked at the open (per-name table). EU×1412 yday $1.07 → 09:30 $1.07 -0.00; LUNR×86 yday $19.31 → 09:30 $18.98 +28.38; OWL×131 yday $11.59 → 09:30 $11.75 -20.96; VERI×1437 yday $0.99 → 09:30 $1.00 -7.19; HIVE×549 yday $2.78 → 09:30 $2.78 -0.00; RNW×243 yday $6.82 → 09:30 $6.84 -4.86 | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 1412 | $1.07 | $18.21 | $+118.60 | $18,353.62 | ▲ +118.60 after sell → book $10,556.75; vs 09:30 mark -18.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 131 | $11.75 | $2.38 | $+118.95 | $16,811.99 | ▲ +118.95 after sell → book $10,554.37; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,811.99 | ▲ close $10,630.54 vs 09:30 $10,574.96 (session +76.18) | 16:00 close · cash $16,811.99 · equity $10,630.54 vs 09:30 $10,574.96 (+55.58; session marks +76.18) · 4 name(s) marked open→close (per-name table). LUNR×86 09:30 $18.98 → close $18.52 +39.56; VERI×1437 09:30 $1.00 → close $0.97 +48.86; HIVE×549 09:30 $2.78 → close $2.82 -21.96; RNW×243 09:30 $6.84 → close $6.80 +9.72 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,811.99 | ▼ 09:30 equity $10,594.59 vs yday $10,630.54 (-35.95) | 09:30 open · cash $16,811.99 (unchanged overnight, no fees) · equity $10,594.59 vs prior close $10,630.54 (-35.95) · 4 name(s) re-marked at the open (per-name table). LUNR×86 yday $18.52 → 09:30 $18.13 +33.54; VERI×1437 yday $0.97 → 09:30 $0.96 +4.31; HIVE×549 yday $2.82 → 09:30 $2.95 -71.37; RNW×243 yday $6.80 → 09:30 $6.81 -2.43 | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 86 | $18.13 | $2.25 | $+84.87 | $15,250.56 | ▲ +84.87 after sell → book $10,592.35; vs 09:30 mark -2.24 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 1437 | $0.96 | $18.15 | $+231.74 | $13,848.58 | ▲ +231.74 after sell → book $10,574.20; vs 09:30 mark -18.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 549 | $2.95 | $7.08 | $+18.63 | $12,221.94 | ▲ +18.63 after sell → book $10,567.11; vs 09:30 mark -7.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `RNW` | 243 | $6.81 | $3.13 | $-8.80 | $10,563.98 | ▼ -8.80 after sell → book $10,563.98; vs 09:30 mark -3.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $11,175.29 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $660.25 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 30 | $21.40 | $2.12 | — | $11,815.18 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_mover; 🔵; ret5=-25.2; leftover $660.25 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 149 | $4.43 | $2.49 | — | $12,472.76 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_mover; 🔵; ret5=-23.1; leftover $660.25 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 55 | $11.81 | $2.19 | — | $13,120.39 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $660.25 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 3 | $173.90 | $2.03 | — | $13,640.06 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+12.2; leftover $660.25 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 14 | $46.85 | $2.07 | — | $14,293.89 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list earn_react; 🔵; ret5=+5.0; leftover $660.25 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 6 | $106.38 | $2.05 | — | $14,930.12 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list earn_react; 🔵; ret5=-1.7; leftover $660.25 | join🟢 sector🟡 gen🟢 news🔴 digest🟡 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 143 | $4.61 | $2.47 | — | $15,586.88 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list mover_buy; 🔵; ⚪; ret5=+8.5; leftover $660.25 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,586.88 | ▲ close $10,622.86 vs 09:30 $10,594.59 (session +76.34) | 16:00 close · cash $15,586.88 · equity $10,622.86 vs 09:30 $10,594.59 (+28.27; session marks +76.34) · 8 name(s) marked open→close (per-name table). AEM×3 09:30 $204.45 → close $212.04 -22.77; WYFI×30 09:30 $21.40 → close $21.16 +7.20; TOYO×149 09:30 $4.43 → close $4.51 -12.66; ABCL×55 09:30 $11.81 → close $11.57 +13.47; TEAM×3 09:30 $173.90 → close $174.91 -3.03; AAP×14 09:30 $46.85 → close $42.39 +62.44; WMT×6 09:30 $106.38 → close $103.84 +15.24; AQST×143 09:30 $4.61 → close $4.50 +16.45 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,586.88 | ▼ 09:30 equity $10,570.35 vs yday $10,622.86 (-52.51) | 09:30 open · cash $15,586.88 (unchanged overnight, no fees) · equity $10,570.35 vs prior close $10,622.86 (-52.51) · 8 name(s) re-marked at the open (per-name table). AEM×3 yday $212.04 → 09:30 $216.30 -12.78; WYFI×30 yday $21.16 → 09:30 $21.54 -11.40; TOYO×149 yday $4.51 → 09:30 $4.68 -24.59; ABCL×55 yday $11.57 → 09:30 $11.57 -0.00; TEAM×3 yday $174.91 → 09:30 $174.22 +2.07; AAP×14 yday $42.39 → 09:30 $42.41 -0.28; WMT×6 yday $103.84 → 09:30 $103.69 +0.90; AQST×143 yday $4.50 → 09:30 $4.54 -6.43 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 7 | $133.11 | $2.06 | — | $16,516.59 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_mover; 🔵; ⚪; ret5=+109.5; leftover $1057.03 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 11 | $89.10 | $2.07 | — | $17,494.62 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+15.8; leftover $1057.03 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 27 | $38.40 | $2.12 | — | $18,529.30 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+15.8; leftover $1057.03 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 50 | $20.90 | $2.19 | — | $19,572.11 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1057.03 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 39 | $27.00 | $2.16 | — | $20,622.96 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; ret5=+10.1; leftover $1057.03 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,622.96 | ▼ close $10,507.45 vs 09:30 $10,570.35 (session -52.31) | 16:00 close · cash $20,622.96 · equity $10,507.45 vs 09:30 $10,570.35 (-62.90; session marks -52.31) · 13 name(s) marked open→close (per-name table). AEM×3 09:30 $216.30 → close $216.06 +0.72; WYFI×30 09:30 $21.54 → close $20.72 +24.60; TOYO×149 09:30 $4.68 → close $4.82 -20.86; ABCL×55 09:30 $11.57 → close $11.32 +13.75; TEAM×3 09:30 $174.22 → close $171.81 +7.23; AAP×14 09:30 $42.41 → close $42.58 -2.38; WMT×6 09:30 $103.69 → close $103.70 -0.06; AQST×143 09:30 $4.54 → close $4.66 -17.16; MRNA×7 09:30 $133.11 → close $145.13 -84.14; AUGO×11 09:30 $89.10 → close $87.26 +20.24; SSRM×27 09:30 $38.40 → close $37.77 +17.01; ARIS×50 09:30 $20.90 → close $20.86 +2.00; NOG×39 09:30 $27.00 → close $27.34 -13.26 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,622.96 | ▲ 09:30 equity $10,567.73 vs yday $10,507.45 (+60.28) | 09:30 open · cash $20,622.96 (unchanged overnight, no fees) · equity $10,567.73 vs prior close $10,507.45 (+60.28) · 13 name(s) re-marked at the open (per-name table). AEM×3 yday $216.06 → 09:30 $217.03 -2.91; WYFI×30 yday $20.72 → 09:30 $20.01 +21.30; TOYO×149 yday $4.82 → 09:30 $4.58 +35.76; ABCL×55 yday $11.32 → 09:30 $10.97 +19.25; TEAM×3 yday $171.81 → 09:30 $169.30 +7.53; AAP×14 yday $42.58 → 09:30 $43.05 -6.58; WMT×6 yday $103.70 → 09:30 $104.14 -2.64; AQST×143 yday $4.66 → 09:30 $4.67 -1.43; MRNA×7 yday $145.13 → 09:30 $142.70 +17.01; AUGO×11 yday $87.26 → 09:30 $88.60 -14.74; SSRM×27 yday $37.77 → 09:30 $38.32 -14.85; ARIS×50 yday $20.86 → 09:30 $20.98 -6.00; NOG×39 yday $27.34 → 09:30 $27.12 +8.58 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,622.96 | ▲ close $10,596.54 vs 09:30 $10,567.73 (session +28.81) | 16:00 close · cash $20,622.96 · equity $10,596.54 vs 09:30 $10,567.73 (+28.81; session marks +28.81) · 13 name(s) marked open→close (per-name table). AEM×3 09:30 $217.03 → close $217.89 -2.58; WYFI×30 09:30 $20.01 → close $20.78 -23.10; TOYO×149 09:30 $4.58 → close $4.38 +29.80; ABCL×55 09:30 $10.97 → close $10.61 +19.80; TEAM×3 09:30 $169.30 → close $171.33 -6.09; AAP×14 09:30 $43.05 → close $43.63 -8.12; WMT×6 09:30 $104.14 → close $106.49 -14.10; AQST×143 09:30 $4.67 → close $4.80 -18.59; MRNA×7 09:30 $142.70 → close $138.89 +26.67; AUGO×11 09:30 $88.60 → close $87.37 +13.53; SSRM×27 09:30 $38.32 → close $38.61 -7.83; ARIS×50 09:30 $20.98 → close $20.81 +8.50; NOG×39 09:30 $27.12 → close $26.84 +10.92 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,622.96 | ▲ 09:30 equity $10,651.88 vs yday $10,596.54 (+55.34) | 09:30 open · cash $20,622.96 (unchanged overnight, no fees) · equity $10,651.88 vs prior close $10,596.54 (+55.34) · 13 name(s) re-marked at the open (per-name table). AEM×3 yday $217.89 → 09:30 $212.00 +17.67; WYFI×30 yday $20.78 → 09:30 $20.90 -3.60; TOYO×149 yday $4.38 → 09:30 $4.42 -5.96; ABCL×55 yday $10.61 → 09:30 $11.00 -21.45; TEAM×3 yday $171.33 → 09:30 $170.64 +2.07; AAP×14 yday $43.63 → 09:30 $43.63 -0.00; WMT×6 yday $106.49 → 09:30 $105.58 +5.46; AQST×143 yday $4.80 → 09:30 $4.77 +4.29; MRNA×7 yday $138.89 → 09:30 $143.50 -32.27; AUGO×11 yday $87.37 → 09:30 $85.78 +17.49; SSRM×27 yday $38.61 → 09:30 $37.75 +23.22; ARIS×50 yday $20.81 → 09:30 $20.45 +18.00; NOG×39 yday $26.84 → 09:30 $26.06 +30.42 | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 3 | $212.00 | $2.00 | $-26.68 | $19,984.96 | ▼ -26.68 after sell → book $10,649.88; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 30 | $20.90 | $2.08 | $+10.80 | $19,355.88 | ▲ +10.80 after sell → book $10,647.80; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 149 | $4.42 | $2.44 | $-3.44 | $18,694.86 | ▼ -3.44 after sell → book $10,645.36; vs 09:30 mark -2.44 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 55 | $11.00 | $2.15 | $+40.48 | $18,087.71 | ▲ +40.48 after sell → book $10,643.21; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 3 | $170.64 | $2.00 | $+5.75 | $17,573.79 | ▲ +5.75 after sell → book $10,641.21; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 14 | $43.63 | $2.03 | $+40.98 | $16,960.94 | ▲ +40.98 after sell → book $10,639.18; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 6 | $105.58 | $2.01 | $+0.75 | $16,325.45 | ▲ +0.75 after sell → book $10,637.17; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 143 | $4.77 | $2.42 | $-27.77 | $15,640.92 | ▼ -27.77 after sell → book $10,634.75; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 130 | $13.62 | $2.46 | — | $17,409.71 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $1772.46 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 32 | $54.51 | $2.16 | — | $19,151.87 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; ret5=+15.1; leftover $1772.46 | join🔴 sector🟡 gen🟡 news🔴 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 4 | $364.35 | $2.06 | — | $20,607.20 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list overnight,overnight_mega; 🔵; ret5=+10.2; leftover $1772.46 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,607.20 | ▼ close $10,462.83 vs 09:30 $10,651.88 (session -165.23) | 16:00 close · cash $20,607.20 · equity $10,462.83 vs 09:30 $10,651.88 (-189.05; session marks -165.23) · 8 name(s) marked open→close (per-name table). MRNA×7 09:30 $143.50 → close $158.83 -107.31; AUGO×11 09:30 $85.78 → close $90.47 -51.59; SSRM×27 09:30 $37.75 → close $39.21 -39.42; ARIS×50 09:30 $20.45 → close $21.18 -36.50; NOG×39 09:30 $26.06 → close $26.42 -14.04; AVAH×130 09:30 $13.62 → close $13.59 +4.55; ARE×32 09:30 $54.51 → close $52.90 +51.52; INTU×4 09:30 $364.35 → close $357.46 +27.56 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,607.20 | ▲ 09:30 equity $10,724.07 vs yday $10,462.83 (+261.24) | 09:30 open · cash $20,607.20 (unchanged overnight, no fees) · equity $10,724.07 vs prior close $10,462.83 (+261.24) · 8 name(s) re-marked at the open (per-name table). MRNA×7 yday $158.83 → 09:30 $154.20 +32.41; AUGO×11 yday $90.47 → 09:30 $88.24 +24.53; SSRM×27 yday $39.21 → 09:30 $38.41 +21.60; ARIS×50 yday $21.18 → 09:30 $20.50 +34.00; NOG×39 yday $26.42 → 09:30 $26.00 +16.38; AVAH×130 yday $13.59 → 09:30 $13.65 -7.80; ARE×32 yday $52.90 → 09:30 $52.77 +4.16; INTU×4 yday $357.46 → 09:30 $323.47 +135.96 | — |
| 2026-08-26 09:30 ET | **COVER** | `MRNA` | 7 | $154.20 | $2.01 | $-151.70 | $19,525.79 | ▼ -151.70 after sell → book $10,722.06; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 11 | $88.24 | $2.02 | $+5.37 | $18,553.13 | ▲ +5.37 after sell → book $10,720.04; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 27 | $38.41 | $2.07 | $-4.46 | $17,513.99 | ▼ -4.46 after sell → book $10,717.97; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 50 | $20.50 | $2.14 | $+15.67 | $16,486.85 | ▲ +15.67 after sell → book $10,715.83; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 39 | $26.00 | $2.11 | $+34.74 | $15,470.74 | ▲ +34.74 after sell → book $10,713.72; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 5 | $213.94 | $2.05 | — | $16,538.39 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $1071.37 | join🟢 sector🔴 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 87 | $12.22 | $2.30 | — | $17,599.22 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+12.4; leftover $1071.37 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 210 | $5.08 | $2.78 | — | $18,663.24 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+17.6; leftover $1071.37 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 8 | $132.64 | $2.06 | — | $19,722.30 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+16.5; leftover $1071.37 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 5 | $199.94 | $2.05 | — | $20,719.94 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list overnight,overnight_mega; ret5=+2.1; leftover $1071.37 | join🟡 sector🔴 gen🟢 news🔴 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 catal🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,719.94 | ▼ close $10,502.05 vs 09:30 $10,724.07 (session -200.41) | 16:00 close · cash $20,719.94 · equity $10,502.05 vs 09:30 $10,724.07 (-222.02; session marks -200.41) · 8 name(s) marked open→close (per-name table). AVAH×130 09:30 $13.65 → close $13.62 +3.90; ARE×32 09:30 $52.77 → close $52.97 -6.40; INTU×4 09:30 $323.47 → close $345.88 -89.64; BE×5 09:30 $213.94 → close $218.21 -21.35; ABCL×87 09:30 $12.22 → close $12.24 -1.74; AQST×210 09:30 $5.08 → close $5.39 -65.10; NEM×8 09:30 $132.64 → close $131.60 +8.32; CRM×5 09:30 $199.94 → close $205.62 -28.40 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,719.94 | ▼ 09:30 equity $10,325.22 vs yday $10,502.05 (-176.83) | 09:30 open · cash $20,719.94 (unchanged overnight, no fees) · equity $10,325.22 vs prior close $10,502.05 (-176.83) · 8 name(s) re-marked at the open (per-name table). AVAH×130 yday $13.62 → 09:30 $13.62 -0.00; ARE×32 yday $52.97 → 09:30 $52.45 +16.64; INTU×4 yday $345.88 → 09:30 $353.54 -30.64; BE×5 yday $218.21 → 09:30 $227.10 -44.45; ABCL×87 yday $12.24 → 09:30 $12.25 -0.87; AQST×210 yday $5.39 → 09:30 $5.39 -0.00; NEM×8 yday $131.60 → 09:30 $131.02 +4.64; CRM×5 yday $205.62 → 09:30 $230.05 -122.15 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,719.94 | ▼ close $10,288.26 vs 09:30 $10,325.22 (session -36.96) | 16:00 close · cash $20,719.94 · equity $10,288.26 vs 09:30 $10,325.22 (-36.96; session marks -36.96) · 8 name(s) marked open→close (per-name table). AVAH×130 09:30 $13.62 → close $13.82 -26.00; ARE×32 09:30 $52.45 → close $52.28 +5.44; INTU×4 09:30 $353.54 → close $348.00 +22.16; BE×5 09:30 $227.10 → close $217.83 +46.35; ABCL×87 09:30 $12.25 → close $12.40 -13.05; AQST×210 09:30 $5.39 → close $5.16 +48.30; NEM×8 09:30 $131.02 → close $132.29 -10.16; CRM×5 09:30 $230.05 → close $252.05 -110.00 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,719.94 | ▲ 09:30 equity $10,308.67 vs yday $10,288.26 (+20.41) | 09:30 open · cash $20,719.94 (unchanged overnight, no fees) · equity $10,308.67 vs prior close $10,288.26 (+20.41) · 8 name(s) re-marked at the open (per-name table). AVAH×130 yday $13.82 → 09:30 $13.90 -10.40; ARE×32 yday $52.28 → 09:30 $52.49 -6.72; INTU×4 yday $348.00 → 09:30 $347.82 +0.72; BE×5 yday $217.83 → 09:30 $215.71 +10.62; ABCL×87 yday $12.40 → 09:30 $12.30 +8.27; AQST×210 yday $5.16 → 09:30 $5.11 +10.50; NEM×8 yday $132.29 → 09:30 $132.35 -0.48; CRM×5 yday $252.05 → 09:30 $250.47 +7.90 | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 130 | $13.90 | $2.38 | $-40.59 | $18,910.56 | ▼ -40.59 after sell → book $10,306.29; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 32 | $52.49 | $2.09 | $+60.40 | $17,228.80 | ▲ +60.40 after sell → book $10,304.21; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 4 | $347.82 | $2.00 | $+62.05 | $15,835.52 | ▲ +62.05 after sell → book $10,302.21; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 10 | $252.24 | $2.12 | — | $18,355.80 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $2575.55 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 85 | $30.18 | $2.35 | — | $20,918.75 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; ret5=+12.1; leftover $2575.55 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,918.75 | ▲ close $10,611.61 vs 09:30 $10,308.67 (session +313.88) | 16:00 close · cash $20,918.75 · equity $10,611.61 vs 09:30 $10,308.67 (+302.94; session marks +313.88) · 7 name(s) marked open→close (per-name table). BE×5 09:30 $215.71 → close $210.77 +24.68; ABCL×87 09:30 $12.30 → close $11.35 +83.09; AQST×210 09:30 $5.11 → close $5.02 +18.90; NEM×8 09:30 $132.35 → close $127.98 +34.96; CRM×5 09:30 $250.47 → close $256.00 -27.65; SIMO×10 09:30 $252.24 → close $245.81 +64.30; FIG×85 09:30 $30.18 → close $28.82 +115.60 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,918.75 | ▲ 09:30 equity $10,755.85 vs yday $10,611.61 (+144.24) | 09:30 open · cash $20,918.75 (unchanged overnight, no fees) · equity $10,755.85 vs prior close $10,611.61 (+144.24) · 7 name(s) re-marked at the open (per-name table). BE×5 yday $210.77 → 09:30 $208.88 +9.45; ABCL×87 yday $11.35 → 09:30 $11.10 +21.75; AQST×210 yday $5.02 → 09:30 $4.97 +9.45; NEM×8 yday $127.98 → 09:30 $127.45 +4.24; CRM×5 yday $256.00 → 09:30 $254.39 +8.05; SIMO×10 yday $245.81 → 09:30 $247.05 -12.40; FIG×85 yday $28.82 → 09:30 $27.60 +103.70 | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 5 | $208.88 | $2.00 | $+21.24 | $19,872.34 | ▲ +21.24 after sell → book $10,753.84; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 87 | $11.10 | $2.25 | $+92.88 | $18,904.39 | ▲ +92.88 after sell → book $10,751.59; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 210 | $4.97 | $2.71 | $+16.56 | $17,856.93 | ▲ +16.56 after sell → book $10,748.88; vs 09:30 mark -2.71 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 8 | $127.45 | $2.01 | $+37.44 | $16,835.32 | ▲ +37.44 after sell → book $10,746.87; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 5 | $254.39 | $2.00 | $-276.31 | $15,561.36 | ▼ -276.31 after sell → book $10,744.86; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | join🔴 sector🟢 gen🔴 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,561.36 | ▲ close $10,756.31 vs 09:30 $10,755.85 (session +11.45) | 16:00 close · cash $15,561.36 · equity $10,756.31 vs 09:30 $10,755.85 (+0.46; session marks +11.45) · 2 name(s) marked open→close (per-name table). SIMO×10 09:30 $247.05 → close $246.84 +2.10; FIG×85 09:30 $27.60 → close $27.49 +9.35 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,561.36 | ▲ 09:30 equity $10,860.36 vs yday $10,756.31 (+104.05) | 09:30 open · cash $15,561.36 (unchanged overnight, no fees) · equity $10,860.36 vs prior close $10,756.31 (+104.05) · 2 name(s) re-marked at the open (per-name table). SIMO×10 yday $246.84 → 09:30 $240.09 +67.50; FIG×85 yday $27.49 → 09:30 $27.06 +36.55 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,561.36 | ▲ close $10,875.86 vs 09:30 $10,860.36 (session +15.50) | 16:00 close · cash $15,561.36 · equity $10,875.86 vs 09:30 $10,860.36 (+15.50; session marks +15.50) · 2 name(s) marked open→close (per-name table). SIMO×10 09:30 $240.09 → close $237.35 +27.40; FIG×85 09:30 $27.06 → close $27.20 -11.90 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,561.36 | ▲ 09:30 equity $10,927.96 vs yday $10,875.86 (+52.10) | 09:30 open · cash $15,561.36 (unchanged overnight, no fees) · equity $10,927.96 vs prior close $10,875.86 (+52.10) · 2 name(s) re-marked at the open (per-name table). SIMO×10 yday $237.35 → 09:30 $235.71 +16.40; FIG×85 yday $27.20 → 09:30 $26.78 +35.70 | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 10 | $235.71 | $2.02 | $+161.16 | $13,202.24 | ▲ +161.16 after sell → book $10,925.94; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 85 | $26.78 | $2.25 | $+284.41 | $10,923.70 | ▲ +284.41 after sell → book $10,923.70; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,923.70 | ▲ close $10,923.70 vs 09:30 $10,927.96 (session +0.00) | 16:00 close · cash $10,923.70 · no lots left · equity $10,923.70. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,923.70 | ▲ 09:30 equity $10,923.70 vs yday $10,923.70 (-0.00) | 09:30 open · cash $10,923.70 · no holdings · equity $10,923.70 vs prior close $10,923.70 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 3194 | $1.71 | $41.93 | — | $16,343.51 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ⚪; ret5=+11.9; leftover $5461.85 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,343.51 | ▲ close $11,201.17 vs 09:30 $10,923.70 (session +319.40) | 16:00 close · cash $16,343.51 · equity $11,201.17 vs 09:30 $10,923.70 (+277.47; session marks +319.40) · 1 name(s) marked open→close (per-name table). OPK×3194 09:30 $1.71 → close $1.61 +319.40 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,343.51 | ▲ 09:30 equity $11,265.05 vs yday $11,201.17 (+63.88) | 09:30 open · cash $16,343.51 (unchanged overnight, no fees) · equity $11,265.05 vs prior close $11,201.17 (+63.88) · 1 name(s) re-marked at the open (per-name table). OPK×3194 yday $1.61 → 09:30 $1.59 +63.88 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 603 | $4.67 | $7.98 | — | $19,151.54 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_gainer; ret5=+11.9; leftover $2816.26 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 36 | $76.55 | $2.21 | — | $21,905.14 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list mover_buy; 🔵; ⚪; ret5=+4.2; leftover $2816.26 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,905.14 | ▼ close $11,077.53 vs 09:30 $11,265.05 (session -177.34) | 16:00 close · cash $21,905.14 · equity $11,077.53 vs 09:30 $11,265.05 (-187.52; session marks -177.34) · 3 name(s) marked open→close (per-name table). OPK×3194 09:30 $1.59 → close $1.64 -159.70; GSM×603 09:30 $4.67 → close $4.67 -0.00; PIPR×36 09:30 $76.55 → close $77.04 -17.64 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,905.14 | ▼ 09:30 equity $11,075.63 vs yday $11,077.53 (-1.90) | 09:30 open · cash $21,905.14 (unchanged overnight, no fees) · equity $11,075.63 vs prior close $11,077.53 (-1.90) · 3 name(s) re-marked at the open (per-name table). OPK×3194 yday $1.64 → 09:30 $1.63 +31.94; GSM×603 yday $4.67 → 09:30 $4.75 -48.24; PIPR×36 yday $77.04 → 09:30 $76.64 +14.40 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,905.14 | ▲ close $11,316.88 vs 09:30 $11,075.63 (session +241.25) | 16:00 close · cash $21,905.14 · equity $11,316.88 vs 09:30 $11,075.63 (+241.25; session marks +241.25) · 3 name(s) marked open→close (per-name table). OPK×3194 09:30 $1.63 → close $1.59 +127.76; GSM×603 09:30 $4.75 → close $4.52 +138.69; PIPR×36 09:30 $76.64 → close $77.34 -25.20 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,905.14 | ▲ 09:30 equity $11,352.42 vs yday $11,316.88 (+35.54) | 09:30 open · cash $21,905.14 (unchanged overnight, no fees) · equity $11,352.42 vs prior close $11,316.88 (+35.54) · 3 name(s) re-marked at the open (per-name table). OPK×3194 yday $1.59 → 09:30 $1.58 +31.94; GSM×603 yday $4.52 → 09:30 $4.52 -0.00; PIPR×36 yday $77.34 → 09:30 $77.24 +3.60 | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 3194 | $1.58 | $41.20 | $+332.09 | $16,817.42 | ▲ +332.09 after sell → book $11,311.22; vs 09:30 mark -41.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,817.42 | ▲ close $11,339.39 vs 09:30 $11,352.42 (session +28.17) | 16:00 close · cash $16,817.42 · equity $11,339.39 vs 09:30 $11,352.42 (-13.03; session marks +28.17) · 2 name(s) marked open→close (per-name table). GSM×603 09:30 $4.52 → close $4.49 +18.09; PIPR×36 09:30 $77.24 → close $76.96 +10.08 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,817.42 | ▲ 09:30 equity $11,423.90 vs yday $11,339.39 (+84.51) | 09:30 open · cash $16,817.42 (unchanged overnight, no fees) · equity $11,423.90 vs prior close $11,339.39 (+84.51) · 2 name(s) re-marked at the open (per-name table). GSM×603 yday $4.49 → 09:30 $4.36 +78.39; PIPR×36 yday $76.96 → 09:30 $76.79 +6.12 | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 603 | $4.36 | $7.78 | $+171.17 | $14,180.56 | ▲ +171.17 after sell → book $11,416.12; vs 09:30 mark -7.78 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 36 | $76.79 | $2.10 | $-12.94 | $11,414.02 | ▼ -12.94 after sell → book $11,414.02; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,414.02 | ▲ close $11,414.02 vs 09:30 $11,423.90 (session +0.00) | 16:00 close · cash $11,414.02 · no lots left · equity $11,414.02. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,414.02 | ▲ 09:30 equity $11,414.02 vs yday $11,414.02 (-0.00) | 09:30 open · cash $11,414.02 · no holdings · equity $11,414.02 vs prior close $11,414.02 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 25 | $112.83 | $2.17 | — | $14,232.72 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_gainer,ohlc_hot; ret5=+11.7; leftover $2853.50 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 846 | $3.37 | $11.15 | — | $17,072.58 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+4.0; leftover $2853.50 | join🔴 sector🟡 gen🟡 news🔴 digest🔴 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,072.58 | ▼ close $11,263.01 vs 09:30 $11,414.02 (session -137.68) | 16:00 close · cash $17,072.58 · equity $11,263.01 vs 09:30 $11,414.02 (-151.01; session marks -137.68) · 2 name(s) marked open→close (per-name table). QRVO×25 09:30 $112.83 → close $116.65 -95.38; MYGN×846 09:30 $3.37 → close $3.42 -42.30 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,072.58 | ▲ 09:30 equity $11,318.05 vs yday $11,263.01 (+55.04) | 09:30 open · cash $17,072.58 (unchanged overnight, no fees) · equity $11,318.05 vs prior close $11,263.01 (+55.04) · 2 name(s) re-marked at the open (per-name table). QRVO×25 yday $116.65 → 09:30 $114.11 +63.50; MYGN×846 yday $3.42 → 09:30 $3.43 -8.46 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,072.58 | ▼ close $11,166.74 vs 09:30 $11,318.05 (session -151.31) | 16:00 close · cash $17,072.58 · equity $11,166.74 vs 09:30 $11,318.05 (-151.31; session marks -151.31) · 2 name(s) marked open→close (per-name table). QRVO×25 09:30 $114.11 → close $107.98 +153.25; MYGN×846 09:30 $3.43 → close $3.79 -304.56 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,072.58 | ▼ 09:30 equity $11,147.78 vs yday $11,166.74 (-18.96) | 09:30 open · cash $17,072.58 (unchanged overnight, no fees) · equity $11,147.78 vs prior close $11,166.74 (-18.96) · 2 name(s) re-marked at the open (per-name table). QRVO×25 yday $107.98 → 09:30 $108.40 -10.50; MYGN×846 yday $3.79 → 09:30 $3.80 -8.46 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,072.58 | ▼ close $10,838.60 vs 09:30 $11,147.78 (session -309.18) | 16:00 close · cash $17,072.58 · equity $10,838.60 vs 09:30 $11,147.78 (-309.18; session marks -309.18) · 2 name(s) marked open→close (per-name table). QRVO×25 09:30 $108.40 → close $118.06 -241.50; MYGN×846 09:30 $3.80 → close $3.88 -67.68 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,072.58 | ▲ 09:30 equity $10,945.58 vs yday $10,838.60 (+106.98) | 09:30 open · cash $17,072.58 (unchanged overnight, no fees) · equity $10,945.58 vs prior close $10,838.60 (+106.98) · 2 name(s) re-marked at the open (per-name table). QRVO×25 yday $118.06 → 09:30 $118.18 -3.00; MYGN×846 yday $3.88 → 09:30 $3.75 +109.98 | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 25 | $118.18 | $2.06 | $-137.86 | $14,116.02 | ▼ -137.86 after sell → book $10,943.52; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 846 | $3.75 | $10.91 | $-343.55 | $10,932.61 | ▼ -343.55 after sell → book $10,932.61; vs 09:30 mark -10.91 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 800 | $6.83 | $10.65 | — | $16,385.96 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; ret5=+11.2; leftover $5466.30 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,385.96 | ▲ close $11,193.96 vs 09:30 $10,945.58 (session +272.00) | 16:00 close · cash $16,385.96 · equity $11,193.96 vs 09:30 $10,945.58 (+248.38; session marks +272.00) · 1 name(s) marked open→close (per-name table). GFR×800 09:30 $6.83 → close $6.49 +272.00 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,385.96 | ▲ 09:30 equity $11,201.96 vs yday $11,193.96 (+8.00) | 09:30 open · cash $16,385.96 (unchanged overnight, no fees) · equity $11,201.96 vs prior close $11,193.96 (+8.00) · 1 name(s) re-marked at the open (per-name table). GFR×800 yday $6.49 → 09:30 $6.48 +8.00 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,385.96 | ▼ close $11,057.96 vs 09:30 $11,201.96 (session -144.00) | 16:00 close · cash $16,385.96 · equity $11,057.96 vs 09:30 $11,201.96 (-144.00; session marks -144.00) · 1 name(s) marked open→close (per-name table). GFR×800 09:30 $6.48 → close $6.66 -144.00 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,385.96 | ▲ 09:30 equity $11,073.96 vs yday $11,057.96 (+16.00) | 09:30 open · cash $16,385.96 (unchanged overnight, no fees) · equity $11,073.96 vs prior close $11,057.96 (+16.00) · 1 name(s) re-marked at the open (per-name table). GFR×800 yday $6.66 → 09:30 $6.64 +16.00 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,385.96 | ▼ close $11,057.96 vs 09:30 $11,073.96 (session -16.00) | 16:00 close · cash $16,385.96 · equity $11,057.96 vs 09:30 $11,073.96 (-16.00; session marks -16.00) · 1 name(s) marked open→close (per-name table). GFR×800 09:30 $6.64 → close $6.66 -16.00 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,385.96 | ▲ 09:30 equity $11,145.96 vs yday $11,057.96 (+88.00) | 09:30 open · cash $16,385.96 (unchanged overnight, no fees) · equity $11,145.96 vs prior close $11,057.96 (+88.00) · 1 name(s) re-marked at the open (per-name table). GFR×800 yday $6.66 → 09:30 $6.55 +88.00 | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 800 | $6.55 | $10.32 | $+203.03 | $11,135.64 | ▲ +203.03 after sell → book $11,135.64; vs 09:30 mark -10.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 337 | $8.26 | $4.50 | — | $13,914.76 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_mover; ret5=+7.7; leftover $2783.91 | join🟡 sector🔴 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 4 | $583.88 | $2.09 | — | $16,248.18 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; ret5=+8.5; leftover $2783.91 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,248.18 | ▲ close $11,454.06 vs 09:30 $11,145.96 (session +325.02) | 16:00 close · cash $16,248.18 · equity $11,454.06 vs 09:30 $11,145.96 (+308.10; session marks +325.02) · 2 name(s) marked open→close (per-name table). AEHL×337 09:30 $8.26 → close $6.92 +451.58; AMD×4 09:30 $583.88 → close $615.52 -126.56 | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,248.18 | ▼ 09:30 equity $11,419.09 vs yday $11,454.06 (-34.97) | 09:30 open · cash $16,248.18 (unchanged overnight, no fees) · equity $11,419.09 vs prior close $11,454.06 (-34.97) · 2 name(s) re-marked at the open (per-name table). AEHL×337 yday $6.92 → 09:30 $7.13 -70.77; AMD×4 yday $615.52 → 09:30 $606.57 +35.80 | — |
| 2026-09-22 09:30 ET | **SHORT** | `FIVN` | 152 | $37.35 | $2.67 | — | $21,922.71 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_gainer,ohlc_hot; ret5=+16.6; leftover $5709.55 | join🟢 sector🔴 gen🔴 news🔴 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,922.71 | ▼ close $11,208.66 vs 09:30 $11,419.09 (session -207.76) | 16:00 close · cash $21,922.71 · equity $11,208.66 vs 09:30 $11,419.09 (-210.43; session marks -207.76) · 3 name(s) marked open→close (per-name table). AEHL×337 09:30 $7.13 → close $7.61 -161.76; AMD×4 09:30 $606.57 → close $623.77 -68.80; FIVN×152 09:30 $37.35 → close $37.20 +22.80 | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,922.71 | ▼ 09:30 equity $10,939.69 vs yday $11,208.66 (-268.97) | 09:30 open · cash $21,922.71 (unchanged overnight, no fees) · equity $10,939.69 vs prior close $11,208.66 (-268.97) · 3 name(s) re-marked at the open (per-name table). AEHL×337 yday $7.61 → 09:30 $7.62 -3.37; AMD×4 yday $623.77 → 09:30 $622.15 +6.48; FIVN×152 yday $37.20 → 09:30 $38.99 -272.08 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,922.71 | ▲ close $10,965.09 vs 09:30 $10,939.69 (session +25.40) | 16:00 close · cash $21,922.71 · equity $10,965.09 vs 09:30 $10,939.69 (+25.40; session marks +25.40) · 3 name(s) marked open→close (per-name table). AEHL×337 09:30 $7.62 → close $8.22 -203.88; AMD×4 09:30 $622.15 → close $614.61 +30.16; FIVN×152 09:30 $38.99 → close $37.68 +199.12 | — |

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
| 2026-08-18 | `HIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `VERI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `HIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `RNW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new buys |
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
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `SSRM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARIS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARIS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `NOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ARE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `INTU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ARE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INTU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `NEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `CRM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `BE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `AQST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `NEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `CRM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `PIPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `GSM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `PIPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `QRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `MYGN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `QRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `MYGN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SPCX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `GFR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `GFR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `AEHL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `AMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `AEHL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `AMD` | min_hold | dropped but min-hold 2/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `AEHL` | 337 | 2026-09-21 @ $8.26 | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_mover; ret5=+7.7; leftover $2783.91 |
| `AMD` | 4 | 2026-09-21 @ $583.88 | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; ret5=+8.5; leftover $2783.91 |
| `FIVN` | 152 | 2026-09-22 @ $37.35 | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_gainer,ohlc_hot; ret5=+16.6; leftover $5709.55 |
