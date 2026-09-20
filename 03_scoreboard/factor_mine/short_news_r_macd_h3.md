# Factor mine action — `short_news_r_macd_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · short news🔴 ∩ prior MACD histogram > 0

Cash book **+12.94%** ($11,294) · signal-only (no cash/fees) was +23.73%. Starts YES **12/26**. Fills 73 · skips 84 · realized $+1674.43.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $17,487.50.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `EU` | 1059 | — | $1.18 | +0.00 | $1.21 | -31.77 | -31.77 | -0.00 | -31.77 |
| 2026-08-14 | `LUNR` | 65 | — | $19.17 | +0.00 | $19.01 | +10.40 | +10.40 | -0.00 | +10.40 |
| 2026-08-14 | `OWL` | 98 | — | $12.70 | +0.00 | $12.22 | +46.55 | +46.55 | -0.00 | +46.55 |
| 2026-08-14 | `SVV` | 114 | — | $10.89 | +0.00 | $10.66 | +26.22 | +26.22 | -0.00 | +26.22 |
| 2026-08-17 | `EU` | 1059 | $1.21 | $1.21 | +0.00 | $1.13 | +84.72 | +84.72 | -31.77 | +52.95 |
| 2026-08-17 | `LUNR` | 65 | $19.01 | $20.25 | -80.60 | $20.38 | -8.45 | -89.05 | -70.20 | -78.65 |
| 2026-08-17 | `OWL` | 98 | $12.22 | $12.12 | +9.80 | $11.66 | +45.08 | +54.88 | +56.35 | +101.43 |
| 2026-08-17 | `SVV` | 114 | $10.66 | $10.49 | +19.38 | $10.56 | -7.98 | +11.40 | +45.60 | +37.62 |
| 2026-08-17 | `VERI` | 1446 | — | $1.15 | +0.00 | $1.08 | +93.99 | +93.99 | -0.00 | +93.99 |
| 2026-08-17 | `HIVE` | 552 | — | $3.01 | +0.00 | $3.07 | -33.12 | -33.12 | -0.00 | -33.12 |
| 2026-08-17 | `BIRK` | 42 | — | $39.48 | +0.00 | $37.86 | +68.04 | +68.04 | -0.00 | +68.04 |
| 2026-08-18 | `EU` | 1059 | $1.13 | $1.13 | +0.00 | $1.07 | +63.54 | +63.54 | +52.95 | +116.49 |
| 2026-08-18 | `LUNR` | 65 | $20.38 | $19.31 | +69.55 | $19.31 | +0.00 | +69.55 | -9.10 | -9.10 |
| 2026-08-18 | `OWL` | 98 | $11.66 | $11.54 | +11.76 | $11.59 | -4.90 | +6.86 | +113.19 | +108.29 |
| 2026-08-18 | `SVV` | 114 | $10.56 | $10.49 | +7.98 | $10.53 | -4.56 | +3.42 | +45.60 | +41.04 |
| 2026-08-18 | `VERI` | 1446 | $1.08 | $1.05 | +50.61 | $0.99 | +79.53 | +130.14 | +144.60 | +224.13 |
| 2026-08-18 | `HIVE` | 552 | $3.07 | $2.96 | +60.72 | $2.78 | +99.36 | +160.08 | +27.60 | +126.96 |
| 2026-08-18 | `BIRK` | 42 | $37.86 | $38.07 | -8.82 | $37.23 | +35.28 | +26.46 | +59.22 | +94.50 |
| 2026-08-19 | `EU` | 1059 | $1.07 | $1.07 | +0.00 | — | +0.00 | +0.00 | +116.49 | — |
| 2026-08-19 | `LUNR` | 65 | $19.31 | $18.98 | +21.45 | $18.52 | +29.90 | +51.35 | +12.35 | +42.25 |
| 2026-08-19 | `OWL` | 98 | $11.59 | $11.75 | -15.68 | — | +0.00 | -15.68 | +92.61 | — |
| 2026-08-19 | `SVV` | 114 | $10.53 | $10.68 | -17.10 | — | +0.00 | -17.10 | +23.94 | — |
| 2026-08-19 | `VERI` | 1446 | $0.99 | $1.00 | -7.23 | $0.97 | +49.16 | +41.93 | +216.90 | +266.06 |
| 2026-08-19 | `HIVE` | 552 | $2.78 | $2.78 | +0.00 | $2.82 | -22.08 | -22.08 | +126.96 | +104.88 |
| 2026-08-19 | `BIRK` | 42 | $37.23 | $37.50 | -11.34 | $36.13 | +57.54 | +46.20 | +83.16 | +140.70 |
| 2026-08-20 | `LUNR` | 65 | $18.52 | $18.13 | +25.35 | — | +0.00 | +25.35 | +67.60 | — |
| 2026-08-20 | `VERI` | 1446 | $0.97 | $0.96 | +4.34 | — | +0.00 | +4.34 | +270.40 | — |
| 2026-08-20 | `HIVE` | 552 | $2.82 | $2.95 | -71.76 | — | +0.00 | -71.76 | +33.12 | — |
| 2026-08-20 | `BIRK` | 42 | $36.13 | $36.00 | +5.46 | — | +0.00 | +5.46 | +146.16 | — |
| 2026-08-20 | `AEM` | 3 | — | $204.45 | +0.00 | $212.04 | -22.77 | -22.77 | -0.00 | -22.77 |
| 2026-08-20 | `WYFI` | 31 | — | $21.40 | +0.00 | $21.16 | +7.44 | +7.44 | -0.00 | +7.44 |
| 2026-08-20 | `TOYO` | 150 | — | $4.43 | +0.00 | $4.51 | -12.75 | -12.75 | -0.00 | -12.75 |
| 2026-08-20 | `ABCL` | 56 | — | $11.81 | +0.00 | $11.57 | +13.72 | +13.72 | -0.00 | +13.72 |
| 2026-08-20 | `TEAM` | 3 | — | $173.90 | +0.00 | $174.91 | -3.03 | -3.03 | -0.00 | -3.03 |
| 2026-08-20 | `AAP` | 14 | — | $46.85 | +0.00 | $42.39 | +62.44 | +62.44 | -0.00 | +62.44 |
| 2026-08-20 | `WMT` | 6 | — | $106.38 | +0.00 | $103.84 | +15.24 | +15.24 | -0.00 | +15.24 |
| 2026-08-20 | `AQST` | 144 | — | $4.61 | +0.00 | $4.50 | +16.56 | +16.56 | -0.00 | +16.56 |
| 2026-08-21 | `AEM` | 3 | $212.04 | $216.30 | -12.78 | $216.06 | +0.72 | -12.06 | -35.55 | -34.83 |
| 2026-08-21 | `WYFI` | 31 | $21.16 | $21.54 | -11.78 | $20.72 | +25.42 | +13.64 | -4.34 | +21.08 |
| 2026-08-21 | `TOYO` | 150 | $4.51 | $4.68 | -24.75 | $4.82 | -21.00 | -45.75 | -37.50 | -58.50 |
| 2026-08-21 | `ABCL` | 56 | $11.57 | $11.57 | +0.00 | $11.32 | +14.00 | +14.00 | +13.72 | +27.72 |
| 2026-08-21 | `TEAM` | 3 | $174.91 | $174.22 | +2.07 | $171.81 | +7.23 | +9.30 | -0.96 | +6.27 |
| 2026-08-21 | `AAP` | 14 | $42.39 | $42.41 | -0.28 | $42.58 | -2.38 | -2.66 | +62.16 | +59.78 |
| 2026-08-21 | `WMT` | 6 | $103.84 | $103.69 | +0.90 | $103.70 | -0.06 | +0.84 | +16.14 | +16.08 |
| 2026-08-21 | `AQST` | 144 | $4.50 | $4.54 | -6.48 | $4.66 | -17.28 | -23.76 | +10.08 | -7.20 |
| 2026-08-21 | `MRNA` | 6 | — | $133.11 | +0.00 | $145.13 | -72.12 | -72.12 | -0.00 | -72.12 |
| 2026-08-21 | `AUGO` | 9 | — | $89.10 | +0.00 | $87.26 | +16.56 | +16.56 | -0.00 | +16.56 |
| 2026-08-21 | `SSRM` | 23 | — | $38.40 | +0.00 | $37.77 | +14.49 | +14.49 | -0.00 | +14.49 |
| 2026-08-21 | `ARIS` | 42 | — | $20.90 | +0.00 | $20.86 | +1.68 | +1.68 | -0.00 | +1.68 |
| 2026-08-21 | `NOG` | 32 | — | $27.00 | +0.00 | $27.34 | -10.88 | -10.88 | -0.00 | -10.88 |
| 2026-08-21 | `AMLX` | 22 | — | $39.80 | +0.00 | $38.66 | +24.97 | +24.97 | -0.00 | +24.97 |
| 2026-08-24 | `AEM` | 3 | $216.06 | $217.03 | -2.91 | $217.89 | -2.58 | -5.49 | -37.74 | -40.32 |
| 2026-08-24 | `WYFI` | 31 | $20.72 | $20.01 | +22.01 | $20.78 | -23.87 | -1.86 | +43.09 | +19.22 |
| 2026-08-24 | `TOYO` | 150 | $4.82 | $4.58 | +36.00 | $4.38 | +30.00 | +66.00 | -22.50 | +7.50 |
| 2026-08-24 | `ABCL` | 56 | $11.32 | $10.97 | +19.60 | $10.61 | +20.16 | +39.76 | +47.32 | +67.48 |
| 2026-08-24 | `TEAM` | 3 | $171.81 | $169.30 | +7.53 | $171.33 | -6.09 | +1.44 | +13.80 | +7.71 |
| 2026-08-24 | `AAP` | 14 | $42.58 | $43.05 | -6.58 | $43.63 | -8.12 | -14.70 | +53.20 | +45.08 |
| 2026-08-24 | `WMT` | 6 | $103.70 | $104.14 | -2.64 | $106.49 | -14.10 | -16.74 | +13.44 | -0.66 |
| 2026-08-24 | `AQST` | 144 | $4.66 | $4.67 | -1.44 | $4.80 | -18.72 | -20.16 | -8.64 | -27.36 |
| 2026-08-24 | `MRNA` | 6 | $145.13 | $142.70 | +14.58 | $138.89 | +22.86 | +37.44 | -57.54 | -34.68 |
| 2026-08-24 | `AUGO` | 9 | $87.26 | $88.60 | -12.06 | $87.37 | +11.07 | -0.99 | +4.50 | +15.57 |
| 2026-08-24 | `SSRM` | 23 | $37.77 | $38.32 | -12.65 | $38.61 | -6.67 | -19.32 | +1.84 | -4.83 |
| 2026-08-24 | `ARIS` | 42 | $20.86 | $20.98 | -5.04 | $20.81 | +7.14 | +2.10 | -3.36 | +3.78 |
| 2026-08-24 | `NOG` | 32 | $27.34 | $27.12 | +7.04 | $26.84 | +8.96 | +16.00 | -3.84 | +5.12 |
| 2026-08-24 | `AMLX` | 22 | $38.66 | $38.64 | +0.44 | $38.15 | +10.78 | +11.22 | +25.41 | +36.19 |
| 2026-08-25 | `AEM` | 3 | $217.89 | $212.00 | +17.67 | — | +0.00 | +17.67 | -22.65 | — |
| 2026-08-25 | `WYFI` | 31 | $20.78 | $20.90 | -3.72 | — | +0.00 | -3.72 | +15.50 | — |
| 2026-08-25 | `TOYO` | 150 | $4.38 | $4.42 | -6.00 | — | +0.00 | -6.00 | +1.50 | — |
| 2026-08-25 | `ABCL` | 56 | $10.61 | $11.00 | -21.84 | — | +0.00 | -21.84 | +45.64 | — |
| 2026-08-25 | `TEAM` | 3 | $171.33 | $170.64 | +2.07 | — | +0.00 | +2.07 | +9.78 | — |
| 2026-08-25 | `AAP` | 14 | $43.63 | $43.63 | +0.00 | — | +0.00 | +0.00 | +45.08 | — |
| 2026-08-25 | `WMT` | 6 | $106.49 | $105.58 | +5.46 | — | +0.00 | +5.46 | +4.80 | — |
| 2026-08-25 | `AQST` | 144 | $4.80 | $4.77 | +4.32 | — | +0.00 | +4.32 | -23.04 | — |
| 2026-08-25 | `MRNA` | 6 | $138.89 | $143.50 | -27.66 | $158.83 | -91.98 | -119.64 | -62.34 | -154.32 |
| 2026-08-25 | `AUGO` | 9 | $87.37 | $85.78 | +14.31 | $90.47 | -42.21 | -27.90 | +29.88 | -12.33 |
| 2026-08-25 | `SSRM` | 23 | $38.61 | $37.75 | +19.78 | $39.21 | -33.58 | -13.80 | +14.95 | -18.63 |
| 2026-08-25 | `ARIS` | 42 | $20.81 | $20.45 | +15.12 | $21.18 | -30.66 | -15.54 | +18.90 | -11.76 |
| 2026-08-25 | `NOG` | 32 | $26.84 | $26.06 | +24.96 | $26.42 | -11.52 | +13.44 | +30.08 | +18.56 |
| 2026-08-25 | `AMLX` | 22 | $38.15 | $38.50 | -7.70 | $37.46 | +22.88 | +15.18 | +28.49 | +51.37 |
| 2026-08-25 | `AVAH` | 197 | — | $13.62 | +0.00 | $13.59 | +6.90 | +6.90 | -0.00 | +6.90 |
| 2026-08-25 | `ARE` | 49 | — | $54.51 | +0.00 | $52.90 | +78.89 | +78.89 | -0.00 | +78.89 |
| 2026-08-26 | `MRNA` | 6 | $158.83 | $154.20 | +27.78 | — | +0.00 | +27.78 | -126.54 | — |
| 2026-08-26 | `AUGO` | 9 | $90.47 | $88.24 | +20.07 | — | +0.00 | +20.07 | +7.74 | — |
| 2026-08-26 | `SSRM` | 23 | $39.21 | $38.41 | +18.40 | — | +0.00 | +18.40 | -0.23 | — |
| 2026-08-26 | `ARIS` | 42 | $21.18 | $20.50 | +28.56 | — | +0.00 | +28.56 | +16.80 | — |
| 2026-08-26 | `NOG` | 32 | $26.42 | $26.00 | +13.44 | — | +0.00 | +13.44 | +32.00 | — |
| 2026-08-26 | `AMLX` | 22 | $37.46 | $37.13 | +7.26 | — | +0.00 | +7.26 | +58.63 | — |
| 2026-08-26 | `AVAH` | 197 | $13.59 | $13.65 | -11.82 | $13.62 | +5.91 | -5.91 | -4.93 | +0.99 |
| 2026-08-26 | `ARE` | 49 | $52.90 | $52.77 | +6.37 | $52.97 | -9.80 | -3.43 | +85.26 | +75.46 |
| 2026-08-26 | `BE` | 6 | — | $213.94 | +0.00 | $218.21 | -25.62 | -25.62 | -0.00 | -25.62 |
| 2026-08-26 | `ABCL` | 109 | — | $12.22 | +0.00 | $12.24 | -2.18 | -2.18 | -0.00 | -2.18 |
| 2026-08-26 | `AQST` | 264 | — | $5.08 | +0.00 | $5.39 | -81.84 | -81.84 | -0.00 | -81.84 |
| 2026-08-26 | `NEM` | 10 | — | $132.64 | +0.00 | $131.60 | +10.40 | +10.40 | -0.00 | +10.40 |
| 2026-08-27 | `AVAH` | 197 | $13.62 | $13.62 | +0.00 | $13.82 | -39.40 | -39.40 | +0.99 | -38.42 |
| 2026-08-27 | `ARE` | 49 | $52.97 | $52.45 | +25.48 | $52.28 | +8.33 | +33.81 | +100.94 | +109.27 |
| 2026-08-27 | `BE` | 6 | $218.21 | $227.10 | -53.34 | $217.83 | +55.62 | +2.28 | -78.96 | -23.34 |
| 2026-08-27 | `ABCL` | 109 | $12.24 | $12.25 | -1.09 | $12.40 | -16.35 | -17.44 | -3.27 | -19.62 |
| 2026-08-27 | `AQST` | 264 | $5.39 | $5.39 | +0.00 | $5.16 | +60.72 | +60.72 | -81.84 | -21.12 |
| 2026-08-27 | `NEM` | 10 | $131.60 | $131.02 | +5.80 | $132.29 | -12.70 | -6.90 | +16.20 | +3.50 |
| 2026-08-28 | `AVAH` | 197 | $13.82 | $13.90 | -15.76 | — | +0.00 | -15.76 | -54.18 | — |
| 2026-08-28 | `ARE` | 49 | $52.28 | $52.49 | -10.29 | — | +0.00 | -10.29 | +98.98 | — |
| 2026-08-28 | `BE` | 6 | $217.83 | $215.71 | +12.75 | $210.77 | +29.61 | +42.36 | -10.59 | +19.02 |
| 2026-08-28 | `ABCL` | 109 | $12.40 | $12.30 | +10.36 | $11.35 | +104.10 | +114.46 | -9.26 | +94.83 |
| 2026-08-28 | `AQST` | 264 | $5.16 | $5.11 | +13.20 | $5.02 | +23.76 | +36.96 | -7.92 | +15.84 |
| 2026-08-28 | `NEM` | 10 | $132.29 | $132.35 | -0.60 | $127.98 | +43.70 | +43.10 | +2.90 | +46.60 |
| 2026-08-28 | `SIMO` | 10 | — | $252.24 | +0.00 | $245.81 | +64.30 | +64.30 | -0.00 | +64.30 |
| 2026-08-28 | `FIG` | 88 | — | $30.18 | +0.00 | $28.82 | +119.68 | +119.68 | -0.00 | +119.68 |
| 2026-08-31 | `BE` | 6 | $210.77 | $208.88 | +11.34 | — | +0.00 | +11.34 | +30.36 | — |
| 2026-08-31 | `ABCL` | 109 | $11.35 | $11.10 | +27.25 | — | +0.00 | +27.25 | +122.08 | — |
| 2026-08-31 | `AQST` | 264 | $5.02 | $4.97 | +11.88 | — | +0.00 | +11.88 | +27.72 | — |
| 2026-08-31 | `NEM` | 10 | $127.98 | $127.45 | +5.30 | — | +0.00 | +5.30 | +51.90 | — |
| 2026-08-31 | `SIMO` | 10 | $245.81 | $247.05 | -12.40 | $246.84 | +2.10 | -10.30 | +51.90 | +54.00 |
| 2026-08-31 | `FIG` | 88 | $28.82 | $27.60 | +107.36 | $27.49 | +9.68 | +117.04 | +227.04 | +236.72 |
| 2026-09-01 | `SIMO` | 10 | $246.84 | $240.09 | +67.50 | $237.35 | +27.40 | +94.90 | +121.50 | +148.90 |
| 2026-09-01 | `FIG` | 88 | $27.49 | $27.06 | +37.84 | $27.20 | -12.32 | +25.52 | +274.56 | +262.24 |
| 2026-09-02 | `SIMO` | 10 | $237.35 | $235.71 | +16.40 | — | +0.00 | +16.40 | +165.30 | — |
| 2026-09-02 | `FIG` | 88 | $27.20 | $26.78 | +36.96 | — | +0.00 | +36.96 | +299.20 | — |
| 2026-09-03 | `OPK` | 3322 | — | $1.71 | +0.00 | $1.61 | +332.20 | +332.20 | -0.00 | +332.20 |
| 2026-09-04 | `OPK` | 3322 | $1.61 | $1.59 | +66.44 | $1.64 | -166.10 | -99.66 | +398.64 | +232.54 |
| 2026-09-04 | `GSM` | 627 | — | $4.67 | +0.00 | $4.67 | +0.00 | +0.00 | -0.00 | -0.00 |
| 2026-09-04 | `PIPR` | 38 | — | $76.55 | +0.00 | $77.04 | -18.62 | -18.62 | -0.00 | -18.62 |
| 2026-09-08 | `OPK` | 3322 | $1.64 | $1.63 | +33.22 | $1.59 | +132.88 | +166.10 | +265.76 | +398.64 |
| 2026-09-08 | `GSM` | 627 | $4.67 | $4.75 | -50.16 | $4.52 | +144.21 | +94.05 | -50.16 | +94.05 |
| 2026-09-08 | `PIPR` | 38 | $77.04 | $76.64 | +15.20 | $77.34 | -26.60 | -11.40 | -3.42 | -30.02 |
| 2026-09-09 | `OPK` | 3322 | $1.59 | $1.58 | +33.22 | — | +0.00 | +33.22 | +431.86 | — |
| 2026-09-09 | `GSM` | 627 | $4.52 | $4.52 | +0.00 | $4.49 | +18.81 | +18.81 | +94.05 | +112.86 |
| 2026-09-09 | `PIPR` | 38 | $77.34 | $77.24 | +3.80 | $76.96 | +10.64 | +14.44 | -26.22 | -15.58 |
| 2026-09-10 | `GSM` | 627 | $4.49 | $4.36 | +81.51 | — | +0.00 | +81.51 | +194.37 | — |
| 2026-09-10 | `PIPR` | 38 | $76.96 | $76.79 | +6.46 | — | +0.00 | +6.46 | -9.12 | — |
| 2026-09-11 | `QRVO` | 13 | — | $112.83 | +0.00 | $116.65 | -49.60 | -49.60 | -0.00 | -49.60 |
| 2026-09-11 | `MYGN` | 440 | — | $3.37 | +0.00 | $3.42 | -22.00 | -22.00 | -0.00 | -22.00 |
| 2026-09-11 | `BKV` | 59 | — | $24.97 | +0.00 | $24.23 | +43.66 | +43.66 | -0.00 | +43.66 |
| 2026-09-11 | `INGM` | 55 | — | $26.62 | +0.00 | $27.55 | -51.15 | -51.15 | -0.00 | -51.15 |
| 2026-09-14 | `QRVO` | 13 | $116.65 | $114.11 | +33.02 | $107.98 | +79.69 | +112.71 | -16.58 | +63.11 |
| 2026-09-14 | `MYGN` | 440 | $3.42 | $3.43 | -4.40 | $3.79 | -158.40 | -162.80 | -26.40 | -184.80 |
| 2026-09-14 | `BKV` | 59 | $24.23 | $24.26 | -1.77 | $23.82 | +25.96 | +24.19 | +41.89 | +67.85 |
| 2026-09-14 | `INGM` | 55 | $27.55 | $26.89 | +36.30 | $26.85 | +2.20 | +38.50 | -14.85 | -12.65 |
| 2026-09-15 | `QRVO` | 13 | $107.98 | $108.40 | -5.46 | $118.06 | -125.58 | -131.04 | +57.65 | -67.93 |
| 2026-09-15 | `MYGN` | 440 | $3.79 | $3.80 | -4.40 | $3.88 | -35.20 | -39.60 | -189.20 | -224.40 |
| 2026-09-15 | `BKV` | 59 | $23.82 | $24.25 | -25.37 | $24.35 | -5.90 | -31.27 | +42.48 | +36.58 |
| 2026-09-15 | `INGM` | 55 | $26.85 | $26.91 | -3.30 | $26.32 | +32.45 | +29.15 | -15.95 | +16.50 |
| 2026-09-16 | `QRVO` | 13 | $118.06 | $118.18 | -1.56 | — | +0.00 | -1.56 | -69.49 | — |
| 2026-09-16 | `MYGN` | 440 | $3.88 | $3.75 | +57.20 | — | +0.00 | +57.20 | -167.20 | — |
| 2026-09-16 | `BKV` | 59 | $24.35 | $24.42 | -4.13 | — | +0.00 | -4.13 | +32.45 | — |
| 2026-09-16 | `INGM` | 55 | $26.32 | $26.05 | +14.85 | — | +0.00 | +14.85 | +31.35 | — |
| 2026-09-16 | `TRMD` | 162 | — | $35.90 | +0.00 | $36.60 | -113.40 | -113.40 | -0.00 | -113.40 |
| 2026-09-17 | `TRMD` | 162 | $36.60 | $36.52 | +12.96 | $36.63 | -17.82 | -4.86 | -100.44 | -118.26 |
| 2026-09-18 | `TRMD` | 162 | $36.63 | $37.71 | -174.96 | $38.23 | -84.24 | -259.20 | -293.22 | -377.46 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +51.40 | EU, LUNR, OWL, SVV | — | $14,960.38 | $10,030.54 | EU×1059, LUNR×65, OWL×98, SVV×114 |
| 2026-08-17 | +2.25 | $14,960.38 | EU×1059, LUNR×65, OWL×98, SVV×114 | $9,979.12 | -51.42 | +242.28 | VERI, HIVE, BIRK | — | $19,914.55 | $10,192.99 | EU×1059, LUNR×65, OWL×98, SVV×114, VERI×1446, HIVE×552, BIRK×42 |
| 2026-08-18 | -6.20 | $19,914.55 | EU×1059, LUNR×65, OWL×98, SVV×114, VERI×1446, HIVE×552, BIRK×42 | $10,384.79 | +191.80 | +268.25 | — | — | $19,914.55 | $10,653.04 | EU×1059, LUNR×65, OWL×98, SVV×114, VERI×1446, HIVE×552, BIRK×42 |
| 2026-08-19 | -7.20 | $19,914.55 | EU×1059, LUNR×65, OWL×98, SVV×114, VERI×1446, HIVE×552, BIRK×42 | $10,623.14 | -29.90 | +114.52 | — | EU, OWL, SVV | $16,394.12 | $10,719.39 | LUNR×65, VERI×1446, HIVE×552, BIRK×42 |
| 2026-08-20 | +1.12 | $16,394.12 | LUNR×65, VERI×1446, HIVE×552, BIRK×42 | $10,682.78 | -36.61 | +76.85 | AEM, WYFI, TOYO, ABCL, TEAM, AAP, WMT, AQST | LUNR, VERI, HIVE, BIRK | $15,718.23 | $10,712.47 | AEM×3, WYFI×31, TOYO×150, ABCL×56, TEAM×3, AAP×14, WMT×6, AQST×144 |
| 2026-08-21 | +3.25 | $15,718.23 | AEM×3, WYFI×31, TOYO×150, ABCL×56, TEAM×3, AAP×14, WMT×6, AQST×144 | $10,659.37 | -53.10 | -18.65 | MRNA, AUGO, SSRM, ARIS, NOG, AMLX | — | $20,806.68 | $10,628.12 | AEM×3, WYFI×31, TOYO×150, ABCL×56, TEAM×3, AAP×14, WMT×6, AQST×144, MRNA×6, AUGO×9, SSRM×23, ARIS×42, NOG×32, AMLX×22 |
| 2026-08-24 | -5.17 | $20,806.68 | AEM×3, WYFI×31, TOYO×150, ABCL×56, TEAM×3, AAP×14, WMT×6, AQST×144, MRNA×6, AUGO×9, SSRM×23, ARIS×42, NOG×32, AMLX×22 | $10,692.00 | +63.88 | +30.82 | — | — | $20,806.68 | $10,722.82 | AEM×3, WYFI×31, TOYO×150, ABCL×56, TEAM×3, AAP×14, WMT×6, AQST×144, MRNA×6, AUGO×9, SSRM×23, ARIS×42, NOG×32, AMLX×22 |
| 2026-08-25 | +1.80 | $20,806.68 | AEM×3, WYFI×31, TOYO×150, ABCL×56, TEAM×3, AAP×14, WMT×6, AQST×144, MRNA×6, AUGO×9, SSRM×23, ARIS×42, NOG×32, AMLX×22 | $10,759.59 | +36.77 | -101.28 | AVAH, ARE | AEM, WYFI, TOYO, ABCL, TEAM, AAP, WMT, AQST | $21,133.70 | $10,636.21 | MRNA×6, AUGO×9, SSRM×23, ARIS×42, NOG×32, AMLX×22, AVAH×197, ARE×49 |
| 2026-08-26 | +2.02 | $21,133.70 | MRNA×6, AUGO×9, SSRM×23, ARIS×42, NOG×32, AMLX×22, AVAH×197, ARE×49 | $10,746.27 | +110.06 | -103.13 | BE, ABCL, AQST, NEM | MRNA, AUGO, SSRM, ARIS, NOG, AMLX | $21,281.83 | $10,620.78 | AVAH×197, ARE×49, BE×6, ABCL×109, AQST×264, NEM×10 |
| 2026-08-27 | — | $21,281.83 | AVAH×197, ARE×49, BE×6, ABCL×109, AQST×264, NEM×10 | $10,597.63 | -23.15 | +56.22 | — | — | $21,281.83 | $10,653.85 | AVAH×197, ARE×49, BE×6, ABCL×109, AQST×264, NEM×10 |
| 2026-08-28 | +0.75 | $21,281.83 | AVAH×197, ARE×49, BE×6, ABCL×109, AQST×264, NEM×10 | $10,663.51 | +9.66 | +385.15 | SIMO, FIG | AVAH, ARE | $21,140.56 | $11,039.45 | BE×6, ABCL×109, AQST×264, NEM×10, SIMO×10, FIG×88 |
| 2026-08-31 | -5.85 | $21,140.56 | BE×6, ABCL×109, AQST×264, NEM×10, SIMO×10, FIG×88 | $11,190.18 | +150.73 | +11.78 | — | BE, ABCL, AQST, NEM | $16,079.73 | $11,192.21 | SIMO×10, FIG×88 |
| 2026-09-01 | -6.30 | $16,079.73 | SIMO×10, FIG×88 | $11,297.55 | +105.34 | +15.08 | — | — | $16,079.73 | $11,312.63 | SIMO×10, FIG×88 |
| 2026-09-02 | -3.83 | $16,079.73 | SIMO×10, FIG×88 | $11,365.99 | +53.36 | +0.00 | — | SIMO, FIG | $11,361.72 | $11,361.72 | — |
| 2026-09-03 | -0.90 | $11,361.72 | — | $11,361.72 | -0.00 | +332.20 | OPK | — | $16,998.73 | $11,650.31 | OPK×3322 |
| 2026-09-04 | +2.25 | $16,998.73 | OPK×3322 | $11,716.75 | +66.44 | -184.72 | GSM, PIPR | — | $22,825.21 | $11,521.52 | OPK×3322, GSM×627, PIPR×38 |
| 2026-09-08 | -11.47 | $22,825.21 | OPK×3322, GSM×627, PIPR×38 | $11,519.78 | -1.74 | +250.49 | — | — | $22,825.21 | $11,770.27 | OPK×3322, GSM×627, PIPR×38 |
| 2026-09-09 | -13.95 | $22,825.21 | OPK×3322, GSM×627, PIPR×38 | $11,807.29 | +37.02 | +29.45 | — | OPK | $17,533.60 | $11,793.89 | GSM×627, PIPR×38 |
| 2026-09-10 | -13.28 | $17,533.60 | GSM×627, PIPR×38 | $11,881.86 | +87.97 | +0.00 | — | GSM, PIPR | $11,871.66 | $11,871.66 | — |
| 2026-09-11 | +0.50 | $11,871.66 | — | $11,871.66 | +0.00 | -79.09 | QRVO, MYGN, BKV, INGM | — | $17,746.31 | $11,780.24 | QRVO×13, MYGN×440, BKV×59, INGM×55 |
| 2026-09-14 | -11.00 | $17,746.31 | QRVO×13, MYGN×440, BKV×59, INGM×55 | $11,843.39 | +63.15 | -50.55 | — | — | $17,746.31 | $11,792.84 | QRVO×13, MYGN×440, BKV×59, INGM×55 |
| 2026-09-15 | -3.84 | $17,746.31 | QRVO×13, MYGN×440, BKV×59, INGM×55 | $11,754.31 | -38.53 | -134.23 | — | — | $17,746.31 | $11,620.08 | QRVO×13, MYGN×440, BKV×59, INGM×55 |
| 2026-09-16 | +5.30 | $17,746.31 | QRVO×13, MYGN×440, BKV×59, INGM×55 | $11,686.44 | +66.36 | -113.40 | TRMD | QRVO, MYGN, BKV, INGM | $17,487.50 | $11,558.30 | TRMD×162 |
| 2026-09-17 | +7.38 | $17,487.50 | TRMD×162 | $11,571.26 | +12.96 | -17.82 | — | — | $17,487.50 | $11,553.44 | TRMD×162 |
| 2026-09-18 | +4.86 | $17,487.50 | TRMD×162 | $11,378.48 | -174.96 | -84.24 | — | — | $17,487.50 | $11,294.24 | TRMD×162 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 1059 | $1.18 | $13.88 | — | $11,235.74 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 65 | $19.17 | $2.24 | — | $12,479.55 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 98 | $12.70 | $2.34 | — | $13,721.31 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+12.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🔴 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `SVV` | 114 | $10.89 | $2.39 | — | $14,960.38 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list oppset; 🔵; ret5=-0.7; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,960.38 | ▲ close $10,030.54 vs 09:30 $10,000.00 (session +51.40) | 16:00 close · cash $14,960.38 · equity $10,030.54 vs 09:30 $10,000.00 (+30.54; session marks +51.40) · 4 name(s) marked open→close (per-name table). EU×1059 09:30 $1.18 → close $1.21 -31.77; LUNR×65 09:30 $19.17 → close $19.01 +10.40; OWL×98 09:30 $12.70 → close $12.22 +46.55; SVV×114 09:30 $10.89 → close $10.66 +26.22 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,960.38 | ▼ 09:30 equity $9,979.12 vs yday $10,030.54 (-51.42) | 09:30 open · cash $14,960.38 (unchanged overnight, no fees) · equity $9,979.12 vs prior close $10,030.54 (-51.42) · 4 name(s) re-marked at the open (per-name table). EU×1059 yday $1.21 → 09:30 $1.21 -0.00; LUNR×65 yday $19.01 → 09:30 $20.25 -80.60; OWL×98 yday $12.22 → 09:30 $12.12 +9.80; SVV×114 yday $10.66 → 09:30 $10.49 +19.38 | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 1446 | $1.15 | $18.95 | — | $16,604.33 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_mover; ⚪; ret5=-12.2; leftover $1663.19 | join🟡 sector🟢 gen🟢 news🔴 judge🟢 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 552 | $3.01 | $7.27 | — | $18,258.58 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list earn_react; ⚪; ret5=-5.3; leftover $1663.19 | join🟢 sector🟢 gen🟢 news🔴 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `BIRK` | 42 | $39.48 | $2.18 | — | $19,914.55 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list oppset; ret5=+2.3; leftover $1663.19 | join🟡 sector🔴 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,914.55 | ▲ close $10,192.99 vs 09:30 $9,979.12 (session +242.28) | 16:00 close · cash $19,914.55 · equity $10,192.99 vs 09:30 $9,979.12 (+213.87; session marks +242.28) · 7 name(s) marked open→close (per-name table). EU×1059 09:30 $1.21 → close $1.13 +84.72; LUNR×65 09:30 $20.25 → close $20.38 -8.45; OWL×98 09:30 $12.12 → close $11.66 +45.08; SVV×114 09:30 $10.49 → close $10.56 -7.98; VERI×1446 09:30 $1.15 → close $1.08 +93.99; HIVE×552 09:30 $3.01 → close $3.07 -33.12; BIRK×42 09:30 $39.48 → close $37.86 +68.04 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,914.55 | ▲ 09:30 equity $10,384.79 vs yday $10,192.99 (+191.80) | 09:30 open · cash $19,914.55 (unchanged overnight, no fees) · equity $10,384.79 vs prior close $10,192.99 (+191.80) · 7 name(s) re-marked at the open (per-name table). EU×1059 yday $1.13 → 09:30 $1.13 -0.00; LUNR×65 yday $20.38 → 09:30 $19.31 +69.55; OWL×98 yday $11.66 → 09:30 $11.54 +11.76; SVV×114 yday $10.56 → 09:30 $10.49 +7.98; VERI×1446 yday $1.08 → 09:30 $1.05 +50.61; HIVE×552 yday $3.07 → 09:30 $2.96 +60.72; BIRK×42 yday $37.86 → 09:30 $38.07 -8.82 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,914.55 | ▲ close $10,653.04 vs 09:30 $10,384.79 (session +268.25) | 16:00 close · cash $19,914.55 · equity $10,653.04 vs 09:30 $10,384.79 (+268.25; session marks +268.25) · 7 name(s) marked open→close (per-name table). EU×1059 09:30 $1.13 → close $1.07 +63.54; LUNR×65 09:30 $19.31 → close $19.31 -0.00; OWL×98 09:30 $11.54 → close $11.59 -4.90; SVV×114 09:30 $10.49 → close $10.53 -4.56; VERI×1446 09:30 $1.05 → close $0.99 +79.53; HIVE×552 09:30 $2.96 → close $2.78 +99.36; BIRK×42 09:30 $38.07 → close $37.23 +35.28 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,914.55 | ▼ 09:30 equity $10,623.14 vs yday $10,653.04 (-29.90) | 09:30 open · cash $19,914.55 (unchanged overnight, no fees) · equity $10,623.14 vs prior close $10,653.04 (-29.90) · 7 name(s) re-marked at the open (per-name table). EU×1059 yday $1.07 → 09:30 $1.07 -0.00; LUNR×65 yday $19.31 → 09:30 $18.98 +21.45; OWL×98 yday $11.59 → 09:30 $11.75 -15.68; SVV×114 yday $10.53 → 09:30 $10.68 -17.10; VERI×1446 yday $0.99 → 09:30 $1.00 -7.23; HIVE×552 yday $2.78 → 09:30 $2.78 -0.00; BIRK×42 yday $37.23 → 09:30 $37.50 -11.34 | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 1059 | $1.07 | $13.66 | $+88.95 | $18,767.76 | ▲ +88.95 after sell → book $10,609.48; vs 09:30 mark -13.66 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 98 | $11.75 | $2.28 | $+87.98 | $17,613.98 | ▲ +87.98 after sell → book $10,607.20; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `SVV` | 114 | $10.68 | $2.33 | $+19.21 | $16,394.12 | ▲ +19.21 after sell → book $10,604.86; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,394.12 | ▲ close $10,719.39 vs 09:30 $10,623.14 (session +114.52) | 16:00 close · cash $16,394.12 · equity $10,719.39 vs 09:30 $10,623.14 (+96.25; session marks +114.52) · 4 name(s) marked open→close (per-name table). LUNR×65 09:30 $18.98 → close $18.52 +29.90; VERI×1446 09:30 $1.00 → close $0.97 +49.16; HIVE×552 09:30 $2.78 → close $2.82 -22.08; BIRK×42 09:30 $37.50 → close $36.13 +57.54 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,394.12 | ▼ 09:30 equity $10,682.78 vs yday $10,719.39 (-36.61) | 09:30 open · cash $16,394.12 (unchanged overnight, no fees) · equity $10,682.78 vs prior close $10,719.39 (-36.61) · 4 name(s) re-marked at the open (per-name table). LUNR×65 yday $18.52 → 09:30 $18.13 +25.35; VERI×1446 yday $0.97 → 09:30 $0.96 +4.34; HIVE×552 yday $2.82 → 09:30 $2.95 -71.76; BIRK×42 yday $36.13 → 09:30 $36.00 +5.46 | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 65 | $18.13 | $2.19 | $+63.18 | $15,213.49 | ▲ +63.18 after sell → book $10,680.59; vs 09:30 mark -2.19 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 1446 | $0.96 | $18.26 | $+233.19 | $13,802.73 | ▲ +233.19 after sell → book $10,662.33; vs 09:30 mark -18.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 552 | $2.95 | $7.12 | $+18.73 | $12,167.21 | ▲ +18.73 after sell → book $10,655.21; vs 09:30 mark -7.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `BIRK` | 42 | $36.00 | $2.12 | $+141.86 | $10,653.09 | ▲ +141.86 after sell → book $10,653.09; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $11,264.41 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $665.82 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 31 | $21.40 | $2.12 | — | $11,925.68 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_mover,oppset; 🔵; ret5=-25.2; leftover $665.82 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 150 | $4.43 | $2.49 | — | $12,587.69 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_mover; 🔵; ret5=-23.1; leftover $665.82 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 56 | $11.81 | $2.20 | — | $13,247.14 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $665.82 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 3 | $173.90 | $2.03 | — | $13,766.80 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+12.2; leftover $665.82 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 14 | $46.85 | $2.07 | — | $14,420.63 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list earn_react; 🔵; ret5=+5.0; leftover $665.82 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 6 | $106.38 | $2.05 | — | $15,056.87 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list earn_react; 🔵; ret5=-1.7; leftover $665.82 | join🟢 sector🟡 gen🟢 news🔴 digest🟡 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 144 | $4.61 | $2.47 | — | $15,718.23 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list mover_buy; 🔵; ⚪; ret5=+8.5; leftover $665.82 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,718.23 | ▲ close $10,712.47 vs 09:30 $10,682.78 (session +76.85) | 16:00 close · cash $15,718.23 · equity $10,712.47 vs 09:30 $10,682.78 (+29.69; session marks +76.85) · 8 name(s) marked open→close (per-name table). AEM×3 09:30 $204.45 → close $212.04 -22.77; WYFI×31 09:30 $21.40 → close $21.16 +7.44; TOYO×150 09:30 $4.43 → close $4.51 -12.75; ABCL×56 09:30 $11.81 → close $11.57 +13.72; TEAM×3 09:30 $173.90 → close $174.91 -3.03; AAP×14 09:30 $46.85 → close $42.39 +62.44; WMT×6 09:30 $106.38 → close $103.84 +15.24; AQST×144 09:30 $4.61 → close $4.50 +16.56 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,718.23 | ▼ 09:30 equity $10,659.37 vs yday $10,712.47 (-53.10) | 09:30 open · cash $15,718.23 (unchanged overnight, no fees) · equity $10,659.37 vs prior close $10,712.47 (-53.10) · 8 name(s) re-marked at the open (per-name table). AEM×3 yday $212.04 → 09:30 $216.30 -12.78; WYFI×31 yday $21.16 → 09:30 $21.54 -11.78; TOYO×150 yday $4.51 → 09:30 $4.68 -24.75; ABCL×56 yday $11.57 → 09:30 $11.57 -0.00; TEAM×3 yday $174.91 → 09:30 $174.22 +2.07; AAP×14 yday $42.39 → 09:30 $42.41 -0.28; WMT×6 yday $103.84 → 09:30 $103.69 +0.90; AQST×144 yday $4.50 → 09:30 $4.54 -6.48 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 6 | $133.11 | $2.05 | — | $16,514.84 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_mover,oppset; 🔵; ⚪; ret5=+109.5; leftover $888.28 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 9 | $89.10 | $2.06 | — | $17,314.68 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+15.8; leftover $888.28 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 23 | $38.40 | $2.10 | — | $18,195.78 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+15.8; leftover $888.28 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 42 | $20.90 | $2.16 | — | $19,071.42 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $888.28 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 32 | $27.00 | $2.13 | — | $19,933.29 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; ret5=+10.1; leftover $888.28 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `AMLX` | 22 | $39.80 | $2.10 | — | $20,806.68 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list oppset; ⚪; ret5=+76.4; leftover $888.28 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,806.68 | ▼ close $10,628.12 vs 09:30 $10,659.37 (session -18.65) | 16:00 close · cash $20,806.68 · equity $10,628.12 vs 09:30 $10,659.37 (-31.25; session marks -18.65) · 14 name(s) marked open→close (per-name table). AEM×3 09:30 $216.30 → close $216.06 +0.72; WYFI×31 09:30 $21.54 → close $20.72 +25.42; TOYO×150 09:30 $4.68 → close $4.82 -21.00; ABCL×56 09:30 $11.57 → close $11.32 +14.00; TEAM×3 09:30 $174.22 → close $171.81 +7.23; AAP×14 09:30 $42.41 → close $42.58 -2.38; WMT×6 09:30 $103.69 → close $103.70 -0.06; AQST×144 09:30 $4.54 → close $4.66 -17.28; MRNA×6 09:30 $133.11 → close $145.13 -72.12; AUGO×9 09:30 $89.10 → close $87.26 +16.56; SSRM×23 09:30 $38.40 → close $37.77 +14.49; ARIS×42 09:30 $20.90 → close $20.86 +1.68; NOG×32 09:30 $27.00 → close $27.34 -10.88; AMLX×22 09:30 $39.80 → close $38.66 +24.97 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,806.68 | ▲ 09:30 equity $10,692.00 vs yday $10,628.12 (+63.88) | 09:30 open · cash $20,806.68 (unchanged overnight, no fees) · equity $10,692.00 vs prior close $10,628.12 (+63.88) · 14 name(s) re-marked at the open (per-name table). AEM×3 yday $216.06 → 09:30 $217.03 -2.91; WYFI×31 yday $20.72 → 09:30 $20.01 +22.01; TOYO×150 yday $4.82 → 09:30 $4.58 +36.00; ABCL×56 yday $11.32 → 09:30 $10.97 +19.60; TEAM×3 yday $171.81 → 09:30 $169.30 +7.53; AAP×14 yday $42.58 → 09:30 $43.05 -6.58; WMT×6 yday $103.70 → 09:30 $104.14 -2.64; AQST×144 yday $4.66 → 09:30 $4.67 -1.44; MRNA×6 yday $145.13 → 09:30 $142.70 +14.58; AUGO×9 yday $87.26 → 09:30 $88.60 -12.06; SSRM×23 yday $37.77 → 09:30 $38.32 -12.65; ARIS×42 yday $20.86 → 09:30 $20.98 -5.04; NOG×32 yday $27.34 → 09:30 $27.12 +7.04; AMLX×22 yday $38.66 → 09:30 $38.64 +0.44 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,806.68 | ▲ close $10,722.82 vs 09:30 $10,692.00 (session +30.82) | 16:00 close · cash $20,806.68 · equity $10,722.82 vs 09:30 $10,692.00 (+30.82; session marks +30.82) · 14 name(s) marked open→close (per-name table). AEM×3 09:30 $217.03 → close $217.89 -2.58; WYFI×31 09:30 $20.01 → close $20.78 -23.87; TOYO×150 09:30 $4.58 → close $4.38 +30.00; ABCL×56 09:30 $10.97 → close $10.61 +20.16; TEAM×3 09:30 $169.30 → close $171.33 -6.09; AAP×14 09:30 $43.05 → close $43.63 -8.12; WMT×6 09:30 $104.14 → close $106.49 -14.10; AQST×144 09:30 $4.67 → close $4.80 -18.72; MRNA×6 09:30 $142.70 → close $138.89 +22.86; AUGO×9 09:30 $88.60 → close $87.37 +11.07; SSRM×23 09:30 $38.32 → close $38.61 -6.67; ARIS×42 09:30 $20.98 → close $20.81 +7.14; NOG×32 09:30 $27.12 → close $26.84 +8.96; AMLX×22 09:30 $38.64 → close $38.15 +10.78 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,806.68 | ▲ 09:30 equity $10,759.59 vs yday $10,722.82 (+36.77) | 09:30 open · cash $20,806.68 (unchanged overnight, no fees) · equity $10,759.59 vs prior close $10,722.82 (+36.77) · 14 name(s) re-marked at the open (per-name table). AEM×3 yday $217.89 → 09:30 $212.00 +17.67; WYFI×31 yday $20.78 → 09:30 $20.90 -3.72; TOYO×150 yday $4.38 → 09:30 $4.42 -6.00; ABCL×56 yday $10.61 → 09:30 $11.00 -21.84; TEAM×3 yday $171.33 → 09:30 $170.64 +2.07; AAP×14 yday $43.63 → 09:30 $43.63 -0.00; WMT×6 yday $106.49 → 09:30 $105.58 +5.46; AQST×144 yday $4.80 → 09:30 $4.77 +4.32; MRNA×6 yday $138.89 → 09:30 $143.50 -27.66; AUGO×9 yday $87.37 → 09:30 $85.78 +14.31; SSRM×23 yday $38.61 → 09:30 $37.75 +19.78; ARIS×42 yday $20.81 → 09:30 $20.45 +15.12; NOG×32 yday $26.84 → 09:30 $26.06 +24.96; AMLX×22 yday $38.15 → 09:30 $38.50 -7.70 | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 3 | $212.00 | $2.00 | $-26.68 | $20,168.68 | ▼ -26.68 after sell → book $10,757.59; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 31 | $20.90 | $2.08 | $+11.30 | $19,518.70 | ▲ +11.30 after sell → book $10,755.51; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 150 | $4.42 | $2.44 | $-3.43 | $18,853.26 | ▼ -3.43 after sell → book $10,753.07; vs 09:30 mark -2.44 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 56 | $11.00 | $2.16 | $+41.29 | $18,235.10 | ▲ +41.29 after sell → book $10,750.91; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 3 | $170.64 | $2.00 | $+5.75 | $17,721.18 | ▲ +5.75 after sell → book $10,748.91; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 14 | $43.63 | $2.03 | $+40.98 | $17,108.33 | ▲ +40.98 after sell → book $10,746.88; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 6 | $105.58 | $2.01 | $+0.75 | $16,472.84 | ▲ +0.75 after sell → book $10,744.87; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 144 | $4.77 | $2.42 | $-27.94 | $15,783.54 | ▼ -27.94 after sell → book $10,742.45; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 197 | $13.62 | $2.71 | — | $18,464.96 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $2685.61 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 49 | $54.51 | $2.24 | — | $21,133.70 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; ret5=+15.1; leftover $2685.61 | join🔴 sector🟡 gen🟡 news🔴 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,133.70 | ▼ close $10,636.21 vs 09:30 $10,759.59 (session -101.28) | 16:00 close · cash $21,133.70 · equity $10,636.21 vs 09:30 $10,759.59 (-123.38; session marks -101.28) · 8 name(s) marked open→close (per-name table). MRNA×6 09:30 $143.50 → close $158.83 -91.98; AUGO×9 09:30 $85.78 → close $90.47 -42.21; SSRM×23 09:30 $37.75 → close $39.21 -33.58; ARIS×42 09:30 $20.45 → close $21.18 -30.66; NOG×32 09:30 $26.06 → close $26.42 -11.52; AMLX×22 09:30 $38.50 → close $37.46 +22.88; AVAH×197 09:30 $13.62 → close $13.59 +6.90; ARE×49 09:30 $54.51 → close $52.90 +78.89 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,133.70 | ▲ 09:30 equity $10,746.27 vs yday $10,636.21 (+110.06) | 09:30 open · cash $21,133.70 (unchanged overnight, no fees) · equity $10,746.27 vs prior close $10,636.21 (+110.06) · 8 name(s) re-marked at the open (per-name table). MRNA×6 yday $158.83 → 09:30 $154.20 +27.78; AUGO×9 yday $90.47 → 09:30 $88.24 +20.07; SSRM×23 yday $39.21 → 09:30 $38.41 +18.40; ARIS×42 yday $21.18 → 09:30 $20.50 +28.56; NOG×32 yday $26.42 → 09:30 $26.00 +13.44; AMLX×22 yday $37.46 → 09:30 $37.13 +7.26; AVAH×197 yday $13.59 → 09:30 $13.65 -11.82; ARE×49 yday $52.90 → 09:30 $52.77 +6.37 | — |
| 2026-08-26 09:30 ET | **COVER** | `MRNA` | 6 | $154.20 | $2.01 | $-130.60 | $20,206.50 | ▼ -130.60 after sell → book $10,744.27; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 9 | $88.24 | $2.02 | $+3.66 | $19,410.32 | ▲ +3.66 after sell → book $10,742.25; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 23 | $38.41 | $2.06 | $-4.39 | $18,524.83 | ▼ -4.39 after sell → book $10,740.19; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 42 | $20.50 | $2.12 | $+12.52 | $17,661.71 | ▲ +12.52 after sell → book $10,738.07; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 32 | $26.00 | $2.09 | $+27.78 | $16,827.63 | ▲ +27.78 after sell → book $10,735.99; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AMLX` | 22 | $37.13 | $2.06 | $+54.47 | $16,008.71 | ▲ +54.47 after sell → book $10,733.93; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 6 | $213.94 | $2.06 | — | $17,290.29 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $1341.74 | join🟢 sector🔴 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 109 | $12.22 | $2.38 | — | $18,619.89 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+12.4; leftover $1341.74 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 264 | $5.08 | $3.50 | — | $19,957.51 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+17.6; leftover $1341.74 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 10 | $132.64 | $2.08 | — | $21,281.83 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+16.5; leftover $1341.74 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,281.83 | ▼ close $10,620.78 vs 09:30 $10,746.27 (session -103.13) | 16:00 close · cash $21,281.83 · equity $10,620.78 vs 09:30 $10,746.27 (-125.49; session marks -103.13) · 6 name(s) marked open→close (per-name table). AVAH×197 09:30 $13.65 → close $13.62 +5.91; ARE×49 09:30 $52.77 → close $52.97 -9.80; BE×6 09:30 $213.94 → close $218.21 -25.62; ABCL×109 09:30 $12.22 → close $12.24 -2.18; AQST×264 09:30 $5.08 → close $5.39 -81.84; NEM×10 09:30 $132.64 → close $131.60 +10.40 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,281.83 | ▼ 09:30 equity $10,597.63 vs yday $10,620.78 (-23.15) | 09:30 open · cash $21,281.83 (unchanged overnight, no fees) · equity $10,597.63 vs prior close $10,620.78 (-23.15) · 6 name(s) re-marked at the open (per-name table). AVAH×197 yday $13.62 → 09:30 $13.62 -0.00; ARE×49 yday $52.97 → 09:30 $52.45 +25.48; BE×6 yday $218.21 → 09:30 $227.10 -53.34; ABCL×109 yday $12.24 → 09:30 $12.25 -1.09; AQST×264 yday $5.39 → 09:30 $5.39 -0.00; NEM×10 yday $131.60 → 09:30 $131.02 +5.80 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,281.83 | ▲ close $10,653.85 vs 09:30 $10,597.63 (session +56.22) | 16:00 close · cash $21,281.83 · equity $10,653.85 vs 09:30 $10,597.63 (+56.22; session marks +56.22) · 6 name(s) marked open→close (per-name table). AVAH×197 09:30 $13.62 → close $13.82 -39.40; ARE×49 09:30 $52.45 → close $52.28 +8.33; BE×6 09:30 $227.10 → close $217.83 +55.62; ABCL×109 09:30 $12.25 → close $12.40 -16.35; AQST×264 09:30 $5.39 → close $5.16 +60.72; NEM×10 09:30 $131.02 → close $132.29 -12.70 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,281.83 | ▲ 09:30 equity $10,663.51 vs yday $10,653.85 (+9.66) | 09:30 open · cash $21,281.83 (unchanged overnight, no fees) · equity $10,663.51 vs prior close $10,653.85 (+9.66) · 6 name(s) re-marked at the open (per-name table). AVAH×197 yday $13.82 → 09:30 $13.90 -15.76; ARE×49 yday $52.28 → 09:30 $52.49 -10.29; BE×6 yday $217.83 → 09:30 $215.71 +12.75; ABCL×109 yday $12.40 → 09:30 $12.30 +10.36; AQST×264 yday $5.16 → 09:30 $5.11 +13.20; NEM×10 yday $132.29 → 09:30 $132.35 -0.60 | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 197 | $13.90 | $2.58 | $-59.46 | $18,540.95 | ▼ -59.46 after sell → book $10,660.93; vs 09:30 mark -2.58 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 49 | $52.49 | $2.14 | $+94.60 | $15,966.80 | ▲ +94.60 after sell → book $10,658.79; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 10 | $252.24 | $2.12 | — | $18,487.09 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $2664.70 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 88 | $30.18 | $2.36 | — | $21,140.56 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; ret5=+12.1; leftover $2664.70 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,140.56 | ▲ close $11,039.45 vs 09:30 $10,663.51 (session +385.15) | 16:00 close · cash $21,140.56 · equity $11,039.45 vs 09:30 $10,663.51 (+375.94; session marks +385.15) · 6 name(s) marked open→close (per-name table). BE×6 09:30 $215.71 → close $210.77 +29.61; ABCL×109 09:30 $12.30 → close $11.35 +104.10; AQST×264 09:30 $5.11 → close $5.02 +23.76; NEM×10 09:30 $132.35 → close $127.98 +43.70; SIMO×10 09:30 $252.24 → close $245.81 +64.30; FIG×88 09:30 $30.18 → close $28.82 +119.68 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,140.56 | ▲ 09:30 equity $11,190.18 vs yday $11,039.45 (+150.73) | 09:30 open · cash $21,140.56 (unchanged overnight, no fees) · equity $11,190.18 vs prior close $11,039.45 (+150.73) · 6 name(s) re-marked at the open (per-name table). BE×6 yday $210.77 → 09:30 $208.88 +11.34; ABCL×109 yday $11.35 → 09:30 $11.10 +27.25; AQST×264 yday $5.02 → 09:30 $4.97 +11.88; NEM×10 yday $127.98 → 09:30 $127.45 +5.30; SIMO×10 yday $245.81 → 09:30 $247.05 -12.40; FIG×88 yday $28.82 → 09:30 $27.60 +107.36 | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 6 | $208.88 | $2.01 | $+26.29 | $19,885.27 | ▲ +26.29 after sell → book $11,188.17; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 109 | $11.10 | $2.32 | $+117.38 | $18,673.06 | ▲ +117.38 after sell → book $11,185.86; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 264 | $4.97 | $3.41 | $+20.82 | $17,356.25 | ▲ +20.82 after sell → book $11,182.45; vs 09:30 mark -3.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 10 | $127.45 | $2.02 | $+47.80 | $16,079.73 | ▲ +47.80 after sell → book $11,180.43; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,079.73 | ▲ close $11,192.21 vs 09:30 $11,190.18 (session +11.78) | 16:00 close · cash $16,079.73 · equity $11,192.21 vs 09:30 $11,190.18 (+2.03; session marks +11.78) · 2 name(s) marked open→close (per-name table). SIMO×10 09:30 $247.05 → close $246.84 +2.10; FIG×88 09:30 $27.60 → close $27.49 +9.68 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,079.73 | ▲ 09:30 equity $11,297.55 vs yday $11,192.21 (+105.34) | 09:30 open · cash $16,079.73 (unchanged overnight, no fees) · equity $11,297.55 vs prior close $11,192.21 (+105.34) · 2 name(s) re-marked at the open (per-name table). SIMO×10 yday $246.84 → 09:30 $240.09 +67.50; FIG×88 yday $27.49 → 09:30 $27.06 +37.84 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,079.73 | ▲ close $11,312.63 vs 09:30 $11,297.55 (session +15.08) | 16:00 close · cash $16,079.73 · equity $11,312.63 vs 09:30 $11,297.55 (+15.08; session marks +15.08) · 2 name(s) marked open→close (per-name table). SIMO×10 09:30 $240.09 → close $237.35 +27.40; FIG×88 09:30 $27.06 → close $27.20 -12.32 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,079.73 | ▲ 09:30 equity $11,365.99 vs yday $11,312.63 (+53.36) | 09:30 open · cash $16,079.73 (unchanged overnight, no fees) · equity $11,365.99 vs prior close $11,312.63 (+53.36) · 2 name(s) re-marked at the open (per-name table). SIMO×10 yday $237.35 → 09:30 $235.71 +16.40; FIG×88 yday $27.20 → 09:30 $26.78 +36.96 | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 10 | $235.71 | $2.02 | $+161.16 | $13,720.61 | ▲ +161.16 after sell → book $11,363.97; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 88 | $26.78 | $2.25 | $+294.58 | $11,361.72 | ▲ +294.58 after sell → book $11,361.72; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,361.72 | ▲ close $11,361.72 vs 09:30 $11,365.99 (session +0.00) | 16:00 close · cash $11,361.72 · no lots left · equity $11,361.72. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,361.72 | ▲ 09:30 equity $11,361.72 vs yday $11,361.72 (-0.00) | 09:30 open · cash $11,361.72 · no holdings · equity $11,361.72 vs prior close $11,361.72 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 3322 | $1.71 | $43.61 | — | $16,998.73 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ⚪; ret5=+11.9; leftover $5680.86 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,998.73 | ▲ close $11,650.31 vs 09:30 $11,361.72 (session +332.20) | 16:00 close · cash $16,998.73 · equity $11,650.31 vs 09:30 $11,361.72 (+288.59; session marks +332.20) · 1 name(s) marked open→close (per-name table). OPK×3322 09:30 $1.71 → close $1.61 +332.20 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,998.73 | ▲ 09:30 equity $11,716.75 vs yday $11,650.31 (+66.44) | 09:30 open · cash $16,998.73 (unchanged overnight, no fees) · equity $11,716.75 vs prior close $11,650.31 (+66.44) · 1 name(s) re-marked at the open (per-name table). OPK×3322 yday $1.61 → 09:30 $1.59 +66.44 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 627 | $4.67 | $8.30 | — | $19,918.53 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_gainer; ret5=+11.9; leftover $2929.19 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 38 | $76.55 | $2.22 | — | $22,825.21 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list mover_buy; 🔵; ⚪; ret5=+4.2; leftover $2929.19 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,825.21 | ▼ close $11,521.52 vs 09:30 $11,716.75 (session -184.72) | 16:00 close · cash $22,825.21 · equity $11,521.52 vs 09:30 $11,716.75 (-195.23; session marks -184.72) · 3 name(s) marked open→close (per-name table). OPK×3322 09:30 $1.59 → close $1.64 -166.10; GSM×627 09:30 $4.67 → close $4.67 -0.00; PIPR×38 09:30 $76.55 → close $77.04 -18.62 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,825.21 | ▼ 09:30 equity $11,519.78 vs yday $11,521.52 (-1.74) | 09:30 open · cash $22,825.21 (unchanged overnight, no fees) · equity $11,519.78 vs prior close $11,521.52 (-1.74) · 3 name(s) re-marked at the open (per-name table). OPK×3322 yday $1.64 → 09:30 $1.63 +33.22; GSM×627 yday $4.67 → 09:30 $4.75 -50.16; PIPR×38 yday $77.04 → 09:30 $76.64 +15.20 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,825.21 | ▲ close $11,770.27 vs 09:30 $11,519.78 (session +250.49) | 16:00 close · cash $22,825.21 · equity $11,770.27 vs 09:30 $11,519.78 (+250.49; session marks +250.49) · 3 name(s) marked open→close (per-name table). OPK×3322 09:30 $1.63 → close $1.59 +132.88; GSM×627 09:30 $4.75 → close $4.52 +144.21; PIPR×38 09:30 $76.64 → close $77.34 -26.60 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,825.21 | ▲ 09:30 equity $11,807.29 vs yday $11,770.27 (+37.02) | 09:30 open · cash $22,825.21 (unchanged overnight, no fees) · equity $11,807.29 vs prior close $11,770.27 (+37.02) · 3 name(s) re-marked at the open (per-name table). OPK×3322 yday $1.59 → 09:30 $1.58 +33.22; GSM×627 yday $4.52 → 09:30 $4.52 -0.00; PIPR×38 yday $77.34 → 09:30 $77.24 +3.80 | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 3322 | $1.58 | $42.85 | $+345.40 | $17,533.60 | ▲ +345.40 after sell → book $11,764.44; vs 09:30 mark -42.85 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,533.60 | ▲ close $11,793.89 vs 09:30 $11,807.29 (session +29.45) | 16:00 close · cash $17,533.60 · equity $11,793.89 vs 09:30 $11,807.29 (-13.40; session marks +29.45) · 2 name(s) marked open→close (per-name table). GSM×627 09:30 $4.52 → close $4.49 +18.81; PIPR×38 09:30 $77.24 → close $76.96 +10.64 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,533.60 | ▲ 09:30 equity $11,881.86 vs yday $11,793.89 (+87.97) | 09:30 open · cash $17,533.60 (unchanged overnight, no fees) · equity $11,881.86 vs prior close $11,793.89 (+87.97) · 2 name(s) re-marked at the open (per-name table). GSM×627 yday $4.49 → 09:30 $4.36 +81.51; PIPR×38 yday $76.96 → 09:30 $76.79 +6.46 | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 627 | $4.36 | $8.09 | $+177.99 | $14,791.79 | ▲ +177.99 after sell → book $11,873.77; vs 09:30 mark -8.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 38 | $76.79 | $2.10 | $-13.44 | $11,871.66 | ▼ -13.44 after sell → book $11,871.66; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,871.66 | ▲ close $11,871.66 vs 09:30 $11,881.86 (session +0.00) | 16:00 close · cash $11,871.66 · no lots left · equity $11,871.66. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,871.66 | ▲ 09:30 equity $11,871.66 vs yday $11,871.66 (+0.00) | 09:30 open · cash $11,871.66 · no holdings · equity $11,871.66 vs prior close $11,871.66 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 13 | $112.83 | $2.09 | — | $13,336.43 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list yday_gainer,ohlc_hot; ret5=+11.7; leftover $1483.96 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 440 | $3.37 | $5.80 | — | $14,813.43 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list ohlc_hot; 🔵; ret5=+4.0; leftover $1483.96 | join🔴 sector🟡 gen🟡 news🔴 digest🔴 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 59 | $24.97 | $2.23 | — | $16,284.43 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list oppset; ret5=-0.6; leftover $1483.96 | join🔴 sector🔴 gen🟡 news🔴 digest🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `INGM` | 55 | $26.62 | $2.22 | — | $17,746.31 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list oppset; ret5=+0.7; leftover $1483.96 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,746.31 | ▼ close $11,780.24 vs 09:30 $11,871.66 (session -79.09) | 16:00 close · cash $17,746.31 · equity $11,780.24 vs 09:30 $11,871.66 (-91.42; session marks -79.09) · 4 name(s) marked open→close (per-name table). QRVO×13 09:30 $112.83 → close $116.65 -49.60; MYGN×440 09:30 $3.37 → close $3.42 -22.00; BKV×59 09:30 $24.97 → close $24.23 +43.66; INGM×55 09:30 $26.62 → close $27.55 -51.15 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,746.31 | ▲ 09:30 equity $11,843.39 vs yday $11,780.24 (+63.15) | 09:30 open · cash $17,746.31 (unchanged overnight, no fees) · equity $11,843.39 vs prior close $11,780.24 (+63.15) · 4 name(s) re-marked at the open (per-name table). QRVO×13 yday $116.65 → 09:30 $114.11 +33.02; MYGN×440 yday $3.42 → 09:30 $3.43 -4.40; BKV×59 yday $24.23 → 09:30 $24.26 -1.77; INGM×55 yday $27.55 → 09:30 $26.89 +36.30 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,746.31 | ▼ close $11,792.84 vs 09:30 $11,843.39 (session -50.55) | 16:00 close · cash $17,746.31 · equity $11,792.84 vs 09:30 $11,843.39 (-50.55; session marks -50.55) · 4 name(s) marked open→close (per-name table). QRVO×13 09:30 $114.11 → close $107.98 +79.69; MYGN×440 09:30 $3.43 → close $3.79 -158.40; BKV×59 09:30 $24.26 → close $23.82 +25.96; INGM×55 09:30 $26.89 → close $26.85 +2.20 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,746.31 | ▼ 09:30 equity $11,754.31 vs yday $11,792.84 (-38.53) | 09:30 open · cash $17,746.31 (unchanged overnight, no fees) · equity $11,754.31 vs prior close $11,792.84 (-38.53) · 4 name(s) re-marked at the open (per-name table). QRVO×13 yday $107.98 → 09:30 $108.40 -5.46; MYGN×440 yday $3.79 → 09:30 $3.80 -4.40; BKV×59 yday $23.82 → 09:30 $24.25 -25.37; INGM×55 yday $26.85 → 09:30 $26.91 -3.30 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,746.31 | ▼ close $11,620.08 vs 09:30 $11,754.31 (session -134.23) | 16:00 close · cash $17,746.31 · equity $11,620.08 vs 09:30 $11,754.31 (-134.23; session marks -134.23) · 4 name(s) marked open→close (per-name table). QRVO×13 09:30 $108.40 → close $118.06 -125.58; MYGN×440 09:30 $3.80 → close $3.88 -35.20; BKV×59 09:30 $24.25 → close $24.35 -5.90; INGM×55 09:30 $26.91 → close $26.32 +32.45 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,746.31 | ▲ 09:30 equity $11,686.44 vs yday $11,620.08 (+66.36) | 09:30 open · cash $17,746.31 (unchanged overnight, no fees) · equity $11,686.44 vs prior close $11,620.08 (+66.36) · 4 name(s) re-marked at the open (per-name table). QRVO×13 yday $118.06 → 09:30 $118.18 -1.56; MYGN×440 yday $3.88 → 09:30 $3.75 +57.20; BKV×59 yday $24.35 → 09:30 $24.42 -4.13; INGM×55 yday $26.32 → 09:30 $26.05 +14.85 | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 13 | $118.18 | $2.03 | $-73.60 | $16,207.94 | ▼ -73.60 after sell → book $11,684.41; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 440 | $3.75 | $5.68 | $-178.68 | $14,552.26 | ▼ -178.68 after sell → book $11,678.73; vs 09:30 mark -5.68 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 59 | $24.42 | $2.17 | $+28.05 | $13,109.32 | ▲ +28.05 after sell → book $11,676.57; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `INGM` | 55 | $26.05 | $2.15 | $+26.98 | $11,674.41 | ▲ +26.98 after sell → book $11,674.41; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SHORT** | `TRMD` | 162 | $35.90 | $2.71 | — | $17,487.50 | — | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list oppset; 🔵; ret5=+2.6; leftover $5837.21 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,487.50 | ▼ close $11,558.30 vs 09:30 $11,686.44 (session -113.40) | 16:00 close · cash $17,487.50 · equity $11,558.30 vs 09:30 $11,686.44 (-128.14; session marks -113.40) · 1 name(s) marked open→close (per-name table). TRMD×162 09:30 $35.90 → close $36.60 -113.40 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,487.50 | ▲ 09:30 equity $11,571.26 vs yday $11,558.30 (+12.96) | 09:30 open · cash $17,487.50 (unchanged overnight, no fees) · equity $11,571.26 vs prior close $11,558.30 (+12.96) · 1 name(s) re-marked at the open (per-name table). TRMD×162 yday $36.60 → 09:30 $36.52 +12.96 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,487.50 | ▼ close $11,553.44 vs 09:30 $11,571.26 (session -17.82) | 16:00 close · cash $17,487.50 · equity $11,553.44 vs 09:30 $11,571.26 (-17.82; session marks -17.82) · 1 name(s) marked open→close (per-name table). TRMD×162 09:30 $36.52 → close $36.63 -17.82 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,487.50 | ▼ 09:30 equity $11,378.48 vs yday $11,553.44 (-174.96) | 09:30 open · cash $17,487.50 (unchanged overnight, no fees) · equity $11,378.48 vs prior close $11,553.44 (-174.96) · 1 name(s) re-marked at the open (per-name table). TRMD×162 yday $36.63 → 09:30 $37.71 -174.96 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,487.50 | ▼ close $11,294.24 vs 09:30 $11,378.48 (session -84.24) | 16:00 close · cash $17,487.50 · equity $11,294.24 vs 09:30 $11,378.48 (-84.24; session marks -84.24) · 1 name(s) marked open→close (per-name table). TRMD×162 09:30 $37.71 → close $38.23 -84.24 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `EU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LUNR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `OWL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `SVV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `EU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LUNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OWL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `SVV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VERI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `HIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BIRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `RNW` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `VERI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `HIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `BIRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RSKD` | hard_red | hard-red S=-7.20 sit; no new buys |
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
| 2026-08-24 | `AMLX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARIS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `NOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AMLX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ARE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ARE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `NEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `BE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `AQST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `NEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TXNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `PIPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `GSM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `PIPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INGM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `QRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BKV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ORCL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `QRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `MYGN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BKV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `INGM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BBD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-18 | `TRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `TRMD` | 162 | 2026-09-16 @ $35.90 | short news🔴 ∩ prior MACD histogram > 0; gate news=bad,macd_up=True; list oppset; 🔵; ret5=+2.6; leftover $5837.21 |
