# Factor mine action — `union_blue_vol_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-3.65%** ($9,635) · signal-only (no cash/fees) was +7.21%. Starts YES **7/26**. Fills 190 · skips 55 · realized $-63.22.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the volume camera (is this name unusually active?) is green.
- Must-have: the name is painted 🔵 (a turn higher on a still-red row).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).
- Must-not: the news camera (does the morning packet like the headline?) is red.

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Keep the first 8 names in list order.
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
- **Gate** `vol=good,blue=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9.34.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `BTBT` | 833 | — | $1.50 | +0.00 | $1.57 | +58.31 | +58.31 | +0.00 | +58.31 |
| 2026-08-14 | `BETR` | 84 | — | $14.80 | +0.00 | $13.73 | -89.88 | -89.88 | +0.00 | -89.88 |
| 2026-08-14 | `ANGX` | 290 | — | $4.31 | +0.00 | $4.37 | +17.40 | +17.40 | +0.00 | +17.40 |
| 2026-08-14 | `HYLN` | 299 | — | $4.18 | +0.00 | $4.06 | -35.88 | -35.88 | +0.00 | -35.88 |
| 2026-08-14 | `ADUR` | 75 | — | $16.50 | +0.00 | $16.17 | -24.75 | -24.75 | +0.00 | -24.75 |
| 2026-08-14 | `ARX` | 63 | — | $19.57 | +0.00 | $19.58 | +0.63 | +0.63 | +0.00 | +0.63 |
| 2026-08-14 | `AIRO` | 112 | — | $11.12 | +0.00 | $9.57 | -173.60 | -173.60 | +0.00 | -173.60 |
| 2026-08-14 | `NCMI` | 464 | — | $2.69 | +0.00 | $2.86 | +78.88 | +78.88 | +0.00 | +78.88 |
| 2026-08-17 | `BTBT` | 833 | $1.57 | $1.52 | -41.65 | — | +0.00 | -41.65 | +16.66 | — |
| 2026-08-17 | `BETR` | 84 | $13.73 | $13.67 | -5.04 | — | +0.00 | -5.04 | -94.92 | — |
| 2026-08-17 | `ANGX` | 290 | $4.37 | $4.60 | +66.70 | — | +0.00 | +66.70 | +84.10 | — |
| 2026-08-17 | `HYLN` | 299 | $4.06 | $4.10 | +11.96 | — | +0.00 | +11.96 | -23.92 | — |
| 2026-08-17 | `ADUR` | 75 | $16.17 | $15.73 | -33.00 | — | +0.00 | -33.00 | -57.75 | — |
| 2026-08-17 | `ARX` | 63 | $19.58 | $19.57 | -0.63 | — | +0.00 | -0.63 | +0.00 | — |
| 2026-08-17 | `AIRO` | 112 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -173.60 | — |
| 2026-08-17 | `NCMI` | 464 | $2.86 | $2.80 | -27.84 | — | +0.00 | -27.84 | +51.04 | — |
| 2026-08-17 | `TMC` | 300 | — | $4.05 | +0.00 | $3.77 | -84.00 | -84.00 | +0.00 | -84.00 |
| 2026-08-17 | `ABX` | 133 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `ALOY` | 83 | — | $14.66 | +0.00 | $13.86 | -66.81 | -66.81 | +0.00 | -66.81 |
| 2026-08-17 | `NU` | 79 | — | $15.40 | +0.00 | $14.74 | -52.14 | -52.14 | +0.00 | -52.14 |
| 2026-08-17 | `INV` | 751 | — | $1.62 | +0.00 | $1.39 | -176.49 | -176.49 | +0.00 | -176.49 |
| 2026-08-17 | `KLC` | 464 | — | $2.62 | +0.00 | $2.56 | -27.84 | -27.84 | +0.00 | -27.84 |
| 2026-08-17 | `ENHA` | 605 | — | $2.01 | +0.00 | $1.71 | -181.50 | -181.50 | +0.00 | -181.50 |
| 2026-08-17 | `MP` | 20 | — | $58.01 | +0.00 | $58.51 | +10.00 | +10.00 | +0.00 | +10.00 |
| 2026-08-18 | `TMC` | 300 | $3.77 | $3.72 | -15.00 | — | +0.00 | -15.00 | -99.00 | — |
| 2026-08-18 | `ABX` | 133 | $9.12 | $9.03 | -11.97 | — | +0.00 | -11.97 | -11.97 | — |
| 2026-08-18 | `ALOY` | 83 | $13.86 | $13.19 | -55.20 | — | +0.00 | -55.20 | -122.01 | — |
| 2026-08-18 | `NU` | 79 | $14.74 | $14.53 | -16.59 | — | +0.00 | -16.59 | -68.73 | — |
| 2026-08-18 | `INV` | 751 | $1.39 | $1.32 | -45.06 | — | +0.00 | -45.06 | -221.55 | — |
| 2026-08-18 | `KLC` | 464 | $2.56 | $2.52 | -18.56 | — | +0.00 | -18.56 | -46.40 | — |
| 2026-08-18 | `ENHA` | 605 | $1.71 | $1.70 | -6.05 | — | +0.00 | -6.05 | -187.55 | — |
| 2026-08-18 | `MP` | 20 | $58.51 | $56.35 | -43.20 | — | +0.00 | -43.20 | -33.20 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 53 | — | $20.55 | +0.00 | $21.19 | +33.92 | +33.92 | +0.00 | +33.92 |
| 2026-08-20 | `BHP` | 12 | — | $91.01 | +0.00 | $93.63 | +31.44 | +31.44 | +0.00 | +31.44 |
| 2026-08-20 | `CDE` | 53 | — | $20.65 | +0.00 | $21.11 | +24.38 | +24.38 | +0.00 | +24.38 |
| 2026-08-20 | `HDSN` | 192 | — | $5.77 | +0.00 | $5.57 | -38.40 | -38.40 | +0.00 | -38.40 |
| 2026-08-20 | `IAG` | 56 | — | $19.63 | +0.00 | $20.50 | +48.72 | +48.72 | +0.00 | +48.72 |
| 2026-08-20 | `KGC` | 37 | — | $29.63 | +0.00 | $31.43 | +66.60 | +66.60 | +0.00 | +66.60 |
| 2026-08-20 | `NFGC` | 633 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 7 | — | $144.54 | +0.00 | $150.25 | +39.97 | +39.97 | +0.00 | +39.97 |
| 2026-08-21 | `AG` | 53 | $21.19 | $21.90 | +37.63 | — | +0.00 | +37.63 | +71.55 | — |
| 2026-08-21 | `BHP` | 12 | $93.63 | $95.72 | +25.08 | — | +0.00 | +25.08 | +56.52 | — |
| 2026-08-21 | `CDE` | 53 | $21.11 | $21.75 | +33.92 | — | +0.00 | +33.92 | +58.30 | — |
| 2026-08-21 | `HDSN` | 192 | $5.57 | $5.67 | +19.20 | — | +0.00 | +19.20 | -19.20 | — |
| 2026-08-21 | `IAG` | 56 | $20.50 | $21.17 | +37.52 | — | +0.00 | +37.52 | +86.24 | — |
| 2026-08-21 | `KGC` | 37 | $31.43 | $32.17 | +27.38 | — | +0.00 | +27.38 | +93.98 | — |
| 2026-08-21 | `NFGC` | 633 | $1.75 | $1.79 | +25.32 | — | +0.00 | +25.32 | +25.32 | — |
| 2026-08-21 | `WPM` | 7 | $150.25 | $154.70 | +31.15 | — | +0.00 | +31.15 | +71.12 | — |
| 2026-08-21 | `AU` | 9 | — | $119.43 | +0.00 | $121.22 | +16.11 | +16.11 | +0.00 | +16.11 |
| 2026-08-21 | `AUPH` | 67 | — | $17.20 | +0.00 | $16.65 | -36.85 | -36.85 | +0.00 | -36.85 |
| 2026-08-21 | `AEM` | 5 | — | $216.30 | +0.00 | $216.06 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-21 | `ARCT` | 104 | — | $11.13 | +0.00 | $13.45 | +241.28 | +241.28 | +0.00 | +241.28 |
| 2026-08-21 | `AUTL` | 469 | — | $2.47 | +0.00 | $2.41 | -28.14 | -28.14 | +0.00 | -28.14 |
| 2026-08-21 | `CRDL` | 600 | — | $1.93 | +0.00 | $1.86 | -42.00 | -42.00 | +0.00 | -42.00 |
| 2026-08-21 | `CRSP` | 19 | — | $59.72 | +0.00 | $59.50 | -4.18 | -4.18 | +0.00 | -4.18 |
| 2026-08-21 | `CYPH` | 877 | — | $1.32 | +0.00 | $1.42 | +87.70 | +87.70 | +0.00 | +87.70 |
| 2026-08-24 | `AU` | 9 | $121.22 | $120.51 | -6.39 | — | +0.00 | -6.39 | +9.72 | — |
| 2026-08-24 | `AUPH` | 67 | $16.65 | $16.57 | -5.36 | — | +0.00 | -5.36 | -42.21 | — |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +3.65 | — |
| 2026-08-24 | `ARCT` | 104 | $13.45 | $13.33 | -12.48 | — | +0.00 | -12.48 | +228.80 | — |
| 2026-08-24 | `AUTL` | 469 | $2.41 | $2.40 | -4.69 | — | +0.00 | -4.69 | -32.83 | — |
| 2026-08-24 | `CRDL` | 600 | $1.86 | $1.88 | +12.00 | — | +0.00 | +12.00 | -30.00 | — |
| 2026-08-24 | `CRSP` | 19 | $59.50 | $58.75 | -14.25 | — | +0.00 | -14.25 | -18.43 | — |
| 2026-08-24 | `CYPH` | 877 | $1.42 | $1.83 | +359.57 | — | +0.00 | +359.57 | +447.27 | — |
| 2026-08-25 | `CAPR` | 168 | — | $7.25 | +0.00 | $8.29 | +174.72 | +174.72 | +0.00 | +174.72 |
| 2026-08-25 | `KURA` | 89 | — | $13.59 | +0.00 | $13.59 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `CCOI` | 128 | — | $9.49 | +0.00 | $9.88 | +49.92 | +49.92 | +0.00 | +49.92 |
| 2026-08-25 | `LIFE` | 33 | — | $36.96 | +0.00 | $38.56 | +52.80 | +52.80 | +0.00 | +52.80 |
| 2026-08-25 | `ZIP` | 268 | — | $4.55 | +0.00 | $4.35 | -53.60 | -53.60 | +0.00 | -53.60 |
| 2026-08-25 | `BMEA` | 748 | — | $1.63 | +0.00 | $1.73 | +74.80 | +74.80 | +0.00 | +74.80 |
| 2026-08-25 | `NPWR` | 610 | — | $2.00 | +0.00 | $1.95 | -30.50 | -30.50 | +0.00 | -30.50 |
| 2026-08-25 | `PUSA` | 317 | — | $3.80 | +0.00 | $3.78 | -6.34 | -6.34 | +0.00 | -6.34 |
| 2026-08-26 | `CAPR` | 168 | $8.29 | $8.29 | +0.00 | — | +0.00 | +0.00 | +174.72 | — |
| 2026-08-26 | `KURA` | 89 | $13.59 | $13.63 | +3.56 | — | +0.00 | +3.56 | +3.56 | — |
| 2026-08-26 | `CCOI` | 128 | $9.88 | $9.89 | +1.28 | — | +0.00 | +1.28 | +51.20 | — |
| 2026-08-26 | `LIFE` | 33 | $38.56 | $38.24 | -10.56 | — | +0.00 | -10.56 | +42.24 | — |
| 2026-08-26 | `ZIP` | 268 | $4.35 | $4.31 | -10.72 | — | +0.00 | -10.72 | -64.32 | — |
| 2026-08-26 | `BMEA` | 748 | $1.73 | $1.75 | +18.70 | — | +0.00 | +18.70 | +93.50 | — |
| 2026-08-26 | `NPWR` | 610 | $1.95 | $1.93 | -12.20 | — | +0.00 | -12.20 | -42.70 | — |
| 2026-08-26 | `PUSA` | 317 | $3.78 | $3.83 | +17.44 | — | +0.00 | +17.44 | +11.10 | — |
| 2026-08-26 | `SLQT` | 4272 | — | $0.58 | +0.00 | $0.55 | -140.98 | -140.98 | +0.00 | -140.98 |
| 2026-08-26 | `USDE` | 428 | — | $5.81 | +0.00 | $5.98 | +72.76 | +72.76 | +0.00 | +72.76 |
| 2026-08-26 | `DKS` | 20 | — | $121.87 | +0.00 | $129.66 | +155.80 | +155.80 | +0.00 | +155.80 |
| 2026-08-26 | `ASST` | 120 | — | $20.72 | +0.00 | $21.50 | +93.60 | +93.60 | +0.00 | +93.60 |
| 2026-08-27 | `SLQT` | 4272 | $0.55 | $0.53 | -85.44 | — | +0.00 | -85.44 | -226.42 | — |
| 2026-08-27 | `USDE` | 428 | $5.98 | $6.50 | +222.56 | — | +0.00 | +222.56 | +295.32 | — |
| 2026-08-27 | `DKS` | 20 | $129.66 | $128.73 | -18.60 | — | +0.00 | -18.60 | +137.20 | — |
| 2026-08-27 | `ASST` | 120 | $21.50 | $22.45 | +114.00 | — | +0.00 | +114.00 | +207.60 | — |
| 2026-08-28 | `SEDG` | 39 | — | $32.90 | +0.00 | $31.41 | -58.11 | -58.11 | +0.00 | -58.11 |
| 2026-08-28 | `URBN` | 16 | — | $79.42 | +0.00 | $81.09 | +26.72 | +26.72 | +0.00 | +26.72 |
| 2026-08-28 | `ANF` | 8 | — | $146.07 | +0.00 | $148.42 | +18.80 | +18.80 | +0.00 | +18.80 |
| 2026-08-28 | `SMTC` | 9 | — | $141.76 | +0.00 | $131.17 | -95.31 | -95.31 | +0.00 | -95.31 |
| 2026-08-28 | `NCNO` | 55 | — | $23.30 | +0.00 | $22.99 | -17.05 | -17.05 | +0.00 | -17.05 |
| 2026-08-28 | `PLAB` | 42 | — | $30.01 | +0.00 | $27.73 | -95.76 | -95.76 | +0.00 | -95.76 |
| 2026-08-28 | `BURL` | 4 | — | $291.30 | +0.00 | $272.95 | -73.40 | -73.40 | +0.00 | -73.40 |
| 2026-08-28 | `TRMD` | 39 | — | $32.23 | +0.00 | $32.62 | +15.21 | +15.21 | +0.00 | +15.21 |
| 2026-08-31 | `SEDG` | 39 | $31.41 | $31.15 | -10.14 | — | +0.00 | -10.14 | -68.25 | — |
| 2026-08-31 | `URBN` | 16 | $81.09 | $80.44 | -10.40 | — | +0.00 | -10.40 | +16.32 | — |
| 2026-08-31 | `ANF` | 8 | $148.42 | $148.03 | -3.12 | — | +0.00 | -3.12 | +15.68 | — |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | — | +0.00 | +10.17 | -85.14 | — |
| 2026-08-31 | `NCNO` | 55 | $22.99 | $22.66 | -18.15 | — | +0.00 | -18.15 | -35.20 | — |
| 2026-08-31 | `PLAB` | 42 | $27.73 | $28.04 | +13.02 | — | +0.00 | +13.02 | -82.74 | — |
| 2026-08-31 | `BURL` | 4 | $272.95 | $270.50 | -9.80 | — | +0.00 | -9.80 | -83.20 | — |
| 2026-08-31 | `TRMD` | 39 | $32.62 | $33.09 | +18.33 | — | +0.00 | +18.33 | +33.54 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `RVTY` | 9 | — | $132.45 | +0.00 | $130.63 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-03 | `CRK` | 80 | — | $15.45 | +0.00 | $14.95 | -40.00 | -40.00 | +0.00 | -40.00 |
| 2026-09-03 | `MRNA` | 8 | — | $145.94 | +0.00 | $148.87 | +23.40 | +23.40 | +0.00 | +23.40 |
| 2026-09-03 | `ARCT` | 74 | — | $16.77 | +0.00 | $15.56 | -89.54 | -89.54 | +0.00 | -89.54 |
| 2026-09-03 | `CRDL` | 571 | — | $2.18 | +0.00 | $2.16 | -11.42 | -11.42 | +0.00 | -11.42 |
| 2026-09-03 | `MMED` | 52 | — | $23.88 | +0.00 | $23.84 | -2.08 | -2.08 | +0.00 | -2.08 |
| 2026-09-03 | `DEFT` | 1915 | — | $0.65 | +0.00 | $0.68 | +55.54 | +55.54 | +0.00 | +55.54 |
| 2026-09-03 | `CTMX` | 333 | — | $3.73 | +0.00 | $3.68 | -16.65 | -16.65 | +0.00 | -16.65 |
| 2026-09-04 | `RVTY` | 9 | $130.63 | $130.03 | -5.40 | — | +0.00 | -5.40 | -21.78 | — |
| 2026-09-04 | `CRK` | 80 | $14.95 | $15.00 | +4.00 | — | +0.00 | +4.00 | -36.00 | — |
| 2026-09-04 | `MRNA` | 8 | $148.87 | $153.62 | +38.00 | — | +0.00 | +38.00 | +61.40 | — |
| 2026-09-04 | `ARCT` | 74 | $15.56 | $15.61 | +3.70 | — | +0.00 | +3.70 | -85.84 | — |
| 2026-09-04 | `CRDL` | 571 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -11.42 | — |
| 2026-09-04 | `MMED` | 52 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.08 | — |
| 2026-09-04 | `DEFT` | 1915 | $0.68 | $0.69 | +21.06 | — | +0.00 | +21.06 | +76.60 | — |
| 2026-09-04 | `CTMX` | 333 | $3.68 | $3.64 | -13.32 | — | +0.00 | -13.32 | -29.97 | — |
| 2026-09-04 | `CABA` | 355 | — | $3.46 | +0.00 | $3.47 | +3.55 | +3.55 | +0.00 | +3.55 |
| 2026-09-04 | `ALEC` | 487 | — | $2.52 | +0.00 | $2.46 | -29.22 | -29.22 | +0.00 | -29.22 |
| 2026-09-04 | `BHC` | 183 | — | $6.71 | +0.00 | $6.56 | -27.45 | -27.45 | +0.00 | -27.45 |
| 2026-09-04 | `BMEA` | 646 | — | $1.90 | +0.00 | $2.03 | +83.98 | +83.98 | +0.00 | +83.98 |
| 2026-09-04 | `OABI` | 257 | — | $4.78 | +0.00 | $4.33 | -115.65 | -115.65 | +0.00 | -115.65 |
| 2026-09-04 | `VIR` | 108 | — | $11.31 | +0.00 | $11.38 | +8.10 | +8.10 | +0.00 | +8.10 |
| 2026-09-04 | `EOSE` | 349 | — | $3.52 | +0.00 | $3.88 | +125.64 | +125.64 | +0.00 | +125.64 |
| 2026-09-04 | `DELL` | 2 | — | $513.78 | +0.00 | $524.14 | +20.72 | +20.72 | +0.00 | +20.72 |
| 2026-09-08 | `CABA` | 355 | $3.47 | $3.43 | -14.20 | — | +0.00 | -14.20 | -10.65 | — |
| 2026-09-08 | `ALEC` | 487 | $2.46 | $2.38 | -38.96 | — | +0.00 | -38.96 | -68.18 | — |
| 2026-09-08 | `BHC` | 183 | $6.56 | $6.57 | +1.83 | — | +0.00 | +1.83 | -25.62 | — |
| 2026-09-08 | `BMEA` | 646 | $2.03 | $2.00 | -19.38 | — | +0.00 | -19.38 | +64.60 | — |
| 2026-09-08 | `OABI` | 257 | $4.33 | $4.30 | -7.71 | — | +0.00 | -7.71 | -123.36 | — |
| 2026-09-08 | `VIR` | 108 | $11.38 | $11.22 | -17.82 | — | +0.00 | -17.82 | -9.72 | — |
| 2026-09-08 | `EOSE` | 349 | $3.88 | $3.99 | +38.39 | — | +0.00 | +38.39 | +164.03 | — |
| 2026-09-08 | `DELL` | 2 | $524.14 | $521.15 | -5.98 | — | +0.00 | -5.98 | +14.74 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `INDP` | 452 | — | $2.70 | +0.00 | $2.77 | +31.64 | +31.64 | +0.00 | +31.64 |
| 2026-09-11 | `WLTH` | 111 | — | $10.95 | +0.00 | $10.38 | -63.27 | -63.27 | +0.00 | -63.27 |
| 2026-09-11 | `BNC` | 248 | — | $4.91 | +0.00 | $4.80 | -27.28 | -27.28 | +0.00 | -27.28 |
| 2026-09-11 | `ANGX` | 226 | — | $5.38 | +0.00 | $5.45 | +15.82 | +15.82 | +0.00 | +15.82 |
| 2026-09-11 | `TSSI` | 135 | — | $8.98 | +0.00 | $8.93 | -6.75 | -6.75 | +0.00 | -6.75 |
| 2026-09-11 | `LDI` | 1436 | — | $0.85 | +0.00 | $0.83 | -21.54 | -21.54 | +0.00 | -21.54 |
| 2026-09-11 | `ASO` | 22 | — | $54.91 | +0.00 | $55.36 | +9.90 | +9.90 | +0.00 | +9.90 |
| 2026-09-11 | `IRD` | 197 | — | $6.16 | +0.00 | $6.04 | -23.64 | -23.64 | +0.00 | -23.64 |
| 2026-09-14 | `INDP` | 452 | $2.77 | $2.80 | +13.56 | — | +0.00 | +13.56 | +45.20 | — |
| 2026-09-14 | `WLTH` | 111 | $10.38 | $10.29 | -9.99 | — | +0.00 | -9.99 | -73.26 | — |
| 2026-09-14 | `BNC` | 248 | $4.80 | $5.03 | +57.04 | — | +0.00 | +57.04 | +29.76 | — |
| 2026-09-14 | `ANGX` | 226 | $5.45 | $5.57 | +27.12 | — | +0.00 | +27.12 | +42.94 | — |
| 2026-09-14 | `TSSI` | 135 | $8.93 | $8.57 | -48.60 | — | +0.00 | -48.60 | -55.35 | — |
| 2026-09-14 | `LDI` | 1436 | $0.83 | $0.84 | +4.31 | — | +0.00 | +4.31 | -17.23 | — |
| 2026-09-14 | `ASO` | 22 | $55.36 | $54.75 | -13.42 | — | +0.00 | -13.42 | -3.52 | — |
| 2026-09-14 | `IRD` | 197 | $6.04 | $6.02 | -3.94 | — | +0.00 | -3.94 | -27.58 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `RIG` | 205 | — | $5.87 | +0.00 | $5.54 | -67.65 | -67.65 | +0.00 | -67.65 |
| 2026-09-16 | `VAL` | 13 | — | $87.40 | +0.00 | $82.52 | -63.44 | -63.44 | +0.00 | -63.44 |
| 2026-09-16 | `ADPT` | 44 | — | $27.09 | +0.00 | $27.67 | +25.52 | +25.52 | +0.00 | +25.52 |
| 2026-09-16 | `SWKS` | 13 | — | $89.38 | +0.00 | $85.59 | -49.27 | -49.27 | +0.00 | -49.27 |
| 2026-09-16 | `SDGR` | 51 | — | $23.29 | +0.00 | $23.93 | +32.64 | +32.64 | +0.00 | +32.64 |
| 2026-09-16 | `FPS` | 36 | — | $33.14 | +0.00 | $34.84 | +61.20 | +61.20 | +0.00 | +61.20 |
| 2026-09-16 | `QRVO` | 10 | — | $118.18 | +0.00 | $113.97 | -42.10 | -42.10 | +0.00 | -42.10 |
| 2026-09-16 | `RVTY` | 8 | — | $140.88 | +0.00 | $145.73 | +38.80 | +38.80 | +0.00 | +38.80 |
| 2026-09-17 | `RIG` | 205 | $5.54 | $5.58 | +8.20 | — | +0.00 | +8.20 | -59.45 | — |
| 2026-09-17 | `VAL` | 13 | $82.52 | $83.20 | +8.84 | — | +0.00 | +8.84 | -54.60 | — |
| 2026-09-17 | `ADPT` | 44 | $27.67 | $28.23 | +24.64 | — | +0.00 | +24.64 | +50.16 | — |
| 2026-09-17 | `SWKS` | 13 | $85.59 | $86.76 | +15.21 | — | +0.00 | +15.21 | -34.06 | — |
| 2026-09-17 | `SDGR` | 51 | $23.93 | $24.09 | +8.16 | — | +0.00 | +8.16 | +40.80 | — |
| 2026-09-17 | `FPS` | 36 | $34.84 | $36.76 | +69.12 | — | +0.00 | +69.12 | +130.32 | — |
| 2026-09-17 | `QRVO` | 10 | $113.97 | $114.90 | +9.30 | — | +0.00 | +9.30 | -32.80 | — |
| 2026-09-17 | `RVTY` | 8 | $145.73 | $147.61 | +15.04 | — | +0.00 | +15.04 | +53.84 | — |
| 2026-09-17 | `PGEN` | 159 | — | $7.59 | +0.00 | $7.87 | +44.52 | +44.52 | +0.00 | +44.52 |
| 2026-09-17 | `ARQT` | 46 | — | $25.95 | +0.00 | $26.46 | +23.46 | +23.46 | +0.00 | +23.46 |
| 2026-09-17 | `SMTC` | 7 | — | $170.85 | +0.00 | $178.19 | +51.38 | +51.38 | +0.00 | +51.38 |
| 2026-09-17 | `SABR` | 504 | — | $2.40 | +0.00 | $2.32 | -40.32 | -40.32 | +0.00 | -40.32 |
| 2026-09-17 | `CIFR` | 67 | — | $18.04 | +0.00 | $16.94 | -73.36 | -73.36 | +0.00 | -73.36 |
| 2026-09-17 | `EMAT` | 313 | — | $3.86 | +0.00 | $4.02 | +50.08 | +50.08 | +0.00 | +50.08 |
| 2026-09-17 | `CYPH` | 452 | — | $2.67 | +0.00 | $3.07 | +178.54 | +178.54 | +0.00 | +178.54 |
| 2026-09-17 | `BRKR` | 19 | — | $61.90 | +0.00 | $63.05 | +21.85 | +21.85 | +0.00 | +21.85 |
| 2026-09-18 | `PGEN` | 159 | $7.87 | $7.98 | +17.49 | — | +0.00 | +17.49 | +62.01 | — |
| 2026-09-18 | `ARQT` | 46 | $26.46 | $26.14 | -14.72 | $25.38 | -34.96 | -49.68 | +8.74 | -26.22 |
| 2026-09-18 | `SMTC` | 7 | $178.19 | $182.33 | +28.98 | — | +0.00 | +28.98 | +80.36 | — |
| 2026-09-18 | `SABR` | 504 | $2.32 | $2.29 | -15.12 | — | +0.00 | -15.12 | -55.44 | — |
| 2026-09-18 | `CIFR` | 67 | $16.94 | $17.80 | +57.62 | — | +0.00 | +57.62 | -15.74 | — |
| 2026-09-18 | `EMAT` | 313 | $4.02 | $3.97 | -15.65 | — | +0.00 | -15.65 | +34.43 | — |
| 2026-09-18 | `CYPH` | 452 | $3.07 | $3.04 | -15.82 | — | +0.00 | -15.82 | +162.72 | — |
| 2026-09-18 | `BRKR` | 19 | $63.05 | $63.37 | +6.08 | — | +0.00 | +6.08 | +27.93 | — |
| 2026-09-18 | `ILMN` | 5 | — | $249.13 | +0.00 | $239.62 | -47.55 | -47.55 | +0.00 | -47.55 |
| 2026-09-18 | `SDGR` | 42 | — | $29.32 | +0.00 | $29.02 | -12.60 | -12.60 | +0.00 | -12.60 |
| 2026-09-18 | `FTRE` | 62 | — | $20.10 | +0.00 | $19.93 | -10.54 | -10.54 | +0.00 | -10.54 |
| 2026-09-18 | `TLSA` | 1287 | — | $0.97 | +0.00 | $0.91 | -77.22 | -77.22 | +0.00 | -77.22 |
| 2026-09-18 | `EYPT` | 316 | — | $3.95 | +0.00 | $3.85 | -31.60 | -31.60 | +0.00 | -31.60 |
| 2026-09-18 | `BHVN` | 88 | — | $14.07 | +0.00 | $13.62 | -39.60 | -39.60 | +0.00 | -39.60 |
| 2026-09-18 | `RARE` | 84 | — | $14.79 | +0.00 | $14.51 | -23.52 | -23.52 | +0.00 | -23.52 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -168.89 | BTBT, BETR, ANGX, HYLN, ADUR, ARX, AIRO, NCMI | — | $10.28 | $9,797.82 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 |
| 2026-08-17 | +2.25 | $10.28 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | $9,768.32 | -29.50 | -578.78 | TMC, ABX, ALOY, NU, INV, KLC, ENHA, MP | BTBT, BETR, ANGX, HYLN, ADUR, ARX, AIRO, NCMI | $28.43 | $9,119.54 | TMC×300, ABX×133, ALOY×83, NU×79, INV×751, KLC×464, ENHA×605, MP×20 |
| 2026-08-18 | -6.20 | $28.43 | TMC×300, ABX×133, ALOY×83, NU×79, INV×751, KLC×464, ENHA×605, MP×20 | $8,907.92 | -211.62 | +0.00 | — | TMC, ABX, ALOY, NU, INV, KLC, ENHA, MP | $8,871.18 | $8,871.18 | — |
| 2026-08-19 | -7.20 | $8,871.18 | — | $8,871.18 | -0.00 | +0.00 | — | — | $8,871.18 | $8,871.18 | — |
| 2026-08-20 | +1.12 | $8,871.18 | — | $8,871.18 | -0.00 | +206.63 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $149.17 | $9,054.48 | AG×53, BHP×12, CDE×53, HDSN×192, IAG×56, KGC×37, NFGC×633, WPM×7 |
| 2026-08-21 | +3.25 | $149.17 | AG×53, BHP×12, CDE×53, HDSN×192, IAG×56, KGC×37, NFGC×633, WPM×7 | $9,291.68 | +237.20 | +232.72 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $157.37 | $9,465.13 | AU×9, AUPH×67, AEM×5, ARCT×104, AUTL×469, CRDL×600, CRSP×19, CYPH×877 |
| 2026-08-24 | -5.17 | $157.37 | AU×9, AUPH×67, AEM×5, ARCT×104, AUTL×469, CRDL×600, CRSP×19, CYPH×877 | $9,798.38 | +333.25 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $9,762.25 | $9,762.25 | — |
| 2026-08-25 | +1.80 | $9,762.25 | — | $9,762.25 | +0.00 | +261.80 | CAPR, KURA, CCOI, LIFE, ZIP, BMEA, NPWR, PUSA | — | $2.82 | $9,989.77 | CAPR×168, KURA×89, CCOI×128, LIFE×33, ZIP×268, BMEA×748, NPWR×610, PUSA×317 |
| 2026-08-26 | +2.02 | $2.82 | CAPR×168, KURA×89, CCOI×128, LIFE×33, ZIP×268, BMEA×748, NPWR×610, PUSA×317 | $9,997.27 | +7.50 | +181.18 | SLQT, USDE, DKS, ASST | CAPR, KURA, CCOI, LIFE, ZIP, BMEA, NPWR, PUSA | $13.81 | $10,096.05 | SLQT×4272, USDE×428, DKS×20, ASST×120 |
| 2026-08-27 | — | $13.81 | SLQT×4272, USDE×428, DKS×20, ASST×120 | $10,328.57 | +232.52 | +0.00 | — | SLQT, USDE, DKS, ASST | $10,282.30 | $10,282.30 | — |
| 2026-08-28 | +0.75 | $10,282.30 | — | $10,282.30 | -0.00 | -278.90 | SEDG, URBN, ANF, SMTC, NCNO, PLAB, BURL, TRMD | — | $303.43 | $9,986.84 | SEDG×39, URBN×16, ANF×8, SMTC×9, NCNO×55, PLAB×42, BURL×4, TRMD×39 |
| 2026-08-31 | -5.85 | $303.43 | SEDG×39, URBN×16, ANF×8, SMTC×9, NCNO×55, PLAB×42, BURL×4, TRMD×39 | $9,976.75 | -10.09 | +0.00 | — | SEDG, URBN, ANF, SMTC, NCNO, PLAB, BURL, TRMD | $9,960.04 | $9,960.04 | — |
| 2026-09-01 | -6.30 | $9,960.04 | — | $9,960.04 | -0.00 | +0.00 | — | — | $9,960.04 | $9,960.04 | — |
| 2026-09-02 | -3.83 | $9,960.04 | — | $9,960.04 | -0.00 | +0.00 | — | — | $9,960.04 | $9,960.04 | — |
| 2026-09-03 | -0.90 | $9,960.04 | — | $9,960.04 | -0.00 | -97.13 | RVTY, CRK, MRNA, ARCT, CRDL, MMED, DEFT, CTMX | — | $109.59 | $9,822.43 | RVTY×9, CRK×80, MRNA×8, ARCT×74, CRDL×571, MMED×52, DEFT×1915, CTMX×333 |
| 2026-09-04 | +2.25 | $109.59 | RVTY×9, CRK×80, MRNA×8, ARCT×74, CRDL×571, MMED×52, DEFT×1915, CTMX×333 | $9,870.47 | +48.04 | +69.67 | CABA, ALEC, BHC, BMEA, OABI, VIR, EOSE, DELL | RVTY, CRK, MRNA, ARCT, CRDL, MMED, DEFT, CTMX | $177.92 | $9,864.44 | CABA×355, ALEC×487, BHC×183, BMEA×646, OABI×257, VIR×108, EOSE×349, DELL×2 |
| 2026-09-08 | -11.47 | $177.92 | CABA×355, ALEC×487, BHC×183, BMEA×646, OABI×257, VIR×108, EOSE×349, DELL×2 | $9,800.61 | -63.83 | +0.00 | — | CABA, ALEC, BHC, BMEA, OABI, VIR, EOSE, DELL | $9,766.26 | $9,766.26 | — |
| 2026-09-09 | -13.95 | $9,766.26 | — | $9,766.26 | +0.00 | +0.00 | — | — | $9,766.26 | $9,766.26 | — |
| 2026-09-10 | -13.28 | $9,766.26 | — | $9,766.26 | +0.00 | +0.00 | — | — | $9,766.26 | $9,766.26 | — |
| 2026-09-11 | +0.50 | $9,766.26 | — | $9,766.26 | +0.00 | -85.12 | INDP, WLTH, BNC, ANGX, TSSI, LDI, ASO, IRD | — | $4.60 | $9,643.33 | INDP×452, WLTH×111, BNC×248, ANGX×226, TSSI×135, LDI×1436, ASO×22, IRD×197 |
| 2026-09-14 | -11.00 | $4.60 | INDP×452, WLTH×111, BNC×248, ANGX×226, TSSI×135, LDI×1436, ASO×22, IRD×197 | $9,669.40 | +26.07 | +0.00 | — | INDP, WLTH, BNC, ANGX, TSSI, LDI, ASO, IRD | $9,631.21 | $9,631.21 | — |
| 2026-09-15 | -3.84 | $9,631.21 | — | $9,631.21 | -0.00 | +0.00 | — | — | $9,631.21 | $9,631.21 | — |
| 2026-09-16 | +5.30 | $9,631.21 | — | $9,631.21 | -0.00 | -64.30 | RIG, VAL, ADPT, SWKS, SDGR, FPS, QRVO, RVTY | — | $230.99 | $9,549.81 | RIG×205, VAL×13, ADPT×44, SWKS×13, SDGR×51, FPS×36, QRVO×10, RVTY×8 |
| 2026-09-17 | +7.38 | $230.99 | RIG×205, VAL×13, ADPT×44, SWKS×13, SDGR×51, FPS×36, QRVO×10, RVTY×8 | $9,708.32 | +158.51 | +256.15 | PGEN, ARQT, SMTC, SABR, CIFR, EMAT, CYPH, BRKR | RIG, VAL, ADPT, SWKS, SDGR, FPS, QRVO, RVTY | $56.03 | $9,919.96 | PGEN×159, ARQT×46, SMTC×7, SABR×504, CIFR×67, EMAT×313, CYPH×452, BRKR×19 |
| 2026-09-18 | +4.86 | $56.03 | PGEN×159, ARQT×46, SMTC×7, SABR×504, CIFR×67, EMAT×313, CYPH×452, BRKR×19 | $9,968.82 | +48.86 | -277.59 | ILMN, SDGR, FTRE, TLSA, EYPT, BHVN, RARE | PGEN, SMTC, SABR, CIFR, EMAT, CYPH, BRKR | $9.34 | $9,634.59 | ARQT×46, ILMN×5, SDGR×42, FTRE×62, TLSA×1287, EYPT×316, BHVN×88, RARE×84 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $8,739.75 | — | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 84 | $14.80 | $2.24 | — | $7,494.31 | — | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $6,240.67 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $4,986.99 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $3,747.28 | — | combo gate; gate vol=good,blue=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $2,512.19 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $1,264.42 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $10.28 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.28 | ▼ close $9,797.82 vs 09:30 $10,000.00 (session -168.89) | 16:00 close · cash $10.28 · equity $9,797.82 vs 09:30 $10,000.00 (-202.18; session marks -168.89) · 8 name(s) marked open→close (per-name table). BTBT×833 09:30 $1.50 → close $1.57 +58.31; BETR×84 09:30 $14.80 → close $13.73 -89.88; ANGX×290 09:30 $4.31 → close $4.37 +17.40; HYLN×299 09:30 $4.18 → close $4.06 -35.88; ADUR×75 09:30 $16.50 → close $16.17 -24.75; ARX×63 09:30 $19.57 → close $19.58 +0.63; AIRO×112 09:30 $11.12 → close $9.57 -173.60; NCMI×464 09:30 $2.69 → close $2.86 +78.88 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.28 | ▼ 09:30 equity $9,768.32 vs yday $9,797.82 (-29.50) | 09:30 open · cash $10.28 (unchanged overnight, no fees) · equity $9,768.32 vs prior close $9,797.82 (-29.50) · 8 name(s) re-marked at the open (per-name table). BTBT×833 yday $1.57 → 09:30 $1.52 -41.65; BETR×84 yday $13.73 → 09:30 $13.67 -5.04; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; HYLN×299 yday $4.06 → 09:30 $4.10 +11.96; ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; AIRO×112 yday $9.57 → 09:30 $9.57 +0.00; NCMI×464 yday $2.86 → 09:30 $2.80 -27.84 | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 833 | $1.52 | $10.89 | $-4.98 | $1,265.54 | ▼ -4.98 after sell → book $9,757.42; vs 09:30 mark -10.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 84 | $13.67 | $2.27 | $-99.43 | $2,411.56 | ▼ -99.43 after sell → book $9,755.16; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $3,741.76 | ▲ +76.56 after sell → book $9,751.36; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 299 | $4.10 | $3.92 | $-31.69 | $4,963.74 | ▼ -31.69 after sell → book $9,747.44; vs 09:30 mark -3.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 75 | $15.73 | $2.24 | $-62.20 | $6,141.25 | ▼ -62.20 after sell → book $9,745.20; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $7,371.97 | ▼ -4.38 after sell → book $9,743.01; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 112 | $9.57 | $2.35 | $-178.28 | $8,441.45 | ▼ -178.28 after sell → book $9,740.65; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 464 | $2.80 | $6.07 | $+38.98 | $9,734.58 | ▲ +38.98 after sell → book $9,734.58; vs 09:30 mark -6.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 300 | $4.05 | $3.87 | — | $8,515.71 | — | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1216.82 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 133 | $9.12 | $2.39 | — | $7,300.36 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1216.82 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 83 | $14.66 | $2.24 | — | $6,081.34 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1216.82 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NU` | 79 | $15.40 | $2.23 | — | $4,862.51 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ⚪; ret5=+10.0; leftover $1216.82 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 751 | $1.62 | $9.69 | — | $3,636.20 | — | combo gate; gate vol=good,blue=True; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $1216.82 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `KLC` | 464 | $2.62 | $5.99 | — | $2,414.54 | — | combo gate; gate vol=good,blue=True; list yday_mover; 🔵; ⚪; ret5=-49.7; leftover $1216.82 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ENHA` | 605 | $2.01 | $7.80 | — | $1,190.68 | — | combo gate; gate vol=good,blue=True; list yday_mover; 🔵; ⚪; ret5=-26.0; leftover $1216.82 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `MP` | 20 | $58.01 | $2.05 | — | $28.43 | — | combo gate; gate vol=good,blue=True; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1216.82 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.43 | ▼ close $9,119.54 vs 09:30 $9,768.32 (session -578.78) | 16:00 close · cash $28.43 · equity $9,119.54 vs 09:30 $9,768.32 (-648.78; session marks -578.78) · 8 name(s) marked open→close (per-name table). TMC×300 09:30 $4.05 → close $3.77 -84.00; ABX×133 09:30 $9.12 → close $9.12 +0.00; ALOY×83 09:30 $14.66 → close $13.86 -66.81; NU×79 09:30 $15.40 → close $14.74 -52.14; INV×751 09:30 $1.62 → close $1.39 -176.49; KLC×464 09:30 $2.62 → close $2.56 -27.84; ENHA×605 09:30 $2.01 → close $1.71 -181.50; MP×20 09:30 $58.01 → close $58.51 +10.00 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.43 | ▼ 09:30 equity $8,907.92 vs yday $9,119.54 (-211.62) | 09:30 open · cash $28.43 (unchanged overnight, no fees) · equity $8,907.92 vs prior close $9,119.54 (-211.62) · 8 name(s) re-marked at the open (per-name table). TMC×300 yday $3.77 → 09:30 $3.72 -15.00; ABX×133 yday $9.12 → 09:30 $9.03 -11.97; ALOY×83 yday $13.86 → 09:30 $13.19 -55.20; NU×79 yday $14.74 → 09:30 $14.53 -16.59; INV×751 yday $1.39 → 09:30 $1.32 -45.06; KLC×464 yday $2.56 → 09:30 $2.52 -18.56; ENHA×605 yday $1.71 → 09:30 $1.70 -6.05; MP×20 yday $58.51 → 09:30 $56.35 -43.20 | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 300 | $3.72 | $3.93 | $-106.80 | $1,140.50 | ▼ -106.80 after sell → book $8,903.99; vs 09:30 mark -3.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 133 | $9.03 | $2.42 | $-16.78 | $2,339.07 | ▼ -16.78 after sell → book $8,901.57; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 83 | $13.19 | $2.26 | $-126.51 | $3,431.58 | ▼ -126.51 after sell → book $8,899.31; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `NU` | 79 | $14.53 | $2.25 | $-73.21 | $4,577.20 | ▼ -73.21 after sell → book $8,897.06; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `INV` | 751 | $1.32 | $9.82 | $-241.06 | $5,562.45 | ▼ -241.06 after sell → book $8,887.23; vs 09:30 mark -9.83 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `KLC` | 464 | $2.52 | $6.07 | $-58.46 | $6,725.66 | ▼ -58.46 after sell → book $8,881.16; vs 09:30 mark -6.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ENHA` | 605 | $1.70 | $7.91 | $-203.27 | $7,746.25 | ▼ -203.27 after sell → book $8,873.25; vs 09:30 mark -7.91 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `MP` | 20 | $56.35 | $2.07 | $-37.32 | $8,871.18 | ▼ -37.32 after sell → book $8,871.18; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,871.18 | ▲ close $8,871.18 vs 09:30 $8,907.92 (session +0.00) | 16:00 close · cash $8,871.18 · no lots left · equity $8,871.18. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,871.18 | ▲ 09:30 equity $8,871.18 vs yday $8,871.18 (-0.00) | 09:30 open · cash $8,871.18 · no holdings · equity $8,871.18 vs prior close $8,871.18 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,871.18 | ▲ close $8,871.18 vs 09:30 $8,871.18 (session +0.00) | 16:00 close · cash $8,871.18 · no lots left · equity $8,871.18. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,871.18 | ▲ 09:30 equity $8,871.18 vs yday $8,871.18 (-0.00) | 09:30 open · cash $8,871.18 · no holdings · equity $8,871.18 vs prior close $8,871.18 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 53 | $20.55 | $2.15 | — | $7,779.88 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1108.90 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 12 | $91.01 | $2.03 | — | $6,685.73 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1108.90 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 53 | $20.65 | $2.15 | — | $5,589.13 | — | combo gate; gate vol=good,blue=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1108.90 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 192 | $5.77 | $2.57 | — | $4,478.73 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1108.90 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 56 | $19.63 | $2.16 | — | $3,377.29 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1108.90 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 37 | $29.63 | $2.10 | — | $2,278.88 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1108.90 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 633 | $1.75 | $8.17 | — | $1,162.96 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1108.90 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 7 | $144.54 | $2.01 | — | $149.17 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy,oppset; 🔵; ⚪; ret5=+9.2; leftover $1108.90 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $149.17 | ▲ close $9,054.48 vs 09:30 $8,871.18 (session +206.63) | 16:00 close · cash $149.17 · equity $9,054.48 vs 09:30 $8,871.18 (+183.30; session marks +206.63) · 8 name(s) marked open→close (per-name table). AG×53 09:30 $20.55 → close $21.19 +33.92; BHP×12 09:30 $91.01 → close $93.63 +31.44; CDE×53 09:30 $20.65 → close $21.11 +24.38; HDSN×192 09:30 $5.77 → close $5.57 -38.40; IAG×56 09:30 $19.63 → close $20.50 +48.72; KGC×37 09:30 $29.63 → close $31.43 +66.60; NFGC×633 09:30 $1.75 → close $1.75 +0.00; WPM×7 09:30 $144.54 → close $150.25 +39.97 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $149.17 | ▲ 09:30 equity $9,291.68 vs yday $9,054.48 (+237.20) | 09:30 open · cash $149.17 (unchanged overnight, no fees) · equity $9,291.68 vs prior close $9,054.48 (+237.20) · 8 name(s) re-marked at the open (per-name table). AG×53 yday $21.19 → 09:30 $21.90 +37.63; BHP×12 yday $93.63 → 09:30 $95.72 +25.08; CDE×53 yday $21.11 → 09:30 $21.75 +33.92; HDSN×192 yday $5.57 → 09:30 $5.67 +19.20; IAG×56 yday $20.50 → 09:30 $21.17 +37.52; KGC×37 yday $31.43 → 09:30 $32.17 +27.38; NFGC×633 yday $1.75 → 09:30 $1.79 +25.32; WPM×7 yday $150.25 → 09:30 $154.70 +31.15 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 53 | $21.90 | $2.17 | $+67.23 | $1,307.70 | ▲ +67.23 after sell → book $9,289.51; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 12 | $95.72 | $2.05 | $+52.45 | $2,454.30 | ▲ +52.45 after sell → book $9,287.47; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 53 | $21.75 | $2.17 | $+53.98 | $3,604.88 | ▲ +53.98 after sell → book $9,285.30; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 192 | $5.67 | $2.61 | $-24.37 | $4,690.91 | ▼ -24.37 after sell → book $9,282.69; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 56 | $21.17 | $2.18 | $+81.90 | $5,874.25 | ▲ +81.90 after sell → book $9,280.51; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 37 | $32.17 | $2.12 | $+89.76 | $7,062.42 | ▲ +89.76 after sell → book $9,278.39; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 633 | $1.79 | $8.28 | $+8.87 | $8,187.21 | ▲ +8.87 after sell → book $9,270.11; vs 09:30 mark -8.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 7 | $154.70 | $2.03 | $+67.08 | $9,268.08 | ▲ +67.08 after sell → book $9,268.08; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 9 | $119.43 | $2.02 | — | $8,191.19 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1158.51 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 67 | $17.20 | $2.19 | — | $7,036.60 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1158.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $5,953.09 | — | combo gate; gate vol=good,blue=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1158.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 104 | $11.13 | $2.30 | — | $4,793.27 | — | combo gate; gate vol=good,blue=True; list flatten,yday_gainer,mover_buy,oppset; 🔵; ⚪; ret5=+39.8; leftover $1158.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 469 | $2.47 | $6.05 | — | $3,628.79 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1158.51 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 600 | $1.93 | $7.74 | — | $2,463.05 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1158.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 19 | $59.72 | $2.05 | — | $1,326.33 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1158.51 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 877 | $1.32 | $11.31 | — | $157.37 | — | combo gate; gate vol=good,blue=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1158.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $157.37 | ▲ close $9,465.13 vs 09:30 $9,291.68 (session +232.72) | 16:00 close · cash $157.37 · equity $9,465.13 vs 09:30 $9,291.68 (+173.45; session marks +232.72) · 8 name(s) marked open→close (per-name table). AU×9 09:30 $119.43 → close $121.22 +16.11; AUPH×67 09:30 $17.20 → close $16.65 -36.85; AEM×5 09:30 $216.30 → close $216.06 -1.20; ARCT×104 09:30 $11.13 → close $13.45 +241.28; AUTL×469 09:30 $2.47 → close $2.41 -28.14; CRDL×600 09:30 $1.93 → close $1.86 -42.00; CRSP×19 09:30 $59.72 → close $59.50 -4.18; CYPH×877 09:30 $1.32 → close $1.42 +87.70 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $157.37 | ▲ 09:30 equity $9,798.38 vs yday $9,465.13 (+333.25) | 09:30 open · cash $157.37 (unchanged overnight, no fees) · equity $9,798.38 vs prior close $9,465.13 (+333.25) · 8 name(s) re-marked at the open (per-name table). AU×9 yday $121.22 → 09:30 $120.51 -6.39; AUPH×67 yday $16.65 → 09:30 $16.57 -5.36; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×104 yday $13.45 → 09:30 $13.33 -12.48; AUTL×469 yday $2.41 → 09:30 $2.40 -4.69; CRDL×600 yday $1.86 → 09:30 $1.88 +12.00; CRSP×19 yday $59.50 → 09:30 $58.75 -14.25; CYPH×877 yday $1.42 → 09:30 $1.83 +359.57 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 9 | $120.51 | $2.04 | $+5.67 | $1,239.92 | ▲ +5.67 after sell → book $9,796.34; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 67 | $16.57 | $2.21 | $-46.61 | $2,347.90 | ▼ -46.61 after sell → book $9,794.13; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,431.03 | ▼ -0.38 after sell → book $9,792.11; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 104 | $13.33 | $2.33 | $+224.17 | $4,815.02 | ▲ +224.17 after sell → book $9,789.78; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 469 | $2.40 | $6.14 | $-45.02 | $5,934.48 | ▼ -45.02 after sell → book $9,783.64; vs 09:30 mark -6.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 600 | $1.88 | $7.85 | $-45.59 | $7,054.63 | ▼ -45.59 after sell → book $9,775.79; vs 09:30 mark -7.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 19 | $58.75 | $2.07 | $-22.54 | $8,168.81 | ▼ -22.54 after sell → book $9,773.72; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 877 | $1.83 | $11.47 | $+424.48 | $9,762.25 | ▲ +424.48 after sell → book $9,762.25; vs 09:30 mark -11.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,762.25 | ▲ close $9,762.25 vs 09:30 $9,798.38 (session +0.00) | 16:00 close · cash $9,762.25 · no lots left · equity $9,762.25. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,762.25 | ▲ 09:30 equity $9,762.25 vs yday $9,762.25 (+0.00) | 09:30 open · cash $9,762.25 · no holdings · equity $9,762.25 vs prior close $9,762.25 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 168 | $7.25 | $2.49 | — | $8,541.76 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,ohlc_hot,oppset; 🔵; ret5=-8.7; leftover $1220.28 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 89 | $13.59 | $2.26 | — | $7,329.99 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1220.28 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 128 | $9.49 | $2.37 | — | $6,112.90 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1220.28 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 33 | $36.96 | $2.09 | — | $4,891.13 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,oppset; 🔵; ret5=+4.4; leftover $1220.28 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 268 | $4.55 | $3.46 | — | $3,668.27 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1220.28 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 748 | $1.63 | $9.65 | — | $2,439.38 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1220.28 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 610 | $2.00 | $7.87 | — | $1,211.51 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+15.0; leftover $1220.28 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 317 | $3.80 | $4.09 | — | $2.82 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $1220.28 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.82 | ▲ close $9,989.77 vs 09:30 $9,762.25 (session +261.80) | 16:00 close · cash $2.82 · equity $9,989.77 vs 09:30 $9,762.25 (+227.52; session marks +261.80) · 8 name(s) marked open→close (per-name table). CAPR×168 09:30 $7.25 → close $8.29 +174.72; KURA×89 09:30 $13.59 → close $13.59 +0.00; CCOI×128 09:30 $9.49 → close $9.88 +49.92; LIFE×33 09:30 $36.96 → close $38.56 +52.80; ZIP×268 09:30 $4.55 → close $4.35 -53.60; BMEA×748 09:30 $1.63 → close $1.73 +74.80; NPWR×610 09:30 $2.00 → close $1.95 -30.50; PUSA×317 09:30 $3.80 → close $3.78 -6.34 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.82 | ▲ 09:30 equity $9,997.27 vs yday $9,989.77 (+7.50) | 09:30 open · cash $2.82 (unchanged overnight, no fees) · equity $9,997.27 vs prior close $9,989.77 (+7.50) · 8 name(s) re-marked at the open (per-name table). CAPR×168 yday $8.29 → 09:30 $8.29 +0.00; KURA×89 yday $13.59 → 09:30 $13.63 +3.56; CCOI×128 yday $9.88 → 09:30 $9.89 +1.28; LIFE×33 yday $38.56 → 09:30 $38.24 -10.56; ZIP×268 yday $4.35 → 09:30 $4.31 -10.72; BMEA×748 yday $1.73 → 09:30 $1.75 +18.70; NPWR×610 yday $1.95 → 09:30 $1.93 -12.20; PUSA×317 yday $3.78 → 09:30 $3.83 +17.44 | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 168 | $8.29 | $2.53 | $+169.69 | $1,393.01 | ▲ +169.69 after sell → book $9,994.73; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 89 | $13.63 | $2.28 | $-0.98 | $2,603.80 | ▼ -0.98 after sell → book $9,992.45; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `CCOI` | 128 | $9.89 | $2.41 | $+46.42 | $3,867.31 | ▲ +46.42 after sell → book $9,990.05; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 33 | $38.24 | $2.11 | $+38.04 | $5,127.12 | ▲ +38.04 after sell → book $9,987.94; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `ZIP` | 268 | $4.31 | $3.51 | $-71.29 | $6,278.69 | ▼ -71.29 after sell → book $9,984.43; vs 09:30 mark -3.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 748 | $1.75 | $9.78 | $+74.07 | $7,581.65 | ▲ +74.07 after sell → book $9,974.64; vs 09:30 mark -9.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `NPWR` | 610 | $1.93 | $7.98 | $-58.55 | $8,750.97 | ▼ -58.55 after sell → book $9,966.66; vs 09:30 mark -7.98 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `PUSA` | 317 | $3.83 | $4.15 | $+2.85 | $9,962.51 | ▲ +2.85 after sell → book $9,962.51; vs 09:30 mark -4.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 4272 | $0.58 | $37.72 | — | $7,434.21 | — | combo gate; gate vol=good,blue=True; list yday_mover; 🔵; ret5=-27.5; leftover $2490.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 428 | $5.81 | $5.52 | — | $4,942.01 | — | combo gate; gate vol=good,blue=True; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $2490.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `DKS` | 20 | $121.87 | $2.05 | — | $2,502.56 | — | combo gate; gate vol=good,blue=True; list yday_mover,oppset; 🔵; ret5=-35.1; leftover $2490.63 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ASST` | 120 | $20.72 | $2.35 | — | $13.81 | — | combo gate; gate vol=good,blue=True; list oppset; 🔵; ret5=+67.1; leftover $2490.63 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.81 | ▲ close $10,096.05 vs 09:30 $9,997.27 (session +181.18) | 16:00 close · cash $13.81 · equity $10,096.05 vs 09:30 $9,997.27 (+98.78; session marks +181.18) · 4 name(s) marked open→close (per-name table). SLQT×4272 09:30 $0.58 → close $0.55 -140.98; USDE×428 09:30 $5.81 → close $5.98 +72.76; DKS×20 09:30 $121.87 → close $129.66 +155.80; ASST×120 09:30 $20.72 → close $21.50 +93.60 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.81 | ▲ 09:30 equity $10,328.57 vs yday $10,096.05 (+232.52) | 09:30 open · cash $13.81 (unchanged overnight, no fees) · equity $10,328.57 vs prior close $10,096.05 (+232.52) · 4 name(s) re-marked at the open (per-name table). SLQT×4272 yday $0.55 → 09:30 $0.53 -85.44; USDE×428 yday $5.98 → 09:30 $6.50 +222.56; DKS×20 yday $129.66 → 09:30 $128.73 -18.60; ASST×120 yday $21.50 → 09:30 $22.45 +114.00 | — |
| 2026-08-27 09:30 ET | **SELL** | `SLQT` | 4272 | $0.53 | $36.18 | $-300.32 | $2,241.79 | ▼ -300.32 after sell → book $10,292.39; vs 09:30 mark -36.18 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 428 | $6.50 | $5.61 | $+284.18 | $5,018.17 | ▲ +284.18 after sell → book $10,286.77; vs 09:30 mark -5.62 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟡 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 20 | $128.73 | $2.08 | $+133.07 | $7,590.69 | ▲ +133.07 after sell → book $10,284.69; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 120 | $22.45 | $2.39 | $+202.86 | $10,282.30 | ▲ +202.86 after sell → book $10,282.30; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,282.30 | ▲ close $10,282.30 vs 09:30 $10,328.57 (session +0.00) | 16:00 close · cash $10,282.30 · no lots left · equity $10,282.30. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,282.30 | ▲ 09:30 equity $10,282.30 vs yday $10,282.30 (-0.00) | 09:30 open · cash $10,282.30 · no holdings · equity $10,282.30 vs prior close $10,282.30 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 39 | $32.90 | $2.11 | — | $8,997.09 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1285.29 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 16 | $79.42 | $2.04 | — | $7,724.33 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,oppset; 🔵; ret5=+8.5; leftover $1285.29 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $146.07 | $2.01 | — | $6,553.76 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover,oppset; 🔵; ret5=+38.8; leftover $1285.29 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $5,275.90 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover,mover_buy,oppset; 🔵; ⚪; ret5=+14.1; leftover $1285.29 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 55 | $23.30 | $2.15 | — | $3,992.25 | — | combo gate; gate vol=good,blue=True; list ohlc_hot; 🔵; ret5=+14.5; leftover $1285.29 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PLAB` | 42 | $30.01 | $2.12 | — | $2,729.71 | — | combo gate; gate vol=good,blue=True; list oppset; 🔵; ret5=-0.9; leftover $1285.29 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BURL` | 4 | $291.30 | $2.00 | — | $1,562.51 | — | combo gate; gate vol=good,blue=True; list oppset; 🔵; ret5=-13.0; leftover $1285.29 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TRMD` | 39 | $32.23 | $2.11 | — | $303.43 | — | combo gate; gate vol=good,blue=True; list oppset; 🔵; ret5=+2.4; leftover $1285.29 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $303.43 | ▼ close $9,986.84 vs 09:30 $10,282.30 (session -278.90) | 16:00 close · cash $303.43 · equity $9,986.84 vs 09:30 $10,282.30 (-295.46; session marks -278.90) · 8 name(s) marked open→close (per-name table). SEDG×39 09:30 $32.90 → close $31.41 -58.11; URBN×16 09:30 $79.42 → close $81.09 +26.72; ANF×8 09:30 $146.07 → close $148.42 +18.80; SMTC×9 09:30 $141.76 → close $131.17 -95.31; NCNO×55 09:30 $23.30 → close $22.99 -17.05; PLAB×42 09:30 $30.01 → close $27.73 -95.76; BURL×4 09:30 $291.30 → close $272.95 -73.40; TRMD×39 09:30 $32.23 → close $32.62 +15.21 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $303.43 | ▼ 09:30 equity $9,976.75 vs yday $9,986.84 (-10.09) | 09:30 open · cash $303.43 (unchanged overnight, no fees) · equity $9,976.75 vs prior close $9,986.84 (-10.09) · 8 name(s) re-marked at the open (per-name table). SEDG×39 yday $31.41 → 09:30 $31.15 -10.14; URBN×16 yday $81.09 → 09:30 $80.44 -10.40; ANF×8 yday $148.42 → 09:30 $148.03 -3.12; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; NCNO×55 yday $22.99 → 09:30 $22.66 -18.15; PLAB×42 yday $27.73 → 09:30 $28.04 +13.02; BURL×4 yday $272.95 → 09:30 $270.50 -9.80; TRMD×39 yday $32.62 → 09:30 $33.09 +18.33 | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 39 | $31.15 | $2.13 | $-72.48 | $1,516.16 | ▼ -72.48 after sell → book $9,974.63; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 16 | $80.44 | $2.06 | $+12.22 | $2,801.14 | ▲ +12.22 after sell → book $9,972.57; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 8 | $148.03 | $2.03 | $+11.63 | $3,983.34 | ▲ +11.63 after sell → book $9,970.53; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $5,172.01 | ▼ -89.19 after sell → book $9,968.50; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 55 | $22.66 | $2.17 | $-39.53 | $6,416.13 | ▼ -39.53 after sell → book $9,966.32; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `PLAB` | 42 | $28.04 | $2.14 | $-86.99 | $7,591.68 | ▼ -86.99 after sell → book $9,964.19; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BURL` | 4 | $270.50 | $2.02 | $-87.22 | $8,671.65 | ▼ -87.22 after sell → book $9,962.16; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `TRMD` | 39 | $33.09 | $2.13 | $+29.31 | $9,960.04 | ▲ +29.31 after sell → book $9,960.04; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,960.04 | ▲ close $9,960.04 vs 09:30 $9,976.75 (session +0.00) | 16:00 close · cash $9,960.04 · no lots left · equity $9,960.04. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,960.04 | ▲ 09:30 equity $9,960.04 vs yday $9,960.04 (-0.00) | 09:30 open · cash $9,960.04 · no holdings · equity $9,960.04 vs prior close $9,960.04 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,960.04 | ▲ close $9,960.04 vs 09:30 $9,960.04 (session +0.00) | 16:00 close · cash $9,960.04 · no lots left · equity $9,960.04. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,960.04 | ▲ 09:30 equity $9,960.04 vs yday $9,960.04 (-0.00) | 09:30 open · cash $9,960.04 · no holdings · equity $9,960.04 vs prior close $9,960.04 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,960.04 | ▲ close $9,960.04 vs 09:30 $9,960.04 (session +0.00) | 16:00 close · cash $9,960.04 · no lots left · equity $9,960.04. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,960.04 | ▲ 09:30 equity $9,960.04 vs yday $9,960.04 (-0.00) | 09:30 open · cash $9,960.04 · no holdings · equity $9,960.04 vs prior close $9,960.04 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $8,765.97 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1245.00 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 80 | $15.45 | $2.23 | — | $7,527.74 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1245.00 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $6,358.17 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1245.00 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 74 | $16.77 | $2.21 | — | $5,114.97 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy,oppset; 🔵; ⚪; ret5=+5.7; leftover $1245.00 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 571 | $2.18 | $7.37 | — | $3,862.83 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1245.00 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 52 | $23.88 | $2.15 | — | $2,618.92 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+21.9; leftover $1245.00 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DEFT` | 1915 | $0.65 | $18.19 | — | $1,355.98 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+10.2; leftover $1245.00 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 333 | $3.73 | $4.30 | — | $109.59 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=+12.0; leftover $1245.00 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $109.59 | ▼ close $9,822.43 vs 09:30 $9,960.04 (session -97.13) | 16:00 close · cash $109.59 · equity $9,822.43 vs 09:30 $9,960.04 (-137.61; session marks -97.13) · 8 name(s) marked open→close (per-name table). RVTY×9 09:30 $132.45 → close $130.63 -16.38; CRK×80 09:30 $15.45 → close $14.95 -40.00; MRNA×8 09:30 $145.94 → close $148.87 +23.40; ARCT×74 09:30 $16.77 → close $15.56 -89.54; CRDL×571 09:30 $2.18 → close $2.16 -11.42; MMED×52 09:30 $23.88 → close $23.84 -2.08; DEFT×1915 09:30 $0.65 → close $0.68 +55.54; CTMX×333 09:30 $3.73 → close $3.68 -16.65 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $109.59 | ▲ 09:30 equity $9,870.47 vs yday $9,822.43 (+48.04) | 09:30 open · cash $109.59 (unchanged overnight, no fees) · equity $9,870.47 vs prior close $9,822.43 (+48.04) · 8 name(s) re-marked at the open (per-name table). RVTY×9 yday $130.63 → 09:30 $130.03 -5.40; CRK×80 yday $14.95 → 09:30 $15.00 +4.00; MRNA×8 yday $148.87 → 09:30 $153.62 +38.00; ARCT×74 yday $15.56 → 09:30 $15.61 +3.70; CRDL×571 yday $2.16 → 09:30 $2.16 +0.00; MMED×52 yday $23.84 → 09:30 $23.84 +0.00; DEFT×1915 yday $0.68 → 09:30 $0.69 +21.06; CTMX×333 yday $3.68 → 09:30 $3.64 -13.32 | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $1,277.83 | ▼ -25.83 after sell → book $9,868.44; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 80 | $15.00 | $2.25 | $-40.48 | $2,475.57 | ▼ -40.48 after sell → book $9,866.18; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $153.62 | $2.03 | $+57.35 | $3,702.50 | ▲ +57.35 after sell → book $9,864.15; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 74 | $15.61 | $2.23 | $-90.29 | $4,855.41 | ▼ -90.29 after sell → book $9,861.92; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 571 | $2.16 | $7.47 | $-26.26 | $6,081.29 | ▼ -26.26 after sell → book $9,854.44; vs 09:30 mark -7.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 52 | $23.84 | $2.17 | $-6.39 | $7,318.81 | ▼ -6.39 after sell → book $9,852.28; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `DEFT` | 1915 | $0.69 | $19.29 | $+39.12 | $8,620.87 | ▲ +39.12 after sell → book $9,832.99; vs 09:30 mark -19.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CTMX` | 333 | $3.64 | $4.36 | $-38.63 | $9,828.63 | ▼ -38.63 after sell → book $9,828.63; vs 09:30 mark -4.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 355 | $3.46 | $4.58 | — | $8,595.75 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1228.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 487 | $2.52 | $6.28 | — | $7,362.23 | — | combo gate; gate vol=good,blue=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1228.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 183 | $6.71 | $2.54 | — | $6,131.76 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1228.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 646 | $1.90 | $8.33 | — | $4,896.03 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1228.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 257 | $4.78 | $3.32 | — | $3,664.25 | — | combo gate; gate vol=good,blue=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1228.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 108 | $11.31 | $2.31 | — | $2,440.46 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1228.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 349 | $3.52 | $4.50 | — | $1,207.48 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.3; leftover $1228.58 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $177.92 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy,oppset; 🔵; ⚪; ret5=+9.3; leftover $1228.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $177.92 | ▲ close $9,864.44 vs 09:30 $9,870.47 (session +69.67) | 16:00 close · cash $177.92 · equity $9,864.44 vs 09:30 $9,870.47 (-6.03; session marks +69.67) · 8 name(s) marked open→close (per-name table). CABA×355 09:30 $3.46 → close $3.47 +3.55; ALEC×487 09:30 $2.52 → close $2.46 -29.22; BHC×183 09:30 $6.71 → close $6.56 -27.45; BMEA×646 09:30 $1.90 → close $2.03 +83.98; OABI×257 09:30 $4.78 → close $4.33 -115.65; VIR×108 09:30 $11.31 → close $11.38 +8.10; EOSE×349 09:30 $3.52 → close $3.88 +125.64; DELL×2 09:30 $513.78 → close $524.14 +20.72 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $177.92 | ▼ 09:30 equity $9,800.61 vs yday $9,864.44 (-63.83) | 09:30 open · cash $177.92 (unchanged overnight, no fees) · equity $9,800.61 vs prior close $9,864.44 (-63.83) · 8 name(s) re-marked at the open (per-name table). CABA×355 yday $3.47 → 09:30 $3.43 -14.20; ALEC×487 yday $2.46 → 09:30 $2.38 -38.96; BHC×183 yday $6.56 → 09:30 $6.57 +1.83; BMEA×646 yday $2.03 → 09:30 $2.00 -19.38; OABI×257 yday $4.33 → 09:30 $4.30 -7.71; VIR×108 yday $11.38 → 09:30 $11.22 -17.82; EOSE×349 yday $3.88 → 09:30 $3.99 +38.39; DELL×2 yday $524.14 → 09:30 $521.15 -5.98 | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 355 | $3.43 | $4.65 | $-19.88 | $1,390.92 | ▼ -19.88 after sell → book $9,795.96; vs 09:30 mark -4.65 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 487 | $2.38 | $6.37 | $-80.84 | $2,543.61 | ▼ -80.84 after sell → book $9,789.59; vs 09:30 mark -6.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 183 | $6.57 | $2.58 | $-30.74 | $3,743.34 | ▼ -30.74 after sell → book $9,787.01; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 646 | $2.00 | $8.45 | $+47.82 | $5,026.89 | ▲ +47.82 after sell → book $9,778.56; vs 09:30 mark -8.45 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 257 | $4.30 | $3.37 | $-130.04 | $6,128.62 | ▼ -130.04 after sell → book $9,775.19; vs 09:30 mark -3.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 108 | $11.22 | $2.34 | $-14.38 | $7,338.04 | ▼ -14.38 after sell → book $9,772.85; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `EOSE` | 349 | $3.99 | $4.57 | $+154.96 | $8,725.98 | ▲ +154.96 after sell → book $9,768.28; vs 09:30 mark -4.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $9,766.26 | ▲ +10.73 after sell → book $9,766.26; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,766.26 | ▲ close $9,766.26 vs 09:30 $9,800.61 (session +0.00) | 16:00 close · cash $9,766.26 · no lots left · equity $9,766.26. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,766.26 | ▲ 09:30 equity $9,766.26 vs yday $9,766.26 (+0.00) | 09:30 open · cash $9,766.26 · no holdings · equity $9,766.26 vs prior close $9,766.26 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,766.26 | ▲ close $9,766.26 vs 09:30 $9,766.26 (session +0.00) | 16:00 close · cash $9,766.26 · no lots left · equity $9,766.26. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,766.26 | ▲ 09:30 equity $9,766.26 vs yday $9,766.26 (+0.00) | 09:30 open · cash $9,766.26 · no holdings · equity $9,766.26 vs prior close $9,766.26 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,766.26 | ▲ close $9,766.26 vs 09:30 $9,766.26 (session +0.00) | 16:00 close · cash $9,766.26 · no lots left · equity $9,766.26. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,766.26 | ▲ 09:30 equity $9,766.26 vs yday $9,766.26 (+0.00) | 09:30 open · cash $9,766.26 · no holdings · equity $9,766.26 vs prior close $9,766.26 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 452 | $2.70 | $5.83 | — | $8,540.03 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1220.78 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 111 | $10.95 | $2.32 | — | $7,322.26 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover,oppset; 🔵; ret5=+20.8; leftover $1220.78 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 248 | $4.91 | $3.20 | — | $6,101.38 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $1220.78 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ANGX` | 226 | $5.38 | $2.92 | — | $4,882.58 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=+19.8; leftover $1220.78 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `TSSI` | 135 | $8.98 | $2.40 | — | $3,667.89 | — | combo gate; gate vol=good,blue=True; list yday_gainer,oppset; 🔵; ret5=+14.1; leftover $1220.78 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `LDI` | 1436 | $0.85 | $16.51 | — | $2,430.77 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=-7.8; leftover $1220.78 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 22 | $54.91 | $2.06 | — | $1,220.70 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=+24.3; leftover $1220.78 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 197 | $6.16 | $2.58 | — | $4.60 | — | combo gate; gate vol=good,blue=True; list yday_gainer,oppset; 🔵; ret5=+36.4; leftover $1220.78 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.60 | ▼ close $9,643.33 vs 09:30 $9,766.26 (session -85.12) | 16:00 close · cash $4.60 · equity $9,643.33 vs 09:30 $9,766.26 (-122.93; session marks -85.12) · 8 name(s) marked open→close (per-name table). INDP×452 09:30 $2.70 → close $2.77 +31.64; WLTH×111 09:30 $10.95 → close $10.38 -63.27; BNC×248 09:30 $4.91 → close $4.80 -27.28; ANGX×226 09:30 $5.38 → close $5.45 +15.82; TSSI×135 09:30 $8.98 → close $8.93 -6.75; LDI×1436 09:30 $0.85 → close $0.83 -21.54; ASO×22 09:30 $54.91 → close $55.36 +9.90; IRD×197 09:30 $6.16 → close $6.04 -23.64 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.60 | ▲ 09:30 equity $9,669.40 vs yday $9,643.33 (+26.07) | 09:30 open · cash $4.60 (unchanged overnight, no fees) · equity $9,669.40 vs prior close $9,643.33 (+26.07) · 8 name(s) re-marked at the open (per-name table). INDP×452 yday $2.77 → 09:30 $2.80 +13.56; WLTH×111 yday $10.38 → 09:30 $10.29 -9.99; BNC×248 yday $4.80 → 09:30 $5.03 +57.04; ANGX×226 yday $5.45 → 09:30 $5.57 +27.12; TSSI×135 yday $8.93 → 09:30 $8.57 -48.60; LDI×1436 yday $0.83 → 09:30 $0.84 +4.31; ASO×22 yday $55.36 → 09:30 $54.75 -13.42; IRD×197 yday $6.04 → 09:30 $6.02 -3.94 | — |
| 2026-09-14 09:30 ET | **SELL** | `INDP` | 452 | $2.80 | $5.92 | $+33.45 | $1,264.28 | ▲ +33.45 after sell → book $9,663.49; vs 09:30 mark -5.91 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🟡 digest🔴 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 111 | $10.29 | $2.35 | $-77.93 | $2,404.12 | ▼ -77.93 after sell → book $9,661.14; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `BNC` | 248 | $5.03 | $3.25 | $+23.31 | $3,648.31 | ▲ +23.31 after sell → book $9,657.89; vs 09:30 mark -3.25 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `ANGX` | 226 | $5.57 | $2.96 | $+37.06 | $4,904.17 | ▲ +37.06 after sell → book $9,654.92; vs 09:30 mark -2.97 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `TSSI` | 135 | $8.57 | $2.43 | $-60.17 | $6,058.69 | ▼ -60.17 after sell → book $9,652.50; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `LDI` | 1436 | $0.84 | $16.59 | $-50.34 | $7,245.47 | ▼ -50.34 after sell → book $9,635.91; vs 09:30 mark -16.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ASO` | 22 | $54.75 | $2.08 | $-7.65 | $8,447.89 | ▼ -7.65 after sell → book $9,633.83; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 197 | $6.02 | $2.62 | $-32.78 | $9,631.21 | ▼ -32.78 after sell → book $9,631.21; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🔴 judge🟡 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,631.21 | ▲ close $9,631.21 vs 09:30 $9,669.40 (session +0.00) | 16:00 close · cash $9,631.21 · no lots left · equity $9,631.21. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,631.21 | ▲ 09:30 equity $9,631.21 vs yday $9,631.21 (-0.00) | 09:30 open · cash $9,631.21 · no holdings · equity $9,631.21 vs prior close $9,631.21 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,631.21 | ▲ close $9,631.21 vs 09:30 $9,631.21 (session +0.00) | 16:00 close · cash $9,631.21 · no lots left · equity $9,631.21. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,631.21 | ▲ 09:30 equity $9,631.21 vs yday $9,631.21 (-0.00) | 09:30 open · cash $9,631.21 · no holdings · equity $9,631.21 vs prior close $9,631.21 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 205 | $5.87 | $2.64 | — | $8,425.21 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1203.90 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 catal🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 13 | $87.40 | $2.03 | — | $7,286.98 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1203.90 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 44 | $27.09 | $2.12 | — | $6,092.90 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1203.90 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 13 | $89.38 | $2.03 | — | $4,928.93 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $1203.90 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 51 | $23.29 | $2.14 | — | $3,739.00 | — | combo gate; gate vol=good,blue=True; list yday_gainer,oppset; 🔵; ret5=+16.1; leftover $1203.90 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 36 | $33.14 | $2.10 | — | $2,543.86 | — | combo gate; gate vol=good,blue=True; list yday_gainer,oppset; 🔵; ret5=-2.9; leftover $1203.90 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 10 | $118.18 | $2.02 | — | $1,360.04 | — | combo gate; gate vol=good,blue=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $1203.90 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `RVTY` | 8 | $140.88 | $2.01 | — | $230.99 | — | combo gate; gate vol=good,blue=True; list yday_gainer,ohlc_hot; 🔵; ret5=+10.3; leftover $1203.90 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $230.99 | ▼ close $9,549.81 vs 09:30 $9,631.21 (session -64.30) | 16:00 close · cash $230.99 · equity $9,549.81 vs 09:30 $9,631.21 (-81.40; session marks -64.30) · 8 name(s) marked open→close (per-name table). RIG×205 09:30 $5.87 → close $5.54 -67.65; VAL×13 09:30 $87.40 → close $82.52 -63.44; ADPT×44 09:30 $27.09 → close $27.67 +25.52; SWKS×13 09:30 $89.38 → close $85.59 -49.27; SDGR×51 09:30 $23.29 → close $23.93 +32.64; FPS×36 09:30 $33.14 → close $34.84 +61.20; QRVO×10 09:30 $118.18 → close $113.97 -42.10; RVTY×8 09:30 $140.88 → close $145.73 +38.80 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $230.99 | ▲ 09:30 equity $9,708.32 vs yday $9,549.81 (+158.51) | 09:30 open · cash $230.99 (unchanged overnight, no fees) · equity $9,708.32 vs prior close $9,549.81 (+158.51) · 8 name(s) re-marked at the open (per-name table). RIG×205 yday $5.54 → 09:30 $5.58 +8.20; VAL×13 yday $82.52 → 09:30 $83.20 +8.84; ADPT×44 yday $27.67 → 09:30 $28.23 +24.64; SWKS×13 yday $85.59 → 09:30 $86.76 +15.21; SDGR×51 yday $23.93 → 09:30 $24.09 +8.16; FPS×36 yday $34.84 → 09:30 $36.76 +69.12; QRVO×10 yday $113.97 → 09:30 $114.90 +9.30; RVTY×8 yday $145.73 → 09:30 $147.61 +15.04 | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 205 | $5.58 | $2.69 | $-64.78 | $1,372.20 | ▼ -64.78 after sell → book $9,705.63; vs 09:30 mark -2.69 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 13 | $83.20 | $2.05 | $-58.68 | $2,451.75 | ▼ -58.68 after sell → book $9,703.58; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 44 | $28.23 | $2.14 | $+45.90 | $3,691.73 | ▲ +45.90 after sell → book $9,701.44; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 13 | $86.76 | $2.05 | $-38.14 | $4,817.56 | ▼ -38.14 after sell → book $9,699.39; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 51 | $24.09 | $2.16 | $+36.49 | $6,043.98 | ▲ +36.49 after sell → book $9,697.22; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `FPS` | 36 | $36.76 | $2.12 | $+126.10 | $7,365.23 | ▲ +126.10 after sell → book $9,695.11; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `QRVO` | 10 | $114.90 | $2.04 | $-36.86 | $8,512.19 | ▼ -36.86 after sell → book $9,693.07; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RVTY` | 8 | $147.61 | $2.03 | $+49.79 | $9,691.03 | ▲ +49.79 after sell → book $9,691.03; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 159 | $7.59 | $2.47 | — | $8,481.76 | — | combo gate; gate vol=good,blue=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1211.38 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 46 | $25.95 | $2.13 | — | $7,285.93 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,ohlc_hot,oppset; 🔵; ret5=+9.6; leftover $1211.38 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 7 | $170.85 | $2.01 | — | $6,087.97 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1211.38 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 504 | $2.40 | $6.50 | — | $4,871.86 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $1211.38 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 67 | $18.04 | $2.19 | — | $3,661.33 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; leftover $1211.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `EMAT` | 313 | $3.86 | $4.04 | — | $2,449.11 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+18.7; leftover $1211.38 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `CYPH` | 452 | $2.67 | $5.83 | — | $1,234.18 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=-0.4; leftover $1211.38 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `BRKR` | 19 | $61.90 | $2.05 | — | $56.03 | — | combo gate; gate vol=good,blue=True; list yday_gainer,ohlc_hot; 🔵; ret5=+11.5; leftover $1211.38 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.03 | ▲ close $9,919.96 vs 09:30 $9,708.32 (session +256.15) | 16:00 close · cash $56.03 · equity $9,919.96 vs 09:30 $9,708.32 (+211.64; session marks +256.15) · 8 name(s) marked open→close (per-name table). PGEN×159 09:30 $7.59 → close $7.87 +44.52; ARQT×46 09:30 $25.95 → close $26.46 +23.46; SMTC×7 09:30 $170.85 → close $178.19 +51.38; SABR×504 09:30 $2.40 → close $2.32 -40.32; CIFR×67 09:30 $18.04 → close $16.94 -73.36; EMAT×313 09:30 $3.86 → close $4.02 +50.08; CYPH×452 09:30 $2.67 → close $3.07 +178.54; BRKR×19 09:30 $61.90 → close $63.05 +21.85 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.03 | ▲ 09:30 equity $9,968.82 vs yday $9,919.96 (+48.86) | 09:30 open · cash $56.03 (unchanged overnight, no fees) · equity $9,968.82 vs prior close $9,919.96 (+48.86) · 8 name(s) re-marked at the open (per-name table). PGEN×159 yday $7.87 → 09:30 $7.98 +17.49; ARQT×46 yday $26.46 → 09:30 $26.14 -14.72; SMTC×7 yday $178.19 → 09:30 $182.33 +28.98; SABR×504 yday $2.32 → 09:30 $2.29 -15.12; CIFR×67 yday $16.94 → 09:30 $17.80 +57.62; EMAT×313 yday $4.02 → 09:30 $3.97 -15.65; CYPH×452 yday $3.07 → 09:30 $3.04 -15.82; BRKR×19 yday $63.05 → 09:30 $63.37 +6.08 | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 159 | $7.98 | $2.50 | $+57.04 | $1,322.35 | ▲ +57.04 after sell → book $9,966.32; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 7 | $182.33 | $2.03 | $+76.32 | $2,596.63 | ▲ +76.32 after sell → book $9,964.29; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 504 | $2.29 | $6.60 | $-68.54 | $3,744.19 | ▼ -68.54 after sell → book $9,957.69; vs 09:30 mark -6.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CIFR` | 67 | $17.80 | $2.21 | $-20.15 | $4,934.58 | ▼ -20.15 after sell → book $9,955.48; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `EMAT` | 313 | $3.97 | $4.10 | $+26.29 | $6,173.09 | ▲ +26.29 after sell → book $9,951.38; vs 09:30 mark -4.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CYPH` | 452 | $3.04 | $5.92 | $+150.97 | $7,538.99 | ▲ +150.97 after sell → book $9,945.46; vs 09:30 mark -5.92 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `BRKR` | 19 | $63.37 | $2.07 | $+23.82 | $8,740.96 | ▲ +23.82 after sell → book $9,943.40; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `ILMN` | 5 | $249.13 | $2.00 | — | $7,493.30 | — | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=+21.8; leftover $1248.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 42 | $29.32 | $2.12 | — | $6,259.75 | — | combo gate; gate vol=good,blue=True; list flatten,yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+60.9; leftover $1248.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `FTRE` | 62 | $20.10 | $2.18 | — | $5,011.37 | — | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=+19.2; leftover $1248.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 1287 | $0.97 | $16.34 | — | $3,746.64 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $1248.71 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 316 | $3.95 | $4.08 | — | $2,494.36 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $1248.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 88 | $14.07 | $2.25 | — | $1,253.95 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1248.71 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 84 | $14.79 | $2.24 | — | $9.34 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $1248.71 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.34 | ▼ close $9,634.59 vs 09:30 $9,968.82 (session -277.59) | 16:00 close · cash $9.34 · equity $9,634.59 vs 09:30 $9,968.82 (-334.23; session marks -277.59) · 8 name(s) marked open→close (per-name table). ARQT×46 09:30 $26.14 → close $25.38 -34.96; ILMN×5 09:30 $249.13 → close $239.62 -47.55; SDGR×42 09:30 $29.32 → close $29.02 -12.60; FTRE×62 09:30 $20.10 → close $19.93 -10.54; TLSA×1287 09:30 $0.97 → close $0.91 -77.22; EYPT×316 09:30 $3.95 → close $3.85 -31.60; BHVN×88 09:30 $14.07 → close $13.62 -39.60; RARE×84 09:30 $14.79 → close $14.51 -23.52 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SRPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `AFRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HQY` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `GENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `YDDL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ZS` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SLDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `LAC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBA` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `FJET` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DHT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `KHC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DBRG` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `MTN` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CLX` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `FLOC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `RPD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `LFMD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBLX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RUM` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ARQT` | 46 | 2026-09-17 @ $25.95 | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,ohlc_hot,oppset; 🔵; ret5=+9.6; leftover $1211.38 |
| `ILMN` | 5 | 2026-09-18 @ $249.13 | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=+21.8; leftover $1248.71 |
| `SDGR` | 42 | 2026-09-18 @ $29.32 | combo gate; gate vol=good,blue=True; list flatten,yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+60.9; leftover $1248.71 |
| `FTRE` | 62 | 2026-09-18 @ $20.10 | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=+19.2; leftover $1248.71 |
| `TLSA` | 1287 | 2026-09-18 @ $0.97 | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $1248.71 |
| `EYPT` | 316 | 2026-09-18 @ $3.95 | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $1248.71 |
| `BHVN` | 88 | 2026-09-18 @ $14.07 | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1248.71 |
| `RARE` | 84 | 2026-09-18 @ $14.79 | combo gate; gate vol=good,blue=True; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $1248.71 |
