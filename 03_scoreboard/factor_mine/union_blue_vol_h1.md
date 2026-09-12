# Factor mine action — `union_blue_vol_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-5.38%** ($9,463) · signal-only (no cash/fees) was +2.71%. Starts YES **4/21**. Fills 136 · skips 34 · realized $-340.30.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $88.89.

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
| 2026-08-26 | `SLQT` | 5696 | — | $0.58 | +0.00 | $0.55 | -187.97 | -187.97 | +0.00 | -187.97 |
| 2026-08-26 | `USDE` | 571 | — | $5.81 | +0.00 | $5.98 | +97.07 | +97.07 | +0.00 | +97.07 |
| 2026-08-26 | `DKS` | 26 | — | $121.87 | +0.00 | $129.66 | +202.54 | +202.54 | +0.00 | +202.54 |
| 2026-08-27 | `SLQT` | 5696 | $0.55 | $0.53 | -113.92 | — | +0.00 | -113.92 | -301.89 | — |
| 2026-08-27 | `USDE` | 571 | $5.98 | $6.50 | +296.92 | — | +0.00 | +296.92 | +393.99 | — |
| 2026-08-27 | `DKS` | 26 | $129.66 | $128.73 | -24.18 | — | +0.00 | -24.18 | +178.36 | — |
| 2026-08-28 | `SEDG` | 61 | — | $32.90 | +0.00 | $31.41 | -90.89 | -90.89 | +0.00 | -90.89 |
| 2026-08-28 | `URBN` | 25 | — | $79.42 | +0.00 | $81.09 | +41.75 | +41.75 | +0.00 | +41.75 |
| 2026-08-28 | `ANF` | 13 | — | $146.07 | +0.00 | $148.42 | +30.55 | +30.55 | +0.00 | +30.55 |
| 2026-08-28 | `SMTC` | 14 | — | $141.76 | +0.00 | $131.17 | -148.26 | -148.26 | +0.00 | -148.26 |
| 2026-08-28 | `NCNO` | 86 | — | $23.30 | +0.00 | $22.99 | -26.66 | -26.66 | +0.00 | -26.66 |
| 2026-08-31 | `SEDG` | 61 | $31.41 | $31.15 | -15.86 | — | +0.00 | -15.86 | -106.75 | — |
| 2026-08-31 | `URBN` | 25 | $81.09 | $80.44 | -16.25 | — | +0.00 | -16.25 | +25.50 | — |
| 2026-08-31 | `ANF` | 13 | $148.42 | $148.03 | -5.07 | — | +0.00 | -5.07 | +25.48 | — |
| 2026-08-31 | `SMTC` | 14 | $131.17 | $132.30 | +15.82 | — | +0.00 | +15.82 | -132.44 | — |
| 2026-08-31 | `NCNO` | 86 | $22.99 | $22.66 | -28.38 | — | +0.00 | -28.38 | -55.04 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `RVTY` | 9 | — | $132.45 | +0.00 | $130.63 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-03 | `CRK` | 79 | — | $15.45 | +0.00 | $14.95 | -39.50 | -39.50 | +0.00 | -39.50 |
| 2026-09-03 | `MRNA` | 8 | — | $145.94 | +0.00 | $148.87 | +23.40 | +23.40 | +0.00 | +23.40 |
| 2026-09-03 | `ARCT` | 73 | — | $16.77 | +0.00 | $15.56 | -88.33 | -88.33 | +0.00 | -88.33 |
| 2026-09-03 | `CRDL` | 564 | — | $2.18 | +0.00 | $2.16 | -11.28 | -11.28 | +0.00 | -11.28 |
| 2026-09-03 | `MMED` | 51 | — | $23.88 | +0.00 | $23.84 | -2.04 | -2.04 | +0.00 | -2.04 |
| 2026-09-03 | `DEFT` | 1894 | — | $0.65 | +0.00 | $0.68 | +54.93 | +54.93 | +0.00 | +54.93 |
| 2026-09-03 | `CTMX` | 330 | — | $3.73 | +0.00 | $3.68 | -16.50 | -16.50 | +0.00 | -16.50 |
| 2026-09-04 | `RVTY` | 9 | $130.63 | $130.03 | -5.40 | — | +0.00 | -5.40 | -21.78 | — |
| 2026-09-04 | `CRK` | 79 | $14.95 | $15.00 | +3.95 | — | +0.00 | +3.95 | -35.55 | — |
| 2026-09-04 | `MRNA` | 8 | $148.87 | $153.62 | +38.00 | — | +0.00 | +38.00 | +61.40 | — |
| 2026-09-04 | `ARCT` | 73 | $15.56 | $15.61 | +3.65 | — | +0.00 | +3.65 | -84.68 | — |
| 2026-09-04 | `CRDL` | 564 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -11.28 | — |
| 2026-09-04 | `MMED` | 51 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.04 | — |
| 2026-09-04 | `DEFT` | 1894 | $0.68 | $0.69 | +20.83 | — | +0.00 | +20.83 | +75.76 | — |
| 2026-09-04 | `CTMX` | 330 | $3.68 | $3.64 | -13.20 | — | +0.00 | -13.20 | -29.70 | — |
| 2026-09-04 | `CABA` | 351 | — | $3.46 | +0.00 | $3.47 | +3.51 | +3.51 | +0.00 | +3.51 |
| 2026-09-04 | `ALEC` | 482 | — | $2.52 | +0.00 | $2.46 | -28.92 | -28.92 | +0.00 | -28.92 |
| 2026-09-04 | `BHC` | 181 | — | $6.71 | +0.00 | $6.56 | -27.15 | -27.15 | +0.00 | -27.15 |
| 2026-09-04 | `BMEA` | 639 | — | $1.90 | +0.00 | $2.03 | +83.07 | +83.07 | +0.00 | +83.07 |
| 2026-09-04 | `OABI` | 254 | — | $4.78 | +0.00 | $4.33 | -114.30 | -114.30 | +0.00 | -114.30 |
| 2026-09-04 | `VIR` | 107 | — | $11.31 | +0.00 | $11.38 | +8.02 | +8.02 | +0.00 | +8.02 |
| 2026-09-04 | `EOSE` | 345 | — | $3.52 | +0.00 | $3.88 | +124.20 | +124.20 | +0.00 | +124.20 |
| 2026-09-04 | `DELL` | 2 | — | $513.78 | +0.00 | $524.14 | +20.72 | +20.72 | +0.00 | +20.72 |
| 2026-09-08 | `CABA` | 351 | $3.47 | $3.43 | -14.04 | — | +0.00 | -14.04 | -10.53 | — |
| 2026-09-08 | `ALEC` | 482 | $2.46 | $2.38 | -38.56 | — | +0.00 | -38.56 | -67.48 | — |
| 2026-09-08 | `BHC` | 181 | $6.56 | $6.57 | +1.81 | — | +0.00 | +1.81 | -25.34 | — |
| 2026-09-08 | `BMEA` | 639 | $2.03 | $2.00 | -19.17 | — | +0.00 | -19.17 | +63.90 | — |
| 2026-09-08 | `OABI` | 254 | $4.33 | $4.30 | -7.62 | — | +0.00 | -7.62 | -121.92 | — |
| 2026-09-08 | `VIR` | 107 | $11.38 | $11.22 | -17.65 | — | +0.00 | -17.65 | -9.63 | — |
| 2026-09-08 | `EOSE` | 345 | $3.88 | $3.99 | +37.95 | — | +0.00 | +37.95 | +162.15 | — |
| 2026-09-08 | `DELL` | 2 | $524.14 | $521.15 | -5.98 | — | +0.00 | -5.98 | +14.74 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 7 | — | $164.43 | +0.00 | $150.28 | -99.05 | -99.05 | +0.00 | -99.05 |
| 2026-09-11 | `INDP` | 447 | — | $2.70 | +0.00 | $2.77 | +31.29 | +31.29 | +0.00 | +31.29 |
| 2026-09-11 | `WLTH` | 110 | — | $10.95 | +0.00 | $10.38 | -62.70 | -62.70 | +0.00 | -62.70 |
| 2026-09-11 | `BNC` | 245 | — | $4.91 | +0.00 | $4.80 | -26.95 | -26.95 | +0.00 | -26.95 |
| 2026-09-11 | `ANGX` | 224 | — | $5.38 | +0.00 | $5.45 | +15.68 | +15.68 | +0.00 | +15.68 |
| 2026-09-11 | `TSSI` | 134 | — | $8.98 | +0.00 | $8.93 | -6.70 | -6.70 | +0.00 | -6.70 |
| 2026-09-11 | `LDI` | 1420 | — | $0.85 | +0.00 | $0.83 | -21.30 | -21.30 | +0.00 | -21.30 |
| 2026-09-11 | `ASO` | 21 | — | $54.91 | +0.00 | $55.36 | +9.45 | +9.45 | +0.00 | +9.45 |

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
| 2026-08-26 | +2.02 | $2.82 | CAPR×168, KURA×89, CCOI×128, LIFE×33, ZIP×268, BMEA×748, NPWR×610, PUSA×317 | $9,997.27 | +7.50 | +111.64 | SLQT, USDE, DKS | CAPR, KURA, CCOI, LIFE, ZIP, BMEA, NPWR, PUSA | $95.88 | $10,014.42 | SLQT×5696, USDE×571, DKS×26 |
| 2026-08-27 | — | $95.88 | SLQT×5696, USDE×571, DKS×26 | $10,173.24 | +158.82 | +0.00 | — | SLQT, USDE, DKS | $10,115.40 | $10,115.40 | — |
| 2026-08-28 | +0.75 | $10,115.40 | — | $10,115.40 | +0.00 | -193.51 | SEDG, URBN, ANF, SMTC, NCNO | — | $225.10 | $9,911.34 | SEDG×61, URBN×25, ANF×13, SMTC×14, NCNO×86 |
| 2026-08-31 | -5.85 | $225.10 | SEDG×61, URBN×25, ANF×13, SMTC×14, NCNO×86 | $9,861.60 | -49.74 | +0.00 | — | SEDG, URBN, ANF, SMTC, NCNO | $9,850.93 | $9,850.93 | — |
| 2026-09-01 | -6.30 | $9,850.93 | — | $9,850.93 | -0.00 | +0.00 | — | — | $9,850.93 | $9,850.93 | — |
| 2026-09-02 | -3.83 | $9,850.93 | — | $9,850.93 | -0.00 | +0.00 | — | — | $9,850.93 | $9,850.93 | — |
| 2026-09-03 | -0.90 | $9,850.93 | — | $9,850.93 | -0.00 | -95.70 | RVTY, CRK, MRNA, ARCT, CRDL, MMED, DEFT, CTMX | — | $97.02 | $9,715.09 | RVTY×9, CRK×79, MRNA×8, ARCT×73, CRDL×564, MMED×51, DEFT×1894, CTMX×330 |
| 2026-09-04 | +2.25 | $97.02 | RVTY×9, CRK×79, MRNA×8, ARCT×73, CRDL×564, MMED×51, DEFT×1894, CTMX×330 | $9,762.92 | +47.83 | +69.15 | CABA, ALEC, BHC, BMEA, OABI, VIR, EOSE, DELL | RVTY, CRK, MRNA, ARCT, CRDL, MMED, DEFT, CTMX | $163.91 | $9,757.03 | CABA×351, ALEC×482, BHC×181, BMEA×639, OABI×254, VIR×107, EOSE×345, DELL×2 |
| 2026-09-08 | -11.47 | $163.91 | CABA×351, ALEC×482, BHC×181, BMEA×639, OABI×254, VIR×107, EOSE×345, DELL×2 | $9,693.76 | -63.27 | +0.00 | — | CABA, ALEC, BHC, BMEA, OABI, VIR, EOSE, DELL | $9,659.72 | $9,659.72 | — |
| 2026-09-09 | -13.95 | $9,659.72 | — | $9,659.72 | +0.00 | +0.00 | — | — | $9,659.72 | $9,659.72 | — |
| 2026-09-10 | -13.28 | $9,659.72 | — | $9,659.72 | +0.00 | +0.00 | — | — | $9,659.72 | $9,659.72 | — |
| 2026-09-11 | +0.50 | $9,659.72 | — | $9,659.72 | +0.00 | -160.28 | ORCL, INDP, WLTH, BNC, ANGX, TSSI, LDI, ASO | — | $88.89 | $9,462.52 | ORCL×7, INDP×447, WLTH×110, BNC×245, ANGX×224, TSSI×134, LDI×1420, ASO×21 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $8,739.75 | — | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 84 | $14.80 | $2.24 | — | $7,494.31 | — | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $6,240.67 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $4,986.99 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $3,747.28 | — | combo gate; gate vol=good,blue=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $2,512.19 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $1,264.42 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $10.28 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.28 | ▼ close $9,797.82 vs 09:30 $10,000.00 (session -168.89) | 16:00 close · cash $10.28 · equity $9,797.82 vs 09:30 $10,000.00 (-202.18; session marks -168.89) · 8 name(s) marked open→close (per-name table). BTBT×833 09:30 $1.50 → close $1.57 +58.31; BETR×84 09:30 $14.80 → close $13.73 -89.88; ANGX×290 09:30 $4.31 → close $4.37 +17.40; HYLN×299 09:30 $4.18 → close $4.06 -35.88; ADUR×75 09:30 $16.50 → close $16.17 -24.75; ARX×63 09:30 $19.57 → close $19.58 +0.63; AIRO×112 09:30 $11.12 → close $9.57 -173.60; NCMI×464 09:30 $2.69 → close $2.86 +78.88 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.28 | ▼ 09:30 equity $9,768.32 vs yday $9,797.82 (-29.50) | 09:30 open · cash $10.28 (unchanged overnight, no fees) · equity $9,768.32 vs prior close $9,797.82 (-29.50) · 8 name(s) re-marked at the open (per-name table). BTBT×833 yday $1.57 → 09:30 $1.52 -41.65; BETR×84 yday $13.73 → 09:30 $13.67 -5.04; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; HYLN×299 yday $4.06 → 09:30 $4.10 +11.96; ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; AIRO×112 yday $9.57 → 09:30 $9.57 +0.00; NCMI×464 yday $2.86 → 09:30 $2.80 -27.84 | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 833 | $1.52 | $10.89 | $-4.98 | $1,265.54 | ▼ -4.98 after sell → book $9,757.42; vs 09:30 mark -10.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 84 | $13.67 | $2.27 | $-99.43 | $2,411.56 | ▼ -99.43 after sell → book $9,755.16; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $3,741.76 | ▲ +76.56 after sell → book $9,751.36; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 299 | $4.10 | $3.92 | $-31.69 | $4,963.74 | ▼ -31.69 after sell → book $9,747.44; vs 09:30 mark -3.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 75 | $15.73 | $2.24 | $-62.20 | $6,141.25 | ▼ -62.20 after sell → book $9,745.20; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $7,371.97 | ▼ -4.38 after sell → book $9,743.01; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 112 | $9.57 | $2.35 | $-178.28 | $8,441.45 | ▼ -178.28 after sell → book $9,740.65; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
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
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 7 | $144.54 | $2.01 | — | $149.17 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1108.90 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
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
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 104 | $11.13 | $2.30 | — | $4,793.27 | — | combo gate; gate vol=good,blue=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1158.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
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
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 168 | $7.25 | $2.49 | — | $8,541.76 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1220.28 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 89 | $13.59 | $2.26 | — | $7,329.99 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1220.28 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 128 | $9.49 | $2.37 | — | $6,112.90 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1220.28 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 33 | $36.96 | $2.09 | — | $4,891.13 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1220.28 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 268 | $4.55 | $3.46 | — | $3,668.27 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1220.28 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 748 | $1.63 | $9.65 | — | $2,439.38 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1220.28 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 610 | $2.00 | $7.87 | — | $1,211.51 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+15.0; leftover $1220.28 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 317 | $3.80 | $4.09 | — | $2.82 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $1220.28 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.82 | ▲ close $9,989.77 vs 09:30 $9,762.25 (session +261.80) | 16:00 close · cash $2.82 · equity $9,989.77 vs 09:30 $9,762.25 (+227.52; session marks +261.80) · 8 name(s) marked open→close (per-name table). CAPR×168 09:30 $7.25 → close $8.29 +174.72; KURA×89 09:30 $13.59 → close $13.59 +0.00; CCOI×128 09:30 $9.49 → close $9.88 +49.92; LIFE×33 09:30 $36.96 → close $38.56 +52.80; ZIP×268 09:30 $4.55 → close $4.35 -53.60; BMEA×748 09:30 $1.63 → close $1.73 +74.80; NPWR×610 09:30 $2.00 → close $1.95 -30.50; PUSA×317 09:30 $3.80 → close $3.78 -6.34 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.82 | ▲ 09:30 equity $9,997.27 vs yday $9,989.77 (+7.50) | 09:30 open · cash $2.82 (unchanged overnight, no fees) · equity $9,997.27 vs prior close $9,989.77 (+7.50) · 8 name(s) re-marked at the open (per-name table). CAPR×168 yday $8.29 → 09:30 $8.29 +0.00; KURA×89 yday $13.59 → 09:30 $13.63 +3.56; CCOI×128 yday $9.88 → 09:30 $9.89 +1.28; LIFE×33 yday $38.56 → 09:30 $38.24 -10.56; ZIP×268 yday $4.35 → 09:30 $4.31 -10.72; BMEA×748 yday $1.73 → 09:30 $1.75 +18.70; NPWR×610 yday $1.95 → 09:30 $1.93 -12.20; PUSA×317 yday $3.78 → 09:30 $3.83 +17.44 | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 168 | $8.29 | $2.53 | $+169.69 | $1,393.01 | ▲ +169.69 after sell → book $9,994.73; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 89 | $13.63 | $2.28 | $-0.98 | $2,603.80 | ▼ -0.98 after sell → book $9,992.45; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `CCOI` | 128 | $9.89 | $2.41 | $+46.42 | $3,867.31 | ▲ +46.42 after sell → book $9,990.05; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 33 | $38.24 | $2.11 | $+38.04 | $5,127.12 | ▲ +38.04 after sell → book $9,987.94; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZIP` | 268 | $4.31 | $3.51 | $-71.29 | $6,278.69 | ▼ -71.29 after sell → book $9,984.43; vs 09:30 mark -3.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 748 | $1.75 | $9.78 | $+74.07 | $7,581.65 | ▲ +74.07 after sell → book $9,974.64; vs 09:30 mark -9.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `NPWR` | 610 | $1.93 | $7.98 | $-58.55 | $8,750.97 | ▼ -58.55 after sell → book $9,966.66; vs 09:30 mark -7.98 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `PUSA` | 317 | $3.83 | $4.15 | $+2.85 | $9,962.51 | ▲ +2.85 after sell → book $9,962.51; vs 09:30 mark -4.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 5696 | $0.58 | $50.30 | — | $6,591.45 | — | combo gate; gate vol=good,blue=True; list yday_mover; 🔵; ret5=-27.5; leftover $3320.84 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 571 | $5.81 | $7.37 | — | $3,266.57 | — | combo gate; gate vol=good,blue=True; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $3320.84 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `DKS` | 26 | $121.87 | $2.07 | — | $95.88 | — | combo gate; gate vol=good,blue=True; list yday_mover; 🔵; ret5=-35.1; leftover $3320.84 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.88 | ▲ close $10,014.42 vs 09:30 $9,997.27 (session +111.64) | 16:00 close · cash $95.88 · equity $10,014.42 vs 09:30 $9,997.27 (+17.15; session marks +111.64) · 3 name(s) marked open→close (per-name table). SLQT×5696 09:30 $0.58 → close $0.55 -187.97; USDE×571 09:30 $5.81 → close $5.98 +97.07; DKS×26 09:30 $121.87 → close $129.66 +202.54 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.88 | ▲ 09:30 equity $10,173.24 vs yday $10,014.42 (+158.82) | 09:30 open · cash $95.88 (unchanged overnight, no fees) · equity $10,173.24 vs prior close $10,014.42 (+158.82) · 3 name(s) re-marked at the open (per-name table). SLQT×5696 yday $0.55 → 09:30 $0.53 -113.92; USDE×571 yday $5.98 → 09:30 $6.50 +296.92; DKS×26 yday $129.66 → 09:30 $128.73 -24.18 | — |
| 2026-08-27 09:30 ET | **SELL** | `SLQT` | 5696 | $0.53 | $48.25 | $-400.43 | $3,066.52 | ▼ -400.43 after sell → book $10,125.00; vs 09:30 mark -48.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 571 | $6.50 | $7.49 | $+379.13 | $6,770.53 | ▲ +379.13 after sell → book $10,117.51; vs 09:30 mark -7.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 26 | $128.73 | $2.10 | $+174.19 | $10,115.40 | ▲ +174.19 after sell → book $10,115.40; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,115.40 | ▲ close $10,115.40 vs 09:30 $10,173.24 (session +0.00) | 16:00 close · cash $10,115.40 · no lots left · equity $10,115.40. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,115.40 | ▲ 09:30 equity $10,115.40 vs yday $10,115.40 (+0.00) | 09:30 open · cash $10,115.40 · no holdings · equity $10,115.40 vs prior close $10,115.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 61 | $32.90 | $2.17 | — | $8,106.33 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $2023.08 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 25 | $79.42 | $2.06 | — | $6,118.76 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $2023.08 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 13 | $146.07 | $2.03 | — | $4,217.82 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $2023.08 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 14 | $141.76 | $2.03 | — | $2,231.15 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $2023.08 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 86 | $23.30 | $2.25 | — | $225.10 | — | combo gate; gate vol=good,blue=True; list ohlc_hot; 🔵; ret5=+14.5; leftover $2023.08 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $225.10 | ▼ close $9,911.34 vs 09:30 $10,115.40 (session -193.51) | 16:00 close · cash $225.10 · equity $9,911.34 vs 09:30 $10,115.40 (-204.06; session marks -193.51) · 5 name(s) marked open→close (per-name table). SEDG×61 09:30 $32.90 → close $31.41 -90.89; URBN×25 09:30 $79.42 → close $81.09 +41.75; ANF×13 09:30 $146.07 → close $148.42 +30.55; SMTC×14 09:30 $141.76 → close $131.17 -148.26; NCNO×86 09:30 $23.30 → close $22.99 -26.66 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $225.10 | ▼ 09:30 equity $9,861.60 vs yday $9,911.34 (-49.74) | 09:30 open · cash $225.10 (unchanged overnight, no fees) · equity $9,861.60 vs prior close $9,911.34 (-49.74) · 5 name(s) re-marked at the open (per-name table). SEDG×61 yday $31.41 → 09:30 $31.15 -15.86; URBN×25 yday $81.09 → 09:30 $80.44 -16.25; ANF×13 yday $148.42 → 09:30 $148.03 -5.07; SMTC×14 yday $131.17 → 09:30 $132.30 +15.82; NCNO×86 yday $22.99 → 09:30 $22.66 -28.38 | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 61 | $31.15 | $2.20 | $-111.12 | $2,123.06 | ▼ -111.12 after sell → book $9,859.41; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 25 | $80.44 | $2.09 | $+21.34 | $4,131.96 | ▲ +21.34 after sell → book $9,857.31; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 13 | $148.03 | $2.05 | $+21.40 | $6,054.30 | ▲ +21.40 after sell → book $9,855.26; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 14 | $132.30 | $2.06 | $-136.53 | $7,904.44 | ▼ -136.53 after sell → book $9,853.20; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 86 | $22.66 | $2.28 | $-59.57 | $9,850.93 | ▼ -59.57 after sell → book $9,850.93; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,850.93 | ▲ close $9,850.93 vs 09:30 $9,861.60 (session +0.00) | 16:00 close · cash $9,850.93 · no lots left · equity $9,850.93. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,850.93 | ▲ 09:30 equity $9,850.93 vs yday $9,850.93 (-0.00) | 09:30 open · cash $9,850.93 · no holdings · equity $9,850.93 vs prior close $9,850.93 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,850.93 | ▲ close $9,850.93 vs 09:30 $9,850.93 (session +0.00) | 16:00 close · cash $9,850.93 · no lots left · equity $9,850.93. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,850.93 | ▲ 09:30 equity $9,850.93 vs yday $9,850.93 (-0.00) | 09:30 open · cash $9,850.93 · no holdings · equity $9,850.93 vs prior close $9,850.93 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,850.93 | ▲ close $9,850.93 vs 09:30 $9,850.93 (session +0.00) | 16:00 close · cash $9,850.93 · no lots left · equity $9,850.93. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,850.93 | ▲ 09:30 equity $9,850.93 vs yday $9,850.93 (-0.00) | 09:30 open · cash $9,850.93 · no holdings · equity $9,850.93 vs prior close $9,850.93 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $8,656.86 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1231.37 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 79 | $15.45 | $2.23 | — | $7,434.08 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1231.37 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $6,264.51 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1231.37 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 73 | $16.77 | $2.21 | — | $5,038.09 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1231.37 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 564 | $2.18 | $7.28 | — | $3,801.29 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1231.37 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 51 | $23.88 | $2.14 | — | $2,581.27 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1231.37 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DEFT` | 1894 | $0.65 | $17.99 | — | $1,332.18 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+10.2; leftover $1231.37 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 330 | $3.73 | $4.26 | — | $97.02 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=+12.0; leftover $1231.37 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.02 | ▼ close $9,715.09 vs 09:30 $9,850.93 (session -95.70) | 16:00 close · cash $97.02 · equity $9,715.09 vs 09:30 $9,850.93 (-135.84; session marks -95.70) · 8 name(s) marked open→close (per-name table). RVTY×9 09:30 $132.45 → close $130.63 -16.38; CRK×79 09:30 $15.45 → close $14.95 -39.50; MRNA×8 09:30 $145.94 → close $148.87 +23.40; ARCT×73 09:30 $16.77 → close $15.56 -88.33; CRDL×564 09:30 $2.18 → close $2.16 -11.28; MMED×51 09:30 $23.88 → close $23.84 -2.04; DEFT×1894 09:30 $0.65 → close $0.68 +54.93; CTMX×330 09:30 $3.73 → close $3.68 -16.50 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.02 | ▲ 09:30 equity $9,762.92 vs yday $9,715.09 (+47.83) | 09:30 open · cash $97.02 (unchanged overnight, no fees) · equity $9,762.92 vs prior close $9,715.09 (+47.83) · 8 name(s) re-marked at the open (per-name table). RVTY×9 yday $130.63 → 09:30 $130.03 -5.40; CRK×79 yday $14.95 → 09:30 $15.00 +3.95; MRNA×8 yday $148.87 → 09:30 $153.62 +38.00; ARCT×73 yday $15.56 → 09:30 $15.61 +3.65; CRDL×564 yday $2.16 → 09:30 $2.16 +0.00; MMED×51 yday $23.84 → 09:30 $23.84 +0.00; DEFT×1894 yday $0.68 → 09:30 $0.69 +20.83; CTMX×330 yday $3.68 → 09:30 $3.64 -13.20 | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $1,265.25 | ▼ -25.83 after sell → book $9,760.88; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 79 | $15.00 | $2.25 | $-40.03 | $2,448.00 | ▼ -40.03 after sell → book $9,758.63; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $153.62 | $2.03 | $+57.35 | $3,674.93 | ▲ +57.35 after sell → book $9,756.60; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 73 | $15.61 | $2.23 | $-89.12 | $4,812.23 | ▼ -89.12 after sell → book $9,754.37; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 564 | $2.16 | $7.38 | $-25.93 | $6,023.09 | ▼ -25.93 after sell → book $9,746.99; vs 09:30 mark -7.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 51 | $23.84 | $2.16 | $-6.35 | $7,236.77 | ▼ -6.35 after sell → book $9,744.83; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DEFT` | 1894 | $0.69 | $19.08 | $+38.69 | $8,524.55 | ▲ +38.69 after sell → book $9,725.75; vs 09:30 mark -19.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CTMX` | 330 | $3.64 | $4.32 | $-38.28 | $9,721.43 | ▼ -38.28 after sell → book $9,721.43; vs 09:30 mark -4.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 351 | $3.46 | $4.53 | — | $8,502.44 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1215.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 482 | $2.52 | $6.22 | — | $7,281.58 | — | combo gate; gate vol=good,blue=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1215.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 181 | $6.71 | $2.53 | — | $6,064.54 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1215.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 639 | $1.90 | $8.24 | — | $4,842.20 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1215.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 254 | $4.78 | $3.28 | — | $3,624.80 | — | combo gate; gate vol=good,blue=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1215.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 107 | $11.31 | $2.31 | — | $2,412.32 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1215.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 345 | $3.52 | $4.45 | — | $1,193.47 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.3; leftover $1215.18 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $163.91 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $1215.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $163.91 | ▲ close $9,757.03 vs 09:30 $9,762.92 (session +69.15) | 16:00 close · cash $163.91 · equity $9,757.03 vs 09:30 $9,762.92 (-5.89; session marks +69.15) · 8 name(s) marked open→close (per-name table). CABA×351 09:30 $3.46 → close $3.47 +3.51; ALEC×482 09:30 $2.52 → close $2.46 -28.92; BHC×181 09:30 $6.71 → close $6.56 -27.15; BMEA×639 09:30 $1.90 → close $2.03 +83.07; OABI×254 09:30 $4.78 → close $4.33 -114.30; VIR×107 09:30 $11.31 → close $11.38 +8.02; EOSE×345 09:30 $3.52 → close $3.88 +124.20; DELL×2 09:30 $513.78 → close $524.14 +20.72 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $163.91 | ▼ 09:30 equity $9,693.76 vs yday $9,757.03 (-63.27) | 09:30 open · cash $163.91 (unchanged overnight, no fees) · equity $9,693.76 vs prior close $9,757.03 (-63.27) · 8 name(s) re-marked at the open (per-name table). CABA×351 yday $3.47 → 09:30 $3.43 -14.04; ALEC×482 yday $2.46 → 09:30 $2.38 -38.56; BHC×181 yday $6.56 → 09:30 $6.57 +1.81; BMEA×639 yday $2.03 → 09:30 $2.00 -19.17; OABI×254 yday $4.33 → 09:30 $4.30 -7.62; VIR×107 yday $11.38 → 09:30 $11.22 -17.65; EOSE×345 yday $3.88 → 09:30 $3.99 +37.95; DELL×2 yday $524.14 → 09:30 $521.15 -5.98 | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 351 | $3.43 | $4.60 | $-19.65 | $1,363.25 | ▼ -19.65 after sell → book $9,689.17; vs 09:30 mark -4.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 482 | $2.38 | $6.31 | $-80.01 | $2,504.10 | ▼ -80.01 after sell → book $9,682.86; vs 09:30 mark -6.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 181 | $6.57 | $2.57 | $-30.45 | $3,690.70 | ▼ -30.45 after sell → book $9,680.29; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 639 | $2.00 | $8.36 | $+47.30 | $4,960.34 | ▲ +47.30 after sell → book $9,671.93; vs 09:30 mark -8.36 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 254 | $4.30 | $3.33 | $-128.53 | $6,049.21 | ▼ -128.53 after sell → book $9,668.60; vs 09:30 mark -3.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 107 | $11.22 | $2.34 | $-14.28 | $7,247.41 | ▼ -14.28 after sell → book $9,666.26; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `EOSE` | 345 | $3.99 | $4.52 | $+153.18 | $8,619.44 | ▲ +153.18 after sell → book $9,661.74; vs 09:30 mark -4.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $9,659.72 | ▲ +10.73 after sell → book $9,659.72; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,659.72 | ▲ close $9,659.72 vs 09:30 $9,693.76 (session +0.00) | 16:00 close · cash $9,659.72 · no lots left · equity $9,659.72. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,659.72 | ▲ 09:30 equity $9,659.72 vs yday $9,659.72 (+0.00) | 09:30 open · cash $9,659.72 · no holdings · equity $9,659.72 vs prior close $9,659.72 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,659.72 | ▲ close $9,659.72 vs 09:30 $9,659.72 (session +0.00) | 16:00 close · cash $9,659.72 · no lots left · equity $9,659.72. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,659.72 | ▲ 09:30 equity $9,659.72 vs yday $9,659.72 (+0.00) | 09:30 open · cash $9,659.72 · no holdings · equity $9,659.72 vs prior close $9,659.72 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,659.72 | ▲ close $9,659.72 vs 09:30 $9,659.72 (session +0.00) | 16:00 close · cash $9,659.72 · no lots left · equity $9,659.72. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,659.72 | ▲ 09:30 equity $9,659.72 vs yday $9,659.72 (+0.00) | 09:30 open · cash $9,659.72 · no holdings · equity $9,659.72 vs prior close $9,659.72 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $8,506.70 | — | combo gate; gate vol=good,blue=True; list flatten,earn_react; 🔵; ⚪; ret5=+9.0; leftover $1207.47 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 447 | $2.70 | $5.77 | — | $7,294.04 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1207.47 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 110 | $10.95 | $2.32 | — | $6,087.22 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+4.6; leftover $1207.47 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 245 | $4.91 | $3.16 | — | $4,881.11 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+69.9; leftover $1207.47 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ANGX` | 224 | $5.38 | $2.89 | — | $3,673.10 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=+19.8; leftover $1207.47 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `TSSI` | 134 | $8.98 | $2.39 | — | $2,467.38 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=+10.0; leftover $1207.47 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `LDI` | 1420 | $0.85 | $16.33 | — | $1,244.05 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=-7.8; leftover $1207.47 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 21 | $54.91 | $2.05 | — | $88.89 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=+4.1; leftover $1207.47 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $88.89 | ▼ close $9,462.52 vs 09:30 $9,659.72 (session -160.28) | 16:00 close · cash $88.89 · equity $9,462.52 vs 09:30 $9,659.72 (-197.20; session marks -160.28) · 8 name(s) marked open→close (per-name table). ORCL×7 09:30 $164.43 → close $150.28 -99.05; INDP×447 09:30 $2.70 → close $2.77 +31.29; WLTH×110 09:30 $10.95 → close $10.38 -62.70; BNC×245 09:30 $4.91 → close $4.80 -26.95; ANGX×224 09:30 $5.38 → close $5.45 +15.68; TSSI×134 09:30 $8.98 → close $8.93 -6.70; LDI×1420 09:30 $0.85 → close $0.83 -21.30; ASO×21 09:30 $54.91 → close $55.36 +9.45 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRGP` | hard_red | hard-red S=-7.20 sit; no new buys |
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
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `YDDL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SLDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBA` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `FJET` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BRZE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `EQ` | hard_red | hard-red S=-13.28 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ORCL` | 7 | 2026-09-11 @ $164.43 | combo gate; gate vol=good,blue=True; list flatten,earn_react; 🔵; ⚪; ret5=+9.0; leftover $1207.47 |
| `INDP` | 447 | 2026-09-11 @ $2.70 | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1207.47 |
| `WLTH` | 110 | 2026-09-11 @ $10.95 | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+4.6; leftover $1207.47 |
| `BNC` | 245 | 2026-09-11 @ $4.91 | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+69.9; leftover $1207.47 |
| `ANGX` | 224 | 2026-09-11 @ $5.38 | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=+19.8; leftover $1207.47 |
| `TSSI` | 134 | 2026-09-11 @ $8.98 | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=+10.0; leftover $1207.47 |
| `LDI` | 1420 | 2026-09-11 @ $0.85 | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=-7.8; leftover $1207.47 |
| `ASO` | 21 | 2026-09-11 @ $54.91 | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=+4.1; leftover $1207.47 |
