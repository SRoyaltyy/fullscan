# Factor mine action — `union_overnight_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ overnight, no 🚨

Cash book **-18.96%** ($8,104) · signal-only (no cash/fees) was -23.59%. Starts YES **0/27**. Fills 109 · skips 65 · realized $-1716.30.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the prior Finviz calendar said this name reports AMC today or BMO next session (the print is still ahead; we buy today 09:30 to own the next open).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

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
- **Gate** `overnight=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $35.28.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `DUOT` | 353 | — | $9.43 | +0.00 | $9.11 | -112.96 | -112.96 | +0.00 | -112.96 |
| 2026-08-14 | `NUAI` | 658 | — | $5.06 | +0.00 | $5.07 | +6.58 | +6.58 | +0.00 | +6.58 |
| 2026-08-14 | `SIDU` | 1298 | — | $2.55 | +0.00 | $2.60 | +64.90 | +64.90 | +0.00 | +64.90 |
| 2026-08-17 | `DUOT` | 353 | $9.11 | $10.35 | +437.72 | — | +0.00 | +437.72 | +324.76 | — |
| 2026-08-17 | `NUAI` | 658 | $5.07 | $5.20 | +85.54 | — | +0.00 | +85.54 | +92.12 | — |
| 2026-08-17 | `SIDU` | 1298 | $2.60 | $2.40 | -259.60 | — | +0.00 | -259.60 | -194.70 | — |
| 2026-08-17 | `HSAI` | 92 | — | $18.32 | +0.00 | $18.07 | -23.00 | -23.00 | +0.00 | -23.00 |
| 2026-08-17 | `IQ` | 1254 | — | $1.35 | +0.00 | $1.33 | -25.08 | -25.08 | +0.00 | -25.08 |
| 2026-08-17 | `KLAR` | 81 | — | $20.67 | +0.00 | $19.51 | -93.96 | -93.96 | +0.00 | -93.96 |
| 2026-08-17 | `PONY` | 207 | — | $8.16 | +0.00 | $7.98 | -37.26 | -37.26 | +0.00 | -37.26 |
| 2026-08-17 | `VNET` | 218 | — | $7.75 | +0.00 | $7.92 | +37.06 | +37.06 | +0.00 | +37.06 |
| 2026-08-17 | `XP` | 106 | — | $15.93 | +0.00 | $15.70 | -24.38 | -24.38 | +0.00 | -24.38 |
| 2026-08-18 | `HSAI` | 92 | $18.07 | $15.77 | -212.06 | — | +0.00 | -212.06 | -235.06 | — |
| 2026-08-18 | `IQ` | 1254 | $1.33 | $1.27 | -75.24 | — | +0.00 | -75.24 | -100.32 | — |
| 2026-08-18 | `KLAR` | 81 | $19.51 | $15.66 | -311.85 | — | +0.00 | -311.85 | -405.81 | — |
| 2026-08-18 | `PONY` | 207 | $7.98 | $7.53 | -93.15 | — | +0.00 | -93.15 | -130.41 | — |
| 2026-08-18 | `VNET` | 218 | $7.92 | $7.00 | -200.56 | — | +0.00 | -200.56 | -163.50 | — |
| 2026-08-18 | `XP` | 106 | $15.70 | $15.70 | +0.00 | — | +0.00 | +0.00 | -24.38 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BEKE` | 106 | — | $17.04 | +0.00 | $16.99 | -5.30 | -5.30 | +0.00 | -5.30 |
| 2026-08-20 | `BJ` | 20 | — | $88.91 | +0.00 | $91.30 | +47.80 | +47.80 | +0.00 | +47.80 |
| 2026-08-20 | `BKE` | 42 | — | $42.60 | +0.00 | $42.64 | +1.68 | +1.68 | +0.00 | +1.68 |
| 2026-08-20 | `FLO` | 243 | — | $7.43 | +0.00 | $7.10 | -80.19 | -80.19 | +0.00 | -80.19 |
| 2026-08-20 | `ROST` | 7 | — | $229.55 | +0.00 | $228.99 | -3.92 | -3.92 | +0.00 | -3.92 |
| 2026-08-21 | `BEKE` | 106 | $16.99 | $17.93 | +100.17 | — | +0.00 | +100.17 | +94.87 | — |
| 2026-08-21 | `BJ` | 20 | $91.30 | $93.98 | +53.60 | — | +0.00 | +53.60 | +101.40 | — |
| 2026-08-21 | `BKE` | 42 | $42.64 | $43.08 | +18.48 | — | +0.00 | +18.48 | +20.16 | — |
| 2026-08-21 | `FLO` | 243 | $7.10 | $6.90 | -48.60 | — | +0.00 | -48.60 | -128.79 | — |
| 2026-08-21 | `ROST` | 7 | $228.99 | $243.85 | +104.02 | — | +0.00 | +104.02 | +100.10 | — |
| 2026-08-21 | `PDD` | 51 | — | $90.03 | +0.00 | $88.38 | -84.15 | -84.15 | +0.00 | -84.15 |
| 2026-08-21 | `XPEV` | 374 | — | $12.29 | +0.00 | $12.19 | -37.40 | -37.40 | +0.00 | -37.40 |
| 2026-08-24 | `PDD` | 51 | $88.38 | $90.95 | +131.07 | — | +0.00 | +131.07 | +46.92 | — |
| 2026-08-24 | `XPEV` | 374 | $12.19 | $11.83 | -134.64 | — | +0.00 | -134.64 | -172.04 | — |
| 2026-08-25 | `ANF` | 10 | — | $112.17 | +0.00 | $108.90 | -32.70 | -32.70 | +0.00 | -32.70 |
| 2026-08-25 | `BBWI` | 59 | — | $19.16 | +0.00 | $17.58 | -93.22 | -93.22 | +0.00 | -93.22 |
| 2026-08-25 | `BOX` | 34 | — | $33.33 | +0.00 | $33.00 | -11.22 | -11.22 | +0.00 | -11.22 |
| 2026-08-25 | `DCI` | 12 | — | $93.64 | +0.00 | $93.26 | -4.56 | -4.56 | +0.00 | -4.56 |
| 2026-08-25 | `DY` | 2 | — | $390.22 | +0.00 | $351.80 | -76.84 | -76.84 | +0.00 | -76.84 |
| 2026-08-25 | `FSCO` | 222 | — | $5.10 | +0.00 | $5.07 | -6.66 | -6.66 | +0.00 | -6.66 |
| 2026-08-25 | `HEI` | 3 | — | $357.15 | +0.00 | $351.05 | -18.30 | -18.30 | +0.00 | -18.30 |
| 2026-08-25 | `INTU` | 3 | — | $364.35 | +0.00 | $357.46 | -20.67 | -20.67 | +0.00 | -20.67 |
| 2026-08-26 | `ANF` | 10 | $108.90 | $131.37 | +224.70 | — | +0.00 | +224.70 | +192.00 | — |
| 2026-08-26 | `BBWI` | 59 | $17.58 | $18.26 | +40.12 | — | +0.00 | +40.12 | -53.10 | — |
| 2026-08-26 | `BOX` | 34 | $33.00 | $34.30 | +44.20 | — | +0.00 | +44.20 | +32.98 | — |
| 2026-08-26 | `DCI` | 12 | $93.26 | $95.13 | +22.44 | — | +0.00 | +22.44 | +17.88 | — |
| 2026-08-26 | `DY` | 2 | $351.80 | $326.91 | -49.78 | — | +0.00 | -49.78 | -126.62 | — |
| 2026-08-26 | `FSCO` | 222 | $5.07 | $5.08 | +2.22 | — | +0.00 | +2.22 | -4.44 | — |
| 2026-08-26 | `HEI` | 3 | $351.05 | $370.00 | +56.85 | — | +0.00 | +56.85 | +38.55 | — |
| 2026-08-26 | `INTU` | 3 | $357.46 | $323.47 | -101.97 | — | +0.00 | -101.97 | -122.64 | — |
| 2026-08-26 | `STDN` | 80 | — | $13.95 | +0.00 | $13.70 | -20.00 | -20.00 | +0.00 | -20.00 |
| 2026-08-26 | `A` | 7 | — | $152.45 | +0.00 | $155.08 | +18.41 | +18.41 | +0.00 | +18.41 |
| 2026-08-26 | `BBY` | 13 | — | $85.19 | +0.00 | $87.44 | +29.25 | +29.25 | +0.00 | +29.25 |
| 2026-08-26 | `BILI` | 69 | — | $16.22 | +0.00 | $16.15 | -4.83 | -4.83 | +0.00 | -4.83 |
| 2026-08-26 | `CM` | 9 | — | $118.50 | +0.00 | $118.20 | -2.70 | -2.70 | +0.00 | -2.70 |
| 2026-08-26 | `CMBT` | 62 | — | $17.91 | +0.00 | $17.65 | -16.12 | -16.12 | +0.00 | -16.12 |
| 2026-08-26 | `CRM` | 5 | — | $199.94 | +0.00 | $205.62 | +28.40 | +28.40 | +0.00 | +28.40 |
| 2026-08-26 | `CRWD` | 6 | — | $182.75 | +0.00 | $189.18 | +38.58 | +38.58 | +0.00 | +38.58 |
| 2026-08-27 | `STDN` | 80 | $13.70 | $13.84 | +11.20 | — | +0.00 | +11.20 | -8.80 | — |
| 2026-08-27 | `A` | 7 | $155.08 | $159.35 | +29.89 | — | +0.00 | +29.89 | +48.30 | — |
| 2026-08-27 | `BBY` | 13 | $87.44 | $80.60 | -88.92 | — | +0.00 | -88.92 | -59.67 | — |
| 2026-08-27 | `BILI` | 69 | $16.15 | $16.18 | +2.07 | — | +0.00 | +2.07 | -2.76 | — |
| 2026-08-27 | `CM` | 9 | $118.20 | $118.77 | +5.13 | — | +0.00 | +5.13 | +2.43 | — |
| 2026-08-27 | `CMBT` | 62 | $17.65 | $17.78 | +8.06 | — | +0.00 | +8.06 | -8.06 | — |
| 2026-08-27 | `CRM` | 5 | $205.62 | $230.05 | +122.15 | — | +0.00 | +122.15 | +150.55 | — |
| 2026-08-27 | `CRWD` | 6 | $189.18 | $208.25 | +114.42 | — | +0.00 | +114.42 | +153.00 | — |
| 2026-08-27 | `GAP` | 55 | — | $20.75 | +0.00 | $20.79 | +2.20 | +2.20 | +0.00 | +2.20 |
| 2026-08-27 | `ADSK` | 4 | — | $261.47 | +0.00 | $270.58 | +36.44 | +36.44 | +0.00 | +36.44 |
| 2026-08-27 | `AFRM` | 15 | — | $76.90 | +0.00 | $77.49 | +8.85 | +8.85 | +0.00 | +8.85 |
| 2026-08-27 | `BBAR` | 77 | — | $14.96 | +0.00 | $14.60 | -27.72 | -27.72 | +0.00 | -27.72 |
| 2026-08-27 | `CHA` | 109 | — | $10.54 | +0.00 | $10.35 | -20.71 | -20.71 | +0.00 | -20.71 |
| 2026-08-27 | `ESTC` | 13 | — | $82.65 | +0.00 | $83.74 | +14.17 | +14.17 | +0.00 | +14.17 |
| 2026-08-27 | `HAFN` | 146 | — | $7.91 | +0.00 | $8.29 | +55.48 | +55.48 | +0.00 | +55.48 |
| 2026-08-27 | `MNSO` | 106 | — | $10.89 | +0.00 | $10.81 | -8.48 | -8.48 | +0.00 | -8.48 |
| 2026-08-28 | `GAP` | 55 | $20.79 | $24.69 | +214.50 | — | +0.00 | +214.50 | +216.70 | — |
| 2026-08-28 | `ADSK` | 4 | $270.58 | $261.16 | -37.68 | — | +0.00 | -37.68 | -1.24 | — |
| 2026-08-28 | `AFRM` | 15 | $77.49 | $86.00 | +127.65 | — | +0.00 | +127.65 | +136.50 | — |
| 2026-08-28 | `BBAR` | 77 | $14.60 | $15.01 | +31.57 | — | +0.00 | +31.57 | +3.85 | — |
| 2026-08-28 | `CHA` | 109 | $10.35 | $10.30 | -5.45 | — | +0.00 | -5.45 | -26.16 | — |
| 2026-08-28 | `ESTC` | 13 | $83.74 | $103.89 | +261.95 | — | +0.00 | +261.95 | +276.12 | — |
| 2026-08-28 | `HAFN` | 146 | $8.29 | $8.35 | +8.76 | — | +0.00 | +8.76 | +64.24 | — |
| 2026-08-28 | `MNSO` | 106 | $10.81 | $10.43 | -40.28 | — | +0.00 | -40.28 | -48.76 | — |
| 2026-08-28 | `LX` | 4240 | — | $1.16 | +0.00 | $1.18 | +84.80 | +84.80 | +0.00 | +84.80 |
| 2026-08-28 | `SAIC` | 37 | — | $129.46 | +0.00 | $125.96 | -129.50 | -129.50 | +0.00 | -129.50 |
| 2026-08-31 | `LX` | 4240 | $1.18 | $1.01 | -720.80 | — | +0.00 | -720.80 | -636.00 | — |
| 2026-08-31 | `SAIC` | 37 | $125.96 | $140.39 | +533.91 | — | +0.00 | +533.91 | +404.41 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AMBA` | 17 | — | $66.61 | +0.00 | $63.38 | -54.91 | -54.91 | +0.00 | -54.91 |
| 2026-09-03 | `ASAN` | 116 | — | $10.16 | +0.00 | $10.09 | -8.12 | -8.12 | +0.00 | -8.12 |
| 2026-09-03 | `DOCU` | 17 | — | $67.06 | +0.00 | $65.97 | -18.53 | -18.53 | +0.00 | -18.53 |
| 2026-09-03 | `DOMO` | 313 | — | $3.78 | +0.00 | $3.79 | +3.13 | +3.13 | +0.00 | +3.13 |
| 2026-09-03 | `GWRE` | 5 | — | $198.00 | +0.00 | $202.86 | +24.30 | +24.30 | +0.00 | +24.30 |
| 2026-09-03 | `IOT` | 31 | — | $37.69 | +0.00 | $38.75 | +32.86 | +32.86 | +0.00 | +32.86 |
| 2026-09-03 | `LULU` | 9 | — | $121.15 | +0.00 | $121.77 | +5.58 | +5.58 | +0.00 | +5.58 |
| 2026-09-03 | `MAMA` | 75 | — | $15.62 | +0.00 | $15.96 | +25.50 | +25.50 | +0.00 | +25.50 |
| 2026-09-04 | `AMBA` | 17 | $63.38 | $63.18 | -3.40 | — | +0.00 | -3.40 | -58.31 | — |
| 2026-09-04 | `ASAN` | 116 | $10.09 | $8.74 | -156.60 | — | +0.00 | -156.60 | -164.72 | — |
| 2026-09-04 | `DOCU` | 17 | $65.97 | $68.52 | +43.35 | — | +0.00 | +43.35 | +24.82 | — |
| 2026-09-04 | `DOMO` | 313 | $3.79 | $3.62 | -54.77 | — | +0.00 | -54.77 | -51.64 | — |
| 2026-09-04 | `GWRE` | 5 | $202.86 | $167.55 | -176.55 | — | +0.00 | -176.55 | -152.25 | — |
| 2026-09-04 | `IOT` | 31 | $38.75 | $44.90 | +190.65 | — | +0.00 | +190.65 | +223.51 | — |
| 2026-09-04 | `LULU` | 9 | $121.77 | $98.15 | -212.58 | — | +0.00 | -212.58 | -207.00 | — |
| 2026-09-04 | `MAMA` | 75 | $15.96 | $15.70 | -19.50 | — | +0.00 | -19.50 | +6.00 | — |
| 2026-09-04 | `ABM` | 96 | — | $46.79 | +0.00 | $47.05 | +24.96 | +24.96 | +0.00 | +24.96 |
| 2026-09-04 | `UNFI` | 103 | — | $43.80 | +0.00 | $43.93 | +13.39 | +13.39 | +0.00 | +13.39 |
| 2026-09-08 | `ABM` | 96 | $47.05 | $45.81 | -119.04 | — | +0.00 | -119.04 | -94.08 | — |
| 2026-09-08 | `UNFI` | 103 | $43.93 | $45.21 | +131.84 | — | +0.00 | +131.84 | +145.23 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `ALMU` | 331 | — | $13.75 | +0.00 | $13.43 | -105.92 | -105.92 | +0.00 | -105.92 |
| 2026-09-16 | `LEN` | 56 | — | $80.63 | +0.00 | $78.36 | -127.12 | -127.12 | +0.00 | -127.12 |
| 2026-09-17 | `ALMU` | 331 | $13.43 | $11.21 | -734.82 | — | +0.00 | -734.82 | -840.74 | — |
| 2026-09-17 | `LEN` | 56 | $78.36 | $81.00 | +147.84 | — | +0.00 | +147.84 | +20.72 | — |
| 2026-09-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-21 | `ABVX` | 78 | — | $105.72 | +0.00 | $103.45 | -177.06 | -177.06 | +0.00 | -177.06 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -41.48 | DUOT, NUAI, SIDU | — | $2.04 | $9,928.73 | DUOT×353, NUAI×658, SIDU×1298 |
| 2026-08-17 | +2.25 | $2.04 | DUOT×353, NUAI×658, SIDU×1298 | $10,192.39 | +263.66 | -166.62 | HSAI, IQ, KLAR, PONY, VNET, XP | DUOT, NUAI, SIDU | $13.87 | $9,967.06 | HSAI×92, IQ×1254, KLAR×81, PONY×207, VNET×218, XP×106 |
| 2026-08-18 | -6.20 | $13.87 | HSAI×92, IQ×1254, KLAR×81, PONY×207, VNET×218, XP×106 | $9,074.20 | -892.86 | +0.00 | — | HSAI, IQ, KLAR, PONY, VNET, XP | $9,045.33 | $9,045.33 | — |
| 2026-08-19 | -7.20 | $9,045.33 | — | $9,045.33 | +0.00 | +0.00 | — | — | $9,045.33 | $9,045.33 | — |
| 2026-08-20 | +1.12 | $9,045.33 | — | $9,045.33 | +0.00 | -39.93 | BEKE, BJ, BKE, FLO, ROST | — | $247.73 | $8,993.78 | BEKE×106, BJ×20, BKE×42, FLO×243, ROST×7 |
| 2026-08-21 | +3.25 | $247.73 | BEKE×106, BJ×20, BKE×42, FLO×243, ROST×7 | $9,221.45 | +227.67 | -121.55 | PDD, XPEV | BEKE, BJ, BKE, FLO, ROST | $14.72 | $9,081.16 | PDD×51, XPEV×374 |
| 2026-08-24 | -5.17 | $14.72 | PDD×51, XPEV×374 | $9,077.59 | -3.57 | +0.00 | — | PDD, XPEV | $9,070.47 | $9,070.47 | — |
| 2026-08-25 | +1.80 | $9,070.47 | — | $9,070.47 | +0.00 | -264.17 | ANF, BBWI, BOX, DCI, DY, FSCO, HEI, INTU | — | $467.13 | $8,789.14 | ANF×10, BBWI×59, BOX×34, DCI×12, DY×2, FSCO×222, HEI×3, INTU×3 |
| 2026-08-26 | +2.02 | $467.13 | ANF×10, BBWI×59, BOX×34, DCI×12, DY×2, FSCO×222, HEI×3, INTU×3 | $9,027.92 | +238.78 | +70.99 | STDN, A, BBY, BILI, CM, CMBT, CRM, CRWD | ANF, BBWI, BOX, DCI, DY, FSCO, HEI, INTU | $310.98 | $9,064.89 | STDN×80, A×7, BBY×13, BILI×69, CM×9, CMBT×62, CRM×5, CRWD×6 |
| 2026-08-27 | — | $310.98 | STDN×80, A×7, BBY×13, BILI×69, CM×9, CMBT×62, CRM×5, CRWD×6 | $9,268.89 | +204.00 | +60.23 | GAP, ADSK, AFRM, BBAR, CHA, ESTC, HAFN, MNSO | STDN, A, BBY, BILI, CM, CMBT, CRM, CRWD | $209.50 | $9,294.79 | GAP×55, ADSK×4, AFRM×15, BBAR×77, CHA×109, ESTC×13, HAFN×146, MNSO×106 |
| 2026-08-28 | +0.75 | $209.50 | GAP×55, ADSK×4, AFRM×15, BBAR×77, CHA×109, ESTC×13, HAFN×146, MNSO×106 | $9,855.81 | +561.02 | -44.70 | LX, SAIC | GAP, ADSK, AFRM, BBAR, CHA, ESTC, HAFN, MNSO | $72.90 | $9,736.62 | LX×4240, SAIC×37 |
| 2026-08-31 | -5.85 | $72.90 | LX×4240, SAIC×37 | $9,549.73 | -186.89 | +0.00 | — | LX, SAIC | $9,492.14 | $9,492.14 | — |
| 2026-09-01 | -6.30 | $9,492.14 | — | $9,492.14 | +0.00 | +0.00 | — | — | $9,492.14 | $9,492.14 | — |
| 2026-09-02 | -3.83 | $9,492.14 | — | $9,492.14 | +0.00 | +0.00 | — | — | $9,492.14 | $9,492.14 | — |
| 2026-09-03 | -0.90 | $9,492.14 | — | $9,492.14 | +0.00 | +9.81 | AMBA, ASAN, DOCU, DOMO, GWRE, IOT, LULU, MAMA | — | $419.03 | $9,483.17 | AMBA×17, ASAN×116, DOCU×17, DOMO×313, GWRE×5, IOT×31, LULU×9, MAMA×75 |
| 2026-09-04 | +2.25 | $419.03 | AMBA×17, ASAN×116, DOCU×17, DOMO×313, GWRE×5, IOT×31, LULU×9, MAMA×75 | $9,093.77 | -389.40 | +38.35 | ABM, UNFI | AMBA, ASAN, DOCU, DOMO, GWRE, IOT, LULU, MAMA | $66.96 | $9,108.55 | ABM×96, UNFI×103 |
| 2026-09-08 | -11.47 | $66.96 | ABM×96, UNFI×103 | $9,121.35 | +12.80 | +0.00 | — | ABM, UNFI | $9,116.67 | $9,116.67 | — |
| 2026-09-09 | -13.95 | $9,116.67 | — | $9,116.67 | -0.00 | +0.00 | — | — | $9,116.67 | $9,116.67 | — |
| 2026-09-10 | -13.28 | $9,116.67 | — | $9,116.67 | -0.00 | +0.00 | — | — | $9,116.67 | $9,116.67 | — |
| 2026-09-11 | +0.50 | $9,116.67 | — | $9,116.67 | -0.00 | +0.00 | — | — | $9,116.67 | $9,116.67 | — |
| 2026-09-14 | -11.00 | $9,116.67 | — | $9,116.67 | -0.00 | +0.00 | — | — | $9,116.67 | $9,116.67 | — |
| 2026-09-15 | -3.84 | $9,116.67 | — | $9,116.67 | -0.00 | +0.00 | — | — | $9,116.67 | $9,116.67 | — |
| 2026-09-16 | +5.30 | $9,116.67 | — | $9,116.67 | -0.00 | -233.04 | ALMU, LEN | — | $43.71 | $8,877.20 | ALMU×331, LEN×56 |
| 2026-09-17 | +7.38 | $43.71 | ALMU×331, LEN×56 | $8,290.22 | -586.98 | +0.00 | — | ALMU, LEN | $8,283.66 | $8,283.66 | — |
| 2026-09-18 | +4.86 | $8,283.66 | — | $8,283.66 | +0.00 | +0.00 | — | — | $8,283.66 | $8,283.66 | — |
| 2026-09-21 | +12.87 | $8,283.66 | — | $8,283.66 | +0.00 | -177.06 | ABVX | — | $35.28 | $8,104.38 | ABVX×78 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `DUOT` | 353 | $9.43 | $4.55 | — | $6,666.66 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ⚪; ret5=+7.7; leftover $3333.33 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NUAI` | 658 | $5.06 | $8.49 | — | $3,328.69 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ⚪; ret5=-3.2; leftover $3333.33 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SIDU` | 1298 | $2.55 | $16.74 | — | $2.04 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+21.5; leftover $3333.33 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.04 | ▼ close $9,928.73 vs 09:30 $10,000.00 (session -41.48) | 16:00 close · cash $2.04 · equity $9,928.73 vs 09:30 $10,000.00 (-71.27; session marks -41.48) · 3 name(s) marked open→close (per-name table). DUOT×353 09:30 $9.43 → close $9.11 -112.96; NUAI×658 09:30 $5.06 → close $5.07 +6.58; SIDU×1298 09:30 $2.55 → close $2.60 +64.90 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.04 | ▲ 09:30 equity $10,192.39 vs yday $9,928.73 (+263.66) | 09:30 open · cash $2.04 (unchanged overnight, no fees) · equity $10,192.39 vs prior close $9,928.73 (+263.66) · 3 name(s) re-marked at the open (per-name table). DUOT×353 yday $9.11 → 09:30 $10.35 +437.72; NUAI×658 yday $5.07 → 09:30 $5.20 +85.54; SIDU×1298 yday $2.60 → 09:30 $2.40 -259.60 | — |
| 2026-08-17 09:30 ET | **SELL** | `DUOT` | 353 | $10.35 | $4.64 | $+315.56 | $3,650.95 | ▲ +315.56 after sell → book $10,187.75; vs 09:30 mark -4.64 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `NUAI` | 658 | $5.20 | $8.62 | $+75.01 | $7,063.93 | ▲ +75.01 after sell → book $10,179.13; vs 09:30 mark -8.62 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `SIDU` | 1298 | $2.40 | $16.98 | $-228.43 | $10,162.14 | ▼ -228.43 after sell → book $10,162.14; vs 09:30 mark -16.99 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HSAI` | 92 | $18.32 | $2.27 | — | $8,474.44 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-5.3; leftover $1693.69 | join🟡 sector🔴 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `IQ` | 1254 | $1.35 | $16.18 | — | $6,765.36 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ⚪; ret5=+1.5; leftover $1693.69 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `KLAR` | 81 | $20.67 | $2.23 | — | $5,088.86 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+4.5; leftover $1693.69 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `PONY` | 207 | $8.16 | $2.67 | — | $3,397.07 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ⚪; ret5=-0.1; leftover $1693.69 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VNET` | 218 | $7.75 | $2.81 | — | $1,704.75 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+3.7; leftover $1693.69 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `XP` | 106 | $15.93 | $2.31 | — | $13.87 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ⚪; ret5=-2.6; leftover $1693.69 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.87 | ▼ close $9,967.06 vs 09:30 $10,192.39 (session -166.62) | 16:00 close · cash $13.87 · equity $9,967.06 vs 09:30 $10,192.39 (-225.33; session marks -166.62) · 6 name(s) marked open→close (per-name table). HSAI×92 09:30 $18.32 → close $18.07 -23.00; IQ×1254 09:30 $1.35 → close $1.33 -25.08; KLAR×81 09:30 $20.67 → close $19.51 -93.96; PONY×207 09:30 $8.16 → close $7.98 -37.26; VNET×218 09:30 $7.75 → close $7.92 +37.06; XP×106 09:30 $15.93 → close $15.70 -24.38 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.87 | ▼ 09:30 equity $9,074.20 vs yday $9,967.06 (-892.86) | 09:30 open · cash $13.87 (unchanged overnight, no fees) · equity $9,074.20 vs prior close $9,967.06 (-892.86) · 6 name(s) re-marked at the open (per-name table). HSAI×92 yday $18.07 → 09:30 $15.77 -212.06; IQ×1254 yday $1.33 → 09:30 $1.27 -75.24; KLAR×81 yday $19.51 → 09:30 $15.66 -311.85; PONY×207 yday $7.98 → 09:30 $7.53 -93.15; VNET×218 yday $7.92 → 09:30 $7.00 -200.56; XP×106 yday $15.70 → 09:30 $15.70 +0.00 | — |
| 2026-08-18 09:30 ET | **SELL** | `HSAI` | 92 | $15.77 | $2.29 | $-239.62 | $1,461.95 | ▼ -239.62 after sell → book $9,071.90; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `IQ` | 1254 | $1.27 | $16.40 | $-132.89 | $3,038.14 | ▼ -132.89 after sell → book $9,055.51; vs 09:30 mark -16.39 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `KLAR` | 81 | $15.66 | $2.26 | $-410.30 | $4,304.34 | ▼ -410.30 after sell → book $9,053.25; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `PONY` | 207 | $7.53 | $2.72 | $-135.80 | $5,860.33 | ▼ -135.80 after sell → book $9,050.53; vs 09:30 mark -2.72 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `VNET` | 218 | $7.00 | $2.86 | $-169.17 | $7,383.47 | ▼ -169.17 after sell → book $9,047.67; vs 09:30 mark -2.86 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `XP` | 106 | $15.70 | $2.34 | $-29.03 | $9,045.33 | ▼ -29.03 after sell → book $9,045.33; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,045.33 | ▲ close $9,045.33 vs 09:30 $9,074.20 (session +0.00) | 16:00 close · cash $9,045.33 · no lots left · equity $9,045.33. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,045.33 | ▲ 09:30 equity $9,045.33 vs yday $9,045.33 (+0.00) | 09:30 open · cash $9,045.33 · no holdings · equity $9,045.33 vs prior close $9,045.33 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,045.33 | ▲ close $9,045.33 vs 09:30 $9,045.33 (session +0.00) | 16:00 close · cash $9,045.33 · no lots left · equity $9,045.33. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,045.33 | ▲ 09:30 equity $9,045.33 vs yday $9,045.33 (+0.00) | 09:30 open · cash $9,045.33 · no holdings · equity $9,045.33 vs prior close $9,045.33 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BEKE` | 106 | $17.04 | $2.31 | — | $7,236.79 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-0.2; leftover $1809.07 | join🟢 sector🔴 gen🟢 news🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BJ` | 20 | $88.91 | $2.05 | — | $5,456.54 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-1.0; leftover $1809.07 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BKE` | 42 | $42.60 | $2.12 | — | $3,665.22 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-4.6; leftover $1809.07 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `FLO` | 243 | $7.43 | $3.13 | — | $1,856.59 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+4.0; leftover $1809.07 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ROST` | 7 | $229.55 | $2.01 | — | $247.73 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight,overnight_mega; 🔵; ret5=-5.5; leftover $1809.07 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $247.73 | ▼ close $8,993.78 vs 09:30 $9,045.33 (session -39.93) | 16:00 close · cash $247.73 · equity $8,993.78 vs 09:30 $9,045.33 (-51.55; session marks -39.93) · 5 name(s) marked open→close (per-name table). BEKE×106 09:30 $17.04 → close $16.99 -5.30; BJ×20 09:30 $88.91 → close $91.30 +47.80; BKE×42 09:30 $42.60 → close $42.64 +1.68; FLO×243 09:30 $7.43 → close $7.10 -80.19; ROST×7 09:30 $229.55 → close $228.99 -3.92 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $247.73 | ▲ 09:30 equity $9,221.45 vs yday $8,993.78 (+227.67) | 09:30 open · cash $247.73 (unchanged overnight, no fees) · equity $9,221.45 vs prior close $8,993.78 (+227.67) · 5 name(s) re-marked at the open (per-name table). BEKE×106 yday $16.99 → 09:30 $17.93 +100.17; BJ×20 yday $91.30 → 09:30 $93.98 +53.60; BKE×42 yday $42.64 → 09:30 $43.08 +18.48; FLO×243 yday $7.10 → 09:30 $6.90 -48.60; ROST×7 yday $228.99 → 09:30 $243.85 +104.02 | — |
| 2026-08-21 09:30 ET | **SELL** | `BEKE` | 106 | $17.93 | $2.34 | $+90.22 | $2,146.50 | ▲ +90.22 after sell → book $9,219.11; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `BJ` | 20 | $93.98 | $2.08 | $+97.28 | $4,024.03 | ▲ +97.28 after sell → book $9,217.04; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `BKE` | 42 | $43.08 | $2.14 | $+15.90 | $5,831.25 | ▲ +15.90 after sell → book $9,214.90; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `FLO` | 243 | $6.90 | $3.19 | $-135.11 | $7,504.76 | ▼ -135.11 after sell → book $9,211.71; vs 09:30 mark -3.19 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ROST` | 7 | $243.85 | $2.03 | $+96.05 | $9,209.67 | ▲ +96.05 after sell → book $9,209.67; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `PDD` | 51 | $90.03 | $2.14 | — | $4,616.00 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight,overnight_mega; 🔵; ret5=+6.4; leftover $4604.84 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XPEV` | 374 | $12.29 | $4.82 | — | $14.72 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+1.9; leftover $4604.84 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.72 | ▼ close $9,081.16 vs 09:30 $9,221.45 (session -121.55) | 16:00 close · cash $14.72 · equity $9,081.16 vs 09:30 $9,221.45 (-140.29; session marks -121.55) · 2 name(s) marked open→close (per-name table). PDD×51 09:30 $90.03 → close $88.38 -84.15; XPEV×374 09:30 $12.29 → close $12.19 -37.40 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.72 | ▼ 09:30 equity $9,077.59 vs yday $9,081.16 (-3.57) | 09:30 open · cash $14.72 (unchanged overnight, no fees) · equity $9,077.59 vs prior close $9,081.16 (-3.57) · 2 name(s) re-marked at the open (per-name table). PDD×51 yday $88.38 → 09:30 $90.95 +131.07; XPEV×374 yday $12.19 → 09:30 $11.83 -134.64 | — |
| 2026-08-24 09:30 ET | **SELL** | `PDD` | 51 | $90.95 | $2.19 | $+42.59 | $4,650.98 | ▲ +42.59 after sell → book $9,075.40; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `XPEV` | 374 | $11.83 | $4.92 | $-181.79 | $9,070.47 | ▼ -181.79 after sell → book $9,070.47; vs 09:30 mark -4.93 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,070.47 | ▲ close $9,070.47 vs 09:30 $9,077.59 (session +0.00) | 16:00 close · cash $9,070.47 · no lots left · equity $9,070.47. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,070.47 | ▲ 09:30 equity $9,070.47 vs yday $9,070.47 (+0.00) | 09:30 open · cash $9,070.47 · no holdings · equity $9,070.47 vs prior close $9,070.47 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `ANF` | 10 | $112.17 | $2.02 | — | $7,946.75 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ⚪; ret5=+6.8; leftover $1133.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BBWI` | 59 | $19.16 | $2.17 | — | $6,814.15 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+3.0; leftover $1133.81 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BOX` | 34 | $33.33 | $2.09 | — | $5,678.84 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+3.7; leftover $1133.81 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DCI` | 12 | $93.64 | $2.03 | — | $4,553.13 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-2.4; leftover $1133.81 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DY` | 2 | $390.22 | $2.00 | — | $3,770.69 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-12.0; leftover $1133.81 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🟡 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FSCO` | 222 | $5.10 | $2.86 | — | $2,635.63 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+0.2; leftover $1133.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HEI` | 3 | $357.15 | $2.00 | — | $1,562.18 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ⚪; ret5=-5.0; leftover $1133.81 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `INTU` | 3 | $364.35 | $2.00 | — | $467.13 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight,overnight_mega; 🔵; ret5=+10.2; leftover $1133.81 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $467.13 | ▼ close $8,789.14 vs 09:30 $9,070.47 (session -264.17) | 16:00 close · cash $467.13 · equity $8,789.14 vs 09:30 $9,070.47 (-281.33; session marks -264.17) · 8 name(s) marked open→close (per-name table). ANF×10 09:30 $112.17 → close $108.90 -32.70; BBWI×59 09:30 $19.16 → close $17.58 -93.22; BOX×34 09:30 $33.33 → close $33.00 -11.22; DCI×12 09:30 $93.64 → close $93.26 -4.56; DY×2 09:30 $390.22 → close $351.80 -76.84; FSCO×222 09:30 $5.10 → close $5.07 -6.66; HEI×3 09:30 $357.15 → close $351.05 -18.30; INTU×3 09:30 $364.35 → close $357.46 -20.67 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $467.13 | ▲ 09:30 equity $9,027.92 vs yday $8,789.14 (+238.78) | 09:30 open · cash $467.13 (unchanged overnight, no fees) · equity $9,027.92 vs prior close $8,789.14 (+238.78) · 8 name(s) re-marked at the open (per-name table). ANF×10 yday $108.90 → 09:30 $131.37 +224.70; BBWI×59 yday $17.58 → 09:30 $18.26 +40.12; BOX×34 yday $33.00 → 09:30 $34.30 +44.20; DCI×12 yday $93.26 → 09:30 $95.13 +22.44; DY×2 yday $351.80 → 09:30 $326.91 -49.78; FSCO×222 yday $5.07 → 09:30 $5.08 +2.22; HEI×3 yday $351.05 → 09:30 $370.00 +56.85; INTU×3 yday $357.46 → 09:30 $323.47 -101.97 | — |
| 2026-08-26 09:30 ET | **SELL** | `ANF` | 10 | $131.37 | $2.04 | $+187.94 | $1,778.79 | ▲ +187.94 after sell → book $9,025.88; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `BBWI` | 59 | $18.26 | $2.19 | $-57.45 | $2,853.94 | ▼ -57.45 after sell → book $9,023.69; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `BOX` | 34 | $34.30 | $2.11 | $+28.78 | $4,018.03 | ▲ +28.78 after sell → book $9,021.58; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `DCI` | 12 | $95.13 | $2.05 | $+13.81 | $5,157.55 | ▲ +13.81 after sell → book $9,019.54; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `DY` | 2 | $326.91 | $2.02 | $-130.63 | $5,809.35 | ▼ -130.63 after sell → book $9,017.52; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `FSCO` | 222 | $5.08 | $2.91 | $-10.21 | $6,934.20 | ▼ -10.21 after sell → book $9,014.61; vs 09:30 mark -2.91 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟡 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `HEI` | 3 | $370.00 | $2.02 | $+34.53 | $8,042.18 | ▲ +34.53 after sell → book $9,012.59; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `INTU` | 3 | $323.47 | $2.02 | $-126.66 | $9,010.57 | ▼ -126.66 after sell → book $9,010.57; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟡 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `STDN` | 80 | $13.95 | $2.23 | — | $7,892.34 | — | union ∩ overnight, no 🚨; gate overnight=True; list ohlc_hot,overnight; 🔵; ret5=+14.3; leftover $1126.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `A` | 7 | $152.45 | $2.01 | — | $6,823.18 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+4.3; leftover $1126.32 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BBY` | 13 | $85.19 | $2.03 | — | $5,713.68 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-2.3; leftover $1126.32 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BILI` | 69 | $16.22 | $2.20 | — | $4,592.30 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-2.5; leftover $1126.32 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 9 | $118.50 | $2.02 | — | $3,523.79 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $1126.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CMBT` | 62 | $17.91 | $2.18 | — | $2,411.19 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+3.9; leftover $1126.32 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CRM` | 5 | $199.94 | $2.00 | — | $1,409.49 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight,overnight_mega; ret5=+2.1; leftover $1126.32 | join🟡 sector🔴 gen🟢 news🔴 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 catal🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CRWD` | 6 | $182.75 | $2.01 | — | $310.98 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight,overnight_mega; 🔵; ret5=-12.9; leftover $1126.32 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $310.98 | ▲ close $9,064.89 vs 09:30 $9,027.92 (session +70.99) | 16:00 close · cash $310.98 · equity $9,064.89 vs 09:30 $9,027.92 (+36.97; session marks +70.99) · 8 name(s) marked open→close (per-name table). STDN×80 09:30 $13.95 → close $13.70 -20.00; A×7 09:30 $152.45 → close $155.08 +18.41; BBY×13 09:30 $85.19 → close $87.44 +29.25; BILI×69 09:30 $16.22 → close $16.15 -4.83; CM×9 09:30 $118.50 → close $118.20 -2.70; CMBT×62 09:30 $17.91 → close $17.65 -16.12; CRM×5 09:30 $199.94 → close $205.62 +28.40; CRWD×6 09:30 $182.75 → close $189.18 +38.58 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $310.98 | ▲ 09:30 equity $9,268.89 vs yday $9,064.89 (+204.00) | 09:30 open · cash $310.98 (unchanged overnight, no fees) · equity $9,268.89 vs prior close $9,064.89 (+204.00) · 8 name(s) re-marked at the open (per-name table). STDN×80 yday $13.70 → 09:30 $13.84 +11.20; A×7 yday $155.08 → 09:30 $159.35 +29.89; BBY×13 yday $87.44 → 09:30 $80.60 -88.92; BILI×69 yday $16.15 → 09:30 $16.18 +2.07; CM×9 yday $118.20 → 09:30 $118.77 +5.13; CMBT×62 yday $17.65 → 09:30 $17.78 +8.06; CRM×5 yday $205.62 → 09:30 $230.05 +122.15; CRWD×6 yday $189.18 → 09:30 $208.25 +114.42 | — |
| 2026-08-27 09:30 ET | **SELL** | `STDN` | 80 | $13.84 | $2.25 | $-13.28 | $1,415.92 | ▼ -13.28 after sell → book $9,266.64; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `A` | 7 | $159.35 | $2.03 | $+44.26 | $2,529.34 | ▲ +44.26 after sell → book $9,264.60; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BBY` | 13 | $80.60 | $2.05 | $-63.75 | $3,575.09 | ▼ -63.75 after sell → book $9,262.56; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `BILI` | 69 | $16.18 | $2.22 | $-7.18 | $4,689.30 | ▼ -7.18 after sell → book $9,260.34; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `CM` | 9 | $118.77 | $2.04 | $-1.62 | $5,756.19 | ▼ -1.62 after sell → book $9,258.30; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `CMBT` | 62 | $17.78 | $2.20 | $-12.43 | $6,856.35 | ▼ -12.43 after sell → book $9,256.10; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `CRM` | 5 | $230.05 | $2.02 | $+146.52 | $8,004.58 | ▲ +146.52 after sell → book $9,254.08; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRWD` | 6 | $208.25 | $2.03 | $+148.96 | $9,252.05 | ▲ +148.96 after sell → book $9,252.05; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `GAP` | 55 | $20.75 | $2.15 | — | $8,108.65 | — | union ∩ overnight, no 🚨; gate overnight=True; list ohlc_hot,overnight; ret5=+5.2; leftover $1156.51 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ADSK` | 4 | $261.47 | $2.00 | — | $7,060.76 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight,overnight_mega; 🔵; ret5=+1.4; leftover $1156.51 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `AFRM` | 15 | $76.90 | $2.04 | — | $5,905.23 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-1.1; leftover $1156.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `BBAR` | 77 | $14.96 | $2.22 | — | $4,751.09 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+3.0; leftover $1156.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CHA` | 109 | $10.54 | $2.32 | — | $3,599.91 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+2.5; leftover $1156.51 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ESTC` | 13 | $82.65 | $2.03 | — | $2,523.43 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-9.3; leftover $1156.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `HAFN` | 146 | $7.91 | $2.43 | — | $1,366.14 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-1.8; leftover $1156.51 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MNSO` | 106 | $10.89 | $2.31 | — | $209.50 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+2.7; leftover $1156.51 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $209.50 | ▲ close $9,294.79 vs 09:30 $9,268.89 (session +60.23) | 16:00 close · cash $209.50 · equity $9,294.79 vs 09:30 $9,268.89 (+25.90; session marks +60.23) · 8 name(s) marked open→close (per-name table). GAP×55 09:30 $20.75 → close $20.79 +2.20; ADSK×4 09:30 $261.47 → close $270.58 +36.44; AFRM×15 09:30 $76.90 → close $77.49 +8.85; BBAR×77 09:30 $14.96 → close $14.60 -27.72; CHA×109 09:30 $10.54 → close $10.35 -20.71; ESTC×13 09:30 $82.65 → close $83.74 +14.17; HAFN×146 09:30 $7.91 → close $8.29 +55.48; MNSO×106 09:30 $10.89 → close $10.81 -8.48 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $209.50 | ▲ 09:30 equity $9,855.81 vs yday $9,294.79 (+561.02) | 09:30 open · cash $209.50 (unchanged overnight, no fees) · equity $9,855.81 vs prior close $9,294.79 (+561.02) · 8 name(s) re-marked at the open (per-name table). GAP×55 yday $20.79 → 09:30 $24.69 +214.50; ADSK×4 yday $270.58 → 09:30 $261.16 -37.68; AFRM×15 yday $77.49 → 09:30 $86.00 +127.65; BBAR×77 yday $14.60 → 09:30 $15.01 +31.57; CHA×109 yday $10.35 → 09:30 $10.30 -5.45; ESTC×13 yday $83.74 → 09:30 $103.89 +261.95; HAFN×146 yday $8.29 → 09:30 $8.35 +8.76; MNSO×106 yday $10.81 → 09:30 $10.43 -40.28 | — |
| 2026-08-28 09:30 ET | **SELL** | `GAP` | 55 | $24.69 | $2.18 | $+212.37 | $1,565.27 | ▲ +212.37 after sell → book $9,853.63; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ADSK` | 4 | $261.16 | $2.02 | $-5.26 | $2,607.89 | ▼ -5.26 after sell → book $9,851.61; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `AFRM` | 15 | $86.00 | $2.06 | $+132.41 | $3,895.83 | ▲ +132.41 after sell → book $9,849.55; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `BBAR` | 77 | $15.01 | $2.24 | $-0.61 | $5,049.36 | ▼ -0.61 after sell → book $9,847.31; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CHA` | 109 | $10.30 | $2.35 | $-30.82 | $6,169.71 | ▼ -30.82 after sell → book $9,844.96; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ESTC` | 13 | $103.89 | $2.05 | $+272.04 | $7,518.23 | ▲ +272.04 after sell → book $9,842.91; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `HAFN` | 146 | $8.35 | $2.46 | $+59.35 | $8,734.87 | ▲ +59.35 after sell → book $9,840.45; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `MNSO` | 106 | $10.43 | $2.34 | $-53.40 | $9,838.12 | ▼ -53.40 after sell → book $9,838.12; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LX` | 4240 | $1.16 | $54.70 | — | $4,865.02 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-13.8; leftover $4919.06 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SAIC` | 37 | $129.46 | $2.10 | — | $72.90 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+2.1; leftover $4919.06 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.90 | ▼ close $9,736.62 vs 09:30 $9,855.81 (session -44.70) | 16:00 close · cash $72.90 · equity $9,736.62 vs 09:30 $9,855.81 (-119.19; session marks -44.70) · 2 name(s) marked open→close (per-name table). LX×4240 09:30 $1.16 → close $1.18 +84.80; SAIC×37 09:30 $129.46 → close $125.96 -129.50 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.90 | ▼ 09:30 equity $9,549.73 vs yday $9,736.62 (-186.89) | 09:30 open · cash $72.90 (unchanged overnight, no fees) · equity $9,549.73 vs prior close $9,736.62 (-186.89) · 2 name(s) re-marked at the open (per-name table). LX×4240 yday $1.18 → 09:30 $1.01 -720.80; SAIC×37 yday $125.96 → 09:30 $140.39 +533.91 | — |
| 2026-08-31 09:30 ET | **SELL** | `LX` | 4240 | $1.01 | $55.43 | $-746.13 | $4,299.86 | ▼ -746.13 after sell → book $9,494.29; vs 09:30 mark -55.44 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `SAIC` | 37 | $140.39 | $2.15 | $+400.16 | $9,492.14 | ▲ +400.16 after sell → book $9,492.14; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,492.14 | ▲ close $9,492.14 vs 09:30 $9,549.73 (session +0.00) | 16:00 close · cash $9,492.14 · no lots left · equity $9,492.14. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,492.14 | ▲ 09:30 equity $9,492.14 vs yday $9,492.14 (+0.00) | 09:30 open · cash $9,492.14 · no holdings · equity $9,492.14 vs prior close $9,492.14 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,492.14 | ▲ close $9,492.14 vs 09:30 $9,492.14 (session +0.00) | 16:00 close · cash $9,492.14 · no lots left · equity $9,492.14. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,492.14 | ▲ 09:30 equity $9,492.14 vs yday $9,492.14 (+0.00) | 09:30 open · cash $9,492.14 · no holdings · equity $9,492.14 vs prior close $9,492.14 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,492.14 | ▲ close $9,492.14 vs 09:30 $9,492.14 (session +0.00) | 16:00 close · cash $9,492.14 · no lots left · equity $9,492.14. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,492.14 | ▲ 09:30 equity $9,492.14 vs yday $9,492.14 (+0.00) | 09:30 open · cash $9,492.14 · no holdings · equity $9,492.14 vs prior close $9,492.14 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AMBA` | 17 | $66.61 | $2.04 | — | $8,357.73 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-3.6; leftover $1186.52 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ASAN` | 116 | $10.16 | $2.34 | — | $7,176.83 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+3.8; leftover $1186.52 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟡 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DOCU` | 17 | $67.06 | $2.04 | — | $6,034.77 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+10.2; leftover $1186.52 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DOMO` | 313 | $3.78 | $4.04 | — | $4,847.59 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-2.4; leftover $1186.52 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GWRE` | 5 | $198.00 | $2.00 | — | $3,855.59 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+0.9; leftover $1186.52 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `IOT` | 31 | $37.69 | $2.08 | — | $2,685.12 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-7.7; leftover $1186.52 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `LULU` | 9 | $121.15 | $2.02 | — | $1,592.75 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+3.2; leftover $1186.52 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MAMA` | 75 | $15.62 | $2.21 | — | $419.03 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-6.7; leftover $1186.52 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $419.03 | ▲ close $9,483.17 vs 09:30 $9,492.14 (session +9.81) | 16:00 close · cash $419.03 · equity $9,483.17 vs 09:30 $9,492.14 (-8.97; session marks +9.81) · 8 name(s) marked open→close (per-name table). AMBA×17 09:30 $66.61 → close $63.38 -54.91; ASAN×116 09:30 $10.16 → close $10.09 -8.12; DOCU×17 09:30 $67.06 → close $65.97 -18.53; DOMO×313 09:30 $3.78 → close $3.79 +3.13; GWRE×5 09:30 $198.00 → close $202.86 +24.30; IOT×31 09:30 $37.69 → close $38.75 +32.86; LULU×9 09:30 $121.15 → close $121.77 +5.58; MAMA×75 09:30 $15.62 → close $15.96 +25.50 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $419.03 | ▼ 09:30 equity $9,093.77 vs yday $9,483.17 (-389.40) | 09:30 open · cash $419.03 (unchanged overnight, no fees) · equity $9,093.77 vs prior close $9,483.17 (-389.40) · 8 name(s) re-marked at the open (per-name table). AMBA×17 yday $63.38 → 09:30 $63.18 -3.40; ASAN×116 yday $10.09 → 09:30 $8.74 -156.60; DOCU×17 yday $65.97 → 09:30 $68.52 +43.35; DOMO×313 yday $3.79 → 09:30 $3.62 -54.77; GWRE×5 yday $202.86 → 09:30 $167.55 -176.55; IOT×31 yday $38.75 → 09:30 $44.90 +190.65; LULU×9 yday $121.77 → 09:30 $98.15 -212.58; MAMA×75 yday $15.96 → 09:30 $15.70 -19.50 | — |
| 2026-09-04 09:30 ET | **SELL** | `AMBA` | 17 | $63.18 | $2.06 | $-62.41 | $1,491.03 | ▼ -62.41 after sell → book $9,091.71; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `ASAN` | 116 | $8.74 | $2.37 | $-169.43 | $2,502.51 | ▼ -169.43 after sell → book $9,089.34; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `DOCU` | 17 | $68.52 | $2.06 | $+20.72 | $3,665.28 | ▲ +20.72 after sell → book $9,087.28; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `DOMO` | 313 | $3.62 | $4.10 | $-59.78 | $4,792.68 | ▼ -59.78 after sell → book $9,083.18; vs 09:30 mark -4.10 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `GWRE` | 5 | $167.55 | $2.02 | $-156.28 | $5,628.41 | ▼ -156.28 after sell → book $9,081.16; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `IOT` | 31 | $44.90 | $2.10 | $+219.32 | $7,018.20 | ▲ +219.32 after sell → book $9,079.05; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `LULU` | 9 | $98.15 | $2.04 | $-211.05 | $7,899.51 | ▼ -211.05 after sell → book $9,077.01; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `MAMA` | 75 | $15.70 | $2.24 | $+1.55 | $9,074.78 | ▲ +1.55 after sell → book $9,074.78; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ABM` | 96 | $46.79 | $2.28 | — | $4,580.66 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+0.2; leftover $4537.39 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `UNFI` | 103 | $43.80 | $2.30 | — | $66.96 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-7.7; leftover $4537.39 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $66.96 | ▲ close $9,108.55 vs 09:30 $9,093.77 (session +38.35) | 16:00 close · cash $66.96 · equity $9,108.55 vs 09:30 $9,093.77 (+14.78; session marks +38.35) · 2 name(s) marked open→close (per-name table). ABM×96 09:30 $46.79 → close $47.05 +24.96; UNFI×103 09:30 $43.80 → close $43.93 +13.39 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $66.96 | ▲ 09:30 equity $9,121.35 vs yday $9,108.55 (+12.80) | 09:30 open · cash $66.96 (unchanged overnight, no fees) · equity $9,121.35 vs prior close $9,108.55 (+12.80) · 2 name(s) re-marked at the open (per-name table). ABM×96 yday $47.05 → 09:30 $45.81 -119.04; UNFI×103 yday $43.93 → 09:30 $45.21 +131.84 | — |
| 2026-09-08 09:30 ET | **SELL** | `ABM` | 96 | $45.81 | $2.33 | $-98.69 | $4,462.39 | ▼ -98.69 after sell → book $9,119.02; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `UNFI` | 103 | $45.21 | $2.35 | $+140.58 | $9,116.67 | ▲ +140.58 after sell → book $9,116.67; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,116.67 | ▲ close $9,116.67 vs 09:30 $9,121.35 (session +0.00) | 16:00 close · cash $9,116.67 · no lots left · equity $9,116.67. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,116.67 | ▲ 09:30 equity $9,116.67 vs yday $9,116.67 (-0.00) | 09:30 open · cash $9,116.67 · no holdings · equity $9,116.67 vs prior close $9,116.67 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,116.67 | ▲ close $9,116.67 vs 09:30 $9,116.67 (session +0.00) | 16:00 close · cash $9,116.67 · no lots left · equity $9,116.67. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,116.67 | ▲ 09:30 equity $9,116.67 vs yday $9,116.67 (-0.00) | 09:30 open · cash $9,116.67 · no holdings · equity $9,116.67 vs prior close $9,116.67 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,116.67 | ▲ close $9,116.67 vs 09:30 $9,116.67 (session +0.00) | 16:00 close · cash $9,116.67 · no lots left · equity $9,116.67. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,116.67 | ▲ 09:30 equity $9,116.67 vs yday $9,116.67 (-0.00) | 09:30 open · cash $9,116.67 · no holdings · equity $9,116.67 vs prior close $9,116.67 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,116.67 | ▲ close $9,116.67 vs 09:30 $9,116.67 (session +0.00) | 16:00 close · cash $9,116.67 · no lots left · equity $9,116.67. | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,116.67 | ▲ 09:30 equity $9,116.67 vs yday $9,116.67 (-0.00) | 09:30 open · cash $9,116.67 · no holdings · equity $9,116.67 vs prior close $9,116.67 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,116.67 | ▲ close $9,116.67 vs 09:30 $9,116.67 (session +0.00) | 16:00 close · cash $9,116.67 · no lots left · equity $9,116.67. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,116.67 | ▲ 09:30 equity $9,116.67 vs yday $9,116.67 (-0.00) | 09:30 open · cash $9,116.67 · no holdings · equity $9,116.67 vs prior close $9,116.67 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,116.67 | ▲ close $9,116.67 vs 09:30 $9,116.67 (session +0.00) | 16:00 close · cash $9,116.67 · no lots left · equity $9,116.67. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,116.67 | ▲ 09:30 equity $9,116.67 vs yday $9,116.67 (-0.00) | 09:30 open · cash $9,116.67 · no holdings · equity $9,116.67 vs prior close $9,116.67 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `ALMU` | 331 | $13.75 | $4.27 | — | $4,561.15 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-1.4; leftover $4558.33 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `LEN` | 56 | $80.63 | $2.16 | — | $43.71 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-0.4; leftover $4558.33 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🔴 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $43.71 | ▼ close $8,877.20 vs 09:30 $9,116.67 (session -233.04) | 16:00 close · cash $43.71 · equity $8,877.20 vs 09:30 $9,116.67 (-239.47; session marks -233.04) · 2 name(s) marked open→close (per-name table). ALMU×331 09:30 $13.75 → close $13.43 -105.92; LEN×56 09:30 $80.63 → close $78.36 -127.12 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.71 | ▼ 09:30 equity $8,290.22 vs yday $8,877.20 (-586.98) | 09:30 open · cash $43.71 (unchanged overnight, no fees) · equity $8,290.22 vs prior close $8,877.20 (-586.98) · 2 name(s) re-marked at the open (per-name table). ALMU×331 yday $13.43 → 09:30 $11.21 -734.82; LEN×56 yday $78.36 → 09:30 $81.00 +147.84 | — |
| 2026-09-17 09:30 ET | **SELL** | `ALMU` | 331 | $11.21 | $4.35 | $-849.36 | $3,749.86 | ▼ -849.36 after sell → book $8,285.86; vs 09:30 mark -4.36 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `LEN` | 56 | $81.00 | $2.20 | $+16.36 | $8,283.66 | ▲ +16.36 after sell → book $8,283.66; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,283.66 | ▲ close $8,283.66 vs 09:30 $8,290.22 (session +0.00) | 16:00 close · cash $8,283.66 · no lots left · equity $8,283.66. | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,283.66 | ▲ 09:30 equity $8,283.66 vs yday $8,283.66 (+0.00) | 09:30 open · cash $8,283.66 · no holdings · equity $8,283.66 vs prior close $8,283.66 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,283.66 | ▲ close $8,283.66 vs 09:30 $8,283.66 (session +0.00) | 16:00 close · cash $8,283.66 · no lots left · equity $8,283.66. | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,283.66 | ▲ 09:30 equity $8,283.66 vs yday $8,283.66 (+0.00) | 09:30 open · cash $8,283.66 · no holdings · equity $8,283.66 vs prior close $8,283.66 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-21 09:30 ET | **BUY** | `ABVX` | 78 | $105.72 | $2.22 | — | $35.28 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+6.5; leftover $8283.66 | join🟡 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.28 | ▼ close $8,104.38 vs 09:30 $8,283.66 (session -177.06) | 16:00 close · cash $35.28 · equity $8,104.38 vs 09:30 $8,283.66 (-179.28; session marks -177.06) · 1 name(s) marked open→close (per-name table). ABVX×78 09:30 $105.72 → close $103.45 -177.06 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `DVLT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `EL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `JKHY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KEYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `LOW` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MRCY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TGT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AEG` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ALVO` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ATAT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ATHM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BABA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BILL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BULL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `BNS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BZ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SHMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SLQT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TUYA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `VIPS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `YEXT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MDT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MMED` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NIO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RZLV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SSL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `GTLB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BF-B` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CRDO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DELL` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FCEL` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PANW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `AI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CHPT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CIEN` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CPB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FIVE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MOMO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NTSK` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PHR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BRZE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `KFY` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ODD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUNB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `AVAV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `COO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `M` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAVN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `DSGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `KR` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LPTH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `REF` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `RH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `FPS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `PLAY` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `TCOM` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ABVX` | 78 | 2026-09-21 @ $105.72 | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+6.5; leftover $8283.66 |
