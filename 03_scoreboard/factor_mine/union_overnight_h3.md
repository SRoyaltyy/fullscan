# Factor mine action — `union_overnight_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ overnight, no 🚨

Cash book **-16.88%** ($8,312) · signal-only (no cash/fees) was -35.08%. Starts YES **0/27**. Fills 81 · skips 159 · realized $-1504.02.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `overnight=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $36.15.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `DUOT` | 353 | — | $9.43 | +0.00 | $9.11 | -112.96 | -112.96 | +0.00 | -112.96 |
| 2026-08-14 | `NUAI` | 658 | — | $5.06 | +0.00 | $5.07 | +6.58 | +6.58 | +0.00 | +6.58 |
| 2026-08-14 | `SIDU` | 1298 | — | $2.55 | +0.00 | $2.60 | +64.90 | +64.90 | +0.00 | +64.90 |
| 2026-08-17 | `DUOT` | 353 | $9.11 | $10.35 | +437.72 | $10.28 | -24.71 | +413.01 | +324.76 | +300.05 |
| 2026-08-17 | `NUAI` | 658 | $5.07 | $5.20 | +85.54 | $5.46 | +171.08 | +256.62 | +92.12 | +263.20 |
| 2026-08-17 | `SIDU` | 1298 | $2.60 | $2.40 | -259.60 | $2.54 | +175.23 | -84.37 | -194.70 | -19.47 |
| 2026-08-18 | `DUOT` | 353 | $10.28 | $11.53 | +439.49 | $11.59 | +22.94 | +462.43 | +739.54 | +762.48 |
| 2026-08-18 | `NUAI` | 658 | $5.46 | $5.23 | -151.34 | $5.13 | -65.80 | -217.14 | +111.86 | +46.06 |
| 2026-08-18 | `SIDU` | 1298 | $2.54 | $2.43 | -136.29 | $2.44 | +12.98 | -123.31 | -155.76 | -142.78 |
| 2026-08-19 | `DUOT` | 353 | $11.59 | $11.52 | -24.71 | — | +0.00 | -24.71 | +737.77 | — |
| 2026-08-19 | `NUAI` | 658 | $5.13 | $5.13 | +0.00 | — | +0.00 | +0.00 | +46.06 | — |
| 2026-08-19 | `SIDU` | 1298 | $2.44 | $2.45 | +12.98 | — | +0.00 | +12.98 | -129.80 | — |
| 2026-08-20 | `BEKE` | 124 | — | $17.04 | +0.00 | $16.99 | -6.20 | -6.20 | +0.00 | -6.20 |
| 2026-08-20 | `BJ` | 23 | — | $88.91 | +0.00 | $91.30 | +54.97 | +54.97 | +0.00 | +54.97 |
| 2026-08-20 | `BKE` | 49 | — | $42.60 | +0.00 | $42.64 | +1.96 | +1.96 | +0.00 | +1.96 |
| 2026-08-20 | `FLO` | 285 | — | $7.43 | +0.00 | $7.10 | -94.05 | -94.05 | +0.00 | -94.05 |
| 2026-08-20 | `ROST` | 9 | — | $229.55 | +0.00 | $228.99 | -5.04 | -5.04 | +0.00 | -5.04 |
| 2026-08-21 | `BEKE` | 124 | $16.99 | $17.93 | +117.18 | $17.75 | -22.94 | +94.24 | +110.98 | +88.04 |
| 2026-08-21 | `BJ` | 23 | $91.30 | $93.98 | +61.64 | $96.42 | +56.12 | +117.76 | +116.61 | +172.73 |
| 2026-08-21 | `BKE` | 49 | $42.64 | $43.08 | +21.56 | $43.81 | +35.77 | +57.33 | +23.52 | +59.29 |
| 2026-08-21 | `FLO` | 285 | $7.10 | $6.90 | -57.00 | $6.95 | +14.25 | -42.75 | -151.05 | -136.80 |
| 2026-08-21 | `ROST` | 9 | $228.99 | $243.85 | +133.74 | $239.04 | -43.29 | +90.45 | +128.70 | +85.41 |
| 2026-08-21 | `XPEV` | 6 | — | $12.29 | +0.00 | $12.19 | -0.60 | -0.60 | +0.00 | -0.60 |
| 2026-08-24 | `BEKE` | 124 | $17.75 | $18.05 | +37.82 | $17.71 | -42.78 | -4.96 | +125.86 | +83.08 |
| 2026-08-24 | `BJ` | 23 | $96.42 | $97.02 | +13.80 | $98.49 | +33.81 | +47.61 | +186.53 | +220.34 |
| 2026-08-24 | `BKE` | 49 | $43.81 | $44.22 | +20.09 | $44.41 | +9.31 | +29.40 | +79.38 | +88.69 |
| 2026-08-24 | `FLO` | 285 | $6.95 | $6.96 | +2.85 | $7.28 | +91.20 | +94.05 | -133.95 | -42.75 |
| 2026-08-24 | `ROST` | 9 | $239.04 | $238.08 | -8.64 | $241.52 | +30.96 | +22.32 | +76.77 | +107.73 |
| 2026-08-24 | `XPEV` | 6 | $12.19 | $11.83 | -2.16 | $11.15 | -4.08 | -6.24 | -2.76 | -6.84 |
| 2026-08-25 | `BEKE` | 124 | $17.71 | $17.63 | -9.92 | — | +0.00 | -9.92 | +73.16 | — |
| 2026-08-25 | `BJ` | 23 | $98.49 | $97.63 | -19.78 | — | +0.00 | -19.78 | +200.56 | — |
| 2026-08-25 | `BKE` | 49 | $44.41 | $44.50 | +4.41 | — | +0.00 | +4.41 | +93.10 | — |
| 2026-08-25 | `FLO` | 285 | $7.28 | $7.25 | -8.55 | — | +0.00 | -8.55 | -51.30 | — |
| 2026-08-25 | `ROST` | 9 | $241.52 | $241.50 | -0.18 | — | +0.00 | -0.18 | +107.55 | — |
| 2026-08-25 | `XPEV` | 6 | $11.15 | $11.19 | +0.21 | $11.60 | +2.49 | +2.70 | -6.63 | -4.14 |
| 2026-08-25 | `ANF` | 12 | — | $112.17 | +0.00 | $108.90 | -39.24 | -39.24 | +0.00 | -39.24 |
| 2026-08-25 | `BBWI` | 71 | — | $19.16 | +0.00 | $17.58 | -112.18 | -112.18 | +0.00 | -112.18 |
| 2026-08-25 | `BOX` | 40 | — | $33.33 | +0.00 | $33.00 | -13.20 | -13.20 | +0.00 | -13.20 |
| 2026-08-25 | `DCI` | 14 | — | $93.64 | +0.00 | $93.26 | -5.32 | -5.32 | +0.00 | -5.32 |
| 2026-08-25 | `DY` | 3 | — | $390.22 | +0.00 | $351.80 | -115.26 | -115.26 | +0.00 | -115.26 |
| 2026-08-25 | `FSCO` | 267 | — | $5.10 | +0.00 | $5.07 | -8.01 | -8.01 | +0.00 | -8.01 |
| 2026-08-25 | `HEI` | 3 | — | $357.15 | +0.00 | $351.05 | -18.30 | -18.30 | +0.00 | -18.30 |
| 2026-08-25 | `INTU` | 3 | — | $364.35 | +0.00 | $357.46 | -20.67 | -20.67 | +0.00 | -20.67 |
| 2026-08-26 | `XPEV` | 6 | $11.60 | $11.90 | +1.80 | — | +0.00 | +1.80 | -2.34 | — |
| 2026-08-26 | `ANF` | 12 | $108.90 | $131.37 | +269.64 | $147.75 | +196.56 | +466.20 | +230.40 | +426.96 |
| 2026-08-26 | `BBWI` | 71 | $17.58 | $18.26 | +48.28 | $18.90 | +45.44 | +93.72 | -63.90 | -18.46 |
| 2026-08-26 | `BOX` | 40 | $33.00 | $34.30 | +52.00 | $33.39 | -36.40 | +15.60 | +38.80 | +2.40 |
| 2026-08-26 | `DCI` | 14 | $93.26 | $95.13 | +26.18 | $95.24 | +1.54 | +27.72 | +20.86 | +22.40 |
| 2026-08-26 | `DY` | 3 | $351.80 | $326.91 | -74.67 | $310.91 | -48.00 | -122.67 | -189.93 | -237.93 |
| 2026-08-26 | `FSCO` | 267 | $5.07 | $5.08 | +2.67 | $5.12 | +10.68 | +13.35 | -5.34 | +5.34 |
| 2026-08-26 | `HEI` | 3 | $351.05 | $370.00 | +56.85 | $346.15 | -71.55 | -14.70 | +38.55 | -33.00 |
| 2026-08-26 | `INTU` | 3 | $357.46 | $323.47 | -101.97 | $345.88 | +67.23 | -34.74 | -122.64 | -55.41 |
| 2026-08-26 | `STDN` | 8 | — | $13.95 | +0.00 | $13.70 | -2.00 | -2.00 | +0.00 | -2.00 |
| 2026-08-26 | `BBY` | 1 | — | $85.19 | +0.00 | $87.44 | +2.25 | +2.25 | +0.00 | +2.25 |
| 2026-08-26 | `BILI` | 7 | — | $16.22 | +0.00 | $16.15 | -0.49 | -0.49 | +0.00 | -0.49 |
| 2026-08-26 | `CMBT` | 6 | — | $17.91 | +0.00 | $17.65 | -1.56 | -1.56 | +0.00 | -1.56 |
| 2026-08-27 | `ANF` | 12 | $147.75 | $144.70 | -36.60 | $145.75 | +12.60 | -24.00 | +390.36 | +402.96 |
| 2026-08-27 | `BBWI` | 71 | $18.90 | $18.69 | -14.91 | $18.65 | -2.84 | -17.75 | -33.37 | -36.21 |
| 2026-08-27 | `BOX` | 40 | $33.39 | $33.79 | +16.00 | $34.74 | +38.00 | +54.00 | +18.40 | +56.40 |
| 2026-08-27 | `DCI` | 14 | $95.24 | $93.52 | -24.08 | $92.41 | -15.54 | -39.62 | -1.68 | -17.22 |
| 2026-08-27 | `DY` | 3 | $310.91 | $314.90 | +11.97 | $308.01 | -20.67 | -8.70 | -225.96 | -246.63 |
| 2026-08-27 | `FSCO` | 267 | $5.12 | $5.10 | -5.34 | $5.12 | +5.34 | +0.00 | +0.00 | +5.34 |
| 2026-08-27 | `HEI` | 3 | $346.15 | $346.19 | +0.12 | $337.01 | -27.54 | -27.42 | -32.88 | -60.42 |
| 2026-08-27 | `INTU` | 3 | $345.88 | $353.54 | +22.98 | $348.00 | -16.62 | +6.36 | -32.43 | -49.05 |
| 2026-08-27 | `STDN` | 8 | $13.70 | $13.84 | +1.12 | $14.31 | +3.76 | +4.88 | -0.88 | +2.88 |
| 2026-08-27 | `BBY` | 1 | $87.44 | $80.60 | -6.84 | $83.56 | +2.96 | -3.88 | -4.59 | -1.63 |
| 2026-08-27 | `BILI` | 7 | $16.15 | $16.18 | +0.21 | $16.77 | +4.10 | +4.31 | -0.28 | +3.82 |
| 2026-08-27 | `CMBT` | 6 | $17.65 | $17.78 | +0.78 | $18.28 | +3.00 | +3.78 | -0.78 | +2.22 |
| 2026-08-27 | `GAP` | 3 | — | $20.75 | +0.00 | $20.79 | +0.12 | +0.12 | +0.00 | +0.12 |
| 2026-08-27 | `BBAR` | 4 | — | $14.96 | +0.00 | $14.60 | -1.44 | -1.44 | +0.00 | -1.44 |
| 2026-08-27 | `CHA` | 5 | — | $10.54 | +0.00 | $10.35 | -0.95 | -0.95 | +0.00 | -0.95 |
| 2026-08-27 | `HAFN` | 7 | — | $7.91 | +0.00 | $8.29 | +2.66 | +2.66 | +0.00 | +2.66 |
| 2026-08-27 | `MNSO` | 5 | — | $10.89 | +0.00 | $10.81 | -0.40 | -0.40 | +0.00 | -0.40 |
| 2026-08-28 | `ANF` | 12 | $145.75 | $146.07 | +3.84 | — | +0.00 | +3.84 | +406.80 | — |
| 2026-08-28 | `BBWI` | 71 | $18.65 | $18.75 | +7.10 | — | +0.00 | +7.10 | -29.11 | — |
| 2026-08-28 | `BOX` | 40 | $34.74 | $34.75 | +0.40 | — | +0.00 | +0.40 | +56.80 | — |
| 2026-08-28 | `DCI` | 14 | $92.41 | $92.25 | -2.24 | — | +0.00 | -2.24 | -19.46 | — |
| 2026-08-28 | `DY` | 3 | $308.01 | $306.34 | -5.01 | — | +0.00 | -5.01 | -251.64 | — |
| 2026-08-28 | `FSCO` | 267 | $5.12 | $5.12 | +0.00 | — | +0.00 | +0.00 | +5.34 | — |
| 2026-08-28 | `HEI` | 3 | $337.01 | $339.95 | +8.82 | — | +0.00 | +8.82 | -51.60 | — |
| 2026-08-28 | `INTU` | 3 | $348.00 | $347.82 | -0.54 | — | +0.00 | -0.54 | -49.59 | — |
| 2026-08-28 | `STDN` | 8 | $14.31 | $14.50 | +1.52 | $14.10 | -3.20 | -1.68 | +4.40 | +1.20 |
| 2026-08-28 | `BBY` | 1 | $83.56 | $83.85 | +0.29 | $82.44 | -1.41 | -1.12 | -1.34 | -2.75 |
| 2026-08-28 | `BILI` | 7 | $16.77 | $16.94 | +1.23 | $16.60 | -2.38 | -1.15 | +5.04 | +2.66 |
| 2026-08-28 | `CMBT` | 6 | $18.28 | $18.58 | +1.80 | $18.35 | -1.38 | +0.42 | +4.02 | +2.64 |
| 2026-08-28 | `GAP` | 3 | $20.79 | $24.69 | +11.70 | $23.48 | -3.63 | +8.07 | +11.82 | +8.19 |
| 2026-08-28 | `BBAR` | 4 | $14.60 | $15.01 | +1.64 | $14.47 | -2.16 | -0.52 | +0.20 | -1.96 |
| 2026-08-28 | `CHA` | 5 | $10.35 | $10.30 | -0.25 | $10.80 | +2.50 | +2.25 | -1.20 | +1.30 |
| 2026-08-28 | `HAFN` | 7 | $8.29 | $8.35 | +0.42 | $8.47 | +0.84 | +1.26 | +3.08 | +3.92 |
| 2026-08-28 | `MNSO` | 5 | $10.81 | $10.43 | -1.90 | $10.33 | -0.50 | -2.40 | -2.30 | -2.80 |
| 2026-08-28 | `LX` | 4444 | — | $1.16 | +0.00 | $1.18 | +88.88 | +88.88 | +0.00 | +88.88 |
| 2026-08-28 | `SAIC` | 39 | — | $129.46 | +0.00 | $125.96 | -136.50 | -136.50 | +0.00 | -136.50 |
| 2026-08-31 | `STDN` | 8 | $14.10 | $14.35 | +2.00 | — | +0.00 | +2.00 | +3.20 | — |
| 2026-08-31 | `BBY` | 1 | $82.44 | $81.94 | -0.50 | — | +0.00 | -0.50 | -3.25 | — |
| 2026-08-31 | `BILI` | 7 | $16.60 | $16.53 | -0.49 | — | +0.00 | -0.49 | +2.17 | — |
| 2026-08-31 | `CMBT` | 6 | $18.35 | $18.82 | +2.82 | — | +0.00 | +2.82 | +5.46 | — |
| 2026-08-31 | `GAP` | 3 | $23.48 | $22.98 | -1.50 | $22.31 | -2.01 | -3.51 | +6.69 | +4.68 |
| 2026-08-31 | `BBAR` | 4 | $14.47 | $14.88 | +1.64 | $15.14 | +1.04 | +2.68 | -0.32 | +0.72 |
| 2026-08-31 | `CHA` | 5 | $10.80 | $11.21 | +2.05 | $11.63 | +2.10 | +4.15 | +3.35 | +5.45 |
| 2026-08-31 | `HAFN` | 7 | $8.47 | $8.53 | +0.42 | $8.44 | -0.63 | -0.21 | +4.34 | +3.71 |
| 2026-08-31 | `MNSO` | 5 | $10.33 | $9.21 | -5.60 | $9.30 | +0.45 | -5.15 | -8.40 | -7.95 |
| 2026-08-31 | `LX` | 4444 | $1.18 | $1.01 | -755.48 | $1.07 | +266.64 | -488.84 | -666.60 | -399.96 |
| 2026-08-31 | `SAIC` | 39 | $125.96 | $140.39 | +562.77 | $128.22 | -474.63 | +88.14 | +426.27 | -48.36 |
| 2026-09-01 | `GAP` | 3 | $22.31 | $22.05 | -0.78 | — | +0.00 | -0.78 | +3.90 | — |
| 2026-09-01 | `BBAR` | 4 | $15.14 | $14.82 | -1.28 | — | +0.00 | -1.28 | -0.56 | — |
| 2026-09-01 | `CHA` | 5 | $11.63 | $11.63 | +0.00 | — | +0.00 | +0.00 | +5.45 | — |
| 2026-09-01 | `HAFN` | 7 | $8.44 | $8.56 | +0.84 | — | +0.00 | +0.84 | +4.55 | — |
| 2026-09-01 | `MNSO` | 5 | $9.30 | $9.39 | +0.45 | — | +0.00 | +0.45 | -7.50 | — |
| 2026-09-01 | `LX` | 4444 | $1.07 | $1.01 | -266.64 | $0.88 | -568.83 | -835.47 | -666.60 | -1235.43 |
| 2026-09-01 | `SAIC` | 39 | $128.22 | $127.01 | -47.19 | $126.78 | -8.97 | -56.16 | -95.55 | -104.52 |
| 2026-09-02 | `LX` | 4444 | $0.88 | $0.91 | +106.66 | — | +0.00 | +106.66 | -1128.78 | — |
| 2026-09-02 | `SAIC` | 39 | $126.78 | $126.10 | -26.52 | — | +0.00 | -26.52 | -131.04 | — |
| 2026-09-03 | `AMBA` | 18 | — | $66.61 | +0.00 | $63.38 | -58.14 | -58.14 | +0.00 | -58.14 |
| 2026-09-03 | `ASAN` | 118 | — | $10.16 | +0.00 | $10.09 | -8.26 | -8.26 | +0.00 | -8.26 |
| 2026-09-03 | `DOCU` | 17 | — | $67.06 | +0.00 | $65.97 | -18.53 | -18.53 | +0.00 | -18.53 |
| 2026-09-03 | `DOMO` | 318 | — | $3.78 | +0.00 | $3.79 | +3.18 | +3.18 | +0.00 | +3.18 |
| 2026-09-03 | `GWRE` | 6 | — | $198.00 | +0.00 | $202.86 | +29.16 | +29.16 | +0.00 | +29.16 |
| 2026-09-03 | `IOT` | 31 | — | $37.69 | +0.00 | $38.75 | +32.86 | +32.86 | +0.00 | +32.86 |
| 2026-09-03 | `LULU` | 9 | — | $121.15 | +0.00 | $121.77 | +5.58 | +5.58 | +0.00 | +5.58 |
| 2026-09-03 | `MAMA` | 77 | — | $15.62 | +0.00 | $15.96 | +26.18 | +26.18 | +0.00 | +26.18 |
| 2026-09-04 | `AMBA` | 18 | $63.38 | $63.18 | -3.60 | $62.89 | -5.22 | -8.82 | -61.74 | -66.96 |
| 2026-09-04 | `ASAN` | 118 | $10.09 | $8.74 | -159.30 | $8.81 | +8.26 | -151.04 | -167.56 | -159.30 |
| 2026-09-04 | `DOCU` | 17 | $65.97 | $68.52 | +43.35 | $68.41 | -1.87 | +41.48 | +24.82 | +22.95 |
| 2026-09-04 | `DOMO` | 318 | $3.79 | $3.62 | -55.65 | $3.88 | +84.27 | +28.62 | -52.47 | +31.80 |
| 2026-09-04 | `GWRE` | 6 | $202.86 | $167.55 | -211.86 | $162.42 | -30.78 | -242.64 | -182.70 | -213.48 |
| 2026-09-04 | `IOT` | 31 | $38.75 | $44.90 | +190.65 | $40.20 | -145.70 | +44.95 | +223.51 | +77.81 |
| 2026-09-04 | `LULU` | 9 | $121.77 | $98.15 | -212.58 | $100.61 | +22.14 | -190.44 | -207.00 | -184.86 |
| 2026-09-04 | `MAMA` | 77 | $15.96 | $15.70 | -20.02 | $15.16 | -41.58 | -61.60 | +6.16 | -35.42 |
| 2026-09-04 | `ABM` | 2 | — | $46.79 | +0.00 | $47.05 | +0.52 | +0.52 | +0.00 | +0.52 |
| 2026-09-04 | `UNFI` | 2 | — | $43.80 | +0.00 | $43.93 | +0.26 | +0.26 | +0.00 | +0.26 |
| 2026-09-08 | `AMBA` | 18 | $62.89 | $63.83 | +16.92 | $63.48 | -6.30 | +10.62 | -50.04 | -56.34 |
| 2026-09-08 | `ASAN` | 118 | $8.81 | $8.73 | -9.44 | $8.79 | +7.08 | -2.36 | -168.74 | -161.66 |
| 2026-09-08 | `DOCU` | 17 | $68.41 | $67.05 | -23.12 | $65.08 | -33.49 | -56.61 | -0.17 | -33.66 |
| 2026-09-08 | `DOMO` | 318 | $3.88 | $3.84 | -12.72 | $3.83 | -3.18 | -15.90 | +19.08 | +15.90 |
| 2026-09-08 | `GWRE` | 6 | $162.42 | $160.52 | -11.40 | $149.71 | -64.86 | -76.26 | -224.88 | -289.74 |
| 2026-09-08 | `IOT` | 31 | $40.20 | $39.56 | -19.84 | $40.15 | +18.29 | -1.55 | +57.97 | +76.26 |
| 2026-09-08 | `LULU` | 9 | $100.61 | $100.58 | -0.27 | $103.19 | +23.49 | +23.22 | -185.13 | -161.64 |
| 2026-09-08 | `MAMA` | 77 | $15.16 | $15.20 | +3.08 | $15.50 | +23.10 | +26.18 | -32.34 | -9.24 |
| 2026-09-08 | `ABM` | 2 | $47.05 | $45.81 | -2.48 | $50.60 | +9.58 | +7.10 | -1.96 | +7.62 |
| 2026-09-08 | `UNFI` | 2 | $43.93 | $45.21 | +2.56 | $44.93 | -0.56 | +2.00 | +2.82 | +2.26 |
| 2026-09-09 | `AMBA` | 18 | $63.48 | $63.07 | -7.38 | — | +0.00 | -7.38 | -63.72 | — |
| 2026-09-09 | `ASAN` | 118 | $8.79 | $8.64 | -17.70 | — | +0.00 | -17.70 | -179.36 | — |
| 2026-09-09 | `DOCU` | 17 | $65.08 | $64.64 | -7.48 | — | +0.00 | -7.48 | -41.14 | — |
| 2026-09-09 | `DOMO` | 318 | $3.83 | $3.86 | +9.54 | — | +0.00 | +9.54 | +25.44 | — |
| 2026-09-09 | `GWRE` | 6 | $149.71 | $147.85 | -11.16 | — | +0.00 | -11.16 | -300.90 | — |
| 2026-09-09 | `IOT` | 31 | $40.15 | $39.60 | -17.05 | — | +0.00 | -17.05 | +59.21 | — |
| 2026-09-09 | `LULU` | 9 | $103.19 | $101.90 | -11.61 | — | +0.00 | -11.61 | -173.25 | — |
| 2026-09-09 | `MAMA` | 77 | $15.50 | $15.31 | -14.63 | — | +0.00 | -14.63 | -23.87 | — |
| 2026-09-09 | `ABM` | 2 | $50.60 | $50.60 | +0.00 | $49.74 | -1.72 | -1.72 | +7.62 | +5.90 |
| 2026-09-09 | `UNFI` | 2 | $44.93 | $45.50 | +1.14 | $44.61 | -1.78 | -0.64 | +3.40 | +1.62 |
| 2026-09-10 | `ABM` | 2 | $49.74 | $49.74 | +0.00 | — | +0.00 | +0.00 | +5.90 | — |
| 2026-09-10 | `UNFI` | 2 | $44.61 | $44.89 | +0.56 | — | +0.00 | +0.56 | +2.18 | — |
| 2026-09-11 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `ALMU` | 324 | — | $13.75 | +0.00 | $13.43 | -103.68 | -103.68 | +0.00 | -103.68 |
| 2026-09-16 | `LEN` | 55 | — | $80.63 | +0.00 | $78.36 | -124.85 | -124.85 | +0.00 | -124.85 |
| 2026-09-17 | `ALMU` | 324 | $13.43 | $11.21 | -719.28 | $11.54 | +108.54 | -610.74 | -822.96 | -714.42 |
| 2026-09-17 | `LEN` | 55 | $78.36 | $81.00 | +145.20 | $79.70 | -71.50 | +73.70 | +20.35 | -51.15 |
| 2026-09-18 | `ALMU` | 324 | $11.54 | $11.64 | +30.78 | $12.72 | +351.54 | +382.32 | -683.64 | -332.10 |
| 2026-09-18 | `LEN` | 55 | $79.70 | $78.25 | -79.75 | $76.43 | -100.10 | -179.85 | -130.90 | -231.00 |
| 2026-09-21 | `ALMU` | 324 | $12.72 | $13.12 | +129.60 | — | +0.00 | +129.60 | -202.50 | — |
| 2026-09-21 | `LEN` | 55 | $76.43 | $76.98 | +30.25 | — | +0.00 | +30.25 | -200.75 | — |
| 2026-09-21 | `ABVX` | 80 | — | $105.72 | +0.00 | $103.45 | -181.60 | -181.60 | +0.00 | -181.60 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -41.48 | DUOT, NUAI, SIDU | — | $2.04 | $9,928.73 | DUOT×353, NUAI×658, SIDU×1298 |
| 2026-08-17 | +2.25 | $2.04 | DUOT×353, NUAI×658, SIDU×1298 | $10,192.39 | +263.66 | +321.60 | — | — | $2.04 | $10,513.99 | DUOT×353, NUAI×658, SIDU×1298 |
| 2026-08-18 | -6.20 | $2.04 | DUOT×353, NUAI×658, SIDU×1298 | $10,665.85 | +151.86 | -29.88 | — | — | $2.04 | $10,635.97 | DUOT×353, NUAI×658, SIDU×1298 |
| 2026-08-19 | -7.20 | $2.04 | DUOT×353, NUAI×658, SIDU×1298 | $10,624.24 | -11.73 | +0.00 | — | DUOT, NUAI, SIDU | $10,593.99 | $10,593.99 | — |
| 2026-08-20 | +1.12 | $10,593.99 | — | $10,593.99 | -0.00 | -48.36 | BEKE, BJ, BKE, FLO, ROST | — | $152.95 | $10,533.38 | BEKE×124, BJ×23, BKE×49, FLO×285, ROST×9 |
| 2026-08-21 | +3.25 | $152.95 | BEKE×124, BJ×23, BKE×49, FLO×285, ROST×9 | $10,810.50 | +277.12 | +39.31 | XPEV | — | $78.45 | $10,849.05 | BEKE×124, BJ×23, BKE×49, FLO×285, ROST×9, XPEV×6 |
| 2026-08-24 | -5.17 | $78.45 | BEKE×124, BJ×23, BKE×49, FLO×285, ROST×9, XPEV×6 | $10,912.81 | +63.76 | +118.42 | — | — | $78.45 | $11,031.23 | BEKE×124, BJ×23, BKE×49, FLO×285, ROST×9, XPEV×6 |
| 2026-08-25 | +1.80 | $78.45 | BEKE×124, BJ×23, BKE×49, FLO×285, ROST×9, XPEV×6 | $10,997.42 | -33.81 | -329.69 | ANF, BBWI, BOX, DCI, DY, FSCO, HEI, INTU | BEKE, BJ, BKE, FLO, ROST | $852.64 | $10,637.48 | XPEV×6, ANF×12, BBWI×71, BOX×40, DCI×14, DY×3, FSCO×267, HEI×3, INTU×3 |
| 2026-08-26 | +2.02 | $852.64 | XPEV×6, ANF×12, BBWI×71, BOX×40, DCI×14, DY×3, FSCO×267, HEI×3, INTU×3 | $10,918.26 | +280.78 | +163.70 | STDN, BBY, BILI, CMBT | XPEV | $501.26 | $11,076.97 | ANF×12, BBWI×71, BOX×40, DCI×14, DY×3, FSCO×267, HEI×3, INTU×3, STDN×8, BBY×1, BILI×7, CMBT×6 |
| 2026-08-27 | — | $501.26 | ANF×12, BBWI×71, BOX×40, DCI×14, DY×3, FSCO×267, HEI×3, INTU×3, STDN×8, BBY×1, BILI×7, CMBT×6 | $11,042.38 | -34.59 | -13.46 | GAP, BBAR, CHA, HAFN, MNSO | — | $213.73 | $11,026.00 | ANF×12, BBWI×71, BOX×40, DCI×14, DY×3, FSCO×267, HEI×3, INTU×3, STDN×8, BBY×1, BILI×7, CMBT×6, GAP×3, BBAR×4, CHA×5, HAFN×7, MNSO×5 |
| 2026-08-28 | +0.75 | $213.73 | ANF×12, BBWI×71, BOX×40, DCI×14, DY×3, FSCO×267, HEI×3, INTU×3, STDN×8, BBY×1, BILI×7, CMBT×6, GAP×3, BBAR×4, CHA×5, HAFN×7, MNSO×5 | $11,054.81 | +28.81 | -58.94 | LX, SAIC | ANF, BBWI, BOX, DCI, DY, FSCO, HEI, INTU | $47.26 | $10,918.42 | STDN×8, BBY×1, BILI×7, CMBT×6, GAP×3, BBAR×4, CHA×5, HAFN×7, MNSO×5, LX×4444, SAIC×39 |
| 2026-08-31 | -5.85 | $47.26 | STDN×8, BBY×1, BILI×7, CMBT×6, GAP×3, BBAR×4, CHA×5, HAFN×7, MNSO×5, LX×4444, SAIC×39 | $10,726.55 | -191.87 | -207.04 | — | STDN, BBY, BILI, CMBT | $468.23 | $10,515.11 | GAP×3, BBAR×4, CHA×5, HAFN×7, MNSO×5, LX×4444, SAIC×39 |
| 2026-09-01 | -6.30 | $468.23 | GAP×3, BBAR×4, CHA×5, HAFN×7, MNSO×5, LX×4444, SAIC×39 | $10,200.51 | -314.60 | -577.80 | — | GAP, BBAR, CHA, HAFN, MNSO | $755.60 | $9,619.63 | LX×4444, SAIC×39 |
| 2026-09-02 | -3.83 | $755.60 | LX×4444, SAIC×39 | $9,699.77 | +80.14 | +0.00 | — | LX, SAIC | $9,643.25 | $9,643.25 | — |
| 2026-09-03 | -0.90 | $9,643.25 | — | $9,643.25 | -0.00 | +12.03 | AMBA, ASAN, DOCU, DOMO, GWRE, IOT, LULU, MAMA | — | $234.99 | $9,636.42 | AMBA×18, ASAN×118, DOCU×17, DOMO×318, GWRE×6, IOT×31, LULU×9, MAMA×77 |
| 2026-09-04 | +2.25 | $234.99 | AMBA×18, ASAN×118, DOCU×17, DOMO×318, GWRE×6, IOT×31, LULU×9, MAMA×77 | $9,207.41 | -429.01 | -109.70 | ABM, UNFI | — | $51.98 | $9,095.88 | AMBA×18, ASAN×118, DOCU×17, DOMO×318, GWRE×6, IOT×31, LULU×9, MAMA×77, ABM×2, UNFI×2 |
| 2026-09-08 | -11.47 | $51.98 | AMBA×18, ASAN×118, DOCU×17, DOMO×318, GWRE×6, IOT×31, LULU×9, MAMA×77, ABM×2, UNFI×2 | $9,039.17 | -56.71 | -26.85 | — | — | $51.98 | $9,012.32 | AMBA×18, ASAN×118, DOCU×17, DOMO×318, GWRE×6, IOT×31, LULU×9, MAMA×77, ABM×2, UNFI×2 |
| 2026-09-09 | -13.95 | $51.98 | AMBA×18, ASAN×118, DOCU×17, DOMO×318, GWRE×6, IOT×31, LULU×9, MAMA×77, ABM×2, UNFI×2 | $8,935.99 | -76.33 | -3.50 | — | AMBA, ASAN, DOCU, DOMO, GWRE, IOT, LULU, MAMA | $8,724.72 | $8,913.42 | ABM×2, UNFI×2 |
| 2026-09-10 | -13.28 | $8,724.72 | ABM×2, UNFI×2 | $8,913.98 | +0.56 | +0.00 | — | ABM, UNFI | $8,912.03 | $8,912.03 | — |
| 2026-09-11 | +0.50 | $8,912.03 | — | $8,912.03 | +0.00 | +0.00 | — | — | $8,912.03 | $8,912.03 | — |
| 2026-09-14 | -11.00 | $8,912.03 | — | $8,912.03 | +0.00 | +0.00 | — | — | $8,912.03 | $8,912.03 | — |
| 2026-09-15 | -3.84 | $8,912.03 | — | $8,912.03 | +0.00 | +0.00 | — | — | $8,912.03 | $8,912.03 | — |
| 2026-09-16 | +5.30 | $8,912.03 | — | $8,912.03 | +0.00 | -228.53 | ALMU, LEN | — | $16.05 | $8,677.17 | ALMU×324, LEN×55 |
| 2026-09-17 | +7.38 | $16.05 | ALMU×324, LEN×55 | $8,103.09 | -574.08 | +37.04 | — | — | $16.05 | $8,140.13 | ALMU×324, LEN×55 |
| 2026-09-18 | +4.86 | $16.05 | ALMU×324, LEN×55 | $8,091.16 | -48.97 | +251.44 | — | — | $16.05 | $8,342.60 | ALMU×324, LEN×55 |
| 2026-09-21 | +12.87 | $16.05 | ALMU×324, LEN×55 | $8,502.45 | +159.85 | -181.60 | ABVX | ALMU, LEN | $36.15 | $8,312.15 | ABVX×80 |

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
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.04 | ▲ close $10,513.99 vs 09:30 $10,192.39 (session +321.60) | 16:00 close · cash $2.04 · equity $10,513.99 vs 09:30 $10,192.39 (+321.60; session marks +321.60) · 3 name(s) marked open→close (per-name table). DUOT×353 09:30 $10.35 → close $10.28 -24.71; NUAI×658 09:30 $5.20 → close $5.46 +171.08; SIDU×1298 09:30 $2.40 → close $2.54 +175.23 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.04 | ▲ 09:30 equity $10,665.85 vs yday $10,513.99 (+151.86) | 09:30 open · cash $2.04 (unchanged overnight, no fees) · equity $10,665.85 vs prior close $10,513.99 (+151.86) · 3 name(s) re-marked at the open (per-name table). DUOT×353 yday $10.28 → 09:30 $11.53 +439.49; NUAI×658 yday $5.46 → 09:30 $5.23 -151.34; SIDU×1298 yday $2.54 → 09:30 $2.43 -136.29 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.04 | ▼ close $10,635.97 vs 09:30 $10,665.85 (session -29.88) | 16:00 close · cash $2.04 · equity $10,635.97 vs 09:30 $10,665.85 (-29.88; session marks -29.88) · 3 name(s) marked open→close (per-name table). DUOT×353 09:30 $11.53 → close $11.59 +22.94; NUAI×658 09:30 $5.23 → close $5.13 -65.80; SIDU×1298 09:30 $2.43 → close $2.44 +12.98 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.04 | ▼ 09:30 equity $10,624.24 vs yday $10,635.97 (-11.73) | 09:30 open · cash $2.04 (unchanged overnight, no fees) · equity $10,624.24 vs prior close $10,635.97 (-11.73) · 3 name(s) re-marked at the open (per-name table). DUOT×353 yday $11.59 → 09:30 $11.52 -24.71; NUAI×658 yday $5.13 → 09:30 $5.13 +0.00; SIDU×1298 yday $2.44 → 09:30 $2.45 +12.98 | — |
| 2026-08-19 09:30 ET | **SELL** | `DUOT` | 353 | $11.52 | $4.64 | $+728.57 | $4,063.96 | ▲ +728.57 after sell → book $10,619.60; vs 09:30 mark -4.64 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `NUAI` | 658 | $5.13 | $8.62 | $+28.95 | $7,430.87 | ▲ +28.95 after sell → book $10,610.97; vs 09:30 mark -8.63 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `SIDU` | 1298 | $2.45 | $16.99 | $-163.53 | $10,593.99 | ▼ -163.53 after sell → book $10,593.99; vs 09:30 mark -16.98 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,593.99 | ▲ close $10,593.99 vs 09:30 $10,624.24 (session +0.00) | 16:00 close · cash $10,593.99 · no lots left · equity $10,593.99. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,593.99 | ▲ 09:30 equity $10,593.99 vs yday $10,593.99 (-0.00) | 09:30 open · cash $10,593.99 · no holdings · equity $10,593.99 vs prior close $10,593.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BEKE` | 124 | $17.04 | $2.36 | — | $8,478.67 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-0.2; leftover $2118.80 | join🟢 sector🔴 gen🟢 news🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BJ` | 23 | $88.91 | $2.06 | — | $6,431.68 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-1.0; leftover $2118.80 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BKE` | 49 | $42.60 | $2.14 | — | $4,342.14 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-4.6; leftover $2118.80 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `FLO` | 285 | $7.43 | $3.68 | — | $2,220.92 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+4.0; leftover $2118.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ROST` | 9 | $229.55 | $2.02 | — | $152.95 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight,overnight_mega; 🔵; ret5=-5.5; leftover $2118.80 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $152.95 | ▼ close $10,533.38 vs 09:30 $10,593.99 (session -48.36) | 16:00 close · cash $152.95 · equity $10,533.38 vs 09:30 $10,593.99 (-60.61; session marks -48.36) · 5 name(s) marked open→close (per-name table). BEKE×124 09:30 $17.04 → close $16.99 -6.20; BJ×23 09:30 $88.91 → close $91.30 +54.97; BKE×49 09:30 $42.60 → close $42.64 +1.96; FLO×285 09:30 $7.43 → close $7.10 -94.05; ROST×9 09:30 $229.55 → close $228.99 -5.04 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $152.95 | ▲ 09:30 equity $10,810.50 vs yday $10,533.38 (+277.12) | 09:30 open · cash $152.95 (unchanged overnight, no fees) · equity $10,810.50 vs prior close $10,533.38 (+277.12) · 5 name(s) re-marked at the open (per-name table). BEKE×124 yday $16.99 → 09:30 $17.93 +117.18; BJ×23 yday $91.30 → 09:30 $93.98 +61.64; BKE×49 yday $42.64 → 09:30 $43.08 +21.56; FLO×285 yday $7.10 → 09:30 $6.90 -57.00; ROST×9 yday $228.99 → 09:30 $243.85 +133.74 | — |
| 2026-08-21 09:30 ET | **BUY** | `XPEV` | 6 | $12.29 | $0.76 | — | $78.45 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+1.9; leftover $76.47 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.45 | ▲ close $10,849.05 vs 09:30 $10,810.50 (session +39.31) | 16:00 close · cash $78.45 · equity $10,849.05 vs 09:30 $10,810.50 (+38.55; session marks +39.31) · 6 name(s) marked open→close (per-name table). BEKE×124 09:30 $17.93 → close $17.75 -22.94; BJ×23 09:30 $93.98 → close $96.42 +56.12; BKE×49 09:30 $43.08 → close $43.81 +35.77; FLO×285 09:30 $6.90 → close $6.95 +14.25; ROST×9 09:30 $243.85 → close $239.04 -43.29; XPEV×6 09:30 $12.29 → close $12.19 -0.60 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.45 | ▲ 09:30 equity $10,912.81 vs yday $10,849.05 (+63.76) | 09:30 open · cash $78.45 (unchanged overnight, no fees) · equity $10,912.81 vs prior close $10,849.05 (+63.76) · 6 name(s) re-marked at the open (per-name table). BEKE×124 yday $17.75 → 09:30 $18.05 +37.82; BJ×23 yday $96.42 → 09:30 $97.02 +13.80; BKE×49 yday $43.81 → 09:30 $44.22 +20.09; FLO×285 yday $6.95 → 09:30 $6.96 +2.85; ROST×9 yday $239.04 → 09:30 $238.08 -8.64; XPEV×6 yday $12.19 → 09:30 $11.83 -2.16 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.45 | ▲ close $11,031.23 vs 09:30 $10,912.81 (session +118.42) | 16:00 close · cash $78.45 · equity $11,031.23 vs 09:30 $10,912.81 (+118.42; session marks +118.42) · 6 name(s) marked open→close (per-name table). BEKE×124 09:30 $18.05 → close $17.71 -42.78; BJ×23 09:30 $97.02 → close $98.49 +33.81; BKE×49 09:30 $44.22 → close $44.41 +9.31; FLO×285 09:30 $6.96 → close $7.28 +91.20; ROST×9 09:30 $238.08 → close $241.52 +30.96; XPEV×6 09:30 $11.83 → close $11.15 -4.08 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.45 | ▼ 09:30 equity $10,997.42 vs yday $11,031.23 (-33.81) | 09:30 open · cash $78.45 (unchanged overnight, no fees) · equity $10,997.42 vs prior close $11,031.23 (-33.81) · 6 name(s) re-marked at the open (per-name table). BEKE×124 yday $17.71 → 09:30 $17.63 -9.92; BJ×23 yday $98.49 → 09:30 $97.63 -19.78; BKE×49 yday $44.41 → 09:30 $44.50 +4.41; FLO×285 yday $7.28 → 09:30 $7.25 -8.55; ROST×9 yday $241.52 → 09:30 $241.50 -0.18; XPEV×6 yday $11.15 → 09:30 $11.19 +0.21 | — |
| 2026-08-25 09:30 ET | **SELL** | `BEKE` | 124 | $17.63 | $2.40 | $+68.40 | $2,262.17 | ▲ +68.40 after sell → book $10,995.02; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BJ` | 23 | $97.63 | $2.09 | $+196.41 | $4,505.58 | ▲ +196.41 after sell → book $10,992.94; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BKE` | 49 | $44.50 | $2.16 | $+88.80 | $6,683.91 | ▲ +88.80 after sell → book $10,990.77; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `FLO` | 285 | $7.25 | $3.74 | $-58.72 | $8,746.42 | ▼ -58.72 after sell → book $10,987.03; vs 09:30 mark -3.74 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ROST` | 9 | $241.50 | $2.04 | $+103.49 | $10,917.88 | ▲ +103.49 after sell → book $10,984.99; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `ANF` | 12 | $112.17 | $2.03 | — | $9,569.81 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ⚪; ret5=+6.8; leftover $1364.73 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BBWI` | 71 | $19.16 | $2.20 | — | $8,207.25 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+3.0; leftover $1364.73 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BOX` | 40 | $33.33 | $2.11 | — | $6,871.94 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+3.7; leftover $1364.73 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DCI` | 14 | $93.64 | $2.03 | — | $5,558.95 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-2.4; leftover $1364.73 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DY` | 3 | $390.22 | $2.00 | — | $4,386.29 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-12.0; leftover $1364.73 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🟡 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FSCO` | 267 | $5.10 | $3.44 | — | $3,021.14 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+0.2; leftover $1364.73 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HEI` | 3 | $357.15 | $2.00 | — | $1,947.69 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ⚪; ret5=-5.0; leftover $1364.73 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `INTU` | 3 | $364.35 | $2.00 | — | $852.64 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight,overnight_mega; 🔵; ret5=+10.2; leftover $1364.73 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $852.64 | ▼ close $10,637.48 vs 09:30 $10,997.42 (session -329.69) | 16:00 close · cash $852.64 · equity $10,637.48 vs 09:30 $10,997.42 (-359.94; session marks -329.69) · 9 name(s) marked open→close (per-name table). XPEV×6 09:30 $11.19 → close $11.60 +2.49; ANF×12 09:30 $112.17 → close $108.90 -39.24; BBWI×71 09:30 $19.16 → close $17.58 -112.18; BOX×40 09:30 $33.33 → close $33.00 -13.20; DCI×14 09:30 $93.64 → close $93.26 -5.32; DY×3 09:30 $390.22 → close $351.80 -115.26; FSCO×267 09:30 $5.10 → close $5.07 -8.01; HEI×3 09:30 $357.15 → close $351.05 -18.30; INTU×3 09:30 $364.35 → close $357.46 -20.67 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $852.64 | ▲ 09:30 equity $10,918.26 vs yday $10,637.48 (+280.78) | 09:30 open · cash $852.64 (unchanged overnight, no fees) · equity $10,918.26 vs prior close $10,637.48 (+280.78) · 9 name(s) re-marked at the open (per-name table). XPEV×6 yday $11.60 → 09:30 $11.90 +1.80; ANF×12 yday $108.90 → 09:30 $131.37 +269.64; BBWI×71 yday $17.58 → 09:30 $18.26 +48.28; BOX×40 yday $33.00 → 09:30 $34.30 +52.00; DCI×14 yday $93.26 → 09:30 $95.13 +26.18; DY×3 yday $351.80 → 09:30 $326.91 -74.67; FSCO×267 yday $5.07 → 09:30 $5.08 +2.67; HEI×3 yday $351.05 → 09:30 $370.00 +56.85; INTU×3 yday $357.46 → 09:30 $323.47 -101.97 | — |
| 2026-08-26 09:30 ET | **SELL** | `XPEV` | 6 | $11.90 | $0.75 | $-3.85 | $923.29 | ▼ -3.85 after sell → book $10,917.51; vs 09:30 mark -0.75 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `STDN` | 8 | $13.95 | $1.14 | — | $810.55 | — | union ∩ overnight, no 🚨; gate overnight=True; list ohlc_hot,overnight; 🔵; ret5=+14.3; leftover $115.41 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BBY` | 1 | $85.19 | $0.85 | — | $724.51 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-2.3; leftover $115.41 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BILI` | 7 | $16.22 | $1.16 | — | $609.81 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-2.5; leftover $115.41 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CMBT` | 6 | $17.91 | $1.09 | — | $501.26 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+3.9; leftover $115.41 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $501.26 | ▲ close $11,076.97 vs 09:30 $10,918.26 (session +163.70) | 16:00 close · cash $501.26 · equity $11,076.97 vs 09:30 $10,918.26 (+158.71; session marks +163.70) · 12 name(s) marked open→close (per-name table). ANF×12 09:30 $131.37 → close $147.75 +196.56; BBWI×71 09:30 $18.26 → close $18.90 +45.44; BOX×40 09:30 $34.30 → close $33.39 -36.40; DCI×14 09:30 $95.13 → close $95.24 +1.54; DY×3 09:30 $326.91 → close $310.91 -48.00; FSCO×267 09:30 $5.08 → close $5.12 +10.68; HEI×3 09:30 $370.00 → close $346.15 -71.55; INTU×3 09:30 $323.47 → close $345.88 +67.23; STDN×8 09:30 $13.95 → close $13.70 -2.00; BBY×1 09:30 $85.19 → close $87.44 +2.25; BILI×7 09:30 $16.22 → close $16.15 -0.49; CMBT×6 09:30 $17.91 → close $17.65 -1.56 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $501.26 | ▼ 09:30 equity $11,042.38 vs yday $11,076.97 (-34.59) | 09:30 open · cash $501.26 (unchanged overnight, no fees) · equity $11,042.38 vs prior close $11,076.97 (-34.59) · 12 name(s) re-marked at the open (per-name table). ANF×12 yday $147.75 → 09:30 $144.70 -36.60; BBWI×71 yday $18.90 → 09:30 $18.69 -14.91; BOX×40 yday $33.39 → 09:30 $33.79 +16.00; DCI×14 yday $95.24 → 09:30 $93.52 -24.08; DY×3 yday $310.91 → 09:30 $314.90 +11.97; FSCO×267 yday $5.12 → 09:30 $5.10 -5.34; HEI×3 yday $346.15 → 09:30 $346.19 +0.12; INTU×3 yday $345.88 → 09:30 $353.54 +22.98; STDN×8 yday $13.70 → 09:30 $13.84 +1.12; BBY×1 yday $87.44 → 09:30 $80.60 -6.84; BILI×7 yday $16.15 → 09:30 $16.18 +0.21; CMBT×6 yday $17.65 → 09:30 $17.78 +0.78 | — |
| 2026-08-27 09:30 ET | **BUY** | `GAP` | 3 | $20.75 | $0.63 | — | $438.38 | — | union ∩ overnight, no 🚨; gate overnight=True; list ohlc_hot,overnight; ret5=+5.2; leftover $62.66 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `BBAR` | 4 | $14.96 | $0.61 | — | $377.93 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+3.0; leftover $62.66 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CHA` | 5 | $10.54 | $0.54 | — | $324.68 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+2.5; leftover $62.66 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `HAFN` | 7 | $7.91 | $0.57 | — | $268.74 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-1.8; leftover $62.66 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MNSO` | 5 | $10.89 | $0.56 | — | $213.73 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+2.7; leftover $62.66 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $213.73 | ▼ close $11,026.00 vs 09:30 $11,042.38 (session -13.46) | 16:00 close · cash $213.73 · equity $11,026.00 vs 09:30 $11,042.38 (-16.38; session marks -13.46) · 17 name(s) marked open→close (per-name table). ANF×12 09:30 $144.70 → close $145.75 +12.60; BBWI×71 09:30 $18.69 → close $18.65 -2.84; BOX×40 09:30 $33.79 → close $34.74 +38.00; DCI×14 09:30 $93.52 → close $92.41 -15.54; DY×3 09:30 $314.90 → close $308.01 -20.67; FSCO×267 09:30 $5.10 → close $5.12 +5.34; HEI×3 09:30 $346.19 → close $337.01 -27.54; INTU×3 09:30 $353.54 → close $348.00 -16.62; STDN×8 09:30 $13.84 → close $14.31 +3.76; BBY×1 09:30 $80.60 → close $83.56 +2.96; BILI×7 09:30 $16.18 → close $16.77 +4.10; CMBT×6 09:30 $17.78 → close $18.28 +3.00; GAP×3 09:30 $20.75 → close $20.79 +0.12; BBAR×4 09:30 $14.96 → close $14.60 -1.44; CHA×5 09:30 $10.54 → close $10.35 -0.95; HAFN×7 09:30 $7.91 → close $8.29 +2.66; MNSO×5 09:30 $10.89 → close $10.81 -0.40 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $213.73 | ▲ 09:30 equity $11,054.81 vs yday $11,026.00 (+28.81) | 09:30 open · cash $213.73 (unchanged overnight, no fees) · equity $11,054.81 vs prior close $11,026.00 (+28.81) · 17 name(s) re-marked at the open (per-name table). ANF×12 yday $145.75 → 09:30 $146.07 +3.84; BBWI×71 yday $18.65 → 09:30 $18.75 +7.10; BOX×40 yday $34.74 → 09:30 $34.75 +0.40; DCI×14 yday $92.41 → 09:30 $92.25 -2.24; DY×3 yday $308.01 → 09:30 $306.34 -5.01; FSCO×267 yday $5.12 → 09:30 $5.12 +0.00; HEI×3 yday $337.01 → 09:30 $339.95 +8.82; INTU×3 yday $348.00 → 09:30 $347.82 -0.54; STDN×8 yday $14.31 → 09:30 $14.50 +1.52; BBY×1 yday $83.56 → 09:30 $83.85 +0.29; BILI×7 yday $16.77 → 09:30 $16.94 +1.23; CMBT×6 yday $18.28 → 09:30 $18.58 +1.80; GAP×3 yday $20.79 → 09:30 $24.69 +11.70; BBAR×4 yday $14.60 → 09:30 $15.01 +1.64; CHA×5 yday $10.35 → 09:30 $10.30 -0.25; HAFN×7 yday $8.29 → 09:30 $8.35 +0.42; MNSO×5 yday $10.81 → 09:30 $10.43 -1.90 | — |
| 2026-08-28 09:30 ET | **SELL** | `ANF` | 12 | $146.07 | $2.05 | $+402.72 | $1,964.52 | ▲ +402.72 after sell → book $11,052.76; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `BBWI` | 71 | $18.75 | $2.23 | $-33.54 | $3,293.54 | ▼ -33.54 after sell → book $11,050.53; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `BOX` | 40 | $34.75 | $2.13 | $+52.56 | $4,681.41 | ▲ +52.56 after sell → book $11,048.40; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DCI` | 14 | $92.25 | $2.05 | $-23.54 | $5,970.86 | ▼ -23.54 after sell → book $11,046.35; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DY` | 3 | $306.34 | $2.02 | $-255.66 | $6,887.86 | ▼ -255.66 after sell → book $11,044.33; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `FSCO` | 267 | $5.12 | $3.50 | $-1.60 | $8,251.40 | ▼ -1.60 after sell → book $11,040.83; vs 09:30 mark -3.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HEI` | 3 | $339.95 | $2.02 | $-55.62 | $9,269.23 | ▼ -55.62 after sell → book $11,038.81; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INTU` | 3 | $347.82 | $2.02 | $-53.61 | $10,310.67 | ▼ -53.61 after sell → book $11,036.79; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `LX` | 4444 | $1.16 | $57.33 | — | $5,098.31 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-13.8; leftover $5155.34 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SAIC` | 39 | $129.46 | $2.11 | — | $47.26 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+2.1; leftover $5155.34 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.26 | ▼ close $10,918.42 vs 09:30 $11,054.81 (session -58.94) | 16:00 close · cash $47.26 · equity $10,918.42 vs 09:30 $11,054.81 (-136.39; session marks -58.94) · 11 name(s) marked open→close (per-name table). STDN×8 09:30 $14.50 → close $14.10 -3.20; BBY×1 09:30 $83.85 → close $82.44 -1.41; BILI×7 09:30 $16.94 → close $16.60 -2.38; CMBT×6 09:30 $18.58 → close $18.35 -1.38; GAP×3 09:30 $24.69 → close $23.48 -3.63; BBAR×4 09:30 $15.01 → close $14.47 -2.16; CHA×5 09:30 $10.30 → close $10.80 +2.50; HAFN×7 09:30 $8.35 → close $8.47 +0.84; MNSO×5 09:30 $10.43 → close $10.33 -0.50; LX×4444 09:30 $1.16 → close $1.18 +88.88; SAIC×39 09:30 $129.46 → close $125.96 -136.50 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.26 | ▼ 09:30 equity $10,726.55 vs yday $10,918.42 (-191.87) | 09:30 open · cash $47.26 (unchanged overnight, no fees) · equity $10,726.55 vs prior close $10,918.42 (-191.87) · 11 name(s) re-marked at the open (per-name table). STDN×8 yday $14.10 → 09:30 $14.35 +2.00; BBY×1 yday $82.44 → 09:30 $81.94 -0.50; BILI×7 yday $16.60 → 09:30 $16.53 -0.49; CMBT×6 yday $18.35 → 09:30 $18.82 +2.82; GAP×3 yday $23.48 → 09:30 $22.98 -1.50; BBAR×4 yday $14.47 → 09:30 $14.88 +1.64; CHA×5 yday $10.80 → 09:30 $11.21 +2.05; HAFN×7 yday $8.47 → 09:30 $8.53 +0.42; MNSO×5 yday $10.33 → 09:30 $9.21 -5.60; LX×4444 yday $1.18 → 09:30 $1.01 -755.48; SAIC×39 yday $125.96 → 09:30 $140.39 +562.77 | — |
| 2026-08-31 09:30 ET | **SELL** | `STDN` | 8 | $14.35 | $1.19 | $+0.87 | $160.87 | ▲ +0.87 after sell → book $10,725.36; vs 09:30 mark -1.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBY` | 1 | $81.94 | $0.84 | $-4.95 | $241.97 | ▼ -4.95 after sell → book $10,724.52; vs 09:30 mark -0.84 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BILI` | 7 | $16.53 | $1.20 | $-0.18 | $356.48 | ▼ -0.18 after sell → book $10,723.32; vs 09:30 mark -1.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CMBT` | 6 | $18.82 | $1.17 | $+3.20 | $468.23 | ▲ +3.20 after sell → book $10,722.15; vs 09:30 mark -1.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $468.23 | ▼ close $10,515.11 vs 09:30 $10,726.55 (session -207.04) | 16:00 close · cash $468.23 · equity $10,515.11 vs 09:30 $10,726.55 (-211.44; session marks -207.04) · 7 name(s) marked open→close (per-name table). GAP×3 09:30 $22.98 → close $22.31 -2.01; BBAR×4 09:30 $14.88 → close $15.14 +1.04; CHA×5 09:30 $11.21 → close $11.63 +2.10; HAFN×7 09:30 $8.53 → close $8.44 -0.63; MNSO×5 09:30 $9.21 → close $9.30 +0.45; LX×4444 09:30 $1.01 → close $1.07 +266.64; SAIC×39 09:30 $140.39 → close $128.22 -474.63 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $468.23 | ▼ 09:30 equity $10,200.51 vs yday $10,515.11 (-314.60) | 09:30 open · cash $468.23 (unchanged overnight, no fees) · equity $10,200.51 vs prior close $10,515.11 (-314.60) · 7 name(s) re-marked at the open (per-name table). GAP×3 yday $22.31 → 09:30 $22.05 -0.78; BBAR×4 yday $15.14 → 09:30 $14.82 -1.28; CHA×5 yday $11.63 → 09:30 $11.63 +0.00; HAFN×7 yday $8.44 → 09:30 $8.56 +0.84; MNSO×5 yday $9.30 → 09:30 $9.39 +0.45; LX×4444 yday $1.07 → 09:30 $1.01 -266.64; SAIC×39 yday $128.22 → 09:30 $127.01 -47.19 | — |
| 2026-09-01 09:30 ET | **SELL** | `GAP` | 3 | $22.05 | $0.69 | $+2.58 | $533.69 | ▲ +2.58 after sell → book $10,199.82; vs 09:30 mark -0.69 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BBAR` | 4 | $14.82 | $0.62 | $-1.80 | $592.35 | ▼ -1.80 after sell → book $10,199.20; vs 09:30 mark -0.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CHA` | 5 | $11.63 | $0.62 | $+4.29 | $649.88 | ▲ +4.29 after sell → book $10,198.58; vs 09:30 mark -0.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `HAFN` | 7 | $8.56 | $0.64 | $+3.34 | $709.16 | ▲ +3.34 after sell → book $10,197.94; vs 09:30 mark -0.64 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `MNSO` | 5 | $9.39 | $0.50 | $-8.56 | $755.60 | ▼ -8.56 after sell → book $10,197.43; vs 09:30 mark -0.51 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $755.60 | ▼ close $9,619.63 vs 09:30 $10,200.51 (session -577.80) | 16:00 close · cash $755.60 · equity $9,619.63 vs 09:30 $10,200.51 (-580.88; session marks -577.80) · 2 name(s) marked open→close (per-name table). LX×4444 09:30 $1.01 → close $0.88 -568.83; SAIC×39 09:30 $127.01 → close $126.78 -8.97 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $755.60 | ▲ 09:30 equity $9,699.77 vs yday $9,619.63 (+80.14) | 09:30 open · cash $755.60 (unchanged overnight, no fees) · equity $9,699.77 vs prior close $9,619.63 (+80.14) · 2 name(s) re-marked at the open (per-name table). LX×4444 yday $0.88 → 09:30 $0.91 +106.66; SAIC×39 yday $126.78 → 09:30 $126.10 -26.52 | — |
| 2026-09-02 09:30 ET | **SELL** | `LX` | 4444 | $0.91 | $54.36 | $-1240.47 | $4,727.50 | ▼ -1,240.47 after sell → book $9,645.40; vs 09:30 mark -54.37 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SAIC` | 39 | $126.10 | $2.16 | $-135.30 | $9,643.25 | ▼ -135.30 after sell → book $9,643.25; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,643.25 | ▲ close $9,643.25 vs 09:30 $9,699.77 (session +0.00) | 16:00 close · cash $9,643.25 · no lots left · equity $9,643.25. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,643.25 | ▲ 09:30 equity $9,643.25 vs yday $9,643.25 (-0.00) | 09:30 open · cash $9,643.25 · no holdings · equity $9,643.25 vs prior close $9,643.25 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AMBA` | 18 | $66.61 | $2.04 | — | $8,442.22 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-3.6; leftover $1205.41 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ASAN` | 118 | $10.16 | $2.34 | — | $7,241.00 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+3.8; leftover $1205.41 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟡 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DOCU` | 17 | $67.06 | $2.04 | — | $6,098.94 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+10.2; leftover $1205.41 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DOMO` | 318 | $3.78 | $4.10 | — | $4,892.80 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-2.4; leftover $1205.41 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GWRE` | 6 | $198.00 | $2.01 | — | $3,702.79 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+0.9; leftover $1205.41 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `IOT` | 31 | $37.69 | $2.08 | — | $2,532.31 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-7.7; leftover $1205.41 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `LULU` | 9 | $121.15 | $2.02 | — | $1,439.95 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+3.2; leftover $1205.41 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MAMA` | 77 | $15.62 | $2.22 | — | $234.99 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-6.7; leftover $1205.41 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $234.99 | ▲ close $9,636.42 vs 09:30 $9,643.25 (session +12.03) | 16:00 close · cash $234.99 · equity $9,636.42 vs 09:30 $9,643.25 (-6.83; session marks +12.03) · 8 name(s) marked open→close (per-name table). AMBA×18 09:30 $66.61 → close $63.38 -58.14; ASAN×118 09:30 $10.16 → close $10.09 -8.26; DOCU×17 09:30 $67.06 → close $65.97 -18.53; DOMO×318 09:30 $3.78 → close $3.79 +3.18; GWRE×6 09:30 $198.00 → close $202.86 +29.16; IOT×31 09:30 $37.69 → close $38.75 +32.86; LULU×9 09:30 $121.15 → close $121.77 +5.58; MAMA×77 09:30 $15.62 → close $15.96 +26.18 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $234.99 | ▼ 09:30 equity $9,207.41 vs yday $9,636.42 (-429.01) | 09:30 open · cash $234.99 (unchanged overnight, no fees) · equity $9,207.41 vs prior close $9,636.42 (-429.01) · 8 name(s) re-marked at the open (per-name table). AMBA×18 yday $63.38 → 09:30 $63.18 -3.60; ASAN×118 yday $10.09 → 09:30 $8.74 -159.30; DOCU×17 yday $65.97 → 09:30 $68.52 +43.35; DOMO×318 yday $3.79 → 09:30 $3.62 -55.65; GWRE×6 yday $202.86 → 09:30 $167.55 -211.86; IOT×31 yday $38.75 → 09:30 $44.90 +190.65; LULU×9 yday $121.77 → 09:30 $98.15 -212.58; MAMA×77 yday $15.96 → 09:30 $15.70 -20.02 | — |
| 2026-09-04 09:30 ET | **BUY** | `ABM` | 2 | $46.79 | $0.94 | — | $140.47 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+0.2; leftover $117.49 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `UNFI` | 2 | $43.80 | $0.88 | — | $51.98 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-7.7; leftover $117.49 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.98 | ▼ close $9,095.88 vs 09:30 $9,207.41 (session -109.70) | 16:00 close · cash $51.98 · equity $9,095.88 vs 09:30 $9,207.41 (-111.53; session marks -109.70) · 10 name(s) marked open→close (per-name table). AMBA×18 09:30 $63.18 → close $62.89 -5.22; ASAN×118 09:30 $8.74 → close $8.81 +8.26; DOCU×17 09:30 $68.52 → close $68.41 -1.87; DOMO×318 09:30 $3.62 → close $3.88 +84.27; GWRE×6 09:30 $167.55 → close $162.42 -30.78; IOT×31 09:30 $44.90 → close $40.20 -145.70; LULU×9 09:30 $98.15 → close $100.61 +22.14; MAMA×77 09:30 $15.70 → close $15.16 -41.58; ABM×2 09:30 $46.79 → close $47.05 +0.52; UNFI×2 09:30 $43.80 → close $43.93 +0.26 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.98 | ▼ 09:30 equity $9,039.17 vs yday $9,095.88 (-56.71) | 09:30 open · cash $51.98 (unchanged overnight, no fees) · equity $9,039.17 vs prior close $9,095.88 (-56.71) · 10 name(s) re-marked at the open (per-name table). AMBA×18 yday $62.89 → 09:30 $63.83 +16.92; ASAN×118 yday $8.81 → 09:30 $8.73 -9.44; DOCU×17 yday $68.41 → 09:30 $67.05 -23.12; DOMO×318 yday $3.88 → 09:30 $3.84 -12.72; GWRE×6 yday $162.42 → 09:30 $160.52 -11.40; IOT×31 yday $40.20 → 09:30 $39.56 -19.84; LULU×9 yday $100.61 → 09:30 $100.58 -0.27; MAMA×77 yday $15.16 → 09:30 $15.20 +3.08; ABM×2 yday $47.05 → 09:30 $45.81 -2.48; UNFI×2 yday $43.93 → 09:30 $45.21 +2.56 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.98 | ▼ close $9,012.32 vs 09:30 $9,039.17 (session -26.85) | 16:00 close · cash $51.98 · equity $9,012.32 vs 09:30 $9,039.17 (-26.85; session marks -26.85) · 10 name(s) marked open→close (per-name table). AMBA×18 09:30 $63.83 → close $63.48 -6.30; ASAN×118 09:30 $8.73 → close $8.79 +7.08; DOCU×17 09:30 $67.05 → close $65.08 -33.49; DOMO×318 09:30 $3.84 → close $3.83 -3.18; GWRE×6 09:30 $160.52 → close $149.71 -64.86; IOT×31 09:30 $39.56 → close $40.15 +18.29; LULU×9 09:30 $100.58 → close $103.19 +23.49; MAMA×77 09:30 $15.20 → close $15.50 +23.10; ABM×2 09:30 $45.81 → close $50.60 +9.58; UNFI×2 09:30 $45.21 → close $44.93 -0.56 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.98 | ▼ 09:30 equity $8,935.99 vs yday $9,012.32 (-76.33) | 09:30 open · cash $51.98 (unchanged overnight, no fees) · equity $8,935.99 vs prior close $9,012.32 (-76.33) · 10 name(s) re-marked at the open (per-name table). AMBA×18 yday $63.48 → 09:30 $63.07 -7.38; ASAN×118 yday $8.79 → 09:30 $8.64 -17.70; DOCU×17 yday $65.08 → 09:30 $64.64 -7.48; DOMO×318 yday $3.83 → 09:30 $3.86 +9.54; GWRE×6 yday $149.71 → 09:30 $147.85 -11.16; IOT×31 yday $40.15 → 09:30 $39.60 -17.05; LULU×9 yday $103.19 → 09:30 $101.90 -11.61; MAMA×77 yday $15.50 → 09:30 $15.31 -14.63; ABM×2 yday $50.60 → 09:30 $50.60 +0.00; UNFI×2 yday $44.93 → 09:30 $45.50 +1.14 | — |
| 2026-09-09 09:30 ET | **SELL** | `AMBA` | 18 | $63.07 | $2.06 | $-67.83 | $1,185.18 | ▼ -67.83 after sell → book $8,933.93; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ASAN` | 118 | $8.64 | $2.37 | $-184.08 | $2,202.33 | ▼ -184.08 after sell → book $8,931.56; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DOCU` | 17 | $64.64 | $2.06 | $-45.24 | $3,299.14 | ▼ -45.24 after sell → book $8,929.49; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DOMO` | 318 | $3.86 | $4.17 | $+17.17 | $4,522.46 | ▲ +17.17 after sell → book $8,925.33; vs 09:30 mark -4.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `GWRE` | 6 | $147.85 | $2.03 | $-304.94 | $5,407.53 | ▼ -304.94 after sell → book $8,923.30; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `IOT` | 31 | $39.60 | $2.10 | $+55.02 | $6,633.03 | ▲ +55.02 after sell → book $8,921.20; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `LULU` | 9 | $101.90 | $2.04 | $-177.30 | $7,548.09 | ▼ -177.30 after sell → book $8,919.16; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MAMA` | 77 | $15.31 | $2.24 | $-28.33 | $8,724.72 | ▼ -28.33 after sell → book $8,916.92; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,724.72 | ▼ close $8,913.42 vs 09:30 $8,935.99 (session -3.50) | 16:00 close · cash $8,724.72 · equity $8,913.42 vs 09:30 $8,935.99 (-22.57; session marks -3.50) · 2 name(s) marked open→close (per-name table). ABM×2 09:30 $50.60 → close $49.74 -1.72; UNFI×2 09:30 $45.50 → close $44.61 -1.78 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,724.72 | ▲ 09:30 equity $8,913.98 vs yday $8,913.42 (+0.56) | 09:30 open · cash $8,724.72 (unchanged overnight, no fees) · equity $8,913.98 vs prior close $8,913.42 (+0.56) · 2 name(s) re-marked at the open (per-name table). ABM×2 yday $49.74 → 09:30 $49.74 +0.00; UNFI×2 yday $44.61 → 09:30 $44.89 +0.56 | — |
| 2026-09-10 09:30 ET | **SELL** | `ABM` | 2 | $49.74 | $1.02 | $+3.94 | $8,823.18 | ▲ +3.94 after sell → book $8,912.96; vs 09:30 mark -1.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `UNFI` | 2 | $44.89 | $0.92 | $+0.37 | $8,912.03 | ▲ +0.37 after sell → book $8,912.03; vs 09:30 mark -0.93 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,912.03 | ▲ close $8,912.03 vs 09:30 $8,913.98 (session +0.00) | 16:00 close · cash $8,912.03 · no lots left · equity $8,912.03. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,912.03 | ▲ 09:30 equity $8,912.03 vs yday $8,912.03 (+0.00) | 09:30 open · cash $8,912.03 · no holdings · equity $8,912.03 vs prior close $8,912.03 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,912.03 | ▲ close $8,912.03 vs 09:30 $8,912.03 (session +0.00) | 16:00 close · cash $8,912.03 · no lots left · equity $8,912.03. | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,912.03 | ▲ 09:30 equity $8,912.03 vs yday $8,912.03 (+0.00) | 09:30 open · cash $8,912.03 · no holdings · equity $8,912.03 vs prior close $8,912.03 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,912.03 | ▲ close $8,912.03 vs 09:30 $8,912.03 (session +0.00) | 16:00 close · cash $8,912.03 · no lots left · equity $8,912.03. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,912.03 | ▲ 09:30 equity $8,912.03 vs yday $8,912.03 (+0.00) | 09:30 open · cash $8,912.03 · no holdings · equity $8,912.03 vs prior close $8,912.03 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,912.03 | ▲ close $8,912.03 vs 09:30 $8,912.03 (session +0.00) | 16:00 close · cash $8,912.03 · no lots left · equity $8,912.03. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,912.03 | ▲ 09:30 equity $8,912.03 vs yday $8,912.03 (+0.00) | 09:30 open · cash $8,912.03 · no holdings · equity $8,912.03 vs prior close $8,912.03 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `ALMU` | 324 | $13.75 | $4.18 | — | $4,452.85 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-1.4; leftover $4456.02 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `LEN` | 55 | $80.63 | $2.15 | — | $16.05 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-0.4; leftover $4456.02 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🔴 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.05 | ▼ close $8,677.17 vs 09:30 $8,912.03 (session -228.53) | 16:00 close · cash $16.05 · equity $8,677.17 vs 09:30 $8,912.03 (-234.86; session marks -228.53) · 2 name(s) marked open→close (per-name table). ALMU×324 09:30 $13.75 → close $13.43 -103.68; LEN×55 09:30 $80.63 → close $78.36 -124.85 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.05 | ▼ 09:30 equity $8,103.09 vs yday $8,677.17 (-574.08) | 09:30 open · cash $16.05 (unchanged overnight, no fees) · equity $8,103.09 vs prior close $8,677.17 (-574.08) · 2 name(s) re-marked at the open (per-name table). ALMU×324 yday $13.43 → 09:30 $11.21 -719.28; LEN×55 yday $78.36 → 09:30 $81.00 +145.20 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.05 | ▲ close $8,140.13 vs 09:30 $8,103.09 (session +37.04) | 16:00 close · cash $16.05 · equity $8,140.13 vs 09:30 $8,103.09 (+37.04; session marks +37.04) · 2 name(s) marked open→close (per-name table). ALMU×324 09:30 $11.21 → close $11.54 +108.54; LEN×55 09:30 $81.00 → close $79.70 -71.50 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.05 | ▼ 09:30 equity $8,091.16 vs yday $8,140.13 (-48.97) | 09:30 open · cash $16.05 (unchanged overnight, no fees) · equity $8,091.16 vs prior close $8,140.13 (-48.97) · 2 name(s) re-marked at the open (per-name table). ALMU×324 yday $11.54 → 09:30 $11.64 +30.78; LEN×55 yday $79.70 → 09:30 $78.25 -79.75 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.05 | ▲ close $8,342.60 vs 09:30 $8,091.16 (session +251.44) | 16:00 close · cash $16.05 · equity $8,342.60 vs 09:30 $8,091.16 (+251.44; session marks +251.44) · 2 name(s) marked open→close (per-name table). ALMU×324 09:30 $11.64 → close $12.72 +351.54; LEN×55 09:30 $78.25 → close $76.43 -100.10 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.05 | ▲ 09:30 equity $8,502.45 vs yday $8,342.60 (+159.85) | 09:30 open · cash $16.05 (unchanged overnight, no fees) · equity $8,502.45 vs prior close $8,342.60 (+159.85) · 2 name(s) re-marked at the open (per-name table). ALMU×324 yday $12.72 → 09:30 $13.12 +129.60; LEN×55 yday $76.43 → 09:30 $76.98 +30.25 | — |
| 2026-09-21 09:30 ET | **SELL** | `ALMU` | 324 | $13.12 | $4.27 | $-210.95 | $4,264.28 | ▼ -210.95 after sell → book $8,498.18; vs 09:30 mark -4.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `LEN` | 55 | $76.98 | $2.20 | $-205.10 | $8,495.98 | ▼ -205.10 after sell → book $8,495.98; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `ABVX` | 80 | $105.72 | $2.23 | — | $36.15 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+6.5; leftover $8495.98 | join🟡 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.15 | ▼ close $8,312.15 vs 09:30 $8,502.45 (session -181.60) | 16:00 close · cash $36.15 · equity $8,312.15 vs 09:30 $8,502.45 (-190.30; session marks -181.60) · 1 name(s) marked open→close (per-name table). ABVX×80 09:30 $105.72 → close $103.45 -181.60 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `DUOT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NUAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `SIDU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HSAI` | cash | leftover split 0.34 < 1 share @ 18.32 |
| 2026-08-17 | `IQ` | cash | leftover split 0.34 < 1 share @ 1.35 |
| 2026-08-17 | `KLAR` | cash | leftover split 0.34 < 1 share @ 20.67 |
| 2026-08-17 | `PONY` | cash | leftover split 0.34 < 1 share @ 8.16 |
| 2026-08-17 | `VNET` | cash | leftover split 0.34 < 1 share @ 7.75 |
| 2026-08-17 | `XP` | cash | leftover split 0.34 < 1 share @ 15.93 |
| 2026-08-18 | `DUOT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NUAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `SIDU` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-08-21 | `BEKE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BJ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BKE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `FLO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ROST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `PDD` | cash | leftover split 76.47 < 1 share @ 90.03 |
| 2026-08-24 | `BEKE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BJ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BKE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `FLO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ROST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `XPEV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BNS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BZ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SHMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SLQT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TUYA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `VIPS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `XPEV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BBWI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BOX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DCI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FSCO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `HEI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `INTU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `A` | cash | leftover split 115.41 < 1 share @ 152.45 |
| 2026-08-26 | `CM` | cash | leftover split 115.41 < 1 share @ 118.50 |
| 2026-08-26 | `CRM` | cash | leftover split 115.41 < 1 share @ 199.94 |
| 2026-08-26 | `CRWD` | cash | leftover split 115.41 < 1 share @ 182.75 |
| 2026-08-27 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BBWI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BOX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DCI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FSCO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HEI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INTU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `STDN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BBY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BILI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `CMBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ADSK` | cash | leftover split 62.66 < 1 share @ 261.47 |
| 2026-08-27 | `AFRM` | cash | leftover split 62.66 < 1 share @ 76.90 |
| 2026-08-27 | `ESTC` | cash | leftover split 62.66 < 1 share @ 82.65 |
| 2026-08-28 | `STDN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `BBY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `BILI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `CMBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `GAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `BBAR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `CHA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `HAFN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `MNSO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `BBAR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CHA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `HAFN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MNSO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `LX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SAIC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `YEXT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MDT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MMED` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NIO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RZLV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SSL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `LX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SAIC` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-04 | `AMBA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ASAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DOCU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DOMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `GWRE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `IOT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `LULU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MAMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `AMBA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ASAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DOCU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DOMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `GWRE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `IOT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `LULU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MAMA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ABM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `UNFI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BRZE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `KFY` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ODD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUNB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ABM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `UNFI` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-17 | `ALMU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `LEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `ALMU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `LEN` | min_hold | dropped but min-hold 2/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ABVX` | 80 | 2026-09-21 @ $105.72 | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+6.5; leftover $8495.98 |
