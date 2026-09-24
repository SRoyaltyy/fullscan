# Factor mine action — `overnight_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `overnight` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-25.14%** ($7,486) · signal-only (no cash/fees) was -22.61%. Starts YES **0/30**. Fills 132 · skips 80 · realized $-2514.32.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at names the prior Finviz calendar said report AMC today or BMO next session (print not in yet) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: names the prior Finviz calendar said report AMC today or BMO next session (print not in yet).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).

### When it buys

- At 09:30, take names on names the prior Finviz calendar said report AMC today or BMO next session (print not in yet) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
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

- **Universe** `overnight` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7,485.66.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `DUOT` | 265 | — | $9.43 | +0.00 | $9.11 | -84.80 | -84.80 | +0.00 | -84.80 |
| 2026-08-14 | `HTHT` | 61 | — | $40.88 | +0.00 | $41.88 | +61.00 | +61.00 | +0.00 | +61.00 |
| 2026-08-14 | `NUAI` | 494 | — | $5.06 | +0.00 | $5.07 | +4.94 | +4.94 | +0.00 | +4.94 |
| 2026-08-14 | `SIDU` | 973 | — | $2.55 | +0.00 | $2.60 | +48.65 | +48.65 | +0.00 | +48.65 |
| 2026-08-17 | `DUOT` | 265 | $9.11 | $10.35 | +328.60 | — | +0.00 | +328.60 | +243.80 | — |
| 2026-08-17 | `HTHT` | 61 | $41.88 | $45.49 | +220.21 | — | +0.00 | +220.21 | +281.21 | — |
| 2026-08-17 | `NUAI` | 494 | $5.07 | $5.20 | +64.22 | — | +0.00 | +64.22 | +69.16 | — |
| 2026-08-17 | `SIDU` | 973 | $2.60 | $2.40 | -194.60 | — | +0.00 | -194.60 | -145.95 | — |
| 2026-08-17 | `AS` | 39 | — | $32.88 | +0.00 | $32.57 | -12.09 | -12.09 | +0.00 | -12.09 |
| 2026-08-17 | `BIDU` | 12 | — | $102.83 | +0.00 | $104.12 | +15.48 | +15.48 | +0.00 | +15.48 |
| 2026-08-17 | `FN` | 2 | — | $583.15 | +0.00 | $598.58 | +30.86 | +30.86 | +0.00 | +30.86 |
| 2026-08-17 | `HD` | 3 | — | $334.71 | +0.00 | $337.88 | +9.51 | +9.51 | +0.00 | +9.51 |
| 2026-08-17 | `HSAI` | 70 | — | $18.32 | +0.00 | $18.07 | -17.50 | -17.50 | +0.00 | -17.50 |
| 2026-08-17 | `IQ` | 962 | — | $1.35 | +0.00 | $1.33 | -19.24 | -19.24 | +0.00 | -19.24 |
| 2026-08-17 | `KLAR` | 62 | — | $20.67 | +0.00 | $19.51 | -71.92 | -71.92 | +0.00 | -71.92 |
| 2026-08-17 | `PONY` | 159 | — | $8.16 | +0.00 | $7.98 | -28.62 | -28.62 | +0.00 | -28.62 |
| 2026-08-18 | `AS` | 39 | $32.57 | $33.89 | +51.48 | — | +0.00 | +51.48 | +39.39 | — |
| 2026-08-18 | `BIDU` | 12 | $104.12 | $94.35 | -117.24 | — | +0.00 | -117.24 | -101.76 | — |
| 2026-08-18 | `FN` | 2 | $598.58 | $513.70 | -169.76 | — | +0.00 | -169.76 | -138.90 | — |
| 2026-08-18 | `HD` | 3 | $337.88 | $336.78 | -3.30 | — | +0.00 | -3.30 | +6.21 | — |
| 2026-08-18 | `HSAI` | 70 | $18.07 | $15.77 | -161.35 | — | +0.00 | -161.35 | -178.85 | — |
| 2026-08-18 | `IQ` | 962 | $1.33 | $1.27 | -57.72 | — | +0.00 | -57.72 | -76.96 | — |
| 2026-08-18 | `KLAR` | 62 | $19.51 | $15.66 | -238.70 | — | +0.00 | -238.70 | -310.62 | — |
| 2026-08-18 | `PONY` | 159 | $7.98 | $7.53 | -71.55 | — | +0.00 | -71.55 | -100.17 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BEKE` | 111 | — | $17.04 | +0.00 | $16.99 | -5.55 | -5.55 | +0.00 | -5.55 |
| 2026-08-20 | `BJ` | 21 | — | $88.91 | +0.00 | $91.30 | +50.19 | +50.19 | +0.00 | +50.19 |
| 2026-08-20 | `BKE` | 44 | — | $42.60 | +0.00 | $42.64 | +1.76 | +1.76 | +0.00 | +1.76 |
| 2026-08-20 | `FLO` | 255 | — | $7.43 | +0.00 | $7.10 | -84.15 | -84.15 | +0.00 | -84.15 |
| 2026-08-20 | `ROST` | 8 | — | $229.55 | +0.00 | $228.99 | -4.48 | -4.48 | +0.00 | -4.48 |
| 2026-08-21 | `BEKE` | 111 | $16.99 | $17.93 | +104.90 | — | +0.00 | +104.90 | +99.34 | — |
| 2026-08-21 | `BJ` | 21 | $91.30 | $93.98 | +56.28 | — | +0.00 | +56.28 | +106.47 | — |
| 2026-08-21 | `BKE` | 44 | $42.64 | $43.08 | +19.36 | — | +0.00 | +19.36 | +21.12 | — |
| 2026-08-21 | `FLO` | 255 | $7.10 | $6.90 | -51.00 | — | +0.00 | -51.00 | -135.15 | — |
| 2026-08-21 | `ROST` | 8 | $228.99 | $243.85 | +118.88 | — | +0.00 | +118.88 | +114.40 | — |
| 2026-08-21 | `PDD` | 53 | — | $90.03 | +0.00 | $88.38 | -87.45 | -87.45 | +0.00 | -87.45 |
| 2026-08-21 | `XPEV` | 393 | — | $12.29 | +0.00 | $12.19 | -39.30 | -39.30 | +0.00 | -39.30 |
| 2026-08-24 | `PDD` | 53 | $88.38 | $90.95 | +136.21 | — | +0.00 | +136.21 | +48.76 | — |
| 2026-08-24 | `XPEV` | 393 | $12.19 | $11.83 | -141.48 | — | +0.00 | -141.48 | -180.78 | — |
| 2026-08-25 | `ANF` | 10 | — | $112.17 | +0.00 | $108.90 | -32.70 | -32.70 | +0.00 | -32.70 |
| 2026-08-25 | `BBWI` | 62 | — | $19.16 | +0.00 | $17.58 | -97.96 | -97.96 | +0.00 | -97.96 |
| 2026-08-25 | `BOX` | 35 | — | $33.33 | +0.00 | $33.00 | -11.55 | -11.55 | +0.00 | -11.55 |
| 2026-08-25 | `DCI` | 12 | — | $93.64 | +0.00 | $93.26 | -4.56 | -4.56 | +0.00 | -4.56 |
| 2026-08-25 | `DY` | 3 | — | $390.22 | +0.00 | $351.80 | -115.26 | -115.26 | +0.00 | -115.26 |
| 2026-08-25 | `FSCO` | 233 | — | $5.10 | +0.00 | $5.07 | -6.99 | -6.99 | +0.00 | -6.99 |
| 2026-08-25 | `HEI` | 3 | — | $357.15 | +0.00 | $351.05 | -18.30 | -18.30 | +0.00 | -18.30 |
| 2026-08-25 | `INTU` | 3 | — | $364.35 | +0.00 | $357.46 | -20.67 | -20.67 | +0.00 | -20.67 |
| 2026-08-26 | `ANF` | 10 | $108.90 | $131.37 | +224.70 | — | +0.00 | +224.70 | +192.00 | — |
| 2026-08-26 | `BBWI` | 62 | $17.58 | $18.26 | +42.16 | — | +0.00 | +42.16 | -55.80 | — |
| 2026-08-26 | `BOX` | 35 | $33.00 | $34.30 | +45.50 | — | +0.00 | +45.50 | +33.95 | — |
| 2026-08-26 | `DCI` | 12 | $93.26 | $95.13 | +22.44 | — | +0.00 | +22.44 | +17.88 | — |
| 2026-08-26 | `DY` | 3 | $351.80 | $326.91 | -74.67 | — | +0.00 | -74.67 | -189.93 | — |
| 2026-08-26 | `FSCO` | 233 | $5.07 | $5.08 | +2.33 | — | +0.00 | +2.33 | -4.66 | — |
| 2026-08-26 | `HEI` | 3 | $351.05 | $370.00 | +56.85 | — | +0.00 | +56.85 | +38.55 | — |
| 2026-08-26 | `INTU` | 3 | $357.46 | $323.47 | -101.97 | — | +0.00 | -101.97 | -122.64 | — |
| 2026-08-26 | `STDN` | 84 | — | $13.95 | +0.00 | $13.70 | -21.00 | -21.00 | +0.00 | -21.00 |
| 2026-08-26 | `A` | 7 | — | $152.45 | +0.00 | $155.08 | +18.41 | +18.41 | +0.00 | +18.41 |
| 2026-08-26 | `BBY` | 13 | — | $85.19 | +0.00 | $87.44 | +29.25 | +29.25 | +0.00 | +29.25 |
| 2026-08-26 | `BILI` | 72 | — | $16.22 | +0.00 | $16.15 | -5.04 | -5.04 | +0.00 | -5.04 |
| 2026-08-26 | `CM` | 9 | — | $118.50 | +0.00 | $118.20 | -2.70 | -2.70 | +0.00 | -2.70 |
| 2026-08-26 | `CMBT` | 65 | — | $17.91 | +0.00 | $17.65 | -16.90 | -16.90 | +0.00 | -16.90 |
| 2026-08-26 | `CRM` | 5 | — | $199.94 | +0.00 | $205.62 | +28.40 | +28.40 | +0.00 | +28.40 |
| 2026-08-26 | `CRWD` | 6 | — | $182.75 | +0.00 | $189.18 | +38.58 | +38.58 | +0.00 | +38.58 |
| 2026-08-27 | `STDN` | 84 | $13.70 | $13.84 | +11.76 | — | +0.00 | +11.76 | -9.24 | — |
| 2026-08-27 | `A` | 7 | $155.08 | $159.35 | +29.89 | — | +0.00 | +29.89 | +48.30 | — |
| 2026-08-27 | `BBY` | 13 | $87.44 | $80.60 | -88.92 | — | +0.00 | -88.92 | -59.67 | — |
| 2026-08-27 | `BILI` | 72 | $16.15 | $16.18 | +2.16 | — | +0.00 | +2.16 | -2.88 | — |
| 2026-08-27 | `CM` | 9 | $118.20 | $118.77 | +5.13 | — | +0.00 | +5.13 | +2.43 | — |
| 2026-08-27 | `CMBT` | 65 | $17.65 | $17.78 | +8.45 | — | +0.00 | +8.45 | -8.45 | — |
| 2026-08-27 | `CRM` | 5 | $205.62 | $230.05 | +122.15 | — | +0.00 | +122.15 | +150.55 | — |
| 2026-08-27 | `CRWD` | 6 | $189.18 | $208.25 | +114.42 | — | +0.00 | +114.42 | +153.00 | — |
| 2026-08-27 | `GAP` | 58 | — | $20.75 | +0.00 | $20.79 | +2.32 | +2.32 | +0.00 | +2.32 |
| 2026-08-27 | `ADSK` | 4 | — | $261.47 | +0.00 | $270.58 | +36.44 | +36.44 | +0.00 | +36.44 |
| 2026-08-27 | `AFRM` | 15 | — | $76.90 | +0.00 | $77.49 | +8.85 | +8.85 | +0.00 | +8.85 |
| 2026-08-27 | `BBAR` | 80 | — | $14.96 | +0.00 | $14.60 | -28.80 | -28.80 | +0.00 | -28.80 |
| 2026-08-27 | `CHA` | 114 | — | $10.54 | +0.00 | $10.35 | -21.66 | -21.66 | +0.00 | -21.66 |
| 2026-08-27 | `ESTC` | 14 | — | $82.65 | +0.00 | $83.74 | +15.26 | +15.26 | +0.00 | +15.26 |
| 2026-08-27 | `HAFN` | 152 | — | $7.91 | +0.00 | $8.29 | +57.76 | +57.76 | +0.00 | +57.76 |
| 2026-08-27 | `IREN` | 29 | — | $40.65 | +0.00 | $40.53 | -3.48 | -3.48 | +0.00 | -3.48 |
| 2026-08-28 | `GAP` | 58 | $20.79 | $24.69 | +226.20 | — | +0.00 | +226.20 | +228.52 | — |
| 2026-08-28 | `ADSK` | 4 | $270.58 | $261.16 | -37.68 | — | +0.00 | -37.68 | -1.24 | — |
| 2026-08-28 | `AFRM` | 15 | $77.49 | $86.00 | +127.65 | — | +0.00 | +127.65 | +136.50 | — |
| 2026-08-28 | `BBAR` | 80 | $14.60 | $15.01 | +32.80 | — | +0.00 | +32.80 | +4.00 | — |
| 2026-08-28 | `CHA` | 114 | $10.35 | $10.30 | -5.70 | — | +0.00 | -5.70 | -27.36 | — |
| 2026-08-28 | `ESTC` | 14 | $83.74 | $103.89 | +282.10 | — | +0.00 | +282.10 | +297.36 | — |
| 2026-08-28 | `HAFN` | 152 | $8.29 | $8.35 | +9.12 | — | +0.00 | +9.12 | +66.88 | — |
| 2026-08-28 | `IREN` | 29 | $40.53 | $37.65 | -83.66 | — | +0.00 | -83.66 | -87.14 | — |
| 2026-08-28 | `LX` | 4403 | — | $1.16 | +0.00 | $1.18 | +88.06 | +88.06 | +0.00 | +88.06 |
| 2026-08-28 | `SAIC` | 39 | — | $129.46 | +0.00 | $125.96 | -136.50 | -136.50 | +0.00 | -136.50 |
| 2026-08-31 | `LX` | 4403 | $1.18 | $1.01 | -748.51 | — | +0.00 | -748.51 | -660.45 | — |
| 2026-08-31 | `SAIC` | 39 | $125.96 | $140.39 | +562.77 | — | +0.00 | +562.77 | +426.27 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AMBA` | 18 | — | $66.61 | +0.00 | $63.38 | -58.14 | -58.14 | +0.00 | -58.14 |
| 2026-09-03 | `ASAN` | 121 | — | $10.16 | +0.00 | $10.09 | -8.47 | -8.47 | +0.00 | -8.47 |
| 2026-09-03 | `DOCU` | 18 | — | $67.06 | +0.00 | $65.97 | -19.62 | -19.62 | +0.00 | -19.62 |
| 2026-09-03 | `DOMO` | 326 | — | $3.78 | +0.00 | $3.79 | +3.26 | +3.26 | +0.00 | +3.26 |
| 2026-09-03 | `GWRE` | 6 | — | $198.00 | +0.00 | $202.86 | +29.16 | +29.16 | +0.00 | +29.16 |
| 2026-09-03 | `IOT` | 32 | — | $37.69 | +0.00 | $38.75 | +33.92 | +33.92 | +0.00 | +33.92 |
| 2026-09-03 | `LULU` | 10 | — | $121.15 | +0.00 | $121.77 | +6.20 | +6.20 | +0.00 | +6.20 |
| 2026-09-03 | `MAMA` | 78 | — | $15.62 | +0.00 | $15.96 | +26.52 | +26.52 | +0.00 | +26.52 |
| 2026-09-04 | `AMBA` | 18 | $63.38 | $63.18 | -3.60 | — | +0.00 | -3.60 | -61.74 | — |
| 2026-09-04 | `ASAN` | 121 | $10.09 | $8.74 | -163.35 | — | +0.00 | -163.35 | -171.82 | — |
| 2026-09-04 | `DOCU` | 18 | $65.97 | $68.52 | +45.90 | — | +0.00 | +45.90 | +26.28 | — |
| 2026-09-04 | `DOMO` | 326 | $3.79 | $3.62 | -57.05 | — | +0.00 | -57.05 | -53.79 | — |
| 2026-09-04 | `GWRE` | 6 | $202.86 | $167.55 | -211.86 | — | +0.00 | -211.86 | -182.70 | — |
| 2026-09-04 | `IOT` | 32 | $38.75 | $44.90 | +196.80 | — | +0.00 | +196.80 | +230.72 | — |
| 2026-09-04 | `LULU` | 10 | $121.77 | $98.15 | -236.20 | — | +0.00 | -236.20 | -230.00 | — |
| 2026-09-04 | `MAMA` | 78 | $15.96 | $15.70 | -20.28 | — | +0.00 | -20.28 | +6.24 | — |
| 2026-09-04 | `ABM` | 100 | — | $46.79 | +0.00 | $47.05 | +26.00 | +26.00 | +0.00 | +26.00 |
| 2026-09-04 | `UNFI` | 107 | — | $43.80 | +0.00 | $43.93 | +13.91 | +13.91 | +0.00 | +13.91 |
| 2026-09-08 | `ABM` | 100 | $47.05 | $45.81 | -124.00 | — | +0.00 | -124.00 | -98.00 | — |
| 2026-09-08 | `UNFI` | 107 | $43.93 | $45.21 | +136.96 | — | +0.00 | +136.96 | +150.87 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `ALMU` | 342 | — | $13.75 | +0.00 | $13.43 | -109.44 | -109.44 | +0.00 | -109.44 |
| 2026-09-16 | `LEN` | 58 | — | $80.63 | +0.00 | $78.36 | -131.66 | -131.66 | +0.00 | -131.66 |
| 2026-09-17 | `ALMU` | 342 | $13.43 | $11.21 | -759.24 | — | +0.00 | -759.24 | -868.68 | — |
| 2026-09-17 | `LEN` | 58 | $78.36 | $81.00 | +153.12 | — | +0.00 | +153.12 | +21.46 | — |
| 2026-09-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-21 | `ABVX` | 27 | — | $105.72 | +0.00 | $103.45 | -61.29 | -61.29 | +0.00 | -61.29 |
| 2026-09-21 | `MLKN` | 137 | — | $20.85 | +0.00 | $20.32 | -72.61 | -72.61 | +0.00 | -72.61 |
| 2026-09-21 | `THO` | 41 | — | $68.39 | +0.00 | $69.94 | +63.55 | +63.55 | +0.00 | +63.55 |
| 2026-09-22 | `ABVX` | 27 | $103.45 | $103.45 | +0.00 | $103.45 | +0.00 | +0.00 | -61.29 | -61.29 |
| 2026-09-22 | `MLKN` | 137 | $20.32 | $20.32 | +0.00 | $20.32 | +0.00 | +0.00 | -72.61 | -72.61 |
| 2026-09-22 | `THO` | 41 | $69.94 | $69.94 | +0.00 | $69.94 | +0.00 | +0.00 | +63.55 | +63.55 |
| 2026-09-23 | `ABVX` | 27 | $103.45 | $98.30 | -139.05 | — | +0.00 | -139.05 | -200.34 | — |
| 2026-09-23 | `MLKN` | 137 | $20.32 | $19.76 | -76.72 | — | +0.00 | -76.72 | -149.33 | — |
| 2026-09-23 | `THO` | 41 | $69.94 | $71.41 | +60.27 | — | +0.00 | +60.27 | +123.82 | — |
| 2026-09-23 | `BB` | 161 | — | $8.60 | +0.00 | $8.38 | -35.42 | -35.42 | +0.00 | -35.42 |
| 2026-09-23 | `DRI` | 6 | — | $215.10 | +0.00 | $213.69 | -8.46 | -8.46 | +0.00 | -8.46 |
| 2026-09-23 | `FUL` | 27 | — | $50.51 | +0.00 | $50.21 | -8.10 | -8.10 | +0.00 | -8.10 |
| 2026-09-23 | `NEOV` | 408 | — | $3.40 | +0.00 | $3.17 | -93.84 | -93.84 | +0.00 | -93.84 |
| 2026-09-23 | `SFIX` | 464 | — | $2.99 | +0.00 | $2.82 | -78.88 | -78.88 | +0.00 | -78.88 |
| 2026-09-23 | `SNX` | 4 | — | $283.46 | +0.00 | $287.89 | +17.72 | +17.72 | +0.00 | +17.72 |
| 2026-09-24 | `BB` | 161 | $8.38 | $8.42 | +6.44 | — | +0.00 | +6.44 | -28.98 | — |
| 2026-09-24 | `DRI` | 6 | $213.69 | $208.88 | -28.86 | — | +0.00 | -28.86 | -37.32 | — |
| 2026-09-24 | `FUL` | 27 | $50.21 | $49.29 | -24.84 | — | +0.00 | -24.84 | -32.94 | — |
| 2026-09-24 | `NEOV` | 408 | $3.17 | $2.65 | -212.16 | — | +0.00 | -212.16 | -306.00 | — |
| 2026-09-24 | `SFIX` | 464 | $2.82 | $2.31 | -236.64 | — | +0.00 | -236.64 | -315.52 | — |
| 2026-09-24 | `SNX` | 4 | $287.89 | $262.12 | -103.08 | — | +0.00 | -103.08 | -85.36 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +29.79 | DUOT, HTHT, NUAI, SIDU | — | $2.06 | $10,005.27 | DUOT×265, HTHT×61, NUAI×494, SIDU×973 |
| 2026-08-17 | +2.25 | $2.06 | DUOT×265, HTHT×61, NUAI×494, SIDU×973 | $10,423.70 | +418.43 | -93.52 | AS, BIDU, FN, HD, HSAI, IQ, KLAR, PONY | DUOT, HTHT, NUAI, SIDU | $524.64 | $10,277.91 | AS×39, BIDU×12, FN×2, HD×3, HSAI×70, IQ×962, KLAR×62, PONY×159 |
| 2026-08-18 | -6.20 | $524.64 | AS×39, BIDU×12, FN×2, HD×3, HSAI×70, IQ×962, KLAR×62, PONY×159 | $9,509.77 | -768.14 | +0.00 | — | AS, BIDU, FN, HD, HSAI, IQ, KLAR, PONY | $9,482.06 | $9,482.06 | — |
| 2026-08-19 | -7.20 | $9,482.06 | — | $9,482.06 | -0.00 | +0.00 | — | — | $9,482.06 | $9,482.06 | — |
| 2026-08-20 | +1.12 | $9,482.06 | — | $9,482.06 | -0.00 | -42.23 | BEKE, BJ, BKE, FLO, ROST | — | $106.26 | $9,428.03 | BEKE×111, BJ×21, BKE×44, FLO×255, ROST×8 |
| 2026-08-21 | +3.25 | $106.26 | BEKE×111, BJ×21, BKE×44, FLO×255, ROST×8 | $9,676.44 | +248.41 | -126.75 | PDD, XPEV | BEKE, BJ, BKE, FLO, ROST | $55.69 | $9,530.50 | PDD×53, XPEV×393 |
| 2026-08-24 | -5.17 | $55.69 | PDD×53, XPEV×393 | $9,525.23 | -5.27 | +0.00 | — | PDD, XPEV | $9,517.86 | $9,517.86 | — |
| 2026-08-25 | +1.80 | $9,517.86 | — | $9,517.86 | +0.00 | -307.99 | ANF, BBWI, BOX, DCI, DY, FSCO, HEI, INTU | — | $377.23 | $9,192.55 | ANF×10, BBWI×62, BOX×35, DCI×12, DY×3, FSCO×233, HEI×3, INTU×3 |
| 2026-08-26 | +2.02 | $377.23 | ANF×10, BBWI×62, BOX×35, DCI×12, DY×3, FSCO×233, HEI×3, INTU×3 | $9,409.89 | +217.34 | +69.00 | STDN, A, BBY, BILI, CM, CMBT, CRM, CRWD | ANF, BBWI, BOX, DCI, DY, FSCO, HEI, INTU | $534.57 | $9,444.68 | STDN×84, A×7, BBY×13, BILI×72, CM×9, CMBT×65, CRM×5, CRWD×6 |
| 2026-08-27 | — | $534.57 | STDN×84, A×7, BBY×13, BILI×72, CM×9, CMBT×65, CRM×5, CRWD×6 | $9,649.72 | +205.04 | +66.69 | GAP, ADSK, AFRM, BBAR, CHA, ESTC, HAFN, IREN | STDN, A, BBY, BILI, CM, CMBT, CRM, CRWD | $276.02 | $9,682.22 | GAP×58, ADSK×4, AFRM×15, BBAR×80, CHA×114, ESTC×14, HAFN×152, IREN×29 |
| 2026-08-28 | +0.75 | $276.02 | GAP×58, ADSK×4, AFRM×15, BBAR×80, CHA×114, ESTC×14, HAFN×152, IREN×29 | $10,233.05 | +550.83 | -48.44 | LX, SAIC | GAP, ADSK, AFRM, BBAR, CHA, ESTC, HAFN, IREN | $0.21 | $10,108.19 | LX×4403, SAIC×39 |
| 2026-08-31 | -5.85 | $0.21 | LX×4403, SAIC×39 | $9,922.45 | -185.74 | +0.00 | — | LX, SAIC | $9,862.73 | $9,862.73 | — |
| 2026-09-01 | -6.30 | $9,862.73 | — | $9,862.73 | -0.00 | +0.00 | — | — | $9,862.73 | $9,862.73 | — |
| 2026-09-02 | -3.83 | $9,862.73 | — | $9,862.73 | -0.00 | +0.00 | — | — | $9,862.73 | $9,862.73 | — |
| 2026-09-03 | -0.90 | $9,862.73 | — | $9,862.73 | -0.00 | +12.83 | AMBA, ASAN, DOCU, DOMO, GWRE, IOT, LULU, MAMA | — | $152.10 | $9,856.57 | AMBA×18, ASAN×121, DOCU×18, DOMO×326, GWRE×6, IOT×32, LULU×10, MAMA×78 |
| 2026-09-04 | +2.25 | $152.10 | AMBA×18, ASAN×121, DOCU×18, DOMO×326, GWRE×6, IOT×32, LULU×10, MAMA×78 | $9,406.93 | -449.64 | +39.91 | ABM, UNFI | AMBA, ASAN, DOCU, DOMO, GWRE, IOT, LULU, MAMA | $17.53 | $9,423.04 | ABM×100, UNFI×107 |
| 2026-09-08 | -11.47 | $17.53 | ABM×100, UNFI×107 | $9,436.00 | +12.96 | +0.00 | — | ABM, UNFI | $9,431.29 | $9,431.29 | — |
| 2026-09-09 | -13.95 | $9,431.29 | — | $9,431.29 | -0.00 | +0.00 | — | — | $9,431.29 | $9,431.29 | — |
| 2026-09-10 | -13.28 | $9,431.29 | — | $9,431.29 | -0.00 | +0.00 | — | — | $9,431.29 | $9,431.29 | — |
| 2026-09-11 | +0.50 | $9,431.29 | — | $9,431.29 | -0.00 | +0.00 | — | — | $9,431.29 | $9,431.29 | — |
| 2026-09-14 | -11.00 | $9,431.29 | — | $9,431.29 | -0.00 | +0.00 | — | — | $9,431.29 | $9,431.29 | — |
| 2026-09-15 | -3.84 | $9,431.29 | — | $9,431.29 | -0.00 | +0.00 | — | — | $9,431.29 | $9,431.29 | — |
| 2026-09-16 | +5.30 | $9,431.29 | — | $9,431.29 | -0.00 | -241.10 | ALMU, LEN | — | $45.67 | $9,183.61 | ALMU×342, LEN×58 |
| 2026-09-17 | +7.38 | $45.67 | ALMU×342, LEN×58 | $8,577.49 | -606.12 | +0.00 | — | ALMU, LEN | $8,570.78 | $8,570.78 | — |
| 2026-09-18 | +4.86 | $8,570.78 | — | $8,570.78 | +0.00 | +0.00 | — | — | $8,570.78 | $8,570.78 | — |
| 2026-09-21 | +12.87 | $8,570.78 | — | $8,570.78 | +0.00 | -70.35 | ABVX, MLKN, THO | — | $49.32 | $8,493.85 | ABVX×27, MLKN×137, THO×41 |
| 2026-09-22 | -0.50 | $49.32 | ABVX×27, MLKN×137, THO×41 | $8,493.85 | -0.00 | +0.00 | — | — | $49.32 | $8,493.85 | ABVX×27, MLKN×137, THO×41 |
| 2026-09-23 | +2.29 | $49.32 | ABVX×27, MLKN×137, THO×41 | $8,338.35 | -155.50 | -206.98 | BB, DRI, FUL, NEOV, SFIX, SNX | ABVX, MLKN, THO | $364.48 | $8,104.87 | BB×161, DRI×6, FUL×27, NEOV×408, SFIX×464, SNX×4 |
| 2026-09-24 | -7.66 | $364.48 | BB×161, DRI×6, FUL×27, NEOV×408, SFIX×464, SNX×4 | $7,505.73 | -599.14 | +0.00 | — | BB, DRI, FUL, NEOV, SFIX, SNX | $7,485.66 | $7,485.66 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `DUOT` | 265 | $9.43 | $3.42 | — | $7,497.63 | — | baseline list, no extra gate; list overnight; 🔵; ⚪; ret5=+7.7; leftover $2500.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HTHT` | 61 | $40.88 | $2.17 | — | $5,001.78 | — | baseline list, no extra gate; list overnight; ret5=-5.4; leftover $2500.00 | join🟢 sector🔴 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NUAI` | 494 | $5.06 | $6.37 | — | $2,495.77 | — | baseline list, no extra gate; list overnight; 🔵; ⚪; ret5=-3.2; leftover $2500.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SIDU` | 973 | $2.55 | $12.55 | — | $2.06 | — | baseline list, no extra gate; list overnight; 🔵; ret5=+21.5; leftover $2500.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.06 | ▲ close $10,005.27 vs 09:30 $10,000.00 (session +29.79) | 16:00 close · cash $2.06 · equity $10,005.27 vs 09:30 $10,000.00 (+5.27; session marks +29.79) · 4 name(s) marked open→close (per-name table). DUOT×265 09:30 $9.43 → close $9.11 -84.80; HTHT×61 09:30 $40.88 → close $41.88 +61.00; NUAI×494 09:30 $5.06 → close $5.07 +4.94; SIDU×973 09:30 $2.55 → close $2.60 +48.65 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.06 | ▲ 09:30 equity $10,423.70 vs yday $10,005.27 (+418.43) | 09:30 open · cash $2.06 (unchanged overnight, no fees) · equity $10,423.70 vs prior close $10,005.27 (+418.43) · 4 name(s) re-marked at the open (per-name table). DUOT×265 yday $9.11 → 09:30 $10.35 +328.60; HTHT×61 yday $41.88 → 09:30 $45.49 +220.21; NUAI×494 yday $5.07 → 09:30 $5.20 +64.22; SIDU×973 yday $2.60 → 09:30 $2.40 -194.60 | — |
| 2026-08-17 09:30 ET | **SELL** | `DUOT` | 265 | $10.35 | $3.48 | $+236.90 | $2,741.33 | ▲ +236.90 after sell → book $10,420.22; vs 09:30 mark -3.48 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `HTHT` | 61 | $45.49 | $2.21 | $+276.83 | $5,514.01 | ▲ +276.83 after sell → book $10,418.01; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `NUAI` | 494 | $5.20 | $6.48 | $+56.31 | $8,076.34 | ▲ +56.31 after sell → book $10,411.54; vs 09:30 mark -6.47 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `SIDU` | 973 | $2.40 | $12.73 | $-171.23 | $10,398.81 | ▼ -171.23 after sell → book $10,398.81; vs 09:30 mark -12.73 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `AS` | 39 | $32.88 | $2.11 | — | $9,114.38 | — | baseline list, no extra gate; list overnight; ret5=-10.8; leftover $1299.85 | join🟡 sector🔴 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `BIDU` | 12 | $102.83 | $2.03 | — | $7,878.39 | — | baseline list, no extra gate; list overnight; ⚪; ret5=-5.5; leftover $1299.85 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `FN` | 2 | $583.15 | $2.00 | — | $6,710.10 | — | baseline list, no extra gate; list overnight; ret5=+1.4; leftover $1299.85 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HD` | 3 | $334.71 | $2.00 | — | $5,703.97 | — | baseline list, no extra gate; list overnight,overnight_mega; ret5=-4.7; leftover $1299.85 | join🟡 sector🔴 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HSAI` | 70 | $18.32 | $2.20 | — | $4,419.37 | — | baseline list, no extra gate; list overnight; ret5=-5.3; leftover $1299.85 | join🟡 sector🔴 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `IQ` | 962 | $1.35 | $12.41 | — | $3,108.26 | — | baseline list, no extra gate; list overnight; ⚪; ret5=+1.5; leftover $1299.85 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `KLAR` | 62 | $20.67 | $2.18 | — | $1,824.54 | — | baseline list, no extra gate; list overnight; ret5=+4.5; leftover $1299.85 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `PONY` | 159 | $8.16 | $2.47 | — | $524.64 | — | baseline list, no extra gate; list overnight; ⚪; ret5=-0.1; leftover $1299.85 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $524.64 | ▼ close $10,277.91 vs 09:30 $10,423.70 (session -93.52) | 16:00 close · cash $524.64 · equity $10,277.91 vs 09:30 $10,423.70 (-145.79; session marks -93.52) · 8 name(s) marked open→close (per-name table). AS×39 09:30 $32.88 → close $32.57 -12.09; BIDU×12 09:30 $102.83 → close $104.12 +15.48; FN×2 09:30 $583.15 → close $598.58 +30.86; HD×3 09:30 $334.71 → close $337.88 +9.51; HSAI×70 09:30 $18.32 → close $18.07 -17.50; IQ×962 09:30 $1.35 → close $1.33 -19.24; KLAR×62 09:30 $20.67 → close $19.51 -71.92; PONY×159 09:30 $8.16 → close $7.98 -28.62 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $524.64 | ▼ 09:30 equity $9,509.77 vs yday $10,277.91 (-768.14) | 09:30 open · cash $524.64 (unchanged overnight, no fees) · equity $9,509.77 vs prior close $10,277.91 (-768.14) · 8 name(s) re-marked at the open (per-name table). AS×39 yday $32.57 → 09:30 $33.89 +51.48; BIDU×12 yday $104.12 → 09:30 $94.35 -117.24; FN×2 yday $598.58 → 09:30 $513.70 -169.76; HD×3 yday $337.88 → 09:30 $336.78 -3.30; HSAI×70 yday $18.07 → 09:30 $15.77 -161.35; IQ×962 yday $1.33 → 09:30 $1.27 -57.72; KLAR×62 yday $19.51 → 09:30 $15.66 -238.70; PONY×159 yday $7.98 → 09:30 $7.53 -71.55 | — |
| 2026-08-18 09:30 ET | **SELL** | `AS` | 39 | $33.89 | $2.13 | $+35.16 | $1,844.22 | ▲ +35.16 after sell → book $9,507.64; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `BIDU` | 12 | $94.35 | $2.05 | $-105.83 | $2,974.37 | ▼ -105.83 after sell → book $9,505.59; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `FN` | 2 | $513.70 | $2.02 | $-142.91 | $3,999.76 | ▼ -142.91 after sell → book $9,503.58; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HD` | 3 | $336.78 | $2.02 | $+2.19 | $5,008.08 | ▲ +2.19 after sell → book $9,501.56; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HSAI` | 70 | $15.77 | $2.22 | $-183.27 | $6,109.41 | ▼ -183.27 after sell → book $9,499.34; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `IQ` | 962 | $1.27 | $12.58 | $-101.95 | $7,318.57 | ▼ -101.95 after sell → book $9,486.76; vs 09:30 mark -12.58 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `KLAR` | 62 | $15.66 | $2.20 | $-314.99 | $8,287.29 | ▼ -314.99 after sell → book $9,484.56; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `PONY` | 159 | $7.53 | $2.50 | $-105.14 | $9,482.06 | ▼ -105.14 after sell → book $9,482.06; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,482.06 | ▲ close $9,482.06 vs 09:30 $9,509.77 (session +0.00) | 16:00 close · cash $9,482.06 · no lots left · equity $9,482.06. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,482.06 | ▲ 09:30 equity $9,482.06 vs yday $9,482.06 (-0.00) | 09:30 open · cash $9,482.06 · no holdings · equity $9,482.06 vs prior close $9,482.06 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,482.06 | ▲ close $9,482.06 vs 09:30 $9,482.06 (session +0.00) | 16:00 close · cash $9,482.06 · no lots left · equity $9,482.06. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,482.06 | ▲ 09:30 equity $9,482.06 vs yday $9,482.06 (-0.00) | 09:30 open · cash $9,482.06 · no holdings · equity $9,482.06 vs prior close $9,482.06 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BEKE` | 111 | $17.04 | $2.32 | — | $7,588.29 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-0.2; leftover $1896.41 | join🟢 sector🔴 gen🟢 news🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BJ` | 21 | $88.91 | $2.05 | — | $5,719.13 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-1.0; leftover $1896.41 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BKE` | 44 | $42.60 | $2.12 | — | $3,842.61 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-4.6; leftover $1896.41 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `FLO` | 255 | $7.43 | $3.29 | — | $1,944.67 | — | baseline list, no extra gate; list overnight; 🔵; ret5=+4.0; leftover $1896.41 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ROST` | 8 | $229.55 | $2.01 | — | $106.26 | — | baseline list, no extra gate; list overnight,overnight_mega; 🔵; ret5=-5.5; leftover $1896.41 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $106.26 | ▼ close $9,428.03 vs 09:30 $9,482.06 (session -42.23) | 16:00 close · cash $106.26 · equity $9,428.03 vs 09:30 $9,482.06 (-54.03; session marks -42.23) · 5 name(s) marked open→close (per-name table). BEKE×111 09:30 $17.04 → close $16.99 -5.55; BJ×21 09:30 $88.91 → close $91.30 +50.19; BKE×44 09:30 $42.60 → close $42.64 +1.76; FLO×255 09:30 $7.43 → close $7.10 -84.15; ROST×8 09:30 $229.55 → close $228.99 -4.48 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $106.26 | ▲ 09:30 equity $9,676.44 vs yday $9,428.03 (+248.41) | 09:30 open · cash $106.26 (unchanged overnight, no fees) · equity $9,676.44 vs prior close $9,428.03 (+248.41) · 5 name(s) re-marked at the open (per-name table). BEKE×111 yday $16.99 → 09:30 $17.93 +104.90; BJ×21 yday $91.30 → 09:30 $93.98 +56.28; BKE×44 yday $42.64 → 09:30 $43.08 +19.36; FLO×255 yday $7.10 → 09:30 $6.90 -51.00; ROST×8 yday $228.99 → 09:30 $243.85 +118.88 | — |
| 2026-08-21 09:30 ET | **SELL** | `BEKE` | 111 | $17.93 | $2.36 | $+94.66 | $2,094.68 | ▲ +94.66 after sell → book $9,674.08; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `BJ` | 21 | $93.98 | $2.08 | $+102.34 | $4,066.18 | ▲ +102.34 after sell → book $9,672.00; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `BKE` | 44 | $43.08 | $2.15 | $+16.85 | $5,959.56 | ▲ +16.85 after sell → book $9,669.86; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `FLO` | 255 | $6.90 | $3.35 | $-141.79 | $7,715.71 | ▼ -141.79 after sell → book $9,666.51; vs 09:30 mark -3.35 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ROST` | 8 | $243.85 | $2.04 | $+110.35 | $9,664.47 | ▲ +110.35 after sell → book $9,664.47; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `PDD` | 53 | $90.03 | $2.15 | — | $4,890.73 | — | baseline list, no extra gate; list overnight,overnight_mega; 🔵; ret5=+6.4; leftover $4832.24 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XPEV` | 393 | $12.29 | $5.07 | — | $55.69 | — | baseline list, no extra gate; list overnight; ret5=+1.9; leftover $4832.24 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $55.69 | ▼ close $9,530.50 vs 09:30 $9,676.44 (session -126.75) | 16:00 close · cash $55.69 · equity $9,530.50 vs 09:30 $9,676.44 (-145.94; session marks -126.75) · 2 name(s) marked open→close (per-name table). PDD×53 09:30 $90.03 → close $88.38 -87.45; XPEV×393 09:30 $12.29 → close $12.19 -39.30 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.69 | ▼ 09:30 equity $9,525.23 vs yday $9,530.50 (-5.27) | 09:30 open · cash $55.69 (unchanged overnight, no fees) · equity $9,525.23 vs prior close $9,530.50 (-5.27) · 2 name(s) re-marked at the open (per-name table). PDD×53 yday $88.38 → 09:30 $90.95 +136.21; XPEV×393 yday $12.19 → 09:30 $11.83 -141.48 | — |
| 2026-08-24 09:30 ET | **SELL** | `PDD` | 53 | $90.95 | $2.20 | $+44.41 | $4,873.85 | ▲ +44.41 after sell → book $9,523.04; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `XPEV` | 393 | $11.83 | $5.17 | $-191.02 | $9,517.86 | ▼ -191.02 after sell → book $9,517.86; vs 09:30 mark -5.18 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,517.86 | ▲ close $9,517.86 vs 09:30 $9,525.23 (session +0.00) | 16:00 close · cash $9,517.86 · no lots left · equity $9,517.86. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,517.86 | ▲ 09:30 equity $9,517.86 vs yday $9,517.86 (+0.00) | 09:30 open · cash $9,517.86 · no holdings · equity $9,517.86 vs prior close $9,517.86 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `ANF` | 10 | $112.17 | $2.02 | — | $8,394.14 | — | baseline list, no extra gate; list overnight; 🔵; ⚪; ret5=+6.8; leftover $1189.73 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BBWI` | 62 | $19.16 | $2.18 | — | $7,204.05 | — | baseline list, no extra gate; list overnight; 🔵; ret5=+3.0; leftover $1189.73 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BOX` | 35 | $33.33 | $2.10 | — | $6,035.40 | — | baseline list, no extra gate; list overnight; 🔵; ret5=+3.7; leftover $1189.73 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DCI` | 12 | $93.64 | $2.03 | — | $4,909.70 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-2.4; leftover $1189.73 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DY` | 3 | $390.22 | $2.00 | — | $3,737.04 | — | baseline list, no extra gate; list overnight; ret5=-12.0; leftover $1189.73 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🟡 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FSCO` | 233 | $5.10 | $3.01 | — | $2,545.73 | — | baseline list, no extra gate; list overnight; ret5=+0.2; leftover $1189.73 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HEI` | 3 | $357.15 | $2.00 | — | $1,472.28 | — | baseline list, no extra gate; list overnight; 🔵; ⚪; ret5=-5.0; leftover $1189.73 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `INTU` | 3 | $364.35 | $2.00 | — | $377.23 | — | baseline list, no extra gate; list overnight,overnight_mega; 🔵; ret5=+10.2; leftover $1189.73 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $377.23 | ▼ close $9,192.55 vs 09:30 $9,517.86 (session -307.99) | 16:00 close · cash $377.23 · equity $9,192.55 vs 09:30 $9,517.86 (-325.31; session marks -307.99) · 8 name(s) marked open→close (per-name table). ANF×10 09:30 $112.17 → close $108.90 -32.70; BBWI×62 09:30 $19.16 → close $17.58 -97.96; BOX×35 09:30 $33.33 → close $33.00 -11.55; DCI×12 09:30 $93.64 → close $93.26 -4.56; DY×3 09:30 $390.22 → close $351.80 -115.26; FSCO×233 09:30 $5.10 → close $5.07 -6.99; HEI×3 09:30 $357.15 → close $351.05 -18.30; INTU×3 09:30 $364.35 → close $357.46 -20.67 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $377.23 | ▲ 09:30 equity $9,409.89 vs yday $9,192.55 (+217.34) | 09:30 open · cash $377.23 (unchanged overnight, no fees) · equity $9,409.89 vs prior close $9,192.55 (+217.34) · 8 name(s) re-marked at the open (per-name table). ANF×10 yday $108.90 → 09:30 $131.37 +224.70; BBWI×62 yday $17.58 → 09:30 $18.26 +42.16; BOX×35 yday $33.00 → 09:30 $34.30 +45.50; DCI×12 yday $93.26 → 09:30 $95.13 +22.44; DY×3 yday $351.80 → 09:30 $326.91 -74.67; FSCO×233 yday $5.07 → 09:30 $5.08 +2.33; HEI×3 yday $351.05 → 09:30 $370.00 +56.85; INTU×3 yday $357.46 → 09:30 $323.47 -101.97 | — |
| 2026-08-26 09:30 ET | **SELL** | `ANF` | 10 | $131.37 | $2.04 | $+187.94 | $1,688.89 | ▲ +187.94 after sell → book $9,407.85; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `BBWI` | 62 | $18.26 | $2.20 | $-60.17 | $2,818.82 | ▼ -60.17 after sell → book $9,405.66; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `BOX` | 35 | $34.30 | $2.12 | $+29.74 | $4,017.20 | ▲ +29.74 after sell → book $9,403.54; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `DCI` | 12 | $95.13 | $2.05 | $+13.81 | $5,156.72 | ▲ +13.81 after sell → book $9,401.50; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `DY` | 3 | $326.91 | $2.02 | $-193.95 | $6,135.43 | ▼ -193.95 after sell → book $9,399.48; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `FSCO` | 233 | $5.08 | $3.05 | $-10.72 | $7,316.01 | ▼ -10.72 after sell → book $9,396.42; vs 09:30 mark -3.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟡 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `HEI` | 3 | $370.00 | $2.02 | $+34.53 | $8,423.99 | ▲ +34.53 after sell → book $9,394.40; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `INTU` | 3 | $323.47 | $2.02 | $-126.66 | $9,392.38 | ▼ -126.66 after sell → book $9,392.38; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟡 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `STDN` | 84 | $13.95 | $2.24 | — | $8,218.34 | — | baseline list, no extra gate; list ohlc_hot,overnight; 🔵; ret5=+14.3; leftover $1174.05 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `A` | 7 | $152.45 | $2.01 | — | $7,149.18 | — | baseline list, no extra gate; list overnight; ret5=+4.3; leftover $1174.05 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BBY` | 13 | $85.19 | $2.03 | — | $6,039.68 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-2.3; leftover $1174.05 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BILI` | 72 | $16.22 | $2.21 | — | $4,869.64 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-2.5; leftover $1174.05 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 9 | $118.50 | $2.02 | — | $3,801.12 | — | baseline list, no extra gate; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $1174.05 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CMBT` | 65 | $17.91 | $2.19 | — | $2,634.78 | — | baseline list, no extra gate; list overnight; ret5=+3.9; leftover $1174.05 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CRM` | 5 | $199.94 | $2.00 | — | $1,633.08 | — | baseline list, no extra gate; list overnight,overnight_mega; ret5=+2.1; leftover $1174.05 | join🟡 sector🔴 gen🟢 news🔴 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 catal🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CRWD` | 6 | $182.75 | $2.01 | — | $534.57 | — | baseline list, no extra gate; list overnight,overnight_mega; 🔵; ret5=-12.9; leftover $1174.05 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $534.57 | ▲ close $9,444.68 vs 09:30 $9,409.89 (session +69.00) | 16:00 close · cash $534.57 · equity $9,444.68 vs 09:30 $9,409.89 (+34.79; session marks +69.00) · 8 name(s) marked open→close (per-name table). STDN×84 09:30 $13.95 → close $13.70 -21.00; A×7 09:30 $152.45 → close $155.08 +18.41; BBY×13 09:30 $85.19 → close $87.44 +29.25; BILI×72 09:30 $16.22 → close $16.15 -5.04; CM×9 09:30 $118.50 → close $118.20 -2.70; CMBT×65 09:30 $17.91 → close $17.65 -16.90; CRM×5 09:30 $199.94 → close $205.62 +28.40; CRWD×6 09:30 $182.75 → close $189.18 +38.58 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $534.57 | ▲ 09:30 equity $9,649.72 vs yday $9,444.68 (+205.04) | 09:30 open · cash $534.57 (unchanged overnight, no fees) · equity $9,649.72 vs prior close $9,444.68 (+205.04) · 8 name(s) re-marked at the open (per-name table). STDN×84 yday $13.70 → 09:30 $13.84 +11.76; A×7 yday $155.08 → 09:30 $159.35 +29.89; BBY×13 yday $87.44 → 09:30 $80.60 -88.92; BILI×72 yday $16.15 → 09:30 $16.18 +2.16; CM×9 yday $118.20 → 09:30 $118.77 +5.13; CMBT×65 yday $17.65 → 09:30 $17.78 +8.45; CRM×5 yday $205.62 → 09:30 $230.05 +122.15; CRWD×6 yday $189.18 → 09:30 $208.25 +114.42 | — |
| 2026-08-27 09:30 ET | **SELL** | `STDN` | 84 | $13.84 | $2.27 | $-13.75 | $1,694.87 | ▼ -13.75 after sell → book $9,647.46; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `A` | 7 | $159.35 | $2.03 | $+44.26 | $2,808.28 | ▲ +44.26 after sell → book $9,645.42; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BBY` | 13 | $80.60 | $2.05 | $-63.75 | $3,854.04 | ▼ -63.75 after sell → book $9,643.38; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `BILI` | 72 | $16.18 | $2.23 | $-7.31 | $5,016.77 | ▼ -7.31 after sell → book $9,641.15; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `CM` | 9 | $118.77 | $2.04 | $-1.62 | $6,083.66 | ▼ -1.62 after sell → book $9,639.11; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `CMBT` | 65 | $17.78 | $2.21 | $-12.84 | $7,237.15 | ▼ -12.84 after sell → book $9,636.90; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `CRM` | 5 | $230.05 | $2.02 | $+146.52 | $8,385.38 | ▲ +146.52 after sell → book $9,634.88; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRWD` | 6 | $208.25 | $2.03 | $+148.96 | $9,632.85 | ▲ +148.96 after sell → book $9,632.85; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `GAP` | 58 | $20.75 | $2.16 | — | $8,427.19 | — | baseline list, no extra gate; list ohlc_hot,overnight; ret5=+5.2; leftover $1204.11 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ADSK` | 4 | $261.47 | $2.00 | — | $7,379.31 | — | baseline list, no extra gate; list overnight,overnight_mega; 🔵; ret5=+1.4; leftover $1204.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `AFRM` | 15 | $76.90 | $2.04 | — | $6,223.77 | — | baseline list, no extra gate; list overnight; ret5=-1.1; leftover $1204.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `BBAR` | 80 | $14.96 | $2.23 | — | $5,024.74 | — | baseline list, no extra gate; list overnight; ret5=+3.0; leftover $1204.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CHA` | 114 | $10.54 | $2.33 | — | $3,820.85 | — | baseline list, no extra gate; list overnight; ret5=+2.5; leftover $1204.11 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ESTC` | 14 | $82.65 | $2.03 | — | $2,661.72 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-9.3; leftover $1204.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `HAFN` | 152 | $7.91 | $2.45 | — | $1,456.95 | — | baseline list, no extra gate; list overnight; ret5=-1.8; leftover $1204.11 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `IREN` | 29 | $40.65 | $2.08 | — | $276.02 | — | baseline list, no extra gate; list overnight; ret5=-7.6; leftover $1204.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟡 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $276.02 | ▲ close $9,682.22 vs 09:30 $9,649.72 (session +66.69) | 16:00 close · cash $276.02 · equity $9,682.22 vs 09:30 $9,649.72 (+32.50; session marks +66.69) · 8 name(s) marked open→close (per-name table). GAP×58 09:30 $20.75 → close $20.79 +2.32; ADSK×4 09:30 $261.47 → close $270.58 +36.44; AFRM×15 09:30 $76.90 → close $77.49 +8.85; BBAR×80 09:30 $14.96 → close $14.60 -28.80; CHA×114 09:30 $10.54 → close $10.35 -21.66; ESTC×14 09:30 $82.65 → close $83.74 +15.26; HAFN×152 09:30 $7.91 → close $8.29 +57.76; IREN×29 09:30 $40.65 → close $40.53 -3.48 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $276.02 | ▲ 09:30 equity $10,233.05 vs yday $9,682.22 (+550.83) | 09:30 open · cash $276.02 (unchanged overnight, no fees) · equity $10,233.05 vs prior close $9,682.22 (+550.83) · 8 name(s) re-marked at the open (per-name table). GAP×58 yday $20.79 → 09:30 $24.69 +226.20; ADSK×4 yday $270.58 → 09:30 $261.16 -37.68; AFRM×15 yday $77.49 → 09:30 $86.00 +127.65; BBAR×80 yday $14.60 → 09:30 $15.01 +32.80; CHA×114 yday $10.35 → 09:30 $10.30 -5.70; ESTC×14 yday $83.74 → 09:30 $103.89 +282.10; HAFN×152 yday $8.29 → 09:30 $8.35 +9.12; IREN×29 yday $40.53 → 09:30 $37.65 -83.66 | — |
| 2026-08-28 09:30 ET | **SELL** | `GAP` | 58 | $24.69 | $2.19 | $+224.17 | $1,705.86 | ▲ +224.17 after sell → book $10,230.86; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ADSK` | 4 | $261.16 | $2.02 | $-5.26 | $2,748.48 | ▼ -5.26 after sell → book $10,228.84; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `AFRM` | 15 | $86.00 | $2.06 | $+132.41 | $4,036.42 | ▲ +132.41 after sell → book $10,226.79; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `BBAR` | 80 | $15.01 | $2.25 | $-0.48 | $5,234.97 | ▼ -0.48 after sell → book $10,224.53; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CHA` | 114 | $10.30 | $2.36 | $-32.05 | $6,406.81 | ▼ -32.05 after sell → book $10,222.17; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ESTC` | 14 | $103.89 | $2.05 | $+293.27 | $7,859.21 | ▲ +293.27 after sell → book $10,220.12; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `HAFN` | 152 | $8.35 | $2.48 | $+61.95 | $9,125.93 | ▲ +61.95 after sell → book $10,217.64; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `IREN` | 29 | $37.65 | $2.10 | $-91.32 | $10,215.54 | ▼ -91.32 after sell → book $10,215.54; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LX` | 4403 | $1.16 | $56.80 | — | $5,051.26 | — | baseline list, no extra gate; list overnight; ret5=-13.8; leftover $5107.77 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SAIC` | 39 | $129.46 | $2.11 | — | $0.21 | — | baseline list, no extra gate; list overnight; ret5=+2.1; leftover $5107.77 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.21 | ▼ close $10,108.19 vs 09:30 $10,233.05 (session -48.44) | 16:00 close · cash $0.21 · equity $10,108.19 vs 09:30 $10,233.05 (-124.86; session marks -48.44) · 2 name(s) marked open→close (per-name table). LX×4403 09:30 $1.16 → close $1.18 +88.06; SAIC×39 09:30 $129.46 → close $125.96 -136.50 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.21 | ▼ 09:30 equity $9,922.45 vs yday $10,108.19 (-185.74) | 09:30 open · cash $0.21 (unchanged overnight, no fees) · equity $9,922.45 vs prior close $10,108.19 (-185.74) · 2 name(s) re-marked at the open (per-name table). LX×4403 yday $1.18 → 09:30 $1.01 -748.51; SAIC×39 yday $125.96 → 09:30 $140.39 +562.77 | — |
| 2026-08-31 09:30 ET | **SELL** | `LX` | 4403 | $1.01 | $57.57 | $-774.81 | $4,389.68 | ▼ -774.81 after sell → book $9,864.89; vs 09:30 mark -57.56 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `SAIC` | 39 | $140.39 | $2.16 | $+422.00 | $9,862.73 | ▲ +422.00 after sell → book $9,862.73; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,862.73 | ▲ close $9,862.73 vs 09:30 $9,922.45 (session +0.00) | 16:00 close · cash $9,862.73 · no lots left · equity $9,862.73. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,862.73 | ▲ 09:30 equity $9,862.73 vs yday $9,862.73 (-0.00) | 09:30 open · cash $9,862.73 · no holdings · equity $9,862.73 vs prior close $9,862.73 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,862.73 | ▲ close $9,862.73 vs 09:30 $9,862.73 (session +0.00) | 16:00 close · cash $9,862.73 · no lots left · equity $9,862.73. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,862.73 | ▲ 09:30 equity $9,862.73 vs yday $9,862.73 (-0.00) | 09:30 open · cash $9,862.73 · no holdings · equity $9,862.73 vs prior close $9,862.73 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,862.73 | ▲ close $9,862.73 vs 09:30 $9,862.73 (session +0.00) | 16:00 close · cash $9,862.73 · no lots left · equity $9,862.73. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,862.73 | ▲ 09:30 equity $9,862.73 vs yday $9,862.73 (-0.00) | 09:30 open · cash $9,862.73 · no holdings · equity $9,862.73 vs prior close $9,862.73 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AMBA` | 18 | $66.61 | $2.04 | — | $8,661.70 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-3.6; leftover $1232.84 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ASAN` | 121 | $10.16 | $2.35 | — | $7,429.99 | — | baseline list, no extra gate; list overnight; 🔵; ret5=+3.8; leftover $1232.84 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟡 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DOCU` | 18 | $67.06 | $2.04 | — | $6,220.87 | — | baseline list, no extra gate; list overnight; 🔵; ret5=+10.2; leftover $1232.84 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DOMO` | 326 | $3.78 | $4.21 | — | $4,984.38 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-2.4; leftover $1232.84 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GWRE` | 6 | $198.00 | $2.01 | — | $3,794.37 | — | baseline list, no extra gate; list overnight; 🔵; ret5=+0.9; leftover $1232.84 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `IOT` | 32 | $37.69 | $2.09 | — | $2,586.21 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-7.7; leftover $1232.84 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `LULU` | 10 | $121.15 | $2.02 | — | $1,372.69 | — | baseline list, no extra gate; list overnight; 🔵; ret5=+3.2; leftover $1232.84 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MAMA` | 78 | $15.62 | $2.22 | — | $152.10 | — | baseline list, no extra gate; list overnight; ret5=-6.7; leftover $1232.84 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $152.10 | ▲ close $9,856.57 vs 09:30 $9,862.73 (session +12.83) | 16:00 close · cash $152.10 · equity $9,856.57 vs 09:30 $9,862.73 (-6.16; session marks +12.83) · 8 name(s) marked open→close (per-name table). AMBA×18 09:30 $66.61 → close $63.38 -58.14; ASAN×121 09:30 $10.16 → close $10.09 -8.47; DOCU×18 09:30 $67.06 → close $65.97 -19.62; DOMO×326 09:30 $3.78 → close $3.79 +3.26; GWRE×6 09:30 $198.00 → close $202.86 +29.16; IOT×32 09:30 $37.69 → close $38.75 +33.92; LULU×10 09:30 $121.15 → close $121.77 +6.20; MAMA×78 09:30 $15.62 → close $15.96 +26.52 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $152.10 | ▼ 09:30 equity $9,406.93 vs yday $9,856.57 (-449.64) | 09:30 open · cash $152.10 (unchanged overnight, no fees) · equity $9,406.93 vs prior close $9,856.57 (-449.64) · 8 name(s) re-marked at the open (per-name table). AMBA×18 yday $63.38 → 09:30 $63.18 -3.60; ASAN×121 yday $10.09 → 09:30 $8.74 -163.35; DOCU×18 yday $65.97 → 09:30 $68.52 +45.90; DOMO×326 yday $3.79 → 09:30 $3.62 -57.05; GWRE×6 yday $202.86 → 09:30 $167.55 -211.86; IOT×32 yday $38.75 → 09:30 $44.90 +196.80; LULU×10 yday $121.77 → 09:30 $98.15 -236.20; MAMA×78 yday $15.96 → 09:30 $15.70 -20.28 | — |
| 2026-09-04 09:30 ET | **SELL** | `AMBA` | 18 | $63.18 | $2.06 | $-65.85 | $1,287.28 | ▼ -65.85 after sell → book $9,404.87; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `ASAN` | 121 | $8.74 | $2.38 | $-176.56 | $2,342.44 | ▼ -176.56 after sell → book $9,402.49; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `DOCU` | 18 | $68.52 | $2.06 | $+22.17 | $3,573.73 | ▲ +22.17 after sell → book $9,400.42; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `DOMO` | 326 | $3.62 | $4.27 | $-62.26 | $4,747.95 | ▼ -62.26 after sell → book $9,396.15; vs 09:30 mark -4.27 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `GWRE` | 6 | $167.55 | $2.03 | $-186.74 | $5,751.23 | ▼ -186.74 after sell → book $9,394.13; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `IOT` | 32 | $44.90 | $2.11 | $+226.53 | $7,185.92 | ▲ +226.53 after sell → book $9,392.02; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `LULU` | 10 | $98.15 | $2.04 | $-234.06 | $8,165.38 | ▼ -234.06 after sell → book $9,389.98; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `MAMA` | 78 | $15.70 | $2.25 | $+1.77 | $9,387.73 | ▲ +1.77 after sell → book $9,387.73; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ABM` | 100 | $46.79 | $2.29 | — | $4,706.44 | — | baseline list, no extra gate; list overnight; ret5=+0.2; leftover $4693.87 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `UNFI` | 107 | $43.80 | $2.31 | — | $17.53 | — | baseline list, no extra gate; list overnight; ret5=-7.7; leftover $4693.87 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.53 | ▲ close $9,423.04 vs 09:30 $9,406.93 (session +39.91) | 16:00 close · cash $17.53 · equity $9,423.04 vs 09:30 $9,406.93 (+16.11; session marks +39.91) · 2 name(s) marked open→close (per-name table). ABM×100 09:30 $46.79 → close $47.05 +26.00; UNFI×107 09:30 $43.80 → close $43.93 +13.91 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.53 | ▲ 09:30 equity $9,436.00 vs yday $9,423.04 (+12.96) | 09:30 open · cash $17.53 (unchanged overnight, no fees) · equity $9,436.00 vs prior close $9,423.04 (+12.96) · 2 name(s) re-marked at the open (per-name table). ABM×100 yday $47.05 → 09:30 $45.81 -124.00; UNFI×107 yday $43.93 → 09:30 $45.21 +136.96 | — |
| 2026-09-08 09:30 ET | **SELL** | `ABM` | 100 | $45.81 | $2.34 | $-102.63 | $4,596.19 | ▼ -102.63 after sell → book $9,433.66; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `UNFI` | 107 | $45.21 | $2.37 | $+146.19 | $9,431.29 | ▲ +146.19 after sell → book $9,431.29; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,431.29 | ▲ close $9,431.29 vs 09:30 $9,436.00 (session +0.00) | 16:00 close · cash $9,431.29 · no lots left · equity $9,431.29. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,431.29 | ▲ 09:30 equity $9,431.29 vs yday $9,431.29 (-0.00) | 09:30 open · cash $9,431.29 · no holdings · equity $9,431.29 vs prior close $9,431.29 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,431.29 | ▲ close $9,431.29 vs 09:30 $9,431.29 (session +0.00) | 16:00 close · cash $9,431.29 · no lots left · equity $9,431.29. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,431.29 | ▲ 09:30 equity $9,431.29 vs yday $9,431.29 (-0.00) | 09:30 open · cash $9,431.29 · no holdings · equity $9,431.29 vs prior close $9,431.29 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,431.29 | ▲ close $9,431.29 vs 09:30 $9,431.29 (session +0.00) | 16:00 close · cash $9,431.29 · no lots left · equity $9,431.29. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,431.29 | ▲ 09:30 equity $9,431.29 vs yday $9,431.29 (-0.00) | 09:30 open · cash $9,431.29 · no holdings · equity $9,431.29 vs prior close $9,431.29 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,431.29 | ▲ close $9,431.29 vs 09:30 $9,431.29 (session +0.00) | 16:00 close · cash $9,431.29 · no lots left · equity $9,431.29. | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,431.29 | ▲ 09:30 equity $9,431.29 vs yday $9,431.29 (-0.00) | 09:30 open · cash $9,431.29 · no holdings · equity $9,431.29 vs prior close $9,431.29 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,431.29 | ▲ close $9,431.29 vs 09:30 $9,431.29 (session +0.00) | 16:00 close · cash $9,431.29 · no lots left · equity $9,431.29. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,431.29 | ▲ 09:30 equity $9,431.29 vs yday $9,431.29 (-0.00) | 09:30 open · cash $9,431.29 · no holdings · equity $9,431.29 vs prior close $9,431.29 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,431.29 | ▲ close $9,431.29 vs 09:30 $9,431.29 (session +0.00) | 16:00 close · cash $9,431.29 · no lots left · equity $9,431.29. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,431.29 | ▲ 09:30 equity $9,431.29 vs yday $9,431.29 (-0.00) | 09:30 open · cash $9,431.29 · no holdings · equity $9,431.29 vs prior close $9,431.29 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `ALMU` | 342 | $13.75 | $4.41 | — | $4,724.38 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-1.4; leftover $4715.64 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `LEN` | 58 | $80.63 | $2.16 | — | $45.67 | — | baseline list, no extra gate; list overnight; ret5=-0.4; leftover $4715.64 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🔴 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $45.67 | ▼ close $9,183.61 vs 09:30 $9,431.29 (session -241.10) | 16:00 close · cash $45.67 · equity $9,183.61 vs 09:30 $9,431.29 (-247.68; session marks -241.10) · 2 name(s) marked open→close (per-name table). ALMU×342 09:30 $13.75 → close $13.43 -109.44; LEN×58 09:30 $80.63 → close $78.36 -131.66 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $45.67 | ▼ 09:30 equity $8,577.49 vs yday $9,183.61 (-606.12) | 09:30 open · cash $45.67 (unchanged overnight, no fees) · equity $8,577.49 vs prior close $9,183.61 (-606.12) · 2 name(s) re-marked at the open (per-name table). ALMU×342 yday $13.43 → 09:30 $11.21 -759.24; LEN×58 yday $78.36 → 09:30 $81.00 +153.12 | — |
| 2026-09-17 09:30 ET | **SELL** | `ALMU` | 342 | $11.21 | $4.50 | $-877.59 | $3,874.99 | ▼ -877.59 after sell → book $8,572.99; vs 09:30 mark -4.50 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `LEN` | 58 | $81.00 | $2.21 | $+17.08 | $8,570.78 | ▲ +17.08 after sell → book $8,570.78; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,570.78 | ▲ close $8,570.78 vs 09:30 $8,577.49 (session +0.00) | 16:00 close · cash $8,570.78 · no lots left · equity $8,570.78. | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,570.78 | ▲ 09:30 equity $8,570.78 vs yday $8,570.78 (+0.00) | 09:30 open · cash $8,570.78 · no holdings · equity $8,570.78 vs prior close $8,570.78 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,570.78 | ▲ close $8,570.78 vs 09:30 $8,570.78 (session +0.00) | 16:00 close · cash $8,570.78 · no lots left · equity $8,570.78. | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,570.78 | ▲ 09:30 equity $8,570.78 vs yday $8,570.78 (+0.00) | 09:30 open · cash $8,570.78 · no holdings · equity $8,570.78 vs prior close $8,570.78 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-21 09:30 ET | **BUY** | `ABVX` | 27 | $105.72 | $2.07 | — | $5,714.27 | — | baseline list, no extra gate; list overnight; ret5=-11.5; leftover $2856.93 | join🟡 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `MLKN` | 137 | $20.85 | $2.40 | — | $2,855.42 | — | baseline list, no extra gate; list overnight; ret5=-2.5; leftover $2856.93 | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `THO` | 41 | $68.39 | $2.11 | — | $49.32 | — | baseline list, no extra gate; list overnight; ret5=-7.0; leftover $2856.93 | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.32 | ▼ close $8,493.85 vs 09:30 $8,570.78 (session -70.35) | 16:00 close · cash $49.32 · equity $8,493.85 vs 09:30 $8,570.78 (-76.93; session marks -70.35) · 3 name(s) marked open→close (per-name table). ABVX×27 09:30 $105.72 → close $103.45 -61.29; MLKN×137 09:30 $20.85 → close $20.32 -72.61; THO×41 09:30 $68.39 → close $69.94 +63.55 | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.32 | ▲ 09:30 equity $8,493.85 vs yday $8,493.85 (-0.00) | 09:30 open · cash $49.32 (unchanged overnight, no fees) · equity $8,493.85 vs prior close $8,493.85 (-0.00) · 3 name(s) re-marked at the open (per-name table). ABVX×27 yday $103.45 → 09:30 $103.45 +0.00; MLKN×137 yday $20.32 → 09:30 $20.32 +0.00; THO×41 yday $69.94 → 09:30 $69.94 +0.00 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.32 | ▲ close $8,493.85 vs 09:30 $8,493.85 (session +0.00) | 16:00 close · cash $49.32 · equity $8,493.85 vs 09:30 $8,493.85 (-0.00; session marks +0.00) · 3 name(s) marked open→close (per-name table). ABVX×27 09:30 $103.45 → close $103.45 +0.00; MLKN×137 09:30 $20.32 → close $20.32 +0.00; THO×41 09:30 $69.94 → close $69.94 +0.00 | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.32 | ▼ 09:30 equity $8,338.35 vs yday $8,493.85 (-155.50) | 09:30 open · cash $49.32 (unchanged overnight, no fees) · equity $8,338.35 vs prior close $8,493.85 (-155.50) · 3 name(s) re-marked at the open (per-name table). ABVX×27 yday $103.45 → 09:30 $98.30 -139.05; MLKN×137 yday $20.32 → 09:30 $19.76 -76.72; THO×41 yday $69.94 → 09:30 $71.41 +60.27 | — |
| 2026-09-23 09:30 ET | **SELL** | `ABVX` | 27 | $98.30 | $2.10 | $-204.51 | $2,701.32 | ▼ -204.51 after sell → book $8,336.25; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MLKN` | 137 | $19.76 | $2.45 | $-154.18 | $5,405.99 | ▼ -154.18 after sell → book $8,333.80; vs 09:30 mark -2.45 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `THO` | 41 | $71.41 | $2.15 | $+119.56 | $8,331.65 | ▲ +119.56 after sell → book $8,331.65; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `BB` | 161 | $8.60 | $2.47 | — | $6,944.58 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-0.4; leftover $1388.61 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `DRI` | 6 | $215.10 | $2.01 | — | $5,651.97 | — | baseline list, no extra gate; list overnight; ret5=-0.6; leftover $1388.61 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `FUL` | 27 | $50.51 | $2.07 | — | $4,286.13 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-2.6; leftover $1388.61 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 judge🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `NEOV` | 408 | $3.40 | $5.26 | — | $2,893.67 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-4.8; leftover $1388.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `SFIX` | 464 | $2.99 | $5.99 | — | $1,500.32 | — | baseline list, no extra gate; list overnight; ret5=+1.7; leftover $1388.61 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `SNX` | 4 | $283.46 | $2.00 | — | $364.48 | — | baseline list, no extra gate; list overnight; 🔵; ⚪; ret5=+2.3; leftover $1388.61 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $364.48 | ▼ close $8,104.87 vs 09:30 $8,338.35 (session -206.98) | 16:00 close · cash $364.48 · equity $8,104.87 vs 09:30 $8,338.35 (-233.48; session marks -206.98) · 6 name(s) marked open→close (per-name table). BB×161 09:30 $8.60 → close $8.38 -35.42; DRI×6 09:30 $215.10 → close $213.69 -8.46; FUL×27 09:30 $50.51 → close $50.21 -8.10; NEOV×408 09:30 $3.40 → close $3.17 -93.84; SFIX×464 09:30 $2.99 → close $2.82 -78.88; SNX×4 09:30 $283.46 → close $287.89 +17.72 | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $364.48 | ▼ 09:30 equity $7,505.73 vs yday $8,104.87 (-599.14) | 09:30 open · cash $364.48 (unchanged overnight, no fees) · equity $7,505.73 vs prior close $8,104.87 (-599.14) · 6 name(s) re-marked at the open (per-name table). BB×161 yday $8.38 → 09:30 $8.42 +6.44; DRI×6 yday $213.69 → 09:30 $208.88 -28.86; FUL×27 yday $50.21 → 09:30 $49.29 -24.84; NEOV×408 yday $3.17 → 09:30 $2.65 -212.16; SFIX×464 yday $2.82 → 09:30 $2.31 -236.64; SNX×4 yday $287.89 → 09:30 $262.12 -103.08 | — |
| 2026-09-24 09:30 ET | **SELL** | `BB` | 161 | $8.42 | $2.51 | $-33.96 | $1,717.59 | ▼ -33.96 after sell → book $7,503.22; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-24 09:30 ET | **SELL** | `DRI` | 6 | $208.88 | $2.03 | $-41.36 | $2,968.84 | ▼ -41.36 after sell → book $7,501.19; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-24 09:30 ET | **SELL** | `FUL` | 27 | $49.29 | $2.09 | $-37.10 | $4,297.58 | ▼ -37.10 after sell → book $7,499.10; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-24 09:30 ET | **SELL** | `NEOV` | 408 | $2.65 | $5.34 | $-316.60 | $5,373.44 | ▼ -316.60 after sell → book $7,493.76; vs 09:30 mark -5.34 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-24 09:30 ET | **SELL** | `SFIX` | 464 | $2.31 | $6.07 | $-327.58 | $6,439.21 | ▼ -327.58 after sell → book $7,487.69; vs 09:30 mark -6.07 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-24 09:30 ET | **SELL** | `SNX` | 4 | $262.12 | $2.02 | $-89.38 | $7,485.66 | ▼ -89.38 after sell → book $7,485.66; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,485.66 | ▲ close $7,485.66 vs 09:30 $7,505.73 (session +0.00) | 16:00 close · cash $7,485.66 · no lots left · equity $7,485.66. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `ZIM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ADI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `DVLT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `EL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `JKHY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KEYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `LOW` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AEG` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ALVO` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ATAT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ATHM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BABA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BILL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BULL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `BMO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BNS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BZ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DKS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GRRR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SHMD` | hard_red | hard-red S=-5.17 sit; no new buys |
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
| 2026-09-02 | `AVGO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CHPT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CIEN` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CPB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FIVE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HPE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MOMO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `ASO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BRZE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CGNT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHWY` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GME` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `AEO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AVAV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `COO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `M` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAVN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `WLTH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `ADBE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CPRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DSGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `KR` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LPTH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `REF` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `RH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `FPS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HITI` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `PLAY` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `TCOM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `ABVX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `MLKN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `THO` | no_price | no 09:30 open — carry |
| 2026-09-22 | `CBRL` | no_price | no 09:30 open |
| 2026-09-22 | `CTAS` | no_price | no 09:30 open |
| 2026-09-22 | `GIS` | cash | leftover split 9.86 < 1 share @ 35.96 |
| 2026-09-22 | `KBH` | cash | leftover split 9.86 < 1 share @ 49.39 |
| 2026-09-22 | `PAYX` | no_price | no 09:30 open |
| 2026-09-24 | `COST` | hard_red | hard-red S=-7.66 sit; no new buys |
