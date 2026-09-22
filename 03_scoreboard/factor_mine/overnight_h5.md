# Factor mine action — `overnight_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `overnight` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-11.63%** ($8,837) · signal-only (no cash/fees) was -27.10%. Starts YES **1/27**. Fills 22 · skips 163 · realized $-968.16.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at names the prior Finviz calendar said report AMC today or BMO next session (print not in yet) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 5 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 5 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 5 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `overnight` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $0.15.

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
| 2026-08-17 | `DUOT` | 265 | $9.11 | $10.35 | +328.60 | $10.28 | -18.55 | +310.05 | +243.80 | +225.25 |
| 2026-08-17 | `HTHT` | 61 | $41.88 | $45.49 | +220.21 | $46.61 | +68.32 | +288.53 | +281.21 | +349.53 |
| 2026-08-17 | `NUAI` | 494 | $5.07 | $5.20 | +64.22 | $5.46 | +128.44 | +192.66 | +69.16 | +197.60 |
| 2026-08-17 | `SIDU` | 973 | $2.60 | $2.40 | -194.60 | $2.54 | +131.36 | -63.24 | -145.95 | -14.59 |
| 2026-08-18 | `DUOT` | 265 | $10.28 | $11.53 | +329.93 | $11.59 | +17.22 | +347.15 | +555.18 | +572.40 |
| 2026-08-18 | `HTHT` | 61 | $46.61 | $46.51 | -6.10 | $45.92 | -35.99 | -42.09 | +343.43 | +307.44 |
| 2026-08-18 | `NUAI` | 494 | $5.46 | $5.23 | -113.62 | $5.13 | -49.40 | -163.02 | +83.98 | +34.58 |
| 2026-08-18 | `SIDU` | 973 | $2.54 | $2.43 | -102.16 | $2.44 | +9.73 | -92.43 | -116.76 | -107.03 |
| 2026-08-19 | `DUOT` | 265 | $11.59 | $11.52 | -18.55 | $10.75 | -204.05 | -222.60 | +553.85 | +349.80 |
| 2026-08-19 | `HTHT` | 61 | $45.92 | $46.82 | +54.90 | $48.59 | +107.97 | +162.87 | +362.34 | +470.31 |
| 2026-08-19 | `NUAI` | 494 | $5.13 | $5.13 | +0.00 | $5.40 | +133.38 | +133.38 | +34.58 | +167.96 |
| 2026-08-19 | `SIDU` | 973 | $2.44 | $2.45 | +9.73 | $2.45 | +0.00 | +9.73 | -97.30 | -97.30 |
| 2026-08-20 | `DUOT` | 265 | $10.75 | $10.69 | -15.90 | $10.41 | -74.20 | -90.10 | +333.90 | +259.70 |
| 2026-08-20 | `HTHT` | 61 | $48.59 | $48.39 | -12.20 | $49.54 | +70.15 | +57.95 | +458.11 | +528.26 |
| 2026-08-20 | `NUAI` | 494 | $5.40 | $5.33 | -37.05 | $5.57 | +121.03 | +83.98 | +130.91 | +251.94 |
| 2026-08-20 | `SIDU` | 973 | $2.45 | $2.42 | -29.19 | $2.34 | -77.84 | -107.03 | -126.49 | -204.33 |
| 2026-08-21 | `DUOT` | 265 | $10.41 | $10.56 | +39.75 | — | +0.00 | +39.75 | +299.45 | — |
| 2026-08-21 | `HTHT` | 61 | $49.54 | $49.58 | +2.44 | — | +0.00 | +2.44 | +530.70 | — |
| 2026-08-21 | `NUAI` | 494 | $5.57 | $5.61 | +19.76 | — | +0.00 | +19.76 | +271.70 | — |
| 2026-08-21 | `SIDU` | 973 | $2.34 | $2.35 | +9.73 | — | +0.00 | +9.73 | -194.60 | — |
| 2026-08-21 | `PDD` | 60 | — | $90.03 | +0.00 | $88.38 | -99.00 | -99.00 | +0.00 | -99.00 |
| 2026-08-21 | `XPEV` | 441 | — | $12.29 | +0.00 | $12.19 | -44.10 | -44.10 | +0.00 | -44.10 |
| 2026-08-24 | `PDD` | 60 | $88.38 | $90.95 | +154.20 | $87.07 | -232.80 | -78.60 | +55.20 | -177.60 |
| 2026-08-24 | `XPEV` | 441 | $12.19 | $11.83 | -158.76 | $11.15 | -299.88 | -458.64 | -202.86 | -502.74 |
| 2026-08-25 | `PDD` | 60 | $87.07 | $86.65 | -25.20 | $87.75 | +66.00 | +40.80 | -202.80 | -136.80 |
| 2026-08-25 | `XPEV` | 441 | $11.15 | $11.19 | +15.44 | $11.60 | +183.01 | +198.45 | -487.30 | -304.29 |
| 2026-08-26 | `PDD` | 60 | $87.75 | $87.83 | +4.80 | $86.74 | -65.40 | -60.60 | -132.00 | -197.40 |
| 2026-08-26 | `XPEV` | 441 | $11.60 | $11.90 | +132.30 | $11.71 | -83.79 | +48.51 | -171.99 | -255.78 |
| 2026-08-27 | `PDD` | 60 | $86.74 | $86.48 | -15.60 | $84.69 | -107.40 | -123.00 | -213.00 | -320.40 |
| 2026-08-27 | `XPEV` | 441 | $11.71 | $11.51 | -88.20 | $11.33 | -79.38 | -167.58 | -343.98 | -423.36 |
| 2026-08-28 | `PDD` | 60 | $84.69 | $85.21 | +31.20 | — | +0.00 | +31.20 | -289.20 | — |
| 2026-08-28 | `XPEV` | 441 | $11.33 | $11.59 | +114.66 | — | +0.00 | +114.66 | -308.70 | — |
| 2026-08-28 | `LX` | 4415 | — | $1.16 | +0.00 | $1.18 | +88.30 | +88.30 | +0.00 | +88.30 |
| 2026-08-28 | `SAIC` | 39 | — | $129.46 | +0.00 | $125.96 | -136.50 | -136.50 | +0.00 | -136.50 |
| 2026-08-31 | `LX` | 4415 | $1.18 | $1.01 | -750.55 | $1.07 | +264.90 | -485.65 | -662.25 | -397.35 |
| 2026-08-31 | `SAIC` | 39 | $125.96 | $140.39 | +562.77 | $128.22 | -474.63 | +88.14 | +426.27 | -48.36 |
| 2026-09-01 | `LX` | 4415 | $1.07 | $1.01 | -264.90 | $0.88 | -565.12 | -830.02 | -662.25 | -1227.37 |
| 2026-09-01 | `SAIC` | 39 | $128.22 | $127.01 | -47.19 | $126.78 | -8.97 | -56.16 | -95.55 | -104.52 |
| 2026-09-02 | `LX` | 4415 | $0.88 | $0.91 | +105.96 | $0.82 | -375.28 | -269.32 | -1121.41 | -1496.68 |
| 2026-09-02 | `SAIC` | 39 | $126.78 | $126.10 | -26.52 | $127.18 | +42.12 | +15.60 | -131.04 | -88.92 |
| 2026-09-03 | `LX` | 4415 | $0.82 | $0.83 | +26.49 | $0.85 | +110.38 | +136.87 | -1470.19 | -1359.82 |
| 2026-09-03 | `SAIC` | 39 | $127.18 | $127.56 | +14.82 | $126.50 | -41.34 | -26.52 | -74.10 | -115.44 |
| 2026-09-04 | `LX` | 4415 | $0.85 | $0.86 | +26.49 | — | +0.00 | +26.49 | -1333.33 | — |
| 2026-09-04 | `SAIC` | 39 | $126.50 | $125.71 | -30.81 | — | +0.00 | -30.81 | -146.25 | — |
| 2026-09-04 | `ABM` | 92 | — | $46.79 | +0.00 | $47.05 | +23.92 | +23.92 | +0.00 | +23.92 |
| 2026-09-04 | `UNFI` | 98 | — | $43.80 | +0.00 | $43.93 | +12.74 | +12.74 | +0.00 | +12.74 |
| 2026-09-08 | `ABM` | 92 | $47.05 | $45.81 | -114.08 | $50.60 | +440.68 | +326.60 | -90.16 | +350.52 |
| 2026-09-08 | `UNFI` | 98 | $43.93 | $45.21 | +125.44 | $44.93 | -27.44 | +98.00 | +138.18 | +110.74 |
| 2026-09-09 | `ABM` | 92 | $50.60 | $50.60 | +0.00 | $49.74 | -79.12 | -79.12 | +350.52 | +271.40 |
| 2026-09-09 | `UNFI` | 98 | $44.93 | $45.50 | +55.86 | $44.61 | -87.22 | -31.36 | +166.60 | +79.38 |
| 2026-09-10 | `ABM` | 92 | $49.74 | $49.74 | +0.00 | $49.07 | -61.64 | -61.64 | +271.40 | +209.76 |
| 2026-09-10 | `UNFI` | 98 | $44.61 | $44.89 | +27.44 | $44.11 | -76.44 | -49.00 | +106.82 | +30.38 |
| 2026-09-11 | `ABM` | 92 | $49.07 | $49.48 | +37.72 | $49.43 | -4.60 | +33.12 | +247.48 | +242.88 |
| 2026-09-11 | `UNFI` | 98 | $44.11 | $45.00 | +87.22 | $44.45 | -53.90 | +33.32 | +117.60 | +63.70 |
| 2026-09-14 | `ABM` | 92 | $49.43 | $49.63 | +18.40 | — | +0.00 | +18.40 | +261.28 | — |
| 2026-09-14 | `UNFI` | 98 | $44.45 | $45.11 | +64.68 | — | +0.00 | +64.68 | +128.38 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `ALMU` | 328 | — | $13.75 | +0.00 | $13.43 | -104.96 | -104.96 | +0.00 | -104.96 |
| 2026-09-16 | `LEN` | 56 | — | $80.63 | +0.00 | $78.36 | -127.12 | -127.12 | +0.00 | -127.12 |
| 2026-09-17 | `ALMU` | 328 | $13.43 | $11.21 | -728.16 | $11.54 | +109.88 | -618.28 | -833.12 | -723.24 |
| 2026-09-17 | `LEN` | 56 | $78.36 | $81.00 | +147.84 | $79.70 | -72.80 | +75.04 | +20.72 | -52.08 |
| 2026-09-18 | `ALMU` | 328 | $11.54 | $11.64 | +31.16 | $12.72 | +355.88 | +387.04 | -692.08 | -336.20 |
| 2026-09-18 | `LEN` | 56 | $79.70 | $78.25 | -81.20 | $76.43 | -101.92 | -183.12 | -133.28 | -235.20 |
| 2026-09-21 | `ALMU` | 328 | $12.72 | $13.12 | +131.20 | $13.61 | +159.08 | +290.28 | -205.00 | -45.92 |
| 2026-09-21 | `LEN` | 56 | $76.43 | $76.98 | +30.80 | $78.08 | +61.60 | +92.40 | -204.40 | -142.80 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +29.79 | DUOT, HTHT, NUAI, SIDU | — | $2.06 | $10,005.27 | DUOT×265, HTHT×61, NUAI×494, SIDU×973 |
| 2026-08-17 | +2.25 | $2.06 | DUOT×265, HTHT×61, NUAI×494, SIDU×973 | $10,423.70 | +418.43 | +309.57 | — | — | $2.06 | $10,733.27 | DUOT×265, HTHT×61, NUAI×494, SIDU×973 |
| 2026-08-18 | -6.20 | $2.06 | DUOT×265, HTHT×61, NUAI×494, SIDU×973 | $10,841.31 | +108.04 | -58.44 | — | — | $2.06 | $10,782.87 | DUOT×265, HTHT×61, NUAI×494, SIDU×973 |
| 2026-08-19 | -7.20 | $2.06 | DUOT×265, HTHT×61, NUAI×494, SIDU×973 | $10,828.95 | +46.08 | +37.30 | — | — | $2.06 | $10,866.25 | DUOT×265, HTHT×61, NUAI×494, SIDU×973 |
| 2026-08-20 | +1.12 | $2.06 | DUOT×265, HTHT×61, NUAI×494, SIDU×973 | $10,771.91 | -94.34 | +39.14 | — | — | $2.06 | $10,811.05 | DUOT×265, HTHT×61, NUAI×494, SIDU×973 |
| 2026-08-21 | +3.25 | $2.06 | DUOT×265, HTHT×61, NUAI×494, SIDU×973 | $10,882.73 | +71.68 | -143.10 | PDD, XPEV | DUOT, HTHT, NUAI, SIDU | $28.28 | $10,706.87 | PDD×60, XPEV×441 |
| 2026-08-24 | -5.17 | $28.28 | PDD×60, XPEV×441 | $10,702.31 | -4.56 | -532.68 | — | — | $28.28 | $10,169.63 | PDD×60, XPEV×441 |
| 2026-08-25 | +1.80 | $28.28 | PDD×60, XPEV×441 | $10,159.87 | -9.76 | +249.01 | — | — | $28.28 | $10,408.88 | PDD×60, XPEV×441 |
| 2026-08-26 | +2.02 | $28.28 | PDD×60, XPEV×441 | $10,545.98 | +137.10 | -149.19 | — | — | $28.28 | $10,396.79 | PDD×60, XPEV×441 |
| 2026-08-27 | — | $28.28 | PDD×60, XPEV×441 | $10,292.99 | -103.80 | -186.78 | — | — | $28.28 | $10,106.21 | PDD×60, XPEV×441 |
| 2026-08-28 | +0.75 | $28.28 | PDD×60, XPEV×441 | $10,252.07 | +145.86 | -48.20 | LX, SAIC | PDD, XPEV | $14.65 | $10,136.79 | LX×4415, SAIC×39 |
| 2026-08-31 | -5.85 | $14.65 | LX×4415, SAIC×39 | $9,949.01 | -187.78 | -209.73 | — | — | $14.65 | $9,739.28 | LX×4415, SAIC×39 |
| 2026-09-01 | -6.30 | $14.65 | LX×4415, SAIC×39 | $9,427.19 | -312.09 | -574.09 | — | — | $14.65 | $8,853.10 | LX×4415, SAIC×39 |
| 2026-09-02 | -3.83 | $14.65 | LX×4415, SAIC×39 | $8,932.54 | +79.44 | -333.16 | — | — | $14.65 | $8,599.39 | LX×4415, SAIC×39 |
| 2026-09-03 | -0.90 | $14.65 | LX×4415, SAIC×39 | $8,640.70 | +41.31 | +69.04 | — | — | $14.65 | $8,709.73 | LX×4415, SAIC×39 |
| 2026-09-04 | +2.25 | $14.65 | LX×4415, SAIC×39 | $8,705.41 | -4.32 | +36.66 | ABM, UNFI | LX, SAIC | $49.74 | $8,683.48 | ABM×92, UNFI×98 |
| 2026-09-08 | -11.47 | $49.74 | ABM×92, UNFI×98 | $8,694.84 | +11.36 | +413.24 | — | — | $49.74 | $9,108.08 | ABM×92, UNFI×98 |
| 2026-09-09 | -13.95 | $49.74 | ABM×92, UNFI×98 | $9,163.94 | +55.86 | -166.34 | — | — | $49.74 | $8,997.60 | ABM×92, UNFI×98 |
| 2026-09-10 | -13.28 | $49.74 | ABM×92, UNFI×98 | $9,025.04 | +27.44 | -138.08 | — | — | $49.74 | $8,886.96 | ABM×92, UNFI×98 |
| 2026-09-11 | +0.50 | $49.74 | ABM×92, UNFI×98 | $9,011.90 | +124.94 | -58.50 | — | — | $49.74 | $8,953.40 | ABM×92, UNFI×98 |
| 2026-09-14 | -11.00 | $49.74 | ABM×92, UNFI×98 | $9,036.48 | +83.08 | +0.00 | — | ABM, UNFI | $9,031.82 | $9,031.82 | — |
| 2026-09-15 | -3.84 | $9,031.82 | — | $9,031.82 | +0.00 | +0.00 | — | — | $9,031.82 | $9,031.82 | — |
| 2026-09-16 | +5.30 | $9,031.82 | — | $9,031.82 | +0.00 | -232.08 | ALMU, LEN | — | $0.15 | $8,793.35 | ALMU×328, LEN×56 |
| 2026-09-17 | +7.38 | $0.15 | ALMU×328, LEN×56 | $8,213.03 | -580.32 | +37.08 | — | — | $0.15 | $8,250.11 | ALMU×328, LEN×56 |
| 2026-09-18 | +4.86 | $0.15 | ALMU×328, LEN×56 | $8,200.07 | -50.04 | +253.96 | — | — | $0.15 | $8,454.03 | ALMU×328, LEN×56 |
| 2026-09-21 | +12.87 | $0.15 | ALMU×328, LEN×56 | $8,616.03 | +162.00 | +220.68 | — | — | $0.15 | $8,836.71 | ALMU×328, LEN×56 |

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
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.06 | ▲ close $10,733.27 vs 09:30 $10,423.70 (session +309.57) | 16:00 close · cash $2.06 · equity $10,733.27 vs 09:30 $10,423.70 (+309.57; session marks +309.57) · 4 name(s) marked open→close (per-name table). DUOT×265 09:30 $10.35 → close $10.28 -18.55; HTHT×61 09:30 $45.49 → close $46.61 +68.32; NUAI×494 09:30 $5.20 → close $5.46 +128.44; SIDU×973 09:30 $2.40 → close $2.54 +131.36 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.06 | ▲ 09:30 equity $10,841.31 vs yday $10,733.27 (+108.04) | 09:30 open · cash $2.06 (unchanged overnight, no fees) · equity $10,841.31 vs prior close $10,733.27 (+108.04) · 4 name(s) re-marked at the open (per-name table). DUOT×265 yday $10.28 → 09:30 $11.53 +329.93; HTHT×61 yday $46.61 → 09:30 $46.51 -6.10; NUAI×494 yday $5.46 → 09:30 $5.23 -113.62; SIDU×973 yday $2.54 → 09:30 $2.43 -102.16 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.06 | ▼ close $10,782.87 vs 09:30 $10,841.31 (session -58.44) | 16:00 close · cash $2.06 · equity $10,782.87 vs 09:30 $10,841.31 (-58.44; session marks -58.44) · 4 name(s) marked open→close (per-name table). DUOT×265 09:30 $11.53 → close $11.59 +17.22; HTHT×61 09:30 $46.51 → close $45.92 -35.99; NUAI×494 09:30 $5.23 → close $5.13 -49.40; SIDU×973 09:30 $2.43 → close $2.44 +9.73 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.06 | ▲ 09:30 equity $10,828.95 vs yday $10,782.87 (+46.08) | 09:30 open · cash $2.06 (unchanged overnight, no fees) · equity $10,828.95 vs prior close $10,782.87 (+46.08) · 4 name(s) re-marked at the open (per-name table). DUOT×265 yday $11.59 → 09:30 $11.52 -18.55; HTHT×61 yday $45.92 → 09:30 $46.82 +54.90; NUAI×494 yday $5.13 → 09:30 $5.13 +0.00; SIDU×973 yday $2.44 → 09:30 $2.45 +9.73 | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.06 | ▲ close $10,866.25 vs 09:30 $10,828.95 (session +37.30) | 16:00 close · cash $2.06 · equity $10,866.25 vs 09:30 $10,828.95 (+37.30; session marks +37.30) · 4 name(s) marked open→close (per-name table). DUOT×265 09:30 $11.52 → close $10.75 -204.05; HTHT×61 09:30 $46.82 → close $48.59 +107.97; NUAI×494 09:30 $5.13 → close $5.40 +133.38; SIDU×973 09:30 $2.45 → close $2.45 +0.00 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.06 | ▼ 09:30 equity $10,771.91 vs yday $10,866.25 (-94.34) | 09:30 open · cash $2.06 (unchanged overnight, no fees) · equity $10,771.91 vs prior close $10,866.25 (-94.34) · 4 name(s) re-marked at the open (per-name table). DUOT×265 yday $10.75 → 09:30 $10.69 -15.90; HTHT×61 yday $48.59 → 09:30 $48.39 -12.20; NUAI×494 yday $5.40 → 09:30 $5.33 -37.05; SIDU×973 yday $2.45 → 09:30 $2.42 -29.19 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.06 | ▲ close $10,811.05 vs 09:30 $10,771.91 (session +39.14) | 16:00 close · cash $2.06 · equity $10,811.05 vs 09:30 $10,771.91 (+39.14; session marks +39.14) · 4 name(s) marked open→close (per-name table). DUOT×265 09:30 $10.69 → close $10.41 -74.20; HTHT×61 09:30 $48.39 → close $49.54 +70.15; NUAI×494 09:30 $5.33 → close $5.57 +121.03; SIDU×973 09:30 $2.42 → close $2.34 -77.84 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.06 | ▲ 09:30 equity $10,882.73 vs yday $10,811.05 (+71.68) | 09:30 open · cash $2.06 (unchanged overnight, no fees) · equity $10,882.73 vs prior close $10,811.05 (+71.68) · 4 name(s) re-marked at the open (per-name table). DUOT×265 yday $10.41 → 09:30 $10.56 +39.75; HTHT×61 yday $49.54 → 09:30 $49.58 +2.44; NUAI×494 yday $5.57 → 09:30 $5.61 +19.76; SIDU×973 yday $2.34 → 09:30 $2.35 +9.73 | — |
| 2026-08-21 09:30 ET | **SELL** | `DUOT` | 265 | $10.56 | $3.48 | $+292.55 | $2,796.98 | ▲ +292.55 after sell → book $10,879.25; vs 09:30 mark -3.48 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `HTHT` | 61 | $49.58 | $2.21 | $+526.32 | $5,819.15 | ▲ +526.32 after sell → book $10,877.04; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `NUAI` | 494 | $5.61 | $6.48 | $+258.85 | $8,584.02 | ▲ +258.85 after sell → book $10,870.57; vs 09:30 mark -6.47 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `SIDU` | 973 | $2.35 | $12.73 | $-219.88 | $10,857.83 | ▼ -219.88 after sell → book $10,857.83; vs 09:30 mark -12.74 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `PDD` | 60 | $90.03 | $2.17 | — | $5,453.86 | — | baseline list, no extra gate; list overnight,overnight_mega; 🔵; ret5=+6.4; leftover $5428.92 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XPEV` | 441 | $12.29 | $5.69 | — | $28.28 | — | baseline list, no extra gate; list overnight; ret5=+1.9; leftover $5428.92 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.28 | ▼ close $10,706.87 vs 09:30 $10,882.73 (session -143.10) | 16:00 close · cash $28.28 · equity $10,706.87 vs 09:30 $10,882.73 (-175.86; session marks -143.10) · 2 name(s) marked open→close (per-name table). PDD×60 09:30 $90.03 → close $88.38 -99.00; XPEV×441 09:30 $12.29 → close $12.19 -44.10 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.28 | ▼ 09:30 equity $10,702.31 vs yday $10,706.87 (-4.56) | 09:30 open · cash $28.28 (unchanged overnight, no fees) · equity $10,702.31 vs prior close $10,706.87 (-4.56) · 2 name(s) re-marked at the open (per-name table). PDD×60 yday $88.38 → 09:30 $90.95 +154.20; XPEV×441 yday $12.19 → 09:30 $11.83 -158.76 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.28 | ▼ close $10,169.63 vs 09:30 $10,702.31 (session -532.68) | 16:00 close · cash $28.28 · equity $10,169.63 vs 09:30 $10,702.31 (-532.68; session marks -532.68) · 2 name(s) marked open→close (per-name table). PDD×60 09:30 $90.95 → close $87.07 -232.80; XPEV×441 09:30 $11.83 → close $11.15 -299.88 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.28 | ▼ 09:30 equity $10,159.87 vs yday $10,169.63 (-9.76) | 09:30 open · cash $28.28 (unchanged overnight, no fees) · equity $10,159.87 vs prior close $10,169.63 (-9.76) · 2 name(s) re-marked at the open (per-name table). PDD×60 yday $87.07 → 09:30 $86.65 -25.20; XPEV×441 yday $11.15 → 09:30 $11.19 +15.44 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.28 | ▲ close $10,408.88 vs 09:30 $10,159.87 (session +249.01) | 16:00 close · cash $28.28 · equity $10,408.88 vs 09:30 $10,159.87 (+249.01; session marks +249.01) · 2 name(s) marked open→close (per-name table). PDD×60 09:30 $86.65 → close $87.75 +66.00; XPEV×441 09:30 $11.19 → close $11.60 +183.01 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.28 | ▲ 09:30 equity $10,545.98 vs yday $10,408.88 (+137.10) | 09:30 open · cash $28.28 (unchanged overnight, no fees) · equity $10,545.98 vs prior close $10,408.88 (+137.10) · 2 name(s) re-marked at the open (per-name table). PDD×60 yday $87.75 → 09:30 $87.83 +4.80; XPEV×441 yday $11.60 → 09:30 $11.90 +132.30 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.28 | ▼ close $10,396.79 vs 09:30 $10,545.98 (session -149.19) | 16:00 close · cash $28.28 · equity $10,396.79 vs 09:30 $10,545.98 (-149.19; session marks -149.19) · 2 name(s) marked open→close (per-name table). PDD×60 09:30 $87.83 → close $86.74 -65.40; XPEV×441 09:30 $11.90 → close $11.71 -83.79 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.28 | ▼ 09:30 equity $10,292.99 vs yday $10,396.79 (-103.80) | 09:30 open · cash $28.28 (unchanged overnight, no fees) · equity $10,292.99 vs prior close $10,396.79 (-103.80) · 2 name(s) re-marked at the open (per-name table). PDD×60 yday $86.74 → 09:30 $86.48 -15.60; XPEV×441 yday $11.71 → 09:30 $11.51 -88.20 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.28 | ▼ close $10,106.21 vs 09:30 $10,292.99 (session -186.78) | 16:00 close · cash $28.28 · equity $10,106.21 vs 09:30 $10,292.99 (-186.78; session marks -186.78) · 2 name(s) marked open→close (per-name table). PDD×60 09:30 $86.48 → close $84.69 -107.40; XPEV×441 09:30 $11.51 → close $11.33 -79.38 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.28 | ▲ 09:30 equity $10,252.07 vs yday $10,106.21 (+145.86) | 09:30 open · cash $28.28 (unchanged overnight, no fees) · equity $10,252.07 vs prior close $10,106.21 (+145.86) · 2 name(s) re-marked at the open (per-name table). PDD×60 yday $84.69 → 09:30 $85.21 +31.20; XPEV×441 yday $11.33 → 09:30 $11.59 +114.66 | — |
| 2026-08-28 09:30 ET | **SELL** | `PDD` | 60 | $85.21 | $2.22 | $-293.59 | $5,138.66 | ▼ -293.59 after sell → book $10,249.85; vs 09:30 mark -2.22 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `XPEV` | 441 | $11.59 | $5.80 | $-320.19 | $10,244.05 | ▼ -320.19 after sell → book $10,244.05; vs 09:30 mark -5.80 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `LX` | 4415 | $1.16 | $56.95 | — | $5,065.70 | — | baseline list, no extra gate; list overnight; ret5=-13.8; leftover $5122.03 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SAIC` | 39 | $129.46 | $2.11 | — | $14.65 | — | baseline list, no extra gate; list overnight; ret5=+2.1; leftover $5122.03 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.65 | ▼ close $10,136.79 vs 09:30 $10,252.07 (session -48.20) | 16:00 close · cash $14.65 · equity $10,136.79 vs 09:30 $10,252.07 (-115.28; session marks -48.20) · 2 name(s) marked open→close (per-name table). LX×4415 09:30 $1.16 → close $1.18 +88.30; SAIC×39 09:30 $129.46 → close $125.96 -136.50 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.65 | ▼ 09:30 equity $9,949.01 vs yday $10,136.79 (-187.78) | 09:30 open · cash $14.65 (unchanged overnight, no fees) · equity $9,949.01 vs prior close $10,136.79 (-187.78) · 2 name(s) re-marked at the open (per-name table). LX×4415 yday $1.18 → 09:30 $1.01 -750.55; SAIC×39 yday $125.96 → 09:30 $140.39 +562.77 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.65 | ▼ close $9,739.28 vs 09:30 $9,949.01 (session -209.73) | 16:00 close · cash $14.65 · equity $9,739.28 vs 09:30 $9,949.01 (-209.73; session marks -209.73) · 2 name(s) marked open→close (per-name table). LX×4415 09:30 $1.01 → close $1.07 +264.90; SAIC×39 09:30 $140.39 → close $128.22 -474.63 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.65 | ▼ 09:30 equity $9,427.19 vs yday $9,739.28 (-312.09) | 09:30 open · cash $14.65 (unchanged overnight, no fees) · equity $9,427.19 vs prior close $9,739.28 (-312.09) · 2 name(s) re-marked at the open (per-name table). LX×4415 yday $1.07 → 09:30 $1.01 -264.90; SAIC×39 yday $128.22 → 09:30 $127.01 -47.19 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.65 | ▼ close $8,853.10 vs 09:30 $9,427.19 (session -574.09) | 16:00 close · cash $14.65 · equity $8,853.10 vs 09:30 $9,427.19 (-574.09; session marks -574.09) · 2 name(s) marked open→close (per-name table). LX×4415 09:30 $1.01 → close $0.88 -565.12; SAIC×39 09:30 $127.01 → close $126.78 -8.97 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.65 | ▲ 09:30 equity $8,932.54 vs yday $8,853.10 (+79.44) | 09:30 open · cash $14.65 (unchanged overnight, no fees) · equity $8,932.54 vs prior close $8,853.10 (+79.44) · 2 name(s) re-marked at the open (per-name table). LX×4415 yday $0.88 → 09:30 $0.91 +105.96; SAIC×39 yday $126.78 → 09:30 $126.10 -26.52 | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.65 | ▼ close $8,599.39 vs 09:30 $8,932.54 (session -333.16) | 16:00 close · cash $14.65 · equity $8,599.39 vs 09:30 $8,932.54 (-333.15; session marks -333.16) · 2 name(s) marked open→close (per-name table). LX×4415 09:30 $0.91 → close $0.82 -375.28; SAIC×39 09:30 $126.10 → close $127.18 +42.12 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.65 | ▲ 09:30 equity $8,640.70 vs yday $8,599.39 (+41.31) | 09:30 open · cash $14.65 (unchanged overnight, no fees) · equity $8,640.70 vs prior close $8,599.39 (+41.31) · 2 name(s) re-marked at the open (per-name table). LX×4415 yday $0.82 → 09:30 $0.83 +26.49; SAIC×39 yday $127.18 → 09:30 $127.56 +14.82 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.65 | ▲ close $8,709.73 vs 09:30 $8,640.70 (session +69.04) | 16:00 close · cash $14.65 · equity $8,709.73 vs 09:30 $8,640.70 (+69.03; session marks +69.04) · 2 name(s) marked open→close (per-name table). LX×4415 09:30 $0.83 → close $0.85 +110.38; SAIC×39 09:30 $127.56 → close $126.50 -41.34 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.65 | ▼ 09:30 equity $8,705.41 vs yday $8,709.73 (-4.32) | 09:30 open · cash $14.65 (unchanged overnight, no fees) · equity $8,705.41 vs prior close $8,709.73 (-4.32) · 2 name(s) re-marked at the open (per-name table). LX×4415 yday $0.85 → 09:30 $0.86 +26.49; SAIC×39 yday $126.50 → 09:30 $125.71 -30.81 | — |
| 2026-09-04 09:30 ET | **SELL** | `LX` | 4415 | $0.86 | $51.89 | $-1442.17 | $3,750.83 | ▼ -1,442.17 after sell → book $8,653.52; vs 09:30 mark -51.89 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SAIC` | 39 | $125.71 | $2.16 | $-150.51 | $8,651.37 | ▼ -150.51 after sell → book $8,651.37; vs 09:30 mark -2.15 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `ABM` | 92 | $46.79 | $2.27 | — | $4,344.42 | — | baseline list, no extra gate; list overnight; ret5=+0.2; leftover $4325.68 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `UNFI` | 98 | $43.80 | $2.28 | — | $49.74 | — | baseline list, no extra gate; list overnight; ret5=-7.7; leftover $4325.68 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.74 | ▲ close $8,683.48 vs 09:30 $8,705.41 (session +36.66) | 16:00 close · cash $49.74 · equity $8,683.48 vs 09:30 $8,705.41 (-21.93; session marks +36.66) · 2 name(s) marked open→close (per-name table). ABM×92 09:30 $46.79 → close $47.05 +23.92; UNFI×98 09:30 $43.80 → close $43.93 +12.74 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.74 | ▲ 09:30 equity $8,694.84 vs yday $8,683.48 (+11.36) | 09:30 open · cash $49.74 (unchanged overnight, no fees) · equity $8,694.84 vs prior close $8,683.48 (+11.36) · 2 name(s) re-marked at the open (per-name table). ABM×92 yday $47.05 → 09:30 $45.81 -114.08; UNFI×98 yday $43.93 → 09:30 $45.21 +125.44 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.74 | ▲ close $9,108.08 vs 09:30 $8,694.84 (session +413.24) | 16:00 close · cash $49.74 · equity $9,108.08 vs 09:30 $8,694.84 (+413.24; session marks +413.24) · 2 name(s) marked open→close (per-name table). ABM×92 09:30 $45.81 → close $50.60 +440.68; UNFI×98 09:30 $45.21 → close $44.93 -27.44 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.74 | ▲ 09:30 equity $9,163.94 vs yday $9,108.08 (+55.86) | 09:30 open · cash $49.74 (unchanged overnight, no fees) · equity $9,163.94 vs prior close $9,108.08 (+55.86) · 2 name(s) re-marked at the open (per-name table). ABM×92 yday $50.60 → 09:30 $50.60 +0.00; UNFI×98 yday $44.93 → 09:30 $45.50 +55.86 | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.74 | ▼ close $8,997.60 vs 09:30 $9,163.94 (session -166.34) | 16:00 close · cash $49.74 · equity $8,997.60 vs 09:30 $9,163.94 (-166.34; session marks -166.34) · 2 name(s) marked open→close (per-name table). ABM×92 09:30 $50.60 → close $49.74 -79.12; UNFI×98 09:30 $45.50 → close $44.61 -87.22 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.74 | ▲ 09:30 equity $9,025.04 vs yday $8,997.60 (+27.44) | 09:30 open · cash $49.74 (unchanged overnight, no fees) · equity $9,025.04 vs prior close $8,997.60 (+27.44) · 2 name(s) re-marked at the open (per-name table). ABM×92 yday $49.74 → 09:30 $49.74 +0.00; UNFI×98 yday $44.61 → 09:30 $44.89 +27.44 | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.74 | ▼ close $8,886.96 vs 09:30 $9,025.04 (session -138.08) | 16:00 close · cash $49.74 · equity $8,886.96 vs 09:30 $9,025.04 (-138.08; session marks -138.08) · 2 name(s) marked open→close (per-name table). ABM×92 09:30 $49.74 → close $49.07 -61.64; UNFI×98 09:30 $44.89 → close $44.11 -76.44 | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.74 | ▲ 09:30 equity $9,011.90 vs yday $8,886.96 (+124.94) | 09:30 open · cash $49.74 (unchanged overnight, no fees) · equity $9,011.90 vs prior close $8,886.96 (+124.94) · 2 name(s) re-marked at the open (per-name table). ABM×92 yday $49.07 → 09:30 $49.48 +37.72; UNFI×98 yday $44.11 → 09:30 $45.00 +87.22 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.74 | ▼ close $8,953.40 vs 09:30 $9,011.90 (session -58.50) | 16:00 close · cash $49.74 · equity $8,953.40 vs 09:30 $9,011.90 (-58.50; session marks -58.50) · 2 name(s) marked open→close (per-name table). ABM×92 09:30 $49.48 → close $49.43 -4.60; UNFI×98 09:30 $45.00 → close $44.45 -53.90 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.74 | ▲ 09:30 equity $9,036.48 vs yday $8,953.40 (+83.08) | 09:30 open · cash $49.74 (unchanged overnight, no fees) · equity $9,036.48 vs prior close $8,953.40 (+83.08) · 2 name(s) re-marked at the open (per-name table). ABM×92 yday $49.43 → 09:30 $49.63 +18.40; UNFI×98 yday $44.45 → 09:30 $45.11 +64.68 | — |
| 2026-09-14 09:30 ET | **SELL** | `ABM` | 92 | $49.63 | $2.32 | $+256.70 | $4,613.38 | ▲ +256.70 after sell → book $9,034.16; vs 09:30 mark -2.32 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `UNFI` | 98 | $45.11 | $2.34 | $+123.76 | $9,031.82 | ▲ +123.76 after sell → book $9,031.82; vs 09:30 mark -2.34 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,031.82 | ▲ close $9,031.82 vs 09:30 $9,036.48 (session +0.00) | 16:00 close · cash $9,031.82 · no lots left · equity $9,031.82. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,031.82 | ▲ 09:30 equity $9,031.82 vs yday $9,031.82 (+0.00) | 09:30 open · cash $9,031.82 · no holdings · equity $9,031.82 vs prior close $9,031.82 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,031.82 | ▲ close $9,031.82 vs 09:30 $9,031.82 (session +0.00) | 16:00 close · cash $9,031.82 · no lots left · equity $9,031.82. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,031.82 | ▲ 09:30 equity $9,031.82 vs yday $9,031.82 (+0.00) | 09:30 open · cash $9,031.82 · no holdings · equity $9,031.82 vs prior close $9,031.82 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `ALMU` | 328 | $13.75 | $4.23 | — | $4,517.59 | — | baseline list, no extra gate; list overnight; 🔵; ret5=-1.4; leftover $4515.91 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `LEN` | 56 | $80.63 | $2.16 | — | $0.15 | — | baseline list, no extra gate; list overnight; ret5=-0.4; leftover $4515.91 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🔴 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.15 | ▼ close $8,793.35 vs 09:30 $9,031.82 (session -232.08) | 16:00 close · cash $0.15 · equity $8,793.35 vs 09:30 $9,031.82 (-238.47; session marks -232.08) · 2 name(s) marked open→close (per-name table). ALMU×328 09:30 $13.75 → close $13.43 -104.96; LEN×56 09:30 $80.63 → close $78.36 -127.12 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.15 | ▼ 09:30 equity $8,213.03 vs yday $8,793.35 (-580.32) | 09:30 open · cash $0.15 (unchanged overnight, no fees) · equity $8,213.03 vs prior close $8,793.35 (-580.32) · 2 name(s) re-marked at the open (per-name table). ALMU×328 yday $13.43 → 09:30 $11.21 -728.16; LEN×56 yday $78.36 → 09:30 $81.00 +147.84 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.15 | ▲ close $8,250.11 vs 09:30 $8,213.03 (session +37.08) | 16:00 close · cash $0.15 · equity $8,250.11 vs 09:30 $8,213.03 (+37.08; session marks +37.08) · 2 name(s) marked open→close (per-name table). ALMU×328 09:30 $11.21 → close $11.54 +109.88; LEN×56 09:30 $81.00 → close $79.70 -72.80 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.15 | ▼ 09:30 equity $8,200.07 vs yday $8,250.11 (-50.04) | 09:30 open · cash $0.15 (unchanged overnight, no fees) · equity $8,200.07 vs prior close $8,250.11 (-50.04) · 2 name(s) re-marked at the open (per-name table). ALMU×328 yday $11.54 → 09:30 $11.64 +31.16; LEN×56 yday $79.70 → 09:30 $78.25 -81.20 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.15 | ▲ close $8,454.03 vs 09:30 $8,200.07 (session +253.96) | 16:00 close · cash $0.15 · equity $8,454.03 vs 09:30 $8,200.07 (+253.96; session marks +253.96) · 2 name(s) marked open→close (per-name table). ALMU×328 09:30 $11.64 → close $12.72 +355.88; LEN×56 09:30 $78.25 → close $76.43 -101.92 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.15 | ▲ 09:30 equity $8,616.03 vs yday $8,454.03 (+162.00) | 09:30 open · cash $0.15 (unchanged overnight, no fees) · equity $8,616.03 vs prior close $8,454.03 (+162.00) · 2 name(s) re-marked at the open (per-name table). ALMU×328 yday $12.72 → 09:30 $13.12 +131.20; LEN×56 yday $76.43 → 09:30 $76.98 +30.80 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.15 | ▲ close $8,836.71 vs 09:30 $8,616.03 (session +220.68) | 16:00 close · cash $0.15 · equity $8,836.71 vs 09:30 $8,616.03 (+220.68; session marks +220.68) · 2 name(s) marked open→close (per-name table). ALMU×328 09:30 $13.12 → close $13.61 +159.08; LEN×56 09:30 $76.98 → close $78.08 +61.60 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `DUOT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `HTHT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `NUAI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `SIDU` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `AS` | cash | leftover split 0.26 < 1 share @ 32.88 |
| 2026-08-17 | `BIDU` | cash | leftover split 0.26 < 1 share @ 102.83 |
| 2026-08-17 | `FN` | cash | leftover split 0.26 < 1 share @ 583.15 |
| 2026-08-17 | `HD` | cash | leftover split 0.26 < 1 share @ 334.71 |
| 2026-08-17 | `HSAI` | cash | leftover split 0.26 < 1 share @ 18.32 |
| 2026-08-17 | `IQ` | cash | leftover split 0.26 < 1 share @ 1.35 |
| 2026-08-17 | `KLAR` | cash | leftover split 0.26 < 1 share @ 20.67 |
| 2026-08-17 | `PONY` | cash | leftover split 0.26 < 1 share @ 8.16 |
| 2026-08-18 | `DUOT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `HTHT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `NUAI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `SIDU` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ZIM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ADI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `DVLT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `EL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `JKHY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KEYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `LOW` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DUOT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `HTHT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `NUAI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `SIDU` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AEG` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ALVO` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ATAT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ATHM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BABA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BILL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BULL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `DUOT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `HTHT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `NUAI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `SIDU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BEKE` | cash | leftover split 0.41 < 1 share @ 17.04 |
| 2026-08-20 | `BJ` | cash | leftover split 0.41 < 1 share @ 88.91 |
| 2026-08-20 | `BKE` | cash | leftover split 0.41 < 1 share @ 42.60 |
| 2026-08-20 | `FLO` | cash | leftover split 0.41 < 1 share @ 7.43 |
| 2026-08-20 | `ROST` | cash | leftover split 0.41 < 1 share @ 229.55 |
| 2026-08-24 | `PDD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `XPEV` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `BMO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BNS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BZ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DKS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GRRR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SHMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `PDD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `XPEV` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ANF` | cash | leftover split 3.54 < 1 share @ 112.17 |
| 2026-08-25 | `BBWI` | cash | leftover split 3.54 < 1 share @ 19.16 |
| 2026-08-25 | `BOX` | cash | leftover split 3.54 < 1 share @ 33.33 |
| 2026-08-25 | `DCI` | cash | leftover split 3.54 < 1 share @ 93.64 |
| 2026-08-25 | `DY` | cash | leftover split 3.54 < 1 share @ 390.22 |
| 2026-08-25 | `FSCO` | cash | leftover split 3.54 < 1 share @ 5.10 |
| 2026-08-25 | `HEI` | cash | leftover split 3.54 < 1 share @ 357.15 |
| 2026-08-25 | `INTU` | cash | leftover split 3.54 < 1 share @ 364.35 |
| 2026-08-26 | `PDD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `XPEV` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `STDN` | cash | leftover split 3.54 < 1 share @ 13.95 |
| 2026-08-26 | `A` | cash | leftover split 3.54 < 1 share @ 152.45 |
| 2026-08-26 | `BBY` | cash | leftover split 3.54 < 1 share @ 85.19 |
| 2026-08-26 | `BILI` | cash | leftover split 3.54 < 1 share @ 16.22 |
| 2026-08-26 | `CM` | cash | leftover split 3.54 < 1 share @ 118.50 |
| 2026-08-26 | `CMBT` | cash | leftover split 3.54 < 1 share @ 17.91 |
| 2026-08-26 | `CRM` | cash | leftover split 3.54 < 1 share @ 199.94 |
| 2026-08-26 | `CRWD` | cash | leftover split 3.54 < 1 share @ 182.75 |
| 2026-08-27 | `PDD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `XPEV` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `GAP` | cash | leftover split 3.54 < 1 share @ 20.75 |
| 2026-08-27 | `ADSK` | cash | leftover split 3.54 < 1 share @ 261.47 |
| 2026-08-27 | `AFRM` | cash | leftover split 3.54 < 1 share @ 76.90 |
| 2026-08-27 | `BBAR` | cash | leftover split 3.54 < 1 share @ 14.96 |
| 2026-08-27 | `CHA` | cash | leftover split 3.54 < 1 share @ 10.54 |
| 2026-08-27 | `ESTC` | cash | leftover split 3.54 < 1 share @ 82.65 |
| 2026-08-27 | `HAFN` | cash | leftover split 3.54 < 1 share @ 7.91 |
| 2026-08-27 | `IREN` | cash | leftover split 3.54 < 1 share @ 40.65 |
| 2026-08-31 | `LX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `SAIC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `YEXT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MDT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MMED` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NIO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RZLV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SSL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `LX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `SAIC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `GTLB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BF-B` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CRDO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DELL` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FCEL` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PANW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `LX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `SAIC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `AI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVGO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CHPT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CIEN` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CPB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FIVE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HPE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MOMO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `LX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `SAIC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `AMBA` | cash | leftover split 1.83 < 1 share @ 66.61 |
| 2026-09-03 | `ASAN` | cash | leftover split 1.83 < 1 share @ 10.16 |
| 2026-09-03 | `DOCU` | cash | leftover split 1.83 < 1 share @ 67.06 |
| 2026-09-03 | `DOMO` | cash | leftover split 1.83 < 1 share @ 3.78 |
| 2026-09-03 | `GWRE` | cash | leftover split 1.83 < 1 share @ 198.00 |
| 2026-09-03 | `IOT` | cash | leftover split 1.83 < 1 share @ 37.69 |
| 2026-09-03 | `LULU` | cash | leftover split 1.83 < 1 share @ 121.15 |
| 2026-09-03 | `MAMA` | cash | leftover split 1.83 < 1 share @ 15.62 |
| 2026-09-08 | `ABM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `UNFI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `ASO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BRZE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CGNT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHWY` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GME` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ABM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `UNFI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `AEO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AVAV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `COO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `M` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAVN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `WLTH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `ABM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `UNFI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `ADBE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CPRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DSGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `KR` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LPTH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `REF` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `RH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-11 | `ABM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `UNFI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-14 | `FPS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HITI` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `PLAY` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `TCOM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `ALMU` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `LEN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `ALMU` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `LEN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `ALMU` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `LEN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `ABVX` | cash | leftover split 0.15 < 1 share @ 105.72 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ALMU` | 328 | 2026-09-16 @ $13.75 | baseline list, no extra gate; list overnight; 🔵; ret5=-1.4; leftover $4515.91 |
| `LEN` | 56 | 2026-09-16 @ $80.63 | baseline list, no extra gate; list overnight; ret5=-0.4; leftover $4515.91 |
