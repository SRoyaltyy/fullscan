# Factor mine action — `union_earn_react_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ earn_react, no 🚨

Cash book **+1.77%** ($10,177) · signal-only (no cash/fees) was -1.04%. Starts YES **7/19**. Fills 128 · skips 32 · realized $+176.81.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the name is in an earnings-reaction window (just reported, we are trading the reaction — not today's print).
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
- **Gate** `earn_react=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,176.81.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `INO` | 6172 | — | $0.81 | +0.00 | $0.90 | +555.48 | +555.48 | +0.00 | +555.48 |
| 2026-08-13 | `VOR` | 223 | — | $22.01 | +0.00 | $23.29 | +285.44 | +285.44 | +0.00 | +285.44 |
| 2026-08-14 | `INO` | 6172 | $0.90 | $0.93 | +185.16 | — | +0.00 | +185.16 | +740.64 | — |
| 2026-08-14 | `VOR` | 223 | $23.29 | $23.33 | +8.92 | — | +0.00 | +8.92 | +294.36 | — |
| 2026-08-14 | `NMAX` | 137 | — | $9.89 | +0.00 | $10.87 | +133.57 | +133.57 | +0.00 | +133.57 |
| 2026-08-14 | `AIRJ` | 246 | — | $5.51 | +0.00 | $6.04 | +130.38 | +130.38 | +0.00 | +130.38 |
| 2026-08-14 | `AMAT` | 2 | — | $499.40 | +0.00 | $507.18 | +15.56 | +15.56 | +0.00 | +15.56 |
| 2026-08-14 | `AMPG` | 311 | — | $4.37 | +0.00 | $4.00 | -116.00 | -116.00 | +0.00 | -116.00 |
| 2026-08-14 | `BRUN` | 51 | — | $26.25 | +0.00 | $22.93 | -169.07 | -169.07 | +0.00 | -169.07 |
| 2026-08-14 | `BZAI` | 1776 | — | $0.77 | +0.00 | $0.59 | -307.25 | -307.25 | +0.00 | -307.25 |
| 2026-08-14 | `DEFT` | 2894 | — | $0.47 | +0.00 | $0.49 | +54.99 | +54.99 | +0.00 | +54.99 |
| 2026-08-14 | `DGXX` | 347 | — | $3.92 | +0.00 | $3.97 | +17.35 | +17.35 | +0.00 | +17.35 |
| 2026-08-17 | `NMAX` | 137 | $10.87 | $10.97 | +13.70 | — | +0.00 | +13.70 | +147.28 | — |
| 2026-08-17 | `AIRJ` | 246 | $6.04 | $6.22 | +44.28 | — | +0.00 | +44.28 | +174.66 | — |
| 2026-08-17 | `AMAT` | 2 | $507.18 | $517.45 | +20.53 | — | +0.00 | +20.53 | +36.09 | — |
| 2026-08-17 | `AMPG` | 311 | $4.00 | $4.09 | +29.54 | — | +0.00 | +29.54 | -86.46 | — |
| 2026-08-17 | `BRUN` | 51 | $22.93 | $23.00 | +3.57 | — | +0.00 | +3.57 | -165.50 | — |
| 2026-08-17 | `BZAI` | 1776 | $0.59 | $0.55 | -72.82 | — | +0.00 | -72.82 | -380.06 | — |
| 2026-08-17 | `DEFT` | 2894 | $0.49 | $0.47 | -40.52 | — | +0.00 | -40.52 | +14.47 | — |
| 2026-08-17 | `DGXX` | 347 | $3.97 | $3.96 | -3.47 | — | +0.00 | -3.47 | +13.88 | — |
| 2026-08-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AAP` | 28 | — | $46.85 | +0.00 | $42.39 | -124.88 | -124.88 | +0.00 | -124.88 |
| 2026-08-20 | `AEG` | 145 | — | $9.01 | +0.00 | $9.01 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `ALVO` | 338 | — | $3.89 | +0.00 | $4.27 | +128.44 | +128.44 | +0.00 | +128.44 |
| 2026-08-20 | `ATAT` | 38 | — | $34.05 | +0.00 | $34.25 | +7.60 | +7.60 | +0.00 | +7.60 |
| 2026-08-20 | `ATHM` | 58 | — | $22.44 | +0.00 | $22.12 | -18.56 | -18.56 | +0.00 | -18.56 |
| 2026-08-20 | `BABA` | 10 | — | $123.47 | +0.00 | $130.53 | +70.60 | +70.60 | +0.00 | +70.60 |
| 2026-08-20 | `BILL` | 26 | — | $49.00 | +0.00 | $47.40 | -41.60 | -41.60 | +0.00 | -41.60 |
| 2026-08-20 | `BULL` | 132 | — | $9.94 | +0.00 | $8.85 | -143.88 | -143.88 | +0.00 | -143.88 |
| 2026-08-21 | `AAP` | 28 | $42.39 | $42.41 | +0.56 | — | +0.00 | +0.56 | -124.32 | — |
| 2026-08-21 | `AEG` | 145 | $9.01 | $9.04 | +4.35 | — | +0.00 | +4.35 | +4.35 | — |
| 2026-08-21 | `ALVO` | 338 | $4.27 | $4.32 | +16.90 | — | +0.00 | +16.90 | +145.34 | — |
| 2026-08-21 | `ATAT` | 38 | $34.25 | $34.31 | +2.28 | — | +0.00 | +2.28 | +9.88 | — |
| 2026-08-21 | `ATHM` | 58 | $22.12 | $22.20 | +4.64 | — | +0.00 | +4.64 | -13.92 | — |
| 2026-08-21 | `BABA` | 10 | $130.53 | $125.35 | -51.80 | — | +0.00 | -51.80 | +18.80 | — |
| 2026-08-21 | `BILL` | 26 | $47.40 | $47.50 | +2.60 | — | +0.00 | +2.60 | -39.00 | — |
| 2026-08-21 | `BULL` | 132 | $8.85 | $8.99 | +18.48 | — | +0.00 | +18.48 | -125.40 | — |
| 2026-08-21 | `BEKE` | 115 | — | $17.93 | +0.00 | $17.75 | -21.27 | -21.27 | +0.00 | -21.27 |
| 2026-08-21 | `BJ` | 22 | — | $93.98 | +0.00 | $96.42 | +53.68 | +53.68 | +0.00 | +53.68 |
| 2026-08-21 | `BKE` | 48 | — | $43.08 | +0.00 | $43.81 | +35.04 | +35.04 | +0.00 | +35.04 |
| 2026-08-21 | `PSEC` | 900 | — | $2.30 | +0.00 | $2.33 | +27.00 | +27.00 | +0.00 | +27.00 |
| 2026-08-21 | `ROST` | 8 | — | $243.85 | +0.00 | $239.04 | -38.48 | -38.48 | +0.00 | -38.48 |
| 2026-08-24 | `BEKE` | 115 | $17.75 | $18.05 | +35.07 | — | +0.00 | +35.07 | +13.80 | — |
| 2026-08-24 | `BJ` | 22 | $96.42 | $97.02 | +13.20 | — | +0.00 | +13.20 | +66.88 | — |
| 2026-08-24 | `BKE` | 48 | $43.81 | $44.22 | +19.68 | — | +0.00 | +19.68 | +54.72 | — |
| 2026-08-24 | `PSEC` | 900 | $2.33 | $2.34 | +9.00 | — | +0.00 | +9.00 | +36.00 | — |
| 2026-08-24 | `ROST` | 8 | $239.04 | $238.08 | -7.68 | — | +0.00 | -7.68 | -46.16 | — |
| 2026-08-25 | `BMO` | 7 | — | $175.01 | +0.00 | $173.46 | -10.85 | -10.85 | +0.00 | -10.85 |
| 2026-08-25 | `BNS` | 14 | — | $88.94 | +0.00 | $93.10 | +58.24 | +58.24 | +0.00 | +58.24 |
| 2026-08-25 | `BZ` | 85 | — | $15.28 | +0.00 | $16.29 | +85.85 | +85.85 | +0.00 | +85.85 |
| 2026-08-25 | `DKS` | 9 | — | $142.36 | +0.00 | $124.31 | -162.45 | -162.45 | +0.00 | -162.45 |
| 2026-08-25 | `EH` | 255 | — | $5.10 | +0.00 | $4.83 | -68.85 | -68.85 | +0.00 | -68.85 |
| 2026-08-25 | `GFI` | 27 | — | $47.89 | +0.00 | $48.87 | +26.46 | +26.46 | +0.00 | +26.46 |
| 2026-08-25 | `GRRR` | 93 | — | $13.92 | +0.00 | $14.04 | +11.16 | +11.16 | +0.00 | +11.16 |
| 2026-08-25 | `SHMD` | 287 | — | $4.54 | +0.00 | $3.42 | -322.88 | -322.88 | +0.00 | -322.88 |
| 2026-08-26 | `BMO` | 7 | $173.46 | $173.22 | -1.68 | — | +0.00 | -1.68 | -12.53 | — |
| 2026-08-26 | `BNS` | 14 | $93.10 | $92.65 | -6.30 | — | +0.00 | -6.30 | +51.94 | — |
| 2026-08-26 | `BZ` | 85 | $16.29 | $16.77 | +40.80 | — | +0.00 | +40.80 | +126.65 | — |
| 2026-08-26 | `DKS` | 9 | $124.31 | $121.87 | -21.96 | — | +0.00 | -21.96 | -184.41 | — |
| 2026-08-26 | `EH` | 255 | $4.83 | $4.77 | -15.30 | — | +0.00 | -15.30 | -84.15 | — |
| 2026-08-26 | `GFI` | 27 | $48.87 | $48.24 | -17.01 | — | +0.00 | -17.01 | +9.45 | — |
| 2026-08-26 | `GRRR` | 93 | $14.04 | $14.03 | -0.93 | — | +0.00 | -0.93 | +10.23 | — |
| 2026-08-26 | `SHMD` | 287 | $3.42 | $3.38 | -11.48 | — | +0.00 | -11.48 | -334.36 | — |
| 2026-08-26 | `TIGR` | 239 | — | $5.21 | +0.00 | $5.46 | +59.75 | +59.75 | +0.00 | +59.75 |
| 2026-08-26 | `ANF` | 9 | — | $131.37 | +0.00 | $147.75 | +147.42 | +147.42 | +0.00 | +147.42 |
| 2026-08-26 | `BBWI` | 68 | — | $18.26 | +0.00 | $18.90 | +43.52 | +43.52 | +0.00 | +43.52 |
| 2026-08-26 | `BOX` | 36 | — | $34.30 | +0.00 | $33.39 | -32.76 | -32.76 | +0.00 | -32.76 |
| 2026-08-26 | `DY` | 3 | — | $326.91 | +0.00 | $310.91 | -48.00 | -48.00 | +0.00 | -48.00 |
| 2026-08-26 | `FSCO` | 245 | — | $5.08 | +0.00 | $5.12 | +9.80 | +9.80 | +0.00 | +9.80 |
| 2026-08-26 | `HEI` | 3 | — | $370.00 | +0.00 | $346.15 | -71.55 | -71.55 | +0.00 | -71.55 |
| 2026-08-26 | `INTU` | 3 | — | $323.47 | +0.00 | $345.88 | +67.23 | +67.23 | +0.00 | +67.23 |
| 2026-08-27 | `TIGR` | 239 | $5.46 | $5.49 | +7.17 | — | +0.00 | +7.17 | +66.92 | — |
| 2026-08-27 | `ANF` | 9 | $147.75 | $144.70 | -27.45 | — | +0.00 | -27.45 | +119.97 | — |
| 2026-08-27 | `BBWI` | 68 | $18.90 | $18.69 | -14.28 | — | +0.00 | -14.28 | +29.24 | — |
| 2026-08-27 | `BOX` | 36 | $33.39 | $33.79 | +14.40 | — | +0.00 | +14.40 | -18.36 | — |
| 2026-08-27 | `DY` | 3 | $310.91 | $314.90 | +11.97 | — | +0.00 | +11.97 | -36.03 | — |
| 2026-08-27 | `FSCO` | 245 | $5.12 | $5.10 | -4.90 | — | +0.00 | -4.90 | +4.90 | — |
| 2026-08-27 | `HEI` | 3 | $346.15 | $346.19 | +0.12 | — | +0.00 | +0.12 | -71.43 | — |
| 2026-08-27 | `INTU` | 3 | $345.88 | $353.54 | +22.98 | — | +0.00 | +22.98 | +90.21 | — |
| 2026-08-27 | `NVDA` | 45 | — | $222.86 | +0.00 | $227.98 | +230.40 | +230.40 | +0.00 | +230.40 |
| 2026-08-28 | `NVDA` | 45 | $227.98 | $227.36 | -27.90 | — | +0.00 | -27.90 | +202.50 | — |
| 2026-08-28 | `ADSK` | 4 | — | $261.16 | +0.00 | $260.66 | -2.00 | -2.00 | +0.00 | -2.00 |
| 2026-08-28 | `BBAR` | 86 | — | $15.01 | +0.00 | $14.47 | -46.44 | -46.44 | +0.00 | -46.44 |
| 2026-08-28 | `ESTC` | 12 | — | $103.89 | +0.00 | $99.91 | -47.76 | -47.76 | +0.00 | -47.76 |
| 2026-08-28 | `FINV` | 332 | — | $3.88 | +0.00 | $3.40 | -159.36 | -159.36 | +0.00 | -159.36 |
| 2026-08-28 | `FRO` | 29 | — | $44.40 | +0.00 | $44.19 | -6.09 | -6.09 | +0.00 | -6.09 |
| 2026-08-28 | `GAP` | 52 | — | $24.69 | +0.00 | $23.48 | -62.92 | -62.92 | +0.00 | -62.92 |
| 2026-08-28 | `HAFN` | 154 | — | $8.35 | +0.00 | $8.47 | +18.48 | +18.48 | +0.00 | +18.48 |
| 2026-08-28 | `IREN` | 34 | — | $37.65 | +0.00 | $35.45 | -74.63 | -74.63 | +0.00 | -74.63 |
| 2026-08-31 | `ADSK` | 4 | $260.66 | $257.71 | -11.80 | — | +0.00 | -11.80 | -13.80 | — |
| 2026-08-31 | `BBAR` | 86 | $14.47 | $14.88 | +35.26 | — | +0.00 | +35.26 | -11.18 | — |
| 2026-08-31 | `ESTC` | 12 | $99.91 | $98.00 | -22.92 | — | +0.00 | -22.92 | -70.68 | — |
| 2026-08-31 | `FINV` | 332 | $3.40 | $3.39 | -3.32 | — | +0.00 | -3.32 | -162.68 | — |
| 2026-08-31 | `FRO` | 29 | $44.19 | $44.85 | +19.14 | — | +0.00 | +19.14 | +13.05 | — |
| 2026-08-31 | `GAP` | 52 | $23.48 | $22.98 | -26.00 | — | +0.00 | -26.00 | -88.92 | — |
| 2026-08-31 | `HAFN` | 154 | $8.47 | $8.53 | +9.24 | — | +0.00 | +9.24 | +27.72 | — |
| 2026-08-31 | `IREN` | 34 | $35.45 | $35.81 | +12.24 | — | +0.00 | +12.24 | -62.39 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AI` | 115 | — | $10.74 | +0.00 | $10.90 | +17.83 | +17.83 | +0.00 | +17.83 |
| 2026-09-03 | `AVGO` | 3 | — | $351.74 | +0.00 | $357.16 | +16.26 | +16.26 | +0.00 | +16.26 |
| 2026-09-03 | `CHPT` | 179 | — | $6.90 | +0.00 | $9.08 | +390.22 | +390.22 | +0.00 | +390.22 |
| 2026-09-03 | `CIEN` | 3 | — | $354.49 | +0.00 | $317.46 | -111.09 | -111.09 | +0.00 | -111.09 |
| 2026-09-03 | `CPB` | 55 | — | $22.32 | +0.00 | $22.13 | -10.45 | -10.45 | +0.00 | -10.45 |
| 2026-09-03 | `FIVE` | 4 | — | $257.00 | +0.00 | $239.96 | -68.16 | -68.16 | +0.00 | -68.16 |
| 2026-09-03 | `HPE` | 26 | — | $47.60 | +0.00 | $54.44 | +177.84 | +177.84 | +0.00 | +177.84 |
| 2026-09-03 | `MEI` | 82 | — | $15.09 | +0.00 | $15.32 | +18.86 | +18.86 | +0.00 | +18.86 |
| 2026-09-04 | `AI` | 115 | $10.90 | $10.91 | +1.15 | — | +0.00 | +1.15 | +18.98 | — |
| 2026-09-04 | `AVGO` | 3 | $357.16 | $359.70 | +7.62 | — | +0.00 | +7.62 | +23.88 | — |
| 2026-09-04 | `CHPT` | 179 | $9.08 | $9.28 | +35.80 | — | +0.00 | +35.80 | +426.02 | — |
| 2026-09-04 | `CIEN` | 3 | $317.46 | $321.67 | +12.63 | — | +0.00 | +12.63 | -98.46 | — |
| 2026-09-04 | `CPB` | 55 | $22.13 | $22.10 | -1.65 | — | +0.00 | -1.65 | -12.10 | — |
| 2026-09-04 | `FIVE` | 4 | $239.96 | $238.88 | -4.32 | — | +0.00 | -4.32 | -72.48 | — |
| 2026-09-04 | `HPE` | 26 | $54.44 | $53.85 | -15.34 | — | +0.00 | -15.34 | +162.50 | — |
| 2026-09-04 | `MEI` | 82 | $15.32 | $15.34 | +1.64 | — | +0.00 | +1.64 | +20.50 | — |
| 2026-09-04 | `AMBA` | 20 | — | $63.18 | +0.00 | $62.89 | -5.80 | -5.80 | +0.00 | -5.80 |
| 2026-09-04 | `ASAN` | 148 | — | $8.74 | +0.00 | $8.81 | +10.36 | +10.36 | +0.00 | +10.36 |
| 2026-09-04 | `DOCU` | 18 | — | $68.52 | +0.00 | $68.41 | -1.98 | -1.98 | +0.00 | -1.98 |
| 2026-09-04 | `DOMO` | 358 | — | $3.62 | +0.00 | $3.88 | +94.87 | +94.87 | +0.00 | +94.87 |
| 2026-09-04 | `GWRE` | 7 | — | $167.55 | +0.00 | $162.42 | -35.91 | -35.91 | +0.00 | -35.91 |
| 2026-09-04 | `IOT` | 28 | — | $44.90 | +0.00 | $40.20 | -131.60 | -131.60 | +0.00 | -131.60 |
| 2026-09-04 | `LULU` | 13 | — | $98.15 | +0.00 | $100.61 | +31.98 | +31.98 | +0.00 | +31.98 |
| 2026-09-04 | `MAMA` | 82 | — | $15.70 | +0.00 | $15.16 | -44.28 | -44.28 | +0.00 | -44.28 |
| 2026-09-08 | `AMBA` | 20 | $62.89 | $63.83 | +18.80 | — | +0.00 | +18.80 | +13.00 | — |
| 2026-09-08 | `ASAN` | 148 | $8.81 | $8.73 | -11.84 | — | +0.00 | -11.84 | -1.48 | — |
| 2026-09-08 | `DOCU` | 18 | $68.41 | $67.05 | -24.48 | — | +0.00 | -24.48 | -26.46 | — |
| 2026-09-08 | `DOMO` | 358 | $3.88 | $3.84 | -14.32 | — | +0.00 | -14.32 | +80.55 | — |
| 2026-09-08 | `GWRE` | 7 | $162.42 | $160.52 | -13.30 | — | +0.00 | -13.30 | -49.21 | — |
| 2026-09-08 | `IOT` | 28 | $40.20 | $39.56 | -17.92 | — | +0.00 | -17.92 | -149.52 | — |
| 2026-09-08 | `LULU` | 13 | $100.61 | $100.58 | -0.39 | — | +0.00 | -0.39 | +31.59 | — |
| 2026-09-08 | `MAMA` | 82 | $15.16 | $15.20 | +3.28 | — | +0.00 | +3.28 | -41.00 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +840.92 | INO, VOR | — | $21.06 | $10,769.53 | INO×6172, VOR×223 |
| 2026-08-14 | +5.50 | $21.06 | INO×6172, VOR×223 | $10,963.61 | +194.08 | -240.47 | NMAX, AIRJ, AMAT, AMPG, BRUN, BZAI, DEFT, DGXX | INO, VOR | $336.60 | $10,583.79 | NMAX×137, AIRJ×246, AMAT×2, AMPG×311, BRUN×51, BZAI×1776, DEFT×2894, DGXX×347 |
| 2026-08-17 | +2.25 | $336.60 | NMAX×137, AIRJ×246, AMAT×2, AMPG×311, BRUN×51, BZAI×1776, DEFT×2894, DGXX×347 | $10,578.62 | -5.17 | +0.00 | — | NMAX, AIRJ, AMAT, AMPG, BRUN, BZAI, DEFT, DGXX | $10,521.80 | $10,521.80 | — |
| 2026-08-18 | -6.20 | $10,521.80 | — | $10,521.80 | +0.00 | +0.00 | — | — | $10,521.80 | $10,521.80 | — |
| 2026-08-19 | -7.20 | $10,521.80 | — | $10,521.80 | +0.00 | +0.00 | — | — | $10,521.80 | $10,521.80 | — |
| 2026-08-20 | +1.12 | $10,521.80 | — | $10,521.80 | +0.00 | -122.28 | AAP, AEG, ALVO, ATAT, ATHM, BABA, BILL, BULL | — | $152.93 | $10,379.92 | AAP×28, AEG×145, ALVO×338, ATAT×38, ATHM×58, BABA×10, BILL×26, BULL×132 |
| 2026-08-21 | +3.25 | $152.93 | AAP×28, AEG×145, ALVO×338, ATAT×38, ATHM×58, BABA×10, BILL×26, BULL×132 | $10,377.93 | -1.99 | +55.97 | BEKE, BJ, BKE, PSEC, ROST | AAP, AEG, ALVO, ATAT, ATHM, BABA, BILL, BULL | $119.22 | $10,393.91 | BEKE×115, BJ×22, BKE×48, PSEC×900, ROST×8 |
| 2026-08-24 | -5.17 | $119.22 | BEKE×115, BJ×22, BKE×48, PSEC×900, ROST×8 | $10,463.18 | +69.27 | +0.00 | — | BEKE, BJ, BKE, PSEC, ROST | $10,442.75 | $10,442.75 | — |
| 2026-08-25 | +1.80 | $10,442.75 | — | $10,442.75 | +0.00 | -383.32 | BMO, BNS, BZ, DKS, EH, GFI, GRRR, SHMD | — | $180.34 | $10,039.80 | BMO×7, BNS×14, BZ×85, DKS×9, EH×255, GFI×27, GRRR×93, SHMD×287 |
| 2026-08-26 | +2.02 | $180.34 | BMO×7, BNS×14, BZ×85, DKS×9, EH×255, GFI×27, GRRR×93, SHMD×287 | $10,005.94 | -33.86 | +175.41 | TIGR, ANF, BBWI, BOX, DY, FSCO, HEI, INTU | BMO, BNS, BZ, DKS, EH, GFI, GRRR, SHMD | $757.77 | $10,142.92 | TIGR×239, ANF×9, BBWI×68, BOX×36, DY×3, FSCO×245, HEI×3, INTU×3 |
| 2026-08-27 | — | $757.77 | TIGR×239, ANF×9, BBWI×68, BOX×36, DY×3, FSCO×245, HEI×3, INTU×3 | $10,152.93 | +10.01 | +230.40 | NVDA | TIGR, ANF, BBWI, BOX, DY, FSCO, HEI, INTU | $103.34 | $10,362.44 | NVDA×45 |
| 2026-08-28 | +0.75 | $103.34 | NVDA×45 | $10,334.54 | -27.90 | -380.72 | ADSK, BBAR, ESTC, FINV, FRO, GAP, HAFN, IREN | NVDA | $305.34 | $9,932.27 | ADSK×4, BBAR×86, ESTC×12, FINV×332, FRO×29, GAP×52, HAFN×154, IREN×34 |
| 2026-08-31 | -5.85 | $305.34 | ADSK×4, BBAR×86, ESTC×12, FINV×332, FRO×29, GAP×52, HAFN×154, IREN×34 | $9,944.11 | +11.84 | +0.00 | — | ADSK, BBAR, ESTC, FINV, FRO, GAP, HAFN, IREN | $9,924.56 | $9,924.56 | — |
| 2026-09-01 | -6.30 | $9,924.56 | — | $9,924.56 | +0.00 | +0.00 | — | — | $9,924.56 | $9,924.56 | — |
| 2026-09-02 | -3.83 | $9,924.56 | — | $9,924.56 | +0.00 | +0.00 | — | — | $9,924.56 | $9,924.56 | — |
| 2026-09-03 | -0.90 | $9,924.56 | — | $9,924.56 | +0.00 | +431.31 | AI, AVGO, CHPT, CIEN, CPB, FIVE, HPE, MEI | — | $587.20 | $10,338.55 | AI×115, AVGO×3, CHPT×179, CIEN×3, CPB×55, FIVE×4, HPE×26, MEI×82 |
| 2026-09-04 | +2.25 | $587.20 | AI×115, AVGO×3, CHPT×179, CIEN×3, CPB×55, FIVE×4, HPE×26, MEI×82 | $10,376.08 | +37.53 | -82.36 | AMBA, ASAN, DOCU, DOMO, GWRE, IOT, LULU, MAMA | AI, AVGO, CHPT, CIEN, CPB, FIVE, HPE, MEI | $261.01 | $10,256.70 | AMBA×20, ASAN×148, DOCU×18, DOMO×358, GWRE×7, IOT×28, LULU×13, MAMA×82 |
| 2026-09-08 | -11.47 | $261.01 | AMBA×20, ASAN×148, DOCU×18, DOMO×358, GWRE×7, IOT×28, LULU×13, MAMA×82 | $10,196.53 | -60.17 | +0.00 | — | AMBA, ASAN, DOCU, DOMO, GWRE, IOT, LULU, MAMA | $10,176.81 | $10,176.81 | — |
| 2026-09-09 | -13.95 | $10,176.81 | — | $10,176.81 | -0.00 | +0.00 | — | — | $10,176.81 | $10,176.81 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 6172 | $0.81 | $68.51 | — | $4,932.17 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list flatten; ⚪; ret5=+13.2; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 223 | $22.01 | $2.88 | — | $21.06 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list flatten; ⚪; ret5=+0.3; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.06 | ▲ close $10,769.53 vs 09:30 $10,000.00 (session +840.92) | 16:00 close · cash $21.06 · equity $10,769.53 vs 09:30 $10,000.00 (+769.53; session marks +840.92) · 2 name(s) marked open→close (per-name table). INO×6172 09:30 $0.81 → close $0.90 +555.48; VOR×223 09:30 $22.01 → close $23.29 +285.44 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.06 | ▲ 09:30 equity $10,963.61 vs yday $10,769.53 (+194.08) | 09:30 open · cash $21.06 (unchanged overnight, no fees) · equity $10,963.61 vs prior close $10,769.53 (+194.08) · 2 name(s) re-marked at the open (per-name table). INO×6172 yday $0.90 → 09:30 $0.93 +185.16; VOR×223 yday $23.29 → 09:30 $23.33 +8.92 | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 6172 | $0.93 | $76.99 | $+595.14 | $5,684.04 | ▲ +595.14 after sell → book $10,886.63; vs 09:30 mark -76.98 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 223 | $23.33 | $2.96 | $+288.53 | $10,883.67 | ▲ +288.53 after sell → book $10,883.67; vs 09:30 mark -2.96 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 137 | $9.89 | $2.40 | — | $9,525.66 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRJ` | 246 | $5.51 | $3.17 | — | $8,167.02 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+13.1; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AMAT` | 2 | $499.40 | $2.00 | — | $7,166.23 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+1.3; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AMPG` | 311 | $4.37 | $4.01 | — | $5,803.77 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+10.3; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BRUN` | 51 | $26.25 | $2.14 | — | $4,463.13 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+31.2; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 1776 | $0.77 | $18.93 | — | $3,083.78 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DEFT` | 2894 | $0.47 | $22.28 | — | $1,701.32 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+11.1; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DGXX` | 347 | $3.92 | $4.48 | — | $336.60 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+10.1; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $336.60 | ▼ close $10,583.79 vs 09:30 $10,963.61 (session -240.47) | 16:00 close · cash $336.60 · equity $10,583.79 vs 09:30 $10,963.61 (-379.82; session marks -240.47) · 8 name(s) marked open→close (per-name table). NMAX×137 09:30 $9.89 → close $10.87 +133.57; AIRJ×246 09:30 $5.51 → close $6.04 +130.38; AMAT×2 09:30 $499.40 → close $507.18 +15.56; AMPG×311 09:30 $4.37 → close $4.00 -116.00; BRUN×51 09:30 $26.25 → close $22.93 -169.07; BZAI×1776 09:30 $0.77 → close $0.59 -307.25; DEFT×2894 09:30 $0.47 → close $0.49 +54.99; DGXX×347 09:30 $3.92 → close $3.97 +17.35 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $336.60 | ▼ 09:30 equity $10,578.62 vs yday $10,583.79 (-5.17) | 09:30 open · cash $336.60 (unchanged overnight, no fees) · equity $10,578.62 vs prior close $10,583.79 (-5.17) · 8 name(s) re-marked at the open (per-name table). NMAX×137 yday $10.87 → 09:30 $10.97 +13.70; AIRJ×246 yday $6.04 → 09:30 $6.22 +44.28; AMAT×2 yday $507.18 → 09:30 $517.45 +20.53; AMPG×311 yday $4.00 → 09:30 $4.09 +29.54; BRUN×51 yday $22.93 → 09:30 $23.00 +3.57; BZAI×1776 yday $0.59 → 09:30 $0.55 -72.82; DEFT×2894 yday $0.49 → 09:30 $0.47 -40.52; DGXX×347 yday $3.97 → 09:30 $3.96 -3.47 | — |
| 2026-08-17 09:30 ET | **SELL** | `NMAX` | 137 | $10.97 | $2.44 | $+142.44 | $1,837.06 | ▲ +142.44 after sell → book $10,576.18; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `AIRJ` | 246 | $6.22 | $3.23 | $+168.26 | $3,363.95 | ▲ +168.26 after sell → book $10,572.95; vs 09:30 mark -3.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AMAT` | 2 | $517.45 | $2.02 | $+32.08 | $4,396.83 | ▲ +32.08 after sell → book $10,570.94; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AMPG` | 311 | $4.09 | $4.07 | $-94.54 | $5,664.74 | ▼ -94.54 after sell → book $10,566.86; vs 09:30 mark -4.08 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `BRUN` | 51 | $23.00 | $2.16 | $-169.80 | $6,835.58 | ▼ -169.80 after sell → book $10,564.70; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BZAI` | 1776 | $0.55 | $15.44 | $-414.43 | $7,800.50 | ▼ -414.43 after sell → book $10,549.27; vs 09:30 mark -15.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DEFT` | 2894 | $0.47 | $22.92 | $-30.73 | $9,152.23 | ▼ -30.73 after sell → book $10,526.35; vs 09:30 mark -22.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DGXX` | 347 | $3.96 | $4.54 | $+4.86 | $10,521.80 | ▲ +4.86 after sell → book $10,521.80; vs 09:30 mark -4.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,521.80 | ▲ close $10,521.80 vs 09:30 $10,578.62 (session +0.00) | 16:00 close · cash $10,521.80 · no lots left · equity $10,521.80. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,521.80 | ▲ 09:30 equity $10,521.80 vs yday $10,521.80 (+0.00) | 09:30 open · cash $10,521.80 · no holdings · equity $10,521.80 vs prior close $10,521.80 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,521.80 | ▲ close $10,521.80 vs 09:30 $10,521.80 (session +0.00) | 16:00 close · cash $10,521.80 · no lots left · equity $10,521.80. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,521.80 | ▲ 09:30 equity $10,521.80 vs yday $10,521.80 (+0.00) | 09:30 open · cash $10,521.80 · no holdings · equity $10,521.80 vs prior close $10,521.80 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,521.80 | ▲ close $10,521.80 vs 09:30 $10,521.80 (session +0.00) | 16:00 close · cash $10,521.80 · no lots left · equity $10,521.80. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,521.80 | ▲ 09:30 equity $10,521.80 vs yday $10,521.80 (+0.00) | 09:30 open · cash $10,521.80 · no holdings · equity $10,521.80 vs prior close $10,521.80 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 28 | $46.85 | $2.07 | — | $9,207.93 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+5.0; leftover $1315.23 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 145 | $9.01 | $2.42 | — | $7,899.05 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-1.3; leftover $1315.23 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 338 | $3.89 | $4.36 | — | $6,579.87 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-0.5; leftover $1315.23 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 38 | $34.05 | $2.10 | — | $5,283.87 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+9.3; leftover $1315.23 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 58 | $22.44 | $2.16 | — | $3,980.18 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.1; leftover $1315.23 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BABA` | 10 | $123.47 | $2.02 | — | $2,743.46 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+2.9; leftover $1315.23 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BILL` | 26 | $49.00 | $2.07 | — | $1,467.40 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-2.0; leftover $1315.23 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BULL` | 132 | $9.94 | $2.39 | — | $152.93 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+12.6; leftover $1315.23 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $152.93 | ▼ close $10,379.92 vs 09:30 $10,521.80 (session -122.28) | 16:00 close · cash $152.93 · equity $10,379.92 vs 09:30 $10,521.80 (-141.88; session marks -122.28) · 8 name(s) marked open→close (per-name table). AAP×28 09:30 $46.85 → close $42.39 -124.88; AEG×145 09:30 $9.01 → close $9.01 +0.00; ALVO×338 09:30 $3.89 → close $4.27 +128.44; ATAT×38 09:30 $34.05 → close $34.25 +7.60; ATHM×58 09:30 $22.44 → close $22.12 -18.56; BABA×10 09:30 $123.47 → close $130.53 +70.60; BILL×26 09:30 $49.00 → close $47.40 -41.60; BULL×132 09:30 $9.94 → close $8.85 -143.88 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $152.93 | ▼ 09:30 equity $10,377.93 vs yday $10,379.92 (-1.99) | 09:30 open · cash $152.93 (unchanged overnight, no fees) · equity $10,377.93 vs prior close $10,379.92 (-1.99) · 8 name(s) re-marked at the open (per-name table). AAP×28 yday $42.39 → 09:30 $42.41 +0.56; AEG×145 yday $9.01 → 09:30 $9.04 +4.35; ALVO×338 yday $4.27 → 09:30 $4.32 +16.90; ATAT×38 yday $34.25 → 09:30 $34.31 +2.28; ATHM×58 yday $22.12 → 09:30 $22.20 +4.64; BABA×10 yday $130.53 → 09:30 $125.35 -51.80; BILL×26 yday $47.40 → 09:30 $47.50 +2.60; BULL×132 yday $8.85 → 09:30 $8.99 +18.48 | — |
| 2026-08-21 09:30 ET | **SELL** | `AAP` | 28 | $42.41 | $2.09 | $-128.49 | $1,338.32 | ▼ -128.49 after sell → book $10,375.84; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AEG` | 145 | $9.04 | $2.46 | $-0.53 | $2,646.66 | ▼ -0.53 after sell → book $10,373.38; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ALVO` | 338 | $4.32 | $4.43 | $+136.55 | $4,102.39 | ▲ +136.55 after sell → book $10,368.95; vs 09:30 mark -4.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 38 | $34.31 | $2.12 | $+5.65 | $5,404.04 | ▲ +5.65 after sell → book $10,366.82; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATHM` | 58 | $22.20 | $2.18 | $-18.27 | $6,689.46 | ▼ -18.27 after sell → book $10,364.64; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BABA` | 10 | $125.35 | $2.04 | $+14.74 | $7,940.92 | ▲ +14.74 after sell → book $10,362.60; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BILL` | 26 | $47.50 | $2.09 | $-43.16 | $9,173.83 | ▼ -43.16 after sell → book $10,360.51; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BULL` | 132 | $8.99 | $2.42 | $-130.20 | $10,358.09 | ▼ -130.20 after sell → book $10,358.09; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 115 | $17.93 | $2.33 | — | $8,293.23 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+0.2; leftover $2071.62 | join🟢 sector🟢 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 22 | $93.98 | $2.06 | — | $6,223.62 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.4; leftover $2071.62 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 48 | $43.08 | $2.13 | — | $4,153.64 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.9; leftover $2071.62 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 900 | $2.30 | $11.61 | — | $2,072.03 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.0; leftover $2071.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ROST` | 8 | $243.85 | $2.01 | — | $119.22 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-6.5; leftover $2071.62 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $119.22 | ▲ close $10,393.91 vs 09:30 $10,377.93 (session +55.97) | 16:00 close · cash $119.22 · equity $10,393.91 vs 09:30 $10,377.93 (+15.98; session marks +55.97) · 5 name(s) marked open→close (per-name table). BEKE×115 09:30 $17.93 → close $17.75 -21.27; BJ×22 09:30 $93.98 → close $96.42 +53.68; BKE×48 09:30 $43.08 → close $43.81 +35.04; PSEC×900 09:30 $2.30 → close $2.33 +27.00; ROST×8 09:30 $243.85 → close $239.04 -38.48 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $119.22 | ▲ 09:30 equity $10,463.18 vs yday $10,393.91 (+69.27) | 09:30 open · cash $119.22 (unchanged overnight, no fees) · equity $10,463.18 vs prior close $10,393.91 (+69.27) · 5 name(s) re-marked at the open (per-name table). BEKE×115 yday $17.75 → 09:30 $18.05 +35.07; BJ×22 yday $96.42 → 09:30 $97.02 +13.20; BKE×48 yday $43.81 → 09:30 $44.22 +19.68; PSEC×900 yday $2.33 → 09:30 $2.34 +9.00; ROST×8 yday $239.04 → 09:30 $238.08 -7.68 | — |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 115 | $18.05 | $2.37 | $+9.09 | $2,193.17 | ▲ +9.09 after sell → book $10,460.81; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 22 | $97.02 | $2.08 | $+62.74 | $4,325.53 | ▲ +62.74 after sell → book $10,458.73; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 48 | $44.22 | $2.16 | $+50.42 | $6,445.93 | ▲ +50.42 after sell → book $10,456.57; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PSEC` | 900 | $2.34 | $11.78 | $+12.61 | $8,540.15 | ▲ +12.61 after sell → book $10,444.79; vs 09:30 mark -11.78 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ROST` | 8 | $238.08 | $2.04 | $-50.21 | $10,442.75 | ▼ -50.21 after sell → book $10,442.75; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,442.75 | ▲ close $10,442.75 vs 09:30 $10,463.18 (session +0.00) | 16:00 close · cash $10,442.75 · no lots left · equity $10,442.75. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,442.75 | ▲ 09:30 equity $10,442.75 vs yday $10,442.75 (+0.00) | 09:30 open · cash $10,442.75 · no holdings · equity $10,442.75 vs prior close $10,442.75 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 7 | $175.01 | $2.01 | — | $9,215.67 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-7.0; leftover $1305.34 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 14 | $88.94 | $2.03 | — | $7,968.48 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-4.9; leftover $1305.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 85 | $15.28 | $2.25 | — | $6,667.44 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-0.7; leftover $1305.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 9 | $142.36 | $2.02 | — | $5,384.18 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-8.6; leftover $1305.34 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 255 | $5.10 | $3.29 | — | $4,080.39 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-8.9; leftover $1305.34 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 27 | $47.89 | $2.07 | — | $2,785.29 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ⚪; ret5=+14.0; leftover $1305.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 93 | $13.92 | $2.27 | — | $1,488.46 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+5.9; leftover $1305.34 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 287 | $4.54 | $3.70 | — | $180.34 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-14.6; leftover $1305.34 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $180.34 | ▼ close $10,039.80 vs 09:30 $10,442.75 (session -383.32) | 16:00 close · cash $180.34 · equity $10,039.80 vs 09:30 $10,442.75 (-402.95; session marks -383.32) · 8 name(s) marked open→close (per-name table). BMO×7 09:30 $175.01 → close $173.46 -10.85; BNS×14 09:30 $88.94 → close $93.10 +58.24; BZ×85 09:30 $15.28 → close $16.29 +85.85; DKS×9 09:30 $142.36 → close $124.31 -162.45; EH×255 09:30 $5.10 → close $4.83 -68.85; GFI×27 09:30 $47.89 → close $48.87 +26.46; GRRR×93 09:30 $13.92 → close $14.04 +11.16; SHMD×287 09:30 $4.54 → close $3.42 -322.88 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $180.34 | ▼ 09:30 equity $10,005.94 vs yday $10,039.80 (-33.86) | 09:30 open · cash $180.34 (unchanged overnight, no fees) · equity $10,005.94 vs prior close $10,039.80 (-33.86) · 8 name(s) re-marked at the open (per-name table). BMO×7 yday $173.46 → 09:30 $173.22 -1.68; BNS×14 yday $93.10 → 09:30 $92.65 -6.30; BZ×85 yday $16.29 → 09:30 $16.77 +40.80; DKS×9 yday $124.31 → 09:30 $121.87 -21.96; EH×255 yday $4.83 → 09:30 $4.77 -15.30; GFI×27 yday $48.87 → 09:30 $48.24 -17.01; GRRR×93 yday $14.04 → 09:30 $14.03 -0.93; SHMD×287 yday $3.42 → 09:30 $3.38 -11.48 | — |
| 2026-08-26 09:30 ET | **SELL** | `BMO` | 7 | $173.22 | $2.03 | $-16.57 | $1,390.85 | ▼ -16.57 after sell → book $10,003.91; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BNS` | 14 | $92.65 | $2.05 | $+47.86 | $2,685.90 | ▲ +47.86 after sell → book $10,001.86; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BZ` | 85 | $16.77 | $2.27 | $+122.13 | $4,109.08 | ▲ +122.13 after sell → book $9,999.59; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **SELL** | `DKS` | 9 | $121.87 | $2.04 | $-188.46 | $5,203.87 | ▼ -188.46 after sell → book $9,997.55; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `EH` | 255 | $4.77 | $3.34 | $-90.78 | $6,416.88 | ▼ -90.78 after sell → book $9,994.21; vs 09:30 mark -3.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GFI` | 27 | $48.24 | $2.09 | $+5.29 | $7,717.27 | ▲ +5.29 after sell → book $9,992.12; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GRRR` | 93 | $14.03 | $2.29 | $+5.67 | $9,019.76 | ▲ +5.67 after sell → book $9,989.82; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `SHMD` | 287 | $3.38 | $3.76 | $-341.82 | $9,986.06 | ▼ -341.82 after sell → book $9,986.06; vs 09:30 mark -3.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 239 | $5.21 | $3.08 | — | $8,737.79 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list ohlc_hot,earn_react; 🔵; ret5=+14.3; leftover $1248.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ANF` | 9 | $131.37 | $2.02 | — | $7,553.44 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.3; leftover $1248.26 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 68 | $18.26 | $2.19 | — | $6,309.57 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-11.4; leftover $1248.26 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 36 | $34.30 | $2.10 | — | $5,072.67 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+1.7; leftover $1248.26 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 3 | $326.91 | $2.00 | — | $4,089.94 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-15.2; leftover $1248.26 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FSCO` | 245 | $5.08 | $3.16 | — | $2,842.18 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-1.6; leftover $1248.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟡 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `HEI` | 3 | $370.00 | $2.00 | — | $1,730.18 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.6; leftover $1248.26 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `INTU` | 3 | $323.47 | $2.00 | — | $757.77 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+2.0; leftover $1248.26 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $757.77 | ▲ close $10,142.92 vs 09:30 $10,005.94 (session +175.41) | 16:00 close · cash $757.77 · equity $10,142.92 vs 09:30 $10,005.94 (+136.98; session marks +175.41) · 8 name(s) marked open→close (per-name table). TIGR×239 09:30 $5.21 → close $5.46 +59.75; ANF×9 09:30 $131.37 → close $147.75 +147.42; BBWI×68 09:30 $18.26 → close $18.90 +43.52; BOX×36 09:30 $34.30 → close $33.39 -32.76; DY×3 09:30 $326.91 → close $310.91 -48.00; FSCO×245 09:30 $5.08 → close $5.12 +9.80; HEI×3 09:30 $370.00 → close $346.15 -71.55; INTU×3 09:30 $323.47 → close $345.88 +67.23 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $757.77 | ▲ 09:30 equity $10,152.93 vs yday $10,142.92 (+10.01) | 09:30 open · cash $757.77 (unchanged overnight, no fees) · equity $10,152.93 vs prior close $10,142.92 (+10.01) · 8 name(s) re-marked at the open (per-name table). TIGR×239 yday $5.46 → 09:30 $5.49 +7.17; ANF×9 yday $147.75 → 09:30 $144.70 -27.45; BBWI×68 yday $18.90 → 09:30 $18.69 -14.28; BOX×36 yday $33.39 → 09:30 $33.79 +14.40; DY×3 yday $310.91 → 09:30 $314.90 +11.97; FSCO×245 yday $5.12 → 09:30 $5.10 -4.90; HEI×3 yday $346.15 → 09:30 $346.19 +0.12; INTU×3 yday $345.88 → 09:30 $353.54 +22.98 | — |
| 2026-08-27 09:30 ET | **SELL** | `TIGR` | 239 | $5.49 | $3.13 | $+60.70 | $2,066.75 | ▲ +60.70 after sell → book $10,149.80; vs 09:30 mark -3.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ANF` | 9 | $144.70 | $2.04 | $+115.92 | $3,367.01 | ▲ +115.92 after sell → book $10,147.76; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BBWI` | 68 | $18.69 | $2.22 | $+24.83 | $4,635.72 | ▲ +24.83 after sell → book $10,145.55; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BOX` | 36 | $33.79 | $2.12 | $-22.58 | $5,850.04 | ▼ -22.58 after sell → book $10,143.43; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DY` | 3 | $314.90 | $2.02 | $-40.05 | $6,792.72 | ▼ -40.05 after sell → book $10,141.41; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FSCO` | 245 | $5.10 | $3.21 | $-1.47 | $8,039.01 | ▼ -1.47 after sell → book $10,138.20; vs 09:30 mark -3.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HEI` | 3 | $346.19 | $2.02 | $-75.45 | $9,075.56 | ▼ -75.45 after sell → book $10,136.18; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INTU` | 3 | $353.54 | $2.02 | $+86.19 | $10,134.16 | ▲ +86.19 after sell → book $10,134.16; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 45 | $222.86 | $2.12 | — | $103.34 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list mover_buy; 🔵; ret5=-3.6; leftover $10134.16 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $103.34 | ▲ close $10,362.44 vs 09:30 $10,152.93 (session +230.40) | 16:00 close · cash $103.34 · equity $10,362.44 vs 09:30 $10,152.93 (+209.51; session marks +230.40) · 1 name(s) marked open→close (per-name table). NVDA×45 09:30 $222.86 → close $227.98 +230.40 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $103.34 | ▼ 09:30 equity $10,334.54 vs yday $10,362.44 (-27.90) | 09:30 open · cash $103.34 (unchanged overnight, no fees) · equity $10,334.54 vs prior close $10,362.44 (-27.90) · 1 name(s) re-marked at the open (per-name table). NVDA×45 yday $227.98 → 09:30 $227.36 -27.90 | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 45 | $227.36 | $2.22 | $+198.16 | $10,332.32 | ▲ +198.16 after sell → book $10,332.32; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $9,285.68 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+7.8; leftover $1291.54 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 86 | $15.01 | $2.25 | — | $7,992.57 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+3.7; leftover $1291.54 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 12 | $103.89 | $2.03 | — | $6,743.86 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.5; leftover $1291.54 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 332 | $3.88 | $4.28 | — | $5,451.42 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-8.6; leftover $1291.54 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 29 | $44.40 | $2.08 | — | $4,161.74 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.4; leftover $1291.54 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 52 | $24.69 | $2.15 | — | $2,875.72 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.8; leftover $1291.54 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 154 | $8.35 | $2.45 | — | $1,587.37 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.1; leftover $1291.54 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 34 | $37.65 | $2.09 | — | $305.34 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.9; leftover $1291.54 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $305.34 | ▼ close $9,932.27 vs 09:30 $10,334.54 (session -380.72) | 16:00 close · cash $305.34 · equity $9,932.27 vs 09:30 $10,334.54 (-402.27; session marks -380.72) · 8 name(s) marked open→close (per-name table). ADSK×4 09:30 $261.16 → close $260.66 -2.00; BBAR×86 09:30 $15.01 → close $14.47 -46.44; ESTC×12 09:30 $103.89 → close $99.91 -47.76; FINV×332 09:30 $3.88 → close $3.40 -159.36; FRO×29 09:30 $44.40 → close $44.19 -6.09; GAP×52 09:30 $24.69 → close $23.48 -62.92; HAFN×154 09:30 $8.35 → close $8.47 +18.48; IREN×34 09:30 $37.65 → close $35.45 -74.63 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $305.34 | ▲ 09:30 equity $9,944.11 vs yday $9,932.27 (+11.84) | 09:30 open · cash $305.34 (unchanged overnight, no fees) · equity $9,944.11 vs prior close $9,932.27 (+11.84) · 8 name(s) re-marked at the open (per-name table). ADSK×4 yday $260.66 → 09:30 $257.71 -11.80; BBAR×86 yday $14.47 → 09:30 $14.88 +35.26; ESTC×12 yday $99.91 → 09:30 $98.00 -22.92; FINV×332 yday $3.40 → 09:30 $3.39 -3.32; FRO×29 yday $44.19 → 09:30 $44.85 +19.14; GAP×52 yday $23.48 → 09:30 $22.98 -26.00; HAFN×154 yday $8.47 → 09:30 $8.53 +9.24; IREN×34 yday $35.45 → 09:30 $35.81 +12.24 | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 4 | $257.71 | $2.02 | $-17.82 | $1,334.16 | ▼ -17.82 after sell → book $9,942.09; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBAR` | 86 | $14.88 | $2.27 | $-15.70 | $2,611.57 | ▼ -15.70 after sell → book $9,939.82; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ESTC` | 12 | $98.00 | $2.05 | $-74.75 | $3,785.52 | ▼ -74.75 after sell → book $9,937.77; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FINV` | 332 | $3.39 | $4.35 | $-171.31 | $4,906.66 | ▼ -171.31 after sell → book $9,933.43; vs 09:30 mark -4.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FRO` | 29 | $44.85 | $2.10 | $+8.88 | $6,205.21 | ▲ +8.88 after sell → book $9,931.33; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 52 | $22.98 | $2.17 | $-93.23 | $7,398.00 | ▼ -93.23 after sell → book $9,929.16; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 154 | $8.53 | $2.49 | $+22.78 | $8,709.13 | ▲ +22.78 after sell → book $9,926.67; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `IREN` | 34 | $35.81 | $2.11 | $-66.59 | $9,924.56 | ▼ -66.59 after sell → book $9,924.56; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,924.56 | ▲ close $9,924.56 vs 09:30 $9,944.11 (session +0.00) | 16:00 close · cash $9,924.56 · no lots left · equity $9,924.56. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,924.56 | ▲ 09:30 equity $9,924.56 vs yday $9,924.56 (+0.00) | 09:30 open · cash $9,924.56 · no holdings · equity $9,924.56 vs prior close $9,924.56 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,924.56 | ▲ close $9,924.56 vs 09:30 $9,924.56 (session +0.00) | 16:00 close · cash $9,924.56 · no lots left · equity $9,924.56. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,924.56 | ▲ 09:30 equity $9,924.56 vs yday $9,924.56 (+0.00) | 09:30 open · cash $9,924.56 · no holdings · equity $9,924.56 vs prior close $9,924.56 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,924.56 | ▲ close $9,924.56 vs 09:30 $9,924.56 (session +0.00) | 16:00 close · cash $9,924.56 · no lots left · equity $9,924.56. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,924.56 | ▲ 09:30 equity $9,924.56 vs yday $9,924.56 (+0.00) | 09:30 open · cash $9,924.56 · no holdings · equity $9,924.56 vs prior close $9,924.56 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 115 | $10.74 | $2.33 | — | $8,686.55 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+8.5; leftover $1240.57 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $7,629.33 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+3.3; leftover $1240.57 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 179 | $6.90 | $2.53 | — | $6,391.71 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-12.8; leftover $1240.57 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 3 | $354.49 | $2.00 | — | $5,326.24 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-12.3; leftover $1240.57 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 55 | $22.32 | $2.15 | — | $4,096.48 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+2.4; leftover $1240.57 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 4 | $257.00 | $2.00 | — | $3,066.48 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-5.5; leftover $1240.57 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 26 | $47.60 | $2.07 | — | $1,826.81 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-6.2; leftover $1240.57 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 82 | $15.09 | $2.24 | — | $587.20 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+6.1; leftover $1240.57 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $587.20 | ▲ close $10,338.55 vs 09:30 $9,924.56 (session +431.31) | 16:00 close · cash $587.20 · equity $10,338.55 vs 09:30 $9,924.56 (+413.99; session marks +431.31) · 8 name(s) marked open→close (per-name table). AI×115 09:30 $10.74 → close $10.90 +17.83; AVGO×3 09:30 $351.74 → close $357.16 +16.26; CHPT×179 09:30 $6.90 → close $9.08 +390.22; CIEN×3 09:30 $354.49 → close $317.46 -111.09; CPB×55 09:30 $22.32 → close $22.13 -10.45; FIVE×4 09:30 $257.00 → close $239.96 -68.16; HPE×26 09:30 $47.60 → close $54.44 +177.84; MEI×82 09:30 $15.09 → close $15.32 +18.86 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $587.20 | ▲ 09:30 equity $10,376.08 vs yday $10,338.55 (+37.53) | 09:30 open · cash $587.20 (unchanged overnight, no fees) · equity $10,376.08 vs prior close $10,338.55 (+37.53) · 8 name(s) re-marked at the open (per-name table). AI×115 yday $10.90 → 09:30 $10.91 +1.15; AVGO×3 yday $357.16 → 09:30 $359.70 +7.62; CHPT×179 yday $9.08 → 09:30 $9.28 +35.80; CIEN×3 yday $317.46 → 09:30 $321.67 +12.63; CPB×55 yday $22.13 → 09:30 $22.10 -1.65; FIVE×4 yday $239.96 → 09:30 $238.88 -4.32; HPE×26 yday $54.44 → 09:30 $53.85 -15.34; MEI×82 yday $15.32 → 09:30 $15.34 +1.64 | — |
| 2026-09-04 09:30 ET | **SELL** | `AI` | 115 | $10.91 | $2.36 | $+14.28 | $1,839.48 | ▲ +14.28 after sell → book $10,373.71; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $359.70 | $2.02 | $+19.86 | $2,916.56 | ▲ +19.86 after sell → book $10,371.69; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CHPT` | 179 | $9.28 | $2.57 | $+420.92 | $4,575.11 | ▲ +420.92 after sell → book $10,369.12; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 3 | $321.67 | $2.02 | $-102.48 | $5,538.10 | ▼ -102.48 after sell → book $10,367.10; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CPB` | 55 | $22.10 | $2.17 | $-16.43 | $6,751.43 | ▼ -16.43 after sell → book $10,364.93; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 4 | $238.88 | $2.02 | $-76.50 | $7,704.93 | ▼ -76.50 after sell → book $10,362.91; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 26 | $53.85 | $2.09 | $+158.34 | $9,102.94 | ▲ +158.34 after sell → book $10,360.82; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MEI` | 82 | $15.34 | $2.26 | $+16.00 | $10,358.56 | ▲ +16.00 after sell → book $10,358.56; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 20 | $63.18 | $2.05 | — | $9,092.91 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-10.9; leftover $1294.82 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 148 | $8.74 | $2.43 | — | $7,796.95 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-0.8; leftover $1294.82 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 18 | $68.52 | $2.04 | — | $6,561.55 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+3.4; leftover $1294.82 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 358 | $3.62 | $4.62 | — | $5,262.76 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-3.1; leftover $1294.82 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 7 | $167.55 | $2.01 | — | $4,087.90 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.9; leftover $1294.82 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 28 | $44.90 | $2.07 | — | $2,828.63 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-7.5; leftover $1294.82 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 13 | $98.15 | $2.03 | — | $1,550.65 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.9; leftover $1294.82 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 82 | $15.70 | $2.24 | — | $261.01 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-0.4; leftover $1294.82 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $261.01 | ▼ close $10,256.70 vs 09:30 $10,376.08 (session -82.36) | 16:00 close · cash $261.01 · equity $10,256.70 vs 09:30 $10,376.08 (-119.38; session marks -82.36) · 8 name(s) marked open→close (per-name table). AMBA×20 09:30 $63.18 → close $62.89 -5.80; ASAN×148 09:30 $8.74 → close $8.81 +10.36; DOCU×18 09:30 $68.52 → close $68.41 -1.98; DOMO×358 09:30 $3.62 → close $3.88 +94.87; GWRE×7 09:30 $167.55 → close $162.42 -35.91; IOT×28 09:30 $44.90 → close $40.20 -131.60; LULU×13 09:30 $98.15 → close $100.61 +31.98; MAMA×82 09:30 $15.70 → close $15.16 -44.28 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $261.01 | ▼ 09:30 equity $10,196.53 vs yday $10,256.70 (-60.17) | 09:30 open · cash $261.01 (unchanged overnight, no fees) · equity $10,196.53 vs prior close $10,256.70 (-60.17) · 8 name(s) re-marked at the open (per-name table). AMBA×20 yday $62.89 → 09:30 $63.83 +18.80; ASAN×148 yday $8.81 → 09:30 $8.73 -11.84; DOCU×18 yday $68.41 → 09:30 $67.05 -24.48; DOMO×358 yday $3.88 → 09:30 $3.84 -14.32; GWRE×7 yday $162.42 → 09:30 $160.52 -13.30; IOT×28 yday $40.20 → 09:30 $39.56 -17.92; LULU×13 yday $100.61 → 09:30 $100.58 -0.39; MAMA×82 yday $15.16 → 09:30 $15.20 +3.28 | — |
| 2026-09-08 09:30 ET | **SELL** | `AMBA` | 20 | $63.83 | $2.07 | $+8.88 | $1,535.54 | ▲ +8.88 after sell → book $10,194.46; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASAN` | 148 | $8.73 | $2.47 | $-6.38 | $2,825.11 | ▼ -6.38 after sell → book $10,191.99; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOCU` | 18 | $67.05 | $2.06 | $-30.57 | $4,029.95 | ▼ -30.57 after sell → book $10,189.93; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOMO` | 358 | $3.84 | $4.69 | $+71.24 | $5,399.98 | ▲ +71.24 after sell → book $10,185.24; vs 09:30 mark -4.69 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GWRE` | 7 | $160.52 | $2.03 | $-53.25 | $6,521.59 | ▼ -53.25 after sell → book $10,183.21; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IOT` | 28 | $39.56 | $2.09 | $-153.69 | $7,627.17 | ▼ -153.69 after sell → book $10,181.11; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 13 | $100.58 | $2.05 | $+27.51 | $8,932.67 | ▲ +27.51 after sell → book $10,179.07; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MAMA` | 82 | $15.20 | $2.26 | $-45.50 | $10,176.81 | ▼ -45.50 after sell → book $10,176.81; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,176.81 | ▲ close $10,176.81 vs 09:30 $10,196.53 (session +0.00) | 16:00 close · cash $10,176.81 · no lots left · equity $10,176.81. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,176.81 | ▲ 09:30 equity $10,176.81 vs yday $10,176.81 (-0.00) | 09:30 open · cash $10,176.81 · no holdings · equity $10,176.81 vs prior close $10,176.81 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,176.81 | ▲ close $10,176.81 vs 09:30 $10,176.81 (session +0.00) | 16:00 close · cash $10,176.81 · no lots left · equity $10,176.81. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `LOW` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `LZB` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MRCY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new buys |
