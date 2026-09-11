# Factor mine action — `union_e_fresh_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ e_fresh, no 🚨

Cash book **+9.32%** ($10,932) · signal-only (no cash/fees) was +15.91%. Starts YES **12/20**. Fills 128 · skips 43 · realized $+932.15.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: earnings (E) printed within the last 1 session(s).
- Must-have: the earnings flag is on.
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
- **Gate** `days_since_E_max=1,flag_E_min=0` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,932.12.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `INO` | 6172 | — | $0.81 | +0.00 | $0.90 | +555.48 | +555.48 | +0.00 | +555.48 |
| 2026-08-13 | `VOR` | 223 | — | $22.01 | +0.00 | $23.29 | +285.44 | +285.44 | +0.00 | +285.44 |
| 2026-08-14 | `INO` | 6172 | $0.90 | $0.93 | +185.16 | — | +0.00 | +185.16 | +740.64 | — |
| 2026-08-14 | `VOR` | 223 | $23.29 | $23.33 | +8.92 | — | +0.00 | +8.92 | +294.36 | — |
| 2026-08-14 | `BTBT` | 906 | — | $1.50 | +0.00 | $1.57 | +63.42 | +63.42 | +0.00 | +63.42 |
| 2026-08-14 | `ARX` | 69 | — | $19.57 | +0.00 | $19.58 | +0.69 | +0.69 | +0.00 | +0.69 |
| 2026-08-14 | `AIRO` | 122 | — | $11.12 | +0.00 | $9.57 | -189.10 | -189.10 | +0.00 | -189.10 |
| 2026-08-14 | `MH` | 100 | — | $13.55 | +0.00 | $13.10 | -45.00 | -45.00 | +0.00 | -45.00 |
| 2026-08-14 | `CLBT` | 125 | — | $10.83 | +0.00 | $11.14 | +38.75 | +38.75 | +0.00 | +38.75 |
| 2026-08-14 | `EU` | 1152 | — | $1.18 | +0.00 | $1.21 | +34.56 | +34.56 | +0.00 | +34.56 |
| 2026-08-14 | `LUNR` | 70 | — | $19.17 | +0.00 | $19.01 | -11.20 | -11.20 | +0.00 | -11.20 |
| 2026-08-14 | `NMAX` | 137 | — | $9.89 | +0.00 | $10.87 | +133.57 | +133.57 | +0.00 | +133.57 |
| 2026-08-17 | `BTBT` | 906 | $1.57 | $1.52 | -45.30 | — | +0.00 | -45.30 | +18.12 | — |
| 2026-08-17 | `ARX` | 69 | $19.58 | $19.57 | -0.69 | — | +0.00 | -0.69 | +0.00 | — |
| 2026-08-17 | `AIRO` | 122 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -189.10 | — |
| 2026-08-17 | `MH` | 100 | $13.10 | $13.16 | +6.00 | — | +0.00 | +6.00 | -39.00 | — |
| 2026-08-17 | `CLBT` | 125 | $11.14 | $11.19 | +6.25 | — | +0.00 | +6.25 | +45.00 | — |
| 2026-08-17 | `EU` | 1152 | $1.21 | $1.21 | +0.00 | — | +0.00 | +0.00 | +34.56 | — |
| 2026-08-17 | `LUNR` | 70 | $19.01 | $20.25 | +86.80 | — | +0.00 | +86.80 | +75.60 | — |
| 2026-08-17 | `NMAX` | 137 | $10.87 | $10.97 | +13.70 | — | +0.00 | +13.70 | +147.28 | — |
| 2026-08-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `EL` | 13 | — | $97.43 | +0.00 | $96.15 | -16.64 | -16.64 | +0.00 | -16.64 |
| 2026-08-20 | `TOYO` | 307 | — | $4.43 | +0.00 | $4.51 | +26.09 | +26.09 | +0.00 | +26.09 |
| 2026-08-20 | `DVLT` | 4539 | — | $0.30 | +0.00 | $0.32 | +90.78 | +90.78 | +0.00 | +90.78 |
| 2026-08-20 | `AAP` | 29 | — | $46.85 | +0.00 | $42.39 | -129.34 | -129.34 | +0.00 | -129.34 |
| 2026-08-20 | `AEG` | 151 | — | $9.01 | +0.00 | $9.01 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `ALVO` | 350 | — | $3.89 | +0.00 | $4.27 | +133.00 | +133.00 | +0.00 | +133.00 |
| 2026-08-20 | `ATAT` | 39 | — | $34.05 | +0.00 | $34.25 | +7.80 | +7.80 | +0.00 | +7.80 |
| 2026-08-20 | `ATHM` | 60 | — | $22.44 | +0.00 | $22.12 | -19.20 | -19.20 | +0.00 | -19.20 |
| 2026-08-21 | `EL` | 13 | $96.15 | $96.75 | +7.80 | — | +0.00 | +7.80 | -8.84 | — |
| 2026-08-21 | `TOYO` | 307 | $4.51 | $4.68 | +50.66 | — | +0.00 | +50.66 | +76.75 | — |
| 2026-08-21 | `DVLT` | 4539 | $0.32 | $0.31 | -45.39 | — | +0.00 | -45.39 | +45.39 | — |
| 2026-08-21 | `AAP` | 29 | $42.39 | $42.41 | +0.58 | $42.58 | +4.93 | +5.51 | -128.76 | -123.83 |
| 2026-08-21 | `AEG` | 151 | $9.01 | $9.04 | +4.53 | — | +0.00 | +4.53 | +4.53 | — |
| 2026-08-21 | `ALVO` | 350 | $4.27 | $4.32 | +17.50 | — | +0.00 | +17.50 | +150.50 | — |
| 2026-08-21 | `ATAT` | 39 | $34.25 | $34.31 | +2.34 | — | +0.00 | +2.34 | +10.14 | — |
| 2026-08-21 | `ATHM` | 60 | $22.12 | $22.20 | +4.80 | — | +0.00 | +4.80 | -14.40 | — |
| 2026-08-21 | `FUTU` | 12 | — | $115.18 | +0.00 | $123.64 | +101.52 | +101.52 | +0.00 | +101.52 |
| 2026-08-21 | `DE` | 2 | — | $623.26 | +0.00 | $647.47 | +48.42 | +48.42 | +0.00 | +48.42 |
| 2026-08-21 | `WMT` | 13 | — | $103.69 | +0.00 | $103.70 | +0.13 | +0.13 | +0.00 | +0.13 |
| 2026-08-21 | `BEKE` | 77 | — | $17.93 | +0.00 | $17.75 | -14.24 | -14.24 | +0.00 | -14.24 |
| 2026-08-21 | `BJ` | 14 | — | $93.98 | +0.00 | $96.42 | +34.16 | +34.16 | +0.00 | +34.16 |
| 2026-08-21 | `BKE` | 32 | — | $43.08 | +0.00 | $43.81 | +23.36 | +23.36 | +0.00 | +23.36 |
| 2026-08-21 | `PSEC` | 602 | — | $2.30 | +0.00 | $2.33 | +18.06 | +18.06 | +0.00 | +18.06 |
| 2026-08-24 | `AAP` | 29 | $42.58 | $43.05 | +13.63 | — | +0.00 | +13.63 | -110.20 | — |
| 2026-08-24 | `FUTU` | 12 | $123.64 | $121.00 | -31.68 | — | +0.00 | -31.68 | +69.84 | — |
| 2026-08-24 | `DE` | 2 | $647.47 | $653.04 | +11.14 | — | +0.00 | +11.14 | +59.56 | — |
| 2026-08-24 | `WMT` | 13 | $103.70 | $104.14 | +5.72 | — | +0.00 | +5.72 | +5.85 | — |
| 2026-08-24 | `BEKE` | 77 | $17.75 | $18.05 | +23.48 | — | +0.00 | +23.48 | +9.24 | — |
| 2026-08-24 | `BJ` | 14 | $96.42 | $97.02 | +8.40 | — | +0.00 | +8.40 | +42.56 | — |
| 2026-08-24 | `BKE` | 32 | $43.81 | $44.22 | +13.12 | — | +0.00 | +13.12 | +36.48 | — |
| 2026-08-24 | `PSEC` | 602 | $2.33 | $2.34 | +6.02 | — | +0.00 | +6.02 | +24.08 | — |
| 2026-08-25 | `BMO` | 7 | — | $175.01 | +0.00 | $173.46 | -10.85 | -10.85 | +0.00 | -10.85 |
| 2026-08-25 | `BNS` | 15 | — | $88.94 | +0.00 | $93.10 | +62.40 | +62.40 | +0.00 | +62.40 |
| 2026-08-25 | `BZ` | 91 | — | $15.28 | +0.00 | $16.29 | +91.91 | +91.91 | +0.00 | +91.91 |
| 2026-08-25 | `DKS` | 9 | — | $142.36 | +0.00 | $124.31 | -162.45 | -162.45 | +0.00 | -162.45 |
| 2026-08-25 | `EH` | 273 | — | $5.10 | +0.00 | $4.83 | -73.71 | -73.71 | +0.00 | -73.71 |
| 2026-08-25 | `GFI` | 29 | — | $47.89 | +0.00 | $48.87 | +28.42 | +28.42 | +0.00 | +28.42 |
| 2026-08-25 | `GRRR` | 100 | — | $13.92 | +0.00 | $14.04 | +12.00 | +12.00 | +0.00 | +12.00 |
| 2026-08-25 | `SHMD` | 306 | — | $4.54 | +0.00 | $3.42 | -344.25 | -344.25 | +0.00 | -344.25 |
| 2026-08-26 | `BMO` | 7 | $173.46 | $173.22 | -1.68 | — | +0.00 | -1.68 | -12.53 | — |
| 2026-08-26 | `BNS` | 15 | $93.10 | $92.65 | -6.75 | — | +0.00 | -6.75 | +55.65 | — |
| 2026-08-26 | `BZ` | 91 | $16.29 | $16.77 | +43.68 | $18.84 | +188.37 | +232.05 | +135.59 | +323.96 |
| 2026-08-26 | `DKS` | 9 | $124.31 | $121.87 | -21.96 | $129.66 | +70.11 | +48.15 | -184.41 | -114.30 |
| 2026-08-26 | `EH` | 273 | $4.83 | $4.77 | -16.38 | — | +0.00 | -16.38 | -90.09 | — |
| 2026-08-26 | `GFI` | 29 | $48.87 | $48.24 | -18.27 | — | +0.00 | -18.27 | +10.15 | — |
| 2026-08-26 | `GRRR` | 100 | $14.04 | $14.03 | -1.00 | — | +0.00 | -1.00 | +11.00 | — |
| 2026-08-26 | `SHMD` | 306 | $3.42 | $3.38 | -12.24 | — | +0.00 | -12.24 | -356.49 | — |
| 2026-08-26 | `SLQT` | 2307 | — | $0.58 | +0.00 | $0.55 | -76.13 | -76.13 | +0.00 | -76.13 |
| 2026-08-26 | `TIGR` | 258 | — | $5.21 | +0.00 | $5.46 | +64.50 | +64.50 | +0.00 | +64.50 |
| 2026-08-26 | `ANF` | 10 | — | $131.37 | +0.00 | $147.75 | +163.80 | +163.80 | +0.00 | +163.80 |
| 2026-08-26 | `BBWI` | 73 | — | $18.26 | +0.00 | $18.90 | +46.72 | +46.72 | +0.00 | +46.72 |
| 2026-08-26 | `BOX` | 39 | — | $34.30 | +0.00 | $33.39 | -35.49 | -35.49 | +0.00 | -35.49 |
| 2026-08-26 | `DY` | 4 | — | $326.91 | +0.00 | $310.91 | -64.00 | -64.00 | +0.00 | -64.00 |
| 2026-08-27 | `BZ` | 91 | $18.84 | $18.50 | -30.94 | — | +0.00 | -30.94 | +293.02 | — |
| 2026-08-27 | `DKS` | 9 | $129.66 | $128.73 | -8.37 | — | +0.00 | -8.37 | -122.67 | — |
| 2026-08-27 | `SLQT` | 2307 | $0.55 | $0.53 | -46.14 | — | +0.00 | -46.14 | -122.27 | — |
| 2026-08-27 | `TIGR` | 258 | $5.46 | $5.49 | +7.74 | — | +0.00 | +7.74 | +72.24 | — |
| 2026-08-27 | `ANF` | 10 | $147.75 | $144.70 | -30.50 | — | +0.00 | -30.50 | +133.30 | — |
| 2026-08-27 | `BBWI` | 73 | $18.90 | $18.69 | -15.33 | — | +0.00 | -15.33 | +31.39 | — |
| 2026-08-27 | `BOX` | 39 | $33.39 | $33.79 | +15.60 | — | +0.00 | +15.60 | -19.89 | — |
| 2026-08-27 | `DY` | 4 | $310.91 | $314.90 | +15.96 | — | +0.00 | +15.96 | -48.04 | — |
| 2026-08-27 | `NVDA` | 48 | — | $222.86 | +0.00 | $227.98 | +245.76 | +245.76 | +0.00 | +245.76 |
| 2026-08-28 | `NVDA` | 48 | $227.98 | $227.36 | -29.76 | — | +0.00 | -29.76 | +216.00 | — |
| 2026-08-28 | `ADSK` | 5 | — | $261.16 | +0.00 | $260.66 | -2.50 | -2.50 | +0.00 | -2.50 |
| 2026-08-28 | `BBAR` | 92 | — | $15.01 | +0.00 | $14.47 | -49.68 | -49.68 | +0.00 | -49.68 |
| 2026-08-28 | `ESTC` | 13 | — | $103.89 | +0.00 | $99.91 | -51.74 | -51.74 | +0.00 | -51.74 |
| 2026-08-28 | `FINV` | 357 | — | $3.88 | +0.00 | $3.40 | -171.36 | -171.36 | +0.00 | -171.36 |
| 2026-08-28 | `FRO` | 31 | — | $44.40 | +0.00 | $44.19 | -6.51 | -6.51 | +0.00 | -6.51 |
| 2026-08-28 | `GAP` | 56 | — | $24.69 | +0.00 | $23.48 | -67.76 | -67.76 | +0.00 | -67.76 |
| 2026-08-28 | `HAFN` | 166 | — | $8.35 | +0.00 | $8.47 | +19.92 | +19.92 | +0.00 | +19.92 |
| 2026-08-28 | `IREN` | 36 | — | $37.65 | +0.00 | $35.45 | -79.02 | -79.02 | +0.00 | -79.02 |
| 2026-08-31 | `ADSK` | 5 | $260.66 | $257.71 | -14.75 | — | +0.00 | -14.75 | -17.25 | — |
| 2026-08-31 | `BBAR` | 92 | $14.47 | $14.88 | +37.72 | — | +0.00 | +37.72 | -11.96 | — |
| 2026-08-31 | `ESTC` | 13 | $99.91 | $98.00 | -24.83 | — | +0.00 | -24.83 | -76.57 | — |
| 2026-08-31 | `FINV` | 357 | $3.40 | $3.39 | -3.57 | — | +0.00 | -3.57 | -174.93 | — |
| 2026-08-31 | `FRO` | 31 | $44.19 | $44.85 | +20.46 | — | +0.00 | +20.46 | +13.95 | — |
| 2026-08-31 | `GAP` | 56 | $23.48 | $22.98 | -28.00 | — | +0.00 | -28.00 | -95.76 | — |
| 2026-08-31 | `HAFN` | 166 | $8.47 | $8.53 | +9.96 | — | +0.00 | +9.96 | +29.88 | — |
| 2026-08-31 | `IREN` | 36 | $35.45 | $35.81 | +12.96 | — | +0.00 | +12.96 | -66.06 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AI` | 124 | — | $10.74 | +0.00 | $10.90 | +19.22 | +19.22 | +0.00 | +19.22 |
| 2026-09-03 | `AVGO` | 3 | — | $351.74 | +0.00 | $357.16 | +16.26 | +16.26 | +0.00 | +16.26 |
| 2026-09-03 | `CHPT` | 193 | — | $6.90 | +0.00 | $9.08 | +420.74 | +420.74 | +0.00 | +420.74 |
| 2026-09-03 | `CIEN` | 3 | — | $354.49 | +0.00 | $317.46 | -111.09 | -111.09 | +0.00 | -111.09 |
| 2026-09-03 | `CPB` | 59 | — | $22.32 | +0.00 | $22.13 | -11.21 | -11.21 | +0.00 | -11.21 |
| 2026-09-03 | `FIVE` | 5 | — | $257.00 | +0.00 | $239.96 | -85.20 | -85.20 | +0.00 | -85.20 |
| 2026-09-03 | `HPE` | 28 | — | $47.60 | +0.00 | $54.44 | +191.52 | +191.52 | +0.00 | +191.52 |
| 2026-09-03 | `MEI` | 88 | — | $15.09 | +0.00 | $15.32 | +20.24 | +20.24 | +0.00 | +20.24 |
| 2026-09-04 | `AI` | 124 | $10.90 | $10.91 | +1.24 | — | +0.00 | +1.24 | +20.46 | — |
| 2026-09-04 | `AVGO` | 3 | $357.16 | $359.70 | +7.62 | — | +0.00 | +7.62 | +23.88 | — |
| 2026-09-04 | `CHPT` | 193 | $9.08 | $9.28 | +38.60 | — | +0.00 | +38.60 | +459.34 | — |
| 2026-09-04 | `CIEN` | 3 | $317.46 | $321.67 | +12.63 | — | +0.00 | +12.63 | -98.46 | — |
| 2026-09-04 | `CPB` | 59 | $22.13 | $22.10 | -1.77 | — | +0.00 | -1.77 | -12.98 | — |
| 2026-09-04 | `FIVE` | 5 | $239.96 | $238.88 | -5.40 | — | +0.00 | -5.40 | -90.60 | — |
| 2026-09-04 | `HPE` | 28 | $54.44 | $53.85 | -16.52 | — | +0.00 | -16.52 | +175.00 | — |
| 2026-09-04 | `MEI` | 88 | $15.32 | $15.34 | +1.76 | — | +0.00 | +1.76 | +22.00 | — |
| 2026-09-04 | `AMBA` | 22 | — | $63.18 | +0.00 | $62.89 | -6.38 | -6.38 | +0.00 | -6.38 |
| 2026-09-04 | `ASAN` | 159 | — | $8.74 | +0.00 | $8.81 | +11.13 | +11.13 | +0.00 | +11.13 |
| 2026-09-04 | `DOCU` | 20 | — | $68.52 | +0.00 | $68.41 | -2.20 | -2.20 | +0.00 | -2.20 |
| 2026-09-04 | `DOMO` | 384 | — | $3.62 | +0.00 | $3.88 | +101.76 | +101.76 | +0.00 | +101.76 |
| 2026-09-04 | `GWRE` | 8 | — | $167.55 | +0.00 | $162.42 | -41.04 | -41.04 | +0.00 | -41.04 |
| 2026-09-04 | `IOT` | 30 | — | $44.90 | +0.00 | $40.20 | -141.00 | -141.00 | +0.00 | -141.00 |
| 2026-09-04 | `LULU` | 14 | — | $98.15 | +0.00 | $100.61 | +34.44 | +34.44 | +0.00 | +34.44 |
| 2026-09-04 | `MAMA` | 88 | — | $15.70 | +0.00 | $15.16 | -47.52 | -47.52 | +0.00 | -47.52 |
| 2026-09-08 | `AMBA` | 22 | $62.89 | $63.83 | +20.68 | — | +0.00 | +20.68 | +14.30 | — |
| 2026-09-08 | `ASAN` | 159 | $8.81 | $8.73 | -12.72 | — | +0.00 | -12.72 | -1.59 | — |
| 2026-09-08 | `DOCU` | 20 | $68.41 | $67.05 | -27.20 | — | +0.00 | -27.20 | -29.40 | — |
| 2026-09-08 | `DOMO` | 384 | $3.88 | $3.84 | -15.36 | — | +0.00 | -15.36 | +86.40 | — |
| 2026-09-08 | `GWRE` | 8 | $162.42 | $160.52 | -15.20 | — | +0.00 | -15.20 | -56.24 | — |
| 2026-09-08 | `IOT` | 30 | $40.20 | $39.56 | -19.20 | — | +0.00 | -19.20 | -160.20 | — |
| 2026-09-08 | `LULU` | 14 | $100.61 | $100.58 | -0.42 | — | +0.00 | -0.42 | +34.02 | — |
| 2026-09-08 | `MAMA` | 88 | $15.16 | $15.20 | +3.52 | — | +0.00 | +3.52 | -44.00 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +840.92 | INO, VOR | — | $21.06 | $10,769.53 | INO×6172, VOR×223 |
| 2026-08-14 | +5.50 | $21.06 | INO×6172, VOR×223 | $10,963.61 | +194.08 | +25.69 | BTBT, ARX, AIRO, MH, CLBT, EU, LUNR, NMAX | INO, VOR | $11.72 | $10,869.01 | BTBT×906, ARX×69, AIRO×122, MH×100, CLBT×125, EU×1152, LUNR×70, NMAX×137 |
| 2026-08-17 | +2.25 | $11.72 | BTBT×906, ARX×69, AIRO×122, MH×100, CLBT×125, EU×1152, LUNR×70, NMAX×137 | $10,935.77 | +66.76 | +0.00 | — | BTBT, ARX, AIRO, MH, CLBT, EU, LUNR, NMAX | $10,894.88 | $10,894.88 | — |
| 2026-08-18 | -6.20 | $10,894.88 | — | $10,894.88 | +0.00 | +0.00 | — | — | $10,894.88 | $10,894.88 | — |
| 2026-08-19 | -7.20 | $10,894.88 | — | $10,894.88 | +0.00 | +0.00 | — | — | $10,894.88 | $10,894.88 | — |
| 2026-08-20 | +1.12 | $10,894.88 | — | $10,894.88 | +0.00 | +92.49 | EL, TOYO, DVLT, AAP, AEG, ALVO, ATAT, ATHM | — | $105.03 | $10,940.84 | EL×13, TOYO×307, DVLT×4539, AAP×29, AEG×151, ALVO×350, ATAT×39, ATHM×60 |
| 2026-08-21 | +3.25 | $105.03 | EL×13, TOYO×307, DVLT×4539, AAP×29, AEG×151, ALVO×350, ATAT×39, ATHM×60 | $10,983.65 | +42.81 | +216.34 | FUTU, DE, WMT, BEKE, BJ, BKE, PSEC | EL, TOYO, DVLT, AEG, ALVO, ATAT, ATHM | $251.18 | $11,133.93 | AAP×29, FUTU×12, DE×2, WMT×13, BEKE×77, BJ×14, BKE×32, PSEC×602 |
| 2026-08-24 | -5.17 | $251.18 | AAP×29, FUTU×12, DE×2, WMT×13, BEKE×77, BJ×14, BKE×32, PSEC×602 | $11,183.76 | +49.83 | +0.00 | — | AAP, FUTU, DE, WMT, BEKE, BJ, BKE, PSEC | $11,161.27 | $11,161.27 | — |
| 2026-08-25 | +1.80 | $11,161.27 | — | $11,161.27 | -0.00 | -396.53 | BMO, BNS, BZ, DKS, EH, GFI, GRRR, SHMD | — | $346.34 | $10,744.58 | BMO×7, BNS×15, BZ×91, DKS×9, EH×273, GFI×29, GRRR×100, SHMD×306 |
| 2026-08-26 | +2.02 | $346.34 | BMO×7, BNS×15, BZ×91, DKS×9, EH×273, GFI×29, GRRR×100, SHMD×306 | $10,709.98 | -34.60 | +357.88 | SLQT, TIGR, ANF, BBWI, BOX, DY | BMO, BNS, EH, GFI, GRRR, SHMD | $57.77 | $11,019.73 | BZ×91, DKS×9, SLQT×2307, TIGR×258, ANF×10, BBWI×73, BOX×39, DY×4 |
| 2026-08-27 | — | $57.77 | BZ×91, DKS×9, SLQT×2307, TIGR×258, ANF×10, BBWI×73, BOX×39, DY×4 | $10,927.75 | -91.98 | +245.76 | NVDA | BZ, DKS, SLQT, TIGR, ANF, BBWI, BOX, DY | $192.66 | $11,135.70 | NVDA×48 |
| 2026-08-28 | +0.75 | $192.66 | NVDA×48 | $11,105.94 | -29.76 | -408.65 | ADSK, BBAR, ESTC, FINV, FRO, GAP, HAFN, IREN | NVDA | $161.17 | $10,675.33 | ADSK×5, BBAR×92, ESTC×13, FINV×357, FRO×31, GAP×56, HAFN×166, IREN×36 |
| 2026-08-31 | -5.85 | $161.17 | ADSK×5, BBAR×92, ESTC×13, FINV×357, FRO×31, GAP×56, HAFN×166, IREN×36 | $10,685.28 | +9.95 | +0.00 | — | ADSK, BBAR, ESTC, FINV, FRO, GAP, HAFN, IREN | $10,665.31 | $10,665.31 | — |
| 2026-09-01 | -6.30 | $10,665.31 | — | $10,665.31 | -0.00 | +0.00 | — | — | $10,665.31 | $10,665.31 | — |
| 2026-09-02 | -3.83 | $10,665.31 | — | $10,665.31 | -0.00 | +0.00 | — | — | $10,665.31 | $10,665.31 | — |
| 2026-09-03 | -0.90 | $10,665.31 | — | $10,665.31 | -0.00 | +460.48 | AI, AVGO, CHPT, CIEN, CPB, FIVE, HPE, MEI | — | $602.51 | $11,108.36 | AI×124, AVGO×3, CHPT×193, CIEN×3, CPB×59, FIVE×5, HPE×28, MEI×88 |
| 2026-09-04 | +2.25 | $602.51 | AI×124, AVGO×3, CHPT×193, CIEN×3, CPB×59, FIVE×5, HPE×28, MEI×88 | $11,146.52 | +38.16 | -90.81 | AMBA, ASAN, DOCU, DOMO, GWRE, IOT, LULU, MAMA | AI, AVGO, CHPT, CIEN, CPB, FIVE, HPE, MEI | $127.70 | $11,018.17 | AMBA×22, ASAN×159, DOCU×20, DOMO×384, GWRE×8, IOT×30, LULU×14, MAMA×88 |
| 2026-09-08 | -11.47 | $127.70 | AMBA×22, ASAN×159, DOCU×20, DOMO×384, GWRE×8, IOT×30, LULU×14, MAMA×88 | $10,952.27 | -65.90 | +0.00 | — | AMBA, ASAN, DOCU, DOMO, GWRE, IOT, LULU, MAMA | $10,932.12 | $10,932.12 | — |
| 2026-09-09 | -13.95 | $10,932.12 | — | $10,932.12 | -0.00 | +0.00 | — | — | $10,932.12 | $10,932.12 | — |
| 2026-09-10 | -13.28 | $10,932.12 | — | $10,932.12 | -0.00 | +0.00 | — | — | $10,932.12 | $10,932.12 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 6172 | $0.81 | $68.51 | — | $4,932.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+13.2; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 223 | $22.01 | $2.88 | — | $21.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+0.3; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.06 | ▲ close $10,769.53 vs 09:30 $10,000.00 (session +840.92) | 16:00 close · cash $21.06 · equity $10,769.53 vs 09:30 $10,000.00 (+769.53; session marks +840.92) · 2 name(s) marked open→close (per-name table). INO×6172 09:30 $0.81 → close $0.90 +555.48; VOR×223 09:30 $22.01 → close $23.29 +285.44 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.06 | ▲ 09:30 equity $10,963.61 vs yday $10,769.53 (+194.08) | 09:30 open · cash $21.06 (unchanged overnight, no fees) · equity $10,963.61 vs prior close $10,769.53 (+194.08) · 2 name(s) re-marked at the open (per-name table). INO×6172 yday $0.90 → 09:30 $0.93 +185.16; VOR×223 yday $23.29 → 09:30 $23.33 +8.92 | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 6172 | $0.93 | $76.99 | $+595.14 | $5,684.04 | ▲ +595.14 after sell → book $10,886.63; vs 09:30 mark -76.98 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 223 | $23.33 | $2.96 | $+288.53 | $10,883.67 | ▲ +288.53 after sell → book $10,883.67; vs 09:30 mark -2.96 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 906 | $1.50 | $11.69 | — | $9,512.99 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 69 | $19.57 | $2.20 | — | $8,160.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 122 | $11.12 | $2.36 | — | $6,801.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 100 | $13.55 | $2.29 | — | $5,444.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 125 | $10.83 | $2.37 | — | $4,088.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-30.1; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 1152 | $1.18 | $14.86 | — | $2,713.84 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 70 | $19.17 | $2.20 | — | $1,369.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🔴 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 137 | $9.89 | $2.40 | — | $11.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.72 | ▲ close $10,869.01 vs 09:30 $10,963.61 (session +25.69) | 16:00 close · cash $11.72 · equity $10,869.01 vs 09:30 $10,963.61 (-94.60; session marks +25.69) · 8 name(s) marked open→close (per-name table). BTBT×906 09:30 $1.50 → close $1.57 +63.42; ARX×69 09:30 $19.57 → close $19.58 +0.69; AIRO×122 09:30 $11.12 → close $9.57 -189.10; MH×100 09:30 $13.55 → close $13.10 -45.00; CLBT×125 09:30 $10.83 → close $11.14 +38.75; EU×1152 09:30 $1.18 → close $1.21 +34.56; LUNR×70 09:30 $19.17 → close $19.01 -11.20; NMAX×137 09:30 $9.89 → close $10.87 +133.57 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.72 | ▲ 09:30 equity $10,935.77 vs yday $10,869.01 (+66.76) | 09:30 open · cash $11.72 (unchanged overnight, no fees) · equity $10,935.77 vs prior close $10,869.01 (+66.76) · 8 name(s) re-marked at the open (per-name table). BTBT×906 yday $1.57 → 09:30 $1.52 -45.30; ARX×69 yday $19.58 → 09:30 $19.57 -0.69; AIRO×122 yday $9.57 → 09:30 $9.57 +0.00; MH×100 yday $13.10 → 09:30 $13.16 +6.00; CLBT×125 yday $11.14 → 09:30 $11.19 +6.25; EU×1152 yday $1.21 → 09:30 $1.21 +0.00; LUNR×70 yday $19.01 → 09:30 $20.25 +86.80; NMAX×137 yday $10.87 → 09:30 $10.97 +13.70 | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 906 | $1.52 | $11.85 | $-5.42 | $1,376.99 | ▼ -5.42 after sell → book $10,923.92; vs 09:30 mark -11.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 69 | $19.57 | $2.22 | $-4.42 | $2,725.10 | ▼ -4.42 after sell → book $10,921.70; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 122 | $9.57 | $2.39 | $-193.84 | $3,890.26 | ▼ -193.84 after sell → book $10,919.32; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 100 | $13.16 | $2.32 | $-43.61 | $5,203.94 | ▼ -43.61 after sell → book $10,917.00; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 125 | $11.19 | $2.40 | $+40.24 | $6,600.29 | ▲ +40.24 after sell → book $10,914.60; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `EU` | 1152 | $1.21 | $15.06 | $+4.64 | $7,979.15 | ▲ +4.64 after sell → book $10,899.54; vs 09:30 mark -15.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 70 | $20.25 | $2.22 | $+71.18 | $9,394.43 | ▲ +71.18 after sell → book $10,897.32; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `NMAX` | 137 | $10.97 | $2.44 | $+142.44 | $10,894.88 | ▲ +142.44 after sell → book $10,894.88; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,894.88 | ▲ close $10,894.88 vs 09:30 $10,935.77 (session +0.00) | 16:00 close · cash $10,894.88 · no lots left · equity $10,894.88. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,894.88 | ▲ 09:30 equity $10,894.88 vs yday $10,894.88 (+0.00) | 09:30 open · cash $10,894.88 · no holdings · equity $10,894.88 vs prior close $10,894.88 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,894.88 | ▲ close $10,894.88 vs 09:30 $10,894.88 (session +0.00) | 16:00 close · cash $10,894.88 · no lots left · equity $10,894.88. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,894.88 | ▲ 09:30 equity $10,894.88 vs yday $10,894.88 (+0.00) | 09:30 open · cash $10,894.88 · no holdings · equity $10,894.88 vs prior close $10,894.88 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,894.88 | ▲ close $10,894.88 vs 09:30 $10,894.88 (session +0.00) | 16:00 close · cash $10,894.88 · no lots left · equity $10,894.88. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,894.88 | ▲ 09:30 equity $10,894.88 vs yday $10,894.88 (+0.00) | 09:30 open · cash $10,894.88 · no holdings · equity $10,894.88 vs prior close $10,894.88 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 13 | $97.43 | $2.03 | — | $9,626.26 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; leftover $1361.86 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 307 | $4.43 | $3.96 | — | $8,262.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; leftover $1361.86 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 4539 | $0.30 | $27.23 | — | $6,873.36 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; leftover $1361.86 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 29 | $46.85 | $2.08 | — | $5,512.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; leftover $1361.86 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 151 | $9.01 | $2.44 | — | $4,149.68 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; leftover $1361.86 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 350 | $3.89 | $4.51 | — | $2,783.66 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; leftover $1361.86 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 39 | $34.05 | $2.11 | — | $1,453.60 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; leftover $1361.86 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 60 | $22.44 | $2.17 | — | $105.03 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; leftover $1361.86 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $105.03 | ▲ close $10,940.84 vs 09:30 $10,894.88 (session +92.49) | 16:00 close · cash $105.03 · equity $10,940.84 vs 09:30 $10,894.88 (+45.96; session marks +92.49) · 8 name(s) marked open→close (per-name table). EL×13 09:30 $97.43 → close $96.15 -16.64; TOYO×307 09:30 $4.43 → close $4.51 +26.09; DVLT×4539 09:30 $0.30 → close $0.32 +90.78; AAP×29 09:30 $46.85 → close $42.39 -129.34; AEG×151 09:30 $9.01 → close $9.01 +0.00; ALVO×350 09:30 $3.89 → close $4.27 +133.00; ATAT×39 09:30 $34.05 → close $34.25 +7.80; ATHM×60 09:30 $22.44 → close $22.12 -19.20 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $105.03 | ▲ 09:30 equity $10,983.65 vs yday $10,940.84 (+42.81) | 09:30 open · cash $105.03 (unchanged overnight, no fees) · equity $10,983.65 vs prior close $10,940.84 (+42.81) · 8 name(s) re-marked at the open (per-name table). EL×13 yday $96.15 → 09:30 $96.75 +7.80; TOYO×307 yday $4.51 → 09:30 $4.68 +50.66; DVLT×4539 yday $0.32 → 09:30 $0.31 -45.39; AAP×29 yday $42.39 → 09:30 $42.41 +0.58; AEG×151 yday $9.01 → 09:30 $9.04 +4.53; ALVO×350 yday $4.27 → 09:30 $4.32 +17.50; ATAT×39 yday $34.25 → 09:30 $34.31 +2.34; ATHM×60 yday $22.12 → 09:30 $22.20 +4.80 | — |
| 2026-08-21 09:30 ET | **SELL** | `EL` | 13 | $96.75 | $2.05 | $-12.92 | $1,360.74 | ▼ -12.92 after sell → book $10,981.61; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TOYO` | 307 | $4.68 | $4.02 | $+68.77 | $2,793.47 | ▲ +68.77 after sell → book $10,977.58; vs 09:30 mark -4.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DVLT` | 4539 | $0.31 | $28.45 | $-10.30 | $4,172.11 | ▼ -10.30 after sell → book $10,949.13; vs 09:30 mark -28.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AEG` | 151 | $9.04 | $2.48 | $-0.39 | $5,534.67 | ▼ -0.39 after sell → book $10,946.65; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ALVO` | 350 | $4.32 | $4.59 | $+141.40 | $7,042.09 | ▲ +141.40 after sell → book $10,942.07; vs 09:30 mark -4.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 39 | $34.31 | $2.13 | $+5.91 | $8,378.05 | ▲ +5.91 after sell → book $10,939.94; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATHM` | 60 | $22.20 | $2.19 | $-18.76 | $9,707.86 | ▼ -18.76 after sell → book $10,937.75; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 12 | $115.18 | $2.03 | — | $8,323.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1386.84 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $7,075.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1386.84 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `WMT` | 13 | $103.69 | $2.03 | — | $5,725.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; ret5=-10.3; leftover $1386.84 | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 77 | $17.93 | $2.22 | — | $4,341.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=+0.2; leftover $1386.84 | join🟢 sector🟢 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 14 | $93.98 | $2.03 | — | $3,024.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.4; leftover $1386.84 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 32 | $43.08 | $2.09 | — | $1,643.54 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; leftover $1386.84 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 602 | $2.30 | $7.77 | — | $251.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; leftover $1386.84 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $251.18 | ▲ close $11,133.93 vs 09:30 $10,983.65 (session +216.34) | 16:00 close · cash $251.18 · equity $11,133.93 vs 09:30 $10,983.65 (+150.28; session marks +216.34) · 8 name(s) marked open→close (per-name table). AAP×29 09:30 $42.41 → close $42.58 +4.93; FUTU×12 09:30 $115.18 → close $123.64 +101.52; DE×2 09:30 $623.26 → close $647.47 +48.42; WMT×13 09:30 $103.69 → close $103.70 +0.13; BEKE×77 09:30 $17.93 → close $17.75 -14.24; BJ×14 09:30 $93.98 → close $96.42 +34.16; BKE×32 09:30 $43.08 → close $43.81 +23.36; PSEC×602 09:30 $2.30 → close $2.33 +18.06 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $251.18 | ▲ 09:30 equity $11,183.76 vs yday $11,133.93 (+49.83) | 09:30 open · cash $251.18 (unchanged overnight, no fees) · equity $11,183.76 vs prior close $11,133.93 (+49.83) · 8 name(s) re-marked at the open (per-name table). AAP×29 yday $42.58 → 09:30 $43.05 +13.63; FUTU×12 yday $123.64 → 09:30 $121.00 -31.68; DE×2 yday $647.47 → 09:30 $653.04 +11.14; WMT×13 yday $103.70 → 09:30 $104.14 +5.72; BEKE×77 yday $17.75 → 09:30 $18.05 +23.48; BJ×14 yday $96.42 → 09:30 $97.02 +8.40; BKE×32 yday $43.81 → 09:30 $44.22 +13.12; PSEC×602 yday $2.33 → 09:30 $2.34 +6.02 | — |
| 2026-08-24 09:30 ET | **SELL** | `AAP` | 29 | $43.05 | $2.10 | $-114.37 | $1,497.53 | ▼ -114.37 after sell → book $11,181.66; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 12 | $121.00 | $2.05 | $+65.77 | $2,947.48 | ▲ +65.77 after sell → book $11,179.62; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.04 | $2.02 | $+55.55 | $4,251.55 | ▲ +55.55 after sell → book $11,177.60; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `WMT` | 13 | $104.14 | $2.05 | $+1.77 | $5,603.32 | ▲ +1.77 after sell → book $11,175.55; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 77 | $18.05 | $2.24 | $+4.77 | $6,991.31 | ▲ +4.77 after sell → book $11,173.31; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 14 | $97.02 | $2.05 | $+38.48 | $8,347.53 | ▲ +38.48 after sell → book $11,171.25; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 32 | $44.22 | $2.11 | $+32.29 | $9,760.47 | ▲ +32.29 after sell → book $11,169.15; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PSEC` | 602 | $2.34 | $7.88 | $+8.44 | $11,161.27 | ▲ +8.44 after sell → book $11,161.27; vs 09:30 mark -7.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,161.27 | ▲ close $11,161.27 vs 09:30 $11,183.76 (session +0.00) | 16:00 close · cash $11,161.27 · no lots left · equity $11,161.27. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,161.27 | ▲ 09:30 equity $11,161.27 vs yday $11,161.27 (-0.00) | 09:30 open · cash $11,161.27 · no holdings · equity $11,161.27 vs prior close $11,161.27 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 7 | $175.01 | $2.01 | — | $9,934.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.0; leftover $1395.16 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 15 | $88.94 | $2.04 | — | $8,598.05 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; leftover $1395.16 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 91 | $15.28 | $2.26 | — | $7,205.31 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-0.7; leftover $1395.16 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 9 | $142.36 | $2.02 | — | $5,922.05 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.6; leftover $1395.16 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 273 | $5.10 | $3.52 | — | $4,526.23 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.9; leftover $1395.16 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 29 | $47.89 | $2.08 | — | $3,135.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+14.0; leftover $1395.16 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 100 | $13.92 | $2.29 | — | $1,741.05 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.9; leftover $1395.16 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 306 | $4.54 | $3.95 | — | $346.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-14.6; leftover $1395.16 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $346.34 | ▼ close $10,744.58 vs 09:30 $11,161.27 (session -396.53) | 16:00 close · cash $346.34 · equity $10,744.58 vs 09:30 $11,161.27 (-416.69; session marks -396.53) · 8 name(s) marked open→close (per-name table). BMO×7 09:30 $175.01 → close $173.46 -10.85; BNS×15 09:30 $88.94 → close $93.10 +62.40; BZ×91 09:30 $15.28 → close $16.29 +91.91; DKS×9 09:30 $142.36 → close $124.31 -162.45; EH×273 09:30 $5.10 → close $4.83 -73.71; GFI×29 09:30 $47.89 → close $48.87 +28.42; GRRR×100 09:30 $13.92 → close $14.04 +12.00; SHMD×306 09:30 $4.54 → close $3.42 -344.25 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $346.34 | ▼ 09:30 equity $10,709.98 vs yday $10,744.58 (-34.60) | 09:30 open · cash $346.34 (unchanged overnight, no fees) · equity $10,709.98 vs prior close $10,744.58 (-34.60) · 8 name(s) re-marked at the open (per-name table). BMO×7 yday $173.46 → 09:30 $173.22 -1.68; BNS×15 yday $93.10 → 09:30 $92.65 -6.75; BZ×91 yday $16.29 → 09:30 $16.77 +43.68; DKS×9 yday $124.31 → 09:30 $121.87 -21.96; EH×273 yday $4.83 → 09:30 $4.77 -16.38; GFI×29 yday $48.87 → 09:30 $48.24 -18.27; GRRR×100 yday $14.04 → 09:30 $14.03 -1.00; SHMD×306 yday $3.42 → 09:30 $3.38 -12.24 | — |
| 2026-08-26 09:30 ET | **SELL** | `BMO` | 7 | $173.22 | $2.03 | $-16.57 | $1,556.85 | ▼ -16.57 after sell → book $10,707.95; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BNS` | 15 | $92.65 | $2.06 | $+51.56 | $2,944.54 | ▲ +51.56 after sell → book $10,705.89; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EH` | 273 | $4.77 | $3.58 | $-97.19 | $4,243.17 | ▼ -97.19 after sell → book $10,702.31; vs 09:30 mark -3.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GFI` | 29 | $48.24 | $2.10 | $+5.97 | $5,640.03 | ▲ +5.97 after sell → book $10,700.21; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GRRR` | 100 | $14.03 | $2.32 | $+6.39 | $7,040.72 | ▲ +6.39 after sell → book $10,697.90; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `SHMD` | 306 | $3.38 | $4.01 | $-364.45 | $8,070.99 | ▼ -364.45 after sell → book $10,693.89; vs 09:30 mark -4.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 2307 | $0.58 | $20.37 | — | $6,705.64 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-27.5; leftover $1345.16 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 258 | $5.21 | $3.33 | — | $5,358.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ret5=+14.3; leftover $1345.16 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ANF` | 10 | $131.37 | $2.02 | — | $4,042.41 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.3; leftover $1345.16 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 73 | $18.26 | $2.21 | — | $2,707.22 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.4; leftover $1345.16 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 39 | $34.30 | $2.11 | — | $1,367.41 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.7; leftover $1345.16 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 4 | $326.91 | $2.00 | — | $57.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-15.2; leftover $1345.16 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.77 | ▲ close $11,019.73 vs 09:30 $10,709.98 (session +357.88) | 16:00 close · cash $57.77 · equity $11,019.73 vs 09:30 $10,709.98 (+309.75; session marks +357.88) · 8 name(s) marked open→close (per-name table). BZ×91 09:30 $16.77 → close $18.84 +188.37; DKS×9 09:30 $121.87 → close $129.66 +70.11; SLQT×2307 09:30 $0.58 → close $0.55 -76.13; TIGR×258 09:30 $5.21 → close $5.46 +64.50; ANF×10 09:30 $131.37 → close $147.75 +163.80; BBWI×73 09:30 $18.26 → close $18.90 +46.72; BOX×39 09:30 $34.30 → close $33.39 -35.49; DY×4 09:30 $326.91 → close $310.91 -64.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.77 | ▼ 09:30 equity $10,927.75 vs yday $11,019.73 (-91.98) | 09:30 open · cash $57.77 (unchanged overnight, no fees) · equity $10,927.75 vs prior close $11,019.73 (-91.98) · 8 name(s) re-marked at the open (per-name table). BZ×91 yday $18.84 → 09:30 $18.50 -30.94; DKS×9 yday $129.66 → 09:30 $128.73 -8.37; SLQT×2307 yday $0.55 → 09:30 $0.53 -46.14; TIGR×258 yday $5.46 → 09:30 $5.49 +7.74; ANF×10 yday $147.75 → 09:30 $144.70 -30.50; BBWI×73 yday $18.90 → 09:30 $18.69 -15.33; BOX×39 yday $33.39 → 09:30 $33.79 +15.60; DY×4 yday $310.91 → 09:30 $314.90 +15.96 | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 91 | $18.50 | $2.29 | $+288.47 | $1,738.98 | ▲ +288.47 after sell → book $10,925.46; vs 09:30 mark -2.29 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 9 | $128.73 | $2.04 | $-126.72 | $2,895.51 | ▼ -126.72 after sell → book $10,923.42; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SLQT` | 2307 | $0.53 | $19.54 | $-162.18 | $4,098.68 | ▼ -162.18 after sell → book $10,903.88; vs 09:30 mark -19.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TIGR` | 258 | $5.49 | $3.38 | $+65.53 | $5,511.72 | ▲ +65.53 after sell → book $10,900.50; vs 09:30 mark -3.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ANF` | 10 | $144.70 | $2.04 | $+129.24 | $6,956.68 | ▲ +129.24 after sell → book $10,898.46; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BBWI` | 73 | $18.69 | $2.23 | $+26.95 | $8,318.81 | ▲ +26.95 after sell → book $10,896.22; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BOX` | 39 | $33.79 | $2.13 | $-24.12 | $9,634.50 | ▼ -24.12 after sell → book $10,894.10; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DY` | 4 | $314.90 | $2.02 | $-52.06 | $10,892.07 | ▼ -52.06 after sell → book $10,892.07; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 48 | $222.86 | $2.13 | — | $192.66 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list mover_buy; 🔵; ret5=-3.6; leftover $10892.07 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $192.66 | ▲ close $11,135.70 vs 09:30 $10,927.75 (session +245.76) | 16:00 close · cash $192.66 · equity $11,135.70 vs 09:30 $10,927.75 (+207.95; session marks +245.76) · 1 name(s) marked open→close (per-name table). NVDA×48 09:30 $222.86 → close $227.98 +245.76 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $192.66 | ▼ 09:30 equity $11,105.94 vs yday $11,135.70 (-29.76) | 09:30 open · cash $192.66 (unchanged overnight, no fees) · equity $11,105.94 vs prior close $11,135.70 (-29.76) · 1 name(s) re-marked at the open (per-name table). NVDA×48 yday $227.98 → 09:30 $227.36 -29.76 | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 48 | $227.36 | $2.23 | $+211.63 | $11,103.71 | ▲ +211.63 after sell → book $11,103.71; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 5 | $261.16 | $2.00 | — | $9,795.90 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.8; leftover $1387.96 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 92 | $15.01 | $2.27 | — | $8,412.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; leftover $1387.96 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 13 | $103.89 | $2.03 | — | $7,060.12 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.5; leftover $1387.96 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 357 | $3.88 | $4.61 | — | $5,670.35 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; leftover $1387.96 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 31 | $44.40 | $2.08 | — | $4,291.87 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; leftover $1387.96 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 56 | $24.69 | $2.16 | — | $2,907.07 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; leftover $1387.96 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 166 | $8.35 | $2.49 | — | $1,518.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; leftover $1387.96 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 36 | $37.65 | $2.10 | — | $161.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; leftover $1387.96 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $161.17 | ▼ close $10,675.33 vs 09:30 $11,105.94 (session -408.65) | 16:00 close · cash $161.17 · equity $10,675.33 vs 09:30 $11,105.94 (-430.61; session marks -408.65) · 8 name(s) marked open→close (per-name table). ADSK×5 09:30 $261.16 → close $260.66 -2.50; BBAR×92 09:30 $15.01 → close $14.47 -49.68; ESTC×13 09:30 $103.89 → close $99.91 -51.74; FINV×357 09:30 $3.88 → close $3.40 -171.36; FRO×31 09:30 $44.40 → close $44.19 -6.51; GAP×56 09:30 $24.69 → close $23.48 -67.76; HAFN×166 09:30 $8.35 → close $8.47 +19.92; IREN×36 09:30 $37.65 → close $35.45 -79.02 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $161.17 | ▲ 09:30 equity $10,685.28 vs yday $10,675.33 (+9.95) | 09:30 open · cash $161.17 (unchanged overnight, no fees) · equity $10,685.28 vs prior close $10,675.33 (+9.95) · 8 name(s) re-marked at the open (per-name table). ADSK×5 yday $260.66 → 09:30 $257.71 -14.75; BBAR×92 yday $14.47 → 09:30 $14.88 +37.72; ESTC×13 yday $99.91 → 09:30 $98.00 -24.83; FINV×357 yday $3.40 → 09:30 $3.39 -3.57; FRO×31 yday $44.19 → 09:30 $44.85 +20.46; GAP×56 yday $23.48 → 09:30 $22.98 -28.00; HAFN×166 yday $8.47 → 09:30 $8.53 +9.96; IREN×36 yday $35.45 → 09:30 $35.81 +12.96 | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 5 | $257.71 | $2.03 | $-21.28 | $1,447.69 | ▼ -21.28 after sell → book $10,683.25; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBAR` | 92 | $14.88 | $2.29 | $-16.52 | $2,814.36 | ▼ -16.52 after sell → book $10,680.96; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ESTC` | 13 | $98.00 | $2.05 | $-80.65 | $4,086.31 | ▼ -80.65 after sell → book $10,678.91; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FINV` | 357 | $3.39 | $4.67 | $-184.21 | $5,291.87 | ▼ -184.21 after sell → book $10,674.24; vs 09:30 mark -4.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FRO` | 31 | $44.85 | $2.10 | $+9.76 | $6,680.11 | ▲ +9.76 after sell → book $10,672.13; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 56 | $22.98 | $2.18 | $-100.10 | $7,964.81 | ▼ -100.10 after sell → book $10,669.95; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 166 | $8.53 | $2.53 | $+24.87 | $9,378.27 | ▲ +24.87 after sell → book $10,667.43; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `IREN` | 36 | $35.81 | $2.12 | $-70.28 | $10,665.31 | ▼ -70.28 after sell → book $10,665.31; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,665.31 | ▲ close $10,665.31 vs 09:30 $10,685.28 (session +0.00) | 16:00 close · cash $10,665.31 · no lots left · equity $10,665.31. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,665.31 | ▲ 09:30 equity $10,665.31 vs yday $10,665.31 (-0.00) | 09:30 open · cash $10,665.31 · no holdings · equity $10,665.31 vs prior close $10,665.31 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,665.31 | ▲ close $10,665.31 vs 09:30 $10,665.31 (session +0.00) | 16:00 close · cash $10,665.31 · no lots left · equity $10,665.31. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,665.31 | ▲ 09:30 equity $10,665.31 vs yday $10,665.31 (-0.00) | 09:30 open · cash $10,665.31 · no holdings · equity $10,665.31 vs prior close $10,665.31 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,665.31 | ▲ close $10,665.31 vs 09:30 $10,665.31 (session +0.00) | 16:00 close · cash $10,665.31 · no lots left · equity $10,665.31. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,665.31 | ▲ 09:30 equity $10,665.31 vs yday $10,665.31 (-0.00) | 09:30 open · cash $10,665.31 · no holdings · equity $10,665.31 vs prior close $10,665.31 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 124 | $10.74 | $2.36 | — | $9,330.57 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; leftover $1333.16 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $8,273.35 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; leftover $1333.16 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 193 | $6.90 | $2.57 | — | $6,939.08 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; leftover $1333.16 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 3 | $354.49 | $2.00 | — | $5,873.61 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; leftover $1333.16 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 59 | $22.32 | $2.17 | — | $4,554.56 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; leftover $1333.16 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 5 | $257.00 | $2.00 | — | $3,267.56 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; leftover $1333.16 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 28 | $47.60 | $2.07 | — | $1,932.68 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; leftover $1333.16 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 88 | $15.09 | $2.25 | — | $602.51 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; leftover $1333.16 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $602.51 | ▲ close $11,108.36 vs 09:30 $10,665.31 (session +460.48) | 16:00 close · cash $602.51 · equity $11,108.36 vs 09:30 $10,665.31 (+443.05; session marks +460.48) · 8 name(s) marked open→close (per-name table). AI×124 09:30 $10.74 → close $10.90 +19.22; AVGO×3 09:30 $351.74 → close $357.16 +16.26; CHPT×193 09:30 $6.90 → close $9.08 +420.74; CIEN×3 09:30 $354.49 → close $317.46 -111.09; CPB×59 09:30 $22.32 → close $22.13 -11.21; FIVE×5 09:30 $257.00 → close $239.96 -85.20; HPE×28 09:30 $47.60 → close $54.44 +191.52; MEI×88 09:30 $15.09 → close $15.32 +20.24 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $602.51 | ▲ 09:30 equity $11,146.52 vs yday $11,108.36 (+38.16) | 09:30 open · cash $602.51 (unchanged overnight, no fees) · equity $11,146.52 vs prior close $11,108.36 (+38.16) · 8 name(s) re-marked at the open (per-name table). AI×124 yday $10.90 → 09:30 $10.91 +1.24; AVGO×3 yday $357.16 → 09:30 $359.70 +7.62; CHPT×193 yday $9.08 → 09:30 $9.28 +38.60; CIEN×3 yday $317.46 → 09:30 $321.67 +12.63; CPB×59 yday $22.13 → 09:30 $22.10 -1.77; FIVE×5 yday $239.96 → 09:30 $238.88 -5.40; HPE×28 yday $54.44 → 09:30 $53.85 -16.52; MEI×88 yday $15.32 → 09:30 $15.34 +1.76 | — |
| 2026-09-04 09:30 ET | **SELL** | `AI` | 124 | $10.91 | $2.39 | $+15.70 | $1,952.96 | ▲ +15.70 after sell → book $11,144.13; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $359.70 | $2.02 | $+19.86 | $3,030.04 | ▲ +19.86 after sell → book $11,142.11; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CHPT` | 193 | $9.28 | $2.62 | $+454.16 | $4,818.46 | ▲ +454.16 after sell → book $11,139.49; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 3 | $321.67 | $2.02 | $-102.48 | $5,781.45 | ▼ -102.48 after sell → book $11,137.47; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CPB` | 59 | $22.10 | $2.19 | $-17.33 | $7,083.17 | ▼ -17.33 after sell → book $11,135.29; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 5 | $238.88 | $2.02 | $-94.63 | $8,275.54 | ▼ -94.63 after sell → book $11,133.26; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 28 | $53.85 | $2.10 | $+170.83 | $9,781.24 | ▲ +170.83 after sell → book $11,131.16; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MEI` | 88 | $15.34 | $2.28 | $+17.47 | $11,128.88 | ▲ +17.47 after sell → book $11,128.88; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 22 | $63.18 | $2.06 | — | $9,736.87 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.9; leftover $1391.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 159 | $8.74 | $2.47 | — | $8,344.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; leftover $1391.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 20 | $68.52 | $2.05 | — | $6,972.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.4; leftover $1391.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 384 | $3.62 | $4.95 | — | $5,579.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; leftover $1391.11 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 8 | $167.55 | $2.01 | — | $4,236.76 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; leftover $1391.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 30 | $44.90 | $2.08 | — | $2,887.68 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.5; leftover $1391.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 14 | $98.15 | $2.03 | — | $1,511.55 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.9; leftover $1391.11 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 88 | $15.70 | $2.25 | — | $127.70 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; leftover $1391.11 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $127.70 | ▼ close $11,018.17 vs 09:30 $11,146.52 (session -90.81) | 16:00 close · cash $127.70 · equity $11,018.17 vs 09:30 $11,146.52 (-128.35; session marks -90.81) · 8 name(s) marked open→close (per-name table). AMBA×22 09:30 $63.18 → close $62.89 -6.38; ASAN×159 09:30 $8.74 → close $8.81 +11.13; DOCU×20 09:30 $68.52 → close $68.41 -2.20; DOMO×384 09:30 $3.62 → close $3.88 +101.76; GWRE×8 09:30 $167.55 → close $162.42 -41.04; IOT×30 09:30 $44.90 → close $40.20 -141.00; LULU×14 09:30 $98.15 → close $100.61 +34.44; MAMA×88 09:30 $15.70 → close $15.16 -47.52 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $127.70 | ▼ 09:30 equity $10,952.27 vs yday $11,018.17 (-65.90) | 09:30 open · cash $127.70 (unchanged overnight, no fees) · equity $10,952.27 vs prior close $11,018.17 (-65.90) · 8 name(s) re-marked at the open (per-name table). AMBA×22 yday $62.89 → 09:30 $63.83 +20.68; ASAN×159 yday $8.81 → 09:30 $8.73 -12.72; DOCU×20 yday $68.41 → 09:30 $67.05 -27.20; DOMO×384 yday $3.88 → 09:30 $3.84 -15.36; GWRE×8 yday $162.42 → 09:30 $160.52 -15.20; IOT×30 yday $40.20 → 09:30 $39.56 -19.20; LULU×14 yday $100.61 → 09:30 $100.58 -0.42; MAMA×88 yday $15.16 → 09:30 $15.20 +3.52 | — |
| 2026-09-08 09:30 ET | **SELL** | `AMBA` | 22 | $63.83 | $2.08 | $+10.17 | $1,529.88 | ▲ +10.17 after sell → book $10,950.19; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASAN` | 159 | $8.73 | $2.50 | $-6.56 | $2,915.45 | ▼ -6.56 after sell → book $10,947.69; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOCU` | 20 | $67.05 | $2.07 | $-33.52 | $4,254.38 | ▼ -33.52 after sell → book $10,945.62; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOMO` | 384 | $3.84 | $5.03 | $+76.42 | $5,723.91 | ▲ +76.42 after sell → book $10,940.59; vs 09:30 mark -5.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GWRE` | 8 | $160.52 | $2.03 | $-60.29 | $7,006.03 | ▼ -60.29 after sell → book $10,938.55; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IOT` | 30 | $39.56 | $2.10 | $-164.38 | $8,190.73 | ▼ -164.38 after sell → book $10,936.45; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 14 | $100.58 | $2.05 | $+29.93 | $9,596.80 | ▲ +29.93 after sell → book $10,934.40; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MAMA` | 88 | $15.20 | $2.28 | $-48.53 | $10,932.12 | ▼ -48.53 after sell → book $10,932.12; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,932.12 | ▲ close $10,932.12 vs 09:30 $10,952.27 (session +0.00) | 16:00 close · cash $10,932.12 · no lots left · equity $10,932.12. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,932.12 | ▲ 09:30 equity $10,932.12 vs yday $10,932.12 (-0.00) | 09:30 open · cash $10,932.12 · no holdings · equity $10,932.12 vs prior close $10,932.12 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,932.12 | ▲ close $10,932.12 vs 09:30 $10,932.12 (session +0.00) | 16:00 close · cash $10,932.12 · no lots left · equity $10,932.12. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,932.12 | ▲ 09:30 equity $10,932.12 vs yday $10,932.12 (-0.00) | 09:30 open · cash $10,932.12 · no holdings · equity $10,932.12 vs prior close $10,932.12 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,932.12 | ▲ close $10,932.12 vs 09:30 $10,932.12 (session +0.00) | 16:00 close · cash $10,932.12 · no lots left · equity $10,932.12. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new buys |
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
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DBI` | hard_red | hard-red S=-13.28 sit; no new buys |
