# Factor mine action — `union_e_fresh_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ e_fresh, no 🚨

Cash book **+14.05%** ($11,405) · signal-only (no cash/fees) was +8.43%. Starts YES **25/26**. Fills 166 · skips 49 · realized $+1404.72.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $11,404.71.

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
| 2026-08-27 | `BBY` | 16 | — | $80.60 | +0.00 | $83.56 | +47.36 | +47.36 | +0.00 | +47.36 |
| 2026-08-27 | `BILI` | 84 | — | $16.18 | +0.00 | $16.77 | +49.14 | +49.14 | +0.00 | +49.14 |
| 2026-08-27 | `CM` | 11 | — | $118.77 | +0.00 | $114.84 | -43.23 | -43.23 | +0.00 | -43.23 |
| 2026-08-27 | `CMBT` | 76 | — | $17.78 | +0.00 | $18.28 | +38.00 | +38.00 | +0.00 | +38.00 |
| 2026-08-27 | `CSIQ` | 101 | — | $13.41 | +0.00 | $13.98 | +57.57 | +57.57 | +0.00 | +57.57 |
| 2026-08-27 | `HQY` | 14 | — | $97.16 | +0.00 | $93.39 | -52.78 | -52.78 | +0.00 | -52.78 |
| 2026-08-27 | `RY` | 6 | — | $206.82 | +0.00 | $204.54 | -13.68 | -13.68 | +0.00 | -13.68 |
| 2026-08-27 | `TD` | 11 | — | $120.17 | +0.00 | $121.09 | +10.12 | +10.12 | +0.00 | +10.12 |
| 2026-08-28 | `BBY` | 16 | $83.56 | $83.85 | +4.64 | — | +0.00 | +4.64 | +52.00 | — |
| 2026-08-28 | `BILI` | 84 | $16.77 | $16.94 | +14.70 | — | +0.00 | +14.70 | +63.84 | — |
| 2026-08-28 | `CM` | 11 | $114.84 | $115.66 | +9.02 | — | +0.00 | +9.02 | -34.21 | — |
| 2026-08-28 | `CMBT` | 76 | $18.28 | $18.58 | +22.80 | — | +0.00 | +22.80 | +60.80 | — |
| 2026-08-28 | `CSIQ` | 101 | $13.98 | $13.65 | -33.33 | — | +0.00 | -33.33 | +24.24 | — |
| 2026-08-28 | `HQY` | 14 | $93.39 | $93.62 | +3.22 | — | +0.00 | +3.22 | -49.56 | — |
| 2026-08-28 | `RY` | 6 | $204.54 | $205.50 | +5.76 | — | +0.00 | +5.76 | -7.92 | — |
| 2026-08-28 | `TD` | 11 | $121.09 | $122.07 | +10.78 | — | +0.00 | +10.78 | +20.90 | — |
| 2026-08-28 | `ADSK` | 5 | — | $261.16 | +0.00 | $260.66 | -2.50 | -2.50 | +0.00 | -2.50 |
| 2026-08-28 | `BBAR` | 91 | — | $15.01 | +0.00 | $14.47 | -49.14 | -49.14 | +0.00 | -49.14 |
| 2026-08-28 | `ESTC` | 13 | — | $103.89 | +0.00 | $99.91 | -51.74 | -51.74 | +0.00 | -51.74 |
| 2026-08-28 | `FINV` | 354 | — | $3.88 | +0.00 | $3.40 | -169.92 | -169.92 | +0.00 | -169.92 |
| 2026-08-28 | `FRO` | 30 | — | $44.40 | +0.00 | $44.19 | -6.30 | -6.30 | +0.00 | -6.30 |
| 2026-08-28 | `GAP` | 55 | — | $24.69 | +0.00 | $23.48 | -66.55 | -66.55 | +0.00 | -66.55 |
| 2026-08-28 | `HAFN` | 164 | — | $8.35 | +0.00 | $8.47 | +19.68 | +19.68 | +0.00 | +19.68 |
| 2026-08-28 | `IREN` | 36 | — | $37.65 | +0.00 | $35.45 | -79.02 | -79.02 | +0.00 | -79.02 |
| 2026-08-31 | `ADSK` | 5 | $260.66 | $257.71 | -14.75 | — | +0.00 | -14.75 | -17.25 | — |
| 2026-08-31 | `BBAR` | 91 | $14.47 | $14.88 | +37.31 | — | +0.00 | +37.31 | -11.83 | — |
| 2026-08-31 | `ESTC` | 13 | $99.91 | $98.00 | -24.83 | — | +0.00 | -24.83 | -76.57 | — |
| 2026-08-31 | `FINV` | 354 | $3.40 | $3.39 | -3.54 | — | +0.00 | -3.54 | -173.46 | — |
| 2026-08-31 | `FRO` | 30 | $44.19 | $44.85 | +19.80 | — | +0.00 | +19.80 | +13.50 | — |
| 2026-08-31 | `GAP` | 55 | $23.48 | $22.98 | -27.50 | — | +0.00 | -27.50 | -94.05 | — |
| 2026-08-31 | `HAFN` | 164 | $8.47 | $8.53 | +9.84 | — | +0.00 | +9.84 | +29.52 | — |
| 2026-08-31 | `IREN` | 36 | $35.45 | $35.81 | +12.96 | — | +0.00 | +12.96 | -66.06 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AI` | 122 | — | $10.74 | +0.00 | $10.90 | +18.91 | +18.91 | +0.00 | +18.91 |
| 2026-09-03 | `AVGO` | 3 | — | $351.74 | +0.00 | $357.16 | +16.26 | +16.26 | +0.00 | +16.26 |
| 2026-09-03 | `CHPT` | 191 | — | $6.90 | +0.00 | $9.08 | +416.38 | +416.38 | +0.00 | +416.38 |
| 2026-09-03 | `CIEN` | 3 | — | $354.49 | +0.00 | $317.46 | -111.09 | -111.09 | +0.00 | -111.09 |
| 2026-09-03 | `CPB` | 59 | — | $22.32 | +0.00 | $22.13 | -11.21 | -11.21 | +0.00 | -11.21 |
| 2026-09-03 | `FIVE` | 5 | — | $257.00 | +0.00 | $239.96 | -85.20 | -85.20 | +0.00 | -85.20 |
| 2026-09-03 | `HPE` | 27 | — | $47.60 | +0.00 | $54.44 | +184.68 | +184.68 | +0.00 | +184.68 |
| 2026-09-03 | `MEI` | 87 | — | $15.09 | +0.00 | $15.32 | +20.01 | +20.01 | +0.00 | +20.01 |
| 2026-09-04 | `AI` | 122 | $10.90 | $10.91 | +1.22 | — | +0.00 | +1.22 | +20.13 | — |
| 2026-09-04 | `AVGO` | 3 | $357.16 | $359.70 | +7.62 | — | +0.00 | +7.62 | +23.88 | — |
| 2026-09-04 | `CHPT` | 191 | $9.08 | $9.28 | +38.20 | — | +0.00 | +38.20 | +454.58 | — |
| 2026-09-04 | `CIEN` | 3 | $317.46 | $321.67 | +12.63 | — | +0.00 | +12.63 | -98.46 | — |
| 2026-09-04 | `CPB` | 59 | $22.13 | $22.10 | -1.77 | — | +0.00 | -1.77 | -12.98 | — |
| 2026-09-04 | `FIVE` | 5 | $239.96 | $238.88 | -5.40 | — | +0.00 | -5.40 | -90.60 | — |
| 2026-09-04 | `HPE` | 27 | $54.44 | $53.85 | -15.93 | — | +0.00 | -15.93 | +168.75 | — |
| 2026-09-04 | `MEI` | 87 | $15.32 | $15.34 | +1.74 | — | +0.00 | +1.74 | +21.75 | — |
| 2026-09-04 | `AMBA` | 21 | — | $63.18 | +0.00 | $62.89 | -6.09 | -6.09 | +0.00 | -6.09 |
| 2026-09-04 | `ASAN` | 157 | — | $8.74 | +0.00 | $8.81 | +10.99 | +10.99 | +0.00 | +10.99 |
| 2026-09-04 | `DOCU` | 20 | — | $68.52 | +0.00 | $68.41 | -2.20 | -2.20 | +0.00 | -2.20 |
| 2026-09-04 | `DOMO` | 380 | — | $3.62 | +0.00 | $3.88 | +100.70 | +100.70 | +0.00 | +100.70 |
| 2026-09-04 | `GWRE` | 8 | — | $167.55 | +0.00 | $162.42 | -41.04 | -41.04 | +0.00 | -41.04 |
| 2026-09-04 | `IOT` | 30 | — | $44.90 | +0.00 | $40.20 | -141.00 | -141.00 | +0.00 | -141.00 |
| 2026-09-04 | `LULU` | 14 | — | $98.15 | +0.00 | $100.61 | +34.44 | +34.44 | +0.00 | +34.44 |
| 2026-09-04 | `MAMA` | 87 | — | $15.70 | +0.00 | $15.16 | -46.98 | -46.98 | +0.00 | -46.98 |
| 2026-09-08 | `AMBA` | 21 | $62.89 | $63.83 | +19.74 | — | +0.00 | +19.74 | +13.65 | — |
| 2026-09-08 | `ASAN` | 157 | $8.81 | $8.73 | -12.56 | — | +0.00 | -12.56 | -1.57 | — |
| 2026-09-08 | `DOCU` | 20 | $68.41 | $67.05 | -27.20 | — | +0.00 | -27.20 | -29.40 | — |
| 2026-09-08 | `DOMO` | 380 | $3.88 | $3.84 | -15.20 | — | +0.00 | -15.20 | +85.50 | — |
| 2026-09-08 | `GWRE` | 8 | $162.42 | $160.52 | -15.20 | — | +0.00 | -15.20 | -56.24 | — |
| 2026-09-08 | `IOT` | 30 | $40.20 | $39.56 | -19.20 | — | +0.00 | -19.20 | -160.20 | — |
| 2026-09-08 | `LULU` | 14 | $100.61 | $100.58 | -0.42 | — | +0.00 | -0.42 | +34.02 | — |
| 2026-09-08 | `MAMA` | 87 | $15.16 | $15.20 | +3.48 | — | +0.00 | +3.48 | -43.50 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 8 | — | $164.43 | +0.00 | $150.28 | -113.20 | -113.20 | +0.00 | -113.20 |
| 2026-09-11 | `DBI` | 228 | — | $5.91 | +0.00 | $5.88 | -6.84 | -6.84 | +0.00 | -6.84 |
| 2026-09-11 | `ADBE` | 5 | — | $242.17 | +0.00 | $252.23 | +50.30 | +50.30 | +0.00 | +50.30 |
| 2026-09-11 | `CPRT` | 42 | — | $32.01 | +0.00 | $29.95 | -86.52 | -86.52 | +0.00 | -86.52 |
| 2026-09-11 | `DSGX` | 18 | — | $71.71 | +0.00 | $76.04 | +77.94 | +77.94 | +0.00 | +77.94 |
| 2026-09-11 | `KR` | 24 | — | $56.02 | +0.00 | $58.49 | +59.28 | +59.28 | +0.00 | +59.28 |
| 2026-09-11 | `LPTH` | 144 | — | $9.37 | +0.00 | $9.20 | -24.48 | -24.48 | +0.00 | -24.48 |
| 2026-09-11 | `REF` | 103 | — | $13.10 | +0.00 | $14.03 | +95.79 | +95.79 | +0.00 | +95.79 |
| 2026-09-14 | `ORCL` | 8 | $150.28 | $141.42 | -70.88 | — | +0.00 | -70.88 | -184.08 | — |
| 2026-09-14 | `DBI` | 228 | $5.88 | $5.86 | -4.56 | — | +0.00 | -4.56 | -11.40 | — |
| 2026-09-14 | `ADBE` | 5 | $252.23 | $261.51 | +46.40 | — | +0.00 | +46.40 | +96.70 | — |
| 2026-09-14 | `CPRT` | 42 | $29.95 | $30.63 | +28.56 | — | +0.00 | +28.56 | -57.96 | — |
| 2026-09-14 | `DSGX` | 18 | $76.04 | $77.68 | +29.52 | — | +0.00 | +29.52 | +107.46 | — |
| 2026-09-14 | `KR` | 24 | $58.49 | $59.31 | +19.68 | — | +0.00 | +19.68 | +78.96 | — |
| 2026-09-14 | `LPTH` | 144 | $9.20 | $8.85 | -50.40 | — | +0.00 | -50.40 | -74.88 | — |
| 2026-09-14 | `REF` | 103 | $14.03 | $14.16 | +13.39 | — | +0.00 | +13.39 | +109.18 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `FPS` | 163 | — | $33.14 | +0.00 | $34.84 | +277.10 | +277.10 | +0.00 | +277.10 |
| 2026-09-16 | `TCOM` | 132 | — | $40.93 | +0.00 | $40.43 | -66.00 | -66.00 | +0.00 | -66.00 |
| 2026-09-17 | `FPS` | 163 | $34.84 | $36.76 | +312.96 | — | +0.00 | +312.96 | +590.06 | — |
| 2026-09-17 | `TCOM` | 132 | $40.43 | $40.79 | +47.52 | — | +0.00 | +47.52 | -18.48 | — |
| 2026-09-17 | `ALMU` | 508 | — | $11.21 | +0.00 | $11.54 | +170.18 | +170.18 | +0.00 | +170.18 |
| 2026-09-17 | `LEN` | 70 | — | $81.00 | +0.00 | $79.70 | -91.00 | -91.00 | +0.00 | -91.00 |
| 2026-09-18 | `ALMU` | 508 | $11.54 | $11.64 | +48.26 | — | +0.00 | +48.26 | +218.44 | — |
| 2026-09-18 | `LEN` | 70 | $79.70 | $78.25 | -101.50 | — | +0.00 | -101.50 | -192.50 | — |

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
| 2026-08-27 | — | $57.77 | BZ×91, DKS×9, SLQT×2307, TIGR×258, ANF×10, BBWI×73, BOX×39, DY×4 | $10,927.75 | -91.98 | +92.50 | BBY, BILI, CM, CMBT, CSIQ, HQY, RY, TD | BZ, DKS, SLQT, TIGR, ANF, BBWI, BOX, DY | $291.29 | $10,967.70 | BBY×16, BILI×84, CM×11, CMBT×76, CSIQ×101, HQY×14, RY×6, TD×11 |
| 2026-08-28 | +0.75 | $291.29 | BBY×16, BILI×84, CM×11, CMBT×76, CSIQ×101, HQY×14, RY×6, TD×11 | $11,005.29 | +37.59 | -405.49 | ADSK, BBAR, ESTC, FINV, FRO, GAP, HAFN, IREN | BBY, BILI, CM, CMBT, CSIQ, HQY, RY, TD | $158.18 | $10,563.06 | ADSK×5, BBAR×91, ESTC×13, FINV×354, FRO×30, GAP×55, HAFN×164, IREN×36 |
| 2026-08-31 | -5.85 | $158.18 | ADSK×5, BBAR×91, ESTC×13, FINV×354, FRO×30, GAP×55, HAFN×164, IREN×36 | $10,572.35 | +9.29 | +0.00 | — | ADSK, BBAR, ESTC, FINV, FRO, GAP, HAFN, IREN | $10,552.44 | $10,552.44 | — |
| 2026-09-01 | -6.30 | $10,552.44 | — | $10,552.44 | -0.00 | +0.00 | — | — | $10,552.44 | $10,552.44 | — |
| 2026-09-02 | -3.83 | $10,552.44 | — | $10,552.44 | -0.00 | +0.00 | — | — | $10,552.44 | $10,552.44 | — |
| 2026-09-03 | -0.90 | $10,552.44 | — | $10,552.44 | -0.00 | +448.74 | AI, AVGO, CHPT, CIEN, CPB, FIVE, HPE, MEI | — | $587.64 | $10,983.77 | AI×122, AVGO×3, CHPT×191, CIEN×3, CPB×59, FIVE×5, HPE×27, MEI×87 |
| 2026-09-04 | +2.25 | $587.64 | AI×122, AVGO×3, CHPT×191, CIEN×3, CPB×59, FIVE×5, HPE×27, MEI×87 | $11,022.08 | +38.31 | -91.18 | AMBA, ASAN, DOCU, DOMO, GWRE, IOT, LULU, MAMA | AI, AVGO, CHPT, CIEN, CPB, FIVE, HPE, MEI | $114.16 | $10,893.44 | AMBA×21, ASAN×157, DOCU×20, DOMO×380, GWRE×8, IOT×30, LULU×14, MAMA×87 |
| 2026-09-08 | -11.47 | $114.16 | AMBA×21, ASAN×157, DOCU×20, DOMO×380, GWRE×8, IOT×30, LULU×14, MAMA×87 | $10,826.88 | -66.56 | +0.00 | — | AMBA, ASAN, DOCU, DOMO, GWRE, IOT, LULU, MAMA | $10,806.80 | $10,806.80 | — |
| 2026-09-09 | -13.95 | $10,806.80 | — | $10,806.80 | -0.00 | +0.00 | — | — | $10,806.80 | $10,806.80 | — |
| 2026-09-10 | -13.28 | $10,806.80 | — | $10,806.80 | -0.00 | +0.00 | — | — | $10,806.80 | $10,806.80 | — |
| 2026-09-11 | +0.50 | $10,806.80 | — | $10,806.80 | -0.00 | +52.27 | ORCL, DBI, ADBE, CPRT, DSGX, KR, LPTH, REF | — | $236.86 | $10,841.16 | ORCL×8, DBI×228, ADBE×5, CPRT×42, DSGX×18, KR×24, LPTH×144, REF×103 |
| 2026-09-14 | -11.00 | $236.86 | ORCL×8, DBI×228, ADBE×5, CPRT×42, DSGX×18, KR×24, LPTH×144, REF×103 | $10,852.87 | +11.71 | +0.00 | — | ORCL, DBI, ADBE, CPRT, DSGX, KR, LPTH, REF | $10,834.76 | $10,834.76 | — |
| 2026-09-15 | -3.84 | $10,834.76 | — | $10,834.76 | -0.00 | +0.00 | — | — | $10,834.76 | $10,834.76 | — |
| 2026-09-16 | +5.30 | $10,834.76 | — | $10,834.76 | -0.00 | +211.10 | FPS, TCOM | — | $25.31 | $11,040.99 | FPS×163, TCOM×132 |
| 2026-09-17 | +7.38 | $25.31 | FPS×163, TCOM×132 | $11,401.47 | +360.48 | +79.18 | ALMU, LEN | FPS, TCOM | $23.03 | $11,466.89 | ALMU×508, LEN×70 |
| 2026-09-18 | +4.86 | $23.03 | ALMU×508, LEN×70 | $11,413.65 | -53.24 | +0.00 | — | ALMU, LEN | $11,404.71 | $11,404.71 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 6172 | $0.81 | $68.51 | — | $4,932.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+13.2; leftover $5000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 223 | $22.01 | $2.88 | — | $21.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+0.3; leftover $5000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.06 | ▲ close $10,769.53 vs 09:30 $10,000.00 (session +840.92) | 16:00 close · cash $21.06 · equity $10,769.53 vs 09:30 $10,000.00 (+769.53; session marks +840.92) · 2 name(s) marked open→close (per-name table). INO×6172 09:30 $0.81 → close $0.90 +555.48; VOR×223 09:30 $22.01 → close $23.29 +285.44 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.06 | ▲ 09:30 equity $10,963.61 vs yday $10,769.53 (+194.08) | 09:30 open · cash $21.06 (unchanged overnight, no fees) · equity $10,963.61 vs prior close $10,769.53 (+194.08) · 2 name(s) re-marked at the open (per-name table). INO×6172 yday $0.90 → 09:30 $0.93 +185.16; VOR×223 yday $23.29 → 09:30 $23.33 +8.92 | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 6172 | $0.93 | $76.99 | $+595.14 | $5,684.04 | ▲ +595.14 after sell → book $10,886.63; vs 09:30 mark -76.98 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 223 | $23.33 | $2.96 | $+288.53 | $10,883.67 | ▲ +288.53 after sell → book $10,883.67; vs 09:30 mark -2.96 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 906 | $1.50 | $11.69 | — | $9,512.99 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 69 | $19.57 | $2.20 | — | $8,160.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+58.7; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 122 | $11.12 | $2.36 | — | $6,801.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+36.3; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 100 | $13.55 | $2.29 | — | $5,444.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 125 | $10.83 | $2.37 | — | $4,088.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover,oppset; 🔵; ⚪; ret5=-30.1; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 1152 | $1.18 | $14.86 | — | $2,713.84 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 70 | $19.17 | $2.20 | — | $1,369.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 137 | $9.89 | $2.40 | — | $11.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.72 | ▲ close $10,869.01 vs 09:30 $10,963.61 (session +25.69) | 16:00 close · cash $11.72 · equity $10,869.01 vs 09:30 $10,963.61 (-94.60; session marks +25.69) · 8 name(s) marked open→close (per-name table). BTBT×906 09:30 $1.50 → close $1.57 +63.42; ARX×69 09:30 $19.57 → close $19.58 +0.69; AIRO×122 09:30 $11.12 → close $9.57 -189.10; MH×100 09:30 $13.55 → close $13.10 -45.00; CLBT×125 09:30 $10.83 → close $11.14 +38.75; EU×1152 09:30 $1.18 → close $1.21 +34.56; LUNR×70 09:30 $19.17 → close $19.01 -11.20; NMAX×137 09:30 $9.89 → close $10.87 +133.57 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.72 | ▲ 09:30 equity $10,935.77 vs yday $10,869.01 (+66.76) | 09:30 open · cash $11.72 (unchanged overnight, no fees) · equity $10,935.77 vs prior close $10,869.01 (+66.76) · 8 name(s) re-marked at the open (per-name table). BTBT×906 yday $1.57 → 09:30 $1.52 -45.30; ARX×69 yday $19.58 → 09:30 $19.57 -0.69; AIRO×122 yday $9.57 → 09:30 $9.57 +0.00; MH×100 yday $13.10 → 09:30 $13.16 +6.00; CLBT×125 yday $11.14 → 09:30 $11.19 +6.25; EU×1152 yday $1.21 → 09:30 $1.21 +0.00; LUNR×70 yday $19.01 → 09:30 $20.25 +86.80; NMAX×137 yday $10.87 → 09:30 $10.97 +13.70 | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 906 | $1.52 | $11.85 | $-5.42 | $1,376.99 | ▼ -5.42 after sell → book $10,923.92; vs 09:30 mark -11.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 69 | $19.57 | $2.22 | $-4.42 | $2,725.10 | ▼ -4.42 after sell → book $10,921.70; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 122 | $9.57 | $2.39 | $-193.84 | $3,890.26 | ▼ -193.84 after sell → book $10,919.32; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 100 | $13.16 | $2.32 | $-43.61 | $5,203.94 | ▼ -43.61 after sell → book $10,917.00; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 125 | $11.19 | $2.40 | $+40.24 | $6,600.29 | ▲ +40.24 after sell → book $10,914.60; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `EU` | 1152 | $1.21 | $15.06 | $+4.64 | $7,979.15 | ▲ +4.64 after sell → book $10,899.54; vs 09:30 mark -15.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 70 | $20.25 | $2.22 | $+71.18 | $9,394.43 | ▲ +71.18 after sell → book $10,897.32; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `NMAX` | 137 | $10.97 | $2.44 | $+142.44 | $10,894.88 | ▲ +142.44 after sell → book $10,894.88; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,894.88 | ▲ close $10,894.88 vs 09:30 $10,935.77 (session +0.00) | 16:00 close · cash $10,894.88 · no lots left · equity $10,894.88. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,894.88 | ▲ 09:30 equity $10,894.88 vs yday $10,894.88 (+0.00) | 09:30 open · cash $10,894.88 · no holdings · equity $10,894.88 vs prior close $10,894.88 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,894.88 | ▲ close $10,894.88 vs 09:30 $10,894.88 (session +0.00) | 16:00 close · cash $10,894.88 · no lots left · equity $10,894.88. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,894.88 | ▲ 09:30 equity $10,894.88 vs yday $10,894.88 (+0.00) | 09:30 open · cash $10,894.88 · no holdings · equity $10,894.88 vs prior close $10,894.88 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,894.88 | ▲ close $10,894.88 vs 09:30 $10,894.88 (session +0.00) | 16:00 close · cash $10,894.88 · no lots left · equity $10,894.88. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,894.88 | ▲ 09:30 equity $10,894.88 vs yday $10,894.88 (+0.00) | 09:30 open · cash $10,894.88 · no holdings · equity $10,894.88 vs prior close $10,894.88 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 13 | $97.43 | $2.03 | — | $9,626.26 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+11.8; leftover $1361.86 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
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
| 2026-08-21 09:30 ET | **BUY** | `WMT` | 13 | $103.69 | $2.03 | — | $5,725.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover,oppset; ret5=-10.3; leftover $1386.84 | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
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
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 9 | $142.36 | $2.02 | — | $5,922.05 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react,oppset; 🔵; ret5=-8.6; leftover $1395.16 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
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
| 2026-08-26 09:30 ET | **BUY** | `DY` | 4 | $326.91 | $2.00 | — | $57.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react,oppset; ret5=-15.2; leftover $1345.16 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.77 | ▲ close $11,019.73 vs 09:30 $10,709.98 (session +357.88) | 16:00 close · cash $57.77 · equity $11,019.73 vs 09:30 $10,709.98 (+309.75; session marks +357.88) · 8 name(s) marked open→close (per-name table). BZ×91 09:30 $16.77 → close $18.84 +188.37; DKS×9 09:30 $121.87 → close $129.66 +70.11; SLQT×2307 09:30 $0.58 → close $0.55 -76.13; TIGR×258 09:30 $5.21 → close $5.46 +64.50; ANF×10 09:30 $131.37 → close $147.75 +163.80; BBWI×73 09:30 $18.26 → close $18.90 +46.72; BOX×39 09:30 $34.30 → close $33.39 -35.49; DY×4 09:30 $326.91 → close $310.91 -64.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.77 | ▼ 09:30 equity $10,927.75 vs yday $11,019.73 (-91.98) | 09:30 open · cash $57.77 (unchanged overnight, no fees) · equity $10,927.75 vs prior close $11,019.73 (-91.98) · 8 name(s) re-marked at the open (per-name table). BZ×91 yday $18.84 → 09:30 $18.50 -30.94; DKS×9 yday $129.66 → 09:30 $128.73 -8.37; SLQT×2307 yday $0.55 → 09:30 $0.53 -46.14; TIGR×258 yday $5.46 → 09:30 $5.49 +7.74; ANF×10 yday $147.75 → 09:30 $144.70 -30.50; BBWI×73 yday $18.90 → 09:30 $18.69 -15.33; BOX×39 yday $33.39 → 09:30 $33.79 +15.60; DY×4 yday $310.91 → 09:30 $314.90 +15.96 | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 91 | $18.50 | $2.29 | $+288.47 | $1,738.98 | ▲ +288.47 after sell → book $10,925.46; vs 09:30 mark -2.29 | dropped from list after 2 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 9 | $128.73 | $2.04 | $-126.72 | $2,895.51 | ▼ -126.72 after sell → book $10,923.42; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `SLQT` | 2307 | $0.53 | $19.54 | $-162.18 | $4,098.68 | ▼ -162.18 after sell → book $10,903.88; vs 09:30 mark -19.54 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `TIGR` | 258 | $5.49 | $3.38 | $+65.53 | $5,511.72 | ▲ +65.53 after sell → book $10,900.50; vs 09:30 mark -3.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ANF` | 10 | $144.70 | $2.04 | $+129.24 | $6,956.68 | ▲ +129.24 after sell → book $10,898.46; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BBWI` | 73 | $18.69 | $2.23 | $+26.95 | $8,318.81 | ▲ +26.95 after sell → book $10,896.22; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BOX` | 39 | $33.79 | $2.13 | $-24.12 | $9,634.50 | ▼ -24.12 after sell → book $10,894.10; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `DY` | 4 | $314.90 | $2.02 | $-52.06 | $10,892.07 | ▼ -52.06 after sell → book $10,892.07; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 16 | $80.60 | $2.04 | — | $9,600.44 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.0; leftover $1361.51 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `BILI` | 84 | $16.18 | $2.24 | — | $8,239.07 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-6.7; leftover $1361.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 11 | $118.77 | $2.02 | — | $6,930.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.3; leftover $1361.51 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CMBT` | 76 | $17.78 | $2.22 | — | $5,577.08 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.2; leftover $1361.51 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 101 | $13.41 | $2.29 | — | $4,220.38 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.1; leftover $1361.51 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `HQY` | 14 | $97.16 | $2.03 | — | $2,858.11 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.5; leftover $1361.51 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RY` | 6 | $206.82 | $2.01 | — | $1,615.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.2; leftover $1361.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 11 | $120.17 | $2.02 | — | $291.29 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; leftover $1361.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $291.29 | ▲ close $10,967.70 vs 09:30 $10,927.75 (session +92.50) | 16:00 close · cash $291.29 · equity $10,967.70 vs 09:30 $10,927.75 (+39.95; session marks +92.50) · 8 name(s) marked open→close (per-name table). BBY×16 09:30 $80.60 → close $83.56 +47.36; BILI×84 09:30 $16.18 → close $16.77 +49.14; CM×11 09:30 $118.77 → close $114.84 -43.23; CMBT×76 09:30 $17.78 → close $18.28 +38.00; CSIQ×101 09:30 $13.41 → close $13.98 +57.57; HQY×14 09:30 $97.16 → close $93.39 -52.78; RY×6 09:30 $206.82 → close $204.54 -13.68; TD×11 09:30 $120.17 → close $121.09 +10.12 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $291.29 | ▲ 09:30 equity $11,005.29 vs yday $10,967.70 (+37.59) | 09:30 open · cash $291.29 (unchanged overnight, no fees) · equity $11,005.29 vs prior close $10,967.70 (+37.59) · 8 name(s) re-marked at the open (per-name table). BBY×16 yday $83.56 → 09:30 $83.85 +4.64; BILI×84 yday $16.77 → 09:30 $16.94 +14.70; CM×11 yday $114.84 → 09:30 $115.66 +9.02; CMBT×76 yday $18.28 → 09:30 $18.58 +22.80; CSIQ×101 yday $13.98 → 09:30 $13.65 -33.33; HQY×14 yday $93.39 → 09:30 $93.62 +3.22; RY×6 yday $204.54 → 09:30 $205.50 +5.76; TD×11 yday $121.09 → 09:30 $122.07 +10.78 | — |
| 2026-08-28 09:30 ET | **SELL** | `BBY` | 16 | $83.85 | $2.06 | $+47.90 | $1,630.83 | ▲ +47.90 after sell → book $11,003.23; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BILI` | 84 | $16.94 | $2.27 | $+59.33 | $3,051.52 | ▲ +59.33 after sell → book $11,000.96; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 11 | $115.66 | $2.04 | $-38.28 | $4,321.74 | ▼ -38.28 after sell → book $10,998.92; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CMBT` | 76 | $18.58 | $2.24 | $+56.34 | $5,731.58 | ▲ +56.34 after sell → book $10,996.68; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CSIQ` | 101 | $13.65 | $2.32 | $+19.63 | $7,107.91 | ▲ +19.63 after sell → book $10,994.36; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `HQY` | 14 | $93.62 | $2.05 | $-53.64 | $8,416.53 | ▼ -53.64 after sell → book $10,992.30; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `RY` | 6 | $205.50 | $2.03 | $-11.96 | $9,647.51 | ▼ -11.96 after sell → book $10,990.28; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TD` | 11 | $122.07 | $2.04 | $+16.83 | $10,988.23 | ▲ +16.83 after sell → book $10,988.23; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 5 | $261.16 | $2.00 | — | $9,680.43 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.8; leftover $1373.53 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 91 | $15.01 | $2.26 | — | $8,312.25 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; leftover $1373.53 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 13 | $103.89 | $2.03 | — | $6,959.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.5; leftover $1373.53 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 354 | $3.88 | $4.57 | — | $5,581.57 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; leftover $1373.53 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 30 | $44.40 | $2.08 | — | $4,247.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; leftover $1373.53 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 55 | $24.69 | $2.15 | — | $2,887.38 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; leftover $1373.53 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 164 | $8.35 | $2.48 | — | $1,515.50 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; leftover $1373.53 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 36 | $37.65 | $2.10 | — | $158.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; leftover $1373.53 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.18 | ▼ close $10,563.06 vs 09:30 $11,005.29 (session -405.49) | 16:00 close · cash $158.18 · equity $10,563.06 vs 09:30 $11,005.29 (-442.23; session marks -405.49) · 8 name(s) marked open→close (per-name table). ADSK×5 09:30 $261.16 → close $260.66 -2.50; BBAR×91 09:30 $15.01 → close $14.47 -49.14; ESTC×13 09:30 $103.89 → close $99.91 -51.74; FINV×354 09:30 $3.88 → close $3.40 -169.92; FRO×30 09:30 $44.40 → close $44.19 -6.30; GAP×55 09:30 $24.69 → close $23.48 -66.55; HAFN×164 09:30 $8.35 → close $8.47 +19.68; IREN×36 09:30 $37.65 → close $35.45 -79.02 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.18 | ▲ 09:30 equity $10,572.35 vs yday $10,563.06 (+9.29) | 09:30 open · cash $158.18 (unchanged overnight, no fees) · equity $10,572.35 vs prior close $10,563.06 (+9.29) · 8 name(s) re-marked at the open (per-name table). ADSK×5 yday $260.66 → 09:30 $257.71 -14.75; BBAR×91 yday $14.47 → 09:30 $14.88 +37.31; ESTC×13 yday $99.91 → 09:30 $98.00 -24.83; FINV×354 yday $3.40 → 09:30 $3.39 -3.54; FRO×30 yday $44.19 → 09:30 $44.85 +19.80; GAP×55 yday $23.48 → 09:30 $22.98 -27.50; HAFN×164 yday $8.47 → 09:30 $8.53 +9.84; IREN×36 yday $35.45 → 09:30 $35.81 +12.96 | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 5 | $257.71 | $2.03 | $-21.28 | $1,444.71 | ▼ -21.28 after sell → book $10,570.33; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBAR` | 91 | $14.88 | $2.29 | $-16.38 | $2,796.50 | ▼ -16.38 after sell → book $10,568.04; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ESTC` | 13 | $98.00 | $2.05 | $-80.65 | $4,068.45 | ▼ -80.65 after sell → book $10,565.99; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `FINV` | 354 | $3.39 | $4.64 | $-182.66 | $5,263.87 | ▼ -182.66 after sell → book $10,561.35; vs 09:30 mark -4.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FRO` | 30 | $44.85 | $2.10 | $+9.32 | $6,607.27 | ▲ +9.32 after sell → book $10,559.25; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 55 | $22.98 | $2.18 | $-98.38 | $7,869.00 | ▼ -98.38 after sell → book $10,557.08; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 164 | $8.53 | $2.52 | $+24.52 | $9,265.40 | ▲ +24.52 after sell → book $10,554.56; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `IREN` | 36 | $35.81 | $2.12 | $-70.28 | $10,552.44 | ▼ -70.28 after sell → book $10,552.44; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,552.44 | ▲ close $10,552.44 vs 09:30 $10,572.35 (session +0.00) | 16:00 close · cash $10,552.44 · no lots left · equity $10,552.44. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,552.44 | ▲ 09:30 equity $10,552.44 vs yday $10,552.44 (-0.00) | 09:30 open · cash $10,552.44 · no holdings · equity $10,552.44 vs prior close $10,552.44 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,552.44 | ▲ close $10,552.44 vs 09:30 $10,552.44 (session +0.00) | 16:00 close · cash $10,552.44 · no lots left · equity $10,552.44. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,552.44 | ▲ 09:30 equity $10,552.44 vs yday $10,552.44 (-0.00) | 09:30 open · cash $10,552.44 · no holdings · equity $10,552.44 vs prior close $10,552.44 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,552.44 | ▲ close $10,552.44 vs 09:30 $10,552.44 (session +0.00) | 16:00 close · cash $10,552.44 · no lots left · equity $10,552.44. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,552.44 | ▲ 09:30 equity $10,552.44 vs yday $10,552.44 (-0.00) | 09:30 open · cash $10,552.44 · no holdings · equity $10,552.44 vs prior close $10,552.44 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 122 | $10.74 | $2.36 | — | $9,239.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; leftover $1319.05 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $8,181.97 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; leftover $1319.05 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 191 | $6.90 | $2.56 | — | $6,861.51 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; leftover $1319.05 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 3 | $354.49 | $2.00 | — | $5,796.04 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; leftover $1319.05 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 59 | $22.32 | $2.17 | — | $4,477.00 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; leftover $1319.05 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 5 | $257.00 | $2.00 | — | $3,189.99 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; leftover $1319.05 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 27 | $47.60 | $2.07 | — | $1,902.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react,oppset; 🔵; ret5=-6.2; leftover $1319.05 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 87 | $15.09 | $2.25 | — | $587.64 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; leftover $1319.05 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $587.64 | ▲ close $10,983.77 vs 09:30 $10,552.44 (session +448.74) | 16:00 close · cash $587.64 · equity $10,983.77 vs 09:30 $10,552.44 (+431.33; session marks +448.74) · 8 name(s) marked open→close (per-name table). AI×122 09:30 $10.74 → close $10.90 +18.91; AVGO×3 09:30 $351.74 → close $357.16 +16.26; CHPT×191 09:30 $6.90 → close $9.08 +416.38; CIEN×3 09:30 $354.49 → close $317.46 -111.09; CPB×59 09:30 $22.32 → close $22.13 -11.21; FIVE×5 09:30 $257.00 → close $239.96 -85.20; HPE×27 09:30 $47.60 → close $54.44 +184.68; MEI×87 09:30 $15.09 → close $15.32 +20.01 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $587.64 | ▲ 09:30 equity $11,022.08 vs yday $10,983.77 (+38.31) | 09:30 open · cash $587.64 (unchanged overnight, no fees) · equity $11,022.08 vs prior close $10,983.77 (+38.31) · 8 name(s) re-marked at the open (per-name table). AI×122 yday $10.90 → 09:30 $10.91 +1.22; AVGO×3 yday $357.16 → 09:30 $359.70 +7.62; CHPT×191 yday $9.08 → 09:30 $9.28 +38.20; CIEN×3 yday $317.46 → 09:30 $321.67 +12.63; CPB×59 yday $22.13 → 09:30 $22.10 -1.77; FIVE×5 yday $239.96 → 09:30 $238.88 -5.40; HPE×27 yday $54.44 → 09:30 $53.85 -15.93; MEI×87 yday $15.32 → 09:30 $15.34 +1.74 | — |
| 2026-09-04 09:30 ET | **SELL** | `AI` | 122 | $10.91 | $2.39 | $+15.39 | $1,916.27 | ▲ +15.39 after sell → book $11,019.69; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $359.70 | $2.02 | $+19.86 | $2,993.35 | ▲ +19.86 after sell → book $11,017.67; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CHPT` | 191 | $9.28 | $2.61 | $+449.41 | $4,763.22 | ▲ +449.41 after sell → book $11,015.06; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 3 | $321.67 | $2.02 | $-102.48 | $5,726.21 | ▼ -102.48 after sell → book $11,013.04; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CPB` | 59 | $22.10 | $2.19 | $-17.33 | $7,027.93 | ▼ -17.33 after sell → book $11,010.86; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 5 | $238.88 | $2.02 | $-94.63 | $8,220.30 | ▼ -94.63 after sell → book $11,008.83; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 27 | $53.85 | $2.09 | $+164.59 | $9,672.16 | ▲ +164.59 after sell → book $11,006.74; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟡 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `MEI` | 87 | $15.34 | $2.28 | $+17.22 | $11,004.46 | ▲ +17.22 after sell → book $11,004.46; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 21 | $63.18 | $2.05 | — | $9,675.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.9; leftover $1375.56 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 157 | $8.74 | $2.46 | — | $8,300.99 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; leftover $1375.56 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 20 | $68.52 | $2.05 | — | $6,928.54 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.4; leftover $1375.56 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 380 | $3.62 | $4.90 | — | $5,549.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; leftover $1375.56 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 8 | $167.55 | $2.01 | — | $4,207.52 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; leftover $1375.56 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 30 | $44.90 | $2.08 | — | $2,858.44 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react,oppset; ret5=-7.5; leftover $1375.56 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 14 | $98.15 | $2.03 | — | $1,482.31 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react,oppset; ret5=+5.9; leftover $1375.56 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 87 | $15.70 | $2.25 | — | $114.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; leftover $1375.56 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $114.16 | ▼ close $10,893.44 vs 09:30 $11,022.08 (session -91.18) | 16:00 close · cash $114.16 · equity $10,893.44 vs 09:30 $11,022.08 (-128.64; session marks -91.18) · 8 name(s) marked open→close (per-name table). AMBA×21 09:30 $63.18 → close $62.89 -6.09; ASAN×157 09:30 $8.74 → close $8.81 +10.99; DOCU×20 09:30 $68.52 → close $68.41 -2.20; DOMO×380 09:30 $3.62 → close $3.88 +100.70; GWRE×8 09:30 $167.55 → close $162.42 -41.04; IOT×30 09:30 $44.90 → close $40.20 -141.00; LULU×14 09:30 $98.15 → close $100.61 +34.44; MAMA×87 09:30 $15.70 → close $15.16 -46.98 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $114.16 | ▼ 09:30 equity $10,826.88 vs yday $10,893.44 (-66.56) | 09:30 open · cash $114.16 (unchanged overnight, no fees) · equity $10,826.88 vs prior close $10,893.44 (-66.56) · 8 name(s) re-marked at the open (per-name table). AMBA×21 yday $62.89 → 09:30 $63.83 +19.74; ASAN×157 yday $8.81 → 09:30 $8.73 -12.56; DOCU×20 yday $68.41 → 09:30 $67.05 -27.20; DOMO×380 yday $3.88 → 09:30 $3.84 -15.20; GWRE×8 yday $162.42 → 09:30 $160.52 -15.20; IOT×30 yday $40.20 → 09:30 $39.56 -19.20; LULU×14 yday $100.61 → 09:30 $100.58 -0.42; MAMA×87 yday $15.16 → 09:30 $15.20 +3.48 | — |
| 2026-09-08 09:30 ET | **SELL** | `AMBA` | 21 | $63.83 | $2.07 | $+9.52 | $1,452.52 | ▲ +9.52 after sell → book $10,824.81; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `ASAN` | 157 | $8.73 | $2.50 | $-6.53 | $2,820.63 | ▼ -6.53 after sell → book $10,822.31; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `DOCU` | 20 | $67.05 | $2.07 | $-33.52 | $4,159.56 | ▼ -33.52 after sell → book $10,820.24; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOMO` | 380 | $3.84 | $4.98 | $+75.62 | $5,613.78 | ▲ +75.62 after sell → book $10,815.26; vs 09:30 mark -4.98 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GWRE` | 8 | $160.52 | $2.03 | $-60.29 | $6,895.91 | ▼ -60.29 after sell → book $10,813.23; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `IOT` | 30 | $39.56 | $2.10 | $-164.38 | $8,080.61 | ▼ -164.38 after sell → book $10,811.13; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 14 | $100.58 | $2.05 | $+29.93 | $9,486.67 | ▲ +29.93 after sell → book $10,809.07; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `MAMA` | 87 | $15.20 | $2.28 | $-48.03 | $10,806.80 | ▼ -48.03 after sell → book $10,806.80; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.80 | ▲ close $10,806.80 vs 09:30 $10,826.88 (session +0.00) | 16:00 close · cash $10,806.80 · no lots left · equity $10,806.80. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.80 | ▲ 09:30 equity $10,806.80 vs yday $10,806.80 (-0.00) | 09:30 open · cash $10,806.80 · no holdings · equity $10,806.80 vs prior close $10,806.80 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.80 | ▲ close $10,806.80 vs 09:30 $10,806.80 (session +0.00) | 16:00 close · cash $10,806.80 · no lots left · equity $10,806.80. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.80 | ▲ 09:30 equity $10,806.80 vs yday $10,806.80 (-0.00) | 09:30 open · cash $10,806.80 · no holdings · equity $10,806.80 vs prior close $10,806.80 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.80 | ▲ close $10,806.80 vs 09:30 $10,806.80 (session +0.00) | 16:00 close · cash $10,806.80 · no lots left · equity $10,806.80. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.80 | ▲ 09:30 equity $10,806.80 vs yday $10,806.80 (-0.00) | 09:30 open · cash $10,806.80 · no holdings · equity $10,806.80 vs prior close $10,806.80 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 8 | $164.43 | $2.01 | — | $9,489.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1350.85 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 228 | $5.91 | $2.94 | — | $8,138.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover,ohlc_hot,oppset; ret5=+14.1; leftover $1350.85 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 5 | $242.17 | $2.00 | — | $6,926.07 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.1; leftover $1350.85 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 42 | $32.01 | $2.12 | — | $5,579.53 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.4; leftover $1350.85 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 18 | $71.71 | $2.04 | — | $4,286.71 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-9.1; leftover $1350.85 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟡 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 24 | $56.02 | $2.06 | — | $2,940.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.2; leftover $1350.85 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 144 | $9.37 | $2.42 | — | $1,588.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.5; leftover $1350.85 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 103 | $13.10 | $2.30 | — | $236.86 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.9; leftover $1350.85 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $236.86 | ▲ close $10,841.16 vs 09:30 $10,806.80 (session +52.27) | 16:00 close · cash $236.86 · equity $10,841.16 vs 09:30 $10,806.80 (+34.36; session marks +52.27) · 8 name(s) marked open→close (per-name table). ORCL×8 09:30 $164.43 → close $150.28 -113.20; DBI×228 09:30 $5.91 → close $5.88 -6.84; ADBE×5 09:30 $242.17 → close $252.23 +50.30; CPRT×42 09:30 $32.01 → close $29.95 -86.52; DSGX×18 09:30 $71.71 → close $76.04 +77.94; KR×24 09:30 $56.02 → close $58.49 +59.28; LPTH×144 09:30 $9.37 → close $9.20 -24.48; REF×103 09:30 $13.10 → close $14.03 +95.79 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $236.86 | ▲ 09:30 equity $10,852.87 vs yday $10,841.16 (+11.71) | 09:30 open · cash $236.86 (unchanged overnight, no fees) · equity $10,852.87 vs prior close $10,841.16 (+11.71) · 8 name(s) re-marked at the open (per-name table). ORCL×8 yday $150.28 → 09:30 $141.42 -70.88; DBI×228 yday $5.88 → 09:30 $5.86 -4.56; ADBE×5 yday $252.23 → 09:30 $261.51 +46.40; CPRT×42 yday $29.95 → 09:30 $30.63 +28.56; DSGX×18 yday $76.04 → 09:30 $77.68 +29.52; KR×24 yday $58.49 → 09:30 $59.31 +19.68; LPTH×144 yday $9.20 → 09:30 $8.85 -50.40; REF×103 yday $14.03 → 09:30 $14.16 +13.39 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 8 | $141.42 | $2.03 | $-188.13 | $1,366.19 | ▼ -188.13 after sell → book $10,850.84; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-14 09:30 ET | **SELL** | `DBI` | 228 | $5.86 | $2.99 | $-17.33 | $2,699.28 | ▼ -17.33 after sell → book $10,847.85; vs 09:30 mark -2.99 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 5 | $261.51 | $2.03 | $+92.67 | $4,004.81 | ▲ +92.67 after sell → book $10,845.83; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CPRT` | 42 | $30.63 | $2.14 | $-62.21 | $5,289.13 | ▼ -62.21 after sell → book $10,843.69; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `DSGX` | 18 | $77.68 | $2.07 | $+103.35 | $6,685.30 | ▲ +103.35 after sell → book $10,841.62; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `KR` | 24 | $59.31 | $2.08 | $+74.81 | $8,106.66 | ▲ +74.81 after sell → book $10,839.54; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `LPTH` | 144 | $8.85 | $2.46 | $-79.76 | $9,378.60 | ▼ -79.76 after sell → book $10,837.08; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `REF` | 103 | $14.16 | $2.33 | $+104.55 | $10,834.76 | ▲ +104.55 after sell → book $10,834.76; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,834.76 | ▲ close $10,834.76 vs 09:30 $10,852.87 (session +0.00) | 16:00 close · cash $10,834.76 · no lots left · equity $10,834.76. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,834.76 | ▲ 09:30 equity $10,834.76 vs yday $10,834.76 (-0.00) | 09:30 open · cash $10,834.76 · no holdings · equity $10,834.76 vs prior close $10,834.76 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,834.76 | ▲ close $10,834.76 vs 09:30 $10,834.76 (session +0.00) | 16:00 close · cash $10,834.76 · no lots left · equity $10,834.76. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,834.76 | ▲ 09:30 equity $10,834.76 vs yday $10,834.76 (-0.00) | 09:30 open · cash $10,834.76 · no holdings · equity $10,834.76 vs prior close $10,834.76 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 163 | $33.14 | $2.48 | — | $5,430.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,oppset; 🔵; ret5=-2.9; leftover $5417.38 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 132 | $40.93 | $2.39 | — | $25.31 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; leftover $5417.38 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.31 | ▲ close $11,040.99 vs 09:30 $10,834.76 (session +211.10) | 16:00 close · cash $25.31 · equity $11,040.99 vs 09:30 $10,834.76 (+206.23; session marks +211.10) · 2 name(s) marked open→close (per-name table). FPS×163 09:30 $33.14 → close $34.84 +277.10; TCOM×132 09:30 $40.93 → close $40.43 -66.00 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.31 | ▲ 09:30 equity $11,401.47 vs yday $11,040.99 (+360.48) | 09:30 open · cash $25.31 (unchanged overnight, no fees) · equity $11,401.47 vs prior close $11,040.99 (+360.48) · 2 name(s) re-marked at the open (per-name table). FPS×163 yday $34.84 → 09:30 $36.76 +312.96; TCOM×132 yday $40.43 → 09:30 $40.79 +47.52 | — |
| 2026-09-17 09:30 ET | **SELL** | `FPS` | 163 | $36.76 | $2.55 | $+585.03 | $6,014.64 | ▲ +585.03 after sell → book $11,398.92; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `TCOM` | 132 | $40.79 | $2.45 | $-23.32 | $11,396.47 | ▼ -23.32 after sell → book $11,396.47; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 508 | $11.21 | $6.55 | — | $5,695.23 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react,oppset; ret5=+1.0; leftover $5698.23 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `LEN` | 70 | $81.00 | $2.20 | — | $23.03 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.0; leftover $5698.23 | join🔴 sector🟢 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.03 | ▲ close $11,466.89 vs 09:30 $11,401.47 (session +79.18) | 16:00 close · cash $23.03 · equity $11,466.89 vs 09:30 $11,401.47 (+65.42; session marks +79.18) · 2 name(s) marked open→close (per-name table). ALMU×508 09:30 $11.21 → close $11.54 +170.18; LEN×70 09:30 $81.00 → close $79.70 -91.00 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.03 | ▼ 09:30 equity $11,413.65 vs yday $11,466.89 (-53.24) | 09:30 open · cash $23.03 (unchanged overnight, no fees) · equity $11,413.65 vs prior close $11,466.89 (-53.24) · 2 name(s) re-marked at the open (per-name table). ALMU×508 yday $11.54 → 09:30 $11.64 +48.26; LEN×70 yday $79.70 → 09:30 $78.25 -101.50 | — |
| 2026-09-18 09:30 ET | **SELL** | `ALMU` | 508 | $11.64 | $6.68 | $+205.20 | $5,929.47 | ▲ +205.20 after sell → book $11,406.97; vs 09:30 mark -6.68 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `LEN` | 70 | $78.25 | $2.26 | $-196.96 | $11,404.71 | ▼ -196.96 after sell → book $11,404.71; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,404.71 | ▲ close $11,404.71 vs 09:30 $11,413.65 (session +0.00) | 16:00 close · cash $11,404.71 · no lots left · equity $11,404.71. | — |

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
| 2026-09-02 | `MMED` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CXM` | hard_red | hard-red S=-3.83 sit; no new buys |
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
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `UROY` | hard_red | hard-red S=-3.84 sit; no new buys |
