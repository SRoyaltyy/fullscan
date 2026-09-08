# Factor mine action — `union_last_red_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ last_red, no 🚨

Cash book **+4.24%** ($10,424) · signal-only (no cash/fees) was +25.82%. Starts YES **14/18**. Fills 88 · skips 171 · realized $+298.81.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the last finished bar was red (closed down).
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
- **Gate** `last_red=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $24.41.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `TGTX` | 50 | — | $49.70 | +0.00 | $47.94 | -88.00 | -88.00 | +0.00 | -88.00 |
| 2026-08-13 | `SLS` | 213 | — | $11.70 | +0.00 | $12.36 | +140.58 | +140.58 | +0.00 | +140.58 |
| 2026-08-13 | `HIMS` | 84 | — | $29.74 | +0.00 | $28.77 | -81.48 | -81.48 | +0.00 | -81.48 |
| 2026-08-13 | `VOR` | 113 | — | $22.01 | +0.00 | $23.29 | +144.64 | +144.64 | +0.00 | +144.64 |
| 2026-08-14 | `TGTX` | 50 | $47.94 | $47.27 | -33.50 | $48.74 | +73.50 | +40.00 | -121.50 | -48.00 |
| 2026-08-14 | `SLS` | 213 | $12.36 | $12.40 | +8.52 | $12.78 | +80.94 | +89.46 | +149.10 | +230.04 |
| 2026-08-14 | `HIMS` | 84 | $28.77 | $29.15 | +31.92 | $28.15 | -84.00 | -52.08 | -49.56 | -133.56 |
| 2026-08-14 | `VOR` | 113 | $23.29 | $23.33 | +4.52 | $23.03 | -33.90 | -29.38 | +149.16 | +115.26 |
| 2026-08-17 | `TGTX` | 50 | $48.74 | $48.74 | +0.00 | $49.28 | +27.00 | +27.00 | -48.00 | -21.00 |
| 2026-08-17 | `SLS` | 213 | $12.78 | $12.78 | +0.00 | $13.00 | +46.86 | +46.86 | +230.04 | +276.90 |
| 2026-08-17 | `HIMS` | 84 | $28.15 | $28.14 | -0.84 | $28.61 | +39.48 | +38.64 | -134.40 | -94.92 |
| 2026-08-17 | `VOR` | 113 | $23.03 | $22.91 | -13.56 | $23.01 | +11.30 | -2.26 | +101.70 | +113.00 |
| 2026-08-17 | `DNN` | 1 | — | $3.24 | +0.00 | $3.19 | -0.05 | -0.05 | +0.00 | -0.05 |
| 2026-08-17 | `INV` | 2 | — | $1.62 | +0.00 | $1.39 | -0.47 | -0.47 | +0.00 | -0.47 |
| 2026-08-17 | `KLC` | 1 | — | $2.62 | +0.00 | $2.56 | -0.06 | -0.06 | +0.00 | -0.06 |
| 2026-08-18 | `TGTX` | 50 | $49.28 | $49.28 | +0.00 | — | +0.00 | +0.00 | -21.00 | — |
| 2026-08-18 | `SLS` | 213 | $13.00 | $12.66 | -72.42 | — | +0.00 | -72.42 | +204.48 | — |
| 2026-08-18 | `HIMS` | 84 | $28.61 | $27.85 | -63.84 | — | +0.00 | -63.84 | -158.76 | — |
| 2026-08-18 | `VOR` | 113 | $23.01 | $22.82 | -21.47 | — | +0.00 | -21.47 | +91.53 | — |
| 2026-08-18 | `DNN` | 1 | $3.19 | $3.11 | -0.08 | $3.15 | +0.04 | -0.04 | -0.13 | -0.09 |
| 2026-08-18 | `INV` | 2 | $1.39 | $1.32 | -0.12 | $1.32 | +0.00 | -0.12 | -0.59 | -0.59 |
| 2026-08-18 | `KLC` | 1 | $2.56 | $2.52 | -0.04 | $2.72 | +0.20 | +0.16 | -0.10 | +0.10 |
| 2026-08-19 | `DNN` | 1 | $3.15 | $3.19 | +0.04 | $3.22 | +0.03 | +0.07 | -0.05 | -0.02 |
| 2026-08-19 | `INV` | 2 | $1.32 | $1.39 | +0.13 | $1.54 | +0.30 | +0.43 | -0.46 | -0.16 |
| 2026-08-19 | `KLC` | 1 | $2.72 | $2.67 | -0.05 | $2.91 | +0.24 | +0.19 | +0.05 | +0.29 |
| 2026-08-20 | `DNN` | 1 | $3.22 | $3.20 | -0.02 | — | +0.00 | -0.02 | -0.04 | — |
| 2026-08-20 | `INV` | 2 | $1.54 | $1.55 | +0.02 | — | +0.00 | +0.02 | -0.14 | — |
| 2026-08-20 | `KLC` | 1 | $2.91 | $2.88 | -0.03 | — | +0.00 | -0.03 | +0.26 | — |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `MRVI` | 171 | — | $7.38 | +0.00 | $8.26 | +150.48 | +150.48 | +0.00 | +150.48 |
| 2026-08-20 | `CRCL` | 15 | — | $83.29 | +0.00 | $83.99 | +10.50 | +10.50 | +0.00 | +10.50 |
| 2026-08-20 | `WYFI` | 58 | — | $21.40 | +0.00 | $21.16 | -13.92 | -13.92 | +0.00 | -13.92 |
| 2026-08-20 | `TOYO` | 284 | — | $4.43 | +0.00 | $4.51 | +24.14 | +24.14 | +0.00 | +24.14 |
| 2026-08-20 | `DVLT` | 4207 | — | $0.30 | +0.00 | $0.32 | +84.14 | +84.14 | +0.00 | +84.14 |
| 2026-08-20 | `SAFX` | 3565 | — | $0.35 | +0.00 | $0.34 | -39.21 | -39.21 | +0.00 | -39.21 |
| 2026-08-20 | `AAP` | 26 | — | $46.85 | +0.00 | $42.39 | -115.96 | -115.96 | +0.00 | -115.96 |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | $97.03 | +17.03 | +44.20 | +61.23 | +78.26 |
| 2026-08-21 | `MRVI` | 171 | $8.26 | $8.20 | -10.26 | $8.70 | +85.50 | +75.24 | +140.22 | +225.72 |
| 2026-08-21 | `CRCL` | 15 | $83.99 | $87.65 | +54.90 | $88.94 | +19.35 | +74.25 | +65.40 | +84.75 |
| 2026-08-21 | `WYFI` | 58 | $21.16 | $21.54 | +22.04 | $20.72 | -47.56 | -25.52 | +8.12 | -39.44 |
| 2026-08-21 | `TOYO` | 284 | $4.51 | $4.68 | +46.86 | $4.82 | +39.76 | +86.62 | +71.00 | +110.76 |
| 2026-08-21 | `DVLT` | 4207 | $0.32 | $0.31 | -42.07 | $0.32 | +42.07 | +0.00 | +42.07 | +84.14 |
| 2026-08-21 | `SAFX` | 3565 | $0.34 | $0.35 | +24.95 | $0.33 | -89.12 | -64.17 | -14.26 | -103.38 |
| 2026-08-21 | `AAP` | 26 | $42.39 | $42.41 | +0.52 | $42.58 | +4.42 | +4.94 | -115.44 | -111.02 |
| 2026-08-21 | `AUTL` | 5 | — | $2.47 | +0.00 | $2.41 | -0.30 | -0.30 | +0.00 | -0.30 |
| 2026-08-21 | `CRDL` | 7 | — | $1.93 | +0.00 | $1.86 | -0.49 | -0.49 | +0.00 | -0.49 |
| 2026-08-21 | `ENHA` | 8 | — | $1.71 | +0.00 | $1.72 | +0.08 | +0.08 | +0.00 | +0.08 |
| 2026-08-21 | `CAN` | 47 | — | $0.29 | +0.00 | $0.35 | +2.87 | +2.87 | +0.00 | +2.87 |
| 2026-08-24 | `BHP` | 13 | $97.03 | $97.34 | +4.03 | $96.66 | -8.84 | -4.81 | +82.29 | +73.45 |
| 2026-08-24 | `MRVI` | 171 | $8.70 | $8.59 | -18.81 | $8.26 | -56.43 | -75.24 | +206.91 | +150.48 |
| 2026-08-24 | `CRCL` | 15 | $88.94 | $89.55 | +9.15 | $89.01 | -8.10 | +1.05 | +93.90 | +85.80 |
| 2026-08-24 | `WYFI` | 58 | $20.72 | $20.02 | -40.60 | $20.79 | +44.66 | +4.06 | -80.04 | -35.38 |
| 2026-08-24 | `TOYO` | 284 | $4.82 | $4.58 | -68.16 | $4.61 | +8.52 | -59.64 | +42.60 | +51.12 |
| 2026-08-24 | `DVLT` | 4207 | $0.32 | $0.31 | -42.07 | $0.31 | +0.00 | -42.07 | +42.07 | +42.07 |
| 2026-08-24 | `SAFX` | 3565 | $0.33 | $0.35 | +89.12 | $0.35 | +0.00 | +89.12 | -14.26 | -14.26 |
| 2026-08-24 | `AAP` | 26 | $42.58 | $43.10 | +13.52 | $43.83 | +18.98 | +32.50 | -97.50 | -78.52 |
| 2026-08-24 | `AUTL` | 5 | $2.41 | $2.36 | -0.25 | $2.38 | +0.10 | -0.15 | -0.55 | -0.45 |
| 2026-08-24 | `CRDL` | 7 | $1.86 | $1.87 | +0.07 | $1.80 | -0.49 | -0.42 | -0.42 | -0.91 |
| 2026-08-24 | `ENHA` | 8 | $1.72 | $1.74 | +0.16 | $1.69 | -0.40 | -0.24 | +0.24 | -0.16 |
| 2026-08-24 | `CAN` | 47 | $0.35 | $0.38 | +1.18 | $0.37 | -0.47 | +0.71 | +4.04 | +3.57 |
| 2026-08-25 | `BHP` | 13 | $96.66 | $95.95 | -9.23 | — | +0.00 | -9.23 | +64.22 | — |
| 2026-08-25 | `MRVI` | 171 | $8.26 | $8.31 | +8.55 | — | +0.00 | +8.55 | +159.03 | — |
| 2026-08-25 | `CRCL` | 15 | $89.01 | $86.02 | -44.85 | — | +0.00 | -44.85 | +40.95 | — |
| 2026-08-25 | `WYFI` | 58 | $20.79 | $20.98 | +11.02 | — | +0.00 | +11.02 | -24.36 | — |
| 2026-08-25 | `TOYO` | 284 | $4.61 | $4.48 | -36.92 | — | +0.00 | -36.92 | +14.20 | — |
| 2026-08-25 | `DVLT` | 4207 | $0.31 | $0.32 | +42.07 | — | +0.00 | +42.07 | +84.14 | — |
| 2026-08-25 | `SAFX` | 3565 | $0.35 | $0.37 | +71.30 | $0.37 | +0.00 | +71.30 | +57.04 | +57.04 |
| 2026-08-25 | `AAP` | 26 | $43.83 | $43.61 | -5.72 | — | +0.00 | -5.72 | -84.24 | — |
| 2026-08-25 | `AUTL` | 5 | $2.38 | $2.32 | -0.30 | $2.34 | +0.10 | -0.20 | -0.75 | -0.65 |
| 2026-08-25 | `CRDL` | 7 | $1.80 | $1.90 | +0.70 | $1.90 | +0.00 | +0.70 | -0.21 | -0.21 |
| 2026-08-25 | `ENHA` | 8 | $1.69 | $1.65 | -0.32 | $1.66 | +0.08 | -0.24 | -0.48 | -0.40 |
| 2026-08-25 | `CAN` | 47 | $0.37 | $0.38 | +0.47 | $0.36 | -0.94 | -0.47 | +4.04 | +3.10 |
| 2026-08-25 | `OCUL` | 116 | — | $10.92 | +0.00 | $10.92 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `CRMD` | 154 | — | $8.28 | +0.00 | $8.28 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `PUSA` | 344 | — | $3.70 | +0.00 | $3.91 | +72.24 | +72.24 | +0.00 | +72.24 |
| 2026-08-25 | `CAPR` | 187 | — | $6.79 | +0.00 | $7.19 | +74.80 | +74.80 | +0.00 | +74.80 |
| 2026-08-25 | `SUJA` | 145 | — | $8.79 | +0.00 | $8.54 | -36.25 | -36.25 | +0.00 | -36.25 |
| 2026-08-25 | `FWDI` | 212 | — | $5.99 | +0.00 | $5.86 | -27.56 | -27.56 | +0.00 | -27.56 |
| 2026-08-25 | `JANX` | 68 | — | $18.52 | +0.00 | $18.99 | +31.96 | +31.96 | +0.00 | +31.96 |
| 2026-08-26 | `SAFX` | 3565 | $0.37 | $0.37 | +0.00 | $0.37 | +0.00 | +0.00 | +57.04 | +57.04 |
| 2026-08-26 | `AUTL` | 5 | $2.34 | $2.34 | +0.00 | $2.34 | +0.00 | +0.00 | -0.65 | -0.65 |
| 2026-08-26 | `CRDL` | 7 | $1.90 | $1.90 | +0.00 | $1.90 | +0.00 | +0.00 | -0.21 | -0.21 |
| 2026-08-26 | `ENHA` | 8 | $1.66 | $1.66 | +0.00 | $1.66 | +0.00 | +0.00 | -0.40 | -0.40 |
| 2026-08-26 | `CAN` | 47 | $0.36 | $0.36 | +0.00 | $0.36 | +0.00 | +0.00 | +3.10 | +3.10 |
| 2026-08-26 | `OCUL` | 116 | $10.92 | $10.92 | +0.00 | $10.92 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `CRMD` | 154 | $8.28 | $8.28 | +0.00 | $8.28 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `PUSA` | 344 | $3.91 | $3.91 | +0.00 | $3.91 | +0.00 | +0.00 | +72.24 | +72.24 |
| 2026-08-26 | `CAPR` | 187 | $7.19 | $7.19 | +0.00 | $7.19 | +0.00 | +0.00 | +74.80 | +74.80 |
| 2026-08-26 | `SUJA` | 145 | $8.54 | $8.54 | +0.00 | $8.54 | +0.00 | +0.00 | -36.25 | -36.25 |
| 2026-08-26 | `FWDI` | 212 | $5.86 | $5.86 | +0.00 | $5.86 | +0.00 | +0.00 | -27.56 | -27.56 |
| 2026-08-26 | `JANX` | 68 | $18.99 | $18.99 | +0.00 | $18.99 | +0.00 | +0.00 | +31.96 | +31.96 |
| 2026-08-27 | `SAFX` | 3565 | $0.37 | $0.35 | -71.30 | — | +0.00 | -71.30 | -14.26 | — |
| 2026-08-27 | `AUTL` | 5 | $2.34 | $2.41 | +0.35 | — | +0.00 | +0.35 | -0.30 | — |
| 2026-08-27 | `CRDL` | 7 | $1.90 | $2.03 | +0.91 | — | +0.00 | +0.91 | +0.70 | — |
| 2026-08-27 | `ENHA` | 8 | $1.66 | $1.63 | -0.24 | — | +0.00 | -0.24 | -0.64 | — |
| 2026-08-27 | `CAN` | 47 | $0.36 | $0.40 | +1.88 | — | +0.00 | +1.88 | +4.98 | — |
| 2026-08-27 | `OCUL` | 116 | $10.92 | $10.79 | -15.08 | $10.77 | -2.32 | -17.40 | -15.08 | -17.40 |
| 2026-08-27 | `CRMD` | 154 | $8.28 | $8.60 | +49.28 | $8.39 | -32.34 | +16.94 | +49.28 | +16.94 |
| 2026-08-27 | `PUSA` | 344 | $3.91 | $3.84 | -24.08 | $3.85 | +3.44 | -20.64 | +48.16 | +51.60 |
| 2026-08-27 | `CAPR` | 187 | $7.19 | $8.29 | +205.70 | $9.36 | +200.09 | +405.79 | +280.50 | +480.59 |
| 2026-08-27 | `SUJA` | 145 | $8.54 | $9.39 | +123.25 | $9.44 | +7.25 | +130.50 | +87.00 | +94.25 |
| 2026-08-27 | `FWDI` | 212 | $5.86 | $5.97 | +23.32 | $5.93 | -8.48 | +14.84 | -4.24 | -12.72 |
| 2026-08-27 | `JANX` | 68 | $18.99 | $18.59 | -27.20 | $18.89 | +20.40 | -6.80 | +4.76 | +25.16 |
| 2026-08-27 | `ACMR` | 2 | — | $80.97 | +0.00 | $79.11 | -3.72 | -3.72 | +0.00 | -3.72 |
| 2026-08-27 | `GGB` | 36 | — | $4.42 | +0.00 | $4.46 | +1.44 | +1.44 | +0.00 | +1.44 |
| 2026-08-27 | `MT` | 2 | — | $75.12 | +0.00 | $74.53 | -1.18 | -1.18 | +0.00 | -1.18 |
| 2026-08-27 | `TX` | 2 | — | $55.20 | +0.00 | $55.13 | -0.14 | -0.14 | +0.00 | -0.14 |
| 2026-08-28 | `OCUL` | 116 | $10.77 | $10.63 | -16.24 | — | +0.00 | -16.24 | -33.64 | — |
| 2026-08-28 | `CRMD` | 154 | $8.39 | $8.49 | +15.40 | — | +0.00 | +15.40 | +32.34 | — |
| 2026-08-28 | `PUSA` | 344 | $3.85 | $3.86 | +3.44 | — | +0.00 | +3.44 | +55.04 | — |
| 2026-08-28 | `CAPR` | 187 | $9.36 | $9.19 | -31.79 | $10.06 | +162.69 | +130.90 | +448.80 | +611.49 |
| 2026-08-28 | `SUJA` | 145 | $9.44 | $9.41 | -4.35 | — | +0.00 | -4.35 | +89.90 | — |
| 2026-08-28 | `FWDI` | 212 | $5.93 | $6.39 | +97.52 | — | +0.00 | +97.52 | +84.80 | — |
| 2026-08-28 | `JANX` | 68 | $18.89 | $19.00 | +7.48 | — | +0.00 | +7.48 | +32.64 | — |
| 2026-08-28 | `ACMR` | 2 | $79.11 | $81.65 | +5.08 | $80.49 | -2.32 | +2.76 | +1.36 | -0.96 |
| 2026-08-28 | `GGB` | 36 | $4.46 | $4.57 | +3.96 | $4.70 | +4.68 | +8.64 | +5.40 | +10.08 |
| 2026-08-28 | `MT` | 2 | $74.53 | $74.54 | +0.02 | $74.63 | +0.18 | +0.20 | -1.16 | -0.98 |
| 2026-08-28 | `TX` | 2 | $55.13 | $55.25 | +0.24 | $55.83 | +1.16 | +1.40 | +0.10 | +1.26 |
| 2026-08-28 | `SEDG` | 36 | — | $33.78 | +0.00 | $33.51 | -9.72 | -9.72 | +0.00 | -9.72 |
| 2026-08-28 | `SMTC` | 8 | — | $149.40 | +0.00 | $142.43 | -55.76 | -55.76 | +0.00 | -55.76 |
| 2026-08-28 | `OPTX` | 143 | — | $8.57 | +0.00 | $8.73 | +22.88 | +22.88 | +0.00 | +22.88 |
| 2026-08-28 | `TTMI` | 9 | — | $127.07 | +0.00 | $124.73 | -21.06 | -21.06 | +0.00 | -21.06 |
| 2026-08-28 | `BBWI` | 65 | — | $18.68 | +0.00 | $18.65 | -1.95 | -1.95 | +0.00 | -1.95 |
| 2026-08-28 | `BTSG` | 19 | — | $61.42 | +0.00 | $60.90 | -9.88 | -9.88 | +0.00 | -9.88 |
| 2026-08-28 | `CRDL` | 586 | — | $2.09 | +0.00 | $2.06 | -17.58 | -17.58 | +0.00 | -17.58 |
| 2026-08-31 | `CAPR` | 187 | $10.06 | $9.44 | -115.94 | — | +0.00 | -115.94 | +495.55 | — |
| 2026-08-31 | `ACMR` | 2 | $80.49 | $75.10 | -10.78 | $75.02 | -0.16 | -10.94 | -11.74 | -11.90 |
| 2026-08-31 | `GGB` | 36 | $4.70 | $4.55 | -5.40 | $4.55 | +0.00 | -5.40 | +4.68 | +4.68 |
| 2026-08-31 | `MT` | 2 | $74.63 | $75.07 | +0.88 | $75.06 | -0.02 | +0.86 | -0.10 | -0.12 |
| 2026-08-31 | `TX` | 2 | $55.83 | $54.84 | -1.98 | $54.84 | +0.00 | -1.98 | -0.72 | -0.72 |
| 2026-08-31 | `SEDG` | 36 | $33.51 | $31.50 | -72.36 | $31.27 | -8.28 | -80.64 | -82.08 | -90.36 |
| 2026-08-31 | `SMTC` | 8 | $142.43 | $133.04 | -75.12 | $132.54 | -4.00 | -79.12 | -130.88 | -134.88 |
| 2026-08-31 | `OPTX` | 143 | $8.73 | $8.52 | -30.03 | $8.52 | +0.00 | -30.03 | -7.15 | -7.15 |
| 2026-08-31 | `TTMI` | 9 | $124.73 | $117.20 | -67.77 | $120.19 | +26.91 | -40.86 | -88.83 | -61.92 |
| 2026-08-31 | `BBWI` | 65 | $18.65 | $19.30 | +42.25 | $19.22 | -5.20 | +37.05 | +40.30 | +35.10 |
| 2026-08-31 | `BTSG` | 19 | $60.90 | $59.66 | -23.56 | $59.66 | +0.00 | -23.56 | -33.44 | -33.44 |
| 2026-08-31 | `CRDL` | 586 | $2.06 | $1.96 | -58.60 | $1.96 | +0.00 | -58.60 | -76.18 | -76.18 |
| 2026-09-01 | `ACMR` | 2 | $75.02 | $71.24 | -7.56 | — | +0.00 | -7.56 | -19.46 | — |
| 2026-09-01 | `GGB` | 36 | $4.55 | $4.61 | +2.16 | — | +0.00 | +2.16 | +6.84 | — |
| 2026-09-01 | `MT` | 2 | $75.06 | $74.31 | -1.50 | — | +0.00 | -1.50 | -1.62 | — |
| 2026-09-01 | `TX` | 2 | $54.84 | $54.82 | -0.04 | — | +0.00 | -0.04 | -0.76 | — |
| 2026-09-01 | `SEDG` | 36 | $31.27 | $32.22 | +34.20 | $31.80 | -15.12 | +19.08 | -56.16 | -71.28 |
| 2026-09-01 | `SMTC` | 8 | $132.54 | $131.65 | -7.12 | $129.50 | -17.20 | -24.32 | -142.00 | -159.20 |
| 2026-09-01 | `OPTX` | 143 | $8.52 | $8.19 | -47.19 | $8.19 | +0.00 | -47.19 | -54.34 | -54.34 |
| 2026-09-01 | `TTMI` | 9 | $120.19 | $119.79 | -3.60 | $116.94 | -25.65 | -29.25 | -65.52 | -91.17 |
| 2026-09-01 | `BBWI` | 65 | $19.22 | $19.10 | -7.80 | $19.10 | +0.00 | -7.80 | +27.30 | +27.30 |
| 2026-09-01 | `BTSG` | 19 | $59.66 | $58.40 | -23.94 | $58.40 | +0.00 | -23.94 | -57.38 | -57.38 |
| 2026-09-01 | `CRDL` | 586 | $1.96 | $1.98 | +11.72 | $1.98 | +0.00 | +11.72 | -64.46 | -64.46 |
| 2026-09-02 | `SEDG` | 36 | $31.80 | $31.87 | +2.52 | — | +0.00 | +2.52 | -68.76 | — |
| 2026-09-02 | `SMTC` | 8 | $129.50 | $127.63 | -14.96 | — | +0.00 | -14.96 | -174.16 | — |
| 2026-09-02 | `OPTX` | 143 | $8.19 | $7.94 | -35.75 | — | +0.00 | -35.75 | -90.09 | — |
| 2026-09-02 | `TTMI` | 9 | $116.94 | $116.68 | -2.34 | — | +0.00 | -2.34 | -93.51 | — |
| 2026-09-02 | `BBWI` | 65 | $19.10 | $18.77 | -21.45 | — | +0.00 | -21.45 | +5.85 | — |
| 2026-09-02 | `BTSG` | 19 | $58.40 | $58.55 | +2.85 | — | +0.00 | +2.85 | -54.53 | — |
| 2026-09-02 | `CRDL` | 586 | $1.98 | $1.94 | -23.44 | — | +0.00 | -23.44 | -87.90 | — |
| 2026-09-03 | `CABA` | 393 | — | $3.27 | +0.00 | $3.57 | +117.90 | +117.90 | +0.00 | +117.90 |
| 2026-09-03 | `FRVO` | 69 | — | $18.40 | +0.00 | $17.98 | -28.98 | -28.98 | +0.00 | -28.98 |
| 2026-09-03 | `CTMX` | 346 | — | $3.72 | +0.00 | $3.72 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `EIX` | 22 | — | $56.78 | +0.00 | $55.19 | -34.98 | -34.98 | +0.00 | -34.98 |
| 2026-09-03 | `CRDL` | 595 | — | $2.16 | +0.00 | $2.17 | +5.95 | +5.95 | +0.00 | +5.95 |
| 2026-09-03 | `SION` | 194 | — | $6.63 | +0.00 | $7.31 | +131.92 | +131.92 | +0.00 | +131.92 |
| 2026-09-03 | `DUOL` | 8 | — | $156.24 | +0.00 | $157.85 | +12.88 | +12.88 | +0.00 | +12.88 |
| 2026-09-03 | `SAFX` | 3300 | — | $0.39 | +0.00 | $0.38 | -33.00 | -33.00 | +0.00 | -33.00 |
| 2026-09-04 | `CABA` | 393 | $3.57 | $3.63 | +23.58 | $3.48 | -58.95 | -35.37 | +141.48 | +82.53 |
| 2026-09-04 | `FRVO` | 69 | $17.98 | $18.27 | +20.01 | $17.16 | -76.59 | -56.58 | -8.97 | -85.56 |
| 2026-09-04 | `CTMX` | 346 | $3.72 | $3.73 | +3.46 | $3.68 | -17.30 | -13.84 | +3.46 | -13.84 |
| 2026-09-04 | `EIX` | 22 | $55.19 | $55.42 | +5.06 | $56.30 | +19.36 | +24.42 | -29.92 | -10.56 |
| 2026-09-04 | `CRDL` | 595 | $2.17 | $2.18 | +5.95 | $2.16 | -11.90 | -5.95 | +11.90 | +0.00 |
| 2026-09-04 | `SION` | 194 | $7.31 | $7.31 | +0.00 | $6.75 | -108.64 | -108.64 | +131.92 | +23.28 |
| 2026-09-04 | `DUOL` | 8 | $157.85 | $161.54 | +29.52 | $158.82 | -21.76 | +7.76 | +42.40 | +20.64 |
| 2026-09-04 | `SAFX` | 3300 | $0.38 | $0.38 | +0.00 | $0.38 | +0.00 | +0.00 | -33.00 | -33.00 |
| 2026-09-04 | `SLBT` | 2 | — | $3.07 | +0.00 | $3.15 | +0.16 | +0.16 | +0.00 | +0.16 |
| 2026-09-04 | `IRD` | 1 | — | $4.66 | +0.00 | $4.60 | -0.06 | -0.06 | +0.00 | -0.06 |
| 2026-09-04 | `JLHL` | 1 | — | $6.20 | +0.00 | $6.18 | -0.02 | -0.02 | +0.00 | -0.02 |
| 2026-09-07 | `CABA` | 393 | $3.48 | $3.46 | -7.86 | $3.47 | +3.93 | -3.93 | +74.67 | +78.60 |
| 2026-09-07 | `FRVO` | 69 | $17.16 | $17.27 | +7.59 | $18.16 | +61.41 | +69.00 | -77.97 | -16.56 |
| 2026-09-07 | `CTMX` | 346 | $3.68 | $3.64 | -13.84 | $3.71 | +24.22 | +10.38 | -27.68 | -3.46 |
| 2026-09-07 | `EIX` | 22 | $56.30 | $55.79 | -11.22 | $56.77 | +21.56 | +10.34 | -21.78 | -0.22 |
| 2026-09-07 | `CRDL` | 595 | $2.16 | $2.16 | +0.00 | $2.20 | +23.80 | +23.80 | +0.00 | +23.80 |
| 2026-09-07 | `SION` | 194 | $6.75 | $6.68 | -13.58 | $7.18 | +97.00 | +83.42 | +9.70 | +106.70 |
| 2026-09-07 | `DUOL` | 8 | $158.82 | $157.46 | -10.88 | $154.46 | -24.00 | -34.88 | +9.76 | -14.24 |
| 2026-09-07 | `SAFX` | 3300 | $0.38 | $0.38 | +0.00 | $0.39 | +33.00 | +33.00 | -33.00 | +0.00 |
| 2026-09-07 | `SLBT` | 2 | $3.15 | $3.15 | +0.00 | $2.88 | -0.54 | -0.54 | +0.16 | -0.38 |
| 2026-09-07 | `IRD` | 1 | $4.60 | $4.53 | -0.07 | $4.67 | +0.14 | +0.07 | -0.13 | +0.01 |
| 2026-09-07 | `JLHL` | 1 | $6.18 | $6.20 | +0.02 | $6.09 | -0.11 | -0.09 | +0.00 | -0.11 |
| 2026-09-07 | `MRLN` | 1 | — | $3.28 | +0.00 | $3.35 | +0.07 | +0.07 | +0.00 | +0.07 |
| 2026-09-07 | `BTBT` | 2 | — | $1.60 | +0.00 | $1.64 | +0.08 | +0.08 | +0.00 | +0.08 |
| 2026-09-07 | `BRR` | 1 | — | $2.51 | +0.00 | $2.66 | +0.15 | +0.15 | +0.00 | +0.15 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +115.74 | TGTX, SLS, HIMS, VOR | — | $28.15 | $10,106.28 | TGTX×50, SLS×213, HIMS×84, VOR×113 |
| 2026-08-14 | +5.50 | $28.15 | TGTX×50, SLS×213, HIMS×84, VOR×113 | $10,117.74 | +11.46 | +36.54 | — | — | $28.15 | $10,154.28 | TGTX×50, SLS×213, HIMS×84, VOR×113 |
| 2026-08-17 | +2.25 | $28.15 | TGTX×50, SLS×213, HIMS×84, VOR×113 | $10,139.88 | -14.40 | +124.06 | DNN, INV, KLC | — | $18.95 | $10,263.84 | TGTX×50, SLS×213, HIMS×84, VOR×113, DNN×1, INV×2, KLC×1 |
| 2026-08-18 | -6.20 | $18.95 | TGTX×50, SLS×213, HIMS×84, VOR×113, DNN×1, INV×2, KLC×1 | $10,105.87 | -157.97 | +0.24 | — | TGTX, SLS, HIMS, VOR | $10,087.97 | $10,096.49 | DNN×1, INV×2, KLC×1 |
| 2026-08-19 | -7.20 | $10,087.97 | DNN×1, INV×2, KLC×1 | $10,096.61 | +0.12 | +0.57 | — | — | $10,087.97 | $10,097.18 | DNN×1, INV×2, KLC×1 |
| 2026-08-20 | +1.12 | $10,087.97 | DNN×1, INV×2, KLC×1 | $10,097.15 | -0.03 | +134.23 | BHP, MRVI, CRCL, WYFI, TOYO, DVLT, SAFX, AAP | DNN, INV, KLC | $97.98 | $10,168.19 | BHP×13, MRVI×171, CRCL×15, WYFI×58, TOYO×284, DVLT×4207, SAFX×3565, AAP×26 |
| 2026-08-21 | +3.25 | $97.98 | BHP×13, MRVI×171, CRCL×15, WYFI×58, TOYO×284, DVLT×4207, SAFX×3565, AAP×26 | $10,292.31 | +124.12 | +73.61 | AUTL, CRDL, ENHA, CAN | — | $43.88 | $10,365.17 | BHP×13, MRVI×171, CRCL×15, WYFI×58, TOYO×284, DVLT×4207, SAFX×3565, AAP×26, AUTL×5, CRDL×7, ENHA×8, CAN×47 |
| 2026-08-24 | -5.17 | $43.88 | BHP×13, MRVI×171, CRCL×15, WYFI×58, TOYO×284, DVLT×4207, SAFX×3565, AAP×26, AUTL×5, CRDL×7, ENHA×8, CAN×47 | $10,312.51 | -52.66 | -2.47 | — | — | $43.88 | $10,310.04 | BHP×13, MRVI×171, CRCL×15, WYFI×58, TOYO×284, DVLT×4207, SAFX×3565, AAP×26, AUTL×5, CRDL×7, ENHA×8, CAN×47 |
| 2026-08-25 | +1.80 | $43.88 | BHP×13, MRVI×171, CRCL×15, WYFI×58, TOYO×284, DVLT×4207, SAFX×3565, AAP×26, AUTL×5, CRDL×7, ENHA×8, CAN×47 | $10,346.81 | +36.77 | +114.43 | OCUL, CRMD, PUSA, CAPR, SUJA, FWDI, JANX | BHP, MRVI, CRCL, WYFI, TOYO, DVLT, AAP | $23.08 | $10,400.68 | SAFX×3565, AUTL×5, CRDL×7, ENHA×8, CAN×47, OCUL×116, CRMD×154, PUSA×344, CAPR×187, SUJA×145, FWDI×212, JANX×68 |
| 2026-08-26 | +2.02 | $23.08 | SAFX×3565, AUTL×5, CRDL×7, ENHA×8, CAN×47, OCUL×116, CRMD×154, PUSA×344, CAPR×187, SUJA×145, FWDI×212, JANX×68 | $10,400.68 | -0.00 | +0.00 | — | — | $23.08 | $10,400.68 | SAFX×3565, AUTL×5, CRDL×7, ENHA×8, CAN×47, OCUL×116, CRMD×154, PUSA×344, CAPR×187, SUJA×145, FWDI×212, JANX×68 |
| 2026-08-27 | — | $23.08 | SAFX×3565, AUTL×5, CRDL×7, ENHA×8, CAN×47, OCUL×116, CRMD×154, PUSA×344, CAPR×187, SUJA×145, FWDI×212, JANX×68 | $10,667.47 | +266.79 | +184.44 | ACMR, GGB, MT, TX | SAFX, AUTL, CRDL, ENHA, CAN | $716.65 | $10,821.33 | OCUL×116, CRMD×154, PUSA×344, CAPR×187, SUJA×145, FWDI×212, JANX×68, ACMR×2, GGB×36, MT×2, TX×2 |
| 2026-08-28 | +0.75 | $716.65 | OCUL×116, CRMD×154, PUSA×344, CAPR×187, SUJA×145, FWDI×212, JANX×68, ACMR×2, GGB×36, MT×2, TX×2 | $10,902.09 | +80.76 | +73.32 | SEDG, SMTC, OPTX, TTMI, BBWI, BTSG, CRDL | OCUL, CRMD, PUSA, SUJA, FWDI, JANX | $172.66 | $10,938.25 | CAPR×187, ACMR×2, GGB×36, MT×2, TX×2, SEDG×36, SMTC×8, OPTX×143, TTMI×9, BBWI×65, BTSG×19, CRDL×586 |
| 2026-08-31 | -5.85 | $172.66 | CAPR×187, ACMR×2, GGB×36, MT×2, TX×2, SEDG×36, SMTC×8, OPTX×143, TTMI×9, BBWI×65, BTSG×19, CRDL×586 | $10,519.84 | -418.41 | +9.25 | — | CAPR | $1,935.35 | $10,526.50 | ACMR×2, GGB×36, MT×2, TX×2, SEDG×36, SMTC×8, OPTX×143, TTMI×9, BBWI×65, BTSG×19, CRDL×586 |
| 2026-09-01 | -6.30 | $1,935.35 | ACMR×2, GGB×36, MT×2, TX×2, SEDG×36, SMTC×8, OPTX×143, TTMI×9, BBWI×65, BTSG×19, CRDL×586 | $10,475.83 | -50.67 | -57.97 | — | ACMR, GGB, MT, TX | $2,496.18 | $10,411.99 | SEDG×36, SMTC×8, OPTX×143, TTMI×9, BBWI×65, BTSG×19, CRDL×586 |
| 2026-09-02 | -3.83 | $2,496.18 | SEDG×36, SMTC×8, OPTX×143, TTMI×9, BBWI×65, BTSG×19, CRDL×586 | $10,319.42 | -92.57 | +0.00 | — | SEDG, SMTC, OPTX, TTMI, BBWI, BTSG, CRDL | $10,298.83 | $10,298.83 | — |
| 2026-09-03 | -0.90 | $10,298.83 | — | $10,298.83 | +0.00 | +171.69 | CABA, FRVO, CTMX, EIX, CRDL, SION, DUOL, SAFX | — | $50.69 | $10,421.71 | CABA×393, FRVO×69, CTMX×346, EIX×22, CRDL×595, SION×194, DUOL×8, SAFX×3300 |
| 2026-09-04 | — | $50.69 | CABA×393, FRVO×69, CTMX×346, EIX×22, CRDL×595, SION×194, DUOL×8, SAFX×3300 | $10,509.29 | +87.58 | -275.70 | SLBT, IRD, JLHL | — | $33.50 | $10,233.40 | CABA×393, FRVO×69, CTMX×346, EIX×22, CRDL×595, SION×194, DUOL×8, SAFX×3300, SLBT×2, IRD×1, JLHL×1 |
| 2026-09-07 | — | $33.50 | CABA×393, FRVO×69, CTMX×346, EIX×22, CRDL×595, SION×194, DUOL×8, SAFX×3300, SLBT×2, IRD×1, JLHL×1 | $10,183.56 | -49.84 | +240.71 | MRLN, BTBT, BRR | — | $24.41 | $10,424.17 | CABA×393, FRVO×69, CTMX×346, EIX×22, CRDL×595, SION×194, DUOL×8, SAFX×3300, SLBT×2, IRD×1, JLHL×1, MRLN×1, BTBT×2, BRR×1 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 50 | $49.70 | $2.14 | — | $7,512.86 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=-0.8; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 213 | $11.70 | $2.75 | — | $5,018.01 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=-0.8; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 84 | $29.74 | $2.24 | — | $2,517.61 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=-5.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 113 | $22.01 | $2.33 | — | $28.15 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=+0.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.15 | ▲ close $10,106.28 vs 09:30 $10,000.00 (session +115.74) | 16:00 close · cash $28.15 · equity $10,106.28 vs 09:30 $10,000.00 (+106.28; session marks +115.74) · 4 name(s) marked open→close (per-name table). TGTX×50 09:30 $49.70 → close $47.94 -88.00; SLS×213 09:30 $11.70 → close $12.36 +140.58; HIMS×84 09:30 $29.74 → close $28.77 -81.48; VOR×113 09:30 $22.01 → close $23.29 +144.64 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.15 | ▲ 09:30 equity $10,117.74 vs yday $10,106.28 (+11.46) | 09:30 open · cash $28.15 (unchanged overnight, no fees) · equity $10,117.74 vs prior close $10,106.28 (+11.46) · 4 name(s) re-marked at the open (per-name table). TGTX×50 yday $47.94 → 09:30 $47.27 -33.50; SLS×213 yday $12.36 → 09:30 $12.40 +8.52; HIMS×84 yday $28.77 → 09:30 $29.15 +31.92; VOR×113 yday $23.29 → 09:30 $23.33 +4.52 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.15 | ▲ close $10,154.28 vs 09:30 $10,117.74 (session +36.54) | 16:00 close · cash $28.15 · equity $10,154.28 vs 09:30 $10,117.74 (+36.54; session marks +36.54) · 4 name(s) marked open→close (per-name table). TGTX×50 09:30 $47.27 → close $48.74 +73.50; SLS×213 09:30 $12.40 → close $12.78 +80.94; HIMS×84 09:30 $29.15 → close $28.15 -84.00; VOR×113 09:30 $23.33 → close $23.03 -33.90 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.15 | ▼ 09:30 equity $10,139.88 vs yday $10,154.28 (-14.40) | 09:30 open · cash $28.15 (unchanged overnight, no fees) · equity $10,139.88 vs prior close $10,154.28 (-14.40) · 4 name(s) re-marked at the open (per-name table). TGTX×50 yday $48.74 → 09:30 $48.74 +0.00; SLS×213 yday $12.78 → 09:30 $12.78 +0.00; HIMS×84 yday $28.15 → 09:30 $28.14 -0.84; VOR×113 yday $23.03 → 09:30 $22.91 -13.56 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 1 | $3.24 | $0.04 | — | $24.88 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=+0.3; leftover $3.52 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 2 | $1.62 | $0.04 | — | $21.60 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $3.52 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `KLC` | 1 | $2.62 | $0.03 | — | $18.95 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-49.7; leftover $3.52 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.95 | ▲ close $10,263.84 vs 09:30 $10,139.88 (session +124.06) | 16:00 close · cash $18.95 · equity $10,263.84 vs 09:30 $10,139.88 (+123.96; session marks +124.06) · 7 name(s) marked open→close (per-name table). TGTX×50 09:30 $48.74 → close $49.28 +27.00; SLS×213 09:30 $12.78 → close $13.00 +46.86; HIMS×84 09:30 $28.14 → close $28.61 +39.48; VOR×113 09:30 $22.91 → close $23.01 +11.30; DNN×1 09:30 $3.24 → close $3.19 -0.05; INV×2 09:30 $1.62 → close $1.39 -0.47; KLC×1 09:30 $2.62 → close $2.56 -0.06 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.95 | ▼ 09:30 equity $10,105.87 vs yday $10,263.84 (-157.97) | 09:30 open · cash $18.95 (unchanged overnight, no fees) · equity $10,105.87 vs prior close $10,263.84 (-157.97) · 7 name(s) re-marked at the open (per-name table). TGTX×50 yday $49.28 → 09:30 $49.28 +0.00; SLS×213 yday $13.00 → 09:30 $12.66 -72.42; HIMS×84 yday $28.61 → 09:30 $27.85 -63.84; VOR×113 yday $23.01 → 09:30 $22.82 -21.47; DNN×1 yday $3.19 → 09:30 $3.11 -0.08; INV×2 yday $1.39 → 09:30 $1.32 -0.12; KLC×1 yday $2.56 → 09:30 $2.52 -0.04 | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 50 | $49.28 | $2.17 | $-25.31 | $2,480.78 | ▼ -25.31 after sell → book $10,103.70; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 213 | $12.66 | $2.80 | $+198.93 | $5,174.55 | ▲ +198.93 after sell → book $10,100.89; vs 09:30 mark -2.81 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 84 | $27.85 | $2.27 | $-163.28 | $7,511.68 | ▼ -163.28 after sell → book $10,098.62; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 113 | $22.82 | $2.37 | $+86.83 | $10,087.97 | ▲ +86.83 after sell → book $10,096.25; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,087.97 | ▲ close $10,096.49 vs 09:30 $10,105.87 (session +0.24) | 16:00 close · cash $10,087.97 · equity $10,096.49 vs 09:30 $10,105.87 (-9.38; session marks +0.24) · 3 name(s) marked open→close (per-name table). DNN×1 09:30 $3.11 → close $3.15 +0.04; INV×2 09:30 $1.32 → close $1.32 +0.00; KLC×1 09:30 $2.52 → close $2.72 +0.20 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,087.97 | ▲ 09:30 equity $10,096.61 vs yday $10,096.49 (+0.12) | 09:30 open · cash $10,087.97 (unchanged overnight, no fees) · equity $10,096.61 vs prior close $10,096.49 (+0.12) · 3 name(s) re-marked at the open (per-name table). DNN×1 yday $3.15 → 09:30 $3.19 +0.04; INV×2 yday $1.32 → 09:30 $1.39 +0.13; KLC×1 yday $2.72 → 09:30 $2.67 -0.05 | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,087.97 | ▲ close $10,097.18 vs 09:30 $10,096.61 (session +0.57) | 16:00 close · cash $10,087.97 · equity $10,097.18 vs 09:30 $10,096.61 (+0.57; session marks +0.57) · 3 name(s) marked open→close (per-name table). DNN×1 09:30 $3.19 → close $3.22 +0.03; INV×2 09:30 $1.39 → close $1.54 +0.30; KLC×1 09:30 $2.67 → close $2.91 +0.24 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,087.97 | ▼ 09:30 equity $10,097.15 vs yday $10,097.18 (-0.03) | 09:30 open · cash $10,087.97 (unchanged overnight, no fees) · equity $10,097.15 vs prior close $10,097.18 (-0.03) · 3 name(s) re-marked at the open (per-name table). DNN×1 yday $3.22 → 09:30 $3.20 -0.02; INV×2 yday $1.54 → 09:30 $1.55 +0.02; KLC×1 yday $2.91 → 09:30 $2.88 -0.03 | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 1 | $3.20 | $0.06 | $-0.13 | $10,091.12 | ▼ -0.13 after sell → book $10,097.10; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `INV` | 2 | $1.55 | $0.06 | $-0.24 | $10,094.16 | ▼ -0.24 after sell → book $10,097.04; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `KLC` | 1 | $2.88 | $0.05 | $+0.18 | $10,096.99 | ▲ +0.18 after sell → book $10,096.99; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,911.83 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1262.12 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 171 | $7.38 | $2.50 | — | $7,647.35 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1262.12 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRCL` | 15 | $83.29 | $2.04 | — | $6,395.96 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable; 🔵; ⚪; ret5=+7.4; leftover $1262.12 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WYFI` | 58 | $21.40 | $2.16 | — | $5,152.60 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-25.2; leftover $1262.12 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 284 | $4.43 | $3.66 | — | $3,890.81 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-23.1; leftover $1262.12 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 4207 | $0.30 | $25.24 | — | $2,603.47 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-3.2; leftover $1262.12 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SAFX` | 3565 | $0.35 | $23.32 | — | $1,318.15 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-29.4; leftover $1262.12 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 26 | $46.85 | $2.07 | — | $97.98 | — | union ∩ last_red, no 🚨; gate last_red=True; list earn_react; 🔵; ret5=+5.0; leftover $1262.12 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.98 | ▲ close $10,168.19 vs 09:30 $10,097.15 (session +134.23) | 16:00 close · cash $97.98 · equity $10,168.19 vs 09:30 $10,097.15 (+71.04; session marks +134.23) · 8 name(s) marked open→close (per-name table). BHP×13 09:30 $91.01 → close $93.63 +34.06; MRVI×171 09:30 $7.38 → close $8.26 +150.48; CRCL×15 09:30 $83.29 → close $83.99 +10.50; WYFI×58 09:30 $21.40 → close $21.16 -13.92; TOYO×284 09:30 $4.43 → close $4.51 +24.14; DVLT×4207 09:30 $0.30 → close $0.32 +84.14; SAFX×3565 09:30 $0.35 → close $0.34 -39.21; AAP×26 09:30 $46.85 → close $42.39 -115.96 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.98 | ▲ 09:30 equity $10,292.31 vs yday $10,168.19 (+124.12) | 09:30 open · cash $97.98 (unchanged overnight, no fees) · equity $10,292.31 vs prior close $10,168.19 (+124.12) · 8 name(s) re-marked at the open (per-name table). BHP×13 yday $93.63 → 09:30 $95.72 +27.17; MRVI×171 yday $8.26 → 09:30 $8.20 -10.26; CRCL×15 yday $83.99 → 09:30 $87.65 +54.90; WYFI×58 yday $21.16 → 09:30 $21.54 +22.04; TOYO×284 yday $4.51 → 09:30 $4.68 +46.86; DVLT×4207 yday $0.32 → 09:30 $0.31 -42.07; SAFX×3565 yday $0.34 → 09:30 $0.35 +24.95; AAP×26 yday $42.39 → 09:30 $42.41 +0.52 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 5 | $2.47 | $0.14 | — | $85.49 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $14.00 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 7 | $1.93 | $0.16 | — | $71.82 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $14.00 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 8 | $1.71 | $0.16 | — | $57.98 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $14.00 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 47 | $0.29 | $0.28 | — | $43.88 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $14.00 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $43.88 | ▲ close $10,365.17 vs 09:30 $10,292.31 (session +73.61) | 16:00 close · cash $43.88 · equity $10,365.17 vs 09:30 $10,292.31 (+72.86; session marks +73.61) · 12 name(s) marked open→close (per-name table). BHP×13 09:30 $95.72 → close $97.03 +17.03; MRVI×171 09:30 $8.20 → close $8.70 +85.50; CRCL×15 09:30 $87.65 → close $88.94 +19.35; WYFI×58 09:30 $21.54 → close $20.72 -47.56; TOYO×284 09:30 $4.68 → close $4.82 +39.76; DVLT×4207 09:30 $0.31 → close $0.32 +42.07; SAFX×3565 09:30 $0.35 → close $0.33 -89.12; AAP×26 09:30 $42.41 → close $42.58 +4.42; AUTL×5 09:30 $2.47 → close $2.41 -0.30; CRDL×7 09:30 $1.93 → close $1.86 -0.49; ENHA×8 09:30 $1.71 → close $1.72 +0.08; CAN×47 09:30 $0.29 → close $0.35 +2.87 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.88 | ▼ 09:30 equity $10,312.51 vs yday $10,365.17 (-52.66) | 09:30 open · cash $43.88 (unchanged overnight, no fees) · equity $10,312.51 vs prior close $10,365.17 (-52.66) · 12 name(s) re-marked at the open (per-name table). BHP×13 yday $97.03 → 09:30 $97.34 +4.03; MRVI×171 yday $8.70 → 09:30 $8.59 -18.81; CRCL×15 yday $88.94 → 09:30 $89.55 +9.15; WYFI×58 yday $20.72 → 09:30 $20.02 -40.60; TOYO×284 yday $4.82 → 09:30 $4.58 -68.16; DVLT×4207 yday $0.32 → 09:30 $0.31 -42.07; SAFX×3565 yday $0.33 → 09:30 $0.35 +89.12; AAP×26 yday $42.58 → 09:30 $43.10 +13.52; AUTL×5 yday $2.41 → 09:30 $2.36 -0.25; CRDL×7 yday $1.86 → 09:30 $1.87 +0.07; ENHA×8 yday $1.72 → 09:30 $1.74 +0.16; CAN×47 yday $0.35 → 09:30 $0.38 +1.18 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $43.88 | ▼ close $10,310.04 vs 09:30 $10,312.51 (session -2.47) | 16:00 close · cash $43.88 · equity $10,310.04 vs 09:30 $10,312.51 (-2.47; session marks -2.47) · 12 name(s) marked open→close (per-name table). BHP×13 09:30 $97.34 → close $96.66 -8.84; MRVI×171 09:30 $8.59 → close $8.26 -56.43; CRCL×15 09:30 $89.55 → close $89.01 -8.10; WYFI×58 09:30 $20.02 → close $20.79 +44.66; TOYO×284 09:30 $4.58 → close $4.61 +8.52; DVLT×4207 09:30 $0.31 → close $0.31 +0.00; SAFX×3565 09:30 $0.35 → close $0.35 +0.00; AAP×26 09:30 $43.10 → close $43.83 +18.98; AUTL×5 09:30 $2.36 → close $2.38 +0.10; CRDL×7 09:30 $1.87 → close $1.80 -0.49; ENHA×8 09:30 $1.74 → close $1.69 -0.40; CAN×47 09:30 $0.38 → close $0.37 -0.47 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.88 | ▲ 09:30 equity $10,346.81 vs yday $10,310.04 (+36.77) | 09:30 open · cash $43.88 (unchanged overnight, no fees) · equity $10,346.81 vs prior close $10,310.04 (+36.77) · 12 name(s) re-marked at the open (per-name table). BHP×13 yday $96.66 → 09:30 $95.95 -9.23; MRVI×171 yday $8.26 → 09:30 $8.31 +8.55; CRCL×15 yday $89.01 → 09:30 $86.02 -44.85; WYFI×58 yday $20.79 → 09:30 $20.98 +11.02; TOYO×284 yday $4.61 → 09:30 $4.48 -36.92; DVLT×4207 yday $0.31 → 09:30 $0.32 +42.07; SAFX×3565 yday $0.35 → 09:30 $0.37 +71.30; AAP×26 yday $43.83 → 09:30 $43.61 -5.72; AUTL×5 yday $2.38 → 09:30 $2.32 -0.30; CRDL×7 yday $1.80 → 09:30 $1.90 +0.70; ENHA×8 yday $1.69 → 09:30 $1.65 -0.32; CAN×47 yday $0.37 → 09:30 $0.38 +0.47 | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.95 | $2.05 | $+60.14 | $1,289.19 | ▲ +60.14 after sell → book $10,344.77; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRVI` | 171 | $8.31 | $2.54 | $+153.98 | $2,707.65 | ▲ +153.98 after sell → book $10,342.22; vs 09:30 mark -2.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRCL` | 15 | $86.02 | $2.06 | $+36.86 | $3,995.90 | ▲ +36.86 after sell → book $10,340.17; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WYFI` | 58 | $20.98 | $2.18 | $-28.71 | $5,210.55 | ▼ -28.71 after sell → book $10,337.98; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `TOYO` | 284 | $4.48 | $3.72 | $+6.82 | $6,479.15 | ▲ +6.82 after sell → book $10,334.26; vs 09:30 mark -3.72 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DVLT` | 4207 | $0.32 | $26.79 | $+32.11 | $7,798.60 | ▲ +32.11 after sell → book $10,307.47; vs 09:30 mark -26.79 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AAP` | 26 | $43.61 | $2.09 | $-88.40 | $8,930.37 | ▼ -88.40 after sell → book $10,305.38; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 116 | $10.92 | $2.34 | — | $7,661.31 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=+10.4; leftover $1275.77 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 154 | $8.28 | $2.45 | — | $6,383.74 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1275.77 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 344 | $3.70 | $4.44 | — | $5,106.50 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $1275.77 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 187 | $6.79 | $2.55 | — | $3,834.22 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $1275.77 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 145 | $8.79 | $2.42 | — | $2,557.25 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.5; leftover $1275.77 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 212 | $5.99 | $2.73 | — | $1,284.63 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+20.7; leftover $1275.77 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `JANX` | 68 | $18.52 | $2.19 | — | $23.08 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=+7.9; leftover $1275.77 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.08 | ▲ close $10,400.68 vs 09:30 $10,346.81 (session +114.43) | 16:00 close · cash $23.08 · equity $10,400.68 vs 09:30 $10,346.81 (+53.87; session marks +114.43) · 12 name(s) marked open→close (per-name table). SAFX×3565 09:30 $0.37 → close $0.37 +0.00; AUTL×5 09:30 $2.32 → close $2.34 +0.10; CRDL×7 09:30 $1.90 → close $1.90 +0.00; ENHA×8 09:30 $1.65 → close $1.66 +0.08; CAN×47 09:30 $0.38 → close $0.36 -0.94; OCUL×116 09:30 $10.92 → close $10.92 +0.00; CRMD×154 09:30 $8.28 → close $8.28 +0.00; PUSA×344 09:30 $3.70 → close $3.91 +72.24; CAPR×187 09:30 $6.79 → close $7.19 +74.80; SUJA×145 09:30 $8.79 → close $8.54 -36.25; FWDI×212 09:30 $5.99 → close $5.86 -27.56; JANX×68 09:30 $18.52 → close $18.99 +31.96 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.08 | ▲ 09:30 equity $10,400.68 vs yday $10,400.68 (-0.00) | 09:30 open · cash $23.08 (unchanged overnight, no fees) · equity $10,400.68 vs prior close $10,400.68 (-0.00) · 12 name(s) re-marked at the open (per-name table). SAFX×3565 yday $0.37 → 09:30 $0.37 +0.00; AUTL×5 yday $2.34 → 09:30 $2.34 +0.00; CRDL×7 yday $1.90 → 09:30 $1.90 +0.00; ENHA×8 yday $1.66 → 09:30 $1.66 +0.00; CAN×47 yday $0.36 → 09:30 $0.36 +0.00; OCUL×116 yday $10.92 → 09:30 $10.92 +0.00; CRMD×154 yday $8.28 → 09:30 $8.28 +0.00; PUSA×344 yday $3.91 → 09:30 $3.91 +0.00; CAPR×187 yday $7.19 → 09:30 $7.19 +0.00; SUJA×145 yday $8.54 → 09:30 $8.54 +0.00; FWDI×212 yday $5.86 → 09:30 $5.86 +0.00; JANX×68 yday $18.99 → 09:30 $18.99 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.08 | ▲ close $10,400.68 vs 09:30 $10,400.68 (session +0.00) | 16:00 close · cash $23.08 · equity $10,400.68 vs 09:30 $10,400.68 (-0.00; session marks +0.00) · 12 name(s) marked open→close (per-name table). SAFX×3565 09:30 $0.37 → close $0.37 +0.00; AUTL×5 09:30 $2.34 → close $2.34 +0.00; CRDL×7 09:30 $1.90 → close $1.90 +0.00; ENHA×8 09:30 $1.66 → close $1.66 +0.00; CAN×47 09:30 $0.36 → close $0.36 +0.00; OCUL×116 09:30 $10.92 → close $10.92 +0.00; CRMD×154 09:30 $8.28 → close $8.28 +0.00; PUSA×344 09:30 $3.91 → close $3.91 +0.00; CAPR×187 09:30 $7.19 → close $7.19 +0.00; SUJA×145 09:30 $8.54 → close $8.54 +0.00; FWDI×212 09:30 $5.86 → close $5.86 +0.00; JANX×68 09:30 $18.99 → close $18.99 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.08 | ▲ 09:30 equity $10,667.47 vs yday $10,400.68 (+266.79) | 09:30 open · cash $23.08 (unchanged overnight, no fees) · equity $10,667.47 vs prior close $10,400.68 (+266.79) · 12 name(s) re-marked at the open (per-name table). SAFX×3565 yday $0.37 → 09:30 $0.35 -71.30; AUTL×5 yday $2.34 → 09:30 $2.41 +0.35; CRDL×7 yday $1.90 → 09:30 $2.03 +0.91; ENHA×8 yday $1.66 → 09:30 $1.63 -0.24; CAN×47 yday $0.36 → 09:30 $0.40 +1.88; OCUL×116 yday $10.92 → 09:30 $10.79 -15.08; CRMD×154 yday $8.28 → 09:30 $8.60 +49.28; PUSA×344 yday $3.91 → 09:30 $3.84 -24.08; CAPR×187 yday $7.19 → 09:30 $8.29 +205.70; SUJA×145 yday $8.54 → 09:30 $9.39 +123.25; FWDI×212 yday $5.86 → 09:30 $5.97 +23.32; JANX×68 yday $18.99 → 09:30 $18.59 -27.20 | — |
| 2026-08-27 09:30 ET | **SELL** | `SAFX` | 3565 | $0.35 | $23.77 | $-61.35 | $1,247.06 | ▼ -61.35 after sell → book $10,643.70; vs 09:30 mark -23.77 | dropped from list after 5 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 5 | $2.41 | $0.16 | $-0.59 | $1,258.95 | ▼ -0.59 after sell → book $10,643.54; vs 09:30 mark -0.16 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 7 | $2.03 | $0.18 | $+0.36 | $1,272.98 | ▲ +0.36 after sell → book $10,643.36; vs 09:30 mark -0.18 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ENHA` | 8 | $1.63 | $0.17 | $-0.98 | $1,285.84 | ▼ -0.98 after sell → book $10,643.18; vs 09:30 mark -0.18 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAN` | 47 | $0.40 | $0.35 | $+4.35 | $1,304.29 | ▲ +4.35 after sell → book $10,642.83; vs 09:30 mark -0.35 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 2 | $80.97 | $1.63 | — | $1,140.73 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=-1.3; leftover $163.04 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 36 | $4.42 | $1.70 | — | $979.91 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; ret5=-8.6; leftover $163.04 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 2 | $75.12 | $1.51 | — | $828.16 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; ret5=-2.2; leftover $163.04 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 2 | $55.20 | $1.11 | — | $716.65 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; ret5=+3.0; leftover $163.04 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $716.65 | ▲ close $10,821.33 vs 09:30 $10,667.47 (session +184.44) | 16:00 close · cash $716.65 · equity $10,821.33 vs 09:30 $10,667.47 (+153.86; session marks +184.44) · 11 name(s) marked open→close (per-name table). OCUL×116 09:30 $10.79 → close $10.77 -2.32; CRMD×154 09:30 $8.60 → close $8.39 -32.34; PUSA×344 09:30 $3.84 → close $3.85 +3.44; CAPR×187 09:30 $8.29 → close $9.36 +200.09; SUJA×145 09:30 $9.39 → close $9.44 +7.25; FWDI×212 09:30 $5.97 → close $5.93 -8.48; JANX×68 09:30 $18.59 → close $18.89 +20.40; ACMR×2 09:30 $80.97 → close $79.11 -3.72; GGB×36 09:30 $4.42 → close $4.46 +1.44; MT×2 09:30 $75.12 → close $74.53 -1.18; TX×2 09:30 $55.20 → close $55.13 -0.14 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $716.65 | ▲ 09:30 equity $10,902.09 vs yday $10,821.33 (+80.76) | 09:30 open · cash $716.65 (unchanged overnight, no fees) · equity $10,902.09 vs prior close $10,821.33 (+80.76) · 11 name(s) re-marked at the open (per-name table). OCUL×116 yday $10.77 → 09:30 $10.63 -16.24; CRMD×154 yday $8.39 → 09:30 $8.49 +15.40; PUSA×344 yday $3.85 → 09:30 $3.86 +3.44; CAPR×187 yday $9.36 → 09:30 $9.19 -31.79; SUJA×145 yday $9.44 → 09:30 $9.41 -4.35; FWDI×212 yday $5.93 → 09:30 $6.39 +97.52; JANX×68 yday $18.89 → 09:30 $19.00 +7.48; ACMR×2 yday $79.11 → 09:30 $81.65 +5.08; GGB×36 yday $4.46 → 09:30 $4.57 +3.96; MT×2 yday $74.53 → 09:30 $74.54 +0.02; TX×2 yday $55.13 → 09:30 $55.25 +0.24 | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 116 | $10.63 | $2.37 | $-38.35 | $1,947.36 | ▼ -38.35 after sell → book $10,899.72; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 154 | $8.49 | $2.49 | $+27.40 | $3,252.34 | ▲ +27.40 after sell → book $10,897.24; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `PUSA` | 344 | $3.86 | $4.51 | $+46.10 | $4,575.67 | ▲ +46.10 after sell → book $10,892.73; vs 09:30 mark -4.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SUJA` | 145 | $9.41 | $2.46 | $+85.02 | $5,937.66 | ▲ +85.02 after sell → book $10,890.27; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FWDI` | 212 | $6.39 | $2.78 | $+79.28 | $7,289.56 | ▲ +79.28 after sell → book $10,887.49; vs 09:30 mark -2.78 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `JANX` | 68 | $19.00 | $2.22 | $+28.23 | $8,579.34 | ▲ +28.23 after sell → book $10,885.27; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 36 | $33.78 | $2.10 | — | $7,361.17 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1225.62 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $6,163.95 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1225.62 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 143 | $8.57 | $2.42 | — | $4,936.02 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=-3.4; leftover $1225.62 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 9 | $127.07 | $2.02 | — | $3,790.38 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=-21.0; leftover $1225.62 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 65 | $18.68 | $2.19 | — | $2,573.99 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=+0.2; leftover $1225.62 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BTSG` | 19 | $61.42 | $2.05 | — | $1,404.96 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=-4.6; leftover $1225.62 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CRDL` | 586 | $2.09 | $7.56 | — | $172.66 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=+3.3; leftover $1225.62 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $172.66 | ▲ close $10,938.25 vs 09:30 $10,902.09 (session +73.32) | 16:00 close · cash $172.66 · equity $10,938.25 vs 09:30 $10,902.09 (+36.16; session marks +73.32) · 12 name(s) marked open→close (per-name table). CAPR×187 09:30 $9.19 → close $10.06 +162.69; ACMR×2 09:30 $81.65 → close $80.49 -2.32; GGB×36 09:30 $4.57 → close $4.70 +4.68; MT×2 09:30 $74.54 → close $74.63 +0.18; TX×2 09:30 $55.25 → close $55.83 +1.16; SEDG×36 09:30 $33.78 → close $33.51 -9.72; SMTC×8 09:30 $149.40 → close $142.43 -55.76; OPTX×143 09:30 $8.57 → close $8.73 +22.88; TTMI×9 09:30 $127.07 → close $124.73 -21.06; BBWI×65 09:30 $18.68 → close $18.65 -1.95; BTSG×19 09:30 $61.42 → close $60.90 -9.88; CRDL×586 09:30 $2.09 → close $2.06 -17.58 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $172.66 | ▼ 09:30 equity $10,519.84 vs yday $10,938.25 (-418.41) | 09:30 open · cash $172.66 (unchanged overnight, no fees) · equity $10,519.84 vs prior close $10,938.25 (-418.41) · 12 name(s) re-marked at the open (per-name table). CAPR×187 yday $10.06 → 09:30 $9.44 -115.94; ACMR×2 yday $80.49 → 09:30 $75.10 -10.78; GGB×36 yday $4.70 → 09:30 $4.55 -5.40; MT×2 yday $74.63 → 09:30 $75.07 +0.88; TX×2 yday $55.83 → 09:30 $54.84 -1.98; SEDG×36 yday $33.51 → 09:30 $31.50 -72.36; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; OPTX×143 yday $8.73 → 09:30 $8.52 -30.03; TTMI×9 yday $124.73 → 09:30 $117.20 -67.77; BBWI×65 yday $18.65 → 09:30 $19.30 +42.25; BTSG×19 yday $60.90 → 09:30 $59.66 -23.56; CRDL×586 yday $2.06 → 09:30 $1.96 -58.60 | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 187 | $9.44 | $2.60 | $+490.40 | $1,935.35 | ▲ +490.40 after sell → book $10,517.25; vs 09:30 mark -2.59 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,935.35 | ▲ close $10,526.50 vs 09:30 $10,519.84 (session +9.25) | 16:00 close · cash $1,935.35 · equity $10,526.50 vs 09:30 $10,519.84 (+6.66; session marks +9.25) · 11 name(s) marked open→close (per-name table). ACMR×2 09:30 $75.10 → close $75.02 -0.16; GGB×36 09:30 $4.55 → close $4.55 +0.00; MT×2 09:30 $75.07 → close $75.06 -0.02; TX×2 09:30 $54.84 → close $54.84 +0.00; SEDG×36 09:30 $31.50 → close $31.27 -8.28; SMTC×8 09:30 $133.04 → close $132.54 -4.00; OPTX×143 09:30 $8.52 → close $8.52 +0.00; TTMI×9 09:30 $117.20 → close $120.19 +26.91; BBWI×65 09:30 $19.30 → close $19.22 -5.20; BTSG×19 09:30 $59.66 → close $59.66 +0.00; CRDL×586 09:30 $1.96 → close $1.96 +0.00 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,935.35 | ▼ 09:30 equity $10,475.83 vs yday $10,526.50 (-50.67) | 09:30 open · cash $1,935.35 (unchanged overnight, no fees) · equity $10,475.83 vs prior close $10,526.50 (-50.67) · 11 name(s) re-marked at the open (per-name table). ACMR×2 yday $75.02 → 09:30 $71.24 -7.56; GGB×36 yday $4.55 → 09:30 $4.61 +2.16; MT×2 yday $75.06 → 09:30 $74.31 -1.50; TX×2 yday $54.84 → 09:30 $54.82 -0.04; SEDG×36 yday $31.27 → 09:30 $32.22 +34.20; SMTC×8 yday $132.54 → 09:30 $131.65 -7.12; OPTX×143 yday $8.52 → 09:30 $8.19 -47.19; TTMI×9 yday $120.19 → 09:30 $119.79 -3.60; BBWI×65 yday $19.22 → 09:30 $19.10 -7.80; BTSG×19 yday $59.66 → 09:30 $58.40 -23.94; CRDL×586 yday $1.96 → 09:30 $1.98 +11.72 | — |
| 2026-09-01 09:30 ET | **SELL** | `ACMR` | 2 | $71.24 | $1.45 | $-22.54 | $2,076.38 | ▼ -22.54 after sell → book $10,474.38; vs 09:30 mark -1.45 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `GGB` | 36 | $4.61 | $1.79 | $+3.35 | $2,240.55 | ▲ +3.35 after sell → book $10,472.59; vs 09:30 mark -1.79 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `MT` | 2 | $74.31 | $1.51 | $-4.64 | $2,387.66 | ▼ -4.64 after sell → book $10,471.08; vs 09:30 mark -1.51 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `TX` | 2 | $54.82 | $1.12 | $-2.99 | $2,496.18 | ▼ -2.99 after sell → book $10,469.96; vs 09:30 mark -1.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,496.18 | ▼ close $10,411.99 vs 09:30 $10,475.83 (session -57.97) | 16:00 close · cash $2,496.18 · equity $10,411.99 vs 09:30 $10,475.83 (-63.84; session marks -57.97) · 7 name(s) marked open→close (per-name table). SEDG×36 09:30 $32.22 → close $31.80 -15.12; SMTC×8 09:30 $131.65 → close $129.50 -17.20; OPTX×143 09:30 $8.19 → close $8.19 +0.00; TTMI×9 09:30 $119.79 → close $116.94 -25.65; BBWI×65 09:30 $19.10 → close $19.10 +0.00; BTSG×19 09:30 $58.40 → close $58.40 +0.00; CRDL×586 09:30 $1.98 → close $1.98 +0.00 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,496.18 | ▼ 09:30 equity $10,319.42 vs yday $10,411.99 (-92.57) | 09:30 open · cash $2,496.18 (unchanged overnight, no fees) · equity $10,319.42 vs prior close $10,411.99 (-92.57) · 7 name(s) re-marked at the open (per-name table). SEDG×36 yday $31.80 → 09:30 $31.87 +2.52; SMTC×8 yday $129.50 → 09:30 $127.63 -14.96; OPTX×143 yday $8.19 → 09:30 $7.94 -35.75; TTMI×9 yday $116.94 → 09:30 $116.68 -2.34; BBWI×65 yday $19.10 → 09:30 $18.77 -21.45; BTSG×19 yday $58.40 → 09:30 $58.55 +2.85; CRDL×586 yday $1.98 → 09:30 $1.94 -23.44 | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 36 | $31.87 | $2.12 | $-72.98 | $3,641.38 | ▼ -72.98 after sell → book $10,317.30; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $127.63 | $2.03 | $-178.21 | $4,660.38 | ▼ -178.21 after sell → book $10,315.26; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `OPTX` | 143 | $7.94 | $2.45 | $-94.96 | $5,793.35 | ▼ -94.96 after sell → book $10,312.81; vs 09:30 mark -2.45 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TTMI` | 9 | $116.68 | $2.04 | $-97.56 | $6,841.43 | ▼ -97.56 after sell → book $10,310.77; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBWI` | 65 | $18.77 | $2.21 | $+1.46 | $8,059.28 | ▲ +1.46 after sell → book $10,308.57; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BTSG` | 19 | $58.55 | $2.07 | $-58.64 | $9,169.66 | ▼ -58.64 after sell → book $10,306.50; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRDL` | 586 | $1.94 | $7.67 | $-103.13 | $10,298.83 | ▼ -103.13 after sell → book $10,298.83; vs 09:30 mark -7.67 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,298.83 | ▲ close $10,298.83 vs 09:30 $10,319.42 (session +0.00) | 16:00 close · cash $10,298.83 · no lots left · equity $10,298.83. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,298.83 | ▲ 09:30 equity $10,298.83 vs yday $10,298.83 (+0.00) | 09:30 open · cash $10,298.83 · no holdings · equity $10,298.83 vs prior close $10,298.83 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 393 | $3.27 | $5.07 | — | $9,008.65 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1287.35 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 69 | $18.40 | $2.20 | — | $7,736.86 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1287.35 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 346 | $3.72 | $4.46 | — | $6,445.27 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1287.35 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 22 | $56.78 | $2.06 | — | $5,194.06 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; ret5=+0.3; leftover $1287.35 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 595 | $2.16 | $7.68 | — | $3,901.18 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1287.35 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SION` | 194 | $6.63 | $2.57 | — | $2,612.39 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=-18.1; leftover $1287.35 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DUOL` | 8 | $156.24 | $2.01 | — | $1,360.46 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=+10.0; leftover $1287.35 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SAFX` | 3300 | $0.39 | $22.77 | — | $50.69 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=-26.5; leftover $1287.35 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.69 | ▲ close $10,421.71 vs 09:30 $10,298.83 (session +171.69) | 16:00 close · cash $50.69 · equity $10,421.71 vs 09:30 $10,298.83 (+122.88; session marks +171.69) · 8 name(s) marked open→close (per-name table). CABA×393 09:30 $3.27 → close $3.57 +117.90; FRVO×69 09:30 $18.40 → close $17.98 -28.98; CTMX×346 09:30 $3.72 → close $3.72 +0.00; EIX×22 09:30 $56.78 → close $55.19 -34.98; CRDL×595 09:30 $2.16 → close $2.17 +5.95; SION×194 09:30 $6.63 → close $7.31 +131.92; DUOL×8 09:30 $156.24 → close $157.85 +12.88; SAFX×3300 09:30 $0.39 → close $0.38 -33.00 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.69 | ▲ 09:30 equity $10,509.29 vs yday $10,421.71 (+87.58) | 09:30 open · cash $50.69 (unchanged overnight, no fees) · equity $10,509.29 vs prior close $10,421.71 (+87.58) · 8 name(s) re-marked at the open (per-name table). CABA×393 yday $3.57 → 09:30 $3.63 +23.58; FRVO×69 yday $17.98 → 09:30 $18.27 +20.01; CTMX×346 yday $3.72 → 09:30 $3.73 +3.46; EIX×22 yday $55.19 → 09:30 $55.42 +5.06; CRDL×595 yday $2.17 → 09:30 $2.18 +5.95; SION×194 yday $7.31 → 09:30 $7.31 +0.00; DUOL×8 yday $157.85 → 09:30 $161.54 +29.52; SAFX×3300 yday $0.38 → 09:30 $0.38 +0.00 | — |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 2 | $3.07 | $0.07 | — | $44.48 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $8.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 1 | $4.66 | $0.05 | — | $39.77 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $8.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `JLHL` | 1 | $6.20 | $0.07 | — | $33.50 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=-8.2; leftover $8.45 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.50 | ▼ close $10,233.40 vs 09:30 $10,509.29 (session -275.70) | 16:00 close · cash $33.50 · equity $10,233.40 vs 09:30 $10,509.29 (-275.89; session marks -275.70) · 11 name(s) marked open→close (per-name table). CABA×393 09:30 $3.63 → close $3.48 -58.95; FRVO×69 09:30 $18.27 → close $17.16 -76.59; CTMX×346 09:30 $3.73 → close $3.68 -17.30; EIX×22 09:30 $55.42 → close $56.30 +19.36; CRDL×595 09:30 $2.18 → close $2.16 -11.90; SION×194 09:30 $7.31 → close $6.75 -108.64; DUOL×8 09:30 $161.54 → close $158.82 -21.76; SAFX×3300 09:30 $0.38 → close $0.38 +0.00; SLBT×2 09:30 $3.07 → close $3.15 +0.16; IRD×1 09:30 $4.66 → close $4.60 -0.06; JLHL×1 09:30 $6.20 → close $6.18 -0.02 | — |
| 2026-09-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.50 | ▼ 09:30 equity $10,183.56 vs yday $10,233.40 (-49.84) | 09:30 open · cash $33.50 (unchanged overnight, no fees) · equity $10,183.56 vs prior close $10,233.40 (-49.84) · 11 name(s) re-marked at the open (per-name table). CABA×393 yday $3.48 → 09:30 $3.46 -7.86; FRVO×69 yday $17.16 → 09:30 $17.27 +7.59; CTMX×346 yday $3.68 → 09:30 $3.64 -13.84; EIX×22 yday $56.30 → 09:30 $55.79 -11.22; CRDL×595 yday $2.16 → 09:30 $2.16 +0.00; SION×194 yday $6.75 → 09:30 $6.68 -13.58; DUOL×8 yday $158.82 → 09:30 $157.46 -10.88; SAFX×3300 yday $0.38 → 09:30 $0.38 +0.00; SLBT×2 yday $3.15 → 09:30 $3.15 +0.00; IRD×1 yday $4.60 → 09:30 $4.53 -0.07; JLHL×1 yday $6.18 → 09:30 $6.20 +0.02 | — |
| 2026-09-07 09:30 ET | **BUY** | `MRLN` | 1 | $3.28 | $0.04 | — | $30.19 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.6; leftover $4.19 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `BTBT` | 2 | $1.60 | $0.04 | — | $26.95 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-2.5; leftover $4.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `BRR` | 1 | $2.51 | $0.03 | — | $24.41 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+28.0; leftover $4.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.41 | ▲ close $10,424.17 vs 09:30 $10,183.56 (session +240.71) | 16:00 close · cash $24.41 · equity $10,424.17 vs 09:30 $10,183.56 (+240.61; session marks +240.71) · 14 name(s) marked open→close (per-name table). CABA×393 09:30 $3.46 → close $3.47 +3.93; FRVO×69 09:30 $17.27 → close $18.16 +61.41; CTMX×346 09:30 $3.64 → close $3.71 +24.22; EIX×22 09:30 $55.79 → close $56.77 +21.56; CRDL×595 09:30 $2.16 → close $2.20 +23.80; SION×194 09:30 $6.68 → close $7.18 +97.00; DUOL×8 09:30 $157.46 → close $154.46 -24.00; SAFX×3300 09:30 $0.38 → close $0.39 +33.00; SLBT×2 09:30 $3.15 → close $2.88 -0.54; IRD×1 09:30 $4.53 → close $4.67 +0.14; JLHL×1 09:30 $6.20 → close $6.09 -0.11; MRLN×1 09:30 $3.28 → close $3.35 +0.07; BTBT×2 09:30 $1.60 → close $1.64 +0.08; BRR×1 09:30 $2.51 → close $2.66 +0.15 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 3.52 < 1 share @ 359.83 |
| 2026-08-14 | `NRG` | cash | leftover split 3.52 < 1 share @ 120.00 |
| 2026-08-14 | `MARA` | cash | leftover split 3.52 < 1 share @ 9.01 |
| 2026-08-14 | `ARX` | cash | leftover split 3.52 < 1 share @ 19.57 |
| 2026-08-14 | `HLIT` | cash | leftover split 3.52 < 1 share @ 13.18 |
| 2026-08-14 | `SECZ` | cash | leftover split 3.52 < 1 share @ 5.84 |
| 2026-08-14 | `LFTO` | cash | leftover split 3.52 < 1 share @ 20.57 |
| 2026-08-14 | `REZI` | cash | leftover split 3.52 < 1 share @ 20.56 |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TMC` | cash | leftover split 3.52 < 1 share @ 4.05 |
| 2026-08-17 | `TGB` | cash | leftover split 3.52 < 1 share @ 8.46 |
| 2026-08-17 | `ELF` | cash | leftover split 3.52 < 1 share @ 90.54 |
| 2026-08-17 | `CAPR` | cash | leftover split 3.52 < 1 share @ 6.87 |
| 2026-08-17 | `NU` | cash | leftover split 3.52 < 1 share @ 15.40 |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `INV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `KLC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TBPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ENVX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `STUB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `INV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `KLC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `FN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CRCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WYFI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TOYO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CRSP` | cash | leftover split 14.00 < 1 share @ 59.72 |
| 2026-08-21 | `FUTU` | cash | leftover split 14.00 < 1 share @ 115.18 |
| 2026-08-21 | `GMAB` | cash | leftover split 14.00 < 1 share @ 33.36 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CRCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WYFI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TOYO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ENHA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PAAS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ENHA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `SAFX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AUTL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRDL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ENHA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CAN` | no_price | no 09:30 open — carry |
| 2026-08-26 | `PUSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SUJA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FWDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `JANX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-26 | `AVEX` | no_price | no 09:30 open |
| 2026-08-26 | `BE` | no_price | no 09:30 open |
| 2026-08-26 | `INDP` | no_price | no 09:30 open |
| 2026-08-26 | `AXTI` | no_price | no 09:30 open |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `PUSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SUJA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FWDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `JANX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `MU` | cash | leftover split 163.04 < 1 share @ 925.74 |
| 2026-08-27 | `LRCX` | cash | leftover split 163.04 < 1 share @ 314.61 |
| 2026-08-27 | `MRVL` | cash | leftover split 163.04 < 1 share @ 240.00 |
| 2026-08-27 | `NUE` | cash | leftover split 163.04 < 1 share @ 248.91 |
| 2026-08-28 | `ACMR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `MT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `TX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ACMR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `GGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `TX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `OPTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TTMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BBWI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `OPTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TTMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BBWI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OKE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `IRD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BEP` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `STIM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VLRS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CTMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `EIX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DUOL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ASND` | cash | leftover split 8.45 < 1 share @ 266.94 |
| 2026-09-04 | `MLYS` | cash | leftover split 8.45 < 1 share @ 29.15 |
| 2026-09-04 | `CCOI` | cash | leftover split 8.45 < 1 share @ 10.22 |
| 2026-09-07 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-07 | `FRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-07 | `CTMX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-07 | `EIX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-07 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-07 | `SION` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-07 | `DUOL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-07 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-07 | `SLBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-07 | `IRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-07 | `JLHL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-07 | `ABTC` | cash | leftover split 4.19 < 1 share @ 8.95 |
| 2026-09-07 | `CRCL` | cash | leftover split 4.19 < 1 share @ 97.98 |
| 2026-09-07 | `MSTR` | cash | leftover split 4.19 < 1 share @ 137.35 |
| 2026-09-07 | `DFDV` | cash | leftover split 4.19 < 1 share @ 5.79 |
| 2026-09-07 | `CIFR` | cash | leftover split 4.19 < 1 share @ 17.33 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CABA` | 393 | 2026-09-03 @ $3.27 | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1287.35 |
| `FRVO` | 69 | 2026-09-03 @ $18.40 | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1287.35 |
| `CTMX` | 346 | 2026-09-03 @ $3.72 | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1287.35 |
| `EIX` | 22 | 2026-09-03 @ $56.78 | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; ret5=+0.3; leftover $1287.35 |
| `CRDL` | 595 | 2026-09-03 @ $2.16 | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1287.35 |
| `SION` | 194 | 2026-09-03 @ $6.63 | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=-18.1; leftover $1287.35 |
| `DUOL` | 8 | 2026-09-03 @ $156.24 | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=+10.0; leftover $1287.35 |
| `SAFX` | 3300 | 2026-09-03 @ $0.39 | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=-26.5; leftover $1287.35 |
| `SLBT` | 2 | 2026-09-04 @ $3.07 | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $8.45 |
| `IRD` | 1 | 2026-09-04 @ $4.66 | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $8.45 |
| `JLHL` | 1 | 2026-09-04 @ $6.20 | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=-8.2; leftover $8.45 |
| `MRLN` | 1 | 2026-09-07 @ $3.28 | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.6; leftover $4.19 |
| `BTBT` | 2 | 2026-09-07 @ $1.60 | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-2.5; leftover $4.19 |
| `BRR` | 1 | 2026-09-07 @ $2.51 | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+28.0; leftover $4.19 |
